//! The snapshot-isolation + provenance correctness oracle ([STL-168], [STL-248]).
//!
//! Three guarantees of the transaction layer that, like every temporal behaviour,
//! are not "done" without an oracle
//! ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart),
//! [architecture invariant 5](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)):
//!
//! * **Snapshot isolation.** A reader at snapshot `s` never observes an
//!   uncommitted version, nor one a concurrent writer commits at `c > s`. The
//!   version it resolves is exactly the one the snapshot fixes — stable for the
//!   transaction's whole life, however the concurrent commits interleave.
//! * **Provenance.** Every version a reader resolves carries the `txn_id` and
//!   `committed_at` of the transaction that actually wrote it — captured inline
//!   at commit, never reconstructed.
//! * **Write-write conflict outcomes** ([STL-248]). When two transactions race to
//!   write the same key, exactly one commits and the other gets the clean
//!   `Conflict` retry signal — first committer wins. The engine's per-transaction
//!   Ok/Conflict *decision* must match an independent first-committer-wins rule,
//!   not merely "some conflicts happened".
//!
//! The system under test is the real concurrency core the DST strategy names —
//! [`stele-txn`](stele_txn)'s [`TxnManager`] over [`stele-storage`](stele_storage)'s
//! append-only [`Delta`]/[`ValidityIndex`] — driven under the deterministic
//! [`Scheduler`](crate::Scheduler) so concurrent multi-statement transactions
//! interleave in a seed-determined order. Both halves of snapshot isolation are
//! checked against the same kind of deliberately-dumb reference docs §4 prescribes:
//! a per-key list of committed periods ([`RefModel`]) whose `AS OF` answer is a
//! linear scan, too simple to be wrong.
//!
//! Three layers of checking:
//! * **Live**, during the interleaving: whenever a transaction reads a key at its
//!   snapshot, [`assert_snapshot_stable`] asserts the resolved version matches the
//!   reference *at that snapshot* — the snapshot-stability guarantee, exercised
//!   while concurrent writers are committing newer versions.
//! * **Differential**, after the run: [`differential_check`] sweeps every commit
//!   boundary and key and asserts the engine's resolved
//!   `(sys_from, sys_to, txn_id, committed_at, payload)` matches the reference.
//! * **Conflict-outcome differential** ([STL-248]), after the run:
//!   [`conflict_check`] replays each transaction's recorded commit attempt
//!   (snapshot, written keys, Ok/Conflict) through an independent
//!   first-committer-wins rule and asserts the engine's decision matches — the
//!   engine's recorded outcomes must be *exactly* what first-committer-wins
//!   prescribes, re-derived from the observable attempt sequence, not its internal
//!   write index.
//!
//! The [mutation tests](#tests) inject three *documented intentional bugs* — a
//! reader that sees a version newer than its snapshot ([`Bug::SeesNewer`], the
//! textbook SI violation), a misattributed writer ([`Bug::WrongWriter`]), and a
//! rule that never predicts a conflict ([`ConflictRule::NeverConflicts`]) — and
//! prove the matching check **catches** each. An oracle that cannot fail a wrong
//! implementation guards nothing (STL-168, STL-248 DoD).
//!
//! [STL-168]: https://allegromusic.atlassian.net/browse/STL-168
//! [STL-248]: https://allegromusic.atlassian.net/browse/STL-248

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::merge;
use stele_storage::validity::{Close, ValidityConfig, ValidityIndex};
use stele_storage::wal::{Wal, WalConfig};
use stele_txn::{Transaction, TxnError, TxnManager};

use crate::{Rng, Scheduler, StepClock, fnv1a, yield_now};

const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

/// Who the oracle records as the writing principal — opaque to the storage layer.
const PRINCIPAL: &[u8] = b"si-oracle";

/// Which visibility / provenance rule the reference applies — the seam where a
/// *documented intentional bug* is injected for the mutation tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bug {
    /// The correct snapshot-isolation rule: a committed period `[sys_from, sys_to)`
    /// is visible at snapshot `s` iff `sys_from ≤ s < sys_to`, and its provenance
    /// is reported verbatim.
    None,
    /// The textbook SI violation: a reader sees a version committed *after* its
    /// snapshot, modelled by shifting the query point forward one tick
    /// (`sys_from ≤ s + 1 < sys_to`). At the instant just before a version
    /// commits, this wrongly reports it as already live where the engine
    /// (correctly) resolves the prior version or nothing. The mutation test
    /// asserts the oracle catches it.
    SeesNewer,
    /// A provenance violation: the reference misattributes the writing
    /// transaction (`txn_id` perturbed). The engine reports the inline-captured
    /// writer; the mutation test asserts the oracle catches the mismatch.
    WrongWriter,
}

impl Bug {
    /// Does the committed period `[sys_from, sys_to)` cover snapshot `s` under
    /// this rule? Only [`Bug::SeesNewer`] perturbs visibility; [`Bug::WrongWriter`]
    /// keeps the correct half-open rule and perturbs the reported writer instead.
    const fn covers(self, sys_from: SystemTimeMicros, sys_to: SystemTimeMicros, s: i64) -> bool {
        match self {
            Self::None | Self::WrongWriter => sys_from.0 <= s && s < sys_to.0,
            Self::SeesNewer => sys_from.0 <= s + 1 && s + 1 < sys_to.0,
        }
    }
}

/// One committed period in the reference model's per-key timeline: the
/// system-time half-open span `[sys_from, sys_to)` over which `payload` (written
/// by transaction `txn_id`) was the live value. The latest period of a key is
/// *open* — its `sys_to` is [`SYSTEM_TIME_OPEN`] — until a later commit closes it.
#[derive(Debug, Clone)]
struct RefPeriod {
    sys_from: SystemTimeMicros,
    sys_to: SystemTimeMicros,
    txn_id: TxnId,
    payload: Vec<u8>,
}

/// The reference model: each business key maps to the ordered, contiguous,
/// non-overlapping list of its committed periods. Independent of the engine's
/// tiered, validity-indexed read path on purpose — it is the check, not a mirror.
#[derive(Debug, Default)]
struct RefModel {
    timelines: BTreeMap<BusinessKey, Vec<RefPeriod>>,
}

impl RefModel {
    /// Close the key's currently-open period (if any) at `sys_to` — the
    /// write-once boundary a superseding commit records.
    fn close_current(&mut self, key: &BusinessKey, sys_to: SystemTimeMicros) {
        if let Some(open) = self
            .timelines
            .get_mut(key)
            .and_then(|periods| periods.last_mut())
            .filter(|p| p.sys_to == SYSTEM_TIME_OPEN)
        {
            open.sys_to = sys_to;
        }
    }

    /// Open a fresh `[sys_from, +∞)` period carrying `payload`, attributed to
    /// `txn_id`.
    fn open(
        &mut self,
        key: BusinessKey,
        sys_from: SystemTimeMicros,
        txn_id: TxnId,
        payload: Vec<u8>,
    ) {
        self.timelines.entry(key).or_default().push(RefPeriod {
            sys_from,
            sys_to: SYSTEM_TIME_OPEN,
            txn_id,
            payload,
        });
    }

    /// The reference `AS OF (s)`: a linear scan for the single period covering `s`
    /// under `bug`, rendered as the [`Answer`] the engine's resolved version is
    /// compared against. `None` when `key` has no period at `s`. Under
    /// [`Bug::WrongWriter`] the reported writer is perturbed so the differential
    /// check's provenance comparison has something to catch.
    fn answer(&self, key: &BusinessKey, s: SystemTimeMicros, bug: Bug) -> Option<Answer> {
        let period = self
            .timelines
            .get(key)?
            .iter()
            .find(|p| bug.covers(p.sys_from, p.sys_to, s.0))?;
        let mut answer = Answer::from_ref(period);
        if matches!(bug, Bug::WrongWriter) {
            answer.txn_id = TxnId(answer.txn_id.0 ^ 1);
        }
        Some(answer)
    }
}

/// A resolved version, rendered for comparison and for the divergence report. The
/// equivalence key against the engine's [`Version`]: payload, system-time
/// interval, **and** inline provenance (`txn_id`, `committed_at`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Answer {
    sys_from: SystemTimeMicros,
    sys_to: SystemTimeMicros,
    txn_id: TxnId,
    committed_at: SystemTimeMicros,
    payload: Vec<u8>,
}

impl Answer {
    fn from_ref(p: &RefPeriod) -> Self {
        Self {
            sys_from: p.sys_from,
            sys_to: p.sys_to,
            txn_id: p.txn_id,
            // A committed period's provenance `committed_at` is, by construction,
            // the instant it opened — the engine must echo exactly that.
            committed_at: p.sys_from,
            payload: p.payload.clone(),
        }
    }

    fn from_engine(v: &Version) -> Self {
        Self {
            sys_from: v.sys_from,
            sys_to: v.sys_to,
            txn_id: v.provenance.txn_id,
            committed_at: v.provenance.committed_at,
            // This oracle never writes a SQL NULL payload ([STL-154]); a `None`
            // here would be a write-path bug, so surface it loudly.
            payload: v
                .payload
                .clone()
                .expect("oracle scenarios never write a NULL payload"),
        }
    }
}

impl fmt::Display for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "live[sys_from={}, sys_to={}, txn_id={}, committed_at={}, payload={}]",
            self.sys_from.0,
            self.sys_to.0,
            self.txn_id.0,
            self.committed_at.0,
            String::from_utf8_lossy(&self.payload)
        )
    }
}

fn render_opt(answer: Option<&Answer>) -> String {
    answer.map_or_else(|| "<not live>".to_string(), ToString::to_string)
}

fn render_key(key: &BusinessKey) -> String {
    String::from_utf8_lossy(key.as_bytes()).into_owned()
}

/// A first point of disagreement between the engine and the reference, carrying
/// everything needed to reproduce and read it: the diverging snapshot, the key,
/// both answers, and the diverging key's committed timeline (the writes that
/// built it — the minimal reproducer for its `AS OF` answer).
#[derive(Debug)]
struct Divergence {
    snapshot: SystemTimeMicros,
    key: BusinessKey,
    engine: Option<Answer>,
    reference: Option<Answer>,
    timeline: Vec<RefPeriod>,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "SI/provenance oracle divergence @ snapshot={} key={}:",
            self.snapshot.0,
            render_key(&self.key)
        )?;
        writeln!(f, "  engine    = {}", render_opt(self.engine.as_ref()))?;
        writeln!(f, "  reference = {}", render_opt(self.reference.as_ref()))?;
        writeln!(
            f,
            "committed timeline for the key ({} period(s)):",
            self.timeline.len()
        )?;
        for (i, p) in self.timeline.iter().enumerate() {
            writeln!(
                f,
                "  #{i:<3} [{}, {}) txn_id={} payload={}",
                p.sys_from.0,
                p.sys_to.0,
                p.txn_id.0,
                String::from_utf8_lossy(&p.payload)
            )?;
        }
        Ok(())
    }
}

/// One transaction's commit attempt, recorded in the order the workload attempted
/// commits — the scheduler's serialization order, since
/// [`commit_and_record`] runs without yielding. Replayed after the run by
/// [`conflict_check`] to assert the engine's first-committer-wins decision matches
/// an independent reference ([STL-248]).
#[derive(Debug, Clone)]
struct Attempt {
    /// The snapshot the transaction pinned at `begin` — its conflict anchor.
    snapshot: SystemTimeMicros,
    /// The business keys it staged a write to (empty for a read-only transaction).
    keys: BTreeSet<BusinessKey>,
    /// The commit instant the engine assigned, or `None` if the engine refused the
    /// commit with a write-write [`TxnError::Conflict`].
    commit_ts: Option<SystemTimeMicros>,
}

/// The world the workload builds: the engine's storage tiers, the independent
/// reference model, the commit boundaries to probe, how many transactions lost a
/// write-write race (so a test can assert the workload genuinely contends), and the
/// per-transaction commit attempts the conflict oracle replays ([STL-248]).
struct World {
    delta: Delta<MemDisk>,
    index: ValidityIndex<MemDisk>,
    model: RefModel,
    commits: Vec<SystemTimeMicros>,
    conflicts: usize,
    attempts: Vec<Attempt>,
}

impl World {
    fn empty() -> Self {
        Self {
            delta: Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta"),
            index: ValidityIndex::open(MemDisk::new(), ValidityConfig::default())
                .expect("open index"),
            model: RefModel::default(),
            commits: Vec::new(),
            conflicts: 0,
            attempts: Vec::new(),
        }
    }
}

/// Read back the single version live for `key` at `at`, resolving its end and
/// `closed_by` overlay from the validity `index` ([ADR-0023]). At most one version
/// is live for a key at a snapshot.
fn read_live(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    key: &BusinessKey,
    at: Snapshot,
) -> Option<Version> {
    delta
        .range_scan(key.clone()..=key.clone(), at, index)
        .expect("range scan")
        .into_iter()
        .next()
}

/// Stage a committed write of `payload` to `key` at `commit` into both the engine
/// and the reference: close the key's prior open version (write-once into the
/// validity index), open the new `[commit, +∞)` version, and mirror the same
/// supersede in the model. The version's inline provenance is `(txn_id, commit)` —
/// the manager-assigned identity the provenance oracle checks the read path
/// preserves.
fn stage_commit(
    world: &mut World,
    key: &BusinessKey,
    txn_id: TxnId,
    commit: SystemTimeMicros,
    payload: Vec<u8>,
) {
    let who = || Provenance::new(txn_id, commit, Principal::new(PRINCIPAL.to_vec()));

    // Engine: close any open version at `commit`, then open the new one. `commit`
    // is strictly greater than every prior `sys_from`, so the open version (if
    // any) is the one resolved at `Snapshot(commit)`.
    let candidates = world
        .delta
        .candidate_versions(key)
        .expect("candidate versions");
    let live = merge::resolve_open(&candidates, &[], &world.index, key, Snapshot(commit))
        .expect("resolve");
    if let Some(open) = live {
        world
            .index
            .insert_close(Close {
                business_key: key.clone(),
                sys_from: open.sys_from,
                seq: open.seq,
                sys_to: commit,
                closed_by: who(),
            })
            .expect("close prior version");
    }
    world
        .delta
        .insert(Version::open(
            key.clone(),
            commit,
            0,
            who(),
            Some(payload.clone()),
        ))
        .expect("open new version");

    // Reference: the same supersede — close the prior period, open the new one.
    world.model.close_current(key, commit);
    world.model.open(key.clone(), commit, txn_id, payload);
}

/// The payload a transaction's statement writes — deterministic in the writer and
/// statement index, so the engine and the reference record the identical bytes.
fn payload_for(txn_id: TxnId, stmt: usize) -> Vec<u8> {
    format!("t{}-s{}", txn_id.0, stmt).into_bytes()
}

/// The snapshot-stability guarantee, asserted live: the version the engine
/// resolves for `key` at `snapshot` matches the reference *at that snapshot* —
/// even as concurrent writers commit newer versions (their commits land strictly
/// after `snapshot`, so the reference's answer at `snapshot` cannot have changed
/// since this transaction began).
///
/// # Panics
///
/// Panics with both answers if the engine's resolved version differs from the
/// reference — an SI or provenance violation, reproducible from the scenario seed.
fn assert_snapshot_stable(
    shared: &Rc<RefCell<World>>,
    key: &BusinessKey,
    snapshot: Snapshot,
    txn_id: TxnId,
) {
    let (engine, reference) = {
        let world = shared.borrow();
        let live = read_live(&world.delta, &world.index, key, snapshot);
        (
            live.as_ref().map(Answer::from_engine),
            world.model.answer(key, snapshot.0, Bug::None),
        )
    };
    assert!(
        engine == reference,
        "snapshot-isolation violation: txn {} reading key {} at snapshot {} resolved {} \
         but its snapshot fixes {}",
        txn_id.0,
        render_key(key),
        snapshot.0.0,
        render_opt(engine.as_ref()),
        render_opt(reference.as_ref()),
    );
}

/// Commit a transaction, then stage and record its writes as one atomic step
/// (no scheduler yield between the commit and the record, so no other task can
/// observe a half-applied commit or reorder it past this one).
///
/// A write-write conflict is the clean, expected SI retry signal: the loser
/// committed nothing, so neither the engine nor the reference records it — exactly
/// the "a reader never sees an uncommitted version" guarantee, by construction.
///
/// Either way the transaction's commit *attempt* — its snapshot, written keys, and
/// Ok/Conflict outcome — is appended to [`World::attempts`] in commit order, so
/// [`conflict_check`] can later re-derive the engine's first-committer-wins
/// decisions from an independent reference ([STL-248]).
fn commit_and_record(
    mgr: &TxnManager<StepClock, MemDisk>,
    shared: &Rc<RefCell<World>>,
    txn: Transaction,
    writes: BTreeMap<BusinessKey, Vec<u8>>,
) {
    // The conflict anchor and write set, captured before `commit` consumes `txn`.
    // `snapshot()` is a [`Snapshot`] newtype; its `.0` is the system-time instant
    // the first-committer-wins comparison is against.
    let snapshot = txn.snapshot().0;
    let keys: BTreeSet<BusinessKey> = writes.keys().cloned().collect();
    match mgr.commit(txn) {
        Ok(committed) => {
            let mut world = shared.borrow_mut();
            // A read-only transaction stages nothing (the loop is empty) but still
            // consumed a commit timestamp; its attempt is recorded all the same so
            // the conflict oracle sees every commit decision.
            for (key, payload) in writes {
                stage_commit(
                    &mut world,
                    &key,
                    committed.txn_id,
                    committed.commit_ts,
                    payload,
                );
            }
            if !keys.is_empty() {
                world.commits.push(committed.commit_ts);
            }
            world.attempts.push(Attempt {
                snapshot,
                keys,
                commit_ts: Some(committed.commit_ts),
            });
        }
        Err(TxnError::Conflict) => {
            let mut world = shared.borrow_mut();
            world.conflicts += 1;
            world.attempts.push(Attempt {
                snapshot,
                keys,
                commit_ts: None,
            });
        }
        // A WAL or time-exhaustion error is a real failure, not a workload
        // outcome — fail loudly rather than digest a "valid" run.
        Err(other) => panic!("unexpected transaction error (not a workload outcome): {other}"),
    }
}

/// Run a seeded workload of concurrent multi-statement transactions through the
/// real [`TxnManager`] + storage, interleaved by the deterministic
/// [`Scheduler`](crate::Scheduler), and return the resulting [`World`].
///
/// Each transaction begins (pinning a snapshot), yields so its peers' snapshots
/// overlap, runs a seed-chosen mix of reads (asserting snapshot stability live)
/// and write declarations, yields again, then commits. Whoever commits first wins
/// a contended key; the loser gets the clean conflict signal. Same seed ⇒ same
/// interleaving ⇒ same world.
fn build_world(seed: u64) -> World {
    const MAX_STATEMENTS: usize = 3;
    const READ_IN: u64 = 3; // ~1 in READ_IN statements is a read.

    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    // Start the clock high enough that the `first_commit - 1` probe is positive.
    let mgr = Rc::new(TxnManager::new(StepClock::new(1_000), wal));
    let shared = Rc::new(RefCell::new(World::empty()));

    let mut driver = Rng::new(seed);
    let key_count = 2 + driver.below_usize(3); // 2..=4 keys → frequent contention
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:02}").into_bytes()))
        .collect();
    let txn_count = 4 + driver.below_usize(4); // 4..=7 concurrent transactions

    let mut sched = Scheduler::new(seed);
    for _ in 0..txn_count {
        let mgr = Rc::clone(&mgr);
        let shared = Rc::clone(&shared);
        let keys = keys.clone();
        // An independent, well-mixed sub-seed per transaction: its statement mix
        // is fixed per scenario seed, so only the conflict outcomes depend on the
        // scheduler's interleaving.
        let mut rng = Rng::new(driver.next_u64());
        sched.spawn(async move {
            let mut txn = mgr.begin();
            let txn_id = txn.id();
            let snapshot = txn.snapshot();
            // Yield so peer transactions begin before this one commits — the
            // overlapping-snapshot setup that makes write-write conflicts possible.
            yield_now().await;

            let mut writes: BTreeMap<BusinessKey, Vec<u8>> = BTreeMap::new();
            let statements = 1 + rng.below_usize(MAX_STATEMENTS);
            for stmt in 0..statements {
                let key = keys[rng.below_usize(keys.len())].clone();
                if rng.below(READ_IN) == 0 {
                    assert_snapshot_stable(&shared, &key, snapshot, txn_id);
                } else {
                    txn.write(key.clone());
                    // Last write to a key in this txn wins (BTreeMap), so each key
                    // is staged once — no degenerate zero-width period.
                    writes.insert(key, payload_for(txn_id, stmt));
                }
                yield_now().await;
            }
            // Yield once more so commits interleave across transactions.
            yield_now().await;
            commit_and_record(&mgr, &shared, txn, writes);
        });
    }
    let _trace = sched.run();
    drop(mgr);

    Rc::try_unwrap(shared)
        .map_err(|_| "no task may outlive the scheduler run")
        .expect("scheduler run drops every task, leaving one strong ref")
        .into_inner()
}

/// The probe set: one tick before, exactly on, and one tick after every commit
/// boundary — the instants where a half-open / closed-interval off-by-one, or a
/// "sees newer than snapshot" violation, bites. De-duplicated and sorted so the
/// digest is order-stable.
fn probes(commits: &[SystemTimeMicros]) -> Vec<SystemTimeMicros> {
    let mut points: BTreeSet<i64> = BTreeSet::new();
    for c in commits {
        points.insert(c.0 - 1);
        points.insert(c.0);
        points.insert(c.0 + 1);
    }
    points.into_iter().map(SystemTimeMicros).collect()
}

/// Sweep every `(probe, key)` and compare the engine's resolved version against
/// the reference under `bug`. Returns a digest of the agreed answers, or the
/// first [`Divergence`]. Keys are taken from the model so a key is probed even
/// where it is not live (asserting both agree on `None`).
fn differential_check(world: &World, bug: Bug) -> Result<u64, Box<Divergence>> {
    let keys: Vec<BusinessKey> = world.model.timelines.keys().cloned().collect();
    let mut digest = FNV_OFFSET;
    for s in probes(&world.commits) {
        for key in &keys {
            let live = read_live(&world.delta, &world.index, key, Snapshot(s));
            let engine = live.as_ref().map(Answer::from_engine);
            let reference = world.model.answer(key, s, bug);

            if engine != reference {
                let timeline = world
                    .model
                    .timelines
                    .get(key)
                    .map(|periods| {
                        periods
                            .iter()
                            .filter(|p| p.sys_from.0 <= s.0)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                return Err(Box::new(Divergence {
                    snapshot: s,
                    key: key.clone(),
                    engine,
                    reference,
                    timeline,
                }));
            }

            digest = fnv1a(digest, key.as_bytes());
            digest = fnv1a(digest, &s.0.to_le_bytes());
            match &engine {
                Some(a) => {
                    digest = fnv1a(digest, &[1]);
                    digest = fnv1a(digest, &a.sys_from.0.to_le_bytes());
                    digest = fnv1a(digest, &a.sys_to.0.to_le_bytes());
                    digest = fnv1a(digest, &a.committed_at.0.to_le_bytes());
                    digest = fnv1a(digest, &a.txn_id.0.to_le_bytes());
                    digest = fnv1a(digest, &a.payload);
                }
                None => digest = fnv1a(digest, &[0]),
            }
        }
    }
    Ok(digest)
}

/// Which first-committer-wins rule the reference applies when replaying the
/// recorded commit attempts ([`conflict_check`]) — the seam a *documented
/// intentional bug* is injected into for the conflict mutation test ([STL-248]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConflictRule {
    /// The correct rule: a transaction conflicts iff some key it wrote was committed
    /// by an earlier transaction at an instant strictly after this one's snapshot.
    Correct,
    /// A broken rule that never predicts a conflict, so the loser of every
    /// write-write race is wrongly expected to commit. The mutation test asserts the
    /// check catches the disagreement with the engine.
    NeverConflicts,
}

impl ConflictRule {
    /// Does a transaction with `snapshot` writing `keys` conflict, given each key's
    /// last committed instant in `committed_at`? First committer wins: a key
    /// committed strictly after our snapshot means a concurrent writer beat us.
    fn conflicts(
        self,
        committed_at: &BTreeMap<BusinessKey, SystemTimeMicros>,
        keys: &BTreeSet<BusinessKey>,
        snapshot: SystemTimeMicros,
    ) -> bool {
        match self {
            Self::Correct => keys
                .iter()
                .any(|k| committed_at.get(k).is_some_and(|&at| at > snapshot)),
            Self::NeverConflicts => false,
        }
    }
}

/// A disagreement between the engine's recorded commit decision and the reference
/// first-committer-wins rule for one attempt, carrying enough to reproduce it.
#[derive(Debug)]
struct ConflictDivergence {
    snapshot: SystemTimeMicros,
    keys: Vec<BusinessKey>,
    engine_conflicted: bool,
    reference_conflicts: bool,
}

impl fmt::Display for ConflictDivergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<String> = self.keys.iter().map(render_key).collect();
        write!(
            f,
            "conflict-outcome oracle divergence @ snapshot={} keys=[{}]: engine {}, reference {}",
            self.snapshot.0,
            keys.join(", "),
            if self.engine_conflicted {
                "conflicted"
            } else {
                "committed"
            },
            if self.reference_conflicts {
                "expects a conflict"
            } else {
                "expects a commit"
            },
        )
    }
}

/// Replay the recorded commit attempts in commit-attempt order through an
/// independent first-committer-wins reference and assert the engine's Ok/Conflict
/// decision matches under `rule` ([STL-248]).
///
/// The reference is a per-key "last committer" index, updated from the engine's own
/// assigned commit timestamps — as the version oracle uses them — so only the
/// *decision rule* is independent: the engine's recorded outcome for each attempt
/// must be exactly what first-committer-wins prescribes given the attempts that
/// committed before it. A bug in the engine's conflict logic (a wrong comparison, a
/// missed committer, a key recorded at the wrong instant) makes its outcomes
/// inconsistent with this replay. Returns the first [`ConflictDivergence`], or `Ok`
/// if every attempt agreed.
fn conflict_check(attempts: &[Attempt], rule: ConflictRule) -> Result<(), Box<ConflictDivergence>> {
    let mut committed_at: BTreeMap<BusinessKey, SystemTimeMicros> = BTreeMap::new();
    for attempt in attempts {
        let reference_conflicts = rule.conflicts(&committed_at, &attempt.keys, attempt.snapshot);
        let engine_conflicted = attempt.commit_ts.is_none();
        if reference_conflicts != engine_conflicted {
            return Err(Box::new(ConflictDivergence {
                snapshot: attempt.snapshot,
                keys: attempt.keys.iter().cloned().collect(),
                engine_conflicted,
                reference_conflicts,
            }));
        }
        // A committed attempt becomes the newest committer of each key it wrote.
        if let Some(commit_ts) = attempt.commit_ts {
            for key in &attempt.keys {
                committed_at.insert(key.clone(), commit_ts);
            }
        }
    }
    Ok(())
}

/// Run the snapshot-isolation + provenance + conflict-outcome oracle for `seed`.
///
/// Drives concurrent multi-statement transactions through the real
/// [`TxnManager`] + storage under the deterministic scheduler (asserting snapshot
/// stability live), then (1) replays every transaction's commit attempt through an
/// independent first-committer-wins rule and asserts the engine's Ok/Conflict
/// decision matches (the conflict check, [STL-248]), and (2) sweeps every commit
/// boundary and asserts the engine's resolved version — payload, interval, and
/// inline provenance — matches the independent reference model
/// ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart),
/// architecture invariant 5, STL-168). Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics with the diverging attempt, or the diverging snapshot, key, and the key's
/// committed timeline, if the engine's conflict decision or any resolved version
/// disagrees with the reference — a correctness regression, not a workload outcome.
#[must_use]
pub fn run_si_oracle_seed(seed: u64) -> u64 {
    let world = build_world(seed);
    if let Err(divergence) = conflict_check(&world.attempts, ConflictRule::Correct) {
        panic!("seed {seed}: {divergence}");
    }
    match differential_check(&world, Bug::None) {
        Ok(digest) => digest,
        Err(divergence) => panic!("seed {seed}: {divergence}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn si_oracle_seed_is_reproducible() {
        // Each seed asserts (internally) snapshot stability live and every
        // resolved version against the reference at every probe (STL-168 DoD).
        for seed in 0..128 {
            assert_eq!(
                run_si_oracle_seed(seed),
                run_si_oracle_seed(seed),
                "seed {seed} must replay to an identical SI-oracle digest"
            );
        }
    }

    #[test]
    fn distinct_seeds_explore_distinct_workloads() {
        let digests: std::collections::HashSet<u64> = (0..128).map(run_si_oracle_seed).collect();
        assert!(
            digests.len() > 1,
            "the SI-oracle workload must actually depend on the seed"
        );
    }

    #[test]
    fn the_workload_actually_contends() {
        // The point of a concurrent SI oracle is write-write contention. Prove the
        // interleaving genuinely produces conflicts across a sweep — otherwise this
        // would be a sequential test wearing a scheduler.
        let total: usize = (0..64).map(|seed| build_world(seed).conflicts).sum();
        assert!(
            total > 0,
            "no transaction ever lost a write-write race across 64 seeds — the workload does not contend"
        );
    }

    /// A hand-built attempt sequence with one genuine write-write conflict: two
    /// transactions pinned at the same snapshot both write `k`; the first commits,
    /// and the second — its snapshot now older than the first's commit — must
    /// conflict. The controlled fixture the conflict mutation test perturbs.
    fn conflicting_attempts() -> Vec<Attempt> {
        let k = BusinessKey::new(b"k".to_vec());
        vec![
            Attempt {
                snapshot: SystemTimeMicros(1_000),
                keys: BTreeSet::from([k.clone()]),
                commit_ts: Some(SystemTimeMicros(1_005)),
            },
            Attempt {
                snapshot: SystemTimeMicros(1_000),
                keys: BTreeSet::from([k]),
                commit_ts: None,
            },
        ]
    }

    #[test]
    fn conflict_oracle_is_sound_under_the_correct_rule() {
        // The correct first-committer-wins replay must agree with a sequence that
        // genuinely conflicts, or the mutation test below would prove nothing.
        conflict_check(&conflicting_attempts(), ConflictRule::Correct)
            .expect("the correct rule must agree with a real first-committer-wins outcome");
    }

    /// The conflict mutation test (STL-248 DoD): a rule that never predicts a
    /// conflict must be caught by the replay exactly where the engine *did*
    /// conflict.
    #[test]
    fn conflict_oracle_catches_a_rule_that_never_predicts_conflicts() {
        let divergence = conflict_check(&conflicting_attempts(), ConflictRule::NeverConflicts)
            .expect_err("a rule that never predicts a conflict must disagree with the engine");
        assert!(
            divergence.engine_conflicted && !divergence.reference_conflicts,
            "the engine conflicted where the buggy rule expected a commit: {divergence}"
        );
    }

    #[test]
    fn the_conflict_oracle_is_non_vacuous_on_real_workloads() {
        // Tie the mutation to the seeded workload: the engine produces real
        // conflicts across seeds (`the_workload_actually_contends`), so the
        // never-conflicts rule must diverge on at least one seed — the conflict
        // check is exercising something the workload actually contains.
        let caught = (0..64).any(|seed| {
            conflict_check(&build_world(seed).attempts, ConflictRule::NeverConflicts).is_err()
        });
        assert!(
            caught,
            "no seed's recorded attempts contained a conflict the never-conflicts rule \
             missed — the conflict oracle would be vacuous"
        );
    }

    /// Hand-build a one-key history with two committed versions, plus a matching
    /// world — the controlled fixture the mutation tests perturb.
    fn two_version_world() -> (World, BusinessKey, SystemTimeMicros, SystemTimeMicros) {
        let key = BusinessKey::new(b"k".to_vec());
        let c1 = SystemTimeMicros(1_000);
        let c2 = SystemTimeMicros(1_005);
        let mut world = World::empty();
        stage_commit(&mut world, &key, TxnId(1), c1, b"v1".to_vec());
        stage_commit(&mut world, &key, TxnId(2), c2, b"v2".to_vec());
        world.commits = vec![c1, c2];
        (world, key, c1, c2)
    }

    #[test]
    fn harness_is_sound_under_the_correct_rule() {
        // The differential check must agree with the staged storage everywhere
        // under the correct rule, or the mutation tests below would prove nothing.
        let (world, _key, _c1, _c2) = two_version_world();
        differential_check(&world, Bug::None)
            .expect("the correct oracle must agree with the engine");
    }

    /// The SI mutation test (STL-168 DoD): a reader that sees a version committed
    /// *after* its snapshot — the textbook snapshot-isolation violation — must be
    /// caught by the differential check, exactly at the instant just before the
    /// version commits.
    #[test]
    fn oracle_catches_a_reader_seeing_a_newer_than_snapshot_version() {
        let (world, key, c1, _c2) = two_version_world();
        let divergence = differential_check(&world, Bug::SeesNewer)
            .expect_err("the oracle must catch the SI violation");
        assert_eq!(
            divergence.snapshot,
            SystemTimeMicros(c1.0 - 1),
            "the violation surfaces just before the first version commits"
        );
        assert_eq!(divergence.key, key);
        assert!(
            divergence.engine.is_none(),
            "the engine resolves nothing before v1 commits"
        );
        assert_eq!(
            divergence.reference.as_ref().map(|a| a.payload.clone()),
            Some(b"v1".to_vec()),
            "the buggy reference sees v1 one tick before it committed"
        );
    }

    /// The provenance mutation test (STL-168 DoD): a misattributed writer must be
    /// caught by the differential check's `txn_id` comparison, at the first
    /// snapshot where the version is live.
    #[test]
    fn oracle_catches_a_misattributed_writer() {
        let (world, key, c1, _c2) = two_version_world();
        let divergence = differential_check(&world, Bug::WrongWriter)
            .expect_err("the oracle must catch the provenance mismatch");
        assert_eq!(
            divergence.snapshot, c1,
            "the mismatch surfaces at the first snapshot where v1 is live"
        );
        assert_eq!(divergence.key, key);
        assert_eq!(
            divergence.engine.as_ref().map(|a| a.txn_id),
            Some(TxnId(1)),
            "the engine reports the inline-captured writer"
        );
        assert_eq!(
            divergence.reference.as_ref().map(|a| a.txn_id),
            Some(TxnId(1 ^ 1)),
            "the buggy reference misattributes the writer"
        );
    }
}
