//! The canonical `AS OF` correctness oracle ([STL-111]).
//!
//! The non-negotiable of a bitemporal database: for every random history a seed
//! generates, every `AS OF (sys)` answer the engine gives **must match** a
//! hand-coded, transparently-correct reference. No oracle, no temporal feature
//! ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)).
//!
//! The reference is the deliberately-dumb model docs §4 names: a
//! [`BTreeMap<BusinessKey, Vec<RefVersion>>`](RefModel) — a per-key list of
//! system-time periods. It answers an `AS OF (s)` query by a linear scan for the
//! single period whose half-open `[sys_from, sys_to)` contains `s`. It is far too
//! slow for production and far too simple to be wrong, which is exactly the point:
//! it is an independent check on the engine's tiered, merged, validity-indexed
//! read path, not a mirror of it.
//!
//! Two halves:
//! * [`run_as_of_oracle_seed`] — the sim scenario. Generate a random
//!   insert/update/delete history, apply it to a real [`Engine`] **and** to the
//!   reference, then sweep `AS OF` probes (including exactly on every commit
//!   boundary, where a half-open / closed-interval off-by-one would bite) and
//!   assert the engine's resolved version matches the reference's
//!   `(sys_from, sys_to, payload)`. On any divergence it panics with the
//!   [minimal reproducing history](Divergence) and the diverging snapshot.
//! * The [mutation test](#tests) — a *documented intentional bug*
//!   ([`Bug::InclusiveUpper`], a closed-interval off-by-one) that the very same
//!   differential check **catches**. An oracle that can't fail a wrong engine
//!   proves nothing; this is the proof that it has teeth (STL-111 DoD).
//!
//! [STL-111]: https://allegromusic.atlassian.net/browse/STL-111

use std::collections::BTreeMap;
use std::fmt;

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Snapshot, Version};
use stele_storage::engine::Engine;

use crate::{Rng, StepClock, fnv1a};

const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

/// Which `AS OF` containment rule the reference applies — the seam where the
/// *documented intentional bug* is injected for the mutation test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bug {
    /// The correct, half-open rule: a period `[sys_from, sys_to)` is live at `s`
    /// iff `sys_from ≤ s < sys_to`. The instant a period closes belongs to its
    /// successor, never to it.
    None,
    /// The intentional bug: treat the period as **closed** at the top —
    /// `sys_from ≤ s ≤ sys_to`. At the exact instant a version is superseded,
    /// this wrongly reports the *old* version as still live, where the engine
    /// (correctly) returns the new one. The mutation test asserts the oracle
    /// catches this.
    InclusiveUpper,
}

impl Bug {
    /// Does the period `[sys_from, sys_to)` contain `s` under this rule?
    const fn covers(self, sys_from: SystemTimeMicros, sys_to: SystemTimeMicros, s: i64) -> bool {
        match self {
            Self::None => sys_from.0 <= s && s < sys_to.0,
            Self::InclusiveUpper => sys_from.0 <= s && s <= sys_to.0,
        }
    }
}

/// One period in the reference model's per-key timeline: the system-time
/// half-open span `[sys_from, sys_to)` over which `payload` was the live value.
/// The latest period of a live key is *open* — its `sys_to` is
/// [`SYSTEM_TIME_OPEN`] — until a later write closes it.
#[derive(Debug, Clone)]
struct RefVersion {
    sys_from: SystemTimeMicros,
    sys_to: SystemTimeMicros,
    payload: Vec<u8>,
}

/// The reference model: each business key maps to the ordered, contiguous,
/// non-overlapping list of its live periods. A `DELETE` simply closes the
/// current period and opens no successor — so a snapshot in the resulting gap
/// resolves to nothing, exactly as the engine's retraction tombstone does.
#[derive(Debug, Default)]
struct RefModel {
    timelines: BTreeMap<BusinessKey, Vec<RefVersion>>,
}

impl RefModel {
    /// Close the key's currently-open period (if any) at `sys_to`. The write-once
    /// boundary an `UPDATE` or `DELETE` records.
    fn close_current(&mut self, key: &BusinessKey, sys_to: SystemTimeMicros) {
        if let Some(open) = self
            .timelines
            .get_mut(key)
            .and_then(|periods| periods.last_mut())
            .filter(|v| v.sys_to == SYSTEM_TIME_OPEN)
        {
            open.sys_to = sys_to;
        }
    }

    /// Open a fresh `[sys_from, +∞)` period carrying `payload`.
    fn open(&mut self, key: BusinessKey, sys_from: SystemTimeMicros, payload: Vec<u8>) {
        self.timelines.entry(key).or_default().push(RefVersion {
            sys_from,
            sys_to: SYSTEM_TIME_OPEN,
            payload,
        });
    }

    /// The reference `AS OF (s)`: a linear scan for the single period covering
    /// `s` under `bug`. `None` when `key` has no period at `s` (never written, or
    /// in a deletion gap). With the correct half-open rule at most one period can
    /// match; the bug can match two, and returning the *first* is what makes the
    /// divergence observable.
    fn as_of(&self, key: &BusinessKey, s: SystemTimeMicros, bug: Bug) -> Option<&RefVersion> {
        self.timelines
            .get(key)?
            .iter()
            .find(|v| bug.covers(v.sys_from, v.sys_to, s.0))
    }
}

/// One applied DML operation, recorded so a divergence dumps a self-contained,
/// minimal reproducing history (STL-111 "failure mode").
#[derive(Debug, Clone)]
enum OpKind {
    Insert(Vec<u8>),
    Update(Vec<u8>),
    Delete,
}

#[derive(Debug, Clone)]
struct HistoryOp {
    key: BusinessKey,
    kind: OpKind,
    commit: SystemTimeMicros,
}

impl fmt::Display for HistoryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = String::from_utf8_lossy(self.key.as_bytes());
        match &self.kind {
            OpKind::Insert(p) => write!(
                f,
                "INSERT key={key} payload={} @sys={}",
                String::from_utf8_lossy(p),
                self.commit.0
            ),
            OpKind::Update(p) => write!(
                f,
                "UPDATE key={key} payload={} @sys={}",
                String::from_utf8_lossy(p),
                self.commit.0
            ),
            OpKind::Delete => write!(f, "DELETE key={key} @sys={}", self.commit.0),
        }
    }
}

/// The reference's answer at a probe, rendered for the divergence report and used
/// as the equivalence key against the engine's resolved [`Version`].
#[derive(Debug, PartialEq, Eq)]
struct Answer {
    sys_from: SystemTimeMicros,
    sys_to: SystemTimeMicros,
    payload: Vec<u8>,
}

impl Answer {
    fn from_ref(v: &RefVersion) -> Self {
        Self {
            sys_from: v.sys_from,
            sys_to: v.sys_to,
            payload: v.payload.clone(),
        }
    }

    fn from_engine(v: &Version) -> Self {
        Self {
            sys_from: v.sys_from,
            sys_to: v.sys_to,
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
            "live[sys_from={}, sys_to={}, payload={}]",
            self.sys_from.0,
            self.sys_to.0,
            String::from_utf8_lossy(&self.payload)
        )
    }
}

fn render_opt(answer: Option<&Answer>) -> String {
    answer.map_or_else(|| "<not live>".to_string(), ToString::to_string)
}

/// A first point of disagreement between the engine and the reference, carrying
/// everything needed to reproduce and read it: the diverging snapshot, the key,
/// both answers, and the **minimal** reproducing history — only the operations on
/// the diverging key up to and including the probe snapshot, since the `AS OF`
/// answer at that snapshot depends on nothing else.
#[derive(Debug)]
struct Divergence {
    snapshot: SystemTimeMicros,
    key: BusinessKey,
    engine: Option<Answer>,
    reference: Option<Answer>,
    history: Vec<HistoryOp>,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "AS OF oracle divergence @ snapshot={} key={}:",
            self.snapshot.0,
            String::from_utf8_lossy(self.key.as_bytes())
        )?;
        writeln!(f, "  engine    = {}", render_opt(self.engine.as_ref()))?;
        writeln!(f, "  reference = {}", render_opt(self.reference.as_ref()))?;
        writeln!(
            f,
            "minimal reproducing history ({} ops):",
            self.history.len()
        )?;
        for (i, op) in self.history.iter().enumerate() {
            writeln!(f, "  #{i:<3} {op}")?;
        }
        Ok(())
    }
}

/// Replay a seeded insert/update/delete history into a fresh [`Engine`] and an
/// independent [`RefModel`], returning both plus the recorded history and the
/// commit timestamps (the natural `AS OF` boundaries to probe).
fn build_history(
    seed: u64,
) -> (
    Engine<StepClock, MemDisk>,
    RefModel,
    Vec<HistoryOp>,
    Vec<SystemTimeMicros>,
) {
    let mut rng = Rng::new(seed);
    // System-only table: payloads carry no valid-time prefix. The clock starts
    // high enough that the `first_commit - 1` probe is still a positive instant.
    let mut engine =
        Engine::open(MemDisk::new(), StepClock::new(1_000), false).expect("open engine");
    let mut model = RefModel::default();
    let mut history: Vec<HistoryOp> = Vec::new();
    let mut commits: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 1 + rng.below_usize(5);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();
    let mut live = vec![false; key_count];

    let ops = 8 + rng.below(24);
    for op in 0..ops {
        let k = rng.below_usize(key_count);
        let key = keys[k].clone();
        let txn = TxnId(op);
        let who = Principal::new(b"sim".to_vec());
        // The delete roll is drawn only when the key is live (short-circuit on
        // `live[k]`); an insert consumes none. Liveness is itself seed-determined,
        // so the whole op sequence stays deterministic and reproducible per seed.
        let is_delete = live[k] && rng.below(4) == 0;

        let kind = if live[k] && is_delete {
            let commit = engine.delete(&key, txn, who).expect("delete").commit;
            model.close_current(&key, commit);
            live[k] = false;
            HistoryOp {
                key,
                kind: OpKind::Delete,
                commit,
            }
        } else if live[k] {
            let payload = format!("v{op}").into_bytes();
            let commit = engine
                .update(key.clone(), None, Some(payload.clone()), 0, txn, who)
                .expect("update")
                .commit;
            // An update closes the prior period and opens the new one at the same
            // instant — exactly the engine's supersede.
            model.close_current(&key, commit);
            model.open(key.clone(), commit, payload.clone());
            HistoryOp {
                key,
                kind: OpKind::Update(payload),
                commit,
            }
        } else {
            let payload = format!("v{op}").into_bytes();
            let commit = engine
                .insert(key.clone(), None, Some(payload.clone()), 0, txn, who)
                .expect("insert")
                .commit;
            model.open(key.clone(), commit, payload.clone());
            live[k] = true;
            HistoryOp {
                key,
                kind: OpKind::Insert(payload),
                commit,
            }
        };
        commits.push(kind.commit);
        history.push(kind);
    }

    (engine, model, history, commits)
}

/// The probe set: just before the first commit, **exactly on** every commit
/// boundary (where a half-open / closed-interval off-by-one diverges), and just
/// past the last.
fn probes(commits: &[SystemTimeMicros]) -> Vec<SystemTimeMicros> {
    let mut probes = Vec::with_capacity(commits.len() + 2);
    if let Some(first) = commits.first() {
        probes.push(SystemTimeMicros(first.0 - 1));
    }
    probes.extend(commits.iter().copied());
    if let Some(last) = commits.last() {
        probes.push(SystemTimeMicros(last.0 + 1));
    }
    probes
}

/// Sweep every `(probe, key)` and compare the engine's resolved version against
/// the reference under `bug`. Returns a digest of the agreed answers, or the
/// first [`Divergence`]. The keys are taken from the model so a key that was
/// touched is probed even where it is not live (asserting both agree on `None`).
fn differential_check(
    engine: &Engine<StepClock, MemDisk>,
    model: &RefModel,
    commits: &[SystemTimeMicros],
    history: &[HistoryOp],
    bug: Bug,
) -> Result<u64, Box<Divergence>> {
    let keys: Vec<BusinessKey> = model.timelines.keys().cloned().collect();
    let mut digest = FNV_OFFSET;
    for s in probes(commits) {
        for key in &keys {
            let engine_v = engine.as_of(key, Snapshot(s)).expect("engine as_of");
            let engine_ans = engine_v.as_ref().map(Answer::from_engine);
            let reference_ans = model.as_of(key, s, bug).map(Answer::from_ref);

            if engine_ans != reference_ans {
                // Minimal reproducer: only this key's operations at or before the
                // probe — the sole inputs to its `AS OF` answer at `s`.
                let repro: Vec<HistoryOp> = history
                    .iter()
                    .filter(|op| op.key == *key && op.commit.0 <= s.0)
                    .cloned()
                    .collect();
                return Err(Box::new(Divergence {
                    snapshot: s,
                    key: key.clone(),
                    engine: engine_ans,
                    reference: reference_ans,
                    history: repro,
                }));
            }

            digest = fnv1a(digest, key.as_bytes());
            digest = fnv1a(digest, &s.0.to_le_bytes());
            match &engine_ans {
                Some(a) => {
                    digest = fnv1a(digest, &[1]);
                    digest = fnv1a(digest, &a.sys_from.0.to_le_bytes());
                    digest = fnv1a(digest, &a.sys_to.0.to_le_bytes());
                    digest = fnv1a(digest, &a.payload);
                }
                None => digest = fnv1a(digest, &[0]),
            }
        }
    }
    Ok(digest)
}

/// Check a seeded random history's every `AS OF (sys)` answer against the
/// in-memory reference model, returning a digest of the agreed answers.
///
/// Generates a random insert/update/delete history, applies it to a real
/// [`Engine`] and to the independent list-of-versions-per-key reference, then
/// sweeps `AS OF` probes — including exactly on every commit boundary — and
/// asserts the engine's resolved `(sys_from, sys_to, payload)` matches the
/// reference at every probe and key
/// ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart),
/// STL-111). Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics with the minimal reproducing history and the diverging snapshot if any
/// `AS OF` answer disagrees with the reference — a correctness regression, not a
/// workload outcome.
#[must_use]
pub fn run_as_of_oracle_seed(seed: u64) -> u64 {
    let (engine, model, history, commits) = build_history(seed);
    match differential_check(&engine, &model, &commits, &history, Bug::None) {
        Ok(digest) => digest,
        Err(divergence) => panic!("seed {seed}: {divergence}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_of_oracle_seed_is_reproducible() {
        // Each seed asserts (internally) that every engine AS OF answer matches
        // the in-memory reference at every probe and key (STL-111 DoD).
        for seed in 0..128 {
            assert_eq!(
                run_as_of_oracle_seed(seed),
                run_as_of_oracle_seed(seed),
                "seed {seed} must replay to an identical AS-OF-oracle digest"
            );
        }
    }

    #[test]
    fn as_of_oracle_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..128).map(run_as_of_oracle_seed).collect();
        assert!(
            digests.len() > 1,
            "the AS-OF-oracle workload must actually depend on the seed"
        );
    }

    /// The mutation test (STL-111 DoD): a *documented intentional bug* in the
    /// reference's `AS OF` rule — [`Bug::InclusiveUpper`], a closed-interval
    /// off-by-one — must be **caught** by the very same differential check that
    /// passes with the correct rule. An oracle that cannot fail a wrong
    /// implementation guards nothing; this proves it has teeth.
    #[test]
    fn oracle_catches_intentional_off_by_one() {
        // A minimal history with one supersede: insert then update the same key.
        // At the exact update instant the half-open engine returns the new value;
        // the closed-interval bug clings to the old one.
        let mut engine =
            Engine::open(MemDisk::new(), StepClock::new(1_000), false).expect("open engine");
        let who = Principal::new(b"sim".to_vec());
        let key = BusinessKey::new(b"k-0000".to_vec());

        let t1 = engine
            .insert(
                key.clone(),
                None,
                Some(b"100".to_vec()),
                0,
                TxnId(1),
                who.clone(),
            )
            .expect("insert")
            .commit;
        let t2 = engine
            .update(key.clone(), None, Some(b"250".to_vec()), 0, TxnId(2), who)
            .expect("update")
            .commit;
        assert!(t2 > t1, "the update must commit after the insert");

        let mut model = RefModel::default();
        model.open(key.clone(), t1, b"100".to_vec());
        model.close_current(&key, t2);
        model.open(key.clone(), t2, b"250".to_vec());

        let history = vec![
            HistoryOp {
                key: key.clone(),
                kind: OpKind::Insert(b"100".to_vec()),
                commit: t1,
            },
            HistoryOp {
                key: key.clone(),
                kind: OpKind::Update(b"250".to_vec()),
                commit: t2,
            },
        ];
        let commits = vec![t1, t2];

        // The correct rule agrees with the engine everywhere — the harness itself
        // is sound.
        differential_check(&engine, &model, &commits, &history, Bug::None)
            .expect("the correct oracle must agree with the engine");

        // The intentional bug is caught, and it is caught *exactly* at the
        // supersede instant, on the right key, with both answers recorded.
        let divergence =
            differential_check(&engine, &model, &commits, &history, Bug::InclusiveUpper)
                .expect_err("the oracle must catch the intentional off-by-one");
        assert_eq!(
            divergence.snapshot, t2,
            "divergence must surface at the supersede instant"
        );
        assert_eq!(divergence.key, key);
        assert_eq!(
            divergence.engine.as_ref().map(|a| a.payload.clone()),
            Some(b"250".to_vec()),
            "engine returns the new (post-supersede) value at t2"
        );
        assert_eq!(
            divergence.reference.as_ref().map(|a| a.payload.clone()),
            Some(b"100".to_vec()),
            "the buggy reference clings to the old value at t2"
        );

        // The dumped history is the minimal reproducer the failure mode promises.
        let report = divergence.to_string();
        assert!(report.contains("minimal reproducing history (2 ops)"));
        assert!(report.contains("INSERT"));
        assert!(report.contains("UPDATE"));
    }
}
