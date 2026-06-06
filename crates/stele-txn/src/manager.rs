//! The transaction manager — snapshot acquisition, commit-time assignment, and
//! write-write conflict detection.
//!
//! [`TxnManager`] is the global authority the storage core defers to for the two
//! things it cannot decide on its own: *which* system-time point a reader sees,
//! and *which* system-time point a writer's versions are stamped with
//! ([architecture §9](../../../docs/02-architecture.md#9-transaction--concurrency-model),
//! [ADR-0008](../../../docs/adr/0008-mvcc-on-append-only.md)). The single-writer
//! [`SysTimeWriter`](stele_storage::systime) keeps one chain non-overlapping; the
//! manager is what makes commit ordering coherent *across* transactions.
//!
//! ## Snapshot isolation, via one monotonic cursor
//!
//! The manager holds a single monotonic system-time cursor. Both ends of the
//! transaction lifecycle advance it through the injectable [`Clock`], and that
//! shared cursor is the whole correctness argument:
//!
//! * [`begin`](TxnManager::begin) hands the transaction a **read snapshot**
//!   `s = max(clock.now(), cursor)` and advances the cursor to `s`.
//! * [`commit`](TxnManager::commit) assigns a **commit timestamp**
//!   `c = max(clock.now(), cursor + 1)` and advances the cursor to `c`.
//!
//! Because `begin` first lifts the cursor to `s`, any commit that follows is
//! drawn from `cursor + 1 ≥ s + 1`, so `c > s` is guaranteed for every snapshot
//! still outstanding when the commit lands. That is exactly the property the
//! definition of done turns on: *a reader at snapshot `s` keeps seeing the
//! version with `sys_from ≤ s < sys_to` even while a concurrent writer commits at
//! `c > s`*. The reader's [`Snapshot`] feeds straight into
//! [`Delta::range_scan`](stele_storage::delta::Delta::range_scan), whose resolver
//! already picks the greatest `sys_from ≤ s` whose `sys_to > s`.
//!
//! Holding the cursor strictly increasing across a stalled or regressing wall
//! clock mirrors the storage layer's own commit-timestamp guard, and refusing a
//! timestamp at the `+∞` open sentinel ([`SYSTEM_TIME_OPEN`]) keeps a real
//! `sys_from` from ever masquerading as an open period.
//!
//! ## Conflict detection — first committer wins
//!
//! Snapshot isolation lets readers and writers run without blocking, but two
//! transactions that began at the same snapshot must not both write the same key
//! ([ADR-0008](../../../docs/adr/0008-mvcc-on-append-only.md): *"write-write
//! conflicts on overlapping snapshots are detected and the loser retries"*). The
//! manager records, per business key, the commit timestamp of its latest writer.
//! At commit it checks every key in the transaction's write set: if one was
//! written by a commit *after* this transaction's snapshot, the two overlapped
//! and this transaction is the loser — [`commit`](TxnManager::commit) returns
//! [`TxnError::Conflict`], the clean retry signal. The first to commit wins; the
//! loser re-runs against a fresh snapshot.
//!
//! ## The commit log — sequence numbers and a hash chain
//!
//! Every accepted commit appends one [`CommitRecord`] carrying two facts beyond
//! the transaction id and timestamp: a per-commit monotonic `seq`
//! ([ADR-0024](../../../docs/adr/0024-time-representation.md)) that totally
//! orders commits independent of the µs `commit_ts` — the tiebreak for same-tick
//! writes — and the [SHA-256](stele_common::hash) of the prior record, so the log
//! is a hash chain anchored at [`Digest::ZERO`]
//! ([ADR-0026](../../../docs/adr/0026-verifiable-audit-log.md)). The manager
//! holds the running head ([`commit_head`](TxnManager::commit_head)); a
//! [`verify_chain`](crate::chain::verify_chain) pass over the WAL detects any
//! tampered historical record. `seq` and the head advance only on a *durable*
//! commit, so they count committed transactions, never aborted or read-only ones.
//!
//! ## Scope at v0.1
//!
//! A v0.1 transaction is **single-statement** ([STL-99] scope): begin, declare
//! the write, commit. Multi-statement transactions, read-committed, and
//! serializable (SSI) are later opt-ins ([ADR-0008]). The conflict index and the
//! commit cursor live in memory for the manager's lifetime; recovering them from
//! the WAL on restart arrives with the multi-statement work in v0.2.
//!
//! ```
//! # use stele_txn::{TxnManager, TxnError};
//! # use stele_storage::backend::MemDisk;
//! # use stele_storage::delta::BusinessKey;
//! # use stele_storage::wal::{Wal, WalConfig};
//! # use stele_common::time::SystemClock;
//! let wal = Wal::open(MemDisk::new(), WalConfig::default()).unwrap();
//! let mgr = TxnManager::new(SystemClock, wal);
//!
//! // Two transactions begin at the same snapshot and both target one key.
//! let mut a = mgr.begin();
//! let mut b = mgr.begin();
//! let key = BusinessKey::new(b"account-1".to_vec());
//! a.write(key.clone());
//! b.write(key);
//!
//! // First committer wins; the loser gets a clean retry signal. `commit`
//! // consumes the transaction, so capture the snapshot before committing.
//! let snapshot_a = a.snapshot();
//! let committed = mgr.commit(a).unwrap();
//! assert!(committed.commit_ts.0 > snapshot_a.0.0);
//! assert!(matches!(mgr.commit(b), Err(TxnError::Conflict)));
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use stele_common::hash::Digest;
use stele_common::provenance::TxnId;
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::delta::{BusinessKey, Snapshot};
use stele_storage::wal::{Disk, Wal, WalError};

use crate::commit_record::CommitRecord;

/// Errors surfaced from the transaction lifecycle.
#[derive(Debug, thiserror::Error)]
pub enum TxnError {
    /// A write-write conflict: another transaction committed a write to one of
    /// this transaction's keys after this transaction's snapshot. The loser
    /// retries against a fresh snapshot — this is the clean retry signal the
    /// definition of done calls for, not a corrupt state.
    #[error("transaction conflict: a concurrent commit wrote an overlapping key; retry")]
    Conflict,

    /// The system-time domain is exhausted: the next commit timestamp would
    /// reach the `+∞` open sentinel ([`SYSTEM_TIME_OPEN`]), where a real
    /// `sys_from` would be indistinguishable from an open period. Mirrors the
    /// storage writer's guard; practically unreachable.
    #[error("system-time domain exhausted: next commit would reach the +∞ sentinel")]
    TimeExhausted,

    /// The commit record could not be appended to or fsynced on the WAL. The
    /// in-memory state (cursor, conflict index) is left untouched either way, but
    /// the durability outcome differs by sub-case:
    ///
    /// * **Append failed:** nothing was staged — the transaction is cleanly
    ///   retryable.
    /// * **Append succeeded, fsync (`tick`) failed:** the record may still become
    ///   durable on a later fsync or segment rotation, so this transaction's
    ///   durability is *indeterminate* until recovery resolves it from the log.
    ///   Recovery is the source of truth here, not this return value. Hardening an
    ///   fsync failure into a terminal manager state is v0.2 recovery work.
    #[error(transparent)]
    Wal(#[from] WalError),
}

/// A handle on an in-flight transaction: its identity, its read snapshot, and
/// the set of keys it intends to write.
///
/// Obtained from [`TxnManager::begin`]. Reads resolve at [`snapshot`](Self::snapshot);
/// each intended write is declared with [`write`](Self::write) so the manager can
/// detect conflicts at commit. In the v0.1 single-statement model a transaction
/// declares its one write and commits.
#[derive(Debug, Clone)]
pub struct Transaction {
    txn_id: TxnId,
    snapshot: SystemTimeMicros,
    writes: BTreeSet<BusinessKey>,
}

impl Transaction {
    /// This transaction's identifier — monotonic, assigned at [`begin`](TxnManager::begin),
    /// and stamped inline on every version it writes
    /// ([provenance invariant 5](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
    #[must_use]
    pub const fn id(&self) -> TxnId {
        self.txn_id
    }

    /// The read snapshot to resolve reads at — pass it straight to
    /// [`Delta::range_scan`](stele_storage::delta::Delta::range_scan). Per key it
    /// selects the version whose `[sys_from, sys_to)` contains the snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> Snapshot {
        Snapshot(self.snapshot)
    }

    /// Declare that this transaction writes `key`. Recorded in the write set the
    /// manager checks for conflicts at [`commit`](TxnManager::commit). Declaring
    /// the same key twice is idempotent.
    pub fn write(&mut self, key: BusinessKey) {
        self.writes.insert(key);
    }
}

/// The outcome of an accepted commit: the transaction and the system-time
/// coordinate it committed at. The commit timestamp is the `sys_from` the
/// transaction's written versions carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    /// The committing transaction.
    pub txn_id: TxnId,
    /// The assigned commit timestamp — strictly greater than the snapshot of any
    /// transaction still outstanding when this commit landed.
    pub commit_ts: SystemTimeMicros,
    /// The per-commit monotonic sequence number ([ADR-0024]). It totally orders
    /// commits independent of `commit_ts`, so two commits in the same µs tick are
    /// still deterministically ordered — and is the tiebreak the version record
    /// will carry ([STL-141]). Distinct from `txn_id`: only *committed*
    /// transactions consume a `seq`.
    ///
    /// [ADR-0024]: ../../../docs/adr/0024-time-representation.md
    /// [STL-141]: https://allegromusic.atlassian.net/browse/STL-141
    pub seq: u64,
}

/// State guarded by the manager's mutex. Kept tiny on purpose: a monotonic
/// system-time cursor, the next transaction id, and the per-key conflict index.
#[derive(Debug)]
struct State {
    /// The highest system-time coordinate handed out (as a snapshot) or assigned
    /// (as a commit). Both `begin` and `commit` advance it; that single shared
    /// cursor is what keeps snapshots and commits globally ordered.
    cursor: SystemTimeMicros,
    /// The next transaction id to allocate — monotonic.
    next_txn: u64,
    /// The next per-commit sequence number to allocate ([ADR-0024]). Bumped only
    /// on a successful commit, so it counts commits, not begins — the total-order
    /// tiebreak for same-µs writes.
    ///
    /// [ADR-0024]: ../../../docs/adr/0024-time-representation.md
    next_seq: u64,
    /// The running head of the hash-chained commit log ([ADR-0026]): the hash of
    /// the most recently committed record, carried as the next record's
    /// `prev_hash`. [`Digest::ZERO`] before the first commit (genesis).
    ///
    /// [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
    commit_head: Digest,
    /// Per business key, the commit timestamp of its most recent committer. A
    /// committing transaction conflicts iff one of its keys appears here with a
    /// timestamp newer than its snapshot.
    write_index: BTreeMap<BusinessKey, SystemTimeMicros>,
}

/// The transaction manager — hands out snapshots and transaction ids, assigns
/// commit timestamps, detects write-write conflicts, and durably logs each
/// commit.
///
/// One manager owns the commit ordering for the rows it stamps. It is `Send +
/// Sync` (its state sits behind a [`Mutex`], its WAL behind the WAL's own lock),
/// so a shared `&TxnManager` drives concurrent transactions — see the
/// [module docs](self) for the snapshot-isolation argument.
pub struct TxnManager<C: Clock, D: Disk> {
    clock: C,
    wal: Wal<D>,
    state: Mutex<State>,
}

// `Wal` is not `Debug` (it guards a `Disk` handle behind a mutex) and the clock
// `C` need not be either, so derive is out; surface the commit-ordering state and
// elide the rest — mirroring [`stele_storage::dml::DmlWriter`].
impl<C: Clock, D: Disk> std::fmt::Debug for TxnManager<C, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnManager")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<C: Clock, D: Disk> TxnManager<C, D> {
    /// Build a manager drawing timestamps from `clock` and durably logging
    /// commits to `wal`. The commit cursor starts below every real timestamp, so
    /// the first transaction's snapshot is the clock's reading.
    pub const fn new(clock: C, wal: Wal<D>) -> Self {
        Self {
            clock,
            wal,
            state: Mutex::new(State {
                // The Unix-epoch floor: any real `clock.now()` lifts the cursor to
                // itself on the first `begin`, so no real version ever sits at or
                // below the starting cursor.
                cursor: SystemTimeMicros(0),
                next_txn: 1,
                // First commit is seq 1; the chain starts at the genesis link.
                next_seq: 1,
                commit_head: Digest::ZERO,
                write_index: BTreeMap::new(),
            }),
        }
    }

    /// Begin a transaction: allocate its id and read snapshot.
    ///
    /// The snapshot is `max(clock.now(), cursor)`, and the cursor is lifted to it
    /// so every later commit is strictly greater — the guarantee a concurrent
    /// writer cannot perturb this snapshot. See the [module docs](self).
    // The snapshot read and the cursor/id bump must be one atomic step under the
    // lock, so two concurrent `begin`s cannot observe the same cursor and hand out
    // a colliding snapshot/id pair. The guard naturally covers the whole body.
    #[allow(clippy::significant_drop_tightening)]
    pub fn begin(&self) -> Transaction {
        let mut st = self.state.lock().expect("txn manager mutex poisoned");
        // A snapshot must stay strictly below the +∞ open sentinel: a reader at
        // the sentinel resolves no open version (the delta resolver's `sys_to > s`
        // test fails at exactly `s == SYSTEM_TIME_OPEN`). A saturating clock
        // reading (`SystemClock::now()` clamps to `i64::MAX`) is therefore pulled
        // back below it — mirroring the storage writer's all-builds sentinel guard.
        let snapshot = self
            .clock
            .now()
            .max(st.cursor)
            .min(SystemTimeMicros(SYSTEM_TIME_OPEN.0 - 1));
        st.cursor = snapshot;
        let txn_id = TxnId(st.next_txn);
        st.next_txn += 1;
        Transaction {
            txn_id,
            snapshot,
            writes: BTreeSet::new(),
        }
    }

    /// Commit `txn`: detect conflicts, assign the commit timestamp, durably log
    /// the commit record, and publish the transaction's writes.
    ///
    /// **Consumes the transaction** — `commit` takes it by value, so a given
    /// transaction (and its `txn_id`) can be committed at most once. This enforces
    /// the "one txn id → one commit" contract at the type level: a transaction
    /// cannot be re-committed or have its write set mutated after committing, and
    /// a conflicted transaction must re-`begin` against a fresh snapshot to retry
    /// (the correct snapshot-isolation retry).
    ///
    /// ```compile_fail
    /// # use stele_txn::TxnManager;
    /// # use stele_storage::backend::MemDisk;
    /// # use stele_storage::wal::{Wal, WalConfig};
    /// # use stele_common::time::SystemClock;
    /// let wal = Wal::open(MemDisk::new(), WalConfig::default()).unwrap();
    /// let mgr = TxnManager::new(SystemClock, wal);
    /// let txn = mgr.begin();
    /// let _ = mgr.commit(txn);
    /// let _ = mgr.commit(txn); // error[E0382]: use of moved value `txn`
    /// ```
    ///
    /// On success the commit timestamp is the `sys_from` the transaction's
    /// versions carry, and every key it wrote is recorded for future conflict
    /// checks. Nothing is published until the commit record is fsynced — the WAL
    /// fsync is the only durability point (invariant 2).
    ///
    /// # Errors
    ///
    /// * [`TxnError::Conflict`] if a concurrent transaction already committed a
    ///   write to one of `txn`'s keys after `txn`'s snapshot — the clean retry
    ///   signal. No state is mutated.
    /// * [`TxnError::TimeExhausted`] if the next timestamp would reach the `+∞`
    ///   sentinel.
    /// * [`TxnError::Wal`] if the commit record cannot be logged or fsynced — see
    ///   the variant docs for the append-failed (cleanly retryable) versus
    ///   fsync-failed (durability indeterminate) distinction.
    //
    // The state lock is intentionally held across the WAL append+fsync: it is what
    // makes the conflict check, the timestamp assignment, the durable log write,
    // and the publish one atomic step, so the WAL's record order matches the
    // commit-timestamp order. Tightening the guard would break that — group-commit
    // batching across transactions is a separate v0.2 concern.
    #[allow(clippy::significant_drop_tightening)]
    pub fn commit(&self, txn: Transaction) -> Result<Committed, TxnError> {
        let mut st = self.state.lock().expect("txn manager mutex poisoned");

        // First committer wins: a key written by a commit newer than our snapshot
        // means we overlapped a concurrent writer and lost the race.
        for key in &txn.writes {
            if st
                .write_index
                .get(key)
                .is_some_and(|&written_at| written_at > txn.snapshot)
            {
                return Err(TxnError::Conflict);
            }
        }

        // Assign the commit timestamp: at least the clock, strictly above the
        // cursor (so it beats every outstanding snapshot), and below the sentinel.
        // `saturating_add` mirrors `SysTimeWriter` — the sentinel check below
        // rejects the saturated value rather than letting `+ 1` overflow.
        let commit_ts = self
            .clock
            .now()
            .max(SystemTimeMicros(st.cursor.0.saturating_add(1)));
        if commit_ts >= SYSTEM_TIME_OPEN {
            return Err(TxnError::TimeExhausted);
        }

        // Allocate this commit's sequence number and chain its record to the
        // prior one: `prev_hash` is the running head, so the durable log is a
        // hash chain anchored at genesis ([ADR-0026], [ADR-0024]).
        let seq = st.next_seq;
        let record = CommitRecord {
            txn_id: txn.txn_id,
            commit_ts,
            seq,
            prev_hash: st.commit_head,
        };

        // Durability first: append + fsync the commit record before any in-memory
        // state moves, so a WAL failure leaves the manager exactly as it was. An
        // append failure is cleanly retryable; an append that succeeds but whose
        // fsync (`tick`) fails leaves durability *indeterminate* until recovery —
        // the append/fsync distinction the `TxnError::Wal` variant docs spell out.
        self.wal.append(&record.encode())?;
        self.wal.tick()?;

        // Now publish: advance the cursor, the sequence counter, and the chain
        // head, and record this transaction's writes as the newest committers of
        // their keys. The write set is moved out of the (consumed) transaction —
        // no clone needed. `next_seq`/`commit_head` advance only here, so they
        // count durable commits, never aborted or read-only transactions.
        let txn_id = txn.txn_id;
        st.cursor = commit_ts;
        st.next_seq += 1;
        st.commit_head = record.hash();
        for key in txn.writes {
            st.write_index.insert(key, commit_ts);
        }
        Ok(Committed {
            txn_id,
            commit_ts,
            seq,
        })
    }

    /// The current commit cursor — the highest snapshot or commit timestamp the
    /// manager has issued. Exposed for observability and tests.
    #[must_use]
    pub fn cursor(&self) -> SystemTimeMicros {
        self.state
            .lock()
            .expect("txn manager mutex poisoned")
            .cursor
    }

    /// The current head of the hash-chained commit log ([ADR-0026]) — the hash
    /// of the most recently committed record, or [`Digest::ZERO`] if nothing has
    /// committed yet.
    ///
    /// This is the anchor a tamper check verifies against: durably remember it
    /// (a checkpoint/witness — the seed of the Merkle proofs in ~v0.5) and pass
    /// it to [`verify_chain_to`](crate::chain::verify_chain_to) to detect even a
    /// wholesale rewrite of the log, which the bare chain walk cannot catch.
    ///
    /// **Only a reliable witness if no [`TxnError::Wal`] has occurred since
    /// startup.** `commit_head` advances only after a commit's record is appended
    /// *and* fsynced, so it tracks the durable log on the happy path. But the
    /// append-succeeded/fsync-failed case ([`TxnError::Wal`]) leaves a staged
    /// record that may still reach the WAL on a later fsync or rotation — so the
    /// durable log can gain a record the in-memory head does not yet reflect, and
    /// an anchor persisted across that window may later fail to verify even
    /// though nothing was tampered with. After any WAL error the manager must be
    /// treated as requiring recovery (a v0.2 concern) before `commit_head` is
    /// used as an external witness again.
    ///
    /// [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
    #[must_use]
    pub fn commit_head(&self) -> Digest {
        self.state
            .lock()
            .expect("txn manager mutex poisoned")
            .commit_head
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    use stele_common::provenance::{Principal, Provenance};
    use stele_storage::backend::MemDisk;
    use stele_storage::delta::{Delta, DeltaConfig, Version};
    use stele_storage::validity::{Close, ValidityConfig, ValidityIndex};
    use stele_storage::wal::{Checkpoint, Wal, WalConfig};

    fn new_index() -> ValidityIndex<MemDisk> {
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open index")
    }

    /// A clock pinned to a fixed reading — a stalled wall clock — to prove the
    /// monotonic cursor advances commits past snapshots on its own, not the
    /// clock. Atomic so it satisfies `Clock: Send + Sync` without `unsafe`.
    struct StubClock(AtomicI64);
    impl StubClock {
        const fn new(start: i64) -> Self {
            Self(AtomicI64::new(start))
        }
    }
    impl Clock for StubClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(self.0.load(Ordering::Relaxed))
        }
    }

    fn manager(clock: StubClock) -> TxnManager<StubClock, MemDisk> {
        let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
        TxnManager::new(clock, wal)
    }

    /// Build the open version a committed insert stages: `[commit, +∞)`.
    fn open_version(key: &BusinessKey, txn_id: TxnId, commit: SystemTimeMicros) -> Version {
        Version::open(
            key.clone(),
            commit,
            0,
            Provenance::new(txn_id, commit, Principal::new(b"tester".to_vec())),
            format!("v@{}", commit.0).into_bytes(),
        )
    }

    /// Read back the single payload live for `key` at `snapshot`, if any —
    /// resolving each version's end from the validity `index` ([ADR-0023]).
    fn read(
        delta: &Delta<MemDisk>,
        index: &ValidityIndex<MemDisk>,
        key: &BusinessKey,
        snapshot: Snapshot,
    ) -> Option<Vec<u8>> {
        delta
            .range_scan(key.clone()..=key.clone(), snapshot, index)
            .expect("range scan")
            .into_iter()
            .next()
            .map(|v| v.payload)
    }

    /// The DoD's headline guarantee: a reader at snapshot `s` keeps seeing the
    /// version with `sys_from ≤ s < sys_to` even as a concurrent writer commits a
    /// superseding version at `c > s`.
    #[test]
    fn reader_snapshot_is_stable_under_a_concurrent_commit() {
        let mgr = manager(StubClock::new(1_000));
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
        let mut index = new_index();
        let key = BusinessKey::new(b"k".to_vec());

        // Writer A inserts v1 and commits.
        let mut a = mgr.begin();
        a.write(key.clone());
        let a_done = mgr.commit(a).expect("commit a");
        let c1 = a_done.commit_ts;
        delta
            .insert(open_version(&key, a_done.txn_id, c1))
            .expect("stage v1");

        // Reader R takes a snapshot that sees v1.
        let r = mgr.begin();
        let s = r.snapshot();
        assert_eq!(
            read(&delta, &index, &key, s),
            Some(format!("v@{}", c1.0).into_bytes())
        );

        // Writer B supersedes K: commit at c2 > s, closing v1 (a write-once entry
        // in the validity index) and opening v2.
        let mut b = mgr.begin();
        b.write(key.clone());
        let b_done = mgr.commit(b).expect("commit b");
        let c2 = b_done.commit_ts;
        assert!(
            c2.0 > s.0.0,
            "the concurrent commit must land strictly after R's snapshot"
        );
        index
            .insert_close(Close {
                business_key: key.clone(),
                sys_from: c1,
                sys_to: c2,
                closed_by: Provenance::new(b_done.txn_id, c2, Principal::new(b"tester".to_vec())),
            })
            .expect("close v1");
        delta
            .insert(open_version(&key, b_done.txn_id, c2))
            .expect("stage v2");

        // R, still at its snapshot, must NOT observe v2.
        assert_eq!(
            read(&delta, &index, &key, s),
            Some(format!("v@{}", c1.0).into_bytes())
        );
        // A fresh reader, snapshotting after c2, sees v2.
        let r2 = mgr.begin();
        assert_eq!(
            read(&delta, &index, &key, r2.snapshot()),
            Some(format!("v@{}", c2.0).into_bytes())
        );
    }

    /// The DoD's conflict guarantee: two writers race on one key; exactly one
    /// wins; the loser gets a clean [`TxnError::Conflict`].
    #[test]
    fn two_writers_race_exactly_one_wins() {
        let mgr = manager(StubClock::new(1));
        let key = BusinessKey::new(b"contended".to_vec());

        let mut a = mgr.begin();
        let mut b = mgr.begin();
        a.write(key.clone());
        b.write(key);

        // Whoever commits first wins; the second to commit loses cleanly.
        assert!(mgr.commit(a).is_ok());
        assert!(matches!(mgr.commit(b), Err(TxnError::Conflict)));
    }

    /// Conflict resolution is symmetric: the winner is the first committer, not a
    /// fixed transaction. Same setup, reversed commit order.
    #[test]
    fn the_winner_is_whoever_commits_first() {
        let mgr = manager(StubClock::new(1));
        let key = BusinessKey::new(b"contended".to_vec());

        let mut a = mgr.begin();
        let mut b = mgr.begin();
        a.write(key.clone());
        b.write(key);

        assert!(mgr.commit(b).is_ok());
        assert!(matches!(mgr.commit(a), Err(TxnError::Conflict)));
    }

    /// A transaction that begins *after* another commits sees that write in its
    /// snapshot and supersedes it cleanly — no conflict, because the snapshots do
    /// not overlap.
    #[test]
    fn sequential_writers_do_not_conflict() {
        let mgr = manager(StubClock::new(1));
        let key = BusinessKey::new(b"k".to_vec());

        let mut a = mgr.begin();
        a.write(key.clone());
        mgr.commit(a).expect("commit a");

        // b begins after a's commit: its snapshot already includes a's write.
        let mut b = mgr.begin();
        b.write(key);
        assert!(mgr.commit(b).is_ok());
    }

    /// Writers touching disjoint keys never conflict, even at the same snapshot.
    #[test]
    fn disjoint_keys_never_conflict() {
        let mgr = manager(StubClock::new(1));
        let mut a = mgr.begin();
        let mut b = mgr.begin();
        a.write(BusinessKey::new(b"key-a".to_vec()));
        b.write(BusinessKey::new(b"key-b".to_vec()));
        assert!(mgr.commit(a).is_ok());
        assert!(mgr.commit(b).is_ok());
    }

    /// Every commit timestamp is strictly greater than the snapshot of any
    /// transaction outstanding when it lands — even while the wall clock is
    /// stalled. This is the invariant the snapshot-stability guarantee rests on:
    /// the monotonic cursor, not the clock, is what orders commits past snapshots.
    #[test]
    fn commit_timestamp_beats_every_outstanding_snapshot() {
        // The clock never moves — only the cursor advances commits.
        let mgr = manager(StubClock::new(100));

        let r = mgr.begin(); // snapshot 100, cursor -> 100
        let s = r.snapshot().0;

        let mut w1 = mgr.begin(); // snapshot still 100, cursor stays 100
        w1.write(BusinessKey::new(b"k1".to_vec()));
        let c1 = mgr.commit(w1).expect("commit w1").commit_ts;
        assert!(
            c1 > s,
            "stalled-clock commit must exceed an outstanding snapshot"
        );

        let mut w2 = mgr.begin();
        w2.write(BusinessKey::new(b"k2".to_vec()));
        let c2 = mgr.commit(w2).expect("commit w2").commit_ts;
        assert!(
            c2 > c1,
            "commit timestamps strictly increase even with a frozen clock"
        );
    }

    /// The commit record reaches the WAL durably: re-opening the log over the
    /// same backing store and replaying it yields exactly the committed
    /// transaction's id and timestamp. The WAL fsync is the only durability point
    /// (invariant 2), so a commit that returned `Ok` is recoverable from the log.
    #[test]
    fn commit_record_is_durable_on_the_wal() {
        let disk = MemDisk::new();
        let wal = Wal::open(disk.clone(), WalConfig::default()).expect("open wal");
        let mgr = TxnManager::new(StubClock::new(5), wal);

        let mut a = mgr.begin();
        a.write(BusinessKey::new(b"k".to_vec()));
        let committed = mgr.commit(a).expect("commit");

        // Re-open over the same store and replay — the record is on disk.
        let reopened = Wal::open(disk, WalConfig::default()).expect("reopen wal");
        let records: Vec<Vec<u8>> = reopened
            .replay_from(Checkpoint::BEGIN)
            .map(|r| r.expect("replay record"))
            .collect();
        assert_eq!(records.len(), 1, "exactly one commit record was logged");
        let decoded = CommitRecord::decode(&records[0]).expect("decode commit record");
        assert_eq!(decoded.txn_id, committed.txn_id);
        assert_eq!(decoded.commit_ts, committed.commit_ts);
        assert_eq!(decoded.seq, committed.seq);
        // The first record chains from genesis; the manager's head is its hash.
        assert_eq!(decoded.prev_hash, Digest::ZERO);
        assert_eq!(mgr.commit_head(), decoded.hash());
    }

    /// `seq` is a per-commit counter, dense and monotonic, and — unlike the
    /// force-bumped `commit_ts` — it does not ride on the clock. Even with a
    /// frozen clock the seq strictly increases, giving a total order independent
    /// of the µs timestamp ([ADR-0024]). Only *committed* transactions consume a
    /// seq: a begin that never commits does not.
    #[test]
    fn seq_is_dense_monotonic_over_commits_only() {
        let mgr = manager(StubClock::new(100));

        // A begin that never commits burns a txn_id but no seq.
        let _abandoned = mgr.begin();

        let mut a = mgr.begin();
        a.write(BusinessKey::new(b"k1".to_vec()));
        let ca = mgr.commit(a).expect("commit a");

        let mut b = mgr.begin();
        b.write(BusinessKey::new(b"k2".to_vec()));
        let cb = mgr.commit(b).expect("commit b");

        // First two commits get seq 1 and 2 regardless of the abandoned begin.
        assert_eq!(ca.seq, 1);
        assert_eq!(cb.seq, 2);
        assert!(cb.seq > ca.seq, "seq strictly increases across commits");
    }

    /// The DoD's reproducibility guarantee: replaying the same sequence of
    /// operations yields byte-identical commit logs — identical `seq` assignment
    /// and an identical hash-chain head — across independent runs.
    #[test]
    fn the_commit_log_is_deterministic_across_runs() {
        fn run() -> (Vec<u64>, Digest, Vec<Vec<u8>>) {
            let disk = MemDisk::new();
            let wal = Wal::open(disk.clone(), WalConfig::default()).expect("open wal");
            let mgr = TxnManager::new(StubClock::new(500), wal);
            let mut seqs = Vec::new();
            for i in 0..6 {
                let mut t = mgr.begin();
                t.write(BusinessKey::new(format!("key-{}", i % 3).into_bytes()));
                // Conflicts are possible across the same key; tolerate the clean
                // retry signal deterministically by re-begin on the next key.
                if let Ok(c) = mgr.commit(t) {
                    seqs.push(c.seq);
                }
            }
            let frames: Vec<Vec<u8>> = Wal::open(disk, WalConfig::default())
                .expect("reopen")
                .replay_from(Checkpoint::BEGIN)
                .map(|r| r.expect("record"))
                .collect();
            (seqs, mgr.commit_head(), frames)
        }

        let (seqs1, head1, frames1) = run();
        let (seqs2, head2, frames2) = run();
        assert_eq!(seqs1, seqs2, "seq assignment is deterministic");
        assert_eq!(head1, head2, "the chain head is deterministic");
        assert_eq!(frames1, frames2, "the commit log is byte-identical");
    }

    /// The DoD's tamper guarantee: a clean commit log verifies, and altering any
    /// historical WAL record is caught by the chain-verify pass. Exercised over
    /// the *real* WAL the manager wrote, not hand-built frames.
    #[test]
    fn clean_log_verifies_and_tampering_is_detected() {
        use crate::chain::{ChainError, verify_chain, verify_chain_to};

        let disk = MemDisk::new();
        let wal = Wal::open(disk.clone(), WalConfig::default()).expect("open wal");
        let mgr = TxnManager::new(StubClock::new(10), wal);
        for i in 0..4 {
            let mut t = mgr.begin();
            t.write(BusinessKey::new(format!("k{i}").into_bytes()));
            mgr.commit(t).expect("commit");
        }
        let head = mgr.commit_head();

        // A clean log verifies, and its head matches the manager's anchor.
        let reopen = || Wal::open(disk.clone(), WalConfig::default()).expect("reopen wal");
        let verified = verify_chain(reopen().replay_from(Checkpoint::BEGIN)).expect("clean verify");
        assert_eq!(verified.len, 4);
        assert_eq!(verified.head, head);
        verify_chain_to(reopen().replay_from(Checkpoint::BEGIN), head).expect("anchored verify");

        // Replay the manager-written frames, forge record 1, and verify the
        // tampered sequence — the chain-verify pass is pure over the frames, so
        // there's no need to write the forgery back to disk.
        let frames: Vec<Vec<u8>> = reopen()
            .replay_from(Checkpoint::BEGIN)
            .map(|r| r.unwrap())
            .collect();
        let mut forged = CommitRecord::decode(&frames[1]).unwrap();
        forged.commit_ts = SystemTimeMicros(forged.commit_ts.0 + 1); // rewrite history
        let tampered: Vec<Result<Vec<u8>, _>> = frames
            .iter()
            .enumerate()
            .map(|(i, f)| {
                Ok(if i == 1 {
                    forged.encode().to_vec()
                } else {
                    f.clone()
                })
            })
            .collect();
        let err = verify_chain(tampered).expect_err("tampered log must fail");
        match err {
            ChainError::BrokenLink { index, .. } => assert_eq!(index, 2),
            other => panic!("expected a broken link, got {other:?}"),
        }
    }

    /// `begin` hands out monotonically increasing transaction ids.
    #[test]
    fn transaction_ids_are_monotonic() {
        let mgr = manager(StubClock::new(1));
        let a = mgr.begin();
        let b = mgr.begin();
        let c = mgr.begin();
        assert_eq!(a.id(), TxnId(1));
        assert_eq!(b.id(), TxnId(2));
        assert_eq!(c.id(), TxnId(3));
    }

    /// A commit at one below the sentinel is allowed; the next would reach `+∞`
    /// and is refused in all builds, mirroring the storage writer's guard.
    #[test]
    fn commit_at_the_sentinel_is_refused() {
        let mgr = manager(StubClock::new(SYSTEM_TIME_OPEN.0 - 1));
        let mut a = mgr.begin(); // snapshot SYSTEM_TIME_OPEN-1, cursor there too
        a.write(BusinessKey::new(b"k".to_vec()));
        // next commit = max(clock, cursor+1) = SYSTEM_TIME_OPEN -> refused.
        assert!(matches!(mgr.commit(a), Err(TxnError::TimeExhausted)));
    }

    /// A clock saturated at the `+∞` sentinel (as `SystemClock::now()` does at the
    /// end of the representable range) must not hand out a snapshot *at* the
    /// sentinel — a reader there would resolve no open version. The snapshot is
    /// pulled back to one below it, where reads still work.
    #[test]
    fn snapshot_never_reaches_the_open_sentinel() {
        let mgr = manager(StubClock::new(SYSTEM_TIME_OPEN.0));
        let r = mgr.begin();
        assert_eq!(r.snapshot().0, SystemTimeMicros(SYSTEM_TIME_OPEN.0 - 1));

        // A version open at that clamped snapshot is still visible.
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
        let index = new_index();
        let key = BusinessKey::new(b"k".to_vec());
        delta
            .insert(open_version(
                &key,
                TxnId(1),
                SystemTimeMicros(SYSTEM_TIME_OPEN.0 - 1),
            ))
            .expect("stage open version");
        assert!(read(&delta, &index, &key, r.snapshot()).is_some());
    }
}
