//! DML write path — `INSERT` / `UPDATE` / `DELETE` that flow through WAL → delta.
//!
//! This is the temporal heart of the write path ([STL-94]). It binds the three
//! data-manipulation operations to the durability + staging machinery built by
//! the surrounding tickets:
//!
//! * [`crate::systime`] / [`crate::validtime`] **resolve** an operation into the
//!   concrete version rows it stages — closing prior periods on the system axis
//!   and opening new ones ([architecture §2](../../../docs/02-architecture.md#2-the-bitemporal-record-model)).
//! * The [`crate::wal`] **logs** that resolved set as one redo record. The WAL
//!   fsync is the only durability point (invariant 2 of
//!   [architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//! * The [`crate::delta`] tier **stages** the rows for reads until compaction.
//!
//! ```text
//! INSERT / UPDATE / DELETE
//!        │  resolve (systime/validtime): close prior, open new
//!        ▼
//!   redo record  ──append──▶  WAL  (durability point at fsync)
//!        │
//!        ▼  apply
//!   delta tier
//! ```
//!
//! The order is **write-ahead**: a record is appended to the WAL *before* the
//! delta is touched ([architecture §3.4](../../../docs/02-architecture.md#34-write-path-sequence)).
//! So if the process dies between the two, recovery still reconstructs the delta
//! by replaying the log — and never the other way around.
//!
//! ## Group commit: one record per transaction ([STL-192])
//!
//! A multi-statement `COMMIT` opens a [group buffer](DmlWriter::begin_group): each
//! write then *applies* to the delta/index immediately — so a later write in the
//! transaction resolves against an earlier one — but its redos are buffered rather
//! than appended, and [`commit_group`](DmlWriter::commit_group) writes the whole
//! transaction as **one** record group-committed with **one** fsync. The delta is
//! non-durable (rebuilt from the log on recovery), so applying before that single
//! fsync is safe: a crash before `commit_group` leaves no record and recovery
//! reconstructs *none* of the transaction's writes; a crash that tears the record
//! drops it whole at the fence. Either way the transaction recovers all-or-none —
//! the WAL record boundary is the transaction boundary.
//!
//! ## One apply path, forward and on recovery
//!
//! A redo record is exactly the set of resolved [`Version`]s the operation
//! stages (one for `INSERT`/`DELETE`, two for `UPDATE`). [`DmlWriter`] applies
//! that set with [`crate::systime`]'s shared apply step; [`replay`] decodes the
//! same records from the WAL and applies them with the *same* step. The delta's
//! idempotent replace on `(business_key, sys_from)` makes replaying an already-
//! applied record a no-op, so "same code path under sim and under real I/O"
//! ([STL-94] scope) is structural, not aspirational. The delta tier itself makes
//! no durability claim ([`crate::delta`]'s crash semantics); the WAL is its
//! canonical truth.
//!
//! ## What this is *not*
//!
//! There is no transaction manager here yet: `txn_id` and `principal` are
//! supplied by the caller, and one [`DmlWriter`] owns a single monotonic
//! commit-timestamp counter ([`crate::systime`]). Global commit ordering across
//! transactions and nodes is the transaction manager's job
//! ([architecture §9](../../../docs/02-architecture.md#9-transaction--concurrency-model)).
//! Nor does this select among versions on either axis — `AS OF` resolution and
//! its correctness oracle are a separate ticket; this module's correctness oracle
//! is the per-key *timeline reconstruction* (no gaps, no overlaps) exercised in
//! `tests/dml.rs`.

use std::collections::BTreeSet;

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};

use crate::delta::{BusinessKey, Delta, DeltaError, Version};
use crate::systime::{Redo, SealedLookup, SysTimeError};
use crate::validity::{Close, ValidityError, ValidityIndex};
use crate::validtime::{ValidInterval, ValidTimeError, ValidTimeWriter};
use crate::wal::{Checkpoint, Disk, LogOffset, Wal, WalError};

/// Errors surfaced from the DML write path.
#[derive(Debug, thiserror::Error)]
pub enum DmlError {
    /// Resolution failed in the system/valid-time write path — a policy
    /// mismatch, a missing or duplicate live version, or an exhausted
    /// system-time domain. Wraps [`ValidTimeError`] (which itself wraps the
    /// system-time errors).
    #[error(transparent)]
    Resolve(#[from] ValidTimeError),

    /// The WAL append (or, on replay, a record read) failed or hit corruption.
    #[error(transparent)]
    Wal(#[from] WalError),

    /// A version redo frame failed to decode, or the delta tier rejected a stage.
    #[error(transparent)]
    Delta(#[from] DeltaError),

    /// A close redo frame failed to decode, or the validity index rejected a
    /// close ([ADR-0023]).
    #[error(transparent)]
    Validity(#[from] ValidityError),

    /// The shared apply step (`crate::systime::apply`) failed staging the redo
    /// set into the delta tier and validity index.
    #[error(transparent)]
    Apply(#[from] SysTimeError),
}

/// The result of a single committed DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlOutcome {
    /// The system-time `sys_from` stamped on the new version (for `INSERT` /
    /// `UPDATE`), or the `sys_to` the period was closed at (for `DELETE`).
    pub commit: SystemTimeMicros,
    /// The WAL position to await for durability. On the auto-commit path it is the
    /// offset immediately *after* this operation's redo record — pass it to
    /// [`Wal::commit`]; the operation is durable once an fsync covers it. In
    /// **group-commit** mode the redo record is deferred to
    /// [`DmlWriter::commit_group`], so this is instead the current durable end (this
    /// write is not past it yet); the caller awaits the group's durability through
    /// `commit_group`, not this offset.
    pub wal: LogOffset,
}

/// Stamps, logs, and stages `INSERT` / `UPDATE` / `DELETE` through WAL → delta.
///
/// Owns a [`Wal`] handle (cheap to clone) and a [`ValidTimeWriter`] that resolves
/// each operation and stamps the commit timestamp. The [`Delta`] is passed per
/// call — the WAL and the delta keep separate [`Disk`] namespaces
/// ([`crate::delta`]).
pub struct DmlWriter<C: Clock, D: Disk> {
    wal: Wal<D>,
    writer: ValidTimeWriter<C>,
    /// The open group-commit buffer, if a multi-statement transaction is in
    /// flight ([`begin_group`](Self::begin_group), [STL-192]).
    ///
    /// `None` is the auto-commit default: each write appends and is logged as its
    /// own WAL record. `Some(buffer)` defers the WAL record: a write *applies* to
    /// the delta/index immediately (so a later write in the same transaction
    /// resolves against it, front-to-back) but its redos accumulate here instead of
    /// being appended, until [`commit_group`](Self::commit_group) writes the whole
    /// transaction as **one** record group-committed with **one** fsync — the
    /// durability point (invariant 2). That single record is the atomic unit
    /// recovery replays whole or, if a crash tears it, drops at the fence — so a
    /// committed transaction's writes recover all-or-none.
    group: Option<Vec<Redo>>,
}

// `Wal` is not `Debug` (it guards a `Disk` handle behind a mutex) and the clock
// `C` need not be either, so derive is out; surface the writer's clock-free
// state and elide the rest.
impl<C: Clock, D: Disk> std::fmt::Debug for DmlWriter<C, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmlWriter")
            .field("valid_time", &self.writer.valid_time_enabled())
            .field("last_commit", &self.writer.last_commit())
            .field("group_buffered", &self.group.as_ref().map(Vec::len))
            .finish_non_exhaustive()
    }
}

impl<C: Clock, D: Disk> DmlWriter<C, D> {
    /// Build a writer for one table: its WAL, a commit `clock` for the system
    /// axis, and whether the table opts into valid-time (`valid_time`, mirroring
    /// the catalog flag).
    pub const fn new(wal: Wal<D>, clock: C, valid_time: bool) -> Self {
        Self {
            wal,
            writer: ValidTimeWriter::new(clock, valid_time),
            group: None,
        }
    }

    /// Borrow the WAL handle — to await durability ([`Wal::commit`]) or drive a
    /// group-commit fsync ([`Wal::tick`]). The handle is also cloneable.
    pub const fn wal(&self) -> &Wal<D> {
        &self.wal
    }

    /// `INSERT`: open a fresh `[commit, +∞)` period for `key`. `seq` is the
    /// per-commit total-order tiebreak ([ADR-0024], [STL-141]) stamped inline on
    /// the new version, supplied by the transaction manager alongside `txn_id`.
    ///
    /// `sealed` is the table's sealed-segment lookup (typically a
    /// [`SealedSegments`](crate::systime::SealedSegments) built from the segment
    /// set, or [`EmptySealed`](crate::systime::EmptySealed) when there are none),
    /// passed per call so it always reflects the segments live at this commit —
    /// the duplicate-key check spans it, so a key whose live version sits only in
    /// a sealed segment is correctly rejected ([STL-140]).
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` already has a live version or the
    /// valid-time policy is violated; [`DmlError::Wal`] / [`DmlError::Delta`] on
    /// a log or staging failure.
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/valid/payload + provenance triple
    pub fn insert<I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, redos) = self.writer.stage_insert(
            delta, index, sealed, key, valid, payload, seq, txn_id, principal,
        )?;
        self.log_and_apply(delta, index, commit, redos)
    }

    /// `UPDATE`: close `key`'s prior period at `commit` and open a new
    /// `[commit, +∞)` one. Never overwrites — both rows are appended. `seq` is
    /// the per-commit total-order tiebreak ([ADR-0024], [STL-141]) stamped on the
    /// new open version.
    ///
    /// The prior period is resolved across the delta tier, the `sealed` segments,
    /// and the validity index, so an `UPDATE` closes a live version that lives
    /// only in a sealed segment — materializing the close in the index while the
    /// new open version stages in the delta ([STL-140]). See [`Self::insert`] for
    /// `sealed`.
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` has no live version or the valid-time
    /// policy is violated; [`DmlError::Wal`] / [`DmlError::Delta`] on a log or
    /// staging failure.
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/valid/payload + provenance triple
    pub fn update<I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, redos) = self.writer.stage_update(
            delta, index, sealed, key, valid, payload, seq, txn_id, principal,
        )?;
        self.log_and_apply(delta, index, commit, redos)
    }

    /// `DELETE`: close `key`'s prior period at `commit` with no successor — a
    /// tombstone expressed as a period close, carrying the deleting
    /// transaction's provenance ([STL-118]).
    ///
    /// The prior period is resolved across the delta tier, the `sealed` segments,
    /// and the validity index, so a `DELETE` retracts a live version that lives
    /// only in a sealed segment ([STL-140]). See [`Self::insert`] for `sealed`.
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` has no live version; [`DmlError::Wal`] /
    /// [`DmlError::Delta`] on a log or staging failure.
    pub fn delete<I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, redos) = self
            .writer
            .stage_delete(delta, index, sealed, key, txn_id, principal)?;
        self.log_and_apply(delta, index, commit, redos)
    }

    /// Log the resolved redo set, then stage it into the delta tier and validity
    /// index.
    ///
    /// **Auto-commit (no open group)** is write-ahead: the redos are appended as
    /// their own WAL record *before* either structure is touched, so the record is
    /// durable-eligible first. [`DmlOutcome::wal`] is the post-record offset to
    /// await for durability.
    ///
    /// **Group-commit (a transaction is in flight, [STL-192])** defers the WAL
    /// record to [`commit_group`](Self::commit_group): the redos apply to the
    /// delta/index now — so a later write in the same transaction resolves against
    /// this one (front-to-back) — and accumulate in the group buffer. Nothing is
    /// durable until `commit_group` writes the whole transaction as one record and
    /// fsyncs once, so a crash before then recovers *none* of the buffered writes.
    /// [`DmlOutcome::wal`] reports the current durable end (this write is not past
    /// it yet); the auto-commit callers ignore the offset.
    fn log_and_apply<I: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        commit: SystemTimeMicros,
        redos: Vec<Redo>,
    ) -> Result<DmlOutcome, DmlError> {
        if self.group.is_some() {
            // Apply before buffering so a same-transaction successor sees this
            // write; the WAL record is deferred to `commit_group`.
            crate::systime::apply(delta, index, redos.clone())?;
            let wal = self.wal.durable_end();
            self.group.as_mut().expect("group is open").extend(redos);
            return Ok(DmlOutcome { commit, wal });
        }
        let record = encode_redo(&redos)?;
        let wal = self.wal.append(&record)?;
        crate::systime::apply(delta, index, redos)?;
        Ok(DmlOutcome { commit, wal })
    }

    /// Open a group-commit buffer: subsequent writes apply to the delta/index but
    /// defer their WAL record to [`commit_group`](Self::commit_group), so the whole
    /// transaction lands as one record group-committed with one fsync ([STL-192]).
    ///
    /// Call once at the start of a multi-statement transaction's apply phase, paired
    /// with exactly one [`commit_group`](Self::commit_group) (durable) or
    /// [`abort_group`](Self::abort_group) (discard). A fresh buffer is installed each
    /// call — a stray prior buffer (a transaction that was neither committed nor
    /// aborted) is discarded, never silently appended.
    pub fn begin_group(&mut self) {
        self.group = Some(Vec::new());
    }

    /// Group-commit the open buffer: append every redo the transaction staged as a
    /// **single** WAL record and fsync once — the one durability point per `COMMIT`
    /// (invariant 2, [STL-192]). Returns the durable end after the fsync.
    ///
    /// The record is the atomic unit: recovery's [`recover_replay`] applies the
    /// whole redo set or, if a crash tears the record, drops it at the durable
    /// fence ([`crate::wal`]'s torn-write contract) — so the transaction's writes
    /// recover all-or-none. An empty buffer (a read-only transaction, or one whose
    /// writes all went to other tables) writes no record and skips the fsync.
    ///
    /// Clears the group buffer, returning the writer to auto-commit mode.
    ///
    /// # Errors
    ///
    /// [`DmlError::Wal`] if the append or fsync fails. Two cases, per the WAL
    /// durability contract:
    ///
    /// * **the append fails / is torn** — no complete record reaches the log, so
    ///   recovery finds nothing of the transaction (a torn frame fails its CRC and
    ///   is dropped at the fence); and
    /// * **the append succeeds but the fsync ([`Wal::tick`]) fails** — the complete
    ///   record is already *staged* in the WAL, so its durability is **indeterminate**:
    ///   a later successful `tick` (e.g. a [`checkpoint`](crate::engine::Engine::checkpoint))
    ///   could still make it durable. An fsync failure must therefore be treated as a
    ///   crash (the engine should stop and recover) rather than as a clean abort —
    ///   hardening the engine to enforce that is [STL-217]. Either way no *new*
    ///   durability point is introduced: the fsync is the only one.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    pub fn commit_group(&mut self) -> Result<LogOffset, DmlError> {
        self.finish_group(None)
    }

    /// Group-commit the open buffer as **one leg of a multi-table transaction**
    /// ([STL-215]): append the transaction's writes as a single **two-phase** WAL
    /// record — tagged with `txn_id` so recovery can recognize it — and fsync once.
    ///
    /// A two-phase record is durable but **inert** until vouched: recovery replays
    /// it only if `txn_id`'s commit marker is durable in the engine commit log
    /// ([`recover_replay`]'s [`CommittedTxns`] gate). The session driver writes one
    /// marker after every table's leg is durable, so a crash between the per-table
    /// commits and the marker discards every leg and the transaction recovers
    /// all-or-none across tables. A **single-table** transaction needs no marker and
    /// takes the plain [`commit_group`](Self::commit_group) fast path instead.
    ///
    /// Clears the group buffer, returning the writer to auto-commit mode. The
    /// fsync-failure caveat is identical to [`commit_group`](Self::commit_group)
    /// ([STL-217]).
    ///
    /// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
    ///
    /// # Errors
    ///
    /// [`DmlError::Wal`] if the append or fsync fails.
    pub fn commit_group_two_phase(&mut self, txn_id: TxnId) -> Result<LogOffset, DmlError> {
        self.finish_group(Some(txn_id))
    }

    /// Append the buffered transaction as one WAL record and fsync once — the
    /// shared body of [`commit_group`](Self::commit_group) (plain, `txn_id` =
    /// [`None`]) and [`commit_group_two_phase`](Self::commit_group_two_phase)
    /// (`txn_id` = [`Some`]). A plain record is byte-for-byte what the single-record
    /// path always wrote; a two-phase record prepends the
    /// [`TWO_PHASE_RECORD_TAG`] envelope. An empty buffer writes no record and skips
    /// the fsync.
    fn finish_group(&mut self, txn_id: Option<TxnId>) -> Result<LogOffset, DmlError> {
        let redos = self.group.take().unwrap_or_default();
        if redos.is_empty() {
            return Ok(self.wal.durable_end());
        }
        let record = match txn_id {
            // Single-table / auto-commit: the record boundary alone is the atomic
            // commit point, so it stays the pre-STL-215 framing — recovery applies
            // it unconditionally.
            None => encode_redo(&redos)?,
            // A leg of a multi-table transaction: tag the record with the committing
            // transaction so recovery can gate it on the commit marker (STL-215).
            Some(txn_id) => {
                let redos = encode_redo(&redos)?;
                let mut record = Vec::with_capacity(1 + 8 + redos.len());
                record.push(TWO_PHASE_RECORD_TAG);
                record.extend_from_slice(&txn_id.0.to_le_bytes());
                record.extend_from_slice(&redos);
                record
            }
        };
        self.wal.append(&record)?;
        // The single group-commit fsync — the transaction's durability point. If it
        // fails after the append above, the staged record's durability is
        // indeterminate (a later tick may still flush it); the caller must treat
        // that as a crash, not a clean abort (STL-217).
        self.wal.tick()?;
        Ok(self.wal.durable_end())
    }

    /// Discard the open group buffer without logging it — the transaction aborted
    /// ([STL-192]). The buffered redos were already applied to the (non-durable)
    /// delta/index, but with no WAL record they are never made durable: a recovery
    /// rebuilds the tiers from the log and so drops them. Returns the writer to
    /// auto-commit mode.
    pub fn abort_group(&mut self) {
        self.group = None;
    }
}

/// Replay the WAL from `checkpoint` into `delta` **and** `index`, reconstructing
/// both the staged versions and the materialized closes a crash discarded.
/// Returns the number of redo entries applied.
///
/// Each WAL record is a redo set of tagged [`Redo`] entries; replay decodes them
/// and applies each with the *same* `crate::systime::apply` the forward
/// [`DmlWriter`] path uses — so a replay over already-applied records converges
/// rather than duplicates (the delta's idempotent insert and the index's
/// idempotent write-once close). Replay stops at the first corrupt record the WAL
/// surfaces ([`crate::wal`]'s torn-write contract).
///
/// Drive this on startup *after* [`Delta::open`] and [`ValidityIndex::open`]
/// (both discard stale spills), per the crash semantics of [`crate::delta`] /
/// [`crate::validity`]. The reconstructed index is **exactly** the pre-crash one
/// ([ADR-0023]'s rebuildability claim).
///
/// # Errors
///
/// [`DmlError::Wal`] if the WAL yields a corrupt or unreadable record;
/// [`DmlError::Delta`] / [`DmlError::Validity`] if a frame fails to decode;
/// [`DmlError::Apply`] if a structure rejects the apply.
pub fn replay<D: Disk, I: Disk>(
    wal: &Wal<D>,
    delta: &mut Delta<D>,
    index: &mut ValidityIndex<I>,
    checkpoint: Checkpoint,
) -> Result<usize, DmlError> {
    let mut applied = 0;
    for record in wal.replay_from(checkpoint) {
        let payload = record?;
        // Strip the optional two-phase envelope (STL-215). Plain verification does
        // not commit-gate — that is [`recover_replay`]'s job — so a two-phase leg is
        // applied like any other record; this path is for single-table / intact logs.
        let (_txn_id, redo_bytes) = split_record(&payload)?;
        let redos = decode_redo(redo_bytes)?;
        applied += redos.len();
        // The same application point the forward DmlWriter path uses.
        crate::systime::apply(delta, index, redos)?;
    }
    Ok(applied)
}

/// Replay like [`replay`], but **tolerate a torn record past the durable fence**.
///
/// This is the difference between *verifying* an intact log and *recovering* a
/// crashed one ([STL-102], [architecture §3.6](../../../docs/02-architecture.md#36-crash-recovery)).
/// [`replay`] propagates the WAL's torn-write signal as an error — right for a log
/// expected to be whole. Recovery instead expects the *tail* to be torn: the only
/// durability point is the fsync ([`crate::wal`] invariant 2), so a crash can shear
/// the record being appended *after* the last fsync.
///
/// `from` is where replay starts; `fence` is the last fully-flushed offset
/// (the persisted checkpoint, or [`LogOffset::ZERO`] when none survives). It
/// replays the full range from `from` and, on the WAL's torn-frame
/// signal ([`std::io::ErrorKind::InvalidData`]), uses the **start offset of the
/// corrupt record** ([`Replay::position`](crate::wal::Replay::position)) to decide:
///
/// * **at or after `fence`** — the unsynced tail a mid-write crash left behind. Stop
///   and keep the durable prefix already applied; this is the expected end of a
///   crashed log.
/// * **before `fence`** — a record the checkpoint vouched durable is corrupt. That is
///   real durable-region corruption, **not** a torn tail, and propagates as an error
///   rather than being silently truncated.
///
/// A non-torn I/O error, or a CRC-*valid* payload that fails to decode (a logic/format
/// bug, not a torn write), always propagates. Returns the number of redo entries
/// applied from the surviving prefix.
///
/// `committed` gates **two-phase** records — the legs of a multi-table `COMMIT`
/// ([STL-215]). A record tagged with a transaction id is replayed only if that
/// transaction is in [`committed`](CommittedTxns) (its commit marker is durable);
/// otherwise it is skipped, so a crash between the per-table commits and the marker
/// recovers the transaction all-or-none across every table. Plain (single-table /
/// auto-commit) records carry no tag and always apply. A single table's bare
/// [`Engine::recover`](crate::engine::Engine::recover) passes [`CommittedTxns::All`]
/// (no cross-table coordination to gate on).
///
/// v0.1 replays from [`Checkpoint::BEGIN`] (the full log) and rebuilds the validity
/// index from it per [ADR-0023]; `fence` here is the *durability boundary*, not yet a
/// replay-*skip* — that realignment rides STL-133 / STL-136.
///
/// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
///
/// # Errors
///
/// [`DmlError::Wal`] on a non-torn I/O failure or corruption before `fence`;
/// [`DmlError::Delta`] / [`DmlError::Validity`] if a CRC-valid record fails to decode;
/// [`DmlError::Apply`] if a structure rejects the apply.
pub fn recover_replay<D: Disk, I: Disk>(
    wal: &Wal<D>,
    delta: &mut Delta<D>,
    index: &mut ValidityIndex<I>,
    from: Checkpoint,
    fence: LogOffset,
    committed: &CommittedTxns,
) -> Result<usize, DmlError> {
    let mut applied = 0;
    let mut replay = wal.replay_from(from);
    while let Some(record) = replay.next() {
        let payload = match record {
            Ok(payload) => payload,
            Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::InvalidData => {
                // The cursor sits at the start of the corrupt record (a failed read
                // does not advance it). Past the fence ⇒ the unsynced tail of a
                // crash: drop it. At/under the fence ⇒ the checkpoint vouched this
                // record durable, so its corruption is a real fault, not a torn tail.
                if replay.position() >= fence {
                    break;
                }
                return Err(WalError::Io(e).into());
            }
            Err(other) => return Err(other.into()),
        };
        let (txn_id, redo_bytes) = split_record(&payload)?;
        if let Some(txn_id) = txn_id {
            if !committed.admits(txn_id) {
                // A multi-table leg whose commit marker never became durable —
                // discard it so the transaction recovers all-or-none (STL-215).
                continue;
            }
        }
        let redos = decode_redo(redo_bytes)?;
        applied += redos.len();
        crate::systime::apply(delta, index, redos)?;
    }
    Ok(applied)
}

/// The set of multi-table transactions whose commit marker is durable — the gate
/// [`recover_replay`] applies to **two-phase** WAL redo records ([STL-215]).
///
/// A multi-table `COMMIT` writes each table's writes as a two-phase record (tagged
/// with the transaction id, [`DmlWriter::commit_group_two_phase`]) and is committed
/// only once a single marker — "transaction T committed" — is fsynced *after* every
/// per-table record is durable. On recovery a two-phase record is replayed **iff**
/// its transaction is in this set; otherwise the marker never became durable (a
/// crash between the per-table commits and the marker) and the leg is discarded, so
/// the transaction recovers all-or-none across every table it wrote. A plain record
/// (auto-commit or the single-table fast path) carries no transaction id and always
/// applies.
///
/// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
#[derive(Debug, Clone)]
pub enum CommittedTxns {
    /// Apply every record regardless of commit mode — the default for a bare
    /// [`Engine::recover`](crate::engine::Engine::recover). A single table's WAL has
    /// no cross-table coordination to gate on, and the per-table sims/tests that
    /// drive it never write two-phase records.
    All,
    /// Apply a two-phase record only if its transaction id is in this set — built by
    /// the session recovery driver from the durable engine commit log.
    Only(BTreeSet<TxnId>),
}

impl CommittedTxns {
    /// Whether a two-phase record committed by `txn_id` should be replayed.
    #[must_use]
    fn admits(&self, txn_id: TxnId) -> bool {
        match self {
            Self::All => true,
            Self::Only(set) => set.contains(&txn_id),
        }
    }
}

/// Tag byte for a [`Redo::Insert`] frame in a WAL redo record.
const REDO_TAG_INSERT: u8 = 0;
/// Tag byte for a [`Redo::Close`] frame in a WAL redo record.
const REDO_TAG_CLOSE: u8 = 1;
/// Tag byte for a [`Redo::Retract`] frame in a WAL redo record. A retraction
/// shares the [`Close`] wire format — the tag is what distinguishes a durable
/// delete (persisted into segments at flush) from a re-derivable supersession
/// close ([ADR-0023]).
const REDO_TAG_RETRACT: u8 = 2;

/// Encode a resolved redo set as a single WAL record: each entry is a one-byte
/// tag (insert / close / retract) followed by the entry's self-delimiting frame,
/// all concatenated back-to-back. The frames carry their own lengths, so no
/// envelope is needed — the WAL record boundary delimits the set and
/// [`decode_redo`] walks it, dispatching on the tag.
fn encode_redo(redos: &[Redo]) -> Result<Vec<u8>, DmlError> {
    let mut buf = Vec::new();
    for redo in redos {
        match redo {
            Redo::Insert(version) => {
                buf.push(REDO_TAG_INSERT);
                version.encode(&mut buf)?;
            }
            Redo::Close(close) => {
                buf.push(REDO_TAG_CLOSE);
                close.encode(&mut buf)?;
            }
            Redo::Retract(close) => {
                buf.push(REDO_TAG_RETRACT);
                close.encode(&mut buf)?;
            }
        }
    }
    Ok(buf)
}

/// Decode a redo record back into its redo set: walk the concatenated
/// `tag || frame` entries until the record is consumed.
///
/// # Errors
///
/// [`DmlError::Delta`] / [`DmlError::Validity`] if a frame is truncated or
/// declares a length past the record's end, or [`DmlError::Delta`] for an unknown
/// tag byte.
fn decode_redo(bytes: &[u8]) -> Result<Vec<Redo>, DmlError> {
    let mut redos = Vec::new();
    let mut rest = bytes;
    while let Some((&tag, body)) = rest.split_first() {
        match tag {
            REDO_TAG_INSERT => {
                let (version, consumed) = Version::decode(body)?;
                redos.push(Redo::Insert(version));
                rest = &body[consumed..];
            }
            REDO_TAG_CLOSE => {
                let (close, consumed) = Close::decode(body)?;
                redos.push(Redo::Close(close));
                rest = &body[consumed..];
            }
            REDO_TAG_RETRACT => {
                let (close, consumed) = Close::decode(body)?;
                redos.push(Redo::Retract(close));
                rest = &body[consumed..];
            }
            _ => {
                return Err(DmlError::Delta(DeltaError::Corrupt(
                    "unknown redo tag byte",
                )));
            }
        }
    }
    Ok(redos)
}

/// Leading byte marking a **two-phase** WAL redo record ([STL-215]): one leg of a
/// multi-table `COMMIT`. The byte is followed by the committing transaction's id
/// (`u64` LE) and then the redo entries ([`encode_redo`]). A single-table or
/// auto-commit record carries **no** such prefix — it begins directly with a redo
/// tag (`0`/`1`/`2`, [`REDO_TAG_INSERT`]…) — so the two framings can never collide,
/// every record the single-record path writes reads back byte-for-byte unchanged,
/// and recovery tells a gated leg from a plain record by this one byte.
///
/// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
const TWO_PHASE_RECORD_TAG: u8 = 0xFF;

/// Split a WAL redo record into its optional two-phase transaction id and the
/// trailing redo-entry bytes ([`decode_redo`] consumes the latter). A record that
/// begins with [`TWO_PHASE_RECORD_TAG`] is one leg of a multi-table transaction
/// (gated on the commit marker, [`CommittedTxns`]); any other record — the common
/// single-record path — is plain, so the whole payload is redo entries.
///
/// # Errors
///
/// [`DmlError::Delta`] if a two-phase record is truncated before its 8-byte
/// transaction id.
fn split_record(payload: &[u8]) -> Result<(Option<TxnId>, &[u8]), DmlError> {
    match payload.split_first() {
        Some((&TWO_PHASE_RECORD_TAG, rest)) => {
            let id = rest.get(..8).ok_or(DmlError::Delta(DeltaError::Corrupt(
                "two-phase redo record truncated before its transaction id",
            )))?;
            let txn_id = TxnId(u64::from_le_bytes(id.try_into().expect("8 bytes")));
            Ok((Some(txn_id), &rest[8..]))
        }
        _ => Ok((None, payload)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An update's redo set — a [`Redo::Close`] (the prior period's end) plus a
    /// [`Redo::Insert`] (the new open version) — round-trips through the tagged
    /// record codec, the property [`replay`] relies on to reconstruct the delta
    /// and the validity index from the WAL.
    #[test]
    fn redo_record_round_trips() {
        use stele_common::provenance::Provenance;
        use stele_common::time::SystemTimeMicros;

        let close = Redo::Close(Close {
            business_key: BusinessKey::new(b"k".to_vec()),
            sys_from: SystemTimeMicros(10),
            // A non-zero `seq` proves the closed version's tiebreak round-trips
            // through the WAL close frame (STL-145).
            seq: 7,
            sys_to: SystemTimeMicros(20),
            closed_by: Provenance::new(
                TxnId(2),
                SystemTimeMicros(20),
                Principal::new(b"b".to_vec()),
            ),
        });
        // A non-zero `seq` proves the per-commit tiebreak round-trips through the
        // WAL redo frame (STL-141) — `decode_redo` must reconstruct it intact.
        let opened = Redo::Insert(Version::open(
            BusinessKey::new(b"k".to_vec()),
            SystemTimeMicros(20),
            99,
            Provenance::new(
                TxnId(2),
                SystemTimeMicros(20),
                Principal::new(b"b".to_vec()),
            ),
            Some(b"new".to_vec()),
        ));

        let record = encode_redo(&[close.clone(), opened.clone()]).expect("encode");
        let decoded = decode_redo(&record).expect("decode");
        assert_eq!(decoded, vec![close, opened]);
    }

    /// A delete's redo set — a single [`Redo::Retract`] — round-trips through the
    /// tagged record codec under its own tag, so replay reconstructs a retraction
    /// (not a plain close) and the durable-tombstone distinction survives the WAL.
    #[test]
    fn retract_redo_record_round_trips() {
        use stele_common::provenance::Provenance;
        use stele_common::time::SystemTimeMicros;

        let retract = Redo::Retract(Close {
            business_key: BusinessKey::new(b"acct".to_vec()),
            sys_from: SystemTimeMicros(10),
            seq: 3,
            sys_to: SystemTimeMicros(30),
            closed_by: Provenance::new(
                TxnId(7),
                SystemTimeMicros(30),
                Principal::new(b"deleter".to_vec()),
            ),
        });

        let record = encode_redo(&[retract.clone()]).expect("encode");
        let decoded = decode_redo(&record).expect("decode");
        assert_eq!(decoded, vec![retract]);
        // The tag must be the retract tag — not the close tag — so the two are
        // genuinely distinguished on the wire.
        assert_eq!(record.first(), Some(&REDO_TAG_RETRACT));
    }

    /// A truncated record is corruption, not a silently-dropped tail.
    #[test]
    fn truncated_redo_record_is_corruption() {
        use stele_common::provenance::Provenance;
        use stele_common::time::SystemTimeMicros;

        let redo = Redo::Insert(Version::open(
            BusinessKey::new(b"k".to_vec()),
            SystemTimeMicros(1),
            0,
            Provenance::new(TxnId(1), SystemTimeMicros(1), Principal::new(b"a".to_vec())),
            Some(b"value".to_vec()),
        ));
        let record = encode_redo(&[redo]).expect("encode");
        let err = decode_redo(&record[..record.len() - 1]).unwrap_err();
        assert!(matches!(err, DmlError::Delta(DeltaError::Corrupt(_))));
    }

    /// An unknown tag byte is corruption, not a silently-dropped entry.
    #[test]
    fn unknown_redo_tag_is_corruption() {
        let err = decode_redo(&[0xFFu8]).unwrap_err();
        assert!(matches!(err, DmlError::Delta(DeltaError::Corrupt(_))));
    }

    /// The torn-tail gate, three ways: plain [`replay`] rejects a torn tail (it
    /// verifies an intact log); [`recover_replay`] *tolerates* it when the shear is
    /// at/after the durable fence (the unsynced tail of a crash); but it stays
    /// *fatal* when the fence claims the corrupt region durable — corruption inside
    /// the durable prefix is never silently truncated ([STL-102]).
    #[test]
    fn recover_replay_gates_a_torn_tail_on_the_durable_fence() {
        use stele_common::provenance::{Principal, Provenance};
        use stele_common::time::SystemTimeMicros;

        use crate::backend::{Disk, DiskFile, MemDisk};
        use crate::delta::{Delta, DeltaConfig};
        use crate::validity::{ValidityConfig, ValidityIndex};
        use crate::wal::{LogOffset, Wal, WalConfig};

        let disk = MemDisk::new();
        let wal = Wal::open(disk.clone(), WalConfig::default()).expect("wal");
        // One complete, durable insert record.
        let record = encode_redo(&[Redo::Insert(Version::open(
            BusinessKey::new(b"k".to_vec()),
            SystemTimeMicros(1),
            0,
            Provenance::new(TxnId(1), SystemTimeMicros(1), Principal::new(b"a".to_vec())),
            Some(b"v".to_vec()),
        ))])
        .expect("encode");
        wal.append(&record).expect("append");
        wal.tick().expect("fsync");
        // The fence is the durable end *before* the shear — exactly where a
        // mid-append crash would tear the next record.
        let fence = wal.durable_end();
        // A crash mid-append of the next record: stray bytes that frame as a torn
        // record (a length/crc header that cannot verify a complete payload).
        let mut seg = disk
            .open("wal-00000000000000000000.log")
            .expect("open wal segment");
        seg.append(&[0xFF; 6]).expect("append torn tail");
        seg.sync().expect("sync");

        // 1. Plain replay rejects the torn tail.
        let mut d = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut i = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        assert!(
            matches!(
                replay(&wal, &mut d, &mut i, Checkpoint::BEGIN),
                Err(DmlError::Wal(_)),
            ),
            "plain replay propagates the torn-write signal",
        );

        // 2. Recovery with the fence *at* the shear tolerates it and applies the
        //    one durable record.
        let mut d = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut i = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let applied = recover_replay(
            &wal,
            &mut d,
            &mut i,
            Checkpoint::BEGIN,
            fence,
            &CommittedTxns::All,
        )
        .expect("recover tolerates a tail at/after the fence");
        assert_eq!(
            applied, 1,
            "the durable prefix is applied, the torn tail dropped"
        );

        // 3. A fence that claims the corrupt region durable makes the same shear
        //    fatal — durable-prefix corruption is not swallowed.
        let beyond = LogOffset {
            segment_index: fence.segment_index,
            byte_offset: fence.byte_offset + 100,
        };
        let mut d = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut i = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        assert!(
            matches!(
                recover_replay(
                    &wal,
                    &mut d,
                    &mut i,
                    Checkpoint::BEGIN,
                    beyond,
                    &CommittedTxns::All,
                ),
                Err(DmlError::Wal(_)),
            ),
            "corruption before the fence is fatal, not a tolerated tail",
        );
    }

    /// A monotonic step clock for the group-commit tests: each reading is one µs
    /// past the last, so the writes in a transaction get distinct `sys_from`s.
    struct StepClock(std::sync::atomic::AtomicI64);
    impl stele_common::time::Clock for StepClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
        }
    }

    /// A group commit defers every buffered write to **one** WAL record and **one**
    /// fsync ([STL-192]): mid-transaction nothing is appended, and `commit_group`
    /// then writes the whole set as a single record that recovery replays whole.
    /// This is the property that makes a transaction's writes recover all-or-none.
    #[test]
    fn group_commit_writes_one_record_with_one_fsync() {
        use stele_common::provenance::Principal;

        use crate::backend::MemDisk;
        use crate::delta::{Delta, DeltaConfig};
        use crate::systime::EmptySealed;
        use crate::validity::{ValidityConfig, ValidityIndex};
        use crate::wal::{Wal, WalConfig};

        let disk = MemDisk::new();
        let wal = Wal::open(disk, WalConfig::default()).expect("wal");
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let mut writer = DmlWriter::new(
            wal.clone(),
            StepClock(std::sync::atomic::AtomicI64::new(0)),
            false,
        );
        let principal = Principal::new(b"p".to_vec());

        writer.begin_group();
        for (i, payload) in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
            .into_iter()
            .enumerate()
        {
            writer
                .insert(
                    &mut delta,
                    &mut index,
                    &EmptySealed,
                    BusinessKey::new(format!("k{i}").into_bytes()),
                    None,
                    Some(payload),
                    0,
                    TxnId(7),
                    principal.clone(),
                )
                .expect("buffered insert");
        }

        // Group mode defers the WAL record: nothing is appended or fsynced yet.
        assert_eq!(
            wal.replay_from(Checkpoint::BEGIN).count(),
            0,
            "the transaction's writes are buffered, not appended per write"
        );
        assert_eq!(
            wal.durable_end(),
            LogOffset::ZERO,
            "no fsync before commit_group"
        );

        writer.commit_group().expect("group commit");

        // Exactly one record now carries all three inserts' redos, and it is durable.
        let records: Vec<Vec<u8>> = wal
            .replay_from(Checkpoint::BEGIN)
            .collect::<Result<_, _>>()
            .expect("replay");
        assert_eq!(records.len(), 1, "the whole transaction is one WAL record");
        let redos = decode_redo(&records[0]).expect("decode the transaction record");
        assert_eq!(redos.len(), 3, "all three inserts ride in the one record");
        assert!(
            wal.durable_end() > LogOffset::ZERO,
            "commit_group fsynced the record once",
        );
    }

    /// Aborting a group discards the buffered writes: no WAL record is ever written,
    /// so a recovery finds no trace of the aborted transaction ([STL-192]).
    #[test]
    fn abort_group_writes_no_record() {
        use stele_common::provenance::Principal;

        use crate::backend::MemDisk;
        use crate::delta::{Delta, DeltaConfig};
        use crate::systime::EmptySealed;
        use crate::validity::{ValidityConfig, ValidityIndex};
        use crate::wal::{Wal, WalConfig};

        let disk = MemDisk::new();
        let wal = Wal::open(disk, WalConfig::default()).expect("wal");
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let mut writer = DmlWriter::new(
            wal.clone(),
            StepClock(std::sync::atomic::AtomicI64::new(0)),
            false,
        );

        writer.begin_group();
        writer
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                BusinessKey::new(b"k".to_vec()),
                None,
                Some(b"v".to_vec()),
                0,
                TxnId(1),
                Principal::new(b"p".to_vec()),
            )
            .expect("buffered insert");
        writer.abort_group();

        assert_eq!(
            wal.replay_from(Checkpoint::BEGIN).count(),
            0,
            "an aborted group leaves no durable record"
        );
    }

    /// A plain (single-table / auto-commit) group commit writes the pre-STL-215
    /// framing: the record begins with a redo tag, not the two-phase tag, and
    /// [`split_record`] reports no transaction — so recovery applies it
    /// unconditionally and records written before STL-215 read back unchanged.
    #[test]
    fn a_plain_group_commit_writes_an_ungated_record() {
        use stele_common::provenance::Principal;

        use crate::backend::MemDisk;
        use crate::delta::{Delta, DeltaConfig};
        use crate::systime::EmptySealed;
        use crate::validity::{ValidityConfig, ValidityIndex};
        use crate::wal::{Wal, WalConfig};

        let disk = MemDisk::new();
        let wal = Wal::open(disk, WalConfig::default()).expect("wal");
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let mut writer = DmlWriter::new(
            wal.clone(),
            StepClock(std::sync::atomic::AtomicI64::new(0)),
            false,
        );

        writer.begin_group();
        writer
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                BusinessKey::new(b"k".to_vec()),
                None,
                Some(b"v".to_vec()),
                0,
                TxnId(7),
                Principal::new(b"p".to_vec()),
            )
            .expect("buffered insert");
        writer.commit_group().expect("plain group commit");

        let records: Vec<Vec<u8>> = wal
            .replay_from(Checkpoint::BEGIN)
            .collect::<Result<_, _>>()
            .expect("replay");
        assert_eq!(records.len(), 1, "one record for the transaction");
        assert_eq!(
            records[0].first(),
            Some(&REDO_TAG_INSERT),
            "a plain record begins with a redo tag, not the two-phase tag",
        );
        let (txn_id, redos) = split_record(&records[0]).expect("split");
        assert_eq!(txn_id, None, "a plain record is ungated");
        assert_eq!(decode_redo(redos).expect("decode").len(), 1);
    }

    /// A multi-table leg writes a **two-phase** record tagged with its transaction,
    /// and [`recover_replay`] applies it only when the [`CommittedTxns`] gate admits
    /// that transaction — the cross-table all-or-none mechanism ([STL-215]). The
    /// same record recovers to *nothing* without the marker and to the whole leg
    /// with it.
    #[test]
    fn recover_replay_gates_a_two_phase_record_on_its_commit_marker() {
        use stele_common::provenance::Principal;

        use crate::backend::MemDisk;
        use crate::delta::{Delta, DeltaConfig};
        use crate::systime::EmptySealed;
        use crate::validity::{ValidityConfig, ValidityIndex};
        use crate::wal::{Wal, WalConfig};

        let disk = MemDisk::new();
        let wal = Wal::open(disk, WalConfig::default()).expect("wal");
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let mut writer = DmlWriter::new(
            wal.clone(),
            StepClock(std::sync::atomic::AtomicI64::new(0)),
            false,
        );
        let principal = Principal::new(b"p".to_vec());

        let txn = TxnId(9);
        writer.begin_group();
        for i in 0..2 {
            writer
                .insert(
                    &mut delta,
                    &mut index,
                    &EmptySealed,
                    BusinessKey::new(format!("k{i}").into_bytes()),
                    None,
                    Some(vec![i]),
                    0,
                    txn,
                    principal.clone(),
                )
                .expect("buffered insert");
        }
        writer
            .commit_group_two_phase(txn)
            .expect("two-phase group commit");
        let fence = wal.durable_end();

        // The record is tagged two-phase and names the committing transaction.
        let records: Vec<Vec<u8>> = wal
            .replay_from(Checkpoint::BEGIN)
            .collect::<Result<_, _>>()
            .expect("replay");
        assert_eq!(records.len(), 1, "the whole leg is one record");
        assert_eq!(
            records[0].first(),
            Some(&TWO_PHASE_RECORD_TAG),
            "a multi-table leg is tagged two-phase",
        );
        assert_eq!(split_record(&records[0]).expect("split").0, Some(txn));

        // Helper: recover the leg under a given gate, returning the redo count applied.
        let recover_under = |committed: &CommittedTxns| {
            let mut d = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
            let mut i =
                ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
            recover_replay(&wal, &mut d, &mut i, Checkpoint::BEGIN, fence, committed)
                .expect("recover")
        };

        // No marker ⇒ the leg is uncommitted ⇒ discarded entirely (all-or-none).
        assert_eq!(
            recover_under(&CommittedTxns::Only(BTreeSet::new())),
            0,
            "a two-phase leg with no commit marker recovers to nothing",
        );
        // Marker present ⇒ both writes apply.
        let committed = CommittedTxns::Only(std::iter::once(txn).collect());
        assert_eq!(
            recover_under(&committed),
            2,
            "a two-phase leg whose marker is durable recovers whole",
        );
        // `All` ignores the gate (a single table's bare recover).
        assert_eq!(recover_under(&CommittedTxns::All), 2);
    }
}
