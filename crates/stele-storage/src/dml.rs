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

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};

use crate::delta::{BusinessKey, Delta, DeltaError, Version};
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

    /// The delta tier rejected an apply, or a redo record failed to decode.
    #[error(transparent)]
    Delta(#[from] DeltaError),
}

/// The result of a single committed DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DmlOutcome {
    /// The system-time `sys_from` stamped on the new version (for `INSERT` /
    /// `UPDATE`), or the `sys_to` the period was closed at (for `DELETE`).
    pub commit: SystemTimeMicros,
    /// The WAL position immediately after this operation's redo record. Pass it
    /// to [`Wal::commit`] to await durability — the operation is staged in the
    /// delta but is only durable once an fsync covers this offset.
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
}

// `Wal` is not `Debug` (it guards a `Disk` handle behind a mutex) and the clock
// `C` need not be either, so derive is out; surface the writer's clock-free
// state and elide the rest.
impl<C: Clock, D: Disk> std::fmt::Debug for DmlWriter<C, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmlWriter")
            .field("valid_time", &self.writer.valid_time_enabled())
            .field("last_commit", &self.writer.last_commit())
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
        }
    }

    /// Borrow the WAL handle — to await durability ([`Wal::commit`]) or drive a
    /// group-commit fsync ([`Wal::tick`]). The handle is also cloneable.
    pub const fn wal(&self) -> &Wal<D> {
        &self.wal
    }

    /// `INSERT`: open a fresh `[commit, +∞)` period for `key`.
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` already has a live version or the
    /// valid-time policy is violated; [`DmlError::Wal`] / [`DmlError::Delta`] on
    /// a log or staging failure.
    pub fn insert(
        &mut self,
        delta: &mut Delta<D>,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, versions) = self
            .writer
            .stage_insert(delta, key, valid, payload, txn_id, principal)?;
        self.log_and_apply(delta, commit, versions)
    }

    /// `UPDATE`: close `key`'s prior period at `commit` and open a new
    /// `[commit, +∞)` one. Never overwrites — both rows are appended.
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` has no live version or the valid-time
    /// policy is violated; [`DmlError::Wal`] / [`DmlError::Delta`] on a log or
    /// staging failure.
    pub fn update(
        &mut self,
        delta: &mut Delta<D>,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, versions) = self
            .writer
            .stage_update(delta, key, valid, payload, txn_id, principal)?;
        self.log_and_apply(delta, commit, versions)
    }

    /// `DELETE`: close `key`'s prior period at `commit` with no successor — a
    /// tombstone expressed as a period close, carrying the deleting
    /// transaction's provenance ([STL-118]).
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` has no live version; [`DmlError::Wal`] /
    /// [`DmlError::Delta`] on a log or staging failure.
    pub fn delete(
        &mut self,
        delta: &mut Delta<D>,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, versions) = self.writer.stage_delete(delta, key, txn_id, principal)?;
        self.log_and_apply(delta, commit, versions)
    }

    /// Log the resolved redo set to the WAL, then stage it in the delta — in
    /// that order, so the record is durable-eligible before the delta is
    /// mutated. The returned [`DmlOutcome::wal`] is the post-record offset to
    /// await for durability.
    fn log_and_apply(
        &self,
        delta: &mut Delta<D>,
        commit: SystemTimeMicros,
        versions: Vec<Version>,
    ) -> Result<DmlOutcome, DmlError> {
        let record = encode_redo(&versions)?;
        let wal = self.wal.append(&record)?;
        crate::systime::apply(delta, versions)?;
        Ok(DmlOutcome { commit, wal })
    }
}

/// Replay the WAL from `checkpoint` into `delta`, reconstructing the staged
/// state a crash discarded. Returns the number of versions applied.
///
/// Each WAL record is a redo set of [`Version`]s; replay decodes them and
/// applies each with the delta's idempotent insert — the *same* application the
/// forward [`DmlWriter`] path uses, so a replay over already-applied records
/// converges rather than duplicates. Replay stops at the first corrupt record
/// the WAL surfaces ([`crate::wal`]'s torn-write contract).
///
/// Drive this on startup *after* [`Delta::open`] (which discards stale spills),
/// per [`crate::delta`]'s crash semantics.
///
/// # Errors
///
/// [`DmlError::Wal`] if the WAL yields a corrupt or unreadable record;
/// [`DmlError::Delta`] if a record fails to decode or the delta rejects an apply.
pub fn replay<D: Disk>(
    wal: &Wal<D>,
    delta: &mut Delta<D>,
    checkpoint: Checkpoint,
) -> Result<usize, DmlError> {
    let mut applied = 0;
    for record in wal.replay_from(checkpoint) {
        let payload = record?;
        let versions = decode_redo(&payload)?;
        applied += versions.len();
        // The same application point the forward DmlWriter path uses.
        crate::systime::apply(delta, versions)?;
    }
    Ok(applied)
}

/// Encode a resolved redo set as a single WAL record: the version frames
/// concatenated back-to-back. Each [`Version`] frame is self-delimiting (it
/// carries its own lengths), so no envelope is needed — the WAL record boundary
/// delimits the set and [`decode_redo`] walks the frames.
fn encode_redo(versions: &[Version]) -> Result<Vec<u8>, DeltaError> {
    let mut buf = Vec::new();
    for version in versions {
        version.encode(&mut buf)?;
    }
    Ok(buf)
}

/// Decode a redo record back into its version set: walk the concatenated frames
/// until the record is consumed.
///
/// # Errors
///
/// [`DeltaError::Corrupt`] (as [`DmlError::Delta`]) if a frame is truncated or
/// declares a length past the record's end.
fn decode_redo(bytes: &[u8]) -> Result<Vec<Version>, DmlError> {
    let mut versions = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (version, consumed) = Version::decode(rest)?;
        versions.push(version);
        rest = &rest[consumed..];
    }
    Ok(versions)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A redo set of one or two versions round-trips through the record codec —
    /// the property [`replay`] relies on to reconstruct the delta from the WAL.
    #[test]
    fn redo_record_round_trips() {
        use stele_common::provenance::Provenance;
        use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};

        let closed = Version {
            business_key: BusinessKey::new(b"k".to_vec()),
            sys_from: SystemTimeMicros(10),
            sys_to: SystemTimeMicros(20),
            provenance: Provenance::new(
                TxnId(1),
                SystemTimeMicros(10),
                Principal::new(b"a".to_vec()),
            ),
            closed_by: Some(Provenance::new(
                TxnId(2),
                SystemTimeMicros(20),
                Principal::new(b"b".to_vec()),
            )),
            payload: b"old".to_vec(),
        };
        let opened = Version {
            business_key: BusinessKey::new(b"k".to_vec()),
            sys_from: SystemTimeMicros(20),
            sys_to: SYSTEM_TIME_OPEN,
            provenance: Provenance::new(
                TxnId(2),
                SystemTimeMicros(20),
                Principal::new(b"b".to_vec()),
            ),
            closed_by: None,
            payload: b"new".to_vec(),
        };

        let record = encode_redo(&[closed.clone(), opened.clone()]).expect("encode");
        let decoded = decode_redo(&record).expect("decode");
        assert_eq!(decoded, vec![closed, opened]);
    }

    /// A truncated record is corruption, not a silently-dropped tail.
    #[test]
    fn truncated_redo_record_is_corruption() {
        use stele_common::provenance::Provenance;
        use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};

        let v = Version {
            business_key: BusinessKey::new(b"k".to_vec()),
            sys_from: SystemTimeMicros(1),
            sys_to: SYSTEM_TIME_OPEN,
            provenance: Provenance::new(
                TxnId(1),
                SystemTimeMicros(1),
                Principal::new(b"a".to_vec()),
            ),
            closed_by: None,
            payload: b"value".to_vec(),
        };
        let record = encode_redo(&[v]).expect("encode");
        let err = decode_redo(&record[..record.len() - 1]).unwrap_err();
        assert!(matches!(err, DmlError::Delta(DeltaError::Corrupt(_))));
    }
}
