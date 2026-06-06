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
use crate::systime::{Redo, SysTimeError};
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
    #[allow(clippy::too_many_arguments)] // tier handles + key/valid/payload + provenance triple
    pub fn insert<I: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, redos) = self
            .writer
            .stage_insert(delta, index, key, valid, payload, txn_id, principal)?;
        self.log_and_apply(delta, index, commit, redos)
    }

    /// `UPDATE`: close `key`'s prior period at `commit` and open a new
    /// `[commit, +∞)` one. Never overwrites — both rows are appended.
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` has no live version or the valid-time
    /// policy is violated; [`DmlError::Wal`] / [`DmlError::Delta`] on a log or
    /// staging failure.
    #[allow(clippy::too_many_arguments)] // tier handles + key/valid/payload + provenance triple
    pub fn update<I: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, redos) = self
            .writer
            .stage_update(delta, index, key, valid, payload, txn_id, principal)?;
        self.log_and_apply(delta, index, commit, redos)
    }

    /// `DELETE`: close `key`'s prior period at `commit` with no successor — a
    /// tombstone expressed as a period close, carrying the deleting
    /// transaction's provenance ([STL-118]).
    ///
    /// # Errors
    ///
    /// [`DmlError::Resolve`] if `key` has no live version; [`DmlError::Wal`] /
    /// [`DmlError::Delta`] on a log or staging failure.
    pub fn delete<I: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, DmlError> {
        let (commit, redos) = self
            .writer
            .stage_delete(delta, index, key, txn_id, principal)?;
        self.log_and_apply(delta, index, commit, redos)
    }

    /// Log the resolved redo set to the WAL, then stage it into the delta tier
    /// and validity index — in that order, so the record is durable-eligible
    /// before either structure is touched. The returned [`DmlOutcome::wal`] is
    /// the post-record offset to await for durability.
    fn log_and_apply<I: Disk>(
        &self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        commit: SystemTimeMicros,
        redos: Vec<Redo>,
    ) -> Result<DmlOutcome, DmlError> {
        let record = encode_redo(&redos)?;
        let wal = self.wal.append(&record)?;
        crate::systime::apply(delta, index, redos)?;
        Ok(DmlOutcome { commit, wal })
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
        let redos = decode_redo(&payload)?;
        applied += redos.len();
        // The same application point the forward DmlWriter path uses.
        crate::systime::apply(delta, index, redos)?;
    }
    Ok(applied)
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
            sys_to: SystemTimeMicros(20),
            closed_by: Provenance::new(
                TxnId(2),
                SystemTimeMicros(20),
                Principal::new(b"b".to_vec()),
            ),
        });
        let opened = Redo::Insert(Version::open(
            BusinessKey::new(b"k".to_vec()),
            SystemTimeMicros(20),
            Provenance::new(
                TxnId(2),
                SystemTimeMicros(20),
                Principal::new(b"b".to_vec()),
            ),
            b"new".to_vec(),
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
            Provenance::new(TxnId(1), SystemTimeMicros(1), Principal::new(b"a".to_vec())),
            b"value".to_vec(),
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
}
