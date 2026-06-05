//! The durable transaction-commit marker.
//!
//! When [`TxnManager::commit`](crate::TxnManager::commit) accepts a transaction
//! it appends one [`CommitRecord`] to the WAL and fsyncs it before the commit is
//! visible — the WAL fsync is the only durability point (invariant 2 of
//! [architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//! The record names the transaction and the system-time coordinate it committed
//! at, so the commit ordering is recoverable from the log alone.
//!
//! ## Scope at v0.1
//!
//! This is the *transaction-boundary* log. It is deliberately distinct from the
//! DML *redo* log ([`stele_storage::dml`]), which records the version rows a
//! write stages. Unifying the two under one tagged WAL record format — and
//! replaying commit records on restart to rebuild the manager's commit
//! high-water mark — is multi-statement transaction work that lands with v0.2;
//! v0.1 single-statement transactions only need the commit marker to be durable,
//! not yet re-read.
//!
//! ## Frame layout
//!
//! A commit record is a fixed 16-byte little-endian frame — no length prefix is
//! needed because the WAL record boundary delimits it:
//!
//! ```text
//! ┌────────────────────┬────────────────────┐
//! │ txn_id   (u64 LE)  │ commit_ts (i64 LE) │
//! │ 8 bytes            │ 8 bytes            │
//! └────────────────────┴────────────────────┘
//! ```

use stele_common::provenance::TxnId;
use stele_common::time::SystemTimeMicros;

/// The number of bytes a [`CommitRecord`] encodes to: a `u64` txn id followed by
/// an `i64` commit timestamp.
pub(crate) const COMMIT_RECORD_LEN: usize = 16;

/// A durable record of one transaction's commit: which transaction, and the
/// system-time coordinate it was assigned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRecord {
    /// The committing transaction.
    pub txn_id: TxnId,
    /// The commit timestamp assigned by the manager — the system-time point at
    /// which this transaction's writes became visible.
    pub commit_ts: SystemTimeMicros,
}

/// Raised when a byte slice cannot be decoded as a [`CommitRecord`].
#[derive(Debug, thiserror::Error)]
#[error("malformed commit record: expected {COMMIT_RECORD_LEN} bytes, got {0}")]
pub struct CommitRecordError(usize);

impl CommitRecord {
    /// Encode into the fixed 16-byte WAL frame.
    #[must_use]
    pub(crate) fn encode(&self) -> [u8; COMMIT_RECORD_LEN] {
        let mut buf = [0u8; COMMIT_RECORD_LEN];
        buf[..8].copy_from_slice(&self.txn_id.0.to_le_bytes());
        buf[8..].copy_from_slice(&self.commit_ts.0.to_le_bytes());
        buf
    }

    /// Decode a commit record from a WAL payload.
    ///
    /// # Errors
    ///
    /// [`CommitRecordError`] if `bytes` is not exactly 16 bytes long — a record
    /// of the wrong size is corruption, not a short read.
    pub fn decode(bytes: &[u8]) -> Result<Self, CommitRecordError> {
        let frame: [u8; COMMIT_RECORD_LEN] = bytes
            .try_into()
            .map_err(|_| CommitRecordError(bytes.len()))?;
        let txn_id = u64::from_le_bytes(frame[..8].try_into().expect("8-byte slice"));
        let commit_ts = i64::from_le_bytes(frame[8..].try_into().expect("8-byte slice"));
        Ok(Self {
            txn_id: TxnId(txn_id),
            commit_ts: SystemTimeMicros(commit_ts),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_record_round_trips() {
        let rec = CommitRecord {
            txn_id: TxnId(42),
            commit_ts: SystemTimeMicros(1_700_000_000_000),
        };
        let decoded = CommitRecord::decode(&rec.encode()).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn wrong_length_is_corruption() {
        let err = CommitRecord::decode(&[0u8; COMMIT_RECORD_LEN - 1]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed commit record: expected 16 bytes, got 15"
        );
    }
}
