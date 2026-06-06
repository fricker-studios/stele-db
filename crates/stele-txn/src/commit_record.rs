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
//! A commit record is a fixed 56-byte little-endian frame — no length prefix is
//! needed because the WAL record boundary delimits it:
//!
//! ```text
//! ┌───────────────────┬────────────────────┬─────────────┬───────────────────────┐
//! │ txn_id  (u64 LE)  │ commit_ts (i64 LE) │ seq (u64 LE)│ prev_hash ([u8; 32])  │
//! │ 8 bytes           │ 8 bytes            │ 8 bytes     │ 32 bytes              │
//! └───────────────────┴────────────────────┴─────────────┴───────────────────────┘
//! ```
//!
//! ## `seq` — the per-commit total-order tiebreak ([ADR-0024])
//!
//! `seq` is a per-commit monotonic counter assigned at
//! [`TxnManager::commit`](crate::TxnManager::commit), distinct from the
//! per-*transaction* [`TxnId`]: a transaction that begins but never commits (a
//! read, or a conflict loser) consumes a `txn_id` but no `seq`. It gives writes a
//! deterministic total order **independent of the µs `commit_ts`**, so two
//! commits that land in the same microsecond tick are still totally ordered. The
//! [version record carries the same tiebreak][STL-141] in a separate change;
//! here it is allocated and made durable on the commit log, the source of truth.
//!
//! [STL-141]: https://allegromusic.atlassian.net/browse/STL-141
//!
//! ## `prev_hash` — the hash-chained commit log ([ADR-0026])
//!
//! Each record carries the [SHA-256](stele_common::hash) digest of the **prior**
//! commit record's full 56-byte frame; the first record chains from
//! [`Digest::ZERO`]. Altering any historical record changes its hash, so the next
//! record's `prev_hash` no longer matches and a [`verify_chain`](crate::chain::verify_chain)
//! pass detects the break. This is the foundation the Merkle inclusion/consistency
//! proofs (~v0.5) build on; the chain is over the **log**, independent of the
//! derived validity index.
//!
//! [ADR-0024]: ../../../docs/adr/0024-time-representation.md
//! [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md

use stele_common::hash::{Digest, SHA256_LEN, sha256};
use stele_common::provenance::TxnId;
use stele_common::time::SystemTimeMicros;

/// The number of bytes a [`CommitRecord`] encodes to: a `u64` txn id, an `i64`
/// commit timestamp, a `u64` sequence number, and a 32-byte predecessor hash.
pub(crate) const COMMIT_RECORD_LEN: usize = 8 + 8 + 8 + SHA256_LEN;

/// A durable record of one transaction's commit: which transaction, the
/// system-time coordinate it was assigned, its per-commit sequence number, and
/// the hash of the prior commit record (the chain link).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRecord {
    /// The committing transaction.
    pub txn_id: TxnId,
    /// The commit timestamp assigned by the manager — the system-time point at
    /// which this transaction's writes became visible.
    pub commit_ts: SystemTimeMicros,
    /// The per-commit monotonic sequence number — the total-order tiebreak for
    /// same-µs commits ([ADR-0024]). See the [module docs](self).
    ///
    /// [ADR-0024]: ../../../docs/adr/0024-time-representation.md
    pub seq: u64,
    /// The SHA-256 digest of the **prior** commit record's frame — the
    /// hash-chain link ([ADR-0026]). [`Digest::ZERO`] for the first record.
    ///
    /// [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
    pub prev_hash: Digest,
}

/// Raised when a byte slice cannot be decoded as a [`CommitRecord`].
#[derive(Debug, thiserror::Error)]
#[error("malformed commit record: expected {COMMIT_RECORD_LEN} bytes, got {0}")]
pub struct CommitRecordError(usize);

impl CommitRecord {
    /// Encode into the fixed 56-byte WAL frame.
    #[must_use]
    pub(crate) fn encode(&self) -> [u8; COMMIT_RECORD_LEN] {
        let mut buf = [0u8; COMMIT_RECORD_LEN];
        buf[..8].copy_from_slice(&self.txn_id.0.to_le_bytes());
        buf[8..16].copy_from_slice(&self.commit_ts.0.to_le_bytes());
        buf[16..24].copy_from_slice(&self.seq.to_le_bytes());
        buf[24..].copy_from_slice(self.prev_hash.as_bytes());
        buf
    }

    /// The chain link this record contributes: the SHA-256 of its own frame.
    ///
    /// The *next* commit record carries this digest as its `prev_hash`, and the
    /// [`verify_chain`](crate::chain::verify_chain) pass recomputes it to check
    /// the chain is intact ([ADR-0026]).
    ///
    /// [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
    #[must_use]
    pub fn hash(&self) -> Digest {
        sha256(&self.encode())
    }

    /// Decode a commit record from a WAL payload.
    ///
    /// # Errors
    ///
    /// [`CommitRecordError`] if `bytes` is not exactly 56 bytes long — a record
    /// of the wrong size is corruption, not a short read.
    pub fn decode(bytes: &[u8]) -> Result<Self, CommitRecordError> {
        let frame: [u8; COMMIT_RECORD_LEN] = bytes
            .try_into()
            .map_err(|_| CommitRecordError(bytes.len()))?;
        let txn_id = u64::from_le_bytes(frame[..8].try_into().expect("8-byte slice"));
        let commit_ts = i64::from_le_bytes(frame[8..16].try_into().expect("8-byte slice"));
        let seq = u64::from_le_bytes(frame[16..24].try_into().expect("8-byte slice"));
        let mut prev_hash = [0u8; SHA256_LEN];
        prev_hash.copy_from_slice(&frame[24..]);
        Ok(Self {
            txn_id: TxnId(txn_id),
            commit_ts: SystemTimeMicros(commit_ts),
            seq,
            prev_hash: Digest(prev_hash),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn record(seq: u64, prev_hash: Digest) -> CommitRecord {
        CommitRecord {
            txn_id: TxnId(42),
            commit_ts: SystemTimeMicros(1_700_000_000_000),
            seq,
            prev_hash,
        }
    }

    #[test]
    fn commit_record_round_trips() {
        let rec = record(7, sha256(b"prior"));
        let decoded = CommitRecord::decode(&rec.encode()).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn round_trips_with_the_genesis_link() {
        let rec = record(1, Digest::ZERO);
        let decoded = CommitRecord::decode(&rec.encode()).expect("decode");
        assert_eq!(decoded, rec);
        assert_eq!(decoded.prev_hash, Digest::ZERO);
    }

    #[test]
    fn wrong_length_is_corruption() {
        let err = CommitRecord::decode(&[0u8; COMMIT_RECORD_LEN - 1]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "malformed commit record: expected 56 bytes, got 55"
        );
    }

    /// `seq` totally orders two commits that share the same µs `commit_ts` — the
    /// tiebreak ADR-0024 mandates. The hash binds each field, so flipping `seq`
    /// changes the record's chain link.
    #[test]
    fn seq_breaks_a_same_microsecond_tie() {
        let earlier = record(1, Digest::ZERO);
        let mut later = earlier;
        later.seq = 2;
        assert_eq!(
            earlier.commit_ts, later.commit_ts,
            "same µs tick — the timestamp cannot order them"
        );
        assert!(earlier.seq < later.seq, "seq provides the total order");
        assert_ne!(
            earlier.hash(),
            later.hash(),
            "seq is bound into the record's hash"
        );
    }

    /// The record's hash covers every field: any single-bit change to a
    /// historical record yields a different chain link, which is what makes the
    /// chain tamper-evident.
    #[test]
    fn hash_covers_every_field() {
        let base = record(1, Digest::ZERO);
        let h = base.hash();
        let mut txn = base;
        txn.txn_id = TxnId(43);
        let mut ts = base;
        ts.commit_ts = SystemTimeMicros(1_700_000_000_001);
        let mut seq = base;
        seq.seq = 2;
        let mut prev = base;
        prev.prev_hash = sha256(b"x");
        for mutated in [txn, ts, seq, prev] {
            assert_ne!(h, mutated.hash(), "a mutated field must change the hash");
        }
    }
}
