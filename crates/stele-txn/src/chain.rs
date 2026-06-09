//! Verifying the hash-chained commit log ([ADR-0026]).
//!
//! Each [`CommitRecord`] carries the
//! [SHA-256](stele_common::hash) of its predecessor's frame, so the durable
//! commit log is a hash chain anchored at [`Digest::ZERO`]. [`verify_chain`]
//! walks that chain from genesis and confirms every link: recompute each
//! record's hash and check the next record's `prev_hash` matches. Altering any
//! historical record changes its hash, breaking the link at the *following*
//! record — which is exactly the tamper-evidence the verifiable-audit-log pillar
//! promises.
//!
//! A correctly-formed but **wholly rewritten** chain (every link recomputed to be
//! internally consistent) is *not* caught by [`verify_chain`] alone — that needs
//! an external anchor. [`verify_chain_to`] supplies one: a caller that durably
//! remembers the head hash (a checkpoint/witness, the seed of the Merkle work in
//! ~v0.5) detects a full rewrite because the recomputed head no longer matches
//! the anchor. The chain is over the **log** — the source of truth — independent
//! of the derived validity index.
//!
//! [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md

use stele_common::hash::Digest;
use stele_common::provenance::TxnId;
use stele_common::time::SystemTimeMicros;
use stele_storage::wal::WalError;

use crate::commit_record::{CommitRecord, CommitRecordError};

/// The outcome of a clean [`verify_chain`] pass: the chain's head and length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainHead {
    /// The hash of the last record in the chain — the running head, suitable as
    /// the anchor a later [`verify_chain_to`] checks against. [`Digest::ZERO`]
    /// for an empty log (genesis with no records).
    pub head: Digest,
    /// The number of commit records verified.
    pub len: u64,
}

/// A verified chain plus the tail state a recovering
/// [`TxnManager`](crate::TxnManager) needs to resume issuing commits without
/// colliding with the durable history ([`TxnManager::recover`]).
///
/// Returned by [`verify_chain_recover`]: it is a [`ChainHead`] (the same
/// fail-closed verification) augmented with the three monotonic facts the
/// manager's in-memory cursor is rebuilt from. Every field is genesis-valued for
/// an empty log, so recovering an empty log yields the same state a fresh
/// [`TxnManager::new`](crate::TxnManager::new) starts at.
///
/// [`TxnManager::recover`]: crate::TxnManager::recover
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredChain {
    /// The verified head — the hash of the last record, [`Digest::ZERO`] for an
    /// empty log. Becomes the recovered manager's `commit_head` (the next
    /// commit's `prev_hash`).
    pub head: Digest,
    /// The number of commit records verified.
    pub len: u64,
    /// The greatest commit timestamp in the log — the last record's, since the
    /// log is written in strictly increasing `commit_ts` order. The recovered
    /// commit cursor; [`SystemTimeMicros(0)`](SystemTimeMicros) for an empty log.
    pub commit_ts: SystemTimeMicros,
    /// The greatest sequence number in the log — the last record's, since `seq`
    /// is dense and monotonic over commits. The next commit takes `seq + 1`; `0`
    /// for an empty log (so recovery resumes at `seq` 1).
    pub seq: u64,
    /// The greatest transaction id over the **whole** log — *not* necessarily the
    /// last record's, because transaction ids are assigned at `begin` but appended
    /// in commit order, so a later-begun transaction can commit earlier. The next
    /// transaction takes `max_txn_id + 1`; [`TxnId(0)`](TxnId) for an empty log.
    pub max_txn_id: TxnId,
}

/// Why a commit-log chain failed to verify.
#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    /// A record at `index` could not be read back from the WAL — the log is
    /// corrupt or truncated below this point.
    #[error("commit log replay failed at record {index}: {source}")]
    Replay {
        /// Zero-based position of the unreadable record.
        index: u64,
        /// The underlying WAL error.
        source: WalError,
    },

    /// A record at `index` was not a well-formed [`CommitRecord`] — wrong size,
    /// i.e. not a commit-log frame.
    #[error("commit log record {index} is malformed: {source}")]
    Decode {
        /// Zero-based position of the malformed record.
        index: u64,
        /// The decode error.
        source: CommitRecordError,
    },

    /// The hash chain is broken at `index`: this record's `prev_hash` does not
    /// match the hash of the record before it. The hallmark of a tampered-with
    /// historical record.
    #[error(
        "commit log chain broken at record {index}: prev_hash {found} does not match predecessor {expected}",
        found = .found.to_hex(),
        expected = .expected.to_hex(),
    )]
    BrokenLink {
        /// Zero-based position of the record whose back-link is wrong.
        index: u64,
        /// The hash the predecessor actually has.
        expected: Digest,
        /// The (mismatching) hash this record claims its predecessor has.
        found: Digest,
    },

    /// The chain verified internally but its head does not match the externally
    /// anchored head — only raised by [`verify_chain_to`]. Catches a wholesale
    /// rewrite that re-linked every record consistently.
    #[error(
        "commit log head {found} does not match the expected anchor {expected}",
        found = .found.to_hex(),
        expected = .expected.to_hex(),
    )]
    HeadMismatch {
        /// The anchor the caller expected.
        expected: Digest,
        /// The head the log actually hashes to.
        found: Digest,
    },
}

/// Verify a hash-chained commit log read from `records`, starting at genesis
/// ([`Digest::ZERO`]).
///
/// `records` is an iterator of raw WAL payloads in log order — pass
/// [`Wal::replay_from(Checkpoint::BEGIN)`](stele_storage::wal::Wal::replay_from)
/// directly. Each payload is decoded as a [`CommitRecord`] and its `prev_hash` is
/// checked against the running hash of the chain so far. A clean log returns its
/// [`ChainHead`]; the first broken link, malformed frame, or replay error stops
/// the pass with the offending record's index.
///
/// # Errors
///
/// * [`ChainError::Replay`] — a record could not be read from the WAL.
/// * [`ChainError::Decode`] — a record was not a valid commit-log frame.
/// * [`ChainError::BrokenLink`] — a record's `prev_hash` does not match its
///   predecessor: tamper detected.
pub fn verify_chain<I>(records: I) -> Result<ChainHead, ChainError>
where
    I: IntoIterator<Item = Result<Vec<u8>, WalError>>,
{
    let recovered = verify_chain_recover(records)?;
    Ok(ChainHead {
        head: recovered.head,
        len: recovered.len,
    })
}

/// Verify a hash-chained commit log and recover its tail state.
///
/// Verifies exactly as [`verify_chain`] does, additionally recovering the tail
/// state a [`TxnManager`](crate::TxnManager) needs to resume — the recovery entry
/// point [`TxnManager::recover`] sits on.
///
/// The verification is identical to [`verify_chain`] (which delegates here): the
/// same genesis walk, the same fail-closed errors, so a single tampered
/// historical record is caught as a [`ChainError::BrokenLink`] before any state
/// is rebuilt. The extra work is purely accumulation — the last record's
/// `commit_ts`/`seq` and the greatest `txn_id` seen — so it costs nothing beyond
/// the walk the log already requires.
///
/// [`TxnManager::recover`]: crate::TxnManager::recover
///
/// # Errors
///
/// Identical to [`verify_chain`]: [`ChainError::Replay`], [`ChainError::Decode`],
/// or [`ChainError::BrokenLink`] at the first offending record.
pub fn verify_chain_recover<I>(records: I) -> Result<RecoveredChain, ChainError>
where
    I: IntoIterator<Item = Result<Vec<u8>, WalError>>,
{
    let mut prev = Digest::ZERO;
    let mut len = 0u64;
    let mut commit_ts = SystemTimeMicros(0);
    let mut seq = 0u64;
    let mut max_txn_id = TxnId(0);
    for (index, item) in records.into_iter().enumerate() {
        let index = index as u64;
        let bytes = item.map_err(|source| ChainError::Replay { index, source })?;
        let record =
            CommitRecord::decode(&bytes).map_err(|source| ChainError::Decode { index, source })?;
        if record.prev_hash != prev {
            return Err(ChainError::BrokenLink {
                index,
                expected: prev,
                found: record.prev_hash,
            });
        }
        prev = record.hash();
        len += 1;
        // The log is in commit order, so the last record carries the greatest
        // `commit_ts`/`seq`; `txn_id` is assigned at begin, not commit, so the
        // greatest can sit anywhere — track it across every record.
        commit_ts = record.commit_ts;
        seq = record.seq;
        if record.txn_id.0 > max_txn_id.0 {
            max_txn_id = record.txn_id;
        }
    }
    Ok(RecoveredChain {
        head: prev,
        len,
        commit_ts,
        seq,
        max_txn_id,
    })
}

/// Verify a commit log and additionally check its head matches `expected_head` —
/// the anchored-verification path.
///
/// Equivalent to [`verify_chain`] followed by asserting the resulting head equals
/// the caller's durably-remembered anchor. Because the anchor is external to the
/// log, this catches a chain that was rewritten wholesale (every link recomputed
/// to be internally consistent) — which [`verify_chain`] cannot detect on its
/// own. The anchor is the seed of the checkpoint/witness mechanism the Merkle
/// proofs (~v0.5) formalize.
///
/// # Errors
///
/// Every error of [`verify_chain`], plus [`ChainError::HeadMismatch`] when the
/// log verifies internally but its head differs from `expected_head`.
pub fn verify_chain_to<I>(records: I, expected_head: Digest) -> Result<ChainHead, ChainError>
where
    I: IntoIterator<Item = Result<Vec<u8>, WalError>>,
{
    let verified = verify_chain(records)?;
    if verified.head != expected_head {
        return Err(ChainError::HeadMismatch {
            expected: expected_head,
            found: verified.head,
        });
    }
    Ok(verified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::provenance::TxnId;
    use stele_common::time::SystemTimeMicros;

    /// Build a chain of `n` well-linked commit records, each chaining from the
    /// prior one's hash. Returns the encoded frames in log order.
    fn chain(n: u64) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut prev = Digest::ZERO;
        for i in 0..n {
            let rec = CommitRecord {
                txn_id: TxnId(i + 1),
                commit_ts: SystemTimeMicros(1_000 + i64::try_from(i).expect("small test n")),
                seq: i + 1,
                prev_hash: prev,
            };
            prev = rec.hash();
            out.push(rec.encode().to_vec());
        }
        out
    }

    fn ok(frames: Vec<Vec<u8>>) -> Vec<Result<Vec<u8>, WalError>> {
        frames.into_iter().map(Ok).collect()
    }

    #[test]
    fn a_clean_chain_verifies() {
        let head = verify_chain(ok(chain(5))).expect("clean chain verifies");
        assert_eq!(head.len, 5);
        assert_ne!(head.head, Digest::ZERO);
    }

    #[test]
    fn an_empty_log_verifies_to_genesis() {
        let head = verify_chain(ok(Vec::new())).expect("empty log verifies");
        assert_eq!(
            head,
            ChainHead {
                head: Digest::ZERO,
                len: 0
            }
        );
    }

    /// Altering any historical record breaks the chain at the *next* record,
    /// whose `prev_hash` no longer matches the tampered predecessor's new hash.
    #[test]
    fn tampering_with_a_historical_record_is_detected() {
        let mut frames = chain(5);
        // Flip a byte inside record 1's payload (its commit_ts) — a silent
        // history rewrite that leaves every other frame untouched.
        frames[1][8] ^= 0x01;
        let err = verify_chain(ok(frames)).expect_err("tamper must be detected");
        match err {
            // Record 2 back-links to record 1; record 1's hash changed, so the
            // break surfaces at index 2.
            ChainError::BrokenLink { index, .. } => assert_eq!(index, 2),
            other => panic!("expected a broken link, got {other:?}"),
        }
    }

    /// Tampering with the *last* record is caught by the anchored check even
    /// though no following record exists to break the internal chain.
    #[test]
    fn anchored_verify_catches_a_rewritten_head() {
        let anchor = verify_chain(ok(chain(4))).expect("baseline").head;

        // Rewrite the whole chain to a different transaction set but keep it
        // internally consistent — verify_chain alone would accept it.
        let mut forged = chain(4);
        let mut rec = CommitRecord::decode(&forged[3]).unwrap();
        rec.txn_id = TxnId(999);
        forged[3] = rec.encode().to_vec();
        // Re-link nothing else: this single change still breaks the internal
        // chain only if a successor exists. It is the last record, so the
        // internal pass passes but the head differs from the anchor.
        let err = verify_chain_to(ok(forged), anchor).expect_err("head mismatch");
        assert!(matches!(err, ChainError::HeadMismatch { .. }));
    }

    /// A wholly-rewritten but internally-consistent chain passes the bare
    /// verify (the limitation an external anchor exists to close).
    #[test]
    fn a_consistent_rewrite_passes_bare_verify_but_fails_against_anchor() {
        let original = chain(3);
        let anchor = verify_chain(ok(original)).expect("baseline").head;

        // A different history, re-chained from genesis so every link is valid.
        let mut forged = Vec::new();
        let mut prev = Digest::ZERO;
        for i in 0..3u64 {
            let rec = CommitRecord {
                txn_id: TxnId(100 + i),
                commit_ts: SystemTimeMicros(9_000 + i64::try_from(i).expect("small test n")),
                seq: i + 1,
                prev_hash: prev,
            };
            prev = rec.hash();
            forged.push(rec.encode().to_vec());
        }
        // Internally consistent ⇒ bare verify accepts it...
        assert!(verify_chain(ok(forged.clone())).is_ok());
        // ...but the anchored verify rejects it: the head differs.
        let err = verify_chain_to(ok(forged), anchor).expect_err("anchor rejects forgery");
        assert!(matches!(err, ChainError::HeadMismatch { .. }));
    }

    #[test]
    fn a_malformed_frame_is_reported_with_its_index() {
        let mut frames = chain(3);
        frames[2].truncate(10); // not a 56-byte commit frame anymore
        let err = verify_chain(ok(frames)).expect_err("malformed frame");
        match err {
            ChainError::Decode { index, .. } => assert_eq!(index, 2),
            other => panic!("expected a decode error, got {other:?}"),
        }
    }

    #[test]
    fn a_replay_error_is_surfaced_with_its_index() {
        let mut items = ok(chain(2));
        items.push(Err(WalError::PayloadTooLarge(99)));
        let err = verify_chain(items).expect_err("replay error");
        match err {
            ChainError::Replay { index, .. } => assert_eq!(index, 2),
            other => panic!("expected a replay error, got {other:?}"),
        }
    }
}
