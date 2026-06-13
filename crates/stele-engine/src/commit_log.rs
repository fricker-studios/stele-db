//! The durable **hash-chained commit log** — cross-table transaction atomicity
//! ([STL-215]) *and* the live server's tamper-evident audit chain ([STL-302],
//! [ADR-0031]).
//!
//! [STL-192] made a multi-statement `COMMIT` crash-atomic *per table*: each table a
//! transaction touches owns its WAL, and its writes land as one group-committed
//! redo record + one fsync, so that table recovers all-or-none. But a transaction
//! spanning **several** tables writes one such record *per table*, and a crash
//! *between* two tables' group commits would leave some tables' writes durable and
//! others not — a partial commit across tables.
//!
//! This log closes that gap with the classic redo + commit-marker protocol. A
//! multi-table `COMMIT` writes each table's writes as a **two-phase** redo record
//! ([`stele_storage::dml`]) — durable, but inert until vouched — and then appends
//! one record here naming the transaction, fsynced *after* every per-table record
//! is durable. That fsync is the commit point: on recovery a two-phase record is
//! replayed only if its transaction's commit record is present
//! ([`SessionEngine::recover`](crate::SessionEngine::recover) →
//! [`CommittedTxns`](stele_storage::dml::CommittedTxns)); a crash before it
//! discards every leg, so the transaction recovers all-or-none across every table
//! it wrote.
//!
//! ## The hash chain ([ADR-0031])
//!
//! Each record is a [`CommitRecord`] — the **same** SHA-256-chained frame
//! [`TxnManager`](stele_txn::TxnManager) writes (STL-178): `{txn_id, commit_ts,
//! seq, prev_hash}`, where `prev_hash` is the hash of the prior record's frame and
//! the first record chains from [`Digest::ZERO`](stele_common::hash::Digest::ZERO).
//! Altering any historical record changes its hash, so the next record's
//! `prev_hash` no longer matches and a [`verify_chain`](stele_txn::verify_chain)
//! pass detects the break — the tamper-evidence invariant 10 promises, now reachable
//! from the live server (the chain previously lived only in `stele-txn`, which
//! `SessionEngine` does not use).
//!
//! Because the chain spans **every** data commit (not just multi-table ones), the
//! single-table fast path now also writes a record — one extra fsync per commit,
//! the cost [ADR-0031] accepts for the audit chain, refining [ADR-0029]'s
//! one-fsync-per-single-table-commit optimization. The cross-table gating is
//! unchanged: a two-phase leg still applies iff its `txn_id` has a durable record.
//!
//! ## Framing and the torn-tail contract
//!
//! The framing mirrors the [catalog log](crate::catalog_log): each record is
//! `magic(4) | payload_len(4 LE) | payload | crc32c(4 LE)` (the CRC covers magic +
//! length + payload; the payload is the [`CommitRecord`] frame), and the same
//! two-shape contract applies. A **torn tail** — a partial trailing frame, or a
//! tail not beginning with the magic — is the debris of a crashed append whose
//! fsync never returned: the record was never acknowledged, so the transaction
//! recovers as *uncommitted* (all-or-none = none) and replay stops cleanly.
//! **Corruption** — a *complete* frame whose CRC fails — is an acknowledged commit
//! gone bad: replay fails closed rather than silently resurrecting or dropping a
//! transaction. (A complete frame whose CRC passes but whose chain link is broken
//! is the *tamper* case, caught one level up by
//! [`verify_chain`](stele_txn::verify_chain) over the payloads this returns.)
//!
//! [STL-192]: https://allegromusic.atlassian.net/browse/STL-192
//! [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
//! [STL-302]: https://allegromusic.atlassian.net/browse/STL-302
//! [ADR-0029]: ../../../docs/adr/0029-cross-table-commit-marker.md
//! [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md

use std::io;

use stele_storage::backend::{Disk, DiskFile};
use stele_storage::checksum::crc32c;
use stele_txn::{COMMIT_RECORD_LEN, CommitRecord};

/// The canonical commit-log filename on the session's shared disk. A bare path
/// component, like the catalog log; per-table files all carry a `t{idx:020}-`
/// namespace prefix ([`NamespacedDisk`](crate::NamespacedDisk)), so it can never
/// collide with a table's tier.
pub(crate) const COMMIT_LOG_FILENAME: &str = "stele.commits";

/// Four-byte record magic — `b"STCM"` (STele CoMmit). Distinguishes a record from a
/// torn / zero-filled tail and is folded into the CRC.
const MAGIC: [u8; 4] = *b"STCM";

/// Bytes before the payload: magic + payload length.
const HEADER_LEN: usize = 8;

/// Bytes of the trailing CRC32C.
const CRC_LEN: usize = 4;

/// One record's payload: the fixed-size [`CommitRecord`] frame ([ADR-0031]).
const PAYLOAD_LEN: usize = COMMIT_RECORD_LEN;

/// `InvalidData` with a context message — the shape every decode failure maps to,
/// so callers can surface one coherent "commit log corrupt" error.
fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Encode one [`CommitRecord`] as a complete frame: header, the record's
/// fixed-size frame, and the trailing CRC over everything before it.
fn encode_frame(record: &CommitRecord) -> Vec<u8> {
    let payload = record.encode();
    let len = u32::try_from(payload.len()).expect("the commit-record payload fits u32");
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len() + CRC_LEN);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&payload);
    let crc = crc32c(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

/// Append one commit record — the chain link for transaction `record.txn_id` — and
/// **fsync** it. This is the commit point: the caller has already made the
/// transaction's per-table writes durable, and acknowledges the `COMMIT` only after
/// this returns ([STL-215] write-ahead ordering, [ADR-0031] hash chain).
///
/// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
/// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
///
/// # Errors
///
/// [`io::Error`] if the log cannot be created/opened, appended, or synced — the
/// commit is refused. A partially appended frame is exactly the torn tail
/// [`replay`] tolerates: the transaction recovers as uncommitted (all-or-none =
/// none), and every per-table leg is discarded.
pub(crate) fn append<D: Disk>(disk: &D, record: &CommitRecord) -> io::Result<()> {
    let frame = encode_frame(record);
    let mut file = match disk.open(COMMIT_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let file = disk.create(COMMIT_LOG_FILENAME)?;
            // Directory fence ([STL-232]): the first record's all-or-none
            // claim is only as durable as the file's directory entry. On
            // fence failure, undo the create (best-effort) so a retry
            // re-creates and re-fences — otherwise the retry would take the
            // `open` path, which never fences, and could acknowledge onto an
            // entry no fence ever vouched for.
            if let Err(e) = disk.sync_dir() {
                drop(file);
                let _ = disk.remove(COMMIT_LOG_FILENAME);
                return Err(e);
            }
            file
        }
        Err(e) => return Err(e),
    };
    file.append(&frame)?;
    file.sync()?;
    Ok(())
}

/// Replay the commit log into the ordered list of [`CommitRecord`] payloads (the
/// raw fixed-size frames, in log order) — the input both
/// [`verify_chain`](stele_txn::verify_chain) (the audit/recovery verdict) and the
/// committed-`txn_id` set ([`CommittedTxns`](stele_storage::dml::CommittedTxns)
/// gating) are derived from. An absent log — a fresh disk, or a session that never
/// committed — is the empty list.
///
/// Applies the torn-tail contract from the [module docs](self): a partial trailing
/// frame, or a tail that does not begin with the magic, is the unacknowledged
/// debris of a crashed append and is ignored; a *complete* frame whose CRC fails is
/// corruption of an acknowledged commit and fails closed. The returned payloads are
/// each exactly [`COMMIT_RECORD_LEN`] bytes, so
/// [`CommitRecord::decode`](stele_txn::CommitRecord::decode) on any of them cannot
/// fail.
///
/// # Errors
///
/// [`io::Error`] if the file cannot be read, or holds a corrupt (CRC-failing or
/// wrong-length) complete record.
pub(crate) fn replay<D: Disk>(disk: &D) -> io::Result<Vec<Vec<u8>>> {
    let file = match disk.open(COMMIT_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let len = file.len();
    let mut records = Vec::new();
    let mut offset = 0u64;

    loop {
        // Header: magic + payload length. A shorter remainder is a torn tail.
        let mut header = [0u8; HEADER_LEN];
        if offset + (HEADER_LEN as u64) > len || file.read_at(offset, &mut header)? < HEADER_LEN {
            break;
        }
        if header[0..4] != MAGIC {
            // Not a record boundary: the zero/garbage fill of a torn append. Nothing
            // past it was acknowledged (every record was fsynced before the next),
            // so stop.
            break;
        }
        let payload_len = u64::from(u32::from_le_bytes(
            header[4..8].try_into().expect("4 bytes"),
        ));
        let frame_len = (HEADER_LEN as u64) + payload_len + (CRC_LEN as u64);
        if offset + frame_len > len {
            break; // torn tail: the record's fsync never completed
        }

        // The frame is complete, so it was acknowledged — from here on, damage is
        // corruption and fails closed. A record payload is the fixed `PAYLOAD_LEN`;
        // validate the on-disk length against it *before* allocating, so a corrupt
        // length field cannot drive a large allocation during recovery.
        let payload_bytes = usize::try_from(payload_len)
            .map_err(|_| corrupt("commit log: record too large for this platform"))?;
        if payload_bytes != PAYLOAD_LEN {
            return Err(corrupt(
                "commit log: a complete record is not the fixed-size payload — corrupt",
            ));
        }
        let mut body = [0u8; PAYLOAD_LEN + CRC_LEN];
        if file.read_at(offset + (HEADER_LEN as u64), &mut body)? < body.len() {
            return Err(corrupt("commit log: short read inside a complete record"));
        }
        let (payload, crc_bytes) = body.split_at(PAYLOAD_LEN);
        let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
        let mut covered = Vec::with_capacity(HEADER_LEN + payload.len());
        covered.extend_from_slice(&header);
        covered.extend_from_slice(payload);
        if crc32c(&covered) != stored {
            return Err(corrupt(
                "commit log: CRC mismatch on a complete record — an acknowledged commit is corrupt",
            ));
        }
        records.push(payload.to_vec());
        offset += frame_len;
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    use stele_common::hash::Digest;
    use stele_common::provenance::TxnId;
    use stele_common::time::SystemTimeMicros;
    use stele_storage::backend::MemDisk;
    use stele_txn::{verify_chain, verify_chain_to};

    /// Build the `i`-th record of a well-linked chain whose head is `prev`.
    fn record(i: u64, prev: Digest) -> CommitRecord {
        CommitRecord {
            txn_id: TxnId(i + 1),
            commit_ts: SystemTimeMicros(1_000 + i64::try_from(i).expect("small i")),
            seq: i + 1,
            prev_hash: prev,
        }
    }

    /// Append `n` chained records and return the running head after each.
    fn append_chain<D: Disk>(disk: &D, n: u64) -> Digest {
        let mut head = Digest::ZERO;
        for i in 0..n {
            let rec = record(i, head);
            append(disk, &rec).expect("append");
            head = rec.hash();
        }
        head
    }

    fn ok_iter(payloads: &[Vec<u8>]) -> Vec<Result<Vec<u8>, stele_storage::wal::WalError>> {
        payloads.iter().cloned().map(Ok).collect()
    }

    #[test]
    fn an_absent_log_replays_empty() {
        let disk = MemDisk::new();
        assert!(replay(&disk).expect("replay").is_empty());
    }

    #[test]
    fn records_round_trip_and_verify_as_a_chain() {
        let disk = MemDisk::new();
        let head = append_chain(&disk, 3);
        let payloads = replay(&disk).expect("replay");
        assert_eq!(payloads.len(), 3);
        // The replayed payloads decode to the chain we wrote, in order...
        let ids: Vec<TxnId> = payloads
            .iter()
            .map(|p| CommitRecord::decode(p).expect("decode").txn_id)
            .collect();
        assert_eq!(ids, vec![TxnId(1), TxnId(2), TxnId(3)]);
        // ...and verify against the STL-178 chain primitive, anchored at the head.
        let verified = verify_chain_to(ok_iter(&payloads), head).expect("chain verifies");
        assert_eq!(verified.len, 3);
        assert_eq!(verified.head, head);
    }

    #[test]
    fn the_first_record_fences_the_directory_entry() {
        // STL-232: the log file's directory entry is fenced at creation — a
        // failed fence fails the append before any record is acknowledged and
        // undoes the create, so a retry re-creates and re-fences rather than
        // acknowledging onto an entry no fence ever vouched for.
        use stele_storage::backend::{FaultOp, Faults};

        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(
            append(&disk, &record(0, Digest::ZERO)).is_err(),
            "fence failure surfaces",
        );
        assert!(replay(&disk).expect("replay").is_empty());

        // The failed create was undone — the retry re-creates and re-fences,
        // consuming a second scheduled fault.
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(
            append(&disk, &record(0, Digest::ZERO)).is_err(),
            "the retry re-fences",
        );
        assert_eq!(faults.pending(), 0);

        // Healthy disk: create + fence + acknowledge; append-path records
        // never re-fence (a pending SyncDir fault stays unconsumed).
        let r0 = record(0, Digest::ZERO);
        append(&disk, &r0).expect("record");
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        append(&disk, &record(1, r0.hash())).expect("append-path record");
        assert_eq!(faults.pending(), 1, "no fence on the append path");
        assert_eq!(replay(&disk).expect("replay").len(), 2);
    }

    #[test]
    fn a_torn_trailing_frame_is_ignored() {
        // A crashed append leaves a partial frame; its fsync never returned, so the
        // commit was never acknowledged — replay keeps the prior records, and the
        // torn transaction recovers as uncommitted.
        let disk = MemDisk::new();
        let r0 = record(0, Digest::ZERO);
        append(&disk, &r0).expect("append");
        let torn = encode_frame(&record(1, r0.hash()));
        let mut file = disk.open(COMMIT_LOG_FILENAME).expect("open");
        file.append(&torn[..torn.len() - 3]).expect("append torn");
        let payloads = replay(&disk).expect("replay");
        assert_eq!(payloads.len(), 1);
        assert_eq!(
            CommitRecord::decode(&payloads[0]).expect("decode").txn_id,
            TxnId(1),
        );
    }

    #[test]
    fn a_zero_filled_tail_is_ignored() {
        let disk = MemDisk::new();
        append(&disk, &record(0, Digest::ZERO)).expect("append");
        let mut file = disk.open(COMMIT_LOG_FILENAME).expect("open");
        file.append(&[0u8; 32]).expect("append zeros");
        assert_eq!(replay(&disk).expect("replay").len(), 1);
    }

    #[test]
    fn a_corrupt_complete_record_fails_closed() {
        // Flip a payload byte inside a complete, previously-fsynced frame: that
        // commit was acknowledged, so replay must refuse rather than silently
        // resurrect or drop a transaction. (A CRC failure — accidental damage —
        // not a chain break; the latter passes the CRC and is caught by
        // verify_chain instead, see `tampering_is_caught_by_verify_chain`.)
        let disk = MemDisk::new();
        append(&disk, &record(0, Digest::ZERO)).expect("append");
        let file = disk.open(COMMIT_LOG_FILENAME).expect("open");
        let len = file.len();
        let mut bytes = vec![0u8; usize::try_from(len).expect("small file")];
        file.read_at(0, &mut bytes).expect("read");
        bytes[HEADER_LEN] ^= 0xFF;
        // MemDisk files are append-only; rebuild the file with the damage.
        disk.remove(COMMIT_LOG_FILENAME).expect("remove");
        let mut rebuilt = disk.create(COMMIT_LOG_FILENAME).expect("create");
        rebuilt.append(&bytes).expect("append");
        let err = replay(&disk).expect_err("corruption must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn tampering_is_caught_by_verify_chain() {
        // Rebuild a historical record's payload so it still frames and CRCs
        // cleanly (a "valid" frame an operator could forge), but its hash no longer
        // matches the next record's `prev_hash`. The CRC-checking `replay` accepts
        // it; the chain check rejects it — the tamper-evidence ADR-0031 wires in.
        let disk = MemDisk::new();
        append_chain(&disk, 3);

        // Read the whole file, swap record 0's payload for a different (well-framed,
        // correctly-CRC'd) record, and rebuild the file.
        let file = disk.open(COMMIT_LOG_FILENAME).expect("open");
        let len = usize::try_from(file.len()).expect("small file");
        let mut bytes = vec![0u8; len];
        file.read_at(0, &mut bytes).expect("read");
        let forged = encode_frame(&CommitRecord {
            txn_id: TxnId(999),
            ..record(0, Digest::ZERO)
        });
        let frame0 = HEADER_LEN + PAYLOAD_LEN + CRC_LEN;
        bytes.splice(0..frame0, forged);
        disk.remove(COMMIT_LOG_FILENAME).expect("remove");
        disk.create(COMMIT_LOG_FILENAME)
            .expect("create")
            .append(&bytes)
            .expect("append");

        // `replay` is happy — every frame is complete and CRCs — but the chain is
        // broken at record 1, whose `prev_hash` still expects the original record 0.
        let payloads = replay(&disk).expect("replay tolerates a well-framed forgery");
        assert_eq!(payloads.len(), 3);
        assert!(
            verify_chain(ok_iter(&payloads)).is_err(),
            "verify_chain detects the tampered historical record",
        );
    }
}
