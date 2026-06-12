//! The durable **commit-marker log** — cross-table transaction atomicity ([STL-215]).
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
//! one marker here naming the transaction, fsynced *after* every per-table record
//! is durable. The marker's fsync is the commit point: on recovery a two-phase
//! record is replayed only if its transaction's marker is present
//! ([`replay`] → [`CommittedTxns`](stele_storage::dml::CommittedTxns)); a crash
//! before the marker discards every leg, so the transaction recovers all-or-none
//! across every table it wrote.
//!
//! A **single-table** `COMMIT` needs no marker — its one record's boundary is
//! already its atomic commit point — so the fast path stays one fsync, untouched.
//!
//! ## Framing and the torn-tail contract
//!
//! The framing mirrors the [catalog log](crate::catalog_log): each record is
//! `magic(4) | payload_len(4 LE) | payload | crc32c(4 LE)` (the CRC covers magic +
//! length + payload), and the same two-shape contract applies. A **torn tail** — a
//! partial trailing frame, or a tail not beginning with the magic — is the debris
//! of a crashed append whose fsync never returned: the marker was never
//! acknowledged, so the transaction recovers as *uncommitted* (all-or-none = none)
//! and replay stops cleanly. **Corruption** — a *complete* frame whose CRC fails —
//! is an acknowledged commit gone bad: replay fails closed rather than silently
//! resurrecting or dropping a transaction.
//!
//! [STL-192]: https://allegromusic.atlassian.net/browse/STL-192
//! [STL-215]: https://allegromusic.atlassian.net/browse/STL-215

use std::collections::BTreeSet;
use std::io;

use stele_common::provenance::TxnId;
use stele_storage::backend::{Disk, DiskFile};
use stele_storage::checksum::crc32c;

/// The canonical commit-marker-log filename on the session's shared disk. A bare
/// path component, like the catalog log; per-table files all carry a `t{idx:020}-`
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

/// One marker's payload: the committing transaction id (`u64` LE).
const PAYLOAD_LEN: usize = 8;

/// `InvalidData` with a context message — the shape every decode failure maps to,
/// so callers can surface one coherent "commit log corrupt" error.
fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Encode one marker as a complete frame: header, the `u64` LE transaction id, and
/// the trailing CRC over everything before it.
fn encode_frame(txn_id: TxnId) -> Vec<u8> {
    let payload = txn_id.0.to_le_bytes();
    let len = u32::try_from(payload.len()).expect("the 8-byte marker payload fits u32");
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len() + CRC_LEN);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&payload);
    let crc = crc32c(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

/// Append one commit marker — "transaction `txn_id` committed" — and **fsync** it.
/// This is the commit point of a multi-table transaction: the caller has already
/// made every per-table two-phase redo record durable, and acknowledges the
/// `COMMIT` only after this returns ([STL-215] write-ahead ordering).
///
/// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
///
/// # Errors
///
/// [`io::Error`] if the log cannot be created/opened, appended, or synced — the
/// commit is refused. A partially appended frame is exactly the torn tail
/// [`replay`] tolerates: the transaction recovers as uncommitted (all-or-none =
/// none), and every per-table leg is discarded.
pub(crate) fn append<D: Disk>(disk: &D, txn_id: TxnId) -> io::Result<()> {
    let frame = encode_frame(txn_id);
    let mut file = match disk.open(COMMIT_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let file = disk.create(COMMIT_LOG_FILENAME)?;
            // Directory fence ([STL-232]): the first marker's all-or-none
            // claim is only as durable as the file's directory entry.
            disk.sync_dir()?;
            file
        }
        Err(e) => return Err(e),
    };
    file.append(&frame)?;
    file.sync()?;
    Ok(())
}

/// Replay the commit-marker log into the set of committed transaction ids. An
/// absent log — a fresh disk, or a session that never ran a multi-table
/// transaction — is the empty set.
///
/// Applies the torn-tail contract from the [module docs](self): a partial trailing
/// frame, or a tail that does not begin with the magic, is the unacknowledged
/// debris of a crashed append and is ignored; a *complete* frame whose CRC fails is
/// corruption of an acknowledged commit and fails closed.
///
/// # Errors
///
/// [`io::Error`] if the file cannot be read, or holds a corrupt (CRC-failing)
/// complete record.
pub(crate) fn replay<D: Disk>(disk: &D) -> io::Result<BTreeSet<TxnId>> {
    let file = match disk.open(COMMIT_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(e),
    };
    let len = file.len();
    let mut committed = BTreeSet::new();
    let mut offset = 0u64;

    loop {
        // Header: magic + payload length. A shorter remainder is a torn tail.
        let mut header = [0u8; HEADER_LEN];
        if offset + (HEADER_LEN as u64) > len || file.read_at(offset, &mut header)? < HEADER_LEN {
            break;
        }
        if header[0..4] != MAGIC {
            // Not a record boundary: the zero/garbage fill of a torn append. Nothing
            // past it was acknowledged (every marker was fsynced before the next),
            // so stop.
            break;
        }
        let payload_len = u64::from(u32::from_le_bytes(
            header[4..8].try_into().expect("4 bytes"),
        ));
        let frame_len = (HEADER_LEN as u64) + payload_len + (CRC_LEN as u64);
        if offset + frame_len > len {
            break; // torn tail: the marker's fsync never completed
        }

        // The frame is complete, so it was acknowledged — from here on, damage is
        // corruption and fails closed. A marker payload is the fixed `PAYLOAD_LEN`;
        // validate the on-disk length against it *before* allocating, so a corrupt
        // length field cannot drive a large allocation during recovery.
        let payload_bytes = usize::try_from(payload_len)
            .map_err(|_| corrupt("commit log: record too large for this platform"))?;
        if payload_bytes != PAYLOAD_LEN {
            return Err(corrupt(
                "commit log: a complete marker is not the fixed-size payload — corrupt",
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
        let id: [u8; PAYLOAD_LEN] = payload.try_into().expect("validated PAYLOAD_LEN bytes");
        committed.insert(TxnId(u64::from_le_bytes(id)));
        offset += frame_len;
    }
    Ok(committed)
}

#[cfg(test)]
mod tests {
    use super::*;

    use stele_storage::backend::MemDisk;

    #[test]
    fn an_absent_log_replays_empty() {
        let disk = MemDisk::new();
        assert_eq!(replay(&disk).expect("replay"), BTreeSet::new());
    }

    #[test]
    fn markers_round_trip_in_commit_order() {
        let disk = MemDisk::new();
        for id in [3u64, 7, 42] {
            append(&disk, TxnId(id)).expect("append");
        }
        let committed = replay(&disk).expect("replay");
        assert_eq!(
            committed,
            [TxnId(3), TxnId(7), TxnId(42)].into_iter().collect()
        );
    }

    #[test]
    fn the_first_marker_fences_the_directory_entry() {
        // STL-232: the log file's directory entry is fenced at creation — a
        // failed fence fails the append before any marker is acknowledged.
        use stele_storage::backend::{FaultOp, Faults};

        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(append(&disk, TxnId(1)).is_err(), "fence failure surfaces");
        assert_eq!(replay(&disk).expect("replay"), BTreeSet::new());

        // The file exists now; append-path markers never re-fence.
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        append(&disk, TxnId(1)).expect("append-path marker");
        assert_eq!(faults.pending(), 1, "no fence on the append path");
        assert_eq!(replay(&disk).expect("replay"), [TxnId(1)].into());
    }

    #[test]
    fn a_torn_trailing_frame_is_ignored() {
        // A crashed append leaves a partial frame; its fsync never returned, so the
        // commit was never acknowledged — replay keeps the prior markers, and the
        // torn transaction recovers as uncommitted.
        let disk = MemDisk::new();
        append(&disk, TxnId(1)).expect("append");
        let torn = encode_frame(TxnId(2));
        let mut file = disk.open(COMMIT_LOG_FILENAME).expect("open");
        file.append(&torn[..torn.len() - 3]).expect("append torn");
        assert_eq!(
            replay(&disk).expect("replay"),
            std::iter::once(TxnId(1)).collect()
        );
    }

    #[test]
    fn a_zero_filled_tail_is_ignored() {
        let disk = MemDisk::new();
        append(&disk, TxnId(1)).expect("append");
        let mut file = disk.open(COMMIT_LOG_FILENAME).expect("open");
        file.append(&[0u8; 32]).expect("append zeros");
        assert_eq!(
            replay(&disk).expect("replay"),
            std::iter::once(TxnId(1)).collect()
        );
    }

    #[test]
    fn a_corrupt_complete_record_fails_closed() {
        // Flip a payload byte inside a complete, previously-fsynced frame: that
        // commit was acknowledged, so replay must refuse rather than silently
        // resurrect or drop a transaction.
        let disk = MemDisk::new();
        append(&disk, TxnId(1)).expect("append");
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
}
