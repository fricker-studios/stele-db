//! The recovery checkpoint — a small file recording the last [`RecoveryPoint`]:
//! where WAL replay may safely resume, how far the WAL is durable, and how many
//! sealed segments the flush has committed.
//!
//! On boot the [`Engine`](crate::engine::Engine) must know three things the
//! recovery flow of [architecture §3.6](../../../docs/02-architecture.md#36-crash-recovery)
//! depends on:
//!
//! * **`replay_floor`** — the WAL offset replay starts from. Everything *before*
//!   it has been flushed into committed sealed segments ([`Engine::flush`](crate::engine::Engine::flush),
//!   [STL-177]), so recovery rebuilds that prefix from the segment store and need
//!   only replay the **tail**. Before the first flush this is the log origin and
//!   recovery replays the whole WAL ([STL-102]).
//! * **`durable_fence`** — the last fully-flushed/`fsync`'d WAL offset. It tells a
//!   *committed* write (durable before the fence) from the *unsynced tail* a
//!   mid-write crash may have torn, gating torn-tail tolerance
//!   ([`crate::dml::recover_replay`]).
//! * **`segment_count`** — how many sealed segments (`seg-0 … seg-{count-1}`) the
//!   committed flushes have produced. A segment with a higher index is an
//!   **orphan** — written by a flush whose checkpoint record never became durable
//!   — and recovery ignores it, falling back to the WAL. This is what makes a
//!   crash *during* a flush safe: the checkpoint record is the atomic commit
//!   point ([STL-177] DoD).
//!
//! ## Append-only, self-checksummed
//!
//! A [`Disk`] file is append-only — there is no in-place overwrite
//! ([`crate::backend`]) — so the checkpoint is an **append-only log of fixed
//! [`RECORD_LEN`]-byte records**, each a magic + the [`RecoveryPoint`] fields +
//! a CRC32C over them. [`load`] scans the file and returns the **last
//! CRC-valid record**, stopping at the first malformed one: a checkpoint write
//! that was itself torn by the crash is simply ignored, falling back to the
//! prior good checkpoint (or [`None`] — replay from the beginning — when none
//! survives). The file therefore needs no rotation for correctness; it grows
//! one tiny record per checkpoint, and trimming it is a noted follow-up.
//!
//! Like the validity index, the checkpoint is **derived, never authoritative**:
//! losing it only costs a longer replay (from the beginning of the WAL) and the
//! re-flush of any uncommitted segment, never correctness — the WAL is the source
//! of truth ([ADR-0023]).

use std::io;

use crate::backend::{Disk, DiskFile};
use crate::checksum::crc32c;
use crate::wal::LogOffset;

/// A durable recovery point: where replay resumes, the durable boundary, and the
/// committed sealed-segment count. See the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveryPoint {
    /// The WAL offset replay starts from — everything before it is durable in the
    /// `segment_count` committed segments.
    pub replay_floor: LogOffset,
    /// The last `fsync`'d WAL offset — the committed/unsynced boundary the
    /// torn-tail gate uses.
    pub durable_fence: LogOffset,
    /// How many sealed segments (`seg-0 … seg-{count-1}`) committed flushes have
    /// produced; segments at a higher index are uncommitted orphans.
    pub segment_count: u64,
}

/// The canonical checkpoint filename on the engine's data disk. A single normal
/// path component, disjoint from every other namespace on the disk (`wal-*.log`,
/// `delta-spill-*.row`, `validity-spill-*.row`, segments).
pub(crate) const CHECKPOINT_FILENAME: &str = "stele.checkpoint";

/// Four-byte record magic — `b"STCK"`. Distinguishes a checkpoint record from a
/// zeroed or foreign tail and is folded into the CRC.
const MAGIC: [u8; 4] = *b"STCK";

/// One checkpoint record: `magic(4) | replay_floor(16) | durable_fence(16) |
/// segment_count(8 LE) | crc32c(4 LE)`, where each `LogOffset` is
/// `segment_index(8 LE) || byte_offset(8 LE)`. Fixed width so [`load`] can scan
/// the file in record-sized strides and detect a torn trailing record by its
/// short length.
const RECORD_LEN: usize = 4 + 16 + 16 + 8 + 4;

/// Byte offset of the trailing CRC32C — everything before it is covered.
const CRC_OFFSET: usize = RECORD_LEN - 4;

/// Encode one [`LogOffset`] as `segment_index(8 LE) || byte_offset(8 LE)` into
/// `buf` at `at`.
fn put_offset(buf: &mut [u8], at: usize, offset: LogOffset) {
    buf[at..at + 8].copy_from_slice(&offset.segment_index.to_le_bytes());
    buf[at + 8..at + 16].copy_from_slice(&offset.byte_offset.to_le_bytes());
}

/// Decode one [`LogOffset`] from `buf` at `at`.
fn get_offset(buf: &[u8], at: usize) -> LogOffset {
    LogOffset {
        segment_index: u64::from_le_bytes(buf[at..at + 8].try_into().expect("8 bytes")),
        byte_offset: u64::from_le_bytes(buf[at + 8..at + 16].try_into().expect("8 bytes")),
    }
}

/// Encode `point` as one checkpoint record (magic + fields + trailing CRC32C
/// over the leading `magic + fields`).
fn encode(point: RecoveryPoint) -> [u8; RECORD_LEN] {
    let mut buf = [0u8; RECORD_LEN];
    buf[0..4].copy_from_slice(&MAGIC);
    put_offset(&mut buf, 4, point.replay_floor);
    put_offset(&mut buf, 20, point.durable_fence);
    buf[36..44].copy_from_slice(&point.segment_count.to_le_bytes());
    let crc = crc32c(&buf[0..CRC_OFFSET]);
    buf[CRC_OFFSET..RECORD_LEN].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode one record, returning the [`RecoveryPoint`] only if the magic matches
/// and the CRC verifies. A wrong magic or a failed CRC — a torn write — yields
/// [`None`].
fn decode(buf: &[u8; RECORD_LEN]) -> Option<RecoveryPoint> {
    if buf[0..4] != MAGIC {
        return None;
    }
    let stored = u32::from_le_bytes(buf[CRC_OFFSET..RECORD_LEN].try_into().expect("4 bytes"));
    if crc32c(&buf[0..CRC_OFFSET]) != stored {
        return None;
    }
    Some(RecoveryPoint {
        replay_floor: get_offset(buf, 4),
        durable_fence: get_offset(buf, 20),
        segment_count: u64::from_le_bytes(buf[36..44].try_into().expect("8 bytes")),
    })
}

/// Append a checkpoint record for `point` and fsync it.
///
/// The fsync is the durability point: once this returns, the recorded point
/// survives a crash, so [`load`] on the next boot will see it. Appending (rather
/// than overwriting) keeps the prior good record intact until this one is
/// durable, so a torn append never destroys the last known-good checkpoint.
///
/// # Errors
///
/// [`io::Error`] if the checkpoint file cannot be created/opened, appended, or
/// synced.
pub(crate) fn store<D: Disk>(disk: &D, point: RecoveryPoint) -> io::Result<()> {
    let record = encode(point);
    let mut file = match disk.open(CHECKPOINT_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let file = disk.create(CHECKPOINT_FILENAME)?;
            // Directory fence ([STL-232]): the first checkpoint's claim is
            // only as durable as the file's directory entry.
            disk.sync_dir()?;
            file
        }
        Err(e) => return Err(e),
    };
    file.append(&record)?;
    file.sync()?;
    Ok(())
}

/// Return the last CRC-valid [`RecoveryPoint`] on `disk`, or [`None`] if the file
/// is absent, empty, or holds no intact record.
///
/// Scans the append-only file one [`RECORD_LEN`]-byte record at a time with
/// `read_at` — bounded memory regardless of how long the file has grown — and
/// keeps the last record that decodes; the scan stops at the first record that
/// fails to decode (a torn trailing write) or at a short final record. [`None`]
/// means "no durable checkpoint" — the caller replays the WAL from the beginning
/// and trusts no sealed segment, which is always correct ([ADR-0023]).
///
/// # Errors
///
/// [`io::Error`] if the file exists but cannot be read.
pub(crate) fn load<D: Disk>(disk: &D) -> io::Result<Option<RecoveryPoint>> {
    let file = match disk.open(CHECKPOINT_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let len = file.len();
    let record_len = RECORD_LEN as u64;

    let mut last = None;
    let mut offset = 0u64;
    let mut record = [0u8; RECORD_LEN];
    // Only whole records are read; a trailing partial record (the torn tail of a
    // crashed append) is left unread, exactly like the prior chunked scan.
    while offset + record_len <= len {
        let read = file.read_at(offset, &mut record)?;
        if read < RECORD_LEN {
            break; // short read at the tail — treat as a torn final record
        }
        match decode(&record) {
            Some(found) => last = Some(found),
            None => break, // a corrupt record — stop; nothing after it is trustworthy
        }
        offset += record_len;
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemDisk;

    const fn offset(segment_index: u64, byte_offset: u64) -> LogOffset {
        LogOffset {
            segment_index,
            byte_offset,
        }
    }

    /// A recovery point with `floor`/`fence` and a `count` of committed segments.
    const fn point(floor: LogOffset, fence: LogOffset, count: u64) -> RecoveryPoint {
        RecoveryPoint {
            replay_floor: floor,
            durable_fence: fence,
            segment_count: count,
        }
    }

    #[test]
    fn absent_checkpoint_loads_none() {
        let disk = MemDisk::new();
        assert_eq!(load(&disk).expect("load"), None);
    }

    #[test]
    fn store_then_load_round_trips() {
        // All three fields — floor, fence, and the committed segment count — must
        // survive the round-trip; a flush advances the floor past the segments it
        // sealed and bumps the count.
        let disk = MemDisk::new();
        let p = point(offset(3, 4096), offset(3, 8192), 7);
        store(&disk, p).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(p));
    }

    #[test]
    fn the_last_appended_checkpoint_wins() {
        // Each periodic checkpoint appends a record; recovery uses the newest.
        let disk = MemDisk::new();
        store(&disk, point(offset(0, 0), offset(0, 100), 0)).expect("store");
        store(&disk, point(offset(0, 0), offset(0, 200), 0)).expect("store");
        let newest = point(offset(0, 200), offset(1, 50), 1);
        store(&disk, newest).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(newest));
    }

    #[test]
    fn the_first_store_fences_the_directory_and_later_stores_do_not() {
        // STL-232: the file's directory entry is fenced exactly once, at
        // creation — a failed fence fails the first checkpoint before anything
        // is acknowledged; append-path stores never consult the fence again.
        use crate::backend::{FaultOp, Faults};

        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let p = point(offset(0, 0), offset(0, 10), 0);

        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(
            store(&disk, p).is_err(),
            "a failed creation fence fails the checkpoint"
        );
        assert_eq!(load(&disk).expect("load"), None, "nothing acknowledged");

        // The file exists now; later stores append without re-fencing, so a
        // pending SyncDir fault is never consumed.
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        store(&disk, p).expect("append-path store");
        assert_eq!(faults.pending(), 1, "no fence on the append path");
        assert_eq!(load(&disk).expect("load"), Some(p));
    }

    #[test]
    fn a_torn_trailing_record_falls_back_to_the_prior_good_one() {
        // A crash mid-append leaves a short, partial final record. The prior
        // fully-written checkpoint must still be recovered.
        let disk = MemDisk::new();
        let good = point(offset(2, 64), offset(2, 64), 1);
        store(&disk, good).expect("store");
        // Simulate a torn append: a few stray bytes with no valid record.
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        file.append(&[0xAB, 0xCD, 0xEF]).expect("append partial");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(good));
    }

    #[test]
    fn a_corrupt_record_body_falls_back_to_the_prior_good_one() {
        // A full-length record whose CRC fails (bit-rot / torn full write) must
        // be rejected, and the prior good checkpoint recovered.
        let disk = MemDisk::new();
        let good = point(offset(5, 5), offset(5, 5), 2);
        store(&disk, good).expect("store");
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        let mut bad = encode(point(offset(9, 9), offset(9, 9), 3));
        bad[12] ^= 0xFF; // flip a field byte without fixing the CRC
        file.append(&bad).expect("append corrupt");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(good));
    }

    #[test]
    fn a_torn_manifest_record_drops_the_segment_it_vouched() {
        // The crash-during-flush shape: a flush sealed a segment and appended its
        // checkpoint record, but the append was torn. `load` falls back to the
        // prior record, whose lower `segment_count` makes recovery treat the new
        // segment as an uncommitted orphan ([STL-177]).
        let disk = MemDisk::new();
        let committed = point(offset(0, 0), offset(0, 500), 1);
        store(&disk, committed).expect("store");
        // The torn flush record that would have vouched seg-1 and advanced the floor.
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        let torn = encode(point(offset(0, 500), offset(0, 900), 2));
        file.append(&torn[..RECORD_LEN - 5]).expect("append torn");
        file.sync().expect("sync");
        assert_eq!(
            load(&disk).expect("load"),
            Some(committed),
            "the prior committed manifest survives a torn flush record",
        );
    }

    #[test]
    fn an_empty_file_loads_none() {
        let disk = MemDisk::new();
        let mut file = disk.create(CHECKPOINT_FILENAME).expect("create");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), None);
    }
}
