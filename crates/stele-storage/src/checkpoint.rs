//! The recovery checkpoint / **segment manifest** — a small append-only file
//! recording the last [`RecoveryPoint`]: where WAL replay may safely resume, how
//! far the WAL is durable, and **which sealed segments are live** ([ADR-0030]).
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
//! * **`live_segments`** — the explicit list of sealed-segment indexes the
//!   committed flushes and compactions have produced ([ADR-0030], [STL-231]).
//!   A `seg-*` file whose index is **not** in the list is dead — either an
//!   **orphan** (written by a flush/compaction whose manifest record never
//!   became durable, [STL-177]) or a **retired** compaction input whose
//!   post-commit cleanup was interrupted — and recovery removes it, falling
//!   back to the WAL for anything an orphan held. This is what makes a crash
//!   *during* a flush or a compaction safe: the manifest record is the atomic
//!   commit point for every segment-set transition.
//!
//! ## Append-only, self-checksummed
//!
//! A [`Disk`] file is append-only — there is no in-place overwrite
//! ([`crate::backend`]) — so the manifest is an **append-only log of records**,
//! each a magic + the [`RecoveryPoint`] fields + a CRC32C over them. [`load`]
//! scans the file and returns the **last CRC-valid record**, stopping at the
//! first malformed one: a record whose append was itself torn by the crash is
//! simply ignored, falling back to the prior good record (or [`None`] — replay
//! from the beginning — when none survives). The file therefore needs no
//! rotation for correctness; it grows one small record per transition, and
//! trimming it is a noted follow-up.
//!
//! Two record formats coexist in one file ([ADR-0030]): the legacy fixed-length
//! `STCK` record (`{floor, fence, segment_count}`, the live set implicitly the
//! contiguous prefix `seg-0 … seg-{count-1}`) and the current variable-length
//! `STMF` record carrying the live list explicitly. [`load`] dispatches on the
//! per-record magic, so a v0.2 data directory boots unchanged and is upgraded
//! by the first new record [`store`] appends.
//!
//! Like the validity index, the manifest is **derived, never authoritative**:
//! losing it only costs a longer replay (from the beginning of the WAL) and the
//! re-flush of any uncommitted segment, never correctness — the WAL is the source
//! of truth ([ADR-0023]).
//!
//! [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
//! [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md

use std::io;

use crate::backend::{Disk, DiskFile};
use crate::checksum::crc32c;
use crate::wal::LogOffset;

/// A durable recovery point: where replay resumes, the durable boundary, and the
/// live sealed-segment set. See the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryPoint {
    /// The WAL offset replay starts from — everything before it is durable in the
    /// live sealed segments.
    pub replay_floor: LogOffset,
    /// The last `fsync`'d WAL offset — the committed/unsynced boundary the
    /// torn-tail gate uses.
    pub durable_fence: LogOffset,
    /// The indexes of the **live** sealed segments (`seg-{idx}`), ascending —
    /// the segment manifest ([ADR-0030]). A segment file whose index is absent
    /// is dead (orphan or retired) and recovery removes it.
    ///
    /// [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md
    pub live_segments: Vec<u64>,
}

/// The canonical checkpoint/manifest filename on the engine's data disk. A
/// single normal path component, disjoint from every other namespace on the
/// disk (`wal-*.log`, `delta-spill-*.row`, `validity-spill-*.row`, segments).
pub(crate) const CHECKPOINT_FILENAME: &str = "stele.checkpoint";

/// Four-byte magic of the **legacy** fixed-length record — `b"STCK"`
/// (`{floor, fence, segment_count}`, the live set implicitly `[0, count)`).
/// Decoded for compatibility with pre-[ADR-0030] data directories; never
/// written anymore.
///
/// [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md
const LEGACY_MAGIC: [u8; 4] = *b"STCK";

/// Four-byte magic of the current **manifest** record — `b"STMF"` — carrying
/// the explicit live-segment list ([ADR-0030]). Distinguishes a record from a
/// zeroed or foreign tail (and from a legacy record) and is folded into the CRC.
///
/// [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md
const MAGIC: [u8; 4] = *b"STMF";

/// One legacy record: `magic(4) | replay_floor(16) | durable_fence(16) |
/// segment_count(8 LE) | crc32c(4 LE)`, where each `LogOffset` is
/// `segment_index(8 LE) || byte_offset(8 LE)`.
const LEGACY_RECORD_LEN: usize = 4 + 16 + 16 + 8 + 4;

/// The fixed prefix of a manifest record: `magic(4) | replay_floor(16) |
/// durable_fence(16) | live_count(4 LE u32)`. The `live_count × 8`-byte index
/// list and the trailing CRC32C follow.
const HEADER_LEN: usize = 4 + 16 + 16 + 4;

/// Ceiling on a decoded `live_count`, so a corrupt length field cannot drive an
/// absurd allocation before the CRC gets a chance to reject the record. A live
/// set this large would mean a million-segment table that has never compacted —
/// far beyond anything the resident-segment model serves ([ADR-0030] cost note).
///
/// [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md
const MAX_LIVE_SEGMENTS: u32 = 1 << 20;

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

/// Encode `point` as one manifest record (magic + fields + live list + trailing
/// CRC32C over everything before it).
fn encode(point: &RecoveryPoint) -> Vec<u8> {
    let count = u32::try_from(point.live_segments.len())
        .expect("live-segment count exceeds u32 — far beyond MAX_LIVE_SEGMENTS");
    let len = HEADER_LEN + point.live_segments.len() * 8 + 4;
    let mut buf = vec![0u8; len];
    buf[0..4].copy_from_slice(&MAGIC);
    put_offset(&mut buf, 4, point.replay_floor);
    put_offset(&mut buf, 20, point.durable_fence);
    buf[36..40].copy_from_slice(&count.to_le_bytes());
    for (i, idx) in point.live_segments.iter().enumerate() {
        let at = HEADER_LEN + i * 8;
        buf[at..at + 8].copy_from_slice(&idx.to_le_bytes());
    }
    let crc_offset = len - 4;
    let crc = crc32c(&buf[0..crc_offset]);
    buf[crc_offset..len].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode one legacy `STCK` record body (everything after the magic), returning
/// the [`RecoveryPoint`] with the implicit contiguous live set `[0, count)`,
/// or [`None`] if the CRC fails.
fn decode_legacy(buf: &[u8; LEGACY_RECORD_LEN]) -> Option<RecoveryPoint> {
    let crc_offset = LEGACY_RECORD_LEN - 4;
    let stored = u32::from_le_bytes(buf[crc_offset..].try_into().expect("4 bytes"));
    if crc32c(&buf[0..crc_offset]) != stored {
        return None;
    }
    let segment_count = u64::from_le_bytes(buf[36..44].try_into().expect("8 bytes"));
    Some(RecoveryPoint {
        replay_floor: get_offset(buf, 4),
        durable_fence: get_offset(buf, 20),
        live_segments: (0..segment_count).collect(),
    })
}

/// Decode one whole manifest record (header + index list + trailing CRC), or
/// [`None`] if the CRC fails.
fn decode(record: &[u8]) -> Option<RecoveryPoint> {
    let crc_offset = record.len() - 4;
    let stored = u32::from_le_bytes(record[crc_offset..].try_into().expect("4 bytes"));
    if crc32c(&record[..crc_offset]) != stored {
        return None;
    }
    let live_segments = record[HEADER_LEN..crc_offset]
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().expect("8 bytes")))
        .collect();
    Some(RecoveryPoint {
        replay_floor: get_offset(record, 4),
        durable_fence: get_offset(record, 20),
        live_segments,
    })
}

/// Append a manifest record for `point` and fsync it.
///
/// The fsync is the durability point: once this returns, the recorded point —
/// including the live segment set it vouches — survives a crash, so [`load`] on
/// the next boot will see it. Appending (rather than overwriting) keeps the
/// prior good record intact until this one is durable, so a torn append never
/// destroys the last known-good record. This single append is the atomic commit
/// point for every segment-set transition ([ADR-0030]): a flush appends a record
/// whose list gains the new segment; a compaction appends one whose list names
/// the outputs instead of the inputs.
///
/// [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md
///
/// # Errors
///
/// [`io::Error`] if the checkpoint file cannot be created/opened, appended, or
/// synced.
pub(crate) fn store<D: Disk>(disk: &D, point: &RecoveryPoint) -> io::Result<()> {
    let record = encode(point);
    let mut file = match disk.open(CHECKPOINT_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let file = disk.create(CHECKPOINT_FILENAME)?;
            // Directory fence ([STL-232]): the first checkpoint's claim is
            // only as durable as the file's directory entry. On fence failure,
            // undo the create (best-effort) so a retry re-creates and
            // re-fences — otherwise the retry would take the `open` path,
            // which never fences, and could claim durability for an entry no
            // fence ever vouched for.
            if let Err(e) = disk.sync_dir() {
                drop(file);
                let _ = disk.remove(CHECKPOINT_FILENAME);
                return Err(e);
            }
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
/// Scans the append-only file record by record with `read_at` — bounded memory
/// regardless of how long the file has grown — dispatching on each record's
/// magic (`STMF` manifest, legacy `STCK`) and keeping the last record that
/// decodes; the scan stops at the first record that fails to decode (a torn
/// trailing write) or at a short final record. [`None`] means "no durable
/// checkpoint" — the caller replays the WAL from the beginning and trusts no
/// sealed segment, which is always correct ([ADR-0023]).
///
/// [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
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

    let mut last = None;
    let mut offset = 0u64;
    // Only whole records are read; a trailing partial record (the torn tail of a
    // crashed append) is left unread, falling back to the prior good record.
    loop {
        let mut header = [0u8; HEADER_LEN];
        if offset + HEADER_LEN as u64 > len || file.read_at(offset, &mut header)? < HEADER_LEN {
            break; // short tail — no further whole record
        }
        let decoded = if header[0..4] == LEGACY_MAGIC {
            // Legacy fixed-length record: re-read it whole (its layout puts the
            // count where the manifest header ends, so the header bytes alone
            // do not cover it).
            let mut record = [0u8; LEGACY_RECORD_LEN];
            if offset + LEGACY_RECORD_LEN as u64 > len
                || file.read_at(offset, &mut record)? < LEGACY_RECORD_LEN
            {
                break;
            }
            offset += LEGACY_RECORD_LEN as u64;
            decode_legacy(&record)
        } else if header[0..4] == MAGIC {
            let count = u32::from_le_bytes(header[36..40].try_into().expect("4 bytes"));
            if count > MAX_LIVE_SEGMENTS {
                break; // implausible length — corrupt record, stop the scan
            }
            let record_len = HEADER_LEN + count as usize * 8 + 4;
            let mut record = vec![0u8; record_len];
            if offset + record_len as u64 > len || file.read_at(offset, &mut record)? < record_len {
                break; // torn mid-record
            }
            offset += record_len as u64;
            decode(&record)
        } else {
            break; // foreign bytes — nothing after them is trustworthy
        };
        match decoded {
            Some(found) => last = Some(found),
            None => break, // a corrupt record — stop; nothing after it is trustworthy
        }
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

    /// A recovery point with `floor`/`fence` and an explicit `live` segment set.
    fn point(floor: LogOffset, fence: LogOffset, live: &[u64]) -> RecoveryPoint {
        RecoveryPoint {
            replay_floor: floor,
            durable_fence: fence,
            live_segments: live.to_vec(),
        }
    }

    /// Encode a **legacy** `STCK` record, as a pre-ADR-0030 binary would have
    /// written it — the compatibility fixture for the upgrade tests.
    fn encode_legacy(floor: LogOffset, fence: LogOffset, segment_count: u64) -> Vec<u8> {
        let mut buf = vec![0u8; LEGACY_RECORD_LEN];
        buf[0..4].copy_from_slice(&LEGACY_MAGIC);
        put_offset(&mut buf, 4, floor);
        put_offset(&mut buf, 20, fence);
        buf[36..44].copy_from_slice(&segment_count.to_le_bytes());
        let crc_offset = LEGACY_RECORD_LEN - 4;
        let crc = crc32c(&buf[0..crc_offset]);
        buf[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn absent_checkpoint_loads_none() {
        let disk = MemDisk::new();
        assert_eq!(load(&disk).expect("load"), None);
    }

    #[test]
    fn store_then_load_round_trips() {
        // All three fields — floor, fence, and the live segment list — must
        // survive the round-trip; a flush advances the floor past the segments it
        // sealed and appends to the list, a compaction swaps the list.
        let disk = MemDisk::new();
        let p = point(offset(3, 4096), offset(3, 8192), &[0, 1, 2, 5, 9]);
        store(&disk, &p).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(p));
    }

    #[test]
    fn an_empty_live_set_round_trips() {
        // Before the first flush a checkpoint vouches no segment at all.
        let disk = MemDisk::new();
        let p = point(offset(0, 0), offset(0, 64), &[]);
        store(&disk, &p).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(p));
    }

    #[test]
    fn the_last_appended_checkpoint_wins() {
        // Each transition appends a record; recovery uses the newest. The
        // compaction shape — a later record *replacing* earlier indexes with one
        // consolidated output — must win exactly like a flush append does.
        let disk = MemDisk::new();
        store(&disk, &point(offset(0, 0), offset(0, 100), &[])).expect("store");
        store(&disk, &point(offset(0, 100), offset(0, 200), &[0, 1])).expect("store");
        let compacted = point(offset(0, 100), offset(0, 250), &[2]);
        store(&disk, &compacted).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(compacted));
    }

    #[test]
    fn a_legacy_record_decodes_as_the_contiguous_prefix() {
        // ADR-0030 compatibility: a v0.2 data dir holds only fixed-length STCK
        // records; their count means live = seg-0 … seg-{count-1}.
        let disk = MemDisk::new();
        let mut file = disk.create(CHECKPOINT_FILENAME).expect("create");
        file.append(&encode_legacy(offset(1, 10), offset(1, 20), 3))
            .expect("append");
        file.sync().expect("sync");
        assert_eq!(
            load(&disk).expect("load"),
            Some(point(offset(1, 10), offset(1, 20), &[0, 1, 2])),
        );
    }

    #[test]
    fn a_manifest_record_appended_after_legacy_records_wins() {
        // The upgrade path: old STCK records followed by the first STMF record
        // the new binary appends — mixed formats in one file, newest wins.
        let disk = MemDisk::new();
        let mut file = disk.create(CHECKPOINT_FILENAME).expect("create");
        file.append(&encode_legacy(offset(0, 0), offset(0, 100), 2))
            .expect("append");
        file.sync().expect("sync");
        drop(file);
        let upgraded = point(offset(0, 100), offset(0, 300), &[4]);
        store(&disk, &upgraded).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(upgraded));
    }

    #[test]
    fn the_creating_store_fences_the_directory_and_append_stores_do_not() {
        // STL-232: the file's directory entry is fenced at creation — a failed
        // fence fails the checkpoint before anything is acknowledged AND undoes
        // the create, so a retry re-creates and re-fences rather than slipping
        // through the fence-free append path on an unvouched entry.
        use crate::backend::{Disk as _, FaultOp, Faults};

        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let p = point(offset(0, 0), offset(0, 10), &[]);

        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(
            store(&disk, &p).is_err(),
            "a failed creation fence fails the checkpoint"
        );
        assert_eq!(load(&disk).expect("load"), None, "nothing acknowledged");
        assert!(
            disk.list().expect("list").is_empty(),
            "the failed create was undone — no unfenced entry lingers"
        );

        // Because the create was undone, a retry goes through create + fence
        // again — a second scheduled fault fails it again (and is consumed).
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(store(&disk, &p).is_err(), "the retry re-fences");
        assert_eq!(faults.pending(), 0);

        // Healthy disk: the store creates, fences, appends, and is loadable.
        store(&disk, &p).expect("store");
        // Later stores append without re-fencing, so a pending SyncDir fault
        // is never consumed.
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        store(&disk, &p).expect("append-path store");
        assert_eq!(faults.pending(), 1, "no fence on the append path");
        assert_eq!(load(&disk).expect("load"), Some(p));
    }

    #[test]
    fn a_torn_trailing_record_falls_back_to_the_prior_good_one() {
        // A crash mid-append leaves a short, partial final record. The prior
        // fully-written checkpoint must still be recovered.
        let disk = MemDisk::new();
        let good = point(offset(2, 64), offset(2, 64), &[0]);
        store(&disk, &good).expect("store");
        // Simulate a torn append: a few stray bytes with no valid record.
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        file.append(&[0xAB, 0xCD, 0xEF]).expect("append partial");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(good));
    }

    #[test]
    fn a_record_torn_inside_its_live_list_falls_back() {
        // A manifest record torn *after* its fixed header — the header (and its
        // plausible count) is intact, the index list is short. The scan must not
        // trust the half-written list.
        let disk = MemDisk::new();
        let good = point(offset(2, 64), offset(2, 64), &[0, 1]);
        store(&disk, &good).expect("store");
        let torn = encode(&point(offset(2, 128), offset(2, 128), &[0, 1, 2]));
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        file.append(&torn[..torn.len() - 7]).expect("append torn");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(good));
    }

    #[test]
    fn a_corrupt_record_body_falls_back_to_the_prior_good_one() {
        // A full-length record whose CRC fails (bit-rot / torn full write) must
        // be rejected, and the prior good checkpoint recovered.
        let disk = MemDisk::new();
        let good = point(offset(5, 5), offset(5, 5), &[0, 1]);
        store(&disk, &good).expect("store");
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        let mut bad = encode(&point(offset(9, 9), offset(9, 9), &[0, 1, 2]));
        bad[12] ^= 0xFF; // flip a field byte without fixing the CRC
        file.append(&bad).expect("append corrupt");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(good));
    }

    #[test]
    fn an_implausible_live_count_stops_the_scan() {
        // A corrupt count field must not drive a huge allocation; the record is
        // rejected by the plausibility bound before its CRC is even read.
        let disk = MemDisk::new();
        let good = point(offset(1, 1), offset(1, 1), &[0]);
        store(&disk, &good).expect("store");
        let mut bad = encode(&point(offset(2, 2), offset(2, 2), &[0]));
        bad[36..40].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        file.append(&bad).expect("append corrupt");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(good));
    }

    #[test]
    fn a_torn_manifest_record_drops_the_segments_it_vouched() {
        // The crash-during-flush/compaction shape: the operation sealed its
        // output and appended its manifest record, but the append was torn.
        // `load` falls back to the prior record, whose live list makes recovery
        // treat the output as a dead orphan ([STL-177], [ADR-0030]).
        let disk = MemDisk::new();
        let committed = point(offset(0, 0), offset(0, 500), &[0]);
        store(&disk, &committed).expect("store");
        // The torn record that would have vouched seg-1 and advanced the floor.
        let torn = encode(&point(offset(0, 500), offset(0, 900), &[0, 1]));
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        file.append(&torn[..torn.len() - 5]).expect("append torn");
        file.sync().expect("sync");
        assert_eq!(
            load(&disk).expect("load"),
            Some(committed),
            "the prior committed manifest survives a torn record",
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
