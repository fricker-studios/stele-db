//! The recovery checkpoint — a small file pointing at the last fully-flushed
//! WAL offset.
//!
//! On boot the [`Engine`](crate::engine::Engine) must know how far the WAL was
//! confirmed durable at the last clean checkpoint, so it can tell a *committed*
//! write (durable before the checkpoint) from the *unsynced tail* a mid-write
//! crash may have torn ([architecture §3.6](../../../docs/02-architecture.md#36-crash-recovery)).
//! This module persists that boundary: a [`LogOffset`] written periodically and
//! on graceful shutdown ([STL-102]).
//!
//! ## Append-only, self-checksummed
//!
//! A [`Disk`] file is append-only — there is no in-place overwrite
//! ([`crate::backend`]) — so the checkpoint is an **append-only log of fixed
//! [`RECORD_LEN`]-byte records**, each a magic + the two `LogOffset` fields +
//! a CRC32C over them. [`load`] scans the file and returns the **last
//! CRC-valid record**, stopping at the first malformed one: a checkpoint write
//! that was itself torn by the crash is simply ignored, falling back to the
//! prior good checkpoint (or [`None`] — replay from the beginning — when none
//! survives). The file therefore needs no rotation for correctness; it grows
//! one tiny record per checkpoint, and trimming it is a noted follow-up.
//!
//! Like the validity index, the checkpoint is **derived, never authoritative**:
//! losing it only costs a longer replay (from the beginning of the WAL), never
//! correctness — the WAL is the source of truth ([ADR-0023]).

use std::io;

use crate::backend::{Disk, DiskFile};
use crate::checksum::crc32c;
use crate::wal::LogOffset;

/// The canonical checkpoint filename on the engine's data disk. A single normal
/// path component, disjoint from every other namespace on the disk (`wal-*.log`,
/// `delta-spill-*.row`, `validity-spill-*.row`, segments).
pub(crate) const CHECKPOINT_FILENAME: &str = "stele.checkpoint";

/// Four-byte record magic — `b"STCK"`. Distinguishes a checkpoint record from a
/// zeroed or foreign tail and is folded into the CRC.
const MAGIC: [u8; 4] = *b"STCK";

/// One checkpoint record: `magic(4) | segment_index(8 LE) | byte_offset(8 LE) |
/// crc32c(4 LE)`. Fixed width so [`load`] can scan the file in record-sized
/// strides and detect a torn trailing record by its short length.
const RECORD_LEN: usize = 4 + 8 + 8 + 4;

/// Encode `offset` as one checkpoint record (magic + fields + trailing CRC32C
/// over the leading `magic + fields`).
fn encode(offset: LogOffset) -> [u8; RECORD_LEN] {
    let mut buf = [0u8; RECORD_LEN];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4..12].copy_from_slice(&offset.segment_index.to_le_bytes());
    buf[12..20].copy_from_slice(&offset.byte_offset.to_le_bytes());
    let crc = crc32c(&buf[0..20]);
    buf[20..24].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode one record, returning the [`LogOffset`] only if the magic matches and
/// the CRC verifies. A wrong magic or a failed CRC — a torn write — yields
/// [`None`].
fn decode(buf: &[u8; RECORD_LEN]) -> Option<LogOffset> {
    if buf[0..4] != MAGIC {
        return None;
    }
    let stored = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
    if crc32c(&buf[0..20]) != stored {
        return None;
    }
    let segment_index = u64::from_le_bytes(buf[4..12].try_into().expect("8 bytes"));
    let byte_offset = u64::from_le_bytes(buf[12..20].try_into().expect("8 bytes"));
    Some(LogOffset {
        segment_index,
        byte_offset,
    })
}

/// Append a checkpoint record for `offset` and fsync it.
///
/// The fsync is the durability point: once this returns, the recorded boundary
/// survives a crash, so [`load`] on the next boot will see it. Appending (rather
/// than overwriting) keeps the prior good record intact until this one is
/// durable, so a torn append never destroys the last known-good checkpoint.
///
/// # Errors
///
/// [`io::Error`] if the checkpoint file cannot be created/opened, appended, or
/// synced.
pub(crate) fn store<D: Disk>(disk: &D, offset: LogOffset) -> io::Result<()> {
    let record = encode(offset);
    let mut file = match disk.open(CHECKPOINT_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => disk.create(CHECKPOINT_FILENAME)?,
        Err(e) => return Err(e),
    };
    file.append(&record)?;
    file.sync()?;
    Ok(())
}

/// Return the last CRC-valid checkpoint on `disk`, or [`None`] if the file is
/// absent, empty, or holds no intact record.
///
/// Scans the append-only file in [`RECORD_LEN`] strides and keeps the offset of
/// the last record that decodes; the scan stops at the first record that fails
/// to decode (a torn trailing write) or at a short final record. [`None`] means
/// "no durable checkpoint" — the caller replays the WAL from the beginning,
/// which is always correct ([ADR-0023]).
///
/// # Errors
///
/// [`io::Error`] if the file exists but cannot be read.
pub(crate) fn load<D: Disk>(disk: &D) -> io::Result<Option<LogOffset>> {
    let file = match disk.open(CHECKPOINT_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let len = file.len();
    if len == 0 {
        return Ok(None);
    }
    let len = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("checkpoint file length {len} exceeds usize"),
        )
    })?;
    let mut bytes = vec![0u8; len];
    let read = file.read_at(0, &mut bytes)?;
    bytes.truncate(read);

    let mut last = None;
    let mut record = [0u8; RECORD_LEN];
    for chunk in bytes.chunks(RECORD_LEN) {
        if chunk.len() < RECORD_LEN {
            break; // a short final record — the torn tail of a crashed append
        }
        record.copy_from_slice(chunk);
        match decode(&record) {
            Some(offset) => last = Some(offset),
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

    #[test]
    fn absent_checkpoint_loads_none() {
        let disk = MemDisk::new();
        assert_eq!(load(&disk).expect("load"), None);
    }

    #[test]
    fn store_then_load_round_trips() {
        let disk = MemDisk::new();
        store(&disk, offset(3, 4096)).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(offset(3, 4096)));
    }

    #[test]
    fn the_last_appended_checkpoint_wins() {
        // Each periodic checkpoint appends a record; recovery uses the newest.
        let disk = MemDisk::new();
        store(&disk, offset(0, 100)).expect("store");
        store(&disk, offset(0, 200)).expect("store");
        store(&disk, offset(1, 50)).expect("store");
        assert_eq!(load(&disk).expect("load"), Some(offset(1, 50)));
    }

    #[test]
    fn a_torn_trailing_record_falls_back_to_the_prior_good_one() {
        // A crash mid-append leaves a short, partial final record. The prior
        // fully-written checkpoint must still be recovered.
        let disk = MemDisk::new();
        store(&disk, offset(2, 64)).expect("store");
        // Simulate a torn append: a few stray bytes with no valid record.
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        file.append(&[0xAB, 0xCD, 0xEF]).expect("append partial");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(offset(2, 64)));
    }

    #[test]
    fn a_corrupt_record_body_falls_back_to_the_prior_good_one() {
        // A full-length record whose CRC fails (bit-rot / torn full write) must
        // be rejected, and the prior good checkpoint recovered.
        let disk = MemDisk::new();
        store(&disk, offset(5, 5)).expect("store");
        let mut file = disk.open(CHECKPOINT_FILENAME).expect("open");
        let mut bad = encode(offset(9, 9));
        bad[12] ^= 0xFF; // flip a field byte without fixing the CRC
        file.append(&bad).expect("append corrupt");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), Some(offset(5, 5)));
    }

    #[test]
    fn an_empty_file_loads_none() {
        let disk = MemDisk::new();
        let mut file = disk.create(CHECKPOINT_FILENAME).expect("create");
        file.sync().expect("sync");
        assert_eq!(load(&disk).expect("load"), None);
    }
}
