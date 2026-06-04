//! Sealed-segment file-format integration tests.
//!
//! Scope (STL-88):
//!
//! * round-trip equality — DoD bullet 1 (write segment, read segment, assert
//!   column-by-column equivalence);
//! * corruption detection — DoD bullet 2 (flipping a byte in any page causes
//!   a checksum failure on read);
//! * append-rejection — DoD bullet 3 (a second writer cannot reopen and
//!   append; sealed segments are append-rejecting at the type level).

#![allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::delta::{BusinessKey, Version};
use stele_storage::segment::{ColumnData, ColumnId, SegmentError, SegmentReader, SegmentWriter};
use stele_storage::wal::{Disk, DiskFile};

// --- MemDisk: minimal in-memory Disk for tests ------------------------------

#[derive(Default, Clone)]
struct MemDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
}

impl MemDisk {
    fn new() -> Self {
        Self::default()
    }

    fn file_len(&self, name: &str) -> u64 {
        let files = self.inner.lock().unwrap();
        files.get(name).unwrap().lock().unwrap().len() as u64
    }

    /// Flip a single byte in the named file.
    fn flip_byte(&self, name: &str, offset: u64) {
        let files = self.inner.lock().unwrap();
        let f = files.get(name).expect("file");
        let mut bytes = f.lock().unwrap();
        bytes[offset as usize] ^= 0xFF;
    }
}

struct MemFile {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Disk for MemDisk {
    type File = MemFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        let mut files = self.inner.lock().unwrap();
        if files.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{name} already exists"),
            ));
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        files.insert(name.to_string(), Arc::clone(&bytes));
        Ok(MemFile { bytes })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(MemFile { bytes })
    }

    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.inner.lock().unwrap().keys().cloned().collect())
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        let mut files = self.inner.lock().unwrap();
        if files.remove(name).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()));
        }
        Ok(())
    }
}

impl DiskFile for MemFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let src = self.bytes.lock().unwrap();
        let start = offset as usize;
        if start >= src.len() {
            return Ok(0);
        }
        let end = (start + buf.len()).min(src.len());
        let n = end - start;
        buf[..n].copy_from_slice(&src[start..end]);
        Ok(n)
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn len(&self) -> u64 {
        self.bytes.lock().unwrap().len() as u64
    }
}

// --- Test helpers -----------------------------------------------------------

fn version(key: &[u8], sys_from: i64, sys_to: SystemTimeMicros, payload: &[u8]) -> Version {
    Version {
        business_key: BusinessKey::new(key.to_vec()),
        sys_from: SystemTimeMicros(sys_from),
        sys_to,
        payload: payload.to_vec(),
    }
}

fn write_segment(disk: &MemDisk, name: &str, versions: &[Version]) {
    let mut w = SegmentWriter::create(disk, name).expect("create writer");
    for v in versions {
        w.push(v.clone()).expect("push");
    }
    w.finish().expect("finish");
}

/// A representative workload: a handful of business keys with version chains
/// of varied length, sys_to mixing closed and open intervals, payloads
/// non-empty including some larger blobs. Exercises both bytes and i64
/// columns and both stats paths.
fn sample_workload() -> Vec<Version> {
    let blob = vec![0xABu8; 4096];
    vec![
        // Key "a": three versions — two closed, one open.
        version(b"a", 10, SystemTimeMicros(20), b"a-v0"),
        version(b"a", 20, SystemTimeMicros(30), b"a-v1"),
        version(b"a", 30, SYSTEM_TIME_OPEN, b"a-v2"),
        // Key "b": one open version.
        version(b"b", 15, SYSTEM_TIME_OPEN, b"only-b"),
        // Key "c": empty payload — exercises the zero-length-bytes legal case.
        version(b"c", 5, SystemTimeMicros(7), b""),
        // Key "long": multi-KB payload — exercises larger bytes values.
        version(b"long", 1, SystemTimeMicros(2), &blob),
    ]
}

// --- DoD bullet 1: round-trip equality -------------------------------------

#[test]
fn round_trip_preserves_every_column_value() {
    let disk = MemDisk::new();
    let written = sample_workload();
    write_segment(&disk, "rt.seg", &written);

    let r = SegmentReader::open(&disk, "rt.seg").expect("open reader");
    assert_eq!(r.row_count(), written.len() as u64);

    // Full reassembly via read_versions().
    let round_tripped = r.read_versions().expect("read versions");
    assert_eq!(
        round_tripped, written,
        "round-trip must preserve every field of every row"
    );

    // Column-by-column projections must agree with the original column views.
    let expected_bk: Vec<Vec<u8>> = written
        .iter()
        .map(|v| v.business_key.as_bytes().to_vec())
        .collect();
    let expected_sys_from: Vec<i64> = written.iter().map(|v| v.sys_from.0).collect();
    let expected_sys_to: Vec<i64> = written.iter().map(|v| v.sys_to.0).collect();
    let expected_payload: Vec<Vec<u8>> = written.iter().map(|v| v.payload.clone()).collect();

    assert_eq!(
        r.read_column(ColumnId::BusinessKey).unwrap(),
        ColumnData::Bytes(expected_bk),
    );
    assert_eq!(
        r.read_column(ColumnId::SysFrom).unwrap(),
        ColumnData::I64(expected_sys_from),
    );
    assert_eq!(
        r.read_column(ColumnId::SysTo).unwrap(),
        ColumnData::I64(expected_sys_to),
    );
    assert_eq!(
        r.read_column(ColumnId::Payload).unwrap(),
        ColumnData::Bytes(expected_payload),
    );
}

#[test]
fn round_trip_handles_empty_input() {
    // Edge case: a segment with zero rows. The format must still be openable
    // — header, footer with one row-group containing zero values per column,
    // trailer.
    let disk = MemDisk::new();
    write_segment(&disk, "empty.seg", &[]);
    let r = SegmentReader::open(&disk, "empty.seg").expect("open");
    assert_eq!(r.row_count(), 0);
    assert!(r.read_versions().unwrap().is_empty());
    assert_eq!(
        r.read_column(ColumnId::BusinessKey).unwrap(),
        ColumnData::Bytes(Vec::new()),
    );
    assert_eq!(
        r.read_column(ColumnId::SysFrom).unwrap(),
        ColumnData::I64(Vec::new()),
    );
}

// --- Late materialization ---------------------------------------------------

/// Reading one column doesn't have to touch the other columns' chunks. We
/// can't observe per-chunk I/O directly through the in-memory MemDisk, but we
/// can verify the contract holds end-to-end: corrupting a *different*
/// column's chunk payload doesn't fail a projection that only requests an
/// unrelated column.
#[test]
fn projection_does_not_touch_other_columns_chunks() {
    let disk = MemDisk::new();
    let written = sample_workload();
    write_segment(&disk, "proj.seg", &written);

    // Find the byte offset of the Payload column's chunk header (and so its
    // payload). We do this without leaning on internals: walk the file from
    // header_end, decoding each chunk header in succession. The byte we flip
    // lands inside the Payload column's payload, which a SysFrom projection
    // must not touch.
    let file = disk.inner.lock().unwrap();
    let bytes = file.get("proj.seg").unwrap().lock().unwrap().clone();
    drop(file);

    const HEADER_LEN: usize = 16;
    const CHUNK_HEADER_LEN: usize = 16;
    // chunk 0: business_key, 1: sys_from, 2: sys_to, 3: payload.
    let mut cursor = HEADER_LEN;
    for _ in 0..3 {
        let payload_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += CHUNK_HEADER_LEN + payload_len;
    }
    let payload_chunk_offset = cursor; // start of Payload chunk header
    let payload_byte_offset = payload_chunk_offset + CHUNK_HEADER_LEN + 16;
    // Flip a byte inside the Payload column's payload region.
    disk.flip_byte("proj.seg", payload_byte_offset as u64);

    // SysFrom projection should succeed — its chunk is untouched.
    let r = SegmentReader::open(&disk, "proj.seg").expect("open");
    let sf = r.read_column(ColumnId::SysFrom).expect("sys_from clean");
    let expected_sf: Vec<i64> = written.iter().map(|v| v.sys_from.0).collect();
    assert_eq!(sf, ColumnData::I64(expected_sf));

    // Payload projection should fail — its chunk's CRC catches the flip.
    let err = r.read_column(ColumnId::Payload).unwrap_err();
    assert!(matches!(err, SegmentError::Corrupt(_)));
}

// --- DoD bullet 2: corruption detection ------------------------------------

/// Flipping a byte anywhere in a column-chunk page must surface as a checksum
/// failure on read. We sweep every byte in the page region of every chunk —
/// no offset escapes detection.
#[test]
fn flipping_any_byte_in_any_page_fails_on_read() {
    // Compute the byte ranges of every chunk's coverage region (header[0..12]
    // || payload). Bytes within these ranges are checksum-protected; a flip
    // in either the header bytes the CRC covers or the payload itself must
    // be caught.
    let disk_origin = MemDisk::new();
    let written = sample_workload();
    write_segment(&disk_origin, "orig.seg", &written);
    let bytes = disk_origin
        .inner
        .lock()
        .unwrap()
        .get("orig.seg")
        .unwrap()
        .lock()
        .unwrap()
        .clone();

    const HEADER_LEN: usize = 16;
    const CHUNK_HEADER_LEN: usize = 16;
    // Compute per-chunk covered byte ranges.
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut cursor = HEADER_LEN;
    for _ in 0..ColumnId::ALL_LEN {
        let payload_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        let header_covered_start = cursor;
        let header_covered_end = cursor + 12;
        let payload_start = cursor + CHUNK_HEADER_LEN;
        let payload_end = payload_start + payload_len;
        ranges.push(header_covered_start..header_covered_end);
        ranges.push(payload_start..payload_end);
        cursor = payload_end;
    }

    // For each covered byte, write a fresh segment and flip exactly that
    // byte. Reading the segment must surface a Corrupt error (whether from
    // the column whose chunk owns that byte, or from header/footer/trailer
    // mismatches when chunk-header fields disagree with the footer).
    for range in &ranges {
        for offset in range.clone() {
            let disk = MemDisk::new();
            write_segment(&disk, "x.seg", &written);
            disk.flip_byte("x.seg", offset as u64);

            // Reading every column must error somewhere along the way — for
            // header fields where the flip causes the chunk length to
            // disagree with the footer's recorded length, the error
            // surfaces at the cross-check; for payload bytes it surfaces as
            // the chunk CRC. Either way, no clean version vector emerges.
            let opened = SegmentReader::open(&disk, "x.seg");
            let touched_all_columns = opened.and_then(|r| r.read_versions());
            assert!(
                touched_all_columns.is_err(),
                "byte {offset} in a page region must be detected"
            );
        }
    }
}

/// A flipped byte in the *footer* must surface as the footer-CRC mismatch.
#[test]
fn flipping_byte_in_footer_fails_at_open() {
    let disk = MemDisk::new();
    let written = sample_workload();
    write_segment(&disk, "f.seg", &written);

    // Find where the footer starts (file_len - trailer_len - footer_len).
    let file_len = disk.file_len("f.seg");
    let bytes = disk
        .inner
        .lock()
        .unwrap()
        .get("f.seg")
        .unwrap()
        .lock()
        .unwrap()
        .clone();
    const TRAILER_LEN: usize = 16;
    let trailer_start = file_len as usize - TRAILER_LEN;
    let footer_len = u32::from_le_bytes(
        bytes[trailer_start + 4..trailer_start + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    let footer_start = trailer_start - footer_len;

    // Flip a byte in the middle of the footer payload.
    let probe = footer_start + footer_len / 2;
    disk.flip_byte("f.seg", probe as u64);

    let result = SegmentReader::open(&disk, "f.seg");
    assert!(matches!(result.err(), Some(SegmentError::Corrupt(_))));
}

/// Truncating the trailer makes the file unopenable — never silently
/// returning a partial reader.
#[test]
fn truncated_file_fails_at_open() {
    let disk = MemDisk::new();
    let written = sample_workload();
    write_segment(&disk, "t.seg", &written);

    {
        let files = disk.inner.lock().unwrap();
        let f = files.get("t.seg").unwrap();
        let mut bytes = f.lock().unwrap();
        bytes.truncate(8); // shorter than even the header
    }
    let result = SegmentReader::open(&disk, "t.seg");
    assert!(matches!(result.err(), Some(SegmentError::Corrupt(_))));
}

// --- DoD bullet 3: append-rejection ----------------------------------------

/// The writer's `create` lifecycle plus the absence of an `open`-for-write
/// surface mean a second writer aimed at the same name returns
/// `AlreadyExists`. Sealed segments are append-rejecting at the type level.
#[test]
fn a_second_writer_cannot_reopen_an_existing_segment() {
    let disk = MemDisk::new();
    let written = sample_workload();
    write_segment(&disk, "sealed.seg", &written);

    let err = SegmentWriter::create(&disk, "sealed.seg").err().expect(
        "second create must error: sealed segments are immutable and have no open-for-write API",
    );
    let io_err = match err {
        SegmentError::Io(e) => e,
        other => panic!("expected i/o error, got {other:?}"),
    };
    assert_eq!(
        io_err.kind(),
        io::ErrorKind::AlreadyExists,
        "rejection must be at the filesystem layer (AlreadyExists), not a softer check that a bad actor could bypass"
    );

    // The original segment must still be readable — failed re-create did not
    // disturb its bytes.
    let r = SegmentReader::open(&disk, "sealed.seg").expect("original still readable");
    assert_eq!(r.read_versions().unwrap(), written);
}

// --- internal: surface ColumnId::ALL length without exposing it -------------

trait ColumnIdAllLen {
    const ALL_LEN: usize;
}
impl ColumnIdAllLen for ColumnId {
    const ALL_LEN: usize = 4;
}
