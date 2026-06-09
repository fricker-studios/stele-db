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

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;
use stele_storage::delta::{BusinessKey, Version};
use stele_storage::segment::{
    ColumnData, ColumnId, SegmentError, SegmentReader, SegmentWriter, ZoneBound, ZoneEnd,
};
use stele_storage::validity::Close;
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

// A segment stores only *birth* state (v6, ADR-0023): there is no stored
// `sys_to` / close-provenance column, and a version read back from a segment is
// always open. So the helper builds open versions via `Version::open`; the
// close/`sys_to` concept is exercised by the validity-index tests, not here.
fn version(key: &[u8], sys_from: i64, payload: &[u8]) -> Version {
    // Provenance varies per row (txn_id/committed_at track sys_from) so the
    // round-trip exercises real values across the three provenance columns.
    Version::open(
        BusinessKey::new(key.to_vec()),
        SystemTimeMicros(sys_from),
        0,
        Provenance::new(
            TxnId(u64::try_from(sys_from).unwrap_or(0)),
            SystemTimeMicros(sys_from),
            Principal::new(format!("svc-{sys_from}").into_bytes()),
        ),
        Some(payload.to_vec()),
    )
}

fn write_segment(disk: &MemDisk, name: &str, versions: &[Version]) {
    let mut w = SegmentWriter::create(disk, name).expect("create writer");
    for v in versions {
        w.push(v.clone()).expect("push");
    }
    w.finish().expect("finish");
}

/// A retraction tombstone closing `(key, target)` at `closed_at` by `txn` —
/// mirrors the [`Close`] the delete write path stages into the delta tier (STL-143).
fn retraction(key: &[u8], target: i64, closed_at: i64, txn: u64) -> Close {
    Close {
        business_key: BusinessKey::new(key.to_vec()),
        sys_from: SystemTimeMicros(target),
        seq: 0,
        sys_to: SystemTimeMicros(closed_at),
        closed_by: Provenance::new(
            TxnId(txn),
            SystemTimeMicros(closed_at),
            Principal::new(format!("deleter-{txn}").into_bytes()),
        ),
    }
}

/// Write a segment carrying both versions and retraction tombstones (format v7),
/// then read both sections back.
fn round_trip(
    disk: &MemDisk,
    name: &str,
    versions: &[Version],
    retractions: &[Close],
) -> (Vec<Version>, Vec<Close>) {
    let mut w = SegmentWriter::create(disk, name).expect("create writer");
    for v in versions {
        w.push(v.clone()).expect("push");
    }
    for r in retractions {
        w.push_retraction(r.clone()).expect("push retraction");
    }
    w.finish().expect("finish");
    let reader = SegmentReader::open(disk, name).expect("open");
    let v = reader.read_versions().expect("read versions");
    let r = reader.read_retractions().expect("read retractions");
    (v, r)
}

/// Retraction tombstones survive the columnar round-trip alongside versions,
/// independently of the version row count (format v7, STL-143).
#[test]
fn retractions_round_trip_alongside_versions() {
    let disk = MemDisk::new();
    let versions = vec![version(b"a", 10, b"a0"), version(b"a", 30, b"a2")];
    // Two tombstones — note: more versions than retractions, so the section's
    // value count is genuinely decoupled from the row-group row count.
    let retractions = vec![retraction(b"a", 10, 20, 7), retraction(b"b", 5, 25, 9)];
    let (got_v, got_r) = round_trip(&disk, "rt.seg", &versions, &retractions);

    assert_eq!(got_v.len(), 2, "versions round-trip unchanged");
    assert_eq!(got_r, retractions, "tombstones round-trip field-for-field");
}

/// The per-commit `seq` tiebreak is an always-on segment column (format v8,
/// STL-141): a non-zero `seq` survives the columnar round-trip field-for-field,
/// distinctly per row. `u64::MAX` exercises the lossless `u64`→`i64`-bits→`u64`
/// reinterpretation the [`ColumnId::Seq`](stele_storage::segment) column uses,
/// the same trick as the `txn_id` column.
#[test]
fn seq_round_trips_through_the_sealed_segment() {
    let disk = MemDisk::new();
    let mut a = version(b"k", 10, b"v0");
    a.seq = 7;
    let mut b = version(b"k", 20, b"v1");
    b.seq = u64::MAX; // high bit set: round-trips through the i64 column intact
    let (got, _) = round_trip(&disk, "seq.seg", &[a.clone(), b.clone()], &[]);
    assert_eq!(got, vec![a, b], "versions round-trip including seq");
    assert_eq!(got[0].seq, 7);
    assert_eq!(
        got[1].seq,
        u64::MAX,
        "full-width seq survives the i64 column"
    );
}

/// A SQL `NULL` payload survives seal → read (format v10, STL-154): the `None`
/// is persisted via the bytes-column sentinel and reconstructs as a `None`
/// payload, kept distinct from an empty payload in the same segment.
#[test]
fn null_payload_round_trips_through_the_sealed_segment() {
    let disk = MemDisk::new();
    let mut null = version(b"a", 10, b"");
    null.payload = None;
    let present = version(b"b", 20, b"v");
    let empty = version(b"c", 30, b""); // Some(vec![]) — must NOT collapse to NULL

    let (got, _) = round_trip(
        &disk,
        "null.seg",
        &[null.clone(), present.clone(), empty.clone()],
        &[],
    );
    assert_eq!(
        got,
        vec![null, present, empty],
        "payloads round-trip exactly"
    );
    assert_eq!(got[0].payload, None, "NULL stays NULL across the seal");
    assert_eq!(got[1].payload, Some(b"v".to_vec()));
    assert_eq!(
        got[2].payload,
        Some(Vec::new()),
        "empty payload is not confused with NULL"
    );

    // The projected payload column reports the NULL as a `None` cell.
    let reader = SegmentReader::open(&disk, "null.seg").expect("open");
    let ColumnData::NullableBytes(payloads) = reader.read_column(ColumnId::Payload).expect("read")
    else {
        panic!("payload column reads back as nullable bytes");
    };
    assert_eq!(
        payloads,
        vec![None, Some(b"v".to_vec()), Some(Vec::new())],
        "the payload column carries the NULL through projection"
    );
}

/// A delete-only flush (no surviving versions in this segment) still produces a
/// valid segment whose tombstones read back — the version row count is zero while
/// the retraction count is not.
#[test]
fn retraction_only_segment_round_trips() {
    let disk = MemDisk::new();
    let retractions = vec![retraction(b"gone", 1, 2, 3)];
    let (got_v, got_r) = round_trip(&disk, "tombs.seg", &[], &retractions);
    assert!(got_v.is_empty(), "no versions in a delete-only segment");
    assert_eq!(got_r, retractions);
}

/// A segment with no deletes writes no retraction columns at all (the
/// optional-columns pattern); `read_retractions` returns empty.
#[test]
fn segment_without_deletes_has_no_retraction_section() {
    let disk = MemDisk::new();
    let (_v, got_r) = round_trip(&disk, "clean.seg", &sample_workload(), &[]);
    assert!(
        got_r.is_empty(),
        "no tombstone columns when nothing was deleted"
    );
}

/// Tombstone columns are zone-map-prunable: the retraction `retract_key` and
/// `retract_closed_at` columns carry min/max stats in the resident zone map, so
/// the planner can skip a segment whose tombstones cannot match.
#[test]
fn retraction_columns_populate_the_zone_map() {
    let disk = MemDisk::new();
    let retractions = vec![
        retraction(b"a", 10, 20, 1),
        retraction(b"m", 30, 40, 2),
        retraction(b"z", 50, 60, 3),
    ];
    let mut w = SegmentWriter::create(&disk, "zm.seg").expect("create");
    w.push(version(b"a", 10, b"a")).expect("push");
    for r in retractions {
        w.push_retraction(r).expect("push retraction");
    }
    w.finish().expect("finish");
    let reader = SegmentReader::open(&disk, "zm.seg").expect("open");
    let zm = reader.zone_map();

    let key_zone = zm
        .column(ColumnId::RetractKey)
        .expect("retract_key has a zone entry");
    assert_eq!(
        key_zone.min,
        ZoneEnd::Value(ZoneBound::Bytes(b"a".to_vec()))
    );
    assert_eq!(
        key_zone.max,
        ZoneEnd::Value(ZoneBound::Bytes(b"z".to_vec()))
    );

    let closed_zone = zm
        .column(ColumnId::RetractClosedAt)
        .expect("retract_closed_at has a zone entry");
    assert_eq!(closed_zone.min, ZoneEnd::Value(ZoneBound::I64(20)));
    assert_eq!(closed_zone.max, ZoneEnd::Value(ZoneBound::I64(60)));
}

/// A representative workload: a handful of business keys with version chains
/// of varied length and distinct `sys_from` birth times, payloads non-empty
/// including some larger blobs. Exercises both bytes and i64 columns and both
/// stats paths. Every version is *open* — a segment stores only birth state,
/// so the period end is not a round-trippable segment field.
fn sample_workload() -> Vec<Version> {
    let blob = vec![0xABu8; 4096];
    vec![
        // Key "a": three versions at distinct birth times.
        version(b"a", 10, b"a-v0"),
        version(b"a", 20, b"a-v1"),
        version(b"a", 30, b"a-v2"),
        // Key "b": one version.
        version(b"b", 15, b"only-b"),
        // Key "c": empty payload — exercises the zero-length-bytes legal case.
        version(b"c", 5, b""),
        // Key "long": multi-KB payload — exercises larger bytes values.
        version(b"long", 1, &blob),
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
    let expected_payload: Vec<Option<Vec<u8>>> =
        written.iter().map(|v| v.payload.clone()).collect();

    assert_eq!(
        r.read_column(ColumnId::BusinessKey).unwrap(),
        ColumnData::Bytes(expected_bk),
    );
    assert_eq!(
        r.read_column(ColumnId::SysFrom).unwrap(),
        ColumnData::I64(expected_sys_from),
    );
    // No `sys_to` column exists (v6, ADR-0023): the period end lives in the
    // validity index, not the segment.
    assert_eq!(
        r.read_column(ColumnId::Payload).unwrap(),
        ColumnData::NullableBytes(expected_payload),
    );

    // Provenance columns are first-class — projectable exactly like the data
    // columns (DoD: every persisted version has all three populated).
    let expected_txn: Vec<i64> = written
        .iter()
        .map(|v| i64::try_from(v.provenance.txn_id.0).unwrap())
        .collect();
    let expected_committed: Vec<i64> = written
        .iter()
        .map(|v| v.provenance.committed_at.0)
        .collect();
    let expected_principal: Vec<Vec<u8>> = written
        .iter()
        .map(|v| v.provenance.principal.as_bytes().to_vec())
        .collect();
    assert_eq!(
        r.read_column(ColumnId::TxnId).unwrap(),
        ColumnData::I64(expected_txn),
    );
    assert_eq!(
        r.read_column(ColumnId::CommittedAt).unwrap(),
        ColumnData::I64(expected_committed),
    );
    assert_eq!(
        r.read_column(ColumnId::Principal).unwrap(),
        ColumnData::Bytes(expected_principal),
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
    // Walk chunks in declaration order until we reach Payload's chunk —
    // ColumnId::ALL is the same order the writer emits, so this naturally
    // tracks any column-list change.
    let payload_index = ColumnId::ALL
        .iter()
        .position(|&c| c == ColumnId::Payload)
        .expect("Payload is in ColumnId::ALL");
    let mut cursor = HEADER_LEN;
    for _ in 0..payload_index {
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
    // Compute the byte ranges every chunk covers: the chunk header
    // (header[0..12]), the on-disk CRC field (header[12..16]), and the
    // payload bytes. A flip in any of those must be caught — the first two
    // ranges by `crc32c(header[0..12] || payload)` not matching the stored
    // CRC; the third either by the CRC or by a chunk-header / footer
    // cross-check.
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
    // Compute per-chunk byte ranges. ColumnId::ALL is the segment module's
    // canonical column list — using it (rather than a duplicated `4`) means
    // adding a column flows into this sweep automatically.
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut cursor = HEADER_LEN;
    for _ in 0..ColumnId::ALL.len() {
        let payload_len =
            u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        // Chunk header bytes the CRC covers.
        ranges.push(cursor..cursor + 12);
        // On-disk CRC field — a flip here makes the stored CRC disagree
        // with the recomputed one and must also be detected.
        ranges.push(cursor + 12..cursor + CHUNK_HEADER_LEN);
        // Payload bytes.
        let payload_start = cursor + CHUNK_HEADER_LEN;
        let payload_end = payload_start + payload_len;
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
