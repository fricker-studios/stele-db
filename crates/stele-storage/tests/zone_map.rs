//! Zone-map pruning integration tests (STL-89, STL-115).
//!
//! Covers the ticket's Definition of done:
//!
//! * **I/O counters** — a query touching one logical (system-time) slice scans
//!   only the matching segments. The counting disk proves a pruned segment
//!   incurs *zero* column-chunk reads.
//! * **Resident metadata** — the zone map survives dropping the segment's file
//!   handle, modelling cold-tiered metadata that is never archived (ADR-0021).
//! * **Correctness oracle** — a seeded differential check that `might_contain`
//!   never prunes a segment that actually holds a matching, visible row (no
//!   false negatives), per testing-strategy §4. STL-115 extends the oracle to
//!   bounded-prefix bytes columns (`Payload`), proving a truncated-down min and
//!   rounded-up max bound never prune a real value/range match. STL-134 extends
//!   it to the valid-time axis (`valid_from` / `valid_to`), so the no-false-
//!   negative proof now covers `sys_from` / value / `valid_*` — the full triple
//!   the segment still stores after `sys_to` moved to the validity index.

#![allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros, ValidTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Snapshot, Version};
use stele_storage::merge::{fold_chains, resolve_snapshot};
use stele_storage::segment::{ColumnId, Predicate, SegmentReader, SegmentWriter, ZoneBound};
use stele_storage::validity::{Close, SysUpperBound, ValidityConfig, ValidityIndex};
use stele_storage::validtime::{VALID_TIME_PREFIX_LEN, ValidInterval, frame_payload};
use stele_storage::wal::{Disk, DiskFile};

// --- CountingDisk: a MemDisk that counts read_at calls ----------------------

#[derive(Default, Clone)]
struct CountingDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
    reads: Arc<AtomicU64>,
}

impl CountingDisk {
    fn new() -> Self {
        Self::default()
    }

    fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }

    fn reset_reads(&self) {
        self.reads.store(0, Ordering::SeqCst);
    }
}

struct CountingFile {
    bytes: Arc<Mutex<Vec<u8>>>,
    reads: Arc<AtomicU64>,
}

impl Disk for CountingDisk {
    type File = CountingFile;

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
        Ok(CountingFile {
            bytes,
            reads: Arc::clone(&self.reads),
        })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(CountingFile {
            bytes,
            reads: Arc::clone(&self.reads),
        })
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

impl DiskFile for CountingFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.reads.fetch_add(1, Ordering::SeqCst);
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

// --- Helpers ----------------------------------------------------------------

// A segment stores only *birth* state (v6, ADR-0023): there is no stored
// `sys_to` column and a version read back from a segment is always open. The
// helper therefore builds open versions via `Version::open`; tests that need a
// period end for their oracle track it separately (it is not a segment field).
fn version(key: &[u8], sys_from: i64, payload: &[u8]) -> Version {
    Version::open(
        BusinessKey::new(key.to_vec()),
        SystemTimeMicros(sys_from),
        Provenance::new(
            TxnId(u64::try_from(sys_from).unwrap_or(0)),
            SystemTimeMicros(sys_from),
            Principal::new(format!("svc-{sys_from}").into_bytes()),
        ),
        payload.to_vec(),
    )
}

fn write_segment(disk: &CountingDisk, name: &str, versions: &[Version]) {
    let mut w = SegmentWriter::create(disk, name).expect("create writer");
    for v in versions {
        w.push(v.clone()).expect("push");
    }
    w.finish().expect("finish");
}

const fn snap(t: i64) -> Snapshot {
    Snapshot(SystemTimeMicros(t))
}

// --- DoD: I/O counters ------------------------------------------------------

/// Three segments, each covering a disjoint system-time era. A snapshot inside
/// one era must prune the other two: a pruned segment, gated by
/// `might_contain`, incurs zero `read_at` calls.
#[test]
fn time_slice_query_scans_only_matching_segment() {
    let disk = CountingDisk::new();

    // Era 0: born [0/10], superseded at 100. Era 1: born 100, superseded at 200.
    // Era 2: born 200, open. Segments store only the birth (`sys_from`); the
    // period ends live in the validity index (v6, ADR-0023), so we record them
    // there as closes and resolve visibility through the index overlay.
    write_segment(
        &disk,
        "era0.seg",
        &[version(b"a", 0, b"a@era0"), version(b"b", 10, b"b@era0")],
    );
    write_segment(
        &disk,
        "era1.seg",
        &[version(b"a", 100, b"a@era1"), version(b"b", 100, b"b@era1")],
    );
    write_segment(
        &disk,
        "era2.seg",
        &[version(b"a", 200, b"a@era2"), version(b"b", 200, b"b@era2")],
    );

    // The materialized period ends: era0 rows close at 100, era1 rows at 200,
    // era2 rows stay open. The closer provenance is immaterial to this test.
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    for (key, sys_from, sys_to) in [
        (b"a".as_slice(), 0, 100),
        (b"b".as_slice(), 10, 100),
        (b"a".as_slice(), 100, 200),
        (b"b".as_slice(), 100, 200),
    ] {
        index
            .insert_close(Close {
                business_key: BusinessKey::new(key.to_vec()),
                sys_from: SystemTimeMicros(sys_from),
                sys_to: SystemTimeMicros(sys_to),
                closed_by: Provenance::new(
                    TxnId(u64::try_from(sys_to).unwrap()),
                    SystemTimeMicros(sys_to),
                    Principal::new(b"svc".to_vec()),
                ),
            })
            .expect("insert close");
    }

    let names = ["era0.seg", "era1.seg", "era2.seg"];
    let readers: Vec<SegmentReader<_>> = names
        .iter()
        .map(|n| SegmentReader::open(&disk, n).expect("open"))
        .collect();

    // Snapshot 150 lives only in era 1.
    let snapshot = snap(150);

    // Pruning decisions touch no chunk bytes: open() already read header +
    // footer, so reset the counter and assert might_contain adds nothing.
    disk.reset_reads();
    let keep: Vec<bool> = readers
        .iter()
        .map(|r| r.might_contain(&Predicate::All, snapshot))
        .collect();
    assert_eq!(
        disk.reads(),
        0,
        "might_contain must not read any column chunk — it works off the resident zone map"
    );
    // The system-time prune is now one-sided (v6, ADR-0023): a segment is pruned
    // only when every row is *born after* the snapshot (`min(sys_from) > snap`).
    // era2's rows are born at 200 > 150, so it prunes; era0 and era1 are both
    // born at/before 150 and are conservatively kept — the index overlay below
    // filters era0's already-superseded rows out at read time, not the zone map.
    assert_eq!(
        keep,
        vec![true, true, false],
        "era2 (born at 200) is pruned; era0/era1 are kept and resolved at read time"
    );

    // Now run the actual query: scan only the segments might_contain keeps, then
    // resolve the snapshot through the validity-index overlay.
    disk.reset_reads();
    let mut raw = Vec::new();
    for (r, &k) in readers.iter().zip(&keep) {
        if k {
            raw.extend(r.read_versions().expect("read"));
        }
    }
    let reads_after_pruned_scan = disk.reads();
    let chains = fold_chains(raw, &index).expect("fold");
    let scanned = resolve_snapshot(&chains, snapshot);

    // Exactly the two era1 rows are live at snapshot 150.
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].payload, b"a@era1");
    assert_eq!(scanned[1].payload, b"b@era1");

    // Reading every segment unconditionally must cost strictly more I/O than
    // the pruned scan — proof the prune actually saved reads, not just that
    // the result happened to be right.
    disk.reset_reads();
    for r in &readers {
        let _ = r.read_versions().expect("read");
    }
    let reads_scanning_all = disk.reads();
    assert!(
        reads_after_pruned_scan < reads_scanning_all,
        "pruned scan ({reads_after_pruned_scan} reads) must be cheaper than scanning all segments ({reads_scanning_all} reads)"
    );
}

// --- DoD: resident metadata survives dropping the file ----------------------

/// The zone map is resident: clone it, drop the reader (releasing the file
/// handle), and pruning still works with zero further I/O — the cold-tiered
/// metadata behaviour ADR-0021 specifies.
#[test]
fn zone_map_is_resident_after_segment_handle_dropped() {
    let disk = CountingDisk::new();
    write_segment(&disk, "cold.seg", &[version(b"k", 100, b"v")]);

    let zone_map = {
        let r = SegmentReader::open(&disk, "cold.seg").expect("open");
        r.zone_map().clone()
        // reader (and its file handle) dropped here
    };

    // Even after the segment file handle is gone, the resident map prunes. The
    // sole row is born at 100, so the one-sided system-time prune (v6,
    // ADR-0023) drops only snapshots strictly before 100; a snapshot at/after
    // 100 is conservatively kept (the period end lives in the validity index,
    // not the zone map, so the map cannot prune on "already superseded").
    disk.reset_reads();
    assert!(!zone_map.might_contain(&Predicate::All, snap(50)));
    assert!(zone_map.might_contain(&Predicate::All, snap(150)));
    assert!(zone_map.might_contain(&Predicate::All, snap(200)));
    assert_eq!(
        disk.reads(),
        0,
        "resident zone map must never touch the (now-archived) segment bytes"
    );
}

// --- Zone-map values are correct -------------------------------------------

#[test]
fn zone_map_records_per_column_min_max() {
    let disk = CountingDisk::new();
    write_segment(
        &disk,
        "z.seg",
        &[
            version(b"m", 30, b"x"),
            version(b"a", 10, b"y"),
            version(b"z", 20, b"w"),
        ],
    );
    let r = SegmentReader::open(&disk, "z.seg").expect("open");
    let zm = r.zone_map();

    let sys_from = zm.column(ColumnId::SysFrom).expect("sys_from stats");
    assert_eq!(sys_from.min, ZoneBound::I64(10));
    assert_eq!(sys_from.max, ZoneBound::I64(30));

    // There is no `sys_to` zone (v6, ADR-0023): `ColumnId::SysTo` no longer
    // exists — the period end lives in the validity index, so the segment
    // carries only the `sys_from` birth zone on the system-time axis.

    let bk = zm
        .column(ColumnId::BusinessKey)
        .expect("business_key stats");
    assert_eq!(bk.min, ZoneBound::Bytes(b"a".to_vec()));
    assert_eq!(bk.max, ZoneBound::Bytes(b"z".to_vec()));

    // Payload now carries bounded-prefix stats (STL-115). These payloads are
    // single bytes, well under the prefix cap, so the bounds are exact.
    let payload = zm.column(ColumnId::Payload).expect("payload stats");
    assert_eq!(payload.min, ZoneBound::Bytes(b"w".to_vec()));
    assert_eq!(payload.max, ZoneBound::Bytes(b"y".to_vec()));
}

// --- DoD: correctness oracle (no false negatives) ---------------------------

/// Tiny deterministic LCG — keeps the oracle dependency-free and seed-reproducible
/// (ADR-0010): every seed replays the same workload bit-for-bit.
struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        // Numerical Recipes constants.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Differential oracle: for many seeds, build a random segment, then fire
/// random (snapshot, business-key range) probes. The brute-force oracle scans
/// every row to decide whether a visible match truly exists; `might_contain`
/// must never answer `false` when the oracle says one does. False positives
/// (prune kept a segment with no match) are allowed — that is just a wasted
/// scan, not a correctness bug.
#[test]
fn might_contain_never_prunes_a_real_match() {
    let keys: [&[u8]; 4] = [b"a", b"g", b"m", b"t"];
    for seed in 0..200u64 {
        let mut rng = Lcg(seed.wrapping_mul(2_654_435_761).wrapping_add(1));
        let disk = CountingDisk::new();

        // Build a random segment of 1..=8 rows. The segment stores only birth
        // state (v6, ADR-0023), so the version is open; the oracle tracks the
        // intended period end (`sys_to`) alongside it — the end lives in the
        // validity index, not the segment, but the no-false-negative invariant
        // is about the closed `[sys_from, sys_to)` visibility window.
        let row_count = 1 + rng.below(8);
        let mut rows = Vec::new();
        for _ in 0..row_count {
            let key = keys[rng.below(keys.len() as u64) as usize];
            let sys_from = rng.below(100) as i64;
            // sys_to is either an open interval or a closed one strictly after
            // sys_from.
            let sys_to = if rng.below(4) == 0 {
                SYSTEM_TIME_OPEN.0
            } else {
                sys_from + 1 + rng.below(100) as i64
            };
            rows.push((version(key, sys_from, b"p"), sys_to));
        }
        let versions: Vec<Version> = rows.iter().map(|(v, _)| v.clone()).collect();
        write_segment(&disk, "o.seg", &versions);
        let reader = SegmentReader::open(&disk, "o.seg").expect("open");

        // Fire several random probes against this segment.
        for _ in 0..20 {
            let snapshot = snap(rng.below(210) as i64 - 5); // includes some out-of-range
            // Random inclusive business-key range over the key alphabet.
            let lo = rng.below(keys.len() as u64) as usize;
            let hi = lo + rng.below((keys.len() - lo) as u64) as usize;
            let predicate = Predicate::Range {
                column: ColumnId::BusinessKey,
                low: ZoneBound::Bytes(keys[lo].to_vec()),
                high: ZoneBound::Bytes(keys[hi].to_vec()),
            };

            // Oracle: does a visible, in-range row truly exist?
            let real_match = rows.iter().any(|(v, sys_to)| {
                v.sys_from <= snapshot.0
                    && *sys_to > snapshot.0.0
                    && v.business_key.as_bytes() >= keys[lo]
                    && v.business_key.as_bytes() <= keys[hi]
            });

            let kept = reader.might_contain(&predicate, snapshot);
            assert!(
                !real_match || kept,
                "seed {seed}: might_contain pruned a segment that holds a real match \
                 (snapshot={snapshot:?}, key_range={lo}..={hi}, rows={rows:?})"
            );
        }
    }
}

// --- DoD: valid-time zone-map pruning (STL-117) -----------------------------

/// A valid-time version: framed payload (16-byte `[valid_from, valid_to)`
/// prefix + user bytes, [STL-92]) over a system interval `[0, open)` so only
/// the *valid* axis distinguishes the rows.
fn valid_version(key: &[u8], valid_from: i64, valid_to: i64) -> Version {
    let interval = ValidInterval::new(ValidTimeMicros(valid_from), ValidTimeMicros(valid_to))
        .expect("well-formed valid interval");
    let payload = frame_payload(true, Some(interval), b"row".to_vec()).expect("frame payload");
    Version::open(
        BusinessKey::new(key.to_vec()),
        SystemTimeMicros(0),
        Provenance::new(
            TxnId(1),
            SystemTimeMicros(0),
            Principal::new(b"svc".to_vec()),
        ),
        payload,
    )
}

fn write_valid_segment(disk: &CountingDisk, name: &str, versions: &[Version]) {
    let mut w = SegmentWriter::create_valid_time(disk, name).expect("create valid-time writer");
    for v in versions {
        w.push(v.clone()).expect("push");
    }
    w.finish().expect("finish");
}

/// The ticket's Definition of done: a valid-time range query skips segments
/// whose valid-interval min/max cannot match. Two segments on disjoint
/// valid-from ranges; a `Predicate::Range` on `valid_from` prunes the one that
/// provably can't intersect — gated by `might_contain`, the pruned segment
/// incurs zero column-chunk reads.
#[test]
fn valid_time_range_query_prunes_non_overlapping_segments() {
    let disk = CountingDisk::new();

    // "early" facts hold for valid_from in [10, 15]; "late" for [100, 110].
    write_valid_segment(
        &disk,
        "vt-early.seg",
        &[valid_version(b"a", 10, 50), valid_version(b"b", 15, 60)],
    );
    write_valid_segment(
        &disk,
        "vt-late.seg",
        &[valid_version(b"c", 100, 200), valid_version(b"d", 110, 300)],
    );

    let readers = [
        SegmentReader::open(&disk, "vt-early.seg").expect("open early"),
        SegmentReader::open(&disk, "vt-late.seg").expect("open late"),
    ];

    // Every row is visible across the whole system axis ([0, open)), so only
    // the valid axis can prune here.
    let snapshot = snap(1);

    // Facts whose valid_from is in [100, 150]: the late segment straddles it;
    // the early segment (max valid_from 15) provably cannot match.
    let by_valid_from = Predicate::Range {
        column: ColumnId::ValidFrom,
        low: ZoneBound::I64(100),
        high: ZoneBound::I64(150),
    };

    disk.reset_reads();
    let keep: Vec<bool> = readers
        .iter()
        .map(|r| r.might_contain(&by_valid_from, snapshot))
        .collect();
    assert_eq!(
        disk.reads(),
        0,
        "valid-axis pruning must work off the resident zone map — no chunk I/O"
    );
    assert_eq!(
        keep,
        vec![false, true],
        "early segment valid_from [10,15] cannot intersect query [100,150]"
    );

    // Symmetric prune on the other boundary column: facts whose valid_to is in
    // [40, 55] keep early (valid_to [50,60]) and prune late (valid_to [200,300]).
    let by_valid_to = Predicate::Range {
        column: ColumnId::ValidTo,
        low: ZoneBound::I64(40),
        high: ZoneBound::I64(55),
    };
    assert!(
        readers[0].might_contain(&by_valid_to, snapshot),
        "early segment valid_to [50,60] overlaps query [40,55]"
    );
    assert!(
        !readers[1].might_contain(&by_valid_to, snapshot),
        "late segment valid_to [200,300] cannot intersect query [40,55]"
    );

    // I/O proof: scanning only the kept segment is strictly cheaper than
    // scanning both — the prune saved reads, it didn't just happen to be right.
    disk.reset_reads();
    for (r, &k) in readers.iter().zip(&keep) {
        if k {
            let _ = r.read_versions().expect("read");
        }
    }
    let pruned_reads = disk.reads();
    disk.reset_reads();
    for r in &readers {
        let _ = r.read_versions().expect("read");
    }
    let full_reads = disk.reads();
    assert!(
        pruned_reads < full_reads,
        "pruned scan ({pruned_reads} reads) must be cheaper than scanning both ({full_reads} reads)"
    );
}

/// The valid-time prefix is *lifted* into first-class columns: the zone map
/// records their min/max, and the framed payload still round-trips byte-for-byte
/// (the lift is additive — nothing that reads `payload` is disturbed).
#[test]
fn valid_time_columns_populate_zone_map_and_round_trip() {
    let disk = CountingDisk::new();
    let rows = [
        valid_version(b"a", 30, 90),
        valid_version(b"b", 10, 100),
        valid_version(b"c", 20, 80),
    ];
    write_valid_segment(&disk, "vt.seg", &rows);
    let r = SegmentReader::open(&disk, "vt.seg").expect("open");
    let zm = r.zone_map();

    let vf = zm.column(ColumnId::ValidFrom).expect("valid_from stats");
    assert_eq!(vf.min, ZoneBound::I64(10));
    assert_eq!(vf.max, ZoneBound::I64(30));

    let vt = zm.column(ColumnId::ValidTo).expect("valid_to stats");
    assert_eq!(vt.min, ZoneBound::I64(80));
    assert_eq!(vt.max, ZoneBound::I64(100));

    let read = r.read_versions().expect("read versions");
    assert_eq!(read, rows.to_vec(), "framed payload round-trips unchanged");
}

/// DoD ([STL-119]): a valid-time segment stores each user payload exactly once.
/// The 16-byte interval prefix lives only in the `valid_from` / `valid_to`
/// columns — it is *not* duplicated in the `payload` column. Proven by comparing
/// the payload column's on-disk byte size against a system-only segment storing
/// the same bare user payloads: equal means no prefix duplication (a v3-style
/// segment that kept the prefix would be `16 * row_count` bytes larger).
#[test]
fn valid_time_segment_stores_user_payload_once() {
    let disk = CountingDisk::new();

    // The valid-time rows carry `b"row"` as their *user* payload (see
    // `valid_version`), framed with an interval. The system-only rows store the
    // same bare `b"row"`. The interval must not inflate the payload column.
    let vt_rows = [
        valid_version(b"a", 30, 90),
        valid_version(b"b", 10, 100),
        valid_version(b"c", 20, 80),
    ];
    write_valid_segment(&disk, "vt-once.seg", &vt_rows);

    let sys_rows = [
        version(b"a", 0, b"row"),
        version(b"b", 0, b"row"),
        version(b"c", 0, b"row"),
    ];
    write_segment(&disk, "sys-once.seg", &sys_rows);

    let vt = SegmentReader::open(&disk, "vt-once.seg").expect("open vt");
    let sys = SegmentReader::open(&disk, "sys-once.seg").expect("open sys");

    let vt_payload = vt
        .column_byte_len(ColumnId::Payload)
        .expect("vt payload column present");
    let sys_payload = sys
        .column_byte_len(ColumnId::Payload)
        .expect("sys payload column present");
    assert_eq!(
        vt_payload,
        sys_payload,
        "valid-time payload column must store only the bare user payload — no \
         duplicated interval prefix (would be {} bytes larger)",
        VALID_TIME_PREFIX_LEN * vt_rows.len()
    );
}

/// A system-only table opts out of valid-time, so its segment carries no
/// `valid_from` / `valid_to` columns: the zone map exposes none, and a
/// valid-axis predicate can never prune it (no stats ⇒ conservatively kept).
#[test]
fn system_only_segment_has_no_valid_time_columns() {
    let disk = CountingDisk::new();
    write_segment(&disk, "sys.seg", &[version(b"k", 10, b"v")]);
    let r = SegmentReader::open(&disk, "sys.seg").expect("open");

    assert!(r.zone_map().column(ColumnId::ValidFrom).is_none());
    assert!(r.zone_map().column(ColumnId::ValidTo).is_none());
    assert!(
        r.might_contain(
            &Predicate::Range {
                column: ColumnId::ValidFrom,
                low: ZoneBound::I64(1000),
                high: ZoneBound::I64(2000),
            },
            snap(50),
        ),
        "a valid-axis predicate must not prune a segment that has no valid-time stats"
    );
}

/// A byte alphabet weighted toward the rounding edges: `0x00`, mid values, and
/// runs of `0xFF` so generated payloads frequently exercise the max-prefix
/// carry (trailing `0xFF` dropped + previous byte bumped) and the all-`0xFF`
/// "no upper bound" path.
const STRESS_ALPHABET: [u8; 6] = [0x00, 0x41, 0x7F, 0xFE, 0xFF, 0xFF];

/// Generate a random byte string of length `len` over [`STRESS_ALPHABET`].
fn stress_bytes(rng: &mut Lcg, len: usize) -> Vec<u8> {
    (0..len)
        .map(|_| STRESS_ALPHABET[rng.below(STRESS_ALPHABET.len() as u64) as usize])
        .collect()
}

/// STL-115 differential oracle for **bounded-prefix bytes columns**. Builds
/// segments whose `Payload` values are *longer than the prefix cap* (so the
/// writer must truncate the min down and round the max up), then fires random
/// `Eq` and `Range` predicates on `Payload`. The brute-force oracle decides
/// whether a visible, predicate-satisfying row truly exists by exact byte
/// comparison; `might_contain` must never answer `false` when it does.
///
/// The invariant holds for any cap value, so the test stays agnostic to the
/// exact `MAX_BYTES_STAT_PREFIX_LEN`; it only needs every payload to be longer
/// than the cap so the min-truncation / max-round-up paths are exercised on
/// every row. The cap is an internal writer constant (64 at time of writing),
/// so the floor below is set well clear of it — raise it if the cap ever grows
/// past ~96.
#[test]
fn might_contain_never_prunes_a_real_match_on_truncated_payload() {
    for seed in 0..300u64 {
        let mut rng = Lcg(seed.wrapping_mul(2_654_435_761).wrapping_add(7));
        let disk = CountingDisk::new();

        // 1..=6 rows, each with a payload comfortably over the prefix cap
        // (97..=144 bytes), drawn from the stress alphabet so the truncation
        // and round-up paths run for every row.
        // The segment stores only birth state (v6, ADR-0023); the oracle tracks
        // the intended period end (`sys_to`) alongside each open version.
        let row_count = 1 + rng.below(6);
        let mut rows = Vec::new();
        for i in 0..row_count {
            let payload_len = 97 + rng.below(48) as usize; // 97..=144 bytes, always over-cap
            let payload = stress_bytes(&mut rng, payload_len);
            let sys_from = rng.below(100) as i64;
            let sys_to = if rng.below(4) == 0 {
                SYSTEM_TIME_OPEN.0
            } else {
                sys_from + 1 + rng.below(100) as i64
            };
            rows.push((
                version(format!("k{i}").as_bytes(), sys_from, &payload),
                sys_to,
            ));
        }
        let versions: Vec<Version> = rows.iter().map(|(v, _)| v.clone()).collect();
        write_segment(&disk, "p.seg", &versions);
        let reader = SegmentReader::open(&disk, "p.seg").expect("open");

        for _ in 0..24 {
            let snapshot = snap(rng.below(210) as i64 - 5);

            // Alternate between an exact-value probe and a range probe. The
            // Eq probe sometimes targets a real row's payload (forcing the
            // keep path) and sometimes a random value.
            let predicate = if rng.below(2) == 0 {
                let value = if rng.below(2) == 0 && !rows.is_empty() {
                    let idx = rng.below(rows.len() as u64) as usize;
                    rows[idx].0.payload.clone()
                } else {
                    let len = 40 + rng.below(50) as usize;
                    stress_bytes(&mut rng, len)
                };
                Predicate::Eq {
                    column: ColumnId::Payload,
                    value: ZoneBound::Bytes(value),
                }
            } else {
                let a_len = 1 + rng.below(80) as usize;
                let a = stress_bytes(&mut rng, a_len);
                let b_len = 1 + rng.below(80) as usize;
                let b = stress_bytes(&mut rng, b_len);
                let (low, high) = if a <= b { (a, b) } else { (b, a) };
                Predicate::Range {
                    column: ColumnId::Payload,
                    low: ZoneBound::Bytes(low),
                    high: ZoneBound::Bytes(high),
                }
            };

            // Oracle: does a visible row satisfy the predicate exactly?
            let real_match = rows.iter().any(|(v, sys_to)| {
                let visible = v.sys_from <= snapshot.0 && *sys_to > snapshot.0.0;
                let pred_holds = match &predicate {
                    Predicate::Eq {
                        value: ZoneBound::Bytes(value),
                        ..
                    } => v.payload == *value,
                    Predicate::Range {
                        low: ZoneBound::Bytes(low),
                        high: ZoneBound::Bytes(high),
                        ..
                    } => {
                        v.payload.as_slice() >= low.as_slice()
                            && v.payload.as_slice() <= high.as_slice()
                    }
                    _ => unreachable!("only bytes Eq/Range predicates are built above"),
                };
                visible && pred_holds
            });

            let kept = reader.might_contain(&predicate, snapshot);
            assert!(
                !real_match || kept,
                "seed {seed}: might_contain pruned a segment holding a real payload match \
                 (snapshot={snapshot:?}, predicate={predicate:?}, rows={rows:?})"
            );
        }
    }
}

/// Valid-axis counterpart to [`might_contain_never_prunes_a_real_match`]: the
/// differential no-false-negative oracle over the **valid-time** columns. This
/// closes the `valid_*` half of STL-134's Definition of done — "the zone-map
/// oracle still proves no false-negative pruning on `sys_from` / value /
/// `valid_*`" — which the system-axis and payload oracles above leave to a
/// targeted prune test ([`valid_time_range_query_prunes_non_overlapping_segments`])
/// rather than a seeded differential sweep.
///
/// Builds random valid-time segments and fires random `valid_from` / `valid_to`
/// range probes — single-column and conjoined — asserting `might_contain` never
/// answers `false` when a visible row truly falls in the queried valid range.
/// Every row is system-visible (`valid_version` stamps `sys_from = 0`, open) and
/// the snapshot is non-negative, so `snapshot_overlaps` never prunes here: the
/// valid axis is the only pruner, isolating the property under test.
#[test]
fn might_contain_never_prunes_a_real_valid_time_match() {
    let keys: [&[u8]; 4] = [b"a", b"g", b"m", b"t"];
    for seed in 0..200u64 {
        let mut rng = Lcg(seed.wrapping_mul(2_654_435_761).wrapping_add(3));
        let disk = CountingDisk::new();

        // 1..=8 valid-time rows with random, well-formed intervals
        // (`valid_to > valid_from`, the `ValidInterval` contract).
        let row_count = 1 + rng.below(8);
        let mut rows: Vec<(Version, i64, i64)> = Vec::new();
        for _ in 0..row_count {
            let key = keys[rng.below(keys.len() as u64) as usize];
            let valid_from = rng.below(100) as i64;
            let valid_to = valid_from + 1 + rng.below(100) as i64;
            rows.push((
                valid_version(key, valid_from, valid_to),
                valid_from,
                valid_to,
            ));
        }
        let versions: Vec<Version> = rows.iter().map(|(v, _, _)| v.clone()).collect();
        write_valid_segment(&disk, "vo.seg", &versions);
        let reader = SegmentReader::open(&disk, "vo.seg").expect("open");

        // All rows are visible at any non-negative snapshot (born at 0, open).
        let snapshot = snap(rng.below(50) as i64);

        for _ in 0..20 {
            // Two independent inclusive i64 ranges. Each deliberately reaches a
            // little past the [0, ~200) data range on both ends so a healthy
            // fraction of probes match nothing (exercising the prune path).
            let vf_lo = rng.below(210) as i64 - 5;
            let vf_hi = vf_lo + rng.below(120) as i64;
            let vt_lo = rng.below(210) as i64 - 5;
            let vt_hi = vt_lo + rng.below(120) as i64;
            let vf = Predicate::Range {
                column: ColumnId::ValidFrom,
                low: ZoneBound::I64(vf_lo),
                high: ZoneBound::I64(vf_hi),
            };
            let vt = Predicate::Range {
                column: ColumnId::ValidTo,
                low: ZoneBound::I64(vt_lo),
                high: ZoneBound::I64(vt_hi),
            };

            // Three probe shapes: valid_from only, valid_to only, or both
            // conjoined. The conjunction must be kept whenever a *single* row
            // satisfies both parts.
            let shape = rng.below(3);
            let predicate = match shape {
                0 => vf,
                1 => vt,
                _ => Predicate::And(vec![vf, vt]),
            };

            // Oracle: a real match is a (visible) row whose valid column(s) fall
            // in the queried range(s) — every part holding on the *same* row.
            let in_range = |x: i64, lo: i64, hi: i64| x >= lo && x <= hi;
            let real_match = rows.iter().any(|(_, valid_from, valid_to)| match shape {
                0 => in_range(*valid_from, vf_lo, vf_hi),
                1 => in_range(*valid_to, vt_lo, vt_hi),
                _ => in_range(*valid_from, vf_lo, vf_hi) && in_range(*valid_to, vt_lo, vt_hi),
            });

            let kept = reader.might_contain(&predicate, snapshot);
            assert!(
                !real_match || kept,
                "seed {seed}: might_contain pruned a valid-time segment holding a real match \
                 (snapshot={snapshot:?}, predicate={predicate:?}, rows={rows:?})"
            );
        }
    }
}

// --- DoD: validity-index–backed segment prune (STL-139) ---------------------

/// Record a materialized close `(key, sys_from) -> sys_to` in the index. The
/// closer provenance is immaterial to the prune, so it is filled in mechanically.
fn insert_close(index: &mut ValidityIndex<MemDisk>, key: &[u8], sys_from: i64, sys_to: i64) {
    index
        .insert_close(Close {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            sys_to: SystemTimeMicros(sys_to),
            closed_by: Provenance::new(
                TxnId(u64::try_from(sys_to).unwrap_or(0)),
                SystemTimeMicros(sys_to),
                Principal::new(b"closer".to_vec()),
            ),
        })
        .expect("insert close");
}

/// Derive a segment's system-time upper bound from the index over its version
/// identities — the planner's per-segment prune input ([STL-139]).
fn segment_bound(
    reader: &SegmentReader<CountingFile>,
    index: &ValidityIndex<MemDisk>,
) -> SysUpperBound {
    let keys = reader.version_keys().expect("version keys");
    index
        .sys_upper_bound(keys.iter().map(|(k, s)| (k, *s)))
        .expect("upper bound")
}

/// STL-139 DoD #1 (selectivity): the validity-index prune restores the
/// upper-bound ("all rows already superseded") system-time skip the zone map lost
/// when `sys_to` left the segment (v6, ADR-0023).
///
/// Three disjoint eras. era0's rows are all closed at 100; at snapshot 150 the
/// zone map alone *keeps* era0 (it only prunes rows born after the snapshot), but
/// the index proves every era0 row superseded at/before 150, so the planner skips
/// it — matching pre-STL-133 selectivity and saving era0's bulk-column reads. The
/// kept set still resolves to exactly the rows live at 150 (no visible row
/// dropped).
#[test]
fn validity_index_prune_skips_an_all_superseded_segment() {
    let disk = CountingDisk::new();
    write_segment(
        &disk,
        "era0.seg",
        &[version(b"a", 0, b"a@era0"), version(b"b", 10, b"b@era0")],
    );
    write_segment(
        &disk,
        "era1.seg",
        &[version(b"a", 100, b"a@era1"), version(b"b", 100, b"b@era1")],
    );
    write_segment(
        &disk,
        "era2.seg",
        &[version(b"a", 200, b"a@era2"), version(b"b", 200, b"b@era2")],
    );

    // era0 rows close at 100, era1 rows at 200, era2 rows stay open.
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    insert_close(&mut index, b"a", 0, 100);
    insert_close(&mut index, b"b", 10, 100);
    insert_close(&mut index, b"a", 100, 200);
    insert_close(&mut index, b"b", 100, 200);

    let names = ["era0.seg", "era1.seg", "era2.seg"];
    let readers: Vec<SegmentReader<_>> = names
        .iter()
        .map(|n| SegmentReader::open(&disk, n).expect("open"))
        .collect();
    let snapshot = snap(150);

    // The zone map alone keeps era0 and era1 (both have rows born <= 150) and
    // prunes only era2 (born at 200) — the one-sided lower-bound prune.
    let zone_keep: Vec<bool> = readers
        .iter()
        .map(|r| r.might_contain(&Predicate::All, snapshot))
        .collect();
    assert_eq!(zone_keep, vec![true, true, false]);

    // The index upper bounds: era0 fully closed at 100, era1 at 200, era2 open.
    let bounds: Vec<SysUpperBound> = readers.iter().map(|r| segment_bound(r, &index)).collect();
    assert_eq!(bounds[0], SysUpperBound::Bounded(SystemTimeMicros(100)));
    assert_eq!(bounds[1], SysUpperBound::Bounded(SystemTimeMicros(200)));
    assert_eq!(bounds[2], SysUpperBound::Unbounded);

    // Composed planner decision: keep iff the zone map keeps AND the index has
    // not proven the segment fully superseded at the snapshot.
    let keep: Vec<bool> = readers
        .iter()
        .zip(&bounds)
        .map(|(r, b)| {
            r.might_contain(&Predicate::All, snapshot) && !b.superseded_at_or_before(snapshot.0)
        })
        .collect();
    assert_eq!(
        keep,
        vec![false, true, false],
        "era0 now prunes on the index upper bound (all rows superseded at 100 <= 150)"
    );

    // No visible row dropped: resolving the index-kept set still yields exactly
    // the two era1 rows live at snapshot 150.
    let mut raw = Vec::new();
    for (r, &k) in readers.iter().zip(&keep) {
        if k {
            raw.extend(r.read_versions().expect("read"));
        }
    }
    let chains = fold_chains(raw, &index).expect("fold");
    let live = resolve_snapshot(&chains, snapshot);
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].payload, b"a@era1");
    assert_eq!(live[1].payload, b"b@era1");

    // I/O proof: pruning era0 on the index avoids its bulk-column reads. The
    // index-composed keep scans only era1's chunks; the zone-only keep would also
    // scan era0 — strictly more reads.
    disk.reset_reads();
    for (r, &k) in readers.iter().zip(&keep) {
        if k {
            let _ = r.read_versions().expect("read");
        }
    }
    let reads_index_pruned = disk.reads();
    disk.reset_reads();
    for (r, &k) in readers.iter().zip(&zone_keep) {
        if k {
            let _ = r.read_versions().expect("read");
        }
    }
    let reads_zone_only = disk.reads();
    assert!(
        reads_index_pruned < reads_zone_only,
        "index prune ({reads_index_pruned} reads) must scan less than the zone-only keep \
         ({reads_zone_only} reads) — it skipped era0's bulk columns"
    );
}

/// STL-139 DoD #2 (no false negatives): the validity-index prune never drops a
/// visible row. A seeded sweep builds segments with open and closed versions
/// interleaved, records the closes in the index, then for many snapshots asserts
/// the soundness contract — whenever the prune fires
/// (`superseded_at_or_before`), a brute-force oracle confirms no row of the
/// segment is visible at that snapshot. The bound's open/closed classification is
/// checked against the oracle too: `Unbounded` exactly when some row is open.
#[test]
fn validity_index_prune_never_drops_a_visible_row() {
    let keys: [&[u8]; 4] = [b"a", b"g", b"m", b"t"];
    for seed in 0..300u64 {
        let mut rng = Lcg(seed.wrapping_mul(2_654_435_761).wrapping_add(11));
        let disk = CountingDisk::new();
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");

        // 1..=8 rows with *distinct* (key, sys_from) targets — the index is
        // write-once per target, so duplicates are skipped. Each row is either
        // open (no close recorded) or closed at a sys_to strictly after its
        // sys_from. `rows` tracks the intended period end for the oracle; the
        // segment itself stores only birth state (v6, ADR-0023).
        let row_count = 1 + rng.below(8);
        let mut rows: Vec<(Version, i64)> = Vec::new();
        let mut used: std::collections::HashSet<(Vec<u8>, i64)> = std::collections::HashSet::new();
        for _ in 0..row_count {
            let key = keys[rng.below(keys.len() as u64) as usize];
            let sys_from = rng.below(100) as i64;
            if !used.insert((key.to_vec(), sys_from)) {
                continue; // duplicate target — skip to keep the index write-once
            }
            let sys_to = if rng.below(4) == 0 {
                SYSTEM_TIME_OPEN.0 // open: no close in the index
            } else {
                let to = sys_from + 1 + rng.below(100) as i64;
                insert_close(&mut index, key, sys_from, to);
                to
            };
            rows.push((version(key, sys_from, b"p"), sys_to));
        }
        let versions: Vec<Version> = rows.iter().map(|(v, _)| v.clone()).collect();
        write_segment(&disk, "s.seg", &versions);
        let reader = SegmentReader::open(&disk, "s.seg").expect("open");

        let bound = segment_bound(&reader, &index);
        let any_open = rows.iter().any(|(_, sys_to)| *sys_to == SYSTEM_TIME_OPEN.0);
        assert_eq!(
            matches!(bound, SysUpperBound::Unbounded),
            any_open,
            "seed {seed}: the bound is Unbounded exactly when some row is open (rows={rows:?})"
        );

        for _ in 0..24 {
            let snapshot = snap(rng.below(220) as i64 - 5); // includes out-of-range
            let pruned = bound.superseded_at_or_before(snapshot.0);

            // Oracle: is any row visible at the snapshot? Visible iff
            // sys_from <= snapshot < sys_to (the closed-period window).
            let any_visible = rows
                .iter()
                .any(|(v, sys_to)| v.sys_from.0 <= snapshot.0.0 && snapshot.0.0 < *sys_to);

            assert!(
                !pruned || !any_visible,
                "seed {seed}: index prune fired but a visible row exists \
                 (snapshot={snapshot:?}, rows={rows:?})"
            );
        }
    }
}
