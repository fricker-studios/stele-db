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
//!   rounded-up max bound never prune a real value/range match.

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
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::delta::{BusinessKey, Snapshot, Version};
use stele_storage::segment::{ColumnId, Predicate, SegmentReader, SegmentWriter, ZoneBound};
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

fn version(key: &[u8], sys_from: i64, sys_to: i64, payload: &[u8]) -> Version {
    Version {
        business_key: BusinessKey::new(key.to_vec()),
        sys_from: SystemTimeMicros(sys_from),
        sys_to: SystemTimeMicros(sys_to),
        provenance: Provenance::new(
            TxnId(u64::try_from(sys_from).unwrap_or(0)),
            SystemTimeMicros(sys_from),
            Principal::new(format!("svc-{sys_from}").into_bytes()),
        ),
        payload: payload.to_vec(),
    }
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

    // Era 0: [0, 100). Era 1: [100, 200). Era 2: [200, open).
    write_segment(
        &disk,
        "era0.seg",
        &[
            version(b"a", 0, 100, b"a@era0"),
            version(b"b", 10, 100, b"b@era0"),
        ],
    );
    write_segment(
        &disk,
        "era1.seg",
        &[
            version(b"a", 100, 200, b"a@era1"),
            version(b"b", 100, 200, b"b@era1"),
        ],
    );
    write_segment(
        &disk,
        "era2.seg",
        &[
            version(b"a", 200, SYSTEM_TIME_OPEN.0, b"a@era2"),
            version(b"b", 200, SYSTEM_TIME_OPEN.0, b"b@era2"),
        ],
    );

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
    assert_eq!(
        keep,
        vec![false, true, false],
        "only era1 overlaps snapshot 150"
    );

    // Now run the actual query: scan only the segments might_contain keeps.
    disk.reset_reads();
    let mut scanned = Vec::new();
    for (r, &k) in readers.iter().zip(&keep) {
        if k {
            let versions = r.read_versions().expect("read");
            scanned.extend(
                versions
                    .into_iter()
                    .filter(|v| v.sys_from <= snapshot.0 && v.sys_to > snapshot.0),
            );
        }
    }
    let reads_after_pruned_scan = disk.reads();

    // Exactly the two era1 rows are live at snapshot 150.
    scanned.sort_by(|x, y| x.business_key.cmp(&y.business_key));
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
    write_segment(&disk, "cold.seg", &[version(b"k", 100, 200, b"v")]);

    let zone_map = {
        let r = SegmentReader::open(&disk, "cold.seg").expect("open");
        r.zone_map().clone()
        // reader (and its file handle) dropped here
    };

    // Even after the segment file handle is gone, the resident map prunes.
    disk.reset_reads();
    assert!(!zone_map.might_contain(&Predicate::All, snap(50)));
    assert!(zone_map.might_contain(&Predicate::All, snap(150)));
    assert!(!zone_map.might_contain(&Predicate::All, snap(200)));
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
            version(b"m", 30, 90, b"x"),
            version(b"a", 10, 100, b"y"),
            version(b"z", 20, 80, b"w"),
        ],
    );
    let r = SegmentReader::open(&disk, "z.seg").expect("open");
    let zm = r.zone_map();

    let sys_from = zm.column(ColumnId::SysFrom).expect("sys_from stats");
    assert_eq!(sys_from.min, ZoneBound::I64(10));
    assert_eq!(sys_from.max, ZoneBound::I64(30));

    let sys_to = zm.column(ColumnId::SysTo).expect("sys_to stats");
    assert_eq!(sys_to.min, ZoneBound::I64(80));
    assert_eq!(sys_to.max, ZoneBound::I64(100));

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

        // Build a random segment of 1..=8 rows.
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
            rows.push(version(key, sys_from, sys_to, b"p"));
        }
        write_segment(&disk, "o.seg", &rows);
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
            let real_match = rows.iter().any(|v| {
                v.sys_from <= snapshot.0
                    && v.sys_to > snapshot.0
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
/// The invariant holds for any cap value, so the test stays agnostic to
/// `MAX_BYTES_STAT_PREFIX_LEN` — it only needs payloads long enough that
/// truncation definitely happens, which lengths `>= 48` guarantee.
#[test]
fn might_contain_never_prunes_a_real_match_on_truncated_payload() {
    for seed in 0..300u64 {
        let mut rng = Lcg(seed.wrapping_mul(2_654_435_761).wrapping_add(7));
        let disk = CountingDisk::new();

        // 1..=6 rows, each with an over-cap payload drawn from the stress
        // alphabet so the prefix truncation/rounding is genuinely exercised.
        let row_count = 1 + rng.below(6);
        let mut rows = Vec::new();
        for i in 0..row_count {
            let payload_len = 48 + rng.below(42) as usize; // 48..=89 bytes
            let payload = stress_bytes(&mut rng, payload_len);
            let sys_from = rng.below(100) as i64;
            let sys_to = if rng.below(4) == 0 {
                SYSTEM_TIME_OPEN.0
            } else {
                sys_from + 1 + rng.below(100) as i64
            };
            rows.push(version(
                format!("k{i}").as_bytes(),
                sys_from,
                sys_to,
                &payload,
            ));
        }
        write_segment(&disk, "p.seg", &rows);
        let reader = SegmentReader::open(&disk, "p.seg").expect("open");

        for _ in 0..24 {
            let snapshot = snap(rng.below(210) as i64 - 5);

            // Alternate between an exact-value probe and a range probe. The
            // Eq probe sometimes targets a real row's payload (forcing the
            // keep path) and sometimes a random value.
            let predicate = if rng.below(2) == 0 {
                let value = if rng.below(2) == 0 && !rows.is_empty() {
                    let idx = rng.below(rows.len() as u64) as usize;
                    rows[idx].payload.clone()
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
            let real_match = rows.iter().any(|v| {
                let visible = v.sys_from <= snapshot.0 && v.sys_to > snapshot.0;
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
