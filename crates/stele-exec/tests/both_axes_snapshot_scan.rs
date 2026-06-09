//! Both-axes (`system`, `valid`) `SnapshotScan` resolution (STL-163).
//!
//! These drive a **valid-time** table through the real DML path
//! ([`DmlWriter`] with the valid-time opt-in), flush some history into sealed
//! segments via [`SegmentWriter::create_valid_time`] so reads cross the columnar
//! flush boundary, then resolve `AS OF (s, v)` with
//! [`SnapshotScan::valid_as_of`] and assert the one version live on **both** axes
//! comes back.
//!
//! The centerpiece is a **correctness oracle** ([testing strategy §4]): a naïve,
//! obviously-correct in-memory bitemporal reference model (a per-key list of
//! `(sys_interval, valid_interval, value)` tuples whose `AS OF (s, v)` answer is
//! a linear scan) is diffed against the executor over an exhaustive `(s, v)`
//! grid across a seed sweep of random histories. A deliberately-wrong (inclusive
//! valid upper bound) variant of the reference is kept alongside and asserted to
//! *disagree* with the executor at least once, so the differential is proven to
//! have teeth rather than being vacuously green.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_exec::{Column, SnapshotScan};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::DmlWriter;
use stele_storage::segment::{ColumnId, SegmentReader, SegmentWriter};
use stele_storage::systime::{EmptySealed, SealedSegments};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::ValidInterval;
use stele_storage::wal::{Wal, WalConfig};

// --- harness ---------------------------------------------------------------

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// Deterministic, strictly-increasing clock — one tick per `now()` (ADR-0010),
/// so a failing seed reproduces bit-for-bit.
struct StepClock(AtomicI64);
impl StepClock {
    const fn new(start: i64) -> Self {
        Self(AtomicI64::new(start))
    }
}
impl Clock for StepClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

/// The business key for pool slot `k` — `['k', k]`, so byte 1 recovers the slot.
fn key_of(k: u8) -> BusinessKey {
    BusinessKey::new(vec![b'k', k])
}

fn iv(from: i64, to: i64) -> ValidInterval {
    ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed interval")
}

fn new_tiers() -> (Wal<MemDisk>, Delta<MemDisk>, ValidityIndex<MemDisk>) {
    (
        Wal::open(MemDisk::new(), WalConfig::default()).expect("wal"),
        Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta"),
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index"),
    )
}

/// Drain the delta into a fresh **valid-time** sealed segment and reopen it —
/// the real columnar flush boundary, where the interval prefix is lifted off the
/// payload into first-class `valid_from` / `valid_to` columns. Returns `None`
/// when the delta is empty.
fn flush_valid(
    disk: &MemDisk,
    n: usize,
    delta: &mut Delta<MemDisk>,
) -> Option<SegmentReader<MemFile>> {
    let rows = delta.flush_to_segment().expect("flush");
    if rows.is_empty() {
        return None;
    }
    let name = format!("seg-{n}.seg");
    let mut w = SegmentWriter::create_valid_time(disk, &name).expect("create valid-time segment");
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    Some(SegmentReader::open(disk, &name).expect("open segment"))
}

/// The executor's per-key `(payload)` live on both axes at `(s, v)`: one
/// [`SnapshotScan`] at system snapshot `s` with the valid axis pinned to `v`.
/// A duplicate key is a hard failure — at most one version per key is live.
fn engine_cell(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    segments: &[SegmentReader<MemFile>],
    s: i64,
    v: i64,
) -> BTreeMap<u8, Vec<u8>> {
    let out = SnapshotScan::new(delta, index, segments, Snapshot(SystemTimeMicros(s)))
        .valid_as_of(ValidTimeMicros(v))
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .execute()
        .expect("scan");

    let keys = bytes_column(&out, ColumnId::BusinessKey);
    let payloads = bytes_column(&out, ColumnId::Payload);
    assert_eq!(keys.len(), payloads.len());
    let mut cell = BTreeMap::new();
    for (key, payload) in keys.iter().zip(&payloads) {
        assert!(
            cell.insert(key[1], payload.clone()).is_none(),
            "@ (s={s}, v={v}): two live versions for key {} — the at-most-one-live invariant broke",
            key[1],
        );
    }
    cell
}

fn bytes_column(out: &stele_exec::ScanOutput, col: ColumnId) -> Vec<Vec<u8>> {
    let (_, column) = out
        .batch
        .columns
        .iter()
        .find(|(c, _)| *c == col)
        .expect("projected column present");
    match column {
        Column::Bytes(rows) => rows
            .iter()
            .map(|c| c.clone().expect("present cell"))
            .collect(),
        Column::I64(_) => panic!("column {col:?} is i64, expected bytes"),
    }
}

// --- 1. half-open valid membership at the boundaries -----------------------

#[test]
fn valid_axis_membership_is_half_open() {
    // One key, valid `[10, 20)`, single delta version. Pin the system axis at the
    // insert commit and sweep the valid axis across both boundaries.
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), true);
    let commit = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key_of(1),
            Some(iv(10, 20)),
            Some(b"A".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;

    let segments: Vec<SegmentReader<MemFile>> = Vec::new();
    let present = |v: i64| -> Option<Vec<u8>> {
        engine_cell(&delta, &index, &segments, commit.0, v)
            .get(&1)
            .cloned()
    };

    assert_eq!(present(9), None, "before valid_from: excluded");
    assert_eq!(present(10), Some(b"A".to_vec()), "inclusive valid_from");
    assert_eq!(present(19), Some(b"A".to_vec()), "inside the interval");
    assert_eq!(present(20), None, "exclusive valid_to");
    // The emitted payload is the bare user value — the 16-byte interval prefix
    // the delta tier frames on must be stripped on a both-axes scan.
    assert_eq!(present(15), Some(b"A".to_vec()));
}

// --- 2. cross-tier: sealed segment (valid_from/valid_to columns) + delta ----

#[test]
fn both_axes_resolves_across_sealed_and_delta_tiers() {
    // key 1 valid `[0, 100)` lands in a sealed valid-time segment; key 2 valid
    // `[50, 150)` stays in the delta. At a snapshot where both are system-live,
    // the valid axis must select between them — the sealed interval read from the
    // segment's `valid_from` / `valid_to` columns, the delta interval unframed
    // from its payload.
    let seg_disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), true);

    dml.insert(
        &mut delta,
        &mut index,
        &EmptySealed,
        key_of(1),
        Some(iv(0, 100)),
        Some(b"sealed".to_vec()),
        0,
        TxnId(1),
        who(),
    )
    .expect("insert key 1");

    // Flush key 1 into a sealed valid-time segment; it now lives only there.
    let segments = vec![flush_valid(&seg_disk, 0, &mut delta).expect("seal")];
    assert_eq!(segments[0].row_count(), 1);

    let c2 = dml
        .insert(
            &mut delta,
            &mut index,
            &SealedSegments::new(&segments),
            key_of(2),
            Some(iv(50, 150)),
            Some(b"delta".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .expect("insert key 2")
        .commit;

    let cell = |v: i64| engine_cell(&delta, &index, &segments, c2.0, v);

    // v = 25: only key 1 (sealed) is valid.
    assert_eq!(cell(25), BTreeMap::from([(1, b"sealed".to_vec())]));
    // v = 75: both keys are valid.
    assert_eq!(
        cell(75),
        BTreeMap::from([(1, b"sealed".to_vec()), (2, b"delta".to_vec())]),
    );
    // v = 125: only key 2 (delta) is valid.
    assert_eq!(cell(125), BTreeMap::from([(2, b"delta".to_vec())]));
}

// --- 3. the two axes are independent ---------------------------------------

#[test]
fn system_supersession_and_valid_membership_are_independent() {
    // INSERT key 1 valid `[0, 100)` "A"; UPDATE it to valid `[200, 300)` "B".
    // The update supersedes "A" on the SYSTEM axis at its commit, and the new
    // version is valid in a disjoint window. So:
    //   * AS OF (insert, v=50) → "A"   (pre-update system, A's valid window)
    //   * AS OF (update, v=50) → none  (A superseded; B not valid at 50)
    //   * AS OF (update, v=250) → "B"  (post-update system, B's valid window)
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), true);
    let segments: Vec<SegmentReader<MemFile>> = Vec::new();

    let c_insert = dml
        .insert(
            &mut delta,
            &mut index,
            &SealedSegments::new(&segments),
            key_of(1),
            Some(iv(0, 100)),
            Some(b"A".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;
    let c_update = dml
        .update(
            &mut delta,
            &mut index,
            &SealedSegments::new(&segments),
            key_of(1),
            Some(iv(200, 300)),
            Some(b"B".to_vec()),
            1,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit;

    let cell = |s: i64, v: i64| {
        engine_cell(&delta, &index, &segments, s, v)
            .get(&1)
            .cloned()
    };
    assert_eq!(cell(c_insert.0, 50), Some(b"A".to_vec()));
    assert_eq!(cell(c_update.0, 50), None);
    assert_eq!(cell(c_update.0, 250), Some(b"B".to_vec()));
    // A's valid window pre-update is unaffected by the later correction.
    assert_eq!(cell(c_insert.0, 250), None);
}

// --- 4. the correctness oracle: differential vs a naïve reference model -----

/// Tiny xorshift64* — deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// A random well-formed valid interval inside `[0, vmax]`, occasionally
/// open-ended to exercise the `+∞` valid sentinel.
fn gen_valid(rng: &mut Rng, vmax: i64) -> ValidInterval {
    let from = rng.range((vmax - 1) as u64) as i64;
    if rng.range(4) == 0 {
        ValidInterval::new(ValidTimeMicros(from), VALID_TIME_OPEN).expect("open interval")
    } else {
        let span = 1 + rng.range((vmax - from) as u64) as i64;
        iv(from, from + span)
    }
}

/// One naïve, obviously-correct version tuple: both axes as half-open intervals
/// plus the user value. `sys_to == i64::MAX` is an open system period.
#[derive(Clone)]
struct RefVersion {
    sys_from: i64,
    sys_to: i64,
    vfrom: i64,
    vto: i64,
    val: Vec<u8>,
}

/// The naïve bitemporal reference: per key, an append-only list of version
/// tuples maintained by the same INSERT/UPDATE/DELETE semantics the engine uses.
#[derive(Default)]
struct RefModel {
    versions: BTreeMap<u8, Vec<RefVersion>>,
}

impl RefModel {
    fn open_idx(&self, k: u8) -> Option<usize> {
        self.versions
            .get(&k)
            .and_then(|vs| vs.iter().position(|v| v.sys_to == i64::MAX))
    }
    fn close(&mut self, k: u8, commit: i64) {
        let i = self
            .open_idx(k)
            .expect("a live key has exactly one open period");
        self.versions.get_mut(&k).unwrap()[i].sys_to = commit;
    }
    fn insert(&mut self, k: u8, commit: i64, valid: ValidInterval, val: &[u8]) {
        self.versions.entry(k).or_default().push(RefVersion {
            sys_from: commit,
            sys_to: i64::MAX,
            vfrom: valid.from.0,
            vto: valid.to.0,
            val: val.to_vec(),
        });
    }
    fn update(&mut self, k: u8, commit: i64, valid: ValidInterval, val: &[u8]) {
        self.close(k, commit);
        self.insert(k, commit, valid, val);
    }

    /// The per-key value live on both axes at `(s, v)`. `inclusive_vto` flips the
    /// valid upper bound to inclusive — the deliberately-wrong variant used to
    /// prove the differential has teeth.
    fn cell(&self, s: i64, v: i64, inclusive_vto: bool) -> BTreeMap<u8, Vec<u8>> {
        let mut out = BTreeMap::new();
        for (k, vs) in &self.versions {
            for ver in vs {
                let sys_ok = ver.sys_from <= s && s < ver.sys_to;
                let valid_ok = ver.vfrom <= v
                    && (if inclusive_vto {
                        v <= ver.vto
                    } else {
                        v < ver.vto
                    });
                if sys_ok && valid_ok {
                    assert!(
                        out.insert(*k, ver.val.clone()).is_none(),
                        "2D-tiling: one row per (s,v,k)"
                    );
                }
            }
        }
        out
    }
}

/// Knobs shared by [`run_seed`] and the differential sweep.
const KEY_POOL: u8 = 4;
const START: i64 = 1_000;
const VMAX: i64 = 12;
const SEEDS: u64 = 64;

/// One seed's built history: the engine tiers, the naïve reference holding the
/// identical history, the last commit tick, and how many segments were sealed.
struct SeedRun {
    delta: Delta<MemDisk>,
    index: ValidityIndex<MemDisk>,
    segments: Vec<SegmentReader<MemFile>>,
    model: RefModel,
    hi: i64,
    flushes: usize,
}

/// Apply one seed's random INSERT/UPDATE/DELETE history (valid-time table) to
/// **both** the engine and the reference, sealing the delta at random so reads
/// later cross the flush boundary. Both ride the engine's actual commit ticks.
fn run_seed(seed: u64) -> SeedRun {
    let mut rng = Rng::new(seed);
    let seg_disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(START), true);
    let mut segments: Vec<SegmentReader<MemFile>> = Vec::new();
    let mut model = RefModel::default();
    let mut alive = vec![false; KEY_POOL as usize];
    let mut hi = START;
    let mut flushes = 0usize;

    let ops = 8 + rng.range(16);
    for op in 0..ops {
        let k = rng.range(u64::from(KEY_POOL)) as u8;
        let key = key_of(k);
        let txn = TxnId(op);
        let val = format!("k{k}-op{op}").into_bytes();
        let sealed = SealedSegments::new(&segments);

        let commit = if alive[k as usize] && rng.range(2) == 0 {
            let c = dml
                .delete(&mut delta, &mut index, &sealed, &key, txn, who())
                .expect("delete")
                .commit;
            model.close(k, c.0);
            alive[k as usize] = false;
            c
        } else if alive[k as usize] {
            let valid = gen_valid(&mut rng, VMAX);
            let c = dml
                .update(
                    &mut delta,
                    &mut index,
                    &sealed,
                    key,
                    Some(valid),
                    Some(val.clone()),
                    op,
                    txn,
                    who(),
                )
                .expect("update")
                .commit;
            model.update(k, c.0, valid, &val);
            c
        } else {
            let valid = gen_valid(&mut rng, VMAX);
            let c = dml
                .insert(
                    &mut delta,
                    &mut index,
                    &sealed,
                    key,
                    Some(valid),
                    Some(val.clone()),
                    op,
                    txn,
                    who(),
                )
                .expect("insert")
                .commit;
            model.insert(k, c.0, valid, &val);
            alive[k as usize] = true;
            c
        };
        hi = hi.max(commit.0);

        // Occasionally seal the delta so later reads cross the flush boundary.
        if rng.range(4) == 0 {
            if let Some(reader) = flush_valid(&seg_disk, flushes, &mut delta) {
                segments.push(reader);
                flushes += 1;
            }
        }
    }

    SeedRun {
        delta,
        index,
        segments,
        model,
        hi,
        flushes,
    }
}

#[test]
fn duckdb_free_differential_matches_a_naive_reference() {
    let mut total_probes: u64 = 0;
    let mut total_flushes: usize = 0;
    let mut rows_seen: u64 = 0;
    // The differential must, at least once, diverge from the inclusive-`vto`
    // reference — otherwise the half-open valid boundary is never actually
    // probed and the oracle proves nothing.
    let mut teeth = false;

    for seed in 0..SEEDS {
        let run = run_seed(seed);
        total_flushes += run.flushes;

        for s in (START - 1)..=(run.hi + 1) {
            for v in -1..=(VMAX + 1) {
                let got = engine_cell(&run.delta, &run.index, &run.segments, s, v);
                let want = run.model.cell(s, v, false);
                assert_eq!(
                    got, want,
                    "seed {seed}: executor diverged from the reference at (s={s}, v={v})"
                );
                if got != run.model.cell(s, v, true) {
                    teeth = true;
                }
                rows_seen += got.len() as u64;
                total_probes += 1;
            }
        }
    }

    assert!(
        total_flushes > 0,
        "no seed ever sealed a segment — the sealed valid-time path went untested"
    );
    assert!(
        rows_seen > 0,
        "every probe was empty — the workload resolved nothing"
    );
    assert!(
        teeth,
        "the differential never hit a half-open valid boundary — it cannot detect an off-by-one"
    );
    assert!(
        total_probes > 10_000,
        "differential probed only {total_probes} (s,v) cells — widen the sweep"
    );
}
