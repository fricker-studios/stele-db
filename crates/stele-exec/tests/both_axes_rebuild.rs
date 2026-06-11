//! Bitemporal DML: the joint system + valid deletion gap survives a **full
//! from-scratch validity-index rebuild** (STL-166, [docs/16 §12], [ADR-0023]).
//!
//! STL-94 gave DML system-axis close/open; STL-143 made retractions durable
//! tombstone rows so a rebuild from the segment store alone can never lose a
//! deletion gap; STL-163 wired the **valid** axis into `SnapshotScan`. This file
//! ties the three together on a **both-axes** (system + valid) table — the
//! historization core of full bitemporality — and proves the one property none of
//! those tickets did:
//!
//! > On a valid-time table, an interleaved INSERT/UPDATE/DELETE workload resolved
//! > `AS OF (s, v)` is **byte-identical** whether the validity index was built by
//! > the live DML path or rebuilt **from the sealed segments alone**, and a DELETE
//! > leaves a deletion gap that is ABSENT across the **entire valid axis** for every
//! > system snapshot in the gap.
//!
//! The deletion gap is a both-axes property even though a retraction is a
//! system-axis fact: a delete at `t3` closes the version's system period, so for
//! `s ∈ [t3, t4)` *no* version is system-live and the key is ABSENT at **every**
//! valid point `v` — the gap is a full-height vertical strip in (system × valid)
//! space, not a notch on one axis. The rebuild reconstructs only the system-axis
//! `sys_to`; the valid interval rides on the version (segment `valid_from` /
//! `valid_to` columns), so getting the system close right is what makes the gap
//! correct on both axes after a rebuild.
//!
//! The centerpiece is the same **correctness oracle** shape as STL-163's
//! `both_axes_snapshot_scan.rs`: a naïve, obviously-correct in-memory bitemporal
//! reference is diffed against the executor over an exhaustive `(s, v)` grid across
//! a seed sweep — but here the executor reads through an index **rebuilt from
//! segments**, and a bundled adjacency-only rebuild is shown to resurrect the
//! deleted row across the gap, so the oracle has teeth.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_exec::{Column, SnapshotScan};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::DmlWriter;
use stele_storage::rebuild::rebuild_index_from_segments;
use stele_storage::segment::{ColumnId, SegmentReader, SegmentWriter};
use stele_storage::systime::{EmptySealed, SealedSegments};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::ValidInterval;
use stele_storage::wal::{Wal, WalConfig};

// --- harness ---------------------------------------------------------------

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// Deterministic, strictly-increasing clock — one tick per `now()` ([ADR-0010]),
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

/// A hand-driven clock behind a shared atomic — for the focused canonical demo,
/// where the test sets the exact wall position between statements so the deletion
/// gap has interior points to probe.
#[derive(Clone)]
struct StubClock(Arc<AtomicI64>);
impl StubClock {
    fn new(start: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start)))
    }
    fn advance(&self, by: i64) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }
}
impl Clock for StubClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.load(Ordering::Relaxed))
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

/// Drain **both** the delta's versions and its persisted retraction tombstones into
/// one fresh **valid-time** segment, then reopen it. Unlike STL-163's flush helper
/// (which only sealed versions, reading through the live index), a rebuild needs
/// the tombstones in the segment store too — so this seals `take_retractions()`
/// alongside, exactly as the real flush path does. Returns `None` when the delta
/// held nothing.
fn flush_valid(
    disk: &MemDisk,
    n: usize,
    delta: &mut Delta<MemDisk>,
) -> Option<SegmentReader<MemFile>> {
    let versions = delta.flush_to_segment().expect("flush");
    let retractions = delta.take_retractions();
    if versions.is_empty() && retractions.is_empty() {
        return None;
    }
    let name = format!("seg-{n}.seg");
    let mut w = SegmentWriter::create_valid_time(disk, &name).expect("create valid-time segment");
    for v in versions {
        w.push(v).expect("push version");
    }
    for r in retractions {
        w.push_retraction(r).expect("push retraction");
    }
    w.finish().expect("finish");
    Some(SegmentReader::open(disk, &name).expect("open segment"))
}

/// Rebuild a fresh validity index from the **union of every segment's** versions
/// and persisted retractions — the from-scratch path ([`rebuild_index_from_segments`]),
/// with no reference to the WAL or the live index.
fn rebuild_from(segments: &[SegmentReader<MemFile>]) -> ValidityIndex<MemDisk> {
    let mut versions = Vec::new();
    let mut retractions = Vec::new();
    for seg in segments {
        versions.extend(seg.read_versions().expect("read versions"));
        retractions.extend(seg.read_retractions().expect("read retractions"));
    }
    let mut rebuilt =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("rebuilt index");
    rebuild_index_from_segments(versions, retractions, &mut rebuilt).expect("rebuild");
    rebuilt
}

/// The executor's per-key payload live on **both** axes at `(s, v)`: one
/// [`SnapshotScan`] at system snapshot `s` with the valid axis pinned to `v`,
/// reading `delta` + `segments` with `index` supplying each version's system end.
/// A duplicate key is a hard failure — at most one version per key is live (the
/// 2D-tiling invariant).
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

// --- the naïve bitemporal reference (mirrors STL-163's) --------------------

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

/// The naïve bitemporal reference: per key, an append-only list of version tuples
/// maintained by the same INSERT/UPDATE/DELETE semantics the engine uses. A DELETE
/// is "close, don't reopen" — it closes the open period on the system axis and
/// opens nothing, leaving a gap that is ABSENT at every valid point.
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
    /// prove the differential has teeth on the valid axis.
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

// --- 1. the canonical §12 deletion gap, on both axes, survives rebuild ------

/// `INSERT@t0 → UPDATE@t1 → UPDATE@t2 → DELETE@t3 → re-INSERT@t4` on a **valid-time**
/// table. Flush every version *and the persisted retraction* into one sealed
/// valid-time segment, rebuild the validity index from the segment store alone,
/// and assert the `AS OF (s, v)` answer over a `(system × valid)` grid is
/// byte-identical before and after the rebuild — and that the deletion gap
/// `[t3, t4)` is ABSENT at **every** valid point. An adjacency-only rebuild
/// (ignoring the tombstone) is shown to resurrect the row across the gap, proving
/// the oracle is not vacuous.
#[test]
#[allow(clippy::too_many_lines)] // a linear five-statement history + an exhaustive (system × valid) sweep
fn deletion_gap_is_absent_across_the_whole_valid_axis_after_rebuild() {
    let seg_disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let clock = StubClock::new(1_000);
    let mut dml = DmlWriter::new(wal, clock.clone(), true);
    let key = key_of(1);

    // A linear history whose valid windows shift, so the valid axis is exercised
    // independently of the system axis.
    let t0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(iv(0, 100)),
            Some(b"v0".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;
    clock.advance(10);
    let t1 = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(iv(0, 100)),
            Some(b"v1".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit;
    clock.advance(10);
    let t2 = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(iv(50, 200)),
            Some(b"v2".to_vec()),
            0,
            TxnId(3),
            who(),
        )
        .expect("update")
        .commit;
    clock.advance(10);
    let t3 = dml
        .delete(&mut delta, &mut index, &EmptySealed, &key, TxnId(4), who())
        .expect("delete")
        .commit;
    clock.advance(10);
    let t4 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(iv(0, 100)),
            Some(b"v4".to_vec()),
            0,
            TxnId(5),
            who(),
        )
        .expect("re-insert")
        .commit;
    assert!(t0 < t1 && t1 < t2 && t2 < t3 && t3 < t4);

    // Seal the whole history into one valid-time segment; the delta is now empty,
    // so every read below comes from the segment store + the index alone.
    let segments = vec![flush_valid(&seg_disk, 0, &mut delta).expect("seal")];
    assert_eq!(
        segments[0].row_count(),
        4,
        "v0,v1,v2,v4 — the delete opened no version"
    );
    assert_eq!(
        segments[0].read_retractions().expect("retractions").len(),
        1,
        "the delete persisted exactly one tombstone into the valid-time segment"
    );

    // The (system × valid) probe grid: system snapshots straddling every commit and
    // the gap interior, valid points on both sides of each window boundary.
    let gap_mid = i64::midpoint(t3.0, t4.0);
    let sys_points = [
        t2.0,     // last live before the delete
        t3.0 - 1, // half-open: still live up to t3
        t3.0,     // at the delete: ABSENT
        gap_mid,  // mid-gap: ABSENT
        t4.0 - 1, // half-open: still ABSENT up to t4
        t4.0,     // re-insert visible
    ];
    let valid_points = [-1, 0, 25, 49, 50, 99, 100, 150, 199, 200];

    let probe = |index: &ValidityIndex<MemDisk>| -> Vec<(i64, i64, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        for &s in &sys_points {
            for &v in &valid_points {
                let got = engine_cell(&delta, index, &segments, s, v).get(&1).cloned();
                out.push((s, v, got));
            }
        }
        out
    };

    // Byte-identical before (live index) and after (rebuilt from segments alone).
    let before = probe(&index);
    let rebuilt = rebuild_from(&segments);
    let after = probe(&rebuilt);
    assert_eq!(
        before, after,
        "as-of over the (system × valid) grid is byte-identical before and after a full rebuild",
    );

    // The deletion gap is ABSENT across the ENTIRE valid axis for every system
    // snapshot inside it — the both-axes deletion gap (docs/16 §12).
    for &s in &[t3.0, gap_mid, t4.0 - 1] {
        for &v in &valid_points {
            assert_eq!(
                engine_cell(&delta, &rebuilt, &segments, s, v).get(&1),
                None,
                "deletion gap must be ABSENT at every valid point: (s={s}, v={v})",
            );
        }
    }

    // Spot-check the live windows resolve on the valid axis (so "all ABSENT in the
    // gap" is a real gap, not the whole key being invisible everywhere).
    assert_eq!(
        engine_cell(&delta, &rebuilt, &segments, t2.0, 75).get(&1),
        Some(&b"v2".to_vec()),
        "v2's window [50,200) is live just before the delete",
    );
    assert_eq!(
        engine_cell(&delta, &rebuilt, &segments, t2.0, 25).get(&1),
        None,
        "v2's window excludes 25 even while system-live — valid axis is independent",
    );
    assert_eq!(
        engine_cell(&delta, &rebuilt, &segments, t4.0, 25).get(&1),
        Some(&b"v4".to_vec()),
        "the re-insert's window [0,100) is live again after the gap",
    );

    // Teeth: an adjacency-only rebuild (no tombstone) RESURRECTS v2 across the gap.
    let mut versions = Vec::new();
    for seg in &segments {
        versions.extend(seg.read_versions().expect("versions"));
    }
    let mut adjacency_only =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("adj");
    rebuild_index_from_segments(versions, Vec::new(), &mut adjacency_only).expect("adj rebuild");
    assert_eq!(
        engine_cell(&delta, &adjacency_only, &segments, gap_mid, 75).get(&1),
        Some(&b"v2".to_vec()),
        "an adjacency-only rebuild resurrects the deleted row across the gap — exactly the bug \
         the persisted retraction prevents, here proven on a both-axes table",
    );
}

// --- 2. the correctness oracle: differential vs a naïve reference, rebuilt --

/// Tiny xorshift64* — deterministic, dependency-free; matches the sibling tests so
/// a failing seed reproduces bit-for-bit ([ADR-0010]).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    const fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    const fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// A random well-formed valid interval inside `[0, vmax]`, occasionally open-ended
/// to exercise the `+∞` valid sentinel.
fn gen_valid(rng: &mut Rng, vmax: i64) -> ValidInterval {
    let from = rng.range((vmax - 1) as u64) as i64;
    if rng.range(4) == 0 {
        ValidInterval::new(ValidTimeMicros(from), VALID_TIME_OPEN).expect("open interval")
    } else {
        let span = 1 + rng.range((vmax - from) as u64) as i64;
        iv(from, from + span)
    }
}

const KEY_POOL: u8 = 4;
const START: i64 = 1_000;
const VMAX: i64 = 12;
const SEEDS: u64 = 64;

/// One seed's built history: the (now-drained) delta, the live validity index, the
/// full segment store, the naïve reference holding the identical history, the last
/// commit tick, and how many segments were sealed.
struct SeedRun {
    delta: Delta<MemDisk>,
    index: ValidityIndex<MemDisk>,
    segments: Vec<SegmentReader<MemFile>>,
    model: RefModel,
    hi: i64,
    flushes: usize,
}

/// Apply one seed's random INSERT/UPDATE/DELETE history (valid-time table) to both
/// the engine and the reference, sealing the delta — versions **and** persisted
/// retractions — into the segment store at random points and once at the end, so a
/// from-scratch rebuild reads a self-contained store and some closes/deletes span
/// the columnar flush boundary.
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

        // Occasionally seal so deletes/closes span the columnar flush boundary.
        if rng.range(4) == 0
            && let Some(reader) = flush_valid(&seg_disk, flushes, &mut delta)
        {
            segments.push(reader);
            flushes += 1;
        }
    }

    // Force a final flush so the segment store is self-contained for a from-scratch
    // rebuild — anything still resident (versions or tombstones) is sealed now.
    if let Some(reader) = flush_valid(&seg_disk, flushes, &mut delta) {
        segments.push(reader);
        flushes += 1;
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

/// The v0.2 bitemporal-DML gate, rebuild edition ([STL-77 \[A5\]]): a random
/// interleaved both-axes workload, read back through an index **rebuilt from the
/// sealed segments alone**, must match a naïve bitemporal reference at every
/// `(s, v)` — and equal the live-index answer byte-for-byte. The deliberately-wrong
/// inclusive-`vto` reference is asserted to diverge at least once, so the half-open
/// valid boundary is provably probed.
#[test]
fn rebuilt_index_differential_matches_a_naive_reference() {
    let mut total_probes: u64 = 0;
    let mut total_flushes: usize = 0;
    let mut total_retractions: usize = 0;
    let mut rows_seen: u64 = 0;
    // The differential must, at least once, diverge from the inclusive-`vto`
    // reference — otherwise the half-open valid boundary is never actually probed.
    let mut teeth = false;

    for seed in 0..SEEDS {
        let run = run_seed(seed);
        total_flushes += run.flushes;
        // Count persisted tombstones: the rebuild oracle only proves something
        // about deletes if the seeded workload actually deleted and the tombstone
        // landed in the segment store (a vacuous "saw an empty probe" check would
        // pass on the always-empty `s = START-1` / `v = -1` grid corners).
        total_retractions += run
            .segments
            .iter()
            .map(|seg| seg.read_retractions().expect("read retractions").len())
            .sum::<usize>();
        // Everything is sealed; the delta is empty. Build the index from the
        // segment store alone — the property under test.
        let rebuilt = rebuild_from(&run.segments);

        for s in (START - 1)..=(run.hi + 1) {
            for v in -1..=(VMAX + 1) {
                let got = engine_cell(&run.delta, &rebuilt, &run.segments, s, v);
                let want = run.model.cell(s, v, false);
                assert_eq!(
                    got, want,
                    "seed {seed}: rebuilt-index executor diverged from the reference at (s={s}, v={v})",
                );
                // The rebuilt index must agree with the live index byte-for-byte.
                let live = engine_cell(&run.delta, &run.index, &run.segments, s, v);
                assert_eq!(
                    got, live,
                    "seed {seed}: rebuilt-from-segments read differs from the live index at (s={s}, v={v})",
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
        "no seed ever sealed a segment — the rebuild path went untested"
    );
    assert!(
        rows_seen > 0,
        "every probe was empty — the workload resolved nothing"
    );
    assert!(
        total_retractions > 0,
        "no seed persisted a retraction tombstone — DELETE-driven deletion gaps were never \
         exercised, so the rebuild oracle proved nothing about deletes"
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
