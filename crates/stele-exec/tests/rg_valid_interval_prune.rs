//! Per-row-group valid-time interval pruning — the finer-grained valid-axis
//! access path (STL-316, STL-336, ADR-0025).
//!
//! STL-241 landed the *per-segment* valid-interval summary: one coalesced union
//! per segment, consulted at the segment level. But a production flush bounds
//! row-groups (STL-197), so within a scatter-heavy segment a single row-group can
//! carry windows spanning the timeline even when the segment as a whole cannot be
//! pruned. STL-316 adds one summary *per row-group* (format v14), so the scan
//! skips an individual row-group whose coverage gaps at the pinned instant — the
//! row-group-granular refinement of STL-241, mirroring how STL-173 refined the
//! system-axis zone maps from segment to row-group granularity.
//!
//! Like the per-segment summary it is **advisory** — it changes scan *speed*,
//! never *results*. These tests pin that with the STL-233 equivalence posture:
//! the **same** rows sealed with the summary enabled vs disabled (the writer's
//! [`SegmentWriter::with_valid_interval_cap`] knob, `0` = off, which disables both
//! the per-segment and per-row-group summaries) must return byte-identical rows,
//! differing only in [`ScanStats`] — the summary-on run skips a row-group, the
//! summary-off run scans it and the row-level valid filter drops the same rows. A
//! brute-force reference is diffed alongside so the sweep is a real correctness
//! oracle, not just an on-vs-off consistency check.
//!
//! Section 3 (STL-336) exercises the same row-group summary against the *overlap*
//! probe a per-row PERIOD predicate pushes down ([`SnapshotScan::prune_valid_overlap`],
//! the row-group refinement of STL-315's segment-level overlap prune). That probe
//! is a prune *hint* — it filters no row — so its exec contract is **soundness**,
//! not byte-identity: a pruned row-group must overlap the probe nowhere, so it can
//! hold no row the downstream predicate would keep.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_exec::{Column, ScanStats, SnapshotScan};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::DmlWriter;
use stele_storage::segment::{ColumnId, SegmentReader, SegmentWriter};
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::ValidInterval;
use stele_storage::wal::{Wal, WalConfig};

// --- harness ---------------------------------------------------------------

/// A positive cap leaves the summaries on; `0` disables them (the writer omits
/// both the per-segment and the per-row-group sections, so a valid-pinned read
/// full-scans the surviving row-groups on the valid axis).
const SUMMARY_ON: usize = 256;
const SUMMARY_OFF: usize = 0;
/// Two rows per row-group, so adjacent business keys pair up into a row-group and
/// the layout below is deterministic ([STL-197] bounds production row-groups).
const GROUP_ROWS: usize = 2;

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// Deterministic, strictly-increasing clock — one tick per `now()` (ADR-0010).
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

/// The business key for slot `k` — `['k', k]`, so byte 1 recovers the slot and
/// the keys sort in slot order (so row `2g` / `2g+1` land in row-group `g`).
fn key_of(k: u8) -> BusinessKey {
    BusinessKey::new(vec![b'k', k])
}

fn iv(from: i64, to: i64) -> ValidInterval {
    ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed interval")
}

fn open(from: i64) -> ValidInterval {
    ValidInterval::new(ValidTimeMicros(from), VALID_TIME_OPEN).expect("open interval")
}

fn new_tiers() -> (Wal<MemDisk>, Delta<MemDisk>, ValidityIndex<MemDisk>) {
    (
        Wal::open(MemDisk::new(), WalConfig::default()).expect("wal"),
        Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta"),
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index"),
    )
}

/// Seal `rows` into a fresh valid-time segment, bounding row-groups at
/// [`GROUP_ROWS`] and capping the interval summaries at `cap` (`0` disables
/// them). Writing the *same* rows at two caps is the equivalence toggle:
/// identical content and identical row-group framing, summaries present or not.
fn seal_rows(disk: &MemDisk, name: &str, rows: Vec<Version>, cap: usize) -> SegmentReader<MemFile> {
    let mut w = SegmentWriter::create_valid_time(disk, name)
        .expect("create valid-time segment")
        .with_max_row_group_rows(GROUP_ROWS)
        .with_valid_interval_cap(cap);
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    SegmentReader::open(disk, name).expect("open segment")
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

/// Resolve the per-key payload live on both axes at `(s, v)` plus the run's
/// [`ScanStats`] — the prune accounting the DoD measures.
fn scan_cell(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    segments: &[SegmentReader<MemFile>],
    s: i64,
    v: i64,
) -> (BTreeMap<u8, Vec<u8>>, ScanStats) {
    let out = SnapshotScan::new(delta, index, segments, Snapshot(SystemTimeMicros(s)))
        .valid_as_of(ValidTimeMicros(v))
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .execute()
        .expect("scan");
    let keys = bytes_column(&out, ColumnId::BusinessKey);
    let payloads = bytes_column(&out, ColumnId::Payload);
    let cell = keys.iter().map(|k| k[1]).zip(payloads).collect();
    (cell, out.stats)
}

// --- 1. centerpiece: a row-group gap prunes where the segment cannot ---------

#[test]
fn a_row_group_gap_is_pruned_even_when_the_segment_summary_cannot() {
    // Four keys, two per row-group, sealed in key order:
    //   row-group 0 — keys 0,1, both valid [40, 60): a mid-band *covering* group.
    //   row-group 1 — key 2 valid [0, 10) and key 3 valid [100, +∞): a *scatter*
    //                 group whose envelope is [0, +∞) — spanning every probe — yet
    //                 whose coverage gaps over [10, 100).
    // The segment-wide union is [0,10) ∪ [40,60) ∪ [100,+∞): it *covers* v = 50,
    // so the per-segment summary (STL-241) cannot prune the segment. Only the
    // per-row-group summary (STL-316) can skip row-group 1 at v = 50.
    let disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), true);

    let inserts = [
        (0u8, iv(40, 60), b"mid0".to_vec()),
        (1, iv(40, 60), b"mid1".to_vec()),
        (2, iv(0, 10), b"old2".to_vec()),
        (3, open(100), b"new3".to_vec()),
    ];
    let mut snapshot = 0;
    for (k, valid, val) in inserts {
        snapshot = dml
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                key_of(k),
                Some(valid),
                Some(val),
                0,
                TxnId(u64::from(k) + 1),
                who(),
            )
            .expect("insert")
            .commit
            .0;
    }

    let rows = delta.flush_to_segment().expect("flush");
    let seg_on = vec![seal_rows(&disk, "on.seg", rows.clone(), SUMMARY_ON)];
    let seg_off = vec![seal_rows(&disk, "off.seg", rows, SUMMARY_OFF)];

    // v = 50 sits in the scatter group's coverage gap but inside the mid-band
    // group's window: keys 0 and 1 are valid there, keys 2 and 3 are not.
    let (on_cell, on_stats) = scan_cell(&delta, &index, &seg_on, snapshot, 50);
    let (off_cell, off_stats) = scan_cell(&delta, &index, &seg_off, snapshot, 50);

    let want = BTreeMap::from([(0u8, b"mid0".to_vec()), (1, b"mid1".to_vec())]);
    assert_eq!(on_cell, want, "the mid-band group's rows are valid at 50");
    assert_eq!(on_cell, off_cell, "summary on/off must agree");

    // The segment as a whole is never valid-pruned — its union covers 50.
    assert_eq!(
        on_stats.segments_pruned_valid, 0,
        "the segment summary cannot prune a point its union covers"
    );
    assert_eq!(on_stats.segments_scanned, 1);
    // …but the scatter row-group *is* skipped by its own summary, with no chunk
    // I/O, while the covering row-group is scanned.
    assert_eq!(on_stats.row_groups_total, 2);
    assert_eq!(
        on_stats.row_groups_pruned_valid, 1,
        "the scatter row-group's coverage gap at 50 prunes it"
    );
    assert_eq!(on_stats.row_groups_pruned_zone, 0);
    assert_eq!(on_stats.row_groups_scanned, 1);

    // Without the summaries, the min/max envelope [0, +∞) cannot prune the
    // scatter group, so both row-groups are scanned and the row-level valid
    // filter drops keys 2 and 3 — same answer, more work.
    assert_eq!(
        off_stats.row_groups_pruned_valid, 0,
        "min/max cannot prune a row-group's coverage gap"
    );
    assert_eq!(
        off_stats.row_groups_scanned, 2,
        "the summary-off run scans both row-groups"
    );

    // Teeth on the other side: at v = 5 the mid-band group is ruled out on the
    // *valid_from* min (40 > 5) by the ordinary zone map (a zone skip, not a
    // summary skip), the scatter group covers 5, and only key 2 is valid.
    let (on5, on5_stats) = scan_cell(&delta, &index, &seg_on, snapshot, 5);
    let (off5, _) = scan_cell(&delta, &index, &seg_off, snapshot, 5);
    assert_eq!(
        on5,
        BTreeMap::from([(2u8, b"old2".to_vec())]),
        "@5: only key 2"
    );
    assert_eq!(on5, off5, "@5: summary on/off must agree");
    assert_eq!(
        on5_stats.row_groups_pruned_zone, 1,
        "@5: the mid-band group is a plain zone skip (valid_from min 40 > 5)"
    );
}

// --- 2. seeded sweep: on ≡ off ≡ reference, and the row-group path fires ------

/// Tiny xorshift64* — deterministic, dependency-free.
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

const KEYS: u8 = 8; // four row-groups of two
const START: i64 = 1_000;
const OLD_HI: i64 = 10;
const MID_LO: i64 = 40;
const MID_HI: i64 = 60;
const NEW_LO: i64 = 100;

/// A random window confined to one band, with guaranteed gaps between the bands
/// `[OLD_HI, MID_LO)` and `[MID_HI, NEW_LO)` so a probe there is provably empty
/// for that key.
fn gen_banded(rng: &mut Rng, band: u8) -> ValidInterval {
    match band {
        0 => {
            let from = rng.range(OLD_HI as u64 - 1) as i64;
            iv(from, from + 1 + rng.range((OLD_HI - from) as u64) as i64)
        }
        1 => {
            let from = MID_LO + rng.range((MID_HI - MID_LO) as u64 - 1) as i64;
            iv(from, from + 1 + rng.range((MID_HI - from) as u64) as i64)
        }
        _ => {
            let from = NEW_LO + rng.range(90) as i64;
            if rng.range(3) == 0 {
                open(from)
            } else {
                iv(from, from + 1 + rng.range(90) as i64)
            }
        }
    }
}

/// One key's reference tuple: when it became system-live and its valid window.
struct Ref {
    k: u8,
    sys_from: i64,
    vfrom: i64,
    vto: i64,
    val: Vec<u8>,
}

#[test]
#[allow(clippy::too_many_lines)] // one self-contained seeded differential oracle
fn row_group_summary_on_matches_off_and_a_reference_across_a_scatter_sweep() {
    const SEEDS: u64 = 64;
    let mut total_rg_pruned_valid: u64 = 0;
    let mut total_probes: u64 = 0;
    let mut rows_seen: u64 = 0;

    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let disk = MemDisk::new();
        let (wal, mut delta, mut index) = new_tiers();
        let mut dml = DmlWriter::new(wal, StepClock::new(START), true);
        let mut refs: Vec<Ref> = Vec::new();

        // Pin the first row-group (keys 0,1) to old/new bands — a scatter group
        // whose envelope straddles the [OLD_HI, NEW_LO) gap, so its summary has
        // something to prune. Pin the second (keys 2,3) into the mid band, so the
        // segment-wide union covers mid-band probes and the segment summary
        // cannot prune them — forcing the *row-group* path to do the work. The
        // rest are random across all bands.
        for k in 0..KEYS {
            let band = match k {
                0 => 0,
                1 => 2,
                2 | 3 => 1,
                _ => (rng.range(3)) as u8,
            };
            let valid = gen_banded(&mut rng, band);
            let val = format!("k{k}").into_bytes();
            let commit = dml
                .insert(
                    &mut delta,
                    &mut index,
                    &EmptySealed,
                    key_of(k),
                    Some(valid),
                    Some(val.clone()),
                    0,
                    TxnId(u64::from(k) + 1),
                    who(),
                )
                .expect("insert")
                .commit
                .0;
            refs.push(Ref {
                k,
                sys_from: commit,
                vfrom: valid.from.0,
                vto: valid.to.0,
                val,
            });
        }
        let hi = refs.iter().map(|r| r.sys_from).max().unwrap();

        let rows = delta.flush_to_segment().expect("flush");
        let seg_on = vec![seal_rows(&disk, "on.seg", rows.clone(), SUMMARY_ON)];
        let seg_off = vec![seal_rows(&disk, "off.seg", rows, SUMMARY_OFF)];

        // Brute-force reference: key live at (s, v) iff it was committed by `s`
        // (inserts only, never superseded) and its window contains `v`.
        let reference = |s: i64, v: i64| -> BTreeMap<u8, Vec<u8>> {
            refs.iter()
                .filter(|r| r.sys_from <= s && r.vfrom <= v && v < r.vto)
                .map(|r| (r.k, r.val.clone()))
                .collect()
        };

        for s in (START - 1)..=(hi + 1) {
            for v in [
                -1i64,
                0,
                5,
                9,
                OLD_HI,
                25,
                MID_LO,
                45,
                50,
                MID_HI,
                80,
                NEW_LO - 1,
                NEW_LO,
                150,
                250,
            ] {
                let (on_cell, on_stats) = scan_cell(&delta, &index, &seg_on, s, v);
                let (off_cell, off_stats) = scan_cell(&delta, &index, &seg_off, s, v);
                let want = reference(s, v);

                assert_eq!(
                    on_cell, want,
                    "seed {seed} @ (s={s}, v={v}): summary-on vs reference"
                );
                assert_eq!(
                    off_cell, want,
                    "seed {seed} @ (s={s}, v={v}): summary-off vs reference"
                );
                // The summary-off run never prunes a row-group on the valid axis —
                // that is the toggle: any valid prune the on-run shows is the
                // per-row-group summary's doing.
                assert_eq!(
                    off_stats.row_groups_pruned_valid, 0,
                    "seed {seed} @ (s={s}, v={v}): summary-off must not valid-prune a row-group",
                );
                // The four row-group counts always partition the segment-level
                // zone survivors (STL-173, STL-316).
                assert_eq!(
                    on_stats.row_groups_total,
                    on_stats.row_groups_pruned_zone
                        + on_stats.row_groups_pruned_valid
                        + on_stats.row_groups_scanned,
                    "seed {seed} @ (s={s}, v={v}): row-group counts must partition",
                );
                total_rg_pruned_valid += on_stats.row_groups_pruned_valid as u64;
                rows_seen += on_cell.len() as u64;
                total_probes += 1;
            }
        }
    }

    assert!(
        total_rg_pruned_valid > 0,
        "the per-row-group summary never pruned a row-group — the access path was untested",
    );
    assert!(
        rows_seen > 0,
        "every probe was empty — the workload resolved nothing"
    );
    assert!(
        total_probes > 5_000,
        "sweep too small ({total_probes} probes) to trust"
    );
}

// --- 3. STL-336: the overlap-probe prune at row-group granularity ------------
//
// STL-315 pushes a per-row PERIOD predicate's constant probe `[lo, hi)` down as a
// *segment* skip; STL-316 (above) refined the *point* stab from segment to
// row-group granularity. STL-336 closes the matrix: it refines the *overlap*
// probe the same way. A scatter-heavy segment whose union overlaps `[lo, hi)` —
// so STL-315 cannot prune it — may still hold a row-group whose own coverage
// overlaps `[lo, hi)` nowhere, now skipped at row-group granularity.
//
// Unlike the point stab, the overlap probe is a prune *hint*: it filters no row
// (the engine's per-row PERIOD predicate does that downstream). So the exec-level
// contract is **soundness**, not byte-identity — a pruned row-group must overlap
// the probe nowhere, hence holds no row the downstream predicate could keep — and
// the summary-on run returns *fewer* keys than the summary-off run exactly when a
// row-group is pruned, never dropping an overlapping one. The byte-identical
// end-to-end result is the engine oracle's job, layering the real per-row filter.

/// Resolve the system-live business-key slots a scan returns at snapshot `s`,
/// pushing a valid-time overlap probe `[lo, hi)` down for row-group pruning
/// (STL-336). The probe filters no row, so this is every system-live key the
/// surviving row-groups hold, plus the run's [`ScanStats`].
fn scan_overlap(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    segments: &[SegmentReader<MemFile>],
    s: i64,
    lo: i64,
    hi: i64,
) -> (BTreeSet<u8>, ScanStats) {
    let out = SnapshotScan::new(delta, index, segments, Snapshot(SystemTimeMicros(s)))
        // Mirror the engine: a valid-time table strips the delta frame even with
        // no valid pin. The probe is a prune hint, not a filter.
        .valid_time(true)
        .prune_valid_overlap(lo, hi)
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .execute()
        .expect("scan");
    let keys = bytes_column(&out, ColumnId::BusinessKey)
        .iter()
        .map(|k| k[1])
        .collect();
    (keys, out.stats)
}

#[test]
fn a_row_group_overlap_gap_is_pruned_even_when_the_segment_summary_cannot() {
    // The same scatter layout the point-stab centerpiece uses:
    //   row-group 0 — keys 0,1, both valid [40, 60): a mid-band *covering* group.
    //   row-group 1 — key 2 valid [0, 10) and key 3 valid [100, +∞): a *scatter*
    //                 group whose envelope is [0, +∞) — overlapping every probe —
    //                 yet whose coverage gaps over [10, 100).
    // The segment-wide union [0,10) ∪ [40,60) ∪ [100,+∞) *overlaps* the probe
    // [45, 55) (via the mid-band window), so the per-segment overlap probe
    // (STL-315) cannot prune the segment. Only the per-row-group summary (STL-336)
    // can skip row-group 1, which overlaps [45, 55) nowhere.
    let disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), true);

    let inserts = [
        (0u8, iv(40, 60), b"mid0".to_vec()),
        (1, iv(40, 60), b"mid1".to_vec()),
        (2, iv(0, 10), b"old2".to_vec()),
        (3, open(100), b"new3".to_vec()),
    ];
    let mut snapshot = 0;
    for (k, valid, val) in inserts {
        snapshot = dml
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                key_of(k),
                Some(valid),
                Some(val),
                0,
                TxnId(u64::from(k) + 1),
                who(),
            )
            .expect("insert")
            .commit
            .0;
    }

    let rows = delta.flush_to_segment().expect("flush");
    let seg_on = vec![seal_rows(&disk, "on.seg", rows.clone(), SUMMARY_ON)];
    let seg_off = vec![seal_rows(&disk, "off.seg", rows, SUMMARY_OFF)];

    // [45, 55) overlaps the mid-band group but lies wholly in the scatter group's
    // coverage gap.
    let (on_keys, on_stats) = scan_overlap(&delta, &index, &seg_on, snapshot, 45, 55);
    let (off_keys, off_stats) = scan_overlap(&delta, &index, &seg_off, snapshot, 45, 55);

    // The probe filters no row, so each surviving row-group's keys come back
    // unfiltered. With the summary, row-group 1 is skipped, so only keys 0,1 (the
    // mid-band group) return; without it, every system-live key does. The dropped
    // keys 2 ([0,10)) and 3 ([100,+∞)) overlap [45, 55) nowhere — the soundness
    // the prune must uphold.
    assert_eq!(
        on_keys,
        BTreeSet::from([0, 1]),
        "only the un-pruned group's keys come back"
    );
    assert_eq!(
        off_keys,
        BTreeSet::from([0, 1, 2, 3]),
        "no prune reads every live key"
    );

    // The segment as a whole is never valid-pruned — its union overlaps [45, 55).
    assert_eq!(
        on_stats.segments_pruned_valid, 0,
        "the segment summary cannot prune a probe its union overlaps"
    );
    assert_eq!(on_stats.segments_scanned, 1);
    // …but the scatter row-group *is* skipped by its own summary, with no chunk
    // I/O, while the covering row-group is scanned.
    assert_eq!(on_stats.row_groups_total, 2);
    assert_eq!(
        on_stats.row_groups_pruned_valid, 1,
        "the scatter row-group overlaps [45, 55) nowhere — it prunes"
    );
    assert_eq!(on_stats.row_groups_pruned_zone, 0);
    assert_eq!(on_stats.row_groups_scanned, 1);

    // Without the summaries, the min/max envelope [0, +∞) cannot prune the scatter
    // group, so both row-groups are scanned — same live keys read, more work.
    assert_eq!(
        off_stats.row_groups_pruned_valid, 0,
        "min/max cannot prune a row-group's coverage gap"
    );
    assert_eq!(
        off_stats.row_groups_scanned, 2,
        "the summary-off run scans both row-groups"
    );

    // Teeth on the other side: a probe reaching a covered window in *both*
    // row-groups must prune neither — every live key is read.
    let (wide_keys, wide_stats) = scan_overlap(&delta, &index, &seg_on, snapshot, 5, 105);
    assert_eq!(
        wide_keys,
        BTreeSet::from([0, 1, 2, 3]),
        "a probe overlapping every row-group reads all keys"
    );
    assert_eq!(
        wide_stats.row_groups_pruned_valid, 0,
        "a probe overlapping every row-group prunes none"
    );
    assert_eq!(wide_stats.row_groups_scanned, 2);
}

#[test]
// One self-contained seeded differential oracle — the per-seed setup, the probe
// grid, and the soundness assertions read as a unit.
#[allow(clippy::too_many_lines)]
fn row_group_overlap_prune_is_sound_across_a_scatter_sweep() {
    const SEEDS: u64 = 64;
    let mut total_rg_pruned_valid: u64 = 0;
    let mut total_probes: u64 = 0;

    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let disk = MemDisk::new();
        let (wal, mut delta, mut index) = new_tiers();
        let mut dml = DmlWriter::new(wal, StepClock::new(START), true);
        let mut refs: Vec<Ref> = Vec::new();

        // The same banded scatter the point-stab sweep uses: row-group 0 (keys
        // 0,1) straddles the old/new gap so its summary can prune a mid-band
        // probe, while row-group 1 (keys 2,3) sits in the mid band so the
        // segment-wide union overlaps mid-band probes and the *segment* overlap
        // prune cannot fire — forcing the row-group path to do the work.
        for k in 0..KEYS {
            let band = match k {
                0 => 0,
                1 => 2,
                2 | 3 => 1,
                _ => (rng.range(3)) as u8,
            };
            let valid = gen_banded(&mut rng, band);
            let val = format!("k{k}").into_bytes();
            let commit = dml
                .insert(
                    &mut delta,
                    &mut index,
                    &EmptySealed,
                    key_of(k),
                    Some(valid),
                    Some(val.clone()),
                    0,
                    TxnId(u64::from(k) + 1),
                    who(),
                )
                .expect("insert")
                .commit
                .0;
            refs.push(Ref {
                k,
                sys_from: commit,
                vfrom: valid.from.0,
                vto: valid.to.0,
                val,
            });
        }
        let hi_s = refs.iter().map(|r| r.sys_from).max().unwrap();

        let rows = delta.flush_to_segment().expect("flush");
        let seg_on = vec![seal_rows(&disk, "on.seg", rows.clone(), SUMMARY_ON)];
        let seg_off = vec![seal_rows(&disk, "off.seg", rows, SUMMARY_OFF)];

        // Keys live at `s` whose valid interval overlaps `[lo, hi)` — the only rows
        // the downstream per-row predicate could keep, so the prune must not drop
        // any of them. (`[a, b)` overlaps `[c, d)` ⟺ `a < d && c < b`.)
        let overlapping = |s: i64, lo: i64, hi: i64| -> BTreeSet<u8> {
            refs.iter()
                .filter(|r| r.sys_from <= s && r.vfrom < hi && lo < r.vto)
                .map(|r| r.k)
                .collect()
        };
        // Every key system-live at `s` — the unfiltered scan result.
        let all_live = |s: i64| -> BTreeSet<u8> {
            refs.iter()
                .filter(|r| r.sys_from <= s)
                .map(|r| r.k)
                .collect()
        };

        for s in (START - 1)..=(hi_s + 1) {
            for &(lo, hi) in &[
                (-5i64, 0i64),
                (0, 5),
                (5, OLD_HI),
                (OLD_HI, MID_LO), // a gap below the mid band
                (20, 80),         // straddles the mid band — segment can't prune
                (MID_HI, NEW_LO), // a gap above the mid band
                (45, 55),         // inside the mid band
                (NEW_LO, 150),
                (150, 250),
                (0, 300),
            ] {
                let (on_keys, on_stats) = scan_overlap(&delta, &index, &seg_on, s, lo, hi);
                let (off_keys, off_stats) = scan_overlap(&delta, &index, &seg_off, s, lo, hi);
                let want_overlap = overlapping(s, lo, hi);

                // The summary-off run never valid-prunes a row-group — that is the
                // toggle: any valid prune the on-run shows is the row-group
                // summary's doing.
                assert_eq!(
                    off_stats.row_groups_pruned_valid, 0,
                    "seed {seed} (s={s}, [{lo}, {hi})): summary-off must not valid-prune",
                );
                assert_eq!(
                    off_keys,
                    all_live(s),
                    "seed {seed} (s={s}): the scan returns every live key unfiltered"
                );

                // Soundness: the prune never drops a row that overlaps the probe,
                // and never invents one — `want_overlap ⊆ on_keys ⊆ all_live`.
                assert!(
                    want_overlap.is_subset(&on_keys),
                    "seed {seed} (s={s}, [{lo}, {hi})): the prune dropped an overlapping row"
                );
                assert!(
                    on_keys.is_subset(&all_live(s)),
                    "seed {seed} (s={s}, [{lo}, {hi})): the prune-only scan invented a key"
                );

                // The four row-group counts always partition the segment-level
                // zone survivors (STL-173, STL-316, STL-336).
                assert_eq!(
                    on_stats.row_groups_total,
                    on_stats.row_groups_pruned_zone
                        + on_stats.row_groups_pruned_valid
                        + on_stats.row_groups_scanned,
                    "seed {seed} (s={s}, [{lo}, {hi})): row-group counts must partition",
                );

                total_rg_pruned_valid += on_stats.row_groups_pruned_valid as u64;
                total_probes += 1;
            }
        }
    }

    assert!(
        total_rg_pruned_valid > 0,
        "the per-row-group overlap probe never pruned a row-group — the access path was untested",
    );
    assert!(
        total_probes > 5_000,
        "sweep too small ({total_probes} probes) to trust"
    );
}
