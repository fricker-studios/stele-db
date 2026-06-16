//! Per-segment valid-time interval pruning — the scatter-resistant valid-axis
//! access path (STL-241, ADR-0025).
//!
//! System-time prunes well via zone maps because it is monotonic. **Valid-time
//! is not**: a backdated correction lands in *today's* segment carrying an *old*
//! valid-time, so the segment's `valid_from` / `valid_to` min/max envelope spans
//! almost the whole timeline and the zone-map valid-axis skips (STL-173) prune
//! nothing — even though the actual covered windows are sparse. The per-segment
//! valid-time interval summary records the **union** of the covered windows, so a
//! `FOR VALID_TIME AS OF v` whose `v` falls in a coverage *gap* skips the whole
//! segment regardless of how wide the envelope is.
//!
//! The summary is **advisory** — it changes scan *speed*, never *results*. These
//! tests pin that with the STL-233 equivalence posture: the **same** segment
//! written with the summary enabled vs disabled (the writer's
//! [`SegmentWriter::with_valid_interval_cap`] knob, `0` = off) must return
//! byte-identical rows, differing only in [`ScanStats`] — the summary-on run
//! skips the segment, the summary-off run scans it and the row-level valid filter
//! drops the same rows. A naïve brute-force reference is diffed alongside so the
//! sweep is a real correctness oracle, not just an on-vs-off consistency check.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
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

/// A positive cap leaves the summary on; `0` disables it (the writer omits the
/// section, so a valid-pinned read full-scans the segment on the valid axis).
const SUMMARY_ON: usize = 256;
const SUMMARY_OFF: usize = 0;

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

/// The business key for slot `k` — `['k', k]`, so byte 1 recovers the slot.
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

/// Seal `rows` into a fresh valid-time segment with the interval summary capped
/// at `cap` (`0` disables it). Writing the *same* rows at two caps is the
/// equivalence toggle: identical content, summary present or not.
fn seal_rows(disk: &MemDisk, name: &str, rows: Vec<Version>, cap: usize) -> SegmentReader<MemFile> {
    let mut w = SegmentWriter::create_valid_time(disk, name)
        .expect("create valid-time segment")
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

// --- 1. the centerpiece: a coverage gap prunes only with the summary --------

#[test]
fn a_scatter_segment_is_pruned_on_a_valid_gap_only_with_the_summary() {
    // One segment holds a backdated window [0, 10) (key 1) and a current open
    // window [100, +∞) (key 2). Its valid envelope is [0, +∞) — spanning every
    // probe — yet no row is valid in the gap [10, 100). Both keys are inserts
    // (system-live at the shared snapshot), so the system axis never prunes
    // either; the valid axis is the whole story.
    let disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), true);

    dml.insert(
        &mut delta,
        &mut index,
        &EmptySealed,
        key_of(1),
        Some(iv(0, 10)),
        Some(b"early".to_vec()),
        0,
        TxnId(1),
        who(),
    )
    .expect("insert key 1");
    let snapshot = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key_of(2),
            Some(open(100)),
            Some(b"current".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .expect("insert key 2")
        .commit
        .0;

    // The same rows sealed twice: with the summary, and without it.
    let rows = delta.flush_to_segment().expect("flush");
    let seg_on = vec![seal_rows(&disk, "on.seg", rows.clone(), SUMMARY_ON)];
    let seg_off = vec![seal_rows(&disk, "off.seg", rows, SUMMARY_OFF)];

    // v = 50 sits in the coverage gap: no row is valid there.
    let (on_cell, on_stats) = scan_cell(&delta, &index, &seg_on, snapshot, 50);
    let (off_cell, off_stats) = scan_cell(&delta, &index, &seg_off, snapshot, 50);

    // Same (empty) answer either way — the summary changes speed, not results.
    assert!(on_cell.is_empty(), "no row is valid at the gap point");
    assert_eq!(on_cell, off_cell, "summary on/off must agree");

    // With the summary the whole segment is skipped with no chunk I/O…
    assert_eq!(on_stats.segments_total, 1);
    assert_eq!(
        on_stats.segments_pruned_valid, 1,
        "the gap prunes the segment"
    );
    assert_eq!(on_stats.segments_scanned, 0);
    // …while without it the zone-map min/max envelope [0, +∞) cannot prune, so
    // the segment is scanned and the row-level valid filter drops both rows.
    assert_eq!(
        off_stats.segments_pruned_valid, 0,
        "min/max cannot prune a gap"
    );
    assert_eq!(
        off_stats.segments_scanned, 1,
        "the summary-off run must scan"
    );

    // Teeth on the other side: a point each band *covers* is never pruned, and
    // returns exactly the row valid there — identical on/off.
    for (v, want) in [
        (5i64, (1u8, b"early".to_vec())),
        (150, (2, b"current".to_vec())),
    ] {
        let (on_cell, on_stats) = scan_cell(&delta, &index, &seg_on, snapshot, v);
        let (off_cell, _) = scan_cell(&delta, &index, &seg_off, snapshot, v);
        assert_eq!(on_cell, BTreeMap::from([want]), "@ v={v}: the covered row");
        assert_eq!(on_cell, off_cell, "@ v={v}: summary on/off must agree");
        assert_eq!(
            on_stats.segments_pruned_valid, 0,
            "@ v={v}: a covered point must not prune the segment"
        );
        assert_eq!(
            on_stats.segments_scanned, 1,
            "@ v={v}: the segment is scanned"
        );
    }
}

// --- 2. seeded sweep: on ≡ off ≡ reference, and the summary actually fires ---

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

const KEYS: u8 = 6;
const START: i64 = 1_000;
/// The backdated ("old") band and the current ("new") band, with a guaranteed
/// gap between them: no interval spans `[OLD_HI, NEW_LO)`, so a probe there is
/// provably empty and the summary must prune.
const OLD_HI: i64 = 10;
const NEW_LO: i64 = 100;

/// A random window confined to one band, so the gap `[OLD_HI, NEW_LO)` stays
/// uncovered. Old-band windows are always bounded below `OLD_HI`; new-band
/// windows start at/after `NEW_LO` and are occasionally open-ended.
fn gen_banded(rng: &mut Rng, old_band: bool) -> ValidInterval {
    if old_band {
        let from = rng.range(OLD_HI as u64 - 1) as i64;
        let span = 1 + rng.range((OLD_HI - from) as u64) as i64;
        iv(from, from + span)
    } else {
        let from = NEW_LO + rng.range(90) as i64;
        if rng.range(3) == 0 {
            open(from)
        } else {
            let span = 1 + rng.range(90) as i64;
            iv(from, from + span)
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
fn summary_on_matches_summary_off_and_a_reference_across_a_backdated_sweep() {
    const SEEDS: u64 = 64;
    let mut total_pruned_valid: u64 = 0;
    let mut total_probes: u64 = 0;
    let mut rows_seen: u64 = 0;

    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let disk = MemDisk::new();
        let (wal, mut delta, mut index) = new_tiers();
        let mut dml = DmlWriter::new(wal, StepClock::new(START), true);
        let mut refs: Vec<Ref> = Vec::new();

        // Insert one version per key, each confined to a band. Keys 0 and 1 are
        // pinned to the old and new bands respectively so every segment straddles
        // the gap — guaranteeing the summary has something to prune.
        for k in 0..KEYS {
            let old_band = match k {
                0 => true,
                1 => false,
                _ => rng.range(2) == 0,
            };
            let valid = gen_banded(&mut rng, old_band);
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
                50,
                NEW_LO - 1,
                NEW_LO,
                150,
                199,
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
                // The summary-off run never prunes on the valid axis — that is
                // the toggle: any valid prune the on-run shows is the summary's.
                assert_eq!(
                    off_stats.segments_pruned_valid, 0,
                    "seed {seed} @ (s={s}, v={v}): summary-off must not valid-prune",
                );
                total_pruned_valid += on_stats.segments_pruned_valid as u64;
                rows_seen += on_cell.len() as u64;
                total_probes += 1;
            }
        }
    }

    assert!(
        total_pruned_valid > 0,
        "the valid-interval summary never pruned a segment — the access path was untested",
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
