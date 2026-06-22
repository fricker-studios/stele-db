//! Valid-time range-scan resolution (STL-328) at the executor level.
//!
//! [`SnapshotScan::valid_range`] is the valid-axis mirror of STL-244's
//! [`SnapshotScan::system_range`]: at one system snapshot it returns every
//! system-live version whose valid interval `[valid_from, valid_to)` overlaps a
//! valid range, paired with that interval so the engine can append the period
//! endpoints. These drive a real **valid-time** table through the DML path,
//! resolve a range across both tiers (a sealed segment's first-class
//! `valid_from` / `valid_to` columns and a delta row's framed payload), and
//! assert the answer is identical before and after flushing — history a range
//! read must reconstruct the same way whether a version is staged or sealed.
//!
//! The `+∞` (until-changed) `valid_to` is exercised on both tiers, since its
//! NULL rendering at the engine layer hinges on the interval surfacing
//! `VALID_TIME_OPEN` unchanged here.

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_exec::SnapshotScan;
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::DmlWriter;
use stele_storage::segment::{SegmentReader, SegmentWriter};
use stele_storage::systime::{EmptySealed, SealedSegments};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::ValidInterval;
use stele_storage::wal::{Wal, WalConfig};

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// Deterministic, strictly-increasing clock — one tick per `now()` (ADR-0010).
struct StepClock(AtomicI64);
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

fn new_tiers() -> (Wal<MemDisk>, Delta<MemDisk>, ValidityIndex<MemDisk>) {
    (
        Wal::open(MemDisk::new(), WalConfig::default()).expect("wal"),
        Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta"),
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index"),
    )
}

/// Drain the delta into a fresh valid-time sealed segment and reopen it — the
/// real columnar flush boundary, where the interval prefix is lifted off the
/// payload into first-class `valid_from` / `valid_to` columns.
fn flush_valid(disk: &MemDisk, delta: &mut Delta<MemDisk>) -> Option<SegmentReader<MemFile>> {
    let rows = delta.flush_to_segment().expect("flush");
    if rows.is_empty() {
        return None;
    }
    let mut w = SegmentWriter::create_valid_time(disk, "seg-0.seg").expect("create valid-time");
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    Some(SegmentReader::open(disk, "seg-0.seg").expect("open segment"))
}

/// Resolve a valid range and return, per key slot, its `(bare payload, interval)`.
fn valid_range(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    segments: &[SegmentReader<MemFile>],
    s: i64,
    lo: i64,
    hi: i64,
    closed_upper: bool,
) -> BTreeMap<u8, (Vec<u8>, ValidInterval)> {
    let (versions, _stats) =
        SnapshotScan::new(delta, index, segments, Snapshot(SystemTimeMicros(s)))
            .valid_range(lo, hi, closed_upper)
            .execute_valid_range()
            .expect("valid range scan");
    let mut out = BTreeMap::new();
    for (v, interval) in versions {
        assert!(
            out.insert(
                v.business_key.as_bytes()[1],
                (v.payload.clone().unwrap_or_default(), interval),
            )
            .is_none(),
            "one system-live version per key",
        );
    }
    out
}

/// Build a three-key valid-time history — key 1 valid `[0, 100)`, key 2 valid
/// `[200, 300)`, key 3 valid `[50, +∞)` (open-ended) — then assert the valid
/// range scan agrees with hand-computed overlap sets, **identically** whether
/// the rows are staged in the delta or sealed into a segment.
#[test]
fn valid_range_resolves_overlap_across_both_tiers_and_the_flush_boundary() {
    let seg_disk = MemDisk::new();
    let (wal, mut delta, mut index) = new_tiers();
    let mut dml = DmlWriter::new(wal, StepClock(AtomicI64::new(1_000)), true);

    for (k, interval, payload) in [
        (1u8, iv(0, 100), b"one".to_vec()),
        (2, iv(200, 300), b"two".to_vec()),
        (3, iv(50, VALID_TIME_OPEN.0), b"three".to_vec()),
    ] {
        dml.insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key_of(k),
            Some(interval),
            Some(payload),
            0,
            TxnId(u64::from(k)),
            who(),
        )
        .expect("insert");
    }
    // A system snapshot where all three are live (every commit is at or below it).
    let s = 2_000;

    // (lo, hi, closed_upper) → the slots whose valid interval overlaps the range.
    let cases: [(i64, i64, bool, &[u8]); 6] = [
        (0, 100, false, &[1, 3]),    // [0,100): k1 exact, k3 reaches in at 50
        (200, 300, false, &[2, 3]),  // [200,300): k2 exact, k3 still open
        (0, 60, false, &[1, 3]),     // [0,60): k1 and k3 (50 < 60)
        (0, 50, false, &[1]),        // [0,50): k3 begins at the exclusive upper edge
        (0, 50, true, &[1, 3]),      // [0,50]: closed upper now includes k3's start
        (1_000, 2_000, false, &[3]), // far future: only the open-ended k3
    ];

    let assert_cases = |delta: &Delta<MemDisk>,
                        index: &ValidityIndex<MemDisk>,
                        segments: &[SegmentReader<MemFile>],
                        label: &str| {
        for (lo, hi, closed, want) in cases {
            let got = valid_range(delta, index, segments, s, lo, hi, closed);
            let keys: Vec<u8> = got.keys().copied().collect();
            assert_eq!(
                keys, want,
                "{label}: FOR VALID_TIME [{lo},{hi}) closed={closed}"
            );
        }
        // The open-ended key 3 surfaces `+∞` unchanged on whichever tier holds it —
        // the engine renders that as a NULL `valid_to`.
        let open = valid_range(delta, index, segments, s, 0, 1_000, false);
        assert_eq!(
            open[&3].0, b"three",
            "{label}: payload is bare (frame stripped)"
        );
        assert_eq!(
            open[&3].1.to, VALID_TIME_OPEN,
            "{label}: an open-ended fact keeps its +∞ valid_to",
        );
    };

    // Everything in the delta tier.
    assert_cases(&delta, &index, &[], "delta");

    // Flush all three into a sealed valid-time segment and re-assert: the answer
    // is identical, now read from the columnar `valid_from` / `valid_to` columns.
    let segments = vec![flush_valid(&seg_disk, &mut delta).expect("seal")];
    assert_eq!(segments[0].row_count(), 3);
    let _ = SealedSegments::new(&segments); // sealed view exists; the scan reads the readers
    assert_cases(&delta, &index, &segments, "sealed");
}
