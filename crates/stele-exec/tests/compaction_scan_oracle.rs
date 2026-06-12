//! Both-axes `(system, valid)` scan oracle across compaction ([STL-231]).
//!
//! The ticket's Definition of Done, verbatim: *every `(sys, valid)` AS OF read
//! returns identical results before/after compacting K segments* — [testing
//! strategy §4] names "a compaction that drops a version" as a headline bug
//! class. This drives a **valid-time** [`Engine`] through a seeded random
//! history spread over several sealed segments plus a staged delta tail, then
//! sweeps an exhaustive `(s, v)` snapshot grid through [`SnapshotScan`] —
//! composed from the engine's tiers exactly as the session executor composes it
//! — and asserts the full grid is identical at three points:
//!
//! 1. **before** compaction (many small segments),
//! 2. **after** compaction (one consolidated segment, inputs retired),
//! 3. **after recovery from the compacted store** (the validity index rebuilt
//!    from the consolidated segment's rows + tombstones, [ADR-0023]).
//!
//! The unpinned system-only read (a valid-time table read with no `FOR
//! VALID_TIME AS OF`, [STL-218]) rides the same grid, so the frame-stripping
//! path is pinned across the swap too.
//!
//! Storage-level compaction crash safety, retirement, and the engine-level
//! `as_of` differential live in `stele-storage/tests/compaction.rs`; this file
//! is the executor's view of the same swap.
//!
//! [STL-231]: https://allegromusic.atlassian.net/browse/STL-231
//! [testing strategy §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart
//! [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};
use stele_exec::{Column, SnapshotScan};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Snapshot};
use stele_storage::engine::Engine;
use stele_storage::segment::{ColumnId, SegmentReader};
use stele_storage::validtime::ValidInterval;

// --- harness ---------------------------------------------------------------

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

/// Tiny xorshift64* — deterministic, dependency-free (ADR-0010).
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

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// The business key for pool slot `k` — `['k', k]`, so byte 1 recovers the slot.
fn key_of(k: u8) -> BusinessKey {
    BusinessKey::new(vec![b'k', k])
}

const KEYS: u8 = 5;
const OPS: usize = 40;
/// The valid-time grid the workload draws interval endpoints from.
const VALID_DOMAIN: i64 = 60;

/// Drive a seeded random valid-time history: inserts, updates, and deletes over
/// a small key pool, flushed at the quarter boundaries so the history spans
/// several sealed segments with a staged delta tail. Returns the first unused
/// system tick (the grid's upper bound).
fn run_workload(engine: &mut Engine<StepClock, MemDisk>, seed: u64) -> i64 {
    let mut rng = Rng::new(seed);
    let mut live: BTreeSet<u8> = BTreeSet::new();
    let mut seq = 0u64;
    for op in 0..OPS {
        if op > 0 && op % (OPS / 4) == 0 {
            engine.flush().expect("flush");
        }
        seq += 1;
        let txn = TxnId(seq);
        let k = rng.range(u64::from(KEYS)) as u8;
        let from = rng.range(VALID_DOMAIN as u64 - 10) as i64;
        let to = from + 1 + rng.range(30) as i64;
        let valid =
            Some(ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("interval"));
        let payload = Some(format!("v{seq}-k{k}").into_bytes());
        if live.contains(&k) {
            if rng.range(5) == 0 {
                engine.delete(&key_of(k), txn, who()).expect("delete");
                live.remove(&k);
            } else {
                engine
                    .update(key_of(k), valid, payload, seq, txn, who())
                    .expect("update");
            }
        } else {
            engine
                .insert(key_of(k), valid, payload, seq, txn, who())
                .expect("insert");
            live.insert(k);
        }
    }
    OPS as i64 + 2
}

/// One scan cell: the per-key live payloads at system snapshot `s`, the valid
/// axis pinned to `v` when given ([STL-164]) and unpinned otherwise — the
/// system-only read of a valid-time table, every system-live row with its
/// delta frame stripped ([STL-218]). Composed from the engine's tiers exactly
/// as `stele-engine`'s `run_select` composes it.
fn scan_cell(
    engine: &Engine<StepClock, MemDisk>,
    readers: &[SegmentReader<MemFile>],
    s: i64,
    v: Option<i64>,
) -> BTreeMap<u8, BTreeSet<Vec<u8>>> {
    let mut scan = SnapshotScan::new(
        engine.delta(),
        engine.index(),
        readers,
        Snapshot(SystemTimeMicros(s)),
    )
    .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
    .valid_time(true);
    if let Some(v) = v {
        scan = scan.valid_as_of(ValidTimeMicros(v));
    }
    let out = scan.execute().expect("scan");

    let keys = bytes_column(&out, ColumnId::BusinessKey);
    let payloads = bytes_column(&out, ColumnId::Payload);
    assert_eq!(keys.len(), payloads.len());
    let mut cell: BTreeMap<u8, BTreeSet<Vec<u8>>> = BTreeMap::new();
    for (key, payload) in keys.iter().zip(payloads) {
        let slot = key[1];
        let fresh = cell.entry(slot).or_default().insert(payload);
        assert!(fresh, "@ (s={s}, v={v:?}): duplicate row for key {slot}");
        if v.is_some() {
            assert_eq!(
                cell[&slot].len(),
                1,
                "@ (s={s}, v={v:?}): two live versions for key {slot} — \
                 the at-most-one-live invariant broke",
            );
        }
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

/// The full `(s, v)` grid — every system tick × every valid grid point, plus
/// the unpinned system-only read at every tick.
type Grid = BTreeMap<(i64, Option<i64>), BTreeMap<u8, BTreeSet<Vec<u8>>>>;

fn capture_grid(engine: &Engine<StepClock, MemDisk>, s_max: i64) -> Grid {
    // One reader set per capture — the same composition `run_select` builds per
    // statement; per-cell reopening would only re-validate the same CRCs.
    let readers = engine.open_segment_readers().expect("open segments");
    let mut grid = BTreeMap::new();
    for s in 0..=s_max {
        grid.insert((s, None), scan_cell(engine, &readers, s, None));
        for v in (0..=VALID_DOMAIN).step_by(3) {
            grid.insert((s, Some(v)), scan_cell(engine, &readers, s, Some(v)));
        }
    }
    grid
}

// --- the oracle --------------------------------------------------------------

#[test]
fn the_sys_valid_grid_is_identical_across_compaction_and_recovery() {
    for seed in 0..8u64 {
        let disk = MemDisk::new();
        let mut engine = Engine::recover(disk.clone(), StepClock::new(0), true).expect("boot");
        let s_max = run_workload(&mut engine, seed);
        assert!(
            engine.segment_names().len() >= 2,
            "seed {seed}: the workload must give compaction real input",
        );

        let before = capture_grid(&engine, s_max);
        // Teeth: a vacuously-empty grid would make the differential meaningless.
        assert!(
            before.values().any(|cell| !cell.is_empty()),
            "seed {seed}: the grid must observe at least one live row",
        );

        let outcome = engine.compact().expect("compact");
        assert!(outcome.segments_in >= 2, "seed {seed}: a real merge ran");
        assert_eq!(
            capture_grid(&engine, s_max),
            before,
            "seed {seed}: compaction changed a (sys, valid) read",
        );

        drop(engine);
        let recovered = Engine::recover(disk, StepClock::new(s_max + 1), true).expect("recover");
        assert_eq!(
            recovered.segment_names().len(),
            1,
            "seed {seed}: recovery trusts exactly the consolidated output",
        );
        assert_eq!(
            capture_grid(&recovered, s_max),
            before,
            "seed {seed}: recovery from the compacted store changed a (sys, valid) read",
        );
    }
}
