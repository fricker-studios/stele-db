//! `Engine::flush` row-group bounding ([STL-197]).
//!
//! STL-155 taught the segment *writer* to split a flush into bounded row-groups
//! and the read path to skip the chunks of row-groups holding no live row, but
//! `Engine::flush` still sealed every segment as one unbounded row-group, so the
//! chunk skipping degenerated to per-column. These tests pin the wiring the
//! ticket adds: a flush wider than the engine's bound carries several row-groups,
//! a narrow flush (or the default policy) stays a single row-group byte-for-byte,
//! and a multi-row-group segment recovers to the same history — the
//! rebuild-from-segments path is unchanged ([ADR-0023]). The read-accounting
//! proof that a scan actually reads fewer bytes lives in `stele-exec`
//! (`tests/snapshot_scan.rs`); the deterministic recovery sweep over random
//! multi-row-group flushes lives in `stele-sim`
//! (`run_engine_flush_recover_seed`).

#![allow(clippy::cast_possible_wrap)]

use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Snapshot};
use stele_storage::engine::Engine;

/// A deterministic, strictly-increasing clock — one tick per `now()`, matching
/// the other storage tests so a failure reproduces bit-for-bit ([ADR-0010]).
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

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

fn key(i: usize) -> BusinessKey {
    BusinessKey::new(format!("k-{i:04}").into_bytes())
}

/// Insert `count` keys (`k-0000 …`) through `engine`, returning each key's commit
/// instant in insert order. Distinct keys, one version each.
fn insert_keys<C: Clock>(engine: &mut Engine<C, MemDisk>, count: usize) -> Vec<SystemTimeMicros> {
    (0..count)
        .map(|i| {
            engine
                .insert(
                    key(i),
                    None,
                    Some(format!("v{i}").into_bytes()),
                    0,
                    TxnId(u64::try_from(i).unwrap() + 1),
                    who(),
                )
                .expect("insert")
                .commit
        })
        .collect()
}

/// A flush wider than the engine's row-group bound seals a segment carrying
/// several row-groups, not one — the wiring STL-197 adds. Five rows under a
/// two-row bound split `2 + 2 + 1`, and a kill-and-recover over that
/// multi-row-group segment still resolves every key's `AS OF`, so the
/// rebuild-from-segments path is unchanged by the split.
#[test]
fn flush_above_the_bound_splits_into_multiple_row_groups() {
    let disk = MemDisk::new();
    let commits = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false)
            .expect("open")
            .with_flush_row_group_rows(2);
        let commits = insert_keys(&mut engine, 5);
        engine.flush().expect("flush"); // seals seg-0 and records the manifest

        let readers = engine.open_segment_readers().expect("open readers");
        assert_eq!(readers.len(), 1, "one flush ⇒ one sealed segment");
        assert_eq!(
            readers[0].row_group_row_counts(),
            vec![2, 2, 1],
            "a five-row flush under a two-row bound splits into three row-groups",
        );
        commits
        // engine dropped — the crash; the segment + manifest are durable.
    };

    // Recovery rebuilds the validity index from the multi-row-group segment; every
    // key's inserted value resolves at its commit instant exactly as before.
    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    for (i, commit) in commits.into_iter().enumerate() {
        assert_eq!(
            recovered
                .as_of_payload(&key(i), Snapshot(commit))
                .expect("as_of"),
            Some(Some(format!("v{i}").into_bytes())),
            "key {i} must resolve to its inserted value after recovery",
        );
    }
}

/// The default flush policy leaves a narrow flush a single row-group — the v0.1
/// shape, byte-identical to before STL-197 — so existing segments and oracles are
/// untouched until a flush actually exceeds the (1024-row) default bound.
#[test]
fn default_flush_keeps_a_narrow_segment_one_row_group() {
    let disk = MemDisk::new();
    let mut engine = Engine::open(disk, StepClock::new(1_000), false).expect("open");
    insert_keys(&mut engine, 5);
    engine.flush().expect("flush");

    let readers = engine.open_segment_readers().expect("open readers");
    assert_eq!(
        readers[0].row_group_row_counts(),
        vec![5],
        "five rows are well under the default bound, so the flush stays one row-group",
    );
}
