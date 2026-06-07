//! DML write-path integration tests (STL-94).
//!
//! These exercise the three data-manipulation operations end-to-end through the
//! real durability + staging path — `INSERT` / `UPDATE` / `DELETE` resolve to
//! version rows, append to the WAL, then stage in the delta tier — and pin the
//! two properties the ticket's Definition of Done calls for:
//!
//! * **Timeline reconstruction (DoD bullet 1).** After a random series of
//!   inserts/updates/deletes, draining the delta and summing each key's
//!   `[sys_from, sys_to)` intervals reconstructs that key's full timeline with no
//!   gaps and no overlaps. A model tracks the exact interval chain each operation
//!   *should* produce (from the commit timestamps the writer returns), and the
//!   drained chain is asserted equal to it — an exact oracle, not a structural
//!   approximation.
//! * **WAL → delta equivalence.** Replaying the WAL into a *fresh* delta
//!   reconstructs byte-for-byte the same staged state the live writes produced —
//!   the "same code path under sim and under real I/O" guarantee and the crash-
//!   recovery contract ([architecture §3.4, §3.6](../../../docs/02-architecture.md#34-write-path-sequence)).
//!
//! `AS OF` read resolution is a separate ticket; nothing here selects among
//! versions on a time axis.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros, ValidTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::{self, DmlWriter};
use stele_storage::merge;
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, unframe_payload};
use stele_storage::wal::{Checkpoint, Wal, WalConfig};

// --- harness ---------------------------------------------------------------

/// A deterministic, strictly-increasing clock — one tick per `now()`. Matches
/// the `stele-sim` `StepClock` so a failing case reproduces bit-for-bit.
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

/// Tiny xorshift64* — deterministic, dependency-free (the workspace keeps no
/// proptest/quickcheck dep; seeded determinism is the house style, ADR-0010).
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

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// A fresh WAL + delta + validity-index triple over independent in-memory
/// disks, and a system-only (`valid_time = false`) DML writer driving them.
fn new_writer(
    start: i64,
) -> (
    DmlWriter<StepClock, MemDisk>,
    Delta<MemDisk>,
    ValidityIndex<MemDisk>,
    Wal<MemDisk>,
) {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open index");
    let writer = DmlWriter::new(wal.clone(), StepClock::new(start), false);
    (writer, delta, index, wal)
}

/// Drain the delta and group every stored version by key, overlaying each
/// version's end (`sys_to` / `closed_by`) from the validity index ([ADR-0023] —
/// the record bodies are open; their ends live in the index). Each `Vec<Version>`
/// is one key's full chain, oldest first by `sys_from` (closed *and* open).
fn drain_chains(
    delta: &mut Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
) -> BTreeMap<BusinessKey, Vec<Version>> {
    let drained = delta.flush_to_segment().expect("flush");
    let folded = merge::fold_chains(drained, index).expect("fold chains");
    folded
        .into_iter()
        .map(|(key, chain)| (key, chain.into_values().collect()))
        .collect()
}

// --- focused flow ----------------------------------------------------------

#[test]
fn insert_update_delete_flow_through_wal_then_delta() {
    let (mut dml, mut delta, mut index, wal) = new_writer(1_000);
    let key = BusinessKey::new(b"acct-7".to_vec());

    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            Some(b"v0".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;
    let c1 = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            Some(b"v1".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit;
    let out = dml
        .delete(
            &mut delta,
            &mut index,
            &EmptySealed,
            &key,
            TxnId(3),
            Principal::new(b"deleter".to_vec()),
        )
        .expect("delete");
    let c2 = out.commit;

    assert!(c0 < c1 && c1 < c2, "commit timestamps strictly increase");

    // Every write is durable once a group-commit fsync covers the last offset.
    wal.tick().expect("fsync");
    assert!(
        wal.durable_end() >= out.wal,
        "the delete's redo record is durable after tick()"
    );

    // No version is live after the delete.
    let live = delta
        .range_scan(.., Snapshot(SystemTimeMicros(i64::MAX - 1)), &index)
        .expect("scan");
    assert!(live.is_empty(), "deleted key has no live version");

    let mut chains = drain_chains(&mut delta, &index);
    let versions = chains.remove(&key).expect("key has a chain");
    // insert opened [c0,+∞); update closed it at c1 and opened [c1,+∞); delete
    // closed that at c2 — two closed periods, no open one, abutting exactly.
    assert_eq!(versions.len(), 2);
    assert_eq!((versions[0].sys_from, versions[0].sys_to), (c0, c1));
    assert_eq!((versions[1].sys_from, versions[1].sys_to), (c1, c2));
    assert_eq!(versions[0].payload.as_deref(), Some(&b"v0"[..]));
    assert_eq!(versions[1].payload.as_deref(), Some(&b"v1"[..]));
    assert!(
        versions.iter().all(|v| v.closed_by.is_some()),
        "both periods are closed and carry their closer's provenance"
    );
    assert!(
        versions.iter().all(|v| v.sys_to != SYSTEM_TIME_OPEN),
        "a deleted key leaves no open period"
    );
    // The delete's tombstone records the deleting transaction (STL-118).
    assert_eq!(versions[1].closed_by.as_ref().unwrap().txn_id, TxnId(3));
}

#[test]
fn insert_on_a_live_key_is_rejected_through_the_dml_path() {
    let (mut dml, mut delta, mut index, _wal) = new_writer(1);
    let key = BusinessKey::new(b"dup".to_vec());
    dml.insert(
        &mut delta,
        &mut index,
        &EmptySealed,
        key.clone(),
        None,
        Some(b"a".to_vec()),
        0,
        TxnId(1),
        who(),
    )
    .expect("first insert");
    let err = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            None,
            Some(b"b".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .unwrap_err();
    assert!(matches!(err, dml::DmlError::Resolve(_)));
}

// --- WAL → delta equivalence (incl. valid-time framing) --------------------

/// Replaying the WAL into a fresh delta reconstructs the exact staged state the
/// live writes produced — for a *valid-time* table, so the framed payload (the
/// 16-byte interval prefix, STL-92) rides through the WAL and back intact.
#[test]
fn wal_replay_reconstructs_the_delta_under_seed_sweep() {
    const KEY_POOL: u64 = 5;

    for seed in 0u64..200 {
        let mut rng = Rng::new(seed);
        let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
        let mut live_delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut live_index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        // valid_time = true: inserts/updates must carry an interval.
        let mut dml = DmlWriter::new(wal.clone(), StepClock::new(1), true);
        let mut live = vec![false; KEY_POOL as usize];

        let ops = 10 + rng.range(30);
        for op in 0..ops {
            let k = (rng.range(KEY_POOL)) as usize;
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let txn = TxnId(op);
            if live[k] {
                if rng.range(2) == 0 {
                    dml.delete(
                        &mut live_delta,
                        &mut live_index,
                        &EmptySealed,
                        &key,
                        txn,
                        who(),
                    )
                    .expect("delete");
                    live[k] = false;
                } else {
                    let from = (rng.range(1_000_000)) as i64;
                    let span = 1 + (rng.range(1_000_000)) as i64;
                    let iv =
                        ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(from + span))
                            .expect("from < to");
                    dml.update(
                        &mut live_delta,
                        &mut live_index,
                        &EmptySealed,
                        key,
                        Some(iv),
                        Some(b"u".to_vec()),
                        0,
                        txn,
                        who(),
                    )
                    .expect("update");
                }
            } else {
                let from = (rng.range(1_000_000)) as i64;
                let span = 1 + (rng.range(1_000_000)) as i64;
                let iv = ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(from + span))
                    .expect("from < to");
                dml.insert(
                    &mut live_delta,
                    &mut live_index,
                    &EmptySealed,
                    key,
                    Some(iv),
                    Some(b"i".to_vec()),
                    0,
                    txn,
                    who(),
                )
                .expect("insert");
                live[k] = true;
            }
        }
        wal.tick().expect("fsync");

        // Snapshot the live delta, then rebuild a fresh delta *and* index purely
        // from the WAL — replay now reconstructs both ([ADR-0023]).
        let live_chains = drain_chains(&mut live_delta, &live_index);
        let mut replayed = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut replayed_index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        dml::replay(&wal, &mut replayed, &mut replayed_index, Checkpoint::BEGIN).expect("replay");
        let replayed_chains = drain_chains(&mut replayed, &replayed_index);

        assert_eq!(
            live_chains, replayed_chains,
            "seed {seed}: WAL replay must reconstruct the live delta exactly"
        );

        // And the framed valid-time prefix survived the WAL round-trip.
        for chain in replayed_chains.values() {
            for v in chain {
                let (interval, _user) =
                    unframe_payload(true, v.payload.as_deref().unwrap()).expect("unframe");
                assert!(interval.is_some(), "seed {seed}: valid interval preserved");
            }
        }
    }
}

// --- timeline reconstruction (DoD bullet 1) --------------------------------

/// The model's view of one stored period: its half-open `[from, to)` and whether
/// it has been closed. `to == SYSTEM_TIME_OPEN` and `closed == false` is the
/// current version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Period {
    from: i64,
    to: i64,
    closed: bool,
}

/// After a random INSERT/UPDATE/DELETE workload, every key's drained chain
/// equals the exact interval timeline the operations should have produced —
/// gap-free across each existence run, with a real gap exactly where a delete
/// fell, and never an overlap.
#[test]
fn timeline_reconstructs_with_no_gaps_or_overlaps_under_seed_sweep() {
    const KEY_POOL: u64 = 6;

    for seed in 0u64..256 {
        let mut rng = Rng::new(seed);
        let (mut dml, mut delta, mut index, _wal) = new_writer(1);

        // Per key: the expected period chain (oldest first) and whether live.
        let mut model: Vec<Vec<Period>> = vec![Vec::new(); KEY_POOL as usize];
        let mut live = vec![false; KEY_POOL as usize];

        let ops = 20 + rng.range(40);
        for op in 0..ops {
            let k = (rng.range(KEY_POOL)) as usize;
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let txn = TxnId(op);

            if live[k] {
                if rng.range(2) == 0 {
                    // DELETE: close the open period, no successor.
                    let commit = dml
                        .delete(&mut delta, &mut index, &EmptySealed, &key, txn, who())
                        .expect("delete")
                        .commit;
                    close_open(&mut model[k], commit.0);
                    live[k] = false;
                } else {
                    // UPDATE: close the open period at `commit`, open a new one.
                    let commit = dml
                        .update(
                            &mut delta,
                            &mut index,
                            &EmptySealed,
                            key,
                            None,
                            Some(b"u".to_vec()),
                            0,
                            txn,
                            who(),
                        )
                        .expect("update")
                        .commit;
                    close_open(&mut model[k], commit.0);
                    model[k].push(Period {
                        from: commit.0,
                        to: SYSTEM_TIME_OPEN.0,
                        closed: false,
                    });
                }
            } else {
                // INSERT: open a fresh period (a gap from any prior delete).
                let commit = dml
                    .insert(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        None,
                        Some(b"i".to_vec()),
                        0,
                        txn,
                        who(),
                    )
                    .expect("insert")
                    .commit;
                model[k].push(Period {
                    from: commit.0,
                    to: SYSTEM_TIME_OPEN.0,
                    closed: false,
                });
                live[k] = true;
            }
        }

        let chains = drain_chains(&mut delta, &index);
        for k in 0..KEY_POOL as usize {
            let key = BusinessKey::new(vec![b'k', k as u8]);
            verify_key_timeline(seed, k, &model[k], chains.get(&key), live[k]);
        }
    }
}

/// Assert one key's drained chain against the model: exact timeline
/// reconstruction plus the independent structural invariants (strictly
/// increasing, non-overlapping, at most one open period, `closed_by` present iff
/// closed).
fn verify_key_timeline(
    seed: u64,
    k: usize,
    expected: &[Period],
    actual: Option<&Vec<Version>>,
    live: bool,
) {
    if expected.is_empty() {
        assert!(
            actual.is_none(),
            "seed {seed} key {k}: untouched key has no chain"
        );
        return;
    }
    let actual = actual.expect("touched key has a chain");

    // Exact reconstruction: the stored chain is precisely the modeled timeline —
    // same intervals, same open/closed shape.
    let actual_periods: Vec<Period> = actual
        .iter()
        .map(|v| Period {
            from: v.sys_from.0,
            to: v.sys_to.0,
            closed: v.closed_by.is_some(),
        })
        .collect();
    assert_eq!(
        actual_periods.as_slice(),
        expected,
        "seed {seed} key {k}: drained chain must reconstruct the modeled timeline"
    );

    // Independent structural guards over the *stored* rows.
    for w in actual.windows(2) {
        let (lo, hi) = (&w[0], &w[1]);
        assert!(
            lo.sys_from < hi.sys_from,
            "seed {seed} key {k}: starts strictly increase"
        );
        assert!(
            lo.sys_to <= hi.sys_from,
            "seed {seed} key {k}: periods never overlap"
        );
        assert_ne!(
            lo.sys_to, SYSTEM_TIME_OPEN,
            "seed {seed} key {k}: only the last period may be open"
        );
    }
    // A closed period carries its closer; an open one does not.
    for v in actual {
        assert_eq!(
            v.closed_by.is_some(),
            v.sys_to != SYSTEM_TIME_OPEN,
            "seed {seed} key {k}: closed_by present iff the period is closed"
        );
    }
    // At most one open period, and only when the key is currently live.
    let open = actual
        .iter()
        .filter(|v| v.sys_to == SYSTEM_TIME_OPEN)
        .count();
    assert_eq!(
        open,
        usize::from(live),
        "seed {seed} key {k}: exactly one open period iff live"
    );
}

/// Close the model's currently-open period at `commit` (set its `to` and mark it
/// closed). There is always exactly one open period when this is called.
fn close_open(chain: &mut [Period], commit: i64) {
    let open = chain
        .last_mut()
        .expect("a live key has an open period to close");
    assert_eq!(
        open.to, SYSTEM_TIME_OPEN.0,
        "the period being closed was open"
    );
    open.to = commit;
    open.closed = true;
}
