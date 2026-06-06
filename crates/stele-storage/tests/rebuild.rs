//! From-scratch validity-index rebuild from segments — the resurrection oracle
//! (STL-143, [docs/16 §12], [ADR-0023]).
//!
//! A delete is a "close with no successor". Version-adjacency inference can only
//! reconstruct *supersessions* (a version's `sys_to` is the next version's
//! `sys_from`); it cannot represent a deletion gap, so a from-scratch rebuild that
//! relied on adjacency alone would infer a deleted version open right up to a
//! later re-insert — **silently resurrecting the row across the gap**. STL-143
//! persists retractions as durable tombstone rows in the segment store and rebuilds
//! the index from *versions + retractions*. This file is the oracle.
//!
//! The three Definition-of-Done items:
//!
//! 1. **Resurrection oracle.** The canonical `INSERT→UPDATE→UPDATE→DELETE@t3→
//!    re-INSERT@t4` history: across the deletion gap `[t3, t4)` the key is ABSENT,
//!    and that answer is **byte-identical before and after** a full rebuild from
//!    segments. A bundled *adjacency-only* rebuild is shown to fail the same probe
//!    (it resurrects the row), so the oracle has teeth.
//! 2. **Full-rebuild-from-segments equals checkpoint + WAL-tail recovery** for
//!    every sim seed: the index rebuilt from the segment store is byte-identical to
//!    the one a WAL replay reconstructs.
//! 3. **Delete provenance is queryable** from the persisted retraction after a
//!    rebuild — who deleted, when, by what transaction.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::{self, DmlWriter};
use stele_storage::merge;
use stele_storage::rebuild::rebuild_index_from_segments;
use stele_storage::segment::{SegmentReader, SegmentWriter};
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{Close, ValidityConfig, ValidityIndex};
use stele_storage::wal::{Checkpoint, Wal, WalConfig};

// --- harness ---------------------------------------------------------------

/// A deterministic, strictly-increasing clock — one tick per `now()`. Matches the
/// other storage tests so a failing seed reproduces bit-for-bit ([ADR-0010]).
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

/// A hand-driven clock behind a shared atomic — for the focused resurrection demo,
/// where the test sets the exact wall position between statements.
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

/// Tiny xorshift64* — deterministic, dependency-free; matches the other storage
/// tests so a failing seed reproduces bit-for-bit ([ADR-0010]).
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

/// Seal `versions` + `retractions` into a fresh system-only segment and read both
/// back — the real columnar flush boundary STL-143 persists tombstones through.
/// Returns what a reader sees: open/unresolved versions (their end lives in the
/// index) plus the durable retraction rows.
fn seal(
    disk: &MemDisk,
    name: &str,
    versions: Vec<Version>,
    retractions: Vec<Close>,
) -> (Vec<Version>, Vec<Close>) {
    let mut writer = SegmentWriter::create(disk, name).expect("create segment");
    for v in versions {
        writer.push(v).expect("push");
    }
    for r in retractions {
        writer.push_retraction(r).expect("push retraction");
    }
    writer.finish().expect("finish");
    let reader = SegmentReader::open(disk, name).expect("open segment");
    let read_versions = reader.read_versions().expect("read versions");
    let read_retractions = reader.read_retractions().expect("read retractions");
    (read_versions, read_retractions)
}

/// The payload live for `key` at snapshot `s`, reading the sealed versions with
/// `index` supplying each version's end — the cross-tier `AS OF` ([`merge`]).
/// `None` means the key is ABSENT at `s` (the deletion-gap answer).
fn as_of(
    versions: &[Version],
    index: &ValidityIndex<MemDisk>,
    key: &BusinessKey,
    s: SystemTimeMicros,
) -> Option<Vec<u8>> {
    let relevant = versions.iter().filter(|v| &v.business_key == key).cloned();
    let chains = merge::fold_chains(relevant, index).expect("fold");
    merge::resolve_snapshot(&chains, Snapshot(s))
        .into_iter()
        .find(|v| &v.business_key == key)
        .map(|v| v.payload)
}

// --- 1. the resurrection oracle ---------------------------------------------

/// `INSERT@t0 → UPDATE@t1 → UPDATE@t2 → DELETE@t3 → re-INSERT@t4`. Flush every
/// version *and the persisted retraction* into one sealed segment, then rebuild
/// the validity index **from the segment store alone**. The deletion gap
/// `[t3, t4)` — key ABSENT — must read byte-identically before and after the
/// rebuild. An adjacency-only rebuild (ignoring the tombstone) is shown to
/// resurrect the row, proving the oracle is not vacuous.
#[test]
#[allow(clippy::too_many_lines)] // a linear five-statement history + an exhaustive boundary sweep
fn resurrection_gap_survives_full_index_rebuild_byte_identical() {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let clock = StubClock::new(1_000);
    let mut dml = DmlWriter::new(wal.clone(), clock.clone(), false);
    let key = BusinessKey::new(b"account-1".to_vec());

    let t0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"v0".to_vec(),
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
            None,
            b"v1".to_vec(),
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
            None,
            b"v2".to_vec(),
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
            None,
            b"v4".to_vec(),
            TxnId(5),
            who(),
        )
        .expect("re-insert")
        .commit;
    wal.tick().expect("fsync");
    assert!(t0 < t1 && t1 < t2 && t2 < t3 && t3 < t4);

    // Flush every version AND the staged retraction into one sealed segment, then
    // read them back — the segment store is now self-contained.
    let disk = MemDisk::new();
    let versions = delta.flush_to_segment().expect("flush");
    let retractions = delta.take_retractions();
    assert_eq!(
        retractions.len(),
        1,
        "the delete persisted exactly one tombstone"
    );
    let (seg_versions, seg_retractions) = seal(&disk, "account-0.seg", versions, retractions);
    assert_eq!(
        seg_versions.len(),
        4,
        "v0,v1,v2,v4 — the delete opened no version"
    );
    assert_eq!(
        seg_retractions.len(),
        1,
        "the tombstone survived the columnar round-trip"
    );

    // Probe the gap and its boundaries *before* rebuild (the live index).
    let gap_mid = SystemTimeMicros((t3.0 + t4.0) / 2);
    let probes = [
        (SystemTimeMicros(t2.0), Some(b"v2".to_vec())), // last live before delete
        (SystemTimeMicros(t3.0 - 1), Some(b"v2".to_vec())), // half-open: live up to t3
        (t3, None),                                     // at the delete: ABSENT
        (gap_mid, None),                                // mid-gap: ABSENT
        (SystemTimeMicros(t4.0 - 1), None),             // half-open: still ABSENT up to t4
        (t4, Some(b"v4".to_vec())),                     // re-insert visible
    ];
    let before: Vec<_> = probes
        .iter()
        .map(|(s, _)| as_of(&seg_versions, &index, &key, *s))
        .collect();
    for ((s, expected), got) in probes.iter().zip(&before) {
        assert_eq!(got, expected, "live read @ s={}", s.0);
    }

    // --- Rebuild the index from the segment store alone (no WAL). ---
    let mut rebuilt =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("rebuilt");
    rebuild_index_from_segments(seg_versions.clone(), seg_retractions, &mut rebuilt)
        .expect("rebuild");

    let after: Vec<_> = probes
        .iter()
        .map(|(s, _)| as_of(&seg_versions, &rebuilt, &key, *s))
        .collect();
    assert_eq!(
        before, after,
        "as-of across the deletion gap is byte-identical before and after a full rebuild",
    );
    // And the whole materialized index matches the live one.
    assert_eq!(
        rebuilt.materialize().expect("mat"),
        index.materialize().expect("mat"),
        "rebuilt-from-segments index equals the live index",
    );

    // --- Teeth: an adjacency-only rebuild RESURRECTS the row across [t3, t4). ---
    let mut adjacency_only =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("adj");
    rebuild_index_from_segments(seg_versions.clone(), Vec::new(), &mut adjacency_only)
        .expect("adjacency-only rebuild");
    assert_eq!(
        as_of(&seg_versions, &adjacency_only, &key, gap_mid),
        Some(b"v2".to_vec()),
        "an adjacency-only rebuild (no tombstone) resurrects the deleted row across the gap — \
         exactly the bug the persisted retraction prevents",
    );
}

// --- 2. rebuild-from-segments == WAL-tail recovery, seed sweep --------------

/// One period in the reference model: a half-open `[from, to)` system-time
/// interval carrying the payload asserted for it. `to == SYSTEM_TIME_OPEN` is the
/// currently-live period.
#[derive(Clone)]
struct Period {
    from: i64,
    to: i64,
    payload: Vec<u8>,
}

/// The reference answer at snapshot `s`: for each key, the payload of the period
/// whose `[from, to)` contains `s` (at most one — the 2D-tiling invariant).
fn reference_as_of(model: &[Vec<Period>], s: i64) -> BTreeMap<BusinessKey, Vec<u8>> {
    let mut live = BTreeMap::new();
    for (k, periods) in model.iter().enumerate() {
        if let Some(p) = periods.iter().find(|p| p.from <= s && s < p.to) {
            live.insert(BusinessKey::new(vec![b'k', k as u8]), p.payload.clone());
        }
    }
    live
}

fn close_open(chain: &mut [Period], commit: i64) {
    let open = chain.last_mut().expect("a live key has an open period");
    assert_eq!(
        open.to, SYSTEM_TIME_OPEN.0,
        "the period being closed was open"
    );
    open.to = commit;
}

/// Over a seed sweep of random INSERT/UPDATE/DELETE histories: flush every version
/// and persisted retraction into the segment store, then assert the index
/// **rebuilt from segments alone** is byte-identical to the one a **WAL replay**
/// reconstructs — and that both serve `AS OF` answers matching a hand-coded
/// reference oracle at every boundary snapshot (so the deletion gaps are real
/// ABSENTs, not coincidental agreement).
#[test]
#[allow(clippy::too_many_lines)] // one self-contained seed-sweep harness; splitting it would scatter the model
fn from_scratch_rebuild_equals_wal_replay_under_seed_sweep() {
    const KEY_POOL: u64 = 6;
    const START: i64 = 1_000;

    for seed in 0u64..200 {
        let mut rng = Rng::new(seed);
        let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
        let mut dml = DmlWriter::new(wal.clone(), StepClock::new(START), false);

        let mut model: Vec<Vec<Period>> = vec![Vec::new(); KEY_POOL as usize];
        let mut live = vec![false; KEY_POOL as usize];
        let mut hi = START;

        let ops = 20 + rng.range(40);
        for op in 0..ops {
            let k = rng.range(KEY_POOL) as usize;
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let txn = TxnId(op);
            let payload = format!("k{k}-op{op}").into_bytes();

            if live[k] {
                if rng.range(2) == 0 {
                    let c = dml
                        .delete(&mut delta, &mut index, &EmptySealed, &key, txn, who())
                        .expect("delete")
                        .commit;
                    close_open(&mut model[k], c.0);
                    live[k] = false;
                    hi = hi.max(c.0);
                } else {
                    let c = dml
                        .update(
                            &mut delta,
                            &mut index,
                            &EmptySealed,
                            key,
                            None,
                            payload.clone(),
                            txn,
                            who(),
                        )
                        .expect("update")
                        .commit;
                    close_open(&mut model[k], c.0);
                    model[k].push(Period {
                        from: c.0,
                        to: SYSTEM_TIME_OPEN.0,
                        payload,
                    });
                    hi = hi.max(c.0);
                }
            } else {
                let c = dml
                    .insert(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        None,
                        payload.clone(),
                        txn,
                        who(),
                    )
                    .expect("insert")
                    .commit;
                model[k].push(Period {
                    from: c.0,
                    to: SYSTEM_TIME_OPEN.0,
                    payload,
                });
                live[k] = true;
                hi = hi.max(c.0);
            }
        }
        wal.tick().expect("fsync");

        // Flush everything into one sealed segment and read it back — the whole
        // segment store, self-contained for a from-scratch rebuild.
        let disk = MemDisk::new();
        let versions = delta.flush_to_segment().expect("flush");
        let retractions = delta.take_retractions();
        let (seg_versions, seg_retractions) = seal(&disk, "seg-0.seg", versions, retractions);

        // Path A: rebuild the index from the segment store alone.
        let mut rebuilt =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("rebuilt");
        rebuild_index_from_segments(seg_versions.clone(), seg_retractions, &mut rebuilt)
            .expect("rebuild");

        // Path B: routine recovery — replay the WAL tail from BEGIN into a fresh
        // index (the "checkpoint + WAL-tail" path, with a trivial BEGIN checkpoint).
        let mut replayed_delta =
            Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut replayed =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
        dml::replay(&wal, &mut replayed_delta, &mut replayed, Checkpoint::BEGIN).expect("replay");

        assert_eq!(
            rebuilt.materialize().expect("mat"),
            replayed.materialize().expect("mat"),
            "seed {seed}: rebuild-from-segments must equal WAL-tail recovery, byte for byte",
        );

        // Probe every integer snapshot from before the first commit to past the
        // last — the half-open boundaries and the deletion gaps both get hit.
        for s in (START - 2)..=(hi + 2) {
            let expected = reference_as_of(&model, s);
            let mut got = BTreeMap::new();
            let chains = merge::fold_chains(seg_versions.iter().cloned(), &rebuilt).expect("fold");
            for v in merge::resolve_snapshot(&chains, Snapshot(SystemTimeMicros(s))) {
                assert!(
                    got.insert(v.business_key.clone(), v.payload).is_none(),
                    "seed {seed} @ s={s}: two live versions for one key — tiling invariant broken",
                );
            }
            assert_eq!(
                got, expected,
                "seed {seed} @ s={s}: rebuilt-index read must match the reference oracle",
            );
        }
    }
}

// --- 3. delete provenance is queryable from the persisted retraction --------

/// After a from-scratch rebuild, "who deleted this, when, by what transaction" is
/// recoverable from the persisted retraction — both directly off the tombstone row
/// ([`SegmentReader::read_retractions`]) and through the rebuilt index's close.
#[test]
fn delete_provenance_is_queryable_from_the_persisted_retraction() {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let mut dml = DmlWriter::new(wal.clone(), StepClock::new(500), false);
    let key = BusinessKey::new(b"doomed".to_vec());
    let deleter = Principal::new(b"alice@audit".to_vec());

    let birth = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"payload".to_vec(),
            TxnId(11),
            who(),
        )
        .expect("insert")
        .commit;
    let deleted_at = dml
        .delete(
            &mut delta,
            &mut index,
            &EmptySealed,
            &key,
            TxnId(99),
            deleter.clone(),
        )
        .expect("delete")
        .commit;
    wal.tick().expect("fsync");

    let disk = MemDisk::new();
    let versions = delta.flush_to_segment().expect("flush");
    let retractions = delta.take_retractions();
    let (seg_versions, seg_retractions) = seal(&disk, "doomed-0.seg", versions, retractions);

    // Directly off the persisted tombstone row.
    assert_eq!(seg_retractions.len(), 1);
    let tomb = &seg_retractions[0];
    assert_eq!(tomb.business_key, key);
    assert_eq!(
        tomb.sys_from, birth,
        "the tombstone names the version it closed"
    );
    assert_eq!(tomb.sys_to, deleted_at, "closed_at = the delete commit");
    assert_eq!(
        tomb.closed_by.txn_id,
        TxnId(99),
        "who deleted (transaction)"
    );
    assert_eq!(tomb.closed_by.committed_at, deleted_at, "when deleted");
    assert_eq!(tomb.closed_by.principal, deleter, "by whom (principal)");

    // And through the index rebuilt from the segment store alone.
    let mut rebuilt =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("rebuilt");
    rebuild_index_from_segments(seg_versions, seg_retractions, &mut rebuilt).expect("rebuild");
    let close = rebuilt
        .close_of(&key, birth)
        .expect("lookup")
        .expect("the deleted version is closed");
    assert_eq!(close.sys_to, deleted_at);
    assert_eq!(close.closed_by.txn_id, TxnId(99));
    assert_eq!(close.closed_by.principal, deleter);
}
