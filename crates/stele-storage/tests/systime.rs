//! System-time versioning integration tests.
//!
//! Scope (STL-91):
//!
//! * **Round-trip** — DoD bullet 1: insert → update produces two versions for
//!   the key, one open (`sys_to = +∞`) and one closed, with the close abutting
//!   the new version's `sys_from`.
//! * **Interval invariant** — DoD bullet 2: for any business key, the set of
//!   `[sys_from, sys_to)` intervals the writer produces is non-overlapping and
//!   gap-free. Driven over a sweep of deterministic seeds with an adversarial
//!   (stalling / regressing) clock, so the property is proven against the worst
//!   the wall clock can do, not just the happy path.
//!
//! The delta tier is the staging target the writer feeds; these tests scan it
//! back to assert what was stored. They live here as integration tests until
//! the `stele-sim` storage scenarios land ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::significant_drop_tightening,
    clippy::type_complexity
)]

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::systime::{EmptySealed, SysTimeError, SysTimeWriter};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::wal::{Disk, DiskFile};

/// A throwaway principal for write-path tests that don't themselves assert on
/// provenance values (the provenance round-trip is covered in `provenance.rs`).
fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

// --- MemDisk: minimal in-memory Disk for tests ------------------------------

#[derive(Default, Clone)]
struct MemDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
}

impl MemDisk {
    fn new() -> Self {
        Self::default()
    }
}

struct MemFile {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Disk for MemDisk {
    type File = MemFile;

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
        Ok(MemFile { bytes })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(MemFile { bytes })
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

impl DiskFile for MemFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
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

// --- StubClock: a clock the test drives by hand -----------------------------

/// A clock the test drives by hand. The reading lives behind a shared atomic so
/// the test can keep one handle to move time while the writer owns a clone —
/// and it satisfies `Clock: Send + Sync` without `unsafe`.
#[derive(Clone)]
struct StubClock(Arc<AtomicI64>);
impl StubClock {
    fn new(start: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start)))
    }
    fn set(&self, micros: i64) {
        self.0.store(micros, Ordering::Relaxed);
    }
    fn now_micros(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
impl stele_common::time::Clock for StubClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.load(Ordering::Relaxed))
    }
}

/// Tiny xorshift64* — deterministic, dependency-free. Matches the helper in the
/// delta-tier tests so a failing seed reproduces bit-for-bit (ADR-0010).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
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

fn new_delta() -> Delta<MemDisk> {
    Delta::open(MemDisk::new(), DeltaConfig::default()).unwrap()
}

fn new_index() -> ValidityIndex<MemDisk> {
    ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).unwrap()
}

/// Drain the delta and group every stored version by business key, preserving
/// the `(business_key, sys_from)` order `flush_to_segment` guarantees — so each
/// `Vec<Version>` is one key's full chain, oldest first (closed *and* open
/// periods, which a snapshot scan can't surface together).
///
/// Under [ADR-0023] a flushed/staged version is raw: its end lives in the
/// [`ValidityIndex`], not on the record, so every drained version comes back
/// `sys_to == SYSTEM_TIME_OPEN` / `closed_by == None`. We overlay each version's
/// materialized end from the index — exactly the resolution the read path does —
/// so the returned chain carries the closed `sys_to`/`closed_by` the tests
/// assert on, leaving the open tail untouched.
///
/// Destructive: call it once, after all writes for the test are done.
fn drain_chains(
    delta: &mut Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
) -> BTreeMap<BusinessKey, Vec<Version>> {
    let mut map: BTreeMap<BusinessKey, Vec<Version>> = BTreeMap::new();
    for mut v in delta.flush_to_segment().unwrap() {
        if let Some(ci) = index.close_of(&v.business_key, v.sys_from, v.seq).unwrap() {
            v.sys_to = ci.sys_to;
            v.closed_by = Some(ci.closed_by);
        }
        map.entry(v.business_key.clone()).or_default().push(v);
    }
    map
}

// --- Round-trip (DoD bullet 1) ----------------------------------------------

#[test]
fn insert_then_update_leaves_one_closed_and_one_open_version() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(1_000);
    let mut writer = SysTimeWriter::new(clock.clone());
    let key = BusinessKey::new(b"acct-42".to_vec());

    let c0 = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            b"balance=100".to_vec(),
            0,
            TxnId(10),
            Principal::new(b"writer-a".to_vec()),
        )
        .unwrap();
    // Move the clock forward for the update's commit timestamp.
    clock.set(2_000);
    let c1 = writer
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            b"balance=150".to_vec(),
            0,
            TxnId(20),
            Principal::new(b"writer-b".to_vec()),
        )
        .unwrap();

    assert!(c0 < c1, "the update's commit must be after the insert's");

    let mut chains = drain_chains(&mut delta, &index);
    let versions = chains.remove(&key).expect("key has a chain");
    assert_eq!(versions.len(), 2, "insert + update ⇒ exactly two versions");

    let closed = &versions[0];
    let open = &versions[1];

    // First version: opened at c0, closed at c1.
    assert_eq!(closed.sys_from, c0);
    assert_eq!(closed.sys_to, c1);
    assert_eq!(closed.payload, b"balance=100");
    assert_ne!(
        closed.sys_to, SYSTEM_TIME_OPEN,
        "prior period must be closed"
    );
    // Closing the prior period must not rewrite its provenance: it keeps the
    // insert's txn/principal and committed_at = its own sys_from (c0).
    assert_eq!(closed.provenance.txn_id, TxnId(10));
    assert_eq!(
        closed.provenance.principal,
        Principal::new(b"writer-a".to_vec())
    );
    assert_eq!(closed.provenance.committed_at, c0);
    // …but the close *adds* who closed it: the superseding transaction's
    // identity, with committed_at = the new sys_to (c1) — STL-118.
    assert_eq!(
        closed.closed_by,
        Some(Provenance::new(
            TxnId(20),
            c1,
            Principal::new(b"writer-b".to_vec())
        )),
        "an updated prior version records its closer"
    );

    // Second version: opened at c1, still current.
    assert_eq!(open.sys_from, c1);
    assert_eq!(open.sys_to, SYSTEM_TIME_OPEN, "current period stays open");
    assert_eq!(open.payload, b"balance=150");
    // The new version carries the update's provenance, committed_at = c1.
    assert_eq!(open.provenance.txn_id, TxnId(20));
    assert_eq!(
        open.provenance.principal,
        Principal::new(b"writer-b".to_vec())
    );
    assert_eq!(open.provenance.committed_at, c1);
    // The current (open) version has not been closed, so it carries no closer.
    assert_eq!(open.closed_by, None, "an open version has no closer");

    // The close abuts the new open — no gap, no overlap.
    assert_eq!(closed.sys_to, open.sys_from);
}

#[test]
fn insert_on_a_live_key_is_rejected() {
    let mut delta = new_delta();
    let mut index = new_index();
    let mut writer = SysTimeWriter::new(StubClock::new(1));
    let key = BusinessKey::new(b"dup".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            b"first".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    let err = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            b"second".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .unwrap_err();
    assert!(matches!(err, SysTimeError::KeyExists));
}

#[test]
fn update_or_delete_without_a_live_version_is_rejected() {
    let mut delta = new_delta();
    let mut index = new_index();
    let mut writer = SysTimeWriter::new(StubClock::new(1));
    let key = BusinessKey::new(b"ghost".to_vec());

    assert!(matches!(
        writer
            .update(
                &mut delta,
                &mut index,
                &EmptySealed,
                key.clone(),
                b"x".to_vec(),
                0,
                TxnId(1),
                who()
            )
            .unwrap_err(),
        SysTimeError::KeyNotFound
    ));
    assert!(matches!(
        writer
            .delete(&mut delta, &mut index, &EmptySealed, &key, TxnId(2), who())
            .unwrap_err(),
        SysTimeError::KeyNotFound
    ));
}

#[test]
fn delete_closes_the_live_period_and_leaves_no_open_version() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(10);
    let mut writer = SysTimeWriter::new(clock);
    let key = BusinessKey::new(b"k".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            b"v".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    let closed_at = writer
        .delete(
            &mut delta,
            &mut index,
            &EmptySealed,
            &key,
            TxnId(2),
            Principal::new(b"deleter".to_vec()),
        )
        .unwrap();

    // No version is live after the delete.
    let live = delta
        .range_scan(.., Snapshot(SystemTimeMicros(i64::MAX - 1)), &index)
        .unwrap();
    assert!(live.is_empty(), "deleted key has no live version");

    let mut chains = drain_chains(&mut delta, &index);
    let versions = chains.remove(&key).expect("key has a chain");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].sys_to, closed_at, "delete closes the period");
    assert_ne!(versions[0].sys_to, SYSTEM_TIME_OPEN);
    // The birth provenance is the inserting txn; the *close* records the
    // deleting txn — the only place a delete's identity survives (STL-118).
    assert_eq!(versions[0].provenance.txn_id, TxnId(1));
    assert_eq!(
        versions[0].closed_by,
        Some(Provenance::new(
            TxnId(2),
            closed_at,
            Principal::new(b"deleter".to_vec())
        )),
        "the tombstone carries the deleting transaction's provenance"
    );
}

#[test]
fn reinsert_after_delete_opens_a_new_period_with_a_gap() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(100);
    let mut writer = SysTimeWriter::new(clock.clone());
    let key = BusinessKey::new(b"k".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            b"a".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    clock.set(200);
    let deleted_at = writer
        .delete(&mut delta, &mut index, &EmptySealed, &key, TxnId(2), who())
        .unwrap();
    clock.set(300);
    let reopened_at = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            b"b".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .unwrap();

    let mut chains = drain_chains(&mut delta, &index);
    let versions = chains.remove(&key).expect("key has a chain");
    assert_eq!(versions.len(), 2);
    // The deleted period ends strictly before the new one starts — a real,
    // correct system-time gap: the row did not exist in the database then.
    assert_eq!(versions[0].sys_to, deleted_at);
    assert_eq!(versions[1].sys_from, reopened_at);
    assert!(
        deleted_at < reopened_at,
        "delete then re-insert is a gap, not an abutment"
    );
}

// --- Interval invariant (DoD bullet 2) --------------------------------------

/// For any business key, the per-key chain is totally ordered by
/// `(sys_from, seq)`, non-overlapping, and gap-free.
///
/// Each seed drives a random series of inserts and updates across a small pool
/// of keys, *with the clock stalling at random* (flat or forward, never
/// backward — the writer no longer force-bumps, so a regressing clock is a
/// rejected error, exercised separately as a unit test). A stalled clock makes
/// consecutive commits share a `sys_from`, so the chain invariant is checked on
/// the `(sys_from, seq)` order: a same-tick supersession closes the prior period
/// degenerately (`sys_to == sys_from`) and the higher-`seq` version abuts it
/// (STL-145). `seq` is the per-op counter, distinct and increasing, the
/// transaction manager's total-order tiebreak. Updates (not deletes) keep a
/// key's chain continuous, so the generator uses exactly those — a delete
/// deliberately introduces a gap and is covered separately above.
#[test]
fn version_chains_are_non_overlapping_and_gap_free_under_seed_sweep() {
    const KEY_POOL: u64 = 6;

    for seed in 0u64..200 {
        let mut rng = Rng::new(seed);
        let mut delta = new_delta();
        let mut index = new_index();
        let clock = StubClock::new(1);
        let mut writer = SysTimeWriter::new(clock.clone());
        let mut live: Vec<bool> = vec![false; KEY_POOL as usize];

        let ops = 30 + rng.range(40);
        for op in 0..ops {
            // Drive the clock non-adversarially on the system axis: sometimes
            // forward, sometimes flat (a stall), never backward. A flat tick is
            // exactly the same-`sys_from` collision `seq` now orders; a backward
            // tick is a rejected error, so the sweep does not produce one here.
            let delta_t = (rng.next_u64() % 4) as i64; // [0, +3]
            let cur = clock.now_micros();
            clock.set(cur + delta_t);

            let key_idx = rng.range(KEY_POOL) as usize;
            let key = BusinessKey::new(vec![b'k', key_idx as u8]);
            let payload = format!("s{seed}-k{key_idx}").into_bytes();
            let txn = TxnId(op);
            // The per-commit sequence number: distinct and increasing across the
            // whole writer, so it totally orders even same-`sys_from` commits.
            let seq = op;

            if live[key_idx] {
                writer
                    .update(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        payload,
                        seq,
                        txn,
                        who(),
                    )
                    .unwrap();
            } else {
                writer
                    .insert(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        payload,
                        seq,
                        txn,
                        who(),
                    )
                    .unwrap();
                live[key_idx] = true;
            }
        }

        let chains = drain_chains(&mut delta, &index);
        for (key_idx, &is_live) in live.iter().enumerate() {
            if !is_live {
                continue;
            }
            let key = BusinessKey::new(vec![b'k', key_idx as u8]);
            let versions = chains.get(&key).expect("live key has a chain");
            assert!(!versions.is_empty(), "seed {seed}: live key has a chain");

            for w in versions.windows(2) {
                let (lo, hi) = (&w[0], &w[1]);
                // Totally ordered by (sys_from, seq) ⇒ no two versions share the
                // full key. Starts may now *tie* (a stalled clock), with seq
                // breaking the tie (STL-145).
                assert!(
                    (lo.sys_from, lo.seq) < (hi.sys_from, hi.seq),
                    "seed {seed} key {key_idx}: chain not ordered by (sys_from, seq)"
                );
                // Half-open, non-overlapping: a period ends no later than the
                // next begins (equal for a same-tick degenerate close).
                assert!(
                    lo.sys_to <= hi.sys_from,
                    "seed {seed} key {key_idx}: intervals overlap"
                );
                // Gap-free: consecutive update periods abut exactly — including a
                // degenerate same-tick close where lo.sys_to == lo.sys_from.
                assert_eq!(
                    lo.sys_to, hi.sys_from,
                    "seed {seed} key {key_idx}: gap between consecutive periods"
                );
                // Every non-final period is closed.
                assert_ne!(
                    lo.sys_to, SYSTEM_TIME_OPEN,
                    "seed {seed} key {key_idx}: a superseded period is still open"
                );
            }

            // Exactly one open period, and it is the last.
            let open_count = versions
                .iter()
                .filter(|v| v.sys_to == SYSTEM_TIME_OPEN)
                .count();
            assert_eq!(
                open_count, 1,
                "seed {seed} key {key_idx}: a live key has exactly one open period"
            );
            assert_eq!(
                versions.last().unwrap().sys_to,
                SYSTEM_TIME_OPEN,
                "seed {seed} key {key_idx}: the open period is the newest"
            );
        }
    }
}
