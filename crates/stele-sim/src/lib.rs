//! Deterministic simulation harness — Stele's correctness substrate.
//!
//! The FoundationDB / TigerBeetle pattern: a virtual clock, virtual disk,
//! virtual network, and a deterministic scheduler. Every test seed is a movie
//! that plays back the same way every time — bugs reproduce instead of haunt
//! ([`docs/06-testing-strategy.md`](../../../docs/06-testing-strategy.md),
//! [ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).
//!
//! Scaffold only at v0.1; the harness fills out across milestones as the core
//! crystallizes. The first thing it can drive end-to-end is the storage engine
//! over the in-memory backend ([STL-90]): [`run_storage_seed`] plays a seeded
//! workload of sealed-segment writes and reads against a [`MemDisk`] and
//! returns a digest of the result. Because the backend is heap-backed and the
//! workload is seed-driven with no wall-clock or RNG of its own, the same seed
//! always produces the same digest — the determinism property the whole
//! testing strategy rests on.
//!
//! The virtual clock/network and the seeded-fault virtual disk ([STL-109]) land
//! in later tickets; [`run_fault_seed`] exercises the minimal fault seam the
//! memory backend already exposes.

#![allow(dead_code)]

use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};
use stele_storage::backend::{Disk, DiskFile, FaultOp, Faults, MemDisk};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::{self, DmlWriter};
use stele_storage::merge;
use stele_storage::segment::{SegmentReader, SegmentWriter};
use stele_storage::systime::{EmptySealed, SysTimeWriter};
use stele_storage::validity::{Close, ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, ValidTimeWriter, unframe_payload};
use stele_storage::wal::{Checkpoint, Wal, WalConfig};
use stele_txn::TxnManager;

/// A deterministic, strictly-increasing clock for seeded scenarios.
///
/// The system-time axis needs commit timestamps that advance ([`stele_storage::systime`]),
/// and determinism forbids reading the wall clock — so the harness hands the
/// writer a counter that ticks once per [`Clock::now`]. Same seed ⇒ same
/// sequence of `sys_from` values.
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

/// Tiny `xorshift64*` PRNG — deterministic and dependency-free.
///
/// Seeded from a `u64` so a failing seed is a number we can replay. This is the
/// only source of "randomness" in a simulation run, which is exactly what makes
/// runs reproducible.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seed the generator. Avoids the zero fixpoint that traps bare xorshift.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    /// Next pseudo-random `u64`.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish integer in `0..bound` (`bound` must be non-zero).
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    /// Uniform-ish `usize` in `0..bound` (`bound` must be non-zero — like
    /// [`below`](Self::below), a zero `bound` panics with a division-by-zero).
    /// The result is `< bound`, so it always fits a `usize`.
    pub fn below_usize(&mut self, bound: usize) -> usize {
        usize::try_from(self.next_u64() % bound as u64).expect("value < bound fits usize")
    }

    /// A non-negative `i64` — used for seed-driven `sys_from` timestamps.
    pub fn next_i64_nonneg(&mut self) -> i64 {
        i64::try_from(self.next_u64() >> 1).expect("63-bit value fits i64")
    }

    /// `len` pseudo-random bytes.
    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next_u64() & 0xFF) as u8).collect()
    }
}

/// FNV-1a over a byte slice, folded into a running 64-bit digest. Order-
/// sensitive by construction, so the caller must feed bytes in a deterministic
/// order (we sort segment names before reading).
fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Fold a version's optional close-provenance into the running digest ([STL-118]).
/// A leading presence byte distinguishes an open version (`0`) from a closed one
/// (`1`, followed by the closing transaction's `txn_id` / `committed_at` /
/// `principal`) so the oracle is sensitive to *who closed a period*, not just to
/// the fact that one was closed.
fn fold_closed_by(mut digest: u64, closed_by: Option<&Provenance>) -> u64 {
    match closed_by {
        Some(c) => {
            digest = fnv1a(digest, &[1]);
            digest = fnv1a(digest, &c.txn_id.0.to_le_bytes());
            digest = fnv1a(digest, &c.committed_at.0.to_le_bytes());
            digest = fnv1a(digest, c.principal.as_bytes());
        }
        None => digest = fnv1a(digest, &[0]),
    }
    digest
}

/// Play a seeded storage workload against a fresh [`MemDisk`] and return a
/// digest of every version read back.
///
/// The workload writes a handful of sealed segments, each with a seed-driven
/// set of versions, then reads them all back in a deterministic (name-sorted)
/// order. Same seed ⇒ same digest; that equality *is* the determinism contract
/// this harness exists to guard.
#[must_use]
pub fn run_storage_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let disk = MemDisk::new();

    let segments = 1 + rng.below(5);
    for i in 0..segments {
        let name = format!("seg-{i:08}");
        let mut writer = SegmentWriter::create(&disk, &name).expect("create segment");
        let rows = 1 + rng.below(8);
        for _ in 0..rows {
            let key_len = 1 + rng.below_usize(4);
            let key = rng.bytes(key_len);
            let payload_len = rng.below_usize(64);
            let payload = rng.bytes(payload_len);
            // Provenance is part of every version — generate it from the same
            // seed stream so the round-trip exercises the provenance columns
            // deterministically ([STL-93]). A sealed segment stores only birth
            // state ([ADR-0023]): `sys_to` / `closed_by` are not segment columns,
            // so every pushed version is open/unresolved.
            let sys_from = rng.next_i64_nonneg() % 1_000_000;
            let txn_id = u64::try_from(rng.next_i64_nonneg()).unwrap_or(0);
            let principal_len = rng.below_usize(8);
            let principal = rng.bytes(principal_len);
            writer
                .push(Version::open(
                    BusinessKey::new(key),
                    SystemTimeMicros(sys_from),
                    Provenance::new(
                        TxnId(txn_id),
                        SystemTimeMicros(sys_from),
                        Principal::new(principal),
                    ),
                    payload,
                ))
                .expect("push version");
        }
        writer.finish().expect("finish segment");
    }

    // Read everything back in a deterministic order. `MemDisk::list` returns
    // HashMap order (non-deterministic across processes), so sort first.
    let mut names = disk.list().expect("list segments");
    names.sort();

    let mut digest = FNV_OFFSET;
    for name in &names {
        let reader = SegmentReader::open(&disk, name).expect("open segment");
        for v in reader.read_versions().expect("read versions") {
            digest = fold_version(digest, &v);
        }
    }
    digest
}

/// Fold a resolved version — birth fields plus the validity-index overlay
/// (`sys_to` / `closed_by`) — into the digest. The single place the seed oracles
/// agree on what a version "is", so a segment read, a delta drain, and a WAL
/// rebuild all hash the same bytes for the same logical version.
fn fold_version(mut digest: u64, v: &Version) -> u64 {
    digest = fnv1a(digest, v.business_key.as_bytes());
    digest = fnv1a(digest, &v.sys_from.0.to_le_bytes());
    digest = fnv1a(digest, &v.sys_to.0.to_le_bytes());
    digest = fnv1a(digest, &v.provenance.txn_id.0.to_le_bytes());
    digest = fnv1a(digest, &v.provenance.committed_at.0.to_le_bytes());
    digest = fnv1a(digest, v.provenance.principal.as_bytes());
    digest = fold_closed_by(digest, v.closed_by.as_ref());
    digest = fnv1a(digest, &v.payload);
    digest
}

/// Drain `delta`, overlay the validity `index` onto every staged version
/// ([`merge::fold_chains`]), and fold the resolved chains — in `(key, sys_from)`
/// order — into `digest`. The oracle for the system-time write paths: it sees
/// each version's materialized `sys_to` / `closed_by` exactly as a reader would.
fn fold_resolved_chains<D: Disk, I: Disk>(
    mut digest: u64,
    delta: &mut Delta<D>,
    index: &ValidityIndex<I>,
) -> u64 {
    let drained = delta.flush_to_segment().expect("flush");
    let chains = merge::fold_chains(drained, index).expect("fold chains");
    for chain in chains.values() {
        for v in chain.values() {
            digest = fold_version(digest, v);
        }
    }
    digest
}

/// Play a seeded valid-time workload against a fresh delta tier and return a
/// digest folding **both** temporal axes of every version read back.
///
/// Each key gets one framed insert ([`ValidTimeWriter`]) carrying a seed-derived
/// `[valid_from, valid_to)` interval and payload; the delta is drained and every
/// version's system interval, decoded valid interval, and user payload are mixed
/// into the digest. Same seed ⇒ same digest — the determinism contract of
/// [`run_storage_seed`], now exercising the valid-time ingestion path ([STL-92]).
#[must_use]
pub fn run_validtime_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let mut writer = ValidTimeWriter::new(StepClock::new(1), true);

    let keys = 1 + rng.below(8);
    for i in 0..keys {
        let key = BusinessKey::new(format!("k-{i:04}").into_bytes());
        let from = rng.next_i64_nonneg() % 1_000_000;
        // `+ 1` guarantees a non-empty half-open interval (valid_from < valid_to).
        let span = 1 + (rng.next_i64_nonneg() % 1_000_000);
        let interval = ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(from + span))
            .expect("from < from + span");
        let payload_len = rng.below_usize(32);
        let payload = rng.bytes(payload_len);
        let txn_id = u64::try_from(rng.next_i64_nonneg()).unwrap_or(0);
        let principal_len = rng.below_usize(8);
        let principal = rng.bytes(principal_len);
        writer
            .insert(
                &mut delta,
                &mut index,
                key,
                Some(interval),
                payload,
                TxnId(txn_id),
                Principal::new(principal),
            )
            .expect("framed insert");
    }

    // `flush_to_segment` drains in `(business_key, sys_from)` order — deterministic.
    let mut digest = FNV_OFFSET;
    for v in delta.flush_to_segment().expect("flush") {
        let (valid, user) = unframe_payload(true, &v.payload).expect("unframe");
        let valid = valid.expect("valid-time table carries an interval");
        digest = fnv1a(digest, v.business_key.as_bytes());
        digest = fnv1a(digest, &v.sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &v.sys_to.0.to_le_bytes());
        digest = fnv1a(digest, &v.provenance.txn_id.0.to_le_bytes());
        digest = fnv1a(digest, &v.provenance.committed_at.0.to_le_bytes());
        digest = fnv1a(digest, v.provenance.principal.as_bytes());
        digest = fnv1a(digest, &valid.from.0.to_le_bytes());
        digest = fnv1a(digest, &valid.to.0.to_le_bytes());
        digest = fnv1a(digest, user);
    }
    digest
}

/// Play a seeded insert/update/delete workload through the real system-time
/// write path and return a digest folding every version's close-provenance.
///
/// Where [`run_storage_seed`] hand-builds closed versions to exercise the
/// segment columns, this drives the *writer* ([`SysTimeWriter`]): each op either
/// opens a key, supersedes it (closing the prior period), or deletes it (closing
/// without re-opening). The drained chain is folded — system interval, birth
/// provenance, and `closed_by` — so the oracle covers "who closed each period"
/// end-to-end through the delta-tier encode/decode, including the delete case
/// where no successor version carries that identity ([STL-118]). Same seed ⇒
/// same digest.
#[must_use]
pub fn run_delete_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let mut writer = SysTimeWriter::new(StepClock::new(1));

    let key_count = 1 + rng.below_usize(6);
    let mut live = vec![false; key_count];
    let ops = 8 + rng.below(24);
    for op in 0..ops {
        let k = rng.below_usize(key_count);
        let key = BusinessKey::new(format!("k-{k:04}").into_bytes());
        let txn = TxnId(op);
        let principal_len = rng.below_usize(8);
        let principal = Principal::new(rng.bytes(principal_len));
        if live[k] {
            // A live key is either superseded (close + re-open) or deleted
            // (close, no re-open) — both record `principal` as the closer in the
            // validity index.
            if rng.below(2) == 0 {
                writer
                    .delete(&mut delta, &mut index, &EmptySealed, &key, txn, principal)
                    .expect("delete");
                live[k] = false;
            } else {
                let payload_len = rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                writer
                    .update(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        payload,
                        txn,
                        principal,
                    )
                    .expect("update");
            }
        } else {
            let payload_len = rng.below_usize(16);
            let payload = rng.bytes(payload_len);
            writer
                .insert(
                    &mut delta,
                    &mut index,
                    &EmptySealed,
                    key,
                    payload,
                    txn,
                    principal,
                )
                .expect("insert");
            live[k] = true;
        }
    }

    // Drain the delta and overlay the validity index — the digest sees each
    // version's materialized `sys_to` / `closed_by`, including the delete case
    // where no successor version carries the closer's identity ([STL-118]).
    fold_resolved_chains(FNV_OFFSET, &mut delta, &index)
}

/// Play a seeded insert/update/delete workload through the **full DML write
/// path** — WAL → delta — then rebuild the delta purely from the WAL and fold
/// the reconstructed chain into the digest.
///
/// Where [`run_delete_seed`] drives the system-time writer straight into the
/// delta, this drives [`DmlWriter`]: each op resolves, appends a redo record to
/// the WAL, and stages into the delta ([STL-94]). After the workload the WAL is
/// fsync'd and replayed into a *fresh* delta, and that reconstructed chain is
/// what gets digested — so the seed sweep regresses on the redo-record codec and
/// the WAL→delta recovery path, not just the in-memory writer. Same seed ⇒ same
/// digest.
#[must_use]
pub fn run_dml_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    // System-only table (`valid_time = false`): payloads carry no interval prefix.
    let mut writer = DmlWriter::new(wal.clone(), StepClock::new(1), false);

    let key_count = 1 + rng.below_usize(6);
    let mut live = vec![false; key_count];
    let ops = 8 + rng.below(24);
    for op in 0..ops {
        let k = rng.below_usize(key_count);
        let key = BusinessKey::new(format!("k-{k:04}").into_bytes());
        let txn = TxnId(op);
        let principal_len = rng.below_usize(8);
        let principal = Principal::new(rng.bytes(principal_len));
        if live[k] {
            if rng.below(2) == 0 {
                writer
                    .delete(&mut delta, &mut index, &key, txn, principal)
                    .expect("delete");
                live[k] = false;
            } else {
                let payload_len = rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                writer
                    .update(&mut delta, &mut index, key, None, payload, txn, principal)
                    .expect("update");
            }
        } else {
            let payload_len = rng.below_usize(16);
            let payload = rng.bytes(payload_len);
            writer
                .insert(&mut delta, &mut index, key, None, payload, txn, principal)
                .expect("insert");
            live[k] = true;
        }
    }
    wal.tick().expect("group-commit fsync");

    // Rebuild the delta **and** the validity index from the WAL alone, then
    // digest the reconstructed, index-overlaid chain — so the seed sweep
    // regresses on the tagged redo codec and the WAL→(delta + index) recovery
    // path, not just the in-memory writer ([ADR-0023]).
    let mut replayed = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut replayed_index =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    dml::replay(&wal, &mut replayed, &mut replayed_index, Checkpoint::BEGIN).expect("replay");
    fold_resolved_chains(FNV_OFFSET, &mut replayed, &replayed_index)
}

/// Assert the validity index rebuilt from the WAL reproduces the pre-crash one.
///
/// Plays a seeded insert/update/delete workload through the full DML write path,
/// then asserts the validity index **rebuilt from the WAL alone** equals the
/// pre-crash index *exactly* — the rebuildability guarantee of [ADR-0023]
/// (STL-133 DoD). Returns a digest of the (identical) rebuilt index so the sweep
/// also regresses on determinism.
///
/// # Panics
///
/// Panics if the rebuilt index is not byte-identical to the pre-crash one — a
/// correctness regression, not a workload outcome.
#[must_use]
pub fn run_recovery_index_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let mut writer = DmlWriter::new(wal.clone(), StepClock::new(1), false);

    let key_count = 1 + rng.below_usize(6);
    let mut live = vec![false; key_count];
    let ops = 8 + rng.below(24);
    for op in 0..ops {
        let k = rng.below_usize(key_count);
        let key = BusinessKey::new(format!("k-{k:04}").into_bytes());
        let txn = TxnId(op);
        let principal_len = rng.below_usize(8);
        let principal = Principal::new(rng.bytes(principal_len));
        if live[k] {
            if rng.below(2) == 0 {
                writer
                    .delete(&mut delta, &mut index, &key, txn, principal)
                    .expect("delete");
                live[k] = false;
            } else {
                let payload_len = rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                writer
                    .update(&mut delta, &mut index, key, None, payload, txn, principal)
                    .expect("update");
            }
        } else {
            let payload_len = rng.below_usize(16);
            let payload = rng.bytes(payload_len);
            writer
                .insert(&mut delta, &mut index, key, None, payload, txn, principal)
                .expect("insert");
            live[k] = true;
        }
    }
    wal.tick().expect("group-commit fsync");

    // Crash: throw away the delta *and* the index, rebuild both from the WAL.
    let mut replayed = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut rebuilt = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
    dml::replay(&wal, &mut replayed, &mut rebuilt, Checkpoint::BEGIN).expect("replay");

    let before = index.materialize().expect("materialize pre-crash index");
    let after = rebuilt.materialize().expect("materialize rebuilt index");
    assert_eq!(
        before, after,
        "seed {seed}: rebuild-from-WAL must reproduce the exact validity index",
    );

    // Digest the (identical) rebuilt index for the determinism sweep.
    let mut digest = FNV_OFFSET;
    for ((key, sys_from), interval) in &after {
        digest = fnv1a(digest, key.as_bytes());
        digest = fnv1a(digest, &sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &interval.sys_to.0.to_le_bytes());
        digest = fold_closed_by(digest, Some(&interval.closed_by));
    }
    digest
}

/// Close any open version of `key` at `commit`, then open a fresh `[commit, +∞)`
/// version — the version a committed writer stages with the manager-assigned
/// commit timestamp as its `sys_from`. Keeps exactly one open version per key, so
/// the per-key chain stays non-overlapping.
fn stage_committed_write(
    delta: &mut Delta<MemDisk>,
    index: &mut ValidityIndex<MemDisk>,
    key: &BusinessKey,
    txn_id: TxnId,
    commit: SystemTimeMicros,
) {
    // Resolve the key's open version across the delta tier and the index; if one
    // exists, materialize its close into the index (write-once) before opening
    // the new version. `commit` is strictly greater than every prior `sys_from`,
    // so the open version (if any) is the one resolved.
    let candidates = delta.candidate_versions(key).expect("candidate versions");
    let live =
        merge::resolve_open(&candidates, &[], index, key, Snapshot(commit)).expect("resolve");
    if let Some(open) = live {
        index
            .insert_close(Close {
                business_key: key.clone(),
                sys_from: open.sys_from,
                sys_to: commit,
                closed_by: Provenance::new(txn_id, commit, Principal::new(b"sim".to_vec())),
            })
            .expect("close prior version");
    }
    delta
        .insert(Version::open(
            key.clone(),
            commit,
            Provenance::new(txn_id, commit, Principal::new(b"sim".to_vec())),
            format!("v@{}", commit.0).into_bytes(),
        ))
        .expect("open new version");
}

/// Play a seeded MVCC workload through the real [`TxnManager`] — concurrent
/// writers contending on a small key space — and fold every commit outcome and
/// snapshot read into a digest.
///
/// Each round begins two transactions whose snapshots overlap, points them at
/// seed-chosen keys, and commits them in a seed-chosen order. Same-key
/// contenders force a write-write conflict: exactly one commits and the other
/// gets [`TxnError`](stele_txn::TxnError)'s clean retry signal ([STL-99] DoD).
/// Disjoint-key writers both commit. A committed writer stages its version at the
/// manager-assigned commit timestamp; a fresh reader then resolves a seed-chosen
/// key at its snapshot, exercising the `sys_from ≤ s < sys_to` visibility rule.
/// The digest folds the commit/conflict pattern, the commit timestamps, and the
/// versions read back — so the seed sweep regresses on commit ordering, conflict
/// detection, and snapshot resolution together. Same seed ⇒ same digest.
#[must_use]
pub fn run_mvcc_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mgr = TxnManager::new(StepClock::new(1), wal);
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");

    let key_count = 1 + rng.below_usize(5);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    let mut digest = FNV_OFFSET;
    let rounds = 8 + rng.below(24);
    for _ in 0..rounds {
        // Two writers begin before either commits — their snapshots overlap, so a
        // shared key forces a conflict the manager must resolve to one winner.
        let mut a = mgr.begin();
        let mut b = mgr.begin();
        let ka = rng.below_usize(key_count);
        let kb = rng.below_usize(key_count);
        a.write(keys[ka].clone());
        b.write(keys[kb].clone());

        // Commit in a seed-chosen order; whoever lands first wins a contended key.
        // `commit` consumes the transaction, so the txns are moved into the array.
        let ordered: [(stele_txn::Transaction, usize); 2] = if rng.below(2) == 0 {
            [(a, ka), (b, kb)]
        } else {
            [(b, kb), (a, ka)]
        };
        for (txn, k) in ordered {
            match mgr.commit(txn) {
                Ok(committed) => {
                    digest = fnv1a(digest, &[1]);
                    digest = fnv1a(digest, &committed.commit_ts.0.to_le_bytes());
                    stage_committed_write(
                        &mut delta,
                        &mut index,
                        &keys[k],
                        committed.txn_id,
                        committed.commit_ts,
                    );
                }
                // The only expected failure is a write-write conflict — folded as
                // a distinct outcome byte. Any other error (WAL failure, time
                // exhaustion) is a real regression, not a workload outcome, so the
                // seed fails loudly rather than silently digesting a "valid" run.
                Err(stele_txn::TxnError::Conflict) => digest = fnv1a(digest, &[0]),
                Err(other) => panic!("unexpected commit error in MVCC seed: {other}"),
            }
        }

        // A fresh reader resolves a seed-chosen key at its snapshot.
        let reader = mgr.begin();
        let rk = rng.below_usize(key_count);
        let seen = delta
            .range_scan(
                keys[rk].clone()..=keys[rk].clone(),
                reader.snapshot(),
                &index,
            )
            .expect("range scan");
        match seen.into_iter().next() {
            Some(v) => {
                digest = fnv1a(digest, &[1]);
                digest = fnv1a(digest, &v.sys_from.0.to_le_bytes());
                digest = fnv1a(digest, &v.payload);
            }
            None => digest = fnv1a(digest, &[0]),
        }
    }
    digest
}

/// Play a seeded sequence of operations against a [`MemDisk`] whose
/// fault schedule is also seed-derived, and return the per-operation
/// success/failure pattern.
///
/// This is a minimal exercise of the memory backend's deterministic fault hook:
/// the same seed schedules the same faults at the same points, so the returned
/// pattern is reproducible. The richer seeded-fault virtual disk is [STL-109].
#[must_use]
pub fn run_fault_seed(seed: u64) -> Vec<bool> {
    let mut rng = Rng::new(seed);
    let faults = Faults::new();

    // Schedule a seed-driven handful of sync faults interleaved with appends.
    let ops = 8 + rng.below_usize(8);
    let op_kinds: Vec<FaultOp> = (0..ops)
        .map(|_| {
            if rng.below(3) == 0 {
                FaultOp::Sync
            } else {
                FaultOp::Append
            }
        })
        .collect();
    for &op in &op_kinds {
        if rng.below(2) == 0 {
            faults.schedule(op, std::io::ErrorKind::Other);
        }
    }

    let disk = MemDisk::with_faults(faults);
    let mut file = disk.create("log").expect("create is not scheduled");
    op_kinds
        .iter()
        .map(|op| match op {
            FaultOp::Append => file.append(b"record").is_ok(),
            FaultOp::Sync => file.sync().is_ok(),
            _ => true,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_seed_is_reproducible() {
        for seed in 0..64 {
            assert_eq!(
                run_storage_seed(seed),
                run_storage_seed(seed),
                "seed {seed} must replay to an identical digest"
            );
        }
    }

    #[test]
    fn distinct_seeds_diverge() {
        // Not a hard guarantee, but across a wide sweep the digests must not all
        // collapse to one value — that would mean the workload ignores the seed.
        let digests: std::collections::HashSet<u64> = (0..64).map(run_storage_seed).collect();
        assert!(
            digests.len() > 1,
            "seeded workloads must actually depend on the seed"
        );
    }

    #[test]
    fn fault_pattern_is_reproducible() {
        for seed in 0..64 {
            assert_eq!(run_fault_seed(seed), run_fault_seed(seed));
        }
    }

    #[test]
    fn validtime_seed_is_reproducible() {
        for seed in 0..64 {
            assert_eq!(
                run_validtime_seed(seed),
                run_validtime_seed(seed),
                "seed {seed} must replay to an identical valid-time digest"
            );
        }
    }

    #[test]
    fn validtime_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..64).map(run_validtime_seed).collect();
        assert!(
            digests.len() > 1,
            "valid-time workload must actually depend on the seed"
        );
    }

    #[test]
    fn delete_seed_is_reproducible() {
        for seed in 0..64 {
            assert_eq!(
                run_delete_seed(seed),
                run_delete_seed(seed),
                "seed {seed} must replay to an identical delete-provenance digest"
            );
        }
    }

    #[test]
    fn delete_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..64).map(run_delete_seed).collect();
        assert!(
            digests.len() > 1,
            "delete/close-provenance workload must actually depend on the seed"
        );
    }

    #[test]
    fn dml_seed_is_reproducible() {
        for seed in 0..64 {
            assert_eq!(
                run_dml_seed(seed),
                run_dml_seed(seed),
                "seed {seed} must replay to an identical DML digest"
            );
        }
    }

    #[test]
    fn dml_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..64).map(run_dml_seed).collect();
        assert!(
            digests.len() > 1,
            "the WAL→delta DML workload must actually depend on the seed"
        );
    }

    #[test]
    fn recovery_index_seed_is_reproducible() {
        // Also asserts (inside the seed) that the WAL-rebuilt validity index is
        // byte-identical to the pre-crash one across the sweep ([ADR-0023] DoD).
        for seed in 0..64 {
            assert_eq!(
                run_recovery_index_seed(seed),
                run_recovery_index_seed(seed),
                "seed {seed} must replay to an identical rebuilt-index digest"
            );
        }
    }

    #[test]
    fn recovery_index_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_recovery_index_seed).collect();
        assert!(
            digests.len() > 1,
            "the WAL→index rebuild workload must actually depend on the seed"
        );
    }

    #[test]
    fn mvcc_seed_is_reproducible() {
        for seed in 0..64 {
            assert_eq!(
                run_mvcc_seed(seed),
                run_mvcc_seed(seed),
                "seed {seed} must replay to an identical MVCC digest"
            );
        }
    }

    #[test]
    fn mvcc_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..64).map(run_mvcc_seed).collect();
        assert!(
            digests.len() > 1,
            "the MVCC commit/conflict workload must actually depend on the seed"
        );
    }
}
