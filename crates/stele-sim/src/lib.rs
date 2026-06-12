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
//! The seeded-fault virtual disk ([STL-109]) lives in the `fault_disk` module:
//! [`run_fault_seed`] drives a [`FaultDisk`] through a seeded workload and folds
//! its fault-event log into a digest. [`run_engine_recover_faults_seed`] then
//! drives the full kill-and-recover path *through* that fault disk ([STL-153]),
//! asserting recovery either converges to a consistent committed prefix or cleanly
//! detects corruption — never silently diverges. The v0.2 surface rides the same
//! pattern ([STL-187]): [`run_txn_commit_rollback_faults_seed`] interleaves
//! committed and rolled-back multi-statement transactions under those faults, and
//! [`run_vectorized_exec_faults_seed`] drives the vectorized aggregate/join
//! operators over fault-recovered state against independent scalar folds.
//!
//! The deterministic substrate — a [`VirtualClock`] that advances on demand, the
//! ChaCha20-backed [`SeededRng`], and the cooperative [`Scheduler`] that drives
//! futures in a seed-determined order ([STL-108]) — now lives alongside the
//! storage scenarios; [`run_schedule_seed`] is its determinism demo (same seed ⇒
//! byte-identical trace). The simulated network lands in an adjacent ticket.
//!
//! The [`Scenario`] trait and [`registry`] turn those per-seed digest functions
//! into a registry the CLI drives ([STL-110]): [`sweep`] runs every registered
//! scenario across N distinct seeds; [`replay`] re-runs one seed across all of
//! them. On an invariant violation a scenario panics with the seed in its
//! message, and [`install_failure_reporter`] turns that into a prominent
//! `scenario + seed` banner — so a failure is a *number* a contributor replays
//! with `just sim-seed <N>`.

#![allow(dead_code)]

mod as_of_oracle;
mod fault_disk;
mod index_build;
mod si_oracle;

pub use as_of_oracle::run_as_of_oracle_seed;
pub use fault_disk::{FaultDisk, FaultEvent, FaultKind, FaultProfile};
pub use index_build::run_index_build_crash_seed;
pub use si_oracle::run_si_oracle_seed;

use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_catalog::{Catalog, ColumnDef, TableTemporal};
use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};
use stele_common::types::LogicalType;
use stele_exec::{
    AggregateFunc, Aggregator, Column, Expr, JoinType, SnapshotScan, Vector, hash_aggregate,
    hash_join,
};
use stele_storage::backend::{Disk, DiskFile, MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::{self, DmlWriter};
use stele_storage::engine::Engine;
use stele_storage::merge;
use stele_storage::segment::{ColumnId, Predicate, SegmentReader, SegmentWriter, ZoneBound};
use stele_storage::systime::{EmptySealed, SealedSegments, SysTimeWriter};
use stele_storage::validity::{Close, ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, ValidTimeWriter, unframe_payload};
use stele_storage::wal::{Checkpoint, Wal, WalConfig};
use stele_txn::{ChainError, TxnManager, verify_chain};

mod chacha;
mod clock;
mod scheduler;

pub use chacha::SeededRng;
pub use clock::VirtualClock;
pub use scheduler::{
    Event, Scheduler, TaskId, encode_events, record, run_schedule_seed, run_schedule_seed_digest,
    schedule_trace, sleep, trace_digest, yield_now,
};

/// A deterministic, strictly-increasing clock for seeded scenarios.
///
/// The system-time axis needs commit timestamps that advance ([`stele_storage::systime`]),
/// and determinism forbids reading the wall clock — so the harness hands the
/// writer a counter that ticks once per [`Clock::now`]. Same seed ⇒ same
/// sequence of `sys_from` values.
pub(crate) struct StepClock(AtomicI64);

impl StepClock {
    pub(crate) const fn new(start: i64) -> Self {
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
/// source of "randomness" the storage scenarios use, which is exactly what makes
/// runs reproducible. The scheduler substrate ([STL-108]) uses the
/// stronger ChaCha20-backed [`SeededRng`] instead; migrating these scenarios onto
/// it is a deferred cleanup.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seed the generator. Avoids the zero fixpoint that traps bare xorshift.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    /// Next pseudo-random `u64`.
    pub const fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish integer in `0..bound` (`bound` must be non-zero).
    pub const fn below(&mut self, bound: u64) -> u64 {
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
pub(crate) fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Fold an optional payload into the running digest with a presence tag, so a SQL
/// `NULL` (`None`) and a present value never collide ([STL-154]): a leading `1`
/// then the bytes for a present value, a lone `0` for `None`.
fn fold_optional_payload(digest: u64, payload: Option<&[u8]>) -> u64 {
    payload.map_or_else(
        || fnv1a(digest, &[0]),
        |bytes| fnv1a(fnv1a(digest, &[1]), bytes),
    )
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
                    // Derived from `sys_from`, not drawn from the seed stream, so
                    // the seq column is exercised with varied values without
                    // perturbing the deterministic rng sequence behind it.
                    u64::try_from(sys_from).unwrap_or(0),
                    Provenance::new(
                        TxnId(txn_id),
                        SystemTimeMicros(sys_from),
                        Principal::new(principal),
                    ),
                    Some(payload),
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
    // `seq` is part of a version's identity — the per-commit total-order tiebreak
    // ([ADR-0024], STL-145). Fold it next to `sys_from`, the timestamp it
    // disambiguates, so the determinism oracle catches a dropped or transposed
    // seq the same way it would a wrong `sys_from`.
    digest = fnv1a(digest, &v.seq.to_le_bytes());
    digest = fnv1a(digest, &v.sys_to.0.to_le_bytes());
    digest = fnv1a(digest, &v.provenance.txn_id.0.to_le_bytes());
    digest = fnv1a(digest, &v.provenance.committed_at.0.to_le_bytes());
    digest = fnv1a(digest, v.provenance.principal.as_bytes());
    digest = fold_closed_by(digest, v.closed_by.as_ref());
    // Fold with a presence tag so a SQL NULL payload (`None`) and an empty
    // payload (`Some(vec![])`) stay distinct in the determinism digest ([STL-154]).
    digest = fold_optional_payload(digest, v.payload.as_deref());
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
                &EmptySealed,
                key,
                Some(interval),
                Some(payload),
                0,
                TxnId(txn_id),
                Principal::new(principal),
            )
            .expect("framed insert");
    }

    // `flush_to_segment` drains in `(business_key, sys_from)` order — deterministic.
    let mut digest = FNV_OFFSET;
    for v in delta.flush_to_segment().expect("flush") {
        let (valid, user) = unframe_payload(
            true,
            v.payload.as_deref().expect("valid-time row has a payload"),
        )
        .expect("unframe");
        let valid = valid.expect("valid-time table carries an interval");
        digest = fnv1a(digest, v.business_key.as_bytes());
        digest = fnv1a(digest, &v.sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &v.seq.to_le_bytes());
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
                        Some(payload),
                        0,
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
                    Some(payload),
                    0,
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
                        None,
                        Some(payload),
                        0,
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
                    None,
                    Some(payload),
                    0,
                    txn,
                    principal,
                )
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
                        None,
                        Some(payload),
                        0,
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
                    None,
                    Some(payload),
                    0,
                    txn,
                    principal,
                )
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
    for ((key, sys_from, seq), interval) in &after {
        digest = fnv1a(digest, key.as_bytes());
        digest = fnv1a(digest, &sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &seq.to_le_bytes());
        digest = fnv1a(digest, &interval.sys_to.0.to_le_bytes());
        digest = fold_closed_by(digest, Some(&interval.closed_by));
    }
    digest
}

/// Kill and `recover` a seeded [`Engine`] workload, asserting it converges.
///
/// Plays a seeded insert/update/delete workload through the full [`Engine`] boot
/// path — with periodic checkpoints — then crashes and recovers, converging to a
/// consistent state with an *exactly* rebuilt validity index ([STL-102] DoD).
///
/// Where [`run_recovery_index_seed`] drives the low-level [`DmlWriter`] +
/// [`dml::replay`] directly, this drives the [`Engine`] recovery *driver*: each op
/// goes through [`Engine::insert`] / [`Engine::update`] / [`Engine::delete`], a
/// checkpoint is taken at seed-chosen points (the periodic + graceful-shutdown
/// fsyncs the boot flow keys off), and after the workload the engine is dropped
/// (the crash) and [`Engine::recover`] rebuilds the delta and index from the WAL
/// alone. A tier-agnostic reference oracle records each key's committed timeline.
/// Two correctness properties are asserted, then folded into the digest: the
/// recovered validity index materializes byte-for-byte to the pre-crash one, and
/// the recovered `AS OF` read matches the reference oracle at every commit
/// boundary. Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics if the rebuilt index differs from the pre-crash one, or if a recovered
/// `AS OF` disagrees with the reference oracle — correctness regressions, not
/// workload outcomes.
#[must_use]
pub fn run_engine_recover_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let disk = MemDisk::new();

    // Tier-agnostic reference oracle: per key, the committed payload-or-delete
    // timeline keyed by commit `sys_from`. An independent model, not a mirror of
    // the engine's merged read path.
    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    let mut commits: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 1 + rng.below_usize(6);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    let before_index = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open engine");
        let mut live = vec![false; key_count];
        let ops = 8 + rng.below(24);
        for op in 0..ops {
            let k = rng.below_usize(key_count);
            let key = keys[k].clone();
            let txn = TxnId(op);
            let principal_len = rng.below_usize(8);
            let principal = Principal::new(rng.bytes(principal_len));
            let commit = if live[k] {
                if rng.below(2) == 0 {
                    let c = engine.delete(&key, txn, principal).expect("delete").commit;
                    oracle.entry(key).or_default().insert(c, None);
                    live[k] = false;
                    c
                } else {
                    let payload_len = rng.below_usize(16);
                    let payload = rng.bytes(payload_len);
                    let c = engine
                        .update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
                        .expect("update")
                        .commit;
                    oracle.entry(key).or_default().insert(c, Some(payload));
                    c
                }
            } else {
                let payload_len = rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                let c = engine
                    .insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
                    .expect("insert")
                    .commit;
                oracle.entry(key).or_default().insert(c, Some(payload));
                live[k] = true;
                c
            };
            commits.push(commit);
            // A periodic checkpoint at seed-chosen points — the fsync + durable
            // fence the recovery flow records.
            if rng.below(3) == 0 {
                engine.checkpoint().expect("periodic checkpoint");
            }
        }
        // Graceful-shutdown checkpoint, then drop the engine — the crash.
        engine.checkpoint().expect("shutdown checkpoint");
        engine
            .materialize_index()
            .expect("materialize pre-crash index")
    };

    // Recover through the driver: validate segments, load checkpoint, replay WAL,
    // rebuild the delta + index.
    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    let after_index = recovered
        .materialize_index()
        .expect("materialize rebuilt index");
    assert_eq!(
        before_index, after_index,
        "seed {seed}: recovery must rebuild the exact validity index",
    );

    // The recovered `AS OF` must match the reference oracle at every boundary —
    // just before the first commit and at every commit thereafter.
    let mut probes: Vec<SystemTimeMicros> = Vec::new();
    if let Some(first) = commits.first() {
        probes.push(SystemTimeMicros(first.0 - 1));
    }
    probes.extend(commits.iter().copied());

    let mut digest = FNV_OFFSET;
    for s in probes {
        for key in &keys {
            let want = oracle
                .get(key)
                .and_then(|timeline| timeline.range(..=s).next_back())
                .and_then(|(_, payload)| payload.clone());
            // `.flatten()` drops the NULL-ness Option: this scenario writes none (STL-154).
            let got = recovered
                .as_of_payload(key, Snapshot(s))
                .expect("as_of")
                .flatten();
            assert_eq!(
                got, want,
                "seed {seed} @ s={s:?} key {key:?}: recovered AS OF must match the oracle",
            );
            digest = fnv1a(digest, key.as_bytes());
            digest = fold_optional_payload(digest, got.as_deref());
        }
    }
    // Fold the (identical) rebuilt index for the determinism sweep.
    for ((key, sys_from, seq), interval) in &after_index {
        digest = fnv1a(digest, key.as_bytes());
        digest = fnv1a(digest, &sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &seq.to_le_bytes());
        digest = fnv1a(digest, &interval.sys_to.0.to_le_bytes());
        digest = fold_closed_by(digest, Some(&interval.closed_by));
    }
    digest
}

/// Kill and `recover` a seeded [`Engine`] workload that periodically **flushes**
/// the delta tier into sealed segments ([STL-177]).
///
/// This is [`run_engine_recover_seed`] with the periodic durability point upgraded
/// from a fence-only [`Engine::checkpoint`] to a [`Engine::flush`]: each flush
/// seals the staged versions/retractions into a segment and advances the replay
/// floor, so recovery rebuilds the flushed prefix **from the segment store** and
/// replays only the WAL tail. There is *no* shutdown flush — the post-last-flush
/// writes live only in the WAL at crash time, so the sweep exercises both halves
/// of recovery: the from-segment rebuild and the bounded tail replay, composed.
///
/// The invariants asserted are the same as the fence-only sweep and pin the
/// STL-177 correctness claim across random histories: the recovered validity index
/// must equal the pre-crash one **exactly** (segment rebuild + tail replay = the
/// live closes), and every `AS OF` boundary must match the tier-agnostic reference
/// oracle. Same seed ⇒ same digest.
#[must_use]
pub fn run_engine_flush_recover_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let disk = MemDisk::new();

    // Tier-agnostic reference oracle: per key, the committed payload-or-delete
    // timeline keyed by commit `sys_from`.
    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    let mut commits: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 1 + rng.below_usize(6);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    let before_index = {
        // Seed a small per-seed row-group bound so a flush of more than the bound's
        // rows seals a *multi*-row-group segment ([STL-197]) — a narrower flush
        // still stays one row-group. Recovery must rebuild the exact validity index
        // across the finer split either way; this is the engine-level analogue of
        // the 1–3-row bound the SnapshotScan oracle seeds at the segment layer
        // ([STL-155]).
        let row_group_rows = 1 + rng.below_usize(3);
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false)
            .expect("open engine")
            .with_flush_row_group_rows(row_group_rows);
        let mut live = vec![false; key_count];
        let ops = 8 + rng.below(24);
        for op in 0..ops {
            let k = rng.below_usize(key_count);
            let key = keys[k].clone();
            let txn = TxnId(op);
            let principal_len = rng.below_usize(8);
            let principal = Principal::new(rng.bytes(principal_len));
            let commit = if live[k] {
                if rng.below(2) == 0 {
                    let c = engine.delete(&key, txn, principal).expect("delete").commit;
                    oracle.entry(key).or_default().insert(c, None);
                    live[k] = false;
                    c
                } else {
                    let payload_len = rng.below_usize(16);
                    let payload = rng.bytes(payload_len);
                    let c = engine
                        .update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
                        .expect("update")
                        .commit;
                    oracle.entry(key).or_default().insert(c, Some(payload));
                    c
                }
            } else {
                let payload_len = rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                let c = engine
                    .insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
                    .expect("insert")
                    .commit;
                oracle.entry(key).or_default().insert(c, Some(payload));
                live[k] = true;
                c
            };
            commits.push(commit);
            // A periodic flush at seed-chosen points — seals the delta into a
            // segment and advances the recovery floor.
            if rng.below(3) == 0 {
                engine.flush().expect("periodic flush");
            }
        }
        // No shutdown flush: drop the engine with a WAL tail past the last flush,
        // so recovery must compose the segment prefix with the replayed tail.
        engine
            .materialize_index()
            .expect("materialize pre-crash index")
    };

    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    let after_index = recovered
        .materialize_index()
        .expect("materialize rebuilt index");
    assert_eq!(
        before_index, after_index,
        "seed {seed}: flush-recovery must rebuild the exact validity index",
    );

    let mut probes: Vec<SystemTimeMicros> = Vec::new();
    if let Some(first) = commits.first() {
        probes.push(SystemTimeMicros(first.0 - 1));
    }
    probes.extend(commits.iter().copied());

    let mut digest = FNV_OFFSET;
    for s in probes {
        for key in &keys {
            let want = oracle
                .get(key)
                .and_then(|timeline| timeline.range(..=s).next_back())
                .and_then(|(_, payload)| payload.clone());
            let got = recovered
                .as_of_payload(key, Snapshot(s))
                .expect("as_of")
                .flatten();
            assert_eq!(
                got, want,
                "seed {seed} @ s={s:?} key {key:?}: flush-recovered AS OF must match the oracle",
            );
            digest = fnv1a(digest, key.as_bytes());
            digest = fold_optional_payload(digest, got.as_deref());
        }
    }
    for ((key, sys_from, seq), interval) in &after_index {
        digest = fnv1a(digest, key.as_bytes());
        digest = fnv1a(digest, &sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &seq.to_le_bytes());
        digest = fnv1a(digest, &interval.sys_to.0.to_le_bytes());
        digest = fold_closed_by(digest, Some(&interval.closed_by));
    }
    digest
}

/// Map a draw from `rng` to a probability in `[lo, hi]` permille (thousandths) —
/// a small, seed-derived spread so different seeds stress different fault mixes
/// while every probability stays integer-derived and reproducible.
#[allow(clippy::cast_precision_loss)] // permille < 1000 is exact in f64
pub(crate) fn prob_permille(rng: &mut Rng, lo: u64, hi: u64) -> f64 {
    let pm = lo + rng.below(hi - lo + 1);
    pm as f64 / 1000.0
}

/// Assert a fault-recovered `engine`'s observable state is a consistent committed
/// prefix of the reference `oracle`, returning the matching cutoff length and the
/// recovered probe fingerprint (for the digest). The cutoff `k` ranges over
/// `0..=committed.len()`; `k = committed.len()` is the full timeline. See
/// [`run_engine_recover_faults_seed`].
///
/// # Panics
///
/// Panics if no committed prefix reproduces the recovered state — a silent
/// divergence, the failure the fault-recovery sweep exists to catch.
fn verify_recovered_prefix<C: Clock, D: Disk + Clone>(
    seed: u64,
    engine: &Engine<C, D>,
    oracle: &BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>>,
    committed: &[SystemTimeMicros],
    keys: &[BusinessKey],
) -> (usize, Vec<Option<Vec<u8>>>) {
    // Probe just before the first commit and at every commit boundary.
    let mut probes: Vec<SystemTimeMicros> = Vec::new();
    if let Some(first) = committed.first() {
        probes.push(SystemTimeMicros(first.0 - 1));
    }
    probes.extend(committed.iter().copied());

    // The recovered observable state, key by key at every probe.
    let mut recovered_fp: Vec<Option<Vec<u8>>> = Vec::new();
    for &s in &probes {
        for key in keys {
            recovered_fp.push(
                // `.flatten()`: no NULL payloads here, so a present row's payload
                // is always `Some` ([STL-154]).
                engine
                    .as_of_payload(key, Snapshot(s))
                    .expect("recovered as_of")
                    .flatten(),
            );
        }
    }

    // The reference oracle truncated at a cutoff instant, read at each probe. A
    // `None` cutoff is the pre-history empty state (`k = 0`).
    let expected = |cutoff: Option<SystemTimeMicros>| -> Vec<Option<Vec<u8>>> {
        let mut fp = Vec::new();
        for &s in &probes {
            for key in keys {
                fp.push(cutoff.and_then(|c| {
                    oracle
                        .get(key)
                        .and_then(|tl| tl.range(..=s.min(c)).next_back())
                        .and_then(|(_, payload)| payload.clone())
                }));
            }
        }
        fp
    };

    // Recovery applies a clean run of WAL records and stops at the first corrupt
    // one, so it can only land on a record boundary — the recovered state must
    // equal the oracle truncated at some committed prefix.
    let k = (0..=committed.len())
        .find(|&k| {
            let cutoff = (k != 0).then(|| committed[k - 1]);
            expected(cutoff) == recovered_fp
        })
        .unwrap_or_else(|| {
            panic!(
                "seed {seed}: recovered state is not a consistent committed prefix \
                 of the oracle — recovery silently diverged"
            )
        });
    (k, recovered_fp)
}

/// Kill and `recover` a seeded [`Engine`] workload **through a [`FaultDisk`]** ([STL-153]).
///
/// Recovery must either converge to a consistent committed prefix or cleanly detect
/// corruption — never silently diverge.
///
/// This is [`run_engine_recover_seed`] hardened against injected disk faults — the
/// second DoD bullet [STL-109] deferred to the recovery layer. The engine's single
/// shared disk is a [`FaultDisk`] with a **seed-derived [`FaultProfile`]**, armed in
/// two phases so each fault class lands where its survival contract is defined:
///
/// * **Workload phase** — torn-write, full-disk, and slow-fsync faults are armed.
///   The write-ahead order ([`stele_storage::dml`]) means a torn or failed WAL
///   append aborts the operation *before* the delta or validity index is touched,
///   so the reference oracle records only the operations that returned `Ok`; the
///   first failure is the crash. At this scale the tiers never spill, so the WAL
///   append is the only write-fault surface and any torn bytes land at the log tail.
/// * **Recovery phase** — bit-flip and short-read faults are armed instead, so every
///   read [`Engine::recover`] makes (the checkpoint scan and the WAL replay) is
///   subject to the silent corruption the WAL's CRC32C framing must catch.
///
/// Recovery's outcome is then asserted *sound*, two ways:
///
/// * `Err(_)` — a checksum caught corruption the durable fence vouched for and
///   recovery refused rather than serving it (the DoD's "detected as corruption"); or
/// * `Ok(engine)` — the recovered state is a genuine **committed prefix**: there is a
///   cutoff `k` for which every recovered `AS OF` read equals the reference oracle
///   truncated at the `k`-th commit. A corrupt or short read past the durable fence
///   is dropped as a torn tail, yielding an *earlier* consistent point; a clean
///   recovery yields the full timeline (`k = n`), matching the oracle at **every**
///   commit boundary exactly as the fault-free sweep does. A recovered value that
///   belongs to no prefix fails the membership check — the "never silently diverge"
///   guarantee.
///
/// The digest folds the recovered answers, the prefix cutoff, and the disk's
/// seed-keyed fault-event log, so the sweep regresses on the whole fault sequence.
/// Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics if recovery returns `Ok` with a state that is not a consistent committed
/// prefix of the reference oracle — a silent divergence, the exact failure this
/// scenario exists to catch.
#[must_use]
pub fn run_engine_recover_faults_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    // Seed-derived fault probabilities: a small spread so different seeds stress
    // different mixes, all reproducible from `seed`. Drawn from a stream distinct
    // from the workload's so the op sequence is independent of the profile.
    let mut prof_rng = Rng::new(seed ^ 0xFA17_D15C_0BAD_F00D);
    let p_torn = prob_permille(&mut prof_rng, 20, 60);
    let p_full = prob_permille(&mut prof_rng, 10, 30);
    let p_slow = prob_permille(&mut prof_rng, 200, 500);
    let p_bit = prob_permille(&mut prof_rng, 80, 200);
    let p_short = prob_permille(&mut prof_rng, 80, 200);

    let mut rng = Rng::new(seed);

    // The reference oracle: per key, the committed payload-or-delete timeline keyed
    // by commit `sys_from` — an independent model, recording only operations the
    // engine confirmed (returned `Ok`). `committed` is every commit instant in order.
    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    let mut committed: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 1 + rng.below_usize(6);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    // Open on a clean disk, *then* arm the write-path faults — a fault during open
    // would only produce a degenerate empty world, not exercise recovery. The disk
    // handle and the engine's internal clones share one fault state (and one seeded
    // stream), so enabling a class here perturbs the engine's writes.
    let disk = FaultDisk::new(seed, FaultProfile::none());
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open engine");
        disk.enable(FaultKind::TornWrite, p_torn);
        disk.enable(FaultKind::FullDisk, p_full);
        disk.enable(FaultKind::SlowSync, p_slow);

        let mut live = vec![false; key_count];
        let ops = 8 + rng.below(24);
        'workload: for op in 0..ops {
            let k = rng.below_usize(key_count);
            let key = keys[k].clone();
            let txn = TxnId(op);
            let principal = Principal::new(b"sim".to_vec());
            // A per-op-unique payload, so each operation changes its key's visible
            // value at its own commit boundary — making the per-prefix oracle
            // fingerprints distinct and the cutoff membership check unambiguous.
            let payload = format!("op{op}").into_bytes();
            let want_delete = live[k] && rng.below(2) == 0;
            let outcome = if live[k] {
                if want_delete {
                    engine.delete(&key, txn, principal)
                } else {
                    engine.update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
                }
            } else {
                engine.insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
            };
            match outcome {
                Ok(o) => {
                    let effect = if want_delete { None } else { Some(payload) };
                    oracle.entry(key).or_default().insert(o.commit, effect);
                    committed.push(o.commit);
                    live[k] = !want_delete;
                }
                // A torn or full-disk WAL append: the op aborted before touching the
                // delta or index (write-ahead order), so it never committed. The
                // torn bytes (if any) are the log tail. This is the crash point.
                Err(_) => break 'workload,
            }
            // A periodic durable checkpoint; a fault writing it is also a crash.
            if rng.below(3) == 0 && engine.checkpoint().is_err() {
                break 'workload;
            }
        }
        // Best-effort graceful-shutdown checkpoint, then drop the engine — the crash.
        let _ = engine.checkpoint();
    }

    // Re-arm: silence the write-path faults, arm read corruption for recovery so
    // every checkpoint/WAL read recovery makes is subject to a flip or short read.
    disk.disable(FaultKind::TornWrite);
    disk.disable(FaultKind::FullDisk);
    disk.disable(FaultKind::SlowSync);
    disk.enable(FaultKind::BitFlip, p_bit);
    disk.enable(FaultKind::ShortRead, p_short);

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), false);

    // Stop perturbing reads — the harness now inspects the recovered engine.
    disk.disable(FaultKind::BitFlip);
    disk.disable(FaultKind::ShortRead);

    let mut digest = FNV_OFFSET;
    match recovered {
        // Corruption the durable fence vouched for was caught by a checksum and
        // surfaced rather than served — a sound outcome.
        Err(_) => digest = fnv1a(digest, &[0xE2]),
        Ok(engine) => {
            // The recovered state must be a consistent committed prefix of the
            // oracle (else the harness panics — a silent divergence).
            let (k, recovered_fp) =
                verify_recovered_prefix(seed, &engine, &oracle, &committed, &keys);
            digest = fnv1a(digest, &[0x0C]);
            digest = fnv1a(
                digest,
                &u64::try_from(k).expect("prefix len fits u64").to_le_bytes(),
            );
            for entry in &recovered_fp {
                match entry {
                    Some(payload) => {
                        digest = fnv1a(digest, &[1]);
                        digest = fnv1a(digest, payload);
                    }
                    None => digest = fnv1a(digest, &[0]),
                }
            }
        }
    }

    // Fold the seed-keyed fault-event log so the digest regresses on the exact
    // injected fault sequence, not only the recovered state.
    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

/// Kill and `recover` a seeded [`Engine`] workload that periodically **flushes**,
/// **through a [`FaultDisk`]** — the crash-during-flush half of [STL-177]'s DoD.
///
/// This is [`run_engine_recover_faults_seed`] with the periodic durability point
/// upgraded to a [`Engine::flush`], so the injected faults now land on the flush's
/// extra write surfaces too: the sealed-segment append/`fsync` and the checkpoint
/// (manifest) record. A torn or full-disk fault mid-flush leaves a **segment whose
/// committing checkpoint record never became durable** — an orphan. Recovery must
/// ignore that orphan and fall back to the WAL, converging to a consistent
/// committed prefix (or cleanly detecting corruption), never silently diverging:
/// exactly "crash during a checkpoint recovers to a consistent state".
///
/// The two-phase fault arming, the committed-prefix membership check
/// (`verify_recovered_prefix`), and the fault-event digest fold are identical to
/// the fence-only sweep. Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics if recovery returns `Ok` with a state that is not a consistent committed
/// prefix of the reference oracle — a silent divergence.
#[must_use]
pub fn run_engine_flush_recover_faults_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    let mut prof_rng = Rng::new(seed ^ 0xFA17_D15C_0BAD_F00D);
    let p_torn = prob_permille(&mut prof_rng, 20, 60);
    let p_full = prob_permille(&mut prof_rng, 10, 30);
    let p_slow = prob_permille(&mut prof_rng, 200, 500);
    let p_bit = prob_permille(&mut prof_rng, 80, 200);
    let p_short = prob_permille(&mut prof_rng, 80, 200);

    let mut rng = Rng::new(seed);

    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    let mut committed: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 1 + rng.below_usize(6);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    let disk = FaultDisk::new(seed, FaultProfile::none());
    {
        // Seed a small per-seed row-group bound so a mid-flush fault can tear a
        // *multi*-row-group segment ([STL-197]) — the orphan / torn-flush recovery
        // paths must survive the finer split too ([STL-155]).
        let row_group_rows = 1 + rng.below_usize(3);
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false)
            .expect("open engine")
            .with_flush_row_group_rows(row_group_rows);
        disk.enable(FaultKind::TornWrite, p_torn);
        disk.enable(FaultKind::FullDisk, p_full);
        disk.enable(FaultKind::SlowSync, p_slow);

        let mut live = vec![false; key_count];
        let ops = 8 + rng.below(24);
        'workload: for op in 0..ops {
            let k = rng.below_usize(key_count);
            let key = keys[k].clone();
            let txn = TxnId(op);
            let principal = Principal::new(b"sim".to_vec());
            let payload = format!("op{op}").into_bytes();
            let want_delete = live[k] && rng.below(2) == 0;
            let outcome = if live[k] {
                if want_delete {
                    engine.delete(&key, txn, principal)
                } else {
                    engine.update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
                }
            } else {
                engine.insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
            };
            match outcome {
                Ok(o) => {
                    let effect = if want_delete { None } else { Some(payload) };
                    oracle.entry(key).or_default().insert(o.commit, effect);
                    committed.push(o.commit);
                    live[k] = !want_delete;
                }
                Err(_) => break 'workload,
            }
            // A periodic flush; a fault sealing the segment or writing its manifest
            // record is a crash mid-flush — the orphan-segment case recovery must
            // survive. The delta is left intact on a flush error, so the WAL stays
            // authoritative.
            if rng.below(3) == 0 && engine.flush().is_err() {
                break 'workload;
            }
        }
        // Best-effort graceful-shutdown flush, then drop the engine — the crash.
        let _ = engine.flush();
    }

    disk.disable(FaultKind::TornWrite);
    disk.disable(FaultKind::FullDisk);
    disk.disable(FaultKind::SlowSync);
    disk.enable(FaultKind::BitFlip, p_bit);
    disk.enable(FaultKind::ShortRead, p_short);

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), false);

    disk.disable(FaultKind::BitFlip);
    disk.disable(FaultKind::ShortRead);

    let mut digest = FNV_OFFSET;
    match recovered {
        Err(_) => digest = fnv1a(digest, &[0xE2]),
        Ok(engine) => {
            let (k, recovered_fp) =
                verify_recovered_prefix(seed, &engine, &oracle, &committed, &keys);
            digest = fnv1a(digest, &[0x0C]);
            digest = fnv1a(
                digest,
                &u64::try_from(k).expect("prefix len fits u64").to_le_bytes(),
            );
            for entry in &recovered_fp {
                match entry {
                    Some(payload) => {
                        digest = fnv1a(digest, &[1]);
                        digest = fnv1a(digest, payload);
                    }
                    None => digest = fnv1a(digest, &[0]),
                }
            }
        }
    }

    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

/// Apply one transaction of [`run_group_commit_recover_faults_seed`]: buffer a
/// distinct-key batch under [`Engine::begin_group`], then [`Engine::commit_group`].
///
/// On a durable commit, fold the batch's effects into `oracle`/`boundaries`/`live`
/// and return `true`; on a torn group commit or a staged-write fault, discard the
/// buffer and return `false` — the crash, after which the caller stops the workload.
/// Each write shares `txn` (the multi-statement commit's one transaction id).
#[allow(clippy::too_many_arguments)] // engine + rng + the oracle/live/boundary model + txn id
fn apply_group_txn(
    engine: &mut Engine<StepClock, FaultDisk>,
    rng: &mut Rng,
    keys: &[BusinessKey],
    live: &mut [bool],
    oracle: &mut BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>>,
    boundaries: &mut Vec<SystemTimeMicros>,
    txn: TxnId,
) -> bool {
    // A distinct-key subset (no intra-group same-key chaining), so each write is an
    // independent insert/update/delete.
    let mut pool: Vec<usize> = (0..keys.len()).collect();
    let take = (1 + rng.below_usize(4)).min(keys.len());
    for i in 0..take {
        let j = i + rng.below_usize(pool.len() - i);
        pool.swap(i, j);
    }
    let chosen = &pool[..take];

    // Buffer each write (no WAL record yet). At this scale resolution never touches
    // the disk, so the only workload fault surface is `commit_group` below.
    engine.begin_group();
    let mut staged: Vec<(BusinessKey, SystemTimeMicros, Option<Vec<u8>>, bool)> = Vec::new();
    for &k in chosen {
        let key = keys[k].clone();
        let principal = Principal::new(b"sim".to_vec());
        let payload = format!("t{}k{k}", txn.0).into_bytes();
        let want_delete = live[k] && rng.below(2) == 0;
        let outcome = if live[k] {
            if want_delete {
                engine.delete(&key, txn, principal)
            } else {
                engine.update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
            }
        } else {
            engine.insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
        };
        let Ok(o) = outcome else {
            engine.abort_group();
            return false;
        };
        staged.push((
            key,
            o.commit,
            if want_delete { None } else { Some(payload) },
            want_delete,
        ));
    }

    // One record + one fsync. A torn or full-disk fault tears the whole transaction.
    if engine.commit_group().is_err() {
        return false;
    }
    for (key, commit, effect, _) in &staged {
        oracle
            .entry(key.clone())
            .or_default()
            .insert(*commit, effect.clone());
    }
    for (i, &k) in chosen.iter().enumerate() {
        live[k] = !staged[i].3;
    }
    if let Some((_, last, _, _)) = staged.last() {
        boundaries.push(*last);
    }
    true
}

/// Kill and `recover` a seeded **multi-statement group-commit** workload through a
/// [`FaultDisk`] — the crash-atomic-commit DoD of [STL-192].
///
/// Each unit of work is a *transaction*: a small batch of writes to distinct keys,
/// applied under [`Engine::begin_group`] (so every write buffers, applying to the
/// delta/index but deferring its WAL record) and then made durable by a single
/// [`Engine::commit_group`] — one WAL record group-committed with one fsync. The
/// crash surface is therefore that one append/fsync: a torn or full-disk fault tears
/// the *whole* transaction at once.
///
/// The reference oracle records a transaction's effects **only when its
/// `commit_group` returned `Ok`** — an all-or-nothing unit — and `boundaries` holds
/// each committed transaction's last commit instant. Recovery must then converge to
/// a consistent **committed-transaction prefix**: every recovered `AS OF` equals the
/// oracle truncated at some transaction boundary. A recovery that surfaced a *partial*
/// transaction (some of its writes present, others not — the old per-write window)
/// would match no boundary and the membership check (`verify_recovered_prefix`)
/// would panic. That is the "never a partial prefix" guarantee, seed-reproducible.
///
/// The two-phase fault arming (write-path faults during the workload, read corruption
/// during recovery), the prefix membership check, and the fault-event digest fold are
/// the same as [`run_engine_recover_faults_seed`]. Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics if recovery returns `Ok` with a state that is not a consistent
/// committed-*transaction* prefix of the oracle — a partial commit survived a crash.
#[must_use]
pub fn run_group_commit_recover_faults_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    // A distinct profile stream from the workload's, so the op sequence is
    // independent of the fault mix. Torn/full skew a little higher than the
    // single-write sweep so a group commit actually gets torn at these scales.
    let mut prof_rng = Rng::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    let p_torn = prob_permille(&mut prof_rng, 40, 120);
    let p_full = prob_permille(&mut prof_rng, 10, 30);
    let p_slow = prob_permille(&mut prof_rng, 200, 500);
    let p_bit = prob_permille(&mut prof_rng, 80, 200);
    let p_short = prob_permille(&mut prof_rng, 80, 200);

    let mut rng = Rng::new(seed);

    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    // The last commit instant of each *durably committed* transaction — the only
    // cutoffs a consistent recovery may land on.
    let mut boundaries: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 2 + rng.below_usize(5);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    let disk = FaultDisk::new(seed, FaultProfile::none());
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open engine");
        disk.enable(FaultKind::TornWrite, p_torn);
        disk.enable(FaultKind::FullDisk, p_full);
        disk.enable(FaultKind::SlowSync, p_slow);

        let mut live = vec![false; key_count];
        let txns = 4 + rng.below(12);
        for t in 0..txns {
            // Each transaction shares one id across its writes (the multi-statement
            // commit semantic). `apply_group_txn` returns `false` on the crash — a
            // torn group commit or a staged-write fault — after which we stop.
            if !apply_group_txn(
                &mut engine,
                &mut rng,
                &keys,
                &mut live,
                &mut oracle,
                &mut boundaries,
                TxnId(t),
            ) {
                break;
            }
            // A periodic durable checkpoint advances the fence; a fault writing it is
            // also a crash.
            if rng.below(3) == 0 && engine.checkpoint().is_err() {
                break;
            }
        }
        let _ = engine.checkpoint();
    }

    // Re-arm for recovery: silence write faults, arm read corruption.
    disk.disable(FaultKind::TornWrite);
    disk.disable(FaultKind::FullDisk);
    disk.disable(FaultKind::SlowSync);
    disk.enable(FaultKind::BitFlip, p_bit);
    disk.enable(FaultKind::ShortRead, p_short);

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), false);

    disk.disable(FaultKind::BitFlip);
    disk.disable(FaultKind::ShortRead);

    let mut digest = FNV_OFFSET;
    match recovered {
        Err(_) => digest = fnv1a(digest, &[0xE2]),
        Ok(engine) => {
            // The recovered state must be a consistent committed-transaction prefix:
            // the cutoffs are transaction boundaries, so a partial transaction fails.
            let (k, recovered_fp) =
                verify_recovered_prefix(seed, &engine, &oracle, &boundaries, &keys);
            digest = fnv1a(digest, &[0x0C]);
            digest = fnv1a(
                digest,
                &u64::try_from(k).expect("prefix len fits u64").to_le_bytes(),
            );
            for entry in &recovered_fp {
                match entry {
                    Some(payload) => {
                        digest = fnv1a(digest, &[1]);
                        digest = fnv1a(digest, payload);
                    }
                    None => digest = fnv1a(digest, &[0]),
                }
            }
        }
    }

    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

/// Stage one multi-statement transaction and **roll it back** — the
/// `BEGIN … ROLLBACK` arm of [`run_txn_commit_rollback_faults_seed`].
///
/// The same seeded distinct-key batch shape as `apply_group_txn`, staged under
/// [`Engine::begin_group`] so every write applies to the resident delta/index
/// but defers its WAL record — then dropped by [`Engine::abort_group`] instead
/// of committed. The oracle is untouched: a rolled-back transaction has no
/// effects.
///
/// After the abort, every chosen key is read back live at the last staged
/// instant and asserted equal to the committed-only oracle — a rolled-back
/// write that stayed visible (a leaked resident application, the [STL-216]
/// regression) fails here with the seed in the message.
///
/// Returns `false` on a staged-write fault — the crash, after which the caller
/// stops the workload (the buffer is aborted first, mirroring `apply_group_txn`).
fn apply_rollback_group_txn(
    engine: &mut Engine<StepClock, FaultDisk>,
    rng: &mut Rng,
    keys: &[BusinessKey],
    live: &[bool],
    oracle: &BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>>,
    txn: TxnId,
    seed: u64,
) -> bool {
    // The same distinct-key subset drawing as the commit arm, so a seed's
    // batches keep their shape whichever way its commit/rollback coins land.
    let mut pool: Vec<usize> = (0..keys.len()).collect();
    let take = (1 + rng.below_usize(4)).min(keys.len());
    for i in 0..take {
        let j = i + rng.below_usize(pool.len() - i);
        pool.swap(i, j);
    }
    let chosen = &pool[..take];

    engine.begin_group();
    let mut last_staged: Option<SystemTimeMicros> = None;
    for &k in chosen {
        let principal = Principal::new(b"sim".to_vec());
        // A payload namespace disjoint from the committed arm's `t{txn}k{key}` —
        // if a rolled-back write ever leaks into a live read or a recovered
        // state, it can never alias a committed write, so the equality checks
        // catch it rather than coincidentally passing.
        let payload = format!("rb-t{}k{k}", txn.0).into_bytes();
        let want_delete = live[k] && rng.below(2) == 0;
        let outcome = if live[k] {
            if want_delete {
                engine.delete(&keys[k], txn, principal)
            } else {
                engine.update(keys[k].clone(), None, Some(payload), 0, txn, principal)
            }
        } else {
            engine.insert(keys[k].clone(), None, Some(payload), 0, txn, principal)
        };
        let Ok(o) = outcome else {
            engine.abort_group();
            return false;
        };
        last_staged = Some(o.commit);
    }

    // ROLLBACK: discard the buffered redos and undo their resident application
    // ([STL-216]) — no WAL record was or will be written for this transaction.
    engine.abort_group();

    // The live engine must show none of the rolled-back writes: every chosen key
    // reads back exactly the committed-only truth at the last staged instant.
    if let Some(s) = last_staged {
        for &k in chosen {
            let got = engine
                .as_of_payload(&keys[k], Snapshot(s))
                .expect("post-rollback as_of")
                .flatten();
            let want = oracle
                .get(&keys[k])
                .and_then(|tl| tl.range(..=s).next_back())
                .and_then(|(_, payload)| payload.clone());
            assert_eq!(
                got, want,
                "seed {seed}: txn {} key {k}: a rolled-back write is visible live",
                txn.0,
            );
        }
    }
    true
}

/// Kill and `recover` a seeded **mixed `BEGIN … COMMIT` / `BEGIN … ROLLBACK`**
/// workload through a [`FaultDisk`] — the multi-statement-transaction DST
/// coverage of [STL-187].
///
/// [`run_group_commit_recover_faults_seed`] proves a committed group is
/// all-or-nothing across a crash; [`run_group_commit_abort_rollback_seed`]
/// proves a fault-free rollback leaves no trace. This scenario closes the gap
/// between them: a seed-chosen interleaving of committed and rolled-back
/// transactions with write-path faults armed throughout — the shape a real
/// session produces, where a `ROLLBACK` lands between two durable commits.
///
/// Each rolled-back transaction stages its writes under [`Engine::begin_group`]
/// and drops them with [`Engine::abort_group`]; the live state is immediately
/// asserted clean (`apply_rollback_group_txn`). Committed transactions, the
/// two-phase fault arming, and the crash/recovery tail are exactly
/// [`run_group_commit_recover_faults_seed`]'s: the reference oracle records
/// committed transactions only, and recovery must converge to a consistent
/// committed-transaction prefix or cleanly detect corruption. Rolled-back
/// payloads live in a disjoint namespace (`rb-…`), so a rollback resurrected by
/// replay can never alias a committed write — it fails the membership check
/// loudly. Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics if a rolled-back write is visible live after its `ROLLBACK`, or if
/// recovery returns `Ok` with a state that is not a consistent
/// committed-transaction prefix of the oracle (e.g. a rolled-back write
/// resurrected by WAL replay).
#[must_use]
pub fn run_txn_commit_rollback_faults_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    // The group-commit sweep's fault mix, drawn from a stream of its own so the
    // commit/rollback interleaving is independent of the profile.
    let mut prof_rng = Rng::new(seed ^ 0x0DDC_0FFE_E0DD_F00D);
    let p_torn = prob_permille(&mut prof_rng, 40, 120);
    let p_full = prob_permille(&mut prof_rng, 10, 30);
    let p_slow = prob_permille(&mut prof_rng, 200, 500);
    let p_bit = prob_permille(&mut prof_rng, 80, 200);
    let p_short = prob_permille(&mut prof_rng, 80, 200);

    let mut rng = Rng::new(seed);

    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    let mut boundaries: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 2 + rng.below_usize(5);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    let disk = FaultDisk::new(seed, FaultProfile::none());
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open engine");
        disk.enable(FaultKind::TornWrite, p_torn);
        disk.enable(FaultKind::FullDisk, p_full);
        disk.enable(FaultKind::SlowSync, p_slow);

        let mut live = vec![false; key_count];
        let txns = 6 + rng.below(12);
        for t in 0..txns {
            // The commit/rollback coin: about a third of the transactions roll
            // back, so most seeds interleave both outcomes before the crash.
            let rollback = rng.below(3) == 0;
            let survived = if rollback {
                apply_rollback_group_txn(
                    &mut engine,
                    &mut rng,
                    &keys,
                    &live,
                    &oracle,
                    TxnId(t),
                    seed,
                )
            } else {
                apply_group_txn(
                    &mut engine,
                    &mut rng,
                    &keys,
                    &mut live,
                    &mut oracle,
                    &mut boundaries,
                    TxnId(t),
                )
            };
            if !survived {
                break;
            }
            // A periodic durable checkpoint advances the fence; a fault writing
            // it is also a crash.
            if rng.below(3) == 0 && engine.checkpoint().is_err() {
                break;
            }
        }
        let _ = engine.checkpoint();
    }

    // Re-arm for recovery: silence write faults, arm read corruption.
    disk.disable(FaultKind::TornWrite);
    disk.disable(FaultKind::FullDisk);
    disk.disable(FaultKind::SlowSync);
    disk.enable(FaultKind::BitFlip, p_bit);
    disk.enable(FaultKind::ShortRead, p_short);

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), false);

    disk.disable(FaultKind::BitFlip);
    disk.disable(FaultKind::ShortRead);

    let mut digest = FNV_OFFSET;
    match recovered {
        Err(_) => digest = fnv1a(digest, &[0xE2]),
        Ok(engine) => {
            // The recovered state must be a consistent committed-transaction
            // prefix; a leaked rollback or a partial commit matches no cutoff.
            let (k, recovered_fp) =
                verify_recovered_prefix(seed, &engine, &oracle, &boundaries, &keys);
            digest = fnv1a(digest, &[0x0C]);
            digest = fnv1a(
                digest,
                &u64::try_from(k).expect("prefix len fits u64").to_le_bytes(),
            );
            for entry in &recovered_fp {
                match entry {
                    Some(payload) => {
                        digest = fnv1a(digest, &[1]);
                        digest = fnv1a(digest, payload);
                    }
                    None => digest = fnv1a(digest, &[0]),
                }
            }
        }
    }

    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

/// Drive a seeded [`Engine`] workload into a **failed WAL fsync** and assert the
/// WAL/engine **poisons** — the [STL-217] DoD, seed-reproducibly.
///
/// The WAL durability contract is "append then fsync; the fsync is the only
/// durability point." If an `append` succeeds but its fsync ([`Wal::tick`]) fails,
/// the staged record's durability is *indeterminate* — a later successful `tick`
/// (a [`checkpoint`](stele_storage::engine::Engine::checkpoint), `flush`, or
/// another commit) could otherwise flush it under the guise of an aborted op. The
/// standard response is to treat the failed fsync as a *crash*: poison the WAL so
/// it refuses every further write until recovery.
///
/// This scenario isolates that path. A handful of auto-commit writes append to the
/// WAL (append-only; not yet fsynced), then a [`checkpoint`](stele_storage::engine::Engine::checkpoint)
/// drives the group-commit fsync through a [`FaultDisk`] whose only armed class is
/// [`FailSync`](FaultKind::FailSync). The checkpoint's fsync fails, so the harness
/// asserts:
///
/// * the engine is [`poisoned`](stele_storage::engine::Engine::is_poisoned);
/// * a subsequent write is **refused** (the poison stops the same instance from
///   serving — so the staged record can never be flushed by a later `tick`); and
/// * a subsequent checkpoint is refused too.
///
/// Then [`FailSync`](FaultKind::FailSync) is disabled and [`Engine::recover`] opens
/// a fresh, unpoisoned WAL and replays the durable log. Recovery's outcome is asserted
/// **sound** via `verify_recovered_prefix`: the recovered state is a consistent
/// committed prefix of the reference oracle (here the full timeline of confirmed
/// writes — none was torn). The digest folds the recovered answers and the disk's
/// seed-keyed fault-event log. Same seed ⇒ same digest.
///
/// # Panics
///
/// Panics if a failed fsync does *not* poison the engine, if a poisoned engine
/// accepts a write or checkpoint, or if recovery returns a state that is not a
/// consistent committed prefix of the oracle.
#[must_use]
pub fn run_wal_fsync_poison_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    let mut rng = Rng::new(seed);

    let mut oracle: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>> =
        BTreeMap::new();
    let mut committed: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 1 + rng.below_usize(5);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();

    // The only armed class is FailSync: the failure is *exactly* a failed fsync, so
    // the path under test is isolated from torn writes / full disks / read rot.
    let disk = FaultDisk::new(seed, FaultProfile::none());
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open engine");

        // A short prelude of auto-commit writes. These append to the WAL but do not
        // fsync (the engine ticks only at checkpoint/flush/group-commit), so they all
        // succeed and become the confirmed timeline the oracle records.
        let mut live = vec![false; key_count];
        let prelude = 1 + rng.below(6);
        for op in 0..prelude {
            let k = rng.below_usize(key_count);
            let key = keys[k].clone();
            let txn = TxnId(op);
            let principal = Principal::new(b"sim".to_vec());
            let payload = format!("op{op}").into_bytes();
            let outcome = if live[k] {
                engine.update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
            } else {
                engine.insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
            };
            let o = outcome.expect("an auto-commit write does not fsync, so it cannot fail here");
            oracle
                .entry(key)
                .or_default()
                .insert(o.commit, Some(payload));
            committed.push(o.commit);
            live[k] = true;
        }

        // Now arm the failed fsync and drive the group-commit fsync via a checkpoint.
        disk.enable(FaultKind::FailSync, 1.0);
        assert!(
            engine.checkpoint().is_err(),
            "seed {seed}: the failed fsync must fail the checkpoint",
        );

        // The heart of STL-217: a failed fsync is a crash, so the engine poisons and
        // refuses to keep serving — a write reported failed can never be flushed by a
        // later tick on this instance.
        assert!(
            engine.is_poisoned(),
            "seed {seed}: a failed fsync must poison the engine",
        );
        let fresh = BusinessKey::new(b"after-poison".to_vec());
        assert!(
            engine
                .insert(
                    fresh,
                    None,
                    Some(b"x".to_vec()),
                    0,
                    TxnId(u64::MAX),
                    Principal::new(b"sim".to_vec())
                )
                .is_err(),
            "seed {seed}: a poisoned engine must refuse further writes",
        );
        assert!(
            engine.checkpoint().is_err(),
            "seed {seed}: a poisoned engine must refuse further checkpoints",
        );
    }

    // Recover into a fresh, unpoisoned WAL with the fault silenced. Recovery is
    // read-only, so it never fsyncs — but disable the class anyway to keep the
    // recovery path clean and the assertion unambiguous.
    disk.disable(FaultKind::FailSync);
    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), false)
        .expect("recover after a poisoned engine must succeed");
    assert!(
        !recovered.is_poisoned(),
        "seed {seed}: recovery opens a fresh, unpoisoned WAL",
    );

    // The recovered state must be a consistent committed prefix of the oracle. No
    // record was torn (FailSync fails the fsync, it does not shear the append), so
    // every confirmed write recovers — the full timeline.
    let (k, recovered_fp) = verify_recovered_prefix(seed, &recovered, &oracle, &committed, &keys);
    assert_eq!(
        k,
        committed.len(),
        "seed {seed}: a failed fsync tears nothing, so recovery is the full timeline",
    );

    let mut digest = FNV_OFFSET;
    digest = fnv1a(
        digest,
        &u64::try_from(k).expect("prefix len fits u64").to_le_bytes(),
    );
    for entry in &recovered_fp {
        match entry {
            Some(payload) => {
                digest = fnv1a(digest, &[1]);
                digest = fnv1a(digest, payload);
            }
            None => digest = fnv1a(digest, &[0]),
        }
    }
    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

/// In-memory rollback of a group commit that fails partway ([STL-216]).
///
/// A multi-statement transaction applies its writes front-to-back into the live
/// delta/index, then a later write fails (here a duplicate-key `INSERT`), so the
/// transaction aborts. The live engine must then show **none** of the
/// transaction's writes — exactly the committed baseline — identical to what a
/// restart reconstructs from the log (the aborted group logged no record).
///
/// The seed varies the baseline, the successful prefix (updates/deletes of live
/// keys plus inserts of brand-new keys), and the chosen victim key, so the abort
/// rolls back every redo flavour. The property checked is the triple equality
/// `live-before-group == live-after-abort == recovered`, read by `as_of` at a
/// fixed instant — the recovery-equivalence the rollback owes, with no disk fault
/// involved (a fsync fault is a crash, not a clean abort — [STL-217]).
///
/// # Panics
///
/// Panics (seed in the message) if any of the three observable states diverge —
/// the live engine kept an aborted transaction's writes, or recovery disagreed.
#[must_use]
#[allow(clippy::too_many_lines)] // baseline + chained txn + abort + recover reads as one scenario
pub fn run_group_commit_abort_rollback_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    let mut rng = Rng::new(seed);
    let principal = Principal::new(b"sim".to_vec());

    // A handful of baseline keys, all inserted live, plus a disjoint pool of
    // brand-new keys the transaction inserts (and the abort must roll back).
    let base_count = 2 + rng.below_usize(4);
    let base_keys: Vec<BusinessKey> = (0..base_count)
        .map(|i| BusinessKey::new(format!("base-{i:04}").into_bytes()))
        .collect();
    let new_count = 1 + rng.below_usize(3);
    let new_keys: Vec<BusinessKey> = (0..new_count)
        .map(|i| BusinessKey::new(format!("new-{i:04}").into_bytes()))
        .collect();
    // Every key the fingerprint observes, baseline then brand-new.
    let all_keys: Vec<BusinessKey> = base_keys.iter().chain(&new_keys).cloned().collect();

    let disk = FaultDisk::new(seed, FaultProfile::none());
    let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open engine");

    // Commit the baseline (auto-commit, one record each), tracking the latest
    // commit instant so the fingerprint reads the live tail of every chain.
    let mut live_at = SystemTimeMicros(0);
    for key in &base_keys {
        let payload = format!("base-v-{}", String::from_utf8_lossy(key.as_bytes())).into_bytes();
        let outcome = engine
            .insert(
                key.clone(),
                None,
                Some(payload),
                0,
                TxnId(1),
                principal.clone(),
            )
            .expect("baseline insert");
        live_at = live_at.max(outcome.commit);
    }
    engine.checkpoint().expect("baseline checkpoint");

    // Read at a fixed instant past the baseline — the version live at `probe` for
    // every key. `base-0` is reserved as the doomed victim and never touched by the
    // successful prefix, so it stays live for the duplicate-key INSERT below.
    let probe = Snapshot(live_at);
    let fingerprint = |engine: &Engine<StepClock, FaultDisk>| -> Vec<Option<Vec<u8>>> {
        all_keys
            .iter()
            .map(|key| engine.as_of_payload(key, probe).expect("as_of").flatten())
            .collect()
    };
    let baseline_fp = fingerprint(&engine);

    // A multi-statement transaction: a successful prefix over `base-1..` and the
    // brand-new keys, then a duplicate-key INSERT of the still-live `base-0` that
    // fails at apply time and aborts the whole COMMIT.
    engine.begin_group();
    let txn = TxnId(2);
    for key in &base_keys[1..] {
        let payload = format!("txn-v-{}", String::from_utf8_lossy(key.as_bytes())).into_bytes();
        let res = if rng.below(2) == 0 {
            engine.delete(key, txn, principal.clone())
        } else {
            engine.update(key.clone(), None, Some(payload), 0, txn, principal.clone())
        };
        res.expect("successful update/delete of a live key");
    }
    for key in &new_keys {
        engine
            .insert(
                key.clone(),
                None,
                Some(b"txn-new".to_vec()),
                0,
                txn,
                principal.clone(),
            )
            .expect("successful insert of a brand-new key");
    }
    // The doomed write: `base-0` is live, so re-inserting it is a duplicate key.
    assert!(
        engine
            .insert(
                base_keys[0].clone(),
                None,
                Some(b"dup".to_vec()),
                0,
                txn,
                principal
            )
            .is_err(),
        "seed {seed}: re-inserting a live key must fail the COMMIT",
    );
    engine.abort_group();

    // The live engine shows none of the aborted transaction's writes.
    let after_abort_fp = fingerprint(&engine);
    assert_eq!(
        after_abort_fp, baseline_fp,
        "seed {seed}: a failed COMMIT left writes visible in the live engine",
    );

    // … and that matches a fresh recovery from the same disk (the aborted group
    // logged no record, so recovery reconstructs the baseline alone).
    drop(engine);
    let recovered =
        Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover after abort");
    let recovered_fp = fingerprint(&recovered);
    assert_eq!(
        recovered_fp, baseline_fp,
        "seed {seed}: recovery diverged from the rolled-back live state",
    );

    let mut digest = FNV_OFFSET;
    for entry in &baseline_fp {
        match entry {
            Some(payload) => {
                digest = fnv1a(digest, &[1]);
                digest = fnv1a(digest, payload);
            }
            None => digest = fnv1a(digest, &[0]),
        }
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
                seq: open.seq,
                sys_to: commit,
                closed_by: Provenance::new(txn_id, commit, Principal::new(b"sim".to_vec())),
            })
            .expect("close prior version");
    }
    delta
        .insert(Version::open(
            key.clone(),
            commit,
            0,
            Provenance::new(txn_id, commit, Principal::new(b"sim".to_vec())),
            Some(format!("v@{}", commit.0).into_bytes()),
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
                    // Fold the per-commit `seq` (ADR-0024) so the seed sweep
                    // regresses on its deterministic, total-order assignment too.
                    digest = fnv1a(digest, &committed.seq.to_le_bytes());
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
                // Presence-tagged so a NULL payload never hashes like an empty one
                // ([STL-154]).
                digest = fold_optional_payload(digest, v.payload.as_deref());
            }
            None => digest = fnv1a(digest, &[0]),
        }
    }
    // Fold the final head of the hash-chained commit log (ADR-0026): the seed
    // sweep now also regresses on the chain being byte-identical across runs.
    digest = fnv1a(digest, mgr.commit_head().as_bytes());
    digest
}

/// Recover a hash-chained commit log under the sim and prove tamper-evidence.
///
/// Drives a seeded run of commits through the real [`TxnManager`], **recovers**
/// the commit log, and proves tamper-evidence is load-bearing on the restart path
/// ([STL-178], [ADR-0026], architecture invariant 10).
///
/// Each seed commits a run of transactions, then "crashes" (drops the manager,
/// keeping only the durable WAL) and reopens it through
/// [`TxnManager::recover`](stele_txn::TxnManager::recover). The clean log must
/// recover to the **same** chain head the crashed manager held, and the recovered
/// manager must continue the chain — the next commit's `seq` follows on and the
/// whole reopened log re-verifies. Then a seed-chosen historical frame is forged
/// (re-encoded, so it is well-formed) and [`verify_chain`]
/// must catch the broken link at the *following* record — recovery fails closed
/// rather than serving a silently-rewritten log. The digest folds the commit
/// sequence, the recovered head, and the detected tamper position, so the seed
/// sweep regresses on chain determinism *and* tamper detection together. Same
/// seed ⇒ same digest.
///
/// [STL-178]: https://allegromusic.atlassian.net/browse/STL-178
/// [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
#[must_use]
pub fn run_chain_recovery_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let disk = MemDisk::new();
    let wal = Wal::open(disk.clone(), WalConfig::default()).expect("open wal");
    let mgr = TxnManager::new(StepClock::new(1), wal);

    // A seeded run of commits builds a real hash-chained commit log.
    let commits = 3 + rng.below(13);
    let mut digest = FNV_OFFSET;
    for i in 0..commits {
        let mut t = mgr.begin();
        t.write(BusinessKey::new(format!("k-{i:04}").into_bytes()));
        let c = mgr.commit(t).expect("commit");
        digest = fnv1a(digest, &c.seq.to_le_bytes());
    }
    let clean_head = mgr.commit_head();
    drop(mgr); // crash: drop the in-memory state, keep the durable WAL.

    // Recovery re-verifies the chain from genesis and rebuilds the head; a clean
    // log recovers to exactly the pre-crash head (fails closed otherwise).
    let reopened = Wal::open(disk.clone(), WalConfig::default()).expect("reopen wal");
    let recovered = TxnManager::recover(StepClock::new(1), reopened).expect("clean recovery");
    assert_eq!(
        recovered.commit_head(),
        clean_head,
        "seed {seed}: recovered head must match the pre-crash head"
    );
    digest = fnv1a(digest, recovered.commit_head().as_bytes());

    // The recovered manager continues the chain: the next commit resumes one past
    // the recovered log and links onto the recovered head.
    let mut extra = recovered.begin();
    extra.write(BusinessKey::new(b"post-recovery".to_vec()));
    let ec = recovered.commit(extra).expect("post-recovery commit");
    assert_eq!(
        ec.seq,
        commits + 1,
        "seed {seed}: seq must resume one past the recovered log"
    );
    digest = fnv1a(digest, &ec.seq.to_le_bytes());

    // Tamper-evidence: forge a seed-chosen historical frame and confirm the chain
    // catches it. The whole log (recovered records + the new one) is read back;
    // a non-tail frame is chosen so a successor exists to break the link.
    let frames: Vec<Vec<u8>> = Wal::open(disk, WalConfig::default())
        .expect("reopen for tamper")
        .replay_from(Checkpoint::BEGIN)
        .map(|r| r.expect("frame"))
        .collect();
    let victim = rng.below_usize(frames.len() - 1);
    let mut forged = frames;
    forged[victim][8] ^= 0x01; // flip a byte of record `victim`'s commit_ts.
    let err = verify_chain(forged.into_iter().map(Ok)).expect_err("tamper must be detected");
    match err {
        ChainError::BrokenLink { index, .. } => {
            assert_eq!(
                usize::try_from(index).expect("record index fits in usize"),
                victim + 1,
                "seed {seed}: the break surfaces at the tampered record's successor"
            );
            digest = fnv1a(digest, &index.to_le_bytes());
        }
        other => panic!("seed {seed}: expected a broken link, got {other:?}"),
    }
    digest
}

/// Play a seeded workload against a [`FaultDisk`] and fold every operation
/// outcome and the disk's seed-keyed fault-event log into a digest ([STL-109]).
///
/// The disk runs a profile that arms **every** fault class — full disk, torn
/// write, short read, bit flip, slow sync — at probabilities high enough that a
/// bounded workload trips each, while the seed alone decides *which* operations
/// are hit. The workload appends seeded payloads, syncs, and reads back at
/// seeded offsets; each result (and its error kind, if any) plus the structured
/// fault log is mixed into the digest. Same seed ⇒ identical fault sequence ⇒
/// identical digest — the determinism contract of the whole harness, now
/// covering the disk-fault model ([docs/06 §5], [ADR-0010]).
///
/// [docs/06 §5]: ../../../docs/06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece
/// [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md
#[must_use]
pub fn run_fault_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    // Arm every class. Probabilities are tuned so a bounded workload exercises
    // each fault while the seed drives which ops are hit; the digest then
    // regresses on every fault class together.
    let profile = FaultProfile::none()
        .with_full_disk(0.03)
        .with_torn_write(0.15)
        .with_short_read(0.25)
        .with_bit_flip(0.20)
        .with_slow_sync(0.40)
        .with_max_slow_ticks(64);
    let disk = FaultDisk::new(seed, profile);

    // A workload RNG independent of the disk's internal fault stream.
    let mut rng = Rng::new(seed);
    // `create` can trip the full-disk fault — retry a *bounded* number of times
    // so a pathological seed/profile can never hang the harness. Each attempt is
    // deterministic, and the budget is far above any plausible full-disk streak.
    let mut file = (0..64)
        .find_map(|_| disk.create("wal").ok())
        .unwrap_or_else(|| panic!("seed {seed}: create kept hitting the full-disk fault"));

    let mut digest = FNV_OFFSET;
    let ops = 48 + rng.below_usize(48);
    for _ in 0..ops {
        match rng.below(4) {
            0 => {
                // sync — outcome (and any slow-sync log entry) folded.
                digest = fold_io(digest, b'S', file.sync().map(|()| 0));
            }
            1 => {
                // read at a seeded offset — fold the returned bytes too.
                let len = file.len();
                let offset = if len == 0 { 0 } else { rng.below(len) };
                let mut buf = vec![0u8; 1 + rng.below_usize(32)];
                match file.read_at(offset, &mut buf) {
                    Ok(n) => {
                        digest = fnv1a(digest, &[b'R', 1]);
                        digest = fnv1a(digest, &buf[..n]);
                    }
                    Err(e) => digest = fold_err(digest, b'R', &e),
                }
            }
            _ => {
                // append a seeded payload — outcome folded (torn/full disk).
                let payload_len = 1 + rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                digest = fold_io(digest, b'A', file.append(&payload).map(|()| 0));
            }
        }
    }

    // Fold the structured fault log — the seed-keyed sequence the DoD pins down.
    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

/// Fold a `usize`-or-error operation outcome into the digest under a `label`
/// (which op), so the success/failure *pattern* is part of the seed's identity.
fn fold_io(digest: u64, label: u8, result: io::Result<usize>) -> u64 {
    match result {
        Ok(n) => {
            let digest = fnv1a(digest, &[label, 1]);
            fnv1a(digest, &(n as u64).to_le_bytes())
        }
        Err(e) => fold_err(digest, label, &e),
    }
}

/// Fold an I/O error (its `label` and [`ErrorKind`](io::ErrorKind)) into the
/// digest — the failures the DoD requires to recur identically per seed.
fn fold_err(digest: u64, label: u8, err: &io::Error) -> u64 {
    let digest = fnv1a(digest, &[label, 0]);
    fnv1a(digest, &[err_kind_tag(err.kind())])
}

/// A stable tag for the I/O error kinds the fault model and the `Disk` contract
/// produce, so the digest does not depend on [`io::ErrorKind`]'s `Debug` text
/// (which is not a stability guarantee) and allocates nothing. An unexpected
/// kind folds to `255` — distinct, so a new failure mode still shifts the
/// digest rather than colliding with a known one.
const fn err_kind_tag(kind: io::ErrorKind) -> u8 {
    match kind {
        io::ErrorKind::StorageFull => 1,
        io::ErrorKind::WriteZero => 2,
        io::ErrorKind::NotFound => 3,
        io::ErrorKind::AlreadyExists => 4,
        io::ErrorKind::InvalidInput => 5,
        io::ErrorKind::UnexpectedEof => 6,
        _ => 255,
    }
}

/// A stable tag byte for a [`FaultOp`](stele_storage::backend::FaultOp), so the
/// folded fault log is order- and identity-sensitive without depending on the
/// enum's `Debug` text.
pub(crate) const fn fault_op_tag(op: stele_storage::backend::FaultOp) -> u8 {
    use stele_storage::backend::FaultOp;
    match op {
        FaultOp::Create => 0,
        FaultOp::Open => 1,
        FaultOp::Append => 2,
        FaultOp::ReadAt => 3,
        FaultOp::Sync => 4,
        FaultOp::List => 5,
        FaultOp::Remove => 6,
    }
}

/// A stable tag byte for a [`FaultKind`].
pub(crate) const fn fault_kind_tag(kind: FaultKind) -> u8 {
    match kind {
        FaultKind::BitFlip => 0,
        FaultKind::ShortRead => 1,
        FaultKind::TornWrite => 2,
        FaultKind::SlowSync => 3,
        FaultKind::FullDisk => 4,
        FaultKind::FailSync => 5,
    }
}

/// Collapse a [`SnapshotScan`] result projecting `[BusinessKey, Payload]` into a
/// `{key → payload}` map — the shape the reference oracle also produces, so the
/// two compare directly.
fn scan_map(out: &stele_exec::ScanOutput) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let column = |col: ColumnId| {
        out.batch
            .columns
            .iter()
            .find(|(c, _)| *c == col)
            .map(|(_, d)| d)
    };
    let (Some(Column::Bytes(keys)), Some(Column::Bytes(payloads))) =
        (column(ColumnId::BusinessKey), column(ColumnId::Payload))
    else {
        panic!("scan must project BusinessKey and Payload as bytes columns");
    };
    // This scenario never writes a SQL NULL, so every projected cell is present;
    // a `None` would be a write-path bug, surfaced loudly here ([STL-154]).
    keys.iter()
        .cloned()
        .zip(payloads.iter().cloned())
        .map(|(k, p)| {
            (
                k.expect("business key is never NULL"),
                p.expect("this scenario writes no NULL payload"),
            )
        })
        .collect()
}

/// The reference oracle: per key, the committed timeline of payload-or-delete,
/// keyed by the commit `sys_from`. Tier-agnostic — it models *logical* state, so
/// it is an independent check on the merged read path, not a mirror of it.
type ScanOracle = BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Option<Vec<u8>>>>;

/// Apply one seeded DML op through [`DmlWriter`], recording the same logical
/// effect into the reference `oracle`. `sealed` is built from every segment
/// flushed so far so the writer's liveness resolution is correct across flush
/// boundaries ([STL-140]). Returns `(commit, new_live)` for the key.
#[allow(clippy::too_many_arguments)] // tier handles + sealed + oracle + key/op/state
fn apply_scan_op(
    writer: &mut DmlWriter<StepClock, MemDisk>,
    delta: &mut Delta<MemDisk>,
    index: &mut ValidityIndex<MemDisk>,
    readers: &[SegmentReader<MemFile>],
    oracle: &mut ScanOracle,
    key: BusinessKey,
    op: u64,
    is_live: bool,
    is_delete: bool,
) -> (SystemTimeMicros, bool) {
    let txn = TxnId(op);
    let who = Principal::new(b"sim".to_vec());
    let sealed = SealedSegments::new(readers);
    if is_live && is_delete {
        let c = writer
            .delete(delta, index, &sealed, &key, txn, who)
            .expect("delete")
            .commit;
        oracle.entry(key).or_default().insert(c, None);
        return (c, false);
    }
    let payload = format!("v{op}").into_bytes();
    let c = if is_live {
        writer
            .update(
                delta,
                index,
                &sealed,
                key.clone(),
                None,
                Some(payload.clone()),
                0,
                txn,
                who,
            )
            .expect("update")
            .commit
    } else {
        writer
            .insert(
                delta,
                index,
                &sealed,
                key.clone(),
                None,
                Some(payload.clone()),
                0,
                txn,
                who,
            )
            .expect("insert")
            .commit
    };
    oracle.entry(key).or_default().insert(c, Some(payload));
    (c, true)
}

/// Flush the resident delta into a fresh sealed segment and append its reader —
/// the columnar flush boundary the scan must merge across. `max_row_group_rows`
/// bounds the segment's row-groups, so seeds sweep multi-row-group segments
/// through the row-group-scoped late-materialization path (STL-155) as well as
/// the classic single-group shape.
fn flush_into_segment(
    seg_disk: &MemDisk,
    name: &str,
    delta: &mut Delta<MemDisk>,
    readers: &mut Vec<SegmentReader<MemFile>>,
    max_row_group_rows: usize,
) {
    let rows = delta.flush_to_segment().expect("flush");
    let mut w = SegmentWriter::create(seg_disk, name)
        .expect("create segment")
        .with_max_row_group_rows(max_row_group_rows);
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    readers.push(SegmentReader::open(seg_disk, name).expect("open"));
}

/// Scan at one snapshot and assert it against the oracle and the prune invariant,
/// folding the agreed answer into `digest`.
fn verify_scan_at(
    seed: u64,
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    readers: &[SegmentReader<MemFile>],
    oracle: &ScanOracle,
    s: SystemTimeMicros,
    digest: u64,
) -> u64 {
    let snapshot = Snapshot(s);
    let out = SnapshotScan::new(delta, index, readers, snapshot)
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .execute()
        .expect("snapshot scan");

    // Prune invariant (STL-100 DoD bullet 2, refined by STL-146). The zone-map
    // prune is an independent function of the readers, so assert it directly. The
    // validity-index prune (STL-139) then carves the zone survivors into the
    // segments actually scanned and those proven fully superseded at the
    // snapshot; assert the accounting partitions the segments exactly. Soundness
    // of the superseded prune — that it never drops a segment holding a live row
    // — is caught by the oracle equivalence below, which would diverge if a live
    // row went missing.
    let zone_survivors = readers
        .iter()
        .filter(|r| r.might_contain(&Predicate::All, snapshot))
        .count();
    assert_eq!(
        out.stats.segments_pruned_zone,
        readers.len() - zone_survivors,
        "seed {seed}: zone-map prune count must match the independent zone test at {s:?}",
    );
    assert_eq!(
        out.stats.segments_scanned + out.stats.segments_pruned_superseded,
        zone_survivors,
        "seed {seed}: scanned + superseded-pruned must equal the zone survivors at {s:?}",
    );
    assert_eq!(out.stats.segments_total, readers.len());
    assert_eq!(
        out.stats.segments_pruned(),
        readers.len() - out.stats.segments_scanned,
    );

    // Row-group prune partition (STL-173). The per-row-group zone prune carves
    // each zone-survivor segment's row-groups into those read for identity and
    // those skipped; the counts must cover exactly the survivors' row-groups.
    // Seeds that bound `max_row_group_rows` make these multi-row-group, so an
    // early snapshot prunes whole row-groups on `sys_from` ("begins after S").
    // Soundness — a skipped row-group never held a live row — rides the oracle
    // equivalence below: a wrongly pruned row-group would drop a key and diverge.
    let rg_survivor_total: usize = readers
        .iter()
        .filter(|r| r.might_contain(&Predicate::All, snapshot))
        .map(|r| r.row_group_row_counts().len())
        .sum();
    assert_eq!(
        out.stats.row_groups_total, rg_survivor_total,
        "seed {seed}: row_groups_total must cover the zone survivors' row-groups at {s:?}",
    );
    assert_eq!(
        out.stats.row_groups_pruned_zone + out.stats.row_groups_scanned,
        out.stats.row_groups_total,
        "seed {seed}: the row-group counts must partition exactly at {s:?}",
    );

    // Oracle equivalence: the merged scan equals the tier-agnostic reference.
    let got = scan_map(&out);
    let mut want: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (key, timeline) in oracle {
        if let Some((_, Some(payload))) = timeline.range(..=s).next_back() {
            want.insert(key.as_bytes().to_vec(), payload.clone());
        }
    }
    assert_eq!(
        got, want,
        "seed {seed}: snapshot scan disagrees with the reference oracle at {s:?}",
    );

    let mut digest = digest;
    for (key, payload) in &want {
        digest = fnv1a(digest, key);
        digest = fnv1a(digest, payload);
    }
    let reads = u64::try_from(out.stats.segments_scanned).expect("read count fits u64");
    fnv1a(digest, &reads.to_le_bytes())
}

/// Read a seeded, flush-interleaved workload back through [`SnapshotScan`].
///
/// Plays insert/update/delete with periodic flushes to sealed segments, then
/// checks every snapshot against an independent in-memory reference oracle.
/// This is the AS-OF correctness oracle the read path requires
/// ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart),
/// STL-100): the reference oracle models each key's history with no
/// knowledge of tiers, so "the value of key `k` at `S`" is its latest event at or
/// before `S`. The scan — merging the delta tier, however many sealed segments
/// the seed flushed, and the validity index across those flush boundaries — must
/// agree at every probe, and the prune accounting must partition the segments
/// across the zone-map prune, the validity-index "all superseded" prune
/// (STL-139/146), and the survivors actually scanned. The returned digest folds
/// every agreed `(snapshot, key, payload)` and scan count. Same seed ⇒ same
/// digest.
///
/// # Panics
///
/// Panics if the scan disagrees with the reference oracle, or if the prune
/// accounting does not partition the segments as expected — correctness
/// regressions, not workload outcomes.
#[must_use]
pub fn run_snapshot_scan_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let seg_disk = MemDisk::new();
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let mut writer = DmlWriter::new(wal, StepClock::new(1), false);
    let mut readers: Vec<SegmentReader<MemFile>> = Vec::new();
    let mut oracle: ScanOracle = BTreeMap::new();
    let mut seg_idx = 0u32;

    let key_count = 1 + rng.below_usize(5);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();
    let mut live = vec![false; key_count];
    let mut commits: Vec<SystemTimeMicros> = Vec::new();

    let ops = 12 + rng.below(36);
    for op in 0..ops {
        let k = rng.below_usize(key_count);
        // Drawn only when the key is live, so the rng stream is independent of the
        // (later) insert path — keeping every seed's op sequence deterministic.
        let is_delete = live[k] && rng.below(4) == 0;
        let (commit, new_live) = apply_scan_op(
            &mut writer,
            &mut delta,
            &mut index,
            &readers,
            &mut oracle,
            keys[k].clone(),
            op,
            live[k],
            is_delete,
        );
        live[k] = new_live;
        commits.push(commit);

        if rng.below(3) == 0 && delta.byte_size() > 0 {
            let name = format!("seg-{seg_idx:04}.seg");
            seg_idx += 1;
            // Seeded row-group bound (1–3 rows per group): most flushes split
            // into several row-groups, so the oracle equivalence below also
            // proves the row-group-scoped late materialization (STL-155)
            // returns exactly what the unscoped read did.
            let row_group_rows = 1 + rng.below_usize(3);
            flush_into_segment(&seg_disk, &name, &mut delta, &mut readers, row_group_rows);
        }
    }

    // Probe before any write, at every commit boundary, and just past the last —
    // exercising the oracle equivalence across the whole timeline, with both
    // resident-delta and fully-sealed tails depending on the seed's flushes.
    let mut probes: Vec<SystemTimeMicros> = Vec::new();
    if let Some(first) = commits.first() {
        probes.push(SystemTimeMicros(first.0 - 1));
    }
    probes.extend(commits.iter().copied());
    if let Some(last) = commits.last() {
        probes.push(SystemTimeMicros(last.0 + 1));
    }

    let mut digest = FNV_OFFSET;
    for s in probes {
        digest = verify_scan_at(seed, &delta, &index, &readers, &oracle, s, digest);
    }
    digest
}

/// A catalog holding the identity demo's `account` table, created early enough
/// (system time 1) that every later read in these scenarios is within history.
fn account_catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "account",
            vec![
                ColumnDef::new("id", LogicalType::Int4).expect("column id"),
                ColumnDef::new("balance", LogicalType::Int4).expect("column balance"),
            ],
            TableTemporal::system_only(),
            SystemTimeMicros(1),
        )
        .expect("create account");
    catalog
}

/// Resolve a probe snapshot through the **real SQL binder** — parse
/// `SELECT * FROM account FOR SYSTEM_TIME AS OF <micros>`, bind it against
/// `catalog`, and assert the bound snapshot is exactly `s`. The harness stands
/// in for the pgwire query loop ([STL-104]) that will own this lowering; the
/// assertion is the binding half of the AS-OF correctness story ([STL-101]).
fn resolve_via_binder(catalog: &Catalog, s: SystemTimeMicros) -> SystemTimeMicros {
    let sql = format!("SELECT * FROM account FOR SYSTEM_TIME AS OF {}", s.0);
    let stmts = stele_sql::parse(&sql).expect("parse AS OF probe");
    let ctx = stele_sql::BindContext {
        // For an integer-literal `AS OF`, `now()` is unused — supply the
        // snapshot itself so the context is well-formed regardless.
        snapshot: s,
        catalog,
    };
    let bound = stele_sql::bind_select(&stmts[0], &ctx).expect("bind AS OF probe");
    assert_eq!(
        bound.snapshot, s,
        "binder must resolve AS OF {} to itself",
        s.0
    );
    bound.snapshot
}

/// The four-statement identity demo, end-to-end ([README](../../../README.md)).
///
/// `CREATE`, `INSERT … 100`, `UPDATE … 250`, then
/// `SELECT … FOR SYSTEM_TIME AS OF (now() - interval '1 second')` reads back the
/// **pre-update** row. Returns that row's payload — the test asserts it is the
/// `INSERT`-era `100`, never the `250` the `UPDATE` wrote (STL-101 DoD).
///
/// The `AS OF` clause is the README's verbatim string; it is folded by the real
/// [`stele_sql::bind_select`] binder. With `now()` one second after the insert,
/// `now() - interval '1 second'` lands on the insert instant — strictly before
/// the update — so the bound snapshot resolves the version that held `100`.
///
/// # Panics
///
/// Panics if any stage (DDL, DML, parse, bind, scan) fails, or if the binder
/// does not fold the `AS OF` to the insert instant — correctness regressions,
/// not workload outcomes.
#[must_use]
pub fn four_statement_identity_demo() -> Vec<u8> {
    // (1) CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING.
    let catalog = account_catalog();

    // The one table's storage tiers.
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    // Commit clock starts above the catalog's creation time, so the row history
    // is strictly inside the table's lifetime.
    let mut writer = DmlWriter::new(wal, StepClock::new(10), false);
    let key = BusinessKey::new(b"1".to_vec());
    let who = Principal::new(b"demo".to_vec());

    // (2) INSERT INTO account VALUES (1, 100). Payload is opaque at v0.1 (no row
    // codec yet); `b"100"` stands for the balance value.
    let t_insert = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            Some(b"100".to_vec()),
            0,
            TxnId(1),
            who.clone(),
        )
        .expect("insert")
        .commit;

    // (3) UPDATE account SET balance = 250 WHERE id = 1.
    let t_update = writer
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            None,
            Some(b"250".to_vec()),
            0,
            TxnId(2),
            who,
        )
        .expect("update")
        .commit;
    assert!(
        t_update > t_insert,
        "the update must commit after the insert"
    );

    // (4) SELECT balance FROM account
    //       FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1;
    let now = SystemTimeMicros(t_insert.0 + 1_000_000);
    let sql = "SELECT balance FROM account \
               FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1";
    let stmts = stele_sql::parse(sql).expect("parse demo SELECT");
    let ctx = stele_sql::BindContext {
        snapshot: now,
        catalog: &catalog,
    };
    let bound = stele_sql::bind_select(&stmts[0], &ctx).expect("bind demo SELECT");
    assert_eq!(
        bound.snapshot, t_insert,
        "AS OF (now() - interval '1 second') must fold to the insert instant",
    );

    // Lower the bound select to a SnapshotScan — the glue the pgwire query loop
    // will own (STL-104). `WHERE id = 1` is the business-key equality.
    let readers: Vec<SegmentReader<MemFile>> = Vec::new();
    let out = SnapshotScan::new(&delta, &index, &readers, Snapshot(bound.snapshot))
        .project(vec![ColumnId::Payload])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: ZoneBound::Bytes(b"1".to_vec()),
        })
        .execute()
        .expect("snapshot scan");

    let Some((_, Column::Bytes(payloads))) = out.batch.columns.into_iter().next() else {
        panic!("scan must project the payload as a bytes column");
    };
    let [payload] =
        <[Option<Vec<u8>>; 1]>::try_from(payloads.to_vec()).expect("exactly one row for id = 1");
    // This scenario writes no SQL NULL payload, so the one cell is always present.
    payload.expect("this scenario writes no NULL payload")
}

/// Read a seeded insert/update/delete history back **through the SQL binder**.
///
/// Each probe is resolved from an `AS OF <micros>` clause through the real SQL
/// binder, and the scan at the bound snapshot is checked against the same independent
/// reference oracle [`run_snapshot_scan_seed`] uses ([docs/06 §4], STL-138).
/// Where that sweep drives the executor directly, this drives it *via AS-OF
/// resolution* — so the seed sweep regresses on the binder picking the right
/// instant and the executor reading the right version together (STL-101 DoD).
/// Same seed ⇒ same digest.
///
/// [docs/06 §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart
///
/// # Panics
///
/// Panics if the binder mis-resolves a probe, or if a scan disagrees with the
/// reference oracle — correctness regressions, not workload outcomes.
#[must_use]
pub fn run_as_of_resolution_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    let mut rng = Rng::new(seed);
    let catalog = account_catalog();
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    // Clock base well above the catalog creation (1), so every probe — including
    // `first - 1` — stays within the table's history and the binder resolves it.
    let mut writer = DmlWriter::new(wal, StepClock::new(1_000), false);
    let readers: Vec<SegmentReader<MemFile>> = Vec::new();
    let mut oracle: ScanOracle = BTreeMap::new();

    let key_count = 1 + rng.below_usize(4);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();
    let mut live = vec![false; key_count];
    let mut commits: Vec<SystemTimeMicros> = Vec::new();

    let ops = 8 + rng.below(24);
    for op in 0..ops {
        let k = rng.below_usize(key_count);
        let is_delete = live[k] && rng.below(4) == 0;
        let (commit, new_live) = apply_scan_op(
            &mut writer,
            &mut delta,
            &mut index,
            &readers,
            &mut oracle,
            keys[k].clone(),
            op,
            live[k],
            is_delete,
        );
        live[k] = new_live;
        commits.push(commit);
    }

    // Probe just before the first commit, at every commit, and just past the
    // last — each routed through the binder before the scan.
    let mut probes: Vec<SystemTimeMicros> = Vec::new();
    if let Some(first) = commits.first() {
        probes.push(SystemTimeMicros(first.0 - 1));
    }
    probes.extend(commits.iter().copied());
    if let Some(last) = commits.last() {
        probes.push(SystemTimeMicros(last.0 + 1));
    }

    let mut digest = FNV_OFFSET;
    for s in probes {
        let resolved = resolve_via_binder(&catalog, s);
        digest = verify_scan_at(seed, &delta, &index, &readers, &oracle, resolved, digest);
    }
    digest
}

// ---------------------------------------------------------------------------
// Vectorized-exec scenario — aggregates and joins over fault-recovered state
// (STL-187).
// ---------------------------------------------------------------------------

/// One typed row of the vectorized-exec scenario's table: the business key plus
/// the `(group, value)` cell pair its payload encodes. `None` is a SQL NULL
/// cell — the payload encodes NULL in-band, so the operators' NULL semantics
/// (NULL keys group together, a NULL join key matches nothing, aggregates skip
/// NULL arguments) are exercised over storage-derived data, not hand-built
/// vectors.
struct ExecRow {
    key: String,
    group: Option<i64>,
    value: Option<i64>,
}

/// Encode a `(group, value)` cell pair into the opaque payload the storage
/// tiers carry — `"{g}|{v}"`, with `n` for a NULL cell.
fn encode_exec_payload(group: Option<i64>, value: Option<i64>) -> Vec<u8> {
    let cell = |c: Option<i64>| c.map_or_else(|| "n".to_owned(), |x| x.to_string());
    format!("{}|{}", cell(group), cell(value)).into_bytes()
}

/// Decode [`encode_exec_payload`]'s framing back into the typed cell pair.
fn decode_exec_payload(seed: u64, payload: &[u8]) -> (Option<i64>, Option<i64>) {
    let text = std::str::from_utf8(payload)
        .unwrap_or_else(|_| panic!("seed {seed}: exec payload must be UTF-8"));
    let (g, v) = text
        .split_once('|')
        .unwrap_or_else(|| panic!("seed {seed}: exec payload must be `g|v`, got `{text}`"));
    let cell = |s: &str| {
        (s != "n").then(|| {
            s.parse()
                .unwrap_or_else(|_| panic!("seed {seed}: exec cell must be an i64, got `{s}`"))
        })
    };
    (cell(g), cell(v))
}

/// Scan the recovered engine at `s`, assert the result equals the reference
/// oracle truncated at the recovered `cutoff`, and decode it into typed rows.
///
/// The returned rows are sorted by business key (the scan map's order) — the
/// deterministic row order the operator verification builds its expected
/// outputs in. The scan-vs-oracle equality also ties [`SnapshotScan`] to the
/// point-lookup path `verify_recovered_prefix` already proved, so the operators
/// below consume input that is *known* correct — any later divergence is theirs.
fn exec_rows_at(
    seed: u64,
    engine: &Engine<StepClock, FaultDisk>,
    oracle: &ScanOracle,
    cutoff: Option<SystemTimeMicros>,
    s: SystemTimeMicros,
) -> Vec<ExecRow> {
    let readers = engine
        .open_segment_readers()
        .expect("re-open recovered segments");
    let out = SnapshotScan::new(engine.delta(), engine.index(), &readers, Snapshot(s))
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .execute()
        .expect("snapshot scan over recovered tiers");
    let got = scan_map(&out);

    let mut want: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    if let Some(c) = cutoff {
        for (key, timeline) in oracle {
            if let Some((_, Some(payload))) = timeline.range(..=s.min(c)).next_back() {
                want.insert(key.as_bytes().to_vec(), payload.clone());
            }
        }
    }
    assert_eq!(
        got, want,
        "seed {seed}: vectorized scan at {s:?} disagrees with the committed-prefix oracle",
    );

    got.into_iter()
        .map(|(key, payload)| {
            let (group, value) = decode_exec_payload(seed, &payload);
            ExecRow {
                key: String::from_utf8(key).expect("business keys are UTF-8"),
                group,
                value,
            }
        })
        .collect()
}

/// The rows as the executor's typed input batch: column 0 the `group` key
/// (`INT8`), column 1 the `value` argument (`INT8`), column 2 the business key
/// (`TEXT`) — the join identity.
fn exec_columns(rows: &[ExecRow]) -> Vec<Vector> {
    vec![
        Vector::Int8(rows.iter().map(|r| r.group).collect()),
        Vector::Int8(rows.iter().map(|r| r.value).collect()),
        Vector::Text(rows.iter().map(|r| Some(r.key.clone())).collect()),
    ]
}

/// Fold a nullable `i64` cell into the digest, presence-tagged so a NULL never
/// hashes like a zero.
fn fold_opt_i64(digest: u64, cell: Option<i64>) -> u64 {
    cell.map_or_else(
        || fnv1a(digest, &[0]),
        |v| fnv1a(fnv1a(digest, &[1]), &v.to_le_bytes()),
    )
}

/// [`fold_opt_i64`] for `u64` cells (AVG bits, join row indices).
fn fold_opt_u64(digest: u64, cell: Option<u64>) -> u64 {
    cell.map_or_else(
        || fnv1a(digest, &[0]),
        |v| fnv1a(fnv1a(digest, &[1]), &v.to_le_bytes()),
    )
}

/// One group's expected aggregate row: `(COUNT(*), COUNT(v), SUM(v), MIN(v),
/// MAX(v), AVG(v) as IEEE-754 bits)`.
type AggRow = (i64, i64, Option<i64>, Option<i64>, Option<i64>, Option<u64>);

/// A group's in-progress scalar reference fold: `(COUNT(v), Σv, MIN(v), MAX(v))`.
type AggFold = (i64, i128, Option<i64>, Option<i64>);

/// Run [`hash_aggregate`] over the scanned rows — `COUNT(*)`, `COUNT(v)`,
/// `SUM(v)`, `MIN(v)`, `MAX(v)`, `AVG(v)` grouped by the `group` column — and
/// assert every group agrees with an independent scalar fold of the same rows.
/// Returns the digest with the verified aggregate table folded in.
#[allow(clippy::cast_precision_loss)] // AVG's reference mean is fractional by definition
fn verify_vectorized_aggregates(seed: u64, rows: &[ExecRow], digest: u64) -> u64 {
    let columns = exec_columns(rows);
    let agg = |func: AggregateFunc, arg: Option<Expr>| Aggregator { func, arg };
    let aggregators = [
        agg(AggregateFunc::Count, None),
        agg(AggregateFunc::Count, Some(Expr::col(1))),
        agg(AggregateFunc::Sum, Some(Expr::col(1))),
        agg(AggregateFunc::Min, Some(Expr::col(1))),
        agg(AggregateFunc::Max, Some(Expr::col(1))),
        agg(AggregateFunc::Avg, Some(Expr::col(1))),
    ];
    let out = hash_aggregate(&[Expr::col(0)], &aggregators, &columns, rows.len())
        .expect("hash_aggregate over scanned rows");

    // Unpack the output columns; a shape mismatch is an operator regression.
    let [Vector::Int8(g)] = out.groups.as_slice() else {
        panic!("seed {seed}: GROUP BY an INT8 column must key the output by INT8");
    };
    let [
        Vector::Int8(count_star),
        Vector::Int8(count_v),
        Vector::Int8(sum),
        Vector::Int8(min),
        Vector::Int8(max),
        Vector::Float8(avg),
    ] = out.aggregates.as_slice()
    else {
        panic!("seed {seed}: the aggregate output columns have the wrong shape");
    };

    // The output rows in the operator's native emission order — the digest
    // folds this (not the re-sorted map), so an output-order regression in
    // `hash_aggregate` breaks same-seed reproducibility loudly.
    let mut native: Vec<(Option<i64>, AggRow)> = Vec::with_capacity(out.num_groups);
    let mut got: BTreeMap<Option<i64>, AggRow> = BTreeMap::new();
    for i in 0..out.num_groups {
        let row = (
            count_star[i].expect("COUNT(*) is never NULL"),
            count_v[i].expect("COUNT(v) is never NULL"),
            sum[i],
            min[i],
            max[i],
            avg[i],
        );
        native.push((g[i], row));
        assert!(
            got.insert(g[i], row).is_none(),
            "seed {seed}: hash_aggregate emitted group {:?} twice",
            g[i],
        );
    }

    // The independent reference: a scalar fold over the same rows.
    let mut star: BTreeMap<Option<i64>, i64> = BTreeMap::new();
    let mut folds: BTreeMap<Option<i64>, AggFold> = BTreeMap::new();
    for row in rows {
        *star.entry(row.group).or_insert(0) += 1;
        let e = folds.entry(row.group).or_insert((0, 0, None, None));
        if let Some(v) = row.value {
            e.0 += 1;
            e.1 += i128::from(v);
            e.2 = Some(e.2.map_or(v, |m: i64| m.min(v)));
            e.3 = Some(e.3.map_or(v, |m: i64| m.max(v)));
        }
    }
    let want: BTreeMap<Option<i64>, AggRow> = folds
        .into_iter()
        .map(|(group, (n, total, min, max))| {
            let sum = (n != 0).then(|| i64::try_from(total).ok()).flatten();
            // The aggregator's documented arithmetic: the exact integer total
            // converted to f64 once, divided by the non-NULL count.
            let avg = (n != 0).then(|| (total as f64 / n as f64).to_bits());
            (group, (star[&group], n, sum, min, max, avg))
        })
        .collect();

    assert_eq!(
        got, want,
        "seed {seed}: hash_aggregate disagrees with the scalar reference fold",
    );

    let mut digest = fnv1a(
        digest,
        &u64::try_from(out.num_groups)
            .expect("group count fits u64")
            .to_le_bytes(),
    );
    for (group, row) in &native {
        digest = fold_opt_i64(digest, *group);
        digest = fnv1a(digest, &row.0.to_le_bytes());
        digest = fnv1a(digest, &row.1.to_le_bytes());
        digest = fold_opt_i64(digest, row.2);
        digest = fold_opt_i64(digest, row.3);
        digest = fold_opt_i64(digest, row.4);
        digest = fold_opt_u64(digest, row.5);
    }
    digest
}

/// Run [`hash_join`] between the live rows (`left`) and an earlier snapshot's
/// rows (`right`) and assert every emitted index list against an independent
/// nested-loop reference — `INNER` on the (nullable, fanning-out) `group`
/// column, then `LEFT` / `SEMI` / `ANTI` on the (per-snapshot-unique) business
/// key. Returns the digest with the verified indices folded in.
fn verify_vectorized_joins(seed: u64, left: &[ExecRow], right: &[ExecRow], digest: u64) -> u64 {
    let lcols = exec_columns(left);
    let rcols = exec_columns(right);
    let join = |ty: JoinType, key: usize| {
        hash_join(
            ty,
            &lcols,
            left.len(),
            &Expr::col(key),
            &rcols,
            right.len(),
            &Expr::col(key),
        )
        .expect("hash_join over scanned rows")
    };

    // INNER on `group`: every (l, r) pair with equal non-NULL groups, probe
    // (left) order outermost and build rows ascending within a probe — the
    // operator's documented deterministic order. A NULL group matches nothing.
    let inner = join(JoinType::Inner, 0);
    let mut want_left = Vec::new();
    let mut want_right = Vec::new();
    for (l, lrow) in left.iter().enumerate() {
        if let Some(g) = lrow.group {
            for (r, rrow) in right.iter().enumerate() {
                if rrow.group == Some(g) {
                    want_left.push(l);
                    want_right.push(Some(r));
                }
            }
        }
    }
    assert_eq!(
        (&inner.left, &inner.right),
        (&want_left, &want_right),
        "seed {seed}: INNER join on the group column disagrees with the reference",
    );

    // LEFT / SEMI / ANTI on the business key: a key is live at most once per
    // snapshot, so each left row has at most one match and the three outputs
    // partition the left rows by "key also live at the earlier snapshot".
    let matched: Vec<(usize, Option<usize>)> = left
        .iter()
        .enumerate()
        .map(|(l, lrow)| (l, right.iter().position(|rrow| rrow.key == lrow.key)))
        .collect();
    let left_join = join(JoinType::Left, 2);
    let (want_l, want_r): (Vec<usize>, Vec<Option<usize>>) = matched.iter().copied().unzip();
    assert_eq!(
        (&left_join.left, &left_join.right),
        (&want_l, &want_r),
        "seed {seed}: LEFT join on the business key disagrees with the reference",
    );
    let semi = join(JoinType::Semi, 2);
    let want_semi: Vec<usize> = matched
        .iter()
        .filter(|(_, r)| r.is_some())
        .map(|&(l, _)| l)
        .collect();
    assert_eq!(
        semi.left, want_semi,
        "seed {seed}: SEMI join on the business key disagrees with the reference",
    );
    assert!(
        semi.right.is_empty(),
        "seed {seed}: a SEMI join emits no right side",
    );
    let anti = join(JoinType::Anti, 2);
    let want_anti: Vec<usize> = matched
        .iter()
        .filter(|(_, r)| r.is_none())
        .map(|&(l, _)| l)
        .collect();
    assert_eq!(
        anti.left, want_anti,
        "seed {seed}: ANTI join on the business key disagrees with the reference",
    );
    assert!(
        anti.right.is_empty(),
        "seed {seed}: an ANTI join emits no right side",
    );

    // Fold the verified shapes: the INNER pair list, then the key-join match
    // vector (which LEFT/SEMI/ANTI all re-derive from).
    let mut digest = fnv1a(
        digest,
        &u64::try_from(inner.left.len())
            .expect("join size fits u64")
            .to_le_bytes(),
    );
    for (&l, r) in inner.left.iter().zip(&inner.right) {
        digest = fnv1a(
            digest,
            &u64::try_from(l).expect("row fits u64").to_le_bytes(),
        );
        if let Some(r) = *r {
            digest = fnv1a(
                digest,
                &u64::try_from(r).expect("row fits u64").to_le_bytes(),
            );
        }
    }
    for (l, r) in matched {
        digest = fnv1a(
            digest,
            &u64::try_from(l).expect("row fits u64").to_le_bytes(),
        );
        digest = fold_opt_u64(digest, r.map(|r| u64::try_from(r).expect("row fits u64")));
    }
    digest
}

/// Apply one seeded op of [`run_vectorized_exec_faults_seed`]'s workload — an
/// insert/update/delete whose payload carries a typed `(group, value)` row,
/// either cell sometimes NULL. Committed effects fold into the oracle; returns
/// `false` on a faulted write — the crash, after which the caller stops.
#[allow(clippy::too_many_arguments)] // engine + rng + the oracle/live model + the row shape
fn apply_exec_op(
    engine: &mut Engine<StepClock, FaultDisk>,
    rng: &mut Rng,
    keys: &[BusinessKey],
    live: &mut [bool],
    oracle: &mut ScanOracle,
    committed: &mut Vec<SystemTimeMicros>,
    group_count: u64,
    op: u64,
) -> bool {
    let k = rng.below_usize(keys.len());
    let key = keys[k].clone();
    let txn = TxnId(op);
    let principal = Principal::new(b"sim".to_vec());
    let group =
        (rng.below(6) != 0).then(|| i64::try_from(rng.below(group_count)).expect("group fits i64"));
    let value = (rng.below(6) != 0).then(|| i64::try_from(op).expect("op fits i64"));
    let payload = encode_exec_payload(group, value);
    let want_delete = live[k] && rng.below(4) == 0;
    let outcome = if live[k] {
        if want_delete {
            engine.delete(&key, txn, principal)
        } else {
            engine.update(key.clone(), None, Some(payload.clone()), 0, txn, principal)
        }
    } else {
        engine.insert(key.clone(), None, Some(payload.clone()), 0, txn, principal)
    };
    outcome.is_ok_and(|o| {
        let effect = if want_delete { None } else { Some(payload) };
        oracle.entry(key).or_default().insert(o.commit, effect);
        committed.push(o.commit);
        live[k] = !want_delete;
        true
    })
}

/// Drive the **vectorized operators over fault-recovered storage** — the
/// vectorized-exec DST coverage of [STL-187].
///
/// The unit tests pin [`hash_aggregate`] / [`hash_join`] semantics over
/// hand-built vectors; what no sweep covered is the pipeline the engine
/// actually runs — typed rows written through a faulty disk, torn by a crash,
/// recovered, scanned, and only then aggregated and joined. Per seed:
///
/// * **Workload phase**: a clean prelude of seeded auto-commit
///   inserts/updates/deletes whose payloads encode a typed `(group, value)`
///   row — either cell seeded NULL sometimes, so the NULL semantics travel
///   end-to-end — then torn-write / full-disk / slow-fsync faults arm and the
///   writing continues until the first faulted write or mid-flush fault: the
///   crash. Periodic [`Engine::flush`]es seal segments under a seeded
///   row-group bound, so the later scan merges delta + sealed tiers across
///   row-group boundaries.
/// * **Recovery phase** (faults silenced): [`Engine::recover`] replays the
///   surviving log — a torn group tail or an orphaned mid-flush segment must
///   resolve to a consistent committed prefix (`verify_recovered_prefix`).
///   Read-rot is deliberately not armed during recovery: that dimension
///   belongs to the recovery sweeps ([`run_engine_recover_faults_seed`] & co.),
///   and refusing recovery would starve the phase this scenario exists for.
/// * **Vectorized phase**: [`SnapshotScan`] reads the
///   recovered tiers at the live snapshot and at a seed-chosen earlier commit
///   boundary, each asserted equal to the committed-prefix oracle; the typed
///   rows then drive [`hash_aggregate`] (`COUNT(*)` / `COUNT` / `SUM` / `MIN` /
///   `MAX` / `AVG` by `group`) and [`hash_join`] (`INNER` on `group`,
///   `LEFT` / `SEMI` / `ANTI` on the business key — a current-vs-historical
///   self-join), each asserted against an independent scalar reference fold of
///   the same rows.
///
/// The digest folds the prefix cutoff, the verified aggregate table, the
/// verified join indices, and the disk's fault-event log. Same seed ⇒ same
/// digest.
///
/// # Panics
///
/// Panics if recovery silently diverges from every committed prefix, if a scan
/// disagrees with the committed-prefix oracle, or if an operator output
/// disagrees with its scalar reference — correctness regressions, not workload
/// outcomes.
#[must_use]
#[allow(clippy::too_many_lines)] // workload + crash + recovery + operator verification reads as one scenario
pub fn run_vectorized_exec_faults_seed(seed: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;

    // Write-path rates match the recovery sweeps. Read-rot is deliberately
    // NOT armed in this scenario: recovery-under-corruption is the recovery
    // sweeps' dimension ([`run_engine_recover_faults_seed`] & co.), and with
    // the many segments this workload seals, even a tiny per-read rate would
    // refuse recovery on most seeds — starving the vectorized phase, which is
    // the coverage this scenario exists to add.
    let mut prof_rng = Rng::new(seed ^ 0xFA17_D15C_0BAD_F00D);
    let p_torn = prob_permille(&mut prof_rng, 20, 60);
    let p_full = prob_permille(&mut prof_rng, 10, 30);
    let p_slow = prob_permille(&mut prof_rng, 200, 500);

    let mut rng = Rng::new(seed);

    let mut oracle: ScanOracle = BTreeMap::new();
    let mut committed: Vec<SystemTimeMicros> = Vec::new();

    let key_count = 4 + rng.below_usize(6);
    let keys: Vec<BusinessKey> = (0..key_count)
        .map(|i| BusinessKey::new(format!("k-{i:04}").into_bytes()))
        .collect();
    // A handful of group ids, so groups usually hold several rows (join
    // fan-out, multi-row aggregates) while several groups coexist.
    let group_count = 2 + rng.below(3);

    let disk = FaultDisk::new(seed, FaultProfile::none());
    {
        let row_group_rows = 1 + rng.below_usize(3);
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false)
            .expect("open engine")
            .with_flush_row_group_rows(row_group_rows);
        let mut live = vec![false; key_count];

        // A clean prelude seeds the table before any fault is armed: the
        // vectorized phase needs rows to chew on, and a crash on the very
        // first writes would starve it (the recovery sweeps own the
        // everything-torn shapes). Prelude flushes seal part of it, so the
        // later scan merges delta + sealed tiers either way.
        let prelude = 10 + rng.below(12);
        for op in 0..prelude {
            assert!(
                apply_exec_op(
                    &mut engine,
                    &mut rng,
                    &keys,
                    &mut live,
                    &mut oracle,
                    &mut committed,
                    group_count,
                    op,
                ),
                "seed {seed}: a clean-prelude write cannot fault",
            );
            if rng.below(4) == 0 {
                engine.flush().expect("a clean-prelude flush cannot fault");
            }
        }

        // Now arm the write faults and keep writing. The first faulted write
        // is the crash; so is a mid-flush fault (the orphan-segment case
        // recovery must survive).
        disk.enable(FaultKind::TornWrite, p_torn);
        disk.enable(FaultKind::FullDisk, p_full);
        disk.enable(FaultKind::SlowSync, p_slow);
        let ops = 8 + rng.below(16);
        'workload: for op in 0..ops {
            if !apply_exec_op(
                &mut engine,
                &mut rng,
                &keys,
                &mut live,
                &mut oracle,
                &mut committed,
                group_count,
                prelude + op,
            ) {
                break 'workload;
            }
            if rng.below(4) == 0 && engine.flush().is_err() {
                break 'workload;
            }
        }
        // Best-effort graceful-shutdown flush, then drop the engine — the crash.
        let _ = engine.flush();
    }

    // Silence the write faults for recovery; reads stay clean (see the
    // profile note above — read-rot is the recovery sweeps' dimension).
    disk.disable(FaultKind::TornWrite);
    disk.disable(FaultKind::FullDisk);
    disk.disable(FaultKind::SlowSync);

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), false);

    let mut digest = FNV_OFFSET;
    match recovered {
        // Defensive: with clean reads recovery is expected to succeed, but a
        // refusal is still a sound (detected) outcome, digested as such.
        Err(_) => digest = fnv1a(digest, &[0xE2]),
        Ok(engine) => {
            let (k, _) = verify_recovered_prefix(seed, &engine, &oracle, &committed, &keys);
            digest = fnv1a(digest, &[0x0C]);
            digest = fnv1a(
                digest,
                &u64::try_from(k).expect("prefix len fits u64").to_le_bytes(),
            );
            let cutoff = (k != 0).then(|| committed[k - 1]);

            // The live side: every key's recovered current row, as typed rows.
            let s_hi = SystemTimeMicros(committed.last().map_or(1, |c| c.0 + 1));
            let current = exec_rows_at(seed, &engine, &oracle, cutoff, s_hi);
            digest = verify_vectorized_aggregates(seed, &current, digest);

            // The historical side: a seed-chosen commit boundary, scanned the
            // same way — the AS-OF self-join's right input.
            if committed.is_empty() {
                digest = fnv1a(digest, &[0x00]);
            } else {
                let s_lo = committed[rng.below_usize(committed.len())];
                let history = exec_rows_at(seed, &engine, &oracle, cutoff, s_lo);
                digest = verify_vectorized_joins(seed, &current, &history, digest);
            }
        }
    }

    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

// ---------------------------------------------------------------------------
// Scenario registry — the surface the CLI drives (STL-110).
// ---------------------------------------------------------------------------

/// One registered simulation scenario.
///
/// Every scenario follows the same lifecycle for a given seed: **setup** a fresh
/// world seeded from the seed alone (tiers, disk, or engine — no wall-clock and
/// no ambient RNG), **run** a seed-driven workload, and **assert its invariants**
/// against an independent reference oracle, returning a digest of the result.
/// Determinism is the invariant every scenario shares — the same seed must
/// always produce the same digest — so a failure is a *number* a contributor
/// replays with `just sim-seed <N>`
/// ([docs/06 §5](../../../docs/06-testing-strategy.md#5-deterministic-simulation-testing-dst--the-centerpiece)).
///
/// Scenarios assert by panicking — the seed is in every assertion message — and
/// [`install_failure_reporter`] turns that panic into a prominent `scenario +
/// seed` banner before the process exits non-zero.
pub trait Scenario {
    /// Stable, kebab-case identifier — printed next to the seed on failure.
    fn name(&self) -> &'static str;

    /// Run the scenario for `seed` and return its digest. Panics on an invariant
    /// violation; same seed ⇒ same digest.
    fn run(&self, seed: u64) -> u64;

    /// Whether this scenario is swept only when fault injection is enabled
    /// (`--fault-injection on`). Defaults to `false`; the fault-disk scenario
    /// overrides it so the flag actually changes what the sweep covers.
    fn requires_fault_injection(&self) -> bool {
        false
    }
}

/// A [`Scenario`] backed by a free `fn(u64) -> u64` digest function — the shape
/// every v0.1 scenario already has.
struct FnScenario {
    name: &'static str,
    run: fn(u64) -> u64,
    fault: bool,
}

impl FnScenario {
    fn boxed(name: &'static str, run: fn(u64) -> u64) -> Box<dyn Scenario> {
        Box::new(Self {
            name,
            run,
            fault: false,
        })
    }

    /// A scenario that only runs under `--fault-injection on`.
    fn fault(name: &'static str, run: fn(u64) -> u64) -> Box<dyn Scenario> {
        Box::new(Self {
            name,
            run,
            fault: true,
        })
    }
}

impl Scenario for FnScenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, seed: u64) -> u64 {
        (self.run)(seed)
    }

    fn requires_fault_injection(&self) -> bool {
        self.fault
    }
}

/// Every scenario the harness drives, in a stable order.
///
/// The v0.1 set covers the system-time and valid-time write paths, the
/// WAL→delta(+index) recovery paths, the MVCC commit/conflict path, the
/// commit-log hash-chain recovery + tamper-evidence path, the merged
/// AS-OF read path with its binder and the canonical AS-OF oracle, the engine
/// kill-and-recover driver and its fault-injected variant, the cooperative
/// scheduler's interleaving demo, and the seeded-fault virtual disk.
#[must_use]
pub fn registry() -> Vec<Box<dyn Scenario>> {
    vec![
        FnScenario::boxed("storage", run_storage_seed),
        FnScenario::boxed("valid-time", run_validtime_seed),
        FnScenario::boxed("delete-provenance", run_delete_seed),
        FnScenario::boxed("dml-wal-replay", run_dml_seed),
        FnScenario::boxed("mvcc", run_mvcc_seed),
        FnScenario::boxed("si-provenance", run_si_oracle_seed),
        FnScenario::boxed("chain-recovery", run_chain_recovery_seed),
        FnScenario::boxed("recovery-index", run_recovery_index_seed),
        FnScenario::boxed("snapshot-scan", run_snapshot_scan_seed),
        FnScenario::boxed("as-of-resolution", run_as_of_resolution_seed),
        FnScenario::boxed("as-of-oracle", run_as_of_oracle_seed),
        FnScenario::boxed("engine-recover", run_engine_recover_seed),
        FnScenario::fault("engine-recover-faults", run_engine_recover_faults_seed),
        FnScenario::boxed("engine-flush-recover", run_engine_flush_recover_seed),
        FnScenario::fault(
            "engine-flush-recover-faults",
            run_engine_flush_recover_faults_seed,
        ),
        FnScenario::fault(
            "group-commit-recover-faults",
            run_group_commit_recover_faults_seed,
        ),
        FnScenario::fault(
            "txn-commit-rollback-faults",
            run_txn_commit_rollback_faults_seed,
        ),
        FnScenario::fault("wal-fsync-poison", run_wal_fsync_poison_seed),
        FnScenario::boxed(
            "group-commit-abort-rollback",
            run_group_commit_abort_rollback_seed,
        ),
        FnScenario::fault("vectorized-exec-faults", run_vectorized_exec_faults_seed),
        FnScenario::boxed("schedule", run_schedule_seed_digest),
        FnScenario::fault("fault-disk", run_fault_seed),
        FnScenario::fault("index-build-crash", run_index_build_crash_seed),
    ]
}

/// The scenario + seed currently executing, read by the panic hook to name a
/// failure. `None` when the harness is idle.
static CURRENT: Mutex<Option<(&'static str, u64)>> = Mutex::new(None);

fn set_current(scenario: &'static str, seed: u64) {
    if let Ok(mut guard) = CURRENT.lock() {
        *guard = Some((scenario, seed));
    }
}

fn clear_current() {
    if let Ok(mut guard) = CURRENT.lock() {
        *guard = None;
    }
}

/// Install a panic hook that names a failing scenario and seed.
///
/// On any scenario panic it prints a prominent `scenario + seed` banner and the
/// command to replay it, then chains to the previous hook so the original
/// assertion message and location still show.
///
/// Call once from the binary's `main` before [`sweep`] or [`replay`]. With
/// `panic = "abort"` in the release profile (the profile `just sim` builds) the
/// process aborts right after the banner; in a debug/unwind build the panic
/// propagates out instead. Either way the exit is non-zero and the seed is
/// reproducible — the "failure is a number" contract of the harness.
pub fn install_failure_reporter() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Some((scenario, seed)) = CURRENT.lock().ok().and_then(|guard| *guard) {
            eprintln!(
                "\nstele-sim: FAIL — scenario `{scenario}` violated an invariant on seed {seed}"
            );
            eprintln!(
                "stele-sim: reproduce with `just sim-seed {seed}`  (cargo run -p stele-sim -- --seed {seed})"
            );
        }
        previous(info);
    }));
}

/// A clean sweep — every active scenario passed every seed.
#[derive(Debug, Clone, Copy)]
pub struct SweepReport {
    /// Scenarios that actually ran (fault-disk only when faults are on).
    pub scenarios: usize,
    /// Seeds swept.
    pub seeds: u64,
    /// Order-sensitive fold of every per-`(seed, scenario)` digest — one
    /// regression signal for the whole sweep.
    pub digest: u64,
}

/// Sweep `seeds` distinct seeds across every registered scenario, returning a
/// [`SweepReport`] once all pass.
///
/// A scenario that violates an invariant panics; with [`install_failure_reporter`]
/// in place that prints the failing scenario + seed before the process exits
/// non-zero, so this only returns on success. `fault_injection` gates the
/// scenarios that opt in via [`Scenario::requires_fault_injection`] — with it
/// off, the fault-disk scenario is skipped, so the `--fault-injection` flag
/// genuinely changes coverage.
#[must_use]
pub fn sweep(seeds: u64, fault_injection: bool) -> SweepReport {
    const OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let registry = registry();
    let active: Vec<&dyn Scenario> = registry
        .iter()
        .map(Box::as_ref)
        .filter(|s| fault_injection || !s.requires_fault_injection())
        .collect();

    let mut digest = OFFSET;
    for seed in 0..seeds {
        for scenario in &active {
            set_current(scenario.name(), seed);
            // Mix each per-seed digest with an order-dependent FNV step (not XOR,
            // which would cancel matching digests) so the sweep stays a sharp
            // regression signal across scenarios and seeds.
            digest = (digest ^ scenario.run(seed)).wrapping_mul(PRIME);
        }
    }
    clear_current();
    SweepReport {
        scenarios: active.len(),
        seeds,
        digest,
    }
}

/// Replay one seed across **every** scenario (fault-disk included), returning each
/// scenario's name and digest.
///
/// The reproduction path behind `just sim-seed K`: a scenario that fails panics
/// with its full assertion (the seed is in the message), which
/// [`install_failure_reporter`] names by scenario. Faults are always included so
/// a fault-disk failure reproduces even though `just sim-seed` passes no
/// `--fault-injection` flag.
#[must_use]
pub fn replay(seed: u64) -> Vec<(&'static str, u64)> {
    let digests = registry()
        .iter()
        .map(|scenario| {
            set_current(scenario.name(), seed);
            let digest = scenario.run(seed);
            (scenario.name(), digest)
        })
        .collect();
    // Clear the marker so a later, unrelated panic can't print a stale
    // `scenario + seed` banner from this finished replay.
    clear_current();
    digests
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
            assert_eq!(
                run_fault_seed(seed),
                run_fault_seed(seed),
                "seed {seed} must replay to an identical fault digest"
            );
        }
    }

    #[test]
    fn fault_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..64).map(run_fault_seed).collect();
        assert!(
            digests.len() > 1,
            "the seeded-fault disk workload must actually depend on the seed"
        );
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
    fn engine_recover_seed_is_reproducible() {
        // Each seed also asserts (internally) that the driver-recovered engine
        // rebuilds the exact validity index and that every recovered AS-OF read
        // matches the reference oracle ([STL-102] DoD).
        for seed in 0..64 {
            assert_eq!(
                run_engine_recover_seed(seed),
                run_engine_recover_seed(seed),
                "seed {seed} must replay to an identical engine-recovery digest"
            );
        }
    }

    #[test]
    fn engine_recover_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_engine_recover_seed).collect();
        assert!(
            digests.len() > 1,
            "the engine kill-and-recover workload must actually depend on the seed"
        );
    }

    #[test]
    fn engine_recover_faults_seed_is_reproducible() {
        // Each seed also asserts (internally) that recovery through the FaultDisk
        // either lands on a consistent committed prefix of the reference oracle or
        // cleanly errors — never silently diverges (STL-153 DoD).
        for seed in 0..64 {
            assert_eq!(
                run_engine_recover_faults_seed(seed),
                run_engine_recover_faults_seed(seed),
                "seed {seed} must replay to an identical fault-recovery digest"
            );
        }
    }

    #[test]
    fn engine_recover_faults_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_engine_recover_faults_seed).collect();
        assert!(
            digests.len() > 1,
            "the fault-injected kill-and-recover workload must depend on the seed"
        );
    }

    #[test]
    fn txn_commit_rollback_faults_seed_is_reproducible() {
        // Each seed also asserts (internally) that a rolled-back transaction's
        // writes are invisible live and that recovery through the FaultDisk
        // lands on a consistent committed-transaction prefix or cleanly errors
        // (STL-187 DoD).
        for seed in 0..64 {
            assert_eq!(
                run_txn_commit_rollback_faults_seed(seed),
                run_txn_commit_rollback_faults_seed(seed),
                "seed {seed} must replay to an identical commit/rollback digest"
            );
        }
    }

    #[test]
    fn txn_commit_rollback_faults_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_txn_commit_rollback_faults_seed).collect();
        assert!(
            digests.len() > 1,
            "the commit/rollback fault workload must actually depend on the seed"
        );
    }

    #[test]
    fn vectorized_exec_faults_seed_is_reproducible() {
        // Each seed also asserts (internally) that the recovered scan matches
        // the committed-prefix oracle and that hash_aggregate / hash_join over
        // the scanned rows match an independent scalar reference (STL-187 DoD).
        for seed in 0..64 {
            assert_eq!(
                run_vectorized_exec_faults_seed(seed),
                run_vectorized_exec_faults_seed(seed),
                "seed {seed} must replay to an identical vectorized-exec digest"
            );
        }
    }

    #[test]
    fn vectorized_exec_faults_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_vectorized_exec_faults_seed).collect();
        assert!(
            digests.len() > 1,
            "the vectorized-exec fault workload must actually depend on the seed"
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

    #[test]
    fn chain_recovery_seed_is_reproducible() {
        // Each seed also asserts (internally) that a clean log recovers to the
        // pre-crash head and that a forged historical frame fails closed.
        for seed in 0..64 {
            assert_eq!(
                run_chain_recovery_seed(seed),
                run_chain_recovery_seed(seed),
                "seed {seed} must replay to an identical chain-recovery digest"
            );
        }
    }

    #[test]
    fn chain_recovery_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_chain_recovery_seed).collect();
        assert!(
            digests.len() > 1,
            "the commit-log recovery workload must actually depend on the seed"
        );
    }

    #[test]
    fn snapshot_scan_seed_is_reproducible() {
        // Each seed also asserts (internally) that the executor's merged AS-OF
        // read matches the in-memory reference oracle and that segment reads
        // equal the zone-map survivors at every probe (STL-100 DoD).
        for seed in 0..64 {
            assert_eq!(
                run_snapshot_scan_seed(seed),
                run_snapshot_scan_seed(seed),
                "seed {seed} must replay to an identical snapshot-scan digest"
            );
        }
    }

    #[test]
    fn snapshot_scan_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> = (0..64).map(run_snapshot_scan_seed).collect();
        assert!(
            digests.len() > 1,
            "the snapshot-scan read workload must actually depend on the seed"
        );
    }

    #[test]
    fn four_statement_demo_reads_the_pre_update_value() {
        // The v0.1 identity: an AS OF before the update reads 100, never the 250
        // the update wrote — history is never destroyed (STL-101 DoD).
        assert_eq!(
            four_statement_identity_demo(),
            b"100".to_vec(),
            "AS OF (now() - interval '1 second') must read the pre-update value",
        );
    }

    #[test]
    fn as_of_resolution_seed_is_reproducible() {
        // Each seed also asserts (internally) that the binder resolves every
        // probe to its intended instant and that the scan at the bound snapshot
        // matches the reference oracle (STL-101 DoD).
        for seed in 0..64 {
            assert_eq!(
                run_as_of_resolution_seed(seed),
                run_as_of_resolution_seed(seed),
                "seed {seed} must replay to an identical AS-OF-resolution digest"
            );
        }
    }

    #[test]
    fn as_of_resolution_distinct_seeds_diverge() {
        let digests: std::collections::HashSet<u64> =
            (0..64).map(run_as_of_resolution_seed).collect();
        assert!(
            digests.len() > 1,
            "the AS-OF-resolution read workload must actually depend on the seed"
        );
    }

    #[test]
    fn registry_names_are_unique_and_kebab() {
        let registry = registry();
        let total = registry.len();
        assert!(total > 1, "the registry must hold the v0.1 scenario set");
        let mut names: Vec<&str> = registry.iter().map(|s| s.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total, "scenario names must be unique");
        for name in names {
            assert!(
                !name.is_empty()
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "scenario name `{name}` must be lowercase kebab-case",
            );
        }
    }

    #[test]
    fn sweep_is_deterministic_and_covers_every_scenario() {
        // Same seeds ⇒ same fold: the determinism the whole harness rests on,
        // now asserted at the registry-sweep level (STL-110 DoD).
        let first = sweep(16, true);
        let second = sweep(16, true);
        assert_eq!(
            first.digest, second.digest,
            "the sweep digest must replay identically"
        );
        assert_eq!(first.seeds, 16);
        assert_eq!(
            first.scenarios,
            registry().len(),
            "faults on ⇒ every registered scenario runs"
        );
    }

    #[test]
    fn fault_injection_gates_exactly_the_fault_scenarios() {
        // The `--fault-injection` flag must genuinely change what runs. Assert it
        // structurally: the fault-gated set is exactly the seeded-fault disk and the
        // fault-injected recovery sweeps, and turning the flag on adds exactly those.
        // (A digest-inequality check would be subtly flaky — different folds can
        // collide in principle — so we assert coverage, not the digest value.)
        let mut gated: Vec<&str> = registry()
            .iter()
            .filter(|s| s.requires_fault_injection())
            .map(|s| s.name())
            .collect();
        gated.sort_unstable();
        assert_eq!(
            gated,
            [
                "engine-flush-recover-faults",
                "engine-recover-faults",
                "fault-disk",
                "group-commit-recover-faults",
                "index-build-crash",
                "txn-commit-rollback-faults",
                "vectorized-exec-faults",
                "wal-fsync-poison",
            ],
            "exactly the fault-injected scenarios are fault-gated"
        );
        let on = sweep(4, true);
        let off = sweep(4, false);
        assert_eq!(
            on.scenarios,
            off.scenarios + gated.len(),
            "fault injection adds exactly the fault-gated scenarios"
        );
    }

    #[test]
    fn replay_wires_each_scenario_to_its_digest_function() {
        // `--seed K` must reproduce exactly what the sweep saw — each registered
        // scenario yields the same digest a direct call to its function does.
        let seed = 42;
        let replayed: std::collections::BTreeMap<&str, u64> = replay(seed).into_iter().collect();
        assert_eq!(
            replayed.len(),
            registry().len(),
            "replay covers every scenario"
        );
        assert_eq!(replayed["storage"], run_storage_seed(seed));
        assert_eq!(replayed["snapshot-scan"], run_snapshot_scan_seed(seed));
        assert_eq!(replayed["as-of-oracle"], run_as_of_oracle_seed(seed));
        assert_eq!(replayed["si-provenance"], run_si_oracle_seed(seed));
        assert_eq!(replayed["engine-recover"], run_engine_recover_seed(seed));
        assert_eq!(
            replayed["engine-recover-faults"],
            run_engine_recover_faults_seed(seed)
        );
        assert_eq!(
            replayed["group-commit-recover-faults"],
            run_group_commit_recover_faults_seed(seed)
        );
        assert_eq!(
            replayed["txn-commit-rollback-faults"],
            run_txn_commit_rollback_faults_seed(seed)
        );
        assert_eq!(
            replayed["vectorized-exec-faults"],
            run_vectorized_exec_faults_seed(seed)
        );
        assert_eq!(
            replayed["wal-fsync-poison"],
            run_wal_fsync_poison_seed(seed)
        );
        assert_eq!(replayed["schedule"], run_schedule_seed_digest(seed));
        assert_eq!(replayed["fault-disk"], run_fault_seed(seed));
    }
}
