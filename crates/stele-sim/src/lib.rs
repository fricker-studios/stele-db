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
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros, ValidTimeMicros};
use stele_storage::backend::{Disk, DiskFile, FaultOp, Faults, MemDisk};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Version};
use stele_storage::segment::{SegmentReader, SegmentWriter};
use stele_storage::systime::SysTimeWriter;
use stele_storage::validtime::{ValidInterval, ValidTimeWriter, unframe_payload};

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
            // deterministically ([STL-93]).
            let sys_from = rng.next_i64_nonneg() % 1_000_000;
            let txn_id = u64::try_from(rng.next_i64_nonneg()).unwrap_or(0);
            let principal_len = rng.below_usize(8);
            let principal = rng.bytes(principal_len);
            // Roughly half the rows are *closed* versions carrying a second
            // (close) provenance group, so the round-trip exercises the v3
            // close-provenance columns deterministically ([STL-118]). A closed
            // version's `sys_to` and `closed_at` are real values below the open
            // sentinel; an open one keeps `SYSTEM_TIME_OPEN` and `None`.
            let (sys_to, closed_by) = if rng.below(2) == 0 {
                let closed_at = sys_from + 1 + (rng.next_i64_nonneg() % 1_000);
                let closer_txn = u64::try_from(rng.next_i64_nonneg()).unwrap_or(0);
                let closer_principal_len = rng.below_usize(8);
                let closer_principal = rng.bytes(closer_principal_len);
                (
                    SystemTimeMicros(closed_at),
                    Some(Provenance::new(
                        TxnId(closer_txn),
                        SystemTimeMicros(closed_at),
                        Principal::new(closer_principal),
                    )),
                )
            } else {
                (SYSTEM_TIME_OPEN, None)
            };
            writer
                .push(Version {
                    business_key: BusinessKey::new(key),
                    sys_from: SystemTimeMicros(sys_from),
                    sys_to,
                    provenance: Provenance::new(
                        TxnId(txn_id),
                        SystemTimeMicros(sys_from),
                        Principal::new(principal),
                    ),
                    closed_by,
                    payload,
                })
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
            digest = fnv1a(digest, v.business_key.as_bytes());
            digest = fnv1a(digest, &v.sys_from.0.to_le_bytes());
            digest = fnv1a(digest, &v.sys_to.0.to_le_bytes());
            digest = fnv1a(digest, &v.provenance.txn_id.0.to_le_bytes());
            digest = fnv1a(digest, &v.provenance.committed_at.0.to_le_bytes());
            digest = fnv1a(digest, v.provenance.principal.as_bytes());
            digest = fold_closed_by(digest, v.closed_by.as_ref());
            digest = fnv1a(digest, &v.payload);
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
            // (close, no re-open) — both record `principal` as the closer.
            if rng.below(2) == 0 {
                writer
                    .delete(&mut delta, &key, txn, principal)
                    .expect("delete");
                live[k] = false;
            } else {
                let payload_len = rng.below_usize(16);
                let payload = rng.bytes(payload_len);
                writer
                    .update(&mut delta, key, payload, txn, principal)
                    .expect("update");
            }
        } else {
            let payload_len = rng.below_usize(16);
            let payload = rng.bytes(payload_len);
            writer
                .insert(&mut delta, key, payload, txn, principal)
                .expect("insert");
            live[k] = true;
        }
    }

    // `flush_to_segment` drains in `(business_key, sys_from)` order — deterministic.
    let mut digest = FNV_OFFSET;
    for v in delta.flush_to_segment().expect("flush") {
        digest = fnv1a(digest, v.business_key.as_bytes());
        digest = fnv1a(digest, &v.sys_from.0.to_le_bytes());
        digest = fnv1a(digest, &v.sys_to.0.to_le_bytes());
        digest = fnv1a(digest, &v.provenance.txn_id.0.to_le_bytes());
        digest = fnv1a(digest, &v.provenance.committed_at.0.to_le_bytes());
        digest = fnv1a(digest, v.provenance.principal.as_bytes());
        digest = fold_closed_by(digest, v.closed_by.as_ref());
        digest = fnv1a(digest, &v.payload);
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
}
