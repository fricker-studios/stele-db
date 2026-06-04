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

use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::backend::{Disk, DiskFile, FaultOp, Faults, MemDisk};
use stele_storage::delta::{BusinessKey, Version};
use stele_storage::segment::{SegmentReader, SegmentWriter};

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
            writer
                .push(Version {
                    business_key: BusinessKey::new(key),
                    sys_from: SystemTimeMicros(rng.next_i64_nonneg() % 1_000_000),
                    sys_to: SYSTEM_TIME_OPEN,
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
            digest = fnv1a(digest, &v.payload);
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
}
