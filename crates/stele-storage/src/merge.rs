//! Cross-tier read merge — fold the delta tier, sealed segments, and the
//! validity index into one resolved view
//! ([architecture §3.5](../../../docs/02-architecture.md#35-read-path--as-of-flow)).
//!
//! A snapshot read sees versions from two tiers — recent ones still staged in the
//! [`Delta`](crate::delta::Delta) tier and older ones flushed into sealed
//! segments — plus the [`ValidityIndex`], which
//! supplies each version's system-time **end** (`sys_to`) and closing provenance.
//! Records never store `sys_to` ([ADR-0023](../../../docs/adr/0023-append-only-record-model-validity-index.md));
//! resolving a key means **overlaying** the index's materialized close onto each
//! candidate version before picking the one live at the snapshot.
//!
//! This module reads the index (deterministic [`Disk`] I/O)
//! but holds no tier handles of its own and no wall-clock or runtime dependency,
//! so it stays deterministic ([architecture §12 invariant 7](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants))
//! and reusable by both the write-path resolver (in [`crate::systime`]) and the
//! eventual query executor. It is distinct from *compaction* merge, which rewrites
//! segments; this never mutates anything.

use std::collections::BTreeMap;
use std::ops::Bound::Included;

use stele_common::time::SystemTimeMicros;

use crate::backend::Disk;
use crate::delta::{BusinessKey, Snapshot, Version};
use crate::validity::{ValidityError, ValidityIndex};

/// One key's version chain, keyed by `(sys_from, seq)`.
///
/// `(sys_from, seq)` is the total order a same-tick supersession depends on
/// ([ADR-0024], STL-145): the `seq` keeps two versions that share a `sys_from`
/// from colliding in the map.
pub type VersionChain = BTreeMap<(SystemTimeMicros, u64), Version>;

/// A pool of per-key [`VersionChain`]s, keyed by business key — the folded,
/// index-overlaid view a snapshot read resolves against.
pub type KeyChains = BTreeMap<BusinessKey, VersionChain>;

/// Overlay the [`ValidityIndex`] onto one chain of a single key's versions:
/// stamp each version's `sys_to` / `closed_by` from the index's materialized
/// close, leaving a version with no index entry **open**
/// ([`SYSTEM_TIME_OPEN`](stele_common::time::SYSTEM_TIME_OPEN) / `None`). The
/// version body — payload and birth provenance — is never touched (corrections
/// append, never rewrite, [STL-118]).
///
/// `closes` is the key's materialized ends, keyed by `(sys_from, seq)`
/// ([`ValidityIndex::closes_for`]) — the `seq` distinguishes two versions of one
/// key that share a `sys_from` (STL-145, [ADR-0024]).
fn overlay_chain(
    chain: &mut VersionChain,
    closes: &BTreeMap<(SystemTimeMicros, u64), crate::validity::ClosedInterval>,
) {
    for (key, version) in chain.iter_mut() {
        if let Some(interval) = closes.get(key) {
            version.sys_to = interval.sys_to;
            version.closed_by = Some(interval.closed_by.clone());
        }
    }
}

/// Fold a pool of versions into per-key chains, overlaying the [`ValidityIndex`].
///
/// Each version is grouped by key, then each key's materialized closes are
/// overlaid so every version carries its resolved `[sys_from, sys_to)` interval.
///
/// `versions` should be the union of every sealed segment's rows and the delta
/// tier's staged versions for the key range of interest; ordering does not
/// matter. When two tiers carry the same `(business_key, sys_from, seq)` (which
/// the flush path does not normally produce), the later one in iteration order
/// wins — pass the delta versions last so freshly-staged state supersedes a
/// stale segment copy.
///
/// The index closes are gathered by [`ValidityIndex::closes_for_keys`], which
/// reads only the spills that may hold one of the fold's keys when that is fewer
/// than a full sweep, and otherwise materializes once — so a small / point fold
/// against a spilled index does work sub-linear in the total spilled closes
/// rather than scanning every spill ([STL-142]). Each key's entries are a
/// contiguous `(business_key, sys_from, seq)` run, so the overlay range-scans
/// just that run.
///
/// # Errors
///
/// [`ValidityError`] if a backing spill of the index cannot be read.
pub fn fold_chains<D: Disk>(
    versions: impl IntoIterator<Item = Version>,
    index: &ValidityIndex<D>,
) -> Result<KeyChains, ValidityError> {
    let mut chains: KeyChains = BTreeMap::new();
    for v in versions {
        chains
            .entry(v.business_key.clone())
            .or_default()
            .insert((v.sys_from, v.seq), v);
    }
    let keys: std::collections::BTreeSet<BusinessKey> = chains.keys().cloned().collect();
    let closes = index.closes_for_keys(&keys)?;
    for (key, chain) in &mut chains {
        let lo = (key.clone(), SystemTimeMicros(i64::MIN), u64::MIN);
        let hi = (key.clone(), SystemTimeMicros(i64::MAX), u64::MAX);
        for ((_, sys_from, seq), interval) in closes.range((Included(lo), Included(hi))) {
            if let Some(version) = chain.get_mut(&(*sys_from, *seq)) {
                version.sys_to = interval.sys_to;
                version.closed_by = Some(interval.closed_by.clone());
            }
        }
    }
    Ok(chains)
}

/// Resolve a snapshot read over already-folded `chains`.
///
/// For each key, returns the version whose `[sys_from, sys_to)` interval
/// contains `snapshot`, in key order. Mirrors the delta tier's per-tier
/// resolver, but over the merged + index-overlaid view.
#[must_use]
pub fn resolve_snapshot(chains: &KeyChains, snapshot: Snapshot) -> Vec<Version> {
    chains
        .values()
        .filter_map(|chain| live_in_chain(chain, snapshot).cloned())
        .collect()
}

/// Resolve the version of `key` open at `at`, across the delta tier, the sealed
/// segments, and the validity index — the signal the write path needs to decide
/// whether a key has a live version to close.
///
/// Folds the delta tier's staged candidates and the key's sealed versions into
/// one chain (a delta version supersedes a sealed one at the same
/// `(sys_from, seq)` — the flush path does not produce that overlap, but
/// preferring the delta keeps a mid-flush view consistent), overlays the index's
/// materialized closes, then returns the greatest `(sys_from, seq)` with
/// `sys_from ≤ at` whose `sys_to > at`.
///
/// Unlike pre-[ADR-0023] code, the result no longer distinguishes *which tier*
/// holds the live version: a close is always a write-once append to the validity
/// index ([`ValidityIndex::insert_close`]), regardless of where the version body
/// lives, so the writer only needs the open version's `sys_from`.
///
/// `at` is a freshly-allocated commit timestamp on the write path, `≥` every
/// `sys_from` on the chain. It may now *equal* the newest version's `sys_from`
/// (the writer no longer force-bumps the timestamp, STL-145), so the resolver
/// scans up to `(at, u64::MAX)` — including every `seq` at that tick — and the
/// open version with the greatest `(sys_from, seq)` is the one returned.
///
/// # Errors
///
/// [`ValidityError`] if a backing spill of the index cannot be read.
pub fn resolve_open<D: Disk>(
    delta_versions: &[Version],
    sealed_versions: &[Version],
    index: &ValidityIndex<D>,
    key: &BusinessKey,
    at: Snapshot,
) -> Result<Option<Version>, ValidityError> {
    let mut chain: VersionChain = BTreeMap::new();
    for v in sealed_versions.iter().filter(|v| &v.business_key == key) {
        chain.insert((v.sys_from, v.seq), v.clone());
    }
    for v in delta_versions.iter().filter(|v| &v.business_key == key) {
        chain.insert((v.sys_from, v.seq), v.clone());
    }
    let closes = index.closes_for(key)?;
    overlay_chain(&mut chain, &closes);
    Ok(live_in_chain(&chain, at).cloned())
}

/// The version of one key's chain live at `snapshot`: the greatest
/// `(sys_from, seq)` with `sys_from ≤ s` whose `sys_to > s`. The `seq` upper
/// bound `u64::MAX` makes the scan include every version sharing the snapshot's
/// tick, so the highest-`seq` version at a shared `sys_from` wins the tie
/// ([ADR-0024], STL-145); a same-tick superseded version closes degenerately
/// (`sys_to == sys_from`) and is dropped by the `sys_to > s` filter. Shared by
/// [`resolve_snapshot`] and [`resolve_open`]; the chain must already carry its
/// index overlay.
fn live_in_chain(chain: &VersionChain, snapshot: Snapshot) -> Option<&Version> {
    chain
        .range(..=(snapshot.0, u64::MAX))
        .next_back()
        .map(|(_, v)| v)
        .filter(|v| v.sys_to > snapshot.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::provenance::{Principal, Provenance, TxnId};
    use stele_common::time::SYSTEM_TIME_OPEN;

    use crate::backend::MemDisk;
    use crate::validity::{Close, ValidityConfig, ValidityIndex};

    fn open(key: &[u8], sys_from: i64) -> Version {
        Version::open(
            BusinessKey::new(key.to_vec()),
            SystemTimeMicros(sys_from),
            0,
            Provenance::new(
                TxnId(1),
                SystemTimeMicros(sys_from),
                Principal::new(b"birth".to_vec()),
            ),
            b"body".to_vec(),
        )
    }

    fn index() -> ValidityIndex<MemDisk> {
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open")
    }

    fn close(idx: &mut ValidityIndex<MemDisk>, key: &[u8], sys_from: i64, sys_to: i64) {
        idx.insert_close(Close {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            seq: 0,
            sys_to: SystemTimeMicros(sys_to),
            closed_by: Provenance::new(
                TxnId(2),
                SystemTimeMicros(sys_to),
                Principal::new(b"closer".to_vec()),
            ),
        })
        .expect("close");
    }

    #[test]
    fn index_overlay_closes_a_versions_interval() {
        // The version is read raw (open); the index closes it. The folded chain
        // must carry the body from the version and the close from the index.
        let mut idx = index();
        close(&mut idx, b"k", 10, 20);
        let chains = fold_chains(vec![open(b"k", 10)], &idx).expect("fold");
        let chain = &chains[&BusinessKey::new(b"k".to_vec())];
        let v = &chain[&(SystemTimeMicros(10), 0)];
        assert_eq!(v.sys_to, SystemTimeMicros(20), "index closes the interval");
        assert_eq!(v.payload, b"body", "body is preserved");
        assert_eq!(v.provenance.principal, Principal::new(b"birth".to_vec()));
        assert_eq!(
            v.closed_by.as_ref().unwrap().principal,
            Principal::new(b"closer".to_vec()),
            "close provenance comes from the index",
        );
    }

    #[test]
    fn a_version_with_no_index_entry_stays_open() {
        let idx = index();
        let chains = fold_chains(vec![open(b"k", 10)], &idx).expect("fold");
        let v = &chains[&BusinessKey::new(b"k".to_vec())][&(SystemTimeMicros(10), 0)];
        assert_eq!(v.sys_to, SYSTEM_TIME_OPEN);
        assert!(v.closed_by.is_none());
    }

    #[test]
    fn resolve_open_finds_a_sealed_open_version() {
        // Sealed open version, no delta staging, no close → it is the live one.
        let idx = index();
        let live = resolve_open(
            &[],
            &[open(b"k", 10)],
            &idx,
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        )
        .expect("resolve");
        assert_eq!(live.unwrap().sys_from, SystemTimeMicros(10));
    }

    #[test]
    fn resolve_open_is_none_once_the_index_closes_the_version() {
        // Sealed version closed by the index at 20 → nothing live at a later
        // snapshot, so a re-insert (not an update) is the correct next op.
        let mut idx = index();
        close(&mut idx, b"k", 10, 20);
        let live = resolve_open(
            &[],
            &[open(b"k", 10)],
            &idx,
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        )
        .expect("resolve");
        assert_eq!(live, None);
    }

    #[test]
    fn resolve_open_picks_the_newest_open_across_two_segments() {
        // Two flushes: v0 closed, v1 open. The live version is v1.
        let mut idx = index();
        close(&mut idx, b"k", 10, 20);
        let live = resolve_open(
            &[],
            &[open(b"k", 10), open(b"k", 20)],
            &idx,
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        )
        .expect("resolve");
        assert_eq!(live.unwrap().sys_from, SystemTimeMicros(20));
    }

    #[test]
    fn snapshot_resolution_sees_the_index_close() {
        let mut idx = index();
        close(&mut idx, b"k", 10, 20);
        let chains = fold_chains(vec![open(b"k", 10), open(b"k", 20)], &idx).expect("fold");
        // Before the close: v0 live. After: v1 live.
        let before = resolve_snapshot(&chains, Snapshot(SystemTimeMicros(15)));
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].sys_from, SystemTimeMicros(10));
        let after = resolve_snapshot(&chains, Snapshot(SystemTimeMicros(25)));
        assert_eq!(after[0].sys_from, SystemTimeMicros(20));
    }

    /// Build an open version of `key` at `sys_from` with an explicit `seq`.
    fn open_seq(key: &[u8], sys_from: i64, seq: u64) -> Version {
        Version::open(
            BusinessKey::new(key.to_vec()),
            SystemTimeMicros(sys_from),
            seq,
            Provenance::new(
                TxnId(1),
                SystemTimeMicros(sys_from),
                Principal::new(b"birth".to_vec()),
            ),
            b"body".to_vec(),
        )
    }

    /// Insert a close for `(key, sys_from, seq)`.
    fn close_seq(
        idx: &mut ValidityIndex<MemDisk>,
        key: &[u8],
        sys_from: i64,
        seq: u64,
        sys_to: i64,
    ) {
        idx.insert_close(Close {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            seq,
            sys_to: SystemTimeMicros(sys_to),
            closed_by: Provenance::new(
                TxnId(2),
                SystemTimeMicros(sys_to),
                Principal::new(b"closer".to_vec()),
            ),
        })
        .expect("close");
    }

    #[test]
    fn two_versions_at_one_tick_resolve_to_the_higher_seq() {
        // Same-tick supersession (STL-145): version (10, seq 0) is closed
        // degenerately at 10 by version (10, seq 1), which stays open. Neither
        // version is dropped — the chain is keyed by (sys_from, seq) — and at
        // every snapshot ≥ 10 the live version is the higher-seq one.
        let mut idx = index();
        close_seq(&mut idx, b"k", 10, 0, 10); // [10,10): degenerate, never live
        let chains =
            fold_chains(vec![open_seq(b"k", 10, 0), open_seq(b"k", 10, 1)], &idx).expect("fold");
        let chain = &chains[&BusinessKey::new(b"k".to_vec())];
        assert_eq!(chain.len(), 2, "both versions survive the same-tick fold");

        let at_tick = resolve_snapshot(&chains, Snapshot(SystemTimeMicros(10)));
        assert_eq!(at_tick.len(), 1, "exactly one version live at the tick");
        assert_eq!(at_tick[0].seq, 1, "the higher-seq version wins the tie");
        let later = resolve_snapshot(&chains, Snapshot(SystemTimeMicros(1_000)));
        assert_eq!(later[0].seq, 1);
        // Before the tick: nothing — the key did not yet exist.
        assert!(resolve_snapshot(&chains, Snapshot(SystemTimeMicros(9))).is_empty());
    }

    #[test]
    fn resolve_open_finds_the_open_version_at_a_shared_tick() {
        // The write-path resolver probes at `at == commit`, which may equal the
        // newest version's sys_from once the force-bump is gone. It must still
        // return the open (higher-seq) version, not the degenerately-closed one.
        let mut idx = index();
        close_seq(&mut idx, b"k", 10, 0, 10);
        let live = resolve_open(
            &[open_seq(b"k", 10, 0), open_seq(b"k", 10, 1)],
            &[],
            &idx,
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(10)),
        )
        .expect("resolve");
        assert_eq!(live.expect("a live version").seq, 1);
    }
}
