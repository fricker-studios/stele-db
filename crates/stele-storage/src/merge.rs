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

/// Overlay the [`ValidityIndex`] onto one chain of a single key's versions:
/// stamp each version's `sys_to` / `closed_by` from the index's materialized
/// close, leaving a version with no index entry **open**
/// ([`SYSTEM_TIME_OPEN`](stele_common::time::SYSTEM_TIME_OPEN) / `None`). The
/// version body — payload and birth provenance — is never touched (corrections
/// append, never rewrite, [STL-118]).
///
/// `closes` is the key's materialized ends, keyed by `sys_from`
/// ([`ValidityIndex::closes_for`]).
fn overlay_chain(
    chain: &mut BTreeMap<SystemTimeMicros, Version>,
    closes: &BTreeMap<SystemTimeMicros, crate::validity::ClosedInterval>,
) {
    for (sys_from, version) in chain.iter_mut() {
        if let Some(interval) = closes.get(sys_from) {
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
/// matter. When two tiers carry the same `(business_key, sys_from)` (which the
/// flush path does not normally produce), the later one in iteration order
/// wins — pass the delta versions last so freshly-staged state supersedes a
/// stale segment copy.
///
/// The index is **materialized once** ([`ValidityIndex::materialize`]) — a single
/// pass over its spills — and then overlaid in memory, so a fold over `K` keys
/// costs O(spills) reads, not O(K × spills). Each key's entries are a contiguous
/// `(business_key, sys_from)` run, so the overlay range-scans just that run.
///
/// # Errors
///
/// [`ValidityError`] if a backing spill of the index cannot be read.
pub fn fold_chains<D: Disk>(
    versions: impl IntoIterator<Item = Version>,
    index: &ValidityIndex<D>,
) -> Result<BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>>, ValidityError> {
    let mut chains: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>> = BTreeMap::new();
    for v in versions {
        chains
            .entry(v.business_key.clone())
            .or_default()
            .insert(v.sys_from, v);
    }
    let closes = index.materialize()?;
    for (key, chain) in &mut chains {
        let lo = (key.clone(), SystemTimeMicros(i64::MIN));
        let hi = (key.clone(), SystemTimeMicros(i64::MAX));
        for ((_, sys_from), interval) in closes.range((Included(lo), Included(hi))) {
            if let Some(version) = chain.get_mut(sys_from) {
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
pub fn resolve_snapshot(
    chains: &BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>>,
    snapshot: Snapshot,
) -> Vec<Version> {
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
/// one chain (a delta version supersedes a sealed one at the same `sys_from` —
/// the flush path does not produce that overlap, but preferring the delta keeps
/// a mid-flush view consistent), overlays the index's materialized closes, then
/// returns the greatest `sys_from ≤ at` whose `sys_to > at`.
///
/// Unlike pre-[ADR-0023] code, the result no longer distinguishes *which tier*
/// holds the live version: a close is always a write-once append to the validity
/// index ([`ValidityIndex::insert_close`]), regardless of where the version body
/// lives, so the writer only needs the open version's `sys_from`.
///
/// `at` is a freshly-allocated commit timestamp on the write path, strictly
/// greater than every `sys_from` on the chain, so the open version (if any) is
/// always the one returned.
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
    let mut chain: BTreeMap<SystemTimeMicros, Version> = BTreeMap::new();
    for v in sealed_versions.iter().filter(|v| &v.business_key == key) {
        chain.insert(v.sys_from, v.clone());
    }
    for v in delta_versions.iter().filter(|v| &v.business_key == key) {
        chain.insert(v.sys_from, v.clone());
    }
    let closes = index.closes_for(key)?;
    overlay_chain(&mut chain, &closes);
    Ok(live_in_chain(&chain, at).cloned())
}

/// The version of one key's chain live at `snapshot`: greatest `sys_from ≤ s`
/// whose `sys_to > s`. Shared by [`resolve_snapshot`] and [`resolve_open`];
/// the chain must already carry its index overlay.
fn live_in_chain(
    chain: &BTreeMap<SystemTimeMicros, Version>,
    snapshot: Snapshot,
) -> Option<&Version> {
    chain
        .range(..=snapshot.0)
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
        let v = &chain[&SystemTimeMicros(10)];
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
        let v = &chains[&BusinessKey::new(b"k".to_vec())][&SystemTimeMicros(10)];
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
}
