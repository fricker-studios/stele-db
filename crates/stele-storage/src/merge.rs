//! Cross-tier read merge — fold the delta tier and sealed segments into one
//! resolved view ([architecture §3.5](../../../docs/02-architecture.md#35-read-path--as-of-flow)).
//!
//! A snapshot read sees both tiers: recent versions still staged in the
//! [`Delta`](crate::delta::Delta) tier and older ones flushed into sealed
//! segments. STL-127 adds a third ingredient — a [`CloseMarker`] in the delta
//! tier that closes a version whose body lives in an already-sealed segment.
//! Resolving a key means **folding** any such marker onto its target before
//! picking the version live at the snapshot.
//!
//! This module is pure: it takes already-read versions and markers and returns
//! the resolved chains. It does no I/O and holds no tier handles, so it stays
//! trivially deterministic ([architecture §12 invariant 7](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants))
//! and reusable by both the write-path resolver (in [`crate::systime`]) and the
//! eventual query executor. It is distinct from *compaction* merge, which
//! rewrites segments; this never mutates anything.

use std::collections::BTreeMap;

use stele_common::time::SystemTimeMicros;

use crate::delta::{BusinessKey, CloseMarker, Snapshot, Version};

/// Overlay one close marker onto its target version: stamp the new `sys_to` and
/// record the closing transaction's provenance. The target's body — payload and
/// birth provenance — is never touched (corrections append, never rewrite,
/// [STL-118]). Only an *open* target is closed; a marker whose target is already
/// closed is inert, which keeps a replayed or duplicated marker harmless.
fn apply_marker(target: &mut Version, marker: &CloseMarker) {
    if target.sys_to == stele_common::time::SYSTEM_TIME_OPEN {
        target.sys_to = marker.sys_to;
        target.closed_by = Some(marker.closed_by.clone());
    }
}

/// Fold a pool of versions (from any tier) and the delta tier's close markers
/// into per-key version chains, applying each marker to the matching
/// `(business_key, sys_from)` version.
///
/// `versions` should be the union of every sealed segment's rows and the delta
/// tier's staged versions for the key range of interest; ordering does not
/// matter. When two tiers carry the same `(business_key, sys_from)` (which the
/// flush path does not normally produce), the later one in iteration order
/// wins — pass the delta versions last so freshly-staged state supersedes a
/// stale segment copy. A marker with no matching version in the pool is
/// ignored: its target segment was not part of this read.
#[must_use]
pub fn fold_chains(
    versions: impl IntoIterator<Item = Version>,
    markers: impl IntoIterator<Item = CloseMarker>,
) -> BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>> {
    let mut chains: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>> = BTreeMap::new();
    for v in versions {
        chains
            .entry(v.business_key.clone())
            .or_default()
            .insert(v.sys_from, v);
    }
    for m in markers {
        if let Some(target) = chains
            .get_mut(&m.business_key)
            .and_then(|chain| chain.get_mut(&m.sys_from))
        {
            apply_marker(target, &m);
        }
    }
    chains
}

/// Resolve a snapshot read over already-folded `chains`.
///
/// For each key, returns the version whose `[sys_from, sys_to)` interval
/// contains `snapshot`, in key order. Mirrors the delta tier's per-tier
/// resolver, but over the merged view.
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

/// Where a key's currently-open version lives, as resolved across tiers — the
/// signal the write path needs to choose between closing in the delta tier and
/// appending a [`CloseMarker`] ([STL-127]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveLocation {
    /// The open version is still staged in the delta tier; its period can be
    /// closed in place by re-staging it (the original [STL-91] path).
    Delta(Version),
    /// The open version has been flushed into a sealed segment; closing it must
    /// be an appended [`CloseMarker`], never a mutation (invariant 1).
    Sealed(Version),
}

impl LiveLocation {
    /// The resolved open version, regardless of which tier holds its body.
    #[must_use]
    pub const fn version(&self) -> &Version {
        match self {
            Self::Delta(v) | Self::Sealed(v) => v,
        }
    }
}

/// Resolve the version of `key` open at `at`, reporting **which tier** holds it.
///
/// Folds the delta tier's staged versions, its close markers, and the sealed
/// segments' versions into one chain, then picks the live version — and reports
/// whether its body lives in the delta tier or a sealed segment.
///
/// `delta_versions` are the key's staged candidates ([`Delta::candidate_versions`](crate::delta::Delta::candidate_versions));
/// `markers` the delta tier's close markers; `sealed_versions` the key's rows
/// from every relevant sealed segment. A delta version supersedes a sealed one
/// at the same `sys_from` (the flush path does not produce that overlap, but
/// preferring the delta keeps a mid-flush view consistent). Markers fold over
/// whichever tier holds the target, then the greatest `sys_from ≤ at` with
/// `sys_to > at` is the live version.
///
/// `at` is a freshly-allocated commit timestamp on the write path, strictly
/// greater than every `sys_from` on the chain, so the open version (if any) is
/// always the one returned.
#[must_use]
pub fn resolve_open(
    delta_versions: &[Version],
    markers: &[CloseMarker],
    sealed_versions: &[Version],
    key: &BusinessKey,
    at: Snapshot,
) -> Option<LiveLocation> {
    // `sys_from → (version, sealed?)`. Sealed first, then delta, so a delta
    // version wins a same-`sys_from` collision.
    let mut chain: BTreeMap<SystemTimeMicros, (Version, bool)> = BTreeMap::new();
    for v in sealed_versions.iter().filter(|v| &v.business_key == key) {
        chain.insert(v.sys_from, (v.clone(), true));
    }
    for v in delta_versions.iter().filter(|v| &v.business_key == key) {
        chain.insert(v.sys_from, (v.clone(), false));
    }
    for m in markers.iter().filter(|m| &m.business_key == key) {
        if let Some((target, _sealed)) = chain.get_mut(&m.sys_from) {
            apply_marker(target, m);
        }
    }
    let (_, (version, sealed)) = chain.range(..=at.0).next_back()?;
    if version.sys_to > at.0 {
        let version = version.clone();
        Some(if *sealed {
            LiveLocation::Sealed(version)
        } else {
            LiveLocation::Delta(version)
        })
    } else {
        None
    }
}

/// The version of one key's chain live at `snapshot`: greatest `sys_from ≤ s`
/// whose `sys_to > s`. Shared by [`resolve_snapshot`].
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

    fn open(key: &[u8], sys_from: i64) -> Version {
        Version {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            sys_to: SYSTEM_TIME_OPEN,
            provenance: Provenance::new(
                TxnId(1),
                SystemTimeMicros(sys_from),
                Principal::new(b"birth".to_vec()),
            ),
            closed_by: None,
            payload: b"body".to_vec(),
        }
    }

    fn marker(key: &[u8], sys_from: i64, sys_to: i64) -> CloseMarker {
        CloseMarker {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            sys_to: SystemTimeMicros(sys_to),
            closed_by: Provenance::new(
                TxnId(2),
                SystemTimeMicros(sys_to),
                Principal::new(b"closer".to_vec()),
            ),
        }
    }

    #[test]
    fn marker_folds_onto_a_sealed_open_version() {
        // The sealed version is open; the delta marker closes it. The folded
        // chain must carry the body from the segment and the close from the
        // marker — no mutation of either input.
        let sealed = vec![open(b"k", 10)];
        let chains = fold_chains(sealed, vec![marker(b"k", 10, 20)]);
        let chain = &chains[&BusinessKey::new(b"k".to_vec())];
        let v = &chain[&SystemTimeMicros(10)];
        assert_eq!(v.sys_to, SystemTimeMicros(20), "marker closes the interval");
        assert_eq!(v.payload, b"body", "body is preserved from the segment");
        assert_eq!(v.provenance.principal, Principal::new(b"birth".to_vec()));
        assert_eq!(
            v.closed_by.as_ref().unwrap().principal,
            Principal::new(b"closer".to_vec()),
            "close provenance comes from the marker",
        );
    }

    #[test]
    fn orphan_marker_without_its_target_is_ignored() {
        // A marker whose target segment was not part of this read leaves the
        // pool untouched rather than fabricating a version.
        let chains = fold_chains(vec![open(b"k", 10)], vec![marker(b"other", 5, 7)]);
        assert!(!chains.contains_key(&BusinessKey::new(b"other".to_vec())));
        assert_eq!(chains.len(), 1);
    }

    #[test]
    fn resolve_open_reports_sealed_when_the_body_is_in_a_segment() {
        // Sealed open version, no delta staging → the writer must learn the body
        // is sealed so it appends a marker instead of re-staging.
        let live = resolve_open(
            &[],
            &[],
            &[open(b"k", 10)],
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        );
        assert!(matches!(live, Some(LiveLocation::Sealed(_))));
        assert_eq!(live.unwrap().version().sys_from, SystemTimeMicros(10));
    }

    #[test]
    fn resolve_open_reports_delta_when_the_body_is_staged() {
        let live = resolve_open(
            &[open(b"k", 10)],
            &[],
            &[],
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        );
        assert!(matches!(live, Some(LiveLocation::Delta(_))));
    }

    #[test]
    fn resolve_open_is_none_once_a_marker_closes_the_sealed_version() {
        // Sealed version closed by a delta marker at 20 → nothing is live at a
        // later snapshot, so a re-insert (not an update) is the correct next op.
        let live = resolve_open(
            &[],
            &[marker(b"k", 10, 20)],
            &[open(b"k", 10)],
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        );
        assert_eq!(live, None);
    }

    #[test]
    fn resolve_open_picks_the_newest_open_across_two_segments() {
        // Two flushes: v0 sealed-and-marker-closed, v1 sealed-open. The live
        // version is v1, and it is reported sealed (its body is in segment 2).
        let live = resolve_open(
            &[],
            &[marker(b"k", 10, 20)],
            &[open(b"k", 10), open(b"k", 20)],
            &BusinessKey::new(b"k".to_vec()),
            Snapshot(SystemTimeMicros(100)),
        );
        match live {
            Some(LiveLocation::Sealed(v)) => assert_eq!(v.sys_from, SystemTimeMicros(20)),
            other => panic!("expected sealed v1, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_resolution_sees_the_folded_close() {
        let chains = fold_chains(
            vec![open(b"k", 10), open(b"k", 20)],
            vec![marker(b"k", 10, 20)],
        );
        // Before the close: v0 live. After: v1 live.
        let before = resolve_snapshot(&chains, Snapshot(SystemTimeMicros(15)));
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].sys_from, SystemTimeMicros(10));
        let after = resolve_snapshot(&chains, Snapshot(SystemTimeMicros(25)));
        assert_eq!(after[0].sys_from, SystemTimeMicros(20));
    }
}
