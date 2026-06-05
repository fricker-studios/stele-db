//! In-memory sorted store for the delta tier.
//!
//! Layout: per business key, an ordered map keyed by `sys_from`. This keeps
//! a key's version chain physically contiguous and makes snapshot resolution
//! a single `range(..=snapshot).next_back()` per key
//! ([architecture §3.1](../../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving)).
//!
//! Snapshot semantics ([architecture §3.5](../../../../docs/02-architecture.md#35-read-path--as-of-flow)):
//! at snapshot `s`, the live version for a key is the one whose `sys` interval
//! contains `s` — i.e. the greatest `sys_from ≤ s` *and* `sys_to > s`. Closed
//! periods (`sys_to ≤ s`) are skipped, so a key with no live version at `s`
//! simply does not appear in the scan output.

use std::collections::BTreeMap;
use std::ops::RangeBounds;

use stele_common::time::SystemTimeMicros;

use super::version::{BusinessKey, Snapshot, Version};

/// In-memory portion of the delta tier.
#[derive(Debug, Default)]
pub(super) struct MemTier {
    /// `business_key → (sys_from → version)`. The outer map gives ordered
    /// range scans over keys; the inner gives ordered access to a single
    /// key's version chain.
    rows: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>>,
    /// Running sum of `Version::encoded_size()` across every row. The spill
    /// decision is taken against this counter so the in-memory and on-spill
    /// thresholds describe the same units.
    byte_size: u64,
}

impl MemTier {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Insert a version. Replaying the same `(business_key, sys_from)` twice
    /// is idempotent — the second insert overwrites the first and the byte
    /// counter is adjusted, so WAL replay never double-counts a record.
    pub(super) fn insert(&mut self, version: Version) {
        let added = version.encoded_size() as u64;
        let chain = self.rows.entry(version.business_key.clone()).or_default();
        if let Some(prev) = chain.insert(version.sys_from, version) {
            // Replace: subtract the displaced row's contribution.
            self.byte_size = self.byte_size.saturating_sub(prev.encoded_size() as u64);
        }
        self.byte_size = self.byte_size.saturating_add(added);
    }

    /// Total encoded bytes currently held in memory.
    pub(super) const fn byte_size(&self) -> u64 {
        self.byte_size
    }

    /// Number of distinct versions in memory.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.rows.values().map(BTreeMap::len).sum()
    }

    /// Iterate every stored version in `(business_key, sys_from)` order —
    /// the input ordering a segment writer expects.
    pub(super) fn iter(&self) -> impl Iterator<Item = &Version> {
        self.rows.values().flat_map(|chain| chain.values())
    }

    /// Drain every stored version in `(business_key, sys_from)` order and
    /// reset state. Used by both `flush_to_segment` and the spill path.
    pub(super) fn drain_sorted(&mut self) -> Vec<Version> {
        let rows = std::mem::take(&mut self.rows);
        self.byte_size = 0;
        rows.into_values().flat_map(BTreeMap::into_values).collect()
    }

    /// Snapshot read across a key range. For each business key in the
    /// supplied range, emit the version whose system-time interval contains
    /// `snapshot`, if any. Output is sorted by business key.
    ///
    /// The range can be any standard Rust [`RangeBounds`] over `BusinessKey`;
    /// pass `..` for "all keys".
    pub(super) fn range_scan<R>(&self, key_range: R, snapshot: Snapshot) -> Vec<Version>
    where
        R: RangeBounds<BusinessKey>,
    {
        let mut out = Vec::new();
        for (_, chain) in self.rows.range(key_range) {
            if let Some(v) = live_version_at(chain, snapshot) {
                out.push(v.clone());
            }
        }
        out
    }
}

/// Pick the version of one key that is live at `snapshot`, if any.
///
/// "Live at `s`" means `sys_from ≤ s < sys_to`. Searching by `range(..=s)`
/// and taking the last element naturally finds the greatest `sys_from ≤ s`;
/// the `sys_to > s` check then rejects a version whose period has already
/// been closed by the time the snapshot wants to look at it.
fn live_version_at(
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

    fn v(key: &[u8], sys_from: i64, sys_to: SystemTimeMicros, payload: &[u8]) -> Version {
        Version {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            sys_to,
            provenance: Provenance::new(
                TxnId(u64::try_from(sys_from).unwrap_or(0)),
                SystemTimeMicros(sys_from),
                Principal::new(b"tester".to_vec()),
            ),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn snapshot_picks_latest_live_version_per_key() {
        let mut tier = MemTier::new();
        // Key "a": three versions. The middle one is closed at sys=20, the
        // last one opens at sys=20 and is still open.
        tier.insert(v(b"a", 0, SystemTimeMicros(10), b"v0"));
        tier.insert(v(b"a", 10, SystemTimeMicros(20), b"v1"));
        tier.insert(v(b"a", 20, SYSTEM_TIME_OPEN, b"v2"));

        // At s=5, v0 is live; at s=15, v1; at s=25, v2.
        let s5 = tier.range_scan(.., Snapshot(SystemTimeMicros(5)));
        assert_eq!(s5.len(), 1);
        assert_eq!(s5[0].payload, b"v0");

        let s15 = tier.range_scan(.., Snapshot(SystemTimeMicros(15)));
        assert_eq!(s15[0].payload, b"v1");

        let s25 = tier.range_scan(.., Snapshot(SystemTimeMicros(25)));
        assert_eq!(s25[0].payload, b"v2");
    }

    #[test]
    fn closed_period_at_exact_snapshot_is_excluded() {
        // sys_to is exclusive: a version with [0, 10) is NOT live at s=10.
        let mut tier = MemTier::new();
        tier.insert(v(b"k", 0, SystemTimeMicros(10), b"old"));
        let live = tier.range_scan(.., Snapshot(SystemTimeMicros(10)));
        assert!(live.is_empty(), "[0,10) must not be live at s=10");
    }

    #[test]
    fn key_with_no_live_version_is_omitted() {
        // Only insertion is a closed period — at a later snapshot the key
        // simply isn't part of the output.
        let mut tier = MemTier::new();
        tier.insert(v(b"k", 0, SystemTimeMicros(10), b"old"));
        let live = tier.range_scan(.., Snapshot(SystemTimeMicros(100)));
        assert!(live.is_empty());
    }

    #[test]
    fn range_scan_is_sorted_and_respects_bounds() {
        let mut tier = MemTier::new();
        for k in [b"d", b"a", b"c", b"b"] {
            tier.insert(v(k, 0, SYSTEM_TIME_OPEN, k));
        }
        let all = tier.range_scan(.., Snapshot(SystemTimeMicros(5)));
        let keys: Vec<&[u8]> = all.iter().map(|v| v.business_key.as_bytes()).collect();
        assert_eq!(keys, vec![&b"a"[..], &b"b"[..], &b"c"[..], &b"d"[..]]);

        // Half-open range [b, d).
        let mid = tier.range_scan(
            BusinessKey::new(b"b".to_vec())..BusinessKey::new(b"d".to_vec()),
            Snapshot(SystemTimeMicros(5)),
        );
        let mid_keys: Vec<&[u8]> = mid.iter().map(|v| v.business_key.as_bytes()).collect();
        assert_eq!(mid_keys, vec![&b"b"[..], &b"c"[..]]);
    }

    #[test]
    fn idempotent_insert_does_not_double_count_bytes() {
        let mut tier = MemTier::new();
        let row = v(b"k", 1, SYSTEM_TIME_OPEN, b"payload");
        let size = row.encoded_size() as u64;
        tier.insert(row.clone());
        tier.insert(row);
        assert_eq!(
            tier.byte_size(),
            size,
            "replaying the same (key, sys_from) keeps byte_size flat"
        );
        assert_eq!(tier.len(), 1);
    }

    #[test]
    fn drain_returns_sorted_and_clears_byte_size() {
        let mut tier = MemTier::new();
        tier.insert(v(b"b", 0, SYSTEM_TIME_OPEN, b"x"));
        tier.insert(v(b"a", 1, SYSTEM_TIME_OPEN, b"y"));
        tier.insert(v(b"a", 0, SYSTEM_TIME_OPEN, b"z"));

        let drained = tier.drain_sorted();
        assert_eq!(drained.len(), 3);
        // Order: (a, 0), (a, 1), (b, 0).
        assert_eq!(drained[0].business_key.as_bytes(), b"a");
        assert_eq!(drained[0].sys_from, SystemTimeMicros(0));
        assert_eq!(drained[1].business_key.as_bytes(), b"a");
        assert_eq!(drained[1].sys_from, SystemTimeMicros(1));
        assert_eq!(drained[2].business_key.as_bytes(), b"b");

        assert_eq!(tier.byte_size(), 0);
        assert_eq!(tier.len(), 0);
    }
}
