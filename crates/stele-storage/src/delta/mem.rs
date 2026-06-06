//! In-memory sorted store for the delta tier.
//!
//! Layout: per business key, an ordered map keyed by `sys_from`. This keeps
//! a key's version chain physically contiguous and makes snapshot resolution
//! a single `range(..=snapshot).next_back()` per key
//! ([architecture §3.1](../../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving)).
//!
//! The tier holds **raw** versions only — it does not store or resolve `sys_to`
//! ([ADR-0023]). Snapshot resolution (overlaying each version's end from the
//! [validity index](crate::validity) and picking the one live at `s`) happens a
//! layer up in [`super::Delta::range_scan`] / [`crate::merge`]; this store just
//! supplies the key-ordered candidates.

use std::collections::BTreeMap;

use stele_common::time::SystemTimeMicros;

use super::version::{BusinessKey, Version};

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::provenance::{Principal, Provenance, TxnId};

    fn v(key: &[u8], sys_from: i64, payload: &[u8]) -> Version {
        Version::open(
            BusinessKey::new(key.to_vec()),
            SystemTimeMicros(sys_from),
            0,
            Provenance::new(
                TxnId(u64::try_from(sys_from).unwrap_or(0)),
                SystemTimeMicros(sys_from),
                Principal::new(b"tester".to_vec()),
            ),
            payload.to_vec(),
        )
    }

    #[test]
    fn idempotent_insert_does_not_double_count_bytes() {
        let mut tier = MemTier::new();
        let row = v(b"k", 1, b"payload");
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
        tier.insert(v(b"b", 0, b"x"));
        tier.insert(v(b"a", 1, b"y"));
        tier.insert(v(b"a", 0, b"z"));

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
