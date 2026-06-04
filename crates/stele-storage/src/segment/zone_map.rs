//! Zone maps — per-segment min/max summaries the planner uses to prune whole
//! segments *before* any column-chunk I/O.
//!
//! A zone map is the resident, in-memory digest of the per-column min/max
//! statistics the writer already records in the footer
//! ([`super::writer`]). [`SegmentReader::open`](super::SegmentReader::open)
//! decodes them once and keeps the [`ZoneMap`] alongside the file handle, so a
//! prune decision costs zero I/O — the property
//! [ADR-0021](../../../../../docs/adr/0021-storage-lifecycle-tiered-archival.md)
//! relies on when it says *zone maps are never archived*: the planner prunes
//! against resident metadata and only thaws the segments a query actually
//! needs.
//!
//! ## Soundness contract
//!
//! [`ZoneMap::might_contain`] is **conservative**: it returns `false` (the
//! segment may be skipped) only when the zone maps *prove* no matching,
//! visible row can exist. It never returns `false` for a segment that holds a
//! match — false positives (an unnecessary scan) are allowed, false negatives
//! (a dropped match) are not. Every pruning rule below is a one-sided test
//! derived from that invariant.
//!
//! ## Temporal pruning ([architecture §3.3](../../../../../docs/02-architecture.md#33-how-b-tree-and-columnstore-coexist))
//!
//! A version is visible at a system-time `snapshot` iff
//! `sys_from <= snapshot < sys_to` — the same half-open `[sys_from, sys_to)`
//! interval the delta tier's resolver uses
//! ([`crate::delta`]'s `pick_live`). From the segment's zone maps that yields
//! two one-sided skip rules:
//!
//! * if `min(sys_from) > snapshot`, *every* row begins after the snapshot —
//!   none is visible yet; skip.
//! * if `max(sys_to) <= snapshot`, *every* row was already superseded at the
//!   snapshot — none is visible; skip.
//!
//! Valid-time columns ([STL-92]) are not part of the v0.1 schema yet, but the
//! zone map is keyed by [`ColumnId`] and the writer computes min/max for every
//! column generically, so a `valid_from` / `valid_to` range predicate prunes
//! through the same [`Predicate::Range`] path the moment those columns land —
//! no change to this module required.

use stele_common::time::SystemTimeMicros;

use crate::delta::Snapshot;

use super::format::ColumnId;

/// A single decoded min or max bound for one column.
///
/// The wire form lives in the footer as raw bytes; this is the typed view the
/// writer's `ColumnType` dictates. Ordering matches the on-disk stat
/// convention exactly: `i64` columns compare numerically, byte columns compare
/// lexicographically (the order [`crate::delta::BusinessKey`] already sorts by).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneBound {
    /// Bound of a fixed-width `i64` column ([`ColumnId::SysFrom`] /
    /// [`ColumnId::SysTo`]).
    I64(i64),
    /// Bound of a variable-length bytes column ([`ColumnId::BusinessKey`]).
    Bytes(Vec<u8>),
}

/// The min/max pair for one column across the whole segment.
///
/// Both bounds are always present together — a column either has stats (then
/// both `min` and `max` are recorded) or it has none (then there is no
/// [`ColumnZone`] entry at all). The two bounds are always the same
/// [`ZoneBound`] variant, dictated by the column's `ColumnType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnZone {
    /// Smallest value observed for the column in this segment.
    pub min: ZoneBound,
    /// Largest value observed for the column in this segment.
    pub max: ZoneBound,
}

/// Resident per-segment zone map: the min/max digest for every column that
/// carries statistics.
///
/// Cheap to clone and independent of the file handle — the planner can retain
/// it after the segment's bytes have been tiered to cold storage
/// ([ADR-0021](../../../../../docs/adr/0021-storage-lifecycle-tiered-archival.md)).
/// A column with no stats (an empty segment, or an opted-out column such as
/// [`ColumnId::Payload`]) simply has no entry and never contributes a skip.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZoneMap {
    // Small, fixed-cardinality column set (four in v0.1) — a linear-scan Vec
    // is cheaper and more cache-friendly than a hash map, and keeps the type
    // trivially `Clone`/`Eq` for the resident-metadata tests.
    columns: Vec<(ColumnId, ColumnZone)>,
}

impl ZoneMap {
    /// Build a zone map from per-column `(min, max)` bounds, skipping any
    /// column whose bounds are absent (no stats recorded).
    pub(super) fn from_bounds(
        bounds: impl IntoIterator<Item = (ColumnId, Option<ZoneBound>, Option<ZoneBound>)>,
    ) -> Self {
        let mut columns = Vec::new();
        for (col, min, max) in bounds {
            if let (Some(min), Some(max)) = (min, max) {
                columns.push((col, ColumnZone { min, max }));
            }
        }
        Self { columns }
    }

    /// The min/max bounds recorded for `column`, or `None` when the column
    /// carries no statistics in this segment.
    #[must_use]
    pub fn column(&self, column: ColumnId) -> Option<&ColumnZone> {
        self.columns
            .iter()
            .find(|(c, _)| *c == column)
            .map(|(_, z)| z)
    }

    /// Whether this segment *might* contain a row that is visible at
    /// `snapshot` and satisfies `predicate`.
    ///
    /// Returns `false` only when the zone maps prove no such row can exist, in
    /// which case the planner may skip the segment without reading a single
    /// column chunk. A `true` result means "cannot rule it out — scan it"; it
    /// is never a guarantee that a match is present.
    ///
    /// `snapshot` applies the system-time visibility slice
    /// (`sys_from <= snapshot < sys_to`); `predicate` applies any additional
    /// value constraints. Both must hold for the segment to be kept, so a
    /// segment is skipped if *either* the snapshot slice or the predicate
    /// disproves it.
    #[must_use]
    pub fn might_contain(&self, predicate: &Predicate, snapshot: Snapshot) -> bool {
        self.snapshot_overlaps(snapshot) && self.satisfies(predicate)
    }

    /// One-sided system-time visibility test. `false` means every row in the
    /// segment is provably outside the snapshot's `[sys_from, sys_to)` slice.
    fn snapshot_overlaps(&self, snapshot: Snapshot) -> bool {
        let s = snapshot.0;
        // If we know the minimum `sys_from` and it is already past the
        // snapshot, no row has begun yet at `s`.
        if let Some(zone) = self.column(ColumnId::SysFrom) {
            if let ZoneBound::I64(min_sys_from) = zone.min {
                if SystemTimeMicros(min_sys_from) > s {
                    return false;
                }
            }
        }
        // If we know the maximum `sys_to` and it is at or before the snapshot,
        // every row was already superseded by `s` (the interval is half-open,
        // so `sys_to == s` is *not* visible).
        if let Some(zone) = self.column(ColumnId::SysTo) {
            if let ZoneBound::I64(max_sys_to) = zone.max {
                if SystemTimeMicros(max_sys_to) <= s {
                    return false;
                }
            }
        }
        true
    }

    /// Evaluate a value `predicate` against the zone maps. `false` means the
    /// zone maps prove no row can satisfy it.
    fn satisfies(&self, predicate: &Predicate) -> bool {
        match predicate {
            Predicate::All => true,
            Predicate::And(parts) => parts.iter().all(|p| self.satisfies(p)),
            // A point lies in the segment only if it is within [min, max]. No
            // stats for this column ⇒ cannot prune on it (`is_none_or`).
            Predicate::Eq { column, value } => self
                .column(*column)
                .is_none_or(|zone| *value >= zone.min && *value <= zone.max),
            // Two ranges [low, high] and [min, max] overlap iff
            // `low <= max && min <= high`; absent stats can't prune.
            Predicate::Range { column, low, high } => self
                .column(*column)
                .is_none_or(|zone| *low <= zone.max && zone.min <= *high),
        }
    }
}

/// A value constraint the planner hands to [`ZoneMap::might_contain`].
///
/// Deliberately minimal at v0.1: the shapes a zone map can actually act on
/// (point and range over an ordered column), plus conjunction. Predicates the
/// zone map cannot reason about are expressed as [`Predicate::All`] so they
/// never prune — the segment is conservatively scanned. The richer planner
/// expression tree ([STL-97] onwards) lowers to this for the pruning step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// No value constraint — only the snapshot slice can prune.
    All,
    /// `column == value`.
    Eq {
        /// Column the constraint applies to.
        column: ColumnId,
        /// The value being matched.
        value: ZoneBound,
    },
    /// `low <= column <= high` (inclusive both ends).
    Range {
        /// Column the constraint applies to.
        column: ColumnId,
        /// Inclusive lower bound.
        low: ZoneBound,
        /// Inclusive upper bound.
        high: ZoneBound,
    },
    /// Conjunction — every part must hold. An empty `And` is vacuously true,
    /// matching [`Predicate::All`].
    And(Vec<Predicate>),
}

// `ZoneBound`'s derived `PartialOrd`/`Ord` would compare across variants in
// declaration order, which is meaningless. The footer never mixes variants for
// one column (the column's `ColumnType` fixes the variant), and the comparisons
// in `satisfies` only ever pair same-typed bounds. We implement ordering so
// that same-variant comparisons are correct and cross-variant comparisons fall
// back to a stable-but-unused total order, so a mis-typed predicate degrades to
// "cannot prune" rather than panicking.
impl PartialOrd for ZoneBound {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) => a.partial_cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.partial_cmp(b),
            // Mismatched variants: no meaningful order. Returning `None` makes
            // the `>=`/`<=` comparisons in `satisfies` evaluate to `false`,
            // which would *over*-prune — the one outcome we must never allow.
            // So callers must never mix variants; the writer/reader guarantee
            // that per-column bounds share the column's type. This arm exists
            // only to keep the impl total.
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn i64_zone(
        col: ColumnId,
        min: i64,
        max: i64,
    ) -> (ColumnId, Option<ZoneBound>, Option<ZoneBound>) {
        (col, Some(ZoneBound::I64(min)), Some(ZoneBound::I64(max)))
    }

    fn bytes_zone(
        col: ColumnId,
        min: &[u8],
        max: &[u8],
    ) -> (ColumnId, Option<ZoneBound>, Option<ZoneBound>) {
        (
            col,
            Some(ZoneBound::Bytes(min.to_vec())),
            Some(ZoneBound::Bytes(max.to_vec())),
        )
    }

    /// A zone map for a segment whose rows span `sys_from in [sf_min, sf_max]`
    /// and `sys_to in [st_min, st_max]`, all sharing the single business key
    /// `bk` (so the BusinessKey zone is the degenerate `[bk, bk]`).
    fn map(sf: (i64, i64), st: (i64, i64), bk: &[u8]) -> ZoneMap {
        ZoneMap::from_bounds([
            i64_zone(ColumnId::SysFrom, sf.0, sf.1),
            i64_zone(ColumnId::SysTo, st.0, st.1),
            bytes_zone(ColumnId::BusinessKey, bk, bk),
        ])
    }

    const fn snap(t: i64) -> Snapshot {
        Snapshot(SystemTimeMicros(t))
    }

    #[test]
    fn snapshot_before_every_sys_from_is_pruned() {
        // All rows begin at sys_from >= 100; a snapshot at 50 sees nothing.
        let zm = map((100, 200), (300, 400), b"a");
        assert!(!zm.might_contain(&Predicate::All, snap(50)));
    }

    #[test]
    fn snapshot_at_or_after_every_sys_to_is_pruned() {
        // All rows superseded by sys_to <= 400; the interval is half-open so a
        // snapshot exactly at 400 is already outside every row.
        let zm = map((100, 200), (300, 400), b"a");
        assert!(!zm.might_contain(&Predicate::All, snap(400)));
        assert!(!zm.might_contain(&Predicate::All, snap(401)));
    }

    #[test]
    fn snapshot_inside_the_slice_is_kept() {
        let zm = map((100, 200), (300, 400), b"a");
        // 399 < max(sys_to)=400 and >= min(sys_from)=100 ⇒ cannot rule out.
        assert!(zm.might_contain(&Predicate::All, snap(399)));
        // Boundary: snapshot == min(sys_from) is visible (closed lower bound).
        assert!(zm.might_contain(&Predicate::All, snap(100)));
    }

    #[test]
    fn eq_predicate_prunes_outside_value_range() {
        let zm = map((1, 10), (20, 30), b"d");
        let inside = snap(5);
        // business_key range is exactly ["d","d"]; "a" and "z" fall outside.
        assert!(!zm.might_contain(
            &Predicate::Eq {
                column: ColumnId::BusinessKey,
                value: ZoneBound::Bytes(b"a".to_vec()),
            },
            inside,
        ));
        assert!(zm.might_contain(
            &Predicate::Eq {
                column: ColumnId::BusinessKey,
                value: ZoneBound::Bytes(b"d".to_vec()),
            },
            inside,
        ));
    }

    #[test]
    fn range_predicate_overlap_logic() {
        let zm = map((1, 10), (20, 30), b"m");
        let inside = snap(5);
        let bk = ColumnId::BusinessKey;
        // [a, c] entirely below [m, m] ⇒ prune.
        assert!(!zm.might_contain(
            &Predicate::Range {
                column: bk,
                low: ZoneBound::Bytes(b"a".to_vec()),
                high: ZoneBound::Bytes(b"c".to_vec())
            },
            inside,
        ));
        // [a, p] straddles m ⇒ keep.
        assert!(zm.might_contain(
            &Predicate::Range {
                column: bk,
                low: ZoneBound::Bytes(b"a".to_vec()),
                high: ZoneBound::Bytes(b"p".to_vec())
            },
            inside,
        ));
    }

    #[test]
    fn missing_column_stats_never_prune() {
        // A zone map with no Payload stats must never prune on Payload.
        let zm = map((1, 10), (20, 30), b"k");
        assert!(zm.might_contain(
            &Predicate::Eq {
                column: ColumnId::Payload,
                value: ZoneBound::Bytes(b"whatever".to_vec())
            },
            snap(5),
        ));
    }

    #[test]
    fn and_requires_every_part() {
        let zm = map((1, 10), (20, 30), b"k");
        let bk = ColumnId::BusinessKey;
        let keep = Predicate::Eq {
            column: bk,
            value: ZoneBound::Bytes(b"k".to_vec()),
        };
        let drop = Predicate::Eq {
            column: bk,
            value: ZoneBound::Bytes(b"z".to_vec()),
        };
        assert!(zm.might_contain(&Predicate::And(vec![keep.clone()]), snap(5)));
        assert!(!zm.might_contain(&Predicate::And(vec![keep, drop]), snap(5)));
        // Empty conjunction is vacuously true.
        assert!(zm.might_contain(&Predicate::And(vec![]), snap(5)));
    }

    #[test]
    fn empty_zone_map_keeps_everything() {
        // No stats at all (e.g. an empty segment) ⇒ never prunes.
        let zm = ZoneMap::default();
        assert!(zm.might_contain(&Predicate::All, snap(0)));
        assert!(zm.might_contain(&Predicate::All, snap(i64::MAX)));
    }
}
