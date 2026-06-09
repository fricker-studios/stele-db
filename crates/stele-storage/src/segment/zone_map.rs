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
//! `sys_from <= snapshot < sys_to`. A sealed segment stores only `sys_from`, not
//! `sys_to` (v6, [ADR-0023](../../../../../docs/adr/0023-append-only-record-model-validity-index.md)):
//! the period end lives in the derived [validity index](crate::validity). So the
//! segment zone map gives **one** one-sided skip rule on the system axis:
//!
//! * if `min(sys_from) > snapshot`, *every* row begins after the snapshot —
//!   none is visible yet; skip.
//!
//! The complementary "every row already superseded" prune (an upper bound on the
//! period end) is **not** the zone map's to give — the segment no longer stores
//! `sys_to`. It is supplied by the validity index
//! ([`ValidityIndex::sys_upper_bound`](crate::validity::ValidityIndex::sys_upper_bound),
//! [STL-139]): the planner derives a per-segment `max(sys_to)` from the index and
//! skips a segment all of whose rows are superseded at/before the snapshot, then
//! composes that with the [`ZoneMap::might_contain`] decision below. A segment the
//! zone map keeps but the index proves fully superseded is pruned there; one with
//! any open version is kept and the index-overlaid resolver filters it out at
//! read time. Conservative either way, never a false negative.
//!
//! Valid-time pruning rides the *same* generic machinery. A valid-time table's
//! segment carries `valid_from` / `valid_to` as first-class `i64` columns,
//! lifted at flush from the payload's valid-time prefix ([STL-117], [STL-92]).
//! The zone map is keyed by [`ColumnId`] and the writer computes min/max for
//! every column generically, so a `valid_from` / `valid_to` [`Predicate::Range`]
//! prunes through [`ZoneMap::might_contain`] with no valid-time-specific code
//! here — exactly as the design anticipated.

use std::cmp::Ordering;

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
    /// Bound of a fixed-width `i64` column (e.g. [`ColumnId::SysFrom`]).
    I64(i64),
    /// Bound of a variable-length bytes column ([`ColumnId::BusinessKey`]).
    Bytes(Vec<u8>),
}

/// One end (min or max) of a column's value range in a [`ColumnZone`].
///
/// Almost always a concrete [`Self::Value`], but a bounded-prefix bytes
/// column (the segment writer) can have an end that is *open*: the lex-min is the
/// empty byte string (so the lower bound is effectively −∞ — everything is
/// `>= b""`), or the lex-max prefix saturates at all-`0xFF` and has no shorter
/// representable upper bound (so the upper bound is +∞). [`Self::Unbounded`]
/// records that end as open so the column **keeps its zone entry and prunes on
/// the other, representable side** ([STL-120]) — instead of the pre-STL-120
/// behaviour, where the degenerate end's zero-length "no stats" sentinel
/// collapsed the whole column zone and gave up pruning on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneEnd {
    /// A concrete recorded bound. On the `min` side a sound lower bound
    /// (truncated *down* for an over-cap bytes prefix); on the `max` side a sound
    /// upper bound (rounded *up*).
    Value(ZoneBound),
    /// The end is open: −∞ for a `min`, +∞ for a `max`. Carries no value and
    /// never prunes — a value can be neither provably below −∞ nor provably
    /// above +∞ — so the segment is always kept on this side, which is exactly
    /// the conservative, no-false-negative behaviour the soundness contract
    /// requires.
    Unbounded,
}

/// The min/max pair for one column across the whole segment.
///
/// Both bounds are always present together — a column either has stats (then
/// both `min` and `max` are recorded) or it has none (then there is no
/// [`ColumnZone`] entry at all). Each bound is a [`ZoneEnd`]: a concrete
/// [`ZoneBound`] value (same variant for both ends, dictated by the column's
/// `ColumnType`), or [`ZoneEnd::Unbounded`] for a degenerate bounded-prefix
/// bytes end ([STL-120]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnZone {
    /// Smallest value observed for the column in this segment, or
    /// [`ZoneEnd::Unbounded`] (−∞) when the lower bound is open.
    pub min: ZoneEnd,
    /// Largest value observed for the column in this segment, or
    /// [`ZoneEnd::Unbounded`] (+∞) when the upper bound is open.
    pub max: ZoneEnd,
}

/// Resident per-segment zone map: the min/max digest for every column that
/// carries statistics.
///
/// Cheap to clone and independent of the file handle — the planner can retain
/// it after the segment's bytes have been tiered to cold storage
/// ([ADR-0021](../../../../../docs/adr/0021-storage-lifecycle-tiered-archival.md)).
/// Variable-length bytes columns such as [`ColumnId::Payload`] record a
/// bounded min/max *prefix* (the writer caps it at `MAX_BYTES_STAT_PREFIX_LEN`)
/// rather than opting out of stats entirely. A degenerate bounded-prefix end —
/// an empty lex-min, or an all-`0xFF` max with no shorter upper bound — is
/// recorded as [`ZoneEnd::Unbounded`] (−∞ / +∞) rather than collapsing the
/// column's zone, so the column still prunes on its representable side ([STL-120]).
/// A column has **no entry** only when it carries no values at all (an empty
/// segment, or an all-NULL `payload`); a column with no entry never contributes
/// a skip.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZoneMap {
    // Small, fixed-cardinality column set (four in v0.1) — a linear-scan Vec
    // is cheaper and more cache-friendly than a hash map, and keeps the type
    // trivially `Clone`/`Eq` for the resident-metadata tests.
    columns: Vec<(ColumnId, ColumnZone)>,
}

impl ZoneMap {
    /// Build a zone map from per-column `(min, max)` bounds, skipping any
    /// column whose bounds are absent (no stats recorded). A present-but-open
    /// end is [`ZoneEnd::Unbounded`] — `Some(ZoneEnd::Unbounded)`, distinct from
    /// the `None` that drops the column ([STL-120]).
    pub(super) fn from_bounds(
        bounds: impl IntoIterator<Item = (ColumnId, Option<ZoneEnd>, Option<ZoneEnd>)>,
    ) -> Self {
        let mut columns = Vec::new();
        for (col, min, max) in bounds {
            if let (Some(min), Some(max)) = (min, max) {
                columns.push((col, ColumnZone { min, max }));
            }
        }
        // Canonicalize by column id so the derived `PartialEq`/`Eq` (and any
        // future hashing) reflect the logical set of bounds, not whatever order
        // the caller happened to supply.
        columns.sort_by_key(|(c, _)| *c);
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
    /// segment provably begins after the snapshot. The complementary "every row
    /// already superseded" prune needs the period end, which a segment no longer
    /// stores (v6, [ADR-0023]) — that prune is the validity index's, via
    /// [`ValidityIndex::sys_upper_bound`](crate::validity::ValidityIndex::sys_upper_bound)
    /// ([STL-139]), which the planner composes with this test.
    fn snapshot_overlaps(&self, snapshot: Snapshot) -> bool {
        let s = snapshot.0;
        // If we know the minimum `sys_from` and it is already past the
        // snapshot, no row has begun yet at `s`. Match on a reference and copy
        // the inner `i64` out — `ZoneBound` is not `Copy`.
        if let Some(zone) = self.column(ColumnId::SysFrom) {
            // `sys_from` is a fixed-width i64 column — its bounds are always
            // concrete (only bounded-prefix *bytes* columns ever go open), so a
            // non-`Value` min here is not reachable and simply skips the prune.
            if let ZoneEnd::Value(ZoneBound::I64(min_sys_from)) = &zone.min {
                if SystemTimeMicros(*min_sys_from) > s {
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
            // A point survives only if it lies within [min, max]. We keep the
            // segment unless we can *prove* `value` is outside — a value whose
            // variant doesn't match the column's stat (a mistyped predicate) is
            // incomparable, and an open end (−∞ / +∞) is unprovable on its side,
            // so in either case the corresponding test does not fire and the
            // segment is conservatively kept. No stats ⇒ `is_none_or` keeps it.
            Predicate::Eq { column, value } => self
                .column(*column)
                .is_none_or(|zone| !zone.min.prunes_below(value) && !zone.max.prunes_above(value)),
            // Ranges [low, high] and [min, max] are provably disjoint only when
            // `high < min` or `low > max`; either proof prunes. Cross-variant or
            // open bounds are unprovable, so neither proof fires and the segment
            // is kept.
            Predicate::Range { column, low, high } => self
                .column(*column)
                .is_none_or(|zone| !zone.min.prunes_below(high) && !zone.max.prunes_above(low)),
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

impl ZoneBound {
    /// Total order *within* a variant. Deliberately **not** a `PartialOrd`
    /// impl: two bounds of different variants are genuinely incomparable
    /// (`None`), and there is no meaningful cross-variant order to expose.
    ///
    /// The footer never mixes variants for one column — the column's
    /// `ColumnType` fixes the variant — so this returns `Some` on every path
    /// the writer/reader can produce. The `None` arm exists for a *mistyped
    /// predicate* (a caller comparing, say, a `Bytes` value against an `I64`
    /// column), which every caller must treat conservatively: incomparable
    /// means "cannot prune", never "prune".
    pub(super) fn cmp_same_variant(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) => Some(a.cmp(b)),
            (Self::Bytes(a), Self::Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// `true` only when `self` is *provably* less than `other` (same variant
    /// and strictly smaller). Cross-variant ⇒ `false`, so a mistyped predicate
    /// can never use this to justify a skip.
    fn is_below(&self, other: &Self) -> bool {
        self.cmp_same_variant(other) == Some(Ordering::Less)
    }

    /// `true` only when `self` is *provably* greater than `other`. Cross-variant
    /// ⇒ `false`.
    fn is_above(&self, other: &Self) -> bool {
        self.cmp_same_variant(other) == Some(Ordering::Greater)
    }
}

impl ZoneEnd {
    /// As a `min` end: `true` only when `value` is *provably* below this lower
    /// bound, justifying a skip. An open end (−∞) and a cross-variant comparison
    /// are both unprovable, so they return `false` — the segment is kept ([STL-120]).
    fn prunes_below(&self, value: &ZoneBound) -> bool {
        match self {
            Self::Value(min) => value.is_below(min),
            Self::Unbounded => false,
        }
    }

    /// As a `max` end: `true` only when `value` is *provably* above this upper
    /// bound, justifying a skip. An open end (+∞) and a cross-variant comparison
    /// are both unprovable, so they return `false` — the segment is kept ([STL-120]).
    fn prunes_above(&self, value: &ZoneBound) -> bool {
        match self {
            Self::Value(max) => value.is_above(max),
            Self::Unbounded => false,
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
    ) -> (ColumnId, Option<ZoneEnd>, Option<ZoneEnd>) {
        (
            col,
            Some(ZoneEnd::Value(ZoneBound::I64(min))),
            Some(ZoneEnd::Value(ZoneBound::I64(max))),
        )
    }

    fn bytes_zone(
        col: ColumnId,
        min: &[u8],
        max: &[u8],
    ) -> (ColumnId, Option<ZoneEnd>, Option<ZoneEnd>) {
        (
            col,
            Some(ZoneEnd::Value(ZoneBound::Bytes(min.to_vec()))),
            Some(ZoneEnd::Value(ZoneBound::Bytes(max.to_vec()))),
        )
    }

    /// A zone map for a segment whose rows span `sys_from in [sf_min, sf_max]`,
    /// all sharing the single business key `bk` (so the BusinessKey zone is the
    /// degenerate `[bk, bk]`). A segment no longer stores `sys_to` (v6,
    /// [ADR-0023]), so the period-end prune is not the zone map's anymore.
    fn map(sf: (i64, i64), bk: &[u8]) -> ZoneMap {
        ZoneMap::from_bounds([
            i64_zone(ColumnId::SysFrom, sf.0, sf.1),
            bytes_zone(ColumnId::BusinessKey, bk, bk),
        ])
    }

    const fn snap(t: i64) -> Snapshot {
        Snapshot(SystemTimeMicros(t))
    }

    #[test]
    fn snapshot_before_every_sys_from_is_pruned() {
        // All rows begin at sys_from >= 100; a snapshot at 50 sees nothing.
        let zm = map((100, 200), b"a");
        assert!(!zm.might_contain(&Predicate::All, snap(50)));
    }

    #[test]
    fn snapshot_at_or_after_min_sys_from_is_kept() {
        // Without a stored sys_to the segment can no longer prune on "every row
        // already superseded" — a snapshot at or after min(sys_from) is kept and
        // the index-overlaid resolver filters at read time.
        let zm = map((100, 200), b"a");
        assert!(zm.might_contain(&Predicate::All, snap(400)));
        // Boundary: snapshot == min(sys_from) is visible (closed lower bound).
        assert!(zm.might_contain(&Predicate::All, snap(100)));
    }

    #[test]
    fn eq_predicate_prunes_outside_value_range() {
        let zm = map((1, 10), b"d");
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
        let zm = map((1, 10), b"m");
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
        let zm = map((1, 10), b"k");
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
        let zm = map((1, 10), b"k");
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
    fn mistyped_predicate_is_conservative_and_keeps_segment() {
        // A predicate whose bound variant disagrees with the column's stat
        // type (a caller bug) is incomparable. Pruning on an incomparable
        // ordering would be a false negative, so might_contain must keep the
        // segment in every such case.
        let zm = map((1, 10), b"k");
        // Bytes value against the i64 SysFrom column.
        assert!(zm.might_contain(
            &Predicate::Eq {
                column: ColumnId::SysFrom,
                value: ZoneBound::Bytes(b"x".to_vec()),
            },
            snap(5),
        ));
        // i64 range against the Bytes BusinessKey column.
        assert!(zm.might_contain(
            &Predicate::Range {
                column: ColumnId::BusinessKey,
                low: ZoneBound::I64(0),
                high: ZoneBound::I64(100),
            },
            snap(5),
        ));
    }

    #[test]
    fn empty_zone_map_keeps_everything() {
        // No stats at all (e.g. an empty segment) ⇒ never prunes.
        let zm = ZoneMap::default();
        assert!(zm.might_contain(&Predicate::All, snap(0)));
        assert!(zm.might_contain(&Predicate::All, snap(i64::MAX)));
    }

    /// A bytes column whose `min` is open (−∞, the empty-lex-min case) still
    /// prunes on its concrete `max`, but never on the low side ([STL-120]).
    #[test]
    fn unbounded_min_prunes_only_on_the_max_side() {
        let pl = ColumnId::Payload;
        let zm = ZoneMap::from_bounds([(
            pl,
            Some(ZoneEnd::Unbounded),
            Some(ZoneEnd::Value(ZoneBound::Bytes(b"m".to_vec()))),
        )]);
        let inside = snap(0);
        // Above the concrete max ⇒ provably outside ⇒ prune.
        assert!(!zm.might_contain(
            &Predicate::Eq {
                column: pl,
                value: ZoneBound::Bytes(b"z".to_vec()),
            },
            inside,
        ));
        // Below the (open) min ⇒ unprovable ⇒ kept: nothing is below −∞.
        assert!(zm.might_contain(
            &Predicate::Eq {
                column: pl,
                value: ZoneBound::Bytes(b"".to_vec()),
            },
            inside,
        ));
        // A range wholly above the max still prunes; one straddling it is kept.
        assert!(!zm.might_contain(
            &Predicate::Range {
                column: pl,
                low: ZoneBound::Bytes(b"x".to_vec()),
                high: ZoneBound::Bytes(b"z".to_vec()),
            },
            inside,
        ));
        assert!(zm.might_contain(
            &Predicate::Range {
                column: pl,
                low: ZoneBound::Bytes(b"a".to_vec()),
                high: ZoneBound::Bytes(b"z".to_vec()),
            },
            inside,
        ));
    }

    /// A bytes column whose `max` is open (+∞, the all-`0xFF` case) still prunes
    /// on its concrete `min`, but never on the high side ([STL-120]).
    #[test]
    fn unbounded_max_prunes_only_on_the_min_side() {
        let pl = ColumnId::Payload;
        let zm = ZoneMap::from_bounds([(
            pl,
            Some(ZoneEnd::Value(ZoneBound::Bytes(b"m".to_vec()))),
            Some(ZoneEnd::Unbounded),
        )]);
        let inside = snap(0);
        // Below the concrete min ⇒ provably outside ⇒ prune.
        assert!(!zm.might_contain(
            &Predicate::Eq {
                column: pl,
                value: ZoneBound::Bytes(b"a".to_vec()),
            },
            inside,
        ));
        // Above the (open) max ⇒ unprovable ⇒ kept: nothing is above +∞.
        assert!(zm.might_contain(
            &Predicate::Eq {
                column: pl,
                value: ZoneBound::Bytes(vec![0xFF; 100]),
            },
            inside,
        ));
        // A range wholly below the min prunes; one straddling it is kept.
        assert!(!zm.might_contain(
            &Predicate::Range {
                column: pl,
                low: ZoneBound::Bytes(b"a".to_vec()),
                high: ZoneBound::Bytes(b"c".to_vec()),
            },
            inside,
        ));
        assert!(zm.might_contain(
            &Predicate::Range {
                column: pl,
                low: ZoneBound::Bytes(b"a".to_vec()),
                high: ZoneBound::Bytes(b"z".to_vec()),
            },
            inside,
        ));
    }

    /// Both ends open (a single-row column whose value is `b""` would land here
    /// if its max also saturated) keeps everything — equivalent to no entry, but
    /// it is a present `ColumnZone`, never a panic on either prune side.
    #[test]
    fn both_ends_unbounded_keeps_everything() {
        let pl = ColumnId::Payload;
        let zm = ZoneMap::from_bounds([(pl, Some(ZoneEnd::Unbounded), Some(ZoneEnd::Unbounded))]);
        assert!(zm.might_contain(
            &Predicate::Eq {
                column: pl,
                value: ZoneBound::Bytes(b"anything".to_vec()),
            },
            snap(0),
        ));
    }
}
