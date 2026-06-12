//! Secondary-index access structures and their lifecycle state ([STL-233]).
//!
//! This is the substrate the v0.3 index family plugs into: the engine owns the
//! lifecycle (build at `CREATE INDEX`, maintain on every committed write,
//! rebuild on cold boot, drop with `DROP INDEX` / `DROP TABLE`), and each index
//! *kind* is just an [`AccessStructure`] behind that lifecycle. The substrate
//! ships the default ordered structure ([`BTreeIndex`]), which serves equality
//! *and* range probes ([STL-237]); the hash/bloom and valid-time families
//! ([STL-238], [STL-241]) add implementations without touching the machinery
//! around them.
//!
//! ## The superset contract — speed, never results
//!
//! A structure is **advisory**: a probe answers with a *candidate window* of
//! business keys, and the scan uses it only to skip work — the exact `WHERE`
//! filter is always re-applied to whatever the scan returns. Correctness
//! therefore rests on one obligation, the **superset contract**:
//!
//! > For every read snapshot at or after the index's [`floor`](IndexState::floor),
//! > the candidates for a value must include **every** business key whose
//! > visible row matches it. Extra candidates are harmless (the filter drops
//! > them); a *missing* candidate would silently drop a row.
//!
//! The structures uphold it by being **add-only over version history**: every
//! committed write *notes* its `(cell, key)` pair and nothing is ever removed —
//! an `UPDATE` away from a value or a `DELETE` leaves the old entry in place,
//! because a key that *ever* carried a value (since the floor) may still carry
//! it at some past snapshot a query can name. This is what makes the structure
//! immune to flush and compaction (which move versions between tiers but never
//! change their content) and cheap to reason about under the bitemporal model.
//! The indexed≡unindexed equivalence oracle pins the contract end to end.
//!
//! ## Derived and rebuildable, never durable
//!
//! Like the validity index ([ADR-0023]), an access structure is derived state:
//! only the *metadata* reaches the durable catalog log ([ADR-0028]), and the
//! structure is (re)built from the table's tiers — at `CREATE INDEX` from the
//! rows live at that instant, and again on every cold boot from the recovered
//! state. A crash mid-build therefore leaves nothing to repair: either the DDL
//! was never acknowledged (no record, no index) or it was (recovery rebuilds).
//! The [`floor`](IndexState::floor) records the snapshot the build ran at;
//! reads *before* it fall back to a full scan, since the build saw only the
//! rows live at that instant, not the history before them.
//!
//! [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
//! [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
//! [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
//! [STL-241]: https://allegromusic.atlassian.net/browse/STL-241
//! [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use stele_common::time::SystemTimeMicros;
use stele_common::types::LogicalType;
use stele_storage::delta::BusinessKey;

/// What an equality probe found — the candidate business keys, shaped for the
/// scan's existing pruning machinery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Probe {
    /// No key has *ever* carried this value (since the floor): under the
    /// superset contract no visible row can match, so the scan is skipped
    /// entirely. This is the load-bearing arm — a structure that wrongly
    /// answers `Empty` returns wrong results, which is exactly what the
    /// equivalence oracle exists to catch.
    Empty,
    /// Every candidate key lies in this **inclusive** window of encoded
    /// business keys. The scan prunes to the window (zone maps, row groups,
    /// delta range) and the re-applied `WHERE` filter keeps the answer exact
    /// regardless of how loose the window is.
    Window {
        /// The smallest candidate key.
        low: BusinessKey,
        /// The largest candidate key.
        high: BusinessKey,
    },
}

/// The contract an index kind implements — deliberately minimal at the
/// substrate: note committed cells, answer equality probes, and — for the
/// ordered family ([STL-237]) — answer range probes. Sibling kinds extend the
/// probe vocabulary further (membership for hash/bloom, interval stabs for
/// valid-time) as they land.
///
/// `Send` so the engine that owns the structures can sit behind the server's
/// session mutex.
///
/// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
pub(crate) trait AccessStructure: Send + std::fmt::Debug {
    /// Record that `key` committed a version whose indexed column holds `cell`
    /// (the value's canonical encoding). Add-only: see the
    /// [module docs](self) for why nothing is ever removed. `NULL` cells are
    /// never noted — neither an equality nor a range probe can ever match one
    /// (three-valued logic), so they would be dead weight.
    fn note(&mut self, cell: &[u8], key: &BusinessKey);

    /// The candidate window for `cell` under the superset contract.
    fn equality_candidates(&self, cell: &[u8]) -> Probe;

    /// The candidate window for every cell **typed-ordered** inside
    /// `(low, high)` (bounds in canonical encoding), or `None` when this kind
    /// cannot serve range probes — `None` means "no answer, full-scan", never
    /// "no rows" ([STL-237]).
    ///
    /// The default declines: range service is the ordered family's extension,
    /// and a kind that cannot walk its cells in the column type's value order
    /// (a hash table, an unorderable column type) must refuse rather than
    /// answer from a different order — a wrong window violates the superset
    /// contract silently.
    ///
    /// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
    fn range_candidates(&self, low: Bound<&[u8]>, high: Bound<&[u8]>) -> Option<Probe> {
        let _ = (low, high);
        None
    }
}

/// The ordered structure's stored key for a canonical `cell` encoding: a
/// **memcomparable** form whose plain byte order equals the column type's value
/// order, so a [`BTreeMap`] walk *is* a typed range scan ([STL-237]).
///
/// The canonical [`ScalarValue::encode`](stele_common::types::ScalarValue)
/// forms are little-endian for the integer family, so their byte order is not
/// their value order; the transform re-encodes them big-endian with the sign
/// bit flipped (`-1` sorts below `0`, which sorts below `1`). Text, bytea,
/// UUID, and boolean canonical encodings already byte-order the way the
/// vectorized evaluator compares them, so they pass through unchanged —
/// as do the types [`range_orderable`] refuses to range-walk (`FLOAT8`,
/// `PERIOD`), which keep exact *equality* service.
///
/// Two properties carry the correctness argument:
/// * **injective** for every type, so an equality probe on the transformed key
///   is exactly an equality on the value;
/// * **strictly monotonic** w.r.t. the typed order for every
///   [`range_orderable`] type, so a byte-range walk visits exactly the cells a
///   typed range contains — one missed cell would silently drop rows (the
///   superset contract).
///
/// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
fn index_key(ty: LogicalType, cell: &[u8]) -> Vec<u8> {
    match ty {
        LogicalType::Int4 | LogicalType::Date => {
            let v = i32::from_le_bytes(cell.try_into().expect("canonical i32 cells are 4 bytes"));
            (v ^ i32::MIN).cast_unsigned().to_be_bytes().to_vec()
        }
        LogicalType::Int8 | LogicalType::Timestamp | LogicalType::TimestampTz => {
            let v = i64::from_le_bytes(cell.try_into().expect("canonical i64 cells are 8 bytes"));
            (v ^ i64::MIN).cast_unsigned().to_be_bytes().to_vec()
        }
        LogicalType::Text
        | LogicalType::Bool
        | LogicalType::Uuid
        | LogicalType::Bytea
        | LogicalType::Float8
        | LogicalType::Period => cell.to_vec(),
    }
}

/// Whether a typed range over this column maps to a byte range of its
/// [`index_key`] form — the gate on the ordered structure's range service.
///
/// `FLOAT8` is refused: its canonical encoding is raw IEEE-754 bits, whose
/// byte order is not its value order in any endianness (sign-magnitude), and
/// the vectorized evaluator does not compare it anyway ([STL-207] left it the
/// sole out-of-scope type). `PERIOD` is refused: intervals compare through the
/// dedicated period predicates ([STL-165]), not the scalar comparison a range
/// probe serves.
///
/// [STL-207]: https://allegromusic.atlassian.net/browse/STL-207
/// [STL-165]: https://allegromusic.atlassian.net/browse/STL-165
const fn range_orderable(ty: LogicalType) -> bool {
    !matches!(ty, LogicalType::Float8 | LogicalType::Period)
}

/// The default ordered access structure: a sorted map from each noted cell's
/// [memcomparable form](index_key) to the set of keys that ever carried it.
/// Equality probes answer from the exact entry; range probes walk the map in
/// the column type's value order and union the visited entries' candidate
/// windows ([STL-237]).
///
/// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
#[derive(Debug)]
pub(crate) struct BTreeIndex {
    /// The indexed column's logical type — fixes the [`index_key`] transform
    /// and whether the structure may serve range probes ([`range_orderable`]).
    ty: LogicalType,
    entries: BTreeMap<Vec<u8>, BTreeSet<BusinessKey>>,
}

impl BTreeIndex {
    /// An empty structure over a column of type `ty`.
    pub(crate) const fn new(ty: LogicalType) -> Self {
        Self {
            ty,
            entries: BTreeMap::new(),
        }
    }
}

impl AccessStructure for BTreeIndex {
    fn note(&mut self, cell: &[u8], key: &BusinessKey) {
        self.entries
            .entry(index_key(self.ty, cell))
            .or_default()
            .insert(key.clone());
    }

    fn equality_candidates(&self, cell: &[u8]) -> Probe {
        self.entries
            .get(&index_key(self.ty, cell))
            .map_or(Probe::Empty, |keys| {
                let low = keys.first().expect("noted sets are never empty").clone();
                let high = keys.last().expect("noted sets are never empty").clone();
                Probe::Window { low, high }
            })
    }

    fn range_candidates(&self, low: Bound<&[u8]>, high: Bound<&[u8]>) -> Option<Probe> {
        if !range_orderable(self.ty) {
            return None;
        }
        let transform = |bound: Bound<&[u8]>| match bound {
            Bound::Included(cell) => Bound::Included(index_key(self.ty, cell)),
            Bound::Excluded(cell) => Bound::Excluded(index_key(self.ty, cell)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let (low, high) = (transform(low), transform(high));
        // An inverted or void cell range has no candidates by construction —
        // answer `Empty` directly rather than let `BTreeMap::range` panic on
        // an invalid range. Today's callers only build one-sided ranges (the
        // binder lowers a single comparison), so this arm is purely defensive.
        if let (Bound::Included(l) | Bound::Excluded(l), Bound::Included(h) | Bound::Excluded(h)) =
            (&low, &high)
        {
            let void = matches!(
                (&low, &high),
                (Bound::Excluded(_), _) | (_, Bound::Excluded(_))
            );
            if l > h || (l == h && void) {
                return Some(Probe::Empty);
            }
        }
        let mut window: Option<(BusinessKey, BusinessKey)> = None;
        for keys in self.entries.range((low, high)).map(|(_, keys)| keys) {
            let first = keys.first().expect("noted sets are never empty");
            let last = keys.last().expect("noted sets are never empty");
            window = Some(match window {
                None => (first.clone(), last.clone()),
                Some((lo, hi)) => (lo.min(first.clone()), hi.max(last.clone())),
            });
        }
        Some(window.map_or(Probe::Empty, |(low, high)| Probe::Window { low, high }))
    }
}

/// One live index's runtime state: the access structure plus the snapshot
/// floor its build covers from.
#[derive(Debug)]
pub(crate) struct IndexState {
    /// The snapshot the structure was (re)built at. Reads at or after it may
    /// consult the structure; reads before it must full-scan — the build saw
    /// only the rows live at this instant, so older history is uncovered.
    pub(crate) floor: SystemTimeMicros,
    /// The access structure itself.
    pub(crate) structure: Box<dyn AccessStructure>,
}

impl IndexState {
    /// Fresh state for a build at `floor`, in the given kind's structure over
    /// a column of type `ty`.
    pub(crate) fn new(
        kind: stele_catalog::IndexKind,
        ty: LogicalType,
        floor: SystemTimeMicros,
    ) -> Self {
        let structure: Box<dyn AccessStructure> = match kind {
            stele_catalog::IndexKind::BTree => Box::new(BTreeIndex::new(ty)),
        };
        Self { floor, structure }
    }
}

#[cfg(test)]
mod tests {
    use stele_common::types::ScalarValue;

    use super::*;

    fn key(bytes: &[u8]) -> BusinessKey {
        BusinessKey::new(bytes.to_vec())
    }

    /// A [`ScalarValue`]'s canonical cell encoding.
    fn cell(value: &ScalarValue) -> Vec<u8> {
        let mut bytes = Vec::new();
        value.encode(&mut bytes);
        bytes
    }

    #[test]
    fn an_unnoted_cell_probes_empty_and_a_noted_one_windows_its_keys() {
        let mut index = BTreeIndex::new(LogicalType::Text);
        assert_eq!(index.equality_candidates(b"v"), Probe::Empty);

        index.note(b"v", &key(b"k2"));
        index.note(b"v", &key(b"k0"));
        index.note(b"v", &key(b"k9"));
        index.note(b"w", &key(b"k5"));

        // The window spans exactly the noted keys for that cell, inclusive.
        assert_eq!(
            index.equality_candidates(b"v"),
            Probe::Window {
                low: key(b"k0"),
                high: key(b"k9"),
            }
        );
        assert_eq!(
            index.equality_candidates(b"w"),
            Probe::Window {
                low: key(b"k5"),
                high: key(b"k5"),
            }
        );
    }

    #[test]
    fn noting_is_add_only_so_a_superseded_value_keeps_its_candidate() {
        // An UPDATE away from `v` must NOT remove k from v's candidates: a
        // past snapshot may still see k carrying v (the superset contract).
        let mut index = BTreeIndex::new(LogicalType::Text);
        index.note(b"v", &key(b"k"));
        index.note(b"w", &key(b"k"));
        assert_eq!(
            index.equality_candidates(b"v"),
            Probe::Window {
                low: key(b"k"),
                high: key(b"k"),
            }
        );
        assert_eq!(
            index.equality_candidates(b"w"),
            Probe::Window {
                low: key(b"k"),
                high: key(b"k"),
            }
        );
    }

    #[test]
    fn the_index_key_transform_is_monotonic_over_the_typed_order() {
        // The load-bearing property ([STL-237]): byte order of the transformed
        // form == typed order, across sign boundaries and extremes — the
        // little-endian canonical form fails exactly these.
        let i32s = [i32::MIN, -2, -1, 0, 1, 2, i32::MAX];
        for pair in i32s.windows(2) {
            let (a, b) = (
                index_key(LogicalType::Int4, &cell(&ScalarValue::Int4(pair[0]))),
                index_key(LogicalType::Int4, &cell(&ScalarValue::Int4(pair[1]))),
            );
            assert!(a < b, "{} must sort below {}", pair[0], pair[1]);
        }
        let i64s = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        for pair in i64s.windows(2) {
            let (a, b) = (
                index_key(LogicalType::Int8, &cell(&ScalarValue::Int8(pair[0]))),
                index_key(LogicalType::Int8, &cell(&ScalarValue::Int8(pair[1]))),
            );
            assert!(a < b, "{} must sort below {}", pair[0], pair[1]);
        }
    }

    #[test]
    fn range_probes_walk_typed_order_and_union_candidate_windows() {
        let mut index = BTreeIndex::new(LogicalType::Int4);
        let note = |index: &mut BTreeIndex, v: i32, k: &[u8]| {
            index.note(&cell(&ScalarValue::Int4(v)), &key(k));
        };
        note(&mut index, -5, b"k7");
        note(&mut index, 1, b"k2");
        note(&mut index, 3, b"k9");
        note(&mut index, 10, b"k0");

        let bound = |v: i32| cell(&ScalarValue::Int4(v));
        let probe = |index: &BTreeIndex, low: Bound<Vec<u8>>, high: Bound<Vec<u8>>| {
            index
                .range_candidates(
                    low.as_ref().map(Vec::as_slice),
                    high.as_ref().map(Vec::as_slice),
                )
                .expect("an orderable B-tree serves ranges")
        };

        // `< 1` catches only -5 — the sign flip keeps a negative *below* the
        // positives (raw little-endian bytes would sort it above them all).
        assert_eq!(
            probe(&index, Bound::Unbounded, Bound::Excluded(bound(1))),
            Probe::Window {
                low: key(b"k7"),
                high: key(b"k7"),
            }
        );
        // `>= 1` unions the 1, 3, and 10 entries' windows: keys k0..k9.
        assert_eq!(
            probe(&index, Bound::Included(bound(1)), Bound::Unbounded),
            Probe::Window {
                low: key(b"k0"),
                high: key(b"k9"),
            }
        );
        // `> 10` walks past every noted cell: provably no candidate.
        assert_eq!(
            probe(&index, Bound::Excluded(bound(10)), Bound::Unbounded),
            Probe::Empty
        );
        // An inverted range is `Empty` by construction, not a panic.
        assert_eq!(
            probe(&index, Bound::Included(bound(5)), Bound::Excluded(bound(5))),
            Probe::Empty
        );
        assert_eq!(
            probe(&index, Bound::Included(bound(7)), Bound::Included(bound(2))),
            Probe::Empty
        );
    }

    #[test]
    fn an_unorderable_type_declines_ranges_but_keeps_exact_equality() {
        // FLOAT8's canonical bytes do not order by value, so the structure
        // must refuse to range-walk them (`None` = full scan, never a wrong
        // window) — while equality stays exact (the transform is injective).
        let mut index = BTreeIndex::new(LogicalType::Float8);
        let bits = 1.5f64.to_bits().to_le_bytes();
        index.note(&bits, &key(b"k1"));
        assert_eq!(
            index.range_candidates(Bound::Included(&bits), Bound::Unbounded),
            None
        );
        assert_eq!(
            index.equality_candidates(&bits),
            Probe::Window {
                low: key(b"k1"),
                high: key(b"k1"),
            }
        );
    }
}
