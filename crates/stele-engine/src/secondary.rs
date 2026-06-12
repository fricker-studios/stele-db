//! Secondary-index access structures and their lifecycle state ([STL-233]).
//!
//! This is the substrate the v0.3 index family plugs into: the engine owns the
//! lifecycle (build at `CREATE INDEX`, maintain on every committed write,
//! rebuild on cold boot, drop with `DROP INDEX` / `DROP TABLE`), and each index
//! *kind* is just an [`AccessStructure`] behind that lifecycle. The substrate
//! ships the default ordered structure ([`BTreeIndex`]); the hash/bloom and
//! valid-time families ([STL-238], [STL-241]) add implementations without
//! touching the machinery around them.
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
//! [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
//! [STL-241]: https://allegromusic.atlassian.net/browse/STL-241
//! [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md

use std::collections::{BTreeMap, BTreeSet};

use stele_common::time::SystemTimeMicros;
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
/// substrate: note committed cells, answer equality probes. Sibling kinds
/// extend the probe vocabulary (ranges for the ordered family, membership for
/// hash/bloom, interval stabs for valid-time) as they land.
///
/// `Send` so the engine that owns the structures can sit behind the server's
/// session mutex.
pub(crate) trait AccessStructure: Send + std::fmt::Debug {
    /// Record that `key` committed a version whose indexed column holds `cell`
    /// (the value's canonical encoding). Add-only: see the
    /// [module docs](self) for why nothing is ever removed. `NULL` cells are
    /// never noted — an equality probe can never match one (three-valued
    /// logic), so they would be dead weight.
    fn note(&mut self, cell: &[u8], key: &BusinessKey);

    /// The candidate window for `cell` under the superset contract.
    fn equality_candidates(&self, cell: &[u8]) -> Probe;
}

/// The default ordered access structure: a sorted map from each noted cell
/// encoding to the set of keys that ever carried it. Equality probes answer
/// from the exact entry; the ordered shape is what the sibling B-tree ticket
/// ([STL-237]) extends with range probes.
///
/// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
#[derive(Debug, Default)]
pub(crate) struct BTreeIndex {
    entries: BTreeMap<Vec<u8>, BTreeSet<BusinessKey>>,
}

impl AccessStructure for BTreeIndex {
    fn note(&mut self, cell: &[u8], key: &BusinessKey) {
        self.entries
            .entry(cell.to_vec())
            .or_default()
            .insert(key.clone());
    }

    fn equality_candidates(&self, cell: &[u8]) -> Probe {
        self.entries.get(cell).map_or(Probe::Empty, |keys| {
            let low = keys.first().expect("noted sets are never empty").clone();
            let high = keys.last().expect("noted sets are never empty").clone();
            Probe::Window { low, high }
        })
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
    /// Fresh state for a build at `floor`, in the given kind's structure.
    pub(crate) fn new(kind: stele_catalog::IndexKind, floor: SystemTimeMicros) -> Self {
        let structure: Box<dyn AccessStructure> = match kind {
            stele_catalog::IndexKind::BTree => Box::new(BTreeIndex::default()),
        };
        Self { floor, structure }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &[u8]) -> BusinessKey {
        BusinessKey::new(bytes.to_vec())
    }

    #[test]
    fn an_unnoted_cell_probes_empty_and_a_noted_one_windows_its_keys() {
        let mut index = BTreeIndex::default();
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
        let mut index = BTreeIndex::default();
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
}
