//! `SnapshotScan` — the read-path operator that answers an `AS OF` snapshot read
//! by merging the delta tier with the sealed segments
//! ([architecture §3.5](../../../docs/02-architecture.md#35-read-path--as-of-flow),
//! [STL-100]).
//!
//! Given an MVCC snapshot `S`, the operator returns one resolved [`Version`] per
//! business key — the version whose system-time interval `[sys_from, sys_to)`
//! contains `S`. A version's *end* is never stored on the record
//! ([ADR-0023](../../../docs/adr/0023-append-only-record-model-validity-index.md)):
//! it is overlaid from the [`ValidityIndex`] at read time, exactly as the
//! write-path resolver and the delta tier's own scan already do
//! ([`stele_storage::merge`]).
//!
//! ## Shape of the read
//!
//! ```text
//! prune segments (zone maps) → read survivors + delta → resolve per key at S
//!   → filter by pushed-down predicate → project columns → Arrow-shaped batch
//! ```
//!
//! 1. **Prune.** Each sealed segment is tested against the predicate and the
//!    snapshot with [`SegmentReader::might_contain`] — a zone-map-only check that
//!    touches no column chunk. A segment the zone maps prove cannot hold a
//!    visible match is skipped before any read I/O. The number of segments
//!    actually read therefore equals the number the zone maps did not prune —
//!    the invariant [`ScanStats`] exposes (STL-100 DoD).
//! 2. **Merge.** The delta tier resolves its own staged versions at `S`
//!    ([`Delta::range_scan`]); the surviving segments' raw versions are folded
//!    with the validity index and resolved the same way
//!    ([`merge::fold_chains`] + [`merge::resolve_snapshot`]). The two per-tier
//!    results are unioned and deduplicated per key, latest visible version
//!    winning. The tiers' per-key system intervals are disjoint by construction
//!    — a superseded version's `sys_to` equals its successor's `sys_from`, and a
//!    flush drains the delta — so at most one tier holds the version live at `S`;
//!    the dedup is a belt-and-suspenders guard, never a real tie-break in v0.1.
//! 3. **Filter.** The pushed-down [`Predicate`] is re-applied at the row level:
//!    zone maps prune *segments* conservatively but a surviving segment can still
//!    carry non-matching rows (e.g. other keys), so the row filter is what makes
//!    `WHERE id = 1` return a single row.
//! 4. **Project.** Only the requested [`ColumnId`]s are materialized into the
//!    output batch.
//!
//! ## Determinism
//!
//! The operator reads the validity index (deterministic [`Disk`] I/O) and holds
//! no runtime or wall-clock dependency, so it runs under the simulation
//! scheduler like the rest of the storage/txn core
//! ([architecture §12 invariant 7](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//!
//! ## Not yet (follow-ups)
//!
//! Projection is realized as *output* trimming plus segment-level zone-map
//! pruning; per-column late materialization *within* a surviving segment (read
//! only the projected column chunks for the rows that survive resolution) is a
//! noted optimization, not wired at v0.1. The complementary "every row already
//! superseded" segment prune via [`ValidityIndex::sys_upper_bound`] (STL-139)
//! composes on top of the zone-map prune and is likewise deferred so the
//! read-count invariant stays a clean function of the zone maps alone.

use std::collections::BTreeMap;

use stele_storage::backend::{Disk, DiskFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaError, Snapshot, Version};
use stele_storage::merge;
use stele_storage::segment::{ColumnId, Predicate, SegmentError, SegmentReader, ZoneBound};
use stele_storage::validity::{ValidityError, ValidityIndex};

/// Errors surfaced while executing a [`SnapshotScan`].
#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    /// The delta tier failed to resolve its staged versions (spill I/O or a
    /// folded validity-index spill read).
    #[error("delta read: {0}")]
    Delta(#[from] DeltaError),

    /// A sealed segment could not be read.
    #[error("segment read: {0}")]
    Segment(#[from] SegmentError),

    /// The validity index's backing spill could not be read while folding the
    /// sealed segments' versions.
    #[error("validity index: {0}")]
    Validity(#[from] ValidityError),

    /// A projection requested a column the operator does not materialize at
    /// v0.1 — the version row-group set ([`ColumnId::ALL`]) is projectable; the
    /// valid-time pair and the retraction tombstone columns are not yet.
    #[error("column {0:?} is not projectable by SnapshotScan at v0.1")]
    UnsupportedProjection(ColumnId),
}

/// One column of a [`Batch`] — Arrow-shaped: a single typed, contiguous array
/// whose length equals the batch's row count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Column {
    /// A variable-length bytes column (business key, payload, principal).
    Bytes(Vec<Vec<u8>>),
    /// A fixed-width `i64` column (system time, seq, provenance scalars). `u64`
    /// columns (`seq`, `txn_id`) are carried as their `i64` bit-reinterpretation,
    /// the same lossless round-trip the segment format uses ([`ColumnId::TxnId`]).
    I64(Vec<i64>),
}

impl Column {
    /// Number of values in the column.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Bytes(v) => v.len(),
            Self::I64(v) => v.len(),
        }
    }

    /// Whether the column has no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A vectorized result batch — the executor's Arrow-shaped output unit.
///
/// Columns appear in projection order, each paired with the [`ColumnId`] it
/// materializes; every column holds exactly [`rows`](Self::rows) values, aligned
/// row-wise across columns. v0.1 emits a single batch even when it is small
/// (STL-100 scope); a batch-at-a-time iterator is a later refinement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// The projected columns, in projection order.
    pub columns: Vec<(ColumnId, Column)>,
    /// Row count shared by every column.
    pub rows: usize,
}

/// Per-scan statistics — chiefly the segment-pruning accounting.
///
/// The STL-100 DoD asserts on it: `segments_scanned` is exactly the number of
/// segments the zone maps did not prune, and the only segments the operator
/// read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanStats {
    /// Total sealed segments offered to the scan.
    pub segments_total: usize,
    /// Segments the zone maps proved could hold no visible match — skipped with
    /// no read I/O.
    pub segments_pruned: usize,
    /// Segments actually read (`segments_total - segments_pruned`).
    pub segments_scanned: usize,
}

/// The result of executing a [`SnapshotScan`]: the projected [`Batch`] and the
/// [`ScanStats`] for the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOutput {
    /// The projected, snapshot-resolved rows.
    pub batch: Batch,
    /// Pruning / read accounting for the scan.
    pub stats: ScanStats,
}

/// The `SnapshotScan { table, snapshot, projection, predicates }` operator.
///
/// A "table" at v0.1 is its storage tiers: the `delta` handle, the `index`
/// (system-time ends), and the table's sealed `segments`. The operator borrows
/// them for the duration of the scan and never mutates them.
pub struct SnapshotScan<'a, D: Disk, I: Disk, F: DiskFile> {
    delta: &'a Delta<D>,
    index: &'a ValidityIndex<I>,
    segments: &'a [SegmentReader<F>],
    snapshot: Snapshot,
    projection: Vec<ColumnId>,
    predicate: Predicate,
}

impl<'a, D: Disk, I: Disk, F: DiskFile> SnapshotScan<'a, D, I, F> {
    /// Build a scan over a table's tiers at `snapshot`.
    ///
    /// Defaults to projecting the full always-on version column set
    /// ([`ColumnId::ALL`]) with no predicate ([`Predicate::All`]); narrow either
    /// with [`project`](Self::project) / [`filter`](Self::filter).
    #[must_use]
    pub fn new(
        delta: &'a Delta<D>,
        index: &'a ValidityIndex<I>,
        segments: &'a [SegmentReader<F>],
        snapshot: Snapshot,
    ) -> Self {
        Self {
            delta,
            index,
            segments,
            snapshot,
            projection: ColumnId::ALL.to_vec(),
            predicate: Predicate::All,
        }
    }

    /// Restrict the output to `columns`, in the given order (projection
    /// pushdown). Only the always-on version columns are projectable at v0.1;
    /// any other column surfaces [`ScanError::UnsupportedProjection`] at
    /// [`execute`](Self::execute) time.
    #[must_use]
    pub fn project(mut self, columns: Vec<ColumnId>) -> Self {
        self.projection = columns;
        self
    }

    /// Push a predicate down into the scan. It prunes whole segments via their
    /// zone maps and then filters surviving rows. Predicates a zone map cannot
    /// reason about should be lowered to [`Predicate::All`] by the planner so
    /// they never prune — here they simply never reject a segment, and the row
    /// filter still applies them where the column is materializable.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = predicate;
        self
    }

    /// Execute the scan: prune, merge, resolve at the snapshot, filter, project.
    ///
    /// # Errors
    ///
    /// [`ScanError`] if a tier read fails or the projection names a column the
    /// operator does not materialize at v0.1.
    pub fn execute(&self) -> Result<ScanOutput, ScanError> {
        let (rows, stats) = self.resolve_rows()?;
        let filtered: Vec<Version> = rows
            .into_iter()
            .filter(|v| predicate_matches(&self.predicate, v))
            .collect();
        let batch = self.project_batch(&filtered)?;
        Ok(ScanOutput { batch, stats })
    }

    /// Resolve one version per key, live at the snapshot, across both tiers,
    /// returning the rows alongside the segment-pruning [`ScanStats`].
    fn resolve_rows(&self) -> Result<(Vec<Version>, ScanStats), ScanError> {
        // The delta tier resolves its own staged versions at the snapshot.
        let delta_live = self.delta.range_scan(.., self.snapshot, self.index)?;

        // Prune sealed segments by zone map, then fold + resolve the survivors.
        let mut scanned = 0usize;
        let mut sealed_candidates: Vec<Version> = Vec::new();
        for reader in self.segments {
            if reader.might_contain(&self.predicate, self.snapshot) {
                scanned += 1;
                sealed_candidates.extend(reader.read_versions()?);
            }
        }
        let stats = ScanStats {
            segments_total: self.segments.len(),
            segments_pruned: self.segments.len() - scanned,
            segments_scanned: scanned,
        };
        let sealed_chains = merge::fold_chains(sealed_candidates, self.index)?;
        let sealed_live = merge::resolve_snapshot(&sealed_chains, self.snapshot);

        // Union the two per-tier results, deduplicated per key. The intervals
        // are disjoint across tiers (see the module docs), so a key resolves to
        // at most one version in each tier; when — defensively — both produce
        // one, the greater `(sys_from, seq)` wins ("latest visible version").
        let mut by_key: BTreeMap<BusinessKey, Version> = BTreeMap::new();
        for v in sealed_live.into_iter().chain(delta_live) {
            match by_key.get(&v.business_key) {
                Some(existing) if (existing.sys_from, existing.seq) >= (v.sys_from, v.seq) => {}
                _ => {
                    by_key.insert(v.business_key.clone(), v);
                }
            }
        }
        Ok((by_key.into_values().collect(), stats))
    }

    /// Materialize the projected columns into an Arrow-shaped [`Batch`].
    fn project_batch(&self, rows: &[Version]) -> Result<Batch, ScanError> {
        let columns = self
            .projection
            .iter()
            .map(|&col| Ok((col, build_column(col, rows)?)))
            .collect::<Result<Vec<_>, ScanError>>()?;
        Ok(Batch {
            columns,
            rows: rows.len(),
        })
    }
}

/// Build one projected column by reading `col` from every resolved row.
fn build_column(col: ColumnId, rows: &[Version]) -> Result<Column, ScanError> {
    Ok(match col {
        ColumnId::BusinessKey => Column::Bytes(
            rows.iter()
                .map(|v| v.business_key.as_bytes().to_vec())
                .collect(),
        ),
        ColumnId::SysFrom => Column::I64(rows.iter().map(|v| v.sys_from.0).collect()),
        // `seq` / `txn_id` are logically `u64`; carry their `i64` bit pattern,
        // the lossless round-trip the segment format defines ([`ColumnId::Seq`]).
        ColumnId::Seq => Column::I64(rows.iter().map(|v| u64_bits(v.seq)).collect()),
        ColumnId::Payload => Column::Bytes(rows.iter().map(|v| v.payload.clone()).collect()),
        ColumnId::TxnId => Column::I64(
            rows.iter()
                .map(|v| u64_bits(v.provenance.txn_id.0))
                .collect(),
        ),
        ColumnId::CommittedAt => {
            Column::I64(rows.iter().map(|v| v.provenance.committed_at.0).collect())
        }
        ColumnId::Principal => Column::Bytes(
            rows.iter()
                .map(|v| v.provenance.principal.as_bytes().to_vec())
                .collect(),
        ),
        other => return Err(ScanError::UnsupportedProjection(other)),
    })
}

/// Reinterpret a `u64` as its `i64` bit pattern — the lossless mapping the
/// segment format uses for the `seq` / `txn_id` columns. Avoids a lossy `as`
/// cast (and its clippy lint) while preserving every bit.
const fn u64_bits(value: u64) -> i64 {
    i64::from_le_bytes(value.to_le_bytes())
}

/// Evaluate a pushed-down predicate against one resolved version (the row-level
/// filter the conservative zone-map prune cannot replace).
///
/// A column the row filter cannot materialize is treated as "cannot decide" —
/// the row is kept, never silently dropped — mirroring the zone map's
/// conservative "cannot prove absent ⇒ keep" stance.
fn predicate_matches(predicate: &Predicate, v: &Version) -> bool {
    match predicate {
        Predicate::All => true,
        Predicate::And(parts) => parts.iter().all(|p| predicate_matches(p, v)),
        Predicate::Eq { column, value } => {
            column_value(*column, v).is_none_or(|actual| &actual == value)
        }
        Predicate::Range { column, low, high } => {
            column_value(*column, v).is_none_or(|actual| within(&actual, low, high))
        }
    }
}

/// The version's value for `col` as a [`ZoneBound`], or `None` for a column the
/// row filter does not decode (the valid-time / retraction columns).
fn column_value(col: ColumnId, v: &Version) -> Option<ZoneBound> {
    Some(match col {
        ColumnId::BusinessKey => ZoneBound::Bytes(v.business_key.as_bytes().to_vec()),
        ColumnId::SysFrom => ZoneBound::I64(v.sys_from.0),
        ColumnId::Seq => ZoneBound::I64(u64_bits(v.seq)),
        ColumnId::Payload => ZoneBound::Bytes(v.payload.clone()),
        ColumnId::TxnId => ZoneBound::I64(u64_bits(v.provenance.txn_id.0)),
        ColumnId::CommittedAt => ZoneBound::I64(v.provenance.committed_at.0),
        ColumnId::Principal => ZoneBound::Bytes(v.provenance.principal.as_bytes().to_vec()),
        _ => return None,
    })
}

/// Inclusive `low <= value <= high` within a single [`ZoneBound`] variant.
/// Cross-variant bounds are incomparable, so — like a mistyped predicate at the
/// zone-map layer — the row is conservatively kept.
fn within(value: &ZoneBound, low: &ZoneBound, high: &ZoneBound) -> bool {
    match (value, low, high) {
        (ZoneBound::I64(c), ZoneBound::I64(l), ZoneBound::I64(h)) => l <= c && c <= h,
        (ZoneBound::Bytes(c), ZoneBound::Bytes(l), ZoneBound::Bytes(h)) => l <= c && c <= h,
        _ => true,
    }
}
