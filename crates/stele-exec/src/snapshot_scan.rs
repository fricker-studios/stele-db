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
//! prune segments (zone maps, then validity index) → resolve per key at S from
//!   the identity columns → late-materialize the projected columns of the live
//!   rows + delta → filter by pushed-down predicate → project → Arrow batch
//! ```
//!
//! 1. **Prune.** Each sealed segment runs two complementary skip tests before
//!    any bulk column is read (STL-146). First, [`SegmentReader::might_contain`]
//!    — a zone-map-only check ("begins after `S`", plus value bounds) that
//!    touches no column chunk. A segment that survives the zone map then has only
//!    its narrow `(business_key, sys_from, seq)` identity columns read, which
//!    [`ValidityIndex::sys_upper_bound`] (STL-139) can use to prove every version
//!    is already superseded at `S` ("ends before `S`") — skipping the bulk
//!    chunks. [`ScanStats`] partitions the segments across the two prunes and the
//!    survivors.
//! 2. **Resolve.** The delta tier resolves its own staged versions at `S`
//!    ([`Delta::range_scan`]); the surviving segments' *identities* are folded
//!    with the validity index and resolved the same way
//!    ([`merge::fold_chains`] + [`merge::resolve_snapshot`]) so the operator
//!    learns which rows are live at `S` before reading their payload or
//!    provenance. The two per-tier results are unioned and deduplicated per key,
//!    latest visible version winning. The tiers' per-key system intervals are
//!    disjoint by construction — a superseded version's `sys_to` equals its
//!    successor's `sys_from`, and a flush drains the delta — so at most one tier
//!    holds the version live at `S`; the dedup is a belt-and-suspenders guard,
//!    never a real tie-break in v0.1.
//! 3. **Materialize.** Only the projected (and predicate-referenced) bulk columns
//!    of the segments that resolved a live row are read, via
//!    [`SegmentReader::read_column`] — late materialization. A surviving segment
//!    with no live row at `S` is never read beyond its identity columns.
//! 4. **Filter.** The pushed-down [`Predicate`] is re-applied at the row level:
//!    zone maps prune *segments* conservatively but a surviving segment can still
//!    carry non-matching rows (e.g. other keys), so the row filter is what makes
//!    `WHERE id = 1` return a single row.
//! 5. **Project.** Only the requested [`ColumnId`]s are materialized into the
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
//! Late materialization is per *column*: [`SegmentReader::read_column`] reads a
//! whole column across every row-group, so a survivor with a single live row
//! still reads that column's full chunks and the operator then indexes the row.
//! True per-*row* skipping — reading only the row-groups (chunks) that hold a
//! live row — is a further refinement that needs chunk-level row addressing the
//! reader does not yet expose, and is left to the v0.2 vectorized-execution work
//! (STL-77).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;
use stele_storage::backend::{Disk, DiskFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaError, Snapshot, Version};
use stele_storage::merge;
use stele_storage::segment::{
    ColumnData, ColumnId, Predicate, SegmentError, SegmentReader, ZoneBound,
};
use stele_storage::validity::{ValidityError, ValidityIndex};

/// A sealed row's identity — its `(business_key, sys_from, seq)` triple, the key
/// the validity index and the per-key version chains are keyed by (STL-145).
/// Read narrowly from a segment ([`SegmentReader::version_keys`]) to resolve
/// which rows are live at the snapshot before any bulk column is materialized.
type VersionId = (BusinessKey, SystemTimeMicros, u64);

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
/// A segment reaches the scan in one of three states, and the counts partition
/// the segment set exactly: `segments_total == segments_pruned_zone +
/// segments_pruned_superseded + segments_scanned`.
///
/// Two complementary prunes run before any bulk column is read (STL-146):
///
/// * **Zone-map prune** ([`segments_pruned_zone`](Self::segments_pruned_zone)) —
///   the zone maps prove the segment holds no row visible at the snapshot that
///   satisfies the predicate ("begins after the snapshot", plus value bounds).
///   No column chunk is touched.
/// * **Validity-index prune**
///   ([`segments_pruned_superseded`](Self::segments_pruned_superseded)) — the
///   segment survives the zone map, but reading only its narrow identity columns
///   ([`SegmentReader::version_keys`]) lets [`ValidityIndex::sys_upper_bound`]
///   prove *every* version is already superseded at the snapshot ("ends before
///   the snapshot", STL-139). The bulk column chunks are never read.
///
/// What remains is [`segments_scanned`](Self::segments_scanned): the segments
/// whose projected columns are materialized for the rows that survive resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanStats {
    /// Total sealed segments offered to the scan.
    pub segments_total: usize,
    /// Segments the zone maps proved could hold no visible match — skipped with
    /// no read I/O at all.
    pub segments_pruned_zone: usize,
    /// Segments that survived the zone map but whose every version the validity
    /// index proved superseded at the snapshot — skipped after reading only the
    /// narrow identity columns, never the bulk chunks (STL-139).
    pub segments_pruned_superseded: usize,
    /// Segments that survived both prunes — their projected columns are read for
    /// any row live at the snapshot.
    pub segments_scanned: usize,
}

impl ScanStats {
    /// Segments skipped by either prune (`segments_pruned_zone +
    /// segments_pruned_superseded`) — the count that never had its bulk columns
    /// materialized.
    #[must_use]
    pub const fn segments_pruned(&self) -> usize {
        self.segments_pruned_zone + self.segments_pruned_superseded
    }
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
        // The delta tier resolves its own staged versions at the snapshot. Bound
        // its scan by the business-key range the predicate implies (predicate
        // pushdown for the delta tier) so a selective `WHERE business_key = …`
        // does not walk the whole keyspace — the segment side is already pruned
        // by `might_contain` below. The range is conservative (never drops a key
        // the predicate could match) and the row filter still applies.
        let key_range = predicate_key_range(&self.predicate);
        let delta_live = self
            .delta
            .range_scan(key_range, self.snapshot, self.index)?;

        // The bulk (non-identity) columns a survivor must materialize for this
        // scan — the union of the projected and predicate-referenced columns.
        let needed = self.materialized_columns();

        // Prune each sealed segment in two complementary stages, then keep the
        // identity columns of the survivors for resolution + late materialization.
        let mut pruned_zone = 0usize;
        let mut pruned_superseded = 0usize;
        let mut survivors: Vec<(usize, Vec<VersionId>)> = Vec::new();
        for (seg_idx, reader) in self.segments.iter().enumerate() {
            // Stage 1 — zone map: rules a segment out touching no column chunk.
            if !reader.might_contain(&self.predicate, self.snapshot) {
                pruned_zone += 1;
                continue;
            }
            // Stage 2 — validity index: read only the narrow `(business_key,
            // sys_from, seq)` identity columns and ask whether every version is
            // already superseded at the snapshot (STL-139). Complementary to the
            // zone map's "begins after the snapshot" test, this is "ends before
            // the snapshot"; either prune skips the bulk column chunks.
            let keys = reader.version_keys()?;
            if self
                .index
                .sys_upper_bound(keys.iter().cloned())?
                .superseded_at_or_before(self.snapshot.0)
            {
                pruned_superseded += 1;
                continue;
            }
            survivors.push((seg_idx, keys));
        }
        let stats = ScanStats {
            segments_total: self.segments.len(),
            segments_pruned_zone: pruned_zone,
            segments_pruned_superseded: pruned_superseded,
            segments_scanned: survivors.len(),
        };

        // Resolve which survivor rows are live at the snapshot from the identity
        // columns alone: `fold_chains` overlays the validity index's closes and
        // `resolve_snapshot` picks, per key, the one version whose system
        // interval contains the snapshot. A side map locates each row's source
        // `(segment, row)` so only the survivors that resolve live ever have
        // their bulk columns read.
        let mut locator: BTreeMap<VersionId, (usize, usize)> = BTreeMap::new();
        let mut identities: Vec<Version> = Vec::new();
        for (seg_idx, keys) in &survivors {
            for (row_idx, (bk, sys_from, seq)) in keys.iter().enumerate() {
                locator.insert((bk.clone(), *sys_from, *seq), (*seg_idx, row_idx));
                identities.push(identity_version(bk.clone(), *sys_from, *seq));
            }
        }
        let sealed_chains = merge::fold_chains(identities, self.index)?;
        let mut sealed_live = merge::resolve_snapshot(&sealed_chains, self.snapshot);

        // Late materialization: read each needed bulk column once per survivor
        // that actually holds a live row, then patch it into the resolved
        // version. `read_column` touches only that column's chunks, so the scan
        // pays for the projected columns of the live rows — never the full row
        // of every survivor, never a survivor with no live row.
        let live_segs: BTreeSet<usize> =
            sealed_live.iter().map(|v| locate(&locator, v).0).collect();
        let mut cols_by_seg: BTreeMap<usize, Vec<(ColumnId, ColumnData)>> = BTreeMap::new();
        for seg_idx in live_segs {
            let mut cols = Vec::with_capacity(needed.len());
            for &col in &needed {
                cols.push((col, self.segments[seg_idx].read_column(col)?));
            }
            cols_by_seg.insert(seg_idx, cols);
        }
        for v in &mut sealed_live {
            let (seg_idx, row_idx) = locate(&locator, v);
            for (col, data) in &cols_by_seg[&seg_idx] {
                patch_version(v, *col, data, row_idx);
            }
        }

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

    /// The bulk (non-identity) columns this scan reads from a surviving segment:
    /// the projected and predicate-referenced columns, restricted to those a
    /// segment materializes *outside* the identity triple. The identity columns
    /// (`business_key`, `sys_from`, `seq`) come for free from
    /// [`SegmentReader::version_keys`] during resolution, so they are never
    /// re-read; a column the operator does not materialize at v0.1 (the
    /// valid-time / retraction columns) is excluded here and surfaces its error
    /// at [`build_column`] / is conservatively kept by the row filter.
    fn materialized_columns(&self) -> Vec<ColumnId> {
        let mut referenced = self.projection.clone();
        collect_predicate_columns(&self.predicate, &mut referenced);
        let mut needed = Vec::new();
        for col in referenced {
            if is_bulk_column(col) && !needed.contains(&col) {
                needed.push(col);
            }
        }
        needed
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

/// The inverse of [`u64_bits`]: recover a `u64` from the `i64` bit pattern a
/// segment stores for the `seq` / `txn_id` columns.
const fn bits_u64(value: i64) -> u64 {
    u64::from_le_bytes(value.to_le_bytes())
}

/// The bulk (non-identity) columns a segment materializes that the row filter
/// and projection can read — everything outside the `(business_key, sys_from,
/// seq)` identity triple that [`patch_version`] knows how to overlay. The
/// valid-time and retraction columns are deliberately absent: the operator does
/// not materialize them at v0.1.
const fn is_bulk_column(col: ColumnId) -> bool {
    matches!(
        col,
        ColumnId::Payload | ColumnId::TxnId | ColumnId::CommittedAt | ColumnId::Principal
    )
}

/// A payload-less, provenance-less stand-in carrying only a sealed row's
/// `(business_key, sys_from, seq)` identity — all [`merge::fold_chains`] /
/// [`merge::resolve_snapshot`] need to decide which row is live at the snapshot
/// before any bulk column is read. The bulk fields are patched in by late
/// materialization ([`patch_version`]) for the rows that survive resolution.
fn identity_version(business_key: BusinessKey, sys_from: SystemTimeMicros, seq: u64) -> Version {
    Version::open(
        business_key,
        sys_from,
        seq,
        Provenance::new(TxnId(0), SystemTimeMicros(0), Principal::new(Vec::new())),
        Vec::new(),
    )
}

/// Locate a resolved sealed version's source `(segment, row)`. `resolve_snapshot`
/// only ever returns identities that went in via [`identity_version`], so the
/// lookup cannot miss — a miss is a resolution-layer invariant break.
fn locate(locator: &BTreeMap<VersionId, (usize, usize)>, v: &Version) -> (usize, usize) {
    *locator
        .get(&(v.business_key.clone(), v.sys_from, v.seq))
        .expect("a resolved sealed identity must locate its source row")
}

/// Overlay one late-materialized bulk column onto a resolved version at `row`.
/// `col` is always one of [`is_bulk_column`]'s set and `data` always has the
/// type that column's arm expects (the writer's schema), so the fallthrough is
/// structurally unreachable.
fn patch_version(v: &mut Version, col: ColumnId, data: &ColumnData, row: usize) {
    match (col, data) {
        (ColumnId::Payload, ColumnData::Bytes(b)) => v.payload.clone_from(&b[row]),
        (ColumnId::Principal, ColumnData::Bytes(b)) => {
            v.provenance.principal = Principal::new(b[row].clone());
        }
        (ColumnId::TxnId, ColumnData::I64(x)) => v.provenance.txn_id = TxnId(bits_u64(x[row])),
        (ColumnId::CommittedAt, ColumnData::I64(x)) => {
            v.provenance.committed_at = SystemTimeMicros(x[row]);
        }
        _ => {}
    }
}

/// Collect every column a predicate references, in tree order (duplicates kept;
/// [`SnapshotScan::materialized_columns`] dedups). The mirror of
/// [`predicate_matches`] over the predicate tree.
fn collect_predicate_columns(predicate: &Predicate, out: &mut Vec<ColumnId>) {
    match predicate {
        Predicate::All => {}
        Predicate::Eq { column, .. } | Predicate::Range { column, .. } => out.push(*column),
        Predicate::And(parts) => {
            for part in parts {
                collect_predicate_columns(part, out);
            }
        }
    }
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

/// The `[business_key]` value of a predicate term, or `None` when the term does
/// not constrain the business key (a different column, or a non-bytes bound).
const fn business_key_bytes(column: ColumnId, bound: &ZoneBound) -> Option<&Vec<u8>> {
    match (column, bound) {
        (ColumnId::BusinessKey, ZoneBound::Bytes(v)) => Some(v),
        _ => None,
    }
}

/// Derive the conservative business-key range a predicate implies, so the delta
/// tier's scan is bounded for a selective query (predicate pushdown). A predicate
/// that does not constrain the business key yields an unbounded range — every key
/// is a candidate and the row-level filter still applies. The range only ever
/// *widens* relative to the true match set, so it can never drop a matching key.
fn predicate_key_range(predicate: &Predicate) -> (Bound<BusinessKey>, Bound<BusinessKey>) {
    match predicate {
        Predicate::All => (Bound::Unbounded, Bound::Unbounded),
        Predicate::Eq { column, value } => {
            business_key_bytes(*column, value).map_or((Bound::Unbounded, Bound::Unbounded), |v| {
                let key = BusinessKey::new(v.clone());
                (Bound::Included(key.clone()), Bound::Included(key))
            })
        }
        Predicate::Range { column, low, high } => {
            match (
                business_key_bytes(*column, low),
                business_key_bytes(*column, high),
            ) {
                (Some(l), Some(h)) => (
                    Bound::Included(BusinessKey::new(l.clone())),
                    Bound::Included(BusinessKey::new(h.clone())),
                ),
                _ => (Bound::Unbounded, Bound::Unbounded),
            }
        }
        // Every conjunct must hold, so the combined range is the intersection of
        // the parts' ranges — the tightest lower and upper bound across them.
        Predicate::And(parts) => {
            parts
                .iter()
                .fold((Bound::Unbounded, Bound::Unbounded), |(lo, hi), p| {
                    let (plo, phi) = predicate_key_range(p);
                    (tighter_lower(lo, plo), tighter_upper(hi, phi))
                })
        }
    }
}

/// The tighter (larger) of two inclusive lower bounds; `Unbounded` is the
/// identity. Only `Included` / `Unbounded` are produced upstream — any other
/// bound widens conservatively to `Unbounded` rather than risk dropping a key.
fn tighter_lower(a: Bound<BusinessKey>, b: Bound<BusinessKey>) -> Bound<BusinessKey> {
    match (a, b) {
        (Bound::Included(x), Bound::Included(y)) => Bound::Included(x.max(y)),
        (Bound::Unbounded, other @ Bound::Included(_))
        | (other @ Bound::Included(_), Bound::Unbounded) => other,
        _ => Bound::Unbounded,
    }
}

/// The tighter (smaller) of two inclusive upper bounds; see [`tighter_lower`].
fn tighter_upper(a: Bound<BusinessKey>, b: Bound<BusinessKey>) -> Bound<BusinessKey> {
    match (a, b) {
        (Bound::Included(x), Bound::Included(y)) => Bound::Included(x.min(y)),
        (Bound::Unbounded, other @ Bound::Included(_))
        | (other @ Bound::Included(_), Bound::Unbounded) => other,
        _ => Bound::Unbounded,
    }
}
