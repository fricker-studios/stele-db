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
//!    of the segments that resolved a live row are read — late materialization.
//!    A surviving segment with no live row at `S` is never read beyond its
//!    identity columns, and within a segment that *is* read, only the row-groups
//!    holding a live row have their chunks touched
//!    ([`SegmentReader::read_column_in_row_groups`], STL-155): the resolved live
//!    row indices are mapped back to their owning row-groups
//!    ([`RowGroupSelection`]) and the read is scoped to those.
//! 4. **Filter.** The pushed-down [`Predicate`] is re-applied at the row level:
//!    zone maps prune *segments* conservatively but a surviving segment can still
//!    carry non-matching rows (e.g. other keys), so the row filter is what makes
//!    `WHERE id = 1` return a single row.
//! 5. **Project.** Only the requested [`ColumnId`]s are materialized into the
//!    output batch.
//!
//! ## The valid axis (STL-163)
//!
//! The steps above resolve the **system** axis. A bitemporal `AS OF (s, v)`
//! query also pins the **valid** axis: [`valid_as_of`](SnapshotScan::valid_as_of)
//! supplies the valid instant `v`, and the operator keeps, per key, only the
//! system-live version whose `[valid_from, valid_to)` interval *also* contains
//! `v` (the half-open
//! [`ValidInterval::contains`](stele_storage::validtime::ValidInterval) test).
//! The filter runs **after** the system-time live set is resolved and the two
//! tiers carry the interval differently: a delta row frames it on the payload
//! ([`unframe_payload`]), a sealed segment lifts it into first-class
//! `valid_from` / `valid_to` columns ([STL-117]) the filter reads directly —
//! before bulk materialization, so a row excluded on the valid axis never has
//! its payload read. On a both-axes scan the emitted payload is the bare user
//! value, the framing prefix stripped. A scan with no valid instant
//! (`valid_as_of` unset) is system-only and untouched by any of this.
//!
//! ## Determinism
//!
//! The operator reads the validity index (deterministic [`Disk`] I/O) and holds
//! no runtime or wall-clock dependency, so it runs under the simulation
//! scheduler like the rest of the storage/txn core
//! ([architecture §12 invariant 7](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//!
//! ## Granularity (STL-146 → STL-155)
//!
//! Late materialization is per *column* (STL-146) **and** per *row-group*
//! (STL-155): a survivor reads only the projected columns, and within each
//! column only the chunks of row-groups that hold a live row. A chunk is the
//! format's I/O unit, so row-group granularity is the finest skipping the
//! on-disk layout admits — a one-row-group segment (the default the engine's
//! flush writes today) degenerates to the STL-146 behavior, and the gain
//! appears once a writer bounds its row-groups
//! ([`SegmentWriter::with_max_row_group_rows`](stele_storage::segment::SegmentWriter::with_max_row_group_rows)).
//! Wiring a row-group bound into the engine's flush policy is a follow-up.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::row_codec::RowCodecError;
use stele_common::time::{SystemTimeMicros, ValidTimeMicros};
use stele_storage::backend::{Disk, DiskFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaError, Snapshot, Version};
use stele_storage::merge;
use stele_storage::segment::{
    ColumnData, ColumnId, Predicate, SegmentError, SegmentReader, ZoneBound,
};
use stele_storage::validity::{ValidityError, ValidityIndex};
use stele_storage::validtime::{VALID_TIME_PREFIX_LEN, ValidTimeError, unframe_payload};

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

    /// A delta version's framed valid-time payload could not be decoded while
    /// resolving the valid axis (a truncated interval prefix on a row from a
    /// table the scan was told tracks valid-time).
    #[error("valid-time decode: {0}")]
    ValidTime(#[from] ValidTimeError),

    /// A projection requested a column the operator does not materialize at
    /// v0.1 — the version row-group set ([`ColumnId::ALL`]) is projectable; the
    /// valid-time pair and the retraction tombstone columns are not yet.
    #[error("column {0:?} is not projectable by SnapshotScan at v0.1")]
    UnsupportedProjection(ColumnId),

    /// A [`Project`](crate::Project) operator asked for a column its child
    /// operator did not emit in the batch — a plan-construction error: the
    /// project list named a column the source was not configured to produce.
    #[error("column {0:?} is not present in the input batch")]
    MissingColumn(ColumnId),

    /// A [`Filter`](crate::Filter) operator's predicate could not be evaluated
    /// over a batch — a structurally invalid expression or an out-of-scope
    /// column type ([STL-170]).
    #[error("filter predicate: {0}")]
    Eval(#[from] crate::expr::ExprError),

    /// An [`ExplodePayload`](crate::ExplodePayload) operator could not slice a
    /// stored payload into its value cells — the bytes do not match the table's
    /// value-column count (corruption, or a width disagreement). See the
    /// [row codec](stele_common::row_codec) ([STL-206]).
    #[error("payload explode: {0}")]
    RowCodec(#[from] RowCodecError),
}

/// One column of a [`Batch`] — Arrow-shaped: a single typed, contiguous array
/// whose length equals the batch's row count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Column {
    /// A variable-length bytes column (business key, payload, principal). Each
    /// cell is `Option<Vec<u8>>` so a SQL `NULL` payload ([STL-154]) is carried
    /// as `None`, distinct from `Some(vec![])` (an empty value). The always-present
    /// columns (business key, principal) only ever hold `Some`.
    Bytes(Vec<Option<Vec<u8>>>),
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

    /// A contiguous `len`-value window starting at `offset`. Used by the
    /// batch-at-a-time pull pipeline ([`crate::Operator`]) to cut a fully
    /// resolved column into fixed-size batches.
    ///
    /// This currently deep-copies the window: `Column` owns its cells rather than
    /// a shared buffer, so an Arrow-style zero-copy slice awaits the shared-buffer
    /// `Column` representation (a tracked v0.2 follow-up; see PR #77 / STL-170).
    ///
    /// # Panics
    ///
    /// If `offset + len` exceeds the column's length — the caller
    /// ([`crate::ScanSource`]) only ever slices within the resolved row count.
    #[must_use]
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        match self {
            Self::Bytes(v) => Self::Bytes(v[offset..offset + len].to_vec()),
            Self::I64(v) => Self::I64(v[offset..offset + len].to_vec()),
        }
    }

    /// Gather the cells at `rows`, in the given order, into a new column — the
    /// row-selection a [`Filter`](crate::Filter) applies once it knows which
    /// rows its predicate kept. Like [`slice`](Self::slice) this copies (a
    /// `Column` owns its cells); the same shared-buffer follow-up would make it
    /// zero-copy.
    ///
    /// # Panics
    ///
    /// If any index is out of range — the caller selects from this column's own
    /// row count.
    #[must_use]
    pub fn take(&self, rows: &[usize]) -> Self {
        match self {
            Self::Bytes(v) => Self::Bytes(rows.iter().map(|&i| v[i].clone()).collect()),
            Self::I64(v) => Self::I64(rows.iter().map(|&i| v[i]).collect()),
        }
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
    /// The valid-time instant to resolve the *second* axis at, set via
    /// [`valid_as_of`](Self::valid_as_of). `None` is a system-only scan — the
    /// valid axis is not consulted and the operator behaves exactly as it did
    /// before STL-163. `Some(v)` turns on both-axes resolution: after the
    /// system-time live set is resolved, each version is kept only when its
    /// `[valid_from, valid_to)` interval contains `v`.
    valid_snapshot: Option<ValidTimeMicros>,
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
            valid_snapshot: None,
            projection: ColumnId::ALL.to_vec(),
            predicate: Predicate::All,
        }
    }

    /// Resolve the **valid-time** axis at `point` as well as the system axis,
    /// turning the scan bitemporal: a row is returned only when its system
    /// interval contains the [`snapshot`](Self::new) *and* its valid interval
    /// `[valid_from, valid_to)` contains `point` (the half-open
    /// [`ValidInterval::contains`](stele_storage::validtime::ValidInterval::contains)
    /// test) — the one version live on both axes at `(snapshot, point)`
    /// (STL-163).
    ///
    /// **Only meaningful for a valid-time-enabled table.** The caller (the
    /// query executor) supplies `point` exactly when the table opts into
    /// valid-time, the same way the write path takes the per-table policy as a
    /// resolved flag rather than re-deriving it
    /// ([`frame_payload`](stele_storage::validtime::frame_payload)). A
    /// valid-time table's delta rows carry the interval framed on the payload
    /// and its sealed segments carry first-class `valid_from` / `valid_to`
    /// columns ([STL-117], written via
    /// [`SegmentWriter::create_valid_time`](stele_storage::segment::SegmentWriter::create_valid_time));
    /// the operator reads the interval from whichever its tier provides. On a
    /// both-axes scan the projected [`ColumnId::Payload`] is the **bare** user
    /// payload — the interval prefix the delta tier frames on is stripped, so
    /// the value is consistent with what a sealed segment already stores and is
    /// the user's value, not the 16-byte temporal envelope.
    #[must_use]
    pub const fn valid_as_of(mut self, point: ValidTimeMicros) -> Self {
        self.valid_snapshot = Some(point);
        self
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

    /// Turn this scan into a [`ScanSource`](crate::ScanSource) — a source
    /// [`Operator`](crate::Operator) that emits the resolved rows in batches of
    /// at most `batch_rows` rows. The concatenation of every emitted batch is
    /// byte-for-byte the [`execute`](Self::execute) result; the source merely
    /// chunks it for the pull pipeline. A `batch_rows` of `0` is clamped to `1`.
    #[must_use]
    pub fn into_source(self, batch_rows: usize) -> crate::ScanSource<'a, D, I, F> {
        crate::ScanSource::new(self, batch_rows)
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
        let mut delta_live = self
            .delta
            .range_scan(key_range, self.snapshot, self.index)?;

        // Valid axis, delta tier. A valid-time table's delta rows carry the
        // interval framed on the payload ([`frame_payload`]); recover it with
        // [`unframe_payload`], keep the row only when its `[valid_from,
        // valid_to)` contains the valid snapshot, and strip the prefix so the
        // emitted payload is the bare user value (the sealed tier already stores
        // it bare). System-only scans (`valid_snapshot == None`) skip this and
        // leave the payload untouched.
        if let Some(point) = self.valid_snapshot {
            delta_live = filter_delta_by_valid(delta_live, point)?;
        }

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

        // Valid axis, sealed tier. A valid-time segment lifts the interval into
        // first-class `valid_from` / `valid_to` i64 columns ([STL-117]), so the
        // membership test reads those two narrow columns — never the bulk
        // payload — and drops every system-live row whose interval does not
        // contain the valid snapshot. Running it *before* late materialization
        // means a row pruned on the valid axis never has its payload/provenance
        // read. The segments touched here are already in `segments_scanned`
        // (they hold a system-live row); the valid filter prunes rows, not
        // segments, so [`ScanStats`] keeps its shape.
        if let Some(point) = self.valid_snapshot {
            self.filter_sealed_by_valid(&mut sealed_live, &locator, point)?;
        }

        // Late materialization: read each needed bulk column once per survivor
        // that actually holds a live row, then patch it into the resolved
        // version. The read touches only that column's chunks, and only in the
        // row-groups that hold a live row ([`RowGroupSelection`], STL-155) — so
        // the scan pays for the projected columns of the live rows' row-groups,
        // never the full column of every survivor, never a survivor with no
        // live row.
        let live_rows = live_rows_by_segment(&sealed_live, &locator);
        let mut cols_by_seg: BTreeMap<usize, (RowGroupSelection, Vec<(ColumnId, ColumnData)>)> =
            BTreeMap::new();
        for (seg_idx, rows) in live_rows {
            let sel = RowGroupSelection::new(&self.segments[seg_idx].row_group_row_counts(), &rows);
            let mut cols = Vec::with_capacity(needed.len());
            for &col in &needed {
                let data = self.segments[seg_idx].read_column_in_row_groups(col, &sel.groups)?;
                // Every bulk column must carry exactly as many values as the
                // selected row-groups declare in the footer (the same counts the
                // identity columns satisfied in [`SegmentReader::version_keys`]);
                // a disagreement is a corrupt segment, surfaced as an error
                // rather than an out-of-bounds panic when the per-row patch
                // indexes into the read result (the same guard
                // `SegmentReader::read_versions` applies across its columns).
                if column_len(&data) != sel.selected_rows {
                    return Err(ScanError::Segment(SegmentError::Corrupt(
                        "segment column value count disagrees with the selected row-groups' row count",
                    )));
                }
                cols.push((col, data));
            }
            cols_by_seg.insert(seg_idx, (sel, cols));
        }
        for v in &mut sealed_live {
            let (seg_idx, row_idx) = locate(&locator, v);
            let (sel, cols) = &cols_by_seg[&seg_idx];
            let local_idx = sel.local_index(row_idx);
            for (col, data) in cols {
                patch_version(v, *col, data, local_idx);
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

    /// Drop, in place, every system-live sealed row whose valid interval does
    /// not contain `point`. Reads only the two narrow `valid_from` / `valid_to`
    /// columns of the segments that hold a live row — and only the row-groups
    /// holding one ([`RowGroupSelection`], STL-155) — the same late-bound,
    /// per-column discipline the bulk materialization uses. Runs before bulk
    /// materialization, so a row pruned here never has its payload read.
    fn filter_sealed_by_valid(
        &self,
        sealed_live: &mut Vec<Version>,
        locator: &BTreeMap<VersionId, (usize, usize)>,
        point: ValidTimeMicros,
    ) -> Result<(), ScanError> {
        let live_rows = live_rows_by_segment(sealed_live, locator);
        let mut valid_by_seg: BTreeMap<usize, (RowGroupSelection, Vec<i64>, Vec<i64>)> =
            BTreeMap::new();
        for (seg_idx, rows) in live_rows {
            let sel = RowGroupSelection::new(&self.segments[seg_idx].row_group_row_counts(), &rows);
            let from = read_i64_column(&self.segments[seg_idx], ColumnId::ValidFrom, &sel.groups)?;
            let to = read_i64_column(&self.segments[seg_idx], ColumnId::ValidTo, &sel.groups)?;
            // The valid-time columns share each row-group's row count, the same
            // contract the bulk columns honor; a disagreement is a corrupt
            // segment, surfaced rather than indexed past its end below.
            if from.len() != sel.selected_rows || to.len() != sel.selected_rows {
                return Err(ScanError::Segment(SegmentError::Corrupt(
                    "valid-time column value count disagrees with the selected row-groups' row count",
                )));
            }
            valid_by_seg.insert(seg_idx, (sel, from, to));
        }
        sealed_live.retain(|v| {
            let (seg_idx, row_idx) = locate(locator, v);
            let (sel, from, to) = &valid_by_seg[&seg_idx];
            let local_idx = sel.local_index(row_idx);
            valid_contains(from[local_idx], to[local_idx], point)
        });
        Ok(())
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

/// Resolve the valid axis over the delta tier's system-live rows: keep each
/// version whose framed `[valid_from, valid_to)` interval contains `point`, and
/// replace its payload with the bare user value (the 16-byte interval prefix
/// stripped). A valid-time table's delta payload is always framed
/// ([`frame_payload`](stele_storage::validtime::frame_payload)), so the interval
/// is present; a row that carries no payload at all (unreachable from v0.1
/// valid-time DML) decodes as a truncated frame and surfaces as
/// [`ScanError::ValidTime`].
fn filter_delta_by_valid(
    versions: Vec<Version>,
    point: ValidTimeMicros,
) -> Result<Vec<Version>, ScanError> {
    let mut kept = Vec::with_capacity(versions.len());
    for mut v in versions {
        let (from, to) = {
            let stored = v.payload.as_deref().unwrap_or_default();
            let (interval, _user) = unframe_payload(true, stored)?;
            let interval = interval.expect("a valid-time table's payload frames an interval");
            (interval.from.0, interval.to.0)
        };
        if valid_contains(from, to, point) {
            // Strip the 16-byte interval prefix in place — `unframe_payload`
            // above already proved the payload is at least that long, so the
            // drain reuses the row's existing buffer rather than allocating a
            // fresh bare copy.
            if let Some(payload) = v.payload.as_mut() {
                payload.drain(0..VALID_TIME_PREFIX_LEN);
            }
            kept.push(v);
        }
    }
    Ok(kept)
}

/// Project one `i64` column out of a sealed segment's selected row-groups,
/// erroring if it decoded as any other [`ColumnData`] variant. The valid-axis
/// filter's narrow read of the `valid_from` / `valid_to` columns; a non-`i64`
/// result is a corrupt segment (the writer types both as `i64`).
fn read_i64_column<F: DiskFile>(
    reader: &SegmentReader<F>,
    col: ColumnId,
    row_groups: &BTreeSet<usize>,
) -> Result<Vec<i64>, ScanError> {
    match reader.read_column_in_row_groups(col, row_groups)? {
        ColumnData::I64(v) => Ok(v),
        ColumnData::Bytes(_) | ColumnData::NullableBytes(_) => Err(ScanError::Segment(
            SegmentError::Corrupt("valid-time column was not stored as i64"),
        )),
    }
}

/// Group the resolved sealed-live rows by their source segment — the per-segment
/// row sets late materialization and the valid-axis filter scope their column
/// reads by ([`RowGroupSelection`], STL-155).
fn live_rows_by_segment(
    sealed_live: &[Version],
    locator: &BTreeMap<VersionId, (usize, usize)>,
) -> BTreeMap<usize, BTreeSet<usize>> {
    let mut live_rows: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    for v in sealed_live {
        let (seg_idx, row_idx) = locate(locator, v);
        live_rows.entry(seg_idx).or_default().insert(row_idx);
    }
    live_rows
}

/// One scanned segment's live rows mapped onto the row-groups that hold them
/// (STL-155): which row-groups a column read must touch, and how a
/// segment-global row index translates into the concatenated read result
/// [`SegmentReader::read_column_in_row_groups`] returns.
struct RowGroupSelection {
    /// The row-groups holding at least one live row, ascending.
    groups: BTreeSet<usize>,
    /// Each row-group's segment-global starting row — the prefix sums of
    /// [`SegmentReader::row_group_row_counts`].
    starts: Vec<usize>,
    /// For each selected row-group, the offset of its first value within the
    /// concatenated scoped read.
    local_base: BTreeMap<usize, usize>,
    /// Total rows across the selected row-groups — the value count every
    /// scoped column read must agree with.
    selected_rows: usize,
}

impl RowGroupSelection {
    /// Map `live_rows` (segment-global row indices) onto their owning
    /// row-groups, given the segment's per-row-group `counts`.
    fn new(counts: &[u32], live_rows: &BTreeSet<usize>) -> Self {
        let mut starts = Vec::with_capacity(counts.len());
        let mut next_start = 0usize;
        for &count in counts {
            starts.push(next_start);
            next_start += count as usize;
        }
        let mut groups = BTreeSet::new();
        for &row in live_rows {
            groups.insert(group_of(&starts, row));
        }
        let mut local_base = BTreeMap::new();
        let mut selected_rows = 0usize;
        for &group in &groups {
            local_base.insert(group, selected_rows);
            selected_rows += counts[group] as usize;
        }
        Self {
            groups,
            starts,
            local_base,
            selected_rows,
        }
    }

    /// Translate a segment-global row index into its position within the
    /// concatenated selected-row-group read.
    fn local_index(&self, row: usize) -> usize {
        let group = group_of(&self.starts, row);
        self.local_base[&group] + (row - self.starts[group])
    }
}

/// The row-group containing segment-global `row`, given the row-groups'
/// starting rows: the last start at or below `row`. `starts` is non-empty and
/// begins at 0, and every `row` comes from this segment's own identity columns,
/// so the search cannot miss.
fn group_of(starts: &[usize], row: usize) -> usize {
    starts.partition_point(|&start| start <= row) - 1
}

/// The half-open `[from, to)` membership test on raw valid-time boundary
/// microseconds — the i64-column mirror of
/// [`ValidInterval::contains`](stele_storage::validtime::ValidInterval::contains),
/// for the sealed tier whose interval lives in columns rather than a framed
/// payload.
const fn valid_contains(from: i64, to: i64, point: ValidTimeMicros) -> bool {
    from <= point.0 && point.0 < to
}

/// Build one projected column by reading `col` from every resolved row.
fn build_column(col: ColumnId, rows: &[Version]) -> Result<Column, ScanError> {
    Ok(match col {
        ColumnId::BusinessKey => Column::Bytes(
            rows.iter()
                .map(|v| Some(v.business_key.as_bytes().to_vec()))
                .collect(),
        ),
        ColumnId::SysFrom => Column::I64(rows.iter().map(|v| v.sys_from.0).collect()),
        // `seq` / `txn_id` are logically `u64`; carry their `i64` bit pattern,
        // the lossless round-trip the segment format defines ([`ColumnId::Seq`]).
        ColumnId::Seq => Column::I64(rows.iter().map(|v| u64_bits(v.seq)).collect()),
        // The payload is already `Option`: a `None` is a SQL `NULL` cell, carried
        // through verbatim ([STL-154]).
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
                .map(|v| Some(v.provenance.principal.as_bytes().to_vec()))
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
        // Placeholder payload — late materialization overwrites it for every row
        // that resolves live, so the unmaterialized stand-in is left `None`.
        None,
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

/// The number of values in a [`ColumnData`], independent of its element type —
/// used to assert a late-materialized column agrees with the segment's identity
/// row count before any row is indexed.
fn column_len(data: &ColumnData) -> usize {
    match data {
        ColumnData::Bytes(v) => v.len(),
        ColumnData::NullableBytes(v) => v.len(),
        ColumnData::I64(v) => v.len(),
    }
}

/// Overlay one late-materialized bulk column onto a resolved version at `row`.
/// `col` is always one of [`is_bulk_column`]'s set (the only columns
/// [`SnapshotScan::materialized_columns`] keeps) and `data` always has the type
/// that column's arm expects — [`SegmentReader::read_column`] picks the
/// [`ColumnData`] variant from [`ColumnId::ty`], so a mismatched pairing cannot
/// arise. The fallthrough is therefore dead and fails fast rather than silently
/// dropping a column, which would mask schema drift (a new bulk column added to
/// [`is_bulk_column`] but not handled here).
fn patch_version(v: &mut Version, col: ColumnId, data: &ColumnData, row: usize) {
    match (col, data) {
        // The payload arrives as `NullableBytes` (it is the one column that can be
        // SQL `NULL`, [STL-154]); a `None` cell patches a NULL payload straight in.
        (ColumnId::Payload, ColumnData::NullableBytes(b)) => v.payload.clone_from(&b[row]),
        (ColumnId::Principal, ColumnData::Bytes(b)) => {
            v.provenance.principal = Principal::new(b[row].clone());
        }
        (ColumnId::TxnId, ColumnData::I64(x)) => v.provenance.txn_id = TxnId(bits_u64(x[row])),
        (ColumnId::CommittedAt, ColumnData::I64(x)) => {
            v.provenance.committed_at = SystemTimeMicros(x[row]);
        }
        _ => unreachable!(
            "patch_version received a non-bulk column or a ColumnData variant that \
             read_column cannot produce for it: {col:?}"
        ),
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
        // A SQL `NULL` payload has no comparable value, so the row filter cannot
        // decide on it — return `None` to keep the row conservatively ([STL-154]).
        ColumnId::Payload => return v.payload.clone().map(ZoneBound::Bytes),
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
