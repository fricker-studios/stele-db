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
//! prune segments (zone maps) → prune row-groups (zone maps) → prune segments
//!   (validity index) → resolve per key at S from the identity columns →
//!   late-materialize the projected columns of the live rows + delta → filter by
//!   pushed-down predicate → project → Arrow batch
//! ```
//!
//! 1. **Prune.** Each sealed segment runs three complementary skip tests before
//!    any bulk column is read (STL-146, STL-173). First, [`SegmentReader::might_contain`]
//!    — a segment-level zone-map check ("begins after `S`", plus value bounds)
//!    that touches no column chunk. A segment that survives is then pruned again
//!    at *row-group* granularity ([`SegmentReader::row_group_zone_maps`], STL-173):
//!    a row-group whose own min/max prove no visible match is dropped even when
//!    the segment-level fold cannot, so only the candidate row-groups' narrow
//!    `(business_key, sys_from, seq)` identity columns are read. Those identities
//!    feed [`ValidityIndex::sys_upper_bound`] (STL-139), which can prove every
//!    surviving version is already superseded at `S` ("ends before `S`") and skip
//!    the bulk chunks. [`ScanStats`] partitions both the segments and the
//!    row-groups across the prunes and the survivors.
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
//! value, the framing prefix stripped.
//!
//! A read of a valid-time table with **no** valid instant — a plain `SELECT`,
//! declared with [`valid_time(true)`](SnapshotScan::valid_time) — still strips the
//! delta tier's framing prefix (so the payload decodes), but keeps every
//! system-live version: the period columns read back as ordinary value cells, no
//! valid-axis filter applied ([STL-218]). Only a genuinely system-only table
//! (`valid_time` unset / `false`) is untouched by any of this — its payload is
//! bare already.
//!
//! ## Determinism
//!
//! The operator reads the validity index (deterministic [`Disk`] I/O) and holds
//! no runtime or wall-clock dependency, so it runs under the simulation
//! scheduler like the rest of the storage/txn core
//! ([architecture §12 invariant 7](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//!
//! ## Granularity (STL-146 → STL-155 → STL-197)
//!
//! Late materialization is per *column* (STL-146) **and** per *row-group*
//! (STL-155): a survivor reads only the projected columns, and within each
//! column only the chunks of row-groups that hold a live row. A chunk is the
//! format's I/O unit, so row-group granularity is the finest skipping the
//! on-disk layout admits — a one-row-group segment degenerates to the STL-146
//! behavior, and the gain appears once a writer bounds its row-groups
//! ([`SegmentWriter::with_max_row_group_rows`](stele_storage::segment::SegmentWriter::with_max_row_group_rows)).
//! `Engine::flush` now bounds them by default (STL-197), so production segments
//! split into skippable row-groups rather than sealing as one.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

use stele_common::metrics::SharedMetrics;
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

/// A shared, immutable, windowed cell buffer — the zero-copy backing store of a
/// [`Column`] ([STL-191]).
///
/// The cells live in one reference-counted allocation (`Arc<[T]>`); a `Cells`
/// value is a `(offset, len)` window over it. Cloning bumps the refcount and
/// [`slice`](Self::slice) narrows the window — neither touches a cell, so
/// cutting a resolved column into pull-pipeline batches ([`crate::ScanSource`])
/// and re-emitting columns through [`Project`](crate::Project) never copy
/// payload bytes. The buffer is never mutated after construction, the same
/// immutability an Arrow buffer guarantees (assumption A7).
///
/// It dereferences to the windowed `[T]` slice, so reading code treats it
/// exactly like the `Vec` it replaced (`iter()`, indexing, `len()`); equality
/// and `Debug` also see only the window, never the shared allocation around it.
pub struct Cells<T> {
    buf: Arc<[T]>,
    offset: usize,
    len: usize,
}

impl<T> Cells<T> {
    /// The windowed cells as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.buf[self.offset..self.offset + self.len]
    }

    /// A `len`-cell window starting at `offset` (relative to this window),
    /// sharing the backing buffer — a refcount bump, no cell is copied.
    ///
    /// # Panics
    ///
    /// If `offset + len` exceeds this window's length.
    #[must_use]
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.checked_add(len).is_some_and(|end| end <= self.len),
            "slice [{offset}, {offset}+{len}) out of bounds of a {}-cell window",
            self.len
        );
        Self {
            buf: Arc::clone(&self.buf),
            offset: self.offset + offset,
            len,
        }
    }
}

impl<T> From<Vec<T>> for Cells<T> {
    fn from(cells: Vec<T>) -> Self {
        let len = cells.len();
        Self {
            buf: cells.into(),
            offset: 0,
            len,
        }
    }
}

impl<T> FromIterator<T> for Cells<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        iter.into_iter().collect::<Vec<T>>().into()
    }
}

impl<T> std::ops::Deref for Cells<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<'a, T> IntoIterator for &'a Cells<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

// Manual impls so none of them bounds `T: Clone` and all of them see the
// window, not the backing allocation: two windows are equal iff their visible
// cells are, regardless of how they share buffers.
impl<T> Clone for Cells<T> {
    fn clone(&self) -> Self {
        Self {
            buf: Arc::clone(&self.buf),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Cells<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

impl<T: PartialEq> PartialEq for Cells<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for Cells<T> {}

/// One column of a [`Batch`] — Arrow-shaped: a single typed, contiguous array
/// whose length equals the batch's row count.
///
/// Backed by a shared [`Cells`] buffer, so cloning, slicing, and re-projecting
/// a column are shallow refcount operations ([STL-191]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Column {
    /// A variable-length bytes column (business key, payload, principal). Each
    /// cell is `Option<Vec<u8>>` so a SQL `NULL` payload ([STL-154]) is carried
    /// as `None`, distinct from `Some(vec![])` (an empty value). The always-present
    /// columns (business key, principal) only ever hold `Some`.
    Bytes(Cells<Option<Vec<u8>>>),
    /// A fixed-width `i64` column (system time, seq, provenance scalars). `u64`
    /// columns (`seq`, `txn_id`) are carried as their `i64` bit-reinterpretation,
    /// the same lossless round-trip the segment format uses ([`ColumnId::TxnId`]).
    I64(Cells<i64>),
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
    /// Zero-copy: the window shares the column's [`Cells`] buffer ([STL-191]),
    /// so slicing costs a refcount bump however large the window — no payload
    /// byte is copied.
    ///
    /// # Panics
    ///
    /// If `offset + len` exceeds the column's length — the caller
    /// ([`crate::ScanSource`]) only ever slices within the resolved row count.
    #[must_use]
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        match self {
            Self::Bytes(v) => Self::Bytes(v.slice(offset, len)),
            Self::I64(v) => Self::I64(v.slice(offset, len)),
        }
    }

    /// Gather the cells at `rows`, in the given order, into a new column — the
    /// **materialization** primitive that resolves a selection-vector batch into a
    /// dense one ([`Batch::into_dense`], STL-214). Unlike [`slice`](Self::slice) a
    /// gather is not a window — the kept rows are not contiguous — so it does copy
    /// the selected cells into a fresh buffer.
    ///
    /// A [`Filter`](crate::Filter) no longer calls this per batch: it keeps the
    /// child's column buffers untouched and carries the surviving row indices as a
    /// [`Batch::selection`] instead, so a downstream consumer that honors the
    /// selection (the engine sink) never pays for this copy at all. The gather
    /// happens once, lazily, only when a consumer actually needs a dense column.
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
/// materializes. v0.1 emits a single batch even when it is small (STL-100 scope);
/// a batch-at-a-time iterator is a later refinement.
///
/// ## Dense vs. selected ([`selection`](Self::selection), STL-214)
///
/// A batch is one of two shapes:
///
/// * **Dense** (`selection == None`) — every column holds exactly
///   [`rows`](Self::rows) values, aligned row-wise: logical row `i` is physical
///   row `i` in every column. A scan source and [`ExplodePayload`](crate::ExplodePayload)
///   emit this shape.
/// * **Selected** (`selection == Some(sel)`) — the columns are the *full* buffers
///   of some upstream batch, carried by move or a shallow [`Cells`] clone (never a
///   payload-byte copy), and `sel` names the surviving rows: logical row `i` is
///   physical row `sel[i]`, with `rows == sel.len()`. A [`Filter`](crate::Filter)
///   emits this so it never deep-copies a surviving cell; a
///   [`Project`](crate::Project) passes whichever shape its child emits straight
///   through.
///
/// A consumer either **honors** the selection — reading cell `(col, i)` as
/// `column[selection[i]]` via [`physical_row`](Self::physical_row) — or
/// **materializes** it once with [`into_dense`](Self::into_dense), which gathers
/// the surviving cells into fresh dense columns. Both yield the same rows; the
/// difference is only whether the surviving payload bytes are ever copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// The projected columns, in projection order. In a dense batch each holds
    /// [`rows`](Self::rows) values; in a selected batch each holds the full
    /// upstream buffer, addressed through [`selection`](Self::selection).
    pub columns: Vec<(ColumnId, Column)>,
    /// Number of **logical** (output) rows — the count a consumer iterates. In a
    /// selected batch this is the selection length, not the columns' physical
    /// height.
    pub rows: usize,
    /// The surviving rows' physical indices into [`columns`](Self::columns), or
    /// `None` for a dense batch. When `Some`, `rows == selection.len()` and the
    /// columns are shared, full-height upstream buffers (STL-214). Held as a
    /// shared [`Cells`] so propagating it through a [`Project`](crate::Project) is
    /// itself a refcount bump.
    pub selection: Option<Cells<usize>>,
}

impl Batch {
    /// A dense batch: `columns` are exactly `rows` tall and addressed directly.
    #[must_use]
    pub const fn new(columns: Vec<(ColumnId, Column)>, rows: usize) -> Self {
        Self {
            columns,
            rows,
            selection: None,
        }
    }

    /// A selected batch over `columns` (shared, full-height upstream buffers),
    /// restricted to the rows `selection` names — the zero-copy row selection a
    /// [`Filter`](crate::Filter) emits. `rows` is taken from the selection length;
    /// the cells themselves are never copied.
    #[must_use]
    pub fn with_selection(columns: Vec<(ColumnId, Column)>, selection: Cells<usize>) -> Self {
        Self {
            rows: selection.len(),
            columns,
            selection: Some(selection),
        }
    }

    /// Map a logical (output) row index to its physical index into
    /// [`columns`](Self::columns): `selection[logical]` for a selected batch, the
    /// identity for a dense one. A consumer that honors the selection reads
    /// cell `(col, logical)` as `column[batch.physical_row(logical)]`.
    #[must_use]
    pub fn physical_row(&self, logical: usize) -> usize {
        self.selection.as_ref().map_or(logical, |sel| sel[logical])
    }

    /// Resolve a selection into a **dense** batch, gathering each column's
    /// surviving cells once ([`Column::take`]); a no-op (returns `self`) when the
    /// batch is already dense, so the common path copies nothing. This is the
    /// "materialize once at the pipeline sink" form — a consumer that does not
    /// honor the selection calls this to read columns directly.
    #[must_use]
    pub fn into_dense(self) -> Self {
        match self.selection {
            None => self,
            Some(sel) => Self::new(
                self.columns
                    .into_iter()
                    .map(|(id, col)| (id, col.take(&sel)))
                    .collect(),
                self.rows,
            ),
        }
    }
}

/// A **zero-copy gathered view** of one join side's output columns ([STL-224]).
///
/// This is the keep-the-buffer-carry-indices row selection a
/// [`Filter`](crate::Filter) emits ([STL-214]), generalized to the hash join's
/// output assembly. It holds the side's full columns as shared [`Cells`] buffers
/// (constructing one is a
/// refcount bump per column, no payload byte copied) paired with a **nullable
/// selection**: output row `i` draws physical row `selection[i]` from every column,
/// or a SQL `NULL` cell when `selection[i]` is `None`. The two extra degrees of
/// freedom over a [`Batch::selection`] are exactly what a join needs and a `Filter`
/// does not — an index may **repeat** (a one-to-many match emits the same left row
/// more than once) and may be **absent** (`None` is a `LEFT` join's `NULL`-extended
/// right side).
///
/// Reading a surviving cell ([`bytes`](Self::bytes)) borrows straight from the
/// shared buffer, so the join references each matched row's cells by index rather
/// than re-allocating them; the single owning copy happens once downstream when the
/// engine materializes the wire `SelectResult`, the same place the `Filter` path
/// materializes ([STL-214]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatheredColumns {
    /// The side's columns, full height — shared [`Cells`] buffers addressed through
    /// [`selection`](Self::selection).
    columns: Vec<Column>,
    /// One entry per output row: the physical row each column is read at, or `None`
    /// for a `NULL`-extended (`LEFT` join unmatched) output row.
    selection: Vec<Option<usize>>,
}

impl GatheredColumns {
    /// A gathered view over `columns` (shared, full-height buffers) restricted and
    /// reordered by `selection` — output row `i` is physical row `selection[i]`, a
    /// `None` naming a `NULL` cell. The cells themselves are never copied.
    #[must_use]
    pub const fn new(columns: Vec<Column>, selection: Vec<Option<usize>>) -> Self {
        Self { columns, selection }
    }

    /// The number of output (logical) rows — the selection length, not the columns'
    /// physical height.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.selection.len()
    }

    /// Whether the gather names no output rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    /// The bytes of column `col` at output `row`, **borrowed** from the shared
    /// buffer — `None` for a `NULL` cell (a `NULL`-extended output row, or a stored
    /// `NULL` payload). No payload byte is copied: the returned slice points into
    /// the same allocation as the source cell.
    ///
    /// # Panics
    ///
    /// If `row` is out of range, the named physical row is out of the column's
    /// height, or `col` is not a [`Column::Bytes`] — the join only ever gathers the
    /// opaque bytes columns its scan produced, at indices its own row counts bound.
    #[must_use]
    pub fn bytes(&self, col: usize, row: usize) -> Option<&[u8]> {
        // Resolve the column's type *before* the nullable selection, so a non-bytes
        // column is a contract break regardless of whether `row` is NULL-extended (a
        // type check inside the `and_then` would silently pass for a `None` slot).
        let Column::Bytes(cells) = &self.columns[col] else {
            panic!("join gather over a non-bytes column {col}");
        };
        self.selection[row].and_then(|physical| cells[physical].as_deref())
    }
}

/// Per-scan statistics — chiefly the segment-pruning accounting.
///
/// A segment reaches the scan in one of four states, and the counts partition
/// the segment set exactly: `segments_total == segments_pruned_zone +
/// segments_pruned_bloom + segments_pruned_superseded + segments_scanned`.
///
/// Three complementary prunes run before any bulk column is read (STL-146,
/// STL-238), none touching a column chunk:
///
/// * **Zone-map prune** ([`segments_pruned_zone`](Self::segments_pruned_zone)) —
///   the zone maps prove the segment holds no row visible at the snapshot that
///   satisfies the predicate ("begins after the snapshot", plus value bounds).
/// * **Bloom-filter prune** ([`segments_pruned_bloom`](Self::segments_pruned_bloom),
///   STL-238) — for a *point* business-key predicate the segment's footer bloom
///   ([`SegmentReader::might_contain_key`]) proves the key is in no version of
///   this segment. This catches the random/hash-key case the zone map cannot: a
///   hash key scatters across the `[min, max]` range every segment spans, so the
///   value bounds never prune it.
/// * **Validity-index prune**
///   ([`segments_pruned_superseded`](Self::segments_pruned_superseded)) — the
///   segment survives the zone map, but reading only its narrow identity columns
///   ([`SegmentReader::version_keys`]) lets [`ValidityIndex::sys_upper_bound`]
///   prove *every* version is already superseded at the snapshot ("ends before
///   the snapshot", STL-139). The bulk column chunks are never read.
///
/// What remains is [`segments_scanned`](Self::segments_scanned): the segments
/// whose projected columns are materialized for the rows that survive resolution.
///
/// ## Row-group accounting (STL-173)
///
/// Within a segment that survives the segment-level zone prune, the scan prunes
/// again at *row-group* granularity: each row-group's own zone map
/// ([`SegmentReader::row_group_zone_maps`]) can prove no visible matching row
/// even when the segment-level fold (whose `[min, max]` spans every row-group)
/// cannot. The three `row_groups_*` counts partition the row-groups of exactly
/// the segment-level zone survivors —
/// `row_groups_total == row_groups_pruned_zone + row_groups_scanned` — and a
/// segment whose *every* row-group is pruned this way is itself counted in
/// [`segments_pruned_zone`](Self::segments_pruned_zone) (no identity chunk read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanStats {
    /// Total sealed segments offered to the scan.
    pub segments_total: usize,
    /// Segments the zone maps proved could hold no visible match — skipped with
    /// no read I/O at all. Includes segments ruled out wholesale at the segment
    /// level *and* segments every one of whose row-groups the per-row-group zone
    /// maps then ruled out (STL-173) — neither has an identity chunk read.
    pub segments_pruned_zone: usize,
    /// Segments the footer bloom proved hold the probed point business key in no
    /// version — skipped with no read I/O (STL-238). Only ever non-zero for a
    /// point business-key predicate against a segment carrying a bloom; the
    /// hash-key acceleration zone maps cannot give.
    pub segments_pruned_bloom: usize,
    /// Segments that survived the zone map but whose every version the validity
    /// index proved superseded at the snapshot — skipped after reading only the
    /// narrow identity columns, never the bulk chunks (STL-139).
    pub segments_pruned_superseded: usize,
    /// Segments the per-segment valid-time interval summary proved hold no row
    /// valid at the pinned valid instant — skipped with no read I/O (STL-241).
    /// Only ever non-zero for a `FOR VALID_TIME AS OF v` read against a segment
    /// carrying a summary; this is the backdated-write scatter case the
    /// `valid_from` / `valid_to` zone-map min/max cannot prune.
    pub segments_pruned_valid: usize,
    /// Segments that survived both prunes — their projected columns are read for
    /// any row live at the snapshot.
    pub segments_scanned: usize,
    /// Row-groups across the segment-level zone survivors — the denominator the
    /// two row-group counts below partition (STL-173). Excludes the row-groups of
    /// segments pruned wholesale at the segment level (those are never examined
    /// at row-group granularity).
    pub row_groups_total: usize,
    /// Row-groups a per-row-group zone map proved hold no visible match — their
    /// identity (and bulk) chunks are never read.
    pub row_groups_pruned_zone: usize,
    /// Row-groups whose narrow identity columns were read to resolve the snapshot
    /// (the candidates that survived the per-row-group prune).
    pub row_groups_scanned: usize,
}

impl ScanStats {
    /// Segments skipped by any prune (`segments_pruned_zone +
    /// segments_pruned_bloom + segments_pruned_superseded + segments_pruned_valid`)
    /// — the count that never had its bulk columns materialized.
    #[must_use]
    pub const fn segments_pruned(&self) -> usize {
        self.segments_pruned_zone
            + self.segments_pruned_bloom
            + self.segments_pruned_superseded
            + self.segments_pruned_valid
    }

    /// Add this run's accounting into the process-wide scan counters
    /// ([STL-253]).
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    pub fn record(&self, metrics: &stele_common::metrics::Metrics) {
        metrics
            .scan_segments_scanned
            .add(self.segments_scanned as u64);
        metrics
            .scan_segments_pruned_zone
            .add(self.segments_pruned_zone as u64);
        metrics
            .scan_segments_pruned_bloom
            .add(self.segments_pruned_bloom as u64);
        metrics
            .scan_segments_pruned_superseded
            .add(self.segments_pruned_superseded as u64);
        metrics
            .scan_segments_pruned_valid
            .add(self.segments_pruned_valid as u64);
        metrics
            .scan_row_groups_scanned
            .add(self.row_groups_scanned as u64);
        metrics
            .scan_row_groups_pruned_zone
            .add(self.row_groups_pruned_zone as u64);
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
    /// [`valid_as_of`](Self::valid_as_of). `None` leaves the valid axis
    /// unfiltered. `Some(v)` turns on both-axes resolution: after the
    /// system-time live set is resolved, each version is kept only when its
    /// `[valid_from, valid_to)` interval contains `v`.
    valid_snapshot: Option<ValidTimeMicros>,
    /// Whether the scanned table opts into valid-time, set via
    /// [`valid_time`](Self::valid_time). It governs the delta tier's framing
    /// independently of [`valid_snapshot`](Self::valid_snapshot): a valid-time
    /// table's delta payload is always framed, so even a **no-pin** read (a plain
    /// `SELECT`, `valid_snapshot == None`) must strip the 16-byte prefix or
    /// `ExplodePayload` decodes the envelope as row data ([STL-218]). A
    /// `valid_as_of` pin implies a valid-time table, so it sets this too; a
    /// system-only table leaves it `false` and its payload is bare already.
    valid_time: bool,
    projection: Vec<ColumnId>,
    predicate: Predicate,
    /// The session's shared metric registry, when installed via
    /// [`metrics`](Self::metrics) ([STL-253]): [`execute`](Self::execute)
    /// reports its pruning [`ScanStats`] into the process-wide scan counters.
    /// Pure atomic bumps, so an uninstrumented scan (tests, the simulator) is
    /// byte-identical with an instrumented one.
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    metrics: Option<SharedMetrics>,
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
            valid_time: false,
            projection: ColumnId::ALL.to_vec(),
            predicate: Predicate::All,
            metrics: None,
        }
    }

    /// Report this scan's pruning [`ScanStats`] into `metrics`'s process-wide
    /// scan counters when it executes ([STL-253]).
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    #[must_use]
    pub fn metrics(mut self, metrics: SharedMetrics) -> Self {
        self.metrics = Some(metrics);
        self
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
        // A valid pin is only ever supplied for a valid-time table, so the delta
        // tier's framing is in play regardless — keep the two flags consistent.
        self.valid_time = true;
        self
    }

    /// Declare whether the scanned table opts into valid-time, independently of a
    /// [`valid_as_of`](Self::valid_as_of) pin.
    ///
    /// A valid-time table's delta rows carry the `[valid_from, valid_to)` interval
    /// framed on the payload ([`frame_payload`](stele_storage::validtime::frame_payload)).
    /// A scan with no valid pin (`valid_as_of` unset — a plain `SELECT`) still has
    /// to strip that 16-byte prefix so the emitted [`ColumnId::Payload`] is the
    /// **bare** user value, matching what a sealed segment already stores;
    /// otherwise the row codec decodes the temporal envelope as data ([STL-218]).
    /// This strips *without* filtering on the valid axis — a no-pin read returns
    /// every system-live version, its period columns readable as ordinary cells.
    /// A [`valid_as_of`](Self::valid_as_of) pin sets this implicitly; call it
    /// explicitly for the no-pin read of a valid-time table.
    ///
    /// **Pass the table's actual policy, not `true` unconditionally.** `enabled`
    /// must mirror the table's valid-time opt-in: with no pin it decides whether
    /// the delta payload is unframed. Setting `true` for a **system-only** table —
    /// whose payload is already bare — makes a no-pin scan drain 16 bytes of real
    /// row data as a phantom prefix, erroring or returning a corrupt payload;
    /// setting `false` for a valid-time table leaves the frame on and the row
    /// codec fails. The engine derives `enabled` from the catalog
    /// (`TableState::valid_time`), so it is always correct there.
    #[must_use]
    pub const fn valid_time(mut self, enabled: bool) -> Self {
        self.valid_time = enabled;
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
        if let Some(m) = &self.metrics {
            stats.record(m);
        }
        let filtered: Vec<Version> = rows
            .into_iter()
            .filter(|v| predicate_matches(&self.predicate, v))
            .collect();
        let batch = self.project_batch(&filtered)?;
        Ok(ScanOutput { batch, stats })
    }

    /// Resolve one version per key, live at the snapshot, across both tiers,
    /// returning the rows alongside the segment-pruning [`ScanStats`].
    /// Prune the sealed segments down to the survivors that may hold a row live
    /// at the snapshot, returning each survivor's candidate identities (paired
    /// with their segment-global row indices) and the pruning [`ScanStats`].
    ///
    /// Complementary skip stages, none of which reads a bulk column chunk: the
    /// segment-level zone map, the per-segment bloom (STL-238), the per-segment
    /// valid-time interval summary on a valid-pinned read (STL-241), the
    /// per-row-group zone maps (STL-173), and the validity index's "every
    /// surviving version superseded" bound (STL-139).
    #[allow(clippy::type_complexity)]
    fn prune_segments(
        &self,
    ) -> Result<(Vec<(usize, Vec<(usize, VersionId)>)>, ScanStats), ScanError> {
        // The predicate the zone maps prune against. A system-only scan prunes
        // against the bare pushed-down predicate, unchanged; a both-axes scan
        // (`valid_as_of` set) conjoins two sound one-sided valid-time skips
        // derived from STL-117's first-class valid_from / valid_to columns. See
        // [`Self::pruning_predicate`].
        let pruning_pred = self.pruning_predicate();
        // The point business key a hash/bloom probe targets, if the predicate
        // pins one ([STL-238]). Computed once: it is predicate-derived, not
        // per-segment. `None` for a range or unconstrained scan, which skip the
        // bloom stage.
        let point_key = predicate_point_key(&pruning_pred);

        let mut pruned_zone = 0usize;
        let mut pruned_bloom = 0usize;
        let mut pruned_superseded = 0usize;
        let mut pruned_valid = 0usize;
        let mut rg_total = 0usize;
        let mut rg_pruned = 0usize;
        let mut rg_scanned = 0usize;
        // (segment index, its surviving rows as (segment-global row index, identity)).
        let mut survivors: Vec<(usize, Vec<(usize, VersionId)>)> = Vec::new();
        for (seg_idx, reader) in self.segments.iter().enumerate() {
            // Stage 1 — segment zone map: rules a whole segment out touching no
            // column chunk.
            if !reader.might_contain(&pruning_pred, self.snapshot) {
                pruned_zone += 1;
                continue;
            }
            // Stage 1b — per-segment bloom ([STL-238]): for a point business-key
            // probe, the footer bloom can prove the key is in no version of this
            // segment — the random/hash-key case the zone map's `[min, max]`
            // cannot rule out. Footer-resident, so still no chunk I/O. A segment
            // with no bloom admits every key, so this never prunes a real match.
            if let Some(key) = &point_key
                && !reader.might_contain_key(key.as_bytes())
            {
                pruned_bloom += 1;
                continue;
            }
            // Stage 1c — per-segment valid-time interval summary ([STL-241]): for
            // a `FOR VALID_TIME AS OF v` read, the footer-resident summary can
            // prove no row in this segment is valid at `v` even when its
            // `valid_from` / `valid_to` zone-map min/max cannot — the backdated
            // scatter case, where a correction lands in today's segment carrying
            // an old valid-time and widens the envelope to span `v` though the
            // actual coverage has a gap there. Footer-resident, so still no chunk
            // I/O. A segment with no summary (system-only, ≤ v11, or a
            // summary-disabled writer) admits every point, so this never prunes a
            // real match.
            if let Some(point) = self.valid_snapshot
                && !reader.might_contain_valid(point.0)
            {
                pruned_valid += 1;
                continue;
            }
            // Stage 2 — per-row-group zone maps (STL-173): rule out the
            // row-groups whose own min/max prove no visible match, even when the
            // segment-level fold (its `[min, max]` spanning every row-group)
            // cannot. Footer-resident, so this costs no chunk I/O — the pruned
            // row-groups' identity chunks are then never read.
            let rg_counts = reader.row_group_row_counts();
            let rg_maps = reader.row_group_zone_maps();
            rg_total += rg_maps.len();
            let candidates: BTreeSet<usize> = (0..rg_maps.len())
                .filter(|&g| rg_maps[g].might_contain(&pruning_pred, self.snapshot))
                .collect();
            rg_pruned += rg_maps.len() - candidates.len();
            if candidates.is_empty() {
                // Every row-group ruled out — the segment holds no visible match
                // even though its coarse segment-level fold could not prove it.
                // Counts as a zone prune: no identity chunk was read.
                pruned_zone += 1;
                continue;
            }
            rg_scanned += candidates.len();
            // Stage 3 — validity index: read only the candidate row-groups'
            // narrow `(business_key, sys_from, seq)` identity columns and ask
            // whether every surviving version is already superseded at the
            // snapshot (STL-139). Complementary to the zone map's "begins after
            // the snapshot" test, this is "ends before the snapshot".
            let keys = reader.version_keys_in_row_groups(&candidates)?;
            if self
                .index
                .sys_upper_bound(keys.iter().cloned())?
                .superseded_at_or_before(self.snapshot.0)
            {
                pruned_superseded += 1;
                continue;
            }
            // Pair each identity with its segment-global row index: the candidate
            // row-groups' rows are contiguous and the scoped read returns them in
            // row-group order, so a global-index walk over the candidates lines
            // up one-to-one with `keys` — the same addressing
            // [`RowGroupSelection`] uses for the bulk read below.
            let globals = global_row_indices(&rg_counts, &candidates);
            if globals.len() != keys.len() {
                return Err(ScanError::Segment(SegmentError::Corrupt(
                    "segment identity row count disagrees with the selected row-groups' row count",
                )));
            }
            survivors.push((seg_idx, globals.into_iter().zip(keys).collect()));
        }
        let stats = ScanStats {
            segments_total: self.segments.len(),
            segments_pruned_zone: pruned_zone,
            segments_pruned_bloom: pruned_bloom,
            segments_pruned_superseded: pruned_superseded,
            segments_pruned_valid: pruned_valid,
            segments_scanned: survivors.len(),
            row_groups_total: rg_total,
            row_groups_pruned_zone: rg_pruned,
            row_groups_scanned: rg_scanned,
        };
        Ok((survivors, stats))
    }

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
        // [`unframe_payload`] and strip the 16-byte prefix so the emitted payload
        // is the bare user value (the sealed tier already stores it bare).
        //  * A valid pin (`valid_as_of`) also keeps only versions whose
        //    `[valid_from, valid_to)` contains the point.
        //  * A no-pin read of a valid-time table (a plain `SELECT`) strips the
        //    prefix from every system-live version without filtering, so the
        //    period columns read back as ordinary cells ([STL-218]).
        //  * A system-only table's payload is already bare — leave it untouched.
        if let Some(point) = self.valid_snapshot {
            delta_live = filter_delta_by_valid(delta_live, point)?;
        } else if self.valid_time {
            delta_live = strip_delta_frames(delta_live)?;
        }

        // The bulk (non-identity) columns a survivor must materialize for this
        // scan — the union of the projected and predicate-referenced columns.
        let needed = self.materialized_columns();

        // Prune the sealed segments (segment- then row-group-level zone maps,
        // then the validity index), keeping each survivor's candidate identities.
        let (survivors, stats) = self.prune_segments()?;

        // Resolve which survivor rows are live at the snapshot from the identity
        // columns alone: `fold_chains` overlays the validity index's closes and
        // `resolve_snapshot` picks, per key, the one version whose system
        // interval contains the snapshot. A side map locates each row's source
        // `(segment, row)` so only the survivors that resolve live ever have
        // their bulk columns read.
        let mut locator: BTreeMap<VersionId, (usize, usize)> = BTreeMap::new();
        let mut identities: Vec<Version> = Vec::new();
        for (seg_idx, keys) in &survivors {
            for (row_idx, (bk, sys_from, seq)) in keys {
                locator.insert((bk.clone(), *sys_from, *seq), (*seg_idx, *row_idx));
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

    /// The predicate the zone maps (segment- and row-group-level) prune against.
    ///
    /// For a system-only scan this is exactly the pushed-down
    /// [`predicate`](Self::filter), so the prune is unchanged from before STL-173.
    /// For a both-axes scan ([`valid_as_of`](Self::valid_as_of) set) it conjoins
    /// the pushed-down predicate with two **sound, one-sided** valid-time skips,
    /// expressed over STL-117's first-class `valid_from` / `valid_to` columns so
    /// they prune through the same generic [`ZoneMap::might_contain`] machinery:
    ///
    /// * `min(valid_from) > v` ⇒ every row's validity begins after `v` ⇒ none is
    ///   valid at `v` — encoded as `valid_from ∈ [i64::MIN, v]`, which the zone
    ///   map can disprove only when the row-group's whole `valid_from` range lies
    ///   above `v`.
    /// * `max(valid_to) <= v` ⇒ every row's validity has ended by `v` (the
    ///   interval is half-open, `point < valid_to`) ⇒ none is valid at `v` —
    ///   encoded as `valid_to ∈ [v + 1, i64::MAX]`.
    ///
    /// Both mirror the half-open membership the row-level valid filter
    /// ([`filter_sealed_by_valid`](Self::filter_sealed_by_valid)) applies, so a
    /// row-group is pruned only when no row it holds could survive that filter —
    /// never a false negative. A table without valid-time columns has no
    /// `valid_from` / `valid_to` zone entry, so these terms simply never prune
    /// (the conservative "no stats ⇒ keep" path), and the valid axis is folded in
    /// only for the prune: the row-level filter still runs on
    /// [`predicate`](Self::filter) alone.
    fn pruning_predicate(&self) -> Predicate {
        let Some(v) = self.valid_snapshot else {
            return self.predicate.clone();
        };
        Predicate::And(vec![
            self.predicate.clone(),
            Predicate::Range {
                column: ColumnId::ValidFrom,
                low: ZoneBound::I64(i64::MIN),
                high: ZoneBound::I64(v.0),
            },
            Predicate::Range {
                column: ColumnId::ValidTo,
                // `v + 1` saturates: at `v == i64::MAX` the low bound becomes
                // i64::MAX, so the term prunes a row-group iff `max(valid_to)`
                // is below i64::MAX — and a half-open interval can never contain
                // i64::MAX anyway, so pruning there is still correct.
                low: ZoneBound::I64(v.0.saturating_add(1)),
                high: ZoneBound::I64(i64::MAX),
            },
        ])
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
        Ok(Batch::new(columns, rows.len()))
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

/// Strip the framed valid-time prefix from every delta row's payload **without**
/// filtering on the valid axis — the no-valid-pin read of a valid-time table (a
/// plain `SELECT`, [STL-218]).
///
/// Like [`filter_delta_by_valid`] but keeping every system-live version: the
/// interval is decoded only to validate (and locate) the 16-byte prefix, then
/// dropped in place so the emitted [`ColumnId::Payload`] is the bare user value
/// the row codec expects. The period bounds remain available as the row's own
/// value cells (they ride the codec payload too), so a plain `SELECT vf, vt`
/// reads them back. A row whose payload is shorter than the prefix (unreachable
/// from valid-time DML, which always frames) surfaces as [`ScanError::ValidTime`]
/// rather than corrupting the row.
fn strip_delta_frames(versions: Vec<Version>) -> Result<Vec<Version>, ScanError> {
    let mut out = Vec::with_capacity(versions.len());
    for mut v in versions {
        // Validate the prefix is present (a clear error on a truncated frame)
        // before draining it; the drain reuses the row's existing buffer.
        let stored = v.payload.as_deref().unwrap_or_default();
        unframe_payload(true, stored)?;
        if let Some(payload) = v.payload.as_mut() {
            payload.drain(0..VALID_TIME_PREFIX_LEN);
        }
        out.push(v);
    }
    Ok(out)
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

/// The segment-global row indices of the selected row-groups' rows, in the
/// row-group (and therefore row) order [`SegmentReader::version_keys_in_row_groups`]
/// returns them — so zipping this with the scoped identity read recovers each
/// surviving row's true position within the segment (STL-173). `counts` is the
/// segment's per-row-group row count ([`SegmentReader::row_group_row_counts`]),
/// and `groups` is the candidate selection, ascending.
fn global_row_indices(counts: &[u32], groups: &BTreeSet<usize>) -> Vec<usize> {
    let mut start = 0usize;
    let mut starts = Vec::with_capacity(counts.len());
    for &count in counts {
        starts.push(start);
        start += count as usize;
    }
    let mut out = Vec::new();
    for &g in groups {
        for local in 0..counts[g] as usize {
            out.push(starts[g] + local);
        }
    }
    out
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
const fn column_len(data: &ColumnData) -> usize {
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

/// The single business key a predicate pins to a *point* — `Some(k)` when it
/// constrains the business key to exactly one value (a literal `= k`, or the
/// degenerate `[k, k]` window an index point probe produces), else `None`.
///
/// This is the one predicate shape the per-segment bloom can answer ([STL-238]):
/// a bloom tests membership of a single key, so a range or an unconstrained scan
/// has no point to probe and skips the bloom stage entirely.
fn predicate_point_key(predicate: &Predicate) -> Option<BusinessKey> {
    match predicate_key_range(predicate) {
        (Bound::Included(low), Bound::Included(high)) if low == high => Some(low),
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
