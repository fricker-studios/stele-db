//! The vectorized **operator framework** — a Volcano-style, batch-at-a-time
//! *pull* pipeline ([architecture §6](../../../docs/02-architecture.md#6-query-layer),
//! assumption A7: Arrow-shaped batches) (STL-169).
//!
//! Every physical operator implements one trait, [`Operator`], whose single
//! method [`next`](Operator::next) pulls the next [`Batch`] from upstream or
//! reports end-of-stream. A query plan is a chain of operators: a *source*
//! ([`ScanSource`], wrapping a [`SnapshotScan`]) at the bottom, with shaping
//! operators ([`Project`], and — on later tickets — filter / aggregate / join)
//! stacked above. Pulling the top operator drives the whole pipeline one batch
//! at a time.
//!
//! ```text
//! Project { columns }          ← caller pulls here
//!   └── ScanSource { batch_rows }
//!         └── SnapshotScan (resolve at the MVCC snapshot)
//! ```
//!
//! ## Batch-at-a-time, not row-at-a-time
//!
//! Operators move [`Batch`]es, not rows — a batch holds up to a configurable
//! number of rows ([`ScanSource::new`]'s `batch_rows`), so the per-call overhead
//! of the pull model is amortized across many rows and the columnar arrays stay
//! SIMD-friendly. [`DEFAULT_BATCH_SIZE`] is the default chunk size.
//!
//! ## What this ticket lands (and what it defers)
//!
//! [`ScanSource`] is *eager at the source*: on its first [`next`](Operator::next)
//! it runs the underlying [`SnapshotScan::execute`] once, then hands out the
//! resolved rows in `batch_rows`-sized windows on each subsequent pull. The
//! concatenation of the emitted batches is therefore byte-for-byte the single
//! batch [`execute`](SnapshotScan::execute) returns today, which is exactly the
//! result-equivalence the DoD requires and keeps the DuckDB differential oracle
//! green. True *streaming* resolution — producing batches without first
//! materializing the whole result — is a later refinement that needs
//! chunk-level row addressing the segment reader does not yet expose (the same
//! follow-up noted in [`crate::SnapshotScan`]'s docs); the operator *interface*
//! here is what the aggregate / join / filter operators (STL-77 C10–C13) build
//! on, independent of how the source fills its batches.
//!
//! ## Determinism
//!
//! The pipeline adds no runtime or wall-clock dependency over [`SnapshotScan`],
//! so it runs under the deterministic simulation scheduler like the rest of the
//! storage/txn core
//! ([architecture §12 invariant 7](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).

use std::collections::BTreeSet;

use stele_common::row_codec;
use stele_common::types::LogicalType;
use stele_storage::backend::{Disk, DiskFile};
use stele_storage::segment::ColumnId;

use crate::expr::{Expr, ExprError, Vector, eval_expr};
use crate::snapshot_scan::{Batch, Column, ScanError, ScanStats, SnapshotScan};

/// Default rows per emitted [`Batch`] when a caller picks no other size.
///
/// Large enough to amortize the pull-loop overhead, small enough to bound peak
/// per-batch memory.
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// A node in the batch-at-a-time pull pipeline.
///
/// [`next`](Self::next) returns the operator's next [`Batch`], or `None` once
/// the stream is exhausted. Implementors never emit an empty (`rows == 0`)
/// batch: end-of-stream is `None`, so a consumer can loop `while let Some(b) =
/// op.next()? {}` without special-casing empties.
///
/// The trait is object-safe (`&mut self`, no generic methods), so a plan can be
/// erased to `Box<dyn Operator>` when its shape is only known at runtime.
pub trait Operator {
    /// Pull the next batch, or `None` at end of stream.
    ///
    /// # Errors
    ///
    /// [`ScanError`] propagated from the source's tier reads, or
    /// [`ScanError::MissingColumn`] if a shaping operator references a column its
    /// child did not emit.
    fn next(&mut self) -> Result<Option<Batch>, ScanError>;
}

/// The fully resolved scan output, plus the cursor into it. Filled lazily on the
/// first [`Operator::next`] so building a [`ScanSource`] does no I/O.
struct Resolved {
    /// The single batch [`SnapshotScan::execute`] produced — sliced into windows.
    batch: Batch,
    /// The scan's pruning accounting, surfaced via [`ScanSource::stats`].
    stats: ScanStats,
    /// The next unread row in [`batch`](Self::batch).
    cursor: usize,
}

/// A **source operator** that emits a [`SnapshotScan`]'s resolved rows in
/// fixed-size batches.
///
/// On the first [`next`](Operator::next) it resolves the whole scan once (the
/// module-level "eager at the source" note), then hands out windows of at most
/// `batch_rows` rows per pull. Construct it with [`SnapshotScan::into_source`] or
/// [`ScanSource::new`].
pub struct ScanSource<'a, D: Disk, I: Disk, F: DiskFile> {
    scan: SnapshotScan<'a, D, I, F>,
    batch_rows: usize,
    resolved: Option<Resolved>,
}

impl<'a, D: Disk, I: Disk, F: DiskFile> ScanSource<'a, D, I, F> {
    /// Wrap `scan` in a source operator emitting batches of at most `batch_rows`
    /// rows. A `batch_rows` of `0` is clamped to `1` — a zero-row batch size
    /// would never make progress.
    #[must_use]
    pub fn new(scan: SnapshotScan<'a, D, I, F>, batch_rows: usize) -> Self {
        Self {
            scan,
            batch_rows: batch_rows.max(1),
            resolved: None,
        }
    }

    /// The scan's pruning [`ScanStats`], available once the first
    /// [`next`](Operator::next) has resolved the scan (`None` before then).
    #[must_use]
    pub fn stats(&self) -> Option<ScanStats> {
        self.resolved.as_ref().map(|r| r.stats)
    }
}

impl<D: Disk, I: Disk, F: DiskFile> Operator for ScanSource<'_, D, I, F> {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        // Resolve the whole scan on first pull, then chunk it. `Option::insert`
        // hands back the `&mut Resolved` either way without an unwrap.
        let resolved = if let Some(ref mut r) = self.resolved {
            r
        } else {
            let out = self.scan.execute()?;
            self.resolved.insert(Resolved {
                batch: out.batch,
                stats: out.stats,
                cursor: 0,
            })
        };

        if resolved.cursor >= resolved.batch.rows {
            return Ok(None);
        }
        let len = self.batch_rows.min(resolved.batch.rows - resolved.cursor);
        let columns = resolved
            .batch
            .columns
            .iter()
            .map(|(id, col)| (*id, col.slice(resolved.cursor, len)))
            .collect();
        resolved.cursor += len;
        Ok(Some(Batch { columns, rows: len }))
    }
}

/// A **projection operator**: re-selects and reorders its child's columns by
/// [`ColumnId`], batch by batch.
///
/// The columns it names must already be present in the child's batches —
/// projection here picks among materialized columns, it does not materialize new
/// ones (the [`ScanSource`]'s underlying [`SnapshotScan`] does the materializing
/// via its own projection pushdown). A named column the child did not emit is a
/// plan error, surfaced as [`ScanError::MissingColumn`].
pub struct Project<C: Operator> {
    child: C,
    columns: Vec<ColumnId>,
}

impl<C: Operator> Project<C> {
    /// Project `child`'s batches down to `columns`, in the given order.
    #[must_use]
    pub const fn new(child: C, columns: Vec<ColumnId>) -> Self {
        Self { child, columns }
    }
}

impl<C: Operator> Operator for Project<C> {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        let Some(batch) = self.child.next()? else {
            return Ok(None);
        };
        let rows = batch.rows;
        // Move the child's columns into takeable slots so a projection that
        // selects + reorders distinct columns (the common case) *moves* each one
        // out of the owned batch — no per-batch deep clone. A column projected
        // more than once (degenerate) clones from the copy already emitted this
        // batch; a column the child never emitted is a plan error.
        let mut slots: Vec<(ColumnId, Option<Column>)> = batch
            .columns
            .into_iter()
            .map(|(id, c)| (id, Some(c)))
            .collect();
        let mut columns: Vec<(ColumnId, Column)> = Vec::with_capacity(self.columns.len());
        for &want in &self.columns {
            let col = match slots.iter_mut().find(|(id, _)| *id == want) {
                Some((_, slot @ Some(_))) => slot.take().expect("matched Some"),
                Some((_, None)) => columns
                    .iter()
                    .find(|(id, _)| *id == want)
                    .map(|(_, c)| c.clone())
                    .expect("a taken column was already emitted this batch"),
                None => return Err(ScanError::MissingColumn(want)),
            };
            columns.push((want, col));
        }
        Ok(Some(Batch { columns, rows }))
    }
}

/// An operator that **explodes** the row-codec [`Payload`](ColumnId::Payload)
/// blob into the table's value columns as first-class typed columns ([STL-206]).
///
/// The scan source materializes a row as two storage columns — the
/// [`BusinessKey`](ColumnId::BusinessKey) and the opaque
/// [`Payload`](ColumnId::Payload) blob that the [row codec](stele_common::row_codec)
/// packs the value columns into. A value-column predicate (a [`Filter`] over
/// `b = 'x'`) can only run vectorized once those value cells are *separate*
/// columns; this operator is the vectorized mirror of
/// [`row_codec::decode_payload`] that produces them, so the rest of the pipeline
/// (filter, then the engine's projection) sees first-class value columns instead
/// of one opaque blob.
///
/// Each input batch's `Payload` column is sliced — per row — into `value_count`
/// cells, transposed into `value_count` [`Column::Bytes`] columns. The output
/// batch is the business key followed by those value columns, in schema order:
/// position `0` is the key, position `i + 1` is value column `i`.
///
/// ## Addressing is positional
///
/// Downstream operators ([`Filter`], the engine's positional projection) address
/// these columns **by position**, not by [`ColumnId`]. A value column has no
/// dedicated id — the storage [`ColumnId`] enum is closed (business key, payload,
/// provenance, valid-time, …) with no per-value-column variant — so every
/// exploded value column is tagged [`ColumnId::Payload`], the blob it was lifted
/// from. Position, not the id, distinguishes them; the [`Project`] operator
/// (which selects *by* id) therefore cannot disambiguate them, and the engine
/// projects the exploded batch positionally instead.
pub struct ExplodePayload<C: Operator> {
    child: C,
    /// The number of value columns packed in the payload — the table's column
    /// count minus the business key. Drives [`row_codec::decode_payload`]'s
    /// slicing; `0` (a key-only table) drops the payload entirely.
    value_count: usize,
}

impl<C: Operator> ExplodePayload<C> {
    /// Explode `child`'s batches into `value_count` value columns plus the
    /// business key. `value_count` is the table's column count minus the
    /// business key (the same count [`row_codec::decode_payload`] takes).
    #[must_use]
    pub const fn new(child: C, value_count: usize) -> Self {
        Self { child, value_count }
    }
}

impl<C: Operator> Operator for ExplodePayload<C> {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        let Some(batch) = self.child.next()? else {
            return Ok(None);
        };
        let rows = batch.rows;

        // The business key passes through unchanged at position 0.
        let key = batch
            .columns
            .iter()
            .find(|(id, _)| *id == ColumnId::BusinessKey)
            .map(|(_, col)| col.clone())
            .ok_or(ScanError::MissingColumn(ColumnId::BusinessKey))?;
        let mut columns: Vec<(ColumnId, Column)> = Vec::with_capacity(self.value_count + 1);
        columns.push((ColumnId::BusinessKey, key));

        // A key-only table stores no value cells: drop the payload, emit the key.
        if self.value_count == 0 {
            return Ok(Some(Batch { columns, rows }));
        }

        let (_, payload) = batch
            .columns
            .iter()
            .find(|(id, _)| *id == ColumnId::Payload)
            .ok_or(ScanError::MissingColumn(ColumnId::Payload))?;
        let Column::Bytes(payload_cells) = payload else {
            // The payload is always a variable-length bytes column; a fixed-width
            // `i64` here would be a plan break, reported as a missing payload.
            return Err(ScanError::MissingColumn(ColumnId::Payload));
        };

        // Decode each row's packed payload into its value cells once, then
        // transpose the per-row cells into `value_count` columns. `decode_payload`
        // always returns exactly `value_count` cells, so the zip is total.
        let mut value_cols: Vec<Vec<Option<Vec<u8>>>> =
            vec![Vec::with_capacity(rows); self.value_count];
        for cell in payload_cells {
            let decoded = row_codec::decode_payload(self.value_count, cell.as_deref())?;
            for (slot, value) in value_cols.iter_mut().zip(decoded) {
                slot.push(value);
            }
        }
        columns.extend(
            value_cols
                .into_iter()
                .map(|cells| (ColumnId::Payload, Column::Bytes(cells))),
        );
        Ok(Some(Batch { columns, rows }))
    }
}

/// A **filter operator** — keeps the `TRUE` rows of its child's batches
/// ([STL-170]).
///
/// `FALSE` *and* `NULL` rows are dropped, the SQL `WHERE` rule that an unknown
/// is not kept.
///
/// The predicate is a vectorized [`Expr`] evaluated a whole batch at a time by
/// [`eval_expr`]; it references the child's batch columns **by position**,
/// and `schema` gives the [`LogicalType`] of each of those columns so the
/// storage [`Column`]s can be decoded into the typed, nullable evaluation form.
/// `schema` is positional — one entry per column the child emits, in the same
/// order — so a predicate over the business key plus a value column reads each
/// from its slot.
///
/// Only the columns the predicate actually references are decoded; an opaque or
/// out-of-scope column the predicate ignores is passed through untouched (its
/// `schema` entry is never consulted). A referenced column whose type the
/// evaluator cannot read surfaces as [`ScanError::Eval`].
///
/// Like every [`Operator`], `Filter` never emits an empty batch: a batch all of
/// whose rows are dropped is skipped, and the next non-empty batch (or
/// end-of-stream) is returned, so a consumer's `while let Some(_)` loop needs no
/// special case.
pub struct Filter<C: Operator> {
    child: C,
    predicate: Expr,
    schema: Vec<LogicalType>,
    /// The columns `predicate` references, ascending — computed once at
    /// construction so each pull decodes only these and allocates no per-batch
    /// reference set.
    referenced: Vec<usize>,
}

impl<C: Operator> Filter<C> {
    /// Filter `child`'s batches by `predicate`, decoding referenced columns with
    /// `schema` (one [`LogicalType`] per child column, positional).
    #[must_use]
    pub fn new(child: C, predicate: Expr, schema: Vec<LogicalType>) -> Self {
        let mut referenced = BTreeSet::new();
        collect_columns(&predicate, &mut referenced);
        Self {
            child,
            predicate,
            schema,
            referenced: referenced.into_iter().collect(),
        }
    }

    /// The row indices of `batch` the predicate keeps (its `TRUE` rows).
    fn kept_rows(&self, batch: &Batch) -> Result<Vec<usize>, ScanError> {
        // Decode only the columns the predicate references (precomputed in
        // `new`) — an unreferenced column (an opaque payload, a provenance
        // scalar) is never touched, so a filter on one column does not force the
        // whole batch through the bridge. Unreferenced slots hold an empty
        // placeholder the evaluator never reads.
        let mut columns: Vec<Vector> = (0..batch.columns.len())
            .map(|_| Vector::Bool(Vec::new()))
            .collect();
        for &index in &self.referenced {
            let (_, column) =
                batch
                    .columns
                    .get(index)
                    .ok_or(ScanError::Eval(ExprError::ColumnOutOfRange {
                        index,
                        columns: batch.columns.len(),
                    }))?;
            let ty =
                *self
                    .schema
                    .get(index)
                    .ok_or(ScanError::Eval(ExprError::ColumnTypeMissing {
                        index,
                        schema_len: self.schema.len(),
                    }))?;
            columns[index] = Vector::from_column(ty, column)?;
        }

        let result = eval_expr(&self.predicate, &columns, batch.rows)?;
        let Vector::Bool(mask) = result else {
            return Err(ScanError::Eval(ExprError::NotBoolean {
                op: "WHERE",
                found: result.logical_type(),
            }));
        };
        Ok(mask
            .iter()
            .enumerate()
            .filter_map(|(row, keep)| (*keep == Some(true)).then_some(row))
            .collect())
    }
}

impl<C: Operator> Operator for Filter<C> {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        // Pull until a batch has a surviving row or the child is exhausted —
        // never surface a fully-filtered batch as an empty one.
        loop {
            let Some(batch) = self.child.next()? else {
                return Ok(None);
            };
            let kept = self.kept_rows(&batch)?;
            if kept.is_empty() {
                continue;
            }
            let columns = batch
                .columns
                .iter()
                .map(|(id, col)| (*id, col.take(&kept)))
                .collect();
            return Ok(Some(Batch {
                columns,
                rows: kept.len(),
            }));
        }
    }
}

/// Collect the positions of every [`Expr::Column`] the predicate references.
fn collect_columns(expr: &Expr, out: &mut BTreeSet<usize>) {
    match expr {
        Expr::Column(index) => {
            out.insert(*index);
        }
        Expr::Literal(_) => {}
        Expr::Not(inner) | Expr::IsNull(inner) => collect_columns(inner, out),
        Expr::Compare { left, right, .. }
        | Expr::Logic { left, right, .. }
        | Expr::Arith { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
    }
}
