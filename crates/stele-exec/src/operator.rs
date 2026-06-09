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

use stele_storage::backend::{Disk, DiskFile};
use stele_storage::segment::ColumnId;

use crate::snapshot_scan::{Batch, ScanError, ScanStats, SnapshotScan};

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
        let mut columns = Vec::with_capacity(self.columns.len());
        for &want in &self.columns {
            let col = batch
                .columns
                .iter()
                .find(|(id, _)| *id == want)
                .map(|(_, c)| c.clone())
                .ok_or(ScanError::MissingColumn(want))?;
            columns.push((want, col));
        }
        Ok(Some(Batch {
            columns,
            rows: batch.rows,
        }))
    }
}
