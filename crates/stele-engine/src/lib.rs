//! The server-session **engine**: the database state a connection holds across
//! statements.
//!
//! Until now nothing owned live state between statements. `stele-server::run`
//! built the storage backend and immediately dropped it; the pgwire front end
//! ([STL-104]) was stateless and could only answer a constant `SELECT`; every
//! other consumer (`stele-sim`'s identity demo) hand-wired a `Wal`/`Delta`/
//! `ValidityIndex`/`DmlWriter`/`Catalog` inline. [`SessionEngine`] is the missing
//! piece both DDL routing ([STL-131]) and DML/SELECT routing ([STL-147]) sit on:
//! one handle that owns
//!
//! * the [`Catalog`] — created/dropped tables and their schemas at a snapshot;
//! * a monotonic **commit clock** ([`MonotonicClock`]) supplying the
//!   [`SystemTimeMicros`] that DDL `apply(at:)` and every DML commit stamp with;
//! * each table's storage tiers — a [`stele_storage::engine::Engine`] bundling its
//!   WAL, delta tier, validity index, and sealed segments, on a per-table
//!   [`NamespacedDisk`] view of the one configured backend.
//!
//! and exposes a single [`execute`](SessionEngine::execute) entry point the pgwire
//! loop calls per parsed [`Statement`]: DDL, `SELECT`, and `INSERT`/`UPDATE`/
//! `DELETE` all route by binding the statement ([STL-147] wired DML in through
//! [`bind_dml`]). The typed
//! [`insert`](SessionEngine::insert) / [`update`](SessionEngine::update) /
//! [`delete`](SessionEngine::delete) methods remain the lower-level write path the
//! DML router and in-process tests call.
//!
//! The session is **durable across restarts** ([STL-210], [ADR-0028]): every
//! DDL mutation is recorded in an fsynced, append-only catalog log beside the
//! tables' own WALs, and [`SessionEngine::recover`] boots from existing on-disk
//! state — replaying the catalog log, reopening each table's tiers through
//! [`Engine::recover`](stele_storage::engine::Engine::recover), and
//! repositioning the commit clock — so `CREATE`/`INSERT`/restart/`SELECT`
//! (including `AS OF`) answers exactly as the live session did.
//!
//! ## Runtime-agnostic
//!
//! This crate is part of the deterministic core ([ADR-0010]): it depends only on
//! storage/catalog/sql/exec, never on `tokio` or wall-clock reads. The async
//! daemon ([`stele-server`]) constructs and drives a `SessionEngine`, but the
//! engine itself runs identically under the sim scheduler — which is what lets the
//! whole connection lifecycle be replayed bit-for-bit from a seed.
//!
//! [STL-104]: https://allegromusic.atlassian.net/browse/STL-104
//! [STL-131]: https://allegromusic.atlassian.net/browse/STL-131
//! [STL-147]: https://allegromusic.atlassian.net/browse/STL-147
//! [STL-149]: https://allegromusic.atlassian.net/browse/STL-149
//! [STL-210]: https://allegromusic.atlassian.net/browse/STL-210
//! [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md

pub mod backup;
mod catalog_log;
mod commit_log;
mod secondary;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io;
use std::ops::Bound;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crate::catalog_log::CatalogRecord;
use crate::secondary::{IndexState, Probe};

use stele_catalog::{
    Catalog, CatalogError, IndexDef, IndexKind, SchemaId, TableSchema, ValidTimeSpec,
};
use stele_common::hash::Digest;
use stele_common::metrics::{SharedMetrics, StatementKind};
use stele_common::period::{Interval, PeriodPredicate};
use stele_common::provenance::{self, Principal, TxnId};
use stele_common::query_stats::QueryStats;
use stele_common::row_codec::{self, RowCodecError};
use stele_common::scram::{self, ScramVerifier};
use stele_common::time::{
    Clock, SYSTEM_TIME_OPEN, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros,
};
use stele_common::types::{LogicalType, ScalarValue};
use stele_exec::{
    AggregateFunc as ExecAggregateFunc, Aggregator, ArithOp as ExecArithOp, Batch, CmpOp, Column,
    DEFAULT_BATCH_SIZE, ExplodePayload, Expr, ExprError, Filter, GatheredColumns, JoinIndices,
    JoinType as ExecJoinType, LogicOp, Operator, ScanError, ScanSource, ScanStats, SnapshotScan,
    SortKey, SystemRange, ValidRange, Vector, distinct_selection, eval_expr, evaluate,
    hash_aggregate, hash_join, limit_selection, sort_selection,
};
use stele_sql::Password;
use stele_sql::ddl::{DdlOutcome, DdlStatement};
use stele_sql::dml::{BoundDml, DmlError, InsertRow};
use stele_sql::merge::{BoundMerge, MergeBound, MergeSource, MergeValid, MergeValue};
use stele_sql::select::{
    AggregateFunc, ArithOp, BoundAggregate, BoundCte, BoundHaving, BoundJoinSide, BoundJoinStep,
    BoundPeriod, BoundPeriodPredicate, BoundPredicate, BoundScalar, BoundSelect,
    BoundSubqueryFilter, CompareOp, CompositeKeyDecorrelation, Correlation, HavingScalar, JoinType,
    OutputItem, PeriodEndpoint, Projection, ProjectionItem, ProjectionValue, SelectError,
    SemiAntiDecorrelation, SortTarget, SubqueryKind, SystemTimeRange, ValidTimeRange,
};
use stele_sql::{
    AdminCommand, BindContext, BindError, BoundCopy, CopyError, CopyShape, Statement,
    StatementBody, TimeDimension, bind_copy, bind_copy_rows, bind_ddl, bind_dml, bind_select,
    resolve_as_of, without_filter,
};
use stele_storage::backend::Disk;
use stele_storage::delta::{BusinessKey, Snapshot, Version};
use stele_storage::dml::{CommittedTxns, DmlOutcome};
use stele_storage::engine::{Engine, EngineError as StorageError};
use stele_storage::segment::{ColumnId, Predicate, ZoneBound};
use stele_storage::validtime::ValidInterval;
use stele_storage::wal::WalError;
use stele_txn::{ChainError, CommitRecord, verify_chain_recover, verify_chain_to};

/// A monotonic, globally-shared commit clock.
///
/// Wraps an inner [`Clock`] (the OS clock in production, a virtual clock under the
/// sim) with a high-water mark so every reading is **strictly greater** than the
/// last — even if the inner clock stalls or steps backwards. One mark is shared
/// across every clone (it is held behind an [`Arc`]), so the commit timestamps a
/// session stamps onto *different tables'* writes — and the system time DDL takes
/// effect at — are totally ordered with each other, which is what the bitemporal
/// `sys_from` ordering relies on (coordinates with the MVCC commit-time cursor,
/// [STL-99]).
///
/// [STL-99]: https://allegromusic.atlassian.net/browse/STL-99
#[derive(Debug, Clone)]
pub struct MonotonicClock<C> {
    inner: C,
    high_water: Arc<AtomicI64>,
}

impl<C> MonotonicClock<C> {
    /// Wrap `inner`, starting the high-water mark at the origin — the first
    /// [`now`](Clock::now) jumps straight to the inner clock's reading.
    #[must_use]
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            high_water: Arc::new(AtomicI64::new(0)),
        }
    }

    /// The latest timestamp handed out, **without** consulting the inner clock.
    /// A reader at this instant sees every commit so far (each had
    /// `sys_from <= high_water`) and nothing not yet committed.
    ///
    /// This is the right instant for resolving *committed state* — catalog
    /// lookups, conflict bookkeeping, the [`commit_clock`](SessionEngine::commit_clock)
    /// the oracle aligns on. It is **not** the right base for a fresh read
    /// snapshot: between writes the mark stands still, so `now()` arithmetic in
    /// an `AS OF` would be frozen at the last commit. Fresh snapshots go through
    /// [`observe`](Self::observe) instead ([STL-227]).
    ///
    /// [STL-227]: https://allegromusic.atlassian.net/browse/STL-227
    #[must_use]
    pub fn current(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.high_water.load(Ordering::Acquire))
    }

    /// Raise the high-water mark to at least `mark` (never lowers it).
    ///
    /// Recovery calls this with the largest commit instant found on disk
    /// ([`SessionEngine::recover`]): afterwards [`current`](Self::current) — the
    /// default read snapshot — covers every recovered commit (a fresh mark would
    /// otherwise sit at the origin and a post-restart `SELECT` would see
    /// nothing), and the next [`now`](Clock::now) is strictly past everything
    /// already written, even if the inner clock has stepped backwards across
    /// the restart ([ADR-0022]).
    ///
    /// [ADR-0022]: ../../../docs/adr/0022-clock-synchronization-and-ordering.md
    pub fn advance_to(&self, mark: SystemTimeMicros) {
        self.high_water.fetch_max(mark.0, Ordering::AcqRel);
    }
}

impl<C: Clock> MonotonicClock<C> {
    /// Take a fresh read snapshot: the inner clock's reading folded into the
    /// high-water mark ([STL-227]).
    ///
    /// On an idle database [`current`](Self::current) is pinned at the last
    /// commit, which froze `AS OF now()` arithmetic there — `now() - interval
    /// '1 second'` resolved to one second before the last *write*, however long
    /// ago that was. Observing the inner clock makes a fresh snapshot track real
    /// time (statement time on auto-commit, transaction-start time inside a
    /// `BEGIN` block — Postgres `now()` semantics).
    ///
    /// Raising the mark while reading is load-bearing, not incidental: a later
    /// commit takes `max(inner, high_water + 1)` ([`now`](Clock::now)), so once
    /// the snapshot is folded in every subsequent commit is **strictly greater**
    /// than it — a pinned snapshot can never retroactively cover a commit, even
    /// if the inner clock stalls or steps backwards (snapshot isolation,
    /// [ADR-0022]). Under the sim the inner clock is virtual and only moves when
    /// the scenario says so, so this degenerates to [`current`](Self::current)
    /// and seeded traces are unchanged.
    ///
    /// [ADR-0022]: ../../../docs/adr/0022-clock-synchronization-and-ordering.md
    /// [STL-227]: https://allegromusic.atlassian.net/browse/STL-227
    #[must_use]
    pub fn observe(&self) -> SystemTimeMicros {
        let reading = self.inner.now().0;
        let prev = self.high_water.fetch_max(reading, Ordering::AcqRel);
        SystemTimeMicros(reading.max(prev))
    }
}

impl<C: Clock> Clock for MonotonicClock<C> {
    fn now(&self) -> SystemTimeMicros {
        let candidate = self.inner.now().0;
        let mut prev = self.high_water.load(Ordering::Acquire);
        loop {
            // Strictly past both the inner reading and the last value handed out.
            let next = candidate.max(prev.saturating_add(1));
            match self.high_water.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return SystemTimeMicros(next),
                Err(observed) => prev = observed,
            }
        }
    }
}

/// A per-table, prefixed view of one shared [`Disk`].
///
/// A [`Disk`] is a *flat* namespace and the storage tiers use fixed file names
/// (`wal-*.log`, `delta-spill-*.row`, `seg-*.seg`, `stele.checkpoint`, …), so two
/// tables on the same backend would collide. This adapter gives each table its own
/// slice of the namespace by prefixing every file name with a unique, fixed-width
/// `t{idx}-` tag and stripping it back off on [`list`](Disk::list) — the tiers
/// underneath see exactly their own files and nothing else. The prefix is a valid
/// single path component, so the backend's name validation still passes.
#[derive(Debug, Clone)]
pub struct NamespacedDisk<D> {
    inner: D,
    prefix: String,
}

impl<D> NamespacedDisk<D> {
    /// A view of `inner` scoped to table namespace `idx`.
    #[must_use]
    pub fn new(inner: D, idx: u64) -> Self {
        Self {
            inner,
            prefix: format!("t{idx:020}-"),
        }
    }

    fn scoped(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

impl<D: Disk + Clone> Disk for NamespacedDisk<D> {
    type File = D::File;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        self.inner.create(&self.scoped(name))
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        self.inner.open(&self.scoped(name))
    }

    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self
            .inner
            .list()?
            .into_iter()
            .filter_map(|name| name.strip_prefix(&self.prefix).map(ToOwned::to_owned))
            .collect())
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        self.inner.remove(&self.scoped(name))
    }

    fn sync_dir(&self) -> io::Result<()> {
        // Every namespace view shares the one physical directory — fencing the
        // view fences it.
        self.inner.sync_dir()
    }
}

/// What [`SessionEngine::execute`] did with one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementOutcome {
    /// A DDL statement ran; carries the Postgres `CommandComplete` tag
    /// (`CREATE TABLE` / `DROP TABLE`).
    Ddl {
        /// The `CommandComplete` tag the wire client expects.
        tag: &'static str,
    },
    /// A `SELECT` ran; carries the projected, snapshot-resolved result.
    Rows(SelectResult),
    /// An `INSERT` / `UPDATE` / `DELETE` committed; carries the affected-row
    /// count for the `CommandComplete` tag.
    Dml(DmlSummary),
}

/// A `SELECT`'s result: the projected columns and one raw-bytes cell per column
/// per row.
///
/// v0.1 projects the `(business key, payload)` pair (the identity-demo shape):
/// the first column is the table's business key, the second its opaque payload.
/// Each cell carries the value's canonical encoding
/// ([`ScalarValue::encode`](stele_common::types::ScalarValue::encode)); the wire
/// layer decodes it back to the column's [`LogicalType`] to render it
/// ([STL-147]). The bytes are kept undecoded here so the engine stays agnostic of
/// how a cell was written — a value staged through the typed
/// [`insert`](SessionEngine::insert) path may carry an opaque payload that is not
/// a `ScalarValue` encoding at all.
///
/// [STL-147]: https://allegromusic.atlassian.net/browse/STL-147
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectResult {
    /// The projected columns, in output order: each a `(name, type)` pair.
    pub columns: Vec<(String, LogicalType)>,
    /// One entry per result row; each row holds one cell per column, aligned to
    /// [`columns`](Self::columns). A cell is `Some(bytes)` for a present value
    /// (the value's canonical encoding) or `None` for a SQL `NULL` ([STL-154]),
    /// which the wire layer renders as the length-`-1` `DataRow` sentinel.
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// Per-query execution accounting for the "see the engine" footer ([STL-201]),
    /// folded from the scan's `ScanStats`. `Some` for any read backed by a storage
    /// scan: the committed-only fast path (the common `SELECT`), a
    /// read-your-own-writes overlay or a provenance read (their unfiltered base
    /// scan), and a join (both sides' scans summed, [STL-318]). `None` only when
    /// there is no single scan to report — a synthetic catalog/history result, or a
    /// join whose side is a materialized CTE / derived table — so the wire layer
    /// suppresses the footer rather than reporting a scan that did not happen.
    pub stats: Option<QueryStats>,
}

/// A committed-only snapshot scan's reconstructed rows plus its pruning
/// accounting — paired so the "see the engine" footer ([STL-201]) can report the
/// scan that produced them. Returned by the fused fast path
/// ([`scan_rows`](SessionEngine::scan_rows)) and by the unfiltered full scans an
/// overlay / provenance read takes as its base
/// ([`scan_all_rows`](SessionEngine::scan_all_rows), [STL-318]).
struct ScannedRows {
    rows: Vec<Vec<Option<Vec<u8>>>>,
    stats: ScanStats,
}

/// A common-table-expression / derived table's **materialized** result ([STL-242]),
/// computed once at the statement snapshot.
///
/// A `WITH name AS (…)` entry, or a `FROM (…) AS d` derived table, runs once and
/// is then referenced like a table. It is held **columnar** — one shared
/// [`Cells`](stele_exec::Cells) buffer per column, the same shape
/// [`scan_all_columns`](SessionEngine::scan_all_columns) returns for a base table —
/// so a reference never re-copies the stored cells ([STL-321]): a join side clones
/// the columns by `Arc` refcount bump ([`join_side_columns`](SessionEngine::join_side_columns)),
/// and a read decodes only the `WHERE`-referenced columns straight off the shared
/// buffers into a selection vector ([`relation_selection`]), then projects through the
/// shared tail as a [`RowSource::Relation`] without a full-width row-major gather
/// ([STL-338]). Each cell
/// is the canonical-bytes form a base-table scan reconstructs (`Some(bytes)`, or
/// `None` for a SQL `NULL`), so the outer query's `WHERE` / aggregate / projection /
/// join feed from it through the very same downstream pipeline as a base table. The
/// relation's column header is carried on the bound plan
/// ([`BoundSelect::relation_columns`] / [`BoundJoinSide::columns`]), so only the
/// cells are kept here.
#[derive(Debug, Clone)]
struct MaterializedRelation {
    /// One column per relation column, in bound-plan order. An empty relation still
    /// holds its full complement of zero-length columns, so a join side's row count
    /// (`columns[0].len()`) stays well-defined.
    columns: Vec<Column>,
    /// The relation's row count — the length of every column. Kept explicitly so a
    /// zero-column edge case (no `columns[0]` to measure) and the `KeepAll` selection
    /// stay unambiguous.
    row_count: usize,
}

impl MaterializedRelation {
    /// Transpose a finished query's row-major `result.rows` into the columnar shape
    /// kept here — done **once** per materialization (not per reference). `ncols` is
    /// the relation's column count (`result.columns.len()`); an empty result still
    /// yields `ncols` zero-length columns so a join side scans it as a well-formed
    /// zero-height input.
    fn from_rows(rows: Vec<Vec<Option<Vec<u8>>>>, ncols: usize) -> Self {
        let row_count = rows.len();
        let mut columns: Vec<Vec<Option<Vec<u8>>>> =
            (0..ncols).map(|_| Vec::with_capacity(row_count)).collect();
        for mut row in rows {
            // `finish_select` produces rectangular rows (each `ncols` wide); pad /
            // truncate defensively so every column ends exactly `row_count` long and
            // the per-column push never indexes out of range.
            row.resize(ncols, None);
            for (i, cell) in row.into_iter().enumerate() {
                columns[i].push(cell);
            }
        }
        Self {
            columns: columns
                .into_iter()
                .map(|c| Column::Bytes(c.into()))
                .collect(),
            row_count,
        }
    }
}

/// The common-table-expressions / derived tables in scope while a `SELECT` runs
/// ([STL-242]) — name → its materialized result, shared by [`Arc`] so a nested
/// query inherits the enclosing scope without copying the rows. A [`BTreeMap`]
/// (not a hash map) keeps the deterministic-core ordering invariant.
type CteScope = BTreeMap<String, Arc<MaterializedRelation>>;

/// Fold a scan's [`ScanStats`] into the wire [`QueryStats`] the "see the engine"
/// footer renders ([STL-201]).
///
/// `rows` is the count the query finally returned (post-filter / post-aggregate);
/// `snapshot` is the resolved system-time the read ran at. The `time_travel` flag
/// is left `false` here and stamped by the caller that knows whether a `FOR
/// SYSTEM_TIME AS OF` qualifier was given (only [`execute_at`](SessionEngine::execute_at)
/// sees the raw statement's temporal clause).
const fn query_stats(scan: &ScanStats, rows: usize, snapshot: SystemTimeMicros) -> QueryStats {
    QueryStats {
        rows: rows as u64,
        system_snapshot: snapshot.0,
        time_travel: false,
        segments_total: scan.segments_total as u64,
        segments_scanned: scan.segments_scanned as u64,
        segments_pruned_zone: scan.segments_pruned_zone as u64,
        segments_pruned_bloom: scan.segments_pruned_bloom as u64,
        segments_pruned_superseded: scan.segments_pruned_superseded as u64,
        segments_pruned_valid: scan.segments_pruned_valid as u64,
        row_groups_total: scan.row_groups_total as u64,
        row_groups_scanned: scan.row_groups_scanned as u64,
        row_groups_pruned_zone: scan.row_groups_pruned_zone as u64,
        row_groups_pruned_valid: scan.row_groups_pruned_valid as u64,
    }
}

/// The affected-row count of a committed `INSERT` / `UPDATE` / `DELETE` /
/// `MERGE`.
///
/// A point write reports `1`; a multi-row `INSERT` ([STL-228]), a predicate-driven
/// `UPDATE` / `DELETE` ([STL-229]), and a `MERGE` ([STL-230]) report the rows they
/// acted on. The variant is carried so the wire layer renders the right
/// `CommandComplete` tag (`INSERT 0 n` / `UPDATE n` / `DELETE n` / `MERGE n`).
///
/// [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
/// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlSummary {
    /// `INSERT` affected `n` rows.
    Insert(u64),
    /// `UPDATE` affected `n` rows.
    Update(u64),
    /// `DELETE` affected `n` rows.
    Delete(u64),
    /// `MERGE` acted on `n` source rows (each an update or an insert) ([STL-230]).
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    Merge(u64),
}

/// The isolation level an open transaction reads under ([STL-248], [ADR-0008]).
///
/// Stele's default and strongest single-node level is **snapshot isolation**,
/// which the SQL surface spells `REPEATABLE READ` (`SERIALIZABLE` — true SSI — is a
/// later opt-in, [01 §B.4]). A transaction can select the weaker `READ COMMITTED`
/// instead, trading the stable single snapshot for a fresher one per statement.
///
/// The level only changes **which snapshot a statement reads at**; the write path
/// is identical, and a write-write conflict is still first-committer-wins, detected
/// at [`commit`](SessionEngine::commit). Because `READ COMMITTED` re-pins toward the
/// present before each statement, its commit conflict check runs against that
/// fresher snapshot, so it raises the retryable [`EngineError::Conflict`] in
/// correspondingly fewer cases than the stable-snapshot default — the expected
/// weaker guarantee of the lower level, not a bug.
///
/// [STL-248]: https://allegromusic.atlassian.net/browse/STL-248
/// [01 §B.4]: ../../../docs/01-feature-plan.md#b4--transactions-concurrency--mvcc
/// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// **Snapshot isolation** — one snapshot pinned at `BEGIN` for the whole
    /// transaction (the SQL `REPEATABLE READ`/`SNAPSHOT` spelling). The default and
    /// strongest level Stele offers single-node ([ADR-0008]).
    #[default]
    RepeatableRead,
    /// **Read committed** — each statement re-pins a fresh snapshot, so a statement
    /// observes every transaction committed before it began. Weaker than the
    /// default: a transaction's successive reads can advance.
    ReadCommitted,
}

/// A multi-statement transaction's buffered, not-yet-applied writes ([STL-174]).
///
/// Created by [`SessionEngine::begin`], fed bound DML one statement at a time by
/// [`SessionEngine::stage_dml`], and applied as a unit by
/// [`SessionEngine::commit`] — or simply **dropped** to roll back. The defining
/// property is that staged writes are *buffered*, never reaching storage until
/// commit: no *other* connection sees anything a transaction writes before
/// `COMMIT`, and `ROLLBACK` discards the buffer with no effect ever reaching
/// storage. The transaction does see its **own** buffered writes when it reads —
/// read-your-own-writes ([STL-203]), overlaid on its pinned snapshot.
///
/// The transaction reads under **snapshot isolation** ([STL-175], [ADR-0008]): a
/// single system-time snapshot is pinned at [`begin`](SessionEngine::begin) and
/// every statement in the block resolves its reads at it, so the transaction sees
/// one consistent snapshot for its whole life even while other connections commit.
/// Write-write conflicts are detected at [`commit`](SessionEngine::commit), first
/// committer wins. (The lone exception: a `CREATE` / `DROP` inside the block
/// auto-commits and *advances* the snapshot, since transactional DDL is not yet
/// modeled — see [`execute_in_txn`](SessionEngine::execute_in_txn).)
///
/// Savepoints ([STL-176]) partition the buffer: [`savepoint`](Self::savepoint)
/// records a marker at the current write position, [`rollback_to`](Self::rollback_to)
/// truncates the buffer back to a marker (undoing only the writes staged after it,
/// the transaction continuing), and [`release`](Self::release) drops a marker while
/// keeping its writes.
///
/// [`commit`](SessionEngine::commit) is **crash-atomic** ([STL-192]): a
/// transaction's writes to each table are group-committed as one WAL record with one
/// fsync, so a crash mid-commit recovers all of that table's writes or none — never
/// a partial prefix — and the writes share one transaction id. A transaction
/// spanning several tables is made atomic *across* them by a commit marker fsynced
/// only after every per-table leg is durable ([STL-215]). And if applying a buffered
/// write fails partway, the writes already applied to the in-memory tiers are rolled
/// back in place ([STL-216]) so the live engine shows none of the failed
/// transaction — matching what a crash recovery (which finds no durable record)
/// reconstructs, without a restart.
///
/// Read-your-own-writes covers **valid-time** tables too ([STL-223]): a write
/// supersedes one live version per business key (the storage path closes the prior
/// system period and opens a new one carrying the new valid interval), so the same
/// business-key overlay the system-time row set uses applies, and a `FOR VALID_TIME
/// AS OF v` read re-filters the overlaid rows on their `[valid_from, valid_to)`
/// bounds. A `FOR SYSTEM_TIME AS OF` read still reads committed history only — the
/// uncommitted buffer is not part of any past system state.
///
/// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
/// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
/// [STL-176]: https://allegromusic.atlassian.net/browse/STL-176
/// [STL-203]: https://allegromusic.atlassian.net/browse/STL-203
/// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
/// [STL-216]: https://allegromusic.atlassian.net/browse/STL-216
/// [STL-223]: https://allegromusic.atlassian.net/browse/STL-223
/// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
#[derive(Debug)]
pub struct SessionTransaction {
    /// The system-time snapshot pinned at [`begin`](SessionEngine::begin). Every
    /// read in the transaction resolves here, and a write-write conflict is one
    /// whose key was committed by another transaction *after* this instant. Under
    /// [`IsolationLevel::ReadCommitted`] it is re-pinned toward the present before
    /// each statement, so the conflict anchor advances with it.
    snapshot: SystemTimeMicros,
    /// The isolation level this transaction reads under ([STL-248]). The default,
    /// [`IsolationLevel::RepeatableRead`], keeps the single `BEGIN`-pinned snapshot;
    /// [`IsolationLevel::ReadCommitted`] re-pins it per statement in
    /// [`execute_in_txn`](SessionEngine::execute_in_txn).
    isolation: IsolationLevel,
    /// The bound writes staged so far, in statement order. Applied front-to-back
    /// at commit so a later `UPDATE` of a key staged after its `INSERT` lands in
    /// the order the client issued them.
    writes: Vec<BoundDml>,
    /// The open savepoints, innermost last ([STL-176]). Each marks the length of
    /// `writes` at the instant the savepoint was established, so `ROLLBACK TO`
    /// truncates `writes` back to that marker — undoing exactly the writes staged
    /// after the savepoint, and nothing before it.
    savepoints: Vec<Savepoint>,
    /// Keeps this transaction's [`snapshot`](Self::snapshot) registered in the
    /// engine's [`open_snapshots`](SessionEngine::open_snapshots) multiset for as
    /// long as the transaction is open. Dropping it — on
    /// [`commit`](SessionEngine::commit), an explicit `ROLLBACK` (the front end
    /// drops the [`SessionTransaction`]), or a dropped connection — releases the
    /// registration, so the engine's prune floor rises with no explicit
    /// end-of-transaction call to miss ([STL-204]).
    lease: SnapshotLease,
}

/// The system-time snapshots pinned by currently-open transactions, as a multiset
/// `instant -> how many open transactions pinned it`. Shared (behind an [`Arc`],
/// like the commit clock's high-water mark) between the [`SessionEngine`] and every
/// live [`SnapshotLease`], so a transaction ending on *any* path decrements its
/// instant without the engine having to observe the end explicitly. The smallest
/// key is the oldest live snapshot, the floor below which the MVCC write index can
/// be pruned ([STL-204], [ADR-0008]).
///
/// The inner [`Mutex`] guards only this small map and is taken for the duration of
/// a single increment / decrement / minimum read — never across other work — so it
/// neither blocks under the single-threaded sim scheduler nor affects any
/// observable result: it bounds *when* unreachable index entries are dropped, not
/// *which* (an entry is pruned only once it can never satisfy a conflict). The
/// engine therefore stays deterministic in every observable behavior ([ADR-0010]).
type OpenSnapshots = Arc<Mutex<BTreeMap<SystemTimeMicros, usize>>>;

/// An RAII registration of one open transaction's pinned snapshot in the engine's
/// [`OpenSnapshots`] multiset ([STL-204]).
///
/// Held inside the [`SessionTransaction`], so it lives exactly as long as the
/// transaction: [`begin`](SessionEngine::begin) acquires it, and dropping the
/// transaction — by `commit`, by `ROLLBACK` (the front end simply drops it), or by
/// a dropped connection — releases it. That makes the bookkeeping leak-free across
/// every end-of-transaction path, including the ones the engine never sees as a
/// method call.
#[derive(Debug)]
struct SnapshotLease {
    open: OpenSnapshots,
    snapshot: SystemTimeMicros,
}

impl SnapshotLease {
    /// Register `snapshot` as pinned by one more open transaction.
    fn new(open: OpenSnapshots, snapshot: SystemTimeMicros) -> Self {
        *Self::lock(&open).entry(snapshot).or_insert(0) += 1;
        Self { open, snapshot }
    }

    /// Move the registration to a new pinned instant — a DDL inside the block
    /// advanced the snapshot ([`repin_snapshot`](SessionEngine::repin_snapshot),
    /// [STL-175]) — releasing the old instant and acquiring the new one.
    fn repin(&mut self, snapshot: SystemTimeMicros) {
        if snapshot == self.snapshot {
            return;
        }
        let mut open = Self::lock(&self.open);
        Self::release(&mut open, self.snapshot);
        *open.entry(snapshot).or_insert(0) += 1;
        drop(open);
        self.snapshot = snapshot;
    }

    /// Decrement `snapshot`'s refcount, removing the key when it reaches zero so
    /// the smallest key always names a *currently* live snapshot.
    fn release(open: &mut BTreeMap<SystemTimeMicros, usize>, snapshot: SystemTimeMicros) {
        if let std::collections::btree_map::Entry::Occupied(mut e) = open.entry(snapshot) {
            if *e.get() <= 1 {
                e.remove();
            } else {
                *e.get_mut() -= 1;
            }
        }
    }

    /// Lock the multiset, recovering the guard through a poisoned lock — the only
    /// thing held under it is integer bookkeeping that cannot leave the map
    /// inconsistent, and a drop must never panic a second time.
    fn lock(open: &OpenSnapshots) -> std::sync::MutexGuard<'_, BTreeMap<SystemTimeMicros, usize>> {
        open.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        Self::release(&mut Self::lock(&self.open), self.snapshot);
    }
}

/// One open savepoint: a name plus the [`SessionTransaction::writes`] length when
/// it was established ([STL-176]).
///
/// [STL-176]: https://allegromusic.atlassian.net/browse/STL-176
#[derive(Debug)]
struct Savepoint {
    /// The savepoint's name, matched verbatim (Stele does not case-fold
    /// identifiers, as elsewhere in the binder).
    name: String,
    /// `writes.len()` at the moment this savepoint was established — the point
    /// `ROLLBACK TO` truncates back to.
    mark: usize,
}

impl SessionTransaction {
    /// The isolation level this transaction reads under ([STL-248]).
    #[must_use]
    pub const fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// Change this transaction's isolation level mid-block — the engine path for
    /// `SET TRANSACTION ISOLATION LEVEL …` ([STL-248]). It takes effect from the
    /// next statement: under [`IsolationLevel::ReadCommitted`],
    /// [`execute_in_txn`](SessionEngine::execute_in_txn) re-pins the snapshot before
    /// each statement; the currently-pinned snapshot is left as-is until then.
    pub const fn set_isolation(&mut self, isolation: IsolationLevel) {
        self.isolation = isolation;
    }

    /// Establish a savepoint at the current write position (`SAVEPOINT name`,
    /// [STL-176]).
    ///
    /// Duplicate names are allowed, matching Postgres: both are kept on the stack
    /// and [`rollback_to`](Self::rollback_to) / [`release`](Self::release) target
    /// the most recent one. Releasing or rolling back to it then re-exposes the
    /// shadowed older savepoint of the same name.
    pub fn savepoint(&mut self, name: &str) {
        self.savepoints.push(Savepoint {
            name: name.to_owned(),
            mark: self.writes.len(),
        });
    }

    /// `ROLLBACK TO SAVEPOINT name` — discard the writes staged after the most
    /// recent savepoint named `name`, and destroy every savepoint established
    /// after it; the named savepoint itself survives and can be rolled back to
    /// again ([STL-176]).
    ///
    /// Returns `false` if no savepoint named `name` is open (the caller surfaces
    /// the Postgres "savepoint does not exist" error); `true` once the truncation
    /// is applied. Writes staged *before* the savepoint are untouched.
    #[must_use]
    pub fn rollback_to(&mut self, name: &str) -> bool {
        let Some(idx) = self.savepoints.iter().rposition(|s| s.name == name) else {
            return false;
        };
        self.writes.truncate(self.savepoints[idx].mark);
        // Keep the named savepoint (index `idx`); drop the ones nested inside it.
        self.savepoints.truncate(idx + 1);
        true
    }

    /// `RELEASE SAVEPOINT name` — destroy the most recent savepoint named `name`
    /// and every savepoint established after it, **keeping** their writes (they
    /// merge into the enclosing scope) ([STL-176]).
    ///
    /// Returns `false` if no savepoint named `name` is open, `true` otherwise.
    #[must_use]
    pub fn release(&mut self, name: &str) -> bool {
        let Some(idx) = self.savepoints.iter().rposition(|s| s.name == name) else {
            return false;
        };
        // Drop the named savepoint (index `idx`) and the ones nested inside it;
        // the writes they staged stay buffered.
        self.savepoints.truncate(idx);
        true
    }
}

/// A live table's shape at the current read snapshot, for catalog introspection.
///
/// The pgwire front end's `pg_catalog` shim (the `psql \d` path, [STL-131]) reads
/// these to answer "what columns does this table have?" without reaching into the
/// catalog's internals. Each entry is a live table — one the catalog resolves at
/// the engine's current instant — and its columns in declaration order.
///
/// [STL-131]: https://allegromusic.atlassian.net/browse/STL-131
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDescription {
    /// The table's name.
    pub name: String,
    /// The table's columns, in declaration order: each a `(name, type)` pair.
    pub columns: Vec<(String, LogicalType)>,
}

/// Errors surfaced from the session engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Binding a DDL statement failed.
    #[error(transparent)]
    Bind(#[from] BindError),

    /// Binding a `SELECT` failed.
    #[error(transparent)]
    Select(#[from] SelectError),

    /// Binding an `INSERT` / `UPDATE` / `DELETE` failed — an unsupported shape,
    /// an unknown table/column, or a bad literal ([STL-149]).
    ///
    /// [STL-149]: https://allegromusic.atlassian.net/browse/STL-149
    #[error(transparent)]
    Dml(#[from] DmlError),

    /// Binding or loading a `COPY ... FROM STDIN` bulk load failed ([STL-236]) —
    /// an unsupported shape (`COPY TO`, a file/program endpoint, binary, a
    /// valid-time target), a bad option, or a row whose fields do not bind. The
    /// wire layer maps it to the matching SQLSTATE (feature-not-supported,
    /// syntax-error, or invalid-text-representation).
    ///
    /// [STL-236]: https://allegromusic.atlassian.net/browse/STL-236
    #[error(transparent)]
    Copy(#[from] CopyError),

    /// Applying DDL to the catalog failed (name already live, non-monotonic
    /// time, …).
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// A storage tier — WAL, delta, validity index, or a sealed segment —
    /// errored on open, write, or recovery.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// Taking an online backup ([STL-249]) failed — the target directory was not
    /// empty, or an I/O error reading the live disk or writing the target. The
    /// fence (flush + checkpoint) had already succeeded, so a re-run into a fresh
    /// target retries cleanly.
    ///
    /// [STL-249]: https://allegromusic.atlassian.net/browse/STL-249
    #[error("backup: {0}")]
    Backup(#[from] backup::BackupError),

    /// The durable catalog log ([ADR-0028]) could not be appended (the DDL is
    /// refused — nothing was acknowledged) or replayed at recovery (the log
    /// could not be read, or an acknowledged record is corrupt — recovery
    /// fails closed rather than serving a different table set).
    ///
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    #[error("catalog log: {0}")]
    CatalogLog(#[source] io::Error),

    /// The durable commit-marker log ([STL-215]) could not be appended (the
    /// multi-table `COMMIT` is refused — its per-table legs were made durable but
    /// the marker that vouches them was not, so recovery discards them and the
    /// transaction is all-or-none = none) or replayed at recovery (the log could
    /// not be read, or an acknowledged marker is corrupt — recovery fails closed).
    ///
    /// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
    #[error("commit log: {0}")]
    CommitLog(#[source] io::Error),

    /// The durable commit log's hash chain failed to verify on recovery — a
    /// historical commit record was tampered with (a broken link), forged
    /// (a head mismatch against nothing to anchor here), or malformed. Recovery
    /// fails closed rather than serving forged history; the tamper-evidence
    /// invariant 10 promises ([ADR-0026], [ADR-0031], STL-302).
    ///
    /// [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
    /// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
    #[error("commit log chain verification failed: {0}")]
    CommitChain(#[source] ChainError),

    /// Executing the snapshot scan failed.
    #[error(transparent)]
    Scan(#[from] ScanError),

    /// A stored payload could not be sliced back into the row's value columns —
    /// the bytes do not match the schema's column count (corruption, or a width
    /// disagreement). See the [row codec](stele_common::row_codec).
    #[error(transparent)]
    RowCodec(#[from] RowCodecError),

    /// A statement named a table that is not **live** in this session — it was
    /// never created, or has been dropped. (A dropped table's tier is retained for
    /// history, but the catalog no longer resolves the name at the current
    /// snapshot, so writes and reads against it are refused.)
    #[error("table {0:?} is not a live table in this session")]
    UnknownTable(String),

    /// A `CREATE TABLE` re-created a still-resident dropped table under a
    /// different valid-time policy than its retained tier was opened with. The
    /// tier's writer bakes the policy in, so v0.1 refuses the change rather than
    /// enforcing the stale one; re-creating with the original policy (or after a
    /// fresh boot) is fine. Re-opening the tier under the new policy is a deferred
    /// follow-up.
    #[error(
        "table {table:?} cannot be re-created with a different valid-time policy in the same session"
    )]
    ValidTimePolicyChange {
        /// The re-created table name.
        table: String,
    },

    /// A bound write was applied against a table whose shape changed since it was
    /// bound — its value-column count no longer matches the bound values /
    /// assignments. Reachable when DDL drops and re-creates a table between
    /// staging a write in a transaction and committing it; refused rather than
    /// writing a payload that no longer matches the live schema (or panicking on
    /// an out-of-range value-column index).
    #[error(
        "table {table:?} shape changed between binding and applying a write \
         (live has {live} value column(s), the write was bound for {bound})"
    )]
    SchemaChanged {
        /// The table written.
        table: String,
        /// The value-column count the table has now.
        live: usize,
        /// The value-column count (or highest index) the write was bound for.
        bound: usize,
    },

    /// A statement kind the session engine does not route — it is neither DDL, a
    /// `SELECT`, nor an `INSERT` / `UPDATE` / `DELETE`.
    #[error("statement not routable by the session engine: {0}")]
    Unsupported(&'static str),

    /// A `\history` introspection key literal ([STL-199]) could not be folded to
    /// the table's key-column type — a `NULL`, wrong-typed, or out-of-range key.
    /// Carries the reason; the wire layer maps it to `22P02`
    /// (`invalid_text_representation`).
    #[error("invalid history key: {0}")]
    IntrospectionKey(String),

    /// A snapshot-isolation **write-write conflict**: a key this transaction wrote
    /// was committed by another transaction *after* this one's pinned snapshot.
    /// First committer wins — the loser is aborted and the **whole transaction
    /// must be retried** ([ADR-0008], [STL-175]). Surfaced at `COMMIT`; the wire
    /// layer maps it to SQLSTATE `40001` (`serialization_failure`), which stock
    /// clients treat as retryable.
    ///
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    #[error(
        "write-write conflict: this transaction's write set was modified by a concurrent commit \
         after its snapshot; retry the transaction"
    )]
    Conflict,

    /// A `FOR VALID_TIME AS OF` pin of a transaction's overlaid rows
    /// (read-your-own-writes — [STL-223]) could not be applied: either the table's
    /// period columns could not be resolved to positions, or a period bound
    /// (`valid_from` / `valid_to`) cell was missing or not a well-formed eight-byte
    /// timestamp. The binder routes a valid pin only to a valid-time table and always
    /// writes both bounds as concrete instants, so this signals an internal contract
    /// break (a corrupt buffered write or scanned row, or a schema/temporal
    /// mismatch), never user input — surfaced rather than silently returning rows
    /// outside the pin.
    ///
    /// [STL-223]: https://allegromusic.atlassian.net/browse/STL-223
    #[error("valid-time period information for an overlaid AS OF read could not be resolved")]
    MalformedValidBound,

    /// A business key scanned while expanding a scan-then-write `UPDATE` /
    /// `DELETE` ([STL-229]) was missing or could not be decoded back to the key
    /// column's type. The scan only returns live rows, whose key is never `NULL`
    /// and always carries the canonical encoding the binder folds literals to, so
    /// this signals corruption or a schema disagreement — the statement fails
    /// closed rather than writing to a wrong key.
    ///
    /// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
    #[error("a scanned business key could not be decoded while expanding a predicate DML")]
    MalformedBusinessKey,

    /// A `CREATE USER` named a user that already exists ([STL-252]). Postgres
    /// wording so the wire layer's `42710` (`duplicate_object`) reads natively.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    #[error("role {0:?} already exists")]
    DuplicateUser(String),

    /// An `ALTER USER` / `DROP USER` named a user that does not exist
    /// ([STL-252]). Postgres wording for the wire layer's `42704`
    /// (`undefined_object`).
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    #[error("role {0:?} does not exist")]
    UnknownUser(String),

    /// The OS entropy source failed while generating a SCRAM salt
    /// ([STL-252]). The user DDL is refused — a predictable salt is not an
    /// acceptable fallback.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    #[error("entropy source unavailable: {0}")]
    Entropy(#[source] io::Error),

    /// Two source rows of one `MERGE` resolved to the same target row — the
    /// statement would update or insert one business key twice, with an
    /// order-dependent result. Refused deterministically at expansion (the
    /// standard's posture, SQLSTATE `21000`), before any write applies, so the
    /// statement leaves the table unchanged ([STL-230]).
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    #[error("MERGE cannot affect the same target row a second time")]
    MergeRowTwice,

    /// A `MERGE` source-table row could not be decoded back to its declared
    /// column types while expanding the plan ([STL-230]). The scan returns the
    /// canonical cell encodings the binder folds literals to, so this signals
    /// corruption or a schema disagreement — the statement fails closed rather
    /// than writing values from a row it cannot read.
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    #[error("a MERGE source row could not be decoded while expanding the plan")]
    MalformedMergeSource,

    /// A scalar subquery used as a comparison operand returned **more than one
    /// row** ([STL-234]). A scalar subquery must yield at most one value; this is
    /// the standard's cardinality violation (SQLSTATE `21000`), raised before the
    /// outer filter runs so the whole statement fails deterministically rather
    /// than picking an arbitrary row's value.
    ///
    /// [STL-234]: https://allegromusic.atlassian.net/browse/STL-234
    #[error("more than one row returned by a subquery used as an expression")]
    ScalarSubqueryCardinality,
}

/// One table's live state inside a session.
struct TableState<C: Clock + Clone, D: Disk + Clone> {
    engine: Engine<MonotonicClock<C>, NamespacedDisk<D>>,
    /// The valid-time policy the tier's writer was opened with. Baked into the
    /// `DmlWriter`, so a re-create that changes it cannot reuse this tier.
    valid_time: bool,
    /// The namespace index this tier lives on — which `t{idx:020}-` slice of the
    /// shared disk. Recorded in the table's `CreateTable` catalog-log records
    /// (the same index again on a tier-reusing re-create), so recovery reopens
    /// exactly this slice ([ADR-0028]).
    ///
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    namespace: u64,
}

/// The per-connection database engine: the catalog, the commit clock, and the
/// per-table storage tiers, over one configured backend.
///
/// Build a fresh session with [`open`](Self::open); thread parsed statements
/// through [`execute`](Self::execute). State persists across statements for the
/// life of the engine — a `CREATE TABLE` registers a table and stands up its
/// tiers, a later `INSERT` writes to them, and a later `SELECT` reads them back.
pub struct SessionEngine<C: Clock + Clone, D: Disk + Clone> {
    catalog: Catalog,
    clock: MonotonicClock<C>,
    disk: D,
    tables: BTreeMap<String, TableState<C, D>>,
    /// The next per-table namespace index to hand out — only ever increases, so
    /// each newly created table gets its own on-disk slice. A dropped name whose
    /// tier is still resident keeps that slice on re-creation (the tier is reused,
    /// not reopened), so its history is never dropped.
    next_namespace: u64,
    /// Optional override for the rows-per-row-group bound each table's flush seals
    /// segments with ([`Engine::with_flush_row_group_rows`], [STL-197]): `None`
    /// keeps the storage default (a narrow flush stays one row-group), `Some(n)`
    /// splits every flush into finer, independently-skippable row-groups. Applied
    /// to each tier as it is opened ([`open_tier`](Self::open_tier)). The
    /// read-accounting tests seed a small bound through it to exercise
    /// row-group-granular pruning end-to-end; production leaves it `None`.
    ///
    /// [STL-197]: https://allegromusic.atlassian.net/browse/STL-197
    flush_row_group_rows: Option<usize>,
    /// The next transaction id to stamp on a routed DML commit. v0.1 has no real
    /// transaction manager yet ([STL-99]); a per-session monotonic counter gives
    /// each `INSERT` / `UPDATE` / `DELETE` distinct provenance until one exists.
    next_txn: u64,
    /// The running head of the durable hash-chained commit log — the SHA-256 of the
    /// last [`CommitRecord`] appended to `stele.commits`, i.e. the `prev_hash` of
    /// the next one ([ADR-0031], STL-302). [`Digest::ZERO`] for a fresh session;
    /// recovered from the verified chain on restart. Reading the **durable** log and
    /// anchoring its verify against this in-memory head is what makes `\audit`'s
    /// verdict catch both an interior tamper (a broken link) and a wholesale tail
    /// rewrite (a head mismatch).
    ///
    /// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
    commit_head: Digest,
    /// The per-commit sequence number the next commit record takes — a dense,
    /// monotonic session counter, the [`CommitRecord::seq`] tiebreak ([ADR-0024]).
    /// Starts at `1` on a fresh session; recovered as `last seq + 1`.
    commit_seq: u64,
    /// The running head of the durable hash-chained **catalog** log — the
    /// SHA-256 link of the last DDL record appended to `stele.catalog`, i.e. the
    /// `prev_hash` of the next one ([ADR-0031], [STL-307]). [`Digest::ZERO`] for
    /// a fresh session; recovered from the verified catalog chain on restart.
    /// Threaded through every `catalog_log::append` so DDL history is
    /// tamper-evident the way the commit log's data history is — the catalog-log
    /// half of invariant 10.
    ///
    /// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
    /// [STL-307]: https://allegromusic.atlassian.net/browse/STL-307
    catalog_head: Digest,
    /// Set when an append to the durable commit log (`stele.commits`) fails *after*
    /// a data leg is already durable. The commit record is now the commit point and
    /// gates recovery ([STL-314]), so a failure here leaves the just-applied
    /// (resident) write witnessed by no record: recovery would discard it, diverging
    /// from the live process. Per the WAL durability contract (invariant 2) that
    /// indeterminate state is a crash, not a clean abort, so the session **poisons**
    /// — [`is_poisoned`](Self::is_poisoned) reports it (the ops `/readyz` turns
    /// unready) and [`execute`](Self::execute) refuses every further statement —
    /// until a restart into [`recover`](Self::recover) drops the unwitnessed leg and
    /// the live/recovered states reconverge. This is the per-table WAL poison's
    /// posture ([STL-217]) for the separate commit-log WAL that [ADR-0031] left
    /// surfaced-but-not-poisoned.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    /// [STL-314]: https://allegromusic.atlassian.net/browse/STL-314
    commit_poisoned: bool,
    /// The MVCC write index: per-`(table, key)`, the commit instant of the most
    /// recent committed write. Every applied write records its commit instant
    /// here, and a multi-statement [`commit`](Self::commit) checks its write set
    /// against it for first-committer-wins conflict detection ([STL-175],
    /// [ADR-0008]). Keyed by table name + business key; one entry per distinct key
    /// (a later write overwrites the instant). [`prune_write_index`](Self::prune_write_index)
    /// bounds it: an entry committed strictly below the oldest live snapshot can
    /// never satisfy a conflict again, so it is dropped — and when no transaction
    /// is open, the whole index is cleared ([STL-204]).
    ///
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    /// [STL-204]: https://allegromusic.atlassian.net/browse/STL-204
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    write_index: BTreeMap<(String, BusinessKey), SystemTimeMicros>,
    /// The snapshots pinned by currently-open transactions ([`OpenSnapshots`]).
    /// Its smallest key is the oldest live snapshot — the floor
    /// [`prune_write_index`](Self::prune_write_index) keeps the write index above.
    /// A [`SnapshotLease`] in each [`SessionTransaction`] maintains the counts, so
    /// a transaction ending on any path (commit, rollback, dropped connection)
    /// updates it without an explicit engine call ([STL-204]).
    open_snapshots: OpenSnapshots,
    /// The floor [`prune_write_index`](Self::prune_write_index) last pruned below:
    /// no write-index entry below it survives. A cheap monotonic guard so a prune
    /// re-scans the index only when the oldest live snapshot has actually risen —
    /// not on every auto-committed write under a long-lived open transaction
    /// ([STL-204]).
    pruned_below: SystemTimeMicros,
    /// The live secondary indexes' access structures, by index name
    /// ([STL-233]). Derived, rebuildable state (see the `secondary` module):
    /// the catalog owns the matching [`IndexDef`] metadata, the durable log
    /// owns its history, and these are (re)built from the table tiers —
    /// at `CREATE INDEX` and on every [`recover`](Self::recover).
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    index_states: BTreeMap<String, IndexState>,
    /// How many reads consulted a secondary index ([STL-233]) — both `Empty`
    /// and `Window` probe answers count, since both replaced full-scan
    /// planning. Monotonic over the session; a [`Cell`] because the read path
    /// is `&self`. Observability for the equivalence oracle today and
    /// `EXPLAIN` ([STL-260]) tomorrow.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    /// [STL-260]: https://allegromusic.atlassian.net/browse/STL-260
    index_probes: Cell<u64>,
    /// The session's metric registry ([STL-253]): every statement, transaction
    /// outcome, flush/checkpoint, scan, and (via [`Engine::set_metrics`]) WAL
    /// append/fsync reports into it. Owned here — the engine is the one place
    /// every instrumented path meets — and shared by `Arc` with the wire front
    /// end and the ops HTTP listener that renders it. Durations read the
    /// registry's installed time source
    /// ([`Metrics::install_time_source`](stele_common::metrics::Metrics::install_time_source)),
    /// which no test or simulator installs, so instrumentation never makes the
    /// engine read a wall clock itself ([ADR-0010]).
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    /// [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md
    metrics: SharedMetrics,
    /// The live user store ([STL-252]): user name → stored SCRAM verifier.
    /// Current state only — the durable history is the catalog log's
    /// `CreateUser`/`AlterUser`/`DropUser` records, which
    /// [`recover`](Self::recover) replays to rebuild this map. Never holds a
    /// password.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    users: BTreeMap<String, ScramVerifier>,
    /// The principal stamped on every version this session commits ([STL-300]).
    ///
    /// Provenance is captured inline at commit (invariant 5); this is its identity
    /// half. A fresh or recovered session defaults to [`WIRE_PRINCIPAL`] (`stele`),
    /// and direct, non-wire callers (engine and oracle tests, recovery's re-derived
    /// closes) leave it there. The pg-wire front end — where one engine is shared
    /// across connections behind a single mutex — overrides it per statement via
    /// [`set_principal`](Self::set_principal), **under the same lock as the dispatch
    /// that follows**, so each committed write records the connection's
    /// authenticated identity even as connections interleave on the shared engine.
    ///
    /// [STL-300]: https://allegromusic.atlassian.net/browse/STL-300
    write_principal: Principal,
}

impl<C: Clock + Clone, D: Disk + Clone> SessionEngine<C, D> {
    /// Open a **fresh** session over `disk` with commit time drawn from `clock`.
    ///
    /// The catalog starts empty and no tiers exist; `CREATE TABLE` populates
    /// both. Intended for an **empty** disk (mirroring [`Engine::open`]): to
    /// boot from existing on-disk state — a restart — use
    /// [`recover`](Self::recover), which replays the durable catalog log and
    /// reopens every table's tiers. Opening a fresh session over a disk that
    /// already holds a catalog log would shadow, not resume, its state.
    #[must_use]
    pub fn open(disk: D, clock: C) -> Self {
        Self {
            catalog: Catalog::new(),
            clock: MonotonicClock::new(clock),
            disk,
            tables: BTreeMap::new(),
            next_namespace: 0,
            flush_row_group_rows: None,
            next_txn: 1,
            commit_head: Digest::ZERO,
            commit_seq: 1,
            catalog_head: Digest::ZERO,
            commit_poisoned: false,
            write_index: BTreeMap::new(),
            open_snapshots: OpenSnapshots::default(),
            pruned_below: SystemTimeMicros(0),
            index_states: BTreeMap::new(),
            index_probes: Cell::new(0),
            metrics: SharedMetrics::default(),
            users: BTreeMap::new(),
            write_principal: Principal::new(WIRE_PRINCIPAL.to_vec()),
        }
    }

    /// Override the rows-per-row-group bound each table's flush seals segments with
    /// ([`Engine::with_flush_row_group_rows`], [STL-197]), for **every** tier this
    /// session opens after the call. Builder-style so the [`open`](Self::open) /
    /// [`recover`](Self::recover) call sites that want the storage default stay
    /// untouched. A smaller bound splits a flush into more, finer row-groups — each
    /// independently skippable by the read path ([STL-155]) — so the read-accounting
    /// tests can exercise row-group-granular pruning end-to-end without a
    /// thousand-row fixture; `0` is clamped to `1`, the same clamp
    /// [`Engine::with_flush_row_group_rows`] applies.
    ///
    /// [STL-155]: https://allegromusic.atlassian.net/browse/STL-155
    /// [STL-197]: https://allegromusic.atlassian.net/browse/STL-197
    #[must_use]
    pub fn with_flush_row_group_rows(mut self, rows: usize) -> Self {
        self.flush_row_group_rows = Some(rows.max(1));
        self
    }

    /// **Recover** a session from existing on-disk state — the cold-boot path
    /// ([STL-210], [ADR-0028]) that closes the loop [`Engine::recover`] left
    /// open at the session level ("enumerating which tables exist needs durable
    /// catalog state"). On an empty disk this equals [`open`](Self::open), so a
    /// server can boot through it unconditionally.
    ///
    /// The flow composes the durable pieces:
    ///
    /// 1. **Replay the catalog log** ([ADR-0028]): apply every recorded
    ///    DDL mutation, in order, at its recorded instant. This reproduces the
    ///    schema-version chains — so an `AS OF` read in the past still resolves
    ///    the schema live *then*, across restarts — and the `SchemaId`
    ///    allocation order, exactly.
    /// 2. **Reopen every recorded namespace** through
    ///    [`Engine::recover_with_commits`](stele_storage::engine::Engine::recover_with_commits)
    ///    (segment checksums + checkpoint + WAL tail replay, [STL-102]/
    ///    [STL-177]) — dropped names included: their retained history must keep
    ///    answering `AS OF` reads, and a re-create must reuse the same tier so
    ///    that history is neither duplicated nor orphaned. The replayed
    ///    **commit-marker log** ([STL-215]) gates each table's two-phase legs: a
    ///    multi-table transaction's writes are replayed only if its marker is
    ///    durable, so a crash between the per-table commits and the marker recovers
    ///    the transaction all-or-none across every table.
    /// 3. **Reposition the allocators.** The shared commit clock's high-water
    ///    mark is raised past every recovered commit instant and DDL instant —
    ///    without this, the default read snapshot would sit at the origin and a
    ///    post-restart `SELECT` would see nothing — and `next_txn` past every
    ///    recovered transaction id, so post-restart commits never share
    ///    provenance with recovered ones.
    ///
    /// The MVCC write index restarts **empty**, deliberately: a conflict is a
    /// commit *after* a transaction's pinned snapshot, every recovered commit
    /// precedes the repositioned high-water mark, and any post-restart
    /// transaction pins its snapshot at or past that mark — so no recovered
    /// commit can ever conflict with a post-restart transaction.
    ///
    /// [STL-102]: https://allegromusic.atlassian.net/browse/STL-102
    /// [STL-177]: https://allegromusic.atlassian.net/browse/STL-177
    /// [STL-210]: https://allegromusic.atlassian.net/browse/STL-210
    /// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    ///
    /// # Errors
    ///
    /// [`EngineError::CatalogLog`] if the catalog log cannot be read or holds a
    /// corrupt acknowledged record; [`EngineError::CommitLog`] if the commit-marker
    /// log cannot be read or holds a corrupt acknowledged marker; [`EngineError::Catalog`]
    /// if replaying a record is refused (a log/catalog invariant break — fails
    /// closed); [`EngineError::Storage`] if a table's tiers cannot be recovered.
    pub fn recover(disk: D, clock: C) -> Result<Self, EngineError> {
        // Replay the durable catalog log and verify its hash chain ([ADR-0031],
        // [STL-307]): a tampered DDL record breaks the chain and fails closed
        // here (mapped to `CatalogLog`), refusing recovery rather than serving
        // forged catalog history. The verified head seeds `catalog_head` so
        // post-restart DDL chains on.
        let (records, catalog_head) =
            catalog_log::replay(&disk).map_err(EngineError::CatalogLog)?;
        // The durable hash-chained commit log ([ADR-0031], STL-302). Replay its
        // ordered commit-record payloads, then:
        //  - verify the chain fails-closed — a tampered historical record refuses
        //    recovery rather than serving forged history, extending STL-178's
        //    recovery verification to the live server — and recover its tail
        //    (`commit_head` / `commit_seq`) so post-restart commits chain on,
        //  - derive the committed-`txn_id` set that gates each table's two-phase
        //    legs ([STL-215]): a leg is replayed only if its transaction
        //    committed, so a crash between the per-table commits and the commit
        //    record recovers all-or-none across every table the transaction wrote.
        let commit_records = commit_log::replay(&disk).map_err(EngineError::CommitLog)?;
        let recovered_chain =
            verify_chain_recover(commit_records.iter().cloned().map(Ok::<_, WalError>))
                .map_err(EngineError::CommitChain)?;
        let committed = CommittedTxns::Only(
            commit_records
                .iter()
                .map(|payload| CommitRecord::decode(payload).map(|record| record.txn_id))
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    EngineError::CommitLog(io::Error::new(
                        io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                })?,
        );
        let clock = MonotonicClock::new(clock);

        // 1. Rebuild the catalog (and the user store) by replaying the DDL
        //    history — see [`fold_catalog_records`].
        let ReplayedCatalog {
            catalog,
            users,
            tiers,
            latest_drop,
            next_namespace,
            mut max_commit,
        } = fold_catalog_records(records)?;

        // 2. Reopen each recorded tier from its slice of the disk, and fold in
        //    its high-water marks (largest commit instant / txn id on disk).
        let mut tables = BTreeMap::new();
        let mut max_txn_id = 0u64;
        for (name, (namespace, valid_time)) in tiers {
            let tier_disk = NamespacedDisk::new(disk.clone(), namespace);
            let engine =
                Engine::recover_with_commits(tier_disk, clock.clone(), valid_time, &committed)?;
            let marks = engine.recovery_marks()?;
            max_commit = max_commit.max(marks.max_commit);
            max_txn_id = max_txn_id.max(marks.max_txn_id);
            tables.insert(
                name,
                TableState {
                    engine,
                    valid_time,
                    namespace,
                },
            );
        }

        // 3. Position the allocators past everything recovered. Saturating: a
        //    recovered id at the u64 ceiling must not wrap the allocator back
        //    into recovered provenance. The commit log's greatest `txn_id` is folded
        //    in alongside the tiers' marks — they agree (both come from commits),
        //    but a committed transaction whose data was pruned would live only in the
        //    chain, so honoring it keeps `next_txn` past every recorded commit.
        clock.advance_to(max_commit);
        let mut next_txn = max_txn_id
            .max(recovered_chain.max_txn_id.0)
            .saturating_add(1);

        // 4. Re-derive each dropped era's storage closes from the durable
        //    catalog drop record ([STL-220], [`close_dropped_eras`](Self::close_dropped_eras)).
        let now = Snapshot(clock.current());
        Self::close_dropped_eras(&mut tables, latest_drop, now, &mut next_txn)?;

        let mut session = Self {
            catalog,
            clock,
            disk,
            tables,
            next_namespace,
            flush_row_group_rows: None,
            next_txn,
            commit_head: recovered_chain.head,
            commit_seq: recovered_chain.seq.saturating_add(1),
            catalog_head,
            commit_poisoned: false,
            write_index: BTreeMap::new(),
            open_snapshots: OpenSnapshots::default(),
            pruned_below: SystemTimeMicros(0),
            index_states: BTreeMap::new(),
            index_probes: Cell::new(0),
            metrics: SharedMetrics::default(),
            users,
            write_principal: Principal::new(WIRE_PRINCIPAL.to_vec()),
        };
        // Recovered tiers were opened before the session's registry existed;
        // point their WALs at it now ([STL-253]).
        for state in session.tables.values() {
            state.engine.set_metrics(Arc::clone(&session.metrics));
        }
        // 5. Rebuild every live secondary index from the recovered tiers
        //    ([STL-233], the ADR-0023 derived-state posture): the durable log
        //    carries only the metadata, so the access structures are
        //    reconstructed from the rows live at the recovered high-water mark.
        //    That instant becomes each structure's floor — reads at or after it
        //    may probe, earlier `AS OF` reads full-scan (exactly the build
        //    semantics `CREATE INDEX` gives a fresh index). This also closes
        //    the crash-mid-build window: an acknowledged `CREATE INDEX` whose
        //    in-memory build died with the process is simply rebuilt here.
        session.index_states = Self::rebuild_index_states(
            &session.catalog,
            &session.tables,
            session.clock.current(),
            &session.metrics,
        )?;
        Ok(session)
    }

    /// Re-derive each dropped era's storage closes from the durable catalog
    /// drop records — step 4 of [`recover`](Self::recover) ([STL-220]). With
    /// the clock at the recovered high-water, `close_dropped_era` resolves each
    /// key's *current* open version there and closes only the ones that predate
    /// the drop — idempotent if the live closes already reached the WAL, and
    /// leaving a re-created era untouched. This makes the drop's row cleanup a
    /// pure function of the fsynced catalog log, so a crash between the drop's
    /// acknowledgement and its (auto-commit) closes recovers the rows retired
    /// rather than leaked. Each close commits strictly past the recovered
    /// high-water, so it never re-selects a row resolved at that snapshot.
    ///
    /// [STL-220]: https://allegromusic.atlassian.net/browse/STL-220
    fn close_dropped_eras(
        tables: &mut BTreeMap<String, TableState<C, D>>,
        latest_drop: BTreeMap<String, SystemTimeMicros>,
        now: Snapshot,
        next_txn: &mut u64,
    ) -> Result<(), EngineError> {
        // Recovery has no connection identity to attribute these re-derived closes
        // to, so they carry the default [`WIRE_PRINCIPAL`] rather than a per-session
        // principal ([STL-300] threads identity into *live* writes, not recovery).
        let principal = Principal::new(WIRE_PRINCIPAL.to_vec());
        for (name, drop_at) in latest_drop {
            if let Some(state) = tables.get_mut(&name) {
                let closed = state.engine.close_dropped_era(
                    Snapshot(drop_at),
                    now,
                    TxnId(*next_txn),
                    &principal,
                )?;
                // Only a drop that actually retired rows consumed the id; a no-op
                // re-derivation leaves the allocator untouched, so a clean restart
                // positions it exactly as before ([STL-210] parity).
                if closed > 0 {
                    *next_txn = next_txn.saturating_add(1);
                }
            }
        }
        Ok(())
    }

    /// Rebuild every live index's access structure from the recovered tiers at
    /// `rebuild_at` — step 5 of [`recover`](Self::recover).
    fn rebuild_index_states(
        catalog: &Catalog,
        tables: &BTreeMap<String, TableState<C, D>>,
        rebuild_at: SystemTimeMicros,
        metrics: &SharedMetrics,
    ) -> Result<BTreeMap<String, IndexState>, EngineError> {
        let mut index_states = BTreeMap::new();
        for def in catalog.live_indexes() {
            // A live index always has a live table (a table drop cascades its
            // indexes away in the same replay), so both lookups must resolve;
            // failing closed beats serving without a recorded index.
            let state = tables
                .get(def.table())
                .ok_or_else(|| EngineError::UnknownTable(def.table().to_owned()))?;
            let schema = catalog
                .resolve(def.table(), rebuild_at)
                .ok_or_else(|| EngineError::UnknownTable(def.table().to_owned()))?;
            index_states.insert(
                def.name().to_owned(),
                Self::build_index_state(state, schema, def, rebuild_at, metrics)?,
            );
        }
        Ok(index_states)
    }

    /// Build one index's access structure from the rows live at `floor` — the
    /// shared core of `CREATE INDEX` ([`apply_ddl`](Self::apply_ddl)) and the
    /// cold-boot rebuild ([`recover`](Self::recover)). Each live row's indexed
    /// cell is noted under its business key; `NULL` cells are skipped (an
    /// equality can never match them). Writes committed after `floor` are noted
    /// by the DML maintenance hook, so together the structure covers every
    /// snapshot at or after `floor` (the superset contract — see the
    /// `secondary` module docs).
    fn build_index_state(
        state: &TableState<C, D>,
        schema: &TableSchema,
        def: &IndexDef,
        floor: SystemTimeMicros,
        metrics: &SharedMetrics,
    ) -> Result<IndexState, EngineError> {
        let columns = schema.columns();
        // The catalog validated the column at create/replay; resolve its
        // position in the (append-only) schema.
        let position = columns
            .iter()
            .position(|c| Some(c.name()) == def.columns().first().map(String::as_str))
            .ok_or_else(|| {
                EngineError::Catalog(CatalogError::IndexColumnUnknown {
                    index: def.name().to_owned(),
                    column: def.columns().first().cloned().unwrap_or_default(),
                })
            })?;
        let value_count = columns.len().saturating_sub(1);
        let mut index = IndexState::new(def.kind(), columns[position].ty(), floor);
        // Index maintenance reads every row; the scan's pruning accounting is for
        // the query footer ([STL-201]), irrelevant here, so the rows alone are taken.
        for row in Self::scan_all_rows(state, floor, value_count, metrics)?.rows {
            let Some(key) = row.first().cloned().flatten() else {
                continue; // a row always carries its key; nothing to note without one
            };
            if let Some(cell) = row.get(position).and_then(|c| c.as_deref()) {
                index.structure.note(cell, &BusinessKey::new(key));
            }
        }
        Ok(index)
    }

    /// Take a lightweight **checkpoint** of every resident table: group-commit
    /// fsync each table's WAL and record its durable fence, *without* sealing the
    /// delta tier ([`Engine::checkpoint`]). This is the cheap durability fence —
    /// recovery still replays each table's log from its floor — and the sibling
    /// of [`flush`](Self::flush), which additionally bounds that replay.
    ///
    /// Drives **every resident tier**, including a dropped table's retained tier:
    /// its WAL is still replayed on the next [`recover`](Self::recover), so
    /// fencing it is meaningful even though the catalog no longer resolves the
    /// name.
    ///
    /// This is the operator-facing **manual trigger** [STL-177] deferred
    /// ([STL-195]). A background *policy* that decides *when* to checkpoint, and a
    /// SQL/admin `CHECKPOINT` command so a wire client can trigger it, are both
    /// out of scope here ([STL-219]).
    ///
    /// Per-table checkpoints are independently durable and idempotent, so a
    /// failure part-way leaves the already-fenced tiers fenced; the call returns
    /// the first error.
    ///
    /// [STL-177]: https://allegromusic.atlassian.net/browse/STL-177
    /// [STL-195]: https://allegromusic.atlassian.net/browse/STL-195
    /// [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if any table's checkpoint fails.
    pub fn checkpoint(&mut self) -> Result<(), EngineError> {
        let started = self.metrics.now_micros();
        for state in self.tables.values_mut() {
            state.engine.checkpoint()?;
        }
        self.metrics
            .checkpoint_seconds
            .observe_micros(self.metrics.now_micros().saturating_sub(started));
        Ok(())
    }

    /// **Flush** every resident table: seal each delta tier into a fresh sealed
    /// segment and advance each table's replay floor past the records it now
    /// covers, so the next [`recover`](Self::recover) replays only each WAL's tail
    /// rather than its whole log ([`Engine::flush`]). This is the bounded-recovery
    /// win [STL-177] landed at the storage layer, now reachable from the session
    /// ([STL-195]).
    ///
    /// Drives **every resident tier**, including a dropped table's retained tier:
    /// recovery reopens and replays that tier's WAL too, so flushing it bounds
    /// that work even though the catalog no longer resolves the name.
    ///
    /// A background *policy* that decides *when* to flush is out of scope here
    /// ([STL-177] / [STL-219]); the wire `FLUSH` admin command drives this
    /// ([STL-219]), and history-preserving compaction builds on it
    /// ([`compact`](Self::compact), [STL-231]).
    ///
    /// Each table's flush is its own crash-atomic, idempotent unit (the new
    /// segment is adopted only once its checkpoint record is durable —
    /// [`Engine::flush`]), so a failure part-way leaves the already-flushed tiers
    /// flushed; the call returns the first error. A re-run re-flushes whatever the
    /// failure left unsealed.
    ///
    /// [STL-177]: https://allegromusic.atlassian.net/browse/STL-177
    /// [STL-195]: https://allegromusic.atlassian.net/browse/STL-195
    /// [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if any table's flush fails.
    pub fn flush(&mut self) -> Result<(), EngineError> {
        let started = self.metrics.now_micros();
        for state in self.tables.values_mut() {
            state.engine.flush()?;
        }
        self.metrics
            .flush_seconds
            .observe_micros(self.metrics.now_micros().saturating_sub(started));
        Ok(())
    }

    /// **Compact** every resident table: flush its delta, then merge its sealed
    /// segments into one consolidated, read-optimized segment, atomically
    /// swapping the live set and retiring the inputs ([`Engine::compact`],
    /// [STL-231], [ADR-0030]). The flush first folds the delta tier in, so
    /// `COMPACT` leaves each table with at most one sealed segment and an empty
    /// delta — the "merge delta + small sealed segments" shape of the ticket.
    ///
    /// Drives **every resident tier**, including a dropped table's retained
    /// tier, for the same reason [`flush`](Self::flush) does: recovery reopens
    /// that tier too, and compacting it bounds that work.
    ///
    /// Each table's flush and compaction are their own crash-atomic units (the
    /// swap is one durable manifest record — [`Engine::compact`]), so a failure
    /// part-way leaves the already-compacted tables compacted; the call returns
    /// the first error and a re-run re-compacts whatever was left. Background
    /// *scheduling* of compaction is a deliberate follow-up; this is the manual
    /// admin trigger ([STL-231] scope).
    ///
    /// [STL-231]: https://allegromusic.atlassian.net/browse/STL-231
    /// [ADR-0030]: ../../../docs/adr/0030-segment-manifest-retirement.md
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if any table's flush or compaction fails.
    pub fn compact(&mut self) -> Result<(), EngineError> {
        let started = self.metrics.now_micros();
        for state in self.tables.values_mut() {
            state.engine.flush()?;
            state.engine.compact()?;
        }
        self.metrics
            .compaction_seconds
            .observe_micros(self.metrics.now_micros().saturating_sub(started));
        Ok(())
    }

    /// Take a consistent, online full **backup** into the (empty) `target` disk
    /// ([STL-249], [ADR-0032]).
    ///
    /// Fences first — [`flush`](Self::flush) seals every table's delta into an
    /// immutable segment and [`checkpoint`](Self::checkpoint) fsyncs every WAL —
    /// so the on-disk set is a complete, recoverable snapshot, then copies the
    /// immutable set (sealed segments, per-table WALs, the durable catalog log,
    /// and the hash-chained commit log) verbatim into `target` with a
    /// [`BackupManifest`](backup::BackupManifest). The *fence instant* the manifest
    /// records is the commit clock's high-water mark: every `AS OF` read at or
    /// before it answers identically on the restored copy
    /// ([`backup::backup_disk`]).
    ///
    /// "Online" here means the server stays up: the call runs synchronously,
    /// holding the session lock for its duration — the same brief stop-the-world
    /// `FLUSH` / `COMPACT` already are ([STL-219]). Concurrent writers queue behind
    /// it, and anything they commit *after* the fence is not in the backup. A
    /// fully non-blocking streaming backup is a deliberate follow-up the recorded
    /// fence leaves room for.
    ///
    /// Restore is the inverse, **offline** operation — [`backup::restore_disk`]
    /// (verify + materialize) then [`recover`](Self::recover) (segment checksums +
    /// the commit-log hash chain re-verify) — exposed as the `stele restore` CLI
    /// verb. The `BACKUP TO '<path>'` admin command drives this method over the
    /// wire ([STL-219] shape).
    ///
    /// # Errors
    ///
    /// [`EngineError`] if the fence (flush / checkpoint) fails, or
    /// [`EngineError::Backup`] if `target` is non-empty or the copy hits an I/O
    /// error. The fence runs before any copy, so a copy failure leaves the live
    /// engine fenced but otherwise untouched; a re-run into a fresh target retries.
    ///
    /// [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
    /// [STL-249]: https://allegromusic.atlassian.net/browse/STL-249
    /// [ADR-0032]: ../../../docs/adr/0032-backup-manifest-format.md
    pub fn backup<T: Disk>(&mut self, target: &T) -> Result<backup::BackupManifest, EngineError> {
        self.flush()?;
        self.checkpoint()?;
        let fence = self.clock.current();
        let manifest = backup::backup_disk(&self.disk, target, fence.0, self.commit_head)?;
        Ok(manifest)
    }

    /// Whether the session is **poisoned** — its durability is indeterminate and it
    /// must stop serving and restart into [`recover`](Self::recover) (a failed fsync
    /// is a crash, not a clean abort; recovery resolves the indeterminate record from
    /// the durable log while opening fresh, unpoisoned WALs). Two sources:
    ///
    /// * **a resident table's WAL** — a prior fsync failed on that table, so its
    ///   staged record's durability is indeterminate and the per-table engine now
    ///   refuses further writes ([`Engine::is_poisoned`], [STL-217]). Spans every
    ///   resident tier, including dropped-but-retained ones, since each owns its WAL.
    /// * **the commit log** — a commit record failed to reach `stele.commits` after
    ///   its data leg was durable (`commit_poisoned`, [STL-314]), so recovery would
    ///   discard a write the live process applied. The commit log is a separate WAL
    ///   ADR-0031 left surfaced-but-not-poisoned; this closes it.
    ///
    /// The ops `/readyz` reads this, so either source turns the server unready.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    /// [STL-314]: https://allegromusic.atlassian.net/browse/STL-314
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.commit_poisoned || self.tables.values().any(|state| state.engine.is_poisoned())
    }

    /// The session's catalog — schemas resolve at a snapshot through it.
    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The commit clock's current high-water mark ([`MonotonicClock::current`]).
    /// After a single auto-committed [`execute`](Self::execute) of an `INSERT` /
    /// `UPDATE` / `DELETE`, this is exactly that statement's commit instant — the
    /// commit is the last thing to advance the clock (the engine assigns commit
    /// time internally, so a caller cannot otherwise observe it). The differential
    /// correctness oracle uses it to align an independent reference's timeline with
    /// the engine's own commit ticks ([STL-167]). Note the mark also rises when a
    /// read takes a fresh snapshot ([`MonotonicClock::observe`], [STL-227]), so
    /// between writes it tracks the last *observation*, not the last commit.
    ///
    /// [STL-227]: https://allegromusic.atlassian.net/browse/STL-227
    ///
    /// [STL-167]: https://allegromusic.atlassian.net/browse/STL-167
    #[must_use]
    pub fn commit_clock(&self) -> SystemTimeMicros {
        self.clock.current()
    }

    /// Whether `table` resolves to a live schema that opts into a valid-time axis —
    /// the predicate the session-time front end consults to decide whether a session
    /// `SET stele.valid_time` pin may be injected over this read ([STL-325],
    /// [`stele_sql::apply_session_time`]). A system-only table (and an unknown name)
    /// is `false`, so a blanket valid pin is withheld from a join with any
    /// system-only input rather than turning it into a bind error — the same
    /// `valid_time_enabled` check the binder makes per join side ([STL-243]).
    ///
    /// Resolved at the commit clock's current instant ([`describe_live_tables`](Self::describe_live_tables)),
    /// the latest committed schema. A table's valid-time policy is fixed at
    /// `CREATE TABLE` (there is no `ALTER … VALID TIME`), so the answer is stable
    /// across snapshots for a live name; the binder does the authoritative per-side
    /// resolution at the read snapshot.
    ///
    /// [STL-243]: https://allegromusic.atlassian.net/browse/STL-243
    /// [STL-325]: https://allegromusic.atlassian.net/browse/STL-325
    #[must_use]
    pub fn table_has_valid_axis(&self, table: &str) -> bool {
        self.catalog
            .resolve(table, self.clock.current())
            .is_some_and(|schema| schema.temporal().valid_time_enabled())
    }

    /// How many reads have consulted a secondary index this session
    /// ([STL-233]) — monotonic, counting every probe (whether it proved
    /// emptiness or produced a candidate window). The indexed≡unindexed
    /// equivalence oracle asserts on this to prove the index path actually
    /// ran; `EXPLAIN` ([STL-260]) is its operator-facing future.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    /// [STL-260]: https://allegromusic.atlassian.net/browse/STL-260
    #[must_use]
    pub const fn index_probe_count(&self) -> u64 {
        self.index_probes.get()
    }

    /// The live tables and their columns at the current read snapshot.
    ///
    /// "Live" means the catalog resolves the name at the commit clock's current
    /// instant — a dropped table keeps its tier resident for history but is not
    /// reported here, so the result matches what a `\d`-style introspection query
    /// should see *now*. Tables are returned in name order (the tier map is a
    /// [`BTreeMap`]). Feeds the pgwire `pg_catalog` shim ([STL-131]).
    ///
    /// [STL-131]: https://allegromusic.atlassian.net/browse/STL-131
    #[must_use]
    pub fn describe_live_tables(&self) -> Vec<TableDescription> {
        let snapshot = self.clock.current();
        self.tables
            .keys()
            .filter_map(|name| {
                let schema = self.catalog.resolve(name, snapshot)?;
                let columns = schema
                    .columns()
                    .iter()
                    .map(|c| (c.name().to_owned(), c.ty()))
                    .collect();
                Some(TableDescription {
                    name: name.clone(),
                    columns,
                })
            })
            .collect()
    }

    /// The append-only version timeline of `key` in `table` — or of every key when
    /// `key` is `None` — for the shell's `\history` / `\timeline` / `\lineage`
    /// temporal commands ([STL-199]).
    ///
    /// The result is a [`SelectResult`] whose columns are a fixed metadata prefix
    /// — `txid`, `op`, `sys_from`, `sys_to`, `current`, `principal` — then the
    /// table's own columns (the business key, then its value columns), so one
    /// reply feeds every renderer. There is one row per version, grouped by key and
    /// ordered oldest-to-newest within each key; superseded and deleted versions
    /// are all present (Stele never destroys history), a current version has
    /// `sys_to = NULL` / `current = true`.
    ///
    /// The key literal is folded to the key column's type **the same way
    /// `bind_dml` folds an `INSERT` key** ([`stele_sql::fold_literal`]), so the
    /// business key matches byte-for-byte. Each version's stored payload is sliced
    /// back into its value columns ([row codec](stele_common::row_codec)); its `op`
    /// is derived from chain adjacency (`version_op`) and its provenance rides
    /// inline on the record. Read-only introspection: it makes no commit and
    /// mutates nothing. An empty result (unknown key, empty table) is `Ok` with
    /// zero rows, not an error.
    ///
    /// # Errors
    ///
    /// [`EngineError::UnknownTable`] if `table` is not live;
    /// [`EngineError::IntrospectionKey`] if `key` cannot be folded to the key
    /// column's type; [`EngineError::Storage`] / [`EngineError::RowCodec`] if a
    /// tier read or a payload decode fails.
    pub fn version_history(
        &self,
        table: &str,
        key: Option<&stele_sql::sqlparser::ast::Expr>,
    ) -> Result<SelectResult, EngineError> {
        // `version_history` resolves the table's schema at the current instant; a
        // missing tier or unresolvable name is an unknown table.
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema = self
            .catalog
            .resolve(table, self.clock.current())
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema_columns: Vec<(String, LogicalType)> = schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect();
        // Column 0 is the business key; the rest are value columns packed into the
        // payload by the row codec.
        let key_ty = schema_columns
            .first()
            .map_or(LogicalType::Int8, |(_, ty)| *ty);
        let value_count = schema_columns.len().saturating_sub(1);

        // Fold the introspection key exactly as `bind_dml` folded the `INSERT` key,
        // so the business key bytes match what was written ([`fold_literal`]).
        let business_key = match key {
            Some(expr) => Some(business_key(
                &stele_sql::fold_literal(expr, key_ty).map_err(EngineError::IntrospectionKey)?,
            )),
            None => None,
        };

        let versions = state.engine.version_history(business_key.as_ref())?;

        // Columns: the metadata prefix, then the table's own columns (key + values).
        let mut columns = vec![
            ("txid".to_owned(), LogicalType::Int8),
            ("op".to_owned(), LogicalType::Text),
            ("sys_from".to_owned(), LogicalType::TimestampTz),
            ("sys_to".to_owned(), LogicalType::TimestampTz),
            ("current".to_owned(), LogicalType::Bool),
            ("principal".to_owned(), LogicalType::Text),
        ];
        columns.extend(schema_columns.iter().cloned());

        let mut rows = Vec::with_capacity(versions.len());
        let mut prev: Option<&Version> = None;
        for v in &versions {
            let op = version_op(prev, v);
            prev = Some(v);
            let current = v.sys_to == SYSTEM_TIME_OPEN;
            let principal = String::from_utf8_lossy(&v.provenance.principal.0).into_owned();

            // Every cell is the value's canonical encoding (or `None` for NULL), the
            // same shape a `SELECT` ships — the wire layer decodes each by its column
            // type ([`stele_common::types::ScalarValue::decode`]). The business key
            // and the row-codec-sliced value cells are already canonical encodings,
            // so only the synthesized metadata cells are encoded here.
            let mut row: Vec<Option<Vec<u8>>> = vec![
                Some(encode_value(&ScalarValue::Int8(txid_as_i64(
                    v.provenance.txn_id,
                )))),
                Some(encode_value(&ScalarValue::Text(op.to_owned()))),
                Some(encode_value(&ScalarValue::TimestampTz(v.sys_from.0))),
                (!current).then(|| encode_value(&ScalarValue::TimestampTz(v.sys_to.0))),
                Some(encode_value(&ScalarValue::Bool(current))),
                Some(encode_value(&ScalarValue::Text(principal))),
                Some(v.business_key.as_bytes().to_vec()),
            ];
            row.extend(row_codec::decode_payload(
                value_count,
                v.payload.as_deref(),
            )?);
            rows.push(row);
        }
        Ok(SelectResult {
            columns,
            rows,
            // A synthetic catalog / history / audit result is not a table scan, so
            // it carries no scan accounting ([STL-201]).
            stats: None,
        })
    }

    /// The Stele-native segment-introspection result for `table` — the wire
    /// surface the shell's `\segments` command reads ([STL-301]), mirroring
    /// [`version_history`](Self::version_history). One row per sealed segment
    /// (oldest first), then the resident delta (hot) tier, each shaped into the
    /// same [`SelectResult`] an ordinary `SELECT` ships so the whole wire path
    /// carries it unchanged.
    ///
    /// The user's value columns are packed into the opaque payload, so a segment
    /// carries no per-value-column statistics; the "zone map" surfaced here is the
    /// **business-key** zone (the column the planner prunes point lookups on). Its
    /// opaque bound bytes are decoded against the table's key column type: a clean
    /// round-trip ships the canonical bytes (the client renders them by the key's
    /// type), while a bound that does not decode — a truncated variable-width
    /// prefix — ships `NULL` rather than risk the wire text encoder on partial
    /// bytes. Because the key encoding is little-endian (not order-preserving),
    /// these are the zone's *encoding-order* bounds, intuitive for text keys and
    /// small-integer keys.
    ///
    /// # Errors
    ///
    /// [`EngineError::UnknownTable`] if `table` has no resident tier or does not
    /// resolve at the current instant, or a storage error from reading the tiers.
    pub fn segment_metadata(&self, table: &str) -> Result<SelectResult, EngineError> {
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema = self
            .catalog
            .resolve(table, self.clock.current())
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        // Column 0 is the business key: its name labels the zone column, its type
        // decodes the zone bounds. `Option<&Column>` is `Copy`, so it is read
        // twice. A schema with no columns is unreachable, but degrade gracefully.
        let key_col = schema.columns().first();
        let key_name = key_col.map_or_else(|| "key".to_owned(), |c| c.name().to_owned());
        let key_ty = key_col.map_or(LogicalType::Int8, stele_catalog::ColumnDef::ty);

        let metas = state.engine.segment_metadata()?;

        let columns = vec![
            ("segment".to_owned(), LogicalType::Text),
            ("state".to_owned(), LogicalType::Text),
            ("rows".to_owned(), LogicalType::Int8),
            ("sys_min".to_owned(), LogicalType::TimestampTz),
            ("sys_max".to_owned(), LogicalType::TimestampTz),
            ("key_column".to_owned(), LogicalType::Text),
            ("key_min".to_owned(), key_ty),
            ("key_max".to_owned(), key_ty),
            ("bytes".to_owned(), LogicalType::Int8),
        ];

        let rows = metas
            .iter()
            .map(|m| {
                // A sealed segment is named by its file; the resident delta tier
                // has no file and renders as the `hot` state.
                let (id, label) = m.name.as_ref().map_or_else(
                    || ("(hot)".to_owned(), "hot"),
                    |name| (name.clone(), "sealed"),
                );
                vec![
                    Some(encode_value(&ScalarValue::Text(id))),
                    Some(encode_value(&ScalarValue::Text(label.to_owned()))),
                    Some(encode_value(&ScalarValue::Int8(int8_of(m.rows)))),
                    m.sys_min
                        .map(|t| encode_value(&ScalarValue::TimestampTz(t.0))),
                    m.sys_max
                        .map(|t| encode_value(&ScalarValue::TimestampTz(t.0))),
                    Some(encode_value(&ScalarValue::Text(key_name.clone()))),
                    decode_key_bound(m.key_min.as_deref(), key_ty),
                    decode_key_bound(m.key_max.as_deref(), key_ty),
                    m.byte_size
                        .map(|b| encode_value(&ScalarValue::Int8(int8_of(b)))),
                ]
            })
            .collect();

        Ok(SelectResult {
            columns,
            rows,
            // A synthetic catalog / history / audit result is not a table scan, so
            // it carries no scan accounting ([STL-201]).
            stats: None,
        })
    }

    /// The tamper-evident commit-chain audit of `table` — or of every key when
    /// `key` is `None` — for the shell's `\audit` and `\lineage` ([STL-302],
    /// [ADR-0031]).
    ///
    /// Reads the **durable** hash-chained commit log (`stele.commits`) — so on-disk
    /// tampering is what the verdict reflects — and verifies it with
    /// [`verify_chain_to`] **anchored against the live in-memory chain head**,
    /// catching both an interior broken link (a mutated historical record) and a
    /// wholesale tail rewrite (a re-linked forgery). A CRC-failing record is
    /// corruption and surfaces as [`EngineError::CommitLog`] from the commit-log
    /// replay; a well-framed forgery whose chain link is wrong surfaces as a `false`
    /// verdict here.
    ///
    /// The result is a [`SelectResult`]: one row per version of `table` carrying
    /// `(txid, op, hash, prev_hash)` — its commit's chain hash and that record's
    /// predecessor — then the global verdict columns `(chain_ok, chain_len,
    /// chain_head)`, repeated on every row so a renderer reads it off any one. When
    /// `table` has no versions a single verdict-only row (null version cells) still
    /// carries the verdict. `op` and the per-version order match
    /// [`version_history`](Self::version_history) exactly, so `\lineage` zips the two
    /// replies positionally. Read-only: it makes no commit and mutates nothing.
    ///
    /// # Errors
    ///
    /// [`EngineError::UnknownTable`] if `table` is not live;
    /// [`EngineError::IntrospectionKey`] if `key` cannot be folded to the key
    /// column's type; [`EngineError::CommitLog`] if the commit log cannot be read or
    /// holds a corrupt record; [`EngineError::Storage`] / [`EngineError::RowCodec`]
    /// if a tier read fails.
    pub fn audit_chain(
        &self,
        table: &str,
        key: Option<&stele_sql::sqlparser::ast::Expr>,
    ) -> Result<SelectResult, EngineError> {
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema = self
            .catalog
            .resolve(table, self.clock.current())
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        // Fold the introspection key exactly as `version_history` (and `bind_dml`)
        // do, so the business-key bytes match what was written.
        let key_ty = schema
            .columns()
            .first()
            .map_or(LogicalType::Int8, stele_catalog::ColumnDef::ty);
        let business_key = match key {
            Some(expr) => Some(business_key(
                &stele_sql::fold_literal(expr, key_ty).map_err(EngineError::IntrospectionKey)?,
            )),
            None => None,
        };
        let versions = state.engine.version_history(business_key.as_ref())?;

        // The durable commit log is the witness: read it back, map every committed
        // transaction to its record's (hash, prev_hash), and form the verdict.
        let payloads = commit_log::replay(&self.disk).map_err(EngineError::CommitLog)?;
        let mut by_txn: BTreeMap<u64, (Digest, Digest)> = BTreeMap::new();
        for payload in &payloads {
            let record = CommitRecord::decode(payload).map_err(|e| {
                EngineError::CommitLog(io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            })?;
            by_txn.insert(record.txn_id.0, (record.hash(), record.prev_hash));
        }
        // Anchor the verify against the in-memory head — the trusted witness from
        // this live session (or the verified-on-recovery tail) — so a rewrite of the
        // last record is caught too, not only an interior link break.
        let chain_ok = verify_chain_to(
            payloads.iter().cloned().map(Ok::<_, WalError>),
            self.commit_head,
        )
        .is_ok();
        let chain_len = i64::try_from(payloads.len()).unwrap_or(i64::MAX);
        let chain_head = self.commit_head.to_hex();

        let columns = vec![
            ("txid".to_owned(), LogicalType::Int8),
            ("op".to_owned(), LogicalType::Text),
            ("hash".to_owned(), LogicalType::Text),
            ("prev_hash".to_owned(), LogicalType::Text),
            ("chain_ok".to_owned(), LogicalType::Bool),
            ("chain_len".to_owned(), LogicalType::Int8),
            ("chain_head".to_owned(), LogicalType::Text),
        ];
        // The verdict cells ride on every row, so a renderer reads them off row 0
        // without a second reply.
        let verdict = |row: &mut Vec<Option<Vec<u8>>>| {
            row.push(Some(encode_value(&ScalarValue::Bool(chain_ok))));
            row.push(Some(encode_value(&ScalarValue::Int8(chain_len))));
            row.push(Some(encode_value(&ScalarValue::Text(chain_head.clone()))));
        };

        let mut rows = Vec::with_capacity(versions.len().max(1));
        let mut prev: Option<&Version> = None;
        for v in &versions {
            let op = version_op(prev, v);
            prev = Some(v);
            // A version's commit hash is its transaction's chain record. A version
            // with no record is one whose write was deliberately not chained — a
            // DROP era's bulk row closes ([STL-211]/[STL-220]), recovery-re-derivable
            // from the catalog drop record, not a data commit ([ADR-0031]). The
            // crash-window unchained commit (data durable, no record) is closed now
            // that every single-table/auto-commit leg is gated on its record
            // ([STL-314]). Its hash is NULL rather than a fabricated value.
            let (hash, prev_hash) =
                by_txn
                    .get(&v.provenance.txn_id.0)
                    .map_or((None, None), |(h, p)| {
                        (
                            Some(encode_value(&ScalarValue::Text(h.to_hex()))),
                            Some(encode_value(&ScalarValue::Text(p.to_hex()))),
                        )
                    });
            let mut row = vec![
                Some(encode_value(&ScalarValue::Int8(txid_as_i64(
                    v.provenance.txn_id,
                )))),
                Some(encode_value(&ScalarValue::Text(op.to_owned()))),
                hash,
                prev_hash,
            ];
            verdict(&mut row);
            rows.push(row);
        }
        // An empty timeline still reports the (global) chain verdict — one row whose
        // version cells are NULL.
        if rows.is_empty() {
            let mut row = vec![None, None, None, None];
            verdict(&mut row);
            rows.push(row);
        }
        Ok(SelectResult {
            columns,
            rows,
            // A synthetic catalog / history / audit result is not a table scan, so
            // it carries no scan accounting ([STL-201]).
            stats: None,
        })
    }

    /// Set the **write principal** stamped on every version subsequently committed
    /// through this session ([STL-300]).
    ///
    /// A fresh or recovered session defaults to the server identity `stele`, which
    /// direct, non-wire callers (engine and oracle tests) leave untouched. The
    /// pg-wire front end — where one engine is shared across connections behind a
    /// single mutex — calls this **under the same lock as the dispatch that
    /// follows**, so each statement stamps the connection's authenticated user (the
    /// unauthenticated startup `user` under `trust`, the SCRAM-verified user under
    /// `scram`, [STL-252]) even though connections interleave on the shared engine.
    /// It changes the stored provenance *value*, not the query surface ([STL-247]).
    ///
    /// # Panics (debug only)
    ///
    /// `_stele_principal` surfaces as SQL `TEXT` (read back through
    /// [`str::from_utf8`]), so the principal must be valid UTF-8. Every production
    /// caller supplies a `String` (the wire `user`), so this is a `debug_assert`
    /// guarding a misusing direct caller rather than a runtime cost — a non-UTF-8
    /// principal would otherwise read back as an internal decode error.
    ///
    /// [STL-247]: https://allegromusic.atlassian.net/browse/STL-247
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    /// [STL-300]: https://allegromusic.atlassian.net/browse/STL-300
    pub fn set_principal(&mut self, principal: Principal) {
        debug_assert!(
            std::str::from_utf8(principal.as_bytes()).is_ok(),
            "write principal must be valid UTF-8 (it reads back as SQL TEXT)",
        );
        self.write_principal = principal;
    }

    /// Resolve a `SET stele.{system,valid}_time = <expr>` value to a concrete
    /// instant ([STL-246]).
    ///
    /// Folds the expression exactly as a `FOR … AS OF <expr>` qualifier does — against
    /// the clock observed fresh, so `now()` is the live instant at the moment of the
    /// `SET` ([STL-227]) — and additionally accepts the Postgres special string
    /// `'now'` as an alias for `now()`. The session (wire) layer pins the result and
    /// replays it as an explicit `AS OF` on each subsequent read
    /// ([`stele_sql::apply_session_time`]), so a session-pinned read resolves byte-for-byte
    /// like the explicit form.
    ///
    /// # Errors
    ///
    /// [`EngineError::Select`] wrapping the binder's [`AsOfError`](stele_sql::AsOfError)
    /// when the value is not a supported instant expression.
    pub fn resolve_session_time(
        &self,
        value: &stele_sql::sqlparser::ast::Expr,
    ) -> Result<SystemTimeMicros, EngineError> {
        let now = self.clock.observe();
        // `'now'` (Postgres's special datetime input string) is an alias for now().
        if is_now_string(value) {
            return Ok(now);
        }
        resolve_as_of(value, now).map_err(|e| EngineError::Select(e.into()))
    }

    /// Execute one parsed [`Statement`] against the session.
    ///
    /// Routes by binding, in order: a `CREATE TABLE` / `DROP TABLE` applies to the
    /// catalog at the commit clock's current instant (and, for `CREATE`, stands up
    /// the table's tiers); a `SELECT` binds against the catalog at the read
    /// snapshot and runs a [`SnapshotScan`] over the table's tiers; an `INSERT` /
    /// `UPDATE` / `DELETE` binds through [`bind_dml`] and stages onto the table's
    /// tiers ([STL-147]). Anything else is [`EngineError::Unsupported`].
    ///
    /// [STL-147]: https://allegromusic.atlassian.net/browse/STL-147
    ///
    /// # Errors
    ///
    /// [`EngineError`] if binding, catalog application, the scan, or the write
    /// fails.
    pub fn execute(&mut self, stmt: &Statement) -> Result<StatementOutcome, EngineError> {
        // An auto-committed statement is its own snapshot: read at statement
        // time — the clock observed fresh, so `AS OF now()` arithmetic tracks
        // real time on an idle database ([STL-227]) — then write immediately.
        // (Snapshot isolation pins one snapshot for a whole multi-statement
        // transaction instead — see [`execute_in_txn`](Self::execute_in_txn).)
        // No write buffer to overlay: an auto-commit read sees only committed
        // state.
        let started = self.metrics.now_micros();
        let result = self.execute_at(stmt, self.clock.observe(), &[]);
        self.observe_statement(stmt, started, result.as_ref());
        result
    }

    /// Resolve a row-returning statement's `RowDescription` columns **without
    /// running it** — the statement-level `Describe` the extended-query protocol
    /// takes for a prepared `SELECT` ([STL-212]).
    ///
    /// A prepared statement is described *before* `Bind`, so its `$1 … $n`
    /// parameters have no values. But a `SELECT`'s output column shape is a
    /// function of its projection and the schema only — never of the `WHERE`
    /// filter or any parameter *value* — so the filter is stripped
    /// ([`without_filter`]) and the columns resolve straight from the schema, with
    /// no scan. Returns `Some(columns)` for a row-returning `SELECT`, or `None` for
    /// a statement that returns no rows (DDL / DML / admin / empty), which the wire
    /// front end answers with `NoData`.
    ///
    /// Binds at the current committed snapshot — the auto-commit / no-transaction
    /// case. Inside an open `BEGIN` block use [`describe_in_txn`](Self::describe_in_txn),
    /// which resolves at the transaction's pinned snapshot so the advertised shape
    /// matches the rows the portal `Execute` will return under snapshot isolation.
    ///
    /// # Errors
    ///
    /// A `SELECT` whose table or projected columns do not resolve at the snapshot
    /// surfaces the binder's [`SelectError`], the same error the read path would
    /// raise.
    ///
    /// [STL-212]: https://allegromusic.atlassian.net/browse/STL-212
    pub fn describe(
        &self,
        stmt: &Statement,
    ) -> Result<Option<Vec<(String, LogicalType)>>, EngineError> {
        // Observe the clock just as `execute` does, so a `Describe` of an
        // `AS OF now() - …` statement resolves at the same kind of instant the
        // `Execute` will ([STL-227]) — a frozen mark here could `BeforeHistory`
        // a statement the execution would accept.
        self.describe_at(self.clock.observe(), stmt)
    }

    /// As [`describe`](Self::describe), but resolving the row shape at an open
    /// transaction's **pinned snapshot** ([STL-175]) rather than the current
    /// committed one.
    ///
    /// A statement-level `Describe('S')` issued inside a `BEGIN` block must
    /// advertise the same columns the portal `Execute` will return, and that read
    /// runs at the transaction's pinned snapshot ([`execute_in_txn`](Self::execute_in_txn)).
    /// Resolving at `clock.current()` instead could disagree if a concurrent
    /// session committed a DDL after the snapshot was pinned (e.g. a `SELECT *`
    /// whose column set changed). Binding here at `txn.snapshot` keeps the
    /// description and the rows consistent.
    ///
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    pub fn describe_in_txn(
        &self,
        stmt: &Statement,
        txn: &SessionTransaction,
    ) -> Result<Option<Vec<(String, LogicalType)>>, EngineError> {
        self.describe_at(txn.snapshot, stmt)
    }

    /// The shared resolver behind [`describe`](Self::describe) and
    /// [`describe_in_txn`](Self::describe_in_txn): strip the `WHERE` filter and bind
    /// the row shape at `read_snapshot`, with no scan.
    fn describe_at(
        &self,
        read_snapshot: SystemTimeMicros,
        stmt: &Statement,
    ) -> Result<Option<Vec<(String, LogicalType)>>, EngineError> {
        let stripped = without_filter(stmt);
        let ctx = BindContext {
            snapshot: read_snapshot,
            catalog: &self.catalog,
        };
        match bind_select(&stripped, &ctx) {
            Ok(bound) => Ok(Some(self.output_columns(&bound)?)),
            // Not a SELECT (DDL / DML / admin / empty) ⇒ no row description.
            Err(SelectError::NotSelect) => Ok(None),
            Err(e) => Err(EngineError::Select(e)),
        }
    }

    /// Execute one statement inside an open multi-statement transaction, under
    /// **snapshot isolation** ([ADR-0008], [STL-175]).
    ///
    /// An `INSERT` / `UPDATE` / `DELETE` is **buffered** into `txn` (applied as a
    /// unit at [`commit`](Self::commit)), bound at the transaction's pinned
    /// snapshot. A `SELECT` runs immediately, with its reads resolved at that
    /// *same* pinned snapshot, so every statement in the block observes one
    /// consistent system-time snapshot even while other connections commit — with
    /// the transaction's own buffered writes overlaid on it (**read-your-own-writes**,
    /// [STL-203]): the buffer rides into the read path so a `SELECT` after a staged
    /// write reflects it, while no other connection sees it until `COMMIT`.
    ///
    /// **DDL inside a transaction** is the one exception. Transactional DDL is not
    /// yet modeled, so a `CREATE` / `DROP` inside a block takes effect at once
    /// (auto-commits) — and its catalog change *must* be visible to the rest of
    /// the block, or a `BEGIN; CREATE TABLE t …; INSERT INTO t …; COMMIT` could not
    /// resolve `t`. So after a committed DDL the pinned snapshot is **advanced** to
    /// the commit clock's current instant. This is the only point the
    /// single-snapshot guarantee yields, and only to the transaction's own
    /// committed DDL; a pure DML/`SELECT` transaction keeps one snapshot for life.
    ///
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    /// [STL-203]: https://allegromusic.atlassian.net/browse/STL-203
    ///
    /// # Errors
    ///
    /// As [`execute`](Self::execute); a malformed buffered DML rejects the
    /// statement and buffers nothing.
    pub fn execute_in_txn(
        &mut self,
        stmt: &Statement,
        txn: &mut SessionTransaction,
    ) -> Result<StatementOutcome, EngineError> {
        let started = self.metrics.now_micros();
        let result = self.execute_in_txn_inner(stmt, txn);
        self.observe_statement(stmt, started, result.as_ref());
        result
    }

    /// The unmetered body of [`execute_in_txn`](Self::execute_in_txn).
    fn execute_in_txn_inner(
        &mut self,
        stmt: &Statement,
        txn: &mut SessionTransaction,
    ) -> Result<StatementOutcome, EngineError> {
        // Under READ COMMITTED every statement reads a fresh snapshot ([STL-248]):
        // re-pin toward the present *before* binding or executing this statement, so
        // it observes every transaction committed since the block began (and binds
        // against the catalog as it stands now). REPEATABLE READ (the default) holds
        // the one `BEGIN`-pinned snapshot, so it is left untouched. The transaction's
        // own buffered writes still overlay either snapshot (read-your-own-writes,
        // [STL-203]) — re-pinning advances only the committed baseline, not the
        // buffer.
        if matches!(txn.isolation, IsolationLevel::ReadCommitted) {
            self.repin_snapshot(txn);
        }
        if let Some(summary) = self.stage_dml(stmt, txn)? {
            return Ok(StatementOutcome::Dml(summary));
        }
        // A `SELECT` overlays the transaction's own buffered writes on its pinned
        // snapshot (read-your-own-writes, [STL-203]); the buffer rides into
        // `execute_at` as the read overlay. A DDL ignores it (it auto-commits).
        let outcome = self.execute_at(stmt, txn.snapshot, &txn.writes)?;
        // A DDL inside the block auto-committed (see above); advance the pinned
        // snapshot past it so a later statement in the same block can resolve the
        // table it created/dropped.
        if matches!(outcome, StatementOutcome::Ddl { .. }) {
            self.repin_snapshot(txn);
        }
        Ok(outcome)
    }

    /// Re-pin an open transaction's read snapshot to the commit clock's current
    /// instant.
    ///
    /// Used after a DDL auto-commits inside a transaction block so the rest of the
    /// block resolves the table it created/dropped (the one relaxation of the
    /// single-snapshot guarantee — see [`execute_in_txn`](Self::execute_in_txn)).
    /// The wire front end calls this on its DDL path, which auto-commits a `CREATE`
    /// / `DROP` through [`execute`](Self::execute) rather than
    /// [`execute_in_txn`](Self::execute_in_txn); the in-process path advances the
    /// snapshot itself.
    pub fn repin_snapshot(&self, txn: &mut SessionTransaction) {
        // Re-pin at the observed instant, matching `begin` ([STL-227]).
        let snapshot = self.clock.observe();
        txn.snapshot = snapshot;
        // Keep the open-snapshot multiset in step with the advanced pin, so the
        // prune floor reflects where this transaction now reads ([STL-204]).
        txn.lease.repin(snapshot);
    }

    // -----------------------------------------------------------------------
    // COPY ... FROM STDIN bulk load ([STL-236])
    //
    // The wire half (the CopyData/CopyDone sub-protocol and the text/CSV lexing)
    // lives in `stele-pgwire`; the engine's job is to bind the target before the
    // data streams (so the wire layer can advertise it) and then apply the lexed
    // field rows. A COPY is just a bulk INSERT: every row folds (via the shared
    // text-field codec, [`bind_copy_rows`]) into the same per-row insert a
    // multi-row INSERT produces, and rides the same crash-atomic group commit, so
    // a parse failure or torn commit leaves zero rows ([STL-192]/[STL-216]).
    //
    // [STL-236]: https://allegromusic.atlassian.net/browse/STL-236
    // -----------------------------------------------------------------------

    /// Resolve a `COPY <table> FROM STDIN`'s shape before its data streams: the
    /// column count the wire layer advertises in `CopyInResponse` and the stream
    /// format it lexes the bytes with. Binds the target at the current committed
    /// snapshot, or — when a transaction is open — at its pinned snapshot, so the
    /// advertised shape matches the rows [`copy_apply`](Self::copy_apply) /
    /// [`copy_stage`](Self::copy_stage) will load.
    ///
    /// # Errors
    ///
    /// [`EngineError::Copy`] if the statement is not a supported `COPY ... FROM
    /// STDIN`, its table/columns do not resolve, or an option is malformed.
    pub fn copy_shape(
        &self,
        stmt: &Statement,
        txn: Option<&SessionTransaction>,
    ) -> Result<CopyShape, EngineError> {
        let snapshot = txn.map_or_else(|| self.clock.observe(), |t| t.snapshot);
        let ctx = BindContext {
            snapshot,
            catalog: &self.catalog,
        };
        Ok(bind_copy(stmt, &ctx)?.shape())
    }

    /// Apply a streamed `COPY ... FROM STDIN` as an **auto-commit** bulk load: bind
    /// the plan at the current snapshot, fold the field rows into per-row inserts,
    /// and apply them as **one crash-atomic group** — the same group commit a
    /// multi-row `INSERT` uses ([STL-192]/[STL-216]), so a parse failure on any row
    /// leaves **zero** rows and a torn commit recovers whole or not at all. Returns
    /// the loaded row count for the `COPY n` tag.
    ///
    /// # Errors
    ///
    /// [`EngineError::Copy`] if the plan does not bind or any row's fields do not
    /// fold (nothing is applied); otherwise the storage error of a failed append.
    pub fn copy_apply(
        &mut self,
        stmt: &Statement,
        rows: &[Vec<Option<String>>],
    ) -> Result<u64, EngineError> {
        let started = self.metrics.now_micros();
        let result = self.copy_apply_inner(stmt, rows);
        self.observe_copy(started, result.as_ref().map(|n| *n));
        result
    }

    /// The unmetered body of [`copy_apply`](Self::copy_apply).
    ///
    /// A load at or under [`BULK_COPY_CHUNK_ROWS`] folds and applies as one resident
    /// atomic group (the multi-row `INSERT` path, byte-for-byte as before); a larger
    /// load streams through the chunked bulk path ([`bulk_copy_apply`](Self::bulk_copy_apply),
    /// [STL-240]) so a million-row `COPY` is fsync-bounded and runs in bounded memory.
    fn copy_apply_inner(
        &mut self,
        stmt: &Statement,
        rows: &[Vec<Option<String>>],
    ) -> Result<u64, EngineError> {
        let snapshot = self.clock.observe();
        if rows.len() <= BULK_COPY_CHUNK_ROWS {
            let dml = self.bind_copy_insert(stmt, snapshot, rows)?;
            let n = match &dml {
                BoundDml::InsertRows { rows, .. } => rows.len() as u64,
                _ => unreachable!("bind_copy_insert always yields InsertRows"),
            };
            self.apply_insert_rows(dml)?;
            return Ok(n);
        }
        self.bulk_copy_apply(stmt, snapshot, rows)
    }

    /// Apply a large auto-commit `COPY` as a **chunked bulk load** ([STL-240]).
    ///
    /// Binds the plan once, flushes the target to isolate the delta, then streams the
    /// rows through the storage bulk-group path in [`BULK_COPY_CHUNK_ROWS`]-sized
    /// chunks: each chunk is bound + folded on its own (so only one chunk's rows and
    /// redos are resident), its inserts apply *spilling* (so the delta stays bounded),
    /// and it commits as one two-phase WAL record + fsync. Every chunk shares one
    /// `txn_id`; a single commit record vouches them all, so the load is **one commit**
    /// whose hash chain ticks once ([ADR-0031]) and a million-row load completes in
    /// bounded memory with O(chunks) fsyncs.
    ///
    /// Crash-atomic and abortable as one group: a crash mid-load leaves the chunk
    /// records inert (no commit record) and recovery discards them — zero rows; a
    /// failure on any row (a duplicate/dead-key conflict, schema drift) discards the
    /// whole load via [`abort_group`](stele_storage::engine::Engine::abort_group),
    /// which drops the spilled delta wholesale — sound because the pre-load flush left
    /// the tier holding only this load's rows. Either way the table is unchanged.
    ///
    /// [STL-240]: https://allegromusic.atlassian.net/browse/STL-240
    /// [ADR-0031]: https://allegromusic.atlassian.net/browse/STL-307
    fn bulk_copy_apply(
        &mut self,
        stmt: &Statement,
        snapshot: SystemTimeMicros,
        rows: &[Vec<Option<String>>],
    ) -> Result<u64, EngineError> {
        let plan = {
            let ctx = BindContext {
                snapshot,
                catalog: &self.catalog,
            };
            bind_copy(stmt, &ctx)?
        };
        let table = plan.table.clone();
        // Isolate the delta: seal any committed-but-unflushed rows into a segment so
        // the tier holds only this load's rows. The bulk path applies spilling and an
        // append-only spill file is not removable in place, so an aborted/crashed load
        // is rolled back by discarding the delta wholesale — exact only once the
        // pre-load rows are sealed elsewhere.
        self.table_mut(&table)?.engine.flush()?;
        let txn_id = TxnId(self.next_txn);
        self.next_txn += 1;
        let principal = self.write_principal.clone();
        self.table_mut(&table)?.engine.begin_bulk_group();
        match self.bulk_copy_chunks(&plan, rows, txn_id, &principal) {
            Ok(total) => {
                self.table_mut(&table)?.engine.end_bulk_group();
                // One commit record vouches every chunk record sharing `txn_id`: the
                // load commits all-or-none and the tamper-evident chain ticks once.
                self.record_commit(txn_id)?;
                self.prune_write_index();
                Ok(total)
            }
            Err(e) => {
                if let Ok(state) = self.table_mut(&table) {
                    state.engine.abort_group();
                }
                // Prune on the abort path too ([`apply_write_group`](Self::apply_write_group)
                // does the same): the failing chunk's already-applied rows were rolled
                // back, so their write-index entries can never conflict again — clear
                // them so an aborted load does not leave the conflict index growing
                // ([STL-204]). The committed chunks before it were already pruned per
                // chunk in `bulk_copy_chunks`.
                self.prune_write_index();
                Err(e)
            }
        }
    }

    /// Stream the bound `COPY` rows through the open bulk group in chunks: bind + fold
    /// each chunk, apply its inserts (buffered + spilling), then commit the chunk as
    /// one two-phase WAL record + fsync ([STL-240]). Returns the loaded row count;
    /// propagates the first error so [`bulk_copy_apply`](Self::bulk_copy_apply) aborts
    /// the whole load.
    fn bulk_copy_chunks(
        &mut self,
        plan: &BoundCopy,
        rows: &[Vec<Option<String>>],
        txn_id: TxnId,
        principal: &Principal,
    ) -> Result<u64, EngineError> {
        let mut total = 0u64;
        for chunk in rows.chunks(BULK_COPY_CHUNK_ROWS) {
            let bound = bind_copy_rows(plan, chunk)?;
            let chunk_dml = BoundDml::InsertRows {
                table: plan.table.clone(),
                schema_id: plan.schema_id,
                rows: bound,
            };
            let (writes, _summary) = expand_insert_rows(chunk_dml);
            for write in writes {
                self.apply_bound_dml(write, txn_id, principal)?;
            }
            self.table_mut(&plan.table)?
                .engine
                .commit_bulk_chunk(txn_id)?;
            total += chunk.len() as u64;
            // Bound the conflict-detection index across the load too, not just at the
            // end: with no concurrent reader pinning an older snapshot this clears the
            // chunk's keys back out ([STL-204]), so the write index stays O(chunk), not
            // O(rows). A reader with an older snapshot legitimately holds entries it
            // may still conflict against — those are kept, as they must be.
            self.prune_write_index();
        }
        Ok(total)
    }

    /// Stage a streamed `COPY ... FROM STDIN` into an open transaction's buffer:
    /// the rows ride the same per-row insert buffer a multi-row `INSERT` stages, so
    /// read-your-own-writes ([STL-203]) sees them and `COMMIT` applies the whole
    /// `COPY` as part of the transaction's atomic group. Returns the staged row
    /// count for the `COPY n` tag.
    ///
    /// # Errors
    ///
    /// [`EngineError::Copy`] if the plan does not bind or any row does not fold —
    /// nothing is staged (the statement errors, aborting the block).
    pub fn copy_stage(
        &self,
        stmt: &Statement,
        rows: &[Vec<Option<String>>],
        txn: &mut SessionTransaction,
    ) -> Result<u64, EngineError> {
        let dml = self.bind_copy_insert(stmt, txn.snapshot, rows)?;
        let (writes, summary) = expand_insert_rows(dml);
        txn.writes.extend(writes);
        match summary {
            DmlSummary::Insert(n) => Ok(n),
            _ => unreachable!("expand_insert_rows of InsertRows summarizes as Insert"),
        }
    }

    /// Bind a `COPY` plan at `snapshot` and fold its streamed rows into a
    /// [`BoundDml::InsertRows`] — the shared front half of [`copy_apply`] and
    /// [`copy_stage`].
    fn bind_copy_insert(
        &self,
        stmt: &Statement,
        snapshot: SystemTimeMicros,
        rows: &[Vec<Option<String>>],
    ) -> Result<BoundDml, EngineError> {
        let ctx = BindContext {
            snapshot,
            catalog: &self.catalog,
        };
        let plan = bind_copy(stmt, &ctx)?;
        let bound = bind_copy_rows(&plan, rows)?;
        Ok(BoundDml::InsertRows {
            table: plan.table,
            schema_id: plan.schema_id,
            rows: bound,
        })
    }

    /// Record a finished auto-commit `COPY` into the metric registry as a bulk
    /// `INSERT` ([STL-253]): the loaded rows into `rows_written`, the latency under
    /// the `INSERT` statement kind, or an error.
    fn observe_copy(&self, started_micros: u64, result: Result<u64, &EngineError>) {
        let m = &self.metrics;
        match result {
            Ok(n) => {
                m.rows_written.add(n);
                m.observe_statement(
                    StatementKind::Insert,
                    m.now_micros().saturating_sub(started_micros),
                );
            }
            Err(_) => m.statement_errors.inc(),
        }
    }

    /// The session's metric registry ([STL-253]) — the wire front end and the
    /// ops HTTP listener share (and render) this exact instance, so engine-side
    /// and wire-side series land on one page.
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    #[must_use]
    pub const fn metrics(&self) -> &SharedMetrics {
        &self.metrics
    }

    /// The stored SCRAM verifier for `user`, if one exists ([STL-252]) — what
    /// the pg-wire SASL exchange authenticates against. `None` is "unknown
    /// user"; the wire layer runs a doomed mock exchange on it so the refusal
    /// is indistinguishable from a wrong password.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    #[must_use]
    pub fn auth_verifier(&self, user: &str) -> Option<ScramVerifier> {
        self.users.get(user).cloned()
    }

    /// How many users the live user store holds ([STL-252]). The server reads
    /// this at boot to warn when `auth = "scram"` is configured with no users
    /// (every connection would be refused until the operator bootstraps one).
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    #[must_use]
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Record one finished statement into the registry ([STL-253]): the
    /// per-kind count, latency, rows in/out, and the error count. `started_micros`
    /// is the registry time-source reading taken before the statement ran (zero
    /// when no source is installed, keeping tests and the simulator
    /// deterministic).
    ///
    /// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
    fn observe_statement(
        &self,
        stmt: &Statement,
        started_micros: u64,
        result: Result<&StatementOutcome, &EngineError>,
    ) {
        let m = &self.metrics;
        match result {
            Ok(outcome) => {
                let kind = match outcome {
                    StatementOutcome::Rows(r) => {
                        m.rows_returned.add(r.rows.len() as u64);
                        StatementKind::Select
                    }
                    StatementOutcome::Dml(summary) => {
                        let (kind, n) = match summary {
                            DmlSummary::Insert(n) => (StatementKind::Insert, *n),
                            DmlSummary::Update(n) => (StatementKind::Update, *n),
                            DmlSummary::Delete(n) => (StatementKind::Delete, *n),
                            DmlSummary::Merge(n) => (StatementKind::Merge, *n),
                        };
                        m.rows_written.add(n);
                        kind
                    }
                    // CHECKPOINT / FLUSH report a DDL-shaped outcome; label them
                    // by the statement's admin body instead.
                    StatementOutcome::Ddl { .. } => {
                        if matches!(stmt.body, StatementBody::Admin(_)) {
                            StatementKind::Admin
                        } else {
                            StatementKind::Ddl
                        }
                    }
                };
                m.observe_statement(kind, m.now_micros().saturating_sub(started_micros));
            }
            Err(_) => m.statement_errors.inc(),
        }
    }

    /// The shared statement router, resolving **reads** — a `SELECT`, and the
    /// table/literal binding of an auto-committed DML — at `read_snapshot`. DDL
    /// always takes effect at the commit clock's next instant, independent of the
    /// read snapshot. Routes, in order: an admin command, then by binding DDL,
    /// then `SELECT`, then `INSERT` / `UPDATE` / `DELETE`.
    ///
    /// `overlay` is the transaction's buffered writes for **read-your-own-writes**
    /// ([STL-203], extended to valid-time tables by [STL-223]) — empty on the
    /// auto-commit path. A `SELECT` overlays them on its resolved rows unless it
    /// time-travels the **system** axis: a `FOR SYSTEM_TIME AS OF` qualifier —
    /// including `FOR SYSTEM_TIME AS OF now()`, which folds to the pinned snapshot —
    /// reads system history and must show only committed state, so the overlay is
    /// dropped for it. A `FOR VALID_TIME AS OF` qualifier does *not* drop it: it
    /// filters the valid axis of the *current* (uncommitted) system state, so the
    /// transaction's own writes still participate ([STL-223]).
    ///
    /// [STL-203]: https://allegromusic.atlassian.net/browse/STL-203
    /// [STL-223]: https://allegromusic.atlassian.net/browse/STL-223
    fn execute_at(
        &mut self,
        stmt: &Statement,
        read_snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<StatementOutcome, EngineError> {
        // A commit-log poison is session-fatal: a prior commit record failed to reach
        // disk *after* its data leg was durable, so the live state and what recovery
        // would reconstruct have diverged ([STL-314]). Refuse every further statement
        // — reads included, since a divergent write may be visible — until a restart
        // into `recover` resolves it. (The per-table WAL poison refuses lazily at the
        // next write; this is the more serious whole-session condition.)
        if self.commit_poisoned {
            return Err(EngineError::CommitLog(io::Error::other(
                "session poisoned by a failed commit-log append; restart to recover",
            )));
        }

        // Admin commands (CHECKPOINT / FLUSH / COMPACT / BACKUP) have no SQL body,
        // so they are routed before the binders, which all assume one ([STL-219]).
        // `clone` (not `Copy`) because `BACKUP` carries an owned path.
        if let StatementBody::Admin(cmd) = &stmt.body {
            return self.apply_admin(cmd.clone());
        }

        // `SET` / `RESET` is per-connection session state handled by the wire layer
        // before the engine ([STL-246]) — it never reaches the engine's routing.
        // Guard explicitly so a stray one fails loudly rather than falling through
        // to the generic "not routable" error.
        if let StatementBody::Session(_) = &stmt.body {
            return Err(EngineError::Unsupported(
                "SET / RESET is handled at the session layer, not the engine",
            ));
        }

        // Stele-native temporal introspection: `SELECT * FROM stele_history('t'[, key])`
        // is the wire surface the shell's `\history` / `\timeline` / `\lineage`
        // commands read ([STL-199]). Recognized structurally here — ahead of the
        // binders, which have no `stele_history` relation — and answered from the
        // version timeline as an ordinary row set, so the whole wire path (rows,
        // errors, the extended protocol) carries it unchanged. Introspection reads
        // committed state at the current instant, not the overlay/`read_snapshot`,
        // so it ignores both (a transaction's buffered writes do not appear).
        if let Some((table, key)) = stele_history_call(stmt) {
            return self
                .version_history(&table, key)
                .map(StatementOutcome::Rows);
        }

        // Stele-native segment introspection: `SELECT * FROM stele_segments('t')`
        // is the wire surface the shell's `\segments` command reads ([STL-301]),
        // mirroring `stele_history` above. Recognized structurally here, ahead of
        // the binders (which have no `stele_segments` relation), and answered from
        // the tier metadata as an ordinary row set — same committed-state, ignore-
        // the-overlay semantics as the history surface.
        if let Some(table) = stele_segments_call(stmt) {
            return self.segment_metadata(&table).map(StatementOutcome::Rows);
        }

        // Its audit sibling: `SELECT * FROM stele_audit('t'[, key])` is the wire
        // surface the shell's `\audit` reads — per-version commit-chain hashes plus
        // an intact/broken verdict over the durable hash-chained commit log
        // ([STL-302], [ADR-0031]), and the `hash ← prevHash` source for `\lineage`.
        // Same structural recognition and read-only semantics as `stele_history`.
        if let Some((table, key)) = stele_audit_call(stmt) {
            return self.audit_chain(&table, key).map(StatementOutcome::Rows);
        }

        // DDL first: `bind_ddl` cleanly rejects non-DDL with `NotDdl`, which we
        // treat as "try the next router".
        match bind_ddl(stmt) {
            Ok(ddl) => return self.apply_ddl(ddl),
            Err(BindError::NotDdl) => {}
            Err(e) => return Err(EngineError::Bind(e)),
        }

        // SELECT next, bound against the read snapshot. The bind context borrows
        // the catalog immutably; the read path is `&self`, so a hit can run before
        // the borrow ends, but DML below needs `&mut self`, so the borrow is scoped
        // and released first.
        {
            let ctx = BindContext {
                snapshot: read_snapshot,
                catalog: &self.catalog,
            };
            match bind_select(stmt, &ctx) {
                Ok(bound) => {
                    // Read-your-own-writes ([STL-203], [STL-223]): a current read in
                    // the transaction overlays its buffered writes. A `FOR SYSTEM_TIME
                    // AS OF` qualifier drops the overlay — it time-travels the system
                    // axis and must show only committed history, and the uncommitted
                    // buffer belongs to the current system state, not a past one. A
                    // `FOR VALID_TIME AS OF` qualifier keeps it: that filters the valid
                    // axis of the *current* (read-your-own-writes) system state, so the
                    // buffer still participates ([STL-223]). Gating on the qualifier's
                    // *dimension*, not on `bound.snapshot == read_snapshot`: `FOR
                    // SYSTEM_TIME AS OF now()` folds to the pinned snapshot, so snapshot
                    // equality would wrongly overlay a system time-travel read.
                    let system_time_travel = stmt
                        .temporal
                        .as_of
                        .iter()
                        .any(|a| a.dimension == TimeDimension::System);
                    let live: &[BoundDml] = if system_time_travel { &[] } else { overlay };
                    let mut outcome = self.run_select(&bound, live)?;
                    // Stamp the "see the engine" footer's time-travel flag here, the
                    // one place that sees the raw statement's `FOR SYSTEM_TIME AS OF`
                    // clause ([STL-201]); `run_select` left it `false`.
                    if system_time_travel
                        && let StatementOutcome::Rows(result) = &mut outcome
                        && let Some(stats) = &mut result.stats
                    {
                        stats.time_travel = true;
                    }
                    return Ok(outcome);
                }
                // Not a SELECT either ⇒ try the DML router below.
                Err(SelectError::NotSelect) => {}
                Err(e) => return Err(EngineError::Select(e)),
            }
        }

        // DML last. `bind_dml` resolves the table at the same snapshot and folds
        // the key/payload literals; `NotDml` means this is none of the routes.
        let bound = {
            let ctx = BindContext {
                snapshot: read_snapshot,
                catalog: &self.catalog,
            };
            match bind_dml(stmt, &ctx) {
                Ok(dml) => dml,
                Err(DmlError::NotDml) => {
                    return Err(EngineError::Unsupported(
                        "not a DDL, SELECT, or INSERT/UPDATE/DELETE statement",
                    ));
                }
                Err(e) => return Err(EngineError::Dml(e)),
            }
        };
        // A predicate-driven (or whole-table) UPDATE / DELETE takes the
        // scan-then-write plan ([STL-229]): enumerate the matching live keys at
        // the read snapshot, then apply the per-key writes as one atomic group.
        // (`overlay` is empty here — an in-transaction DML is intercepted by
        // `stage_dml` and never reaches this router — but threading it keeps the
        // expansion correct for any caller.)
        match bound {
            dml @ (BoundDml::UpdateScan { .. } | BoundDml::DeleteScan { .. }) => {
                self.apply_scan_dml(dml, read_snapshot, overlay)
            }
            // A MERGE expands the same way ([STL-230]): resolve each source row
            // against the live keys at the read snapshot, then apply the write
            // set as one atomic group.
            BoundDml::Merge(merge) => self.apply_merge(&merge, read_snapshot, overlay),
            // A multi-row INSERT ([STL-228]) fans out into one point INSERT per
            // row, applied as one atomic group — the same group-commit machinery,
            // so a failure on any row leaves zero rows. It needs no snapshot read:
            // the binder already folded every row.
            dml @ BoundDml::InsertRows { .. } => self.apply_insert_rows(dml),
            dml => self.apply_dml(dml),
        }
    }

    /// Apply a bound DDL statement, taking effect at the commit clock's next
    /// instant, durably record it in the catalog log, and reconcile the tier
    /// map.
    ///
    /// The ordering is the write-ahead discipline of [ADR-0028]:
    ///
    /// 1. For `CREATE TABLE`, the storage tier is stood up first — a backend
    ///    failure aborts before anything else, so the catalog never names a
    ///    table with no storage behind it.
    /// 2. The mutation is validated by applying it to a **copy** of the
    ///    catalog (DDL is rare and the catalog small, so the clone is noise).
    /// 3. The catalog-log record is appended and **fsynced** — the durability
    ///    point. On failure the statement errors with the live catalog
    ///    untouched, so the log and the session can never disagree. (A fresh
    ///    `CREATE`'s just-opened tier is left behind as empty, unreferenced
    ///    files — harmless: no record names its namespace, so recovery ignores
    ///    it and a later table opening on that slice starts from the same
    ///    empty state.)
    /// 4. Only then is the copy committed and the tier map updated.
    ///
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    fn apply_ddl(&mut self, ddl: DdlStatement) -> Result<StatementOutcome, EngineError> {
        let at = self.clock.now();
        match ddl {
            DdlStatement::CreateTable {
                name,
                columns,
                temporal,
            } => {
                let valid_time = temporal.valid_time_enabled();
                // A re-created name whose tier is still resident keeps it (and
                // its namespace), so history is never dropped — but only if the
                // valid-time policy is unchanged: the tier's writer bakes the
                // policy in, so reusing it under a different policy would
                // silently enforce the stale one (re-opening the tier with the
                // new policy is the deferred alternative).
                let (tier, namespace) = match self.tables.get(&name) {
                    Some(prev) if prev.valid_time != valid_time => {
                        return Err(EngineError::ValidTimePolicyChange { table: name });
                    }
                    Some(prev) => (None, prev.namespace),
                    None => {
                        let tier = self.open_tier(valid_time)?;
                        let namespace = tier.namespace;
                        (Some(tier), namespace)
                    }
                };
                let record = CatalogRecord::CreateTable {
                    at,
                    namespace,
                    name: name.clone(),
                    columns: columns.clone(),
                    temporal: temporal.clone(),
                };
                let mut staged = self.catalog.clone();
                let schema_id = staged.create_table(name.clone(), columns, temporal, at)?;
                self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
                    .map_err(EngineError::CatalogLog)?;
                self.catalog = staged;
                if let Some(tier) = tier {
                    self.tables.insert(name, tier);
                }
                Ok(StatementOutcome::Ddl {
                    tag: DdlOutcome::Created(schema_id).command_tag(),
                })
            }
            // A drop closes the catalog name (above) and then retires the dropped
            // era's still-open storage rows ([STL-211]). The `IF EXISTS` no-op
            // writes no record and touches no storage — nothing changed, so there
            // is nothing to recover.
            DdlStatement::DropTable { name, if_exists } => {
                let mut staged = self.catalog.clone();
                let outcome = match staged.drop_table(&name, at) {
                    Ok(id) => DdlOutcome::Dropped(id),
                    Err(CatalogError::UnknownTable(_)) if if_exists => DdlOutcome::DropNoOp,
                    Err(e) => return Err(EngineError::Catalog(e)),
                };
                if matches!(outcome, DdlOutcome::Dropped(_)) {
                    let record = CatalogRecord::DropTable {
                        at,
                        name: name.clone(),
                    };
                    self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
                        .map_err(EngineError::CatalogLog)?;
                    self.catalog = staged;
                    // The catalog drop cascaded the table's index *metadata*
                    // away ([STL-233]); discard the orphaned access structures
                    // with it. Derived state only — nothing durable to undo,
                    // and replay re-derives the same cascade from the drop
                    // record.
                    self.index_states
                        .retain(|index_name, _| self.catalog.index(index_name).is_some());
                    // The catalog name is now closed, but the tier stays resident
                    // (history survives, and a re-create reuses it). Close every
                    // row still system-live at the drop instant so the re-created
                    // name does not inherit them in a current read, and re-using
                    // one of their keys is not refused as a duplicate. Append-only
                    // closes ([ADR-0023]): an AS OF read inside the dropped era is
                    // unaffected. The closes the catalog log does not carry are
                    // replayed from the tier's own WAL.
                    //
                    // Ordering is deliberate: the fsynced catalog record above is
                    // the DROP's commit point ([ADR-0028]), so the storage half
                    // runs *after* it. If `close_all_open` then fails (an I/O
                    // fault) — or a crash lands in the window before these
                    // auto-commit closes reach the WAL — the DROP is already
                    // durably committed and only its row cleanup is outstanding,
                    // never a half-applied close on a table the catalog still
                    // shows as live. Recovery re-derives the cleanup from the
                    // durable drop record (`Engine::close_dropped_era`, [STL-220]),
                    // so the dropped era is retired, not leaked, across that window.
                    if let Some(state) = self.tables.get_mut(&name) {
                        let txn_id = TxnId(self.next_txn);
                        self.next_txn += 1;
                        let principal = self.write_principal.clone();
                        state
                            .engine
                            .close_all_open(Snapshot(at), txn_id, &principal)?;
                    }
                }
                Ok(StatementOutcome::Ddl {
                    tag: outcome.command_tag(),
                })
            }
            DdlStatement::CreateIndex {
                name,
                table,
                kind,
                columns,
            } => self.apply_create_index(name, table, kind, columns, at),
            DdlStatement::DropIndex { name, if_exists } => {
                self.apply_drop_index(&name, if_exists, at)
            }
            user @ (DdlStatement::CreateUser { .. }
            | DdlStatement::AlterUserPassword { .. }
            | DdlStatement::DropUser { .. }) => self.apply_user_ddl(user, at),
        }
    }

    /// Apply a `CREATE`/`ALTER`/`DROP USER` ([STL-252]) to the durable user
    /// store, following the same write-ahead discipline as the table arms of
    /// [`apply_ddl`](Self::apply_ddl): validate against the live store, fsync
    /// the catalog-log record — the durability point — then commit the
    /// in-memory map. The record carries the derived verifier, never the
    /// password.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    fn apply_user_ddl(
        &mut self,
        ddl: DdlStatement,
        at: SystemTimeMicros,
    ) -> Result<StatementOutcome, EngineError> {
        match ddl {
            DdlStatement::CreateUser { name, password } => {
                if self.users.contains_key(&name) {
                    return Err(EngineError::DuplicateUser(name));
                }
                let verifier = derive_verifier(&password)?;
                let record = CatalogRecord::CreateUser {
                    at,
                    name: name.clone(),
                    verifier: verifier.clone(),
                };
                self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
                    .map_err(EngineError::CatalogLog)?;
                self.users.insert(name, verifier);
                Ok(StatementOutcome::Ddl {
                    tag: DdlOutcome::CreatedUser.command_tag(),
                })
            }
            DdlStatement::AlterUserPassword { name, password } => {
                if !self.users.contains_key(&name) {
                    return Err(EngineError::UnknownUser(name));
                }
                // A rotation derives under a *fresh* salt — reusing the old one
                // would let a captured pre-rotation exchange confirm whether the
                // password changed.
                let verifier = derive_verifier(&password)?;
                let record = CatalogRecord::AlterUser {
                    at,
                    name: name.clone(),
                    verifier: verifier.clone(),
                };
                self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
                    .map_err(EngineError::CatalogLog)?;
                self.users.insert(name, verifier);
                Ok(StatementOutcome::Ddl {
                    tag: DdlOutcome::AlteredUser.command_tag(),
                })
            }
            // The `IF EXISTS` no-op writes no record — nothing changed, nothing
            // to recover (the same posture as `DROP TABLE IF EXISTS`).
            DdlStatement::DropUser { name, if_exists } => {
                if !self.users.contains_key(&name) {
                    if if_exists {
                        return Ok(StatementOutcome::Ddl {
                            tag: DdlOutcome::DropUserNoOp.command_tag(),
                        });
                    }
                    return Err(EngineError::UnknownUser(name));
                }
                let record = CatalogRecord::DropUser {
                    at,
                    name: name.clone(),
                };
                self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
                    .map_err(EngineError::CatalogLog)?;
                self.users.remove(&name);
                Ok(StatementOutcome::Ddl {
                    tag: DdlOutcome::DroppedUser.command_tag(),
                })
            }
            // The table/index arms route through `apply_ddl` itself.
            _ => unreachable!("apply_user_ddl is only called with user DDL"),
        }
    }

    /// Apply a `CREATE INDEX` ([STL-233]), following the same write-ahead
    /// discipline as the table arms of [`apply_ddl`](Self::apply_ddl)
    /// ([ADR-0028]), with the build standing in for the tier setup:
    ///
    /// 1. validate on a catalog copy,
    /// 2. build the access structure from the rows live at `at` — a scan
    ///    failure aborts with nothing acknowledged, and a *crash* here leaves
    ///    no record, so the DDL simply never happened (the rebuildable
    ///    mid-build state the ticket's DoD names),
    /// 3. fsync the log record — the durability point,
    /// 4. commit the copy and adopt the structure.
    ///
    /// After this instant the DML maintenance hook keeps the structure
    /// current, and recovery rebuilds it from the durable record.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    fn apply_create_index(
        &mut self,
        name: String,
        table: String,
        kind: IndexKind,
        columns: Vec<String>,
        at: SystemTimeMicros,
    ) -> Result<StatementOutcome, EngineError> {
        let def = IndexDef::new(name, table, kind, columns)?;
        let mut staged = self.catalog.clone();
        staged.create_index(def.clone())?;
        let state = self
            .tables
            .get(def.table())
            .ok_or_else(|| EngineError::UnknownTable(def.table().to_owned()))?;
        let schema = staged
            .resolve(def.table(), at)
            .ok_or_else(|| EngineError::UnknownTable(def.table().to_owned()))?;
        let built = Self::build_index_state(state, schema, &def, at, &self.metrics)?;
        let record = CatalogRecord::CreateIndex {
            at,
            name: def.name().to_owned(),
            table: def.table().to_owned(),
            kind: def.kind(),
            columns: def.columns().to_vec(),
        };
        self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
            .map_err(EngineError::CatalogLog)?;
        let name = def.name().to_owned();
        self.catalog = staged;
        self.index_states.insert(name, built);
        Ok(StatementOutcome::Ddl {
            tag: DdlOutcome::CreatedIndex.command_tag(),
        })
    }

    /// Apply a `DROP INDEX` ([STL-233]): record the drop durably, then discard
    /// the metadata and the access structure. The `IF EXISTS` no-op writes no
    /// record — nothing changed, nothing to recover.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    fn apply_drop_index(
        &mut self,
        name: &str,
        if_exists: bool,
        at: SystemTimeMicros,
    ) -> Result<StatementOutcome, EngineError> {
        let mut staged = self.catalog.clone();
        let outcome = match staged.drop_index(name) {
            Ok(_) => DdlOutcome::DroppedIndex,
            Err(CatalogError::UnknownIndex(_)) if if_exists => DdlOutcome::DropIndexNoOp,
            Err(e) => return Err(EngineError::Catalog(e)),
        };
        if matches!(outcome, DdlOutcome::DroppedIndex) {
            let record = CatalogRecord::DropIndex {
                at,
                name: name.to_owned(),
            };
            self.catalog_head = catalog_log::append(&self.disk, &record, self.catalog_head)
                .map_err(EngineError::CatalogLog)?;
            self.catalog = staged;
            self.index_states.remove(name);
        }
        Ok(StatementOutcome::Ddl {
            tag: outcome.command_tag(),
        })
    }

    /// Apply an operator-facing admin command ([STL-219]): drive the matching
    /// session-wide durability operation over every resident table, and report it
    /// with the command's `CommandComplete` tag.
    ///
    /// `CHECKPOINT` → [`checkpoint`](Self::checkpoint) (the lightweight WAL fence);
    /// `FLUSH` → [`flush`](Self::flush) (seal each delta into a segment + bound
    /// recovery); `COMPACT` → [`compact`](Self::compact) (flush, then merge each
    /// table's sealed segments into one, retiring the inputs — [STL-231]). The
    /// outcome reuses [`StatementOutcome::Ddl`] purely to carry the static tag
    /// the wire layer renders — no catalog change happens.
    ///
    /// [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
    /// [STL-231]: https://allegromusic.atlassian.net/browse/STL-231
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if a table's checkpoint, flush, or compaction
    /// fails; [`EngineError::Backup`] if a `BACKUP` cannot open or write its
    /// target directory.
    fn apply_admin(&mut self, cmd: AdminCommand) -> Result<StatementOutcome, EngineError> {
        let tag = match cmd {
            AdminCommand::Checkpoint => {
                self.checkpoint()?;
                "CHECKPOINT"
            }
            AdminCommand::Flush => {
                self.flush()?;
                "FLUSH"
            }
            AdminCommand::Compact => {
                self.compact()?;
                "COMPACT"
            }
            AdminCommand::Backup { path } => {
                // The target is a local filesystem directory (object-store targets
                // are v0.4 — [STL-249] scope); `LocalDisk::open` creates it if
                // absent. `backup` itself refuses a non-empty target. The wire
                // path always backs up to local disk regardless of the engine's
                // own backend, so this is the one place the generic engine names a
                // concrete backend.
                let target = stele_storage::backend::LocalDisk::open(&path)
                    .map_err(backup::BackupError::Io)?;
                self.backup(&target)?;
                "BACKUP"
            }
        };
        Ok(StatementOutcome::Ddl { tag })
    }

    /// Open a fresh storage tier on the next namespace, advancing the namespace
    /// counter only once the open succeeds.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the backend cannot open the tier's files.
    fn open_tier(&mut self, valid_time: bool) -> Result<TableState<C, D>, EngineError> {
        let namespace = self.next_namespace;
        let disk = NamespacedDisk::new(self.disk.clone(), namespace);
        let mut engine = Engine::open(disk, self.clock.clone(), valid_time)?;
        if let Some(rows) = self.flush_row_group_rows {
            engine = engine.with_flush_row_group_rows(rows);
        }
        engine.set_metrics(Arc::clone(&self.metrics));
        self.next_namespace += 1;
        Ok(TableState {
            engine,
            valid_time,
            namespace,
        })
    }

    /// The `(name, type)` output columns a bound `SELECT` produces, resolved
    /// **without scanning** — the `RowDescription` shape both the streaming read
    /// (`run_select`) and the statement-level `Describe` ([STL-212]) report.
    ///
    /// A `JOIN` and an aggregate plan carry their output columns directly (the
    /// binder computed them); a plain projection resolves them from the schema live
    /// at the bound snapshot. The shape is a function of the projection and schema
    /// only, so this never touches storage.
    ///
    /// [STL-212]: https://allegromusic.atlassian.net/browse/STL-212
    fn output_columns(
        &self,
        bound: &BoundSelect,
    ) -> Result<Vec<(String, LogicalType)>, EngineError> {
        if let Some(join) = &bound.join {
            return Ok(join.columns.clone());
        }
        if let Some(agg) = &bound.aggregate {
            return Ok(agg.columns.clone());
        }
        // A CTE reference / derived table ([STL-242]) carries its own resolved
        // columns; its shape is a function of the projection over those, with no
        // catalog lookup or provenance pseudo-columns.
        if let Some(columns) = &bound.relation_columns {
            let n_schema = columns.len();
            return Ok(projected_columns(&bound.projection, columns, n_schema));
        }
        let table = bound.table.as_str();
        let schema = self
            .catalog
            .resolve(table, bound.snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema_columns: Vec<(String, LogicalType)> = schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect();
        let n_schema = schema_columns.len();
        // A range scan ([STL-244] system axis, [STL-328] valid axis) appends the
        // period endpoints to the schema it projects over, then exposes the
        // provenance pseudo-columns past them ([STL-329]) — the same addressable set
        // the executed result shapes against, so `Describe` and the run agree on the
        // row shape (endpoints and a named provenance column included).
        if bound.range_endpoint_names().is_some() {
            let range_schema_columns = range_schema_columns(bound, &schema_columns);
            let range_addressable = addressable_columns(&range_schema_columns);
            return Ok(projected_columns(
                &bound.projection,
                &range_addressable,
                range_schema_columns.len(),
            ));
        }
        Ok(projected_columns(
            &bound.projection,
            &addressable_columns(&schema_columns),
            n_schema,
        ))
    }

    /// Run a snapshot scan for a bound `SELECT`, honoring its projection list and
    /// `WHERE` filter through the vectorized operator pipeline ([STL-151], [STL-206]).
    ///
    /// The scan materializes the `(business_key, payload)` pair into a source
    /// operator; [`ExplodePayload`] slices the packed payload back into the row's
    /// value columns ([row codec](stele_common::row_codec)) as first-class typed
    /// columns in schema order (the business key, then the value columns); the
    /// [`Filter`] operator evaluates the bound `WHERE <col> = <lit>` over each batch
    /// via `eval_expr`; and the projection selects exactly the requested columns. A
    /// key-equality predicate is additionally pushed down to the scan so its zone
    /// maps can prune; the same `Filter` re-applies it so the answer is exact
    /// regardless of what the prune could prove. A constant period predicate
    /// ([STL-165]) short-circuits to an empty result when false, before the scan;
    /// a per-row one ([STL-193]) is evaluated against each decoded row.
    ///
    /// The schema is resolved at the read snapshot, so an `AS OF` read names and
    /// types its columns under the schema version live then.
    ///
    /// A `GROUP BY` / aggregate query ([STL-171]) folds the same reconstructed,
    /// filtered rows into grouped output ([`run_aggregate`]); a plain query
    /// projects them.
    // Threading the scan accounting up for the query-stats footer ([STL-201])
    // pushed the row-production match a few lines past the limit; the control flow
    // is unchanged, so splitting it would scatter it rather than clarify it.
    #[allow(clippy::too_many_lines)]
    fn run_select(
        &self,
        bound: &BoundSelect,
        overlay: &[BoundDml],
    ) -> Result<StatementOutcome, EngineError> {
        self.run_select_scoped(bound, overlay, &CteScope::new())
    }

    /// As [`run_select`](Self::run_select), but with the enclosing
    /// common-table-expressions / derived tables in scope ([STL-242]).
    ///
    /// First materializes this query's own `WITH` list (and any lowered derived
    /// tables) once, in declaration order — each at the statement snapshot, over
    /// the same `overlay`, so a later CTE may read an earlier one and the one
    /// consistent per-statement `(sys, valid)` rule holds (docs/16 §6) — extending
    /// `parent_scope` into the scope the body resolves references against. A `FROM`
    /// that names a materialized relation ([`BoundSelect::relation_columns`]) reads
    /// its rows from that scope and then runs the very same `WHERE` / aggregate /
    /// projection pipeline a base-table read does; otherwise the base-table and
    /// join paths run as before.
    // The committed-fast-path / overlay / provenance row reconstruction reads as one
    // sequence (as it did on the pre-split `run_select`); the scope/CTE prelude only
    // adds to it, so splitting would scatter the read path rather than clarify it.
    #[allow(clippy::too_many_lines)]
    fn run_select_scoped(
        &self,
        bound: &BoundSelect,
        overlay: &[BoundDml],
        parent_scope: &CteScope,
    ) -> Result<StatementOutcome, EngineError> {
        // Materialize this query's CTEs / derived tables into the scope (extending
        // the inherited one) before anything references them. The common no-CTE
        // case borrows the parent untouched; an extended scope shares each relation
        // by `Arc`, so layering it costs no row copy.
        let mut owned_scope;
        let scope: &CteScope = if bound.ctes.is_empty() {
            parent_scope
        } else {
            owned_scope = parent_scope.clone();
            for cte in &bound.ctes {
                let relation = self.materialize_cte(cte, overlay, &owned_scope)?;
                owned_scope.insert(cte.name.clone(), Arc::new(relation));
            }
            &owned_scope
        };

        // A two-table `JOIN` ([STL-172]) takes a wholly different path: it scans
        // both sides and combines their rows, rather than projecting one table's
        // reconstructed rows. The single-table fields below are unused for it. Every
        // input reads at the one statement-level `(sys, valid)` pin ([STL-243],
        // docs/16 §8) — a `FOR … AS OF` on either axis threads through here. The
        // read-your-own-writes overlay ([STL-203], [STL-223]) rides into the join
        // too ([STL-325]): each side's scan is overlaid with the transaction's
        // buffered writes for that table before the hash join. The caller already
        // dropped the overlay for a system time-travel read (`execute_at` passes an
        // empty slice), so a `FOR SYSTEM_TIME AS OF` join sees committed history and
        // a `FOR VALID_TIME AS OF` join keeps the overlay.
        if bound.join.is_some() {
            // A range qualifier over the join is the "history of the joined result
            // over an interval" read ([STL-344]): the join plan *and* a system /
            // valid range are both bound, routing to the range-join path (each input
            // range-scanned, the per-input intervals intersected) rather than the
            // point-snapshot join below. `overlay` is not threaded to the base-table
            // inputs, so they read committed-only — read-your-own-writes over a range
            // *join* is not implemented yet, unlike the single-table range path, which
            // overlays buffered writes ([STL-343]). `scope` is threaded, so a CTE /
            // derived input ([STL-349]) is read from its materialization, which already
            // reflects the transaction's overlay ([STL-242]).
            if bound.system_range.is_some() || bound.valid_range.is_some() {
                return self.run_join_range(bound, scope);
            }
            return self.run_join(bound, overlay, scope);
        }

        // A `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` range scan
        // ([STL-244]) returns every version overlapping the interval — many per key
        // — with the period endpoints (`sys_from`, `sys_to`) appended after the
        // projected columns. It reconstructs versions differently from a point read,
        // then routes through the shared `finish_select` tail so result-shaping,
        // aggregation, and the provenance pseudo-columns compose over it ([STL-329]).
        // Inside a transaction the buffered writes overlay the committed version set
        // (read-your-own-writes, [STL-343]); a system time-travel read has already
        // dropped the overlay upstream ([`execute_at`](Self::execute_at)).
        if let Some(range) = bound.system_range {
            return self.run_system_range(bound, range, overlay, scope);
        }

        // A `FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` range scan ([STL-328])
        // is the valid-axis mirror: every version system-live at the snapshot whose
        // valid interval overlaps the range, with the period endpoints (`valid_from`,
        // `valid_to`) appended. Same shape, same shared `finish_select` tail.
        if let Some(range) = bound.valid_range {
            return self.run_valid_range(bound, range, overlay, scope);
        }

        // A `FROM` that names a materialized relation — a CTE reference or a derived
        // table ([STL-242]) — reads its rows from the scope, not from storage. The
        // relation carries no provenance and no valid axis, so its addressable set
        // is just its own columns; the `WHERE` is applied over the materialized rows
        // ([`filter_rows`]) and the shared tail ([`finish_select`]) runs the
        // correlated-subquery / aggregate / projection pipeline exactly as a base
        // table's would.
        if let Some(columns) = &bound.relation_columns {
            let relation: &MaterializedRelation = scope
                .get(bound.table.as_str())
                .ok_or_else(|| EngineError::UnknownTable(bound.table.clone()))?;
            let schema_columns = columns.clone();
            let plan = self.resolve_filter(bound, overlay, scope)?;
            // Filter over the relation's shared columns ([STL-321]): the `WHERE`
            // decodes only the columns it references, straight off those buffers, into
            // a selection vector of the surviving rows ([`relation_selection`]). The
            // selection rides into the shared tail as a [`RowSource::Relation`]
            // ([STL-338]) so shaping decodes only the columns a clause names and a
            // passthrough / projected read gathers only the projected output cells —
            // both straight off the shared columns by index. Nothing materializes the
            // full-width row-major intermediate the per-reference gather once built,
            // let alone the full copy `relation.rows.clone()` the original [STL-242]
            // read made.
            let selection = relation_selection(&plan, &schema_columns, relation)?;
            return self.finish_select(
                bound,
                &schema_columns,
                &schema_columns,
                RowSource::Relation {
                    relation,
                    selection,
                },
                None,
                overlay,
                scope,
            );
        }

        let table = bound.table.as_str();
        let snapshot = bound.snapshot;
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        // `bind_select` already proved the table resolves here, so a `None` would
        // be an internal contract break — surface it rather than panic.
        let schema = self
            .catalog
            .resolve(table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        // Column 0 is the business key; the rest are value columns packed into the
        // payload. `value_count` drives the codec's slicing.
        let schema_columns: Vec<(String, LogicalType)> = schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect();
        let value_count = schema_columns.len().saturating_sub(1);
        let n_schema = schema_columns.len();

        // The columns the projection / `WHERE` address by position: the table's own
        // columns, then the provenance pseudo-columns ([STL-247]) at the fixed
        // virtual layout after them. `SELECT *` spans only the first `n_schema`; a
        // pseudo-column is reachable only when named. When the query references one,
        // the read must materialize the version's provenance alongside its payload.
        let addressable = addressable_columns(&schema_columns);
        let needs_provenance = references_provenance(bound, &addressable, n_schema);

        // The valid-time period columns' positions in the schema (`(from, to)`, each
        // an index into `schema_columns` — and so into a reconstructed row, which is
        // key-then-values in the same order), used to pin the valid axis of overlaid
        // rows for a `FOR VALID_TIME AS OF` read ([STL-223]); `None` for a
        // system-only table.
        let valid_cols = schema.temporal().valid_time().and_then(|spec| {
            let idx = |name: &str| schema_columns.iter().position(|(n, _)| n == name);
            Some((idx(spec.from_column())?, idx(spec.to_column())?))
        });

        // Resolve the `WHERE` to a concrete row filter. For a plain or period
        // `WHERE` this is the syntactic [`filter_plan`]; for an uncorrelated
        // subquery `WHERE` ([STL-234]) it runs the inner query **once** at this
        // plan's snapshot and folds the result into the same [`FilterPlan`] shape
        // (a literal comparison, an equality-`OR` set test, or a constant
        // keep-all/keep-none) — so the scan and overlay paths below see one
        // resolved plan, identical to a non-subquery `WHERE`. The inner read sees
        // the outer's overlay too, so an in-transaction subquery is consistent
        // with read-your-own-writes ([STL-203]).
        let plan = self.resolve_filter(bound, overlay, scope)?;

        // Reconstruct the full rows [key, value cells…] live at the snapshot, after
        // the `WHERE` filter. Read-your-own-writes ([STL-203], [STL-223]): when this
        // read sits inside a transaction that has buffered writes for this table,
        // overlay their effect on the pinned-snapshot rows before filtering/projecting;
        // otherwise take the committed-only fused scan+filter fast path ([STL-206]). A
        // valid-time table is overlaid too — its writes supersede one version per
        // business key like a system-only table, and a `FOR VALID_TIME AS OF` pin is
        // re-applied to the overlaid rows ([STL-223]).
        // The scan's pruning accounting ([`ScanStats`], STL-146), threaded up into
        // the result for the "see the engine" footer ([STL-201]). Every branch now
        // reports its scan: the committed-only fast path below its fused scan; a
        // read-your-own-writes overlay and a provenance read their *base* scan — an
        // unfiltered scan (the `WHERE` is re-applied in the engine over the
        // overlaid / widened rows, so it gets none of the zone-map / bloom pruning a
        // pushed-down predicate would drive, though the validity index and any valid
        // pin still prune what they prove), so the footer reports that base scan's
        // real accounting ([STL-318]).
        let (rows, scan_stats) = if overlay.iter().any(|d| d.table() == table) {
            let (rows, stats) = Self::overlaid_rows(
                bound,
                state,
                &addressable,
                value_count,
                overlay,
                valid_cols,
                &plan,
                needs_provenance,
                &self.metrics,
            )?;
            (rows, Some(stats))
        } else if needs_provenance {
            // A provenance pseudo-column ([STL-247]) is referenced: materialize each
            // version's provenance after its value columns and filter over the
            // extended rows in the engine (the fused vectorized `Filter` addresses
            // only the table's own columns, so a `WHERE` on a pseudo-column — or a
            // mix — cannot ride it). Honors `AS OF` on either axis through the same
            // `SnapshotScan` as the fast path.
            let base = Self::scan_all_rows_with_provenance(
                state,
                bound.snapshot,
                bound.valid_snapshot,
                value_count,
                &self.metrics,
            )?;
            (
                filter_rows(&plan, &addressable, base.rows)?,
                Some(base.stats),
            )
        } else {
            // Rule-based index use ([STL-233], ranges [STL-237]): an equality
            // or one-sided range comparison on an indexed value column probes
            // the table's access structure for the candidate-key window.
            // `Empty` proves no visible row can match (the superset contract),
            // so the scan is skipped outright; a window prunes the scan to the
            // candidates' key range, and the exact `Filter` below keeps the
            // answer identical to a full scan either way — an index changes
            // speed, never results.
            match self.index_window(table, bound, &schema_columns) {
                // The index proved no key can match, so no scan ran; report the
                // all-zero accounting rather than suppressing the footer.
                Some(Probe::Empty) => (Vec::new(), Some(ScanStats::default())),
                Some(Probe::Window { low, high }) => {
                    let scanned = Self::scan_rows(
                        bound,
                        state,
                        &schema_columns,
                        value_count,
                        valid_cols,
                        Some(&(low, high)),
                        &plan,
                        &self.metrics,
                    )?;
                    (scanned.rows, Some(scanned.stats))
                }
                None => {
                    let scanned = Self::scan_rows(
                        bound,
                        state,
                        &schema_columns,
                        value_count,
                        valid_cols,
                        None,
                        &plan,
                        &self.metrics,
                    )?;
                    (scanned.rows, Some(scanned.stats))
                }
            }
        };

        self.finish_select(
            bound,
            &schema_columns,
            &addressable,
            RowSource::Rows(rows),
            scan_stats,
            overlay,
            scope,
        )
    }

    /// Materialize a CTE / derived table once ([STL-242]): run its defining plan at
    /// the statement snapshot, over the same `overlay` and `scope` (so it reads the
    /// transaction's own writes and any earlier sibling CTE), and capture its
    /// columns + rows.
    fn materialize_cte(
        &self,
        cte: &BoundCte,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<MaterializedRelation, EngineError> {
        let StatementOutcome::Rows(result) = self.run_select_scoped(&cte.plan, overlay, scope)?
        else {
            return Err(EngineError::Unsupported("a CTE body must be a SELECT"));
        };
        // Capture the result columnar (one shared buffer per column), so each later
        // reference reads the same buffers without re-cloning the rows ([STL-321]).
        let ncols = result.columns.len();
        Ok(MaterializedRelation::from_rows(result.rows, ncols))
    }

    /// The shared result-shaping tail of a single-relation read ([STL-242]): the
    /// correlated-subquery `WHERE` ([STL-239]), then either grouped aggregation
    /// ([STL-171]) or `DISTINCT` → `ORDER BY` → `OFFSET`/`LIMIT` projection
    /// ([STL-263]) — over the reconstructed `rows`, identically for a base table and
    /// a materialized CTE / derived table.
    ///
    /// `schema_columns` types the relation's own columns (for aggregation and the
    /// correlated decode); `addressable` is the projection/shaping addressable set —
    /// the schema columns plus provenance pseudo-columns for a base table, or just
    /// the schema columns for a materialized relation. `scan_stats` is the feeding
    /// scan's accounting (`None` for an overlay / provenance / materialized read,
    /// which suppresses the "see the engine" footer).
    // Eight inputs because the tail is shared by the base-table and CTE read paths,
    // which reconstruct rows differently (provenance addressable set, scan stats)
    // but converge here; threading them keeps the one shaping pipeline in one place.
    #[allow(clippy::too_many_arguments)]
    fn finish_select(
        &self,
        bound: &BoundSelect,
        schema_columns: &[(String, LogicalType)],
        addressable: &[(String, LogicalType)],
        rows: RowSource<'_>,
        scan_stats: Option<ScanStats>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<StatementOutcome, EngineError> {
        let n_schema = schema_columns.len();

        // A **correlated** subquery `WHERE` ([STL-239]) cannot fold to a constant
        // plan, so the scan above kept every row (`resolve_filter` → `KeepAll`); the
        // inner is re-run once per surviving outer row, with that row's value
        // substituted for the correlation reference, and the row is dropped unless
        // its predicate holds. This sits before the aggregate / projection so a
        // correlated `WHERE` filters rows *before* grouping, exactly as a plain one.
        let rows = match &bound.subquery_filter {
            Some(sub) if sub.correlation.is_some() => {
                RowSource::Rows(self.filter_correlated_subquery(
                    sub,
                    schema_columns,
                    addressable,
                    rows.into_rows(),
                    overlay,
                    scope,
                )?)
            }
            _ => rows,
        };

        // An aggregate query folds those rows into grouped output ([STL-171]); a
        // plain query shapes and projects them. Both paths end with the same
        // result-shaping pipeline ([STL-263]) — and because it runs over the
        // reconstructed `rows`, it applies identically under `AS OF` (either
        // axis) and over the read-your-own-writes overlay (ordering after
        // overlay, [STL-203]).
        if let Some(agg) = &bound.aggregate {
            let mut result = run_aggregate(bound, agg, schema_columns, &rows)?;
            // The footer reports the rows the aggregate *returned* (its grouped
            // output), over the scan that fed it ([STL-201]).
            result.stats = scan_stats.map(|s| query_stats(&s, result.rows.len(), bound.snapshot));
            return Ok(StatementOutcome::Rows(result));
        }

        let columns = projected_columns(&bound.projection, addressable, n_schema);
        let out_rows: Vec<Vec<Option<Vec<u8>>>> = if bound.projection.is_all_columns() {
            // Fast path: every item is a plain addressable column, projected by
            // gathering its cell — no per-row expression evaluation. For a CTE read
            // ([STL-338]) the cell is read straight off the relation's shared columns
            // by index, so only the projected output is materialized — never the
            // full-width row-major intermediate a per-reference gather once built.
            let projection = projection_indices(&bound.projection, addressable, n_schema);
            let selection = shape_rows(bound, addressable, &projection, &rows)?;
            selection
                .iter()
                .map(|&r| projection.iter().map(|&i| rows.cell(r, i)).collect())
                .collect()
        } else {
            // A computed expression or scalar subquery is projected ([STL-303]):
            // evaluate each item into a materialized column, append it as a virtual
            // column to every row, and shape / gather over the extended rows so
            // `DISTINCT` / `ORDER BY` / `LIMIT` apply identically to a column read.
            // Widening appends per-row cells, so this shape is inherently row-major;
            // it re-enters the tail through [`RowSource::Rows`].
            let MaterializedProjection {
                columns: ext_columns,
                rows: ext_rows,
                indices,
            } = self.materialize_projection(
                bound,
                addressable,
                schema_columns,
                rows,
                overlay,
                scope,
            )?;
            let ext = RowSource::Rows(ext_rows);
            let selection = shape_rows(bound, &ext_columns, &indices, &ext)?;
            selection
                .iter()
                .map(|&r| indices.iter().map(|&i| ext.cell(r, i)).collect())
                .collect()
        };
        let stats = scan_stats.map(|s| query_stats(&s, out_rows.len(), bound.snapshot));
        Ok(StatementOutcome::Rows(SelectResult {
            columns,
            rows: out_rows,
            stats,
        }))
    }

    /// Materialize a projection that contains a computed expression or scalar
    /// subquery ([STL-303]): evaluate each item into a column of canonical-encoded
    /// cells, append those as virtual columns to every reconstructed row, and return
    /// the extended column metadata, the extended rows, and the output-position
    /// indices into them — the shape [`shape_rows`] then sorts / deduplicates /
    /// limits exactly as it does a plain column projection.
    ///
    /// A bare column item keeps its addressable index; a computed item evaluates
    /// `eval_expr` over the row's typed columns ([`eval_projection_scalar`]); an
    /// uncorrelated scalar subquery resolves **once** at the statement snapshot (over
    /// the same `overlay` / `scope`, [`resolve_scalar_subquery`]) and broadcasts its
    /// single value, while a **correlated** one ([STL-331]) re-runs per outer row
    /// ([`materialize_correlated_subquery`]) — SQL `NULL` for an empty inner, SQLSTATE
    /// `21000` for `>1` row, either way.
    fn materialize_projection(
        &self,
        bound: &BoundSelect,
        addressable: &[(String, LogicalType)],
        schema_columns: &[(String, LogicalType)],
        rows: RowSource<'_>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<MaterializedProjection, EngineError> {
        let Projection::Items(items) = &bound.projection else {
            // `is_all_columns()` gates this path; `All` / a column-only list never
            // reaches here.
            return Err(EngineError::Unsupported(
                "materialize_projection requires a projection-item list",
            ));
        };
        // A computed projection widens every row with per-row virtual cells, so it
        // is inherently row-major; a CTE read materializes its surviving rows here
        // ([STL-338]) — only the column-only fast path stays gather-free.
        let rows = rows.into_rows();
        let row_count = rows.len();
        // The virtual columns start after the columns the rows already carry: the
        // schema columns, plus the provenance pseudo-columns when a read
        // materialized them (a provenance read widens every row). With no rows to
        // read the width from, fall back to the width a row *would* have had — the
        // full addressable set when this read referenced provenance, else just the
        // schema columns — so the appended virtual columns and `shape_rows`' type
        // decode stay in range even for an empty `DISTINCT` / `ORDER BY` result.
        let base_width = rows.first().map_or_else(
            || {
                if references_provenance(bound, addressable, schema_columns.len()) {
                    addressable.len()
                } else {
                    schema_columns.len()
                }
            },
            Vec::len,
        );
        let mut columns: Vec<(String, LogicalType)> =
            addressable.iter().take(base_width).cloned().collect();
        let mut computed: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut indices: Vec<usize> = Vec::with_capacity(items.len());
        for item in items {
            match &item.value {
                ProjectionValue::Column(source) => {
                    let idx = addressable.iter().position(|(n, _)| n == source).ok_or(
                        EngineError::Unsupported(
                            "a projected column is missing from the addressable set",
                        ),
                    )?;
                    indices.push(idx);
                }
                ProjectionValue::Computed { scalar, ty } => {
                    let column = self.eval_computed_projection(
                        scalar,
                        schema_columns,
                        &rows,
                        overlay,
                        scope,
                    )?;
                    indices.push(base_width + computed.len());
                    columns.push((item.name.clone(), *ty));
                    computed.push(column);
                }
                ProjectionValue::Subquery {
                    subquery,
                    ty,
                    correlation,
                } => {
                    // Uncorrelated: resolve once and broadcast the constant. Correlated
                    // ([STL-331]): re-run the inner per outer row — the [STL-239] per-row
                    // machinery producing a projected cell instead of a row keep/drop.
                    let column = match correlation {
                        None => {
                            let value = self.resolve_scalar_subquery(subquery, overlay, scope)?;
                            vec![value.as_ref().map(encode_value); row_count]
                        }
                        Some(correlation) => self.materialize_correlated_subquery(
                            subquery,
                            *correlation,
                            schema_columns,
                            &rows,
                            overlay,
                            scope,
                        )?,
                    };
                    indices.push(base_width + computed.len());
                    columns.push((item.name.clone(), *ty));
                    computed.push(column);
                }
            }
        }
        let rows: Vec<Vec<Option<Vec<u8>>>> = rows
            .into_iter()
            .enumerate()
            .map(|(r, mut row)| {
                for column in &computed {
                    row.push(column[r].clone());
                }
                row
            })
            .collect();
        Ok(MaterializedProjection {
            columns,
            rows,
            indices,
        })
    }

    /// Resolve a projected uncorrelated scalar subquery to its single value
    /// ([STL-303]): run the inner once at the statement snapshot (over the same
    /// `overlay` / `scope`, so it sees the outer's `(sys, valid)` state and any
    /// read-your-own-writes), then reduce its one output column to a value — SQL
    /// `NULL` for an empty inner, the lone value for one row, SQLSTATE `21000` for
    /// more than one ([`scalar_subquery_value`]).
    fn resolve_scalar_subquery(
        &self,
        subquery: &BoundSelect,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Option<ScalarValue>, EngineError> {
        let StatementOutcome::Rows(result) = self.run_select_scoped(subquery, overlay, scope)?
        else {
            return Err(EngineError::Unsupported("a subquery must be a SELECT"));
        };
        scalar_subquery_value(&result)
    }

    /// Evaluate a computed projection item ([STL-303], [STL-332]) into a column of
    /// canonical-encoded cells.
    ///
    /// Any embedded **uncorrelated** scalar subquery operand
    /// (`a + (SELECT max(b) FROM s)`) is resolved **once** at the statement snapshot
    /// ([`resolve_scalar_subquery`](Self::resolve_scalar_subquery)) and substituted as
    /// a constant, after which the arithmetic evaluates per row over the typed columns
    /// exactly as a column-only computed expression does. A subquery resolving to SQL
    /// `NULL` makes the whole arithmetic expression `NULL` for every row — NULL
    /// propagates through arithmetic (3VL), and the computed scalar vocabulary is
    /// arithmetic-only — so the column is all-`NULL` with no per-row evaluation. A
    /// `>1`-row inner still raises SQLSTATE `21000` (propagated from the resolve).
    fn eval_computed_projection(
        &self,
        scalar: &BoundScalar,
        schema_columns: &[(String, LogicalType)],
        rows: &[Vec<Option<Vec<u8>>>],
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        self.resolve_scalar_subqueries(scalar, overlay, scope)?
            .map_or_else(
                || Ok(vec![None; rows.len()]),
                |resolved| eval_projection_scalar(&resolved, schema_columns, rows),
            )
    }

    /// Resolve every embedded uncorrelated scalar subquery in a computed-projection
    /// scalar to a constant ([STL-332]), returning the scalar with each
    /// [`BoundScalar::Subquery`] replaced by the resolved [`BoundScalar::Literal`] —
    /// or `Ok(None)` if **any** subquery resolves to SQL `NULL`, the signal that the
    /// whole arithmetic expression is `NULL` for every row.
    ///
    /// Both operands of an arithmetic node are resolved eagerly (Postgres's
    /// uncorrelated-subquery / InitPlan posture), so a `>1`-row cardinality violation
    /// in either side surfaces even when the other resolved to `NULL`.
    fn resolve_scalar_subqueries(
        &self,
        scalar: &BoundScalar,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Option<BoundScalar>, EngineError> {
        Ok(match scalar {
            BoundScalar::Column(index) => Some(BoundScalar::Column(*index)),
            BoundScalar::Literal(value) => Some(BoundScalar::Literal(value.clone())),
            BoundScalar::Subquery(inner) => self
                .resolve_scalar_subquery(inner, overlay, scope)?
                .map(BoundScalar::Literal),
            BoundScalar::Arith { op, left, right } => {
                let left = self.resolve_scalar_subqueries(left, overlay, scope)?;
                let right = self.resolve_scalar_subqueries(right, overlay, scope)?;
                match (left, right) {
                    (Some(left), Some(right)) => Some(BoundScalar::Arith {
                        op: *op,
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                    _ => None,
                }
            }
        })
    }

    /// Materialize a **correlated** projected scalar subquery ([STL-331]) into a
    /// per-row column — the [STL-239] [`filter_correlated_subquery`] pattern, reduced
    /// to a projected cell per outer row instead of a row keep/drop.
    ///
    /// For each outer row this substitutes that row's correlation value into the
    /// inner's filter (`inner_column <op> value`, [`correlated_inner`]), re-runs the
    /// inner over the **same** `overlay` / `scope` / snapshot the outer reads (so the
    /// per-statement `(sys, valid)` and read-your-own-writes rules hold for every
    /// re-execution — docs/16 §6), and reduces the result with the same
    /// [`scalar_subquery_value`] the uncorrelated path uses: an empty inner ⇒ SQL
    /// `NULL`, one row ⇒ its value, more than one ⇒ SQLSTATE `21000`. A **NULL**
    /// correlation value short-circuits to a `NULL` cell without a run (`inner <op>
    /// NULL` is unknown for every inner row ⇒ empty ⇒ `NULL`), the [`empty_inner_keeps`]
    /// rule for a scalar.
    ///
    /// Performance is explicitly not the v0.3 bar (`O(outer rows × inner cost)`),
    /// mirroring the WHERE side; decorrelation is the STL-317 follow-up.
    fn materialize_correlated_subquery(
        &self,
        subquery: &BoundSelect,
        correlation: Correlation,
        schema_columns: &[(String, LogicalType)],
        rows: &[Vec<Option<Vec<u8>>>],
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        // Decode the outer correlation column once, exactly as the per-row `WHERE`
        // filter does ([`filter_correlated_subquery`]).
        let outer_ty = schema_columns[correlation.outer_column].1;
        let vector = key_vector(rows, correlation.outer_column, outer_ty)?;
        let mut cells = Vec::with_capacity(rows.len());
        for i in 0..rows.len() {
            let cell = match vector.get(i) {
                // A NULL correlation value makes the inner empty without a re-run.
                None => None,
                Some(value) => {
                    let inner = correlated_inner(subquery, correlation, value);
                    let StatementOutcome::Rows(result) =
                        self.run_select_scoped(&inner, overlay, scope)?
                    else {
                        return Err(EngineError::Unsupported("a subquery must be a SELECT"));
                    };
                    scalar_subquery_value(&result)?.as_ref().map(encode_value)
                }
            };
            cells.push(cell);
        }
        Ok(cells)
    }

    /// Run a `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` range scan
    /// ([STL-244]): return **every** version whose system interval
    /// `[sys_from, sys_to)` overlaps the range, with the period endpoints
    /// (`sys_from`, `sys_to`, both `TIMESTAMPTZ`) appended after the projected
    /// columns. `sys_to` is `NULL` for a still-current (open) version.
    ///
    /// The version selection is the executor's interval mode
    /// ([`SnapshotScan::execute_range`]); the engine reconstructs each version's row
    /// from its bare payload (the row codec), materializes provenance after the
    /// endpoints when the query references it ([STL-247]), applies the bound `WHERE`,
    /// then routes the rows through the shared [`finish_select`](Self::finish_select)
    /// tail — so result-shaping ([STL-263]), aggregation ([STL-171]), and the
    /// provenance pseudo-columns compose over the range output exactly as over a
    /// point read ([STL-329]). The endpoints are *addressable* columns there: the
    /// binder bound the projection / `ORDER BY` / `GROUP BY` against a schema that
    /// includes them ([`range_schema_columns`]), so they line up positionally.
    ///
    /// A key-equality `WHERE` is pushed down to the scan for zone-map pruning; the
    /// full predicate is re-applied over the reconstructed rows.
    ///
    /// Read-your-own-writes ([STL-343]): inside a transaction with buffered writes
    /// for this table, the buffer is spliced into the committed version set
    /// ([`overlay_system_range_rows`]) before the tail. A buffered write is observed
    /// at the pinned snapshot `now` (what `now()` folds to): it opens a `[now, +∞)`
    /// version and closes the prior live one at `now`, so whether the new version
    /// appears turns on the range's upper bound — a `BETWEEN … AND now()` admits it,
    /// a half-open `FROM … TO now()` does not. A `FOR SYSTEM_TIME AS OF` system
    /// time-travel read drops the overlay upstream ([`execute_at`](Self::execute_at)),
    /// so only the genuinely-current range reads here ever overlay.
    fn run_system_range(
        &self,
        bound: &BoundSelect,
        range: SystemTimeRange,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<StatementOutcome, EngineError> {
        let table = bound.table.as_str();
        let snapshot = bound.snapshot;
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema = self
            .catalog
            .resolve(table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema_columns: Vec<(String, LogicalType)> = schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect();
        let value_count = schema_columns.len().saturating_sub(1);

        // The addressable set the projection / shaping / aggregate clauses bound
        // against: the table's own columns, then the two appended endpoints
        // (`range_schema_columns`), then the provenance pseudo-columns. A read
        // referencing provenance materializes it after the endpoints — the fixed
        // virtual layout the binder and `references_provenance` agree on ([STL-329]).
        let range_schema_columns = range_schema_columns(bound, &schema_columns);
        let range_addressable = addressable_columns(&range_schema_columns);
        let needs_provenance =
            references_provenance(bound, &range_addressable, range_schema_columns.len());

        // Push a business-key equality down to the scan for zone-map pruning when the
        // `WHERE` pins one; any richer predicate lives in the payload and is applied
        // by the row filter below (re-applied exactly, so the prune need not be
        // tight). Declare the valid-time policy so a valid-time table's framed delta
        // payload is stripped to the bare row before the codec decodes it ([STL-218]).
        let predicate = bound
            .filter
            .as_ref()
            .and_then(BoundPredicate::key_equality)
            .map_or(Predicate::All, |value| Predicate::Eq {
                column: ColumnId::BusinessKey,
                value: ZoneBound::Bytes(encode_value(value)),
            });
        // Project provenance alongside the payload when the query references it, so
        // each resolved version carries its writing provenance ([STL-247]).
        let mut project = vec![ColumnId::BusinessKey, ColumnId::Payload];
        if needs_provenance {
            project.extend([ColumnId::TxnId, ColumnId::CommittedAt, ColumnId::Principal]);
        }
        let readers = state.engine.open_segment_readers()?;
        let scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        .project(project)
        .filter(predicate)
        .valid_time(state.valid_time)
        .system_range(range.from.0, range.to.0, range.closed_upper)
        .metrics(Arc::clone(&self.metrics));
        let (versions, stats) = scan.execute_range()?;

        // Read-your-own-writes ([STL-343]): a range read inside a transaction that
        // has buffered writes for this table observes them, spliced into the
        // committed version set by [`overlay_system_range_rows`] — a buffered write
        // opens a `[now, +∞)` version (now = the pinned snapshot, what `now()` folds
        // to) and closes the prior live one at `now`. The buffer's live values come
        // from the same unfiltered point scan + fold the single-table overlay uses
        // ([`overlaid_rows`]); `buffered` is the set of keys it touched (closing each
        // one's open version), `buffer_live` its surviving rows (the new versions).
        // With no buffered write for the table both are empty and the helper renders
        // the committed versions unchanged.
        let exec_range = SystemRange {
            lo: range.from.0,
            hi: range.to.0,
            closed_upper: range.closed_upper,
        };
        let (buffered, buffer_live) = if overlay.iter().any(|d| d.table() == table) {
            let buffered: BTreeSet<Vec<u8>> = overlay
                .iter()
                .filter(|d| d.table() == table)
                .filter_map(buffered_key_bytes)
                .collect();
            let base = Self::scan_all_rows(state, snapshot, value_count, &self.metrics)?;
            // Only the buffer's own keys ever open a new `[now, +∞)` version, so keep
            // just those from the merged live state — the rest of the table's live rows
            // are never looked up and would only bloat the map.
            let buffer_live: BTreeMap<Vec<u8>, Vec<Option<Vec<u8>>>> =
                overlay_table_writes(base.rows, overlay, table, value_count, false)
                    .into_iter()
                    .filter_map(|r| r.first().cloned().flatten().map(|k| (k, r)))
                    .filter(|(k, _)| buffered.contains(k))
                    .collect();
            (buffered, buffer_live)
        } else {
            (BTreeSet::new(), BTreeMap::new())
        };
        let rows = overlay_system_range_rows(
            &versions,
            &buffered,
            &buffer_live,
            exec_range,
            snapshot.0,
            value_count,
            needs_provenance,
            range_addressable.len(),
        )?;

        // Apply the bound `WHERE` over the reconstructed rows. A subquery `WHERE`
        // is rejected over a range, so the syntactic [`filter_plan`] is exact — and
        // it now also carries a period-predicate `WHERE` ([STL-345]): a constant
        // predicate folds to keep-all / keep-none, a per-row `PERIOD(col, …)` to an
        // `Expr::Period` the row filter evaluates. The endpoints and any
        // materialized provenance are addressable to it, so it filters over the full
        // `range_addressable` set; a period predicate's value-column endpoints
        // address the same indices in the reconstructed row (the endpoints are
        // appended *after* the value columns) that the binder bound them to.
        let plan = filter_plan(bound);
        let rows = filter_rows(&plan, &range_addressable, rows)?;

        // Route through the shared shaping / aggregate / projection tail, with the
        // endpoints as the trailing schema columns and provenance past them.
        self.finish_select(
            bound,
            &range_schema_columns,
            &range_addressable,
            RowSource::Rows(rows),
            Some(stats),
            overlay,
            scope,
        )
    }

    /// Run a `FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` range scan
    /// ([STL-328]) — the valid-axis mirror of [`run_system_range`](Self::run_system_range).
    /// Return every version **system-live at the statement snapshot** whose valid
    /// interval `[valid_from, valid_to)` overlaps the range, with the period
    /// endpoints (`valid_from`, `valid_to`, both `TIMESTAMPTZ`) appended after the
    /// projected columns. `valid_to` is `NULL` for an open-ended ("until changed")
    /// fact, mirroring how `sys_to` renders for a still-current version.
    ///
    /// The version selection is the executor's valid-interval mode
    /// ([`SnapshotScan::execute_valid_range`]), which surfaces each version's valid
    /// interval alongside the bare user row (read from the delta frame or the sealed
    /// `valid_from` / `valid_to` columns); the engine reconstructs the row from that
    /// bare payload, materializes provenance after the endpoints when referenced
    /// ([STL-247]), applies the bound `WHERE`, then routes through the shared
    /// [`finish_select`](Self::finish_select) tail so result-shaping, aggregation,
    /// and the provenance pseudo-columns compose over the range output ([STL-329]) —
    /// exactly as [`run_system_range`](Self::run_system_range) does on the system axis.
    ///
    /// A key-equality `WHERE` is pushed down for zone-map pruning; the full predicate
    /// is re-applied over the reconstructed rows.
    ///
    /// Read-your-own-writes ([STL-343]): inside a transaction with buffered writes for
    /// this table, the overlay replaces the committed-scan base — the unfiltered
    /// system-live point scan + buffer fold the single-table overlay uses
    /// ([`overlaid_rows`](Self::overlaid_rows)), each row's `[valid_from, valid_to)`
    /// then re-filtered against the range ([`overlay_valid_range_rows`]). A
    /// retroactive or post-dated buffered write can move a key's valid window into or
    /// out of the queried range, so the filter runs *after* the overlay, building on
    /// the [STL-223] overlay + post-overlay re-filter. A `FOR SYSTEM_TIME AS OF` pin
    /// drops the overlay upstream ([`execute_at`](Self::execute_at)).
    fn run_valid_range(
        &self,
        bound: &BoundSelect,
        range: ValidTimeRange,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<StatementOutcome, EngineError> {
        let table = bound.table.as_str();
        let snapshot = bound.snapshot;
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema = self
            .catalog
            .resolve(table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let schema_columns: Vec<(String, LogicalType)> = schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect();
        let value_count = schema_columns.len().saturating_sub(1);

        // The endpoints and provenance pseudo-columns the rest of the SELECT surface
        // binds against, as in [`run_system_range`](Self::run_system_range).
        let range_schema_columns = range_schema_columns(bound, &schema_columns);
        let range_addressable = addressable_columns(&range_schema_columns);
        let needs_provenance =
            references_provenance(bound, &range_addressable, range_schema_columns.len());

        let exec_range = ValidRange {
            lo: range.from.0,
            hi: range.to.0,
            closed_upper: range.closed_upper,
        };
        let (rows, stats) = if overlay.iter().any(|d| d.table() == table) {
            // Read-your-own-writes ([STL-343]): a valid-time write supersedes one
            // system-live version per key ([STL-223]), so the overlay's row set is the
            // point-read base — the unfiltered system-live scan (provenance-bearing
            // when referenced) with the buffer folded on — re-filtered on each row's
            // own `[valid_from, valid_to)` cells against the range. The period columns
            // ride the row codec ([STL-194]), so the bounds are read straight off
            // them; a buffered write's provenance is `NULL` ([`overlay_table_writes`]).
            let (from_idx, to_idx) = valid_period_indices(schema, &schema_columns)
                .ok_or(EngineError::MalformedValidBound)?;
            let base = if needs_provenance {
                Self::scan_all_rows_with_provenance(
                    state,
                    snapshot,
                    None,
                    value_count,
                    &self.metrics,
                )?
            } else {
                Self::scan_all_rows(state, snapshot, value_count, &self.metrics)?
            };
            let overlaid =
                overlay_table_writes(base.rows, overlay, table, value_count, needs_provenance);
            let rows = overlay_valid_range_rows(
                overlaid,
                exec_range,
                from_idx,
                to_idx,
                value_count,
                needs_provenance,
                range_addressable.len(),
            )?;
            (rows, base.stats)
        } else {
            // Committed-only fast path. Push a business-key equality down for zone-map
            // pruning when the `WHERE` pins one; the row filter below re-applies the
            // full predicate. The [`valid_range`](SnapshotScan::valid_range) builder
            // declares the valid-time policy itself (a valid range is always over a
            // valid-time table — the binder proved it), so the delta tier's framed
            // payload is stripped to the bare row.
            let predicate = bound
                .filter
                .as_ref()
                .and_then(BoundPredicate::key_equality)
                .map_or(Predicate::All, |value| Predicate::Eq {
                    column: ColumnId::BusinessKey,
                    value: ZoneBound::Bytes(encode_value(value)),
                });
            let mut project = vec![ColumnId::BusinessKey, ColumnId::Payload];
            if needs_provenance {
                project.extend([ColumnId::TxnId, ColumnId::CommittedAt, ColumnId::Principal]);
            }
            let readers = state.engine.open_segment_readers()?;
            let scan = SnapshotScan::new(
                state.engine.delta(),
                state.engine.index(),
                &readers,
                Snapshot(snapshot),
            )
            .project(project)
            .filter(predicate)
            .valid_range(range.from.0, range.to.0, range.closed_upper)
            .metrics(Arc::clone(&self.metrics));
            let (versions, stats) = scan.execute_valid_range()?;
            // Reconstruct each resolved version's row `[key, value cells…, valid_from,
            // valid_to, (provenance…)]` — the committed counterpart of the overlay's
            // [`overlay_valid_range_rows`].
            let rows = render_valid_range_rows(
                &versions,
                value_count,
                needs_provenance,
                range_addressable.len(),
            )?;
            (rows, stats)
        };

        // Apply the bound `WHERE` over the reconstructed rows (a subquery `WHERE` is
        // rejected over a range; a period-predicate `WHERE` composes — [STL-345]),
        // then route through the shared shaping / aggregate / projection tail with
        // the endpoints as the trailing schema columns. A per-row `PERIOD(vf, vt)`
        // predicate reads the user's valid-time value columns at their schema
        // indices, which the row reconstruction preserves ahead of the appended
        // `valid_from` / `valid_to` endpoints.
        let plan = filter_plan(bound);
        let rows = filter_rows(&plan, &range_addressable, rows)?;

        self.finish_select(
            bound,
            &range_schema_columns,
            &range_addressable,
            RowSource::Rows(rows),
            Some(stats),
            overlay,
            scope,
        )
    }

    /// Resolve a bound `SELECT`'s `WHERE` to a concrete [`FilterPlan`].
    ///
    /// A plain or period `WHERE` (no subquery) is the syntactic [`filter_plan`]
    /// unchanged. An **uncorrelated subquery** `WHERE` ([STL-234]) runs its inner
    /// query **once** — at *this* plan's snapshot and over the same `overlay`, so
    /// it reads the outer's `(sys, valid)` state and any in-transaction buffered
    /// writes ([read-your-own-writes](SessionEngine::run_select), docs/16 §6) —
    /// and folds the materialized result into the same `FilterPlan` the plain
    /// path produces:
    ///
    /// * a **scalar** subquery becomes `<column> <op> <literal>` (or
    ///   [`Empty`](FilterPlan::Empty) when it yields `NULL` / no row; SQLSTATE
    ///   `21000` when it yields more than one row);
    /// * an **`IN`** subquery becomes an equality-`OR` set test (three-valued —
    ///   see [`in_subquery_plan`]);
    /// * an **`EXISTS`** subquery becomes a constant
    ///   [`KeepAll`](FilterPlan::KeepAll) / [`Empty`](FilterPlan::Empty), since
    ///   the test is one value for the whole scan.
    ///
    /// A **correlated** subquery ([STL-239]) cannot fold to a constant plan — the
    /// inner depends on each outer row — so this keeps every outer row
    /// ([`KeepAll`](FilterPlan::KeepAll)); the per-row re-execution filter
    /// ([`filter_correlated_subquery`](SessionEngine::filter_correlated_subquery))
    /// decides each row after the scan.
    fn resolve_filter(
        &self,
        bound: &BoundSelect,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<FilterPlan, EngineError> {
        let Some(sub) = &bound.subquery_filter else {
            return Ok(filter_plan(bound));
        };
        if sub.correlation.is_some() {
            return Ok(FilterPlan::KeepAll);
        }
        // The inner is itself a bound `SELECT`, so it always returns rows; it runs
        // under the same CTE scope, so it may reference an enclosing CTE.
        let StatementOutcome::Rows(result) =
            self.run_select_scoped(&sub.subquery, overlay, scope)?
        else {
            return Err(EngineError::Unsupported("a subquery must be a SELECT"));
        };
        fold_subquery(sub.kind, &result)
    }

    /// Filter the reconstructed outer `rows` by a **correlated** subquery `WHERE`
    /// ([STL-239]) — the per-row re-execution path.
    ///
    /// The inner references an outer column, so it is not constant over the outer
    /// rows and cannot fold to one [`FilterPlan`]. Instead, for each outer row this
    /// substitutes that row's correlation value into the inner's filter
    /// (`inner_column <op> <value>`), re-runs the inner over the **same** `overlay`
    /// and snapshot the outer reads (so the per-statement `(sys, valid)` and
    /// read-your-own-writes rules hold for every re-execution — docs/16 §6), folds
    /// the result with the same [`fold_subquery`] the uncorrelated path uses, and
    /// evaluates that one-row plan with [`filter_rows`]. A **NULL** correlation value
    /// makes `inner <op> NULL` unknown for every inner row, so the inner result is
    /// empty without a run ([`empty_inner_keeps`]).
    ///
    /// Performance is explicitly not the v0.3 bar: this is `O(outer rows × inner
    /// cost)`. Decorrelating the common `EXISTS`/`IN` cases onto a semi/anti join is
    /// a tracked follow-up.
    fn filter_correlated_subquery(
        &self,
        sub: &BoundSubqueryFilter,
        schema_columns: &[(String, LogicalType)],
        addressable: &[(String, LogicalType)],
        rows: Vec<Vec<Option<Vec<u8>>>>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
        let correlation = sub
            .correlation
            .expect("filter_correlated_subquery called on an uncorrelated subquery");

        // A correlated `EXISTS` / `NOT EXISTS` on an equality key decorrelates onto
        // a single set-based semi / anti hash join ([STL-317]) — one inner scan
        // instead of `O(outer rows)` re-executions — when the binder recognizes the
        // shape.
        if let Some(plan) = sub.semi_anti_decorrelation() {
            return self.filter_semi_anti_decorrelated(
                plan,
                sub,
                schema_columns,
                rows,
                overlay,
                scope,
            );
        }
        // A correlated non-negated `IN` on an equality key decorrelates onto a single
        // **composite-key** semi join ([STL-337]) — the same one-scan win for the
        // two-key shape `EXISTS` could not fold.
        if let Some(plan) = sub.composite_semi_decorrelation() {
            return self.filter_composite_semi_decorrelated(
                plan,
                sub,
                schema_columns,
                rows,
                overlay,
                scope,
            );
        }
        // A correlated `NOT IN` on an equality key decorrelates onto a NULL-aware
        // composite **anti** join ([STL-346]) — the same composite-key plan, run as an
        // anti join with per-correlation-group NULL tracking layered on for the 3VL
        // trap a plain anti join cannot express. Every remaining correlation (a range
        // comparison, a scalar lookup, or a non-plain inner) keeps the per-row
        // fallback below.
        if let Some(plan) = sub.composite_anti_decorrelation() {
            return self.filter_composite_anti_decorrelated(
                plan,
                sub,
                schema_columns,
                rows,
                overlay,
                scope,
            );
        }

        // Decode the outer correlation column once, the same way the uncorrelated
        // fold decodes an inner column ([`subquery_column_values`]).
        let outer_ty = schema_columns[correlation.outer_column].1;
        let vector = key_vector(&rows, correlation.outer_column, outer_ty)?;
        let outer_values: Vec<Option<ScalarValue>> =
            (0..rows.len()).map(|i| vector.get(i)).collect();

        let mut kept = Vec::with_capacity(rows.len());
        for (row, outer_value) in rows.into_iter().zip(outer_values) {
            let matched = match outer_value {
                None => empty_inner_keeps(sub.kind),
                Some(value) => {
                    let inner = correlated_inner(&sub.subquery, correlation, value);
                    let StatementOutcome::Rows(result) =
                        self.run_select_scoped(&inner, overlay, scope)?
                    else {
                        return Err(EngineError::Unsupported("a subquery must be a SELECT"));
                    };
                    let plan = fold_subquery(sub.kind, &result)?;
                    !filter_rows(&plan, addressable, vec![row.clone()])?.is_empty()
                }
            };
            if matched {
                kept.push(row);
            }
        }
        Ok(kept)
    }

    /// Filter the reconstructed outer `rows` by a **decorrelated** correlated
    /// `EXISTS` / `NOT EXISTS` — the set-based semi / anti hash-join path ([STL-317])
    /// the [`semi_anti_decorrelation`](BoundSubqueryFilter::semi_anti_decorrelation)
    /// recognizer selects, replacing the [STL-239] per-row re-execution.
    ///
    /// The inner is run **once**, unfiltered, over the same `overlay` and snapshot
    /// the outer reads — so the per-statement `(sys, valid)` and read-your-own-writes
    /// rules still hold across both inputs (docs/16 §6, [STL-203]), exactly as the
    /// per-row path's per-row re-runs did. Its correlation column (`inner_column`,
    /// read straight off the `SELECT *` inner result) and the outer's (`outer_column`)
    /// become the join keys; [`hash_join`] then computes a `SEMI` join (`EXISTS`) or
    /// `ANTI` join (`NOT EXISTS`) and returns the surviving outer-row indices.
    ///
    /// NULL-key semantics fall out of the join for free: a NULL key never matches
    /// ([`hash_join`]), so a NULL outer correlation value drops under `SEMI` and
    /// survives under `ANTI` — the same answer
    /// [`empty_inner_keeps`] gives the per-row path for a NULL.
    fn filter_semi_anti_decorrelated(
        &self,
        plan: SemiAntiDecorrelation,
        sub: &BoundSubqueryFilter,
        schema_columns: &[(String, LogicalType)],
        rows: Vec<Vec<Option<Vec<u8>>>>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
        // One inner scan, at the outer's snapshot and overlay. The inner projects
        // `*` (the binder normalizes any `[NOT] EXISTS` inner — it keys off the
        // predicate, not the negation), so its column `inner_column` is the
        // correlation key in schema order.
        let StatementOutcome::Rows(inner) =
            self.run_select_scoped(&sub.subquery, overlay, scope)?
        else {
            return Err(EngineError::Unsupported("a subquery must be a SELECT"));
        };

        // The correlation key is decoded with the *outer* column's type; the binder
        // proved the inner column shares it ([`match_correlation`]).
        let key_ty = schema_columns[plan.outer_column].1;
        let outer_keys = key_vector(&rows, plan.outer_column, key_ty)?;
        let inner_keys = key_vector(&inner.rows, plan.inner_column, key_ty)?;

        let indices = hash_join(
            lower_join_type(plan.join_type),
            std::slice::from_ref(&outer_keys),
            rows.len(),
            &Expr::col(0),
            std::slice::from_ref(&inner_keys),
            inner.rows.len(),
            &Expr::col(0),
        )
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

        // `indices.left` are the surviving outer-row positions in ascending,
        // duplicate-free order (a `SEMI` / `ANTI` join emits each probe row at most
        // once, in probe order), so taking each keeps the outer scan order — exactly
        // what the per-row filter preserved.
        let mut rows = rows;
        Ok(indices
            .left
            .iter()
            .map(|&l| std::mem::take(&mut rows[l]))
            .collect())
    }

    /// Filter the reconstructed outer `rows` by a **decorrelated** correlated `IN` —
    /// the set-based **composite-key** semi-join path ([STL-337]) the
    /// [`composite_semi_decorrelation`](BoundSubqueryFilter::composite_semi_decorrelation)
    /// recognizer selects, replacing the [STL-239] per-row re-execution for the one
    /// shape `EXISTS` could not fold (an `IN` carries a second equality — the
    /// membership column — so "∃ an inner row for this outer row" is a *two-key* test:
    /// `s.k = t.k` **and** `s.a = t.a`).
    ///
    /// The inner runs **once**, over the same `overlay` and snapshot the outer reads,
    /// so the per-statement `(sys, valid)` and read-your-own-writes rules hold across
    /// both inputs (docs/16 §6, [STL-203]) exactly as the per-row re-runs did. It
    /// projects `[membership, correlation key]` (the binder appended the key).
    /// [`hash_join`] is single-key, so each side's
    /// `(correlation key, membership)` pair is folded into one **synthetic composite
    /// key** ([`composite_key_vector`]); a `SEMI` join then yields the outer rows
    /// whose composite is a member of the inner's composite set.
    ///
    /// This reproduces `IN`'s three-valued logic exactly: the composite is NULL when
    /// *either* component is NULL, and a NULL key never matches ([`hash_join`]), so a
    /// NULL outer membership value (or correlation key) — and a NULL inner one — can
    /// never make `IN` `TRUE`, matching the per-row fold ([`in_subquery_plan`], and
    /// [`empty_inner_keeps`] for a NULL correlation key). `NOT IN` is deliberately
    /// *not* decorrelated (its per-group NULL-in-set trap is not an anti join), so it
    /// never reaches here — it keeps the per-row fallback.
    fn filter_composite_semi_decorrelated(
        &self,
        plan: CompositeKeyDecorrelation,
        sub: &BoundSubqueryFilter,
        schema_columns: &[(String, LogicalType)],
        rows: Vec<Vec<Option<Vec<u8>>>>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
        let StatementOutcome::Rows(inner) =
            self.run_select_scoped(&sub.subquery, overlay, scope)?
        else {
            return Err(EngineError::Unsupported("a subquery must be a SELECT"));
        };

        // Each composite component decodes with its outer column's type: the binder
        // proved the inner correlation key shares the outer key column's type and the
        // inner membership the outer tested column's type.
        let key_ty = schema_columns[plan.outer_key_column].1;
        let member_ty = schema_columns[plan.outer_member_column].1;
        let outer_keys = composite_key_vector(
            &rows,
            (plan.outer_key_column, key_ty),
            (plan.outer_member_column, member_ty),
        )?;
        let inner_keys = composite_key_vector(
            &inner.rows,
            (plan.inner_key_column, key_ty),
            (plan.inner_member_column, member_ty),
        )?;

        let indices = hash_join(
            ExecJoinType::Semi,
            std::slice::from_ref(&outer_keys),
            rows.len(),
            &Expr::col(0),
            std::slice::from_ref(&inner_keys),
            inner.rows.len(),
            &Expr::col(0),
        )
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

        // A `SEMI` join emits each surviving outer row once, in ascending probe
        // order, so taking each keeps the outer scan order the per-row filter kept.
        let mut rows = rows;
        Ok(indices
            .left
            .iter()
            .map(|&l| std::mem::take(&mut rows[l]))
            .collect())
    }

    /// Filter the reconstructed outer `rows` by a **decorrelated** correlated `NOT IN`
    /// — the NULL-aware **composite-key anti-join** path ([STL-346]) the
    /// [`composite_anti_decorrelation`](BoundSubqueryFilter::composite_anti_decorrelation)
    /// recognizer selects, replacing the [STL-239] per-row re-execution.
    ///
    /// `NOT IN` is **not** a plain anti join. Under SQL three-valued logic, `t.a NOT IN
    /// (SELECT s.a FROM s WHERE s.k = t.k)` keeps an outer row iff its correlation group
    /// `{s.a : s.k = t.k}` is empty, **or** `t.a` is non-NULL, the group holds no NULL
    /// membership value, and `t.a` differs from every member. It is `UNKNOWN` (drop)
    /// whenever `t.a` is NULL over a non-empty group, or the group contains a NULL
    /// membership value and `t.a` matches no non-NULL member.
    ///
    /// A plain composite anti join — keep `(t.k, t.a)` when no inner `(s.k, s.a)`
    /// matches — gets every case right **except** it wrongly *keeps* those two
    /// `UNKNOWN` rows (their composite key is NULL, or no non-NULL inner composite
    /// matches). So this runs the composite anti join (reusing [`composite_key_vector`],
    /// like the `IN` semi path) and then drops the over-kept rows with
    /// **per-correlation-group NULL tracking** built from the *same* single inner scan:
    ///
    /// * `nonempty` — the correlation keys `k` (non-NULL) with ≥ 1 inner row
    ///   (`s.k = k`), i.e. a non-empty group; and
    /// * `null_member` — those whose group holds a NULL membership value (`s.k = k`,
    ///   `s.a` NULL).
    ///
    /// An anti-kept row drops iff its group is non-empty **and** either its `t.a` is
    /// NULL or its group is in `null_member` (a non-NULL `t.a` that matched a member
    /// was already dropped by the anti join). A NULL `t.k` is an empty group —
    /// `NOT IN ()` is TRUE — and the anti join keeps it (its composite is NULL),
    /// correctly, with no entry in `nonempty`.
    ///
    /// The inner runs **once**, over the same `overlay` and snapshot the outer reads,
    /// so the per-statement `(sys, valid)` and read-your-own-writes rules hold across
    /// both join inputs (docs/16 §6, [STL-203]) exactly as the per-row re-runs did.
    fn filter_composite_anti_decorrelated(
        &self,
        plan: CompositeKeyDecorrelation,
        sub: &BoundSubqueryFilter,
        schema_columns: &[(String, LogicalType)],
        rows: Vec<Vec<Option<Vec<u8>>>>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
        let StatementOutcome::Rows(inner) =
            self.run_select_scoped(&sub.subquery, overlay, scope)?
        else {
            return Err(EngineError::Unsupported("a subquery must be a SELECT"));
        };

        // Each composite component decodes with its outer column's type: the binder
        // proved the inner correlation key shares the outer key column's type and the
        // inner membership the outer tested column's type.
        let key_ty = schema_columns[plan.outer_key_column].1;
        let member_ty = schema_columns[plan.outer_member_column].1;

        // The plain composite ANTI join: keep each outer row whose `(t.k, t.a)`
        // composite matches no inner `(s.k, s.a)` composite (NULL when either component
        // is NULL, and a NULL key never matches).
        let outer_keys = composite_key_vector(
            &rows,
            (plan.outer_key_column, key_ty),
            (plan.outer_member_column, member_ty),
        )?;
        let inner_keys = composite_key_vector(
            &inner.rows,
            (plan.inner_key_column, key_ty),
            (plan.inner_member_column, member_ty),
        )?;
        let indices = hash_join(
            ExecJoinType::Anti,
            std::slice::from_ref(&outer_keys),
            rows.len(),
            &Expr::col(0),
            std::slice::from_ref(&inner_keys),
            inner.rows.len(),
            &Expr::col(0),
        )
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

        // Per-correlation-group NULL tracking, built from the same inner scan. A NULL
        // inner correlation key (`s.k`) matches no outer key (`s.k = t.k` needs both
        // non-NULL), so it forms no group and is skipped.
        let inner_key_col = key_vector(&inner.rows, plan.inner_key_column, key_ty)?;
        let inner_member_col = key_vector(&inner.rows, plan.inner_member_column, member_ty)?;
        let mut nonempty: HashSet<Vec<u8>> = HashSet::new();
        let mut null_member: HashSet<Vec<u8>> = HashSet::new();
        for j in 0..inner.rows.len() {
            if let Some(sk) = inner_key_col.get(j) {
                let key = encode_value(&sk);
                if inner_member_col.get(j).is_none() {
                    null_member.insert(key.clone());
                }
                nonempty.insert(key);
            }
        }

        // Drop the rows the plain anti join over-keeps. Decode the outer key / member
        // components to read each surviving row's `(t.k, t.a)`.
        let outer_key_col = key_vector(&rows, plan.outer_key_column, key_ty)?;
        let outer_member_col = key_vector(&rows, plan.outer_member_column, member_ty)?;
        let mut rows = rows;
        let mut kept = Vec::with_capacity(indices.left.len());
        for &l in &indices.left {
            // A NULL correlation key → empty group → `NOT IN ()` is TRUE → keep.
            if let Some(tk) = outer_key_col.get(l) {
                let key = encode_value(&tk);
                // Non-empty group + (NULL outer value, or a NULL member in the group)
                // → 3VL `UNKNOWN` → drop. A non-NULL `t.a` equal to a member is already
                // gone (the anti join dropped it).
                let drop = nonempty.contains(&key)
                    && (outer_member_col.get(l).is_none() || null_member.contains(&key));
                if drop {
                    continue;
                }
            }
            // The anti join emits each surviving outer row once in ascending probe
            // order, and this drop pass preserves it, so the outer scan order holds.
            kept.push(std::mem::take(&mut rows[l]));
        }
        Ok(kept)
    }

    /// Resolve a bound `SELECT`'s rows through the vectorized operator pipeline
    /// ([STL-206], ADR-0027): the scan source emits `(business_key, payload)`
    /// batches, [`ExplodePayload`] slices the packed payload into first-class typed
    /// value columns in schema order (position 0 the key, position i+1 value column
    /// i), and the [`Filter`] operator evaluates the bound `WHERE <col> = <lit>`
    /// over each batch via `eval_expr`. Returns each surviving row as a full
    /// `[business key, value cells…]` tuple — the shared input the projection (a
    /// plain `SELECT`) and the aggregation ([`run_aggregate`], [STL-171]) both read.
    ///
    /// A constant period predicate ([STL-165]) that folds false excludes every row,
    /// so no scan runs (never a silently-unfiltered read); a per-row period predicate
    /// ([STL-193]) builds each row's `[from, to)` from its value cells and drops the
    /// rows it excludes. A key-equality predicate is pushed down to the scan for
    /// zone-map pruning; the same `Filter` re-applies it so the answer is exact
    /// regardless of what the prune could prove.
    // The tier handles + schema + plan + valid-time column positions + metrics are
    // each a distinct input the fused scan needs; bundling them into a context
    // struct would only indirect the one call site.
    #[allow(clippy::too_many_arguments)]
    fn scan_rows(
        bound: &BoundSelect,
        state: &TableState<C, D>,
        schema_columns: &[(String, LogicalType)],
        value_count: usize,
        valid_cols: Option<(usize, usize)>,
        key_window: Option<&(BusinessKey, BusinessKey)>,
        plan: &FilterPlan,
        metrics: &SharedMetrics,
    ) -> Result<ScannedRows, EngineError> {
        // The `WHERE` resolves to a single vectorized predicate ([STL-213]): a
        // `<col> <cmp> <scalar>` comparison ([STL-151]), a per-row period
        // predicate lowered to `Expr::Period` over `MakePeriod` operands
        // ([STL-193]), or an uncorrelated subquery folded to its constant filter
        // ([STL-234]). A fully-constant predicate folds to a truth value instead —
        // an `Empty` plan excludes every row, so skip the scan entirely (never a
        // silently-unfiltered read).
        let filter_expr = match plan {
            // A constant-false predicate skips the scan entirely — no segment is
            // examined, so the accounting is the all-zero default.
            FilterPlan::Empty => {
                return Ok(ScannedRows {
                    rows: Vec::new(),
                    stats: ScanStats::default(),
                });
            }
            FilterPlan::KeepAll => None,
            FilterPlan::Predicate(expr) => Some(expr.clone()),
        };

        // Push a business-key constraint down to the scan for zone-map pruning:
        // an index probe's candidate window when one was taken ([STL-233]), else
        // a literal key equality. Any richer predicate (a value-column compare,
        // an arithmetic, a period) lives inside the opaque payload, which a zone
        // map cannot reason about, so the vectorized `Filter` below is where it
        // is applied. The pushed-down predicate is re-applied by that same
        // `Filter`, so the answer is exact regardless of what the prune could
        // prove — for the index window, because every key outside it is no
        // candidate at all (the superset contract).
        let predicate = match key_window {
            Some((low, high)) => Predicate::Range {
                column: ColumnId::BusinessKey,
                low: ZoneBound::Bytes(low.as_bytes().to_vec()),
                high: ZoneBound::Bytes(high.as_bytes().to_vec()),
            },
            None => bound
                .filter
                .as_ref()
                .and_then(BoundPredicate::key_equality)
                .map_or(Predicate::All, |value| Predicate::Eq {
                    column: ColumnId::BusinessKey,
                    value: ZoneBound::Bytes(encode_value(value)),
                }),
        };

        let readers = state.engine.open_segment_readers()?;
        let mut scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(bound.snapshot),
        )
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .filter(predicate)
        // Declare the table's valid-time policy so a no-pin read still strips the
        // delta tier's framed prefix — otherwise a plain `SELECT` over a
        // valid-time table decodes the temporal envelope as row data ([STL-218]).
        .valid_time(state.valid_time)
        // Report the scan's pruning accounting into the session series ([STL-253]).
        .metrics(Arc::clone(metrics));
        // Pin the valid axis too when the bound plan carries a `FOR VALID_TIME
        // AS OF v` instant ([STL-164]); without one a valid-time table is read
        // unfiltered (every system-live version, period columns readable).
        if let Some(v) = bound.valid_snapshot {
            scan = scan.valid_as_of(ValidTimeMicros(v.0));
        }
        // Push a valid-time interval probe down for *segment pruning* when the
        // `WHERE` is a per-row PERIOD predicate over this table's own valid-time
        // period against a constant probe — `PERIOD(valid_from, valid_to)
        // OVERLAPS / CONTAINS PERIOD(lo, hi)` ([STL-193]) — so the per-segment
        // valid-interval summary can skip a segment whose coverage cannot overlap
        // `[lo, hi)` ([STL-315]). Prune-only: the `Filter` below re-applies the
        // exact predicate, so the answer is identical to the unpruned scan.
        if let Some(cols) = valid_cols
            && let Some(period) = bound.period_filter.as_ref()
            && let Some((lo, hi)) = valid_overlap_probe(period, cols)
        {
            scan = scan.prune_valid_overlap(lo, hi);
        }

        // ScanSource → ExplodePayload → [Filter]: explode the packed payload into
        // first-class typed value columns (schema order: position 0 the key,
        // position i+1 value column i), then filter the whole batch. Exploded value
        // columns have no `ColumnId`, so the full row is read positionally.
        let source = ScanSource::new(scan, DEFAULT_BATCH_SIZE);
        let exploded = ExplodePayload::new(source, value_count);
        let mut pipeline: Box<dyn Operator + '_> = match filter_expr {
            Some(expr) => {
                let schema_types = schema_columns.iter().map(|(_, ty)| *ty).collect();
                Box::new(Filter::new(exploded, expr, schema_types))
            }
            None => Box::new(exploded),
        };

        let ncols = value_count + 1;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        while let Some(batch) = pipeline.next()? {
            for r in 0..batch.rows {
                rows.push((0..ncols).map(|i| batch_cell(&batch, i, r)).collect());
            }
        }
        // The pruning accounting bubbles up from the `ScanSource` at the bottom of
        // the pipeline through the shaping operators ([`Operator::stats`], STL-201).
        // The loop above drained at least one `next`, so the scan has resolved; the
        // default guards the impossible un-resolved case rather than panicking.
        let stats = pipeline.stats().unwrap_or_default();
        Ok(ScannedRows { rows, stats })
    }

    /// The candidate window for a bound `SELECT`'s `WHERE`, when a secondary
    /// index can serve it ([STL-233], ranges [STL-237]) — the rule-based "use
    /// the index when usable" the v0.3 substrate ships in place of a cost
    /// model. Usable means **all** of:
    ///
    /// * the `WHERE` is exactly `<value column> <cmp> <literal>`
    ///   ([`column_comparison`](BoundPredicate::column_comparison) — a key
    ///   equality keeps its own zone-map push-down);
    /// * a live index covers exactly that column, and its structure answers
    ///   the operator's probe shape — `=` is an equality probe, `<` `<=` `>`
    ///   `>=` are one-sided range probes, and a kind that cannot range-walk
    ///   (or a `<>`, whose complement no window covers) declines;
    /// * the read snapshot is at or after the index's build/rebuild
    ///   [floor](crate::secondary::IndexState) — an `AS OF` before it reads
    ///   history the build never saw, so it must full-scan.
    ///
    /// The caller never consults the structure for an overlaid
    /// (read-your-own-writes) read: buffered writes are not committed, so they
    /// are not noted. `None` means "no index applies — full scan"; both probe
    /// answers count toward [`index_probe_count`](Self::index_probe_count),
    /// a declined probe does not (no structure served the read).
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    /// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
    fn index_window(
        &self,
        table: &str,
        bound: &BoundSelect,
        schema_columns: &[(String, LogicalType)],
    ) -> Option<Probe> {
        if self.index_states.is_empty() {
            return None;
        }
        let (position, op, literal) = bound.filter.as_ref()?.column_comparison()?;
        let (column, _) = schema_columns.get(position)?;
        // The binder folded the literal to the column's type, so its canonical
        // encoding is exactly what the maintenance hook noted.
        let cell = encode_value(literal);
        // Any live index on this column whose floor admits the read snapshot
        // can serve. Floors differ when indexes were created or rebuilt at
        // different instants, and (with sibling kinds, [STL-238]) structures
        // differ in which probe shapes they answer — so the first (name-ordered)
        // match must not veto the probe for a usable sibling.
        let probe = self
            .catalog
            .indexes_on(table)
            .filter(|def| matches!(def.columns(), [c] if c == column))
            .filter_map(|def| self.index_states.get(def.name()))
            .filter(|state| bound.snapshot >= state.floor)
            .find_map(|state| match op {
                CompareOp::Eq => Some(state.structure.equality_candidates(&cell)),
                CompareOp::Lt => state
                    .structure
                    .range_candidates(Bound::Unbounded, Bound::Excluded(&cell)),
                CompareOp::Le => state
                    .structure
                    .range_candidates(Bound::Unbounded, Bound::Included(&cell)),
                CompareOp::Gt => state
                    .structure
                    .range_candidates(Bound::Excluded(&cell), Bound::Unbounded),
                CompareOp::Ge => state
                    .structure
                    .range_candidates(Bound::Included(&cell), Bound::Unbounded),
                CompareOp::Ne => None,
            })?;
        self.index_probes.set(self.index_probes.get() + 1);
        Some(probe)
    }

    /// The rows a transaction sees under **read-your-own-writes** ([STL-203]): the
    /// pinned-snapshot rows of this table with the transaction's own buffered writes
    /// overlaid, then `WHERE`/period-filtered. Storage is never touched — the overlay
    /// is purely in-memory, so a `ROLLBACK` (dropping the buffer) leaves nothing
    /// behind.
    ///
    /// The base is the *unfiltered* snapshot scan
    /// ([`scan_all_rows`](Self::scan_all_rows)): a buffered write can flip a row's
    /// `WHERE` membership, so the filter is applied *after* the overlay
    /// ([`filter_rows`]) rather than fused into the scan as on the committed-only
    /// path. Byte-equality on the canonical encoding is exactly the typed `=` the
    /// fused [`Filter`] applies, so the two paths agree on which rows survive.
    ///
    /// On a **valid-time** table ([STL-223]) the unfiltered base also leaves the valid
    /// axis open (`scan_all_rows` pins no valid instant), so a `FOR VALID_TIME AS OF v`
    /// read filters the overlaid rows to those whose `[valid_from, valid_to)` contains
    /// `v` ([`filter_overlaid_valid`]) — the same half-open cut the committed-only scan
    /// makes with [`SnapshotScan::valid_as_of`] ([STL-164]) — before the `WHERE`.
    /// `valid_cols` is the `(from, to)` period-column positions, `None` for a
    /// system-only table (which never carries a valid pin).
    ///
    /// The returned [`ScanStats`] is the **base scan's** accounting, reported as
    /// the read's footer ([STL-318]). The base scan is *unfiltered* — the `WHERE`
    /// is applied in-engine after the overlay, never pushed down — so it gets none
    /// of the zone-map / bloom pruning a predicate would drive (the validity index
    /// still prunes wholly-superseded segments); the footer thus reflects the real
    /// storage cost a read-your-own-writes read pays, which is typically a much
    /// wider scan than the same `WHERE` on the committed fast path. The buffered
    /// writes the overlay adds are not a storage scan and so are not in this
    /// accounting; the footer's row count is the post-overlay, post-filter total the
    /// caller folds in.
    // The tuple is just `(overlaid+filtered rows, base-scan stats)`; it is *not* a
    // `ScannedRows` (whose `rows` are a scan's own output, not overlaid ones), so
    // the row-vec shape trips `type_complexity` here rather than being aliased.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn overlaid_rows(
        bound: &BoundSelect,
        state: &TableState<C, D>,
        addressable: &[(String, LogicalType)],
        value_count: usize,
        overlay: &[BoundDml],
        valid_cols: Option<(usize, usize)>,
        plan: &FilterPlan,
        needs_provenance: bool,
        metrics: &SharedMetrics,
    ) -> Result<(Vec<Vec<Option<Vec<u8>>>>, ScanStats), EngineError> {
        // The valid-axis pin is applied *after* the overlay (`filter_overlaid_valid`
        // below), so the base scan leaves the valid axis open — `valid_snapshot` is
        // `None` here even on a `FOR VALID_TIME AS OF` read. When a provenance
        // pseudo-column ([STL-247]) is referenced the base carries each version's
        // provenance; a buffered (uncommitted) write then has no commit provenance,
        // so the overlay stamps `NULL` for it.
        let n_schema = value_count + 1;
        let base = if needs_provenance {
            Self::scan_all_rows_with_provenance(state, bound.snapshot, None, value_count, metrics)?
        } else {
            Self::scan_all_rows(state, bound.snapshot, value_count, metrics)?
        };
        let overlaid = overlay_table_writes(
            base.rows,
            overlay,
            bound.table.as_str(),
            value_count,
            needs_provenance,
        );
        // Pin the valid axis when the read carries `FOR VALID_TIME AS OF v`. The pin
        // only ever reaches a valid-time table (`bind_select` rejects it otherwise),
        // so the period columns are present whenever `valid_snapshot` is set — but if
        // they cannot be resolved, fail closed (`MalformedValidBound`) rather than
        // silently skip the filter and return rows outside the pin.
        let pinned = match bound.valid_snapshot {
            Some(v) => {
                let (from_idx, to_idx) = valid_cols.ok_or(EngineError::MalformedValidBound)?;
                filter_overlaid_valid(overlaid, from_idx, to_idx, v.0)?
            }
            None => overlaid,
        };
        // Filter over exactly the columns the rows carry: the schema columns alone, or
        // the schema columns plus the three provenance pseudo-columns ([STL-247]).
        let columns = if needs_provenance {
            addressable
        } else {
            &addressable[..n_schema]
        };
        Ok((filter_rows(plan, columns, pinned)?, base.stats))
    }

    /// One join side's columns in the executor's columnar shape — a base table
    /// scanned at `snapshot` ([`scan_all_columns`](Self::scan_all_columns)), or a
    /// materialized CTE / derived table read from the scope ([STL-242]).
    ///
    /// A CTE side is already held columnar in the same `scan_all_columns` shape —
    /// one shared [`Cells`](stele_exec::Cells) buffer per column — so the side is its
    /// columns cloned by `Arc` refcount bump, no cell copied however many times the
    /// CTE is joined ([STL-321]); [`decode_key_column`] and the join output assembly
    /// consume it exactly as a base-table scan's columns.
    ///
    /// Whether a side is materialized is decided by its **schema id**, not its name:
    /// the binder stamps a CTE / derived table with the ephemeral `SchemaId(0)`
    /// sentinel ([`TableSchema::ephemeral`]), and the catalog never allocates `0`. A
    /// base table is therefore always scanned from storage even when a same-named
    /// relation is in scope — e.g. `FROM (SELECT …) AS t JOIN t AS t2`, where the
    /// derived table aliased `t` must not shadow the base table `t` on the other
    /// side.
    ///
    /// A base side inside a transaction is **read-your-own-writes** consistent
    /// ([STL-325]): when `overlay` holds buffered writes for the side's table, the
    /// side is scanned row-major with the valid axis left open
    /// ([`scan_all_rows`](Self::scan_all_rows)), the buffer is layered on
    /// ([`overlay_table_writes`]), any `FOR VALID_TIME AS OF` pin is re-applied to the
    /// overlaid rows ([`filter_overlaid_valid`], [STL-223]), and the result is
    /// transposed back into the columnar shape the join consumes — the exact pipeline
    /// the single-table overlay path runs ([`overlaid_rows`](Self::overlaid_rows)). A
    /// side with no buffered writes keeps the zero-copy columnar scan. A materialized
    /// (CTE / derived) side already reflects the overlay: it was materialized over the
    /// same buffer ([`materialize_cte`](Self::materialize_cte)).
    ///
    /// The returned [`ScanStats`] is the side's storage-scan accounting for the
    /// join footer ([STL-318]) — `Some` for a base-table scan (the overlay reports its
    /// base scan, as the single-table path does), `None` for a materialized relation,
    /// whose storage reads happened (and were accounted) when it was materialized
    /// ([`materialize_cte`](Self::materialize_cte)), not here. A join with a
    /// materialized side therefore carries no single accounting and suppresses the
    /// footer.
    fn join_side_columns(
        &self,
        side: &BoundJoinSide,
        snapshot: SystemTimeMicros,
        valid_snapshot: Option<SystemTimeMicros>,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<(Vec<Column>, Option<ScanStats>), EngineError> {
        if side.schema_id == SchemaId(0) {
            // A materialized relation (CTE / derived table) is system-only, so it
            // carries no valid axis; the binder rejects a `FOR VALID_TIME AS OF`
            // pin over a join with such a side ([STL-243]), so `valid_snapshot` is
            // always `None` on this branch.
            let relation = scope
                .get(side.table.as_str())
                .ok_or_else(|| EngineError::UnknownTable(side.table.clone()))?;
            // The relation is already columnar in the `scan_all_columns` shape, with
            // one column per `BoundJoinSide::columns` entry (an empty relation still
            // carries its full complement of zero-length columns). Clone the columns
            // by `Arc` refcount bump — no cell is copied, however many times the CTE
            // is joined ([STL-321]). The `None` stats suppress the footer: a
            // materialized side was scanned and accounted at materialization, not here
            // ([STL-318]).
            //
            // A full `assert_eq!` (not `debug_assert_eq!`): the binder guarantees the
            // counts match, but a contract break must fail fast here with context
            // rather than slip through to a less actionable `columns[key]` index panic
            // in `decode_key_column` / the join output assembly. The check is one
            // length compare per join side — negligible next to the scan it guards.
            assert_eq!(
                relation.columns.len(),
                side.columns.len(),
                "a materialized relation carries exactly its bound column count"
            );
            return Ok((relation.columns.clone(), None));
        }
        let state = self
            .tables
            .get(&side.table)
            .ok_or_else(|| EngineError::UnknownTable(side.table.clone()))?;
        let value_count = side.columns.len().saturating_sub(1);
        // Read-your-own-writes ([STL-325]): this side has buffered writes in the open
        // transaction, so overlay them before the join — the same row-major pipeline a
        // single-table read runs ([`overlaid_rows`]). The base scan leaves the valid
        // axis open; a `FOR VALID_TIME AS OF` pin is re-applied after the overlay
        // ([`filter_overlaid_valid`], [STL-223]), then the rows are transposed into the
        // columnar shape `decode_key_column` / the output assembly consume.
        if overlay.iter().any(|d| d.table() == side.table) {
            let base = Self::scan_all_rows(state, snapshot, value_count, &self.metrics)?;
            let overlaid =
                overlay_table_writes(base.rows, overlay, &side.table, value_count, false);
            let pinned = match valid_snapshot {
                Some(v) => {
                    let (from_idx, to_idx) = self.side_valid_cols(&side.table, snapshot)?;
                    filter_overlaid_valid(overlaid, from_idx, to_idx, v.0)?
                }
                None => overlaid,
            };
            let columns = columns_from_rows(pinned, value_count + 1);
            return Ok((columns, Some(base.stats)));
        }
        let (columns, stats) =
            Self::scan_all_columns(state, snapshot, valid_snapshot, value_count)?;
        Ok((columns, Some(stats)))
    }

    /// The `(from, to)` indices of a valid-time join side's period columns within its
    /// schema row (`[business key, value cells…]`) — the positions
    /// [`filter_overlaid_valid`] re-applies a `FOR VALID_TIME AS OF` pin over after a
    /// read-your-own-writes overlay ([STL-325]). The binder only pins the valid axis
    /// of a join when **every** input has one ([STL-243]), so a valid-pinned side is
    /// always a valid-time table here; a side whose schema or period columns cannot be
    /// resolved is an internal contract break, surfaced (never silently dropping the
    /// pin and admitting rows outside it).
    fn side_valid_cols(
        &self,
        table: &str,
        snapshot: SystemTimeMicros,
    ) -> Result<(usize, usize), EngineError> {
        let schema = self
            .catalog
            .resolve(table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let columns = schema.columns();
        let idx = |name: &str| columns.iter().position(|c| c.name() == name);
        schema
            .temporal()
            .valid_time()
            .and_then(|spec| Some((idx(spec.from_column())?, idx(spec.to_column())?)))
            .ok_or(EngineError::MalformedValidBound)
    }

    /// Run a bound `JOIN` — a two-table join ([STL-172], [STL-264]) or an N-way
    /// left-deep chain ([STL-323]).
    ///
    /// The leftmost input is scanned at the one statement `(sys, valid)` pin
    /// ([STL-243]; `AS OF` on either axis threads through here, every input read at
    /// the *same* point — docs/16 §8) into the executor's columnar shape
    /// ([`scan_all_columns`](Self::scan_all_columns)) — shared
    /// [`Cells`](stele_exec::Cells) buffers, not a row-major copy. The chain then
    /// folds **left-deep** ([`join_step`](Self::join_step)): each step scans its
    /// right input, decodes the two join-key columns into typed [`Vector`]s, and
    /// hands them to the [`hash_join`] operator, which returns the surviving rows as
    /// input-row indices. A zero-copy [`GatheredColumns`] view ([STL-224])
    /// materializes the step's **addressable output** — the accumulated columns,
    /// then the right's for an `INNER` / `LEFT` join (a `LEFT` join's unmatched row
    /// draws `NULL` for every right column), `SEMI` / `ANTI` keeping the accumulated
    /// left alone. Intermediate steps stay columnar (the next step joins their
    /// gathered buffers); the final step materializes row-major for the shaping
    /// tail. A two-table join is the one-step chain, so its hot path is unchanged —
    /// the intermediate loop is empty and the single step materializes directly.
    ///
    /// The `WHERE` / aggregate / `DISTINCT` / `ORDER BY` / `OFFSET` / `LIMIT` tail
    /// then runs over those rows through the **same** shared helpers a single-table
    /// read uses ([`filter_rows`] / [`run_aggregate`] / [`shape_rows`]), addressing
    /// the addressable columns by the indices the binder bound them against (the
    /// binder's `JoinScope`, STL-264) — so the join composes with the rest of the
    /// SELECT surface. A join inside a transaction is **read-your-own-writes**
    /// consistent ([STL-325]): each side's scan is overlaid with the transaction's
    /// buffered writes for that table ([`join_side_columns`](Self::join_side_columns))
    /// exactly as a single-table read is ([STL-203], [STL-223]). The caller gates the
    /// overlay by AS-OF dimension — `execute_at` drops it for a system time-travel
    /// read — so `overlay` is already empty for a `FOR SYSTEM_TIME AS OF` join.
    ///
    /// The "see the engine" footer ([STL-201]) reports every input's scan accounting
    /// summed ([`ScanStats::combine`], STL-318) over the join's returned row count —
    /// unless any input is a materialized CTE / derived table, which was accounted at
    /// materialization, not here, so the join then carries no single accounting and
    /// the footer is suppressed.
    fn run_join(
        &self,
        bound: &BoundSelect,
        overlay: &[BoundDml],
        scope: &CteScope,
    ) -> Result<StatementOutcome, EngineError> {
        let join = bound
            .join
            .as_ref()
            .expect("run_join is routed only for a bound join plan");
        let snapshot = bound.snapshot;
        let valid_snapshot = bound.valid_snapshot;

        // The leftmost input seeds the accumulated output; the chain folds the rest
        // onto it. Each input is a base table scanned at the one statement
        // `(sys, valid)` pin ([STL-243]), or a materialized CTE / derived table read
        // from the scope ([STL-242]) — both yield the same shared-`Cells` columnar
        // shape the join consumes, every input at the *same* pins (docs/16 §8). The
        // same `overlay` reaches every input ([STL-325]), so an in-transaction join
        // (a self-join included) overlays each side's buffered writes consistently.
        let (mut acc_cols, mut acc_stats) =
            self.join_side_columns(&join.left, snapshot, valid_snapshot, overlay, scope)?;
        let mut acc_meta = join.left.columns.clone();

        // Intermediate steps fold left-deep, staying columnar: each gathers the
        // surviving accumulated + right cells into fresh column buffers the next step
        // joins against ([STL-323]). The final step is materialized row-major below,
        // so the two-table case (no intermediate steps) is unchanged.
        let (last, rest) = join
            .steps
            .split_last()
            .expect("a bound join carries at least one step");
        for step in rest {
            let (right_cols, indices, right_stats) = self.join_step(
                &acc_cols,
                &acc_meta,
                step,
                snapshot,
                valid_snapshot,
                overlay,
                scope,
            )?;
            let keeps_right = lower_join_type(step.join_type).keeps_right();
            let left =
                GatheredColumns::new(acc_cols, indices.left.iter().map(|&l| Some(l)).collect());
            let mut next: Vec<Column> = (0..acc_meta.len())
                .map(|c| gather_column(&left, c))
                .collect();
            if keeps_right {
                let right = GatheredColumns::new(right_cols, indices.right);
                next.extend((0..step.right.columns.len()).map(|j| gather_column(&right, j)));
                acc_meta.extend(step.right.columns.iter().cloned());
            }
            acc_cols = next;
            acc_stats = combine_join_stats(acc_stats, right_stats);
        }

        // The final step joins the accumulated output against the last right input
        // and materializes the **addressable output** row-major. The accumulated
        // columns first, then the right's for an `INNER` / `LEFT` join (a `None`
        // right index — a `LEFT` join's unmatched row — draws NULL right cells);
        // `SEMI` / `ANTI` keep the accumulated left alone. Each surviving cell is
        // copied once here, off the shared buffers ([STL-224]).
        let (right_cols, indices, right_stats) = self.join_step(
            &acc_cols,
            &acc_meta,
            last,
            snapshot,
            valid_snapshot,
            overlay,
            scope,
        )?;
        // Every input's scan accounting summed ([STL-318]) — `None` (footer
        // suppressed) if any input was a materialized relation.
        let join_stats = combine_join_stats(acc_stats, right_stats);

        let join_type = lower_join_type(last.join_type);
        let left_width = acc_meta.len();
        let left = GatheredColumns::new(acc_cols, indices.left.iter().map(|&l| Some(l)).collect());
        let rows: Vec<Vec<Option<Vec<u8>>>> = if join_type.keeps_right() {
            let right_width = last.right.columns.len();
            let right = GatheredColumns::new(right_cols, indices.right);
            (0..left.rows())
                .map(|t| {
                    let mut row = Vec::with_capacity(left_width + right_width);
                    row.extend((0..left_width).map(|i| left.bytes(i, t).map(<[u8]>::to_vec)));
                    row.extend((0..right_width).map(|j| right.bytes(j, t).map(<[u8]>::to_vec)));
                    row
                })
                .collect()
        } else {
            (0..left.rows())
                .map(|t| {
                    (0..left_width)
                        .map(|i| left.bytes(i, t).map(<[u8]>::to_vec))
                        .collect()
                })
                .collect()
        };

        // The addressable columns `(name, type)` the bound `WHERE` / aggregate /
        // `ORDER BY` index into — the same layout the binder bound them against.
        let mut addressable = acc_meta;
        if join_type.keeps_right() {
            addressable.extend(last.right.columns.iter().cloned());
        }

        // The WHERE over the materialized rows, then the aggregate or the
        // projection + shaping tail — the shared single-table helpers ([STL-264]).
        let plan = filter_plan(bound);
        let rows = filter_rows(&plan, &addressable, rows)?;
        // The join's addressable output is already gathered row-major, so it rides the
        // shared shaping tail through [`RowSource::Rows`] ([STL-338]).
        let rows = RowSource::Rows(rows);

        if let Some(agg) = &bound.aggregate {
            // A join reads every input's scan; the footer reports their summed
            // accounting ([STL-318]) over the rows the aggregate *returned* (its
            // grouped output), mirroring the single-table aggregate path
            // ([`finish_select`]).
            let mut result = run_aggregate(bound, agg, &addressable, &rows)?;
            result.stats = join_stats.map(|s| query_stats(&s, result.rows.len(), snapshot));
            return Ok(StatementOutcome::Rows(result));
        }

        // The projection: the output columns, by addressable index. `DISTINCT` /
        // `ORDER BY` / `OFFSET` / `LIMIT` move row indices only ([`shape_rows`]),
        // then each surviving row is projected.
        let projection = &join.output;
        let selection = shape_rows(bound, &addressable, projection, &rows)?;
        let out_rows: Vec<Vec<Option<Vec<u8>>>> = selection
            .iter()
            .map(|&r| projection.iter().map(|&i| rows.cell(r, i)).collect())
            .collect();
        // Every input's scan accounting summed ([STL-318]), over the join's returned
        // row count; `None` (footer suppressed) if any input was a materialized relation.
        let stats = join_stats.map(|s| query_stats(&s, out_rows.len(), snapshot));
        Ok(StatementOutcome::Rows(SelectResult {
            columns: join.columns.clone(),
            rows: out_rows,
            stats,
        }))
    }

    /// Run a `FOR { SYSTEM_TIME | VALID_TIME } { FROM a TO b | BETWEEN a AND b }`
    /// range over a join — the "history of the joined result over an interval"
    /// read ([STL-344]).
    ///
    /// This is the interval generalization of the both-axes `AS OF` join
    /// ([`run_join`](Self::run_join), [STL-243]). Where that pins one `(sys, valid)`
    /// point and reads one version per key, this **range-scans every input** over
    /// the qualified interval (the single-table [`run_system_range`](Self::run_system_range)
    /// / [`run_valid_range`](Self::run_valid_range) version selection, per side) and
    /// **intersects** the matched versions' intervals: a joined tuple's period is
    /// `[max(from), min(to))` over its inputs — docs/16 §8's "a temporal join
    /// intersects both axes", lifted from a point to an interval. An empty
    /// intersection means the two versions were never both live, so they never join.
    ///
    /// The chain folds left-deep over [`hash_join`] exactly as the point path does —
    /// so the business-key match (NULL never matches, typed equality) is identical —
    /// carrying each surviving row's running interval alongside. Each step keys its
    /// matches through an `INNER` [`hash_join`] and then layers the **temporal** shape
    /// on top of the key match ([STL-348]):
    ///
    /// * `INNER` — one output row per matched pair, interval `[max(from), min(to))`;
    ///   an empty intersection means the two versions were never both live, so they
    ///   never join.
    /// * `LEFT` — those matched rows, **plus** for each left version the maximal
    ///   sub-intervals of its period left uncovered by any matched right (the
    ///   interval *difference*, [`interval_gaps`]) as `NULL`-extended rows: the
    ///   instants the left row was live with no temporally-overlapping match.
    /// * `SEMI` — each left version over the coalesced sub-intervals it *did* have a
    ///   match ([`merge_covers`]); `ANTI` — over the gap sub-intervals it did not.
    ///   `SEMI` / `ANTI` keep only the left columns. A right match strictly inside a
    ///   left version's window fragments it into several output rows.
    ///
    /// After the fold, a row is kept iff its interval **overlaps the query range**
    /// (the same [`SystemRange::overlaps`] boundary test the single-table range
    /// applies — the formula is axis-agnostic), and the endpoints are appended
    /// **unclipped** (the tuple's actual period, like the single-table range exposes a
    /// version's actual period; `to == +∞` renders `NULL`). The `WHERE` / aggregate /
    /// projection / shaping tail then runs over those rows through the shared join
    /// helpers, so the range composes with the rest of the SELECT surface the way the
    /// point join does ([STL-264]).
    ///
    /// A materialized (CTE / derived) input has no axis to range, so it is read once
    /// from its materialization in `scope` and treated as a degenerate `[−∞, +∞)`-live
    /// side ([STL-349]) — the identity for the interval intersection, so a joined
    /// tuple's period comes from the ranged *base* sides (the binder guarantees at
    /// least one). It is admitted only under the `INNER` shape: the binder rejects a
    /// materialized side in a `LEFT` / `SEMI` / `ANTI` range join, whose interval
    /// *difference* over an unbounded period has no meaningful endpoints — a tracked
    /// follow-up. The footer reports the base inputs' summed scan accounting,
    /// suppressed (`None`) when any input is materialized, exactly as the point join
    /// does ([STL-318]).
    ///
    /// The base-table inputs are read **committed-only** — `overlay` is not threaded to
    /// them, so read-your-own-writes over a range *join* is not implemented yet (the
    /// single-table range path, by contrast, overlays buffered writes, [STL-343]). A
    /// CTE / derived input reflects its materialization, which already incorporates the
    /// transaction's overlay ([STL-242]).
    fn run_join_range(
        &self,
        bound: &BoundSelect,
        scope: &CteScope,
    ) -> Result<StatementOutcome, EngineError> {
        let join = bound
            .join
            .as_ref()
            .expect("run_join_range is routed only for a bound join plan");
        let snapshot = bound.snapshot;
        let axis = match (bound.system_range, bound.valid_range) {
            (Some(range), _) => RangeAxis::System(range),
            (_, Some(range)) => RangeAxis::Valid(range),
            (None, None) => {
                return Err(EngineError::Unsupported(
                    "run_join_range requires a system or valid range",
                ));
            }
        };

        // The leftmost input seeds the accumulated output; each row carries its
        // range version's interval `[from, to)` (`to == +∞` for an open version, or
        // the whole `[−∞, +∞)` line for a materialized side, [STL-349]), the running
        // intersection the fold narrows.
        let (mut acc_rows, mut acc_intervals, mut stats) =
            self.join_side_range_rows(&join.left, snapshot, axis, scope)?;
        let mut acc_meta = join.left.columns.clone();

        // Fold each step left-deep ([STL-323]); [`range_join_step`](Self::range_join_step)
        // scans the right input, key-matches through an `INNER` [`hash_join`], and
        // applies the step's temporal shape (`INNER` intersect, `LEFT` / `SEMI` /
        // `ANTI` interval difference, [STL-348]). The accumulated columns grow by the
        // step's right only when it keeps them.
        for step in &join.steps {
            let (next_rows, next_intervals, right_stats) = self.range_join_step(
                &acc_rows,
                &acc_intervals,
                &acc_meta,
                step,
                snapshot,
                axis,
                scope,
            )?;
            acc_rows = next_rows;
            acc_intervals = next_intervals;
            if lower_join_type(step.join_type).keeps_right() {
                acc_meta.extend(step.right.columns.iter().cloned());
            }
            stats = combine_join_stats(stats, right_stats);
        }

        // The addressable output: the join columns, then the two intersected period
        // endpoints — the same `[…join columns…, from, to]` shape the single-table
        // range appends ([STL-329]), so the bound projection (which the binder bound
        // against the join scope *plus* these endpoints) and the shaping tail address
        // it identically.
        let (from_name, to_name) = bound
            .range_endpoint_names()
            .expect("a range-join names its period endpoints");
        let mut addressable = acc_meta;
        addressable.push((from_name.to_owned(), LogicalType::TimestampTz));
        addressable.push((to_name.to_owned(), LogicalType::TimestampTz));

        // Keep a tuple iff its intersected interval overlaps the query range (the
        // joined tuple was live at some instant in it), then append the **unclipped**
        // endpoints — the tuple's actual period (docs/16 §8), exactly as the
        // single-table range exposes a version's actual `[sys_from, sys_to)`. The
        // overlap test reuses the single-table boundary predicate; an open `to`
        // (`+∞`) renders `NULL`, the convention `run_system_range` shares.
        let overlap = axis.overlap_window();
        let open = axis.open_sentinel();
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(acc_rows.len());
        for (cells, &(from, to)) in acc_rows.into_iter().zip(acc_intervals.iter()) {
            if overlap.overlaps(from, to) {
                let mut row = cells;
                row.push(Some(encode_value(&ScalarValue::TimestampTz(from))));
                row.push((to != open).then(|| encode_value(&ScalarValue::TimestampTz(to))));
                rows.push(row);
            }
        }

        // The `WHERE` over the materialized rows, then the aggregate or the
        // projection + shaping tail — the shared single-table / join helpers
        // ([STL-264]), addressing the endpoints as the trailing columns.
        let plan = filter_plan(bound);
        let rows = RowSource::Rows(filter_rows(&plan, &addressable, rows)?);
        // The base inputs' summed scan accounting ([STL-318]) over the rows the read
        // returns — `None` (footer suppressed) if any input was a materialized
        // relation ([STL-349]), the same convention the point join uses.
        let join_stats = stats;

        if let Some(agg) = &bound.aggregate {
            let mut result = run_aggregate(bound, agg, &addressable, &rows)?;
            result.stats = join_stats.map(|s| query_stats(&s, result.rows.len(), snapshot));
            return Ok(StatementOutcome::Rows(result));
        }

        let projection = &join.output;
        let selection = shape_rows(bound, &addressable, projection, &rows)?;
        let out_rows: Vec<Vec<Option<Vec<u8>>>> = selection
            .iter()
            .map(|&r| projection.iter().map(|&i| rows.cell(r, i)).collect())
            .collect();
        let stats = join_stats.map(|s| query_stats(&s, out_rows.len(), snapshot));
        Ok(StatementOutcome::Rows(SelectResult {
            columns: join.columns.clone(),
            rows: out_rows,
            stats,
        }))
    }

    /// Fold one chain step of a range join ([STL-348]): scan the step's right input
    /// over the range axis, key-match it against the accumulated rows through an
    /// `INNER` [`hash_join`] (the point join's exact matching — NULL never matches,
    /// typed equality), and apply the step's **temporal shape** over that match.
    ///
    /// For each left (accumulated) version with period `[lf, lt)`, the matched right
    /// versions' intervals are clipped to it — the "covers". The step's join type then
    /// emits:
    ///
    /// * `INNER` — the matched row per cover (interval `[max(from), min(to))`).
    /// * `LEFT` — those matched rows, **plus** a `NULL`-extended row per gap in
    ///   `[lf, lt)` not covered by any match ([`interval_gaps`] over [`merge_covers`]).
    /// * `SEMI` — the left cells over each coalesced cover ([`merge_covers`]).
    /// * `ANTI` — the left cells over each gap.
    ///
    /// Returns the next `(rows, intervals)` and the right input's scan accounting;
    /// every output row carries exactly one interval, so the next step folds onto it
    /// the same way. The caller grows the addressable columns by the step's right only
    /// when it [keeps them](stele_exec::JoinType::keeps_right).
    ///
    /// `scope` carries the materialized CTE / derived relations ([STL-349]); a step's
    /// right input may be one (read as a degenerate `[−∞, +∞)`-live side), though the
    /// binder admits a materialized side only under the `INNER` shape, so the `LEFT` /
    /// `SEMI` / `ANTI` interval difference below always works over base-table periods.
    #[allow(clippy::too_many_arguments)] // accumulated state + step + pin + axis + CTE scope
    fn range_join_step(
        &self,
        acc_rows: &[Vec<Option<Vec<u8>>>],
        acc_intervals: &[(i64, i64)],
        acc_meta: &[(String, LogicalType)],
        step: &BoundJoinStep,
        snapshot: SystemTimeMicros,
        axis: RangeAxis,
        scope: &CteScope,
    ) -> Result<RangeSideRows, EngineError> {
        let (right_rows, right_intervals, right_stats) =
            self.join_side_range_rows(&step.right, snapshot, axis, scope)?;
        let left_keys = range_key_vectors(acc_rows, acc_meta, step.left_key)?;
        let right_keys = range_key_vectors(&right_rows, &step.right.columns, step.right_key)?;
        // `INNER` over the keys gives every key-matched `(left, right)` pair; the
        // temporal logic below intersects / differences their intervals.
        let indices = hash_join(
            ExecJoinType::Inner,
            &left_keys,
            acc_rows.len(),
            &Expr::col(step.left_key),
            &right_keys,
            right_rows.len(),
            &Expr::col(step.right_key),
        )
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

        // `INNER` fast path: emit the intersected matched pairs directly, in
        // [`hash_join`] order — no per-left grouping (the common, pre-[STL-348] shape).
        if step.join_type == JoinType::Inner {
            let mut next_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(indices.left.len());
            let mut next_intervals: Vec<(i64, i64)> = Vec::with_capacity(indices.left.len());
            for (&li, ri) in indices.left.iter().zip(indices.right.iter()) {
                let ri = ri.expect("an INNER hash join pairs every left with a right");
                let (lf, lt) = acc_intervals[li];
                let (rf, rt) = right_intervals[ri];
                let from = lf.max(rf);
                let to = lt.min(rt);
                if from < to {
                    let mut row = acc_rows[li].clone();
                    row.extend(right_rows[ri].iter().cloned());
                    next_rows.push(row);
                    next_intervals.push((from, to));
                }
            }
            return Ok((next_rows, next_intervals, right_stats));
        }

        // `LEFT` / `SEMI` / `ANTI`: group the matches by left row so an unmatched left
        // row still emits its full-period gap and the interval difference sees every
        // cover at once.
        let mut matches: Vec<Vec<usize>> = vec![Vec::new(); acc_rows.len()];
        for (&li, ri) in indices.left.iter().zip(indices.right.iter()) {
            matches[li].push(ri.expect("an INNER hash join pairs every left with a right"));
        }
        let right_width = step.right.columns.len();
        let mut next_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut next_intervals: Vec<(i64, i64)> = Vec::new();
        for (li, ris) in matches.iter().enumerate() {
            let (lf, lt) = acc_intervals[li];
            // The matched right sub-intervals clipped to this left version's period —
            // the "covers" the difference works over; `LEFT` also emits the matched row.
            let mut covers: Vec<(i64, i64)> = Vec::new();
            for &ri in ris {
                let (rf, rt) = right_intervals[ri];
                let from = lf.max(rf);
                let to = lt.min(rt);
                if from < to {
                    if step.join_type == JoinType::Left {
                        let mut row = acc_rows[li].clone();
                        row.extend(right_rows[ri].iter().cloned());
                        next_rows.push(row);
                        next_intervals.push((from, to));
                    }
                    covers.push((from, to));
                }
            }
            let merged = merge_covers(covers);
            match step.join_type {
                // The matched rows above, plus a NULL-extended row per uncovered gap.
                JoinType::Left => {
                    for (from, to) in interval_gaps(lf, lt, &merged) {
                        let mut row = acc_rows[li].clone();
                        row.extend(std::iter::repeat_n(None, right_width));
                        next_rows.push(row);
                        next_intervals.push((from, to));
                    }
                }
                // The coalesced sub-intervals the left version *did* have a match.
                JoinType::Semi => {
                    for (from, to) in merged {
                        next_rows.push(acc_rows[li].clone());
                        next_intervals.push((from, to));
                    }
                }
                // The gap sub-intervals the left version did *not* have a match.
                JoinType::Anti => {
                    for (from, to) in interval_gaps(lf, lt, &merged) {
                        next_rows.push(acc_rows[li].clone());
                        next_intervals.push((from, to));
                    }
                }
                JoinType::Inner => unreachable!("the INNER fast path returned above"),
            }
        }
        Ok((next_rows, next_intervals, right_stats))
    }

    /// Scan one join input over a range axis ([STL-344]) into row-major rows paired
    /// with each row's interval — the per-side feed
    /// [`run_join_range`](Self::run_join_range) folds.
    ///
    /// **A base-table side** returns the side's canonical `[business key, value cells…]`
    /// (the same reconstruction [`run_system_range`](Self::run_system_range) /
    /// [`run_valid_range`](Self::run_valid_range) make), paired with that version's
    /// `[from, to)` interval on the ranged axis — the system interval
    /// `[sys_from, sys_to)` or the valid interval `[valid_from, valid_to)`, with
    /// `to == +∞` ([`SYSTEM_TIME_OPEN`] / [`VALID_TIME_OPEN`]) for an open version —
    /// and `Some` scan accounting for the footer.
    ///
    /// **A materialized (CTE / derived) side** ([`SchemaId(0)`](SchemaId), [STL-349])
    /// has no axis to range: it is read once from its materialization in `scope`
    /// (already computed at the statement snapshot over the transaction's overlay,
    /// [`materialize_cte`](Self::materialize_cte)) and each row is paired with the whole
    /// `[−∞, +∞)` line — the identity for the interval intersection the fold computes,
    /// so it contributes no narrowing and a joined tuple's period comes from the ranged
    /// base sides. Its `i64::MIN` lower sentinel is always dominated by a base side's
    /// finite `from` (the binder guarantees at least one base side), and the `+∞` upper
    /// is the axis open sentinel. The accounting is `None` (footer suppressed) — the
    /// relation's storage reads were accounted at materialization, the same convention
    /// the point join uses ([`join_side_columns`](Self::join_side_columns), [STL-318]).
    fn join_side_range_rows(
        &self,
        side: &BoundJoinSide,
        snapshot: SystemTimeMicros,
        axis: RangeAxis,
        scope: &CteScope,
    ) -> Result<RangeSideRows, EngineError> {
        if side.schema_id == SchemaId(0) {
            let relation = scope
                .get(side.table.as_str())
                .ok_or_else(|| EngineError::UnknownTable(side.table.clone()))?;
            // The binder stamps a materialized relation with exactly its bound column
            // count; a mismatch is a contract break, surfaced here with context rather
            // than slipping through to an index panic in the fold's key decode.
            assert_eq!(
                relation.columns.len(),
                side.columns.len(),
                "a materialized relation carries exactly its bound column count"
            );
            let rows = rows_from_columns(&relation.columns, relation.row_count);
            // Each materialized row is `[−∞, +∞)`-live ([STL-349]): `i64::MIN` is below
            // every real instant, the axis open sentinel is `+∞`.
            let intervals = vec![(i64::MIN, axis.open_sentinel()); relation.row_count];
            return Ok((rows, intervals, None));
        }
        let state = self
            .tables
            .get(&side.table)
            .ok_or_else(|| EngineError::UnknownTable(side.table.clone()))?;
        let value_count = side.columns.len().saturating_sub(1);
        let readers = state.engine.open_segment_readers()?;
        let scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .valid_time(state.valid_time)
        .metrics(Arc::clone(&self.metrics));

        // One reconstructed row `[business key, value cells…]` per version (the same
        // codec the single-table range path decodes), paired with the version's
        // interval on the ranged axis.
        let reconstruct = |key: &BusinessKey,
                           payload: Option<&[u8]>|
         -> Result<Vec<Option<Vec<u8>>>, EngineError> {
            let mut cells: Vec<Option<Vec<u8>>> = Vec::with_capacity(value_count + 1);
            cells.push(Some(key.as_bytes().to_vec()));
            cells.extend(row_codec::decode_payload(value_count, payload)?);
            Ok(cells)
        };
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut intervals: Vec<(i64, i64)> = Vec::new();
        let stats = match axis {
            RangeAxis::System(range) => {
                let (versions, stats) = scan
                    .system_range(range.from.0, range.to.0, range.closed_upper)
                    .execute_range()?;
                for v in &versions {
                    rows.push(reconstruct(&v.business_key, v.payload.as_deref())?);
                    intervals.push((v.sys_from.0, v.sys_to.0));
                }
                stats
            }
            RangeAxis::Valid(range) => {
                let (versions, stats) = scan
                    .valid_range(range.from.0, range.to.0, range.closed_upper)
                    .execute_valid_range()?;
                for (v, interval) in &versions {
                    rows.push(reconstruct(&v.business_key, v.payload.as_deref())?);
                    intervals.push((interval.from.0, interval.to.0));
                }
                stats
            }
        };
        // A base-table side always reports its scan accounting; a materialized side
        // returned `None` above ([STL-349]).
        Ok((rows, intervals, Some(stats)))
    }

    /// Scan and hash-join one chain step ([STL-323]).
    ///
    /// Scans the step's right input at the statement pin — overlaid with the
    /// transaction's buffered writes for read-your-own-writes ([STL-325], the same
    /// `overlay` every input in the chain receives) — decodes the two join-key
    /// columns (the accumulated left key by its flat addressable index, the right key
    /// by its schema index) and runs [`hash_join`], returning the right input's
    /// columns (for the caller's gather), the surviving-row [`JoinIndices`], and the
    /// right input's scan accounting. Only the key columns are decoded; the
    /// accumulated and right value columns stay opaque bytes (gathered by index), so
    /// a carried-through column is never forced through the evaluator.
    #[allow(clippy::too_many_arguments)] // accumulated state + step + both pins + RYOW overlay + scope
    fn join_step(
        &self,
        acc_cols: &[Column],
        acc_meta: &[(String, LogicalType)],
        step: &BoundJoinStep,
        snapshot: SystemTimeMicros,
        valid_snapshot: Option<SystemTimeMicros>,
        overlay: &[BoundDml],
        cte_scope: &CteScope,
    ) -> Result<(Vec<Column>, JoinIndices, Option<ScanStats>), EngineError> {
        let (right_cols, right_stats) =
            self.join_side_columns(&step.right, snapshot, valid_snapshot, overlay, cte_scope)?;
        let acc_rows = acc_cols[0].len();
        let right_rows = right_cols[0].len();
        let left_keys = decode_key_column(acc_cols, acc_meta, step.left_key)?;
        let right_keys = decode_key_column(&right_cols, &step.right.columns, step.right_key)?;
        let indices = hash_join(
            lower_join_type(step.join_type),
            &left_keys,
            acc_rows,
            &Expr::col(step.left_key),
            &right_keys,
            right_rows,
            &Expr::col(step.right_key),
        )
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
        Ok((right_cols, indices, right_stats))
    }

    /// Scan a table's reconstructed rows at `snapshot`, unfiltered — the base a
    /// read-your-own-writes overlay ([`overlaid_rows`](Self::overlaid_rows)) and
    /// index maintenance ([`build_index_state`](Self::build_index_state)) read over.
    ///
    /// The same `ScanSource → ExplodePayload` pipeline [`scan_rows`](Self::scan_rows)
    /// runs, minus the `WHERE` filter and a valid-axis *pin*, so each row comes back
    /// as its full `[business key, value cells…]` canonical bytes, paired with the
    /// scan's [`ScanStats`] ([STL-318]) — an unfiltered scan, so it gets no
    /// zone-map / bloom pruning (the validity index still prunes wholly-superseded
    /// segments). The table's valid-time policy is still declared so a valid-time
    /// table's delta frame is stripped ([STL-218]).
    fn scan_all_rows(
        state: &TableState<C, D>,
        snapshot: SystemTimeMicros,
        value_count: usize,
        metrics: &SharedMetrics,
    ) -> Result<ScannedRows, EngineError> {
        let readers = state.engine.open_segment_readers()?;
        let scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .valid_time(state.valid_time)
        .metrics(Arc::clone(metrics));
        let source = ScanSource::new(scan, DEFAULT_BATCH_SIZE);
        let mut exploded = ExplodePayload::new(source, value_count);

        let ncols = value_count + 1;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        while let Some(batch) = exploded.next()? {
            for r in 0..batch.rows {
                rows.push((0..ncols).map(|i| batch_cell(&batch, i, r)).collect());
            }
        }
        // The scan's pruning accounting ([`ScanStats`], STL-146) bubbles up through
        // `ExplodePayload` from the `ScanSource` ([`Operator::stats`], STL-201) —
        // an unfiltered scan here, so it gets no zone-map / bloom pruning (the
        // validity index can still prune wholly-superseded segments). The first
        // `next` above resolved it; the default guards the un-resolved case rather
        // than panicking. The overlay / provenance read paths report it as their
        // base scan ([STL-318]); index maintenance ignores it.
        let stats = exploded.stats().unwrap_or_default();
        Ok(ScannedRows { rows, stats })
    }

    /// Scan a table's reconstructed rows at `snapshot`, **with provenance** — each
    /// row is `[business key, value cells…, txn_id, committed_at, principal]`, the
    /// extended shape a provenance pseudo-column read needs ([STL-247]).
    ///
    /// The same `ScanSource → ExplodePayload` pipeline [`scan_all_rows`](Self::scan_all_rows)
    /// runs, but the scan additionally projects the three provenance columns
    /// ([`ColumnId::TxnId`] / [`CommittedAt`](ColumnId::CommittedAt) /
    /// [`Principal`](ColumnId::Principal)) — which every version already carries
    /// inline (invariant 5) — and [`ExplodePayload`] passes them through after the
    /// value columns. The provenance scalars are read straight off the version, so
    /// `AS OF` on either axis (a past `snapshot`, a `valid_snapshot` pin) returns
    /// each historical version's *own* writing provenance, with no extra work.
    ///
    /// The read is **unfiltered**: a `WHERE` — over a user *or* a provenance column,
    /// or a mix — is applied by the engine over the extended rows afterwards
    /// ([`filter_rows`]), because the fused vectorized [`Filter`] addresses only the
    /// table's own columns. The valid-time policy is declared (so a valid-time
    /// table's delta frame is stripped, [STL-218]) and the valid axis pinned when
    /// the read carries `FOR VALID_TIME AS OF v`, exactly as
    /// [`scan_rows`](Self::scan_rows) does.
    fn scan_all_rows_with_provenance(
        state: &TableState<C, D>,
        snapshot: SystemTimeMicros,
        valid_snapshot: Option<SystemTimeMicros>,
        value_count: usize,
        metrics: &SharedMetrics,
    ) -> Result<ScannedRows, EngineError> {
        let readers = state.engine.open_segment_readers()?;
        let mut scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        .project(vec![
            ColumnId::BusinessKey,
            ColumnId::Payload,
            ColumnId::TxnId,
            ColumnId::CommittedAt,
            ColumnId::Principal,
        ])
        .valid_time(state.valid_time)
        .metrics(Arc::clone(metrics));
        if let Some(v) = valid_snapshot {
            scan = scan.valid_as_of(ValidTimeMicros(v.0));
        }
        let source = ScanSource::new(scan, DEFAULT_BATCH_SIZE);
        let mut exploded = ExplodePayload::new(source, value_count);

        // key + value columns + the three provenance scalars — the fixed
        // `addressable_columns` width ([STL-247]).
        let ncols = value_count + 1 + provenance::PSEUDO_COLUMNS.len();
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        while let Some(batch) = exploded.next()? {
            for r in 0..batch.rows {
                rows.push((0..ncols).map(|i| batch_cell(&batch, i, r)).collect());
            }
        }
        // Carry the scan's pruning accounting up for the footer ([STL-201],
        // STL-318): unfiltered w.r.t. the `WHERE` (re-applied in the engine over the
        // widened rows), so it gets no zone-map / bloom pruning — but the validity
        // index and, on a `FOR VALID_TIME AS OF v` read, the valid pin above can
        // still prune, so this is not necessarily an all-segments scan.
        let stats = exploded.stats().unwrap_or_default();
        Ok(ScannedRows { rows, stats })
    }

    /// Scan a table's reconstructed rows at `snapshot` into **columns** — the join's
    /// per-side input in the executor's columnar shape ([STL-224]).
    ///
    /// The same unfiltered `ScanSource → ExplodePayload` pipeline as
    /// [`scan_all_rows`](Self::scan_all_rows) (same valid-time stripping, [STL-218]),
    /// but the result is kept columnar: one [`Column`] per output column — the
    /// business key, then each value column — every one a shared
    /// [`Cells`](stele_exec::Cells) buffer. Keeping the buffers (rather than the
    /// row-major copy `scan_all_rows` makes) is what lets the join's output assembly
    /// name matched rows by index instead of cloning each surviving cell.
    ///
    /// When `valid_snapshot` is set the valid axis is pinned too ([STL-243]), the
    /// same [`SnapshotScan::valid_as_of`] the single-table read uses ([STL-164]) —
    /// so a bitemporal join reads every input at one consistent `(sys, valid)` point
    /// (docs/16 §8). The binder guarantees a valid pin reaches only a valid-time
    /// side.
    ///
    /// A single emitted batch is handed back as-is — its buffers shared, nothing
    /// copied. Multiple batches are concatenated per column into one buffer, since
    /// the hash join must address every row of a side at once; that per-cell copy is
    /// no more than [`scan_all_rows`](Self::scan_all_rows) already pays, and the
    /// later per-matched-row clone is gone either way.
    fn scan_all_columns(
        state: &TableState<C, D>,
        snapshot: SystemTimeMicros,
        valid_snapshot: Option<SystemTimeMicros>,
        value_count: usize,
    ) -> Result<(Vec<Column>, ScanStats), EngineError> {
        let readers = state.engine.open_segment_readers()?;
        let mut scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .valid_time(state.valid_time);
        // Pin the valid axis when the statement carries a `FOR VALID_TIME AS OF v`
        // ([STL-243]) — the same half-open `from <= v < to` filter the single-table
        // read applies ([STL-164]); without one a valid-time side is read unfiltered.
        if let Some(v) = valid_snapshot {
            scan = scan.valid_as_of(ValidTimeMicros(v.0));
        }
        let source = ScanSource::new(scan, DEFAULT_BATCH_SIZE);
        let mut exploded = ExplodePayload::new(source, value_count);

        let ncols = value_count + 1;
        let mut batches: Vec<Batch> = Vec::new();
        while let Some(batch) = exploded.next()? {
            // ExplodePayload emits dense batches; `into_dense` is a no-op that makes
            // the shape explicit so a column read below never has to honor a selection.
            batches.push(batch.into_dense());
        }
        // This side's pruning accounting ([`ScanStats`], STL-146), to be summed
        // with the other side's for the join footer ([STL-318]). Captured before the
        // returns below — a zero-row side still resolved the scan on the first
        // `next`, so its (all-zero / delta-only) accounting is real, not the default.
        let stats = exploded.stats().unwrap_or_default();

        // No rows: `ncols` empty columns the join scans as a zero-height side.
        if batches.is_empty() {
            let empty = (0..ncols)
                .map(|_| Column::Bytes(Vec::new().into()))
                .collect();
            return Ok((empty, stats));
        }
        // One batch: its columns are already the shared buffers — hand them back
        // untouched (zero-copy), dropping the per-column `ColumnId` tag the join
        // addresses positionally.
        if batches.len() == 1 {
            let columns = batches
                .pop()
                .expect("one batch")
                .columns
                .into_iter()
                .map(|(_, col)| col)
                .collect();
            return Ok((columns, stats));
        }
        // Several batches: concatenate each column's cells into one buffer.
        let mut columns: Vec<Vec<Option<Vec<u8>>>> = (0..ncols).map(|_| Vec::new()).collect();
        for batch in &batches {
            for (i, slot) in columns.iter_mut().enumerate() {
                match &batch.columns[i].1 {
                    Column::Bytes(cells) => slot.extend(cells.iter().cloned()),
                    Column::I64(values) => {
                        slot.extend(values.iter().map(|v| Some(v.to_le_bytes().to_vec())));
                    }
                }
            }
        }
        let columns = columns
            .into_iter()
            .map(|cells| Column::Bytes(cells.into()))
            .collect();
        Ok((columns, stats))
    }

    /// Apply a bound DML statement to the table's tiers under fresh provenance,
    /// and report the affected-row count. The encoding details (key + value
    /// columns through the row codec, `UPDATE`'s read-modify-write merge) live in
    /// [`apply_bound_dml`](Self::apply_bound_dml).
    ///
    /// This is the **auto-commit point path**. A key-equality `UPDATE` /
    /// `DELETE` whose key has no live row is a 0-row no-op (`UPDATE 0` /
    /// `DELETE 0`, Postgres set semantics) rather than the storage writers'
    /// `KeyNotFound` ([STL-294], [`absent_point_tag`](Self::absent_point_tag)) —
    /// no write, no transaction id consumed.
    ///
    /// The write itself goes through the **group-commit path** (a one-statement
    /// group), so its data record is the two-phase, commit-record-gated leg the
    /// crash window ([STL-314], [ADR-0031]) requires — not a plain, unconditionally-
    /// applied record. The trade is the same one ADR-0031 accepts for the chain: the
    /// data fsync and the commit-record fsync (group-commit amortization is the
    /// follow-up).
    fn apply_dml(&mut self, dml: BoundDml) -> Result<StatementOutcome, EngineError> {
        // An absent-key point UPDATE/DELETE reports zero rows and writes nothing
        // ([STL-294]); probe against the committed state (no overlay).
        if let Some(summary) = self.absent_point_tag(&dml, self.clock.current(), &[])? {
            return Ok(StatementOutcome::Dml(summary));
        }
        let txn_id = TxnId(self.next_txn);
        self.next_txn += 1;
        let principal = self.write_principal.clone();
        // Route the point write through the **group path** so its data record is the
        // same two-phase, commit-record-gated leg a single-table `COMMIT` writes
        // ([STL-314]): the leg is fsynced first, then the commit record is the commit
        // point, so a crash between the two discards the leg on recovery rather than
        // leaving a durable-but-unchained commit ([ADR-0031]). (Pre-STL-314 this path
        // appended a plain record and recorded its commit separately, leaving exactly
        // that window.)
        let table = dml.table().to_owned();
        self.table_mut(&table)?.engine.begin_group();
        let summary = match self.apply_bound_dml(dml, txn_id, &principal) {
            Ok(summary) => summary,
            Err(e) => {
                // The statement failed mid-apply: discard the buffered (and resident-
                // applied) write so the table is left unchanged ([STL-216]) and no
                // stray group buffer lingers for the next write.
                if let Ok(state) = self.table_mut(&table) {
                    state.engine.abort_group();
                }
                return Err(e);
            }
        };
        self.finish_group_commit(txn_id, std::slice::from_ref(&table))?;
        // An auto-committed write pins no snapshot, so this is the steady-state
        // prune point under auto-commit traffic — without it the index would grow
        // with distinct keys on a server that never opens a transaction ([STL-204]).
        self.prune_write_index();
        Ok(StatementOutcome::Dml(summary))
    }

    /// The 0-row command tag a point `UPDATE` / `DELETE` of an **absent key**
    /// should report — `Some(Update(0))` / `Some(Delete(0))` — or `None` when the
    /// key is live (proceed with the write) or `dml` is not a point UPDATE/DELETE.
    ///
    /// STL-229 made predicate `UPDATE` / `DELETE` count matched live rows, but the
    /// key-equality fast path kept its pre-existing contract and *errored* on a
    /// missing key. STL-294 aligns it with set semantics: an absent key is a 0-row
    /// no-op (Postgres `UPDATE 0` / `DELETE 0`) on both the auto-commit path
    /// ([`apply_dml`](Self::apply_dml)) and at in-transaction staging
    /// ([`stage_dml`](Self::stage_dml)). The typed in-process
    /// [`update`](Self::update) / [`delete`](Self::delete) and the storage writers
    /// keep `KeyNotFound` — only the SQL-bound point path softens.
    ///
    /// Liveness is the scan-then-write plan's own answer for the single-key
    /// predicate: `SELECT <key> FROM t WHERE <key> = <literal>` through
    /// [`run_select`](Self::run_select), so it sees the in-transaction overlay
    /// (read-your-own-writes, [STL-203]) and pushes the key equality down to
    /// zone-map pruning ([`BoundPredicate::key_equality`]) rather than full-scanning.
    /// The scan-then-write and `MERGE` expansions only ever emit point writes for
    /// keys they already enumerated live, so they never reach this — they call
    /// [`apply_bound_dml`](Self::apply_bound_dml) directly and pay no per-key
    /// re-probe.
    ///
    /// [STL-294]: https://allegromusic.atlassian.net/browse/STL-294
    fn absent_point_tag(
        &self,
        dml: &BoundDml,
        snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<Option<DmlSummary>, EngineError> {
        let (table, schema_id, key, zero) = match dml {
            BoundDml::Update {
                table,
                schema_id,
                key,
                ..
            } => (table, *schema_id, key, DmlSummary::Update(0)),
            BoundDml::Delete {
                table,
                schema_id,
                key,
            } => (table, *schema_id, key, DmlSummary::Delete(0)),
            _ => return Ok(None),
        };
        let schema = self
            .catalog
            .resolve(table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.clone()))?;
        let key_col = schema
            .columns()
            .first()
            .ok_or_else(|| EngineError::UnknownTable(table.clone()))?;
        // `SELECT <key> FROM t WHERE <key> = <literal>`, read exactly as the
        // scan-then-write plan reads its predicate — so the single-key answer
        // matches the predicate path (overlay, key-equality zone-map push-down).
        let probe = BoundSelect {
            table: table.clone(),
            schema_id,
            snapshot,
            valid_snapshot: None,
            // A range scan is never lowered to one of these synthetic DML
            // probe/scan plans ([STL-244]).
            system_range: None,
            valid_range: None,
            projection: Projection::Items(vec![ProjectionItem::column(key_col.name())]),
            filter: Some(BoundPredicate {
                left: BoundScalar::Column(0),
                op: CompareOp::Eq,
                right: BoundScalar::Literal(key.clone()),
            }),
            period_filter: None,
            subquery_filter: None,
            aggregate: None,
            join: None,
            ctes: Vec::new(),
            relation_columns: None,
            distinct: false,
            order_by: Vec::new(),
            offset: 0,
            limit: None,
        };
        let StatementOutcome::Rows(matched) = self.run_select(&probe, overlay)? else {
            return Err(EngineError::Unsupported(
                "the point-DML liveness probe returned no row set",
            ));
        };
        Ok(matched.rows.is_empty().then_some(zero))
    }

    /// Which plan [`merge_live_keys`](Self::merge_live_keys) resolves a `MERGE`'s
    /// target membership with: `true` to point-probe each of the `probe_keys`
    /// distinct source keys against the always-indexed business key (per-segment
    /// bloom + zone pruning, [STL-238]), `false` to read every live key in one
    /// full-keyset scan.
    ///
    /// A **cost-based** choice ([STL-312]): the per-source-key plan does one
    /// pruned point read per distinct source key, the full-keyset plan one scan of
    /// the whole live keyspace, so probing wins exactly when the source touches
    /// fewer keys than the keyspace holds. `probe_keys` is the count of *distinct,
    /// non-NULL* join keys — the reads the probe path actually issues, since a NULL
    /// never matches and a repeated key is probed once — not the raw source-row
    /// count. The keyspace size is
    /// [`Engine::live_version_estimate`](stele_storage::engine::Engine::live_version_estimate)
    /// — the resident sealed version set plus the delta — a version count that
    /// never *undercounts* the keys a full scan reads. Both plans yield the same
    /// `live ∩ source` membership ([`merge_live_keys`](Self::merge_live_keys)), so
    /// the estimate only ever picks the faster plan, never a different upsert.
    ///
    /// This replaces the original "the target holds ≥1 sealed segment" heuristic,
    /// which always full-scanned an all-delta target and always point-probed a
    /// flushed one — wrong at the corner the estimate now catches: a flushed
    /// target whose source touches *more* keys than its live keyspace, where the
    /// single scan beats `probe_keys` point reads.
    ///
    /// An unknown table (an internal contract break — `bind_merge` proved it
    /// resolves) reports `false`, leaving the subsequent read to surface the error.
    ///
    /// [STL-312]: https://allegromusic.atlassian.net/browse/STL-312
    fn merge_should_probe_per_key(&self, table: &str, probe_keys: usize) -> bool {
        let Some(state) = self.tables.get(table) else {
            return false;
        };
        (probe_keys as u64) < state.engine.live_version_estimate()
    }

    /// Whether `key` resolves to a live target row at the statement snapshot — the
    /// per-source-row `MERGE` probe ([STL-238]).
    ///
    /// A point `SELECT <key> FROM t WHERE <key> = key` through
    /// [`run_select`](Self::run_select), exactly as
    /// [`absent_point_tag`](Self::absent_point_tag) probes a single key: the key
    /// equality pushes down to the always-indexed business key, so the read prunes
    /// to the segments whose **per-segment bloom** admits the key (and zone maps
    /// rule out the rest) rather than scanning the keyspace, and it sees the
    /// transaction overlay (read-your-own-writes, [STL-203]). Key for key this is
    /// the same answer the full-keyset path's `live.contains(key)` gives, so the
    /// two MERGE plans are result-identical — the probe changes speed, never the
    /// upsert.
    fn merge_key_is_live(
        &self,
        merge: &BoundMerge,
        key_col: &str,
        snapshot: SystemTimeMicros,
        key: &ScalarValue,
        overlay: &[BoundDml],
    ) -> Result<bool, EngineError> {
        let probe = BoundSelect {
            table: merge.table.clone(),
            schema_id: merge.schema_id,
            snapshot,
            valid_snapshot: None,
            // A range scan is never lowered to one of these synthetic DML
            // probe/scan plans ([STL-244]).
            system_range: None,
            valid_range: None,
            projection: Projection::Items(vec![ProjectionItem::column(key_col)]),
            filter: Some(BoundPredicate {
                left: BoundScalar::Column(0),
                op: CompareOp::Eq,
                right: BoundScalar::Literal(key.clone()),
            }),
            period_filter: None,
            subquery_filter: None,
            aggregate: None,
            join: None,
            ctes: Vec::new(),
            relation_columns: None,
            distinct: false,
            order_by: Vec::new(),
            offset: 0,
            limit: None,
        };
        let StatementOutcome::Rows(matched) = self.run_select(&probe, overlay)? else {
            return Err(EngineError::Unsupported(
                "the MERGE liveness probe returned no row set",
            ));
        };
        Ok(!matched.rows.is_empty())
    }

    /// Apply an auto-committed **scan-then-write** `UPDATE` / `DELETE`
    /// ([STL-229]): expand the predicate into one per-key write per matching live
    /// row at `read_snapshot` ([`expand_scan_dml`](Self::expand_scan_dml)), then
    /// apply the whole set as a single **atomic group** — the same
    /// [`apply_group`](Self::apply_group) → [`finish_group_commit`](Self::finish_group_commit)
    /// machinery a multi-statement `COMMIT` uses ([STL-192]). All writes target
    /// one table, so the commit is the single-record fast path: one WAL record,
    /// one fsync. A failure applying any write of the set discards the group
    /// ([`abort_group`](stele_storage::engine::Engine::abort_group)) — nothing is
    /// made durable and the in-memory tiers are rolled back ([STL-216]), so the
    /// statement leaves the table unchanged.
    ///
    /// The reported tag counts the **matched live rows at the snapshot** — `0`
    /// when nothing matched, in which case no group is opened and no WAL record
    /// is written.
    ///
    /// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
    fn apply_scan_dml(
        &mut self,
        dml: BoundDml,
        read_snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<StatementOutcome, EngineError> {
        let (writes, summary) = self.expand_scan_dml(dml, read_snapshot, overlay)?;
        self.apply_write_group(writes, summary)
    }

    /// Apply an auto-committed `MERGE` ([STL-230]): resolve each source row
    /// against the target's live keys at `read_snapshot`
    /// ([`expand_merge`](Self::expand_merge)) and apply the resulting write set
    /// as a single atomic group — exactly the scan-then-write machinery
    /// ([`apply_scan_dml`](Self::apply_scan_dml)), so a failure on any row of
    /// the set discards the group and the statement leaves the table unchanged
    /// ([STL-216]).
    ///
    /// The reported tag counts the **source rows acted on** (each one update or
    /// one insert); a row whose arm is absent is skipped and not counted.
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    fn apply_merge(
        &mut self,
        merge: &BoundMerge,
        read_snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<StatementOutcome, EngineError> {
        let (writes, summary) = self.expand_merge(merge, read_snapshot, overlay)?;
        self.apply_write_group(writes, summary)
    }

    /// Apply an auto-committed multi-row `INSERT … VALUES (…), (…), …`
    /// ([STL-228]): fan the bound rows out into one point [`BoundDml::Insert`]
    /// each ([`expand_insert_rows`]) and apply the whole set as a single atomic
    /// group — exactly the scan-then-write machinery
    /// ([`apply_scan_dml`](Self::apply_scan_dml)), so a failure applying any row
    /// (a duplicate key, a schema drift between binding and applying) discards the
    /// group and the statement leaves the table unchanged ([STL-216]). It needs no
    /// snapshot read — the binder already folded every row — so the expansion is a
    /// pure unpacking.
    ///
    /// The reported tag counts the rows inserted: `INSERT 0 N`.
    ///
    /// [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
    fn apply_insert_rows(&mut self, dml: BoundDml) -> Result<StatementOutcome, EngineError> {
        let (writes, summary) = expand_insert_rows(dml);
        self.apply_write_group(writes, summary)
    }

    /// Apply an expanded statement's per-key writes as a single **atomic
    /// group** — the same [`apply_group`](Self::apply_group) →
    /// [`finish_group_commit`](Self::finish_group_commit) machinery a
    /// multi-statement `COMMIT` uses ([STL-192]). All writes target one table,
    /// so the commit is the single-record fast path: one WAL record, one fsync.
    /// A failure applying any write of the set discards the group
    /// ([`abort_group`](stele_storage::engine::Engine::abort_group)) — nothing
    /// is made durable and the in-memory tiers are rolled back ([STL-216]), so
    /// the statement leaves the table unchanged.
    ///
    /// An empty set opens no group and writes no WAL record; the summary is
    /// reported unchanged.
    fn apply_write_group(
        &mut self,
        writes: Vec<BoundDml>,
        summary: DmlSummary,
    ) -> Result<StatementOutcome, EngineError> {
        if !writes.is_empty() {
            let txn_id = TxnId(self.next_txn);
            self.next_txn += 1;
            let principal = self.write_principal.clone();
            let mut touched: Vec<String> = Vec::new();
            let result = match self.apply_group(writes, txn_id, &principal, &mut touched) {
                Ok(()) => self.finish_group_commit(txn_id, &touched),
                Err(e) => {
                    // Mid-set failure: discard the group so nothing is made
                    // durable and the already-applied prefix is undone in memory
                    // — the statement is all-or-none ([STL-216]).
                    for table in &touched {
                        if let Ok(state) = self.table_mut(table) {
                            state.engine.abort_group();
                        }
                    }
                    Err(e)
                }
            };
            // The same steady-state prune point as a single auto-committed write
            // ([`apply_dml`](Self::apply_dml), [STL-204]).
            self.prune_write_index();
            result?;
        }
        Ok(StatementOutcome::Dml(summary))
    }

    /// Expand a scan-then-write `UPDATE` / `DELETE` ([STL-229]) into the per-key
    /// point writes it stands for, reporting the affected-row summary alongside.
    ///
    /// Runs the statement's `WHERE` as a key-projecting snapshot read through the
    /// **same** [`run_select`](Self::run_select) path a `SELECT` takes — so the
    /// predicate selects exactly the rows the equivalent `SELECT` returns: the
    /// fused scan+filter on committed-only state, the buffered-write overlay
    /// inside a transaction (read-your-own-writes, [STL-203]), and the
    /// valid-time payload framing ([STL-218]) all behave identically. Each
    /// matched row's business key is decoded back to its typed value and becomes
    /// one [`BoundDml::Update`] / [`BoundDml::Delete`]; an `UPDATE`'s matched keys
    /// all carry the same `SET` assignments (and, on a valid-time table, the same
    /// new `[from, to)` period — the same posture as the point write, [STL-194]).
    ///
    /// The keys are sorted by their canonical encoding, so the expansion — and
    /// with it the group's WAL record — is deterministic regardless of scan
    /// order. A system-time snapshot resolves at most one live version per key,
    /// so the matched keys are distinct; the summary counts them.
    ///
    /// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
    // The scan-probe `BoundSelect` gained the CTE/derived-table fields ([STL-242]),
    // nudging this one-piece expansion just past the line limit; it reads as one
    // sequence, so splitting it would scatter rather than clarify it.
    #[allow(clippy::too_many_lines)]
    fn expand_scan_dml(
        &self,
        dml: BoundDml,
        snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<(Vec<BoundDml>, DmlSummary), EngineError> {
        let (table, schema_id, filter) = match &dml {
            BoundDml::UpdateScan {
                table,
                schema_id,
                filter,
                ..
            }
            | BoundDml::DeleteScan {
                table,
                schema_id,
                filter,
            } => (table.clone(), *schema_id, filter.clone()),
            // The router and `stage_dml` only pass the scan variants here.
            _ => {
                return Err(EngineError::Unsupported(
                    "only a scan-then-write UPDATE/DELETE expands",
                ));
            }
        };

        // `bind_dml` already proved the table resolves at this snapshot with at
        // least the key column, so a miss is an internal contract break — surface
        // it rather than panic (the same posture as `run_select`).
        let schema = self
            .catalog
            .resolve(&table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(table.clone()))?;
        let key_col = schema
            .columns()
            .first()
            .ok_or_else(|| EngineError::UnknownTable(table.clone()))?;
        let key_ty = key_col.ty();

        // The statement's WHERE, run exactly as a `SELECT <key> FROM t WHERE …`
        // at the statement snapshot. The filter is evaluated over the full
        // reconstructed rows before this key-only projection applies, so a
        // value-column predicate works unchanged.
        let scan = BoundSelect {
            table: table.clone(),
            schema_id,
            snapshot,
            valid_snapshot: None,
            // A range scan is never lowered to one of these synthetic DML
            // probe/scan plans ([STL-244]).
            system_range: None,
            valid_range: None,
            projection: Projection::Items(vec![ProjectionItem::column(key_col.name())]),
            filter,
            period_filter: None,
            subquery_filter: None,
            aggregate: None,
            join: None,
            ctes: Vec::new(),
            relation_columns: None,
            // DML row selection takes no result shaping — every match writes.
            distinct: false,
            order_by: Vec::new(),
            offset: 0,
            limit: None,
        };
        let StatementOutcome::Rows(matched) = self.run_select(&scan, overlay)? else {
            return Err(EngineError::Unsupported(
                "the scan-then-write expansion read returned no row set",
            ));
        };

        // One live version per key at a system snapshot, so the matched keys are
        // distinct; sorting by the canonical encoding makes the apply order (and
        // the group's WAL record) deterministic regardless of scan order.
        let mut key_cells: Vec<Vec<u8>> = matched
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .next()
                    .flatten()
                    .ok_or(EngineError::MalformedBusinessKey)
            })
            .collect::<Result<_, _>>()?;
        key_cells.sort_unstable();

        let keys: Vec<ScalarValue> = key_cells
            .iter()
            .map(|bytes| {
                ScalarValue::decode(key_ty, bytes).map_err(|_| EngineError::MalformedBusinessKey)
            })
            .collect::<Result<_, _>>()?;

        let count = keys.len() as u64;
        let (writes, summary) = match dml {
            BoundDml::UpdateScan {
                table,
                schema_id,
                assignments,
                valid,
                ..
            } => (
                keys.into_iter()
                    .map(|key| BoundDml::Update {
                        table: table.clone(),
                        schema_id,
                        key,
                        assignments: assignments.clone(),
                        valid,
                    })
                    .collect(),
                DmlSummary::Update(count),
            ),
            BoundDml::DeleteScan {
                table, schema_id, ..
            } => (
                keys.into_iter()
                    .map(|key| BoundDml::Delete {
                        table: table.clone(),
                        schema_id,
                        key,
                    })
                    .collect(),
                DmlSummary::Delete(count),
            ),
            // Unreachable: the match above already rejected every other variant.
            _ => unreachable!("expand_scan_dml only receives the scan variants"),
        };
        Ok((writes, summary))
    }

    /// The `MERGE` probe set: which target business keys (encoded) are live at
    /// the statement snapshot, for the arm resolution in
    /// [`expand_merge`](Self::expand_merge) to test membership against.
    ///
    /// When `probe_per_key`, probe each source key as a point read
    /// ([`merge_key_is_live`](Self::merge_key_is_live)) — the always-indexed
    /// business key prunes to the segments the per-segment blooms admit
    /// ([STL-238]), so the probe reads no more of the keyspace than the source
    /// touches. Otherwise read every live key in a single scan
    /// (`SELECT <key> FROM t`). Both yield the same `live ∩ source` membership, so
    /// the upsert is identical either way — the caller's
    /// [`merge_should_probe_per_key`](Self::merge_should_probe_per_key) cost
    /// estimate picks the faster plan, never a different result ([STL-312]).
    fn merge_live_keys(
        &self,
        merge: &BoundMerge,
        key_name: &str,
        snapshot: SystemTimeMicros,
        rows: &[Vec<Option<ScalarValue>>],
        probe_per_key: bool,
        overlay: &[BoundDml],
    ) -> Result<HashSet<Vec<u8>>, EngineError> {
        if probe_per_key {
            let mut live = HashSet::new();
            for row in rows {
                let Some(key) = row.get(merge.on).and_then(|c| c.as_ref()) else {
                    continue; // a NULL join key matches nothing — never live
                };
                let encoded = encode_value(key);
                if !live.contains(&encoded)
                    && self.merge_key_is_live(merge, key_name, snapshot, key, overlay)?
                {
                    live.insert(encoded);
                }
            }
            return Ok(live);
        }
        let probe = BoundSelect {
            table: merge.table.clone(),
            schema_id: merge.schema_id,
            snapshot,
            valid_snapshot: None,
            // A range scan is never lowered to one of these synthetic DML
            // probe/scan plans ([STL-244]).
            system_range: None,
            valid_range: None,
            projection: Projection::Items(vec![ProjectionItem::column(key_name)]),
            filter: None,
            period_filter: None,
            subquery_filter: None,
            aggregate: None,
            join: None,
            ctes: Vec::new(),
            relation_columns: None,
            distinct: false,
            order_by: Vec::new(),
            offset: 0,
            limit: None,
        };
        let StatementOutcome::Rows(live) = self.run_select(&probe, overlay)? else {
            return Err(EngineError::Unsupported(
                "the MERGE probe read returned no row set",
            ));
        };
        live.rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .next()
                    .flatten()
                    .ok_or(EngineError::MalformedBusinessKey)
            })
            .collect()
    }

    /// Expand a `MERGE` plan ([STL-230]) into the per-key point writes it stands
    /// for, reporting the acted-on-row summary alongside.
    ///
    /// The probe and the source read both run through the **same**
    /// [`run_select`](Self::run_select) path a `SELECT` takes at the statement
    /// snapshot — committed-only state on the auto-commit path, the buffered-write
    /// overlay inside a transaction (read-your-own-writes, [STL-203]). Each source
    /// row resolves its arm against the target's live keys ([`merge_live_keys`](Self::merge_live_keys)):
    /// matched ⇒ one [`BoundDml::Update`] from the `WHEN MATCHED` template,
    /// unmatched ⇒ one [`BoundDml::Insert`] from the `WHEN NOT MATCHED` template (a
    /// row whose arm is absent is skipped). A `NULL` join key matches nothing — SQL
    /// equality — and a `NULL` resolving into the **inserted business key** fails
    /// the statement.
    ///
    /// Two source rows resolving to the same target row are refused with
    /// [`EngineError::MergeRowTwice`] — the standard's deterministic posture —
    /// *before* any write applies. The writes are keyed (and therefore ordered) by
    /// the canonical key encoding, so the expansion — and with it the group's WAL
    /// record — is deterministic regardless of source order.
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    fn expand_merge(
        &self,
        merge: &BoundMerge,
        snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<(Vec<BoundDml>, DmlSummary), EngineError> {
        // `bind_merge` already proved the target resolves at this snapshot with at
        // least the key column — a miss is an internal contract break, surfaced
        // rather than panicking (the same posture as `expand_scan_dml`).
        let schema = self
            .catalog
            .resolve(&merge.table, snapshot)
            .ok_or_else(|| EngineError::UnknownTable(merge.table.clone()))?;
        let key_name = schema
            .columns()
            .first()
            .map(|c| c.name().to_owned())
            .ok_or_else(|| EngineError::UnknownTable(merge.table.clone()))?;
        // The valid axis (if any) names the period columns the per-row interval
        // bounds resolve against ([STL-308]); `None` on a system-only table, where
        // both arms' `*_valid` descriptors are also `None`.
        let period = schema.temporal().valid_time();

        let rows = self.merge_source_rows(&merge.source, snapshot, overlay)?;
        // Cost-based plan choice ([STL-312]): point-probe each source key when the
        // source touches fewer keys than the live keyspace holds, else read every
        // live key once. The probe path reads one key per *distinct, non-NULL* join
        // key (a NULL never matches; a repeat is probed once), so the cost compares
        // that count — not the raw source-row count — against the keyspace.
        let probe_keys: HashSet<Vec<u8>> = rows
            .iter()
            .filter_map(|row| row.get(merge.on).and_then(|c| c.as_ref()))
            .map(encode_value)
            .collect();
        let probe_per_key = self.merge_should_probe_per_key(&merge.table, probe_keys.len());
        let live =
            self.merge_live_keys(merge, &key_name, snapshot, &rows, probe_per_key, overlay)?;

        // Resolve each source row's arm. Keying the writes by the canonical key
        // encoding both rejects a second write to one target row and fixes the
        // apply order deterministically.
        let mut writes: BTreeMap<Vec<u8>, BoundDml> = BTreeMap::new();
        for row in &rows {
            let joined = row
                .get(merge.on)
                .ok_or(EngineError::MalformedMergeSource)?
                .as_ref();
            // SQL equality: a NULL join key matches nothing — the row is
            // unmatched. Encode the join key once and reuse it for the probe and,
            // on the matched arm, the write key (which *is* the join key there).
            let probe = joined.map(|key| (key, encode_value(key)));
            let matched = probe.as_ref().filter(|(_, encoded)| live.contains(encoded));
            let (write_key, write) = if let Some((key, encoded)) = matched {
                let Some(assignments) = &merge.matched else {
                    continue;
                };
                let assignments = assignments
                    .iter()
                    .map(|(idx, value)| (*idx, resolve_merge_value(value, row)))
                    .collect();
                (
                    encoded.clone(),
                    BoundDml::Update {
                        table: merge.table.clone(),
                        schema_id: merge.schema_id,
                        key: (*key).clone(),
                        assignments,
                        // On a valid-time table the matched arm closes the prior
                        // version and opens a new one over this interval ([STL-235]),
                        // derived per source row ([STL-308]); `None` for a
                        // system-only table.
                        valid: resolve_arm_valid(
                            merge.matched_valid.as_ref(),
                            period,
                            row,
                            &merge.table,
                        )?,
                    },
                )
            } else {
                let Some(template) = &merge.not_matched else {
                    continue;
                };
                let mut cells = template.iter().map(|value| resolve_merge_value(value, row));
                // The template is aligned to all target columns, key first; a
                // NULL resolving into the key can never insert. The inserted key
                // need not equal the join key, so it is encoded separately.
                let key = cells.next().flatten().ok_or_else(|| {
                    EngineError::Dml(DmlError::NullValue {
                        table: merge.table.clone(),
                        column: key_name.clone(),
                    })
                })?;
                let values: Vec<Option<ScalarValue>> = cells.collect();
                (
                    encode_value(&key),
                    BoundDml::Insert {
                        table: merge.table.clone(),
                        schema_id: merge.schema_id,
                        key,
                        values,
                        // The not-matched arm inserts with the arm's valid interval
                        // ([STL-235]), derived per source row ([STL-308]); `None`
                        // for a system-only table.
                        valid: resolve_arm_valid(
                            merge.not_matched_valid.as_ref(),
                            period,
                            row,
                            &merge.table,
                        )?,
                    },
                )
            };
            if writes.insert(write_key, write).is_some() {
                return Err(EngineError::MergeRowTwice);
            }
        }
        let count = writes.len() as u64;
        Ok((writes.into_values().collect(), DmlSummary::Merge(count)))
    }

    /// Materialize a `MERGE`'s source rows ([STL-230]): a `VALUES` source was
    /// folded to typed rows at bind; a table source is read here — at the same
    /// snapshot (+ overlay) the probe uses — and decoded back to typed cells by
    /// its declared column types.
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    fn merge_source_rows(
        &self,
        source: &MergeSource,
        snapshot: SystemTimeMicros,
        overlay: &[BoundDml],
    ) -> Result<Vec<Vec<Option<ScalarValue>>>, EngineError> {
        match source {
            MergeSource::Values(rows) => Ok(rows.clone()),
            MergeSource::Table {
                name,
                schema_id,
                columns,
            } => {
                let scan = BoundSelect {
                    table: name.clone(),
                    schema_id: *schema_id,
                    snapshot,
                    valid_snapshot: None,
                    system_range: None,
                    valid_range: None,
                    projection: Projection::Items(
                        columns
                            .iter()
                            .map(|(name, _)| ProjectionItem::column(name.clone()))
                            .collect(),
                    ),
                    filter: None,
                    period_filter: None,
                    subquery_filter: None,
                    aggregate: None,
                    join: None,
                    ctes: Vec::new(),
                    relation_columns: None,
                    distinct: false,
                    order_by: Vec::new(),
                    offset: 0,
                    limit: None,
                };
                let StatementOutcome::Rows(result) = self.run_select(&scan, overlay)? else {
                    return Err(EngineError::Unsupported(
                        "the MERGE source read returned no row set",
                    ));
                };
                result
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .zip(columns)
                            .map(|(cell, (_, ty))| {
                                cell.map(|bytes| ScalarValue::decode(*ty, &bytes))
                                    .transpose()
                                    .map_err(|_| EngineError::MalformedMergeSource)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect()
            }
        }
    }

    /// The oldest system-time snapshot pinned by a currently-open transaction, or
    /// `None` when none is open. The floor [`prune_write_index`](Self::prune_write_index)
    /// keeps the write index above ([STL-204]).
    fn oldest_live_snapshot(&self) -> Option<SystemTimeMicros> {
        SnapshotLease::lock(&self.open_snapshots)
            .keys()
            .next()
            .copied()
    }

    /// Drop write-index entries that can no longer produce a write-write conflict,
    /// bounding the index on a long-lived server ([STL-204], [ADR-0008]).
    ///
    /// A conflict requires a write committed *strictly after* some open
    /// transaction's pinned snapshot (`committed_at > snapshot`), so any entry at or
    /// below the **oldest** live snapshot can never conflict with that transaction —
    /// nor any newer one, whose snapshot is at least as high. This drops every entry
    /// committed *strictly below* that snapshot (`retain(committed_at >= floor)`);
    /// the at-most-one-instant's worth sitting exactly at it is kept — harmless, and
    /// matching the ticket's "strictly below" wording. When no transaction is open
    /// the whole index goes: every future transaction pins at or past the current
    /// instant, which is at or past every recorded write.
    ///
    /// The `pruned_below` guard skips the (O(index)) scan when the floor has not
    /// risen since the last prune, so steady auto-commit traffic under a single
    /// long-lived open transaction stays cheap — the index can only grow with keys
    /// that *could* still conflict with that transaction, and is reclaimed the
    /// moment it ends.
    fn prune_write_index(&mut self) {
        match self.oldest_live_snapshot() {
            // No open reader: nothing recorded can ever conflict again.
            None => {
                self.write_index.clear();
                self.pruned_below = self.clock.current();
            }
            // The floor rose: drop everything strictly below the oldest live
            // snapshot. (Entries exactly at it are kept — they cannot conflict with
            // it, but the conservative `>=` bound matches the ticket's wording and
            // keeps at most one instant's worth of harmless extra entries.)
            Some(floor) if floor > self.pruned_below => {
                self.write_index
                    .retain(|_, &mut committed_at| committed_at >= floor);
                self.pruned_below = floor;
            }
            // The floor has not advanced since the last prune — nothing new to drop.
            Some(_) => {}
        }
    }

    /// Apply one already-bound DML operation under the given provenance, reporting
    /// the affected-row count. The shared core of the auto-commit path
    /// ([`apply_dml`](Self::apply_dml)) and the multi-statement commit path
    /// ([`commit`](Self::commit)) — the latter passes one `txn_id` for every write
    /// in the transaction, so they share provenance.
    ///
    /// The row's value columns are folded to bytes with
    /// [`ScalarValue::encode`] and packed into the stored payload by the
    /// [row codec](stele_common::row_codec) — the inverse of the decode
    /// [`run_select`](Self::run_select) applies — so an `INSERT`ed row round-trips
    /// through a later `SELECT`. An `UPDATE` is a read-modify-write: it starts
    /// from the live row's value cells, overwrites the assigned columns, and
    /// re-packs, so columns the `SET` did not name keep their prior value. `seq`
    /// is `0`: the commit clock hands each write a distinct `sys_from`, so the
    /// per-commit tiebreak never decides between two versions.
    ///
    /// On a valid-time table the bound `[from, to)` interval ([STL-194]) rides
    /// down to the storage writer, which frames it onto the stored payload; the
    /// period columns also sit in the row codec as their `Timestamp` cells. An
    /// `UPDATE`'s read-modify-write therefore reads the prior row through
    /// [`live_value_cells`](Self::live_value_cells), which strips that frame before
    /// decoding.
    fn apply_bound_dml(
        &mut self,
        dml: BoundDml,
        txn_id: TxnId,
        principal: &Principal,
    ) -> Result<DmlSummary, EngineError> {
        // A multi-row INSERT ([STL-228]), a scan-then-write variant ([STL-229]),
        // or a MERGE plan ([STL-230]) stands for several writes, not one keyed
        // write — each is expanded into per-key writes *before* it can reach an
        // apply (`apply_insert_rows` / `apply_scan_dml` / `apply_merge` /
        // `stage_dml`), so one arriving here is an internal contract break. Refuse
        // it rather than write something wrong.
        if matches!(
            dml,
            BoundDml::InsertRows { .. }
                | BoundDml::UpdateScan { .. }
                | BoundDml::DeleteScan { .. }
                | BoundDml::Merge(_)
        ) {
            return Err(EngineError::Unsupported(
                "a multi-row INSERT / scan-then-write UPDATE/DELETE / MERGE must be expanded before it is applied",
            ));
        }
        // The (table, business key) this write commits, captured before the match
        // consumes `dml`, so its commit instant can be recorded for conflict
        // detection once the write lands.
        let committed = (dml.table().to_owned(), dml_business_key(&dml));
        let summary = match dml {
            BoundDml::Insert {
                table,
                key,
                values,
                valid,
                ..
            } => {
                // The bound row width must still match the live schema — DDL could
                // have changed it since binding (drop/re-create between staging and
                // committing a transaction). Refuse rather than write a payload the
                // current schema cannot decode.
                let value_count = self.value_column_count(&table)?;
                if values.len() != value_count {
                    return Err(EngineError::SchemaChanged {
                        table,
                        live: value_count,
                        bound: values.len(),
                    });
                }
                let cells: Vec<Option<Vec<u8>>> = values
                    .iter()
                    .map(|v| v.as_ref().map(encode_value))
                    .collect();
                self.insert(
                    &table,
                    business_key(&key),
                    valid.map(to_valid_interval),
                    row_codec::encode_payload(&cells),
                    0,
                    txn_id,
                    principal.clone(),
                )?;
                // The write is applied; note its cells into the table's live
                // access structures ([STL-233]) so a probe at any later
                // snapshot finds this key (the superset contract).
                self.note_indexes(&table, &committed.1, &cells);
                DmlSummary::Insert(1)
            }
            BoundDml::Update {
                table,
                key,
                assignments,
                valid,
                ..
            } => {
                // Read-modify-write: merge the SET overrides onto the live row's
                // value cells so unnamed columns keep their prior value, then
                // re-pack. The base is read at the committed state, which — for a
                // key that passed `commit`'s write-write conflict check — is
                // unchanged since this transaction's snapshot. (In a group commit an
                // earlier staged write of the same key is already applied to the
                // delta, so a later UPDATE reads it — front-to-back ordering. This is
                // the apply path; a mid-transaction SELECT reads the buffer via the
                // overlay instead, [STL-203].)
                let value_count = self.value_column_count(&table)?;
                // Guard against a narrowed schema since binding: an assignment
                // index past the live value columns would otherwise panic on the
                // `cells[*idx]` write below.
                if let Some(&(idx, _)) = assignments.iter().find(|(idx, _)| *idx >= value_count) {
                    return Err(EngineError::SchemaChanged {
                        table,
                        live: value_count,
                        bound: idx + 1,
                    });
                }
                let key = business_key(&key);
                let mut cells = self.live_value_cells(&table, &key, value_count)?;
                for (idx, value) in &assignments {
                    cells[*idx] = value.as_ref().map(encode_value);
                }
                self.update(
                    &table,
                    key,
                    valid.map(to_valid_interval),
                    row_codec::encode_payload(&cells),
                    0,
                    txn_id,
                    principal.clone(),
                )?;
                // Note the *merged* row's cells ([STL-233]). Add-only: the
                // prior value's entry deliberately stays — a past snapshot may
                // still see it (the superset contract).
                self.note_indexes(&table, &committed.1, &cells);
                DmlSummary::Update(1)
            }
            BoundDml::Delete { table, key, .. } => {
                self.delete(&table, &business_key(&key), txn_id, principal.clone())?;
                DmlSummary::Delete(1)
            }
            BoundDml::InsertRows { .. }
            | BoundDml::UpdateScan { .. }
            | BoundDml::DeleteScan { .. }
            | BoundDml::Merge(_) => {
                unreachable!("rejected at the top of apply_bound_dml")
            }
        };
        // Record this key's commit instant for first-committer-wins conflict
        // detection (ADR-0008). The write advanced the commit clock to its
        // `sys_from`, so the high-water mark is this write's commit instant — the
        // latest in a multi-statement commit, a conservative upper bound any
        // transaction whose pinned snapshot precedes it will conflict against. Both
        // the auto-commit path and a multi-statement `COMMIT` funnel through here,
        // so every committed write is tracked.
        self.write_index.insert(committed, self.clock.current());
        Ok(summary)
    }

    /// Note one committed row's indexed cells into the table's live access
    /// structures ([STL-233]) — the DML maintenance half of the superset
    /// contract (the `secondary` module docs). Called after every applied
    /// `INSERT` / `UPDATE` (both the auto-commit path and a multi-statement
    /// `COMMIT` funnel through [`apply_bound_dml`](Self::apply_bound_dml)); a
    /// `DELETE` notes nothing — the structures are add-only, and a delete
    /// introduces no new value. Infallible by design: the write is already
    /// applied when this runs, so there is nothing sound to do with an error —
    /// and none arises, since the catalog validated the indexed columns at
    /// `CREATE INDEX` and the schema is append-only.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    fn note_indexes(&mut self, table: &str, key: &BusinessKey, cells: &[Option<Vec<u8>>]) {
        if self.index_states.is_empty() {
            return;
        }
        let Some(schema) = self.catalog.resolve(table, self.clock.current()) else {
            // The write that just applied resolved this table; a miss here is
            // unreachable, and noting nothing only costs a probe its prune.
            return;
        };
        let columns = schema.columns();
        let targets: Vec<(String, usize)> = self
            .catalog
            .indexes_on(table)
            .filter_map(|def| {
                let position = columns
                    .iter()
                    .position(|c| Some(c.name()) == def.columns().first().map(String::as_str))?;
                Some((def.name().to_owned(), position))
            })
            .collect();
        for (name, position) in targets {
            // Schema position 0 is the business key; the value cells are
            // offset by one. A NULL cell is never noted — an equality probe
            // can never match it (three-valued logic).
            let Some(cell) = position
                .checked_sub(1)
                .and_then(|i| cells.get(i))
                .and_then(|c| c.as_deref())
            else {
                continue;
            };
            if let Some(state) = self.index_states.get_mut(&name) {
                state.structure.note(cell, key);
            }
        }
    }

    /// The number of value columns (the schema's column count minus the business
    /// key) for `table`, resolved at the current snapshot. Drives the row codec.
    fn value_column_count(&self, table: &str) -> Result<usize, EngineError> {
        let schema = self
            .catalog
            .resolve(table, self.clock.current())
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        Ok(schema.columns().len().saturating_sub(1))
    }

    /// The live row's value cells for `key` at the current snapshot, sliced out of
    /// its payload by the [row codec](stele_common::row_codec) — or an all-`NULL`
    /// row when `key` is not live (so an `UPDATE` of an absent key opens a fresh
    /// row whose unset columns are `NULL`). The starting point for an `UPDATE`'s
    /// read-modify-write merge.
    ///
    /// On a valid-time table the prior version's stored payload framing depends on
    /// which tier resolves it: a delta row carries the 16-byte interval prefix on
    /// its payload ([`frame_payload`](stele_storage::validtime::frame_payload),
    /// [STL-194]), whereas a sealed segment stores the payload **bare** with the
    /// interval lifted into its own `valid_from` / `valid_to` columns ([STL-163]).
    /// The scan therefore runs with [`valid_time`](SnapshotScan::valid_time) set to
    /// the table's policy, which strips the delta frame and reads the sealed payload
    /// bare, emitting the bare user payload uniformly across tiers ([STL-218]); the
    /// row codec then decodes it directly. Stripping a fixed prefix here instead
    /// would corrupt a sealed prior version's real row data ([STL-226]).
    fn live_value_cells(
        &self,
        table: &str,
        key: &BusinessKey,
        value_count: usize,
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        if value_count == 0 {
            return Ok(Vec::new());
        }
        let state = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))?;
        let snapshot = self.clock.current();
        let readers = state.engine.open_segment_readers()?;
        let out = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        // Emit the bare user payload regardless of which tier resolves the prior
        // version: with the table's valid-time policy set, the scan strips the
        // delta tier's framed interval prefix and reads the sealed tier's
        // already-bare payload, so the row codec below decodes one consistent shape
        // ([STL-218], [STL-226]). For a system-only table this is `false` and the
        // payload (already bare) is untouched.
        .valid_time(state.valid_time)
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: ZoneBound::Bytes(key.as_bytes().to_vec()),
        })
        .execute()?;
        // The key resolves to at most one live version; take its (now bare) payload
        // (the Eq predicate narrows the scan, but re-match the key defensively).
        let payload = (0..out.batch.rows)
            .find(|&r| {
                column_cell(&out.batch, ColumnId::BusinessKey, r).as_deref() == Some(key.as_bytes())
            })
            .and_then(|r| column_cell(&out.batch, ColumnId::Payload, r));
        Ok(row_codec::decode_payload(value_count, payload.as_deref())?)
    }

    /// Begin a multi-statement transaction — an empty write buffer the caller
    /// feeds with [`stage_dml`](Self::stage_dml) / [`execute_in_txn`](Self::execute_in_txn)
    /// and applies with [`commit`](Self::commit) ([STL-174]).
    ///
    /// The transaction's **read snapshot is pinned here, at `BEGIN`** — the commit
    /// clock's current instant — so every statement in the block reads one
    /// consistent system-time snapshot under snapshot isolation ([STL-175],
    /// [ADR-0008]).
    ///
    /// The transaction is held *per connection* (the pgwire front end owns one per
    /// session), not on the shared engine, so two connections' open transactions
    /// stay independent. No transaction id is allocated until
    /// [`commit`](Self::commit), so a `BEGIN` followed by `ROLLBACK` (or a
    /// read-only transaction) consumes none.
    ///
    /// By default the transaction reads under [`IsolationLevel::RepeatableRead`]
    /// (snapshot isolation); [`begin_with_isolation`](Self::begin_with_isolation)
    /// selects [`IsolationLevel::ReadCommitted`] instead ([STL-248]).
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    #[must_use]
    pub fn begin(&self) -> SessionTransaction {
        self.begin_with_isolation(IsolationLevel::default())
    }

    /// Begin a transaction reading under a chosen [`IsolationLevel`] — the engine
    /// path for `BEGIN ISOLATION LEVEL …` ([STL-248]). [`begin`](Self::begin) is the
    /// default-level shorthand.
    ///
    /// The snapshot is pinned here as in [`begin`](Self::begin); the level only
    /// governs whether [`execute_in_txn`](Self::execute_in_txn) re-pins it per
    /// statement ([`IsolationLevel::ReadCommitted`]) or holds it for the whole block
    /// ([`IsolationLevel::RepeatableRead`], the default).
    #[must_use]
    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> SessionTransaction {
        // Pin at transaction-start time (clock observed fresh, [STL-227]):
        // `now()` inside the block is the `BEGIN` instant, Postgres-style, and
        // every later commit lands strictly past the pin (`observe` folds the
        // reading into the high-water mark), so the snapshot stays consistent.
        let snapshot = self.clock.observe();
        SessionTransaction {
            snapshot,
            isolation,
            writes: Vec::new(),
            savepoints: Vec::new(),
            // Register the pinned snapshot so it holds the prune floor down for as
            // long as this transaction is open ([STL-204]). `begin` is `&self`, but
            // the multiset is behind its own lock, so no `&mut self` is needed.
            lease: SnapshotLease::new(Arc::clone(&self.open_snapshots), snapshot),
        }
    }

    /// Bind a DML statement and **buffer** it into `txn` without applying it,
    /// returning the affected-row summary the wire client expects for its
    /// `CommandComplete`. Returns `Ok(None)` if `stmt` is not an
    /// `INSERT`/`UPDATE`/`DELETE` — a `SELECT` or DDL inside a transaction routes
    /// through [`execute_in_txn`](Self::execute_in_txn), which runs it at once
    /// against the pinned snapshot (the buffer stays write-only, [STL-174]).
    ///
    /// Binding here folds the statement's literals and resolves its table against
    /// the catalog at the transaction's **pinned snapshot** ([STL-175]) — so the
    /// whole block binds under one consistent schema view — and only the
    /// *application* is deferred to [`commit`](Self::commit).
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    ///
    /// # Errors
    ///
    /// [`EngineError::Dml`] if the statement is malformed DML (unknown table or
    /// column, a bad literal); the statement is rejected and nothing is buffered.
    pub fn stage_dml(
        &self,
        stmt: &Statement,
        txn: &mut SessionTransaction,
    ) -> Result<Option<DmlSummary>, EngineError> {
        let ctx = BindContext {
            snapshot: txn.snapshot,
            catalog: &self.catalog,
        };
        match bind_dml(stmt, &ctx) {
            // A scan-then-write UPDATE / DELETE ([STL-229]) expands **now**, at
            // the statement: the matching live keys are enumerated at the pinned
            // snapshot with the transaction's own buffered writes overlaid
            // (read-your-own-writes, [STL-203] — an INSERT staged earlier in the
            // block is matchable), and the resulting per-key writes are what the
            // buffer holds. So the tag reports the rows matched *at statement
            // time*, later statements in the block cannot retroactively change
            // this statement's row set, and everything downstream of the buffer
            // (overlay reads, savepoint truncation, conflict detection, commit)
            // only ever sees per-key writes.
            Ok(dml @ (BoundDml::UpdateScan { .. } | BoundDml::DeleteScan { .. })) => {
                let (writes, summary) = self.expand_scan_dml(dml, txn.snapshot, &txn.writes)?;
                txn.writes.extend(writes);
                Ok(Some(summary))
            }
            // A MERGE expands at staging the same way ([STL-230]): the probe and
            // the source read resolve at the pinned snapshot with the
            // transaction's own buffered writes overlaid (read-your-own-writes,
            // [STL-203]), and the buffer only ever holds the per-key writes.
            Ok(BoundDml::Merge(merge)) => {
                let (writes, summary) = self.expand_merge(&merge, txn.snapshot, &txn.writes)?;
                txn.writes.extend(writes);
                Ok(Some(summary))
            }
            // A multi-row INSERT ([STL-228]) expands at staging into one buffered
            // point INSERT per row, so the buffer only ever holds per-key writes:
            // read-your-own-writes overlay ([STL-203]), savepoint truncation,
            // conflict detection, and the group commit each see N inserts and
            // commit them as one atomic group. The tag reports the row count now.
            Ok(dml @ BoundDml::InsertRows { .. }) => {
                let (writes, summary) = expand_insert_rows(dml);
                txn.writes.extend(writes);
                Ok(Some(summary))
            }
            Ok(dml) => {
                // A point UPDATE / DELETE reports the rows it will affect: an
                // absent key is a 0-row no-op (`UPDATE 0` / `DELETE 0`, [STL-294]),
                // not the storage writers' `KeyNotFound`. Probe at the pinned
                // snapshot with the transaction's own buffered writes overlaid
                // (read-your-own-writes, [STL-203]) so the staged tag is exact, and
                // leave a no-op **unbuffered** — it stays out of the write set, so
                // COMMIT neither applies it nor lets it abort the block on a
                // concurrent writer. A point INSERT (and a live point write) buffers
                // as before.
                if let Some(summary) = self.absent_point_tag(&dml, txn.snapshot, &txn.writes)? {
                    return Ok(Some(summary));
                }
                let summary = dml_summary(&dml);
                txn.writes.push(dml);
                Ok(Some(summary))
            }
            Err(DmlError::NotDml) => Ok(None),
            Err(e) => Err(EngineError::Dml(e)),
        }
    }

    /// Apply a transaction's buffered writes as a unit, stamping every one with a
    /// single shared transaction id, and report success ([STL-174]). Dropping the
    /// [`SessionTransaction`] instead of calling this rolls the transaction back —
    /// the buffer is discarded and no effect reaches storage.
    ///
    /// One `txn_id` is allocated for the whole transaction, so every row it writes
    /// carries the same provenance — the property that makes the writes one
    /// logical commit. The writes are applied in staged order through the same
    /// typed path the auto-commit route uses.
    ///
    /// ## Snapshot-isolation conflict detection ([STL-175], [ADR-0008])
    ///
    /// Before any write is applied, the transaction's write set is checked against
    /// the engine's per-key MVCC write index: if any key it writes was committed by
    /// another transaction *after* this one's pinned snapshot, this transaction
    /// lost the race (first committer wins) and the commit is refused with
    /// [`EngineError::Conflict`] — a retryable error — having touched nothing.
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    ///
    /// ## Crash-atomic group commit ([STL-192], [STL-215])
    ///
    /// The buffered writes are not replayed one WAL record at a time. Each table the
    /// transaction touches is put in **group-commit** mode
    /// ([`Engine::begin_group`](stele_storage::engine::Engine::begin_group)): its
    /// writes apply to the delta/index in staged order (so a later write sees an
    /// earlier one) but their redos accumulate into one WAL record per table — the
    /// atomic unit recovery replays whole or, if a crash tears it, drops at the
    /// durable fence. If applying a write fails, every touched table's buffer is
    /// discarded ([`abort_group`](stele_storage::engine::Engine::abort_group)) so
    /// nothing is made durable.
    ///
    /// A **single-table** transaction commits that one record with a single fsync —
    /// the record boundary *is* the transaction boundary, so it recovers all-or-none
    /// with no extra coordination. A transaction spanning **multiple tables** writes
    /// one record per table (each table owns its WAL), so a crash *between* two
    /// tables' commits could otherwise leave one durable and the other not. To make
    /// the whole transaction atomic, each table's record is committed as a
    /// **two-phase** leg ([STL-215]) — durable but inert — and a single commit marker
    /// naming the transaction is fsynced to the engine commit log only after every
    /// leg is durable. Recovery replays a leg only if that marker is present, so the
    /// transaction recovers all-or-none across **every** table it wrote.
    ///
    /// # Errors
    ///
    /// [`EngineError::Conflict`] if a concurrent commit modified this transaction's
    /// write set after its snapshot (retry the transaction). Otherwise
    /// [`EngineError`] if applying any buffered write fails (e.g. its table was
    /// dropped between staging and commit) or a group-commit append/fsync fails. A
    /// failure before the fsync makes nothing durable (a torn record is dropped on
    /// recovery); a *failed fsync after a successful append* leaves the staged record's
    /// durability **indeterminate**, so it is treated as a crash, not a clean abort:
    /// the WAL poisons and the session refuses further writes
    /// ([`is_poisoned`](Self::is_poisoned)) until [`recover`](Self::recover) resolves
    /// it from the log (the WAL contract, [STL-217]). A write already applied to the
    /// in-memory tiers when a *later* one fails is not yet rolled back in memory
    /// ([STL-216]).
    pub fn commit(&mut self, txn: SessionTransaction) -> Result<(), EngineError> {
        // First-committer-wins write-write conflict detection. Checked up front, so
        // a conflict aborts the whole transaction before any write lands.
        for dml in &txn.writes {
            let key = (dml.table().to_owned(), dml_business_key(dml));
            if self
                .write_index
                .get(&key)
                .is_some_and(|&committed_at| committed_at > txn.snapshot)
            {
                self.metrics.txn_conflicts.inc();
                return Err(EngineError::Conflict);
            }
        }
        let txn_id = TxnId(self.next_txn);
        self.next_txn += 1;
        let principal = self.write_principal.clone();

        // The conflict window is closed; take the buffered writes and the snapshot
        // lease out of the transaction. Releasing the lease (below) before pruning
        // is what lets this transaction's own pinned snapshot stop holding the
        // floor down, so the index can be pruned below it ([STL-204]).
        let SessionTransaction { writes, lease, .. } = txn;

        // Apply every write into per-table group-commit buffers, tracking the tables
        // touched so they can be group-committed (success) or discarded (failure).
        let mut touched: Vec<String> = Vec::new();
        let result = match self.apply_group(writes, txn_id, &principal, &mut touched) {
            Ok(()) => self.finish_group_commit(txn_id, &touched),
            Err(e) => {
                // Discard every buffered (un-logged) write so nothing is made durable
                // and no table is left stuck in group-commit mode.
                for table in &touched {
                    if let Ok(state) = self.table_mut(table) {
                        state.engine.abort_group();
                    }
                }
                Err(e)
            }
        };
        // This transaction no longer pins a snapshot — release the lease, then prune
        // the write index below the new oldest live snapshot ([STL-204]).
        drop(lease);
        self.prune_write_index();
        if result.is_ok() {
            self.metrics.txn_commits.inc();
        }
        result
    }

    /// Durably commit every `touched` table, atomically across **all** of them
    /// ([`commit`](Self::commit), [STL-215]).
    ///
    /// **Single-table (or empty) path.** With at most one table touched, the
    /// table's writes are committed as a single **two-phase** record
    /// ([`commit_group_two_phase`](stele_storage::engine::Engine::commit_group_two_phase))
    /// gated on this commit's record, then the commit record is fsynced as the
    /// commit point — the same gating the multi-table legs use ([STL-215]), now
    /// applied to the single-table path so a crash between the data fsync and the
    /// commit-record fsync discards the leg rather than leaving it durable-but-
    /// unchained ([STL-314], [ADR-0031]). STL-302 left this path on the plain,
    /// unconditionally-applied [`commit_group`](stele_storage::engine::Engine::commit_group)
    /// (the data fsync + an additive commit record); gating it closes that window.
    /// An empty (no-write) commit writes neither record.
    ///
    /// **Multi-table two-phase path.** Across several tables a single record per
    /// table is *not* atomic — a crash between two tables' commits would leave one
    /// durable and the other not. So each table's writes are committed as a
    /// **two-phase** record ([`commit_group_two_phase`](stele_storage::engine::Engine::commit_group_two_phase)),
    /// durable but inert, and once **every** leg is durable a single commit marker
    /// naming `txn_id` is fsynced to the engine commit log. That marker's fsync is
    /// the commit point: on recovery a leg is replayed only if the marker is present
    /// ([`recover`](Self::recover)), so a crash *before* the marker discards every
    /// leg and the transaction recovers all-or-none across tables.
    ///
    /// On a mid-sequence failure no marker is written, so the transaction is durably
    /// uncommitted (recovery discards whatever legs reached disk); the remaining
    /// tables' buffers are discarded
    /// ([`abort_group`](stele_storage::engine::Engine::abort_group)) so none is left
    /// buffering. The failing leg rolls its own resident writes back in place (a clean
    /// append failure, [STL-295]); the in-memory state of legs that *already* committed
    /// a durable (inert) two-phase record before the failure is not rolled back here —
    /// the cross-table in-memory rollback stays a follow-up ([STL-216]).
    ///
    /// One caveat on a failing leg: if its commit failed *after* the WAL append (a
    /// failed fsync, not a torn write), its staged record's durability is
    /// indeterminate. Per the WAL contract that is a crash, not a clean abort — and
    /// it is now enforced: the leg's WAL poisons, so the session refuses further
    /// writes ([`is_poisoned`](Self::is_poisoned)) until [`recover`](Self::recover)
    /// resolves it from the log ([STL-217]). (The session-level commit *marker* log
    /// is a separate WAL and not covered by this poison; a marker fsync failure is
    /// still surfaced as an error.)
    ///
    /// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
    fn finish_group_commit(
        &mut self,
        txn_id: TxnId,
        touched: &[String],
    ) -> Result<(), EngineError> {
        // Fast path: zero or one table — no cross-table coordination needed, so the
        // plain single-record commit stands as the atomic boundary. Recovery applies
        // a plain record unconditionally. A touched table that no longer resolves is
        // an invariant break (`apply_group` already resolved and wrote it, and no DDL
        // interleaves a commit) — fail closed via `?` rather than silently
        // acknowledge a commit that never reached the WAL.
        if touched.len() <= 1 {
            let Some(table) = touched.first() else {
                // A no-write COMMIT commits nothing — no chain record (the chain
                // covers data commits, [ADR-0031]).
                return Ok(());
            };
            // Single-table commit: write the data as a **two-phase** record gated
            // on this commit's record, exactly like the multi-table legs ([STL-215],
            // [STL-314]). The leg is durable but inert until the commit record below
            // vouches for it, so a crash in the window between the data fsync and the
            // commit-record fsync discards the leg on recovery (presumed abort)
            // rather than leaving a durable-but-unchained commit ([ADR-0031]). The
            // commit record's fsync is the commit point.
            self.table_mut(table)?
                .engine
                .commit_group_two_phase(txn_id)?;
            return self.record_commit(txn_id);
        }

        // Multi-table: make every leg durable as a two-phase record first. Once any
        // leg fails the rest are discarded and no commit record is written, so the
        // transaction recovers all-or-none. A touched table that no longer resolves
        // is the same invariant break as above — treat it as a leg failure rather
        // than skip it and then vouch a record for a leg that was never committed.
        let mut error: Option<EngineError> = None;
        for table in touched {
            match self.table_mut(table) {
                Ok(state) if error.is_some() => state.engine.abort_group(),
                Ok(state) => {
                    if let Err(e) = state.engine.commit_group_two_phase(txn_id) {
                        error = Some(EngineError::from(e));
                    }
                }
                Err(e) => error = error.or(Some(e)),
            }
        }
        if let Some(e) = error {
            // A leg failed: no commit record, so recovery discards every leg.
            return Err(e);
        }

        // Every per-table leg is durable; the commit record's fsync is the commit
        // point — and this transaction's link in the tamper-evident chain. Its
        // `txn_id` is the marker recovery gates the two-phase legs on ([STL-215]);
        // the record's hash chain is [ADR-0031].
        self.record_commit(txn_id)
    }

    /// Append one [`CommitRecord`] for transaction `txn_id` to the durable
    /// hash-chained commit log (`stele.commits`) and advance the in-memory chain
    /// head / seq ([ADR-0031], STL-302).
    ///
    /// Called once per data-mutating commit, *after* that write's own data is
    /// durable, so the commit-record fsync is the commit point and the head only
    /// advances for a durably-recorded commit. The record links to `commit_head`
    /// (the prior record's hash, [`Digest::ZERO`] for the first) and takes the next
    /// `commit_seq`; the commit timestamp is the commit clock's current instant,
    /// monotonic across commits. This is the live-server writer of the same chain
    /// [`TxnManager`](stele_txn::TxnManager) maintains in `stele-txn` (STL-178).
    ///
    /// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
    ///
    /// # Errors
    ///
    /// [`EngineError::CommitLog`] if the record cannot be appended or fsynced — the
    /// commit is refused; nothing further was acknowledged and the head does not
    /// advance.
    fn record_commit(&mut self, txn_id: TxnId) -> Result<(), EngineError> {
        let record = CommitRecord {
            txn_id,
            commit_ts: self.clock.current(),
            seq: self.commit_seq,
            prev_hash: self.commit_head,
        };
        if let Err(e) = commit_log::append(&self.disk, &record) {
            // The data leg is already durable, but its commit record is not. Since
            // the commit record is the commit point and gates recovery ([STL-314]),
            // recovery would discard the just-applied (resident) write — diverging
            // from this live process. Per the WAL durability contract that is a
            // crash, not a clean abort: poison the session so it stops serving and a
            // restart into `recover` drops the unwitnessed leg (`commit_poisoned`).
            self.commit_poisoned = true;
            return Err(EngineError::CommitLog(e));
        }
        self.commit_head = record.hash();
        self.commit_seq = self.commit_seq.saturating_add(1);
        Ok(())
    }

    /// Apply a transaction's buffered writes into per-table group-commit buffers, in
    /// staged order, recording each table touched in `touched` (in first-touch order)
    /// so the caller can group-commit or discard them ([`commit`](Self::commit)).
    ///
    /// A table is put in group-commit mode the first time the transaction writes to
    /// it; the write itself routes through the shared [`apply_bound_dml`](Self::apply_bound_dml)
    /// path, which now buffers rather than appends (the table is in group mode).
    ///
    /// The table name is allocated only on first touch — the membership scan is over
    /// `touched`, the set of *distinct* tables (typically one or a few), not the
    /// write count — so a large single-table transaction stays allocation-free here.
    fn apply_group(
        &mut self,
        writes: Vec<BoundDml>,
        txn_id: TxnId,
        principal: &Principal,
        touched: &mut Vec<String>,
    ) -> Result<(), EngineError> {
        for dml in writes {
            if !touched.iter().any(|t| t == dml.table()) {
                self.table_mut(dml.table())?.engine.begin_group();
                touched.push(dml.table().to_owned());
            }
            self.apply_bound_dml(dml, txn_id, principal)?;
        }
        Ok(())
    }

    /// `INSERT` `key` into `table` through its WAL → delta path.
    ///
    /// The bound-statement form is [STL-149]; this typed method is what that
    /// router (and the in-process tests) call.
    ///
    /// [STL-149]: https://allegromusic.atlassian.net/browse/STL-149
    ///
    /// # Errors
    ///
    /// [`EngineError::UnknownTable`] if `table` has no tier; [`EngineError::Storage`]
    /// on a write failure.
    #[allow(clippy::too_many_arguments)] // mirrors the storage Engine: table + key/valid/payload + seq + provenance triple
    pub fn insert(
        &mut self,
        table: &str,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        let state = self.table_mut(table)?;
        Ok(state
            .engine
            .insert(key, valid, payload, seq, txn_id, principal)?)
    }

    /// `UPDATE` `key` in `table`: close its prior period and open a new one.
    ///
    /// # Errors
    ///
    /// As [`insert`](Self::insert).
    #[allow(clippy::too_many_arguments)] // mirrors the storage Engine: table + key/valid/payload + seq + provenance triple
    pub fn update(
        &mut self,
        table: &str,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        let state = self.table_mut(table)?;
        Ok(state
            .engine
            .update(key, valid, payload, seq, txn_id, principal)?)
    }

    /// `DELETE` `key` from `table`: close its prior period with no successor.
    ///
    /// # Errors
    ///
    /// As [`insert`](Self::insert).
    pub fn delete(
        &mut self,
        table: &str,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        let state = self.table_mut(table)?;
        Ok(state.engine.delete(key, txn_id, principal)?)
    }

    /// The mutable tier for `table`, but only if it names a **live** table.
    ///
    /// A dropped table keeps its tier resident (history is preserved), so a tier
    /// in the map is not on its own proof the name is writable — the catalog is.
    /// Guarding here keeps the typed DML writers from mutating a logically dropped
    /// (or never-created) table.
    fn table_mut(&mut self, table: &str) -> Result<&mut TableState<C, D>, EngineError> {
        if self.catalog.resolve(table, self.clock.current()).is_none() {
            return Err(EngineError::UnknownTable(table.to_owned()));
        }
        self.tables
            .get_mut(table)
            .ok_or_else(|| EngineError::UnknownTable(table.to_owned()))
    }
}

/// The **default** provenance principal stamped on writes routed through the
/// session engine — [`SessionEngine::write_principal`]'s initial value.
///
/// Direct, non-wire callers (engine and oracle tests) and recovery's re-derived
/// drop-era closes leave it here, so a write with no connection behind it is
/// attributed to the server itself. The pg-wire front end overrides it per
/// connection via [`SessionEngine::set_principal`] ([STL-300]), stamping the
/// authenticated user — the unauthenticated startup `user` under `trust`, the
/// SCRAM-verified user under `scram` ([STL-252]) — so a wire-issued commit records
/// *who* wrote the row. Provenance is captured inline at commit either way, per the
/// architectural invariant; only the identity differs.
///
/// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
/// [STL-300]: https://allegromusic.atlassian.net/browse/STL-300
const WIRE_PRINCIPAL: &[u8] = b"stele";

/// Rows per chunk on the bulk `COPY` fast path ([STL-240]).
///
/// A `COPY` larger than this streams through the chunked bulk-load path: each chunk
/// is bound, applied (spilling), and committed as one two-phase WAL record + fsync, so
/// a million-row load is fsync-bounded (O(rows / this)) rather than row-bounded, and
/// only one chunk's bound rows and redos are resident at a time. A load at or under it
/// stays on the single-group resident path, byte-for-byte as before. Sized to amortize
/// the per-chunk fsync over many rows while keeping the per-chunk working set small;
/// it is independent of the delta's byte-based spill bound, which caps resident
/// memory across chunks regardless of this value.
const BULK_COPY_CHUNK_ROWS: usize = 4096;

/// The in-memory state step 1 of [`SessionEngine::recover`] derives from the
/// replayed catalog log, before any tier is reopened.
struct ReplayedCatalog {
    /// The schema-version chains, reproduced in recorded order.
    catalog: Catalog,
    /// The user store ([STL-252]): name → latest acknowledged verifier.
    users: BTreeMap<String, ScramVerifier>,
    /// Per name, the tier to reopen: the namespace and valid-time policy of
    /// its *latest* create. (A drop keeps the entry — the tier stays resident
    /// for history, exactly as in a live session.)
    tiers: BTreeMap<String, (u64, bool)>,
    /// The instant of each name's *latest* drop, if any ([STL-220]). After the
    /// tiers are reopened, recovery re-derives that drop's storage closes from
    /// this durable catalog record, closing the cross-log window in which the
    /// drop was acknowledged but the tier's auto-commit closes never reached
    /// its WAL. The latest drop suffices (the WAL is append-only, so at most
    /// one era is open at recovery — see [`Engine::close_dropped_era`]).
    ///
    /// [STL-220]: https://allegromusic.atlassian.net/browse/STL-220
    latest_drop: BTreeMap<String, SystemTimeMicros>,
    /// One past the largest recorded namespace — the allocator floor.
    next_namespace: u64,
    /// The largest DDL instant seen (tier replay folds commits in after).
    max_commit: SystemTimeMicros,
}

/// Fold the replayed catalog-log records, in order, into the recovered
/// in-memory state — step 1 of [`SessionEngine::recover`] ([ADR-0028]).
///
/// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
fn fold_catalog_records(records: Vec<CatalogRecord>) -> Result<ReplayedCatalog, EngineError> {
    let mut folded = ReplayedCatalog {
        catalog: Catalog::new(),
        users: BTreeMap::new(),
        tiers: BTreeMap::new(),
        latest_drop: BTreeMap::new(),
        next_namespace: 0,
        max_commit: SystemTimeMicros(0),
    };
    for record in records {
        match record {
            CatalogRecord::CreateTable {
                at,
                namespace,
                name,
                columns,
                temporal,
            } => {
                let valid_time = temporal.valid_time_enabled();
                folded
                    .catalog
                    .create_table(name.clone(), columns, temporal, at)?;
                folded.tiers.insert(name, (namespace, valid_time));
                folded.next_namespace = folded.next_namespace.max(namespace + 1);
                folded.max_commit = folded.max_commit.max(at);
            }
            CatalogRecord::DropTable { at, name } => {
                // Cascades the dropped table's index metadata away, exactly
                // as the live session did ([STL-233]) — drops carry no
                // per-index records.
                folded.catalog.drop_table(&name, at)?;
                // Records are in log order, so the last drop for a name wins.
                folded.latest_drop.insert(name, at);
                folded.max_commit = folded.max_commit.max(at);
            }
            CatalogRecord::CreateIndex {
                at,
                name,
                table,
                kind,
                columns,
            } => {
                folded
                    .catalog
                    .create_index(IndexDef::new(name, table, kind, columns)?)?;
                folded.max_commit = folded.max_commit.max(at);
            }
            CatalogRecord::DropIndex { at, name } => {
                folded.catalog.drop_index(&name)?;
                folded.max_commit = folded.max_commit.max(at);
            }
            // The user store ([STL-252]): the latest create/alter's verifier
            // wins, a drop removes the name. Records are in log order, so
            // plain map mutations reproduce the acknowledged end state.
            CatalogRecord::CreateUser { at, name, verifier }
            | CatalogRecord::AlterUser { at, name, verifier } => {
                folded.users.insert(name, verifier);
                folded.max_commit = folded.max_commit.max(at);
            }
            CatalogRecord::DropUser { at, name } => {
                folded.users.remove(&name);
                folded.max_commit = folded.max_commit.max(at);
            }
        }
    }
    Ok(folded)
}

/// Derive a fresh SCRAM verifier for `password` under an OS-entropy salt and
/// the default iteration count ([STL-252]). The only entropy read in this
/// crate, and only on the user-DDL path — never in the storage/txn core the
/// simulator drives ([ADR-0010]).
///
/// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
/// [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md
fn derive_verifier(password: &Password) -> Result<ScramVerifier, EngineError> {
    let mut salt = [0u8; scram::SALT_LEN];
    getrandom::fill(&mut salt).map_err(|e| EngineError::Entropy(io::Error::from(e)))?;
    Ok(ScramVerifier::derive(
        &password.0,
        &salt,
        scram::DEFAULT_ITERATIONS,
    ))
}

/// The affected-row summary a bound **point** DML operation reports — one row
/// per statement, tagged by kind so the wire layer renders the right
/// `CommandComplete` ([`stage_dml`](SessionEngine::stage_dml) reports it before
/// the write is applied). A scan-then-write variant's count is only known after
/// expansion ([`expand_scan_dml`](SessionEngine::expand_scan_dml) reports it),
/// so it never reaches here.
fn dml_summary(dml: &BoundDml) -> DmlSummary {
    match dml {
        BoundDml::Insert { .. } => DmlSummary::Insert(1),
        BoundDml::Update { .. } => DmlSummary::Update(1),
        BoundDml::Delete { .. } => DmlSummary::Delete(1),
        BoundDml::InsertRows { .. }
        | BoundDml::UpdateScan { .. }
        | BoundDml::DeleteScan { .. }
        | BoundDml::Merge(_) => {
            unreachable!(
                "a multi-row INSERT / scan-then-write DML reports its summary from the expansion"
            )
        }
    }
}

/// Expand a multi-row `INSERT` ([STL-228]) into the per-row point
/// [`BoundDml::Insert`]s it stands for, reporting the inserted-row summary
/// alongside. The rows keep **statement order** (the order the user wrote them);
/// unlike the scan expansion there is no scan to order, so none is imposed — the
/// group's WAL record is already deterministic.
fn expand_insert_rows(dml: BoundDml) -> (Vec<BoundDml>, DmlSummary) {
    let BoundDml::InsertRows {
        table,
        schema_id,
        rows,
    } = dml
    else {
        unreachable!("expand_insert_rows only receives InsertRows");
    };
    let count = rows.len() as u64;
    let writes = rows
        .into_iter()
        .map(|InsertRow { key, values, valid }| BoundDml::Insert {
            table: table.clone(),
            schema_id,
            key,
            values,
            valid,
        })
        .collect();
    (writes, DmlSummary::Insert(count))
}

/// Resolve one `WHEN`-arm value slot against a concrete source row ([STL-230]):
/// a literal passes through; a source-column reference takes the row's cell
/// (`None` = a SQL `NULL` cell). The binder fixed every index against the
/// source's width, so the lookup is total.
///
/// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
fn resolve_merge_value(value: &MergeValue, row: &[Option<ScalarValue>]) -> Option<ScalarValue> {
    match value {
        MergeValue::Literal(value) => value.clone(),
        MergeValue::Source(idx) => row.get(*idx).cloned().flatten(),
    }
}

/// Resolve a `MERGE` arm's valid-time descriptor against a concrete source row
/// into the `[from, to)` interval the expanded write frames onto the payload, or
/// `None` for a system-only arm ([STL-308]).
///
/// A `Some` descriptor only occurs on a valid-time table, where `period` is also
/// `Some` — the binder pairs them — so the mismatched case is a contract break.
fn resolve_arm_valid(
    valid: Option<&MergeValid>,
    period: Option<&ValidTimeSpec>,
    row: &[Option<ScalarValue>],
    table: &str,
) -> Result<Option<Interval>, EngineError> {
    match (valid, period) {
        (Some(valid), Some(period)) => Ok(Some(resolve_merge_valid(valid, period, row, table)?)),
        (Some(_), None) => Err(EngineError::MalformedMergeSource),
        (None, _) => Ok(None),
    }
}

/// Derive one source row's `[from, to)` valid interval from a [`MergeValid`]
/// ([STL-308]): resolve each bound to a microsecond instant and build the
/// half-open interval, surfacing a reversed/empty per-row interval the way the
/// binder surfaces a statement-level one ([`DmlError::EmptyValidInterval`]).
fn resolve_merge_valid(
    valid: &MergeValid,
    period: &ValidTimeSpec,
    row: &[Option<ScalarValue>],
    table: &str,
) -> Result<Interval, EngineError> {
    let from = resolve_merge_bound(valid.from, row, table, period.from_column())?;
    let to = resolve_merge_bound(valid.to, row, table, period.to_column())?;
    Interval::new(from, to).map_err(|_| {
        EngineError::Dml(DmlError::EmptyValidInterval {
            table: table.to_owned(),
            from,
            to,
        })
    })
}

/// Resolve one [`MergeBound`] against a source row to its microsecond instant: an
/// instant passes through; a per-source-row bound reads the source cell, which
/// the binder constrained to an instant-bearing type. A `NULL` source cell is a
/// per-row data error — a valid-time bound has no microsecond ([STL-308]).
fn resolve_merge_bound(
    bound: MergeBound,
    row: &[Option<ScalarValue>],
    table: &str,
    column: &str,
) -> Result<i64, EngineError> {
    match bound {
        MergeBound::Instant(micros) => Ok(micros),
        // The binder fixed every index against the source width, so the lookup is
        // total; only the cell's nullability and (defensively) its type can vary.
        MergeBound::Source(idx) => row.get(idx).and_then(|c| c.as_ref()).map_or_else(
            || {
                Err(EngineError::Dml(DmlError::NullValue {
                    table: table.to_owned(),
                    column: column.to_owned(),
                }))
            },
            |value| instant_micros(value).ok_or(EngineError::MalformedMergeSource),
        ),
    }
}

/// The microsecond instant a resolved period-bound [`ScalarValue`] carries. The
/// binder constrains a per-row period source to an instant-bearing type — a
/// `VALUES` cell to `INT8`, a table column to `TIMESTAMP` / `TIMESTAMPTZ` — whose
/// `i64` body is microseconds. Anything else is an internal contract break
/// (`None`).
const fn instant_micros(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(v) | ScalarValue::Timestamp(v) | ScalarValue::TimestampTz(v) => Some(*v),
        _ => None,
    }
}

/// Encode a [`ScalarValue`] to its canonical, type-erased byte form.
fn encode_value(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// A `u64` row count / byte size as an `int8` cell value for `\segments`
/// ([STL-301]). A figure beyond `i64::MAX` is unreachable for either, but
/// saturate rather than wrap so the value stays monotonic if it ever were.
fn int8_of(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Decode a business-key zone bound at the key column's type for `\segments`
/// ([STL-301]): ship the canonical bytes when they round-trip, `NULL` when the
/// bound is absent (`None`) or does not decode — a truncated variable-width
/// prefix the wire text encoder could otherwise choke on. A fixed-width key's
/// bound is its exact encoding and always round-trips.
fn decode_key_bound(bound: Option<&[u8]>, key_ty: LogicalType) -> Option<Vec<u8>> {
    let bytes = bound?;
    ScalarValue::decode(key_ty, bytes)
        .ok()
        .map(|_| bytes.to_vec())
}

/// The business key for a folded key [`ScalarValue`] — its canonical encoding, the
/// same bytes a later `UPDATE` / `DELETE` / `SELECT` folds the literal to, so the
/// key matches across operations.
fn business_key(value: &ScalarValue) -> BusinessKey {
    BusinessKey::new(encode_value(value))
}

/// Whether `expr` is the Postgres special datetime string `'now'`
/// (case-insensitive), possibly parenthesized — the `SET stele.system_time = 'now'`
/// spelling the ticket lists alongside `now()` ([STL-246]).
fn is_now_string(expr: &stele_sql::sqlparser::ast::Expr) -> bool {
    use stele_sql::sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Nested(inner) => is_now_string(inner),
        Expr::Value(v) => {
            matches!(&v.value, Value::SingleQuotedString(s) if s.eq_ignore_ascii_case("now"))
        }
        _ => false,
    }
}

/// Recognize the Stele-native temporal introspection call `stele_history('t'[,
/// key])` — the wire surface the shell's `\history` / `\timeline` / `\lineage`
/// commands issue ([STL-199]) — returning the table name and the optional key
/// literal (borrowed from `stmt`, folded to the key type later). `None` for any
/// other statement, so the normal binders run.
fn stele_history_call(
    stmt: &Statement,
) -> Option<(String, Option<&stele_sql::sqlparser::ast::Expr>)> {
    let args = stele_native_args(stmt, "stele_history")?;
    // First argument: the table name, a single-quoted string literal. An optional
    // second is the business-key literal (folded to the key type by
    // [`SessionEngine::version_history`]); absent ⇒ every key's timeline. A third
    // (or further) argument is malformed — fall through to the binders rather than
    // silently ignoring it.
    if args.len() > 2 {
        return None;
    }
    let table = string_literal(args.first().copied()?)?;
    let key = args.get(1).copied();
    Some((table, key))
}

/// Recognize the Stele-native audit introspection call `stele_audit('t'[, key])` —
/// the wire surface the shell's `\audit` command issues, and the `hash ← prevHash`
/// source for `\lineage` ([STL-302]). The exact `('t'[, key])` shape as
/// [`stele_history_call`], over the same [`stele_native_args`] matcher; answered by
/// [`SessionEngine::audit_chain`].
fn stele_audit_call(
    stmt: &Statement,
) -> Option<(String, Option<&stele_sql::sqlparser::ast::Expr>)> {
    let args = stele_native_args(stmt, "stele_audit")?;
    if args.len() > 2 {
        return None;
    }
    let table = string_literal(args.first().copied()?)?;
    let key = args.get(1).copied();
    Some((table, key))
}

/// Recognize the Stele-native segment-introspection call `stele_segments('t')` —
/// the wire surface the shell's `\segments` command issues ([STL-301]), the exact
/// sibling of [`stele_history_call`] — returning the table name. `None` for any
/// other statement, so the normal binders run.
///
/// Takes a single string-literal argument (the table); a missing, non-string, or
/// extra argument falls through to the binders unchanged.
fn stele_segments_call(stmt: &Statement) -> Option<String> {
    let args = stele_native_args(stmt, "stele_segments")?;
    let [table] = args.as_slice() else {
        return None;
    };
    string_literal(table)
}

/// The unnamed argument expressions of an **unshaped** Stele-native introspection
/// call `SELECT * FROM <name>(...)`, or `None` for any other statement shape — the
/// structural gate shared by [`stele_history_call`] / [`stele_audit_call`] /
/// [`stele_segments_call`].
///
/// Recognized structurally, like the `pg_catalog` shim: a single-relation `FROM`
/// whose base is a table-valued function named `<name>` (case-insensitive, last
/// name part). "Unshaped" means a bare `SELECT *` with no projection list, filter,
/// grouping, ordering, or limit — this path bypasses the binder/planner, so any
/// shaping clause would be silently dropped. A shaped query (`SELECT id … WHERE …
/// ORDER BY …`) instead falls through to the binders, which reject the unknown
/// relation with a normal error. A `JOIN`, a non-function relation, a name
/// mismatch, or any non-unnamed-expression argument (a named `key => 1`, a
/// wildcard) all return `None`.
fn stele_native_args<'a>(
    stmt: &'a Statement,
    name: &str,
) -> Option<Vec<&'a stele_sql::sqlparser::ast::Expr>> {
    use stele_sql::sqlparser::ast::{
        FunctionArg, FunctionArgExpr, GroupByExpr, SelectItem, SetExpr, Statement as SqlStatement,
        TableFactor,
    };

    let SqlStatement::Query(query) = stmt.sql()? else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name: relation,
        args: Some(args),
        ..
    } = &from.relation
    else {
        return None;
    };
    if !relation
        .0
        .last()?
        .as_ident()
        .is_some_and(|id| id.value.eq_ignore_ascii_case(name))
    {
        return None;
    }
    let unshaped = matches!(select.projection.as_slice(), [SelectItem::Wildcard(_)])
        && select.selection.is_none()
        && select.distinct.is_none()
        && select.having.is_none()
        && matches!(&select.group_by, GroupByExpr::Expressions(g, m) if g.is_empty() && m.is_empty())
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && query.fetch.is_none();
    if !unshaped {
        return None;
    }
    // Every argument must be a plain unnamed expression. A named argument
    // (`key => 1`), a wildcard, or any other shape collapses the whole `collect`
    // to `None` so the statement falls through to the binders — rather than being
    // silently dropped, which would route a malformed call as if the extra
    // argument were absent.
    args.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
            _ => None,
        })
        .collect()
}

/// The owned `String` of a single-quoted string-literal argument, or `None` for
/// any other expression — the table-name argument both Stele-native calls take.
fn string_literal(expr: &stele_sql::sqlparser::ast::Expr) -> Option<String> {
    use stele_sql::sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// The operation that produced `version`, for the `\history` / `\lineage`
/// timeline ([STL-199]): an `UPDATE` iff its predecessor in the **same key's**
/// chain abuts it — the prior period's `sys_to` equals this version's `sys_from`,
/// a supersession — otherwise an `INSERT` (the first version of a key, or a
/// re-insert across a deletion gap, where the prior period ended strictly earlier).
///
/// A pure function of chain adjacency: the same insight that lets a from-scratch
/// rebuild re-derive a supersession close from version adjacency but not a
/// retraction ([ADR-0023]). `prev` must be the version immediately before
/// `version` in the timeline (`version_history` returns them grouped by key and
/// ordered by `(sys_from, seq)`), or `None` at the start. A deleted key needs no
/// special case — its final version's `op` is whatever opened it; the deletion
/// shows only as that version's closed `sys_to`.
/// A transaction id as a lossless `int8` for the `\history` `txid` column: a bit
/// reinterpretation of the `u64`, the **same** encoding segment storage uses for
/// its `TxnId` column (STL-145), so a `txn_id > i64::MAX` keeps its bits rather
/// than saturating (which would collapse distinct ids and break ordering).
#[expect(
    clippy::cast_possible_wrap,
    reason = "lossless u64→i64 bit reinterpretation, matching segment storage"
)]
const fn txid_as_i64(txn_id: TxnId) -> i64 {
    txn_id.0 as i64
}

fn version_op(prev: Option<&Version>, version: &Version) -> &'static str {
    match prev {
        // The abutment is `prev.sys_to == version.sys_from` by design — the prior
        // period's *end* meets this version's *start*. Clippy reads the asymmetry
        // (`sys_to`/`sys_from`) as a likely typo for `sys_to == version.sys_to`,
        // but that mirror would be the bug: it is the gap-free chain check.
        #[expect(
            clippy::suspicious_operation_groupings,
            reason = "sys_to abuts sys_from — the adjacency test, not a typo"
        )]
        Some(p) if p.business_key == version.business_key && p.sys_to == version.sys_from => {
            "UPDATE"
        }
        _ => "INSERT",
    }
}

/// Lower the binder's [`Interval`] (the `stele-sql` layer does not depend on
/// storage) into the storage [`ValidInterval`] the write path takes ([STL-194]).
///
/// Total: the binder built the interval through
/// [`Interval::new`](stele_common::period::Interval::new), which already rejects
/// `from >= to`, so the storage constructor's same check cannot fail here.
fn to_valid_interval(interval: Interval) -> ValidInterval {
    ValidInterval::new(ValidTimeMicros(interval.from), ValidTimeMicros(interval.to))
        .expect("the binder validated from < to")
}

/// The [`BusinessKey`] a bound DML writes — the unit of write-write conflict
/// detection ([`commit`](SessionEngine::commit)). Every `BoundDml` variant carries
/// a single key (the positional first column), so this is total over the enum.
fn dml_business_key(dml: &BoundDml) -> BusinessKey {
    let key = match dml {
        BoundDml::Insert { key, .. }
        | BoundDml::Update { key, .. }
        | BoundDml::Delete { key, .. } => key,
        // The transaction buffer only ever holds per-key writes: a multi-row
        // INSERT, a scan-then-write, or a MERGE statement is expanded at staging
        // ([`SessionEngine::stage_dml`], STL-228 / STL-229 / STL-230).
        BoundDml::InsertRows { .. }
        | BoundDml::UpdateScan { .. }
        | BoundDml::DeleteScan { .. }
        | BoundDml::Merge(_) => {
            unreachable!(
                "a multi-row INSERT / scan-then-write DML is expanded before it is buffered"
            )
        }
    };
    business_key(key)
}

/// The cell of bytes column `id` at logical `row`, or `None` if the column is
/// absent, not a bytes column, or its cell is a SQL `NULL` ([STL-154]). The scan
/// only ever projects [`ColumnId::BusinessKey`] / [`ColumnId::Payload`], both
/// bytes columns; the business key is always present, the payload may be `None`.
///
/// `row` is a *logical* index: a selection-vector batch ([STL-214]) is honored by
/// resolving it to the physical row the selection names before indexing.
fn column_cell(batch: &Batch, id: ColumnId, row: usize) -> Option<Vec<u8>> {
    let row = batch.physical_row(row);
    batch.columns.iter().find_map(|(cid, col)| match col {
        Column::Bytes(v) if *cid == id => v.get(row).cloned().flatten(),
        _ => None,
    })
}

/// Lower a bound `WHERE <scalar> <cmp> <scalar>` predicate ([STL-151], [STL-213])
/// to the vectorized [`Expr`] the [`Filter`] operator evaluates over a whole batch
/// ([STL-206]). The comparison maps to [`Expr::Compare`], and each [`BoundScalar`]
/// side lowers straight to its evaluator node — a column to its schema position
/// (the same index [`ExplodePayload`] puts it at), a literal broadcast as a
/// constant, arithmetic to [`Expr::Arith`]. A typed comparison over the decoded
/// values is equivalent to the byte-equality the original `=`-only loop applied,
/// since the encoding is canonical and a NULL cell decodes to a NULL the comparison
/// (and `Filter`'s "keep TRUE only") drops.
fn lower_predicate(predicate: &BoundPredicate) -> Expr {
    Expr::Compare {
        op: lower_compare_op(predicate.op),
        left: Box::new(lower_scalar(&predicate.left)),
        right: Box::new(lower_scalar(&predicate.right)),
    }
}

/// Lower a [`BoundScalar`] WHERE operand to its [`Expr`] node ([STL-213]).
fn lower_scalar(scalar: &BoundScalar) -> Expr {
    match scalar {
        BoundScalar::Column(index) => Expr::col(*index),
        BoundScalar::Literal(value) => Expr::lit(value.clone()),
        BoundScalar::Arith { op, left, right } => Expr::Arith {
            op: lower_arith_op(*op),
            left: Box::new(lower_scalar(left)),
            right: Box::new(lower_scalar(right)),
        },
        // A computed-projection subquery operand ([STL-332]) is resolved to a
        // `Literal` by [`SessionEngine::resolve_scalar_subqueries`] before the scalar
        // is lowered, and a `WHERE` `BoundScalar` never carries one — so a subquery
        // cannot reach the lowerer. Lowering it has no meaning (it is not a per-row
        // expression), so this is an enforced invariant, not a fallback value.
        BoundScalar::Subquery(_) => {
            unreachable!("a projected subquery operand is resolved to a literal before lowering")
        }
    }
}

/// Lower a bound `HAVING <scalar> <cmp> <scalar>` predicate ([STL-265]) to the
/// vectorized [`Expr`] evaluated over the **grouped** batch in [`run_aggregate`].
///
/// The grouped batch presents the `group_count` grouping columns first (`out.groups`,
/// in `group_by` order) then the aggregate columns (`out.aggregates`), so a
/// [`HavingScalar::Group`] addresses position `j` and a [`HavingScalar::Aggregate`]
/// addresses position `group_count + k` — the same flat-column convention
/// [`lower_predicate`] uses over a scanned row, only the columns are aggregate
/// outputs rather than schema cells.
fn lower_having(having: &BoundHaving, group_count: usize) -> Expr {
    Expr::Compare {
        op: lower_compare_op(having.op),
        left: Box::new(lower_having_scalar(&having.left, group_count)),
        right: Box::new(lower_having_scalar(&having.right, group_count)),
    }
}

/// Lower a [`HavingScalar`] operand to its [`Expr`] node over the grouped batch
/// ([STL-265]) — see [`lower_having`] for the column layout.
fn lower_having_scalar(scalar: &HavingScalar, group_count: usize) -> Expr {
    match scalar {
        HavingScalar::Group(position) => Expr::col(*position),
        HavingScalar::Aggregate(index) => Expr::col(group_count + *index),
        HavingScalar::Literal(value) => Expr::lit(value.clone()),
        HavingScalar::Arith { op, left, right } => Expr::Arith {
            op: lower_arith_op(*op),
            left: Box::new(lower_having_scalar(left, group_count)),
            right: Box::new(lower_having_scalar(right, group_count)),
        },
    }
}

/// Map the binder's [`CompareOp`] to the executor's `CmpOp`. The two enums are
/// parallel; stele-sql and stele-exec do not depend on each other, so the engine is
/// the lowering point (the same split [`lower_join_type`] / [`lower_arith_op`] draw).
const fn lower_compare_op(op: CompareOp) -> CmpOp {
    match op {
        CompareOp::Eq => CmpOp::Eq,
        CompareOp::Ne => CmpOp::Ne,
        CompareOp::Lt => CmpOp::Lt,
        CompareOp::Le => CmpOp::Le,
        CompareOp::Gt => CmpOp::Gt,
        CompareOp::Ge => CmpOp::Ge,
    }
}

/// Map the binder's [`ArithOp`] to the executor's `ExecArithOp` ([STL-213]).
const fn lower_arith_op(op: ArithOp) -> ExecArithOp {
    match op {
        ArithOp::Add => ExecArithOp::Add,
        ArithOp::Sub => ExecArithOp::Sub,
        ArithOp::Mul => ExecArithOp::Mul,
        ArithOp::Div => ExecArithOp::Div,
        ArithOp::Mod => ExecArithOp::Mod,
    }
}

/// Lower a per-row period predicate ([STL-193]) to an [`Expr::Period`] over two
/// `MakePeriod` operands ([STL-213]). Each `PERIOD(from, to)` becomes the evaluator
/// node that builds the `[from, to)` interval per row from its endpoint instants,
/// so the predicate runs through the same vectorized [`Filter`] as a `<col> <cmp>`
/// comparison rather than a bespoke row loop. A fully-constant predicate never
/// reaches here — [`const_period_truth`] folds it to a single truth value instead.
fn lower_period_predicate(predicate: &BoundPeriodPredicate) -> Expr {
    Expr::Period {
        pred: predicate.predicate,
        left: Box::new(lower_period_operand(&predicate.left)),
        right: Box::new(lower_period_operand(&predicate.right)),
    }
}

/// Lower one `PERIOD(from, to)` operand to an [`Expr::MakePeriod`] over its two
/// endpoint instants ([STL-213]).
fn lower_period_operand(operand: &BoundPeriod) -> Expr {
    Expr::make_period(
        lower_period_endpoint(operand.from),
        lower_period_endpoint(operand.to),
    )
}

/// Lower one period endpoint to its instant [`Expr`]: a constant to an `int8` µs
/// literal, a column to its schema-position reference (the binder already proved
/// it is a `BIGINT`/`TIMESTAMP`/`TIMESTAMPTZ` instant the evaluator reads as `i64`).
const fn lower_period_endpoint(endpoint: PeriodEndpoint) -> Expr {
    match endpoint {
        PeriodEndpoint::Const(micros) => Expr::lit(ScalarValue::Int8(micros)),
        PeriodEndpoint::Column(index) => Expr::col(index),
    }
}

/// The single truth value of a fully-constant period predicate ([STL-165]), or
/// `None` when any endpoint references a value column (then it is lowered to a
/// per-row [`Expr::Period`] — [STL-193], [STL-213]).
fn const_period_truth(predicate: &BoundPeriodPredicate) -> Option<bool> {
    let left = const_period_interval(&predicate.left)?;
    let right = const_period_interval(&predicate.right)?;
    Some(evaluate(predicate.predicate, left, right))
}

/// The `[from, to)` interval of an operand whose endpoints are both constants, or
/// `None` if either is a column. The binder already proved a constant operand is
/// well-formed (`from < to`), so the [`Interval::new`] never fails here.
fn const_period_interval(operand: &BoundPeriod) -> Option<Interval> {
    match (operand.from, operand.to) {
        (PeriodEndpoint::Const(from), PeriodEndpoint::Const(to)) => Interval::new(from, to).ok(),
        _ => None,
    }
}

/// The half-open valid-time probe `[lo, hi)` a per-row PERIOD predicate pushes
/// down into the scan to skip segments whose valid coverage cannot overlap it
/// ([STL-315]), or `None` when the predicate's shape admits no sound probe.
///
/// Sound exactly when the predicate relates the row's **own valid-time period** —
/// `PERIOD(valid_from, valid_to)`, the schema columns at `valid_cols` (the same
/// `[valid_from, valid_to)` the per-segment summary indexes) — to a
/// fully-constant probe period `PERIOD(lo, hi)`, under a predicate whose truth
/// *requires* the two to overlap ([`period_implies_overlap`]). For those, a row
/// whose interval does not overlap `[lo, hi)` can never satisfy the predicate, so
/// a segment the summary proves holds no overlapping row holds no matching row —
/// the superset contract [`SnapshotScan::prune_valid_overlap`] relies on.
///
/// Either operand may be the row period (the qualifying predicates are symmetric,
/// or sound in both directions — see [`period_implies_overlap`]), so both orders
/// are tried; the other operand must be fully constant, else there is no constant
/// probe to push and the scan runs unpruned. A predicate over any other column
/// pair, or against a non-constant probe, yields `None`: the prune never fires and
/// the per-row [`Filter`] still decides every row.
fn valid_overlap_probe(
    period: &BoundPeriodPredicate,
    valid_cols: (usize, usize),
) -> Option<(i64, i64)> {
    if !period_implies_overlap(period.predicate) {
        return None;
    }
    let (from_idx, to_idx) = valid_cols;
    let is_valid_axis = |operand: &BoundPeriod| {
        operand.from == PeriodEndpoint::Column(from_idx)
            && operand.to == PeriodEndpoint::Column(to_idx)
    };
    // One operand is the row's valid-time period; the probe is the constant
    // interval of the other. A fully-constant predicate (both operands constant)
    // is folded to a truth value before it reaches here ([`const_period_truth`]),
    // and neither operand matches the column axis, so this returns `None` for it.
    let probe = if is_valid_axis(&period.left) {
        const_period_interval(&period.right)
    } else if is_valid_axis(&period.right) {
        const_period_interval(&period.left)
    } else {
        None
    }?;
    Some((probe.from, probe.to))
}

/// Whether a period predicate's truth *requires* its two operands to share at
/// least one point — so a row whose valid interval does not overlap the probe can
/// never satisfy it, the condition the valid-time overlap prune needs ([STL-315]).
///
/// True for the three SQL:2011 predicates that hold only on overlapping intervals
/// — `OVERLAPS`, `CONTAINS` (a contained, non-empty period necessarily overlaps
/// its container, in either operand order), `EQUALS` — and false for the
/// disjoint-by-definition ones (`PRECEDES`, `SUCCEEDS`, and the two `IMMEDIATELY`
/// / `MEETS` forms), which can be true *precisely when* the intervals do not
/// overlap and so admit no overlap prune.
const fn period_implies_overlap(predicate: PeriodPredicate) -> bool {
    match predicate {
        PeriodPredicate::Contains | PeriodPredicate::Overlaps | PeriodPredicate::Equals => true,
        PeriodPredicate::Precedes
        | PeriodPredicate::Succeeds
        | PeriodPredicate::ImmediatelyPrecedes
        | PeriodPredicate::ImmediatelySucceeds => false,
    }
}

/// A computed projection materialized for shaping ([STL-303]): the reconstructed
/// rows widened with the evaluated virtual columns, the matching extended column
/// metadata, and the output-position indices into them — the trio [`shape_rows`]
/// sorts / deduplicates / limits before the gather. See
/// [`SessionEngine::materialize_projection`].
struct MaterializedProjection {
    /// The addressable columns the rows carry, then one entry per appended virtual
    /// (computed / subquery) column.
    columns: Vec<(String, LogicalType)>,
    /// Each reconstructed row, widened with its computed virtual cells.
    rows: Vec<Vec<Option<Vec<u8>>>>,
    /// For each output position, its index into `columns` / a row of `rows`.
    indices: Vec<usize>,
}

/// The post-`WHERE` row set the shared [`finish_select`](SessionEngine::finish_select)
/// tail shapes and projects ([STL-338]), addressed by `(row, column)` — either a
/// row-major buffer the read already reconstructed, or a materialized CTE / derived
/// table read **columnar** through a selection vector over its shared
/// [`Cells`](stele_exec::Cells) buffers.
///
/// A base-table point read, a system / valid range scan, a join, and the slow CTE
/// shapes (a computed projection, a correlated-subquery `WHERE`) all feed
/// [`Rows`](Self::Rows): the rows are already row-major, so the tail reads them in
/// place — behavior identical to the pre-[STL-338] `Vec<Vec<…>>` it replaced. A
/// passthrough / projected CTE read feeds [`Relation`](Self::Relation): the tail
/// decodes only the columns a clause references and gathers only the projected output
/// cells straight off the shared buffers, never the full-width row-major intermediate
/// a per-reference gather once built ([STL-321]'s read path) — the
/// [`Filter`](Filter) selection-vector posture ([STL-214]) carried through the
/// shaping tail.
///
/// `selection` holds the surviving rows' **physical** indices into the relation's
/// columns (a `WHERE` keeps an ordered subset, [`relation_selection`]); the logical
/// row `r` the tail addresses is the cell at `selection[r]`. The [`Rows`](Self::Rows)
/// variant's logical and physical indices coincide.
enum RowSource<'a> {
    /// Row-major reconstructed rows, owned — each row one cell per addressable column.
    Rows(Vec<Vec<Option<Vec<u8>>>>),
    /// A materialized CTE / derived table read columnar: its shared columns, plus the
    /// `WHERE`-surviving physical row indices ([`relation_selection`]).
    Relation {
        relation: &'a MaterializedRelation,
        selection: Vec<usize>,
    },
}

impl RowSource<'_> {
    /// The number of logical (post-`WHERE`) rows the tail shapes.
    const fn row_count(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.len(),
            Self::Relation { selection, .. } => selection.len(),
        }
    }

    /// Logical row `row`'s cell at addressable column `col` — the per-cell read the
    /// projection gather uses. An out-of-range column reads as a SQL `NULL` (`None`),
    /// the same defensive decode [`eval_projection_scalar`] takes.
    fn cell(&self, row: usize, col: usize) -> Option<Vec<u8>> {
        match self {
            Self::Rows(rows) => rows[row].get(col).cloned().flatten(),
            Self::Relation {
                relation,
                selection,
            } => relation
                .columns
                .get(col)
                .and_then(|column| relation_cell(column, selection[row])),
        }
    }

    /// Materialize column `col`'s cells over the logical row set — the column-decode
    /// step [`shape_rows`] / [`run_aggregate`] take to build a typed [`Vector`] from
    /// just the columns a clause references. Reads straight off the shared buffers for
    /// a [`Relation`](Self::Relation), with no full-width row-major gather.
    fn column(&self, col: usize) -> Vec<Option<Vec<u8>>> {
        match self {
            Self::Rows(rows) => rows.iter().map(|r| r.get(col).cloned().flatten()).collect(),
            Self::Relation {
                relation,
                selection,
            } => {
                let column = relation.columns.get(col);
                selection
                    .iter()
                    .map(|&phys| column.and_then(|c| relation_cell(c, phys)))
                    .collect()
            }
        }
    }

    /// Materialize the full-width row-major form — the shape the row-iterating slow
    /// paths (a correlated-subquery `WHERE`
    /// [`filter_correlated_subquery`](SessionEngine::filter_correlated_subquery), a
    /// computed projection
    /// [`materialize_projection`](SessionEngine::materialize_projection)) consume. For
    /// a [`Relation`](Self::Relation) this gathers each surviving row's cells off the
    /// shared columns; a passthrough / projected read never reaches here, so it pays
    /// no full-width gather.
    fn into_rows(self) -> Vec<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Rows(rows) => rows,
            Self::Relation {
                relation,
                selection,
            } => selection
                .iter()
                .map(|&phys| {
                    relation
                        .columns
                        .iter()
                        .map(|column| relation_cell(column, phys))
                        .collect()
                })
                .collect(),
        }
    }
}

/// What a bound `SELECT`'s `WHERE` resolves to over the row set ([STL-213]).
///
/// Both the committed-only fused scan ([`scan_rows`](SessionEngine::scan_rows)) and
/// the read-your-own-writes overlay ([`filter_rows`]) read the same plan, so a
/// `WHERE` filters identically whether or not the transaction has buffered writes.
///
/// Resolved once per read ([`resolve_filter`](SessionEngine::resolve_filter)) and
/// shared by both paths, so it is `Clone` (the scan moves the predicate into the
/// streaming [`Filter`], the overlay borrows it).
#[derive(Clone)]
enum FilterPlan {
    /// No predicate — keep every row.
    KeepAll,
    /// A constant predicate that folds false — keep no row. A fully-constant
    /// period predicate ([STL-165]), an empty / NULL-only `IN` set, a `NOT
    /// EXISTS` over a non-empty inner, or a scalar subquery that resolved to
    /// `NULL` ([STL-234]).
    Empty,
    /// A vectorized predicate to evaluate per row: a `<col> <cmp> <scalar>`
    /// comparison ([STL-151], [STL-213]), a per-row period predicate lowered to
    /// `Expr::Period` ([STL-193], [STL-213]), or an uncorrelated subquery folded
    /// to a literal comparison or equality-`OR` set test ([STL-234]).
    Predicate(Expr),
}

/// Resolve a bound `SELECT`'s mutually-exclusive `WHERE` shapes to a [`FilterPlan`].
///
/// The subquery shape ([`BoundSelect::subquery_filter`]) is resolved separately
/// ([`resolve_filter`](SessionEngine::resolve_filter)), since folding it requires
/// running the inner query — this is the snapshot-pure plain/period part.
fn filter_plan(bound: &BoundSelect) -> FilterPlan {
    if let Some(predicate) = &bound.filter {
        return FilterPlan::Predicate(lower_predicate(predicate));
    }
    bound
        .period_filter
        .as_ref()
        .map_or(FilterPlan::KeepAll, |period| {
            match const_period_truth(period) {
                Some(false) => FilterPlan::Empty,
                Some(true) => FilterPlan::KeepAll,
                None => FilterPlan::Predicate(lower_period_predicate(period)),
            }
        })
}

/// Fold a once-materialized subquery `result` into the [`FilterPlan`] its predicate
/// kind implies ([STL-234], [STL-239]).
///
/// Shared by the uncorrelated path (folded once over the whole inner result, in
/// [`resolve_filter`](SessionEngine::resolve_filter)) and the correlated per-row
/// path (folded over each outer row's re-execution, in
/// [`filter_correlated_subquery`](SessionEngine::filter_correlated_subquery)): the
/// fold is the same either way — only what it folds over differs.
fn fold_subquery(kind: SubqueryKind, result: &SelectResult) -> Result<FilterPlan, EngineError> {
    match kind {
        SubqueryKind::Scalar {
            column,
            op,
            subquery_left,
        } => scalar_subquery_plan(result, column, op, subquery_left),
        SubqueryKind::In { column, negated } => in_subquery_plan(result, column, negated),
        SubqueryKind::Exists { negated } => {
            let exists = !result.rows.is_empty();
            // EXISTS keeps every row when the inner has any; NOT EXISTS keeps them
            // when it has none.
            Ok(if exists ^ negated {
                FilterPlan::KeepAll
            } else {
                FilterPlan::Empty
            })
        }
    }
}

/// Whether a correlated subquery whose inner result is **empty** keeps the outer
/// row ([STL-239]) — the answer when the outer correlation value is `NULL`, which
/// makes `inner <op> NULL` unknown for every inner row (an empty result, decided
/// without re-running the inner).
///
/// `EXISTS` over an empty inner is false (keep iff `NOT EXISTS`); `IN ()` is false
/// (keep iff `NOT IN`, since `NOT IN ()` is true); a scalar from an empty inner is
/// `NULL`, so the comparison is unknown and the row is dropped.
const fn empty_inner_keeps(kind: SubqueryKind) -> bool {
    match kind {
        SubqueryKind::Exists { negated } | SubqueryKind::In { negated, .. } => negated,
        SubqueryKind::Scalar { .. } => false,
    }
}

/// Build the inner [`BoundSelect`] for one outer row of a correlated subquery
/// ([STL-239]): the bound inner with its (empty) filter set to the correlation
/// comparison resolved against that row — `inner_column <op> <value>`.
///
/// The inner carries no filter from bind time (the correlation was lifted off it),
/// so this is the sole place the comparison is applied. Snapshot and valid pin are
/// inherited unchanged, so each re-execution reads the outer statement's one
/// consistent snapshot.
fn correlated_inner(
    inner: &BoundSelect,
    correlation: Correlation,
    value: ScalarValue,
) -> BoundSelect {
    let mut inner = inner.clone();
    inner.filter = Some(BoundPredicate {
        left: BoundScalar::Column(correlation.inner_column),
        op: correlation.op,
        right: BoundScalar::Literal(value),
    });
    inner
}

/// Decode an uncorrelated subquery's single output column into typed, nullable
/// scalar values ([STL-234]).
///
/// Reuses the streaming `Filter`'s decode ([`Vector::from_column`]) rather than a
/// second decode path, so a malformed inner cell surfaces the identical
/// [`ScanError::Eval`]. The binder proved the inner returns exactly one column
/// ([`check_subquery_column_type`](stele_sql::select)), so column `0` is read.
fn subquery_column_values(result: &SelectResult) -> Result<Vec<Option<ScalarValue>>, EngineError> {
    let ty = result.columns[0].1;
    let cells: Vec<Option<Vec<u8>>> = result
        .rows
        .iter()
        .map(|row| row.first().cloned().flatten())
        .collect();
    let n = cells.len();
    let column = Column::Bytes(cells.into());
    let vector =
        Vector::from_column(ty, &column).map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
    Ok((0..n).map(|i| vector.get(i)).collect())
}

/// Decode column `col` of each reconstructed row into a typed [`Vector`] of `ty` —
/// the correlation-key currency the correlated-subquery filters work in.
///
/// Shared by the [STL-239] per-row path (the outer correlation column, read once
/// then probed per row) and the [STL-317] decorrelated path (both the outer and
/// inner join-key columns). A missing cell or a short row reads as a SQL `NULL`,
/// which the comparison / join treats as never-matching.
fn key_vector(
    rows: &[Vec<Option<Vec<u8>>>],
    col: usize,
    ty: LogicalType,
) -> Result<Vector, EngineError> {
    let cells: Vec<Option<Vec<u8>>> = rows
        .iter()
        .map(|row| row.get(col).cloned().flatten())
        .collect();
    let column = Column::Bytes(cells.into());
    Vector::from_column(ty, &column).map_err(|e| EngineError::Scan(ScanError::Eval(e)))
}

/// Build a **synthetic composite-key** [`Vector`] from two columns of `rows` — the
/// join currency the [STL-337] composite-key `IN` semi join needs, since
/// [`hash_join`] joins on a single key per side.
///
/// Each cell is the length-prefixed concatenation of the two components' canonical
/// encodings (length-prefixing each so `(x, y)` and `(xy, ⟨⟩)` cannot collide on the
/// same bytes), carried as opaque [`Vector::Bytea`] so [`hash_join`] hashes it by
/// exactly those bytes. The cell is **NULL when either component is NULL**, so the
/// join's "a NULL key never matches" rule reproduces the composite equality's
/// three-valued logic (`a = b AND c = d` is never `TRUE` when any operand is NULL).
/// Each component decodes through [`key_vector`], so a malformed cell surfaces the
/// same [`ScanError::Eval`] the single-key path raises, and the round-trip yields the
/// canonical encoding [`hash_join`] would hash a single key by.
fn composite_key_vector(
    rows: &[Vec<Option<Vec<u8>>>],
    first: (usize, LogicalType),
    second: (usize, LogicalType),
) -> Result<Vector, EngineError> {
    let a = key_vector(rows, first.0, first.1)?;
    let b = key_vector(rows, second.0, second.1)?;
    let cells: Vec<Option<Vec<u8>>> = (0..rows.len())
        .map(|i| match (a.get(i), b.get(i)) {
            (Some(va), Some(vb)) => {
                let (ea, eb) = (encode_value(&va), encode_value(&vb));
                // Pre-size to the exact composite length (two `u64` prefixes + both
                // components) so the cell buffer never reallocates on this hot path.
                let mut bytes = Vec::with_capacity(2 * size_of::<u64>() + ea.len() + eb.len());
                push_len_prefixed(&mut bytes, &ea);
                push_len_prefixed(&mut bytes, &eb);
                Some(bytes)
            }
            // A NULL in either component → a NULL composite key → never matches.
            _ => None,
        })
        .collect();
    Ok(Vector::Bytea(cells))
}

/// Append `bytes` to `out`, framed by a big-endian `u64` length prefix, so the
/// concatenation of two framed components is injective in the components — the
/// identity a [`composite_key_vector`] cell needs.
fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Reduce a once-materialized scalar subquery `result` to its single value
/// ([STL-303]) — the [`SubqueryKind::Scalar`] cardinality rule without the
/// comparison, for a scalar subquery projected in the SELECT list: no row is SQL
/// `NULL`, one row is its value, more than one is the standard's cardinality
/// violation ([`ScalarSubqueryCardinality`], `21000`).
fn scalar_subquery_value(result: &SelectResult) -> Result<Option<ScalarValue>, EngineError> {
    let values = subquery_column_values(result)?;
    match values.as_slice() {
        [] => Ok(None),
        [value] => Ok(value.clone()),
        _ => Err(EngineError::ScalarSubqueryCardinality),
    }
}

/// Evaluate a computed projection expression ([STL-303]) into a column of
/// canonical-encoded cells — one per reconstructed row.
///
/// Decodes only the schema columns the expression references into typed [`Vector`]s
/// (the [`rows_passing_filter`] discipline), runs the lowered expression through
/// [`eval_expr`], then re-encodes each result cell to its canonical bytes (`None` →
/// a SQL `NULL` on the wire). An empty row set short-circuits before `eval_expr`.
fn eval_projection_scalar(
    scalar: &BoundScalar,
    schema_columns: &[(String, LogicalType)],
    rows: &[Vec<Option<Vec<u8>>>],
) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let expr = lower_scalar(scalar);
    let mut referenced = BTreeSet::new();
    collect_expr_columns(&expr, &mut referenced);
    let mut columns: Vec<Vector> = (0..schema_columns.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    for position in referenced {
        let Some((_, ty)) = schema_columns.get(position) else {
            continue;
        };
        let cells: Vec<Option<Vec<u8>>> = rows
            .iter()
            .map(|row| row.get(position).cloned().flatten())
            .collect();
        columns[position] = Vector::from_column(*ty, &Column::Bytes(cells.into()))
            .map_err(|err| EngineError::Scan(ScanError::Eval(err)))?;
    }
    let vector = eval_expr(&expr, &columns, row_count)
        .map_err(|err| EngineError::Scan(ScanError::Eval(err)))?;
    Ok((0..row_count)
        .map(|i| vector.get(i).as_ref().map(encode_value))
        .collect())
}

/// Fold a scalar subquery's result into a comparison [`FilterPlan`] ([STL-234]).
///
/// No row — or a single `NULL` — makes the scalar unknown, so the comparison is
/// unknown for every row and nothing passes ([`Empty`](FilterPlan::Empty)). One
/// concrete value folds to `<column> <op> <literal>` (the operand order is
/// preserved for a non-commutative `op` via `subquery_left`). More than one row
/// is the standard's cardinality violation ([`ScalarSubqueryCardinality`], `21000`).
fn scalar_subquery_plan(
    result: &SelectResult,
    column: usize,
    op: CompareOp,
    subquery_left: bool,
) -> Result<FilterPlan, EngineError> {
    let values = subquery_column_values(result)?;
    match values.as_slice() {
        [] | [None] => Ok(FilterPlan::Empty),
        [Some(value)] => {
            let cmp = lower_compare_op(op);
            let col = Expr::col(column);
            let lit = Expr::lit(value.clone());
            // `subquery_left` keeps `(SELECT …) < col` from lowering as `col < …`.
            let expr = if subquery_left {
                lit.compare(cmp, col)
            } else {
                col.compare(cmp, lit)
            };
            Ok(FilterPlan::Predicate(expr))
        }
        _ => Err(EngineError::ScalarSubqueryCardinality),
    }
}

/// Fold an `IN` / `NOT IN` subquery's result into a [`FilterPlan`] ([STL-234]),
/// with SQL three-valued semantics for the `WHERE` context (only a `TRUE` row is
/// kept).
///
/// `IN` is `col = m1 OR col = m2 OR …` over the **non-NULL** members: a NULL
/// member (or a NULL `col`) can never make the predicate `TRUE`, so dropping NULL
/// members is exact, and an empty / all-NULL set keeps no row. `NOT IN` is `col
/// <> m1 AND col <> m2 AND …`, but a NULL **anywhere** in the set makes the
/// predicate unknown for every row — the classic trap — so it keeps no row; an
/// empty set keeps every row.
fn in_subquery_plan(
    result: &SelectResult,
    column: usize,
    negated: bool,
) -> Result<FilterPlan, EngineError> {
    let values = subquery_column_values(result)?;
    if negated {
        // `NOT IN (set containing NULL)` is never true (the NULL makes membership
        // unknown for every outer row), so nothing passes.
        if values.iter().any(Option::is_none) {
            return Ok(FilterPlan::Empty);
        }
        let terms = values
            .into_iter()
            .flatten()
            .map(|v| Expr::col(column).compare(CmpOp::Ne, Expr::lit(v)))
            .collect();
        // `NOT IN ()` keeps every row.
        Ok(balanced_logic(terms, LogicOp::And).map_or(FilterPlan::KeepAll, FilterPlan::Predicate))
    } else {
        let terms = values
            .into_iter()
            .flatten()
            .map(|v| Expr::col(column).compare(CmpOp::Eq, Expr::lit(v)))
            .collect();
        // `IN ()` matches no row.
        Ok(balanced_logic(terms, LogicOp::Or).map_or(FilterPlan::Empty, FilterPlan::Predicate))
    }
}

/// Combine `terms` under `op` (`AND`/`OR`) into a **balanced** expression tree of
/// depth `⌈log₂ n⌉`, built iteratively (pairwise), rather than a left-deep chain.
///
/// `IN (SELECT …)` folds its inner result into an `OR` of `col = vᵢ` (and `NOT IN`
/// into an `AND` of `col ≠ vᵢ`). A left-deep chain over a large inner result is `n`
/// deep, and `eval_expr` (and the tree's own `Drop`) walk it **recursively**, so a
/// few thousand inner rows overflow a runtime worker thread's stack and abort the
/// whole server — a single well-formed query must never do that. A balanced tree
/// caps that depth at `⌈log₂ n⌉` (~20 even for a million values). `AND`/`OR` are
/// associative and commutative (3-valued `min`/`max`), so re-nesting cannot change
/// the result. Returns `None` for empty `terms`.
fn balanced_logic(terms: Vec<Expr>, op: LogicOp) -> Option<Expr> {
    if terms.is_empty() {
        return None;
    }
    let mut level = terms;
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut pairs = level.into_iter();
        while let Some(left) = pairs.next() {
            next.push(match pairs.next() {
                Some(right) => left.logic(op, right),
                None => left, // an odd term carries up to the next level unchanged
            });
        }
        level = next;
    }
    level.into_iter().next()
}

/// The business-key bytes a per-key buffered DML targets ([STL-343]). Only
/// [`Insert`](BoundDml::Insert) / [`Update`](BoundDml::Update) /
/// [`Delete`](BoundDml::Delete) ever reach a transaction's buffer — the multi-row,
/// scan-then-write, and `MERGE` shapes are expanded into those at staging
/// ([`stage_dml`](SessionEngine::stage_dml)) — so any other variant maps to `None`.
fn buffered_key_bytes(dml: &BoundDml) -> Option<Vec<u8>> {
    match dml {
        BoundDml::Insert { key, .. }
        | BoundDml::Update { key, .. }
        | BoundDml::Delete { key, .. } => Some(encode_value(key)),
        _ => None,
    }
}

/// Splice a transaction's buffered writes into a system-time range's committed
/// version set and render the result rows — the read-your-own-writes overlay for
/// [`run_system_range`](SessionEngine::run_system_range) ([STL-343]).
///
/// A buffered write is observed at the transaction's pinned snapshot `s` (the value
/// `now()` folds to): it opens a new live version `[s, +∞)` and closes the prior one
/// at `s`. So for every key the buffer touched (`buffered`) the currently-open
/// committed version is rendered `[sys_from, s)` instead of open — and dropped when
/// that shrinks it out of the range (an empty `[s, s)` among them) — while every key
/// the buffer leaves live (`buffer_live`, its `[business key, value cells…]` rows)
/// contributes a `[s, +∞)` version, included exactly when that interval overlaps the
/// range. Whether the new version appears therefore turns on the upper bound: a
/// `BETWEEN … AND now()` (`s <= hi`) admits it, a half-open `FROM … TO now()`
/// (`s < hi`) does not. A buffered version has no commit provenance yet, so its
/// pseudo-columns render `NULL`, exactly as the point-read overlay stamps them
/// ([STL-247], [`overlay_row`]).
///
/// Overlap is re-decided by the executor's own [`SystemRange::overlaps`] — the
/// production predicate the committed scan used, not a re-derivation — so the
/// closed-prior and new-version cuts agree with it at the half-open / closed
/// boundary. With `buffered` and `buffer_live` both empty (no buffered write for the
/// table) every committed version renders unchanged: the committed-only path.
#[allow(clippy::too_many_arguments)]
fn overlay_system_range_rows(
    committed: &[Version],
    buffered: &BTreeSet<Vec<u8>>,
    buffer_live: &BTreeMap<Vec<u8>, Vec<Option<Vec<u8>>>>,
    range: SystemRange,
    s: i64,
    value_count: usize,
    needs_provenance: bool,
    width: usize,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
    let prov_nulls = if needs_provenance {
        provenance::PSEUDO_COLUMNS.len()
    } else {
        0
    };
    // At most one new `[s, +∞)` version per *buffered* key, so size to `buffered`
    // rather than `buffer_live` (which may hold every live row before its caller
    // filters it).
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(committed.len() + buffered.len());
    for v in committed {
        let open = v.sys_to == SYSTEM_TIME_OPEN;
        // The buffer closes a touched key's currently-open version at `s`; an
        // already-closed historical version is untouched by the buffer.
        let closed_by_buffer = open && buffered.contains(v.business_key.as_bytes());
        let sys_to = if closed_by_buffer { s } else { v.sys_to.0 };
        // Closing can shrink a version out of the range (`[sys_from, s)` no longer
        // reaches it, or the degenerate `[s, s)`); the executor's predicate decides.
        if closed_by_buffer && !range.overlaps(v.sys_from.0, sys_to) {
            continue;
        }
        let mut row: Vec<Option<Vec<u8>>> = Vec::with_capacity(width);
        row.push(Some(v.business_key.as_bytes().to_vec()));
        row.extend(row_codec::decode_payload(
            value_count,
            v.payload.as_deref(),
        )?);
        row.push(Some(encode_value(&ScalarValue::TimestampTz(v.sys_from.0))));
        // `sys_to` is NULL only for a still-open version the buffer did not close.
        row.push(
            (closed_by_buffer || !open).then(|| encode_value(&ScalarValue::TimestampTz(sys_to))),
        );
        if needs_provenance {
            row.extend(provenance_cells(&v.provenance));
        }
        rows.push(row);
    }
    // The buffer's surviving live state opens a `[s, +∞)` version per touched key.
    // They all share that one interval, so a single overlap check gates the set.
    if range.overlaps(s, SYSTEM_TIME_OPEN.0) {
        for key in buffered {
            let Some(brow) = buffer_live.get(key) else {
                continue; // the key was net-deleted by the buffer — no live version
            };
            let mut row: Vec<Option<Vec<u8>>> = Vec::with_capacity(width);
            row.push(Some(key.clone()));
            row.extend(brow.iter().skip(1).take(value_count).cloned());
            row.push(Some(encode_value(&ScalarValue::TimestampTz(s))));
            row.push(None); // still open
            row.extend(std::iter::repeat_n(None, prov_nulls));
            rows.push(row);
        }
    }
    Ok(rows)
}

/// The `(from, to)` positions of a valid-time table's period columns within a row
/// (key-then-values) — the indices [`overlay_valid_range_rows`] / [`filter_overlaid_valid`]
/// read the `[valid_from, valid_to)` bounds from. `None` for a system-only table (so
/// the caller fails closed rather than reading bounds from the wrong cells).
fn valid_period_indices(
    schema: &TableSchema,
    schema_columns: &[(String, LogicalType)],
) -> Option<(usize, usize)> {
    let spec = schema.temporal().valid_time()?;
    let idx = |name: &str| schema_columns.iter().position(|(n, _)| n == name);
    Some((idx(spec.from_column())?, idx(spec.to_column())?))
}

/// Render a committed valid-time range's resolved versions to result rows
/// `[key, value cells…, valid_from, valid_to, (provenance…)]` ([STL-328]) — the
/// committed-only counterpart of [`overlay_valid_range_rows`]. The value cells decode
/// from the bare payload (the valid-time period columns ride the row codec too, so a
/// `SELECT *` still reads the user's `vf`/`vt`); the appended endpoints are the
/// resolved valid interval, `valid_to` `NULL` for an open-ended (`+∞`) period;
/// provenance, when referenced, follows the endpoints.
fn render_valid_range_rows(
    versions: &[(Version, ValidInterval)],
    value_count: usize,
    needs_provenance: bool,
    width: usize,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(versions.len());
    for (v, interval) in versions {
        let mut row: Vec<Option<Vec<u8>>> = Vec::with_capacity(width);
        row.push(Some(v.business_key.as_bytes().to_vec()));
        row.extend(row_codec::decode_payload(
            value_count,
            v.payload.as_deref(),
        )?);
        let open = interval.to == VALID_TIME_OPEN;
        row.push(Some(encode_value(&ScalarValue::TimestampTz(
            interval.from.0,
        ))));
        row.push((!open).then(|| encode_value(&ScalarValue::TimestampTz(interval.to.0))));
        if needs_provenance {
            row.extend(provenance_cells(&v.provenance));
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Re-filter a transaction's overlaid valid-time rows by a valid-time range and
/// render the result — the read-your-own-writes overlay for
/// [`run_valid_range`](SessionEngine::run_valid_range) ([STL-343]).
///
/// A valid-time write supersedes one system-live version per business key ([STL-223]),
/// so the buffer's effect is the point-read overlay's row set — [`overlay_table_writes`]
/// over the unfiltered system-live scan — one row per key carrying its
/// `[valid_from, valid_to)` in its own value cells ([STL-194]). Each overlaid row is
/// kept iff that interval overlaps the range, the valid-axis mirror of the committed
/// scan's [`SnapshotScan::execute_valid_range`] filter — decided here by the
/// executor's own [`ValidRange::overlaps`], so the two paths agree at the half-open /
/// closed boundary — then rendered `[key, value cells…, valid_from, valid_to,
/// (provenance…)]` exactly as the committed path does: `valid_to` is `NULL` for an
/// open-ended (`+∞`) period, and a buffered write's provenance pseudo-columns are
/// already `NULL` ([`overlay_table_writes`]).
///
/// `from_idx` / `to_idx` are the period columns' positions in a row (key-then-values),
/// the same indices [`filter_overlaid_valid`] reads.
#[allow(clippy::too_many_arguments)]
fn overlay_valid_range_rows(
    overlaid: Vec<Vec<Option<Vec<u8>>>>,
    range: ValidRange,
    from_idx: usize,
    to_idx: usize,
    value_count: usize,
    needs_provenance: bool,
    width: usize,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(overlaid.len());
    for row in overlaid {
        let vf = valid_bound_micros(&row, from_idx)?;
        let vt = valid_bound_micros(&row, to_idx)?;
        if !range.overlaps(vf, vt) {
            continue;
        }
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(width);
        // `[key, value cells…]` carry over verbatim — the user's `vf`/`vt` columns
        // ride the value cells too ([STL-194]), so a `SELECT *` still reads them.
        out.extend(row.iter().take(value_count + 1).cloned());
        out.push(Some(encode_value(&ScalarValue::TimestampTz(vf))));
        out.push((vt != VALID_TIME_OPEN.0).then(|| encode_value(&ScalarValue::TimestampTz(vt))));
        if needs_provenance {
            // Provenance follows the endpoints ([STL-329]); the overlaid row carries
            // it after the value cells (`NULL` for a buffered write), so copy it across.
            out.extend(row.iter().skip(value_count + 1).cloned());
        }
        rows.push(out);
    }
    Ok(rows)
}

/// Overlay a transaction's buffered writes for `table` onto the snapshot-resolved
/// `base` rows — **read-your-own-writes** ([STL-203]) — returning the row set the
/// transaction sees. The writes apply in staged order, keyed by business key, so a
/// later write to a key supersedes an earlier one — the same effect `COMMIT` would
/// make durable. Each row is `[business key, value cells…]` (the
/// [`ExplodePayload`] shape); storage is never touched.
///
/// `INSERT` sets the key's row to the inserted values; `UPDATE` is a read-modify-
/// write merging the `SET` overrides onto the key's current row (an absent key
/// starts all-`NULL`, mirroring [`live_value_cells`](SessionEngine::live_value_cells));
/// `DELETE` removes the key. Keying by business key models the row set both a
/// system-only and a valid-time table resolve at a system snapshot: each write
/// supersedes one live version per key (the storage write path closes the prior
/// system period and opens a new one — [`ValidTimeWriter::update`] / `insert`), so a
/// snapshot resolves at most one live version per key on the system axis. A
/// valid-time table carries its `[valid_from, valid_to)` bounds in the row's own
/// value cells ([STL-194]); a `FOR VALID_TIME AS OF` read filters on them *after*
/// this overlay ([`filter_overlaid_valid`], [STL-223]).
///
/// [STL-194]: https://allegromusic.atlassian.net/browse/STL-194
/// [STL-223]: https://allegromusic.atlassian.net/browse/STL-223
/// [`ValidTimeWriter::update`]: stele_storage::validtime::ValidTimeWriter::update
fn overlay_table_writes(
    base: Vec<Vec<Option<Vec<u8>>>>,
    overlay: &[BoundDml],
    table: &str,
    value_count: usize,
    provenance: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    // Index by business-key bytes (cell 0); a system-time snapshot resolves at most
    // one live row per key. A `BTreeMap` keeps the output deterministic (ascending
    // key bytes) independent of scan order.
    let mut rows: BTreeMap<Vec<u8>, Vec<Option<Vec<u8>>>> = BTreeMap::new();
    for row in base {
        if let Some(key) = row.first().and_then(Clone::clone) {
            rows.insert(key, row);
        }
    }
    for dml in overlay.iter().filter(|d| d.table() == table) {
        match dml {
            BoundDml::Insert { key, values, .. } => {
                let key_bytes = encode_value(key);
                let row = overlay_row(&key_bytes, values, value_count, provenance);
                rows.insert(key_bytes, row);
            }
            BoundDml::Update {
                key, assignments, ..
            } => {
                let key_bytes = encode_value(key);
                let mut row = rows
                    .remove(&key_bytes)
                    .unwrap_or_else(|| overlay_row(&key_bytes, &[], value_count, provenance));
                for (idx, value) in assignments {
                    // The +1 skips the business key at cell 0; an index past the
                    // live value columns (a schema narrowed since binding) is
                    // ignored here — the real apply path rejects it at commit.
                    if let Some(cell) = row.get_mut(idx + 1) {
                        *cell = value.as_ref().map(encode_value);
                    }
                }
                // A buffered write is uncommitted, so its provenance ([STL-247]) is
                // not yet decided: clear the three trailing cells to `NULL`, whether
                // this update started from a committed base row (which carried the
                // superseded version's provenance) or from an absent key.
                clear_overlay_provenance(&mut row, value_count, provenance);
                rows.insert(key_bytes, row);
            }
            BoundDml::Delete { key, .. } => {
                rows.remove(&encode_value(key));
            }
            // The buffer only ever holds per-key writes: a multi-row INSERT, a
            // scan-then-write, or a MERGE statement expands at staging
            // ([`SessionEngine::stage_dml`], STL-228 / STL-229 / STL-230).
            BoundDml::InsertRows { .. }
            | BoundDml::UpdateScan { .. }
            | BoundDml::DeleteScan { .. }
            | BoundDml::Merge(_) => {
                unreachable!(
                    "a multi-row INSERT / scan-then-write DML is expanded before it is buffered"
                )
            }
        }
    }
    rows.into_values().collect()
}

/// Build one overlaid row `[business key, value cells…]` of width `value_count + 1`
/// from a folded key and value list — the in-memory mirror of what
/// [`apply_bound_dml`](SessionEngine::apply_bound_dml) packs into the stored
/// payload. Each value is its canonical encoding (`None` for a SQL `NULL`); a value
/// the list omits (an `UPDATE`'s read-modify-write base passes an empty list) is a
/// `NULL` cell, matching an absent key under
/// [`live_value_cells`](SessionEngine::live_value_cells).
fn overlay_row(
    key_bytes: &[u8],
    values: &[Option<ScalarValue>],
    value_count: usize,
    provenance: bool,
) -> Vec<Option<Vec<u8>>> {
    let prov_count = if provenance {
        provenance::PSEUDO_COLUMNS.len()
    } else {
        0
    };
    let mut row = Vec::with_capacity(value_count + 1 + prov_count);
    row.push(Some(key_bytes.to_vec()));
    for i in 0..value_count {
        row.push(values.get(i).and_then(|v| v.as_ref().map(encode_value)));
    }
    // A buffered (uncommitted) write has no commit provenance ([STL-247]); the three
    // trailing pseudo-column cells are `NULL` until it commits.
    row.extend(std::iter::repeat_n(None, prov_count));
    row
}

/// Clear an overlaid row's three trailing provenance cells to `NULL` ([STL-247]) —
/// a no-op unless the read materializes provenance. Used after an `UPDATE`'s
/// read-modify-write, whose base row may have carried the superseded version's
/// provenance that the now-buffered write replaces.
fn clear_overlay_provenance(row: &mut [Option<Vec<u8>>], value_count: usize, provenance: bool) {
    if !provenance {
        return;
    }
    for cell in row.iter_mut().skip(value_count + 1) {
        *cell = None;
    }
}

/// Keep the overlaid rows whose valid-time period `[valid_from, valid_to)` contains
/// `point` — the post-overlay valid-axis pin a `FOR VALID_TIME AS OF v`
/// read-your-own-writes read applies ([STL-223]).
///
/// The overlay base ([`scan_all_rows`](SessionEngine::scan_all_rows)) leaves the
/// valid axis open, so every system-live period is present with its bounds in the
/// row's own value cells (`from_idx` / `to_idx`, the row codec carries them —
/// [STL-194]). This reproduces the half-open `from ≤ point < to` cut the
/// committed-only scan makes with [`SnapshotScan::valid_as_of`] ([STL-164]), so the
/// overlay and committed paths agree on which periods a valid pin admits.
fn filter_overlaid_valid(
    rows: Vec<Vec<Option<Vec<u8>>>>,
    from_idx: usize,
    to_idx: usize,
    point: i64,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let from = valid_bound_micros(&row, from_idx)?;
        let to = valid_bound_micros(&row, to_idx)?;
        if from <= point && point < to {
            kept.push(row);
        }
    }
    Ok(kept)
}

/// Transpose row-major overlaid rows back into the columnar shape the hash join
/// consumes ([STL-325]). Each of the `ncols` output columns is a [`Column::Bytes`]
/// buffer carrying that position's cell across every row — the same shape
/// [`scan_all_columns`](SessionEngine::scan_all_columns) returns, so
/// [`decode_key_column`] and the join's output assembly read an overlaid side exactly
/// as a committed-only scan. An empty row set still yields `ncols` empty columns (a
/// zero-height side the join scans cleanly). Cells move (no byte copy); a row is
/// `resize`d to `ncols` first, so the overlay pipeline's fixed `value_count + 1`
/// width holds even if a malformed buffered row were ever shorter (padded `NULL`).
fn columns_from_rows(rows: Vec<Vec<Option<Vec<u8>>>>, ncols: usize) -> Vec<Column> {
    let mut columns: Vec<Vec<Option<Vec<u8>>>> =
        (0..ncols).map(|_| Vec::with_capacity(rows.len())).collect();
    for mut row in rows {
        row.resize(ncols, None);
        for (slot, cell) in columns.iter_mut().zip(row) {
            slot.push(cell);
        }
    }
    columns
        .into_iter()
        .map(|cells| Column::Bytes(cells.into()))
        .collect()
}

/// Transpose a materialized relation's columnar cells ([STL-242]) into the row-major
/// shape the range-join fold consumes ([STL-349]) — the inverse of
/// [`columns_from_rows`]. A materialized relation's columns are always
/// [`Column::Bytes`] (canonical cell encodings, [`MaterializedRelation::from_rows`]);
/// a fixed-width column never appears, but is reinterpreted losslessly rather than
/// panicking if one ever did, mirroring [`batch_cell`].
fn rows_from_columns(columns: &[Column], row_count: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    (0..row_count)
        .map(|r| {
            columns
                .iter()
                .map(|col| match col {
                    Column::Bytes(cells) => cells[r].clone(),
                    Column::I64(values) => Some(values[r].to_le_bytes().to_vec()),
                })
                .collect()
        })
        .collect()
}

/// Read one valid-time period bound (`valid_from` / `valid_to`) out of an overlaid
/// row as raw microseconds. The bound is one of the row's value cells, stored as a
/// little-endian `i64` — a `TIMESTAMP` / `TIMESTAMPTZ` / `BIGINT` cell, all of which
/// encode to the same eight bytes ([`ScalarValue::encode`]). The binder always
/// writes both bounds as concrete instants (an omitted upper bound becomes
/// `VALID_TIME_OPEN`, [STL-194]), so a missing or wrong-width cell is a corrupt
/// buffered write or scanned row rather than user input — surfaced, never silently
/// admitted or dropped.
fn valid_bound_micros(row: &[Option<Vec<u8>>], idx: usize) -> Result<i64, EngineError> {
    let bytes: [u8; 8] = row
        .get(idx)
        .and_then(Option::as_deref)
        .and_then(|cell| cell.try_into().ok())
        .ok_or(EngineError::MalformedValidBound)?;
    Ok(i64::from_le_bytes(bytes))
}

/// Apply a bound `SELECT`'s `WHERE` to already-materialized rows — the overlaid
/// read-your-own-writes path ([STL-203]), where the buffer was layered on *after*
/// the scan so the filter cannot be fused into it. The same [`FilterPlan`] the
/// committed-only path runs is evaluated here ([STL-213]): a fully-constant period
/// predicate that folds false drops every row, and any vectorized predicate is run
/// over the materialized rows by [`rows_passing_filter`] — so the two paths agree
/// on which rows survive, whatever the predicate's shape.
fn filter_rows(
    plan: &FilterPlan,
    schema_columns: &[(String, LogicalType)],
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
    match plan {
        FilterPlan::Empty => Ok(Vec::new()),
        FilterPlan::KeepAll => Ok(rows),
        FilterPlan::Predicate(predicate) => rows_passing_filter(predicate, schema_columns, rows),
    }
}

/// The batch-column positions an [`Expr`] reads, collected by descending the whole
/// tree ([`Expr::Column`] leaves) — the columns [`rows_passing_filter`] must decode.
fn collect_expr_columns(expr: &Expr, out: &mut BTreeSet<usize>) {
    match expr {
        Expr::Column(index) => {
            out.insert(*index);
        }
        Expr::Literal(_) => {}
        Expr::Not(inner) | Expr::IsNull(inner) => collect_expr_columns(inner, out),
        Expr::Compare { left, right, .. }
        | Expr::Logic { left, right, .. }
        | Expr::Arith { left, right, .. }
        | Expr::Period { left, right, .. } => {
            collect_expr_columns(left, out);
            collect_expr_columns(right, out);
        }
        Expr::MakePeriod { from, to } => {
            collect_expr_columns(from, out);
            collect_expr_columns(to, out);
        }
    }
}

/// Evaluate a boolean `WHERE` predicate over already-decoded typed columns,
/// returning the per-row keep mask. The semantics match the committed-only
/// [`Filter`]: only a `TRUE` keeps a row; a `FALSE` *or* `NULL` drops it. Shared by
/// the row-major [`rows_passing_filter`] and the columnar [`relation_selection`]
/// ([STL-321]).
///
/// The binder types every predicate as boolean, so a non-boolean result is a plan
/// break rather than a data error — surfaced as [`ExprError::NotBoolean`], never
/// silently kept.
fn predicate_mask(
    predicate: &Expr,
    columns: &[Vector],
    row_count: usize,
) -> Result<Vec<Option<bool>>, EngineError> {
    match eval_expr(predicate, columns, row_count)
        .map_err(|err| EngineError::Scan(ScanError::Eval(err)))?
    {
        Vector::Bool(mask) => Ok(mask),
        other => Err(EngineError::Scan(ScanError::Eval(ExprError::NotBoolean {
            op: "WHERE",
            found: other.logical_type(),
        }))),
    }
}

/// Evaluate a vectorized `WHERE` predicate over already-materialized rows, keeping
/// the rows it reports TRUE ([STL-213]).
///
/// Bridges the row-major encoded cells into one typed column [`Vector`] per schema
/// position — the same form the streaming [`Filter`] decodes from a batch — then
/// runs the predicate through [`eval_expr`]. Only the columns the predicate
/// **references** are decoded; the rest stay empty placeholders the evaluator never
/// reads (the [`run_aggregate`] discipline), so this stays cheap when used over a
/// large materialized set — a provenance read of a wide table ([STL-247]) decodes
/// just its predicate's columns, not every column of every row.
fn rows_passing_filter(
    predicate: &Expr,
    schema_columns: &[(String, LogicalType)],
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
    let row_count = rows.len();
    if row_count == 0 {
        return Ok(rows);
    }
    let mut referenced = BTreeSet::new();
    collect_expr_columns(predicate, &mut referenced);
    let mut columns: Vec<Vector> = (0..schema_columns.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    for position in referenced {
        let Some((_, ty)) = schema_columns.get(position) else {
            continue;
        };
        let cells: Vec<Option<Vec<u8>>> = rows
            .iter()
            .map(|row| row.get(position).cloned().flatten())
            .collect();
        let column = Column::Bytes(cells.into());
        columns[position] = Vector::from_column(*ty, &column)
            .map_err(|err| EngineError::Scan(ScanError::Eval(err)))?;
    }
    let mask = predicate_mask(predicate, &columns, row_count)?;
    Ok(rows
        .into_iter()
        .zip(mask)
        .filter_map(|(row, keep)| (keep == Some(true)).then_some(row))
        .collect())
}

/// The rows of a materialized CTE / derived table its `WHERE` keeps, as row indices
/// into the relation's shared columns — a selection vector, computed without copying
/// a single row ([STL-321], mirroring the [`Filter`] selection-vector posture of
/// [STL-214]).
///
/// `Empty` keeps nothing and `KeepAll` keeps every row; a `Predicate` decodes **only
/// the columns it references** straight off the relation's shared
/// [`Cells`](stele_exec::Cells) buffers (the [`rows_passing_filter`] discipline,
/// minus the per-column re-collection a row-major set would need), evaluates the
/// mask, and keeps the `TRUE` rows.
fn relation_selection(
    plan: &FilterPlan,
    schema_columns: &[(String, LogicalType)],
    relation: &MaterializedRelation,
) -> Result<Vec<usize>, EngineError> {
    let row_count = relation.row_count;
    let predicate = match plan {
        FilterPlan::Empty => return Ok(Vec::new()),
        FilterPlan::KeepAll => return Ok((0..row_count).collect()),
        FilterPlan::Predicate(predicate) => predicate,
    };
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let mut referenced = BTreeSet::new();
    collect_expr_columns(predicate, &mut referenced);
    let mut columns: Vec<Vector> = (0..schema_columns.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    for position in referenced {
        // The relation's columns and `schema_columns` are the same bound header, so
        // a referenced position resolves in both; guard rather than index, so a stale
        // out-of-range reference surfaces as "unfilled placeholder" not a panic.
        let (Some((_, ty)), Some(column)) =
            (schema_columns.get(position), relation.columns.get(position))
        else {
            continue;
        };
        columns[position] = Vector::from_column(*ty, column)
            .map_err(|err| EngineError::Scan(ScanError::Eval(err)))?;
    }
    let mask = predicate_mask(predicate, &columns, row_count)?;
    Ok((0..row_count)
        .zip(mask)
        .filter_map(|(i, keep)| (keep == Some(true)).then_some(i))
        .collect())
}

/// Read one cell of a resolved columnar [`Column`] as the row-major canonical-bytes
/// form the shared `finish_select` tail consumes — the non-batch counterpart of
/// [`batch_cell`]. A materialized relation's columns are always [`Column::Bytes`];
/// a fixed-width column is reinterpreted losslessly rather than panicking if one
/// ever appeared.
fn relation_cell(column: &Column, row: usize) -> Option<Vec<u8>> {
    match column {
        Column::Bytes(cells) => cells[row].clone(),
        Column::I64(values) => Some(values[row].to_le_bytes().to_vec()),
    }
}

/// Map a bound [`JoinType`] to the executor's `ExecJoinType`. The two enums are
/// parallel; stele-sql and stele-exec do not depend on each other, so the engine
/// is the lowering point (the same split [`lower_aggregate_func`] draws).
const fn lower_join_type(join_type: JoinType) -> ExecJoinType {
    match join_type {
        JoinType::Inner => ExecJoinType::Inner,
        JoinType::Left => ExecJoinType::Left,
        JoinType::Semi => ExecJoinType::Semi,
        JoinType::Anti => ExecJoinType::Anti,
    }
}

/// Coalesce a left version's matched right sub-intervals into maximal, sorted,
/// disjoint covers — the matched region a [`JoinType::Semi`] range join emits and the
/// [`JoinType::Left`] / [`JoinType::Anti`] interval *difference* subtracts ([STL-348],
/// the fold in [`run_join_range`](SessionEngine::run_join_range)).
///
/// Inputs are already clipped to the left version's period. Half-open intervals that
/// *touch* (`next.from == cur.to`) coalesce: coverage is continuous across the shared
/// instant — there is no gap — so the existential "is there a match" the `SEMI` form
/// asks stays true with no break. Overlapping covers (the same key live on the system
/// axis across two valid regions) coalesce likewise.
fn merge_covers(mut covers: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    covers.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(covers.len());
    for (from, to) in covers {
        match merged.last_mut() {
            Some(last) if from <= last.1 => last.1 = last.1.max(to),
            _ => merged.push((from, to)),
        }
    }
    merged
}

/// The maximal sub-intervals of `[lo, hi)` left uncovered by `merged` (already sorted,
/// disjoint, and clipped to `[lo, hi)` by [`merge_covers`]) — the interval *difference*
/// ([STL-348]).
///
/// These gaps are the instants a left version is live with **no** temporally-overlapping
/// right match: a [`JoinType::Left`] range join `NULL`-extends them, a [`JoinType::Anti`]
/// keeps them. A right match strictly inside `[lo, hi)` fragments it into the surrounding
/// gaps (e.g. a cover `[3, 5)` of `[0, 10)` yields `[0, 3)` and `[5, 10)`); a cover
/// running to `+∞` leaves an open-ended gap only if it starts past `lo`.
fn interval_gaps(lo: i64, hi: i64, merged: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut gaps: Vec<(i64, i64)> = Vec::new();
    let mut cursor = lo;
    for &(from, to) in merged {
        if cursor < from {
            gaps.push((cursor, from));
        }
        cursor = cursor.max(to);
    }
    if cursor < hi {
        gaps.push((cursor, hi));
    }
    gaps
}

/// One input's range scan for a range-over-join ([STL-344]): the row-major
/// reconstructed rows (`[business key, value cells…]` for a base table, the
/// materialized cells for a CTE / derived side), the per-row interval `[from, to)` on
/// the ranged axis (index-aligned to the rows), and the side's scan accounting —
/// `Some` for a base-table scan, `None` for a materialized (CTE / derived) side, whose
/// storage reads were accounted at materialization ([STL-349], the same
/// footer-suppression convention the point join uses, [STL-318]). The fold in
/// [`run_join_range`](SessionEngine::run_join_range) hash-joins the rows and combines
/// the intervals (`INNER` intersect, `LEFT` / `SEMI` / `ANTI` interval difference).
type RangeSideRows = (
    Vec<Vec<Option<Vec<u8>>>>,
    Vec<(i64, i64)>,
    Option<ScanStats>,
);

/// Which temporal axis an interval read over a join ranges ([STL-344]): the system
/// axis (a version's `[sys_from, sys_to)`) or the valid axis (`[valid_from,
/// valid_to)`). It both selects the per-side range scan
/// ([`join_side_range_rows`](SessionEngine::join_side_range_rows)) and carries the
/// query range the intersected interval is filtered against.
#[derive(Clone, Copy)]
enum RangeAxis {
    /// A `FOR SYSTEM_TIME` range — intersect the inputs' system intervals.
    System(SystemTimeRange),
    /// A `FOR VALID_TIME` range — intersect the inputs' valid intervals, every
    /// input read system-live at the statement snapshot.
    Valid(ValidTimeRange),
}

impl RangeAxis {
    /// The half-open / closed overlap window the intersected interval is selected
    /// by — the same [`SystemRange::overlaps`] boundary test the single-table range
    /// applies ([STL-244]). The formula is axis-agnostic raw-µs math (`ValidRange`
    /// draws the identical `<` vs `<=`, [STL-328]), so the system range type serves
    /// the valid axis too.
    const fn overlap_window(self) -> SystemRange {
        let (lo, hi, closed_upper) = match self {
            Self::System(r) => (r.from.0, r.to.0, r.closed_upper),
            Self::Valid(r) => (r.from.0, r.to.0, r.closed_upper),
        };
        SystemRange {
            lo,
            hi,
            closed_upper,
        }
    }

    /// The open-interval sentinel (`+∞`) on the ranged axis — the `to` an open
    /// version's interval carries, and the value the appended `to` endpoint renders
    /// as `NULL`. Both axes are `i64::MAX`, but each names its own constant.
    const fn open_sentinel(self) -> i64 {
        match self {
            Self::System(_) => SYSTEM_TIME_OPEN.0,
            Self::Valid(_) => VALID_TIME_OPEN.0,
        }
    }
}

/// Decode one join-key column out of row-major range rows into the
/// [`hash_join`]-shaped [`Vector`] set ([STL-344]) — the row-major mirror of
/// [`decode_key_column`].
///
/// Only the `key` slot is a real [`Vector`]; the rest are empty placeholders the
/// key expression (`Expr::col(key)`) never reads, so a carried-through column is
/// never forced through the evaluator. The key cells are canonical encodings (a
/// business key is `encode_value` of the key, a value column its codec cell) —
/// exactly what a fresh scan's [`decode_key_column`] reads — so the match is
/// identical to the point join's (NULL never matches, typed equality).
fn range_key_vectors(
    rows: &[Vec<Option<Vec<u8>>>],
    schema: &[(String, LogicalType)],
    key: usize,
) -> Result<Vec<Vector>, EngineError> {
    let column = Column::Bytes(rows.iter().map(|r| r[key].clone()).collect());
    let mut cols: Vec<Vector> = (0..schema.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    cols[key] = Vector::from_column(schema[key].1, &column)
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
    Ok(cols)
}

/// Decode one side's join-key column into a positional [`Vector`] slot for the
/// [`hash_join`] operator ([STL-172]).
///
/// Only the key column at `key` is decoded — the rest stay empty placeholders the
/// key expression (`Expr::col(key)`) never reads (the same discipline
/// [`run_aggregate`] uses), so a non-key column is never forced through the
/// evaluator. The vector is one slot per side column so `Expr::col(key)` addresses
/// the key by its schema index.
fn decode_key_column(
    columns: &[Column],
    schema: &[(String, LogicalType)],
    key: usize,
) -> Result<Vec<Vector>, EngineError> {
    // Only the key position is decoded; every other slot is an empty placeholder the
    // evaluator never reads (the join carries non-key columns through as opaque
    // bytes). Decoding straight from the shared key column avoids re-collecting the
    // cells the row-major path used to ([STL-224]).
    let mut cols: Vec<Vector> = (0..schema.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    cols[key] = Vector::from_column(schema[key].1, &columns[key])
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
    Ok(cols)
}

/// Materialize one gathered column into an owned [`Column::Bytes`] — the surviving
/// cells of an intermediate `JOIN`-chain step ([STL-323]), copied once off the
/// shared buffers so the next step joins against fresh column buffers ([STL-224]).
/// The scanned and gathered join columns are always [`Column::Bytes`] (canonical
/// cell encodings), so the next step's [`decode_key_column`] reads them back
/// identically to a fresh scan.
fn gather_column(gather: &GatheredColumns, col: usize) -> Column {
    Column::Bytes(
        (0..gather.rows())
            .map(|row| gather.bytes(col, row).map(<[u8]>::to_vec))
            .collect(),
    )
}

/// Sum two inputs' scan accounting for the join footer ([STL-318]): `Some` only when
/// both are base-table scans, since a materialized CTE / derived table contributes
/// `None` (it was scanned and accounted at materialization, not at the join).
const fn combine_join_stats(a: Option<ScanStats>, b: Option<ScanStats>) -> Option<ScanStats> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.combine(b)),
        _ => None,
    }
}

/// Read the cell at `position`/logical `row` of an exploded pipeline batch as the
/// [`SelectResult`]'s raw-bytes form ([STL-206]). Every column the pipeline
/// projects — the business key and the [`ExplodePayload`]-produced value columns —
/// is a [`Column::Bytes`] carrying each cell's canonical encoding (`None` for a
/// SQL `NULL`); a fixed-width column never reaches a projected position, but is
/// reinterpreted losslessly rather than panicking if one ever did.
///
/// The [`Filter`] feeding this sink emits a selection-vector batch ([STL-214]):
/// its columns are the full upstream buffers and `row` is a logical index, so
/// resolve it through the selection — reading only the surviving cell, never
/// materializing the whole filtered column.
fn batch_cell(batch: &Batch, position: usize, row: usize) -> Option<Vec<u8>> {
    let row = batch.physical_row(row);
    match &batch.columns[position].1 {
        Column::Bytes(cells) => cells[row].clone(),
        Column::I64(values) => Some(values[row].to_le_bytes().to_vec()),
    }
}

/// The addressable indices a **column-only** [`Projection`] selects, in output
/// order: `All` is every **schema** column left-to-right (the first `n_schema` of
/// `columns`); a [`Projection::Items`] list maps each column item's source name to
/// its position in `columns`.
///
/// `columns` is the addressable set ([`addressable_columns`]) — the table's own
/// columns followed by the provenance pseudo-columns ([STL-247]) — so a named
/// pseudo-column resolves past `n_schema`, while `SELECT *` stops at the schema.
/// `bind_select` has already proved every named column is either a schema column
/// or a pseudo-column, so the lookup never misses — a miss would be a
/// binder/engine contract break.
///
/// Only the all-column fast path ([`Projection::is_all_columns`]) calls this; a
/// computed expression / scalar subquery has no single addressable index and is
/// materialized by [`SessionEngine::materialize_projection`] instead.
fn projection_indices(
    projection: &Projection,
    columns: &[(String, LogicalType)],
    n_schema: usize,
) -> Vec<usize> {
    match projection {
        Projection::All => (0..n_schema).collect(),
        Projection::Items(items) => items
            .iter()
            .map(|item| match &item.value {
                ProjectionValue::Column(source) => columns
                    .iter()
                    .position(|(n, _)| n == source)
                    .expect("bind_select validated the projected column exists"),
                ProjectionValue::Computed { .. } | ProjectionValue::Subquery { .. } => {
                    unreachable!("projection_indices is only called for all-column projections")
                }
            })
            .collect(),
    }
}

/// The schema columns a range scan exposes ([STL-329]): the table's own columns
/// followed by the two period endpoints the read appends — `(sys_from, sys_to)` for
/// a system range ([STL-244]), `(valid_from, valid_to)` for a valid range
/// ([STL-328]), both `TIMESTAMPTZ`. The endpoints are part of this "schema" so a
/// `SELECT *` includes them and the binder's projection / `ORDER BY` / `GROUP BY`
/// indices (bound against the same shape) line up positionally. Callers that need
/// the full addressable set wrap this in [`addressable_columns`], appending the
/// provenance pseudo-columns past the endpoints; a non-range read passes its schema
/// columns through unchanged.
fn range_schema_columns(
    bound: &BoundSelect,
    schema_columns: &[(String, LogicalType)],
) -> Vec<(String, LogicalType)> {
    let mut columns = schema_columns.to_vec();
    if let Some((from, to)) = bound.range_endpoint_names() {
        columns.push((from.to_owned(), LogicalType::TimestampTz));
        columns.push((to.to_owned(), LogicalType::TimestampTz));
    }
    columns
}

/// Encode a version's inline [`Provenance`](provenance::Provenance) into the three
/// pseudo-column cells, in canonical [`PSEUDO_COLUMNS`](provenance::PSEUDO_COLUMNS)
/// order ([STL-247]): `_stele_txn_id` (`int8`, the `u64` id's `i64` bit pattern),
/// `_stele_committed_at` (`timestamptz`), `_stele_principal` (`text`). Byte-for-byte
/// identical to the point path's provenance cells ([`batch_cell`] over the executor's
/// projected provenance columns), so a range read renders a version's provenance the
/// same way a point read does ([STL-329]).
fn provenance_cells(prov: &provenance::Provenance) -> [Option<Vec<u8>>; 3] {
    [
        Some(encode_value(&ScalarValue::Int8(i64::from_ne_bytes(
            prov.txn_id.0.to_ne_bytes(),
        )))),
        Some(encode_value(&ScalarValue::TimestampTz(prov.committed_at.0))),
        Some(prov.principal.as_bytes().to_vec()),
    ]
}

/// The `(name, type)` output columns a projection produces ([STL-303]): `All` is
/// the addressable schema columns; a [`Projection::Items`] list takes each item's
/// output name and type — a column item's type from the addressable set, a computed
/// expression / scalar subquery's from its own resolved type. Shared by the
/// streaming read (`run_select`) and the parameter-free statement `Describe`
/// (`SessionEngine::describe`), so both agree on a `SELECT`'s `RowDescription`
/// shape, pseudo-columns included.
fn projected_columns(
    projection: &Projection,
    columns: &[(String, LogicalType)],
    n_schema: usize,
) -> Vec<(String, LogicalType)> {
    match projection {
        Projection::All => columns[..n_schema].to_vec(),
        Projection::Items(items) => items
            .iter()
            .map(|item| {
                let ty = match &item.value {
                    ProjectionValue::Column(source) => columns
                        .iter()
                        .find(|(n, _)| n == source)
                        .map(|(_, ty)| *ty)
                        .expect("bind_select validated the projected column exists"),
                    ProjectionValue::Computed { ty, .. } | ProjectionValue::Subquery { ty, .. } => {
                        *ty
                    }
                };
                (item.name.clone(), ty)
            })
            .collect(),
    }
}

/// The columns a bound `SELECT` can address by position: the table's own schema
/// columns (key, then value columns), then the three provenance pseudo-columns
/// ([STL-247]) at the fixed virtual layout `[n_schema, n_schema + 1, n_schema + 2]`
/// = (`_stele_txn_id`, `_stele_committed_at`, `_stele_principal`).
///
/// They are appended, never woven in, so `SELECT *` (`Projection::All`, the first
/// `n_schema`) and the `\d` shim never surface them — the Postgres system-column
/// posture; a read materializes them only when one is named.
fn addressable_columns(schema_columns: &[(String, LogicalType)]) -> Vec<(String, LogicalType)> {
    let mut columns = schema_columns.to_vec();
    columns.extend(
        provenance::PSEUDO_COLUMNS
            .iter()
            .map(|(name, ty)| ((*name).to_owned(), *ty)),
    );
    columns
}

/// Whether a bound `SELECT` references a provenance pseudo-column ([STL-247]) — in
/// its projection or its `WHERE` — so the read must materialize each version's
/// provenance alongside its payload. `addressable` is the addressable set (schema
/// columns then pseudo-columns); `n_schema` is the table's own column count, so a
/// projected column item resolving at or past it names a pseudo-column.
///
/// Only a bare **column** item can name a pseudo-column: the binder resolves a
/// computed expression's columns against the schema alone ([STL-303]), so a
/// computed item never pulls provenance in.
fn references_provenance(
    bound: &BoundSelect,
    addressable: &[(String, LogicalType)],
    n_schema: usize,
) -> bool {
    let projection_hits_pseudo = match &bound.projection {
        Projection::All => false,
        Projection::Items(items) => items.iter().any(|item| match &item.value {
            ProjectionValue::Column(source) => addressable
                .iter()
                .position(|(n, _)| n == source)
                .is_some_and(|i| i >= n_schema),
            ProjectionValue::Computed { .. } | ProjectionValue::Subquery { .. } => false,
        }),
    };
    projection_hits_pseudo
        || bound
            .filter
            .as_ref()
            .is_some_and(|p| predicate_references_pseudo(p, n_schema))
}

/// Whether a bound `WHERE` predicate addresses a column at or past `n_schema` — a
/// provenance pseudo-column ([STL-247]).
fn predicate_references_pseudo(predicate: &BoundPredicate, n_schema: usize) -> bool {
    scalar_references_pseudo(&predicate.left, n_schema)
        || scalar_references_pseudo(&predicate.right, n_schema)
}

/// Whether a bound `WHERE` scalar addresses a column at or past `n_schema`,
/// descending through arithmetic ([STL-247]).
fn scalar_references_pseudo(scalar: &BoundScalar, n_schema: usize) -> bool {
    match scalar {
        BoundScalar::Column(index) => *index >= n_schema,
        BoundScalar::Arith { left, right, .. } => {
            scalar_references_pseudo(left, n_schema) || scalar_references_pseudo(right, n_schema)
        }
        // A subquery operand ([STL-332]) only appears in a computed projection (whose
        // pseudo-column detection is handled separately, never descending into the
        // scalar), and never in a `WHERE` scalar — this checker's only caller. Its
        // own column references are inner-schema, not the outer's pseudo-columns, so
        // it joins the literal in never naming an outer pseudo-column.
        BoundScalar::Literal(_) | BoundScalar::Subquery(_) => false,
    }
}

/// Fold reconstructed rows into grouped aggregate output ([STL-171]).
///
/// Decodes the schema columns the plan references into typed, nullable
/// [`Vector`]s, runs the vectorized [`hash_aggregate`], then re-interleaves the
/// grouping and aggregate columns into SELECT-list order, encoding each output
/// cell back to its canonical bytes for the wire. `rows` are the full rows
/// (`[business key, value cells…]`) the scan produced after `WHERE`; `row_count`
/// of `0` still yields one row for an ungrouped aggregate (`COUNT(*)` is `0`).
///
/// The grouped output then runs the rest of the pipeline in Postgres order: the
/// `HAVING` post-grouping filter ([STL-265]), then the result-shaping tail
/// ([STL-263]) `DISTINCT` → `ORDER BY` → `OFFSET` → `LIMIT` over the output
/// columns (an aggregate `ORDER BY` key is always a select-list output
/// position — the binder enforces it).
fn run_aggregate(
    bound: &BoundSelect,
    agg: &BoundAggregate,
    schema_columns: &[(String, LogicalType)],
    rows: &RowSource<'_>,
) -> Result<SelectResult, EngineError> {
    // Decode each referenced schema column into a typed vector; a column the plan
    // never reads stays an empty placeholder the evaluator never touches (the same
    // discipline the Filter operator uses). The column is read off the row source by
    // index ([STL-338]) — straight from a CTE's shared buffers, with no full-width
    // row-major gather.
    let mut columns: Vec<Vector> = (0..schema_columns.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    for &i in &referenced_columns(agg) {
        let cells = rows.column(i);
        columns[i] = Vector::from_column(schema_columns[i].1, &Column::Bytes(cells.into()))
            .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
    }

    // Lower the bound plan to the executor's grouping keys + aggregators, both
    // addressing columns by schema index.
    let group_keys: Vec<Expr> = agg.group_by.iter().map(|&i| Expr::col(i)).collect();
    let aggregators: Vec<Aggregator> = agg
        .aggregates
        .iter()
        .map(|call| Aggregator {
            func: lower_aggregate_func(call.func),
            arg: call.arg.map(Expr::col),
        })
        .collect();

    let out = hash_aggregate(&group_keys, &aggregators, &columns, rows.row_count())
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

    // Re-interleave grouping + aggregate columns into SELECT-list order.
    let output: Vec<&Vector> = agg
        .items
        .iter()
        .map(|item| match item {
            OutputItem::Group(j) => &out.groups[*j],
            OutputItem::Aggregate(k) => &out.aggregates[*k],
        })
        .collect();

    // The selection of group indices the tail shapes — every group, to start.
    let mut selection: Vec<usize> = (0..out.num_groups).collect();

    // HAVING ([STL-265]): filter groups *before* the result-shaping tail, the
    // Postgres pipeline position (aggregate → HAVING → DISTINCT → ORDER BY →
    // LIMIT). The predicate evaluates over the grouped batch directly — the
    // grouping columns then the aggregate columns, the flat layout `lower_having`
    // addresses (already typed [`Vector`]s, no decode) — and a group is kept iff
    // it is TRUE, dropping FALSE *and* NULL, the same rule the row Filter applies.
    // An aggregate the HAVING references but the SELECT list omits was appended to
    // `agg.aggregates`, so it is present here though absent from `output`.
    if let Some(having) = &agg.having {
        let group_count = out.groups.len();
        let predicate = lower_having(having, group_count);
        // Materialize only the columns the predicate references (a supported
        // HAVING touches one or two), leaving the rest empty placeholders the
        // evaluator never reads — the `run_aggregate` / `rows_passing_filter`
        // discipline, so a wide / high-cardinality group-by does not clone every
        // grouped vector.
        let mut grouped: Vec<Vector> = (0..group_count + out.aggregates.len())
            .map(|_| Vector::Bool(Vec::new()))
            .collect();
        let mut referenced = BTreeSet::new();
        collect_expr_columns(&predicate, &mut referenced);
        for position in referenced {
            if position < group_count {
                grouped[position] = out.groups[position].clone();
            } else if let Some(vector) = out.aggregates.get(position - group_count) {
                grouped[position] = vector.clone();
            }
        }
        let mask = match eval_expr(&predicate, &grouped, out.num_groups)
            .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?
        {
            Vector::Bool(mask) => mask,
            // The binder types every HAVING as boolean, so a non-boolean result is
            // a plan break, not a data error — surface it rather than silently keep.
            other => {
                return Err(EngineError::Scan(ScanError::Eval(ExprError::NotBoolean {
                    op: "HAVING",
                    found: other.logical_type(),
                })));
            }
        };
        selection.retain(|&g| mask[g] == Some(true));
    }

    // The result-shaping tail ([STL-263]): DISTINCT → ORDER BY → OFFSET →
    // LIMIT over the surviving grouped output rows.
    if bound.distinct {
        selection = distinct_selection(&output, &selection);
    }
    if !bound.order_by.is_empty() {
        let keys: Vec<SortKey<'_>> = bound
            .order_by
            .iter()
            .map(|key| match key.column {
                SortTarget::Output(pos) => Ok(SortKey {
                    column: output[pos],
                    descending: key.descending,
                }),
                // The binder resolves an aggregate ORDER BY key against the
                // select list only, so a schema-column key here is a contract
                // break — surface it rather than panic.
                SortTarget::Schema(_) => Err(EngineError::Unsupported(
                    "an aggregate ORDER BY key must be a select-list output column",
                )),
            })
            .collect::<Result<_, _>>()?;
        sort_selection(&keys, &mut selection);
    }
    limit_selection(&mut selection, bound.offset, bound.limit);

    // Encode each surviving cell back to its canonical bytes (`None` → a SQL
    // NULL on the wire).
    let result_rows: Vec<Vec<Option<Vec<u8>>>> = selection
        .iter()
        .map(|&g| {
            output
                .iter()
                .map(|v| v.get(g).as_ref().map(encode_value))
                .collect()
        })
        .collect();

    Ok(SelectResult {
        columns: agg.columns.clone(),
        rows: result_rows,
        // The caller ([`run_select`](SessionEngine::run_select)) folds in the scan
        // accounting that fed this aggregate ([STL-201]).
        stats: None,
    })
}

/// Apply the result-shaping pipeline to a plain (non-aggregate) read's
/// reconstructed full rows ([STL-263]): `DISTINCT` over the projected row, then
/// `ORDER BY`, then `OFFSET`/`LIMIT` — returning the surviving row indices in
/// output order, for the projection to gather.
///
/// Shaping moves row *indices* only (the executor's selection-vector
/// machinery); the only cell work is decoding the columns a clause actually
/// references into typed [`Vector`]s, each once. An `ORDER BY` key may name an
/// unprojected schema column (the Postgres plain-`SELECT` allowance) — the full
/// rows carry every schema column, so it sorts the same way before the
/// projection drops it.
fn shape_rows(
    bound: &BoundSelect,
    schema_columns: &[(String, LogicalType)],
    projection: &[usize],
    rows: &RowSource<'_>,
) -> Result<Vec<usize>, EngineError> {
    let mut selection: Vec<usize> = (0..rows.row_count()).collect();
    if !bound.distinct && bound.order_by.is_empty() {
        limit_selection(&mut selection, bound.offset, bound.limit);
        return Ok(selection);
    }

    // The ORDER BY keys as `(schema index, direction)`: an output-position key
    // maps through the projection; a schema key (an unprojected column on a
    // non-DISTINCT read — the binder enforces that) is already one.
    let key_indices: Vec<(usize, bool)> = bound
        .order_by
        .iter()
        .map(|key| {
            let idx = match key.column {
                SortTarget::Output(pos) => projection[pos],
                SortTarget::Schema(idx) => idx,
            };
            (idx, key.descending)
        })
        .collect();

    // Decode each schema column a shaping clause references, once. Columns no
    // clause touches stay opaque bytes.
    let mut referenced: BTreeSet<usize> = key_indices.iter().map(|&(i, _)| i).collect();
    if bound.distinct {
        referenced.extend(projection.iter().copied());
    }
    let mut decoded: BTreeMap<usize, Vector> = BTreeMap::new();
    for &i in &referenced {
        let cells = rows.column(i);
        let vector = Vector::from_column(schema_columns[i].1, &Column::Bytes(cells.into()))
            .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
        decoded.insert(i, vector);
    }

    // DISTINCT deduplicates the full projected row, before ORDER BY (whose
    // keys DISTINCT restricts to the select list — the 42P10 rule).
    if bound.distinct {
        let columns: Vec<&Vector> = projection.iter().map(|i| &decoded[i]).collect();
        selection = distinct_selection(&columns, &selection);
    }
    if !key_indices.is_empty() {
        let keys: Vec<SortKey<'_>> = key_indices
            .iter()
            .map(|&(i, descending)| SortKey {
                column: &decoded[&i],
                descending,
            })
            .collect();
        sort_selection(&keys, &mut selection);
    }
    limit_selection(&mut selection, bound.offset, bound.limit);
    Ok(selection)
}

/// The schema-column indices an aggregate plan reads — the union of its grouping
/// columns and its aggregate arguments (`COUNT(*)` has none), ascending and
/// deduplicated, so each is decoded into a vector once.
fn referenced_columns(agg: &BoundAggregate) -> Vec<usize> {
    let mut set: BTreeSet<usize> = BTreeSet::new();
    set.extend(agg.group_by.iter().copied());
    set.extend(agg.aggregates.iter().filter_map(|call| call.arg));
    set.into_iter().collect()
}

/// Map a bound [`AggregateFunc`] to the executor's `ExecAggregateFunc`. The two
/// enums are parallel; stele-sql and stele-exec do not depend on each other, so
/// the engine is the lowering point.
const fn lower_aggregate_func(func: AggregateFunc) -> ExecAggregateFunc {
    match func {
        AggregateFunc::Count => ExecAggregateFunc::Count,
        AggregateFunc::Sum => ExecAggregateFunc::Sum,
        AggregateFunc::Min => ExecAggregateFunc::Min,
        AggregateFunc::Max => ExecAggregateFunc::Max,
        AggregateFunc::Avg => ExecAggregateFunc::Avg,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use stele_storage::backend::MemDisk;

    /// A constant inner clock; [`MonotonicClock`] turns its readings into the
    /// strictly increasing sequence `1, 2, 3, …`, which is all the tests need and
    /// keeps them deterministic.
    #[derive(Debug, Clone, Copy)]
    struct ZeroClock;
    impl Clock for ZeroClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(0)
        }
    }

    fn session() -> SessionEngine<ZeroClock, MemDisk> {
        SessionEngine::open(MemDisk::new(), ZeroClock)
    }

    /// A settable inner clock, for the tests that need real-looking microsecond
    /// gaps between commits and idle stretches where time passes with no writes
    /// ([STL-227]). `set` only steps where the test says so — deterministic.
    #[derive(Debug, Clone)]
    struct SteppedClock(Arc<AtomicI64>);
    impl SteppedClock {
        fn new(start: i64) -> Self {
            Self(Arc::new(AtomicI64::new(start)))
        }
        fn set(&self, micros: i64) {
            self.0.store(micros, Ordering::Release);
        }
    }
    impl Clock for SteppedClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(self.0.load(Ordering::Acquire))
        }
    }

    fn parse_one(sql: &str) -> Statement {
        stele_sql::parse(sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one statement")
    }

    const CREATE: &str =
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING";

    /// The projected `balance` cell at an `AS OF` qualifier, or `None` when no
    /// version is live there. `qualifier` is spliced verbatim so the tests can
    /// exercise `now()` arithmetic exactly as a client writes it ([STL-227]).
    fn balance_as_of(
        engine: &mut SessionEngine<SteppedClock, MemDisk>,
        qualifier: &str,
    ) -> Result<Option<Vec<u8>>, EngineError> {
        let sql =
            format!("SELECT balance FROM account FOR SYSTEM_TIME AS OF {qualifier} WHERE id = 1");
        let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql))? else {
            panic!("SELECT must return rows");
        };
        assert!(
            r.rows.len() <= 1,
            "one key resolves to at most one live version"
        );
        Ok(r.rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().expect("the projected balance cell")))
    }

    // -- COPY ... FROM STDIN bulk load ([STL-236]) --------------------------

    /// The set of `id`s currently live in `account`, sorted — for asserting which
    /// rows a `COPY` made visible.
    fn loaded_ids(engine: &mut SessionEngine<ZeroClock, MemDisk>) -> Vec<i32> {
        let StatementOutcome::Rows(r) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select")
        else {
            panic!("SELECT returns rows");
        };
        let mut ids: Vec<i32> = r
            .rows
            .iter()
            .map(|row| {
                match stele_common::types::ScalarValue::decode(
                    LogicalType::Int4,
                    row[0].as_ref().expect("id cell"),
                )
                .expect("decode id")
                {
                    stele_common::types::ScalarValue::Int4(v) => v,
                    // Name the type, not the value: Debug-formatting a ScalarValue
                    // trips CodeQL's "cleartext logging" heuristic (it can hold a
                    // UUID) — a recurring false positive in test messages.
                    other => panic!(
                        "id column decoded to {:?}, expected int4",
                        other.logical_type()
                    ),
                }
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// `(id, balance)` field rows in the wire-lexed shape `copy_apply` consumes.
    fn copy_rows(specs: &[(&str, &str)]) -> Vec<Vec<Option<String>>> {
        specs
            .iter()
            .map(|(id, bal)| vec![Some((*id).to_owned()), Some((*bal).to_owned())])
            .collect()
    }

    #[test]
    fn copy_apply_loads_every_row_auto_commit() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let stmt = parse_one("COPY account FROM STDIN");
        let n = engine
            .copy_apply(
                &stmt,
                &copy_rows(&[("1", "100"), ("2", "200"), ("3", "300")]),
            )
            .expect("copy");
        assert_eq!(n, 3);
        assert_eq!(loaded_ids(&mut engine), vec![1, 2, 3]);
    }

    #[test]
    fn copy_apply_is_all_or_nothing_on_a_bad_row() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let stmt = parse_one("COPY account FROM STDIN");
        // Row 2's balance is not an integer — the whole COPY fails and leaves zero
        // rows ([STL-216]), not a partial prefix.
        let err = engine
            .copy_apply(
                &stmt,
                &copy_rows(&[("1", "100"), ("2", "oops"), ("3", "300")]),
            )
            .expect_err("bad row fails the copy");
        assert!(matches!(err, EngineError::Copy(_)), "{err:?}");
        assert_eq!(loaded_ids(&mut engine), Vec::<i32>::new());
    }

    #[test]
    fn copy_stage_in_a_transaction_is_read_your_own_writes_then_commits() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        let n = engine
            .copy_stage(
                &parse_one("COPY account FROM STDIN"),
                &copy_rows(&[("1", "100"), ("2", "200")]),
                &mut txn,
            )
            .expect("stage copy");
        assert_eq!(n, 2);
        // Read-your-own-writes: a SELECT in the same block sees the staged rows
        // ([STL-203]), before any other connection could.
        let StatementOutcome::Rows(seen) = engine
            .execute_in_txn(&parse_one("SELECT id FROM account"), &mut txn)
            .expect("ryow select")
        else {
            panic!("rows");
        };
        assert_eq!(seen.rows.len(), 2, "the txn sees its own staged COPY");
        // Nothing is visible outside the block until COMMIT.
        assert_eq!(loaded_ids(&mut engine), Vec::<i32>::new());
        engine.commit(txn).expect("commit");
        assert_eq!(loaded_ids(&mut engine), vec![1, 2]);
    }

    #[test]
    fn copy_into_an_unknown_table_errors() {
        let mut engine = session();
        let err = engine
            .copy_apply(
                &parse_one("COPY ghost FROM STDIN"),
                &copy_rows(&[("1", "1")]),
            )
            .expect_err("unknown table");
        assert!(matches!(err, EngineError::Copy(_)), "{err:?}");
    }

    /// The STL-227 repro: on an idle database, `AS OF now() - interval '…'` must
    /// track real elapsed time, not stay frozen at the last commit. The stepped
    /// clock plays the reporter's timeline — insert, update 5s later, then 10s of
    /// idle — and the offsets pick out each system-time era deterministically.
    #[test]
    fn as_of_now_tracks_the_clock_between_writes() {
        let clock = SteppedClock::new(1_000_000_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        engine.execute(&parse_one(CREATE)).expect("create");

        clock.set(1_010_000_000);
        engine
            .execute(&parse_one(
                "INSERT INTO account (id, balance) VALUES (1, 100)",
            ))
            .expect("insert");
        clock.set(1_015_000_000);
        engine
            .execute(&parse_one("UPDATE account SET balance = 250 WHERE id = 1"))
            .expect("update");

        // 10 idle seconds: time passes, nothing commits, the high-water mark
        // would have stood still — the frozen-`now()` bug lived here.
        clock.set(1_025_000_000);

        // now() - 1s = t+24s: past the update — the *new* value, however long
        // the database has been idle.
        assert_eq!(
            balance_as_of(&mut engine, "(now() - interval '1 second')").expect("select"),
            cell(Some(ScalarValue::Int4(250)))
        );
        // now() - 11s = t+14s: inside [insert, update) — the old value.
        assert_eq!(
            balance_as_of(&mut engine, "(now() - interval '11 second')").expect("select"),
            cell(Some(ScalarValue::Int4(100)))
        );
        // now() - 20s = t+5s: after CREATE, before the insert — no live version.
        assert_eq!(
            balance_as_of(&mut engine, "(now() - interval '20 second')").expect("select"),
            None
        );
        // now() - 30s: before the table's first commit — the documented error,
        // never a silent empty read.
        assert!(matches!(
            balance_as_of(&mut engine, "(now() - interval '30 second')"),
            Err(EngineError::Select(SelectError::BeforeHistory { .. }))
        ));
    }

    /// Apply one seed's deterministic INSERT/UPDATE/DELETE workload to a fresh
    /// system-versioned `account` engine — a flush partway seals the early
    /// timeline — returning, per key, the `(op, balance)` sequence of versions it
    /// created: the reference `version_history` must reproduce. A delete clears the
    /// key (its next insert is an INSERT again) and makes no version of its own.
    fn apply_account_workload(
        engine: &mut SessionEngine<SteppedClock, MemDisk>,
        clock: &SteppedClock,
        seed: u64,
    ) -> BTreeMap<i64, Vec<(&'static str, i64)>> {
        const KEY_POOL: i64 = 3;
        let mut rng = PlainOracleRng(seed.wrapping_mul(0x1234_5678).wrapping_add(1));
        let mut model: BTreeMap<i64, Vec<(&'static str, i64)>> = BTreeMap::new();
        let mut live: BTreeMap<i64, i64> = BTreeMap::new();
        let mut now = 1_000_000_i64;
        let mut next_balance = 0_i64;

        let ops = 10 + rng.below(14);
        for op in 0..ops {
            now += 1 + rng.below(1000);
            clock.set(now);
            let id = rng.below(KEY_POOL);
            if op == ops / 2 {
                engine.flush().expect("flush"); // seal the timeline so far
            }
            if live.contains_key(&id) && rng.below(3) == 0 {
                engine
                    .execute(&parse_one(&format!("DELETE FROM account WHERE id = {id}")))
                    .expect("delete");
                live.remove(&id);
                continue;
            }
            next_balance += 1;
            if let std::collections::btree_map::Entry::Vacant(e) = live.entry(id) {
                e.insert(next_balance);
                engine
                    .execute(&parse_one(&format!(
                        "INSERT INTO account (id, balance) VALUES ({id}, {next_balance})"
                    )))
                    .expect("insert");
                model.entry(id).or_default().push(("INSERT", next_balance));
            } else {
                live.insert(id, next_balance);
                engine
                    .execute(&parse_one(&format!(
                        "UPDATE account SET balance = {next_balance} WHERE id = {id}"
                    )))
                    .expect("update");
                model.entry(id).or_default().push(("UPDATE", next_balance));
            }
        }
        model
    }

    /// `\history`'s introspection surface ([STL-199]), differentially oracled
    /// against the canonical `FOR SYSTEM_TIME AS OF` read path (testing-strategy
    /// §4). A deterministic random INSERT/UPDATE/DELETE workload over a small key
    /// pool, with a flush partway so the timeline spans the delta tier and a sealed
    /// segment, then two checks per seed:
    ///
    /// * **shape** — the per-key `(op, balance)` sequence `version_history`
    ///   reports equals the sequence the workload applied (INSERT for a key's first
    ///   version *and a re-insert across a deletion gap*, UPDATE for a supersession;
    ///   a DELETE makes no version), and `current` flags exactly the open tail;
    /// * **agreement** — every version's stated value at its own `sys_from` is what
    ///   a snapshot read at that instant returns (a bare-µs `AS OF`, [STL-164]) —
    ///   the history view never disagrees with time travel.
    #[test]
    fn version_history_matches_the_as_of_read_path() {
        for seed in 0..40u64 {
            let clock = SteppedClock::new(1_000_000);
            let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
            engine.execute(&parse_one(CREATE)).expect("create");
            let model = apply_account_workload(&mut engine, &clock, seed);

            let history = engine
                .version_history("account", None)
                .expect("version_history");
            // Column layout: txid, op, sys_from, sys_to, current, principal, id, balance.
            let col = |name: &str| {
                history
                    .columns
                    .iter()
                    .position(|(n, _)| n == name)
                    .unwrap_or_else(|| panic!("history has a {name} column"))
            };
            let (c_op, c_from, c_cur, c_id, c_bal) = (
                col("op"),
                col("sys_from"),
                col("current"),
                col("id"),
                col("balance"),
            );

            let mut reported: BTreeMap<i64, Vec<(&'static str, i64)>> = BTreeMap::new();
            for row in &history.rows {
                let id = decode_int(row[c_id].as_ref(), LogicalType::Int4);
                let balance = decode_int(row[c_bal].as_ref(), LogicalType::Int4);
                let ScalarValue::Text(op_text) =
                    ScalarValue::decode(LogicalType::Text, row[c_op].as_ref().expect("non-null"))
                        .expect("decode text")
                else {
                    unreachable!("op is text")
                };
                let op: &'static str = if op_text == "INSERT" {
                    "INSERT"
                } else {
                    "UPDATE"
                };
                reported.entry(id).or_default().push((op, balance));

                // `current` is true exactly when this version has no `sys_to`.
                let ScalarValue::Bool(current) =
                    ScalarValue::decode(LogicalType::Bool, row[c_cur].as_ref().expect("non-null"))
                        .expect("decode bool")
                else {
                    unreachable!("current is bool")
                };
                assert_eq!(current, row[col("sys_to")].is_none(), "seed {seed}");

                // Agreement with time travel: the version's value at its own start
                // instant, read through the canonical `FOR SYSTEM_TIME AS OF` path
                // with a bare-µs literal ([STL-164]).
                let ScalarValue::TimestampTz(sys_from) = ScalarValue::decode(
                    LogicalType::TimestampTz,
                    row[c_from].as_ref().expect("non-null"),
                )
                .expect("decode ts") else {
                    unreachable!("sys_from is a timestamptz")
                };
                let sql = format!(
                    "SELECT balance FROM account FOR SYSTEM_TIME AS OF {sys_from} WHERE id = {id}"
                );
                let StatementOutcome::Rows(r) =
                    engine.execute(&parse_one(&sql)).expect("as_of read")
                else {
                    panic!("SELECT must return rows");
                };
                let live_here = r
                    .rows
                    .into_iter()
                    .next()
                    .and_then(|mut cells| cells.remove(0))
                    .map(|bytes| decode_int(Some(&bytes), LogicalType::Int4));
                // `assert!` on the comparison, with a `seed`-only message — not
                // `assert_eq!`, which would `Debug`-format the decode-derived
                // operands into the panic text and trip CodeQL's (false)
                // `rust/cleartext-logging` taint on a possible `ScalarValue::Uuid`
                // (the same reason `decode_int` keeps a static panic message). The
                // seed alone replays the failure deterministically.
                assert!(
                    live_here == Some(balance),
                    "seed {seed}: version-history value disagrees with the AS OF read",
                );
            }
            assert_eq!(
                reported, model,
                "seed {seed}: reported timeline vs workload"
            );
        }
    }

    /// The wire-facing surface: `SELECT * FROM stele_history('t'[, key])` routes
    /// through `execute` as an ordinary row set ([STL-199]) — fixed metadata
    /// columns then the table's own columns, the keyed form filtered to one key
    /// and the keyless form spanning every key, with unknown-table and bad-key
    /// literals surfaced as engine errors (not silent empties).
    #[test]
    fn stele_history_query_routes_through_execute() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account (id, balance) VALUES (1, 100)",
            "UPDATE account SET balance = 250 WHERE id = 1",
            "INSERT INTO account (id, balance) VALUES (2, 500)",
        ] {
            engine.execute(&parse_one(sql)).expect("write");
        }

        let rows = |engine: &mut SessionEngine<ZeroClock, MemDisk>, q: &str| {
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(q)).expect("history") else {
                panic!("stele_history returns rows");
            };
            r
        };

        // The keyed form: key 1's two versions, oldest first, the metadata prefix
        // then the table's `id` / `balance` columns.
        let keyed = rows(&mut engine, "SELECT * FROM stele_history('account', 1)");
        assert_eq!(
            keyed
                .columns
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            vec![
                "txid",
                "op",
                "sys_from",
                "sys_to",
                "current",
                "principal",
                "id",
                "balance"
            ],
        );
        let op = |row: &[Option<Vec<u8>>]| match ScalarValue::decode(
            LogicalType::Text,
            row[1].as_ref().expect("op present"),
        )
        .expect("decode")
        {
            ScalarValue::Text(s) => s,
            _ => unreachable!(),
        };
        assert_eq!(keyed.rows.len(), 2);
        assert_eq!(op(&keyed.rows[0]), "INSERT");
        assert_eq!(op(&keyed.rows[1]), "UPDATE");
        // Oldest version closed (has a sys_to), newest current (none).
        assert!(keyed.rows[0][3].is_some());
        assert!(keyed.rows[1][3].is_none());
        // The value column decodes to the second version's balance.
        assert_eq!(
            decode_int(keyed.rows[1][7].as_ref(), LogicalType::Int4),
            250
        );

        // The keyless form spans every key: key 1's two versions + key 2's one.
        let all = rows(&mut engine, "SELECT * FROM stele_history('account')");
        assert_eq!(all.rows.len(), 3);

        // An unknown table and a wrong-typed key are errors, never empty rows.
        assert!(matches!(
            engine.execute(&parse_one("SELECT * FROM stele_history('ghost', 1)")),
            Err(EngineError::UnknownTable(_)),
        ));
        assert!(matches!(
            engine.execute(&parse_one("SELECT * FROM stele_history('account', 'oops')")),
            Err(EngineError::IntrospectionKey(_)),
        ));

        // Only the unshaped `SELECT *` form is intercepted — a projection, a
        // filter, or a third argument falls through to the binders (which reject
        // the unknown `stele_history` relation), never silently dropping the
        // shaping clause.
        for shaped in [
            "SELECT id FROM stele_history('account', 1)",
            "SELECT * FROM stele_history('account', 1) WHERE id = 1",
            "SELECT * FROM stele_history('account', 1) ORDER BY id",
            "SELECT * FROM stele_history('account', 1, 2)",
            // A named argument is malformed — never silently dropped to route as
            // the bare `stele_history('account')`.
            "SELECT * FROM stele_history('account', key => 1)",
        ] {
            assert!(
                engine.execute(&parse_one(shaped)).is_err(),
                "shaped/over-argument call must not route: {shaped}",
            );
        }
    }

    /// The wire-facing surface: `SELECT * FROM stele_segments('t')` routes through
    /// `execute` as an ordinary row set ([STL-301]) — one row per sealed segment
    /// (oldest first) then the resident delta (hot) tier, the fixed metadata
    /// columns carrying the real footer/delta figures: state, rows, the system-
    /// time range, the business-key zone, and the byte size (`NULL` for hot).
    #[test]
    fn stele_segments_query_routes_through_execute() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        // Two writes, flushed into one sealed segment; a third stays in the delta.
        for sql in [
            "INSERT INTO account (id, balance) VALUES (1, 100)",
            "INSERT INTO account (id, balance) VALUES (2, 500)",
        ] {
            engine.execute(&parse_one(sql)).expect("write");
        }
        engine.flush().expect("flush");
        engine
            .execute(&parse_one(
                "INSERT INTO account (id, balance) VALUES (3, 900)",
            ))
            .expect("write");

        let StatementOutcome::Rows(set) = engine
            .execute(&parse_one("SELECT * FROM stele_segments('account')"))
            .expect("segments")
        else {
            panic!("stele_segments returns rows");
        };

        assert_eq!(
            set.columns
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            vec![
                "segment",
                "state",
                "rows",
                "sys_min",
                "sys_max",
                "key_column",
                "key_min",
                "key_max",
                "bytes",
            ],
        );
        assert_eq!(set.rows.len(), 2, "one sealed segment + the hot delta tier");

        let text = |cell: Option<&Vec<u8>>| match ScalarValue::decode(
            LogicalType::Text,
            cell.expect("text cell present"),
        )
        .expect("decode text")
        {
            ScalarValue::Text(s) => s,
            _ => unreachable!(),
        };
        let int8 = |cell: Option<&Vec<u8>>| match ScalarValue::decode(
            LogicalType::Int8,
            cell.expect("int8 cell present"),
        )
        .expect("decode int8")
        {
            ScalarValue::Int8(v) => v,
            _ => unreachable!(),
        };

        // The sealed segment: a real `seg-…` filename, two rows, the `id` zone
        // spanning the two flushed keys, and a non-zero on-disk byte size.
        let sealed = &set.rows[0];
        assert!(
            text(sealed[0].as_ref()).starts_with("seg-"),
            "sealed id is the segment filename",
        );
        assert_eq!(text(sealed[1].as_ref()), "sealed");
        assert_eq!(int8(sealed[2].as_ref()), 2);
        assert_eq!(text(sealed[5].as_ref()), "id", "zone column is the key");
        assert_eq!(decode_int(sealed[6].as_ref(), LogicalType::Int4), 1);
        assert_eq!(decode_int(sealed[7].as_ref(), LogicalType::Int4), 2);
        assert!(int8(sealed[8].as_ref()) > 0, "sealed has a byte footprint");

        // The hot tier: the `(hot)` id, the one resident row, key 3, NULL bytes.
        let hot = &set.rows[1];
        assert_eq!(text(hot[0].as_ref()), "(hot)");
        assert_eq!(text(hot[1].as_ref()), "hot");
        assert_eq!(int8(hot[2].as_ref()), 1);
        assert_eq!(decode_int(hot[6].as_ref(), LogicalType::Int4), 3);
        assert_eq!(decode_int(hot[7].as_ref(), LogicalType::Int4), 3);
        assert!(
            hot[8].is_none(),
            "the in-memory hot tier reports no byte size"
        );

        // An unknown table errors, never empty rows; only the unshaped `SELECT *`
        // form routes — a projection, filter, or extra argument falls through.
        assert!(matches!(
            engine.execute(&parse_one("SELECT * FROM stele_segments('ghost')")),
            Err(EngineError::UnknownTable(_)),
        ));
        for shaped in [
            "SELECT segment FROM stele_segments('account')",
            "SELECT * FROM stele_segments('account') WHERE rows = 2",
            "SELECT * FROM stele_segments('account', 1)",
            "SELECT * FROM stele_segments('account', n => 1)",
            "SELECT * FROM stele_segments()",
        ] {
            assert!(
                engine.execute(&parse_one(shaped)).is_err(),
                "shaped/over-argument call must not route: {shaped}",
            );
        }
    }

    /// The clean `\audit` surface ([STL-302]): `SELECT * FROM stele_audit('t')`
    /// returns per-version `(txid, op, hash, prev_hash)` over the durable hash chain
    /// plus the global verdict columns, the chain verifies, and the per-version links
    /// chain (first from genesis, each later `prev_hash` the previous `hash`).
    #[test]
    fn stele_audit_query_routes_through_execute() {
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk, ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account (id, balance) VALUES (1, 100)",
            "UPDATE account SET balance = 250 WHERE id = 1",
            "INSERT INTO account (id, balance) VALUES (2, 500)",
        ] {
            engine.execute(&parse_one(sql)).expect("write");
        }

        let StatementOutcome::Rows(audit) = engine
            .execute(&parse_one("SELECT * FROM stele_audit('account')"))
            .expect("audit")
        else {
            panic!("stele_audit returns rows");
        };
        assert_eq!(
            audit
                .columns
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            vec![
                "txid",
                "op",
                "hash",
                "prev_hash",
                "chain_ok",
                "chain_len",
                "chain_head",
            ],
        );

        let text = |cell: &Option<Vec<u8>>| -> String {
            match ScalarValue::decode(LogicalType::Text, cell.as_ref().expect("text cell")) {
                Ok(ScalarValue::Text(s)) => s,
                _ => panic!("expected a text cell"),
            }
        };
        let flag = |cell: &Option<Vec<u8>>| -> bool {
            matches!(
                ScalarValue::decode(LogicalType::Bool, cell.as_ref().expect("bool cell")),
                Ok(ScalarValue::Bool(true)),
            )
        };

        // Three versions (key 1 insert+update, key 2 insert), oldest first; the
        // verdict rides every row — the chain is intact, three links.
        assert_eq!(audit.rows.len(), 3);
        for row in &audit.rows {
            assert!(flag(&row[4]), "clean chain verifies");
        }
        assert!(
            matches!(
                ScalarValue::decode(LogicalType::Int8, audit.rows[0][5].as_ref().expect("len")),
                Ok(ScalarValue::Int8(3)),
            ),
            "three commit records in the chain",
        );

        // This workload's key order matches its commit order, so the version rows are
        // in chain order: the first chains from genesis, and each later version's
        // `prev_hash` is the previous version's `hash` — the links the renderer draws.
        assert_eq!(
            text(&audit.rows[0][3]),
            Digest::ZERO.to_hex(),
            "the first commit chains from genesis",
        );
        assert_eq!(text(&audit.rows[1][3]), text(&audit.rows[0][2]));
        assert_eq!(text(&audit.rows[2][3]), text(&audit.rows[1][2]));
    }

    /// Tamper-evidence ([ADR-0031], testing-strategy §4): a clean session audits
    /// intact; rewriting a historical commit record on disk — well-framed and
    /// re-CRC'd, the forgery an operator could attempt — flips the `\audit` verdict
    /// to broken, because the next record's `prev_hash` no longer matches.
    #[test]
    fn audit_detects_a_tampered_commit_record() {
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account (id, balance) VALUES (1, 100)",
            "UPDATE account SET balance = 250 WHERE id = 1",
            "INSERT INTO account (id, balance) VALUES (2, 500)",
        ] {
            engine.execute(&parse_one(sql)).expect("write");
        }

        let verdict = |engine: &mut SessionEngine<ZeroClock, MemDisk>| -> bool {
            let StatementOutcome::Rows(r) = engine
                .execute(&parse_one("SELECT * FROM stele_audit('account')"))
                .expect("audit")
            else {
                panic!("rows");
            };
            matches!(
                ScalarValue::decode(LogicalType::Bool, r.rows[0][4].as_ref().expect("verdict")),
                Ok(ScalarValue::Bool(true)),
            )
        };
        assert!(verdict(&mut engine), "the clean chain verifies");

        forge_first_commit_record(&disk);

        assert!(
            !verdict(&mut engine),
            "the tampered chain is detected — the verdict flips to broken",
        );
    }

    /// Recovery re-verifies the commit chain (extending STL-178's recovery
    /// verification to the live server, [ADR-0031]): a tampered historical record
    /// refuses recovery rather than serving forged history.
    #[test]
    fn recovery_fails_closed_on_a_tampered_commit_chain() {
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account (id, balance) VALUES (1, 100)",
            "INSERT INTO account (id, balance) VALUES (2, 500)",
        ] {
            engine.execute(&parse_one(sql)).expect("write");
        }
        drop(engine);

        forge_first_commit_record(&disk);

        assert!(
            matches!(
                SessionEngine::recover(disk, ZeroClock),
                Err(EngineError::CommitChain(_)),
            ),
            "recovery fails closed on a broken commit chain",
        );
    }

    /// Rewrite the first commit record on disk as a well-framed, correctly-CRC'd but
    /// *different* record — the forgery an operator could attempt. The frame still
    /// parses and passes CRC, so [`commit_log::replay`] accepts it; its hash no
    /// longer matches record 1's `prev_hash`, so the chain breaks at record 1.
    fn forge_first_commit_record(disk: &MemDisk) {
        use stele_storage::backend::DiskFile as _;
        use stele_storage::checksum::crc32c;

        const FRAME: usize = 8 + stele_txn::COMMIT_RECORD_LEN + 4;
        let file = disk
            .open(crate::commit_log::COMMIT_LOG_FILENAME)
            .expect("open");
        let len = usize::try_from(file.len()).expect("small file");
        assert!(len >= FRAME, "at least one record on disk to forge");
        let mut bytes = vec![0u8; len];
        file.read_at(0, &mut bytes).expect("read");

        let forged = CommitRecord {
            txn_id: TxnId(9_999),
            commit_ts: SystemTimeMicros(42),
            seq: 1,
            prev_hash: Digest::ZERO,
        };
        let mut frame = Vec::with_capacity(FRAME);
        frame.extend_from_slice(b"STCM");
        frame.extend_from_slice(
            &u32::try_from(stele_txn::COMMIT_RECORD_LEN)
                .expect("fits u32")
                .to_le_bytes(),
        );
        frame.extend_from_slice(&forged.encode());
        let crc = crc32c(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());

        bytes.splice(0..FRAME, frame);
        disk.remove(crate::commit_log::COMMIT_LOG_FILENAME)
            .expect("remove");
        disk.create(crate::commit_log::COMMIT_LOG_FILENAME)
            .expect("create")
            .append(&bytes)
            .expect("append");
    }

    /// Recovery re-verifies the **catalog** hash chain ([ADR-0031], [STL-307]):
    /// the untampered catalog recovers cleanly, but a tampered DDL record refuses
    /// recovery rather than serving forged catalog history — the catalog-log half
    /// of the commit-chain fail-closed guarantee (invariant 10), the engine-level
    /// tamper oracle (testing-strategy §4).
    #[test]
    fn recovery_fails_closed_on_a_tampered_catalog_chain() {
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one(
                "CREATE TABLE ledger (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create 2");
        drop(engine);

        // Baseline: the untampered two-record catalog chain recovers cleanly.
        SessionEngine::recover(disk.clone(), ZeroClock).expect("clean catalog recovers");

        forge_first_catalog_record(&disk);

        assert!(
            matches!(
                SessionEngine::recover(disk, ZeroClock),
                Err(EngineError::CatalogLog(_)),
            ),
            "recovery fails closed on a broken catalog chain",
        );
    }

    /// Rewrite the first catalog record on disk in place — flip a payload byte
    /// and re-CRC it, the well-framed forgery a privileged operator could
    /// attempt. The frame still parses and passes CRC, so [`catalog_log::replay`]
    /// gets past the CRC gate; but its hash no longer matches the *second*
    /// record's `prev_hash`, so the chain breaks at record 1.
    fn forge_first_catalog_record(disk: &MemDisk) {
        use stele_storage::backend::DiskFile as _;
        use stele_storage::checksum::crc32c;

        // Frame: magic(4) | payload_len(4 LE) | prev_hash(32) | payload | crc(4).
        const HEADER: usize = 8;
        const PREV_HASH: usize = 32;
        const CRC: usize = 4;
        let file = disk
            .open(crate::catalog_log::CATALOG_LOG_FILENAME)
            .expect("open");
        let len = usize::try_from(file.len()).expect("small file");
        let mut bytes = vec![0u8; len];
        file.read_at(0, &mut bytes).expect("read");

        let payload_len = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")) as usize;
        let payload_start = HEADER + PREV_HASH;
        let crc_start = payload_start + payload_len;
        // Flip a `CreateTable.at` byte (still decodes; the content just differs)
        // and recompute the CRC, leaving the genesis prev_hash so only the hash
        // chain — not the CRC — catches the forgery.
        bytes[payload_start + 1] ^= 0xFF;
        let crc = crc32c(&bytes[..crc_start]);
        bytes[crc_start..crc_start + CRC].copy_from_slice(&crc.to_le_bytes());

        disk.remove(crate::catalog_log::CATALOG_LOG_FILENAME)
            .expect("remove");
        disk.create(crate::catalog_log::CATALOG_LOG_FILENAME)
            .expect("create")
            .append(&bytes)
            .expect("append");
    }

    /// Observing the clock for a read snapshot must not let a later commit slide
    /// at or under it, even when the inner clock stalls or steps backwards —
    /// `observe` folds the reading into the high-water mark, so the next commit
    /// is strictly greater and a pinned `BEGIN` snapshot stays consistent.
    #[test]
    fn commits_after_an_observed_snapshot_stay_strictly_later() {
        let clock = SteppedClock::new(1_000_000_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one(
                "INSERT INTO account (id, balance) VALUES (1, 100)",
            ))
            .expect("insert");

        // Pin a transaction snapshot at t+10s, then step the clock *backwards*
        // before a concurrent auto-commit writes.
        clock.set(1_010_000_000);
        let mut txn = engine.begin();
        let pinned = engine.commit_clock();
        clock.set(1_005_000_000);
        engine
            .execute(&parse_one("UPDATE account SET balance = 250 WHERE id = 1"))
            .expect("auto-commit update");
        assert!(
            engine.commit_clock() > pinned,
            "a commit after an observed snapshot lands strictly past it, \
             even against a backwards-stepping inner clock"
        );

        // The pinned snapshot still reads the pre-update value; the live read
        // (a fresh observation) sees the update.
        let StatementOutcome::Rows(in_txn) = engine
            .execute_in_txn(&parse_one("SELECT balance FROM account"), &mut txn)
            .expect("select in txn")
        else {
            panic!("rows");
        };
        assert_eq!(in_txn.rows, vec![vec![cell(Some(ScalarValue::Int4(100)))]]);
        drop(txn);
        let StatementOutcome::Rows(live) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("live select")
        else {
            panic!("rows");
        };
        assert_eq!(live.rows, vec![vec![cell(Some(ScalarValue::Int4(250)))]]);
    }

    #[test]
    fn create_then_insert_then_select_within_one_session() {
        let mut engine = session();

        // (1) CREATE TABLE — registers the table and stands up its tiers.
        let created = engine.execute(&parse_one(CREATE)).expect("create");
        assert_eq!(
            created,
            StatementOutcome::Ddl {
                tag: "CREATE TABLE"
            }
        );

        // (2) INSERT (id=1, balance=100) — opaque payload at v0.1.
        engine
            .insert(
                "account",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"100".to_vec()),
                0,
                TxnId(1),
                Principal::new(b"demo".to_vec()),
            )
            .expect("insert");

        // (3) SELECT — reads the just-inserted row back, proving the tiers the
        // CREATE stood up are the same ones the INSERT wrote and the SELECT reads:
        // state persists across statements on the one session.
        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT id, balance FROM account"))
            .expect("select")
        else {
            panic!("SELECT must return rows");
        };
        assert_eq!(result.rows.len(), 1, "exactly the one inserted row");
        assert_eq!(
            result.columns,
            vec![
                ("id".to_owned(), LogicalType::Int4),
                ("balance".to_owned(), LogicalType::Int4),
            ],
            "the projection names the key and payload columns"
        );
        assert_eq!(
            payload_column(&result),
            vec![b"100".to_vec()],
            "the inserted balance reads back"
        );
    }

    #[test]
    fn describe_resolves_a_parameterized_select_without_its_parameters() {
        // The statement-level Describe path (STL-212): a prepared `SELECT … WHERE
        // id = $1` is described *before* Bind, so its parameter has no value. The
        // output shape is the projection over the schema — independent of the
        // filter — so describe resolves it with the placeholder still present and
        // without scanning (no INSERT needed).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let columns = engine
            .describe(&parse_one("SELECT id, balance FROM account WHERE id = $1"))
            .expect("describe")
            .expect("a SELECT returns a row description");
        assert_eq!(
            columns,
            vec![
                ("id".to_owned(), LogicalType::Int4),
                ("balance".to_owned(), LogicalType::Int4),
            ]
        );

        // A named single-column projection narrows the description to that column.
        let one = engine
            .describe(&parse_one("SELECT balance FROM account"))
            .expect("describe")
            .expect("rows");
        assert_eq!(one, vec![("balance".to_owned(), LogicalType::Int4)]);
    }

    #[test]
    fn describe_returns_none_for_a_statement_with_no_rows() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        // DML, DDL, and an admin command all return no result columns — the wire
        // front end answers `Describe` on these with `NoData`.
        for sql in ["INSERT INTO account VALUES (1, 100)", CREATE, "CHECKPOINT"] {
            assert_eq!(
                engine.describe(&parse_one(sql)).expect("describe"),
                None,
                "{sql} returns no rows"
            );
        }
    }

    #[test]
    fn describe_surfaces_an_unknown_table() {
        let engine = session();
        // No `account` created — describing a read of it is the same undefined-table
        // error the read path raises, not a silent empty description.
        let err = engine
            .describe(&parse_one("SELECT id FROM account WHERE id = $1"))
            .expect_err("unknown table");
        assert!(
            matches!(err, EngineError::Select(SelectError::UnknownTable(_))),
            "got {err:?}"
        );
    }

    #[test]
    fn describe_in_txn_resolves_at_the_transactions_pinned_snapshot() {
        // A statement-level Describe inside a BEGIN block must resolve the shape at
        // the transaction's pinned snapshot, not the current committed one, so it
        // agrees with the rows the portal Execute returns under snapshot isolation.
        let mut engine = session();
        // Pin a snapshot *before* the table exists.
        let txn = engine.begin();
        engine.execute(&parse_one(CREATE)).expect("create");

        // At the current snapshot the table resolves and describes...
        let now = engine
            .describe(&parse_one("SELECT id, balance FROM account"))
            .expect("describe")
            .expect("rows");
        assert_eq!(now.len(), 2);
        // ...but the transaction's pinned snapshot predates the CREATE, so the same
        // statement resolves against a catalog where `account` is not yet live —
        // the description tracks the snapshot the portal Execute reads at.
        assert!(
            engine
                .describe_in_txn(&parse_one("SELECT id, balance FROM account"), &txn)
                .is_err(),
            "account is not live at the pinned snapshot"
        );
    }

    #[test]
    fn select_before_insert_is_empty_but_resolves_the_table() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select")
        else {
            panic!("SELECT must return rows");
        };
        assert_eq!(result.rows.len(), 0, "no rows yet, but the table resolves");
    }

    #[test]
    fn select_on_a_single_column_table_keeps_header_and_cells_aligned() {
        // A one-column table projects only the business key, so the header and the
        // materialized cells stay the same width — no silent truncation/mislabel
        // from a fixed two-column projection over a narrower schema.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE solo (id INT PRIMARY KEY) WITH SYSTEM VERSIONING",
            ))
            .expect("create single-column table");
        engine
            .insert(
                "solo",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(Vec::new()),
                0,
                TxnId(1),
                Principal::new(b"demo".to_vec()),
            )
            .expect("insert");

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT id FROM solo"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            result.columns,
            vec![("id".to_owned(), LogicalType::Int4)],
            "only the key column is projected"
        );
        assert!(
            result
                .rows
                .iter()
                .all(|row| row.len() == result.columns.len()),
            "every row has exactly one cell, matching the header"
        );
    }

    #[test]
    fn update_is_visible_to_a_later_select() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let who = Principal::new(b"demo".to_vec());
        engine
            .insert(
                "account",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"100".to_vec()),
                0,
                TxnId(1),
                who.clone(),
            )
            .expect("insert");
        engine
            .update(
                "account",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"250".to_vec()),
                0,
                TxnId(2),
                who,
            )
            .expect("update");

        let StatementOutcome::Rows(batch) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        // The latest read sees the post-update value; history (100) is still on
        // disk for an AS OF read, which STL-147 will route over the wire.
        assert_eq!(payload_column(&batch), &[b"250".to_vec()]);
    }

    #[test]
    fn two_tables_do_not_collide_on_the_shared_disk() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create account");
        engine
            .execute(&parse_one(
                "CREATE TABLE ledger (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create ledger");
        let who = Principal::new(b"demo".to_vec());
        engine
            .insert(
                "account",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"aaa".to_vec()),
                0,
                TxnId(1),
                who.clone(),
            )
            .expect("insert account");
        engine
            .insert(
                "ledger",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"bbb".to_vec()),
                0,
                TxnId(2),
                who,
            )
            .expect("insert ledger");

        // Same business key in both tables, distinct payloads — the namespaced
        // tiers keep them apart. Projecting the value column reads each back.
        let StatementOutcome::Rows(a) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("a")
        else {
            panic!("rows");
        };
        let StatementOutcome::Rows(l) = engine
            .execute(&parse_one("SELECT amount FROM ledger"))
            .expect("l")
        else {
            panic!("rows");
        };
        assert_eq!(payload_column(&a), &[b"aaa".to_vec()]);
        assert_eq!(payload_column(&l), &[b"bbb".to_vec()]);
    }

    #[test]
    fn drop_table_makes_it_unresolvable() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let dropped = engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        assert_eq!(dropped, StatementOutcome::Ddl { tag: "DROP TABLE" });
        // A SELECT against the dropped name no longer binds.
        let err = engine
            .execute(&parse_one("SELECT id FROM account"))
            .unwrap_err();
        assert!(matches!(err, EngineError::Select(_)), "got {err:?}");
    }

    #[test]
    fn dml_against_a_dropped_table_is_refused() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        // The tier is kept for history, but the catalog no longer resolves the
        // name — a typed write must not mutate a logically dropped table.
        let err = engine
            .insert(
                "account",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"100".to_vec()),
                0,
                TxnId(1),
                Principal::new(b"demo".to_vec()),
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::UnknownTable(_)), "got {err:?}");
    }

    #[test]
    fn recreate_with_a_different_valid_time_policy_is_refused() {
        let mut engine = session();
        // System-only table, then dropped (tier retained for history).
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        // Re-create the same name as a valid-time table: the retained tier's
        // writer was opened system-only, so the policy change is refused rather
        // than silently enforcing the stale policy.
        let err = engine
            .execute(&parse_one(
                "CREATE TABLE account (id INT PRIMARY KEY, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ValidTimePolicyChange { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn valid_time_as_of_resolves_the_cell_live_on_both_axes() {
        // The valid-axis sibling of the system-time identity demo ([STL-164]): a
        // single key whose value differs across two disjoint valid windows, where
        // the later version also superseded the earlier one on the *system* axis.
        //
        //   INSERT id=1, balance=100, valid [10, 20)  → commit c1
        //   UPDATE id=1, balance=250, valid [20, 30)  → commit c2
        //
        // Pinning both axes with literal-microsecond `AS OF` instants
        // (`resolve_as_of` reads a bare integer as micros) proves `run_select`
        // threads `BoundSelect::valid_snapshot` into the both-axes scan: the same
        // valid instant returns different cells at different system snapshots, and
        // the same system snapshot returns different cells at different valid
        // instants — neither axis alone explains the four answers. The underlying
        // resolution is the oracle-backed one from [STL-163]; this asserts only
        // that the engine glue reaches it.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE account (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");

        let who = || Principal::new(b"demo".to_vec());
        let key = || business_key(&ScalarValue::Int4(1));
        // The stored payload packs all three value columns (balance, vf, vt); the
        // valid interval itself rides the framed prefix `engine.insert` adds, so
        // the period cells are redundant scaffolding here and only `balance` is
        // asserted (materializing the period columns from the interval is the
        // deferred binder/executor work this ticket explicitly excludes).
        let payload = |balance: i32, from: i64, to: i64| {
            row_codec::encode_payload(&[
                cell(Some(ScalarValue::Int4(balance))),
                cell(Some(ScalarValue::Timestamp(from))),
                cell(Some(ScalarValue::Timestamp(to))),
            ])
        };
        let iv = |from: i64, to: i64| {
            ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed")
        };

        let c1 = engine
            .insert(
                "account",
                key(),
                Some(iv(10, 20)),
                payload(100, 10, 20),
                0,
                TxnId(1),
                who(),
            )
            .expect("insert")
            .commit;
        let c2 = engine
            .update(
                "account",
                key(),
                Some(iv(20, 30)),
                payload(250, 20, 30),
                0,
                TxnId(2),
                who(),
            )
            .expect("update")
            .commit;
        assert!(c1.0 < c2.0, "the update commits strictly after the insert");

        // The single `balance` cell of a both-axes `SELECT`, or `None` when no
        // version is live on both axes at `(sys, valid)`.
        let mut balance = |sys: i64, valid: i64| -> Option<Vec<u8>> {
            let sql = format!(
                "SELECT balance FROM account \
                 FOR SYSTEM_TIME AS OF {sys} FOR VALID_TIME AS OF {valid}"
            );
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select")
            else {
                panic!("SELECT must return rows");
            };
            assert!(
                r.rows.len() <= 1,
                "one key resolves to at most one live version, got {}",
                r.rows.len()
            );
            r.rows
                .into_iter()
                .next()
                .and_then(|row| row.into_iter().next().expect("the projected balance cell"))
        };

        // Pre-update system + first valid window → 100.
        assert_eq!(balance(c1.0, 15), cell(Some(ScalarValue::Int4(100))));
        // Post-update system + second valid window → 250.
        assert_eq!(balance(c2.0, 25), cell(Some(ScalarValue::Int4(250))));
        // Post-update system + first valid window → none: v1 is superseded on the
        // system axis and v2's window `[20, 30)` excludes 15. (Only the valid axis
        // differs from the 250 case — so the valid instant is load-bearing.)
        assert_eq!(balance(c2.0, 15), None);
        // Pre-update system + second valid window → none: v1 is system-live but its
        // window `[10, 20)` excludes 25. (Only the system axis differs from the 100
        // case — so the system instant is load-bearing.)
        assert_eq!(balance(c1.0, 25), None);
    }

    #[test]
    fn both_axes_as_of_join_reads_every_input_at_one_consistent_snapshot() {
        // The join sibling of the single-table test above ([STL-243]): a join under
        // `FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS OF v` must read *every* input
        // at the one pinned `(sys, valid)` point (docs/16 §8). `acct` carries two
        // both-axes versions of key 1; `tier` carries one wide-window version; the
        // join on the key proves the engine honored both axes on the `acct` side and
        // the same snapshot on the `tier` side — and that an `acct` row absent at
        // `(s, v)` drops the joined pair (inner join), not just its own columns.
        let mut engine = session();
        for ddl in [
            "CREATE TABLE acct (k INT PRIMARY KEY, bal INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            "CREATE TABLE tier (k INT PRIMARY KEY, lvl INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
        ] {
            engine
                .execute(&parse_one(ddl))
                .expect("create valid-time table");
        }

        let who = || Principal::new(b"demo".to_vec());
        let key = || business_key(&ScalarValue::Int4(1));
        let payload = |value: i32, from: i64, to: i64| {
            row_codec::encode_payload(&[
                cell(Some(ScalarValue::Int4(value))),
                cell(Some(ScalarValue::Timestamp(from))),
                cell(Some(ScalarValue::Timestamp(to))),
            ])
        };
        let iv = |from: i64, to: i64| {
            ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed")
        };

        // tier key 1: lvl 5 valid [0, 40), committed first so it is system-live at
        // both acct snapshots below. acct key 1: bal 100 valid [10, 20) (c1), then
        // bal 250 valid [20, 30) (c2, superseding v1 on the system axis).
        engine
            .insert(
                "tier",
                key(),
                Some(iv(0, 40)),
                payload(5, 0, 40),
                0,
                TxnId(1),
                who(),
            )
            .expect("insert tier");
        let c1 = engine
            .insert(
                "acct",
                key(),
                Some(iv(10, 20)),
                payload(100, 10, 20),
                0,
                TxnId(2),
                who(),
            )
            .expect("insert acct v1")
            .commit;
        let c2 = engine
            .update(
                "acct",
                key(),
                Some(iv(20, 30)),
                payload(250, 20, 30),
                0,
                TxnId(3),
                who(),
            )
            .expect("update acct v2")
            .commit;

        let mut join = |sys: i64, valid: i64| -> Vec<Vec<Option<Vec<u8>>>> {
            let sql = format!(
                "SELECT acct.bal, tier.lvl FROM acct JOIN tier ON acct.k = tier.k \
                 FOR SYSTEM_TIME AS OF {sys} FOR VALID_TIME AS OF {valid}"
            );
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select")
            else {
                panic!("SELECT must return rows");
            };
            sorted(r.rows)
        };

        // Pre-update system + acct's first window → bal 100 joined to tier lvl 5.
        assert_eq!(join(c1.0, 15), vec![vec![i4(100), i4(5)]]);
        // Post-update system + second window → bal 250 joined to the same tier row.
        assert_eq!(join(c2.0, 25), vec![vec![i4(250), i4(5)]]);
        // Post-update system + first window → acct has no live version (v1 superseded,
        // v2's [20, 30) excludes 15), so the inner join drops the pair entirely.
        assert!(join(c2.0, 15).is_empty());
        // Pre-update system + second window → acct's [10, 20) excludes 25 → empty.
        assert!(join(c1.0, 25).is_empty());
    }

    #[test]
    fn valid_time_dml_round_trips_over_sql() {
        // STL-194: the same both-axes scenario as above, but the *write* side now
        // runs entirely through the SQL DML path — `INSERT`/`UPDATE` naming the
        // period columns, the binder lifting their bounds into the framed interval
        // — instead of the typed in-process `insert`/`update` with a hand-built
        // payload. This is the round-trip the ticket's Definition of Done demands:
        // a valid interval written over SQL, read back at a `FOR VALID_TIME AS OF`
        // point.
        //
        //   INSERT id=1, balance=100, valid [10, 20)
        //   UPDATE id=1, balance=250, valid [20, 30)   (a valid-time RMW)
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE account (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");

        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100, 10, 20)"))
            .expect("insert over SQL");
        // The write advanced the commit clock to its `sys_from`; read it back as
        // the insert's commit instant (no public DML-commit accessor at v0.2).
        let c1 = engine.clock.current().0;
        engine
            .execute(&parse_one(
                "UPDATE account SET balance = 250, vf = 20, vt = 30 WHERE id = 1",
            ))
            .expect("update over SQL");
        let c2 = engine.clock.current().0;
        assert!(c1 < c2, "the update commits strictly after the insert");

        let mut balance = |sys: i64, valid: i64| -> Option<Vec<u8>> {
            let sql = format!(
                "SELECT balance FROM account \
                 FOR SYSTEM_TIME AS OF {sys} FOR VALID_TIME AS OF {valid}"
            );
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select")
            else {
                panic!("SELECT must return rows");
            };
            // Static assert message — interpolating the result here trips CodeQL's
            // (false) cleartext-logging taint on the row payloads.
            assert!(
                r.rows.len() <= 1,
                "one key resolves to at most one live version"
            );
            r.rows
                .into_iter()
                .next()
                .and_then(|row| row.into_iter().next().expect("the projected balance cell"))
        };

        // The four corners: each cell needs *both* axes to agree, proving the SQL
        // INSERT and the SQL UPDATE each wrote a distinct, correct valid interval.
        assert_eq!(balance(c1, 15), cell(Some(ScalarValue::Int4(100))));
        assert_eq!(balance(c2, 25), cell(Some(ScalarValue::Int4(250))));
        assert_eq!(balance(c2, 15), None);
        assert_eq!(balance(c1, 25), None);
    }

    #[test]
    fn insert_opens_a_valid_period_to_infinity_over_sql() {
        // The open-period default: an INSERT naming only the start bound opens
        // `[from, +∞)`, so the fact is valid at every instant at or after `from`.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE account (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        engine
            .execute(&parse_one(
                "INSERT INTO account (id, balance, vf) VALUES (1, 100, 50)",
            ))
            .expect("insert open-ended period");
        let c = engine.clock.current().0;

        let mut balance = |valid: i64| -> Option<Vec<u8>> {
            let sql = format!(
                "SELECT balance FROM account FOR SYSTEM_TIME AS OF {c} FOR VALID_TIME AS OF {valid}"
            );
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select")
            else {
                panic!("rows");
            };
            r.rows
                .into_iter()
                .next()
                .and_then(|row| row.into_iter().next().expect("balance cell"))
        };

        assert_eq!(balance(49), None, "before the open period's start");
        assert_eq!(
            balance(50),
            cell(Some(ScalarValue::Int4(100))),
            "at the start"
        );
        assert_eq!(
            balance(1_000_000),
            cell(Some(ScalarValue::Int4(100))),
            "far past the start — the period never closes"
        );
    }

    // --- STL-226: valid-time UPDATE RMW reads its prior version across tiers --

    /// A fresh session holding an empty valid-time `acct(id, balance, vf, vt)`.
    fn valid_acct() -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        engine
    }

    /// `acct`'s `balance` for `id = 1`, read over SQL at the latest system time and
    /// the given valid instant — `None` when no version is live there.
    fn read_balance(engine: &mut SessionEngine<ZeroClock, MemDisk>, valid: i64) -> Option<Vec<u8>> {
        let c = engine.clock.current().0;
        let sql = format!(
            "SELECT balance FROM acct FOR SYSTEM_TIME AS OF {c} FOR VALID_TIME AS OF {valid}"
        );
        let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select") else {
            panic!("SELECT must return rows");
        };
        // A pinned (sys, valid) point resolves to at most one live version per key;
        // a duplicate would mean the scan returned two versions for one key, which
        // these regression cases must catch loudly rather than silently take the
        // first of.
        assert!(
            r.rows.len() <= 1,
            "at most one version is live at a pinned (sys, valid) point — got {}",
            r.rows.len(),
        );
        r.rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().expect("balance cell"))
    }

    #[test]
    fn valid_time_update_after_flush_reads_prior_version_from_a_sealed_segment() {
        // STL-226: a valid-time UPDATE is a read-modify-write — it reads the prior
        // live row's value cells so columns the SET does not name keep their prior
        // value. When that prior version has been sealed into a segment (after a
        // CHECKPOINT / FLUSH, or once the delta spilled), its payload is stored
        // *bare* with the interval in the segment's own ValidFrom/ValidTo columns
        // ([STL-163]) — unlike a delta prior version, whose payload is framed
        // ([STL-194]). The RMW read must strip the frame only where one exists; the
        // pre-fix code stripped a fixed 16-byte prefix unconditionally, so a sealed
        // payload's real row bytes were drained as a phantom prefix and the row
        // codec rejected the remainder (`RowCodecError::TrailingBytes`). Each
        // sub-case writes a prior version, flushes to seal it, then updates the key.

        // (a) bounded prior period — the literal repro.
        {
            let mut engine = valid_acct();
            engine
                .execute(&parse_one("INSERT INTO acct VALUES (1, 10, 0, 20)"))
                .expect("insert bounded period");
            engine.flush().expect("seal the delta into a segment");
            engine
                .execute(&parse_one(
                    "UPDATE acct SET balance = 11, vf = 0, vt = 20 WHERE id = 1",
                ))
                .expect("UPDATE reads the prior version from the sealed segment");
            assert_eq!(
                read_balance(&mut engine, 5),
                cell(Some(ScalarValue::Int4(11))),
                "the updated balance is live across the new [0, 20) period",
            );
        }

        // (b) open-ended prior period — the interval frames the +∞ sentinel, a
        // distinct payload shape from the bounded case.
        {
            let mut engine = valid_acct();
            engine
                .execute(&parse_one(
                    "INSERT INTO acct (id, balance, vf) VALUES (1, 10, 0)",
                ))
                .expect("insert open period");
            engine.flush().expect("seal the delta into a segment");
            engine
                .execute(&parse_one(
                    "UPDATE acct SET balance = 11, vf = 0 WHERE id = 1",
                ))
                .expect("UPDATE reads the prior open-period version from the sealed segment");
            assert_eq!(
                read_balance(&mut engine, 1_000_000),
                cell(Some(ScalarValue::Int4(11))),
                "the updated balance is live across the re-opened [0, +∞) period",
            );
        }

        // (c) DELETE never decodes the prior payload, so it was unaffected — assert
        // it still closes the period after a flush (control).
        {
            let mut engine = valid_acct();
            engine
                .execute(&parse_one("INSERT INTO acct VALUES (1, 10, 0, 20)"))
                .expect("insert");
            engine.flush().expect("seal");
            engine
                .execute(&parse_one("DELETE FROM acct WHERE id = 1"))
                .expect("DELETE closes the prior period without decoding its payload");
            assert_eq!(read_balance(&mut engine, 5), None, "the period is closed");
        }
    }

    #[test]
    fn valid_time_update_after_flush_preserves_unset_columns_from_the_sealed_read() {
        // STL-226 (RMW correctness): an UPDATE naming only the period columns keeps
        // `balance` at its prior value, read back from the *sealed* segment. This
        // proves the prior payload decodes correctly across the tier boundary — not
        // merely that the UPDATE no longer errors.
        let mut engine = valid_acct();
        engine
            .execute(&parse_one("INSERT INTO acct VALUES (1, 42, 0, 20)"))
            .expect("insert balance=42 over [0, 20)");
        engine.flush().expect("seal the delta into a segment");
        // Move the valid window to [5, 20) without naming balance; the RMW must
        // carry balance=42 over from the sealed prior version.
        engine
            .execute(&parse_one("UPDATE acct SET vf = 5, vt = 20 WHERE id = 1"))
            .expect("period-only UPDATE reads balance from the sealed segment");

        assert_eq!(
            read_balance(&mut engine, 10),
            cell(Some(ScalarValue::Int4(42))),
            "balance is preserved from the sealed prior version across the new window",
        );
        assert_eq!(
            read_balance(&mut engine, 1),
            None,
            "before the new window's start the row is not valid",
        );
    }

    // --- STL-194: the SQL valid-time DML correctness oracle ------------------

    /// A deterministic splitmix64 — a seed replays an identical workload, with no
    /// dependency on the sim crate (this oracle drives the SQL path, not storage).
    struct ValidOracleRng(u64);
    impl ValidOracleRng {
        const fn new(seed: u64) -> Self {
            Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
        }
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        /// A uniform value in `0..n` (`n > 0`), no `as` casts so the pedantic
        /// truncation lints stay clean.
        fn below(&mut self, n: i64) -> i64 {
            let n = u64::try_from(n).expect("positive bound");
            i64::try_from(self.next() % n).expect("fits i64")
        }
    }

    /// One naïve, obviously-correct version tuple: both axes as half-open
    /// intervals plus the value. `sys_to == i64::MAX` is an open system period;
    /// `vto == i64::MAX` an open valid period.
    #[derive(Clone)]
    struct ValidRefVersion {
        sys_from: i64,
        sys_to: i64,
        vfrom: i64,
        vto: i64,
        balance: i32,
    }

    /// The naïve bitemporal reference ([STL-163]'s, re-expressed for the SQL
    /// path): per key, an append-only list of version tuples maintained by the
    /// same INSERT/UPDATE/DELETE semantics the engine uses. Far too simple to be
    /// wrong, which is the point — an independent check on the binder's interval
    /// lift and the engine's framed-payload write.
    #[derive(Default)]
    struct ValidRefModel {
        versions: BTreeMap<i32, Vec<ValidRefVersion>>,
    }
    impl ValidRefModel {
        fn open_idx(&self, k: i32) -> Option<usize> {
            self.versions
                .get(&k)
                .and_then(|vs| vs.iter().position(|v| v.sys_to == i64::MAX))
        }
        fn close(&mut self, k: i32, commit: i64) {
            let i = self.open_idx(k).expect("a live key has one open period");
            self.versions.get_mut(&k).expect("key present")[i].sys_to = commit;
        }
        fn insert(&mut self, k: i32, commit: i64, vfrom: i64, vto: i64, balance: i32) {
            self.versions.entry(k).or_default().push(ValidRefVersion {
                sys_from: commit,
                sys_to: i64::MAX,
                vfrom,
                vto,
                balance,
            });
        }
        fn update(&mut self, k: i32, commit: i64, vfrom: i64, vto: i64, balance: i32) {
            self.close(k, commit);
            self.insert(k, commit, vfrom, vto, balance);
        }

        /// The per-key `(id bytes → balance bytes)` map live on both axes at
        /// `(s, v)`, encoded the way a `SELECT id, balance` returns them.
        /// `inclusive_vto` flips the valid upper bound to inclusive — the
        /// deliberately-wrong variant that proves the differential has teeth.
        fn cell(&self, s: i64, v: i64, inclusive_vto: bool) -> BTreeMap<Vec<u8>, Vec<u8>> {
            let mut out = BTreeMap::new();
            for (k, vs) in &self.versions {
                for ver in vs {
                    let sys_ok = ver.sys_from <= s && s < ver.sys_to;
                    let valid_ok = ver.vfrom <= v
                        && (if inclusive_vto {
                            v <= ver.vto
                        } else {
                            v < ver.vto
                        });
                    if sys_ok && valid_ok {
                        let id = encode_value(&ScalarValue::Int4(*k));
                        let balance = encode_value(&ScalarValue::Int4(ver.balance));
                        assert!(out.insert(id, balance).is_none(), "one row per (s,v,k)");
                    }
                }
            }
            out
        }
    }

    /// The engine's `(id bytes → balance bytes)` map at `(s, v)`, read entirely
    /// over SQL with both axes pinned by literal-microsecond `AS OF` instants.
    fn read_valid_cells(
        engine: &mut SessionEngine<ZeroClock, MemDisk>,
        s: i64,
        v: i64,
    ) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let sql = format!(
            "SELECT id, balance FROM acct \
             FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}"
        );
        let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select") else {
            panic!("SELECT must return rows");
        };
        let mut out = BTreeMap::new();
        for row in r.rows {
            let id = row[0].clone().expect("the id key is never NULL");
            let balance = row[1]
                .clone()
                .expect("the balance is never NULL in this workload");
            assert!(
                out.insert(id, balance).is_none(),
                "@ (s={s}, v={v}): two live versions for one key — the at-most-one-live invariant broke",
            );
        }
        out
    }

    #[test]
    fn sql_valid_time_dml_matches_a_naive_reference() {
        // STL-194's correctness oracle. A random INSERT/UPDATE/DELETE history is
        // applied to a valid-time table **entirely over SQL**, with each write's
        // interval named in the statement and lifted by the binder onto the framed
        // payload. An exhaustive `(system, valid)` AS OF grid is then swept and the
        // engine's rows are diffed against the naïve reference. Because the
        // *resolution* is STL-163's already-oracled logic, agreement here isolates
        // the new code: the binder's interval lift and the engine's interval write
        // (including the valid-time UPDATE read-modify-write and the open-period
        // default). The teeth check (an inclusive-`vto` reference that must diverge
        // at least once) proves the half-open valid boundary is really probed.
        const KEY_POOL: i64 = 3;
        const VMAX: i64 = 10;
        const SEEDS: u64 = 48;

        let mut total_probes: u64 = 0;
        let mut rows_seen: u64 = 0;
        let mut teeth = false;

        for seed in 0..SEEDS {
            let mut rng = ValidOracleRng::new(seed);
            let mut engine = session();
            engine
                .execute(&parse_one(
                    "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                     WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
                ))
                .expect("create valid-time table");
            let create_c = engine.clock.current().0;

            let mut model = ValidRefModel::default();
            let mut alive = vec![false; usize::try_from(KEY_POOL).expect("fits")];
            let mut hi = create_c;

            let ops = 8 + rng.below(12);
            for op in 0..ops {
                let k = rng.below(KEY_POOL);
                let ku = usize::try_from(k).expect("fits");
                let ki = i32::try_from(k).expect("key fits i32");
                let balance = i32::try_from(op + 1).expect("balance fits i32");
                // A well-formed valid window inside `[0, VMAX]`, sometimes open to
                // exercise the `+∞` sentinel and the open-period default.
                let from = rng.below(VMAX);
                let open = rng.below(4) == 0;
                let to = if open {
                    i64::MAX
                } else {
                    from + 1 + rng.below(VMAX - from)
                };

                if alive[ku] && rng.below(2) == 0 {
                    engine
                        .execute(&parse_one(&format!("DELETE FROM acct WHERE id = {ki}")))
                        .expect("delete");
                    let c = engine.clock.current().0;
                    model.close(ki, c);
                    alive[ku] = false;
                    hi = hi.max(c);
                } else if alive[ku] {
                    let set = if open {
                        format!("SET balance = {balance}, vf = {from}")
                    } else {
                        format!("SET balance = {balance}, vf = {from}, vt = {to}")
                    };
                    engine
                        .execute(&parse_one(&format!("UPDATE acct {set} WHERE id = {ki}")))
                        .expect("update");
                    let c = engine.clock.current().0;
                    model.update(ki, c, from, to, balance);
                    hi = hi.max(c);
                } else {
                    let stmt = if open {
                        format!(
                            "INSERT INTO acct (id, balance, vf) VALUES ({ki}, {balance}, {from})"
                        )
                    } else {
                        format!("INSERT INTO acct VALUES ({ki}, {balance}, {from}, {to})")
                    };
                    engine.execute(&parse_one(&stmt)).expect("insert");
                    let c = engine.clock.current().0;
                    model.insert(ki, c, from, to, balance);
                    alive[ku] = true;
                    hi = hi.max(c);
                }
            }

            // Sweep both axes: system from the table's creation through one past the
            // last commit; valid across `[0, VMAX]` and one past each end.
            for s in create_c..=(hi + 1) {
                for v in 0..=(VMAX + 1) {
                    let got = read_valid_cells(&mut engine, s, v);
                    let want = model.cell(s, v, false);
                    assert_eq!(
                        got, want,
                        "seed {seed}: engine diverged from the reference at (s={s}, v={v})"
                    );
                    if got != model.cell(s, v, true) {
                        teeth = true;
                    }
                    rows_seen += u64::try_from(got.len()).expect("fits");
                    total_probes += 1;
                }
            }
        }

        assert!(
            rows_seen > 0,
            "every probe was empty — the workload resolved nothing"
        );
        assert!(
            teeth,
            "the differential never hit a half-open valid boundary — it cannot detect an off-by-one"
        );
        assert!(
            total_probes > 5_000,
            "differential probed only {total_probes} (s,v) cells — widen the sweep"
        );
    }

    // --- STL-235: the temporal-MERGE historization oracle --------------------

    /// Sweep an exhaustive `(system, valid)` AS OF grid and diff the engine's rows
    /// against the naïve reference. Returns `(probes, rows_seen, teeth)`; the
    /// teeth flag records whether an inclusive-`vto` reference would have diverged
    /// at least once, proving the half-open valid boundary is really probed. The
    /// per-cell `read_valid_cells` assertion that a key never resolves to two live
    /// versions is the **no-overlap** check; this diff is the **no-gap** check.
    fn merge_sweep_grid(
        engine: &mut SessionEngine<ZeroClock, MemDisk>,
        model: &ValidRefModel,
        span: (i64, i64, i64),
        label: &str,
        seed: u64,
    ) -> (u64, u64, bool) {
        let (create_c, hi, vmax) = span;
        let (mut probes, mut rows, mut teeth) = (0u64, 0u64, false);
        for s in create_c..=(hi + 1) {
            for v in 0..=(vmax + 1) {
                let got = read_valid_cells(engine, s, v);
                let want = model.cell(s, v, false);
                assert_eq!(
                    got, want,
                    "seed {seed} [{label}]: engine diverged from the reference at (s={s}, v={v})"
                );
                if got != model.cell(s, v, true) {
                    teeth = true;
                }
                rows += u64::try_from(got.len()).expect("fits");
                probes += 1;
            }
        }
        (probes, rows, teeth)
    }

    const MERGE_KEY_POOL: i64 = 4;
    const MERGE_VMAX: i64 = 9;

    /// Where a historizing `MERGE`'s valid-time period bounds come from — the two
    /// surfaces the oracle sweeps, which must produce the *same* timeline.
    #[derive(Clone, Copy)]
    enum MergeBoundStyle {
        /// STL-235: statement-level instant bounds (`vf = 5`), folded at bind, the
        /// same interval for every affected key.
        StatementInstant,
        /// STL-308: per-source-row bounds (`vf = s.vfrom`), each affected key
        /// carrying its own interval drawn from the source row.
        PerRowSource,
    }

    /// One seeded random bitemporal-MERGE history plus the naïve reference it was
    /// mirrored into, ready for the AS OF grid sweep.
    struct MergeHistory {
        engine: SessionEngine<ZeroClock, MemDisk>,
        disk: MemDisk,
        model: ValidRefModel,
        create_c: i64,
        hi: i64,
        merges: u64,
        deletes: u64,
    }

    /// Apply a seeded random workload of **single-key** bitemporal `MERGE`
    /// statements (one source row ⇒ one write at one commit instant) to a fresh
    /// valid-time table, with an occasional `DELETE` for an intentional deletion
    /// gap, mirroring every write into the naïve [`ValidRefModel`].
    ///
    /// `style` selects where the period bounds come from. The *same seed* drives
    /// the identical logical workload through both surfaces (statement-level
    /// instants and per-source-row source columns), so the per-row path
    /// ([STL-308]) is held to the very same timeline as the instant path
    /// ([STL-235]).
    fn build_merge_history(seed: u64, style: MergeBoundStyle) -> MergeHistory {
        // A stream distinct from the plain-DML oracle's same-seed stream.
        let mut rng = ValidOracleRng::new(seed ^ 0x5713_2735_0BAD_F00D);
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine
            .execute(&parse_one(
                "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        let create_c = engine.clock.current().0;
        let mut model = ValidRefModel::default();
        let mut alive = vec![false; usize::try_from(MERGE_KEY_POOL).expect("fits")];
        let mut hi = create_c;
        let (mut merges, mut deletes) = (0u64, 0u64);

        let ops = 6 + rng.below(10);
        for op in 0..ops {
            let k = rng.below(MERGE_KEY_POOL);
            let ku = usize::try_from(k).expect("fits");
            let ki = i32::try_from(k).expect("key fits i32");
            let balance = i32::try_from(op + 1).expect("balance fits i32");
            let from = rng.below(MERGE_VMAX);
            let open = rng.below(4) == 0;
            // The canonical open-period sentinel the engine uses when `vt` is
            // omitted — the reference model's open marker (`vto == i64::MAX`) is the
            // same value (`VALID_TIME_OPEN = ValidTimeMicros(i64::MAX)`).
            let to = if open {
                stele_common::time::VALID_TIME_OPEN.0
            } else {
                from + 1 + rng.below(MERGE_VMAX - from)
            };

            // 1-in-5 on a live key: a DELETE, the intentional deletion gap a later
            // MERGE re-opens (not-matched ⇒ insert). The gap must survive.
            if alive[ku] && rng.below(5) == 0 {
                dml(&mut engine, &format!("DELETE FROM acct WHERE id = {ki}"));
                model.close(ki, engine.clock.current().0);
                alive[ku] = false;
                deletes += 1;
                hi = hi.max(engine.clock.current().0);
                continue;
            }

            let sql = match style {
                // STL-235: statement-level instant bounds; an open period omits
                // `vt` so the engine defaults it ([`VALID_TIME_OPEN`]).
                MergeBoundStyle::StatementInstant => {
                    let set = if open {
                        format!("SET balance = s.bal, vf = {from}")
                    } else {
                        format!("SET balance = s.bal, vf = {from}, vt = {to}")
                    };
                    let (cols, vals) = if open {
                        ("(id, balance, vf)", format!("(s.id, s.bal, {from})"))
                    } else {
                        (
                            "(id, balance, vf, vt)",
                            format!("(s.id, s.bal, {from}, {to})"),
                        )
                    };
                    format!(
                        "MERGE INTO acct USING (VALUES ({ki}, {balance})) AS s (id, bal) \
                         ON acct.id = s.id \
                         WHEN MATCHED THEN UPDATE {set} \
                         WHEN NOT MATCHED THEN INSERT {cols} VALUES {vals}"
                    )
                }
                // STL-308: both bounds ride source columns. An open period passes
                // the open sentinel (`to == i64::MAX`) as the source cell, the same
                // value the model's open marker uses — so the per-row interval
                // resolves to open exactly as the instant path's omission does.
                MergeBoundStyle::PerRowSource => format!(
                    "MERGE INTO acct USING (VALUES ({ki}, {balance}, {from}, {to})) \
                     AS s (id, bal, vfrom, vto) ON acct.id = s.id \
                     WHEN MATCHED THEN UPDATE SET balance = s.bal, vf = s.vfrom, vt = s.vto \
                     WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) \
                     VALUES (s.id, s.bal, s.vfrom, s.vto)"
                ),
            };
            assert_eq!(
                dml(&mut engine, &sql),
                DmlSummary::Merge(1),
                "one row acted on"
            );
            let c = engine.clock.current().0;
            if alive[ku] {
                model.update(ki, c, from, to, balance);
            } else {
                model.insert(ki, c, from, to, balance);
                alive[ku] = true;
            }
            merges += 1;
            hi = hi.max(c);
        }
        MergeHistory {
            engine,
            disk,
            model,
            create_c,
            hi,
            merges,
            deletes,
        }
    }

    #[test]
    fn sql_temporal_merge_historization_matches_a_naive_reference() {
        // STL-235's historization oracle ([06 §4]), extended for STL-308. A random
        // workload of bitemporal `MERGE` statements is applied to a valid-time
        // table entirely over SQL: a matched row gets the joint system+valid
        // close/open and an unmatched row inserts, each carrying its valid
        // interval. The reference is the **same** naïve list-of-versions the plain
        // valid-time DML oracle uses — a MERGE arm *is* an UPDATE / INSERT — so
        // agreement isolates the new code: the binder's arm-interval lift and the
        // engine's thread of it.
        //
        // Each seed drives the identical logical workload through **two surfaces**,
        // both held to the same model:
        //   * `StatementInstant` — STL-235 statement-level instant bounds;
        //   * `PerRowSource` — STL-308 per-source-row bounds (`vf = s.vfrom`),
        //     where each affected key's interval is derived per row at execution
        //     and an open period rides the source cell as the open sentinel.
        //
        // The named property (no gaps / no overlaps unless intended) is asserted
        // three ways, each over an exhaustive `(system, valid)` grid:
        //   * **no overlaps** — `read_valid_cells` refuses to resolve a key to two
        //     live versions at any `(s, v)` (the at-most-one-live invariant);
        //   * **no unintended gaps** — the grid diff: the engine resolves a row
        //     wherever the model does and nowhere it does not, so a DELETE leaves a
        //     gap exactly where intended and a MERGE leaves none;
        //   * **survives flush + index rebuild** — the grid is re-swept after
        //     `flush()` (delta sealed into segments) and after cold-boot `recover()`
        //     (validity index rebuilt from the durable segments + WAL).
        //
        // Re-reading every system instant `< hi` after later writes also asserts the
        // bedrock audit property — a later MERGE never changes an AS OF read of
        // pre-MERGE history ([16 §7] monotonicity).
        const SEEDS: u64 = 24;
        let (mut total_probes, mut rows_seen, mut teeth) = (0u64, 0u64, false);
        let (mut merges, mut deletes) = (0u64, 0u64);

        for style in [
            MergeBoundStyle::StatementInstant,
            MergeBoundStyle::PerRowSource,
        ] {
            let label = match style {
                MergeBoundStyle::StatementInstant => "instant",
                MergeBoundStyle::PerRowSource => "per-row",
            };
            for seed in 0..SEEDS {
                let MergeHistory {
                    mut engine,
                    disk,
                    model,
                    create_c,
                    hi,
                    merges: m,
                    deletes: d,
                } = build_merge_history(seed, style);
                merges += m;
                deletes += d;
                let span = (create_c, hi, MERGE_VMAX);
                // (1) Live, from the delta tier.
                let (p1, r, t) =
                    merge_sweep_grid(&mut engine, &model, span, &format!("{label}/live"), seed);
                // (2) After flush: the delta is sealed into columnar segments.
                engine.flush().expect("flush");
                let (p2, _, _) = merge_sweep_grid(
                    &mut engine,
                    &model,
                    span,
                    &format!("{label}/post-flush"),
                    seed,
                );
                // (3) After cold-boot recovery: the validity index is rebuilt from
                // the durable segments + WAL tail — the timeline survives the rebuild.
                let mut recovered =
                    SessionEngine::recover(disk.clone(), ZeroClock).expect("recover");
                let (p3, _, _) = merge_sweep_grid(
                    &mut recovered,
                    &model,
                    span,
                    &format!("{label}/recovered"),
                    seed,
                );
                total_probes += p1 + p2 + p3;
                rows_seen += r;
                teeth |= t;
            }
        }

        assert!(
            rows_seen > 0,
            "every probe was empty — the workload resolved nothing"
        );
        assert!(
            teeth,
            "the differential never hit a half-open valid boundary — it cannot detect an off-by-one"
        );
        assert!(
            merges > 0 && deletes > 0,
            "the workload must exercise both MERGE and an intentional deletion gap (merges={merges}, deletes={deletes})"
        );
        assert!(
            total_probes > 10_000,
            "differential probed only {total_probes} (s,v) cells across live/flush/recover — widen the sweep"
        );
    }

    #[test]
    fn recreate_with_the_same_policy_reuses_the_tier() {
        // A re-created name reuses the dropped table's resident tier — history is
        // preserved and no second namespace is burned — but the dropped era's
        // rows do **not** leak into the re-created table's *current* read: the
        // `DROP` closed them ([STL-211]). History survives where it belongs (in
        // the past): an `AS OF` read before the drop still sees the old row.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        let s1 = engine.clock.current();
        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        engine
            .execute(&parse_one(CREATE))
            .expect("re-create same policy");

        // The dropped era's row is closed, so a current read of the re-created
        // table is empty — no leak.
        assert!(
            select(&mut engine, "SELECT id, balance FROM account")
                .rows
                .is_empty(),
            "the dropped era's row does not leak into the re-created table"
        );
        // But the history is still there in the past.
        let as_of = format!(
            "SELECT id, balance FROM account FOR SYSTEM_TIME AS OF {}",
            s1.0
        );
        assert_eq!(
            select(&mut engine, &as_of).rows,
            vec![vec![i4(1), i4(100)]],
            "the pre-drop history is still readable AS OF"
        );
        // Re-using the dropped era's business key is no longer a duplicate.
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 9)"))
            .expect("re-insert a key the dropped era had used");
        assert_eq!(
            select(&mut engine, "SELECT id, balance FROM account").rows,
            vec![vec![i4(1), i4(9)]],
            "the re-inserted row is the only current row"
        );

        // The tier was reused: same namespace, none burned.
        let state = engine.tables.get("account").expect("tier resident");
        assert_eq!(state.namespace, 0, "the re-create reused the namespace");
        assert_eq!(engine.next_namespace, 1, "no second namespace was burned");
    }

    #[test]
    fn drop_table_retires_the_dropped_eras_rows() {
        // The ticket's exact reproduction ([STL-211]), one live session, no
        // restart: a name dropped and re-created with *different* columns must
        // not let the dropped era's rows bleed into the new era's current read,
        // nor block re-using one of their business keys as a duplicate. An AS OF
        // read inside the dropped era still resolves the old row under the old
        // schema, because the closes are append-only ([ADR-0023]).
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE t (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 100)"))
            .expect("insert");
        let s1 = engine.clock.current();
        engine.execute(&parse_one("DROP TABLE t")).expect("drop");
        engine
            .execute(&parse_one(
                "CREATE TABLE t (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
            ))
            .expect("re-create with different columns");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (2, 5)"))
            .expect("insert into the new era");

        // Symptom 1 — the dropped-era row no longer leaks into the current read.
        assert_eq!(
            sorted(select(&mut engine, "SELECT id, amount FROM t").rows),
            vec![vec![i4(2), i4(5)]],
            "the current read sees only the new era"
        );
        // Symptom 2 — re-inserting a business key the dropped era had used is no
        // longer refused as a duplicate (its old version is closed).
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 9)"))
            .expect("re-insert key 1 — the dropped era's open version was closed");
        assert_eq!(
            sorted(select(&mut engine, "SELECT id, amount FROM t").rows),
            vec![vec![i4(1), i4(9)], vec![i4(2), i4(5)]],
            "both new-era rows are current"
        );
        // The dropped era is untouched: AS OF before the drop still resolves the
        // old row under the *old* schema.
        let as_of = format!("SELECT id, balance FROM t FOR SYSTEM_TIME AS OF {}", s1.0);
        assert_eq!(
            select(&mut engine, &as_of).rows,
            vec![vec![i4(1), i4(100)]],
            "the dropped era reads its one row under the old schema"
        );
    }

    #[test]
    fn drop_if_exists_absent_is_a_no_op() {
        let mut engine = session();
        let outcome = engine
            .execute(&parse_one("DROP TABLE IF EXISTS nope"))
            .expect("drop if exists");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "DROP TABLE" });
    }

    #[test]
    fn insert_update_delete_route_through_execute() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        // INSERT routes through `execute` (bind_dml → typed insert) and reports a
        // single affected row.
        let inserted = engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        assert_eq!(inserted, StatementOutcome::Dml(DmlSummary::Insert(1)));

        // The inserted value reads back, decoded from the canonical encoding the
        // DML path wrote (int4 100 → little-endian bytes).
        let read = |engine: &mut SessionEngine<ZeroClock, MemDisk>| {
            let StatementOutcome::Rows(result) = engine
                .execute(&parse_one("SELECT balance FROM account"))
                .expect("select")
            else {
                panic!("rows");
            };
            payload_column(&result)
        };
        assert_eq!(
            read(&mut engine),
            vec![encode_value(&ScalarValue::Int4(100))]
        );

        // UPDATE then DELETE likewise route and tag their row counts.
        let updated = engine
            .execute(&parse_one("UPDATE account SET balance = 250 WHERE id = 1"))
            .expect("update");
        assert_eq!(updated, StatementOutcome::Dml(DmlSummary::Update(1)));
        assert_eq!(
            read(&mut engine),
            vec![encode_value(&ScalarValue::Int4(250))]
        );

        let deleted = engine
            .execute(&parse_one("DELETE FROM account WHERE id = 1"))
            .expect("delete");
        assert_eq!(deleted, StatementOutcome::Dml(DmlSummary::Delete(1)));
        assert!(read(&mut engine).is_empty(), "the row is gone after DELETE");
    }

    #[test]
    fn insert_null_payload_reads_back_as_null() {
        // A SQL NULL payload routes through `execute` (bind_dml → typed insert)
        // and reads back as a `None` cell — distinct from an empty payload, and
        // carried as a distinct NULL all the way through storage ([STL-154]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let inserted = engine
            .execute(&parse_one("INSERT INTO account VALUES (1, NULL)"))
            .expect("insert null");
        assert_eq!(inserted, StatementOutcome::Dml(DmlSummary::Insert(1)));

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT id, balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(result.rows.len(), 1, "the row exists");
        assert_eq!(result.rows[0][0], Some(encode_value(&ScalarValue::Int4(1))));
        assert_eq!(
            result.rows[0][1], None,
            "the payload reads back as SQL NULL"
        );
    }

    #[test]
    fn update_to_null_then_back_is_visible() {
        // An UPDATE can set the payload to NULL and a later UPDATE can set it
        // back to a value — both are visible to a subsequent read ([STL-154]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        engine
            .execute(&parse_one("UPDATE account SET balance = NULL WHERE id = 1"))
            .expect("update to null");

        let payload_cell = |engine: &mut SessionEngine<ZeroClock, MemDisk>| {
            let StatementOutcome::Rows(result) = engine
                .execute(&parse_one("SELECT balance FROM account"))
                .expect("select")
            else {
                panic!("rows");
            };
            // `SELECT balance` now projects exactly that one column ([STL-151]).
            result.rows[0][0].clone()
        };
        assert_eq!(payload_cell(&mut engine), None, "balance is now NULL");

        engine
            .execute(&parse_one("UPDATE account SET balance = 250 WHERE id = 1"))
            .expect("update back to a value");
        assert_eq!(
            payload_cell(&mut engine),
            Some(encode_value(&ScalarValue::Int4(250))),
            "balance reads back as 250 again"
        );
    }

    #[test]
    fn execute_as_of_reads_the_pre_update_value() {
        // The identity demo's heart, deterministically: with the synthetic clock
        // CREATE/INSERT/UPDATE land at sys_from 1/2/3, so an `AS OF 2` read sees
        // the inserted value, not the updated one. The temporal correctness is
        // bind_select's (STL-101) and SnapshotScan's (STL-100); this only proves
        // execute routes an AS OF SELECT to them.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create"); // sys_from 1
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert"); // sys_from 2
        engine
            .execute(&parse_one("UPDATE account SET balance = 250 WHERE id = 1"))
            .expect("update"); // sys_from 3

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one(
                "SELECT balance FROM account FOR SYSTEM_TIME AS OF 2",
            ))
            .expect("as-of select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&result),
            vec![encode_value(&ScalarValue::Int4(100))],
            "AS OF 2 reads the pre-update balance"
        );
    }

    #[test]
    fn transaction_commit_applies_all_buffered_writes_atomically() {
        // BEGIN; INSERT; INSERT; COMMIT — both rows are buffered (invisible until
        // commit) and then applied together ([STL-174]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        let one = engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage insert 1");
        let two = engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (2, 200)"), &mut txn)
            .expect("stage insert 2");
        assert_eq!(one, Some(DmlSummary::Insert(1)));
        assert_eq!(two, Some(DmlSummary::Insert(1)));

        // Nothing is visible while the writes sit in the buffer.
        let StatementOutcome::Rows(before) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select before commit")
        else {
            panic!("rows");
        };
        assert_eq!(before.rows.len(), 0, "buffered writes are invisible");

        engine.commit(txn).expect("commit");

        let StatementOutcome::Rows(after) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select after commit")
        else {
            panic!("rows");
        };
        assert_eq!(after.rows.len(), 2, "both buffered writes land at commit");
    }

    #[test]
    fn dropping_a_transaction_rolls_it_back() {
        // A buffered write that is never committed — the transaction is simply
        // dropped — leaves no trace, the ROLLBACK semantics ([STL-174]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage insert");
        drop(txn); // ROLLBACK: discard the buffer

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(result.rows.len(), 0, "rolled-back write never applied");
    }

    #[test]
    fn committed_transaction_is_readable_and_updatable_afterwards() {
        // After COMMIT the rows behave like any other committed state: a later
        // UPDATE (auto-commit) sees and supersedes them.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage");
        engine.commit(txn).expect("commit");

        engine
            .execute(&parse_one("UPDATE account SET balance = 250 WHERE id = 1"))
            .expect("update after commit");
        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&result),
            vec![encode_value(&ScalarValue::Int4(250))]
        );
    }

    #[test]
    fn same_key_insert_then_update_in_one_transaction_applies_front_to_back() {
        // STL-174 semantics preserved under group commit ([STL-192]): the writes
        // apply front-to-back, so an UPDATE of a key the same COMMIT inserted earlier
        // sees it and supersedes it — one live row, the updated value.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage insert");
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 200 WHERE id = 1"),
                &mut txn,
            )
            .expect("stage update of the just-inserted key");
        engine.commit(txn).expect("commit");

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(result.rows.len(), 1, "one live row, not two");
        assert_eq!(
            payload_column(&result),
            vec![encode_value(&ScalarValue::Int4(200))],
            "the UPDATE saw the INSERT staged before it",
        );
    }

    #[test]
    fn a_commit_that_fails_partway_shows_none_of_its_writes_matching_recovery() {
        // STL-216: a multi-statement COMMIT applies its buffered writes front-to-back
        // into the live tiers, then fails on a later write (here a duplicate-key
        // INSERT). The transaction is reported failed and *nothing* is made durable —
        // so the live engine must show none of its writes, identical to a post-crash
        // recovery (which finds no record for the aborted transaction), without a
        // restart. Before STL-216 the already-applied id=2 writes lingered in memory.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        // Committed baseline (durable, auto-commit) — pins the txn snapshot below.
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("baseline insert");

        // A transaction that stages a fresh key front-to-back, then an INSERT of the
        // already-live id=1 — which only fails when applied at commit (KeyExists),
        // after id=2's insert+update have already landed in the live tiers.
        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (2, 200)"), &mut txn)
            .expect("stage insert 2");
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 222 WHERE id = 2"),
                &mut txn,
            )
            .expect("stage update of the just-staged key");
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 999)"), &mut txn)
            .expect("stage the doomed duplicate-key insert");
        let outcome = engine.commit(txn);
        assert!(
            outcome.is_err(),
            "the duplicate-key INSERT aborts the whole COMMIT",
        );

        // The live engine shows only the committed baseline: id=2's applied
        // insert+update were rolled back out of the in-memory tiers.
        let now_sql = "SELECT id, balance FROM account";
        let live_now = sorted(select(&mut engine, now_sql).rows);
        assert_eq!(
            live_now,
            vec![vec![i4(1), i4(100)]],
            "none of the failed transaction's writes are visible after the abort",
        );

        // … and that is exactly what a restart reconstructs from the durable log.
        drop(engine);
        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, now_sql).rows),
            live_now,
            "the live post-abort state matches a from-the-WAL recovery",
        );
    }

    // --- Multi-row INSERT (STL-228) ----------------------------------------
    //
    // `INSERT INTO t VALUES (…), (…), …` binds every row and applies them as one
    // atomic group: all rows commit together (`INSERT 0 N`) or, if any row fails,
    // none do — the same group-commit / abort-rollback discipline (STL-192,
    // STL-216) a multi-statement COMMIT uses.

    #[test]
    fn multi_row_insert_auto_commit_writes_every_row() {
        // An auto-committed multi-row INSERT reports `INSERT 0 N` and commits all N
        // rows durably (they survive a from-the-WAL recovery).
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");

        let outcome = engine
            .execute(&parse_one(
                "INSERT INTO account VALUES (1, 100), (2, 200), (3, 300)",
            ))
            .expect("multi-row insert");
        assert_eq!(
            outcome,
            StatementOutcome::Dml(DmlSummary::Insert(3)),
            "the tag counts every inserted row (INSERT 0 3)",
        );

        let all = "SELECT id, balance FROM account";
        let expected = vec![
            vec![i4(1), i4(100)],
            vec![i4(2), i4(200)],
            vec![i4(3), i4(300)],
        ];
        assert_eq!(sorted(select(&mut engine, all).rows), expected);

        // Durable: a from-the-WAL recovery rebuilds the same three rows.
        drop(engine);
        let mut engine = recover_session(&disk);
        assert_eq!(sorted(select(&mut engine, all).rows), expected);
    }

    #[test]
    fn multi_row_insert_inside_a_txn_is_visible_then_committed() {
        // STL-203 read-your-own-writes: a multi-row INSERT staged in a transaction
        // reports its count at once, all N rows are visible to a later SELECT in
        // the same block, and COMMIT applies them as one group (recovers whole).
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        let summary = engine
            .stage_dml(
                &parse_one("INSERT INTO account VALUES (1, 100), (2, 200), (3, 300)"),
                &mut txn,
            )
            .expect("stage multi-row insert");
        assert_eq!(summary, Some(DmlSummary::Insert(3)));

        let StatementOutcome::Rows(seen) = engine
            .execute_in_txn(&parse_one("SELECT id, balance FROM account"), &mut txn)
            .expect("ryow select")
        else {
            panic!("rows");
        };
        let expected = vec![
            vec![i4(1), i4(100)],
            vec![i4(2), i4(200)],
            vec![i4(3), i4(300)],
        ];
        assert_eq!(
            sorted(seen.rows),
            expected,
            "all N buffered rows are visible mid-transaction",
        );

        engine.commit(txn).expect("commit");
        let all = "SELECT id, balance FROM account";
        assert_eq!(sorted(select(&mut engine, all).rows), expected);

        drop(engine);
        let mut engine = recover_session(&disk);
        assert_eq!(sorted(select(&mut engine, all).rows), expected);
    }

    #[test]
    fn multi_row_insert_failing_on_a_row_leaves_zero_rows() {
        // STL-228 DoD: a failure on a row (here a duplicate of the already-live
        // id=1) aborts the whole statement — none of its rows are visible, matching
        // a from-the-WAL recovery. The earlier good row (id=2) must not linger in
        // the in-memory tiers (the STL-216 abort rollback).
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("baseline");

        // Row 2 duplicates the live id=1: it fails at apply (KeyExists) *after*
        // id=2 has already landed in the live tiers within the group.
        let outcome = engine.execute(&parse_one("INSERT INTO account VALUES (2, 200), (1, 999)"));
        assert!(
            outcome.is_err(),
            "the duplicate-key row aborts the statement"
        );

        let all = "SELECT id, balance FROM account";
        let only_baseline = vec![vec![i4(1), i4(100)]];
        assert_eq!(
            sorted(select(&mut engine, all).rows),
            only_baseline,
            "neither the new id=2 nor the doomed id=1 row is visible",
        );

        drop(engine);
        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, all).rows),
            only_baseline,
            "the live post-abort state matches a from-the-WAL recovery",
        );
    }

    #[test]
    fn multi_row_insert_with_a_duplicate_key_within_the_statement_aborts() {
        // Two rows of one statement sharing a key is an in-statement duplicate: the
        // second fails against the first (already staged in the group), so the whole
        // statement leaves zero rows.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let outcome = engine.execute(&parse_one("INSERT INTO account VALUES (5, 1), (5, 2)"));
        assert!(outcome.is_err(), "the repeated key aborts the statement");
        assert!(
            select(&mut engine, "SELECT id FROM account")
                .rows
                .is_empty(),
            "no row of the aborted statement is visible",
        );
    }

    #[test]
    fn a_multi_table_transaction_commits_every_table() {
        // A transaction spanning two tables commits both — group commit is per
        // table (one record + one fsync each, [STL-192]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create account");
        engine
            .execute(&parse_one(
                "CREATE TABLE ledger (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create ledger");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage account insert");
        engine
            .stage_dml(&parse_one("INSERT INTO ledger VALUES (9, 42)"), &mut txn)
            .expect("stage ledger insert");
        engine.commit(txn).expect("commit");

        let StatementOutcome::Rows(account) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select account")
        else {
            panic!("rows");
        };
        let StatementOutcome::Rows(ledger) = engine
            .execute(&parse_one("SELECT id FROM ledger"))
            .expect("select ledger")
        else {
            panic!("rows");
        };
        assert_eq!(account.rows.len(), 1, "account row committed");
        assert_eq!(ledger.rows.len(), 1, "ledger row committed");
    }

    // --- Read-your-own-writes (STL-203, ADR-0008) --------------------------
    //
    // A SELECT inside an open transaction overlays the transaction's own buffered
    // INSERT/UPDATE/DELETE on its pinned snapshot, in staged order — while another
    // connection sees nothing until COMMIT and ROLLBACK discards the buffer.

    #[test]
    fn a_transaction_reads_its_own_buffered_writes() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage insert");

        // The transaction sees its own buffered INSERT.
        let StatementOutcome::Rows(seen) = engine
            .execute_in_txn(&parse_one("SELECT id, balance FROM account"), &mut txn)
            .expect("read-your-own insert")
        else {
            panic!("rows");
        };
        assert_eq!(
            seen.rows.len(),
            1,
            "the buffered insert is visible to the transaction"
        );
        assert_eq!(
            payload_column(&seen),
            vec![encode_value(&ScalarValue::Int4(100))],
            "and carries the inserted value",
        );

        // Another connection (auto-commit, its own snapshot) sees nothing — the
        // write is still only buffered.
        let StatementOutcome::Rows(other) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("auto-commit read")
        else {
            panic!("rows");
        };
        assert_eq!(
            other.rows.len(),
            0,
            "buffered writes are invisible to other readers until COMMIT"
        );

        // UPDATE-then-read: the SELECT layers the staged UPDATE over the staged
        // INSERT (repeated writes to the same key, applied front-to-back).
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 250 WHERE id = 1"),
                &mut txn,
            )
            .expect("stage update");
        let StatementOutcome::Rows(updated) = engine
            .execute_in_txn(&parse_one("SELECT balance FROM account"), &mut txn)
            .expect("read-your-own update")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&updated),
            vec![encode_value(&ScalarValue::Int4(250))],
            "the SELECT sees the staged UPDATE, not the staged INSERT's value",
        );

        // DELETE-then-read: the row is gone for the transaction.
        engine
            .stage_dml(&parse_one("DELETE FROM account WHERE id = 1"), &mut txn)
            .expect("stage delete");
        let StatementOutcome::Rows(deleted) = engine
            .execute_in_txn(&parse_one("SELECT id FROM account"), &mut txn)
            .expect("read-your-own delete")
        else {
            panic!("rows");
        };
        assert_eq!(
            deleted.rows.len(),
            0,
            "the staged DELETE hides the row from the transaction"
        );

        // ROLLBACK (drop the buffer): nothing ever reached storage.
        drop(txn);
        let StatementOutcome::Rows(after) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("after rollback")
        else {
            panic!("rows");
        };
        assert_eq!(
            after.rows.len(),
            0,
            "rolled back: no buffered write reached storage"
        );
    }

    #[test]
    fn a_transaction_overlays_buffered_writes_on_committed_rows() {
        // The pinned snapshot already holds committed rows; the transaction's
        // buffered UPDATE overlays only the one it touches — through a key
        // predicate and in a whole-table read — while the others keep their
        // committed value.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed 1");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
            .expect("seed 2");

        let mut txn = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 999 WHERE id = 1"),
                &mut txn,
            )
            .expect("stage update");

        // A WHERE on the updated key reads the staged value; the untouched key
        // reads its committed value.
        let StatementOutcome::Rows(one) = engine
            .execute_in_txn(
                &parse_one("SELECT balance FROM account WHERE id = 1"),
                &mut txn,
            )
            .expect("select id=1")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&one),
            vec![encode_value(&ScalarValue::Int4(999))],
            "the buffered UPDATE is visible through a key predicate",
        );
        let StatementOutcome::Rows(two) = engine
            .execute_in_txn(
                &parse_one("SELECT balance FROM account WHERE id = 2"),
                &mut txn,
            )
            .expect("select id=2")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&two),
            vec![encode_value(&ScalarValue::Int4(200))],
            "an untouched committed row reads its committed value inside the transaction",
        );

        // The whole table for the transaction: id=1 overlaid, id=2 committed.
        let StatementOutcome::Rows(all) = engine
            .execute_in_txn(&parse_one("SELECT id, balance FROM account"), &mut txn)
            .expect("select all")
        else {
            panic!("rows");
        };
        let mut got = all.rows;
        got.sort();
        let mut want = vec![
            vec![
                Some(encode_value(&ScalarValue::Int4(1))),
                Some(encode_value(&ScalarValue::Int4(999))),
            ],
            vec![
                Some(encode_value(&ScalarValue::Int4(2))),
                Some(encode_value(&ScalarValue::Int4(200))),
            ],
        ];
        want.sort();
        assert_eq!(
            got, want,
            "the overlaid row and the committed row both appear"
        );
    }

    #[test]
    fn an_explicit_as_of_read_inside_a_transaction_ignores_buffered_writes() {
        // Gating regression: read-your-own-writes overlays a *plain current* read
        // only. An explicit `FOR SYSTEM_TIME AS OF` — even one that folds to the
        // pinned snapshot — is a time-travel read and must show committed state
        // only, never the transaction's uncommitted buffer. (Snapshot equality is
        // *not* a sufficient gate: `AS OF now()` folds to the pinned snapshot.)
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed 100");

        let mut txn = engine.begin();
        let snap = txn.snapshot.0;
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 999 WHERE id = 1"),
                &mut txn,
            )
            .expect("stage update");

        // A plain read overlays the buffer → 999.
        let StatementOutcome::Rows(plain) = engine
            .execute_in_txn(&parse_one("SELECT balance FROM account"), &mut txn)
            .expect("plain read")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&plain),
            vec![encode_value(&ScalarValue::Int4(999))],
            "a plain read sees the buffered UPDATE",
        );

        // An explicit AS OF at the pinned snapshot reads committed state only → 100.
        let sql = format!("SELECT balance FROM account FOR SYSTEM_TIME AS OF {snap}");
        let StatementOutcome::Rows(as_of) = engine
            .execute_in_txn(&parse_one(&sql), &mut txn)
            .expect("as-of read")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&as_of),
            vec![encode_value(&ScalarValue::Int4(100))],
            "an explicit AS OF read ignores the transaction's buffered writes",
        );
    }

    // --- STL-223: read-your-own-writes on a valid-time table ----------------

    /// The four-column plain read of the valid-time oracle table `acct`.
    const VALID_PLAIN: &str = "SELECT id, balance, vf, vt FROM acct";

    /// Run a `FOR VALID_TIME AS OF v` read of `acct` inside `txn` (the overlay
    /// path — [STL-223]).
    fn read_acct_in_txn_as_of(
        engine: &mut SessionEngine<ZeroClock, MemDisk>,
        txn: &mut SessionTransaction,
        v: i64,
    ) -> StatementOutcome {
        let sql = format!("SELECT id, balance FROM acct FOR VALID_TIME AS OF {v}");
        engine
            .execute_in_txn(&parse_one(&sql), txn)
            .expect("valid-time AS OF read inside the transaction")
    }

    /// A `SELECT`'s rows keyed by their (never-NULL) `id` cell, asserting the
    /// at-most-one-live-version-per-key invariant a valid-time table resolves at a
    /// snapshot — so a collapsed overlay or a duplicated reference row is caught.
    fn rows_by_id(out: StatementOutcome) -> BTreeMap<Vec<u8>, Vec<Option<Vec<u8>>>> {
        let StatementOutcome::Rows(r) = out else {
            panic!("a SELECT must return rows");
        };
        let mut by_id = BTreeMap::new();
        for row in r.rows {
            let id = row
                .first()
                .and_then(Clone::clone)
                .expect("the id key is never NULL");
            assert!(
                by_id.insert(id, row).is_none(),
                "one system-live version per key broke",
            );
        }
        by_id
    }

    /// Generate one well-formed valid-time DML statement against `acct`, advancing
    /// `alive` (which keys currently hold a live version) so the workload never
    /// updates/deletes an absent key (the apply path would `KeyNotFound`) nor
    /// re-inserts a live one (`KeyExists`). `tick` distinguishes successive
    /// balances; a fraction of updates keep the prior balance (`SET` only the
    /// period) to exercise the read-modify-write carry-over through the overlay.
    fn gen_valid_stmt(
        rng: &mut ValidOracleRng,
        alive: &mut [bool],
        tick: i64,
        key_pool: i64,
        vmax: i64,
    ) -> String {
        let k = rng.below(key_pool);
        let ku = usize::try_from(k).expect("fits");
        let ki = i32::try_from(k).expect("key fits i32");
        let balance = i32::try_from(tick + 1).expect("balance fits i32");
        let from = rng.below(vmax);
        let open = rng.below(4) == 0;
        let to = if open {
            i64::MAX
        } else {
            from + 1 + rng.below(vmax - from)
        };

        if alive[ku] && rng.below(2) == 0 {
            alive[ku] = false;
            format!("DELETE FROM acct WHERE id = {ki}")
        } else if alive[ku] {
            let keep_balance = rng.below(3) == 0;
            let set = if keep_balance && open {
                format!("SET vf = {from}")
            } else if keep_balance {
                format!("SET vf = {from}, vt = {to}")
            } else if open {
                format!("SET balance = {balance}, vf = {from}")
            } else {
                format!("SET balance = {balance}, vf = {from}, vt = {to}")
            };
            format!("UPDATE acct {set} WHERE id = {ki}")
        } else {
            alive[ku] = true;
            if open {
                format!("INSERT INTO acct (id, balance, vf) VALUES ({ki}, {balance}, {from})")
            } else {
                format!("INSERT INTO acct VALUES ({ki}, {balance}, {from}, {to})")
            }
        }
    }

    #[test]
    fn a_valid_time_transaction_reads_its_own_writes_period_by_period() {
        // STL-223: read-your-own-writes on a *valid-time* table. A plain read and a
        // `FOR VALID_TIME AS OF v` read inside the block both reflect the buffered
        // INSERT/UPDATE/DELETE at the correct valid periods; another connection sees
        // nothing until COMMIT; ROLLBACK discards. The general differential against
        // committing the same buffer is the sibling oracle below; this pins the exact
        // period semantics readably.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        // A committed base: key 1 valid over [10, 20).
        engine
            .execute(&parse_one("INSERT INTO acct VALUES (1, 100, 10, 20)"))
            .expect("seed committed base");

        // The sorted (id, balance) cells a read returns, in canonical-byte space.
        // A surviving row's id and balance are never NULL in this workload.
        let kv = |id: i32, bal: i32| {
            (
                encode_value(&ScalarValue::Int4(id)),
                encode_value(&ScalarValue::Int4(bal)),
            )
        };
        let pairs = |out: StatementOutcome| -> Vec<(Vec<u8>, Vec<u8>)> {
            let StatementOutcome::Rows(r) = out else {
                panic!("rows");
            };
            let mut v: Vec<(Vec<u8>, Vec<u8>)> = r
                .rows
                .into_iter()
                .map(|row| {
                    (
                        row[0].clone().expect("id is never NULL"),
                        row[1].clone().expect("balance is never NULL"),
                    )
                })
                .collect();
            v.sort();
            v
        };

        let mut txn = engine.begin();
        // Stage a second key valid over [30, 40).
        engine
            .stage_dml(
                &parse_one("INSERT INTO acct VALUES (2, 200, 30, 40)"),
                &mut txn,
            )
            .expect("stage insert key 2");

        // Plain read: both the committed and the buffered key are visible.
        assert_eq!(
            pairs(
                engine
                    .execute_in_txn(&parse_one("SELECT id, balance FROM acct"), &mut txn)
                    .expect("plain read")
            ),
            vec![kv(1, 100), kv(2, 200)],
            "the buffered INSERT is visible alongside the committed row",
        );
        // FOR VALID_TIME AS OF picks each key only inside its own period.
        assert_eq!(
            pairs(read_acct_in_txn_as_of(&mut engine, &mut txn, 15)),
            vec![kv(1, 100)],
            "valid 15 lands in key 1's [10,20) only",
        );
        assert_eq!(
            pairs(read_acct_in_txn_as_of(&mut engine, &mut txn, 35)),
            vec![kv(2, 200)],
            "valid 35 lands in the buffered key 2's [30,40) only",
        );

        // A buffered UPDATE widens key 2's period to [30, 50) and changes balance —
        // closing the prior [30,40) and opening the new one, period-by-period.
        engine
            .stage_dml(
                &parse_one("UPDATE acct SET balance = 250, vf = 30, vt = 50 WHERE id = 2"),
                &mut txn,
            )
            .expect("stage update key 2");
        assert_eq!(
            pairs(read_acct_in_txn_as_of(&mut engine, &mut txn, 45)),
            vec![kv(2, 250)],
            "valid 45 now lands in key 2's widened [30,50) with the updated balance",
        );
        assert!(
            pairs(read_acct_in_txn_as_of(&mut engine, &mut txn, 25)).is_empty(),
            "valid 25 is in neither key's period (key 2 starts at 30)",
        );

        // A buffered DELETE of key 1 removes it for the transaction at every instant.
        engine
            .stage_dml(&parse_one("DELETE FROM acct WHERE id = 1"), &mut txn)
            .expect("stage delete key 1");
        assert!(
            pairs(read_acct_in_txn_as_of(&mut engine, &mut txn, 15)).is_empty(),
            "the buffered DELETE hides key 1 even at its own valid instant",
        );

        // Another connection (auto-commit, its own snapshot) still sees only the
        // committed base — none of the buffer.
        assert_eq!(
            pairs(
                engine
                    .execute(&parse_one("SELECT id, balance FROM acct"))
                    .expect("auto-commit read")
            ),
            vec![kv(1, 100)],
            "buffered valid-time writes are invisible to other readers until COMMIT",
        );

        // ROLLBACK (drop the buffer): nothing reached storage.
        drop(txn);
        assert_eq!(
            pairs(
                engine
                    .execute(&parse_one("SELECT id, balance FROM acct"))
                    .expect("after rollback")
            ),
            vec![kv(1, 100)],
            "rolled back: only the committed base remains",
        );
    }

    /// Run one seed of the STL-223 differential, returning `(valid_probes,
    /// rows_seen, overlay_changed_a_plain_read)`. A random committed base is applied
    /// identically to a staging engine and a reference; a random buffer is then
    /// STAGED on the staging engine (the overlay path) and COMMITTED on the reference
    /// (the durable apply + committed-read path). A swept valid grid plus the plain
    /// read must agree across the two; another (auto-commit) reader on the staging
    /// engine sees only the committed base, and dropping the transaction (ROLLBACK)
    /// leaves it unchanged.
    fn run_valid_ryow_seed(seed: u64, key_pool: i64, vmax: i64, create: &str) -> (u64, u64, bool) {
        let mut rng = ValidOracleRng::new(seed);
        let mut sut = session();
        let mut reference = session();
        sut.execute(&parse_one(create)).expect("create sut");
        reference
            .execute(&parse_one(create))
            .expect("create reference");

        // A committed base both engines share, applied identically (auto-commit).
        let mut alive = vec![false; usize::try_from(key_pool).expect("fits")];
        let mut tick: i64 = 0;
        let base_ops = 2 + rng.below(4);
        for _ in 0..base_ops {
            let sql = gen_valid_stmt(&mut rng, &mut alive, tick, key_pool, vmax);
            tick += 1;
            sut.execute(&parse_one(&sql)).expect("base op on sut");
            reference
                .execute(&parse_one(&sql))
                .expect("base op on reference");
        }
        // The committed base another (auto-commit) reader must keep seeing while the
        // transaction is open and after it rolls back.
        let base_plain = rows_by_id(sut.execute(&parse_one(VALID_PLAIN)).expect("base plain"));

        // Build a random buffer: STAGE it on `sut`, COMMIT it on `reference`.
        let mut txn = sut.begin();
        let buffer_ops = 3 + rng.below(7);
        for _ in 0..buffer_ops {
            let sql = gen_valid_stmt(&mut rng, &mut alive, tick, key_pool, vmax);
            tick += 1;
            sut.stage_dml(&parse_one(&sql), &mut txn)
                .expect("stage on sut");
            reference
                .execute(&parse_one(&sql))
                .expect("commit on reference");
        }

        // The differential: a swept valid grid, then the plain read, agree.
        let mut probes = 0;
        let mut rows_seen = 0;
        for v in 0..=(vmax + 1) {
            let sql = format!("{VALID_PLAIN} FOR VALID_TIME AS OF {v}");
            let got = rows_by_id(
                sut.execute_in_txn(&parse_one(&sql), &mut txn)
                    .expect("overlay AS OF read"),
            );
            let want = rows_by_id(
                reference
                    .execute(&parse_one(&sql))
                    .expect("committed AS OF read"),
            );
            assert_eq!(
                got, want,
                "seed {seed}: overlay vs commit diverged at valid v={v}"
            );
            rows_seen += u64::try_from(got.len()).expect("fits");
            probes += 1;
        }
        let got_plain = rows_by_id(
            sut.execute_in_txn(&parse_one(VALID_PLAIN), &mut txn)
                .expect("overlay plain read"),
        );
        let want_plain = rows_by_id(
            reference
                .execute(&parse_one(VALID_PLAIN))
                .expect("committed plain"),
        );
        assert_eq!(
            got_plain, want_plain,
            "seed {seed}: plain overlay vs commit diverged"
        );
        let overlay_changed = got_plain != base_plain;

        // Other-reader invisibility, then ROLLBACK discards.
        assert_eq!(
            rows_by_id(
                sut.execute(&parse_one(VALID_PLAIN))
                    .expect("auto-commit mid-txn")
            ),
            base_plain,
            "seed {seed}: the open transaction's buffer leaked to another reader",
        );
        drop(txn);
        assert_eq!(
            rows_by_id(
                sut.execute(&parse_one(VALID_PLAIN))
                    .expect("after rollback")
            ),
            base_plain,
            "seed {seed}: a rolled-back valid-time buffer left a trace",
        );
        (probes, rows_seen, overlay_changed)
    }

    #[test]
    fn valid_time_read_your_own_writes_matches_committing_the_buffer() {
        // STL-223's correctness oracle. A transaction's mid-flight reads of a
        // valid-time table — plain and `FOR VALID_TIME AS OF v` across a swept grid —
        // must match committing the *same* buffer in a second engine and reading it
        // back. One engine STAGES a random INSERT/UPDATE/DELETE buffer in an open
        // transaction (the overlay path); the reference engine COMMITS the identical
        // buffer via auto-commit (the durable apply + committed-read path). Agreement
        // proves the period-by-period overlay reproduces exactly what COMMIT makes
        // durable, including the half-open valid boundary (the reference's scan
        // applies the same `from ≤ v < to` cut). Two checks ride along: another
        // (auto-commit) reader on the staging engine never sees the buffer, and
        // dropping the transaction (ROLLBACK) leaves only the committed base. The
        // teeth assert proves the buffer actually moved a read.
        const KEY_POOL: i64 = 3;
        const VMAX: i64 = 10;
        const SEEDS: u64 = 64;

        let create = "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                      WITH SYSTEM VERSIONING VALID TIME (vf, vt)";

        let mut probes: u64 = 0;
        let mut rows_seen: u64 = 0;
        let mut overlay_diverged_from_base = false;

        for seed in 0..SEEDS {
            let (p, r, changed) = run_valid_ryow_seed(seed, KEY_POOL, VMAX, create);
            probes += p;
            rows_seen += r;
            overlay_diverged_from_base |= changed;
        }

        assert!(
            rows_seen > 0,
            "every probe was empty — the workload resolved nothing"
        );
        assert!(
            probes > 700,
            "differential probed only {probes} valid cells — widen the sweep"
        );
        assert!(
            overlay_diverged_from_base,
            "the buffer never changed a plain read — the differential never exercised the overlay",
        );
    }

    // --- Snapshot isolation oracle (STL-175, ADR-0008) ---------------------
    //
    // The engine is mutex-serialized in the server, so concurrency is modeled
    // here as interleaved `begin`/`stage_dml`/`commit` calls — the same shape a
    // pair of connections produces. These assert the two STL-175 properties: a
    // transaction reads one consistent snapshot, and first-committer-wins
    // write-write conflict detection surfaces a retryable error.

    #[test]
    fn a_transaction_reads_one_consistent_snapshot() {
        // A transaction pins its read snapshot at BEGIN; every statement reads at
        // it, even while another transaction commits a newer value.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed 100");

        // Pin the snapshot — it sees balance = 100.
        let mut txn = engine.begin();

        // A concurrent auto-committed write moves the live balance to 200.
        engine
            .execute(&parse_one("UPDATE account SET balance = 200 WHERE id = 1"))
            .expect("concurrent update");

        // The transaction still reads its pinned snapshot: 100, and stably so
        // across repeated reads.
        for _ in 0..2 {
            let StatementOutcome::Rows(in_txn) = engine
                .execute_in_txn(&parse_one("SELECT balance FROM account"), &mut txn)
                .expect("read inside the transaction")
            else {
                panic!("rows");
            };
            assert_eq!(
                payload_column(&in_txn),
                vec![encode_value(&ScalarValue::Int4(100))],
                "the transaction reads its pinned snapshot, not the concurrent commit"
            );
        }

        // An auto-committed read outside the transaction is its own snapshot and
        // sees the latest value, 200.
        let StatementOutcome::Rows(live) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("read live")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&live),
            vec![encode_value(&ScalarValue::Int4(200))],
            "outside a transaction each statement reads the latest committed state"
        );
    }

    #[test]
    fn concurrent_writes_to_the_same_key_conflict_first_committer_wins() {
        // Two transactions pin the same snapshot and both write id = 1. The first
        // to commit wins; the second sees a retryable conflict and never lands.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed");

        let mut first = engine.begin();
        let mut second = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 200 WHERE id = 1"),
                &mut first,
            )
            .expect("stage first");
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 300 WHERE id = 1"),
                &mut second,
            )
            .expect("stage second");

        engine.commit(first).expect("first committer wins");
        let err = engine.commit(second).unwrap_err();
        assert!(
            matches!(err, EngineError::Conflict),
            "the loser gets a retryable conflict, got {err:?}"
        );

        // The winner's value stands; the loser touched nothing.
        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&result),
            vec![encode_value(&ScalarValue::Int4(200))],
            "first committer wins; the conflicting transaction had no effect"
        );
    }

    #[test]
    fn the_metric_registry_tracks_statements_transactions_and_flushes() {
        // The engine-side series of STL-253: per-kind statement counts, rows
        // in/out, transaction outcomes, and the flush/checkpoint histograms.
        // No time source is installed, so durations observe as zero — the
        // counts are the deterministic part and the point here.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        engine
            .execute(&parse_one("SELECT id, balance FROM account"))
            .expect("select");
        engine
            .execute(&parse_one("SELECT id FROM missing"))
            .expect_err("unknown table");

        let mut winner = engine.begin();
        let mut loser = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 200 WHERE id = 1"),
                &mut winner,
            )
            .expect("stage winner");
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 300 WHERE id = 1"),
                &mut loser,
            )
            .expect("stage loser");
        engine.commit(winner).expect("first committer wins");
        engine.commit(loser).expect_err("conflict");

        engine.flush().expect("flush");
        engine.checkpoint().expect("checkpoint");

        let m = engine.metrics();
        assert_eq!(m.statements(StatementKind::Ddl), 1);
        assert_eq!(m.statements(StatementKind::Insert), 1);
        assert_eq!(m.statements(StatementKind::Select), 1);
        assert_eq!(m.statement_errors.get(), 1, "the unknown-table SELECT");
        assert_eq!(m.rows_returned.get(), 1, "one row out of the SELECT");
        assert_eq!(m.rows_written.get(), 1, "the auto-commit INSERT");
        assert_eq!(m.txn_commits.get(), 1);
        assert_eq!(m.txn_conflicts.get(), 1);
        assert_eq!(m.flush_seconds.count(), 1);
        assert_eq!(m.checkpoint_seconds.count(), 1);
        assert!(
            m.wal_appends.get() >= 2,
            "the insert and the group commit reached the WAL, got {}",
            m.wal_appends.get()
        );
    }

    #[test]
    fn concurrent_writes_to_distinct_keys_do_not_conflict() {
        // Conflict detection is per key: two transactions on the same snapshot
        // writing *different* keys both commit — no false serialization failure.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed 1");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
            .expect("seed 2");

        let mut first = engine.begin();
        let mut second = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 111 WHERE id = 1"),
                &mut first,
            )
            .expect("stage first");
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 222 WHERE id = 2"),
                &mut second,
            )
            .expect("stage second");

        engine.commit(first).expect("first commits");
        engine
            .commit(second)
            .expect("second commits — a distinct key never conflicts");
    }

    #[test]
    fn a_serial_transaction_does_not_conflict_with_an_earlier_one() {
        // A transaction that begins *after* another committed the same key sees
        // that write in its snapshot and updates on top of it — no conflict.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed");

        let mut first = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 200 WHERE id = 1"),
                &mut first,
            )
            .expect("stage first");
        engine.commit(first).expect("first commits");

        // Begins now — its snapshot already includes the first transaction's write.
        let mut second = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 300 WHERE id = 1"),
                &mut second,
            )
            .expect("stage second");
        engine
            .commit(second)
            .expect("second commits — it started after the first");

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&result),
            vec![encode_value(&ScalarValue::Int4(300))]
        );
    }

    #[test]
    fn read_committed_advances_the_snapshot_each_statement() {
        // STL-248: REPEATABLE READ (the default) holds the BEGIN-pinned snapshot
        // for the whole block, so a concurrent commit is invisible inside it; READ
        // COMMITTED re-pins per statement, so the same transaction's later read
        // observes the commit. Both levels watch the *same* concurrent write here.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed 100");

        let balance = |engine: &mut SessionEngine<ZeroClock, MemDisk>,
                       txn: &mut SessionTransaction| {
            let StatementOutcome::Rows(rows) = engine
                .execute_in_txn(&parse_one("SELECT balance FROM account"), txn)
                .expect("read inside the transaction")
            else {
                panic!("rows")
            };
            payload_column(&rows)
        };
        let hundred = vec![encode_value(&ScalarValue::Int4(100))];
        let two_hundred = vec![encode_value(&ScalarValue::Int4(200))];

        // Both transactions pin at the same instant, before the concurrent commit.
        let mut rr = engine.begin();
        let mut rc = engine.begin_with_isolation(IsolationLevel::ReadCommitted);
        assert_eq!(rr.isolation(), IsolationLevel::RepeatableRead);
        assert_eq!(rc.isolation(), IsolationLevel::ReadCommitted);
        assert_eq!(balance(&mut engine, &mut rr), hundred);
        assert_eq!(balance(&mut engine, &mut rc), hundred);

        // A concurrent auto-commit moves the live balance to 200.
        engine
            .execute(&parse_one("UPDATE account SET balance = 200 WHERE id = 1"))
            .expect("concurrent update");

        assert_eq!(
            balance(&mut engine, &mut rr),
            hundred,
            "REPEATABLE READ holds the snapshot pinned at BEGIN"
        );
        assert_eq!(
            balance(&mut engine, &mut rc),
            two_hundred,
            "READ COMMITTED re-pins a fresh snapshot per statement and sees the commit"
        );
    }

    #[test]
    fn set_isolation_switches_an_open_block_to_read_committed() {
        // STL-248: `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` mid-block — the
        // engine path is `SessionTransaction::set_isolation` — takes effect from the
        // next statement, after which the snapshot advances per statement.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("seed 100");

        let balance = |engine: &mut SessionEngine<ZeroClock, MemDisk>,
                       txn: &mut SessionTransaction| {
            let StatementOutcome::Rows(rows) = engine
                .execute_in_txn(&parse_one("SELECT balance FROM account"), txn)
                .expect("read inside the transaction")
            else {
                panic!("rows")
            };
            payload_column(&rows)
        };

        let mut txn = engine.begin();
        assert_eq!(
            txn.isolation(),
            IsolationLevel::RepeatableRead,
            "the default level is snapshot isolation"
        );

        engine
            .execute(&parse_one("UPDATE account SET balance = 200 WHERE id = 1"))
            .expect("concurrent update");
        assert_eq!(
            balance(&mut engine, &mut txn),
            vec![encode_value(&ScalarValue::Int4(100))],
            "before the switch the block reads its fixed BEGIN snapshot"
        );

        txn.set_isolation(IsolationLevel::ReadCommitted);
        assert_eq!(
            balance(&mut engine, &mut txn),
            vec![encode_value(&ScalarValue::Int4(200))],
            "after the switch the block re-pins per statement and sees the commit"
        );
    }

    #[test]
    fn commit_prunes_write_index_below_the_oldest_live_snapshot() {
        // The MVCC write index is bounded by the oldest live snapshot ([STL-204]):
        // an entry committed strictly before it can never produce a conflict, so it
        // is dropped, while an entry at it (still reachable) survives.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        // `low` stays open across two auto-committed inserts, holding the floor at
        // its snapshot so both their write-index entries are retained.
        let low = engine.begin();
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert id=1");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
            .expect("insert id=2");

        // `high` pins a later snapshot: id=1 committed strictly before it, id=2 at
        // it (the second insert advanced the clock to exactly this instant).
        let high = engine.begin();
        let floor = high.snapshot;

        let key1 = ("account".to_owned(), business_key(&ScalarValue::Int4(1)));
        let key2 = ("account".to_owned(), business_key(&ScalarValue::Int4(2)));
        assert!(engine.write_index.contains_key(&key1), "id=1 recorded");
        assert!(engine.write_index.contains_key(&key2), "id=2 recorded");
        assert!(
            engine.write_index[&key1] < floor,
            "id=1 committed strictly below the later snapshot"
        );

        // `low` ends: the oldest live snapshot rises to `high`'s, and committing
        // prunes the index below it.
        engine
            .commit(low)
            .expect("read-only commit of the low transaction");

        assert!(
            !engine.write_index.contains_key(&key1),
            "the entry below the oldest live snapshot is pruned"
        );
        assert!(
            engine.write_index.contains_key(&key2),
            "the entry at the oldest live snapshot is retained"
        );
        assert!(
            engine.write_index.values().all(|&at| at >= floor),
            "no entry remains below the oldest live snapshot"
        );

        drop(high);
    }

    #[test]
    fn rollback_releases_the_snapshot_so_a_later_write_prunes_the_index() {
        // Rolling a transaction back is just dropping it ([STL-174]); its snapshot
        // lease is released on drop, so the floor it pinned is gone and the next
        // write reclaims the index ([STL-204]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let held = engine.begin();
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert id=1");
        let key1 = ("account".to_owned(), business_key(&ScalarValue::Int4(1)));
        assert!(
            engine.write_index.contains_key(&key1),
            "the open transaction holds the floor, so id=1 is retained"
        );

        // ROLLBACK: drop the transaction without committing.
        drop(held);

        // With no live snapshot, the next auto-committed write prunes the whole
        // index — proving the rolled-back transaction no longer pins the floor (had
        // its lease leaked, id=1 would still be retained above it).
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
            .expect("insert id=2");
        assert!(
            engine.write_index.is_empty(),
            "no open snapshot ⇒ every entry is unreachable and dropped"
        );
    }

    #[test]
    fn ddl_inside_a_transaction_is_visible_to_later_statements() {
        // `BEGIN; CREATE TABLE t …; INSERT INTO t …; COMMIT`: DDL inside a block
        // auto-commits (transactional DDL is deferred) and advances the pinned
        // snapshot, so the later INSERT — and a SELECT — resolve the new table
        // rather than failing at the pre-CREATE snapshot.
        let mut engine = session();
        let mut txn = engine.begin();

        let created = engine
            .execute_in_txn(
                &parse_one("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING"),
                &mut txn,
            )
            .expect("create inside the transaction");
        assert!(
            matches!(created, StatementOutcome::Ddl { .. }),
            "got {created:?}"
        );

        // The INSERT binds against the advanced snapshot and resolves `t`.
        let inserted = engine
            .execute_in_txn(&parse_one("INSERT INTO t VALUES (1, 100)"), &mut txn)
            .expect("insert resolves the table created earlier in the block");
        assert_eq!(inserted, StatementOutcome::Dml(DmlSummary::Insert(1)));

        engine.commit(txn).expect("commit");

        let StatementOutcome::Rows(result) = engine
            .execute(&parse_one("SELECT v FROM t"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&result),
            vec![encode_value(&ScalarValue::Int4(100))],
            "the buffered insert into the in-transaction table landed at commit"
        );
    }

    // --- Savepoints ([STL-176]) -------------------------------------------
    //
    // The savepoint stack rides the buffered write set: `savepoint` marks the
    // current buffer length, `rollback_to` truncates back to a marker (undoing
    // only the writes staged after it), and `release` drops a marker while
    // keeping its writes. The buffer is asserted directly — these are pure
    // pre-commit mechanics with no storage surface until COMMIT.

    /// Stage `INSERT INTO account VALUES (id, balance)` into `txn`.
    fn stage_insert(
        engine: &SessionEngine<ZeroClock, MemDisk>,
        txn: &mut SessionTransaction,
        id: i32,
        balance: i32,
    ) {
        engine
            .stage_dml(
                &parse_one(&format!("INSERT INTO account VALUES ({id}, {balance})")),
                txn,
            )
            .expect("stage insert");
    }

    #[test]
    fn rollback_to_savepoint_undoes_only_writes_after_it() {
        // BEGIN; INSERT 1; SAVEPOINT sp1; INSERT 2; INSERT 3; ROLLBACK TO sp1;
        // COMMIT. The pre-savepoint insert survives; the two staged after it are
        // discarded — the DoD of [STL-176].
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        stage_insert(&engine, &mut txn, 1, 100);
        txn.savepoint("sp1");
        stage_insert(&engine, &mut txn, 2, 200);
        stage_insert(&engine, &mut txn, 3, 300);
        assert_eq!(txn.writes.len(), 3, "three writes staged");

        assert!(txn.rollback_to("sp1"), "the savepoint exists");
        assert_eq!(
            txn.writes.len(),
            1,
            "only the pre-savepoint write remains buffered"
        );

        engine.commit(txn).expect("commit");
        let StatementOutcome::Rows(after) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&after),
            vec![encode_value(&ScalarValue::Int4(100))],
            "only the pre-savepoint row committed"
        );
    }

    #[test]
    fn statements_after_rollback_to_continue_in_the_same_transaction() {
        // ROLLBACK TO does not end the transaction: a write staged afterwards
        // still commits alongside the surviving pre-savepoint writes ([STL-176]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        stage_insert(&engine, &mut txn, 1, 100);
        txn.savepoint("sp1");
        stage_insert(&engine, &mut txn, 2, 200);
        assert!(txn.rollback_to("sp1"));
        stage_insert(&engine, &mut txn, 3, 300); // continues in the same txn
        engine.commit(txn).expect("commit");

        let StatementOutcome::Rows(after) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        let mut got = payload_column(&after);
        got.sort();
        let mut want = vec![
            encode_value(&ScalarValue::Int4(100)),
            encode_value(&ScalarValue::Int4(300)),
        ];
        want.sort();
        assert_eq!(
            got, want,
            "the pre-savepoint and post-rollback writes commit; the rolled-back one does not"
        );
    }

    #[test]
    fn nested_savepoints_roll_back_independently() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        stage_insert(&engine, &mut txn, 1, 100);
        txn.savepoint("a");
        stage_insert(&engine, &mut txn, 2, 200);
        txn.savepoint("b");
        stage_insert(&engine, &mut txn, 3, 300);

        assert!(txn.rollback_to("b"));
        assert_eq!(
            txn.writes.len(),
            2,
            "rolling back to the inner savepoint drops only the last write"
        );
        assert!(txn.rollback_to("a"));
        assert_eq!(
            txn.writes.len(),
            1,
            "rolling back to the outer savepoint drops the rest"
        );
        drop(txn);
    }

    #[test]
    fn rollback_to_destroys_nested_savepoints_and_keeps_its_own() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        txn.savepoint("a");
        stage_insert(&engine, &mut txn, 1, 100);
        txn.savepoint("b");
        stage_insert(&engine, &mut txn, 2, 200);

        assert!(txn.rollback_to("a"), "outer savepoint exists");
        assert_eq!(txn.writes.len(), 0, "everything after `a` is discarded");
        // `b` was established after `a`, so the rollback destroyed it...
        assert!(!txn.rollback_to("b"), "the nested savepoint is gone");
        // ...but `a` itself survives and can be rolled back to again.
        stage_insert(&engine, &mut txn, 3, 300);
        assert!(txn.rollback_to("a"), "the target savepoint is reusable");
        assert_eq!(txn.writes.len(), 0);
        drop(txn);
    }

    #[test]
    fn release_keeps_writes_but_drops_the_savepoint() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        stage_insert(&engine, &mut txn, 1, 100);
        txn.savepoint("sp1");
        stage_insert(&engine, &mut txn, 2, 200);

        assert!(txn.release("sp1"), "the savepoint exists");
        assert_eq!(
            txn.writes.len(),
            2,
            "release keeps the writes staged after the savepoint"
        );
        assert!(txn.savepoints.is_empty(), "the marker is gone");
        assert!(
            !txn.rollback_to("sp1"),
            "a released savepoint can no longer be rolled back to"
        );

        engine.commit(txn).expect("commit");
        let StatementOutcome::Rows(after) = engine
            .execute(&parse_one("SELECT id FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(after.rows.len(), 2, "both writes committed");
    }

    #[test]
    fn rollback_to_or_release_of_an_unknown_savepoint_reports_missing() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        assert!(
            !txn.rollback_to("nope"),
            "no such savepoint to roll back to"
        );
        assert!(!txn.release("nope"), "no such savepoint to release");
        drop(txn);
    }

    #[test]
    fn a_duplicate_savepoint_name_targets_the_most_recent() {
        // Postgres keeps both savepoints of the same name. ROLLBACK TO hits the
        // most recent one and keeps it (re-runnable); the older one stays shadowed
        // until the most recent is released, which re-exposes it ([STL-176]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        stage_insert(&engine, &mut txn, 1, 100);
        txn.savepoint("sp"); // marks buffer length 1
        stage_insert(&engine, &mut txn, 2, 200);
        txn.savepoint("sp"); // shadows the first; marks buffer length 2
        stage_insert(&engine, &mut txn, 3, 300);

        assert!(txn.rollback_to("sp"));
        assert_eq!(
            txn.writes.len(),
            2,
            "rolled back to the most recent `sp` (after write 2)"
        );
        // The most recent `sp` survives the rollback and is hit again — the older
        // one is still shadowed.
        assert!(txn.rollback_to("sp"));
        assert_eq!(
            txn.writes.len(),
            2,
            "still the most recent `sp`, not the older one"
        );

        // Releasing the most recent `sp` re-exposes the older one (after write 1).
        assert!(txn.release("sp"));
        assert!(txn.rollback_to("sp"));
        assert_eq!(
            txn.writes.len(),
            1,
            "the older `sp` (after write 1) is now the target"
        );
        drop(txn);
    }

    #[test]
    fn stage_dml_passes_non_dml_through() {
        // A non-DML statement returns `None` so the caller routes it through
        // `execute` instead of buffering it.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        let select = engine
            .stage_dml(&parse_one("SELECT id FROM account"), &mut txn)
            .expect("stage select");
        assert_eq!(select, None, "a SELECT is not buffered");
        let create = engine
            .stage_dml(
                &parse_one("CREATE TABLE t (id INT PRIMARY KEY) WITH SYSTEM VERSIONING"),
                &mut txn,
            )
            .expect("stage ddl");
        assert_eq!(create, None, "DDL is not buffered");
    }

    #[test]
    fn stage_dml_surfaces_a_malformed_write() {
        // Staging binds the statement, so a write against an unknown table is
        // rejected at stage time, not silently buffered.
        let engine = session();
        let mut txn = engine.begin();
        let err = engine
            .stage_dml(&parse_one("INSERT INTO nope VALUES (1, 100)"), &mut txn)
            .unwrap_err();
        assert!(matches!(err, EngineError::Dml(_)), "got {err:?}");
    }

    #[test]
    fn non_routable_statement_is_unsupported() {
        let mut engine = session();
        // A statement that is neither DDL, SELECT, nor INSERT/UPDATE/DELETE.
        let err = engine.execute(&parse_one("TRUNCATE account")).unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    // The clone is precisely what's under test: a per-table engine holds a clone
    // of the session clock, and all clones must advance one shared mark.
    #[allow(clippy::redundant_clone)]
    fn monotonic_clock_is_strictly_increasing_and_shared_across_clones() {
        let clock = MonotonicClock::new(ZeroClock);
        let a = clock.now();
        let cloned = clock.clone();
        let b = cloned.now();
        let c = clock.now();
        assert!(a.0 < b.0 && b.0 < c.0, "shared mark advances across clones");
        assert_eq!(clock.current(), c, "current() is the last value handed out");
    }

    #[test]
    fn describe_live_tables_reports_columns_and_excludes_dropped() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create account");
        engine
            .execute(&parse_one(
                "CREATE TABLE ledger (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create ledger");

        // Both tables are live; columns come back in declaration order.
        let live = engine.describe_live_tables();
        assert_eq!(
            live,
            vec![
                TableDescription {
                    name: "account".to_owned(),
                    columns: vec![
                        ("id".to_owned(), LogicalType::Int4),
                        ("balance".to_owned(), LogicalType::Int4),
                    ],
                },
                TableDescription {
                    name: "ledger".to_owned(),
                    columns: vec![
                        ("id".to_owned(), LogicalType::Int4),
                        ("amount".to_owned(), LogicalType::Int4),
                    ],
                },
            ]
        );

        // A dropped table is no longer live, so it drops out of the listing even
        // though its tier stays resident for history.
        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop account");
        let live = engine.describe_live_tables();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].name, "ledger");
    }

    /// A multi-column table `t (id INT, a INT, b TEXT)` — a key plus two value
    /// columns — for the projection/predicate tests ([STL-151]).
    const CREATE_WIDE: &str =
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT) WITH SYSTEM VERSIONING";

    /// Run a SELECT and return its `(columns, rows)`.
    fn select(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> SelectResult {
        let StatementOutcome::Rows(result) = engine.execute(&parse_one(sql)).expect("select")
        else {
            panic!("SELECT must return rows");
        };
        result
    }

    /// A row cell's expected encoding: `Some(value)` → its canonical bytes,
    /// `None` → a SQL `NULL` cell — matching what a `SelectResult` row carries.
    // Inlining the `.map` at every expected-row literal (what `single_option_map`
    // asks for) would only duplicate it; this test helper reads better as-is.
    #[allow(clippy::single_option_map)]
    fn cell(value: Option<ScalarValue>) -> Option<Vec<u8>> {
        value.map(|v| encode_value(&v))
    }

    #[test]
    fn a_write_bound_against_a_changed_shape_fails_safely() {
        // Stage an UPDATE against a 2-value-column table, then drop and re-create
        // the table narrower before committing. The buffered assignment's index
        // now points past the live value columns; the guard returns a clean error
        // instead of panicking on an out-of-range cell write.
        let mut engine = session();
        engine
            .execute(&parse_one(CREATE_WIDE))
            .expect("create wide");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10, 'one')"))
            .expect("insert");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("UPDATE t SET b = 'two' WHERE id = 1"), &mut txn)
            .expect("stage update of column b (value index 1)");

        // Re-create `t` with only one value column — same (system-only) policy, so
        // the tier is reused, but `b` no longer exists.
        engine.execute(&parse_one("DROP TABLE t")).expect("drop");
        engine
            .execute(&parse_one(
                "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
            ))
            .expect("re-create narrower");

        let err = engine.commit(txn).unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::SchemaChanged {
                    live: 1,
                    bound: 2,
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn multi_column_select_honors_projection_and_where() {
        // The DoD: a multi-row, multi-column table returns exactly the projected
        // columns for exactly the matching rows.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 10, 'one')",
            "INSERT INTO t VALUES (2, 20, 'two')",
            "INSERT INTO t VALUES (3, 20, 'three')",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        // Project a subset, filter on the key: exactly one row, exactly the asked
        // columns, in the asked order.
        let r = select(&mut engine, "SELECT b, a FROM t WHERE id = 2");
        assert_eq!(
            r.columns,
            vec![
                ("b".to_owned(), LogicalType::Text),
                ("a".to_owned(), LogicalType::Int4),
            ]
        );
        assert_eq!(
            r.rows,
            vec![vec![
                cell(Some(ScalarValue::Text("two".to_owned()))),
                cell(Some(ScalarValue::Int4(20))),
            ]]
        );

        // Filter on a non-key value column: both rows with a = 20, key order.
        let r = select(&mut engine, "SELECT id FROM t WHERE a = 20");
        assert_eq!(
            r.rows,
            vec![
                vec![cell(Some(ScalarValue::Int4(2)))],
                vec![cell(Some(ScalarValue::Int4(3)))],
            ]
        );

        // Filter on the text value column.
        let r = select(&mut engine, "SELECT id FROM t WHERE b = 'three'");
        assert_eq!(r.rows, vec![vec![cell(Some(ScalarValue::Int4(3)))]]);

        // SELECT * projects every column in declaration order.
        let r = select(&mut engine, "SELECT * FROM t WHERE id = 1");
        assert_eq!(
            r.columns,
            vec![
                ("id".to_owned(), LogicalType::Int4),
                ("a".to_owned(), LogicalType::Int4),
                ("b".to_owned(), LogicalType::Text),
            ]
        );
        assert_eq!(
            r.rows,
            vec![vec![
                cell(Some(ScalarValue::Int4(1))),
                cell(Some(ScalarValue::Int4(10))),
                cell(Some(ScalarValue::Text("one".to_owned()))),
            ]]
        );

        // A predicate matching nothing returns no rows (not every row).
        assert!(
            select(&mut engine, "SELECT id FROM t WHERE id = 99")
                .rows
                .is_empty()
        );
    }

    #[test]
    fn group_by_and_aggregates_end_to_end() {
        // STL-171: grouped + ungrouped aggregates, incl. NULL handling, end-to-end
        // through the session engine.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 10, 'x')",
            "INSERT INTO t VALUES (2, 10, 'y')",
            "INSERT INTO t VALUES (3, 20, 'x')",
            "INSERT INTO t VALUES (4, 20, NULL)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        // Grouped: COUNT(*) and SUM(id) per `a`. Groups emit in key order (10, 20).
        let r = select(&mut engine, "SELECT a, COUNT(*), SUM(id) FROM t GROUP BY a");
        assert_eq!(
            r.columns,
            vec![
                ("a".to_owned(), LogicalType::Int4),
                ("count".to_owned(), LogicalType::Int8),
                ("sum".to_owned(), LogicalType::Int8),
            ]
        );
        assert_eq!(
            r.rows,
            vec![
                vec![
                    cell(Some(ScalarValue::Int4(10))),
                    cell(Some(ScalarValue::Int8(2))),
                    cell(Some(ScalarValue::Int8(3))), // ids 1 + 2
                ],
                vec![
                    cell(Some(ScalarValue::Int4(20))),
                    cell(Some(ScalarValue::Int8(2))),
                    cell(Some(ScalarValue::Int8(7))), // ids 3 + 4
                ],
            ]
        );

        // Ungrouped MIN / MAX / AVG: MIN/MAX keep the argument type (int4); AVG is
        // the exact fractional mean as float8 ([STL-209]). (10+10+20+20)/4 = 15.
        let r = select(&mut engine, "SELECT MIN(a), MAX(a), AVG(a) FROM t");
        assert_eq!(
            r.columns,
            vec![
                ("min".to_owned(), LogicalType::Int4),
                ("max".to_owned(), LogicalType::Int4),
                ("avg".to_owned(), LogicalType::Float8),
            ]
        );
        assert_eq!(
            r.rows,
            vec![vec![
                cell(Some(ScalarValue::Int4(10))),
                cell(Some(ScalarValue::Int4(20))),
                cell(Some(ScalarValue::float8(15.0))),
            ]]
        );

        // AVG over ids 1..=4 is the genuinely fractional 2.5 — proving the mean is
        // no longer truncated toward zero (which would have shown 2).
        let r = select(&mut engine, "SELECT AVG(id) FROM t");
        assert_eq!(r.columns, vec![("avg".to_owned(), LogicalType::Float8)]);
        assert_eq!(r.rows, vec![vec![cell(Some(ScalarValue::float8(2.5)))]]);

        // COUNT(*) counts rows; COUNT(b) skips the one NULL `b`.
        let r = select(&mut engine, "SELECT COUNT(*), COUNT(b) FROM t");
        assert_eq!(
            r.rows,
            vec![vec![
                cell(Some(ScalarValue::Int8(4))),
                cell(Some(ScalarValue::Int8(3))),
            ]]
        );

        // GROUP BY a column with a NULL: the NULL forms its own group, sorting
        // first (NULL before any present key).
        let r = select(&mut engine, "SELECT b, COUNT(*) FROM t GROUP BY b");
        assert_eq!(
            r.rows,
            vec![
                vec![cell(None), cell(Some(ScalarValue::Int8(1)))], // NULL b: row 4
                vec![
                    cell(Some(ScalarValue::Text("x".to_owned()))),
                    cell(Some(ScalarValue::Int8(2))), // rows 1, 3
                ],
                vec![
                    cell(Some(ScalarValue::Text("y".to_owned()))),
                    cell(Some(ScalarValue::Int8(1))), // row 2
                ],
            ]
        );
    }

    #[test]
    fn ungrouped_aggregate_over_an_empty_table_returns_one_row() {
        // `SELECT COUNT(*), SUM(a) FROM empty` → exactly one row: COUNT 0, SUM NULL.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        let r = select(&mut engine, "SELECT COUNT(*), SUM(a) FROM t");
        assert_eq!(
            r.rows,
            vec![vec![cell(Some(ScalarValue::Int8(0))), cell(None)]]
        );

        // A grouped aggregate over the empty table returns *no* rows.
        let r = select(&mut engine, "SELECT a, COUNT(*) FROM t GROUP BY a");
        assert!(r.rows.is_empty());
    }

    // ---- result shaping: ORDER BY / LIMIT / OFFSET / DISTINCT (STL-263) ----

    /// `t` seeded with duplicate values and a NULL, the shaping fixtures:
    /// `(1, 20, 'x'), (2, 10, 'y'), (3, 20, 'x'), (4, NULL, 'z'), (5, 10, 'y')`.
    fn seeded_wide() -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 20, 'x')",
            "INSERT INTO t VALUES (2, 10, 'y')",
            "INSERT INTO t VALUES (3, 20, 'x')",
            "INSERT INTO t VALUES (4, NULL, 'z')",
            "INSERT INTO t VALUES (5, 10, 'y')",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        engine
    }

    #[test]
    fn order_by_sorts_with_postgres_null_placement() {
        let mut engine = seeded_wide();
        // ASC: NULLs last; ties on `a` broken by the second key.
        let r = select(&mut engine, "SELECT id, a FROM t ORDER BY a, id");
        assert_eq!(
            r.rows,
            vec![
                vec![i4(2), i4(10)],
                vec![i4(5), i4(10)],
                vec![i4(1), i4(20)],
                vec![i4(3), i4(20)],
                vec![i4(4), cell(None)],
            ]
        );
        // DESC: NULLs first; both keys flipped.
        let r = select(&mut engine, "SELECT id, a FROM t ORDER BY a DESC, id DESC");
        assert_eq!(
            r.rows,
            vec![
                vec![i4(4), cell(None)],
                vec![i4(3), i4(20)],
                vec![i4(1), i4(20)],
                vec![i4(5), i4(10)],
                vec![i4(2), i4(10)],
            ]
        );
    }

    #[test]
    fn order_by_may_sort_on_an_unprojected_column() {
        // Postgres lets a plain SELECT sort on a column it does not project —
        // the sort runs over the full rows, before the projection drops `a`.
        let mut engine = seeded_wide();
        let r = select(&mut engine, "SELECT id FROM t ORDER BY a, id");
        assert_eq!(
            r.rows,
            vec![int_row(2), int_row(5), int_row(1), int_row(3), int_row(4)]
        );
    }

    #[test]
    fn limit_and_offset_slice_the_ordered_result() {
        let mut engine = seeded_wide();
        let ids =
            |sql: &str, engine: &mut SessionEngine<ZeroClock, MemDisk>| select(engine, sql).rows;
        assert_eq!(
            ids("SELECT id FROM t ORDER BY id LIMIT 2", &mut engine),
            vec![int_row(1), int_row(2)]
        );
        assert_eq!(
            ids("SELECT id FROM t ORDER BY id OFFSET 3", &mut engine),
            vec![int_row(4), int_row(5)]
        );
        assert_eq!(
            ids("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2", &mut engine),
            vec![int_row(3), int_row(4)]
        );
        // The standard FETCH FIRST spelling is the LIMIT alias.
        assert_eq!(
            ids(
                "SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH FIRST 2 ROWS ONLY",
                &mut engine
            ),
            vec![int_row(2), int_row(3)]
        );
        // LIMIT 0 and an OFFSET past the end are valid empty reads — the
        // header still describes the projection.
        let r = select(&mut engine, "SELECT id FROM t LIMIT 0");
        assert!(r.rows.is_empty());
        assert_eq!(r.columns, vec![("id".to_owned(), LogicalType::Int4)]);
        assert!(
            select(&mut engine, "SELECT id FROM t ORDER BY id OFFSET 99")
                .rows
                .is_empty()
        );
        // LIMIT without ORDER BY bounds the row count (which rows is
        // unspecified, as in Postgres).
        assert_eq!(
            select(&mut engine, "SELECT id FROM t LIMIT 3").rows.len(),
            3
        );
    }

    #[test]
    fn distinct_deduplicates_full_rows_including_nulls() {
        let mut engine = seeded_wide();
        // DISTINCT over one column: NULL rows collapse into one NULL output row
        // (the GROUP BY rule, not `=`). Ordered for a pinned expectation.
        let r = select(&mut engine, "SELECT DISTINCT a FROM t ORDER BY a");
        assert_eq!(r.rows, vec![vec![i4(10)], vec![i4(20)], vec![cell(None)]]);
        // DISTINCT is over the *full* projected row: (10,'y') and (20,'x')
        // each appear twice in the data and once here.
        let r = select(&mut engine, "SELECT DISTINCT a, b FROM t ORDER BY a");
        assert_eq!(
            r.rows,
            vec![
                vec![i4(10), txt("y")],
                vec![i4(20), txt("x")],
                vec![cell(None), txt("z")],
            ]
        );
        // Without ORDER BY the order is unspecified but the dedup holds.
        assert_eq!(
            select(&mut engine, "SELECT DISTINCT a FROM t").rows.len(),
            3
        );
    }

    #[test]
    fn distinct_composes_with_order_by_and_limit() {
        let mut engine = seeded_wide();
        // Pipeline order: DISTINCT → ORDER BY (DESC ⇒ NULL first) → LIMIT.
        let r = select(
            &mut engine,
            "SELECT DISTINCT a FROM t ORDER BY a DESC LIMIT 2",
        );
        assert_eq!(r.rows, vec![vec![cell(None)], vec![i4(20)]]);
    }

    #[test]
    fn distinct_order_by_outside_the_select_list_is_rejected() {
        let mut engine = seeded_wide();
        // Sorting on a column DISTINCT discarded — the 42P10 bind error
        // surfaces through execute, never a wrong answer.
        let err = engine
            .execute(&parse_one("SELECT DISTINCT a FROM t ORDER BY id"))
            .expect_err("DISTINCT + unprojected ORDER BY must fail");
        assert!(matches!(
            err,
            EngineError::Select(SelectError::DistinctOrderBy)
        ));
    }

    #[test]
    fn shaping_applies_to_aggregate_output() {
        let mut engine = seeded_wide();
        // Groups: a=10 ×2, a=20 ×2, a=NULL ×1. ORDER BY the aggregate's output
        // column (its default name), ties broken by the grouping column.
        let r = select(
            &mut engine,
            "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY count DESC, a LIMIT 2",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![i4(10), cell(Some(ScalarValue::Int8(2)))],
                vec![i4(20), cell(Some(ScalarValue::Int8(2)))],
            ]
        );
        // DISTINCT over the aggregate's output rows: counts {2, 2, 1} → {1, 2}.
        let r = select(
            &mut engine,
            "SELECT DISTINCT COUNT(*) FROM t GROUP BY a ORDER BY count",
        );
        assert_eq!(
            r.rows,
            vec![
                vec![cell(Some(ScalarValue::Int8(1)))],
                vec![cell(Some(ScalarValue::Int8(2)))],
            ]
        );
    }

    #[test]
    fn having_filters_groups_after_aggregation() {
        // STL-265: groups a=10 ×2, a=20 ×2, a=NULL ×1 (ids 1..=5, sums per group:
        // a=10 → 2+5=7, a=20 → 1+3=4, a=NULL → 4).
        let mut engine = seeded_wide();
        let i8 = |v: i64| cell(Some(ScalarValue::Int8(v)));

        // HAVING on the projected COUNT(*): the singleton NULL group (count 1) is
        // dropped, the two count-2 groups kept (emitted in key order, NULL first
        // had it survived).
        let r = select(
            &mut engine,
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
        );
        assert_eq!(r.rows, vec![vec![i4(10), i8(2)], vec![i4(20), i8(2)]]);

        // HAVING on an aggregate the SELECT list never projects: only SUM(id) > 5
        // (group a=10, sum 7) survives; the output column is still just `a`.
        let r = select(&mut engine, "SELECT a FROM t GROUP BY a HAVING SUM(id) > 5");
        assert_eq!(r.columns, vec![("a".to_owned(), LogicalType::Int4)]);
        assert_eq!(r.rows, vec![vec![i4(10)]]);

        // HAVING on a grouping column: a >= 20 keeps the a=20 group; the NULL group
        // is dropped (NULL >= 20 is unknown, never TRUE — the keep-TRUE-only rule).
        let r = select(
            &mut engine,
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING a >= 20",
        );
        assert_eq!(r.rows, vec![vec![i4(20), i8(2)]]);

        // HAVING composes with the result-shaping tail in Postgres order
        // (aggregate → HAVING → ORDER BY → LIMIT): filter to the count-2 groups,
        // then order by the grouping key descending, then take one.
        let r = select(
            &mut engine,
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1 ORDER BY a DESC LIMIT 1",
        );
        assert_eq!(r.rows, vec![vec![i4(20), i8(2)]]);

        // A HAVING that matches no group returns no rows (not every group).
        assert!(
            select(
                &mut engine,
                "SELECT a FROM t GROUP BY a HAVING COUNT(*) > 100",
            )
            .rows
            .is_empty()
        );
    }

    #[test]
    fn having_over_the_whole_table_group() {
        // No GROUP BY: HAVING filters the single whole-table group. COUNT(*) = 5.
        let mut engine = seeded_wide();
        let i8 = |v: i64| cell(Some(ScalarValue::Int8(v)));

        // The group passes — one row.
        let r = select(&mut engine, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 2");
        assert_eq!(r.rows, vec![vec![i8(5)]]);

        // The group fails — no rows at all (the whole-table group is filtered out).
        assert!(
            select(&mut engine, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 100")
                .rows
                .is_empty()
        );
    }

    #[test]
    fn having_compares_two_anchors_and_float_operands() {
        // STL-327: two-anchor comparisons and FLOAT8/AVG operands, end to end.
        // Groups: a=10 (ids 2,5: sum 7, max 5, avg 3.5), a=20 (ids 1,3: sum 4, max 3,
        // avg 2.0), a=NULL (id 4: sum 4, max 4, avg 4.0). Groups emit in key order.
        let mut engine = seeded_wide();

        // Grouping column (INT4) vs aggregate (INT8), promoted: `a > COUNT(*)` keeps
        // a=10 (10>2) and a=20 (20>2); the NULL group drops (NULL > 1 is unknown).
        let r = select(
            &mut engine,
            "SELECT a FROM t GROUP BY a HAVING a > COUNT(*)",
        );
        assert_eq!(r.rows, vec![vec![i4(10)], vec![i4(20)]]);

        // Aggregate (INT8 SUM) vs aggregate (INT4 MAX), promoted: `SUM(id) > MAX(id)`
        // keeps a=10 (7>5) and a=20 (4>3); the singleton NULL group drops (4 > 4).
        let r = select(
            &mut engine,
            "SELECT a FROM t GROUP BY a HAVING SUM(id) > MAX(id)",
        );
        assert_eq!(r.rows, vec![vec![i4(10)], vec![i4(20)]]);

        // FLOAT8 AVG against an integer literal: `AVG(id) < 3` keeps only a=20 (2.0);
        // a=10 (3.5) and the NULL group (4.0) drop.
        let r = select(&mut engine, "SELECT a FROM t GROUP BY a HAVING AVG(id) < 3");
        assert_eq!(r.rows, vec![vec![i4(20)]]);

        // FLOAT8 AVG against a decimal literal: `AVG(id) = 3.5` keeps only a=10.
        let r = select(
            &mut engine,
            "SELECT a FROM t GROUP BY a HAVING AVG(id) = 3.5",
        );
        assert_eq!(r.rows, vec![vec![i4(10)]]);

        // INT8 aggregate vs FLOAT8 aggregate, promoted to f64: `SUM(id) > AVG(id)`
        // keeps a=10 (7 > 3.5) and a=20 (4 > 2.0); the NULL group drops (4 > 4.0).
        let r = select(
            &mut engine,
            "SELECT a FROM t GROUP BY a HAVING SUM(id) > AVG(id)",
        );
        assert_eq!(r.rows, vec![vec![i4(10)], vec![i4(20)]]);
    }

    #[test]
    fn having_filters_groups_over_a_join() {
        // STL-327: HAVING composes over a join, resolving through the same scope the
        // GROUP BY / aggregates do. alice (id 1) has orders 10 & 11, bob (id 2) has
        // order 12, carol none (dropped by the inner join). Groups emit in key order.
        let mut engine = joinable_session();

        // Plain aggregate HAVING: alice's 2 orders survive `> 1`, bob's 1 does not.
        let r = select(
            &mut engine,
            "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.name HAVING COUNT(*) > 1",
        );
        assert_eq!(
            r.rows,
            vec![vec![txt("alice"), cell(Some(ScalarValue::Int8(2)))]]
        );

        // Two-anchor over the join: SUM(oid) (INT8) vs MAX(oid) (INT4), promoted.
        // alice sum 21 > max 11 (T); bob sum 12 > max 12 (F).
        let r = select(
            &mut engine,
            "SELECT users.name FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.name HAVING SUM(orders.oid) > MAX(orders.oid)",
        );
        assert_eq!(r.rows, vec![vec![txt("alice")]]);

        // FLOAT8 AVG over the join against a decimal literal: alice avg 10.5 (not >
        // 10.5), bob avg 12.0 (> 10.5).
        let r = select(
            &mut engine,
            "SELECT users.name FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.name HAVING AVG(orders.oid) > 10.5",
        );
        assert_eq!(r.rows, vec![vec![txt("bob")]]);
    }

    #[test]
    fn order_by_under_as_of_sorts_the_past_state() {
        // Shaping runs over the rows the snapshot resolves, so an AS OF read
        // orders by the *past* cells — deterministically.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 30, 'x')",
            "INSERT INTO t VALUES (2, 20, 'y')",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        let before = engine.commit_clock().0;
        // Flip the order of the two rows in the present.
        engine
            .execute(&parse_one("UPDATE t SET a = 10 WHERE id = 1"))
            .expect("update");

        let past = format!("SELECT id FROM t FOR SYSTEM_TIME AS OF {before} ORDER BY a, id");
        assert_eq!(
            select(&mut engine, &past).rows,
            vec![int_row(2), int_row(1)],
            "the past ordering uses the pre-update cell"
        );
        assert_eq!(
            select(&mut engine, "SELECT id FROM t ORDER BY a, id").rows,
            vec![int_row(1), int_row(2)],
            "the present ordering uses the updated cell"
        );
    }

    #[test]
    fn shaping_orders_after_the_ryow_overlay() {
        // Inside a transaction the shaping pipeline runs over the overlaid row
        // set ([STL-203]): buffered writes participate in DISTINCT/ORDER BY/
        // LIMIT exactly as committed rows do.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 20, 'x')",
            "INSERT INTO t VALUES (3, 10, 'y')",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        let mut txn = engine.begin();
        for sql in [
            "INSERT INTO t VALUES (2, 15, 'z')",
            "UPDATE t SET a = 5 WHERE id = 1",
        ] {
            engine
                .execute_in_txn(&parse_one(sql), &mut txn)
                .expect("stage");
        }
        let in_txn = |engine: &mut SessionEngine<ZeroClock, MemDisk>, txn: &mut _, sql: &str| {
            let StatementOutcome::Rows(r) = engine
                .execute_in_txn(&parse_one(sql), txn)
                .expect("in-txn select")
            else {
                panic!("rows");
            };
            r.rows
        };
        // Overlaid cells: id 1 → a=5 (buffered update), id 2 → a=15 (buffered
        // insert), id 3 → a=10 (committed).
        assert_eq!(
            in_txn(&mut engine, &mut txn, "SELECT id FROM t ORDER BY a"),
            vec![int_row(1), int_row(3), int_row(2)]
        );
        assert_eq!(
            in_txn(
                &mut engine,
                &mut txn,
                "SELECT id FROM t ORDER BY a DESC LIMIT 1"
            ),
            vec![int_row(2)]
        );
        // DISTINCT spans committed + buffered rows: ids 1 and 3 collapse on
        // b ('x' was overwritten? no — b is untouched by the update), so use
        // `a` values: {5, 10, 15} are already distinct; dedupe a duplicated
        // buffered value instead.
        engine
            .execute_in_txn(&parse_one("INSERT INTO t VALUES (4, 10, 'w')"), &mut txn)
            .expect("stage");
        assert_eq!(
            in_txn(&mut engine, &mut txn, "SELECT DISTINCT a FROM t ORDER BY a"),
            vec![vec![i4(5)], vec![i4(10)], vec![i4(15)]],
            "a committed 10 and a buffered 10 dedupe to one row"
        );
        drop(txn);
    }

    #[test]
    fn a_constant_period_predicate_is_honored_end_to_end() {
        // STL-165: a `WHERE PERIOD(..) <pred> PERIOD(..)` folds to a constant
        // truth value the engine applies. A true predicate returns every row; a
        // false one returns none — never a silently-unfiltered read.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 10, 'one')",
            "INSERT INTO t VALUES (2, 20, 'two')",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        // [10,40) CONTAINS [20,30) → true: every row survives, header preserved.
        let r = select(
            &mut engine,
            "SELECT id FROM t WHERE PERIOD(10, 40) CONTAINS PERIOD(20, 30)",
        );
        assert_eq!(r.columns, vec![("id".to_owned(), LogicalType::Int4)]);
        assert_eq!(
            r.rows,
            vec![
                vec![cell(Some(ScalarValue::Int4(1)))],
                vec![cell(Some(ScalarValue::Int4(2)))],
            ]
        );

        // [10,20) OVERLAPS [20,30) → false (half-open, they only touch): no rows,
        // but the column header is still the projected one.
        let r = select(
            &mut engine,
            "SELECT id FROM t WHERE PERIOD(10, 20) OVERLAPS PERIOD(20, 30)",
        );
        assert_eq!(r.columns, vec![("id".to_owned(), LogicalType::Int4)]);
        assert!(r.rows.is_empty());

        // The touching pair *does* satisfy PRECEDES (and MEETS): all rows return.
        let r = select(
            &mut engine,
            "SELECT id FROM t WHERE PERIOD(10, 20) PRECEDES PERIOD(20, 30)",
        );
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn per_row_period_predicate_matches_evaluate_oracle() {
        use stele_common::period::PeriodPredicate;

        // STL-193 oracle: a per-row `PERIOD(vf, vt) <pred> PERIOD(lo, hi)` must
        // return exactly the rows whose own `[vf, vt)` satisfies the predicate
        // against the probe `[lo, hi)`. The reference is `stele_exec::evaluate`
        // called directly over the decoded intervals — the same primitive the
        // engine evaluates per row, so a mismatch is a wiring bug, not a
        // semantics one.
        //
        // The rows and probes are *every* half-open `[a, b)` over a small grid of
        // boundary-relevant points, so each predicate is exercised true and false
        // across the touch / overlap / abut boundaries the half-open rule turns
        // on ([STL-165] truth table, lifted to the row level — DoD half-open
        // correctness). `vf` / `vt` are BIGINT so the rows are writable in plain
        // SQL (the zone-less TIMESTAMP literal codec is the deferred civil-time
        // follow-up); the engine reads each cell as µs identically.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE ev (id INT PRIMARY KEY, vf BIGINT, vt BIGINT) WITH SYSTEM VERSIONING",
            ))
            .expect("create");

        let grid = [0_i64, 5, 10, 15, 20, 25, 30];
        let intervals: Vec<(i64, i64)> = grid
            .iter()
            .flat_map(|&a| grid.iter().filter(move |&&b| b > a).map(move |&b| (a, b)))
            .collect();
        for (i, (vf, vt)) in intervals.iter().enumerate() {
            engine
                .execute(&parse_one(&format!(
                    "INSERT INTO ev VALUES ({i}, {vf}, {vt})"
                )))
                .expect("insert row");
        }

        let predicates = [
            ("CONTAINS", PeriodPredicate::Contains),
            ("OVERLAPS", PeriodPredicate::Overlaps),
            ("EQUALS", PeriodPredicate::Equals),
            ("PRECEDES", PeriodPredicate::Precedes),
            ("SUCCEEDS", PeriodPredicate::Succeeds),
            ("IMMEDIATELY PRECEDES", PeriodPredicate::ImmediatelyPrecedes),
            ("IMMEDIATELY SUCCEEDS", PeriodPredicate::ImmediatelySucceeds),
        ];
        for (kw, predicate) in predicates {
            for &(lo, hi) in &intervals {
                let probe = Interval::new(lo, hi).expect("probe well-formed");
                let sql = format!("SELECT id FROM ev WHERE PERIOD(vf, vt) {kw} PERIOD({lo}, {hi})");
                let mut got: Vec<i32> = select(&mut engine, &sql)
                    .rows
                    .into_iter()
                    .map(|row| {
                        let bytes = row[0].clone().expect("id is never NULL");
                        match ScalarValue::decode(LogicalType::Int4, &bytes).expect("decode id") {
                            ScalarValue::Int4(id) => id,
                            _ => panic!("id column is declared INT"),
                        }
                    })
                    .collect();
                got.sort_unstable();

                let mut expected: Vec<i32> = intervals
                    .iter()
                    .enumerate()
                    .filter(|&(_, &(vf, vt))| {
                        evaluate(
                            predicate,
                            Interval::new(vf, vt).expect("row well-formed"),
                            probe,
                        )
                    })
                    .map(|(i, _)| i32::try_from(i).expect("id fits i32"))
                    .collect();
                expected.sort_unstable();

                assert_eq!(got, expected, "predicate {kw} probe [{lo}, {hi})");
            }
        }
    }

    /// A `WHERE <timestamptz> <cmp> <literal>` now reaches the vectorized evaluator
    /// ([STL-213], closing the [STL-206] `UnsupportedColumn` gap for the new types)
    /// and orders by the underlying UTC instant. The reference is the same instant
    /// comparison computed directly from `parse_timestamptz` — the dumb oracle the
    /// testing strategy asks for the temporal case. A probe literal in a different
    /// zone is normalized first, so a row whose `ts` is one instant written two ways
    /// compares equal regardless of spelling.
    #[test]
    fn timestamptz_comparison_filters_by_the_instant() {
        use stele_common::datetime::parse_timestamptz;

        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE) \
                 WITH SYSTEM VERSIONING",
            ))
            .expect("create");

        // Distinct ids over assorted zones; row 3 is a `+05` spelling of row 2's
        // exact instant, so an equality probe at that instant must match both.
        let rows = [
            (1, "2024-01-15 00:00:00Z"),
            (2, "2024-01-15 07:00:00+00"),
            (3, "2024-01-15 12:00:00+05"),
            (4, "2024-06-01 12:30:00-04"),
            (5, "2023-12-31 23:59:59.5Z"),
        ];
        for (id, literal) in rows {
            engine
                .execute(&parse_one(&format!(
                    "INSERT INTO ev VALUES ({id}, '{literal}')"
                )))
                .expect("insert ts row");
        }
        let instant = |literal: &str| parse_timestamptz(literal).expect("probe literal parses");
        let row_instants: Vec<(i32, i64)> =
            rows.iter().map(|&(id, lit)| (id, instant(lit))).collect();

        let holds = |op: CmpOp, a: i64, b: i64| match op {
            CmpOp::Eq => a == b,
            CmpOp::Ne => a != b,
            CmpOp::Lt => a < b,
            CmpOp::Le => a <= b,
            CmpOp::Gt => a > b,
            CmpOp::Ge => a >= b,
        };
        let comparisons = [
            ("=", CmpOp::Eq),
            ("<>", CmpOp::Ne),
            ("<", CmpOp::Lt),
            ("<=", CmpOp::Le),
            (">", CmpOp::Gt),
            (">=", CmpOp::Ge),
        ];
        // Probes: one equals rows 2/3's instant (a third zone spelling of it), one
        // sits mid-set, one is past every row.
        let probes = [
            "2024-01-15 07:00:00Z",
            "2024-01-15 02:00:00-05",
            "2024-03-01 00:00:00Z",
        ];
        for &(sym, op) in &comparisons {
            for &probe in &probes {
                let pin = instant(probe);
                let sql = format!("SELECT id FROM ev WHERE ts {sym} '{probe}'");
                let mut got: Vec<i32> = select(&mut engine, &sql)
                    .rows
                    .into_iter()
                    .map(|row| {
                        let bytes = row[0].clone().expect("id is never NULL");
                        match ScalarValue::decode(LogicalType::Int4, &bytes).expect("decode id") {
                            ScalarValue::Int4(id) => id,
                            _ => panic!("id column is declared INT"),
                        }
                    })
                    .collect();
                got.sort_unstable();
                let mut want: Vec<i32> = row_instants
                    .iter()
                    .filter(|&&(_, t)| holds(op, t, pin))
                    .map(|&(id, _)| id)
                    .collect();
                want.sort_unstable();
                assert_eq!(got, want, "ts {sym} '{probe}'");
            }
        }
    }

    /// Integer `/` and `%` in a `WHERE` reach the evaluator over the live SQL path
    /// ([STL-213]). Truncating division and a remainder that follows the dividend's
    /// sign select exactly the expected keys.
    #[test]
    fn integer_division_and_modulo_filter_over_sql() {
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE n (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create");
        for (id, v) in [(1, 0), (2, 3), (3, 4), (4, 7), (5, -7)] {
            engine
                .execute(&parse_one(&format!("INSERT INTO n VALUES ({id}, {v})")))
                .expect("insert");
        }
        let ids = |engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str| -> Vec<i32> {
            let mut out: Vec<i32> = select(engine, sql)
                .rows
                .into_iter()
                .map(|row| {
                    let bytes = row[0].clone().expect("id never NULL");
                    match ScalarValue::decode(LogicalType::Int4, &bytes).expect("decode id") {
                        ScalarValue::Int4(id) => id,
                        _ => panic!("id is INT"),
                    }
                })
                .collect();
            out.sort_unstable();
            out
        };
        // v % 2 = 0 → even v: 0, 4 (ids 1, 3). -7 % 2 = -1, 3 % 2 = 1, 7 % 2 = 1.
        assert_eq!(
            ids(&mut engine, "SELECT id FROM n WHERE v % 2 = 0"),
            vec![1, 3]
        );
        // v / 2 = 2 → trunc-toward-zero: 4/2 = 2 (id 3) only (7/2 = 3, 3/2 = 1).
        assert_eq!(
            ids(&mut engine, "SELECT id FROM n WHERE v / 2 = 2"),
            vec![3]
        );
        // v % 2 = -1 → remainder takes the dividend's sign, so only -7 (id 5).
        assert_eq!(
            ids(&mut engine, "SELECT id FROM n WHERE v % 2 = -1"),
            vec![5]
        );
    }

    #[test]
    fn a_per_row_period_excludes_rows_with_a_null_endpoint() {
        // A NULL endpoint cell makes the row's period unknown; an unknown period
        // is never TRUE, so the row is dropped — the same 3VL stance the
        // `<col> = <lit>` filter takes for a NULL cell.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE ev (id INT PRIMARY KEY, vf BIGINT, vt BIGINT) WITH SYSTEM VERSIONING",
            ))
            .expect("create");
        engine
            .execute(&parse_one("INSERT INTO ev VALUES (1, 10, 40)"))
            .expect("row 1");
        engine
            .execute(&parse_one("INSERT INTO ev VALUES (2, NULL, 40)"))
            .expect("row 2 with a NULL vf");

        // [10, 40) CONTAINS [20, 30) holds for row 1; row 2's NULL `vf` excludes it.
        let r = select(
            &mut engine,
            "SELECT id FROM ev WHERE PERIOD(vf, vt) CONTAINS PERIOD(20, 30)",
        );
        assert_eq!(r.rows, vec![vec![cell(Some(ScalarValue::Int4(1)))]]);
    }

    #[test]
    fn per_row_period_over_a_both_axes_table() {
        // STL-193 on a genuine bitemporal table: the period is built from the
        // valid-time value columns `vf` / `vt`. A *plain* SELECT now reads those
        // value columns correctly — STL-218 strips the delta tier's framed prefix
        // on a no-valid-pin read, so no `FOR VALID_TIME AS OF` workaround is
        // needed and the per-row period predicate alone decides the result.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE booking (id INT PRIMARY KEY, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create");

        // Three keys, one valid version each; every window contains the pin (25).
        let rows = [(1, 10, 40), (2, 20, 30), (3, 0, 50)];
        for (txn, &(id, vf, vt)) in rows.iter().enumerate() {
            let payload = row_codec::encode_payload(&[
                cell(Some(ScalarValue::Timestamp(vf))),
                cell(Some(ScalarValue::Timestamp(vt))),
            ]);
            engine
                .insert(
                    "booking",
                    business_key(&ScalarValue::Int4(id)),
                    Some(ValidInterval::new(ValidTimeMicros(vf), ValidTimeMicros(vt)).unwrap()),
                    payload,
                    0,
                    TxnId(u64::try_from(txn).unwrap() + 1),
                    Principal::new(b"demo".to_vec()),
                )
                .expect("insert");
        }

        let ids = |engine: &mut SessionEngine<ZeroClock, MemDisk>, pred: &str| -> Vec<i32> {
            let sql = format!("SELECT id FROM booking WHERE PERIOD(vf, vt) {pred}");
            let mut got: Vec<i32> = select(engine, &sql)
                .rows
                .into_iter()
                .map(|row| {
                    let bytes = row[0].clone().expect("id is never NULL");
                    match ScalarValue::decode(LogicalType::Int4, &bytes).expect("decode id") {
                        ScalarValue::Int4(id) => id,
                        _ => panic!("id column is declared INT"),
                    }
                })
                .collect();
            got.sort_unstable();
            got
        };

        // CONTAINS [22, 28): every window covers it.
        assert_eq!(ids(&mut engine, "CONTAINS PERIOD(22, 28)"), vec![1, 2, 3]);
        // CONTAINS [5, 45): only the widest window [0, 50) covers it.
        assert_eq!(ids(&mut engine, "CONTAINS PERIOD(5, 45)"), vec![3]);
        // EQUALS [20, 30): only key 2's window matches exactly.
        assert_eq!(ids(&mut engine, "EQUALS PERIOD(20, 30)"), vec![2]);
        // PRECEDES [40, 50): windows ending at or before 40 — keys 1 and 2, the
        // half-open touch at 40 counting (key 1's `[10, 40)` precedes `[40, 50)`).
        assert_eq!(ids(&mut engine, "PRECEDES PERIOD(40, 50)"), vec![1, 2]);
    }

    // --- STL-315: valid-time interval pruning for per-row PERIOD predicates ---

    #[test]
    fn valid_overlap_probe_extracts_only_sound_pushdowns() {
        // A per-row PERIOD predicate over the table's own valid-time columns
        // against a constant probe yields a segment-prune pushdown only when the
        // predicate's truth *requires* the two periods to overlap.
        let valid_cols = (1usize, 2usize); // (valid_from, valid_to) schema positions
        let row = BoundPeriod {
            from: PeriodEndpoint::Column(1),
            to: PeriodEndpoint::Column(2),
        };
        let probe = BoundPeriod {
            from: PeriodEndpoint::Const(10),
            to: PeriodEndpoint::Const(20),
        };
        let pred = |predicate, left, right| BoundPeriodPredicate {
            left,
            predicate,
            right,
        };

        // Overlap-implying predicates push the constant probe down, with the row
        // period on either side (these predicates are symmetric or sound both ways).
        for p in [
            PeriodPredicate::Overlaps,
            PeriodPredicate::Contains,
            PeriodPredicate::Equals,
        ] {
            assert!(period_implies_overlap(p), "{p:?} implies overlap");
            assert_eq!(
                valid_overlap_probe(&pred(p, row, probe), valid_cols),
                Some((10, 20)),
                "{p:?}: row-on-left pushes the constant probe"
            );
            assert_eq!(
                valid_overlap_probe(&pred(p, probe, row), valid_cols),
                Some((10, 20)),
                "{p:?}: row-on-right pushes the constant probe"
            );
        }

        // Disjoint-by-definition predicates are true precisely when the periods do
        // *not* overlap, so they admit no overlap prune.
        for p in [
            PeriodPredicate::Precedes,
            PeriodPredicate::Succeeds,
            PeriodPredicate::ImmediatelyPrecedes,
            PeriodPredicate::ImmediatelySucceeds,
        ] {
            assert!(!period_implies_overlap(p), "{p:?} must not imply overlap");
            assert_eq!(
                valid_overlap_probe(&pred(p, row, probe), valid_cols),
                None,
                "{p:?} admits no overlap probe"
            );
        }

        // A period over the wrong column pair is not this table's valid axis.
        let other = BoundPeriod {
            from: PeriodEndpoint::Column(3),
            to: PeriodEndpoint::Column(4),
        };
        assert_eq!(
            valid_overlap_probe(&pred(PeriodPredicate::Overlaps, other, probe), valid_cols),
            None,
            "a non-valid-axis column pair is not pushed"
        );
        // The valid columns supplied in reversed (to, from) order do not match the
        // summary's `[valid_from, valid_to)` orientation.
        let swapped = BoundPeriod {
            from: PeriodEndpoint::Column(2),
            to: PeriodEndpoint::Column(1),
        };
        assert_eq!(
            valid_overlap_probe(&pred(PeriodPredicate::Overlaps, swapped, probe), valid_cols),
            None,
            "reversed valid columns are not the row's valid period"
        );
        // A non-constant "probe" (a column endpoint) leaves nothing constant to push.
        let non_const = BoundPeriod {
            from: PeriodEndpoint::Column(5),
            to: PeriodEndpoint::Const(20),
        };
        assert_eq!(
            valid_overlap_probe(&pred(PeriodPredicate::Overlaps, row, non_const), valid_cols),
            None,
            "a probe with a column endpoint is not pushable"
        );
        // Both operands the row axis (degenerate) — no constant probe at all.
        assert_eq!(
            valid_overlap_probe(&pred(PeriodPredicate::Overlaps, row, row), valid_cols),
            None,
        );
    }

    #[test]
    fn backdated_per_row_period_prunes_segments_on_a_valid_gap() {
        // STL-315 end-to-end oracle: a backdated workload sealed into one segment
        // whose valid envelope spans the timeline but whose coverage has a gap. A
        // per-row `PERIOD(vf, vt) OVERLAPS/CONTAINS PERIOD(lo, hi)` whose probe
        // falls in the gap must (a) return exactly the brute-force reference and
        // (b) skip the segment on the valid axis — byte-identical results, the
        // summary changing only speed. A probe in a covered band must not prune.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE booking (id INT PRIMARY KEY, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create");

        // Two old-band windows in [0, 10) and two new-band windows in [100, 200):
        // the segment envelope is [0, 200) yet nothing is valid in the gap
        // [10, 100), so the zone-map min/max cannot prune a gap probe — only the
        // interval summary can.
        let rows = [(1, 0, 10), (2, 2, 8), (3, 100, 150), (4, 120, 200)];
        for (txn, &(id, vf, vt)) in rows.iter().enumerate() {
            let payload = row_codec::encode_payload(&[
                cell(Some(ScalarValue::Timestamp(vf))),
                cell(Some(ScalarValue::Timestamp(vt))),
            ]);
            engine
                .insert(
                    "booking",
                    business_key(&ScalarValue::Int4(id)),
                    Some(ValidInterval::new(ValidTimeMicros(vf), ValidTimeMicros(vt)).unwrap()),
                    payload,
                    0,
                    TxnId(u64::try_from(txn).unwrap() + 1),
                    Principal::new(b"demo".to_vec()),
                )
                .expect("insert");
        }
        // Seal the delta into a segment carrying the valid-interval summary.
        engine.flush().expect("flush seals the backdated rows");

        let ids = |engine: &mut SessionEngine<ZeroClock, MemDisk>, pred: &str| -> Vec<i32> {
            let sql = format!("SELECT id FROM booking WHERE PERIOD(vf, vt) {pred}");
            let mut got: Vec<i32> = select(engine, &sql)
                .rows
                .into_iter()
                .map(|row| {
                    let bytes = row[0].clone().expect("id is never NULL");
                    match ScalarValue::decode(LogicalType::Int4, &bytes).expect("decode id") {
                        ScalarValue::Int4(id) => id,
                        _ => panic!("id column is declared INT"),
                    }
                })
                .collect();
            got.sort_unstable();
            got
        };
        let pruned_valid = |engine: &SessionEngine<ZeroClock, MemDisk>| {
            engine.metrics().scan_segments_pruned_valid.get()
        };

        // OVERLAPS rows: vf < hi && lo < vt. CONTAINS rows: vf <= lo && hi <= vt.
        // Each case names the expected result, whether a prune is expected, and
        // runs against the segment.
        let cases: &[(&str, Vec<i32>, bool)] = &[
            // A probe wholly in the gap [10, 100) — no row overlaps, the segment
            // is provably empty on the valid axis, so it is pruned.
            ("OVERLAPS PERIOD(20, 80)", vec![], true),
            ("CONTAINS PERIOD(30, 60)", vec![], true),
            // Probes that reach a covered band must not prune; the result is the
            // brute-force set.
            ("OVERLAPS PERIOD(5, 105)", vec![1, 2, 3], false),
            ("CONTAINS PERIOD(3, 7)", vec![1, 2], false),
            ("OVERLAPS PERIOD(110, 130)", vec![3, 4], false),
        ];
        let mut total_pruned = 0u64;
        for (pred, want, expect_prune) in cases {
            let before = pruned_valid(&engine);
            assert_eq!(&ids(&mut engine, pred), want, "result for `{pred}`");
            let delta = pruned_valid(&engine) - before;
            if *expect_prune {
                assert_eq!(
                    delta, 1,
                    "`{pred}` must prune the one scatter segment on a gap"
                );
            } else {
                assert_eq!(delta, 0, "`{pred}` reaches a covered band — no valid prune");
            }
            total_pruned += delta;
        }
        assert!(
            total_pruned > 0,
            "the overlap probe never pruned — the access path was untested"
        );
    }

    #[test]
    fn valid_axis_segment_prune_surfaces_in_the_query_stats_footer() {
        // STL-333 DoD: the per-query footer DTO (`QueryStats`, the "see the engine"
        // NoticeResponse trailer) must carry the valid-axis segment prune, which the
        // engine fold `query_stats()` previously dropped. Same backdated fixture as
        // the STL-315 oracle above — one segment, valid envelope [0, 200), coverage
        // gap [10, 100) — read through the full SQL bind→exec path so the assertion
        // is on the wire DTO, not the raw `ScanStats`. A per-row `PERIOD … OVERLAPS`
        // probe in the gap prunes the segment on the valid axis, and that prune must
        // appear in `segments_pruned_valid` (and so in `segments_pruned()`, keeping
        // the footer's `scanned + pruned == total` honest). A probe reaching a
        // covered band prunes nothing on the valid axis and reads the segment.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE booking (id INT PRIMARY KEY, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create");

        let rows = [(1, 0, 10), (2, 2, 8), (3, 100, 150), (4, 120, 200)];
        for (txn, &(id, vf, vt)) in rows.iter().enumerate() {
            let payload = row_codec::encode_payload(&[
                cell(Some(ScalarValue::Timestamp(vf))),
                cell(Some(ScalarValue::Timestamp(vt))),
            ]);
            engine
                .insert(
                    "booking",
                    business_key(&ScalarValue::Int4(id)),
                    Some(ValidInterval::new(ValidTimeMicros(vf), ValidTimeMicros(vt)).unwrap()),
                    payload,
                    0,
                    TxnId(u64::try_from(txn).unwrap() + 1),
                    Principal::new(b"demo".to_vec()),
                )
                .expect("insert");
        }
        engine.flush().expect("flush seals the backdated rows");

        let footer = |engine: &mut SessionEngine<ZeroClock, MemDisk>, pred: &str| -> QueryStats {
            let sql = format!("SELECT id FROM booking WHERE PERIOD(vf, vt) {pred}");
            select(engine, &sql)
                .stats
                .expect("a sealed read reports scan stats")
        };

        // A probe wholly inside the gap [10, 100): the segment holds no row valid in
        // the probe window, so the footer accounts it as a valid-axis prune.
        let gap = footer(&mut engine, "OVERLAPS PERIOD(20, 80)");
        assert_eq!(gap.segments_total, 1, "one sealed segment offered");
        assert_eq!(
            gap.segments_pruned_valid, 1,
            "the gap probe prunes the segment on the valid axis — and the footer says so"
        );
        assert_eq!(gap.segments_scanned, 0, "no segment is materialized");
        assert_eq!(
            gap.segments_pruned(),
            gap.segments_total,
            "the valid prune keeps the footer's scanned + pruned == total honest"
        );

        // A probe reaching a covered band prunes nothing on the valid axis; the
        // segment is read instead, so the footer shows no valid prune.
        let covered = footer(&mut engine, "OVERLAPS PERIOD(5, 105)");
        assert_eq!(
            covered.segments_pruned_valid, 0,
            "a probe reaching a covered band does not prune on the valid axis"
        );
        assert_eq!(covered.segments_scanned, 1, "the segment is materialized");
    }

    #[test]
    fn valid_axis_row_group_prune_surfaces_in_the_query_stats_footer() {
        // STL-339 DoD: the per-query footer DTO must also carry the *row-group-level*
        // valid prune (`row_groups_pruned_valid`, STL-316/STL-336) the engine fold
        // `query_stats()` previously dropped — the row-group-granular companion to
        // the segment-level prune STL-333 surfaced just above. Flushing one row per
        // row-group (the `with_flush_row_group_rows(1)` knob) scatters backdated
        // valid windows across several row-groups of a single segment. A per-row
        // `PERIOD … OVERLAPS` probe then survives the *segment*-level summary (one
        // row-group covers it) yet prunes the row-groups whose own summary it misses
        // — and that skip must show in the footer's `row_groups_pruned_valid` (so in
        // `row_groups_pruned()`, keeping the displayed `scanned + pruned == total`
        // honest on the row-group axis).
        let mut engine = session().with_flush_row_group_rows(1);
        engine
            .execute(&parse_one(
                "CREATE TABLE booking (id INT PRIMARY KEY, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create");

        // Two rows whose windows overlap the probe [20, 80) and two backdated to
        // [100, 200) — entirely outside it. One row per row-group, so each is its
        // own independently-prunable row-group whatever the business-key sort order.
        let rows = [(1, 20, 80), (2, 30, 70), (3, 100, 150), (4, 120, 200)];
        for (txn, &(id, vf, vt)) in rows.iter().enumerate() {
            let payload = row_codec::encode_payload(&[
                cell(Some(ScalarValue::Timestamp(vf))),
                cell(Some(ScalarValue::Timestamp(vt))),
            ]);
            engine
                .insert(
                    "booking",
                    business_key(&ScalarValue::Int4(id)),
                    Some(ValidInterval::new(ValidTimeMicros(vf), ValidTimeMicros(vt)).unwrap()),
                    payload,
                    0,
                    TxnId(u64::try_from(txn).unwrap() + 1),
                    Principal::new(b"demo".to_vec()),
                )
                .expect("insert");
        }
        engine.flush().expect("flush seals one row per row-group");

        let footer = |engine: &mut SessionEngine<ZeroClock, MemDisk>, pred: &str| -> QueryStats {
            let sql = format!("SELECT id FROM booking WHERE PERIOD(vf, vt) {pred}");
            select(engine, &sql)
                .stats
                .expect("a sealed read reports scan stats")
        };

        // The probe [20, 80) overlaps the first two row-groups but neither backdated
        // one. The segment as a whole still overlaps it, so the *segment* is not
        // pruned — the skip happens at row-group granularity.
        let gap = footer(&mut engine, "OVERLAPS PERIOD(20, 80)");
        assert_eq!(gap.segments_total, 1, "one sealed segment offered");
        assert_eq!(gap.segments_scanned, 1, "the segment survives — it is read");
        assert_eq!(
            gap.segments_pruned_valid, 0,
            "the prune is at row-group granularity, not segment-wholesale"
        );
        assert_eq!(
            gap.row_groups_total, 4,
            "one row per row-group ⇒ four row-groups"
        );
        assert_eq!(
            gap.row_groups_pruned_valid, 2,
            "the two out-of-window row-groups are pruned on the valid axis — the footer says so"
        );
        assert_eq!(
            gap.row_groups_scanned, 2,
            "only the overlapping row-groups are read"
        );
        assert_eq!(
            gap.row_groups_scanned + gap.row_groups_pruned(),
            gap.row_groups_total,
            "the valid prune keeps scanned + pruned == total honest on the row-group axis"
        );

        // A probe spanning every window prunes no row-group on the valid axis; all
        // four are read, so the footer shows no row-group valid prune.
        let covered = footer(&mut engine, "OVERLAPS PERIOD(0, 250)");
        assert_eq!(
            covered.row_groups_pruned_valid, 0,
            "a probe overlapping every window prunes no row-group on the valid axis"
        );
        assert_eq!(covered.row_groups_scanned, 4, "every row-group is read");
    }

    // --- STL-218: plain (no-valid-pin) reads of a both-axes table ------------

    /// A valid-time `acct (id, balance, vf, vt)` table with one typed-inserted
    /// row per `(id, balance, [vf, vt))`. The interval is framed on the delta
    /// payload and the period columns also ride the row codec, the same layout
    /// the SQL DML path (STL-194) writes.
    fn valid_time_acct(rows: &[(i32, i32, i64, i64)]) -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        for (i, &(id, balance, vf, vt)) in rows.iter().enumerate() {
            let payload = row_codec::encode_payload(&[
                cell(Some(ScalarValue::Int4(balance))),
                cell(Some(ScalarValue::Timestamp(vf))),
                cell(Some(ScalarValue::Timestamp(vt))),
            ]);
            engine
                .insert(
                    "acct",
                    business_key(&ScalarValue::Int4(id)),
                    Some(ValidInterval::new(ValidTimeMicros(vf), ValidTimeMicros(vt)).expect("iv")),
                    payload,
                    0,
                    TxnId(u64::try_from(i).unwrap() + 1),
                    Principal::new(b"demo".to_vec()),
                )
                .expect("insert");
        }
        engine
    }

    #[test]
    fn plain_select_reads_value_columns_on_a_both_axes_table() {
        // STL-218: a plain SELECT (no FOR VALID_TIME AS OF) on a valid-time table
        // returns every system-live row with its value columns — including the
        // period columns — decoded correctly. Before the fix the delta tier's
        // framed payload made ExplodePayload fail with an InvalidTag. The three
        // windows are *disjoint* (one open-ended), so no single valid instant
        // could keep them all: the plain read applies no valid filter.
        let mut engine = valid_time_acct(&[
            (1, 100, 10, 20),
            (2, 200, 30, 40),
            (3, 300, 50, i64::MAX), // open-ended valid period
        ]);

        // Project every column — the period columns read back as their stored
        // Timestamp cells, proving the frame is stripped.
        let mut r = select(&mut engine, "SELECT id, balance, vf, vt FROM acct");
        r.rows.sort_by(|a, b| a[0].cmp(&b[0]));
        assert_eq!(
            r.rows,
            vec![
                vec![
                    cell(Some(ScalarValue::Int4(1))),
                    cell(Some(ScalarValue::Int4(100))),
                    cell(Some(ScalarValue::Timestamp(10))),
                    cell(Some(ScalarValue::Timestamp(20))),
                ],
                vec![
                    cell(Some(ScalarValue::Int4(2))),
                    cell(Some(ScalarValue::Int4(200))),
                    cell(Some(ScalarValue::Timestamp(30))),
                    cell(Some(ScalarValue::Timestamp(40))),
                ],
                vec![
                    cell(Some(ScalarValue::Int4(3))),
                    cell(Some(ScalarValue::Int4(300))),
                    cell(Some(ScalarValue::Timestamp(50))),
                    cell(Some(ScalarValue::Timestamp(i64::MAX))),
                ],
            ],
        );

        // A value-column WHERE on a plain read filters correctly.
        assert_eq!(
            select(&mut engine, "SELECT id FROM acct WHERE balance = 200").rows,
            vec![vec![cell(Some(ScalarValue::Int4(2)))]],
        );
        // A period-column projection under a key predicate.
        assert_eq!(
            select(&mut engine, "SELECT vf, vt FROM acct WHERE id = 3").rows,
            vec![vec![
                cell(Some(ScalarValue::Timestamp(50))),
                cell(Some(ScalarValue::Timestamp(i64::MAX))),
            ]],
        );
        // An aggregate folds the same plain-read rows.
        assert_eq!(
            select(&mut engine, "SELECT COUNT(*), SUM(balance) FROM acct").rows,
            vec![vec![
                cell(Some(ScalarValue::Int8(3))),
                cell(Some(ScalarValue::Int8(600))),
            ]],
        );
    }

    /// A deterministic splitmix64 for the STL-218 oracle — a seed replays an
    /// identical workload, with no dependency on the sim crate.
    struct PlainOracleRng(u64);
    impl PlainOracleRng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        /// A uniform value in `0..n` (`n > 0`), no `as` casts.
        fn below(&mut self, n: i64) -> i64 {
            i64::try_from(self.next() % u64::try_from(n).expect("positive")).expect("fits")
        }
    }

    /// Decode an `int4` / `timestamp` result cell back to its `i64` payload.
    fn decode_int(value: Option<&Vec<u8>>, ty: LogicalType) -> i64 {
        let bytes = value.expect("cell is never NULL in this workload");
        match ScalarValue::decode(ty, bytes).expect("decode") {
            ScalarValue::Int4(v) => i64::from(v),
            ScalarValue::Timestamp(v) => v,
            // Static message: interpolating the decoded ScalarValue here trips
            // CodeQL's (false) cleartext-logging taint on its Debug.
            _ => panic!("expected an int4/timestamp result column"),
        }
    }

    /// One seed's built history: a valid-time `acct` engine and the naïve
    /// reference — per key, the present system-live `(balance, vf, vt)`.
    type HistoryRun = (
        SessionEngine<ZeroClock, MemDisk>,
        BTreeMap<i32, (i64, i64, i64)>,
    );

    /// Apply one seed's random INSERT/UPDATE/DELETE history to a fresh valid-time
    /// `acct` engine, returning both the engine and the naïve reference: per key,
    /// the present system-live `(balance, vf, vt)`, absent after a delete.
    fn build_valid_time_history(rng: &mut PlainOracleRng) -> HistoryRun {
        const KEY_POOL: i64 = 4;
        let mut engine = valid_time_acct(&[]);
        let mut model: BTreeMap<i32, (i64, i64, i64)> = BTreeMap::new();
        let who = || Principal::new(b"demo".to_vec());

        let ops = 8 + rng.below(12);
        for op in 0..ops {
            let id = i32::try_from(rng.below(KEY_POOL)).expect("fits");
            let key = business_key(&ScalarValue::Int4(id));
            let txn = TxnId(u64::try_from(op).expect("fits") + 1);
            let alive = model.contains_key(&id);
            if alive && rng.below(3) == 0 {
                engine.delete("acct", &key, txn, who()).expect("delete");
                model.remove(&id);
                continue;
            }
            let balance = i64::from(i32::try_from(op + 1).expect("fits"));
            let vf = rng.below(50);
            let vt = if rng.below(4) == 0 {
                i64::MAX
            } else {
                vf + 1 + rng.below(50)
            };
            let payload = row_codec::encode_payload(&[
                cell(Some(ScalarValue::Int4(
                    i32::try_from(balance).expect("fits"),
                ))),
                cell(Some(ScalarValue::Timestamp(vf))),
                cell(Some(ScalarValue::Timestamp(vt))),
            ]);
            let interval =
                ValidInterval::new(ValidTimeMicros(vf), ValidTimeMicros(vt)).expect("iv");
            if alive {
                engine
                    .update("acct", key, Some(interval), payload, 0, txn, who())
                    .expect("update");
            } else {
                engine
                    .insert("acct", key, Some(interval), payload, 0, txn, who())
                    .expect("insert");
            }
            model.insert(id, (balance, vf, vt));
        }
        (engine, model)
    }

    #[test]
    fn plain_select_both_axes_matches_a_naive_reference() {
        // STL-218 correctness oracle. A random typed INSERT/UPDATE/DELETE history
        // on a valid-time table (varied windows, some open-ended) is applied to
        // both the engine and a naïve reference, then a *plain* SELECT (no valid
        // pin) is diffed against the reference. The reference keeps, per key, the
        // latest system-live version's `(balance, vf, vt)` — exactly the
        // "every system-live row, no valid filter" semantics. The query itself is
        // the teeth: before the fix a plain read of a framed delta payload errors.
        const SEEDS: u64 = 48;
        let mut rng = PlainOracleRng(0x1234_5678);
        let mut rows_seen: u64 = 0;

        for _seed in 0..SEEDS {
            let (mut engine, model) = build_valid_time_history(&mut rng);

            let mut got: BTreeMap<i32, (i64, i64, i64)> = BTreeMap::new();
            for row in select(&mut engine, "SELECT id, balance, vf, vt FROM acct").rows {
                let id =
                    i32::try_from(decode_int(row[0].as_ref(), LogicalType::Int4)).expect("id fits");
                let cells = (
                    decode_int(row[1].as_ref(), LogicalType::Int4),
                    decode_int(row[2].as_ref(), LogicalType::Timestamp),
                    decode_int(row[3].as_ref(), LogicalType::Timestamp),
                );
                let fresh = got.insert(id, cells).is_none();
                // Static message — a decode-derived value in the message trips
                // CodeQL's (false) cleartext-logging taint.
                assert!(fresh, "the plain read returned two rows for one key");
            }
            assert_eq!(got, model, "plain read diverged from the naïve reference");
            rows_seen += u64::try_from(got.len()).expect("fits");
        }
        assert!(
            rows_seen > 0,
            "every seed resolved an empty table — widen the workload"
        );
    }

    #[test]
    fn multi_column_update_preserves_unset_columns() {
        // A partial UPDATE rewrites only its named columns; the rest keep their
        // prior value via the engine's read-modify-write.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10, 'one')"))
            .expect("insert");
        engine
            .execute(&parse_one("UPDATE t SET b = 'updated' WHERE id = 1"))
            .expect("update b only");

        let r = select(&mut engine, "SELECT a, b FROM t WHERE id = 1");
        assert_eq!(
            r.rows,
            vec![vec![
                cell(Some(ScalarValue::Int4(10))),                   // unchanged
                cell(Some(ScalarValue::Text("updated".to_owned()))), // rewritten
            ]],
            "the unset column `a` keeps its prior value"
        );
    }

    #[test]
    fn multi_column_null_cell_round_trips_and_filters_out() {
        // A NULL value cell reads back as NULL, and `WHERE col = <lit>` excludes
        // it (NULL = x is never true).
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, NULL, 'one')"))
            .expect("insert with null a");

        let r = select(&mut engine, "SELECT a FROM t WHERE id = 1");
        assert_eq!(r.rows, vec![vec![cell(None)]], "a reads back as SQL NULL");

        assert!(
            select(&mut engine, "SELECT id FROM t WHERE a = 10")
                .rows
                .is_empty(),
            "a NULL cell does not match an equality predicate"
        );
    }

    /// The value cell of every row in a [`SelectResult`] — the **last** projected
    /// column. Now that the projection list is honored ([STL-151]), the value
    /// column the tests assert on is whichever they listed last (`SELECT balance`
    /// or `SELECT id, balance`), not a fixed second cell.
    fn payload_column(result: &SelectResult) -> Vec<Vec<u8>> {
        result
            .rows
            .iter()
            .map(|row| {
                row.last()
                    .cloned()
                    .flatten()
                    .expect("non-null value in this test")
            })
            .collect()
    }

    // ---- joins (STL-172) ----

    /// The canonical encoding of an `int4` cell — the bytes the join's
    /// reconstructed rows carry, so expected rows are built without decoding.
    fn i4(v: i32) -> Option<Vec<u8>> {
        cell(Some(ScalarValue::Int4(v)))
    }

    /// The canonical encoding of a `text` cell.
    fn txt(s: &str) -> Option<Vec<u8>> {
        cell(Some(ScalarValue::Text(s.to_owned())))
    }

    /// The result rows, sorted — joins do not order their output (no `ORDER BY`),
    /// so tests compare row *sets*.
    fn sorted(mut rows: Vec<Vec<Option<Vec<u8>>>>) -> Vec<Vec<Option<Vec<u8>>>> {
        rows.sort();
        rows
    }

    /// A session with `users (id INT, name TEXT)` and `orders (oid INT, uid INT)`,
    /// rows joinable on `users.id = orders.uid`:
    /// users `{1: alice, 2: bob, 3: carol}`; orders `{10→1, 11→1, 12→2}` (so alice
    /// has two orders, bob one, carol none).
    fn joinable_session() -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        for ddl in [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING",
            "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        for dml in [
            "INSERT INTO users VALUES (1, 'alice')",
            "INSERT INTO users VALUES (2, 'bob')",
            "INSERT INTO users VALUES (3, 'carol')",
            "INSERT INTO orders VALUES (10, 1)",
            "INSERT INTO orders VALUES (11, 1)",
            "INSERT INTO orders VALUES (12, 2)",
        ] {
            engine.execute(&parse_one(dml)).expect("insert");
        }
        engine
    }

    #[test]
    fn inner_join_combines_matching_rows() {
        let mut engine = joinable_session();
        let result = select(
            &mut engine,
            "SELECT * FROM users JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(
            result.columns,
            vec![
                ("id".to_owned(), LogicalType::Int4),
                ("name".to_owned(), LogicalType::Text),
                ("oid".to_owned(), LogicalType::Int4),
                ("uid".to_owned(), LogicalType::Int4),
            ]
        );
        // alice(1) has orders 10 and 11; bob(2) has 12; carol(3) is dropped.
        assert_eq!(
            sorted(result.rows),
            sorted(vec![
                vec![i4(1), txt("alice"), i4(10), i4(1)],
                vec![i4(1), txt("alice"), i4(11), i4(1)],
                vec![i4(2), txt("bob"), i4(12), i4(2)],
            ])
        );
    }

    #[test]
    fn left_join_keeps_unmatched_left_rows_null_extended() {
        let mut engine = joinable_session();
        let result = select(
            &mut engine,
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.uid",
        );
        // carol(3) has no order → a single NULL-extended row.
        assert_eq!(
            sorted(result.rows),
            sorted(vec![
                vec![i4(1), txt("alice"), i4(10), i4(1)],
                vec![i4(1), txt("alice"), i4(11), i4(1)],
                vec![i4(2), txt("bob"), i4(12), i4(2)],
                vec![i4(3), txt("carol"), None, None],
            ])
        );
    }

    #[test]
    fn semi_join_keeps_left_rows_with_a_match_once() {
        let mut engine = joinable_session();
        let result = select(
            &mut engine,
            "SELECT * FROM users SEMI JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(
            result.columns,
            vec![
                ("id".to_owned(), LogicalType::Int4),
                ("name".to_owned(), LogicalType::Text),
            ],
            "SEMI projects only the left table's columns"
        );
        // alice and bob have orders (alice once, not twice); carol does not.
        assert_eq!(
            sorted(result.rows),
            sorted(vec![vec![i4(1), txt("alice")], vec![i4(2), txt("bob")]])
        );
    }

    #[test]
    fn anti_join_keeps_left_rows_with_no_match() {
        let mut engine = joinable_session();
        let result = select(
            &mut engine,
            "SELECT id, name FROM users ANTI JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(sorted(result.rows), vec![vec![i4(3), txt("carol")]]);
    }

    #[test]
    fn join_projection_selects_and_reorders_across_both_sides() {
        let mut engine = joinable_session();
        let result = select(
            &mut engine,
            "SELECT orders.oid, users.name FROM users JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(
            result.columns,
            vec![
                ("oid".to_owned(), LogicalType::Int4),
                ("name".to_owned(), LogicalType::Text),
            ]
        );
        assert_eq!(
            sorted(result.rows),
            sorted(vec![
                vec![i4(10), txt("alice")],
                vec![i4(11), txt("alice")],
                vec![i4(12), txt("bob")],
            ])
        );
    }

    #[test]
    fn join_over_an_empty_right_side() {
        // No orders: INNER empty, LEFT all NULL-extended, ANTI all kept.
        let mut engine = session();
        for ddl in [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING",
            "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        engine
            .execute(&parse_one("INSERT INTO users VALUES (1, 'alice')"))
            .expect("insert");

        let inner = select(
            &mut engine,
            "SELECT * FROM users JOIN orders ON users.id = orders.uid",
        );
        assert!(inner.rows.is_empty());

        let left = select(
            &mut engine,
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(left.rows, vec![vec![i4(1), txt("alice"), None, None]]);

        let anti = select(
            &mut engine,
            "SELECT id FROM users ANTI JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(anti.rows, vec![vec![i4(1)]]);
    }

    #[test]
    fn join_on_a_text_key() {
        // A TEXT join key on both sides (a value column = a business key) exercises
        // the non-integer decode path through `run_join`.
        let mut engine = session();
        for ddl in [
            "CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT) WITH SYSTEM VERSIONING",
            "CREATE TABLE dept (name TEXT PRIMARY KEY, floor INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        for dml in [
            "INSERT INTO emp VALUES (1, 'eng')",
            "INSERT INTO emp VALUES (2, 'sales')",
            "INSERT INTO dept VALUES ('eng', 3)",
        ] {
            engine.execute(&parse_one(dml)).expect("insert");
        }
        // emp.dept (a TEXT value column) joins to dept.name (the TEXT business key);
        // emp 2 ('sales') has no department, so the inner join drops it.
        let result = select(
            &mut engine,
            "SELECT emp.id, dept.floor FROM emp JOIN dept ON emp.dept = dept.name",
        );
        assert_eq!(sorted(result.rows), vec![vec![i4(1), i4(3)]]);
    }

    // ---- clauses compose over a join (STL-264) ----

    #[test]
    fn where_filters_join_output() {
        let mut engine = joinable_session();
        // A WHERE over a qualified output column ([STL-264]): only alice's orders.
        let by_key = select(
            &mut engine,
            "SELECT users.name, orders.oid FROM users JOIN orders \
             ON users.id = orders.uid WHERE users.id = 1",
        );
        assert_eq!(
            sorted(by_key.rows),
            sorted(vec![vec![txt("alice"), i4(10)], vec![txt("alice"), i4(11)]])
        );
        // The WHERE may filter on an *unprojected* join column (`users.name`).
        let unprojected = select(
            &mut engine,
            "SELECT orders.oid FROM users JOIN orders ON users.id = orders.uid \
             WHERE users.name = 'bob'",
        );
        assert_eq!(unprojected.rows, vec![vec![i4(12)]]);
    }

    #[test]
    fn aggregate_groups_join_output() {
        let mut engine = joinable_session();
        // GROUP BY a qualified left column over the join; groups emit in key order.
        let grouped = select(
            &mut engine,
            "SELECT users.name, COUNT(*) FROM users JOIN orders \
             ON users.id = orders.uid GROUP BY users.name",
        );
        assert_eq!(
            grouped.columns,
            vec![
                ("name".to_owned(), LogicalType::Text),
                ("count".to_owned(), LogicalType::Int8),
            ]
        );
        // carol has no orders (dropped by the inner join); alice 2, bob 1.
        assert_eq!(
            grouped.rows,
            vec![
                vec![txt("alice"), cell(Some(ScalarValue::Int8(2)))],
                vec![txt("bob"), cell(Some(ScalarValue::Int8(1)))],
            ]
        );
        // Ungrouped: the whole join is one group — three matched rows.
        let total = select(
            &mut engine,
            "SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(total.rows, vec![vec![cell(Some(ScalarValue::Int8(3)))]]);
    }

    #[test]
    fn order_by_limit_and_distinct_shape_join_output() {
        let mut engine = joinable_session();
        // ORDER BY a (qualified) column over the join, then LIMIT — deterministic.
        let top = select(
            &mut engine,
            "SELECT orders.oid FROM users JOIN orders ON users.id = orders.uid \
             ORDER BY orders.oid DESC LIMIT 2",
        );
        assert_eq!(top.rows, vec![vec![i4(12)], vec![i4(11)]]);
        // DISTINCT dedups the projected join rows (alice appears twice).
        let names = select(
            &mut engine,
            "SELECT DISTINCT users.name FROM users JOIN orders ON users.id = orders.uid",
        );
        assert_eq!(
            sorted(names.rows),
            sorted(vec![vec![txt("alice")], vec![txt("bob")]])
        );
        // DISTINCT + ORDER BY a *qualified* projected column is legal and ordered
        // (the qualifier disambiguates after the join; not a 42P10 — STL-264).
        let ordered_distinct = select(
            &mut engine,
            "SELECT DISTINCT users.name FROM users JOIN orders ON users.id = orders.uid \
             ORDER BY users.name DESC",
        );
        assert_eq!(
            ordered_distinct.rows,
            vec![vec![txt("bob")], vec![txt("alice")]]
        );
    }

    #[test]
    fn full_clause_stack_composes_over_a_join() {
        // The DoD shape: SELECT … FROM a JOIN b ON … WHERE … GROUP BY … ORDER BY …
        // LIMIT … end to end ([STL-264]).
        let mut engine = joinable_session();
        let result = select(
            &mut engine,
            "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.uid \
             WHERE orders.oid > 9 GROUP BY users.name ORDER BY count DESC, name LIMIT 1",
        );
        // All oids > 9, so every match survives: alice 2, bob 1 → DESC, LIMIT 1.
        assert_eq!(
            result.rows,
            vec![vec![txt("alice"), cell(Some(ScalarValue::Int8(2)))]]
        );
    }

    // ---- N-way left-deep join chains (STL-323) ----

    /// `users ⋈ orders ⋈ products`: orders link a user (`uid`) to a product
    /// (`pid`). Order 13 references a missing product (300), so it survives the first
    /// inner join but is dropped by an inner/semi join to `products` — exercising the
    /// chain's second `ON` against a non-seed accumulated input.
    fn three_table_session() -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        for ddl in [
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING",
            "CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, pid INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE products (pid INT PRIMARY KEY, label TEXT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        for dml in [
            "INSERT INTO users VALUES (1, 'alice')",
            "INSERT INTO users VALUES (2, 'bob')",
            "INSERT INTO users VALUES (3, 'carol')",
            "INSERT INTO orders VALUES (10, 1, 100)",
            "INSERT INTO orders VALUES (11, 1, 200)",
            "INSERT INTO orders VALUES (12, 2, 100)",
            "INSERT INTO orders VALUES (13, 2, 300)",
            "INSERT INTO products VALUES (100, 'widget')",
            "INSERT INTO products VALUES (200, 'gadget')",
        ] {
            engine.execute(&parse_one(dml)).expect("insert");
        }
        engine
    }

    #[test]
    fn three_way_inner_join_chains_left_deep() {
        // The second `ON` references the *middle* input (`orders.pid`): the chain
        // joins (users ⋈ orders) against products, so the left key addresses the
        // accumulated output, not the seed alone ([STL-323]).
        let mut engine = three_table_session();
        let result = select(
            &mut engine,
            "SELECT users.name, orders.oid, products.label FROM users \
             JOIN orders ON users.id = orders.uid \
             JOIN products ON orders.pid = products.pid",
        );
        assert_eq!(
            result.columns,
            vec![
                ("name".to_owned(), LogicalType::Text),
                ("oid".to_owned(), LogicalType::Int4),
                ("label".to_owned(), LogicalType::Text),
            ]
        );
        // Order 13 (product 300, missing) is dropped by the inner join to products;
        // carol (no order) by the first inner join.
        assert_eq!(
            sorted(result.rows),
            sorted(vec![
                vec![txt("alice"), i4(10), txt("widget")],
                vec![txt("alice"), i4(11), txt("gadget")],
                vec![txt("bob"), i4(12), txt("widget")],
            ])
        );
    }

    #[test]
    fn a_left_join_in_a_chain_null_extends_downstream() {
        // A LEFT chain keeps every left row: carol (no order) and order 13 (missing
        // product) both survive, NULL-extended on the unmatched side.
        let mut engine = three_table_session();
        let result = select(
            &mut engine,
            "SELECT users.name, orders.oid, products.label FROM users \
             LEFT JOIN orders ON users.id = orders.uid \
             LEFT JOIN products ON orders.pid = products.pid",
        );
        assert_eq!(
            sorted(result.rows),
            sorted(vec![
                vec![txt("alice"), i4(10), txt("widget")],
                vec![txt("alice"), i4(11), txt("gadget")],
                vec![txt("bob"), i4(12), txt("widget")],
                vec![txt("bob"), i4(13), None],
                vec![txt("carol"), None, None],
            ])
        );
    }

    #[test]
    fn a_semi_or_anti_step_filters_the_accumulated_left() {
        // A SEMI step keeps the accumulated (users ⋈ orders) rows that have a product
        // (dropping order 13); an ANTI step keeps only those that don't.
        let mut engine = three_table_session();
        let semi = select(
            &mut engine,
            "SELECT users.name, orders.oid FROM users \
             JOIN orders ON users.id = orders.uid \
             SEMI JOIN products ON orders.pid = products.pid",
        );
        assert_eq!(
            sorted(semi.rows),
            sorted(vec![
                vec![txt("alice"), i4(10)],
                vec![txt("alice"), i4(11)],
                vec![txt("bob"), i4(12)],
            ])
        );
        let anti = select(
            &mut engine,
            "SELECT users.name, orders.oid FROM users \
             JOIN orders ON users.id = orders.uid \
             ANTI JOIN products ON orders.pid = products.pid",
        );
        assert_eq!(anti.rows, vec![vec![txt("bob"), i4(13)]]);
    }

    #[test]
    fn n_way_join_composes_with_the_full_clause_stack() {
        // The DoD shape over three inputs: WHERE / GROUP BY / ORDER BY over the
        // chain's output ([STL-323]).
        let mut engine = three_table_session();
        let result = select(
            &mut engine,
            "SELECT products.label, COUNT(*) FROM users \
             JOIN orders ON users.id = orders.uid \
             JOIN products ON orders.pid = products.pid \
             WHERE users.id = 1 GROUP BY products.label ORDER BY label",
        );
        assert_eq!(
            result.columns,
            vec![
                ("label".to_owned(), LogicalType::Text),
                ("count".to_owned(), LogicalType::Int8),
            ]
        );
        // alice (id 1) bought widget (order 10) and gadget (order 11): one each,
        // ordered by label.
        assert_eq!(
            result.rows,
            vec![
                vec![txt("gadget"), cell(Some(ScalarValue::Int8(1)))],
                vec![txt("widget"), cell(Some(ScalarValue::Int8(1)))],
            ]
        );
    }

    #[test]
    fn n_way_join_under_as_of_reads_one_consistent_snapshot() {
        // A statement-level `FOR SYSTEM_TIME AS OF s` over a three-input chain reads
        // *every* input at the one pin ([STL-243], [STL-323]): the join at `s1` sees
        // all three tables' first version, never a mix with the later updates.
        let mut engine = session();
        for ddl in [
            "CREATE TABLE a (k INT PRIMARY KEY, av INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE b (k INT PRIMARY KEY, bv INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE c (k INT PRIMARY KEY, cv INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        for dml in [
            "INSERT INTO a VALUES (1, 10)",
            "INSERT INTO b VALUES (1, 20)",
            "INSERT INTO c VALUES (1, 30)",
        ] {
            engine.execute(&parse_one(dml)).expect("insert v1");
        }
        let s1 = engine.commit_clock();
        for dml in [
            "UPDATE a SET av = 11 WHERE k = 1",
            "UPDATE b SET bv = 21 WHERE k = 1",
            "UPDATE c SET cv = 31 WHERE k = 1",
        ] {
            engine.execute(&parse_one(dml)).expect("update v2");
        }

        let chain = "SELECT a.av, b.bv, c.cv FROM a \
                     JOIN b ON a.k = b.k \
                     JOIN c ON b.k = c.k";
        // At `s1`: every input's first version, the one consistent snapshot.
        let as_of = select(
            &mut engine,
            &format!("{chain} FOR SYSTEM_TIME AS OF {}", s1.0),
        );
        assert_eq!(as_of.rows, vec![vec![i4(10), i4(20), i4(30)]]);
        // At the present: every input's updated version.
        let live = select(&mut engine, chain);
        assert_eq!(live.rows, vec![vec![i4(11), i4(21), i4(31)]]);
    }

    #[test]
    fn a_four_table_chain_folds_through_two_intermediate_steps() {
        // Three steps: two intermediate columnar folds feed each other before the
        // final row-major step ([STL-323]) — so a gathered accumulated column is
        // itself re-gathered by the next step, the path a 3-table chain never hits.
        let mut engine = session();
        for ddl in [
            "CREATE TABLE a (k INT PRIMARY KEY, av INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE b (k INT PRIMARY KEY, bv INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE c (k INT PRIMARY KEY, cv INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE d (k INT PRIMARY KEY, dv INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        for dml in [
            "INSERT INTO a VALUES (1, 10)",
            "INSERT INTO a VALUES (2, 20)",
            "INSERT INTO b VALUES (1, 11)",
            "INSERT INTO c VALUES (1, 12)",
            "INSERT INTO d VALUES (1, 13)",
            "INSERT INTO d VALUES (2, 23)",
        ] {
            engine.execute(&parse_one(dml)).expect("insert");
        }
        // Only key 1 is present in all four inputs; key 2 (missing from b/c) is
        // dropped by the inner chain.
        let result = select(
            &mut engine,
            "SELECT a.av, b.bv, c.cv, d.dv FROM a \
             JOIN b ON a.k = b.k \
             JOIN c ON b.k = c.k \
             JOIN d ON c.k = d.k",
        );
        assert_eq!(result.rows, vec![vec![i4(10), i4(11), i4(12), i4(13)]]);
    }

    // ---- durable catalog + cold-boot recovery (STL-210, ADR-0028) ----

    use stele_storage::backend::{DiskFile as _, FaultOp, Faults};

    /// Boot a session from `disk`'s existing on-disk state — the restart half
    /// of every round-trip below.
    fn recover_session(disk: &MemDisk) -> SessionEngine<ZeroClock, MemDisk> {
        SessionEngine::recover(disk.clone(), ZeroClock).expect("recover")
    }

    #[test]
    fn recovery_round_trips_rows_and_as_of_across_a_restart() {
        // The DoD round trip: CREATE → INSERT/UPDATE/DELETE, then a process
        // restart, then SELECT (current and AS OF) answers exactly as the live
        // session did. Dropping the engine *is* the kill: the session never
        // checkpoints or flushes, so recovery runs from the WALs + catalog log
        // alone — the crash-consistency the WAL-fsync invariant promises.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert 1");
        let s1 = engine.clock.current();
        for dml in [
            "UPDATE account SET balance = 250 WHERE id = 1",
            "INSERT INTO account VALUES (2, 7)",
            "DELETE FROM account WHERE id = 2", // a retraction must survive too
        ] {
            engine.execute(&parse_one(dml)).expect("dml");
        }
        let now_sql = "SELECT id, balance FROM account";
        let as_of_sql = format!("{now_sql} FOR SYSTEM_TIME AS OF {}", s1.0);
        let live_now = sorted(select(&mut engine, now_sql).rows);
        let live_as_of = sorted(select(&mut engine, &as_of_sql).rows);
        drop(engine);

        let mut engine = recover_session(&disk);
        let tables = engine.describe_live_tables();
        assert_eq!(
            tables,
            vec![TableDescription {
                name: "account".to_owned(),
                columns: vec![
                    ("id".to_owned(), LogicalType::Int4),
                    ("balance".to_owned(), LogicalType::Int4),
                ],
            }],
            "the catalog resolves the table at its schema"
        );
        assert_eq!(
            sorted(select(&mut engine, now_sql).rows),
            live_now,
            "the current read answers as the live session did (update + deletion gap survive)"
        );
        assert_eq!(live_now, vec![vec![i4(1), i4(250)]]);
        assert_eq!(
            sorted(select(&mut engine, &as_of_sql).rows),
            live_as_of,
            "the AS OF read answers as the live session did"
        );
        assert_eq!(live_as_of, vec![vec![i4(1), i4(100)]]);
    }

    // ---- online backup + restore (STL-249) ----
    //
    // The v0.3 exit criterion's second clause: a backup taken under live write
    // load, restored into a fresh data dir, is byte-for-byte identical for the
    // immutable set, and every AS OF read at or before the fence answers
    // identically pre/post restore. This is a SessionEngine-level differential
    // oracle (in-process MemDisk sweep, the same home as the STL-210/215 recovery
    // coverage) — backup is the multi-table, shared-log operation SessionEngine
    // owns, and the oracle drives the whole SQL bind→exec→storage path.

    /// A small, dependency-free deterministic PRNG (SplitMix64) so the backup
    /// oracle's write load varies per seed yet replays identically — the same
    /// determinism the simulation harness relies on, without pulling stele-sim in.
    fn split_mix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Apply one random, always-valid bitemporal op against `account`, keeping
    /// `live` (the currently-present business keys) in sync. Inserts use a fresh,
    /// never-reused id (`next_id`) so there is never a primary-key conflict;
    /// updates and deletes target a currently-live id. The mix builds genuine
    /// system-time history — supersessions and deletion gaps — for `AS OF` to read.
    fn apply_random_op(
        engine: &mut SessionEngine<ZeroClock, MemDisk>,
        rng: &mut u64,
        next_id: &mut u64,
        live: &mut std::collections::BTreeSet<u64>,
    ) {
        let roll = split_mix(rng) % 3;
        if live.is_empty() || roll == 0 {
            let id = *next_id;
            *next_id += 1;
            let bal = split_mix(rng) % 1000;
            engine
                .execute(&parse_one(&format!(
                    "INSERT INTO account VALUES ({id}, {bal})"
                )))
                .expect("insert");
            live.insert(id);
        } else if roll == 1 {
            let id = nth_live(live, split_mix(rng));
            let bal = split_mix(rng) % 1000;
            engine
                .execute(&parse_one(&format!(
                    "UPDATE account SET balance = {bal} WHERE id = {id}"
                )))
                .expect("update");
        } else {
            let id = nth_live(live, split_mix(rng));
            engine
                .execute(&parse_one(&format!("DELETE FROM account WHERE id = {id}")))
                .expect("delete");
            live.remove(&id);
        }
    }

    /// The `r`-th currently-live id (wrapping) — a deterministic pick from the set.
    fn nth_live(live: &std::collections::BTreeSet<u64>, r: u64) -> u64 {
        let idx = usize::try_from(r % live.len() as u64).expect("index fits usize");
        *live.iter().nth(idx).expect("live is non-empty")
    }

    #[test]
    fn backup_under_write_load_round_trips_byte_for_byte_and_preserves_as_of() {
        const NOW_SQL: &str = "SELECT id, balance FROM account";
        for seed in 0..24u64 {
            let mut rng = seed.wrapping_add(1);
            let mut next_id = 1u64;
            let mut live = std::collections::BTreeSet::new();

            // The byte-for-byte check compares the restored disk against the
            // backup; the live engine's own disk is internal.
            let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
            engine.execute(&parse_one(CREATE)).expect("create");

            // Phase 1: pre-fence write load, sampling AS OF probe instants along
            // the way.
            let mut probes: Vec<SystemTimeMicros> = Vec::new();
            let pre_ops = 12 + split_mix(&mut rng) % 18;
            for _ in 0..pre_ops {
                apply_random_op(&mut engine, &mut rng, &mut next_id, &mut live);
                if split_mix(&mut rng) % 3 == 0 {
                    probes.push(engine.clock.current());
                }
            }
            probes.push(engine.clock.current()); // always probe the latest pre-fence instant

            // Fence + backup, under (the absence of) concurrent writes — the admin
            // command holds the session lock, so the on-disk set is the fence state.
            let backup = MemDisk::new();
            let manifest = engine.backup(&backup).expect("backup");
            let fence = SystemTimeMicros(manifest.fence_micros);
            probes.push(fence);
            assert!(
                !manifest.files.is_empty(),
                "seed {seed}: the backup captured a non-empty immutable set"
            );

            // The live engine's answers at the fence: current state (== AS OF
            // fence, since nothing has committed past it yet) and each probe.
            let live_at_fence = sorted(select(&mut engine, NOW_SQL).rows);
            let live_as_of: Vec<_> = probes
                .iter()
                .map(|t| {
                    sorted(
                        select(
                            &mut engine,
                            &format!("{NOW_SQL} FOR SYSTEM_TIME AS OF {}", t.0),
                        )
                        .rows,
                    )
                })
                .collect();

            // Phase 2: post-fence write load — these commits must NOT survive into
            // the restore (the backup is a clean prefix cut at the fence).
            let post_ops = 5 + split_mix(&mut rng) % 12;
            for _ in 0..post_ops {
                apply_random_op(&mut engine, &mut rng, &mut next_id, &mut live);
            }

            // Restore into a fresh disk and boot it through normal recovery.
            let restored_disk = MemDisk::new();
            crate::backup::restore_disk(&backup, &restored_disk).expect("restore");
            let mut restored = recover_session(&restored_disk);

            // (1) Every AS OF read at or before the fence is identical pre/post
            // restore — system-time history is immutable, so the cut is exact.
            for (t, expected) in probes.iter().zip(&live_as_of) {
                assert_eq!(
                    &sorted(
                        select(
                            &mut restored,
                            &format!("{NOW_SQL} FOR SYSTEM_TIME AS OF {}", t.0)
                        )
                        .rows
                    ),
                    expected,
                    "seed {seed}: AS OF {} diverged after restore",
                    t.0
                );
            }

            // (2) The restored "now" is exactly the live AS-OF-fence state: the
            // post-fence writes are absent.
            assert_eq!(
                sorted(select(&mut restored, NOW_SQL).rows),
                live_at_fence,
                "seed {seed}: restored current state is not the fence state (post-fence writes leaked in, or data was lost)"
            );

            // (3) Byte-for-byte: every file the manifest lists is identical in the
            // restored dir (independently re-read, not just trusting restore's own
            // checksum pass), and the manifest itself is not materialized into the
            // data dir.
            assert!(
                restored_disk
                    .list()
                    .unwrap()
                    .iter()
                    .all(|n| n != "MANIFEST")
            );
            for entry in &manifest.files {
                let restored_bytes =
                    crate::backup::read_all(&restored_disk, &entry.name).expect("read restored");
                let backup_bytes =
                    crate::backup::read_all(&backup, &entry.name).expect("read backup");
                assert_eq!(
                    restored_bytes, backup_bytes,
                    "seed {seed}: {} is not byte-for-byte identical after restore",
                    entry.name
                );
            }
        }
    }

    #[test]
    fn restore_refuses_a_tampered_commit_log() {
        // A flipped byte in the backed-up commit log is caught at restore by the
        // manifest's per-file checksum (the first of the two tamper layers; the
        // STL-178 hash chain re-verifies on the recover that follows). Build real
        // committed history so `stele.commits` is non-trivial.
        let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        engine
            .execute(&parse_one("UPDATE account SET balance = 200 WHERE id = 1"))
            .expect("update");

        let backup = MemDisk::new();
        let manifest = engine.backup(&backup).expect("backup");
        assert!(
            manifest.files.iter().any(|f| f.name == "stele.commits"),
            "the commit log is part of the immutable set"
        );

        // Flip a byte in the backed-up commit log (same length, different content).
        use stele_storage::backend::DiskFile as _;
        let mut tampered = crate::backup::read_all(&backup, "stele.commits").expect("read commits");
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        backup.remove("stele.commits").expect("remove");
        {
            let mut f = backup.create("stele.commits").expect("recreate");
            f.append(&tampered).expect("append");
            f.sync().expect("sync");
        }

        let restored = MemDisk::new();
        let err = crate::backup::restore_disk(&backup, &restored).expect_err("tamper refused");
        assert!(
            matches!(err, crate::backup::RestoreError::ChecksumMismatch { name } if name == "stele.commits"),
            "a flipped byte in the commit log must be refused at restore"
        );
    }

    #[test]
    fn backup_to_admin_command_produces_a_restorable_backup() {
        // The wire trigger ([STL-219] shape): `BACKUP TO '<dir>'` routes through
        // `execute` → `apply_admin`, fences, and writes a backup to a *local*
        // directory regardless of the engine's own backend. Exercises the one
        // place the generic engine names a concrete backend, so it uses the real
        // filesystem (cleaned up on drop).
        let dirs = Scratch::new("engine-backup-admin");
        let backup_dir = dirs.path().join("backup");

        let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (7, 70)"))
            .expect("insert");

        let backup_sql = format!("BACKUP TO '{}'", backup_dir.display());
        let outcome = engine
            .execute(&parse_one(&backup_sql))
            .expect("backup command");
        assert_eq!(
            outcome,
            StatementOutcome::Ddl { tag: "BACKUP" },
            "BACKUP reports its CommandComplete tag"
        );
        assert!(
            backup_dir.join("MANIFEST").is_file(),
            "a manifest was written"
        );

        // The on-disk backup restores and recovers, and the row survives.
        let src = stele_storage::backend::LocalDisk::open(&backup_dir).expect("open backup");
        let restored_dir = dirs.path().join("restored");
        let dst = stele_storage::backend::LocalDisk::open(&restored_dir).expect("open restored");
        crate::backup::restore_disk(&src, &dst).expect("restore");
        let mut restored = SessionEngine::recover(dst, ZeroClock).expect("recover");
        // `select` is MemDisk-typed; this engine is LocalDisk-backed, so read directly.
        let StatementOutcome::Rows(result) = restored
            .execute(&parse_one("SELECT id, balance FROM account"))
            .expect("select")
        else {
            panic!("SELECT must return rows");
        };
        assert_eq!(
            sorted(result.rows),
            vec![vec![i4(7), i4(70)]],
            "the restored engine serves the backed-up row"
        );
    }

    /// A unique scratch directory under the OS temp dir, removed on drop. The
    /// backup oracle is otherwise all-`MemDisk`; only the `BACKUP TO '<path>'`
    /// wire path needs a real local directory (its target is always local disk).
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("stele-engine-{label}-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create scratch dir");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    // ---- cross-table crash-atomic commit (STL-215) ----
    //
    // A multi-table COMMIT makes each table's writes a durable-but-inert two-phase
    // record, then fsyncs one commit marker after every leg is durable. On recovery
    // a leg is replayed only if its marker is present, so a crash between the
    // per-table commits and the marker recovers the whole transaction all-or-none
    // across every table. A single-table / auto-commit write is the same shape now
    // ([STL-314]): its data leg is two-phase, gated on its own commit record, so a
    // crash between the data fsync and the commit-record fsync discards it too. The
    // cross-table coordination lives in `SessionEngine`, which stele-sim cannot
    // depend on (the per-table sims cover the storage half), so the seed-reproducible
    // crash coverage is this in-process FaultDisk/MemDisk sweep — the same pattern
    // STL-210 used for session-level kill coverage.

    /// Drop the **last** commit record from `stele.commits`, keeping every earlier
    /// one — the precise on-disk shape of a crash after a commit's data leg is
    /// durable but before its own commit record's fsync completes ([STL-314]).
    /// Removing the whole file would also drop the *baseline* commits' records,
    /// which are now their own commit-record-gated legs (a single-table commit is
    /// no longer an unconditionally-applied plain record).
    fn truncate_last_commit_record(disk: &MemDisk) {
        use stele_storage::backend::DiskFile as _;
        const FRAME: usize = 8 + stele_txn::COMMIT_RECORD_LEN + 4;
        let name = crate::commit_log::COMMIT_LOG_FILENAME;
        let file = disk.open(name).expect("open commit log");
        let len = usize::try_from(file.len()).expect("small file");
        assert!(len >= FRAME, "at least one commit record to drop");
        let mut bytes = vec![0u8; len];
        file.read_at(0, &mut bytes).expect("read");
        bytes.truncate(len - FRAME);
        disk.remove(name).expect("remove");
        disk.create(name)
            .expect("create")
            .append(&bytes)
            .expect("append");
    }

    /// Create two system-versioned tables `a` and `b`, then auto-commit a baseline
    /// row into each. Each baseline insert is its own commit-record-gated two-phase
    /// leg ([STL-314]), so it survives recovery as long as *its* commit record does.
    fn two_tables_with_baseline(engine: &mut SessionEngine<ZeroClock, MemDisk>) {
        for ddl in [
            "CREATE TABLE a (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE b (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        engine
            .execute(&parse_one("INSERT INTO a VALUES (1, 100)"))
            .expect("a baseline");
        engine
            .execute(&parse_one("INSERT INTO b VALUES (1, 10)"))
            .expect("b baseline");
    }

    /// Stage and commit a two-table transaction inserting `id = 2` into both tables.
    fn commit_two_table_txn(
        engine: &mut SessionEngine<ZeroClock, MemDisk>,
    ) -> Result<(), EngineError> {
        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO a VALUES (2, 200)"), &mut txn)
            .expect("stage a");
        engine
            .stage_dml(&parse_one("INSERT INTO b VALUES (2, 20)"), &mut txn)
            .expect("stage b");
        engine.commit(txn)
    }

    /// The sorted `id` cells of every current row in `table`.
    fn ids(engine: &mut SessionEngine<ZeroClock, MemDisk>, table: &str) -> Vec<Option<Vec<u8>>> {
        sorted(select(engine, &format!("SELECT id FROM {table}")).rows)
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect()
    }

    #[test]
    fn a_multi_table_commit_is_durable_when_its_marker_lands() {
        // The happy path: every leg and the marker reach disk, so after a restart
        // both tables show the transaction's row.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        two_tables_with_baseline(&mut engine);
        commit_two_table_txn(&mut engine).expect("commit");
        // The multi-table commit's record is the marker recovery gates its two-phase
        // legs on (and, post-ADR-0031, its link in the hash chain).
        assert!(
            disk.open(crate::commit_log::COMMIT_LOG_FILENAME).is_ok(),
            "a multi-table commit writes a commit record",
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            ids(&mut engine, "a"),
            vec![i4(1), i4(2)],
            "a recovers both rows",
        );
        assert_eq!(
            ids(&mut engine, "b"),
            vec![i4(1), i4(2)],
            "b recovers both rows",
        );
    }

    #[test]
    fn a_lost_commit_marker_discards_every_table_leg() {
        // The crash STL-215 closes: every per-table leg reached disk, but the marker
        // that vouches them did not (modelled by removing it). Recovery must discard
        // *both* legs — all-or-none = none — not leave one table's write durable (the
        // partial commit the per-table-WAL design would otherwise allow).
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        two_tables_with_baseline(&mut engine);
        commit_two_table_txn(&mut engine).expect("commit");
        drop(engine);

        // The marker's fsync never completed: drop just that last record, keeping
        // every leg — and the baseline commits' own records — on disk.
        truncate_last_commit_record(&disk);

        let mut engine = recover_session(&disk);
        assert_eq!(
            ids(&mut engine, "a"),
            vec![i4(1)],
            "a's two-phase leg is discarded without the marker — only the baseline survives",
        );
        assert_eq!(
            ids(&mut engine, "b"),
            vec![i4(1)],
            "b's leg is discarded too — the transaction recovers all-or-none = none",
        );
    }

    #[test]
    fn a_torn_commit_marker_is_ignored_on_recovery() {
        // A marker whose append was sheared by a crash (a partial trailing frame) is
        // not acknowledged, so the transaction recovers as uncommitted.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        two_tables_with_baseline(&mut engine);
        commit_two_table_txn(&mut engine).expect("commit");
        drop(engine);

        // Re-write the marker file truncated to a partial frame (MemDisk files are
        // append-only, so rebuild it with the shear).
        let name = crate::commit_log::COMMIT_LOG_FILENAME;
        let file = disk.open(name).expect("open marker");
        let len = usize::try_from(file.len()).expect("small file");
        let mut bytes = vec![0u8; len];
        file.read_at(0, &mut bytes).expect("read");
        disk.remove(name).expect("remove");
        let mut torn = disk.create(name).expect("create");
        torn.append(&bytes[..len - 3]).expect("append torn");

        let mut engine = recover_session(&disk);
        assert_eq!(
            ids(&mut engine, "a"),
            vec![i4(1)],
            "a torn marker leaves a's leg uncommitted",
        );
        assert_eq!(
            ids(&mut engine, "b"),
            vec![i4(1)],
            "a torn marker leaves b's leg uncommitted",
        );
    }

    #[test]
    fn the_single_table_fast_path_writes_a_commit_chain_record() {
        // A single-table COMMIT writes one hash-chain commit record (ADR-0031) that
        // is now *also* the marker its two-phase data leg is gated on ([STL-314]) —
        // so the live commit log is verifiable end-to-end and the leg recovers
        // all-or-none with its record. Observable proxy: the commit log exists with
        // one record, and the writes recover whole.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create account");
        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (1, 100)"), &mut txn)
            .expect("stage 1");
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (2, 200)"), &mut txn)
            .expect("stage 2");
        engine.commit(txn).expect("commit");
        let records = crate::commit_log::replay(&disk).expect("replay commit log");
        assert_eq!(
            records.len(),
            1,
            "a single-table commit writes exactly one hash-chain record",
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            ids(&mut engine, "account"),
            vec![i4(1), i4(2)],
            "the single-table commit recovers whole",
        );
    }

    #[test]
    fn an_auto_commit_crash_before_the_commit_record_discards_the_leg() {
        // The commit-record crash window ([STL-314], [ADR-0031]): an auto-commit
        // write's data leg is now a two-phase record gated on its own commit record,
        // so a crash after the leg is durable but before the commit record fsyncs
        // recovers all-or-none — the unwitnessed leg is discarded, never left
        // durable-but-unchained. (Before STL-314 this leg was a plain record applied
        // unconditionally, so it would have survived as an unchained commit — the
        // very gap this oracle pins shut.)
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("commit 1");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
            .expect("commit 2 — its data leg is durable");
        drop(engine);

        // The crash: the 2nd commit's data leg reached the table WAL (durable,
        // two-phase), but its commit record never fsynced — drop just that record.
        truncate_last_commit_record(&disk);

        let mut engine = recover_session(&disk);
        assert_eq!(
            ids(&mut engine, "account"),
            vec![i4(1)],
            "the chained commit survives; the unchained leg is discarded — window closed",
        );
        // The recovered chain still verifies clean — no leg without a record, no
        // record without its leg.
        let StatementOutcome::Rows(audit) = engine
            .execute(&parse_one("SELECT * FROM stele_audit('account')"))
            .expect("audit")
        else {
            panic!("rows");
        };
        assert!(
            matches!(
                ScalarValue::decode(
                    LogicalType::Bool,
                    audit.rows[0][4].as_ref().expect("verdict")
                ),
                Ok(ScalarValue::Bool(true)),
            ),
            "the recovered commit chain verifies",
        );
    }

    #[test]
    fn a_commit_log_failure_poisons_the_session_and_recovery_drops_the_leg() {
        // If the commit record fails to reach disk *after* the data leg is durable
        // (the commit record is the commit point now, [STL-314]), the just-applied
        // write is witnessed by no record — recovery would discard it, diverging from
        // the live process. So the session poisons (is_poisoned → ops `/readyz`
        // unready), refuses further statements, and a restart into `recover` drops the
        // unwitnessed leg, reconverging. (Mirrors the per-table WAL poison for the
        // commit-log WAL ADR-0031 left surfaced-but-not-poisoned.)
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("commit 1 creates the commit log");

        // Fail the *next* `Disk::open` — the commit-log append opens `stele.commits`,
        // but the data leg's WAL is already open, so its append + fsync complete first
        // and only the commit record fails. The write is durable but unwitnessed.
        disk.faults().schedule(FaultOp::Open, io::ErrorKind::Other);
        assert!(
            engine
                .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
                .is_err(),
            "a commit-log failure fails the statement",
        );
        assert!(
            engine.is_poisoned(),
            "a commit-log failure poisons the session (ops /readyz turns unready)",
        );
        assert!(
            engine
                .execute(&parse_one("SELECT id FROM account"))
                .is_err(),
            "a poisoned session refuses every further statement",
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert!(
            !engine.is_poisoned(),
            "recovery opens a fresh, unpoisoned session",
        );
        assert_eq!(
            ids(&mut engine, "account"),
            vec![i4(1)],
            "the unwitnessed leg is discarded on recovery — live and recovered converge",
        );
    }

    #[test]
    fn a_multi_table_commit_under_injected_faults_recovers_all_or_none() {
        // Seed-reproducible: across crash models — a lost marker, an fsync fault on
        // the first leg, an append fault on the first leg, and a clean commit — a
        // multi-table commit never recovers a partial subset. Either both tables show
        // the transaction's row (marker durable) or neither does (marker absent);
        // never one. A fixed seed always drives the same model, so a failure
        // reproduces exactly.
        for seed in 0..48u64 {
            let disk = MemDisk::new();
            let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
            two_tables_with_baseline(&mut engine);

            let model = seed % 4;
            let expect_committed = match model {
                // All legs + marker durable, then the marker is lost (just that
                // last record — the baseline commits' records stay).
                0 => {
                    commit_two_table_txn(&mut engine).expect("commit");
                    truncate_last_commit_record(&disk);
                    false
                }
                // The first leg's fsync fails — commit aborts before the marker.
                1 => {
                    disk.faults()
                        .schedule(FaultOp::Sync, std::io::ErrorKind::Other);
                    assert!(
                        commit_two_table_txn(&mut engine).is_err(),
                        "seed {seed}: an fsync fault must fail the commit",
                    );
                    false
                }
                // The first leg's append fails — nothing of the txn is durable.
                2 => {
                    disk.faults()
                        .schedule(FaultOp::Append, std::io::ErrorKind::Other);
                    assert!(
                        commit_two_table_txn(&mut engine).is_err(),
                        "seed {seed}: an append fault must fail the commit",
                    );
                    false
                }
                // A clean commit: the marker lands, the transaction is durable.
                _ => {
                    commit_two_table_txn(&mut engine).expect("commit");
                    true
                }
            };
            drop(engine);

            let mut engine = recover_session(&disk);
            let a = ids(&mut engine, "a");
            let b = ids(&mut engine, "b");
            if expect_committed {
                assert_eq!(a, vec![i4(1), i4(2)], "seed {seed}: a committed whole");
                assert_eq!(b, vec![i4(1), i4(2)], "seed {seed}: b committed whole");
            } else {
                assert_eq!(
                    a,
                    vec![i4(1)],
                    "seed {seed} (model {model}): a recovers baseline only",
                );
                assert_eq!(
                    b,
                    vec![i4(1)],
                    "seed {seed} (model {model}): b recovers baseline only — never a partial subset",
                );
            }
        }
    }

    // ---- manual flush / checkpoint (STL-195) ----

    use stele_storage::wal::LogOffset;

    #[test]
    fn flush_seals_every_table_and_recover_replays_only_the_tail() {
        // The session-level manual flush ([STL-195]) drives *every* resident
        // table's storage `Engine`: it seals each delta into a sealed segment and
        // advances each table's recovery floor, so a later restart replays only
        // the WAL tail written *after* the flush, not the whole log. Two tables
        // prove the driver fans out rather than touching just the first.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        for ddl in [
            "CREATE TABLE a (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE b (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        // Pre-flush writes — these land in the delta tier + WAL.
        engine
            .execute(&parse_one("INSERT INTO a VALUES (1, 100)"))
            .expect("a1");
        engine
            .execute(&parse_one("INSERT INTO b VALUES (1, 10)"))
            .expect("b1");

        // The manual flush: seals both deltas into segments and advances both
        // floors past the records those segments now cover.
        engine.flush().expect("flush");
        for name in ["a", "b"] {
            let st = engine.tables.get(name).expect("tier resident");
            assert!(
                !st.engine.segment_names().is_empty(),
                "{name}: flush sealed a segment",
            );
            assert!(
                st.engine.replay_floor() > LogOffset::ZERO,
                "{name}: flush advanced the recovery floor off the origin",
            );
        }
        let floor_a = engine.tables.get("a").unwrap().engine.replay_floor();
        let floor_b = engine.tables.get("b").unwrap().engine.replay_floor();

        // Post-flush writes — these stay in the WAL tail, past the floors.
        engine
            .execute(&parse_one("INSERT INTO a VALUES (2, 200)"))
            .expect("a2");
        engine
            .execute(&parse_one("UPDATE b SET balance = 20 WHERE id = 1"))
            .expect("b2");
        let live_a = sorted(select(&mut engine, "SELECT id, balance FROM a").rows);
        let live_b = sorted(select(&mut engine, "SELECT id, balance FROM b").rows);
        drop(engine);

        // Restart: recovery composes each segment prefix with the replayed tail.
        let mut engine = recover_session(&disk);
        // The recovered floors are exactly the flushed floors loaded from each
        // checkpoint manifest — recovery resumed replay from the tail, not the log
        // origin (a full replay would leave the floor at `ZERO`).
        assert_eq!(
            engine.tables.get("a").unwrap().engine.replay_floor(),
            floor_a,
            "a resumed from the flushed floor, not the log origin",
        );
        assert_eq!(
            engine.tables.get("b").unwrap().engine.replay_floor(),
            floor_b,
            "b resumed from the flushed floor, not the log origin",
        );
        for name in ["a", "b"] {
            assert!(
                !engine
                    .tables
                    .get(name)
                    .unwrap()
                    .engine
                    .segment_names()
                    .is_empty(),
                "{name}: the sealed segment is adopted on recovery",
            );
        }
        // …and the data is whole: the pre-flush segment rows and the post-flush
        // tail rows both survive and compose.
        assert_eq!(
            sorted(select(&mut engine, "SELECT id, balance FROM a").rows),
            live_a,
        );
        assert_eq!(live_a, vec![vec![i4(1), i4(100)], vec![i4(2), i4(200)]]);
        assert_eq!(
            sorted(select(&mut engine, "SELECT id, balance FROM b").rows),
            live_b,
        );
        assert_eq!(live_b, vec![vec![i4(1), i4(20)]]);
    }

    #[test]
    fn checkpoint_fences_every_table_without_sealing() {
        // A session checkpoint is the *lightweight* durability fence: it
        // group-commit fsyncs each table's WAL and records the fence, but does
        // NOT seal the delta into a segment, so recovery still replays each
        // table's whole log. It is flush's cheaper sibling ([STL-195]/[STL-177]).
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        engine.checkpoint().expect("checkpoint");
        let st = engine.tables.get("account").expect("tier resident");
        assert!(
            st.engine.segment_names().is_empty(),
            "a checkpoint seals no segment",
        );
        assert_eq!(
            st.engine.replay_floor(),
            LogOffset::ZERO,
            "a checkpoint leaves the replay floor at the log origin",
        );
        assert!(
            st.engine.durable_fence().is_some(),
            "a checkpoint records a durable fence",
        );

        let live = sorted(select(&mut engine, "SELECT id, balance FROM account").rows);
        drop(engine);
        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, "SELECT id, balance FROM account").rows),
            live,
            "the checkpointed data survives the restart",
        );
        assert_eq!(live, vec![vec![i4(1), i4(100)]]);
    }

    #[test]
    fn flush_drives_a_dropped_tables_retained_tier() {
        // A dropped table keeps its tier resident for AS OF history, and that
        // tier's WAL is replayed on the next recover — so the manual flush must
        // drive it too, bounding that replay. The driver iterates *every* resident
        // tier, not just the catalog-live ones; this pins that choice against a
        // regression to live-only iteration. (No restart here — the bounded
        // replay across recover is the previous test; this one only proves the
        // dropped tier is *reached*.)
        let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        assert!(
            engine.describe_live_tables().is_empty(),
            "the catalog no longer resolves the dropped name",
        );

        engine.flush().expect("flush drives the dropped tier");
        let st = engine
            .tables
            .get("account")
            .expect("the dropped table's tier stays resident");
        assert!(
            !st.engine.segment_names().is_empty(),
            "the dropped table's retained tier was flushed",
        );
        assert!(
            st.engine.replay_floor() > LogOffset::ZERO,
            "and its recovery floor advanced off the origin",
        );
    }

    #[test]
    fn execute_routes_checkpoint_and_flush_admin_commands() {
        // STL-219: the SQL admin commands route through `execute` to the same
        // session-wide durability ops, returning the wire `CommandComplete` tag.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        // CHECKPOINT fences without sealing.
        let outcome = engine
            .execute(&parse_one("CHECKPOINT"))
            .expect("checkpoint");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "CHECKPOINT" });
        let st = engine.tables.get("account").expect("tier resident");
        assert!(
            st.engine.durable_fence().is_some(),
            "CHECKPOINT fenced the WAL"
        );
        assert!(
            st.engine.segment_names().is_empty(),
            "CHECKPOINT seals no segment",
        );

        // FLUSH seals the delta into a segment and advances the recovery floor.
        let outcome = engine.execute(&parse_one("FLUSH")).expect("flush");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "FLUSH" });
        let st = engine.tables.get("account").expect("tier resident");
        assert!(
            !st.engine.segment_names().is_empty(),
            "FLUSH sealed a segment",
        );
        assert!(
            st.engine.replay_floor() > LogOffset::ZERO,
            "FLUSH advanced the recovery floor off the origin",
        );
    }

    // --- user DDL + durable user store (STL-252) ----------------------------

    #[test]
    fn user_ddl_routes_through_execute_with_postgres_tags() {
        let mut engine = session();
        let outcome = engine
            .execute(&parse_one("CREATE USER alice PASSWORD 's3cret'"))
            .expect("create user");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "CREATE ROLE" });
        assert!(engine.auth_verifier("alice").is_some());
        assert_eq!(engine.user_count(), 1);

        let outcome = engine
            .execute(&parse_one("ALTER USER alice PASSWORD 'rotated'"))
            .expect("alter user");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "ALTER ROLE" });

        let outcome = engine
            .execute(&parse_one("DROP USER alice"))
            .expect("drop user");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "DROP ROLE" });
        assert!(engine.auth_verifier("alice").is_none());
        assert_eq!(engine.user_count(), 0);

        // IF EXISTS on an absent user is a tagged no-op, not an error.
        let outcome = engine
            .execute(&parse_one("DROP USER IF EXISTS alice"))
            .expect("drop if exists");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "DROP ROLE" });
    }

    #[test]
    fn duplicate_and_unknown_users_are_refused() {
        let mut engine = session();
        engine
            .execute(&parse_one("CREATE USER alice PASSWORD 'pw'"))
            .expect("create user");
        assert!(matches!(
            engine.execute(&parse_one("CREATE USER alice PASSWORD 'other'")),
            Err(EngineError::DuplicateUser(name)) if name == "alice"
        ));
        assert!(matches!(
            engine.execute(&parse_one("ALTER USER ghost PASSWORD 'pw'")),
            Err(EngineError::UnknownUser(name)) if name == "ghost"
        ));
        assert!(matches!(
            engine.execute(&parse_one("DROP USER ghost")),
            Err(EngineError::UnknownUser(name)) if name == "ghost"
        ));
        // The refused statements left the store untouched.
        assert_eq!(engine.user_count(), 1);
    }

    #[test]
    fn verifiers_survive_recovery() {
        // The DoD's restart half: CREATE USER on one session, recover a fresh
        // session from the same disk, and the verifier — the exact key
        // material — is back. A dropped user stays dropped.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine
            .execute(&parse_one("CREATE USER alice PASSWORD 's3cret'"))
            .expect("create alice");
        engine
            .execute(&parse_one("CREATE USER bob PASSWORD 'hunter2'"))
            .expect("create bob");
        engine
            .execute(&parse_one("ALTER USER alice PASSWORD 'rotated'"))
            .expect("rotate alice");
        engine
            .execute(&parse_one("DROP USER bob"))
            .expect("drop bob");
        let live = engine.auth_verifier("alice").expect("alice exists");
        drop(engine);

        let recovered = SessionEngine::recover(disk, ZeroClock).expect("recover");
        assert_eq!(recovered.user_count(), 1);
        let verifier = recovered.auth_verifier("alice").expect("alice recovered");
        assert_eq!(verifier, live, "recovered verifier is byte-identical");
        assert!(
            recovered.auth_verifier("bob").is_none(),
            "bob stays dropped"
        );

        // The recovered verifier authenticates the post-rotation password and
        // refuses the original — proof the *latest* record won.
        let msg = b"n=alice,r=cnonce,r=cnoncesnonce,s=salt,i=4096,c=biws,r=cnoncesnonce";
        let good =
            stele_common::scram::client_proof("rotated", &verifier.salt, verifier.iterations, msg);
        let stale =
            stele_common::scram::client_proof("s3cret", &verifier.salt, verifier.iterations, msg);
        assert!(verifier.verify_client_proof(msg, &good));
        assert!(!verifier.verify_client_proof(msg, &stale));
    }

    #[test]
    fn fresh_salts_make_equal_passwords_distinct() {
        // Two users with the same password — and a rotation back to the same
        // password — must never share salt or key material (no cross-user or
        // cross-rotation correlation).
        let mut engine = session();
        engine
            .execute(&parse_one("CREATE USER a PASSWORD 'same'"))
            .expect("create a");
        engine
            .execute(&parse_one("CREATE USER b PASSWORD 'same'"))
            .expect("create b");
        let a = engine.auth_verifier("a").expect("a");
        let b = engine.auth_verifier("b").expect("b");
        assert_ne!(a.salt, b.salt, "fresh salt per user");
        assert_ne!(a.stored_key, b.stored_key);

        engine
            .execute(&parse_one("ALTER USER a PASSWORD 'same'"))
            .expect("rotate a");
        let rotated = engine.auth_verifier("a").expect("a rotated");
        assert_ne!(rotated.salt, a.salt, "fresh salt per rotation");
    }

    #[test]
    fn execute_routes_compact_and_it_consolidates_segments() {
        // STL-231: COMPACT routes through `execute` like the other admin
        // commands, folds the staged delta in (the internal flush), merges the
        // accumulated segments into one, and the table reads identically after.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        engine.execute(&parse_one("FLUSH")).expect("flush");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 250)"))
            .expect("insert");
        engine.execute(&parse_one("FLUSH")).expect("flush");
        engine
            .execute(&parse_one("UPDATE account SET balance = 175 WHERE id = 1"))
            .expect("update staged in the delta");
        {
            let st = engine.tables.get("account").expect("tier resident");
            assert_eq!(st.engine.segment_names().len(), 2, "two flushed segments");
        }
        let before = sorted(select(&mut engine, "SELECT id, balance FROM account").rows);

        let outcome = engine.execute(&parse_one("COMPACT")).expect("compact");
        assert_eq!(outcome, StatementOutcome::Ddl { tag: "COMPACT" });
        let st = engine.tables.get("account").expect("tier resident");
        assert_eq!(
            st.engine.segment_names().len(),
            1,
            "COMPACT consolidated the segments (delta folded in via flush)",
        );
        let after = sorted(select(&mut engine, "SELECT id, balance FROM account").rows);
        assert_eq!(before, after, "the read surface is unchanged by COMPACT");
        assert_eq!(before, vec![vec![i4(1), i4(175)], vec![i4(2), i4(250)]]);
    }

    #[test]
    fn recovery_resolves_old_schema_versions_and_reuses_the_namespace() {
        // A dropped name re-created with different columns: post-restart, the
        // live read sees only the new era and an AS OF read inside the old era
        // resolves the *old* schema — neither duplicated nor orphaned, because
        // the re-create's catalog-log record carries the *same* namespace and
        // recovery reopens that one tier. The recovered session must answer
        // exactly as the live one did — and the live session no longer leaks the
        // dropped era's rows into the current read: the `DROP` closes the storage
        // rows alongside the catalog name ([STL-211]), and recovery replays those
        // closes from the tier's WAL. Both reads are captured live and compared
        // across the kill, with the corrected current read pinned below.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine
            .execute(&parse_one(
                "CREATE TABLE t (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 100)"))
            .expect("insert");
        let s1 = engine.clock.current();
        engine.execute(&parse_one("DROP TABLE t")).expect("drop");
        engine
            .execute(&parse_one(
                "CREATE TABLE t (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
            ))
            .expect("re-create");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (2, 5)"))
            .expect("insert into the new era");
        let now_sql = "SELECT id, amount FROM t";
        let as_of_sql = format!("SELECT id, balance FROM t FOR SYSTEM_TIME AS OF {}", s1.0);
        let live_now = sorted(select(&mut engine, now_sql).rows);
        let live_as_of = sorted(select(&mut engine, &as_of_sql).rows);
        assert_eq!(
            live_now,
            vec![vec![i4(2), i4(5)]],
            "the current read sees only the new era — the dropped era's row was closed by the DROP"
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, now_sql).rows),
            live_now,
            "the current read answers as the live session did"
        );
        assert_eq!(
            sorted(select(&mut engine, &as_of_sql).rows),
            live_as_of,
            "the dropped era reads under the old schema, as live"
        );
        assert_eq!(
            live_as_of,
            vec![vec![i4(1), i4(100)]],
            "the old era resolves the old schema and exactly its one row"
        );
        let state = engine.tables.get("t").expect("tier resident");
        assert_eq!(state.namespace, 0, "the re-create reused the namespace");
        assert_eq!(engine.next_namespace, 1, "no second namespace was burned");
    }

    // ---- DROP closes crash-atomic with the catalog drop record (STL-220) ----
    //
    // A DROP's catalog record is fsynced (ADR-0028) but its storage closes are
    // auto-commit WAL appends — durability-deferred. A crash after that fsync but
    // before the closes reach the tier WAL would recover the name dropped yet the
    // rows still open, re-opening the STL-211 leak on a later re-create. Recovery
    // re-derives the closes from the durable drop record, so a crash-window
    // restart converges to the same retired state as a clean kill.
    //
    // The crash is modelled by rewinding the dropped tier's namespace-0 files to
    // their pre-DROP bytes — the un-fsynced closes vanish — while leaving the
    // fsynced shared catalog log intact. stele-sim cannot depend on stele-engine,
    // so this session-level crash coverage is in-process (the STL-210 / STL-215
    // pattern), not a sim scenario.

    /// The fixed-width namespace prefix [`NamespacedDisk`] gives the first table.
    const NS0: &str = "t00000000000000000000-";

    /// Snapshot the bytes of every file under tier-namespace prefix `ns` — the
    /// durable image to roll back to when modelling a crash before a tier's
    /// auto-commit writes were fsynced ([STL-220]).
    fn snapshot_ns(disk: &MemDisk, ns: &str) -> Vec<(String, Vec<u8>)> {
        disk.list()
            .expect("list")
            .into_iter()
            .filter(|name| name.starts_with(ns))
            .map(|name| {
                let file = disk.open(&name).expect("open tier file");
                let len = usize::try_from(file.len()).expect("small file");
                let mut bytes = vec![0u8; len];
                file.read_at(0, &mut bytes).expect("read tier file");
                (name, bytes)
            })
            .collect()
    }

    /// Roll every tier-namespace-`ns` file back to `snapshot`, discarding anything
    /// appended since — the un-fsynced closes a crash would lose. Files created
    /// after the snapshot are removed; the fsynced shared catalog log is untouched.
    fn rewind_ns(disk: &MemDisk, ns: &str, snapshot: &[(String, Vec<u8>)]) {
        for name in disk.list().expect("list") {
            if name.starts_with(ns) {
                disk.remove(&name).expect("remove tier file");
            }
        }
        for (name, bytes) in snapshot {
            let mut file = disk.create(name).expect("recreate tier file");
            file.append(bytes).expect("rewrite tier file");
        }
    }

    #[test]
    fn recovery_re_derives_a_dropped_eras_closes_after_a_crash_in_the_commit_window() {
        // Drive a system-versioned table to the brink of DROP, returning the disk,
        // the pre-DROP image of its tier files, and a snapshot instant inside the
        // dropped era. ZeroClock is deterministic, so every run is byte-identical.
        let build = || -> (MemDisk, Vec<(String, Vec<u8>)>, SystemTimeMicros) {
            let disk = MemDisk::new();
            let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
            engine
                .execute(&parse_one(
                    "CREATE TABLE t (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
                ))
                .expect("create");
            for dml in [
                "INSERT INTO t VALUES (1, 100)",
                "INSERT INTO t VALUES (2, 7)",
            ] {
                engine.execute(&parse_one(dml)).expect("insert");
            }
            let s1 = engine.clock.current();
            let image = snapshot_ns(&disk, NS0);
            engine.execute(&parse_one("DROP TABLE t")).expect("drop");
            // Dropping the engine is the kill — no checkpoint, no flush.
            drop(engine);
            (disk, image, s1)
        };

        // After recovery, re-create the name on the reused tier and report what a
        // client observes: the current read (must not leak the dropped era), a
        // re-insert of a dropped business key (must not be refused as a duplicate),
        // the read after it, and the AS-OF-the-old-era read under the old schema.
        let observe = |disk: &MemDisk, s1: SystemTimeMicros| {
            let mut engine = recover_session(disk);
            engine
                .execute(&parse_one(
                    "CREATE TABLE t (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
                ))
                .expect("re-create");
            let leaked = sorted(select(&mut engine, "SELECT id, amount FROM t").rows);
            let reinsert = engine
                .execute(&parse_one("INSERT INTO t VALUES (1, 9)"))
                .is_ok();
            let after = sorted(select(&mut engine, "SELECT id, amount FROM t").rows);
            let as_of = format!("SELECT id, balance FROM t FOR SYSTEM_TIME AS OF {}", s1.0);
            let dropped_era = sorted(select(&mut engine, &as_of).rows);
            (leaked, reinsert, after, dropped_era)
        };

        // Clean kill: the closes reached the WAL (MemDisk retains them), so
        // recovery retires the era by replay — the re-derivation is a verified
        // no-op there. Crash: same run, but the tier's auto-commit closes never
        // became durable, so namespace 0 is rewound to its pre-DROP image.
        let (clean_disk, _, s1) = build();
        let (crash_disk, pre_drop, _) = build();
        rewind_ns(&crash_disk, NS0, &pre_drop);

        let clean = observe(&clean_disk, s1);
        let crash = observe(&crash_disk, s1);

        // The oracle: re-deriving the drop's closes from the durable catalog
        // record makes the crash-window restart identical to the clean kill.
        assert_eq!(
            crash, clean,
            "the crash-window recovery converges to the clean-kill recovery",
        );

        // And both retire the dropped era rather than leaking it.
        let (leaked, reinsert, after, dropped_era) = crash;
        assert!(
            leaked.is_empty(),
            "the reused tier shows no dropped-era row in the current read",
        );
        assert!(
            reinsert,
            "a business key the dropped era used re-inserts — its old version is closed",
        );
        assert_eq!(
            after,
            vec![vec![i4(1), i4(9)]],
            "only the new era's row is current"
        );
        assert_eq!(
            dropped_era,
            vec![vec![i4(1), i4(100)], vec![i4(2), i4(7)]],
            "AS OF inside the dropped era still resolves both rows under the old schema",
        );
    }

    #[test]
    fn recovering_an_empty_disk_is_a_fresh_session() {
        // The server boots through `recover` unconditionally, so a first boot
        // (no catalog log at all) must come up empty and fully usable.
        let disk = MemDisk::new();
        let mut engine = recover_session(&disk);
        assert!(engine.describe_live_tables().is_empty());
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        assert_eq!(
            select(&mut engine, "SELECT balance FROM account").rows,
            vec![vec![i4(100)]]
        );
    }

    #[test]
    fn recovery_composes_across_repeated_restarts() {
        // Lifecycle across two kills: writes and DDL issued *after* a recovery
        // (including a committed multi-statement transaction — the recovered
        // MVCC write index starts empty, and a post-restart snapshot must still
        // commit cleanly) survive the next recovery too, on distinct namespaces.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create account");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        drop(engine);

        let mut engine = recover_session(&disk);
        engine
            .execute(&parse_one(
                "CREATE TABLE ledger (id INT PRIMARY KEY, amount INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create ledger after restart");
        engine
            .execute(&parse_one("INSERT INTO ledger VALUES (1, 5)"))
            .expect("insert ledger");
        let mut txn = engine.begin();
        engine
            .stage_dml(
                &parse_one("UPDATE account SET balance = 111 WHERE id = 1"),
                &mut txn,
            )
            .expect("stage");
        engine
            .commit(txn)
            .expect("a post-restart snapshot commits cleanly");
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            select(&mut engine, "SELECT balance FROM account").rows,
            vec![vec![i4(111)]],
            "the post-restart transactional update survives the second restart"
        );
        assert_eq!(
            select(&mut engine, "SELECT amount FROM ledger").rows,
            vec![vec![i4(5)]],
            "the post-restart table survives the second restart"
        );
        assert_eq!(engine.tables["account"].namespace, 0);
        assert_eq!(engine.tables["ledger"].namespace, 1);
        assert_eq!(engine.next_namespace, 2);
    }

    #[test]
    fn recovery_positions_the_transaction_id_allocator_past_recovered_commits() {
        // Provenance distinctness across restarts: the recovered allocator must
        // start past every transaction id on disk — including a *close's*
        // provenance (the delete below), which no version row carries.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        let who = Principal::new(b"demo".to_vec());
        engine
            .insert(
                "account",
                BusinessKey::new(b"1".to_vec()),
                None,
                Some(b"100".to_vec()),
                0,
                TxnId(7),
                who.clone(),
            )
            .expect("insert");
        engine
            .delete("account", &BusinessKey::new(b"1".to_vec()), TxnId(9), who)
            .expect("delete");
        drop(engine);

        let engine = recover_session(&disk);
        assert_eq!(
            engine.next_txn, 10,
            "the next transaction id starts past the deleting close's id"
        );
    }

    #[test]
    fn a_committed_transaction_survives_a_restart() {
        // Crash-atomic group commit, the "all present" branch ([STL-192]): a
        // multi-statement COMMIT is group-committed (one fsynced WAL record), so
        // every buffered write is durable across a restart.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        let mut txn = engine.begin();
        for sql in [
            "INSERT INTO account VALUES (1, 100)",
            "INSERT INTO account VALUES (2, 200)",
            "INSERT INTO account VALUES (3, 300)",
        ] {
            engine.stage_dml(&parse_one(sql), &mut txn).expect("stage");
        }
        engine.commit(txn).expect("commit");
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, "SELECT id FROM account").rows),
            vec![vec![i4(1)], vec![i4(2)], vec![i4(3)]],
            "every committed write is durable across the restart",
        );
    }

    #[test]
    fn a_torn_group_commit_recovers_none_of_the_transaction() {
        // Crash-atomic group commit, the "none" branch ([STL-192]): if the single
        // group-commit WAL append fails, nothing the transaction wrote becomes
        // durable — recovery finds none of it, never a partial prefix. Group mode
        // buffers every write, so the commit's *only* append is the group-commit
        // record; failing it fails the whole transaction. `MemDisk` injects this as a
        // *clean* append failure — the fault fires before any byte is copied (no torn
        // record) — which also exercises the STL-295 live-session rollback below.
        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");

        let mut txn = engine.begin();
        for sql in [
            "INSERT INTO account VALUES (1, 100)",
            "INSERT INTO account VALUES (2, 200)",
            "INSERT INTO account VALUES (3, 300)",
        ] {
            engine.stage_dml(&parse_one(sql), &mut txn).expect("stage");
        }
        // Fail the one group-commit append — the transaction's sole durability write.
        faults.schedule(FaultOp::Append, io::ErrorKind::Other);
        let err = engine
            .commit(txn)
            .expect_err("the clean group-commit append failure aborts the commit");
        assert!(matches!(err, EngineError::Storage(_)), "got {err:?}");

        // The *live* session must already match what recovery will reconstruct: the
        // refused commit's buffered writes are rolled back in memory, so a SELECT on
        // the still-running engine shows none of them — not the applied-but-undurable
        // rows a restart would erase ([STL-295]). The append did not poison the WAL, so
        // the engine stays healthy and serves reads.
        assert!(
            !engine.is_poisoned(),
            "a clean append failure does not poison the session",
        );
        assert!(
            select(&mut engine, "SELECT id FROM account")
                .rows
                .is_empty(),
            "the refused commit leaves no rows live — the buffered writes were rolled back",
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert!(
            select(&mut engine, "SELECT id FROM account")
                .rows
                .is_empty(),
            "a failed group commit leaves none of the transaction's writes",
        );
    }

    #[test]
    fn a_failed_fsync_poisons_the_session_until_recovery() {
        // STL-217: a failed WAL fsync (here the checkpoint's group-commit tick) is a
        // crash, not a clean abort — the table's engine poisons and the session
        // surfaces it through `is_poisoned`. The session must then refuse further
        // writes and restart into recovery, which opens fresh, unpoisoned WALs and
        // still serves everything that committed before the failure.
        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        assert!(!engine.is_poisoned(), "healthy before the fault");

        // Fail the next fsync — the checkpoint's group-commit tick. (Scheduled
        // *after* the writes above, so it lands on the checkpoint, whatever the
        // single-statement path itself fsynced.)
        faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
        assert!(
            engine.checkpoint().is_err(),
            "the injected fsync fault fails the checkpoint",
        );
        assert!(engine.is_poisoned(), "a failed fsync poisons the session");

        // A poisoned session refuses further writes (the WAL append is refused),
        // even though the scheduled fault was already consumed.
        assert!(
            engine
                .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
                .is_err(),
            "a poisoned session refuses writes",
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert!(
            !engine.is_poisoned(),
            "recovery opens fresh, unpoisoned WALs"
        );
        assert_eq!(
            select(&mut engine, "SELECT id FROM account").rows,
            vec![vec![i4(1)]],
            "the write committed before the failed fsync survives; the refused one never landed",
        );
    }

    #[test]
    fn a_failed_catalog_log_append_rolls_the_ddl_back() {
        // Schedule the next file append to fail and run a CREATE: whichever
        // append the fault lands on (the tier's WAL or the catalog log's
        // record), the statement must fail atomically — no live table, no
        // durable record — and both a retry and a later recovery see a single,
        // consistent creation.
        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        faults.schedule(FaultOp::Append, io::ErrorKind::Other);
        engine
            .execute(&parse_one(CREATE))
            .expect_err("the injected append failure refuses the CREATE");
        assert!(
            engine.describe_live_tables().is_empty(),
            "the failed CREATE left no live table behind"
        );

        engine.execute(&parse_one(CREATE)).expect("retry");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            engine.describe_live_tables().len(),
            1,
            "exactly one account"
        );
        assert_eq!(
            select(&mut engine, "SELECT balance FROM account").rows,
            vec![vec![i4(100)]]
        );
    }

    #[test]
    fn recovery_tolerates_a_torn_catalog_log_tail() {
        // A kill mid-DDL-append leaves a partial frame at the log's tail. Its
        // fsync never returned — the statement was never acknowledged — so
        // recovery must ignore it and serve everything acknowledged before it.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        drop(engine);
        let mut file = disk
            .open(crate::catalog_log::CATALOG_LOG_FILENAME)
            .expect("open catalog log");
        file.append(b"STCG\x40\x00\x00\x00partial")
            .expect("torn tail");

        let mut engine = recover_session(&disk);
        assert_eq!(
            select(&mut engine, "SELECT balance FROM account").rows,
            vec![vec![i4(100)]],
            "the acknowledged history survives the torn, unacknowledged tail"
        );
    }

    #[test]
    fn recovery_reopens_a_valid_time_tier_under_its_policy() {
        // A bitemporal table round-trips: both-axes AS OF reads answer after
        // the restart exactly as live (the tier reopened with valid-time
        // framing), and the recovered policy still refuses a policy-changing
        // re-create — proving the flag came back from the catalog log, not
        // from a default.
        let disk = MemDisk::new();
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine
            .execute(&parse_one(
                "CREATE TABLE account (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        let who = || Principal::new(b"demo".to_vec());
        let key = || business_key(&ScalarValue::Int4(1));
        let payload = |balance: i32, from: i64, to: i64| {
            row_codec::encode_payload(&[
                cell(Some(ScalarValue::Int4(balance))),
                cell(Some(ScalarValue::Timestamp(from))),
                cell(Some(ScalarValue::Timestamp(to))),
            ])
        };
        let iv = |from: i64, to: i64| {
            ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed")
        };
        let c1 = engine
            .insert(
                "account",
                key(),
                Some(iv(10, 20)),
                payload(100, 10, 20),
                0,
                TxnId(1),
                who(),
            )
            .expect("insert")
            .commit;
        let c2 = engine
            .update(
                "account",
                key(),
                Some(iv(20, 30)),
                payload(250, 20, 30),
                0,
                TxnId(2),
                who(),
            )
            .expect("update")
            .commit;
        drop(engine);

        let mut engine = recover_session(&disk);
        let mut balance = |sys: i64, valid: i64| -> Option<Vec<u8>> {
            let sql = format!(
                "SELECT balance FROM account \
                 FOR SYSTEM_TIME AS OF {sys} FOR VALID_TIME AS OF {valid}"
            );
            select(&mut engine, &sql)
                .rows
                .into_iter()
                .next()
                .and_then(|row| row.into_iter().next().expect("projected cell"))
        };
        assert_eq!(balance(c1.0, 15), cell(Some(ScalarValue::Int4(100))));
        assert_eq!(balance(c2.0, 25), cell(Some(ScalarValue::Int4(250))));
        assert_eq!(balance(c2.0, 15), None, "superseded on the system axis");

        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        let err = engine
            .execute(&parse_one(
                "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            ))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ValidTimePolicyChange { .. }),
            "the recovered tier still enforces its valid-time policy, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Secondary-index substrate (STL-233)
    // -----------------------------------------------------------------------

    /// Execute one statement, panicking with the SQL on error.
    fn run_sql<C: Clock + Clone>(
        engine: &mut SessionEngine<C, MemDisk>,
        sql: &str,
    ) -> StatementOutcome {
        engine
            .execute(&parse_one(sql))
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// Execute one `SELECT` and return its raw row cells.
    fn query_rows<C: Clock + Clone>(
        engine: &mut SessionEngine<C, MemDisk>,
        sql: &str,
    ) -> Vec<Vec<Option<Vec<u8>>>> {
        let StatementOutcome::Rows(r) = run_sql(engine, sql) else {
            panic!("`{sql}` must return rows");
        };
        r.rows
    }

    /// One single-cell `Int4` row, in the canonical encoding `SelectResult` carries.
    fn int_row(v: i32) -> Vec<Option<Vec<u8>>> {
        vec![cell(Some(ScalarValue::Int4(v)))]
    }

    /// The address of a [`Column::Bytes`]' first cell — its shared-buffer identity.
    /// Two columns sharing one [`Cells`](stele_exec::Cells) allocation (a clone, not
    /// a copy) report the same address; a fresh per-cell copy would not.
    fn bytes_addr(column: &Column) -> *const Option<Vec<u8>> {
        match column {
            Column::Bytes(cells) => cells.as_ptr(),
            Column::I64(_) => panic!("a materialized relation column is always Bytes"),
        }
    }

    #[test]
    fn cte_join_reference_clones_share_buffers_not_cells() {
        // A materialized relation is held columnar over shared `Cells` buffers; a
        // join side (`join_side_columns`) reads it by cloning those columns — an
        // `Arc` refcount bump, not a per-cell copy — so a CTE joined N times never
        // re-copies its rows ([STL-321]).
        let rows = vec![
            vec![Some(b"k0".to_vec()), Some(b"v0".to_vec())],
            vec![Some(b"k1".to_vec()), None],
            vec![Some(b"k2".to_vec()), Some(b"v2".to_vec())],
        ];
        let relation = MaterializedRelation::from_rows(rows, 2);
        assert_eq!(relation.row_count, 3);
        assert_eq!(relation.columns.len(), 2);

        // Every reference clones the stored columns; each clone shares the exact
        // buffer the relation holds, however many references there are.
        let refs: Vec<Vec<Column>> = (0..4).map(|_| relation.columns.clone()).collect();
        for (i, stored) in relation.columns.iter().enumerate() {
            for reference in &refs {
                assert_eq!(
                    bytes_addr(stored),
                    bytes_addr(&reference[i]),
                    "a join-side reference shares the relation's buffer, not a copy"
                );
            }
        }
        // The shared buffers still read the right cells (NULL preserved distinct
        // from an empty value).
        assert_eq!(relation_cell(&refs[0][0], 1), Some(b"k1".to_vec()));
        assert_eq!(relation_cell(&refs[0][1], 1), None);
        assert_eq!(relation_cell(&refs[3][1], 2), Some(b"v2".to_vec()));
    }

    #[test]
    fn cte_empty_relation_keeps_its_column_complement() {
        // An empty CTE still carries its full column complement (zero-length
        // columns), so a join side's row count `columns[0].len()` is well-defined.
        let relation = MaterializedRelation::from_rows(Vec::new(), 2);
        assert_eq!(relation.row_count, 0);
        assert_eq!(relation.columns.len(), 2);
        assert!(relation.columns.iter().all(Column::is_empty));
        // A `KeepAll` / `Empty` selection over zero rows is empty either way.
        let cols = vec![("a".to_owned(), LogicalType::Int4)];
        assert!(
            relation_selection(&FilterPlan::KeepAll, &cols, &relation)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn cte_read_filters_over_shared_columns() {
        // The read path filters a CTE reference via a selection vector over the
        // shared columns and gathers only the survivors — the same answer the old
        // clone-then-filter row-major path gave ([STL-321]). The wire suite and the
        // `cte_differential` oracle guard the broader no-behavior-change contract.
        let mut engine = session();
        run_sql(&mut engine, CREATE);
        for (id, bal) in [(1, 100), (2, 200), (3, 300)] {
            run_sql(
                &mut engine,
                &format!("INSERT INTO account (id, balance) VALUES ({id}, {bal})"),
            );
        }
        // A `WHERE` over a reference keeps only the matching rows.
        assert_eq!(
            query_rows(
                &mut engine,
                "WITH c AS (SELECT id, balance FROM account) SELECT id FROM c WHERE balance >= 200"
            ),
            vec![int_row(2), int_row(3)]
        );
        // A passthrough reference returns every row, in business-key order.
        assert_eq!(
            query_rows(
                &mut engine,
                "WITH c AS (SELECT id, balance FROM account) SELECT id FROM c"
            ),
            vec![int_row(1), int_row(2), int_row(3)]
        );
        // The same CTE referenced twice in one statement (a self-join) answers
        // consistently — each side a zero-copy clone of the one materialization.
        assert_eq!(
            query_rows(
                &mut engine,
                "WITH c AS (SELECT id, balance FROM account) \
                 SELECT a.id FROM c AS a JOIN c AS b ON a.balance = b.balance WHERE a.id >= 2"
            ),
            vec![int_row(2), int_row(3)]
        );
    }

    #[test]
    fn row_source_relation_matches_row_major_through_the_tail_api() {
        // The columnar CTE read path ([STL-338]) must be indistinguishable from the
        // row-major path through the shared tail's accessor API: a
        // `RowSource::Relation` over a relation + `WHERE` selection answers
        // `row_count` / `cell` / `column` / `into_rows` exactly as a `RowSource::Rows`
        // over the same surviving rows would. This is the per-cell
        // behavior-preservation the wire suite and the `cte_differential` oracle guard
        // end-to-end.
        let stored = vec![
            vec![Some(b"k0".to_vec()), Some(b"v0".to_vec())],
            vec![Some(b"k1".to_vec()), None],
            vec![Some(b"k2".to_vec()), Some(b"v2".to_vec())],
            vec![Some(b"k3".to_vec()), Some(b"v3".to_vec())],
        ];
        let relation = MaterializedRelation::from_rows(stored.clone(), 2);
        // A `WHERE` that kept rows 1 and 3 — an ordered subset, as a real selection is.
        let selection = vec![1usize, 3];
        let columnar = RowSource::Relation {
            relation: &relation,
            selection: selection.clone(),
        };
        // The equivalent row-major source: just the surviving rows, gathered.
        let row_major = RowSource::Rows(selection.iter().map(|&r| stored[r].clone()).collect());

        assert_eq!(columnar.row_count(), 2);
        assert_eq!(columnar.row_count(), row_major.row_count());
        // Every `(row, col)` cell agrees — including the NULL in row 1's value column,
        // kept distinct from an empty value.
        for r in 0..columnar.row_count() {
            for c in 0..2 {
                assert_eq!(columnar.cell(r, c), row_major.cell(r, c));
            }
        }
        // A whole-column decode (the `shape_rows` / `run_aggregate` step) agrees.
        assert_eq!(
            columnar.column(0),
            vec![Some(b"k1".to_vec()), Some(b"k3".to_vec())]
        );
        assert_eq!(columnar.column(0), row_major.column(0));
        assert_eq!(columnar.column(1), vec![None, Some(b"v3".to_vec())]);
        assert_eq!(columnar.column(1), row_major.column(1));
        // An out-of-range column reads as SQL NULL, not a panic (the defensive decode).
        assert_eq!(columnar.cell(0, 9), None);
        assert_eq!(columnar.column(9), vec![None, None]);
        // The slow-path materialization yields exactly the surviving rows.
        assert_eq!(columnar.into_rows(), row_major.into_rows());
    }

    #[test]
    fn cte_passthrough_read_shapes_and_projects_off_shared_columns() {
        // The shared shaping tail now runs over a CTE reference's shared columns via a
        // selection vector ([STL-338]): `shape_rows` decodes only the ORDER BY /
        // DISTINCT columns off the buffers, and a passthrough / projected read gathers
        // only its output cells — no full-width row-major intermediate. The answers
        // match the row-major path a base table takes (the `cte_differential` oracle
        // is the broad witness).
        let mut engine = session();
        run_sql(&mut engine, CREATE);
        for (id, bal) in [(1, 300), (2, 100), (3, 300), (4, 200)] {
            run_sql(
                &mut engine,
                &format!("INSERT INTO account (id, balance) VALUES ({id}, {bal})"),
            );
        }
        // ORDER BY an *unprojected* column + LIMIT over a passthrough reference:
        // shaping decodes `balance` straight off the shared column, the projection
        // emits only `id`.
        assert_eq!(
            query_rows(
                &mut engine,
                "WITH c AS (SELECT id, balance FROM account) \
                 SELECT id FROM c ORDER BY balance DESC, id ASC LIMIT 2"
            ),
            vec![int_row(1), int_row(3)]
        );
        // DISTINCT over a projected column dedups off the shared buffer.
        assert_eq!(
            query_rows(
                &mut engine,
                "WITH c AS (SELECT id, balance FROM account) \
                 SELECT DISTINCT balance FROM c ORDER BY balance"
            ),
            vec![int_row(100), int_row(200), int_row(300)]
        );
        // An aggregate over a CTE reads its grouping / argument columns straight off
        // the shared buffers ([STL-338]) through the same `RowSource`.
        assert_eq!(
            query_rows(
                &mut engine,
                "WITH c AS (SELECT id, balance FROM account) \
                 SELECT balance, COUNT(*) FROM c GROUP BY balance ORDER BY balance"
            ),
            vec![
                vec![
                    cell(Some(ScalarValue::Int4(100))),
                    cell(Some(ScalarValue::Int8(1)))
                ],
                vec![
                    cell(Some(ScalarValue::Int4(200))),
                    cell(Some(ScalarValue::Int8(1)))
                ],
                vec![
                    cell(Some(ScalarValue::Int4(300))),
                    cell(Some(ScalarValue::Int8(2)))
                ],
            ]
        );
    }

    #[test]
    fn index_probes_serve_equality_reads_across_dml() {
        let mut engine = session();
        run_sql(&mut engine, CREATE);
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (1, 100)",
        );
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (2, 200)",
        );
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (3, 100)",
        );

        let outcome = run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");
        assert!(matches!(
            outcome,
            StatementOutcome::Ddl {
                tag: "CREATE INDEX"
            }
        ));
        assert_eq!(engine.index_probe_count(), 0, "DDL itself probes nothing");

        // The build covered the pre-existing rows.
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            vec![int_row(1), int_row(3)]
        );
        assert_eq!(engine.index_probe_count(), 1, "the equality read probed");

        // Maintenance keeps the structure current across UPDATE and DELETE —
        // and the superset posture (old entries linger) never leaks a stale
        // row, because the exact filter re-applies.
        run_sql(&mut engine, "UPDATE account SET balance = 300 WHERE id = 1");
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            vec![int_row(3)]
        );
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 300"),
            vec![int_row(1)]
        );
        run_sql(&mut engine, "DELETE FROM account WHERE id = 3");
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            Vec::<Vec<Option<Vec<u8>>>>::new()
        );
        // A value never written probes `Empty` and skips the scan outright.
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 999"),
            Vec::<Vec<Option<Vec<u8>>>>::new()
        );
        assert_eq!(engine.index_probe_count(), 5);

        // A `<>` on the indexed column (no window covers a complement), and
        // any read on an unindexed column, never probe.
        let probes = engine.index_probe_count();
        let _ = query_rows(&mut engine, "SELECT id FROM account WHERE balance <> 100");
        let _ = query_rows(&mut engine, "SELECT balance FROM account WHERE id = 2");
        assert_eq!(engine.index_probe_count(), probes);
    }

    #[test]
    fn range_probes_serve_comparison_reads_across_signs() {
        // The ordered structure's range service ([STL-237]): one-sided
        // comparisons on the indexed column probe a candidate window walked in
        // *typed* order — the negative balance must sort below the positives
        // (the raw little-endian cell bytes would sort it above them all).
        let mut engine = session();
        run_sql(&mut engine, CREATE);
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (1, -50)",
        );
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (2, 100)",
        );
        run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");
        // Maintenance notes post-build writes through the same transform.
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (3, 200)",
        );

        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance < 0"),
            vec![int_row(1)]
        );
        assert_eq!(engine.index_probe_count(), 1, "the range read probed");
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance >= 100"),
            vec![int_row(2), int_row(3)]
        );
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance <= -50"),
            vec![int_row(1)]
        );
        // A literal-first comparison mirrors to the same probe: `150 < balance`
        // is `balance > 150`.
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE 150 < balance"),
            vec![int_row(3)]
        );
        // A range beyond every noted cell probes `Empty` and skips the scan.
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance > 200"),
            Vec::<Vec<Option<Vec<u8>>>>::new()
        );
        assert_eq!(engine.index_probe_count(), 5);
    }

    #[test]
    fn as_of_before_the_index_floor_full_scans_and_stays_correct() {
        let clock = SteppedClock::new(1_000_000_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        run_sql(&mut engine, CREATE);
        clock.set(1_010_000_000);
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (1, 100)",
        );
        clock.set(1_020_000_000);
        run_sql(&mut engine, "UPDATE account SET balance = 200 WHERE id = 1");
        clock.set(1_030_000_000);
        run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");

        // An `AS OF` inside pre-index history must not probe — the build never
        // saw the superseded version carrying 100 — and still answers exactly.
        // The same floor gate covers range probes ([STL-237]).
        assert_eq!(
            query_rows(
                &mut engine,
                "SELECT id FROM account FOR SYSTEM_TIME AS OF 1015000000 WHERE balance = 100"
            ),
            vec![int_row(1)]
        );
        assert_eq!(
            query_rows(
                &mut engine,
                "SELECT id FROM account FOR SYSTEM_TIME AS OF 1015000000 WHERE balance < 150"
            ),
            vec![int_row(1)]
        );
        assert_eq!(engine.index_probe_count(), 0, "pre-floor reads full-scan");

        // At-or-after the floor the probe serves: 100 was superseded before the
        // build, so the structure proves emptiness; 200 resolves through it —
        // by equality and by range alike.
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            Vec::<Vec<Option<Vec<u8>>>>::new()
        );
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 200"),
            vec![int_row(1)]
        );
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance >= 150"),
            vec![int_row(1)]
        );
        assert_eq!(engine.index_probe_count(), 3);
    }

    #[test]
    fn a_usable_same_column_sibling_serves_when_a_newer_index_cannot() {
        // Two live indexes on one column with different floors: the read
        // snapshot predates the (name-earlier) newer index's floor, so the
        // older sibling must serve the probe rather than the first name-order
        // match vetoing it.
        let clock = SteppedClock::new(1_000_000_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        run_sql(&mut engine, CREATE);
        clock.set(1_010_000_000);
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (1, 100)",
        );
        clock.set(1_020_000_000);
        run_sql(&mut engine, "CREATE INDEX z_old ON account (balance)");
        // A snapshot between the two creations…
        clock.set(1_030_000_000);
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (2, 200)",
        );
        let between = engine.commit_clock().0;
        // …then a second index on the same column, named *before* the first.
        clock.set(1_040_000_000);
        run_sql(&mut engine, "CREATE INDEX a_new ON account (balance)");

        assert_eq!(
            query_rows(
                &mut engine,
                &format!(
                    "SELECT id FROM account FOR SYSTEM_TIME AS OF {between} WHERE balance = 100"
                )
            ),
            vec![int_row(1)]
        );
        assert_eq!(
            engine.index_probe_count(),
            1,
            "the older same-column index serves the pre-floor-of-the-newer read"
        );
    }

    // ---- predicate-driven UPDATE / DELETE (STL-229) ----

    /// Run a DML statement and return its summary, naming the statement on
    /// failure (the seeded oracle runs many).
    fn dml(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> DmlSummary {
        match engine.execute(&parse_one(sql)) {
            Ok(StatementOutcome::Dml(summary)) => summary,
            Ok(_) => panic!("DML {sql:?} must return a summary"),
            Err(e) => panic!("DML {sql:?} failed: {e:?}"),
        }
    }

    #[test]
    fn predicate_update_affects_exactly_the_matching_rows() {
        // A value-column predicate selects rows the v0.1 key-equality path never
        // could; the scan-then-write plan must touch exactly those and report
        // their count.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_WIDE)).expect("create");
        for sql in [
            "INSERT INTO t VALUES (1, 10, 'keep')",
            "INSERT INTO t VALUES (2, 30, 'hit')",
            "INSERT INTO t VALUES (3, 40, 'hit')",
            "INSERT INTO t VALUES (4, 20, 'keep')",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        assert_eq!(
            dml(&mut engine, "UPDATE t SET b = 'zapped' WHERE a > 20"),
            DmlSummary::Update(2),
            "the tag counts the matched live rows at the snapshot"
        );
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM t").rows),
            sorted(vec![
                vec![i4(1), i4(10), txt("keep")],
                vec![i4(2), i4(30), txt("zapped")],
                vec![i4(3), i4(40), txt("zapped")],
                vec![i4(4), i4(20), txt("keep")],
            ]),
            "exactly the matching rows changed; column a kept its value (RMW)"
        );

        assert_eq!(
            dml(&mut engine, "DELETE FROM t WHERE b = 'zapped'"),
            DmlSummary::Delete(2)
        );
        assert_eq!(
            sorted(select(&mut engine, "SELECT id FROM t").rows),
            sorted(vec![vec![i4(1)], vec![i4(4)]]),
            "exactly the matching rows were deleted"
        );
    }

    #[test]
    fn whole_table_update_and_delete_affect_every_live_row() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account VALUES (1, 100)",
            "INSERT INTO account VALUES (2, 200)",
            "INSERT INTO account VALUES (3, 300)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        assert_eq!(
            dml(&mut engine, "UPDATE account SET balance = 0"),
            DmlSummary::Update(3),
            "no WHERE matches every live row"
        );
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            sorted(vec![
                vec![i4(1), i4(0)],
                vec![i4(2), i4(0)],
                vec![i4(3), i4(0)],
            ])
        );

        assert_eq!(
            dml(&mut engine, "DELETE FROM account"),
            DmlSummary::Delete(3)
        );
        assert!(
            select(&mut engine, "SELECT id FROM account")
                .rows
                .is_empty(),
            "a whole-table DELETE closes every live row"
        );
    }

    #[test]
    fn predicate_dml_reports_zero_matches_and_changes_nothing() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        assert_eq!(
            dml(
                &mut engine,
                "UPDATE account SET balance = 0 WHERE balance > 500"
            ),
            DmlSummary::Update(0)
        );
        assert_eq!(
            dml(&mut engine, "DELETE FROM account WHERE balance > 500"),
            DmlSummary::Delete(0)
        );
        assert_eq!(
            select(&mut engine, "SELECT * FROM account").rows,
            vec![vec![i4(1), i4(100)]],
            "an empty matched set writes nothing"
        );
    }

    #[test]
    fn point_dml_on_an_absent_key_reports_zero_rows() {
        // STL-294: a key-equality WHERE on an absent key is a 0-row no-op
        // (Postgres `UPDATE 0` / `DELETE 0`) on the auto-commit point fast path,
        // not the storage writers' `KeyNotFound`.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        assert_eq!(
            dml(&mut engine, "UPDATE account SET balance = 0 WHERE id = 99"),
            DmlSummary::Update(0)
        );
        assert_eq!(
            dml(&mut engine, "DELETE FROM account WHERE id = 99"),
            DmlSummary::Delete(0)
        );
        // Reversed operand order is still the point fast path (STL-229).
        assert_eq!(
            dml(&mut engine, "DELETE FROM account WHERE 99 = id"),
            DmlSummary::Delete(0)
        );
        assert_eq!(
            select(&mut engine, "SELECT * FROM account").rows,
            vec![vec![i4(1), i4(100)]],
            "an absent-key point write changes nothing"
        );

        // A live key still acts — the fast path is unchanged for a present row.
        assert_eq!(
            dml(&mut engine, "UPDATE account SET balance = 5 WHERE id = 1"),
            DmlSummary::Update(1)
        );
        assert_eq!(
            dml(&mut engine, "DELETE FROM account WHERE id = 1"),
            DmlSummary::Delete(1)
        );
        // Now that key is absent too, so a repeat point delete is also a no-op.
        assert_eq!(
            dml(&mut engine, "DELETE FROM account WHERE id = 1"),
            DmlSummary::Delete(0)
        );
    }

    #[test]
    fn an_absent_key_point_write_in_a_transaction_is_zero_and_commits_siblings() {
        // STL-294 in-transaction: the staged tag is exact (0 for an absent key,
        // via the read-your-own-writes probe), the COMMIT is not aborted by it,
        // and the block's other writes still land.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        let mut txn = engine.begin();
        // A sibling write that must survive the block.
        assert_eq!(
            engine
                .stage_dml(&parse_one("INSERT INTO account VALUES (2, 200)"), &mut txn)
                .expect("stage insert"),
            Some(DmlSummary::Insert(1))
        );
        // Absent key → staged tag 0, nothing buffered.
        assert_eq!(
            engine
                .stage_dml(
                    &parse_one("UPDATE account SET balance = 0 WHERE id = 99"),
                    &mut txn
                )
                .expect("stage update"),
            Some(DmlSummary::Update(0))
        );
        assert_eq!(
            engine
                .stage_dml(&parse_one("DELETE FROM account WHERE id = 99"), &mut txn)
                .expect("stage delete"),
            Some(DmlSummary::Delete(0))
        );
        // Read-your-own-writes: the key inserted earlier in the block is live, so
        // a point UPDATE of it reports 1 and buffers (the probe sees the overlay).
        assert_eq!(
            engine
                .stage_dml(
                    &parse_one("UPDATE account SET balance = 222 WHERE id = 2"),
                    &mut txn
                )
                .expect("stage update of buffered insert"),
            Some(DmlSummary::Update(1))
        );
        engine
            .commit(txn)
            .expect("commit despite the absent-key no-ops");

        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            sorted(vec![vec![i4(1), i4(100)], vec![i4(2), i4(222)]]),
            "the absent-key writes were no-ops; the block's real writes committed"
        );
    }

    #[test]
    fn a_committed_transaction_maintains_the_index_and_ryow_reads_never_probe() {
        let mut engine = session();
        run_sql(&mut engine, CREATE);
        run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");

        let mut txn = engine.begin();
        engine
            .execute_in_txn(
                &parse_one("INSERT INTO account (id, balance) VALUES (1, 100)"),
                &mut txn,
            )
            .expect("stage insert");
        // The buffered row is visible to the transaction (read-your-own-writes)
        // but is not committed, so the read takes the overlay path — no probe.
        let StatementOutcome::Rows(r) = engine
            .execute_in_txn(
                &parse_one("SELECT id FROM account WHERE balance = 100"),
                &mut txn,
            )
            .expect("ryow select")
        else {
            panic!("rows");
        };
        assert_eq!(r.rows, vec![int_row(1)]);
        assert_eq!(engine.index_probe_count(), 0, "overlaid reads never probe");

        // Commit funnels through the same apply path as auto-commit, so the
        // index now covers the transaction's writes.
        engine.commit(txn).expect("commit");
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            vec![int_row(1)]
        );
        assert_eq!(engine.index_probe_count(), 1);
    }

    #[test]
    fn index_ddl_round_trips_catalog_persistence_and_cold_boot_rebuild() {
        let disk = MemDisk::new();
        {
            let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
            run_sql(&mut engine, CREATE);
            run_sql(
                &mut engine,
                "INSERT INTO account (id, balance) VALUES (1, 100)",
            );
            run_sql(
                &mut engine,
                "INSERT INTO account (id, balance) VALUES (2, 200)",
            );
            // Seal part of the history so the rebuild reads sealed + delta.
            engine.flush().expect("flush");
            run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");
            run_sql(
                &mut engine,
                "INSERT INTO account (id, balance) VALUES (3, 100)",
            );
            // The engine is dropped here — the crash/restart boundary.
        }

        let mut engine = recover_session(&disk);
        assert_eq!(engine.index_probe_count(), 0);
        // The rebuilt structure serves probes over both pre-flush (sealed) and
        // post-index (maintained, WAL-replayed) rows.
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            vec![int_row(1), int_row(3)]
        );
        assert_eq!(engine.index_probe_count(), 1, "the recovered index serves");

        // …and post-recovery writes keep maintaining it.
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (4, 100)",
        );
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            vec![int_row(1), int_row(3), int_row(4)]
        );
    }

    #[test]
    fn predicate_dml_leaves_pre_statement_history_readable() {
        // The append-only contract under a bulk write: a whole-table UPDATE
        // closes and rewrites every row, but an `AS OF` read pinned before the
        // statement still answers from the pre-statement versions. With the
        // synthetic clock the commits land at sys_from 1 (CREATE), 2, 3 (the
        // two INSERT rows), so `AS OF 3` is the instant just before the bulk
        // write.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (2, 200)"))
            .expect("insert");

        assert_eq!(
            dml(&mut engine, "UPDATE account SET balance = 0"),
            DmlSummary::Update(2)
        );

        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account FOR SYSTEM_TIME AS OF 3").rows),
            sorted(vec![vec![i4(1), i4(100)], vec![i4(2), i4(200)]]),
            "the pre-statement snapshot still reads the original values"
        );
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            sorted(vec![vec![i4(1), i4(0)], vec![i4(2), i4(0)]]),
            "the current snapshot reads the bulk update"
        );
    }

    #[test]
    fn predicate_dml_in_a_transaction_sees_buffered_writes() {
        // Read-your-own-writes at statement time ([STL-203] × [STL-229]): an
        // INSERT buffered earlier in the block is matchable by a later predicate
        // UPDATE, the tag counts it, and the block's SELECT sees the combined
        // effect — all before anything commits.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (2, 200)"), &mut txn)
            .expect("stage insert");
        let summary = engine
            .stage_dml(&parse_one("UPDATE account SET balance = 0"), &mut txn)
            .expect("stage scan update")
            .expect("dml summary");
        assert_eq!(
            summary,
            DmlSummary::Update(2),
            "the buffered INSERT joins the committed row in the matched set"
        );

        let StatementOutcome::Rows(inside) = engine
            .execute_in_txn(&parse_one("SELECT * FROM account"), &mut txn)
            .expect("select in txn")
        else {
            panic!("rows");
        };
        assert_eq!(
            sorted(inside.rows),
            sorted(vec![vec![i4(1), i4(0)], vec![i4(2), i4(0)]]),
            "the block reads its own bulk write"
        );

        engine.commit(txn).expect("commit");
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            sorted(vec![vec![i4(1), i4(0)], vec![i4(2), i4(0)]]),
            "the combined effect is durable after COMMIT"
        );
    }

    #[test]
    fn dropped_indexes_stay_dropped_across_recovery() {
        let disk = MemDisk::new();
        {
            let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
            run_sql(&mut engine, CREATE);
            run_sql(
                &mut engine,
                "INSERT INTO account (id, balance) VALUES (1, 100)",
            );
            run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");
            let outcome = run_sql(&mut engine, "DROP INDEX i_balance");
            assert!(matches!(
                outcome,
                StatementOutcome::Ddl { tag: "DROP INDEX" }
            ));
            // Equality reads fall back to full scans, exactly.
            assert_eq!(
                query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
                vec![int_row(1)]
            );
            assert_eq!(engine.index_probe_count(), 0, "no live index, no probe");
        }

        let mut engine = recover_session(&disk);
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
            vec![int_row(1)]
        );
        assert_eq!(engine.index_probe_count(), 0, "the drop replayed");
    }

    #[test]
    fn drop_table_cascades_indexes_in_session_and_across_recovery() {
        let disk = MemDisk::new();
        {
            let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
            run_sql(&mut engine, CREATE);
            run_sql(
                &mut engine,
                "INSERT INTO account (id, balance) VALUES (1, 100)",
            );
            run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");
            run_sql(&mut engine, "DROP TABLE account");
            // The cascade freed the name; the re-created table starts
            // index-free and its reads never probe.
            run_sql(&mut engine, CREATE);
            assert_eq!(
                query_rows(&mut engine, "SELECT id FROM account WHERE balance = 100"),
                Vec::<Vec<Option<Vec<u8>>>>::new()
            );
            assert_eq!(engine.index_probe_count(), 0);
            // …and the index name is reusable on the fresh era.
            run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");
        }

        // Replay re-derives the same cascade: create → drop(cascade) →
        // re-create → fresh index. Recovery rebuilds only the live one.
        let mut engine = recover_session(&disk);
        run_sql(
            &mut engine,
            "INSERT INTO account (id, balance) VALUES (7, 700)",
        );
        assert_eq!(
            query_rows(&mut engine, "SELECT id FROM account WHERE balance = 700"),
            vec![int_row(7)]
        );
        assert_eq!(engine.index_probe_count(), 1);
    }

    #[test]
    fn index_ddl_misuse_is_refused_over_execute() {
        let mut engine = session();
        run_sql(&mut engine, CREATE);
        run_sql(&mut engine, "CREATE INDEX i_balance ON account (balance)");

        // Duplicate name — one namespace across the live set.
        let err = engine
            .execute(&parse_one("CREATE INDEX i_balance ON account (balance)"))
            .expect_err("duplicate index name");
        assert!(
            matches!(
                err,
                EngineError::Catalog(CatalogError::IndexAlreadyExists(_))
            ),
            "got {err:?}"
        );
        // Unknown table / column, and the always-indexed business key.
        let err = engine
            .execute(&parse_one("CREATE INDEX i ON ghost (balance)"))
            .expect_err("unknown table");
        assert!(
            matches!(err, EngineError::Catalog(CatalogError::UnknownTable(_))),
            "got {err:?}"
        );
        let err = engine
            .execute(&parse_one("CREATE INDEX i ON account (ghost)"))
            .expect_err("unknown column");
        assert!(
            matches!(
                err,
                EngineError::Catalog(CatalogError::IndexColumnUnknown { .. })
            ),
            "got {err:?}"
        );
        let err = engine
            .execute(&parse_one("CREATE INDEX i ON account (id)"))
            .expect_err("business key");
        assert!(
            matches!(
                err,
                EngineError::Catalog(CatalogError::IndexOnBusinessKey { .. })
            ),
            "got {err:?}"
        );
        // DROP of an absent index errors without IF EXISTS, no-ops with it.
        let err = engine
            .execute(&parse_one("DROP INDEX i_ghost"))
            .expect_err("unknown index");
        assert!(
            matches!(err, EngineError::Catalog(CatalogError::UnknownIndex(_))),
            "got {err:?}"
        );
        let outcome = run_sql(&mut engine, "DROP INDEX IF EXISTS i_ghost");
        assert!(matches!(
            outcome,
            StatementOutcome::Ddl { tag: "DROP INDEX" }
        ));
    }

    #[test]
    fn predicate_dml_expands_at_its_statement_not_at_commit() {
        // The matched set is fixed when the statement runs: a row staged *after*
        // the predicate UPDATE does not retroactively join it — exactly what the
        // `UPDATE n` tag promised the client.
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
        engine
            .execute(&parse_one("INSERT INTO account VALUES (1, 100)"))
            .expect("insert");

        let mut txn = engine.begin();
        let summary = engine
            .stage_dml(&parse_one("UPDATE account SET balance = 0"), &mut txn)
            .expect("stage scan update")
            .expect("dml summary");
        assert_eq!(summary, DmlSummary::Update(1));
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (2, 200)"), &mut txn)
            .expect("stage later insert");
        engine.commit(txn).expect("commit");

        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            sorted(vec![vec![i4(1), i4(0)], vec![i4(2), i4(200)]]),
            "the later INSERT kept its value — it was not part of the earlier statement"
        );
    }

    #[test]
    fn a_torn_predicate_dml_commit_recovers_unchanged() {
        // Atomicity across the WAL boundary: an auto-committed whole-table UPDATE
        // is one group-commit record; failing its append makes none of the
        // statement durable — recovery reads the pre-statement table, never a
        // partial prefix ([STL-192] discipline applied to the scan-then-write
        // plan). All rows are delta-resident, so the statement's only disk write
        // is that record. As above, `MemDisk` models this as a *clean* append
        // failure (the fault fires before any byte is copied), so it also pins the
        // STL-295 live-session rollback below.
        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account VALUES (1, 100)",
            "INSERT INTO account VALUES (2, 200)",
            "INSERT INTO account VALUES (3, 300)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }

        faults.schedule(FaultOp::Append, io::ErrorKind::Other);
        let err = engine
            .execute(&parse_one("UPDATE account SET balance = 0"))
            .expect_err("the clean group-commit append failure fails the statement");
        assert!(matches!(err, EngineError::Storage(_)), "got {err:?}");

        let unchanged = sorted(vec![
            vec![i4(1), i4(100)],
            vec![i4(2), i4(200)],
            vec![i4(3), i4(300)],
        ]);
        // The *live* session matches recovery without a restart: the refused auto-commit
        // rolled its scan-then-write group back in memory, so a SELECT on the still-
        // running engine shows the pre-statement rows — not the `balance = 0` rows it
        // applied but never made durable ([STL-295]). A clean append failure does not
        // poison the WAL, so the engine keeps serving.
        assert!(
            !engine.is_poisoned(),
            "a clean append failure does not poison the session",
        );
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            unchanged,
            "the refused UPDATE left the live table at its pre-statement values",
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            unchanged,
            "none of the statement's writes survive the failed commit"
        );
    }

    #[test]
    fn a_mid_set_apply_failure_leaves_the_table_unchanged() {
        // Atomicity of the apply itself, with a genuinely applied prefix: the
        // transaction stages an INSERT and then a whole-table UPDATE (which
        // expands over the buffered row too). At COMMIT the INSERT applies first
        // (resident, no reads); the first UPDATE's read-modify-write then opens
        // the sealed segment and its read fails (injected). The statement set
        // must abort as a unit: the already-applied INSERT is rolled back in
        // memory ([STL-216]) and nothing is durable — the table is unchanged,
        // live *and* across recovery.
        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        engine.execute(&parse_one(CREATE)).expect("create");
        for sql in [
            "INSERT INTO account VALUES (1, 100)",
            "INSERT INTO account VALUES (2, 200)",
            "INSERT INTO account VALUES (3, 300)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // Seal the rows so every UPDATE's read-modify-write must read a segment.
        engine.flush().expect("flush seals the delta");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO account VALUES (4, 400)"), &mut txn)
            .expect("stage insert");
        let summary = engine
            .stage_dml(&parse_one("UPDATE account SET balance = 0"), &mut txn)
            .expect("stage scan update — the expansion scan runs now, faults unarmed")
            .expect("dml summary");
        assert_eq!(summary, DmlSummary::Update(4));

        // Arm the fault now: the next sealed-segment read — the first UPDATE's
        // read-modify-write — fails mid-set, after the INSERT already applied.
        faults.schedule(FaultOp::ReadAt, io::ErrorKind::Other);
        engine
            .commit(txn)
            .expect_err("the mid-set apply failure aborts the whole statement set");

        let unchanged = sorted(vec![
            vec![i4(1), i4(100)],
            vec![i4(2), i4(200)],
            vec![i4(3), i4(300)],
        ]);
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            unchanged,
            "the applied prefix (the INSERT) was rolled back in memory"
        );
        drop(engine);

        let mut engine = recover_session(&disk);
        assert_eq!(
            sorted(select(&mut engine, "SELECT * FROM account").rows),
            unchanged,
            "nothing of the aborted statement set is durable"
        );
    }

    #[test]
    fn predicate_dml_on_a_valid_time_table_writes_the_system_axis() {
        // Valid-time tables take the same scan-then-write plan, system-axis-only
        // ([STL-229] scope): the WHERE selects among system-live rows, an UPDATE
        // opens each matched key's new version under the SET's valid period
        // (mandatory `vf`, as for the point write — [STL-194]), and a whole-table
        // DELETE closes every system-live row.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE vt (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
            ))
            .expect("create valid-time table");
        engine
            .execute(&parse_one("INSERT INTO vt VALUES (1, 100, 10, 20)"))
            .expect("insert");
        engine
            .execute(&parse_one("INSERT INTO vt VALUES (2, 200, 10, 20)"))
            .expect("insert");

        assert_eq!(
            dml(
                &mut engine,
                "UPDATE vt SET balance = 0, vf = 30 WHERE balance >= 200"
            ),
            DmlSummary::Update(1),
            "the predicate matched one system-live row"
        );
        assert_eq!(
            sorted(select(&mut engine, "SELECT id, balance FROM vt").rows),
            sorted(vec![vec![i4(1), i4(100)], vec![i4(2), i4(0)]])
        );

        assert_eq!(dml(&mut engine, "DELETE FROM vt"), DmlSummary::Delete(2));
        assert!(
            select(&mut engine, "SELECT id FROM vt").rows.is_empty(),
            "the whole-table DELETE closed every system-live row"
        );
    }

    /// A tiny deterministic RNG (xorshift64*) for the seeded differential
    /// oracle — the engine crate stays free of dev-only RNG dependencies.
    struct TestRng(u64);
    impl TestRng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// The predicate-selection correctness oracle (testing strategy §4):
    /// a seeded random workload of point and predicate DML runs against both the
    /// engine and an in-process reference model (a map of live rows). After every
    /// statement the reported tag must equal the model's matched-row count and
    /// the full table must equal the model — across every comparison operator,
    /// key- and value-column anchors, arithmetic predicates, NULL cells (which a
    /// predicate never matches, but a whole-table write does), and a mid-workload
    /// flush so the scan-then-write plan also runs over sealed segments.
    #[test]
    fn predicate_dml_matches_a_reference_model_over_seeded_workloads() {
        const OPS: u64 = 60;
        for seed in 1..=5u64 {
            let mut rng = TestRng(seed);
            let mut engine = session();
            engine
                .execute(&parse_one(
                    "CREATE TABLE o (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
                ))
                .expect("create");
            // The reference model: business key -> live value (None = SQL NULL).
            let mut model: BTreeMap<i32, Option<i32>> = BTreeMap::new();

            for op in 0..OPS {
                // Exercise the sealed tier too: seal whatever is resident at the
                // workload's midpoint.
                if op == OPS / 2 {
                    engine.flush().expect("flush");
                }
                match rng.below(10) {
                    // Upsert a random key (INSERT when absent, point UPDATE when
                    // live — the point fast path stays in the mix).
                    0..=3 => {
                        let key = i32::try_from(rng.below(20)).expect("small key");
                        let value = (rng.below(5) > 0)
                            .then(|| i32::try_from(rng.below(100)).expect("small value"));
                        let cell = value.map_or("NULL".to_owned(), |v| v.to_string());
                        let sql = if model.contains_key(&key) {
                            format!("UPDATE o SET v = {cell} WHERE id = {key}")
                        } else {
                            format!("INSERT INTO o VALUES ({key}, {cell})")
                        };
                        engine.execute(&parse_one(&sql)).expect("point write");
                        model.insert(key, value);
                    }
                    // Point-delete a live key.
                    4 => {
                        if let Some(&key) = {
                            let keys: Vec<&i32> = model.keys().collect();
                            let pick = keys.len();
                            (pick > 0).then(|| {
                                keys[usize::try_from(rng.below(pick as u64)).expect("index")]
                            })
                        } {
                            engine
                                .execute(&parse_one(&format!("DELETE FROM o WHERE id = {key}")))
                                .expect("point delete");
                            model.remove(&key);
                        }
                    }
                    // Predicate (or whole-table) UPDATE / DELETE.
                    kind => {
                        let (where_sql, matched) = random_predicate(&mut rng, &model);
                        let is_update = kind <= 7;
                        let new = i32::try_from(rng.below(100)).expect("small value");
                        let sql = if is_update {
                            format!("UPDATE o SET v = {new}{where_sql}")
                        } else {
                            format!("DELETE FROM o{where_sql}")
                        };
                        // Every shape — including a key-equality WHERE on an absent
                        // key, which takes the point fast path — reports the matched
                        // live-row count. STL-294 aligned that fast path with set
                        // semantics: an absent key is `UPDATE 0` / `DELETE 0`, the
                        // same 0-count this `matched.is_empty()` branch expects, with
                        // no special-case error arm.
                        let got = dml(&mut engine, &sql);
                        let count = matched.len() as u64;
                        let want = if is_update {
                            for key in &matched {
                                model.insert(*key, Some(new));
                            }
                            DmlSummary::Update(count)
                        } else {
                            for key in &matched {
                                model.remove(key);
                            }
                            DmlSummary::Delete(count)
                        };
                        assert_eq!(
                            got, want,
                            "seed {seed} op {op}: tag must count the matched live rows"
                        );
                    }
                }

                let want: Vec<Vec<Option<Vec<u8>>>> = model
                    .iter()
                    .map(|(k, v)| vec![i4(*k), cell(v.map(ScalarValue::Int4))])
                    .collect();
                assert_eq!(
                    sorted(select(&mut engine, "SELECT * FROM o").rows),
                    sorted(want),
                    "seed {seed} op {op}: the table must equal the reference model"
                );
            }
        }
    }

    /// Draw a random `WHERE` for the differential oracle and compute the keys it
    /// matches in the model. Covers: whole-table (no WHERE), all six comparison
    /// operators anchored on the key or the value column, and an arithmetic
    /// predicate. A NULL value cell never matches a predicate (the evaluator's
    /// three-valued logic keeps only TRUE rows) but is matched by no-WHERE.
    ///
    /// A key **equality on an absent key** lowers to the point fast path, which
    /// since STL-294 reports the matched-row count like every other shape — an
    /// empty match here, so no special-case arm.
    fn random_predicate(
        rng: &mut TestRng,
        model: &BTreeMap<i32, Option<i32>>,
    ) -> (String, Vec<i32>) {
        const OPS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];
        let cmp = |op: &str, lhs: i64, rhs: i64| match op {
            "=" => lhs == rhs,
            "<>" => lhs != rhs,
            "<" => lhs < rhs,
            "<=" => lhs <= rhs,
            ">" => lhs > rhs,
            _ => lhs >= rhs,
        };
        match rng.below(8) {
            // Whole-table: no WHERE — NULL-valued rows match too.
            0 => (String::new(), model.keys().copied().collect()),
            // Arithmetic over the value column: `v % 2 = 0`.
            1 => (
                " WHERE v % 2 = 0".to_owned(),
                model
                    .iter()
                    .filter(|(_, v)| v.is_some_and(|v| v % 2 == 0))
                    .map(|(k, _)| *k)
                    .collect(),
            ),
            // A comparison anchored on the key column. Equality is the point fast
            // path (an absent key reports 0 rows, [STL-294]).
            2 | 3 => {
                let op = OPS[usize::try_from(rng.below(6)).expect("op index")];
                let lit = i64::try_from(rng.below(20)).expect("small literal");
                let matched: Vec<i32> = model
                    .keys()
                    .filter(|k| cmp(op, i64::from(**k), lit))
                    .copied()
                    .collect();
                (format!(" WHERE id {op} {lit}"), matched)
            }
            // A comparison anchored on the value column; NULL never matches.
            _ => {
                let op = OPS[usize::try_from(rng.below(6)).expect("op index")];
                let lit = i64::try_from(rng.below(100)).expect("small literal");
                (
                    format!(" WHERE v {op} {lit}"),
                    model
                        .iter()
                        .filter(|(_, v)| v.is_some_and(|v| cmp(op, i64::from(v), lit)))
                        .map(|(k, _)| *k)
                        .collect(),
                )
            }
        }
    }

    // --- STL-230: MERGE — WHEN MATCHED / NOT MATCHED upsert ------------------

    /// The full table as `(id, v)` rows, sorted — the oracle's observable state.
    fn table_state(engine: &mut SessionEngine<ZeroClock, MemDisk>) -> Vec<Vec<Option<Vec<u8>>>> {
        sorted(select(engine, "SELECT * FROM o").rows)
    }

    const CREATE_O: &str = "CREATE TABLE o (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING";

    #[test]
    fn merge_upserts_a_mixed_values_batch() {
        // The DoD shape: one statement over a batch where some keys exist
        // (updated) and some don't (inserted), the tag counting every acted-on
        // source row.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");
        dml(&mut engine, "INSERT INTO o VALUES (2, 20)");

        let got = dml(
            &mut engine,
            "MERGE INTO o USING (VALUES (1, 100), (3, 300)) AS s (id, v) ON o.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        );
        assert_eq!(got, DmlSummary::Merge(2));
        assert_eq!(
            table_state(&mut engine),
            sorted(vec![
                vec![i4(1), i4(100)],
                vec![i4(2), i4(20)],
                vec![i4(3), i4(300)],
            ]),
            "key 1 updated, key 2 untouched, key 3 inserted"
        );
    }

    #[test]
    fn merge_with_a_single_arm_skips_the_other_rows() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");

        // Only WHEN MATCHED: the unmatched source row (key 9) is skipped — not
        // inserted, not counted.
        let got = dml(
            &mut engine,
            "MERGE INTO o USING (VALUES (1, 11), (9, 99)) AS s (id, v) ON o.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v",
        );
        assert_eq!(got, DmlSummary::Merge(1));
        assert_eq!(table_state(&mut engine), vec![vec![i4(1), i4(11)]]);

        // Only WHEN NOT MATCHED: the matched source row (key 1) is skipped — its
        // live value stands.
        let got = dml(
            &mut engine,
            "MERGE INTO o USING (VALUES (1, 12), (9, 99)) AS s (id, v) ON o.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        );
        assert_eq!(got, DmlSummary::Merge(1));
        assert_eq!(
            table_state(&mut engine),
            sorted(vec![vec![i4(1), i4(11)], vec![i4(9), i4(99)]])
        );
    }

    #[test]
    fn merge_reads_a_table_source_at_the_statement_snapshot() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        engine
            .execute(&parse_one(
                "CREATE TABLE src (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
            ))
            .expect("create source");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");
        dml(&mut engine, "INSERT INTO src VALUES (1, 100)");
        dml(&mut engine, "INSERT INTO src VALUES (2, 200)");
        // A NULL source value column rides through both arms as a NULL cell.
        dml(&mut engine, "INSERT INTO src VALUES (3, NULL)");

        let got = dml(
            &mut engine,
            "MERGE INTO o USING src AS s ON o.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        );
        assert_eq!(got, DmlSummary::Merge(3));
        assert_eq!(
            table_state(&mut engine),
            sorted(vec![
                vec![i4(1), i4(100)],
                vec![i4(2), i4(200)],
                vec![i4(3), cell(None)],
            ])
        );
    }

    #[test]
    fn merge_in_a_transaction_overlays_its_own_writes() {
        // Read-your-own-writes ([STL-203]): an INSERT staged earlier in the block
        // is *matched* by a later MERGE in the same block; the buffered MERGE
        // writes stay invisible to other connections until COMMIT.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");

        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO o VALUES (2, 20)"), &mut txn)
            .expect("stage insert")
            .expect("a DML summary");
        let staged = engine
            .stage_dml(
                &parse_one(
                    "MERGE INTO o USING (VALUES (1, 100), (2, 200), (3, 300)) AS s (id, v) \
                     ON o.id = s.id \
                     WHEN MATCHED THEN UPDATE SET v = s.v \
                     WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
                ),
                &mut txn,
            )
            .expect("stage merge")
            .expect("a DML summary");
        // Key 1 (committed) and key 2 (buffered, RYOW) are matched; key 3 inserts.
        assert_eq!(staged, DmlSummary::Merge(3));

        // Uncommitted: another connection still sees only the pre-txn state.
        assert_eq!(table_state(&mut engine), vec![vec![i4(1), i4(10)]]);

        engine.commit(txn).expect("commit");
        assert_eq!(
            table_state(&mut engine),
            sorted(vec![
                vec![i4(1), i4(100)],
                vec![i4(2), i4(200)],
                vec![i4(3), i4(300)],
            ])
        );
    }

    #[test]
    fn merge_affecting_a_row_twice_fails_and_leaves_the_table_unchanged() {
        // The DoD atomicity oracle: row 3 of the source is a second write to key
        // 7 — the statement fails (deterministically, the standard's cardinality
        // posture) and *none* of the batch applies: not the update to key 1, not
        // the first insert of key 7.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");
        let before = table_state(&mut engine);

        let err = engine
            .execute(&parse_one(
                "MERGE INTO o USING (VALUES (1, 100), (7, 70), (7, 71)) AS s (id, v) \
                 ON o.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.v \
                 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
            ))
            .unwrap_err();
        assert!(matches!(err, EngineError::MergeRowTwice), "got {err:?}");
        assert_eq!(
            table_state(&mut engine),
            before,
            "a failed MERGE leaves the table unchanged"
        );

        // The same failure at staging, inside a transaction: nothing is buffered,
        // the block's earlier writes are untouched, and the commit applies them.
        let mut txn = engine.begin();
        engine
            .stage_dml(&parse_one("INSERT INTO o VALUES (2, 20)"), &mut txn)
            .expect("stage insert")
            .expect("a DML summary");
        let err = engine
            .stage_dml(
                &parse_one(
                    "MERGE INTO o USING (VALUES (7, 70), (7, 71)) AS s (id, v) ON o.id = s.id \
                     WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
                ),
                &mut txn,
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::MergeRowTwice), "got {err:?}");
        engine.commit(txn).expect("commit");
        assert_eq!(
            table_state(&mut engine),
            sorted(vec![vec![i4(1), i4(10)], vec![i4(2), i4(20)]]),
            "the failed MERGE staged nothing; the block's other write committed"
        );
    }

    #[test]
    fn a_failing_write_mid_group_aborts_the_whole_merge_set() {
        // Drive the shared group-apply with a write set whose second write fails
        // (an UPDATE of a key with no live version — unreachable through a real
        // MERGE expansion, which is what makes it the right poison here): the
        // already-applied first insert must be rolled back ([STL-216]), so the
        // statement-shaped group leaves the table unchanged.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");
        let before = table_state(&mut engine);

        let schema_id = stele_catalog::SchemaId(1);
        let writes = vec![
            BoundDml::Insert {
                table: "o".to_owned(),
                schema_id,
                key: ScalarValue::Int4(5),
                values: vec![Some(ScalarValue::Int4(50))],
                valid: None,
            },
            BoundDml::Update {
                table: "o".to_owned(),
                schema_id,
                key: ScalarValue::Int4(999),
                assignments: vec![(0, Some(ScalarValue::Int4(0)))],
                valid: None,
            },
        ];
        let err = engine
            .apply_write_group(writes, DmlSummary::Merge(2))
            .unwrap_err();
        assert!(matches!(err, EngineError::Storage(_)), "got {err:?}");
        assert_eq!(
            table_state(&mut engine),
            before,
            "the applied prefix of a failed group is rolled back in memory"
        );
    }

    #[test]
    fn merge_null_join_keys_never_match_and_a_null_insert_key_fails() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");
        dml(&mut engine, "INSERT INTO o VALUES (1, 10)");
        let before = table_state(&mut engine);

        // A NULL join key matches nothing; with only a MATCHED arm the row is
        // skipped — the statement acts on zero rows.
        let got = dml(
            &mut engine,
            "MERGE INTO o USING (VALUES (NULL, 100)) AS s (id, v) ON o.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v",
        );
        assert_eq!(got, DmlSummary::Merge(0));
        assert_eq!(table_state(&mut engine), before);

        // The same NULL flowing into the *inserted business key* can never
        // write — the statement fails closed and the table is unchanged.
        let err = engine
            .execute(&parse_one(
                "MERGE INTO o USING (VALUES (NULL, 100)) AS s (id, v) ON o.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
            ))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Dml(DmlError::NullValue { .. })),
            "got {err:?}"
        );
        assert_eq!(table_state(&mut engine), before);
    }

    /// The MERGE upsert correctness oracle (testing strategy §4): seeded random
    /// source batches — mixed matched/not-matched keys, both arms, single arms,
    /// NULL value cells — run against the engine and an in-process reference
    /// model. After every statement the tag must count the acted-on source rows
    /// and the full table must equal the model; a mid-workload flush makes the
    /// probe also run over sealed segments.
    #[test]
    fn merge_matches_a_reference_model_over_seeded_workloads() {
        const OPS: u64 = 40;
        for seed in 1..=5u64 {
            let mut rng = TestRng(seed);
            let mut engine = session();
            engine.execute(&parse_one(CREATE_O)).expect("create");
            let mut model: BTreeMap<i32, Option<i32>> = BTreeMap::new();

            for op in 0..OPS {
                if op == OPS / 2 {
                    engine.flush().expect("flush");
                }
                // Draw a batch of distinct source keys (a duplicate is the
                // statement-level error oracled separately).
                let batch_len = 1 + rng.below(5);
                let mut batch: Vec<(i32, Option<i32>)> = Vec::new();
                for _ in 0..batch_len {
                    let key = i32::try_from(rng.below(25)).expect("small key");
                    if batch.iter().any(|(k, _)| *k == key) {
                        continue;
                    }
                    let value = (rng.below(5) > 0)
                        .then(|| i32::try_from(rng.below(100)).expect("small value"));
                    batch.push((key, value));
                }
                let values = batch
                    .iter()
                    .map(|(k, v)| {
                        format!("({k}, {})", v.map_or("NULL".to_owned(), |v| v.to_string()))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                // Both arms, or one — the model mirrors exactly the arms given.
                let (matched_arm, not_matched_arm) = match rng.below(4) {
                    0 => (true, false),
                    1 => (false, true),
                    _ => (true, true),
                };
                let mut sql =
                    format!("MERGE INTO o USING (VALUES {values}) AS s (id, v) ON o.id = s.id");
                if matched_arm {
                    sql.push_str(" WHEN MATCHED THEN UPDATE SET v = s.v");
                }
                if not_matched_arm {
                    sql.push_str(" WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)");
                }

                let mut acted = 0u64;
                for (key, value) in &batch {
                    // A live key takes the MATCHED arm, an absent one the NOT
                    // MATCHED arm — and either way the upsert lands the value.
                    let arm = if model.contains_key(key) {
                        matched_arm
                    } else {
                        not_matched_arm
                    };
                    if arm {
                        model.insert(*key, *value);
                        acted += 1;
                    }
                }

                let got = dml(&mut engine, &sql);
                assert_eq!(
                    got,
                    DmlSummary::Merge(acted),
                    "seed {seed} op {op}: the tag must count acted-on source rows"
                );
                let want: Vec<Vec<Option<Vec<u8>>>> = model
                    .iter()
                    .map(|(k, v)| vec![i4(*k), cell(v.map(ScalarValue::Int4))])
                    .collect();
                assert_eq!(
                    table_state(&mut engine),
                    sorted(want),
                    "seed {seed} op {op}: the table must equal the reference model"
                );
            }
        }
    }

    // --- STL-312: cost-based MERGE probe-plan selection ----------------------

    #[test]
    fn merge_probe_plan_follows_the_source_vs_keyspace_cost() {
        // STL-312: the per-source-key indexed probe is chosen by a cost estimate,
        // not by "the target holds a sealed segment". Probe when the source touches
        // fewer keys than the live keyspace holds (sealed versions + delta);
        // otherwise read every live key in one full-keyset scan. Either plan yields
        // the same upsert — that result-identity is the existing oracle
        // (`merge_matches_a_reference_model_over_seeded_workloads`) — so this pins
        // only the *choice*, including the corners the old heuristic missed.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_O)).expect("create");

        // Empty target: no live key to point-read, so any source full-scans.
        assert!(!engine.merge_should_probe_per_key("o", 0));
        assert!(!engine.merge_should_probe_per_key("o", 4));

        // Ten resident (all-delta) keys → a keyspace estimate of ten.
        let batch = (0..10)
            .map(|k| format!("({k}, {k})"))
            .collect::<Vec<_>>()
            .join(", ");
        dml(&mut engine, &format!("INSERT INTO o VALUES {batch}"));

        // The boundary the old heuristic could not see: a probe-key count below the
        // keyspace probes, one at or above it full-scans — even though every key is
        // still in the delta (the old rule *always* full-scanned an all-delta
        // target, whatever the source size).
        assert!(
            engine.merge_should_probe_per_key("o", 9),
            "9 < 10 keys → probe per key",
        );
        assert!(
            !engine.merge_should_probe_per_key("o", 10),
            "10 ≥ 10 keys → full-keyset scan",
        );
        assert!(
            !engine.merge_should_probe_per_key("o", 40),
            "40 ≥ 10 keys → full-keyset scan",
        );

        // Flushing the ten versions into a sealed segment leaves the estimate at
        // ten — it counts sealed versions and delta rows alike — so the choice is
        // unchanged. (The old rule flipped the small-source case to the probe the
        // instant a segment existed, regardless of how the sizes actually compare.)
        engine.flush().expect("flush");
        assert!(
            engine.merge_should_probe_per_key("o", 9),
            "9 < 10 keys → probe per key (now sealed)",
        );
        assert!(
            !engine.merge_should_probe_per_key("o", 10),
            "10 ≥ 10 keys → full-keyset scan (now sealed)",
        );

        // An unknown table reports false; the subsequent read surfaces the error.
        assert!(!engine.merge_should_probe_per_key("absent", 1));
    }

    // ---- STL-235: temporal MERGE historization (close/open over both axes) ----

    const CREATE_VACCT: &str = "CREATE TABLE vacct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)";

    /// `SELECT id, balance FROM vacct` pinned at `(system, valid)`, sorted.
    fn vt_asof(
        engine: &mut SessionEngine<ZeroClock, MemDisk>,
        s: i64,
        v: i64,
    ) -> Vec<Vec<Option<Vec<u8>>>> {
        let sql = format!(
            "SELECT id, balance FROM vacct FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}"
        );
        sorted(select(engine, &sql).rows)
    }

    #[test]
    fn merge_historizes_a_matched_valid_time_row() {
        // A matched MERGE closes the prior version on the system axis and opens a
        // new one over the arm's valid interval ([STL-166] close/open): the new
        // fact is valid only over [5, 10), and the pre-MERGE history is untouched.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        // Key 1 valid [0, +∞), balance 100.
        dml(
            &mut engine,
            "INSERT INTO vacct (id, balance, vf) VALUES (1, 100, 0)",
        );
        let s1 = engine.clock.current().0;
        assert_eq!(
            dml(
                &mut engine,
                "MERGE INTO vacct USING (VALUES (1, 200)) AS s (id, bal) ON vacct.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.bal, vf = 5, vt = 10 \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) VALUES (s.id, s.bal, 5, 10)"
            ),
            DmlSummary::Merge(1),
        );
        let now = engine.clock.current().0;
        // Pre-MERGE history is immutable: AS OF s1 still sees the wide [0, +∞) fact.
        assert_eq!(
            vt_asof(&mut engine, s1, 0),
            vec![vec![i4(1), i4(100)]],
            "pre-MERGE v=0 unchanged"
        );
        assert_eq!(
            vt_asof(&mut engine, s1, 7),
            vec![vec![i4(1), i4(100)]],
            "pre-MERGE v=7 unchanged"
        );
        // Now the fact is valid only over [5, 10): inside sees the new value;
        // outside is absent (the close/open narrowed the period — no overlap, no
        // resurrection at v=0), and the half-open upper bound excludes v=10.
        assert!(
            vt_asof(&mut engine, now, 0).is_empty(),
            "v=0 no longer covered after the close/open"
        );
        assert_eq!(
            vt_asof(&mut engine, now, 7),
            vec![vec![i4(1), i4(200)]],
            "v=7 sees the new fact"
        );
        assert!(
            vt_asof(&mut engine, now, 10).is_empty(),
            "half-open: v=10 is excluded from [5, 10)"
        );
    }

    #[test]
    fn merge_not_matched_inserts_with_the_valid_interval() {
        // An unmatched source row inserts with the arm's valid interval — here an
        // open period [3, +∞).
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        assert_eq!(
            dml(
                &mut engine,
                "MERGE INTO vacct USING (VALUES (2, 50)) AS s (id, bal) ON vacct.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.bal, vf = 3 \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf) VALUES (s.id, s.bal, 3)"
            ),
            DmlSummary::Merge(1),
        );
        let now = engine.clock.current().0;
        assert!(
            vt_asof(&mut engine, now, 2).is_empty(),
            "before the valid start the row is absent"
        );
        assert_eq!(
            vt_asof(&mut engine, now, 3),
            vec![vec![i4(2), i4(50)]],
            "the open period covers its start"
        );
        assert_eq!(
            vt_asof(&mut engine, now, 1_000),
            vec![vec![i4(2), i4(50)]],
            "the open period extends to +∞"
        );
    }

    #[test]
    fn merge_mixed_batch_on_a_valid_time_table() {
        // One MERGE over a mixed batch: key 1 exists (matched ⇒ close/open) and key
        // 3 is new (not-matched ⇒ insert), both opening valid [5, +∞). Key 1's
        // pre-MERGE history stays readable AS OF the past.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        dml(
            &mut engine,
            "INSERT INTO vacct (id, balance, vf) VALUES (1, 100, 0)",
        );
        let s1 = engine.clock.current().0;
        assert_eq!(
            dml(
                &mut engine,
                "MERGE INTO vacct USING (VALUES (1, 111), (3, 333)) AS s (id, bal) \
                 ON vacct.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.bal, vf = 5 \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf) VALUES (s.id, s.bal, 5)"
            ),
            DmlSummary::Merge(2),
        );
        let now = engine.clock.current().0;
        // Both new facts are valid [5, +∞): a current read at v=7 sees both.
        assert_eq!(
            vt_asof(&mut engine, now, 7),
            sorted(vec![vec![i4(1), i4(111)], vec![i4(3), i4(333)]]),
            "both the updated and inserted facts are live at v=7"
        );
        // Key 1's pre-MERGE fact (valid [0, +∞), balance 100) is unchanged AS OF s1.
        assert_eq!(
            vt_asof(&mut engine, s1, 0),
            vec![vec![i4(1), i4(100)]],
            "the pre-MERGE history is immutable"
        );
        // …and at v=0 *now* both keys are absent: the matched close/open narrowed
        // key 1 to [5, +∞), and key 3 was inserted there — neither covers v=0.
        assert!(
            vt_asof(&mut engine, now, 0).is_empty(),
            "neither new fact covers v=0"
        );
    }

    // ---- STL-308: per-source-row valid-time bounds ----

    #[test]
    fn merge_per_source_row_bounds_give_each_key_its_own_window() {
        // STL-308: the headline shape — one MERGE whose arm takes `vf`/`vt` from the
        // source row, so each affected key carries its own `[from, to)` interval.
        // Two unmatched keys insert over disjoint windows drawn from their rows.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        assert_eq!(
            dml(
                &mut engine,
                "MERGE INTO vacct USING (VALUES (1, 100, 2, 5), (2, 200, 7, 9)) \
                 AS s (id, bal, vfrom, vto) ON vacct.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) \
                 VALUES (s.id, s.bal, s.vfrom, s.vto)"
            ),
            DmlSummary::Merge(2),
        );
        let now = engine.clock.current().0;
        // Key 1 is valid only over [2, 5); key 2 only over [7, 9).
        assert_eq!(
            vt_asof(&mut engine, now, 3),
            vec![vec![i4(1), i4(100)]],
            "v=3 sees only key 1's window"
        );
        assert_eq!(
            vt_asof(&mut engine, now, 8),
            vec![vec![i4(2), i4(200)]],
            "v=8 sees only key 2's window"
        );
        assert!(
            vt_asof(&mut engine, now, 6).is_empty(),
            "v=6 falls in the gap between the two per-row windows"
        );
        assert!(
            vt_asof(&mut engine, now, 5).is_empty(),
            "half-open: v=5 is excluded from key 1's [2, 5)"
        );
    }

    #[test]
    fn merge_matched_close_open_uses_the_per_source_row_interval() {
        // A matched arm with a per-row bound closes the prior version on the system
        // axis and opens the new one over the source row's own interval — the
        // close/open at scale, but the window is data-dependent ([STL-308]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        dml(
            &mut engine,
            "INSERT INTO vacct (id, balance, vf) VALUES (1, 100, 0)",
        );
        let s1 = engine.clock.current().0;
        assert_eq!(
            dml(
                &mut engine,
                "MERGE INTO vacct USING (VALUES (1, 200, 4, 8)) AS s (id, bal, vfrom, vto) \
                 ON vacct.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.bal, vf = s.vfrom, vt = s.vto"
            ),
            DmlSummary::Merge(1),
        );
        let now = engine.clock.current().0;
        // Pre-MERGE history is immutable: AS OF s1 still sees the wide [0, +∞) fact.
        assert_eq!(
            vt_asof(&mut engine, s1, 0),
            vec![vec![i4(1), i4(100)]],
            "pre-MERGE history unchanged"
        );
        // Now valid only over [4, 8): inside sees the new value; the close/open
        // narrowed the window, so v=0 no longer resolves and v=8 is excluded.
        assert!(
            vt_asof(&mut engine, now, 0).is_empty(),
            "v=0 no longer covered after the per-row close/open"
        );
        assert_eq!(
            vt_asof(&mut engine, now, 6),
            vec![vec![i4(1), i4(200)]],
            "v=6 sees the new fact over its per-row window"
        );
        assert!(
            vt_asof(&mut engine, now, 8).is_empty(),
            "half-open: v=8 is excluded from [4, 8)"
        );
    }

    #[test]
    fn merge_table_source_timestamp_columns_feed_per_row_bounds() {
        // STL-308 reconciliation: a *table* source's TIMESTAMP columns feed the
        // per-row bounds — their microsecond bodies are the instants, exactly as
        // the VALUES integer cells are. Here 5 s / 9 s past the epoch.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        engine
            .execute(&parse_one(
                "CREATE TABLE feed (id INT PRIMARY KEY, bal INT, vfrom TIMESTAMP, vto TIMESTAMP) \
                 WITH SYSTEM VERSIONING",
            ))
            .expect("create feed");
        engine
            .execute(&parse_one(
                "INSERT INTO feed (id, bal, vfrom, vto) VALUES \
                 (1, 100, TIMESTAMP '1970-01-01 00:00:05', TIMESTAMP '1970-01-01 00:00:09')",
            ))
            .expect("insert feed");
        assert_eq!(
            dml(
                &mut engine,
                "MERGE INTO vacct USING feed AS s ON vacct.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) \
                 VALUES (s.id, s.bal, s.vfrom, s.vto)"
            ),
            DmlSummary::Merge(1),
        );
        let now = engine.clock.current().0;
        assert_eq!(
            vt_asof(&mut engine, now, 5_000_000),
            vec![vec![i4(1), i4(100)]],
            "the row is valid from 5 s past the epoch"
        );
        assert!(
            vt_asof(&mut engine, now, 9_000_000).is_empty(),
            "half-open: 9 s is excluded from [5 s, 9 s)"
        );
    }

    #[test]
    fn merge_null_per_source_row_bound_is_rejected_at_execution() {
        // A per-row bound that resolves to NULL has no microsecond instant — a
        // valid-time version must say when it begins, so the statement fails and
        // (atomic group) leaves the table unchanged. A NULL is data-dependent, so
        // unlike a literal NULL key it is caught at execution, not bind.
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        let err = engine
            .execute(&parse_one(
                "MERGE INTO vacct USING (VALUES (1, 100, NULL, 5)) AS s (id, bal, vfrom, vto) \
                 ON vacct.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) \
                 VALUES (s.id, s.bal, s.vfrom, s.vto)",
            ))
            .expect_err("a NULL per-row start bound must be rejected");
        assert!(
            matches!(&err, EngineError::Dml(DmlError::NullValue { column, .. }) if column == "vf"),
            "got {err:?}"
        );
        assert!(
            select(&mut engine, "SELECT id, balance FROM vacct")
                .rows
                .is_empty(),
            "the failed MERGE left no row behind"
        );
    }

    #[test]
    fn merge_reversed_per_source_row_interval_is_rejected_at_execution() {
        // A reversed/empty per-row interval (`from >= to`) is rejected the way the
        // binder rejects a statement-level one — but here at execution, since the
        // bounds are only known per source row ([STL-308]).
        let mut engine = session();
        engine.execute(&parse_one(CREATE_VACCT)).expect("create");
        let err = engine
            .execute(&parse_one(
                "MERGE INTO vacct USING (VALUES (1, 100, 9, 4)) AS s (id, bal, vfrom, vto) \
                 ON vacct.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) \
                 VALUES (s.id, s.bal, s.vfrom, s.vto)",
            ))
            .expect_err("a reversed per-row interval must be rejected");
        assert!(
            matches!(
                &err,
                EngineError::Dml(DmlError::EmptyValidInterval { from: 9, to: 4, .. })
            ),
            "got {err:?}"
        );
    }

    // ---- STL-234: uncorrelated subqueries (scalar, IN, EXISTS) ----

    /// A session with an outer `t` and an inner `s`, each a key plus one INT
    /// value column — the substrate for the subquery tests.
    fn subquery_session() -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        engine
    }

    /// The `id` column of a `SELECT id FROM …` result, ascending — the outer rows
    /// a subquery `WHERE` kept.
    fn subquery_ids(result: &SelectResult) -> Vec<i32> {
        let mut ids: Vec<i32> = result
            .rows
            .iter()
            .map(|row| {
                let bytes = row[0].as_ref().expect("id is never NULL");
                match ScalarValue::decode(LogicalType::Int4, bytes).expect("decode id") {
                    ScalarValue::Int4(id) => id,
                    _ => panic!("id is INT"),
                }
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn scalar_subquery_in_where_folds_to_a_literal() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 20)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // `a = (SELECT a FROM s WHERE id = 1)` folds to `a = 20` → row 2.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a = (SELECT a FROM s WHERE id = 1)",
        ));
        assert_eq!(got, vec![2]);
        // A non-commutative op keeps its operand order: `a > 20` → row 3.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a > (SELECT a FROM s WHERE id = 1)",
        ));
        assert_eq!(got, vec![3]);
        // Subquery on the left: `20 < a` → row 3 (not mis-lowered as `a < 20`).
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE (SELECT a FROM s WHERE id = 1) < a",
        ));
        assert_eq!(got, vec![3]);
    }

    #[test]
    fn scalar_subquery_with_no_row_is_null_and_matches_nothing() {
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert");
        // The inner returns no row → the scalar is NULL → the comparison is
        // unknown for every row → empty result (never a silently-unfiltered read).
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a = (SELECT a FROM s WHERE id = 99)",
        ));
        assert!(got.is_empty(), "got {got:?}");
    }

    #[test]
    fn scalar_subquery_returning_many_rows_is_cardinality_violation() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO s VALUES (1, 10)",
            "INSERT INTO s VALUES (2, 20)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // `SELECT a FROM s` returns two rows used as a scalar → SQLSTATE 21000.
        let err = engine
            .execute(&parse_one("SELECT id FROM t WHERE a = (SELECT a FROM s)"))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ScalarSubqueryCardinality),
            "got {err:?}"
        );
    }

    #[test]
    fn in_subquery_is_set_membership() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 10)",
            "INSERT INTO s VALUES (2, 30)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s)",
        ));
        assert_eq!(got, vec![1, 3]);
    }

    #[test]
    fn in_empty_set_matches_nothing_and_not_in_empty_set_matches_all() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // `s` is empty: `IN ()` is false for every row, `NOT IN ()` true for every.
        let in_got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s)",
        ));
        assert!(in_got.is_empty(), "got {in_got:?}");
        let not_in_got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s)",
        ));
        assert_eq!(not_in_got, vec![1, 2]);
    }

    #[test]
    fn balanced_logic_builds_a_logarithmic_depth_tree() {
        // A left-deep fold of N terms is N deep; the balanced build caps depth at
        // ceil(log2 N) — what keeps `eval_expr` (and the tree's `Drop`) off a stack
        // overflow when an `IN (SELECT …)` set is large.
        fn logic_depth(e: &Expr) -> usize {
            match e {
                Expr::Logic { left, right, .. } => 1 + logic_depth(left).max(logic_depth(right)),
                _ => 0,
            }
        }
        let terms: Vec<Expr> = (0..1024)
            .map(|i| Expr::col(0).compare(CmpOp::Eq, Expr::lit(ScalarValue::Int4(i))))
            .collect();
        let tree = balanced_logic(terms, LogicOp::Or).expect("non-empty");
        assert_eq!(logic_depth(&tree), 10, "1024 leaves → depth 10, not 1023");

        // Empty → None; a lone term is returned unwrapped (depth 0).
        assert!(balanced_logic(Vec::new(), LogicOp::Or).is_none());
        let one = balanced_logic(
            vec![Expr::col(0).compare(CmpOp::Eq, Expr::lit(ScalarValue::Int4(7)))],
            LogicOp::And,
        )
        .expect("one");
        assert_eq!(logic_depth(&one), 0);
    }

    #[test]
    fn in_subquery_over_a_large_inner_result_does_not_overflow_the_stack() {
        // Regression: an `IN (SELECT …)` whose inner result is a few thousand rows
        // used to fold into an N-deep OR tree that `eval_expr` walked recursively,
        // overflowing a runtime worker thread's stack and aborting the whole server.
        // Run the real bind→scan→eval path on a thread with a worker-sized (2 MiB)
        // stack — the size that crashed — so a re-introduced left-deep fold fails
        // here (a stack overflow aborts the test process) instead of in production.
        const N: i32 = 2_000;
        let (in_count, not_in_count) = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                use std::fmt::Write as _;
                let mut engine = subquery_session();
                let mut insert = String::from("INSERT INTO t VALUES ");
                for i in 1..=N {
                    if i > 1 {
                        insert.push(',');
                    }
                    let _ = write!(insert, "({i},{})", i * 3);
                }
                engine.execute(&parse_one(&insert)).expect("bulk insert");
                // Every row's `a` is in `SELECT a FROM t`: `IN` keeps all N rows,
                // `NOT IN` (the AND-tree path) keeps none — both fold over the full set.
                let in_rows = select(&mut engine, "SELECT id FROM t WHERE a IN (SELECT a FROM t)")
                    .rows
                    .len();
                let not_in_rows = select(
                    &mut engine,
                    "SELECT id FROM t WHERE a NOT IN (SELECT a FROM t)",
                )
                .rows
                .len();
                (in_rows, not_in_rows)
            })
            .expect("spawn")
            .join()
            .expect("the IN-subquery must not overflow the stack");
        assert_eq!(in_count, usize::try_from(N).unwrap(), "IN keeps every row");
        assert_eq!(not_in_count, 0, "NOT IN over the full set keeps none");
    }

    #[test]
    fn not_in_subquery_with_null_in_set_matches_nothing() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO s VALUES (1, 10)",
            "INSERT INTO s VALUES (2, NULL)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // The classic three-valued trap: `a NOT IN (10, NULL)` is never TRUE.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s)",
        ));
        assert!(got.is_empty(), "got {got:?}");
        // Plain `IN` still matches the non-NULL member (the NULL is inert).
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s)",
        ));
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn in_subquery_with_null_outer_value_is_excluded() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, NULL)",
            "INSERT INTO s VALUES (1, 10)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // A NULL outer value is never provably IN (nor NOT IN) a set.
        let in_got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s)",
        ));
        assert_eq!(in_got, vec![1]);
        let not_in_got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s)",
        ));
        assert!(not_in_got.is_empty(), "got {not_in_got:?}");
    }

    #[test]
    fn exists_subquery_is_a_constant_keep_or_drop() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // `s` empty: EXISTS keeps none, NOT EXISTS keeps all (idiomatic SELECT 1).
        assert!(
            subquery_ids(&select(
                &mut engine,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s)"
            ))
            .is_empty()
        );
        assert_eq!(
            subquery_ids(&select(
                &mut engine,
                "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s)"
            )),
            vec![1, 2]
        );
        engine
            .execute(&parse_one("INSERT INTO s VALUES (1, 99)"))
            .expect("insert");
        // `s` non-empty: EXISTS keeps all, NOT EXISTS keeps none.
        assert_eq!(
            subquery_ids(&select(
                &mut engine,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s)"
            )),
            vec![1, 2]
        );
        assert!(
            subquery_ids(&select(
                &mut engine,
                "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s)"
            ))
            .is_empty()
        );
    }

    #[test]
    fn exists_with_an_inner_where_tests_filtered_presence() {
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert");
        engine
            .execute(&parse_one("INSERT INTO s VALUES (1, 5)"))
            .expect("insert");
        // The inner WHERE excludes every `s` row → EXISTS keeps none.
        assert!(
            subquery_ids(&select(
                &mut engine,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE a > 100)"
            ))
            .is_empty()
        );
        // The inner WHERE keeps a row → EXISTS keeps the outer row.
        assert_eq!(
            subquery_ids(&select(
                &mut engine,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE a > 1)"
            )),
            vec![1]
        );
    }

    #[test]
    fn scalar_subquery_over_an_aggregate_inner() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 10)",
            "INSERT INTO s VALUES (2, 30)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // `a = (SELECT MAX(a) FROM s)` → `a = 30` → row 3 (aggregate inner yields one row).
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a = (SELECT MAX(a) FROM s)",
        ));
        assert_eq!(got, vec![3]);
    }

    #[test]
    fn subquery_inherits_the_outer_statement_snapshot() {
        // STL-234 DoD oracle: an uncorrelated subquery is evaluated at the outer
        // statement's snapshot (docs/16 §6). Reading the integrated
        // `WHERE a IN (SELECT a FROM s)` at an `AS OF` instant must equal composing
        // two *independent* `AS OF` reads of `t` and `s` at that same instant — so
        // the inner can never leak the present into a time-travel read.
        let clock = SteppedClock::new(1_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }

        // Era 1 @ 2_000: t = {1:10, 2:20, 3:30}, s = {1:10, 2:30}.
        clock.set(2_000);
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 10)",
            "INSERT INTO s VALUES (2, 30)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert era1");
        }

        // Era 2 @ 5_000: rewrite s to {1:20, 2:20} — the inner result now picks t.id 2.
        clock.set(5_000);
        for sql in [
            "UPDATE s SET a = 20 WHERE id = 1",
            "UPDATE s SET a = 20 WHERE id = 2",
        ] {
            engine.execute(&parse_one(sql)).expect("update era2");
        }
        clock.set(9_000);

        let run = |engine: &mut SessionEngine<SteppedClock, MemDisk>, sql: &str| -> SelectResult {
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(sql)).expect("select") else {
                panic!("SELECT must return rows");
            };
            r
        };
        let int_at = |row: &Option<Vec<u8>>| -> Option<i32> {
            row.as_ref().map(
                |b| match ScalarValue::decode(LogicalType::Int4, b).expect("decode") {
                    ScalarValue::Int4(v) => v,
                    _ => panic!("INT column"),
                },
            )
        };

        // The integrated subquery at each instant must equal the composed reference.
        for at in [4_000_i64, 9_000] {
            let set: Vec<i32> = run(
                &mut engine,
                &format!("SELECT a FROM s FOR SYSTEM_TIME AS OF {at}"),
            )
            .rows
            .iter()
            .filter_map(|row| int_at(&row[0]))
            .collect();
            let mut want: Vec<i32> = run(
                &mut engine,
                &format!("SELECT id, a FROM t FOR SYSTEM_TIME AS OF {at}"),
            )
            .rows
            .iter()
            .filter(|row| int_at(&row[1]).is_some_and(|a| set.contains(&a)))
            .map(|row| int_at(&row[0]).expect("id"))
            .collect();
            want.sort_unstable();
            let got = subquery_ids(&run(
                &mut engine,
                &format!(
                    "SELECT id FROM t WHERE a IN (SELECT a FROM s) FOR SYSTEM_TIME AS OF {at}"
                ),
            ));
            assert_eq!(
                got, want,
                "AS OF {at}: integrated subquery vs composed reference"
            );
        }

        // And the two eras genuinely differ, so the test cannot pass by reading the
        // present at both: era 1 → {1, 3}, present → {2}.
        assert_eq!(
            subquery_ids(&run(
                &mut engine,
                "SELECT id FROM t WHERE a IN (SELECT a FROM s) FOR SYSTEM_TIME AS OF 4000",
            )),
            vec![1, 3]
        );
        assert_eq!(
            subquery_ids(&run(
                &mut engine,
                "SELECT id FROM t WHERE a IN (SELECT a FROM s) FOR SYSTEM_TIME AS OF 9000",
            )),
            vec![2]
        );
    }

    #[test]
    fn subquery_reads_its_own_uncommitted_writes_in_a_transaction() {
        // Read-your-own-writes ([STL-203]): an in-transaction subquery sees the
        // transaction's buffered writes, the same as the outer read.
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert t");

        let mut txn = engine.begin();
        engine
            .execute_in_txn(&parse_one("INSERT INTO s VALUES (1, 10)"), &mut txn)
            .expect("stage insert into s");
        // The buffered `s` row is visible to the subquery → the outer row matches.
        let StatementOutcome::Rows(result) = engine
            .execute_in_txn(
                &parse_one("SELECT id FROM t WHERE a IN (SELECT a FROM s)"),
                &mut txn,
            )
            .expect("select in txn")
        else {
            panic!("rows");
        };
        assert_eq!(subquery_ids(&result), vec![1]);
        engine.commit(txn).expect("commit");
    }

    #[test]
    fn subquery_inherits_the_outer_valid_time_snapshot() {
        // The `(sys, valid)` rule on the *valid* axis (docs/16 §6): an uncorrelated
        // subquery over a valid-time table inherits the outer `FOR VALID_TIME AS OF`
        // pin, so the inner reads the same valid slice. The rows have disjoint valid
        // windows, so each instant makes exactly one row live — the integrated
        // subquery must agree with composing two independent valid-`AS OF` reads.
        let mut engine =
            valid_time_acct(&[(1, 100, 10, 20), (2, 200, 30, 40), (3, 300, 50, i64::MAX)]);
        let int_at = |row: &Option<Vec<u8>>| -> Option<i32> {
            row.as_ref().map(
                |b| match ScalarValue::decode(LogicalType::Int4, b).expect("decode") {
                    ScalarValue::Int4(v) => v,
                    _ => panic!("INT column"),
                },
            )
        };
        for (at, want_len) in [(15_i64, 1), (35, 1), (55, 1)] {
            // Compose two independent valid-`AS OF` reads at the same instant: the
            // inner's live balance set, then the outer rows whose balance is in it.
            let set: Vec<i32> = select(
                &mut engine,
                &format!("SELECT balance FROM acct FOR VALID_TIME AS OF {at}"),
            )
            .rows
            .iter()
            .filter_map(|row| int_at(&row[0]))
            .collect();
            assert_eq!(set.len(), want_len, "the live valid slice at {at}");
            let mut want: Vec<i32> = select(
                &mut engine,
                &format!("SELECT id, balance FROM acct FOR VALID_TIME AS OF {at}"),
            )
            .rows
            .iter()
            .filter(|row| int_at(&row[1]).is_some_and(|b| set.contains(&b)))
            .map(|row| int_at(&row[0]).expect("id"))
            .collect();
            want.sort_unstable();
            // The integrated subquery, pinned at the same valid instant, must match.
            let got = subquery_ids(&select(
                &mut engine,
                &format!(
                    "SELECT id FROM acct WHERE balance IN (SELECT balance FROM acct) \
                     FOR VALID_TIME AS OF {at}"
                ),
            ));
            assert_eq!(
                got, want,
                "VALID AS OF {at}: integrated subquery vs composed"
            );
        }
    }

    // --- correlated subqueries ([STL-239]) -------------------------------------

    /// Two tables keyed by a **non-unique** `k`, so a correlation on `k` makes the
    /// inner return a set (more than one row per outer value) — the substrate for
    /// the correlated `IN` / scalar-cardinality tests, which `subquery_session`
    /// (unique `id`) cannot express.
    fn keyed_session() -> SessionEngine<ZeroClock, MemDisk> {
        let mut engine = session();
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        engine
    }

    #[test]
    fn correlated_exists_filters_per_outer_row() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 99)",
            "INSERT INTO s VALUES (3, 99)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // EXISTS keeps the outer rows whose id has a matching inner row (1, 3); the
        // correlation `s.id = t.id` is re-checked per outer row, not folded once.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
        ));
        assert_eq!(got, vec![1, 3]);
        // NOT EXISTS is the exact complement.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
        ));
        assert_eq!(got, vec![2]);
    }

    #[test]
    fn correlated_in_decorrelates_to_a_composite_semi_join() {
        // `a IN (SELECT a FROM s WHERE s.k = t.k)` folds onto a composite-key SEMI
        // join on `(k, a)` ([STL-337]). This checks the cases that distinguish the
        // join from a per-row fold: a key with *several* matching inner rows is kept
        // once (not once per match), and membership requires *both* components — a
        // right `k` with a wrong `a` (and a right `a` under the wrong `k`) drops.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, 100, 7)",
            "INSERT INTO t VALUES (3, 200, 8)",
            "INSERT INTO t VALUES (4, 200, 9)",
            "INSERT INTO t VALUES (5, 100, 8)",
            // The distinct inner `(k, a)` set is {(100,5), (100,6), (200,8)};
            // (100,5) appears twice — the multi-match dedup case.
            "INSERT INTO s VALUES (10, 100, 5)",
            "INSERT INTO s VALUES (11, 100, 5)",
            "INSERT INTO s VALUES (12, 100, 6)",
            "INSERT INTO s VALUES (13, 200, 8)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        //   id 1 (100,5): ∈ set, matches s twice → kept once (dedup)
        //   id 2 (100,7): a=7 ∉ {5,6} under k=100 → drop (right k, wrong a)
        //   id 3 (200,8): ∈ set                   → keep
        //   id 4 (200,9): a=9 ∉ {8}    under k=200 → drop
        //   id 5 (100,8): 8 is in s, but under k=200 not k=100 → drop (wrong k)
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![1, 3]);
    }

    #[test]
    fn decorrelated_in_drops_a_null_key_or_member() {
        // The composite key is NULL when *either* component is NULL, so the join's
        // NULL-never-matches rule reproduces `IN`'s 3VL ([STL-337]): a NULL correlation
        // key, a NULL outer membership value, and a NULL inner membership value all
        // fail to match — the same answers the per-row fold gives.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, NULL, 5)",
            "INSERT INTO t VALUES (3, 100, NULL)",
            "INSERT INTO t VALUES (4, 200, 7)",
            "INSERT INTO s VALUES (10, 100, 5)",
            "INSERT INTO s VALUES (11, 200, NULL)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        //   id 1 (100,5):    ∈ {(100,5)}                          → keep
        //   id 2 (NULL,5):   NULL key → empty group → 5 IN ()     → drop
        //   id 3 (100,NULL): NULL member → never TRUE             → drop
        //   id 4 (200,7):    inner for k=200 is {NULL} (excluded) → drop
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn decorrelated_not_in_drops_the_per_group_null_trap() {
        // `a NOT IN (SELECT a FROM s WHERE s.k = t.k)` folds onto a NULL-aware
        // composite ANTI join on `(k, a)` ([STL-346]). The trap a plain anti join
        // cannot express: a NULL membership value *anywhere* in an outer row's
        // correlation group makes `NOT IN` UNKNOWN for it, even though its composite
        // never matches.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, 200, 9)",
            "INSERT INTO s VALUES (10, 100, NULL)",
            "INSERT INTO s VALUES (11, 200, 8)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        //   id 1 (k=100): group {NULL} → `5 NOT IN (NULL)` UNKNOWN → drop (the
        //                 per-group NULL trap the post-anti pass catches)
        //   id 2 (k=200): group {8}    → `9 NOT IN (8)` TRUE       → keep
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![2]);
        // Plain `IN` ignores the NULL member: id 1's `5 IN (NULL)` is also not TRUE,
        // so it too is dropped, and id 2's `9 IN (8)` is false — neither matches.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k)",
        ));
        assert!(got.is_empty(), "got {got:?}");
    }

    #[test]
    fn decorrelated_not_in_keeps_the_complement_of_the_in_semi_join() {
        // The mirror of `correlated_in_decorrelates_to_a_composite_semi_join`: over a
        // fixture with no NULLs, `NOT IN` is the exact complement of `IN` on `(k, a)`
        // ([STL-346]). A right `k` with a wrong `a`, and a right `a` under the wrong
        // `k`, are kept (no `(k, a)` match); a row matching a member is dropped.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, 100, 7)",
            "INSERT INTO t VALUES (3, 200, 8)",
            "INSERT INTO t VALUES (4, 200, 9)",
            "INSERT INTO t VALUES (5, 100, 8)",
            // Distinct inner `(k, a)` set {(100,5), (100,6), (200,8)}; (100,5) twice.
            "INSERT INTO s VALUES (10, 100, 5)",
            "INSERT INTO s VALUES (11, 100, 5)",
            "INSERT INTO s VALUES (12, 100, 6)",
            "INSERT INTO s VALUES (13, 200, 8)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        //   id 1 (100,5): ∈ set                    → drop
        //   id 2 (100,7): 7 ∉ {5,6} under k=100     → keep (right k, wrong a)
        //   id 3 (200,8): ∈ set                    → drop
        //   id 4 (200,9): 9 ∉ {8}   under k=200     → keep
        //   id 5 (100,8): 8 only under k=200        → keep (wrong k)
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![2, 4, 5]);
    }

    #[test]
    fn decorrelated_not_in_keeps_empty_groups_and_drops_a_null_outer_value() {
        // The two non-trap drops/keeps the post-anti pass must get right ([STL-346]):
        // an empty correlation group keeps regardless of `t.a` (even NULL), and a NULL
        // `t.a` over a *non-empty* group drops (`NULL NOT IN (non-empty)` is UNKNOWN).
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, 300, 5)",
            "INSERT INTO t VALUES (3, 300, NULL)",
            "INSERT INTO t VALUES (4, 100, NULL)",
            "INSERT INTO t VALUES (5, NULL, NULL)",
            "INSERT INTO s VALUES (10, 100, 6)",
            "INSERT INTO s VALUES (11, 100, 7)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        //   id 1 (100,5):    group {6,7}, 5 ∉, no NULL → `5 NOT IN (6,7)` TRUE → keep
        //   id 2 (300,5):    empty group → `5 NOT IN ()` TRUE              → keep
        //   id 3 (300,NULL): empty group → `NULL NOT IN ()` TRUE          → keep
        //   id 4 (100,NULL): non-empty group → `NULL NOT IN (6,7)` UNKNOWN → drop
        //   id 5 (NULL,*):   NULL key → empty group → TRUE                → keep
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![1, 2, 3, 5]);
    }

    #[test]
    fn decorrelated_in_handles_a_computed_membership() {
        // The `IN` membership may be a *computed* projection (type-checked to the
        // outer column's type), not just a bare column. It still decorrelates: the
        // binder preserves the computed value at inner result position 0 and appends
        // the correlation key at 1, and the composite join reads the computed value
        // as the membership component ([STL-337]).
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 6)",
            "INSERT INTO t VALUES (2, 100, 9)",
            "INSERT INTO s VALUES (10, 100, 5)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // For k=100 the inner membership set is {s.a + 1} = {6}: id 1 (a=6) ∈ {6} →
        // keep; id 2 (a=9) ∉ → drop.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a IN (SELECT a + 1 FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn correlated_scalar_lookup_per_row() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 10)",
            "INSERT INTO s VALUES (2, 99)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // `a = (SELECT a FROM s WHERE s.id = t.id)`, one inner row per outer key:
        //   id 1: inner 10 = 10 → keep
        //   id 2: inner 99 ≠ 20 → drop
        //   id 3: inner empty → NULL → unknown → drop
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE a = (SELECT a FROM s WHERE s.id = t.id)",
        ));
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn correlated_scalar_more_than_one_row_is_cardinality_violation() {
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO s VALUES (10, 100, 5)",
            "INSERT INTO s VALUES (11, 100, 6)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // The inner for k=100 yields two rows used as a scalar → SQLSTATE 21000,
        // raised per outer row exactly as the uncorrelated cardinality check does.
        let err = engine
            .execute(&parse_one(
                "SELECT id FROM t WHERE a = (SELECT a FROM s WHERE s.k = t.k)",
            ))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ScalarSubqueryCardinality),
            "got {err:?}"
        );
    }

    #[test]
    fn correlated_null_outer_key_yields_an_empty_inner() {
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, NULL, 5)",
            "INSERT INTO t VALUES (2, 100, 5)",
            "INSERT INTO s VALUES (10, 100, 1)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // A NULL correlation value makes `s.k = NULL` unknown for every inner row →
        // the inner is empty without a re-run: EXISTS drops id 1, keeps id 2.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![2]);
        // NOT EXISTS over the same empty inner keeps the NULL-key row (id 1).
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn decorrelated_exists_dedups_multiple_inner_matches() {
        // The set-based semi / anti decorrelation ([STL-317]) must agree with the
        // per-row reference on the cases that distinguish a join from a fold: a key
        // with *several* matching inner rows is kept exactly once (not once per
        // match), a key with none is dropped, and a NULL key follows
        // `empty_inner_keeps`.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 0)",
            "INSERT INTO t VALUES (2, 200, 0)",
            "INSERT INTO t VALUES (3, 300, 0)",
            "INSERT INTO t VALUES (4, NULL, 0)",
            // k=100 has two inner rows (the multi-match dedup case), k=200 one,
            // k=300 none.
            "INSERT INTO s VALUES (10, 100, 0)",
            "INSERT INTO s VALUES (11, 100, 0)",
            "INSERT INTO s VALUES (12, 200, 0)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // SEMI: keep the outer rows whose `k` is in the inner key set {100, 200},
        // each once despite k=100's two matches; drop k=300 (no match) and the NULL.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![1, 2]);
        // ANTI: the exact complement — the unmatched key and the NULL key survive.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k)",
        ));
        assert_eq!(got, vec![3, 4]);
    }

    #[test]
    fn self_correlated_subquery_resolves_through_the_alias() {
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // The same table on both sides: the inner alias `t2` and the (aliased) outer
        // `t1` disambiguate the two scopes (an alias hides the table name). Keeps the
        // rows that are not the maximum `a` — each has some larger peer.
        let got = subquery_ids(&select(
            &mut engine,
            "SELECT id FROM t t1 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.a > t1.a)",
        ));
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn correlated_subquery_rejects_an_unsupported_correlation_shape() {
        let mut engine = subquery_session();
        // The outer reference is paired with an inner *arithmetic*, not a bare inner
        // column — recognized as correlated but not a shape the per-row fallback
        // lowers, so it is rejected rather than mis-bound.
        let err = engine
            .execute(&parse_one(
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.a + 1 = t.a)",
            ))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Select(SelectError::Subquery(_))),
            "got {err:?}"
        );
    }

    #[test]
    fn correlated_subquery_inherits_the_outer_statement_snapshot() {
        // STL-239 DoD oracle (the temporal heart, docs/16 §6): a correlated subquery
        // is re-run at the *outer statement's* snapshot, so reading the integrated
        // `WHERE EXISTS (… s.id = t.id)` at an `AS OF` instant equals composing two
        // independent `AS OF` reads of `t` and `s` at that same instant — the inner
        // can never leak the present into a time-travel read.
        let clock = SteppedClock::new(1_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }

        // Era 1 @ 2_000: t = {1, 2, 3}; s carries ids {1, 3}.
        clock.set(2_000);
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 0)",
            "INSERT INTO s VALUES (3, 0)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert era1");
        }

        // Era 2 @ 5_000: s now carries ids {2, 3} — the EXISTS answer must shift.
        clock.set(5_000);
        for sql in ["DELETE FROM s WHERE id = 1", "INSERT INTO s VALUES (2, 0)"] {
            engine.execute(&parse_one(sql)).expect("update era2");
        }
        clock.set(9_000);

        let run = |engine: &mut SessionEngine<SteppedClock, MemDisk>, sql: &str| -> SelectResult {
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(sql)).expect("select") else {
                panic!("SELECT must return rows");
            };
            r
        };
        let ids = |result: &SelectResult| -> Vec<i32> {
            let mut out: Vec<i32> = result
                .rows
                .iter()
                .map(|row| {
                    let bytes = row[0].as_ref().expect("id");
                    match ScalarValue::decode(LogicalType::Int4, bytes).expect("decode") {
                        ScalarValue::Int4(v) => v,
                        _ => panic!("INT"),
                    }
                })
                .collect();
            out.sort_unstable();
            out
        };

        for at in [4_000_i64, 9_000] {
            // Compose the reference: the inner's id set, then the outer ids in it.
            let s_ids = ids(&run(
                &mut engine,
                &format!("SELECT id FROM s FOR SYSTEM_TIME AS OF {at}"),
            ));
            let mut want: Vec<i32> = ids(&run(
                &mut engine,
                &format!("SELECT id FROM t FOR SYSTEM_TIME AS OF {at}"),
            ))
            .into_iter()
            .filter(|id| s_ids.contains(id))
            .collect();
            want.sort_unstable();
            let got = subquery_ids(&run(
                &mut engine,
                &format!(
                    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id) \
                     FOR SYSTEM_TIME AS OF {at}"
                ),
            ));
            assert_eq!(
                got, want,
                "AS OF {at}: correlated EXISTS vs composed reference"
            );
        }

        // And the two eras genuinely differ, so the test cannot pass by reading the
        // present at both: era 1 → {1, 3}, present → {2, 3}.
        assert_eq!(
            subquery_ids(&run(
                &mut engine,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id) \
                 FOR SYSTEM_TIME AS OF 4000",
            )),
            vec![1, 3]
        );
        assert_eq!(
            subquery_ids(&run(
                &mut engine,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id) \
                 FOR SYSTEM_TIME AS OF 9000",
            )),
            vec![2, 3]
        );
    }

    #[test]
    fn decorrelated_in_inherits_the_outer_statement_snapshot() {
        // STL-337 DoD (docs/16 §6): the composite-key IN decorrelation runs the inner
        // once at the *outer statement's* snapshot, so the one consistent `(sys,
        // valid)` snapshot holds across both join inputs — an `AS OF` read can never
        // leak the present into the membership set. The two eras give genuinely
        // different answers, so reading the present at either would fail.
        let clock = SteppedClock::new(1_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        // Era 1 @ 2_000: s's `(k, a)` set is {(100,5), (200,9)}.
        clock.set(2_000);
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, 100, 6)",
            "INSERT INTO t VALUES (3, 200, 7)",
            "INSERT INTO s VALUES (10, 100, 5)",
            "INSERT INTO s VALUES (11, 200, 9)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert era1");
        }
        // Era 2 @ 5_000: (100,5) leaves, (100,6) joins — so the IN answer shifts from
        // t id 1 to t id 2.
        clock.set(5_000);
        for sql in [
            "DELETE FROM s WHERE id = 10",
            "INSERT INTO s VALUES (12, 100, 6)",
        ] {
            engine.execute(&parse_one(sql)).expect("update era2");
        }
        clock.set(9_000);

        let at = |engine: &mut SessionEngine<SteppedClock, MemDisk>, instant: i64| -> Vec<i32> {
            let sql = format!(
                "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k) \
                 FOR SYSTEM_TIME AS OF {instant}"
            );
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select")
            else {
                panic!("SELECT must return rows");
            };
            subquery_ids(&r)
        };
        // Era 1: only t id 1 (100,5) is in s's set; era 2 / present: only t id 2 (100,6).
        assert_eq!(at(&mut engine, 4_000), vec![1]);
        assert_eq!(at(&mut engine, 9_000), vec![2]);
    }

    #[test]
    fn decorrelated_not_in_inherits_the_outer_statement_snapshot() {
        // STL-346 DoD (docs/16 §6): the composite anti-join `NOT IN` decorrelation
        // runs the inner once at the *outer statement's* snapshot, so the one
        // consistent `(sys, valid)` snapshot holds across both join inputs *and* the
        // per-group NULL tracking is built from the snapshot's inner set, not the
        // present. The two eras give genuinely different answers, so reading the
        // present at either would fail. Same fixture as the `IN` oracle; `NOT IN` is
        // its complement under these no-NULL groups.
        let clock = SteppedClock::new(1_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }
        // Era 1 @ 2_000: s's `(k, a)` set is {(100,5), (200,9)}.
        clock.set(2_000);
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO t VALUES (2, 100, 6)",
            "INSERT INTO t VALUES (3, 200, 7)",
            "INSERT INTO s VALUES (10, 100, 5)",
            "INSERT INTO s VALUES (11, 200, 9)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert era1");
        }
        // Era 2 @ 5_000: (100,5) leaves, (100,6) joins.
        clock.set(5_000);
        for sql in [
            "DELETE FROM s WHERE id = 10",
            "INSERT INTO s VALUES (12, 100, 6)",
        ] {
            engine.execute(&parse_one(sql)).expect("update era2");
        }
        clock.set(9_000);

        let at = |engine: &mut SessionEngine<SteppedClock, MemDisk>, instant: i64| -> Vec<i32> {
            let sql = format!(
                "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.k = t.k) \
                 FOR SYSTEM_TIME AS OF {instant}"
            );
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(&sql)).expect("select")
            else {
                panic!("SELECT must return rows");
            };
            subquery_ids(&r)
        };
        // Era 1 groups k=100→{5}, k=200→{9}: id 1 `5 NOT IN {5}` drops; id 2 `6 NOT IN
        // {5}` and id 3 `7 NOT IN {9}` keep → [2, 3].
        assert_eq!(at(&mut engine, 4_000), vec![2, 3]);
        // Present groups k=100→{6}, k=200→{9}: id 1 `5 NOT IN {6}` keeps; id 2 `6 NOT
        // IN {6}` drops; id 3 keeps → [1, 3].
        assert_eq!(at(&mut engine, 9_000), vec![1, 3]);
    }

    #[test]
    fn correlated_subquery_filters_before_an_outer_aggregate() {
        // The per-row correlated filter runs *before* grouping, so an aggregate over
        // a correlated `WHERE` counts only the rows the subquery kept ([STL-171]).
        let mut engine = subquery_session();
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 0)",
            "INSERT INTO s VALUES (3, 0)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        // Only ids 1 and 3 have a matching `s` row → COUNT(*) = 2.
        let result = select(
            &mut engine,
            "SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
        );
        let ScalarValue::Int8(count) = ScalarValue::decode(
            LogicalType::Int8,
            result.rows[0][0].as_ref().expect("count"),
        )
        .expect("decode count") else {
            panic!("COUNT is INT8");
        };
        assert_eq!(count, 2);
    }

    #[test]
    fn correlated_subquery_reads_its_own_uncommitted_writes() {
        // Read-your-own-writes ([STL-203]): a correlated inner re-run inside a
        // transaction sees that transaction's buffered writes, just like the outer.
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert t");

        let mut txn = engine.begin();
        engine
            .execute_in_txn(&parse_one("INSERT INTO s VALUES (1, 99)"), &mut txn)
            .expect("stage insert into s");
        let StatementOutcome::Rows(result) = engine
            .execute_in_txn(
                &parse_one("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)"),
                &mut txn,
            )
            .expect("select in txn")
        else {
            panic!("rows");
        };
        assert_eq!(subquery_ids(&result), vec![1]);
        engine.commit(txn).expect("commit");
    }

    // ---- STL-303: expression select items + scalar subqueries in the SELECT list --

    /// Decode one **present** projected integer cell to `i64` (int4 / int8 widen to
    /// it) — the STL-303 oracle projects only integer expressions, so a present cell
    /// is always an integer. Callers map over the `Option` for the `NULL` cell.
    fn int_cell(ty: LogicalType, bytes: &[u8]) -> i64 {
        match ScalarValue::decode(ty, bytes).expect("decode integer cell") {
            ScalarValue::Int4(v) => i64::from(v),
            ScalarValue::Int8(v) => v,
            // Name the type, not the value (the CodeQL cleartext-logging heuristic).
            other => panic!("expected an integer cell, got {:?}", other.logical_type()),
        }
    }

    /// Every row of `result` as `Vec<Option<i64>>`, decoding each present cell by its
    /// column type — the shape the STL-303 projection oracle compares against a
    /// reference (`None` is a SQL `NULL` cell).
    fn int_rows(result: &SelectResult) -> Vec<Vec<Option<i64>>> {
        result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .zip(&result.columns)
                    .map(|(cell, (_, ty))| cell.as_ref().map(|bytes| int_cell(*ty, bytes)))
                    .collect()
            })
            .collect()
    }

    /// The output column names of a result, for asserting projection headers.
    fn column_names(result: &SelectResult) -> Vec<&str> {
        result.columns.iter().map(|(n, _)| n.as_str()).collect()
    }

    #[test]
    fn projected_expressions_match_an_in_process_reference() {
        // In-process oracle ([STL-303]): a bare column, an arithmetic expression, a
        // constant literal, and an uncorrelated scalar subquery, projected in one
        // SELECT, must match a hand-computed Rust reference over the inserted rows —
        // the DoD shape `SELECT a, (SELECT max(b) FROM s), a + 1 AS plus FROM t`.
        let mut engine = subquery_session();
        let t: &[(i32, i32)] = &[(1, 10), (2, 20), (3, 30)];
        let s: &[(i32, i32)] = &[(4, 5), (5, 25)];
        for (id, a) in t {
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES ({id}, {a})")))
                .expect("insert t");
        }
        for (id, a) in s {
            engine
                .execute(&parse_one(&format!("INSERT INTO s VALUES ({id}, {a})")))
                .expect("insert s");
        }

        let result = select(
            &mut engine,
            "SELECT id, a + 1 AS plus, 7 AS seven, (SELECT max(a) FROM s) AS m FROM t ORDER BY id",
        );
        assert_eq!(
            column_names(&result),
            vec!["id", "plus", "seven", "m"],
            "bare column, then three aliased computed/subquery items"
        );

        let max_s = s.iter().map(|&(_, a)| i64::from(a)).max();
        let want: Vec<Vec<Option<i64>>> = t
            .iter()
            .map(|&(id, a)| vec![Some(i64::from(id)), Some(i64::from(a) + 1), Some(7), max_s])
            .collect();
        assert_eq!(int_rows(&result), want);
    }

    #[test]
    fn bare_literal_projection_broadcasts_with_the_postgres_fallback_name() {
        // `SELECT 1 FROM t` projects a constant column on every row; an unaliased
        // computed item takes the Postgres `?column?` fallback name.
        let mut engine = subquery_session();
        for id in [1, 2] {
            engine
                .execute(&parse_one(&format!(
                    "INSERT INTO t VALUES ({id}, {})",
                    id * 10
                )))
                .expect("insert");
        }
        let result = select(&mut engine, "SELECT 1 FROM t");
        assert_eq!(column_names(&result), vec!["?column?"]);
        assert_eq!(int_rows(&result), vec![vec![Some(1)], vec![Some(1)]]);
    }

    #[test]
    fn projected_arithmetic_propagates_null() {
        // A NULL value cell makes the arithmetic NULL for that row (3VL), exactly as
        // the WHERE evaluator treats it — never a silent 0 or a dropped row.
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (2, NULL)"))
            .expect("insert null");

        let result = select(&mut engine, "SELECT id, a + 1 AS plus FROM t ORDER BY id");
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), Some(11)], vec![Some(2), None]]
        );
    }

    #[test]
    fn projected_scalar_subquery_inherits_the_inner_column_name() {
        // An unaliased scalar subquery takes the inner's sole output column name (the
        // Postgres rule), compared against binding the inner standalone so the test
        // never hardcodes the aggregate's naming.
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert t");
        engine
            .execute(&parse_one("INSERT INTO s VALUES (4, 25)"))
            .expect("insert s");

        let expected = select(&mut engine, "SELECT max(a) FROM s").columns[0]
            .0
            .clone();
        let outer = select(&mut engine, "SELECT id, (SELECT max(a) FROM s) FROM t");
        assert_eq!(outer.columns[1].0, expected);
    }

    #[test]
    fn projected_scalar_subquery_with_no_row_is_null() {
        // An empty inner makes the projected scalar SQL NULL on every outer row (the
        // zero-row branch of the cardinality rule).
        let mut engine = subquery_session();
        for id in [1, 2] {
            engine
                .execute(&parse_one(&format!(
                    "INSERT INTO t VALUES ({id}, {})",
                    id * 10
                )))
                .expect("insert");
        }
        // `s` stays empty, so the inner returns no row.
        let result = select(
            &mut engine,
            "SELECT id, (SELECT a FROM s WHERE id = 99) AS m FROM t ORDER BY id",
        );
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), None], vec![Some(2), None]]
        );
    }

    #[test]
    fn projected_scalar_subquery_returning_many_rows_is_cardinality_violation() {
        // More than one inner row used as a projected scalar is SQLSTATE 21000 — and
        // because the uncorrelated inner resolves once up front, it fires even when
        // the outer produces no rows (the Postgres InitPlan posture).
        let mut engine = subquery_session();
        // `t` stays empty; `s` has two rows.
        engine
            .execute(&parse_one("INSERT INTO s VALUES (1, 10)"))
            .expect("insert");
        engine
            .execute(&parse_one("INSERT INTO s VALUES (2, 20)"))
            .expect("insert");
        let err = engine
            .execute(&parse_one("SELECT id, (SELECT a FROM s) FROM t"))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ScalarSubqueryCardinality),
            "got {err:?}"
        );
    }

    #[test]
    fn projected_computed_column_orders_and_dedups() {
        // ORDER BY and DISTINCT resolve over a *computed* output column: ORDER BY on
        // an aliased column sorts by its value (fast path), and DISTINCT deduplicates
        // the projected computed rows.
        let mut engine = subquery_session();
        for (id, a) in [(1, 30), (2, 10), (3, 20), (4, 10)] {
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES ({id}, {a})")))
                .expect("insert");
        }
        // ORDER BY the alias `v` (= a), ties broken by id → ids 2, 4, 3, 1.
        let asc = select(&mut engine, "SELECT id, a AS v FROM t ORDER BY v, id");
        let ids: Vec<i64> = int_rows(&asc)
            .iter()
            .map(|row| row[0].expect("id"))
            .collect();
        assert_eq!(ids, vec![2, 4, 3, 1]);
        // DISTINCT over a computed column: a + 1 → {31, 11, 21, 11} → {11, 21, 31}.
        let distinct = select(&mut engine, "SELECT DISTINCT a + 1 AS v FROM t ORDER BY v");
        assert_eq!(
            int_rows(&distinct),
            vec![vec![Some(11)], vec![Some(21)], vec![Some(31)]]
        );
    }

    #[test]
    fn computed_column_coexists_with_a_provenance_pseudo_column() {
        // A computed expression projected alongside a provenance pseudo-column
        // ([STL-247]): the read materializes provenance (widening each row), and the
        // virtual computed column is appended after it without disturbing the layout.
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert");
        let result = select(&mut engine, "SELECT a + 1 AS plus, _stele_txn_id FROM t");
        assert_eq!(column_names(&result), vec!["plus", "_stele_txn_id"]);
        assert_eq!(
            result.rows[0][0]
                .as_ref()
                .map(|bytes| int_cell(result.columns[0].1, bytes)),
            Some(11)
        );
        assert!(
            result.rows[0][1].is_some(),
            "the provenance txn id is materialized"
        );
    }

    #[test]
    fn projected_correlated_subquery_matches_an_in_process_reference() {
        // In-process oracle ([STL-331], the DoD shape `SELECT a, (SELECT b FROM s
        // WHERE s.id = t.id) FROM t`): the projected correlated scalar is re-evaluated
        // per outer row, so each cell is the matching inner value — a *different* value
        // per row, which an uncorrelated broadcast could never produce — or SQL `NULL`
        // when no inner row matches.
        let mut engine = subquery_session();
        let t: &[(i32, i32)] = &[(1, 10), (2, 20), (3, 30)];
        let s: &[(i32, i32)] = &[(1, 100), (3, 300)]; // id 2 has no matching inner row.
        for (id, a) in t {
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES ({id}, {a})")))
                .expect("insert t");
        }
        for (id, a) in s {
            engine
                .execute(&parse_one(&format!("INSERT INTO s VALUES ({id}, {a})")))
                .expect("insert s");
        }

        let result = select(
            &mut engine,
            "SELECT id, (SELECT a FROM s WHERE s.id = t.id) AS m FROM t ORDER BY id",
        );
        assert_eq!(column_names(&result), vec!["id", "m"]);
        // Reference: for each outer id, the `s.a` whose `s.id` equals it, else NULL.
        let want: Vec<Vec<Option<i64>>> = t
            .iter()
            .map(|&(id, _)| {
                let m = s
                    .iter()
                    .find(|&&(sid, _)| sid == id)
                    .map(|&(_, a)| i64::from(a));
                vec![Some(i64::from(id)), m]
            })
            .collect();
        assert_eq!(int_rows(&result), want);
    }

    #[test]
    fn projected_correlated_subquery_more_than_one_row_is_cardinality_violation() {
        // More than one inner row for an outer key, used as a projected scalar, is
        // SQLSTATE 21000 — raised per outer row exactly as the uncorrelated and the
        // correlated-WHERE cardinality checks do.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, 100, 5)",
            "INSERT INTO s VALUES (10, 100, 1)",
            "INSERT INTO s VALUES (11, 100, 2)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        let err = engine
            .execute(&parse_one(
                "SELECT id, (SELECT a FROM s WHERE s.k = t.k) FROM t",
            ))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ScalarSubqueryCardinality),
            "got {err:?}"
        );
    }

    #[test]
    fn projected_correlated_subquery_null_correlation_value_is_null() {
        // A NULL correlation value makes `s.k = NULL` unknown for every inner row, so
        // the projected scalar is SQL NULL without a re-run — distinct from a matching
        // row, which still resolves to its value.
        let mut engine = keyed_session();
        for sql in [
            "INSERT INTO t VALUES (1, NULL, 0)",
            "INSERT INTO t VALUES (2, 100, 0)",
            "INSERT INTO s VALUES (10, 100, 42)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert");
        }
        let result = select(
            &mut engine,
            "SELECT id, (SELECT a FROM s WHERE s.k = t.k) AS m FROM t ORDER BY id",
        );
        // id 1: NULL key → NULL; id 2: k=100 → the inner's a = 42.
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), None], vec![Some(2), Some(42)]]
        );
    }

    #[test]
    fn projected_correlated_subquery_inherits_the_outer_statement_snapshot() {
        // The temporal heart (docs/16 §6): a correlated SELECT-list subquery is re-run
        // at the *outer statement's* snapshot, so its projected value under
        // `FOR SYSTEM_TIME AS OF t` equals an independent `AS OF t` read of the inner —
        // the per-row re-execution can never leak the present into a time-travel read.
        let clock = SteppedClock::new(1_000);
        let mut engine = SessionEngine::open(MemDisk::new(), clock.clone());
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("create");
        }

        // Era 1 @ 2_000: t = {1, 2, 3}; s carries ids {1, 2} with a = {100, 200}.
        clock.set(2_000);
        for sql in [
            "INSERT INTO t VALUES (1, 10)",
            "INSERT INTO t VALUES (2, 20)",
            "INSERT INTO t VALUES (3, 30)",
            "INSERT INTO s VALUES (1, 100)",
            "INSERT INTO s VALUES (2, 200)",
        ] {
            engine.execute(&parse_one(sql)).expect("insert era1");
        }

        // Era 2 @ 5_000: id 1's value changes, id 2 leaves, id 3 appears — every
        // projected lookup must shift with it.
        clock.set(5_000);
        for sql in [
            "UPDATE s SET a = 111 WHERE id = 1",
            "DELETE FROM s WHERE id = 2",
            "INSERT INTO s VALUES (3, 300)",
        ] {
            engine.execute(&parse_one(sql)).expect("update era2");
        }
        clock.set(9_000);

        let rows_of = |engine: &mut SessionEngine<SteppedClock, MemDisk>,
                       sql: &str|
         -> SelectResult {
            let StatementOutcome::Rows(r) = engine.execute(&parse_one(sql)).expect("select") else {
                panic!("SELECT must return rows");
            };
            r
        };

        for at in [4_000_i64, 9_000] {
            // The outer ids visible at `at`, ascending.
            let t_rows = rows_of(
                &mut engine,
                &format!("SELECT id FROM t FOR SYSTEM_TIME AS OF {at}"),
            );
            let mut ids: Vec<i32> = t_rows
                .rows
                .iter()
                .map(|row| {
                    match ScalarValue::decode(LogicalType::Int4, row[0].as_ref().expect("id"))
                        .expect("decode id")
                    {
                        ScalarValue::Int4(v) => v,
                        _ => panic!("id is INT"),
                    }
                })
                .collect();
            ids.sort_unstable();
            // Reference: pair each id with an independent `AS OF at` read of the inner.
            let want: Vec<Vec<Option<i64>>> =
                ids.iter()
                    .map(|&id| {
                        let inner = rows_of(
                            &mut engine,
                            &format!("SELECT a FROM s WHERE id = {id} FOR SYSTEM_TIME AS OF {at}"),
                        );
                        let m = inner.rows.first().and_then(|row| {
                            row[0].as_ref().map(|b| int_cell(inner.columns[0].1, b))
                        });
                        vec![Some(i64::from(id)), m]
                    })
                    .collect();
            // The integrated correlated projection, read at the same instant.
            let got = int_rows(&rows_of(
                &mut engine,
                &format!(
                    "SELECT id, (SELECT a FROM s WHERE s.id = t.id) AS m FROM t \
                     FOR SYSTEM_TIME AS OF {at} ORDER BY id"
                ),
            ));
            assert_eq!(
                got, want,
                "AS OF {at}: correlated projection vs composed reference"
            );
        }

        // And the eras genuinely differ, so the AS OF 4000 read cannot pass by reading
        // the present: era 1 projects {100, 200, NULL}, the present {111, NULL, 300}.
        assert_eq!(
            int_rows(&rows_of(
                &mut engine,
                "SELECT id, (SELECT a FROM s WHERE s.id = t.id) AS m FROM t \
                 FOR SYSTEM_TIME AS OF 4000 ORDER BY id",
            )),
            vec![
                vec![Some(1), Some(100)],
                vec![Some(2), Some(200)],
                vec![Some(3), None],
            ],
        );
    }

    #[test]
    fn multi_column_computed_projection_evaluates() {
        // A computed select item over more than one column ([STL-332]): `k + a` adds
        // the two value columns per row. This lifts the STL-303 single-anchor
        // restriction the test used to pin.
        let mut engine = keyed_session();
        for (id, k, a) in [(1, 100, 5), (2, 200, 7)] {
            engine
                .execute(&parse_one(&format!(
                    "INSERT INTO t VALUES ({id}, {k}, {a})"
                )))
                .expect("insert");
        }
        let result = select(&mut engine, "SELECT id, k + a AS sum FROM t ORDER BY id");
        assert_eq!(column_names(&result), vec!["id", "sum"]);
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), Some(105)], vec![Some(2), Some(207)]]
        );
    }

    #[test]
    fn multi_column_computed_projection_propagates_null() {
        // A NULL in either column makes the whole arithmetic NULL for that row (3VL),
        // exactly as a single-column computed expression does ([STL-332]).
        let mut engine = keyed_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 100, 5)"))
            .expect("insert");
        engine
            .execute(&parse_one("INSERT INTO t VALUES (2, NULL, 7)"))
            .expect("insert null k");
        let result = select(&mut engine, "SELECT id, k + a AS sum FROM t ORDER BY id");
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), Some(105)], vec![Some(2), None]]
        );
    }

    #[test]
    fn column_free_arithmetic_projection_evaluates() {
        // Column-free arithmetic ([STL-332]): `1 + 2` is no longer rejected — it
        // broadcasts its computed value (3) on every row, evaluated by the same
        // arithmetic kernel the per-row path uses (so div-by-zero etc. stay
        // consistent), not folded by a separate bind-time path.
        let mut engine = keyed_session();
        for id in [1, 2] {
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES ({id}, 0, 0)")))
                .expect("insert");
        }
        let result = select(&mut engine, "SELECT id, 1 + 2 AS three FROM t ORDER BY id");
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), Some(3)], vec![Some(2), Some(3)]]
        );
    }

    #[test]
    fn embedded_scalar_subquery_in_arithmetic_resolves_once() {
        // A scalar subquery embedded inside an arithmetic expression ([STL-332]):
        // `a + (SELECT max(a) FROM s)` resolves the inner once and adds it to each
        // outer row's `a`. `s` holds {5, 25}, so max is 25.
        let mut engine = keyed_session();
        for (id, a) in [(1, 10), (2, 20)] {
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES ({id}, 0, {a})")))
                .expect("insert t");
        }
        for (id, a) in [(10, 5), (11, 25)] {
            engine
                .execute(&parse_one(&format!("INSERT INTO s VALUES ({id}, 0, {a})")))
                .expect("insert s");
        }
        let result = select(
            &mut engine,
            "SELECT id, a + (SELECT max(a) FROM s) AS shifted FROM t ORDER BY id",
        );
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), Some(35)], vec![Some(2), Some(45)]]
        );
    }

    #[test]
    fn embedded_scalar_subquery_null_propagates_through_arithmetic() {
        // An embedded subquery that resolves to SQL NULL makes the whole arithmetic
        // NULL for every row ([STL-332]) — NULL propagates through arithmetic (3VL).
        // `s` is empty, so `max(a)` is NULL.
        let mut engine = keyed_session();
        for id in [1, 2] {
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES ({id}, 0, {id})")))
                .expect("insert t");
        }
        let result = select(
            &mut engine,
            "SELECT id, a + (SELECT max(a) FROM s) AS shifted FROM t ORDER BY id",
        );
        assert_eq!(
            int_rows(&result),
            vec![vec![Some(1), None], vec![Some(2), None]]
        );
    }

    #[test]
    fn embedded_scalar_subquery_many_rows_is_cardinality_violation() {
        // An embedded subquery returning more than one row is SQLSTATE 21000, the same
        // rule a whole-projection scalar subquery enforces ([STL-303]/[STL-332]); the
        // uncorrelated inner resolves once up front, so it fires even with no outer row.
        let mut engine = keyed_session();
        // `t` stays empty; `s` has two rows, so a bare `SELECT a FROM s` is >1 row.
        for (id, a) in [(10, 5), (11, 25)] {
            engine
                .execute(&parse_one(&format!("INSERT INTO s VALUES ({id}, 0, {a})")))
                .expect("insert s");
        }
        let err = engine
            .execute(&parse_one("SELECT id, a + (SELECT a FROM s) FROM t"))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::ScalarSubqueryCardinality),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_projection_rejects_a_cross_type_operand() {
        // Stele does not implicitly coerce ([STL-332]): an integer column added to a
        // string literal is a bind-time error, not a silent cast or a per-row failure.
        let mut engine = keyed_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 100, 5)"))
            .expect("insert");
        let err = engine
            .execute(&parse_one("SELECT id, a + 'x' FROM t"))
            .unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Select(SelectError::UnsupportedProjection(_))
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_projection_reconciles_int4_and_int8_operands() {
        // No implicit coercion ([STL-332]): an `int4` *literal* widens to meet an
        // `int8` column (`big + 1` is `int8`), but two concrete columns of different
        // integer widths (`small + big`) do not coerce — that is a bind error.
        let mut engine = session();
        engine
            .execute(&parse_one(
                "CREATE TABLE w (id INT PRIMARY KEY, small INT, big BIGINT) WITH SYSTEM VERSIONING",
            ))
            .expect("create");
        engine
            .execute(&parse_one("INSERT INTO w VALUES (1, 5, 5000000000)"))
            .expect("insert");
        // `int8` column + `int4` literal: the literal widens, the result is `int8`.
        let result = select(&mut engine, "SELECT id, big + 1 AS c FROM w");
        assert_eq!(result.columns[1].1, LogicalType::Int8);
        assert_eq!(int_rows(&result), vec![vec![Some(1), Some(5_000_000_001)]]);
        // `int4` column + `int8` column: two concrete operands, no coercion → rejected.
        let err = engine
            .execute(&parse_one("SELECT id, small + big FROM w"))
            .unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Select(SelectError::UnsupportedProjection(_))
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_projection_rejects_a_provenance_pseudo_column() {
        // A provenance pseudo-column ([STL-247]) inside a *computed* expression is
        // rejected at bind time: the engine's `eval_projection_scalar` decodes schema
        // columns only, so allowing it would fail at runtime. It stays projectable
        // only as a bare column (Copilot review on PR #207).
        let mut engine = subquery_session();
        engine
            .execute(&parse_one("INSERT INTO t VALUES (1, 10)"))
            .expect("insert");
        let err = engine
            .execute(&parse_one("SELECT id, _stele_txn_id + 1 FROM t"))
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Select(SelectError::UnknownColumn { .. })),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_projection_over_an_empty_provenance_read_does_not_panic() {
        // Regression (Copilot review on PR #207): a computed projection that also
        // names a provenance pseudo-column over an *empty* table, with DISTINCT, must
        // not index the column metadata out of range — the empty-rows width falls
        // back to the full addressable set when provenance is referenced.
        let mut engine = subquery_session();
        // `t` stays empty.
        let result = select(
            &mut engine,
            "SELECT DISTINCT _stele_principal, a + 1 AS plus FROM t",
        );
        assert_eq!(column_names(&result), vec!["_stele_principal", "plus"]);
        assert!(result.rows.is_empty(), "an empty table projects no rows");
    }
}
