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

mod catalog_log;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::catalog_log::CatalogRecord;

use stele_catalog::{Catalog, CatalogError};
use stele_common::period::Interval;
use stele_common::provenance::{Principal, TxnId};
use stele_common::row_codec::{self, RowCodecError};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};
use stele_exec::{
    AggregateFunc as ExecAggregateFunc, Aggregator, Batch, CmpOp, Column, DEFAULT_BATCH_SIZE,
    ExplodePayload, Expr, Filter, JoinType as ExecJoinType, Operator, ScanError, ScanSource,
    SnapshotScan, Vector, evaluate, hash_aggregate, hash_join,
};
use stele_sql::ddl::{DdlOutcome, DdlStatement};
use stele_sql::dml::{BoundDml, DmlError};
use stele_sql::select::{
    AggregateFunc, BoundAggregate, BoundJoin, BoundPeriod, BoundPeriodPredicate, BoundPredicate,
    BoundSelect, JoinColumnRef, JoinType, OutputItem, PeriodEndpoint, Projection, SelectError,
};
use stele_sql::{BindContext, BindError, Statement, bind_ddl, bind_dml, bind_select};
use stele_storage::backend::Disk;
use stele_storage::delta::{BusinessKey, Snapshot};
use stele_storage::dml::DmlOutcome;
use stele_storage::engine::{Engine, EngineError as StorageError};
use stele_storage::segment::{ColumnId, Predicate, ZoneBound};
use stele_storage::validtime::ValidInterval;

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

    /// The latest timestamp handed out, **without** advancing the clock — the
    /// default read snapshot ("read the current state"). A reader at this instant
    /// sees every commit so far (each had `sys_from <= high_water`) and nothing
    /// not yet committed.
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
}

/// The affected-row count of a committed `INSERT` / `UPDATE` / `DELETE`.
///
/// v0.1 DML writes a single row per statement, so the count is always `1` on
/// success — but the variant is carried so the wire layer can render the right
/// `CommandComplete` tag (`INSERT 0 n` / `UPDATE n` / `DELETE n`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlSummary {
    /// `INSERT` affected `n` rows.
    Insert(u64),
    /// `UPDATE` affected `n` rows.
    Update(u64),
    /// `DELETE` affected `n` rows.
    Delete(u64),
}

/// A multi-statement transaction's buffered, not-yet-applied writes ([STL-174]).
///
/// Created by [`SessionEngine::begin`], fed bound DML one statement at a time by
/// [`SessionEngine::stage_dml`], and applied as a unit by
/// [`SessionEngine::commit`] — or simply **dropped** to roll back. The defining
/// property is that staged writes are *buffered*, never applied, until commit:
/// nothing a transaction writes is visible — to it, or to any other connection —
/// before `COMMIT`, and `ROLLBACK` discards the buffer with no effect ever
/// reaching storage.
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
/// a partial prefix — and the writes share one transaction id.
///
/// What this deliberately does *not* yet do (each its own follow-up):
/// * **Read-your-own-writes.** A `SELECT` inside the transaction reads the pinned
///   snapshot, but does **not** see the transaction's own buffered, not-yet-
///   committed writes — overlaying the write buffer on the snapshot read is a
///   follow-up ([STL-203]).
/// * **Cross-table commit atomicity.** A transaction spanning several tables writes
///   one record + one fsync *per table* (each table owns its WAL), so each table's
///   portion is crash-atomic but a crash *between* tables can leave some durable and
///   some not; a transaction commit marker across the per-table logs is the follow-up.
/// * **In-memory rollback of an aborted commit.** If applying a buffered write
///   fails, nothing is made durable, but writes already applied to the in-memory
///   tiers are not yet rolled back in place (recovery would drop them); targeted
///   in-memory rollback is the follow-up.
///
/// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
/// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
/// [STL-176]: https://allegromusic.atlassian.net/browse/STL-176
/// [STL-203]: https://allegromusic.atlassian.net/browse/STL-203
/// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
#[derive(Debug)]
pub struct SessionTransaction {
    /// The system-time snapshot pinned at [`begin`](SessionEngine::begin). Every
    /// read in the transaction resolves here, and a write-write conflict is one
    /// whose key was committed by another transaction *after* this instant.
    snapshot: SystemTimeMicros,
    /// The bound writes staged so far, in statement order. Applied front-to-back
    /// at commit so a later `UPDATE` of a key staged after its `INSERT` lands in
    /// the order the client issued them.
    writes: Vec<BoundDml>,
    /// The open savepoints, innermost last ([STL-176]). Each marks the length of
    /// `writes` at the instant the savepoint was established, so `ROLLBACK TO`
    /// truncates `writes` back to that marker — undoing exactly the writes staged
    /// after the savepoint, and nothing before it.
    savepoints: Vec<Savepoint>,
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

    /// Applying DDL to the catalog failed (name already live, non-monotonic
    /// time, …).
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// A storage tier — WAL, delta, validity index, or a sealed segment —
    /// errored on open, write, or recovery.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// The durable catalog log ([ADR-0028]) could not be appended (the DDL is
    /// refused — nothing was acknowledged) or replayed at recovery (the log
    /// could not be read, or an acknowledged record is corrupt — recovery
    /// fails closed rather than serving a different table set).
    ///
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    #[error("catalog log: {0}")]
    CatalogLog(#[source] io::Error),

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
    /// The next transaction id to stamp on a routed DML commit. v0.1 has no real
    /// transaction manager yet ([STL-99]); a per-session monotonic counter gives
    /// each `INSERT` / `UPDATE` / `DELETE` distinct provenance until one exists.
    next_txn: u64,
    /// The MVCC write index: per-`(table, key)`, the commit instant of the most
    /// recent committed write. Every applied write records its commit instant
    /// here, and a multi-statement [`commit`](Self::commit) checks its write set
    /// against it for first-committer-wins conflict detection ([STL-175],
    /// [ADR-0008]). Keyed by table name + business key; one entry per distinct key
    /// (a later write overwrites the instant), so it grows with the number of
    /// distinct keys ever written, not with the number of writes — pruning entries
    /// older than the oldest live snapshot is a deferred refinement ([STL-204]).
    ///
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    /// [STL-204]: https://allegromusic.atlassian.net/browse/STL-204
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    write_index: BTreeMap<(String, BusinessKey), SystemTimeMicros>,
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
            next_txn: 1,
            write_index: BTreeMap::new(),
        }
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
    /// 2. **Reopen every recorded namespace** through [`Engine::recover`]
    ///    (segment checksums + checkpoint + WAL tail replay, [STL-102]/
    ///    [STL-177]) — dropped names included: their retained history must keep
    ///    answering `AS OF` reads, and a re-create must reuse the same tier so
    ///    that history is neither duplicated nor orphaned.
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
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    ///
    /// # Errors
    ///
    /// [`EngineError::CatalogLog`] if the catalog log cannot be read or holds a
    /// corrupt acknowledged record; [`EngineError::Catalog`] if replaying a
    /// record is refused (a log/catalog invariant break — fails closed);
    /// [`EngineError::Storage`] if a table's tiers cannot be recovered.
    pub fn recover(disk: D, clock: C) -> Result<Self, EngineError> {
        let records = catalog_log::replay(&disk).map_err(EngineError::CatalogLog)?;
        let clock = MonotonicClock::new(clock);

        // 1. Rebuild the catalog by replaying the DDL history, tracking per
        //    name the tier to reopen: the namespace and valid-time policy of
        //    its *latest* create. (A drop keeps the entry — the tier stays
        //    resident for history, exactly as in a live session.)
        let mut catalog = Catalog::new();
        let mut tiers: BTreeMap<String, (u64, bool)> = BTreeMap::new();
        let mut next_namespace = 0u64;
        let mut max_commit = SystemTimeMicros(0);
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
                    catalog.create_table(name.clone(), columns, temporal, at)?;
                    tiers.insert(name, (namespace, valid_time));
                    next_namespace = next_namespace.max(namespace + 1);
                    max_commit = max_commit.max(at);
                }
                CatalogRecord::DropTable { at, name } => {
                    catalog.drop_table(&name, at)?;
                    max_commit = max_commit.max(at);
                }
            }
        }

        // 2. Reopen each recorded tier from its slice of the disk, and fold in
        //    its high-water marks (largest commit instant / txn id on disk).
        let mut tables = BTreeMap::new();
        let mut max_txn_id = 0u64;
        for (name, (namespace, valid_time)) in tiers {
            let tier_disk = NamespacedDisk::new(disk.clone(), namespace);
            let engine = Engine::recover(tier_disk, clock.clone(), valid_time)?;
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
        //    into recovered provenance.
        clock.advance_to(max_commit);
        Ok(Self {
            catalog,
            clock,
            disk,
            tables,
            next_namespace,
            next_txn: max_txn_id.saturating_add(1),
            write_index: BTreeMap::new(),
        })
    }

    /// The session's catalog — schemas resolve at a snapshot through it.
    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
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
        // An auto-committed statement is its own snapshot: read the latest
        // committed state, then write immediately. (Snapshot isolation pins one
        // snapshot for a whole multi-statement transaction instead — see
        // [`execute_in_txn`](Self::execute_in_txn).)
        self.execute_at(stmt, self.clock.current())
    }

    /// Execute one statement inside an open multi-statement transaction, under
    /// **snapshot isolation** ([ADR-0008], [STL-175]).
    ///
    /// An `INSERT` / `UPDATE` / `DELETE` is **buffered** into `txn` (applied as a
    /// unit at [`commit`](Self::commit)), bound at the transaction's pinned
    /// snapshot. A `SELECT` runs immediately, with its reads resolved at that
    /// *same* pinned snapshot, so every statement in the block observes one
    /// consistent system-time snapshot even while other connections commit.
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
        if let Some(summary) = self.stage_dml(stmt, txn)? {
            return Ok(StatementOutcome::Dml(summary));
        }
        let outcome = self.execute_at(stmt, txn.snapshot)?;
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
        txn.snapshot = self.clock.current();
    }

    /// The shared statement router, resolving **reads** — a `SELECT`, and the
    /// table/literal binding of an auto-committed DML — at `read_snapshot`. DDL
    /// always takes effect at the commit clock's next instant, independent of the
    /// read snapshot. Routes by binding, in order: DDL, then `SELECT`, then
    /// `INSERT` / `UPDATE` / `DELETE`.
    fn execute_at(
        &mut self,
        stmt: &Statement,
        read_snapshot: SystemTimeMicros,
    ) -> Result<StatementOutcome, EngineError> {
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
                Ok(bound) => return self.run_select(&bound),
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
        self.apply_dml(bound)
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
                catalog_log::append(&self.disk, &record).map_err(EngineError::CatalogLog)?;
                self.catalog = staged;
                if let Some(tier) = tier {
                    self.tables.insert(name, tier);
                }
                Ok(StatementOutcome::Ddl {
                    tag: DdlOutcome::Created(schema_id).command_tag(),
                })
            }
            // A drop never opens storage. The `IF EXISTS` no-op writes no
            // record — nothing changed, so there is nothing to recover.
            DdlStatement::DropTable { name, if_exists } => {
                let mut staged = self.catalog.clone();
                let outcome = match staged.drop_table(&name, at) {
                    Ok(id) => DdlOutcome::Dropped(id),
                    Err(CatalogError::UnknownTable(_)) if if_exists => DdlOutcome::DropNoOp,
                    Err(e) => return Err(EngineError::Catalog(e)),
                };
                if matches!(outcome, DdlOutcome::Dropped(_)) {
                    let record = CatalogRecord::DropTable { at, name };
                    catalog_log::append(&self.disk, &record).map_err(EngineError::CatalogLog)?;
                    self.catalog = staged;
                }
                Ok(StatementOutcome::Ddl {
                    tag: outcome.command_tag(),
                })
            }
        }
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
        let engine = Engine::open(disk, self.clock.clone(), valid_time)?;
        self.next_namespace += 1;
        Ok(TableState {
            engine,
            valid_time,
            namespace,
        })
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
    fn run_select(&self, bound: &BoundSelect) -> Result<StatementOutcome, EngineError> {
        // A two-table `JOIN` ([STL-172]) takes a wholly different path: it scans
        // both sides and combines their rows, rather than projecting one table's
        // reconstructed rows. The single-table fields below are unused for it.
        if let Some(join) = &bound.join {
            return self.run_join(join, bound.snapshot);
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

        // Reconstruct the full rows [key, value cells…] live at the snapshot, after
        // the `WHERE` filter, through the vectorized operator pipeline ([STL-206]).
        let rows = Self::scan_rows(bound, state, &schema_columns, value_count)?;

        // An aggregate query folds those rows into grouped output ([STL-171]); a
        // plain query projects them.
        if let Some(agg) = &bound.aggregate {
            return Ok(StatementOutcome::Rows(run_aggregate(
                agg,
                &schema_columns,
                &rows,
            )?));
        }

        let projection = projection_indices(&bound.projection, &schema_columns);
        let columns = projection
            .iter()
            .map(|&i| schema_columns[i].clone())
            .collect();
        let out_rows: Vec<Vec<Option<Vec<u8>>>> = rows
            .iter()
            .map(|full| projection.iter().map(|&i| full[i].clone()).collect())
            .collect();
        Ok(StatementOutcome::Rows(SelectResult {
            columns,
            rows: out_rows,
        }))
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
    fn scan_rows(
        bound: &BoundSelect,
        state: &TableState<C, D>,
        schema_columns: &[(String, LogicalType)],
        value_count: usize,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
        // A period predicate ([STL-165], [STL-193]) is one of two shapes. A
        // fully-constant one folds to a single truth value: a `false` excludes
        // every row, so skip the scan entirely (never a silently-unfiltered read).
        // One built from value columns ([STL-193]) cannot be decided until the
        // rows are decoded, so it is evaluated per row in the collection loop
        // below; `per_row_period` carries it there.
        let per_row_period = match &bound.period_filter {
            Some(p) => match const_period_truth(p) {
                Some(false) => return Ok(Vec::new()),
                Some(true) => None,
                None => Some(p),
            },
            None => None,
        };

        // Push a key-equality predicate down to the scan for zone-map pruning; a
        // filter on a value column lives inside the opaque payload, which a zone
        // map cannot reason about, so the vectorized `Filter` below is where it is
        // applied. The pushed-down key predicate is re-applied by that same
        // `Filter`, so the answer is exact regardless of what the prune could prove.
        let predicate = match &bound.filter {
            Some(p) if p.column_index == 0 => Predicate::Eq {
                column: ColumnId::BusinessKey,
                value: ZoneBound::Bytes(encode_value(&p.value)),
            },
            _ => Predicate::All,
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
        .valid_time(state.valid_time);
        // Pin the valid axis too when the bound plan carries a `FOR VALID_TIME
        // AS OF v` instant ([STL-164]); without one a valid-time table is read
        // unfiltered (every system-live version, period columns readable).
        if let Some(v) = bound.valid_snapshot {
            scan = scan.valid_as_of(ValidTimeMicros(v.0));
        }

        // ScanSource → ExplodePayload → [Filter]: explode the packed payload into
        // first-class typed value columns (schema order: position 0 the key,
        // position i+1 value column i), then filter the whole batch. Exploded value
        // columns have no `ColumnId`, so the full row is read positionally.
        let source = ScanSource::new(scan, DEFAULT_BATCH_SIZE);
        let exploded = ExplodePayload::new(source, value_count);
        let mut pipeline: Box<dyn Operator + '_> = match &bound.filter {
            Some(p) => {
                let schema_types = schema_columns.iter().map(|(_, ty)| *ty).collect();
                Box::new(Filter::new(exploded, lower_predicate(p), schema_types))
            }
            None => Box::new(exploded),
        };

        let ncols = value_count + 1;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        while let Some(batch) = pipeline.next()? {
            for r in 0..batch.rows {
                let row: Vec<Option<Vec<u8>>> =
                    (0..ncols).map(|i| batch_cell(&batch, i, r)).collect();
                // A per-row period predicate ([STL-193]) builds each operand's
                // `[from, to)` from the row's cells and drops the rows it excludes.
                if let Some(p) = per_row_period {
                    if !period_keeps_row(p, &row, schema_columns) {
                        continue;
                    }
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Run a bound two-table `JOIN` ([STL-172]).
    ///
    /// Both sides are scanned to their reconstructed rows at `snapshot`
    /// ([`scan_all_rows`](Self::scan_all_rows)); the join key column of each side is
    /// decoded into a typed [`Vector`] and handed to the [`hash_join`] operator,
    /// which returns the surviving rows as input-row indices. The output rows are
    /// then assembled by gathering each side's raw cells per the bound
    /// [`output`](BoundJoin::output) references — a `LEFT` join's unmatched row
    /// drawing `NULL` for every right column. Non-key columns are never decoded;
    /// they pass through as the opaque canonical bytes the scan produced.
    fn run_join(
        &self,
        join: &BoundJoin,
        snapshot: SystemTimeMicros,
    ) -> Result<StatementOutcome, EngineError> {
        let left_state = self
            .tables
            .get(&join.left.table)
            .ok_or_else(|| EngineError::UnknownTable(join.left.table.clone()))?;
        let right_state = self
            .tables
            .get(&join.right.table)
            .ok_or_else(|| EngineError::UnknownTable(join.right.table.clone()))?;
        let left_value_count = join.left.columns.len().saturating_sub(1);
        let right_value_count = join.right.columns.len().saturating_sub(1);

        let left_rows = Self::scan_all_rows(left_state, snapshot, left_value_count)?;
        let right_rows = Self::scan_all_rows(right_state, snapshot, right_value_count)?;

        // Decode only the join-key column of each side into a typed vector; every
        // other column stays opaque bytes (gathered by index below), so a column
        // the join merely carries through is never forced through the evaluator.
        let left_keys = decode_key_column(&left_rows, &join.left.columns, join.left_key)?;
        let right_keys = decode_key_column(&right_rows, &join.right.columns, join.right_key)?;

        let join_type = lower_join_type(join.join_type);
        let indices = hash_join(
            join_type,
            &left_keys,
            left_rows.len(),
            &Expr::col(join.left_key),
            &right_keys,
            right_rows.len(),
            &Expr::col(join.right_key),
        )
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

        // Gather each output row's cells per the bound output references. A
        // right-keeping join reads both sides (a `None` right index — a LEFT join's
        // unmatched row — yields NULL right cells); SEMI/ANTI read the left alone.
        let rows: Vec<Vec<Option<Vec<u8>>>> = if join_type.keeps_right() {
            indices
                .left
                .iter()
                .zip(&indices.right)
                .map(|(&l, &r)| {
                    join.output
                        .iter()
                        .map(|col| match col {
                            JoinColumnRef::Left(i) => left_rows[l][*i].clone(),
                            JoinColumnRef::Right(j) => r.and_then(|rr| right_rows[rr][*j].clone()),
                        })
                        .collect()
                })
                .collect()
        } else {
            indices
                .left
                .iter()
                .map(|&l| {
                    join.output
                        .iter()
                        .map(|col| match col {
                            JoinColumnRef::Left(i) => left_rows[l][*i].clone(),
                            // The binder proves a SEMI/ANTI output is left-only.
                            JoinColumnRef::Right(_) => {
                                unreachable!("SEMI/ANTI output references only the left side")
                            }
                        })
                        .collect()
                })
                .collect()
        };

        Ok(StatementOutcome::Rows(SelectResult {
            columns: join.columns.clone(),
            rows,
        }))
    }

    /// Scan a table's reconstructed rows at `snapshot`, unfiltered — the join's
    /// per-side input ([STL-172]).
    ///
    /// The same `ScanSource → ExplodePayload` pipeline [`scan_rows`](Self::scan_rows)
    /// runs, minus the `WHERE` filter and a valid-axis *pin* (a join carries no
    /// per-side predicate or `FOR VALID_TIME AS OF` at v0.2), so each row comes
    /// back as its full `[business key, value cells…]` canonical bytes. The
    /// table's valid-time policy is still declared so a valid-time side's delta
    /// frame is stripped ([STL-218]); full sequenced temporal joins (intersecting
    /// both axes) stay deferred to [STL-172].
    fn scan_all_rows(
        state: &TableState<C, D>,
        snapshot: SystemTimeMicros,
        value_count: usize,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, EngineError> {
        let readers = state.engine.open_segment_readers()?;
        let scan = SnapshotScan::new(
            state.engine.delta(),
            state.engine.index(),
            &readers,
            Snapshot(snapshot),
        )
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .valid_time(state.valid_time);
        let source = ScanSource::new(scan, DEFAULT_BATCH_SIZE);
        let mut exploded = ExplodePayload::new(source, value_count);

        let ncols = value_count + 1;
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        while let Some(batch) = exploded.next()? {
            for r in 0..batch.rows {
                rows.push((0..ncols).map(|i| batch_cell(&batch, i, r)).collect());
            }
        }
        Ok(rows)
    }

    /// Apply a bound DML statement to the table's tiers under fresh provenance,
    /// and report the affected-row count. The encoding details (key + value
    /// columns through the row codec, `UPDATE`'s read-modify-write merge) live in
    /// [`apply_bound_dml`](Self::apply_bound_dml).
    fn apply_dml(&mut self, dml: BoundDml) -> Result<StatementOutcome, EngineError> {
        let txn_id = TxnId(self.next_txn);
        self.next_txn += 1;
        let principal = Principal::new(WIRE_PRINCIPAL.to_vec());
        let summary = self.apply_bound_dml(dml, txn_id, &principal)?;
        Ok(StatementOutcome::Dml(summary))
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
    fn apply_bound_dml(
        &mut self,
        dml: BoundDml,
        txn_id: TxnId,
        principal: &Principal,
    ) -> Result<DmlSummary, EngineError> {
        // The (table, business key) this write commits, captured before the match
        // consumes `dml`, so its commit instant can be recorded for conflict
        // detection once the write lands.
        let committed = (dml.table().to_owned(), dml_business_key(&dml));
        let summary = match dml {
            BoundDml::Insert {
                table, key, values, ..
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
                    None,
                    row_codec::encode_payload(&cells),
                    0,
                    txn_id,
                    principal.clone(),
                )?;
                DmlSummary::Insert(1)
            }
            BoundDml::Update {
                table,
                key,
                assignments,
                ..
            } => {
                // Read-modify-write: merge the SET overrides onto the live row's
                // value cells so unnamed columns keep their prior value, then
                // re-pack. The base is read at the committed state, which — for a
                // key that passed `commit`'s write-write conflict check — is
                // unchanged since this transaction's snapshot. (A transaction does
                // not yet read its own buffered writes, [STL-203].)
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
                    None,
                    row_codec::encode_payload(&cells),
                    0,
                    txn_id,
                    principal.clone(),
                )?;
                DmlSummary::Update(1)
            }
            BoundDml::Delete { table, key, .. } => {
                self.delete(&table, &business_key(&key), txn_id, principal.clone())?;
                DmlSummary::Delete(1)
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
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: ZoneBound::Bytes(key.as_bytes().to_vec()),
        })
        .execute()?;
        // The key resolves to at most one live version; take its payload (the Eq
        // predicate narrows the scan, but re-match the key defensively).
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
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    /// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
    /// [ADR-0008]: ../../../docs/adr/0008-mvcc-on-append-only.md
    #[must_use]
    pub fn begin(&self) -> SessionTransaction {
        SessionTransaction {
            snapshot: self.clock.current(),
            writes: Vec::new(),
            savepoints: Vec::new(),
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
            Ok(dml) => {
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
    /// ## Crash-atomic group commit ([STL-192])
    ///
    /// The buffered writes are not replayed one WAL record at a time. Each table the
    /// transaction touches is put in **group-commit** mode
    /// ([`Engine::begin_group`](stele_storage::engine::Engine::begin_group)): its
    /// writes apply to the delta/index in staged order (so a later write sees an
    /// earlier one) but their redos accumulate, and a single
    /// [`commit_group`](stele_storage::engine::Engine::commit_group) then writes the
    /// whole table's portion as **one** WAL record group-committed with **one** fsync
    /// — the only durability point (invariant 2). That record is the atomic unit:
    /// recovery replays it whole or, if a crash tears it, drops it at the durable
    /// fence — so the transaction's writes recover all-or-none, never a partial
    /// prefix. If applying a write fails, every touched table's buffer is discarded
    /// ([`abort_group`](stele_storage::engine::Engine::abort_group)) so nothing is
    /// made durable.
    ///
    /// A transaction spanning **multiple tables** writes one record + one fsync *per
    /// table* (each table owns its WAL), so each table's portion is crash-atomic;
    /// cross-table all-or-none would need a transaction commit marker across the
    /// per-table logs and is a follow-up.
    ///
    /// # Errors
    ///
    /// [`EngineError::Conflict`] if a concurrent commit modified this transaction's
    /// write set after its snapshot (retry the transaction). Otherwise
    /// [`EngineError`] if applying any buffered write fails (e.g. its table was
    /// dropped between staging and commit) or a group-commit append/fsync fails. A
    /// failure before the fsync makes nothing durable (a torn record is dropped on
    /// recovery); a *failed fsync after a successful append* leaves the staged record's
    /// durability **indeterminate** and must be treated as a crash, not a clean abort
    /// (the WAL contract, [STL-217]). A write already applied to the in-memory tiers
    /// when a *later* one fails is not yet rolled back in memory ([STL-216]).
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
                return Err(EngineError::Conflict);
            }
        }
        let txn_id = TxnId(self.next_txn);
        self.next_txn += 1;
        let principal = Principal::new(WIRE_PRINCIPAL.to_vec());

        // Apply every write into per-table group-commit buffers, tracking the tables
        // touched so they can be group-committed (success) or discarded (failure).
        let mut touched: Vec<String> = Vec::new();
        match self.apply_group(txn.writes, txn_id, &principal, &mut touched) {
            Ok(()) => self.finish_group_commit(&touched),
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
        }
    }

    /// Group-commit every `touched` table — one WAL record + one fsync each — and
    /// report the first failure, if any ([`commit`](Self::commit), [STL-192]).
    ///
    /// Every touched table is taken out of group-commit mode here: the ones up to a
    /// failure are committed (durable), and any after it are discarded
    /// ([`abort_group`](stele_storage::engine::Engine::abort_group)) so none is left
    /// buffering — which would otherwise silently swallow a later auto-commit write.
    /// A mid-sequence failure can thus leave earlier tables durable and later ones
    /// not — the cross-table atomicity limitation noted on [`commit`](Self::commit).
    ///
    /// One caveat on the failing table: if its `commit_group` failed *after* the WAL
    /// append (a failed fsync, not a torn write), its staged record's durability is
    /// indeterminate — returning `Err` reports the commit as failed, but the record
    /// may still flush on a later `tick`. Per the WAL contract that case is a crash,
    /// not a clean abort; enforcing it (poisoning the engine on fsync failure) is
    /// [STL-217].
    fn finish_group_commit(&mut self, touched: &[String]) -> Result<(), EngineError> {
        let mut error: Option<EngineError> = None;
        for table in touched {
            let Ok(state) = self.table_mut(table) else {
                continue;
            };
            if error.is_some() {
                state.engine.abort_group();
            } else if let Err(e) = state.engine.commit_group() {
                error = Some(EngineError::from(e));
            }
        }
        error.map_or(Ok(()), Err)
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

/// The provenance principal stamped on writes routed through the session engine.
///
/// v0.1 has no authentication ([ADR-0003] defers SCRAM/TLS to v0.3), so every
/// routed DML commit shares this fixed principal until a connection carries a real
/// identity. Provenance is still captured inline at commit, per the architectural
/// invariant — only the identity is a placeholder.
const WIRE_PRINCIPAL: &[u8] = b"stele";

/// The affected-row summary a bound DML operation reports — always one row per
/// statement at v0.1, tagged by kind so the wire layer renders the right
/// `CommandComplete` ([`stage_dml`](SessionEngine::stage_dml) reports it before
/// the write is applied).
const fn dml_summary(dml: &BoundDml) -> DmlSummary {
    match dml {
        BoundDml::Insert { .. } => DmlSummary::Insert(1),
        BoundDml::Update { .. } => DmlSummary::Update(1),
        BoundDml::Delete { .. } => DmlSummary::Delete(1),
    }
}

/// Encode a [`ScalarValue`] to its canonical, type-erased byte form.
fn encode_value(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// The business key for a folded key [`ScalarValue`] — its canonical encoding, the
/// same bytes a later `UPDATE` / `DELETE` / `SELECT` folds the literal to, so the
/// key matches across operations.
fn business_key(value: &ScalarValue) -> BusinessKey {
    BusinessKey::new(encode_value(value))
}

/// The [`BusinessKey`] a bound DML writes — the unit of write-write conflict
/// detection ([`commit`](SessionEngine::commit)). Every `BoundDml` variant carries
/// a single key (the positional first column), so this is total over the enum.
fn dml_business_key(dml: &BoundDml) -> BusinessKey {
    let key = match dml {
        BoundDml::Insert { key, .. }
        | BoundDml::Update { key, .. }
        | BoundDml::Delete { key, .. } => key,
    };
    business_key(key)
}

/// The cell of bytes column `id` at `row`, or `None` if the column is absent,
/// not a bytes column, or its cell is a SQL `NULL` ([STL-154]). The scan only
/// ever projects [`ColumnId::BusinessKey`] / [`ColumnId::Payload`], both bytes
/// columns; the business key is always present, the payload may be `None`.
fn column_cell(batch: &Batch, id: ColumnId, row: usize) -> Option<Vec<u8>> {
    batch.columns.iter().find_map(|(cid, col)| match col {
        Column::Bytes(v) if *cid == id => v.get(row).cloned().flatten(),
        _ => None,
    })
}

/// Lower a bound `WHERE <column> = <literal>` predicate ([STL-151]) to the
/// vectorized [`Expr`] the [`Filter`] operator evaluates over a whole batch
/// ([STL-206]). The column is referenced by its schema position — the same index
/// [`ExplodePayload`] puts it at — and the literal is broadcast as a constant. A
/// typed equality over the decoded values is equivalent to the byte-equality the
/// old loop applied, since the encoding is canonical and a NULL cell decodes to a
/// NULL that the comparison (and `Filter`'s "keep TRUE only") drops.
fn lower_predicate(predicate: &BoundPredicate) -> Expr {
    Expr::col(predicate.column_index).compare(CmpOp::Eq, Expr::lit(predicate.value.clone()))
}

/// The single truth value of a fully-constant period predicate ([STL-165]), or
/// `None` when any endpoint references a value column (then it must be evaluated
/// per row — [STL-193]).
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

/// Whether a decoded row satisfies a per-row period predicate ([STL-193]).
///
/// Each operand's `[from, to)` is rebuilt from the row's cells (a constant
/// endpoint is its folded instant; a column endpoint is that cell's µs value). A
/// NULL cell or an empty/reversed `[from, to)` makes the operand *unknown*, and
/// an unknown operand excludes the row — the three-valued-logic stance the
/// `WHERE` filter takes everywhere else (only a TRUE keeps the row).
fn period_keeps_row(
    predicate: &BoundPeriodPredicate,
    row: &[Option<Vec<u8>>],
    schema_columns: &[(String, LogicalType)],
) -> bool {
    match (
        row_period_interval(&predicate.left, row, schema_columns),
        row_period_interval(&predicate.right, row, schema_columns),
    ) {
        (Some(left), Some(right)) => evaluate(predicate.predicate, left, right),
        _ => false,
    }
}

/// Build one operand's `[from, to)` interval for a single row, or `None` when an
/// endpoint cell is NULL or the resulting period is empty/reversed.
fn row_period_interval(
    operand: &BoundPeriod,
    row: &[Option<Vec<u8>>],
    schema_columns: &[(String, LogicalType)],
) -> Option<Interval> {
    let from = period_endpoint_micros(operand.from, row, schema_columns)?;
    let to = period_endpoint_micros(operand.to, row, schema_columns)?;
    Interval::new(from, to).ok()
}

/// Resolve one period endpoint to its microsecond instant for a single row: a
/// constant is itself; a column is its cell decoded as a µs instant
/// (`BIGINT`/`TIMESTAMP`/`TIMESTAMPTZ` — the binder already enforced the type).
/// `None` for a NULL cell or a cell that does not decode to an instant.
fn period_endpoint_micros(
    endpoint: PeriodEndpoint,
    row: &[Option<Vec<u8>>],
    schema_columns: &[(String, LogicalType)],
) -> Option<i64> {
    match endpoint {
        PeriodEndpoint::Const(micros) => Some(micros),
        PeriodEndpoint::Column(index) => {
            let bytes = row[index].as_ref()?;
            match ScalarValue::decode(schema_columns[index].1, bytes).ok()? {
                ScalarValue::Int8(v) | ScalarValue::Timestamp(v) | ScalarValue::TimestampTz(v) => {
                    Some(v)
                }
                _ => None,
            }
        }
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

/// Decode one side's join-key column into a positional [`Vector`] slot for the
/// [`hash_join`] operator ([STL-172]).
///
/// Only the key column at `key` is decoded — the rest stay empty placeholders the
/// key expression (`Expr::col(key)`) never reads (the same discipline
/// [`run_aggregate`] uses), so a non-key column is never forced through the
/// evaluator. The vector is one slot per side column so `Expr::col(key)` addresses
/// the key by its schema index.
fn decode_key_column(
    rows: &[Vec<Option<Vec<u8>>>],
    columns: &[(String, LogicalType)],
    key: usize,
) -> Result<Vec<Vector>, EngineError> {
    let mut cols: Vec<Vector> = (0..columns.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    let cells: Vec<Option<Vec<u8>>> = rows.iter().map(|r| r[key].clone()).collect();
    cols[key] = Vector::from_column(columns[key].1, &Column::Bytes(cells.into()))
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;
    Ok(cols)
}

/// Read the cell at `position`/`row` of an exploded pipeline batch as the
/// [`SelectResult`]'s raw-bytes form ([STL-206]). Every column the pipeline
/// projects — the business key and the [`ExplodePayload`]-produced value columns —
/// is a [`Column::Bytes`] carrying each cell's canonical encoding (`None` for a
/// SQL `NULL`); a fixed-width column never reaches a projected position, but is
/// reinterpreted losslessly rather than panicking if one ever did.
fn batch_cell(batch: &Batch, position: usize, row: usize) -> Option<Vec<u8>> {
    match &batch.columns[position].1 {
        Column::Bytes(cells) => cells[row].clone(),
        Column::I64(values) => Some(values[row].to_le_bytes().to_vec()),
    }
}

/// The schema-column indices a [`Projection`] selects, in output order: `All` is
/// every column left-to-right; `Columns` maps each name to its position.
///
/// `bind_select` has already proved every named column exists in this schema, so
/// the lookup never misses — a miss would be a binder/engine contract break.
fn projection_indices(projection: &Projection, columns: &[(String, LogicalType)]) -> Vec<usize> {
    match projection {
        Projection::All => (0..columns.len()).collect(),
        Projection::Columns(names) => names
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .position(|(n, _)| n == name)
                    .expect("bind_select validated the projected column exists")
            })
            .collect(),
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
fn run_aggregate(
    agg: &BoundAggregate,
    schema_columns: &[(String, LogicalType)],
    rows: &[Vec<Option<Vec<u8>>>],
) -> Result<SelectResult, EngineError> {
    // Decode each referenced schema column into a typed vector; a column the plan
    // never reads stays an empty placeholder the evaluator never touches (the same
    // discipline the Filter operator uses).
    let mut columns: Vec<Vector> = (0..schema_columns.len())
        .map(|_| Vector::Bool(Vec::new()))
        .collect();
    for &i in &referenced_columns(agg) {
        let cells: Vec<Option<Vec<u8>>> = rows.iter().map(|r| r[i].clone()).collect();
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

    let out = hash_aggregate(&group_keys, &aggregators, &columns, rows.len())
        .map_err(|e| EngineError::Scan(ScanError::Eval(e)))?;

    // Re-interleave grouping + aggregate columns into SELECT-list order and encode
    // each cell back to its canonical bytes (`None` → a SQL NULL on the wire).
    let output: Vec<&Vector> = agg
        .items
        .iter()
        .map(|item| match item {
            OutputItem::Group(j) => &out.groups[*j],
            OutputItem::Aggregate(k) => &out.aggregates[*k],
        })
        .collect();
    let result_rows: Vec<Vec<Option<Vec<u8>>>> = (0..out.num_groups)
        .map(|g| {
            output
                .iter()
                .map(|v| v.get(g).as_ref().map(encode_value))
                .collect()
        })
        .collect();

    Ok(SelectResult {
        columns: agg.columns.clone(),
        rows: result_rows,
    })
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

    fn parse_one(sql: &str) -> Statement {
        stele_sql::parse(sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one statement")
    }

    const CREATE: &str =
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING";

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
    fn recreate_with_the_same_policy_reuses_the_tier() {
        let mut engine = session();
        engine.execute(&parse_one(CREATE)).expect("create");
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
        engine
            .execute(&parse_one("DROP TABLE account"))
            .expect("drop");
        // Same (system-only) policy on re-create: the tier is reused, so the
        // pre-drop history is still readable.
        engine
            .execute(&parse_one(CREATE))
            .expect("re-create same policy");
        let StatementOutcome::Rows(batch) = engine
            .execute(&parse_one("SELECT balance FROM account"))
            .expect("select")
        else {
            panic!("rows");
        };
        assert_eq!(
            payload_column(&batch),
            &[b"100".to_vec()],
            "history survives"
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

    #[test]
    fn recovery_resolves_old_schema_versions_and_reuses_the_namespace() {
        // A dropped name re-created with different columns: post-restart, the
        // live read sees only the new era and an AS OF read inside the old era
        // resolves the *old* schema — neither duplicated nor orphaned, because
        // the re-create's catalog-log record carries the *same* namespace and
        // recovery reopens that one tier. The recovered session must answer
        // exactly as the live one did — including the live session's existing
        // quirk that a reused tier's dropped-era open rows stay visible to
        // current reads (a DROP closes the catalog name, not the storage rows;
        // tightening that is the filed follow-up STL-211) — so both reads are
        // captured live and compared across the kill.
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
        // group-commit WAL append fails (a crash mid-commit), nothing the
        // transaction wrote becomes durable — recovery finds none of it, never a
        // partial prefix. Group mode buffers every write, so the commit's *only*
        // append is the group-commit record; tearing it tears the whole transaction.
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
            .expect_err("the torn group-commit append aborts the commit");
        assert!(matches!(err, EngineError::Storage(_)), "got {err:?}");
        drop(engine);

        let mut engine = recover_session(&disk);
        assert!(
            select(&mut engine, "SELECT id FROM account")
                .rows
                .is_empty(),
            "a torn group commit leaves none of the transaction's writes",
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
}
