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
//! [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_catalog::{Catalog, CatalogError};
use stele_common::provenance::{Principal, TxnId};
use stele_common::row_codec::{self, RowCodecError};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};
use stele_exec::{Batch, Column, ScanError, SnapshotScan, evaluate};
use stele_sql::ddl::{DdlOutcome, DdlStatement};
use stele_sql::dml::{BoundDml, DmlError};
use stele_sql::select::{BoundSelect, Projection, SelectError};
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
/// Savepoints ([STL-176]) partition the buffer: [`savepoint`](Self::savepoint)
/// records a marker at the current write position, [`rollback_to`](Self::rollback_to)
/// truncates the buffer back to a marker (undoing only the writes staged after it,
/// the transaction continuing), and [`release`](Self::release) drops a marker while
/// keeping its writes.
///
/// What this deliberately does *not* yet do (each its own follow-up):
/// * **Read-your-own-writes.** A `SELECT` inside the transaction still reads the
///   committed snapshot and does not see the buffer — a consistent
///   transaction-local snapshot is snapshot-isolation work ([STL-175]).
/// * **Crash-atomic group commit.** [`commit`](SessionEngine::commit) replays the
///   buffer through the per-write WAL path, so a crash *mid-commit* can leave a
///   prefix durable; a single transaction-boundary WAL record (the `stele-txn`
///   `commit_record` deferral note) is the follow-up that closes that window.
///   Absent a crash, commit is all-or-nothing and shares one transaction id
///   across every write.
///
/// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
/// [STL-175]: https://allegromusic.atlassian.net/browse/STL-175
/// [STL-176]: https://allegromusic.atlassian.net/browse/STL-176
#[derive(Debug, Default)]
pub struct SessionTransaction {
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
    /// A fresh transaction with an empty write buffer and no savepoints.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            writes: Vec::new(),
            savepoints: Vec::new(),
        }
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

    /// Applying DDL to the catalog failed (name already live, non-monotonic
    /// time, …).
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// A storage tier — WAL, delta, validity index, or a sealed segment —
    /// errored on open, write, or recovery.
    #[error(transparent)]
    Storage(#[from] StorageError),

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
}

/// One table's live state inside a session.
struct TableState<C: Clock + Clone, D: Disk + Clone> {
    engine: Engine<MonotonicClock<C>, NamespacedDisk<D>>,
    /// The valid-time policy the tier's writer was opened with. Baked into the
    /// `DmlWriter`, so a re-create that changes it cannot reuse this tier.
    valid_time: bool,
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
}

impl<C: Clock + Clone, D: Disk + Clone> SessionEngine<C, D> {
    /// Open a **fresh** session over `disk` with commit time drawn from `clock`.
    ///
    /// The catalog starts empty and no tiers exist; `CREATE TABLE` populates both.
    /// To boot from existing on-disk state, the recovery hook composes
    /// [`Engine::recover`] per table — but enumerating which tables exist on a
    /// cold start needs durable catalog state, which is a separate concern (see
    /// the crate-level note). This constructor is the v0.1 in-process path.
    #[must_use]
    pub fn open(disk: D, clock: C) -> Self {
        Self {
            catalog: Catalog::new(),
            clock: MonotonicClock::new(clock),
            disk,
            tables: BTreeMap::new(),
            next_namespace: 0,
            next_txn: 1,
        }
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
        // DDL first: `bind_ddl` cleanly rejects non-DDL with `NotDdl`, which we
        // treat as "try the next router".
        match bind_ddl(stmt) {
            Ok(ddl) => return self.apply_ddl(ddl),
            Err(BindError::NotDdl) => {}
            Err(e) => return Err(EngineError::Bind(e)),
        }

        // SELECT next, bound against the current read snapshot. The bind context
        // borrows the catalog immutably; the read path is `&self`, so a hit can
        // run before the borrow ends, but DML below needs `&mut self`, so the
        // borrow is scoped and released first.
        let snapshot = self.clock.current();
        {
            let ctx = BindContext {
                snapshot,
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
                snapshot,
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
    /// instant, and reconcile the tier map.
    ///
    /// For `CREATE TABLE` the storage tier is stood up **before** the catalog
    /// mutation: if the backend fails to open the tier the statement aborts, so
    /// the catalog never names a table with no storage behind it.
    fn apply_ddl(&mut self, ddl: DdlStatement) -> Result<StatementOutcome, EngineError> {
        let at = self.clock.now();
        match ddl {
            DdlStatement::CreateTable {
                name,
                columns,
                temporal,
            } => {
                let valid_time = temporal.valid_time_enabled();
                // A re-created name whose tier is still resident keeps it, so
                // history is never dropped — but only if the valid-time policy is
                // unchanged: the tier's writer bakes the policy in, so reusing it
                // under a different policy would silently enforce the stale one
                // (re-opening the tier with the new policy is the deferred
                // alternative). A fresh name opens its tier first, so a backend
                // failure aborts before the catalog is touched.
                let tier = match self.tables.get(&name).map(|s| s.valid_time) {
                    Some(prev) if prev != valid_time => {
                        return Err(EngineError::ValidTimePolicyChange { table: name });
                    }
                    Some(_) => None,
                    None => Some(self.open_tier(valid_time)?),
                };
                let schema_id = self
                    .catalog
                    .create_table(name.clone(), columns, temporal, at)?;
                if let Some(tier) = tier {
                    self.tables.insert(name, tier);
                }
                Ok(StatementOutcome::Ddl {
                    tag: DdlOutcome::Created(schema_id).command_tag(),
                })
            }
            // A drop never opens storage; `apply` owns the `IF EXISTS` no-op.
            DdlStatement::DropTable { .. } => {
                let outcome = ddl.apply(&mut self.catalog, at)?;
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
        let disk = NamespacedDisk::new(self.disk.clone(), self.next_namespace);
        let engine = Engine::open(disk, self.clock.clone(), valid_time)?;
        self.next_namespace += 1;
        Ok(TableState { engine, valid_time })
    }

    /// Run a snapshot scan for a bound `SELECT`, honoring its projection list and
    /// `WHERE` filter ([STL-151]).
    ///
    /// The scan materializes the `(business_key, payload)` pair; the payload is
    /// sliced back into the row's value columns by the
    /// [row codec](stele_common::row_codec), reconstructing the full row in schema
    /// order (the business key, then the value columns). The row is then
    /// **filtered** by the bound predicate and **projected** to exactly the
    /// requested columns. A key-equality predicate is additionally pushed down to
    /// the scan so its zone maps can prune; every predicate is re-applied here so
    /// the answer is correct regardless of what the prune could prove.
    ///
    /// The schema is resolved at the read snapshot, so an `AS OF` read names and
    /// types its columns under the schema version live then.
    fn run_select(&self, bound: &BoundSelect) -> Result<StatementOutcome, EngineError> {
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

        // A constant period predicate ([STL-165]) is a whole-`WHERE` filter that
        // folds to a single truth value: when it is false no row qualifies, so the
        // scan is skipped and an empty result with the correct header is returned
        // (never a silently-unfiltered read). A true predicate constrains no
        // individual row, so the scan below proceeds unfiltered.
        if let Some(p) = &bound.period_filter {
            if !evaluate(p.predicate, p.left, p.right) {
                let projection = projection_indices(&bound.projection, &schema_columns);
                let columns = projection
                    .iter()
                    .map(|&i| schema_columns[i].clone())
                    .collect();
                return Ok(StatementOutcome::Rows(SelectResult {
                    columns,
                    rows: Vec::new(),
                }));
            }
        }

        // Push a key-equality predicate down to the scan for zone-map pruning; a
        // filter on a value column lives inside the opaque payload, which a zone
        // map cannot reason about, so it is applied only after decode.
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
            Snapshot(snapshot),
        )
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .filter(predicate);
        // Pin the valid axis too when the bound plan carries a `FOR VALID_TIME
        // AS OF v` instant ([STL-164]). `bind_select` sets `valid_snapshot` only
        // for a table that opts into a valid-time period, so turning on both-axes
        // resolution here is sound; the micros name the same instant, reinterpreted
        // on the valid axis. `None` leaves the scan system-only — byte-for-byte the
        // prior behavior.
        if let Some(v) = bound.valid_snapshot {
            scan = scan.valid_as_of(ValidTimeMicros(v.0));
        }
        let out = scan.execute()?;

        // Reconstruct each full row [key, value cells…], then filter + project.
        let projection = projection_indices(&bound.projection, &schema_columns);
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(out.batch.rows);
        for r in 0..out.batch.rows {
            let key = column_cell(&out.batch, ColumnId::BusinessKey, r);
            let payload = column_cell(&out.batch, ColumnId::Payload, r);
            let mut full_row = Vec::with_capacity(schema_columns.len());
            full_row.push(key);
            full_row.extend(row_codec::decode_payload(value_count, payload.as_deref())?);

            // Re-apply the predicate on the reconstructed row (belt-and-suspenders
            // for the key case, the only filter for a value column). `WHERE col =
            // <lit>` keeps a row iff that column's cell equals the literal's
            // encoding; a NULL cell (`None`) never equals a (non-null) literal.
            if let Some(p) = &bound.filter {
                let want = encode_value(&p.value);
                let cell = full_row.get(p.column_index).cloned().flatten();
                if cell.as_deref() != Some(want.as_slice()) {
                    continue;
                }
            }

            rows.push(projection.iter().map(|&i| full_row[i].clone()).collect());
        }

        let columns = projection
            .iter()
            .map(|&i| schema_columns[i].clone())
            .collect();
        Ok(StatementOutcome::Rows(SelectResult { columns, rows }))
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
        match dml {
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
                Ok(DmlSummary::Insert(1))
            }
            BoundDml::Update {
                table,
                key,
                assignments,
                ..
            } => {
                // Read-modify-write: merge the SET overrides onto the live row's
                // value cells so unnamed columns keep their prior value, then
                // re-pack. (A read here sees the committed snapshot — a
                // transaction does not yet read its own buffered writes, [STL-175].)
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
                Ok(DmlSummary::Update(1))
            }
            BoundDml::Delete { table, key, .. } => {
                self.delete(&table, &business_key(&key), txn_id, principal.clone())?;
                Ok(DmlSummary::Delete(1))
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
    /// feeds with [`stage_dml`](Self::stage_dml) and applies with
    /// [`commit`](Self::commit) ([STL-174]).
    ///
    /// The transaction is held *per connection* (the pgwire front end owns one per
    /// session), not on the shared engine, so two connections' open transactions
    /// stay independent. Nothing is allocated against the engine here — a
    /// transaction id is taken only at [`commit`](Self::commit), so a `BEGIN`
    /// followed by `ROLLBACK` (or a read-only transaction) consumes none.
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    #[must_use]
    pub const fn begin(&self) -> SessionTransaction {
        SessionTransaction::new()
    }

    /// Bind a DML statement and **buffer** it into `txn` without applying it,
    /// returning the affected-row summary the wire client expects for its
    /// `CommandComplete`. Returns `Ok(None)` if `stmt` is not an
    /// `INSERT`/`UPDATE`/`DELETE` — a `SELECT` or DDL inside a transaction is the
    /// caller's to route through [`execute`](Self::execute), which runs it at once
    /// against the committed state (the buffer stays write-only, [STL-174]).
    ///
    /// Binding here folds the statement's literals and resolves its table against
    /// the catalog at the current snapshot, exactly as the auto-commit path does;
    /// only the *application* is deferred to [`commit`](Self::commit).
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
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
            snapshot: self.clock.current(),
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
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    ///
    /// # Errors
    ///
    /// [`EngineError`] if applying any buffered write fails (e.g. its table was
    /// dropped between staging and commit). A write that has already been applied
    /// when a later one fails is **not** rolled back — crash/-failure-atomic group
    /// commit is the deferred follow-up noted on [`SessionTransaction`].
    pub fn commit(&mut self, txn: SessionTransaction) -> Result<(), EngineError> {
        let txn_id = TxnId(self.next_txn);
        self.next_txn += 1;
        let principal = Principal::new(WIRE_PRINCIPAL.to_vec());
        for dml in txn.writes {
            self.apply_bound_dml(dml, txn_id, &principal)?;
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
}
