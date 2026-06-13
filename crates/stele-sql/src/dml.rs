//! DML binding: lower a parsed `INSERT` / `UPDATE` / `DELETE` into the storage
//! write the engine applies.
//!
//! This is the SQL-layer half of DML-over-the-wire ([STL-149]) — the sibling of
//! [`bind_select`](crate::bind_select) (reads) and [`bind_ddl`](crate::bind_ddl)
//! (schema). [`parse`](crate::parse) handles the grammar; [`bind_dml`] turns the
//! data-manipulation subset into a typed [`BoundDml`] the session engine lowers
//! to a `DmlWriter` call (`insert` / `update` / `delete`). The pgwire query
//! loop ([STL-147]) is the consumer.
//!
//! ## The column mapping — key + value columns
//!
//! The catalog does not yet record which column is the primary key
//! ([`bind_ddl`](crate::bind_ddl) parses `PRIMARY KEY` but stores only name +
//! type), so DML keeps the positional convention: the **first column is the
//! business key**, and the remaining columns are the row's **value columns**. A
//! table may now be any width ([STL-151]) — the value columns are packed into the
//! one stored payload by the [row codec](stele_common::row_codec) when the engine
//! applies the write, and sliced back out on read. Each supplied literal is folded
//! to a [`ScalarValue`] of its column's type (via the `fold` module); the engine
//! encodes those to bytes with
//! [`ScalarValue::encode`](stele_common::types::ScalarValue::encode), so the
//! round-trip back through a read is exact.
//!
//! The three operations map to the write path ([STL-94],
//! [architecture §3.4](../../../docs/02-architecture.md#34-write-path-sequence)):
//!
//! * `INSERT INTO t VALUES (k, …)` — open a fresh period for `k` carrying the
//!   row's value columns.
//! * `UPDATE t SET <col> = v[, …] WHERE <key> = k` — close `k`'s prior period and
//!   open a new one; columns the `SET` does not name keep their prior value
//!   (a read-modify-write the engine performs).
//! * `DELETE FROM t WHERE <key> = k` — close `k`'s prior period, no successor.
//!
//! ## Predicate-driven `UPDATE` / `DELETE` ([STL-229])
//!
//! The `WHERE` is no longer restricted to `<key> = <literal>`: it binds through
//! the same predicate binder a `SELECT`'s `WHERE` uses
//! ([`bind_where_predicate`](crate::select)), so the two statement families share
//! one vocabulary — a single comparison anchored on one column, any of the six
//! comparison operators, with integer arithmetic on either side ([STL-213]). A
//! missing `WHERE` is a **whole-table** write (match-all). A predicate that is
//! exactly `<key> = <literal>` keeps the point fast path ([`BoundDml::Update`] /
//! [`BoundDml::Delete`], no scan); everything else binds to the scan-then-write
//! [`BoundDml::UpdateScan`] / [`BoundDml::DeleteScan`], which the engine expands
//! into one point write per matched live key, applied as a single atomic group.
//!
//! [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
//!
//! ## Valid-time tables ([STL-194])
//!
//! A table that opted into a valid axis (`… VALID TIME (vf, vt)`) carries a second
//! `[from, to)` interval per version: *when the fact is true in the world*. The
//! two period columns (`vf`, `vt`) are ordinary declared columns, so they appear
//! in the `INSERT` value list / `UPDATE … SET` like any other — but their values
//! are *lifted* into an [`Interval`] the engine
//! frames onto the stored payload, **and** kept as the row's
//! [`Timestamp`](stele_common::types::ScalarValue::Timestamp) value cells so the
//! row codec stays width-agnostic to the opt-in. The bounds fold the same way a
//! [`FOR VALID_TIME AS OF`](crate::select) instant does (an integer microsecond
//! value, `now()`, or `now() ± interval`) — they are not civil-time `TIMESTAMP`
//! literals. The start (`vf`) is mandatory; the end (`vt`) defaults to
//! [`VALID_TIME_OPEN`] (an open period) when
//! omitted from the column list or given `NULL`. The bound interval rides
//! the `valid` field of [`BoundDml::Insert`] / [`BoundDml::Update`]; it is `None` for a
//! system-only table.
//!
//! ## Multi-row `INSERT` ([STL-228])
//!
//! `INSERT INTO t VALUES (…), (…), …` binds every row — each through the same
//! per-row column/codec validation a single-row `INSERT` uses, with a per-row
//! failure naming the offending row ([`DmlError::RowError`]). One row stays a
//! point [`BoundDml::Insert`]; two or more bind to [`BoundDml::InsertRows`],
//! which the engine applies as a single atomic group (all rows or none). The v0.1
//! single-row restriction ([STL-149]) is lifted now that the row codec
//! ([STL-151]), group commit ([STL-192]), and abort rollback ([STL-216]) make the
//! group atomic.
//!
//! [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
//!
//! ## What this rejects (with a clear bind error, never a wrong write)
//!
//! `INSERT … SELECT`, a `WHERE` outside the shared `SELECT`
//! predicate vocabulary (an `AND`/`OR` chain, a column-to-column comparison, …),
//! updating the key column, `RETURNING`, `ON CONFLICT`, `USING`/`FROM` joins,
//! qualified names, a `NULL` business key, and out-of-range literals. A `NULL`
//! **value column** is accepted (it folds to `None`, [STL-154]); a `NULL` key is
//! not. Folding a `TIMESTAMP`/`DATE` literal is still out of scope (no civil-time
//! codec — mirrors the [`AS OF`](crate::select) stance).
//!
//! [STL-151]: https://allegromusic.atlassian.net/browse/STL-151

use sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, Insert, ObjectName, SetExpr,
    Statement as SqlStatement, TableFactor, TableObject, TableWithJoins, Update,
};
use stele_catalog::{ColumnDef, SchemaId, TableSchema, ValidTimeSpec};
use stele_common::period::Interval;
use stele_common::time::{SystemTimeMicros, VALID_TIME_OPEN};
use stele_common::types::{LogicalType, ScalarValue};

use crate::ast::Statement;
use crate::fold::{self, FoldError};
use crate::select::{
    AsOfError, BindContext, BoundPredicate, SelectError, TableResolution, bind_where_predicate,
    resolve_as_of, resolve_table_at,
};

/// One row of a multi-row `INSERT … VALUES (…), (…), …` ([STL-228]).
///
/// Holds the already-folded business key, value columns, and (valid-time)
/// interval a single-row [`BoundDml::Insert`] would carry. The engine expands an
/// [`InsertRows`](BoundDml::InsertRows) into one [`Insert`](BoundDml::Insert) per
/// row, applied as one atomic group.
///
/// [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertRow {
    /// The business key (the first column's value).
    pub key: ScalarValue,
    /// The value columns' values, in schema order, exactly as a single-row
    /// [`Insert`](BoundDml::Insert) carries them (each `None` for a SQL `NULL`
    /// cell; period columns kept as [`ScalarValue::Timestamp`] cells on a
    /// valid-time table).
    pub values: Vec<Option<ScalarValue>>,
    /// The `[from, to)` valid-time period, or `None` for a system-only table —
    /// derived from this row's own period-column bounds.
    pub valid: Option<Interval>,
}

/// A bound `INSERT` / `UPDATE` / `DELETE`, ready for the engine to apply.
///
/// Carries the resolved table, the schema version it bound under, and the
/// already-folded [`ScalarValue`]s for the business key and the row's value
/// columns. See the [module docs](self) for the key + value-column mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundDml {
    /// `INSERT`: open a fresh `[commit, +∞)` period for `key` carrying the row's
    /// value columns.
    Insert {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key (the first column's value).
        key: ScalarValue,
        /// The value columns' values, in schema order (the columns *after* the
        /// business key). Each is `None` for a SQL `NULL` cell ([STL-154]). The
        /// engine packs these into the stored payload with the
        /// [row codec](stele_common::row_codec). On a valid-time table the period
        /// columns are *included here* as [`ScalarValue::Timestamp`] cells (their
        /// literal bounds), so the row codec is width-agnostic to the valid-time
        /// opt-in; the `valid` interval is *derived* from the same two bounds and
        /// framed onto the stored payload ([STL-194]).
        values: Vec<Option<ScalarValue>>,
        /// The `[from, to)` valid-time period for this write, or `None` for a
        /// system-only table. `Some` for a valid-time table — the engine frames
        /// it onto the stored payload; `to` is [`VALID_TIME_OPEN`] when the
        /// `INSERT` named only the start bound ([STL-194]).
        valid: Option<Interval>,
    },
    /// A multi-row `INSERT … VALUES (…), (…), …` ([STL-228]): every `VALUES` row
    /// bound to its own key + value columns.
    ///
    /// Like the scan variants, this never reaches the per-key write path
    /// directly: the engine expands it into one [`Insert`](Self::Insert) per row
    /// and applies the whole set as a single atomic group (one WAL record + one
    /// fsync), so a failure on any row aborts the statement and leaves **zero**
    /// rows ([STL-216]). The binder emits this only for **two or more** rows; a
    /// single-row `INSERT` stays the point [`Insert`](Self::Insert), byte-for-byte
    /// the pre-STL-228 plan.
    ///
    /// [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
    InsertRows {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The rows to insert, in statement order ([`InsertRow`]).
        rows: Vec<InsertRow>,
    },
    /// `UPDATE`: close `key`'s prior period and open a new one. The `assignments`
    /// overwrite the named value columns; any column the `SET` does not name keeps
    /// its prior value (the engine reads the current row and merges).
    Update {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key the `WHERE` clause selected.
        key: ScalarValue,
        /// The `SET` clause as `(value-column index, new value)` pairs, where the
        /// index is 0-based over the **value columns** (the columns after the
        /// business key). A `None` value is a SQL `NULL` ([STL-154]). On a
        /// valid-time table the period columns appear here too, carrying the new
        /// version's bounds as [`ScalarValue::Timestamp`] cells ([STL-194]).
        assignments: Vec<(usize, Option<ScalarValue>)>,
        /// The new version's `[from, to)` valid-time period, or `None` for a
        /// system-only table ([STL-194]). A valid-time `UPDATE` opens a new period
        /// and so must `SET` the start bound; the end bound defaults to
        /// [`VALID_TIME_OPEN`] when the `SET` omits it.
        valid: Option<Interval>,
    },
    /// `DELETE`: close `key`'s prior period with no successor (a period close,
    /// not a row removal — history is preserved).
    Delete {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key the `WHERE` clause selected.
        key: ScalarValue,
    },
    /// `UPDATE` selecting its rows by **predicate** rather than a single key — a
    /// non-key `WHERE`, or no `WHERE` at all (whole-table) ([STL-229]).
    ///
    /// The engine runs this as a **scan-then-write** plan: enumerate the business
    /// keys of the live rows matching [`filter`](Self::UpdateScan::filter) at the
    /// statement snapshot, then apply one [`Update`](Self::Update) per matched key
    /// as a single atomic group. It never reaches the per-key write path directly —
    /// the engine expands it before buffering or applying.
    ///
    /// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
    UpdateScan {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The bound `WHERE` predicate selecting the rows to update — the same
        /// vocabulary a `SELECT`'s `WHERE` binds ([STL-213]) — or `None` for a
        /// whole-table `UPDATE` (every live row matches).
        filter: Option<BoundPredicate>,
        /// The `SET` clause, exactly as on [`Update`](Self::Update): `(value-column
        /// index, new value)` pairs applied read-modify-write per matched key.
        assignments: Vec<(usize, Option<ScalarValue>)>,
        /// The new versions' `[from, to)` valid-time period, or `None` for a
        /// system-only table — as on [`Update`](Self::Update); every matched key's
        /// new version carries the same bounds ([STL-194]).
        valid: Option<Interval>,
    },
    /// `DELETE` selecting its rows by **predicate** rather than a single key — a
    /// non-key `WHERE`, or no `WHERE` at all (whole-table) ([STL-229]).
    ///
    /// Scan-then-write like [`UpdateScan`](Self::UpdateScan): the engine expands it
    /// into one [`Delete`](Self::Delete) per matched live key, applied as a single
    /// atomic group.
    DeleteScan {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The bound `WHERE` predicate selecting the rows to delete, or `None` for
        /// a whole-table `DELETE`.
        filter: Option<BoundPredicate>,
    },
    /// `MERGE INTO … USING … ON … WHEN …` — the upsert plan ([STL-230]).
    ///
    /// Like the scan variants, this never reaches the per-key write path directly:
    /// the engine resolves each source row against the target's live keys at the
    /// statement snapshot (matched ⇒ [`Update`](Self::Update), not matched ⇒
    /// [`Insert`](Self::Insert)) and applies the whole set as a single atomic
    /// group. See [`BoundMerge`](crate::merge::BoundMerge).
    ///
    /// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
    Merge(crate::merge::BoundMerge),
}

impl BoundDml {
    /// The table this operation writes.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            Self::Insert { table, .. }
            | Self::InsertRows { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. }
            | Self::UpdateScan { table, .. }
            | Self::DeleteScan { table, .. } => table,
            Self::Merge(merge) => &merge.table,
        }
    }
}

/// Why binding a parsed statement into a [`BoundDml`] failed.
///
/// The input parsed as valid SQL; these are the bind-time reasons it is not a
/// DML operation v0.1 can apply. Every variant is a *clear* refusal rather than
/// a silently wrong write ([STL-149] Definition of Done).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DmlError {
    /// The statement is not an `INSERT` / `UPDATE` / `DELETE`. The caller routes
    /// those elsewhere ([`bind_select`](crate::bind_select) / [`bind_ddl`](crate::bind_ddl)).
    #[error("not a DML statement")]
    NotDml,

    /// A clause, dialect extension, or statement shape outside the v0.1 DML
    /// surface (`RETURNING`, `ON CONFLICT`, `USING`, a join, `INSERT … SELECT`,
    /// a non-`<key> = <literal>` `WHERE`, …).
    #[error("{0} is not supported in v0.1 DML")]
    Unsupported(String),

    /// A schema- or database-qualified name (a table or column with more than
    /// one part, e.g. `public.account` or `account.id`). v0.1 has a single
    /// implicit namespace, so only bare names resolve.
    #[error("qualified name {0:?} — only bare names are supported in v0.1")]
    QualifiedName(String),

    /// A single-part name (table or column) that is not a plain identifier.
    #[error("name {0:?} is not a plain identifier")]
    InvalidName(String),

    /// The catalog has never registered this table name.
    #[error("unknown table {0:?}")]
    UnknownTable(String),

    /// The bind snapshot precedes the table's first commit — a write *before the
    /// table existed*. Mirrors [`SelectError::BeforeHistory`].
    #[error(
        "table {table:?} did not exist at the bind snapshot {snapshot} (first commit at {first_commit})"
    )]
    BeforeHistory {
        /// The table written.
        table: String,
        /// The bind snapshot, in system-time microseconds.
        snapshot: i64,
        /// The table's first-commit system time; `snapshot` precedes it.
        first_commit: i64,
    },

    /// The table exists in the catalog timeline but is not live at the snapshot —
    /// dropped, or in a re-creation gap.
    #[error(
        "table {table:?} is not live at the bind snapshot {snapshot} (dropped, or not yet created)"
    )]
    TableNotLive {
        /// The table written.
        table: String,
        /// The bind snapshot, in system-time microseconds.
        snapshot: i64,
    },

    /// The table declares no columns — every table has at least a business-key
    /// column, so this guards an otherwise-impossible shape (DDL never creates
    /// one) rather than panicking.
    #[error("table {table:?} has no columns")]
    EmptyTable {
        /// The table written.
        table: String,
    },

    /// A row of a multi-row `INSERT … VALUES (…), (…), …` ([STL-228]) failed to
    /// bind. The wrapped error is exactly the one a single-row `INSERT` of that
    /// row would give (arity, type mismatch, bad literal, a `NULL` key, …); this
    /// names which row it occurred on (1-based, in statement order). A malformed
    /// *column list* is a statement-level error, reported un-wrapped — it is not a
    /// property of any one row.
    ///
    /// [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
    #[error("INSERT VALUES row {row}: {source}")]
    RowError {
        /// The 1-based position of the offending row in the `VALUES` list.
        row: usize,
        /// The per-row bind failure, exactly as a single-row `INSERT` reports it.
        #[source]
        source: Box<DmlError>,
    },

    /// An `INSERT`'s value count does not match the target column count.
    #[error("INSERT supplies {found} value(s) for {expected} column(s)")]
    ColumnCountMismatch {
        /// The number of target columns.
        expected: usize,
        /// The number of values supplied.
        found: usize,
    },

    /// A column named in an `INSERT` column list or an `UPDATE … SET` target is
    /// not a column of the table.
    #[error("column {column:?} does not exist in table {table:?}")]
    UnknownColumn {
        /// The table written.
        table: String,
        /// The column named that the schema does not contain.
        column: String,
    },

    /// An `INSERT` column list omitted a required column (the key or the payload).
    #[error("INSERT into {table:?} does not supply a value for column {column:?}")]
    MissingColumn {
        /// The table written.
        table: String,
        /// The required column with no supplied value.
        column: String,
    },

    /// An `INSERT` column list named the same column twice. Keeping the last
    /// value would silently bind the wrong key/payload, so it is rejected.
    #[error("INSERT into {table:?} names column {column:?} more than once")]
    DuplicateColumn {
        /// The table written.
        table: String,
        /// The column named more than once.
        column: String,
    },

    /// An `UPDATE` targeted the business-key column. v0.1 cannot rewrite the key
    /// (it is the identity a version's history hangs on).
    #[error("UPDATE of table {table:?} cannot assign to the business-key column {column:?}")]
    CannotUpdateKey {
        /// The table written.
        table: String,
        /// The key column the `SET` clause tried to assign.
        column: String,
    },

    /// An `UPDATE` / `DELETE` `WHERE` did not bind as a row-selection predicate.
    /// DML shares the `SELECT` `WHERE` vocabulary ([STL-229]): a single comparison
    /// anchored on one column, with the six comparison operators and integer
    /// arithmetic ([STL-213]) — the wrapped [`SelectError`] names the unsupported
    /// shape, unknown column, or bad literal exactly as a `SELECT` would.
    ///
    /// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
    #[error("DML WHERE: {0}")]
    Predicate(#[source] SelectError),

    /// A literal's shape does not match its column's type (e.g. a string for an
    /// `int4` column).
    #[error("value {found} for column {column:?} in table {table:?} is not a {expected}")]
    TypeMismatch {
        /// The table written.
        table: String,
        /// The column the value was bound to.
        column: String,
        /// The column's declared type.
        expected: LogicalType,
        /// A short description of the value actually given.
        found: String,
    },

    /// A literal is the right shape for its column's type but cannot be
    /// represented (out of range, not an integer, or a malformed civil-time
    /// literal).
    #[error(
        "literal {literal:?} is not a valid {ty} value for column {column:?} in table {table:?}{}",
        reason.map(|r| format!(" ({r})")).unwrap_or_default()
    )]
    BadLiteral {
        /// The table written.
        table: String,
        /// The column the value was bound to.
        column: String,
        /// The column's declared type.
        ty: LogicalType,
        /// The offending literal text.
        literal: String,
        /// A short, stable reason from the type's codec, when it has one (e.g.
        /// `"day out of range for month"` for a `timestamptz`); `None` for the
        /// integer types, whose failure is self-evident from the literal.
        reason: Option<&'static str>,
    },

    /// A `NULL` was bound to the **business key**. A key is the identity a
    /// version's history hangs on and is never null. (A `NULL` *payload* is
    /// supported — it folds to `None` through `fold_payload`, [STL-154].)
    #[error("NULL is not supported for column {column:?} in table {table:?} in v0.1 DML")]
    NullValue {
        /// The table written.
        table: String,
        /// The column a `NULL` was bound to.
        column: String,
    },

    /// A write to a valid-time table did not supply its period **start** (`from`)
    /// bound: the `INSERT` omitted that column or gave it `NULL`, or the `UPDATE`'s
    /// `SET` did not assign it. Every version of a valid-time row must say when it
    /// begins being true; only the `to` bound defaults (to an open period,
    /// [`VALID_TIME_OPEN`]) ([STL-194]).
    #[error("valid-time table {table:?} requires a value for its period start column {column:?}")]
    ValidTimeStartRequired {
        /// The valid-time table written.
        table: String,
        /// The period's `from` (start) column.
        column: String,
    },

    /// A valid-time period boundary (`vf` / `vt`) could not be folded to a concrete
    /// instant. The bounds accept the same shapes a [`FOR VALID_TIME AS OF`](crate::select)
    /// instant does — an integer microsecond value, `now()`, or `now() ± interval`
    /// — and are *not* civil-time `TIMESTAMP` literals (no civil-time codec yet).
    #[error(
        "valid-time bound for column {column:?} in table {table:?} is not a valid instant: {source}"
    )]
    BadValidTimeBound {
        /// The valid-time table written.
        table: String,
        /// The period boundary column the bad bound was given for.
        column: String,
        /// The fold failure from the shared `AS OF` resolver.
        source: AsOfError,
    },

    /// The valid-time period a write named is empty or reversed (`from >= to`).
    /// Half-open `[from, to)` requires the start strictly before the end.
    #[error(
        "valid-time period for table {table:?} is empty or reversed: from ({from}) must be < to ({to})"
    )]
    EmptyValidInterval {
        /// The valid-time table written.
        table: String,
        /// The period's resolved start microseconds.
        from: i64,
        /// The period's resolved end microseconds.
        to: i64,
    },
}

/// Bind a parsed [`Statement`] into a [`BoundDml`].
///
/// Routes on the statement kind, resolves the table against the catalog at the
/// context snapshot (the same resolution [`bind_select`](crate::bind_select)
/// uses), enforces the v0.1 `(key, payload)` shape, and folds the key / payload
/// literals into typed [`ScalarValue`]s. See the [module docs](self) for the full
/// surface.
///
/// # Errors
///
/// [`DmlError::NotDml`] if the statement is not `INSERT` / `UPDATE` / `DELETE`;
/// otherwise a [`DmlError`] variant naming the unknown table / column,
/// unsupported shape, or bad literal.
pub fn bind_dml(stmt: &Statement, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    // An admin command (CHECKPOINT / FLUSH) has no SQL body, so it is "not DML".
    let Some(body) = stmt.sql() else {
        return Err(DmlError::NotDml);
    };
    match body {
        SqlStatement::Insert(insert) => bind_insert(insert, ctx),
        SqlStatement::Update(update) => bind_update(update, ctx),
        SqlStatement::Delete(delete) => bind_delete(delete, ctx),
        SqlStatement::Merge(merge) => crate::merge::bind_merge(merge, ctx),
        _ => Err(DmlError::NotDml),
    }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

fn bind_insert(insert: &Insert, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    reject_insert_extensions(insert)?;

    let TableObject::TableName(name) = &insert.table else {
        return Err(DmlError::Unsupported(
            "INSERT into a table function or subquery".to_owned(),
        ));
    };
    let table = bare_name(name)?;
    let (schema, key_col, value_cols) = resolve_shape(ctx, &table)?;

    let rows = values_rows(insert)?;

    // The optional column list is row-independent — validate it once (a malformed
    // list is a statement-level error, not a property of any one row), then bind
    // every row against it. `None` means positional mapping.
    let names: Option<Vec<String>> = match insert.columns.as_slice() {
        [] => None,
        cols => Some(validated_columns(&table, cols, schema)?),
    };
    let schema_id = schema.schema_id();

    match rows.as_slice() {
        // A single row is the point `INSERT` — and reports its arity/type/literal
        // errors *unwrapped*, byte-for-byte the pre-STL-228 surface ([STL-149]).
        [row] => {
            let (key, values, valid) = bind_insert_row(
                row,
                &table,
                schema,
                key_col,
                value_cols,
                names.as_deref(),
                ctx,
            )?;
            Ok(BoundDml::Insert {
                table,
                schema_id,
                key,
                values,
                valid,
            })
        }
        // Two or more rows ([STL-228]): bind each through the same per-row path,
        // wrapping any failure with its 1-based row position so the diagnostic
        // names the offending row. The engine applies the set as one atomic group.
        rows => {
            let bound = rows
                .iter()
                .enumerate()
                .map(|(i, row)| {
                    bind_insert_row(
                        row,
                        &table,
                        schema,
                        key_col,
                        value_cols,
                        names.as_deref(),
                        ctx,
                    )
                    .map(|(key, values, valid)| InsertRow { key, values, valid })
                    .map_err(|source| DmlError::RowError {
                        row: i + 1,
                        source: Box::new(source),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(BoundDml::InsertRows {
                table,
                schema_id,
                rows: bound,
            })
        }
    }
}

/// The folded business key, value columns, and valid-time interval one `VALUES`
/// row yields — the shared product of [`bind_insert_row`], assembled into a
/// [`BoundDml::Insert`] or an [`InsertRow`].
type BoundInsertRow = (ScalarValue, Vec<Option<ScalarValue>>, Option<Interval>);

/// Bind one `VALUES` row against the table's columns: align the row's expressions
/// to the schema columns (positionally, or by `names` when an explicit column
/// list was given), fold the business key and value columns, and derive the
/// valid-time interval. Shared by the single-row [`BoundDml::Insert`] and every
/// row of a multi-row [`BoundDml::InsertRows`] ([STL-228]), so both paths reject
/// the same arities, type mismatches, and bad literals identically.
fn bind_insert_row(
    row: &[Expr],
    table: &str,
    schema: &TableSchema,
    key_col: &ColumnDef,
    value_cols: &[ColumnDef],
    names: Option<&[String]>,
    ctx: &BindContext,
) -> Result<BoundInsertRow, DmlError> {
    // Resolve, for every schema column in declaration order, the value expression
    // that supplies it (`None` when omitted). With no explicit column list the
    // values map positionally (and the count must match exactly); with a list,
    // each schema column takes the value at its name's position. An omitted column
    // is only legal for the valid-time period's *end* bound (which opens the
    // period); every other omission is a `MissingColumn` at the point of use.
    let columns = schema.columns();
    let exprs: Vec<Option<&Expr>> = match names {
        None => {
            if row.len() != columns.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: columns.len(),
                    found: row.len(),
                });
            }
            row.iter().map(Some).collect()
        }
        Some(names) => {
            if names.len() != row.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: names.len(),
                    found: row.len(),
                });
            }
            columns
                .iter()
                .map(|column| {
                    names
                        .iter()
                        .position(|n| n == column.name())
                        .map(|i| &row[i])
                })
                .collect()
        }
    };

    // `exprs` is aligned to `columns`: the first is the business key, the rest are
    // the value columns in order.
    let key_expr = exprs[0].ok_or_else(|| DmlError::MissingColumn {
        table: table.to_owned(),
        column: key_col.name().to_owned(),
    })?;
    let key = fold_value(key_expr, table, key_col)?;
    let (values, valid) = fold_value_columns(
        table,
        value_cols,
        &exprs[1..],
        schema.temporal().valid_time(),
        ctx.snapshot,
    )?;
    Ok((key, values, valid))
}

/// Reject the dialect-specific `INSERT` forms outside the v0.1 surface, before
/// looking at the values.
fn reject_insert_extensions(insert: &Insert) -> Result<(), DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if insert.or.is_some() {
        return reject("INSERT OR …");
    }
    if insert.ignore {
        return reject("INSERT IGNORE");
    }
    if insert.overwrite {
        return reject("INSERT OVERWRITE");
    }
    if insert.replace_into {
        return reject("REPLACE INTO");
    }
    if !insert.assignments.is_empty() {
        return reject("INSERT … SET");
    }
    if insert.partitioned.is_some() {
        return reject("partitioned INSERT");
    }
    if insert.on.is_some() {
        return reject("INSERT … ON CONFLICT / ON DUPLICATE KEY");
    }
    if insert.returning.is_some() {
        return reject("INSERT … RETURNING");
    }
    if insert.multi_table_insert_type.is_some() {
        return reject("INSERT ALL / INSERT FIRST");
    }
    Ok(())
}

/// Every row of an `INSERT`'s `VALUES`, after rejecting `INSERT … SELECT` and the
/// value-less form. v0.1 rejected more than one row ([STL-149]); the machinery
/// that made that the safe choice — the row codec ([STL-151]), group commit
/// ([STL-192]), and abort rollback ([STL-216]) — has since landed, so a multi-row
/// list now binds and applies as one atomic group ([STL-228]).
fn values_rows(insert: &Insert) -> Result<Vec<&[Expr]>, DmlError> {
    let Some(query) = insert.source.as_deref() else {
        return Err(DmlError::Unsupported("INSERT without VALUES".to_owned()));
    };
    let SetExpr::Values(values) = query.body.as_ref() else {
        return Err(DmlError::Unsupported("INSERT … SELECT".to_owned()));
    };
    if values.rows.is_empty() {
        // A `VALUES` with no rows is not producible by the parser; guard rather
        // than emit an empty (no-op) write group.
        return Err(DmlError::Unsupported(
            "INSERT with no VALUES rows".to_owned(),
        ));
    }
    Ok(values
        .rows
        .iter()
        .map(|row| row.content.as_slice())
        .collect())
}

/// Resolve an `INSERT` column list to bare names in positional order, rejecting a
/// name that is not a real column ([`UnknownColumn`](DmlError::UnknownColumn)) or
/// that repeats ([`DuplicateColumn`](DmlError::DuplicateColumn) — keeping only the
/// last value for a repeated name would silently bind the wrong cell). The caller
/// then matches a target column to the value at its position in this list.
pub(crate) fn validated_columns(
    table: &str,
    cols: &[ObjectName],
    schema: &TableSchema,
) -> Result<Vec<String>, DmlError> {
    let mut names: Vec<String> = Vec::with_capacity(cols.len());
    for name in cols {
        let name = bare_name(name)?;
        if schema.column(&name).is_none() {
            return Err(DmlError::UnknownColumn {
                table: table.to_owned(),
                column: name,
            });
        }
        if names.iter().any(|prev| prev == &name) {
            return Err(DmlError::DuplicateColumn {
                table: table.to_owned(),
                column: name,
            });
        }
        names.push(name);
    }
    Ok(names)
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn bind_update(update: &Update, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if update.from.is_some() {
        return reject("UPDATE … FROM");
    }
    if update.returning.is_some() {
        return reject("UPDATE … RETURNING");
    }
    if update.or.is_some() {
        return reject("UPDATE OR …");
    }
    if !update.order_by.is_empty() {
        return reject("UPDATE … ORDER BY");
    }
    if update.limit.is_some() {
        return reject("UPDATE … LIMIT");
    }

    let table = table_of(&update.table)?;
    let (schema, key_col, value_cols) = resolve_shape(ctx, &table)?;

    // One or more `SET <col> = <value>` assignments, each targeting a value
    // column. Columns the SET does not name keep their prior value — the engine
    // reads the current row and merges, so the binder only carries the overrides.
    if update.assignments.is_empty() {
        return Err(DmlError::Unsupported(
            "UPDATE with no SET assignments".to_owned(),
        ));
    }
    // A valid-time UPDATE opens a *new* period, so its SET supplies the new
    // version's bounds (the period columns fold as instants, like a `FOR
    // VALID_TIME AS OF`); a system-only UPDATE carries no interval. Tracked while
    // walking the assignments and reconciled into the interval below.
    let period = schema.temporal().valid_time();
    let mut assignments: Vec<(usize, Option<ScalarValue>)> = Vec::new();
    let mut from: Option<i64> = None;
    let mut to: Option<i64> = None;
    for assignment in &update.assignments {
        let target = assignment_column(assignment)?;
        if target == key_col.name() {
            return Err(DmlError::CannotUpdateKey {
                table: table.clone(),
                column: target.to_owned(),
            });
        }
        let idx = value_cols
            .iter()
            .position(|c| c.name() == target)
            .ok_or_else(|| DmlError::UnknownColumn {
                table: table.clone(),
                column: target.to_owned(),
            })?;
        // A column assigned twice would silently keep only the last value — reject
        // it rather than guess which the user meant.
        if assignments.iter().any(|(prev, _)| *prev == idx) {
            return Err(DmlError::Unsupported(format!(
                "UPDATE assigns column {target:?} more than once"
            )));
        }
        let value = match period_role(period, target) {
            Some(PeriodRole::From) => {
                let micros =
                    fold_from_bound(Some(&assignment.value), &table, target, ctx.snapshot)?;
                from = Some(micros);
                Some(ScalarValue::Timestamp(micros))
            }
            Some(PeriodRole::To) => {
                let micros = fold_to_bound(Some(&assignment.value), &table, target, ctx.snapshot)?;
                to = Some(micros);
                Some(ScalarValue::Timestamp(micros))
            }
            None => fold_payload(&assignment.value, &table, &value_cols[idx])?,
        };
        assignments.push((idx, value));
    }

    // The end bound defaults to an open period when the SET omits it — synthesize
    // the cell so the row codec payload and the framed interval agree.
    if let Some(period) = period
        && to.is_none()
    {
        if let Some(to_idx) = value_cols
            .iter()
            .position(|c| c.name() == period.to_column())
        {
            assignments.push((to_idx, Some(ScalarValue::Timestamp(VALID_TIME_OPEN.0))));
        }
        to = Some(VALID_TIME_OPEN.0);
    }
    let valid = build_interval(&table, period, from, to)?;

    Ok(
        match dml_selection(update.selection.as_ref(), &table, schema)? {
            DmlSelection::Key(key) => BoundDml::Update {
                table,
                schema_id: schema.schema_id(),
                key,
                assignments,
                valid,
            },
            DmlSelection::Scan(filter) => BoundDml::UpdateScan {
                table,
                schema_id: schema.schema_id(),
                filter,
                assignments,
                valid,
            },
        },
    )
}

/// The single, unqualified column an `UPDATE … SET` assignment targets.
pub(crate) fn assignment_column(assignment: &Assignment) -> Result<&str, DmlError> {
    match &assignment.target {
        AssignmentTarget::ColumnName(name) => single_ident(name),
        AssignmentTarget::Tuple(_) => Err(DmlError::Unsupported(
            "UPDATE … SET (a, b) = … tuple assignment".to_owned(),
        )),
    }
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn bind_delete(delete: &Delete, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if !delete.tables.is_empty() {
        return reject("multi-table DELETE");
    }
    if delete.using.is_some() {
        return reject("DELETE … USING");
    }
    if delete.returning.is_some() {
        return reject("DELETE … RETURNING");
    }
    if !delete.order_by.is_empty() {
        return reject("DELETE … ORDER BY");
    }
    if delete.limit.is_some() {
        return reject("DELETE … LIMIT");
    }

    let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) = &delete.from;
    let [target] = tables.as_slice() else {
        return Err(DmlError::Unsupported(
            "DELETE from multiple tables".to_owned(),
        ));
    };
    let table = table_of(target)?;
    let (schema, _key_col, _value_cols) = resolve_shape(ctx, &table)?;

    Ok(
        match dml_selection(delete.selection.as_ref(), &table, schema)? {
            DmlSelection::Key(key) => BoundDml::Delete {
                table,
                schema_id: schema.schema_id(),
                key,
            },
            DmlSelection::Scan(filter) => BoundDml::DeleteScan {
                table,
                schema_id: schema.schema_id(),
                filter,
            },
        },
    )
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Resolve `table` at the context snapshot and split its schema into the business
/// key (the first column) and the value columns (the rest), returning the schema
/// alongside.
pub(crate) fn resolve_shape<'a>(
    ctx: &'a BindContext,
    table: &str,
) -> Result<(&'a TableSchema, &'a ColumnDef, &'a [ColumnDef]), DmlError> {
    let schema = match resolve_table_at(ctx.catalog, table, ctx.snapshot) {
        TableResolution::Found(schema) => schema,
        TableResolution::Unknown => return Err(DmlError::UnknownTable(table.to_owned())),
        TableResolution::BeforeHistory { first_commit } => {
            return Err(DmlError::BeforeHistory {
                table: table.to_owned(),
                snapshot: ctx.snapshot.0,
                first_commit: first_commit.0,
            });
        }
        TableResolution::NotLive => {
            return Err(DmlError::TableNotLive {
                table: table.to_owned(),
                snapshot: ctx.snapshot.0,
            });
        }
    };
    // The first column is the business key; the rest are value columns (possibly
    // none, for a key-only table).
    let (key, values) = schema
        .columns()
        .split_first()
        .ok_or_else(|| DmlError::EmptyTable {
            table: table.to_owned(),
        })?;
    Ok((schema, key, values))
}

/// How an `UPDATE` / `DELETE` selects the rows it writes ([STL-229]).
enum DmlSelection {
    /// `WHERE <key> = <literal>` — the point fast path: one business key, no scan.
    Key(ScalarValue),
    /// Any other `WHERE` (`Some`) or no `WHERE` at all (`None`, whole-table) —
    /// the scan-then-write plan.
    Scan(Option<BoundPredicate>),
}

/// Bind an `UPDATE` / `DELETE` `WHERE` clause to its row selection ([STL-229]).
///
/// A missing `WHERE` is a whole-table write (match-all). Otherwise the predicate
/// binds through the same [`bind_where_predicate`] a `SELECT`'s `WHERE` uses —
/// one vocabulary, not a DML-special dialect. A predicate that is exactly
/// `<key> = <literal>` (in either operand order,
/// [`BoundPredicate::key_equality`]) keeps the existing single-key fast path:
/// it lowers to the point op with no scan, byte-for-byte the pre-STL-229 plan.
fn dml_selection(
    selection: Option<&Expr>,
    table: &str,
    schema: &TableSchema,
) -> Result<DmlSelection, DmlError> {
    let Some(expr) = selection else {
        return Ok(DmlSelection::Scan(None));
    };
    let predicate = bind_where_predicate(expr, schema, table).map_err(DmlError::Predicate)?;
    if let Some(key) = predicate.key_equality() {
        return Ok(DmlSelection::Key(key.clone()));
    }
    Ok(DmlSelection::Scan(Some(predicate)))
}

/// Which boundary of a table's valid-time period a column is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeriodRole {
    /// The period's `from` (inclusive start) column.
    From,
    /// The period's `to` (exclusive end) column.
    To,
}

/// The valid-time role of `column`, or `None` when the table has no valid axis or
/// the column is an ordinary value column.
pub(crate) fn period_role(period: Option<&ValidTimeSpec>, column: &str) -> Option<PeriodRole> {
    let period = period?;
    if period.from_column() == column {
        Some(PeriodRole::From)
    } else if period.to_column() == column {
        Some(PeriodRole::To)
    } else {
        None
    }
}

/// Fold an `INSERT`'s value columns into row-codec cells and, for a valid-time
/// table, the `[from, to)` interval framed onto the stored payload.
///
/// `exprs` is aligned to `value_cols`; a `None` entry is a column omitted from the
/// `INSERT`'s column list. For a system-only table every column must be supplied
/// and the returned interval is `None`. For a valid-time table the two period
/// columns are *also* kept as [`ScalarValue::Timestamp`] cells — so the row codec
/// stays width-agnostic to the valid-time opt-in (the engine's read path slices
/// the same N value columns either way) — and the interval is derived from their
/// bounds: the `from` bound is mandatory, the `to` bound defaults to
/// [`VALID_TIME_OPEN`] when omitted or `NULL`.
fn fold_value_columns(
    table: &str,
    value_cols: &[ColumnDef],
    exprs: &[Option<&Expr>],
    period: Option<&ValidTimeSpec>,
    now: SystemTimeMicros,
) -> Result<(Vec<Option<ScalarValue>>, Option<Interval>), DmlError> {
    let mut values = Vec::with_capacity(value_cols.len());
    let mut from: Option<i64> = None;
    let mut to: Option<i64> = None;
    for (column, expr) in value_cols.iter().zip(exprs) {
        let value = match period_role(period, column.name()) {
            Some(PeriodRole::From) => {
                let micros = fold_from_bound(*expr, table, column.name(), now)?;
                from = Some(micros);
                Some(ScalarValue::Timestamp(micros))
            }
            Some(PeriodRole::To) => {
                let micros = fold_to_bound(*expr, table, column.name(), now)?;
                to = Some(micros);
                Some(ScalarValue::Timestamp(micros))
            }
            None => {
                let expr = expr.ok_or_else(|| DmlError::MissingColumn {
                    table: table.to_owned(),
                    column: column.name().to_owned(),
                })?;
                fold_payload(expr, table, column)?
            }
        };
        values.push(value);
    }
    let valid = build_interval(table, period, from, to)?;
    Ok((values, valid))
}

/// Build the `[from, to)` interval for a write from its resolved bounds and the
/// table's valid-time policy: `None` for a system-only table; for a valid-time
/// table the `from` bound is mandatory ([`DmlError::ValidTimeStartRequired`]) and
/// the `to` bound defaults to [`VALID_TIME_OPEN`].
pub(crate) fn build_interval(
    table: &str,
    period: Option<&ValidTimeSpec>,
    from: Option<i64>,
    to: Option<i64>,
) -> Result<Option<Interval>, DmlError> {
    let Some(period) = period else {
        return Ok(None);
    };
    let from = from.ok_or_else(|| DmlError::ValidTimeStartRequired {
        table: table.to_owned(),
        column: period.from_column().to_owned(),
    })?;
    let to = to.unwrap_or(VALID_TIME_OPEN.0);
    Interval::new(from, to)
        .map(Some)
        .map_err(|_| DmlError::EmptyValidInterval {
            table: table.to_owned(),
            from,
            to,
        })
}

/// Fold a valid-time `from` (period start) bound to its microsecond instant. The
/// start is mandatory — an omitted column (`None`) or a SQL `NULL` is
/// [`DmlError::ValidTimeStartRequired`].
pub(crate) fn fold_from_bound(
    expr: Option<&Expr>,
    table: &str,
    column: &str,
    now: SystemTimeMicros,
) -> Result<i64, DmlError> {
    match expr {
        Some(expr) if !fold::is_null(expr) => fold_instant(expr, table, column, now),
        _ => Err(DmlError::ValidTimeStartRequired {
            table: table.to_owned(),
            column: column.to_owned(),
        }),
    }
}

/// Fold a valid-time `to` (period end) bound. An omitted column (`None`) or a SQL
/// `NULL` opens the period ([`VALID_TIME_OPEN`]).
pub(crate) fn fold_to_bound(
    expr: Option<&Expr>,
    table: &str,
    column: &str,
    now: SystemTimeMicros,
) -> Result<i64, DmlError> {
    match expr {
        Some(expr) if !fold::is_null(expr) => fold_instant(expr, table, column, now),
        _ => Ok(VALID_TIME_OPEN.0),
    }
}

/// Fold a valid-time boundary expression to microseconds with the shared `AS OF`
/// resolver ([`resolve_as_of`]) — an integer microsecond instant, `now()`, or
/// `now() ± interval`. The bounds deliberately reuse the read side's folding, so a
/// value written into a period column resolves the same way it does in a `FOR
/// VALID_TIME AS OF`; they are *not* civil-time `TIMESTAMP` literals (no
/// civil-time codec yet — mirrors the [`AS OF`](crate::select) stance).
fn fold_instant(
    expr: &Expr,
    table: &str,
    column: &str,
    now: SystemTimeMicros,
) -> Result<i64, DmlError> {
    resolve_as_of(expr, now)
        .map(|m| m.0)
        .map_err(|source| DmlError::BadValidTimeBound {
            table: table.to_owned(),
            column: column.to_owned(),
            source,
        })
}

/// Fold a value-column literal, accepting SQL `NULL` as `None` ([STL-154]).
///
/// A value column is nullable end to end (storage carries a `None` cell
/// distinctly), so `NULL` here is a valid write rather than a rejected one — the
/// sibling of [`fold_value`], which still rejects `NULL` for the never-null
/// business key. A present literal folds through [`fold_value`] unchanged.
fn fold_payload(
    expr: &Expr,
    table: &str,
    column: &ColumnDef,
) -> Result<Option<ScalarValue>, DmlError> {
    if fold::is_null(expr) {
        return Ok(None);
    }
    fold_value(expr, table, column).map(Some)
}

/// Fold a literal expression into a [`ScalarValue`] of `column`'s type, rejecting
/// `NULL`, type mismatches, and out-of-range / unsupported literals. Used for the
/// business key (never nullable); a value column folds through [`fold_payload`],
/// which accepts `NULL`. The folding itself lives in the `fold` module; this only
/// attaches the table/column names to the failure.
fn fold_value(expr: &Expr, table: &str, column: &ColumnDef) -> Result<ScalarValue, DmlError> {
    fold::fold_scalar(expr, column.ty()).map_err(|err| fold_err_to_dml(err, table, column))
}

/// Map a table/column-agnostic [`FoldError`] to the DML error that names the
/// table and column it occurred on. Reproduces the binder's pre-existing errors
/// exactly, so the surface is unchanged.
pub(crate) fn fold_err_to_dml(err: FoldError, table: &str, column: &ColumnDef) -> DmlError {
    match err {
        FoldError::Null => DmlError::NullValue {
            table: table.to_owned(),
            column: column.name().to_owned(),
        },
        FoldError::TypeMismatch { found } => DmlError::TypeMismatch {
            table: table.to_owned(),
            column: column.name().to_owned(),
            expected: column.ty(),
            found: found.to_owned(),
        },
        FoldError::BadLiteral { literal, reason } => DmlError::BadLiteral {
            table: table.to_owned(),
            column: column.name().to_owned(),
            ty: column.ty(),
            literal,
            reason,
        },
        FoldError::UnsupportedType(ty) => {
            DmlError::Unsupported(format!("a {ty} literal for column {:?}", column.name()))
        }
    }
}

/// The bare, unqualified table name of a `FROM`/`UPDATE` target — rejecting
/// joins and non-table relations.
fn table_of(twj: &TableWithJoins) -> Result<String, DmlError> {
    if !twj.joins.is_empty() {
        return Err(DmlError::Unsupported("a JOIN in a DML target".to_owned()));
    }
    match &twj.relation {
        TableFactor::Table { name, .. } => bare_name(name),
        _ => Err(DmlError::Unsupported(
            "a non-table DML target (subquery, function, …)".to_owned(),
        )),
    }
}

/// Extract a single, unqualified identifier from an [`ObjectName`] — the table /
/// column name forms v0.1 accepts. Mirrors the DDL binder's `bare_name`.
pub(crate) fn bare_name(name: &ObjectName) -> Result<String, DmlError> {
    single_ident(name).map(ToOwned::to_owned)
}

/// Borrow a single, unqualified identifier out of an [`ObjectName`].
fn single_ident(name: &ObjectName) -> Result<&str, DmlError> {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|id| id.value.as_str())
            .ok_or_else(|| DmlError::InvalidName(name.to_string())),
        _ => Err(DmlError::QualifiedName(name.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use crate::select::{BoundScalar, CompareOp};
    use stele_catalog::{Catalog, TableTemporal};
    use stele_common::time::SystemTimeMicros;

    /// The bind snapshot for the fixtures.
    const NOW: SystemTimeMicros = SystemTimeMicros(2_000_000_000_000_000);

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1, "expected one statement");
        stmts.remove(0)
    }

    /// A catalog with the identity-demo `account (id INT, balance INT)` table,
    /// created at system time `1_000`.
    fn account_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "account",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create account");
        catalog
    }

    fn bind(sql: &str, catalog: &Catalog) -> Result<BoundDml, DmlError> {
        let ctx = BindContext {
            snapshot: NOW,
            catalog,
        };
        bind_dml(&parse_one(sql), &ctx)
    }

    #[test]
    fn binds_the_demo_insert() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1, 100)", &catalog),
            Ok(BoundDml::Insert {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![Some(ScalarValue::Int4(100))],
                valid: None,
            })
        );
    }

    #[test]
    fn binds_the_demo_update() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET balance = 250 WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![(0, Some(ScalarValue::Int4(250)))],
                valid: None,
            })
        );
    }

    #[test]
    fn binds_the_demo_delete() {
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE id = 1", &catalog),
            Ok(BoundDml::Delete {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
            })
        );
    }

    #[test]
    fn insert_column_list_maps_by_name_in_any_order() {
        let catalog = account_catalog();
        // Reversed column list — values must still bind to the right columns.
        assert_eq!(
            bind(
                "INSERT INTO account (balance, id) VALUES (100, 1)",
                &catalog
            ),
            Ok(BoundDml::Insert {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![Some(ScalarValue::Int4(100))],
                valid: None,
            })
        );
    }

    #[test]
    fn where_accepts_the_key_on_either_side() {
        let catalog = account_catalog();
        let flipped = bind("DELETE FROM account WHERE 1 = id", &catalog).expect("bind");
        assert_eq!(
            flipped,
            BoundDml::Delete {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
            }
        );
    }

    #[test]
    fn negative_literals_fold() {
        let catalog = account_catalog();
        let BoundDml::Insert { values, .. } =
            bind("INSERT INTO account VALUES (1, -42)", &catalog).expect("bind")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(values, vec![Some(ScalarValue::Int4(-42))]);
    }

    #[test]
    fn non_dml_is_not_dml() {
        let catalog = account_catalog();
        assert_eq!(
            bind("SELECT balance FROM account", &catalog),
            Err(DmlError::NotDml)
        );
        assert_eq!(
            bind("CREATE TABLE t (a INT) WITH SYSTEM VERSIONING", &catalog),
            Err(DmlError::NotDml)
        );
    }

    #[test]
    fn unknown_table_is_reported() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO ghost VALUES (1, 2)", &catalog),
            Err(DmlError::UnknownTable("ghost".to_owned()))
        );
    }

    #[test]
    fn multi_row_insert_binds_every_row() {
        // STL-228: the v0.1 single-row restriction is lifted — two or more rows
        // bind to `InsertRows`, each folded like its own single-row INSERT.
        let catalog = account_catalog();
        assert_eq!(
            bind(
                "INSERT INTO account VALUES (1, 100), (2, 200), (3, 300)",
                &catalog
            ),
            Ok(BoundDml::InsertRows {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                rows: vec![
                    InsertRow {
                        key: ScalarValue::Int4(1),
                        values: vec![Some(ScalarValue::Int4(100))],
                        valid: None,
                    },
                    InsertRow {
                        key: ScalarValue::Int4(2),
                        values: vec![Some(ScalarValue::Int4(200))],
                        valid: None,
                    },
                    InsertRow {
                        key: ScalarValue::Int4(3),
                        values: vec![Some(ScalarValue::Int4(300))],
                        valid: None,
                    },
                ],
            })
        );
    }

    #[test]
    fn single_row_insert_stays_a_point_insert() {
        // The one-row case is unchanged: it binds the point `Insert`, not
        // `InsertRows` — the common path is byte-for-byte the pre-STL-228 plan.
        let catalog = account_catalog();
        assert!(matches!(
            bind("INSERT INTO account VALUES (1, 100)", &catalog),
            Ok(BoundDml::Insert { .. })
        ));
    }

    #[test]
    fn multi_row_insert_reuses_the_column_list_for_every_row() {
        // STL-228: an explicit column list is validated once and applied to every
        // row, mapping each value to its column by name (here, reversed).
        let catalog = account_catalog();
        assert_eq!(
            bind(
                "INSERT INTO account (balance, id) VALUES (100, 1), (200, 2)",
                &catalog
            ),
            Ok(BoundDml::InsertRows {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                rows: vec![
                    InsertRow {
                        key: ScalarValue::Int4(1),
                        values: vec![Some(ScalarValue::Int4(100))],
                        valid: None,
                    },
                    InsertRow {
                        key: ScalarValue::Int4(2),
                        values: vec![Some(ScalarValue::Int4(200))],
                        valid: None,
                    },
                ],
            })
        );
    }

    #[test]
    fn multi_row_insert_names_the_offending_row() {
        // STL-228: a per-row failure is wrapped with the 1-based row position, and
        // its source is exactly the error a single-row INSERT of that row gives.
        let catalog = account_catalog();
        // Row 2 has a type-mismatched key.
        assert_eq!(
            bind(
                "INSERT INTO account VALUES (1, 100), ('two', 200)",
                &catalog
            ),
            Err(DmlError::RowError {
                row: 2,
                source: Box::new(DmlError::TypeMismatch {
                    table: "account".to_owned(),
                    column: "id".to_owned(),
                    expected: LogicalType::Int4,
                    found: "a string literal".to_owned(),
                }),
            })
        );
        // Row 3 has the wrong arity.
        assert_eq!(
            bind(
                "INSERT INTO account VALUES (1, 100), (2, 200), (3)",
                &catalog
            ),
            Err(DmlError::RowError {
                row: 3,
                source: Box::new(DmlError::ColumnCountMismatch {
                    expected: 2,
                    found: 1,
                }),
            })
        );
    }

    #[test]
    fn multi_row_insert_with_a_bad_column_list_is_a_statement_error() {
        // A malformed column list is a statement-level error, reported un-wrapped
        // (not attributed to any one row).
        let catalog = account_catalog();
        assert_eq!(
            bind(
                "INSERT INTO account (id, nonesuch) VALUES (1, 2), (3, 4)",
                &catalog
            ),
            Err(DmlError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn multi_row_insert_on_a_valid_time_table_lifts_each_rows_interval() {
        // STL-228 × STL-194: each row derives its own `[from, to)` from its period
        // columns, kept as Timestamp cells so the row codec stays width-agnostic.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind(
                "INSERT INTO vt VALUES (1, 100, 10, 20), (2, 200, 30, NULL)",
                &catalog
            ),
            Ok(BoundDml::InsertRows {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                rows: vec![
                    InsertRow {
                        key: ScalarValue::Int4(1),
                        values: vec![
                            Some(ScalarValue::Int4(100)),
                            Some(ScalarValue::Timestamp(10)),
                            Some(ScalarValue::Timestamp(20)),
                        ],
                        valid: Some(Interval::new(10, 20).expect("interval")),
                    },
                    InsertRow {
                        key: ScalarValue::Int4(2),
                        values: vec![
                            Some(ScalarValue::Int4(200)),
                            Some(ScalarValue::Timestamp(30)),
                            Some(ScalarValue::Timestamp(VALID_TIME_OPEN.0)),
                        ],
                        valid: Some(Interval::new(30, VALID_TIME_OPEN.0).expect("open interval")),
                    },
                ],
            })
        );
    }

    #[test]
    fn insert_select_is_rejected() {
        let catalog = account_catalog();
        assert!(matches!(
            bind(
                "INSERT INTO account SELECT id, balance FROM account",
                &catalog
            ),
            Err(DmlError::Unsupported(_))
        ));
    }

    #[test]
    fn wrong_value_count_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1)", &catalog),
            Err(DmlError::ColumnCountMismatch {
                expected: 2,
                found: 1
            })
        );
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES ('one', 100)", &catalog),
            Err(DmlError::TypeMismatch {
                table: "account".to_owned(),
                column: "id".to_owned(),
                expected: LogicalType::Int4,
                found: "a string literal".to_owned(),
            })
        );
    }

    #[test]
    fn out_of_range_literal_is_a_bad_literal() {
        let catalog = account_catalog();
        // 5_000_000_000 overflows i32 (the `balance` column is int4).
        assert_eq!(
            bind("INSERT INTO account VALUES (1, 5000000000)", &catalog),
            Err(DmlError::BadLiteral {
                table: "account".to_owned(),
                column: "balance".to_owned(),
                ty: LogicalType::Int4,
                literal: "5000000000".to_owned(),
                reason: None,
            })
        );
    }

    #[test]
    fn null_payload_is_accepted() {
        // A NULL payload folds to `None` — a valid write, not a rejection
        // ([STL-154]). The key stays its concrete value.
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1, NULL)", &catalog),
            Ok(BoundDml::Insert {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![None],
                valid: None,
            })
        );
    }

    #[test]
    fn null_payload_in_update_is_accepted() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET balance = NULL WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![(0, None)],
                valid: None,
            })
        );
    }

    #[test]
    fn null_key_is_rejected() {
        // The business key is never nullable — a NULL key is still refused
        // ([STL-154] lifts the rejection for the payload only).
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (NULL, 100)", &catalog),
            Err(DmlError::NullValue {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
        // A NULL comparand in a WHERE predicate is refused too — by the shared
        // SELECT predicate binder ([STL-229]), so the diagnostic matches a
        // SELECT's.
        assert!(matches!(
            bind("DELETE FROM account WHERE id = NULL", &catalog),
            Err(DmlError::Predicate(SelectError::UnsupportedPredicate(_)))
        ));
    }

    #[test]
    fn updating_the_key_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET id = 2 WHERE id = 1", &catalog),
            Err(DmlError::CannotUpdateKey {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
    }

    #[test]
    fn updating_an_unknown_column_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET nonesuch = 2 WHERE id = 1", &catalog),
            Err(DmlError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn where_on_a_non_key_column_binds_a_scan_delete() {
        // STL-229: a value-column predicate is no longer rejected — it binds the
        // scan-then-write plan, through the same predicate binder a SELECT uses.
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE balance = 100", &catalog),
            Ok(BoundDml::DeleteScan {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                filter: Some(BoundPredicate {
                    left: BoundScalar::Column(1),
                    op: CompareOp::Eq,
                    right: BoundScalar::Literal(ScalarValue::Int4(100)),
                }),
            })
        );
    }

    #[test]
    fn whole_table_update_binds_a_match_all_scan() {
        // STL-229: no WHERE = every live row (the v0.1 rejection is lifted).
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET balance = 0", &catalog),
            Ok(BoundDml::UpdateScan {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                filter: None,
                assignments: vec![(0, Some(ScalarValue::Int4(0)))],
                valid: None,
            })
        );
    }

    #[test]
    fn whole_table_delete_binds_a_match_all_scan() {
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account", &catalog),
            Ok(BoundDml::DeleteScan {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                filter: None,
            })
        );
    }

    #[test]
    fn non_equality_key_where_binds_a_scan() {
        // A key comparison that is not plain equality cannot take the point fast
        // path — it scans, with the predicate anchored on the key column.
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE id > 1", &catalog),
            Ok(BoundDml::DeleteScan {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                filter: Some(BoundPredicate {
                    left: BoundScalar::Column(0),
                    op: CompareOp::Gt,
                    right: BoundScalar::Literal(ScalarValue::Int4(1)),
                }),
            })
        );
    }

    #[test]
    fn reversed_key_equality_keeps_the_point_fast_path() {
        // `1 = id` is still exactly `<key> = <literal>` — the fast path detection
        // (`BoundPredicate::key_equality`) accepts either operand order, so the
        // statement lowers to the point op with no scan.
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE 1 = id", &catalog),
            Ok(BoundDml::Delete {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
            })
        );
    }

    #[test]
    fn an_arithmetic_key_equality_scans_rather_than_point_writes() {
        // `id + 0 = 1` selects the same row as `id = 1`, but it is not the
        // literal fast-path shape — it binds the scan plan, whose filter the
        // engine evaluates exactly like a SELECT's.
        let catalog = account_catalog();
        assert!(matches!(
            bind("DELETE FROM account WHERE id + 0 = 1", &catalog),
            Ok(BoundDml::DeleteScan {
                filter: Some(_),
                ..
            })
        ));
    }

    #[test]
    fn an_unsupported_where_shape_is_rejected_via_the_shared_binder() {
        // AND chains are outside the shared SELECT predicate vocabulary — the
        // refusal comes from the same binder, wrapped as `DmlError::Predicate`.
        let catalog = account_catalog();
        assert!(matches!(
            bind(
                "DELETE FROM account WHERE id = 1 AND balance = 100",
                &catalog
            ),
            Err(DmlError::Predicate(SelectError::UnsupportedPredicate(_)))
        ));
    }

    /// A catalog with a three-value-column table `wide (id, a, b, c)`
    /// (`a`/`b` int4, `c` text), created at system time `1_000`.
    fn wide_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "wide",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("a", LogicalType::Int4).expect("col"),
                    ColumnDef::new("b", LogicalType::Int4).expect("col"),
                    ColumnDef::new("c", LogicalType::Text).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create wide");
        catalog
    }

    #[test]
    fn multi_column_insert_binds_all_value_columns_in_order() {
        // The row codec ([STL-151]) lets a table be wider than (key, value): the
        // value columns bind in schema order, NULLs included.
        let catalog = wide_catalog();
        assert_eq!(
            bind("INSERT INTO wide VALUES (1, 2, 3, 'x')", &catalog),
            Ok(BoundDml::Insert {
                table: "wide".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(2)),
                    Some(ScalarValue::Int4(3)),
                    Some(ScalarValue::Text("x".to_owned())),
                ],
                valid: None,
            })
        );
        // A reordering column list still maps each value to its column by name.
        assert_eq!(
            bind(
                "INSERT INTO wide (c, id, b, a) VALUES ('x', 1, 3, 2)",
                &catalog
            ),
            Ok(BoundDml::Insert {
                table: "wide".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(2)),
                    Some(ScalarValue::Int4(3)),
                    Some(ScalarValue::Text("x".to_owned())),
                ],
                valid: None,
            })
        );
    }

    #[test]
    fn multi_column_update_sets_a_subset_by_index() {
        // Each SET targets a value column; the index is 0-based over the value
        // columns (a=0, b=1, c=2). Unnamed columns are merged by the engine.
        let catalog = wide_catalog();
        assert_eq!(
            bind("UPDATE wide SET b = 9, c = 'z' WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "wide".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![
                    (1, Some(ScalarValue::Int4(9))),
                    (2, Some(ScalarValue::Text("z".to_owned()))),
                ],
                valid: None,
            })
        );
    }

    #[test]
    fn multi_column_update_rejects_a_repeated_set_target() {
        let catalog = wide_catalog();
        assert!(matches!(
            bind("UPDATE wide SET a = 1, a = 2 WHERE id = 1", &catalog),
            Err(DmlError::Unsupported(_))
        ));
    }

    #[test]
    fn key_only_table_inserts_just_the_key() {
        // A one-column table has no value columns: the row is the key alone.
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "solo",
                vec![ColumnDef::new("id", LogicalType::Int4).expect("col")],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create solo");
        assert_eq!(
            bind("INSERT INTO solo VALUES (1)", &catalog),
            Ok(BoundDml::Insert {
                table: "solo".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![],
                valid: None,
            })
        );
    }

    #[test]
    fn wrong_value_count_for_a_wide_table_is_rejected() {
        let catalog = wide_catalog();
        assert_eq!(
            bind("INSERT INTO wide VALUES (1, 2)", &catalog),
            Err(DmlError::ColumnCountMismatch {
                expected: 4,
                found: 2,
            })
        );
    }

    #[test]
    fn unknown_column_in_insert_list_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account (id, nonesuch) VALUES (1, 2)", &catalog),
            Err(DmlError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn qualified_column_in_where_is_rejected() {
        // The shared SELECT predicate binder ([STL-229]) resolves bare names
        // only, so a qualified column is refused — with the same diagnostic a
        // SELECT's WHERE gives (it sees no bindable column).
        let catalog = account_catalog();
        assert!(matches!(
            bind("DELETE FROM account WHERE account.id = 1", &catalog),
            Err(DmlError::Predicate(SelectError::UnsupportedPredicate(_)))
        ));
    }

    #[test]
    fn duplicate_column_in_insert_list_is_rejected() {
        let catalog = account_catalog();
        // Keeping the last `id` would silently bind the wrong key — reject it.
        assert_eq!(
            bind(
                "INSERT INTO account (id, balance, id) VALUES (1, 100, 2)",
                &catalog
            ),
            Err(DmlError::DuplicateColumn {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
    }

    #[test]
    fn qualified_table_name_is_rejected() {
        let catalog = account_catalog();
        assert!(matches!(
            bind("INSERT INTO public.account VALUES (1, 100)", &catalog),
            Err(DmlError::QualifiedName(_))
        ));
    }

    #[test]
    fn text_and_bool_columns_fold() {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "flags",
                vec![
                    ColumnDef::new("name", LogicalType::Text).expect("col"),
                    ColumnDef::new("on", LogicalType::Bool).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create flags");
        assert_eq!(
            bind("INSERT INTO flags VALUES ('alpha', true)", &catalog),
            Ok(BoundDml::Insert {
                table: "flags".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Text("alpha".to_owned()),
                values: vec![Some(ScalarValue::Bool(true))],
                valid: None,
            })
        );
    }

    // --- valid-time DML ([STL-194]) ----------------------------------------

    /// A catalog with a valid-time `vt (id, balance, vf, vt)` table — the period
    /// columns `vf`/`vt` are declared `TIMESTAMP` columns named by `VALID TIME`.
    fn valid_time_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "vt",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                    ColumnDef::new("vf", LogicalType::Timestamp).expect("col"),
                    ColumnDef::new("vt", LogicalType::Timestamp).expect("col"),
                ],
                TableTemporal::with_valid_time(ValidTimeSpec::new("vf", "vt").expect("spec")),
                SystemTimeMicros(1_000),
            )
            .expect("create vt");
        catalog
    }

    #[test]
    fn insert_lifts_the_period_columns_into_an_interval() {
        // The period columns fold as instants (integer microseconds) into the
        // bound interval, and are *also* kept as Timestamp value cells so the row
        // codec stays width-agnostic to the valid-time opt-in.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind("INSERT INTO vt VALUES (1, 100, 10, 20)", &catalog),
            Ok(BoundDml::Insert {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(100)),
                    Some(ScalarValue::Timestamp(10)),
                    Some(ScalarValue::Timestamp(20)),
                ],
                valid: Some(Interval::new(10, 20).expect("interval")),
            })
        );
    }

    #[test]
    fn insert_omitting_the_end_bound_opens_the_period() {
        // A column list that names only the start bound opens `[from, +∞)` — the
        // omitted `vt` defaults to VALID_TIME_OPEN, in both the interval and the
        // synthesized period cell.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind(
                "INSERT INTO vt (id, balance, vf) VALUES (1, 100, 10)",
                &catalog
            ),
            Ok(BoundDml::Insert {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(100)),
                    Some(ScalarValue::Timestamp(10)),
                    Some(ScalarValue::Timestamp(VALID_TIME_OPEN.0)),
                ],
                valid: Some(Interval::new(10, VALID_TIME_OPEN.0).expect("open interval")),
            })
        );
    }

    #[test]
    fn insert_null_end_bound_opens_the_period() {
        // A positional INSERT can open the period by passing NULL for `vt`.
        let catalog = valid_time_catalog();
        let bound = bind("INSERT INTO vt VALUES (1, 100, 10, NULL)", &catalog).expect("bind");
        assert_eq!(
            bound,
            BoundDml::Insert {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(100)),
                    Some(ScalarValue::Timestamp(10)),
                    Some(ScalarValue::Timestamp(VALID_TIME_OPEN.0)),
                ],
                valid: Some(Interval::new(10, VALID_TIME_OPEN.0).expect("open interval")),
            }
        );
    }

    #[test]
    fn insert_now_relative_bounds_fold_like_as_of() {
        // The bounds reuse the `AS OF` resolver: `now()` and `now() ± interval`
        // fold against the bind snapshot, exactly as a `FOR VALID_TIME AS OF` does.
        let catalog = valid_time_catalog();
        let now = NOW.0;
        let bound = bind(
            "INSERT INTO vt VALUES (1, 100, now(), now() + interval '1 day')",
            &catalog,
        )
        .expect("bind");
        let day = 86_400_000_000i64;
        assert_eq!(
            bound,
            BoundDml::Insert {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(100)),
                    Some(ScalarValue::Timestamp(now)),
                    Some(ScalarValue::Timestamp(now + day)),
                ],
                valid: Some(Interval::new(now, now + day).expect("interval")),
            }
        );
    }

    #[test]
    fn insert_missing_the_start_bound_is_rejected() {
        let catalog = valid_time_catalog();
        // Column list omits `vf`.
        assert_eq!(
            bind(
                "INSERT INTO vt (id, balance, vt) VALUES (1, 100, 20)",
                &catalog
            ),
            Err(DmlError::ValidTimeStartRequired {
                table: "vt".to_owned(),
                column: "vf".to_owned(),
            })
        );
        // Positional INSERT supplies NULL for `vf`.
        assert_eq!(
            bind("INSERT INTO vt VALUES (1, 100, NULL, 20)", &catalog),
            Err(DmlError::ValidTimeStartRequired {
                table: "vt".to_owned(),
                column: "vf".to_owned(),
            })
        );
    }

    #[test]
    fn insert_empty_or_reversed_interval_is_rejected() {
        let catalog = valid_time_catalog();
        assert_eq!(
            bind("INSERT INTO vt VALUES (1, 100, 20, 10)", &catalog),
            Err(DmlError::EmptyValidInterval {
                table: "vt".to_owned(),
                from: 20,
                to: 10,
            })
        );
        assert_eq!(
            bind("INSERT INTO vt VALUES (1, 100, 15, 15)", &catalog),
            Err(DmlError::EmptyValidInterval {
                table: "vt".to_owned(),
                from: 15,
                to: 15,
            })
        );
    }

    #[test]
    fn insert_non_instant_bound_is_a_bad_bound() {
        // A period bound is not a civil-time TIMESTAMP literal — a string surfaces
        // as a BadValidTimeBound, never a silently wrong instant.
        let catalog = valid_time_catalog();
        assert!(matches!(
            bind("INSERT INTO vt VALUES (1, 100, 'noon', 20)", &catalog),
            Err(DmlError::BadValidTimeBound {
                column,
                ..
            }) if column == "vf"
        ));
    }

    #[test]
    fn update_sets_a_new_period() {
        // An UPDATE opens a new version with a new interval; the period columns are
        // assigned alongside any value column and lifted into the interval.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind(
                "UPDATE vt SET balance = 250, vf = 20, vt = 30 WHERE id = 1",
                &catalog
            ),
            Ok(BoundDml::Update {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![
                    (0, Some(ScalarValue::Int4(250))),
                    (1, Some(ScalarValue::Timestamp(20))),
                    (2, Some(ScalarValue::Timestamp(30))),
                ],
                valid: Some(Interval::new(20, 30).expect("interval")),
            })
        );
    }

    #[test]
    fn update_omitting_the_end_bound_opens_the_period() {
        // Setting only the start bound opens the new period; the end-bound cell is
        // synthesized to VALID_TIME_OPEN so the payload and the interval agree.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind("UPDATE vt SET vf = 40 WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![
                    (1, Some(ScalarValue::Timestamp(40))),
                    (2, Some(ScalarValue::Timestamp(VALID_TIME_OPEN.0))),
                ],
                valid: Some(Interval::new(40, VALID_TIME_OPEN.0).expect("open interval")),
            })
        );
    }

    #[test]
    fn update_without_the_start_bound_is_rejected() {
        // A valid-time UPDATE must say when the new fact starts being true.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind("UPDATE vt SET balance = 250 WHERE id = 1", &catalog),
            Err(DmlError::ValidTimeStartRequired {
                table: "vt".to_owned(),
                column: "vf".to_owned(),
            })
        );
    }

    #[test]
    fn delete_on_a_valid_time_table_carries_no_interval() {
        // A DELETE closes the system period; it records no valid-time interval (a
        // delete is a system-time fact), so `BoundDml::Delete` is unchanged.
        let catalog = valid_time_catalog();
        assert_eq!(
            bind("DELETE FROM vt WHERE id = 1", &catalog),
            Ok(BoundDml::Delete {
                table: "vt".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
            })
        );
    }
}
