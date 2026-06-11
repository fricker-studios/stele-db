//! Binding a `SELECT … [FOR SYSTEM_TIME AS OF <expr>]` into a snapshot-scan plan.
//!
//! This is the query that *is* the v0.1 identity ([README](../../../README.md)):
//! time-travel along the system axis. [`parse`](crate::parse) lifts the
//! `FOR SYSTEM_TIME AS OF` qualifier into [`Temporal::as_of`](crate::Temporal);
//! [`bind_select`] turns the whole `SELECT` into a [`BoundSelect`] — a table, a
//! resolved system-time [`snapshot`](BoundSelect::snapshot), and a projection —
//! that the executor lowers to a `SnapshotScan` ([STL-100]).
//!
//! ## What binding does ([STL-101])
//!
//! 1. **Resolve the snapshot.** The `AS OF <expr>` is folded to a concrete
//!    system-time microsecond value ([`resolve_as_of`]). `now()` folds to the
//!    transaction snapshot; `now() ± interval '…'` shifts it; a bare integer is
//!    an explicit instant. **With no `AS OF`, the snapshot is the transaction
//!    snapshot** — a plain `SELECT` reads the present.
//! 2. **Resolve the table at that snapshot.** Against the versioned catalog
//!    ([`Catalog::resolve`]), so a past `AS OF` binds under the schema that was
//!    live *then* ([architecture §5](../../../docs/02-architecture.md#5-catalog--metadata)).
//!    A snapshot *before the table's first commit* is the documented
//!    [`SelectError::BeforeHistory`] — never a silent empty read.
//! 3. **Push the snapshot down.** The resolved `s` is the `sys_from ≤ s` bound
//!    the executor pushes into segment-level zone-map pruning (system-time only;
//!    the close bound comes from the validity index — [ADR-0023], STL-133). The
//!    binder does not re-implement that prune; carrying the snapshot on
//!    [`BoundSelect`] *is* the rewrite — the executor's `SnapshotScan` already
//!    prunes by it ([architecture §3.5](../../../docs/02-architecture.md#35-read-path--as-of-flow)).
//!
//! ## The valid axis ([STL-162])
//!
//! A `FOR VALID_TIME AS OF <expr>` qualifier resolves the same way (same fold,
//! same `now()`) and is carried as [`BoundSelect::valid_snapshot`] alongside the
//! system-time snapshot — a query may give one qualifier per axis, in either
//! order, so the executor can resolve a version at a joint `(sys, valid)` point
//! ([STL-163]). Valid-time `AS OF` only means something on a table that opts into
//! a valid-time period: against a system-only table it is the documented
//! [`SelectError::ValidTimeUnsupported`]. With no valid qualifier,
//! `valid_snapshot` is `None`: the executor reads the valid-time table
//! *unfiltered* — every system-live version, its period columns readable as
//! ordinary cells — and the caller filters the valid axis explicitly (a
//! `FOR VALID_TIME AS OF` or a period predicate). Valid-time is not auto-filtered
//! to "now" the way the system axis is ([STL-218]).
//!
//! ## Scope
//!
//! A single, unqualified table; projection of `*` or bare column names; the
//! `WHERE` clause is left on the AST for the executor-glue layer to lower
//! (pgwire, [STL-104]). Absolute `TIMESTAMP '…'` / `DATE '…'` literals in an
//! `AS OF` are not folded yet (no civil-time codec); they surface
//! [`AsOfError::Unsupported`] rather than a wrong instant.

use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, TableWithJoins, Value,
};
use stele_catalog::{Catalog, SchemaId, TableSchema};
use stele_common::period::{Interval, IntervalError, PeriodPredicate};
use stele_common::time::SystemTimeMicros;
use stele_common::types::{LogicalType, ScalarValue};

use crate::ast::{PeriodExpr, PeriodPredicateClause, Statement, TimeDimension};
use crate::fold::{self, FoldError};

/// The context a [`bind_select`] needs: the transaction snapshot and the
/// catalog to resolve names against.
#[derive(Debug, Clone, Copy)]
pub struct BindContext<'a> {
    /// The transaction's MVCC snapshot. Two roles: the **default** system-time
    /// when the query carries no `AS OF`, and the value `now()` folds to inside
    /// an `AS OF` expression.
    pub snapshot: SystemTimeMicros,
    /// The versioned catalog, for resolving the table at the bound snapshot.
    pub catalog: &'a Catalog,
}

/// What columns a [`BoundSelect`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// `SELECT *` — every column of the resolved schema, in declaration order.
    All,
    /// `SELECT a, b, …` — the named columns, in projection order.
    Columns(Vec<String>),
}

/// A bound `WHERE <column> = <literal>` predicate ([STL-151]).
///
/// The one filter shape v0.2 lowers: a single column compared for equality
/// against a folded literal. The executor applies it after resolving the row's
/// cells, and pushes it down to segment zone-map pruning when the column is the
/// business key (the only column a zone map can currently reason about). Richer
/// comparisons (`<`, `>`, ranges, conjunctions) are a deferred follow-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundPredicate {
    /// The column the predicate compares (its name, for diagnostics).
    pub column: String,
    /// The column's position in the resolved schema — `0` is the business key,
    /// the rest are value columns. The executor projects this column out of the
    /// resolved row to test it.
    pub column_index: usize,
    /// The literal the column must equal, folded to the column's type.
    pub value: ScalarValue,
}

/// A bound `SELECT … [FOR SYSTEM_TIME AS OF …]`, ready to lower to a
/// `SnapshotScan`.
///
/// Carries the resolved system-time [`snapshot`](Self::snapshot) — the
/// `sys_from ≤ s` bound the executor pushes into zone-map pruning — together
/// with the table, the schema that was live at that snapshot, the projection, and
/// the lowered `WHERE` [`filter`](Self::filter) ([STL-151]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSelect {
    /// The (single, unqualified) table the query reads.
    pub table: String,
    /// The schema id live at [`snapshot`](Self::snapshot) — the version a past
    /// `AS OF` resolves columns under.
    pub schema_id: SchemaId,
    /// The resolved system-time snapshot the scan reads at.
    pub snapshot: SystemTimeMicros,
    /// The resolved valid-time instant from a `FOR VALID_TIME AS OF <expr>`
    /// qualifier, or `None` when the query gave none. `Some(v)` only when the
    /// table opts into a valid-time period (else [`SelectError::ValidTimeUnsupported`]);
    /// the executor resolves the version live at the joint `(snapshot, v)` point
    /// ([STL-163]). `None` reads the valid-time table unfiltered on the valid
    /// axis — every system-live version, period columns readable ([STL-218]).
    pub valid_snapshot: Option<SystemTimeMicros>,
    /// The columns the query projects.
    pub projection: Projection,
    /// The lowered `WHERE` predicate, or `None` for an unfiltered read. v0.2
    /// lowers `<column> = <literal>` only ([STL-151]).
    pub filter: Option<BoundPredicate>,
    /// A bound `WHERE PERIOD(a, b) <pred> PERIOD(c, d)` period predicate, or
    /// `None` when the `WHERE` is not one ([STL-165], [STL-193]). When every
    /// endpoint is a constant the predicate is a constant truth value the
    /// executor applies once (a `false` predicate excludes every row); when an
    /// endpoint references a value column the executor builds each row's interval
    /// from its cells and evaluates per row. Mutually exclusive with
    /// [`filter`](Self::filter) — a `WHERE` is one shape or the other.
    pub period_filter: Option<BoundPeriodPredicate>,
    /// A bound `GROUP BY` + aggregate plan, or `None` for a plain row-returning
    /// query ([STL-171]). When `Some`, the executor folds the scanned rows into
    /// grouped aggregate output and the query's result columns are the
    /// [aggregate's](BoundAggregate::columns), not [`projection`](Self::projection)
    /// (which is left a placeholder). [`filter`](Self::filter) still applies — a
    /// `WHERE` filters rows *before* grouping.
    ///
    /// [STL-171]: https://allegromusic.atlassian.net/browse/STL-171
    pub aggregate: Option<BoundAggregate>,
    /// A bound two-table `JOIN` plan, or `None` for a single-table read
    /// ([STL-172]). When `Some`, the query reads the join's two sides and the
    /// result columns are the [join's](BoundJoin::columns); the single-table
    /// fields above ([`table`](Self::table), [`schema_id`](Self::schema_id),
    /// [`filter`](Self::filter), [`aggregate`](Self::aggregate), …) are left at
    /// their defaults — the executor routes to the join path instead. A `WHERE` /
    /// aggregate / `AS OF` *over* a join is rejected at bind time (each a tracked
    /// follow-up), so they cannot co-occur.
    ///
    /// [STL-172]: https://allegromusic.atlassian.net/browse/STL-172
    pub join: Option<BoundJoin>,
}

/// A bound `GROUP BY` + aggregate query: the grouping columns, the aggregates to
/// compute, and the output columns in SELECT-list order ([STL-171]).
///
/// The executor evaluates each grouping key and aggregate argument over the
/// scanned rows (via the vectorized evaluator), folds them per group, and emits
/// one row per group. Columns are referenced by **schema index** (0 = business
/// key, the rest value columns) — the same positional convention
/// [`BoundPredicate`] uses — so the executor reads them straight out of the
/// reconstructed row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAggregate {
    /// The grouping columns, as schema indices. Empty for an ungrouped aggregate
    /// (`SELECT COUNT(*) FROM t`), which is one whole-table group.
    pub group_by: Vec<usize>,
    /// The aggregates to compute, in first-appearance order. An
    /// [`OutputItem::Aggregate`] indexes into this list.
    pub aggregates: Vec<AggregateCall>,
    /// The output columns, in SELECT-list order — each either a passed-through
    /// grouping column or an aggregate.
    pub items: Vec<OutputItem>,
    /// The result columns `(name, type)`, aligned to [`items`](Self::items): a
    /// `RowDescription` header for the grouped result.
    pub columns: Vec<(String, LogicalType)>,
}

/// One output column of an aggregate query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputItem {
    /// A passed-through grouping column — the i-th entry of
    /// [`BoundAggregate::group_by`].
    Group(usize),
    /// An aggregate — the i-th entry of [`BoundAggregate::aggregates`].
    Aggregate(usize),
}

/// A bound aggregate function call: a function over an optional value column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateCall {
    /// Which aggregate to compute.
    pub func: AggregateFunc,
    /// The value column the aggregate reads, as a schema index, or `None` for
    /// `COUNT(*)` (which counts rows, not a column's values).
    pub arg: Option<usize>,
}

/// The aggregate functions the binder lowers ([STL-171]).
///
/// Mirrors the executor's `stele_exec::AggregateFunc`; the engine maps between
/// them when it lowers the bound plan (the two crates do not depend on each
/// other).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    /// `COUNT` — row / non-NULL-value count. Result `INT8`.
    Count,
    /// `SUM` — total of non-NULL integer values. Result `INT8`.
    Sum,
    /// `MIN` — least non-NULL value. Result the argument's type.
    Min,
    /// `MAX` — greatest non-NULL value. Result the argument's type.
    Max,
    /// `AVG` — exact fractional mean of non-NULL integer values. Result `FLOAT8`
    /// ([STL-209]).
    Avg,
}

impl AggregateFunc {
    /// The aggregate a function name denotes (case-insensitive), or `None` if the
    /// name is not one of the core aggregates.
    fn from_name(name: &str) -> Option<Self> {
        Some(match () {
            () if name.eq_ignore_ascii_case("count") => Self::Count,
            () if name.eq_ignore_ascii_case("sum") => Self::Sum,
            () if name.eq_ignore_ascii_case("min") => Self::Min,
            () if name.eq_ignore_ascii_case("max") => Self::Max,
            () if name.eq_ignore_ascii_case("avg") => Self::Avg,
            () => return None,
        })
    }

    /// The default output column name (when no `AS` alias is given) — the
    /// lowercase function name, as Postgres labels an unaliased aggregate.
    const fn default_name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
        }
    }

    /// The result type for this aggregate over an argument of type `arg` (`None`
    /// for `COUNT(*)`). `COUNT` / `SUM` produce `INT8`; `AVG` produces the
    /// fractional `FLOAT8` ([STL-209]); `MIN` / `MAX` produce the argument's own
    /// type.
    const fn result_type(self, arg: Option<LogicalType>) -> LogicalType {
        match self {
            Self::Count | Self::Sum => LogicalType::Int8,
            Self::Avg => LogicalType::Float8,
            Self::Min | Self::Max => arg.expect("MIN/MAX carries an argument"),
        }
    }
}

/// A bound `PERIOD(a, b) <predicate> PERIOD(c, d)` clause: two half-open period
/// operands and the predicate relating them ([STL-165], [STL-193]).
///
/// Each operand's endpoints may be constant instants (the STL-165 form — the
/// whole predicate is then a constant truth value the executor applies once) or
/// references to a row's value columns (the STL-193 per-row form — the executor
/// builds each row's interval from its cells and calls `stele_exec::evaluate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundPeriodPredicate {
    /// The left period operand.
    pub left: BoundPeriod,
    /// The predicate relating the two operands.
    pub predicate: PeriodPredicate,
    /// The right period operand.
    pub right: BoundPeriod,
}

/// A bound `PERIOD(from, to)` operand — a half-open `[from, to)` whose endpoints
/// are each a constant instant or a row's value column ([STL-193]).
///
/// A fully-constant operand is checked for `from < to` at bind time (the STL-165
/// rule); an operand with a column endpoint can only be checked per row, so that
/// well-formedness is enforced at evaluation (a NULL or reversed cell excludes
/// the row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundPeriod {
    /// The inclusive start endpoint.
    pub from: PeriodEndpoint,
    /// The exclusive end endpoint.
    pub to: PeriodEndpoint,
}

/// One endpoint of a `PERIOD(from, to)` operand: a constant instant or a row's
/// value column ([STL-193]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodEndpoint {
    /// A constant-folded instant in microseconds — `now()`, `now() ± interval`,
    /// or a bare integer literal, folded the same way an `AS OF` operand is
    /// ([`resolve_as_of`], [STL-165]).
    Const(i64),
    /// The value column at this index in the resolved schema; each row's cell
    /// supplies the endpoint's instant ([STL-193]). The column's type is one of
    /// the microsecond-instant types (`BIGINT` / `TIMESTAMP` / `TIMESTAMPTZ`).
    Column(usize),
}

/// The join algorithms the binder lowers ([STL-172]).
///
/// Mirrors the executor's `stele_exec::JoinType`; the engine maps between them
/// when it lowers the bound plan (the two crates do not depend on each other, the
/// same split [`AggregateFunc`] draws).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// `INNER JOIN` / `JOIN` — matching rows from both sides.
    Inner,
    /// `LEFT [OUTER] JOIN` — every left row, `NULL`-extended on the right when
    /// unmatched.
    Left,
    /// `LEFT SEMI JOIN` / `SEMI JOIN` — left rows that have a right match, once.
    Semi,
    /// `LEFT ANTI JOIN` / `ANTI JOIN` — left rows that have no right match.
    Anti,
}

impl JoinType {
    /// Whether the output carries the right side's columns. `INNER` / `LEFT`
    /// combine both sides; `SEMI` / `ANTI` filter the left and emit only it — so a
    /// projected right column is meaningless for them.
    const fn keeps_right(self) -> bool {
        matches!(self, Self::Inner | Self::Left)
    }
}

/// One side of a bound [`BoundJoin`]: the table and the schema-ordered columns it
/// contributes ([STL-172]).
///
/// The column list is the side's schema (`(name, type)`, position `0` the business
/// key) — it gives the executor the side's value-column count (for the row codec)
/// and the join key's type (for decoding), and a [`JoinColumnRef`] indexes into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundJoinSide {
    /// The table this side reads.
    pub table: String,
    /// The schema id live at the read snapshot.
    pub schema_id: SchemaId,
    /// The side's columns in schema order — `(name, type)`, position `0` the
    /// business key.
    pub columns: Vec<(String, LogicalType)>,
}

/// Which side of a join an output column is drawn from, and its position in that
/// side's schema ([STL-172]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinColumnRef {
    /// The left side's column at this schema index.
    Left(usize),
    /// The right side's column at this schema index. Never produced for a
    /// [`Semi`](JoinType::Semi) / [`Anti`](JoinType::Anti) join (it has no right
    /// columns).
    Right(usize),
}

/// A bound two-table equi-join ([STL-172]).
///
/// The executor scans each side at the read snapshot, joins their rows on
/// `left[left_key] = right[right_key]`, and assembles the [`output`](Self::output)
/// columns. v0.2 binds a single equality condition over one column per side; a
/// `WHERE` / aggregate / `AS OF` over the join, multi-condition / non-equi joins,
/// `RIGHT` / `FULL` joins, and N-way joins are tracked follow-ups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundJoin {
    /// Which join to compute.
    pub join_type: JoinType,
    /// The left (probe) side.
    pub left: BoundJoinSide,
    /// The right (build) side.
    pub right: BoundJoinSide,
    /// The equi-join key's column index in the left side's schema.
    pub left_key: usize,
    /// The equi-join key's column index in the right side's schema. The two key
    /// columns share a [`LogicalType`] (the binder enforces it).
    pub right_key: usize,
    /// The output columns, in SELECT-list order — each drawn from one side. For a
    /// `SEMI` / `ANTI` join every entry is [`JoinColumnRef::Left`].
    pub output: Vec<JoinColumnRef>,
    /// The result columns `(name, type)`, aligned to [`output`](Self::output) — a
    /// `RowDescription` header for the joined result.
    pub columns: Vec<(String, LogicalType)>,
}

/// Why binding a `SELECT` failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SelectError {
    /// The statement is not a `SELECT` query (e.g. an `INSERT`, or DDL). The
    /// caller routes those elsewhere ([`bind_ddl`](crate::bind_ddl) / DML).
    #[error("not a SELECT query")]
    NotSelect,

    /// The query reads something other than exactly one plain table — a join,
    /// a subquery, a set operation, or a table-valued function. v0.1 binds a
    /// single-table scan only.
    #[error("v0.1 binds a single-table SELECT only ({0})")]
    UnsupportedFrom(&'static str),

    /// A projected item is neither `*` nor a bare column name (e.g. an
    /// expression, an aggregate, or a qualified wildcard). v0.1 projects plain
    /// columns only.
    #[error("v0.1 projects `*` or bare column names only ({0})")]
    UnsupportedProjection(String),

    /// The query carried a clause outside the v0.1 single-table snapshot-scan
    /// surface (`WITH`, `ORDER BY`, `LIMIT`/`OFFSET`/`FETCH`, `DISTINCT`,
    /// `GROUP BY`, `HAVING`, locking, …). [`BoundSelect`] does not carry these,
    /// so accepting them would silently drop user intent — they are rejected.
    #[error("v0.1 does not support `{0}` in a SELECT")]
    UnsupportedClause(&'static str),

    /// A projected column is not present in the table's schema **at the resolved
    /// snapshot** — it does not exist, or was only added after the `AS OF`
    /// instant. Caught at bind time rather than deferred to a confusing executor
    /// error.
    #[error("column {column:?} does not exist in table {table:?} at the AS OF snapshot")]
    UnknownColumn {
        /// The table read.
        table: String,
        /// The projected column that the resolved schema does not contain.
        column: String,
    },

    /// The `WHERE` clause is not the one shape v0.2 lowers — `<column> =
    /// <literal>` ([STL-151]). A join predicate, a non-equality comparison, a
    /// column-to-column compare, an `AND`/`OR` chain, or a literal that cannot
    /// fold to the column's type all surface here rather than being silently
    /// dropped (which would return unfiltered rows — a wrong answer).
    #[error("v0.2 supports only a `<column> = <literal>` WHERE ({0})")]
    UnsupportedPredicate(String),

    /// A `SELECT` item in an aggregate query is a bare column that is **not** in
    /// the `GROUP BY` ([STL-171]). SQL requires every non-aggregated output column
    /// to be a grouping column; one that is not has no single value per group, so
    /// it is rejected rather than returning an arbitrary row's value.
    #[error(
        "column {column:?} of table {table:?} must appear in GROUP BY or be used in an aggregate"
    )]
    UngroupedColumn {
        /// The table read.
        table: String,
        /// The ungrouped, non-aggregated column.
        column: String,
    },

    /// An aggregate query's `GROUP BY`, a `SELECT` item, or an aggregate call is
    /// not a shape v0.2 supports ([STL-171]) — a non-column grouping key, an
    /// unknown function, `DISTINCT` / `FILTER` / `OVER` on an aggregate, a wrong
    /// argument arity (`SUM(*)`), or an aggregate over a type the evaluator does
    /// not read. Rejected with the reason rather than computed wrongly.
    #[error("unsupported aggregate query: {0}")]
    UnsupportedAggregate(String),

    /// `FOR VALID_TIME AS OF` was given for a table that does not opt into a
    /// valid-time period — there is no valid axis to travel along. The catalog's
    /// system-only tables (every table without `VALID TIME (…)`) reject it here.
    #[error("table {table:?} has no valid-time period — FOR VALID_TIME AS OF does not apply")]
    ValidTimeUnsupported {
        /// The table read.
        table: String,
    },

    /// More than one `AS OF` qualifier appeared on the same axis. A table may
    /// carry at most one `FOR SYSTEM_TIME AS OF` and one `FOR VALID_TIME AS OF`;
    /// a repeated axis would name two snapshots for one dimension.
    #[error("multiple FOR {0:?} AS OF qualifiers — at most one per axis")]
    MultipleAsOf(TimeDimension),

    /// The `AS OF` expression could not be folded to a concrete instant.
    #[error("AS OF: {0}")]
    AsOf(#[from] AsOfError),

    /// A `PERIOD(from, to)` operand of a period predicate could not be folded to
    /// a concrete instant ([STL-165]). The endpoints fold the same way as `AS OF`
    /// expressions (`now()`, `now() ± interval`, integer microseconds).
    #[error("period predicate operand: {0}")]
    PeriodOperand(AsOfError),

    /// A `PERIOD(from, to)` operand folded to an empty or reversed interval
    /// (`from >= to`) ([STL-165]). Half-open `[from, to)` requires `from < to`.
    /// Only a fully-constant operand is checked here; a per-row operand's
    /// well-formedness is enforced at evaluation.
    #[error("period predicate: {0}")]
    PeriodInterval(IntervalError),

    /// A `PERIOD(from, to)` endpoint named a value column whose type is not a
    /// microsecond instant the period codec can read ([STL-193]). Only `BIGINT`,
    /// `TIMESTAMP`, and `TIMESTAMPTZ` columns form a period endpoint — never a
    /// silently mis-scaled `INT` or `DATE`.
    #[error(
        "period predicate: column {column:?} of table {table:?} has type {ty:?}, not a microsecond instant (BIGINT/TIMESTAMP/TIMESTAMPTZ)"
    )]
    PeriodColumnType {
        /// The table the endpoint column belongs to.
        table: String,
        /// The offending column's name.
        column: String,
        /// The column's logical type.
        ty: LogicalType,
    },

    /// The catalog has never registered this table name.
    #[error("unknown table {0:?}")]
    UnknownTable(String),

    /// The snapshot precedes the table's first commit — a read *before the
    /// table existed*. The documented "before history" error: an `AS OF` older
    /// than the table returns this, not an empty result that masquerades as
    /// "no rows".
    #[error(
        "AS OF {snapshot} is before table {table:?}'s history begins (first commit at {first_commit})"
    )]
    BeforeHistory {
        /// The table read.
        table: String,
        /// The resolved snapshot, in system-time microseconds.
        snapshot: i64,
        /// The table's first-commit system time; `snapshot` precedes it.
        first_commit: i64,
    },

    /// The table existed but was not live at the snapshot — dropped by then, or
    /// in the gap between a dropped era and a re-creation. Distinct from
    /// [`BeforeHistory`](Self::BeforeHistory): the snapshot is *within* the
    /// table's recorded timeline, just not in a live era.
    #[error("table {table:?} is not live at AS OF {snapshot} (dropped, or in a re-creation gap)")]
    TableNotLive {
        /// The table read.
        table: String,
        /// The resolved snapshot, in system-time microseconds.
        snapshot: i64,
    },

    /// A `JOIN` shape v0.2 does not bind — a `RIGHT` / `FULL` / `CROSS` / `ASOF`
    /// join, an N-way join, or a clause v0.2 does not support *over* a join
    /// (`WHERE`, an aggregate, `FOR … AS OF`) ([STL-172]). Each is a tracked
    /// follow-up; rejected rather than silently mis-bound.
    #[error("v0.2 does not support this JOIN ({0})")]
    UnsupportedJoin(String),

    /// A `JOIN`'s condition is not the one shape v0.2 lowers — a single
    /// `left.col = right.col` equality relating the two sides ([STL-172]). A
    /// `USING` / `NATURAL` / missing `ON`, a non-equality, a non-column operand, or
    /// an equality that does not span both tables surfaces here.
    #[error("v0.2 supports only a `left.col = right.col` JOIN condition ({0})")]
    JoinCondition(String),

    /// A `JOIN`'s two equi-join key columns have different types, so their values
    /// can never compare equal ([STL-172]).
    #[error(
        "JOIN key types differ: {left_column:?} is {left_type} but {right_column:?} is {right_type}"
    )]
    JoinColumnTypeMismatch {
        /// The left key column.
        left_column: String,
        /// The right key column.
        right_column: String,
        /// The left key column's type.
        left_type: LogicalType,
        /// The right key column's type.
        right_type: LogicalType,
    },

    /// A bare (unqualified) column in a `JOIN`'s condition or projection exists in
    /// *both* tables, so it is ambiguous — qualify it with a table name or alias
    /// ([STL-172]).
    #[error("column {column:?} is ambiguous in the JOIN — qualify it with a table name")]
    AmbiguousColumn {
        /// The ambiguous column name.
        column: String,
    },

    /// A column named in a `JOIN`'s condition or projection is in neither side's
    /// schema, or its qualifier names neither table ([STL-172]).
    #[error("column {column:?} is not a column of either JOIN table")]
    UnknownJoinColumn {
        /// The unresolved (possibly qualified) column reference.
        column: String,
    },

    /// A projected item in a `JOIN` is not one v0.2 lowers — `*` mixed with named
    /// columns, a non-column expression, or (for a `SEMI` / `ANTI` join) a
    /// right-table column the join does not output ([STL-172]). Distinct from the
    /// single-table [`UnsupportedProjection`](Self::UnsupportedProjection) so the
    /// rejection does not borrow that variant's v0.1 single-table wording.
    #[error("v0.2 cannot project this from the JOIN ({0})")]
    UnsupportedJoinProjection(String),
}

/// Why folding an `AS OF <expr>` to an instant failed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AsOfError {
    /// The expression is not a form v0.1 folds: it must be `now()`, `now() ±
    /// interval '…'`, or an integer microsecond instant.
    #[error("unsupported AS OF expression ({0})")]
    Unsupported(String),

    /// An `interval '…'` literal could not be parsed into a fixed duration.
    #[error("malformed interval ({0})")]
    BadInterval(String),

    /// The interval used a **calendar** unit (month/year). These are not a fixed
    /// number of microseconds, so they cannot fold to a system-time offset; use
    /// days/weeks or smaller.
    #[error(
        "calendar interval unit {0:?} has no fixed microsecond length (use day/week or smaller)"
    )]
    CalendarInterval(String),

    /// The arithmetic overflowed `i64` microseconds.
    #[error("AS OF arithmetic overflowed the system-time range")]
    Overflow,
}

/// Bind a parsed [`Statement`] into a [`BoundSelect`].
///
/// Resolves the `AS OF` snapshot (or defaults to the transaction snapshot),
/// resolves the table against the catalog at that snapshot, and captures the
/// projection. See the [module docs](self) for the full shape.
///
/// # Errors
///
/// A [`SelectError`] variant: the statement is not a single-table `SELECT`, an
/// `AS OF` expression cannot be folded, a valid-time `AS OF` names a table with
/// no valid axis, or the table is unknown / not live (including the
/// [before-history](SelectError::BeforeHistory) case) at the resolved snapshot.
pub fn bind_select(stmt: &Statement, ctx: &BindContext) -> Result<BoundSelect, SelectError> {
    let select = single_select(&stmt.body)?;
    // A two-table `JOIN` binds to a wholly different shape (two sides, a join
    // condition, a combined header), so it is routed before the single-table path.
    if let Some(join) = detect_join(select)? {
        return bind_join(stmt, ctx, select, join);
    }
    let table = single_table(select)?;
    // An aggregate query (a `GROUP BY`, or an aggregate in the SELECT list) takes
    // a different shape: its output columns come from the aggregate plan, so the
    // plain projection is bound only for a non-aggregate read. Detection is purely
    // syntactic, so it runs before name resolution.
    let aggregate_query = is_aggregate_query(select);
    let projection = if aggregate_query {
        // The output columns are the aggregate's; the projection is an unused
        // placeholder the executor never consults on the aggregate path.
        Projection::All
    } else {
        bind_projection(select)?
    };
    let (snapshot, valid_snapshot) = resolve_snapshots(stmt, ctx.snapshot)?;

    let schema = match resolve_table_at(ctx.catalog, table, snapshot) {
        TableResolution::Found(schema) => schema,
        TableResolution::Unknown => return Err(SelectError::UnknownTable(table.to_owned())),
        TableResolution::BeforeHistory { first_commit } => {
            return Err(SelectError::BeforeHistory {
                table: table.to_owned(),
                snapshot: snapshot.0,
                first_commit: first_commit.0,
            });
        }
        TableResolution::NotLive => {
            return Err(SelectError::TableNotLive {
                table: table.to_owned(),
                snapshot: snapshot.0,
            });
        }
    };

    // A valid-time `AS OF` only means something on a table with a valid-time
    // period; against a system-only table there is no valid axis to travel.
    if valid_snapshot.is_some() && !schema.temporal().valid_time_enabled() {
        return Err(SelectError::ValidTimeUnsupported {
            table: table.to_owned(),
        });
    }

    // Every named projected column must exist in the schema live *at the
    // snapshot* — a column added after the `AS OF` instant is not yet present
    // and is rejected here rather than deferred to the executor.
    if let Projection::Columns(columns) = &projection {
        for column in columns {
            if schema.column(column).is_none() {
                return Err(SelectError::UnknownColumn {
                    table: table.to_owned(),
                    column: column.clone(),
                });
            }
        }
    }

    // Bind the `GROUP BY` + aggregate plan against the resolved schema, which
    // gives the grouping/argument columns their indices and types.
    let aggregate = if aggregate_query {
        Some(bind_aggregate(select, schema, table)?)
    } else {
        None
    };

    let filter = bind_filter(select, schema, table)?;

    // A period predicate is lifted off the token stream (the executor-glue
    // `WHERE` is gone by the time `bind_filter` runs), so the two filter shapes
    // are naturally mutually exclusive. Its `PERIOD(...)` endpoints fold against
    // the transaction `now` (`ctx.snapshot`), like `AS OF` operands.
    let period_filter = stmt
        .temporal
        .period_predicate
        .as_ref()
        .map(|clause| bind_period_predicate(clause, ctx.snapshot, schema, table))
        .transpose()?;

    Ok(BoundSelect {
        table: table.to_owned(),
        schema_id: schema.schema_id(),
        snapshot,
        valid_snapshot,
        projection,
        filter,
        period_filter,
        aggregate,
        join: None,
    })
}

/// Whether a `SELECT` is an aggregate query — it carries a non-empty `GROUP BY`,
/// or any projected item is an aggregate function call. Purely syntactic (no
/// catalog), so it gates binding before name resolution.
fn is_aggregate_query(select: &Select) -> bool {
    let grouped = matches!(
        &select.group_by,
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty()
    );
    grouped || select.projection.iter().any(projection_item_is_aggregate)
}

/// Whether a projected item is an aggregate function call (`COUNT(*)`,
/// `SUM(x)`, …) — used to detect an aggregate query.
fn projection_item_is_aggregate(item: &SelectItem) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::ExprWithAliases { .. }
        | SelectItem::Wildcard(_)
        | SelectItem::QualifiedWildcard(..) => return false,
    };
    matches!(expr, Expr::Function(func) if function_name(func).is_some_and(|n| AggregateFunc::from_name(n).is_some()))
}

/// The single, unqualified name of a function call, or `None` for a qualified or
/// empty name (which is never one of the core aggregates).
fn function_name(func: &sqlparser::ast::Function) -> Option<&str> {
    match func.name.0.as_slice() {
        [part] => part.as_ident().map(|id| id.value.as_str()),
        _ => None,
    }
}

/// Bind a `GROUP BY` + aggregate `SELECT` into a [`BoundAggregate`] against the
/// resolved `schema` ([STL-171]).
///
/// Resolves the grouping columns to schema indices, then walks the SELECT list:
/// each item is either an aggregate function call or a passed-through grouping
/// column (one that is not in `GROUP BY` is [`SelectError::UngroupedColumn`]).
fn bind_aggregate(
    select: &Select,
    schema: &TableSchema,
    table: &str,
) -> Result<BoundAggregate, SelectError> {
    let group_by = bind_group_by(select, schema, table)?;

    let mut aggregates: Vec<AggregateCall> = Vec::new();
    let mut items: Vec<OutputItem> = Vec::new();
    let mut columns: Vec<(String, LogicalType)> = Vec::new();

    for item in &select.projection {
        let (expr, alias) = select_item(item)?;
        if let Some(call) = bind_aggregate_call(expr, schema, table)? {
            let arg_ty = call.arg.map(|i| schema.columns()[i].ty());
            let ty = call.func.result_type(arg_ty);
            let name = alias.unwrap_or_else(|| call.func.default_name().to_owned());
            items.push(OutputItem::Aggregate(aggregates.len()));
            aggregates.push(call);
            columns.push((name, ty));
        } else {
            // Not an aggregate ⇒ it must be a grouping column passed through.
            let column = bare_column(expr).ok_or_else(|| {
                SelectError::UnsupportedAggregate(
                    "a SELECT item must be a grouping column or an aggregate".to_owned(),
                )
            })?;
            let idx = column_index(schema, column).ok_or_else(|| SelectError::UnknownColumn {
                table: table.to_owned(),
                column: column.to_owned(),
            })?;
            let group_pos = group_by.iter().position(|&g| g == idx).ok_or_else(|| {
                SelectError::UngroupedColumn {
                    table: table.to_owned(),
                    column: column.to_owned(),
                }
            })?;
            let name = alias.unwrap_or_else(|| column.to_owned());
            items.push(OutputItem::Group(group_pos));
            columns.push((name, schema.columns()[idx].ty()));
        }
    }

    Ok(BoundAggregate {
        group_by,
        aggregates,
        items,
        columns,
    })
}

/// Resolve the `GROUP BY` columns to schema indices, rejecting a non-column
/// grouping key or a grouping column of a type the evaluator cannot read.
fn bind_group_by(
    select: &Select,
    schema: &TableSchema,
    table: &str,
) -> Result<Vec<usize>, SelectError> {
    let exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(SelectError::UnsupportedAggregate(
                    "GROUP BY modifiers (ROLLUP/CUBE/GROUPING SETS) are not supported".to_owned(),
                ));
            }
            exprs
        }
        GroupByExpr::All(_) => {
            return Err(SelectError::UnsupportedAggregate(
                "GROUP BY ALL is not supported".to_owned(),
            ));
        }
    };
    let mut group_by = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let column = bare_column(expr).ok_or_else(|| {
            SelectError::UnsupportedAggregate("GROUP BY supports bare column names only".to_owned())
        })?;
        let idx = column_index(schema, column).ok_or_else(|| SelectError::UnknownColumn {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
        require_evaluable(schema.columns()[idx].ty(), || {
            format!(
                "GROUP BY on a {} column is not supported yet",
                schema.columns()[idx].ty()
            )
        })?;
        group_by.push(idx);
    }
    Ok(group_by)
}

/// Bind one aggregate function call expression, or `Ok(None)` if `expr` is not a
/// function call at all (so the caller treats it as a grouping column). A
/// function call that is *not* a core aggregate, or a supported aggregate in an
/// unsupported form, is an error rather than `None`.
fn bind_aggregate_call(
    expr: &Expr,
    schema: &TableSchema,
    table: &str,
) -> Result<Option<AggregateCall>, SelectError> {
    let Expr::Function(func) = expr else {
        return Ok(None);
    };
    let name = function_name(func).ok_or_else(|| {
        SelectError::UnsupportedAggregate("qualified function names are not supported".to_owned())
    })?;
    let func_kind = AggregateFunc::from_name(name).ok_or_else(|| {
        SelectError::UnsupportedAggregate(format!("function {name}() is not a supported aggregate"))
    })?;

    // Reject everything beyond a plain single-argument aggregate: `DISTINCT`,
    // `FILTER (WHERE …)`, an `OVER` window, `WITHIN GROUP`, and parametric calls.
    // Each changes the meaning, so silently ignoring it would be a wrong answer.
    if func.over.is_some() {
        return Err(SelectError::UnsupportedAggregate(
            "window aggregates (OVER) are not supported".to_owned(),
        ));
    }
    if func.filter.is_some() {
        return Err(SelectError::UnsupportedAggregate(
            "aggregate FILTER (WHERE …) is not supported".to_owned(),
        ));
    }
    if !func.within_group.is_empty() || func.null_treatment.is_some() {
        return Err(SelectError::UnsupportedAggregate(
            "WITHIN GROUP / NULL-treatment aggregates are not supported".to_owned(),
        ));
    }
    if !matches!(func.parameters, FunctionArguments::None) {
        return Err(SelectError::UnsupportedAggregate(
            "parametric aggregates are not supported".to_owned(),
        ));
    }

    let FunctionArguments::List(list) = &func.args else {
        return Err(SelectError::UnsupportedAggregate(format!(
            "{name}() requires a single argument"
        )));
    };
    // `DISTINCT` changes the meaning and is unsupported; `ALL` is the default
    // (`COUNT(ALL col)` == `COUNT(col)`) and is accepted, as is no treatment.
    if matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct)) {
        return Err(SelectError::UnsupportedAggregate(format!(
            "{name}(DISTINCT …) is not supported"
        )));
    }
    if !list.clauses.is_empty() {
        return Err(SelectError::UnsupportedAggregate(format!(
            "{name}() with an argument-list clause (e.g. ORDER BY) is not supported"
        )));
    }
    let [FunctionArg::Unnamed(arg)] = list.args.as_slice() else {
        return Err(SelectError::UnsupportedAggregate(format!(
            "{name}() takes exactly one positional argument"
        )));
    };

    // `COUNT(*)` is the one wildcard-argument aggregate; everything else needs a
    // column. A bare `COUNT(col)` / `SUM(col)` / … resolves the column and checks
    // its type fits the aggregate.
    let arg = match arg {
        FunctionArgExpr::Wildcard => {
            if func_kind != AggregateFunc::Count {
                return Err(SelectError::UnsupportedAggregate(format!(
                    "{name}(*) is not valid; only COUNT(*) takes a wildcard"
                )));
            }
            None
        }
        FunctionArgExpr::Expr(expr) => {
            let column = bare_column(expr).ok_or_else(|| {
                SelectError::UnsupportedAggregate(format!(
                    "{name}() supports a bare column argument only"
                ))
            })?;
            let idx = column_index(schema, column).ok_or_else(|| SelectError::UnknownColumn {
                table: table.to_owned(),
                column: column.to_owned(),
            })?;
            check_aggregate_arg_type(func_kind, schema.columns()[idx].ty())?;
            Some(idx)
        }
        FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::WildcardWithOptions(_) => {
            return Err(SelectError::UnsupportedAggregate(format!(
                "{name}() does not support a qualified or option-bearing wildcard"
            )));
        }
    };

    Ok(Some(AggregateCall {
        func: func_kind,
        arg,
    }))
}

/// Check an aggregate argument's column type: `SUM` / `AVG` need an integer;
/// `MIN` / `MAX` / `COUNT(col)` need any type the evaluator can read.
fn check_aggregate_arg_type(func: AggregateFunc, ty: LogicalType) -> Result<(), SelectError> {
    match func {
        AggregateFunc::Sum | AggregateFunc::Avg => {
            if matches!(ty, LogicalType::Int4 | LogicalType::Int8) {
                Ok(())
            } else {
                Err(SelectError::UnsupportedAggregate(format!(
                    "{} requires an integer argument, got {ty}",
                    func.default_name().to_uppercase()
                )))
            }
        }
        AggregateFunc::Count | AggregateFunc::Min | AggregateFunc::Max => {
            require_evaluable(ty, || {
                format!(
                    "{}({ty}) is not supported yet",
                    func.default_name().to_uppercase()
                )
            })
        }
    }
}

/// Accept a type the vectorized evaluator can decode (`INT4` / `INT8` / `BOOL` /
/// `TEXT`); reject anything else with `reason`. Grouping keys and `MIN` / `MAX` /
/// `COUNT` arguments must be evaluable because the executor decodes them into
/// typed vectors; the temporal / `PERIOD` / `UUID` / `BYTEA` types are the
/// evaluator's tracked follow-up (STL-207).
fn require_evaluable(ty: LogicalType, reason: impl FnOnce() -> String) -> Result<(), SelectError> {
    if matches!(
        ty,
        LogicalType::Int4 | LogicalType::Int8 | LogicalType::Bool | LogicalType::Text
    ) {
        Ok(())
    } else {
        Err(SelectError::UnsupportedAggregate(reason()))
    }
}

/// The `(expression, optional alias)` of a SELECT item, rejecting a wildcard
/// (`*` is meaningless in an aggregate query — every column must be grouped or
/// aggregated).
fn select_item(item: &SelectItem) -> Result<(&Expr, Option<String>), SelectError> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias.value.clone()))),
        SelectItem::ExprWithAliases { .. } => Err(SelectError::UnsupportedAggregate(
            "multi-alias `AS (a, b, …)` SELECT items are not supported".to_owned(),
        )),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
            Err(SelectError::UnsupportedAggregate(
                "`*` is not supported with GROUP BY / aggregates".to_owned(),
            ))
        }
    }
}

/// The bare column name an expression references (peeling parentheses), or `None`
/// for any non-bare-identifier expression.
fn bare_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(id) => Some(id.value.as_str()),
        Expr::Nested(inner) => bare_column(inner),
        _ => None,
    }
}

/// A column's position in the schema (0 = business key), or `None` if absent.
fn column_index(schema: &TableSchema, column: &str) -> Option<usize> {
    schema.columns().iter().position(|c| c.name() == column)
}

/// A single two-table `JOIN` in the `FROM` clause, or `None` for any other shape
/// (a single table, a comma join, an N-way join — each handled or rejected
/// elsewhere).
///
/// Returns the [`TableWithJoins`] (its `relation` the left table, its lone join
/// the right) only for exactly one table with exactly one join. More than one join
/// is the explicit [`SelectError::UnsupportedJoin`] (N-way joins are a follow-up);
/// zero joins or a non-singleton `FROM` returns `None` so the single-table path
/// reports it.
fn detect_join(select: &Select) -> Result<Option<&TableWithJoins>, SelectError> {
    let [from] = select.from.as_slice() else {
        return Ok(None);
    };
    match from.joins.len() {
        0 => Ok(None),
        1 => Ok(Some(from)),
        _ => Err(SelectError::UnsupportedJoin(
            "only a single JOIN is supported (N-way joins are a follow-up)".to_owned(),
        )),
    }
}

/// Bind a two-table `JOIN` `SELECT` into a [`BoundSelect`] carrying a
/// [`BoundJoin`] ([STL-172]).
///
/// Resolves both tables at the transaction snapshot, lowers the join operator to a
/// [`JoinType`], binds the `ON left.col = right.col` equi-condition to a key column
/// per side, and binds the projection to output columns drawn from the two sides.
/// A `WHERE` / aggregate / `FOR … AS OF` *over* the join, and `RIGHT` / `FULL` /
/// `CROSS` joins, are rejected (each a tracked follow-up) rather than mis-bound.
fn bind_join<'a>(
    stmt: &Statement,
    ctx: &BindContext,
    select: &'a Select,
    from: &'a TableWithJoins,
) -> Result<BoundSelect, SelectError> {
    // Clauses v0.2 does not yet support over a join: rejected, never dropped.
    if !stmt.temporal.as_of.is_empty() {
        return Err(SelectError::UnsupportedJoin(
            "FOR … AS OF over a JOIN".to_owned(),
        ));
    }
    if stmt.temporal.period_predicate.is_some() {
        return Err(SelectError::UnsupportedJoin(
            "a period predicate over a JOIN".to_owned(),
        ));
    }
    if select.selection.is_some() {
        return Err(SelectError::UnsupportedJoin(
            "a WHERE over a JOIN".to_owned(),
        ));
    }
    if is_aggregate_query(select) {
        return Err(SelectError::UnsupportedJoin(
            "an aggregate over a JOIN".to_owned(),
        ));
    }

    let snapshot = ctx.snapshot;
    let join_ast: &Join = &from.joins[0];
    let (join_type, constraint) = join_kind_and_constraint(&join_ast.join_operator)?;

    let left_ref = table_ref(&from.relation)?;
    let right_ref = table_ref(&join_ast.relation)?;
    let left = SideSchema {
        table: left_ref.name,
        alias: left_ref.alias,
        schema: resolve_join_table(ctx.catalog, left_ref.name, snapshot)?,
    };
    let right = SideSchema {
        table: right_ref.name,
        alias: right_ref.alias,
        schema: resolve_join_table(ctx.catalog, right_ref.name, snapshot)?,
    };

    let (left_key, right_key) = bind_join_condition(constraint, &left, &right)?;
    let (output, columns) = bind_join_projection(select, join_type, &left, &right)?;

    Ok(BoundSelect {
        // The single-table fields are unused on the join path (see `BoundSelect`):
        // the executor routes to the join plan, never reading these.
        table: String::new(),
        schema_id: left.schema.schema_id(),
        snapshot,
        valid_snapshot: None,
        projection: Projection::All,
        filter: None,
        period_filter: None,
        aggregate: None,
        join: Some(BoundJoin {
            join_type,
            left: bound_side(&left),
            right: bound_side(&right),
            left_key,
            right_key,
            output,
            columns,
        }),
    })
}

/// Which side of a join a resolved column came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

/// A join side during binding: its table name, optional alias, and resolved
/// schema. The alias and table name are both valid qualifiers for the side's
/// columns (`t.c` or `alias.c`).
struct SideSchema<'a> {
    table: &'a str,
    alias: Option<&'a str>,
    schema: &'a TableSchema,
}

impl SideSchema<'_> {
    /// Whether `qualifier` (a `t.c` prefix) names this side — its table name or
    /// its alias. Stele does not case-fold identifiers, so the match is exact.
    fn qualifier_matches(&self, qualifier: &str) -> bool {
        self.table == qualifier || self.alias == Some(qualifier)
    }

    /// The schema index of `column` in this side, or `None` if absent.
    fn column_index(&self, column: &str) -> Option<usize> {
        self.schema
            .columns()
            .iter()
            .position(|c| c.name() == column)
    }
}

/// Snapshot the binding [`SideSchema`] into the owned [`BoundJoinSide`] the plan
/// carries.
fn bound_side(side: &SideSchema) -> BoundJoinSide {
    BoundJoinSide {
        table: side.table.to_owned(),
        schema_id: side.schema.schema_id(),
        columns: side
            .schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect(),
    }
}

/// A table reference in a `JOIN`'s `FROM`: an unqualified table name plus an
/// optional alias.
struct TableRef<'a> {
    name: &'a str,
    alias: Option<&'a str>,
}

/// Extract the (unqualified) table name and optional alias of a join relation.
/// A subquery, table function, or schema-qualified name is rejected.
fn table_ref(factor: &TableFactor) -> Result<TableRef, SelectError> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(SelectError::UnsupportedJoin(
            "a non-table relation (subquery / derived table) in a JOIN".to_owned(),
        ));
    };
    let name = match name.0.as_slice() {
        [part] => part.as_ident().map(|id| id.value.as_str()).ok_or_else(|| {
            SelectError::UnsupportedJoin("a non-identifier table name in a JOIN".to_owned())
        })?,
        _ => {
            return Err(SelectError::UnsupportedJoin(
                "a schema-qualified table name in a JOIN".to_owned(),
            ));
        }
    };
    Ok(TableRef {
        name,
        alias: alias.as_ref().map(|a| a.name.value.as_str()),
    })
}

/// Resolve a join table at the snapshot, mapping the shared
/// [`TableResolution`](TableResolution) taxonomy to a [`SelectError`] (the same
/// errors the single-table path reports).
fn resolve_join_table<'a>(
    catalog: &'a Catalog,
    table: &str,
    snapshot: SystemTimeMicros,
) -> Result<&'a TableSchema, SelectError> {
    match resolve_table_at(catalog, table, snapshot) {
        TableResolution::Found(schema) => Ok(schema),
        TableResolution::Unknown => Err(SelectError::UnknownTable(table.to_owned())),
        TableResolution::BeforeHistory { first_commit } => Err(SelectError::BeforeHistory {
            table: table.to_owned(),
            snapshot: snapshot.0,
            first_commit: first_commit.0,
        }),
        TableResolution::NotLive => Err(SelectError::TableNotLive {
            table: table.to_owned(),
            snapshot: snapshot.0,
        }),
    }
}

/// Lower a parsed [`JoinOperator`] to a [`JoinType`] and its constraint. Only the
/// left-driven inner/left/semi/anti operators bind; `RIGHT` / `FULL` / `CROSS` /
/// `ASOF` and the apply/array operators are the [`SelectError::UnsupportedJoin`]
/// follow-ups.
fn join_kind_and_constraint(op: &JoinOperator) -> Result<(JoinType, &JoinConstraint), SelectError> {
    use JoinOperator as J;
    Ok(match op {
        J::Inner(c) | J::Join(c) => (JoinType::Inner, c),
        J::Left(c) | J::LeftOuter(c) => (JoinType::Left, c),
        J::Semi(c) | J::LeftSemi(c) => (JoinType::Semi, c),
        J::Anti(c) | J::LeftAnti(c) => (JoinType::Anti, c),
        J::Right(_) | J::RightOuter(_) | J::RightSemi(_) | J::RightAnti(_) => {
            return Err(SelectError::UnsupportedJoin(
                "a RIGHT join — rewrite it as a LEFT join".to_owned(),
            ));
        }
        J::FullOuter(_) => {
            return Err(SelectError::UnsupportedJoin("a FULL OUTER join".to_owned()));
        }
        J::CrossJoin(_) => return Err(SelectError::UnsupportedJoin("a CROSS join".to_owned())),
        _ => {
            return Err(SelectError::UnsupportedJoin(
                "this join operator".to_owned(),
            ));
        }
    })
}

/// Bind a join's `ON` constraint to the `(left_key, right_key)` schema indices of
/// a single `left.col = right.col` equality. The two key columns must share a
/// type.
fn bind_join_condition(
    constraint: &JoinConstraint,
    left: &SideSchema,
    right: &SideSchema,
) -> Result<(usize, usize), SelectError> {
    let JoinConstraint::On(expr) = constraint else {
        return Err(SelectError::JoinCondition(match constraint {
            JoinConstraint::Using(_) => "USING is not supported — use ON".to_owned(),
            JoinConstraint::Natural => "NATURAL join is not supported".to_owned(),
            JoinConstraint::None | JoinConstraint::On(_) => {
                "the JOIN has no ON condition".to_owned()
            }
        }));
    };
    let Expr::BinaryOp {
        left: lhs,
        op: BinaryOperator::Eq,
        right: rhs,
    } = unwrap_nested(expr)
    else {
        return Err(SelectError::JoinCondition(
            "the ON condition is not an equality".to_owned(),
        ));
    };
    // The two operands must be one column from each side; either order is fine.
    let a = resolve_join_column(lhs, left, right)?;
    let b = resolve_join_column(rhs, left, right)?;
    let (((Side::Left, li), (Side::Right, ri)) | ((Side::Right, ri), (Side::Left, li))) = (a, b)
    else {
        return Err(SelectError::JoinCondition(
            "the ON equality must relate a column of each joined table".to_owned(),
        ));
    };
    let left_ty = left.schema.columns()[li].ty();
    let right_ty = right.schema.columns()[ri].ty();
    if left_ty != right_ty {
        return Err(SelectError::JoinColumnTypeMismatch {
            left_column: left.schema.columns()[li].name().to_owned(),
            right_column: right.schema.columns()[ri].name().to_owned(),
            left_type: left_ty,
            right_type: right_ty,
        });
    }
    Ok((li, ri))
}

/// Resolve a column reference in a join (a bare `c` or qualified `t.c`) to the
/// side it belongs to and its index in that side's schema.
///
/// A bare column must be in exactly one side (in both is
/// [`SelectError::AmbiguousColumn`], in neither
/// [`SelectError::UnknownJoinColumn`]). A qualified `t.c`'s qualifier must name one
/// side (by table name or alias), and `c` must be a column of it.
fn resolve_join_column(
    expr: &Expr,
    left: &SideSchema,
    right: &SideSchema,
) -> Result<(Side, usize), SelectError> {
    match expr {
        Expr::Nested(inner) => resolve_join_column(inner, left, right),
        Expr::Identifier(id) => match (left.column_index(&id.value), right.column_index(&id.value))
        {
            (Some(i), None) => Ok((Side::Left, i)),
            (None, Some(j)) => Ok((Side::Right, j)),
            (Some(_), Some(_)) => Err(SelectError::AmbiguousColumn {
                column: id.value.clone(),
            }),
            (None, None) => Err(SelectError::UnknownJoinColumn {
                column: id.value.clone(),
            }),
        },
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return Err(SelectError::UnknownJoinColumn {
                    column: compound_name(parts),
                });
            };
            let (q, c) = (qualifier.value.as_str(), column.value.as_str());
            let on_left = left.qualifier_matches(q);
            let on_right = right.qualifier_matches(q);
            let qualified = || format!("{q}.{c}");
            match (on_left, on_right) {
                (true, false) => left
                    .column_index(c)
                    .map(|i| (Side::Left, i))
                    .ok_or_else(|| SelectError::UnknownJoinColumn {
                        column: qualified(),
                    }),
                (false, true) => right
                    .column_index(c)
                    .map(|j| (Side::Right, j))
                    .ok_or_else(|| SelectError::UnknownJoinColumn {
                        column: qualified(),
                    }),
                (true, true) => Err(SelectError::AmbiguousColumn {
                    column: qualified(),
                }),
                (false, false) => Err(SelectError::UnknownJoinColumn {
                    column: qualified(),
                }),
            }
        }
        other => Err(SelectError::JoinCondition(format!(
            "operand `{other}` is not a column reference"
        ))),
    }
}

/// Render a multi-part identifier (`a.b.c`) for a diagnostic.
fn compound_name(parts: &[sqlparser::ast::Ident]) -> String {
    parts
        .iter()
        .map(|p| p.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// A bound join projection: the output column references and their aligned
/// `(name, type)` result header.
type JoinProjection = (Vec<JoinColumnRef>, Vec<(String, LogicalType)>);

/// Bind a join's projection to its output columns ([`JoinColumnRef`]) and the
/// result header. `SELECT *` is the left side's columns followed by the right's
/// (left only for `SEMI` / `ANTI`); a named list resolves each item to a side.
fn bind_join_projection(
    select: &Select,
    join_type: JoinType,
    left: &SideSchema,
    right: &SideSchema,
) -> Result<JoinProjection, SelectError> {
    // `SELECT *` — every column of each kept side, in schema order.
    if let [SelectItem::Wildcard(_)] = select.projection.as_slice() {
        let mut output = Vec::new();
        let mut columns = Vec::new();
        for (i, c) in left.schema.columns().iter().enumerate() {
            output.push(JoinColumnRef::Left(i));
            columns.push((c.name().to_owned(), c.ty()));
        }
        if join_type.keeps_right() {
            for (j, c) in right.schema.columns().iter().enumerate() {
                output.push(JoinColumnRef::Right(j));
                columns.push((c.name().to_owned(), c.ty()));
            }
        }
        return Ok((output, columns));
    }

    let mut output = Vec::with_capacity(select.projection.len());
    let mut columns = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(
                expr @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Nested(_)),
            ) => expr,
            SelectItem::Wildcard(_) => {
                return Err(SelectError::UnsupportedJoinProjection(
                    "`*` mixed with named columns".to_owned(),
                ));
            }
            other => return Err(SelectError::UnsupportedJoinProjection(other.to_string())),
        };
        let (side, idx) = resolve_join_column(expr, left, right)?;
        let schema = match side {
            Side::Left => left.schema,
            Side::Right => {
                // A SEMI / ANTI join's result is the left table alone, so a right
                // column has nowhere to come from.
                if !join_type.keeps_right() {
                    return Err(SelectError::UnsupportedJoinProjection(
                        "a SEMI/ANTI join projects only its left table's columns".to_owned(),
                    ));
                }
                right.schema
            }
        };
        let col = &schema.columns()[idx];
        columns.push((col.name().to_owned(), col.ty()));
        output.push(match side {
            Side::Left => JoinColumnRef::Left(idx),
            Side::Right => JoinColumnRef::Right(idx),
        });
    }
    Ok((output, columns))
}

/// Bind a parsed [`PeriodPredicateClause`] to a [`BoundPeriodPredicate`]
/// ([STL-165], [STL-193]).
///
/// Each `PERIOD(from, to)` endpoint is either a constant instant — folded the
/// same way an `AS OF` operand is ([`resolve_as_of`]) — or a reference to a value
/// column in `schema`, bound to its index. A fully-constant operand must be a
/// well-formed half-open interval at bind time (`from < to`); an operand with a
/// column endpoint defers that check to evaluation.
fn bind_period_predicate(
    clause: &PeriodPredicateClause,
    now: SystemTimeMicros,
    schema: &TableSchema,
    table: &str,
) -> Result<BoundPeriodPredicate, SelectError> {
    Ok(BoundPeriodPredicate {
        left: bind_period_operand(&clause.left, now, schema, table)?,
        predicate: clause.predicate,
        right: bind_period_operand(&clause.right, now, schema, table)?,
    })
}

/// Bind one `PERIOD(from, to)` operand to a [`BoundPeriod`] of two endpoints.
///
/// A fully-constant operand is checked for `from < to` here (the STL-165 rule);
/// an operand naming a value column can only be checked per row, so its
/// well-formedness is left to evaluation.
fn bind_period_operand(
    operand: &PeriodExpr,
    now: SystemTimeMicros,
    schema: &TableSchema,
    table: &str,
) -> Result<BoundPeriod, SelectError> {
    let from = bind_period_endpoint(&operand.from, now, schema, table)?;
    let to = bind_period_endpoint(&operand.to, now, schema, table)?;
    if let (PeriodEndpoint::Const(f), PeriodEndpoint::Const(t)) = (from, to) {
        Interval::new(f, t).map_err(SelectError::PeriodInterval)?;
    }
    Ok(BoundPeriod { from, to })
}

/// Bind one `PERIOD(...)` endpoint: a bare column identifier resolves to a value
/// column ([STL-193]); anything else folds to a constant instant ([STL-165]).
///
/// A column endpoint must be a microsecond-instant type — `BIGINT`, `TIMESTAMP`,
/// or `TIMESTAMPTZ` — since the period codec reads each cell as `i64` µs; an
/// `INT` (too narrow) or `DATE` (days, not µs) column is rejected rather than
/// silently mis-scaled.
fn bind_period_endpoint(
    expr: &Expr,
    now: SystemTimeMicros,
    schema: &TableSchema,
    table: &str,
) -> Result<PeriodEndpoint, SelectError> {
    if let Some(column) = where_column(expr) {
        let index = column_index(schema, column).ok_or_else(|| SelectError::UnknownColumn {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
        let ty = schema.columns()[index].ty();
        if !matches!(
            ty,
            LogicalType::Int8 | LogicalType::Timestamp | LogicalType::TimestampTz
        ) {
            return Err(SelectError::PeriodColumnType {
                table: table.to_owned(),
                column: column.to_owned(),
                ty,
            });
        }
        return Ok(PeriodEndpoint::Column(index));
    }
    resolve_as_of(expr, now)
        .map(|instant| PeriodEndpoint::Const(instant.0))
        .map_err(SelectError::PeriodOperand)
}

/// Lower a `WHERE` clause to a [`BoundPredicate`], or `None` when there is none.
///
/// v0.2 lowers exactly `<column> = <literal>` (the column on either side): the
/// column must exist in the schema and the literal must fold to its type. Every
/// other shape is [`SelectError::UnsupportedPredicate`] — never silently dropped,
/// since dropping a filter returns rows the query excluded.
fn bind_filter(
    select: &Select,
    schema: &TableSchema,
    table: &str,
) -> Result<Option<BoundPredicate>, SelectError> {
    let Some(expr) = select.selection.as_ref() else {
        return Ok(None);
    };
    // Peel parentheses around the whole predicate so `WHERE (id = 1)` binds like
    // `WHERE id = 1` — the column/comparand sides are unwrapped the same way.
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested(expr)
    else {
        return Err(SelectError::UnsupportedPredicate(
            "the WHERE is not an equality".to_owned(),
        ));
    };
    // The column may be on either side: `col = <lit>` or `<lit> = col`. A bare
    // identifier is a column; anything else (a literal, a qualified name, an
    // expression) is the comparand side.
    let (column, value_expr) = match (where_column(left), where_column(right)) {
        (Some(column), None) => (column, right.as_ref()),
        (None, Some(column)) => (column, left.as_ref()),
        _ => {
            return Err(SelectError::UnsupportedPredicate(
                "the WHERE is not `<column> = <literal>`".to_owned(),
            ));
        }
    };
    let column_index = schema
        .columns()
        .iter()
        .position(|c| c.name() == column)
        .ok_or_else(|| SelectError::UnknownColumn {
            table: table.to_owned(),
            column: column.to_owned(),
        })?;
    let ty = schema.columns()[column_index].ty();
    let value = fold::fold_scalar(value_expr, ty)
        .map_err(|err| SelectError::UnsupportedPredicate(predicate_reason(&err, column, ty)))?;
    Ok(Some(BoundPredicate {
        column: column.to_owned(),
        column_index,
        value,
    }))
}

/// Peel any number of parentheses (`Expr::Nested`) wrapping `expr`, returning the
/// inner expression. Lets a fully-parenthesized predicate bind like its bare form.
const fn unwrap_nested(expr: &Expr) -> &Expr {
    let mut expr = expr;
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

/// The bare column name a `WHERE` side references, peeling parentheses; `None`
/// for any non-bare-identifier expression (a literal, a qualified name, an
/// arithmetic expression), which marks the comparand side.
fn where_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(id) => Some(id.value.as_str()),
        Expr::Nested(inner) => where_column(inner),
        _ => None,
    }
}

/// Render a literal-fold failure as the reason carried by
/// [`SelectError::UnsupportedPredicate`].
fn predicate_reason(err: &FoldError, column: &str, ty: LogicalType) -> String {
    match err {
        FoldError::Null => format!("NULL cannot be compared to column {column:?}"),
        FoldError::TypeMismatch { found } => {
            format!("{found} is not a {ty} value for column {column:?}")
        }
        FoldError::BadLiteral { literal, reason } => {
            let detail = reason.map(|r| format!(" ({r})")).unwrap_or_default();
            format!("{literal:?} is not a valid {ty} for column {column:?}{detail}")
        }
        FoldError::UnsupportedType(ty) => {
            format!("comparing a {ty} column ({column:?}) to a literal is not supported yet")
        }
    }
}

/// Resolve the statement's `(system-time, valid-time)` snapshots from its
/// `FOR { SYSTEM_TIME | VALID_TIME } AS OF` qualifiers.
///
/// The system-time snapshot defaults to `now` (the transaction snapshot) when no
/// system qualifier is given — a plain `SELECT` reads the present. The valid-time
/// instant is `None` unless a `FOR VALID_TIME AS OF` qualifier is present; both
/// fold the same way, and `now` is the value `now()` folds to on either axis.
///
/// # Errors
///
/// [`SelectError::MultipleAsOf`] if either axis carries more than one qualifier,
/// or [`SelectError::AsOf`] if an expression cannot be folded to an instant.
fn resolve_snapshots(
    stmt: &Statement,
    now: SystemTimeMicros,
) -> Result<(SystemTimeMicros, Option<SystemTimeMicros>), SelectError> {
    let mut system: Option<SystemTimeMicros> = None;
    let mut valid: Option<SystemTimeMicros> = None;
    for as_of in &stmt.temporal.as_of {
        let slot = match as_of.dimension {
            TimeDimension::System => &mut system,
            TimeDimension::Valid => &mut valid,
        };
        if slot.is_some() {
            return Err(SelectError::MultipleAsOf(as_of.dimension));
        }
        *slot = Some(resolve_as_of(&as_of.timestamp, now)?);
    }
    Ok((system.unwrap_or(now), valid))
}

/// The outcome of resolving a table name against the catalog at a snapshot.
///
/// Shared by the [`SELECT`](bind_select) and [`DML`](crate::bind_dml) binders so
/// both report the *same* taxonomy for a name that does not resolve — a name the
/// catalog never registered, a snapshot before the table's first commit, or a
/// snapshot in a dropped / re-creation-gap era. Each binder maps these to its own
/// error type; the discrimination logic lives here, once.
pub(crate) enum TableResolution<'a> {
    /// The table resolved to a live schema version at the snapshot.
    Found(&'a TableSchema),
    /// The catalog has never registered this name.
    Unknown,
    /// The snapshot precedes the table's first commit — a *before-history* read.
    BeforeHistory {
        /// The table's first-commit system time; the snapshot precedes it.
        first_commit: SystemTimeMicros,
    },
    /// The name is in the catalog's timeline but not live at the snapshot
    /// (dropped by then, or in the gap before a re-creation).
    NotLive,
}

/// Resolve `table` to the schema version live at `snapshot`, distinguishing the
/// three "no live version" cases [`resolve`](Catalog::resolve) collapses to
/// `None` (it returns the schema or nothing; [`history_start`](Catalog::history_start)
/// recovers *why* there is no schema).
pub(crate) fn resolve_table_at<'a>(
    catalog: &'a Catalog,
    table: &str,
    snapshot: SystemTimeMicros,
) -> TableResolution<'a> {
    if let Some(schema) = catalog.resolve(table, snapshot) {
        return TableResolution::Found(schema);
    }
    match catalog.history_start(table) {
        None => TableResolution::Unknown,
        Some(first) if snapshot < first => TableResolution::BeforeHistory {
            first_commit: first,
        },
        Some(_) => TableResolution::NotLive,
    }
}

/// Fold an `AS OF <expr>` to a concrete system-time instant, given the value of
/// `now()`.
///
/// Supported forms: `now()`; `now() ± interval '<n> <unit>[ …]'` (and nested
/// parentheses around either); and a bare non-negative-or-negative integer read
/// as explicit microseconds. Every other form — column references, absolute
/// `TIMESTAMP '…'` literals, function calls other than `now()` — is rejected
/// rather than guessed at.
///
/// # Errors
///
/// [`AsOfError`]: an unsupported expression shape, a malformed or calendar
/// interval, or arithmetic that overflows the `i64` microsecond range.
pub fn resolve_as_of(expr: &Expr, now: SystemTimeMicros) -> Result<SystemTimeMicros, AsOfError> {
    match expr {
        // Parentheses (the demo wraps the whole expression in them).
        Expr::Nested(inner) => resolve_as_of(inner, now),
        // `now()` — the transaction snapshot.
        Expr::Function(func) if is_now(func) => Ok(now),
        // An explicit microsecond instant.
        Expr::Value(value) => integer_instant(&value.value),
        // `<instant> ± interval '…'`.
        Expr::BinaryOp { left, op, right } => {
            let base = resolve_as_of(left, now)?;
            let magnitude = interval_micros(right)?;
            let offset = match op {
                BinaryOperator::Plus => magnitude,
                BinaryOperator::Minus => magnitude.checked_neg().ok_or(AsOfError::Overflow)?,
                other => {
                    return Err(AsOfError::Unsupported(format!("operator `{other}`")));
                }
            };
            base.0
                .checked_add(offset)
                .map(SystemTimeMicros)
                .ok_or(AsOfError::Overflow)
        }
        other => Err(AsOfError::Unsupported(describe_expr(other))),
    }
}

/// Read a literal as an explicit microsecond instant — only an integer numeric
/// literal qualifies.
fn integer_instant(value: &Value) -> Result<SystemTimeMicros, AsOfError> {
    match value {
        Value::Number(digits, _) => digits.parse::<i64>().map(SystemTimeMicros).map_err(|_| {
            // A digits-only literal that fails to parse is an integer too large
            // for the system-time range — surface that as `Overflow`, not as a
            // misleading "non-integer" diagnostic. A genuinely non-integer
            // literal (a decimal/scientific float) keeps the latter.
            if is_integer_literal(digits) {
                AsOfError::Overflow
            } else {
                AsOfError::Unsupported(format!("non-integer timestamp literal `{digits}`"))
            }
        }),
        other => Err(AsOfError::Unsupported(format!(
            "literal `{other}` is not an instant"
        ))),
    }
}

/// Whether `s` is a plain base-10 integer literal (optional leading `-`, then
/// only ASCII digits) — used to tell an overflowing integer apart from a
/// non-integer numeric literal.
fn is_integer_literal(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Whether a parsed function call is a no-argument `now()` (case-insensitive,
/// unqualified).
fn is_now(func: &sqlparser::ast::Function) -> bool {
    let [part] = func.name.0.as_slice() else {
        return false;
    };
    let is_now_name = part
        .as_ident()
        .is_some_and(|id| id.value.eq_ignore_ascii_case("now"));
    let no_args = match &func.args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty(),
        FunctionArguments::Subquery(_) => false,
    };
    is_now_name && no_args
}

/// The fixed microsecond magnitude of an `interval '…'` operand (always
/// non-negative; the `+`/`-` operator decides direction).
fn interval_micros(expr: &Expr) -> Result<i64, AsOfError> {
    let Expr::Interval(interval) = expr else {
        return Err(AsOfError::Unsupported(format!(
            "arithmetic operand `{}` is not an interval",
            describe_expr(expr)
        )));
    };
    let Expr::Value(value) = interval.value.as_ref() else {
        return Err(AsOfError::BadInterval(interval.value.to_string()));
    };
    let Value::SingleQuotedString(text) = &value.value else {
        return Err(AsOfError::BadInterval(value.value.to_string()));
    };
    let leading_unit = interval.leading_field.as_ref().map(ToString::to_string);
    parse_interval(text, leading_unit.as_deref())
}

/// Parse an interval body into microseconds. Accepts the Postgres-style
/// `'<n> <unit> [<n> <unit> …]'` (unit inside the string), and the
/// `'<n>' <UNIT>` form where the unit is a leading field.
fn parse_interval(text: &str, leading_unit: Option<&str>) -> Result<i64, AsOfError> {
    let tokens: Vec<&str> = text.split_whitespace().collect();

    // `'<n>' <UNIT>` — a single numeric token with the unit as a leading field.
    if let ([digits], Some(unit)) = (tokens.as_slice(), leading_unit) {
        return scaled(digits, unit, text);
    }

    // `'<n> <unit> [<n> <unit> …]'` — number/unit pairs, summed.
    if tokens.is_empty() || tokens.len() % 2 != 0 {
        return Err(AsOfError::BadInterval(text.to_owned()));
    }
    let mut total: i64 = 0;
    for pair in tokens.chunks_exact(2) {
        total = total
            .checked_add(scaled(pair[0], pair[1], text)?)
            .ok_or(AsOfError::Overflow)?;
    }
    Ok(total)
}

/// `digits × micros-per(unit)`, with overflow and unit checking.
fn scaled(digits: &str, unit: &str, whole: &str) -> Result<i64, AsOfError> {
    let count: i64 = digits
        .parse()
        .map_err(|_| AsOfError::BadInterval(whole.to_owned()))?;
    count
        .checked_mul(unit_micros(unit)?)
        .ok_or(AsOfError::Overflow)
}

/// Microseconds in one of `unit`. Calendar units (month/year) are rejected —
/// they have no fixed microsecond length.
fn unit_micros(unit: &str) -> Result<i64, AsOfError> {
    Ok(match unit.to_ascii_lowercase().as_str() {
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" => 1,
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => 1_000,
        "second" | "seconds" | "sec" | "secs" => 1_000_000,
        "minute" | "minutes" | "min" | "mins" => 60_000_000,
        "hour" | "hours" | "hr" | "hrs" => 3_600_000_000,
        "day" | "days" => 86_400_000_000,
        "week" | "weeks" => 604_800_000_000,
        "month" | "months" | "mon" | "year" | "years" | "yr" | "yrs" => {
            return Err(AsOfError::CalendarInterval(unit.to_owned()));
        }
        _ => {
            return Err(AsOfError::BadInterval(format!(
                "unknown interval unit `{unit}`"
            )));
        }
    })
}

/// A short label for an expression shape, for the "unsupported" diagnostics.
fn describe_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => "column reference".to_owned(),
        Expr::TypedString { .. } => "typed literal (e.g. TIMESTAMP '…')".to_owned(),
        Expr::Interval(_) => "bare interval (AS OF needs an instant, not a duration)".to_owned(),
        Expr::Function(_) => "function call other than now()".to_owned(),
        other => format!("`{other}`"),
    }
}

/// The single `SELECT` body of a query statement, after rejecting every query-
/// and select-level clause outside the v0.1 single-table snapshot-scan surface.
///
/// [`BoundSelect`] carries only a table, snapshot, and projection — so a clause
/// it cannot represent (`ORDER BY`, `LIMIT`, `GROUP BY`, …) must be rejected,
/// not silently dropped when the plan is later executed.
fn single_select(body: &SqlStatement) -> Result<&Select, SelectError> {
    let SqlStatement::Query(query) = body else {
        return Err(SelectError::NotSelect);
    };
    reject_unsupported_query_clauses(query)?;
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        SetExpr::Query(_) => return Err(SelectError::UnsupportedFrom("parenthesized subquery")),
        SetExpr::SetOperation { .. } => {
            return Err(SelectError::UnsupportedFrom("UNION/INTERSECT/EXCEPT"));
        }
        _ => return Err(SelectError::NotSelect),
    };
    reject_unsupported_select_clauses(select)?;
    Ok(select)
}

/// Reject query-level clauses outside the v0.1 surface. `WHERE` lives on the
/// inner `Select` and is deliberately *kept* (lowered downstream); everything
/// that reshapes or reorders the result set is rejected.
fn reject_unsupported_query_clauses(query: &Query) -> Result<(), SelectError> {
    let reject = |what| Err(SelectError::UnsupportedClause(what));
    if query.with.is_some() {
        return reject("WITH (CTE)");
    }
    if query.order_by.is_some() {
        return reject("ORDER BY");
    }
    if query.limit_clause.is_some() {
        return reject("LIMIT/OFFSET");
    }
    if query.fetch.is_some() {
        return reject("FETCH");
    }
    if !query.locks.is_empty() {
        return reject("FOR UPDATE/SHARE");
    }
    Ok(())
}

/// Reject select-level clauses outside the v0.1 surface — anything that
/// aggregates, deduplicates, limits, or otherwise transforms the row set
/// [`BoundSelect`] does not model. `WHERE` ([`Select::selection`]) is allowed.
fn reject_unsupported_select_clauses(select: &Select) -> Result<(), SelectError> {
    let reject = |what| Err(SelectError::UnsupportedClause(what));
    if select.distinct.is_some() {
        return reject("DISTINCT");
    }
    if select.top.is_some() {
        return reject("TOP");
    }
    if select.into.is_some() {
        return reject("SELECT INTO");
    }
    // `GROUP BY <exprs>` is bound as an aggregate query ([STL-171], in
    // `bind_aggregate`); only the non-standard `GROUP BY ALL` (group by the whole
    // projection) is rejected here, since the binder does not model it. Trailing
    // modifiers (ROLLUP/CUBE/GROUPING SETS) on an expression list are rejected in
    // `bind_group_by` with a precise reason.
    if matches!(&select.group_by, GroupByExpr::All(_)) {
        return reject("GROUP BY ALL");
    }
    if select.having.is_some() {
        return reject("HAVING");
    }
    if select.qualify.is_some() {
        return reject("QUALIFY");
    }
    if !select.named_window.is_empty() {
        return reject("WINDOW");
    }
    if !select.lateral_views.is_empty() {
        return reject("LATERAL VIEW");
    }
    if select.prewhere.is_some() {
        return reject("PREWHERE");
    }
    if select.exclude.is_some() {
        return reject("EXCLUDE");
    }
    if select.value_table_mode.is_some() {
        return reject("SELECT AS VALUE/STRUCT");
    }
    if !select.cluster_by.is_empty() {
        return reject("CLUSTER BY");
    }
    if !select.distribute_by.is_empty() {
        return reject("DISTRIBUTE BY");
    }
    if !select.sort_by.is_empty() {
        return reject("SORT BY");
    }
    if !select.connect_by.is_empty() {
        return reject("CONNECT BY");
    }
    Ok(())
}

/// The single, unqualified table name a `SELECT` reads from.
fn single_table(select: &Select) -> Result<&str, SelectError> {
    let [from] = select.from.as_slice() else {
        return Err(SelectError::UnsupportedFrom("not exactly one table"));
    };
    if !from.joins.is_empty() {
        return Err(SelectError::UnsupportedFrom("join"));
    }
    let TableFactor::Table { name, .. } = &from.relation else {
        return Err(SelectError::UnsupportedFrom("non-table relation"));
    };
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|id| id.value.as_str())
            .ok_or(SelectError::UnsupportedFrom("non-identifier table name")),
        _ => Err(SelectError::UnsupportedFrom("schema-qualified table name")),
    }
}

/// Lower the projection list to [`Projection`]: `*` or bare column names only.
fn bind_projection(select: &Select) -> Result<Projection, SelectError> {
    // `SELECT *` is the lone wildcard item.
    if let [SelectItem::Wildcard(_)] = select.projection.as_slice() {
        return Ok(Projection::All);
    }
    let mut columns = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => columns.push(ident.value.clone()),
            SelectItem::Wildcard(_) => {
                return Err(SelectError::UnsupportedProjection(
                    "`*` mixed with named columns".to_owned(),
                ));
            }
            other => return Err(SelectError::UnsupportedProjection(other.to_string())),
        }
    }
    Ok(Projection::Columns(columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use stele_catalog::{ColumnDef, TableTemporal, ValidTimeSpec};
    use stele_common::types::LogicalType;

    /// `now()` as a fixed instant for deterministic folding tests.
    const NOW: SystemTimeMicros = SystemTimeMicros(2_000_000_000_000_000);

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1, "expected one statement");
        stmts.remove(0)
    }

    fn as_of(sql: &str) -> Result<SystemTimeMicros, AsOfError> {
        let stmt = parse_one(sql);
        let [as_of] = stmt.temporal.as_of.as_slice() else {
            panic!("expected one AS OF qualifier");
        };
        resolve_as_of(&as_of.timestamp, NOW)
    }

    /// A catalog with `account` created at system time `created`.
    fn catalog_with_account(created: i64) -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "account",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(created),
            )
            .expect("create account");
        catalog
    }

    /// A catalog with `booking` — a bitemporal table opting into a valid-time
    /// period over `(vf, vt)` — created at system time `created`.
    fn catalog_with_booking(created: i64) -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "booking",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("vf", LogicalType::Timestamp).expect("col"),
                    ColumnDef::new("vt", LogicalType::Timestamp).expect("col"),
                ],
                TableTemporal::with_valid_time(ValidTimeSpec::new("vf", "vt").expect("spec")),
                SystemTimeMicros(created),
            )
            .expect("create booking");
        catalog
    }

    /// A fully-constant `PERIOD(from, to)` operand, the STL-165 shape.
    const fn const_period(from: i64, to: i64) -> BoundPeriod {
        BoundPeriod {
            from: PeriodEndpoint::Const(from),
            to: PeriodEndpoint::Const(to),
        }
    }

    #[test]
    fn now_folds_to_the_transaction_snapshot() {
        assert_eq!(
            as_of("SELECT balance FROM account FOR SYSTEM_TIME AS OF now()"),
            Ok(NOW)
        );
    }

    #[test]
    fn the_identity_demo_expression_folds_one_second_before_now() {
        // The README's exact AS OF clause.
        assert_eq!(
            as_of(
                "SELECT balance FROM account \
                 FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1"
            ),
            Ok(SystemTimeMicros(NOW.0 - 1_000_000))
        );
    }

    #[test]
    fn interval_units_and_directions() {
        let cases = [
            ("now() + interval '1 second'", NOW.0 + 1_000_000),
            ("now() - interval '2 hours'", NOW.0 - 2 * 3_600_000_000),
            ("now() - interval '1 day'", NOW.0 - 86_400_000_000),
            ("now() - interval '500 milliseconds'", NOW.0 - 500_000),
            ("now() + interval '1 minute 30 seconds'", NOW.0 + 90_000_000),
        ];
        for (expr, want) in cases {
            let sql = format!("SELECT x FROM t FOR SYSTEM_TIME AS OF ({expr})");
            assert_eq!(as_of(&sql), Ok(SystemTimeMicros(want)), "{expr}");
        }
    }

    #[test]
    fn integer_literal_is_an_explicit_instant() {
        assert_eq!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF 1700000000000000"),
            Ok(SystemTimeMicros(1_700_000_000_000_000))
        );
    }

    #[test]
    fn calendar_intervals_are_rejected() {
        assert_eq!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF (now() - interval '1 month')"),
            Err(AsOfError::CalendarInterval("month".to_owned()))
        );
        assert_eq!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF (now() - interval '1 year')"),
            Err(AsOfError::CalendarInterval("year".to_owned()))
        );
    }

    #[test]
    fn unfoldable_as_of_expressions_are_rejected_not_guessed() {
        // An absolute typed literal is not folded at v0.1.
        assert!(matches!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-02 03:04:05'"),
            Err(AsOfError::Unsupported(_))
        ));
        // A bad interval unit.
        assert!(matches!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF (now() - interval '1 fortnight')"),
            Err(AsOfError::BadInterval(_))
        ));
    }

    #[test]
    fn no_as_of_defaults_to_the_transaction_snapshot() {
        let stmt = parse_one("SELECT balance FROM account WHERE id = 1");
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let bound = bind_select(&stmt, &ctx).expect("bind");
        assert_eq!(bound.snapshot, NOW);
        assert_eq!(bound.table, "account");
        assert_eq!(
            bound.projection,
            Projection::Columns(vec!["balance".to_owned()])
        );
    }

    #[test]
    fn wildcard_projects_all() {
        let stmt = parse_one("SELECT * FROM account");
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        assert_eq!(
            bind_select(&stmt, &ctx).expect("bind").projection,
            Projection::All
        );
    }

    #[test]
    fn as_of_before_first_commit_is_before_history() {
        // Table created at 1_000; AS OF resolves to 999 < 1_000.
        let stmt = parse_one("SELECT balance FROM account FOR SYSTEM_TIME AS OF 999");
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        assert_eq!(
            bind_select(&stmt, &ctx),
            Err(SelectError::BeforeHistory {
                table: "account".to_owned(),
                snapshot: 999,
                first_commit: 1_000,
            })
        );
    }

    #[test]
    fn unknown_table_is_distinct_from_before_history() {
        let stmt = parse_one("SELECT balance FROM ghost FOR SYSTEM_TIME AS OF 999");
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        assert_eq!(
            bind_select(&stmt, &ctx),
            Err(SelectError::UnknownTable("ghost".to_owned()))
        );
    }

    #[test]
    fn dropped_table_is_not_live_not_before_history() {
        let mut catalog = catalog_with_account(1_000);
        catalog
            .drop_table("account", SystemTimeMicros(2_000))
            .expect("drop");
        // AS OF 3_000: after the drop, but inside the table's recorded timeline.
        let stmt = parse_one("SELECT balance FROM account FOR SYSTEM_TIME AS OF 3000");
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        assert_eq!(
            bind_select(&stmt, &ctx),
            Err(SelectError::TableNotLive {
                table: "account".to_owned(),
                snapshot: 3_000,
            })
        );
        // …but AS OF 1_500 (a live era before the drop) binds fine.
        let live = parse_one("SELECT balance FROM account FOR SYSTEM_TIME AS OF 1500");
        assert!(bind_select(&live, &ctx).is_ok());
    }

    #[test]
    fn valid_time_as_of_on_a_system_only_table_is_unsupported() {
        // `account` has no valid axis, so a valid-time AS OF has nothing to
        // travel along — caught at bind time.
        let stmt = parse_one("SELECT balance FROM account FOR VALID_TIME AS OF now()");
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        assert_eq!(
            bind_select(&stmt, &ctx),
            Err(SelectError::ValidTimeUnsupported {
                table: "account".to_owned(),
            })
        );
    }

    #[test]
    fn valid_time_as_of_binds_on_a_bitemporal_table() {
        // Valid-only AS OF: the valid instant is carried; the system axis
        // defaults to the transaction snapshot.
        let stmt = parse_one(
            "SELECT id FROM booking FOR VALID_TIME AS OF (now() - interval '1 day') WHERE id = 1",
        );
        let catalog = catalog_with_booking(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let bound = bind_select(&stmt, &ctx).expect("bind");
        assert_eq!(bound.snapshot, NOW);
        assert_eq!(
            bound.valid_snapshot,
            Some(SystemTimeMicros(NOW.0 - 86_400_000_000))
        );
    }

    #[test]
    fn both_axes_as_of_carries_both_instants() {
        let catalog = catalog_with_booking(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        // Both orders bind to the same pair — the axis, not the position, decides.
        for sql in [
            "SELECT id FROM booking \
             FOR SYSTEM_TIME AS OF 1700000000000000 FOR VALID_TIME AS OF 1600000000000000",
            "SELECT id FROM booking \
             FOR VALID_TIME AS OF 1600000000000000 FOR SYSTEM_TIME AS OF 1700000000000000",
        ] {
            let stmt = parse_one(sql);
            let bound = bind_select(&stmt, &ctx).expect("bind");
            assert_eq!(
                bound.snapshot,
                SystemTimeMicros(1_700_000_000_000_000),
                "{sql}"
            );
            assert_eq!(
                bound.valid_snapshot,
                Some(SystemTimeMicros(1_600_000_000_000_000)),
                "{sql}"
            );
        }
    }

    #[test]
    fn system_only_as_of_leaves_the_valid_axis_unset() {
        let stmt = parse_one("SELECT id FROM booking FOR SYSTEM_TIME AS OF 1700000000000000");
        let catalog = catalog_with_booking(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let bound = bind_select(&stmt, &ctx).expect("bind");
        assert_eq!(bound.snapshot, SystemTimeMicros(1_700_000_000_000_000));
        assert_eq!(bound.valid_snapshot, None);
    }

    #[test]
    fn a_repeated_axis_is_rejected() {
        let catalog = catalog_with_booking(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let dup_system =
            parse_one("SELECT id FROM booking FOR SYSTEM_TIME AS OF 1 FOR SYSTEM_TIME AS OF 2");
        assert_eq!(
            bind_select(&dup_system, &ctx),
            Err(SelectError::MultipleAsOf(TimeDimension::System))
        );
        let dup_valid =
            parse_one("SELECT id FROM booking FOR VALID_TIME AS OF 1 FOR VALID_TIME AS OF 2");
        assert_eq!(
            bind_select(&dup_valid, &ctx),
            Err(SelectError::MultipleAsOf(TimeDimension::Valid))
        );
    }

    #[test]
    fn overflowing_integer_literal_is_overflow_not_non_integer() {
        // A digits-only literal too large for i64 must read as Overflow…
        assert_eq!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF 99999999999999999999999"),
            Err(AsOfError::Overflow)
        );
        // …while a genuine non-integer numeric literal stays Unsupported.
        assert!(matches!(
            as_of("SELECT x FROM t FOR SYSTEM_TIME AS OF 1.5"),
            Err(AsOfError::Unsupported(_))
        ));
    }

    #[test]
    fn unsupported_query_and_select_clauses_are_rejected() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        for sql in [
            "SELECT balance FROM account ORDER BY balance",
            "SELECT balance FROM account LIMIT 1",
            "SELECT DISTINCT balance FROM account",
            // `GROUP BY balance` now binds as an aggregate query ([STL-171]);
            // HAVING and GROUP BY ALL remain unsupported clauses.
            "SELECT balance FROM account GROUP BY balance HAVING balance > 0",
            "SELECT balance FROM account GROUP BY ALL",
            "WITH t AS (SELECT 1) SELECT balance FROM account",
        ] {
            let stmt = parse_one(sql);
            assert!(
                matches!(
                    bind_select(&stmt, &ctx),
                    Err(SelectError::UnsupportedClause(_))
                ),
                "expected UnsupportedClause for: {sql}"
            );
        }
    }

    #[test]
    fn projecting_an_unknown_column_is_rejected_at_bind_time() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let stmt = parse_one("SELECT nonesuch FROM account");
        assert_eq!(
            bind_select(&stmt, &ctx),
            Err(SelectError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
        // The demo's real columns still bind.
        let ok = parse_one("SELECT id, balance FROM account");
        assert_eq!(
            bind_select(&ok, &ctx).expect("bind").projection,
            Projection::Columns(vec!["id".to_owned(), "balance".to_owned()])
        );
    }

    #[test]
    fn non_select_is_rejected() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let ddl = parse_one("CREATE TABLE t (a INT) WITH SYSTEM VERSIONING");
        assert_eq!(bind_select(&ddl, &ctx), Err(SelectError::NotSelect));
    }

    #[test]
    fn a_join_against_an_unknown_table_reports_the_unknown_table() {
        // Joins now bind ([STL-172]); a join with an unknown right table fails on
        // *resolution*, not as an unsupported FROM shape.
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let join = parse_one("SELECT id FROM account JOIN other ON account.id = other.id");
        assert_eq!(
            bind_select(&join, &ctx),
            Err(SelectError::UnknownTable("other".to_owned()))
        );
    }

    fn bind(sql: &str, catalog: &Catalog) -> Result<BoundSelect, SelectError> {
        let ctx = BindContext {
            snapshot: NOW,
            catalog,
        };
        bind_select(&parse_one(sql), &ctx)
    }

    #[test]
    fn no_where_leaves_the_filter_unset() {
        let catalog = catalog_with_account(1_000);
        assert_eq!(
            bind("SELECT balance FROM account", &catalog)
                .unwrap()
                .filter,
            None
        );
    }

    #[test]
    fn where_on_the_key_binds_to_column_zero() {
        // `id` is the business key — column index 0 — so the executor can push it
        // down to zone-map pruning.
        let catalog = catalog_with_account(1_000);
        assert_eq!(
            bind("SELECT balance FROM account WHERE id = 7", &catalog)
                .unwrap()
                .filter,
            Some(BoundPredicate {
                column: "id".to_owned(),
                column_index: 0,
                value: ScalarValue::Int4(7),
            })
        );
    }

    #[test]
    fn where_on_a_value_column_binds_to_its_index() {
        // `balance` is a value column — index 1 — folded against its int4 type.
        // The column may sit on either side of the `=`.
        let catalog = catalog_with_account(1_000);
        let want = Some(BoundPredicate {
            column: "balance".to_owned(),
            column_index: 1,
            value: ScalarValue::Int4(100),
        });
        assert_eq!(
            bind("SELECT id FROM account WHERE balance = 100", &catalog)
                .unwrap()
                .filter,
            want
        );
        assert_eq!(
            bind("SELECT id FROM account WHERE 100 = balance", &catalog)
                .unwrap()
                .filter,
            want
        );
    }

    #[test]
    fn a_parenthesized_where_binds_like_its_bare_form() {
        let catalog = catalog_with_account(1_000);
        assert_eq!(
            bind("SELECT balance FROM account WHERE (id = 7)", &catalog)
                .unwrap()
                .filter,
            Some(BoundPredicate {
                column: "id".to_owned(),
                column_index: 0,
                value: ScalarValue::Int4(7),
            })
        );
    }

    #[test]
    fn where_on_an_unknown_column_is_rejected() {
        let catalog = catalog_with_account(1_000);
        assert_eq!(
            bind("SELECT id FROM account WHERE nope = 1", &catalog),
            Err(SelectError::UnknownColumn {
                table: "account".to_owned(),
                column: "nope".to_owned(),
            })
        );
    }

    #[test]
    fn unsupported_where_shapes_are_rejected_not_dropped() {
        // A dropped filter would return rows the query excluded — a wrong answer —
        // so each unsupported shape is a bind error.
        let catalog = catalog_with_account(1_000);
        for sql in [
            "SELECT id FROM account WHERE balance > 100", // non-equality
            "SELECT id FROM account WHERE id = balance",  // column = column
            "SELECT id FROM account WHERE balance = 'x'", // type mismatch
            "SELECT id FROM account WHERE balance = NULL", // NULL comparand
            "SELECT id FROM account WHERE id = 1 AND balance = 2", // conjunction
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::UnsupportedPredicate(_))
                ),
                "expected UnsupportedPredicate for: {sql}"
            );
        }
    }

    // ---- period predicates (STL-165) ----

    #[test]
    fn no_period_predicate_leaves_the_field_unset() {
        let catalog = catalog_with_account(1_000);
        assert_eq!(
            bind("SELECT balance FROM account", &catalog)
                .unwrap()
                .period_filter,
            None
        );
    }

    #[test]
    fn each_predicate_binds_to_its_kind_and_intervals() {
        let catalog = catalog_with_account(1_000);
        let cases = [
            ("CONTAINS", PeriodPredicate::Contains),
            ("OVERLAPS", PeriodPredicate::Overlaps),
            ("EQUALS", PeriodPredicate::Equals),
            ("PRECEDES", PeriodPredicate::Precedes),
            ("SUCCEEDS", PeriodPredicate::Succeeds),
            ("MEETS", PeriodPredicate::ImmediatelyPrecedes),
            ("IMMEDIATELY PRECEDES", PeriodPredicate::ImmediatelyPrecedes),
            ("IMMEDIATELY SUCCEEDS", PeriodPredicate::ImmediatelySucceeds),
        ];
        for (kw, predicate) in cases {
            let sql =
                format!("SELECT balance FROM account WHERE PERIOD(10, 20) {kw} PERIOD(30, 40)");
            assert_eq!(
                bind(&sql, &catalog).unwrap().period_filter,
                Some(BoundPeriodPredicate {
                    left: const_period(10, 20),
                    predicate,
                    right: const_period(30, 40),
                }),
                "binding `{kw}`"
            );
        }
    }

    #[test]
    fn period_operands_fold_like_as_of_instants() {
        // `now()` and `now() ± interval` fold the same way an AS OF operand does.
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "SELECT balance FROM account \
             WHERE PERIOD(now() - interval '1 second', now()) OVERLAPS PERIOD(0, 1)",
            &catalog,
        )
        .unwrap()
        .period_filter
        .unwrap();
        assert_eq!(bound.left, const_period(NOW.0 - 1_000_000, NOW.0));
    }

    #[test]
    fn period_endpoints_bind_to_value_columns() {
        // STL-193: a `PERIOD(...)` endpoint may be a value column, bound to its
        // index against the resolved schema. `booking` has `vf`/`vt` TIMESTAMP
        // columns at indices 1 and 2.
        let catalog = catalog_with_booking(1_000);
        let bound = bind(
            "SELECT id FROM booking WHERE PERIOD(vf, vt) OVERLAPS PERIOD(0, 100)",
            &catalog,
        )
        .unwrap()
        .period_filter
        .unwrap();
        assert_eq!(
            bound,
            BoundPeriodPredicate {
                left: BoundPeriod {
                    from: PeriodEndpoint::Column(1),
                    to: PeriodEndpoint::Column(2),
                },
                predicate: PeriodPredicate::Overlaps,
                right: const_period(0, 100),
            }
        );
    }

    #[test]
    fn a_mixed_period_operand_binds_a_column_and_a_constant() {
        // One column endpoint, one constant endpoint — bound, not rejected. A
        // mixed operand is not range-checked at bind time (the column side is
        // only known per row).
        let catalog = catalog_with_booking(1_000);
        let bound = bind(
            "SELECT id FROM booking WHERE PERIOD(vf, 100) CONTAINS PERIOD(20, 30)",
            &catalog,
        )
        .unwrap()
        .period_filter
        .unwrap();
        assert_eq!(
            bound.left,
            BoundPeriod {
                from: PeriodEndpoint::Column(1),
                to: PeriodEndpoint::Const(100),
            }
        );
    }

    #[test]
    fn a_period_endpoint_on_a_non_instant_column_is_rejected() {
        // STL-193: only BIGINT/TIMESTAMP/TIMESTAMPTZ columns form a period
        // endpoint. `balance` is INT (too narrow for µs), so it is rejected with
        // its type rather than silently mis-scaled.
        let catalog = catalog_with_account(1_000);
        assert!(matches!(
            bind(
                "SELECT id FROM account WHERE PERIOD(balance, 20) CONTAINS PERIOD(12, 15)",
                &catalog,
            ),
            Err(SelectError::PeriodColumnType {
                ty: LogicalType::Int4,
                ..
            })
        ));
    }

    #[test]
    fn a_period_endpoint_on_an_unknown_column_is_rejected() {
        let catalog = catalog_with_booking(1_000);
        assert!(matches!(
            bind(
                "SELECT id FROM booking WHERE PERIOD(vf, missing) CONTAINS PERIOD(12, 15)",
                &catalog,
            ),
            Err(SelectError::UnknownColumn { column, .. }) if column == "missing"
        ));
    }

    #[test]
    fn a_period_predicate_leaves_the_equality_filter_unset() {
        // The two `WHERE` shapes are mutually exclusive: lifting the period
        // predicate strips the clause, so the equality binder sees no selection.
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "SELECT balance FROM account WHERE PERIOD(10, 20) CONTAINS PERIOD(12, 15)",
            &catalog,
        )
        .unwrap();
        assert_eq!(bound.filter, None);
        assert!(bound.period_filter.is_some());
    }

    #[test]
    fn a_reversed_period_operand_is_rejected() {
        let catalog = catalog_with_account(1_000);
        assert!(matches!(
            bind(
                "SELECT id FROM account WHERE PERIOD(20, 10) CONTAINS PERIOD(12, 15)",
                &catalog,
            ),
            Err(SelectError::PeriodInterval(IntervalError::EmptyOrReversed(
                20, 10
            )))
        ));
    }

    #[test]
    fn an_empty_period_operand_is_rejected() {
        let catalog = catalog_with_account(1_000);
        assert!(matches!(
            bind(
                "SELECT id FROM account WHERE PERIOD(10, 10) OVERLAPS PERIOD(12, 15)",
                &catalog,
            ),
            Err(SelectError::PeriodInterval(IntervalError::EmptyOrReversed(
                10, 10
            )))
        ));
    }

    #[test]
    fn an_unfoldable_period_operand_is_a_period_operand_error() {
        // A non-column endpoint that is not a foldable instant is rejected, not
        // guessed — here a calendar interval, which has no fixed µs length (the
        // same stance `AS OF` takes).
        let catalog = catalog_with_account(1_000);
        assert!(matches!(
            bind(
                "SELECT id FROM account \
                 WHERE PERIOD(now() + interval '1 month', now()) CONTAINS PERIOD(12, 15)",
                &catalog,
            ),
            Err(SelectError::PeriodOperand(_))
        ));
    }

    // ---- GROUP BY + aggregates (STL-171) ----

    /// `sales(id INT key, region TEXT, amount INT)` — a 3-column table to group on
    /// a value column and aggregate another.
    fn catalog_with_sales() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "sales",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("region", LogicalType::Text).expect("col"),
                    ColumnDef::new("amount", LogicalType::Int8).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create sales");
        catalog
    }

    #[test]
    fn a_plain_select_has_no_aggregate_plan() {
        let catalog = catalog_with_sales();
        assert_eq!(
            bind("SELECT region FROM sales", &catalog)
                .unwrap()
                .aggregate,
            None
        );
    }

    #[test]
    fn group_by_with_aggregates_binds() {
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(agg.group_by, vec![1]); // region is value column index 1
        assert_eq!(
            agg.aggregates,
            vec![
                AggregateCall {
                    func: AggregateFunc::Count,
                    arg: None
                },
                AggregateCall {
                    func: AggregateFunc::Sum,
                    arg: Some(2)
                },
            ]
        );
        assert_eq!(
            agg.items,
            vec![
                OutputItem::Group(0),
                OutputItem::Aggregate(0),
                OutputItem::Aggregate(1),
            ]
        );
        assert_eq!(
            agg.columns,
            vec![
                ("region".to_owned(), LogicalType::Text),
                ("count".to_owned(), LogicalType::Int8),
                ("sum".to_owned(), LogicalType::Int8),
            ]
        );
    }

    #[test]
    fn ungrouped_aggregate_binds_with_no_grouping_columns() {
        let catalog = catalog_with_sales();
        let agg = bind("SELECT COUNT(*), MAX(amount) FROM sales", &catalog)
            .unwrap()
            .aggregate
            .expect("aggregate plan");
        assert!(agg.group_by.is_empty());
        assert_eq!(
            agg.items,
            vec![OutputItem::Aggregate(0), OutputItem::Aggregate(1)]
        );
        // MAX's result type is its argument's type (int8 here).
        assert_eq!(
            agg.columns,
            vec![
                ("count".to_owned(), LogicalType::Int8),
                ("max".to_owned(), LogicalType::Int8),
            ]
        );
    }

    #[test]
    fn aliases_name_the_output_columns() {
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT region AS r, SUM(amount) AS total FROM sales GROUP BY region",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.columns,
            vec![
                ("r".to_owned(), LogicalType::Text),
                ("total".to_owned(), LogicalType::Int8),
            ]
        );
    }

    #[test]
    fn min_max_result_type_is_the_argument_type() {
        let catalog = catalog_with_sales();
        let agg = bind("SELECT MIN(region) FROM sales", &catalog)
            .unwrap()
            .aggregate
            .expect("aggregate plan");
        assert_eq!(agg.columns, vec![("min".to_owned(), LogicalType::Text)]);
    }

    #[test]
    fn grouping_with_no_aggregate_is_distinct() {
        // `SELECT region FROM sales GROUP BY region` is a valid (DISTINCT-like)
        // aggregate query — it has a GROUP BY even with no aggregate function.
        let catalog = catalog_with_sales();
        let agg = bind("SELECT region FROM sales GROUP BY region", &catalog)
            .unwrap()
            .aggregate
            .expect("aggregate plan");
        assert_eq!(agg.group_by, vec![1]);
        assert!(agg.aggregates.is_empty());
        assert_eq!(agg.items, vec![OutputItem::Group(0)]);
    }

    #[test]
    fn a_non_grouped_column_is_rejected() {
        // `region` is neither grouped nor aggregated — there is no single value
        // per group, so it is rejected (SQL's GROUP BY rule).
        let catalog = catalog_with_sales();
        assert_eq!(
            bind("SELECT region, SUM(amount) FROM sales", &catalog),
            Err(SelectError::UngroupedColumn {
                table: "sales".to_owned(),
                column: "region".to_owned(),
            })
        );
    }

    #[test]
    fn unsupported_aggregate_forms_are_rejected() {
        let catalog = catalog_with_sales();
        for sql in [
            "SELECT SUM(region) FROM sales GROUP BY id", // SUM of text
            "SELECT AVG(region) FROM sales GROUP BY id", // AVG of text
            "SELECT SUM(*) FROM sales",                  // SUM(*) invalid
            "SELECT COUNT(DISTINCT amount) FROM sales",  // DISTINCT
            "SELECT SUM(amount) OVER () FROM sales",     // window
            "SELECT lower(region) FROM sales GROUP BY region", // non-aggregate fn
            "SELECT * FROM sales GROUP BY region",       // wildcard with GROUP BY
            "SELECT SUM(amount + 1) FROM sales",         // non-column argument
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::UnsupportedAggregate(_))
                ),
                "expected UnsupportedAggregate for: {sql}"
            );
        }
    }

    #[test]
    fn count_all_is_accepted_as_the_default() {
        // `ALL` is the default duplicate treatment (`COUNT(ALL col)` == `COUNT(col)`),
        // so it binds; only `DISTINCT` is rejected.
        let catalog = catalog_with_sales();
        let agg = bind("SELECT COUNT(ALL amount) FROM sales", &catalog)
            .unwrap()
            .aggregate
            .expect("aggregate plan");
        assert_eq!(
            agg.aggregates,
            vec![AggregateCall {
                func: AggregateFunc::Count,
                arg: Some(2),
            }]
        );
    }

    #[test]
    fn group_by_on_an_unsupported_type_is_rejected() {
        // `vf` is a TIMESTAMP — outside the evaluator's scalar set, so grouping on
        // it is rejected rather than mis-decoded.
        let catalog = catalog_with_booking(1_000);
        assert!(matches!(
            bind("SELECT COUNT(*) FROM booking GROUP BY vf", &catalog),
            Err(SelectError::UnsupportedAggregate(_))
        ));
    }

    #[test]
    fn an_unknown_aggregate_or_group_column_is_rejected() {
        let catalog = catalog_with_sales();
        assert_eq!(
            bind("SELECT SUM(nope) FROM sales", &catalog),
            Err(SelectError::UnknownColumn {
                table: "sales".to_owned(),
                column: "nope".to_owned(),
            })
        );
        assert_eq!(
            bind("SELECT COUNT(*) FROM sales GROUP BY nope", &catalog),
            Err(SelectError::UnknownColumn {
                table: "sales".to_owned(),
                column: "nope".to_owned(),
            })
        );
    }

    #[test]
    fn where_filters_rows_before_grouping() {
        // A WHERE on an aggregate query still binds (it filters pre-aggregation).
        let catalog = catalog_with_sales();
        let bound = bind(
            "SELECT region, COUNT(*) FROM sales WHERE id = 1 GROUP BY region",
            &catalog,
        )
        .unwrap();
        assert!(bound.aggregate.is_some());
        assert_eq!(
            bound.filter,
            Some(BoundPredicate {
                column: "id".to_owned(),
                column_index: 0,
                value: ScalarValue::Int4(1),
            })
        );
    }

    // ---- joins (STL-172) ----

    /// A catalog with two joinable tables: `users (id INT, name TEXT)` and
    /// `orders (oid INT, uid INT)`, joinable on `users.id = orders.uid` (both INT).
    fn catalog_with_join_tables() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("name", LogicalType::Text).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create users");
        catalog
            .create_table(
                "orders",
                vec![
                    ColumnDef::new("oid", LogicalType::Int4).expect("col"),
                    ColumnDef::new("uid", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create orders");
        catalog
    }

    fn join_of(sql: &str, catalog: &Catalog) -> BoundJoin {
        bind(sql, catalog)
            .expect("bind join")
            .join
            .expect("join plan")
    }

    #[test]
    fn inner_join_binds_keys_output_and_header() {
        let catalog = catalog_with_join_tables();
        let join = join_of(
            "SELECT * FROM users JOIN orders ON users.id = orders.uid",
            &catalog,
        );
        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(join.left.table, "users");
        assert_eq!(join.right.table, "orders");
        // users.id is index 0; orders.uid is index 1.
        assert_eq!((join.left_key, join.right_key), (0, 1));
        // `SELECT *` over an inner join = the left's columns then the right's.
        assert_eq!(
            join.output,
            vec![
                JoinColumnRef::Left(0),
                JoinColumnRef::Left(1),
                JoinColumnRef::Right(0),
                JoinColumnRef::Right(1),
            ]
        );
        assert_eq!(
            join.columns,
            vec![
                ("id".to_owned(), LogicalType::Int4),
                ("name".to_owned(), LogicalType::Text),
                ("oid".to_owned(), LogicalType::Int4),
                ("uid".to_owned(), LogicalType::Int4),
            ]
        );
    }

    #[test]
    fn each_join_keyword_binds_to_its_type() {
        let catalog = catalog_with_join_tables();
        let cases = [
            ("JOIN", JoinType::Inner),
            ("INNER JOIN", JoinType::Inner),
            ("LEFT JOIN", JoinType::Left),
            ("LEFT OUTER JOIN", JoinType::Left),
            ("SEMI JOIN", JoinType::Semi),
            ("LEFT SEMI JOIN", JoinType::Semi),
            ("ANTI JOIN", JoinType::Anti),
            ("LEFT ANTI JOIN", JoinType::Anti),
        ];
        for (kw, want) in cases {
            let sql = format!("SELECT users.id FROM users {kw} orders ON users.id = orders.uid");
            assert_eq!(join_of(&sql, &catalog).join_type, want, "{kw}");
        }
    }

    #[test]
    fn semi_and_anti_project_only_the_left_side() {
        let catalog = catalog_with_join_tables();
        for kw in ["SEMI JOIN", "ANTI JOIN"] {
            let sql = format!("SELECT * FROM users {kw} orders ON users.id = orders.uid");
            let join = join_of(&sql, &catalog);
            assert_eq!(
                join.output,
                vec![JoinColumnRef::Left(0), JoinColumnRef::Left(1)],
                "{kw}"
            );
            assert_eq!(
                join.columns,
                vec![
                    ("id".to_owned(), LogicalType::Int4),
                    ("name".to_owned(), LogicalType::Text),
                ],
                "{kw}"
            );
        }
    }

    #[test]
    fn the_on_equality_binds_in_either_order() {
        let catalog = catalog_with_join_tables();
        for on in ["users.id = orders.uid", "orders.uid = users.id"] {
            let sql = format!("SELECT users.id FROM users JOIN orders ON {on}");
            let join = join_of(&sql, &catalog);
            assert_eq!((join.left_key, join.right_key), (0, 1), "{on}");
        }
    }

    #[test]
    fn table_aliases_and_bare_columns_resolve() {
        let catalog = catalog_with_join_tables();
        // Aliased, qualified projection.
        let aliased = join_of(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid",
            &catalog,
        );
        assert_eq!(
            aliased.output,
            vec![JoinColumnRef::Left(1), JoinColumnRef::Right(0)]
        );
        assert_eq!(
            aliased.columns,
            vec![
                ("name".to_owned(), LogicalType::Text),
                ("oid".to_owned(), LogicalType::Int4),
            ]
        );
        // Unqualified columns unique to one side resolve without a qualifier.
        let bare = join_of(
            "SELECT name, oid FROM users JOIN orders ON id = uid",
            &catalog,
        );
        assert_eq!((bare.left_key, bare.right_key), (0, 1));
        assert_eq!(
            bare.output,
            vec![JoinColumnRef::Left(1), JoinColumnRef::Right(0)]
        );
    }

    #[test]
    fn unsupported_join_operators_are_rejected() {
        let catalog = catalog_with_join_tables();
        for sql in [
            "SELECT users.id FROM users RIGHT JOIN orders ON users.id = orders.uid",
            "SELECT users.id FROM users FULL OUTER JOIN orders ON users.id = orders.uid",
            "SELECT users.id FROM users CROSS JOIN orders",
        ] {
            assert!(
                matches!(bind(sql, &catalog), Err(SelectError::UnsupportedJoin(_))),
                "expected UnsupportedJoin for: {sql}"
            );
        }
    }

    #[test]
    fn an_n_way_join_is_rejected() {
        let catalog = catalog_with_join_tables();
        let sql = "SELECT users.id FROM users \
                   JOIN orders ON users.id = orders.uid \
                   JOIN orders o2 ON users.id = o2.uid";
        assert!(matches!(
            bind(sql, &catalog),
            Err(SelectError::UnsupportedJoin(_))
        ));
    }

    #[test]
    fn clauses_over_a_join_are_rejected_not_dropped() {
        let catalog = catalog_with_join_tables();
        for sql in [
            // A WHERE over the join (a follow-up) — dropping it would over-return.
            "SELECT users.id FROM users JOIN orders ON users.id = orders.uid WHERE users.id = 1",
            // An aggregate over the join.
            "SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.uid",
        ] {
            assert!(
                matches!(bind(sql, &catalog), Err(SelectError::UnsupportedJoin(_))),
                "expected UnsupportedJoin for: {sql}"
            );
        }
    }

    #[test]
    fn unsupported_join_conditions_are_rejected() {
        let catalog = catalog_with_join_tables();
        for sql in [
            // Non-equality.
            "SELECT users.id FROM users JOIN orders ON users.id > orders.uid",
            // Equality that does not span both tables.
            "SELECT users.id FROM users JOIN orders ON users.id = users.id",
            // USING / NATURAL are not lowered.
            "SELECT users.id FROM users JOIN orders USING (id)",
            "SELECT users.id FROM users NATURAL JOIN orders",
        ] {
            assert!(
                matches!(bind(sql, &catalog), Err(SelectError::JoinCondition(_))),
                "expected JoinCondition for: {sql}"
            );
        }
    }

    #[test]
    fn mismatched_join_key_types_are_rejected() {
        // users.name is TEXT, orders.uid is INT — they can never compare equal.
        let catalog = catalog_with_join_tables();
        assert!(matches!(
            bind(
                "SELECT users.id FROM users JOIN orders ON users.name = orders.uid",
                &catalog,
            ),
            Err(SelectError::JoinColumnTypeMismatch { .. })
        ));
    }

    #[test]
    fn unknown_join_columns_are_rejected() {
        let catalog = catalog_with_join_tables();
        // Unknown column in the ON.
        assert!(matches!(
            bind(
                "SELECT users.id FROM users JOIN orders ON users.id = orders.nope",
                &catalog,
            ),
            Err(SelectError::UnknownJoinColumn { .. })
        ));
        // Unknown column in the projection.
        assert!(matches!(
            bind(
                "SELECT nope FROM users JOIN orders ON users.id = orders.uid",
                &catalog,
            ),
            Err(SelectError::UnknownJoinColumn { .. })
        ));
    }

    #[test]
    fn a_semi_join_cannot_project_a_right_column() {
        let catalog = catalog_with_join_tables();
        assert!(matches!(
            bind(
                "SELECT orders.oid FROM users SEMI JOIN orders ON users.id = orders.uid",
                &catalog,
            ),
            Err(SelectError::UnsupportedJoinProjection(_))
        ));
    }

    #[test]
    fn a_column_in_both_tables_is_ambiguous_unless_qualified() {
        // Two tables that share an `id` column.
        let mut catalog = Catalog::new();
        for table in ["a", "b"] {
            catalog
                .create_table(
                    table,
                    vec![
                        ColumnDef::new("id", LogicalType::Int4).expect("col"),
                        ColumnDef::new("v", LogicalType::Int4).expect("col"),
                    ],
                    TableTemporal::system_only(),
                    SystemTimeMicros(1_000),
                )
                .expect("create");
        }
        // Bare `id` is ambiguous in the ON…
        assert!(matches!(
            bind("SELECT a.v FROM a JOIN b ON id = id", &catalog),
            Err(SelectError::AmbiguousColumn { .. })
        ));
        // …and in the projection.
        assert!(matches!(
            bind("SELECT id FROM a JOIN b ON a.id = b.id", &catalog),
            Err(SelectError::AmbiguousColumn { .. })
        ));
        // Qualifying resolves it.
        let join = join_of("SELECT a.id FROM a JOIN b ON a.id = b.id", &catalog);
        assert_eq!(join.output, vec![JoinColumnRef::Left(0)]);
    }
}
