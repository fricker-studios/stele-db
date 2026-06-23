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
//! (pgwire, [STL-104]). The result-shaping clauses — `ORDER BY` (bare columns,
//! multi-key), `LIMIT`/`OFFSET`/`FETCH` (non-negative integer literals), and
//! `SELECT DISTINCT` — bind onto [`BoundSelect`] (STL-263); the executor
//! applies them after filtering/aggregation in the Postgres pipeline order.
//! Absolute `TIMESTAMP '…'` / `DATE '…'` literals in an `AS OF` are not folded
//! yet (no civil-time codec); they surface [`AsOfError::Unsupported`] rather
//! than a wrong instant.

use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SetExpr, Statement as SqlStatement, TableFactor,
    TableWithJoins, Value, WildcardAdditionalOptions,
};
use stele_catalog::{Catalog, ColumnDef, SchemaId, TableSchema};
use stele_common::period::{Interval, IntervalError, PeriodPredicate};
use stele_common::provenance;
use stele_common::time::{SystemTimeMicros, ValidTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};

use crate::ast::{
    AsOf, PeriodExpr, PeriodPredicateClause, Statement, StatementBody, Temporal, TimeDimension,
};
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
    /// `SELECT a, b AS x, a + 1 AS p, (SELECT max(b) FROM s), …` — one
    /// [`ProjectionItem`] per select-list entry, in projection order ([STL-303]).
    ///
    /// [STL-303]: https://allegromusic.atlassian.net/browse/STL-303
    Items(Vec<ProjectionItem>),
}

impl Projection {
    /// Whether every projected item is a plain addressable column (or `*`) — the
    /// fast path the engine projects by gathering cells, with no per-row expression
    /// evaluation ([STL-303]). A computed expression or scalar subquery item makes
    /// this `false`, routing the read to the materialized-projection path.
    #[must_use]
    pub fn is_all_columns(&self) -> bool {
        match self {
            Self::All => true,
            Self::Items(items) => items
                .iter()
                .all(|item| matches!(item.value, ProjectionValue::Column(_))),
        }
    }
}

/// One select-list entry of a [`Projection::Items`] list ([STL-303]): the output
/// column name and the value the entry projects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionItem {
    /// The output column name on the wire: an explicit `AS` alias, a bare column's
    /// own name, an unaliased scalar subquery's inherited inner-column name (the
    /// Postgres rule), or the `?column?` fallback for an unaliased computed
    /// expression.
    pub name: String,
    /// What this item projects.
    pub value: ProjectionValue,
}

impl ProjectionItem {
    /// A bare addressable column projected under its own name — the common case,
    /// and the shape the engine's internal point-read plans build.
    #[must_use]
    pub fn column(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            value: ProjectionValue::Column(name),
        }
    }
}

/// What a [`ProjectionItem`] projects ([STL-303]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionValue {
    /// A bare addressable column (a schema column or a provenance pseudo-column),
    /// by its **source** name — resolved to an addressable index at execution. The
    /// output name may differ (an `AS` alias) and is carried by
    /// [`ProjectionItem::name`].
    Column(String),
    /// A computed scalar expression over the row, with its resolved result type.
    /// Reuses the `WHERE` [`BoundScalar`] vocabulary — integer arithmetic over any
    /// number of columns (`a + b`), folded literals, column-free arithmetic
    /// (`1 + 2`), and an embedded uncorrelated scalar subquery operand
    /// (`a + (SELECT max(b) FROM s)`, [STL-332]); the engine evaluates it per row
    /// with the same `eval_expr` the filter uses, after resolving any embedded
    /// subquery to a constant.
    Computed {
        /// The scalar to evaluate per row.
        scalar: BoundScalar,
        /// The expression's result type — the output column's wire type.
        ty: LogicalType,
    },
    /// A scalar subquery projected in the SELECT list ([STL-303], [STL-331]): its
    /// single value becomes the output cell — no inner row is SQL `NULL`, more than
    /// one is SQLSTATE `21000`.
    ///
    /// When the `correlation` field is `None` the subquery is
    /// **uncorrelated** ([STL-303]): the inner references no outer column, so it is
    /// resolved **once** at the statement snapshot and broadcast as a constant
    /// column — the projection-path analogue of the [STL-234] uncorrelated `WHERE`
    /// fold, materializing a single value instead of a row filter. When `Some` it
    /// is **correlated** ([STL-331]): the inner
    /// references an outer column, so it is re-run once per outer row with that
    /// row's value substituted — the [STL-239] per-row machinery producing a
    /// projected cell instead of a row keep/drop.
    Subquery {
        /// The bound inner query (capped at two rows for the cardinality check). For
        /// a correlated inner its single-comparison `WHERE` was lifted off at bind
        /// time (its [`filter`](BoundSelect::filter) is `None`) and is re-applied per
        /// outer row by the executor; see the `correlation` field.
        subquery: Box<BoundSelect>,
        /// The inner's sole output column type — the output column's wire type.
        ty: LogicalType,
        /// The outer-column correlation the inner references, or `None` when the
        /// subquery is uncorrelated ([STL-303]). `Some` marks the per-row
        /// re-execution path ([STL-331]): the engine substitutes each outer row's
        /// [`outer_column`](Correlation::outer_column) value into the inner's filter
        /// and re-runs it, reducing each result to the projected cell, instead of
        /// broadcasting one once-resolved value.
        correlation: Option<Correlation>,
    },
}

/// One bound `ORDER BY` key: which column to sort on and in which direction
/// ([STL-263]).
///
/// NULL placement is pinned to the Postgres defaults — NULLS LAST under `ASC`,
/// NULLS FIRST under `DESC` (a NULL sorts as if larger than every value). An
/// explicit `NULLS FIRST`/`NULLS LAST` override is not bound yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundSortKey {
    /// The column the key sorts on.
    pub column: SortTarget,
    /// `true` for `DESC`; `false` for `ASC` (the default).
    pub descending: bool,
}

/// Where a bound `ORDER BY` column lives ([STL-263]).
///
/// An `ORDER BY` name resolves against the **select list first** (Postgres
/// output-column semantics); a name not projected falls back to the table's
/// schema — legal for a plain `SELECT` (Postgres sorts on non-projected columns)
/// but rejected for `SELECT DISTINCT` ([`SelectError::DistinctOrderBy`], the
/// 42P10 rule) and for an aggregate query (whose rows have no schema columns).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortTarget {
    /// A select-list column, by **output position**. For an aggregate query this
    /// indexes [`BoundAggregate::columns`]; otherwise the projected columns.
    Output(usize),
    /// A schema column not in the select list, by **schema index** (`0` the
    /// business key). Only produced for a plain, non-`DISTINCT` projection.
    Schema(usize),
}

/// A comparison operator a `WHERE` predicate lowers ([STL-213]).
///
/// Mirrors the executor's `stele_exec::CmpOp`; the engine maps between them when
/// it lowers the bound plan (the two crates do not depend on each other — the same
/// split [`AggregateFunc`] / [`JoinType`] draw).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// `=` — equal.
    Eq,
    /// `<>` / `!=` — not equal.
    Ne,
    /// `<` — strictly less.
    Lt,
    /// `<=` — less or equal.
    Le,
    /// `>` — strictly greater.
    Gt,
    /// `>=` — greater or equal.
    Ge,
}

impl CompareOp {
    /// The operator with its operands swapped: `lit < col` is `col > lit`.
    /// Lets [`BoundPredicate::column_comparison`] normalize a literal-first
    /// comparison so the column always reads as the left operand ([STL-237]).
    ///
    /// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
    #[must_use]
    pub const fn mirror(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
        }
    }
}

/// An integer arithmetic operator a `WHERE` scalar lowers ([STL-213]).
///
/// Mirrors the executor's `stele_exec::ArithOp` (same crate-split reason as
/// [`CompareOp`]). `/` and `%` divide-by-zero to a NULL cell in the evaluator, so
/// a `WHERE` over them is total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    /// `+` — addition.
    Add,
    /// `-` — subtraction.
    Sub,
    /// `*` — multiplication.
    Mul,
    /// `/` — truncating integer division.
    Div,
    /// `%` — remainder (sign follows the dividend).
    Mod,
}

/// One side of a bound `WHERE` comparison or a computed select item ([STL-213]).
///
/// A value column, a folded literal, an integer arithmetic combination of them, or
/// — only inside a computed projection ([STL-332]) — an embedded scalar subquery
/// resolved to a constant operand.
///
/// Columns are referenced by **schema index** (`0` is the business key, the rest
/// value columns) — the same positional convention [`BoundPredicate`] and
/// [`BoundAggregate`] use. The executor lowers this straight to a vectorized
/// `stele_exec::Expr` over the reconstructed row, after resolving any [`Subquery`]
/// operand to a literal.
///
/// [`Subquery`]: BoundScalar::Subquery
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundScalar {
    /// The value column at this schema index.
    Column(usize),
    /// A constant, folded to the predicate's anchor-column type.
    Literal(ScalarValue),
    /// `left <op> right` — integer arithmetic over two scalars.
    Arith {
        /// The arithmetic operator.
        op: ArithOp,
        /// The left operand.
        left: Box<BoundScalar>,
        /// The right operand.
        right: Box<BoundScalar>,
    },
    /// An **uncorrelated** scalar subquery used as an operand inside a computed
    /// select item ([STL-332]): `a + (SELECT max(b) FROM s)`. The engine resolves it
    /// **once** at the statement snapshot ([STL-303] `resolve_scalar_subquery`) and
    /// substitutes its single value as a constant before lowering the surrounding
    /// arithmetic — no inner row ⇒ the whole expression is SQL `NULL` (NULL
    /// propagates through arithmetic), more than one ⇒ SQLSTATE `21000`. Only ever
    /// produced by the projection binder; a `WHERE` [`BoundScalar`] never carries
    /// one (subquery `WHERE`s ride [`BoundSubqueryFilter`]).
    Subquery(Box<BoundSelect>),
}

/// A bound `WHERE <scalar> <compare> <scalar>` predicate ([STL-151], [STL-213]).
///
/// v0.2 lowers a single comparison over exactly one column — `<column> = <literal>`
/// to start with ([STL-151]), now any of the six [comparison operators](CompareOp)
/// with either side an integer arithmetic expression of that column ([STL-213],
/// e.g. `qty % 2 = 0`, `price > 100`). The column anchors the literal folding, so
/// both sides share its type and the executor's typed comparison is well-formed.
///
/// The executor applies it after resolving the row's cells, and pushes it down to
/// segment zone-map pruning when it is a business-key equality
/// ([`key_equality`](Self::key_equality) — the only shape a zone map can reason
/// about). Column-to-column comparisons, `AND`/`OR` chains, and `BETWEEN`/`IN`
/// remain deferred follow-ups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundPredicate {
    /// The comparison's left operand.
    pub left: BoundScalar,
    /// The comparison operator.
    pub op: CompareOp,
    /// The comparison's right operand.
    pub right: BoundScalar,
}

impl BoundPredicate {
    /// The literal this predicate equates the **business key** (schema column `0`)
    /// to, if it is exactly `<key> = <literal>` (in either operand order) — the one
    /// shape the scan pushes down to zone-map pruning. `None` for every richer
    /// predicate (a non-equality, a value-column compare, an arithmetic side),
    /// which the vectorized filter still applies exactly.
    #[must_use]
    pub const fn key_equality(&self) -> Option<&ScalarValue> {
        if !matches!(self.op, CompareOp::Eq) {
            return None;
        }
        match (&self.left, &self.right) {
            (BoundScalar::Column(0), BoundScalar::Literal(value))
            | (BoundScalar::Literal(value), BoundScalar::Column(0)) => Some(value),
            _ => None,
        }
    }

    /// The `(schema index, literal)` this predicate equates a **value column**
    /// to, if it is exactly `<column> = <literal>` (in either operand order)
    /// over a non-key column — the shape a secondary-index equality probe can
    /// serve ([STL-233]). `None` for a business-key equality (which has its own
    /// zone-map push-down, [`key_equality`](Self::key_equality)) and for every
    /// richer predicate, which the vectorized filter still applies exactly.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    #[must_use]
    pub const fn column_equality(&self) -> Option<(usize, &ScalarValue)> {
        match self.column_comparison() {
            Some((col, CompareOp::Eq, value)) => Some((col, value)),
            _ => None,
        }
    }

    /// The `(schema index, operator, literal)` of a bare **value-column**
    /// comparison — exactly `<column> <cmp> <literal>` in either operand order,
    /// normalized so the column reads as the left operand (a literal-first form
    /// [mirrors](CompareOp::mirror) the operator) — the shape a secondary-index
    /// equality or range probe can serve ([STL-237]). `None` for a business-key
    /// comparison (key *equality* has its own zone-map push-down,
    /// [`key_equality`](Self::key_equality)) and for every richer predicate (an
    /// arithmetic side, column-to-column), which the vectorized filter still
    /// applies exactly.
    ///
    /// `Ne` is reported as itself: whether an operator can window candidates is
    /// the index's call, not the binder's.
    ///
    /// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
    #[must_use]
    pub const fn column_comparison(&self) -> Option<(usize, CompareOp, &ScalarValue)> {
        match (&self.left, &self.right) {
            (BoundScalar::Column(col), BoundScalar::Literal(value)) if *col > 0 => {
                Some((*col, self.op, value))
            }
            (BoundScalar::Literal(value), BoundScalar::Column(col)) if *col > 0 => {
                Some((*col, self.op.mirror(), value))
            }
            _ => None,
        }
    }
}

/// A bound `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` range ([STL-244]).
///
/// Both endpoints are folded to concrete microsecond instants the same way an
/// `AS OF` operand is ([`resolve_as_of`]). [`closed_upper`](Self::closed_upper)
/// distinguishes the half-open `FROM a TO b` (the range `[from, to)`) from the
/// closed `BETWEEN a AND b` (`[from, to]`, the SQL:2011 convention) — the only
/// difference between the two spellings, and the source of the "off-by-one on a
/// half-open interval" boundary class the oracle pins (docs/16 §4). A scan
/// returns every version whose system interval `[sys_from, sys_to)` overlaps this
/// range; the binder has already proved the range is non-empty
/// (`from < to`, or `from <= to` when closed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemTimeRange {
    /// The inclusive lower bound, in system-time microseconds.
    pub from: SystemTimeMicros,
    /// The upper bound, in system-time microseconds — exclusive when
    /// [`closed_upper`](Self::closed_upper) is `false`, inclusive when `true`.
    pub to: SystemTimeMicros,
    /// `true` for the closed `BETWEEN` (upper inclusive), `false` for the
    /// half-open `FROM..TO` (upper exclusive).
    pub closed_upper: bool,
}

/// A bound `FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` range ([STL-328]) —
/// the valid-axis mirror of [`SystemTimeRange`].
///
/// Both endpoints fold to concrete microsecond instants exactly as a
/// [`SystemTimeRange`]'s do (the `now`-relative [`resolve_as_of`] fold). A scan
/// returns every version *system-live at the statement snapshot* whose valid
/// interval `[valid_from, valid_to)` overlaps this range — many versions over the
/// valid axis at one system instant — and appends the valid period endpoints
/// (`valid_from`, `valid_to`) after the projected columns. Endpoints are typed
/// [`ValidTimeMicros`] — the valid axis's own µs/UTC newtype ([ADR-0024]) — so a
/// valid bound can never be mixed with a system-time one (the executor's valid
/// APIs use the same newtype); the binder has already proved the range is
/// non-empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidTimeRange {
    /// The inclusive lower bound, in valid-time microseconds.
    pub from: ValidTimeMicros,
    /// The upper bound, in valid-time microseconds — exclusive when
    /// [`closed_upper`](Self::closed_upper) is `false`, inclusive when `true`.
    pub to: ValidTimeMicros,
    /// `true` for the closed `BETWEEN` (upper inclusive), `false` for the
    /// half-open `FROM..TO` (upper exclusive).
    pub closed_upper: bool,
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
    /// A bound `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` range
    /// ([STL-244]), or `None` for a point-in-time / plain read. When `Some`, the
    /// query is a **range scan**: the executor returns every version whose system
    /// interval `[sys_from, sys_to)` overlaps the range — not just the one live at
    /// [`snapshot`](Self::snapshot) — exposing the period endpoints (`sys_from`,
    /// `sys_to`) as addressable columns after the table's own ([STL-329]). Result
    /// shaping, aggregation, and projected provenance pseudo-columns compose over it;
    /// mutually exclusive with an `AS OF` point read, and the binder still rejects a
    /// range combined with a `JOIN`, a subquery / period `WHERE`, a computed
    /// projection, or a CTE / derived source (each a tracked follow-up).
    pub system_range: Option<SystemTimeRange>,
    /// A bound `FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` range ([STL-328]),
    /// or `None`. When `Some`, the query is a **valid-time range scan**: the
    /// executor returns every version system-live at [`snapshot`](Self::snapshot)
    /// whose valid interval `[valid_from, valid_to)` overlaps the range, appending
    /// the valid period endpoints (`valid_from`, `valid_to`) after the projected
    /// columns. Requires a valid-time table; mutually exclusive with a
    /// [`system_range`](Self::system_range) and with a `FOR VALID_TIME AS OF`
    /// [`valid_snapshot`](Self::valid_snapshot) (a system `AS OF` pin is allowed and
    /// sets [`snapshot`](Self::snapshot)). The same SELECT surface composes over it
    /// as the system range — shaping, aggregation, projected provenance ([STL-329]) —
    /// and the binder rejects the same residual shapes (subquery / period `WHERE`,
    /// `JOIN`, computed projection, CTE / derived source).
    pub valid_range: Option<ValidTimeRange>,
    /// The columns the query projects.
    pub projection: Projection,
    /// The lowered `WHERE` predicate, or `None` for an unfiltered read. v0.2
    /// lowers a single comparison over one column — `<column> = <literal>`
    /// ([STL-151]) through any comparison operator with an integer-arithmetic side
    /// ([STL-213]).
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
    /// A bound **uncorrelated subquery** `WHERE` — a scalar comparison,
    /// `[NOT] IN`, or `[NOT] EXISTS` against a once-evaluated inner query
    /// ([STL-234]). Mutually exclusive with [`filter`](Self::filter) and
    /// [`period_filter`](Self::period_filter) — a `WHERE` is exactly one of the
    /// three shapes. The executor runs the inner query once at *this* plan's
    /// snapshot (docs/16 §6) and folds its result into the row filter.
    pub subquery_filter: Option<BoundSubqueryFilter>,
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
    /// The non-recursive common-table-expressions (and lowered derived tables)
    /// this query must materialize before it runs ([STL-242]), in declaration
    /// order — a later one may reference an earlier one. Empty for a query with no
    /// `WITH` clause and no `FROM (SELECT …)` derived table. The executor runs each
    /// once at this statement's snapshot, materializes its rows, and resolves a
    /// reference to one (by name, in [`table`](Self::table) or a
    /// [`BoundJoinSide::table`]) from that materialization rather than from storage.
    ///
    /// [STL-242]: https://allegromusic.atlassian.net/browse/STL-242
    pub ctes: Vec<BoundCte>,
    /// When the query's `FROM` is a **materialized relation** — a CTE reference or
    /// a derived table ([STL-242]) — its resolved output columns `(name, type)`, so
    /// the executor reads the relation's shape from here and its rows from the
    /// matching entry in [`ctes`](Self::ctes) (a query-local name, not a
    /// catalog-registered table), instead of scanning storage. `None` for an
    /// ordinary base-table read, and on the join path (whose sides carry their own
    /// columns). [`table`](Self::table) holds the relation's name in either case.
    pub relation_columns: Option<Vec<(String, LogicalType)>>,
    /// `SELECT DISTINCT` — deduplicate the projected rows ([STL-263]). Applies
    /// over the **full projected row** (aggregate output for an aggregate
    /// query); `DISTINCT ON (…)` is not bound. Two rows are duplicates iff every
    /// projected cell matches, NULLs equal (the `GROUP BY` rule, not `=`).
    pub distinct: bool,
    /// The bound `ORDER BY` keys, first key outermost; empty when the query
    /// gave none ([STL-263]). The executor sorts **after** `DISTINCT` and
    /// before `OFFSET`/`LIMIT` (the Postgres pipeline).
    pub order_by: Vec<BoundSortKey>,
    /// Rows to skip before returning any (`OFFSET m`), `0` when absent
    /// ([STL-263]). An offset past the end yields an empty result, not an error.
    pub offset: u64,
    /// Maximum rows to return (`LIMIT n` / `FETCH FIRST n ROWS ONLY`), or
    /// `None` for unlimited (including the explicit `LIMIT ALL`) ([STL-263]).
    /// `LIMIT 0` is a valid empty read. Applied after `ORDER BY` and `OFFSET`.
    pub limit: Option<u64>,
}

impl BoundSelect {
    /// Whether this is a **plain single base-table scan**: no aggregate, join,
    /// subquery, CTE / derived relation, period or system-range read, `DISTINCT`,
    /// `ORDER BY`, or `LIMIT` / `OFFSET`, and no residual `WHERE` ([STL-317]).
    ///
    /// Gates semi / anti decorrelation of a correlated `EXISTS`
    /// ([`BoundSubqueryFilter::semi_anti_decorrelation`]): only such an inner has
    /// row-*presence* equal to "∃ a row whose correlation key equals the outer
    /// key", so only it may fold onto a key-set join. Any of these clauses can
    /// change whether the inner yields a row — an aggregate inner always returns
    /// one, a `LIMIT 0` or empty-overshooting `OFFSET` returns none — so each forces
    /// the [STL-239] per-row fallback rather than risk a wrong answer.
    pub(crate) const fn is_plain_scan(&self) -> bool {
        self.filter.is_none()
            && self.period_filter.is_none()
            && self.aggregate.is_none()
            && self.subquery_filter.is_none()
            && self.join.is_none()
            && self.system_range.is_none()
            && self.valid_range.is_none()
            && self.relation_columns.is_none()
            && self.ctes.is_empty()
            && !self.distinct
            && self.order_by.is_empty()
            && self.offset == 0
            && self.limit.is_none()
    }

    /// The names of the period-endpoint columns a range scan appends after the
    /// table's own columns, or `None` for a non-range read ([STL-244], [STL-328]).
    ///
    /// A system range exposes `(sys_from, sys_to)`, a valid range
    /// `(valid_from, valid_to)`; both are `TIMESTAMPTZ` with a `NULL` upper for an
    /// open-ended period. These are *addressable* columns ([STL-329]): the
    /// projection, `ORDER BY`, `GROUP BY`, and aggregate clauses resolve them by
    /// name, the binder appends them to the schema it binds those clauses against,
    /// and the engine names them identically in the executed result and the
    /// statement `Describe`.
    #[must_use]
    pub const fn range_endpoint_names(&self) -> Option<(&'static str, &'static str)> {
        range_endpoint_names(self.system_range.is_some(), self.valid_range.is_some())
    }
}

/// The period-endpoint column names for a range scan over the given axis, or
/// `None` when neither axis ranges ([STL-329]). Shared by [`BoundSelect`] and the
/// binder's range-schema construction so both agree on `(sys_from, sys_to)` /
/// `(valid_from, valid_to)`.
const fn range_endpoint_names(system: bool, valid: bool) -> Option<(&'static str, &'static str)> {
    if system {
        Some(("sys_from", "sys_to"))
    } else if valid {
        Some(("valid_from", "valid_to"))
    } else {
        None
    }
}

/// Build the schema a **range scan** binds its projection, result-shaping,
/// aggregate, and `WHERE` clauses against ([STL-329]): the table's own columns
/// followed by the two period endpoints the read appends (`sys_from`/`sys_to`, or
/// `valid_from`/`valid_to`). With the endpoints in the bound schema, `SELECT *`
/// expands to include them, and `ORDER BY sys_from` / a named projection /
/// `GROUP BY` resolve them — the engine appends them at the matching positions.
/// Returns `None` for a non-range read, which binds against the catalog schema
/// unchanged.
///
/// The endpoints are `TIMESTAMPTZ`, appended in `(from, to)` order. The provenance
/// pseudo-columns are deliberately *not* part of this schema: they stay virtual,
/// resolved past it exactly as on a point read ([STL-247]), so a projected
/// `_stele_txn_id` lands after the endpoints in the engine's addressable set.
///
/// # Errors
///
/// [`SelectError::UnsupportedSystemRange`] / [`SelectError::UnsupportedValidRange`]
/// if the table already has a column named like an appended endpoint — the output
/// name would be ambiguous, so the range read is rejected rather than silently
/// shadowing the user's column.
fn range_effective_schema(
    schema: &TableSchema,
    system_range: bool,
    valid_range: bool,
) -> Result<Option<TableSchema>, SelectError> {
    let Some((from_name, to_name)) = range_endpoint_names(system_range, valid_range) else {
        return Ok(None);
    };
    let mut columns: Vec<ColumnDef> = schema.columns().to_vec();
    let endpoint = |name: &str| {
        ColumnDef::new(name, LogicalType::TimestampTz).expect("a static endpoint name is non-empty")
    };
    columns.push(endpoint(from_name));
    columns.push(endpoint(to_name));
    TableSchema::ephemeral(columns).map(Some).map_err(|_| {
        let what = "over a table with a column named like the appended endpoint".to_owned();
        if valid_range {
            SelectError::UnsupportedValidRange(what)
        } else {
            SelectError::UnsupportedSystemRange(what)
        }
    })
}

/// A bound `WHERE` clause whose predicate is a subquery — a scalar comparison,
/// `[NOT] IN`, or `[NOT] EXISTS` ([STL-234], [STL-239]).
///
/// When [`correlation`](Self::correlation) is `None` the subquery is
/// **uncorrelated** ([STL-234]): the inner references no outer column, so its
/// result is a **constant** with respect to the outer rows. The executor evaluates
/// [`subquery`](Self::subquery) **once**, at the outer statement's `(sys, valid)`
/// snapshot (the binder binds it under the same [`BindContext`], so it inherits the
/// one consistent per-statement snapshot — docs/16 §6), materializes the result,
/// and folds it into the outer row filter (a folded literal, an equality-`OR` set
/// test, or a constant keep-all/keep-none).
///
/// When `correlation` is `Some` the subquery is **correlated** ([STL-239]): the
/// inner references an outer column, so it is re-run once per outer row with that
/// row's value substituted (the per-row fallback the v0.3 bar permits). The inner
/// still binds under the same snapshot, so the per-statement `(sys, valid)` rule
/// holds for every re-execution.
///
/// Mutually exclusive with [`BoundSelect::filter`] and
/// [`BoundSelect::period_filter`]: a `WHERE` is exactly one of the three shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSubqueryFilter {
    /// Which subquery predicate the `WHERE` is, and the outer column it tests.
    pub kind: SubqueryKind,
    /// The bound inner query. For an **uncorrelated** subquery ([STL-234]) this is
    /// evaluated **once** at the outer plan's snapshot; for a **correlated** one
    /// ([STL-239]) — see [`correlation`](Self::correlation) — it is re-run once per
    /// outer row with that row's value substituted for the outer reference, so its
    /// own [`filter`](BoundSelect::filter) is left empty (the correlation predicate
    /// is lifted off it at bind time and re-applied per row by the executor).
    pub subquery: Box<BoundSelect>,
    /// The outer-column correlation the inner references, or `None` when the
    /// subquery is uncorrelated ([STL-234]). `Some` marks the per-row
    /// re-execution path ([STL-239]): the inner is **not** constant over the outer
    /// rows, so the executor substitutes each outer row's
    /// [`outer_column`](Correlation::outer_column) value into the inner's filter
    /// and re-runs it, rather than folding a once-evaluated result.
    pub correlation: Option<Correlation>,
}

/// A single correlation between an inner subquery and its outer query ([STL-239]).
///
/// The inner's `WHERE` relates one of its own columns to an **outer-query** column
/// (`… WHERE inner.k = outer.k`), so the inner is not constant over the outer rows.
/// The binder lifts this single comparison off the inner (the inner binds with no
/// `WHERE`, since the engine's one-comparison `WHERE` limit means the correlation
/// *is* the whole `WHERE`) and records it here. The executor re-runs the inner once
/// per outer row, substituting that row's [`outer_column`](Self::outer_column) value
/// for the reference — the per-row fallback the v0.3 bar permits (correctness, not
/// performance; decorrelating the common `EXISTS`/`IN` cases onto a semi/anti join
/// is a tracked follow-up).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Correlation {
    /// The outer-query column the inner references, by **outer** schema index
    /// (`0` is the business key). Read from each outer row and substituted into the
    /// inner's per-row filter.
    pub outer_column: usize,
    /// The inner column the correlation constrains, by **inner** schema index.
    pub inner_column: usize,
    /// The comparison, normalized so the inner column is the left operand
    /// (`inner_column <op> <outer value>`). A source comparison written outer-first
    /// (`outer.k < inner.k`) is [mirrored](CompareOp::mirror) here so the executor
    /// always builds `inner_column op literal`.
    pub op: CompareOp,
}

/// A correlated `EXISTS` / `NOT EXISTS` subquery lowered to a **semi / anti hash
/// join** ([STL-317]).
///
/// The set-based replacement for the [STL-239] per-row re-execution, for the shape
/// that decorrelates onto STL-172's single-key join.
///
/// Recognized by [`BoundSubqueryFilter::semi_anti_decorrelation`]: the correlation
/// is an **equality** on the key (`inner.k = outer.k`), so "∃ an inner row for this
/// outer row" is exactly "the outer key is a member of the inner key set" — a hash
/// **semi** join for `EXISTS`, an **anti** join for `NOT EXISTS`. The join key is
/// the correlation key on each side; a NULL key never matches, which reproduces the
/// per-row [`empty_inner_keeps`](https://allegromusic.atlassian.net/browse/STL-239)
/// rule (a NULL outer key drops under `EXISTS`, survives under `NOT EXISTS`) without
/// any per-row run.
///
/// `IN` / `NOT IN` (which carry a second equality — the membership column — so they
/// need a composite key, and whose `NOT IN` NULL-in-set trap is not an anti join)
/// and a correlated scalar lookup keep the per-row fallback, so this is `None` for
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SemiAntiDecorrelation {
    /// The join to compute: [`JoinType::Semi`] for `EXISTS`, [`JoinType::Anti`] for
    /// `NOT EXISTS`.
    pub join_type: JoinType,
    /// The outer correlation column (by **outer** schema index) — the join's probe
    /// key, read from each outer row.
    pub outer_column: usize,
    /// The inner correlation column (by **inner** schema index) — the join's build
    /// key, read from each inner row. The two key columns share a type (the binder
    /// enforced it on the [`Correlation`]).
    pub inner_column: usize,
}

/// A correlated `IN` subquery lowered to a **composite-key (two-column) semi hash
/// join** ([STL-337]).
///
/// The `IN` analogue of [`SemiAntiDecorrelation`]: `t.a IN (SELECT s.a FROM s WHERE
/// s.k = t.k)` keeps an outer row iff there is an inner row with `s.k = t.k` **and**
/// `s.a = t.a` — a two-key semi join, not the single-key one `EXISTS` folds onto.
/// `hash_join` is single-key, so the engine joins on a **synthetic composite key**
/// `(correlation key, membership value)` per side; a key is NULL when *either*
/// component is NULL, so the join's "a NULL key never matches" rule reproduces
/// `IN`'s three-valued logic exactly — a NULL outer membership value (or correlation
/// key), or a NULL inner one, can never make `IN` `TRUE`, so the row drops.
///
/// Recognized by [`BoundSubqueryFilter::composite_semi_decorrelation`]: a
/// **non-negated** `IN` whose correlation is an **equality** on the key and whose
/// inner is a plain single-table scan. `NOT IN` is deliberately excluded — its
/// NULL-in-set trap is per-correlation-group and **not** an anti join (a NULL
/// membership value anywhere in an outer row's inner set makes `NOT IN` unknown for
/// it, which a plain composite anti join would wrongly keep), so it stays on the
/// [STL-239] per-row fallback (a NULL-aware anti variant is a tracked follow-up).
///
/// The binder lays the inner result out as `[membership, correlation key]` (it
/// appends the correlation key after the `IN` membership column — see
/// `project_in_correlation_key`), so [`inner_member_column`](Self::inner_member_column)
/// is `0` and [`inner_key_column`](Self::inner_key_column) is `1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompositeSemiDecorrelation {
    /// The outer correlation column (by **outer** schema index) — the first
    /// component of the outer probe key (`t.k`).
    pub outer_key_column: usize,
    /// The outer membership column (by **outer** schema index) — the second
    /// component of the outer probe key (`t.a`, the operand of `IN`).
    pub outer_member_column: usize,
    /// The inner correlation key column, by **inner result** position — the first
    /// component of the inner build key (`s.k`). Shares a type with
    /// [`outer_key_column`](Self::outer_key_column) (binder-enforced).
    pub inner_key_column: usize,
    /// The inner membership column, by **inner result** position — the second
    /// component of the inner build key (`s.a`). Shares a type with
    /// [`outer_member_column`](Self::outer_member_column) (binder-enforced).
    pub inner_member_column: usize,
}

impl BoundSubqueryFilter {
    /// Recognize a correlated `EXISTS` / `NOT EXISTS` that decorrelates onto a
    /// single-key semi / anti hash join ([STL-317]), or `None` to keep the
    /// [STL-239] per-row fallback.
    ///
    /// Decorrelation is sound only when the inner's row-*presence* for an outer row
    /// is exactly "∃ an inner row whose correlation key equals the outer key":
    ///
    /// * the predicate is `[NOT] EXISTS` — a scalar comparison or `[NOT] IN` is not
    ///   a bare presence test (an `IN` adds the membership equality, a scalar a
    ///   value), so neither folds onto a single-key join; and
    /// * the correlation comparison is an **equality** (`=`) — a `<` / `>` / … is a
    ///   range, not key-set membership; and
    /// * the inner is a **plain single base-table scan** (`is_plain_scan`): an
    ///   aggregate inner always returns one row, and a join / nested subquery / CTE /
    ///   `DISTINCT` / `LIMIT`-`OFFSET` / period or range read can each change
    ///   presence, so each keeps the per-row path.
    ///
    /// The inner binds with its correlation `WHERE` lifted off (so its `filter` is
    /// `None`) and its projection normalized to `*` — the binder does this for any
    /// `[NOT] EXISTS` inner (the `bind_inner_query` step keys off the predicate, not
    /// the negation) — so the executor reads the correlation key straight out of the
    /// inner result at [`inner_column`](SemiAntiDecorrelation::inner_column).
    #[must_use]
    pub fn semi_anti_decorrelation(&self) -> Option<SemiAntiDecorrelation> {
        let correlation = self.correlation?;
        if correlation.op != CompareOp::Eq {
            return None;
        }
        let join_type = match self.kind {
            SubqueryKind::Exists { negated } => {
                if negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                }
            }
            SubqueryKind::In { .. } | SubqueryKind::Scalar { .. } => return None,
        };
        if !self.subquery.is_plain_scan() {
            return None;
        }
        Some(SemiAntiDecorrelation {
            join_type,
            outer_column: correlation.outer_column,
            inner_column: correlation.inner_column,
        })
    }

    /// Recognize a correlated `IN` that decorrelates onto a **composite-key semi
    /// hash join** ([STL-337]), or `None` to keep the [STL-239] per-row fallback.
    ///
    /// Decorrelation is sound only when keeping an outer row is exactly "∃ an inner
    /// row whose `(correlation key, membership value)` equals the outer
    /// `(correlation key, tested column)`":
    ///
    /// * the predicate is a **non-negated `IN`** — `NOT IN`'s per-group NULL-in-set
    ///   trap is not an anti join (see [`CompositeSemiDecorrelation`]), and a scalar
    ///   / `EXISTS` is a different shape ([`semi_anti_decorrelation`] handles
    ///   `EXISTS`); and
    /// * the correlation comparison is an **equality** (`=`) — a range correlation
    ///   is not key-set membership; and
    /// * the inner is a **plain single base-table scan** (`is_plain_scan`) — the same
    ///   gate `EXISTS` uses, for the same reason (an aggregate / `DISTINCT` /
    ///   `LIMIT` / nested inner can change which rows the membership set contains).
    ///
    /// For such an `IN` the binder appended the correlation key after the membership
    /// column (`project_in_correlation_key`), so the inner result is exactly
    /// `[membership, correlation key]`; this confirms that two-column layout before
    /// returning a plan, so the engine never reads a column the binder did not
    /// project (a missing layout falls back to the still-correct per-row path).
    ///
    /// [`semi_anti_decorrelation`]: Self::semi_anti_decorrelation
    #[must_use]
    pub fn composite_semi_decorrelation(&self) -> Option<CompositeSemiDecorrelation> {
        let correlation = self.correlation?;
        if correlation.op != CompareOp::Eq {
            return None;
        }
        let SubqueryKind::In { column, negated } = self.kind else {
            return None;
        };
        if negated {
            return None;
        }
        if !self.subquery.is_plain_scan() {
            return None;
        }
        // The binder appended the correlation key as the inner's second projected
        // column, so a decorrelatable `IN` inner projects exactly `[membership,
        // correlation key]`. Anything else means the layout was not built — keep the
        // per-row path.
        let Projection::Items(items) = &self.subquery.projection else {
            return None;
        };
        if items.len() != 2 {
            return None;
        }
        Some(CompositeSemiDecorrelation {
            outer_key_column: correlation.outer_column,
            outer_member_column: column,
            inner_key_column: 1,
            inner_member_column: 0,
        })
    }
}

/// The shape of an uncorrelated-subquery `WHERE` predicate ([STL-234]).
///
/// The outer operand is always a single value column (by schema index) — the
/// same one-anchor-column restriction the plain [`BoundPredicate`] filter uses;
/// a richer comparand (an arithmetic of the column, the SELECT-list position) is
/// a tracked follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryKind {
    /// `<column> <op> (SELECT <scalar>)` — the inner yields one scalar value: the
    /// executor folds it to a literal and applies `<column> <op> <literal>`. An
    /// inner returning **no row** makes the scalar `NULL` (the comparison is
    /// then unknown for every row — none pass); one returning **more than one
    /// row** is the standard's cardinality violation (SQLSTATE `21000`).
    Scalar {
        /// The outer value column compared, by schema index.
        column: usize,
        /// The comparison operator, as written with the column on its original
        /// side.
        op: CompareOp,
        /// `true` when the subquery is the **left** operand (`(SELECT …) < col`),
        /// so the lowering preserves operand order for a non-commutative `op`.
        subquery_left: bool,
    },
    /// `<column> [NOT] IN (SELECT <col>)` — membership of the outer column in the
    /// inner's single-column result set, under SQL three-valued logic. An empty
    /// set makes `IN` false (and `NOT IN` true) for every row; a `NOT IN` whose
    /// set contains a `NULL` matches no row (the classic three-valued trap).
    In {
        /// The outer value column tested, by schema index.
        column: usize,
        /// `true` for `NOT IN`.
        negated: bool,
    },
    /// `[NOT] EXISTS (SELECT …)` — whether the inner returns any row. Being
    /// uncorrelated, the test is a single constant for the whole outer scan; the
    /// inner's select-list is irrelevant (only row presence matters).
    Exists {
        /// `true` for `NOT EXISTS`.
        negated: bool,
    },
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
    /// [`OutputItem::Aggregate`] indexes into this list. A [`HavingScalar::Aggregate`]
    /// indexes into it too — an aggregate a `HAVING` references but the SELECT
    /// list does not is appended here (computed, never emitted), so it is wider
    /// than [`items`](Self::items) when `HAVING` introduces one ([STL-265]).
    pub aggregates: Vec<AggregateCall>,
    /// The output columns, in SELECT-list order — each either a passed-through
    /// grouping column or an aggregate.
    pub items: Vec<OutputItem>,
    /// A bound `HAVING` predicate over the grouped output, or `None` when the
    /// query gave none ([STL-265]). Applied **after** the aggregate folds and
    /// **before** the result-shaping tail (the Postgres pipeline:
    /// aggregate → `HAVING` → `DISTINCT` → `ORDER BY` → `LIMIT`): a group is kept
    /// iff the predicate is `TRUE` for it. Its operands address this plan's
    /// [`group_by`](Self::group_by) positions and [`aggregates`](Self::aggregates)
    /// — not the SELECT-list [`items`](Self::items) — so it may filter on an
    /// aggregate the query never projects.
    pub having: Option<BoundHaving>,
    /// The result columns `(name, type)`, aligned to [`items`](Self::items): a
    /// `RowDescription` header for the grouped result.
    pub columns: Vec<(String, LogicalType)>,
}

/// A bound `HAVING <scalar> <compare> <scalar>` predicate — the post-aggregation
/// filter ([STL-265], richer predicates [STL-327]).
///
/// Structurally the aggregate-output analog of the `WHERE`-row [`BoundPredicate`]:
/// the same six [comparison operators](CompareOp), each side a grouping column, an
/// aggregate result, a literal, or an integer arithmetic of one. The difference is
/// the operand vocabulary — a [`HavingScalar`] addresses the *grouped* batch (a
/// grouping column or an aggregate result), not a scanned row's cells — so it
/// evaluates over [`BoundAggregate`]'s output rather than the pre-grouping rows a
/// `WHERE` sees. Both sides may anchor (`COUNT(*) > SUM(amount)`, `dept > COUNT(*)`),
/// comparing across the numeric types the evaluator promotes, and a `FLOAT8` `AVG`
/// operand compares ([STL-327]); a subquery inside `HAVING` rides the subquery
/// tickets' vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundHaving {
    /// The comparison's left operand.
    pub left: HavingScalar,
    /// The comparison operator.
    pub op: CompareOp,
    /// The comparison's right operand.
    pub right: HavingScalar,
}

/// One side of a bound `HAVING` comparison: a grouping column, an aggregate
/// result, a folded literal, or an integer arithmetic of them ([STL-265]).
///
/// Operands address the aggregate's *output* — a grouping column by its position
/// in [`BoundAggregate::group_by`], an aggregate by its index into
/// [`BoundAggregate::aggregates`] — the executor reads them straight out of the
/// grouped batch (`out.groups[j]` / `out.aggregates[k]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HavingScalar {
    /// The grouping column at this position in [`BoundAggregate::group_by`] (the
    /// same index the grouped batch's `groups` are emitted in).
    Group(usize),
    /// The aggregate at this index in [`BoundAggregate::aggregates`].
    Aggregate(usize),
    /// A constant, folded to the predicate's anchor type.
    Literal(ScalarValue),
    /// `left <op> right` — integer arithmetic over two having scalars.
    Arith {
        /// The arithmetic operator.
        op: ArithOp,
        /// The left operand.
        left: Box<HavingScalar>,
        /// The right operand.
        right: Box<HavingScalar>,
    },
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
    const fn from_name(name: &str) -> Option<Self> {
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
/// and the join key's type (for decoding). The two sides' column lists, in order
/// (the left's, then the right's for an `INNER` / `LEFT` join), are the join's
/// **addressable output** — the flat row a [`BoundJoin::output`] index, a `WHERE`,
/// an aggregate, and an `ORDER BY` over the join all address ([STL-264]).
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

/// A bound equi-join — a two-table join ([STL-172], [STL-264]) or an N-way
/// left-deep chain ([STL-323]).
///
/// The executor scans [`left`](Self::left) (the leftmost input) at the read
/// snapshot, then folds the [`steps`](Self::steps) **left-deep**: each step joins
/// the accumulated output so far against a freshly scanned right input, growing the
/// **addressable output** — the accumulated columns, then the step's right columns
/// for an `INNER` / `LEFT` join (a `SEMI` / `ANTI` step keeps only the accumulated
/// left). A `WHERE`, an aggregate, and the `DISTINCT` / `ORDER BY` / `OFFSET` /
/// `LIMIT` tail then run over the final flat row exactly as the single-table path
/// runs them over a reconstructed row ([STL-264]): the bound [`BoundSelect`] carries
/// them, addressing columns by their index in the addressable output.
/// [`output`](Self::output) is the projection — the indices the `SELECT` list
/// selects from it.
///
/// A two-table join is the degenerate chain of one step. Each step binds a single
/// equality over one column of the accumulated output and one of the new input.
/// `AS OF` over the join ([STL-243]) and `RIGHT` / `FULL` / non-equi joins
/// ([STL-270]) are tracked follow-ups; join reordering is out of scope (the chain
/// runs in syntactic, left-deep order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundJoin {
    /// The leftmost (seed) input — the left side of the first join in the chain.
    pub left: BoundJoinSide,
    /// The left-deep chain of joins, in syntactic order: one [`BoundJoinStep`] per
    /// `JOIN` keyword. A two-table join has exactly one step; an N-way join
    /// `a JOIN b … JOIN c …` has one step per added input ([STL-323]). Always
    /// non-empty (the join path is routed only when at least one `JOIN` is present).
    pub steps: Vec<BoundJoinStep>,
    /// The projection: the `SELECT`-list output columns, as indices into the
    /// **addressable output** (the accumulated columns the whole chain produces).
    /// `SELECT *` is every index in order. Unused on the aggregate path, whose
    /// output columns come from [`BoundSelect::aggregate`].
    pub output: Vec<usize>,
    /// The result columns `(name, type)`, aligned to [`output`](Self::output) — a
    /// `RowDescription` header for the projected (non-aggregate) join result.
    pub columns: Vec<(String, LogicalType)>,
}

/// One join in a [`BoundJoin`]'s left-deep chain ([STL-323]): the right input
/// folded in at this step and the equi-condition relating it to the accumulated
/// output so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundJoinStep {
    /// Which join to compute at this step.
    pub join_type: JoinType,
    /// The right (build) input joined in at this step.
    pub right: BoundJoinSide,
    /// The equi-join key as a **flat addressable index** into the accumulated
    /// output the chain has built *before* this step (for the first step, that is
    /// just [`BoundJoin::left`]'s columns).
    pub left_key: usize,
    /// The equi-join key's column index in [`right`](Self::right)'s schema. The two
    /// key columns share a [`LogicalType`] (the binder enforces it).
    pub right_key: usize,
}

/// A bound non-recursive common-table-expression or derived table ([STL-242]).
///
/// A named subquery whose result the executor materializes **once** at the
/// statement's snapshot, then references like a table in the outer query.
///
/// Both a `WITH name AS (SELECT …)` entry and a `FROM (SELECT …) AS d` derived
/// table lower to this same shape — a derived table is just an inline,
/// single-use CTE named by its alias. The defining query is a fully bound
/// [`plan`](Self::plan); [`columns`](Self::columns) is its output header (with any
/// `name(col, …)` alias applied), which the binder resolves outer references
/// against and the executor uses to type the materialized rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCte {
    /// The relation name introduced into the query's scope — the `WITH` name or
    /// the derived table's alias. A reference resolves a [`BoundSelect::table`] /
    /// [`BoundJoinSide::table`] to this name.
    pub name: String,
    /// The bound defining query, evaluated once at the outer statement's snapshot
    /// (it binds under the same snapshot, so the one consistent per-statement
    /// `(sys, valid)` rule holds — docs/16 §6). May itself reference an earlier
    /// CTE in the same `WITH` list.
    pub plan: Box<BoundSelect>,
    /// The relation's output columns `(name, type)`, in order — the materialized
    /// result's header and the shape outer references resolve against.
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

    /// The query carried a clause outside the supported single-table surface
    /// (`WITH RECURSIVE`, `DISTINCT ON`, `QUALIFY`, locking, …). [`BoundSelect`]
    /// does not carry these, so accepting them would silently drop user intent —
    /// they are rejected. (`ORDER BY` / `LIMIT` / `OFFSET` / `FETCH` / `DISTINCT`
    /// bind since [STL-263]; `GROUP BY` since [STL-171]; `HAVING` since [STL-265].)
    #[error("unsupported `{0}` in a SELECT")]
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

    /// The `WHERE` clause is not a shape v0.2 lowers — a single comparison over
    /// one column, either side optionally an integer arithmetic of it ([STL-151],
    /// [STL-213]). A join predicate, a column-to-column compare, an `AND`/`OR`
    /// chain, `BETWEEN`/`IN`, arithmetic over a non-integer column, or a literal
    /// that cannot fold to the column's type all surface here rather than being
    /// silently dropped (which would return unfiltered rows — a wrong answer).
    #[error("unsupported WHERE predicate ({0})")]
    UnsupportedPredicate(String),

    /// A `WHERE` subquery predicate ([STL-234]) — a scalar comparison,
    /// `[NOT] IN`, or `[NOT] EXISTS` — is not a shape v0.3 binds: the outer
    /// operand of a scalar comparison or `IN` is not a bare value column, the
    /// inner query returns a number of columns other than one, the inner's
    /// single column has a different type than the outer column it is compared
    /// to, or the inner query itself does not bind. Correlated subqueries (the
    /// inner referencing an outer column) are a sibling ticket ([STL-239]) and
    /// surface here too. Rejected with the reason rather than silently dropped.
    ///
    /// [STL-239]: https://allegromusic.atlassian.net/browse/STL-239
    #[error("unsupported subquery ({0})")]
    Subquery(String),

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

    /// A `HAVING` is not a shape v0.3 binds ([STL-265]) — it is not a single
    /// comparison, it compares two aggregates / grouping columns (the
    /// aggregate-output analog of a column-to-column `WHERE`), it references no
    /// aggregate or grouping column to anchor on, a literal cannot fold to the
    /// anchor's type, or it filters on a value type the evaluator cannot compare
    /// (a `FLOAT8` `AVG`). A subquery inside `HAVING` rides the subquery tickets'
    /// vocabulary. Rejected with the reason rather than silently dropped (which
    /// would return unfiltered groups — a wrong answer).
    #[error("unsupported HAVING ({0})")]
    UnsupportedHaving(String),

    /// The `ORDER BY` is not a shape v0.3 binds ([STL-263]) — a non-column key
    /// (an expression, an ordinal), an explicit `NULLS FIRST`/`NULLS LAST`
    /// override, `ORDER BY ALL`, a ClickHouse `WITH FILL`/`INTERPOLATE`, or (in
    /// an aggregate query) a name that is not a select-list output column.
    /// Rejected with the reason rather than silently mis-ordered.
    #[error("unsupported ORDER BY: {0}")]
    UnsupportedOrderBy(String),

    /// `SELECT DISTINCT … ORDER BY <col>` where `<col>` is not in the select
    /// list ([STL-263]). After deduplication the non-projected value is
    /// ambiguous, so Postgres rejects this (SQLSTATE `42P10`,
    /// `invalid_column_reference`) — and so does Stele, with the same wording.
    #[error("for SELECT DISTINCT, ORDER BY expressions must appear in select list")]
    DistinctOrderBy,

    /// The `LIMIT` / `OFFSET` / `FETCH` clause is not a shape v0.3 binds
    /// ([STL-263]) — a non-literal count (an expression, a parameter), a
    /// negative literal, `FETCH … WITH TIES` / `PERCENT`, a MySQL
    /// `LIMIT off, n`, or a ClickHouse `LIMIT … BY`. Rejected with the reason
    /// rather than silently returning the wrong number of rows.
    #[error("unsupported LIMIT/OFFSET/FETCH: {0}")]
    UnsupportedLimit(String),

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

    /// A `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` range scan
    /// ([STL-244]) was combined with a clause v0.3 does not yet support over a
    /// range — any `AS OF` point qualifier, a `JOIN`, an aggregate / `GROUP BY`,
    /// `DISTINCT` / `ORDER BY` / `LIMIT` / `OFFSET`, a subquery or period-predicate
    /// `WHERE`, or a CTE / derived-table source. Each is a tracked follow-up;
    /// rejected rather than silently mis-bound.
    #[error("unsupported range scan: {0}")]
    UnsupportedSystemRange(String),

    /// A `FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` range scan ([STL-328])
    /// was combined with a clause v0.3 does not yet support over a range. The valid
    /// axis allows a `FOR SYSTEM_TIME AS OF` point pin (it fixes the system snapshot
    /// the valid history is read at — `v(k, S, V_range)`), but rejects a
    /// `FOR VALID_TIME AS OF` (a point and a range on the *same* axis), and the same
    /// shaping / aggregate / subquery / period-predicate / `JOIN` / CTE clauses the
    /// system-axis range rejects. Each is a tracked follow-up.
    #[error("unsupported valid-time range scan: {0}")]
    UnsupportedValidRange(String),

    /// A `FOR SYSTEM_TIME` range folded to an empty or reversed interval
    /// ([STL-244]). The half-open `FROM a TO b` requires `a < b` (it covers no
    /// instant otherwise); the closed `BETWEEN a AND b` requires `a <= b`. Mirrors
    /// the §2 reversed / zero-length rejection on the write path.
    #[error(
        "empty or reversed FOR SYSTEM_TIME range [{from}, {to}{}",
        if *closed_upper { "]" } else { ")" }
    )]
    EmptySystemRange {
        /// The folded lower bound, in microseconds.
        from: i64,
        /// The folded upper bound, in microseconds.
        to: i64,
        /// Whether the upper bound was inclusive (`BETWEEN`).
        closed_upper: bool,
    },

    /// A `FOR VALID_TIME` range folded to an empty or reversed interval
    /// ([STL-328]) — the valid-axis mirror of [`Self::EmptySystemRange`]. The
    /// half-open `FROM a TO b` requires `a < b`; the closed `BETWEEN a AND b`
    /// requires `a <= b`.
    #[error(
        "empty or reversed FOR VALID_TIME range [{from}, {to}{}",
        if *closed_upper { "]" } else { ")" }
    )]
    EmptyValidRange {
        /// The folded lower bound, in microseconds.
        from: i64,
        /// The folded upper bound, in microseconds.
        to: i64,
        /// Whether the upper bound was inclusive (`BETWEEN`).
        closed_upper: bool,
    },

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

    /// A `WITH` clause or derived table is not a shape v0.3 binds ([STL-242]) — a
    /// `WITH RECURSIVE` (deferred to v0.5), a derived table with no alias (a
    /// `FROM (SELECT …)` must be named), a `LATERAL` derived table, a `name(col, …)`
    /// column-alias list whose arity does not match the relation's columns, two
    /// CTEs sharing a name, or a CTE whose body does not bind. Rejected with the
    /// reason rather than silently mis-bound.
    ///
    /// [STL-242]: https://allegromusic.atlassian.net/browse/STL-242
    #[error("unsupported CTE/derived table ({0})")]
    Cte(String),
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
    // The public entry binds at the top level — no enclosing CTEs in scope. The
    // output-column header [`bind_select_scoped`] also returns is only needed when
    // a CTE / derived table binds this query (to type its materialization), so it
    // is dropped here.
    Ok(bind_select_scoped(stmt, ctx, &[])?.0)
}

/// Bind a `SELECT`, with `outer_ctes` the common-table-expressions already in
/// scope ([STL-242]) — the empty slice at the top level, the enclosing `WITH`
/// signatures when binding a nested CTE / derived-table body.
///
/// Returns the bound plan **and** its output-column header `(name, type)` — the
/// shape a `WITH name AS (…)` / `FROM (…) AS d` reference resolves against, which
/// the binder computes here where the schema is live (the engine's
/// `output_columns` agrees with it for the same plan).
// The single-table binding tail (snapshot, CTE-list, FROM resolution, projection,
// WHERE, aggregate, result shaping) reads top-to-bottom; splitting it would
// scatter one cohesive lowering across helpers rather than clarify it.
#[allow(clippy::too_many_lines)]
fn bind_select_scoped(
    stmt: &Statement,
    ctx: &BindContext,
    outer_ctes: &[CteSig],
) -> Result<(BoundSelect, Vec<(String, LogicalType)>), SelectError> {
    // An admin command (CHECKPOINT / FLUSH) has no SQL body, so it is "not a
    // SELECT" — the binder cleanly defers it to the engine's admin route.
    let body = stmt.sql().ok_or(SelectError::NotSelect)?;
    let (query, select) = single_select(body)?;

    // Resolve the statement's `(sys, valid)` snapshot up front: the `WITH` list and
    // the `FROM` relation bind at it, so every CTE inherits the one consistent
    // per-statement snapshot (docs/16 §6). A query with no temporal clause folds
    // to `ctx.snapshot`.
    let (snapshot, valid_snapshot) = resolve_snapshots(stmt, ctx.snapshot)?;

    // A `FOR { SYSTEM_TIME | VALID_TIME } { FROM a TO b | BETWEEN a AND b }` range
    // qualifier ([STL-244], [STL-328]): fold and validate it up front, splitting it
    // by axis. The system axis pins no point (it ranges); the valid axis ranges
    // over the valid period at the system `snapshot` resolved above (which a
    // `FOR SYSTEM_TIME AS OF` may have set — the valid range allows that pin).
    let (system_range, valid_range) = match resolve_temporal_range(stmt, ctx.snapshot)? {
        None => (None, None),
        Some(BoundTemporalRange::System(r)) => (Some(r), None),
        Some(BoundTemporalRange::Valid(r)) => (None, Some(r)),
    };

    // Bind this query's non-recursive `WITH` list, extending `outer_ctes` so a
    // later CTE may reference an earlier one ([STL-242]). `sigs` is the full scope
    // (outer + this query's) the body resolves relation names against.
    let cte_ctx = BindContext {
        snapshot,
        catalog: ctx.catalog,
    };
    let (mut ctes, sigs) = bind_with_list(query, &cte_ctx, outer_ctes)?;

    // A `JOIN` chain binds to a wholly different shape (a seed input, a left-deep
    // chain of join steps, a combined header), so it is routed before the
    // single-table path. The `WHERE` / aggregate / `DISTINCT` / `ORDER BY` / `LIMIT`
    // clauses compose over the join's output ([STL-264]) — bound against its
    // addressable columns.
    if let Some(join) = detect_join(select) {
        // A range scan over a join is a tracked follow-up — the single consistent
        // `(sys, valid)` snapshot rule a join pins (docs/16 §8) does not generalize
        // to an interval read of every input.
        if system_range.is_some() {
            return Err(SelectError::UnsupportedSystemRange(
                "over a JOIN".to_owned(),
            ));
        }
        if valid_range.is_some() {
            return Err(SelectError::UnsupportedValidRange("over a JOIN".to_owned()));
        }
        let (mut bound, mut side_ctes) = bind_join(
            stmt,
            ctx,
            query,
            select,
            join,
            &sigs,
            snapshot,
            valid_snapshot,
        )?;
        // The `WITH` relations materialize first (a derived join side may reference
        // one), then any derived tables the join sides introduced.
        ctes.append(&mut side_ctes);
        reject_duplicate_relation_names(&ctes)?;
        bound.ctes = ctes;
        // An aggregate over the join takes its header from the aggregate plan; a
        // plain projection takes it from the join's projected columns.
        let header = bound.aggregate.as_ref().map_or_else(
            || {
                bound
                    .join
                    .as_ref()
                    .expect("the join path sets `join`")
                    .columns
                    .clone()
            },
            |agg| agg.columns.clone(),
        );
        return Ok((bound, header));
    }

    // The single `FROM` relation: a base table, a CTE in scope, or a derived table
    // (`FROM (SELECT …) AS d`, lowered to a single-use CTE named by its alias).
    let resolved = resolve_from(select, ctx, snapshot, &sigs)?;
    if let Some(derived) = resolved.derived {
        ctes.push(derived);
        reject_duplicate_relation_names(&ctes)?;
    }
    let schema: &TableSchema = &resolved.schema;
    let table: &str = &resolved.name;
    let materialized = resolved.materialized;

    // An aggregate query (a `GROUP BY`, or an aggregate in the SELECT list) takes
    // a different shape: its output columns come from the aggregate plan, so the
    // plain projection is bound only for a non-aggregate read. Detection is purely
    // syntactic, so it runs before name resolution.
    let aggregate_query = is_aggregate_query(select);

    // A valid-time `AS OF` — or a `FOR VALID_TIME` range ([STL-328]) — only means
    // something on a table with a valid-time period; against a system-only table —
    // and a query-local CTE / derived table, whose ephemeral schema is system-only
    // — there is no valid axis to travel or range over.
    if (valid_snapshot.is_some() || valid_range.is_some())
        && !schema.temporal().valid_time_enabled()
    {
        return Err(SelectError::ValidTimeUnsupported {
            table: table.to_owned(),
        });
    }

    // A range scan ([STL-244], [STL-328]) binds its projection, result-shaping,
    // aggregate, and `WHERE` clauses against the table's columns *plus* the two
    // period endpoints it appends (`sys_from`/`sys_to`, or `valid_from`/`valid_to`),
    // so the rest of the SELECT surface composes over the range output the way it
    // does a point read ([STL-329]): `SELECT *` includes the endpoints, `ORDER BY
    // sys_from` and a named projection resolve them, and the provenance
    // pseudo-columns stay virtual past them. A non-range read binds against the
    // catalog schema unchanged (`range_schema` is `None`).
    let range_schema =
        range_effective_schema(schema, system_range.is_some(), valid_range.is_some())?;
    let bind_schema: &TableSchema = range_schema.as_ref().unwrap_or(schema);

    // Bind the projection list ([STL-303]): `*`, or one item per select-list entry
    // — a bare column (optionally `AS`-aliased), a computed scalar expression, or an
    // uncorrelated scalar subquery. An aggregate query takes its output columns from
    // the aggregate plan instead, so its projection is an unused `All` placeholder
    // the executor never consults. Each bare column is validated against the schema
    // live *at the snapshot* — a column added after the `AS OF` instant is not yet
    // present and is rejected here rather than deferred to the executor. A
    // provenance pseudo-column ([STL-247]) is accepted on a base table (the engine
    // resolves it against the fixed virtual layout after the table's own columns)
    // but not on a materialized CTE / derived relation, which carries none.
    let projection = if aggregate_query {
        Projection::All
    } else {
        bind_projection(
            select,
            bind_schema,
            table,
            materialized,
            ctx,
            snapshot,
            valid_snapshot,
        )?
    };

    // Bind the `GROUP BY` + aggregate plan against the resolved schema, which
    // gives the grouping/argument columns their indices and types.
    let aggregate = if aggregate_query {
        Some(bind_aggregate(select, bind_schema, table)?)
    } else {
        None
    };

    // The result-shaping clauses ([STL-263]): `DISTINCT` first (the 42P10 rule
    // needs it), then `ORDER BY` (resolved against the select list, falling
    // back to the schema), then `LIMIT`/`OFFSET`/`FETCH`.
    let distinct = bind_distinct(select)?;
    let order_by = bind_order_by(
        query,
        bind_schema,
        table,
        distinct,
        aggregate.as_ref(),
        &projection,
    )?;
    let (limit, offset) = bind_limit_offset(query)?;

    // The `WHERE` is one of three mutually-exclusive shapes: an uncorrelated
    // subquery predicate ([STL-234]), or the plain `<col> <cmp> <scalar>`
    // comparison ([STL-213]). (A period predicate is the third — lifted off the
    // token stream below.) The subquery dispatcher is tried first, since a
    // comparison whose operand is a `(SELECT …)` is its shape, not the plain
    // predicate binder's.
    let (filter, subquery_filter) =
        bind_where(select, bind_schema, table, ctx, snapshot, valid_snapshot)?;

    // A period predicate is lifted off the token stream (the executor-glue
    // `WHERE` is gone by the time `bind_where` runs), so the two filter shapes
    // are naturally mutually exclusive. Its `PERIOD(...)` endpoints fold against
    // the transaction `now` (`ctx.snapshot`), like `AS OF` operands.
    let period_filter = stmt
        .temporal
        .period_predicate
        .as_ref()
        .map(|clause| bind_period_predicate(clause, ctx.snapshot, schema, table))
        .transpose()?;

    // A materialized relation (CTE / derived table) carries its resolved columns so
    // the executor reads them from the materialization, not the catalog.
    let relation_columns = materialized.then(|| {
        schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect()
    });

    // A range scan ([STL-244] system axis, [STL-328] valid axis) is the "all
    // versions over an interval" read. Result-shaping ([STL-263]), aggregation
    // ([STL-171]), the provenance pseudo-columns ([STL-247], all [STL-329]), and a
    // period-predicate `WHERE` ([STL-165] / [STL-193], [STL-345]) now compose over
    // it: they bind against `bind_schema` above (the table's columns plus the
    // appended endpoints) and run through the engine's shared `finish_select`
    // pipeline. A period predicate's `PERIOD(...)` operands bind against the base
    // `schema` (above), so a value-column endpoint addresses the same index in the
    // reconstructed range row — where the endpoints are appended *after* the
    // value columns — that it does on a point read; the engine const-folds the
    // constant case and per-row-evaluates the column case identically over the
    // range output. What does *not* yet compose is rejected here so a range scan
    // never silently drops a clause — a subquery `WHERE` ([STL-234]), a CTE /
    // derived relation, and a computed / scalar-subquery select item ([STL-303]),
    // each a tracked follow-up. (The read-your-own-writes overlay and a range over
    // a `JOIN` are deferred too, handled at the engine / join paths respectively.)
    // Both axes reject the same shapes; only the error axis differs.
    if system_range.is_some() || valid_range.is_some() {
        let valid_axis = valid_range.is_some();
        let reject = |what: &str| {
            Err(if valid_axis {
                SelectError::UnsupportedValidRange(what.to_owned())
            } else {
                SelectError::UnsupportedSystemRange(what.to_owned())
            })
        };
        if subquery_filter.is_some() {
            return reject("with a subquery WHERE");
        }
        if materialized {
            return reject("over a CTE / derived table");
        }
        // A bare-column list — the table's columns, the appended endpoints, and the
        // provenance pseudo-columns — is what composes today; a computed expression
        // or scalar-subquery select item ([STL-303]) over a range is a tracked
        // follow-up.
        if !projection.is_all_columns() {
            return reject("with a computed or subquery projection");
        }
    }

    let header = aggregate.as_ref().map_or_else(
        || projected_header(bind_schema, &projection),
        |agg| agg.columns.clone(),
    );

    let bound = BoundSelect {
        table: table.to_owned(),
        schema_id: schema.schema_id(),
        snapshot,
        valid_snapshot,
        system_range,
        valid_range,
        projection,
        filter,
        period_filter,
        subquery_filter,
        aggregate,
        join: None,
        ctes,
        relation_columns,
        distinct,
        order_by,
        offset,
        limit,
    };
    Ok((bound, header))
}

/// One common-table-expression / derived table in binding scope ([STL-242]): its
/// name and the ephemeral [`TableSchema`] of its output columns, which outer
/// references resolve against.
#[derive(Debug, Clone)]
struct CteSig {
    /// The relation name introduced into scope (the `WITH` name / derived alias).
    name: String,
    /// The relation's ephemeral schema — its output columns, system-only and
    /// carrying the reserved `SchemaId(0)` sentinel ([`TableSchema::ephemeral`]).
    schema: TableSchema,
}

/// A resolved single-table `FROM` relation: either a catalog base table (a
/// **borrowed** schema) or a query-local CTE / derived table (an **owned**
/// ephemeral schema), behind one [`Deref`](std::ops::Deref) to [`TableSchema`] so
/// the binding helpers consume it uniformly ([STL-242]).
enum RelationSchema<'a> {
    /// A catalog base table's schema, borrowed at the read snapshot.
    Borrowed(&'a TableSchema),
    /// A CTE / derived table's ephemeral schema, owned for the bind.
    Owned(TableSchema),
}

impl std::ops::Deref for RelationSchema<'_> {
    type Target = TableSchema;
    fn deref(&self) -> &TableSchema {
        match self {
            Self::Borrowed(schema) => schema,
            Self::Owned(schema) => schema,
        }
    }
}

/// What a single-table `FROM` resolved to ([STL-242]): the relation name, its
/// schema, whether it is a materialized relation (CTE / derived table the engine
/// reads from its materialization, not storage), and — for a derived table — the
/// freshly bound [`BoundCte`] the query must register.
struct ResolvedFrom<'a> {
    name: String,
    schema: RelationSchema<'a>,
    materialized: bool,
    derived: Option<BoundCte>,
}

/// Reject two relations introduced under one name within a single query
/// ([STL-242]) — a `WITH` name and a derived-table alias, or two derived-table
/// aliases, that collide.
///
/// CTEs and derived tables share one flat per-statement materialization map
/// (the engine's `CteScope`, keyed by name), so two same-named relations would
/// silently overwrite one another there. Rejecting the collision keeps a
/// reference unambiguous. (`WITH`×`WITH` collisions are already caught earlier,
/// by [`bind_with_list`].) This is marginally stricter than Postgres, which lets
/// a derived-table alias *shadow* an otherwise-unused CTE; that pathological
/// shape is rejected here rather than mis-resolved.
fn reject_duplicate_relation_names(ctes: &[BoundCte]) -> Result<(), SelectError> {
    for (i, cte) in ctes.iter().enumerate() {
        if ctes[..i].iter().any(|earlier| earlier.name == cte.name) {
            return Err(SelectError::Cte(format!(
                "relation name {:?} is introduced more than once (a CTE name and a \
                 derived-table alias, or two aliases, collide)",
                cte.name
            )));
        }
    }
    Ok(())
}

/// Bind a query's non-recursive `WITH` list into [`BoundCte`]s plus the
/// accumulated [`CteSig`] scope (the `outer_ctes` already in scope, then this
/// query's, in declaration order), so a later CTE may reference an earlier one
/// ([STL-242]).
fn bind_with_list(
    query: &Query,
    ctx: &BindContext,
    outer_ctes: &[CteSig],
) -> Result<(Vec<BoundCte>, Vec<CteSig>), SelectError> {
    let mut sigs: Vec<CteSig> = outer_ctes.to_vec();
    let mut ctes: Vec<BoundCte> = Vec::new();
    let Some(with) = &query.with else {
        return Ok((ctes, sigs));
    };
    if with.recursive {
        return Err(SelectError::Cte(
            "WITH RECURSIVE is not supported (deferred to v0.5)".to_owned(),
        ));
    }
    for cte in &with.cte_tables {
        let name = cte.alias.name.value.clone();
        if sigs.iter().any(|s| s.name == name) {
            return Err(SelectError::Cte(format!("duplicate CTE name {name:?}")));
        }
        // Bind the body against the scope built so far (earlier CTEs visible).
        let (bound, schema) = bind_named_subquery(&cte.query, &cte.alias, ctx, &sigs)?;
        sigs.push(CteSig { name, schema });
        ctes.push(bound);
    }
    Ok((ctes, sigs))
}

/// Bind a named subquery — a `WITH name AS (subquery)` body or a
/// `FROM (subquery) AS name` derived table — into a [`BoundCte`] and its ephemeral
/// [`TableSchema`] ([STL-242]).
///
/// The subquery binds under `sigs` (the enclosing CTE scope), so it may reference
/// an earlier sibling CTE; a `name(col, …)` alias renames the output columns.
fn bind_named_subquery(
    subquery: &Query,
    alias: &sqlparser::ast::TableAlias,
    ctx: &BindContext,
    sigs: &[CteSig],
) -> Result<(BoundCte, TableSchema), SelectError> {
    let name = alias.name.value.clone();
    let stmt = Statement {
        body: StatementBody::Sql(SqlStatement::Query(Box::new(subquery.clone()))),
        temporal: Temporal::default(),
    };
    let (plan, header) = bind_select_scoped(&stmt, ctx, sigs)?;
    let columns = apply_column_aliases(header, &alias.columns)?;
    let defs = columns
        .iter()
        .map(|(n, t)| ColumnDef::new(n.clone(), *t))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SelectError::Cte(e.to_string()))?;
    let schema = TableSchema::ephemeral(defs).map_err(|e| SelectError::Cte(e.to_string()))?;
    Ok((
        BoundCte {
            name,
            plan: Box::new(plan),
            columns,
        },
        schema,
    ))
}

/// Apply an optional `name(col, …)` column-alias list to a relation's output
/// header ([STL-242]): empty leaves the header as-is; otherwise each output column
/// is renamed (its type kept), rejecting an arity mismatch or a typed alias.
fn apply_column_aliases(
    header: Vec<(String, LogicalType)>,
    aliases: &[sqlparser::ast::TableAliasColumnDef],
) -> Result<Vec<(String, LogicalType)>, SelectError> {
    if aliases.is_empty() {
        return Ok(header);
    }
    if aliases.iter().any(|a| a.data_type.is_some()) {
        return Err(SelectError::Cte(
            "a typed column alias (name(col TYPE, …)) is not supported".to_owned(),
        ));
    }
    if aliases.len() != header.len() {
        return Err(SelectError::Cte(format!(
            "column alias list has {} names but the relation has {} columns",
            aliases.len(),
            header.len()
        )));
    }
    Ok(aliases
        .iter()
        .zip(header)
        .map(|(alias, (_, ty))| (alias.name.value.clone(), ty))
        .collect())
}

/// Resolve a single-table `FROM` relation ([STL-242]): a CTE in `sigs` (which
/// shadows a catalog table of the same name), a base table at `snapshot`, or a
/// `FROM (SELECT …) AS d` derived table (bound into a single-use [`BoundCte`]).
fn resolve_from<'a>(
    select: &Select,
    ctx: &'a BindContext<'a>,
    snapshot: SystemTimeMicros,
    sigs: &[CteSig],
) -> Result<ResolvedFrom<'a>, SelectError> {
    let [from] = select.from.as_slice() else {
        return Err(SelectError::UnsupportedFrom("not exactly one table"));
    };
    if !from.joins.is_empty() {
        return Err(SelectError::UnsupportedFrom("join"));
    }
    match &from.relation {
        TableFactor::Table { name, .. } => {
            let name = table_factor_name(name)?;
            // A CTE shadows a catalog table of the same name (the SQL scoping rule);
            // the innermost binding wins, so the scope is searched last-to-first.
            if let Some(sig) = sigs.iter().rev().find(|s| s.name == name) {
                return Ok(ResolvedFrom {
                    name: name.to_owned(),
                    schema: RelationSchema::Owned(sig.schema.clone()),
                    materialized: true,
                    derived: None,
                });
            }
            let schema = match resolve_table_at(ctx.catalog, name, snapshot) {
                TableResolution::Found(schema) => schema,
                TableResolution::Unknown => {
                    return Err(SelectError::UnknownTable(name.to_owned()));
                }
                TableResolution::BeforeHistory { first_commit } => {
                    return Err(SelectError::BeforeHistory {
                        table: name.to_owned(),
                        snapshot: snapshot.0,
                        first_commit: first_commit.0,
                    });
                }
                TableResolution::NotLive => {
                    return Err(SelectError::TableNotLive {
                        table: name.to_owned(),
                        snapshot: snapshot.0,
                    });
                }
            };
            Ok(ResolvedFrom {
                name: name.to_owned(),
                schema: RelationSchema::Borrowed(schema),
                materialized: false,
                derived: None,
            })
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            ..
        } => {
            if *lateral {
                return Err(SelectError::Cte(
                    "a LATERAL derived table is not supported".to_owned(),
                ));
            }
            let Some(alias) = alias else {
                return Err(SelectError::Cte(
                    "a derived table (a FROM (SELECT …)) must have an alias".to_owned(),
                ));
            };
            let derived_ctx = BindContext {
                snapshot,
                catalog: ctx.catalog,
            };
            let (cte, schema) = bind_named_subquery(subquery, alias, &derived_ctx, sigs)?;
            Ok(ResolvedFrom {
                name: cte.name.clone(),
                schema: RelationSchema::Owned(schema),
                materialized: true,
                derived: Some(cte),
            })
        }
        _ => Err(SelectError::UnsupportedFrom("non-table relation")),
    }
}

/// The single, unqualified identifier of a `FROM` table name, or the matching
/// [`SelectError::UnsupportedFrom`] for a non-identifier / schema-qualified name.
fn table_factor_name(name: &sqlparser::ast::ObjectName) -> Result<&str, SelectError> {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|id| id.value.as_str())
            .ok_or(SelectError::UnsupportedFrom("non-identifier table name")),
        _ => Err(SelectError::UnsupportedFrom("schema-qualified table name")),
    }
}

/// The `(name, type)` output header a plain (non-aggregate, non-join) read
/// projects ([STL-242]) — the same shape the executor's `output_columns` /
/// `projected_columns` produce, so a CTE / derived table's recorded columns match
/// the rows the engine materializes. `SELECT *` is the schema columns in order; a
/// named list resolves each against the schema's columns and the provenance
/// pseudo-columns ([STL-247]).
fn projected_header(schema: &TableSchema, projection: &Projection) -> Vec<(String, LogicalType)> {
    let mut addressable: Vec<(String, LogicalType)> = schema
        .columns()
        .iter()
        .map(|c| (c.name().to_owned(), c.ty()))
        .collect();
    let n_schema = addressable.len();
    addressable.extend(
        provenance::PSEUDO_COLUMNS
            .iter()
            .map(|(name, ty)| ((*name).to_owned(), *ty)),
    );
    match projection {
        Projection::All => addressable[..n_schema].to_vec(),
        Projection::Items(items) => items
            .iter()
            .map(|item| projection_output_column(item, &addressable))
            .collect(),
    }
}

/// The `(output name, type)` a projection item contributes to the result header
/// ([STL-303]): a column item resolves its type from the addressable set; a
/// computed expression / scalar subquery carries its own resolved type. The binder
/// has already validated a column item resolves, so the lookup never misses.
fn projection_output_column(
    item: &ProjectionItem,
    addressable: &[(String, LogicalType)],
) -> (String, LogicalType) {
    let ty = match &item.value {
        ProjectionValue::Column(source) => addressable
            .iter()
            .find(|(n, _)| n == source)
            .map(|(_, ty)| *ty)
            .expect("bind validated the projected column exists"),
        ProjectionValue::Computed { ty, .. } | ProjectionValue::Subquery { ty, .. } => *ty,
    };
    (item.name.clone(), ty)
}

/// Strip a statement's `WHERE` filter, for statement-level `Describe` ([STL-212]).
///
/// A prepared `SELECT` is described *before* `Bind`, so its `$1 … $n` parameters
/// have no values yet — and a parameter most often lives in the `WHERE`
/// (`… WHERE k = $1`), which the `WHERE` binder would try (and fail) to fold
/// against the column type. But a query's output column shape is a function of its
/// projection and the schema *only*; the filter cannot add, remove, or retype a
/// result column. So the Describe path binds a copy with every `WHERE` removed —
/// both the executor-glue `<column> = <literal>` predicate (a [`Select`]'s
/// `selection`) and a lifted `PERIOD(...)` predicate
/// ([`Temporal::period_predicate`](crate::Temporal::period_predicate)) — letting a
/// parameterized read describe its row shape with no bound values. The
/// `FOR … AS OF` qualifiers are kept: they select the *schema version* the columns
/// resolve under, which the shape does depend on.
#[must_use]
pub fn without_filter(stmt: &Statement) -> Statement {
    let mut out = stmt.clone();
    out.temporal.period_predicate = None;
    if let Some(SqlStatement::Query(query)) = out.sql_mut() {
        strip_where(&mut query.body);
    }
    out
}

/// Clear the `WHERE` selection of every `SELECT` reached from a set expression —
/// the top-level query, a parenthesized inner query, and each arm of a set
/// operation. The projection and `FROM` are untouched, so the bound shape is
/// unchanged.
fn strip_where(set: &mut SetExpr) {
    match set {
        SetExpr::Select(select) => select.selection = None,
        SetExpr::Query(inner) => strip_where(&mut inner.body),
        SetExpr::SetOperation { left, right, .. } => {
            strip_where(left);
            strip_where(right);
        }
        _ => {}
    }
}

/// Whether a `SELECT` is an aggregate query — it carries a non-empty `GROUP BY`,
/// or any projected item is an aggregate function call. Purely syntactic (no
/// catalog), so it gates binding before name resolution.
fn is_aggregate_query(select: &Select) -> bool {
    let grouped = matches!(
        &select.group_by,
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty()
    );
    // A `HAVING` implies aggregation even without a `GROUP BY` or a projected
    // aggregate (`SELECT … FROM t HAVING COUNT(*) > 0` is one whole-table group),
    // so it routes through the aggregate path where `HAVING` binds — rather than
    // being silently dropped on the plain-projection path ([STL-265]).
    grouped || select.having.is_some() || select.projection.iter().any(projection_item_is_aggregate)
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
/// Finally binds the post-grouping `HAVING` filter, if any ([`bind_having`],
/// [STL-265]) — which may append an aggregate the SELECT list never projects.
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

    // The post-grouping `HAVING` filter ([STL-265]). It binds against the same
    // grouping columns and may reference an aggregate the SELECT list does not —
    // such an aggregate is appended to `aggregates` (computed, never emitted), so
    // `bind_having` takes the list by `&mut`.
    let having = select
        .having
        .as_ref()
        .map(|expr| bind_having(expr, &group_by, &mut aggregates, schema, table))
        .transpose()?;

    Ok(BoundAggregate {
        group_by,
        aggregates,
        items,
        having,
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

/// An aggregate call's validated shape ([`aggregate_call_shape`]): its kind, the
/// function name (for diagnostics), and its optional argument expression (`None`
/// for `COUNT(*)`), all borrowing the source `Expr`.
type AggregateCallShape<'a> = (AggregateFunc, &'a str, Option<&'a Expr>);

/// Validate an aggregate function call's shape ([STL-171]), returning its
/// [`AggregateCallShape`] — or `Ok(None)` if `expr` is not a function call at all
/// (so the caller treats it as a grouping column).
///
/// Everything beyond a plain single-argument aggregate — `DISTINCT`, `FILTER`, an
/// `OVER` window, `WITHIN GROUP`, a parametric call — is an error, not `None`,
/// since silently ignoring it would be a wrong answer. The single-table
/// ([`bind_aggregate_call`]) and join ([`bind_join_aggregate_call`]) paths share
/// this and differ only in how they resolve the argument column.
fn aggregate_call_shape(expr: &Expr) -> Result<Option<AggregateCallShape<'_>>, SelectError> {
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
    // column the caller resolves against its relation.
    let arg = match arg {
        FunctionArgExpr::Wildcard => {
            if func_kind != AggregateFunc::Count {
                return Err(SelectError::UnsupportedAggregate(format!(
                    "{name}(*) is not valid; only COUNT(*) takes a wildcard"
                )));
            }
            None
        }
        FunctionArgExpr::Expr(arg) => Some(arg),
        FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::WildcardWithOptions(_) => {
            return Err(SelectError::UnsupportedAggregate(format!(
                "{name}() does not support a qualified or option-bearing wildcard"
            )));
        }
    };

    Ok(Some((func_kind, name, arg)))
}

/// Bind one aggregate function call over a base table, or `Ok(None)` if `expr` is
/// not a function call (so the caller treats it as a grouping column) ([STL-171]).
/// The call's shape is validated by [`aggregate_call_shape`]; the argument column
/// resolves against the base-table `schema`.
fn bind_aggregate_call(
    expr: &Expr,
    schema: &TableSchema,
    table: &str,
) -> Result<Option<AggregateCall>, SelectError> {
    let Some((func, name, arg_expr)) = aggregate_call_shape(expr)? else {
        return Ok(None);
    };
    let arg = match arg_expr {
        None => None,
        Some(expr) => {
            let column = bare_column(expr).ok_or_else(|| {
                SelectError::UnsupportedAggregate(format!(
                    "{name}() supports a bare column argument only"
                ))
            })?;
            let idx = column_index(schema, column).ok_or_else(|| SelectError::UnknownColumn {
                table: table.to_owned(),
                column: column.to_owned(),
            })?;
            check_aggregate_arg_type(func, schema.columns()[idx].ty())?;
            Some(idx)
        }
    };
    Ok(Some(AggregateCall { func, arg }))
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

/// Bind a `HAVING <scalar> <compare> <scalar>` predicate over the grouped output
/// ([STL-265], richer predicates [STL-327]).
///
/// The shape mirrors the single-column `WHERE` ([`bind_where_predicate`]): the top
/// level is one of the six comparisons, and each side is a grouping column, an
/// aggregate call, an integer arithmetic of one, or a literal. A bare identifier
/// must be a grouping column (an ungrouped one is the Postgres grouping error,
/// [`SelectError::UngroupedColumn`] / SQLSTATE 42803); a function call is an
/// aggregate whose computed column the predicate filters on — appended to
/// `aggregates` (and so computed) if the SELECT list did not already request it.
///
/// Each side is typed independently ([`having_side_type`]): a pure-literal side
/// folds to the other side's type — the original single-anchor shape — and a
/// **two-anchor** comparison (`COUNT(*) > SUM(amount)`, `dept > COUNT(*)`) binds
/// when the two types are comparable ([`having_compare_types`]). A `FLOAT8` `AVG`
/// operand now compares too, the evaluator promoting the numeric pair ([STL-327]).
fn bind_having(
    expr: &Expr,
    group_by: &[usize],
    aggregates: &mut Vec<AggregateCall>,
    schema: &TableSchema,
    table: &str,
) -> Result<BoundHaving, SelectError> {
    let Expr::BinaryOp { left, op, right } = unwrap_nested(expr) else {
        return Err(SelectError::UnsupportedHaving(
            "the HAVING is not a comparison".to_owned(),
        ));
    };
    let Some(compare) = compare_op(op) else {
        return Err(SelectError::UnsupportedHaving(format!(
            "operator `{op}` is not a comparison"
        )));
    };
    // Type each side independently — the grouping column or aggregate it anchors
    // on, or `None` for a pure literal. The pair fixes the per-side fold type and
    // validates the comparison (a literal side folds to the other; two anchors must
    // be comparable — both numeric, or the same evaluable type).
    let left_ty = having_side_type(left, group_by, schema, table)?;
    let right_ty = having_side_type(right, group_by, schema, table)?;
    let (left_fold, right_fold) = having_compare_types(left_ty, right_ty)?;
    Ok(BoundHaving {
        left: bind_having_scalar(left, left_fold, group_by, aggregates, schema, table)?,
        op: compare,
        right: bind_having_scalar(right, right_fold, group_by, aggregates, schema, table)?,
    })
}

/// The type anchoring one side of a `HAVING` comparison — the grouping column or
/// aggregate it references — or `None` when the side is a pure literal (folding to
/// the *other* side's type) ([STL-265], [STL-327]). Descends parentheses and
/// integer arithmetic; a side mixing two distinct anchor types (an `int4` column
/// and an `int8` aggregate in one arithmetic) has no single type and is rejected.
fn having_side_type(
    expr: &Expr,
    group_by: &[usize],
    schema: &TableSchema,
    table: &str,
) -> Result<Option<LogicalType>, SelectError> {
    let mut types = Vec::new();
    collect_having_types(expr, group_by, schema, table, &mut types)?;
    having_single_type(types)
}

/// Resolve the per-side fold types of a `HAVING` comparison and validate it
/// ([STL-265], [STL-327]). A pure-literal side (type `None`) folds to the other
/// side's anchor type — the single-anchor shape. With two anchors the types must be
/// comparable: both numeric (`int4`/`int8`/`float8`, which the evaluator promotes
/// to a common type) or the same evaluable type. Two literals anchor nothing.
fn having_compare_types(
    left: Option<LogicalType>,
    right: Option<LogicalType>,
) -> Result<(LogicalType, LogicalType), SelectError> {
    match (left, right) {
        (None, None) => Err(SelectError::UnsupportedHaving(
            "the HAVING references no aggregate or grouping column".to_owned(),
        )),
        (Some(ty), None) | (None, Some(ty)) => Ok((ty, ty)),
        (Some(l), Some(r)) if having_types_comparable(l, r) => Ok((l, r)),
        (Some(l), Some(r)) => Err(SelectError::UnsupportedHaving(format!(
            "a HAVING cannot compare a {l} value to a {r} value"
        ))),
    }
}

/// Reduce a side's collected anchor types to its single type, erroring if the side
/// mixes two distinct ones; `Ok(None)` for a side with no anchor (a pure literal).
fn having_single_type(
    types: impl IntoIterator<Item = LogicalType>,
) -> Result<Option<LogicalType>, SelectError> {
    let mut single: Option<LogicalType> = None;
    for ty in types {
        match single {
            Some(prev) if prev != ty => {
                return Err(SelectError::UnsupportedHaving(format!(
                    "a HAVING operand mixing {prev} and {ty} values is not supported"
                )));
            }
            _ => single = Some(ty),
        }
    }
    Ok(single)
}

/// Whether two `HAVING` operand types may be compared: both numeric (the evaluator
/// promotes them to a common type, [STL-327]) or the identical evaluable type.
fn having_types_comparable(left: LogicalType, right: LogicalType) -> bool {
    (is_numeric_type(left) && is_numeric_type(right)) || left == right
}

/// The numeric types a `HAVING` comparison promotes across (`int4`/`int8`/`float8`).
const fn is_numeric_type(ty: LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::Int4 | LogicalType::Int8 | LogicalType::Float8
    )
}

/// Collect the type of each grouping-column / aggregate a `HAVING` operand
/// references, descending through parentheses and integer arithmetic ([STL-265]). A
/// bare identifier must be a grouping column (else the grouping error); a function
/// call is an aggregate (its shape validated, its result type computed); a literal
/// contributes nothing. Does **not** descend into an aggregate's argument — that
/// column need not be grouped (`SUM(price)` over an ungrouped `price` is legal), so
/// it is not itself an anchor.
fn collect_having_types(
    expr: &Expr,
    group_by: &[usize],
    schema: &TableSchema,
    table: &str,
    out: &mut Vec<LogicalType>,
) -> Result<(), SelectError> {
    match unwrap_nested(expr) {
        Expr::Identifier(id) => {
            let index = having_group_column(&id.value, group_by, schema, table)?;
            out.push(schema.columns()[index].ty());
        }
        function @ Expr::Function(_) => {
            let call = bind_aggregate_call(function, schema, table)?
                .expect("an Expr::Function binds to an aggregate call or errors");
            let arg_ty = call.arg.map(|i| schema.columns()[i].ty());
            out.push(call.func.result_type(arg_ty));
        }
        Expr::BinaryOp { left, op, right } => {
            if arith_op(op).is_none() {
                return Err(SelectError::UnsupportedHaving(format!(
                    "operator `{op}` is not supported in a HAVING comparand"
                )));
            }
            collect_having_types(left, group_by, schema, table, out)?;
            collect_having_types(right, group_by, schema, table, out)?;
        }
        // A literal (or any other leaf) contributes no type.
        _ => {}
    }
    Ok(())
}

/// Resolve a bare `HAVING` column to its schema index, requiring it be a grouping
/// column ([STL-265]). An unknown name is [`SelectError::UnknownColumn`]; a real
/// column that is not in `GROUP BY` is the Postgres grouping error
/// [`SelectError::UngroupedColumn`] (SQLSTATE 42803) — it has no single value per
/// group, exactly as a non-aggregated SELECT-list column would.
fn having_group_column(
    name: &str,
    group_by: &[usize],
    schema: &TableSchema,
    table: &str,
) -> Result<usize, SelectError> {
    let index = column_index(schema, name).ok_or_else(|| SelectError::UnknownColumn {
        table: table.to_owned(),
        column: name.to_owned(),
    })?;
    if group_by.contains(&index) {
        Ok(index)
    } else {
        Err(SelectError::UngroupedColumn {
            table: table.to_owned(),
            column: name.to_owned(),
        })
    }
}

/// Bind one side of a `HAVING` comparison to a [`HavingScalar`] ([STL-265]): a
/// grouping column (by its `group_by` position), an aggregate (registered into
/// `aggregates`, deduplicated, returning its index), an integer arithmetic of
/// them, or a literal folded to the anchor type. Arithmetic is integer-only (the
/// evaluator computes `+ - * / %` over `INT4`/`INT8`), so it is rejected over a
/// non-integer anchor at bind time rather than erroring per group.
fn bind_having_scalar(
    expr: &Expr,
    anchor_ty: LogicalType,
    group_by: &[usize],
    aggregates: &mut Vec<AggregateCall>,
    schema: &TableSchema,
    table: &str,
) -> Result<HavingScalar, SelectError> {
    match unwrap_nested(expr) {
        Expr::Identifier(id) => {
            let index = having_group_column(&id.value, group_by, schema, table)?;
            let pos = group_by
                .iter()
                .position(|&g| g == index)
                .expect("having_group_column accepts only a grouping column");
            Ok(HavingScalar::Group(pos))
        }
        function @ Expr::Function(_) => {
            let call = bind_aggregate_call(function, schema, table)?
                .expect("an Expr::Function binds to an aggregate call or errors");
            Ok(HavingScalar::Aggregate(register_aggregate(
                aggregates, call,
            )))
        }
        Expr::BinaryOp { left, op, right } => {
            let Some(arith) = arith_op(op) else {
                return Err(SelectError::UnsupportedHaving(format!(
                    "operator `{op}` is not supported in a HAVING comparand"
                )));
            };
            if !matches!(anchor_ty, LogicalType::Int4 | LogicalType::Int8) {
                return Err(SelectError::UnsupportedHaving(format!(
                    "arithmetic in a HAVING needs an integer aggregate or grouping column \
                     (the anchor is {anchor_ty})"
                )));
            }
            Ok(HavingScalar::Arith {
                op: arith,
                left: Box::new(bind_having_scalar(
                    left, anchor_ty, group_by, aggregates, schema, table,
                )?),
                right: Box::new(bind_having_scalar(
                    right, anchor_ty, group_by, aggregates, schema, table,
                )?),
            })
        }
        leaf => {
            let value = fold::fold_scalar(leaf, anchor_ty).map_err(|err| {
                SelectError::UnsupportedHaving(having_fold_reason(&err, anchor_ty))
            })?;
            Ok(HavingScalar::Literal(value))
        }
    }
}

/// Add an aggregate call to the plan's compute list, returning its index — reusing
/// an identical call already present (the SELECT list's, or an earlier `HAVING`
/// operand's) so a `HAVING` aggregate the query also projects is computed once
/// ([STL-265]).
fn register_aggregate(aggregates: &mut Vec<AggregateCall>, call: AggregateCall) -> usize {
    if let Some(index) = aggregates.iter().position(|existing| *existing == call) {
        return index;
    }
    aggregates.push(call);
    aggregates.len() - 1
}

/// Render a literal-fold failure in a `HAVING` as the reason carried by
/// [`SelectError::UnsupportedHaving`] — like [`predicate_reason`] but the anchor may
/// be an aggregate, so the message names the target type rather than a column.
fn having_fold_reason(err: &FoldError, ty: LogicalType) -> String {
    match err {
        FoldError::Null => format!("NULL cannot be compared to a {ty} value"),
        FoldError::TypeMismatch { found } => format!("{found} is not a {ty} value"),
        FoldError::BadLiteral { literal, reason } => {
            let detail = reason.map(|r| format!(" ({r})")).unwrap_or_default();
            format!("{literal:?} is not a valid {ty}{detail}")
        }
        FoldError::UnsupportedType(ty) => {
            format!("comparing a {ty} value to a literal is not supported yet")
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

/// Resolve a `WHERE` column name to its addressable index and type ([STL-247]).
///
/// A user column resolves to its schema index (`0` the business key). A name not
/// in the schema is matched against the provenance pseudo-columns
/// ([`provenance::PSEUDO_COLUMNS`]); a match resolves to the **virtual index**
/// `schema.columns().len() + k` — the fixed position the executor materializes
/// the k-th provenance fact at, after the table's own columns — with the
/// pseudo-column's type. `None` for a name that is neither, which the caller
/// reports as [`SelectError::UnknownColumn`]. The schema is searched first, so a
/// user column never loses to a like-named pseudo-column.
fn resolve_filter_column(schema: &TableSchema, name: &str) -> Option<(usize, LogicalType)> {
    if let Some(index) = column_index(schema, name) {
        return Some((index, schema.columns()[index].ty()));
    }
    let n_schema = schema.columns().len();
    provenance::PSEUDO_COLUMNS
        .iter()
        .position(|(n, _)| *n == name)
        .map(|k| (n_schema + k, provenance::PSEUDO_COLUMNS[k].1))
}

/// A `JOIN` chain in the `FROM` clause, or `None` for any other shape (a single
/// table, or a comma join — each handled or rejected elsewhere).
///
/// Returns the [`TableWithJoins`] — its `relation` the leftmost (seed) input and
/// its `joins` the left-deep chain folded onto it ([STL-323]) — for exactly one
/// `FROM` item carrying at least one `JOIN`. Zero joins or a comma-separated
/// (multi-item) `FROM` returns `None`, so the single-table path reports it.
fn detect_join(select: &Select) -> Option<&TableWithJoins> {
    let [from] = select.from.as_slice() else {
        return None;
    };
    (!from.joins.is_empty()).then_some(from)
}

/// Bind a `JOIN`-chain `SELECT` into a [`BoundSelect`] carrying a [`BoundJoin`]
/// ([STL-172], [STL-264], [STL-323]).
///
/// Resolves every input at the statement's `(sys, valid)` snapshot — the single
/// per-statement pin every input reads at (docs/16 §8: a temporal join takes one
/// consistent snapshot across the query) — then folds the chain **left-deep**: the
/// leftmost input seeds the accumulated output, and each `JOIN` lowers to a
/// [`JoinType`] and binds its `ON acc.col = new.col` equi-condition (one key from
/// the accumulated output, one from the freshly joined input). The growing
/// **addressable output** ([`JoinScope`]) is the surface the rest of the `SELECT`
/// binds against — the projection, a `WHERE` ([STL-213]), a `GROUP BY` + aggregates
/// with an optional `HAVING` ([STL-171], [STL-327]), and the `DISTINCT` / `ORDER BY`
/// / `OFFSET` / `LIMIT` tail ([STL-263]) — each resolving its (bare or qualified)
/// column references to an addressable index, so the executor runs the same
/// downstream pipeline a single-table read does ([STL-264]). A two-table join is the
/// one-step chain.
///
/// A `FOR … AS OF` qualifier on *either* axis is honored ([STL-243]): it is the
/// statement-level pin lifted off the token stream ([STL-162]), applied to every
/// input. (`resolve_snapshots` already rejects two qualifiers on one axis, so a
/// per-input "different instant per table" join cannot reach here.) A `FOR
/// VALID_TIME AS OF` pin is meaningful only where every input has a valid axis, so
/// a system-only side under one is rejected, mirroring the single-table
/// [`SelectError::ValidTimeUnsupported`]. A period predicate over the join and the
/// `RIGHT` / `FULL` / `CROSS` / non-equi joins ([`join_kind_and_constraint`],
/// [STL-270]) stay rejected (each a tracked follow-up); join reordering /
/// cost-based planning is out of scope — the chain runs in syntactic, left-deep
/// order.
// Eight inputs because the join binds the whole `SELECT` over every relation: the
// statement (temporal qualifiers) and its `query` / `select` halves, the catalog
// `ctx`, the `from` relations, the CTE `sigs` in scope, and the resolved
// `(snapshot, valid_snapshot)` pin every input reads at. Bundling any subset only
// moves the plumbing without clarifying it.
#[allow(clippy::too_many_arguments)]
fn bind_join<'a>(
    stmt: &Statement,
    ctx: &'a BindContext<'a>,
    query: &'a Query,
    select: &'a Select,
    from: &'a TableWithJoins,
    sigs: &[CteSig],
    snapshot: SystemTimeMicros,
    valid_snapshot: Option<SystemTimeMicros>,
) -> Result<(BoundSelect, Vec<BoundCte>), SelectError> {
    // A `FOR … AS OF` on either axis *is* threaded ([STL-243]); a period predicate
    // over a join stays a follow-up: rejected, never dropped.
    if stmt.temporal.period_predicate.is_some() {
        return Err(SelectError::UnsupportedJoin(
            "a period predicate over a JOIN".to_owned(),
        ));
    }
    // Resolve every input up front — the seed (`from.relation`) then each chained
    // `JOIN`'s relation. Any input may be a base table, a CTE in scope, or a derived
    // table ([STL-242]); a derived input is bound into a single-use CTE the query
    // must register. Every input resolves at the *statement* snapshot, so a
    // `FOR SYSTEM_TIME AS OF s` travels each schema to the same instant.
    let mut sides: Vec<SideSchema<'a>> = Vec::with_capacity(from.joins.len() + 1);
    let mut ctes: Vec<BoundCte> = Vec::new();
    let (seed, seed_cte) = resolve_join_side(&from.relation, ctx, snapshot, sigs)?;
    sides.push(seed);
    ctes.extend(seed_cte);
    for join_ast in &from.joins {
        let (side, side_cte) = resolve_join_side(&join_ast.relation, ctx, snapshot, sigs)?;
        sides.push(side);
        ctes.extend(side_cte);
    }

    // A `FOR VALID_TIME AS OF v` pin only travels an input that has a valid axis; a
    // system-only side (a base table without `VALID TIME`, or a CTE / derived
    // table, whose ephemeral schema is always system-only) has none, so reject
    // rather than silently ignore the pin on that side ([STL-243], docs/16 §8).
    if valid_snapshot.is_some() {
        for side in &sides {
            if !side.schema.temporal().valid_time_enabled() {
                return Err(SelectError::ValidTimeUnsupported {
                    table: side.table.to_owned(),
                });
            }
        }
    }

    // Fold the chain left-deep. `scope` accumulates the addressable output; each
    // step binds its `ON` condition against (the accumulated output) + (the new
    // input), then — for an `INNER` / `LEFT` join — widens the scope with the new
    // input's columns (a `SEMI` / `ANTI` step keeps only the accumulated left).
    let mut scope = JoinScope::seed(&sides[0]);
    let mut steps: Vec<BoundJoinStep> = Vec::with_capacity(from.joins.len());
    for (i, join_ast) in from.joins.iter().enumerate() {
        let (join_type, constraint) = join_kind_and_constraint(&join_ast.join_operator)?;
        let new_side = &sides[i + 1];
        let (left_key, right_key) = bind_step_condition(constraint, &scope, new_side)?;
        steps.push(BoundJoinStep {
            join_type,
            right: bound_side(new_side),
            left_key,
            right_key,
        });
        scope.push(new_side, join_type);
    }

    // An aggregate query (`GROUP BY`, or an aggregate in the SELECT list) replaces
    // the plain projection with a grouped plan that names its own output columns;
    // otherwise the projection selects addressable indices. Detection is syntactic,
    // so it runs before column resolution, exactly as the single-table path.
    let aggregate = if is_aggregate_query(select) {
        Some(bind_join_aggregate(select, &scope)?)
    } else {
        None
    };
    let (output, columns) = match &aggregate {
        // The projection is unused on the aggregate path; the header is the
        // aggregate's. Keeping `columns` here lets the caller read one header field.
        Some(agg) => (Vec::new(), agg.columns.clone()),
        None => bind_join_projection(select, &scope)?,
    };

    // The result-shaping tail and the `WHERE`, bound over the addressable output
    // exactly as the single-table path binds them over a schema — only column
    // resolution differs (`JoinScope` vs a base-table schema).
    let distinct = bind_distinct(select)?;
    let order_by = bind_join_order_by(
        query,
        &scope,
        distinct,
        aggregate.as_ref(),
        &output,
        &columns,
    )?;
    let (limit, offset) = bind_limit_offset(query)?;
    let filter = select
        .selection
        .as_ref()
        .map(|expr| bind_join_filter(expr, &scope))
        .transpose()?;

    let bound = BoundSelect {
        // The single-table relation fields are unused on the join path (see
        // `BoundSelect`): the executor routes to the join plan, never reading these.
        table: String::new(),
        schema_id: sides[0].schema.schema_id(),
        snapshot,
        valid_snapshot,
        // A range scan over a join is rejected at bind time (see the join path in
        // `bind_select_scoped`), so the join plan never carries one on either axis.
        system_range: None,
        valid_range: None,
        projection: Projection::All,
        filter,
        period_filter: None,
        subquery_filter: None,
        aggregate,
        join: Some(BoundJoin {
            left: bound_side(&sides[0]),
            steps,
            output,
            columns,
        }),
        // The `WITH` relations are prepended by the caller; a derived join side's
        // CTE is returned alongside (`ctes`) for the caller to register.
        ctes: Vec::new(),
        relation_columns: None,
        distinct,
        order_by,
        offset,
        limit,
    };
    Ok((bound, ctes))
}

/// Resolve one `JOIN` side ([STL-242]): a CTE in `sigs` (which shadows a catalog
/// table of the same name), a base table at `snapshot`, or a `(SELECT …) AS d`
/// derived table — the last bound into a single-use [`BoundCte`] returned for the
/// query to register.
fn resolve_join_side<'a>(
    factor: &'a TableFactor,
    ctx: &'a BindContext<'a>,
    snapshot: SystemTimeMicros,
    sigs: &[CteSig],
) -> Result<(SideSchema<'a>, Option<BoundCte>), SelectError> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table = join_table_name(name)?;
            let alias = alias.as_ref().map(|a| a.name.value.as_str());
            if let Some(sig) = sigs.iter().rev().find(|s| s.name == table) {
                return Ok((
                    SideSchema {
                        table,
                        alias,
                        schema: RelationSchema::Owned(sig.schema.clone()),
                    },
                    None,
                ));
            }
            let schema = resolve_join_table(ctx.catalog, table, snapshot)?;
            Ok((
                SideSchema {
                    table,
                    alias,
                    schema: RelationSchema::Borrowed(schema),
                },
                None,
            ))
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            ..
        } => {
            if *lateral {
                return Err(SelectError::Cte(
                    "a LATERAL derived table is not supported".to_owned(),
                ));
            }
            let Some(alias) = alias else {
                return Err(SelectError::Cte(
                    "a derived table in a JOIN must have an alias".to_owned(),
                ));
            };
            let derived_ctx = BindContext {
                snapshot,
                catalog: ctx.catalog,
            };
            let (cte, schema) = bind_named_subquery(subquery, alias, &derived_ctx, sigs)?;
            // The alias is the side's exposed name (it *is* the relation name the
            // engine looks up in the materialization), so a `d.col` qualifier
            // matches it through `table`; there is no separate alias to carry.
            Ok((
                SideSchema {
                    table: alias.name.value.as_str(),
                    alias: None,
                    schema: RelationSchema::Owned(schema),
                },
                Some(cte),
            ))
        }
        _ => Err(SelectError::UnsupportedJoin(
            "a non-table relation (subquery / derived table) in a JOIN".to_owned(),
        )),
    }
}

/// The single, unqualified identifier of a `JOIN` table name, mapping a
/// non-identifier / schema-qualified name to the [`SelectError::UnsupportedJoin`]
/// the join path reports.
fn join_table_name(name: &sqlparser::ast::ObjectName) -> Result<&str, SelectError> {
    match name.0.as_slice() {
        [part] => part.as_ident().map(|id| id.value.as_str()).ok_or_else(|| {
            SelectError::UnsupportedJoin("a non-identifier table name in a JOIN".to_owned())
        }),
        _ => Err(SelectError::UnsupportedJoin(
            "a schema-qualified table name in a JOIN".to_owned(),
        )),
    }
}

/// A join side during binding: its table name, optional alias, and resolved
/// schema. The alias and table name are both valid qualifiers for the side's
/// columns (`t.c` or `alias.c`). The schema is a base table's (borrowed) or a
/// CTE / derived table's ephemeral one (owned) — [`RelationSchema`] derefs to
/// either ([STL-242]).
struct SideSchema<'a> {
    table: &'a str,
    alias: Option<&'a str>,
    schema: RelationSchema<'a>,
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
fn table_ref(factor: &TableFactor) -> Result<TableRef<'_>, SelectError> {
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

/// Bind one chain step's `ON` constraint to `(left_key, right_key)` — a flat
/// addressable index into the accumulated output and a schema index into the freshly
/// joined input ([STL-323]).
///
/// The single `acc.col = new.col` equality must relate one column of the
/// accumulated output (the chain so far, addressed through `scope`) and one of
/// `new_side`; either operand order is fine. The two key columns must share a type.
/// For a two-table join the accumulated output is just the seed input, so this
/// reduces to STL-172's `left.col = right.col`.
fn bind_step_condition(
    constraint: &JoinConstraint,
    scope: &JoinScope,
    new_side: &SideSchema,
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
    // Resolve each operand against (the accumulated output) + (the new input), the
    // new input addressed past the accumulated width. Exactly one operand must come
    // from each: one flat index below the boundary (an accumulated column), one at
    // or above it (a column of the new input).
    let boundary = scope.width();
    let combined = scope.with_new_side(new_side);
    let a = resolve_scope_column(lhs, &combined)?;
    let b = resolve_scope_column(rhs, &combined)?;
    let (left_key, right_flat) = match (a < boundary, b < boundary) {
        (true, false) => (a, b),
        (false, true) => (b, a),
        _ => {
            return Err(SelectError::JoinCondition(
                "the ON equality must relate a column of each joined table".to_owned(),
            ));
        }
    };
    let right_key = right_flat - boundary;
    let left_ty = scope.columns()[left_key].1;
    let right_ty = new_side.schema.columns()[right_key].ty();
    if left_ty != right_ty {
        return Err(SelectError::JoinColumnTypeMismatch {
            left_column: scope.columns()[left_key].0.clone(),
            right_column: new_side.schema.columns()[right_key].name().to_owned(),
            left_type: left_ty,
            right_type: right_ty,
        });
    }
    Ok((left_key, right_key))
}

/// Resolve a column reference (a bare `c` or qualified `t.c`) against an ordered
/// list of join inputs to its **flat addressable index** ([STL-323]).
///
/// A bare column must be in exactly one input (in several is
/// [`SelectError::AmbiguousColumn`], in none [`SelectError::UnknownJoinColumn`]). A
/// qualified `t.c`'s qualifier must name exactly one input (by table name or alias),
/// and `c` must be a column of it. Generalizes STL-172's two-side resolution to the
/// N inputs a left-deep chain addresses.
fn resolve_scope_column(expr: &Expr, sides: &[ScopeSide]) -> Result<usize, SelectError> {
    match expr {
        Expr::Nested(inner) => resolve_scope_column(inner, sides),
        Expr::Identifier(id) => {
            let mut found: Option<usize> = None;
            for side in sides {
                if let Some(i) = side.schema.column_index(&id.value) {
                    if found.is_some() {
                        return Err(SelectError::AmbiguousColumn {
                            column: id.value.clone(),
                        });
                    }
                    found = Some(side.offset + i);
                }
            }
            found.ok_or_else(|| SelectError::UnknownJoinColumn {
                column: id.value.clone(),
            })
        }
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return Err(SelectError::UnknownJoinColumn {
                    column: compound_name(parts),
                });
            };
            let (q, c) = (qualifier.value.as_str(), column.value.as_str());
            let qualified = || format!("{q}.{c}");
            let mut matched: Option<&ScopeSide> = None;
            for side in sides {
                if side.schema.qualifier_matches(q) {
                    if matched.is_some() {
                        return Err(SelectError::AmbiguousColumn {
                            column: qualified(),
                        });
                    }
                    matched = Some(side);
                }
            }
            let side = matched.ok_or_else(|| SelectError::UnknownJoinColumn {
                column: qualified(),
            })?;
            side.schema
                .column_index(c)
                .map(|i| side.offset + i)
                .ok_or_else(|| SelectError::UnknownJoinColumn {
                    column: qualified(),
                })
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

/// One addressable input in a [`JoinScope`]: a join side and the flat index of its
/// first column in the accumulated output.
#[derive(Clone, Copy)]
struct ScopeSide<'a> {
    schema: &'a SideSchema<'a>,
    /// The flat addressable index of this input's first column.
    offset: usize,
}

/// The accumulated output of a `JOIN` chain, as the surface a projection / `WHERE`
/// / `GROUP BY` / `ORDER BY` resolves its column references against ([STL-264],
/// [STL-323]).
///
/// The **addressable** columns are the seed input's, then each `INNER` / `LEFT`
/// step's right input's, in the chain's left-deep order (a `SEMI` / `ANTI` step
/// keeps only the accumulated left, so its right input is *not* addressable). A
/// reference (bare `c` or qualified `t.c`) resolves through [`resolve_scope_column`]
/// to a single flat addressable index, so the clause binders address one flat row,
/// exactly as the single-table path addresses a schema row.
struct JoinScope<'a> {
    /// The addressable inputs, in output order, each with its flat column offset.
    sides: Vec<ScopeSide<'a>>,
    /// Inputs dropped by a `SEMI` / `ANTI` step, kept only so a reference to one
    /// yields the "exposes only its left" diagnostic rather than a bare "unknown
    /// column".
    dropped: Vec<&'a SideSchema<'a>>,
    /// The flat addressable columns `(name, type)`, in output order.
    columns: Vec<(String, LogicalType)>,
}

impl<'a> JoinScope<'a> {
    /// The scope of a chain's leftmost (seed) input alone — every column addressable.
    fn seed(side: &'a SideSchema<'a>) -> Self {
        let columns = side
            .schema
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.ty()))
            .collect();
        Self {
            sides: vec![ScopeSide {
                schema: side,
                offset: 0,
            }],
            dropped: Vec::new(),
            columns,
        }
    }

    /// Fold a freshly joined input into the scope: an `INNER` / `LEFT` step widens
    /// the addressable output with the input's columns; a `SEMI` / `ANTI` step keeps
    /// only the accumulated left, dropping the input (retained for diagnostics).
    fn push(&mut self, side: &'a SideSchema<'a>, join_type: JoinType) {
        if join_type.keeps_right() {
            let offset = self.columns.len();
            self.columns.extend(
                side.schema
                    .columns()
                    .iter()
                    .map(|c| (c.name().to_owned(), c.ty())),
            );
            self.sides.push(ScopeSide {
                schema: side,
                offset,
            });
        } else {
            self.dropped.push(side);
        }
    }

    /// The number of addressable columns — the boundary a chain step's new input is
    /// addressed past.
    const fn width(&self) -> usize {
        self.columns.len()
    }

    /// The addressable inputs plus `new_side` appended past the current width — the
    /// scope a chain step's `ON` condition resolves against ([`bind_step_condition`]).
    fn with_new_side(&self, new_side: &'a SideSchema<'a>) -> Vec<ScopeSide<'a>> {
        let mut sides = self.sides.clone();
        sides.push(ScopeSide {
            schema: new_side,
            offset: self.width(),
        });
        sides
    }

    /// The addressable output columns `(name, type)`, in order.
    fn columns(&self) -> &[(String, LogicalType)] {
        &self.columns
    }

    /// Resolve a column reference (bare or qualified) to its addressable index and
    /// type. A reference naming an input dropped by a `SEMI` / `ANTI` step is
    /// rejected with a pointed diagnostic rather than addressing a column the output
    /// omits.
    fn resolve(&self, expr: &Expr) -> Result<(usize, LogicalType), SelectError> {
        match resolve_scope_column(expr, &self.sides) {
            Ok(index) => Ok((index, self.columns[index].1)),
            Err(SelectError::UnknownJoinColumn { .. }) if self.names_dropped(expr) => {
                Err(SelectError::UnsupportedJoinProjection(
                    "a SEMI/ANTI join exposes only its left table's columns".to_owned(),
                ))
            }
            Err(e) => Err(e),
        }
    }

    /// Whether `expr` names a column of an input dropped by a `SEMI` / `ANTI` step —
    /// a bare column in one, or a qualifier naming one.
    fn names_dropped(&self, expr: &Expr) -> bool {
        match unwrap_nested(expr) {
            Expr::Identifier(id) => self
                .dropped
                .iter()
                .any(|s| s.column_index(&id.value).is_some()),
            Expr::CompoundIdentifier(parts) => matches!(
                parts.as_slice(),
                [qualifier, _] if self.dropped.iter().any(|s| s.qualifier_matches(&qualifier.value))
            ),
            _ => false,
        }
    }

    /// A label naming the joined relations, for an ungrouped-column diagnostic.
    fn relation_label(&self) -> String {
        self.sides
            .iter()
            .map(|s| s.schema.table)
            .collect::<Vec<_>>()
            .join(" JOIN ")
    }
}

/// A bound join projection: the output-column **addressable indices** and their
/// aligned `(name, type)` result header.
type JoinProjection = (Vec<usize>, Vec<(String, LogicalType)>);

/// Bind a join's projection to addressable output-column indices and the result
/// header ([STL-264]). `SELECT *` is every addressable column in order (the left
/// side, then the right for an `INNER` / `LEFT` join); a named list resolves each
/// item (bare or qualified) through the [`JoinScope`]. The header uses each
/// column's bare name, the Postgres `RowDescription` convention.
fn bind_join_projection(select: &Select, scope: &JoinScope) -> Result<JoinProjection, SelectError> {
    // `SELECT *` — every addressable column, in output order.
    if let [SelectItem::Wildcard(_)] = select.projection.as_slice() {
        let output: Vec<usize> = (0..scope.columns().len()).collect();
        return Ok((output, scope.columns().to_vec()));
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
        let (index, ty) = scope.resolve(expr)?;
        columns.push((scope.columns()[index].0.clone(), ty));
        output.push(index);
    }
    Ok((output, columns))
}

/// One column a `WHERE` over a join anchors on — its addressable index, type, and
/// name (for fold diagnostics). The join counterpart of [`FilterAnchor`].
struct JoinFilterAnchor {
    index: usize,
    ty: LogicalType,
    name: String,
}

/// Bind a `WHERE` over a join's output to a [`BoundPredicate`] ([STL-264]).
///
/// The join counterpart of [`bind_where_predicate`]: the same single-comparison,
/// one-anchor-column shape ([STL-213]) — six comparison operators, either side an
/// integer arithmetic of the anchor column or a literal folded to its type — but
/// each column reference (bare or qualified) resolves through the [`JoinScope`] to
/// an addressable index. A subquery / period `WHERE` over a join is not a shape the
/// v0.3 join path binds (its non-comparison form is rejected here).
fn bind_join_filter(expr: &Expr, scope: &JoinScope) -> Result<BoundPredicate, SelectError> {
    let Expr::BinaryOp { left, op, right } = unwrap_nested(expr) else {
        return Err(SelectError::UnsupportedPredicate(
            "the WHERE over a JOIN is not a comparison".to_owned(),
        ));
    };
    let Some(compare) = compare_op(op) else {
        return Err(SelectError::UnsupportedPredicate(format!(
            "operator `{op}` is not a comparison"
        )));
    };
    let anchor = join_filter_anchor(left, right, scope)?;
    Ok(BoundPredicate {
        left: bind_join_scalar(left, &anchor, scope)?,
        op: compare,
        right: bind_join_scalar(right, &anchor, scope)?,
    })
}

/// Resolve the one column a join `WHERE` comparison references to a
/// [`JoinFilterAnchor`]. Like [`filter_anchor`], a predicate with no column has no
/// type to anchor, and one referencing two distinct columns is a column-to-column
/// comparison — both unsupported.
fn join_filter_anchor(
    left: &Expr,
    right: &Expr,
    scope: &JoinScope,
) -> Result<JoinFilterAnchor, SelectError> {
    let mut refs: Vec<&Expr> = Vec::new();
    collect_join_columns(left, &mut refs);
    collect_join_columns(right, &mut refs);
    let mut anchor: Option<JoinFilterAnchor> = None;
    for r in refs {
        let (index, ty) = scope.resolve(r)?;
        match &anchor {
            Some(a) if a.index != index => {
                return Err(SelectError::UnsupportedPredicate(
                    "a column-to-column comparison over a JOIN is not supported".to_owned(),
                ));
            }
            Some(_) => {}
            None => {
                anchor = Some(JoinFilterAnchor {
                    index,
                    ty,
                    name: scope.columns()[index].0.clone(),
                });
            }
        }
    }
    anchor.ok_or_else(|| {
        SelectError::UnsupportedPredicate("the WHERE over a JOIN references no column".to_owned())
    })
}

/// Collect the column-reference operands (bare or qualified) a join `WHERE` side
/// references, descending through parentheses and arithmetic — the join
/// counterpart of [`collect_where_columns`], which collects bare names only.
fn collect_join_columns<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => out.push(expr),
        Expr::Nested(inner) => collect_join_columns(inner, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_join_columns(left, out);
            collect_join_columns(right, out);
        }
        _ => {}
    }
}

/// Bind one side of a join `WHERE` comparison to a [`BoundScalar`] — the anchor
/// column (by addressable index), an integer arithmetic of it, or a literal folded
/// to the anchor's type. The join counterpart of [`bind_scalar`].
fn bind_join_scalar(
    expr: &Expr,
    anchor: &JoinFilterAnchor,
    scope: &JoinScope,
) -> Result<BoundScalar, SelectError> {
    match unwrap_nested(expr) {
        column @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
            let (index, _) = scope.resolve(column)?;
            Ok(BoundScalar::Column(index))
        }
        Expr::BinaryOp { left, op, right } => {
            let Some(arith) = arith_op(op) else {
                return Err(SelectError::UnsupportedPredicate(format!(
                    "operator `{op}` is not supported in a WHERE comparand"
                )));
            };
            if !matches!(anchor.ty, LogicalType::Int4 | LogicalType::Int8) {
                return Err(SelectError::UnsupportedPredicate(format!(
                    "arithmetic in a WHERE needs an integer column, but `{}` is {}",
                    anchor.name, anchor.ty
                )));
            }
            Ok(BoundScalar::Arith {
                op: arith,
                left: Box::new(bind_join_scalar(left, anchor, scope)?),
                right: Box::new(bind_join_scalar(right, anchor, scope)?),
            })
        }
        leaf => {
            let value = fold::fold_scalar(leaf, anchor.ty).map_err(|err| {
                SelectError::UnsupportedPredicate(predicate_reason(&err, &anchor.name, anchor.ty))
            })?;
            Ok(BoundScalar::Literal(value))
        }
    }
}

/// Bind a `GROUP BY` + aggregate `SELECT` over a join's output into a
/// [`BoundAggregate`] ([STL-171], [STL-264]) — the join counterpart of
/// [`bind_aggregate`], resolving grouping / argument / passed-through columns
/// against the [`JoinScope`] (addressable indices) rather than a base-table schema.
fn bind_join_aggregate(select: &Select, scope: &JoinScope) -> Result<BoundAggregate, SelectError> {
    let group_by = bind_join_group_by(select, scope)?;

    let mut aggregates: Vec<AggregateCall> = Vec::new();
    let mut items: Vec<OutputItem> = Vec::new();
    let mut columns: Vec<(String, LogicalType)> = Vec::new();

    for item in &select.projection {
        let (expr, alias) = select_item(item)?;
        if let Some(call) = bind_join_aggregate_call(expr, scope)? {
            let arg_ty = call.arg.map(|i| scope.columns()[i].1);
            let ty = call.func.result_type(arg_ty);
            let name = alias.unwrap_or_else(|| call.func.default_name().to_owned());
            items.push(OutputItem::Aggregate(aggregates.len()));
            aggregates.push(call);
            columns.push((name, ty));
        } else {
            // Not an aggregate ⇒ it must be a grouping column passed through.
            let column = unwrap_nested(expr);
            if !matches!(column, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return Err(SelectError::UnsupportedAggregate(
                    "a SELECT item must be a grouping column or an aggregate".to_owned(),
                ));
            }
            let (index, ty) = scope.resolve(column)?;
            let group_pos = group_by.iter().position(|&g| g == index).ok_or_else(|| {
                SelectError::UngroupedColumn {
                    table: scope.relation_label(),
                    column: scope.columns()[index].0.clone(),
                }
            })?;
            let name = alias.unwrap_or_else(|| scope.columns()[index].0.clone());
            items.push(OutputItem::Group(group_pos));
            columns.push((name, ty));
        }
    }

    // The post-grouping `HAVING`, resolved through the same [`JoinScope`] the
    // grouping columns / aggregates bound against — the join counterpart of the
    // single-table `bind_having` ([STL-327]). It may append an aggregate the SELECT
    // list never projects, so it takes `aggregates` by `&mut`.
    let having = select
        .having
        .as_ref()
        .map(|expr| bind_join_having(expr, &group_by, &mut aggregates, scope))
        .transpose()?;

    Ok(BoundAggregate {
        group_by,
        aggregates,
        items,
        having,
        columns,
    })
}

/// Resolve a join query's `GROUP BY` columns to addressable indices ([STL-264]) —
/// the join counterpart of [`bind_group_by`].
fn bind_join_group_by(select: &Select, scope: &JoinScope) -> Result<Vec<usize>, SelectError> {
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
        let column = unwrap_nested(expr);
        if !matches!(column, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            return Err(SelectError::UnsupportedAggregate(
                "GROUP BY supports bare or qualified column names only".to_owned(),
            ));
        }
        let (index, ty) = scope.resolve(column)?;
        require_evaluable(ty, || {
            format!("GROUP BY on a {ty} column is not supported yet")
        })?;
        group_by.push(index);
    }
    Ok(group_by)
}

/// Bind one aggregate function call over a join's output, or `Ok(None)` if `expr`
/// is not a function call ([STL-264]). The call's shape is validated by the shared
/// [`aggregate_call_shape`]; the argument column resolves against the [`JoinScope`].
fn bind_join_aggregate_call(
    expr: &Expr,
    scope: &JoinScope,
) -> Result<Option<AggregateCall>, SelectError> {
    let Some((func, name, arg_expr)) = aggregate_call_shape(expr)? else {
        return Ok(None);
    };
    let arg = match arg_expr {
        None => None,
        Some(expr) => {
            let column = unwrap_nested(expr);
            if !matches!(column, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return Err(SelectError::UnsupportedAggregate(format!(
                    "{name}() supports a bare or qualified column argument only"
                )));
            }
            let (index, ty) = scope.resolve(column)?;
            check_aggregate_arg_type(func, ty)?;
            Some(index)
        }
    };
    Ok(Some(AggregateCall { func, arg }))
}

/// Bind a `HAVING` over a join's grouped output into a [`BoundHaving`] ([STL-327]) —
/// the join counterpart of [`bind_having`], resolving grouping columns / aggregates
/// through the [`JoinScope`] (qualified-name) rather than a base-table schema. The
/// predicate shape, per-side typing, and two-anchor / `FLOAT8` rules are identical
/// (the shared [`having_compare_types`]); only column resolution differs.
fn bind_join_having(
    expr: &Expr,
    group_by: &[usize],
    aggregates: &mut Vec<AggregateCall>,
    scope: &JoinScope,
) -> Result<BoundHaving, SelectError> {
    let Expr::BinaryOp { left, op, right } = unwrap_nested(expr) else {
        return Err(SelectError::UnsupportedHaving(
            "the HAVING is not a comparison".to_owned(),
        ));
    };
    let Some(compare) = compare_op(op) else {
        return Err(SelectError::UnsupportedHaving(format!(
            "operator `{op}` is not a comparison"
        )));
    };
    let left_ty = join_having_side_type(left, group_by, scope)?;
    let right_ty = join_having_side_type(right, group_by, scope)?;
    let (left_fold, right_fold) = having_compare_types(left_ty, right_ty)?;
    Ok(BoundHaving {
        left: bind_join_having_scalar(left, left_fold, group_by, aggregates, scope)?,
        op: compare,
        right: bind_join_having_scalar(right, right_fold, group_by, aggregates, scope)?,
    })
}

/// The type anchoring one side of a join `HAVING` comparison, or `None` for a pure
/// literal ([STL-327]) — the join counterpart of [`having_side_type`].
fn join_having_side_type(
    expr: &Expr,
    group_by: &[usize],
    scope: &JoinScope,
) -> Result<Option<LogicalType>, SelectError> {
    let mut types = Vec::new();
    collect_join_having_types(expr, group_by, scope, &mut types)?;
    having_single_type(types)
}

/// Collect the type of each grouping column / aggregate a join `HAVING` operand
/// references ([STL-327]) — the join counterpart of [`collect_having_types`],
/// resolving (bare or qualified) columns through the [`JoinScope`].
fn collect_join_having_types(
    expr: &Expr,
    group_by: &[usize],
    scope: &JoinScope,
    out: &mut Vec<LogicalType>,
) -> Result<(), SelectError> {
    match unwrap_nested(expr) {
        column @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
            let (_, ty) = join_having_group_column(column, group_by, scope)?;
            out.push(ty);
        }
        function @ Expr::Function(_) => {
            let call = bind_join_aggregate_call(function, scope)?
                .expect("an Expr::Function binds to an aggregate call or errors");
            let arg_ty = call.arg.map(|i| scope.columns()[i].1);
            out.push(call.func.result_type(arg_ty));
        }
        Expr::BinaryOp { left, op, right } => {
            if arith_op(op).is_none() {
                return Err(SelectError::UnsupportedHaving(format!(
                    "operator `{op}` is not supported in a HAVING comparand"
                )));
            }
            collect_join_having_types(left, group_by, scope, out)?;
            collect_join_having_types(right, group_by, scope, out)?;
        }
        // A literal (or any other leaf) contributes no type.
        _ => {}
    }
    Ok(())
}

/// Resolve a join `HAVING` column reference (bare or qualified) to its position in
/// `group_by` and its type ([STL-327]) — the join counterpart of
/// [`having_group_column`]. A column the join does not expose is the scope's own
/// diagnostic; a real but ungrouped one is the Postgres grouping error
/// ([`SelectError::UngroupedColumn`], SQLSTATE 42803).
fn join_having_group_column(
    column: &Expr,
    group_by: &[usize],
    scope: &JoinScope,
) -> Result<(usize, LogicalType), SelectError> {
    let (index, ty) = scope.resolve(column)?;
    let position =
        group_by
            .iter()
            .position(|&g| g == index)
            .ok_or_else(|| SelectError::UngroupedColumn {
                table: scope.relation_label(),
                column: scope.columns()[index].0.clone(),
            })?;
    Ok((position, ty))
}

/// Bind one side of a join `HAVING` comparison to a [`HavingScalar`] ([STL-327]) —
/// the join counterpart of [`bind_having_scalar`]. A grouping column resolves to its
/// `group_by` position, an aggregate is registered (deduplicated) into `aggregates`,
/// and a literal folds to `fold_ty`; arithmetic stays integer-only.
fn bind_join_having_scalar(
    expr: &Expr,
    fold_ty: LogicalType,
    group_by: &[usize],
    aggregates: &mut Vec<AggregateCall>,
    scope: &JoinScope,
) -> Result<HavingScalar, SelectError> {
    match unwrap_nested(expr) {
        column @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
            let (position, _) = join_having_group_column(column, group_by, scope)?;
            Ok(HavingScalar::Group(position))
        }
        function @ Expr::Function(_) => {
            let call = bind_join_aggregate_call(function, scope)?
                .expect("an Expr::Function binds to an aggregate call or errors");
            Ok(HavingScalar::Aggregate(register_aggregate(
                aggregates, call,
            )))
        }
        Expr::BinaryOp { left, op, right } => {
            let Some(arith) = arith_op(op) else {
                return Err(SelectError::UnsupportedHaving(format!(
                    "operator `{op}` is not supported in a HAVING comparand"
                )));
            };
            if !matches!(fold_ty, LogicalType::Int4 | LogicalType::Int8) {
                return Err(SelectError::UnsupportedHaving(format!(
                    "arithmetic in a HAVING needs an integer aggregate or grouping column \
                     (the anchor is {fold_ty})"
                )));
            }
            Ok(HavingScalar::Arith {
                op: arith,
                left: Box::new(bind_join_having_scalar(
                    left, fold_ty, group_by, aggregates, scope,
                )?),
                right: Box::new(bind_join_having_scalar(
                    right, fold_ty, group_by, aggregates, scope,
                )?),
            })
        }
        leaf => {
            let value = fold::fold_scalar(leaf, fold_ty)
                .map_err(|err| SelectError::UnsupportedHaving(having_fold_reason(&err, fold_ty)))?;
            Ok(HavingScalar::Literal(value))
        }
    }
}

/// Bind a join query's `ORDER BY` into [`BoundSortKey`]s ([STL-263], [STL-264]) —
/// the join counterpart of [`bind_order_by`], resolving each key against the
/// projected output columns first, then the join's addressable columns.
///
/// `projection` is the addressable indices the select list projects (a plain
/// query's [`BoundJoin::output`]) and `header` their `(name, type)` columns — both
/// empty/aggregate on the aggregate path, which resolves against `aggregate`'s
/// output columns instead.
fn bind_join_order_by(
    query: &Query,
    scope: &JoinScope,
    distinct: bool,
    aggregate: Option<&BoundAggregate>,
    projection: &[usize],
    header: &[(String, LogicalType)],
) -> Result<Vec<BoundSortKey>, SelectError> {
    let Some(order_by) = &query.order_by else {
        return Ok(Vec::new());
    };
    if order_by.interpolate.is_some() {
        return Err(SelectError::UnsupportedOrderBy("INTERPOLATE".to_owned()));
    }
    let exprs = match &order_by.kind {
        OrderByKind::All(_) => {
            return Err(SelectError::UnsupportedOrderBy("ORDER BY ALL".to_owned()));
        }
        OrderByKind::Expressions(exprs) => exprs,
    };
    exprs
        .iter()
        .map(|key| bind_join_sort_key(key, scope, distinct, aggregate, projection, header))
        .collect()
}

/// Bind one join `ORDER BY` key ([STL-264]). Like [`bind_sort_key`], the name
/// resolves against the **select list first**: an aggregate query's output columns;
/// a plain query's projected columns, **by name for a bare key and by resolved
/// addressable column for a qualified one** — so `SELECT DISTINCT t.a … ORDER BY
/// t.a` sorts on the projected `t.a` (a qualifier is often needed to disambiguate
/// same-named columns after a join) rather than tripping 42P10. A plain,
/// non-`DISTINCT` query may also sort on an unprojected addressable column (the
/// Postgres allowance); under `DISTINCT` that is the 42P10
/// [`SelectError::DistinctOrderBy`], and an aggregate query's rows have no
/// addressable columns to fall back to.
fn bind_join_sort_key(
    key: &OrderByExpr,
    scope: &JoinScope,
    distinct: bool,
    aggregate: Option<&BoundAggregate>,
    projection: &[usize],
    header: &[(String, LogicalType)],
) -> Result<BoundSortKey, SelectError> {
    if key.with_fill.is_some() {
        return Err(SelectError::UnsupportedOrderBy("WITH FILL".to_owned()));
    }
    if key.options.nulls_first.is_some() {
        return Err(SelectError::UnsupportedOrderBy(
            "explicit NULLS FIRST/LAST (the Postgres defaults apply: \
             NULLS LAST under ASC, NULLS FIRST under DESC)"
                .to_owned(),
        ));
    }
    let descending = key.options.asc == Some(false);
    let sort_output = |pos| {
        Ok(BoundSortKey {
            column: SortTarget::Output(pos),
            descending,
        })
    };
    // A bare name may match a select-list output column by name — even one whose
    // bare form is ambiguous across the inputs (the projected column disambiguates
    // it), so this precedes addressable resolution.
    let bare = bare_column(&key.expr);

    // An aggregate query's result rows are its output columns — there is no
    // addressable column to fall back to.
    if let Some(agg) = aggregate {
        let name = bare.ok_or_else(|| {
            SelectError::UnsupportedOrderBy(format!(
                "key `{}` — an aggregate query orders only by a select-list column",
                key.expr
            ))
        })?;
        return agg.columns.iter().position(|(n, _)| n == name).map_or_else(
            || {
                Err(SelectError::UnsupportedOrderBy(format!(
                    "column {name:?} is not a select-list column of the aggregate query"
                )))
            },
            sort_output,
        );
    }

    // Plain query: a bare name matching a projected output column by name.
    if let Some(name) = bare
        && let Some(pos) = header.iter().position(|(n, _)| n == name)
    {
        return sort_output(pos);
    }

    // Resolve the key (bare or qualified) to an addressable column.
    let column_expr = match &key.expr {
        e @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Nested(_)) => e,
        other => {
            return Err(SelectError::UnsupportedOrderBy(format!(
                "key `{other}` — only a column name sorts"
            )));
        }
    };
    let (index, _) = scope.resolve(column_expr)?;

    // A qualified key that resolves to a **projected** column *is* a select-list
    // column (`SELECT DISTINCT t.a … ORDER BY t.a`) — sort by its output position,
    // legal under `DISTINCT`. Only a column the projection does not carry falls
    // through: allowed for a plain read, the 42P10 ambiguity under `DISTINCT`.
    if let Some(pos) = projection.iter().position(|&p| p == index) {
        return sort_output(pos);
    }
    if distinct {
        return Err(SelectError::DistinctOrderBy);
    }
    Ok(BoundSortKey {
        column: SortTarget::Schema(index),
        descending,
    })
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

/// Lower a `WHERE` clause to its bound shape, or `(None, None)` when there is
/// none.
///
/// A `WHERE` is one of two row-filter shapes the binder distinguishes here (the
/// third, a period predicate, is lifted off the token stream before this runs):
///
/// * an **uncorrelated subquery** predicate ([STL-234]) — `<col> <cmp> (SELECT
///   <scalar>)`, `<col> [NOT] IN (SELECT <col>)`, or `[NOT] EXISTS (SELECT …)`,
///   returned as the second tuple element;
/// * the plain `<col> <cmp> <scalar>` comparison ([STL-151], [STL-213]),
///   returned as the first.
///
/// The subquery dispatcher runs first, since a comparison whose operand is a
/// `(SELECT …)` is the subquery shape, not a plain predicate (its operand would
/// not fold to a literal). The two are mutually exclusive — at most one tuple
/// element is `Some`.
fn bind_where(
    select: &Select,
    schema: &TableSchema,
    table: &str,
    ctx: &BindContext,
    snapshot: SystemTimeMicros,
    valid_snapshot: Option<SystemTimeMicros>,
) -> Result<(Option<BoundPredicate>, Option<BoundSubqueryFilter>), SelectError> {
    let Some(expr) = select.selection.as_ref() else {
        return Ok((None, None));
    };
    // The inner subquery inherits the outer's **resolved** system snapshot (after
    // `AS OF` folding), not the raw transaction snapshot `ctx.snapshot` — so a
    // `… (SELECT … FROM s) … FOR SYSTEM_TIME AS OF p` reads `s` at `p` too, the
    // one consistent per-statement snapshot (docs/16 §6). The valid axis is
    // inherited separately ([`inherit_valid_snapshot`]).
    let inner_ctx = BindContext {
        snapshot,
        catalog: ctx.catalog,
    };
    // The outer query's single table, as the correlation resolver sees it: its name
    // (and optional alias) qualify outer-column references the inner makes, and its
    // schema gives them indices and types ([STL-239]).
    let outer = OuterScope {
        table,
        alias: select_table_alias(select),
        schema,
    };
    if let Some(mut subquery) = try_bind_subquery_filter(expr, &outer, &inner_ctx)? {
        inherit_valid_snapshot(&mut subquery.subquery, valid_snapshot, ctx.catalog)?;
        return Ok((None, Some(subquery)));
    }
    Ok((Some(bind_where_predicate(expr, schema, table)?), None))
}

/// The alias of a single-table `SELECT`'s `FROM` relation (`FROM t outer`), or
/// `None` for an unaliased table or any non-table relation ([STL-239]).
///
/// Used to qualify outer-column references inside a correlated subquery: an alias,
/// when present, is the relation's exposed name (it hides the table name, the SQL
/// scoping rule), so `outer.k` in the inner resolves against the alias.
fn select_table_alias(select: &Select) -> Option<&str> {
    let [from] = select.from.as_slice() else {
        return None;
    };
    table_ref(&from.relation).ok().and_then(|r| r.alias)
}

/// Pin an inner subquery's valid axis to the outer statement's `FOR VALID_TIME
/// AS OF` instant, when the outer carried one and the inner reads a valid-time
/// table (docs/16 §6 — one consistent `(sys, valid)` snapshot per statement).
///
/// The system axis is inherited automatically — the inner binds under the same
/// [`BindContext`], so its [`snapshot`](BoundSelect::snapshot) is the outer's.
/// The valid axis is not, since the inner's own [`Temporal`] is empty; it is
/// applied here. A system-only inner has no valid axis to pin (it is left
/// unpinned — there is no valid axis to travel).
///
/// A **join** inner is the one case that **fails closed**. A direct join now
/// carries a valid-time pin ([STL-243], applied + side-validated in
/// [`bind_join`]), but *this* path sets the pin on an already-bound join plan
/// without re-checking that each side has a valid axis — and the subquery + join
/// composition is itself not yet wired ([STL-264]). Rather than set a pin that
/// might land on a system-only join side, or read the join's valid-time sides
/// *unpinned* (silently violating the one-snapshot-per-statement rule), an outer
/// `FOR VALID_TIME AS OF` over a subquery that joins tables is rejected
/// ([`SelectError::Subquery`]) until that composition lands.
fn inherit_valid_snapshot(
    inner: &mut BoundSelect,
    outer_valid: Option<SystemTimeMicros>,
    catalog: &Catalog,
) -> Result<(), SelectError> {
    let Some(valid) = outer_valid else {
        return Ok(());
    };
    if inner.join.is_some() {
        return Err(SelectError::Subquery(
            "a FOR VALID_TIME AS OF read cannot pin the valid axis of a subquery that joins \
             tables (inheriting an outer valid pin into a joined subquery is not wired yet — \
             STL-264)"
                .to_owned(),
        ));
    }
    if let TableResolution::Found(schema) = resolve_table_at(catalog, &inner.table, inner.snapshot)
        && schema.temporal().valid_time_enabled()
    {
        inner.valid_snapshot = Some(valid);
    }
    Ok(())
}

/// The outer query's single table during subquery binding ([STL-239]): its name,
/// optional alias, and resolved schema. The name/alias qualify outer-column
/// references a correlated inner makes; the schema resolves them to indices/types.
struct OuterScope<'a> {
    table: &'a str,
    alias: Option<&'a str>,
    schema: &'a TableSchema,
}

impl OuterScope<'_> {
    /// Whether `qualifier` (a `t.c` prefix) names this scope. An alias, when
    /// present, **replaces** the table name as the relation's exposed name (the SQL
    /// scoping rule), so an aliased outer is reachable only through its alias — which
    /// is what lets a self-correlated `… FROM t WHERE EXISTS (SELECT 1 FROM t inner
    /// WHERE inner.k = t.k)` distinguish the two `t`s.
    fn qualifier_matches(&self, qualifier: &str) -> bool {
        self.alias
            .map_or(self.table == qualifier, |a| a == qualifier)
    }
}

/// Recognize and bind a subquery `WHERE` ([STL-234], [STL-239]): `[NOT] EXISTS`,
/// `[NOT] IN (SELECT …)`, or a comparison with a `(SELECT …)` operand. Returns
/// `None` for every other `WHERE` — those fall through to the plain
/// [`bind_where_predicate`].
fn try_bind_subquery_filter(
    expr: &Expr,
    outer: &OuterScope,
    ctx: &BindContext,
) -> Result<Option<BoundSubqueryFilter>, SelectError> {
    match unwrap_nested(expr) {
        Expr::Exists { subquery, negated } => {
            Ok(Some(bind_exists_subquery(subquery, *negated, outer, ctx)?))
        }
        Expr::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => Ok(Some(bind_in_subquery(lhs, subquery, *negated, outer, ctx)?)),
        Expr::BinaryOp { left, op, right } => {
            bind_scalar_subquery_compare(left, op, right, outer, ctx)
        }
        _ => Ok(None),
    }
}

/// Bind `[NOT] EXISTS (SELECT …)`. `EXISTS` tests only row presence, so the
/// inner select-list is irrelevant; the inner binds with its projection
/// normalized (see [`bind_inner_query`]). A correlated inner ([STL-239]) carries
/// its [`Correlation`] out of [`bind_inner_query`] for the executor's per-row path.
fn bind_exists_subquery(
    subquery: &Query,
    negated: bool,
    outer: &OuterScope,
    ctx: &BindContext,
) -> Result<BoundSubqueryFilter, SelectError> {
    let (inner, correlation) = bind_inner_query(subquery, outer, ctx, /* exists = */ true)?;
    Ok(BoundSubqueryFilter {
        kind: SubqueryKind::Exists { negated },
        subquery: Box::new(inner),
        correlation,
    })
}

/// Bind `<column> [NOT] IN (SELECT <col>)`. The outer operand must be a bare
/// value column, and the inner must return exactly one column of the same type.
///
/// A non-negated, equality-correlated `IN` over a plain-scan inner is shaped for
/// **composite-key decorrelation** ([STL-337]): its correlation key is appended to
/// the inner's projection ([`project_in_correlation_key`]) so the engine can fold it
/// onto a single semi join. `NOT IN`, a range correlation, an uncorrelated `IN`, and
/// a non-plain inner keep the per-row / once-folded shape (a lone membership column).
fn bind_in_subquery(
    lhs: &Expr,
    subquery: &Query,
    negated: bool,
    outer: &OuterScope,
    ctx: &BindContext,
) -> Result<BoundSubqueryFilter, SelectError> {
    let column = subquery_anchor_column(lhs, outer.schema, outer.table, "IN")?;
    let (mut inner, correlation) =
        bind_inner_query(subquery, outer, ctx, /* exists = */ false)?;
    check_subquery_column_type(&inner, outer.schema, column, ctx.catalog, "IN")?;
    if let Some(corr) = correlation
        && corr.op == CompareOp::Eq
        && !negated
        && inner.is_plain_scan()
    {
        project_in_correlation_key(&mut inner, corr, ctx.catalog)?;
    }
    Ok(BoundSubqueryFilter {
        kind: SubqueryKind::In { column, negated },
        subquery: Box::new(inner),
        correlation,
    })
}

/// Append the **correlation key** column to a decorrelatable `IN` inner's
/// projection ([STL-337]), so its result is `[membership, correlation key]` — the
/// two components the composite-key semi join
/// ([`BoundSubqueryFilter::composite_semi_decorrelation`]) reads per inner row.
///
/// The membership the `IN` projects is preserved unchanged at result position `0`
/// (so the [STL-239] per-row fold, which reads column `0`, is unaffected for any `IN`
/// that does not in fact decorrelate) — it is whatever single column the `IN`
/// selected, a plain column or a computed expression already type-checked to the
/// outer column's type. The appended correlation key (`s.k`, a plain column by inner
/// schema index [`Correlation::inner_column`]) lands at position `1`. The binder
/// type-checked the membership before this widened the result to two columns.
fn project_in_correlation_key(
    inner: &mut BoundSelect,
    correlation: Correlation,
    catalog: &Catalog,
) -> Result<(), SelectError> {
    let TableResolution::Found(schema) = resolve_table_at(catalog, &inner.table, inner.snapshot)
    else {
        return Err(SelectError::Subquery(format!(
            "subquery table {:?} is no longer resolvable",
            inner.table
        )));
    };
    let key_item = ProjectionItem::column(schema.columns()[correlation.inner_column].name());
    match &mut inner.projection {
        Projection::Items(items) => items.push(key_item),
        Projection::All => {
            // `IN (SELECT * FROM s …)` binds only for a single-column `s` (an `IN`
            // inner must yield one column), so `*` is that lone membership column;
            // make it explicit, then append the key, to reach the same
            // `[membership, correlation key]` two-column layout.
            let member = ProjectionItem::column(schema.columns()[0].name());
            inner.projection = Projection::Items(vec![member, key_item]);
        }
    }
    Ok(())
}

/// Bind a comparison with a `(SELECT …)` scalar operand, or `None` when neither
/// side is a subquery (then it is a plain comparison, bound elsewhere).
///
/// Exactly one side must be a subquery; the other must be a bare value column of
/// the inner's output type. A comparison between two subqueries, or with a
/// non-column outer operand, is a [`SelectError::Subquery`] rather than a
/// fall-through (which would mis-bind it as a plain predicate).
fn bind_scalar_subquery_compare(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    outer: &OuterScope,
    ctx: &BindContext,
) -> Result<Option<BoundSubqueryFilter>, SelectError> {
    let left = unwrap_nested(left);
    let right = unwrap_nested(right);
    let (column_expr, subquery, subquery_left) = match (as_subquery(left), as_subquery(right)) {
        (None, None) => return Ok(None),
        (Some(_), Some(_)) => {
            return Err(SelectError::Subquery(
                "a comparison between two subqueries is not supported".to_owned(),
            ));
        }
        (Some(query), None) => (right, query, true),
        (None, Some(query)) => (left, query, false),
    };
    // Only a comparison operator lowers to a scalar-subquery test; a non-comparison
    // (`a + (SELECT …)`) over a subquery is not a v0.3 shape.
    let Some(compare) = compare_op(op) else {
        return Err(SelectError::Subquery(format!(
            "operator `{op}` is not supported with a subquery operand"
        )));
    };
    let column = subquery_anchor_column(
        column_expr,
        outer.schema,
        outer.table,
        "a scalar subquery comparison",
    )?;
    let (mut inner, correlation) =
        bind_inner_query(subquery, outer, ctx, /* exists = */ false)?;
    check_subquery_column_type(
        &inner,
        outer.schema,
        column,
        ctx.catalog,
        "a scalar subquery",
    )?;
    // A scalar subquery only needs the 0 / 1 / >1 distinction, so cap it at two
    // rows: the engine still raises the cardinality violation (≥2 rows → still 2),
    // without materializing an arbitrarily large inner result first. A user's
    // tighter `LIMIT` (e.g. `LIMIT 1`) is kept. The cap holds per outer row for a
    // correlated scalar ([STL-239]) too — each re-execution still yields ≤2 rows.
    inner.limit = Some(inner.limit.map_or(2, |existing| existing.min(2)));
    Ok(Some(BoundSubqueryFilter {
        kind: SubqueryKind::Scalar {
            column,
            op: compare,
            subquery_left,
        },
        subquery: Box::new(inner),
        correlation,
    }))
}

/// The `(SELECT …)` a parenthesized-subquery expression wraps, or `None`.
const fn as_subquery(expr: &Expr) -> Option<&Query> {
    match expr {
        Expr::Subquery(query) => Some(query),
        _ => None,
    }
}

/// Resolve the bare value column an `IN` / scalar-subquery comparison tests, to
/// its schema index. A non-bare-column outer operand (an arithmetic, a literal,
/// the business key compared in a richer way) is a [`SelectError::Subquery`]
/// with the predicate's name, since the executor folds the inner result against
/// exactly this one column.
fn subquery_anchor_column(
    expr: &Expr,
    schema: &TableSchema,
    table: &str,
    what: &str,
) -> Result<usize, SelectError> {
    match unwrap_nested(expr) {
        Expr::Identifier(id) => {
            column_index(schema, &id.value).ok_or_else(|| SelectError::UnknownColumn {
                table: table.to_owned(),
                column: id.value.clone(),
            })
        }
        other => Err(SelectError::Subquery(format!(
            "the outer operand of {what} must be a bare column, not `{other}`"
        ))),
    }
}

/// Check that an inner subquery returns exactly one column whose type matches the
/// outer column it is compared to. The executor compares the materialized inner
/// values against the outer column's typed vector, which requires identical
/// types (Stele does not implicitly coerce — the same posture the plain filter's
/// literal folding takes).
fn check_subquery_column_type(
    inner: &BoundSelect,
    outer_schema: &TableSchema,
    outer_column: usize,
    catalog: &Catalog,
    what: &str,
) -> Result<(), SelectError> {
    let inner_ty = sole_output_type(inner, catalog)?;
    let outer_ty = outer_schema.columns()[outer_column].ty();
    if inner_ty != outer_ty {
        return Err(SelectError::Subquery(format!(
            "{what} yields {inner_ty}, but the outer column is {outer_ty}"
        )));
    }
    Ok(())
}

/// The single output column type of an inner subquery, or a
/// [`SelectError::Subquery`] when it returns a number of columns other than one.
fn sole_output_type(bound: &BoundSelect, catalog: &Catalog) -> Result<LogicalType, SelectError> {
    sole_output_column(bound, catalog).map(|(_, ty)| ty)
}

/// The single `(name, type)` output column of an inner subquery, or a
/// [`SelectError::Subquery`] when it returns a number of columns other than one
/// ([STL-303]). The name is what an unaliased scalar subquery inherits as its
/// output column name (the Postgres rule).
///
/// Mirrors the engine's output-column resolution: an aggregate query's columns
/// are its [`BoundAggregate::columns`], a join's its [`BoundJoin::columns`], and a
/// plain projection's are read from the schema live at the inner's snapshot.
fn sole_output_column(
    bound: &BoundSelect,
    catalog: &Catalog,
) -> Result<(String, LogicalType), SelectError> {
    if let Some(agg) = &bound.aggregate {
        return single_output_column(&agg.columns);
    }
    if let Some(join) = &bound.join {
        return single_output_column(&join.columns);
    }
    // `bind_select` already resolved the inner table here, so a miss is a
    // contract break rather than user input.
    let TableResolution::Found(schema) = resolve_table_at(catalog, &bound.table, bound.snapshot)
    else {
        return Err(SelectError::Subquery(format!(
            "subquery table {:?} is no longer resolvable",
            bound.table
        )));
    };
    match &bound.projection {
        Projection::All => {
            let columns = schema.columns();
            if columns.len() != 1 {
                return Err(SelectError::Subquery(format!(
                    "a subquery used here must return one column, but it returns {}",
                    columns.len()
                )));
            }
            Ok((columns[0].name().to_owned(), columns[0].ty()))
        }
        Projection::Items(items) => match items.as_slice() {
            [item] => Ok((item.name.clone(), projection_item_type(item, schema)?)),
            _ => Err(SelectError::Subquery(format!(
                "a subquery used here must return one column, but it returns {}",
                items.len()
            ))),
        },
    }
}

/// The result type of a single inner-subquery projection item ([STL-303]): a column
/// item resolves its source column's type from the inner schema; a computed
/// expression / scalar subquery carries its own resolved type.
fn projection_item_type(
    item: &ProjectionItem,
    schema: &TableSchema,
) -> Result<LogicalType, SelectError> {
    match &item.value {
        ProjectionValue::Column(source) => resolve_filter_column(schema, source)
            .map(|(_, ty)| ty)
            .ok_or_else(|| SelectError::Subquery(format!("unknown subquery column {source:?}"))),
        ProjectionValue::Computed { ty, .. } | ProjectionValue::Subquery { ty, .. } => Ok(*ty),
    }
}

/// The single `(name, type)` of a one-element output-column list, or a
/// [`SelectError::Subquery`] when the list is not exactly one column.
fn single_output_column(
    columns: &[(String, LogicalType)],
) -> Result<(String, LogicalType), SelectError> {
    match columns {
        [(name, ty)] => Ok((name.clone(), *ty)),
        _ => Err(SelectError::Subquery(format!(
            "a subquery used here must return one column, but it returns {}",
            columns.len()
        ))),
    }
}

/// Bind an inner (subquery) `SELECT` under the **same** [`BindContext`] as the
/// outer query, so it inherits the one consistent per-statement snapshot
/// (docs/16 §6) — the temporal rule that makes a subquery's result well-defined,
/// correlated or not.
///
/// The inner query carries no temporal grammar of its own ([`Temporal::default`]):
/// the parser lifts every `FOR … AS OF` to the statement level, so a subquery is
/// always read at the outer snapshot. For an `EXISTS` subquery (`exists`), a
/// non-aggregate inner's select-list is normalized to `*` so the idiomatic
/// `EXISTS (SELECT 1 …)` binds without constant-projection support — `EXISTS`
/// ignores the select-list anyway. An aggregate inner keeps its shape (it always
/// yields exactly one row, so the row-presence test is well-defined).
///
/// **Correlation** ([STL-239]): if the inner's `WHERE` relates an inner column to
/// an outer-query column (`… WHERE inner.k = outer.k`), that single comparison is
/// lifted off the inner *before* it binds — otherwise the inner would reject the
/// outer column as unknown — and returned as a [`Correlation`] for the executor's
/// per-row path. The inner then binds with no `WHERE` (the correlation *was* the
/// whole `WHERE`, since the engine lowers only a single-comparison `WHERE`); a
/// `Some` return is the only thing that distinguishes a correlated subquery from an
/// uncorrelated one downstream.
fn bind_inner_query(
    query: &Query,
    outer: &OuterScope,
    ctx: &BindContext,
    exists: bool,
) -> Result<(BoundSelect, Option<Correlation>), SelectError> {
    let mut query = query.clone();
    let mut correlation = None;
    if let SetExpr::Select(select) = query.body.as_mut() {
        // Lift any correlated `WHERE` off the inner before binding (an outer-column
        // reference would otherwise be an unknown column against the inner schema).
        correlation = strip_correlation(select, outer, ctx)?;
        if exists && !is_aggregate_query(select) {
            select.projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
        }
    }
    let stmt = Statement {
        body: StatementBody::Sql(SqlStatement::Query(Box::new(query))),
        temporal: Temporal::default(),
    };
    Ok((bind_select(&stmt, ctx)?, correlation))
}

/// Detect a **correlated** inner `WHERE` and lift it off `select`, returning the
/// [`Correlation`] it describes ([STL-239]); returns `None` (leaving the `WHERE`
/// in place for the inner binder) when the subquery is uncorrelated.
///
/// Correlation is recognized only for a single-table inner whose `WHERE` is one
/// comparison relating an inner column to an outer column — the engine's per-row
/// fallback re-applies exactly that one comparison. A join inner, or a `WHERE` that
/// is not such a comparison, is left untouched: it either binds as an uncorrelated
/// query or surfaces its own error (a join rejects any `WHERE`; an outer reference
/// the resolver cannot place becomes an unknown column against the inner schema).
fn strip_correlation(
    select: &mut Select,
    outer: &OuterScope,
    ctx: &BindContext,
) -> Result<Option<Correlation>, SelectError> {
    let Some(expr) = select.selection.as_ref() else {
        return Ok(None);
    };
    // Resolve the inner's single table to give its columns indices/types. A
    // non-single-table inner (a join) has no single inner schema to correlate
    // against — leave its `WHERE` for the inner binder (which rejects it).
    let Ok(inner_ref) = table_ref_of(select) else {
        return Ok(None);
    };
    let TableResolution::Found(inner_schema) =
        resolve_table_at(ctx.catalog, inner_ref.name, ctx.snapshot)
    else {
        return Ok(None);
    };
    let inner = OuterScope {
        table: inner_ref.name,
        alias: inner_ref.alias,
        schema: inner_schema,
    };
    let Some(correlation) = match_correlation(expr, &inner, outer)? else {
        return Ok(None);
    };
    // The correlation is the whole `WHERE` (the engine lowers only a single
    // comparison), so strip it: the inner binds unfiltered and the executor
    // re-applies the comparison per outer row.
    select.selection = None;
    Ok(Some(correlation))
}

/// The single-table reference of an inner `SELECT`'s `FROM` (`FROM s` / `FROM s
/// alias`), or an error for an empty / multi-relation / non-table `FROM`.
fn table_ref_of(select: &Select) -> Result<TableRef<'_>, SelectError> {
    let [from] = select.from.as_slice() else {
        return Err(SelectError::UnsupportedFrom("not exactly one table"));
    };
    if !from.joins.is_empty() {
        return Err(SelectError::UnsupportedFrom("join"));
    }
    table_ref(&from.relation)
}

/// Match a single-comparison inner `WHERE` as a correlation: exactly one operand an
/// inner column and the other an outer column ([STL-239]).
///
/// Returns `None` when no operand resolves to the outer scope (an uncorrelated
/// `WHERE`, left for the inner binder). A comparison that *does* reference the
/// outer but is not the supported `inner_column <op> outer_column` shape (an outer
/// reference paired with a literal, an arithmetic, or another outer reference) is a
/// [`SelectError::Subquery`] — recognized as correlated but not a shape the per-row
/// fallback lowers.
fn match_correlation(
    expr: &Expr,
    inner: &OuterScope,
    outer: &OuterScope,
) -> Result<Option<Correlation>, SelectError> {
    let Expr::BinaryOp { left, op, right } = unwrap_nested(expr) else {
        return Ok(None);
    };
    let Some(compare) = compare_op(op) else {
        return Ok(None);
    };
    let left_ref = resolve_correlation_operand(left, inner, outer)?;
    let right_ref = resolve_correlation_operand(right, inner, outer)?;
    let (outer_column, inner_column, op) = match (left_ref, right_ref) {
        // Inner on the left: keep the operator as written.
        (Some(CorrRef::Inner(ic)), Some(CorrRef::Outer(oc))) => (oc, ic, compare),
        // Outer on the left: mirror so the inner column reads as the left operand.
        (Some(CorrRef::Outer(oc)), Some(CorrRef::Inner(ic))) => (oc, ic, compare.mirror()),
        // References the outer, but not as `inner_column <op> outer_column`.
        (Some(CorrRef::Outer(_)), _) | (_, Some(CorrRef::Outer(_))) => {
            return Err(SelectError::Subquery(
                "a correlated subquery WHERE must compare an inner column to an outer column"
                    .to_owned(),
            ));
        }
        // No outer reference: an uncorrelated WHERE, bound by the inner binder.
        _ => return Ok(None),
    };
    let inner_ty = inner.schema.columns()[inner_column].ty();
    let outer_ty = outer.schema.columns()[outer_column].ty();
    if inner_ty != outer_ty {
        return Err(SelectError::Subquery(format!(
            "a correlated subquery compares {inner_ty} to the outer {outer_ty}"
        )));
    }
    Ok(Some(Correlation {
        outer_column,
        inner_column,
        op,
    }))
}

/// Which scope a column reference in a correlated `WHERE` resolves to.
#[derive(Debug, Clone, Copy)]
enum CorrRef {
    /// A column of the inner (subquery) table, by inner schema index.
    Inner(usize),
    /// A column of the outer query's table, by outer schema index.
    Outer(usize),
}

/// Resolve one operand of an inner `WHERE` comparison to the scope it names
/// ([STL-239]), or `None` for a non-column operand (a literal / arithmetic) or a
/// bare name in neither scope.
///
/// A **bare** column resolves to the inner scope when the inner has it (the
/// innermost-scope rule — an inner column shadows an equally-named outer one), else
/// to the outer when only the outer has it. A **qualified** `q.c` resolves by which
/// scope `q` names; a qualifier that names neither is not a correlation reference
/// (`None`), and one that names a scope without the column is an unknown column.
fn resolve_correlation_operand(
    expr: &Expr,
    inner: &OuterScope,
    outer: &OuterScope,
) -> Result<Option<CorrRef>, SelectError> {
    match unwrap_nested(expr) {
        Expr::Identifier(id) => Ok(column_index(inner.schema, &id.value)
            .map(CorrRef::Inner)
            .or_else(|| column_index(outer.schema, &id.value).map(CorrRef::Outer))),
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return Ok(None);
            };
            let (q, c) = (qualifier.value.as_str(), column.value.as_str());
            let unknown = |scope: &OuterScope| SelectError::UnknownColumn {
                table: scope.table.to_owned(),
                column: c.to_owned(),
            };
            // The inner scope wins a tie: a self-correlation aliases one side, so an
            // un-hidden table name that matches both is the inner's.
            if inner.qualifier_matches(q) {
                column_index(inner.schema, c)
                    .map(|i| Some(CorrRef::Inner(i)))
                    .ok_or_else(|| unknown(inner))
            } else if outer.qualifier_matches(q) {
                column_index(outer.schema, c)
                    .map(|o| Some(CorrRef::Outer(o)))
                    .ok_or_else(|| unknown(outer))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

/// Bind one `WHERE` expression to a [`BoundPredicate`] against `table`'s schema —
/// the shared predicate vocabulary of a `SELECT`'s filter ([STL-213]) and a
/// scan-then-write `UPDATE` / `DELETE`'s row selection ([STL-229]), so the two
/// statement families accept exactly the same `WHERE` shapes.
///
/// [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
pub(crate) fn bind_where_predicate(
    expr: &Expr,
    schema: &TableSchema,
    table: &str,
) -> Result<BoundPredicate, SelectError> {
    // Peel parentheses around the whole predicate so `WHERE (id = 1)` binds like
    // `WHERE id = 1`. The top level must be a comparison; its operands are bound as
    // scalars below.
    let Expr::BinaryOp { left, op, right } = unwrap_nested(expr) else {
        return Err(SelectError::UnsupportedPredicate(
            "the WHERE is not a comparison".to_owned(),
        ));
    };
    let Some(compare) = compare_op(op) else {
        return Err(SelectError::UnsupportedPredicate(format!(
            "operator `{op}` is not a comparison"
        )));
    };
    // Exactly one column may appear across the whole predicate; it anchors the type
    // every literal folds to (and the type any arithmetic computes in).
    let anchor = filter_anchor(left, right, schema, table)?;
    Ok(BoundPredicate {
        left: bind_scalar(left, &anchor, schema, table)?,
        op: compare,
        right: bind_scalar(right, &anchor, schema, table)?,
    })
}

/// The single column a `WHERE` predicate filters on, resolved to its schema index
/// and type — the anchor every literal in the predicate folds against.
struct FilterAnchor<'a> {
    /// The column's name, for fold diagnostics.
    name: &'a str,
    /// The column's schema index (`0` is the business key).
    index: usize,
    /// The column's type.
    ty: LogicalType,
}

/// Resolve the one column a comparison references to a [`FilterAnchor`].
///
/// A predicate with no column has no type to anchor (a constant `WHERE` v0.2 does
/// not lower); one referencing two distinct columns is a column-to-column compare
/// (a deferred follow-up). Both are [`SelectError::UnsupportedPredicate`]; an
/// unknown single column is [`SelectError::UnknownColumn`].
fn filter_anchor<'a>(
    left: &'a Expr,
    right: &'a Expr,
    schema: &TableSchema,
    table: &str,
) -> Result<FilterAnchor<'a>, SelectError> {
    let mut names: Vec<&str> = Vec::new();
    collect_where_columns(left, &mut names);
    collect_where_columns(right, &mut names);
    names.sort_unstable();
    names.dedup();
    match names.as_slice() {
        [name] => {
            let (index, ty) =
                resolve_filter_column(schema, name).ok_or_else(|| SelectError::UnknownColumn {
                    table: table.to_owned(),
                    column: (*name).to_owned(),
                })?;
            Ok(FilterAnchor { name, index, ty })
        }
        [] => Err(SelectError::UnsupportedPredicate(
            "the WHERE references no column".to_owned(),
        )),
        _ => Err(SelectError::UnsupportedPredicate(
            "a column-to-column comparison is not supported".to_owned(),
        )),
    }
}

/// Collect the bare column names a `WHERE` operand references, descending through
/// parentheses and arithmetic. A non-identifier leaf (a literal) contributes none.
fn collect_where_columns<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
    match expr {
        Expr::Identifier(id) => out.push(id.value.as_str()),
        Expr::Nested(inner) => collect_where_columns(inner, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_where_columns(left, out);
            collect_where_columns(right, out);
        }
        _ => {}
    }
}

/// Bind one side of a `WHERE` comparison to a [`BoundScalar`]: the anchor column,
/// an integer arithmetic of it, or a literal folded to the anchor's type.
///
/// Arithmetic is integer-only (the evaluator computes `+ - * / %` over
/// `int4`/`int8`); over a non-integer anchor it is rejected at bind time rather
/// than erroring per row.
fn bind_scalar(
    expr: &Expr,
    anchor: &FilterAnchor<'_>,
    schema: &TableSchema,
    table: &str,
) -> Result<BoundScalar, SelectError> {
    match unwrap_nested(expr) {
        Expr::Identifier(id) => {
            let (index, _) = resolve_filter_column(schema, &id.value).ok_or_else(|| {
                SelectError::UnknownColumn {
                    table: table.to_owned(),
                    column: id.value.clone(),
                }
            })?;
            Ok(BoundScalar::Column(index))
        }
        Expr::BinaryOp { left, op, right } => {
            let Some(arith) = arith_op(op) else {
                return Err(SelectError::UnsupportedPredicate(format!(
                    "operator `{op}` is not supported in a WHERE comparand"
                )));
            };
            if !matches!(anchor.ty, LogicalType::Int4 | LogicalType::Int8) {
                return Err(SelectError::UnsupportedPredicate(format!(
                    "arithmetic in a WHERE needs an integer column, but `{}` is {}",
                    anchor.name, anchor.ty
                )));
            }
            Ok(BoundScalar::Arith {
                op: arith,
                left: Box::new(bind_scalar(left, anchor, schema, table)?),
                right: Box::new(bind_scalar(right, anchor, schema, table)?),
            })
        }
        leaf => {
            let value = fold::fold_scalar(leaf, anchor.ty).map_err(|err| {
                SelectError::UnsupportedPredicate(predicate_reason(&err, anchor.name, anchor.ty))
            })?;
            Ok(BoundScalar::Literal(value))
        }
    }
}

/// Map a parsed comparison [`BinaryOperator`] to a [`CompareOp`], or `None` if it
/// is not a comparison (a connective, an arithmetic, a string/regex operator).
const fn compare_op(op: &BinaryOperator) -> Option<CompareOp> {
    Some(match op {
        BinaryOperator::Eq => CompareOp::Eq,
        BinaryOperator::NotEq => CompareOp::Ne,
        BinaryOperator::Lt => CompareOp::Lt,
        BinaryOperator::LtEq => CompareOp::Le,
        BinaryOperator::Gt => CompareOp::Gt,
        BinaryOperator::GtEq => CompareOp::Ge,
        _ => return None,
    })
}

/// Map a parsed arithmetic [`BinaryOperator`] to an [`ArithOp`], or `None` if it is
/// not one of the integer arithmetic operators.
const fn arith_op(op: &BinaryOperator) -> Option<ArithOp> {
    Some(match op {
        BinaryOperator::Plus => ArithOp::Add,
        BinaryOperator::Minus => ArithOp::Sub,
        BinaryOperator::Multiply => ArithOp::Mul,
        BinaryOperator::Divide => ArithOp::Div,
        BinaryOperator::Modulo => ArithOp::Mod,
        _ => return None,
    })
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

/// The axis-tagged outcome of resolving the statement's
/// `FOR { SYSTEM_TIME | VALID_TIME } { FROM a TO b | BETWEEN a AND b }` range
/// qualifier ([STL-244], [STL-328]). The two axes lower to different
/// [`BoundSelect`] fields and engine paths, so the axis is carried out of the
/// resolver rather than re-derived.
enum BoundTemporalRange {
    /// A `FOR SYSTEM_TIME` range — "show me the history" over the system axis.
    System(SystemTimeRange),
    /// A `FOR VALID_TIME` range — every version whose valid interval overlaps the
    /// range, at the statement's system snapshot.
    Valid(ValidTimeRange),
}

/// Whether a folded `[from, to]`/`[from, to)` range of raw microseconds is
/// non-empty — `from < to` for the half-open `FROM..TO`, `from <= to` for the
/// closed `BETWEEN`. Mirrors the docs/16 §2 reversed / zero-length rejection;
/// takes bare `i64` so it serves either axis's microsecond newtype.
const fn range_well_formed(from: i64, to: i64, closed_upper: bool) -> bool {
    if closed_upper { from <= to } else { from < to }
}

/// Resolve the statement's range qualifier (if any) into an axis-tagged
/// [`BoundTemporalRange`], or `None` when the statement carries no range.
///
/// Both endpoints fold the same way an `AS OF` operand does ([`resolve_as_of`]),
/// against `now`. The `AS OF` conflict rules differ by axis:
///
/// * A **system** range rejects *any* `AS OF` ([STL-244]) — a system point read
///   and a range read of one table are not yet composed.
/// * A **valid** range rejects only a `FOR VALID_TIME AS OF` (a point and a range
///   on the *same* axis), but **allows** a `FOR SYSTEM_TIME AS OF`: it fixes the
///   system snapshot the valid history is read at (`v(k, S, V_range)`), the
///   cross-axis composition the both-axes point read already supports.
///
/// # Errors
///
/// [`SelectError::UnsupportedSystemRange`] / [`SelectError::UnsupportedValidRange`]
/// for an `AS OF` conflict; [`SelectError::AsOf`] if an endpoint cannot be folded;
/// [`SelectError::EmptySystemRange`] / [`SelectError::EmptyValidRange`] for an
/// empty or reversed interval.
fn resolve_temporal_range(
    stmt: &Statement,
    now: SystemTimeMicros,
) -> Result<Option<BoundTemporalRange>, SelectError> {
    let Some(range) = &stmt.temporal.range else {
        return Ok(None);
    };
    match range.dimension {
        TimeDimension::System => {
            // A point `AS OF` and a range are two different reads of the same table;
            // naming both is contradictory rather than a composition.
            if !stmt.temporal.as_of.is_empty() {
                return Err(SelectError::UnsupportedSystemRange(
                    "a FOR ... AS OF point qualifier cannot be combined with a FROM/BETWEEN range"
                        .to_owned(),
                ));
            }
            let from = resolve_as_of(&range.from, now)?;
            let to = resolve_as_of(&range.to, now)?;
            if !range_well_formed(from.0, to.0, range.closed_upper) {
                return Err(SelectError::EmptySystemRange {
                    from: from.0,
                    to: to.0,
                    closed_upper: range.closed_upper,
                });
            }
            Ok(Some(BoundTemporalRange::System(SystemTimeRange {
                from,
                to,
                closed_upper: range.closed_upper,
            })))
        }
        TimeDimension::Valid => {
            // A `FOR VALID_TIME AS OF` point and a valid range are the same axis —
            // contradictory. A `FOR SYSTEM_TIME AS OF` is the other axis and is kept
            // (it pins the system snapshot in `resolve_snapshots`).
            if stmt
                .temporal
                .as_of
                .iter()
                .any(|a| matches!(a.dimension, TimeDimension::Valid))
            {
                return Err(SelectError::UnsupportedValidRange(
                    "a FOR VALID_TIME AS OF point qualifier cannot be combined with a FOR VALID_TIME FROM/BETWEEN range"
                        .to_owned(),
                ));
            }
            // `resolve_as_of` folds to bare µs; on the valid axis they are
            // valid-time instants, so carry them in the valid-time newtype.
            let from = ValidTimeMicros(resolve_as_of(&range.from, now)?.0);
            let to = ValidTimeMicros(resolve_as_of(&range.to, now)?.0);
            if !range_well_formed(from.0, to.0, range.closed_upper) {
                return Err(SelectError::EmptyValidRange {
                    from: from.0,
                    to: to.0,
                    closed_upper: range.closed_upper,
                });
            }
            Ok(Some(BoundTemporalRange::Valid(ValidTimeRange {
                from,
                to,
                closed_upper: range.closed_upper,
            })))
        }
    }
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

/// The single `SELECT` body of a query statement (and its enclosing [`Query`],
/// which carries the result-shaping clauses), after rejecting every query- and
/// select-level clause outside the supported surface.
///
/// A clause [`BoundSelect`] cannot represent (`WITH RECURSIVE`, `QUALIFY`,
/// locking, …) must be rejected, not silently dropped when the plan is later
/// executed. The aggregate (`GROUP BY` / `HAVING`) and result-shaping clauses —
/// `ORDER BY`, `LIMIT`/`OFFSET`/`FETCH`, `DISTINCT` — are bound, not rejected
/// ([STL-171], [STL-263], [STL-265]); their own unsupported shapes surface in
/// [`bind_aggregate`] / [`bind_having`] / [`bind_order_by`] / [`bind_limit_offset`]
/// / [`bind_distinct`] with precise reasons.
fn single_select(body: &SqlStatement) -> Result<(&Query, &Select), SelectError> {
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
    Ok((query, select))
}

/// Reject query-level clauses outside the supported surface. `WHERE` lives on
/// the inner `Select` and is deliberately *kept* (lowered downstream), and the
/// result-shaping clauses (`ORDER BY` / `LIMIT` / `OFFSET` / `FETCH`) bind
/// ([STL-263]).
fn reject_unsupported_query_clauses(query: &Query) -> Result<(), SelectError> {
    let reject = |what| Err(SelectError::UnsupportedClause(what));
    // A `WITH` clause is bound, not rejected, since [STL-242] (non-recursive CTEs);
    // `bind_with_list` rejects `WITH RECURSIVE` with its own diagnostic.
    if !query.locks.is_empty() {
        return reject("FOR UPDATE/SHARE");
    }
    Ok(())
}

/// Reject select-level clauses outside the supported surface — anything that
/// aggregates or otherwise transforms the row set [`BoundSelect`] does not
/// model. `WHERE` ([`Select::selection`]) is allowed; `DISTINCT` binds
/// ([STL-263], its unsupported `DISTINCT ON` shape surfaces in
/// [`bind_distinct`]).
fn reject_unsupported_select_clauses(select: &Select) -> Result<(), SelectError> {
    let reject = |what| Err(SelectError::UnsupportedClause(what));
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
    // `HAVING` is bound as the aggregate query's post-grouping filter ([STL-265],
    // in `bind_having`); the join path rejects it with its own diagnostic
    // (`bind_join`), since `HAVING` over a join is a tracked follow-up.
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

/// The outer-query context a projection item binds against ([STL-303]): the
/// resolved schema/table (for column resolution), the relation alias and the
/// per-statement snapshot (for a scalar subquery's correlation scope and snapshot
/// inheritance), and whether the relation is a materialized CTE / derived table
/// (which has no provenance pseudo-columns).
struct ProjectionScope<'a> {
    schema: &'a TableSchema,
    table: &'a str,
    alias: Option<&'a str>,
    materialized: bool,
    catalog: &'a Catalog,
    snapshot: SystemTimeMicros,
    valid_snapshot: Option<SystemTimeMicros>,
}

/// Lower the projection list to [`Projection`] ([STL-303]): `*` ([`Projection::All`]),
/// or a [`ProjectionItem`] per select-list entry — a bare column (optionally
/// `AS`-aliased), a computed scalar expression, or an uncorrelated scalar subquery.
///
/// Every bare column is validated against the schema live at the snapshot (a
/// provenance pseudo-column is accepted on a base table, [STL-247], but not on a
/// materialized relation). A computed expression reuses the `WHERE` [`BoundScalar`]
/// vocabulary; a scalar subquery binds under the same per-statement snapshot
/// (docs/16 §6) and must be uncorrelated and single-column.
#[allow(clippy::too_many_arguments)]
fn bind_projection(
    select: &Select,
    schema: &TableSchema,
    table: &str,
    materialized: bool,
    ctx: &BindContext,
    snapshot: SystemTimeMicros,
    valid_snapshot: Option<SystemTimeMicros>,
) -> Result<Projection, SelectError> {
    // `SELECT *` is the lone wildcard item.
    if let [SelectItem::Wildcard(_)] = select.projection.as_slice() {
        return Ok(Projection::All);
    }
    let scope = ProjectionScope {
        schema,
        table,
        alias: select_table_alias(select),
        materialized,
        catalog: ctx.catalog,
        snapshot,
        valid_snapshot,
    };
    let mut items = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::Wildcard(_) => {
                return Err(SelectError::UnsupportedProjection(
                    "`*` mixed with named columns".to_owned(),
                ));
            }
            other => return Err(SelectError::UnsupportedProjection(other.to_string())),
        };
        items.push(bind_projection_item(unwrap_nested(expr), alias, &scope)?);
    }
    Ok(Projection::Items(items))
}

/// Bind one select-list entry to a [`ProjectionItem`] ([STL-303]): a bare column,
/// an uncorrelated scalar subquery, or a computed scalar expression — in that order
/// of recognition. `alias` is the explicit `AS` name, if any.
fn bind_projection_item(
    expr: &Expr,
    alias: Option<String>,
    scope: &ProjectionScope,
) -> Result<ProjectionItem, SelectError> {
    // 1. A bare addressable column — the fast path. An `AS` alias only renames the
    //    output; the source column is still looked up by its own name.
    if let Expr::Identifier(id) = expr {
        let source = id.value.clone();
        let pseudo_ok = !scope.materialized && provenance::pseudo_column_type(&source).is_some();
        if scope.schema.column(&source).is_none() && !pseudo_ok {
            return Err(SelectError::UnknownColumn {
                table: scope.table.to_owned(),
                column: source,
            });
        }
        let name = alias.unwrap_or_else(|| source.clone());
        return Ok(ProjectionItem {
            name,
            value: ProjectionValue::Column(source),
        });
    }
    // 2. An uncorrelated scalar subquery `(SELECT …)`.
    if let Some(query) = as_subquery(expr) {
        return bind_projected_subquery(query, alias, scope);
    }
    // 3. A computed scalar expression (`a + 1`, `a + b`, `1 + 2`,
    //    `a + (SELECT max(b) FROM s)`).
    let (scalar, ty) = bind_projection_scalar(expr, scope)?;
    let name = alias.unwrap_or_else(|| "?column?".to_owned());
    Ok(ProjectionItem {
        name,
        value: ProjectionValue::Computed { scalar, ty },
    })
}

/// Bind a scalar subquery in the SELECT list ([STL-303], [STL-331]).
///
/// The inner binds under the same per-statement snapshot as the outer (docs/16 §6),
/// inheriting any `FOR VALID_TIME AS OF` pin, and must be **single-column**. It may
/// be **uncorrelated** ([STL-303]) — resolved once and broadcast as a constant — or
/// **correlated** ([STL-331]): [`bind_inner_query`] lifts a single-comparison
/// correlated `WHERE` off the inner and returns the [`Correlation`], which rides on
/// the [`ProjectionValue::Subquery`] for the engine's per-row re-execution (the
/// [STL-239] machinery, producing a projected cell instead of a row keep/drop). The
/// inner is capped at two rows so the engine can raise the `21000` cardinality
/// violation without materializing an arbitrarily large result. An unaliased
/// subquery inherits the inner's sole output column name (the Postgres rule).
fn bind_projected_subquery(
    query: &Query,
    alias: Option<String>,
    scope: &ProjectionScope,
) -> Result<ProjectionItem, SelectError> {
    let inner_ctx = BindContext {
        snapshot: scope.snapshot,
        catalog: scope.catalog,
    };
    let outer = OuterScope {
        table: scope.table,
        alias: scope.alias,
        schema: scope.schema,
    };
    let (mut inner, correlation) =
        bind_inner_query(query, &outer, &inner_ctx, /* exists = */ false)?;
    let (inner_name, ty) = sole_output_column(&inner, scope.catalog)?;
    inner.limit = Some(inner.limit.map_or(2, |existing| existing.min(2)));
    inherit_valid_snapshot(&mut inner, scope.valid_snapshot, scope.catalog)?;
    let name = alias.unwrap_or(inner_name);
    Ok(ProjectionItem {
        name,
        value: ProjectionValue::Subquery {
            subquery: Box::new(inner),
            ty,
            correlation,
        },
    })
}

/// Bind a computed (non-bare-column) projection expression to a [`BoundScalar`]
/// and its result type ([STL-303], [STL-332]).
///
/// Reuses the `WHERE` scalar vocabulary, generalized past the single-column anchor:
/// integer arithmetic over **any number of columns** (`a + b`), folded literals
/// (`a + 1`), **column-free** arithmetic (`1 + 2`), and an embedded **uncorrelated
/// scalar subquery** operand (`a + (SELECT max(b) FROM s)`). Each column / subquery
/// types itself independently; Stele does not implicitly coerce, so the two
/// operands of an arithmetic node must share one integer type — with the single
/// exception that an `int4` *literal* widens to meet an `int8` operand, exactly as
/// the single-anchor fold did. A non-integer operand in arithmetic, or two concrete
/// operands of different types, is an
/// [`UnsupportedProjection`](SelectError::UnsupportedProjection).
///
/// Columns resolve against **schema columns only** — unlike a `WHERE` scalar, a
/// provenance pseudo-column ([STL-247]) is *not* usable inside a computed
/// expression (the engine's `eval_projection_scalar` decodes schema columns alone),
/// so a pseudo-column here is [`SelectError::UnknownColumn`]; it stays projectable
/// only as a bare column.
fn bind_projection_scalar(
    expr: &Expr,
    scope: &ProjectionScope,
) -> Result<(BoundScalar, LogicalType), SelectError> {
    let (scalar, kind) = bind_computed_operand(expr, scope)?;
    Ok((scalar, kind.ty()))
}

/// Whether a bound computed-projection operand's type is fixed by a column or
/// subquery — which never coerces — or came from a literal, which re-folds to a
/// sibling's concrete integer type (an `int4` literal widens to `int8`). This is
/// what lets `a + b` require an exact type match while `a + 1` lets the literal
/// adapt, the projection mirror of the single-anchor `WHERE` fold.
#[derive(Clone, Copy)]
enum OperandKind {
    /// A column or subquery operand: its type is fixed and never coerces.
    Concrete(LogicalType),
    /// A literal (or all-literal subtree): its natural type, re-foldable from
    /// `int4` to `int8` to meet an `int8` sibling.
    Literal(LogicalType),
}

impl OperandKind {
    /// The operand's resolved type.
    const fn ty(self) -> LogicalType {
        match self {
            Self::Concrete(ty) | Self::Literal(ty) => ty,
        }
    }

    /// Whether the operand came from a literal (so it may widen `int4` → `int8`).
    const fn is_literal(self) -> bool {
        matches!(self, Self::Literal(_))
    }
}

/// Bind one operand of a computed projection expression bottom-up, returning the
/// [`BoundScalar`] and its [`OperandKind`] ([STL-332]). A bare column resolves to
/// its schema index, an embedded `(SELECT …)` to a resolve-once [`BoundScalar::Subquery`],
/// an arithmetic node recurses and [reconciles](reconcile_arith) its operand types,
/// and any other leaf is a column-free literal ([`infer_constant_projection`]).
fn bind_computed_operand(
    expr: &Expr,
    scope: &ProjectionScope,
) -> Result<(BoundScalar, OperandKind), SelectError> {
    let expr = unwrap_nested(expr);
    if let Expr::Identifier(id) = expr {
        let index =
            column_index(scope.schema, &id.value).ok_or_else(|| SelectError::UnknownColumn {
                table: scope.table.to_owned(),
                column: id.value.clone(),
            })?;
        let ty = scope.schema.columns()[index].ty();
        return Ok((BoundScalar::Column(index), OperandKind::Concrete(ty)));
    }
    if let Some(query) = as_subquery(expr) {
        let (scalar, ty) = bind_embedded_subquery(query, scope)?;
        return Ok((scalar, OperandKind::Concrete(ty)));
    }
    if let Expr::BinaryOp { left, op, right } = expr {
        let Some(arith) = arith_op(op) else {
            return Err(SelectError::UnsupportedProjection(format!(
                "operator `{op}` is not supported in a computed select item"
            )));
        };
        let (left_scalar, left_kind) = bind_computed_operand(left, scope)?;
        let (right_scalar, right_kind) = bind_computed_operand(right, scope)?;
        let (ty, left_scalar, right_scalar) =
            reconcile_arith(left_kind, left_scalar, right_kind, right_scalar)?;
        // The node is constant only when *both* sides are — a column or subquery
        // anywhere fixes the type concretely (no further widening).
        let kind = if left_kind.is_literal() && right_kind.is_literal() {
            OperandKind::Literal(ty)
        } else {
            OperandKind::Concrete(ty)
        };
        return Ok((
            BoundScalar::Arith {
                op: arith,
                left: Box::new(left_scalar),
                right: Box::new(right_scalar),
            },
            kind,
        ));
    }
    // A column-free literal leaf (`1`, `'x'`, `TRUE`): its natural type.
    let value = infer_constant_projection(expr)?;
    let ty = value.logical_type();
    Ok((BoundScalar::Literal(value), OperandKind::Literal(ty)))
}

/// Reconcile the operand types of a computed arithmetic node, returning the result
/// type and the (possibly widened) operands ([STL-332]).
///
/// Arithmetic is integer-only (`int4` / `int8` — the evaluator's two kernels), and
/// Stele does not implicitly coerce: two operands must share one integer type. The
/// sole give is that an `int4` **literal** widens to `int8` to meet an `int8`
/// sibling (a concrete `int4` column / subquery does not), exactly the latitude the
/// single-anchor `WHERE` fold took when it folded a literal to its anchor column.
fn reconcile_arith(
    left_kind: OperandKind,
    left_scalar: BoundScalar,
    right_kind: OperandKind,
    right_scalar: BoundScalar,
) -> Result<(LogicalType, BoundScalar, BoundScalar), SelectError> {
    let is_int = |ty| matches!(ty, LogicalType::Int4 | LogicalType::Int8);
    if !is_int(left_kind.ty()) || !is_int(right_kind.ty()) {
        return Err(SelectError::UnsupportedProjection(format!(
            "arithmetic in a computed select item needs integer operands, got {} and {}",
            left_kind.ty(),
            right_kind.ty()
        )));
    }
    // Same integer type ⇒ no coercion needed.
    if left_kind.ty() == right_kind.ty() {
        return Ok((left_kind.ty(), left_scalar, right_scalar));
    }
    // Mixed `int4` / `int8`: only an `int4` *literal* may widen to meet an `int8`
    // sibling. A concrete `int4` (a column / subquery) does not coerce.
    match (left_kind, right_kind) {
        (OperandKind::Literal(LogicalType::Int4), _) if right_kind.ty() == LogicalType::Int8 => {
            Ok((
                LogicalType::Int8,
                widen_int4_literal(left_scalar),
                right_scalar,
            ))
        }
        (_, OperandKind::Literal(LogicalType::Int4)) if left_kind.ty() == LogicalType::Int8 => {
            Ok((
                LogicalType::Int8,
                left_scalar,
                widen_int4_literal(right_scalar),
            ))
        }
        _ => Err(SelectError::UnsupportedProjection(format!(
            "operands of a computed select item have incompatible types {} and {} \
             (Stele does not implicitly coerce)",
            left_kind.ty(),
            right_kind.ty()
        ))),
    }
}

/// Widen every `int4` literal in an all-literal computed subtree to `int8`
/// ([STL-332]), so an `int4` constant can meet an `int8` operand under the
/// evaluator's same-type arithmetic. Only ever called on an
/// [`OperandKind::Literal`] `int4` subtree, which by construction holds nothing but
/// `int4` literals and arithmetic over them; the final arm is an unreachable
/// pass-through kept total rather than panicking.
fn widen_int4_literal(scalar: BoundScalar) -> BoundScalar {
    match scalar {
        BoundScalar::Literal(ScalarValue::Int4(v)) => {
            BoundScalar::Literal(ScalarValue::Int8(i64::from(v)))
        }
        BoundScalar::Arith { op, left, right } => BoundScalar::Arith {
            op,
            left: Box::new(widen_int4_literal(*left)),
            right: Box::new(widen_int4_literal(*right)),
        },
        other => other,
    }
}

/// Bind a scalar subquery used as an **operand** inside a computed projection
/// expression ([STL-332]): `a + (SELECT max(b) FROM s)`.
///
/// The inner binds under the outer's per-statement snapshot (docs/16 §6), inheriting
/// any `FOR VALID_TIME AS OF` pin, and must be **single-column**. It must be
/// **uncorrelated** — resolved once by the engine ([STL-303] `resolve_scalar_subquery`)
/// and fed into the surrounding per-row arithmetic as a constant. A *correlated*
/// embedded operand (the inner referencing an outer column) is not bound here yet:
/// the whole-projection STL-331 path handles correlation, but threading a per-row
/// re-resolved value into per-row arithmetic is a tracked follow-up. The inner is
/// capped at two rows so a `>1`-row result raises the `21000` cardinality violation
/// without materializing an unbounded inner.
fn bind_embedded_subquery(
    query: &Query,
    scope: &ProjectionScope,
) -> Result<(BoundScalar, LogicalType), SelectError> {
    let inner_ctx = BindContext {
        snapshot: scope.snapshot,
        catalog: scope.catalog,
    };
    let outer = OuterScope {
        table: scope.table,
        alias: scope.alias,
        schema: scope.schema,
    };
    let (mut inner, correlation) =
        bind_inner_query(query, &outer, &inner_ctx, /* exists = */ false)?;
    if correlation.is_some() {
        return Err(SelectError::Subquery(
            "a correlated subquery inside a computed expression is not supported yet — only an \
             uncorrelated, resolve-once embedded subquery (`a + (SELECT max(b) FROM s)`)"
                .to_owned(),
        ));
    }
    let (_inner_name, ty) = sole_output_column(&inner, scope.catalog)?;
    inner.limit = Some(inner.limit.map_or(2, |existing| existing.min(2)));
    inherit_valid_snapshot(&mut inner, scope.valid_snapshot, scope.catalog)?;
    Ok((BoundScalar::Subquery(Box::new(inner)), ty))
}

/// Infer the value and type of a column-free constant projection **leaf** ([STL-303]):
/// an integer literal (`int4`, or `int8` if it overflows `i32`), a single-quoted
/// string (`text`), or a boolean (`bool`). Column-free *arithmetic* (`1 + 2`) is
/// composed from these leaves by [`bind_computed_operand`]; anything else here (NULL,
/// a float, a non-literal) is an
/// [`UnsupportedProjection`](SelectError::UnsupportedProjection).
fn infer_constant_projection(expr: &Expr) -> Result<ScalarValue, SelectError> {
    if let Some(digits) = fold::signed_number(expr) {
        if let Ok(v) = digits.parse::<i32>() {
            return Ok(ScalarValue::Int4(v));
        }
        if let Ok(v) = digits.parse::<i64>() {
            return Ok(ScalarValue::Int8(v));
        }
        // A wider-than-`i64` integer or a decimal (no `float8` literal codec) falls
        // through to the generic rejection below.
    }
    match fold::literal(expr) {
        Some(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
        Some(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        _ => Err(SelectError::UnsupportedProjection(format!(
            "constant select item `{expr}` is not a supported literal"
        ))),
    }
}

/// Bind the `SELECT [ALL | DISTINCT]` set quantifier ([STL-263]).
///
/// `DISTINCT` deduplicates the full projected row. The Postgres
/// `DISTINCT ON (…)` extension is a different operation (one row per group,
/// picked by an ordering rule) and stays out — rejected, never approximated.
const fn bind_distinct(select: &Select) -> Result<bool, SelectError> {
    match &select.distinct {
        None | Some(Distinct::All) => Ok(false),
        Some(Distinct::Distinct) => Ok(true),
        Some(Distinct::On(_)) => Err(SelectError::UnsupportedClause("DISTINCT ON")),
    }
}

/// Bind `ORDER BY` into [`BoundSortKey`]s ([STL-263]): bare column names, each
/// optionally `ASC` / `DESC` (NULL placement pinned to the Postgres defaults —
/// see [`BoundSortKey`]), first key outermost.
fn bind_order_by(
    query: &Query,
    schema: &TableSchema,
    table: &str,
    distinct: bool,
    aggregate: Option<&BoundAggregate>,
    projection: &Projection,
) -> Result<Vec<BoundSortKey>, SelectError> {
    let Some(order_by) = &query.order_by else {
        return Ok(Vec::new());
    };
    if order_by.interpolate.is_some() {
        return Err(SelectError::UnsupportedOrderBy("INTERPOLATE".to_owned()));
    }
    let exprs = match &order_by.kind {
        OrderByKind::All(_) => {
            return Err(SelectError::UnsupportedOrderBy("ORDER BY ALL".to_owned()));
        }
        OrderByKind::Expressions(exprs) => exprs,
    };
    exprs
        .iter()
        .map(|key| bind_sort_key(key, schema, table, distinct, aggregate, projection))
        .collect()
}

/// Bind one `ORDER BY` key, resolving its name the way Postgres does: the
/// **select list first** (an aggregate query's output columns, aliases
/// included; a plain query's projected columns), binding by output position. A
/// plain non-`DISTINCT` query may also sort on an unprojected **schema**
/// column; with `DISTINCT` that key is ambiguous after deduplication — the
/// 42P10 [`SelectError::DistinctOrderBy`] — and an aggregate query's output
/// rows have no schema columns to fall back to.
fn bind_sort_key(
    key: &OrderByExpr,
    schema: &TableSchema,
    table: &str,
    distinct: bool,
    aggregate: Option<&BoundAggregate>,
    projection: &Projection,
) -> Result<BoundSortKey, SelectError> {
    if key.with_fill.is_some() {
        return Err(SelectError::UnsupportedOrderBy("WITH FILL".to_owned()));
    }
    if key.options.nulls_first.is_some() {
        return Err(SelectError::UnsupportedOrderBy(
            "explicit NULLS FIRST/LAST (the Postgres defaults apply: \
             NULLS LAST under ASC, NULLS FIRST under DESC)"
                .to_owned(),
        ));
    }
    let Expr::Identifier(ident) = &key.expr else {
        return Err(SelectError::UnsupportedOrderBy(format!(
            "key `{}` — only a bare column name sorts",
            key.expr
        )));
    };
    let name = ident.value.as_str();
    let descending = key.options.asc == Some(false);
    let output = |pos| {
        Ok(BoundSortKey {
            column: SortTarget::Output(pos),
            descending,
        })
    };

    // An aggregate query's result rows are its output columns — there is no
    // schema column to fall back to.
    if let Some(agg) = aggregate {
        return agg.columns.iter().position(|(n, _)| n == name).map_or_else(
            || {
                Err(SelectError::UnsupportedOrderBy(format!(
                    "column {name:?} is not a select-list column of the aggregate query"
                )))
            },
            output,
        );
    }

    // Plain query: the select list first. Under `SELECT *` the output order is
    // the schema order, so a schema hit *is* the output position.
    let output_pos = match projection {
        Projection::All => column_index(schema, name),
        Projection::Items(items) => items.iter().position(|item| item.name == name),
    };
    if let Some(pos) = output_pos {
        return output(pos);
    }
    let idx = column_index(schema, name).ok_or_else(|| SelectError::UnknownColumn {
        table: table.to_owned(),
        column: name.to_owned(),
    })?;
    if distinct {
        // Sorting on a column DISTINCT discarded is ambiguous — Postgres's
        // 42P10 (invalid_column_reference).
        return Err(SelectError::DistinctOrderBy);
    }
    Ok(BoundSortKey {
        column: SortTarget::Schema(idx),
        descending,
    })
}

/// Bind `LIMIT n` / `OFFSET m` / `FETCH FIRST n ROWS ONLY` to concrete row
/// counts ([STL-263]), returning `(limit, offset)`.
///
/// Only non-negative integer literals bind (a negative or non-literal count is
/// rejected with the reason); `LIMIT ALL` is explicitly unlimited; the
/// standard `FETCH FIRST [n] ROWS ONLY` is the `LIMIT n` alias (count omitted
/// = 1, as the standard reads it). Giving both `LIMIT` and `FETCH` names two
/// counts for one query and is rejected, as Postgres does.
fn bind_limit_offset(query: &Query) -> Result<(Option<u64>, u64), SelectError> {
    let mut limit: Option<u64> = None;
    let mut offset: u64 = 0;
    if let Some(clause) = &query.limit_clause {
        match clause {
            LimitClause::LimitOffset {
                limit: count,
                offset: skip,
                limit_by,
            } => {
                if !limit_by.is_empty() {
                    return Err(SelectError::UnsupportedLimit("LIMIT … BY".to_owned()));
                }
                // `LIMIT ALL` parses to no count expression — explicitly
                // unlimited, same as omitting the clause.
                limit = count.as_ref().map(|e| row_count(e, "LIMIT")).transpose()?;
                if let Some(skip) = skip {
                    offset = row_count(&skip.value, "OFFSET")?;
                }
            }
            LimitClause::OffsetCommaLimit { .. } => {
                return Err(SelectError::UnsupportedLimit(
                    "the MySQL `LIMIT <offset>, <limit>` form (use LIMIT … OFFSET …)".to_owned(),
                ));
            }
        }
    }
    if let Some(fetch) = &query.fetch {
        if limit.is_some() {
            return Err(SelectError::UnsupportedLimit(
                "both LIMIT and FETCH in one query".to_owned(),
            ));
        }
        if fetch.with_ties {
            return Err(SelectError::UnsupportedLimit(
                "FETCH … WITH TIES".to_owned(),
            ));
        }
        if fetch.percent {
            return Err(SelectError::UnsupportedLimit("FETCH … PERCENT".to_owned()));
        }
        limit = Some(match &fetch.quantity {
            Some(expr) => row_count(expr, "FETCH")?,
            None => 1,
        });
    }
    Ok((limit, offset))
}

/// Read a `LIMIT` / `OFFSET` / `FETCH` row count: a **non-negative integer
/// literal** only ([STL-263]). A negative count parses as a unary minus over
/// the literal, so it lands in the non-literal arm with the offending text in
/// the message; an expression or parameter is a tracked later breadth.
fn row_count(expr: &Expr, clause: &str) -> Result<u64, SelectError> {
    let unsupported = |got: &dyn std::fmt::Display| {
        SelectError::UnsupportedLimit(format!(
            "{clause} takes a non-negative integer literal, got `{got}`"
        ))
    };
    let Expr::Value(value) = expr else {
        return Err(unsupported(&expr));
    };
    match &value.value {
        Value::Number(digits, _) => digits.parse::<u64>().map_err(|_| unsupported(&digits)),
        other => Err(unsupported(&other)),
    }
}

/// Inject a default `LIMIT` on an unbounded **plain single-table** `SELECT` — the
/// interactive-client result cap ([STL-306]).
///
/// A bare `SELECT … FROM big_table` carries no row bound, so it reads the whole
/// table. Over the **simple** query protocol — the `stele` shell, `psql`, any
/// ad-hoc tool that types a statement and consumes every row at once — that
/// floods the terminal and the client's memory. When `stmt` is a plain
/// single-table read with no **finite** `LIMIT`/`FETCH` count — a bare read, an
/// `OFFSET`-only read, or `LIMIT ALL` all qualify — this rewrites it as if the
/// user had written `LIMIT max_rows`, so the read returns at most `max_rows` rows
/// (an existing `OFFSET` is kept). The wire front end applies it on the simple-query path only;
/// the extended protocol leaves the row count to the client's `Execute`
/// `max_rows` — a driver fetches exactly what it asked for.
///
/// Deliberately narrow:
/// * **Plain single-table reads only.** Only a one-table `SELECT` over a named
///   relation accepts a `LIMIT` in Stele. A `JOIN` and a table-valued-function
///   read (the `stele_history`/`stele_audit`/`stele_segments` introspection calls,
///   recognized as an *unshaped* `SELECT *`) both reject result shaping, so
///   injecting a `LIMIT` would turn a working query into an error — they are left
///   untouched, as are a set operation and a `FROM`-less constant `SELECT`.
/// * **Top-level only.** It rewrites the statement's own `limit_clause`, never a
///   subquery's — capping `WHERE id IN (SELECT … )` would change the *result*,
///   not just truncate the output. A subquery lives inside the query body and is
///   left untouched.
/// * **No finite count only.** A query that already chose a finite bound — an
///   explicit `LIMIT n` (including `LIMIT 0`) or a `FETCH FIRST … ROWS` — keeps
///   it. Everything the binder reads as *unlimited* is capped: a bare query, an
///   `OFFSET`-only query, and `LIMIT ALL` (which `sqlparser` collapses to no
///   count — the binder already treats it identically to no clause). An existing
///   `OFFSET` is preserved, and the rewrite is idempotent.
/// * **Reads only.** A non-query statement (`INSERT`/`UPDATE`/`DELETE`, DDL, an
///   admin command) is returned unchanged — including the `SELECT` *source* of
///   an `INSERT … SELECT`, which is not a top-level query here.
///
/// [STL-306]: https://allegromusic.atlassian.net/browse/STL-306
pub fn cap_unbounded_select(stmt: &mut Statement, max_rows: u64) {
    // A `FOR { SYSTEM_TIME | VALID_TIME }` range scan now binds a `LIMIT` like any
    // plain single-table read ([STL-329]), so it is capped too: a `… FROM big_table
    // FOR SYSTEM_TIME FROM a TO b` history read can return far more rows than a point
    // read and would flood an interactive client just the same. An explicit `LIMIT`
    // or the extended protocol still reads it all.
    let Some(SqlStatement::Query(query)) = stmt.sql_mut() else {
        return;
    };
    // Only a plain single-table read accepts a `LIMIT`; everything else (a JOIN, a
    // table-valued introspection call, a set operation, a constant) would error if
    // one were injected, so leave it untouched.
    if !is_plain_single_table_read(query) {
        return;
    }
    // A `FETCH FIRST … ROWS` count is the caller's chosen bound.
    if query.fetch.is_some() {
        return;
    }
    let count = || Expr::Value(Value::Number(max_rows.to_string(), false).into());
    match &mut query.limit_clause {
        // An explicit `LIMIT n` count, or the MySQL `LIMIT a, b` form (rejected by
        // the binder, but its own bound): leave the caller's choice in place.
        Some(
            LimitClause::LimitOffset { limit: Some(_), .. } | LimitClause::OffsetCommaLimit { .. },
        ) => {}
        // A clause with no count (`OFFSET m`, `LIMIT ALL OFFSET m`): fill in the
        // cap and keep the offset the caller gave.
        Some(LimitClause::LimitOffset { limit, .. }) => *limit = Some(count()),
        // No `LIMIT`/`OFFSET` clause at all (a bare read or `LIMIT ALL`): add the cap.
        None => {
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: Some(count()),
                offset: None,
                limit_by: Vec::new(),
            });
        }
    }
}

/// Apply a connection's session time context ([STL-246]) to a statement.
///
/// Works by **injecting an explicit `FOR <dim> AS OF <instant>` qualifier** for
/// each axis the session pins and the statement does not already qualify itself.
///
/// This is the whole mechanism behind `SET stele.system_time = …`: a session-pinned
/// read is rewritten into the exact explicit-`AS OF` form, so the engine's existing
/// snapshot resolution, read-your-own-writes overlay rules, and query-stats path all
/// handle it unchanged — and a session-pinned read is byte-for-byte the explicit-`AS
/// OF` read by construction (the equivalence the ticket's oracle pins down). The
/// instants are pre-resolved microsecond values (folded once, at the time of the
/// `SET`), injected as integer literals that re-fold to themselves.
///
/// Deliberately narrow:
/// * **Plain single-table reads and two-table joins** of named base tables are
///   eligible ([STL-325]). A table-valued introspection call (`stele_history('t')`),
///   a set operation, a derived table, a `FROM`-less constant `SELECT`, and an N-way
///   join are left untouched — the session pin does not apply (they read live), as
///   for [`cap_unbounded_select`].
/// * **Per-axis, by applicability.** The **system** pin is injected over any eligible
///   shape — the system axis is always present, so a `FOR SYSTEM_TIME AS OF` binds
///   over a single table or a join alike ([STL-243]). The **valid** pin is injected
///   only when *every* input opts into a valid axis (`is_valid_time_table`, the same
///   check the binder makes per join side, [STL-243]); when an input is system-only
///   the valid pin is silently withheld (the read stays live on the valid axis)
///   rather than injected into a bind error — mirroring `inherit_valid_snapshot`, the
///   subquery-inheritance twin. A CTE / derived name shadows any same-named base
///   table and is system-only, so it never carries a valid pin.
/// * **Per-axis, explicit wins.** An axis the statement already qualifies with its
///   own `FOR <dim> AS OF` keeps that qualifier; the session pin fills only the
///   axes left unqualified.
/// * **Reads only.** A non-`SELECT` statement is returned unchanged.
///
/// `is_valid_time_table` answers whether a base table opts into a valid axis — the
/// engine resolves it against the live catalog (`SessionEngine::table_has_valid_axis`);
/// it is consulted only when a valid pin is set and the shape is eligible.
///
/// [STL-246]: https://allegromusic.atlassian.net/browse/STL-246
/// [STL-243]: https://allegromusic.atlassian.net/browse/STL-243
/// [STL-325]: https://allegromusic.atlassian.net/browse/STL-325
pub fn apply_session_time(
    stmt: &mut Statement,
    system: Option<SystemTimeMicros>,
    valid: Option<SystemTimeMicros>,
    is_valid_time_table: impl Fn(&str) -> bool,
) {
    if system.is_none() && valid.is_none() {
        return;
    }
    // A `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` range scan ([STL-244])
    // lifts its qualifier off the token stream like an `AS OF` point, so the residual
    // query still looks like a plain single-table read here. The binder rejects a
    // range combined with a point `AS OF`, so injecting a session pin would turn an
    // otherwise-valid range scan into a bind error — leave it untouched, mirroring
    // [`cap_unbounded_select`].
    if stmt.temporal.range.is_some() {
        return;
    }
    // Only a read whose shape accepts an `AS OF` is eligible: a plain single-table
    // read or a two-table join of named base tables. Everything else would error if
    // one were injected, so leave it (and the session pin) untouched.
    let Some(SqlStatement::Query(query)) = stmt.sql() else {
        return;
    };
    let Some(targets) = session_pin_targets(query) else {
        return;
    };
    let has_system = stmt
        .temporal
        .as_of
        .iter()
        .any(|a| a.dimension == TimeDimension::System);
    let has_valid = stmt
        .temporal
        .as_of
        .iter()
        .any(|a| a.dimension == TimeDimension::Valid);
    // Resolve valid-axis eligibility only when a valid pin is actually set and the
    // statement does not already qualify the valid axis — a system-only pin consults
    // neither the catalog predicate nor the `WITH` list (`&&` short-circuits). The
    // borrow of `query` is immutable and ends here, before the injection mutates
    // `stmt.temporal`. A CTE / derived name shadows any same-named base table and is
    // system-only, so a target the `WITH` list names never carries a valid pin.
    let valid_eligible = valid.is_some() && !has_valid && {
        let cte_names = cte_names(query);
        targets
            .iter()
            .all(|t| !cte_names.contains(t) && is_valid_time_table(t))
    };
    // An explicit microsecond instant, the form `resolve_as_of` reads straight back
    // to the same value — so the injected qualifier is identical to one a user
    // could have written by hand.
    let instant =
        |micros: SystemTimeMicros| Expr::Value(Value::Number(micros.0.to_string(), false).into());
    // The system pin applies over any eligible shape; the valid pin only when every
    // input has a valid axis (`valid_eligible`, else withheld — not an error).
    if let Some(micros) = system
        && !has_system
    {
        stmt.temporal.as_of.push(AsOf {
            dimension: TimeDimension::System,
            timestamp: instant(micros),
        });
    }
    if let Some(micros) = valid
        && valid_eligible
    {
        stmt.temporal.as_of.push(AsOf {
            dimension: TimeDimension::Valid,
            timestamp: instant(micros),
        });
    }
}

/// The base-table inputs a session pin may target ([STL-325]), or `None` if the
/// read's shape is not pinnable. A plain single-table read returns its one table; a
/// two-table join returns both; an N-way left-deep join ([STL-323]) returns every
/// input, seed-first in chain order — the binder threads an `AS OF` to all of them
/// alike. A table-valued function (an `args`-carrying relation, e.g.
/// `stele_history('t')`), a derived table, a set operation (`UNION`), a `FROM`-less
/// constant `SELECT`, and a schema-qualified name make the whole read un-pinnable
/// (`None`) — the session pin is left un-injected, exactly the shapes
/// [`is_plain_single_table_read`] already excludes plus the now-included joins.
///
/// A returned name may be a CTE / derived-table reference (indistinguishable from a
/// base table at this layer); the caller resolves valid-axis eligibility against the
/// catalog and the `WITH` list, which such a relation fails.
fn session_pin_targets(query: &Query) -> Option<Vec<&str>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [from] = select.from.as_slice() else {
        return None;
    };
    // The seed input, then each joined input in the left-deep chain ([STL-323]) — one
    // entry per `JOIN`, so a single-table read yields one target and an N-way join
    // yields all of them. Any non-base-table input (a TVF, derived table, or
    // schema-qualified name) makes the whole read un-pinnable (`?` short-circuits).
    let mut targets = vec![base_table_name(&from.relation)?];
    for join in &from.joins {
        targets.push(base_table_name(&join.relation)?);
    }
    Some(targets)
}

/// The single unqualified identifier of a base-table [`TableFactor`], or `None` for a
/// table-valued function (`args` present), a derived table, or a schema-qualified
/// name — the relations a session pin must not inject an `AS OF` over ([STL-325]).
fn base_table_name(factor: &TableFactor) -> Option<&str> {
    match factor {
        TableFactor::Table {
            name, args: None, ..
        } => match name.0.as_slice() {
            [part] => part.as_ident().map(|id| id.value.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// The relation names a query's `WITH` list introduces — the CTEs / derived tables
/// that shadow any same-named base table. A session valid pin is withheld from a
/// target named here (a CTE's ephemeral schema is system-only — [STL-325]).
fn cte_names(query: &Query) -> Vec<&str> {
    query.with.as_ref().map_or_else(Vec::new, |with| {
        with.cte_tables
            .iter()
            .map(|cte| cte.alias.name.value.as_str())
            .collect()
    })
}

/// Whether `query` is a one-table `SELECT` over a **named relation** — the only
/// shape that accepts a `LIMIT` in Stele, and so the only shape
/// [`cap_unbounded_select`] may rewrite. A `JOIN`, a table-valued function (an
/// `args`-carrying relation, e.g. `stele_history('t')`), a set operation
/// (`UNION`), and a `FROM`-less constant `SELECT` all return `false`. A `WHERE`
/// (subquery included), `GROUP BY`/aggregate, `ORDER BY`, or `DISTINCT` over the
/// one table is fine — those bind on the single-table path with `LIMIT`.
fn is_plain_single_table_read(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    from.joins.is_empty() && matches!(&from.relation, TableFactor::Table { args: None, .. })
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

    /// The real session-pin valid-axis predicate ([STL-325]) against a catalog: a
    /// base table opts into a valid axis at `NOW`. The closure form
    /// [`apply_session_time`] consults to decide whether a valid pin is injectable.
    fn valid_axis(catalog: &Catalog, table: &str) -> bool {
        catalog
            .resolve(table, NOW)
            .is_some_and(|schema| schema.temporal().valid_time_enabled())
    }

    /// A fully-constant `PERIOD(from, to)` operand, the STL-165 shape.
    const fn const_period(from: i64, to: i64) -> BoundPeriod {
        BoundPeriod {
            from: PeriodEndpoint::Const(from),
            to: PeriodEndpoint::Const(to),
        }
    }

    /// The `WHERE` of a bare top-level `SELECT` (`SetExpr::Select`), or `None` for
    /// any other shape — enough for these single-`SELECT` describe tests.
    fn selection_of(stmt: &Statement) -> Option<&Expr> {
        let SqlStatement::Query(query) = stmt.sql()? else {
            return None;
        };
        match query.body.as_ref() {
            SetExpr::Select(select) => select.selection.as_ref(),
            _ => None,
        }
    }

    #[test]
    fn without_filter_lets_a_parameterized_select_describe() {
        // A `$1` in the WHERE makes `bind_select` fail to fold the comparand, so a
        // prepared `SELECT … WHERE k = $1` cannot bind before its parameter is
        // bound. Stripping the filter for statement-level Describe lets it bind —
        // and the projected output columns are the same regardless of the (absent)
        // parameter value, which is the whole point.
        let stmt = parse_one("SELECT id, balance FROM account WHERE id = $1");
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        assert!(
            selection_of(&stmt).is_some(),
            "the parsed query has a WHERE"
        );
        assert!(
            bind_select(&stmt, &ctx).is_err(),
            "a placeholder WHERE cannot bind"
        );

        let described = without_filter(&stmt);
        assert!(selection_of(&described).is_none(), "the WHERE is stripped");
        let bound = bind_select(&described, &ctx).expect("the stripped copy binds");
        assert_eq!(
            bound.projection,
            Projection::Items(vec![
                ProjectionItem::column("id"),
                ProjectionItem::column("balance"),
            ])
        );
        assert!(bound.filter.is_none(), "no filter survives the strip");
    }

    #[test]
    fn without_filter_clears_a_period_predicate_but_keeps_as_of() {
        // A lifted `WHERE PERIOD(...) <pred> PERIOD(...)` is removed too — its
        // endpoints can carry parameters just like an equality WHERE — while the
        // `AS OF` qualifier, which selects the schema version the columns resolve
        // under, is preserved.
        let stmt = parse_one(
            "SELECT balance FROM account FOR SYSTEM_TIME AS OF 1700000000000000 \
             WHERE PERIOD(10, 20) CONTAINS PERIOD(30, 40)",
        );
        assert!(stmt.temporal.period_predicate.is_some());
        let described = without_filter(&stmt);
        assert!(
            described.temporal.period_predicate.is_none(),
            "the period predicate is cleared"
        );
        assert_eq!(
            described.temporal.as_of, stmt.temporal.as_of,
            "the AS OF qualifier is preserved"
        );
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
            Projection::Items(vec![ProjectionItem::column("balance")])
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
            // `ORDER BY` / `LIMIT` / `DISTINCT` now bind ([STL-263]); the
            // Postgres `DISTINCT ON (…)` extension stays out.
            "SELECT DISTINCT ON (balance) balance FROM account",
            // `GROUP BY balance` now binds as an aggregate query ([STL-171]) and
            // `HAVING` over it binds too ([STL-265]); `GROUP BY ALL` stays out.
            "SELECT balance FROM account GROUP BY ALL",
            // `WITH (CTE)` now binds ([STL-242]); see the CTE tests below.
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

    // ---- result shaping: ORDER BY / LIMIT / OFFSET / DISTINCT (STL-263) ----

    #[test]
    fn order_by_binds_multi_key_with_mixed_directions() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "SELECT id, balance FROM account ORDER BY balance DESC, id ASC",
            &catalog,
        )
        .expect("bind");
        assert_eq!(
            bound.order_by,
            vec![
                BoundSortKey {
                    column: SortTarget::Output(1),
                    descending: true,
                },
                BoundSortKey {
                    column: SortTarget::Output(0),
                    descending: false,
                },
            ]
        );
        assert!(!bound.distinct);
        assert_eq!((bound.limit, bound.offset), (None, 0));
    }

    #[test]
    fn order_by_resolves_against_the_select_list_then_the_schema() {
        let catalog = catalog_with_account(1_000);
        // `balance` is not projected: a plain SELECT may still sort on it, by
        // schema index (Postgres sorts on non-projected columns).
        let bound = bind("SELECT id FROM account ORDER BY balance", &catalog).expect("bind");
        assert_eq!(
            bound.order_by,
            vec![BoundSortKey {
                column: SortTarget::Schema(1),
                descending: false,
            }]
        );
        // Under `SELECT *` the output order is the schema order.
        let star = bind("SELECT * FROM account ORDER BY balance", &catalog).expect("bind");
        assert_eq!(
            star.order_by,
            vec![BoundSortKey {
                column: SortTarget::Output(1),
                descending: false,
            }]
        );
        // A name in neither the select list nor the schema is the usual
        // unknown-column error.
        assert_eq!(
            bind("SELECT id FROM account ORDER BY nonesuch", &catalog),
            Err(SelectError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn distinct_order_by_outside_the_select_list_is_42p10() {
        let catalog = catalog_with_account(1_000);
        // Sorting on a column DISTINCT discarded is ambiguous — Postgres 42P10.
        assert_eq!(
            bind("SELECT DISTINCT id FROM account ORDER BY balance", &catalog),
            Err(SelectError::DistinctOrderBy)
        );
        // In the select list it is fine.
        let ok = bind("SELECT DISTINCT id FROM account ORDER BY id", &catalog).expect("bind");
        assert!(ok.distinct);
        assert_eq!(
            ok.order_by,
            vec![BoundSortKey {
                column: SortTarget::Output(0),
                descending: false,
            }]
        );
    }

    #[test]
    fn unsupported_order_by_shapes_are_rejected_with_the_reason() {
        let catalog = catalog_with_account(1_000);
        for sql in [
            // Only a bare column name sorts — no expressions or ordinals yet.
            "SELECT id FROM account ORDER BY balance + 1",
            "SELECT id FROM account ORDER BY 1",
            // Explicit NULL placement is not bound (the Postgres defaults apply).
            "SELECT id FROM account ORDER BY id NULLS FIRST",
            "SELECT id FROM account ORDER BY id DESC NULLS LAST",
            // (`ORDER BY ALL` parses as a column named "all" under this
            // dialect — Postgres-faithful — so it surfaces as UnknownColumn,
            // not here; the `OrderByKind::All` arm stays as dialect defense.)
        ] {
            assert!(
                matches!(bind(sql, &catalog), Err(SelectError::UnsupportedOrderBy(_))),
                "expected UnsupportedOrderBy for: {sql}"
            );
        }
    }

    #[test]
    fn order_by_in_an_aggregate_query_names_output_columns_only() {
        let catalog = catalog_with_sales();
        let bound = bind(
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region \
             ORDER BY total DESC, region",
            &catalog,
        )
        .expect("bind");
        assert_eq!(
            bound.order_by,
            vec![
                BoundSortKey {
                    column: SortTarget::Output(1),
                    descending: true,
                },
                BoundSortKey {
                    column: SortTarget::Output(0),
                    descending: false,
                },
            ]
        );
        // A schema column outside the aggregate's select list has no single
        // value per output row.
        assert!(matches!(
            bind(
                "SELECT region FROM sales GROUP BY region ORDER BY amount",
                &catalog,
            ),
            Err(SelectError::UnsupportedOrderBy(_))
        ));
    }

    #[test]
    fn limit_offset_and_fetch_bind_non_negative_literals() {
        let catalog = catalog_with_account(1_000);
        let bound = bind("SELECT id FROM account LIMIT 10 OFFSET 3", &catalog).expect("bind");
        assert_eq!((bound.limit, bound.offset), (Some(10), 3));

        // LIMIT 0 is a valid empty read, not an error.
        let zero = bind("SELECT id FROM account LIMIT 0", &catalog).expect("bind");
        assert_eq!(zero.limit, Some(0));

        // LIMIT ALL is explicitly unlimited.
        let all = bind("SELECT id FROM account LIMIT ALL OFFSET 2", &catalog).expect("bind");
        assert_eq!((all.limit, all.offset), (None, 2));

        // The standard FETCH FIRST alias; an omitted count reads as 1.
        let fetch = bind("SELECT id FROM account FETCH FIRST 5 ROWS ONLY", &catalog).expect("bind");
        assert_eq!(fetch.limit, Some(5));
        let one = bind("SELECT id FROM account FETCH FIRST ROW ONLY", &catalog).expect("bind");
        assert_eq!(one.limit, Some(1));
    }

    #[test]
    fn cap_unbounded_select_defaults_a_missing_limit() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        // A bare read gains the cap, so the binder (and executor) see `LIMIT 1000`.
        let mut bare = parse_one("SELECT id FROM account");
        cap_unbounded_select(&mut bare, 1000);
        assert_eq!(bind_select(&bare, &ctx).expect("bind").limit, Some(1000));
        // Idempotent: a second pass sees the limit already present and leaves it.
        cap_unbounded_select(&mut bare, 1000);
        assert_eq!(bind_select(&bare, &ctx).expect("bind").limit, Some(1000));
    }

    #[test]
    fn cap_unbounded_select_respects_an_explicit_finite_bound() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        // A finite `LIMIT`/`FETCH` count is the caller's own choice: left untouched
        // (including `LIMIT 0`, a valid empty read).
        for (sql, want) in [
            ("SELECT id FROM account LIMIT 5", Some(5)),
            ("SELECT id FROM account LIMIT 0", Some(0)),
            ("SELECT id FROM account FETCH FIRST 3 ROWS ONLY", Some(3)),
        ] {
            let mut stmt = parse_one(sql);
            cap_unbounded_select(&mut stmt, 1000);
            assert_eq!(bind_select(&stmt, &ctx).expect("bind").limit, want, "{sql}");
        }
    }

    #[test]
    fn cap_unbounded_select_caps_every_unlimited_form_and_keeps_the_offset() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        // Everything the binder reads as unlimited is capped — a bare read, an
        // `OFFSET`-only read, and `LIMIT ALL` (which the binder treats identically
        // to no clause). Any `OFFSET` the caller gave is preserved.
        for (sql, want) in [
            ("SELECT id FROM account", (Some(1000), 0)),
            ("SELECT id FROM account LIMIT ALL", (Some(1000), 0)),
            ("SELECT id FROM account OFFSET 2", (Some(1000), 2)),
            ("SELECT id FROM account LIMIT ALL OFFSET 7", (Some(1000), 7)),
        ] {
            let mut stmt = parse_one(sql);
            cap_unbounded_select(&mut stmt, 1000);
            let bound = bind_select(&stmt, &ctx).expect("bind");
            assert_eq!((bound.limit, bound.offset), want, "{sql}");
        }
    }

    #[test]
    fn cap_unbounded_select_touches_only_the_top_level_query() {
        // The cap rewrites the outer query's `LIMIT`, never a subquery's: capping
        // `WHERE … IN (SELECT …)` would change the *result*, not just truncate it.
        let mut nested = parse_one("SELECT id FROM account WHERE id IN (SELECT id FROM account)");
        cap_unbounded_select(&mut nested, 1000);
        let rendered = nested.sql().expect("SQL body").to_string();
        assert!(rendered.ends_with("LIMIT 1000"), "{rendered}");
        assert_eq!(
            rendered.matches("LIMIT").count(),
            1,
            "only the outer query is capped: {rendered}"
        );

        // A non-query statement has no `LIMIT` to set — `INSERT … SELECT` included,
        // so a bulk load's source read is never truncated.
        for sql in [
            "INSERT INTO account VALUES (1, 2)",
            "INSERT INTO account SELECT id, balance FROM account",
        ] {
            let mut stmt = parse_one(sql);
            cap_unbounded_select(&mut stmt, 1000);
            assert!(
                !stmt.sql().expect("SQL body").to_string().contains("LIMIT"),
                "{sql} must not gain a LIMIT"
            );
        }
    }

    #[test]
    fn cap_unbounded_select_skips_shapes_that_reject_a_limit() {
        // Only a plain single-table read accepts a `LIMIT`. A `JOIN`, a
        // table-valued introspection call (`stele_history`/`stele_segments`/
        // `stele_audit`, recognized by the engine as an *unshaped* `SELECT *`), and
        // a set operation all reject result shaping, so injecting a `LIMIT` would
        // turn a working query into a bind error — the cap must leave them alone.
        for sql in [
            "SELECT a.id FROM account a JOIN account b ON a.id = b.id",
            "SELECT * FROM stele_history('account', 1)",
            "SELECT * FROM stele_segments('account')",
            "SELECT * FROM stele_audit('account')",
            "SELECT id FROM account UNION SELECT id FROM account",
        ] {
            let mut stmt = parse_one(sql);
            cap_unbounded_select(&mut stmt, 1000);
            assert!(
                !stmt.sql().expect("SQL body").to_string().contains("LIMIT"),
                "{sql} must not gain a LIMIT"
            );
        }
    }

    #[test]
    fn unsupported_limit_shapes_are_rejected_with_the_reason() {
        let catalog = catalog_with_account(1_000);
        for sql in [
            // Negative and non-literal counts.
            "SELECT id FROM account LIMIT -1",
            "SELECT id FROM account OFFSET -2",
            "SELECT id FROM account LIMIT 1 + 1",
            "SELECT id FROM account LIMIT 1.5",
            // FETCH variants that change semantics.
            "SELECT id FROM account FETCH FIRST 5 ROWS WITH TIES",
            "SELECT id FROM account FETCH FIRST 5 PERCENT ROWS ONLY",
            // Two counts for one query.
            "SELECT id FROM account LIMIT 5 FETCH FIRST 5 ROWS ONLY",
        ] {
            assert!(
                matches!(bind(sql, &catalog), Err(SelectError::UnsupportedLimit(_))),
                "expected UnsupportedLimit for: {sql}"
            );
        }
    }

    #[test]
    fn distinct_binds_over_plain_and_aggregate_reads() {
        let catalog = catalog_with_sales();
        assert!(
            bind("SELECT DISTINCT region FROM sales", &catalog)
                .expect("bind")
                .distinct
        );
        // DISTINCT over an aggregate's output rows.
        let agg = bind(
            "SELECT DISTINCT COUNT(*) FROM sales GROUP BY region",
            &catalog,
        )
        .expect("bind");
        assert!(agg.distinct && agg.aggregate.is_some());
        // `SELECT ALL` is the explicit default.
        assert!(
            !bind("SELECT ALL region FROM sales", &catalog)
                .expect("bind")
                .distinct
        );
    }

    #[test]
    fn result_shaping_binds_over_a_join() {
        let catalog = catalog_with_join_tables();
        // ORDER BY a projected output column ([STL-264]).
        let ordered = bind(
            "SELECT name FROM users JOIN orders ON users.id = orders.uid ORDER BY name",
            &catalog,
        )
        .expect("bind ORDER BY over a join");
        assert_eq!(ordered.order_by.len(), 1);
        // LIMIT / OFFSET.
        let limited = bind(
            "SELECT name FROM users JOIN orders ON users.id = orders.uid LIMIT 1 OFFSET 2",
            &catalog,
        )
        .expect("bind LIMIT over a join");
        assert_eq!((limited.limit, limited.offset), (Some(1), 2));
        // DISTINCT.
        assert!(
            bind(
                "SELECT DISTINCT name FROM users JOIN orders ON users.id = orders.uid",
                &catalog,
            )
            .expect("bind DISTINCT over a join")
            .distinct
        );
        // `SELECT ALL` is the explicit default (not DISTINCT) and still binds.
        assert!(
            !bind(
                "SELECT ALL name FROM users JOIN orders ON users.id = orders.uid",
                &catalog,
            )
            .expect("bind")
            .distinct
        );
    }

    #[test]
    fn distinct_order_by_a_projected_join_column_is_legal_qualified_or_bare() {
        // A qualified ORDER BY key that is in the select list is valid under
        // DISTINCT — qualification disambiguates same-named columns after a join,
        // and the key resolves to its output position, not a 42P10 ([STL-264]).
        let catalog = catalog_with_join_tables();
        for sql in [
            "SELECT DISTINCT users.name FROM users JOIN orders ON users.id = orders.uid \
             ORDER BY users.name",
            "SELECT DISTINCT users.name FROM users JOIN orders ON users.id = orders.uid \
             ORDER BY name",
        ] {
            let bound = bind(sql, &catalog).expect("DISTINCT ORDER BY a projected join column");
            assert_eq!(
                bound.order_by,
                vec![BoundSortKey {
                    column: SortTarget::Output(0),
                    descending: false,
                }],
                "{sql}"
            );
        }
        // But ORDER BY a column the DISTINCT projection discarded is the 42P10.
        assert!(matches!(
            bind(
                "SELECT DISTINCT users.name FROM users JOIN orders ON users.id = orders.uid \
                 ORDER BY orders.oid",
                &catalog,
            ),
            Err(SelectError::DistinctOrderBy)
        ));
        // Without DISTINCT, sorting on an unprojected qualified column is allowed.
        let plain = bind(
            "SELECT users.name FROM users JOIN orders ON users.id = orders.uid ORDER BY orders.oid",
            &catalog,
        )
        .expect("plain ORDER BY an unprojected join column");
        assert!(matches!(
            plain.order_by.as_slice(),
            [BoundSortKey {
                column: SortTarget::Schema(_),
                ..
            }]
        ));
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
            Projection::Items(vec![
                ProjectionItem::column("id"),
                ProjectionItem::column("balance"),
            ])
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
    fn provenance_pseudo_columns_bind_in_a_projection() {
        // The three provenance pseudo-columns ([STL-247]) are not schema columns,
        // but they bind in a named projection — alongside real columns and each
        // other — without an `UnknownColumn` error. The engine resolves them
        // against the virtual layout after the table's own columns.
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let stmt = parse_one(
            "SELECT id, _stele_txn_id, _stele_committed_at, _stele_principal FROM account",
        );
        assert_eq!(
            bind_select(&stmt, &ctx).expect("bind").projection,
            Projection::Items(vec![
                ProjectionItem::column("id"),
                ProjectionItem::column("_stele_txn_id"),
                ProjectionItem::column("_stele_committed_at"),
                ProjectionItem::column("_stele_principal"),
            ])
        );
    }

    #[test]
    fn a_provenance_pseudo_column_binds_in_where() {
        // `WHERE _stele_txn_id = <literal>` binds: the pseudo-column anchors the
        // predicate at its virtual index (past the two schema columns), and the
        // literal folds to its `int8` type ([STL-247]).
        let catalog = catalog_with_account(1_000);
        let bound = bind("SELECT id FROM account WHERE _stele_txn_id = 5", &catalog)
            .expect("bind a pseudo-column WHERE");
        let predicate = bound.filter.expect("a filter binds");
        // Two schema columns (id, balance) ⇒ `_stele_txn_id` is the first pseudo
        // index, 2.
        assert_eq!(
            (predicate.left, predicate.right),
            (
                BoundScalar::Column(2),
                BoundScalar::Literal(ScalarValue::Int8(5)),
            )
        );
    }

    #[test]
    fn an_unknown_pseudo_like_column_is_still_rejected() {
        // Only the three documented pseudo-columns are accepted; a different
        // `_stele_*` name is an unknown column, not a silent pass-through.
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let stmt = parse_one("SELECT _stele_statement FROM account");
        assert_eq!(
            bind_select(&stmt, &ctx),
            Err(SelectError::UnknownColumn {
                table: "account".to_owned(),
                column: "_stele_statement".to_owned(),
            })
        );
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
    fn from_to_binds_a_half_open_system_range() {
        let catalog = catalog_with_account(1_000);
        let range = bind(
            "SELECT * FROM account FOR SYSTEM_TIME FROM 10 TO 20",
            &catalog,
        )
        .unwrap()
        .system_range
        .expect("a range was bound");
        assert_eq!(range.from, SystemTimeMicros(10));
        assert_eq!(range.to, SystemTimeMicros(20));
        assert!(!range.closed_upper, "FROM..TO is half-open");
    }

    #[test]
    fn between_binds_a_closed_system_range() {
        let catalog = catalog_with_account(1_000);
        let range = bind(
            "SELECT * FROM account FOR SYSTEM_TIME BETWEEN 10 AND 20",
            &catalog,
        )
        .unwrap()
        .system_range
        .expect("a range was bound");
        assert!(range.closed_upper, "BETWEEN..AND is closed");
        // A single-instant closed range (from == to) is valid; the equivalent
        // half-open range is not.
        assert!(
            bind(
                "SELECT * FROM account FOR SYSTEM_TIME BETWEEN 10 AND 10",
                &catalog
            )
            .is_ok()
        );
    }

    #[test]
    fn empty_or_reversed_ranges_are_rejected() {
        let catalog = catalog_with_account(1_000);
        for sql in [
            "SELECT * FROM account FOR SYSTEM_TIME FROM 20 TO 10", // reversed
            "SELECT * FROM account FOR SYSTEM_TIME FROM 10 TO 10", // zero-length half-open
            "SELECT * FROM account FOR SYSTEM_TIME BETWEEN 20 AND 10", // reversed closed
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::EmptySystemRange { .. })
                ),
                "expected EmptySystemRange for: {sql}"
            );
        }
    }

    #[test]
    fn a_range_with_a_where_binds_the_filter() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "SELECT id FROM account FOR SYSTEM_TIME FROM 1 TO 9 WHERE id = 1",
            &catalog,
        )
        .unwrap();
        assert!(bound.system_range.is_some());
        assert!(
            bound.filter.is_some(),
            "the WHERE binds alongside the range"
        );
    }

    #[test]
    fn a_range_combined_with_unsupported_clauses_is_rejected() {
        let catalog = catalog_with_account(1_000);
        for sql in [
            // A point AS OF and a range are contradictory reads of one table.
            "SELECT * FROM account FOR SYSTEM_TIME AS OF 5 FOR SYSTEM_TIME FROM 1 TO 9",
            // A subquery WHERE over a range is a tracked follow-up ([STL-329]).
            "SELECT * FROM account FOR SYSTEM_TIME FROM 1 TO 9 WHERE id IN (SELECT id FROM account)",
            // A computed / scalar-subquery select item over a range is a tracked
            // follow-up ([STL-303] does not yet compose over a range).
            "SELECT id + 1 FROM account FOR SYSTEM_TIME FROM 1 TO 9",
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::UnsupportedSystemRange(_))
                ),
                "expected UnsupportedSystemRange for: {sql}"
            );
        }
    }

    #[test]
    fn a_period_predicate_where_composes_over_a_range() {
        // [STL-345] lifts the [STL-329] rejection: a `WHERE PERIOD(..) <pred>
        // PERIOD(..)` now binds over a range, both axes, in both the constant
        // ([STL-165]) and per-row value-column ([STL-193]) shapes. The predicate
        // lands in `period_filter` (not the plain `filter`), alongside the range.
        let account = catalog_with_account(1_000);
        let booking = catalog_with_booking(1_000);

        // System range + a constant period predicate (account has no instant-typed
        // value column, so the per-row shape is exercised on the valid axis below).
        let sys = bind(
            "SELECT * FROM account FOR SYSTEM_TIME FROM 1 TO 9 \
             WHERE PERIOD(10, 20) CONTAINS PERIOD(12, 15)",
            &account,
        )
        .expect("a constant period predicate binds over a system range");
        assert!(sys.system_range.is_some());
        assert!(
            sys.period_filter.is_some() && sys.filter.is_none(),
            "the period predicate is the WHERE, not a plain filter"
        );

        // Valid range + a per-row period predicate over the table's own valid
        // columns — the natural `PERIOD(vf, vt)` shape against a constant probe.
        let valid = bind(
            "SELECT * FROM booking FOR VALID_TIME FROM 1 TO 9 \
             WHERE PERIOD(vf, vt) OVERLAPS PERIOD(0, 100)",
            &booking,
        )
        .expect("a per-row period predicate binds over a valid range");
        assert!(valid.valid_range.is_some());
        assert!(valid.period_filter.is_some() && valid.filter.is_none());

        // The constant shape composes over a valid range too.
        assert!(
            bind(
                "SELECT * FROM booking FOR VALID_TIME FROM 1 TO 9 \
                 WHERE PERIOD(10, 20) EQUALS PERIOD(10, 20)",
                &booking,
            )
            .expect("a constant period predicate binds over a valid range")
            .period_filter
            .is_some()
        );

        // …and the per-row shape composes over a *system* range as well: the period
        // endpoints only need an instant-typed value column, not the valid axis, so
        // `PERIOD(vf, vt)` over `booking`'s `TIMESTAMP` columns binds against a
        // `FOR SYSTEM_TIME` range (the appended `sys_from`/`sys_to` endpoints follow
        // the value columns, so the column indices line up the same way).
        let sys_per_row = bind(
            "SELECT * FROM booking FOR SYSTEM_TIME FROM 1 TO 9 \
             WHERE PERIOD(vf, vt) OVERLAPS PERIOD(0, 100)",
            &booking,
        )
        .expect("a per-row period predicate binds over a system range");
        assert!(sys_per_row.system_range.is_some());
        assert!(sys_per_row.period_filter.is_some() && sys_per_row.filter.is_none());
    }

    #[test]
    fn a_range_composes_shaping_aggregates_and_provenance() {
        let catalog = catalog_with_account(1_000);

        // Result-shaping ([STL-263]) binds over a range ([STL-329]), including
        // ordering on the appended `sys_from` / `sys_to` endpoints.
        let shaped = bind(
            "SELECT * FROM account FOR SYSTEM_TIME FROM 1 TO 9 \
             ORDER BY sys_from DESC LIMIT 10 OFFSET 2",
            &catalog,
        )
        .expect("shaping binds over a range");
        assert!(shaped.system_range.is_some());
        assert_eq!(shaped.limit, Some(10));
        assert_eq!(shaped.offset, 2);
        assert_eq!(shaped.order_by.len(), 1, "ORDER BY sys_from bound");

        assert!(
            bind(
                "SELECT DISTINCT id FROM account FOR SYSTEM_TIME FROM 1 TO 9",
                &catalog,
            )
            .expect("DISTINCT binds over a range")
            .distinct
        );

        // An aggregate / GROUP BY ([STL-171]) folds the range output.
        assert!(
            bind(
                "SELECT id, count(*) FROM account FOR SYSTEM_TIME FROM 1 TO 9 GROUP BY id",
                &catalog,
            )
            .expect("an aggregate binds over a range")
            .aggregate
            .is_some()
        );

        // A provenance pseudo-column ([STL-247]) projects from a range, alongside a
        // user column and an endpoint.
        let prov = bind(
            "SELECT id, _stele_txn_id, sys_to FROM account FOR SYSTEM_TIME FROM 1 TO 9",
            &catalog,
        )
        .expect("a provenance pseudo-column projects from a range");
        assert!(prov.system_range.is_some());
        let Projection::Items(items) = &prov.projection else {
            panic!("expected a named projection");
        };
        assert_eq!(items.len(), 3, "id, _stele_txn_id, sys_to");

        // `GROUP BY` on an endpoint is bounded by the same grouping-key type rule as
        // any read — `TIMESTAMPTZ` is not yet a groupable type — so it is the
        // pre-existing `UnsupportedAggregate`, not a range-specific rejection.
        assert!(matches!(
            bind(
                "SELECT sys_from, count(*) FROM account FOR SYSTEM_TIME FROM 1 TO 9 GROUP BY sys_from",
                &catalog,
            ),
            Err(SelectError::UnsupportedAggregate(_))
        ));
    }

    #[test]
    fn from_to_binds_a_half_open_valid_range() {
        let catalog = catalog_with_booking(1_000);
        let bound = bind(
            "SELECT * FROM booking FOR VALID_TIME FROM 10 TO 20",
            &catalog,
        )
        .unwrap();
        let range = bound.valid_range.expect("a valid range was bound");
        assert_eq!(range.from, ValidTimeMicros(10));
        assert_eq!(range.to, ValidTimeMicros(20));
        assert!(!range.closed_upper, "FROM..TO is half-open");
        // The valid range does not also pin the valid point, and reads at `now`.
        assert!(bound.valid_snapshot.is_none());
        assert!(bound.system_range.is_none());
    }

    #[test]
    fn between_binds_a_closed_valid_range() {
        let catalog = catalog_with_booking(1_000);
        let range = bind(
            "SELECT * FROM booking FOR VALID_TIME BETWEEN 10 AND 20",
            &catalog,
        )
        .unwrap()
        .valid_range
        .expect("a valid range was bound");
        assert!(range.closed_upper, "BETWEEN..AND is closed");
    }

    #[test]
    fn a_system_as_of_pins_the_snapshot_a_valid_range_reads_at() {
        // The cross-axis composition `v(k, S_past, V_range)`: a system point pins
        // the snapshot, the valid axis ranges. Allowed (unlike a same-axis mix).
        let catalog = catalog_with_booking(1_000);
        let bound = bind(
            "SELECT * FROM booking FOR SYSTEM_TIME AS OF 1234 FOR VALID_TIME FROM 1 TO 9",
            &catalog,
        )
        .unwrap();
        assert!(bound.valid_range.is_some());
        assert_eq!(
            bound.snapshot,
            SystemTimeMicros(1234),
            "system AS OF pins it"
        );
    }

    #[test]
    fn empty_or_reversed_valid_ranges_are_rejected() {
        let catalog = catalog_with_booking(1_000);
        for sql in [
            "SELECT * FROM booking FOR VALID_TIME FROM 20 TO 10",
            "SELECT * FROM booking FOR VALID_TIME FROM 10 TO 10",
            "SELECT * FROM booking FOR VALID_TIME BETWEEN 20 AND 10",
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::EmptyValidRange { .. })
                ),
                "expected EmptyValidRange for: {sql}"
            );
        }
    }

    #[test]
    fn a_valid_range_over_a_system_only_table_is_rejected() {
        // No valid axis to range over — `account` is system-only.
        let catalog = catalog_with_account(1_000);
        assert!(matches!(
            bind("SELECT * FROM account FOR VALID_TIME FROM 1 TO 9", &catalog),
            Err(SelectError::ValidTimeUnsupported { .. })
        ));
    }

    #[test]
    fn a_valid_range_combined_with_unsupported_clauses_is_rejected() {
        let catalog = catalog_with_booking(1_000);
        for sql in [
            // A valid point AS OF and a valid range are the same axis — contradictory.
            "SELECT * FROM booking FOR VALID_TIME AS OF 5 FOR VALID_TIME FROM 1 TO 9",
            // A subquery WHERE over a range is a tracked follow-up ([STL-329]).
            "SELECT * FROM booking FOR VALID_TIME FROM 1 TO 9 WHERE id IN (SELECT id FROM booking)",
            // A computed select item over a range is a tracked follow-up.
            "SELECT id + 1 FROM booking FOR VALID_TIME FROM 1 TO 9",
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::UnsupportedValidRange(_))
                ),
                "expected UnsupportedValidRange for: {sql}"
            );
        }
    }

    #[test]
    fn a_valid_range_composes_shaping_aggregates_and_provenance() {
        let catalog = catalog_with_booking(1_000);

        // Result-shaping binds over a valid range ([STL-329]), ordering on the
        // appended `valid_from` / `valid_to` endpoints included.
        let shaped = bind(
            "SELECT * FROM booking FOR VALID_TIME FROM 1 TO 9 \
             ORDER BY valid_to LIMIT 5",
            &catalog,
        )
        .expect("shaping binds over a valid range");
        assert!(shaped.valid_range.is_some());
        assert_eq!(shaped.limit, Some(5));
        assert_eq!(shaped.order_by.len(), 1, "ORDER BY valid_to bound");

        // An aggregate and a projected provenance pseudo-column both bind.
        assert!(
            bind(
                "SELECT count(*) FROM booking FOR VALID_TIME FROM 1 TO 9",
                &catalog,
            )
            .expect("an aggregate binds over a valid range")
            .aggregate
            .is_some()
        );
        assert!(
            bind(
                "SELECT id, _stele_txn_id FROM booking FOR VALID_TIME FROM 1 TO 9",
                &catalog,
            )
            .expect("a provenance pseudo-column projects from a valid range")
            .valid_range
            .is_some()
        );
    }

    #[test]
    fn where_on_the_key_binds_to_column_zero() {
        // `id` is the business key — column index 0 — so the executor can push it
        // down to zone-map pruning (a [`BoundPredicate::key_equality`]).
        let catalog = catalog_with_account(1_000);
        let bound = bind("SELECT balance FROM account WHERE id = 7", &catalog)
            .unwrap()
            .filter
            .unwrap();
        assert_eq!(
            bound,
            BoundPredicate {
                left: BoundScalar::Column(0),
                op: CompareOp::Eq,
                right: BoundScalar::Literal(ScalarValue::Int4(7)),
            }
        );
        assert_eq!(bound.key_equality(), Some(&ScalarValue::Int4(7)));
    }

    #[test]
    fn where_on_a_value_column_binds_to_its_index() {
        // `balance` is a value column — index 1 — folded against its int4 type.
        // The column may sit on either side of the `=`; the bound predicate mirrors
        // the operand order, and neither is a business-key equality.
        let catalog = catalog_with_account(1_000);
        let lit = || BoundScalar::Literal(ScalarValue::Int4(100));
        let col = || BoundScalar::Column(1);
        let column_first = bind("SELECT id FROM account WHERE balance = 100", &catalog)
            .unwrap()
            .filter
            .unwrap();
        assert_eq!(
            column_first,
            BoundPredicate {
                left: col(),
                op: CompareOp::Eq,
                right: lit(),
            }
        );
        assert_eq!(
            bind("SELECT id FROM account WHERE 100 = balance", &catalog)
                .unwrap()
                .filter,
            Some(BoundPredicate {
                left: lit(),
                op: CompareOp::Eq,
                right: col(),
            })
        );
        // A value-column equality is not pushed down to the key zone map.
        assert_eq!(column_first.key_equality(), None);
    }

    #[test]
    fn column_comparison_normalizes_the_column_to_the_left() {
        // The probe-facing accessor ([STL-237]): a bare `<col> <cmp> <lit>` in
        // either operand order reads back column-left, with a literal-first
        // form mirroring the operator (`100 < balance` is `balance > 100`).
        let catalog = catalog_with_account(1_000);
        let comparison = |sql: &str| {
            bind(sql, &catalog)
                .unwrap()
                .filter
                .unwrap()
                .column_comparison()
                .map(|(col, op, value)| (col, op, value.clone()))
        };
        assert_eq!(
            comparison("SELECT id FROM account WHERE balance < 100"),
            Some((1, CompareOp::Lt, ScalarValue::Int4(100)))
        );
        assert_eq!(
            comparison("SELECT id FROM account WHERE 100 < balance"),
            Some((1, CompareOp::Gt, ScalarValue::Int4(100)))
        );
        assert_eq!(
            comparison("SELECT id FROM account WHERE 100 >= balance"),
            Some((1, CompareOp::Le, ScalarValue::Int4(100)))
        );
        // The equality accessor is the comparison's `Eq` arm.
        let eq = bind("SELECT id FROM account WHERE 100 = balance", &catalog)
            .unwrap()
            .filter
            .unwrap();
        assert_eq!(eq.column_equality(), Some((1, &ScalarValue::Int4(100))));
        // A business-key comparison is not a value-column probe shape…
        assert_eq!(comparison("SELECT id FROM account WHERE id > 7"), None);
        // …and neither is an arithmetic side: the filter still applies it
        // exactly, but no single column anchors a candidate window.
        assert_eq!(
            comparison("SELECT id FROM account WHERE balance % 2 = 0"),
            None
        );
    }

    #[test]
    fn each_comparison_operator_binds() {
        // All six comparisons over a value column reach the evaluator (STL-213) —
        // the non-equalities were rejected before.
        let catalog = catalog_with_account(1_000);
        let cases = [
            ("=", CompareOp::Eq),
            ("<>", CompareOp::Ne),
            ("!=", CompareOp::Ne),
            ("<", CompareOp::Lt),
            ("<=", CompareOp::Le),
            (">", CompareOp::Gt),
            (">=", CompareOp::Ge),
        ];
        for (sym, op) in cases {
            let sql = format!("SELECT id FROM account WHERE balance {sym} 100");
            assert_eq!(
                bind(&sql, &catalog).unwrap().filter,
                Some(BoundPredicate {
                    left: BoundScalar::Column(1),
                    op,
                    right: BoundScalar::Literal(ScalarValue::Int4(100)),
                }),
                "binding `{sym}`"
            );
        }
    }

    #[test]
    fn integer_arithmetic_in_a_where_binds_div_and_mod() {
        // `/` and `%` (and the other integer ops) bind to a nested arithmetic
        // scalar over the column (STL-213).
        let catalog = catalog_with_account(1_000);
        assert_eq!(
            bind("SELECT id FROM account WHERE balance % 2 = 0", &catalog)
                .unwrap()
                .filter,
            Some(BoundPredicate {
                left: BoundScalar::Arith {
                    op: ArithOp::Mod,
                    left: Box::new(BoundScalar::Column(1)),
                    right: Box::new(BoundScalar::Literal(ScalarValue::Int4(2))),
                },
                op: CompareOp::Eq,
                right: BoundScalar::Literal(ScalarValue::Int4(0)),
            })
        );
        // Arithmetic may sit on the literal side too, and `/` binds the same way.
        assert_eq!(
            bind("SELECT id FROM account WHERE 5 = balance / 10", &catalog)
                .unwrap()
                .filter,
            Some(BoundPredicate {
                left: BoundScalar::Literal(ScalarValue::Int4(5)),
                op: CompareOp::Eq,
                right: BoundScalar::Arith {
                    op: ArithOp::Div,
                    left: Box::new(BoundScalar::Column(1)),
                    right: Box::new(BoundScalar::Literal(ScalarValue::Int4(10))),
                },
            })
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
                left: BoundScalar::Column(0),
                op: CompareOp::Eq,
                right: BoundScalar::Literal(ScalarValue::Int4(7)),
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
            "SELECT id FROM account WHERE id = balance", // column = column
            "SELECT id FROM account WHERE balance = 'x'", // type mismatch
            "SELECT id FROM account WHERE balance = NULL", // NULL comparand
            "SELECT id FROM account WHERE id = 1 AND balance = 2", // conjunction
            "SELECT id FROM account WHERE id % 2 = balance", // arithmetic vs a 2nd column
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

    #[test]
    fn arithmetic_over_a_non_integer_column_is_rejected() {
        // The evaluator computes arithmetic over int4/int8 only; `vf` is TIMESTAMP,
        // so `vf / 2` is a bind error rather than a per-row evaluator failure.
        let catalog = catalog_with_booking(1_000);
        assert!(matches!(
            bind("SELECT id FROM booking WHERE vf / 2 = 0", &catalog),
            Err(SelectError::UnsupportedPredicate(_))
        ));
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
                left: BoundScalar::Column(0),
                op: CompareOp::Eq,
                right: BoundScalar::Literal(ScalarValue::Int4(1)),
            })
        );
    }

    // ---- HAVING (STL-265) ----

    #[test]
    fn having_on_a_projected_aggregate_reuses_its_index() {
        // `COUNT(*)` is projected *and* in HAVING: it is computed once (one entry
        // in `aggregates`) and HAVING references that index.
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.aggregates,
            vec![AggregateCall {
                func: AggregateFunc::Count,
                arg: None,
            }],
            "the projected COUNT(*) is reused, not duplicated"
        );
        assert_eq!(
            agg.items,
            vec![OutputItem::Group(0), OutputItem::Aggregate(0)]
        );
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Aggregate(0),
                op: CompareOp::Gt,
                right: HavingScalar::Literal(ScalarValue::Int8(1)),
            })
        );
    }

    #[test]
    fn having_aggregate_absent_from_the_select_list_is_computed_not_emitted() {
        // `SUM(amount)` is only in HAVING: appended to `aggregates` (so it is
        // computed) but not to `items`/`columns` (so it is never emitted).
        let catalog = catalog_with_sales();
        let bound = bind(
            "SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 100",
            &catalog,
        )
        .unwrap();
        let agg = bound.aggregate.expect("aggregate plan");
        assert_eq!(
            agg.aggregates,
            vec![AggregateCall {
                func: AggregateFunc::Sum,
                arg: Some(2),
            }]
        );
        assert_eq!(
            agg.items,
            vec![OutputItem::Group(0)],
            "SUM is not projected"
        );
        assert_eq!(agg.columns, vec![("region".to_owned(), LogicalType::Text)]);
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Aggregate(0),
                op: CompareOp::Gt,
                right: HavingScalar::Literal(ScalarValue::Int8(100)),
            })
        );
    }

    #[test]
    fn having_on_a_grouping_column_binds() {
        // A grouping column anchors the predicate; it is addressed by its position
        // in `group_by`, the literal folding to the column's type.
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT id, COUNT(*) FROM sales GROUP BY id HAVING id >= 2",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Group(0),
                op: CompareOp::Ge,
                right: HavingScalar::Literal(ScalarValue::Int4(2)),
            })
        );
    }

    #[test]
    fn having_arithmetic_over_an_aggregate_binds() {
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT region FROM sales GROUP BY region HAVING COUNT(*) * 2 > 10",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Arith {
                    op: ArithOp::Mul,
                    left: Box::new(HavingScalar::Aggregate(0)),
                    right: Box::new(HavingScalar::Literal(ScalarValue::Int8(2))),
                },
                op: CompareOp::Gt,
                right: HavingScalar::Literal(ScalarValue::Int8(10)),
            })
        );
    }

    #[test]
    fn having_without_a_group_by_is_one_whole_table_group() {
        // No GROUP BY, but a HAVING (and a projected aggregate) makes this an
        // aggregate query — one whole-table group the HAVING filters.
        let catalog = catalog_with_sales();
        let agg = bind("SELECT COUNT(*) FROM sales HAVING COUNT(*) > 0", &catalog)
            .unwrap()
            .aggregate
            .expect("aggregate plan");
        assert!(agg.group_by.is_empty());
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Aggregate(0),
                op: CompareOp::Gt,
                right: HavingScalar::Literal(ScalarValue::Int8(0)),
            })
        );
    }

    #[test]
    fn an_ungrouped_column_in_having_is_a_grouping_error() {
        // `amount` is neither grouped nor wrapped in an aggregate — Postgres's
        // 42803 grouping error, not a silent pass.
        let catalog = catalog_with_sales();
        assert_eq!(
            bind(
                "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING amount > 5",
                &catalog,
            ),
            Err(SelectError::UngroupedColumn {
                table: "sales".to_owned(),
                column: "amount".to_owned(),
            })
        );
    }

    #[test]
    fn an_unknown_column_in_having_is_rejected() {
        let catalog = catalog_with_sales();
        assert_eq!(
            bind(
                "SELECT region FROM sales GROUP BY region HAVING nope > 5",
                &catalog,
            ),
            Err(SelectError::UnknownColumn {
                table: "sales".to_owned(),
                column: "nope".to_owned(),
            })
        );
    }

    #[test]
    fn unsupported_having_shapes_are_rejected_with_a_reason() {
        let catalog = catalog_with_sales();
        for sql in [
            // Not a comparison at all.
            "SELECT region FROM sales GROUP BY region HAVING COUNT(*)",
            // A boolean connective is not the single-comparison shape.
            "SELECT region FROM sales GROUP BY region HAVING COUNT(*) > 1 AND COUNT(*) < 9",
            // Two anchors of incomparable types: a TEXT grouping column against an
            // INT8 aggregate is neither both-numeric nor the same type ([STL-327]).
            "SELECT region FROM sales GROUP BY region HAVING region > COUNT(*)",
            // FLOAT8 arithmetic stays out of scope — the evaluator has no float
            // arithmetic kernel, so the AVG anchor is not an integer ([STL-327]).
            "SELECT region FROM sales GROUP BY region HAVING AVG(amount) + 1 > 5",
        ] {
            assert!(
                matches!(bind(sql, &catalog), Err(SelectError::UnsupportedHaving(_))),
                "expected UnsupportedHaving for: {sql}"
            );
        }
    }

    #[test]
    fn having_two_aggregate_comparison_binds() {
        // Aggregate-to-aggregate (two anchors), both INT8 — lifted in [STL-327]. The
        // HAVING's SUM is appended after the projected COUNT, so both are computed.
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT region FROM sales GROUP BY region HAVING COUNT(*) > SUM(amount)",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.aggregates,
            vec![
                AggregateCall {
                    func: AggregateFunc::Count,
                    arg: None,
                },
                AggregateCall {
                    func: AggregateFunc::Sum,
                    arg: Some(2),
                },
            ]
        );
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Aggregate(0),
                op: CompareOp::Gt,
                right: HavingScalar::Aggregate(1),
            })
        );
    }

    #[test]
    fn having_column_to_aggregate_comparison_binds() {
        // A grouping column (INT4) compared to an aggregate (INT8) — two anchors of
        // different numeric widths; the evaluator promotes at run time ([STL-327]).
        let catalog = catalog_with_sales();
        let agg = bind(
            "SELECT id FROM sales GROUP BY id HAVING id > COUNT(*)",
            &catalog,
        )
        .unwrap()
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Group(0),
                op: CompareOp::Gt,
                right: HavingScalar::Aggregate(0),
            })
        );
    }

    #[test]
    fn having_float8_avg_operand_binds() {
        // A FLOAT8 `AVG` operand against a literal — the literal folds to FLOAT8,
        // integer (`5`) or decimal (`2.5`) ([STL-327]).
        let catalog = catalog_with_sales();
        for (sql, want) in [
            (
                "SELECT region FROM sales GROUP BY region HAVING AVG(amount) > 5",
                ScalarValue::float8(5.0),
            ),
            (
                "SELECT region FROM sales GROUP BY region HAVING AVG(amount) > 2.5",
                ScalarValue::float8(2.5),
            ),
        ] {
            let agg = bind(sql, &catalog)
                .unwrap()
                .aggregate
                .expect("aggregate plan");
            assert_eq!(
                agg.aggregates,
                vec![AggregateCall {
                    func: AggregateFunc::Avg,
                    arg: Some(2),
                }]
            );
            assert_eq!(
                agg.having,
                Some(BoundHaving {
                    left: HavingScalar::Aggregate(0),
                    op: CompareOp::Gt,
                    right: HavingScalar::Literal(want),
                }),
                "for: {sql}"
            );
        }
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
        // A third relation for N-way chains ([STL-323]): `items.oid` joins the
        // accumulated `orders.oid`, so a chain's second `ON` references a non-seed
        // input.
        catalog
            .create_table(
                "items",
                vec![
                    ColumnDef::new("item_id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("oid", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create items");
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
        // A two-table join is the one-step chain.
        assert_eq!(join.steps.len(), 1);
        assert_eq!(join.steps[0].join_type, JoinType::Inner);
        assert_eq!(join.left.table, "users");
        assert_eq!(join.steps[0].right.table, "orders");
        // users.id is index 0; orders.uid is index 1.
        assert_eq!((join.steps[0].left_key, join.steps[0].right_key), (0, 1));
        // `SELECT *` over an inner join = every addressable index in order (the
        // left's columns then the right's).
        assert_eq!(join.output, vec![0, 1, 2, 3]);
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
            assert_eq!(join_of(&sql, &catalog).steps[0].join_type, want, "{kw}");
        }
    }

    #[test]
    fn semi_and_anti_project_only_the_left_side() {
        let catalog = catalog_with_join_tables();
        for kw in ["SEMI JOIN", "ANTI JOIN"] {
            let sql = format!("SELECT * FROM users {kw} orders ON users.id = orders.uid");
            let join = join_of(&sql, &catalog);
            assert_eq!(join.output, vec![0, 1], "{kw}");
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
            assert_eq!(
                (join.steps[0].left_key, join.steps[0].right_key),
                (0, 1),
                "{on}"
            );
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
        assert_eq!(aliased.output, vec![1, 2]);
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
        assert_eq!((bare.steps[0].left_key, bare.steps[0].right_key), (0, 1));
        assert_eq!(bare.output, vec![1, 2]);
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
    fn an_n_way_join_binds_a_left_deep_chain() {
        let catalog = catalog_with_join_tables();
        // The chain's second `ON` references a *non-seed* accumulated input
        // (`orders.oid`) and the new input (`items.oid`) ([STL-323]).
        let join = join_of(
            "SELECT users.id FROM users \
             JOIN orders ON users.id = orders.uid \
             JOIN items ON orders.oid = items.oid",
            &catalog,
        );
        assert_eq!(join.left.table, "users");
        assert_eq!(join.steps.len(), 2);
        // Step 0: users(0,1) ⋈ orders(2,3) on users.id (flat 0) = orders.uid (1).
        assert_eq!(join.steps[0].join_type, JoinType::Inner);
        assert_eq!(join.steps[0].right.table, "orders");
        assert_eq!((join.steps[0].left_key, join.steps[0].right_key), (0, 1));
        // Step 1: (…) ⋈ items(4,5) on orders.oid (flat 2) = items.oid (1) — the
        // left key addresses the *accumulated* output, not the seed alone.
        assert_eq!(join.steps[1].join_type, JoinType::Inner);
        assert_eq!(join.steps[1].right.table, "items");
        assert_eq!((join.steps[1].left_key, join.steps[1].right_key), (2, 1));
        // `SELECT users.id` projects flat index 0.
        assert_eq!(join.output, vec![0]);
    }

    #[test]
    fn a_chain_resolves_qualified_names_across_every_input() {
        let catalog = catalog_with_join_tables();
        // A projection / WHERE may name any input in the chain by table or alias.
        let join = join_of(
            "SELECT users.name, items.item_id FROM users \
             JOIN orders o ON users.id = o.uid \
             JOIN items i ON o.oid = i.oid \
             WHERE i.item_id > 0",
            &catalog,
        );
        // users.name is flat 1; items.item_id is flat 4 (after users 0-1, orders 2-3).
        assert_eq!(join.output, vec![1, 4]);
        assert_eq!(
            join.columns,
            vec![
                ("name".to_owned(), LogicalType::Text),
                ("item_id".to_owned(), LogicalType::Int4),
            ]
        );
        // The aliased second step binds against the accumulated `o.oid`.
        assert_eq!((join.steps[1].left_key, join.steps[1].right_key), (2, 1));
    }

    #[test]
    fn a_semi_step_drops_its_input_from_the_chain_scope() {
        let catalog = catalog_with_join_tables();
        // A SEMI step keeps only the accumulated left, so `items` is not addressable
        // after it — a projection of an `items` column is rejected, pointedly.
        let err = bind(
            "SELECT items.item_id FROM users \
             JOIN orders ON users.id = orders.uid \
             SEMI JOIN items ON orders.oid = items.oid",
            &catalog,
        )
        .expect_err("a SEMI step's input is not projectable");
        assert!(
            matches!(err, SelectError::UnsupportedJoinProjection(_)),
            "{err:?}"
        );
    }

    #[test]
    fn where_and_aggregate_bind_over_a_join() {
        let catalog = catalog_with_join_tables();
        // A WHERE over the join's output ([STL-264]) — a qualified column resolves.
        let filtered = bind(
            "SELECT users.id FROM users JOIN orders ON users.id = orders.uid WHERE orders.uid = 1",
            &catalog,
        )
        .expect("bind WHERE over a join");
        assert!(filtered.filter.is_some());
        // An ungrouped aggregate over the whole join.
        let agg = bind(
            "SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.uid",
            &catalog,
        )
        .expect("bind aggregate over a join");
        assert!(agg.aggregate.is_some());
        // GROUP BY a qualified column, with a passed-through grouping column.
        let grouped = bind(
            "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.name",
            &catalog,
        )
        .expect("bind GROUP BY over a join");
        assert!(grouped.aggregate.is_some());
    }

    #[test]
    fn having_over_a_join_binds_through_the_scope() {
        // HAVING over a join now binds, resolving its operands through the JoinScope
        // exactly as the GROUP BY / aggregates do ([STL-327]). `orders.uid` is the
        // join's addressable index 3 (users.id=0, users.name=1, orders.oid=2).
        let catalog = catalog_with_join_tables();

        // A plain aggregate HAVING on the projected COUNT(*).
        let agg = bind(
            "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.name HAVING COUNT(*) > 1",
            &catalog,
        )
        .expect("bind HAVING over a join")
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Aggregate(0),
                op: CompareOp::Gt,
                right: HavingScalar::Literal(ScalarValue::Int8(1)),
            })
        );

        // Two-anchor: a qualified grouping column (INT4) against an aggregate (INT8)
        // the SELECT list omits — the SUM is appended after the projected COUNT.
        let agg = bind(
            "SELECT users.id, COUNT(*) FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.id HAVING users.id < SUM(orders.uid)",
            &catalog,
        )
        .expect("bind two-anchor HAVING over a join")
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.aggregates,
            vec![
                AggregateCall {
                    func: AggregateFunc::Count,
                    arg: None,
                },
                AggregateCall {
                    func: AggregateFunc::Sum,
                    arg: Some(3),
                },
            ],
            "the HAVING's SUM(orders.uid) is appended after the projected COUNT"
        );
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Group(0),
                op: CompareOp::Lt,
                right: HavingScalar::Aggregate(1),
            })
        );

        // A FLOAT8 `AVG` operand over a join, against a decimal literal.
        let agg = bind(
            "SELECT users.name FROM users JOIN orders ON users.id = orders.uid \
             GROUP BY users.name HAVING AVG(orders.uid) > 2.5",
            &catalog,
        )
        .expect("bind float8 HAVING over a join")
        .aggregate
        .expect("aggregate plan");
        assert_eq!(
            agg.having,
            Some(BoundHaving {
                left: HavingScalar::Aggregate(0),
                op: CompareOp::Gt,
                right: HavingScalar::Literal(ScalarValue::float8(2.5)),
            })
        );

        // An ungrouped column in a join HAVING is the Postgres grouping error.
        assert!(matches!(
            bind(
                "SELECT users.name FROM users JOIN orders ON users.id = orders.uid \
                 GROUP BY users.name HAVING orders.oid > 1",
                &catalog,
            ),
            Err(SelectError::UngroupedColumn { .. })
        ));
    }

    /// Two valid-time tables sharing a key column, plus a system-only one — the
    /// mixed shapes a bitemporal join's `AS OF` binding must distinguish ([STL-243]).
    fn catalog_with_bitemporal_join_tables() -> Catalog {
        let mut catalog = Catalog::new();
        for name in ["la", "lb"] {
            catalog
                .create_table(
                    name,
                    vec![
                        ColumnDef::new("k", LogicalType::Int4).expect("col"),
                        ColumnDef::new("v", LogicalType::Int4).expect("col"),
                        ColumnDef::new("vf", LogicalType::Timestamp).expect("col"),
                        ColumnDef::new("vt", LogicalType::Timestamp).expect("col"),
                    ],
                    TableTemporal::with_valid_time(ValidTimeSpec::new("vf", "vt").expect("spec")),
                    SystemTimeMicros(1_000),
                )
                .expect("create valid-time table");
        }
        catalog
            .create_table(
                "sys",
                vec![
                    ColumnDef::new("k", LogicalType::Int4).expect("col"),
                    ColumnDef::new("v", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create system-only table");
        catalog
    }

    #[test]
    fn both_axes_as_of_over_a_join_pins_every_input() {
        // The headline ([STL-243]): a `FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS OF v`
        // over a join carries one statement-level `(sys, valid)` pin every input
        // reads at (docs/16 §8) — no longer rejected.
        let catalog = catalog_with_bitemporal_join_tables();
        let bound = bind(
            "SELECT la.k, lb.v FROM la JOIN lb ON la.k = lb.k \
             FOR SYSTEM_TIME AS OF 5000 FOR VALID_TIME AS OF 25",
            &catalog,
        )
        .expect("bind both-axes AS OF join");
        assert!(bound.join.is_some(), "the join plan is set");
        assert_eq!(bound.snapshot, SystemTimeMicros(5_000));
        assert_eq!(bound.valid_snapshot, Some(SystemTimeMicros(25)));
    }

    #[test]
    fn a_system_time_as_of_over_a_join_pins_only_the_system_axis() {
        // A system-axis pin needs no valid axis, so it binds over plain
        // system-only join tables; the valid axis stays unset.
        let catalog = catalog_with_join_tables();
        let bound = bind(
            "SELECT users.id FROM users JOIN orders ON users.id = orders.uid \
             FOR SYSTEM_TIME AS OF 5000",
            &catalog,
        )
        .expect("bind system-axis AS OF join");
        assert_eq!(bound.snapshot, SystemTimeMicros(5_000));
        assert_eq!(bound.valid_snapshot, None);
    }

    #[test]
    fn a_valid_time_pin_over_a_system_only_join_side_is_rejected() {
        // `la` has a valid axis but `sys` does not; a `FOR VALID_TIME AS OF` pin
        // must travel *every* input, so the system-only side is rejected rather
        // than read at the wrong valid slice — mirroring the single-table rule.
        let catalog = catalog_with_bitemporal_join_tables();
        let err = bind(
            "SELECT la.k FROM la JOIN sys ON la.k = sys.k FOR VALID_TIME AS OF 25",
            &catalog,
        )
        .unwrap_err();
        assert!(
            matches!(&err, SelectError::ValidTimeUnsupported { table } if table == "sys"),
            "got {err:?}"
        );
    }

    #[test]
    fn two_system_qualifiers_across_join_inputs_are_rejected() {
        // The per-table SQL:2011 form is sugar for one statement-level pin
        // ([STL-243] — the v0.3 floor). The parser lifts both qualifiers regardless
        // of placement, and the binder rejects a repeated axis by *count*, not
        // value: two `SYSTEM_TIME` qualifiers are `MultipleAsOf` whether they name
        // different instants or the same one.
        let catalog = catalog_with_join_tables();
        for sql in [
            "SELECT users.id FROM users FOR SYSTEM_TIME AS OF 5000 \
             JOIN orders FOR SYSTEM_TIME AS OF 6000 ON users.id = orders.uid",
            // Same instant on both inputs — still two qualifiers, still rejected.
            "SELECT users.id FROM users FOR SYSTEM_TIME AS OF 5000 \
             JOIN orders FOR SYSTEM_TIME AS OF 5000 ON users.id = orders.uid",
        ] {
            assert!(
                matches!(
                    bind(sql, &catalog),
                    Err(SelectError::MultipleAsOf(TimeDimension::System))
                ),
                "expected MultipleAsOf for: {sql}"
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
        assert_eq!(join.output, vec![0]);
    }

    // ---- STL-234: uncorrelated subquery binding ----

    /// A catalog with an outer `t (id INT, a INT)`, an inner `s (id INT, a INT)`,
    /// and a type-mismatched `s2 (id INT, b TEXT)` — the subquery-binding fixtures.
    fn catalog_with_subquery_tables() -> Catalog {
        let mut catalog = Catalog::new();
        for (name, value_col, ty) in [
            ("t", "a", LogicalType::Int4),
            ("s", "a", LogicalType::Int4),
            ("s2", "b", LogicalType::Text),
        ] {
            catalog
                .create_table(
                    name,
                    vec![
                        ColumnDef::new("id", LogicalType::Int4).expect("col"),
                        ColumnDef::new(value_col, ty).expect("col"),
                    ],
                    TableTemporal::system_only(),
                    SystemTimeMicros(1_000),
                )
                .expect("create");
        }
        catalog
    }

    #[test]
    fn binds_each_uncorrelated_subquery_shape() {
        let catalog = catalog_with_subquery_tables();
        let kind = |sql: &str| {
            bind(sql, &catalog)
                .expect("bind subquery")
                .subquery_filter
                .expect("a subquery filter")
                .kind
        };
        assert_eq!(
            kind("SELECT id FROM t WHERE a = (SELECT a FROM s WHERE id = 1)"),
            SubqueryKind::Scalar {
                column: 1,
                op: CompareOp::Eq,
                subquery_left: false,
            }
        );
        // A subquery on the left records the operand order.
        assert_eq!(
            kind("SELECT id FROM t WHERE (SELECT a FROM s WHERE id = 1) < a"),
            SubqueryKind::Scalar {
                column: 1,
                op: CompareOp::Lt,
                subquery_left: true,
            }
        );
        assert_eq!(
            kind("SELECT id FROM t WHERE a IN (SELECT a FROM s)"),
            SubqueryKind::In {
                column: 1,
                negated: false,
            }
        );
        assert_eq!(
            kind("SELECT id FROM t WHERE a NOT IN (SELECT a FROM s)"),
            SubqueryKind::In {
                column: 1,
                negated: true,
            }
        );
        assert_eq!(
            kind("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s)"),
            SubqueryKind::Exists { negated: false }
        );
        assert_eq!(
            kind("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s)"),
            SubqueryKind::Exists { negated: true }
        );
    }

    // ---- STL-317: semi / anti decorrelation recognition ----

    /// The [`SemiAntiDecorrelation`] a subquery `WHERE` lowers to, if any.
    fn decorrelation(sql: &str, catalog: &Catalog) -> Option<SemiAntiDecorrelation> {
        bind(sql, catalog)
            .expect("bind subquery")
            .subquery_filter
            .expect("a subquery filter")
            .semi_anti_decorrelation()
    }

    #[test]
    fn correlated_exists_on_an_equality_key_decorrelates_to_a_semi_anti_join() {
        let catalog = catalog_with_subquery_tables();
        // `EXISTS` on the equality correlation `s.id = t.id` → a SEMI join on the
        // key column (index 0 on both sides).
        assert_eq!(
            decorrelation(
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
                &catalog,
            ),
            Some(SemiAntiDecorrelation {
                join_type: JoinType::Semi,
                outer_column: 0,
                inner_column: 0,
            })
        );
        // `NOT EXISTS` is the ANTI join; the value column (index 1) correlates too.
        assert_eq!(
            decorrelation(
                "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.a = t.a)",
                &catalog,
            ),
            Some(SemiAntiDecorrelation {
                join_type: JoinType::Anti,
                outer_column: 1,
                inner_column: 1,
            })
        );
    }

    #[test]
    fn non_equality_or_non_exists_correlations_keep_the_per_row_fallback() {
        let catalog = catalog_with_subquery_tables();
        // A `>` correlation is a range, not key-set membership.
        assert_eq!(
            decorrelation(
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.a > t.a)",
                &catalog,
            ),
            None
        );
        // Correlated `IN` carries a second equality (the membership column), so it
        // is not the *single-key* shape — it folds onto the composite-key join
        // instead ([`composite_semi_decorrelation`], STL-337), tested below.
        assert_eq!(
            decorrelation(
                "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.id = t.id)",
                &catalog,
            ),
            None
        );
        // A correlated scalar lookup has no set form.
        assert_eq!(
            decorrelation(
                "SELECT id FROM t WHERE a = (SELECT a FROM s WHERE s.id = t.id)",
                &catalog,
            ),
            None
        );
        // An *uncorrelated* EXISTS folds to a constant, not a join.
        assert_eq!(
            decorrelation("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s)", &catalog),
            None
        );
    }

    #[test]
    fn an_aggregate_inner_exists_keeps_the_per_row_fallback() {
        let catalog = catalog_with_subquery_tables();
        // An aggregate inner always returns exactly one row, so its row-presence is
        // not "∃ a row with the correlation key" — a SEMI join would be wrong. The
        // inner is no longer a plain scan, so it stays on the per-row path.
        assert_eq!(
            decorrelation(
                "SELECT id FROM t WHERE EXISTS (SELECT count(*) FROM s WHERE s.id = t.id)",
                &catalog,
            ),
            None
        );
    }

    // ---- STL-337: composite-key IN decorrelation recognition ----

    /// The bound subquery filter for `sql`, for inspecting the inner shape the
    /// composite-key decorrelation depends on.
    fn subquery_filter(sql: &str, catalog: &Catalog) -> BoundSubqueryFilter {
        bind(sql, catalog)
            .expect("bind subquery")
            .subquery_filter
            .expect("a subquery filter")
    }

    #[test]
    fn correlated_in_on_an_equality_key_decorrelates_to_a_composite_semi_join() {
        let catalog = catalog_with_subquery_tables();
        let filter = subquery_filter(
            "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.id = t.id)",
            &catalog,
        );
        // `t.a IN (SELECT s.a FROM s WHERE s.id = t.id)` → a composite SEMI join on
        // `(id, a)`: the correlation key is the outer/inner `id` (column 0), the
        // membership column the outer `a` (column 1).
        assert_eq!(
            filter.composite_semi_decorrelation(),
            Some(CompositeSemiDecorrelation {
                outer_key_column: 0,
                outer_member_column: 1,
                inner_key_column: 1,
                inner_member_column: 0,
            })
        );
        // The binder appended the correlation key after the membership column, so the
        // inner projects exactly `[a, id]` — the two composite-key components in
        // result order. It is not the single-key `EXISTS` shape.
        assert_eq!(
            filter.subquery.projection,
            Projection::Items(vec![
                ProjectionItem::column("a"),
                ProjectionItem::column("id")
            ])
        );
        assert_eq!(filter.semi_anti_decorrelation(), None);
    }

    #[test]
    fn not_in_and_non_decorrelatable_in_keep_the_per_row_fallback() {
        let catalog = catalog_with_subquery_tables();
        // `NOT IN`'s NULL-in-set trap is per-correlation-group, not an anti join, so
        // it stays per-row (a NULL-aware anti is a tracked follow-up). Its inner is
        // left as the lone membership column.
        let not_in = subquery_filter(
            "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.id = t.id)",
            &catalog,
        );
        assert_eq!(not_in.composite_semi_decorrelation(), None);
        assert_eq!(
            not_in.subquery.projection,
            Projection::Items(vec![ProjectionItem::column("a")])
        );
        // A range correlation is not key-set membership.
        assert_eq!(
            subquery_filter(
                "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.id > t.id)",
                &catalog,
            )
            .composite_semi_decorrelation(),
            None
        );
        // An *uncorrelated* IN folds to an OR-set once, not a join — and its inner is
        // never widened.
        let uncorrelated =
            subquery_filter("SELECT id FROM t WHERE a IN (SELECT a FROM s)", &catalog);
        assert_eq!(uncorrelated.composite_semi_decorrelation(), None);
        assert_eq!(
            uncorrelated.subquery.projection,
            Projection::Items(vec![ProjectionItem::column("a")])
        );
    }

    #[test]
    fn the_inner_subquery_inherits_the_outer_snapshot() {
        let catalog = catalog_with_subquery_tables();
        // With no AS OF, the inner reads the transaction snapshot, like the outer.
        let bound =
            bind("SELECT id FROM t WHERE a IN (SELECT a FROM s)", &catalog).expect("bind subquery");
        let inner = &bound.subquery_filter.expect("subquery").subquery;
        assert_eq!(inner.snapshot, NOW);
        assert_eq!(inner.snapshot, bound.snapshot);

        // A `FOR SYSTEM_TIME AS OF p` on the outer pins the inner at `p` too.
        let bound = bind(
            "SELECT id FROM t WHERE a IN (SELECT a FROM s) FOR SYSTEM_TIME AS OF 1500",
            &catalog,
        )
        .expect("bind subquery AS OF");
        assert_eq!(bound.snapshot, SystemTimeMicros(1_500));
        let inner = &bound.subquery_filter.expect("subquery").subquery;
        assert_eq!(inner.snapshot, SystemTimeMicros(1_500));
    }

    #[test]
    fn rejects_a_type_mismatched_in_subquery() {
        let catalog = catalog_with_subquery_tables();
        // `t.a` is INT, `s2.b` is TEXT — no implicit coercion.
        let err = bind("SELECT id FROM t WHERE a IN (SELECT b FROM s2)", &catalog).unwrap_err();
        assert!(matches!(err, SelectError::Subquery(_)), "got {err:?}");
    }

    #[test]
    fn rejects_a_multi_column_in_subquery() {
        let catalog = catalog_with_subquery_tables();
        let err = bind(
            "SELECT id FROM t WHERE a IN (SELECT id, a FROM s)",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Subquery(_)), "got {err:?}");
    }

    #[test]
    fn rejects_a_comparison_between_two_subqueries() {
        let catalog = catalog_with_subquery_tables();
        let err = bind(
            "SELECT id FROM t WHERE (SELECT a FROM s) = (SELECT a FROM s)",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Subquery(_)), "got {err:?}");
    }

    #[test]
    fn rejects_a_non_column_outer_operand() {
        let catalog = catalog_with_subquery_tables();
        // The outer operand of a scalar/IN subquery must be a bare column.
        let err = bind(
            "SELECT id FROM t WHERE a + 1 IN (SELECT a FROM s)",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Subquery(_)), "got {err:?}");
    }

    #[test]
    fn rejects_a_valid_time_pin_over_a_join_subquery() {
        // A direct join now time-travels ([STL-243]), but inheriting the outer's
        // FOR VALID_TIME AS OF pin into a subquery that joins tables is a separate
        // composition ([STL-264]); until it lands the subquery fails closed rather
        // than pinning a side that may have no valid axis.
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "bk",
                vec![
                    ColumnDef::new("k", LogicalType::Int4).expect("col"),
                    ColumnDef::new("v", LogicalType::Int4).expect("col"),
                    ColumnDef::new("vf", LogicalType::Timestamp).expect("col"),
                    ColumnDef::new("ve", LogicalType::Timestamp).expect("col"),
                ],
                TableTemporal::with_valid_time(ValidTimeSpec::new("vf", "ve").expect("spec")),
                SystemTimeMicros(1_000),
            )
            .expect("create bk");
        for name in ["a", "b"] {
            catalog
                .create_table(
                    name,
                    vec![
                        ColumnDef::new("id", LogicalType::Int4).expect("col"),
                        ColumnDef::new("n", LogicalType::Int4).expect("col"),
                    ],
                    TableTemporal::system_only(),
                    SystemTimeMicros(1_000),
                )
                .expect("create");
        }
        let err = bind(
            "SELECT k FROM bk WHERE v IN (SELECT a.n FROM a JOIN b ON a.id = b.id) \
             FOR VALID_TIME AS OF 100",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Subquery(_)), "got {err:?}");
    }

    // ---- session time context injection ([STL-246]) ---------------------------

    #[test]
    fn session_time_injects_an_as_of_per_axis() {
        let mut stmt = parse_one("SELECT * FROM booking");
        apply_session_time(
            &mut stmt,
            Some(SystemTimeMicros(111)),
            Some(SystemTimeMicros(222)),
            |t| t == "booking",
        );
        // Both axes are now qualified, each folding back to the pinned instant —
        // exactly as if the user had typed the explicit `FOR … AS OF` form.
        let mut by_dim: Vec<_> = stmt
            .temporal
            .as_of
            .iter()
            .map(|a| (a.dimension, resolve_as_of(&a.timestamp, NOW).expect("fold")))
            .collect();
        by_dim.sort_by_key(|(d, _)| matches!(d, TimeDimension::Valid));
        assert_eq!(
            by_dim,
            vec![
                (TimeDimension::System, SystemTimeMicros(111)),
                (TimeDimension::Valid, SystemTimeMicros(222)),
            ]
        );
    }

    #[test]
    fn session_pinned_read_binds_identically_to_the_explicit_as_of() {
        // The core equivalence at the binder layer: a session-pinned bare read and
        // the hand-written `FOR … AS OF` read bind to the same snapshots.
        let catalog = catalog_with_booking(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };

        let mut pinned = parse_one("SELECT id FROM booking");
        apply_session_time(
            &mut pinned,
            Some(SystemTimeMicros(5_000)),
            Some(SystemTimeMicros(6_000)),
            |t| valid_axis(&catalog, t),
        );
        let pinned = bind_select(&pinned, &ctx).expect("bind pinned");

        let explicit = parse_one(
            "SELECT id FROM booking \
             FOR SYSTEM_TIME AS OF 5000 FOR VALID_TIME AS OF 6000",
        );
        let explicit = bind_select(&explicit, &ctx).expect("bind explicit");

        assert_eq!(pinned.snapshot, explicit.snapshot);
        assert_eq!(pinned.valid_snapshot, explicit.valid_snapshot);
        assert_eq!(pinned.snapshot, SystemTimeMicros(5_000));
        assert_eq!(pinned.valid_snapshot, Some(SystemTimeMicros(6_000)));
    }

    #[test]
    fn an_explicit_as_of_wins_over_the_session_pin() {
        let mut stmt = parse_one("SELECT * FROM account FOR SYSTEM_TIME AS OF 999");
        apply_session_time(&mut stmt, Some(SystemTimeMicros(111)), None, |_| false);
        // The statement's own qualifier is kept; the pin does not add a second one
        // (which would be a `MultipleAsOf` error at bind time).
        assert_eq!(stmt.temporal.as_of.len(), 1);
        assert_eq!(
            resolve_as_of(&stmt.temporal.as_of[0].timestamp, NOW),
            Ok(SystemTimeMicros(999))
        );
    }

    #[test]
    fn session_time_leaves_writes_tvfs_and_constants_untouched() {
        // A write never time-travels.
        let mut dml = parse_one("INSERT INTO account VALUES (1, 2)");
        apply_session_time(&mut dml, Some(SystemTimeMicros(111)), None, |_| true);
        assert!(dml.temporal.as_of.is_empty());

        // A FROM-less constant SELECT has no table to pin.
        let mut constant = parse_one("SELECT 1");
        apply_session_time(&mut constant, Some(SystemTimeMicros(111)), None, |_| true);
        assert!(constant.temporal.as_of.is_empty());

        // A table-valued introspection call (`args` present) rejects `AS OF`
        // outright, so the session pin must not inject one — kept out by the TVF
        // exclusion the join change preserves ([STL-325]).
        let mut tvf = parse_one("SELECT * FROM stele_history('account')");
        apply_session_time(&mut tvf, Some(SystemTimeMicros(111)), None, |_| true);
        assert!(tvf.temporal.as_of.is_empty());
    }

    #[test]
    fn session_system_pin_injects_over_a_join() {
        // [STL-325]: the system axis is always present, so a session system pin is
        // injected over a join just as an explicit `FOR SYSTEM_TIME AS OF` binds over
        // one ([STL-243]) — the join no longer reads live under a system pin.
        let mut join = parse_one("SELECT a.x FROM a JOIN b ON a.id = b.id");
        apply_session_time(&mut join, Some(SystemTimeMicros(111)), None, |_| true);
        let [as_of] = join.temporal.as_of.as_slice() else {
            panic!("the system pin injects exactly one AS OF over the join");
        };
        assert_eq!(as_of.dimension, TimeDimension::System);
        assert_eq!(
            resolve_as_of(&as_of.timestamp, NOW),
            Ok(SystemTimeMicros(111))
        );
    }

    #[test]
    fn session_valid_pin_over_a_join_needs_every_input_to_have_a_valid_axis() {
        // [STL-325]: a valid pin is injected over a join when *both* inputs opt into a
        // valid axis (the same check the binder makes per side, [STL-243]).
        let mut both_valid = parse_one("SELECT a.x FROM a JOIN b ON a.id = b.id");
        apply_session_time(
            &mut both_valid,
            Some(SystemTimeMicros(111)),
            Some(SystemTimeMicros(222)),
            |t| matches!(t, "a" | "b"),
        );
        assert_eq!(both_valid.temporal.as_of.len(), 2, "both axes pin");
        let has = |stmt: &Statement, dim| stmt.temporal.as_of.iter().any(|a| a.dimension == dim);
        assert!(
            has(&both_valid, TimeDimension::System) && has(&both_valid, TimeDimension::Valid),
            "both axes pin over a join of two valid-time tables"
        );

        // But withheld (not an error) when an input is system-only: the system pin
        // still applies; the valid pin is simply not injected (the join reads live on
        // the valid axis), so a working join over a system-only table never breaks.
        let mut mixed = parse_one("SELECT a.x FROM a JOIN b ON a.id = b.id");
        apply_session_time(
            &mut mixed,
            Some(SystemTimeMicros(111)),
            Some(SystemTimeMicros(222)),
            |t| t == "a",
        );
        let [as_of] = mixed.temporal.as_of.as_slice() else {
            panic!("only the system pin injects over a join with a system-only input");
        };
        assert_eq!(as_of.dimension, TimeDimension::System);
    }

    #[test]
    fn session_pin_extends_over_an_n_way_join() {
        // [STL-325] + [STL-323]: a session pin reaches every input of a left-deep
        // N-way join, exactly as an explicit `AS OF` does (the binder threads it to
        // all sides). The system pin always injects; the valid pin only when *every*
        // input has a valid axis.
        let three_way = "SELECT a.x FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
        let has = |stmt: &Statement, dim| stmt.temporal.as_of.iter().any(|a| a.dimension == dim);

        // All three valid-time: both axes pin over the whole chain.
        let mut all_valid = parse_one(three_way);
        apply_session_time(
            &mut all_valid,
            Some(SystemTimeMicros(111)),
            Some(SystemTimeMicros(222)),
            |t| matches!(t, "a" | "b" | "c"),
        );
        assert_eq!(
            all_valid.temporal.as_of.len(),
            2,
            "both axes pin over a 3-way join"
        );
        assert!(has(&all_valid, TimeDimension::System) && has(&all_valid, TimeDimension::Valid));

        // One input system-only: the valid pin is withheld (no error), system stays.
        let mut mixed = parse_one(three_way);
        apply_session_time(
            &mut mixed,
            Some(SystemTimeMicros(111)),
            Some(SystemTimeMicros(222)),
            |t| matches!(t, "a" | "b"),
        );
        let [as_of] = mixed.temporal.as_of.as_slice() else {
            panic!("only the system pin injects when a 3-way input is system-only");
        };
        assert_eq!(as_of.dimension, TimeDimension::System);
    }

    #[test]
    fn session_valid_pin_is_withheld_from_a_system_only_single_table() {
        // [STL-325]: the valid-axis gate applies to single-table reads too — a session
        // valid pin over a system-only table is withheld (the read stays live on the
        // valid axis), not injected into a `ValidTimeUnsupported` bind error.
        let mut stmt = parse_one("SELECT * FROM account");
        apply_session_time(
            &mut stmt,
            Some(SystemTimeMicros(111)),
            Some(SystemTimeMicros(222)),
            |_| false,
        );
        let [as_of] = stmt.temporal.as_of.as_slice() else {
            panic!("only the system pin injects over a system-only single table");
        };
        assert_eq!(as_of.dimension, TimeDimension::System);
    }

    #[test]
    fn session_time_leaves_a_range_scan_untouched() {
        // A `FOR { SYSTEM_TIME | VALID_TIME } FROM/BETWEEN` range scan ([STL-244],
        // [STL-328]) lifts its qualifier off the token stream like an `AS OF` point, so
        // its residual query looks single-table here — but the binder rejects a range
        // combined with a point `AS OF`, so the session pin must not inject one
        // ([STL-325], mirroring `cap_unbounded_select`). Both axes share the one
        // `temporal.range` field, so the guard covers each.
        for range_sql in [
            "SELECT id FROM booking FOR SYSTEM_TIME FROM 100 TO 200",
            "SELECT id FROM booking FOR VALID_TIME BETWEEN 100 AND 200",
        ] {
            let mut range = parse_one(range_sql);
            assert!(
                range.temporal.range.is_some(),
                "the range qualifier is lifted off the token stream: {range_sql}"
            );
            apply_session_time(
                &mut range,
                Some(SystemTimeMicros(111)),
                Some(SystemTimeMicros(222)),
                |_| true,
            );
            assert!(
                range.temporal.as_of.is_empty(),
                "a session pin must not inject an AS OF into a range scan: {range_sql}"
            );
        }
    }

    #[test]
    fn a_system_only_pin_does_not_consult_the_valid_axis_predicate() {
        // With no valid pin set the catalog predicate must not be consulted ([STL-325]):
        // a system-only pin needs no valid-axis resolution. A panicking closure proves
        // the `valid.is_some()` short-circuit.
        let mut stmt = parse_one("SELECT a.x FROM a JOIN b ON a.id = b.id");
        apply_session_time(&mut stmt, Some(SystemTimeMicros(111)), None, |_| {
            panic!("the valid-axis predicate was consulted with no valid pin set")
        });
        let [as_of] = stmt.temporal.as_of.as_slice() else {
            panic!("the system pin injects exactly one AS OF");
        };
        assert_eq!(as_of.dimension, TimeDimension::System);
    }

    #[test]
    fn an_empty_session_context_is_a_no_op() {
        let mut stmt = parse_one("SELECT * FROM account");
        apply_session_time(&mut stmt, None, None, |_| false);
        assert!(stmt.temporal.as_of.is_empty());
    }

    // ----- CTEs + derived tables ([STL-242]) -----

    #[test]
    fn cte_reference_resolves_as_a_materialized_relation() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "WITH c AS (SELECT id, balance FROM account) SELECT id FROM c",
            &catalog,
        )
        .expect("bind CTE reference");
        // One CTE to materialize; the FROM names it and carries its columns.
        assert_eq!(bound.ctes.len(), 1);
        assert_eq!(bound.ctes[0].name, "c");
        assert_eq!(bound.table, "c");
        assert_eq!(
            bound.relation_columns,
            Some(vec![
                ("id".to_owned(), LogicalType::Int4),
                ("balance".to_owned(), LogicalType::Int4),
            ])
        );
        assert_eq!(
            bound.projection,
            Projection::Items(vec![ProjectionItem::column("id")])
        );
        // The CTE body is a plain base-table read of `account`.
        assert_eq!(bound.ctes[0].plan.table, "account");
        assert_eq!(bound.ctes[0].plan.relation_columns, None);
    }

    #[test]
    fn derived_table_lowers_to_a_single_use_cte() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "SELECT id FROM (SELECT id, balance FROM account) AS d",
            &catalog,
        )
        .expect("bind derived table");
        assert_eq!(bound.ctes.len(), 1);
        assert_eq!(bound.ctes[0].name, "d");
        assert_eq!(bound.table, "d");
        assert!(bound.relation_columns.is_some());
    }

    #[test]
    fn later_cte_references_an_earlier_one() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "WITH c1 AS (SELECT id, balance FROM account), \
                  c2 AS (SELECT id FROM c1) \
             SELECT id FROM c2",
            &catalog,
        )
        .expect("bind chained CTEs");
        assert_eq!(bound.ctes.len(), 2);
        assert_eq!(bound.ctes[0].name, "c1");
        assert_eq!(bound.ctes[1].name, "c2");
        // c2's body reads c1 — a materialized relation, not the catalog.
        assert_eq!(bound.ctes[1].plan.table, "c1");
        assert!(bound.ctes[1].plan.relation_columns.is_some());
    }

    #[test]
    fn cte_column_aliases_rename_the_output_header() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "WITH c(k, v) AS (SELECT id, balance FROM account) SELECT k FROM c",
            &catalog,
        )
        .expect("bind aliased CTE");
        assert_eq!(
            bound.ctes[0].columns,
            vec![
                ("k".to_owned(), LogicalType::Int4),
                ("v".to_owned(), LogicalType::Int4),
            ]
        );
        assert_eq!(
            bound.projection,
            Projection::Items(vec![ProjectionItem::column("k")])
        );
    }

    #[test]
    fn cte_joined_to_a_base_table_binds_both_sides() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "WITH c AS (SELECT id, balance FROM account) \
             SELECT c.id, account.balance FROM c JOIN account ON c.id = account.id",
            &catalog,
        )
        .expect("bind CTE joined to base table");
        let join = bound.join.expect("a join plan");
        assert_eq!(join.left.table, "c");
        assert_eq!(join.steps[0].right.table, "account");
        // The side's schema id is the executor's materialized-vs-storage signal:
        // the CTE side carries the ephemeral SchemaId(0) sentinel, the base table a
        // catalog-allocated id (never 0). `join_side_columns` reads the CTE from the
        // scope and always scans the base table from storage on that basis.
        assert_eq!(join.left.schema_id, SchemaId(0));
        assert_ne!(join.steps[0].right.schema_id, SchemaId(0));
        // The CTE side is registered for the executor to materialize.
        assert_eq!(bound.ctes.len(), 1);
        assert_eq!(bound.ctes[0].name, "c");
    }

    #[test]
    fn aggregate_over_a_cte_binds() {
        let catalog = catalog_with_account(1_000);
        let bound = bind(
            "WITH c AS (SELECT id, balance FROM account) SELECT count(*) FROM c",
            &catalog,
        )
        .expect("bind aggregate over a CTE");
        assert!(bound.aggregate.is_some());
        assert!(bound.relation_columns.is_some());
        assert_eq!(bound.table, "c");
    }

    #[test]
    fn recursive_with_is_rejected() {
        let catalog = catalog_with_account(1_000);
        let err = bind(
            "WITH RECURSIVE c AS (SELECT id, balance FROM account) SELECT id FROM c",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Cte(_)), "got {err:?}");
    }

    #[test]
    fn duplicate_cte_name_is_rejected() {
        let catalog = catalog_with_account(1_000);
        let err = bind(
            "WITH c AS (SELECT id FROM account), c AS (SELECT id FROM account) SELECT id FROM c",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Cte(_)), "got {err:?}");
    }

    #[test]
    fn column_alias_arity_mismatch_is_rejected() {
        let catalog = catalog_with_account(1_000);
        let err = bind(
            "WITH c(only_one) AS (SELECT id, balance FROM account) SELECT only_one FROM c",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Cte(_)), "got {err:?}");
    }

    #[test]
    fn valid_time_as_of_over_a_cte_is_unsupported() {
        let catalog = catalog_with_account(1_000);
        // A CTE's ephemeral schema is system-only, so a valid-axis pin has nothing
        // to travel — the documented `ValidTimeUnsupported`, not a wrong read.
        let err = bind(
            "WITH c AS (SELECT id, balance FROM account) \
             SELECT id FROM c FOR VALID_TIME AS OF 100",
            &catalog,
        )
        .unwrap_err();
        assert!(
            matches!(err, SelectError::ValidTimeUnsupported { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn unknown_column_over_a_cte_is_rejected() {
        let catalog = catalog_with_account(1_000);
        // `nope` is not one of the CTE's output columns.
        let err = bind(
            "WITH c AS (SELECT id, balance FROM account) SELECT nope FROM c",
            &catalog,
        )
        .unwrap_err();
        assert!(
            matches!(err, SelectError::UnknownColumn { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn derived_alias_colliding_with_a_cte_name_is_rejected() {
        let catalog = catalog_with_account(1_000);
        // A derived table aliased `c` collides with the `WITH c` name; the flat
        // materialization scope cannot hold both, so it is rejected (not silently
        // overwritten).
        let err = bind(
            "WITH c AS (SELECT id, balance FROM account) \
             SELECT id FROM (SELECT id, balance FROM account) AS c",
            &catalog,
        )
        .unwrap_err();
        assert!(matches!(err, SelectError::Cte(_)), "got {err:?}");
    }
}
