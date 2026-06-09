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
//! `valid_snapshot` is `None` — the executor reads the present of the valid axis.
//!
//! ## Scope
//!
//! A single, unqualified table; projection of `*` or bare column names; the
//! `WHERE` clause is left on the AST for the executor-glue layer to lower
//! (pgwire, [STL-104]). Absolute `TIMESTAMP '…'` / `DATE '…'` literals in an
//! `AS OF` are not folded yet (no civil-time codec); they surface
//! [`AsOfError::Unsupported`] rather than a wrong instant.

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArguments, GroupByExpr, Query, Select, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, Value,
};
use stele_catalog::{Catalog, SchemaId, TableSchema};
use stele_common::time::SystemTimeMicros;
use stele_common::types::{LogicalType, ScalarValue};

use crate::ast::{Statement, TimeDimension};
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
    /// ([STL-163]). `None` reads the present of the valid axis.
    pub valid_snapshot: Option<SystemTimeMicros>,
    /// The columns the query projects.
    pub projection: Projection,
    /// The lowered `WHERE` predicate, or `None` for an unfiltered read. v0.2
    /// lowers `<column> = <literal>` only ([STL-151]).
    pub filter: Option<BoundPredicate>,
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
    let table = single_table(select)?;
    let projection = bind_projection(select)?;
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

    let filter = bind_filter(select, schema, table)?;

    Ok(BoundSelect {
        table: table.to_owned(),
        schema_id: schema.schema_id(),
        snapshot,
        valid_snapshot,
        projection,
        filter,
    })
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
        FoldError::BadLiteral { literal } => {
            format!("{literal:?} is not a valid {ty} for column {column:?}")
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
    // `GROUP BY ALL`, or `GROUP BY <exprs>` / a trailing modifier — only the
    // empty `Expressions(<none>, <none>)` is "no grouping".
    if !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
    {
        return reject("GROUP BY");
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
            "SELECT balance FROM account GROUP BY balance",
            "SELECT balance FROM account GROUP BY balance HAVING balance > 0",
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
    fn non_select_and_joins_are_rejected() {
        let catalog = catalog_with_account(1_000);
        let ctx = BindContext {
            snapshot: NOW,
            catalog: &catalog,
        };
        let ddl = parse_one("CREATE TABLE t (a INT) WITH SYSTEM VERSIONING");
        assert_eq!(bind_select(&ddl, &ctx), Err(SelectError::NotSelect));
        let join = parse_one("SELECT a FROM account JOIN other ON account.id = other.id");
        assert!(matches!(
            bind_select(&join, &ctx),
            Err(SelectError::UnsupportedFrom(_))
        ));
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
}
