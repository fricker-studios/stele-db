//! The **vectorized scalar expression evaluator** — comparisons, integer
//! arithmetic, boolean `AND`/`OR`/`NOT`, and SQL three-valued NULL logic
//! evaluated a whole batch at a time ([STL-170] `[C10]`).
//!
//! It is the value engine the vectorized [`Filter`](crate::Filter) operator runs
//! on, and the same evaluator a projected expression (`SELECT a + 1`) will lower
//! to. Where [`crate::period`] evaluates a single constant period predicate, this
//! module evaluates an arbitrary expression *tree* over columnar data.
//!
//! ## Shape
//!
//! An [`Expr`] is a small tree of column references ([`Expr::Column`], by
//! position in the batch), constant [`Expr::Literal`]s, and the operators above.
//! [`eval_expr`] walks it bottom-up, producing one [`Vector`] per node — a typed,
//! **nullable** columnar array the same height as the batch. The leaves read the
//! batch's columns; each interior node consumes its children's vectors and
//! produces a new one. The result of a boolean-typed expression is a
//! `Vector::Bool` whose `Some(true)` cells are the rows a `WHERE` keeps.
//!
//! The evaluation currency is [`Vector`] rather than the storage-facing
//! [`Column`](crate::Column): a `Column` is either non-null `i64` or
//! per-cell-nullable bytes ([ADR-0027](../../../docs/adr/0027-vectorized-execution-model.md)),
//! which cannot represent a *nullable* intermediate (a comparison over a NULL
//! cell, an overflowed sum). [`Vector::from_column`] bridges a storage column
//! into the typed, nullable evaluation form, decoding bytes by the column's
//! [`LogicalType`].
//!
//! ## Three-valued logic
//!
//! Every operator follows SQL's three-valued logic: NULL is "unknown", and it
//! propagates except where the result is already determined.
//!
//! * **Comparisons / arithmetic** are *strict*: if either operand cell is NULL
//!   the result cell is NULL. (`5 = NULL` is NULL, not false; `NULL + 1` is
//!   NULL.) Integer arithmetic that overflows `i32`/`i64` also yields NULL rather
//!   than trapping, keeping the evaluator total over its data.
//! * **`AND` / `OR`** are the SQL truth tables, where a determining operand wins
//!   over an unknown one:
//!
//!   | `AND`     | T | F | N |   | `OR`      | T | F | N |
//!   |-----------|---|---|---|---|-----------|---|---|---|
//!   | **T**     | T | F | N |   | **T**     | T | T | T |
//!   | **F**     | F | F | F |   | **F**     | T | F | N |
//!   | **N**     | N | F | N |   | **N**     | T | N | N |
//!
//! * **`NOT`** maps T↔F and leaves N unchanged.
//! * **`IS NULL`** is the one two-valued operator: it is never NULL, reporting
//!   `true`/`false` for whether the operand cell is NULL.
//!
//! ## Scope
//!
//! The evaluator handles every scalar logical type a `WHERE` over the v0.2 row
//! codec can reach: the four original [`LogicalType::Int4`],
//! [`LogicalType::Int8`], [`LogicalType::Bool`], [`LogicalType::Text`]
//! ([STL-170]), the temporal [`LogicalType::Timestamp`] /
//! [`LogicalType::TimestampTz`] / [`LogicalType::Date`], byte-ordered
//! [`LogicalType::Uuid`] / [`LogicalType::Bytea`], and [`LogicalType::Period`]
//! ([STL-207]). Period operands additionally compose with the SQL:2011 period
//! predicates ([`Expr::Period`], over [`crate::period::evaluate`]). Integer
//! arithmetic covers `+`/`-`/`*` and `/`/`%`.
//!
//! [`LogicalType::Float8`] is the one type still outside the evaluator: it exists
//! only as the result of `AVG` ([STL-209]) — no column decodes into it, no
//! literal of it is folded — so an expression that reaches one surfaces a typed
//! [`ExprError`] rather than a wrong answer.
//!
//! ## Divide-by-zero (and overflow) are NULL, not a trap
//!
//! Integer division and modulo by zero, like arithmetic overflow, yield a NULL
//! result cell rather than erroring — the evaluator stays *total* over its data
//! ([`arith`]). This deliberately diverges from Postgres, which raises
//! `division_by_zero`: a vectorized kernel evaluates every row of a batch
//! unconditionally, so a single zero divisor cannot be allowed to abort the
//! whole batch, and "unknown result" is exactly what NULL already means here.
//!
//! [STL-170]: https://allegromusic.atlassian.net/browse/STL-170
//! [STL-207]: https://allegromusic.atlassian.net/browse/STL-207
//! [STL-209]: https://allegromusic.atlassian.net/browse/STL-209

use stele_common::period::{Interval, PeriodPredicate};
use stele_common::types::{DecodeError, LogicalType, ScalarValue};

use crate::period::evaluate;
use crate::snapshot_scan::Column;

/// A comparison operator. Defined for every supported scalar type via that
/// type's total order (`false < true` for booleans, lexicographic for text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `=` — equal.
    Eq,
    /// `<>` — not equal.
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

/// A binary boolean connective. `NOT` is the separate unary [`Expr::Not`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicOp {
    /// `AND` — conjunction (a determining `false` wins over an unknown NULL).
    And,
    /// `OR` — disjunction.
    Or,
}

/// An integer arithmetic operator. `/` and `%` divide-by-zero to a NULL cell
/// (like overflow), keeping the evaluator total — see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    /// `+` — addition.
    Add,
    /// `-` — subtraction.
    Sub,
    /// `*` — multiplication.
    Mul,
    /// `/` — truncating integer division; divide-by-zero ⇒ NULL.
    Div,
    /// `%` — remainder (sign follows the dividend); divide-by-zero ⇒ NULL.
    Mod,
}

/// A scalar expression over a batch's columns.
///
/// A small tree the [`eval_expr`] walker turns into a [`Vector`]. Columns are
/// referenced **by position** in the batch ([`Expr::Column`]); literals are
/// non-null [`ScalarValue`]s (a NULL constant is not a useful filter and the
/// binder rejects it, so NULL enters only through column cells).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// The batch column at this position, read as-is.
    Column(usize),
    /// A constant, broadcast to every row.
    Literal(ScalarValue),
    /// `NOT operand` — three-valued negation.
    Not(Box<Expr>),
    /// `operand IS NULL` — a non-null boolean per row.
    IsNull(Box<Expr>),
    /// `left <op> right` — a comparison yielding a (nullable) boolean.
    Compare {
        /// The comparison operator.
        op: CmpOp,
        /// Left operand.
        left: Box<Expr>,
        /// Right operand.
        right: Box<Expr>,
    },
    /// `left <op> right` — a boolean connective.
    Logic {
        /// `AND` or `OR`.
        op: LogicOp,
        /// Left operand.
        left: Box<Expr>,
        /// Right operand.
        right: Box<Expr>,
    },
    /// `left <op> right` — integer arithmetic.
    Arith {
        /// The arithmetic operator.
        op: ArithOp,
        /// Left operand.
        left: Box<Expr>,
        /// Right operand.
        right: Box<Expr>,
    },
    /// `left <pred> right` — a SQL:2011 period predicate (`CONTAINS`, `OVERLAPS`,
    /// …) over two [`LogicalType::Period`] operands, yielding a (nullable)
    /// boolean. The membership/adjacency relations [`crate::period::evaluate`]
    /// defines, lifted into the expression tree so they apply per row rather than
    /// only to a constant-folded pair ([STL-165]).
    Period {
        /// The period predicate to apply.
        pred: PeriodPredicate,
        /// Left period operand.
        left: Box<Expr>,
        /// Right period operand.
        right: Box<Expr>,
    },
}

impl Expr {
    /// Reference the batch column at `index`.
    #[must_use]
    pub const fn col(index: usize) -> Self {
        Self::Column(index)
    }

    /// A constant operand.
    #[must_use]
    pub const fn lit(value: ScalarValue) -> Self {
        Self::Literal(value)
    }

    /// `self <op> other` — a comparison node.
    #[must_use]
    pub fn compare(self, op: CmpOp, other: Self) -> Self {
        Self::Compare {
            op,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// `self <op> other` — a boolean connective node.
    #[must_use]
    pub fn logic(self, op: LogicOp, other: Self) -> Self {
        Self::Logic {
            op,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// `self <op> other` — an arithmetic node.
    #[must_use]
    pub fn arith(self, op: ArithOp, other: Self) -> Self {
        Self::Arith {
            op,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// `self <pred> other` — a period-predicate node over two PERIOD operands.
    #[must_use]
    pub fn period(self, pred: PeriodPredicate, other: Self) -> Self {
        Self::Period {
            pred,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// `NOT self`.
    #[must_use]
    pub fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }

    /// `self IS NULL`.
    #[must_use]
    pub fn is_null(self) -> Self {
        Self::IsNull(Box::new(self))
    }
}

/// A typed, nullable columnar array — the evaluator's working representation.
///
/// One variant per supported scalar type, each a per-cell-nullable vector. This
/// is the inter-node currency of [`eval_expr`]: a leaf produces one, every
/// interior node consumes its children's and produces a new one. Unlike the
/// storage [`Column`], every variant carries per-cell nullability so a NULL that
/// arises mid-expression (a comparison over a NULL, an overflowed sum) has a
/// representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Vector {
    /// Booleans — the result of comparisons and connectives.
    Bool(Vec<Option<bool>>),
    /// 32-bit integers ([`LogicalType::Int4`]).
    Int4(Vec<Option<i32>>),
    /// 64-bit integers ([`LogicalType::Int8`]).
    Int8(Vec<Option<i64>>),
    /// UTF-8 text ([`LogicalType::Text`]).
    Text(Vec<Option<String>>),
    /// Microsecond UTC instants ([`LogicalType::Timestamp`]).
    Timestamp(Vec<Option<i64>>),
    /// Time-zone-aware microsecond instants ([`LogicalType::TimestampTz`]) —
    /// stored as the same UTC microseconds as [`Self::Timestamp`] but a distinct
    /// type, so the two never silently compare against each other.
    TimestampTz(Vec<Option<i64>>),
    /// Calendar dates as days since the Unix epoch ([`LogicalType::Date`]).
    Date(Vec<Option<i32>>),
    /// 128-bit UUIDs ([`LogicalType::Uuid`]). Comparisons are byte-ordered over
    /// the 16 raw network-order bytes.
    Uuid(Vec<Option<[u8; 16]>>),
    /// Variable-length byte strings ([`LogicalType::Bytea`]). Comparisons are
    /// lexicographic over the raw bytes.
    Bytea(Vec<Option<Vec<u8>>>),
    /// Half-open `[from, to)` periods ([`LogicalType::Period`]). Plain
    /// comparisons order lexicographically by `(from, to)`; the SQL:2011 period
    /// predicates apply through [`Expr::Period`].
    Period(Vec<Option<Interval>>),
    /// Double-precision floats, each held as its IEEE-754 bit pattern
    /// (`f64::to_bits`, the same representation [`ScalarValue::Float8`] uses) so
    /// the vector stays `Eq`. The aggregator produces this for `AVG`
    /// ([STL-209]); no storage column decodes into it, and the scalar evaluator
    /// neither compares nor computes over it — `float8` is the one logical type
    /// still outside the evaluator's scope.
    Float8(Vec<Option<u64>>),
}

impl Vector {
    /// The number of cells.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(v) => v.len(),
            Self::Int4(v) | Self::Date(v) => v.len(),
            Self::Int8(v) | Self::Timestamp(v) | Self::TimestampTz(v) => v.len(),
            Self::Text(v) => v.len(),
            Self::Uuid(v) => v.len(),
            Self::Bytea(v) => v.len(),
            Self::Period(v) => v.len(),
            Self::Float8(v) => v.len(),
        }
    }

    /// Whether the vector has no cells.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The logical type of the vector's cells — for diagnostics and the
    /// same-type checks comparisons/arithmetic enforce.
    #[must_use]
    pub const fn logical_type(&self) -> LogicalType {
        match self {
            Self::Bool(_) => LogicalType::Bool,
            Self::Int4(_) => LogicalType::Int4,
            Self::Int8(_) => LogicalType::Int8,
            Self::Text(_) => LogicalType::Text,
            Self::Timestamp(_) => LogicalType::Timestamp,
            Self::TimestampTz(_) => LogicalType::TimestampTz,
            Self::Date(_) => LogicalType::Date,
            Self::Uuid(_) => LogicalType::Uuid,
            Self::Bytea(_) => LogicalType::Bytea,
            Self::Period(_) => LogicalType::Period,
            Self::Float8(_) => LogicalType::Float8,
        }
    }

    /// The cell at `row` as an `Option<ScalarValue>` — `None` for a NULL cell.
    ///
    /// The inverse of the broadcast leaves: it reconstitutes a single scalar so a
    /// caller (the row-at-a-time oracle, a result reader) can read one cell out
    /// of the columnar result.
    ///
    /// # Panics
    ///
    /// If `row` is out of range — callers index within [`len`](Self::len).
    #[must_use]
    pub fn get(&self, row: usize) -> Option<ScalarValue> {
        match self {
            Self::Bool(v) => v[row].map(ScalarValue::Bool),
            Self::Int4(v) => v[row].map(ScalarValue::Int4),
            Self::Int8(v) => v[row].map(ScalarValue::Int8),
            Self::Text(v) => v[row].clone().map(ScalarValue::Text),
            Self::Timestamp(v) => v[row].map(ScalarValue::Timestamp),
            Self::TimestampTz(v) => v[row].map(ScalarValue::TimestampTz),
            Self::Date(v) => v[row].map(ScalarValue::Date),
            Self::Uuid(v) => v[row].map(ScalarValue::Uuid),
            Self::Bytea(v) => v[row].clone().map(ScalarValue::Bytea),
            Self::Period(v) => v[row].map(ScalarValue::Period),
            // The cell already holds `f64::to_bits`, the same form
            // `ScalarValue::Float8` carries — pass it straight through.
            Self::Float8(v) => v[row].map(ScalarValue::Float8),
        }
    }

    /// Gather cells at `rows` into a new vector of the same type, taking the
    /// cell value for a `Some(r)` index and a NULL cell for a `None` index.
    ///
    /// The row-selection the hash-aggregation operator
    /// ([`crate::hash_aggregate`]) uses to assemble its output columns: a group's
    /// representative row for a passed-through grouping key, the row holding a
    /// group's extreme value for `MIN` / `MAX`, or `None` for a group whose
    /// `MIN` / `MAX` saw only NULLs. Type-preserving and total — every index in
    /// `rows` must be in range for a `Some`, which the operator guarantees by
    /// only ever naming rows of the vector it gathers from.
    ///
    /// # Panics
    ///
    /// If a `Some(r)` index is out of range.
    #[must_use]
    pub fn gather(&self, rows: &[Option<usize>]) -> Self {
        fn pick<T: Clone>(cells: &[Option<T>], rows: &[Option<usize>]) -> Vec<Option<T>> {
            rows.iter()
                .map(|slot| slot.and_then(|r| cells[r].clone()))
                .collect()
        }
        match self {
            Self::Bool(v) => Self::Bool(pick(v, rows)),
            Self::Int4(v) => Self::Int4(pick(v, rows)),
            Self::Int8(v) => Self::Int8(pick(v, rows)),
            Self::Text(v) => Self::Text(pick(v, rows)),
            Self::Timestamp(v) => Self::Timestamp(pick(v, rows)),
            Self::TimestampTz(v) => Self::TimestampTz(pick(v, rows)),
            Self::Date(v) => Self::Date(pick(v, rows)),
            Self::Uuid(v) => Self::Uuid(pick(v, rows)),
            Self::Bytea(v) => Self::Bytea(pick(v, rows)),
            Self::Period(v) => Self::Period(pick(v, rows)),
            Self::Float8(v) => Self::Float8(pick(v, rows)),
        }
    }

    /// Bridge a storage [`Column`] into the typed, nullable evaluation form,
    /// decoding by the column's `ty`.
    ///
    /// A fixed-width [`Column::I64`] is read as [`LogicalType::Int8`] directly —
    /// the one i64-width type that reaches the evaluator through that physical
    /// shape (the metadata columns). A [`Column::Bytes`] column decodes each
    /// present cell from the canonical [`ScalarValue`] byte layout
    /// ([`ScalarValue::decode`]), and a `None` cell stays NULL — this is the path
    /// every row-codec value column (including the temporal, `uuid`, `bytea`, and
    /// `period` types) arrives on.
    ///
    /// # Errors
    ///
    /// [`ExprError::UnsupportedColumn`] if `ty` is outside the evaluator's scope
    /// (only `float8`) or does not match the column's physical shape, or
    /// [`ExprError::Decode`] if a byte cell is not a valid encoding of `ty`.
    pub fn from_column(ty: LogicalType, column: &Column) -> Result<Self, ExprError> {
        match (ty, column) {
            // The fixed-width column carries `int8` directly.
            (LogicalType::Int8, Column::I64(v)) => {
                Ok(Self::Int8(v.iter().copied().map(Some).collect()))
            }
            // Everything else the evaluator reads is a byte cell decoded by type.
            (LogicalType::Int4, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_int4(&v)).map(Self::Int4)
            }
            (LogicalType::Int8, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_int8(&v)).map(Self::Int8)
            }
            (LogicalType::Bool, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_bool(&v)).map(Self::Bool)
            }
            (LogicalType::Text, Column::Bytes(cells)) => {
                decode_cells(ty, cells, as_text).map(Self::Text)
            }
            (LogicalType::Timestamp, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_int64_payload(&v)).map(Self::Timestamp)
            }
            (LogicalType::TimestampTz, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_int64_payload(&v)).map(Self::TimestampTz)
            }
            (LogicalType::Date, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_date(&v)).map(Self::Date)
            }
            (LogicalType::Uuid, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_uuid(&v)).map(Self::Uuid)
            }
            (LogicalType::Bytea, Column::Bytes(cells)) => {
                decode_cells(ty, cells, as_bytea).map(Self::Bytea)
            }
            (LogicalType::Period, Column::Bytes(cells)) => {
                decode_cells(ty, cells, |v| as_period(&v)).map(Self::Period)
            }
            (_, Column::Bytes(_)) => Err(ExprError::UnsupportedColumn {
                logical: ty,
                physical: "bytes",
            }),
            (_, Column::I64(_)) => Err(ExprError::UnsupportedColumn {
                logical: ty,
                physical: "i64",
            }),
        }
    }
}

/// Why evaluating an [`Expr`] failed.
///
/// Every variant is a *plan* error — a structurally ill-formed expression or a
/// column the evaluator cannot read — not a data-dependent one: NULLs and
/// arithmetic overflow are handled in-band as NULL cells, never as errors. A
/// well-typed expression over supported columns never returns one of these.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExprError {
    /// A column reference points past the batch's columns.
    #[error("expression references column {index} but the batch has {columns} column(s)")]
    ColumnOutOfRange {
        /// The referenced index.
        index: usize,
        /// How many columns the batch actually has.
        columns: usize,
    },

    /// A referenced column has no entry in the filter's column-type schema —
    /// the schema vector is shorter than the position the predicate names.
    /// Distinct from [`ColumnOutOfRange`](Self::ColumnOutOfRange) (the *batch*
    /// lacks the column): here the batch has it but no type was supplied to
    /// decode it.
    #[error("column {index} has no type in the filter schema of {schema_len} column(s)")]
    ColumnTypeMissing {
        /// The referenced index with no schema entry.
        index: usize,
        /// How many type entries the filter schema carries.
        schema_len: usize,
    },

    /// A column's value count disagrees with the batch's row count.
    #[error("column {index} carries {got} value(s) but the batch has {rows} row(s)")]
    LengthMismatch {
        /// The offending column index.
        index: usize,
        /// The batch's row count.
        rows: usize,
        /// The column's actual value count.
        got: usize,
    },

    /// A comparison's operands are different types (`int4` vs `text`, …). The
    /// binder is expected to produce same-typed operands; a mismatch is a plan
    /// error, not a runtime NULL.
    #[error("cannot compare a {left} value with a {right} value")]
    CompareTypeMismatch {
        /// Left operand type.
        left: LogicalType,
        /// Right operand type.
        right: LogicalType,
    },

    /// Arithmetic was applied to a non-integer operand, or to two different
    /// integer widths.
    #[error("arithmetic requires two operands of one integer type, got {left} and {right}")]
    ArithTypeMismatch {
        /// Left operand type.
        left: LogicalType,
        /// Right operand type.
        right: LogicalType,
    },

    /// A boolean connective, `NOT`, or a top-level predicate got a non-boolean
    /// operand.
    #[error("{op} requires a boolean operand, got {found}")]
    NotBoolean {
        /// The operator that wanted a boolean (`AND`, `OR`, `NOT`, `WHERE`).
        op: &'static str,
        /// The type actually supplied.
        found: LogicalType,
    },

    /// A literal of a type the evaluator does not handle (only `float8`, which
    /// exists solely as the `AVG` aggregate result).
    #[error("the vectorized evaluator does not support {0} literals yet")]
    UnsupportedLiteral(LogicalType),

    /// A period predicate ([`Expr::Period`]) got a non-`PERIOD` operand. The
    /// binder is expected to type both sides as `PERIOD`; anything else is a plan
    /// error.
    #[error("a period predicate requires PERIOD operands, got {found}")]
    PeriodOperand {
        /// The non-period operand's type.
        found: LogicalType,
    },

    /// A column whose logical type is out of scope, or does not match its
    /// physical (`i64` / bytes) shape.
    #[error("the vectorized evaluator cannot read a {logical} value from a {physical} column")]
    UnsupportedColumn {
        /// The requested logical type.
        logical: LogicalType,
        /// The column's physical shape (`i64` or `bytes`).
        physical: &'static str,
    },

    /// A byte cell was not a valid encoding of its column's type.
    #[error("decoding a column value: {0}")]
    Decode(#[from] DecodeError),
}

/// Evaluate `expr` over a batch of `columns` (`rows` rows each), producing one
/// result [`Vector`] of `rows` cells.
///
/// The columns are addressed by position — [`Expr::Column(i)`](Expr::Column)
/// reads `columns[i]`. NULL propagates through comparisons and arithmetic
/// (either operand NULL ⇒ NULL), `AND`/`OR` follow the SQL truth tables, and
/// integer overflow yields NULL — see [`Expr`] for the operator set.
///
/// # Errors
///
/// An [`ExprError`] for a structurally invalid expression (out-of-range column,
/// length disagreement, type mismatch, non-boolean connective operand, or an
/// out-of-scope literal/column). Data NULLs and integer overflow are *not*
/// errors — they become NULL result cells.
pub fn eval_expr(expr: &Expr, columns: &[Vector], rows: usize) -> Result<Vector, ExprError> {
    match expr {
        Expr::Column(index) => {
            let col = columns.get(*index).ok_or(ExprError::ColumnOutOfRange {
                index: *index,
                columns: columns.len(),
            })?;
            if col.len() != rows {
                return Err(ExprError::LengthMismatch {
                    index: *index,
                    rows,
                    got: col.len(),
                });
            }
            Ok(col.clone())
        }
        Expr::Literal(value) => broadcast(value, rows),
        Expr::Not(inner) => not(&eval_expr(inner, columns, rows)?),
        Expr::IsNull(inner) => Ok(is_null(&eval_expr(inner, columns, rows)?)),
        Expr::Compare { op, left, right } => {
            let l = eval_expr(left, columns, rows)?;
            let r = eval_expr(right, columns, rows)?;
            compare(*op, &l, &r)
        }
        Expr::Logic { op, left, right } => {
            let l = eval_expr(left, columns, rows)?;
            let r = eval_expr(right, columns, rows)?;
            logic(*op, &l, &r)
        }
        Expr::Arith { op, left, right } => {
            let l = eval_expr(left, columns, rows)?;
            let r = eval_expr(right, columns, rows)?;
            arith(*op, &l, &r)
        }
        Expr::Period { pred, left, right } => {
            let l = eval_expr(left, columns, rows)?;
            let r = eval_expr(right, columns, rows)?;
            period(*pred, &l, &r)
        }
    }
}

/// Broadcast a constant to a `rows`-length [`Vector`] of the matching type.
fn broadcast(value: &ScalarValue, rows: usize) -> Result<Vector, ExprError> {
    Ok(match value {
        ScalarValue::Bool(b) => Vector::Bool(vec![Some(*b); rows]),
        ScalarValue::Int4(v) => Vector::Int4(vec![Some(*v); rows]),
        ScalarValue::Int8(v) => Vector::Int8(vec![Some(*v); rows]),
        ScalarValue::Text(s) => Vector::Text(vec![Some(s.clone()); rows]),
        ScalarValue::Timestamp(v) => Vector::Timestamp(vec![Some(*v); rows]),
        ScalarValue::TimestampTz(v) => Vector::TimestampTz(vec![Some(*v); rows]),
        ScalarValue::Date(v) => Vector::Date(vec![Some(*v); rows]),
        ScalarValue::Uuid(v) => Vector::Uuid(vec![Some(*v); rows]),
        ScalarValue::Bytea(v) => Vector::Bytea(vec![Some(v.clone()); rows]),
        ScalarValue::Period(iv) => Vector::Period(vec![Some(*iv); rows]),
        // `float8` has no column, literal, or arithmetic in the evaluator.
        ScalarValue::Float8(_) => return Err(ExprError::UnsupportedLiteral(value.logical_type())),
    })
}

/// Three-valued comparison: NULL on either side ⇒ NULL, else the boolean.
fn compare(op: CmpOp, left: &Vector, right: &Vector) -> Result<Vector, ExprError> {
    // Same-type operands compare through that type's total order. Arms are
    // grouped by physical cell type — the `i64` (int8 + the temporal instants)
    // and `i32` (int4 + date) groups share a kernel binding, so they merge.
    let out = match (left, right) {
        (Vector::Int4(a), Vector::Int4(b)) | (Vector::Date(a), Vector::Date(b)) => {
            compare_cells(op, a, b)
        }
        (Vector::Int8(a), Vector::Int8(b))
        | (Vector::Timestamp(a), Vector::Timestamp(b))
        | (Vector::TimestampTz(a), Vector::TimestampTz(b)) => compare_cells(op, a, b),
        (Vector::Bool(a), Vector::Bool(b)) => compare_cells(op, a, b),
        (Vector::Text(a), Vector::Text(b)) => compare_cells(op, a, b),
        // Byte-ordered: UUID over its 16 network-order bytes, BYTEA lexicographic.
        (Vector::Uuid(a), Vector::Uuid(b)) => compare_cells(op, a, b),
        (Vector::Bytea(a), Vector::Bytea(b)) => compare_cells(op, a, b),
        // Periods order lexicographically by `(from, to)` (`Interval`'s `Ord`).
        (Vector::Period(a), Vector::Period(b)) => compare_cells(op, a, b),
        _ => {
            return Err(ExprError::CompareTypeMismatch {
                left: left.logical_type(),
                right: right.logical_type(),
            });
        }
    };
    Ok(Vector::Bool(out))
}

/// The per-cell comparison kernel, generic over any totally ordered cell type.
fn compare_cells<T: Ord>(op: CmpOp, a: &[Option<T>], b: &[Option<T>]) -> Vec<Option<bool>> {
    a.iter()
        .zip(b)
        .map(|(x, y)| match (x, y) {
            (Some(x), Some(y)) => Some(apply_cmp(op, x.cmp(y))),
            _ => None,
        })
        .collect()
}

/// Turn an [`Ordering`](std::cmp::Ordering) into the boolean a [`CmpOp`] reports.
const fn apply_cmp(op: CmpOp, ord: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match op {
        CmpOp::Eq => matches!(ord, Equal),
        CmpOp::Ne => !matches!(ord, Equal),
        CmpOp::Lt => matches!(ord, Less),
        CmpOp::Le => !matches!(ord, Greater),
        CmpOp::Gt => matches!(ord, Greater),
        CmpOp::Ge => !matches!(ord, Less),
    }
}

/// Three-valued SQL:2011 period predicate over two PERIOD vectors: NULL on
/// either side ⇒ NULL, else the boolean [`evaluate`] gives for the pair.
fn period(pred: PeriodPredicate, left: &Vector, right: &Vector) -> Result<Vector, ExprError> {
    let (Vector::Period(a), Vector::Period(b)) = (left, right) else {
        let found = if matches!(left, Vector::Period(_)) {
            right
        } else {
            left
        };
        return Err(ExprError::PeriodOperand {
            found: found.logical_type(),
        });
    };
    let out = a
        .iter()
        .zip(b)
        .map(|(x, y)| match (x, y) {
            (Some(a), Some(b)) => Some(evaluate(pred, *a, *b)),
            _ => None,
        })
        .collect();
    Ok(Vector::Bool(out))
}

/// Three-valued `AND` / `OR` over two boolean vectors.
fn logic(op: LogicOp, left: &Vector, right: &Vector) -> Result<Vector, ExprError> {
    let (Vector::Bool(a), Vector::Bool(b)) = (left, right) else {
        let found = if matches!(left, Vector::Bool(_)) {
            right
        } else {
            left
        };
        return Err(ExprError::NotBoolean {
            op: match op {
                LogicOp::And => "AND",
                LogicOp::Or => "OR",
            },
            found: found.logical_type(),
        });
    };
    let out = a
        .iter()
        .zip(b)
        .map(|(x, y)| match op {
            LogicOp::And => and3(*x, *y),
            LogicOp::Or => or3(*x, *y),
        })
        .collect();
    Ok(Vector::Bool(out))
}

/// `AND` truth table (a determining `false` wins over an unknown).
const fn and3(x: Option<bool>, y: Option<bool>) -> Option<bool> {
    match (x, y) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// `OR` truth table (a determining `true` wins over an unknown).
const fn or3(x: Option<bool>, y: Option<bool>) -> Option<bool> {
    match (x, y) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// Three-valued `NOT`: flips T↔F, leaves NULL unchanged.
fn not(operand: &Vector) -> Result<Vector, ExprError> {
    let Vector::Bool(cells) = operand else {
        return Err(ExprError::NotBoolean {
            op: "NOT",
            found: operand.logical_type(),
        });
    };
    Ok(Vector::Bool(
        cells.iter().map(|cell| cell.map(|b| !b)).collect(),
    ))
}

/// `IS NULL`: a non-null boolean per cell, regardless of the operand's type.
fn is_null(operand: &Vector) -> Vector {
    // Every variant reports the same per-cell `is_none()` mask; arms are grouped
    // by physical cell type so the identical bodies merge.
    let mask = match operand {
        Vector::Bool(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Int4(v) | Vector::Date(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Int8(v) | Vector::Timestamp(v) | Vector::TimestampTz(v) => {
            v.iter().map(|c| Some(c.is_none())).collect()
        }
        Vector::Text(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Uuid(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Bytea(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Period(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Float8(v) => v.iter().map(|c| Some(c.is_none())).collect(),
    };
    Vector::Bool(mask)
}

/// Three-valued integer arithmetic: NULL on either side ⇒ NULL; an overflowing
/// op ⇒ NULL.
fn arith(op: ArithOp, left: &Vector, right: &Vector) -> Result<Vector, ExprError> {
    match (left, right) {
        (Vector::Int4(a), Vector::Int4(b)) => {
            Ok(Vector::Int4(arith_cells(a, b, |x, y| arith_i32(op, x, y))))
        }
        (Vector::Int8(a), Vector::Int8(b)) => {
            Ok(Vector::Int8(arith_cells(a, b, |x, y| arith_i64(op, x, y))))
        }
        _ => Err(ExprError::ArithTypeMismatch {
            left: left.logical_type(),
            right: right.logical_type(),
        }),
    }
}

/// The per-cell arithmetic kernel: NULL operand ⇒ NULL; `op` returns `None` on
/// overflow, which becomes a NULL cell.
fn arith_cells<T: Copy>(
    a: &[Option<T>],
    b: &[Option<T>],
    op: impl Fn(T, T) -> Option<T>,
) -> Vec<Option<T>> {
    a.iter()
        .zip(b)
        .map(|(x, y)| match (x, y) {
            (Some(x), Some(y)) => op(*x, *y),
            _ => None,
        })
        .collect()
}

/// Checked `i32` arithmetic; `None` on overflow *and* on divide-by-zero
/// (`checked_div`/`checked_rem` return `None` for a zero divisor and for the
/// `MIN / -1` overflow alike), which becomes a NULL cell.
const fn arith_i32(op: ArithOp, a: i32, b: i32) -> Option<i32> {
    match op {
        ArithOp::Add => a.checked_add(b),
        ArithOp::Sub => a.checked_sub(b),
        ArithOp::Mul => a.checked_mul(b),
        ArithOp::Div => a.checked_div(b),
        ArithOp::Mod => a.checked_rem(b),
    }
}

/// Checked `i64` arithmetic; `None` on overflow and on divide-by-zero (see
/// [`arith_i32`]).
const fn arith_i64(op: ArithOp, a: i64, b: i64) -> Option<i64> {
    match op {
        ArithOp::Add => a.checked_add(b),
        ArithOp::Sub => a.checked_sub(b),
        ArithOp::Mul => a.checked_mul(b),
        ArithOp::Div => a.checked_div(b),
        ArithOp::Mod => a.checked_rem(b),
    }
}

/// Decode each present byte cell of a [`Column::Bytes`] column into a typed,
/// nullable vector, mapping a decoded [`ScalarValue`] through `pick`. A `None`
/// cell stays NULL.
fn decode_cells<T>(
    ty: LogicalType,
    cells: &[Option<Vec<u8>>],
    pick: impl Fn(ScalarValue) -> T,
) -> Result<Vec<Option<T>>, ExprError> {
    cells
        .iter()
        .map(|cell| {
            cell.as_ref().map_or(Ok(None), |bytes| {
                ScalarValue::decode(ty, bytes)
                    .map(|value| Some(pick(value)))
                    .map_err(ExprError::from)
            })
        })
        .collect()
}

// `ScalarValue::decode(ty, …)` returns the variant matching `ty`, so these
// extractors only ever see their own arm; the fallback is defensive (it cannot
// fire) and is preferred over an `unreachable!` so a future decode change can
// never turn into a panic.
const fn as_int4(value: &ScalarValue) -> i32 {
    match value {
        ScalarValue::Int4(v) => *v,
        _ => 0,
    }
}
const fn as_int8(value: &ScalarValue) -> i64 {
    match value {
        ScalarValue::Int8(v) => *v,
        _ => 0,
    }
}
const fn as_bool(value: &ScalarValue) -> bool {
    matches!(value, ScalarValue::Bool(true))
}
fn as_text(value: ScalarValue) -> String {
    match value {
        ScalarValue::Text(s) => s,
        _ => String::new(),
    }
}
// `Timestamp` and `TimestampTz` share an `i64` payload — the column's `ty` (not
// the bytes) chooses the `Vector` variant in `from_column`, so one extractor
// serves both.
const fn as_int64_payload(value: &ScalarValue) -> i64 {
    match value {
        ScalarValue::Timestamp(v) | ScalarValue::TimestampTz(v) => *v,
        _ => 0,
    }
}
const fn as_date(value: &ScalarValue) -> i32 {
    match value {
        ScalarValue::Date(v) => *v,
        _ => 0,
    }
}
const fn as_uuid(value: &ScalarValue) -> [u8; 16] {
    match value {
        ScalarValue::Uuid(bytes) => *bytes,
        _ => [0; 16],
    }
}
fn as_bytea(value: ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Bytea(bytes) => bytes,
        _ => Vec::new(),
    }
}
const fn as_period(value: &ScalarValue) -> Interval {
    match value {
        ScalarValue::Period(iv) => *iv,
        // Defensive only (decode returns the variant matching `ty`); use a
        // well-formed `from < to` sentinel so the `Interval` invariant holds even
        // if this ever fires.
        _ => Interval { from: 0, to: 1 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `c0 = c1` over two int4 columns, exercising NULL propagation on both sides.
    #[test]
    fn comparison_is_strict_in_nulls() {
        let cols = vec![
            Vector::Int4(vec![Some(1), Some(2), None, Some(4)]),
            Vector::Int4(vec![Some(1), Some(3), Some(3), None]),
        ];
        let expr = Expr::col(0).compare(CmpOp::Eq, Expr::col(1));
        let out = eval_expr(&expr, &cols, 4).expect("eval");
        // 1=1 → T; 2=3 → F; NULL=3 → NULL; 4=NULL → NULL.
        assert_eq!(out, Vector::Bool(vec![Some(true), Some(false), None, None]));
    }

    #[test]
    fn all_six_comparisons_on_one_pair() {
        let cols = vec![Vector::Int8(vec![Some(5)]), Vector::Int8(vec![Some(7)])];
        let cases = [
            (CmpOp::Eq, false),
            (CmpOp::Ne, true),
            (CmpOp::Lt, true),
            (CmpOp::Le, true),
            (CmpOp::Gt, false),
            (CmpOp::Ge, false),
        ];
        for (op, want) in cases {
            let expr = Expr::col(0).compare(op, Expr::col(1));
            assert_eq!(
                eval_expr(&expr, &cols, 1).expect("eval"),
                Vector::Bool(vec![Some(want)]),
                "{op:?}"
            );
        }
    }

    /// The full `AND` / `OR` truth tables over the three values, NULL included.
    #[test]
    fn and_or_truth_tables() {
        let vals = [Some(true), Some(false), None];
        let left = Vector::Bool(vals.iter().flat_map(|&x| [x, x, x]).collect::<Vec<_>>());
        let right = Vector::Bool(vals.iter().cycle().take(9).copied().collect());
        let cols = vec![left, right];

        let and =
            eval_expr(&Expr::col(0).logic(LogicOp::And, Expr::col(1)), &cols, 9).expect("eval and");
        // rows: (T,T)(T,F)(T,N) (F,T)(F,F)(F,N) (N,T)(N,F)(N,N)
        assert_eq!(
            and,
            Vector::Bool(vec![
                Some(true),
                Some(false),
                None,
                Some(false),
                Some(false),
                Some(false),
                None,
                Some(false),
                None,
            ])
        );

        let or =
            eval_expr(&Expr::col(0).logic(LogicOp::Or, Expr::col(1)), &cols, 9).expect("eval or");
        assert_eq!(
            or,
            Vector::Bool(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
                None,
            ])
        );
    }

    #[test]
    fn not_flips_known_and_preserves_null() {
        let cols = vec![Vector::Bool(vec![Some(true), Some(false), None])];
        let out = eval_expr(&Expr::col(0).negate(), &cols, 3).expect("eval");
        assert_eq!(out, Vector::Bool(vec![Some(false), Some(true), None]));
    }

    #[test]
    fn is_null_is_two_valued() {
        let cols = vec![Vector::Text(vec![Some("x".to_owned()), None])];
        let out = eval_expr(&Expr::col(0).is_null(), &cols, 2).expect("eval");
        assert_eq!(out, Vector::Bool(vec![Some(false), Some(true)]));
    }

    #[test]
    fn arithmetic_is_strict_and_overflow_is_null() {
        let cols = vec![
            Vector::Int4(vec![Some(2), Some(i32::MAX), None]),
            Vector::Int4(vec![Some(3), Some(1), Some(9)]),
        ];
        let out =
            eval_expr(&Expr::col(0).arith(ArithOp::Add, Expr::col(1)), &cols, 3).expect("eval");
        // 2+3 → 5; MAX+1 → overflow → NULL; NULL+9 → NULL.
        assert_eq!(out, Vector::Int4(vec![Some(5), None, None]));
    }

    #[test]
    fn nested_expression_composes() {
        // (c0 + 1) > c1  AND  c2 IS NULL
        let cols = vec![
            Vector::Int8(vec![Some(10), Some(1), Some(5)]),
            Vector::Int8(vec![Some(10), Some(5), Some(5)]),
            Vector::Text(vec![None, Some("x".to_owned()), None]),
        ];
        let gt = Expr::col(0)
            .arith(ArithOp::Add, Expr::lit(ScalarValue::Int8(1)))
            .compare(CmpOp::Gt, Expr::col(1));
        let expr = gt.logic(LogicOp::And, Expr::col(2).is_null());
        let out = eval_expr(&expr, &cols, 3).expect("eval");
        // row0: 11>10 (T) AND null→T  = T
        // row1:  2>5  (F) AND ...      = F   (false short-circuits)
        // row2:  6>5  (T) AND null→T  = T
        assert_eq!(out, Vector::Bool(vec![Some(true), Some(false), Some(true)]));
    }

    #[test]
    fn type_mismatch_in_comparison_is_a_plan_error() {
        let cols = vec![
            Vector::Int4(vec![Some(1)]),
            Vector::Text(vec![Some("1".to_owned())]),
        ];
        let expr = Expr::col(0).compare(CmpOp::Eq, Expr::col(1));
        assert_eq!(
            eval_expr(&expr, &cols, 1),
            Err(ExprError::CompareTypeMismatch {
                left: LogicalType::Int4,
                right: LogicalType::Text,
            })
        );
    }

    #[test]
    fn non_boolean_connective_operand_is_rejected() {
        let cols = vec![Vector::Int4(vec![Some(1)])];
        let expr = Expr::col(0).logic(LogicOp::And, Expr::lit(ScalarValue::Bool(true)));
        assert_eq!(
            eval_expr(&expr, &cols, 1),
            Err(ExprError::NotBoolean {
                op: "AND",
                found: LogicalType::Int4,
            })
        );
    }

    #[test]
    fn out_of_range_column_and_length_mismatch_are_errors() {
        let cols = vec![Vector::Int4(vec![Some(1), Some(2)])];
        assert_eq!(
            eval_expr(&Expr::col(3), &cols, 2),
            Err(ExprError::ColumnOutOfRange {
                index: 3,
                columns: 1
            })
        );
        assert_eq!(
            eval_expr(&Expr::col(0), &cols, 5),
            Err(ExprError::LengthMismatch {
                index: 0,
                rows: 5,
                got: 2
            })
        );
    }

    #[test]
    fn unsupported_literal_is_rejected() {
        // `float8` is the one type the evaluator still rejects as a literal — it
        // exists only as the `AVG` aggregate result.
        let cols: Vec<Vector> = vec![];
        let expr = Expr::lit(ScalarValue::float8(1.5));
        assert_eq!(
            eval_expr(&expr, &cols, 0),
            Err(ExprError::UnsupportedLiteral(LogicalType::Float8))
        );
    }

    /// A byte column round-trips into a [`Vector`] through the canonical
    /// [`ScalarValue`] encoding — pinning [`Vector::from_column`] to the same
    /// layout the storage codec writes, so the two cannot drift.
    #[test]
    fn from_column_matches_scalar_encoding() {
        let values = [
            ScalarValue::Int4(7),
            ScalarValue::Int4(-1),
            ScalarValue::Text("hi".to_owned()),
            ScalarValue::Bool(true),
            ScalarValue::Timestamp(1_700_000_000_000_000),
            ScalarValue::TimestampTz(-1),
            ScalarValue::Date(20_000),
            ScalarValue::Uuid([
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ]),
            ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ScalarValue::Bytea(Vec::new()),
            ScalarValue::Period(Interval::new(10, 20).unwrap()),
            ScalarValue::Period(Interval::new(1_700_000_000_000_000, i64::MAX).unwrap()),
        ];
        for value in values {
            let mut buf = Vec::new();
            value.encode(&mut buf);
            let col = Column::Bytes(vec![Some(buf), None]);
            let vector = Vector::from_column(value.logical_type(), &col).expect("bridge");
            assert_eq!(vector.get(0), Some(value.clone()), "present cell");
            assert_eq!(vector.get(1), None, "null cell");
        }
    }

    #[test]
    fn from_column_reads_i64_fixed_width_directly() {
        let col = Column::I64(vec![1, -2, 3]);
        assert_eq!(
            Vector::from_column(LogicalType::Int8, &col).expect("bridge"),
            Vector::Int8(vec![Some(1), Some(-2), Some(3)])
        );
    }

    #[test]
    fn from_column_rejects_out_of_scope_type() {
        // `float8` is the only logical type still outside the evaluator's scope.
        let col = Column::Bytes(vec![Some(vec![0; 8])]);
        assert_eq!(
            Vector::from_column(LogicalType::Float8, &col),
            Err(ExprError::UnsupportedColumn {
                logical: LogicalType::Float8,
                physical: "bytes",
            })
        );
    }

    /// `/` and `%` with a NULL result for a zero divisor — the documented
    /// divide-by-zero semantics, plus the `MIN / -1` overflow that also NULLs.
    #[test]
    fn division_and_modulo_null_on_zero_divisor_and_overflow() {
        let cols = vec![
            Vector::Int4(vec![Some(7), Some(7), Some(-7), Some(i32::MIN), None]),
            Vector::Int4(vec![Some(2), Some(0), Some(2), Some(-1), Some(3)]),
        ];
        let div =
            eval_expr(&Expr::col(0).arith(ArithOp::Div, Expr::col(1)), &cols, 5).expect("div");
        // 7/2 → 3 (trunc); 7/0 → NULL; -7/2 → -3; MIN/-1 → overflow → NULL; NULL/3 → NULL.
        assert_eq!(div, Vector::Int4(vec![Some(3), None, Some(-3), None, None]));
        let rem =
            eval_expr(&Expr::col(0).arith(ArithOp::Mod, Expr::col(1)), &cols, 5).expect("mod");
        // 7%2 → 1; 7%0 → NULL; -7%2 → -1 (sign of dividend); MIN%-1 → NULL; NULL%3 → NULL.
        assert_eq!(rem, Vector::Int4(vec![Some(1), None, Some(-1), None, None]));
    }

    /// The new scalar types each compare through their natural total order, with
    /// NULL propagating, and a cross-type comparison stays a plan error.
    #[test]
    fn new_types_compare_in_their_total_order() {
        // Byte-ordered UUID: 0x00.. < 0x01.. , and a NULL cell yields NULL.
        let lo = [0u8; 16];
        let mut hi = [0u8; 16];
        hi[0] = 1;
        let cols = vec![
            Vector::Uuid(vec![Some(lo), Some(hi), None]),
            Vector::Uuid(vec![Some(hi), Some(hi), Some(lo)]),
        ];
        let lt = eval_expr(&Expr::col(0).compare(CmpOp::Lt, Expr::col(1)), &cols, 3).expect("lt");
        assert_eq!(lt, Vector::Bool(vec![Some(true), Some(false), None]));

        // Timestamp orders by its i64 instant.
        let ts = vec![
            Vector::Timestamp(vec![Some(10), Some(20)]),
            Vector::Timestamp(vec![Some(20), Some(20)]),
        ];
        let le = eval_expr(&Expr::col(0).compare(CmpOp::Le, Expr::col(1)), &ts, 2).expect("le");
        assert_eq!(le, Vector::Bool(vec![Some(true), Some(true)]));

        // Timestamp vs TimestampTz are distinct types — comparing them is a plan
        // error, not a silent same-instant match.
        let mixed = vec![
            Vector::Timestamp(vec![Some(1)]),
            Vector::TimestampTz(vec![Some(1)]),
        ];
        assert_eq!(
            eval_expr(&Expr::col(0).compare(CmpOp::Eq, Expr::col(1)), &mixed, 1),
            Err(ExprError::CompareTypeMismatch {
                left: LogicalType::Timestamp,
                right: LogicalType::TimestampTz,
            })
        );
    }

    /// A period predicate over two PERIOD vectors evaluates per row with NULL
    /// propagation, matching [`crate::period::evaluate`].
    #[test]
    fn period_predicate_evaluates_per_row() {
        let iv = |from, to| Interval::new(from, to).expect("interval");
        let cols = vec![
            Vector::Period(vec![Some(iv(10, 40)), Some(iv(10, 20)), None]),
            Vector::Period(vec![Some(iv(20, 30)), Some(iv(20, 30)), Some(iv(0, 5))]),
        ];
        // [10,40) CONTAINS [20,30) → T; [10,20) CONTAINS [20,30) → F; NULL → NULL.
        let contains = eval_expr(
            &Expr::col(0).period(PeriodPredicate::Contains, Expr::col(1)),
            &cols,
            3,
        )
        .expect("contains");
        assert_eq!(contains, Vector::Bool(vec![Some(true), Some(false), None]));

        // A non-PERIOD operand is a plan error.
        let bad = vec![
            Vector::Period(vec![Some(iv(1, 2))]),
            Vector::Int4(vec![Some(1)]),
        ];
        assert_eq!(
            eval_expr(
                &Expr::col(0).period(PeriodPredicate::Overlaps, Expr::col(1)),
                &bad,
                1
            ),
            Err(ExprError::PeriodOperand {
                found: LogicalType::Int4,
            })
        );
    }
}
