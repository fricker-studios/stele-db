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
//! The evaluator handles the scalar types a `WHERE` over the v0.2 row codec
//! reaches: [`LogicalType::Int4`], [`LogicalType::Int8`], [`LogicalType::Bool`],
//! and [`LogicalType::Text`]. The remaining logical types (temporal, `PERIOD`,
//! `UUID`, `BYTEA`) and division/modulo are deliberate follow-ups; an expression
//! that reaches one surfaces a typed [`ExprError`] rather than a wrong answer.
//!
//! [STL-170]: https://allegromusic.atlassian.net/browse/STL-170

use stele_common::types::{DecodeError, LogicalType, ScalarValue};

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

/// An integer arithmetic operator. Division and modulo are a follow-up (their
/// divide-by-zero semantics warrant their own treatment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    /// `+` — addition.
    Add,
    /// `-` — subtraction.
    Sub,
    /// `*` — multiplication.
    Mul,
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
}

impl Vector {
    /// The number of cells.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(v) => v.len(),
            Self::Int4(v) => v.len(),
            Self::Int8(v) => v.len(),
            Self::Text(v) => v.len(),
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
        }
    }

    /// Bridge a storage [`Column`] into the typed, nullable evaluation form,
    /// decoding by the column's `ty`.
    ///
    /// A fixed-width [`Column::I64`] carries the i64-width logical types
    /// directly; a [`Column::Bytes`] column decodes each present cell from the
    /// canonical [`ScalarValue`] byte layout
    /// ([`ScalarValue::decode`]) and a `None` cell stays NULL.
    ///
    /// # Errors
    ///
    /// [`ExprError::UnsupportedColumn`] if `ty` is outside the evaluator's scope
    /// or does not match the column's physical shape, or [`ExprError::Decode`] if
    /// a byte cell is not a valid encoding of `ty`.
    pub fn from_column(ty: LogicalType, column: &Column) -> Result<Self, ExprError> {
        match (ty, column) {
            // i64-width logical types live in the fixed-width column directly.
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

    /// A literal of a type the evaluator does not handle yet (temporal, `PERIOD`,
    /// `UUID`, `BYTEA`).
    #[error("the vectorized evaluator does not support {0} literals yet")]
    UnsupportedLiteral(LogicalType),

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
    }
}

/// Broadcast a constant to a `rows`-length [`Vector`] of the matching type.
fn broadcast(value: &ScalarValue, rows: usize) -> Result<Vector, ExprError> {
    Ok(match value {
        ScalarValue::Bool(b) => Vector::Bool(vec![Some(*b); rows]),
        ScalarValue::Int4(v) => Vector::Int4(vec![Some(*v); rows]),
        ScalarValue::Int8(v) => Vector::Int8(vec![Some(*v); rows]),
        ScalarValue::Text(s) => Vector::Text(vec![Some(s.clone()); rows]),
        other => return Err(ExprError::UnsupportedLiteral(other.logical_type())),
    })
}

/// Three-valued comparison: NULL on either side ⇒ NULL, else the boolean.
fn compare(op: CmpOp, left: &Vector, right: &Vector) -> Result<Vector, ExprError> {
    let out = match (left, right) {
        (Vector::Int4(a), Vector::Int4(b)) => compare_cells(op, a, b),
        (Vector::Int8(a), Vector::Int8(b)) => compare_cells(op, a, b),
        (Vector::Bool(a), Vector::Bool(b)) => compare_cells(op, a, b),
        (Vector::Text(a), Vector::Text(b)) => compare_cells(op, a, b),
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
    let mask = match operand {
        Vector::Bool(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Int4(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Int8(v) => v.iter().map(|c| Some(c.is_none())).collect(),
        Vector::Text(v) => v.iter().map(|c| Some(c.is_none())).collect(),
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

/// Checked `i32` arithmetic; `None` on overflow.
const fn arith_i32(op: ArithOp, a: i32, b: i32) -> Option<i32> {
    match op {
        ArithOp::Add => a.checked_add(b),
        ArithOp::Sub => a.checked_sub(b),
        ArithOp::Mul => a.checked_mul(b),
    }
}

/// Checked `i64` arithmetic; `None` on overflow.
const fn arith_i64(op: ArithOp, a: i64, b: i64) -> Option<i64> {
    match op {
        ArithOp::Add => a.checked_add(b),
        ArithOp::Sub => a.checked_sub(b),
        ArithOp::Mul => a.checked_mul(b),
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
        let cols: Vec<Vector> = vec![];
        let expr = Expr::lit(ScalarValue::Uuid([0; 16]));
        assert_eq!(
            eval_expr(&expr, &cols, 0),
            Err(ExprError::UnsupportedLiteral(LogicalType::Uuid))
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
        let col = Column::Bytes(vec![Some(vec![0; 16])]);
        assert_eq!(
            Vector::from_column(LogicalType::Uuid, &col),
            Err(ExprError::UnsupportedColumn {
                logical: LogicalType::Uuid,
                physical: "bytes",
            })
        );
    }
}
