//! Folding a SQL literal expression into a typed [`ScalarValue`] of a known
//! column type.
//!
//! Shared by the DML binder ([`bind_dml`](crate::bind_dml) — key / value
//! literals) and the SELECT binder ([`bind_select`](crate::bind_select) — a
//! `WHERE <col> = <literal>` comparand). Both need the same thing: take a parsed
//! literal and a column's [`LogicalType`], and produce the value or a precise
//! reason it cannot. The reason is reported here as a table/column-agnostic
//! [`FoldError`]; each binder maps it to its own error type with the names it
//! knows, so the two surfaces stay consistent without sharing an error enum.

use sqlparser::ast::{Expr, UnaryOperator, Value};
use stele_common::types::{LogicalType, ScalarValue};

/// Why folding a literal to a typed value failed — without table/column context,
/// which the calling binder adds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FoldError {
    /// The literal was SQL `NULL`. Whether that is an error is the caller's call:
    /// a value column accepts it (folds to `None`), a business key or a `WHERE`
    /// comparand does not.
    Null,
    /// The literal's shape does not match the column's type (e.g. a string for an
    /// `int4` column). Carries a short description of what was actually given.
    TypeMismatch {
        /// A short label for the offending literal shape (see [`describe`]).
        found: &'static str,
    },
    /// The literal is the right shape for the type but cannot be represented
    /// (out of range, or not an integer). Carries the offending literal text.
    BadLiteral {
        /// The literal text that could not be represented.
        literal: String,
    },
    /// The column's type has no literal codec at v0.2 (`TIMESTAMP` / `DATE` —
    /// no civil-time literal parsing yet; mirrors the `AS OF` stance).
    UnsupportedType(LogicalType),
}

/// Fold `expr` into a [`ScalarValue`] of `ty`, or report why it cannot.
///
/// Rejects `NULL` ([`FoldError::Null`]) — the caller decides whether that is
/// fatal — along with type mismatches, out-of-range literals, and the
/// not-yet-supported civil-time types.
pub(crate) fn fold_scalar(expr: &Expr, ty: LogicalType) -> Result<ScalarValue, FoldError> {
    if is_null(expr) {
        return Err(FoldError::Null);
    }
    match ty {
        LogicalType::Int4 => {
            let digits = signed_number(expr).ok_or(FoldError::TypeMismatch {
                found: describe(expr),
            })?;
            digits
                .parse::<i32>()
                .map(ScalarValue::Int4)
                .map_err(|_| FoldError::BadLiteral { literal: digits })
        }
        LogicalType::Int8 => {
            let digits = signed_number(expr).ok_or(FoldError::TypeMismatch {
                found: describe(expr),
            })?;
            digits
                .parse::<i64>()
                .map(ScalarValue::Int8)
                .map_err(|_| FoldError::BadLiteral { literal: digits })
        }
        LogicalType::Text => match literal(expr) {
            Some(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
            _ => Err(FoldError::TypeMismatch {
                found: describe(expr),
            }),
        },
        LogicalType::Bool => match literal(expr) {
            Some(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
            _ => Err(FoldError::TypeMismatch {
                found: describe(expr),
            }),
        },
        // No civil-time or period literal codec at v0.2 (mirrors AS OF); a
        // TIMESTAMP/DATE/PERIOD column cannot be written or compared against a
        // literal yet. (Period predicates build their intervals from PERIOD(a,b)
        // endpoints, not from a folded period scalar — see stele-exec.)
        ty @ (LogicalType::Timestamp | LogicalType::Date | LogicalType::Period) => {
            Err(FoldError::UnsupportedType(ty))
        }
    }
}

/// Whether an expression is the `NULL` literal.
pub(crate) fn is_null(expr: &Expr) -> bool {
    matches!(literal(expr), Some(Value::Null))
}

/// The literal [`Value`] an expression carries, peeling parentheses; `None` if it
/// is not a bare literal.
pub(crate) fn literal(expr: &Expr) -> Option<&Value> {
    match expr {
        Expr::Value(v) => Some(&v.value),
        Expr::Nested(inner) => literal(inner),
        _ => None,
    }
}

/// The (possibly signed) decimal digits of a numeric literal, folding a leading
/// unary `+` / `-` into the string so it parses directly. `None` for any
/// non-numeric expression.
pub(crate) fn signed_number(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        Expr::Nested(inner) => signed_number(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => signed_number(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => signed_number(expr).map(|s| {
            s.strip_prefix('-')
                .map_or_else(|| format!("-{s}"), ToOwned::to_owned)
        }),
        _ => None,
    }
}

/// A short label for an expression, for the type-mismatch diagnostics.
pub(crate) fn describe(expr: &Expr) -> &'static str {
    match literal(expr) {
        Some(Value::SingleQuotedString(_)) => "a string literal",
        Some(Value::Boolean(_)) => "a boolean literal",
        Some(Value::Number(..)) => "a numeric literal",
        Some(Value::Null) => "NULL",
        _ => "a non-literal expression",
    }
}
