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

use crate::types::logical_type;

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
    /// (out of range, not an integer, or a malformed civil-time literal). Carries
    /// the offending literal text and, where the codec produced one, a short
    /// reason (e.g. `"month out of range"`).
    BadLiteral {
        /// The literal text that could not be represented.
        literal: String,
        /// A short, stable explanation from the type's codec, when it has one.
        reason: Option<&'static str>,
    },
    /// The column's type has no literal codec: `PERIOD` (period predicates build
    /// intervals from `PERIOD(a,b)` endpoints, not a folded scalar) and `FLOAT8`
    /// (an aggregate result type only — there is no float8 column to fold into).
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
                .map_err(|_| FoldError::BadLiteral {
                    literal: digits,
                    reason: None,
                })
        }
        LogicalType::Int8 => {
            let digits = signed_number(expr).ok_or(FoldError::TypeMismatch {
                found: describe(expr),
            })?;
            digits
                .parse::<i64>()
                .map(ScalarValue::Int8)
                .map_err(|_| FoldError::BadLiteral {
                    literal: digits,
                    reason: None,
                })
        }
        LogicalType::Text => string_literal(expr, ty).map_or_else(
            || {
                Err(FoldError::TypeMismatch {
                    found: describe(expr),
                })
            },
            |s| Ok(ScalarValue::Text(s.clone())),
        ),
        LogicalType::Bool => match literal(expr) {
            Some(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
            _ => Err(FoldError::TypeMismatch {
                found: describe(expr),
            }),
        },
        // UUID and BYTEA take their value from a single-quoted string literal,
        // the way a Postgres client writes them (`'550e…'`, `'\xDEADBEEF'`).
        LogicalType::Uuid => {
            let s = string_literal(expr, ty).ok_or_else(|| FoldError::TypeMismatch {
                found: describe(expr),
            })?;
            parse_uuid(s)
                .map(ScalarValue::Uuid)
                .ok_or_else(|| FoldError::BadLiteral {
                    literal: s.clone(),
                    reason: Some("expected a UUID: 32 hex digits, optionally hyphenated"),
                })
        }
        LogicalType::Bytea => {
            let s = string_literal(expr, ty).ok_or_else(|| FoldError::TypeMismatch {
                found: describe(expr),
            })?;
            parse_bytea(s)
                .map(ScalarValue::Bytea)
                .ok_or_else(|| FoldError::BadLiteral {
                    literal: s.clone(),
                    reason: Some("expected bytea hex: `\\x` then an even number of hex digits"),
                })
        }
        // The three civil-time literal codecs ([`stele_common::datetime`]):
        // `timestamptz` normalizes its zone offset to the engine's UTC
        // microsecond scale (STL-189); the zone-less `timestamp` shares the
        // grammar but rejects an explicit offset; `date` is the pure
        // `YYYY-MM-DD` day count.
        LogicalType::TimestampTz => fold_civil(expr, ty, |s| {
            stele_common::datetime::parse_timestamptz(s).map(ScalarValue::TimestampTz)
        }),
        LogicalType::Timestamp => fold_civil(expr, ty, |s| {
            stele_common::datetime::parse_timestamp(s).map(ScalarValue::Timestamp)
        }),
        LogicalType::Date => fold_civil(expr, ty, |s| {
            stele_common::datetime::parse_date(s).map(ScalarValue::Date)
        }),
        // No literal codec for PERIOD (predicates build their intervals from
        // PERIOD(a,b) endpoints, not from a folded period scalar — see
        // stele-exec) or FLOAT8 (an aggregate result type only, [STL-209] —
        // there is no `float8` column or literal to fold into, STL-207).
        ty @ (LogicalType::Period | LogicalType::Float8) => Err(FoldError::UnsupportedType(ty)),
    }
}

/// Fold a civil-time literal through one of the [`stele_common::datetime`]
/// codecs, mapping its parse error onto [`FoldError::BadLiteral`].
fn fold_civil(
    expr: &Expr,
    ty: LogicalType,
    parse: impl Fn(&str) -> Result<ScalarValue, stele_common::datetime::DatetimeParseError>,
) -> Result<ScalarValue, FoldError> {
    let s = string_literal(expr, ty).ok_or_else(|| FoldError::TypeMismatch {
        found: describe(expr),
    })?;
    parse(s).map_err(|e| FoldError::BadLiteral {
        literal: e.literal,
        reason: Some(e.reason),
    })
}

/// The text a string-driven literal carries for a column of type `ty`: a bare
/// single-quoted string (`'…'`, peeling parentheses), or a *typed* string whose
/// declared type lowers to the same `ty` — so `TIMESTAMP '2024-01-15 12:00'`
/// and `UUID '550e…'` fold for matching columns, the way Postgres clients
/// write them. A typed string of a *different* type is `None` (a type
/// mismatch, never an implicit cast).
fn string_literal(expr: &Expr, ty: LogicalType) -> Option<&String> {
    match expr {
        Expr::Nested(inner) => string_literal(inner, ty),
        Expr::TypedString(typed) => match (logical_type(&typed.data_type), &typed.value.value) {
            (Ok(declared), Value::SingleQuotedString(s)) if declared == ty => Some(s),
            _ => None,
        },
        _ => match literal(expr) {
            Some(Value::SingleQuotedString(s)) => Some(s),
            _ => None,
        },
    }
}

/// The value of a single hex digit (`0-9`, `a-f`, `A-F`), or `None`.
fn hex_val(c: char) -> Option<u8> {
    c.to_digit(16).and_then(|d| u8::try_from(d).ok())
}

/// Parse a UUID's textual form into its 16 raw bytes, accepting the canonical
/// hyphenated rendering (`550e8400-e29b-41d4-a716-446655440000`) and the bare
/// 32-hex-digit form, case-insensitively — the inverse of the wire text encoder.
/// Hyphens may appear anywhere and are ignored; the digits must total exactly 32.
/// `None` for anything else.
fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let mut bytes = [0u8; 16];
    let mut nibbles = 0usize;
    for c in text.chars() {
        if c == '-' {
            continue;
        }
        let v = hex_val(c)?;
        if nibbles >= 32 {
            return None; // too many digits
        }
        let byte = &mut bytes[nibbles / 2];
        *byte = (*byte << 4) | v;
        nibbles += 1;
    }
    (nibbles == 32).then_some(bytes)
}

/// Parse Postgres `bytea` hex input (`\x` followed by an even number of hex
/// digits, case-insensitive) into the raw bytes — the inverse of the wire text
/// encoder. The historical escape format is intentionally not accepted. `None`
/// for any other shape.
fn parse_bytea(text: &str) -> Option<Vec<u8>> {
    let hex = text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.chars();
    while let Some(hi) = chars.next() {
        let lo = chars.next()?;
        out.push((hex_val(hi)? << 4) | hex_val(lo)?);
    }
    Some(out)
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
    if matches!(expr, Expr::TypedString(_)) {
        // A typed string of the *wrong* type lands here — `string_literal`
        // already matched the right-typed ones.
        return "a typed literal of a different type";
    }
    match literal(expr) {
        Some(Value::SingleQuotedString(_)) => "a string literal",
        Some(Value::Boolean(_)) => "a boolean literal",
        Some(Value::Number(..)) => "a numeric literal",
        Some(Value::Null) => "NULL",
        _ => "a non-literal expression",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single-quoted string-literal expression, the way the parser yields one.
    fn str_lit(s: &str) -> Expr {
        Expr::Value(Value::SingleQuotedString(s.to_owned()).into())
    }

    const SAMPLE: [u8; 16] = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];

    #[test]
    fn folds_uuid_from_hyphenated_and_bare_forms() {
        assert_eq!(
            fold_scalar(
                &str_lit("550e8400-e29b-41d4-a716-446655440000"),
                LogicalType::Uuid
            ),
            Ok(ScalarValue::Uuid(SAMPLE))
        );
        // Bare 32-hex and uppercase both accepted; case is normalized away.
        assert_eq!(
            fold_scalar(
                &str_lit("550E8400E29B41D4A716446655440000"),
                LogicalType::Uuid
            ),
            Ok(ScalarValue::Uuid(SAMPLE))
        );
    }

    #[test]
    fn rejects_malformed_uuid_and_non_string() {
        // Wrong digit count and a non-hex char are bad literals.
        assert!(matches!(
            fold_scalar(&str_lit("550e8400"), LogicalType::Uuid),
            Err(FoldError::BadLiteral { .. })
        ));
        assert!(matches!(
            fold_scalar(
                &str_lit("zzze8400-e29b-41d4-a716-446655440000"),
                LogicalType::Uuid
            ),
            Err(FoldError::BadLiteral { .. })
        ));
        // A numeric literal for a UUID column is a type mismatch, not a bad value.
        let num = Expr::Value(Value::Number("1".to_owned(), false).into());
        assert!(matches!(
            fold_scalar(&num, LogicalType::Uuid),
            Err(FoldError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn folds_bytea_from_hex_input() {
        assert_eq!(
            fold_scalar(&str_lit("\\xdeadbeef"), LogicalType::Bytea),
            Ok(ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        );
        // Empty payload and uppercase prefix/digits.
        assert_eq!(
            fold_scalar(&str_lit("\\x"), LogicalType::Bytea),
            Ok(ScalarValue::Bytea(vec![]))
        );
        assert_eq!(
            fold_scalar(&str_lit("\\XDEAD"), LogicalType::Bytea),
            Ok(ScalarValue::Bytea(vec![0xDE, 0xAD]))
        );
    }

    #[test]
    fn rejects_malformed_bytea() {
        // Missing `\x` prefix, odd digit count, and a non-hex digit all fail.
        for bad in ["deadbeef", "\\xabc", "\\xzz"] {
            assert!(
                matches!(
                    fold_scalar(&str_lit(bad), LogicalType::Bytea),
                    Err(FoldError::BadLiteral { .. })
                ),
                "expected {bad:?} to be a bad bytea literal"
            );
        }
    }

    /// A typed-string expression (`TIMESTAMP '…'`), the way the parser yields one.
    fn typed_lit(sql_type: sqlparser::ast::DataType, s: &str) -> Expr {
        Expr::TypedString(sqlparser::ast::TypedString {
            data_type: sql_type,
            value: Value::SingleQuotedString(s.to_owned()).into(),
            uses_odbc_syntax: false,
        })
    }

    #[test]
    fn folds_civil_time_literals_from_plain_strings() {
        // 1_700_000_000 s = 2023-11-14 22:13:20 UTC; day 19_675.
        assert_eq!(
            fold_scalar(&str_lit("2023-11-14 22:13:20"), LogicalType::Timestamp),
            Ok(ScalarValue::Timestamp(1_700_000_000_000_000))
        );
        assert_eq!(
            fold_scalar(&str_lit("2023-11-14 22:13:20Z"), LogicalType::TimestampTz),
            Ok(ScalarValue::TimestampTz(1_700_000_000_000_000))
        );
        assert_eq!(
            fold_scalar(&str_lit("2023-11-14"), LogicalType::Date),
            Ok(ScalarValue::Date(19_675))
        );
    }

    #[test]
    fn folds_typed_string_literals_of_the_matching_type() {
        use sqlparser::ast::{DataType, TimezoneInfo};
        assert_eq!(
            fold_scalar(
                &typed_lit(
                    DataType::Timestamp(None, TimezoneInfo::None),
                    "2023-11-14 22:13:20"
                ),
                LogicalType::Timestamp
            ),
            Ok(ScalarValue::Timestamp(1_700_000_000_000_000))
        );
        assert_eq!(
            fold_scalar(&typed_lit(DataType::Date, "2023-11-14"), LogicalType::Date),
            Ok(ScalarValue::Date(19_675))
        );
        // A typed string also works for the non-temporal string-driven types.
        assert_eq!(
            fold_scalar(
                &typed_lit(DataType::Uuid, "550e8400-e29b-41d4-a716-446655440000"),
                LogicalType::Uuid
            ),
            Ok(ScalarValue::Uuid(SAMPLE))
        );
        // …but a typed string of a DIFFERENT type is a mismatch, not a cast.
        assert!(matches!(
            fold_scalar(
                &typed_lit(DataType::Date, "2023-11-14"),
                LogicalType::Timestamp
            ),
            Err(FoldError::TypeMismatch {
                found: "a typed literal of a different type"
            })
        ));
    }

    #[test]
    fn zone_on_a_zone_less_timestamp_is_a_bad_literal() {
        // The codec's explicit-rejection stance surfaces as BadLiteral with the
        // use-TIMESTAMPTZ reason, not as a silently shifted instant.
        let err = fold_scalar(&str_lit("2023-11-14 22:13:20+05"), LogicalType::Timestamp);
        assert!(
            matches!(err, Err(FoldError::BadLiteral { reason: Some(r), .. }) if r.contains("TIMESTAMPTZ")),
            "{err:?}"
        );
    }

    /// Folding a UUID/BYTEA literal and re-encoding it returns to the original
    /// text via the wire encoder — the round-trip the DoD rests on, checked at
    /// the SQL-literal boundary.
    #[test]
    fn folded_value_round_trips_through_encode_decode() {
        for value in [
            ScalarValue::Uuid(SAMPLE),
            ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ScalarValue::Bytea(vec![]),
        ] {
            let mut buf = Vec::new();
            value.encode(&mut buf);
            assert_eq!(
                ScalarValue::decode(value.logical_type(), &buf),
                Ok(value.clone())
            );
        }
    }
}
