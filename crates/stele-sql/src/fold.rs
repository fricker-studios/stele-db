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
    /// A type with no literal codec on the path that raised this. `PERIOD` has none
    /// on either path — its predicates build intervals from `PERIOD(a,b)` endpoints,
    /// not a folded scalar. `FLOAT8` is path-dependent: the comparand fold takes a
    /// numeric literal ([STL-327]), but the `INSERT` / `COPY` value path still raises
    /// this, since there is no float8 *column* to write into.
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
        // A FLOAT8 comparand folds from a numeric literal — `HAVING AVG(x) > 5`
        // (or `> 2.5`) ([STL-327]). It is the one fold target with no storage
        // column: `AVG` produces it ([STL-209]), and the evaluator compares it by
        // promoting the other numeric operand. The integer literal (`5`) widens to
        // `5.0`, matching the implicit cast Postgres applies to the comparand.
        LogicalType::Float8 => {
            let number = signed_number(expr).ok_or(FoldError::TypeMismatch {
                found: describe(expr),
            })?;
            number
                .parse::<f64>()
                .map(ScalarValue::float8)
                .map_err(|_| FoldError::BadLiteral {
                    literal: number,
                    reason: None,
                })
        }
        // No literal codec for PERIOD: predicates build their intervals from
        // PERIOD(a,b) endpoints, not from a folded period scalar (see stele-exec).
        LogicalType::Period => Err(FoldError::UnsupportedType(LogicalType::Period)),
    }
}

/// Public façade over the internal literal folder for callers outside the binder.
///
/// The engine's temporal introspection folds a `\history` key literal to the
/// table's key type ([STL-199]) and must produce **byte-identical** bytes to the
/// ones [`bind_dml`](crate::bind_dml) folded the original `INSERT` key to, so the
/// business key matches. Reusing the same folding path is what guarantees that;
/// this only collapses the crate-private fold error into a short human reason,
/// keeping the error enum internal to the binder.
///
/// # Errors
///
/// A one-line reason if `expr` is not a literal of type `ty` (NULL, a type
/// mismatch, an out-of-range / malformed literal, or an unsupported column type).
///
/// [STL-199]: https://allegromusic.atlassian.net/browse/STL-199
pub fn fold_literal(expr: &Expr, ty: LogicalType) -> Result<ScalarValue, String> {
    fold_scalar(expr, ty).map_err(|err| match err {
        FoldError::Null => "key literal is NULL".to_owned(),
        FoldError::TypeMismatch { found } => {
            format!("key literal is a {found}, expected {ty}")
        }
        FoldError::BadLiteral { literal, reason } => reason.map_or_else(
            || format!("key literal {literal:?} is not a valid {ty}"),
            |reason| format!("key literal {literal:?} is not a valid {ty}: {reason}"),
        ),
        FoldError::UnsupportedType(ty) => format!("{ty} keys are not supported"),
    })
}

/// Fold one **`COPY`/text-format field** — already a plain string, not a quoted
/// SQL literal — into a typed [`ScalarValue`] of `ty`, or report why it cannot.
///
/// The bulk-load sibling of [`fold_scalar`] ([STL-236]): it shares the very same
/// per-type codecs ([`str::parse`] for the integers, [`parse_uuid`] / [`parse_bytea`]
/// for the binary types, [`stele_common::datetime`] for the civil-time types), so a
/// value loaded by `COPY` is byte-identical to the same value written by `INSERT`
/// and round-trips through a read unchanged. The difference from [`fold_scalar`] is
/// only the input shape: a `COPY` field is the raw value text (`123`, `t`,
/// `2023-11-14`), where an `INSERT` carries a parsed SQL literal (`123`, `TRUE`,
/// `DATE '2023-11-14'`).
///
/// SQL `NULL` is resolved by the caller — the wire layer maps the `COPY` null
/// marker to an absent cell upstream — so this is only ever called on a present
/// field and never returns [`FoldError::Null`]. Whitespace is significant for
/// `text`/`uuid`/`bytea` (taken verbatim) but trimmed for the numeric, boolean,
/// and civil-time types, matching Postgres's input functions.
///
/// [STL-236]: https://allegromusic.atlassian.net/browse/STL-236
pub(crate) fn fold_text_field(text: &str, ty: LogicalType) -> Result<ScalarValue, FoldError> {
    let bad = |reason: &'static str| FoldError::BadLiteral {
        literal: text.to_owned(),
        reason: Some(reason),
    };
    match ty {
        LogicalType::Int4 => text
            .trim()
            .parse::<i32>()
            .map(ScalarValue::Int4)
            .map_err(|_| FoldError::BadLiteral {
                literal: text.to_owned(),
                reason: None,
            }),
        LogicalType::Int8 => text
            .trim()
            .parse::<i64>()
            .map(ScalarValue::Int8)
            .map_err(|_| FoldError::BadLiteral {
                literal: text.to_owned(),
                reason: None,
            }),
        LogicalType::Text => Ok(ScalarValue::Text(text.to_owned())),
        LogicalType::Bool => parse_copy_bool(text.trim())
            .map(ScalarValue::Bool)
            .ok_or_else(|| bad("expected a boolean: t/f, true/false, yes/no, on/off, 1/0")),
        LogicalType::Uuid => parse_uuid(text)
            .map(ScalarValue::Uuid)
            .ok_or_else(|| bad("expected a UUID: 32 hex digits, optionally hyphenated")),
        LogicalType::Bytea => parse_bytea(text)
            .map(ScalarValue::Bytea)
            .ok_or_else(|| bad("expected bytea hex: `\\x` then an even number of hex digits")),
        LogicalType::TimestampTz => fold_civil_text(text, |s| {
            stele_common::datetime::parse_timestamptz(s).map(ScalarValue::TimestampTz)
        }),
        LogicalType::Timestamp => fold_civil_text(text, |s| {
            stele_common::datetime::parse_timestamp(s).map(ScalarValue::Timestamp)
        }),
        LogicalType::Date => fold_civil_text(text, |s| {
            stele_common::datetime::parse_date(s).map(ScalarValue::Date)
        }),
        ty @ (LogicalType::Period | LogicalType::Float8) => Err(FoldError::UnsupportedType(ty)),
    }
}

/// Parse a `COPY` boolean field, accepting the Postgres boolean input spellings
/// case-insensitively. `None` for anything else.
fn parse_copy_bool(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "t" | "true" | "yes" | "y" | "on" | "1" => Some(true),
        "f" | "false" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Fold a civil-time `COPY` field (the raw value text, trimmed) through one of the
/// [`stele_common::datetime`] codecs — the [`fold_civil`] of the text-field path.
fn fold_civil_text(
    text: &str,
    parse: impl Fn(&str) -> Result<ScalarValue, stele_common::datetime::DatetimeParseError>,
) -> Result<ScalarValue, FoldError> {
    parse(text.trim()).map_err(|e| FoldError::BadLiteral {
        literal: e.literal,
        reason: Some(e.reason),
    })
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
    match expr {
        // Peel parentheses so `(DATE '…')` describes like `DATE '…'`.
        Expr::Nested(inner) => describe(inner),
        // A typed string of the *wrong* type lands here — `string_literal`
        // already matched the right-typed ones.
        Expr::TypedString(_) => "a typed literal of a different type",
        _ => match literal(expr) {
            Some(Value::SingleQuotedString(_)) => "a string literal",
            Some(Value::Boolean(_)) => "a boolean literal",
            Some(Value::Number(..)) => "a numeric literal",
            Some(Value::Null) => "NULL",
            _ => "a non-literal expression",
        },
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

    #[test]
    fn folds_float8_from_integer_and_decimal_literals() {
        // A FLOAT8 comparand (`HAVING AVG(x) > 5`) folds an integer literal to its
        // exact `f64`, and a decimal literal too ([STL-327]).
        let int = Expr::Value(Value::Number("5".to_owned(), false).into());
        assert_eq!(
            fold_scalar(&int, LogicalType::Float8),
            Ok(ScalarValue::float8(5.0))
        );
        let dec = Expr::Value(Value::Number("2.5".to_owned(), false).into());
        assert_eq!(
            fold_scalar(&dec, LogicalType::Float8),
            Ok(ScalarValue::float8(2.5))
        );
        // A non-numeric literal for a FLOAT8 comparand is a type mismatch, the same
        // shape the integer arms report.
        assert!(matches!(
            fold_scalar(&str_lit("x"), LogicalType::Float8),
            Err(FoldError::TypeMismatch { .. })
        ));
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
        // The match is on the LOWERED type: VARCHAR '…' folds into a TEXT
        // column because both lower to `Text`.
        assert_eq!(
            fold_scalar(&typed_lit(DataType::Varchar(None), "hi"), LogicalType::Text),
            Ok(ScalarValue::Text("hi".to_owned()))
        );
        // …but a typed string of a DIFFERENT type is a mismatch, not a cast —
        // and parentheses don't blunt the diagnostic.
        for expr in [
            typed_lit(DataType::Date, "2023-11-14"),
            Expr::Nested(Box::new(typed_lit(DataType::Date, "2023-11-14"))),
        ] {
            assert!(matches!(
                fold_scalar(&expr, LogicalType::Timestamp),
                Err(FoldError::TypeMismatch {
                    found: "a typed literal of a different type"
                })
            ));
        }
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

    #[test]
    fn fold_text_field_parses_each_type_from_raw_text() {
        // Numbers parse from the bare value text (no SQL-literal wrapper), trimmed.
        assert_eq!(
            fold_text_field("123", LogicalType::Int4),
            Ok(ScalarValue::Int4(123))
        );
        assert_eq!(
            fold_text_field(" -5 ", LogicalType::Int8),
            Ok(ScalarValue::Int8(-5))
        );
        // Text is taken verbatim — surrounding spaces are significant, unlike a
        // quoted SQL literal.
        assert_eq!(
            fold_text_field("  hi  ", LogicalType::Text),
            Ok(ScalarValue::Text("  hi  ".to_owned()))
        );
        // Civil-time, uuid, bytea share the literal codecs.
        assert_eq!(
            fold_text_field("2023-11-14", LogicalType::Date),
            Ok(ScalarValue::Date(19_675))
        );
        assert_eq!(
            fold_text_field("\\xdead", LogicalType::Bytea),
            Ok(ScalarValue::Bytea(vec![0xDE, 0xAD]))
        );
    }

    #[test]
    fn fold_text_field_accepts_the_copy_boolean_spellings() {
        for t in ["t", "true", "TRUE", "yes", "y", "on", "1"] {
            assert_eq!(
                fold_text_field(t, LogicalType::Bool),
                Ok(ScalarValue::Bool(true)),
                "{t:?}"
            );
        }
        for f in ["f", "false", "FALSE", "no", "n", "off", "0"] {
            assert_eq!(
                fold_text_field(f, LogicalType::Bool),
                Ok(ScalarValue::Bool(false)),
                "{f:?}"
            );
        }
        assert!(matches!(
            fold_text_field("maybe", LogicalType::Bool),
            Err(FoldError::BadLiteral { .. })
        ));
    }

    #[test]
    fn fold_text_field_rejects_a_malformed_number() {
        assert!(matches!(
            fold_text_field("not-an-int", LogicalType::Int4),
            Err(FoldError::BadLiteral { .. })
        ));
        // An out-of-range value for the narrower integer is also rejected.
        assert!(matches!(
            fold_text_field("99999999999", LogicalType::Int4),
            Err(FoldError::BadLiteral { .. })
        ));
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
