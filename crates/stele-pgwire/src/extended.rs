//! Postgres **extended-query** protocol decoding + parameter substitution
//! ([STL-182]).
//!
//! This module is the *pure* half of the extended-query feature: it parses the
//! `Parse` / `Bind` / `Describe` / `Execute` / `Close` message bodies into owned
//! structs and substitutes a [`Bind`](BindMsg)'s parameter values into a parsed
//! [`Statement`] before it reaches the engine. It does no socket I/O and never
//! touches the session engine, so the substitution rules — the part most likely
//! to be subtly wrong — are unit-testable in isolation. The async state machine
//! that drives these (and owns the prepared-statement / portal cache) lives in
//! [`lib`](super).
//!
//! ## Parameter substitution
//!
//! A prepared statement carries `$1 … $n` placeholders, which `sqlparser` parses
//! as [`Value::Placeholder`]. On `Bind`, each placeholder is replaced in-place
//! with a literal [`Value`] built from the wire parameter's **text** bytes
//! (binary format is [STL-77] \[G23\], not this ticket). The literal's *variant*
//! is chosen so the column-directed binder folds it correctly — the binder wants
//! a numeric literal for an `int` column, a string literal for `text`, a boolean
//! for `bool` — keyed off the parameter type OID the client declared in `Parse`:
//!
//! | declared OID            | substituted literal              |
//! |-------------------------|----------------------------------|
//! | `int4` / `int8`         | [`Value::Number`]                |
//! | `text`                  | [`Value::SingleQuotedString`]    |
//! | `bool`                  | [`Value::Boolean`]               |
//! | `timestamptz`           | string (binder parses the zone offset to UTC) |
//! | `timestamp` / `date`    | string (binder rejects — no DML codec yet) |
//! | unspecified (`0`) / other | inferred: integer → number, else string |
//!
//! A wrong inference for an unspecified-type parameter can only produce a clean
//! binder *type error*, never a wrong write, because the binder re-validates the
//! folded literal against the actual column type. Richer type inference from
//! query context is a follow-up.
//!
//! [STL-182]: https://allegromusic.atlassian.net/browse/STL-182
//! [STL-77]: https://allegromusic.atlassian.net/browse/STL-77

use stele_common::types::LogicalType;
use stele_sql::Statement;
use stele_sql::sqlparser::ast::{
    Expr, Query, SelectItem, SetExpr, Statement as SqlStatement, Value,
};

// ---------------------------------------------------------------------------
// Decoded message bodies
// ---------------------------------------------------------------------------

/// A decoded `Parse` ('P') message: name the statement, the SQL text, and the
/// caller-declared parameter type OIDs (`0` = "you infer it").
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParseMsg {
    pub name: String,
    pub query: String,
    pub param_oids: Vec<u32>,
}

/// A decoded `Bind` ('B') message. Parameter values are raw bytes (`None` is a
/// SQL `NULL`, the length-`-1` sentinel); the format-code lists are kept verbatim
/// so the caller can reject binary format (deferred to \[G23\]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BindMsg {
    pub portal: String,
    pub statement: String,
    pub param_formats: Vec<i16>,
    pub params: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<i16>,
}

/// A decoded `Describe` ('D') or `Close` ('C') target: a named prepared
/// statement or a named portal. The two messages share this shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    Statement(String),
    Portal(String),
}

/// A decoded `Execute` ('E') message: the portal to run and the row cap (`0` =
/// no limit, fetch every row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecuteMsg {
    pub portal: String,
    pub max_rows: i32,
}

// ---------------------------------------------------------------------------
// Message decoding
// ---------------------------------------------------------------------------

/// A bounds-checked big-endian cursor over a message payload. Every read returns
/// `None` on truncation rather than panicking, so a malformed frame surfaces as
/// a clean protocol violation in the caller.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn i16(&mut self) -> Option<i16> {
        Some(i16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    /// A NUL-terminated string, consuming the terminator. Lossy UTF-8 — a name /
    /// SQL string with invalid bytes is mangled, not rejected, matching the
    /// startup-param reader.
    fn cstring(&mut self) -> Option<String> {
        let rel = self.buf.get(self.pos..)?.iter().position(|&b| b == 0)?;
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
        self.pos += rel + 1;
        Some(s)
    }

    /// A signed count read as the next `i16`, rejecting a negative (Postgres
    /// count fields are non-negative).
    fn count(&mut self) -> Option<usize> {
        usize::try_from(self.i16()?).ok()
    }
}

/// Decode a `Parse` body: stmt-name, query (both cstrings), then an `Int16` count
/// of parameter type OIDs followed by that many `Int32` OIDs.
pub(crate) fn parse_parse(payload: &[u8]) -> Option<ParseMsg> {
    let mut r = Reader::new(payload);
    let name = r.cstring()?;
    let query = r.cstring()?;
    let n = r.count()?;
    let mut param_oids = Vec::with_capacity(n);
    for _ in 0..n {
        param_oids.push(r.u32()?);
    }
    Some(ParseMsg {
        name,
        query,
        param_oids,
    })
}

/// Decode a `Bind` body: portal + statement names, the parameter format-code
/// array, the parameter value array (`Int32` length, `-1` = NULL, else that many
/// bytes), and the result format-code array.
pub(crate) fn parse_bind(payload: &[u8]) -> Option<BindMsg> {
    let mut r = Reader::new(payload);
    let portal = r.cstring()?;
    let statement = r.cstring()?;

    let n_formats = r.count()?;
    let mut param_formats = Vec::with_capacity(n_formats);
    for _ in 0..n_formats {
        param_formats.push(r.i16()?);
    }

    let n_params = r.count()?;
    let mut params = Vec::with_capacity(n_params);
    for _ in 0..n_params {
        // Only `-1` is the NULL sentinel; any other negative length is a
        // corrupt frame, not a NULL, so reject the whole message.
        match r.i32()? {
            -1 => params.push(None),
            len if len >= 0 => {
                let bytes = r.take(usize::try_from(len).ok()?)?;
                params.push(Some(bytes.to_vec()));
            }
            _ => return None,
        }
    }

    let n_results = r.count()?;
    let mut result_formats = Vec::with_capacity(n_results);
    for _ in 0..n_results {
        result_formats.push(r.i16()?);
    }

    Some(BindMsg {
        portal,
        statement,
        param_formats,
        params,
        result_formats,
    })
}

/// Decode a `Describe` or `Close` body: a one-byte target kind (`S` = statement,
/// `P` = portal) followed by the name cstring.
pub(crate) fn parse_target(payload: &[u8]) -> Option<Target> {
    let mut r = Reader::new(payload);
    let kind = r.take(1)?[0];
    let name = r.cstring()?;
    match kind {
        b'S' => Some(Target::Statement(name)),
        b'P' => Some(Target::Portal(name)),
        _ => None,
    }
}

/// Decode an `Execute` body: the portal name then an `Int32` maximum row count.
pub(crate) fn parse_execute(payload: &[u8]) -> Option<ExecuteMsg> {
    let mut r = Reader::new(payload);
    let portal = r.cstring()?;
    let max_rows = r.i32()?;
    Some(ExecuteMsg { portal, max_rows })
}

// ---------------------------------------------------------------------------
// Parameter → literal
// ---------------------------------------------------------------------------

/// Why a wire parameter could not be turned into an AST literal.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ParamError {
    #[error("parameter ${index} has no bound value")]
    MissingParam { index: usize },

    #[error("parameter is not valid UTF-8 text")]
    NotUtf8,

    #[error("invalid input syntax for type boolean: {0:?}")]
    BadBool(String),
}

/// Build the AST literal a text-format wire parameter substitutes for, choosing
/// the [`Value`] variant from the declared type `oid` (see the module docs).
pub(crate) fn param_to_value(oid: u32, bytes: Option<&[u8]>) -> Result<Value, ParamError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };
    let text = std::str::from_utf8(bytes).map_err(|_| ParamError::NotUtf8)?;
    let value = match LogicalType::from_pg_oid(oid) {
        // The numeric types substitute as a numeric literal. `float8` ([STL-209])
        // has no column or DML codec yet, so a float8 param has nowhere to fold —
        // the binder rejects it cleanly against any real column, like the integers.
        Some(LogicalType::Int4 | LogicalType::Int8 | LogicalType::Float8) => {
            Value::Number(text.to_owned(), false)
        }
        Some(LogicalType::Bool) => {
            Value::Boolean(parse_bool(text).ok_or_else(|| ParamError::BadBool(text.to_owned()))?)
        }
        // Every text-bearing type is substituted as a single-quoted string for the
        // binder to fold against the column type. `text` is verbatim; the textual
        // `uuid` / `bytea` forms (`550e…`, `\xDEAD…`, STL-181) and `timestamptz`
        // (zone offset → UTC) fold to a value, while the zone-less `timestamp` /
        // `date` / `period` have no DML codec yet and surface the binder's
        // documented "unsupported" error. Either way this layer never guesses a
        // calendar/range encoding. (Binary-format params ride in with STL-183.)
        Some(
            LogicalType::Text
            | LogicalType::Uuid
            | LogicalType::Bytea
            | LogicalType::TimestampTz
            | LogicalType::Timestamp
            | LogicalType::Date
            | LogicalType::Period,
        ) => Value::SingleQuotedString(text.to_owned()),
        // Unspecified (OID 0) or a type outside the set: infer from the text. An
        // integer-looking value folds to a numeric literal so `WHERE id = $1`
        // works without a declared type; everything else is a string. The binder
        // re-checks against the column type, so a wrong guess is a clean type
        // error, never a wrong write.
        None => {
            if text.parse::<i64>().is_ok() {
                Value::Number(text.to_owned(), false)
            } else {
                Value::SingleQuotedString(text.to_owned())
            }
        }
    };
    Ok(value)
}

/// Postgres `boolin` text input: `t`/`true`/`yes`/`on`/`1` and their negatives,
/// case-insensitive. `None` for anything else.
fn parse_bool(text: &str) -> Option<bool> {
    let t = text.trim();
    if t.eq_ignore_ascii_case("t")
        || t.eq_ignore_ascii_case("true")
        || t.eq_ignore_ascii_case("yes")
        || t.eq_ignore_ascii_case("on")
        || t == "1"
    {
        Some(true)
    } else if t.eq_ignore_ascii_case("f")
        || t.eq_ignore_ascii_case("false")
        || t.eq_ignore_ascii_case("no")
        || t.eq_ignore_ascii_case("off")
        || t == "0"
    {
        Some(false)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Placeholder substitution
// ---------------------------------------------------------------------------

/// Substitute bound parameter values into a parsed statement's `$1 … $n`
/// placeholders, returning a fresh [`Statement`] ready to bind + execute.
///
/// Only the expression positions the engine reads are visited — `INSERT … VALUES`
/// rows, `UPDATE … SET` values and `WHERE`, `DELETE … WHERE`, and a `SELECT`'s
/// projection + `WHERE`. A placeholder the walker does not reach (or a `$k` with
/// `k` beyond the supplied parameters) is left in place and surfaces as a binder
/// error rather than a silently-wrong literal — except the out-of-range case,
/// which we flag here as [`ParamError::MissingParam`].
pub(crate) fn substitute(stmt: &Statement, params: &[Value]) -> Result<Statement, ParamError> {
    let mut out = stmt.clone();
    let mut err = None;
    walk_statement(&mut out.body, params, &mut err);
    err.map_or(Ok(out), Err)
}

fn walk_statement(stmt: &mut SqlStatement, params: &[Value], err: &mut Option<ParamError>) {
    match stmt {
        SqlStatement::Query(query) => walk_query(query, params, err),
        SqlStatement::Insert(insert) => {
            if let Some(source) = insert.source.as_deref_mut() {
                walk_query(source, params, err);
            }
        }
        SqlStatement::Update(update) => {
            for assignment in &mut update.assignments {
                walk_expr(&mut assignment.value, params, err);
            }
            if let Some(selection) = &mut update.selection {
                walk_expr(selection, params, err);
            }
        }
        SqlStatement::Delete(delete) => {
            if let Some(selection) = &mut delete.selection {
                walk_expr(selection, params, err);
            }
        }
        _ => {}
    }
}

fn walk_query(query: &mut Query, params: &[Value], err: &mut Option<ParamError>) {
    walk_set_expr(&mut query.body, params, err);
}

fn walk_set_expr(set: &mut SetExpr, params: &[Value], err: &mut Option<ParamError>) {
    match set {
        SetExpr::Select(select) => {
            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        walk_expr(expr, params, err);
                    }
                    _ => {}
                }
            }
            if let Some(selection) = &mut select.selection {
                walk_expr(selection, params, err);
            }
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in &mut row.content {
                    walk_expr(expr, params, err);
                }
            }
        }
        SetExpr::Query(inner) => walk_query(inner, params, err),
        _ => {}
    }
}

fn walk_expr(expr: &mut Expr, params: &[Value], err: &mut Option<ParamError>) {
    match expr {
        Expr::Value(vws) => {
            if let Value::Placeholder(name) = &vws.value {
                if let Some(index) = placeholder_index(name) {
                    match params.get(index - 1) {
                        // Keep the placeholder's span; only its value changes.
                        Some(value) => vws.value = value.clone(),
                        None if err.is_none() => *err = Some(ParamError::MissingParam { index }),
                        None => {}
                    }
                }
            }
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => walk_expr(expr, params, err),
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, params, err);
            walk_expr(right, params, err);
        }
        Expr::InList { expr, list, .. } => {
            walk_expr(expr, params, err);
            for item in list {
                walk_expr(item, params, err);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_expr(expr, params, err);
            walk_expr(low, params, err);
            walk_expr(high, params, err);
        }
        _ => {}
    }
}

/// The 1-based index of a `$n` placeholder, or `None` for any other placeholder
/// form (`?`, `$foo`, `$0`). `$0` is rejected — Postgres parameters are 1-based.
fn placeholder_index(name: &str) -> Option<usize> {
    let digits = name.strip_prefix('$')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let index = digits.parse::<usize>().ok()?;
    (index > 0).then_some(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = stele_sql::parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1);
        stmts.remove(0)
    }

    /// The literal a substituted placeholder folded to, peeling the span wrapper.
    fn literal_at(expr: &Expr) -> &Value {
        match expr {
            Expr::Value(vws) => &vws.value,
            _ => panic!("not a literal: {expr:?}"),
        }
    }

    #[test]
    fn parse_message_round_trips_name_query_and_oids() {
        // "s1\0SELECT 1\0" + Int16 2 + Int32 23 + Int32 25
        let mut body = b"s1\0SELECT 1\0".to_vec();
        body.extend_from_slice(&2i16.to_be_bytes());
        body.extend_from_slice(&23u32.to_be_bytes());
        body.extend_from_slice(&25u32.to_be_bytes());
        assert_eq!(
            parse_parse(&body),
            Some(ParseMsg {
                name: "s1".to_owned(),
                query: "SELECT 1".to_owned(),
                param_oids: vec![23, 25],
            })
        );
    }

    #[test]
    fn truncated_parse_message_is_rejected() {
        // A count of 2 OIDs but only one present.
        let mut body = b"\0SELECT 1\0".to_vec();
        body.extend_from_slice(&2i16.to_be_bytes());
        body.extend_from_slice(&23u32.to_be_bytes());
        assert_eq!(parse_parse(&body), None);
    }

    #[test]
    fn bind_message_decodes_formats_values_and_nulls() {
        // portal "" / statement "s1"; one text format; two params ("7", NULL);
        // one text result format.
        let mut body = b"\0s1\0".to_vec();
        body.extend_from_slice(&1i16.to_be_bytes()); // one param format
        body.extend_from_slice(&0i16.to_be_bytes()); // text
        body.extend_from_slice(&2i16.to_be_bytes()); // two params
        body.extend_from_slice(&1i32.to_be_bytes()); // len 1
        body.push(b'7');
        body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        body.extend_from_slice(&1i16.to_be_bytes()); // one result format
        body.extend_from_slice(&0i16.to_be_bytes()); // text
        assert_eq!(
            parse_bind(&body),
            Some(BindMsg {
                portal: String::new(),
                statement: "s1".to_owned(),
                param_formats: vec![0],
                params: vec![Some(b"7".to_vec()), None],
                result_formats: vec![0],
            })
        );
    }

    #[test]
    fn bind_rejects_a_negative_length_other_than_null() {
        // A parameter length of `-2` is not the NULL sentinel (`-1`); it is a
        // corrupt frame and must be rejected, not misread as NULL.
        let mut body = b"\0s\0".to_vec();
        body.extend_from_slice(&0i16.to_be_bytes()); // no param formats
        body.extend_from_slice(&1i16.to_be_bytes()); // one param
        body.extend_from_slice(&(-2i32).to_be_bytes()); // invalid length
        assert_eq!(parse_bind(&body), None);
    }

    #[test]
    fn target_and_execute_decode() {
        assert_eq!(parse_target(b"S\0"), Some(Target::Statement(String::new())));
        assert_eq!(
            parse_target(b"Pmy_portal\0"),
            Some(Target::Portal("my_portal".to_owned()))
        );
        assert_eq!(parse_target(b"X\0"), None);

        let mut exec = b"p\0".to_vec();
        exec.extend_from_slice(&100i32.to_be_bytes());
        assert_eq!(
            parse_execute(&exec),
            Some(ExecuteMsg {
                portal: "p".to_owned(),
                max_rows: 100,
            })
        );
    }

    #[test]
    fn oid_drives_the_literal_variant() {
        // int4 / int8 → numeric literal; text → string; bool → boolean.
        assert_eq!(
            param_to_value(23, Some(b"42")),
            Ok(Value::Number("42".to_owned(), false))
        );
        assert_eq!(
            param_to_value(20, Some(b"-5")),
            Ok(Value::Number("-5".to_owned(), false))
        );
        assert_eq!(
            param_to_value(25, Some(b"hi")),
            Ok(Value::SingleQuotedString("hi".to_owned()))
        );
        assert_eq!(param_to_value(16, Some(b"t")), Ok(Value::Boolean(true)));
        assert_eq!(
            param_to_value(16, Some(b"FALSE")),
            Ok(Value::Boolean(false))
        );
        assert_eq!(
            param_to_value(16, Some(b"maybe")),
            Err(ParamError::BadBool("maybe".to_owned()))
        );
        // NULL regardless of type.
        assert_eq!(param_to_value(23, None), Ok(Value::Null));
    }

    #[test]
    fn unspecified_oid_infers_number_or_string() {
        assert_eq!(
            param_to_value(0, Some(b"123")),
            Ok(Value::Number("123".to_owned(), false))
        );
        assert_eq!(
            param_to_value(0, Some(b"abc")),
            Ok(Value::SingleQuotedString("abc".to_owned()))
        );
    }

    #[test]
    fn substitute_fills_insert_values() {
        let stmt = parse_one("INSERT INTO account VALUES ($1, $2)");
        let params = vec![Value::Number("1".to_owned(), false), Value::Null];
        let bound = substitute(&stmt, &params).expect("substitute");
        let SqlStatement::Insert(insert) = &bound.body else {
            panic!("insert");
        };
        let query = insert.source.as_deref().expect("source");
        let SetExpr::Values(values) = query.body.as_ref() else {
            panic!("values");
        };
        let row = &values.rows[0].content;
        assert_eq!(literal_at(&row[0]), &Value::Number("1".to_owned(), false));
        assert_eq!(literal_at(&row[1]), &Value::Null);
    }

    #[test]
    fn substitute_fills_update_set_and_where() {
        let stmt = parse_one("UPDATE account SET balance = $1 WHERE id = $2");
        let params = vec![
            Value::Number("250".to_owned(), false),
            Value::Number("1".to_owned(), false),
        ];
        let bound = substitute(&stmt, &params).expect("substitute");
        let SqlStatement::Update(update) = &bound.body else {
            panic!("update");
        };
        assert_eq!(
            literal_at(&update.assignments[0].value),
            &Value::Number("250".to_owned(), false)
        );
        let Some(Expr::BinaryOp { right, .. }) = &update.selection else {
            panic!("where binop");
        };
        assert_eq!(literal_at(right), &Value::Number("1".to_owned(), false));
    }

    #[test]
    fn substitute_reports_a_missing_parameter() {
        let stmt = parse_one("INSERT INTO account VALUES ($1, $2)");
        // Only one parameter supplied for two placeholders.
        let params = vec![Value::Number("1".to_owned(), false)];
        assert!(matches!(
            substitute(&stmt, &params),
            Err(ParamError::MissingParam { index: 2 })
        ));
    }

    #[test]
    fn placeholder_index_only_accepts_dollar_n() {
        assert_eq!(placeholder_index("$1"), Some(1));
        assert_eq!(placeholder_index("$42"), Some(42));
        assert_eq!(placeholder_index("$0"), None);
        assert_eq!(placeholder_index("$foo"), None);
        assert_eq!(placeholder_index("?"), None);
        assert_eq!(placeholder_index("1"), None);
    }
}
