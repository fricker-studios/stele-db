//! SQL entry point: tokenize, strip Stele's temporal clauses, hand the rest to
//! `sqlparser-rs`, then re-attach the temporal grammar as typed annotations.
//!
//! ## Why preprocess at the token level
//!
//! `sqlparser-rs` has no grammar for Stele's temporal clauses (`WITH SYSTEM
//! VERSIONING`, `VALID TIME (..)`), no concept of the `VALID_TIME` axis, and —
//! crucially — only one `FOR … AS OF` qualifier per table, so it cannot parse a
//! bitemporal `… FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS OF v`. Rather than
//! fork the parser this early ([`docs/02-architecture.md` §6] says start from
//! `sqlparser` and revisit a hand-written parser only if needed), we run a small
//! pass over the token stream: we lift the non-standard clauses out into
//! [`Temporal`] — including **every** `FOR { SYSTEM_TIME | VALID_TIME } AS OF
//! <expr>` qualifier, parsing each `<expr>` with `sqlparser`'s own expression
//! parser — and hand the clean standard-SQL remainder to `sqlparser`. The lifted
//! qualifiers are the single source of truth for the binder; `sqlparser` never
//! sees a versioned table (the dialect leaves `supports_table_versioning` off),
//! so a qualifier we somehow failed to lift fails loudly rather than parsing as a
//! lone system-time version that silently drops the second axis.
//!
//! [`docs/02-architecture.md` §6]: ../../../docs/02-architecture.md#6-query-layer

use sqlparser::ast::{Expr, Ident, Statement as SqlStatement};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};
use stele_common::period::PeriodPredicate;

use crate::ast::{
    AdminCommand, AsOf, PeriodExpr, PeriodPredicateClause, Statement, StatementBody, Temporal,
    TimeDimension, ValidTimePeriod,
};
use crate::dialect::SteleDialect;
use crate::error::ParseError;

/// Parse SQL text into a sequence of [`Statement`]s.
///
/// Multiple `;`-separated statements are supported. Comments and whitespace are
/// ignored. Each statement's Stele temporal grammar is captured in
/// [`Statement::temporal`]; the standard-SQL remainder lives in
/// [`Statement::body`].
///
/// # Errors
///
/// Returns [`ParseError`] if the input fails to tokenize, if a temporal clause
/// is malformed, or if the standard-SQL remainder is not valid SQL.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let dialect = SteleDialect::default();

    // Tokenize once, then drop insignificant tokens. In sqlparser, comments are
    // `Token::Whitespace` variants, so this filter removes both spaces and
    // comments. The parser skips them anyway, and removing them keeps the
    // clause-extraction indexing simple.
    let tokens: Vec<Token> = Tokenizer::new(&dialect, sql)
        .tokenize()?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();

    split_statements(tokens)
        .into_iter()
        .map(parse_one)
        .collect()
}

/// Split a whitespace-free token stream into one run per top-level statement,
/// dropping the `;` separators and any empty (e.g. trailing `;`) runs.
fn split_statements(tokens: Vec<Token>) -> Vec<Vec<Token>> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    let mut depth: i32 = 0;
    for tok in tokens {
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::SemiColon if depth == 0 => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                continue;
            }
            _ => {}
        }
        current.push(tok);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Extract temporal grammar from one statement's tokens, parse the remainder,
/// and stitch the two back together.
fn parse_one(mut tokens: Vec<Token>) -> Result<Statement, ParseError> {
    let dialect = SteleDialect::default();

    // Stele admin commands (`CHECKPOINT` / `FLUSH`) have no `sqlparser` grammar,
    // so recognize them at the token level before handing the remainder to
    // `sqlparser` — which would reject the bare keyword. Same lift discipline as
    // the temporal clauses; an admin command carries no body and no temporal.
    if let Some(admin) = lift_admin_command(&tokens)? {
        return Ok(Statement {
            body: StatementBody::Admin(admin),
            temporal: Temporal::default(),
        });
    }

    let (system_versioning, valid_time) = extract_create_table_clauses(&mut tokens)?;
    let as_of = lift_as_of(&mut tokens, &dialect)?;
    let period_predicate = lift_period_predicate(&mut tokens, &dialect)?;

    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    let body = parser.parse_statement()?;

    // `parse_statement` parses one statement and stops; it does not object to
    // leftover tokens. Reject any trailing junk (e.g. a dangling comma left by
    // clause stripping) so invalid input doesn't parse silently.
    let trailing = parser.peek_token().token;
    if trailing != Token::EOF {
        return Err(ParseError::Syntax(ParserError::ParserError(format!(
            "unexpected trailing token after statement: {trailing}"
        ))));
    }

    // `FOR … AS OF` is a read-time qualifier — it only makes sense on a SELECT.
    // Because we lift it off the token stream, a stray qualifier on a write or
    // DDL would otherwise be silently stripped and the statement run against the
    // present. Reject it here so the misuse fails loudly. (AS OF on DML is
    // deferred grammar — see docs/sql-grammar.md.)
    if !as_of.is_empty() && !matches!(body, SqlStatement::Query(_)) {
        return Err(ParseError::Temporal(
            "FOR ... AS OF applies only to a SELECT query".to_string(),
        ));
    }

    // A period predicate is a `WHERE` filter — only a SELECT has one. Lifting it
    // off a write/DDL would silently drop it, so reject the misuse loudly.
    if period_predicate.is_some() && !matches!(body, SqlStatement::Query(_)) {
        return Err(ParseError::Temporal(
            "a period predicate applies only to a SELECT query".to_string(),
        ));
    }

    Ok(Statement {
        body: StatementBody::Sql(body),
        temporal: Temporal {
            system_versioning,
            valid_time,
            as_of,
            period_predicate,
        },
    })
}

/// Recognize a bare Stele admin command (`CHECKPOINT` / `FLUSH` / `COMPACT`) — a
/// single keyword that `sqlparser` has no grammar for ([STL-219], [STL-231]).
/// Returns the command, `None` if the tokens are not an admin command, or an
/// error if the keyword carries trailing tokens (the commands take no arguments).
fn lift_admin_command(tokens: &[Token]) -> Result<Option<AdminCommand>, ParseError> {
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    let (cmd, name) = if word_is(first, "CHECKPOINT") {
        (AdminCommand::Checkpoint, "CHECKPOINT")
    } else if word_is(first, "FLUSH") {
        (AdminCommand::Flush, "FLUSH")
    } else if word_is(first, "COMPACT") {
        (AdminCommand::Compact, "COMPACT")
    } else {
        return Ok(None);
    };
    if tokens.len() != 1 {
        return Err(ParseError::Syntax(ParserError::ParserError(format!(
            "{name} takes no arguments"
        ))));
    }
    Ok(Some(cmd))
}

/// Whether a token is an (unquoted) word equal to `kw`, case-insensitively.
fn word_is(tok: &Token, kw: &str) -> bool {
    matches!(tok, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(kw))
}

/// Build an [`Ident`] from a word token, preserving its quoting.
fn token_as_ident(tok: &Token) -> Option<Ident> {
    match tok {
        Token::Word(w) => {
            let mut id = Ident::new(w.value.clone());
            id.quote_style = w.quote_style;
            Some(id)
        }
        _ => None,
    }
}

/// Strip `WITH SYSTEM VERSIONING` and `VALID TIME (from, to)` from a
/// `CREATE TABLE`'s trailing table-option list, returning what they declared.
///
/// Both clauses sit after the parenthesized column list. Anything we don't
/// recognize is left in place for `sqlparser` to parse (or reject).
fn extract_create_table_clauses(
    tokens: &mut Vec<Token>,
) -> Result<(bool, Option<ValidTimePeriod>), ParseError> {
    // Only `CREATE TABLE …`.
    if !(tokens.first().is_some_and(|t| word_is(t, "CREATE"))
        && tokens.get(1).is_some_and(|t| word_is(t, "TABLE")))
    {
        return Ok((false, None));
    }

    // Find the matching close of the column-list parenthesis; clauses follow it.
    let Some(close) = column_list_close(tokens) else {
        return Ok((false, None));
    };

    let mut system_versioning = false;
    let mut valid_time = None;
    let mut keep = Vec::new();

    let tail = &tokens[close + 1..];
    let mut i = 0;
    while i < tail.len() {
        if word_is(&tail[i], "WITH")
            && tail.get(i + 1).is_some_and(|t| word_is(t, "SYSTEM"))
            && tail.get(i + 2).is_some_and(|t| word_is(t, "VERSIONING"))
        {
            system_versioning = true;
            i += 3;
        } else if word_is(&tail[i], "VALID") && tail.get(i + 1).is_some_and(|t| word_is(t, "TIME"))
        {
            let (period, next) = parse_valid_time_clause(tail, i + 2)?;
            valid_time = Some(period);
            i = next;
        } else {
            keep.push(tail[i].clone());
            i += 1;
            continue;
        }
        // The clauses may be comma-separated (`WITH SYSTEM VERSIONING, VALID
        // TIME (..)`); swallow a separator comma so it doesn't leak into the
        // stripped SQL. Only when another clause actually follows — a trailing
        // comma with nothing after it is left in place so `sqlparser` rejects
        // the invalid statement.
        if tail.get(i).is_some_and(|t| matches!(t, Token::Comma))
            && starts_temporal_clause(tail, i + 1)
        {
            i += 1;
        }
    }

    tokens.truncate(close + 1);
    tokens.extend(keep);
    Ok((system_versioning, valid_time))
}

/// Whether a recognized `CREATE TABLE` temporal clause (`WITH SYSTEM
/// VERSIONING` or `VALID TIME`) begins at `i`.
fn starts_temporal_clause(tail: &[Token], i: usize) -> bool {
    let with_versioning = tail.get(i).is_some_and(|t| word_is(t, "WITH"))
        && tail.get(i + 1).is_some_and(|t| word_is(t, "SYSTEM"))
        && tail.get(i + 2).is_some_and(|t| word_is(t, "VERSIONING"));
    let valid_time = tail.get(i).is_some_and(|t| word_is(t, "VALID"))
        && tail.get(i + 1).is_some_and(|t| word_is(t, "TIME"));
    with_versioning || valid_time
}

/// Index of the `)` that closes the first top-level `(` — the column list.
fn column_list_close(tokens: &[Token]) -> Option<usize> {
    let open = tokens.iter().position(|t| matches!(t, Token::LParen))?;
    let mut depth = 0i32;
    for (offset, tok) in tokens[open..].iter().enumerate() {
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse `(from, to)` starting at `start` (the `(`); returns the period and the
/// index just past the closing `)`.
fn parse_valid_time_clause(
    tail: &[Token],
    start: usize,
) -> Result<(ValidTimePeriod, usize), ParseError> {
    let err = || ParseError::Temporal("expected `VALID TIME (from, to)`".to_string());

    if !matches!(tail.get(start), Some(Token::LParen)) {
        return Err(err());
    }
    let from = tail
        .get(start + 1)
        .and_then(token_as_ident)
        .ok_or_else(err)?;
    if !matches!(tail.get(start + 2), Some(Token::Comma)) {
        return Err(err());
    }
    let to = tail
        .get(start + 3)
        .and_then(token_as_ident)
        .ok_or_else(err)?;
    if !matches!(tail.get(start + 4), Some(Token::RParen)) {
        return Err(err());
    }
    Ok((ValidTimePeriod { from, to }, start + 5))
}

/// Lift every `FOR { SYSTEM_TIME | VALID_TIME } AS OF <expr>` qualifier out of
/// the token stream, in source order, leaving a clean standard-SQL remainder.
///
/// `sqlparser` allows at most one `FOR … AS OF` per table and has no `VALID_TIME`
/// axis, so a bitemporal `… FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS OF v` can
/// never parse natively. Instead each qualifier's `<expr>` is parsed here with
/// `sqlparser`'s own expression parser — the boundary of the expression is found
/// by parsing and asking how many tokens were consumed — and the whole qualifier
/// (the `FOR … AS OF` prefix plus the expression's tokens) is removed.
///
/// # Errors
///
/// [`ParseError`] if a qualifier has no expression after `AS OF`, or if that
/// expression does not parse.
fn lift_as_of(tokens: &mut Vec<Token>, dialect: &SteleDialect) -> Result<Vec<AsOf>, ParseError> {
    let mut as_of = Vec::new();
    let mut keep = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let is_qualifier = word_is(&tokens[i], "FOR")
            && tokens
                .get(i + 1)
                .is_some_and(|t| word_is(t, "SYSTEM_TIME") || word_is(t, "VALID_TIME"))
            && tokens.get(i + 2).is_some_and(|t| word_is(t, "AS"))
            && tokens.get(i + 3).is_some_and(|t| word_is(t, "OF"));
        if !is_qualifier {
            keep.push(tokens[i].clone());
            i += 1;
            continue;
        }
        let dimension = if word_is(&tokens[i + 1], "VALID_TIME") {
            TimeDimension::Valid
        } else {
            TimeDimension::System
        };
        let (timestamp, consumed) = parse_as_of_expr(&tokens[i + 4..], dialect)?;
        as_of.push(AsOf {
            dimension,
            timestamp,
        });
        i += 4 + consumed;
    }
    *tokens = keep;
    Ok(as_of)
}

/// Parse the single expression at the head of `tokens` (the operand just after
/// an `AS OF`), returning it and the number of tokens it consumed. The remainder
/// (the next clause — `WHERE`, another `FOR …`, end of statement) is left for the
/// caller.
fn parse_as_of_expr(tokens: &[Token], dialect: &SteleDialect) -> Result<(Expr, usize), ParseError> {
    if tokens.is_empty() {
        return Err(ParseError::Temporal(
            "expected an expression after `AS OF`".to_string(),
        ));
    }
    let mut parser = Parser::new(dialect).with_tokens(tokens.to_vec());
    let expr = parser.parse_expr()?;
    // `parse_expr` stops without consuming the terminating token; the parser's
    // current index is the count of tokens it consumed. (Always ≥ 1 on success,
    // so `get_current_index() + 1` is exact.)
    let consumed = parser.get_current_index() + 1;
    Ok((expr, consumed))
}

/// Lift a `WHERE PERIOD(a, b) <pred> PERIOD(c, d)` period predicate off the token
/// stream when the whole `WHERE` clause is one such predicate ([STL-165]).
///
/// `sqlparser` has no grammar for the SQL:2011 period predicates (`CONTAINS`,
/// `OVERLAPS`, `PRECEDES`, …), so — like `FOR … AS OF` — they are lifted here and
/// bound separately, leaving `sqlparser` a clean `SELECT … FROM t`.
///
/// Recognized **only** when the `WHERE` clause is a single period predicate and
/// nothing else: the clause must begin with `PERIOD` and consume the whole
/// remainder of the statement. A `col = <lit>` `WHERE` is left in place for the
/// equality binder, and a period predicate combined with other conditions
/// (`… AND id = 1`, a trailing `ORDER BY`, …) is a loud error rather than a
/// silent partial-filter.
///
/// # Errors
///
/// [`ParseError`] if a `WHERE` begins with `PERIOD` but is not a well-formed
/// `PERIOD(a, b) <pred> PERIOD(c, d)` spanning the rest of the statement.
fn lift_period_predicate(
    tokens: &mut Vec<Token>,
    dialect: &SteleDialect,
) -> Result<Option<PeriodPredicateClause>, ParseError> {
    let Some(where_idx) = top_level_position(tokens, |t| word_is(t, "WHERE")) else {
        return Ok(None);
    };
    // Only a `WHERE` that *starts* with `PERIOD` is a period predicate; anything
    // else (`WHERE id = 1`) is left for the standard equality binder.
    if !tokens
        .get(where_idx + 1)
        .is_some_and(|t| word_is(t, "PERIOD"))
    {
        return Ok(None);
    }

    let body = &tokens[where_idx + 1..];
    let (left, after_left) = parse_period_operand(body, 0, dialect)?;
    let (predicate, after_pred) = parse_period_keyword(body, after_left)?;
    if !body.get(after_pred).is_some_and(|t| word_is(t, "PERIOD")) {
        return Err(period_err("expected a second `PERIOD(...)` operand"));
    }
    let (right, after_right) = parse_period_operand(body, after_pred, dialect)?;
    if after_right != body.len() {
        return Err(period_err(
            "a period predicate must be the whole WHERE clause (no trailing conditions)",
        ));
    }

    tokens.truncate(where_idx);
    Ok(Some(PeriodPredicateClause {
        left,
        predicate,
        right,
    }))
}

/// Parse a `PERIOD ( <expr> , <expr> )` operand starting at `start` (the `PERIOD`
/// word) within `body`; returns the operand and the index just past its `)`.
fn parse_period_operand(
    body: &[Token],
    start: usize,
    dialect: &SteleDialect,
) -> Result<(PeriodExpr, usize), ParseError> {
    if !body.get(start).is_some_and(|t| word_is(t, "PERIOD")) {
        return Err(period_err("expected `PERIOD(from, to)`"));
    }
    if !matches!(body.get(start + 1), Some(Token::LParen)) {
        return Err(period_err("expected `(` after `PERIOD`"));
    }
    let open = start + 1;
    let close = matching_paren(body, open).ok_or_else(|| period_err("unbalanced `PERIOD(...)`"))?;
    let inner = &body[open + 1..close];
    let comma = top_level_position(inner, |t| matches!(t, Token::Comma))
        .ok_or_else(|| period_err("`PERIOD(from, to)` needs two comma-separated bounds"))?;
    let (from_toks, to_toks) = (&inner[..comma], &inner[comma + 1..]);
    if top_level_position(to_toks, |t| matches!(t, Token::Comma)).is_some() {
        return Err(period_err("`PERIOD(from, to)` takes exactly two bounds"));
    }
    let from = parse_full_expr(from_toks, dialect)?;
    let to = parse_full_expr(to_toks, dialect)?;
    Ok((PeriodExpr { from, to }, close + 1))
}

/// Parse the predicate keyword(s) at `i` within `body`; returns the predicate and
/// the index just past it. Accepts the single-word forms and the two-word
/// `IMMEDIATELY PRECEDES` / `IMMEDIATELY SUCCEEDS`; `MEETS` is a synonym for
/// `IMMEDIATELY PRECEDES`.
fn parse_period_keyword(body: &[Token], i: usize) -> Result<(PeriodPredicate, usize), ParseError> {
    let Some(first) = body.get(i) else {
        return Err(period_err("expected a period predicate keyword"));
    };
    if word_is(first, "IMMEDIATELY") {
        return match body.get(i + 1) {
            Some(t) if word_is(t, "PRECEDES") => Ok((PeriodPredicate::ImmediatelyPrecedes, i + 2)),
            Some(t) if word_is(t, "SUCCEEDS") => Ok((PeriodPredicate::ImmediatelySucceeds, i + 2)),
            _ => Err(period_err(
                "`IMMEDIATELY` must be followed by PRECEDES or SUCCEEDS",
            )),
        };
    }
    let predicate = if word_is(first, "CONTAINS") {
        PeriodPredicate::Contains
    } else if word_is(first, "OVERLAPS") {
        PeriodPredicate::Overlaps
    } else if word_is(first, "EQUALS") {
        PeriodPredicate::Equals
    } else if word_is(first, "PRECEDES") {
        PeriodPredicate::Precedes
    } else if word_is(first, "SUCCEEDS") {
        PeriodPredicate::Succeeds
    } else if word_is(first, "MEETS") {
        PeriodPredicate::ImmediatelyPrecedes
    } else {
        return Err(period_err(
            "expected one of CONTAINS / OVERLAPS / EQUALS / PRECEDES / SUCCEEDS / MEETS / IMMEDIATELY {PRECEDES|SUCCEEDS}",
        ));
    };
    Ok((predicate, i + 1))
}

/// Parse `tokens` as exactly one expression, requiring it to consume them all —
/// a `PERIOD(...)` bound is a complete expression, so anything left over is junk.
fn parse_full_expr(tokens: &[Token], dialect: &SteleDialect) -> Result<Expr, ParseError> {
    if tokens.is_empty() {
        return Err(period_err("a `PERIOD(...)` bound is empty"));
    }
    let mut parser = Parser::new(dialect).with_tokens(tokens.to_vec());
    let expr = parser.parse_expr()?;
    if parser.peek_token().token != Token::EOF {
        return Err(period_err(
            "a `PERIOD(...)` bound is not a single expression",
        ));
    }
    Ok(expr)
}

/// Index of the `)` matching the `(` at `open` within `body`, or `None` if
/// unbalanced.
fn matching_paren(body: &[Token], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (offset, tok) in body[open..].iter().enumerate() {
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

/// Position of the first token at paren-depth 0 satisfying `pred`, or `None`.
fn top_level_position(tokens: &[Token], pred: impl Fn(&Token) -> bool) -> Option<usize> {
    let mut depth = 0i32;
    for (i, tok) in tokens.iter().enumerate() {
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ if depth == 0 && pred(tok) => return Some(i),
            _ => {}
        }
    }
    None
}

/// A period-predicate parse error with a uniform prefix.
fn period_err(msg: &str) -> ParseError {
    ParseError::Temporal(format!("period predicate: {msg}"))
}

#[cfg(test)]
mod period_tests {
    use super::*;

    fn parse_one_ok(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1);
        stmts.remove(0)
    }

    #[test]
    fn lifts_a_period_predicate_off_the_where() {
        let stmt = parse_one_ok("SELECT x FROM t WHERE PERIOD(10, 20) CONTAINS PERIOD(12, 15)");
        let clause = stmt
            .temporal
            .period_predicate
            .as_ref()
            .expect("period predicate lifted");
        assert_eq!(clause.predicate, PeriodPredicate::Contains);
        // The standard-SQL body is left with a clean, predicate-free WHERE.
        match stmt.sql() {
            Some(SqlStatement::Query(q)) => {
                if let sqlparser::ast::SetExpr::Select(s) = q.body.as_ref() {
                    assert!(
                        s.selection.is_none(),
                        "the WHERE was stripped from the body"
                    );
                } else {
                    panic!("expected a SELECT body");
                }
            }
            other => panic!("expected a query, got {other:?}"),
        }
    }

    #[test]
    fn two_word_immediately_forms_parse() {
        for (kw, want) in [
            ("IMMEDIATELY PRECEDES", PeriodPredicate::ImmediatelyPrecedes),
            ("IMMEDIATELY SUCCEEDS", PeriodPredicate::ImmediatelySucceeds),
            ("MEETS", PeriodPredicate::ImmediatelyPrecedes),
        ] {
            let stmt = parse_one_ok(&format!(
                "SELECT x FROM t WHERE PERIOD(1, 2) {kw} PERIOD(3, 4)"
            ));
            assert_eq!(
                stmt.temporal.period_predicate.expect("lifted").predicate,
                want,
                "{kw}"
            );
        }
    }

    #[test]
    fn a_plain_equality_where_is_left_for_the_equality_binder() {
        // No `PERIOD` after `WHERE`, so the clause is not lifted — it stays on the
        // body for `bind_filter`.
        let stmt = parse_one_ok("SELECT x FROM t WHERE id = 1");
        assert!(stmt.temporal.period_predicate.is_none());
    }

    #[test]
    fn a_period_predicate_on_a_non_select_is_rejected() {
        // A period predicate is a read-time filter; lifting it off a DELETE would
        // silently drop it.
        assert!(matches!(
            parse("DELETE FROM t WHERE PERIOD(1, 2) OVERLAPS PERIOD(3, 4)"),
            Err(ParseError::Temporal(_))
        ));
    }

    #[test]
    fn a_period_predicate_mixed_with_other_conditions_is_rejected() {
        // The predicate must be the whole WHERE; a trailing `AND …` is a loud error
        // rather than a silent partial filter.
        assert!(matches!(
            parse("SELECT x FROM t WHERE PERIOD(1, 2) OVERLAPS PERIOD(3, 4) AND id = 1"),
            Err(ParseError::Temporal(_))
        ));
    }

    #[test]
    fn malformed_period_predicates_are_rejected() {
        for sql in [
            "SELECT x FROM t WHERE PERIOD(1, 2) FROBNICATES PERIOD(3, 4)", // unknown predicate
            "SELECT x FROM t WHERE PERIOD(1, 2) CONTAINS PERIOD(3, 4, 5)", // three bounds
            "SELECT x FROM t WHERE PERIOD(1) CONTAINS PERIOD(3, 4)",       // one bound
            "SELECT x FROM t WHERE PERIOD(1, 2) CONTAINS id",              // RHS not a PERIOD
            "SELECT x FROM t WHERE PERIOD(1, 2) IMMEDIATELY PERIOD(3, 4)", // dangling IMMEDIATELY
        ] {
            assert!(parse(sql).is_err(), "expected a parse error for: {sql}");
        }
    }
}
