//! SQL entry point: tokenize, strip Stele's temporal clauses, hand the rest to
//! `sqlparser-rs`, then re-attach the temporal grammar as typed annotations.
//!
//! ## Why preprocess at the token level
//!
//! `sqlparser-rs` parses `FOR SYSTEM_TIME AS OF` natively once the dialect opts
//! in ([`SteleDialect`]), but it has no grammar for Stele's other temporal
//! clauses (`WITH SYSTEM VERSIONING`, `VALID TIME (..)`) and no concept of the
//! `VALID_TIME` axis. Rather than fork the parser this early
//! ([`docs/02-architecture.md` §6] says start from `sqlparser` and revisit a
//! hand-written parser only if needed), we run a small pass over the token
//! stream: we lift the non-standard clauses out into [`Temporal`], rewrite
//! `VALID_TIME` to `SYSTEM_TIME` so the qualifier parses, and let `sqlparser`
//! handle the standard remainder. The lifted axis is recovered afterward from
//! the recorded dimensions.
//!
//! [`docs/02-architecture.md` §6]: ../../../docs/02-architecture.md#6-query-layer

use sqlparser::ast::{
    Expr, Ident, Query, SetExpr, Statement as SqlStatement, TableFactor, TableVersion,
    TableWithJoins,
};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::ast::{AsOf, Statement, Temporal, TimeDimension, ValidTimePeriod};
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
    let (system_versioning, valid_time) = extract_create_table_clauses(&mut tokens)?;
    let dimensions = lift_as_of_dimensions(&mut tokens);

    let dialect = SteleDialect::default();
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

    let as_of = build_as_of(&body, &dimensions);
    Ok(Statement {
        body,
        temporal: Temporal {
            system_versioning,
            valid_time,
            as_of,
        },
    })
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

/// Record, in source order, the axis of each `FOR { SYSTEM_TIME | VALID_TIME }
/// AS OF` qualifier, rewriting `VALID_TIME` to `SYSTEM_TIME` so `sqlparser`
/// parses the qualifier. The axis is recovered later via [`build_as_of`].
fn lift_as_of_dimensions(tokens: &mut [Token]) -> Vec<TimeDimension> {
    let mut dimensions = Vec::new();
    for i in 0..tokens.len() {
        let is_qualifier = word_is(&tokens[i], "FOR")
            && tokens
                .get(i + 1)
                .is_some_and(|t| word_is(t, "SYSTEM_TIME") || word_is(t, "VALID_TIME"))
            && tokens.get(i + 2).is_some_and(|t| word_is(t, "AS"))
            && tokens.get(i + 3).is_some_and(|t| word_is(t, "OF"));
        if !is_qualifier {
            continue;
        }
        if word_is(&tokens[i + 1], "VALID_TIME") {
            dimensions.push(TimeDimension::Valid);
            tokens[i + 1] = Token::make_word("SYSTEM_TIME", None);
        } else {
            dimensions.push(TimeDimension::System);
        }
    }
    dimensions
}

/// Pair each `FOR SYSTEM_TIME AS OF` expression `sqlparser` parsed (in source
/// order) with the axis [`lift_as_of_dimensions`] recorded for it.
fn build_as_of(body: &SqlStatement, dimensions: &[TimeDimension]) -> Vec<AsOf> {
    let mut exprs = Vec::new();
    if let SqlStatement::Query(query) = body {
        collect_set_expr(&query.body, &mut exprs);
    }
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, timestamp)| AsOf {
            // Default to System if the counts ever diverge, so we never reject a
            // query sqlparser accepted; in practice the lengths match.
            dimension: dimensions
                .get(idx)
                .copied()
                .unwrap_or(TimeDimension::System),
            timestamp,
        })
        .collect()
}

fn collect_set_expr(set_expr: &SetExpr, out: &mut Vec<Expr>) {
    match set_expr {
        SetExpr::Select(select) => {
            for twj in &select.from {
                collect_table_with_joins(twj, out);
            }
        }
        SetExpr::Query(query) => collect_set_expr(&query.body, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr(left, out);
            collect_set_expr(right, out);
        }
        _ => {}
    }
}

fn collect_table_with_joins(twj: &TableWithJoins, out: &mut Vec<Expr>) {
    collect_table_factor(&twj.relation, out);
    for join in &twj.joins {
        collect_table_factor(&join.relation, out);
    }
}

fn collect_table_factor(factor: &TableFactor, out: &mut Vec<Expr>) {
    match factor {
        TableFactor::Table {
            version: Some(TableVersion::ForSystemTimeAsOf(expr)),
            ..
        } => out.push(expr.clone()),
        TableFactor::Derived { subquery, .. } => collect_query(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_table_with_joins(table_with_joins, out),
        _ => {}
    }
}

fn collect_query(query: &Query, out: &mut Vec<Expr>) {
    collect_set_expr(&query.body, out);
}
