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
    let dialect = SteleDialect::default();
    let (system_versioning, valid_time) = extract_create_table_clauses(&mut tokens)?;
    let as_of = lift_as_of(&mut tokens, &dialect)?;

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
