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
    AdminCommand, AsOf, ExplainStmt, Password, PeriodExpr, PeriodPredicateClause, SessionCommand,
    Statement, StatementBody, Temporal, TemporalRange, TimeDimension, UserDdl, ValidTimePeriod,
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

    // `EXPLAIN [ANALYZE] <statement>` ([STL-260]) wraps an inner statement, so it
    // is lifted first — ahead of the other token-level lifts and `sqlparser` —
    // and the remainder is parsed recursively through this same path (so the
    // inner's temporal clauses are stripped as usual). `sqlparser` has its own
    // `EXPLAIN`, but it would reject those clauses.
    if let Some(explain) = lift_explain(&tokens)? {
        return Ok(Statement {
            body: StatementBody::Explain(Box::new(explain)),
            temporal: Temporal::default(),
        });
    }

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

    // User DDL (`CREATE`/`ALTER`/`DROP USER`, [STL-252]) is lifted the same
    // way: `sqlparser` parses the family with Snowflake's `KEY = VALUE` option
    // grammar and rejects the Postgres `PASSWORD '…'` form Stele speaks. Like
    // an admin command, user DDL carries no temporal grammar — a stray
    // temporal clause fails the strict pattern and errors loudly.
    if let Some(user) = lift_user_ddl(&tokens)? {
        return Ok(Statement {
            body: StatementBody::User(user),
            temporal: Temporal::default(),
        });
    }

    // `SET` / `RESET` ([STL-246]) is lifted the same way: Stele owns the session
    // time-context surface (`SET stele.system_time = …`) and tolerates every other
    // variable as a no-op, so the whole family is recognized here rather than
    // handed to `sqlparser`'s own `SET` grammar. Like the lifts above it carries no
    // temporal grammar of its own.
    if let Some(session) = lift_session_command(&tokens, &dialect)? {
        return Ok(Statement {
            body: StatementBody::Session(session),
            temporal: Temporal::default(),
        });
    }

    let (system_versioning, valid_time) = extract_create_table_clauses(&mut tokens)?;
    let (as_of, range) = lift_temporal_qualifiers(&mut tokens, &dialect)?;
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

    // A `FOR … FROM/BETWEEN` range is likewise a read-time qualifier — lifting it
    // off a write/DDL would silently strip it, so reject the misuse loudly
    // ([STL-244]).
    if range.is_some() && !matches!(body, SqlStatement::Query(_)) {
        return Err(ParseError::Temporal(
            "FOR ... FROM/BETWEEN applies only to a SELECT query".to_string(),
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
            range,
            period_predicate,
        },
    })
}

/// Recognize a leading `EXPLAIN [ANALYZE]` and lift it off the token stream
/// ([STL-260]), recursively parsing the remainder as the statement being
/// explained.
///
/// `sqlparser` has its own `EXPLAIN` grammar, but the inner statement may carry
/// Stele temporal clauses (`EXPLAIN SELECT … FOR SYSTEM_TIME AS OF …`) it would
/// reject — so EXPLAIN is recognized here and the inner is parsed through
/// [`parse_one`], which strips those clauses. Returns the lifted statement,
/// `None` if the tokens are not an EXPLAIN, or an error for a malformed one (no
/// inner statement, or an inner that is not a plannable SQL statement — a nested
/// `EXPLAIN`, an admin/session/user command).
fn lift_explain(tokens: &[Token]) -> Result<Option<ExplainStmt>, ParseError> {
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    if !word_is(first, "EXPLAIN") {
        return Ok(None);
    }
    let syntax = |msg: &str| ParseError::Syntax(ParserError::ParserError(msg.to_owned()));
    // `EXPLAIN ANALYZE` opts into executing the inner statement and measuring it;
    // a bare `EXPLAIN` only renders the plan shape. The parenthesized option list
    // (`EXPLAIN (FORMAT …)`) and other modifiers are out of v0.3 scope.
    let mut rest = &tokens[1..];
    let analyze = rest.first().is_some_and(|t| word_is(t, "ANALYZE"));
    if analyze {
        rest = &rest[1..];
    }
    if rest.is_empty() {
        return Err(syntax("EXPLAIN requires a statement to explain"));
    }
    let inner = parse_one(rest.to_vec())?;
    // EXPLAIN renders a query/DML plan; an admin/session/user command or a nested
    // EXPLAIN has no plan to render. (A DDL body parses as `Sql` and is rejected
    // later, at bind time, with a clearer message.)
    if !matches!(inner.body, StatementBody::Sql(_)) {
        return Err(syntax(
            "EXPLAIN supports only a SELECT / INSERT / UPDATE / DELETE statement",
        ));
    }
    Ok(Some(ExplainStmt { analyze, inner }))
}

/// Recognize a Stele admin command — `CHECKPOINT` / `FLUSH` / `COMPACT` (a bare
/// keyword) or `BACKUP TO '<path>'` — which `sqlparser` has no grammar for
/// ([STL-219], [STL-231], [STL-249]). Returns the command, `None` if the tokens
/// are not an admin command, or an error if the syntax is malformed (a trailing
/// token on a no-argument command, or a missing/ill-formed `BACKUP` target).
fn lift_admin_command(tokens: &[Token]) -> Result<Option<AdminCommand>, ParseError> {
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    // `BACKUP TO '<path>'` is the one admin command that takes an argument, so it
    // gets its own shape check rather than the no-arguments rule below.
    if word_is(first, "BACKUP") {
        return lift_backup(tokens).map(Some);
    }
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

/// Parse `BACKUP TO '<path>'` into [`AdminCommand::Backup`] ([STL-249]). The
/// caller has already matched the leading `BACKUP`; the rest must be exactly `TO`
/// followed by a non-empty single-quoted path.
fn lift_backup(tokens: &[Token]) -> Result<AdminCommand, ParseError> {
    let syntax = |msg: &str| ParseError::Syntax(ParserError::ParserError(msg.to_owned()));
    const USAGE: &str = "BACKUP syntax: BACKUP TO '<path>'";
    if tokens.len() != 3 || !word_is(&tokens[1], "TO") {
        return Err(syntax(USAGE));
    }
    match &tokens[2] {
        Token::SingleQuotedString(path) if !path.is_empty() => {
            Ok(AdminCommand::Backup { path: path.clone() })
        }
        Token::SingleQuotedString(_) => Err(syntax("BACKUP target path must not be empty")),
        _ => Err(syntax(USAGE)),
    }
}

/// Recognize Stele's user-administration DDL ([STL-252]):
///
/// ```text
/// CREATE USER <name> [WITH] PASSWORD '<password>'
/// ALTER  USER <name> [WITH] PASSWORD '<password>'
/// DROP   USER [IF EXISTS] <name>
/// ```
///
/// Returns `None` when the tokens do not start with one of the three verbs
/// followed by `USER` (the statement belongs to another route); once the
/// prefix matches, the rest must parse **exactly** — a malformed tail is a
/// loud syntax error, never a fall-through to `sqlparser`'s Snowflake-shaped
/// `KEY = VALUE` grammar for the same statements.
fn lift_user_ddl(tokens: &[Token]) -> Result<Option<UserDdl>, ParseError> {
    let verb = match tokens.first() {
        Some(t) if word_is(t, "CREATE") => "CREATE",
        Some(t) if word_is(t, "ALTER") => "ALTER",
        Some(t) if word_is(t, "DROP") => "DROP",
        _ => return Ok(None),
    };
    if !tokens.get(1).is_some_and(|t| word_is(t, "USER")) {
        return Ok(None);
    }
    let syntax = |msg: String| ParseError::Syntax(ParserError::ParserError(msg));
    let rest = &tokens[2..];

    if verb == "DROP" {
        // DROP USER [IF EXISTS] <name>
        let (if_exists, rest) = if rest.first().is_some_and(|t| word_is(t, "IF"))
            && rest.get(1).is_some_and(|t| word_is(t, "EXISTS"))
        {
            (true, &rest[2..])
        } else {
            (false, rest)
        };
        let [name_tok] = rest else {
            return Err(syntax("expected DROP USER [IF EXISTS] <name>".to_owned()));
        };
        let name = token_as_ident(name_tok)
            .ok_or_else(|| syntax(format!("expected a user name, found: {name_tok}")))?;
        return Ok(Some(UserDdl::DropUser {
            name: name.value,
            if_exists,
        }));
    }

    // CREATE | ALTER USER <name> [WITH] PASSWORD '<password>'
    let Some(name_tok) = rest.first() else {
        return Err(syntax(format!(
            "expected {verb} USER <name> [WITH] PASSWORD '<password>'"
        )));
    };
    let name = token_as_ident(name_tok)
        .ok_or_else(|| syntax(format!("expected a user name, found: {name_tok}")))?;
    let mut rest = &rest[1..];
    if rest.first().is_some_and(|t| word_is(t, "WITH")) {
        rest = &rest[1..];
    }
    let [password_kw, password_tok] = rest else {
        return Err(syntax(format!(
            "expected {verb} USER <name> [WITH] PASSWORD '<password>'"
        )));
    };
    if !word_is(password_kw, "PASSWORD") {
        return Err(syntax(format!(
            "expected PASSWORD, found: {password_kw} \
             (only the PASSWORD option is supported on {verb} USER)"
        )));
    }
    let Token::SingleQuotedString(password) = password_tok else {
        return Err(syntax(format!(
            "expected a single-quoted password string, found: {password_tok}"
        )));
    };
    if password.is_empty() {
        return Err(syntax("password must not be empty".to_owned()));
    }
    let password = Password(password.clone());
    Ok(Some(if verb == "CREATE" {
        UserDdl::CreateUser {
            name: name.value,
            password,
        }
    } else {
        UserDdl::AlterUserPassword {
            name: name.value,
            password,
        }
    }))
}

/// Recognize a `SET` / `RESET` session command ([STL-246]).
///
/// Returns `None` when the tokens are not a `SET`/`RESET` (the statement belongs
/// to another route). Otherwise:
///
/// * `SET stele.system_time = <expr>` / `… TO <expr>` (and the `valid_time` twin)
///   → [`SessionCommand::SetTime`]; the value parses as a full scalar expression,
///   resolved later exactly as a `FOR … AS OF` operand.
/// * `RESET stele.{system,valid}_time` → [`SessionCommand::ResetTime`];
///   `RESET ALL` → [`SessionCommand::ResetAll`].
/// * Any other variable → [`SessionCommand::Tolerated`], a no-op so a driver's
///   connect-time `SET` preamble does not error ([STL-184]).
///
/// A malformed `SET` of a Stele time variable (no `=`/`TO`, or no value) is a loud
/// syntax error rather than a silent tolerated no-op.
fn lift_session_command(
    tokens: &[Token],
    dialect: &SteleDialect,
) -> Result<Option<SessionCommand>, ParseError> {
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    let is_reset = if word_is(first, "SET") {
        false
    } else if word_is(first, "RESET") {
        true
    } else {
        return Ok(None);
    };
    let rest = &tokens[1..];
    let syntax = |msg: &str| ParseError::Syntax(ParserError::ParserError(msg.to_owned()));

    if is_reset {
        // `RESET ALL` clears every session setting — for the time context, both axes.
        if let [only] = rest
            && word_is(only, "ALL")
        {
            return Ok(Some(SessionCommand::ResetAll));
        }
        // A `RESET` of a Stele time axis is **ours**: it must be exactly the
        // three-token name, so a trailing token (`RESET stele.system_time = 1`) is a
        // loud error rather than a silent tolerated no-op that leaves the session
        // pinned. Any non-`stele.*` variable stays tolerated.
        if let Some(dimension) = stele_time_axis(rest) {
            if rest.len() != 3 {
                return Err(syntax(
                    "RESET of a session time variable takes no arguments",
                ));
            }
            return Ok(Some(SessionCommand::ResetTime { dimension }));
        }
        return Ok(Some(SessionCommand::Tolerated {
            name: variable_name(rest),
            is_reset: true,
        }));
    }

    // `SET TRANSACTION …` is standard-SQL transaction control ([STL-248]), not a
    // session variable. Leave it for `sqlparser`'s `SET` grammar so it reaches the
    // pgwire `txn_control` handler — which refuses it outside a `BEGIN` block and
    // selects the isolation level inside one. Swallowing it here as a tolerated
    // no-op (it is not a `stele.*` axis) silently drops both behaviors. Only the
    // `SET TRANSACTION` form is handed back; `SET SESSION CHARACTERISTICS AS
    // TRANSACTION …` (a session default Stele does not model) and every other
    // variable stay tolerated below.
    if rest.first().is_some_and(|t| word_is(t, "TRANSACTION")) {
        return Ok(None);
    }

    // `SET <variable> { = | TO } <value…>`.
    let Some(dimension) = stele_time_axis(rest) else {
        // A `SET` of any non-Stele variable is tolerated as a no-op (the value is
        // not even parsed — a driver's preamble may use forms Stele cannot bind).
        return Ok(Some(SessionCommand::Tolerated {
            name: variable_name(rest),
            is_reset: false,
        }));
    };
    // The qualified name spans three tokens (`stele` `.` `<axis>_time`); the
    // assignment operator and value follow.
    let after = &rest[3..];
    let value_tokens = match after.first() {
        Some(Token::Eq) => &after[1..],
        Some(t) if word_is(t, "TO") => &after[1..],
        _ => return Err(syntax("expected `=` or TO after a session time variable")),
    };
    if value_tokens.is_empty() {
        return Err(syntax(
            "expected a value after a session time variable assignment",
        ));
    }
    let value = parse_full_expr(value_tokens, dialect)?;
    Ok(Some(SessionCommand::SetTime {
        dimension,
        value: Box::new(value),
    }))
}

/// If `tokens` begins with the qualified name `stele.system_time` /
/// `stele.valid_time` (the three tokens `word` `.` `word`), the time axis it
/// names. The match is case-insensitive; the caller knows the name spans three
/// tokens.
fn stele_time_axis(tokens: &[Token]) -> Option<TimeDimension> {
    let [namespace, dot, name, ..] = tokens else {
        return None;
    };
    if !word_is(namespace, "stele") || !matches!(dot, Token::Period) {
        return None;
    }
    if word_is(name, "system_time") {
        Some(TimeDimension::System)
    } else if word_is(name, "valid_time") {
        Some(TimeDimension::Valid)
    } else {
        None
    }
}

/// A best-effort variable name for a tolerated `SET`/`RESET`, for diagnostics: the
/// leading run of word / `.` tokens (e.g. `extra_float_digits`, `stele.unknown`).
/// Empty when there is no leading identifier.
fn variable_name(tokens: &[Token]) -> String {
    let mut name = String::new();
    for tok in tokens {
        match tok {
            Token::Word(w) => name.push_str(&w.value),
            Token::Period => name.push('.'),
            _ => break,
        }
    }
    name
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

/// Lift every `FOR { SYSTEM_TIME | VALID_TIME } …` temporal qualifier out of the
/// token stream, in source order, leaving a clean standard-SQL remainder. Two
/// forms are recognized, each beginning with `FOR <axis>`:
///
/// * **`AS OF <expr>`** — a point-in-time read ([STL-101], [STL-162]); collected
///   into the returned `Vec<AsOf>`, one per qualifier in source order.
/// * **`FROM <a> TO <b>`** / **`BETWEEN <a> AND <b>`** — a range read returning
///   all versions over an interval ([STL-244]); collected into the returned
///   `Option<TemporalRange>` (at most one per statement — a repeated range is an
///   error).
///
/// `sqlparser` allows at most one `FOR … AS OF` per table, has no `VALID_TIME`
/// axis, and no grammar for the range forms at all, so a bitemporal or range
/// qualifier can never parse natively. Each operand `<expr>` is parsed here with
/// `sqlparser`'s own expression parser — its boundary found by parsing and asking
/// how many tokens were consumed — and the whole qualifier removed.
///
/// # Errors
///
/// [`ParseError`] if a qualifier has no/ill-formed operand, if a second range
/// qualifier appears, or if a `FOR <axis>` is followed by none of `AS OF` /
/// `FROM` / `BETWEEN` (e.g. the unsupported `FOR SYSTEM_TIME ALL`).
fn lift_temporal_qualifiers(
    tokens: &mut Vec<Token>,
    dialect: &SteleDialect,
) -> Result<(Vec<AsOf>, Option<TemporalRange>), ParseError> {
    let mut as_of = Vec::new();
    let mut range: Option<TemporalRange> = None;
    let mut keep = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let is_for_axis = word_is(&tokens[i], "FOR")
            && tokens
                .get(i + 1)
                .is_some_and(|t| word_is(t, "SYSTEM_TIME") || word_is(t, "VALID_TIME"));
        if !is_for_axis {
            keep.push(tokens[i].clone());
            i += 1;
            continue;
        }
        let dimension = if word_is(&tokens[i + 1], "VALID_TIME") {
            TimeDimension::Valid
        } else {
            TimeDimension::System
        };
        // The keyword after `FOR <axis>` selects the form. `rest` is the operand
        // tokens that follow it (clamped so a truncated `FOR <axis> FROM` at the
        // tail is an empty operand the range parser rejects, not a slice panic).
        let keyword = tokens.get(i + 2);
        let rest = &tokens[(i + 3).min(tokens.len())..];
        if keyword.is_some_and(|t| word_is(t, "AS"))
            && tokens.get(i + 3).is_some_and(|t| word_is(t, "OF"))
        {
            let (timestamp, consumed) = parse_as_of_expr(&tokens[i + 4..], dialect)?;
            as_of.push(AsOf {
                dimension,
                timestamp,
            });
            i += 4 + consumed;
        } else if keyword.is_some_and(|t| word_is(t, "FROM")) {
            let (temporal_range, consumed) = lift_range(dimension, rest, "TO", false, dialect)?;
            set_range(&mut range, temporal_range)?;
            i += 3 + consumed;
        } else if keyword.is_some_and(|t| word_is(t, "BETWEEN")) {
            let (temporal_range, consumed) = lift_range(dimension, rest, "AND", true, dialect)?;
            set_range(&mut range, temporal_range)?;
            i += 3 + consumed;
        } else {
            // `FOR SYSTEM_TIME ALL` (out of scope, [STL-244]) and any other stray
            // form land here — a loud error rather than a confusing `sqlparser`
            // failure on the leftover `FOR SYSTEM_TIME` tokens.
            return Err(ParseError::Temporal(format!(
                "FOR {} must be followed by AS OF <expr>, FROM <a> TO <b>, or BETWEEN <a> AND <b>",
                if matches!(dimension, TimeDimension::Valid) {
                    "VALID_TIME"
                } else {
                    "SYSTEM_TIME"
                }
            )));
        }
    }
    *tokens = keep;
    Ok((as_of, range))
}

/// Record a lifted range qualifier, rejecting a second one — a statement carries
/// at most one range ([STL-244]).
fn set_range(slot: &mut Option<TemporalRange>, range: TemporalRange) -> Result<(), ParseError> {
    if slot.is_some() {
        return Err(ParseError::Temporal(
            "at most one FOR ... FROM/BETWEEN range qualifier per statement".to_string(),
        ));
    }
    *slot = Some(range);
    Ok(())
}

/// Parse a range qualifier's two bound expressions starting at `rest` (the tokens
/// after `FOR <axis> {FROM|BETWEEN}`), with `separator` the keyword between them
/// (`TO` for `FROM..TO`, `AND` for `BETWEEN..AND`). Returns the [`TemporalRange`]
/// and the number of `rest` tokens consumed (the lower bound, the separator, and
/// the upper bound).
///
/// The separator is located at paren-depth 0 first, so the lower bound is the run
/// before it (parsed as one complete expression) and the upper bound the run
/// after (parsed up to the next clause). `BETWEEN`'s `AND` would otherwise be
/// swallowed as a boolean operator by the expression parser, so the explicit
/// split is what keeps both forms honest.
///
/// # Errors
///
/// [`ParseError`] if the separator is absent, or either bound is missing or does
/// not parse as a single expression.
fn lift_range(
    dimension: TimeDimension,
    rest: &[Token],
    separator: &str,
    closed_upper: bool,
    dialect: &SteleDialect,
) -> Result<(TemporalRange, usize), ParseError> {
    let sep_idx = top_level_position(rest, |t| word_is(t, separator)).ok_or_else(|| {
        ParseError::Temporal(format!(
            "expected `{separator}` between the two range bounds"
        ))
    })?;
    let from = parse_complete_expr(&rest[..sep_idx], dialect)?;
    let (to, to_consumed) = parse_as_of_expr(&rest[sep_idx + 1..], dialect)?;
    Ok((
        TemporalRange {
            dimension,
            from,
            to,
            closed_upper,
        },
        sep_idx + 1 + to_consumed,
    ))
}

/// Parse `tokens` as exactly one scalar expression, requiring it to consume them
/// all — a range's lower bound is a complete expression, so anything left over is
/// junk.
///
/// # Errors
///
/// [`ParseError`] if the bound is empty, fails to parse, or leaves trailing
/// tokens.
fn parse_complete_expr(tokens: &[Token], dialect: &SteleDialect) -> Result<Expr, ParseError> {
    if tokens.is_empty() {
        return Err(ParseError::Temporal("a range bound is empty".to_string()));
    }
    let mut parser = Parser::new(dialect).with_tokens(tokens.to_vec());
    let expr = parser.parse_expr()?;
    if parser.peek_token().token != Token::EOF {
        return Err(ParseError::Temporal(
            "a range bound is not a single expression".to_string(),
        ));
    }
    Ok(expr)
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

#[cfg(test)]
mod session_tests {
    use super::*;

    fn parse_one_ok(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1);
        stmts.remove(0)
    }

    fn session(sql: &str) -> SessionCommand {
        match parse_one_ok(sql).body {
            StatementBody::Session(cmd) => cmd,
            other => panic!("expected a session command, got {other:?}"),
        }
    }

    #[test]
    fn lifts_set_stele_time_on_both_axes() {
        for (sql, want) in [
            ("SET stele.system_time = 100", TimeDimension::System),
            ("SET stele.valid_time = 100", TimeDimension::Valid),
            // `TO` is accepted in place of `=`, and the name is case-insensitive.
            ("SET STELE.SYSTEM_TIME TO 100", TimeDimension::System),
        ] {
            match session(sql) {
                SessionCommand::SetTime { dimension, .. } => assert_eq!(dimension, want, "{sql}"),
                other => panic!("expected SetTime, got {other:?} for {sql}"),
            }
        }
    }

    #[test]
    fn set_time_value_is_a_full_as_of_expression() {
        // The value parses with the same grammar a `FOR … AS OF` operand does.
        for sql in [
            "SET stele.system_time = now()",
            "SET stele.system_time = now() - interval '1 hour'",
            "SET stele.system_time = 1750000000000000",
        ] {
            assert!(
                matches!(session(sql), SessionCommand::SetTime { .. }),
                "{sql}"
            );
        }
    }

    #[test]
    fn lifts_reset_of_the_time_axes_and_all() {
        assert_eq!(
            session("RESET stele.system_time"),
            SessionCommand::ResetTime {
                dimension: TimeDimension::System
            }
        );
        assert_eq!(
            session("RESET stele.valid_time"),
            SessionCommand::ResetTime {
                dimension: TimeDimension::Valid
            }
        );
        assert_eq!(session("RESET ALL"), SessionCommand::ResetAll);
    }

    #[test]
    fn unknown_variables_are_tolerated_no_ops() {
        // A driver's connect-time preamble must not error ([STL-184]).
        match session("SET extra_float_digits = 3") {
            SessionCommand::Tolerated { name, is_reset } => {
                assert_eq!(name, "extra_float_digits");
                assert!(!is_reset);
            }
            other => panic!("expected Tolerated, got {other:?}"),
        }
        match session("SET application_name = 'PostgreSQL JDBC Driver'") {
            SessionCommand::Tolerated { is_reset, .. } => assert!(!is_reset),
            other => panic!("expected Tolerated, got {other:?}"),
        }
        match session("RESET extra_float_digits") {
            SessionCommand::Tolerated { name, is_reset } => {
                assert_eq!(name, "extra_float_digits");
                assert!(is_reset);
            }
            other => panic!("expected Tolerated, got {other:?}"),
        }
    }

    #[test]
    fn set_transaction_is_not_lifted_as_a_session_command() {
        // `SET TRANSACTION …` is standard-SQL transaction control ([STL-248]), not a
        // session variable: it must reach `sqlparser`'s `SET` grammar (and the
        // pgwire `txn_control` handler), never be swallowed here as a tolerated
        // no-op (which left it neither refused outside a block nor applied inside).
        for sql in [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ] {
            match parse_one_ok(sql).body {
                StatementBody::Sql(SqlStatement::Set(sqlparser::ast::Set::SetTransaction {
                    session,
                    ..
                })) => assert!(
                    !session,
                    "{sql}: the per-transaction form, not a session default"
                ),
                other => {
                    panic!("expected a SET TRANSACTION SQL statement, got {other:?} for {sql}")
                }
            }
        }

        // `SET SESSION CHARACTERISTICS AS TRANSACTION …` sets a session default
        // Stele does not model, so it stays a tolerated no-op — a driver preamble
        // using it must not error.
        match session("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED") {
            SessionCommand::Tolerated { is_reset, .. } => assert!(!is_reset),
            other => panic!("expected Tolerated, got {other:?}"),
        }
    }

    #[test]
    fn malformed_stele_time_set_is_a_loud_error() {
        // The Stele time variables are ours: a malformed assignment errors rather
        // than being swallowed as a tolerated no-op.
        for sql in [
            "SET stele.system_time",     // no `= value`
            "SET stele.system_time = ",  // no value
            "SET stele.system_time 100", // missing `=`/`TO`
        ] {
            assert!(parse(sql).is_err(), "expected a parse error for: {sql}");
        }
    }

    #[test]
    fn reset_of_a_stele_time_variable_is_strict() {
        // A trailing token on `RESET stele.<axis>_time` is a loud error, not a
        // silent tolerated no-op that would leave the session pinned.
        for sql in [
            "RESET stele.system_time = 1",
            "RESET stele.system_time now()",
            "RESET stele.valid_time junk",
        ] {
            assert!(parse(sql).is_err(), "expected a parse error for: {sql}");
        }
        // A non-Stele `RESET` with trailing tokens stays tolerated (we do not own it).
        assert!(matches!(
            session("RESET extra_float_digits"),
            SessionCommand::Tolerated { is_reset: true, .. }
        ));
    }

    #[test]
    fn a_plain_select_is_not_a_session_command() {
        assert!(matches!(
            parse_one_ok("SELECT 1").body,
            StatementBody::Sql(_)
        ));
    }
}

#[cfg(test)]
mod range_tests {
    use super::*;

    fn parse_one_ok(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1);
        stmts.remove(0)
    }

    fn range(sql: &str) -> TemporalRange {
        parse_one_ok(sql)
            .temporal
            .range
            .expect("a range qualifier was lifted")
    }

    #[test]
    fn from_to_is_half_open_and_between_is_closed() {
        let from_to = range("SELECT * FROM t FOR SYSTEM_TIME FROM 10 TO 20");
        assert_eq!(from_to.dimension, TimeDimension::System);
        assert!(!from_to.closed_upper, "FROM..TO is half-open");

        let between = range("SELECT * FROM t FOR SYSTEM_TIME BETWEEN 10 AND 20");
        assert!(between.closed_upper, "BETWEEN..AND is closed");
    }

    #[test]
    fn valid_time_range_lifts_on_the_valid_axis() {
        // The grammar is symmetric across axes ([STL-328]): a `FOR VALID_TIME`
        // range lifts the same way, tagged with the valid dimension.
        let from_to = range("SELECT * FROM t FOR VALID_TIME FROM 10 TO 20");
        assert_eq!(from_to.dimension, TimeDimension::Valid);
        assert!(!from_to.closed_upper, "FROM..TO is half-open");

        let between = range("SELECT * FROM t FOR VALID_TIME BETWEEN 10 AND 20");
        assert_eq!(between.dimension, TimeDimension::Valid);
        assert!(between.closed_upper, "BETWEEN..AND is closed");
    }

    #[test]
    fn range_leaves_a_clean_standard_sql_body() {
        // The qualifier is lifted off, so the body is a plain `SELECT … WHERE`.
        let stmt = parse_one_ok("SELECT id FROM t FOR SYSTEM_TIME FROM 1 TO 9 WHERE id = 1");
        match stmt.sql() {
            Some(SqlStatement::Query(q)) => match q.body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => {
                    assert!(s.selection.is_some(), "the WHERE survives the lift");
                }
                other => panic!("expected a SELECT body, got {other:?}"),
            },
            other => panic!("expected a query, got {other:?}"),
        }
    }

    #[test]
    fn range_bounds_accept_the_full_as_of_expression_grammar() {
        // The bounds parse with the same grammar an `AS OF` operand does.
        let r = range("SELECT * FROM t FOR SYSTEM_TIME FROM now() - interval '1 hour' TO now()");
        assert!(matches!(r.from, Expr::BinaryOp { .. }));
        assert!(matches!(r.to, Expr::Function(_)));
    }

    #[test]
    fn between_does_not_swallow_its_and_as_a_boolean_operator() {
        // `BETWEEN a AND b`'s `AND` is the separator, not a boolean operator over
        // the two bounds — each bound is its own expression.
        let r = range("SELECT * FROM t FOR SYSTEM_TIME BETWEEN 100 AND 200");
        assert!(matches!(r.from, Expr::Value(_)));
        assert!(matches!(r.to, Expr::Value(_)));
    }

    #[test]
    fn a_second_range_qualifier_is_rejected() {
        assert!(matches!(
            parse("SELECT * FROM t FOR SYSTEM_TIME FROM 1 TO 2 FOR VALID_TIME FROM 3 TO 4"),
            Err(ParseError::Temporal(_))
        ));
    }

    #[test]
    fn a_range_on_a_non_select_is_rejected() {
        assert!(matches!(
            parse("DELETE FROM t FOR SYSTEM_TIME FROM 1 TO 2"),
            Err(ParseError::Temporal(_))
        ));
    }

    #[test]
    fn malformed_ranges_and_unsupported_forms_are_rejected() {
        for sql in [
            "SELECT * FROM t FOR SYSTEM_TIME FROM 1",    // no TO bound
            "SELECT * FROM t FOR SYSTEM_TIME FROM 1 TO", // empty upper bound
            "SELECT * FROM t FOR SYSTEM_TIME TO 2",      // no FROM/BETWEEN
            "SELECT * FROM t FOR SYSTEM_TIME BETWEEN 1", // no AND bound
            "SELECT * FROM t FOR SYSTEM_TIME ALL",       // ALL is out of scope
        ] {
            assert!(parse(sql).is_err(), "expected a parse error for: {sql}");
        }
    }

    #[test]
    fn a_plain_select_has_no_range() {
        assert!(parse_one_ok("SELECT 1").temporal.range.is_none());
    }
}
