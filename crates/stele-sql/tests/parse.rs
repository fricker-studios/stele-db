//! Parser acceptance tests.
//!
//! The positive cases pin the v0.1 identity demo from
//! [`docs/05-dev-environment.md`](../../../docs/05-dev-environment.md): the four
//! statements that prove time-travel must parse and surface their temporal
//! grammar. The negative cases pin what the parser must reject.

use sqlparser::ast::{SetExpr, Statement as SqlStatement, TableFactor};
use stele_sql::{AdminCommand, Password, StatementBody, TimeDimension, UserDdl, parse};

/// The canonical four-statement identity demo.
const DEMO: &str = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 250 WHERE id = 1;
SELECT balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1;";

#[test]
fn identity_demo_round_trips() {
    let stmts = parse(DEMO).expect("identity demo should parse");
    assert_eq!(stmts.len(), 4, "four statements");

    assert!(matches!(stmts[0].sql(), Some(SqlStatement::CreateTable(_))));
    assert!(matches!(stmts[1].sql(), Some(SqlStatement::Insert(_))));
    assert!(matches!(stmts[2].sql(), Some(SqlStatement::Update { .. })));
    assert!(matches!(stmts[3].sql(), Some(SqlStatement::Query(_))));
}

#[test]
fn create_table_captures_system_versioning() {
    let stmts =
        parse("CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING")
            .unwrap();
    assert!(stmts[0].temporal.system_versioning);
    assert!(stmts[0].temporal.valid_time.is_none());

    // The stripped body is clean, standard CREATE TABLE.
    let Some(SqlStatement::CreateTable(ct)) = stmts[0].sql() else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(ct.name.to_string(), "account");
    assert_eq!(ct.columns.len(), 2);
}

#[test]
fn create_table_without_versioning_is_not_temporal() {
    let stmts = parse("CREATE TABLE t (id INT)").unwrap();
    assert!(!stmts[0].temporal.system_versioning);
    assert!(!stmts[0].is_temporal());
}

#[test]
fn create_table_captures_valid_time_period() {
    let stmts = parse(
        "CREATE TABLE t (id INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    )
    .unwrap();
    let temporal = &stmts[0].temporal;
    assert!(temporal.system_versioning);
    let period = temporal.valid_time.as_ref().expect("valid time period");
    assert_eq!(period.from.value, "vf");
    assert_eq!(period.to.value, "vt");

    // Clause order is interchangeable.
    let other = parse(
        "CREATE TABLE t (id INT, vf TIMESTAMP, vt TIMESTAMP) \
         VALID TIME (vf, vt) WITH SYSTEM VERSIONING",
    )
    .unwrap();
    assert_eq!(other[0].temporal.valid_time, temporal.valid_time);
    assert!(other[0].temporal.system_versioning);
}

#[test]
fn create_table_allows_comma_separated_clauses() {
    let stmts = parse(
        "CREATE TABLE t (id INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING, VALID TIME (vf, vt)",
    )
    .unwrap();
    assert!(stmts[0].temporal.system_versioning);
    let period = stmts[0]
        .temporal
        .valid_time
        .as_ref()
        .expect("valid time period");
    assert_eq!(period.from.value, "vf");
    assert_eq!(period.to.value, "vt");
}

#[test]
fn select_captures_system_time_as_of() {
    let stmts = parse(
        "SELECT balance FROM account \
         FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1",
    )
    .unwrap();
    let as_of = &stmts[0].temporal.as_of;
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].dimension, TimeDimension::System);

    // The qualifier is lifted off the token stream into `temporal`; the body
    // `sqlparser` parses is clean standard SQL with no native version.
    assert!(!table_has_version(stmts[0].sql().expect("sql body")));
}

#[test]
fn select_captures_valid_time_as_of() {
    let stmts = parse(
        "SELECT balance FROM account \
         FOR VALID_TIME AS OF TIMESTAMP '2020-01-01 00:00:00' WHERE id = 1",
    )
    .unwrap();
    let as_of = &stmts[0].temporal.as_of;
    assert_eq!(as_of.len(), 1);
    assert_eq!(as_of[0].dimension, TimeDimension::Valid);
    assert!(!table_has_version(stmts[0].sql().expect("sql body")));
}

#[test]
fn select_captures_both_axes_as_of_in_source_order() {
    // sqlparser allows only one `FOR … AS OF` per table; both qualifiers are
    // lifted at the token level, preserving source order and per-axis tagging.
    let stmts = parse(
        "SELECT id FROM booking \
         FOR VALID_TIME AS OF 1600000000000000 \
         FOR SYSTEM_TIME AS OF 1700000000000000 WHERE id = 1",
    )
    .unwrap();
    let dims: Vec<TimeDimension> = stmts[0]
        .temporal
        .as_of
        .iter()
        .map(|a| a.dimension)
        .collect();
    assert_eq!(dims, vec![TimeDimension::Valid, TimeDimension::System]);
    assert!(!table_has_version(stmts[0].sql().expect("sql body")));
}

#[test]
fn plain_select_has_no_as_of() {
    let stmts = parse("SELECT balance FROM account WHERE id = 1").unwrap();
    assert!(stmts[0].temporal.as_of.is_empty());
    assert!(!stmts[0].is_temporal());
}

#[test]
fn insert_and_update_parse_without_temporal() {
    let stmts =
        parse("INSERT INTO account VALUES (1, 100); UPDATE account SET balance = 250 WHERE id = 1")
            .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(!stmts[0].is_temporal());
    assert!(!stmts[1].is_temporal());
}

#[test]
fn trailing_semicolons_and_whitespace_are_ignored() {
    let stmts = parse("  SELECT 1 ;\n\n ; ").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn empty_input_parses_to_no_statements() {
    assert!(parse("").unwrap().is_empty());
    assert!(parse("   ;  ; \n").unwrap().is_empty());
}

// --- Negative cases ------------------------------------------------------

#[test]
fn rejects_gibberish() {
    assert!(parse("WOMBAT 1").is_err());
}

#[test]
fn rejects_unterminated_select() {
    assert!(parse("SELECT * FROM").is_err());
}

#[test]
fn rejects_as_of_without_timestamp() {
    assert!(parse("SELECT * FROM account FOR SYSTEM_TIME AS OF").is_err());
}

#[test]
fn rejects_trailing_comma_after_clause() {
    // A dangling comma with no clause after it must not parse.
    assert!(parse("CREATE TABLE t (id INT) WITH SYSTEM VERSIONING,").is_err());
    assert!(
        parse("CREATE TABLE t (id INT, vf TIMESTAMP, vt TIMESTAMP) VALID TIME (vf, vt),").is_err()
    );
}

#[test]
fn rejects_malformed_valid_time_clause() {
    // VALID TIME present but missing the second column.
    let err = parse("CREATE TABLE t (id INT) VALID TIME (vf)").unwrap_err();
    assert!(matches!(err, stele_sql::ParseError::Temporal(_)));
}

#[test]
fn rejects_as_of_on_non_select_statements() {
    // `FOR … AS OF` is lifted off the token stream for every statement, so a
    // stray qualifier on a write or DDL must be rejected — otherwise it would be
    // silently stripped and the statement run against the present.
    for sql in [
        "DELETE FROM account FOR SYSTEM_TIME AS OF 1 WHERE id = 1",
        "UPDATE account SET balance = 1 FOR SYSTEM_TIME AS OF 1 WHERE id = 1",
        "INSERT INTO account FOR VALID_TIME AS OF 1 VALUES (1, 2)",
        "CREATE TABLE t (id INT) FOR SYSTEM_TIME AS OF 1",
    ] {
        assert!(
            matches!(parse(sql), Err(stele_sql::ParseError::Temporal(_))),
            "expected a temporal-grammar rejection for: {sql}"
        );
    }
}

// --- admin commands (STL-219, STL-231) ------------------------------------

#[test]
fn parses_checkpoint_and_flush_as_admin_commands() {
    // The storage admin commands have no sqlparser grammar; they are lifted
    // at the token level into an admin body (case-insensitive, trailing `;` ok).
    for (sql, want) in [
        ("CHECKPOINT", AdminCommand::Checkpoint),
        ("checkpoint;", AdminCommand::Checkpoint),
        ("FLUSH", AdminCommand::Flush),
        ("Flush ;", AdminCommand::Flush),
        ("COMPACT", AdminCommand::Compact),
        ("compact ;", AdminCommand::Compact),
    ] {
        let stmts = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
        assert_eq!(stmts.len(), 1, "{sql:?} is one statement");
        assert!(
            !stmts[0].is_temporal(),
            "{sql:?} carries no temporal grammar"
        );
        assert!(stmts[0].sql().is_none(), "{sql:?} has no SQL body");
        match &stmts[0].body {
            StatementBody::Admin(cmd) => assert_eq!(*cmd, want, "{sql:?}"),
            other => panic!("{sql:?} must be an admin command, got {other:?}"),
        }
    }
}

#[test]
fn admin_commands_take_no_arguments() {
    // A trailing token after the keyword is a hard error, not a silent strip.
    assert!(parse("CHECKPOINT 5").is_err());
    assert!(parse("FLUSH TABLES").is_err());
    assert!(parse("COMPACT t").is_err());
}

// --- user DDL (STL-252) ----------------------------------------------------

#[test]
fn parses_the_user_ddl_family() {
    // The Postgres-compatible forms, lifted at the token level (sqlparser's
    // CREATE USER grammar is Snowflake's `KEY = VALUE`, which would reject
    // `PASSWORD '…'`). Keyword case-insensitive; names verbatim.
    let cases: &[(&str, UserDdl)] = &[
        (
            "CREATE USER alice PASSWORD 's3cret'",
            UserDdl::CreateUser {
                name: "alice".to_owned(),
                password: Password("s3cret".to_owned()),
            },
        ),
        (
            "create user Bob with password 'p w';",
            UserDdl::CreateUser {
                name: "Bob".to_owned(),
                password: Password("p w".to_owned()),
            },
        ),
        (
            "ALTER USER alice WITH PASSWORD 'rotated'",
            UserDdl::AlterUserPassword {
                name: "alice".to_owned(),
                password: Password("rotated".to_owned()),
            },
        ),
        (
            "DROP USER alice",
            UserDdl::DropUser {
                name: "alice".to_owned(),
                if_exists: false,
            },
        ),
        (
            "DROP USER IF EXISTS ghost",
            UserDdl::DropUser {
                name: "ghost".to_owned(),
                if_exists: true,
            },
        ),
    ];
    for (sql, want) in cases {
        let stmts = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
        assert_eq!(stmts.len(), 1, "{sql:?} is one statement");
        assert!(stmts[0].sql().is_none(), "{sql:?} has no SQL body");
        match &stmts[0].body {
            StatementBody::User(ddl) => assert_eq!(ddl, want, "{sql:?}"),
            other => panic!("{sql:?} must be user DDL, got {other:?}"),
        }
    }
}

#[test]
fn malformed_user_ddl_is_a_loud_syntax_error() {
    // Once the `<verb> USER` prefix matches, the tail must parse exactly —
    // nothing falls through to sqlparser's Snowflake-shaped grammar.
    for sql in [
        "CREATE USER alice",                                                // no password
        "CREATE USER alice PASSWORD",                                       // dangling keyword
        "CREATE USER alice PASSWORD s3cret",                                // unquoted password
        "CREATE USER alice PASSWORD 'pw' LOGIN",                            // trailing option
        "CREATE USER alice SUPERUSER",                                      // unsupported option
        "CREATE USER alice PASSWORD ''",                                    // empty password
        "ALTER USER alice",                                                 // nothing to alter
        "DROP USER",                                                        // no name
        "DROP USER alice bob",                                              // trailing junk
        "DROP USER IF EXISTS", // no name after IF EXISTS
        "CREATE USER alice WITH PASSWORD 'pw' FOR SYSTEM_TIME AS OF now()", // temporal misuse
    ] {
        assert!(parse(sql).is_err(), "{sql:?} must be rejected");
    }
}

#[test]
fn user_ddl_password_is_redacted_in_debug_output() {
    // The parsed statement gets Debug-printed in traces and assertion
    // messages; the password literal must never surface there.
    let stmts = parse("CREATE USER alice PASSWORD 'hunter2'").expect("parses");
    let rendered = format!("{:?}", stmts[0]);
    assert!(!rendered.contains("hunter2"), "{rendered}");
    assert!(rendered.contains("<redacted>"), "{rendered}");
}

#[test]
fn quoted_user_names_are_preserved_verbatim() {
    let stmts = parse("CREATE USER \"Mixed Case\" PASSWORD 'pw'").expect("parses");
    match &stmts[0].body {
        StatementBody::User(UserDdl::CreateUser { name, .. }) => {
            assert_eq!(name, "Mixed Case");
        }
        other => panic!("expected CREATE USER, got {other:?}"),
    }
}

#[test]
fn checkpoint_mid_batch_is_one_of_several_statements() {
    // An admin command composes with ordinary statements in a `;`-separated batch.
    let stmts = parse("CREATE TABLE t (id INT) WITH SYSTEM VERSIONING; CHECKPOINT; SELECT 1")
        .expect("batch parses");
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0].sql(), Some(SqlStatement::CreateTable(_))));
    assert!(matches!(
        &stmts[1].body,
        StatementBody::Admin(AdminCommand::Checkpoint)
    ));
    assert!(matches!(stmts[2].sql(), Some(SqlStatement::Query(_))));
}

/// True if the (single) statement is a query whose first table factor carries a
/// version qualifier — i.e. `sqlparser` parsed the `AS OF` natively.
fn table_has_version(stmt: &SqlStatement) -> bool {
    let SqlStatement::Query(query) = stmt else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(|twj| {
        matches!(
            &twj.relation,
            TableFactor::Table {
                version: Some(_),
                ..
            }
        )
    })
}
