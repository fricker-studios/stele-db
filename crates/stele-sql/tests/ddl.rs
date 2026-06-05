//! DDL binding + apply tests (STL-95).
//!
//! Pins the v0.1 DDL surface: the identity-demo `CREATE TABLE` and a logical
//! `DROP TABLE` bind to catalog mutations, and everything outside the surface is
//! rejected with a clear message rather than silently mis-bound. The end-to-end
//! cases run parse → bind → apply against a real `Catalog`, the catalog-level
//! stand-in for the eventual `psql` `\d account` (wired through pg-wire as a
//! follow-up).

use stele_catalog::Catalog;
use stele_common::time::SystemTimeMicros;
use stele_common::types::LogicalType;
use stele_sql::{BindError, DdlOutcome, DdlStatement, bind_ddl, parse};

/// Parse exactly one statement and bind it as DDL.
fn bind_one(sql: &str) -> Result<DdlStatement, BindError> {
    let mut stmts = parse(sql).expect("input should parse");
    assert_eq!(stmts.len(), 1, "expected a single statement");
    bind_ddl(&stmts.remove(0))
}

#[test]
fn binds_the_identity_demo_create_table() {
    let ddl =
        bind_one("CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING")
            .expect("identity-demo CREATE TABLE should bind");
    let DdlStatement::CreateTable {
        name,
        columns,
        temporal,
    } = ddl
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(name, "account");
    // PRIMARY KEY is accepted but does not appear as anything extra — just the
    // column, lowered to the catalog type set.
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name(), "id");
    assert_eq!(columns[0].ty(), LogicalType::Int4);
    assert_eq!(columns[1].name(), "balance");
    assert_eq!(columns[1].ty(), LogicalType::Int4);
    // WITH SYSTEM VERSIONING only → system-time, no valid-time opt-in.
    assert!(!temporal.valid_time_enabled());
}

#[test]
fn binds_valid_time_opt_in_with_declared_timestamp_columns() {
    let ddl = bind_one(
        "CREATE TABLE booking (id INT, valid_from TIMESTAMP, valid_to TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (valid_from, valid_to)",
    )
    .expect("valid-time CREATE TABLE should bind");
    let DdlStatement::CreateTable { temporal, .. } = ddl else {
        panic!("expected CreateTable");
    };
    let spec = temporal.valid_time().expect("opted into valid-time");
    assert_eq!(spec.from_column(), "valid_from");
    assert_eq!(spec.to_column(), "valid_to");
}

#[test]
fn create_table_without_system_versioning_is_rejected() {
    assert!(matches!(
        bind_one("CREATE TABLE t (id INT)"),
        Err(BindError::MissingSystemVersioning)
    ));
}

#[test]
fn valid_time_naming_an_unknown_column_is_rejected() {
    let err = bind_one(
        "CREATE TABLE t (id INT, vf TIMESTAMP) WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    )
    .expect_err("vt is not a column");
    assert!(
        matches!(&err, BindError::ValidTimeColumnUnknown(c) if c.as_str() == "vt"),
        "got {err:?}"
    );
}

#[test]
fn valid_time_on_a_non_timestamp_column_is_rejected() {
    let err = bind_one(
        "CREATE TABLE t (id INT, vf INT, vt TIMESTAMP) WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    )
    .expect_err("vf is INT, not TIMESTAMP");
    assert!(
        matches!(
            &err,
            BindError::ValidTimeColumnNotTimestamp { column, ty }
                if column.as_str() == "vf" && *ty == LogicalType::Int4
        ),
        "got {err:?}"
    );
}

#[test]
fn unsupported_column_constraints_are_rejected_pointing_at_the_roadmap() {
    // FOREIGN KEY / REFERENCES, UNIQUE, CHECK, NOT NULL, DEFAULT — none enforced
    // in v0.1, so each is rejected rather than silently accepted.
    for sql in [
        "CREATE TABLE t (id INT REFERENCES other(id)) WITH SYSTEM VERSIONING",
        "CREATE TABLE t (id INT UNIQUE) WITH SYSTEM VERSIONING",
        "CREATE TABLE t (id INT CHECK (id > 0)) WITH SYSTEM VERSIONING",
        "CREATE TABLE t (id INT NOT NULL) WITH SYSTEM VERSIONING",
        "CREATE TABLE t (id INT DEFAULT 1) WITH SYSTEM VERSIONING",
    ] {
        let err = bind_one(sql).expect_err(sql);
        assert!(matches!(err, BindError::Unsupported(_)), "{sql} → {err:?}");
        assert!(
            err.to_string().contains("docs/03-roadmap.md"),
            "message should point at the roadmap: {err}"
        );
    }
}

#[test]
fn table_level_constraints_and_ctas_and_qualified_names_are_rejected() {
    assert!(matches!(
        bind_one("CREATE TABLE t (id INT, PRIMARY KEY (id)) WITH SYSTEM VERSIONING"),
        Err(BindError::Unsupported(_))
    ));
    assert!(matches!(
        bind_one("CREATE TABLE t AS SELECT 1"),
        Err(BindError::Unsupported(_))
    ));
    let qualified = bind_one("CREATE TABLE public.t (id INT) WITH SYSTEM VERSIONING")
        .expect_err("qualified name");
    assert!(matches!(qualified, BindError::QualifiedName(_)));
    // The diagnostic must not read as if bare names were the unsupported ones.
    let msg = qualified.to_string();
    assert!(
        msg.contains("only bare names are supported")
            && msg.contains("qualified names are not supported"),
        "confusing qualified-name diagnostic: {msg}"
    );
}

#[test]
fn an_unsupported_column_type_is_rejected() {
    assert!(matches!(
        bind_one("CREATE TABLE t (name VARCHAR) WITH SYSTEM VERSIONING"),
        Err(BindError::Type(_))
    ));
}

#[test]
fn binds_drop_table_and_rejects_cascade_and_multi_drop() {
    let ddl = bind_one("DROP TABLE account").expect("DROP TABLE should bind");
    assert_eq!(
        ddl,
        DdlStatement::DropTable {
            name: "account".to_owned(),
            if_exists: false,
        }
    );
    assert!(matches!(
        bind_one("DROP TABLE IF EXISTS account"),
        Ok(DdlStatement::DropTable {
            if_exists: true,
            ..
        })
    ));
    assert!(matches!(
        bind_one("DROP TABLE a CASCADE"),
        Err(BindError::Unsupported(_))
    ));
    assert!(matches!(
        bind_one("DROP TABLE a, b"),
        Err(BindError::Unsupported(_))
    ));
}

#[test]
fn non_ddl_statements_are_not_ddl() {
    assert!(matches!(bind_one("SELECT 1"), Err(BindError::NotDdl)));
}

/// The end-to-end Definition-of-Done at the catalog level: a parsed
/// `CREATE TABLE` applies, the table then resolves with the expected columns
/// (the `\d account` stand-in), a `DROP TABLE` logically removes it, and the
/// pre-drop history still resolves.
#[test]
fn create_then_describe_then_drop_round_trips_against_the_catalog() {
    let mut cat = Catalog::new();
    let create_at = SystemTimeMicros(100);

    let outcome =
        bind_one("CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING")
            .expect("bind create")
            .apply(&mut cat, create_at)
            .expect("apply create");
    assert!(matches!(outcome, DdlOutcome::Created(_)));
    assert_eq!(outcome.command_tag(), "CREATE TABLE");

    // `\d account`, catalog-level: the table resolves at the creation snapshot
    // with the columns and Postgres OIDs a describe would render.
    let schema = cat
        .resolve("account", create_at)
        .expect("account resolves after creation");
    let cols = schema.columns();
    assert_eq!(cols.len(), 2);
    assert_eq!((cols[0].name(), cols[0].ty().pg_oid()), ("id", 23));
    assert_eq!((cols[1].name(), cols[1].ty().pg_oid()), ("balance", 23));

    // Logical drop at a later instant.
    let drop_at = SystemTimeMicros(200);
    let dropped = bind_one("DROP TABLE account")
        .expect("bind drop")
        .apply(&mut cat, drop_at)
        .expect("apply drop");
    assert!(matches!(dropped, DdlOutcome::Dropped(_)));

    // Gone now; still there in the past — a transition, not a deletion.
    assert!(cat.resolve("account", drop_at).is_none());
    assert!(cat.resolve("account", create_at).is_some());
}

#[test]
fn drop_table_if_exists_on_an_absent_table_is_a_no_op() {
    let mut cat = Catalog::new();
    let outcome = bind_one("DROP TABLE IF EXISTS ghost")
        .expect("bind drop-if-exists")
        .apply(&mut cat, SystemTimeMicros(1))
        .expect("apply is a no-op, not an error");
    assert_eq!(outcome, DdlOutcome::DropNoOp);
    assert_eq!(outcome.command_tag(), "DROP TABLE");
}

#[test]
fn dropping_an_absent_table_without_if_exists_errors() {
    let mut cat = Catalog::new();
    let err = bind_one("DROP TABLE ghost")
        .expect("bind drop")
        .apply(&mut cat, SystemTimeMicros(1))
        .expect_err("no such table");
    // Surfaces the catalog's unknown-table error.
    assert!(err.to_string().contains("ghost"), "got {err}");
}
