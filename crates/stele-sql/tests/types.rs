//! Type-lowering tests: SQL surface types → `stele-common` `LogicalType`.
//!
//! Pins the v0.1 vocabulary (the STL-96 type set) and that out-of-set spellings
//! are rejected rather than silently coerced. The end-to-end path is driven from
//! a parsed `CREATE TABLE`, the shape STL-98 (catalog) will consume.

use sqlparser::ast::{DataType, Statement as SqlStatement};
use stele_common::types::LogicalType;
use stele_sql::{ParseError, logical_type, parse};

#[test]
fn v0_1_vocabulary_lowers() {
    let cases = [
        ("INT", LogicalType::Int4),
        ("INTEGER", LogicalType::Int4),
        ("BIGINT", LogicalType::Int8),
        ("TEXT", LogicalType::Text),
        ("BOOL", LogicalType::Bool),
        ("BOOLEAN", LogicalType::Bool),
        ("TIMESTAMP", LogicalType::Timestamp),
        ("TIMESTAMP WITHOUT TIME ZONE", LogicalType::Timestamp),
        ("TIMESTAMP WITH TIME ZONE", LogicalType::TimestampTz),
        ("TIMESTAMPTZ", LogicalType::TimestampTz),
        ("DATE", LogicalType::Date),
        // The character-varying family is Text under the hood; a declared
        // length is accepted as documentation, not enforced.
        ("VARCHAR", LogicalType::Text),
        ("VARCHAR(10)", LogicalType::Text),
        ("CHARACTER VARYING(10)", LogicalType::Text),
        ("NVARCHAR(10)", LogicalType::Text),
    ];
    for (sql_ty, expected) in cases {
        let dt = column_type(sql_ty);
        assert_eq!(
            logical_type(&dt).unwrap(),
            expected,
            "{sql_ty} should lower to {expected:?}"
        );
    }
}

#[test]
fn out_of_vocabulary_types_are_rejected() {
    // CHAR(n) blank-pads (Text cannot honor that); REAL has no column type.
    for sql_ty in ["CHAR(3)", "REAL"] {
        let dt = column_type(sql_ty);
        assert!(
            matches!(logical_type(&dt), Err(ParseError::UnsupportedType(_))),
            "{sql_ty} should be unsupported"
        );
    }
}

#[test]
fn lowers_every_column_of_the_demo_create_table() {
    // The identity-demo table: both columns resolve to Int4.
    let stmts =
        parse("CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING")
            .unwrap();
    let Some(SqlStatement::CreateTable(ct)) = stmts[0].sql() else {
        panic!("expected CREATE TABLE");
    };
    let resolved: Vec<LogicalType> = ct
        .columns
        .iter()
        .map(|c| logical_type(&c.data_type).unwrap())
        .collect();
    assert_eq!(resolved, [LogicalType::Int4, LogicalType::Int4]);
}

/// Parse `CREATE TABLE t (c <sql_ty>)` and return column `c`'s `DataType`.
fn column_type(sql_ty: &str) -> DataType {
    let sql = format!("CREATE TABLE t (c {sql_ty})");
    let stmts = parse(&sql).expect("create table should parse");
    let Some(SqlStatement::CreateTable(ct)) = stmts[0].sql() else {
        panic!("expected CREATE TABLE");
    };
    ct.columns[0].data_type.clone()
}
