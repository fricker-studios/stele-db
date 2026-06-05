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
        ("DATE", LogicalType::Date),
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
    for sql_ty in ["VARCHAR(10)", "CHAR(3)", "REAL", "TIMESTAMP WITH TIME ZONE"] {
        let dt = column_type(sql_ty);
        assert!(
            matches!(logical_type(&dt), Err(ParseError::UnsupportedType(_))),
            "{sql_ty} should be unsupported in v0.1"
        );
    }
}

#[test]
fn lowers_every_column_of_the_demo_create_table() {
    // The identity-demo table: both columns resolve to Int4.
    let stmts =
        parse("CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING")
            .unwrap();
    let SqlStatement::CreateTable(ct) = &stmts[0].body else {
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
    let SqlStatement::CreateTable(ct) = &stmts[0].body else {
        panic!("expected CREATE TABLE");
    };
    ct.columns[0].data_type.clone()
}
