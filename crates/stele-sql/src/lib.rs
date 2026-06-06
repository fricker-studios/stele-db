//! SQL frontend — parse, bind, plan, optimize.
//!
//! Starts from `sqlparser-rs` (cheap dependency, mature) and grows
//! Stele-specific temporal grammar on top
//! ([`docs/02-architecture.md` §6](../../../docs/02-architecture.md#6-query-layer)).
//!
//! The interesting optimizer rules are **temporal-aware**: pushing an `AS OF`
//! predicate into segment-level `sys_time` zone-map pruning is what makes
//! time-travel cheap.
//!
//! ## What's here at v0.1
//!
//! The parser layer ([`parse`]). It accepts standard SQL plus Stele's temporal
//! grammar and returns [`Statement`]s that pair the underlying `sqlparser` AST
//! with typed temporal annotations:
//!
//! - `SELECT … FOR SYSTEM_TIME AS OF <expr>` — time-travel along system time.
//! - `SELECT … FOR VALID_TIME AS OF <expr>` — parsed and tagged, but
//!   [not yet implemented](ast::TimeDimension::is_implemented) downstream.
//! - `CREATE TABLE … WITH SYSTEM VERSIONING` — opt into system-time history.
//! - `CREATE TABLE … VALID TIME (from, to)` — opt into a valid-time period.
//!
//! It also lowers SQL surface types to `stele-common`'s logical type vocabulary
//! ([`logical_type`]) — the seam between parsed `CREATE TABLE` column types and
//! the catalog/executor type set.
//!
//! The DDL binder ([`bind_ddl`]) sits one step further on: it turns a parsed
//! `CREATE TABLE` / `DROP TABLE` into a [`DdlStatement`] that
//! [applies](DdlStatement::apply) to a `stele-catalog` `Catalog`, rejecting
//! constraints and clauses outside the v0.1 surface. Wiring it to the pg-wire
//! query loop is a follow-up; the parse → bind → apply path is complete and
//! tested here.
//!
//! ```
//! let stmts = stele_sql::parse(
//!     "SELECT balance FROM account \
//!      FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1",
//! )
//! .unwrap();
//! assert_eq!(stmts.len(), 1);
//! assert!(stmts[0].is_temporal());
//! ```
//!
//! The query binder ([`bind_select`]) folds a `SELECT … FOR SYSTEM_TIME AS OF
//! <expr>` into a [`BoundSelect`]: it resolves the `AS OF` to a concrete
//! system-time snapshot (`now()`, `now() ± interval '…'`, or an explicit
//! instant), defaults to the transaction snapshot when no `AS OF` is given, and
//! resolves the table against the versioned catalog at that snapshot — surfacing
//! the documented [before-history](select::SelectError::BeforeHistory) error for
//! a read older than the table. The snapshot it carries is the `sys_from ≤ s`
//! bound the executor pushes into zone-map pruning ([STL-101]).
//!
//! Planner and cost-based optimizer beyond this are still scaffold.

#![allow(dead_code)]

pub mod ast;
pub mod ddl;
pub mod dialect;
pub mod error;
mod parser;
pub mod select;
pub mod types;

pub use ast::{AsOf, Statement, Temporal, TimeDimension, ValidTimePeriod};
pub use ddl::{BindError, DdlOutcome, DdlStatement, bind_ddl};
pub use dialect::SteleDialect;
pub use error::ParseError;
pub use parser::parse;
pub use select::{
    AsOfError, BindContext, BoundSelect, Projection, SelectError, bind_select, resolve_as_of,
};
pub use types::logical_type;

// Re-exported so downstream crates (binder, planner) can name the underlying
// AST without taking their own direct dependency on a specific `sqlparser`
// version — the version is pinned here, at the seam.
pub use sqlparser;
