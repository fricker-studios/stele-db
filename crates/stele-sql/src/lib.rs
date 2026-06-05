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
//! Binder, planner, and optimizer are still scaffold.

#![allow(dead_code)]

pub mod ast;
pub mod dialect;
pub mod error;
mod parser;
pub mod types;

pub use ast::{AsOf, Statement, Temporal, TimeDimension, ValidTimePeriod};
pub use dialect::SteleDialect;
pub use error::ParseError;
pub use parser::parse;
pub use types::logical_type;

// Re-exported so downstream crates (binder, planner) can name the underlying
// AST without taking their own direct dependency on a specific `sqlparser`
// version — the version is pinned here, at the seam.
pub use sqlparser;
