//! Stele's parsed-statement representation.
//!
//! A [`Statement`] is a thin envelope around the underlying `sqlparser-rs`
//! [`SqlStatement`] plus the **temporal grammar** Stele layers on top — the
//! parts standard SQL (and therefore `sqlparser`) has no AST node for. The
//! envelope keeps the bootstrap honest: the bulk of the grammar rides on a
//! mature parser, while the bitemporal constructs that make Stele *Stele* get
//! first-class, typed homes the binder can act on.
//!
//! See [`docs/sql-grammar.md`](../../../docs/sql-grammar.md) for the grammar
//! these types capture and the v0.1 implementation status of each piece.

use sqlparser::ast::{Expr, Ident, Statement as SqlStatement};

/// One parsed top-level SQL statement.
#[derive(Debug, Clone)]
pub struct Statement {
    /// The underlying `sqlparser-rs` statement. Stele's non-standard temporal
    /// clauses have been stripped from the token stream before this was parsed,
    /// so it is always a clean, standard-SQL AST.
    pub body: SqlStatement,
    /// Temporal grammar captured from the clauses that were stripped (plus
    /// `FOR SYSTEM_TIME AS OF`, which rides natively on `body` but is surfaced
    /// here too, with its time dimension, for the binder's convenience).
    pub temporal: Temporal,
}

impl Statement {
    /// Whether this statement carried any Stele temporal grammar.
    pub fn is_temporal(&self) -> bool {
        self.temporal != Temporal::default()
    }
}

/// The temporal annotations Stele recognizes, captured per statement.
///
/// All fields are independently optional so the type stays forward-compatible
/// as the grammar grows; a non-temporal statement carries the [`Default`]
/// (all-empty) value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Temporal {
    /// `CREATE TABLE … WITH SYSTEM VERSIONING` was present — the table keeps a
    /// full system-time history.
    pub system_versioning: bool,
    /// `CREATE TABLE … VALID TIME (from, to)` — the pair of columns that form
    /// the table's application-time (valid-time) period, if opted in.
    pub valid_time: Option<ValidTimePeriod>,
    /// `FOR { SYSTEM_TIME | VALID_TIME } AS OF <expr>` qualifiers, one per
    /// table reference that carried one, in left-to-right source order.
    pub as_of: Vec<AsOf>,
}

/// The `(from, to)` column pair declared by `VALID TIME (from, to)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidTimePeriod {
    /// Column holding the inclusive start of the valid-time period.
    pub from: Ident,
    /// Column holding the exclusive end of the valid-time period.
    pub to: Ident,
}

/// A single `FOR <dimension> AS OF <timestamp>` table qualifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsOf {
    /// Which time axis the snapshot is taken along.
    pub dimension: TimeDimension,
    /// The snapshot instant. An arbitrary scalar expression — the binder and
    /// optimizer fold it to a concrete timestamp.
    pub timestamp: Expr,
}

/// A bitemporal time axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeDimension {
    /// `SYSTEM_TIME` — when a fact was recorded. Implemented in v0.1.
    System,
    /// `VALID_TIME` — when a fact was true in the world. Parsed in v0.1 but not
    /// yet wired through the binder/executor; see [`Self::is_implemented`].
    Valid,
}

impl TimeDimension {
    /// Whether v0.1 implements time-travel along this axis. The parser accepts
    /// both dimensions; the binder rejects [`Self::Valid`] until valid-time
    /// `AS OF` lands (tracked post-v0.1).
    pub const fn is_implemented(self) -> bool {
        matches!(self, Self::System)
    }
}
