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
use stele_common::period::PeriodPredicate;

/// One parsed top-level SQL statement.
#[derive(Debug, Clone)]
pub struct Statement {
    /// The statement body — standard SQL, or a Stele admin command that has no
    /// `sqlparser` grammar. See [`StatementBody`].
    pub body: StatementBody,
    /// Temporal grammar captured from the clauses that were stripped — including
    /// every `FOR { SYSTEM_TIME | VALID_TIME } AS OF` qualifier, lifted off the
    /// token stream with its time dimension for the binder to act on. The SQL
    /// `body` itself is always clean standard SQL with no version qualifier; an
    /// admin command carries the [`Default`] (empty) temporal.
    pub temporal: Temporal,
}

/// The body of a parsed [`Statement`].
///
/// Almost every statement is standard SQL parsed by `sqlparser`. The exception is
/// a Stele **admin command** (`CHECKPOINT` / `FLUSH`): `sqlparser` has no grammar
/// for it, so it is recognized at the token level — the same lift discipline the
/// temporal clauses use — and represented here as its own variant rather than a
/// `sqlparser` AST node ([STL-219]).
///
/// [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
// `Sql` is the overwhelmingly common variant and wraps `sqlparser`'s already-large
// `Statement` AST; boxing it to shrink the rare 1-byte `Admin` case would add a
// heap allocation to every parsed statement on the hot path — and break the
// `const fn sql()` accessor, since `Box` deref is not const. The envelope is
// short-lived (parsed, bound, dropped), so the stack size is not a concern.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum StatementBody {
    /// A standard-SQL statement. Stele's non-standard temporal clauses have been
    /// stripped from the token stream before this was parsed, so it is always a
    /// clean, standard-SQL AST.
    Sql(SqlStatement),
    /// A Stele admin command lifted off the token stream before `sqlparser`.
    Admin(AdminCommand),
    /// User-administration DDL lifted off the token stream before `sqlparser`
    /// ([STL-252]): `sqlparser` parses `CREATE USER` / `ALTER USER` with
    /// Snowflake's `KEY = VALUE` option grammar, not the Postgres
    /// `PASSWORD '…'` form Stele speaks, so the family is recognized at the
    /// token level — the same lift discipline as [`AdminCommand`].
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    User(UserDdl),
}

/// A user-administration statement ([STL-252]) — Stele's Postgres-compatible
/// subset of the role DDL family, recognized at the token level.
///
/// Names are matched verbatim (no case-folding), as Stele does for table,
/// column, and savepoint names. The password literal rides in a [`Password`]
/// so it never reaches a log line through `Debug`.
///
/// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserDdl {
    /// `CREATE USER <name> [WITH] PASSWORD '<password>'`.
    CreateUser {
        /// The user name.
        name: String,
        /// The password to derive a SCRAM verifier from.
        password: Password,
    },
    /// `ALTER USER <name> [WITH] PASSWORD '<password>'` — rotate the password.
    AlterUserPassword {
        /// The user name.
        name: String,
        /// The replacement password.
        password: Password,
    },
    /// `DROP USER [IF EXISTS] <name>`.
    DropUser {
        /// The user name.
        name: String,
        /// `IF EXISTS` was given — dropping an absent user is then a no-op.
        if_exists: bool,
    },
}

/// A password literal in flight between the parser and the engine's verifier
/// derivation ([STL-252]).
///
/// A newtype purely so `Debug` — on the statement, a bind error, a trace span
/// — redacts it instead of echoing the secret.
///
/// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
#[derive(Clone, PartialEq, Eq)]
pub struct Password(pub String);

impl std::fmt::Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Password(<redacted>)")
    }
}

/// An operator-facing storage durability command, triggered over the wire.
///
/// Recognized at the token level (no `sqlparser` grammar) and routed by the
/// engine to the matching `SessionEngine` durability operation ([STL-219]).
///
/// [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminCommand {
    /// `CHECKPOINT` — the lightweight durability fence: fsync every table's WAL
    /// and record its fence, without sealing the delta. Maps to
    /// `SessionEngine::checkpoint`.
    Checkpoint,
    /// `FLUSH` — seal every table's delta into a segment and advance its replay
    /// floor, bounding recovery. Maps to `SessionEngine::flush`.
    Flush,
}

impl Statement {
    /// Whether this statement carried any Stele temporal grammar.
    pub fn is_temporal(&self) -> bool {
        self.temporal != Temporal::default()
    }

    /// The standard-SQL body, or `None` if this is an admin command
    /// ([`StatementBody::Admin`]) or user DDL ([`StatementBody::User`]) with no
    /// `sqlparser` AST. The binders and the wire layer's statement-shape checks
    /// read the body through this, so a lifted statement cleanly classifies as
    /// "none of the SQL routes".
    #[must_use]
    pub const fn sql(&self) -> Option<&SqlStatement> {
        match &self.body {
            StatementBody::Sql(body) => Some(body),
            StatementBody::Admin(_) | StatementBody::User(_) => None,
        }
    }

    /// The standard-SQL body for an in-place rewrite (extended-protocol parameter
    /// substitution), or `None` for an admin command or user DDL.
    #[must_use]
    pub const fn sql_mut(&mut self) -> Option<&mut SqlStatement> {
        match &mut self.body {
            StatementBody::Sql(body) => Some(body),
            StatementBody::Admin(_) | StatementBody::User(_) => None,
        }
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
    /// A `WHERE PERIOD(a, b) <pred> PERIOD(c, d)` period predicate lifted off the
    /// token stream, when the whole `WHERE` is one ([STL-165]). `sqlparser` has
    /// no grammar for the period predicates, so — like `AS OF` — they are lifted
    /// here and bound separately; `body`'s `WHERE` is left clean.
    pub period_predicate: Option<PeriodPredicateClause>,
}

/// A parsed `PERIOD(from, to) <predicate> PERIOD(from, to)` clause — the
/// SQL:2011 period predicate forms, captured before binding ([STL-165]).
///
/// Each side's `from` / `to` is an arbitrary scalar expression (the binder folds
/// each to a concrete microsecond instant, the same way `AS OF` operands fold).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodPredicateClause {
    /// The left period operand.
    pub left: PeriodExpr,
    /// Which period predicate relates the two operands.
    pub predicate: PeriodPredicate,
    /// The right period operand.
    pub right: PeriodExpr,
}

/// A `PERIOD(from, to)` operand: the two endpoint expressions of one period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodExpr {
    /// The inclusive-start expression.
    pub from: Expr,
    /// The exclusive-end expression.
    pub to: Expr,
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
    /// `SYSTEM_TIME` — when a fact was recorded.
    System,
    /// `VALID_TIME` — when a fact was true in the world.
    Valid,
}
