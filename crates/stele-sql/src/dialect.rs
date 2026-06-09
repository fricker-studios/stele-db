//! The Stele SQL dialect.
//!
//! Stele starts from a conservative ANSI/Postgres-flavored baseline (the
//! `sqlparser-rs` [`Dialect`] trait defaults) and inherits its lexical rules
//! (what may start/continue an identifier, how delimited identifiers open) from
//! [`GenericDialect`] so Stele tokenizes identifiers the same way the rest of the
//! ecosystem does.
//!
//! Notably it does **not** enable `supports_table_versioning`: Stele's
//! `FOR { SYSTEM_TIME | VALID_TIME } AS OF` qualifiers are lifted out of the
//! token stream and parsed by [`crate::parse`] (a table may carry one per
//! axis — more than `sqlparser`'s single native version supports), so
//! `sqlparser` never parses a versioned table itself.
//!
//! As the grammar grows, prefer turning on individual `supports_*` flags here
//! over forking the parser ([`docs/02-architecture.md` §6]).
//!
//! [`docs/02-architecture.md` §6]: ../../../docs/02-architecture.md#6-query-layer

use sqlparser::dialect::{Dialect, GenericDialect};

/// Stele's SQL dialect: ANSI baseline plus temporal time-travel grammar.
#[derive(Debug, Default)]
pub struct SteleDialect {
    /// Lexical rules are delegated to the generic dialect.
    base: GenericDialect,
}

impl Dialect for SteleDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.base.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.base.is_identifier_part(ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.base.is_delimited_identifier_start(ch)
    }
}
