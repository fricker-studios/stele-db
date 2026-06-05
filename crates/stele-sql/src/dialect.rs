//! The Stele SQL dialect.
//!
//! Stele starts from a conservative ANSI/Postgres-flavored baseline (the
//! `sqlparser-rs` [`Dialect`] trait defaults) and layers on exactly the
//! behavior its temporal grammar needs. Today that is a single flag —
//! [`Dialect::supports_table_versioning`] — which unlocks native parsing of
//! `FOR SYSTEM_TIME AS OF <expr>` table qualifiers. Lexical rules (what may
//! start/continue an identifier, how delimited identifiers open) are inherited
//! from [`GenericDialect`] so Stele tokenizes identifiers the same way the rest
//! of the ecosystem does.
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

    /// Enables `FOR SYSTEM_TIME AS OF <expr>` after a table reference — the
    /// syntactic anchor of Stele's time-travel queries.
    fn supports_table_versioning(&self) -> bool {
        true
    }
}
