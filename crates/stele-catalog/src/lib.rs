//! Versioned catalog & metadata.
//!
//! The catalog is itself bitemporal — an `AS OF` query in the past resolves
//! columns using the schema that was in effect *then*
//! ([`docs/02-architecture.md` §5](../../../docs/02-architecture.md#5-catalog--metadata)).
//!
//! Scaffold only at v0.1; resolution against `sys_time` snapshots lands with
//! the binder in [`stele-sql`].
//!
//! ## Temporal configuration ([STL-92])
//!
//! Every table is system-versioned — that is invariant 4 of
//! [architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)
//! and is not optional. **Valid-time is the per-table opt-in:** a table either
//! tracks only system-time, or also tracks a valid-time period whose two
//! boundary columns are named at DDL time:
//!
//! ```text
//! CREATE TABLE t (...) WITH SYSTEM VERSIONING;                       -- system only
//! CREATE TABLE t (...) WITH SYSTEM VERSIONING, VALID TIME (vf, vt);  -- + valid-time
//! ```
//!
//! [`TableTemporal`] is the catalog flag this DDL populates. The DDL grammar
//! itself ([STL-95]) and the DML write path that consults the flag ([STL-94])
//! are separate tickets; this type is the metadata both sides agree on. The
//! storage write path turns [`TableTemporal::valid_time_enabled`] into its
//! require-vs-reject policy (`stele_storage::validtime`).
//!
//! ## Versioned resolution ([STL-98])
//!
//! [`Catalog`] holds, per table, a chain of [`TableSchema`] versions on the
//! system-time axis: each DDL change appends a version stamped with the system
//! time it took effect, and [`Catalog::resolve`] returns the schema in effect at
//! a given snapshot. That is what lets an `AS OF` read in the past resolve
//! columns under the schema that was live *then*. Persisting the catalog onto
//! the sealed-segment substrate and wiring [`SchemaId`] into the footer write
//! path are follow-ups; the resolution semantics live here.

#![allow(dead_code)] // scaffold for the not-yet-wired binder/DML seams ([STL-94]/[STL-95])

mod schema;
mod versioned;

pub use schema::{ColumnDef, SchemaId, TableSchema};
pub use versioned::Catalog;

/// Error raised when a catalog operation is malformed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    /// A `VALID TIME (from, to)` clause named columns that were empty or not
    /// distinct. The period needs two different, non-empty boundary columns —
    /// a column cannot be both the start and the end of its own period.
    #[error("valid-time period needs two distinct, non-empty columns (got from={0:?}, to={1:?})")]
    InvalidValidTimeColumns(String, String),

    /// A [`ColumnDef`] was given an empty name. A column needs a name to be
    /// resolvable.
    #[error("column name must be non-empty")]
    InvalidColumnName,

    /// Two columns in one schema share a name. Column names must be distinct
    /// within a schema so a reference resolves unambiguously.
    #[error("duplicate column {0:?} in schema")]
    DuplicateColumn(String),

    /// [`Catalog::create_table`] named a table that already exists.
    #[error("table {0:?} already exists")]
    TableAlreadyExists(String),

    /// An operation referenced a table not registered in the catalog.
    #[error("unknown table {0:?}")]
    UnknownTable(String),

    /// A schema change carried a system time at or before the current version's
    /// start. System time never moves backward, and a zero-width version would
    /// break the gap-free/non-overlapping interval invariant.
    #[error(
        "schema change for table {table:?} at sys_time {at} is not after the current version start {current_from}"
    )]
    NonMonotonicSchemaChange {
        /// The table whose schema change was rejected.
        table: String,
        /// The offending change's system time.
        at: i64,
        /// The current open version's start, which `at` must exceed.
        current_from: i64,
    },

    /// A schema change carried a system time at or past the open-interval
    /// sentinel (`SYSTEM_TIME_OPEN`, `i64::MAX`). That value is reserved to mark
    /// a version as still open, so a finite snapshot can never fall in
    /// `[SYSTEM_TIME_OPEN, …)` — the change would be unresolvable. Mirrors
    /// `stele-storage`'s `SysTimeError::TimeExhausted`.
    #[error("schema change for table {table:?} at sys_time {at} is at or past the open sentinel")]
    SystemTimeExhausted {
        /// The table whose schema change was rejected.
        table: String,
        /// The offending change's system time.
        at: i64,
    },
}

/// The two boundary columns of a table's valid-time period, captured from a
/// `VALID TIME (from_column, to_column)` clause.
///
/// Holds only the *names*; the column types and the values flow through the
/// storage write path (`stele_storage::validtime`). The boundary columns are
/// guaranteed distinct and non-empty by [`ValidTimeSpec::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidTimeSpec {
    from_column: String,
    to_column: String,
}

impl ValidTimeSpec {
    /// Build a spec from the two period-boundary column names.
    ///
    /// # Errors
    ///
    /// [`CatalogError::InvalidValidTimeColumns`] if either name is empty or the
    /// two names are equal — a period must span two different columns.
    pub fn new(
        from_column: impl Into<String>,
        to_column: impl Into<String>,
    ) -> Result<Self, CatalogError> {
        let from_column = from_column.into();
        let to_column = to_column.into();
        if from_column.is_empty() || to_column.is_empty() || from_column == to_column {
            return Err(CatalogError::InvalidValidTimeColumns(
                from_column,
                to_column,
            ));
        }
        Ok(Self {
            from_column,
            to_column,
        })
    }

    /// The valid-time period's start column.
    #[must_use]
    pub fn from_column(&self) -> &str {
        &self.from_column
    }

    /// The valid-time period's end column.
    #[must_use]
    pub fn to_column(&self) -> &str {
        &self.to_column
    }
}

/// A table's temporal configuration.
///
/// System-versioning is **always on** (invariant 4) and so is not represented
/// as a toggle — there is no constructor that turns it off. The only degree of
/// freedom is whether the table also opts into valid-time.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableTemporal {
    valid_time: Option<ValidTimeSpec>,
}

impl TableTemporal {
    /// System-versioned only — the default. (`WITH SYSTEM VERSIONING`.)
    #[must_use]
    pub const fn system_only() -> Self {
        Self { valid_time: None }
    }

    /// System-versioned **and** valid-time.
    /// (`WITH SYSTEM VERSIONING, VALID TIME (from, to)`.)
    #[must_use]
    pub const fn with_valid_time(spec: ValidTimeSpec) -> Self {
        Self {
            valid_time: Some(spec),
        }
    }

    /// The valid-time period columns, if this table opts in.
    #[must_use]
    pub const fn valid_time(&self) -> Option<&ValidTimeSpec> {
        self.valid_time.as_ref()
    }

    /// Whether writes to this table must supply a valid-time interval. This is
    /// the policy the storage write path enforces: `true` ⇒ a write *must*
    /// carry a `[valid_from, valid_to)` pair; `false` ⇒ it must *not*.
    #[must_use]
    pub const fn valid_time_enabled(&self) -> bool {
        self.valid_time.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_only_table_does_not_enable_valid_time() {
        let t = TableTemporal::system_only();
        assert!(!t.valid_time_enabled());
        assert!(t.valid_time().is_none());
        assert_eq!(t, TableTemporal::default());
    }

    #[test]
    fn valid_time_table_carries_its_period_columns() {
        let spec = ValidTimeSpec::new("valid_from", "valid_to").unwrap();
        let t = TableTemporal::with_valid_time(spec);
        assert!(t.valid_time_enabled());
        let cols = t.valid_time().expect("opted in");
        assert_eq!(cols.from_column(), "valid_from");
        assert_eq!(cols.to_column(), "valid_to");
    }

    #[test]
    fn period_columns_must_be_non_empty_and_distinct() {
        assert_eq!(
            ValidTimeSpec::new("", "valid_to"),
            Err(CatalogError::InvalidValidTimeColumns(
                String::new(),
                "valid_to".to_string()
            ))
        );
        assert_eq!(
            ValidTimeSpec::new("valid_from", ""),
            Err(CatalogError::InvalidValidTimeColumns(
                "valid_from".to_string(),
                String::new()
            ))
        );
        assert_eq!(
            ValidTimeSpec::new("vt", "vt"),
            Err(CatalogError::InvalidValidTimeColumns(
                "vt".to_string(),
                "vt".to_string()
            ))
        );
    }
}
