//! DDL binding: lower a parsed `CREATE TABLE` / `DROP TABLE` into a catalog
//! mutation.
//!
//! This is the seam the catalog's doc comment calls out — "wired in by the
//! binder/DML in [STL-95]". [`parse`](crate::parse) produces a [`Statement`]
//! pairing the standard-SQL [`body`](Statement::body) with Stele's
//! [`temporal`](Statement::temporal) annotations; [`bind_ddl`] turns the DDL
//! subset of that into a typed [`DdlStatement`] whose
//! [`apply`](DdlStatement::apply) effects the change against a [`Catalog`].
//!
//! ## What v0.1 binds
//!
//! * `CREATE TABLE <name> (<cols>) WITH SYSTEM VERSIONING [, VALID TIME (f, t)]`
//!   — every table is system-versioned (invariant 4), so `WITH SYSTEM
//!   VERSIONING` is **required**; a bare `CREATE TABLE` is rejected rather than
//!   silently given semantics it did not ask for. `VALID TIME (f, t)` opts the
//!   table into a valid-time period; its two columns must be declared in the
//!   table and typed `TIMESTAMP`.
//! * `DROP TABLE <name>` — a **logical** drop ([`Catalog::drop_table`]): a
//!   catalog version transition, never a segment deletion.
//!
//! ## What v0.1 rejects (with a message pointing at the roadmap)
//!
//! Column and table constraints other than a column-level `PRIMARY KEY`
//! (`FOREIGN KEY`/`REFERENCES`, `UNIQUE`, `CHECK`, `NOT NULL`, `DEFAULT`, …),
//! indexes, `CREATE TABLE … AS SELECT`, `LIKE`/`CLONE`, `IF NOT EXISTS`,
//! `OR REPLACE`, temporary/external tables, schema-qualified names, and
//! `DROP … CASCADE`. `PRIMARY KEY` is **accepted but not enforced** in v0.1
//! (uniqueness/indexing is a later ticket) so the identity-demo
//! `CREATE TABLE account (id INT PRIMARY KEY, balance INT) …` binds.

use sqlparser::ast::{
    ColumnDef as SqlColumnDef, ColumnOption, CreateTable, ObjectName, ObjectType,
    Statement as SqlStatement,
};
use stele_catalog::{Catalog, CatalogError, ColumnDef, SchemaId, TableTemporal, ValidTimeSpec};
use stele_common::time::SystemTimeMicros;
use stele_common::types::LogicalType;

use crate::ast::Statement;
use crate::error::ParseError;
use crate::types::logical_type;

/// A roadmap pointer appended to "not in v0.1 yet" rejections.
const ROADMAP: &str = "not supported in v0.1 (see docs/03-roadmap.md)";

/// Why binding a parsed statement into a [`DdlStatement`] failed.
///
/// Distinct from [`ParseError`]: the input tokenized and parsed as valid SQL,
/// but carries DDL Stele does not (yet) implement, or is not DDL at all.
#[derive(Debug, thiserror::Error)]
pub enum BindError {
    /// The statement is not a DDL statement [`bind_ddl`] handles (e.g. a
    /// `SELECT`, `INSERT`). The caller routes those elsewhere.
    #[error("not a DDL statement")]
    NotDdl,

    /// A `CREATE TABLE` omitted the required `WITH SYSTEM VERSIONING` clause.
    /// Every Stele table is system-versioned (invariant 4); the clause is
    /// mandatory so the intent is explicit rather than silently assumed.
    #[error(
        "CREATE TABLE requires `WITH SYSTEM VERSIONING` (every Stele table is system-versioned)"
    )]
    MissingSystemVersioning,

    /// A clause, constraint, or option outside the v0.1 DDL surface.
    #[error("{0} — {ROADMAP}")]
    Unsupported(String),

    /// A schema- or database-qualified name (`schema.table`). v0.1 has a single
    /// implicit namespace, so only bare names resolve unambiguously.
    #[error("qualified table name {0:?} — only bare names are {ROADMAP}")]
    QualifiedName(String),

    /// A `CREATE TABLE` declared no columns.
    #[error("CREATE TABLE {0:?} declares no columns")]
    NoColumns(String),

    /// A `VALID TIME (from, to)` named a column the table does not declare.
    #[error("VALID TIME column {0:?} is not a column of the table")]
    ValidTimeColumnUnknown(String),

    /// A `VALID TIME` boundary column is not a `TIMESTAMP`. Valid-time period
    /// bounds are instants, so both columns must be timestamps.
    #[error("VALID TIME column {column:?} must be TIMESTAMP, found {ty}")]
    ValidTimeColumnNotTimestamp {
        /// The offending boundary column.
        column: String,
        /// The type it was actually declared with.
        ty: LogicalType,
    },

    /// A column declared a type outside Stele's v0.1 vocabulary.
    #[error(transparent)]
    Type(#[from] ParseError),

    /// The catalog rejected a name the binder built — a malformed valid-time
    /// period or a duplicate/empty column name.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

/// A bound DDL statement, ready to [`apply`](Self::apply) to a [`Catalog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlStatement {
    /// `CREATE TABLE` — register a new (or re-create a dropped) table.
    CreateTable {
        /// The table name.
        name: String,
        /// The columns, in declaration order, lowered to the catalog type set.
        columns: Vec<ColumnDef>,
        /// The table's temporal configuration (system-only or + valid-time).
        temporal: TableTemporal,
    },
    /// `DROP TABLE` — logically drop a table.
    DropTable {
        /// The table name.
        name: String,
        /// `IF EXISTS` was given — dropping an absent table is then a no-op
        /// rather than an error.
        if_exists: bool,
    },
}

/// What [`DdlStatement::apply`] did to the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlOutcome {
    /// A table was created (or a dropped name re-created) under this schema id.
    Created(SchemaId),
    /// A table was logically dropped; this is the id of the version that closed.
    Dropped(SchemaId),
    /// A `DROP TABLE IF EXISTS` named a table that did not exist — a no-op.
    DropNoOp,
}

impl DdlOutcome {
    /// The Postgres `CommandComplete` tag a wire client expects for this DDL —
    /// the seam the pgwire front end reads when it routes DDL (STL-95 follow-up).
    #[must_use]
    pub const fn command_tag(self) -> &'static str {
        match self {
            Self::Created(_) => "CREATE TABLE",
            Self::Dropped(_) | Self::DropNoOp => "DROP TABLE",
        }
    }
}

/// Bind a parsed [`Statement`] into a [`DdlStatement`].
///
/// # Errors
///
/// [`BindError::NotDdl`] if the statement is not a `CREATE TABLE` / `DROP TABLE`;
/// otherwise a [`BindError`] variant describing the unsupported or malformed DDL.
pub fn bind_ddl(stmt: &Statement) -> Result<DdlStatement, BindError> {
    match &stmt.body {
        SqlStatement::CreateTable(create) => bind_create_table(create, stmt),
        SqlStatement::Drop {
            object_type: ObjectType::Table,
            if_exists,
            names,
            cascade,
            restrict: _,
            purge,
            temporary,
            table: _,
        } => {
            // Reject the drop-behavior modifiers v0.1 does not honor before
            // looking at the targets.
            if *cascade {
                return Err(BindError::Unsupported("DROP TABLE … CASCADE".to_owned()));
            }
            if *purge {
                return Err(BindError::Unsupported("DROP TABLE … PURGE".to_owned()));
            }
            if *temporary {
                return Err(BindError::Unsupported("DROP TEMPORARY TABLE".to_owned()));
            }
            bind_drop_table(names, *if_exists)
        }
        // A DROP of some other object kind is DDL we don't implement.
        SqlStatement::Drop { object_type, .. } => Err(BindError::Unsupported(format!(
            "DROP {object_type} (only DROP TABLE is supported)"
        ))),
        _ => Err(BindError::NotDdl),
    }
}

fn bind_create_table(create: &CreateTable, stmt: &Statement) -> Result<DdlStatement, BindError> {
    reject_unsupported_create_modifiers(create)?;

    if !stmt.temporal.system_versioning {
        return Err(BindError::MissingSystemVersioning);
    }

    let name = bare_name(&create.name)?;

    if create.columns.is_empty() {
        return Err(BindError::NoColumns(name));
    }
    let columns = create
        .columns
        .iter()
        .map(bind_column)
        .collect::<Result<Vec<_>, _>>()?;

    let temporal = bind_temporal(stmt, &columns)?;

    Ok(DdlStatement::CreateTable {
        name,
        columns,
        temporal,
    })
}

/// Reject the `CREATE TABLE` forms outside the v0.1 surface. Only the realistic
/// Postgres-style spellings are checked; the exotic dialect flags cannot be
/// produced through Stele's Postgres-leaning dialect.
fn reject_unsupported_create_modifiers(create: &CreateTable) -> Result<(), BindError> {
    let unsupported = |what: &str| Err(BindError::Unsupported(what.to_owned()));
    if create.or_replace {
        return unsupported("CREATE OR REPLACE TABLE");
    }
    if create.if_not_exists {
        return unsupported("CREATE TABLE IF NOT EXISTS");
    }
    if create.temporary {
        return unsupported("CREATE TEMPORARY TABLE");
    }
    if create.external {
        return unsupported("CREATE EXTERNAL TABLE");
    }
    if create.query.is_some() {
        return unsupported("CREATE TABLE … AS SELECT");
    }
    if create.like.is_some() || create.clone.is_some() {
        return unsupported("CREATE TABLE … LIKE/CLONE");
    }
    if !create.constraints.is_empty() {
        return unsupported("table-level constraints (PRIMARY KEY/FOREIGN KEY/UNIQUE/CHECK)");
    }
    Ok(())
}

/// Lower one parsed column to a catalog [`ColumnDef`], accepting only a
/// column-level `PRIMARY KEY` option (parsed, but not enforced in v0.1) and
/// rejecting every other constraint.
fn bind_column(col: &SqlColumnDef) -> Result<ColumnDef, BindError> {
    for opt in &col.options {
        match &opt.option {
            // PRIMARY KEY is accepted so the identity demo binds; v0.1 does not
            // yet enforce uniqueness or build an index for it.
            ColumnOption::PrimaryKey(_) => {}
            other => {
                return Err(BindError::Unsupported(format!(
                    "column constraint `{other}` on {:?}",
                    col.name.value
                )));
            }
        }
    }
    let ty = logical_type(&col.data_type)?;
    Ok(ColumnDef::new(col.name.value.clone(), ty)?)
}

/// Build the table's temporal config from the lifted `VALID TIME` clause,
/// checking that each named boundary column is a declared `TIMESTAMP`.
fn bind_temporal(stmt: &Statement, columns: &[ColumnDef]) -> Result<TableTemporal, BindError> {
    let Some(period) = &stmt.temporal.valid_time else {
        return Ok(TableTemporal::system_only());
    };
    let from = period.from.value.as_str();
    let to = period.to.value.as_str();
    check_valid_time_column(from, columns)?;
    check_valid_time_column(to, columns)?;
    Ok(TableTemporal::with_valid_time(ValidTimeSpec::new(
        from, to,
    )?))
}

fn check_valid_time_column(name: &str, columns: &[ColumnDef]) -> Result<(), BindError> {
    let col = columns
        .iter()
        .find(|c| c.name() == name)
        .ok_or_else(|| BindError::ValidTimeColumnUnknown(name.to_owned()))?;
    if col.ty() == LogicalType::Timestamp {
        Ok(())
    } else {
        Err(BindError::ValidTimeColumnNotTimestamp {
            column: name.to_owned(),
            ty: col.ty(),
        })
    }
}

fn bind_drop_table(names: &[ObjectName], if_exists: bool) -> Result<DdlStatement, BindError> {
    let [name] = names else {
        return Err(BindError::Unsupported(
            "DROP TABLE of multiple tables in one statement".to_owned(),
        ));
    };
    Ok(DdlStatement::DropTable {
        name: bare_name(name)?,
        if_exists,
    })
}

/// Extract a single, unqualified identifier from an [`ObjectName`].
fn bare_name(name: &ObjectName) -> Result<String, BindError> {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|id| id.value.clone())
            .ok_or_else(|| BindError::QualifiedName(name.to_string())),
        _ => Err(BindError::QualifiedName(name.to_string())),
    }
}

impl DdlStatement {
    /// Apply this DDL to `catalog`, taking effect at system time `at`.
    ///
    /// `at` is supplied by the caller — the commit clock once DDL runs inside a
    /// transaction; an explicit instant in tests. It threads straight through to
    /// the catalog's system-time bookkeeping.
    ///
    /// # Errors
    ///
    /// Propagates the [`CatalogError`] the underlying
    /// [`create_table`](Catalog::create_table) / [`drop_table`](Catalog::drop_table)
    /// raises (name already live, non-monotonic time, …). A
    /// `DROP TABLE IF EXISTS` of an absent table is **not** an error — it yields
    /// [`DdlOutcome::DropNoOp`].
    pub fn apply(
        self,
        catalog: &mut Catalog,
        at: SystemTimeMicros,
    ) -> Result<DdlOutcome, CatalogError> {
        match self {
            Self::CreateTable {
                name,
                columns,
                temporal,
            } => catalog
                .create_table(name, columns, temporal, at)
                .map(DdlOutcome::Created),
            Self::DropTable { name, if_exists } => match catalog.drop_table(&name, at) {
                Ok(id) => Ok(DdlOutcome::Dropped(id)),
                Err(CatalogError::UnknownTable(_)) if if_exists => Ok(DdlOutcome::DropNoOp),
                Err(e) => Err(e),
            },
        }
    }
}
