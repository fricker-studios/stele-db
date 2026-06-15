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
//! ## What binds
//!
//! * `CREATE TABLE <name> (<cols>) WITH SYSTEM VERSIONING [, VALID TIME (f, t)]`
//!   — every table is system-versioned (invariant 4), so `WITH SYSTEM
//!   VERSIONING` is **required**; a bare `CREATE TABLE` is rejected rather than
//!   silently given semantics it did not ask for. `VALID TIME (f, t)` opts the
//!   table into a valid-time period; its two columns must be declared in the
//!   table and typed `TIMESTAMP`.
//! * `DROP TABLE <name>` — a **logical** drop ([`Catalog::drop_table`]): a
//!   catalog version transition, never a segment deletion.
//! * `CREATE INDEX <name> ON <table> (<column>)` / `DROP INDEX <name>` — the
//!   v0.3 secondary-index substrate ([STL-233]): a named, single-column index
//!   in the default (B-tree) kind. An index is derived, rebuildable state, so
//!   both directions are catalog/engine transitions that can change *speed*
//!   but never *results*.
//!
//! ## What is rejected (with a message pointing at the roadmap)
//!
//! Column and table constraints other than a column-level `PRIMARY KEY`
//! (`FOREIGN KEY`/`REFERENCES`, `UNIQUE`, `CHECK`, `NOT NULL`, `DEFAULT`, …),
//! `CREATE TABLE … AS SELECT`, `LIKE`/`CLONE`, `IF NOT EXISTS`,
//! `OR REPLACE`, temporary/external tables, schema-qualified names, and
//! `DROP … CASCADE`. `PRIMARY KEY` is **accepted but not enforced** in v0.1
//! (uniqueness/indexing is a later ticket) so the identity-demo
//! `CREATE TABLE account (id INT PRIMARY KEY, balance INT) …` binds. On the
//! index side: the bare/`USING BTREE` ordered kind and the `USING HASH`
//! equality kind ([STL-238]) bind; `UNIQUE`, other `USING` kinds, multi-column,
//! expression, and partial (`WHERE`) indexes are rejected until their sibling
//! tickets land — as are
//! `CONCURRENTLY`, `IF NOT EXISTS`, `INCLUDE`, `NULLS [NOT] DISTINCT`,
//! `WITH (…)`, per-column `ASC`/`DESC`/`NULLS` ordering and operator classes,
//! an unnamed `CREATE INDEX`, and `DROP INDEX … CASCADE` / multi-index drops.
//!
//! [STL-233]: https://allegromusic.atlassian.net/browse/STL-233

use sqlparser::ast::{
    ColumnDef as SqlColumnDef, ColumnOption, CreateIndex, CreateTable, Expr, IndexColumn,
    IndexType as SqlIndexType, ObjectName, ObjectType, Statement as SqlStatement,
};
use stele_catalog::{
    Catalog, CatalogError, ColumnDef, IndexDef, IndexKind, SchemaId, TableTemporal, ValidTimeSpec,
};
use stele_common::time::SystemTimeMicros;
use stele_common::types::LogicalType;

use crate::ast::{Password, Statement, StatementBody, UserDdl};
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
    #[error(
        "qualified table name {0:?} — only bare names are supported; qualified names are {ROADMAP}"
    )]
    QualifiedName(String),

    /// A single-part table name that is not a plain identifier (e.g. a
    /// dialect-specific function-valued name part). Not *qualified* — just not a
    /// name v0.1 can use.
    #[error("table name {0:?} is not a plain identifier")]
    InvalidTableName(String),

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
    /// `CREATE INDEX <name> ON <table> (<column>)` — register a secondary
    /// index ([STL-233]). The substrate binds the single-column form in the
    /// default B-tree kind or the equality-only hash kind (`USING HASH`,
    /// [STL-238]); multi-column, `UNIQUE`, and partial indexes are later tickets.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    /// [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
    CreateIndex {
        /// The index name — unique across the live index set.
        name: String,
        /// The table the index accelerates.
        table: String,
        /// The access-structure family — B-tree (default) or hash (`USING HASH`).
        kind: IndexKind,
        /// The indexed value column names, in declaration order.
        columns: Vec<String>,
    },
    /// `DROP INDEX <name>` — remove a secondary index ([STL-233]). Purely a
    /// catalog/engine transition: an index is derived state, so nothing
    /// historical is lost.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    DropIndex {
        /// The index name.
        name: String,
        /// `IF EXISTS` was given — dropping an absent index is then a no-op
        /// rather than an error.
        if_exists: bool,
    },
    /// `CREATE USER <name> PASSWORD '…'` — register a user; the engine derives
    /// and durably stores a SCRAM verifier, never the password ([STL-252]).
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    CreateUser {
        /// The user name.
        name: String,
        /// The password to derive the verifier from (redacted `Debug`).
        password: Password,
    },
    /// `ALTER USER <name> PASSWORD '…'` — rotate a user's password
    /// ([STL-252]): a fresh salt and verifier replace the stored one.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    AlterUserPassword {
        /// The user name.
        name: String,
        /// The replacement password (redacted `Debug`).
        password: Password,
    },
    /// `DROP USER [IF EXISTS] <name>` — remove a user ([STL-252]). Existing
    /// connections are unaffected; the next authentication attempt is refused.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    DropUser {
        /// The user name.
        name: String,
        /// `IF EXISTS` was given — dropping an absent user is then a no-op
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
    /// A secondary index was registered ([STL-233]).
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    CreatedIndex,
    /// A secondary index was removed ([STL-233]).
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    DroppedIndex,
    /// A `DROP INDEX IF EXISTS` named an index that did not exist — a no-op.
    DropIndexNoOp,
    /// A user was registered ([STL-252]).
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    CreatedUser,
    /// A user's password (verifier) was rotated ([STL-252]).
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    AlteredUser,
    /// A user was removed ([STL-252]).
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    DroppedUser,
    /// A `DROP USER IF EXISTS` named a user that did not exist — a no-op.
    DropUserNoOp,
}

impl DdlOutcome {
    /// The Postgres `CommandComplete` tag a wire client expects for this DDL —
    /// the seam the pgwire front end reads when it routes DDL (STL-95 follow-up).
    /// User DDL reports the `ROLE` tags, exactly as Postgres does for its
    /// `CREATE`/`ALTER`/`DROP USER` aliases.
    #[must_use]
    pub const fn command_tag(self) -> &'static str {
        match self {
            Self::Created(_) => "CREATE TABLE",
            Self::Dropped(_) | Self::DropNoOp => "DROP TABLE",
            Self::CreatedIndex => "CREATE INDEX",
            Self::DroppedIndex | Self::DropIndexNoOp => "DROP INDEX",
            Self::CreatedUser => "CREATE ROLE",
            Self::AlteredUser => "ALTER ROLE",
            Self::DroppedUser | Self::DropUserNoOp => "DROP ROLE",
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
    // Token-lifted user DDL ([STL-252]) carries no `sqlparser` AST; the parser
    // already validated its shape, so binding is a direct mapping.
    if let StatementBody::User(user) = &stmt.body {
        return Ok(match user {
            UserDdl::CreateUser { name, password } => DdlStatement::CreateUser {
                name: name.clone(),
                password: password.clone(),
            },
            UserDdl::AlterUserPassword { name, password } => DdlStatement::AlterUserPassword {
                name: name.clone(),
                password: password.clone(),
            },
            UserDdl::DropUser { name, if_exists } => DdlStatement::DropUser {
                name: name.clone(),
                if_exists: *if_exists,
            },
        });
    }

    // An admin command (CHECKPOINT / FLUSH) has no SQL body, so it is "not DDL".
    let Some(body) = stmt.sql() else {
        return Err(BindError::NotDdl);
    };
    match body {
        SqlStatement::CreateTable(create) => bind_create_table(create, stmt),
        SqlStatement::CreateIndex(create) => bind_create_index(create),
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
        SqlStatement::Drop {
            object_type: ObjectType::Index,
            if_exists,
            names,
            cascade,
            ..
        } => {
            if *cascade {
                return Err(BindError::Unsupported("DROP INDEX … CASCADE".to_owned()));
            }
            bind_drop_index(names, *if_exists)
        }
        // A DROP of some other object kind is DDL we don't implement.
        SqlStatement::Drop { object_type, .. } => Err(BindError::Unsupported(format!(
            "DROP {object_type} (only DROP TABLE / DROP INDEX is supported)"
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

/// Lower a parsed `CREATE INDEX` to the substrate surface: a named,
/// single-column index in the default B-tree kind or the hash kind
/// (`USING HASH`, [STL-238]). Everything richer — other `USING` kinds,
/// multi-column, `UNIQUE`, partial (`WHERE`), `INCLUDE`, expression columns — is
/// rejected with a roadmap pointer; the sibling index tickets lift those as
/// their access structures land.
///
/// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
/// [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
fn bind_create_index(create: &CreateIndex) -> Result<DdlStatement, BindError> {
    let unsupported = |what: &str| Err(BindError::Unsupported(what.to_owned()));
    if create.unique {
        return unsupported("CREATE UNIQUE INDEX");
    }
    if create.concurrently {
        return unsupported("CREATE INDEX CONCURRENTLY");
    }
    if create.if_not_exists {
        return unsupported("CREATE INDEX IF NOT EXISTS");
    }
    // The access method selects the structure: bare / `USING BTREE` is the
    // ordered default ([STL-237]); `USING HASH` is the equality-only hash family
    // ([STL-238]). Any other method (GIN, GiST, …) has no structure yet.
    let kind = match &create.using {
        None | Some(SqlIndexType::BTree) => IndexKind::BTree,
        Some(SqlIndexType::Hash) => IndexKind::Hash,
        Some(other) => return unsupported(&format!("CREATE INDEX … USING {other:?}")),
    };
    if !create.include.is_empty() {
        return unsupported("CREATE INDEX … INCLUDE");
    }
    if create.nulls_distinct.is_some() {
        return unsupported("CREATE INDEX … NULLS [NOT] DISTINCT");
    }
    if !create.with.is_empty() {
        return unsupported("CREATE INDEX … WITH (…)");
    }
    if create.predicate.is_some() {
        return unsupported("partial index (CREATE INDEX … WHERE)");
    }
    if !create.index_options.is_empty() || !create.alter_options.is_empty() {
        return unsupported("CREATE INDEX options");
    }
    // `DROP INDEX` resolves by name alone, so an anonymous index would be
    // undroppable; require the name rather than synthesizing one.
    let Some(name) = &create.name else {
        return unsupported("CREATE INDEX without an index name");
    };
    let name = bare_name(name)?;
    let table = bare_name(&create.table_name)?;
    // The substrate indexes exactly one column; the metadata carries a list so
    // multi-column is an extension, not a format change.
    let [column] = create.columns.as_slice() else {
        return unsupported("multi-column CREATE INDEX");
    };
    Ok(DdlStatement::CreateIndex {
        name,
        table,
        kind,
        columns: vec![index_column(column)?],
    })
}

/// Extract a plain column name from one parsed index column, rejecting the
/// per-column decorations (`ASC`/`DESC`, `NULLS FIRST`, operator classes,
/// expression columns) the substrate does not honor.
fn index_column(column: &IndexColumn) -> Result<String, BindError> {
    let unsupported = |what: &str| Err(BindError::Unsupported(what.to_owned()));
    if column.operator_class.is_some() {
        return unsupported("an operator class on an index column");
    }
    let order = &column.column;
    if order.options.asc.is_some() || order.options.nulls_first.is_some() {
        return unsupported("ASC/DESC/NULLS ordering on an index column");
    }
    if order.with_fill.is_some() {
        return unsupported("WITH FILL on an index column");
    }
    match &order.expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        other => Err(BindError::Unsupported(format!(
            "index over an expression `{other}` (plain columns only)"
        ))),
    }
}

fn bind_drop_index(names: &[ObjectName], if_exists: bool) -> Result<DdlStatement, BindError> {
    let [name] = names else {
        return Err(BindError::Unsupported(
            "DROP INDEX of multiple indexes in one statement".to_owned(),
        ));
    };
    Ok(DdlStatement::DropIndex {
        name: bare_name(name)?,
        if_exists,
    })
}

/// Extract a single, unqualified identifier from an [`ObjectName`].
fn bare_name(name: &ObjectName) -> Result<String, BindError> {
    match name.0.as_slice() {
        // A lone identifier part is the only accepted form. A single *non*-ident
        // part (e.g. a dialect-specific function-valued name) is malformed, not
        // qualified — keep the two diagnostics distinct.
        [part] => part
            .as_ident()
            .map(|id| id.value.clone())
            .ok_or_else(|| BindError::InvalidTableName(name.to_string())),
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
            // Index metadata is live-only (an index changes speed, never
            // results), so `at` does not participate ([STL-233]).
            Self::CreateIndex {
                name,
                table,
                kind,
                columns,
            } => catalog
                .create_index(IndexDef::new(name, table, kind, columns)?)
                .map(|()| DdlOutcome::CreatedIndex),
            Self::DropIndex { name, if_exists } => match catalog.drop_index(&name) {
                Ok(_) => Ok(DdlOutcome::DroppedIndex),
                Err(CatalogError::UnknownIndex(_)) if if_exists => Ok(DdlOutcome::DropIndexNoOp),
                Err(e) => Err(e),
            },
            // User DDL ([STL-252]) mutates the engine's durable user store, not
            // the schema catalog — the engine matches these variants itself and
            // never routes them here. Reaching this arm is a routing bug.
            Self::CreateUser { .. } | Self::AlterUserPassword { .. } | Self::DropUser { .. } => {
                unreachable!("user DDL is applied by the session engine's user store, not Catalog")
            }
        }
    }
}
