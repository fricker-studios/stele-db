//! DML binding: lower a parsed `INSERT` / `UPDATE` / `DELETE` into the storage
//! write the engine applies.
//!
//! This is the SQL-layer half of DML-over-the-wire ([STL-149]) — the sibling of
//! [`bind_select`](crate::bind_select) (reads) and [`bind_ddl`](crate::bind_ddl)
//! (schema). [`parse`](crate::parse) handles the grammar; [`bind_dml`] turns the
//! data-manipulation subset into a typed [`BoundDml`] the session engine lowers
//! to a `DmlWriter` call (`insert` / `update` / `delete`). The pgwire query
//! loop ([STL-147]) is the consumer.
//!
//! ## The column mapping — key + value columns
//!
//! The catalog does not yet record which column is the primary key
//! ([`bind_ddl`](crate::bind_ddl) parses `PRIMARY KEY` but stores only name +
//! type), so DML keeps the positional convention: the **first column is the
//! business key**, and the remaining columns are the row's **value columns**. A
//! table may now be any width ([STL-151]) — the value columns are packed into the
//! one stored payload by the [row codec](stele_common::row_codec) when the engine
//! applies the write, and sliced back out on read. Each supplied literal is folded
//! to a [`ScalarValue`] of its column's type (via the `fold` module); the engine
//! encodes those to bytes with
//! [`ScalarValue::encode`](stele_common::types::ScalarValue::encode), so the
//! round-trip back through a read is exact.
//!
//! The three operations map to the write path ([STL-94],
//! [architecture §3.4](../../../docs/02-architecture.md#34-write-path-sequence)):
//!
//! * `INSERT INTO t VALUES (k, …)` — open a fresh period for `k` carrying the
//!   row's value columns.
//! * `UPDATE t SET <col> = v[, …] WHERE <key> = k` — close `k`'s prior period and
//!   open a new one; columns the `SET` does not name keep their prior value
//!   (a read-modify-write the engine performs).
//! * `DELETE FROM t WHERE <key> = k` — close `k`'s prior period, no successor.
//!
//! ## What this rejects (with a clear bind error, never a wrong write)
//!
//! Multi-row `INSERT`, `INSERT … SELECT`, a `WHERE` that is not `<key> =
//! <literal>` (including a whole-table `UPDATE`/`DELETE` with no `WHERE`),
//! updating the key column, `RETURNING`, `ON CONFLICT`, `USING`/`FROM` joins,
//! qualified names, a `NULL` business key, and out-of-range literals. A `NULL`
//! **value column** is accepted (it folds to `None`, [STL-154]); a `NULL` key is
//! not. Folding a `TIMESTAMP`/`DATE` literal is still out of scope (no civil-time
//! codec — mirrors the [`AS OF`](crate::select) stance).
//!
//! [STL-151]: https://allegromusic.atlassian.net/browse/STL-151

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Insert, ObjectName,
    SetExpr, Statement as SqlStatement, TableFactor, TableObject, TableWithJoins, Update,
};
use stele_catalog::{ColumnDef, SchemaId, TableSchema};
use stele_common::types::{LogicalType, ScalarValue};

use crate::ast::Statement;
use crate::fold::{self, FoldError};
use crate::select::{BindContext, TableResolution, resolve_table_at};

/// A bound `INSERT` / `UPDATE` / `DELETE`, ready for the engine to apply.
///
/// Carries the resolved table, the schema version it bound under, and the
/// already-folded [`ScalarValue`]s for the business key and the row's value
/// columns. See the [module docs](self) for the key + value-column mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundDml {
    /// `INSERT`: open a fresh `[commit, +∞)` period for `key` carrying the row's
    /// value columns.
    Insert {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key (the first column's value).
        key: ScalarValue,
        /// The value columns' values, in schema order (the columns *after* the
        /// business key). Each is `None` for a SQL `NULL` cell ([STL-154]). The
        /// engine packs these into the stored payload with the
        /// [row codec](stele_common::row_codec).
        values: Vec<Option<ScalarValue>>,
    },
    /// `UPDATE`: close `key`'s prior period and open a new one. The `assignments`
    /// overwrite the named value columns; any column the `SET` does not name keeps
    /// its prior value (the engine reads the current row and merges).
    Update {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key the `WHERE` clause selected.
        key: ScalarValue,
        /// The `SET` clause as `(value-column index, new value)` pairs, where the
        /// index is 0-based over the **value columns** (the columns after the
        /// business key). A `None` value is a SQL `NULL` ([STL-154]).
        assignments: Vec<(usize, Option<ScalarValue>)>,
    },
    /// `DELETE`: close `key`'s prior period with no successor (a period close,
    /// not a row removal — history is preserved).
    Delete {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key the `WHERE` clause selected.
        key: ScalarValue,
    },
}

impl BoundDml {
    /// The table this operation writes.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }
}

/// Why binding a parsed statement into a [`BoundDml`] failed.
///
/// The input parsed as valid SQL; these are the bind-time reasons it is not a
/// DML operation v0.1 can apply. Every variant is a *clear* refusal rather than
/// a silently wrong write ([STL-149] Definition of Done).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DmlError {
    /// The statement is not an `INSERT` / `UPDATE` / `DELETE`. The caller routes
    /// those elsewhere ([`bind_select`](crate::bind_select) / [`bind_ddl`](crate::bind_ddl)).
    #[error("not a DML statement")]
    NotDml,

    /// A clause, dialect extension, or statement shape outside the v0.1 DML
    /// surface (`RETURNING`, `ON CONFLICT`, `USING`, a join, `INSERT … SELECT`,
    /// a non-`<key> = <literal>` `WHERE`, …).
    #[error("{0} is not supported in v0.1 DML")]
    Unsupported(String),

    /// A schema- or database-qualified name (a table or column with more than
    /// one part, e.g. `public.account` or `account.id`). v0.1 has a single
    /// implicit namespace, so only bare names resolve.
    #[error("qualified name {0:?} — only bare names are supported in v0.1")]
    QualifiedName(String),

    /// A single-part name (table or column) that is not a plain identifier.
    #[error("name {0:?} is not a plain identifier")]
    InvalidName(String),

    /// The catalog has never registered this table name.
    #[error("unknown table {0:?}")]
    UnknownTable(String),

    /// The bind snapshot precedes the table's first commit — a write *before the
    /// table existed*. Mirrors [`SelectError::BeforeHistory`](crate::select::SelectError::BeforeHistory).
    #[error(
        "table {table:?} did not exist at the bind snapshot {snapshot} (first commit at {first_commit})"
    )]
    BeforeHistory {
        /// The table written.
        table: String,
        /// The bind snapshot, in system-time microseconds.
        snapshot: i64,
        /// The table's first-commit system time; `snapshot` precedes it.
        first_commit: i64,
    },

    /// The table exists in the catalog timeline but is not live at the snapshot —
    /// dropped, or in a re-creation gap.
    #[error(
        "table {table:?} is not live at the bind snapshot {snapshot} (dropped, or not yet created)"
    )]
    TableNotLive {
        /// The table written.
        table: String,
        /// The bind snapshot, in system-time microseconds.
        snapshot: i64,
    },

    /// The table declares no columns — every table has at least a business-key
    /// column, so this guards an otherwise-impossible shape (DDL never creates
    /// one) rather than panicking.
    #[error("table {table:?} has no columns")]
    EmptyTable {
        /// The table written.
        table: String,
    },

    /// An `INSERT` supplied more than one row of `VALUES`. v0.1 inserts a single
    /// row per statement.
    #[error("v0.1 INSERT writes a single row; {rows} rows were given")]
    MultiRowInsert {
        /// How many `VALUES` rows the statement carried.
        rows: usize,
    },

    /// An `INSERT`'s value count does not match the target column count.
    #[error("INSERT supplies {found} value(s) for {expected} column(s)")]
    ColumnCountMismatch {
        /// The number of target columns.
        expected: usize,
        /// The number of values supplied.
        found: usize,
    },

    /// A column named in an `INSERT` column list or an `UPDATE … SET` target is
    /// not a column of the table.
    #[error("column {column:?} does not exist in table {table:?}")]
    UnknownColumn {
        /// The table written.
        table: String,
        /// The column named that the schema does not contain.
        column: String,
    },

    /// An `INSERT` column list omitted a required column (the key or the payload).
    #[error("INSERT into {table:?} does not supply a value for column {column:?}")]
    MissingColumn {
        /// The table written.
        table: String,
        /// The required column with no supplied value.
        column: String,
    },

    /// An `INSERT` column list named the same column twice. Keeping the last
    /// value would silently bind the wrong key/payload, so it is rejected.
    #[error("INSERT into {table:?} names column {column:?} more than once")]
    DuplicateColumn {
        /// The table written.
        table: String,
        /// The column named more than once.
        column: String,
    },

    /// An `UPDATE` targeted the business-key column. v0.1 cannot rewrite the key
    /// (it is the identity a version's history hangs on).
    #[error("UPDATE of table {table:?} cannot assign to the business-key column {column:?}")]
    CannotUpdateKey {
        /// The table written.
        table: String,
        /// The key column the `SET` clause tried to assign.
        column: String,
    },

    /// An `UPDATE` / `DELETE` `WHERE` compared a column other than the business
    /// key. v0.1 selects rows by their key only.
    #[error("v0.1 DML WHERE must compare the business key {key:?}, not {column:?}")]
    PredicateNotOnKey {
        /// The key column the predicate should have named.
        key: String,
        /// The column it actually named.
        column: String,
    },

    /// A literal's shape does not match its column's type (e.g. a string for an
    /// `int4` column).
    #[error("value {found} for column {column:?} in table {table:?} is not a {expected}")]
    TypeMismatch {
        /// The table written.
        table: String,
        /// The column the value was bound to.
        column: String,
        /// The column's declared type.
        expected: LogicalType,
        /// A short description of the value actually given.
        found: String,
    },

    /// A literal is the right shape for its column's type but cannot be
    /// represented (out of range, not an integer, or a malformed civil-time
    /// literal).
    #[error(
        "literal {literal:?} is not a valid {ty} value for column {column:?} in table {table:?}{}",
        reason.map(|r| format!(" ({r})")).unwrap_or_default()
    )]
    BadLiteral {
        /// The table written.
        table: String,
        /// The column the value was bound to.
        column: String,
        /// The column's declared type.
        ty: LogicalType,
        /// The offending literal text.
        literal: String,
        /// A short, stable reason from the type's codec, when it has one (e.g.
        /// `"day out of range for month"` for a `timestamptz`); `None` for the
        /// integer types, whose failure is self-evident from the literal.
        reason: Option<&'static str>,
    },

    /// A `NULL` was bound to the **business key**. A key is the identity a
    /// version's history hangs on and is never null. (A `NULL` *payload* is
    /// supported — it folds to `None` through `fold_payload`, [STL-154].)
    #[error("NULL is not supported for column {column:?} in table {table:?} in v0.1 DML")]
    NullValue {
        /// The table written.
        table: String,
        /// The column a `NULL` was bound to.
        column: String,
    },
}

/// Bind a parsed [`Statement`] into a [`BoundDml`].
///
/// Routes on the statement kind, resolves the table against the catalog at the
/// context snapshot (the same resolution [`bind_select`](crate::bind_select)
/// uses), enforces the v0.1 `(key, payload)` shape, and folds the key / payload
/// literals into typed [`ScalarValue`]s. See the [module docs](self) for the full
/// surface.
///
/// # Errors
///
/// [`DmlError::NotDml`] if the statement is not `INSERT` / `UPDATE` / `DELETE`;
/// otherwise a [`DmlError`] variant naming the unknown table / column,
/// unsupported shape, or bad literal.
pub fn bind_dml(stmt: &Statement, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    match &stmt.body {
        SqlStatement::Insert(insert) => bind_insert(insert, ctx),
        SqlStatement::Update(update) => bind_update(update, ctx),
        SqlStatement::Delete(delete) => bind_delete(delete, ctx),
        _ => Err(DmlError::NotDml),
    }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

fn bind_insert(insert: &Insert, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    reject_insert_extensions(insert)?;

    let TableObject::TableName(name) = &insert.table else {
        return Err(DmlError::Unsupported(
            "INSERT into a table function or subquery".to_owned(),
        ));
    };
    let table = bare_name(name)?;
    let (schema, key_col, value_cols) = resolve_shape(ctx, &table)?;

    let row = single_values_row(insert)?;

    // Resolve, for every schema column in declaration order, the value expression
    // that supplies it. With no explicit column list the values map positionally;
    // with a list, each schema column takes the value at its name's position (and
    // every column must be supplied — there are no defaults).
    let columns = schema.columns();
    let exprs: Vec<&Expr> = match insert.columns.as_slice() {
        [] => {
            if row.len() != columns.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: columns.len(),
                    found: row.len(),
                });
            }
            row.iter().collect()
        }
        cols => {
            if cols.len() != row.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: cols.len(),
                    found: row.len(),
                });
            }
            let names = validated_columns(&table, cols, schema)?;
            columns
                .iter()
                .map(|column| {
                    names
                        .iter()
                        .position(|n| n == column.name())
                        .map(|i| &row[i])
                        .ok_or_else(|| DmlError::MissingColumn {
                            table: table.clone(),
                            column: column.name().to_owned(),
                        })
                })
                .collect::<Result<_, _>>()?
        }
    };

    // `exprs` is aligned to `columns`: the first is the business key, the rest are
    // the value columns in order.
    let key = fold_value(exprs[0], &table, key_col)?;
    let values = value_cols
        .iter()
        .zip(&exprs[1..])
        .map(|(column, expr)| fold_payload(expr, &table, column))
        .collect::<Result<_, _>>()?;

    Ok(BoundDml::Insert {
        table: table.clone(),
        schema_id: schema.schema_id(),
        key,
        values,
    })
}

/// Reject the dialect-specific `INSERT` forms outside the v0.1 surface, before
/// looking at the values.
fn reject_insert_extensions(insert: &Insert) -> Result<(), DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if insert.or.is_some() {
        return reject("INSERT OR …");
    }
    if insert.ignore {
        return reject("INSERT IGNORE");
    }
    if insert.overwrite {
        return reject("INSERT OVERWRITE");
    }
    if insert.replace_into {
        return reject("REPLACE INTO");
    }
    if !insert.assignments.is_empty() {
        return reject("INSERT … SET");
    }
    if insert.partitioned.is_some() {
        return reject("partitioned INSERT");
    }
    if insert.on.is_some() {
        return reject("INSERT … ON CONFLICT / ON DUPLICATE KEY");
    }
    if insert.returning.is_some() {
        return reject("INSERT … RETURNING");
    }
    if insert.multi_table_insert_type.is_some() {
        return reject("INSERT ALL / INSERT FIRST");
    }
    Ok(())
}

/// The single row of `VALUES` an `INSERT` carries, after rejecting `INSERT …
/// SELECT` and the multi-row form.
fn single_values_row(insert: &Insert) -> Result<&[Expr], DmlError> {
    let Some(query) = insert.source.as_deref() else {
        return Err(DmlError::Unsupported("INSERT without VALUES".to_owned()));
    };
    let SetExpr::Values(values) = query.body.as_ref() else {
        return Err(DmlError::Unsupported("INSERT … SELECT".to_owned()));
    };
    match values.rows.as_slice() {
        [row] => Ok(row.content.as_slice()),
        rows => Err(DmlError::MultiRowInsert { rows: rows.len() }),
    }
}

/// Resolve an `INSERT` column list to bare names in positional order, rejecting a
/// name that is not a real column ([`UnknownColumn`](DmlError::UnknownColumn)) or
/// that repeats ([`DuplicateColumn`](DmlError::DuplicateColumn) — keeping only the
/// last value for a repeated name would silently bind the wrong cell). The caller
/// then matches a target column to the value at its position in this list.
fn validated_columns(
    table: &str,
    cols: &[ObjectName],
    schema: &TableSchema,
) -> Result<Vec<String>, DmlError> {
    let mut names: Vec<String> = Vec::with_capacity(cols.len());
    for name in cols {
        let name = bare_name(name)?;
        if schema.column(&name).is_none() {
            return Err(DmlError::UnknownColumn {
                table: table.to_owned(),
                column: name,
            });
        }
        if names.iter().any(|prev| prev == &name) {
            return Err(DmlError::DuplicateColumn {
                table: table.to_owned(),
                column: name,
            });
        }
        names.push(name);
    }
    Ok(names)
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn bind_update(update: &Update, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if update.from.is_some() {
        return reject("UPDATE … FROM");
    }
    if update.returning.is_some() {
        return reject("UPDATE … RETURNING");
    }
    if update.or.is_some() {
        return reject("UPDATE OR …");
    }
    if !update.order_by.is_empty() {
        return reject("UPDATE … ORDER BY");
    }
    if update.limit.is_some() {
        return reject("UPDATE … LIMIT");
    }

    let table = table_of(&update.table)?;
    let (schema, key_col, value_cols) = resolve_shape(ctx, &table)?;

    // One or more `SET <col> = <value>` assignments, each targeting a value
    // column. Columns the SET does not name keep their prior value — the engine
    // reads the current row and merges, so the binder only carries the overrides.
    if update.assignments.is_empty() {
        return Err(DmlError::Unsupported(
            "UPDATE with no SET assignments".to_owned(),
        ));
    }
    let mut assignments: Vec<(usize, Option<ScalarValue>)> = Vec::new();
    for assignment in &update.assignments {
        let target = assignment_column(assignment)?;
        if target == key_col.name() {
            return Err(DmlError::CannotUpdateKey {
                table: table.clone(),
                column: target.to_owned(),
            });
        }
        let idx = value_cols
            .iter()
            .position(|c| c.name() == target)
            .ok_or_else(|| DmlError::UnknownColumn {
                table: table.clone(),
                column: target.to_owned(),
            })?;
        // A column assigned twice would silently keep only the last value — reject
        // it rather than guess which the user meant.
        if assignments.iter().any(|(prev, _)| *prev == idx) {
            return Err(DmlError::Unsupported(format!(
                "UPDATE assigns column {target:?} more than once"
            )));
        }
        let value = fold_payload(&assignment.value, &table, &value_cols[idx])?;
        assignments.push((idx, value));
    }

    let key = key_predicate(update.selection.as_ref(), &table, key_col)?;
    Ok(BoundDml::Update {
        table,
        schema_id: schema.schema_id(),
        key,
        assignments,
    })
}

/// The single, unqualified column an `UPDATE … SET` assignment targets.
fn assignment_column(assignment: &Assignment) -> Result<&str, DmlError> {
    match &assignment.target {
        AssignmentTarget::ColumnName(name) => single_ident(name),
        AssignmentTarget::Tuple(_) => Err(DmlError::Unsupported(
            "UPDATE … SET (a, b) = … tuple assignment".to_owned(),
        )),
    }
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn bind_delete(delete: &Delete, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if !delete.tables.is_empty() {
        return reject("multi-table DELETE");
    }
    if delete.using.is_some() {
        return reject("DELETE … USING");
    }
    if delete.returning.is_some() {
        return reject("DELETE … RETURNING");
    }
    if !delete.order_by.is_empty() {
        return reject("DELETE … ORDER BY");
    }
    if delete.limit.is_some() {
        return reject("DELETE … LIMIT");
    }

    let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) = &delete.from;
    let [target] = tables.as_slice() else {
        return Err(DmlError::Unsupported(
            "DELETE from multiple tables".to_owned(),
        ));
    };
    let table = table_of(target)?;
    let (schema, key_col, _value_cols) = resolve_shape(ctx, &table)?;

    let key = key_predicate(delete.selection.as_ref(), &table, key_col)?;
    Ok(BoundDml::Delete {
        table,
        schema_id: schema.schema_id(),
        key,
    })
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Resolve `table` at the context snapshot and split its schema into the business
/// key (the first column) and the value columns (the rest), returning the schema
/// alongside.
fn resolve_shape<'a>(
    ctx: &'a BindContext,
    table: &str,
) -> Result<(&'a TableSchema, &'a ColumnDef, &'a [ColumnDef]), DmlError> {
    let schema = match resolve_table_at(ctx.catalog, table, ctx.snapshot) {
        TableResolution::Found(schema) => schema,
        TableResolution::Unknown => return Err(DmlError::UnknownTable(table.to_owned())),
        TableResolution::BeforeHistory { first_commit } => {
            return Err(DmlError::BeforeHistory {
                table: table.to_owned(),
                snapshot: ctx.snapshot.0,
                first_commit: first_commit.0,
            });
        }
        TableResolution::NotLive => {
            return Err(DmlError::TableNotLive {
                table: table.to_owned(),
                snapshot: ctx.snapshot.0,
            });
        }
    };
    // The first column is the business key; the rest are value columns (possibly
    // none, for a key-only table).
    let (key, values) = schema
        .columns()
        .split_first()
        .ok_or_else(|| DmlError::EmptyTable {
            table: table.to_owned(),
        })?;
    Ok((schema, key, values))
}

/// Bind an `UPDATE` / `DELETE` `WHERE` clause: it must be `<key> = <literal>`,
/// naming the business key, and the literal folds against the key's type. A
/// missing `WHERE` (a whole-table write) is rejected.
fn key_predicate(
    selection: Option<&Expr>,
    table: &str,
    key_col: &ColumnDef,
) -> Result<ScalarValue, DmlError> {
    let Some(expr) = selection else {
        return Err(DmlError::Unsupported(
            "a whole-table UPDATE/DELETE (v0.1 requires `WHERE <key> = <literal>`)".to_owned(),
        ));
    };
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return Err(DmlError::Unsupported(
            "a WHERE that is not `<key> = <literal>`".to_owned(),
        ));
    };
    // Accept the column on either side: `id = 1` or `1 = id`. A qualified column
    // (`t.id`) surfaces `QualifiedName`, not the generic "not `<key> = <literal>`".
    let (column, value) = match (column_side(left)?, column_side(right)?) {
        (Some(column), None) => (column, right.as_ref()),
        (None, Some(column)) => (column, left.as_ref()),
        _ => {
            return Err(DmlError::Unsupported(
                "a WHERE that is not `<key> = <literal>`".to_owned(),
            ));
        }
    };
    if column != key_col.name() {
        return Err(DmlError::PredicateNotOnKey {
            key: key_col.name().to_owned(),
            column: column.to_owned(),
        });
    }
    fold_value(value, table, key_col)
}

/// The column name a `WHERE` side references, peeling parentheses.
///
/// `Ok(Some(name))` for a bare identifier, `Ok(None)` for a non-column expression
/// (so a literal side is told apart from a column side), and
/// [`Err(QualifiedName)`](DmlError::QualifiedName) for a qualified column like
/// `t.id` — a clearer diagnostic than the generic "not `<key> = <literal>`".
fn column_side(expr: &Expr) -> Result<Option<&str>, DmlError> {
    match expr {
        Expr::Identifier(id) => Ok(Some(id.value.as_str())),
        Expr::CompoundIdentifier(parts) => Err(DmlError::QualifiedName(
            parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        )),
        Expr::Nested(inner) => column_side(inner),
        _ => Ok(None),
    }
}

/// Fold a value-column literal, accepting SQL `NULL` as `None` ([STL-154]).
///
/// A value column is nullable end to end (storage carries a `None` cell
/// distinctly), so `NULL` here is a valid write rather than a rejected one — the
/// sibling of [`fold_value`], which still rejects `NULL` for the never-null
/// business key. A present literal folds through [`fold_value`] unchanged.
fn fold_payload(
    expr: &Expr,
    table: &str,
    column: &ColumnDef,
) -> Result<Option<ScalarValue>, DmlError> {
    if fold::is_null(expr) {
        return Ok(None);
    }
    fold_value(expr, table, column).map(Some)
}

/// Fold a literal expression into a [`ScalarValue`] of `column`'s type, rejecting
/// `NULL`, type mismatches, and out-of-range / unsupported literals. Used for the
/// business key (never nullable); a value column folds through [`fold_payload`],
/// which accepts `NULL`. The folding itself lives in the `fold` module; this only
/// attaches the table/column names to the failure.
fn fold_value(expr: &Expr, table: &str, column: &ColumnDef) -> Result<ScalarValue, DmlError> {
    fold::fold_scalar(expr, column.ty()).map_err(|err| fold_err_to_dml(err, table, column))
}

/// Map a table/column-agnostic [`FoldError`] to the DML error that names the
/// table and column it occurred on. Reproduces the binder's pre-existing errors
/// exactly, so the surface is unchanged.
fn fold_err_to_dml(err: FoldError, table: &str, column: &ColumnDef) -> DmlError {
    match err {
        FoldError::Null => DmlError::NullValue {
            table: table.to_owned(),
            column: column.name().to_owned(),
        },
        FoldError::TypeMismatch { found } => DmlError::TypeMismatch {
            table: table.to_owned(),
            column: column.name().to_owned(),
            expected: column.ty(),
            found: found.to_owned(),
        },
        FoldError::BadLiteral { literal, reason } => DmlError::BadLiteral {
            table: table.to_owned(),
            column: column.name().to_owned(),
            ty: column.ty(),
            literal,
            reason,
        },
        FoldError::UnsupportedType(ty) => {
            DmlError::Unsupported(format!("a {ty} literal for column {:?}", column.name()))
        }
    }
}

/// The bare, unqualified table name of a `FROM`/`UPDATE` target — rejecting
/// joins and non-table relations.
fn table_of(twj: &TableWithJoins) -> Result<String, DmlError> {
    if !twj.joins.is_empty() {
        return Err(DmlError::Unsupported("a JOIN in a DML target".to_owned()));
    }
    match &twj.relation {
        TableFactor::Table { name, .. } => bare_name(name),
        _ => Err(DmlError::Unsupported(
            "a non-table DML target (subquery, function, …)".to_owned(),
        )),
    }
}

/// Extract a single, unqualified identifier from an [`ObjectName`] — the table /
/// column name forms v0.1 accepts. Mirrors the DDL binder's `bare_name`.
fn bare_name(name: &ObjectName) -> Result<String, DmlError> {
    single_ident(name).map(ToOwned::to_owned)
}

/// Borrow a single, unqualified identifier out of an [`ObjectName`].
fn single_ident(name: &ObjectName) -> Result<&str, DmlError> {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|id| id.value.as_str())
            .ok_or_else(|| DmlError::InvalidName(name.to_string())),
        _ => Err(DmlError::QualifiedName(name.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use stele_catalog::{Catalog, TableTemporal};
    use stele_common::time::SystemTimeMicros;

    /// The bind snapshot for the fixtures.
    const NOW: SystemTimeMicros = SystemTimeMicros(2_000_000_000_000_000);

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1, "expected one statement");
        stmts.remove(0)
    }

    /// A catalog with the identity-demo `account (id INT, balance INT)` table,
    /// created at system time `1_000`.
    fn account_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "account",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create account");
        catalog
    }

    fn bind(sql: &str, catalog: &Catalog) -> Result<BoundDml, DmlError> {
        let ctx = BindContext {
            snapshot: NOW,
            catalog,
        };
        bind_dml(&parse_one(sql), &ctx)
    }

    #[test]
    fn binds_the_demo_insert() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1, 100)", &catalog),
            Ok(BoundDml::Insert {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![Some(ScalarValue::Int4(100))],
            })
        );
    }

    #[test]
    fn binds_the_demo_update() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET balance = 250 WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![(0, Some(ScalarValue::Int4(250)))],
            })
        );
    }

    #[test]
    fn binds_the_demo_delete() {
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE id = 1", &catalog),
            Ok(BoundDml::Delete {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
            })
        );
    }

    #[test]
    fn insert_column_list_maps_by_name_in_any_order() {
        let catalog = account_catalog();
        // Reversed column list — values must still bind to the right columns.
        assert_eq!(
            bind(
                "INSERT INTO account (balance, id) VALUES (100, 1)",
                &catalog
            ),
            Ok(BoundDml::Insert {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![Some(ScalarValue::Int4(100))],
            })
        );
    }

    #[test]
    fn where_accepts_the_key_on_either_side() {
        let catalog = account_catalog();
        let flipped = bind("DELETE FROM account WHERE 1 = id", &catalog).expect("bind");
        assert_eq!(
            flipped,
            BoundDml::Delete {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
            }
        );
    }

    #[test]
    fn negative_literals_fold() {
        let catalog = account_catalog();
        let BoundDml::Insert { values, .. } =
            bind("INSERT INTO account VALUES (1, -42)", &catalog).expect("bind")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(values, vec![Some(ScalarValue::Int4(-42))]);
    }

    #[test]
    fn non_dml_is_not_dml() {
        let catalog = account_catalog();
        assert_eq!(
            bind("SELECT balance FROM account", &catalog),
            Err(DmlError::NotDml)
        );
        assert_eq!(
            bind("CREATE TABLE t (a INT) WITH SYSTEM VERSIONING", &catalog),
            Err(DmlError::NotDml)
        );
    }

    #[test]
    fn unknown_table_is_reported() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO ghost VALUES (1, 2)", &catalog),
            Err(DmlError::UnknownTable("ghost".to_owned()))
        );
    }

    #[test]
    fn multi_row_insert_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1, 100), (2, 200)", &catalog),
            Err(DmlError::MultiRowInsert { rows: 2 })
        );
    }

    #[test]
    fn insert_select_is_rejected() {
        let catalog = account_catalog();
        assert!(matches!(
            bind(
                "INSERT INTO account SELECT id, balance FROM account",
                &catalog
            ),
            Err(DmlError::Unsupported(_))
        ));
    }

    #[test]
    fn wrong_value_count_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1)", &catalog),
            Err(DmlError::ColumnCountMismatch {
                expected: 2,
                found: 1
            })
        );
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES ('one', 100)", &catalog),
            Err(DmlError::TypeMismatch {
                table: "account".to_owned(),
                column: "id".to_owned(),
                expected: LogicalType::Int4,
                found: "a string literal".to_owned(),
            })
        );
    }

    #[test]
    fn out_of_range_literal_is_a_bad_literal() {
        let catalog = account_catalog();
        // 5_000_000_000 overflows i32 (the `balance` column is int4).
        assert_eq!(
            bind("INSERT INTO account VALUES (1, 5000000000)", &catalog),
            Err(DmlError::BadLiteral {
                table: "account".to_owned(),
                column: "balance".to_owned(),
                ty: LogicalType::Int4,
                literal: "5000000000".to_owned(),
                reason: None,
            })
        );
    }

    #[test]
    fn null_payload_is_accepted() {
        // A NULL payload folds to `None` — a valid write, not a rejection
        // ([STL-154]). The key stays its concrete value.
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1, NULL)", &catalog),
            Ok(BoundDml::Insert {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![None],
            })
        );
    }

    #[test]
    fn null_payload_in_update_is_accepted() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET balance = NULL WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![(0, None)],
            })
        );
    }

    #[test]
    fn null_key_is_rejected() {
        // The business key is never nullable — a NULL key is still refused
        // ([STL-154] lifts the rejection for the payload only).
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (NULL, 100)", &catalog),
            Err(DmlError::NullValue {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
        // A NULL key in a WHERE predicate is refused too.
        assert_eq!(
            bind("DELETE FROM account WHERE id = NULL", &catalog),
            Err(DmlError::NullValue {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
    }

    #[test]
    fn updating_the_key_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET id = 2 WHERE id = 1", &catalog),
            Err(DmlError::CannotUpdateKey {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
    }

    #[test]
    fn updating_an_unknown_column_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("UPDATE account SET nonesuch = 2 WHERE id = 1", &catalog),
            Err(DmlError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn where_on_a_non_key_column_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE balance = 100", &catalog),
            Err(DmlError::PredicateNotOnKey {
                key: "id".to_owned(),
                column: "balance".to_owned(),
            })
        );
    }

    #[test]
    fn whole_table_update_is_rejected() {
        let catalog = account_catalog();
        assert!(matches!(
            bind("UPDATE account SET balance = 0", &catalog),
            Err(DmlError::Unsupported(_))
        ));
    }

    #[test]
    fn non_equality_where_is_rejected() {
        let catalog = account_catalog();
        assert!(matches!(
            bind("DELETE FROM account WHERE id > 1", &catalog),
            Err(DmlError::Unsupported(_))
        ));
    }

    /// A catalog with a three-value-column table `wide (id, a, b, c)`
    /// (`a`/`b` int4, `c` text), created at system time `1_000`.
    fn wide_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "wide",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("a", LogicalType::Int4).expect("col"),
                    ColumnDef::new("b", LogicalType::Int4).expect("col"),
                    ColumnDef::new("c", LogicalType::Text).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create wide");
        catalog
    }

    #[test]
    fn multi_column_insert_binds_all_value_columns_in_order() {
        // The row codec ([STL-151]) lets a table be wider than (key, value): the
        // value columns bind in schema order, NULLs included.
        let catalog = wide_catalog();
        assert_eq!(
            bind("INSERT INTO wide VALUES (1, 2, 3, 'x')", &catalog),
            Ok(BoundDml::Insert {
                table: "wide".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(2)),
                    Some(ScalarValue::Int4(3)),
                    Some(ScalarValue::Text("x".to_owned())),
                ],
            })
        );
        // A reordering column list still maps each value to its column by name.
        assert_eq!(
            bind(
                "INSERT INTO wide (c, id, b, a) VALUES ('x', 1, 3, 2)",
                &catalog
            ),
            Ok(BoundDml::Insert {
                table: "wide".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![
                    Some(ScalarValue::Int4(2)),
                    Some(ScalarValue::Int4(3)),
                    Some(ScalarValue::Text("x".to_owned())),
                ],
            })
        );
    }

    #[test]
    fn multi_column_update_sets_a_subset_by_index() {
        // Each SET targets a value column; the index is 0-based over the value
        // columns (a=0, b=1, c=2). Unnamed columns are merged by the engine.
        let catalog = wide_catalog();
        assert_eq!(
            bind("UPDATE wide SET b = 9, c = 'z' WHERE id = 1", &catalog),
            Ok(BoundDml::Update {
                table: "wide".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                assignments: vec![
                    (1, Some(ScalarValue::Int4(9))),
                    (2, Some(ScalarValue::Text("z".to_owned()))),
                ],
            })
        );
    }

    #[test]
    fn multi_column_update_rejects_a_repeated_set_target() {
        let catalog = wide_catalog();
        assert!(matches!(
            bind("UPDATE wide SET a = 1, a = 2 WHERE id = 1", &catalog),
            Err(DmlError::Unsupported(_))
        ));
    }

    #[test]
    fn key_only_table_inserts_just_the_key() {
        // A one-column table has no value columns: the row is the key alone.
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "solo",
                vec![ColumnDef::new("id", LogicalType::Int4).expect("col")],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create solo");
        assert_eq!(
            bind("INSERT INTO solo VALUES (1)", &catalog),
            Ok(BoundDml::Insert {
                table: "solo".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Int4(1),
                values: vec![],
            })
        );
    }

    #[test]
    fn wrong_value_count_for_a_wide_table_is_rejected() {
        let catalog = wide_catalog();
        assert_eq!(
            bind("INSERT INTO wide VALUES (1, 2)", &catalog),
            Err(DmlError::ColumnCountMismatch {
                expected: 4,
                found: 2,
            })
        );
    }

    #[test]
    fn unknown_column_in_insert_list_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account (id, nonesuch) VALUES (1, 2)", &catalog),
            Err(DmlError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn qualified_column_in_where_is_rejected_as_qualified_name() {
        let catalog = account_catalog();
        assert_eq!(
            bind("DELETE FROM account WHERE account.id = 1", &catalog),
            Err(DmlError::QualifiedName("account.id".to_owned()))
        );
    }

    #[test]
    fn duplicate_column_in_insert_list_is_rejected() {
        let catalog = account_catalog();
        // Keeping the last `id` would silently bind the wrong key — reject it.
        assert_eq!(
            bind(
                "INSERT INTO account (id, balance, id) VALUES (1, 100, 2)",
                &catalog
            ),
            Err(DmlError::DuplicateColumn {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
    }

    #[test]
    fn qualified_table_name_is_rejected() {
        let catalog = account_catalog();
        assert!(matches!(
            bind("INSERT INTO public.account VALUES (1, 100)", &catalog),
            Err(DmlError::QualifiedName(_))
        ));
    }

    #[test]
    fn text_and_bool_columns_fold() {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "flags",
                vec![
                    ColumnDef::new("name", LogicalType::Text).expect("col"),
                    ColumnDef::new("on", LogicalType::Bool).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create flags");
        assert_eq!(
            bind("INSERT INTO flags VALUES ('alpha', true)", &catalog),
            Ok(BoundDml::Insert {
                table: "flags".to_owned(),
                schema_id: SchemaId(1),
                key: ScalarValue::Text("alpha".to_owned()),
                values: vec![Some(ScalarValue::Bool(true))],
            })
        );
    }
}
