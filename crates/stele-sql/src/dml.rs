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
//! ## The v0.1 mapping — `(key, payload)`
//!
//! v0.1 has **no row codec** (a v0.2 concern) and the catalog does not yet record
//! which column is the primary key ([`bind_ddl`](crate::bind_ddl) parses
//! `PRIMARY KEY` but stores only name + type). So DML binds the *identity-demo
//! shape* positionally: a
//! table must have exactly two columns, the **first is the business key** and the
//! **second is the (opaque) payload**. Each is folded from its SQL literal into a
//! [`ScalarValue`] of the column's type; the engine encodes those to bytes with
//! [`ScalarValue::encode`](stele_common::types::ScalarValue::encode) when it
//! applies the write, so the round-trip back through a read is exact. A table
//! with any other shape is [rejected](DmlError::UnsupportedTableShape) rather than
//! guessed at.
//!
//! The three operations map to the write path ([STL-94],
//! [architecture §3.4](../../../docs/02-architecture.md#34-write-path-sequence)):
//!
//! * `INSERT INTO t VALUES (k, p)` — open a fresh period for `k` carrying `p`.
//! * `UPDATE t SET <payload> = p WHERE <key> = k` — close `k`'s prior period,
//!   open a new one with `p`.
//! * `DELETE FROM t WHERE <key> = k` — close `k`'s prior period, no successor.
//!
//! ## What v0.1 rejects (with a clear bind error, never a wrong write)
//!
//! Multi-row `INSERT`, `INSERT … SELECT`, a `WHERE` that is not `<key> =
//! <literal>` (including a whole-table `UPDATE`/`DELETE` with no `WHERE`),
//! updating the key column, multi-column-payload tables, `RETURNING`, `ON
//! CONFLICT`, `USING`/`FROM` joins, qualified names, and `NULL` or out-of-range
//! literals. Folding a `TIMESTAMP`/`DATE` literal is also out of scope at v0.1
//! (no civil-time codec — mirrors the [`AS OF`](crate::select) stance).

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Insert, ObjectName,
    SetExpr, Statement as SqlStatement, TableFactor, TableObject, TableWithJoins, UnaryOperator,
    Update, Value,
};
use stele_catalog::{ColumnDef, SchemaId, TableSchema};
use stele_common::types::{LogicalType, ScalarValue};

use crate::ast::Statement;
use crate::select::{BindContext, TableResolution, resolve_table_at};

/// A bound `INSERT` / `UPDATE` / `DELETE`, ready for the engine to apply.
///
/// Carries the resolved table, the schema version it bound under, and the
/// already-folded [`ScalarValue`]s for the business key and (for writes) the
/// payload. See the [module docs](self) for the `(key, payload)` mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundDml {
    /// `INSERT`: open a fresh `[commit, +∞)` period for `key` carrying `payload`.
    Insert {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key (the first column's value).
        key: ScalarValue,
        /// The opaque payload (the second column's value).
        payload: ScalarValue,
    },
    /// `UPDATE`: close `key`'s prior period and open a new one with `payload`.
    Update {
        /// The table written.
        table: String,
        /// The schema version `table` resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The business key the `WHERE` clause selected.
        key: ScalarValue,
        /// The new opaque payload from the `SET` clause.
        payload: ScalarValue,
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

    /// The table is not the v0.1 two-column `(key, payload)` shape. A general
    /// multi-column row codec is a v0.2 concern, so a wider (or narrower) table
    /// cannot be written through this path yet.
    #[error(
        "v0.1 DML requires a two-column (key, payload) table; {table:?} has {columns} column(s)"
    )]
    UnsupportedTableShape {
        /// The table written.
        table: String,
        /// How many columns it actually declares.
        columns: usize,
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
    /// represented (out of range, or not an integer).
    #[error(
        "literal {literal:?} is not a valid {ty} value for column {column:?} in table {table:?}"
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
    },

    /// A `NULL` was bound to a key or payload. v0.1 models nullability one level
    /// up (`Option<ScalarValue>`) and the DML write path does not carry it yet —
    /// and a business key is never null.
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
    let (schema, key_col, payload_col) = resolve_shape(ctx, &table)?;

    let row = single_values_row(insert)?;

    // Map each target column to the value expression at the same position. With
    // no explicit column list the order is the schema's declaration order
    // (key first, payload second); with a list, values are matched by name.
    let (key_expr, payload_expr) = match insert.columns.as_slice() {
        [] => match row {
            [key, payload] => (key, payload),
            _ => {
                return Err(DmlError::ColumnCountMismatch {
                    expected: 2,
                    found: row.len(),
                });
            }
        },
        cols => {
            if cols.len() != row.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: cols.len(),
                    found: row.len(),
                });
            }
            let names = validated_columns(&table, cols, schema)?;
            let value_for = |column: &ColumnDef| {
                names
                    .iter()
                    .position(|n| n == column.name())
                    .map(|i| &row[i])
                    .ok_or_else(|| DmlError::MissingColumn {
                        table: table.clone(),
                        column: column.name().to_owned(),
                    })
            };
            (value_for(key_col)?, value_for(payload_col)?)
        }
    };

    Ok(BoundDml::Insert {
        table: table.clone(),
        schema_id: schema.schema_id(),
        key: fold_value(key_expr, &table, key_col)?,
        payload: fold_value(payload_expr, &table, payload_col)?,
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
    let (schema, key_col, payload_col) = resolve_shape(ctx, &table)?;

    // Exactly one assignment, targeting the payload column.
    let [assignment] = update.assignments.as_slice() else {
        return Err(DmlError::Unsupported(format!(
            "UPDATE with {} assignments (v0.1 sets the one payload column)",
            update.assignments.len()
        )));
    };
    let target = assignment_column(assignment)?;
    if target == key_col.name() {
        return Err(DmlError::CannotUpdateKey {
            table: table.clone(),
            column: target.to_owned(),
        });
    }
    if target != payload_col.name() {
        return Err(DmlError::UnknownColumn {
            table: table.clone(),
            column: target.to_owned(),
        });
    }

    let key = key_predicate(update.selection.as_ref(), &table, key_col)?;
    let payload = fold_value(&assignment.value, &table, payload_col)?;
    Ok(BoundDml::Update {
        table,
        schema_id: schema.schema_id(),
        key,
        payload,
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
    let (schema, key_col, _payload_col) = resolve_shape(ctx, &table)?;

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

/// Resolve `table` at the context snapshot and enforce the v0.1 two-column
/// `(key, payload)` shape, returning the schema and its key / payload columns.
fn resolve_shape<'a>(
    ctx: &'a BindContext,
    table: &str,
) -> Result<(&'a TableSchema, &'a ColumnDef, &'a ColumnDef), DmlError> {
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
    match schema.columns() {
        [key, payload] => Ok((schema, key, payload)),
        cols => Err(DmlError::UnsupportedTableShape {
            table: table.to_owned(),
            columns: cols.len(),
        }),
    }
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

/// Fold a literal expression into a [`ScalarValue`] of `column`'s type, rejecting
/// `NULL`, type mismatches, and out-of-range / unsupported literals.
fn fold_value(expr: &Expr, table: &str, column: &ColumnDef) -> Result<ScalarValue, DmlError> {
    if is_null(expr) {
        return Err(DmlError::NullValue {
            table: table.to_owned(),
            column: column.name().to_owned(),
        });
    }
    let mismatch = |found: &str| DmlError::TypeMismatch {
        table: table.to_owned(),
        column: column.name().to_owned(),
        expected: column.ty(),
        found: found.to_owned(),
    };
    match column.ty() {
        LogicalType::Int4 => {
            let digits = signed_number(expr).ok_or_else(|| mismatch(describe(expr)))?;
            digits
                .parse::<i32>()
                .map(ScalarValue::Int4)
                .map_err(|_| bad_literal(table, column, &digits))
        }
        LogicalType::Int8 => {
            let digits = signed_number(expr).ok_or_else(|| mismatch(describe(expr)))?;
            digits
                .parse::<i64>()
                .map(ScalarValue::Int8)
                .map_err(|_| bad_literal(table, column, &digits))
        }
        LogicalType::Text => match literal(expr) {
            Some(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
            _ => Err(mismatch(describe(expr))),
        },
        LogicalType::Bool => match literal(expr) {
            Some(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
            _ => Err(mismatch(describe(expr))),
        },
        // No civil-time literal codec at v0.1 (mirrors AS OF); a TIMESTAMP/DATE
        // column cannot be written through DML yet.
        ty @ (LogicalType::Timestamp | LogicalType::Date) => Err(DmlError::Unsupported(format!(
            "a {ty} literal for column {:?}",
            column.name()
        ))),
    }
}

fn bad_literal(table: &str, column: &ColumnDef, literal: &str) -> DmlError {
    DmlError::BadLiteral {
        table: table.to_owned(),
        column: column.name().to_owned(),
        ty: column.ty(),
        literal: literal.to_owned(),
    }
}

/// Whether an expression is the `NULL` literal.
fn is_null(expr: &Expr) -> bool {
    matches!(literal(expr), Some(Value::Null))
}

/// The literal [`Value`] an expression carries, peeling parentheses; `None` if it
/// is not a bare literal.
fn literal(expr: &Expr) -> Option<&Value> {
    match expr {
        Expr::Value(v) => Some(&v.value),
        Expr::Nested(inner) => literal(inner),
        _ => None,
    }
}

/// The (possibly signed) decimal digits of a numeric literal, folding a leading
/// unary `+` / `-` into the string so it parses directly. `None` for any
/// non-numeric expression.
fn signed_number(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        Expr::Nested(inner) => signed_number(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => signed_number(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => signed_number(expr).map(|s| {
            s.strip_prefix('-')
                .map_or_else(|| format!("-{s}"), ToOwned::to_owned)
        }),
        _ => None,
    }
}

/// A short label for an expression, for the type-mismatch diagnostics.
fn describe(expr: &Expr) -> &'static str {
    match literal(expr) {
        Some(Value::SingleQuotedString(_)) => "a string literal",
        Some(Value::Boolean(_)) => "a boolean literal",
        Some(Value::Number(..)) => "a numeric literal",
        Some(Value::Null) => "NULL",
        _ => "a non-literal expression",
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
                payload: ScalarValue::Int4(100),
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
                payload: ScalarValue::Int4(250),
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
                payload: ScalarValue::Int4(100),
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
        let BoundDml::Insert { payload, .. } =
            bind("INSERT INTO account VALUES (1, -42)", &catalog).expect("bind")
        else {
            panic!("expected an INSERT");
        };
        assert_eq!(payload, ScalarValue::Int4(-42));
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
            })
        );
    }

    #[test]
    fn null_is_rejected() {
        let catalog = account_catalog();
        assert_eq!(
            bind("INSERT INTO account VALUES (1, NULL)", &catalog),
            Err(DmlError::NullValue {
                table: "account".to_owned(),
                column: "balance".to_owned(),
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

    #[test]
    fn multi_column_payload_table_is_rejected() {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "wide",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("a", LogicalType::Int4).expect("col"),
                    ColumnDef::new("b", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create wide");
        assert_eq!(
            bind("INSERT INTO wide VALUES (1, 2, 3)", &catalog),
            Err(DmlError::UnsupportedTableShape {
                table: "wide".to_owned(),
                columns: 3,
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
                payload: ScalarValue::Bool(true),
            })
        );
    }
}
