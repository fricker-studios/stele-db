//! `MERGE` binding: lower a parsed `MERGE INTO … USING … ON … WHEN …` into the
//! upsert plan the engine expands at execution ([STL-230]).
//!
//! The sibling of [`bind_dml`](crate::bind_dml)'s `INSERT` / `UPDATE` / `DELETE`
//! lowering, routed from the same entry point (`SqlStatement::Merge` binds here).
//! A bound `MERGE` is **not** a per-key write: it is a [`BoundMerge`] *plan* —
//! source rows, the business-key join, and one value *template* per `WHEN` arm —
//! that the engine resolves **per source row at execution**, the same posture as
//! the scan-then-write `UPDATE` / `DELETE` ([STL-229]): probe the target at the
//! statement snapshot, then apply the whole write set as one atomic group.
//!
//! ## The supported subset
//!
//! ```sql
//! MERGE INTO t [AS a]
//! USING (VALUES (…), (…)) AS s (c1, c2, …)   -- or: USING source_table [AS s]
//! ON t.key = s.c1                             -- the business-key equality
//! WHEN MATCHED THEN UPDATE SET col = s.c2 [, …]
//! WHEN NOT MATCHED THEN INSERT [(cols)] VALUES (s.c1, s.c2, …)
//! ```
//!
//! * The **source** is a `VALUES` list (folded to typed rows at bind) or a plain
//!   table (read at the statement snapshot when the plan expands — inside a
//!   transaction the read-your-own-writes overlay applies, [STL-203]).
//! * The **`ON` condition** is exactly one equality joining the target's business
//!   key to one source column — the probe the plan stands on. Anything else
//!   (a non-key target column, `AND` chains, expressions) is rejected.
//! * Each **arm value** is a literal or a source-column reference (`s.c`). A
//!   `VALUES` source column takes its type from where it is used (the join, a
//!   `SET` target, an `INSERT` column); conflicting uses are a bind error.
//! * At most one `WHEN MATCHED THEN UPDATE` and one `WHEN NOT MATCHED THEN
//!   INSERT`, either alone: a source row whose arm is absent is skipped.
//!
//! ## Valid-time historization ([STL-235])
//!
//! On a table with a valid axis the `MERGE` is the historization workhorse: a
//! matched row gets the joint system+valid **close/open** ([STL-166]) — close the
//! prior version on the system axis, open a new one carrying the arm's valid
//! interval — and an unmatched row inserts with that interval, exactly the
//! [STL-194] `INSERT` / `UPDATE` surface. The period columns (`vf`, `vt`) ride the
//! arms like any other column and their bounds fold as **instants** (an integer
//! microsecond value, `now()`, or `now() ± interval` — *not* civil-time literals),
//! the start mandatory and the end defaulting to an open period
//! ([`VALID_TIME_OPEN`]). The per-arm interval rides
//! [`BoundMerge::matched_valid`] / [`BoundMerge::not_matched_valid`]; the engine
//! frames it onto the stored payload at apply. A bound that is a **source column**
//! (a per-source-row valid interval) is rejected for now — only statement-level
//! instant bounds are supported.
//!
//! ## What this rejects (with a clear bind error, never a wrong write)
//!
//! `WHEN NOT MATCHED BY SOURCE`, `WHEN MATCHED THEN DELETE`, clause predicates
//! (`WHEN MATCHED AND <expr>`), referencing the target row in an arm value,
//! subquery sources beyond `VALUES`/table, `OUTPUT`, updating the business key,
//! and a source column used as a valid-time period bound.
//!
//! [STL-166]: https://allegromusic.atlassian.net/browse/STL-166
//! [STL-194]: https://allegromusic.atlassian.net/browse/STL-194
//! [STL-229]: https://allegromusic.atlassian.net/browse/STL-229
//! [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
//! [STL-235]: https://allegromusic.atlassian.net/browse/STL-235

use sqlparser::ast::{
    Expr, Merge, MergeAction, MergeClause, MergeClauseKind, MergeInsertExpr, MergeInsertKind,
    SetExpr, TableAlias, TableFactor,
};
use stele_catalog::{ColumnDef, SchemaId, TableSchema, ValidTimeSpec};
use stele_common::period::Interval;
use stele_common::time::{SystemTimeMicros, VALID_TIME_OPEN};
use stele_common::types::{LogicalType, ScalarValue};

use crate::dml::{
    BoundDml, DmlError, PeriodRole, assignment_column, bare_name, build_interval, fold_err_to_dml,
    fold_from_bound, fold_to_bound, period_role, resolve_shape, validated_columns,
};
use crate::fold;
use crate::select::BindContext;

/// A bound `MERGE` plan, ready for the engine to expand into per-key writes at
/// the statement snapshot ([STL-230]).
///
/// Carried as [`BoundDml::Merge`]; like the scan-then-write variants it never
/// reaches the per-key write path directly — the engine resolves each source
/// row's arm (matched ⇒ update, not matched ⇒ insert) against the target's live
/// keys and applies the whole set as one atomic group.
///
/// [STL-230]: https://allegromusic.atlassian.net/browse/STL-230
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundMerge {
    /// The target table written.
    pub table: String,
    /// The schema version the target resolved to at the bind snapshot.
    pub schema_id: SchemaId,
    /// Where the source rows come from.
    pub source: MergeSource,
    /// The source column the `ON` equality joins to the target's business key —
    /// an index into each source row.
    pub on: usize,
    /// The `WHEN MATCHED THEN UPDATE SET` template: `(value-column index, value)`
    /// pairs — the same positional convention as [`BoundDml::Update`] — or
    /// `None` when the statement has no matched arm (matched rows are skipped).
    pub matched: Option<Vec<(usize, MergeValue)>>,
    /// The `WHEN NOT MATCHED THEN INSERT` template, aligned to **all** target
    /// columns in declaration order (the business key first), or `None` when the
    /// statement has no not-matched arm (unmatched rows are skipped).
    pub not_matched: Option<Vec<MergeValue>>,
    /// The `[from, to)` valid-time period each **matched** row's new version
    /// opens, or `None` for a system-only table or an absent matched arm. `Some`
    /// on a valid-time table: derived from the matched arm's period-column bounds
    /// (`vf` mandatory, `vt` defaulting to [`VALID_TIME_OPEN`]), it rides down to
    /// every expanded [`BoundDml::Update`], which closes the prior version and
    /// opens the new one over this interval ([STL-235]).
    ///
    /// [STL-235]: https://allegromusic.atlassian.net/browse/STL-235
    pub matched_valid: Option<Interval>,
    /// The `[from, to)` valid-time period each **not-matched** row inserts with,
    /// or `None` for a system-only table or an absent not-matched arm — the
    /// insert-arm counterpart of [`matched_valid`](Self::matched_valid).
    pub not_matched_valid: Option<Interval>,
}

/// Where a [`BoundMerge`]'s source rows come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeSource {
    /// `USING (VALUES …) AS s (cols)` — the rows, folded to typed values at
    /// bind. A cell is `None` for a SQL `NULL` **or** for a source column no arm
    /// (nor the `ON`) references — an unused column's literals are never read.
    Values(Vec<Vec<Option<ScalarValue>>>),
    /// `USING source_table [AS s]` — read at the statement snapshot when the
    /// plan expands (with the transaction's read-your-own-writes overlay,
    /// [STL-203]).
    Table {
        /// The source table read.
        name: String,
        /// The schema version the source resolved to at the bind snapshot.
        schema_id: SchemaId,
        /// The source's columns — `(name, type)` in declaration order; the
        /// engine projects and decodes the scanned rows by these.
        columns: Vec<(String, LogicalType)>,
    },
}

/// One value slot of a `WHEN` arm template, resolved per source row at
/// execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeValue {
    /// A literal, folded to the target column's type at bind (`None` = `NULL`).
    Literal(Option<ScalarValue>),
    /// The source column at this index of the source row.
    Source(usize),
}

/// The names a side of the statement answers to: the table name and its alias.
struct Names {
    table: String,
    alias: Option<String>,
}

impl Names {
    /// Whether `qualifier` names this side (`t.col` / `alias.col`).
    fn matches(&self, qualifier: &str) -> bool {
        qualifier == self.table || self.alias.as_deref() == Some(qualifier)
    }
}

/// The source's bind-time shape: its qualifier and columns, plus — for a
/// `VALUES` source — the per-column type each use site requires (folded in one
/// pass at the end of binding).
struct SourceBinding {
    names: Names,
    columns: Vec<String>,
    /// `Some` for a table source: the schema version it resolved to and its
    /// declared column types, checked at use.
    declared: Option<(SchemaId, Vec<LogicalType>)>,
    /// For a `VALUES` source: the type each used column folds to, inferred from
    /// its use sites. `None` = not used anywhere (cells stay unfolded).
    required: Vec<Option<LogicalType>>,
}

impl SourceBinding {
    /// Record that source column `idx` is used where a `ty` value is expected,
    /// rejecting a declared- or previously-inferred-type conflict.
    fn use_column(&mut self, idx: usize, ty: LogicalType) -> Result<(), DmlError> {
        if let Some((_, declared)) = &self.declared {
            if declared[idx] != ty {
                return Err(DmlError::Unsupported(format!(
                    "MERGE source column {:?} is a {} where a {ty} is required",
                    self.columns[idx], declared[idx]
                )));
            }
            return Ok(());
        }
        match self.required[idx] {
            Some(prior) if prior != ty => Err(DmlError::Unsupported(format!(
                "MERGE source column {:?} is used as both {prior} and {ty}",
                self.columns[idx]
            ))),
            _ => {
                self.required[idx] = Some(ty);
                Ok(())
            }
        }
    }

    /// The index of the source column named `name`, if any.
    fn column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }
}

/// Bind a parsed `MERGE` into a [`BoundDml::Merge`] plan. Routed from
/// [`bind_dml`](crate::bind_dml); see the [module docs](self) for the surface.
pub(crate) fn bind_merge(merge: &Merge, ctx: &BindContext) -> Result<BoundDml, DmlError> {
    let reject = |what: &str| Err(DmlError::Unsupported(what.to_owned()));
    if merge.output.is_some() {
        return reject("MERGE … OUTPUT");
    }
    if !merge.optimizer_hints.is_empty() {
        return reject("optimizer hints on MERGE");
    }

    // The target: a plain table, optionally aliased.
    let (table, target_names) = target_table(&merge.table)?;
    let (schema, key_col, value_cols) = resolve_shape(ctx, &table)?;
    // On a valid-time table the arms carry the period columns; their bounds fold
    // as instants against the bind snapshot, exactly as a plain INSERT/UPDATE does
    // ([STL-194]). `None` for a system-only table, where the arms carry no period.
    let period = schema.temporal().valid_time();
    let now = ctx.snapshot;

    let mut source = bind_source(&merge.source, ctx)?;
    if target_names.matches(&source.names.table)
        || source
            .names
            .alias
            .as_deref()
            .is_some_and(|a| target_names.matches(a))
    {
        return reject("a MERGE source that shares a name with its target");
    }

    let on = bind_on(&merge.on, &target_names, key_col, &mut source)?;

    let mut matched: Option<Vec<(usize, MergeValue)>> = None;
    let mut not_matched: Option<Vec<MergeValue>> = None;
    let mut matched_valid: Option<Interval> = None;
    let mut not_matched_valid: Option<Interval> = None;
    for clause in &merge.clauses {
        bind_clause(
            clause,
            &table,
            schema,
            key_col,
            value_cols,
            &target_names,
            period,
            now,
            &mut source,
            &mut matched,
            &mut not_matched,
            &mut matched_valid,
            &mut not_matched_valid,
        )?;
    }
    if matched.is_none() && not_matched.is_none() {
        return reject("MERGE with no WHEN clause");
    }

    let source = fold_source(source, &merge.source)?;

    Ok(BoundDml::Merge(BoundMerge {
        table,
        schema_id: schema.schema_id(),
        source,
        on,
        matched,
        not_matched,
        matched_valid,
        not_matched_valid,
    }))
}

/// The target table's bare name and the names it answers to in qualified
/// references.
fn target_table(factor: &TableFactor) -> Result<(String, Names), DmlError> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(DmlError::Unsupported(
            "a non-table MERGE target (subquery, function, …)".to_owned(),
        ));
    };
    let table = bare_name(name)?;
    let alias = alias.as_ref().map(plain_alias).transpose()?;
    let names = Names {
        table: table.clone(),
        alias,
    };
    Ok((table, names))
}

/// An alias's bare name, rejecting an attached column list (only a `VALUES`
/// source names its columns through the alias).
fn plain_alias(alias: &TableAlias) -> Result<String, DmlError> {
    if !alias.columns.is_empty() {
        return Err(DmlError::Unsupported(
            "a column list on a table alias in MERGE".to_owned(),
        ));
    }
    Ok(alias.name.value.clone())
}

/// Bind the `USING` source: a plain table or a `(VALUES …) AS s (cols)` derived
/// table.
fn bind_source(factor: &TableFactor, ctx: &BindContext) -> Result<SourceBinding, DmlError> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table = bare_name(name)?;
            let (schema, key_col, value_cols) = resolve_shape(ctx, &table)?;
            let alias = alias.as_ref().map(plain_alias).transpose()?;
            let columns: Vec<String> = std::iter::once(key_col)
                .chain(value_cols)
                .map(|c| c.name().to_owned())
                .collect();
            let declared: Vec<LogicalType> = std::iter::once(key_col)
                .chain(value_cols)
                .map(ColumnDef::ty)
                .collect();
            let required = vec![None; columns.len()];
            Ok(SourceBinding {
                names: Names { table, alias },
                columns,
                declared: Some((schema.schema_id(), declared)),
                required,
            })
        }
        TableFactor::Derived {
            lateral: false,
            subquery,
            alias: Some(alias),
            ..
        } => {
            let SetExpr::Values(values) = subquery.body.as_ref() else {
                return Err(DmlError::Unsupported(
                    "a MERGE source subquery other than VALUES".to_owned(),
                ));
            };
            if alias.columns.is_empty() {
                return Err(DmlError::Unsupported(
                    "a VALUES MERGE source without named columns (USING (VALUES …) AS s (c1, …))"
                        .to_owned(),
                ));
            }
            let columns: Vec<String> = alias.columns.iter().map(|c| c.name.value.clone()).collect();
            for row in &values.rows {
                if row.content.len() != columns.len() {
                    return Err(DmlError::ColumnCountMismatch {
                        expected: columns.len(),
                        found: row.content.len(),
                    });
                }
            }
            let required = vec![None; columns.len()];
            Ok(SourceBinding {
                names: Names {
                    table: alias.name.value.clone(),
                    alias: None,
                },
                columns,
                declared: None,
                required,
            })
        }
        _ => Err(DmlError::Unsupported(
            "a MERGE source other than a table or VALUES".to_owned(),
        )),
    }
}

/// Bind the `ON` condition: exactly one equality between the target's business
/// key and one source column (either operand order), returning the source
/// column's index.
fn bind_on(
    on: &Expr,
    target: &Names,
    key_col: &ColumnDef,
    source: &mut SourceBinding,
) -> Result<usize, DmlError> {
    let unsupported = || {
        DmlError::Unsupported(format!(
            "a MERGE ON condition other than <target>.{} = <source column>",
            key_col.name()
        ))
    };
    let Expr::BinaryOp { left, op, right } = on else {
        return Err(unsupported());
    };
    if *op != sqlparser::ast::BinaryOperator::Eq {
        return Err(unsupported());
    }
    let sides = (
        on_side(left, target, key_col, source)?,
        on_side(right, target, key_col, source)?,
    );
    let ((OnSide::TargetKey, OnSide::Source(idx)) | (OnSide::Source(idx), OnSide::TargetKey)) =
        sides
    else {
        return Err(unsupported());
    };
    source.use_column(idx, key_col.ty())?;
    Ok(idx)
}

/// One resolved operand of the `ON` equality.
enum OnSide {
    /// The target's business-key column.
    TargetKey,
    /// The source column at this index.
    Source(usize),
}

/// Resolve one `ON` operand to the target key or a source column. A bare name
/// that exists on both sides is ambiguous and must be qualified.
fn on_side(
    expr: &Expr,
    target: &Names,
    key_col: &ColumnDef,
    source: &SourceBinding,
) -> Result<OnSide, DmlError> {
    match expr {
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [qualifier, column] => {
                let (qualifier, column) = (qualifier.value.as_str(), column.value.as_str());
                if target.matches(qualifier) {
                    if column == key_col.name() {
                        Ok(OnSide::TargetKey)
                    } else {
                        Err(DmlError::Unsupported(format!(
                            "a MERGE ON condition on the non-key target column {column:?}"
                        )))
                    }
                } else if source.names.matches(qualifier) {
                    source.column(column).map(OnSide::Source).ok_or_else(|| {
                        DmlError::UnknownColumn {
                            table: source.names.table.clone(),
                            column: column.to_owned(),
                        }
                    })
                } else {
                    Err(DmlError::QualifiedName(format!("{qualifier}.{column}")))
                }
            }
            _ => Err(DmlError::QualifiedName(
                parts
                    .iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            )),
        },
        Expr::Identifier(ident) => {
            let name = ident.value.as_str();
            let in_source = source.column(name);
            let is_key = name == key_col.name();
            match (is_key, in_source) {
                (true, Some(_)) => Err(DmlError::Unsupported(format!(
                    "the ambiguous name {name:?} in a MERGE ON condition (qualify it)"
                ))),
                (true, None) => Ok(OnSide::TargetKey),
                (false, Some(idx)) => Ok(OnSide::Source(idx)),
                // Neither the business key nor a source column. The name is not a
                // source-column reference, so attributing it to the source table
                // would mislead (e.g. `ON balance = s.id` — a non-key target
                // column, or a typo); say what the operand must be instead.
                (false, None) => Err(unsupported_on(key_col)),
            }
        }
        _ => Err(unsupported_on(key_col)),
    }
}

/// The shared "ON must be a key equality" rejection.
fn unsupported_on(key_col: &ColumnDef) -> DmlError {
    DmlError::Unsupported(format!(
        "a MERGE ON condition other than <target>.{} = <source column>",
        key_col.name()
    ))
}

/// Bind one `WHEN` clause into its arm template, rejecting the forms outside
/// the subset.
#[allow(clippy::too_many_arguments)]
fn bind_clause(
    clause: &MergeClause,
    table: &str,
    schema: &TableSchema,
    key_col: &ColumnDef,
    value_cols: &[ColumnDef],
    target: &Names,
    period: Option<&ValidTimeSpec>,
    now: SystemTimeMicros,
    source: &mut SourceBinding,
    matched: &mut Option<Vec<(usize, MergeValue)>>,
    not_matched: &mut Option<Vec<MergeValue>>,
    matched_valid: &mut Option<Interval>,
    not_matched_valid: &mut Option<Interval>,
) -> Result<(), DmlError> {
    let reject = |what: String| Err(DmlError::Unsupported(what));
    if clause.predicate.is_some() {
        return reject("a predicate on a MERGE WHEN clause (WHEN … AND <expr>)".to_owned());
    }
    match (clause.clause_kind, &clause.action) {
        (MergeClauseKind::Matched, MergeAction::Update(update)) => {
            if matched.is_some() {
                return reject("more than one WHEN MATCHED clause".to_owned());
            }
            if update.update_predicate.is_some() || update.delete_predicate.is_some() {
                return reject("a predicate on a MERGE UPDATE action".to_owned());
            }
            // On a valid-time table the matched arm opens a new version, so — like
            // an STL-194 `UPDATE` — its `SET` supplies the new period's bounds. The
            // period columns are assigned alongside any value column; their bounds
            // fold as instants and are tracked into the interval below.
            let mut assignments: Vec<(usize, MergeValue)> = Vec::new();
            let mut from: Option<i64> = None;
            let mut to: Option<i64> = None;
            for assignment in &update.assignments {
                let column = assignment_column(assignment)?;
                if column == key_col.name() {
                    return Err(DmlError::CannotUpdateKey {
                        table: table.to_owned(),
                        column: column.to_owned(),
                    });
                }
                let idx = value_cols
                    .iter()
                    .position(|c| c.name() == column)
                    .ok_or_else(|| DmlError::UnknownColumn {
                        table: table.to_owned(),
                        column: column.to_owned(),
                    })?;
                if assignments.iter().any(|(prev, _)| *prev == idx) {
                    return reject(format!(
                        "MERGE UPDATE assigns column {column:?} more than once"
                    ));
                }
                let value = match period_role(period, column) {
                    Some(role) => {
                        let micros = bind_period_bound(
                            role,
                            Some(&assignment.value),
                            table,
                            &value_cols[idx],
                            now,
                            source,
                        )?;
                        match role {
                            PeriodRole::From => from = Some(micros),
                            PeriodRole::To => to = Some(micros),
                        }
                        MergeValue::Literal(Some(ScalarValue::Timestamp(micros)))
                    }
                    None => bind_value(&assignment.value, table, &value_cols[idx], target, source)?,
                };
                assignments.push((idx, value));
            }
            if assignments.is_empty() {
                return reject("MERGE UPDATE with no SET assignments".to_owned());
            }
            // The end bound defaults to an open period when the SET omits it —
            // synthesize the cell so the row-codec payload and the framed interval
            // agree, mirroring the plain valid-time `UPDATE` ([STL-194]).
            if let Some(period) = period
                && to.is_none()
                && let Some(to_idx) = value_cols
                    .iter()
                    .position(|c| c.name() == period.to_column())
            {
                assignments.push((
                    to_idx,
                    MergeValue::Literal(Some(ScalarValue::Timestamp(VALID_TIME_OPEN.0))),
                ));
                to = Some(VALID_TIME_OPEN.0);
            }
            *matched = Some(assignments);
            *matched_valid = build_interval(table, period, from, to)?;
            Ok(())
        }
        (MergeClauseKind::Matched, MergeAction::Delete { .. }) => {
            reject("WHEN MATCHED THEN DELETE".to_owned())
        }
        (MergeClauseKind::Matched, MergeAction::Insert(_)) => {
            reject("WHEN MATCHED THEN INSERT".to_owned())
        }
        (
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget,
            MergeAction::Insert(insert),
        ) => {
            if not_matched.is_some() {
                return reject("more than one WHEN NOT MATCHED clause".to_owned());
            }
            let (template, valid) =
                bind_insert_arm(insert, table, schema, key_col, target, period, now, source)?;
            *not_matched = Some(template);
            *not_matched_valid = valid;
            Ok(())
        }
        (MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget, _) => {
            reject("a WHEN NOT MATCHED action other than INSERT".to_owned())
        }
        (MergeClauseKind::NotMatchedBySource, _) => reject("WHEN NOT MATCHED BY SOURCE".to_owned()),
    }
}

/// Bind the `WHEN NOT MATCHED THEN INSERT` arm to a value template aligned to
/// all target columns — the same column-list discipline as a plain `INSERT`
/// ([`bind_dml`](crate::bind_dml)): with no list the values map positionally
/// (count must match exactly); with a list each target column takes the value at
/// its name's position, and an omitted column is a [`DmlError::MissingColumn`].
///
/// On a valid-time table the period columns ride the template as instant-folded
/// [`ScalarValue::Timestamp`] cells and the second return is the `[from, to)`
/// interval the engine frames onto the insert — the `from` bound mandatory, the
/// `to` bound defaulting to an open period when omitted, exactly the STL-194
/// `INSERT` surface. The interval is `None` for a system-only table.
#[allow(clippy::too_many_arguments)]
fn bind_insert_arm(
    insert: &MergeInsertExpr,
    table: &str,
    schema: &TableSchema,
    key_col: &ColumnDef,
    target: &Names,
    period: Option<&ValidTimeSpec>,
    now: SystemTimeMicros,
    source: &mut SourceBinding,
) -> Result<(Vec<MergeValue>, Option<Interval>), DmlError> {
    if insert.insert_predicate.is_some() {
        return Err(DmlError::Unsupported(
            "a predicate on a MERGE INSERT action".to_owned(),
        ));
    }
    let MergeInsertKind::Values(values) = &insert.kind else {
        return Err(DmlError::Unsupported("MERGE … INSERT ROW".to_owned()));
    };
    let [row] = values.rows.as_slice() else {
        return Err(DmlError::Unsupported(
            "a multi-row VALUES in a MERGE INSERT action".to_owned(),
        ));
    };
    let row = row.content.as_slice();

    let columns = schema.columns();
    let exprs: Vec<Option<&Expr>> = match insert.columns.as_slice() {
        [] => {
            if row.len() != columns.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: columns.len(),
                    found: row.len(),
                });
            }
            row.iter().map(Some).collect()
        }
        cols => {
            if cols.len() != row.len() {
                return Err(DmlError::ColumnCountMismatch {
                    expected: cols.len(),
                    found: row.len(),
                });
            }
            let names = validated_columns(table, cols, schema)?;
            columns
                .iter()
                .map(|column| {
                    names
                        .iter()
                        .position(|n| n == column.name())
                        .map(|i| &row[i])
                })
                .collect()
        }
    };

    let mut template = Vec::with_capacity(columns.len());
    let mut from: Option<i64> = None;
    let mut to: Option<i64> = None;
    for (column, expr) in columns.iter().zip(&exprs) {
        // A valid-time period column folds as an instant (the `to` bound may be
        // omitted to open the period); every other column must be supplied.
        if let Some(role) = period_role(period, column.name()) {
            let micros = bind_period_bound(role, *expr, table, column, now, source)?;
            match role {
                PeriodRole::From => from = Some(micros),
                PeriodRole::To => to = Some(micros),
            }
            template.push(MergeValue::Literal(Some(ScalarValue::Timestamp(micros))));
            continue;
        }
        let expr = expr.ok_or_else(|| DmlError::MissingColumn {
            table: table.to_owned(),
            column: column.name().to_owned(),
        })?;
        let value = bind_value(expr, table, column, target, source)?;
        // A literal NULL business key can never insert — reject it at bind, the
        // same posture as a plain INSERT. (A NULL arriving through a *source
        // column* is data-dependent and rejected at execution.)
        if column.name() == key_col.name() && value == MergeValue::Literal(None) {
            return Err(DmlError::NullValue {
                table: table.to_owned(),
                column: column.name().to_owned(),
            });
        }
        template.push(value);
    }
    let valid = build_interval(table, period, from, to)?;
    Ok((template, valid))
}

/// Fold a `MERGE` arm's value for a valid-time **period** column to its
/// microsecond instant, the same way a plain `INSERT`/`UPDATE` bound folds
/// ([`fold_from_bound`] / [`fold_to_bound`]): an integer microsecond value,
/// `now()`, or `now() ± interval`. A **source column** as a period bound (a
/// per-source-row valid interval) is rejected — only statement-level instant
/// bounds are supported for now (a deferred follow-up), so the close/open instant
/// is fixed at bind, not data-dependent.
fn bind_period_bound(
    role: PeriodRole,
    expr: Option<&Expr>,
    table: &str,
    column: &ColumnDef,
    now: SystemTimeMicros,
    source: &SourceBinding,
) -> Result<i64, DmlError> {
    if let Some(expr) = expr {
        match classify_period_bound(expr, source) {
            // An existing source column — the deferred per-source-row bound.
            PeriodBoundRef::SourceColumn => {
                return Err(DmlError::Unsupported(format!(
                    "a MERGE source column as the valid-time period bound {:?} (only an instant — \
                     microseconds, now(), or now() ± interval — is supported)",
                    column.name()
                )));
            }
            // The source qualifier naming a column it doesn't have — a typo, named
            // precisely rather than mislabeled as a (rejected) source bound.
            PeriodBoundRef::UnknownSourceColumn(name) => {
                return Err(DmlError::UnknownColumn {
                    table: source.names.table.clone(),
                    column: name,
                });
            }
            // Not a source reference — fold it as an instant below.
            PeriodBoundRef::Instant => {}
        }
    }
    match role {
        PeriodRole::From => fold_from_bound(expr, table, column.name(), now),
        PeriodRole::To => fold_to_bound(expr, table, column.name(), now),
    }
}

/// How a `MERGE` arm's period-bound expression relates to the source, deciding
/// the precise rejection [`bind_period_bound`] gives.
enum PeriodBoundRef {
    /// An existing source column (`s.c` or a bare source-column name) — the
    /// shape a per-source-row valid interval would take, rejected for now.
    SourceColumn,
    /// The source qualifier naming a column it does not have (`s.<typo>`) — an
    /// [`UnknownColumn`](DmlError::UnknownColumn), not a source bound.
    UnknownSourceColumn(String),
    /// Anything else — fold it as an instant (a literal, `now()`, …).
    Instant,
}

/// Classify a `MERGE` period-bound expression against the source columns. A bare
/// name only counts as a source reference when it actually names a source column;
/// a *qualified* `s.<col>` whose column is missing is reported as an
/// [`UnknownColumn`] rather than misclassified as a (rejected) source bound.
fn classify_period_bound(expr: &Expr, source: &SourceBinding) -> PeriodBoundRef {
    match expr {
        Expr::Identifier(ident) if source.column(&ident.value).is_some() => {
            PeriodBoundRef::SourceColumn
        }
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [qualifier, column] if source.names.matches(&qualifier.value) => {
                if source.column(&column.value).is_some() {
                    PeriodBoundRef::SourceColumn
                } else {
                    PeriodBoundRef::UnknownSourceColumn(column.value.clone())
                }
            }
            _ => PeriodBoundRef::Instant,
        },
        _ => PeriodBoundRef::Instant,
    }
}

/// Bind one arm value: a source-column reference (`s.c` or a bare source column
/// name) or a literal folded to the target column's type.
fn bind_value(
    expr: &Expr,
    table: &str,
    column: &ColumnDef,
    target: &Names,
    source: &mut SourceBinding,
) -> Result<MergeValue, DmlError> {
    match expr {
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [qualifier, name] => {
                let (qualifier, name) = (qualifier.value.as_str(), name.value.as_str());
                if source.names.matches(qualifier) {
                    let idx = source.column(name).ok_or_else(|| DmlError::UnknownColumn {
                        table: source.names.table.clone(),
                        column: name.to_owned(),
                    })?;
                    source.use_column(idx, column.ty())?;
                    Ok(MergeValue::Source(idx))
                } else if target.matches(qualifier) {
                    Err(DmlError::Unsupported(format!(
                        "referencing the target row ({qualifier}.{name}) in a MERGE value"
                    )))
                } else {
                    Err(DmlError::QualifiedName(format!("{qualifier}.{name}")))
                }
            }
            _ => Err(DmlError::QualifiedName(
                parts
                    .iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            )),
        },
        Expr::Identifier(ident) => {
            let name = ident.value.as_str();
            if let Some(idx) = source.column(name) {
                source.use_column(idx, column.ty())?;
                Ok(MergeValue::Source(idx))
            } else {
                Err(DmlError::UnknownColumn {
                    table: source.names.table.clone(),
                    column: name.to_owned(),
                })
            }
        }
        _ if fold::is_null(expr) => Ok(MergeValue::Literal(None)),
        _ => fold::fold_scalar(expr, column.ty())
            .map(|v| MergeValue::Literal(Some(v)))
            .map_err(|err| fold_err_to_dml(err, table, column)),
    }
}

/// Finish a source binding: a table source carries its name + columns for the
/// engine's snapshot read; a `VALUES` source folds each **used** column's cells
/// to the type its use sites required (an unused column's literals are never
/// read and stay unfolded as `None`).
fn fold_source(binding: SourceBinding, factor: &TableFactor) -> Result<MergeSource, DmlError> {
    if let Some((schema_id, declared)) = binding.declared {
        return Ok(MergeSource::Table {
            name: binding.names.table,
            schema_id,
            columns: binding.columns.into_iter().zip(declared).collect(),
        });
    }
    let TableFactor::Derived { subquery, .. } = factor else {
        return Err(DmlError::Unsupported(
            "a MERGE source other than a table or VALUES".to_owned(),
        ));
    };
    let SetExpr::Values(values) = subquery.body.as_ref() else {
        return Err(DmlError::Unsupported(
            "a MERGE source subquery other than VALUES".to_owned(),
        ));
    };
    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let mut cells = Vec::with_capacity(binding.columns.len());
        for (idx, expr) in row.content.iter().enumerate() {
            let cell = match binding.required[idx] {
                // The column is never referenced: its literal is never read.
                None => None,
                Some(_) if fold::is_null(expr) => None,
                Some(ty) => Some(fold::fold_scalar(expr, ty).map_err(|err| {
                    fold_source_err(err, &binding.names.table, &binding.columns[idx], ty)
                })?),
            };
            cells.push(cell);
        }
        rows.push(cells);
    }
    Ok(MergeSource::Values(rows))
}

/// Name a `VALUES`-source fold failure against the source alias and column —
/// the cells live in the statement, not a real table, but the shape of the
/// error matches the plain DML binder's.
fn fold_source_err(err: fold::FoldError, source: &str, column: &str, ty: LogicalType) -> DmlError {
    match err {
        fold::FoldError::Null => DmlError::NullValue {
            table: source.to_owned(),
            column: column.to_owned(),
        },
        fold::FoldError::TypeMismatch { found } => DmlError::TypeMismatch {
            table: source.to_owned(),
            column: column.to_owned(),
            expected: ty,
            found: found.to_owned(),
        },
        fold::FoldError::BadLiteral { literal, reason } => DmlError::BadLiteral {
            table: source.to_owned(),
            column: column.to_owned(),
            ty,
            literal,
            reason,
        },
        fold::FoldError::UnsupportedType(t) => {
            DmlError::Unsupported(format!("a {t} literal for MERGE source column {column:?}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use stele_catalog::{Catalog, TableTemporal, ValidTimeSpec};
    use stele_common::time::SystemTimeMicros;

    const NOW: SystemTimeMicros = SystemTimeMicros(2_000_000_000_000_000);

    /// A catalog with the target `account (id INT, balance INT)` and a source
    /// `feed (id INT, amount INT)`, both created at system time `1_000`.
    fn catalog() -> Catalog {
        let mut catalog = Catalog::new();
        for (table, value_col) in [("account", "balance"), ("feed", "amount")] {
            catalog
                .create_table(
                    table,
                    vec![
                        ColumnDef::new("id", LogicalType::Int4).expect("col"),
                        ColumnDef::new(value_col, LogicalType::Int4).expect("col"),
                    ],
                    TableTemporal::system_only(),
                    SystemTimeMicros(1_000),
                )
                .expect("create");
        }
        catalog
    }

    fn bind(sql: &str, catalog: &Catalog) -> Result<BoundDml, DmlError> {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1, "expected one statement");
        let ctx = BindContext {
            snapshot: NOW,
            catalog,
        };
        crate::bind_dml(&stmts.remove(0), &ctx)
    }

    #[test]
    fn binds_the_canonical_values_merge() {
        let catalog = catalog();
        assert_eq!(
            bind(
                "MERGE INTO account USING (VALUES (1, 100), (2, 200)) AS s (id, balance) \
                 ON account.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.balance \
                 WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.balance)",
                &catalog
            ),
            Ok(BoundDml::Merge(BoundMerge {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                source: MergeSource::Values(vec![
                    vec![Some(ScalarValue::Int4(1)), Some(ScalarValue::Int4(100))],
                    vec![Some(ScalarValue::Int4(2)), Some(ScalarValue::Int4(200))],
                ]),
                on: 0,
                matched: Some(vec![(0, MergeValue::Source(1))]),
                not_matched: Some(vec![MergeValue::Source(0), MergeValue::Source(1)]),
                matched_valid: None,
                not_matched_valid: None,
            }))
        );
    }

    #[test]
    fn binds_a_table_source_and_aliases() {
        let catalog = catalog();
        // Target alias + source alias; the ON sides may come in either order.
        assert_eq!(
            bind(
                "MERGE INTO account AS t USING feed AS s ON s.id = t.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.amount \
                 WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.amount)",
                &catalog
            ),
            Ok(BoundDml::Merge(BoundMerge {
                table: "account".to_owned(),
                schema_id: SchemaId(1),
                source: MergeSource::Table {
                    name: "feed".to_owned(),
                    schema_id: SchemaId(2),
                    columns: vec![
                        ("id".to_owned(), LogicalType::Int4),
                        ("amount".to_owned(), LogicalType::Int4),
                    ],
                },
                on: 0,
                matched: Some(vec![(0, MergeValue::Source(1))]),
                not_matched: Some(vec![MergeValue::Source(0), MergeValue::Source(1)]),
                matched_valid: None,
                not_matched_valid: None,
            }))
        );
    }

    #[test]
    fn single_arm_forms_bind() {
        let catalog = catalog();
        let only_matched = bind(
            "MERGE INTO account USING (VALUES (1, 5)) AS s (id, v) ON account.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = s.v",
            &catalog,
        )
        .expect("bind");
        let BoundDml::Merge(merge) = only_matched else {
            panic!("expected a MERGE");
        };
        assert!(merge.matched.is_some() && merge.not_matched.is_none());

        let only_insert = bind(
            "MERGE INTO account USING (VALUES (1, 5)) AS s (id, v) ON account.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.v)",
            &catalog,
        )
        .expect("bind");
        let BoundDml::Merge(merge) = only_insert else {
            panic!("expected a MERGE");
        };
        assert!(merge.matched.is_none() && merge.not_matched.is_some());
    }

    #[test]
    fn insert_arm_without_a_column_list_maps_positionally() {
        let catalog = catalog();
        let bound = bind(
            "MERGE INTO account USING (VALUES (1, 5)) AS s (id, v) ON account.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)",
            &catalog,
        )
        .expect("bind");
        let BoundDml::Merge(merge) = bound else {
            panic!("expected a MERGE");
        };
        assert_eq!(
            merge.not_matched,
            Some(vec![MergeValue::Source(0), MergeValue::Source(1)])
        );
    }

    #[test]
    fn arm_values_may_be_literals_and_nulls() {
        let catalog = catalog();
        let bound = bind(
            "MERGE INTO account USING (VALUES (1)) AS s (id) ON account.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = NULL \
             WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, 42)",
            &catalog,
        )
        .expect("bind");
        let BoundDml::Merge(merge) = bound else {
            panic!("expected a MERGE");
        };
        assert_eq!(merge.matched, Some(vec![(0, MergeValue::Literal(None))]));
        assert_eq!(
            merge.not_matched,
            Some(vec![
                MergeValue::Source(0),
                MergeValue::Literal(Some(ScalarValue::Int4(42))),
            ])
        );
    }

    #[test]
    fn an_unused_values_column_stays_unfolded() {
        // `extra` is never referenced: its cells are not folded (no type to fold
        // to) and bind as None without erroring — even a string in an
        // otherwise-int batch.
        let catalog = catalog();
        let bound = bind(
            "MERGE INTO account USING (VALUES (1, 'ignored')) AS s (id, extra) \
             ON account.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = 0",
            &catalog,
        )
        .expect("bind");
        let BoundDml::Merge(merge) = bound else {
            panic!("expected a MERGE");
        };
        assert_eq!(
            merge.source,
            MergeSource::Values(vec![vec![Some(ScalarValue::Int4(1)), None]])
        );
    }

    #[test]
    fn conflicting_source_column_types_are_rejected() {
        let catalog = catalog();
        // `s.v` joins the int4 key *and* assigns the text column of `wide` — no
        // single fold type satisfies both.
        let mut catalog2 = catalog;
        catalog2
            .create_table(
                "wide",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("name", LogicalType::Text).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create wide");
        assert!(matches!(
            bind(
                "MERGE INTO wide USING (VALUES (1)) AS s (v) ON wide.id = s.v \
                 WHEN MATCHED THEN UPDATE SET name = s.v",
                &catalog2
            ),
            Err(DmlError::Unsupported(msg)) if msg.contains("both")
        ));
    }

    #[test]
    fn a_mistyped_table_source_column_is_rejected() {
        let catalog = catalog();
        let mut catalog = catalog;
        catalog
            .create_table(
                "named",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("label", LogicalType::Text).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create named");
        // feed.amount is int4; assigning it to the text column `label` is a
        // declared-type conflict, caught at bind.
        assert!(matches!(
            bind(
                "MERGE INTO named USING feed AS s ON named.id = s.id \
                 WHEN MATCHED THEN UPDATE SET label = s.amount",
                &catalog
            ),
            Err(DmlError::Unsupported(msg)) if msg.contains("required")
        ));
    }

    #[test]
    fn the_rejected_shapes_name_themselves() {
        let catalog = catalog();
        let cases: &[(&str, &str)] = &[
            (
                // WHEN NOT MATCHED BY SOURCE is explicitly out of scope.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN NOT MATCHED BY SOURCE THEN DELETE",
                "BY SOURCE",
            ),
            (
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN MATCHED THEN DELETE",
                "DELETE",
            ),
            (
                // A clause predicate.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN MATCHED AND s.v > 0 THEN UPDATE SET balance = s.v",
                "predicate",
            ),
            (
                // The ON must join the business key.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.balance = s.v \
                 WHEN MATCHED THEN UPDATE SET balance = s.v",
                "non-key",
            ),
            (
                // ... with one equality, not a conjunction.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) \
                 ON account.id = s.id AND s.v > 0 \
                 WHEN MATCHED THEN UPDATE SET balance = s.v",
                "ON condition",
            ),
            (
                // Referencing the target row in an arm value.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = account.balance",
                "target row",
            ),
            (
                // A VALUES source must name its columns.
                "MERGE INTO account USING (VALUES (1, 2)) AS s ON account.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.v",
                "named columns",
            ),
            (
                // Updating the business key.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN MATCHED THEN UPDATE SET id = s.v",
                "",
            ),
            (
                // A multi-row VALUES inside the INSERT arm.
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.v), (9, 9)",
                "multi-row",
            ),
        ];
        for (sql, needle) in cases {
            let err = bind(sql, &catalog).expect_err(sql);
            let msg = err.to_string();
            assert!(
                msg.contains(needle),
                "{sql:?} must reject mentioning {needle:?}; got {msg:?}"
            );
        }
    }

    #[test]
    fn an_unqualified_non_key_non_source_on_operand_names_the_requirement() {
        // A bare name in the ON that is neither the business key nor a source
        // column is not a source-column reference — the error must say the
        // operand has to be the key or a source column, not blame the source
        // table (which would mislead for `balance` — a non-key target column).
        let catalog = catalog();
        let err = bind(
            "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON balance = s.v \
             WHEN MATCHED THEN UPDATE SET balance = s.v",
            &catalog,
        )
        .expect_err("balance is neither the key nor a source column");
        assert!(
            matches!(&err, DmlError::Unsupported(msg) if msg.contains("ON condition")),
            "got {err:?}"
        );
    }

    /// `catalog()` plus a valid-time target `vt (id, balance, vf, vt)` whose
    /// period columns are named by `VALID TIME (vf, vt)` ([STL-235]).
    fn vt_catalog() -> Catalog {
        let mut catalog = catalog();
        catalog
            .create_table(
                "vt",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                    ColumnDef::new("vf", LogicalType::Timestamp).expect("col"),
                    ColumnDef::new("vt", LogicalType::Timestamp).expect("col"),
                ],
                TableTemporal::with_valid_time(ValidTimeSpec::new("vf", "vt").expect("spec")),
                SystemTimeMicros(1_000),
            )
            .expect("create vt");
        catalog
    }

    fn ts(micros: i64) -> MergeValue {
        MergeValue::Literal(Some(ScalarValue::Timestamp(micros)))
    }

    #[test]
    fn valid_time_merge_lifts_each_arms_interval() {
        // STL-235: a MERGE into a valid-time table is no longer refused — the arms
        // carry the period columns (folded as instants) and each arm's `[from, to)`
        // rides its own interval field, the matched arm closing-and-opening, the
        // not-matched arm inserting.
        let catalog = vt_catalog();
        let bound = bind(
            "MERGE INTO vt USING (VALUES (1, 100), (3, 300)) AS s (id, balance) ON vt.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = 10, vt = 20 \
             WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) VALUES (s.id, s.balance, 30, 40)",
            &catalog,
        )
        .expect("bind");
        let BoundDml::Merge(merge) = bound else {
            panic!("expected a MERGE");
        };
        // Matched: value column from the source, the period columns as instant
        // cells (vf=10, vt=20), and the framed interval [10, 20).
        assert_eq!(
            merge.matched,
            Some(vec![(0, MergeValue::Source(1)), (1, ts(10)), (2, ts(20))])
        );
        assert_eq!(
            merge.matched_valid,
            Some(Interval::new(10, 20).expect("iv"))
        );
        // Not-matched: aligned to all columns, the period bounds as instants, and
        // its own interval [30, 40).
        assert_eq!(
            merge.not_matched,
            Some(vec![
                MergeValue::Source(0),
                MergeValue::Source(1),
                ts(30),
                ts(40),
            ])
        );
        assert_eq!(
            merge.not_matched_valid,
            Some(Interval::new(30, 40).expect("iv"))
        );
    }

    #[test]
    fn valid_time_merge_defaults_the_open_end_bound() {
        // Naming only the start bound opens `[from, +∞)` in both arms — the omitted
        // `vt` defaults to VALID_TIME_OPEN, synthesized into the cell and the
        // interval alike, exactly as a plain valid-time INSERT/UPDATE.
        let catalog = vt_catalog();
        let BoundDml::Merge(merge) = bind(
            "MERGE INTO vt USING (VALUES (1, 100)) AS s (id, balance) ON vt.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = 10 \
             WHEN NOT MATCHED THEN INSERT (id, balance, vf) VALUES (s.id, s.balance, 10)",
            &catalog,
        )
        .expect("bind") else {
            panic!("expected a MERGE");
        };
        let open = VALID_TIME_OPEN.0;
        assert_eq!(
            merge.matched,
            Some(vec![(0, MergeValue::Source(1)), (1, ts(10)), (2, ts(open))])
        );
        assert_eq!(
            merge.matched_valid,
            Some(Interval::new(10, open).expect("open"))
        );
        assert_eq!(
            merge.not_matched,
            Some(vec![
                MergeValue::Source(0),
                MergeValue::Source(1),
                ts(10),
                ts(open)
            ])
        );
        assert_eq!(
            merge.not_matched_valid,
            Some(Interval::new(10, open).expect("open"))
        );
    }

    #[test]
    fn valid_time_merge_now_relative_bounds_fold_like_as_of() {
        // The bounds reuse the AS OF resolver: `now()` and `now() ± interval` fold
        // against the bind snapshot, the same as a plain valid-time write.
        let catalog = vt_catalog();
        let now = NOW.0;
        let day = 86_400_000_000i64;
        let BoundDml::Merge(merge) = bind(
            "MERGE INTO vt USING (VALUES (1, 100)) AS s (id, balance) ON vt.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = now(), vt = now() + interval '1 day'",
            &catalog,
        )
        .expect("bind") else {
            panic!("expected a MERGE");
        };
        assert_eq!(
            merge.matched_valid,
            Some(Interval::new(now, now + day).expect("iv"))
        );
    }

    #[test]
    fn valid_time_merge_without_the_start_bound_is_rejected() {
        // Every new valid-time version must say when it begins being true: a
        // matched arm that omits `vf`, and a not-matched arm that omits it, both
        // fail with ValidTimeStartRequired — mirroring the plain INSERT/UPDATE.
        let catalog = vt_catalog();
        assert_eq!(
            bind(
                "MERGE INTO vt USING (VALUES (1, 100)) AS s (id, balance) ON vt.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.balance",
                &catalog
            ),
            Err(DmlError::ValidTimeStartRequired {
                table: "vt".to_owned(),
                column: "vf".to_owned(),
            })
        );
        assert_eq!(
            bind(
                "MERGE INTO vt USING (VALUES (1, 100)) AS s (id, balance) ON vt.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.balance)",
                &catalog
            ),
            Err(DmlError::ValidTimeStartRequired {
                table: "vt".to_owned(),
                column: "vf".to_owned(),
            })
        );
    }

    #[test]
    fn valid_time_merge_empty_or_reversed_interval_is_rejected() {
        let catalog = vt_catalog();
        assert_eq!(
            bind(
                "MERGE INTO vt USING (VALUES (1, 100)) AS s (id, balance) ON vt.id = s.id \
                 WHEN MATCHED THEN UPDATE SET vf = 20, vt = 10",
                &catalog
            ),
            Err(DmlError::EmptyValidInterval {
                table: "vt".to_owned(),
                from: 20,
                to: 10,
            })
        );
    }

    #[test]
    fn valid_time_merge_source_column_period_bound_is_rejected() {
        // A per-source-row valid interval (a source column feeding a period
        // boundary) is a deferred follow-up — rejected with a clear error, never a
        // wrong instant. Both the qualified (`s.vfrom`) and bare (`vfrom`) shapes.
        let catalog = vt_catalog();
        for set in ["vf = s.vfrom", "vf = vfrom"] {
            let sql = format!(
                "MERGE INTO vt USING (VALUES (1, 100, 5)) AS s (id, balance, vfrom) ON vt.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.balance, {set}"
            );
            assert!(
                matches!(bind(&sql, &catalog), Err(DmlError::Unsupported(msg)) if msg.contains("source column")),
                "{set:?} must reject a source-column period bound"
            );
        }
        // A *qualified* source reference whose column doesn't exist is a precise
        // UnknownColumn, not a misleading "source column" bound.
        assert_eq!(
            bind(
                "MERGE INTO vt USING (VALUES (1, 100, 5)) AS s (id, balance, vfrom) ON vt.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = s.nonesuch",
                &catalog
            ),
            Err(DmlError::UnknownColumn {
                table: "s".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn merge_into_a_system_only_table_carries_no_interval() {
        // The system-only path is unchanged: both arms bind with `None` intervals,
        // byte-for-byte the pre-STL-235 plan.
        let catalog = catalog();
        let BoundDml::Merge(merge) = bind(
            "MERGE INTO account USING (VALUES (1, 100)) AS s (id, balance) ON account.id = s.id \
             WHEN MATCHED THEN UPDATE SET balance = s.balance \
             WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.balance)",
            &catalog,
        )
        .expect("bind") else {
            panic!("expected a MERGE");
        };
        assert_eq!(merge.matched_valid, None);
        assert_eq!(merge.not_matched_valid, None);
    }

    #[test]
    fn a_null_literal_insert_key_is_rejected_at_bind() {
        let catalog = catalog();
        assert_eq!(
            bind(
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (NULL, s.v)",
                &catalog
            ),
            Err(DmlError::NullValue {
                table: "account".to_owned(),
                column: "id".to_owned(),
            })
        );
    }

    #[test]
    fn unknown_names_are_reported() {
        let catalog = catalog();
        assert_eq!(
            bind(
                "MERGE INTO ghost USING (VALUES (1)) AS s (id) ON ghost.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = 0",
                &catalog
            ),
            Err(DmlError::UnknownTable("ghost".to_owned()))
        );
        assert_eq!(
            bind(
                "MERGE INTO account USING (VALUES (1)) AS s (id) ON account.id = s.nonesuch \
                 WHEN MATCHED THEN UPDATE SET balance = 0",
                &catalog
            ),
            Err(DmlError::UnknownColumn {
                table: "s".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
        assert_eq!(
            bind(
                "MERGE INTO account USING (VALUES (1)) AS s (id) ON account.id = s.id \
                 WHEN MATCHED THEN UPDATE SET nonesuch = 0",
                &catalog
            ),
            Err(DmlError::UnknownColumn {
                table: "account".to_owned(),
                column: "nonesuch".to_owned(),
            })
        );
    }

    #[test]
    fn values_row_width_must_match_the_alias_columns() {
        let catalog = catalog();
        assert_eq!(
            bind(
                "MERGE INTO account USING (VALUES (1, 2, 3)) AS s (id, v) ON account.id = s.id \
                 WHEN MATCHED THEN UPDATE SET balance = s.v",
                &catalog
            ),
            Err(DmlError::ColumnCountMismatch {
                expected: 2,
                found: 3,
            })
        );
    }

    #[test]
    fn insert_arm_must_supply_every_target_column() {
        let catalog = catalog();
        assert_eq!(
            bind(
                "MERGE INTO account USING (VALUES (1, 2)) AS s (id, v) ON account.id = s.id \
                 WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
                &catalog
            ),
            Err(DmlError::MissingColumn {
                table: "account".to_owned(),
                column: "balance".to_owned(),
            })
        );
    }
}
