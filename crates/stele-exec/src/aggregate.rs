//! Hash **GROUP BY + aggregates** — `COUNT` / `SUM` / `MIN` / `MAX` / `AVG` over
//! a batch, grouped by zero or more keys ([STL-171] `[C11]`).
//!
//! This is the analytical bread-and-butter operator. It builds on the vectorized
//! scalar evaluator ([`eval_expr`], `[C10]`): the grouping keys and each
//! aggregate's argument are arbitrary [`Expr`]s evaluated a whole batch at a time
//! into [`Vector`]s, then folded per group.
//!
//! ## Currency: [`Vector`], not the storage [`Column`](crate::Column)
//!
//! Aggregation is over a query's **value columns** — the decoded, per-cell
//! nullable [`Vector`]s the evaluator works in — not the storage-shaped
//! `(ColumnId, Column)` [`Batch`](crate::Batch) a [`SnapshotScan`](crate::SnapshotScan)
//! emits (whose value columns are packed opaquely into one payload). So the
//! operator takes its input as `&[Vector]` (one per query column, addressed by
//! position, exactly as [`eval_expr`] addresses them) and produces its output as
//! [`Vector`]s, leaving the decode of storage columns into vectors to the caller
//! (the engine's `run_select`, via [`Vector::from_column`]). This is the same
//! split the [`Filter`](crate::Filter) operator draws.
//!
//! ## Grouping & NULL semantics (SQL three-valued logic)
//!
//! * Rows are bucketed by the tuple of their grouping-key values. A NULL key is
//!   its own group — every row whose key tuple is NULL-in-the-same-positions
//!   lands together (SQL groups NULLs, unlike `=`).
//! * `COUNT(*)` counts rows; `COUNT(expr)` counts the **non-NULL** values of
//!   `expr`. Both are never NULL (`0` for an empty group).
//! * `SUM` / `AVG` skip NULL inputs; over a group with no non-NULL value the
//!   result is NULL. `SUM` accumulates in `i128` and narrows to `INT8`; a sum
//!   that overflows `i64` yields NULL — the same "overflow ⇒ NULL" rule the
//!   evaluator's arithmetic follows.
//! * `MIN` / `MAX` skip NULLs; over an all-NULL (or empty) group the result is
//!   NULL.
//! * An **ungrouped** aggregate (no grouping keys) always emits exactly one row,
//!   even over zero input rows — `SELECT COUNT(*) FROM empty` is `0`, not no
//!   rows. A **grouped** aggregate over zero rows emits zero rows.
//!
//! ## Result types
//!
//! `COUNT` / `SUM` produce [`INT8`](stele_common::types::LogicalType::Int8);
//! `AVG` produces [`FLOAT8`](stele_common::types::LogicalType::Float8) — the
//! exact fractional mean (`SUM / COUNT` as `f64`, no truncation), now that a
//! fractional type exists ([STL-209]; it returned the truncated integer mean
//! before). `MIN` / `MAX` produce their argument's type. The `i128` sum is
//! converted to `f64` once and divided by the count, so the result is the
//! nearest double to the true rational mean.
//!
//! ## Output shape
//!
//! Output order is deterministic — groups are emitted sorted by their encoded key
//! tuple (SQL does not order grouped results without `ORDER BY`; a stable order
//! keeps the simulation reproducible). [`AggregateOutput`] returns the grouping
//! columns and the aggregate columns separately, each a [`Vector`] with one cell
//! per group, so the caller can interleave them back into SELECT-list order.
//!
//! [STL-171]: https://allegromusic.atlassian.net/browse/STL-171
//! [STL-209]: https://allegromusic.atlassian.net/browse/STL-209

use std::cmp::Ordering;
use std::collections::BTreeMap;

use stele_common::types::ScalarValue;

use crate::expr::{Expr, ExprError, Vector, eval_expr};

/// Which aggregate to compute over a group's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    /// `COUNT` — number of rows (`COUNT(*)`) or of non-NULL argument values
    /// (`COUNT(expr)`). Result `INT8`, never NULL.
    Count,
    /// `SUM` — total of the non-NULL integer arguments. Result `INT8`, NULL over
    /// an all-NULL group or on `i64` overflow.
    Sum,
    /// `MIN` — least non-NULL argument. Result the argument's type, NULL over an
    /// all-NULL group.
    Min,
    /// `MAX` — greatest non-NULL argument. Result the argument's type, NULL over
    /// an all-NULL group.
    Max,
    /// `AVG` — exact fractional mean of the non-NULL integer arguments
    /// (`SUM / COUNT` as `f64`). Result `FLOAT8`, NULL over an all-NULL group
    /// ([STL-209]).
    Avg,
}

/// One aggregate to compute: a function over an optional argument expression.
///
/// `arg` is the expression the aggregate folds (e.g. `Expr::Column(2)` for
/// `SUM(amount)`); it is `None` only for `COUNT(*)`, which counts rows rather
/// than values. `SUM` / `AVG` / `MIN` / `MAX` always carry an argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregator {
    /// The aggregate function.
    pub func: AggregateFunc,
    /// The argument expression, or `None` for `COUNT(*)`.
    pub arg: Option<Expr>,
}

/// The result of a [`hash_aggregate`]: one row per group, columns split into the
/// grouping keys and the aggregates.
///
/// Every [`Vector`] in [`groups`](Self::groups) and [`aggregates`](Self::aggregates)
/// has exactly [`num_groups`](Self::num_groups) cells, aligned row-wise: group
/// `g`'s output is `groups[*][g]` followed by `aggregates[*][g]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateOutput {
    /// One column per grouping key, in `group_keys` order.
    pub groups: Vec<Vector>,
    /// One column per [`Aggregator`], in input order.
    pub aggregates: Vec<Vector>,
    /// The number of groups (output rows). Always `1` for an ungrouped aggregate.
    pub num_groups: usize,
}

/// Group `columns` by `group_keys` and fold each [`Aggregator`] over the groups.
///
/// `columns` are the input value columns (one [`Vector`] per query column,
/// addressed by position); `rows` is their shared height. `group_keys` and each
/// aggregator's `arg` are [`Expr`]s over those columns, evaluated a whole batch at
/// a time. The module documentation covers the NULL and ordering semantics.
///
/// # Errors
///
/// [`ExprError`] if a grouping key or aggregate argument is a structurally
/// invalid expression (an out-of-range column, a type mismatch). Data NULLs and
/// integer overflow are handled in-band as NULL output cells, never errors.
pub fn hash_aggregate(
    group_keys: &[Expr],
    aggregators: &[Aggregator],
    columns: &[Vector],
    rows: usize,
) -> Result<AggregateOutput, ExprError> {
    // Evaluate the grouping keys and each aggregator's argument once over the
    // whole batch. `COUNT(*)` has no argument, so its slot is `None`.
    let key_vectors: Vec<Vector> = group_keys
        .iter()
        .map(|expr| eval_expr(expr, columns, rows))
        .collect::<Result<_, _>>()?;
    let arg_vectors: Vec<Option<Vector>> = aggregators
        .iter()
        .map(|agg| {
            agg.arg
                .as_ref()
                .map(|e| eval_expr(e, columns, rows))
                .transpose()
        })
        .collect::<Result<_, _>>()?;

    // Groups keyed by the encoded key tuple, kept in a `BTreeMap` so output order
    // is deterministic (sorted by the encoded key) without a separate sort.
    let mut groups: BTreeMap<Vec<Option<Vec<u8>>>, GroupState> = BTreeMap::new();

    // An ungrouped aggregate always emits one row, even over zero input rows, so
    // seed the single whole-table group before scanning.
    if group_keys.is_empty() {
        groups.insert(Vec::new(), GroupState::new(0, aggregators));
    }

    for r in 0..rows {
        let key: Vec<Option<Vec<u8>>> = key_vectors
            .iter()
            .map(|v| v.get(r).as_ref().map(encode_scalar))
            .collect();
        let state = groups
            .entry(key)
            .or_insert_with(|| GroupState::new(r, aggregators));
        for (k, slot) in state.accs.iter_mut().enumerate() {
            accumulate(slot, arg_vectors[k].as_ref(), r);
        }
    }

    let ordered: Vec<GroupState> = groups.into_values().collect();
    let num_groups = ordered.len();

    // Grouping-key output: gather each key vector at one representative row per
    // group (its first occurrence) — which carries that group's key value,
    // including a NULL key.
    let representatives: Vec<Option<usize>> =
        ordered.iter().map(|g| Some(g.representative)).collect();
    let group_cols: Vec<Vector> = key_vectors
        .iter()
        .map(|v| v.gather(&representatives))
        .collect();

    // Aggregate output: one cell per group, in group (sorted-key) order.
    let aggregates: Vec<Vector> = aggregators
        .iter()
        .enumerate()
        .map(|(k, agg)| build_aggregate_column(agg.func, &ordered, k, arg_vectors[k].as_ref()))
        .collect();

    Ok(AggregateOutput {
        groups: group_cols,
        aggregates,
        num_groups,
    })
}

/// Assemble one aggregate's output column across `groups`, in order.
fn build_aggregate_column(
    func: AggregateFunc,
    groups: &[GroupState],
    k: usize,
    arg: Option<&Vector>,
) -> Vector {
    match func {
        // `COUNT` / `SUM` are computed integers, built directly as `INT8`.
        // COUNT is never NULL, so its cells are always `Some`.
        AggregateFunc::Count => Vector::Int8(
            groups
                .iter()
                .map(|g| Some(g.accs[k].count_value()))
                .collect(),
        ),
        AggregateFunc::Sum => Vector::Int8(groups.iter().map(|g| g.accs[k].sum_value()).collect()),
        // `AVG` is the fractional mean, a `FLOAT8` column carrying each group's
        // mean as IEEE-754 bits (NULL over an all-NULL group).
        AggregateFunc::Avg => Vector::Float8(
            groups
                .iter()
                .map(|g| g.accs[k].avg_value().map(f64::to_bits))
                .collect(),
        ),
        // `MIN` / `MAX` are a value of the argument's type — gathered from the
        // argument vector at the row that held each group's extreme (or `None`,
        // a NULL cell, for an all-NULL group).
        AggregateFunc::Min | AggregateFunc::Max => {
            let arg = arg.expect("MIN/MAX carries an argument");
            let extremes: Vec<Option<usize>> =
                groups.iter().map(|g| g.accs[k].extreme_row()).collect();
            arg.gather(&extremes)
        }
    }
}

/// One group's running state: a representative row (for its grouping-key values)
/// and one accumulator per aggregate.
struct GroupState {
    /// The first input row that fell into this group — gathered to recover the
    /// group's key values for output.
    representative: usize,
    /// One accumulator per [`Aggregator`], in input order.
    accs: Vec<Acc>,
}

impl GroupState {
    fn new(representative: usize, aggregators: &[Aggregator]) -> Self {
        Self {
            representative,
            accs: aggregators.iter().map(|a| Acc::new(a.func)).collect(),
        }
    }
}

/// One aggregate's accumulator. The variant always matches its aggregator's
/// [`AggregateFunc`] (set in [`Acc::new`]).
enum Acc {
    /// `COUNT` running tally.
    Count(i64),
    /// `SUM`: `i128` running total and whether any non-NULL value was seen.
    Sum { acc: i128, any: bool },
    /// `AVG`: `i128` running total and the count of non-NULL values.
    Avg { acc: i128, count: i64 },
    /// `MIN`: the row holding the least value seen, or `None` if all NULL so far.
    Min { row: Option<usize> },
    /// `MAX`: the row holding the greatest value seen, or `None` if all NULL.
    Max { row: Option<usize> },
}

impl Acc {
    const fn new(func: AggregateFunc) -> Self {
        match func {
            AggregateFunc::Count => Self::Count(0),
            AggregateFunc::Sum => Self::Sum { acc: 0, any: false },
            AggregateFunc::Avg => Self::Avg { acc: 0, count: 0 },
            AggregateFunc::Min => Self::Min { row: None },
            AggregateFunc::Max => Self::Max { row: None },
        }
    }

    /// `COUNT`'s tally (never NULL — `0` for an empty group). The caller wraps it
    /// in `Some` for the output column.
    const fn count_value(&self) -> i64 {
        match self {
            Self::Count(c) => *c,
            // Unreachable: a COUNT aggregator always builds an `Acc::Count`.
            _ => 0,
        }
    }

    /// `SUM` narrowed to `INT8`: NULL over an all-NULL group or on `i64` overflow.
    fn sum_value(&self) -> Option<i64> {
        match self {
            Self::Sum { acc, any } => any.then(|| i64::try_from(*acc).ok()).flatten(),
            _ => None,
        }
    }

    /// `AVG` as the exact fractional mean (`acc / count` in `f64`): NULL over an
    /// all-NULL group. The `i128` total is converted to `f64` once and divided by
    /// the count, yielding the nearest double to the true rational mean.
    #[allow(clippy::cast_precision_loss)] // the mean is fractional; f64 is the result type
    fn avg_value(&self) -> Option<f64> {
        match self {
            Self::Avg { acc, count } => (*count != 0).then(|| *acc as f64 / *count as f64),
            _ => None,
        }
    }

    /// The row holding a `MIN` / `MAX` group's extreme value, or `None` for an
    /// all-NULL group.
    const fn extreme_row(&self) -> Option<usize> {
        match self {
            Self::Min { row } | Self::Max { row } => *row,
            _ => None,
        }
    }
}

/// Fold input row `r` into `acc`. `arg` is the aggregate's evaluated argument
/// column (`None` only for `COUNT(*)`).
fn accumulate(acc: &mut Acc, arg: Option<&Vector>, r: usize) {
    match acc {
        Acc::Count(c) => {
            // `COUNT(*)` (no argument) counts every row; `COUNT(expr)` counts only
            // non-NULL argument values.
            let counted = arg.is_none_or(|v| v.get(r).is_some());
            if counted {
                *c += 1;
            }
        }
        Acc::Sum { acc, any } => {
            if let Some(value) = arg.and_then(|v| v.get(r)) {
                *acc += scalar_i128(&value);
                *any = true;
            }
        }
        Acc::Avg { acc, count } => {
            if let Some(value) = arg.and_then(|v| v.get(r)) {
                *acc += scalar_i128(&value);
                *count += 1;
            }
        }
        Acc::Min { row } => update_extreme(row, arg, r, Ordering::Less),
        Acc::Max { row } => update_extreme(row, arg, r, Ordering::Greater),
    }
}

/// Update a `MIN` / `MAX` accumulator: adopt row `r` when its (non-NULL) argument
/// value compares `want` against the incumbent extreme. NULL inputs are ignored.
fn update_extreme(row: &mut Option<usize>, arg: Option<&Vector>, r: usize, want: Ordering) {
    let arg = arg.expect("MIN/MAX carries an argument");
    let Some(candidate) = arg.get(r) else {
        return; // NULL is ignored by MIN/MAX.
    };
    match *row {
        None => *row = Some(r),
        Some(best) => {
            // The tracked best row is always non-NULL (only non-NULL rows are
            // ever stored), so its value is present.
            if let Some(incumbent) = arg.get(best) {
                if scalar_cmp(&candidate, &incumbent) == want {
                    *row = Some(r);
                }
            }
        }
    }
}

/// Total-order comparison of two same-typed scalar values, over the evaluator's
/// scalar set. `MIN` / `MAX` fold a single argument column, so both operands come
/// from one [`Vector`] and are the same type by construction — a mixed pairing is
/// impossible here. Every type the evaluator can read into a [`Vector`] has an
/// arm (STL-207 broadened the set to the temporal / `uuid` / `bytea` / `period`
/// types); only `float8`, which no column decodes into, is left out, so the
/// fallthrough still **fails fast** rather than silently mis-ordering.
fn scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    match (a, b) {
        // `i32`- and `i64`-payload types share a comparison; group them so the
        // identical arms merge.
        (ScalarValue::Int4(x), ScalarValue::Int4(y))
        | (ScalarValue::Date(x), ScalarValue::Date(y)) => x.cmp(y),
        (ScalarValue::Int8(x), ScalarValue::Int8(y))
        | (ScalarValue::Timestamp(x), ScalarValue::Timestamp(y))
        | (ScalarValue::TimestampTz(x), ScalarValue::TimestampTz(y)) => x.cmp(y),
        (ScalarValue::Bool(x), ScalarValue::Bool(y)) => x.cmp(y),
        (ScalarValue::Text(x), ScalarValue::Text(y)) => x.cmp(y),
        (ScalarValue::Uuid(x), ScalarValue::Uuid(y)) => x.cmp(y),
        (ScalarValue::Bytea(x), ScalarValue::Bytea(y)) => x.cmp(y),
        (ScalarValue::Period(x), ScalarValue::Period(y)) => x.cmp(y),
        _ => unreachable!(
            "MIN/MAX compares values from one column, so both operands share a type \
             the evaluator can read: got {} vs {}",
            a.logical_type(),
            b.logical_type()
        ),
    }
}

/// The `i128` value of an integer scalar — `SUM` / `AVG` accumulate here to dodge
/// `i64` overflow mid-fold. The binder restricts `SUM` / `AVG` arguments to
/// integers, so a non-integer is a contract break: this **fails fast** with a
/// panic rather than silently summing a wrong `0`.
fn scalar_i128(value: &ScalarValue) -> i128 {
    match value {
        ScalarValue::Int4(v) => i128::from(*v),
        ScalarValue::Int8(v) => i128::from(*v),
        other => unreachable!(
            "SUM/AVG argument is binder-restricted to integers, got {}",
            other.logical_type()
        ),
    }
}

/// Encode a scalar to its canonical bytes — the grouping-key tuple's identity.
/// Within a key position every value shares one type, so the type-directed
/// encoding is injective there and equal values hash to one group.
fn encode_scalar(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use stele_common::types::LogicalType;

    /// `Expr::Column(i)` shorthand.
    const fn col(i: usize) -> Expr {
        Expr::col(i)
    }

    const fn count_star() -> Aggregator {
        Aggregator {
            func: AggregateFunc::Count,
            arg: None,
        }
    }

    const fn agg(func: AggregateFunc, arg: usize) -> Aggregator {
        Aggregator {
            func,
            arg: Some(col(arg)),
        }
    }

    #[test]
    fn ungrouped_over_zero_rows_emits_one_row() {
        // `SELECT COUNT(*), SUM(x) FROM empty` → exactly one row: COUNT 0, SUM NULL.
        let columns = vec![Vector::Int8(Vec::new())];
        let out = hash_aggregate(
            &[],
            &[count_star(), agg(AggregateFunc::Sum, 0)],
            &columns,
            0,
        )
        .expect("aggregate");
        assert_eq!(out.num_groups, 1);
        assert!(out.groups.is_empty());
        assert_eq!(out.aggregates[0], Vector::Int8(vec![Some(0)]));
        assert_eq!(out.aggregates[1], Vector::Int8(vec![None]));
    }

    #[test]
    fn grouped_over_zero_rows_emits_no_rows() {
        let columns = vec![Vector::Int4(Vec::new())];
        let out = hash_aggregate(&[col(0)], &[count_star()], &columns, 0).expect("aggregate");
        assert_eq!(out.num_groups, 0);
        assert_eq!(out.groups[0], Vector::Int4(Vec::new()));
        assert_eq!(out.aggregates[0], Vector::Int8(Vec::new()));
    }

    #[test]
    fn count_star_counts_rows_count_arg_skips_nulls() {
        // group key in col 0, counted value in col 1.
        let keys = Vector::Int4(vec![Some(1), Some(1), Some(2), Some(2)]);
        let vals = Vector::Int4(vec![Some(10), None, Some(30), Some(40)]);
        let out = hash_aggregate(
            &[col(0)],
            &[count_star(), agg(AggregateFunc::Count, 1)],
            &[keys, vals],
            4,
        )
        .expect("aggregate");
        // Groups sorted by key: 1 then 2.
        assert_eq!(out.groups[0], Vector::Int4(vec![Some(1), Some(2)]));
        assert_eq!(out.aggregates[0], Vector::Int8(vec![Some(2), Some(2)])); // COUNT(*)
        assert_eq!(out.aggregates[1], Vector::Int8(vec![Some(1), Some(2)])); // COUNT(val): one NULL in group 1
    }

    #[test]
    fn sum_skips_nulls_and_is_null_for_all_null_group() {
        let keys = Vector::Int4(vec![Some(1), Some(1), Some(2)]);
        let vals = Vector::Int8(vec![Some(100), None, None]);
        let out = hash_aggregate(&[col(0)], &[agg(AggregateFunc::Sum, 1)], &[keys, vals], 3)
            .expect("aggregate");
        // group 1: 100 + NULL = 100; group 2: only NULL = NULL.
        assert_eq!(out.aggregates[0], Vector::Int8(vec![Some(100), None]));
    }

    #[test]
    fn sum_over_int4_widens_past_i32() {
        // Two i32::MAX values sum past i32 but fit i64 — proving the i128 accumulator.
        let vals = Vector::Int4(vec![Some(i32::MAX), Some(i32::MAX)]);
        let out =
            hash_aggregate(&[], &[agg(AggregateFunc::Sum, 0)], &[vals], 2).expect("aggregate");
        assert_eq!(
            out.aggregates[0],
            Vector::Int8(vec![Some(i64::from(i32::MAX) * 2)])
        );
    }

    #[test]
    fn sum_overflowing_i64_is_null() {
        let vals = Vector::Int8(vec![Some(i64::MAX), Some(i64::MAX)]);
        let out =
            hash_aggregate(&[], &[agg(AggregateFunc::Sum, 0)], &[vals], 2).expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Int8(vec![None]));
    }

    #[test]
    fn min_max_skip_nulls() {
        let keys = Vector::Int4(vec![Some(1), Some(1), Some(1)]);
        let vals = Vector::Int8(vec![Some(5), None, Some(2)]);
        let out = hash_aggregate(
            &[col(0)],
            &[agg(AggregateFunc::Min, 1), agg(AggregateFunc::Max, 1)],
            &[keys, vals],
            3,
        )
        .expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Int8(vec![Some(2)])); // MIN
        assert_eq!(out.aggregates[1], Vector::Int8(vec![Some(5)])); // MAX
    }

    #[test]
    fn min_max_over_text() {
        let vals = Vector::Text(vec![Some("banana".into()), Some("apple".into()), None]);
        let out = hash_aggregate(
            &[],
            &[agg(AggregateFunc::Min, 0), agg(AggregateFunc::Max, 0)],
            &[vals],
            3,
        )
        .expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Text(vec![Some("apple".into())]));
        assert_eq!(out.aggregates[1], Vector::Text(vec![Some("banana".into())]));
    }

    /// MIN/MAX over the STL-207 types (`from_column` now decodes them, so they
    /// reach `scalar_cmp` — previously an `unreachable!` panic).
    #[test]
    fn min_max_over_new_types() {
        // Timestamp orders by its instant.
        let ts = Vector::Timestamp(vec![Some(30), Some(10), None, Some(20)]);
        let out = hash_aggregate(
            &[],
            &[agg(AggregateFunc::Min, 0), agg(AggregateFunc::Max, 0)],
            &[ts],
            4,
        )
        .expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Timestamp(vec![Some(10)]));
        assert_eq!(out.aggregates[1], Vector::Timestamp(vec![Some(30)]));

        // UUID is byte-ordered.
        let lo = [0u8; 16];
        let mut hi = [0u8; 16];
        hi[0] = 1;
        let uuids = Vector::Uuid(vec![Some(hi), Some(lo)]);
        let out = hash_aggregate(
            &[],
            &[agg(AggregateFunc::Min, 0), agg(AggregateFunc::Max, 0)],
            &[uuids],
            2,
        )
        .expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Uuid(vec![Some(lo)]));
        assert_eq!(out.aggregates[1], Vector::Uuid(vec![Some(hi)]));
    }

    #[test]
    fn min_max_all_null_group_is_null() {
        let vals = Vector::Int8(vec![None, None]);
        let out = hash_aggregate(
            &[],
            &[agg(AggregateFunc::Min, 0), agg(AggregateFunc::Max, 0)],
            &[vals],
            2,
        )
        .expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Int8(vec![None]));
        assert_eq!(out.aggregates[1], Vector::Int8(vec![None]));
    }

    #[test]
    fn avg_is_exact_fractional_mean_skipping_nulls() {
        // (100 + 200 + 250) / 3 = 183.333…, the exact f64 mean — *not* truncated
        // to 183. NULL is skipped. The column is FLOAT8, carrying the mean's bits.
        let vals = Vector::Int8(vec![Some(100), Some(200), Some(250), None]);
        let out =
            hash_aggregate(&[], &[agg(AggregateFunc::Avg, 0)], &[vals], 4).expect("aggregate");
        assert_eq!(
            out.aggregates[0],
            Vector::Float8(vec![Some((550.0_f64 / 3.0).to_bits())])
        );
        // And it is genuinely fractional, not the old truncated 183.
        assert_eq!(
            out.aggregates[0].get(0).and_then(|v| v.as_f64()),
            Some(550.0 / 3.0)
        );
    }

    #[test]
    fn avg_all_null_group_is_null() {
        let vals = Vector::Int4(vec![None]);
        let out =
            hash_aggregate(&[], &[agg(AggregateFunc::Avg, 0)], &[vals], 1).expect("aggregate");
        assert_eq!(out.aggregates[0], Vector::Float8(vec![None]));
    }

    #[test]
    fn null_key_forms_its_own_group() {
        // Keys [1, NULL, 1, NULL] → two groups: {1} and {NULL}.
        let keys = Vector::Int4(vec![Some(1), None, Some(1), None]);
        let vals = Vector::Int8(vec![Some(10), Some(20), Some(30), Some(40)]);
        let out = hash_aggregate(
            &[col(0)],
            &[count_star(), agg(AggregateFunc::Sum, 1)],
            &[keys, vals],
            4,
        )
        .expect("aggregate");
        assert_eq!(out.num_groups, 2);
        // Deterministic key order: a NULL key (`None`) sorts before a present one.
        assert_eq!(out.groups[0], Vector::Int4(vec![None, Some(1)]));
        assert_eq!(out.aggregates[0], Vector::Int8(vec![Some(2), Some(2)]));
        // NULL group sums rows 20 + 40 = 60; key-1 group sums 10 + 30 = 40.
        assert_eq!(out.aggregates[1], Vector::Int8(vec![Some(60), Some(40)]));
    }

    #[test]
    fn multi_key_grouping() {
        // Group by (region, tier).
        let region = Vector::Text(vec![Some("e".into()), Some("e".into()), Some("w".into())]);
        let tier = Vector::Int4(vec![Some(1), Some(2), Some(1)]);
        let amount = Vector::Int8(vec![Some(10), Some(20), Some(30)]);
        let out = hash_aggregate(
            &[col(0), col(1)],
            &[agg(AggregateFunc::Sum, 2)],
            &[region, tier, amount],
            3,
        )
        .expect("aggregate");
        assert_eq!(out.num_groups, 3);
        assert_eq!(
            out.groups[0],
            Vector::Text(vec![Some("e".into()), Some("e".into()), Some("w".into())])
        );
        assert_eq!(out.groups[1], Vector::Int4(vec![Some(1), Some(2), Some(1)]));
        assert_eq!(
            out.aggregates[0],
            Vector::Int8(vec![Some(10), Some(20), Some(30)])
        );
    }

    #[test]
    fn distinct_via_group_with_no_aggregate() {
        // `SELECT k FROM t GROUP BY k` — grouping with no aggregate is DISTINCT.
        let keys = Vector::Int4(vec![Some(3), Some(1), Some(3), Some(1), Some(2)]);
        let out = hash_aggregate(&[col(0)], &[], &[keys], 5).expect("aggregate");
        assert_eq!(out.num_groups, 3);
        assert_eq!(out.groups[0], Vector::Int4(vec![Some(1), Some(2), Some(3)]));
        assert!(out.aggregates.is_empty());
    }

    // ---- Differential vs an independent naive reference ----

    /// A tiny deterministic PRNG (SplitMix64) — keeps the differential seeded and
    /// dependency-free.
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// A value in `0..n`.
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        /// A nullable small integer cell.
        fn cell(&mut self) -> Option<i64> {
            // ~1 in 5 cells NULL.
            if self.below(5) == 0 {
                None
            } else {
                Some(i64::try_from(self.below(20)).expect("small") - 10)
            }
        }
    }

    /// The reference: group rows with a plain `HashMap` and compute each aggregate
    /// the obvious way. Returns, per group key (`Option<i64>`), the tuple
    /// `(count_star, count_v, sum, min, max, avg)`. `avg` is the exact fractional
    /// mean as `f64` — the same `i128`-sum-then-one-`f64`-division the operator
    /// does, so the two agree bit-for-bit ([STL-209]).
    #[allow(clippy::type_complexity, clippy::cast_precision_loss)]
    fn reference(
        keys: &[Option<i64>],
        vals: &[Option<i64>],
    ) -> HashMap<Option<i64>, (i64, i64, Option<i64>, Option<i64>, Option<i64>, Option<f64>)> {
        let mut groups: HashMap<Option<i64>, Vec<Option<i64>>> = HashMap::new();
        for (k, v) in keys.iter().zip(vals) {
            groups.entry(*k).or_default().push(*v);
        }
        groups
            .into_iter()
            .map(|(k, vs)| {
                let count_star = i64::try_from(vs.len()).expect("len");
                let present: Vec<i64> = vs.iter().flatten().copied().collect();
                let count_v = i64::try_from(present.len()).expect("len");
                let sum = if present.is_empty() {
                    None
                } else {
                    Some(present.iter().sum())
                };
                let min = present.iter().min().copied();
                let max = present.iter().max().copied();
                let avg = if present.is_empty() {
                    None
                } else {
                    let total: i128 = present.iter().map(|&v| i128::from(v)).sum();
                    Some(total as f64 / count_v as f64)
                };
                (k, (count_star, count_v, sum, min, max, avg))
            })
            .collect()
    }

    #[test]
    fn differential_vs_naive_reference() {
        for seed in 0..64u64 {
            let mut rng = SplitMix64(seed.wrapping_mul(0x1234_5678).wrapping_add(1));
            let n = 1 + usize::try_from(rng.below(40)).expect("below 40 fits usize");
            let keys: Vec<Option<i64>> = (0..n).map(|_| rng.cell()).collect();
            let vals: Vec<Option<i64>> = (0..n).map(|_| rng.cell()).collect();

            let key_col = Vector::Int8(keys.clone());
            let val_col = Vector::Int8(vals.clone());
            let out = hash_aggregate(
                &[col(0)],
                &[
                    count_star(),
                    agg(AggregateFunc::Count, 1),
                    agg(AggregateFunc::Sum, 1),
                    agg(AggregateFunc::Min, 1),
                    agg(AggregateFunc::Max, 1),
                    agg(AggregateFunc::Avg, 1),
                ],
                &[key_col, val_col],
                n,
            )
            .expect("aggregate");

            let want = reference(&keys, &vals);
            assert_eq!(out.num_groups, want.len(), "group count (seed {seed})");

            let Vector::Int8(out_keys) = &out.groups[0] else {
                panic!("int8 key column");
            };
            for (g, key) in out_keys.iter().enumerate() {
                let (cs, cv, sum, min, max, avg) = want[key];
                let cell = |v: &Vector| -> Option<i64> {
                    let Vector::Int8(c) = v else {
                        panic!("int8 aggregate")
                    };
                    c[g]
                };
                // AVG is a FLOAT8 column carrying each mean's bits. Compare those
                // bits against the reference mean's bits: an exact match witnesses
                // the two computed the identical f64, with no `float_cmp` fuzz.
                let avg_bits = |v: &Vector| -> Option<u64> {
                    let Vector::Float8(c) = v else {
                        panic!("float8 aggregate")
                    };
                    c[g]
                };
                assert_eq!(
                    cell(&out.aggregates[0]),
                    Some(cs),
                    "COUNT(*) seed {seed} key {key:?}"
                );
                assert_eq!(
                    cell(&out.aggregates[1]),
                    Some(cv),
                    "COUNT(v) seed {seed} key {key:?}"
                );
                assert_eq!(cell(&out.aggregates[2]), sum, "SUM seed {seed} key {key:?}");
                assert_eq!(cell(&out.aggregates[3]), min, "MIN seed {seed} key {key:?}");
                assert_eq!(cell(&out.aggregates[4]), max, "MAX seed {seed} key {key:?}");
                assert_eq!(
                    avg_bits(&out.aggregates[5]),
                    avg.map(f64::to_bits),
                    "AVG seed {seed} key {key:?}"
                );
            }
        }
    }

    #[test]
    fn out_of_range_grouping_key_is_a_plan_error() {
        let columns = vec![Vector::Int4(vec![Some(1)])];
        assert!(matches!(
            hash_aggregate(&[col(5)], &[count_star()], &columns, 1),
            Err(ExprError::ColumnOutOfRange { .. })
        ));
    }

    /// `SUM` is `INT8`; `AVG` is the fractional `FLOAT8` ([STL-209]).
    #[test]
    fn sum_is_int8_and_avg_is_float8() {
        let vals = Vector::Int4(vec![Some(1), Some(2)]);
        let out = hash_aggregate(
            &[],
            &[agg(AggregateFunc::Sum, 0), agg(AggregateFunc::Avg, 0)],
            &[vals],
            2,
        )
        .expect("aggregate");
        assert_eq!(out.aggregates[0].logical_type(), LogicalType::Int8);
        assert_eq!(out.aggregates[1].logical_type(), LogicalType::Float8);
        // AVG(1, 2) is the exact 1.5, not a truncated 1.
        assert_eq!(out.aggregates[1].get(0).and_then(|v| v.as_f64()), Some(1.5));
    }
}
