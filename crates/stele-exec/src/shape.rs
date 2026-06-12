//! Result shaping — `DISTINCT`, `ORDER BY`, `OFFSET`/`LIMIT` over a
//! materialized result ([STL-263]).
//!
//! These are the three clauses that reshape a query's *result set* after every
//! row-producing stage (scan, filter, aggregation) has run. They share one
//! executor currency: a **selection vector** of row indices into the
//! materialized output columns (the same row-selection idea the
//! [`Batch::selection`](crate::Batch) carries, [STL-214]). Each operation
//! permutes, prunes, or slices the *indices*; the caller gathers its final rows
//! through the surviving indices once, at output time. [`sort_selection`] and
//! [`limit_selection`] touch no cell values at all (compare-in-place / slice).
//! [`distinct_selection`] is the one that reads values — it encodes each
//! projected cell into a key tuple to bucket duplicates (cloning `TEXT`/`BYTEA`
//! cells exactly as the [`hash_aggregate`](crate::hash_aggregate) it mirrors
//! does, since `DISTINCT` ≡ `GROUP BY` all projected columns).
//!
//! The caller applies them in the Postgres pipeline order:
//!
//! ```text
//! filter → [aggregate] → DISTINCT → ORDER BY → OFFSET → LIMIT
//! ```
//!
//! ## Currency: [`Vector`], not the storage `Column`
//!
//! Like [`hash_aggregate`](crate::hash_aggregate) and the
//! [`Filter`](crate::Filter) operator, shaping works over the decoded,
//! per-cell-nullable [`Vector`]s — the caller (the engine's `run_select`)
//! decodes the columns a clause references and keeps everything else opaque.
//!
//! ## Semantics (pinned to Postgres)
//!
//! * **`ORDER BY`** ([`sort_selection`]): NULLS LAST under `ASC`, NULLS FIRST
//!   under `DESC` — a NULL sorts as if larger than every value. The sort is
//!   **stable**, so rows equal on every key keep their incoming order and a
//!   given input always shapes to one deterministic output (the simulation's
//!   reproducibility bar; Postgres itself leaves tie order unspecified).
//!   Comparison covers every [`Vector`] type: integers/booleans/temporals by
//!   value, `TEXT` lexicographically over its UTF-8 bytes (Rust `str` `Ord` —
//!   equivalently code-point order; Stele applies no collation, matching
//!   Postgres under the `C` locale), `UUID`/`BYTEA` byte-wise (as Postgres
//!   orders them), `PERIOD` lexicographically by `(from, to)`, and `FLOAT8`
//!   (an `AVG` output) by IEEE-754 total order — identical to Postgres over
//!   everything an integer `AVG` can produce (no NaNs).
//! * **`DISTINCT`** ([`distinct_selection`]): deduplicates the full projected
//!   row — exactly `GROUP BY` every output column with no aggregates, and it
//!   reuses that machinery: rows are bucketed by their encoded cell tuple
//!   (`encode_scalar`, the [`hash_aggregate`](crate::hash_aggregate)
//!   grouping-key identity), NULLs equal (the `GROUP BY` rule, not `=`), and
//!   each group keeps its **first** row in selection order. Output order is
//!   deterministic (sorted by encoded tuple, as grouped output is); a query
//!   that wants a specific order says `ORDER BY`, which runs after.
//! * **`OFFSET` / `LIMIT`** ([`limit_selection`]): a pure slice of the
//!   selection. An offset past the end or a `LIMIT 0` is a valid empty
//!   result, never an error.
//!
//! [STL-263]: https://allegromusic.atlassian.net/browse/STL-263
//! [STL-214]: https://allegromusic.atlassian.net/browse/STL-214

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::aggregate::encode_scalar;
use crate::expr::Vector;

/// One `ORDER BY` key: the (decoded) column to sort on and its direction.
///
/// Keys are applied first-key-outermost by [`sort_selection`]; NULL placement
/// follows the Postgres defaults (see the module docs).
#[derive(Debug, Clone, Copy)]
pub struct SortKey<'a> {
    /// The column the key sorts on, decoded to the evaluator's typed form.
    pub column: &'a Vector,
    /// `true` for `DESC` (which also flips NULLs first); `false` for `ASC`.
    pub descending: bool,
}

/// Stable-sort a selection of row indices by the given keys, first key
/// outermost ([STL-263] `ORDER BY`).
///
/// Only the indices move — cell values are compared in place, never copied.
/// NULL placement is the Postgres default: a NULL compares **greater** than
/// every value, so it lands last under `ASC`; `DESC` reverses the whole
/// comparison, placing NULLs first. Stability makes tie order deterministic
/// (the incoming selection order survives), which Postgres permits and the
/// deterministic simulation requires.
///
/// With no keys the selection is left untouched (the caller never builds that
/// call, but it is total).
///
/// # Panics
///
/// If a selection index is out of range for a key column — the caller selects
/// only rows of the vectors it sorts, the same contract [`Vector::gather`]
/// documents.
pub fn sort_selection(keys: &[SortKey<'_>], selection: &mut [usize]) {
    selection.sort_by(|&a, &b| {
        for key in keys {
            let mut ord = cmp_cells(key.column, a, b);
            if key.descending {
                ord = ord.reverse();
            }
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
}

/// Deduplicate a selection over the projected `columns` ([STL-263]
/// `DISTINCT`): one surviving index per distinct row, in deterministic
/// (encoded-tuple) order.
///
/// `DISTINCT` ≡ `GROUP BY` all projected columns with no aggregates, and this
/// is that machinery ([STL-171]): each row's identity is the tuple of its
/// cells' canonical encodings (`encode_scalar`), NULL cells equal (the
/// `GROUP BY` rule), and each group's representative is its **first** row in
/// selection order. Output order matches grouped-aggregate output (sorted by
/// encoded tuple) — deterministic without a sort; an `ORDER BY` runs after
/// and fixes the order the query asked for.
///
/// # Panics
///
/// If a selection index is out of range for a column — the caller selects
/// only rows of the vectors it deduplicates, as for [`sort_selection`].
#[must_use]
pub fn distinct_selection(columns: &[&Vector], selection: &[usize]) -> Vec<usize> {
    let mut groups: BTreeMap<Vec<Option<Vec<u8>>>, usize> = BTreeMap::new();
    for &row in selection {
        let key: Vec<Option<Vec<u8>>> = columns
            .iter()
            .map(|column| column.get(row).as_ref().map(encode_scalar))
            .collect();
        groups.entry(key).or_insert(row);
    }
    groups.into_values().collect()
}

/// Slice a selection to `OFFSET skip` / `LIMIT keep` rows ([STL-263]).
///
/// Pure index arithmetic, saturating at the ends: an offset past the end
/// empties the selection (a valid empty result, as Postgres reads it), a
/// `LIMIT` larger than the remainder keeps everything, `LIMIT 0` keeps
/// nothing, and `None` is unlimited (`LIMIT ALL`). Runs **after**
/// [`sort_selection`] — the slice is only meaningful over the final order.
pub fn limit_selection(selection: &mut Vec<usize>, offset: u64, limit: Option<u64>) {
    let skip = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(selection.len());
    selection.drain(..skip);
    if let Some(limit) = limit {
        let keep = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .min(selection.len());
        selection.truncate(keep);
    }
}

/// Compare one column's cells at rows `left` and `right`, NULLs **greatest** (the
/// Postgres `ASC` default; [`sort_selection`] reverses the whole ordering for
/// `DESC`, which places NULLs first).
///
/// Values compare in place — no [`Vector::get`] materialization, so sorting
/// `TEXT`/`BYTEA` clones nothing. Every variant is ordered the way Postgres
/// orders the type (see the module docs); `FLOAT8` cells hold IEEE-754
/// bits and compare by [`f64::total_cmp`].
fn cmp_cells(column: &Vector, left: usize, right: usize) -> Ordering {
    /// NULL-greatest comparison of two nullable cells.
    fn cmp_opt<T: Ord>(left: Option<&T>, right: Option<&T>) -> Ordering {
        match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(x), Some(y)) => x.cmp(y),
        }
    }
    match column {
        Vector::Bool(v) => cmp_opt(v[left].as_ref(), v[right].as_ref()),
        // The `i32`- and `i64`-payload types share their integer comparisons.
        Vector::Int4(v) | Vector::Date(v) => cmp_opt(v[left].as_ref(), v[right].as_ref()),
        Vector::Int8(v) | Vector::Timestamp(v) | Vector::TimestampTz(v) => {
            cmp_opt(v[left].as_ref(), v[right].as_ref())
        }
        Vector::Text(v) => cmp_opt(v[left].as_ref(), v[right].as_ref()),
        Vector::Uuid(v) => cmp_opt(v[left].as_ref(), v[right].as_ref()),
        Vector::Bytea(v) => cmp_opt(v[left].as_ref(), v[right].as_ref()),
        Vector::Period(v) => cmp_opt(v[left].as_ref(), v[right].as_ref()),
        // FLOAT8 cells are IEEE-754 bit patterns; total_cmp is the total order
        // (identical to `<` over everything an integer AVG can produce).
        Vector::Float8(v) => match (v[left], v[right]) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(x), Some(y)) => f64::from_bits(x).total_cmp(&f64::from_bits(y)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::hash_aggregate;
    use crate::expr::Expr;
    use stele_common::period::Interval;

    /// A fresh identity selection over `n` rows.
    fn all(n: usize) -> Vec<usize> {
        (0..n).collect()
    }

    #[test]
    fn sort_places_nulls_last_under_asc_and_first_under_desc() {
        let col = Vector::Int4(vec![Some(2), None, Some(1), None, Some(3)]);
        let mut sel = all(5);
        sort_selection(
            &[SortKey {
                column: &col,
                descending: false,
            }],
            &mut sel,
        );
        assert_eq!(sel, vec![2, 0, 4, 1, 3], "ASC: values first, NULLs last");

        let mut sel = all(5);
        sort_selection(
            &[SortKey {
                column: &col,
                descending: true,
            }],
            &mut sel,
        );
        assert_eq!(sel, vec![1, 3, 4, 0, 2], "DESC: NULLs first, then values");
    }

    #[test]
    fn sort_applies_keys_outermost_first_and_is_stable_on_ties() {
        // (region, amount): sort by region ASC, amount DESC.
        let region = Vector::Text(vec![
            Some("w".into()),
            Some("e".into()),
            Some("e".into()),
            Some("w".into()),
        ]);
        let amount = Vector::Int8(vec![Some(1), Some(5), Some(7), Some(1)]);
        let mut sel = all(4);
        sort_selection(
            &[
                SortKey {
                    column: &region,
                    descending: false,
                },
                SortKey {
                    column: &amount,
                    descending: true,
                },
            ],
            &mut sel,
        );
        assert_eq!(sel, vec![2, 1, 0, 3]);

        // Ties on every key keep the incoming selection order (stability):
        // rows 0 and 3 tie on (w, 1) and stay 0-before-3 above; reversing the
        // incoming selection reverses them.
        let mut rev = vec![3, 2, 1, 0];
        sort_selection(
            &[
                SortKey {
                    column: &region,
                    descending: false,
                },
                SortKey {
                    column: &amount,
                    descending: true,
                },
            ],
            &mut rev,
        );
        assert_eq!(rev, vec![2, 1, 3, 0], "ties keep incoming order");
    }

    /// Every `Vector` variant orders — the STL-263 bar is every shipped type,
    /// byte-wise where Postgres is byte-wise.
    #[test]
    fn sort_orders_every_vector_type() {
        let two = |col: &Vector| {
            let mut sel = vec![0, 1];
            sort_selection(
                &[SortKey {
                    column: col,
                    descending: false,
                }],
                &mut sel,
            );
            sel
        };
        // bool: false < true.
        assert_eq!(two(&Vector::Bool(vec![Some(true), Some(false)])), [1, 0]);
        // text: code-point order.
        assert_eq!(
            two(&Vector::Text(vec![Some("b".into()), Some("a".into())])),
            [1, 0]
        );
        // temporals order by instant / day.
        assert_eq!(two(&Vector::Timestamp(vec![Some(9), Some(3)])), [1, 0]);
        assert_eq!(two(&Vector::TimestampTz(vec![Some(9), Some(3)])), [1, 0]);
        assert_eq!(two(&Vector::Date(vec![Some(9), Some(3)])), [1, 0]);
        // uuid/bytea: byte-wise.
        let lo = [0u8; 16];
        let mut hi = [0u8; 16];
        hi[0] = 1;
        assert_eq!(two(&Vector::Uuid(vec![Some(hi), Some(lo)])), [1, 0]);
        assert_eq!(
            two(&Vector::Bytea(vec![Some(vec![2]), Some(vec![1, 255])])),
            [1, 0]
        );
        // period: lexicographic by (from, to).
        let p = |from, to| Interval::new(from, to).expect("interval");
        assert_eq!(
            two(&Vector::Period(vec![Some(p(5, 9)), Some(p(1, 2))])),
            [1, 0]
        );
        // float8 (an AVG output): numeric order via total_cmp, negatives right.
        assert_eq!(
            two(&Vector::Float8(vec![
                Some(1.5f64.to_bits()),
                Some((-2.5f64).to_bits()),
            ])),
            [1, 0]
        );
    }

    #[test]
    fn distinct_keeps_one_first_seen_row_per_duplicate_group() {
        // Rows: (1, "a"), (2, "b"), (1, "a"), (1, "b") — row 2 duplicates row 0.
        let k = Vector::Int4(vec![Some(1), Some(2), Some(1), Some(1)]);
        let t = Vector::Text(vec![
            Some("a".into()),
            Some("b".into()),
            Some("a".into()),
            Some("b".into()),
        ]);
        let kept = distinct_selection(&[&k, &t], &all(4));
        // Three distinct rows survive; the duplicate kept its first occurrence
        // (row 0, not row 2).
        assert_eq!(kept.len(), 3);
        assert!(kept.contains(&0) && kept.contains(&1) && kept.contains(&3));
        assert!(!kept.contains(&2), "the duplicate keeps its first row");
    }

    #[test]
    fn distinct_groups_null_rows_together() {
        // NULLs are equal under DISTINCT (the GROUP BY rule, not `=`): the two
        // all-NULL rows collapse, and (NULL, 1) stays distinct from (NULL, NULL).
        let a = Vector::Int4(vec![None, None, None]);
        let b = Vector::Int4(vec![None, Some(1), None]);
        let kept = distinct_selection(&[&a, &b], &all(3));
        assert_eq!(kept.len(), 2);
        assert!(kept.contains(&0) && kept.contains(&1));
    }

    /// DISTINCT ≡ GROUP BY all projected columns with no aggregates: the same
    /// machinery must agree on the surviving rows.
    #[test]
    fn distinct_matches_group_by_all_columns() {
        let k = Vector::Int8(vec![Some(3), None, Some(3), Some(1), None, Some(1)]);
        let kept = distinct_selection(&[&k], &all(6));
        let grouped = hash_aggregate(&[Expr::col(0)], &[], std::slice::from_ref(&k), 6)
            .expect("group by the one column");
        assert_eq!(kept.len(), grouped.num_groups);
        // Same values, same (encoded-key) order.
        let kept_values: Vec<_> = kept.iter().map(|&r| k.get(r)).collect();
        let group_values: Vec<_> = (0..grouped.num_groups)
            .map(|g| grouped.groups[0].get(g))
            .collect();
        assert_eq!(kept_values, group_values);
    }

    #[test]
    fn limit_and_offset_slice_with_saturating_edges() {
        // Plain slice.
        let mut sel = all(5);
        limit_selection(&mut sel, 1, Some(2));
        assert_eq!(sel, vec![1, 2]);

        // LIMIT 0 keeps nothing; no limit keeps the remainder.
        let mut sel = all(3);
        limit_selection(&mut sel, 0, Some(0));
        assert!(sel.is_empty());
        let mut sel = all(3);
        limit_selection(&mut sel, 1, None);
        assert_eq!(sel, vec![1, 2]);

        // An offset past the end is a valid empty result; a limit past the end
        // keeps everything left.
        let mut sel = all(3);
        limit_selection(&mut sel, 7, Some(1));
        assert!(sel.is_empty());
        let mut sel = all(3);
        limit_selection(&mut sel, 2, Some(u64::MAX));
        assert_eq!(sel, vec![2]);
    }
}
