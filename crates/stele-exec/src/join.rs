//! Hash **join** — `INNER` / `LEFT` / `SEMI` / `ANTI` equi-joins of two inputs
//! ([STL-172] `[C12]`).
//!
//! This lifts the v0.1 single-table-scan restriction: a query can read two tables
//! and combine their rows on an equality condition. Like the
//! [aggregate](crate::hash_aggregate) operator it builds on the vectorized scalar
//! evaluator ([`eval_expr`], `[C10]`) — the join key on each side is an arbitrary
//! [`Expr`] evaluated a whole batch at a time into a [`Vector`].
//!
//! ## Currency: [`Vector`] + row indices, not the storage [`Column`](crate::Column)
//!
//! The operator works in the decoded, per-cell-nullable [`Vector`] currency the
//! evaluator uses (a join key is typed and comparable), and it returns the join as
//! a set of **row indices** — [`JoinIndices`] — rather than materializing output
//! columns. Only the *key* columns are evaluated here; the caller (the engine's
//! `run_join`) gathers the surviving non-key cells of each side by index, so a
//! column the join merely carries through never has to be decoded into a vector.
//! This is the same split the [`Filter`](crate::Filter) and
//! [`hash_aggregate`](crate::hash_aggregate) operators draw, and it sidesteps the
//! closed-[`ColumnId`](stele_storage::segment::ColumnId) problem (two tables'
//! value columns cannot share the storage column ids) by addressing output
//! positionally downstream.
//!
//! ## Build / probe and the four join types
//!
//! The **right** side is hashed (the *build* side) and the **left** side drives
//! the scan (the *probe* side), so the left side's row order is preserved — which
//! is exactly the order `LEFT` / `SEMI` / `ANTI` must keep. Each output row names
//! a left row and, for the row-combining joins, the right row it matched:
//!
//! * `INNER` — one output row per matching `(left, right)` pair.
//! * `LEFT` — every left row at least once: one row per right match, or a single
//!   row with **no** right side (a SQL `NULL`-extended right) when it matches none.
//! * `SEMI` — each left row that has *at least one* right match, once (no
//!   duplication, no right columns).
//! * `ANTI` — each left row that has *no* right match (no right columns).
//!
//! ## NULL keys (SQL semantics)
//!
//! A join condition `l = r` is *unknown*, never true, when either side is NULL —
//! so a NULL key never matches. A NULL key on the **right** is dropped from the
//! build table; a NULL key on the **left** matches nothing — invisible to `INNER`
//! / `SEMI`, `NULL`-extended by `LEFT`, and *kept* by `ANTI` (it has no match).
//!
//! ## Determinism
//!
//! The build table is a [`BTreeMap`] keyed by each key cell's canonical encoding,
//! and each bucket holds its right rows in ascending order, so a probe emits its
//! matches in a stable order and the whole operator is reproducible under the
//! simulation scheduler.
//!
//! ## Scope
//!
//! A single equi-join condition (one `Expr` per side, compared for equality).
//! Multi-key / non-equi / `AND`-chained conditions, and `RIGHT` / `FULL` joins,
//! are deliberate follow-ups; the binder accepts only what this evaluates.
//!
//! [STL-172]: https://allegromusic.atlassian.net/browse/STL-172

use std::collections::BTreeMap;

use stele_common::types::ScalarValue;

use crate::expr::{Expr, ExprError, Vector, eval_expr};

/// Which join to compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// `INNER` — matching `(left, right)` pairs only.
    Inner,
    /// `LEFT` (outer) — every left row, `NULL`-extended on the right when it has
    /// no match.
    Left,
    /// `SEMI` — left rows that have at least one right match, once each; no right
    /// columns.
    Semi,
    /// `ANTI` — left rows that have no right match; no right columns.
    Anti,
}

impl JoinType {
    /// Whether the output carries the right side's cells. `INNER` / `LEFT` combine
    /// both inputs' columns; `SEMI` / `ANTI` filter the left input and emit only
    /// its columns.
    #[must_use]
    pub const fn keeps_right(self) -> bool {
        matches!(self, Self::Inner | Self::Left)
    }
}

/// The result of a [`hash_join`]: one entry per output row, naming the input rows
/// it draws from.
///
/// [`left`](Self::left) holds the left input row index of each output row, in
/// output order. For a [right-keeping](JoinType::keeps_right) join
/// ([`Inner`](JoinType::Inner) / [`Left`](JoinType::Left)) [`right`](Self::right)
/// is the same length and gives the matched right row — or `None` for a `LEFT`
/// join's unmatched row, the `NULL`-extended right side. For a
/// [`Semi`](JoinType::Semi) / [`Anti`](JoinType::Anti) join the output is the left
/// input alone, so [`right`](Self::right) is empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinIndices {
    /// The left input row index of each output row, in output order.
    pub left: Vec<usize>,
    /// The right input row index of each output row (a right-keeping join), or
    /// `None` for a `LEFT` join's unmatched row. Empty for `SEMI` / `ANTI`.
    pub right: Vec<Option<usize>>,
}

/// Equi-join `left` and `right` on `left_key == right_key`, returning the output
/// rows as input-row indices ([`JoinIndices`]).
///
/// `left` / `right` are each side's input columns (one [`Vector`] per column,
/// addressed by position); `left_rows` / `right_rows` are their heights.
/// `left_key` / `right_key` are the equi-join key expressions, evaluated over
/// their side's columns by [`eval_expr`]. The module documentation covers the
/// build/probe split, the four join types, and NULL-key semantics.
///
/// The two key expressions must evaluate to the same [`LogicalType`](stele_common::types::LogicalType)
/// — the binder enforces this, so a key value on one side matches a key on the
/// other exactly when their canonical encodings are equal.
///
/// # Errors
///
/// [`ExprError`] if either key expression is structurally invalid over its side's
/// columns (an out-of-range column, a type the evaluator cannot read). Data NULLs
/// are handled in-band (a NULL key matches nothing), never as errors.
pub fn hash_join(
    join_type: JoinType,
    left: &[Vector],
    left_rows: usize,
    left_key: &Expr,
    right: &[Vector],
    right_rows: usize,
    right_key: &Expr,
) -> Result<JoinIndices, ExprError> {
    let left_keys = eval_expr(left_key, left, left_rows)?;
    let right_keys = eval_expr(right_key, right, right_rows)?;

    // Build the hash table on the right (build) side, keyed by each non-NULL key
    // cell's canonical encoding. A NULL right key joins to nothing (`NULL = NULL`
    // is unknown), so it never enters the table. The `BTreeMap` keeps bucket lookup
    // deterministic, and pushing row indices in ascending order keeps each bucket
    // ascending — so a probe (the left side) emits its matches in a stable order.
    let mut table: BTreeMap<Vec<u8>, Vec<usize>> = BTreeMap::new();
    for r in 0..right_rows {
        if let Some(key) = right_keys.get(r) {
            table.entry(encode_scalar(&key)).or_default().push(r);
        }
    }

    let mut indices = JoinIndices::default();
    for l in 0..left_rows {
        // A NULL left key matches nothing, so `matches` is the empty slice — which
        // is invisible to INNER/SEMI, NULL-extended by LEFT, and kept by ANTI.
        let matches = left_keys
            .get(l)
            .and_then(|key| table.get(&encode_scalar(&key)))
            .map_or(&[][..], Vec::as_slice);
        match join_type {
            JoinType::Inner => {
                for &r in matches {
                    indices.left.push(l);
                    indices.right.push(Some(r));
                }
            }
            JoinType::Left => {
                if matches.is_empty() {
                    indices.left.push(l);
                    indices.right.push(None);
                } else {
                    for &r in matches {
                        indices.left.push(l);
                        indices.right.push(Some(r));
                    }
                }
            }
            JoinType::Semi => {
                if !matches.is_empty() {
                    indices.left.push(l);
                }
            }
            JoinType::Anti => {
                if matches.is_empty() {
                    indices.left.push(l);
                }
            }
        }
    }
    Ok(indices)
}

/// Encode a scalar to its canonical bytes — a key cell's identity in the build
/// table. The binder constrains both join keys to one type, so the type-directed
/// encoding is injective across the rows that must match and equal keys hash
/// together.
fn encode_scalar(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Expr::Column(0)` — both fixtures key on their first column.
    const fn key0() -> Expr {
        Expr::col(0)
    }

    /// Reconstruct the row-combining output (`INNER` / `LEFT`) as
    /// `(left_key, left_val, Option<(right_key, right_val)>)` tuples, reading the
    /// `(key, val)` cells back through the indices. The right side is `None` for a
    /// `LEFT` join's unmatched row.
    #[allow(clippy::type_complexity)]
    fn combined(
        indices: &JoinIndices,
        left: &[(Option<i64>, i64)],
        right: &[(Option<i64>, i64)],
    ) -> Vec<(Option<i64>, i64, Option<(i64, i64)>)> {
        assert_eq!(indices.left.len(), indices.right.len(), "aligned");
        indices
            .left
            .iter()
            .zip(&indices.right)
            .map(|(&l, &r)| {
                let rr = r.map(|rr| {
                    (
                        right[rr].0.expect("matched right key is non-NULL"),
                        right[rr].1,
                    )
                });
                (left[l].0, left[l].1, rr)
            })
            .collect()
    }

    /// Reconstruct the left-only output (`SEMI` / `ANTI`) as `(key, val)` tuples.
    fn left_only(indices: &JoinIndices, left: &[(Option<i64>, i64)]) -> Vec<(Option<i64>, i64)> {
        assert!(indices.right.is_empty(), "SEMI/ANTI carry no right side");
        indices.left.iter().map(|&l| left[l]).collect()
    }

    /// Build the two `(key, val)` int8 vectors a fixture's rows decode to.
    fn vectors(rows: &[(Option<i64>, i64)]) -> Vec<Vector> {
        vec![
            Vector::Int8(rows.iter().map(|(k, _)| *k).collect()),
            Vector::Int8(rows.iter().map(|(_, v)| Some(*v)).collect()),
        ]
    }

    fn run(
        join_type: JoinType,
        left: &[(Option<i64>, i64)],
        right: &[(Option<i64>, i64)],
    ) -> JoinIndices {
        let lv = vectors(left);
        let rv = vectors(right);
        hash_join(
            join_type,
            &lv,
            left.len(),
            &key0(),
            &rv,
            right.len(),
            &key0(),
        )
        .expect("join")
    }

    #[test]
    fn inner_matches_only_equal_keys() {
        // left keys 1,2,3 — right keys 2,3,3 (3 appears twice → one-to-many).
        let left = [(Some(1), 10), (Some(2), 20), (Some(3), 30)];
        let right = [(Some(2), 200), (Some(3), 300), (Some(3), 301)];
        let out = combined(&run(JoinType::Inner, &left, &right), &left, &right);
        assert_eq!(
            out,
            vec![
                (Some(2), 20, Some((2, 200))),
                (Some(3), 30, Some((3, 300))),
                (Some(3), 30, Some((3, 301))),
            ]
        );
    }

    #[test]
    fn left_keeps_unmatched_rows_null_extended() {
        let left = [(Some(1), 10), (Some(2), 20)];
        let right = [(Some(2), 200)];
        let out = combined(&run(JoinType::Left, &left, &right), &left, &right);
        // Key 1 has no match → a single NULL-extended row; key 2 matches.
        assert_eq!(
            out,
            vec![(Some(1), 10, None), (Some(2), 20, Some((2, 200)))]
        );
    }

    #[test]
    fn semi_keeps_each_matching_left_row_once() {
        // Right has key 2 twice, but SEMI emits the left row once, not per match.
        let left = [(Some(1), 10), (Some(2), 20), (Some(3), 30)];
        let right = [(Some(2), 200), (Some(2), 201), (Some(3), 300)];
        let out = left_only(&run(JoinType::Semi, &left, &right), &left);
        assert_eq!(out, vec![(Some(2), 20), (Some(3), 30)]);
    }

    #[test]
    fn anti_keeps_left_rows_with_no_match() {
        let left = [(Some(1), 10), (Some(2), 20), (Some(3), 30)];
        let right = [(Some(2), 200)];
        let out = left_only(&run(JoinType::Anti, &left, &right), &left);
        assert_eq!(out, vec![(Some(1), 10), (Some(3), 30)]);
    }

    #[test]
    fn null_keys_never_match() {
        // NULL on either side is unknown, never equal.
        let left = [(None, 10), (Some(2), 20)];
        let right = [(None, 200), (Some(2), 201)];

        // INNER: only the 2=2 pair; both NULLs invisible.
        let inner = combined(&run(JoinType::Inner, &left, &right), &left, &right);
        assert_eq!(inner, vec![(Some(2), 20, Some((2, 201)))]);

        // LEFT: the NULL-keyed left row is kept, NULL-extended (it matched no
        // right, not even the NULL-keyed one).
        let left_join = combined(&run(JoinType::Left, &left, &right), &left, &right);
        assert_eq!(
            left_join,
            vec![(None, 10, None), (Some(2), 20, Some((2, 201)))]
        );

        // ANTI: the NULL-keyed left row has no match, so it survives.
        let anti = left_only(&run(JoinType::Anti, &left, &right), &left);
        assert_eq!(anti, vec![(None, 10)]);

        // SEMI: the NULL-keyed left row has no match, so it is excluded.
        let semi = left_only(&run(JoinType::Semi, &left, &right), &left);
        assert_eq!(semi, vec![(Some(2), 20)]);
    }

    #[test]
    fn empty_inputs_produce_empty_or_preserved_output() {
        let rows = [(Some(1), 10), (Some(2), 20)];
        // Empty right: INNER/SEMI empty; LEFT all NULL-extended; ANTI all kept.
        assert!(run(JoinType::Inner, &rows, &[]).left.is_empty());
        assert!(run(JoinType::Semi, &rows, &[]).left.is_empty());
        assert_eq!(run(JoinType::Left, &rows, &[]).right, vec![None, None]);
        assert_eq!(
            left_only(&run(JoinType::Anti, &rows, &[]), &rows),
            rows.to_vec()
        );
        // Empty left: every join is empty.
        assert!(run(JoinType::Left, &[], &rows).left.is_empty());
    }

    #[test]
    fn out_of_range_key_is_a_plan_error() {
        let lv = vectors(&[(Some(1), 10)]);
        let rv = vectors(&[(Some(1), 10)]);
        assert!(matches!(
            hash_join(JoinType::Inner, &lv, 1, &Expr::col(9), &rv, 1, &key0()),
            Err(ExprError::ColumnOutOfRange { .. })
        ));
    }

    // ---- Differential vs an independent naive nested-loop reference ----

    /// A tiny deterministic PRNG (SplitMix64) — keeps the differential seeded and
    /// dependency-free (the same generator the aggregate oracle uses).
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        /// A `(key, val)` row. Keys are drawn from a small domain so collisions
        /// (one-to-many matches) are common; ~1 in 5 keys is NULL.
        fn row(&mut self) -> (Option<i64>, i64) {
            let key = if self.below(5) == 0 {
                None
            } else {
                Some(i64::try_from(self.below(6)).expect("small"))
            };
            (key, i64::try_from(self.next() % 1000).expect("small"))
        }

        fn table(&mut self, max: u64) -> Vec<(Option<i64>, i64)> {
            let n = usize::try_from(self.below(max + 1)).expect("fits");
            (0..n).map(|_| self.row()).collect()
        }
    }

    /// The reference combined output: a plain nested loop, NULL keys never equal.
    #[allow(clippy::type_complexity)]
    fn reference_combined(
        join_type: JoinType,
        left: &[(Option<i64>, i64)],
        right: &[(Option<i64>, i64)],
    ) -> Vec<(Option<i64>, i64, Option<(i64, i64)>)> {
        let mut out = Vec::new();
        for &(lk, lv) in left {
            let matches: Vec<&(Option<i64>, i64)> = right
                .iter()
                .filter(|(rk, _)| lk.is_some() && *rk == lk)
                .collect();
            if matches.is_empty() && join_type == JoinType::Left {
                out.push((lk, lv, None));
            }
            for &&(rk, rv) in &matches {
                out.push((lk, lv, Some((rk.expect("non-NULL match"), rv))));
            }
        }
        out
    }

    /// The reference left-only output (`SEMI` / `ANTI`).
    fn reference_left_only(
        join_type: JoinType,
        left: &[(Option<i64>, i64)],
        right: &[(Option<i64>, i64)],
    ) -> Vec<(Option<i64>, i64)> {
        left.iter()
            .filter(|(lk, _)| {
                // A NULL left key matches nothing; otherwise it matches if any
                // right key equals it.
                let has_match = lk.is_some() && right.iter().any(|(rk, _)| rk == lk);
                match join_type {
                    JoinType::Semi => has_match,
                    JoinType::Anti => !has_match,
                    _ => unreachable!("reference_left_only is SEMI/ANTI only"),
                }
            })
            .copied()
            .collect()
    }

    #[test]
    fn differential_vs_naive_reference() {
        for seed in 0..200u64 {
            let mut rng = SplitMix64(seed.wrapping_mul(0x1234_5678).wrapping_add(1));
            let left = rng.table(8);
            let right = rng.table(8);

            for join_type in [JoinType::Inner, JoinType::Left] {
                let got = {
                    let mut v = combined(&run(join_type, &left, &right), &left, &right);
                    v.sort();
                    v
                };
                let want = {
                    let mut v = reference_combined(join_type, &left, &right);
                    v.sort();
                    v
                };
                assert_eq!(got, want, "{join_type:?} seed {seed}");
            }
            for join_type in [JoinType::Semi, JoinType::Anti] {
                let got = {
                    let mut v = left_only(&run(join_type, &left, &right), &left);
                    v.sort();
                    v
                };
                let want = {
                    let mut v = reference_left_only(join_type, &left, &right);
                    v.sort();
                    v
                };
                assert_eq!(got, want, "{join_type:?} seed {seed}");
            }
        }
    }
}
