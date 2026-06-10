//! Fuzz oracle for the vectorized scalar expression evaluator (STL-170 `[C10]`).
//!
//! The Definition of Done is "expression eval matches scalar semantics on a
//! fuzzed input set incl. NULLs (three-valued logic)". This test is that oracle:
//! a seeded generator builds a random, **well-typed** expression tree
//! (comparisons, integer arithmetic, `AND`/`OR`/`NOT`, `IS NULL`) over random
//! columns sprinkled with NULLs, evaluates it two independent ways, and asserts
//! they agree cell-for-cell:
//!
//! * the **vectorized** path under test — [`stele_exec::eval_expr`] over whole
//!   [`Vector`]s; and
//! * an independent **scalar reference** ([`eval_scalar`]) that walks the same
//!   tree one row at a time, returning an `Option<ScalarValue>` per row.
//!
//! The two share only the [`Expr`] vocabulary; the evaluation code is disjoint,
//! so an agreement across thousands of (seed × row) cells is real evidence the
//! vectorized kernels implement the scalar three-valued semantics — including
//! NULL propagation through comparisons/arithmetic, the `AND`/`OR` truth tables,
//! and integer-overflow-to-NULL. A mismatch prints the seed for a one-line
//! repro.

use stele_common::types::ScalarValue;
use stele_exec::{ArithOp, CmpOp, Expr, ExprError, LogicOp, Vector, eval_expr};

// --- deterministic PRNG ----------------------------------------------------

/// A tiny xorshift64* generator — deterministic, seeded, and dependency-free,
/// so a failing seed reproduces byte-for-byte (the repo's sim ethos, ADR-0010).
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        // Avoid the zero fixed point.
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `0..n`.
    fn below(&mut self, n: u32) -> u32 {
        u32::try_from(self.next_u64() % u64::from(n)).expect("masked below u32")
    }

    /// `true` with probability `1/n`.
    fn one_in(&mut self, n: u32) -> bool {
        self.below(n) == 0
    }
}

// --- the four column types the evaluator supports --------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ty {
    Int4,
    Int8,
    Bool,
    Text,
}

const TYPES: [Ty; 4] = [Ty::Int4, Ty::Int8, Ty::Bool, Ty::Text];

/// Build a random column of `ty` with `rows` cells, each NULL ~1/4 of the time.
/// Small value/text domains make collisions (and so true comparisons) frequent.
fn random_column(rng: &mut Rng, ty: Ty, rows: usize) -> Vector {
    let null = |rng: &mut Rng| rng.one_in(4);
    match ty {
        Ty::Int4 => Vector::Int4(
            (0..rows)
                .map(|_| (!null(rng)).then(|| i32::try_from(rng.below(7)).expect("0..7") - 3))
                .collect(),
        ),
        Ty::Int8 => Vector::Int8(
            (0..rows)
                .map(|_| (!null(rng)).then(|| i64::from(rng.below(7)) - 3))
                .collect(),
        ),
        Ty::Bool => Vector::Bool(
            (0..rows)
                .map(|_| (!null(rng)).then(|| rng.one_in(2)))
                .collect(),
        ),
        Ty::Text => Vector::Text(
            (0..rows)
                .map(|_| (!null(rng)).then(|| ["a", "b", "c"][rng.below(3) as usize].to_owned()))
                .collect(),
        ),
    }
}

/// A random non-null literal of `ty`, from the same small domains.
fn random_literal(rng: &mut Rng, ty: Ty) -> ScalarValue {
    match ty {
        Ty::Int4 => ScalarValue::Int4(i32::try_from(rng.below(7)).expect("0..7") - 3),
        Ty::Int8 => ScalarValue::Int8(i64::from(rng.below(7)) - 3),
        Ty::Bool => ScalarValue::Bool(rng.one_in(2)),
        Ty::Text => ScalarValue::Text(["a", "b", "c"][rng.below(3) as usize].to_owned()),
    }
}

// --- typed expression generator --------------------------------------------

/// The column layout shared by the batch and the generator: one column per type,
/// at the position the type's index gives.
const fn column_of(ty: Ty) -> usize {
    match ty {
        Ty::Int4 => 0,
        Ty::Int8 => 1,
        Ty::Bool => 2,
        Ty::Text => 3,
    }
}

/// Generate a random, **well-typed** expression producing a value of `ty`.
///
/// Type-correct by construction (no comparing int4 to text) so the oracle tests
/// value/NULL semantics, not the evaluator's type checking (covered by unit
/// tests). `budget` bounds the tree depth.
fn gen_expr(rng: &mut Rng, ty: Ty, budget: u32) -> Expr {
    // A leaf: the column of this type, or a constant.
    let leaf = |rng: &mut Rng| {
        if rng.one_in(2) {
            Expr::col(column_of(ty))
        } else {
            Expr::lit(random_literal(rng, ty))
        }
    };
    if budget == 0 {
        return leaf(rng);
    }
    match ty {
        Ty::Int4 | Ty::Int8 => {
            if rng.one_in(2) {
                leaf(rng)
            } else {
                let op = [ArithOp::Add, ArithOp::Sub, ArithOp::Mul][rng.below(3) as usize];
                gen_expr(rng, ty, budget - 1).arith(op, gen_expr(rng, ty, budget - 1))
            }
        }
        Ty::Text => leaf(rng),
        Ty::Bool => match rng.below(5) {
            0 => leaf(rng),
            1 => {
                // A comparison over a randomly chosen operand type.
                let operand = TYPES[rng.below(4) as usize];
                let op = [
                    CmpOp::Eq,
                    CmpOp::Ne,
                    CmpOp::Lt,
                    CmpOp::Le,
                    CmpOp::Gt,
                    CmpOp::Ge,
                ][rng.below(6) as usize];
                gen_expr(rng, operand, budget - 1).compare(op, gen_expr(rng, operand, budget - 1))
            }
            2 => {
                let op = if rng.one_in(2) {
                    LogicOp::And
                } else {
                    LogicOp::Or
                };
                gen_expr(rng, Ty::Bool, budget - 1).logic(op, gen_expr(rng, Ty::Bool, budget - 1))
            }
            3 => gen_expr(rng, Ty::Bool, budget - 1).negate(),
            _ => {
                // IS NULL over any operand type.
                let operand = TYPES[rng.below(4) as usize];
                gen_expr(rng, operand, budget - 1).is_null()
            }
        },
    }
}

// --- the scalar reference (independent row-at-a-time evaluator) -------------

/// Evaluate `expr` against one row, returning that row's value (or `None` for
/// SQL NULL). Written plainly and independently of the vectorized kernels — this
/// is the spec the oracle holds the vectorized path to.
fn eval_scalar(expr: &Expr, row: &[Option<ScalarValue>]) -> Result<Option<ScalarValue>, ExprError> {
    Ok(match expr {
        Expr::Column(i) => row[*i].clone(),
        Expr::Literal(v) => Some(v.clone()),
        Expr::Not(inner) => {
            as_bool(eval_scalar(inner, row)?.as_ref())?.map(|b| ScalarValue::Bool(!b))
        }
        Expr::IsNull(inner) => Some(ScalarValue::Bool(eval_scalar(inner, row)?.is_none())),
        Expr::Compare { op, left, right } => {
            let lhs = eval_scalar(left, row)?;
            let rhs = eval_scalar(right, row)?;
            match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => Some(ScalarValue::Bool(scalar_compare(*op, &lhs, &rhs)?)),
                _ => None,
            }
        }
        Expr::Logic { op, left, right } => {
            let lhs = as_bool(eval_scalar(left, row)?.as_ref())?;
            let rhs = as_bool(eval_scalar(right, row)?.as_ref())?;
            let out = match op {
                LogicOp::And => match (lhs, rhs) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                },
                LogicOp::Or => match (lhs, rhs) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                },
            };
            out.map(ScalarValue::Bool)
        }
        Expr::Arith { op, left, right } => {
            let lhs = eval_scalar(left, row)?;
            let rhs = eval_scalar(right, row)?;
            match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => scalar_arith(*op, &lhs, &rhs)?,
                _ => None,
            }
        }
    })
}

/// Pull a boolean out of an optional scalar, erroring on a non-boolean — the
/// reference's mirror of the evaluator's `NotBoolean` check.
const fn as_bool(value: Option<&ScalarValue>) -> Result<Option<bool>, ExprError> {
    match value {
        None => Ok(None),
        Some(ScalarValue::Bool(b)) => Ok(Some(*b)),
        Some(other) => Err(ExprError::NotBoolean {
            op: "ref",
            found: other.logical_type(),
        }),
    }
}

fn scalar_compare(op: CmpOp, lhs: &ScalarValue, rhs: &ScalarValue) -> Result<bool, ExprError> {
    use std::cmp::Ordering;
    let ord = match (lhs, rhs) {
        (ScalarValue::Int4(a), ScalarValue::Int4(b)) => a.cmp(b),
        (ScalarValue::Int8(a), ScalarValue::Int8(b)) => a.cmp(b),
        (ScalarValue::Bool(a), ScalarValue::Bool(b)) => a.cmp(b),
        (ScalarValue::Text(a), ScalarValue::Text(b)) => a.cmp(b),
        _ => {
            return Err(ExprError::CompareTypeMismatch {
                left: lhs.logical_type(),
                right: rhs.logical_type(),
            });
        }
    };
    Ok(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    })
}

fn scalar_arith(
    op: ArithOp,
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> Result<Option<ScalarValue>, ExprError> {
    Ok(match (lhs, rhs) {
        (ScalarValue::Int4(a), ScalarValue::Int4(b)) => {
            let v = match op {
                ArithOp::Add => a.checked_add(*b),
                ArithOp::Sub => a.checked_sub(*b),
                ArithOp::Mul => a.checked_mul(*b),
            };
            v.map(ScalarValue::Int4)
        }
        (ScalarValue::Int8(a), ScalarValue::Int8(b)) => {
            let v = match op {
                ArithOp::Add => a.checked_add(*b),
                ArithOp::Sub => a.checked_sub(*b),
                ArithOp::Mul => a.checked_mul(*b),
            };
            v.map(ScalarValue::Int8)
        }
        _ => {
            return Err(ExprError::ArithTypeMismatch {
                left: lhs.logical_type(),
                right: rhs.logical_type(),
            });
        }
    })
}

// --- the oracle ------------------------------------------------------------

#[test]
fn vectorized_eval_matches_scalar_semantics_under_fuzz() {
    const SEEDS: u64 = 2_000;
    const ROWS: usize = 12;

    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        // One column per supported type, in `column_of` order.
        let columns: Vec<Vector> = TYPES
            .iter()
            .map(|&ty| random_column(&mut rng, ty, ROWS))
            .collect();
        // A boolean expression (the WHERE-predicate shape) and an integer one
        // (so arithmetic is exercised top-level, not only inside comparisons).
        let int_ty = if rng.one_in(2) { Ty::Int4 } else { Ty::Int8 };
        for expr in [
            gen_expr(&mut rng, Ty::Bool, 4),
            gen_expr(&mut rng, int_ty, 4),
        ] {
            let vectorized = eval_expr(&expr, &columns, ROWS).expect("vectorized eval");
            for row in 0..ROWS {
                let cells: Vec<Option<ScalarValue>> = columns.iter().map(|c| c.get(row)).collect();
                let reference = eval_scalar(&expr, &cells).expect("scalar eval");
                assert_eq!(
                    vectorized.get(row),
                    reference,
                    "seed {seed} row {row}: vectorized vs scalar diverged for {expr:?}",
                );
            }
        }
    }
}
