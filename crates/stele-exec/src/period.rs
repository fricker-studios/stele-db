//! Evaluating the SQL:2011 period predicates over half-open intervals
//! ([STL-165]).
//!
//! The [`Interval`] value and the [`PeriodPredicate`] vocabulary live in
//! [`stele_common::period`] (so the binder and the executor share one
//! definition); this module is the *evaluator* — the half-open semantics that
//! turn a `(predicate, a, b)` triple into a truth value. The binder folds a SQL
//! `PERIOD(from, to) <pred> PERIOD(from, to)` into two `Interval`s and a
//! `PeriodPredicate`; the future SQL-path correctness oracle (STL-167 `[O1]`)
//! and the row-filter operator call [`evaluate`] per probe.
//!
//! ## Half-open boundary semantics
//!
//! All intervals are `[from, to)` with `to = i64::MAX` denoting `+∞`. The
//! definitions below are written so a touching boundary lands on the correct
//! side of the line:
//!
//! | predicate              | `a <pred> b` is true iff      |
//! |------------------------|-------------------------------|
//! | `CONTAINS`             | `a.from <= b.from && b.to <= a.to` |
//! | `OVERLAPS`             | `a.from < b.to && b.from < a.to`   |
//! | `EQUALS`               | `a.from == b.from && a.to == b.to` |
//! | `PRECEDES`             | `a.to <= b.from`              |
//! | `SUCCEEDS`             | `b.to <= a.from`              |
//! | `IMMEDIATELY PRECEDES` | `a.to == b.from`              |
//! | `IMMEDIATELY SUCCEEDS` | `a.from == b.to`              |
//!
//! `MEETS` is the SQL surface spelling of `IMMEDIATELY PRECEDES`. The exhaustive
//! boundary truth table is pinned in this module's tests.

use stele_common::period::{Interval, PeriodPredicate};

/// Evaluate `a <predicate> b` over two half-open `[from, to)` intervals.
///
/// Pure and total: every predicate is defined for every well-formed interval
/// pair (including `+∞` ends), so there is no error path. See this module's
/// documentation for the boundary semantics and truth table.
#[must_use]
pub const fn evaluate(predicate: PeriodPredicate, a: Interval, b: Interval) -> bool {
    match predicate {
        // Every point of `b` falls within `a`. With `from` inclusive and `to`
        // exclusive on both, the closed comparison on the bounds is exact.
        PeriodPredicate::Contains => a.from <= b.from && b.to <= a.to,
        // Nonempty intersection: each starts strictly before the other ends. The
        // strict `<` is what makes `[10,20)` and `[20,30)` *not* overlap.
        PeriodPredicate::Overlaps => a.from < b.to && b.from < a.to,
        PeriodPredicate::Equals => a.from == b.from && a.to == b.to,
        // `a` lies entirely at or before `b`. Touching (`a.to == b.from`) counts
        // — the shared instant belongs to neither half-open interval.
        PeriodPredicate::Precedes => a.to <= b.from,
        PeriodPredicate::Succeeds => b.to <= a.from,
        // Adjacency: the intervals touch exactly, with no gap and no overlap.
        PeriodPredicate::ImmediatelyPrecedes => a.to == b.from,
        PeriodPredicate::ImmediatelySucceeds => a.from == b.to,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::period::PeriodPredicate::{
        Contains, Equals, ImmediatelyPrecedes, ImmediatelySucceeds, Overlaps, Precedes, Succeeds,
    };

    fn iv(from: i64, to: i64) -> Interval {
        Interval::new(from, to).expect("well-formed test interval")
    }

    /// Assert the full predicate truth table for one ordered pair `(a, b)`.
    /// Each `expected_*` is whether `a <pred> b` should hold.
    #[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
    fn check(
        a: Interval,
        b: Interval,
        contains: bool,
        overlaps: bool,
        equals: bool,
        precedes: bool,
        succeeds: bool,
        imm_precedes: bool,
        imm_succeeds: bool,
    ) {
        assert_eq!(evaluate(Contains, a, b), contains, "CONTAINS {a:?} {b:?}");
        assert_eq!(evaluate(Overlaps, a, b), overlaps, "OVERLAPS {a:?} {b:?}");
        assert_eq!(evaluate(Equals, a, b), equals, "EQUALS {a:?} {b:?}");
        assert_eq!(evaluate(Precedes, a, b), precedes, "PRECEDES {a:?} {b:?}");
        assert_eq!(evaluate(Succeeds, a, b), succeeds, "SUCCEEDS {a:?} {b:?}");
        assert_eq!(
            evaluate(ImmediatelyPrecedes, a, b),
            imm_precedes,
            "IMMEDIATELY PRECEDES {a:?} {b:?}"
        );
        assert_eq!(
            evaluate(ImmediatelySucceeds, a, b),
            imm_succeeds,
            "IMMEDIATELY SUCCEEDS {a:?} {b:?}"
        );
    }

    // The truth table is organized by the geometric relationship between the two
    // intervals; every column is asserted in each row, so each predicate is
    // exercised true *and* false across the boundary cases.

    #[test]
    fn disjoint_with_a_gap() {
        // a = [10,20), b = [30,40): a strictly before b, with a gap.
        //          contains overlaps equals precedes succeeds immP  immS
        check(
            iv(10, 20),
            iv(30, 40),
            false,
            false,
            false,
            true,
            false,
            false,
            false,
        );
        // The mirror: b before a → a succeeds b.
        check(
            iv(30, 40),
            iv(10, 20),
            false,
            false,
            false,
            false,
            true,
            false,
            false,
        );
    }

    #[test]
    fn touching_is_the_half_open_boundary() {
        // a = [10,20), b = [20,30): a.to == b.from. They touch but share no
        // point — PRECEDES holds, OVERLAPS does not, and this is the MEETS case.
        check(
            iv(10, 20),
            iv(20, 30),
            false,
            false,
            false,
            true,
            false,
            true,
            false,
        );
        // Mirror: a = [20,30), b = [10,20): a.from == b.to → SUCCEEDS + imm-succeeds.
        check(
            iv(20, 30),
            iv(10, 20),
            false,
            false,
            false,
            false,
            true,
            false,
            true,
        );
    }

    #[test]
    fn overlapping_by_one_microsecond() {
        // a = [10,21), b = [20,30): they share exactly the point 20.
        check(
            iv(10, 21),
            iv(20, 30),
            false,
            true,
            false,
            false,
            false,
            false,
            false,
        );
    }

    #[test]
    fn proper_containment() {
        // a = [10,40) strictly contains b = [20,30).
        check(
            iv(10, 40),
            iv(20, 30),
            true,
            true,
            false,
            false,
            false,
            false,
            false,
        );
        // b does not contain a, but they still overlap.
        check(
            iv(20, 30),
            iv(10, 40),
            false,
            true,
            false,
            false,
            false,
            false,
            false,
        );
    }

    #[test]
    fn shared_boundary_containment() {
        // Containment is closed on `from` and on `to`: equal-start / equal-end
        // nested intervals are contained.
        check(
            iv(10, 30),
            iv(10, 20),
            true,
            true,
            false,
            false,
            false,
            false,
            false,
        );
        check(
            iv(10, 30),
            iv(20, 30),
            true,
            true,
            false,
            false,
            false,
            false,
            false,
        );
    }

    #[test]
    fn equality_contains_and_overlaps_itself() {
        // Equal intervals contain each other and overlap; never precede/succeed.
        check(
            iv(10, 20),
            iv(10, 20),
            true,
            true,
            true,
            false,
            false,
            false,
            false,
        );
    }

    #[test]
    fn open_ended_to_infinity() {
        let inf = i64::MAX;
        // a = [10,+∞) contains every later finite interval and overlaps it.
        check(
            iv(10, inf),
            iv(20, 30),
            true,
            true,
            false,
            false,
            false,
            false,
            false,
        );
        // Two open-ended intervals with the same start are equal (and contain).
        check(
            iv(10, inf),
            iv(10, inf),
            true,
            true,
            true,
            false,
            false,
            false,
            false,
        );
        // A finite interval immediately precedes an open-ended one that starts
        // where it ends.
        check(
            iv(10, 20),
            iv(20, inf),
            false,
            false,
            false,
            true,
            false,
            true,
            false,
        );
    }
}
