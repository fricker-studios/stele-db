//! Half-open temporal intervals and the SQL:2011 period predicates over them
//! ([STL-165]).
//!
//! A **period** is a half-open `[from, to)` range of microseconds on either
//! bitemporal axis (system or valid) — the same `[start, end)` rule the storage
//! engine enforces ([`docs/16-bitemporal-semantics.md` §2]). This module owns the
//! axis-agnostic *value* (`Interval`) and the *vocabulary* of predicates
//! (`PeriodPredicate`); the binder ([`stele-sql`]) folds SQL `PERIOD(a, b)`
//! operands into `Interval`s, and the executor ([`stele-exec`]) evaluates a
//! predicate over a pair of them.
//!
//! Keeping the type here (rather than in `stele-sql` or `stele-exec`) lets both
//! of those sibling crates name the same predicate without depending on each
//! other — the role `LogicalType` already plays for the scalar type system.
//!
//! ## Half-open is the whole point
//!
//! Every predicate is defined so the boundary cases land on the right side of the
//! `[from, to)` line — the off-by-one trap period predicates exist to get right.
//! `[10, 20)` **precedes** `[20, 30)` (they touch but share no point), and
//! `[10, 20)` does **not** overlap `[20, 30)` (the point `20` belongs to neither
//! the first interval's coverage nor a shared region). The truth table is pinned
//! in [`stele-exec`]'s tests.
//!
//! [`docs/16-bitemporal-semantics.md` §2]: ../../../docs/16-bitemporal-semantics.md#2-intervals
//! [`stele-sql`]: ../../stele_sql/index.html
//! [`stele-exec`]: ../../stele_exec/index.html

/// A half-open interval `[from, to)` of microseconds on one bitemporal axis.
///
/// Constructed through [`Interval::new`], which rejects empty/reversed ranges
/// (`from >= to`), so a value is always well-formed — `from < to`. `to` may be
/// `i64::MAX`, the `+∞` sentinel both axes use for an open-ended period
/// ([`SYSTEM_TIME_OPEN`](crate::time::SYSTEM_TIME_OPEN) /
/// [`VALID_TIME_OPEN`](crate::time::VALID_TIME_OPEN)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    /// Inclusive start of the period.
    pub from: i64,
    /// Exclusive end of the period; `i64::MAX` for an open-ended period.
    pub to: i64,
}

/// Why a [`PERIOD(from, to)`](Interval) operand is not a valid interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IntervalError {
    /// `from >= to`: the period is empty or reversed. Half-open `[from, to)`
    /// requires the start strictly before the end — a zero-length period covers
    /// no point, a reversed one is nonsense ([`docs/16` §2]).
    ///
    /// [`docs/16` §2]: ../../../docs/16-bitemporal-semantics.md#2-intervals
    #[error("period is empty or reversed: from ({0}) must be < to ({1})")]
    EmptyOrReversed(i64, i64),
}

impl Interval {
    /// Build a half-open `[from, to)` interval.
    ///
    /// # Errors
    ///
    /// [`IntervalError::EmptyOrReversed`] if `from >= to`.
    pub const fn new(from: i64, to: i64) -> Result<Self, IntervalError> {
        if from >= to {
            return Err(IntervalError::EmptyOrReversed(from, to));
        }
        Ok(Self { from, to })
    }

    /// Whether `point` lies in `[from, to)` — half-open membership.
    #[must_use]
    pub const fn contains_point(&self, point: i64) -> bool {
        self.from <= point && point < self.to
    }
}

/// The SQL:2011 period predicates Stele evaluates over a pair of [`Interval`]s
/// ([STL-165]).
///
/// Each is a boolean relation `a <pred> b` between a left interval `a` and a
/// right interval `b`; see `stele_exec::evaluate` for the exact half-open
/// semantics and the truth table.
///
/// `MEETS` in the SQL surface is a spelling of [`ImmediatelyPrecedes`]
/// (Allen's interval algebra "meets"): `a.to == b.from`.
///
/// [`ImmediatelyPrecedes`]: PeriodPredicate::ImmediatelyPrecedes
/// [`stele-exec`]: ../../stele_exec/index.html
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodPredicate {
    /// `a CONTAINS b`: every point of `b` is within `a` (`a.from <= b.from` and
    /// `b.to <= a.to`).
    Contains,
    /// `a OVERLAPS b`: `a` and `b` share at least one point.
    Overlaps,
    /// `a EQUALS b`: identical bounds.
    Equals,
    /// `a PRECEDES b`: `a` ends at or before `b` starts (`a.to <= b.from`).
    Precedes,
    /// `a SUCCEEDS b`: `a` starts at or after `b` ends (`b.to <= a.from`).
    Succeeds,
    /// `a IMMEDIATELY PRECEDES b` (a.k.a. `MEETS`): `a.to == b.from`.
    ImmediatelyPrecedes,
    /// `a IMMEDIATELY SUCCEEDS b`: `a.from == b.to`.
    ImmediatelySucceeds,
}

impl PeriodPredicate {
    /// The canonical SQL keyword(s) for this predicate, for diagnostics.
    #[must_use]
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::Contains => "CONTAINS",
            Self::Overlaps => "OVERLAPS",
            Self::Equals => "EQUALS",
            Self::Precedes => "PRECEDES",
            Self::Succeeds => "SUCCEEDS",
            Self::ImmediatelyPrecedes => "IMMEDIATELY PRECEDES",
            Self::ImmediatelySucceeds => "IMMEDIATELY SUCCEEDS",
        }
    }
}
