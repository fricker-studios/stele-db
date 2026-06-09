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
///
/// This is the value behind the first-class `PERIOD` logical type
/// ([`crate::types::ScalarValue::Period`], [STL-180]). Two periods compare
/// lexicographically by `(from, to)` — the natural total order on ranges, and
/// what the derived [`Ord`] gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

    /// Append this period's **Postgres `tsrange` binary** wire representation to
    /// `out` ([STL-180]).
    ///
    /// PERIOD maps to Postgres `tsrange` (a range of `timestamp without time
    /// zone`), whose default `[)` bound flavor is exactly our half-open
    /// `[from, to)`. The byte layout matches Postgres's `range_send`:
    ///
    /// * 1 flags byte — always `RANGE_LB_INC` (lower inclusive), plus
    ///   `RANGE_UB_INF` when the period is open-ended (`to == i64::MAX`).
    /// * the lower bound as an `int32` length (`8`) followed by the `timestamp`
    ///   binary body (microseconds since the **Postgres** epoch, big-endian).
    /// * the upper bound, in the same framing, omitted entirely when open.
    ///
    /// [`Self::from_pg_binary`] is the exact inverse. Timestamps cross the wire
    /// relative to 2000-01-01, so the body is shifted by
    /// [`PG_EPOCH_OFFSET_MICROS`] here and shifted back on the way in.
    ///
    /// # Errors
    ///
    /// [`PeriodWireError::TimestampOutOfRange`] if a finite bound cannot be
    /// re-based onto the Postgres epoch without `i64` overflow — a value so far
    /// before 2000-01-01 that no stock driver could represent it. The codec
    /// rejects it rather than wrap to an unrelated instant.
    pub fn to_pg_binary(self, out: &mut Vec<u8>) -> Result<(), PeriodWireError> {
        let open_upper = self.to == i64::MAX;
        let mut flags = RANGE_LB_INC;
        if open_upper {
            flags |= RANGE_UB_INF;
        }
        out.push(flags);
        push_timestamp(out, self.from)?;
        if !open_upper {
            push_timestamp(out, self.to)?;
        }
        Ok(())
    }

    /// Decode the bytes [`Self::to_pg_binary`] produced — a Postgres `tsrange`
    /// binary value — back into an [`Interval`].
    ///
    /// # Errors
    ///
    /// [`PeriodWireError`] if the buffer is not a half-open `tsrange` Stele can
    /// represent: an empty range, an unbounded or exclusive lower bound, an
    /// inclusive upper bound, a non-8-byte `timestamp` body, a truncated or
    /// trailing buffer, or bounds that do not satisfy `from < to`.
    pub fn from_pg_binary(bytes: &[u8]) -> Result<Self, PeriodWireError> {
        let (&flags, mut rest) = bytes.split_first().ok_or(PeriodWireError::Empty)?;
        if flags & RANGE_EMPTY != 0 {
            return Err(PeriodWireError::EmptyRange);
        }
        // A Stele period always has a finite, inclusive lower bound.
        if flags & RANGE_LB_INF != 0 || flags & RANGE_LB_INC == 0 {
            return Err(PeriodWireError::UnsupportedLowerBound);
        }
        let from = pop_timestamp(&mut rest)?;
        let to = if flags & RANGE_UB_INF != 0 {
            i64::MAX
        } else {
            // …and an exclusive upper bound (or `+∞`); an inclusive upper is a
            // range we did not write and cannot faithfully half-open.
            if flags & RANGE_UB_INC != 0 {
                return Err(PeriodWireError::UnsupportedUpperBound);
            }
            pop_timestamp(&mut rest)?
        };
        if !rest.is_empty() {
            return Err(PeriodWireError::TrailingBytes);
        }
        Self::new(from, to).map_err(PeriodWireError::Interval)
    }
}

/// Microseconds between the Unix epoch and the Postgres epoch (2000-01-01).
///
/// The offset a `timestamp` value carries on the wire: Stele stores Unix-epoch
/// microseconds internally, while Postgres binary `timestamp` is relative to
/// 2000-01-01, so the two differ by exactly this constant.
pub const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

// Postgres range `flags` byte (`rangetypes.h`). Only the bits a half-open
// `tsrange` can carry are named.
const RANGE_EMPTY: u8 = 0x01;
const RANGE_LB_INC: u8 = 0x02;
const RANGE_UB_INC: u8 = 0x04;
const RANGE_LB_INF: u8 = 0x08;
const RANGE_UB_INF: u8 = 0x10;

/// Append one bound as Postgres frames a `tsrange` element: an `int32` length of
/// `8` then the `timestamp` body (big-endian microseconds since 2000-01-01).
///
/// Re-bases onto the Postgres epoch with checked arithmetic so an
/// unrepresentable instant is rejected, never silently wrapped.
fn push_timestamp(out: &mut Vec<u8>, unix_micros: i64) -> Result<(), PeriodWireError> {
    let pg_micros = unix_micros
        .checked_sub(PG_EPOCH_OFFSET_MICROS)
        .ok_or(PeriodWireError::TimestampOutOfRange)?;
    out.extend_from_slice(&8i32.to_be_bytes());
    out.extend_from_slice(&pg_micros.to_be_bytes());
    Ok(())
}

/// Read one length-framed `timestamp` element from the front of `rest`,
/// advancing it past the bytes consumed and shifting back to the Unix epoch.
///
/// The epoch shift is checked: a Postgres-epoch value so large it cannot be a
/// Unix-epoch `i64` is rejected, not wrapped into an unrelated instant.
fn pop_timestamp(rest: &mut &[u8]) -> Result<i64, PeriodWireError> {
    let (len_bytes, after_len) = rest
        .split_first_chunk::<4>()
        .ok_or(PeriodWireError::Truncated)?;
    if i32::from_be_bytes(*len_bytes) != 8 {
        return Err(PeriodWireError::BadElementLength);
    }
    let (body, after_body) = after_len
        .split_first_chunk::<8>()
        .ok_or(PeriodWireError::Truncated)?;
    *rest = after_body;
    i64::from_be_bytes(*body)
        .checked_add(PG_EPOCH_OFFSET_MICROS)
        .ok_or(PeriodWireError::TimestampOutOfRange)
}

/// Why a buffer is not a Postgres `tsrange` binary value Stele can decode into
/// a half-open [`Interval`] ([`Interval::from_pg_binary`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeriodWireError {
    /// The buffer was empty — not even a flags byte.
    #[error("period wire value is empty")]
    Empty,
    /// The range carried the `EMPTY` flag; a Stele period always covers a span.
    #[error("period wire value is the empty range")]
    EmptyRange,
    /// The lower bound was unbounded or exclusive; Stele periods are `[from, …`.
    #[error("period wire value has an unsupported lower bound (must be finite and inclusive)")]
    UnsupportedLowerBound,
    /// The upper bound was inclusive; Stele periods are `…, to)` or open.
    #[error("period wire value has an unsupported upper bound (must be exclusive or +∞)")]
    UnsupportedUpperBound,
    /// A bound element did not advertise the 8-byte `timestamp` width.
    #[error("period wire bound is not an 8-byte timestamp")]
    BadElementLength,
    /// A bound could not be re-based between the Unix and Postgres epochs
    /// without `i64` overflow — an instant no stock driver can represent.
    #[error("period wire bound is out of the representable timestamp range")]
    TimestampOutOfRange,
    /// The buffer ended mid-bound.
    #[error("period wire value is truncated")]
    Truncated,
    /// Bytes remained after both bounds were read.
    #[error("period wire value has trailing bytes")]
    TrailingBytes,
    /// The decoded bounds were not a valid half-open interval (`from >= to`).
    #[error("period wire bounds are not a valid interval: {0}")]
    Interval(IntervalError),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_empty_or_reversed() {
        assert_eq!(
            Interval::new(5, 5),
            Err(IntervalError::EmptyOrReversed(5, 5))
        );
        assert_eq!(
            Interval::new(9, 1),
            Err(IntervalError::EmptyOrReversed(9, 1))
        );
        assert!(Interval::new(1, 9).is_ok());
        // An open-ended period (`to == +∞`) is well-formed.
        assert!(Interval::new(1, i64::MAX).is_ok());
    }

    #[test]
    fn intervals_order_lexicographically_by_from_then_to() {
        let mut ivs = [
            Interval::new(10, 30).unwrap(),
            Interval::new(10, 20).unwrap(),
            Interval::new(5, 100).unwrap(),
            Interval::new(10, i64::MAX).unwrap(),
        ];
        ivs.sort();
        assert_eq!(
            ivs,
            [
                Interval::new(5, 100).unwrap(),
                Interval::new(10, 20).unwrap(),
                Interval::new(10, 30).unwrap(),
                Interval::new(10, i64::MAX).unwrap(),
            ]
        );
    }

    /// 2023-11-14 22:13:20 UTC and one hour later, the goldens reused across the
    /// wire tests. In Unix-epoch microseconds.
    const T0: i64 = 1_700_000_000_000_000;
    const T1: i64 = T0 + 3_600_000_000;

    #[test]
    fn pg_binary_round_trips_closed_and_open_periods() {
        for iv in [
            Interval::new(T0, T1).unwrap(),
            Interval::new(T0, i64::MAX).unwrap(), // open upper
            Interval::new(-1, 1).unwrap(),
            // The widest representable closed range: a lower bound exactly at the
            // Postgres epoch floor, an upper bound just inside the ceiling.
            Interval::new(i64::MIN + PG_EPOCH_OFFSET_MICROS, i64::MAX - 1).unwrap(),
        ] {
            let mut buf = Vec::new();
            iv.to_pg_binary(&mut buf).expect("bound is representable");
            assert_eq!(
                Interval::from_pg_binary(&buf),
                Ok(iv),
                "round-trip changed the period"
            );
        }
    }

    #[test]
    fn pg_binary_rejects_out_of_range_bounds() {
        // A lower bound further before 2000-01-01 than `i64` can re-base: the
        // codec rejects it rather than wrap to an unrelated instant.
        let mut buf = Vec::new();
        assert_eq!(
            Interval::new(i64::MIN, 0).unwrap().to_pg_binary(&mut buf),
            Err(PeriodWireError::TimestampOutOfRange)
        );
        // The decode mirror: a Postgres-epoch body so large it cannot be a
        // Unix-epoch `i64` is rejected, not wrapped.
        let mut overflow = vec![0x02];
        overflow.extend_from_slice(&8i32.to_be_bytes());
        overflow.extend_from_slice(&i64::MAX.to_be_bytes());
        assert_eq!(
            Interval::from_pg_binary(&overflow),
            Err(PeriodWireError::TimestampOutOfRange)
        );
    }

    #[test]
    fn pg_binary_layout_matches_postgres_range_send() {
        // A finite `tsrange`: flags `LB_INC` (0x02), then each bound as an int32
        // length of 8 and a big-endian timestamp body relative to 2000-01-01.
        let mut buf = Vec::new();
        Interval::new(T0, T1)
            .unwrap()
            .to_pg_binary(&mut buf)
            .unwrap();

        let lo = (T0 - PG_EPOCH_OFFSET_MICROS).to_be_bytes();
        let hi = (T1 - PG_EPOCH_OFFSET_MICROS).to_be_bytes();
        let mut expected = vec![0x02];
        expected.extend_from_slice(&8i32.to_be_bytes());
        expected.extend_from_slice(&lo);
        expected.extend_from_slice(&8i32.to_be_bytes());
        expected.extend_from_slice(&hi);
        assert_eq!(buf, expected);

        // An open-ended period sets `UB_INF` (0x10) and omits the upper element.
        let mut open = Vec::new();
        Interval::new(T0, i64::MAX)
            .unwrap()
            .to_pg_binary(&mut open)
            .unwrap();
        let mut expected_open = vec![0x02 | 0x10];
        expected_open.extend_from_slice(&8i32.to_be_bytes());
        expected_open.extend_from_slice(&lo);
        assert_eq!(open, expected_open);
    }

    #[test]
    fn pg_binary_decode_rejects_malformed_ranges() {
        assert_eq!(Interval::from_pg_binary(&[]), Err(PeriodWireError::Empty));
        // EMPTY flag.
        assert_eq!(
            Interval::from_pg_binary(&[0x01]),
            Err(PeriodWireError::EmptyRange)
        );
        // Lower bound infinite (LB_INF) — unrepresentable.
        assert_eq!(
            Interval::from_pg_binary(&[0x08]),
            Err(PeriodWireError::UnsupportedLowerBound)
        );
        // Inclusive upper bound (LB_INC | UB_INC) after a valid lower.
        let mut buf = vec![0x02 | 0x04];
        push_timestamp(&mut buf, T0).unwrap();
        push_timestamp(&mut buf, T1).unwrap();
        assert_eq!(
            Interval::from_pg_binary(&buf),
            Err(PeriodWireError::UnsupportedUpperBound)
        );
        // Trailing bytes after a complete open range.
        let mut trailing = Vec::new();
        Interval::new(T0, i64::MAX)
            .unwrap()
            .to_pg_binary(&mut trailing)
            .unwrap();
        trailing.push(0xFF);
        assert_eq!(
            Interval::from_pg_binary(&trailing),
            Err(PeriodWireError::TrailingBytes)
        );
        // A bound element claiming a width other than 8.
        let mut bad_len = vec![0x02];
        bad_len.extend_from_slice(&4i32.to_be_bytes());
        bad_len.extend_from_slice(&0i32.to_be_bytes());
        assert_eq!(
            Interval::from_pg_binary(&bad_len),
            Err(PeriodWireError::BadElementLength)
        );
        // Truncated mid-bound.
        assert_eq!(
            Interval::from_pg_binary(&[0x02, 0, 0, 0, 8, 0, 0]),
            Err(PeriodWireError::Truncated)
        );
    }
}
