//! Civil-time text → epoch-microseconds parsing, for the time-zone-aware
//! `timestamptz` type ([STL-189]).
//!
//! Stele stores every instant as microseconds since the Unix epoch in **UTC**
//! ([ADR-0024](../../../docs/adr/0024-time-representation.md)). A `timestamptz`
//! literal a client writes carries a zone offset (`+05`, `-08:00`, `Z`, …); this
//! module is the seam that normalizes it to that single UTC scale, so two
//! literals naming the *same instant in different zones* fold to the *same*
//! `i64` — the property the round-trip Definition of Done rests on.
//!
//! The inverse — rendering a stored UTC instant back to text — is the wire
//! encoder's job ([`stele-pgwire`'s `text_format`]); `timestamptz` always renders
//! with a `+00` offset because the engine has no session time zone to localize
//! into ([16 §10](../../../docs/16-bitemporal-semantics.md#10-valid-time-as-a-business-date)).
//!
//! ## What is accepted
//!
//! ```text
//! YYYY-MM-DD<sep>HH:MM[:SS][.ffffff][zone]
//!   <sep>  one ASCII space or `T`/`t`
//!   zone   `Z`/`z`, `±HH`, `±HH:MM`, `±HHMM`, `±HH:MM:SS`, or absent (= UTC)
//! ```
//!
//! Calendar fields are fully range-checked (months, days-in-month with the
//! proleptic-Gregorian leap rule, clock fields); sub-microsecond fractional
//! digits are truncated, the µs floor [ADR-0024] makes load-bearing. A literal
//! with no zone is read as UTC — the engine is UTC-internal and exposes no
//! session zone to default to, so this is a *defined* choice, not Postgres's
//! session-relative one (documented in [16 §10]).
//!
//! [STL-189]: https://allegromusic.atlassian.net/browse/STL-189

/// Microseconds in one second.
const MICROS_PER_SEC: i64 = 1_000_000;
/// Microseconds in one calendar day (no leap seconds — UTC instants, per
/// [ADR-0024](../../../docs/adr/0024-time-representation.md)).
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;
/// Seconds in one hour, for offset arithmetic.
const SECS_PER_HOUR: i64 = 3_600;

/// Why a `timestamptz` literal could not be parsed. Carries the offending text
/// and a short, stable reason for diagnostics; the SQL binder re-wraps this with
/// the column name it knows.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid input syntax for type timestamp with time zone: \"{literal}\" ({reason})")]
pub struct TimestamptzParseError {
    /// The literal text that failed to parse.
    pub literal: String,
    /// A short, stable explanation of what was wrong.
    pub reason: &'static str,
}

/// Parse a `timestamptz` literal into microseconds since the Unix epoch, **UTC**.
///
/// The literal's zone offset is subtracted so the result is on the engine's
/// single UTC scale: `2024-01-15 12:00:00+05` and `2024-01-15 02:00:00-05` both
/// return the instant for `2024-01-15 07:00:00Z`. A literal with no zone is read
/// as already-UTC (see the module docs).
///
/// # Errors
///
/// [`TimestamptzParseError`] if the text is not a well-formed civil timestamp
/// with an optional zone, if any calendar/clock field is out of range, or if the
/// instant overflows `i64` microseconds.
///
/// ```
/// use stele_common::datetime::parse_timestamptz;
///
/// // The two offsets name the same instant, so they fold to the same micros.
/// let a = parse_timestamptz("2024-01-15 12:00:00+05").unwrap();
/// let b = parse_timestamptz("2024-01-15 02:00:00-05").unwrap();
/// assert_eq!(a, b);
/// // …which is 2024-01-15 07:00:00 UTC.
/// assert_eq!(a, parse_timestamptz("2024-01-15 07:00:00Z").unwrap());
/// ```
pub fn parse_timestamptz(input: &str) -> Result<i64, TimestamptzParseError> {
    let err = |reason: &'static str| TimestamptzParseError {
        literal: input.to_owned(),
        reason,
    };

    let s = input.trim();
    // Date and time-of-day are split on a single ASCII space or `T`. The date is
    // pure `YYYY-MM-DD` (its `-`s are field separators), so the zone sign can only
    // appear in the time half — that is what makes the later sign-scan unambiguous.
    let sep = s
        .find([' ', 'T', 't'])
        .ok_or_else(|| err("expected a date and time separated by space or 'T'"))?;
    let (date_str, after) = s.split_at(sep);
    let rest = &after[1..];

    // Peel the zone off the clock: the first `+`/`-`/`Z` in the time half begins
    // it (the clock itself has none). Absent → UTC.
    let (clock_str, zone_str) = rest
        .find(['+', '-', 'Z', 'z'])
        .map_or((rest, ""), |i| (&rest[..i], &rest[i..]));

    let (year, month, day) = parse_date(date_str).ok_or_else(|| err("malformed date"))?;
    if !(1..=12).contains(&month) {
        return Err(err("month out of range"));
    }
    if !(1..=days_in_month(year, month)).contains(&day) {
        return Err(err("day out of range for month"));
    }

    let (hh, mm, ss, frac_us) = parse_clock(clock_str).ok_or_else(|| err("malformed time"))?;
    if hh > 23 || mm > 59 || ss > 59 {
        // Leap seconds (`:60`) are deliberately not representable — Stele's
        // physical type is leap-second-free UTC microseconds ([ADR-0024]).
        return Err(err("clock field out of range"));
    }

    let offset_secs = parse_zone(zone_str).ok_or_else(|| err("malformed time zone offset"))?;

    let days = days_from_civil(year, month, day).ok_or_else(|| err("instant out of range"))?;
    let tod_us = (i64::from(hh) * SECS_PER_HOUR + i64::from(mm) * 60 + i64::from(ss))
        * MICROS_PER_SEC
        + frac_us;
    let local_us = days
        .checked_mul(MICROS_PER_DAY)
        .and_then(|d| d.checked_add(tod_us))
        .ok_or_else(|| err("instant out of range"))?;
    // Subtract the zone offset to land on UTC: a `+05` literal is five hours
    // *ahead* of UTC, so its UTC instant is five hours *earlier*.
    local_us
        .checked_sub(offset_secs * MICROS_PER_SEC)
        .ok_or_else(|| err("instant out of range"))
}

/// Parse a `YYYY-MM-DD` date into `(year, month, day)` with a non-negative,
/// at-least-4-digit year (proleptic BC dates are out of scope on input). `None`
/// on any structural problem; field-range checks are the caller's.
fn parse_date(s: &str) -> Option<(i64, u32, u32)> {
    let mut parts = s.split('-');
    let y = parts.next()?;
    let m = parts.next()?;
    let d = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    // A 4+-digit year keeps the grammar unambiguous and matches ISO-8601 / what
    // the encoder emits; `01-02-03` is rejected rather than guessed.
    if y.len() < 4 || !all_ascii_digits(y) || !all_ascii_digits(m) || !all_ascii_digits(d) {
        return None;
    }
    Some((y.parse().ok()?, m.parse().ok()?, d.parse().ok()?))
}

/// Parse `HH:MM[:SS][.ffffff]` into `(hour, minute, second, fractional µs)`.
/// Missing seconds default to `0`; fractional digits beyond microseconds are
/// truncated. `None` on any structural problem; field-range checks are the
/// caller's.
fn parse_clock(s: &str) -> Option<(u32, u32, u32, i64)> {
    let (clock, frac) = match s.split_once('.') {
        Some((c, f)) => (c, Some(f)),
        None => (s, None),
    };
    let mut parts = clock.split(':');
    let hh = parts.next()?;
    let mm = parts.next()?;
    let ss = parts.next();
    if parts.next().is_some() {
        return None;
    }
    if hh.len() != 2 || mm.len() != 2 || !all_ascii_digits(hh) || !all_ascii_digits(mm) {
        return None;
    }
    let ss_val = match ss {
        Some(ss) if ss.len() == 2 && all_ascii_digits(ss) => ss.parse().ok()?,
        Some(_) => return None,
        None => 0,
    };

    let frac_us = match frac {
        None => 0,
        Some(f) if !f.is_empty() && all_ascii_digits(f) => {
            // Pad/truncate to exactly 6 digits: `.12` is 120000µs, `.1234567` is
            // 123456µs (the 7th digit is below the µs floor and dropped).
            let mut digits = String::with_capacity(6);
            digits.extend(f.chars().take(6));
            while digits.len() < 6 {
                digits.push('0');
            }
            digits.parse().ok()?
        }
        Some(_) => return None,
    };

    Some((hh.parse().ok()?, mm.parse().ok()?, ss_val, frac_us))
}

/// Parse a zone suffix into a signed offset in **seconds** east of UTC. Accepts
/// `""` (UTC), `Z`/`z`, and `±HH`, `±HHMM`, `±HH:MM`, `±HH:MM:SS`.
fn parse_zone(s: &str) -> Option<i64> {
    if s.is_empty() || s.eq_ignore_ascii_case("z") {
        return Some(0);
    }
    let (sign, rest) = match s.split_at(1) {
        ("+", rest) => (1, rest),
        ("-", rest) => (-1, rest),
        _ => return None,
    };
    // Hours, then optional minutes, then optional seconds — colon-separated, or a
    // bare `HHMM` with no colons.
    let (hh, mm, ss) = if rest.contains(':') {
        let mut parts = rest.split(':');
        let hh = parts.next()?;
        let mm = parts.next().unwrap_or("00");
        let ss = parts.next().unwrap_or("00");
        if parts.next().is_some() {
            return None;
        }
        (hh, mm, ss)
    } else {
        match rest.len() {
            2 => (rest, "00", "00"),
            4 => (&rest[..2], &rest[2..], "00"),
            _ => return None,
        }
    };
    if hh.len() != 2 || mm.len() != 2 || ss.len() != 2 {
        return None;
    }
    if !all_ascii_digits(hh) || !all_ascii_digits(mm) || !all_ascii_digits(ss) {
        return None;
    }
    let (h, m, sec): (i64, i64, i64) = (hh.parse().ok()?, mm.parse().ok()?, ss.parse().ok()?);
    if h > 23 || m > 59 || sec > 59 {
        return None;
    }
    Some(sign * (h * SECS_PER_HOUR + m * 60 + sec))
}

/// Days in `month` of `year`, with the proleptic-Gregorian leap-year rule. The
/// caller has already bounded `month` to `1..=12`.
const fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// The proleptic-Gregorian leap-year rule: divisible by 4, except centuries not
/// divisible by 400.
const fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Days from the Unix epoch (1970-01-01) for a proleptic-Gregorian `(year,
/// month, day)`, or `None` if the result does not fit `i64`. Howard Hinnant's
/// `days_from_civil` (public domain) — the exact inverse of the encoder's
/// `civil_from_days`.
///
/// The intermediates (`era * 146_097`) overflow `i64` for years near its bounds —
/// a `9223372036854775807-01-01` literal would silently wrap — so the math runs
/// in `i128` and narrows back with a checked conversion. An out-of-range year
/// then surfaces as the caller's `"instant out of range"` rather than a wrong
/// instant or a debug panic.
fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    let m = i128::from(month);
    let d = i128::from(day);
    let y = i128::from(year) - i128::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    i64::try_from(era * 146_097 + doe - 719_468).ok()
}

/// True if every byte of `s` is an ASCII digit (and `s` is non-empty).
fn all_ascii_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Definition-of-Done property: the same instant written in different
    /// zones folds to one UTC microsecond value, across at least two offsets.
    #[test]
    fn equal_instants_in_different_zones_fold_equal() {
        let utc = parse_timestamptz("2024-01-15 07:00:00Z").unwrap();
        assert_eq!(parse_timestamptz("2024-01-15 12:00:00+05").unwrap(), utc);
        assert_eq!(parse_timestamptz("2024-01-15 02:00:00-05").unwrap(), utc);
        assert_eq!(parse_timestamptz("2024-01-15 07:00:00+00").unwrap(), utc);
        assert_eq!(parse_timestamptz("2024-01-15 07:00:00").unwrap(), utc);
    }

    #[test]
    fn known_instant_matches_epoch_micros() {
        // 1_700_000_000 s = 2023-11-14 22:13:20 UTC.
        assert_eq!(
            parse_timestamptz("2023-11-14 22:13:20Z").unwrap(),
            1_700_000_000_000_000
        );
        // The Unix epoch itself.
        assert_eq!(parse_timestamptz("1970-01-01 00:00:00+00").unwrap(), 0);
    }

    #[test]
    fn fractional_offset_zones_apply() {
        // India's zone is +05:30; 17:30 local there is 12:00 UTC.
        assert_eq!(
            parse_timestamptz("2024-01-15 17:30:00+05:30").unwrap(),
            parse_timestamptz("2024-01-15 12:00:00Z").unwrap()
        );
        // A bare `±HHMM` form is also accepted.
        assert_eq!(
            parse_timestamptz("2024-01-15 17:30:00+0530").unwrap(),
            parse_timestamptz("2024-01-15 12:00:00Z").unwrap()
        );
    }

    #[test]
    fn fractional_seconds_truncate_to_micros() {
        let base = parse_timestamptz("2023-11-14 22:13:20Z").unwrap();
        assert_eq!(
            parse_timestamptz("2023-11-14 22:13:20.123456Z").unwrap(),
            base + 123_456
        );
        // `.12` is 120000µs, not 12µs.
        assert_eq!(
            parse_timestamptz("2023-11-14 22:13:20.12Z").unwrap(),
            base + 120_000
        );
        // The 7th fractional digit is below the µs floor and is dropped.
        assert_eq!(
            parse_timestamptz("2023-11-14 22:13:20.1234569Z").unwrap(),
            base + 123_456
        );
    }

    #[test]
    fn t_separator_and_missing_seconds_are_accepted() {
        assert_eq!(
            parse_timestamptz("2024-01-15T12:00:00+05").unwrap(),
            parse_timestamptz("2024-01-15 12:00:00+05").unwrap()
        );
        // `HH:MM` with no seconds defaults the seconds to zero.
        assert_eq!(
            parse_timestamptz("2024-01-15 07:00Z").unwrap(),
            parse_timestamptz("2024-01-15 07:00:00Z").unwrap()
        );
    }

    #[test]
    fn pre_epoch_instants_are_negative() {
        // One microsecond before the epoch.
        assert_eq!(
            parse_timestamptz("1969-12-31 23:59:59.999999Z").unwrap(),
            -1
        );
    }

    #[test]
    fn leap_day_round_trips_but_invalid_days_reject() {
        // 2000 is a leap year (÷400): Feb 29 is valid.
        assert!(parse_timestamptz("2000-02-29 00:00:00Z").is_ok());
        // 2023 is not: Feb 29 does not exist.
        assert!(parse_timestamptz("2023-02-29 00:00:00Z").is_err());
        // 1900 is a century not divisible by 400: not a leap year.
        assert!(parse_timestamptz("1900-02-29 00:00:00Z").is_err());
    }

    #[test]
    fn out_of_range_and_malformed_fields_reject() {
        for bad in [
            "",
            "2024-01-15",             // no time
            "2024-13-01 00:00:00Z",   // month 13
            "2024-00-01 00:00:00Z",   // month 0
            "2024-01-32 00:00:00Z",   // day 32
            "2024-01-15 24:00:00Z",   // hour 24
            "2024-01-15 12:60:00Z",   // minute 60
            "2024-01-15 23:59:60Z",   // leap second rejected
            "2024-01-15 12:00:00+99", // absurd offset
            "2024-01-15 12:00:00+5",  // one-digit hour offset
            "not-a-timestamp",
            "24-01-15 12:00:00Z", // two-digit year is ambiguous, rejected
        ] {
            assert!(
                parse_timestamptz(bad).is_err(),
                "`{bad}` should be rejected"
            );
        }
    }

    /// `days_from_civil` here must be the exact inverse of the encoder's
    /// `civil_from_days`; check a dense range so the calendar math is exact across
    /// eras, not just at hand-picked dates.
    #[test]
    fn days_from_civil_is_exact_across_eras() {
        // Howard Hinnant's `civil_from_days` — the inverse we validate against.
        fn civil_from_days(days: i64) -> (i64, u32, u32) {
            let z = days + 719_468;
            let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
            let doe = z - era * 146_097;
            let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
            let y = yoe + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            (y + i64::from(m <= 2), m as u32, d as u32)
        }
        for days in (-800_000..=800_000).step_by(37) {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(
                days_from_civil(y, m, d),
                Some(days),
                "round-trip failed at {days}"
            );
        }
    }

    #[test]
    fn astronomically_large_year_is_rejected_not_wrapped() {
        // A year near `i64::MAX` overflows the `era * 146_097` intermediate; the
        // i128 math + checked narrowing must turn that into a clean error rather
        // than a silently-wrapped (or debug-panicking) instant.
        assert!(parse_timestamptz("9223372036854775807-01-01 00:00:00Z").is_err());
        // A merely huge-but-representable calendar year still overflows the µs
        // instant, which the caller's `checked_mul` rejects.
        assert!(parse_timestamptz("900000-01-01 00:00:00Z").is_err());
    }
}
