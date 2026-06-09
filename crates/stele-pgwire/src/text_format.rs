//! Postgres **text-format** value encoding for Stele's scalar set
//! (originally the v0.1 types, [STL-105]; extended with `uuid` / `bytea` in
//! [STL-181]).
//!
//! [`stele_common::types`] fixes the logical types and their OIDs but
//! deliberately stops short of wire serialization — turning a [`ScalarValue`]
//! into the bytes a `DataRow` carries is the wire front end's job, and this is
//! where it lives. The simple-query loop sends every value in **text format**
//! (format code `0`); binary format rides in with Extended Query in v0.2, so
//! there is exactly one rendering per type here.
//!
//! The contract is *bug-for-bug* Postgres text output, because the whole point
//! of the wire protocol is that a stock `psql` / driver displays a Stele value
//! identically to a Postgres one:
//!
//! | type        | rendering                                            |
//! |-------------|------------------------------------------------------|
//! | `int4`/`int8` | decimal, e.g. `42`, `-1`                           |
//! | `text`        | the bytes verbatim                                 |
//! | `bool`        | `t` / `f`                                          |
//! | `timestamp`   | ISO-8601 `YYYY-MM-DD HH:MM:SS[.ffffff]` (UTC)      |
//! | `timestamptz` | the same, with a `+00` UTC offset suffix           |
//! | `date`        | ISO-8601 `YYYY-MM-DD`                              |
//! | `uuid`        | canonical lowercase `8-4-4-4-12` hex               |
//! | `bytea`       | `\x` + lowercase hex (Postgres `bytea_output = hex`) |
//! | `period`      | `tsrange` text, e.g. `["…","…")` (half-open)       |
//!
//! Timestamps and dates are stored as raw offsets from the Unix epoch
//! (microseconds and days respectively — see [`stele_common::types`]); the
//! workspace pulls in no calendar crate, so the proleptic-Gregorian conversion
//! is done here with Howard Hinnant's well-known `civil_from_days` algorithm.
//! Years at or before astronomical `0` render with Postgres's ` BC` suffix
//! (`0001-01-01 BC` is proleptic year `0`).
//!
//! NULL is *not* a [`ScalarValue`]; the `DataRow` writer renders a NULL cell as
//! the length-`-1` sentinel ([STL-105] Definition of Done) and never calls in
//! here.

use stele_common::period::Interval;
use stele_common::types::{LogicalType, ScalarValue};

/// Microseconds in one second.
const MICROS_PER_SEC: i64 = 1_000_000;
/// Microseconds in one calendar day (no leap seconds — UTC instants).
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

/// Postgres `pg_type.typlen` for a logical type: the fixed on-the-wire width of
/// the binary form, or `-1` for the variable-length types (`text`, `bytea`).
/// This is the `typlen` advertised in a `RowDescription` field — it describes the
/// type, not the rendered text length (text format is always length-prefixed in
/// the `DataRow` regardless).
pub(crate) const fn pg_typlen(ty: LogicalType) -> i16 {
    match ty {
        LogicalType::Int4 | LogicalType::Date => 4,
        LogicalType::Int8 | LogicalType::Timestamp | LogicalType::TimestampTz => 8,
        LogicalType::Bool => 1,
        LogicalType::Uuid => 16,
        // `text`, `bytea`, and `tsrange` are all variable-length (`typlen = -1`).
        LogicalType::Text | LogicalType::Bytea | LogicalType::Period => -1,
    }
}

/// Render a non-null [`ScalarValue`] in Postgres text format. The returned
/// `String` is the exact byte payload of the value's `DataRow` field.
pub(crate) fn encode_text(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Int4(v) => v.to_string(),
        ScalarValue::Int8(v) => v.to_string(),
        ScalarValue::Text(s) => s.clone(),
        // Postgres prints booleans as the single chars `t` / `f`, not
        // `true` / `false`, in both text output and `\d`-style displays.
        ScalarValue::Bool(b) => if *b { "t" } else { "f" }.to_owned(),
        ScalarValue::Timestamp(micros) => format_timestamp(*micros),
        // `timestamptz` shares the civil-time rendering but appends the UTC zone
        // offset: the engine is UTC-internal, so the offset is always `+00`.
        ScalarValue::TimestampTz(micros) => format_timestamptz(*micros),
        ScalarValue::Date(days) => format_date(*days),
        ScalarValue::Uuid(bytes) => format_uuid(bytes),
        ScalarValue::Bytea(bytes) => format_bytea(bytes),
        ScalarValue::Period(iv) => format_period(iv),
    }
}

/// Render a `period` in Postgres `tsrange` text format: a `[`, the lower bound,
/// the upper bound, and a `)` — lower inclusive, upper exclusive, matching the
/// half-open `[from, to)` value. Each finite bound is the `timestamp` rendering
/// in double quotes (Postgres quotes range elements that contain spaces); an
/// open-ended period (`to == i64::MAX`) leaves the upper slot empty, exactly as
/// `psql` prints `["…",)`.
fn format_period(iv: &Interval) -> String {
    let lower = format_timestamp(iv.from);
    if iv.to == i64::MAX {
        format!("[\"{lower}\",)")
    } else {
        let upper = format_timestamp(iv.to);
        format!("[\"{lower}\",\"{upper}\")")
    }
}

/// Append `byte` as two lowercase hex digits (high nibble then low) — the shared
/// primitive behind the `uuid` and `bytea` renderings.
fn push_hex(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(HEX[usize::from(byte >> 4)] as char);
    out.push(HEX[usize::from(byte & 0x0F)] as char);
}

/// Render a 16-byte UUID as Postgres's canonical lowercase `8-4-4-4-12` form,
/// e.g. `550e8400-e29b-41d4-a716-446655440000`. Hyphens fall after bytes 4, 6,
/// 8, and 10.
fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, &b) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        push_hex(&mut out, b);
    }
    out
}

/// Render a byte string in Postgres's default `bytea` hex output: a `\x` prefix
/// followed by two lowercase hex digits per byte (empty input renders as `\x`).
fn format_bytea(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for &b in bytes {
        push_hex(&mut out, b);
    }
    out
}

/// Convert days since the Unix epoch (1970-01-01) into a proleptic-Gregorian
/// `(year, month, day)`. `year` is *astronomical* (… `-1`, `0`, `1` …); the
/// caller maps `year <= 0` to Postgres's `n BC` display via [`pg_year`].
///
/// This is Howard Hinnant's `civil_from_days` (public domain), the standard
/// branch-free epoch→calendar conversion; it is exact across the full `i64`
/// day range, including dates before the epoch.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // Shift the era origin from 1970-01-01 to 0000-03-01 so leap-year math is
    // uniform within a 400-year era. Everything stays `i64` — the intermediates
    // are small and bounded, and avoiding `u32` narrowing keeps the conversion
    // cast-free.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // day-of-era      [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day-of-year, Mar-based [0, 365]
    let mp = (5 * doy + 2) / 153; // Mar-based month [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    // March-based years roll the Jan/Feb tail into the next calendar year.
    (year + i64::from(month <= 2), month, day)
}

/// Map an astronomical year to Postgres's display year and era suffix: there is
/// no year `0` in the AD/BC scheme, so astronomical `0` is `1 BC`, `-1` is
/// `2 BC`, and so on. AD years carry an empty suffix.
const fn pg_year(year: i64) -> (i64, &'static str) {
    if year <= 0 {
        (1 - year, " BC")
    } else {
        (year, "")
    }
}

/// Render a `date` (days since the Unix epoch) as `YYYY-MM-DD`, with a trailing
/// ` BC` for non-positive astronomical years.
fn format_date(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    let (y, suffix) = pg_year(year);
    format!("{y:04}-{month:02}-{day:02}{suffix}")
}

/// Render a `timestamp` (microseconds since the Unix epoch, UTC) as
/// `YYYY-MM-DD HH:MM:SS[.ffffff]`. Fractional seconds are emitted only when
/// non-zero and with trailing zeros trimmed, matching Postgres's ISO output.
fn format_timestamp(micros: i64) -> String {
    // Floor-divide so the time-of-day is always in `[0, MICROS_PER_DAY)` even
    // for instants before the epoch (e.g. `-1µs` is `1969-12-31 23:59:59.999999`).
    let days = micros.div_euclid(MICROS_PER_DAY);
    let tod = micros.rem_euclid(MICROS_PER_DAY);

    let (year, month, day) = civil_from_days(days);
    let (y, suffix) = pg_year(year);

    let secs = tod / MICROS_PER_SEC;
    let frac = tod % MICROS_PER_SEC;
    let (hh, mm, ss) = (secs / 3600, (secs % 3600) / 60, secs % 60);

    let mut out = format!("{y:04}-{month:02}-{day:02} {hh:02}:{mm:02}:{ss:02}");
    if frac != 0 {
        // `frac` is in `[1, 999_999]`; zero-pad to 6 digits then trim trailing
        // zeros so `.120000` shows as `.12`, matching Postgres.
        let digits = format!("{frac:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    out.push_str(suffix);
    out
}

/// Render a `timestamptz` (microseconds since the Unix epoch, UTC) the same way
/// as a `timestamp`, then append the zone offset. Stele stores every instant
/// UTC-internal and carries no session time zone, so the offset is always `+00`
/// — matching what Postgres prints for a `timestamptz` read back with `TimeZone`
/// set to `UTC` (STL-189).
///
/// The offset goes *before* any era suffix: Postgres renders a pre-AD instant as
/// `0001-01-01 00:00:00+00 BC`, with ` BC` last. `format_timestamp` already
/// appends ` BC` for proleptic years ≤ 0, so it is split back off before the
/// offset is inserted.
fn format_timestamptz(micros: i64) -> String {
    let base = format_timestamp(micros);
    base.strip_suffix(" BC")
        .map_or_else(|| format!("{base}+00"), |head| format!("{head}+00 BC"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typlen_matches_pg_type() {
        assert_eq!(pg_typlen(LogicalType::Int4), 4);
        assert_eq!(pg_typlen(LogicalType::Int8), 8);
        assert_eq!(pg_typlen(LogicalType::Bool), 1);
        assert_eq!(pg_typlen(LogicalType::Date), 4);
        assert_eq!(pg_typlen(LogicalType::Timestamp), 8);
        assert_eq!(pg_typlen(LogicalType::TimestampTz), 8);
        assert_eq!(pg_typlen(LogicalType::Text), -1, "text is variable-length");
        assert_eq!(pg_typlen(LogicalType::Uuid), 16);
        assert_eq!(
            pg_typlen(LogicalType::Bytea),
            -1,
            "bytea is variable-length"
        );
        assert_eq!(
            pg_typlen(LogicalType::Period),
            -1,
            "tsrange is variable-length"
        );
    }

    #[test]
    fn uuid_renders_canonical_lowercase_hyphenated() {
        let bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            encode_text(&ScalarValue::Uuid(bytes)),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        // Upper-half bytes render with their leading zero preserved.
        assert_eq!(
            encode_text(&ScalarValue::Uuid([0; 16])),
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            encode_text(&ScalarValue::Uuid([0xFF; 16])),
            "ffffffff-ffff-ffff-ffff-ffffffffffff"
        );
    }

    #[test]
    fn bytea_renders_hex_with_backslash_x_prefix() {
        // Postgres's default `bytea_output = hex`.
        assert_eq!(encode_text(&ScalarValue::Bytea(vec![])), "\\x");
        assert_eq!(
            encode_text(&ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            "\\xdeadbeef"
        );
        assert_eq!(
            encode_text(&ScalarValue::Bytea(vec![0x00, 0x01, 0x0F, 0xA0])),
            "\\x00010fa0"
        );
    }

    #[test]
    fn period_renders_as_postgres_tsrange_text() {
        // 2023-11-14 22:13:20 UTC and one hour later — half-open `[lower,upper)`.
        let from = 1_700_000_000_000_000;
        let to = from + 3_600_000_000;
        assert_eq!(
            encode_text(&ScalarValue::Period(Interval::new(from, to).unwrap())),
            "[\"2023-11-14 22:13:20\",\"2023-11-14 23:13:20\")"
        );
        // An open-ended period leaves the upper slot empty, as psql prints it.
        assert_eq!(
            encode_text(&ScalarValue::Period(Interval::new(from, i64::MAX).unwrap())),
            "[\"2023-11-14 22:13:20\",)"
        );
    }

    #[test]
    fn integers_render_as_decimal() {
        assert_eq!(encode_text(&ScalarValue::Int4(0)), "0");
        assert_eq!(encode_text(&ScalarValue::Int4(-1)), "-1");
        assert_eq!(encode_text(&ScalarValue::Int4(i32::MIN)), "-2147483648");
        assert_eq!(
            encode_text(&ScalarValue::Int8(i64::MAX)),
            "9223372036854775807"
        );
    }

    #[test]
    fn text_renders_verbatim() {
        assert_eq!(encode_text(&ScalarValue::Text(String::new())), "");
        assert_eq!(
            encode_text(&ScalarValue::Text("héllo — 世界 🦀".into())),
            "héllo — 世界 🦀"
        );
    }

    #[test]
    fn bool_renders_as_t_or_f() {
        // Postgres prints `t`/`f`, never `true`/`false`.
        assert_eq!(encode_text(&ScalarValue::Bool(true)), "t");
        assert_eq!(encode_text(&ScalarValue::Bool(false)), "f");
    }

    #[test]
    fn date_renders_iso_8601() {
        assert_eq!(encode_text(&ScalarValue::Date(0)), "1970-01-01");
        // 2023-11-14 is 19675 days after the epoch.
        assert_eq!(encode_text(&ScalarValue::Date(19_675)), "2023-11-14");
        // The day before the epoch.
        assert_eq!(encode_text(&ScalarValue::Date(-1)), "1969-12-31");
    }

    #[test]
    fn date_handles_leap_day_and_bc() {
        // 2000 is a leap year (divisible by 400): 2000-02-29 must round-trip.
        // 2000-02-29 is 11016 days after the epoch.
        assert_eq!(encode_text(&ScalarValue::Date(11_016)), "2000-02-29");
        // Proleptic astronomical year 0 = `1 BC` in Postgres; 0000-01-01 is
        // 719528 days before the epoch.
        assert_eq!(encode_text(&ScalarValue::Date(-719_528)), "0001-01-01 BC");
    }

    #[test]
    fn timestamp_renders_iso_8601() {
        assert_eq!(
            encode_text(&ScalarValue::Timestamp(0)),
            "1970-01-01 00:00:00"
        );
        // 1_700_000_000 s = 2023-11-14 22:13:20 UTC, no fractional part.
        assert_eq!(
            encode_text(&ScalarValue::Timestamp(1_700_000_000_000_000)),
            "2023-11-14 22:13:20"
        );
    }

    #[test]
    fn timestamp_fractional_seconds_trim_trailing_zeros() {
        let base = 1_700_000_000_000_000;
        assert_eq!(
            encode_text(&ScalarValue::Timestamp(base + 123_456)),
            "2023-11-14 22:13:20.123456"
        );
        // .120000 → .12
        assert_eq!(
            encode_text(&ScalarValue::Timestamp(base + 120_000)),
            "2023-11-14 22:13:20.12"
        );
        // .000001 keeps its leading zeros.
        assert_eq!(
            encode_text(&ScalarValue::Timestamp(base + 1)),
            "2023-11-14 22:13:20.000001"
        );
    }

    #[test]
    fn timestamptz_renders_with_a_utc_offset_suffix() {
        // Same instant as the bare timestamp, plus the `+00` UTC offset Stele
        // always emits (it is UTC-internal, with no session zone to localize to).
        assert_eq!(
            encode_text(&ScalarValue::TimestampTz(0)),
            "1970-01-01 00:00:00+00"
        );
        assert_eq!(
            encode_text(&ScalarValue::TimestampTz(1_700_000_000_000_000)),
            "2023-11-14 22:13:20+00"
        );
        // Fractional seconds still trim, with the offset after them.
        assert_eq!(
            encode_text(&ScalarValue::TimestampTz(1_700_000_000_120_000)),
            "2023-11-14 22:13:20.12+00"
        );
    }

    #[test]
    fn timestamptz_offset_precedes_the_bc_era_suffix() {
        // Proleptic astronomical year 0 = `1 BC`; 0001-01-01 BC at 00:00:00 UTC.
        // Postgres orders the offset before the era: `... 00:00:00+00 BC`.
        let micros = -719_528 * MICROS_PER_DAY;
        assert_eq!(
            encode_text(&ScalarValue::TimestampTz(micros)),
            "0001-01-01 00:00:00+00 BC"
        );
    }

    #[test]
    fn timestamp_before_epoch_floors_into_previous_day() {
        // -1µs is the last microsecond of 1969-12-31, not a negative time-of-day.
        assert_eq!(
            encode_text(&ScalarValue::Timestamp(-1)),
            "1969-12-31 23:59:59.999999"
        );
    }

    /// civil_from_days is the inverse of the standard `days_from_civil`; check a
    /// dense range round-trips so the calendar math is exact across eras, not
    /// just at the hand-picked goldens above.
    #[test]
    fn civil_from_days_round_trips_against_days_from_civil() {
        // Howard Hinnant's `days_from_civil` — the inverse we validate against.
        fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
            let y = y - i64::from(m <= 2);
            let era = if y >= 0 { y } else { y - 399 } / 400;
            let yoe = y - era * 400;
            let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
            let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
            era * 146_097 + doe - 719_468
        }
        for days in (-800_000..=800_000).step_by(37) {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(
                days_from_civil(y, m, d),
                days,
                "round-trip failed at day {days} -> {y:04}-{m:02}-{d:02}"
            );
        }
    }
}
