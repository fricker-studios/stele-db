//! Postgres **binary-format** value encoding + decoding for Stele's scalar set
//! ([STL-183]).
//!
//! The sibling [`text_format`](super::text_format) module renders a value as the
//! ASCII bytes a stock `psql` displays (format code `0`); this module is its
//! binary counterpart (format code `1`), the on-the-wire representation a driver
//! negotiates per column / parameter in the extended-query `Bind` message. The
//! two are interchangeable on the wire — a `RowDescription` field and a `Bind`
//! format code pick which one a given cell rides in — so this is a parallel
//! `encode` / `decode` pair, not a replacement.
//!
//! The contract is *bug-for-bug* Postgres `*_send` / `*_recv`, because the whole
//! point of the wire protocol is that a driver requesting binary results
//! (`tokio-postgres`, the JDBC driver, …) decodes a Stele value with its own
//! type codecs, byte-identically to a Postgres one:
//!
//! | type          | binary representation                                    |
//! |---------------|----------------------------------------------------------|
//! | `int4` / `date` | 4-byte big-endian                                      |
//! | `int8` / `timestamp` / `timestamptz` / `float8` | 8-byte big-endian      |
//! | `bool`        | one byte, `0` / `1`                                      |
//! | `text`        | the UTF-8 bytes verbatim                                 |
//! | `bytea`       | the raw bytes verbatim                                   |
//! | `uuid`        | the 16 bytes verbatim                                    |
//! | `tsrange`     | range wire format: a flags byte + length-prefixed bounds |
//!
//! ## Epoch
//!
//! Postgres's binary `timestamp` / `timestamptz` / `date` count from
//! **2000-01-01**, not the Unix epoch Stele stores internally
//! ([`stele_common::types`]). The fixed offset between the two epochs
//! ([`PG_EPOCH_UNIX_MICROS`] / [`PG_EPOCH_UNIX_DAYS`]) is applied on the way out
//! and reversed on the way in, so a value round-trips through a binary-aware
//! driver unchanged.
//!
//! NULL is *not* a [`ScalarValue`]; it rides the same length-`-1` `DataRow` /
//! `Bind` sentinel as in text format and never reaches this module.
//!
//! [STL-183]: https://allegromusic.atlassian.net/browse/STL-183

use stele_common::period::{Interval, IntervalError};
use stele_common::types::{LogicalType, ScalarValue};

/// Microseconds from the Unix epoch (1970-01-01) to the Postgres epoch
/// (2000-01-01): `10957` days × 86 400 s × 1 000 000 µs. Postgres binary
/// `timestamp` / `timestamptz` are micros relative to this instant.
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
/// Days from the Unix epoch to the Postgres epoch — the `date` counterpart of
/// [`PG_EPOCH_UNIX_MICROS`]. 2000-01-01 is 10 957 days after 1970-01-01.
const PG_EPOCH_UNIX_DAYS: i32 = 10_957;

// Range-type flag bits (`rangetypes.h`), the ones a half-open `[from, to)` Stele
// [`Interval`] can carry. Lower is always present and inclusive; upper is either
// present-and-exclusive or unbounded (`to == i64::MAX`).
/// The range is empty — Stele never produces one, and rejects it on decode.
const RANGE_EMPTY: u8 = 0x01;
/// The lower bound is inclusive.
const RANGE_LB_INC: u8 = 0x02;
/// The upper bound is inclusive.
const RANGE_UB_INC: u8 = 0x04;
/// The lower bound is unbounded (`-infinity`).
const RANGE_LB_INF: u8 = 0x08;
/// The upper bound is unbounded (`+infinity`) — Stele's open-ended period.
const RANGE_UB_INF: u8 = 0x10;

/// Why a binary-format wire value could not be decoded into a [`ScalarValue`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum BinaryError {
    /// A fixed-width type's payload was not exactly the expected number of bytes.
    #[error("binary {ty} value has wrong length: expected {expected} bytes, got {actual}")]
    WrongLength {
        ty: LogicalType,
        expected: usize,
        actual: usize,
    },
    /// A `text` value's bytes were not valid UTF-8.
    #[error("binary text value is not valid UTF-8")]
    NotUtf8,
    /// A `tsrange` value did not match the half-open `[from, to)` shape Stele's
    /// [`Interval`] models (empty range, an open lower bound, an inclusive upper
    /// bound, or a truncated frame).
    #[error("unsupported binary tsrange: {0}")]
    BadRange(&'static str),
    /// The decoded `tsrange` bounds were not a well-formed interval (`from >= to`).
    #[error(transparent)]
    Interval(#[from] IntervalError),
}

/// Encode a non-null [`ScalarValue`] in Postgres binary format — the exact byte
/// payload of the value's `DataRow` field when the column negotiated format `1`.
pub(crate) fn encode_binary(value: &ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Int4(v) => v.to_be_bytes().to_vec(),
        ScalarValue::Int8(v) => v.to_be_bytes().to_vec(),
        // The IEEE-754 double in network byte order — `to_be_bytes` is the same
        // bit pattern Postgres `float8send` emits, NaN / Infinity included.
        ScalarValue::Float8(bits) => f64::from_bits(*bits).to_be_bytes().to_vec(),
        ScalarValue::Bool(b) => vec![u8::from(*b)],
        ScalarValue::Text(s) => s.as_bytes().to_vec(),
        ScalarValue::Bytea(bytes) => bytes.clone(),
        ScalarValue::Uuid(bytes) => bytes.to_vec(),
        // Shift from the Unix epoch to the Postgres epoch before serializing.
        ScalarValue::Timestamp(micros) | ScalarValue::TimestampTz(micros) => {
            (micros - PG_EPOCH_UNIX_MICROS).to_be_bytes().to_vec()
        }
        ScalarValue::Date(days) => (days - PG_EPOCH_UNIX_DAYS).to_be_bytes().to_vec(),
        ScalarValue::Period(iv) => encode_tsrange(iv),
    }
}

/// Encode a half-open `[from, to)` [`Interval`] as a Postgres `tsrange` binary
/// value: a flags byte, then each present bound as an `Int32` length (`8`)
/// followed by its `timestamp` binary form. The lower bound is always present
/// and inclusive; an open-ended period (`to == i64::MAX`) sets the
/// upper-infinite flag and writes no upper bound.
fn encode_tsrange(iv: &Interval) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 2 * (4 + 8));
    let open = iv.to == i64::MAX;
    let flags = if open {
        RANGE_LB_INC | RANGE_UB_INF
    } else {
        RANGE_LB_INC
    };
    out.push(flags);
    push_bound(&mut out, iv.from);
    if !open {
        push_bound(&mut out, iv.to);
    }
    out
}

/// Append one range bound: an `Int32` length of `8` then the `timestamp` binary
/// form of `unix_micros` (shifted to the Postgres epoch).
fn push_bound(out: &mut Vec<u8>, unix_micros: i64) {
    out.extend_from_slice(&8i32.to_be_bytes());
    out.extend_from_slice(&(unix_micros - PG_EPOCH_UNIX_MICROS).to_be_bytes());
}

/// Decode a Postgres binary-format value of logical type `ty` into a
/// [`ScalarValue`] — the inverse of [`encode_binary`], used for binary-format
/// `Bind` parameters.
pub(crate) fn decode_binary(ty: LogicalType, bytes: &[u8]) -> Result<ScalarValue, BinaryError> {
    match ty {
        LogicalType::Int4 => Ok(ScalarValue::Int4(i32::from_be_bytes(fixed(ty, bytes)?))),
        LogicalType::Int8 => Ok(ScalarValue::Int8(i64::from_be_bytes(fixed(ty, bytes)?))),
        LogicalType::Float8 => Ok(ScalarValue::float8(f64::from_be_bytes(fixed(ty, bytes)?))),
        LogicalType::Bool => {
            let [b] = fixed::<1>(ty, bytes)?;
            Ok(ScalarValue::Bool(b != 0))
        }
        LogicalType::Text => std::str::from_utf8(bytes)
            .map(|s| ScalarValue::Text(s.to_owned()))
            .map_err(|_| BinaryError::NotUtf8),
        LogicalType::Bytea => Ok(ScalarValue::Bytea(bytes.to_vec())),
        LogicalType::Uuid => Ok(ScalarValue::Uuid(fixed(ty, bytes)?)),
        LogicalType::Timestamp => Ok(ScalarValue::Timestamp(
            i64::from_be_bytes(fixed(ty, bytes)?) + PG_EPOCH_UNIX_MICROS,
        )),
        LogicalType::TimestampTz => Ok(ScalarValue::TimestampTz(
            i64::from_be_bytes(fixed(ty, bytes)?) + PG_EPOCH_UNIX_MICROS,
        )),
        LogicalType::Date => Ok(ScalarValue::Date(
            i32::from_be_bytes(fixed(ty, bytes)?) + PG_EPOCH_UNIX_DAYS,
        )),
        LogicalType::Period => decode_tsrange(bytes).map(ScalarValue::Period),
    }
}

/// Read the exact `N` bytes a fixed-width type expects, or report the mismatch.
fn fixed<const N: usize>(ty: LogicalType, bytes: &[u8]) -> Result<[u8; N], BinaryError> {
    bytes.try_into().map_err(|_| BinaryError::WrongLength {
        ty,
        expected: N,
        actual: bytes.len(),
    })
}

/// Decode a Postgres `tsrange` binary value into a half-open [`Interval`]. Only
/// the canonical `[from, to)` shape Stele models is accepted — a lower-inclusive,
/// finite-or-unbounded-upper range; an empty range, an unbounded lower bound, or
/// an inclusive upper bound is rejected rather than silently coerced.
fn decode_tsrange(bytes: &[u8]) -> Result<Interval, BinaryError> {
    let (&flags, mut rest) = bytes
        .split_first()
        .ok_or(BinaryError::BadRange("empty payload"))?;
    if flags & RANGE_EMPTY != 0 {
        return Err(BinaryError::BadRange("empty range"));
    }
    if flags & RANGE_LB_INF != 0 {
        return Err(BinaryError::BadRange("unbounded lower bound"));
    }
    if flags & RANGE_LB_INC == 0 {
        return Err(BinaryError::BadRange("exclusive lower bound"));
    }
    if flags & RANGE_UB_INC != 0 {
        return Err(BinaryError::BadRange("inclusive upper bound"));
    }
    let from = take_bound(&mut rest)?;
    let to = if flags & RANGE_UB_INF != 0 {
        i64::MAX
    } else {
        take_bound(&mut rest)?
    };
    if !rest.is_empty() {
        return Err(BinaryError::BadRange("trailing bytes"));
    }
    Ok(Interval::new(from, to)?)
}

/// Consume one range bound from `rest`: an `Int32` length of `8` followed by the
/// `timestamp` binary form, returning Unix-epoch micros and advancing `rest`.
fn take_bound(rest: &mut &[u8]) -> Result<i64, BinaryError> {
    let (len_bytes, after_len) = split_n::<4>(rest)?;
    if i32::from_be_bytes(len_bytes) != 8 {
        return Err(BinaryError::BadRange("bound is not an 8-byte timestamp"));
    }
    let (micros_bytes, after_bound) = split_n::<8>(after_len)?;
    *rest = after_bound;
    Ok(i64::from_be_bytes(micros_bytes) + PG_EPOCH_UNIX_MICROS)
}

/// Split the first `N` bytes off a slice, or report a truncated range frame.
fn split_n<const N: usize>(bytes: &[u8]) -> Result<([u8; N], &[u8]), BinaryError> {
    let head = bytes
        .get(..N)
        .ok_or(BinaryError::BadRange("truncated bound"))?;
    Ok((head.try_into().expect("slice is N bytes"), &bytes[N..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode then decode must recover the original value for every type.
    fn assert_round_trips(value: &ScalarValue) {
        let bytes = encode_binary(value);
        let back = decode_binary(value.logical_type(), &bytes).expect("decode");
        assert_eq!(&back, value, "round-trip for {:?}", value.logical_type());
    }

    #[test]
    fn integers_are_big_endian() {
        assert_eq!(encode_binary(&ScalarValue::Int4(1)), vec![0, 0, 0, 1]);
        assert_eq!(
            encode_binary(&ScalarValue::Int4(-1)),
            vec![0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            encode_binary(&ScalarValue::Int8(1)),
            vec![0, 0, 0, 0, 0, 0, 0, 1]
        );
        for v in [0, 1, -1, i32::MIN, i32::MAX] {
            assert_round_trips(&ScalarValue::Int4(v));
        }
        for v in [0, 1, -1, i64::MIN, i64::MAX] {
            assert_round_trips(&ScalarValue::Int8(v));
        }
    }

    #[test]
    fn float8_is_ieee754_big_endian_including_non_finite() {
        // 1.5 == 0x3FF8000000000000.
        assert_eq!(
            encode_binary(&ScalarValue::float8(1.5)),
            vec![0x3F, 0xF8, 0, 0, 0, 0, 0, 0]
        );
        for v in [0.0, 1.5, -2.5, f64::MIN, f64::MAX, f64::INFINITY] {
            assert_round_trips(&ScalarValue::float8(v));
        }
        // NaN: the bit pattern survives the round-trip even though it is != itself.
        let bytes = encode_binary(&ScalarValue::float8(f64::NAN));
        let ScalarValue::Float8(bits) = decode_binary(LogicalType::Float8, &bytes).unwrap() else {
            panic!("float8");
        };
        assert!(f64::from_bits(bits).is_nan());
    }

    #[test]
    fn bool_is_one_byte() {
        assert_eq!(encode_binary(&ScalarValue::Bool(true)), vec![1]);
        assert_eq!(encode_binary(&ScalarValue::Bool(false)), vec![0]);
        assert_round_trips(&ScalarValue::Bool(true));
        assert_round_trips(&ScalarValue::Bool(false));
        // Postgres only ever sends 0/1, but any non-zero byte decodes as true.
        assert_eq!(
            decode_binary(LogicalType::Bool, &[2]),
            Ok(ScalarValue::Bool(true))
        );
    }

    #[test]
    fn text_and_bytea_are_verbatim() {
        assert_eq!(
            encode_binary(&ScalarValue::Text("hé🦀".into())),
            "hé🦀".as_bytes()
        );
        assert_round_trips(&ScalarValue::Text(String::new()));
        assert_round_trips(&ScalarValue::Text("héllo — 世界 🦀".into()));
        assert_eq!(
            encode_binary(&ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_round_trips(&ScalarValue::Bytea(vec![]));
        assert_round_trips(&ScalarValue::Bytea(vec![0, 1, 0xFF, 0x0F]));
        // Invalid UTF-8 is a clean decode error, not a panic.
        assert_eq!(
            decode_binary(LogicalType::Text, &[0xFF, 0xFE]),
            Err(BinaryError::NotUtf8)
        );
    }

    #[test]
    fn uuid_is_sixteen_bytes_verbatim() {
        let bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(encode_binary(&ScalarValue::Uuid(bytes)), bytes.to_vec());
        assert_round_trips(&ScalarValue::Uuid(bytes));
        assert_round_trips(&ScalarValue::Uuid([0; 16]));
    }

    #[test]
    fn timestamps_and_date_shift_to_the_postgres_epoch() {
        // The Postgres epoch itself encodes as all-zero micros / days.
        assert_eq!(
            encode_binary(&ScalarValue::Timestamp(PG_EPOCH_UNIX_MICROS)),
            vec![0; 8]
        );
        assert_eq!(
            encode_binary(&ScalarValue::Date(PG_EPOCH_UNIX_DAYS)),
            vec![0; 4]
        );
        // The Unix epoch is a negative offset from the Postgres epoch.
        assert_eq!(
            encode_binary(&ScalarValue::Timestamp(0)),
            (-PG_EPOCH_UNIX_MICROS).to_be_bytes().to_vec()
        );
        for micros in [0, 1, -1, PG_EPOCH_UNIX_MICROS, 1_700_000_000_000_000] {
            assert_round_trips(&ScalarValue::Timestamp(micros));
            assert_round_trips(&ScalarValue::TimestampTz(micros));
        }
        for days in [0, -1, PG_EPOCH_UNIX_DAYS, 19_675] {
            assert_round_trips(&ScalarValue::Date(days));
        }
    }

    #[test]
    fn fixed_width_decode_rejects_wrong_length() {
        assert_eq!(
            decode_binary(LogicalType::Int4, &[0, 0, 1]),
            Err(BinaryError::WrongLength {
                ty: LogicalType::Int4,
                expected: 4,
                actual: 3,
            })
        );
        assert_eq!(
            decode_binary(LogicalType::Uuid, &[0; 15]),
            Err(BinaryError::WrongLength {
                ty: LogicalType::Uuid,
                expected: 16,
                actual: 15,
            })
        );
    }

    #[test]
    fn tsrange_finite_round_trips() {
        let from = 1_700_000_000_000_000;
        let to = from + 3_600_000_000;
        let iv = Interval::new(from, to).unwrap();
        let bytes = encode_binary(&ScalarValue::Period(iv));
        // flags(0x02) + [len 8 + lower] + [len 8 + upper].
        assert_eq!(bytes[0], RANGE_LB_INC);
        assert_eq!(bytes.len(), 1 + 2 * (4 + 8));
        assert_round_trips(&ScalarValue::Period(iv));
    }

    #[test]
    fn tsrange_open_ended_sets_upper_infinite() {
        let from = 1_700_000_000_000_000;
        let iv = Interval::new(from, i64::MAX).unwrap();
        let bytes = encode_binary(&ScalarValue::Period(iv));
        // flags(0x12) + just the lower bound (no upper).
        assert_eq!(bytes[0], RANGE_LB_INC | RANGE_UB_INF);
        assert_eq!(bytes.len(), 1 + (4 + 8));
        assert_round_trips(&ScalarValue::Period(iv));
    }

    #[test]
    fn tsrange_decode_rejects_non_canonical_and_truncated() {
        // Empty range.
        assert_eq!(
            decode_tsrange(&[RANGE_EMPTY]),
            Err(BinaryError::BadRange("empty range"))
        );
        // Inclusive upper bound (`[a,b]`) is not the half-open form.
        let mut incl = vec![RANGE_LB_INC | RANGE_UB_INC];
        incl.extend_from_slice(&8i32.to_be_bytes());
        incl.extend_from_slice(&0i64.to_be_bytes());
        incl.extend_from_slice(&8i32.to_be_bytes());
        incl.extend_from_slice(&1i64.to_be_bytes());
        assert_eq!(
            decode_tsrange(&incl),
            Err(BinaryError::BadRange("inclusive upper bound"))
        );
        // Truncated: flags claim a lower bound but no bytes follow.
        assert_eq!(
            decode_tsrange(&[RANGE_LB_INC]),
            Err(BinaryError::BadRange("truncated bound"))
        );
    }

    /// Oracle: our binary bytes are byte-identical to what the `tokio-postgres`
    /// type codecs (`postgres-types` `ToSql`) produce, and our decoder recovers a
    /// value the driver encoded (`FromSql`) — for every type the driver supports
    /// natively. This pins the wire format to a real Postgres client, the
    /// STL-183 Definition of Done ("binary round-trip per type vs … tokio-postgres").
    #[test]
    fn matches_tokio_postgres_type_codecs() {
        use bytes::BytesMut;
        use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};

        /// `to_sql` into a fresh buffer, asserting the value is non-NULL.
        fn pg_encode<T: ToSql>(value: &T, ty: &Type) -> Vec<u8> {
            let mut buf = BytesMut::new();
            let is_null = value.to_sql(ty, &mut buf).expect("tokio-postgres to_sql");
            assert!(matches!(is_null, IsNull::No));
            buf.to_vec()
        }

        // int4 / int8 / float8 / bool: our bytes == the driver's, both directions.
        let i4 = ScalarValue::Int4(-12_345);
        assert_eq!(encode_binary(&i4), pg_encode(&-12_345i32, &Type::INT4));
        assert_eq!(
            decode_binary(LogicalType::Int4, &pg_encode(&-12_345i32, &Type::INT4)),
            Ok(i4)
        );

        let i8 = ScalarValue::Int8(9_000_000_000);
        assert_eq!(
            encode_binary(&i8),
            pg_encode(&9_000_000_000i64, &Type::INT8)
        );

        let f8 = ScalarValue::float8(550.0 / 3.0);
        assert_eq!(
            encode_binary(&f8),
            pg_encode(&(550.0f64 / 3.0), &Type::FLOAT8)
        );

        let b = ScalarValue::Bool(true);
        assert_eq!(encode_binary(&b), pg_encode(&true, &Type::BOOL));

        // text / bytea: verbatim, and the driver decodes our bytes back.
        let t = ScalarValue::Text("héllo 🦀".into());
        let t_bytes = encode_binary(&t);
        assert_eq!(t_bytes, pg_encode(&"héllo 🦀", &Type::TEXT));
        assert_eq!(
            <&str as FromSql>::from_sql(&Type::TEXT, &t_bytes).unwrap(),
            "héllo 🦀"
        );

        let by = ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let by_bytes = encode_binary(&by);
        assert_eq!(
            by_bytes,
            pg_encode(&[0xDEu8, 0xAD, 0xBE, 0xEF].as_slice(), &Type::BYTEA)
        );
        assert_eq!(
            <Vec<u8> as FromSql>::from_sql(&Type::BYTEA, &by_bytes).unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }
}
