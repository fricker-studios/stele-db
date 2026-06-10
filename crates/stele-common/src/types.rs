//! Stele's logical type system: the scalar + temporal set the engine speaks,
//! with the metadata that lets values round-trip cleanly through the Postgres
//! wire protocol.
//!
//! It began as the minimal v0.1 set the early demos need ([STL-96]) and grows
//! additively — the v0.2 `UUID` / `BYTEA` types ([STL-181]) are the current
//! additions.
//!
//! Three things live here, and only here:
//!
//! 1. [`LogicalType`] — the closed set of logical column types (the v0.1 scalar
//!    set — `INT4`, `INT8`, `TEXT`, `BOOL`, `TIMESTAMP`, `DATE` — plus the v0.2
//!    additions `UUID` and `BYTEA`), each carrying its **Postgres OID**
//!    ([`LogicalType::pg_oid`]) so a stock driver can interpret the column, and
//!    its **default codec choice** ([`LogicalType::default_codec`]) so the
//!    writer knows how to physically encode it.
//! 2. [`ScalarValue`] — the in-memory value of each type, with a canonical,
//!    self-describing-given-the-type byte encoding ([`ScalarValue::encode`] /
//!    [`ScalarValue::decode`]). This is the "stored exactly, read back exactly"
//!    contract the ticket's round-trip Definition of Done rests on.
//! 3. [`ColumnCodec`] — the *choice* of physical encoding for a column. The on-disk
//!    codec tag in the segment writer is a separate, storage-private concern;
//!    this enum is the planner-facing policy ([architecture §3.2](../../../docs/02-architecture.md#32-on-disk-segment-format)).
//!
//! ## What this is *not*
//!
//! * **Not** the pg-wire serialization. Turning a [`ScalarValue`] into the bytes
//!   a `RowDescription` / `DataRow` carries (text format for v0.1) is the wire
//!   front end's job ([STL-105]); this module only fixes the *types* and their
//!   OIDs that encoding is driven by.
//! * **Not** nullability. A SQL `NULL` is modeled one level up as
//!   `Option<ScalarValue>` at the column/cell — keeping NULL out of the value
//!   enum means [`ScalarValue::logical_type`] is total, never ambiguous.
//! * **Not** the storage codec implementations. v0.1 storage emits only the
//!   plain layout; [`ColumnCodec::Dictionary`] / [`ColumnCodec::Delta`] are the *intended*
//!   choice per type and are honored as those codecs land in the segment writer
//!   (the format already dispatches on a per-chunk codec tag, so they drop in
//!   without an on-disk format bump).

use std::fmt;

use crate::period::{Interval, IntervalError};

/// A Postgres type OID, as it appears in a `RowDescription` field and in the
/// `pg_type` catalog. `u32` matches libpq's `Oid`.
pub type PgOid = u32;

// Well-known Postgres type OIDs. These are frozen in `pg_catalog.pg_type` and
// every Postgres driver hard-codes them, so they are safe to treat as
// constants rather than resolve at runtime.
const OID_BOOL: PgOid = 16;
const OID_BYTEA: PgOid = 17;
const OID_INT8: PgOid = 20;
const OID_INT4: PgOid = 23;
const OID_TEXT: PgOid = 25;
const OID_FLOAT8: PgOid = 701;
const OID_DATE: PgOid = 1082;
const OID_TIMESTAMP: PgOid = 1114;
const OID_TIMESTAMPTZ: PgOid = 1184;
const OID_UUID: PgOid = 2950;
const OID_TSRANGE: PgOid = 3908;

/// The closed set of logical column types Stele understands.
///
/// Started deliberately minimal — the six scalar/temporal types the
/// identity-proof demos need and that every Postgres driver can already decode
/// ([STL-96] scope). The set grows additively: each new type is a new variant
/// plus its OID, with no churn to the existing ones — v0.2 added the
/// time-zone-aware [`Self::TimestampTz`] ([STL-189]), [`Self::Period`]
/// ([STL-180]), `UUID` / `BYTEA` ([STL-181]), and the fractional
/// [`Self::Float8`] that `AVG` returns ([STL-209]). Arbitrary-precision
/// `NUMERIC` comes later.
///
/// ```
/// use stele_common::types::LogicalType;
///
/// // Every type maps to the Postgres OID a stock driver expects.
/// assert_eq!(LogicalType::Int4.pg_oid(), 23);
/// assert_eq!(LogicalType::Text.pg_oid(), 25);
/// // …and round-trips back from it.
/// assert_eq!(LogicalType::from_pg_oid(23), Some(LogicalType::Int4));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalType {
    /// 32-bit signed integer — SQL `INT` / `INTEGER`, Postgres `int4`.
    Int4,
    /// 64-bit signed integer — SQL `BIGINT`, Postgres `int8`.
    Int8,
    /// Variable-length UTF-8 text — SQL `TEXT`, Postgres `text`.
    Text,
    /// Boolean — SQL `BOOLEAN`, Postgres `bool`.
    Bool,
    /// Microsecond-precision instant, **anchored to UTC** — SQL `TIMESTAMP`,
    /// Postgres `timestamp`.
    ///
    /// The value is microseconds since the Unix epoch ([`ScalarValue::Timestamp`]),
    /// the same epoch and precision the bitemporal core uses on disk
    /// ([`crate::time::SystemTimeMicros`]). We map it to the Postgres `timestamp`
    /// OID (1114, *without* time zone) to match the bare SQL type name; Stele
    /// interprets every such value as UTC, and the time-zone-aware
    /// [`Self::TimestampTz`] (OID 1184) is the explicit zone-carrying counterpart
    /// ([assumption A9](../../../docs/assumptions.md) — document the choice where
    /// SQL:2011 and Postgres conventions diverge).
    Timestamp,
    /// Time-zone-aware microsecond instant, **stored UTC-internal** — SQL
    /// `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ`, Postgres `timestamptz`
    /// (OID 1184).
    ///
    /// The stored value is the same microseconds-since-the-Unix-epoch UTC instant
    /// as [`Self::Timestamp`] ([`ScalarValue::TimestampTz`]); the difference is at
    /// the edges of the system. On input a literal's zone offset is normalized
    /// away to UTC, and on output the instant renders with a `+00` offset — so two
    /// literals naming the same instant in different zones store identically
    /// ([STL-189], [ADR-0024](../../../docs/adr/0024-time-representation.md);
    /// parsing in [`crate::datetime`]).
    TimestampTz,
    /// Calendar date with no time component — SQL `DATE`, Postgres `date`. The
    /// value is days since the Unix epoch ([`ScalarValue::Date`]).
    Date,
    /// A half-open `[from, to)` period of [`Self::Timestamp`] instants — the
    /// first-class type backing system/valid time, instead of two loose `int8`
    /// columns ([STL-180]).
    ///
    /// It maps to Postgres `tsrange` (OID 3908), a range of `timestamp without
    /// time zone` whose default `[)` bound flavor is exactly Stele's half-open
    /// rule — so a stock driver decodes it with no custom type support. The
    /// value is a [`ScalarValue::Period`] wrapping an [`Interval`]; the upper
    /// bound may be `+∞` for an open-ended period. Generic user range types over
    /// other element types are a deliberate later addition, each its own OID.
    Period,
    /// A 128-bit universally-unique identifier — SQL `UUID`, Postgres `uuid`
    /// (OID 2950). The value is the 16 raw bytes in network order
    /// ([`ScalarValue::Uuid`]); the text form is the canonical lowercase
    /// hyphenated rendering. Backs provenance identifiers ([STL-181]).
    Uuid,
    /// A variable-length byte string — SQL `BYTEA`, Postgres `bytea` (OID 17).
    /// The value is the raw bytes verbatim ([`ScalarValue::Bytea`]); the text
    /// form is Postgres's `\x`-prefixed lowercase-hex output. This is the
    /// hash-digest / opaque-blob type that backs business-key hash digests
    /// ([STL-181]).
    Bytea,
    /// IEEE-754 double-precision float — SQL `DOUBLE PRECISION` / `FLOAT8`,
    /// Postgres `float8` (OID 701). The fractional result type `AVG` returns
    /// over integer columns, instead of the truncated integer mean it produced
    /// before any fractional type existed ([STL-209]). The value is a
    /// [`ScalarValue::Float8`]; the text form is the shortest decimal that
    /// round-trips. v0.2 introduces it only as that aggregate result — there is
    /// no `float8` column, literal, or arithmetic yet (the evaluator's tracked
    /// follow-up, STL-207).
    Float8,
}

impl LogicalType {
    /// Every logical type, in a stable order. The single source of truth tests
    /// and exhaustive consumers iterate so a new variant can't be silently
    /// missed.
    pub const ALL: [Self; 11] = [
        Self::Int4,
        Self::Int8,
        Self::Text,
        Self::Bool,
        Self::Timestamp,
        Self::TimestampTz,
        Self::Date,
        Self::Period,
        Self::Uuid,
        Self::Bytea,
        Self::Float8,
    ];

    /// The Postgres type OID a wire client uses to interpret this type
    /// ([`RowDescription`](https://www.postgresql.org/docs/current/protocol-message-formats.html)
    /// field `dataTypeOID`). Stable, well-known `pg_type` values.
    #[must_use]
    pub const fn pg_oid(self) -> PgOid {
        match self {
            Self::Int4 => OID_INT4,
            Self::Int8 => OID_INT8,
            Self::Text => OID_TEXT,
            Self::Bool => OID_BOOL,
            Self::Timestamp => OID_TIMESTAMP,
            Self::TimestampTz => OID_TIMESTAMPTZ,
            Self::Date => OID_DATE,
            Self::Period => OID_TSRANGE,
            Self::Uuid => OID_UUID,
            Self::Bytea => OID_BYTEA,
            Self::Float8 => OID_FLOAT8,
        }
    }

    /// The logical type a Postgres OID denotes, or `None` if it is outside the
    /// set Stele understands. Inverse of [`Self::pg_oid`].
    #[must_use]
    pub const fn from_pg_oid(oid: PgOid) -> Option<Self> {
        match oid {
            OID_INT4 => Some(Self::Int4),
            OID_INT8 => Some(Self::Int8),
            OID_TEXT => Some(Self::Text),
            OID_BOOL => Some(Self::Bool),
            OID_TIMESTAMP => Some(Self::Timestamp),
            OID_TIMESTAMPTZ => Some(Self::TimestampTz),
            OID_DATE => Some(Self::Date),
            OID_TSRANGE => Some(Self::Period),
            OID_UUID => Some(Self::Uuid),
            OID_BYTEA => Some(Self::Bytea),
            OID_FLOAT8 => Some(Self::Float8),
            _ => None,
        }
    }

    /// The canonical lowercase Postgres type name (`int4`, `int8`, `text`,
    /// `bool`, `timestamp`, `date`) — what `pg_type.typname` holds and what a
    /// `RowDescription`-driven `\d` rendering shows.
    #[must_use]
    pub const fn pg_type_name(self) -> &'static str {
        match self {
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Text => "text",
            Self::Bool => "bool",
            Self::Timestamp => "timestamp",
            Self::TimestampTz => "timestamptz",
            Self::Date => "date",
            Self::Period => "tsrange",
            Self::Uuid => "uuid",
            Self::Bytea => "bytea",
            Self::Float8 => "float8",
        }
    }

    /// The default physical codec a column of this type should be written with.
    ///
    /// This is the per-column *choice* the ticket calls for, expressed as
    /// type-directed policy rather than baked into the writer:
    ///
    /// * [`ColumnCodec::Dictionary`] for [`Self::Text`] — string columns are usually
    ///   low-cardinality (statuses, enums, country codes), where a dictionary
    ///   collapses repeats hard.
    /// * [`ColumnCodec::Delta`] for [`Self::Timestamp`], [`Self::TimestampTz`] and
    ///   [`Self::Date`] — temporal columns trend monotonic, so successive deltas
    ///   are tiny.
    /// * [`ColumnCodec::Plain`] for the fixed-width numerics and `bool`, which a
    ///   general codec rarely beats at v0.1 scale, and for the high-entropy
    ///   `uuid` / `bytea` (hash digests, opaque blobs), which neither a dictionary
    ///   nor a delta can compress.
    ///
    /// The choice is advisory: storage may fall back to [`ColumnCodec::Plain`] for any
    /// column until the richer codecs land in the segment writer, since the
    /// per-chunk codec tag is the dispatch point ([architecture §3.2](../../../docs/02-architecture.md#32-on-disk-segment-format)).
    #[must_use]
    pub const fn default_codec(self) -> ColumnCodec {
        match self {
            Self::Text => ColumnCodec::Dictionary,
            Self::Timestamp | Self::TimestampTz | Self::Date => ColumnCodec::Delta,
            Self::Int4
            | Self::Int8
            | Self::Bool
            | Self::Period
            | Self::Uuid
            | Self::Bytea
            | Self::Float8 => ColumnCodec::Plain,
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.pg_type_name())
    }
}

/// The physical encoding chosen for a column's values.
///
/// A planner-facing policy enum, distinct from the storage-private on-disk codec
/// tag: this names the *intent* ("dictionary-encode this column"), the segment
/// writer owns the byte layout. v0.1 storage realizes only [`Self::Plain`]; the
/// other variants are the chosen default for their types ([`LogicalType::default_codec`])
/// and become effective as the writer grows to honor them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnCodec {
    /// Verbatim, fixed- or length-prefixed layout. The universal fallback.
    Plain,
    /// Dictionary + small codes — wins on low-cardinality columns (most `TEXT`).
    Dictionary,
    /// Store successive differences — wins on monotonic columns (timestamps,
    /// dates, ascending keys).
    Delta,
}

/// A single typed, non-null value.
///
/// One variant per [`LogicalType`]; [`Self::logical_type`] is the total mapping
/// back. NULL is *not* a variant — model a nullable cell as
/// `Option<ScalarValue>` so that "what type is this value?" always has an
/// answer.
///
/// ```
/// use stele_common::types::{LogicalType, ScalarValue};
///
/// let v = ScalarValue::Int8(42);
/// assert_eq!(v.logical_type(), LogicalType::Int8);
///
/// // Values round-trip exactly through the canonical encoding.
/// let mut buf = Vec::new();
/// v.encode(&mut buf);
/// assert_eq!(ScalarValue::decode(LogicalType::Int8, &buf), Ok(v));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    /// A 32-bit integer ([`LogicalType::Int4`]).
    Int4(i32),
    /// A 64-bit integer ([`LogicalType::Int8`]).
    Int8(i64),
    /// UTF-8 text ([`LogicalType::Text`]).
    Text(String),
    /// A boolean ([`LogicalType::Bool`]).
    Bool(bool),
    /// A UTC instant in microseconds since the Unix epoch
    /// ([`LogicalType::Timestamp`]).
    Timestamp(i64),
    /// A time-zone-aware UTC instant in microseconds since the Unix epoch
    /// ([`LogicalType::TimestampTz`]). Stored identically to [`Self::Timestamp`]
    /// — the zone offset was normalized away on input; only the wire rendering
    /// (a `+00` suffix) and the advertised OID differ.
    TimestampTz(i64),
    /// A date in days since the Unix epoch ([`LogicalType::Date`]).
    Date(i32),
    /// A half-open `[from, to)` period of timestamp microseconds
    /// ([`LogicalType::Period`]). The wrapped [`Interval`] is well-formed by
    /// construction (`from < to`); the upper bound may be `i64::MAX` for an
    /// open-ended period. This is the value the SQL:2011 period predicates
    /// ([`crate::period::PeriodPredicate`]) range over.
    Period(Interval),
    /// A 128-bit UUID, the 16 raw bytes in network order ([`LogicalType::Uuid`]).
    Uuid([u8; 16]),
    /// A variable-length byte string ([`LogicalType::Bytea`]).
    Bytea(Vec<u8>),
    /// An IEEE-754 double, held as its **bit pattern** ([`LogicalType::Float8`]).
    ///
    /// The payload is `f64::to_bits` of the value, not the `f64` itself: a raw
    /// `f64` is not `Eq` / `Ord` / `Hash`, which the whole [`ScalarValue`] /
    /// [`Vector`](../../stele_exec/expr/enum.Vector.html) set derives, so the
    /// bits are carried instead and compared bitwise (two values are equal iff
    /// their encodings are). Construct with [`Self::float8`] and read with
    /// [`Self::as_f64`] rather than touching the bits directly. The result type
    /// of `AVG` ([STL-209]).
    Float8(u64),
}

/// Why decoding a [`ScalarValue`] from bytes failed.
///
/// Decoding is driven by a [`LogicalType`] the caller already knows (from the
/// catalog / `RowDescription`), so the only failures are a byte buffer that does
/// not match that type's fixed width or is not valid UTF-8 — both of which mean
/// the input is corrupt, not merely unexpected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// A fixed-width type's buffer was not exactly the expected length.
    #[error("decoding {ty}: expected {expected} bytes, got {got}")]
    Width {
        /// The type being decoded.
        ty: LogicalType,
        /// The exact byte count that type requires.
        expected: usize,
        /// The byte count actually supplied.
        got: usize,
    },
    /// A [`LogicalType::Text`] buffer was not valid UTF-8.
    #[error("decoding text: invalid utf-8")]
    Utf8,
    /// A [`LogicalType::Period`] buffer held bounds that are not a valid
    /// half-open interval (`from >= to`).
    #[error("decoding period: {0}")]
    Period(#[from] IntervalError),
}

impl ScalarValue {
    /// The logical type of this value. Total — every value has exactly one type.
    #[must_use]
    pub const fn logical_type(&self) -> LogicalType {
        match self {
            Self::Int4(_) => LogicalType::Int4,
            Self::Int8(_) => LogicalType::Int8,
            Self::Text(_) => LogicalType::Text,
            Self::Bool(_) => LogicalType::Bool,
            Self::Timestamp(_) => LogicalType::Timestamp,
            Self::TimestampTz(_) => LogicalType::TimestampTz,
            Self::Date(_) => LogicalType::Date,
            Self::Period(_) => LogicalType::Period,
            Self::Uuid(_) => LogicalType::Uuid,
            Self::Bytea(_) => LogicalType::Bytea,
            Self::Float8(_) => LogicalType::Float8,
        }
    }

    /// A [`Self::Float8`] from an `f64`, storing its IEEE-754 bit pattern.
    ///
    /// The constructor to use instead of `Self::Float8(bits)` directly — it keeps
    /// the bits-not-`f64` representation an implementation detail. [`Self::as_f64`]
    /// is the inverse.
    #[must_use]
    pub const fn float8(value: f64) -> Self {
        Self::Float8(value.to_bits())
    }

    /// The `f64` a [`Self::Float8`] carries (decoding its stored bits), or `None`
    /// for any other type. Inverse of [`Self::float8`].
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float8(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    /// The half-open [`Interval`] a [`Self::Period`] value carries, or `None` for
    /// any other type.
    ///
    /// The bridge from a stored PERIOD value to the SQL:2011 period predicates:
    /// `stele_exec::evaluate` ranges over [`Interval`]s, and a `Period` value
    /// hands one straight over.
    #[must_use]
    pub const fn as_period(&self) -> Option<Interval> {
        match self {
            Self::Period(iv) => Some(*iv),
            _ => None,
        }
    }

    /// Append this value's canonical byte encoding to `out`.
    ///
    /// The encoding is **not self-describing**: it carries the value's bytes but
    /// not its type, because the type is always known from the column when these
    /// bytes are read back. Layout, all little-endian:
    ///
    /// * `Int4` / `Date` — 4 bytes.
    /// * `Int8` / `Timestamp` / `TimestampTz` — 8 bytes.
    /// * `Float8` — 8 bytes: the IEEE-754 bit pattern, little-endian.
    /// * `Bool` — 1 byte (`0` / `1`).
    /// * `Uuid` — the 16 raw bytes, network order.
    /// * `Text` / `Bytea` — the raw bytes; the surrounding column framing carries
    ///   the length.
    /// * `Period` — 16 bytes: the `from` bound then the `to` bound, each an
    ///   `i64`. (This canonical storage form is distinct from the Postgres
    ///   `tsrange` *wire* encoding in [`crate::period`].)
    ///
    /// This is the value-level half of the ticket's round-trip contract;
    /// [`Self::decode`] is its exact inverse for the matching [`LogicalType`].
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            // i32 payloads (Int4, Date) and i64 payloads (Int8, Timestamp) share
            // a body each — the column's type, not the bytes, tells them apart on
            // the way back in [`Self::decode`].
            Self::Int4(v) | Self::Date(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::Int8(v) | Self::Timestamp(v) | Self::TimestampTz(v) => {
                out.extend_from_slice(&v.to_le_bytes());
            }
            Self::Text(s) => out.extend_from_slice(s.as_bytes()),
            Self::Bool(b) => out.push(u8::from(*b)),
            Self::Period(iv) => {
                out.extend_from_slice(&iv.from.to_le_bytes());
                out.extend_from_slice(&iv.to.to_le_bytes());
            }
            Self::Uuid(bytes) => out.extend_from_slice(bytes),
            Self::Bytea(bytes) => out.extend_from_slice(bytes),
            // The stored `u64` is already `f64::to_bits`, so its little-endian
            // bytes are exactly the IEEE-754 form.
            Self::Float8(bits) => out.extend_from_slice(&bits.to_le_bytes()),
        }
    }

    /// Decode the bytes [`Self::encode`] produced, given the column's
    /// [`LogicalType`].
    ///
    /// # Errors
    ///
    /// [`DecodeError::Width`] if a fixed-width type's buffer is the wrong length,
    /// or [`DecodeError::Utf8`] if a [`LogicalType::Text`] buffer is not valid
    /// UTF-8.
    pub fn decode(ty: LogicalType, bytes: &[u8]) -> Result<Self, DecodeError> {
        match ty {
            LogicalType::Int4 => Ok(Self::Int4(i32::from_le_bytes(fixed(ty, bytes)?))),
            LogicalType::Int8 => Ok(Self::Int8(i64::from_le_bytes(fixed(ty, bytes)?))),
            LogicalType::Timestamp => Ok(Self::Timestamp(i64::from_le_bytes(fixed(ty, bytes)?))),
            LogicalType::TimestampTz => {
                Ok(Self::TimestampTz(i64::from_le_bytes(fixed(ty, bytes)?)))
            }
            LogicalType::Date => Ok(Self::Date(i32::from_le_bytes(fixed(ty, bytes)?))),
            LogicalType::Bool => {
                let [b] = fixed::<1>(ty, bytes)?;
                Ok(Self::Bool(b != 0))
            }
            LogicalType::Text => std::str::from_utf8(bytes)
                .map(|s| Self::Text(s.to_owned()))
                .map_err(|_| DecodeError::Utf8),
            LogicalType::Period => {
                let raw = fixed::<16>(ty, bytes)?;
                let from = i64::from_le_bytes(raw[..8].try_into().unwrap());
                let to = i64::from_le_bytes(raw[8..].try_into().unwrap());
                Ok(Self::Period(Interval::new(from, to)?))
            }
            LogicalType::Uuid => Ok(Self::Uuid(fixed::<16>(ty, bytes)?)),
            LogicalType::Bytea => Ok(Self::Bytea(bytes.to_vec())),
            LogicalType::Float8 => Ok(Self::Float8(u64::from_le_bytes(fixed::<8>(ty, bytes)?))),
        }
    }
}

/// Read exactly `N` bytes for a fixed-width type, or report the width mismatch.
fn fixed<const N: usize>(ty: LogicalType, bytes: &[u8]) -> Result<[u8; N], DecodeError> {
    bytes.try_into().map_err(|_| DecodeError::Width {
        ty,
        expected: N,
        got: bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_oids_match_postgres_well_known_values() {
        assert_eq!(LogicalType::Int4.pg_oid(), 23);
        assert_eq!(LogicalType::Int8.pg_oid(), 20);
        assert_eq!(LogicalType::Text.pg_oid(), 25);
        assert_eq!(LogicalType::Bool.pg_oid(), 16);
        assert_eq!(LogicalType::Timestamp.pg_oid(), 1114);
        assert_eq!(LogicalType::TimestampTz.pg_oid(), 1184);
        assert_eq!(LogicalType::Date.pg_oid(), 1082);
        assert_eq!(LogicalType::Uuid.pg_oid(), 2950);
        assert_eq!(LogicalType::Bytea.pg_oid(), 17);
        assert_eq!(LogicalType::Float8.pg_oid(), 701);
        assert_eq!(LogicalType::Float8.pg_type_name(), "float8");
        // PERIOD borrows Postgres `tsrange` so a stock driver can decode it.
        assert_eq!(LogicalType::Period.pg_oid(), 3908);
        assert_eq!(LogicalType::Period.pg_type_name(), "tsrange");
    }

    #[test]
    fn type_set_grew_additively_without_disturbing_the_original_six() {
        // v0.2 added TIMESTAMPTZ (STL-189), PERIOD (STL-180), UUID/BYTEA
        // (STL-181), and FLOAT8 (STL-209); the original six keep their identity
        // (OID + name), so a wire client that knew only the original set is
        // unaffected.
        assert_eq!(LogicalType::ALL.len(), 11);
        for (ty, oid, name) in [
            (LogicalType::Int4, 23, "int4"),
            (LogicalType::Int8, 20, "int8"),
            (LogicalType::Text, 25, "text"),
            (LogicalType::Bool, 16, "bool"),
            (LogicalType::Timestamp, 1114, "timestamp"),
            (LogicalType::Date, 1082, "date"),
        ] {
            assert_eq!(ty.pg_oid(), oid, "{ty} OID drifted");
            assert_eq!(ty.pg_type_name(), name, "{ty} name drifted");
        }
    }

    #[test]
    fn period_value_carries_its_interval() {
        let iv = Interval::new(10, 20).unwrap();
        let v = ScalarValue::Period(iv);
        assert_eq!(v.logical_type(), LogicalType::Period);
        assert_eq!(v.as_period(), Some(iv));
        assert_eq!(ScalarValue::Int4(1).as_period(), None);
    }

    #[test]
    fn period_decode_rejects_reversed_bounds() {
        // 16 bytes whose `from` (5) is not < `to` (5): a corrupt period.
        let mut buf = Vec::new();
        buf.extend_from_slice(&5i64.to_le_bytes());
        buf.extend_from_slice(&5i64.to_le_bytes());
        assert_eq!(
            ScalarValue::decode(LogicalType::Period, &buf),
            Err(DecodeError::Period(IntervalError::EmptyOrReversed(5, 5)))
        );
        // A wrong-width period buffer is a plain width error.
        assert_eq!(
            ScalarValue::decode(LogicalType::Period, &[0; 8]),
            Err(DecodeError::Width {
                ty: LogicalType::Period,
                expected: 16,
                got: 8
            })
        );
    }

    #[test]
    fn pg_oid_round_trips_for_every_type() {
        for ty in LogicalType::ALL {
            assert_eq!(LogicalType::from_pg_oid(ty.pg_oid()), Some(ty));
        }
    }

    #[test]
    fn unknown_oid_is_not_a_v0_1_type() {
        // int2 (smallint, OID 21) is real Postgres but outside the v0.1 set.
        assert_eq!(LogicalType::from_pg_oid(21), None);
        assert_eq!(LogicalType::from_pg_oid(0), None);
    }

    #[test]
    fn type_names_are_the_postgres_names() {
        assert_eq!(LogicalType::Int4.pg_type_name(), "int4");
        assert_eq!(LogicalType::Timestamp.to_string(), "timestamp");
        assert_eq!(LogicalType::TimestampTz.to_string(), "timestamptz");
        assert_eq!(LogicalType::Date.to_string(), "date");
    }

    #[test]
    fn default_codec_follows_the_documented_policy() {
        assert_eq!(LogicalType::Text.default_codec(), ColumnCodec::Dictionary);
        assert_eq!(LogicalType::Timestamp.default_codec(), ColumnCodec::Delta);
        assert_eq!(LogicalType::TimestampTz.default_codec(), ColumnCodec::Delta);
        assert_eq!(LogicalType::Date.default_codec(), ColumnCodec::Delta);
        assert_eq!(LogicalType::Int4.default_codec(), ColumnCodec::Plain);
        assert_eq!(LogicalType::Int8.default_codec(), ColumnCodec::Plain);
        assert_eq!(LogicalType::Bool.default_codec(), ColumnCodec::Plain);
        assert_eq!(LogicalType::Period.default_codec(), ColumnCodec::Plain);
    }

    #[test]
    fn logical_type_is_the_inverse_of_each_value() {
        let samples = [
            (ScalarValue::Int4(1), LogicalType::Int4),
            (ScalarValue::Int8(1), LogicalType::Int8),
            (ScalarValue::Text("x".into()), LogicalType::Text),
            (ScalarValue::Bool(true), LogicalType::Bool),
            (ScalarValue::Timestamp(1), LogicalType::Timestamp),
            (ScalarValue::TimestampTz(1), LogicalType::TimestampTz),
            (ScalarValue::Date(1), LogicalType::Date),
            (
                ScalarValue::Period(Interval::new(1, 2).unwrap()),
                LogicalType::Period,
            ),
            (ScalarValue::Uuid([0; 16]), LogicalType::Uuid),
            (ScalarValue::Bytea(vec![1, 2, 3]), LogicalType::Bytea),
            (ScalarValue::float8(3.5), LogicalType::Float8),
        ];
        for (value, ty) in samples {
            assert_eq!(value.logical_type(), ty);
        }
    }

    #[test]
    fn float8_carries_its_f64_through_the_bit_pattern() {
        // The constructor stores `to_bits`; `as_f64` reads it back exactly.
        for v in [0.0, -0.0, 1.5, -2.5, f64::MIN, f64::MAX, f64::INFINITY] {
            assert_eq!(ScalarValue::float8(v).as_f64(), Some(v));
        }
        // NaN survives as the same bit pattern (compares equal to itself here,
        // unlike `f64` `==`, because the value identity is bitwise).
        let nan = ScalarValue::float8(f64::NAN);
        assert!(nan.as_f64().is_some_and(f64::is_nan));
        assert_eq!(nan, ScalarValue::float8(f64::NAN));
        assert_eq!(nan.logical_type(), LogicalType::Float8);
        // `as_f64` is float8-only.
        assert_eq!(ScalarValue::Int8(1).as_f64(), None);
    }

    /// The Definition-of-Done property at the value level: every type encodes
    /// and decodes back to exactly the same value, edge cases included.
    #[test]
    fn every_value_round_trips_through_encode_decode() {
        let cases = [
            ScalarValue::Int4(0),
            ScalarValue::Int4(i32::MIN),
            ScalarValue::Int4(i32::MAX),
            ScalarValue::Int8(0),
            ScalarValue::Int8(i64::MIN),
            ScalarValue::Int8(i64::MAX),
            ScalarValue::Text(String::new()),
            ScalarValue::Text("hello".into()),
            ScalarValue::Text("héllo — 世界 🦀".into()),
            ScalarValue::Bool(false),
            ScalarValue::Bool(true),
            ScalarValue::Timestamp(i64::MIN),
            ScalarValue::Timestamp(0),
            ScalarValue::Timestamp(1_700_000_000_000_000),
            ScalarValue::TimestampTz(i64::MIN),
            ScalarValue::TimestampTz(0),
            ScalarValue::TimestampTz(1_700_000_000_000_000),
            ScalarValue::Date(i32::MIN),
            ScalarValue::Date(0),
            ScalarValue::Date(20_000),
            ScalarValue::Period(Interval::new(0, 1).unwrap()),
            ScalarValue::Period(Interval::new(i64::MIN, i64::MAX).unwrap()),
            ScalarValue::Period(Interval::new(1_700_000_000_000_000, i64::MAX).unwrap()),
            ScalarValue::Uuid([0; 16]),
            ScalarValue::Uuid([0xFF; 16]),
            ScalarValue::Uuid([
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ]),
            ScalarValue::Bytea(Vec::new()),
            ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ScalarValue::Bytea(vec![0; 32]),
            ScalarValue::float8(0.0),
            ScalarValue::float8(-0.0),
            ScalarValue::float8(183.333_333_333_333_34),
            ScalarValue::float8(f64::MIN),
            ScalarValue::float8(f64::MAX),
            ScalarValue::float8(f64::INFINITY),
            ScalarValue::float8(f64::NEG_INFINITY),
            ScalarValue::float8(f64::NAN),
        ];
        for value in cases {
            let mut buf = Vec::new();
            value.encode(&mut buf);
            let decoded = ScalarValue::decode(value.logical_type(), &buf)
                .expect("decode of a freshly encoded value must succeed");
            assert_eq!(decoded, value, "round-trip changed the value");
        }
    }

    #[test]
    fn fixed_width_types_reject_wrong_length_buffers() {
        // An int4 needs exactly 4 bytes; 3 and 5 are both corrupt.
        assert_eq!(
            ScalarValue::decode(LogicalType::Int4, &[0, 0, 0]),
            Err(DecodeError::Width {
                ty: LogicalType::Int4,
                expected: 4,
                got: 3
            })
        );
        assert_eq!(
            ScalarValue::decode(LogicalType::Bool, &[]),
            Err(DecodeError::Width {
                ty: LogicalType::Bool,
                expected: 1,
                got: 0
            })
        );
        // A uuid is exactly 16 bytes; a 15-byte buffer is corrupt.
        assert_eq!(
            ScalarValue::decode(LogicalType::Uuid, &[0; 15]),
            Err(DecodeError::Width {
                ty: LogicalType::Uuid,
                expected: 16,
                got: 15
            })
        );
    }

    #[test]
    fn bytea_accepts_any_byte_length_including_empty() {
        // bytea is variable-length like text but with no UTF-8 constraint, so
        // arbitrary bytes (including 0xFF, never valid UTF-8) decode cleanly.
        for bytes in [Vec::new(), vec![0xFF], vec![0, 1, 2, 0xFF, 0xFE]] {
            assert_eq!(
                ScalarValue::decode(LogicalType::Bytea, &bytes),
                Ok(ScalarValue::Bytea(bytes.clone()))
            );
        }
    }

    #[test]
    fn text_decode_rejects_invalid_utf8() {
        // 0xFF is never a valid UTF-8 byte.
        assert_eq!(
            ScalarValue::decode(LogicalType::Text, &[0xFF]),
            Err(DecodeError::Utf8)
        );
    }

    #[test]
    fn bool_decodes_any_nonzero_as_true() {
        // Our encoder emits 0/1, but a decoder should treat any nonzero as true
        // rather than silently corrupt — mirrors Postgres's permissive bool input.
        assert_eq!(
            ScalarValue::decode(LogicalType::Bool, &[2]),
            Ok(ScalarValue::Bool(true))
        );
    }
}
