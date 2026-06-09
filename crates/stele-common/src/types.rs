//! The v0.1 logical type system: the minimal scalar + temporal set the early
//! demos need, with the metadata that lets values round-trip cleanly through
//! the Postgres wire protocol ([STL-96]).
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
const OID_DATE: PgOid = 1082;
const OID_TIMESTAMP: PgOid = 1114;
const OID_UUID: PgOid = 2950;

/// The closed set of logical column types Stele understands.
///
/// Started deliberately minimal — the six types the identity-proof demos need
/// and that every Postgres driver can already decode ([STL-96] scope). The set
/// grows additively: each new type is a new variant plus its OID, with no churn
/// to the existing ones ([STL-181] added `UUID` and `BYTEA`). Numeric breadth
/// (`NUMERIC`, `FLOAT8`) and `timestamptz` are still later additions.
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
    /// interprets every such value as UTC, and the time-zone-aware `timestamptz`
    /// (OID 1184) is a deliberate later addition rather than a silent
    /// re-labelling ([assumption A9](../../../docs/assumptions.md) — document the
    /// choice where SQL:2011 and Postgres conventions diverge).
    Timestamp,
    /// Calendar date with no time component — SQL `DATE`, Postgres `date`. The
    /// value is days since the Unix epoch ([`ScalarValue::Date`]).
    Date,
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
}

impl LogicalType {
    /// Every logical type, in a stable order. The single source of truth tests
    /// and exhaustive consumers iterate so a new variant can't be silently
    /// missed.
    pub const ALL: [Self; 8] = [
        Self::Int4,
        Self::Int8,
        Self::Text,
        Self::Bool,
        Self::Timestamp,
        Self::Date,
        Self::Uuid,
        Self::Bytea,
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
            Self::Date => OID_DATE,
            Self::Uuid => OID_UUID,
            Self::Bytea => OID_BYTEA,
        }
    }

    /// The logical type a Postgres OID denotes, or `None` if it is outside the
    /// v0.1 set. Inverse of [`Self::pg_oid`].
    #[must_use]
    pub const fn from_pg_oid(oid: PgOid) -> Option<Self> {
        match oid {
            OID_INT4 => Some(Self::Int4),
            OID_INT8 => Some(Self::Int8),
            OID_TEXT => Some(Self::Text),
            OID_BOOL => Some(Self::Bool),
            OID_TIMESTAMP => Some(Self::Timestamp),
            OID_DATE => Some(Self::Date),
            OID_UUID => Some(Self::Uuid),
            OID_BYTEA => Some(Self::Bytea),
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
            Self::Date => "date",
            Self::Uuid => "uuid",
            Self::Bytea => "bytea",
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
    /// * [`ColumnCodec::Delta`] for [`Self::Timestamp`] and [`Self::Date`] — temporal
    ///   columns trend monotonic, so successive deltas are tiny.
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
            Self::Timestamp | Self::Date => ColumnCodec::Delta,
            Self::Int4 | Self::Int8 | Self::Bool | Self::Uuid | Self::Bytea => ColumnCodec::Plain,
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
    /// A date in days since the Unix epoch ([`LogicalType::Date`]).
    Date(i32),
    /// A 128-bit UUID, the 16 raw bytes in network order ([`LogicalType::Uuid`]).
    Uuid([u8; 16]),
    /// A variable-length byte string ([`LogicalType::Bytea`]).
    Bytea(Vec<u8>),
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
            Self::Date(_) => LogicalType::Date,
            Self::Uuid(_) => LogicalType::Uuid,
            Self::Bytea(_) => LogicalType::Bytea,
        }
    }

    /// Append this value's canonical byte encoding to `out`.
    ///
    /// The encoding is **not self-describing**: it carries the value's bytes but
    /// not its type, because the type is always known from the column when these
    /// bytes are read back. Layout, all little-endian:
    ///
    /// * `Int4` / `Date` — 4 bytes.
    /// * `Int8` / `Timestamp` — 8 bytes.
    /// * `Bool` — 1 byte (`0` / `1`).
    /// * `Uuid` — the 16 raw bytes, network order.
    /// * `Text` / `Bytea` — the raw bytes; the surrounding column framing carries
    ///   the length.
    ///
    /// This is the value-level half of the ticket's round-trip contract;
    /// [`Self::decode`] is its exact inverse for the matching [`LogicalType`].
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            // i32 payloads (Int4, Date) and i64 payloads (Int8, Timestamp) share
            // a body each — the column's type, not the bytes, tells them apart on
            // the way back in [`Self::decode`].
            Self::Int4(v) | Self::Date(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::Int8(v) | Self::Timestamp(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::Text(s) => out.extend_from_slice(s.as_bytes()),
            Self::Bool(b) => out.push(u8::from(*b)),
            Self::Uuid(bytes) => out.extend_from_slice(bytes),
            Self::Bytea(bytes) => out.extend_from_slice(bytes),
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
            LogicalType::Date => Ok(Self::Date(i32::from_le_bytes(fixed(ty, bytes)?))),
            LogicalType::Bool => {
                let [b] = fixed::<1>(ty, bytes)?;
                Ok(Self::Bool(b != 0))
            }
            LogicalType::Text => std::str::from_utf8(bytes)
                .map(|s| Self::Text(s.to_owned()))
                .map_err(|_| DecodeError::Utf8),
            LogicalType::Uuid => Ok(Self::Uuid(fixed::<16>(ty, bytes)?)),
            LogicalType::Bytea => Ok(Self::Bytea(bytes.to_vec())),
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
        assert_eq!(LogicalType::Date.pg_oid(), 1082);
        assert_eq!(LogicalType::Uuid.pg_oid(), 2950);
        assert_eq!(LogicalType::Bytea.pg_oid(), 17);
    }

    #[test]
    fn type_set_grew_additively_without_disturbing_the_original_six() {
        // STL-181 added two variants; the v0.1 six keep their identity (OID +
        // name), so a wire client that knew only the original set is unaffected.
        assert_eq!(LogicalType::ALL.len(), 8);
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
        assert_eq!(LogicalType::Date.to_string(), "date");
    }

    #[test]
    fn default_codec_follows_the_documented_policy() {
        assert_eq!(LogicalType::Text.default_codec(), ColumnCodec::Dictionary);
        assert_eq!(LogicalType::Timestamp.default_codec(), ColumnCodec::Delta);
        assert_eq!(LogicalType::Date.default_codec(), ColumnCodec::Delta);
        assert_eq!(LogicalType::Int4.default_codec(), ColumnCodec::Plain);
        assert_eq!(LogicalType::Int8.default_codec(), ColumnCodec::Plain);
        assert_eq!(LogicalType::Bool.default_codec(), ColumnCodec::Plain);
    }

    #[test]
    fn logical_type_is_the_inverse_of_each_value() {
        let samples = [
            (ScalarValue::Int4(1), LogicalType::Int4),
            (ScalarValue::Int8(1), LogicalType::Int8),
            (ScalarValue::Text("x".into()), LogicalType::Text),
            (ScalarValue::Bool(true), LogicalType::Bool),
            (ScalarValue::Timestamp(1), LogicalType::Timestamp),
            (ScalarValue::Date(1), LogicalType::Date),
            (ScalarValue::Uuid([0; 16]), LogicalType::Uuid),
            (ScalarValue::Bytea(vec![1, 2, 3]), LogicalType::Bytea),
        ];
        for (value, ty) in samples {
            assert_eq!(value.logical_type(), ty);
        }
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
            ScalarValue::Date(i32::MIN),
            ScalarValue::Date(0),
            ScalarValue::Date(20_000),
            ScalarValue::Uuid([0; 16]),
            ScalarValue::Uuid([0xFF; 16]),
            ScalarValue::Uuid([
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ]),
            ScalarValue::Bytea(Vec::new()),
            ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ScalarValue::Bytea(vec![0; 32]),
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
