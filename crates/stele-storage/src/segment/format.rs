//! On-disk constants and shared layout helpers for the sealed segment format.
//!
//! See [`super`] for the file shape and rationale. This module exists to keep
//! magic bytes, fixed widths, and the column-id ↔ logical-type mapping in one
//! reviewable place — any future format change touches a single file.

/// Header magic — `"STLSEG\0\0"`, 8 bytes. Detects "this is not a Stele
/// segment" at file-open time before any further parsing.
pub(super) const HEADER_MAGIC: [u8; 8] = *b"STLSEG\0\0";

/// Trailer magic — `"STLSEGFT"`, 8 bytes. Sits at the very end of the file so
/// a reader knows "this file's tail is a Stele-segment trailer" without
/// first decoding the footer.
pub(super) const TRAILER_MAGIC: [u8; 8] = *b"STLSEGFT";

/// On-disk format version embedded in the header. Bump on any
/// backwards-incompatible layout change; readers refuse newer versions
/// outright.
pub(super) const FORMAT_VERSION: u16 = 1;

/// Header size in bytes — magic (8) + version (2) + flags (2) + reserved (4).
pub(super) const HEADER_LEN: usize = 16;

/// Trailer size in bytes — footer CRC (4) + footer length (4) + magic (8).
pub(super) const TRAILER_LEN: usize = 16;

/// Per-column-chunk header size in bytes — payload length (4) + value count
/// (4) + codec (1) + reserved (3) + CRC32C (4).
pub(super) const CHUNK_HEADER_LEN: usize = 16;

/// Logical schema id stored in the footer.
///
/// v0.1 has exactly one implicit schema — the four `Version` fields — so the
/// id is hard-coded. Once [STL-98] lands the versioned catalog, this becomes a
/// real schema reference resolved through the catalog at read time; the
/// footer field is wide enough already that no format change is needed.
pub(super) const SCHEMA_ID_IMPLICIT_VERSION: u32 = 0;

/// Column codecs the format can describe. v0.1 emits [`Codec::Plain`]; the
/// architecture-listed codecs (dict + bitpack, RLE, delta, FOR — see
/// [02 §3.2](../../../../../docs/02-architecture.md#32-on-disk-segment-format))
/// drop in as new variants without bumping [`FORMAT_VERSION`], because the
/// per-chunk codec tag is the dispatch point and unknown values are rejected
/// at read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Codec {
    /// Verbatim — the layout depends on the column's [`ColumnType`].
    Plain = 0,
}

impl Codec {
    pub(super) const fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Plain),
            _ => None,
        }
    }
}

/// Stable, format-level column identifiers.
///
/// One enum value per column the v0.1 schema describes. Numeric values are
/// frozen — they live in every sealed-segment footer — so additions go at the
/// end and the existing entries never renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum ColumnId {
    /// Opaque business-key bytes (variable-length).
    BusinessKey = 0,
    /// System-time `sys_from` (fixed `i64`, microseconds).
    SysFrom = 1,
    /// System-time `sys_to` (fixed `i64`, microseconds). `i64::MAX` for the
    /// open sentinel — see [`stele_common::time::SYSTEM_TIME_OPEN`].
    SysTo = 2,
    /// Opaque payload bytes (variable-length).
    Payload = 3,
}

impl ColumnId {
    /// Every column the implicit-Version schema carries, in writer/reader
    /// canonical order. Exposed publicly so tests and other consumers share
    /// a single source of truth for the column set — adding a column here
    /// flows into both writer/reader and every test that iterates the
    /// schema, no shadow constants left to drift.
    pub const ALL: [Self; 4] = [Self::BusinessKey, Self::SysFrom, Self::SysTo, Self::Payload];

    pub(super) const fn ty(self) -> ColumnType {
        match self {
            Self::BusinessKey | Self::Payload => ColumnType::Bytes,
            Self::SysFrom | Self::SysTo => ColumnType::I64,
        }
    }

    pub(super) const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::BusinessKey),
            1 => Some(Self::SysFrom),
            2 => Some(Self::SysTo),
            3 => Some(Self::Payload),
            _ => None,
        }
    }

    pub(super) const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// The wire-level type the codec sees, derived from [`ColumnId::ty`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnType {
    /// Variable-length opaque bytes. Plain layout: `[u32 len][bytes]` repeated.
    Bytes,
    /// Fixed-width signed 64-bit integer. Plain layout: 8 LE bytes per value.
    I64,
}
