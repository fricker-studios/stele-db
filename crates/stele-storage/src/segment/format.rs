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
///
/// * **v1** — the four-column implicit `Version` schema (business_key, sys_from,
///   sys_to, payload).
/// * **v2** — adds the three always-on provenance columns (`txn_id`,
///   `committed_at`, `principal`), ids 4..=6 ([STL-93]). This is a
///   backwards-incompatible layout change: a v1 reader encountering the new
///   column ids would fail with a confusing `Corrupt("unknown column id")`
///   mid-footer, so the version bump makes a v1 reader reject a v2 segment
///   cleanly at the header with [`SegmentError::UnsupportedVersion`](super::SegmentError::UnsupportedVersion)
///   instead.
/// * **v3** — adds the per-table opt-in valid-time pair (`valid_from`,
///   `valid_to`), ids 7..=8 ([STL-117]). Unlike provenance these are *not*
///   on every segment: only a valid-time table's segments carry them, lifted
///   from the payload's 16-byte prefix ([`crate::validtime`], [STL-92]) so the
///   planner can prune on the valid axis. The footer's column list is the
///   source of truth for which columns a given segment actually holds; the
///   version marks the generation, and bumping it makes a v2 reader reject a
///   valid-time segment cleanly at the header rather than choking on column
///   id 7 mid-footer.
pub(super) const FORMAT_VERSION: u16 = 3;

/// Header size in bytes — magic (8) + version (2) + flags (2) + reserved (4).
pub(super) const HEADER_LEN: usize = 16;

/// Trailer size in bytes — footer CRC (4) + footer length (4) + magic (8).
pub(super) const TRAILER_LEN: usize = 16;

/// Per-column-chunk header size in bytes — payload length (4) + value count
/// (4) + codec (1) + reserved (3) + CRC32C (4).
pub(super) const CHUNK_HEADER_LEN: usize = 16;

/// Maximum bytes retained for a variable-length column's zone-map min/max stat.
///
/// Bytes columns ([`ColumnType::Bytes`]) can hold values up to
/// `MAX_VERSION_FRAME_LEN` (16 MiB) each — `Payload`, `Principal`, and even a
/// pathologically long `BusinessKey`. Inlining a full lex-min/max of such a
/// value would let one row push the footer past its `u32` `footer_len` ceiling,
/// so the writer records only a bounded *prefix* of the lex-min/max instead
/// ([`super::writer`]): the min prefix is truncated *down* (a byte prefix is
/// lex-`<=` its source, so it stays a sound lower bound) and the max prefix is
/// rounded *up* (so it stays a sound upper bound). This caps each bytes
/// column's footer contribution at `2 * MAX_BYTES_STAT_PREFIX_LEN`, independent
/// of value size, which keeps [`ZoneMap::might_contain`](super::zone_map::ZoneMap::might_contain)'s
/// no-false-negatives contract intact for worst-case blob inputs.
///
/// 64 bytes trades footer size against prune selectivity: long enough that
/// realistic keys/prefixes still discriminate, small enough that the worst-case
/// footer stays tiny. It is purely a writer-side choice — the on-disk stat
/// field is length-prefixed, so changing it needs no [`FORMAT_VERSION`] bump.
pub(super) const MAX_BYTES_STAT_PREFIX_LEN: usize = 64;

/// Logical schema id stored in the footer.
///
/// v0.1 has exactly one implicit schema — the seven always-on `Version` columns
/// (the four data/temporal fields plus the three provenance columns,
/// [`FORMAT_VERSION`] v2), optionally extended with the valid-time pair for a
/// valid-time table ([`FORMAT_VERSION`] v3) — so the id is hard-coded. Once
/// [STL-98] lands the versioned catalog,
/// this becomes a real schema reference resolved through the catalog at read
/// time; the footer field is wide enough already that no format change is
/// needed.
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
    /// Provenance: writing transaction id (fixed 8 bytes). Stored in the `i64`
    /// column layout — `txn_id` is logically a `u64`
    /// ([`stele_common::provenance::TxnId`]); the writer reinterprets the bits
    /// (`u64 as i64`) and the reader reverses it (`i64 as u64`), a lossless
    /// round-trip. Only the zone-map ordering would differ for ids ≥ 2^63,
    /// unreachable for the same reason the system-time `i64` axis is.
    TxnId = 4,
    /// Provenance: commit timestamp (fixed `i64`, microseconds) —
    /// [`stele_common::provenance::Provenance::committed_at`].
    CommittedAt = 5,
    /// Provenance: opaque principal bytes (variable-length) —
    /// [`stele_common::provenance::Principal`].
    Principal = 6,
    /// Valid-time period start (fixed `i64`, microseconds) — the inclusive
    /// `valid_from` boundary, lifted at flush from the payload's valid-time
    /// prefix ([`crate::validtime`], [STL-92]). Present only on a valid-time
    /// table's segments (format v3); absent otherwise.
    ValidFrom = 7,
    /// Valid-time period end (fixed `i64`, microseconds) — the exclusive
    /// `valid_to` boundary; `i64::MAX` for an open-ended fact
    /// ([`stele_common::time::VALID_TIME_OPEN`]). Present only on a valid-time
    /// table's segments, alongside [`Self::ValidFrom`].
    ValidTo = 8,
}

impl ColumnId {
    /// Every column the implicit-Version schema carries, in writer/reader
    /// canonical order. Exposed publicly so tests and other consumers share
    /// a single source of truth for the column set — adding a column here
    /// flows into both writer/reader and every test that iterates the
    /// schema, no shadow constants left to drift.
    ///
    /// The three provenance columns ([`Self::TxnId`], [`Self::CommittedAt`],
    /// [`Self::Principal`]) sit at the end — additions never renumber the
    /// frozen ids 0..=3 ([architecture §3.2](../../../../../docs/02-architecture.md#32-on-disk-segment-format)).
    ///
    /// This is the **always-on** set, present on every segment. A valid-time
    /// table's segments additionally carry [`Self::ValidFrom`] /
    /// [`Self::ValidTo`]; use the crate-internal `schema` helper to get the full
    /// ordered set for a given valid-time policy rather than iterating `ALL`
    /// directly when the opt-in columns matter.
    pub const ALL: [Self; 7] = [
        Self::BusinessKey,
        Self::SysFrom,
        Self::SysTo,
        Self::Payload,
        Self::TxnId,
        Self::CommittedAt,
        Self::Principal,
    ];

    /// The always-on set ([`Self::ALL`]) extended with the valid-time pair —
    /// the column set a *valid-time* table's segment carries, in writer/reader
    /// canonical order. The valid-time columns sit at the end so the always-on
    /// ids keep their frozen positions.
    const ALL_WITH_VALID_TIME: [Self; 9] = [
        Self::BusinessKey,
        Self::SysFrom,
        Self::SysTo,
        Self::Payload,
        Self::TxnId,
        Self::CommittedAt,
        Self::Principal,
        Self::ValidFrom,
        Self::ValidTo,
    ];

    /// The ordered column set a segment carries given the table's valid-time
    /// opt-in: [`Self::ALL`] for a system-only table, or that set plus
    /// `valid_from` / `valid_to` when the table tracks valid-time ([STL-117]).
    /// The writer iterates this to lay out chunks; the reader recovers the set
    /// from the footer's column list, so the two never drift.
    pub(super) const fn schema(valid_time: bool) -> &'static [Self] {
        if valid_time {
            &Self::ALL_WITH_VALID_TIME
        } else {
            &Self::ALL
        }
    }

    pub(super) const fn ty(self) -> ColumnType {
        match self {
            Self::BusinessKey | Self::Payload | Self::Principal => ColumnType::Bytes,
            Self::SysFrom
            | Self::SysTo
            | Self::TxnId
            | Self::CommittedAt
            | Self::ValidFrom
            | Self::ValidTo => ColumnType::I64,
        }
    }

    pub(super) const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::BusinessKey),
            1 => Some(Self::SysFrom),
            2 => Some(Self::SysTo),
            3 => Some(Self::Payload),
            4 => Some(Self::TxnId),
            5 => Some(Self::CommittedAt),
            6 => Some(Self::Principal),
            7 => Some(Self::ValidFrom),
            8 => Some(Self::ValidTo),
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
