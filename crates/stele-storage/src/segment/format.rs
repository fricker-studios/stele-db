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
///   id 7 mid-footer. v3 still stored the interval *twice* — once as the
///   columns, once as the surviving 16-byte payload prefix.
/// * **v4** — adds the three always-on close-provenance columns
///   (`closed_by_txn`, `closed_at`, `closed_by_principal`), ids 9..=11
///   ([STL-118]): who closed each version's system-time period and when. Like
///   the v2 provenance columns these are on every segment, but populated only on
///   a *closed* version — an open version stores the [`ColumnId::ClosedAt`]
///   sentinel (`SYSTEM_TIME_OPEN`) to mean "not closed". Same
///   backwards-incompatible reasoning as v2/v3: a new column-id set is a clean
///   header-level reject for an older reader.
/// * **v5** — stops duplicating the valid-time interval ([STL-119]). A
///   valid-time segment now stores only the *bare* user payload in the
///   `payload` column; the interval lives solely in `valid_from` / `valid_to`,
///   and the reader re-frames the payload from those columns on read
///   ([`crate::validtime::reframe_payload`]). This is a backwards-incompatible
///   change to the `payload` column's meaning for valid-time segments — a v4
///   reader would return a bare payload, and reading a v4 segment with a v5
///   reader would double-frame it — so the version bump makes the two
///   generations reject each other at the header rather than silently corrupt
///   the payload. System-only segments are byte-identical to v4; the bump
///   covers them too so one generation number describes the whole format.
/// * **v6** — **drops the stored `sys_to` column and the three close-provenance
///   columns** (STL-133, [ADR-0023](../../../../../docs/adr/0023-append-only-record-model-validity-index.md)).
///   A version's system-time *end* and the provenance of the transaction that
///   closed it are no longer stored on the record at all — they are materialized
///   once into the derived, rebuildable [validity index](crate::validity) and
///   overlaid at read time ([`crate::merge`]). A sealed segment now carries only
///   *birth* state: the four data/temporal fields minus `sys_to`, plus the three
///   always-on provenance columns (and, for a valid-time table, the valid-time
///   pair). The column ids are renumbered contiguously; this is a clean
///   header-level reject for an older reader. This is what makes the append-only
///   / tamper-evidence claims hold under scrutiny: nothing on the durable record
///   can be rewritten to say a version's period ended.
/// * **v7** — **persists retractions (logical deletes) as payload-less tombstone
///   rows** (STL-143, [ADR-0023](../../../../../docs/adr/0023-append-only-record-model-validity-index.md)).
///   A delete is a "close with no successor", which version adjacency cannot
///   reconstruct — so a from-scratch rebuild from segments would silently
///   resurrect a deleted row across the deletion gap
///   ([docs/16 §12](../../../../../docs/16-bitemporal-semantics.md#12-deletes-retractions--the-deletion-gap)).
///   v7 stores retractions in a **separate footer section** as six tombstone
///   columns (ids 8..=13: `retract_key`, `retract_sys_from`, `retract_closed_at`,
///   `retract_closed_by_txn`, `retract_closed_by_committed_at`,
///   `retract_closed_by_principal`) — the [`crate::validity::Close`] fields, no
///   payload. They are present only when the segment holds at least one
///   retraction (the optional-columns pattern, like the valid-time pair), carry
///   their **own** value count (independent of the version row count), and get
///   per-column zone-map stats for free. This makes the segment store
///   self-contained for an index rebuild even after WAL truncation. The version
///   row-group is byte-identical to v6; the bump makes a v6 reader reject a v7
///   segment cleanly at the header rather than choke on column id 8 in the
///   footer's new section.
///
/// * **v8** — **adds the always-on per-commit `seq` column** ([`ColumnId::Seq`],
///   id 14; STL-141, [ADR-0024](../../../../../docs/adr/0024-time-representation.md)).
///   `seq` is the per-commit monotonic sequence number that totally orders writes
///   sharing the same µs `sys_from` — carried inline on every version like
///   provenance, so the sealed segment must persist it alongside `sys_from`. It
///   joins the always-on version row-group ([`ColumnId::ALL`]); same
///   backwards-incompatible reasoning as v2–v7: a v7 reader encountering column
///   id 14 in the footer would fail with a confusing `Corrupt("unknown column
///   id")` mid-footer, so the bump makes it reject the segment cleanly at the
///   header instead. The v0.1 chain does not yet *order* on `(sys_from, seq)` at
///   this generation (the column is carried, not yet load-bearing); that follow-up
///   is STL-141 Part B (STL-145), which lands as **v9**.
///
/// * **v9** — **adds the per-commit `seq` to the retraction tombstone**
///   ([`ColumnId::RetractSeq`], id 15; STL-145,
///   [ADR-0024](../../../../../docs/adr/0024-time-representation.md)). STL-141
///   Part B makes `(sys_from, seq)` load-bearing in the read / merge / index
///   paths; deletes must be totally ordered against a same-tick sibling too, so
///   the retraction section gains `seq` (the [`crate::validity::Close::seq`] of
///   the deleted version). Like every column addition since v2 this is a new
///   column id, and *that* is why it bumps the generation rather than riding v8:
///   the footer parser rejects an unknown column id mid-footer
///   (`Corrupt("unknown column id in footer")`), so without a bump an older v8
///   reader would choke on id 15 instead of rejecting cleanly at the header, and
///   this reader could not tell a v8 retraction segment (no `retract_seq` column)
///   from a corrupt one. The version bump restores the clean header-level reject
///   ([`SegmentError::UnsupportedVersion`](super::SegmentError::UnsupportedVersion))
///   in both directions. The version row-group is byte-identical to v8.
///
/// * **v10** — **lets the `payload` column carry SQL `NULL`** ([STL-154]). A
///   value-less (`None`) payload is encoded in the bytes column with its
///   per-value length set to the reserved sentinel [`BYTES_NULL_SENTINEL`]
///   (`u32::MAX`) and no value bytes, mirroring the delta frame's
///   `PAYLOAD_NULL_SENTINEL`. A real value can never
///   reach that length (`MAX_VERSION_FRAME_LEN` caps it at 16 MiB), so the
///   sentinel is unambiguous and only ever appears in the `payload` column. It
///   bumps the generation because a v9 reader would mis-decode the sentinel
///   length as a 4 GiB value (`Corrupt`) rather than reject cleanly at the
///   header; the bump restores the clean header-level reject in both directions.
///   Every other column and the row-group framing are byte-identical to v9.
pub(super) const FORMAT_VERSION: u16 = 10;

/// The per-value length reserved in a bytes column to mean "this cell is SQL
/// `NULL`" ([STL-154], [`FORMAT_VERSION`] v10). Only the `payload` column ever
/// writes it; a present value's length is bounded by `MAX_VERSION_FRAME_LEN`
/// (16 MiB), so `u32::MAX` is otherwise unreachable. The mirror of the delta
/// frame's `PAYLOAD_NULL_SENTINEL`, kept as a distinct constant because the two
/// encodings are independent on-disk formats.
pub(super) const BYTES_NULL_SENTINEL: u32 = u32::MAX;

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
/// pathologically long `BusinessKey`. Inlining a
/// full lex-min/max of such a value would push the footer past its `u32`
/// `footer_len` ceiling, so the writer records only a bounded *prefix* of the
/// lex-min/max instead ([`super::writer`]): the min prefix is truncated *down*
/// (a byte prefix is lex-`<=` its source, so it stays a sound lower bound) and
/// the max prefix is rounded *up* (so it stays a sound upper bound). This caps
/// each bytes column's footer contribution at `2 * MAX_BYTES_STAT_PREFIX_LEN`,
/// independent of value size, which keeps [`ZoneMap::might_contain`](super::zone_map::ZoneMap::might_contain)'s
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
/// (the three birth data/temporal fields `business_key` / `sys_from` / `payload`,
/// the per-commit `seq` tiebreak of [`FORMAT_VERSION`] v8, and the three
/// provenance columns of v2), optionally extended with the valid-time pair for a
/// valid-time table (v3). There is no stored `sys_to` or close-provenance column
/// (v6, [ADR-0023]). The id is
/// hard-coded; once [STL-98] lands the versioned catalog this becomes a real
/// schema reference resolved through the catalog at read time.
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
    /// System-time `sys_from` (fixed `i64`, microseconds). The period *end*
    /// (`sys_to`) is **not** a segment column — it lives in the derived
    /// [validity index](crate::validity) (v6, [ADR-0023]).
    SysFrom = 1,
    /// Opaque payload bytes (variable-length).
    Payload = 2,
    /// Provenance: writing transaction id (fixed 8 bytes). Stored in the `i64`
    /// column layout — `txn_id` is logically a `u64`
    /// ([`stele_common::provenance::TxnId`]); the writer reinterprets the bits
    /// (`u64 as i64`) and the reader reverses it (`i64 as u64`), a lossless
    /// round-trip. Only the zone-map ordering would differ for ids ≥ 2^63,
    /// unreachable for the same reason the system-time `i64` axis is.
    TxnId = 3,
    /// Provenance: commit timestamp (fixed `i64`, microseconds) —
    /// [`stele_common::provenance::Provenance::committed_at`].
    CommittedAt = 4,
    /// Provenance: opaque principal bytes (variable-length) —
    /// [`stele_common::provenance::Principal`].
    Principal = 5,
    /// Valid-time period start (fixed `i64`, microseconds) — the inclusive
    /// `valid_from` boundary, lifted at flush from the payload's valid-time
    /// prefix ([`crate::validtime`], [STL-92]). Present only on a valid-time
    /// table's segments (format v3); absent otherwise.
    ValidFrom = 6,
    /// Valid-time period end (fixed `i64`, microseconds) — the exclusive
    /// `valid_to` boundary; `i64::MAX` for an open-ended fact
    /// ([`stele_common::time::VALID_TIME_OPEN`]). Present only on a valid-time
    /// table's segments, alongside [`Self::ValidFrom`].
    ValidTo = 7,
    /// Retraction tombstone: the business key of the deleted version
    /// (variable-length bytes). Mirrors [`crate::validity::Close::business_key`].
    /// Present only in the segment's retraction section (format v7), never in the
    /// version row-group.
    RetractKey = 8,
    /// Retraction tombstone: the `sys_from` of the version this delete closes
    /// (fixed `i64`) — [`crate::validity::Close::sys_from`].
    RetractSysFrom = 9,
    /// Retraction tombstone: the system-time the period was closed at (fixed
    /// `i64`) — [`crate::validity::Close::sys_to`], the "closed_at" of the delete.
    RetractClosedAt = 10,
    /// Retraction tombstone: the deleting transaction id (fixed 8 bytes, `u64`
    /// bits in the `i64` column like [`Self::TxnId`]) —
    /// `Close::closed_by.txn_id`. The "who deleted" of delete provenance.
    RetractClosedByTxn = 11,
    /// Retraction tombstone: the deleting transaction's commit timestamp (fixed
    /// `i64`) — `Close::closed_by.committed_at`. The "when deleted" of delete
    /// provenance.
    RetractClosedByCommittedAt = 12,
    /// Retraction tombstone: the deleting principal (variable-length bytes) —
    /// `Close::closed_by.principal`. The "by whom" of delete provenance.
    RetractClosedByPrincipal = 13,
    /// Per-commit monotonic sequence number (fixed 8 bytes, `u64` bits in the
    /// `i64` column like [`Self::TxnId`]) — [`crate::delta::Version::seq`]. The
    /// total-order tiebreak for versions sharing the same µs `sys_from`
    /// ([ADR-0024], STL-141). Always-on, on every segment's version row-group
    /// (format v8).
    Seq = 14,
    /// Retraction tombstone: the `seq` of the version this delete closes (fixed 8
    /// bytes, `u64` bits in the `i64` column like [`Self::Seq`]) —
    /// [`crate::validity::Close::seq`]. Completes the deleted version's
    /// `(sys_from, seq)` identity so a delete is totally ordered against a
    /// same-tick sibling ([ADR-0024], STL-145). Present only in the segment's
    /// retraction section, alongside the other tombstone columns (format v9 — a
    /// new column id bumps the generation, see the `FORMAT_VERSION` note above).
    RetractSeq = 15,
}

impl ColumnId {
    /// Every column an always-on (system-only) segment carries, in writer/reader
    /// canonical order. Exposed publicly so tests and other consumers share
    /// a single source of truth for the column set — adding a column here
    /// flows into both writer/reader and every test that iterates the
    /// schema, no shadow constants left to drift.
    ///
    /// The provenance columns ([`Self::TxnId`], [`Self::CommittedAt`],
    /// [`Self::Principal`]) sit after the birth data/temporal fields
    /// ([architecture §3.2](../../../../../docs/02-architecture.md#32-on-disk-segment-format)).
    /// The per-commit [`Self::Seq`] tiebreak (v8) sits next to `sys_from`, the
    /// timestamp it disambiguates. There is no stored `sys_to` or
    /// close-provenance column (v6, [ADR-0023]): a segment carries only birth
    /// state.
    ///
    /// This is the **always-on** set, present on every segment. A valid-time
    /// table's segments additionally carry [`Self::ValidFrom`] /
    /// [`Self::ValidTo`]; use the crate-internal `schema` helper to get the full
    /// ordered set for a given valid-time policy rather than iterating `ALL`
    /// directly when the opt-in columns matter.
    pub const ALL: [Self; 7] = [
        Self::BusinessKey,
        Self::SysFrom,
        Self::Seq,
        Self::Payload,
        Self::TxnId,
        Self::CommittedAt,
        Self::Principal,
    ];

    /// The always-on set ([`Self::ALL`]) plus the valid-time pair — the column
    /// set a *valid-time* table's segment carries, in writer/reader canonical
    /// order. Array position is just the write order, while the footer records
    /// each column's id, so the two never drift.
    const ALL_WITH_VALID_TIME: [Self; 9] = [
        Self::BusinessKey,
        Self::SysFrom,
        Self::Seq,
        Self::Payload,
        Self::TxnId,
        Self::CommittedAt,
        Self::Principal,
        Self::ValidFrom,
        Self::ValidTo,
    ];

    /// The ordered tombstone column set a segment's **retraction section**
    /// carries (format v7, STL-143), in writer/reader canonical order. These
    /// mirror the [`crate::validity::Close`] fields — the business key, the
    /// closed version's `sys_from` and `seq`, the close timestamp, and the closing
    /// transaction's provenance triple — with no payload. The `seq` (STL-145)
    /// completes the deleted version's `(sys_from, seq)` identity so deletes are
    /// totally ordered. Present only when the segment holds at least one
    /// retraction; the footer's retraction-section column list is the source of
    /// truth, so writer and reader never drift.
    pub(super) const RETRACTION: [Self; 7] = [
        Self::RetractKey,
        Self::RetractSysFrom,
        Self::RetractSeq,
        Self::RetractClosedAt,
        Self::RetractClosedByTxn,
        Self::RetractClosedByCommittedAt,
        Self::RetractClosedByPrincipal,
    ];

    /// The ordered column set a segment carries given the table's valid-time
    /// opt-in: [`Self::ALL`] for a system-only table, or that set plus
    /// `valid_from` / `valid_to` when the table tracks valid-time ([STL-117]).
    /// The writer iterates this to lay out chunks; the reader recovers the set
    /// from the footer's column list, so the two never drift. This is the
    /// **version** row-group schema; retraction tombstones live in their own
    /// footer section ([`Self::RETRACTION`]), not here.
    pub(super) const fn schema(valid_time: bool) -> &'static [Self] {
        if valid_time {
            &Self::ALL_WITH_VALID_TIME
        } else {
            &Self::ALL
        }
    }

    pub(super) const fn ty(self) -> ColumnType {
        match self {
            Self::BusinessKey
            | Self::Payload
            | Self::Principal
            | Self::RetractKey
            | Self::RetractClosedByPrincipal => ColumnType::Bytes,
            Self::SysFrom
            | Self::Seq
            | Self::TxnId
            | Self::CommittedAt
            | Self::ValidFrom
            | Self::ValidTo
            | Self::RetractSysFrom
            | Self::RetractSeq
            | Self::RetractClosedAt
            | Self::RetractClosedByTxn
            | Self::RetractClosedByCommittedAt => ColumnType::I64,
        }
    }

    pub(super) const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::BusinessKey),
            1 => Some(Self::SysFrom),
            2 => Some(Self::Payload),
            3 => Some(Self::TxnId),
            4 => Some(Self::CommittedAt),
            5 => Some(Self::Principal),
            6 => Some(Self::ValidFrom),
            7 => Some(Self::ValidTo),
            8 => Some(Self::RetractKey),
            9 => Some(Self::RetractSysFrom),
            10 => Some(Self::RetractClosedAt),
            11 => Some(Self::RetractClosedByTxn),
            12 => Some(Self::RetractClosedByCommittedAt),
            13 => Some(Self::RetractClosedByPrincipal),
            14 => Some(Self::Seq),
            15 => Some(Self::RetractSeq),
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
