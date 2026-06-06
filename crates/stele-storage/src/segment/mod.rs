//! Sealed segment file format — Stele's immutable columnar on-disk format.
//!
//! Sealed segments are the bulk-storage half of the storage engine: the delta
//! tier flushes [`crate::delta::Version`] rows into one of these, and from
//! then on the file is immutable
//! ([architecture §3.1–3.2](../../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving),
//! [ADR-0002](../../../../docs/adr/0002-on-disk-storage-format.md)).
//!
//! ## On-disk layout
//!
//! ```text
//! +------------------+
//! | HEADER (16 B)    |  magic "STLSEG\0\0" || format_version: u16 || flags: u16 || reserved: u32
//! +------------------+
//! | ROW-GROUP 0      |  concatenation of COLUMN_CHUNKs in column order
//! |   COLUMN_CHUNK 0 |    16-B header + CRC32C-protected payload
//! |   COLUMN_CHUNK 1 |
//! |   ...            |
//! +------------------+
//! | ROW-GROUP 1      |  v0.1 emits exactly one row-group; format supports N
//! | ...              |
//! +------------------+
//! | FOOTER (var)     |  schema_id || flags || row_groups: [ row_count, columns: [ id, codec, offset, length, value_count, min, max ] ]
//! +------------------+
//! | TRAILER (16 B)   |  footer_crc: u32 || footer_len: u32 || magic "STLSEGFT"
//! +------------------+
//! ```
//!
//! Every column chunk's CRC32C covers `chunk_header[0..12] || payload`, and
//! the footer's CRC32C covers its entire payload — so a single-byte flip
//! anywhere in a page or in the footer is detected at read time.
//!
//! ## Immutability — append-rejecting at the type level
//!
//! [`SegmentWriter::create`] is the only public surface that produces a
//! writable file handle, and it routes through [`crate::wal::Disk::create`],
//! which errors with `AlreadyExists` if the named file is on disk. There is
//! no `SegmentWriter::open(...)`; [`SegmentReader::open`] is read-only and
//! never invokes `append` / `sync`. Together that means a sealed segment
//! cannot be reopened for append through this API — invariant 1 from
//! [architecture §12](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)
//! is enforced by the absence of a writing path, not by a runtime check.
//!
//! ## Codecs (v0.1)
//!
//! v0.1 emits a single codec — `Plain`. The per-chunk codec tag is the
//! dispatch point in both writer and reader, so the architecture-listed
//! codecs (dict + bitpack, RLE, delta, FOR — see
//! [architecture §3.2](../../../../docs/02-architecture.md#32-on-disk-segment-format))
//! drop in as new variants without bumping the on-disk format version. The
//! follow-up tickets carrying them are tracked under the v0.1 epic [STL-76].
//!
//! ## What is *not* in v0.1
//!
//! * Multi-row-group writes. The format describes N row-groups and the reader
//!   walks them all; the writer emits one. Adding row-group flushing is a
//!   writer-side change with no format implications.
//! * Bloom filters. The footer reserves a stats area per column; the per-column
//!   min/max stats it records feed the zone-map pruning that landed with
//!   [STL-89] ([`ZoneMap`]), and bloom filters slot in alongside them as new
//!   typed fields in a later ticket.
//! * Schema evolution. v0.1 has one implicit schema id — 0, the implicit
//!   `Version` schema. Real schema resolution rides on [STL-98]'s versioned
//!   catalog.
//!
//! ## Format versioning & pre-transition segments (migration note)
//!
//! Dropping the stored `sys_to` column (and the close-provenance columns) into
//! the derived [validity index](crate::validity) bumped the on-disk
//! `FORMAT_VERSION` v5 → v6 (STL-134,
//! [ADR-0023](../../../../docs/adr/0023-append-only-record-model-validity-index.md)).
//! That is a backwards-incompatible layout change: a *pre-transition* segment —
//! one written at v5 or earlier, which still carries a `sys_to` column — is
//! **rejected outright at open** with
//! [`SegmentError::UnsupportedVersion`]. The version check is a single
//! header-level comparison inside [`SegmentReader::open`]: a reader only decodes
//! a segment whose advertised version equals the one it was built for, so an old
//! `sys_to`-bearing footer can never be half-parsed into a v6 `Version`.
//!
//! **There is no read-compat shim and no one-shot rewrite tool, by design.**
//! The on-disk format is a *pre-1.0* surface: it may break between minor
//! versions, each break documented, and there is no forward-compatibility
//! promise until v1.0 ([docs/03 roadmap](../../../../docs/03-roadmap.md),
//! [docs/08 §7](../../../../docs/08-packaging-distribution-and-releases.md#7-versioning--compatibility-policy-the-important-part),
//! [ADR-0014](../../../../docs/adr/0014-release-channels-and-versioning-policy.md)).
//! Because no v0.1 on-disk data has been released, no deployed v5 segment
//! exists to migrate — the "migration" for this change is the clean reject
//! above, not a converter. Were such a segment to exist, the relocation is
//! cheap to honour precisely because the validity index is *derived and
//! rebuildable from the log* ([ADR-0023]): a one-shot rewrite would re-emit the
//! birth columns at v6 and replay the close records into the index, never
//! mutating a sealed file. Building that converter is deferred until the format
//! is stabilised (post-1.0), when a real pre-existing-data migration is owed.

mod format;
mod reader;
mod writer;
mod zone_map;

use std::io;

pub use format::ColumnId;
pub use reader::{ColumnData, SegmentReader};
pub use writer::SegmentWriter;
pub use zone_map::{ColumnZone, Predicate, ZoneBound, ZoneMap};

/// Errors surfaced from the sealed-segment writer and reader.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    /// Underlying disk I/O. Includes `AlreadyExists` returned by
    /// [`SegmentWriter::create`] when a second writer targets a name a
    /// sealed segment already occupies.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// The file on disk does not parse as a valid sealed segment — a
    /// checksum failed, a length disagreed, or a structural field was out
    /// of bounds.
    #[error("malformed segment: {0}")]
    Corrupt(&'static str),

    /// The header advertised a format version the reader does not understand.
    #[error("unsupported segment format version: got {got}, expected {expected}")]
    UnsupportedVersion { got: u16, expected: u16 },

    /// A row, column chunk, or footer field exceeded the per-frame limits
    /// encoded in the format (typically u32 lengths).
    #[error("segment field too large: {0}")]
    TooLarge(&'static str),
}

impl From<crate::delta::DeltaError> for SegmentError {
    fn from(err: crate::delta::DeltaError) -> Self {
        // The writer only forwards `check_encodable` from `Version`, so the
        // only DeltaError variants reachable here are the size precondition
        // errors. Everything else from the delta module routes through its
        // own paths, not this one.
        match err {
            crate::delta::DeltaError::TooLarge(_) => Self::TooLarge("version frame too large"),
            crate::delta::DeltaError::Corrupt(msg) => Self::Corrupt(msg),
            crate::delta::DeltaError::Io(e) => Self::Io(e),
            // The segment writer never folds the validity index, so this variant
            // is unreachable on this path; map it to a corruption marker rather
            // than widen SegmentError for a case the writer cannot produce.
            crate::delta::DeltaError::Validity(_) => {
                Self::Corrupt("unexpected validity-index error on the segment path")
            }
        }
    }
}
