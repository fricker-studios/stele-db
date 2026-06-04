//! `SegmentWriter` — assemble a sealed segment from a delta flush.
//!
//! Lifecycle is deliberately three-step:
//!
//! ```ignore
//! let mut w = SegmentWriter::create(&disk, "segment-00.seg")?;
//! for v in delta.flush_to_segment()? { w.push(v)?; }
//! w.finish()?; // consumes `self`; the segment is now sealed.
//! ```
//!
//! `create` calls [`Disk::create`], so a second writer aimed at the same
//! name returns `AlreadyExists` from the underlying disk — there is no
//! `open`-for-write surface on this API, which is what makes sealed
//! segments append-rejecting at the type level
//! ([architecture §12 invariant 1](../../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).

use crate::checksum::crc32c;
use crate::delta::Version;
use crate::wal::{Disk, DiskFile};

use super::SegmentError;
use super::format::{
    CHUNK_HEADER_LEN, Codec, ColumnId, ColumnType, FORMAT_VERSION, HEADER_LEN, HEADER_MAGIC,
    SCHEMA_ID_IMPLICIT_VERSION, TRAILER_LEN, TRAILER_MAGIC,
};

/// Streaming writer over a single sealed-segment file.
///
/// v0.1 emits exactly one row-group per segment, so all pushed rows are held
/// in memory until [`finish`](Self::finish) drains them into chunks. The
/// on-disk footer already enumerates row-groups, so a future writer can flush
/// in row-group-sized batches without bumping the format version.
pub struct SegmentWriter<F: DiskFile> {
    file: F,
    rows: Vec<Version>,
}

impl<F: DiskFile> SegmentWriter<F> {
    /// Create a new sealed segment file at `name` on `disk`. Errors with
    /// [`std::io::ErrorKind::AlreadyExists`] (surfaced as
    /// [`SegmentError::Io`]) if the file already exists — sealed segments
    /// are immutable, so the writer never opens an existing file for append.
    pub fn create<D: Disk<File = F>>(disk: &D, name: &str) -> Result<Self, SegmentError> {
        let mut file = disk.create(name)?;
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(&HEADER_MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes()); // flags
        header.extend_from_slice(&0u32.to_le_bytes()); // reserved
        debug_assert_eq!(header.len(), HEADER_LEN);
        file.append(&header)?;
        Ok(Self {
            file,
            rows: Vec::new(),
        })
    }

    /// Buffer one row for inclusion in the current (and, in v0.1, only)
    /// row-group. The row is not yet on disk — [`finish`](Self::finish)
    /// commits the row-group's bytes.
    pub fn push(&mut self, version: Version) -> Result<(), SegmentError> {
        // Surface the same encoding-size precondition the delta tier enforces
        // before bytes hit any column chunk: keeps the typed `TooLarge` error
        // localized to the row that caused it, rather than letting a runaway
        // column buffer surface as a less-specific i/o error at finish().
        version.check_encodable()?;
        self.rows.push(version);
        Ok(())
    }

    /// Seal the segment: emit every buffered row as one row-group, then write
    /// the footer and trailer and `sync`. After return the file is immutable
    /// in the format's sense — no writer API can reach it.
    pub fn finish(mut self) -> Result<(), SegmentError> {
        // Per-column buffers. Row order is preserved: column i's k-th value
        // came from `self.rows[k]`.
        let mut chunks: Vec<EncodedChunk> = Vec::with_capacity(ColumnId::ALL.len());
        let mut offset: u64 = HEADER_LEN as u64;

        for &col in &ColumnId::ALL {
            let encoded = encode_column(col, &self.rows)?;
            // Each chunk is laid out contiguously in `(business_key, sys_from,
            // sys_to, payload)` order. The footer records the absolute
            // offset, so the reader projects exactly the columns it wants.
            let length = (CHUNK_HEADER_LEN + encoded.payload.len()) as u64;
            chunks.push(EncodedChunk {
                col,
                offset,
                length,
                value_count: u32::try_from(self.rows.len()).map_err(|_| {
                    SegmentError::TooLarge("row count exceeds u32::MAX in one row-group")
                })?,
                payload: encoded.payload,
                stat_min: encoded.stat_min,
                stat_max: encoded.stat_max,
            });
            offset += length;
        }

        // Emit every chunk in declaration order. Each chunk header is
        // self-checksummed: `chunk_crc` covers `(header[0..12] || payload)`,
        // so a flip anywhere except the CRC field itself is detected — and a
        // flip in the CRC field is detected as a mismatch.
        for chunk in &chunks {
            let mut header = Vec::with_capacity(CHUNK_HEADER_LEN);
            let payload_len = u32::try_from(chunk.payload.len()).map_err(|_| {
                SegmentError::TooLarge("column-chunk payload exceeds u32::MAX bytes")
            })?;
            header.extend_from_slice(&payload_len.to_le_bytes());
            header.extend_from_slice(&chunk.value_count.to_le_bytes());
            header.push(Codec::Plain as u8);
            header.extend_from_slice(&[0u8; 3]); // reserved
            debug_assert_eq!(header.len(), CHUNK_HEADER_LEN - 4);
            let mut crc_input = Vec::with_capacity(header.len() + chunk.payload.len());
            crc_input.extend_from_slice(&header);
            crc_input.extend_from_slice(&chunk.payload);
            let crc = crc32c(&crc_input);
            header.extend_from_slice(&crc.to_le_bytes());
            debug_assert_eq!(header.len(), CHUNK_HEADER_LEN);
            self.file.append(&header)?;
            self.file.append(&chunk.payload)?;
        }

        let footer = encode_footer(self.rows.len(), &chunks)?;
        let footer_crc = crc32c(&footer);
        let footer_len = u32::try_from(footer.len())
            .map_err(|_| SegmentError::TooLarge("footer exceeds u32::MAX bytes"))?;
        self.file.append(&footer)?;

        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&footer_crc.to_le_bytes());
        trailer.extend_from_slice(&footer_len.to_le_bytes());
        trailer.extend_from_slice(&TRAILER_MAGIC);
        debug_assert_eq!(trailer.len(), TRAILER_LEN);
        self.file.append(&trailer)?;

        // Best-effort `sync`. Sealed segments are downstream of the WAL —
        // a crash before this sync leaves a malformed segment that the
        // checksum-validating reader will reject, which is exactly the
        // outcome we want; the WAL drives re-flush.
        self.file.sync()?;
        Ok(())
    }
}

struct EncodedColumn {
    payload: Vec<u8>,
    stat_min: Vec<u8>,
    stat_max: Vec<u8>,
}

struct EncodedChunk {
    col: ColumnId,
    offset: u64,
    length: u64,
    value_count: u32,
    payload: Vec<u8>,
    stat_min: Vec<u8>,
    stat_max: Vec<u8>,
}

fn encode_column(col: ColumnId, rows: &[Version]) -> Result<EncodedColumn, SegmentError> {
    match col.ty() {
        ColumnType::Bytes => {
            // Plain layout: `[u32 len][bytes]` repeated. Min/max stats are the
            // lex-min and lex-max of the actual byte values; the catalog will
            // later attach a column-level comparator, but at the format layer
            // bytewise order is the natural choice (it matches how
            // BusinessKey already sorts via `Vec<u8>`'s Ord).
            let mut payload = Vec::new();
            let mut min: Option<&[u8]> = None;
            let mut max: Option<&[u8]> = None;
            for row in rows {
                let bytes = extract_bytes(col, row);
                let len = u32::try_from(bytes.len()).map_err(|_| {
                    SegmentError::TooLarge("value length exceeds u32::MAX in one chunk")
                })?;
                payload.extend_from_slice(&len.to_le_bytes());
                payload.extend_from_slice(bytes);
                min = Some(min.map_or(bytes, |m| if bytes < m { bytes } else { m }));
                max = Some(max.map_or(bytes, |m| if bytes > m { bytes } else { m }));
            }
            Ok(EncodedColumn {
                payload,
                stat_min: min.map(<[u8]>::to_vec).unwrap_or_default(),
                stat_max: max.map(<[u8]>::to_vec).unwrap_or_default(),
            })
        }
        ColumnType::I64 => {
            // Plain layout: 8 LE bytes per value. Min/max stored as 8 LE
            // bytes of the min/max i64. An empty column emits zero-length
            // stat fields (sentinel for "no stats").
            let mut payload = Vec::with_capacity(rows.len() * 8);
            let mut min: Option<i64> = None;
            let mut max: Option<i64> = None;
            for row in rows {
                let v = extract_i64(col, row);
                payload.extend_from_slice(&v.to_le_bytes());
                min = Some(min.map_or(v, |m| m.min(v)));
                max = Some(max.map_or(v, |m| m.max(v)));
            }
            Ok(EncodedColumn {
                payload,
                stat_min: min.map(|v| v.to_le_bytes().to_vec()).unwrap_or_default(),
                stat_max: max.map(|v| v.to_le_bytes().to_vec()).unwrap_or_default(),
            })
        }
    }
}

fn extract_bytes(col: ColumnId, row: &Version) -> &[u8] {
    match col {
        ColumnId::BusinessKey => row.business_key.as_bytes(),
        ColumnId::Payload => &row.payload,
        ColumnId::SysFrom | ColumnId::SysTo => unreachable!("not a bytes column"),
    }
}

fn extract_i64(col: ColumnId, row: &Version) -> i64 {
    match col {
        ColumnId::SysFrom => row.sys_from.0,
        ColumnId::SysTo => row.sys_to.0,
        ColumnId::BusinessKey | ColumnId::Payload => unreachable!("not an i64 column"),
    }
}

fn encode_footer(row_count: usize, chunks: &[EncodedChunk]) -> Result<Vec<u8>, SegmentError> {
    let row_count = u32::try_from(row_count)
        .map_err(|_| SegmentError::TooLarge("row count exceeds u32::MAX in one row-group"))?;
    let column_count = u32::try_from(chunks.len())
        .map_err(|_| SegmentError::TooLarge("column count exceeds u32::MAX"))?;
    let mut out = Vec::new();
    out.extend_from_slice(&SCHEMA_ID_IMPLICIT_VERSION.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // flags
    out.extend_from_slice(&1u32.to_le_bytes()); // row_group_count — v0.1 emits exactly one
    out.extend_from_slice(&row_count.to_le_bytes());
    out.extend_from_slice(&column_count.to_le_bytes());
    for chunk in chunks {
        out.extend_from_slice(&chunk.col.as_u16().to_le_bytes());
        out.push(Codec::Plain as u8);
        out.push(0u8); // reserved
        out.extend_from_slice(&chunk.offset.to_le_bytes());
        out.extend_from_slice(&chunk.length.to_le_bytes());
        out.extend_from_slice(&chunk.value_count.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // reserved
        let min_len = u32::try_from(chunk.stat_min.len())
            .map_err(|_| SegmentError::TooLarge("stat min length exceeds u32::MAX"))?;
        out.extend_from_slice(&min_len.to_le_bytes());
        out.extend_from_slice(&chunk.stat_min);
        let max_len = u32::try_from(chunk.stat_max.len())
            .map_err(|_| SegmentError::TooLarge("stat max length exceeds u32::MAX"))?;
        out.extend_from_slice(&max_len.to_le_bytes());
        out.extend_from_slice(&chunk.stat_max);
    }
    Ok(out)
}
