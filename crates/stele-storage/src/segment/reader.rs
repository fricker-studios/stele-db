//! `SegmentReader` — open a sealed segment and project columns.
//!
//! The reader is **read-only** by construction: it opens through
//! [`Disk::open`] but never calls [`DiskFile::append`] / [`DiskFile::sync`] on
//! the resulting handle, and it surfaces no API that lets a caller do so
//! either. Paired with [`super::writer::SegmentWriter`]'s create-only
//! lifecycle, this means the segment format has no path to mutate a sealed
//! file — invariant 1 from
//! [architecture §12](../../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants).
//!
//! ## Validation
//!
//! [`SegmentReader::open`] eagerly verifies:
//!
//! * header magic + format version,
//! * trailer magic,
//! * footer CRC32C (covers the entire footer payload),
//! * footer self-consistency (lengths, column ids).
//!
//! Per-chunk CRCs are verified on the read path — opening a segment does not
//! pay the cost of scanning every chunk, which preserves the late
//! materialization contract: a caller projecting one column out of four pays
//! for one chunk's I/O and one CRC.

use stele_common::time::SystemTimeMicros;

use crate::checksum::crc32c;
use crate::delta::{BusinessKey, Version};
use crate::wal::{Disk, DiskFile};

use super::SegmentError;
use super::format::{
    CHUNK_HEADER_LEN, Codec, ColumnId, ColumnType, FORMAT_VERSION, HEADER_LEN, HEADER_MAGIC,
    TRAILER_LEN, TRAILER_MAGIC,
};

/// Decoded contents of one projected column chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnData {
    /// Variable-length bytes column ([`ColumnId::BusinessKey`] or
    /// [`ColumnId::Payload`]).
    Bytes(Vec<Vec<u8>>),
    /// Fixed-width `i64` column ([`ColumnId::SysFrom`] or
    /// [`ColumnId::SysTo`]).
    I64(Vec<i64>),
}

/// A sealed segment opened for read.
///
/// The constructor reads + validates the header and footer up front; per-column
/// chunk bytes are not touched until a projection call. Drop the reader to
/// release the file handle.
pub struct SegmentReader<F: DiskFile> {
    file: F,
    footer: Footer,
}

#[derive(Debug, Clone)]
struct Footer {
    schema_id: u32,
    row_groups: Vec<RowGroup>,
}

#[derive(Debug, Clone)]
struct RowGroup {
    row_count: u32,
    columns: Vec<ColumnChunkMeta>,
}

#[derive(Debug, Clone)]
struct ColumnChunkMeta {
    column_id: ColumnId,
    codec: Codec,
    offset: u64,
    length: u64,
    value_count: u32,
}

impl<F: DiskFile> SegmentReader<F> {
    /// Open the sealed segment at `name` for read. Validates header magic,
    /// format version, trailer magic, and footer CRC; returns
    /// [`SegmentError::Corrupt`] on any mismatch.
    pub fn open<D: Disk<File = F>>(disk: &D, name: &str) -> Result<Self, SegmentError> {
        let file = disk.open(name)?;
        validate_header(&file)?;
        let footer = read_footer(&file)?;
        Ok(Self { file, footer })
    }

    /// Schema id stored in the footer. v0.1 always returns `0` (the implicit
    /// `Version` schema).
    #[must_use]
    pub const fn schema_id(&self) -> u32 {
        self.footer.schema_id
    }

    /// Total number of rows summed across every row-group in this segment.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.footer
            .row_groups
            .iter()
            .map(|rg| u64::from(rg.row_count))
            .sum()
    }

    /// Read one column end-to-end across every row-group, in row order. The
    /// late-materialization path: only the requested column's chunks are
    /// touched, and each chunk's CRC32C is verified before any of its bytes
    /// are decoded.
    pub fn read_column(&self, col: ColumnId) -> Result<ColumnData, SegmentError> {
        // Saturate the `with_capacity` cast: row_count() is a `u64` but the
        // resulting `Vec` cannot logically exceed `usize::MAX` rows. On a
        // 32-bit host, a row count over `usize::MAX` is itself a corrupt
        // segment that the read path will reject when it tries to allocate
        // the column buffer — but the cast itself is non-truncating in
        // practice on 64-bit, and harmless as a starting capacity hint
        // (the `Vec` regrows on demand).
        let cap = usize::try_from(self.row_count()).unwrap_or(usize::MAX);
        match col.ty() {
            ColumnType::Bytes => {
                let mut out: Vec<Vec<u8>> = Vec::with_capacity(cap);
                for rg in &self.footer.row_groups {
                    let meta = chunk_meta(rg, col)?;
                    let payload = read_chunk_payload(&self.file, meta)?;
                    decode_bytes_chunk(&payload, meta.value_count, &mut out)?;
                }
                Ok(ColumnData::Bytes(out))
            }
            ColumnType::I64 => {
                let mut out: Vec<i64> = Vec::with_capacity(cap);
                for rg in &self.footer.row_groups {
                    let meta = chunk_meta(rg, col)?;
                    let payload = read_chunk_payload(&self.file, meta)?;
                    decode_i64_chunk(&payload, meta.value_count, &mut out)?;
                }
                Ok(ColumnData::I64(out))
            }
        }
    }

    /// Read every column and reassemble [`Version`]s in row order — the
    /// dual of [`super::writer::SegmentWriter::push`]. Useful for tests and
    /// for the compaction reader; query execution prefers the projected
    /// [`Self::read_column`].
    pub fn read_versions(&self) -> Result<Vec<Version>, SegmentError> {
        let business_keys = self.read_column(ColumnId::BusinessKey)?;
        let sys_from = self.read_column(ColumnId::SysFrom)?;
        let sys_to = self.read_column(ColumnId::SysTo)?;
        let payloads = self.read_column(ColumnId::Payload)?;
        let (
            ColumnData::Bytes(business_keys),
            ColumnData::I64(sys_from),
            ColumnData::I64(sys_to),
            ColumnData::Bytes(payloads),
        ) = (business_keys, sys_from, sys_to, payloads)
        else {
            // Each column's decoder picks the right ColumnData arm from
            // ColumnId::ty(), so this is structurally unreachable. Keep the
            // typed error rather than `unreachable!()` so a future codec
            // expansion that loosens the mapping has a single place to fail
            // loudly.
            return Err(SegmentError::Corrupt(
                "column data type mismatched expected schema",
            ));
        };
        if !(business_keys.len() == sys_from.len()
            && sys_from.len() == sys_to.len()
            && sys_to.len() == payloads.len())
        {
            return Err(SegmentError::Corrupt(
                "per-column value counts disagree within row-group",
            ));
        }
        let mut out = Vec::with_capacity(business_keys.len());
        for (((bk, sf), st), pl) in business_keys
            .into_iter()
            .zip(sys_from)
            .zip(sys_to)
            .zip(payloads)
        {
            out.push(Version {
                business_key: BusinessKey::new(bk),
                sys_from: SystemTimeMicros(sf),
                sys_to: SystemTimeMicros(st),
                payload: pl,
            });
        }
        Ok(out)
    }
}

fn validate_header<F: DiskFile>(file: &F) -> Result<(), SegmentError> {
    if file.len() < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Err(SegmentError::Corrupt(
            "file shorter than minimum (header + trailer)",
        ));
    }
    let mut buf = [0u8; HEADER_LEN];
    let n = file.read_at(0, &mut buf)?;
    if n != HEADER_LEN {
        return Err(SegmentError::Corrupt("short read on header"));
    }
    if buf[0..8] != HEADER_MAGIC {
        return Err(SegmentError::Corrupt("header magic mismatch"));
    }
    let version = u16::from_le_bytes(buf[8..10].try_into().expect("2 bytes"));
    if version != FORMAT_VERSION {
        return Err(SegmentError::UnsupportedVersion {
            got: version,
            expected: FORMAT_VERSION,
        });
    }
    Ok(())
}

fn read_footer<F: DiskFile>(file: &F) -> Result<Footer, SegmentError> {
    let file_len = file.len();
    let trailer_off = file_len
        .checked_sub(TRAILER_LEN as u64)
        .ok_or(SegmentError::Corrupt("file shorter than trailer"))?;
    let mut trailer = [0u8; TRAILER_LEN];
    let n = file.read_at(trailer_off, &mut trailer)?;
    if n != TRAILER_LEN {
        return Err(SegmentError::Corrupt("short read on trailer"));
    }
    let footer_crc = u32::from_le_bytes(trailer[0..4].try_into().expect("4 bytes"));
    let footer_len = u32::from_le_bytes(trailer[4..8].try_into().expect("4 bytes"));
    if trailer[8..16] != TRAILER_MAGIC {
        return Err(SegmentError::Corrupt("trailer magic mismatch"));
    }
    let footer_off = trailer_off
        .checked_sub(u64::from(footer_len))
        .ok_or(SegmentError::Corrupt("footer length exceeds file size"))?;
    if footer_off < HEADER_LEN as u64 {
        return Err(SegmentError::Corrupt("footer overlaps header"));
    }
    // `footer_len` is u32, so the allocation is bounded — no risk of an
    // attacker-controlled gigantic allocation from a corrupt trailer.
    let mut payload = vec![0u8; footer_len as usize];
    let n = file.read_at(footer_off, &mut payload)?;
    if n != payload.len() {
        return Err(SegmentError::Corrupt("short read on footer"));
    }
    if crc32c(&payload) != footer_crc {
        return Err(SegmentError::Corrupt("footer CRC mismatch"));
    }
    parse_footer(&payload)
}

fn parse_footer(bytes: &[u8]) -> Result<Footer, SegmentError> {
    let mut p = Parser::new(bytes);
    let schema_id = p.u32()?;
    let _flags = p.u32()?;
    let row_group_count = p.u32()?;
    let mut row_groups = Vec::with_capacity(row_group_count as usize);
    for _ in 0..row_group_count {
        let row_count = p.u32()?;
        let column_count = p.u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let column_id_raw = p.u16()?;
            let column_id = ColumnId::from_u16(column_id_raw)
                .ok_or(SegmentError::Corrupt("unknown column id in footer"))?;
            let codec_raw = p.u8()?;
            let codec = Codec::from_byte(codec_raw)
                .ok_or(SegmentError::Corrupt("unknown codec in footer"))?;
            let _reserved = p.u8()?;
            let offset = p.u64()?;
            let length = p.u64()?;
            let value_count = p.u32()?;
            let _reserved = p.u32()?;
            let min_len = p.u32()? as usize;
            // Stats payloads are intentionally skipped here — STL-89 consumes
            // them for zone-map pruning, but the format-level reader only
            // needs to walk over their bytes to advance the parser cursor.
            // Bounded by the footer-CRC envelope so an oversized declared
            // length can't escape the footer.
            let _min = p.bytes(min_len)?;
            let max_len = p.u32()? as usize;
            let _max = p.bytes(max_len)?;
            columns.push(ColumnChunkMeta {
                column_id,
                codec,
                offset,
                length,
                value_count,
            });
        }
        row_groups.push(RowGroup { row_count, columns });
    }
    if !p.is_empty() {
        return Err(SegmentError::Corrupt("trailing bytes in footer"));
    }
    Ok(Footer {
        schema_id,
        row_groups,
    })
}

fn chunk_meta(rg: &RowGroup, col: ColumnId) -> Result<&ColumnChunkMeta, SegmentError> {
    rg.columns
        .iter()
        .find(|c| c.column_id == col)
        .ok_or(SegmentError::Corrupt("column missing from row-group"))
}

fn read_chunk_payload<F: DiskFile>(
    file: &F,
    meta: &ColumnChunkMeta,
) -> Result<Vec<u8>, SegmentError> {
    let length = usize::try_from(meta.length)
        .map_err(|_| SegmentError::Corrupt("chunk length exceeds usize"))?;
    if length < CHUNK_HEADER_LEN {
        return Err(SegmentError::Corrupt(
            "chunk shorter than its own header — footer disagrees with file",
        ));
    }
    let mut buf = vec![0u8; length];
    let n = file.read_at(meta.offset, &mut buf)?;
    if n != buf.len() {
        return Err(SegmentError::Corrupt("short read on column chunk"));
    }
    let payload_len = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes")) as usize;
    let value_count = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
    let codec_raw = buf[8];
    let crc = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
    if Codec::from_byte(codec_raw) != Some(meta.codec) {
        return Err(SegmentError::Corrupt(
            "chunk codec disagrees with footer entry",
        ));
    }
    if value_count != meta.value_count {
        return Err(SegmentError::Corrupt(
            "chunk value_count disagrees with footer entry",
        ));
    }
    if CHUNK_HEADER_LEN + payload_len != length {
        return Err(SegmentError::Corrupt(
            "chunk length disagrees with declared payload",
        ));
    }
    // CRC covers header[0..12] || payload. Zero out the crc field in the
    // covered region — equivalent to never having written it.
    let mut crc_input = Vec::with_capacity(12 + payload_len);
    crc_input.extend_from_slice(&buf[0..12]);
    crc_input.extend_from_slice(&buf[CHUNK_HEADER_LEN..]);
    if crc32c(&crc_input) != crc {
        return Err(SegmentError::Corrupt("chunk CRC mismatch"));
    }
    Ok(buf[CHUNK_HEADER_LEN..].to_vec())
}

fn decode_bytes_chunk(
    payload: &[u8],
    value_count: u32,
    out: &mut Vec<Vec<u8>>,
) -> Result<(), SegmentError> {
    let mut p = Parser::new(payload);
    for _ in 0..value_count {
        let len = p.u32()? as usize;
        let bytes = p.bytes(len)?;
        out.push(bytes.to_vec());
    }
    if !p.is_empty() {
        return Err(SegmentError::Corrupt("trailing bytes in bytes column"));
    }
    Ok(())
}

fn decode_i64_chunk(
    payload: &[u8],
    value_count: u32,
    out: &mut Vec<i64>,
) -> Result<(), SegmentError> {
    let expected = value_count as usize * 8;
    if payload.len() != expected {
        return Err(SegmentError::Corrupt(
            "i64 column payload length is not value_count * 8",
        ));
    }
    for i in 0..value_count as usize {
        let start = i * 8;
        let val = i64::from_le_bytes(payload[start..start + 8].try_into().expect("8 bytes"));
        out.push(val);
    }
    Ok(())
}

/// Minimal cursor-style byte parser. Saves a thicket of slice-length checks
/// at every footer / payload offset; one place to surface `Corrupt`.
struct Parser<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Parser<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    const fn is_empty(&self) -> bool {
        self.cursor == self.bytes.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], SegmentError> {
        let end = self
            .cursor
            .checked_add(n)
            .ok_or(SegmentError::Corrupt("parser offset overflow"))?;
        if end > self.bytes.len() {
            return Err(SegmentError::Corrupt("short read parsing footer/chunk"));
        }
        let out = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, SegmentError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, SegmentError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, SegmentError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, SegmentError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], SegmentError> {
        self.take(n)
    }
}
