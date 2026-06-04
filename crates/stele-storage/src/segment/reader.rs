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

use std::cmp::Ordering;

use stele_common::time::SystemTimeMicros;

use crate::backend::{Disk, DiskFile};
use crate::checksum::crc32c;
use crate::delta::{BusinessKey, Version};

use super::SegmentError;
use super::format::{
    CHUNK_HEADER_LEN, Codec, ColumnId, ColumnType, FORMAT_VERSION, HEADER_LEN, HEADER_MAGIC,
    TRAILER_LEN, TRAILER_MAGIC,
};
use super::zone_map::{Predicate, ZoneBound, ZoneMap};

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
    zone_map: ZoneMap,
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
    stat_min: Option<ZoneBound>,
    stat_max: Option<ZoneBound>,
}

impl<F: DiskFile> SegmentReader<F> {
    /// Open the sealed segment at `name` for read. Validates header magic,
    /// format version, trailer magic, and footer CRC; returns
    /// [`SegmentError::Corrupt`] on any mismatch.
    pub fn open<D: Disk<File = F>>(disk: &D, name: &str) -> Result<Self, SegmentError> {
        let file = disk.open(name)?;
        validate_header(&file)?;
        let footer = read_footer(&file)?;
        let zone_map = build_zone_map(&footer);
        Ok(Self {
            file,
            footer,
            zone_map,
        })
    }

    /// Schema id stored in the footer. v0.1 always returns `0` (the implicit
    /// `Version` schema); [`Self::open`] rejects any other value, so callers
    /// can treat this as a constant for as long as the format version is `1`.
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

    /// The segment's resident [`ZoneMap`], decoded once at open from the
    /// footer's per-column min/max stats.
    ///
    /// The returned map is independent of the segment's column-chunk bytes:
    /// the planner can clone it and keep it after the segment has been tiered
    /// to cold storage, the property
    /// [ADR-0021](../../../../../docs/adr/0021-storage-lifecycle-tiered-archival.md)
    /// relies on (*zone maps are never archived*).
    #[must_use]
    pub const fn zone_map(&self) -> &ZoneMap {
        &self.zone_map
    }

    /// Whether this segment *might* contain a row visible at `snapshot` that
    /// satisfies `predicate` — the planner's per-segment skip test.
    ///
    /// Delegates to [`ZoneMap::might_contain`] and so touches **no** column
    /// chunk: a `false` result lets the planner prune the segment before any
    /// read I/O. Conservative by construction — never `false` for a segment
    /// that holds a match.
    #[must_use]
    pub fn might_contain(&self, predicate: &Predicate, snapshot: crate::delta::Snapshot) -> bool {
        self.zone_map.might_contain(predicate, snapshot)
    }

    /// Read one column end-to-end across every row-group, in row order. The
    /// late-materialization path: only the requested column's chunks are
    /// touched, and each chunk's CRC32C is verified before any of its bytes
    /// are decoded.
    pub fn read_column(&self, col: ColumnId) -> Result<ColumnData, SegmentError> {
        // No `with_capacity` from `self.row_count()` — that figure is
        // footer-derived and the natural-growth `Vec` is the safer baseline
        // against a corrupt footer that advertises billions of rows. Each
        // chunk read appends `value_count` (a `u32`) values, and chunk
        // payload bytes are independently bounded by `read_chunk_payload`'s
        // file-length check below, so the in-loop growth is itself bounded
        // by the file's actual size.
        match col.ty() {
            ColumnType::Bytes => {
                let mut out: Vec<Vec<u8>> = Vec::new();
                for rg in &self.footer.row_groups {
                    let meta = chunk_meta(rg, col)?;
                    let payload = read_chunk_payload(&self.file, meta)?;
                    decode_bytes_chunk(&payload, meta.value_count, &mut out)?;
                }
                Ok(ColumnData::Bytes(out))
            }
            ColumnType::I64 => {
                let mut out: Vec<i64> = Vec::new();
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
    // v0.1 has exactly one schema: id 0, the implicit `Version` schema. A
    // segment carrying any other id was written by a version of the format
    // this reader does not understand — refuse, with a typed error, before
    // any further trust in the footer.
    if schema_id != 0 {
        return Err(SegmentError::Corrupt("unknown schema id in footer"));
    }
    let _flags = p.u32()?;
    let row_group_count = p.u32()?;
    // No `Vec::with_capacity(row_group_count)` — the count is footer-derived
    // and an oversized value would force a giant allocation before the
    // parser has scanned enough bytes to disbelieve it. Pushing into an
    // empty `Vec` and letting it grow is naturally bounded by the parser's
    // per-field bounds check (a corrupt count exhausts the footer buffer
    // and surfaces as `Corrupt` on the next field read).
    let mut row_groups: Vec<RowGroup> = Vec::new();
    for _ in 0..row_group_count {
        let row_count = p.u32()?;
        let column_count = p.u32()?;
        // Same reasoning: don't trust footer-derived `column_count` for an
        // up-front allocation.
        let mut columns: Vec<ColumnChunkMeta> = Vec::new();
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
            // Stats feed zone-map pruning (STL-89). A zero-length field is the
            // writer's "no stats" sentinel; a non-empty field is decoded into a
            // typed bound matching the column's `ColumnType`. The declared
            // lengths are bounded by the footer-CRC envelope, so an oversized
            // length can't escape the footer.
            let min_len = p.u32()? as usize;
            let min_bytes = p.bytes(min_len)?;
            let max_len = p.u32()? as usize;
            let max_bytes = p.bytes(max_len)?;
            let stat_min = decode_stat(column_id, min_bytes)?;
            let stat_max = decode_stat(column_id, max_bytes)?;
            // Every column in a row-group shares the row-group's row count.
            // Detect a footer that claims a row count contradicting its own
            // per-column figures at open time, so the inconsistency surfaces
            // here rather than as a silent disagreement between
            // `row_count()` and what a projection actually returns.
            if value_count != row_count {
                return Err(SegmentError::Corrupt(
                    "column value_count disagrees with row-group row_count",
                ));
            }
            columns.push(ColumnChunkMeta {
                column_id,
                codec,
                offset,
                length,
                value_count,
                stat_min,
                stat_max,
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

/// Decode one footer stat field into a typed [`ZoneBound`]. The zero-length
/// sentinel maps to `None` ("no stats"); a non-empty field is interpreted
/// according to the column's [`ColumnType`], and an `i64` stat whose length is
/// not exactly 8 bytes is rejected as corruption rather than silently
/// truncated.
fn decode_stat(col: ColumnId, bytes: &[u8]) -> Result<Option<ZoneBound>, SegmentError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    match col.ty() {
        ColumnType::I64 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| SegmentError::Corrupt("i64 column stat is not 8 bytes"))?;
            Ok(Some(ZoneBound::I64(i64::from_le_bytes(arr))))
        }
        ColumnType::Bytes => Ok(Some(ZoneBound::Bytes(bytes.to_vec()))),
    }
}

/// Fold the per-chunk stats across every row-group into one segment-level
/// [`ZoneMap`]: the overall min is the least of the row-group mins, the overall
/// max the greatest of the row-group maxes. v0.1 emits a single row-group, so
/// this collapses to a copy; the fold keeps the segment-level digest correct
/// once multi-row-group writes land.
fn build_zone_map(footer: &Footer) -> ZoneMap {
    let bounds = ColumnId::ALL.into_iter().map(|col| {
        let mut min: Option<ZoneBound> = None;
        let mut max: Option<ZoneBound> = None;
        for rg in &footer.row_groups {
            for c in rg.columns.iter().filter(|c| c.column_id == col) {
                // Compare by reference (`ZoneBound` isn't `Copy`); replace only
                // on a *provable* same-variant ordering. Every chunk for one
                // column shares that column's type, so the fold always sees a
                // `Some` ordering here.
                if let Some(m) = &c.stat_min {
                    if min
                        .as_ref()
                        .is_none_or(|cur| m.cmp_same_variant(cur) == Some(Ordering::Less))
                    {
                        min = Some(m.clone());
                    }
                }
                if let Some(m) = &c.stat_max {
                    if max
                        .as_ref()
                        .is_none_or(|cur| m.cmp_same_variant(cur) == Some(Ordering::Greater))
                    {
                        max = Some(m.clone());
                    }
                }
            }
        }
        (col, min, max)
    });
    ZoneMap::from_bounds(bounds)
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
    // Bound the allocation by the file's actual size *before* allocating.
    // A corrupt footer could declare a multi-GB chunk that the read would
    // then short-read; without this check, the `vec![0u8; length]` below
    // would attempt the giant allocation first.
    let end = meta
        .offset
        .checked_add(meta.length)
        .ok_or(SegmentError::Corrupt("chunk offset + length overflows u64"))?;
    if end > file.len() {
        return Err(SegmentError::Corrupt(
            "chunk extends past end of file — footer disagrees with file",
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
    // CRC covers header[0..12] || payload — i.e. the chunk header bytes
    // *excluding* the CRC field itself (header[12..16]) followed by the
    // payload bytes. This is the same byte range the writer fed into
    // `crc32c` before stamping the CRC into header[12..16], so a flip
    // anywhere in those bytes — or in the CRC field itself — fails this
    // comparison.
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

#[cfg(test)]
mod tests {
    //! Footer-parser unit tests that exercise the defensive checks too
    //! awkward to reach via an integration test (CRC-protected fields can't
    //! be flipped in-place without recomputing the envelope CRC, which is
    //! exactly what the public-facing corruption sweep already covers).
    //!
    //! These tests build footer-payload byte sequences directly and call
    //! `parse_footer` — the byte-level format is the same shape
    //! `SegmentWriter` emits, so a writer-side change that drifts the
    //! footer layout breaks both these tests and the integration sweep at
    //! once.

    use super::*;

    /// Build a footer payload for a single row-group with the given
    /// per-column overrides. Defaults match a freshly-written one-row
    /// segment: schema 0, one row-group with `row_count`, every column
    /// `Plain`, every chunk with `row_count` values, zero-length stats.
    fn footer_payload(schema_id: u32, row_count: u32, override_column_value_count: u32) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&schema_id.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // flags
        out.extend_from_slice(&1u32.to_le_bytes()); // row_group_count
        // row-group 0
        out.extend_from_slice(&row_count.to_le_bytes());
        // `ColumnId::ALL.len()` is the const `4`; the cast can never truncate.
        let column_count = u32::try_from(ColumnId::ALL.len()).expect("ColumnId::ALL fits in u32");
        out.extend_from_slice(&column_count.to_le_bytes());
        let mut offset: u64 = HEADER_LEN as u64;
        for &col in &ColumnId::ALL {
            out.extend_from_slice(&col.as_u16().to_le_bytes());
            out.push(Codec::Plain as u8);
            out.push(0u8); // reserved
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&16u64.to_le_bytes()); // length — header only, no payload
            out.extend_from_slice(&override_column_value_count.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes()); // reserved
            out.extend_from_slice(&0u32.to_le_bytes()); // stat_min_len
            out.extend_from_slice(&0u32.to_le_bytes()); // stat_max_len
            offset += 16;
        }
        out
    }

    #[test]
    fn unknown_schema_id_is_rejected() {
        let bytes = footer_payload(1, 0, 0);
        let err = parse_footer(&bytes).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("schema id")),
            "schema_id != 0 must be rejected with a typed schema-id error, got {err:?}"
        );
    }

    #[test]
    fn row_count_disagreeing_with_column_value_count_is_rejected() {
        // row_count = 5, but every column reports value_count = 4. A reader
        // that trusted row_count for sizing would return inconsistent rows;
        // the open-time cross-check catches it.
        let bytes = footer_payload(0, 5, 4);
        let err = parse_footer(&bytes).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("value_count")),
            "row_count vs value_count disagreement must surface a typed error, got {err:?}"
        );
    }

    #[test]
    fn matching_row_count_and_value_count_parses_clean() {
        // Regression: the row_count cross-check must not reject a
        // well-formed footer.
        let bytes = footer_payload(0, 7, 7);
        let footer = parse_footer(&bytes).expect("clean footer must parse");
        assert_eq!(footer.schema_id, 0);
        assert_eq!(footer.row_groups.len(), 1);
        assert_eq!(footer.row_groups[0].row_count, 7);
        for col in &footer.row_groups[0].columns {
            assert_eq!(col.value_count, 7);
        }
    }

    #[test]
    fn empty_stat_decodes_to_no_stats_sentinel() {
        assert_eq!(decode_stat(ColumnId::SysFrom, &[]).unwrap(), None);
        assert_eq!(decode_stat(ColumnId::BusinessKey, &[]).unwrap(), None);
    }

    #[test]
    fn typed_stats_decode_by_column_type() {
        assert_eq!(
            decode_stat(ColumnId::SysFrom, &42i64.to_le_bytes()).unwrap(),
            Some(ZoneBound::I64(42)),
        );
        assert_eq!(
            decode_stat(ColumnId::BusinessKey, b"abc").unwrap(),
            Some(ZoneBound::Bytes(b"abc".to_vec())),
        );
    }

    #[test]
    fn i64_stat_with_non_8_byte_length_is_rejected() {
        // A corrupt footer that declares a 4-byte min for an i64 column must
        // surface a typed error, not silently decode a truncated value.
        let err = decode_stat(ColumnId::SysTo, &[0u8; 4]).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("8 bytes")),
            "i64 stat length mismatch must be rejected, got {err:?}"
        );
    }

    /// In-memory `DiskFile` that reports a fixed `len`. Used by the
    /// `read_chunk_payload` bounds tests so they can probe the allocation
    /// guard without standing up a full segment + footer round-trip.
    struct LenOnlyFile {
        len: u64,
    }
    impl DiskFile for LenOnlyFile {
        fn append(&mut self, _: &[u8]) -> std::io::Result<()> {
            unreachable!("test file is read-only")
        }
        fn read_at(&self, _: u64, _: &mut [u8]) -> std::io::Result<usize> {
            // `read_chunk_payload`'s bounds check fires before any read, so
            // these tests never reach this path.
            unreachable!("bounds check must fire before read_at is called")
        }
        fn sync(&mut self) -> std::io::Result<()> {
            unreachable!("test file is read-only")
        }
        fn len(&self) -> u64 {
            self.len
        }
    }

    const fn meta(offset: u64, length: u64) -> ColumnChunkMeta {
        ColumnChunkMeta {
            column_id: ColumnId::SysFrom,
            codec: Codec::Plain,
            offset,
            length,
            value_count: 1,
            stat_min: None,
            stat_max: None,
        }
    }

    #[test]
    fn chunk_extending_past_file_end_is_rejected_before_allocation() {
        // Footer claims a 100-byte chunk at offset 50, but the file is only
        // 100 bytes long — the bounds check must surface as `Corrupt`
        // *before* `vec![0u8; length]` runs. The `LenOnlyFile`'s `read_at`
        // is `unreachable!()`, so any test that allocates and reads would
        // panic instead of returning the typed error.
        let file = LenOnlyFile { len: 100 };
        let err = read_chunk_payload(&file, &meta(50, 100)).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("past end of file")),
            "expected end-of-file rejection, got {err:?}"
        );
    }

    #[test]
    fn chunk_offset_plus_length_overflow_is_rejected_before_allocation() {
        // `meta.offset + meta.length` overflows `u64` — the writer can
        // never produce this, but a corrupt footer could. The checked_add
        // must surface as `Corrupt` rather than wrap-and-pass.
        let file = LenOnlyFile { len: 100 };
        let err = read_chunk_payload(&file, &meta(u64::MAX - 8, 100)).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("overflow")),
            "expected u64 overflow rejection, got {err:?}"
        );
    }
}
