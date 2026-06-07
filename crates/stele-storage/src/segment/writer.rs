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

use crate::backend::{Disk, DiskFile};
use crate::checksum::crc32c;
use crate::delta::Version;
use crate::validity::Close;
use crate::validtime::{VALID_TIME_PREFIX_LEN, unframe_payload};

use super::SegmentError;
use super::format::{
    BYTES_NULL_SENTINEL, CHUNK_HEADER_LEN, Codec, ColumnId, ColumnType, FORMAT_VERSION, HEADER_LEN,
    HEADER_MAGIC, MAX_BYTES_STAT_PREFIX_LEN, SCHEMA_ID_IMPLICIT_VERSION, TRAILER_LEN,
    TRAILER_MAGIC,
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
    /// Retraction tombstones (logical deletes) to persist into this segment's
    /// retraction section (format v7, STL-143). Buffered in memory like `rows`;
    /// [`finish`](Self::finish) emits them as their own columnar chunks after the
    /// version row-group. Empty for a segment with no deletes.
    retractions: Vec<Close>,
    /// Whether this segment's table tracks valid-time. When set, [`finish`]
    /// lifts the payload's valid-time prefix into the `valid_from` / `valid_to`
    /// columns ([STL-117]) and stores only the bare user payload in the
    /// `payload` column ([STL-119]); when clear, those columns are absent and
    /// the payload is stored verbatim.
    valid_time: bool,
}

impl<F: DiskFile> SegmentWriter<F> {
    /// Create a new sealed segment file at `name` on `disk` for a **system-only**
    /// table (no valid-time columns). Errors with
    /// [`std::io::ErrorKind::AlreadyExists`] (surfaced as
    /// [`SegmentError::Io`]) if the file already exists — sealed segments
    /// are immutable, so the writer never opens an existing file for append.
    pub fn create<D: Disk<File = F>>(disk: &D, name: &str) -> Result<Self, SegmentError> {
        Self::create_inner(disk, name, false)
    }

    /// Create a new sealed segment file at `name` on `disk` for a **valid-time**
    /// table. Every pushed [`Version`]'s payload must carry the 16-byte
    /// valid-time prefix ([`crate::validtime::frame_payload`], [STL-92]);
    /// [`finish`](Self::finish) decodes it into first-class `valid_from` /
    /// `valid_to` columns so the planner can prune on the valid axis ([STL-117]),
    /// then stores only the bare user payload in the `payload` column so the
    /// interval is not persisted twice ([STL-119]). A reader re-frames the
    /// payload from the columns ([`crate::validtime::reframe_payload`]).
    ///
    /// Same immutability and `AlreadyExists` semantics as [`Self::create`].
    pub fn create_valid_time<D: Disk<File = F>>(
        disk: &D,
        name: &str,
    ) -> Result<Self, SegmentError> {
        Self::create_inner(disk, name, true)
    }

    fn create_inner<D: Disk<File = F>>(
        disk: &D,
        name: &str,
        valid_time: bool,
    ) -> Result<Self, SegmentError> {
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
            retractions: Vec::new(),
            valid_time,
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

    /// Buffer one retraction tombstone for this segment's retraction section
    /// (format v7, STL-143) — a payload-less durable record of a logical delete,
    /// drained from the delta tier at flush
    /// ([`crate::delta::Delta::take_retractions`]). Like [`push`](Self::push) the
    /// row is not yet on disk; [`finish`](Self::finish) commits the retraction
    /// chunks after the version row-group. A segment with no retractions writes no
    /// retraction columns at all.
    ///
    /// # Errors
    ///
    /// [`SegmentError::TooLarge`] if the tombstone's `business_key` or closing
    /// `principal` exceeds the `u32` length the retraction columns are framed
    /// with — preflighted here so the error localizes to the offending row, just
    /// as [`push`](Self::push) does for a [`Version`], rather than surfacing late
    /// in [`finish`](Self::finish).
    pub fn push_retraction(&mut self, close: Close) -> Result<(), SegmentError> {
        if u32::try_from(close.business_key.as_bytes().len()).is_err()
            || u32::try_from(close.closed_by.principal.as_bytes().len()).is_err()
        {
            return Err(SegmentError::TooLarge(
                "retraction key/principal length exceeds u32::MAX in one chunk",
            ));
        }
        self.retractions.push(close);
        Ok(())
    }

    /// Seal the segment: emit every buffered row as one row-group, then write
    /// the footer and trailer and `sync`. After return the file is immutable
    /// in the format's sense — no writer API can reach it.
    pub fn finish(mut self) -> Result<(), SegmentError> {
        // Per-column buffers. Row order is preserved: column i's k-th value
        // came from `self.rows[k]`. A valid-time table's schema carries the
        // two extra `valid_from` / `valid_to` columns ([STL-117]).
        let schema = ColumnId::schema(self.valid_time);
        // Decode each row's valid-time prefix exactly once up front (not once
        // per valid-time column), so emitting both valid_from and valid_to
        // re-uses the same parse ([STL-117]).
        let valid_pairs: Option<Vec<(i64, i64)>> = if self.valid_time {
            Some(decode_valid_pairs(&self.rows)?)
        } else {
            None
        };
        let mut chunks: Vec<EncodedChunk> = Vec::with_capacity(schema.len());
        let mut offset: u64 = HEADER_LEN as u64;

        let version_count = u32::try_from(self.rows.len())
            .map_err(|_| SegmentError::TooLarge("row count exceeds u32::MAX in one row-group"))?;
        for &col in schema {
            let encoded = encode_column(col, &self.rows, valid_pairs.as_deref())?;
            // Each chunk is laid out contiguously in `ColumnId::schema` order
            // (`business_key`, `sys_from`, `payload`, the three provenance
            // columns, then the valid-time pair when present) — there is no
            // stored `sys_to` (v6, [ADR-0023]). The footer records the absolute
            // offset, so the reader projects exactly the columns it wants.
            let length = (CHUNK_HEADER_LEN + encoded.payload.len()) as u64;
            chunks.push(EncodedChunk {
                col,
                offset,
                length,
                value_count: version_count,
                payload: encoded.payload,
                stat_min: encoded.stat_min,
                stat_max: encoded.stat_max,
            });
            offset += length;
        }

        // Retraction tombstones (format v7, STL-143) follow the version
        // row-group as their own columnar chunks — emitted only when the segment
        // holds at least one delete (the optional-columns pattern, like the
        // valid-time pair). They carry their *own* value count (the number of
        // retractions, independent of the version row count), laid out
        // contiguously after the version chunks so offsets stay monotonic.
        let mut retraction_chunks: Vec<EncodedChunk> = Vec::new();
        let retraction_count = u32::try_from(self.retractions.len())
            .map_err(|_| SegmentError::TooLarge("retraction count exceeds u32::MAX"))?;
        if !self.retractions.is_empty() {
            for &col in &ColumnId::RETRACTION {
                let encoded = encode_retraction_column(col, &self.retractions)?;
                let length = (CHUNK_HEADER_LEN + encoded.payload.len()) as u64;
                retraction_chunks.push(EncodedChunk {
                    col,
                    offset,
                    length,
                    value_count: retraction_count,
                    payload: encoded.payload,
                    stat_min: encoded.stat_min,
                    stat_max: encoded.stat_max,
                });
                offset += length;
            }
        }

        // Emit every chunk in declaration order — version row-group first, then
        // the retraction section. Each chunk header is self-checksummed:
        // `chunk_crc` covers `(header[0..12] || payload)`, so a flip anywhere
        // except the CRC field itself is detected — and a flip in the CRC field
        // is detected as a mismatch.
        for chunk in chunks.iter().chain(&retraction_chunks) {
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

        let footer = encode_footer(self.rows.len(), &chunks, &retraction_chunks)?;
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

fn encode_column(
    col: ColumnId,
    rows: &[Version],
    valid_pairs: Option<&[(i64, i64)]>,
) -> Result<EncodedColumn, SegmentError> {
    match col.ty() {
        ColumnType::Bytes => {
            // `valid_pairs` is `Some` exactly for a valid-time segment, where
            // the `payload` column stores only the bare user payload — the
            // 16-byte interval prefix is carried by the valid_from / valid_to
            // columns instead ([STL-119]). `extract_bytes` strips it for the
            // `Payload` column when that holds.
            let valid_time = valid_pairs.is_some();
            // A `None` here is a SQL `NULL` cell (only the `Payload` column ever
            // yields one); `encode_bytes_values` writes the reserved sentinel for
            // it ([STL-154]).
            let mut vals: Vec<Option<&[u8]>> = Vec::with_capacity(rows.len());
            for row in rows {
                vals.push(extract_bytes(col, row, valid_time)?);
            }
            encode_bytes_values(vals.into_iter())
        }
        ColumnType::I64 => {
            // Stream the per-row values straight into the encoder — no
            // intermediate `Vec<i64>`. `rows.iter().enumerate().map(..)` is an
            // `ExactSizeIterator`, so `encode_i64_values` still reserves the full
            // payload from `size_hint` up front. The `valid_from` / `valid_to`
            // columns read from the prefix decoded once up front
            // (`decode_valid_pairs`); every other i64 column reads a `Version`
            // field directly.
            Ok(encode_i64_values(rows.iter().enumerate().map(
                |(i, row)| match col {
                    ColumnId::ValidFrom => {
                        valid_pairs.expect("valid-time schema carries decoded pairs")[i].0
                    }
                    ColumnId::ValidTo => {
                        valid_pairs.expect("valid-time schema carries decoded pairs")[i].1
                    }
                    _ => extract_i64(col, row),
                },
            )))
        }
    }
}

/// Encode one retraction-section column (format v7, STL-143) from the buffered
/// [`Close`] tombstones. Shares the plain bytes/i64 layout *and* the bounded
/// zone-stat logic with the version columns via [`encode_bytes_values`] /
/// [`encode_i64_values`], so a tombstone column prunes through the same zone map
/// with no special-casing.
//
// `txn_id.0 as i64` is the same lossless bit reinterpretation as the version
// `TxnId` column (see `ColumnId::TxnId`); the reader reverses it with `as u64`.
#[allow(clippy::cast_possible_wrap)]
fn encode_retraction_column(
    col: ColumnId,
    closes: &[Close],
) -> Result<EncodedColumn, SegmentError> {
    match col {
        // Retraction tombstone bytes columns are never NULL — wrap each value as
        // present so it shares the `Option`-aware bytes encoder ([STL-154]).
        ColumnId::RetractKey => {
            encode_bytes_values(closes.iter().map(|c| Some(c.business_key.as_bytes())))
        }
        ColumnId::RetractClosedByPrincipal => encode_bytes_values(
            closes
                .iter()
                .map(|c| Some(c.closed_by.principal.as_bytes())),
        ),
        ColumnId::RetractSysFrom => Ok(encode_i64_values(closes.iter().map(|c| c.sys_from.0))),
        // `seq` is a u64; store its bits in the i64 column (lossless round-trip —
        // see `ColumnId::RetractSeq`, same reinterpretation as `TxnId`).
        ColumnId::RetractSeq => Ok(encode_i64_values(closes.iter().map(|c| c.seq as i64))),
        ColumnId::RetractClosedAt => Ok(encode_i64_values(closes.iter().map(|c| c.sys_to.0))),
        ColumnId::RetractClosedByTxn => Ok(encode_i64_values(
            closes.iter().map(|c| c.closed_by.txn_id.0 as i64),
        )),
        ColumnId::RetractClosedByCommittedAt => Ok(encode_i64_values(
            closes.iter().map(|c| c.closed_by.committed_at.0),
        )),
        _ => unreachable!("not a retraction column"),
    }
}

/// Encode a bytes column: plain `[u32 len][bytes]` per value, plus a bounded
/// lex-min/max prefix stat.
///
/// Min/max stats are a bounded *prefix* of the lex-min and lex-max byte values;
/// the catalog will later attach a column-level comparator, but at the format
/// layer bytewise order is the natural choice (it matches how `BusinessKey`
/// already sorts via `Vec<u8>`'s Ord).
///
/// Every bytes column can be an unbounded blob — `Payload` runs up to
/// `MAX_VERSION_FRAME_LEN` (16 MiB) per row, and `BusinessKey` / `Principal` are
/// only bounded by the same frame ceiling — so inlining a full lex-min/max would
/// let one row push the footer past the `u32` `footer_len` limit. Instead we
/// record a bounded prefix capped at `MAX_BYTES_STAT_PREFIX_LEN`: the min prefix
/// is truncated *down* and the max prefix is rounded *up*, so the `[min, max]`
/// envelope stays a superset of the real value range and `might_contain` keeps
/// its no-false-negatives contract. This caps every bytes column's footer cost
/// regardless of value size.
fn encode_bytes_values<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
) -> Result<EncodedColumn, SegmentError> {
    let mut payload = Vec::new();
    let mut min: Option<&[u8]> = None;
    let mut max: Option<&[u8]> = None;
    for value in values {
        let Some(bytes) = value else {
            // SQL `NULL`: the reserved sentinel length, no value bytes, and no
            // contribution to the zone-map min/max ([STL-154]).
            payload.extend_from_slice(&BYTES_NULL_SENTINEL.to_le_bytes());
            continue;
        };
        let len = u32::try_from(bytes.len())
            .map_err(|_| SegmentError::TooLarge("value length exceeds u32::MAX in one chunk"))?;
        // A present value can never reach the sentinel length — the frame ceiling
        // (`MAX_VERSION_FRAME_LEN`, 16 MiB) caps it far below `u32::MAX` — so a
        // present cell and a NULL cell are always distinguishable on read.
        debug_assert_ne!(
            len, BYTES_NULL_SENTINEL,
            "present value reached NULL sentinel length"
        );
        payload.extend_from_slice(&len.to_le_bytes());
        payload.extend_from_slice(bytes);
        min = Some(min.map_or(bytes, |m| if bytes < m { bytes } else { m }));
        max = Some(max.map_or(bytes, |m| if bytes > m { bytes } else { m }));
    }
    Ok(EncodedColumn {
        payload,
        stat_min: min.map(bounded_min_prefix).unwrap_or_default(),
        stat_max: max.map(bounded_max_prefix).unwrap_or_default(),
    })
}

/// Encode an `i64` column: plain 8 LE bytes per value, plus the min/max as 8 LE
/// bytes each. An empty column emits zero-length stat fields (the "no stats"
/// sentinel).
fn encode_i64_values(values: impl Iterator<Item = i64>) -> EncodedColumn {
    // 8 LE bytes per value — reserve from the iterator's lower bound to avoid
    // re-growing the payload on a large column (the version path used to size
    // this from `rows.len()`).
    let mut payload = Vec::with_capacity(values.size_hint().0.saturating_mul(8));
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for v in values {
        payload.extend_from_slice(&v.to_le_bytes());
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
    }
    EncodedColumn {
        payload,
        stat_min: min.map(|v| v.to_le_bytes().to_vec()).unwrap_or_default(),
        stat_max: max.map(|v| v.to_le_bytes().to_vec()).unwrap_or_default(),
    }
}

/// Truncate a lex-min byte value *down* to a bounded prefix for the footer
/// stat. A byte prefix is lex-`<=` the value it came from, so the prefix is a
/// sound lower bound for every value in the column — pruning against it can
/// never drop a real match. An empty result (the min value is itself empty)
/// encodes as the footer's zero-length "no stats" sentinel; because the reader
/// records a zone entry only when *both* bounds are present (`ZoneMap::from_bounds`),
/// that drops the column's zone for the segment entirely — no pruning on either
/// side. Conservative, never wrong.
fn bounded_min_prefix(value: &[u8]) -> Vec<u8> {
    value[..value.len().min(MAX_BYTES_STAT_PREFIX_LEN)].to_vec()
}

/// Round a lex-max byte value *up* to a bounded prefix that stays `>=` the
/// value. If the value already fits within the cap it is its own exact upper
/// bound; otherwise keep the first `MAX_BYTES_STAT_PREFIX_LEN` bytes and
/// increment them — drop any trailing `0xFF` bytes and bump the last byte below
/// `0xFF` — so the result is `>=` every value sharing that prefix. A prefix that
/// is *all* `0xFF` has no shorter upper bound representable, so it encodes as the
/// zero-length "no stats" sentinel; as with an empty min, the column then records
/// no zone entry for the segment (an entry needs both bounds), so it never prunes
/// at all — still conservative, never a false negative.
fn bounded_max_prefix(value: &[u8]) -> Vec<u8> {
    if value.len() <= MAX_BYTES_STAT_PREFIX_LEN {
        return value.to_vec();
    }
    let mut prefix = value[..MAX_BYTES_STAT_PREFIX_LEN].to_vec();
    while let Some(last) = prefix.last_mut() {
        if *last < u8::MAX {
            *last += 1;
            return prefix;
        }
        prefix.pop();
    }
    Vec::new()
}

// `None` is a SQL `NULL` cell — only the `Payload` column can produce one; every
// other bytes column is always present, returned as `Some` ([STL-154]).
fn extract_bytes(
    col: ColumnId,
    row: &Version,
    valid_time: bool,
) -> Result<Option<&[u8]>, SegmentError> {
    match col {
        ColumnId::BusinessKey => Ok(Some(row.business_key.as_bytes())),
        // On a valid-time segment the interval lives in the valid_from /
        // valid_to columns, so the payload column stores only the bare user
        // payload — strip the 16-byte prefix rather than persist it twice
        // ([STL-119]). `decode_valid_pairs` already decoded *and validated*
        // every row's interval up front, so here we only need to drop the fixed
        // prefix length — slice it off directly rather than re-parse and
        // re-validate the interval per row on the flush hot path. The `get`
        // still guards a truncated payload as `Corrupt`. A system-only segment
        // stores the payload verbatim. A `None` payload (SQL `NULL`) is carried
        // through as `None`; it never reaches the valid-time branch because a
        // valid-time row's payload always carries the interval prefix.
        ColumnId::Payload => match &row.payload {
            None => Ok(None),
            Some(bytes) if valid_time => {
                bytes
                    .get(VALID_TIME_PREFIX_LEN..)
                    .map(Some)
                    .ok_or(SegmentError::Corrupt(
                        "valid-time payload shorter than its interval prefix",
                    ))
            }
            Some(bytes) => Ok(Some(bytes)),
        },
        ColumnId::Principal => Ok(Some(row.provenance.principal.as_bytes())),
        ColumnId::SysFrom
        | ColumnId::Seq
        | ColumnId::TxnId
        | ColumnId::CommittedAt
        | ColumnId::ValidFrom
        | ColumnId::ValidTo => {
            unreachable!("not a bytes column")
        }
        ColumnId::RetractKey
        | ColumnId::RetractSysFrom
        | ColumnId::RetractSeq
        | ColumnId::RetractClosedAt
        | ColumnId::RetractClosedByTxn
        | ColumnId::RetractClosedByCommittedAt
        | ColumnId::RetractClosedByPrincipal => {
            unreachable!("retraction columns are encoded via encode_retraction_column")
        }
    }
}

// `txn_id.0 as i64` is an intentional, lossless bit reinterpretation (the
// reader reverses it with `as u64`); the wrap is the point, not a hazard.
#[allow(clippy::cast_possible_wrap)]
fn extract_i64(col: ColumnId, row: &Version) -> i64 {
    match col {
        ColumnId::SysFrom => row.sys_from.0,
        // `seq` is a u64; store its bits in the i64 column (lossless round-trip —
        // see `ColumnId::Seq`, same reinterpretation as `TxnId`).
        ColumnId::Seq => row.seq as i64,
        // `txn_id` is a u64; store its bits in the i64 column (lossless
        // round-trip — see `ColumnId::TxnId`).
        ColumnId::TxnId => row.provenance.txn_id.0 as i64,
        ColumnId::CommittedAt => row.provenance.committed_at.0,
        // The valid-time columns are not `Version` fields — they are lifted
        // from the payload prefix by `decode_valid_pairs`, which the caller
        // reads from directly.
        ColumnId::ValidFrom | ColumnId::ValidTo => {
            unreachable!("valid-time columns are extracted via decode_valid_pairs")
        }
        ColumnId::BusinessKey | ColumnId::Payload | ColumnId::Principal => {
            unreachable!("not an i64 column")
        }
        ColumnId::RetractKey
        | ColumnId::RetractSysFrom
        | ColumnId::RetractSeq
        | ColumnId::RetractClosedAt
        | ColumnId::RetractClosedByTxn
        | ColumnId::RetractClosedByCommittedAt
        | ColumnId::RetractClosedByPrincipal => {
            unreachable!("retraction columns are encoded via encode_retraction_column")
        }
    }
}

// Decode every row's 16-byte valid-time prefix into `(valid_from, valid_to)`
// once, so `finish` can emit both the valid_from and valid_to columns without
// re-parsing the payload per column. Only called for a valid-time table's
// segment, where every payload was framed by [`crate::validtime::frame_payload`]
// and therefore carries the prefix; a frame that fails to decode (too short, or
// an inverted/invalid interval) is a malformed input row, surfaced as `Corrupt`.
fn decode_valid_pairs(rows: &[Version]) -> Result<Vec<(i64, i64)>, SegmentError> {
    rows.iter()
        .map(|row| {
            // A valid-time row's payload always carries the interval prefix, so a
            // `None` (SQL `NULL`) payload here is a malformed row ([STL-154]).
            let stored = row.payload.as_deref().ok_or(SegmentError::Corrupt(
                "valid-time row has a NULL payload, which cannot carry a valid-time interval",
            ))?;
            let (interval, _user) = unframe_payload(true, stored).map_err(|_| {
                SegmentError::Corrupt(
                    "valid-time payload could not be decoded into valid_from/valid_to columns",
                )
            })?;
            let interval = interval.expect("valid-time enabled ⇒ unframe yields an interval");
            Ok((interval.from.0, interval.to.0))
        })
        .collect()
}

fn encode_footer(
    row_count: usize,
    chunks: &[EncodedChunk],
    retraction_chunks: &[EncodedChunk],
) -> Result<Vec<u8>, SegmentError> {
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
        encode_chunk_meta(&mut out, chunk)?;
    }
    // Retraction section (format v7, STL-143): a count of tombstone rows (the
    // shared value_count for every retraction column) followed by that many
    // column-chunk metas. `0` columns when the segment holds no deletes — the
    // section is always present, just empty, so a v7 reader parses it
    // unconditionally.
    let retraction_count = retraction_chunks.first().map_or(0, |c| c.value_count);
    let retraction_column_count = u32::try_from(retraction_chunks.len())
        .map_err(|_| SegmentError::TooLarge("retraction column count exceeds u32::MAX"))?;
    out.extend_from_slice(&retraction_count.to_le_bytes());
    out.extend_from_slice(&retraction_column_count.to_le_bytes());
    for chunk in retraction_chunks {
        encode_chunk_meta(&mut out, chunk)?;
    }
    Ok(out)
}

/// Append one column-chunk's footer entry: id, codec, reserved, absolute offset,
/// length, value count, reserved, then the length-prefixed min/max zone stats.
/// Shared by the version row-group and the retraction section so the two never
/// drift in layout.
fn encode_chunk_meta(out: &mut Vec<u8>, chunk: &EncodedChunk) -> Result<(), SegmentError> {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Unit tests for the bounded-prefix bytes-stat helpers. The cross-module
    //! proof that these bounds never prune a real match lives in the seed-swept
    //! oracle (`tests/zone_map.rs`); these pin the rounding arithmetic — the
    //! easy place to get an off-by-one that silently produces a too-tight
    //! upper bound.

    use super::{MAX_BYTES_STAT_PREFIX_LEN, bounded_max_prefix, bounded_min_prefix};

    #[test]
    fn short_values_round_trip_exactly() {
        // Values within the cap are their own exact bounds — no truncation,
        // no rounding — so pruning stays as precise as the old full min/max.
        assert_eq!(bounded_min_prefix(b"apple"), b"apple");
        assert_eq!(bounded_max_prefix(b"apple"), b"apple");
        // A value exactly at the cap is still exact (boundary, no truncation).
        let at_cap = vec![b'a'; MAX_BYTES_STAT_PREFIX_LEN];
        assert_eq!(bounded_min_prefix(&at_cap), at_cap);
        assert_eq!(bounded_max_prefix(&at_cap), at_cap);
    }

    #[test]
    fn min_prefix_truncates_down() {
        // One byte over the cap: the min keeps the first cap bytes verbatim —
        // a prefix is lex-<= its source, a sound lower bound.
        let value = vec![b'z'; MAX_BYTES_STAT_PREFIX_LEN + 1];
        let min = bounded_min_prefix(&value);
        assert_eq!(min.len(), MAX_BYTES_STAT_PREFIX_LEN);
        assert!(min.as_slice() <= value.as_slice());
    }

    #[test]
    fn max_prefix_rounds_up_past_the_value() {
        // Over-cap value: the rounded prefix must be strictly greater than the
        // full value so it stays a sound upper bound.
        let mut value = vec![b'm'; MAX_BYTES_STAT_PREFIX_LEN + 5];
        value[MAX_BYTES_STAT_PREFIX_LEN] = b'm'; // suffix shares the prefix byte
        let max = bounded_max_prefix(&value);
        assert!(max.len() <= MAX_BYTES_STAT_PREFIX_LEN);
        assert!(
            max.as_slice() > value.as_slice(),
            "rounded max {max:?} must exceed the source value"
        );
    }

    #[test]
    fn max_prefix_carries_over_trailing_ff() {
        // The first cap bytes end in 0xFF: incrementing must carry — drop the
        // 0xFF tail and bump the last byte below it. Result still >= value.
        let mut prefix = vec![b'k'; MAX_BYTES_STAT_PREFIX_LEN];
        prefix[MAX_BYTES_STAT_PREFIX_LEN - 1] = 0xFF;
        prefix[MAX_BYTES_STAT_PREFIX_LEN - 2] = 0xFF;
        let mut value = prefix.clone();
        value.extend_from_slice(b"anything");
        let max = bounded_max_prefix(&value);
        // Carried two bytes: length cap-2, last byte bumped from b'k' to b'k'+1.
        assert_eq!(max.len(), MAX_BYTES_STAT_PREFIX_LEN - 2);
        assert_eq!(*max.last().unwrap(), b'k' + 1);
        assert!(max.as_slice() > value.as_slice());
    }

    #[test]
    fn max_prefix_all_ff_has_no_bound() {
        // An all-0xFF prefix has no shorter upper bound — emit the empty "no
        // stats" sentinel, which makes the column simply not prune on its max.
        let value = vec![0xFFu8; MAX_BYTES_STAT_PREFIX_LEN + 3];
        assert!(bounded_max_prefix(&value).is_empty());
    }

    #[test]
    fn bounds_stay_ordered_for_over_cap_values() {
        // For any value, the truncated-down min never exceeds the rounded-up
        // max — the zone envelope is never inverted.
        let value = vec![0x7Fu8; MAX_BYTES_STAT_PREFIX_LEN + 10];
        let min = bounded_min_prefix(&value);
        let max = bounded_max_prefix(&value);
        assert!(!max.is_empty(), "0x7F prefix rounds up cleanly");
        assert!(min.as_slice() <= max.as_slice());
    }
}
