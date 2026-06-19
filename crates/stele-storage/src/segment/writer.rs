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

use std::collections::HashMap;

use crate::backend::{Disk, DiskFile};
use crate::bloom::{DEFAULT_BITS_PER_KEY, KeyBloom};
use crate::checksum::crc32c;
use crate::delta::Version;
use crate::validity::Close;
use crate::validtime::{
    DEFAULT_VALID_INTERVAL_CAP, VALID_TIME_PREFIX_LEN, ValidIntervalSummary, unframe_payload,
};

use super::SegmentError;
use super::format::{
    BYTES_NULL_SENTINEL, CHUNK_HEADER_LEN, Codec, ColumnId, ColumnType, FOOTER_FLAG_BLOOM,
    FOOTER_FLAG_VALID_INTERVALS, FORMAT_VERSION, HEADER_LEN, HEADER_MAGIC,
    MAX_BYTES_STAT_PREFIX_LEN, SCHEMA_ID_IMPLICIT_VERSION, STAT_MAX_UNBOUNDED, STAT_MIN_UNBOUNDED,
    TRAILER_LEN, TRAILER_MAGIC,
};

/// Streaming writer over a single sealed-segment file.
///
/// All pushed rows are held in memory until [`finish`](Self::finish) drains
/// them into chunks. By default the writer emits exactly one row-group per
/// segment (the v0.1 shape); [`with_max_row_group_rows`](Self::with_max_row_group_rows)
/// bounds each row-group so a wide segment splits into several, which is what
/// lets the read path skip the chunks of row-groups holding no live row
/// ([`SegmentReader::read_column_in_row_groups`](super::SegmentReader::read_column_in_row_groups),
/// [STL-155]). The on-disk footer has enumerated row-groups since v1, so the
/// split needs no format-version bump.
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
    /// Upper bound on rows per row-group ([STL-155]). `None` — the default —
    /// keeps the v0.1 shape: every pushed row lands in one row-group, so the
    /// emitted bytes are identical to what earlier writers produced.
    max_row_group_rows: Option<usize>,
    /// Bits per key for the per-segment business-key bloom filter ([STL-238],
    /// format v11). Defaults to [`DEFAULT_BITS_PER_KEY`]; `0` disables the bloom
    /// (the footer's [`FOOTER_FLAG_BLOOM`] stays clear). Higher values trade
    /// footer bytes for a lower false-positive rate — the configurable bound the
    /// hash/bloom index family exposes.
    bloom_bits_per_key: usize,
    /// Cap on the per-segment valid-time interval summary ([STL-241], format
    /// v12). Defaults to [`DEFAULT_VALID_INTERVAL_CAP`]; `0` disables the summary
    /// (the footer's [`FOOTER_FLAG_VALID_INTERVALS`] stays clear). Only ever
    /// written for a valid-time segment — a system-only table has no valid
    /// windows to summarize.
    valid_interval_cap: usize,
    /// Whether to consider per-column dictionary encoding ([STL-250], format
    /// v13). `false` — the default — emits every column [`Codec::Plain`], so a
    /// segment is byte-identical to a v12 one apart from the header version. When
    /// `true`, each bytes column is encoded as a dictionary *or* plain, whichever
    /// is smaller (the writer-by-statistics choice of architecture §3.2), so a
    /// value repeated across a key's version chain is stored once. Enabled by
    /// compaction ([`crate::engine::Engine::compact`]) — the natural place to
    /// spend the CPU consolidating history — and left off for the latency-bound
    /// flush path.
    dictionary: bool,
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
            max_row_group_rows: None,
            bloom_bits_per_key: DEFAULT_BITS_PER_KEY,
            valid_interval_cap: DEFAULT_VALID_INTERVAL_CAP,
            dictionary: false,
        })
    }

    /// Enable (or disable) per-column dictionary encoding ([STL-250], format
    /// v13). When on, each bytes column is stored as a dictionary of its distinct
    /// values plus a narrow per-row code *when that is smaller* than the plain
    /// layout, and verbatim otherwise — the writer-by-statistics choice never
    /// makes a chunk larger, so a value repeated across a key's version chain is
    /// stored once but an all-distinct column is unaffected. Off by default,
    /// which keeps a segment byte-identical to v12 apart from the header version;
    /// compaction turns it on. Purely a writer-side choice — a reader decodes
    /// either codec transparently — so toggling it changes a segment's *size*,
    /// never the values it reads back (the equivalence the STL-250 oracle pins).
    #[must_use]
    pub const fn with_dictionary(mut self, enabled: bool) -> Self {
        self.dictionary = enabled;
        self
    }

    /// Size (or disable) the per-segment business-key bloom filter ([STL-238]).
    /// `bits` bits per key trades footer size against the false-positive rate
    /// (the default is ~12 bits/key, near a 1% rate); `0` disables the bloom
    /// entirely, leaving the footer byte-identical to a pre-v11 segment apart from
    /// the header version. Purely a writer-side choice — a reader admits any
    /// segment whether or not it carries a bloom.
    #[must_use]
    pub const fn with_bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    /// Cap (or disable) the per-segment valid-time interval summary ([STL-241],
    /// format v12). The summary coalesces the segment's `[valid_from, valid_to)`
    /// windows into at most `cap` disjoint intervals (merging the smallest gaps
    /// when there are more — a sound widening); `0` disables it, leaving the
    /// footer's `FOOTER_FLAG_VALID_INTERVALS` clear so a valid-pinned read
    /// full-scans the segment on the valid axis. Only ever written for a
    /// valid-time segment. Purely a writer-side choice — a reader admits any
    /// segment whether or not it carries a summary, so toggling it changes scan
    /// *speed*, never results (the STL-241 equivalence the oracle pins).
    #[must_use]
    pub const fn with_valid_interval_cap(mut self, cap: usize) -> Self {
        self.valid_interval_cap = cap;
        self
    }

    /// Bound each row-group to at most `rows` rows, so
    /// [`finish`](Self::finish) splits the buffered rows into several
    /// row-groups instead of the single default one ([STL-155]). A bounded
    /// segment lets the read path skip whole row-groups that hold no live row
    /// ([`SegmentReader::read_column_in_row_groups`](super::SegmentReader::read_column_in_row_groups));
    /// the footer has enumerated row-groups since v1, so the split is not a
    /// format change. A `rows` of `0` is clamped to `1` (the same clamp
    /// `SnapshotScan::into_source` applies to its batch size).
    #[must_use]
    pub fn with_max_row_group_rows(mut self, rows: usize) -> Self {
        self.max_row_group_rows = Some(rows.max(1));
        self
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

    /// Seal the segment: emit every buffered row as row-groups (one by
    /// default; several when [`with_max_row_group_rows`](Self::with_max_row_group_rows)
    /// bounded them), then write the footer and trailer and `sync`. After
    /// return the file is immutable in the format's sense — no writer API can
    /// reach it.
    /// The per-segment business-key bloom ([STL-238], format v11): a membership
    /// filter over every version's business key, so a point / `MERGE` probe can
    /// skip this whole segment when the bloom proves the key absent — the
    /// random/hash-key case zone maps cannot prune. `None` when the bloom is
    /// disabled (`bits_per_key == 0`) or the segment holds no version — an empty
    /// segment has no keys to summarize and is already pruned by its zone maps.
    fn business_key_bloom(&self) -> Option<KeyBloom> {
        (self.bloom_bits_per_key > 0 && !self.rows.is_empty()).then(|| {
            KeyBloom::build(
                self.bloom_bits_per_key,
                self.rows.iter().map(|v| v.business_key.as_bytes()),
            )
        })
    }

    /// The per-segment valid-time interval summary ([STL-241], format v12): the
    /// coalesced union of every row's `[valid_from, valid_to)` window (already
    /// decoded once into `valid_pairs`), so a `FOR VALID_TIME AS OF v` read can
    /// skip this whole segment when `v` falls in a coverage gap — the backdated
    /// scatter case zone maps cannot prune. `None` when the summary is disabled
    /// (`valid_interval_cap == 0`), the table is system-only (`valid_pairs` is
    /// `None`), or the segment holds no row. Also `None` in the (unreachable from
    /// well-formed valid-time DML, where every window is `from < to`) case that
    /// every window is degenerate: an empty summary covers nothing and would
    /// wrongly prune the segment, so the section is omitted and the valid axis
    /// full-scans rather than dropping rows.
    fn valid_interval_summary(
        &self,
        valid_pairs: Option<&[(i64, i64)]>,
    ) -> Option<ValidIntervalSummary> {
        if self.valid_interval_cap == 0 {
            return None;
        }
        let pairs = valid_pairs?;
        if pairs.is_empty() {
            return None;
        }
        let summary = ValidIntervalSummary::build(pairs.iter().copied(), self.valid_interval_cap);
        (!summary.is_empty()).then_some(summary)
    }

    #[allow(clippy::too_many_lines)] // one sequential seal: encode columns → write chunks → footer → trailer → sync
    pub fn finish(mut self) -> Result<(), SegmentError> {
        // Per-column buffers. Row order is preserved: column i's k-th value
        // came from `self.rows[k]`. A valid-time table's schema carries the
        // two extra `valid_from` / `valid_to` columns ([STL-117]).
        let schema = ColumnId::schema(self.valid_time);
        // Decode each row's valid-time prefix exactly once up front (not once
        // per valid-time column), so emitting both valid_from and valid_to
        // re-uses the same parse ([STL-117]). Decoded across all rows, then
        // sliced per row-group below, so the split does not re-parse either.
        let valid_pairs: Option<Vec<(i64, i64)>> = if self.valid_time {
            Some(decode_valid_pairs(&self.rows)?)
        } else {
            None
        };
        // Partition the buffered rows into row-groups ([STL-155]). The unbounded
        // default produces the single row-group every earlier writer emitted; an
        // empty segment keeps its one empty row-group rather than zero, so the
        // empty-segment footer shape is unchanged too.
        let group_rows = self.max_row_group_rows.unwrap_or(usize::MAX);
        let groups: Vec<&[Version]> = if self.rows.is_empty() {
            vec![&[][..]]
        } else {
            self.rows.chunks(group_rows).collect()
        };
        let mut row_groups: Vec<RowGroupChunks> = Vec::with_capacity(groups.len());
        let mut offset: u64 = HEADER_LEN as u64;
        let mut row_base = 0usize;
        for group in groups {
            let version_count = u32::try_from(group.len()).map_err(|_| {
                SegmentError::TooLarge("row count exceeds u32::MAX in one row-group")
            })?;
            let group_pairs = valid_pairs
                .as_deref()
                .map(|pairs| &pairs[row_base..row_base + group.len()]);
            let mut chunks: Vec<EncodedChunk> = Vec::with_capacity(schema.len());
            for &col in schema {
                let encoded = encode_column(col, group, group_pairs, self.dictionary)?;
                // Within a row-group each chunk is laid out contiguously in
                // `ColumnId::schema` order (`business_key`, `sys_from`,
                // `payload`, the three provenance columns, then the valid-time
                // pair when present) — there is no stored `sys_to` (v6,
                // [ADR-0023]); row-groups follow one another in row order. The
                // footer records the absolute offset, so the reader projects
                // exactly the columns (and row-groups) it wants.
                let length = (CHUNK_HEADER_LEN + encoded.payload.len()) as u64;
                chunks.push(EncodedChunk {
                    col,
                    codec: encoded.codec,
                    offset,
                    length,
                    value_count: version_count,
                    payload: encoded.payload,
                    stat_min: encoded.stat_min,
                    stat_max: encoded.stat_max,
                });
                offset += length;
            }
            row_groups.push(RowGroupChunks {
                row_count: version_count,
                chunks,
            });
            row_base += group.len();
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
                let encoded = encode_retraction_column(col, &self.retractions, self.dictionary)?;
                let length = (CHUNK_HEADER_LEN + encoded.payload.len()) as u64;
                retraction_chunks.push(EncodedChunk {
                    col,
                    codec: encoded.codec,
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

        // Emit every chunk in declaration order — version row-groups first (in
        // row order), then the retraction section. Each chunk header is
        // self-checksummed: `chunk_crc` covers `(header[0..12] || payload)`, so
        // a flip anywhere except the CRC field itself is detected — and a flip
        // in the CRC field is detected as a mismatch.
        for chunk in row_groups
            .iter()
            .flat_map(|rg| rg.chunks.iter())
            .chain(&retraction_chunks)
        {
            let mut header = Vec::with_capacity(CHUNK_HEADER_LEN);
            let payload_len = u32::try_from(chunk.payload.len()).map_err(|_| {
                SegmentError::TooLarge("column-chunk payload exceeds u32::MAX bytes")
            })?;
            header.extend_from_slice(&payload_len.to_le_bytes());
            header.extend_from_slice(&chunk.value_count.to_le_bytes());
            header.push(chunk.codec.as_byte());
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

        let bloom = self.business_key_bloom();
        let valid_summary = self.valid_interval_summary(valid_pairs.as_deref());
        let footer = encode_footer(
            &row_groups,
            &retraction_chunks,
            bloom.as_ref(),
            valid_summary.as_ref(),
        )?;
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

/// One end (min or max) of a column's encoded zone-map stat, before it is laid
/// into the footer column entry.
///
/// The footer encodes a stat field as a length-prefixed byte run; [`Self::Absent`]
/// and [`Self::Unbounded`] both write zero bytes and are told apart by the
/// per-entry stat-presence flag ([STL-120]): an absent stat leaves the flag bit
/// clear (the classic "no stats" sentinel), an unbounded stat sets it.
enum StatBound {
    /// No statistic for this end — the column had no (non-NULL) values. Encodes
    /// as a zero-length field with the flag bit clear.
    Absent,
    /// A concrete bound: the length-prefixed bytes (lex prefix for a bytes
    /// column) or the 8 LE bytes of an `i64` bound.
    Value(Vec<u8>),
    /// A present but *open* end — −∞ for a min, +∞ for a max ([STL-120]). Arises
    /// only for a bounded-prefix bytes column whose prefix degenerates (empty
    /// lex-min, or an all-`0xFF` max with no shorter upper bound). Encodes as a
    /// zero-length field with the flag bit set.
    Unbounded,
}

impl StatBound {
    /// The flag bit this end contributes to the column entry's stat-flags byte:
    /// `unbounded_bit` when open, otherwise none.
    const fn flag(&self, unbounded_bit: u8) -> u8 {
        match self {
            Self::Unbounded => unbounded_bit,
            Self::Absent | Self::Value(_) => 0,
        }
    }

    /// The length-prefixed stat bytes — empty for an absent or unbounded end.
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Value(v) => v,
            Self::Absent | Self::Unbounded => &[],
        }
    }
}

struct EncodedColumn {
    /// The codec the encoder chose for this column's payload — [`Codec::Plain`]
    /// or, when dictionary encoding was enabled *and* smaller, [`Codec::Dict`]
    /// ([STL-250]). Stamped into both the chunk header and the footer entry so the
    /// reader dispatches the matching decoder.
    codec: Codec,
    payload: Vec<u8>,
    stat_min: StatBound,
    stat_max: StatBound,
}

struct EncodedChunk {
    col: ColumnId,
    codec: Codec,
    offset: u64,
    length: u64,
    value_count: u32,
    payload: Vec<u8>,
    stat_min: StatBound,
    stat_max: StatBound,
}

/// One encoded row-group: its row count and its per-column chunks, in
/// `ColumnId::schema` order ([STL-155]).
struct RowGroupChunks {
    row_count: u32,
    chunks: Vec<EncodedChunk>,
}

fn encode_column(
    col: ColumnId,
    rows: &[Version],
    valid_pairs: Option<&[(i64, i64)]>,
    use_dict: bool,
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
            encode_bytes_values(vals.into_iter(), use_dict)
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
    use_dict: bool,
) -> Result<EncodedColumn, SegmentError> {
    match col {
        // Retraction tombstone bytes columns are never NULL — wrap each value as
        // present so it shares the `Option`-aware bytes encoder ([STL-154]).
        ColumnId::RetractKey => encode_bytes_values(
            closes.iter().map(|c| Some(c.business_key.as_bytes())),
            use_dict,
        ),
        ColumnId::RetractClosedByPrincipal => encode_bytes_values(
            closes
                .iter()
                .map(|c| Some(c.closed_by.principal.as_bytes())),
            use_dict,
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

/// Encode a bytes column, choosing the smaller of plain and dictionary layout.
///
/// **Plain** is `[u32 len][bytes]` per value (a NULL writes [`BYTES_NULL_SENTINEL`]
/// and no body — [STL-154]). **Dictionary** ([`Codec::Dict`], [STL-250]) stores
/// the distinct values once — `[u8 code_width][u32 dict_count][(u32 len, bytes) ×
/// dict_count][code × value_count]` — so a value repeated across a key's version
/// chain (the *identical* `business_key`, a repeated `principal` / `payload`)
/// costs one dictionary entry plus a narrow code per row instead of being
/// re-stored wholesale. The writer keeps the dictionary only when `use_dict` is
/// set *and* it is strictly smaller than plain, so an all-distinct column never
/// grows — the "chosen by the writer from column statistics" rule of
/// [architecture §3.2](../../../../../docs/02-architecture.md#32-on-disk-segment-format).
///
/// The zone-map min/max stats are a bounded *prefix* of the lex-min and lex-max
/// byte values, computed over the **logical** values and therefore identical for
/// either codec — pruning is unaffected. (At the format layer bytewise order is
/// the natural choice; it matches how `BusinessKey` sorts via `Vec<u8>`'s Ord.)
/// Every bytes column can be an unbounded blob — `Payload` runs up to
/// `MAX_VERSION_FRAME_LEN` (16 MiB) per row, `BusinessKey` / `Principal` only by
/// the same ceiling — so inlining a full lex-min/max could push the footer past
/// its `u32` `footer_len` limit. Instead we record a bounded prefix capped at
/// `MAX_BYTES_STAT_PREFIX_LEN`: the min prefix is truncated *down* and the max
/// rounded *up*, so the `[min, max]` envelope stays a superset and `might_contain`
/// keeps its no-false-negatives contract, regardless of value size.
fn encode_bytes_values<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
    use_dict: bool,
) -> Result<EncodedColumn, SegmentError> {
    // Materialize once: the zone-map pass, the plain build, and the optional
    // dictionary build each look at the values, and the iterator borrows from the
    // row buffer (no copies). The values live exactly as long as this call.
    let values: Vec<Option<&[u8]>> = values.collect();

    // Zone-map min/max over the present (non-NULL) values, and a single up-front
    // preflight of every value's `u32` length so a too-large value surfaces here
    // (localized to this column) rather than deep inside an encoder.
    let mut min: Option<&[u8]> = None;
    let mut max: Option<&[u8]> = None;
    for value in &values {
        let Some(bytes) = *value else { continue };
        let len = u32::try_from(bytes.len())
            .map_err(|_| SegmentError::TooLarge("value length exceeds u32::MAX in one chunk"))?;
        // A present value can never reach the sentinel length — the frame ceiling
        // (`MAX_VERSION_FRAME_LEN`, 16 MiB) caps it far below `u32::MAX` — so a
        // present cell and a NULL cell are always distinguishable on read.
        debug_assert_ne!(
            len, BYTES_NULL_SENTINEL,
            "present value reached NULL sentinel length"
        );
        min = Some(min.map_or(bytes, |m| if bytes < m { bytes } else { m }));
        max = Some(max.map_or(bytes, |m| if bytes > m { bytes } else { m }));
    }
    // A column with no (non-NULL) values has no bound (`Absent`). For a present
    // value, an empty bounded prefix is the degenerate edge of the scheme — an
    // empty lex-min, or an all-`0xFF` max — recorded as a present *open* end
    // (`Unbounded`, −∞ / +∞) so the column keeps its zone and prunes on the other
    // side, instead of the bare zero-length sentinel that used to collapse the
    // whole zone ([STL-120]).
    let stat_min = min.map_or(StatBound::Absent, |m| {
        bound_or_unbounded(bounded_min_prefix(m))
    });
    let stat_max = max.map_or(StatBound::Absent, |m| {
        bound_or_unbounded(bounded_max_prefix(m))
    });

    // Plain is the always-correct fallback; its size is a 4-byte length per value
    // plus the present bytes (a NULL contributes only its 4-byte sentinel).
    let plain_size: usize = values.iter().map(|v| 4 + v.map_or(0, <[u8]>::len)).sum();
    // Keep a dictionary only when enabled *and* strictly smaller than plain.
    let (codec, payload) = match use_dict.then(|| build_dict_payload(&values)).flatten() {
        Some(dict) if dict.len() < plain_size => (Codec::Dict, dict),
        _ => (Codec::Plain, encode_plain_bytes(&values)),
    };
    Ok(EncodedColumn {
        codec,
        payload,
        stat_min,
        stat_max,
    })
}

/// The plain bytes layout: `[u32 len][bytes]` per value, or [`BYTES_NULL_SENTINEL`]
/// and no body for a SQL `NULL` cell ([STL-154]). Every value's length was already
/// preflighted to fit `u32` by [`encode_bytes_values`], so the conversion here
/// cannot fail.
fn encode_plain_bytes(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut payload = Vec::new();
    for value in values {
        match value {
            None => payload.extend_from_slice(&BYTES_NULL_SENTINEL.to_le_bytes()),
            Some(bytes) => {
                let len = u32::try_from(bytes.len()).expect("length preflighted to fit u32");
                payload.extend_from_slice(&len.to_le_bytes());
                payload.extend_from_slice(bytes);
            }
        }
    }
    payload
}

/// Build the dictionary payload for a bytes column ([`Codec::Dict`], [STL-250]),
/// or `None` when a dictionary cannot beat plain — an empty column, or one whose
/// values are *all distinct* (a dictionary then stores every value once like
/// plain, plus a header and a code per row, so it can only be larger).
///
/// The dictionary is built in **first-appearance order**, which is deterministic
/// given row order, so the same input always produces byte-identical output
/// ([ADR-0010]). Layout: `[u8 code_width][u32 dict_count][(u32 len, bytes) ×
/// dict_count][code × value_count]`, where a dictionary `len` of
/// [`BYTES_NULL_SENTINEL`] marks a NULL entry (the `payload` column) and each code
/// is the `code_width`-byte little-endian index of its value in the dictionary.
fn build_dict_payload(values: &[Option<&[u8]>]) -> Option<Vec<u8>> {
    if values.is_empty() {
        return None;
    }
    let mut index: HashMap<Option<&[u8]>, u32> = HashMap::new();
    let mut distinct: Vec<Option<&[u8]>> = Vec::new();
    let mut codes: Vec<u32> = Vec::with_capacity(values.len());
    for &value in values {
        let code = if let Some(&c) = index.get(&value) {
            c
        } else {
            // `distinct.len() <= values.len()`, and `value_count` was bounded by
            // `u32` before this column was encoded (`finish`), so the index fits.
            let c = u32::try_from(distinct.len()).expect("dictionary index fits u32");
            index.insert(value, c);
            distinct.push(value);
            c
        };
        codes.push(code);
    }
    // No repeats ⇒ a dictionary can only be larger than plain. Bail before paying
    // to build it, so the all-distinct common case is free.
    if distinct.len() == values.len() {
        return None;
    }
    let code_width = code_width_for(distinct.len());
    let mut payload = Vec::new();
    payload.push(code_width);
    let dict_count = u32::try_from(distinct.len()).expect("dictionary count fits u32");
    payload.extend_from_slice(&dict_count.to_le_bytes());
    for &entry in &distinct {
        match entry {
            None => payload.extend_from_slice(&BYTES_NULL_SENTINEL.to_le_bytes()),
            Some(bytes) => {
                let len = u32::try_from(bytes.len()).expect("length preflighted to fit u32");
                payload.extend_from_slice(&len.to_le_bytes());
                payload.extend_from_slice(bytes);
            }
        }
    }
    for &code in &codes {
        push_code(&mut payload, code, code_width);
    }
    Some(payload)
}

/// The narrowest code width (bytes) that addresses a dictionary of `dict_count`
/// entries: 1 byte for ≤256 entries, 2 for ≤65536, else 4 — the dict + (byte-)
/// packing the architecture lists. A wider dictionary is rare for a version-chain
/// column (a key has one business key), and the size comparison in
/// [`encode_bytes_values`] discards the dictionary anyway when it does not pay.
const fn code_width_for(dict_count: usize) -> u8 {
    if dict_count <= 256 {
        1
    } else if dict_count <= 65536 {
        2
    } else {
        4
    }
}

/// Append `code` as `code_width` little-endian bytes. `code < dict_count <=
/// 2^(8·code_width)` ([`code_width_for`]), so the high bytes of its `u32`
/// representation are zero and the low `code_width` bytes carry the whole value —
/// no truncation, no cast.
fn push_code(buf: &mut Vec<u8>, code: u32, code_width: u8) {
    let le = code.to_le_bytes();
    buf.extend_from_slice(&le[..code_width as usize]);
}

/// A non-empty bounded prefix is a concrete [`StatBound::Value`]; an *empty* one
/// is the degenerate edge of the bounded-prefix scheme, recorded as a present
/// open end ([`StatBound::Unbounded`], −∞ for a min / +∞ for a max, [STL-120]).
fn bound_or_unbounded(prefix: Vec<u8>) -> StatBound {
    if prefix.is_empty() {
        StatBound::Unbounded
    } else {
        StatBound::Value(prefix)
    }
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
        // An `i64` column is always stored verbatim — the version-chain repeats a
        // dictionary captures live in the bytes columns ([STL-250]); a delta / FOR
        // codec for the monotonic `sys_from` / `seq` axes is a separate follow-up.
        codec: Codec::Plain,
        payload,
        // An i64 bound is always exactly representable, so it is never open —
        // only `Absent` (empty column) or a concrete `Value` ([STL-120]).
        stat_min: min.map_or(StatBound::Absent, |v| {
            StatBound::Value(v.to_le_bytes().to_vec())
        }),
        stat_max: max.map_or(StatBound::Absent, |v| {
            StatBound::Value(v.to_le_bytes().to_vec())
        }),
    }
}

/// Truncate a lex-min byte value *down* to a bounded prefix for the footer
/// stat. A byte prefix is lex-`<=` the value it came from, so the prefix is a
/// sound lower bound for every value in the column — pruning against it can
/// never drop a real match. An empty result means the min value is itself the
/// empty byte string; the caller ([`bound_or_unbounded`]) records that as a
/// present *open* (−∞) end so the column keeps pruning on its max side
/// ([STL-120]) — everything is `>= b""`, so an exact `b""` lower bound would
/// prune nothing anyway. Conservative, never wrong.
fn bounded_min_prefix(value: &[u8]) -> Vec<u8> {
    value[..value.len().min(MAX_BYTES_STAT_PREFIX_LEN)].to_vec()
}

/// Round a lex-max byte value *up* to a bounded prefix that stays `>=` the
/// value. If the value already fits within the cap it is its own exact upper
/// bound; otherwise keep the first `MAX_BYTES_STAT_PREFIX_LEN` bytes and
/// increment them — drop any trailing `0xFF` bytes and bump the last byte below
/// `0xFF` — so the result is `>=` every value sharing that prefix. A prefix that
/// is *all* `0xFF` has no shorter upper bound representable, so it returns empty;
/// the caller ([`bound_or_unbounded`]) records that as a present *open* (+∞) end
/// so the column keeps pruning on its min side ([STL-120]) — +∞ never prunes on
/// the max side, still conservative, never a false negative.
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
    row_groups: &[RowGroupChunks],
    retraction_chunks: &[EncodedChunk],
    bloom: Option<&KeyBloom>,
    valid_summary: Option<&ValidIntervalSummary>,
) -> Result<Vec<u8>, SegmentError> {
    let row_group_count = u32::try_from(row_groups.len())
        .map_err(|_| SegmentError::TooLarge("row-group count exceeds u32::MAX"))?;
    let mut out = Vec::new();
    out.extend_from_slice(&SCHEMA_ID_IMPLICIT_VERSION.to_le_bytes());
    // The flags word signals the optional trailing sections — the bloom
    // ([STL-238]) and the valid-time interval summary ([STL-241]). A footer with
    // neither writes `0` here and is byte-identical to a v10 footer.
    let mut flags = 0u32;
    if bloom.is_some() {
        flags |= FOOTER_FLAG_BLOOM;
    }
    if valid_summary.is_some() {
        flags |= FOOTER_FLAG_VALID_INTERVALS;
    }
    out.extend_from_slice(&flags.to_le_bytes());
    // One by default; several when the writer was bounded ([STL-155]). The
    // footer has carried this count since v1, so a multi-row-group segment is
    // not a format change.
    out.extend_from_slice(&row_group_count.to_le_bytes());
    for rg in row_groups {
        let column_count = u32::try_from(rg.chunks.len())
            .map_err(|_| SegmentError::TooLarge("column count exceeds u32::MAX"))?;
        out.extend_from_slice(&rg.row_count.to_le_bytes());
        out.extend_from_slice(&column_count.to_le_bytes());
        for chunk in &rg.chunks {
            encode_chunk_meta(&mut out, chunk)?;
        }
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
    // Per-segment bloom section ([STL-238], v11) — present iff `FOOTER_FLAG_BLOOM`
    // was set above. Trailing, so a footer with no bloom is byte-identical to v10
    // and the reader only reaches this section when the flag tells it to.
    if let Some(bloom) = bloom {
        bloom.encode(&mut out);
    }
    // Per-segment valid-time interval summary ([STL-241], v12) — present iff
    // `FOOTER_FLAG_VALID_INTERVALS` was set above, written *after* the bloom so a
    // reader decodes the two trailing sections in a fixed order. A footer with no
    // summary is byte-identical to v11.
    if let Some(summary) = valid_summary {
        summary.encode(&mut out);
    }
    Ok(out)
}

/// Append one column-chunk's footer entry: id, codec, reserved, absolute offset,
/// length, value count, reserved, then the length-prefixed min/max zone stats.
/// Shared by the version row-group and the retraction section so the two never
/// drift in layout.
fn encode_chunk_meta(out: &mut Vec<u8>, chunk: &EncodedChunk) -> Result<(), SegmentError> {
    out.extend_from_slice(&chunk.col.as_u16().to_le_bytes());
    out.push(chunk.codec.as_byte());
    // Stat-presence flags ([STL-120]): formerly an always-zero reserved byte.
    // Marks a present-but-open min/max (−∞ / +∞) so the reader tells it apart
    // from the zero-length "no stats" sentinel. Zero for every i64 column and
    // any bytes column with concrete bounds, so existing layouts are unchanged.
    let stat_flags =
        chunk.stat_min.flag(STAT_MIN_UNBOUNDED) | chunk.stat_max.flag(STAT_MAX_UNBOUNDED);
    out.push(stat_flags);
    out.extend_from_slice(&chunk.offset.to_le_bytes());
    out.extend_from_slice(&chunk.length.to_le_bytes());
    out.extend_from_slice(&chunk.value_count.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved
    let min_bytes = chunk.stat_min.bytes();
    let min_len = u32::try_from(min_bytes.len())
        .map_err(|_| SegmentError::TooLarge("stat min length exceeds u32::MAX"))?;
    out.extend_from_slice(&min_len.to_le_bytes());
    out.extend_from_slice(min_bytes);
    let max_bytes = chunk.stat_max.bytes();
    let max_len = u32::try_from(max_bytes.len())
        .map_err(|_| SegmentError::TooLarge("stat max length exceeds u32::MAX"))?;
    out.extend_from_slice(&max_len.to_le_bytes());
    out.extend_from_slice(max_bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Unit tests for the bounded-prefix bytes-stat helpers. The cross-module
    //! proof that these bounds never prune a real match lives in the seed-swept
    //! oracle (`tests/zone_map.rs`); these pin the rounding arithmetic — the
    //! easy place to get an off-by-one that silently produces a too-tight
    //! upper bound.

    use super::{
        Codec, MAX_BYTES_STAT_PREFIX_LEN, bounded_max_prefix, bounded_min_prefix,
        build_dict_payload, code_width_for, encode_bytes_values,
    };

    #[test]
    fn code_width_grows_with_dictionary_size() {
        // 1 byte addresses up to 256 entries (codes 0..=255), 2 up to 65536, then 4.
        assert_eq!(code_width_for(1), 1);
        assert_eq!(code_width_for(256), 1);
        assert_eq!(code_width_for(257), 2);
        assert_eq!(code_width_for(65_536), 2);
        assert_eq!(code_width_for(65_537), 4);
    }

    #[test]
    fn all_distinct_values_decline_the_dictionary() {
        // No repeats ⇒ a dictionary stores every value once like plain, plus a
        // header and a code per row, so it can only be larger. `build_dict_payload`
        // bails, and `encode_bytes_values` keeps `Plain` even with dict enabled.
        let vals: Vec<Option<&[u8]>> = vec![
            Some(b"a".as_slice()),
            Some(b"b".as_slice()),
            Some(b"c".as_slice()),
        ];
        assert!(build_dict_payload(&vals).is_none());
        let enc = encode_bytes_values(vals.into_iter(), true).expect("encode");
        assert_eq!(enc.codec, Codec::Plain);
    }

    #[test]
    fn an_empty_column_declines_the_dictionary() {
        assert!(build_dict_payload(&[]).is_none());
    }

    #[test]
    fn repeated_values_choose_the_dictionary_when_smaller() {
        // A long key repeated many times: the dictionary stores it once plus one
        // code per row, far smaller than re-storing it each time.
        let key: &[u8] = b"a-fairly-long-repeated-business-key-value";
        let on = encode_bytes_values((0..50).map(|_| Some(key)), true).expect("encode");
        assert_eq!(on.codec, Codec::Dict);
        // Disabled ⇒ always plain, and larger than the dictionary it declined.
        let off = encode_bytes_values((0..50).map(|_| Some(key)), false).expect("encode");
        assert_eq!(off.codec, Codec::Plain);
        assert!(
            on.payload.len() < off.payload.len(),
            "dictionary {} B should beat plain {} B",
            on.payload.len(),
            off.payload.len(),
        );
    }

    #[test]
    fn dictionary_and_plain_compute_identical_zone_stats() {
        // The codec changes only the physical layout — the logical min/max the
        // zone map prunes on must be identical, so pruning is unaffected. Two
        // longer values alternating so the dictionary wins (and is exercised).
        let hi: &[u8] = b"mmmmmmmmmmmmmmmm";
        let lo: &[u8] = b"aaaaaaaaaaaaaaaa";
        let row = |i: usize| Some(if i % 2 == 0 { hi } else { lo });
        let on = encode_bytes_values((0..10).map(row), true).expect("encode");
        let off = encode_bytes_values((0..10).map(row), false).expect("encode");
        assert_eq!(on.codec, Codec::Dict);
        assert_eq!(off.codec, Codec::Plain);
        assert_eq!(on.stat_min.bytes(), off.stat_min.bytes());
        assert_eq!(on.stat_max.bytes(), off.stat_max.bytes());
        assert_eq!(on.stat_min.bytes(), lo, "min is the lex-least value");
        assert_eq!(on.stat_max.bytes(), hi, "max is the lex-greatest value");
    }

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
