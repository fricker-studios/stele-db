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
use std::collections::BTreeSet;

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;

use crate::backend::{Disk, DiskFile};
use crate::checksum::crc32c;
use crate::delta::{BusinessKey, Version};
use crate::validity::Close;
use crate::validtime::reframe_payload;

use super::SegmentError;
use super::format::{
    BYTES_NULL_SENTINEL, CHUNK_HEADER_LEN, Codec, ColumnId, ColumnType, FORMAT_VERSION, HEADER_LEN,
    HEADER_MAGIC, STAT_MAX_UNBOUNDED, STAT_MIN_UNBOUNDED, TRAILER_LEN, TRAILER_MAGIC,
};
use super::zone_map::{Predicate, ZoneBound, ZoneEnd, ZoneMap};

/// Decoded contents of one projected column chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnData {
    /// Variable-length, always-present bytes column ([`ColumnId::BusinessKey`]
    /// or [`ColumnId::Principal`]).
    Bytes(Vec<Vec<u8>>),
    /// A variable-length bytes column whose cells may be SQL `NULL` — only the
    /// [`ColumnId::Payload`] column ([STL-154]). A `None` cell is a NULL,
    /// distinct from `Some(vec![])` (an empty payload).
    NullableBytes(Vec<Option<Vec<u8>>>),
    /// Fixed-width `i64` column ([`ColumnId::SysFrom`], [`ColumnId::TxnId`],
    /// [`ColumnId::CommittedAt`], or — on a valid-time table's segment —
    /// [`ColumnId::ValidFrom`] / [`ColumnId::ValidTo`]).
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
    /// The retraction section's tombstone columns (format v7, STL-143) — empty
    /// when the segment holds no deletes. Each column shares `retraction_count`
    /// as its value count, independent of any row-group's row count.
    retractions: Vec<ColumnChunkMeta>,
    /// Number of retraction tombstone rows — the shared value count for every
    /// column in [`Self::retractions`].
    retraction_count: u32,
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
    stat_min: Option<ZoneEnd>,
    stat_max: Option<ZoneEnd>,
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
    /// can treat this as a constant while the catalog ([STL-98]) is not yet the
    /// schema authority.
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

    /// The number of rows in each row-group, in on-disk (row) order.
    ///
    /// Footer-derived, so it costs no column-chunk I/O. This is the addressing
    /// a caller needs to map a segment-global row index back to its owning
    /// row-group — the prefix sums of these counts are each row-group's
    /// starting row — and then scope a column read to just those row-groups
    /// via [`Self::read_column_in_row_groups`] ([STL-155]).
    #[must_use]
    pub fn row_group_row_counts(&self) -> Vec<u32> {
        self.footer
            .row_groups
            .iter()
            .map(|rg| rg.row_count)
            .collect()
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

    /// One [`ZoneMap`] per row-group, in on-disk (row) order — the finer-grained
    /// sibling of [`Self::zone_map`] (which folds these into a single
    /// segment-level digest).
    ///
    /// Each map is built from only its own row-group's column-chunk stats, so a
    /// planner can rule out individual row-groups before reading even their
    /// narrow identity columns ([STL-173]): a value/system-time predicate that
    /// the segment-level fold cannot disprove (its `[min, max]` spans every
    /// row-group) may still be provably disjoint from a particular row-group.
    /// Footer-derived, so building these costs no column-chunk I/O — the same
    /// resident-metadata property [`Self::zone_map`] documents, at row-group
    /// granularity. Indices line up with [`Self::row_group_row_counts`] and the
    /// selection [`Self::read_column_in_row_groups`] /
    /// [`Self::version_keys_in_row_groups`] take.
    ///
    /// The retraction tombstone section is **not** a row-group and is excluded
    /// here (it carries its own value count, decoupled from any row-group's row
    /// count); it folds only into the segment-level [`Self::zone_map`].
    #[must_use]
    pub fn row_group_zone_maps(&self) -> Vec<ZoneMap> {
        self.footer
            .row_groups
            .iter()
            .map(|rg| zone_map_from_metas(rg.columns.iter()))
            .collect()
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
        self.read_column_from(col, self.footer.row_groups.iter())
    }

    /// Read one column across only the selected row-groups, in row-group
    /// (and therefore row) order — the chunk-level late-materialization path
    /// ([STL-155]). Only the named row-groups' chunks for `col` are touched;
    /// each chunk's CRC32C is still verified before any of its bytes are
    /// decoded. The returned values are the concatenation of the selected
    /// row-groups' values, so a caller addressing individual rows must
    /// translate segment-global row indices through the selection (see
    /// [`Self::row_group_row_counts`]).
    ///
    /// # Panics
    ///
    /// If a selected index is not below the footer's row-group count — the
    /// caller derives its selection from this reader's own
    /// [`row_group_row_counts`](Self::row_group_row_counts), so an
    /// out-of-range index is a caller bug, not data corruption (the same
    /// contract as `Column::slice` in the executor).
    pub fn read_column_in_row_groups(
        &self,
        col: ColumnId,
        row_groups: &BTreeSet<usize>,
    ) -> Result<ColumnData, SegmentError> {
        self.read_column_from(col, row_groups.iter().map(|&g| &self.footer.row_groups[g]))
    }

    /// Shared body of [`Self::read_column`] / [`Self::read_column_in_row_groups`]:
    /// decode `col`'s chunk from each yielded row-group, appending values in
    /// iteration order.
    fn read_column_from<'g>(
        &self,
        col: ColumnId,
        row_groups: impl Iterator<Item = &'g RowGroup>,
    ) -> Result<ColumnData, SegmentError> {
        // No `with_capacity` from `self.row_count()` — that figure is
        // footer-derived and the natural-growth `Vec` is the safer baseline
        // against a corrupt footer that advertises billions of rows. Each
        // chunk read appends `value_count` (a `u32`) values, and chunk
        // payload bytes are independently bounded by `read_chunk_payload`'s
        // file-length check below, so the in-loop growth is itself bounded
        // by the file's actual size.
        match col.ty() {
            // The `Payload` column may carry SQL `NULL` cells (format v10,
            // [STL-154]); it decodes through the sentinel-aware path into a
            // `NullableBytes`. Every other bytes column is always present.
            ColumnType::Bytes if col == ColumnId::Payload => {
                let mut out: Vec<Option<Vec<u8>>> = Vec::new();
                for rg in row_groups {
                    let meta = chunk_meta(rg, col)?;
                    let payload = read_chunk_payload(&self.file, meta)?;
                    decode_nullable_bytes_chunk(&payload, meta.value_count, &mut out)?;
                }
                Ok(ColumnData::NullableBytes(out))
            }
            ColumnType::Bytes => {
                let mut out: Vec<Vec<u8>> = Vec::new();
                for rg in row_groups {
                    let meta = chunk_meta(rg, col)?;
                    let payload = read_chunk_payload(&self.file, meta)?;
                    decode_bytes_chunk(&payload, meta.value_count, &mut out)?;
                }
                Ok(ColumnData::Bytes(out))
            }
            ColumnType::I64 => {
                let mut out: Vec<i64> = Vec::new();
                for rg in row_groups {
                    let meta = chunk_meta(rg, col)?;
                    let payload = read_chunk_payload(&self.file, meta)?;
                    decode_i64_chunk(&payload, meta.value_count, &mut out)?;
                }
                Ok(ColumnData::I64(out))
            }
        }
    }

    /// Project one bytes column, erroring if the segment typed it as `i64`.
    /// Structurally unreachable — each decoder picks its [`ColumnData`] arm
    /// from [`ColumnId::ty`] — but kept as a typed error so a future codec
    /// change that loosens the mapping fails loudly in one place.
    fn read_bytes_column(&self, col: ColumnId) -> Result<Vec<Vec<u8>>, SegmentError> {
        match self.read_column(col)? {
            ColumnData::Bytes(v) => Ok(v),
            ColumnData::NullableBytes(_) | ColumnData::I64(_) => Err(SegmentError::Corrupt(
                "column data type mismatched expected schema",
            )),
        }
    }

    /// Project the nullable `payload` column, erroring if the segment typed it
    /// otherwise. The dual of [`Self::read_bytes_column`] for the one bytes
    /// column whose cells can be SQL `NULL` ([STL-154]).
    fn read_nullable_bytes_column(
        &self,
        col: ColumnId,
    ) -> Result<Vec<Option<Vec<u8>>>, SegmentError> {
        match self.read_column(col)? {
            ColumnData::NullableBytes(v) => Ok(v),
            ColumnData::Bytes(_) | ColumnData::I64(_) => Err(SegmentError::Corrupt(
                "column data type mismatched expected schema",
            )),
        }
    }

    /// Project one `i64` column, erroring if the segment typed it as bytes.
    /// See [`Self::read_bytes_column`] for why the mismatch is a typed error.
    fn read_i64_column(&self, col: ColumnId) -> Result<Vec<i64>, SegmentError> {
        match self.read_column(col)? {
            ColumnData::I64(v) => Ok(v),
            ColumnData::Bytes(_) | ColumnData::NullableBytes(_) => Err(SegmentError::Corrupt(
                "column data type mismatched expected schema",
            )),
        }
    }

    /// Project the segment's version identities — each row's
    /// `(business_key, sys_from, seq)` triple — in row order.
    ///
    /// The minimal read the validity-index segment prune needs: it touches only
    /// the three narrow key columns ([`ColumnId::BusinessKey`],
    /// [`ColumnId::SysFrom`], and [`ColumnId::Seq`]), not the payload or provenance
    /// columns. A planner feeds these to
    /// [`ValidityIndex::sys_upper_bound`](crate::validity::ValidityIndex::sys_upper_bound)
    /// to derive the segment's system-time upper bound and skip the segment's
    /// bulk columns entirely when every row is already superseded at the read
    /// snapshot ([STL-139]). The `seq` completes each version's `(sys_from, seq)`
    /// identity so the bound probes the right index entry when two versions share
    /// a `sys_from` ([ADR-0024], STL-145).
    pub fn version_keys(&self) -> Result<Vec<(BusinessKey, SystemTimeMicros, u64)>, SegmentError> {
        let business_keys = self.read_bytes_column(ColumnId::BusinessKey)?;
        let sys_from = self.read_i64_column(ColumnId::SysFrom)?;
        let seqs = self.read_i64_column(ColumnId::Seq)?;
        assemble_version_keys(business_keys, sys_from, seqs)
    }

    /// The version identities of only the selected `row_groups`, in row-group
    /// (and therefore row) order — [`Self::version_keys`] scoped to a subset.
    ///
    /// The row-group-pruned identity read ([STL-173]): once a planner has ruled
    /// out the row-groups whose [`Self::row_group_zone_maps`] prove no visible
    /// match, it resolves the snapshot from just the survivors' narrow
    /// `(business_key, sys_from, seq)` columns — the pruned row-groups' identity
    /// chunks are never touched. The returned keys are the concatenation of the
    /// selected row-groups' rows, so a caller addressing segment-global row
    /// indices maps them back through [`Self::row_group_row_counts`] exactly as
    /// for [`Self::read_column_in_row_groups`].
    ///
    /// # Panics
    ///
    /// If a selected index is not below the footer's row-group count — the caller
    /// derives its selection from this reader's own
    /// [`row_group_zone_maps`](Self::row_group_zone_maps) /
    /// [`row_group_row_counts`](Self::row_group_row_counts), so an out-of-range
    /// index is a caller bug, not data corruption.
    pub fn version_keys_in_row_groups(
        &self,
        row_groups: &BTreeSet<usize>,
    ) -> Result<Vec<(BusinessKey, SystemTimeMicros, u64)>, SegmentError> {
        let business_keys = self.read_bytes_column_in(ColumnId::BusinessKey, row_groups)?;
        let sys_from = self.read_i64_column_in(ColumnId::SysFrom, row_groups)?;
        let seqs = self.read_i64_column_in(ColumnId::Seq, row_groups)?;
        assemble_version_keys(business_keys, sys_from, seqs)
    }

    /// Project one bytes column from only the selected row-groups, erroring on a
    /// type mismatch — the scoped sibling of [`Self::read_bytes_column`].
    fn read_bytes_column_in(
        &self,
        col: ColumnId,
        row_groups: &BTreeSet<usize>,
    ) -> Result<Vec<Vec<u8>>, SegmentError> {
        match self.read_column_in_row_groups(col, row_groups)? {
            ColumnData::Bytes(v) => Ok(v),
            ColumnData::NullableBytes(_) | ColumnData::I64(_) => Err(SegmentError::Corrupt(
                "column data type mismatched expected schema",
            )),
        }
    }

    /// Project one `i64` column from only the selected row-groups, erroring on a
    /// type mismatch — the scoped sibling of [`Self::read_i64_column`].
    fn read_i64_column_in(
        &self,
        col: ColumnId,
        row_groups: &BTreeSet<usize>,
    ) -> Result<Vec<i64>, SegmentError> {
        match self.read_column_in_row_groups(col, row_groups)? {
            ColumnData::I64(v) => Ok(v),
            ColumnData::Bytes(_) | ColumnData::NullableBytes(_) => Err(SegmentError::Corrupt(
                "column data type mismatched expected schema",
            )),
        }
    }

    /// Read every column and reassemble [`Version`]s in row order — the
    /// dual of [`super::writer::SegmentWriter::push`]. Useful for tests and
    /// for the compaction reader; query execution prefers the projected
    /// [`Self::read_column`].
    #[allow(clippy::cast_sign_loss)] // `txn_id` round-trips i64-bits → u64 (see `ColumnId::TxnId`).
    pub fn read_versions(&self) -> Result<Vec<Version>, SegmentError> {
        // `mut` so the row loop can `mem::take` each owned byte vector out
        // instead of cloning it — see the loop below.
        let mut business_keys = self.read_bytes_column(ColumnId::BusinessKey)?;
        // The payload column may carry SQL `NULL` cells ([STL-154]); a `None`
        // reconstructs a NULL-payload `Version`.
        let mut payloads = self.read_nullable_bytes_column(ColumnId::Payload)?;
        let mut principals = self.read_bytes_column(ColumnId::Principal)?;
        let sys_from = self.read_i64_column(ColumnId::SysFrom)?;
        let seqs = self.read_i64_column(ColumnId::Seq)?;
        let txn_ids = self.read_i64_column(ColumnId::TxnId)?;
        let committed_ats = self.read_i64_column(ColumnId::CommittedAt)?;

        let n = business_keys.len();
        if ![
            payloads.len(),
            principals.len(),
            sys_from.len(),
            seqs.len(),
            txn_ids.len(),
            committed_ats.len(),
        ]
        .iter()
        .all(|&len| len == n)
        {
            return Err(SegmentError::Corrupt(
                "per-column value counts disagree within row-group",
            ));
        }
        // On a valid-time segment the payload column holds only the bare user
        // payload ([STL-119]); re-frame each one in place from the first-class
        // valid_from / valid_to columns so the reconstructed Version is
        // byte-for-byte what was written. The footer carrying the valid-time
        // columns is the signal — the read path stays oblivious to whether the
        // stored bytes were framed or split. The row loop below `mem::take`s the
        // reframed bytes out.
        //
        // The two columns are written as a pair, so they must be present
        // together: exactly one present is a corrupt footer (or an unsupported
        // schema) that would otherwise silently return unframed payloads.
        let has_valid_from = self.has_column(ColumnId::ValidFrom);
        let has_valid_to = self.has_column(ColumnId::ValidTo);
        if has_valid_from != has_valid_to {
            return Err(SegmentError::Corrupt(
                "valid-time columns must be present as a pair (valid_from with valid_to)",
            ));
        }
        if has_valid_from {
            let valid_from = self.read_i64_column(ColumnId::ValidFrom)?;
            let valid_to = self.read_i64_column(ColumnId::ValidTo)?;
            if valid_from.len() != n || valid_to.len() != n {
                return Err(SegmentError::Corrupt(
                    "valid-time column value counts disagree within row-group",
                ));
            }
            for i in 0..n {
                // Re-impose the interval prefix on the bare user payload. A NULL
                // payload (`None`) is not reachable on a valid-time segment via
                // v0.1 paths (its interval prefix is always stored), so it is left
                // as-is rather than invented ([STL-154]).
                if let Some(bare) = &payloads[i] {
                    payloads[i] = Some(reframe_payload(valid_from[i], valid_to[i], bare));
                }
            }
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            // A sealed segment stores only birth state (v6, [ADR-0023]): the
            // reconstructed version is **open/unresolved** — its `sys_to` /
            // `closed_by` overlay is supplied by the validity index at read time
            // ([`crate::merge`]). Move the owned byte vectors out by index
            // (`mem::take` leaves a cheap empty `Vec` placeholder) rather than
            // cloning — the column vectors are discarded at function end. `i64`
            // columns are `Copy`, so they read by value.
            out.push(Version::open(
                BusinessKey::new(std::mem::take(&mut business_keys[i])),
                SystemTimeMicros(sys_from[i]),
                // `seq` round-trips i64-bits → u64, the reverse of the writer's
                // `as i64` reinterpretation (see `ColumnId::Seq`).
                seqs[i] as u64,
                Provenance {
                    txn_id: TxnId(txn_ids[i] as u64),
                    committed_at: SystemTimeMicros(committed_ats[i]),
                    principal: Principal::new(std::mem::take(&mut principals[i])),
                },
                std::mem::take(&mut payloads[i]),
            ));
        }
        Ok(out)
    }

    /// Read this segment's retraction tombstones (format v7, STL-143) — the
    /// durable record of every logical delete the segment holds, each a
    /// [`Close`] with the deleted version's `(business_key, sys_from)`, the close
    /// timestamp (`sys_to`), and the deleting transaction's provenance.
    ///
    /// Returns an empty vector when the segment has no retraction section (a
    /// delete-free segment writes no tombstone columns). The retraction columns
    /// share their own value count, decoupled from the version row count, so a
    /// segment can carry zero versions and several tombstones (a flush whose only
    /// activity was deletes), or vice versa.
    ///
    /// This is what makes the segment store **self-contained for a from-scratch
    /// validity-index rebuild** ([`crate::rebuild`]): supersession closes are
    /// re-derived from version adjacency, but a delete has no successor, so its
    /// close survives only here. Also the queryable home of delete provenance
    /// ("who deleted, when") after WAL truncation.
    #[allow(clippy::cast_sign_loss)] // `closed_by_txn` round-trips i64-bits → u64 (see `ColumnId::TxnId`).
    pub fn read_retractions(&self) -> Result<Vec<Close>, SegmentError> {
        use stele_common::provenance::Provenance;
        if self.footer.retractions.is_empty() {
            return Ok(Vec::new());
        }
        let mut keys = self.read_retraction_bytes(ColumnId::RetractKey)?;
        let mut principals = self.read_retraction_bytes(ColumnId::RetractClosedByPrincipal)?;
        let sys_from = self.read_retraction_i64(ColumnId::RetractSysFrom)?;
        let seqs = self.read_retraction_i64(ColumnId::RetractSeq)?;
        let closed_at = self.read_retraction_i64(ColumnId::RetractClosedAt)?;
        let closed_txn = self.read_retraction_i64(ColumnId::RetractClosedByTxn)?;
        let closed_committed = self.read_retraction_i64(ColumnId::RetractClosedByCommittedAt)?;

        let n = keys.len();
        if ![
            principals.len(),
            sys_from.len(),
            seqs.len(),
            closed_at.len(),
            closed_txn.len(),
            closed_committed.len(),
        ]
        .iter()
        .all(|&len| len == n)
        {
            return Err(SegmentError::Corrupt(
                "per-column value counts disagree within retraction section",
            ));
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(Close {
                business_key: BusinessKey::new(std::mem::take(&mut keys[i])),
                sys_from: SystemTimeMicros(sys_from[i]),
                // `seq` round-trips i64-bits → u64, the reverse of the writer's
                // `as i64` reinterpretation (see `ColumnId::RetractSeq`).
                seq: seqs[i] as u64,
                sys_to: SystemTimeMicros(closed_at[i]),
                closed_by: Provenance {
                    txn_id: TxnId(closed_txn[i] as u64),
                    committed_at: SystemTimeMicros(closed_committed[i]),
                    principal: Principal::new(std::mem::take(&mut principals[i])),
                },
            });
        }
        Ok(out)
    }

    /// Read one retraction-section column from [`Footer::retractions`] (not a
    /// row-group). Mirrors [`Self::read_column`]'s late-materialization +
    /// per-chunk CRC, but the retraction section is a single chunk per column.
    fn read_retraction_column(&self, col: ColumnId) -> Result<ColumnData, SegmentError> {
        let meta = self
            .footer
            .retractions
            .iter()
            .find(|c| c.column_id == col)
            .ok_or(SegmentError::Corrupt(
                "retraction column missing from segment",
            ))?;
        let payload = read_chunk_payload(&self.file, meta)?;
        match col.ty() {
            ColumnType::Bytes => {
                let mut out: Vec<Vec<u8>> = Vec::new();
                decode_bytes_chunk(&payload, meta.value_count, &mut out)?;
                Ok(ColumnData::Bytes(out))
            }
            ColumnType::I64 => {
                let mut out: Vec<i64> = Vec::new();
                decode_i64_chunk(&payload, meta.value_count, &mut out)?;
                Ok(ColumnData::I64(out))
            }
        }
    }

    fn read_retraction_bytes(&self, col: ColumnId) -> Result<Vec<Vec<u8>>, SegmentError> {
        match self.read_retraction_column(col)? {
            ColumnData::Bytes(v) => Ok(v),
            // No retraction column is the nullable `payload`, so `NullableBytes`
            // is as much a schema mismatch here as `I64`.
            ColumnData::NullableBytes(_) | ColumnData::I64(_) => Err(SegmentError::Corrupt(
                "retraction column data type mismatched expected schema",
            )),
        }
    }

    fn read_retraction_i64(&self, col: ColumnId) -> Result<Vec<i64>, SegmentError> {
        match self.read_retraction_column(col)? {
            ColumnData::I64(v) => Ok(v),
            ColumnData::Bytes(_) | ColumnData::NullableBytes(_) => Err(SegmentError::Corrupt(
                "retraction column data type mismatched expected schema",
            )),
        }
    }

    /// Total on-disk bytes the chunk(s) for `col` occupy across every
    /// row-group, including each chunk's 16-byte header — or `None` if the
    /// column is absent from the segment.
    ///
    /// Footer-derived, so it costs no column-chunk I/O. Exposed for IO-cost
    /// estimation and for measuring per-column storage footprint (e.g. the
    /// provenance-overhead check in [STL-93]).
    #[must_use]
    pub fn column_byte_len(&self, col: ColumnId) -> Option<u64> {
        let mut total = 0u64;
        let mut seen = false;
        for rg in &self.footer.row_groups {
            for c in rg.columns.iter().filter(|c| c.column_id == col) {
                total = total.saturating_add(c.length);
                seen = true;
            }
        }
        seen.then_some(total)
    }

    /// Whether any row-group declares `col` in the footer. Footer-derived, so
    /// it costs no column-chunk I/O — the cheap test for an opt-in column such
    /// as the valid-time pair ([`ColumnId::ValidFrom`] / [`ColumnId::ValidTo`]).
    fn has_column(&self, col: ColumnId) -> bool {
        self.footer
            .row_groups
            .iter()
            .any(|rg| rg.columns.iter().any(|c| c.column_id == col))
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
            // Every column in a row-group shares the row-group's row count.
            // Detect a footer that claims a row count contradicting its own
            // per-column figures at open time, so the inconsistency surfaces
            // here rather than as a silent disagreement between `row_count()`
            // and what a projection actually returns.
            columns.push(parse_chunk_meta(&mut p, row_count, "row-group row_count")?);
        }
        row_groups.push(RowGroup { row_count, columns });
    }
    // Retraction section (format v7, STL-143): a tombstone-row count followed by
    // that many column metas. Always present in a v7 footer — `0` columns when
    // the segment holds no deletes. Each retraction column shares
    // `retraction_count` as its value count, *not* any row-group's row count.
    let retraction_count = p.u32()?;
    let retraction_column_count = p.u32()?;
    let mut retractions: Vec<ColumnChunkMeta> = Vec::new();
    for _ in 0..retraction_column_count {
        retractions.push(parse_chunk_meta(
            &mut p,
            retraction_count,
            "retraction_count",
        )?);
    }
    // The two retraction counts move together: the writer emits either an empty
    // section (both zero) or the full tombstone column set with a positive row
    // count. A footer claiming tombstone rows but no columns (or vice versa) would
    // let `read_retractions` silently return empty on the `is_empty` short-circuit,
    // masking the corruption — reject it here instead.
    if (retraction_count == 0) != retractions.is_empty() {
        return Err(SegmentError::Corrupt(
            "retraction section inconsistent: row count and column presence disagree",
        ));
    }
    if !p.is_empty() {
        return Err(SegmentError::Corrupt("trailing bytes in footer"));
    }
    Ok(Footer {
        schema_id,
        row_groups,
        retractions,
        retraction_count,
    })
}

/// Parse one column-chunk meta from the footer, verifying its `value_count`
/// matches `expected_value_count` (the row-group row count, or the retraction
/// count). `what` names that expectation for the typed corruption error. Shared
/// by the version row-group and the retraction-section parse so the two never
/// drift in layout.
fn parse_chunk_meta(
    p: &mut Parser,
    expected_value_count: u32,
    what: &'static str,
) -> Result<ColumnChunkMeta, SegmentError> {
    let column_id_raw = p.u16()?;
    let column_id = ColumnId::from_u16(column_id_raw)
        .ok_or(SegmentError::Corrupt("unknown column id in footer"))?;
    let codec_raw = p.u8()?;
    let codec =
        Codec::from_byte(codec_raw).ok_or(SegmentError::Corrupt("unknown codec in footer"))?;
    // Stat presence flags ([STL-120]): the byte after the codec marks an
    // empty-but-*present* (open) min/max distinctly from an absent one. An older
    // writer left this byte zero, so an absent bit reads exactly as before.
    let stat_flags = p.u8()?;
    let offset = p.u64()?;
    let length = p.u64()?;
    let value_count = p.u32()?;
    let _reserved = p.u32()?;
    // Stats feed zone-map pruning (STL-89). A zero-length field is the writer's
    // "no stats" sentinel *unless* the matching unbounded flag is set, in which
    // case it is a present open end (−∞ / +∞, STL-120); a non-empty field is
    // decoded into a typed bound matching the column's `ColumnType`. The declared
    // lengths are bounded by the footer-CRC envelope, so an oversized length
    // can't escape the footer.
    let min_len = p.u32()? as usize;
    let min_bytes = p.bytes(min_len)?;
    let max_len = p.u32()? as usize;
    let max_bytes = p.bytes(max_len)?;
    let stat_min = decode_stat(column_id, min_bytes, stat_flags & STAT_MIN_UNBOUNDED != 0)?;
    let stat_max = decode_stat(column_id, max_bytes, stat_flags & STAT_MAX_UNBOUNDED != 0)?;
    if value_count != expected_value_count {
        // One typed message; `what` distinguishes which section disagreed.
        return Err(SegmentError::Corrupt(match what {
            "retraction_count" => "retraction column value_count disagrees with retraction_count",
            _ => "column value_count disagrees with row-group row_count",
        }));
    }
    Ok(ColumnChunkMeta {
        column_id,
        codec,
        offset,
        length,
        value_count,
        stat_min,
        stat_max,
    })
}

/// Decode one footer stat field into a typed [`ZoneEnd`]. When `unbounded` is
/// set the end is open (−∞ for a min, +∞ for a max, [STL-120]) and the field
/// must carry no bytes — an unbounded end has no value, so a non-empty field
/// alongside the flag is corruption — *and* only a bytes column can legitimately
/// produce an open end ([STL-120]): an `i64` bound is always exactly
/// representable, so the writer never flags one, and the flag on an `i64` column
/// is rejected as corruption (it would otherwise also bypass the 8-byte length
/// check). Otherwise the zero-length sentinel maps to `None` ("no stats"); a
/// non-empty field is interpreted according to the column's [`ColumnType`], and
/// an `i64` stat whose length is not exactly 8 bytes is rejected as corruption
/// rather than silently truncated.
fn decode_stat(
    col: ColumnId,
    bytes: &[u8],
    unbounded: bool,
) -> Result<Option<ZoneEnd>, SegmentError> {
    if unbounded {
        if col.ty() != ColumnType::Bytes {
            return Err(SegmentError::Corrupt(
                "unbounded stat flag set on a non-bytes column",
            ));
        }
        if !bytes.is_empty() {
            return Err(SegmentError::Corrupt(
                "unbounded stat flag set but the stat field carries bytes",
            ));
        }
        return Ok(Some(ZoneEnd::Unbounded));
    }
    if bytes.is_empty() {
        return Ok(None);
    }
    match col.ty() {
        ColumnType::I64 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| SegmentError::Corrupt("i64 column stat is not 8 bytes"))?;
            Ok(Some(ZoneEnd::Value(ZoneBound::I64(i64::from_le_bytes(
                arr,
            )))))
        }
        ColumnType::Bytes => Ok(Some(ZoneEnd::Value(ZoneBound::Bytes(bytes.to_vec())))),
    }
}

/// Fold the per-chunk stats across every row-group into one segment-level
/// [`ZoneMap`]: the overall min is the least of the row-group mins, the overall
/// max the greatest of the row-group maxes. v0.1 emits a single row-group, so
/// this collapses to a copy; the fold keeps the segment-level digest correct
/// once multi-row-group writes land. The retraction tombstone columns (v7,
/// STL-143) fold in here too, so a predicate on `retract_key` / `retract_closed_at`
/// prunes a delete-free-irrelevant segment exactly like a version column does;
/// the per-row-group maps ([`SegmentReader::row_group_zone_maps`]) deliberately
/// exclude them.
fn build_zone_map(footer: &Footer) -> ZoneMap {
    zone_map_from_metas(
        footer
            .row_groups
            .iter()
            .flat_map(|rg| rg.columns.iter())
            .chain(footer.retractions.iter()),
    )
}

/// Fold an arbitrary set of column-chunk metas into a [`ZoneMap`]: the least min
/// and greatest max per column id. The shared core of [`build_zone_map`] (fed
/// every row-group's chunks plus the retraction section) and
/// [`SegmentReader::row_group_zone_maps`] (fed a single row-group's chunks).
///
/// Folding over the column ids the metas actually declare — not a fixed list —
/// keeps this schema-agnostic: the always-on set, a valid-time table's
/// valid_from / valid_to ([STL-117]), and the retraction tombstone columns each
/// flow through without special-casing.
fn zone_map_from_metas<'a>(metas: impl IntoIterator<Item = &'a ColumnChunkMeta>) -> ZoneMap {
    let metas: Vec<&ColumnChunkMeta> = metas.into_iter().collect();
    let mut present: Vec<ColumnId> = Vec::new();
    for c in &metas {
        if !present.contains(&c.column_id) {
            present.push(c.column_id);
        }
    }
    let bounds = present.into_iter().map(|col| {
        let mut min: Option<ZoneEnd> = None;
        let mut max: Option<ZoneEnd> = None;
        for c in metas.iter().filter(|c| c.column_id == col) {
            // Fold the least min / greatest max across the column's chunks. An
            // open end dominates: `Unbounded` is −∞ for a min (least) and +∞ for
            // a max (greatest). Concrete ends compare same-variant — every chunk
            // for one column shares that column's type.
            if let Some(m) = &c.stat_min {
                if end_is_smaller(m, min.as_ref()) {
                    min = Some(m.clone());
                }
            }
            if let Some(m) = &c.stat_max {
                if end_is_larger(m, max.as_ref()) {
                    max = Some(m.clone());
                }
            }
        }
        (col, min, max)
    });
    ZoneMap::from_bounds(bounds)
}

/// Whether `cand` should replace the running *min* `cur` — i.e. `cand` is the
/// smaller lower bound, with [`ZoneEnd::Unbounded`] (−∞) the smallest of all.
fn end_is_smaller(cand: &ZoneEnd, cur: Option<&ZoneEnd>) -> bool {
    match (cur, cand) {
        (Some(ZoneEnd::Unbounded), _) => false, // running min already −∞
        // no running min yet, or the candidate is −∞ ⇒ take the candidate
        (None, _) | (Some(_), ZoneEnd::Unbounded) => true,
        (Some(ZoneEnd::Value(c)), ZoneEnd::Value(v)) => {
            v.cmp_same_variant(c) == Some(Ordering::Less)
        }
    }
}

/// Whether `cand` should replace the running *max* `cur` — i.e. `cand` is the
/// larger upper bound, with [`ZoneEnd::Unbounded`] (+∞) the largest of all.
fn end_is_larger(cand: &ZoneEnd, cur: Option<&ZoneEnd>) -> bool {
    match (cur, cand) {
        (Some(ZoneEnd::Unbounded), _) => false, // running max already +∞
        // no running max yet, or the candidate is +∞ ⇒ take the candidate
        (None, _) | (Some(_), ZoneEnd::Unbounded) => true,
        (Some(ZoneEnd::Value(c)), ZoneEnd::Value(v)) => {
            v.cmp_same_variant(c) == Some(Ordering::Greater)
        }
    }
}

/// Zip the three identity columns into `(business_key, sys_from, seq)` triples,
/// in row order — the shared tail of [`SegmentReader::version_keys`] and its
/// row-group-scoped sibling [`SegmentReader::version_keys_in_row_groups`]. The
/// columns must agree in length (the same row-group contract every per-column
/// read upholds); a disagreement is a corrupt segment.
#[allow(clippy::cast_sign_loss)] // `seq` round-trips i64-bits → u64 (see `ColumnId::Seq`).
fn assemble_version_keys(
    mut business_keys: Vec<Vec<u8>>,
    sys_from: Vec<i64>,
    seqs: Vec<i64>,
) -> Result<Vec<(BusinessKey, SystemTimeMicros, u64)>, SegmentError> {
    if business_keys.len() != sys_from.len() || business_keys.len() != seqs.len() {
        return Err(SegmentError::Corrupt(
            "business_key / sys_from / seq value counts disagree",
        ));
    }
    // `mem::take` the owned key bytes out — the column vector is discarded at the
    // call site, so there is nothing to gain from cloning each key.
    Ok(business_keys
        .iter_mut()
        .zip(sys_from)
        .zip(seqs)
        .map(|((bk, sf), seq)| {
            (
                BusinessKey::new(std::mem::take(bk)),
                SystemTimeMicros(sf),
                seq as u64,
            )
        })
        .collect())
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

/// Decode a bytes chunk whose cells may be SQL `NULL` (the `payload` column,
/// format v10, [STL-154]). A per-value length of [`BYTES_NULL_SENTINEL`] marks a
/// `None` cell with no body bytes; any other length is a present value. The
/// inverse of the writer's `encode_bytes_values` over `Option<&[u8]>`.
fn decode_nullable_bytes_chunk(
    payload: &[u8],
    value_count: u32,
    out: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), SegmentError> {
    let mut p = Parser::new(payload);
    for _ in 0..value_count {
        let len = p.u32()?;
        if len == BYTES_NULL_SENTINEL {
            out.push(None);
        } else {
            let bytes = p.bytes(len as usize)?;
            out.push(Some(bytes.to_vec()));
        }
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
        // `ColumnId::ALL.len()` is a small compile-time const; the cast can
        // never truncate.
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
        // Retraction section (format v7): empty for these version-only fixtures.
        out.extend_from_slice(&0u32.to_le_bytes()); // retraction_count
        out.extend_from_slice(&0u32.to_le_bytes()); // retraction_column_count
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
    fn inconsistent_retraction_section_is_rejected() {
        // A footer claiming 3 tombstone rows but 0 retraction columns: a reader
        // that trusted only the column list would silently report no deletes,
        // masking the corruption. The self-consistency check must reject it.
        let mut out = Vec::new();
        out.extend_from_slice(&0u32.to_le_bytes()); // schema_id
        out.extend_from_slice(&0u32.to_le_bytes()); // flags
        out.extend_from_slice(&1u32.to_le_bytes()); // row_group_count
        out.extend_from_slice(&0u32.to_le_bytes()); // row_count
        out.extend_from_slice(&0u32.to_le_bytes()); // column_count
        out.extend_from_slice(&3u32.to_le_bytes()); // retraction_count (non-zero)
        out.extend_from_slice(&0u32.to_le_bytes()); // retraction_column_count (zero)
        let err = parse_footer(&out).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("retraction section inconsistent")),
            "count/column disagreement must be rejected, got {err:?}"
        );
    }

    #[test]
    fn empty_stat_decodes_to_no_stats_sentinel() {
        // Flag clear + zero length ⇒ absent (no stats).
        assert_eq!(decode_stat(ColumnId::SysFrom, &[], false).unwrap(), None);
        assert_eq!(
            decode_stat(ColumnId::BusinessKey, &[], false).unwrap(),
            None
        );
    }

    #[test]
    fn typed_stats_decode_by_column_type() {
        assert_eq!(
            decode_stat(ColumnId::SysFrom, &42i64.to_le_bytes(), false).unwrap(),
            Some(ZoneEnd::Value(ZoneBound::I64(42))),
        );
        assert_eq!(
            decode_stat(ColumnId::BusinessKey, b"abc", false).unwrap(),
            Some(ZoneEnd::Value(ZoneBound::Bytes(b"abc".to_vec()))),
        );
    }

    #[test]
    fn unbounded_flag_decodes_to_open_end() {
        // Flag set + zero length ⇒ a present open end ([STL-120]) — distinct
        // from the `None` an absent (flag-clear) zero-length field decodes to.
        assert_eq!(
            decode_stat(ColumnId::Payload, &[], true).unwrap(),
            Some(ZoneEnd::Unbounded),
        );
    }

    #[test]
    fn unbounded_flag_on_i64_column_is_rejected() {
        // Only a bounded-prefix bytes column can produce an open end; an i64
        // bound is always exactly representable. The flag on an i64 column is a
        // corrupt footer (and would otherwise bypass the 8-byte length check).
        let err = decode_stat(ColumnId::SysFrom, &[], true).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("non-bytes")),
            "unbounded flag on an i64 column must be rejected, got {err:?}"
        );
    }

    #[test]
    fn unbounded_flag_with_bytes_is_rejected() {
        // The flag means "no value"; bytes alongside it are contradictory and
        // must surface as corruption rather than be silently dropped.
        let err = decode_stat(ColumnId::Payload, b"x", true).unwrap_err();
        assert!(
            matches!(err, SegmentError::Corrupt(msg) if msg.contains("unbounded")),
            "unbounded flag + bytes must be rejected, got {err:?}"
        );
    }

    #[test]
    fn i64_stat_with_non_8_byte_length_is_rejected() {
        // A corrupt footer that declares a 4-byte min for an i64 column must
        // surface a typed error, not silently decode a truncated value.
        let err = decode_stat(ColumnId::SysFrom, &[0u8; 4], false).unwrap_err();
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
