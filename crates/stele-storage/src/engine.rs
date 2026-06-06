//! The storage engine boot + recovery driver.
//!
//! [`Engine`] bundles the durable WAL, the row-oriented delta tier, the derived
//! validity index, and the sealed segment set behind one handle, and — the point
//! of [STL-102] — gives crash recovery a single entry point, [`Engine::recover`],
//! that walks the boot flow of [architecture §3.6](../../../docs/02-architecture.md#36-crash-recovery):
//!
//! ```text
//!   boot ─▶ verify sealed segments (checksums)
//!        ─▶ load the last checkpoint (durable WAL fence)
//!        ─▶ replay the WAL forward (idempotent redo)
//!        ─▶ rebuild the delta tier + validity index
//!        ─▶ ready (consistent)
//! ```
//!
//! Every step here already existed as a primitive — segment checksum validation
//! ([`SegmentReader`]), the from-segments index rebuild ([`rebuild_index_from_segments`]),
//! idempotent WAL replay ([`dml::replay`]). The engine is the *driver* that
//! composes them deterministically, plus the one new piece of durable state the
//! flow needs: the checkpoint file (see [`Engine::checkpoint`]).
//!
//! ## What recovery reconstructs, and from where
//!
//! The WAL is the only source of truth (invariant 2). The delta tier and the
//! validity index make **no durability claim** — both discard any stale spill on
//! open and are rebuilt from the log ([`crate::delta`], [`crate::validity`]).
//! Sealed segments are immutable and self-checksummed, so recovery validates them
//! by checksum and rebuilds the supersession/retraction closes they imply
//! ([ADR-0023]); the WAL replay then overlays every close the log recorded. The
//! two agree by construction — a close re-applied from both a segment and the WAL
//! is idempotent ([`ValidityIndex::insert_close`]) — so the rebuilt index is
//! **exactly** the pre-crash one, the property the million-seed sim sweep pins
//! ([STL-102] DoD).
//!
//! ## The checkpoint, and what it is *not* in v0.1
//!
//! [`Engine::checkpoint`] fsyncs the WAL and records the **last fully-flushed WAL
//! offset** to a small checkpoint file — periodically and on
//! graceful shutdown. Per [ADR-0023] the index is rebuilt *from the WAL*, not from
//! a persisted index snapshot, so v0.1 recovery replays the full log and the
//! checkpoint serves as the **durable boundary**: records up to it are committed
//! and must survive a crash; the unsynced tail past it is what a mid-write
//! `kill -9` may tear, and is dropped at the first corrupt record by the WAL's
//! torn-write contract ([`crate::wal`]). Turning the checkpoint into a
//! replay-*skip* (so routine recovery is `checkpoint + tail` over a persisted
//! validity-index checkpoint) is the realignment tracked in STL-133 / STL-136;
//! this engine leaves the seam exactly where they pick it up.

use std::collections::BTreeMap;
use std::io;

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};

use crate::backend::Disk;
use crate::checkpoint;
use crate::delta::{BusinessKey, Delta, DeltaConfig, DeltaError, Snapshot, Version};
use crate::dml::{self, DmlError, DmlOutcome, DmlWriter};
use crate::merge;
use crate::rebuild::rebuild_index_from_segments;
use crate::segment::{SegmentError, SegmentReader};
use crate::systime::SealedVersions;
use crate::validity::{ClosedInterval, ValidityConfig, ValidityError, ValidityIndex};
use crate::validtime::ValidInterval;
use crate::wal::{Checkpoint, LogOffset, Wal, WalConfig, WalError};

/// Filename prefix for the engine's sealed segments on the data disk. Disjoint
/// from the WAL (`wal-*.log`), delta spill (`delta-spill-*.row`), validity spill
/// (`validity-spill-*.row`), and checkpoint (`stele.checkpoint`) namespaces, so
/// the whole engine shares one [`Disk`] without name collisions.
const SEGMENT_FILENAME_PREFIX: &str = "seg-";
/// Filename suffix for sealed segments.
const SEGMENT_FILENAME_SUFFIX: &str = ".seg";

/// Errors surfaced from the engine boot/recovery flow and the write path.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Opening or replaying the WAL failed.
    #[error(transparent)]
    Wal(#[from] WalError),

    /// Opening the delta tier failed.
    #[error(transparent)]
    Delta(#[from] DeltaError),

    /// Opening or rebuilding the validity index failed.
    #[error(transparent)]
    Validity(#[from] ValidityError),

    /// A sealed segment failed checksum validation on boot, or could not be
    /// read — recovery refuses to serve from a corrupt segment store.
    #[error(transparent)]
    Segment(#[from] SegmentError),

    /// A WAL replay record failed to decode/apply, or a write failed.
    #[error(transparent)]
    Dml(#[from] DmlError),

    /// Listing the data disk or reading the checkpoint file failed.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

/// The storage engine handle: one durable WAL, the delta tier, the validity
/// index, and the sealed segment set, over a single shared data [`Disk`].
///
/// Build a fresh engine with [`Engine::open`]; boot from existing on-disk state
/// with [`Engine::recover`]. The two share the same field layout — recovery just
/// reconstructs the non-durable tiers from the log first.
pub struct Engine<C: Clock, D: Disk + Clone> {
    disk: D,
    /// A clone of the writer's WAL handle, for [`Self::checkpoint`].
    wal: Wal<D>,
    writer: DmlWriter<C, D>,
    delta: Delta<D>,
    index: ValidityIndex<D>,
    /// Every version read out of the validated sealed segments — the write
    /// path's [`SealedLookup`](crate::systime::SealedLookup) and the read path's
    /// sealed tier. v0.1 keeps them resident; a per-key segment index is the
    /// follow-up the [`crate::systime`] module anticipates.
    sealed_versions: Vec<Version>,
    /// The validated sealed segment filenames, in sorted order — observability
    /// for tests and a future compaction/manifest hook.
    segment_names: Vec<String>,
    /// The durable WAL fence loaded at recovery / last stored by
    /// [`Self::checkpoint`]. [`None`] means no checkpoint has been taken — replay
    /// covers the whole log.
    checkpoint: Option<LogOffset>,
}

impl<C: Clock, D: Disk + Clone> Engine<C, D> {
    /// Open a **fresh** engine over `disk`. Intended for an empty data disk: the
    /// WAL starts a new log and the tiers start empty. To boot from existing
    /// on-disk state after a crash or shutdown, use [`Engine::recover`] — it
    /// replays the log; this does not.
    ///
    /// `valid_time` mirrors the table's catalog flag ([`DmlWriter::new`]).
    ///
    /// # Errors
    ///
    /// [`EngineError`] if any tier fails to open.
    pub fn open(disk: D, clock: C, valid_time: bool) -> Result<Self, EngineError> {
        let wal = Wal::open(disk.clone(), WalConfig::default())?;
        let delta = Delta::open(disk.clone(), DeltaConfig::default())?;
        let index = ValidityIndex::open(disk.clone(), ValidityConfig::default())?;
        let writer = DmlWriter::new(wal.clone(), clock, valid_time);
        Ok(Self {
            disk,
            wal,
            writer,
            delta,
            index,
            sealed_versions: Vec::new(),
            segment_names: Vec::new(),
            checkpoint: None,
        })
    }

    /// **Recover** an engine from `disk`, walking the boot flow of
    /// [architecture §3.6](../../../docs/02-architecture.md#36-crash-recovery):
    /// validate sealed segments by checksum, load the last checkpoint, replay the
    /// WAL forward, and rebuild the delta tier and validity index — deterministically.
    ///
    /// The result is byte-for-byte the pre-crash state for everything the WAL
    /// made durable: re-running `recover` on the same disk yields the same engine
    /// (idempotent replay), and the rebuilt validity index equals the live one
    /// exactly ([ADR-0023], [STL-102] DoD).
    ///
    /// `valid_time` mirrors the table's catalog flag; `clock` stamps post-recovery
    /// writes — position it past the recovered high-water mark before writing
    /// again (recovery is otherwise read-complete).
    ///
    /// # Errors
    ///
    /// [`EngineError::Segment`] if a sealed segment fails checksum validation;
    /// [`EngineError::Wal`] / [`EngineError::Dml`] if the log cannot be replayed;
    /// [`EngineError`] for any other tier-open or I/O failure.
    pub fn recover(disk: D, clock: C, valid_time: bool) -> Result<Self, EngineError> {
        // 1. Validate every sealed segment by checksum. `SegmentReader::open`
        //    checks the header + footer CRC; reading the versions and retractions
        //    forces every per-column-chunk CRC, so a torn page is caught here and
        //    recovery refuses rather than serving corrupt history.
        let mut segment_names = list_segment_names(&disk)?;
        segment_names.sort();
        let mut sealed_versions = Vec::new();
        let mut sealed_retractions = Vec::new();
        for name in &segment_names {
            let reader = SegmentReader::open(&disk, name)?;
            sealed_versions.extend(reader.read_versions()?);
            sealed_retractions.extend(reader.read_retractions()?);
        }

        // 2. Load the durable checkpoint fence (None ⇒ replay from the beginning,
        //    which is always correct — the WAL is the source of truth).
        let checkpoint = checkpoint::load(&disk)?;

        // 3. Open the non-durable tiers — both discard any stale spill left by the
        //    crashed process; the log is about to repopulate them.
        let wal = Wal::open(disk.clone(), WalConfig::default())?;
        let mut delta = Delta::open(disk.clone(), DeltaConfig::default())?;
        let mut index = ValidityIndex::open(disk.clone(), ValidityConfig::default())?;

        // 4. Rebuild the validity index from the sealed segment store alone —
        //    supersession closes from version adjacency, deletion closes from the
        //    persisted retraction tombstones ([ADR-0023], STL-143).
        rebuild_index_from_segments(
            sealed_versions.iter().cloned(),
            sealed_retractions,
            &mut index,
        )?;

        // 5. Replay the WAL forward, rebuilding the delta tier and overlaying the
        //    log's closes. `insert_close` is write-once but idempotent on an
        //    identical close, so a close already materialized from a segment in
        //    step 4 re-applies cleanly — segment-derived and WAL-derived closes
        //    agree by construction. `recover_replay` tolerates the torn tail of a
        //    mid-write crash: it applies the durable prefix and stops at the first
        //    sheared record ([`dml::recover_replay`]).
        dml::recover_replay(&wal, &mut delta, &mut index, Checkpoint::BEGIN)?;

        let writer = DmlWriter::new(wal.clone(), clock, valid_time);
        Ok(Self {
            disk,
            wal,
            writer,
            delta,
            index,
            sealed_versions,
            segment_names,
            checkpoint,
        })
    }

    /// `INSERT` `key` through the WAL → delta path ([`DmlWriter::insert`]),
    /// consulting the sealed segment set for the duplicate-key check.
    ///
    /// # Errors
    ///
    /// [`EngineError::Dml`] on a resolution, log, or staging failure.
    #[allow(clippy::too_many_arguments)] // mirrors DmlWriter: key/valid/payload + seq + provenance triple
    pub fn insert(
        &mut self,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        let sealed = SealedVersions::new(self.sealed_versions.clone());
        Ok(self.writer.insert(
            &mut self.delta,
            &mut self.index,
            &sealed,
            key,
            valid,
            payload,
            seq,
            txn_id,
            principal,
        )?)
    }

    /// `UPDATE` `key`: close its prior period and open a new one
    /// ([`DmlWriter::update`]).
    ///
    /// # Errors
    ///
    /// [`EngineError::Dml`] on a resolution, log, or staging failure.
    #[allow(clippy::too_many_arguments)] // mirrors DmlWriter: key/valid/payload + seq + provenance triple
    pub fn update(
        &mut self,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        let sealed = SealedVersions::new(self.sealed_versions.clone());
        Ok(self.writer.update(
            &mut self.delta,
            &mut self.index,
            &sealed,
            key,
            valid,
            payload,
            seq,
            txn_id,
            principal,
        )?)
    }

    /// `DELETE` `key`: close its prior period with no successor — a retraction
    /// tombstone ([`DmlWriter::delete`]).
    ///
    /// # Errors
    ///
    /// [`EngineError::Dml`] on a resolution, log, or staging failure.
    pub fn delete(
        &mut self,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        let sealed = SealedVersions::new(self.sealed_versions.clone());
        Ok(self.writer.delete(
            &mut self.delta,
            &mut self.index,
            &sealed,
            key,
            txn_id,
            principal,
        )?)
    }

    /// Take a **checkpoint**: group-commit fsync the WAL, then record the new
    /// durable end as the last fully-flushed offset in the checkpoint file.
    ///
    /// Call this periodically and on graceful shutdown ([STL-102] scope). Returns
    /// the recorded fence.
    ///
    /// # Errors
    ///
    /// [`EngineError::Wal`] if the fsync fails; [`EngineError::Io`] if the
    /// checkpoint file cannot be written.
    pub fn checkpoint(&mut self) -> Result<LogOffset, EngineError> {
        self.wal.tick()?;
        let fence = self.wal.durable_end();
        checkpoint::store(&self.disk, fence)?;
        self.checkpoint = Some(fence);
        Ok(fence)
    }

    /// The version of `key` live at `snapshot` — the `AS OF` read, merging the
    /// delta tier and the sealed segments with the validity index supplying each
    /// version's `sys_to` ([`merge::fold_chains`]). [`None`] if `key` has no
    /// version visible at `snapshot` (never written, or in a deletion gap).
    ///
    /// # Errors
    ///
    /// [`EngineError::Delta`] / [`EngineError::Validity`] if a backing spill
    /// cannot be read.
    pub fn as_of(
        &self,
        key: &BusinessKey,
        snapshot: Snapshot,
    ) -> Result<Option<Version>, EngineError> {
        let mut candidates = self.delta.candidate_versions(key)?;
        candidates.extend(
            self.sealed_versions
                .iter()
                .filter(|v| &v.business_key == key)
                .cloned(),
        );
        let chains = merge::fold_chains(candidates, &self.index)?;
        Ok(merge::resolve_snapshot(&chains, snapshot)
            .into_iter()
            .find(|v| &v.business_key == key))
    }

    /// Convenience over [`Self::as_of`]: just the payload of the live version.
    ///
    /// # Errors
    ///
    /// As [`Self::as_of`].
    pub fn as_of_payload(
        &self,
        key: &BusinessKey,
        snapshot: Snapshot,
    ) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self.as_of(key, snapshot)?.map(|v| v.payload))
    }

    /// Materialize the whole validity index — every `(business_key, sys_from,
    /// seq) → ClosedInterval`. The differential oracle compares this across a
    /// crash to prove the index was rebuilt *exactly* ([STL-102] DoD).
    ///
    /// # Errors
    ///
    /// [`EngineError::Validity`] if a backing spill cannot be read.
    pub fn materialize_index(
        &self,
    ) -> Result<BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval>, EngineError> {
        Ok(self.index.materialize()?)
    }

    /// The durable WAL fence loaded at recovery / last recorded by
    /// [`Self::checkpoint`] — the committed/unsynced boundary. [`None`] if no
    /// checkpoint has been taken.
    #[must_use]
    pub const fn durable_fence(&self) -> Option<LogOffset> {
        self.checkpoint
    }

    /// The validated sealed segment filenames, sorted — observability for tests.
    #[must_use]
    pub fn segment_names(&self) -> &[String] {
        &self.segment_names
    }
}

/// Build the canonical sealed-segment filename for `index`.
pub(crate) fn segment_name(index: u64) -> String {
    format!("{SEGMENT_FILENAME_PREFIX}{index:020}{SEGMENT_FILENAME_SUFFIX}")
}

/// List every sealed-segment filename on `disk` (unsorted — the caller sorts).
fn list_segment_names<D: Disk>(disk: &D) -> io::Result<Vec<String>> {
    Ok(disk
        .list()?
        .into_iter()
        .filter(|name| {
            name.starts_with(SEGMENT_FILENAME_PREFIX) && name.ends_with(SEGMENT_FILENAME_SUFFIX)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_name_round_trips_under_the_prefix_filter() {
        let disk = crate::backend::MemDisk::new();
        // Two segments plus foreign files from the other namespaces sharing the
        // disk — only the `seg-*.seg` files are picked up.
        for n in [0u64, 1] {
            disk.create(&segment_name(n)).expect("create segment file");
        }
        disk.create("wal-00000000000000000000.log").expect("wal");
        disk.create("delta-spill-00000000000000000000.row")
            .expect("delta spill");
        disk.create(checkpoint::CHECKPOINT_FILENAME).expect("ckpt");

        let mut names = list_segment_names(&disk).expect("list");
        names.sort();
        assert_eq!(names, vec![segment_name(0), segment_name(1)]);
    }
}
