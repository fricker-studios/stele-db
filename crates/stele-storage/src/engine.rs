//! The storage engine boot + recovery driver.
//!
//! [`Engine`] bundles the durable WAL, the row-oriented delta tier, the derived
//! validity index, and the sealed segment set behind one handle, and — the point
//! of [STL-102] — gives crash recovery a single entry point, [`Engine::recover`],
//! that walks the boot flow of [architecture §3.6](../../../docs/02-architecture.md#36-crash-recovery):
//!
//! ```text
//!   boot ─▶ load the recovery point (replay floor + durable fence + committed segments)
//!        ─▶ verify the committed sealed segments (checksums); drop orphans
//!        ─▶ rebuild the validity index from the segment store
//!        ─▶ replay the WAL tail from the floor (idempotent redo)
//!        ─▶ rebuild the delta tier + overlay the tail's closes
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
//! ## Two checkpoints: the durable fence, and the flush
//!
//! [`Engine::checkpoint`] is the lightweight one: it fsyncs the WAL and records
//! the **last fully-flushed WAL offset** as the durable boundary — records up to
//! it are committed and must survive a crash; the unsynced tail past it is what a
//! mid-write `kill -9` may tear, dropped at the first corrupt record by the WAL's
//! torn-write contract ([`crate::wal`]). It does not bound replay: recovery still
//! replays from the log origin.
//!
//! [`Engine::flush`] is the **bounding** one ([STL-177]): it seals the in-memory
//! delta tier into a fresh sealed segment, then advances the **replay floor** —
//! the offset recovery resumes from — past the records that segment now covers.
//! Everything before the floor is durable in committed segments and rebuilt from
//! the segment store ([ADR-0023], no persisted index snapshot), so routine
//! recovery is `segment rebuild + tail replay` rather than a full-log scan. The
//! checkpoint file is the manifest that makes a crash *during* a flush safe: a
//! segment is committed only once its checkpoint record (carrying the advanced
//! floor and the bumped committed-segment count) is durable, so a
//! segment written by a torn flush is an **orphan** recovery ignores, falling
//! back to the WAL — the atomic-commit seam STL-133 / STL-136 anticipated.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};

use crate::backend::Disk;
use crate::checkpoint::{self, RecoveryPoint};
use crate::delta::{BusinessKey, Delta, DeltaConfig, DeltaError, Snapshot, Version};
use crate::dml::{self, CommittedTxns, DmlError, DmlOutcome, DmlWriter};
use crate::merge;
use crate::rebuild::rebuild_index_from_segments;
use crate::segment::{SegmentError, SegmentReader, SegmentWriter};
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

/// Default upper bound on rows per row-group when [`Engine::flush`] seals the
/// delta tier into a segment ([STL-197]). A flush wider than this splits into
/// several row-groups at this granularity, so a later scan can skip the chunks
/// of the row-groups holding no live row ([STL-155],
/// [`SegmentReader::read_column_in_row_groups`]) instead of materializing the
/// whole column. Sized off the vectorized batch size ([ADR-0027]: 1024 rows) —
/// large enough to amortize the per-chunk header + zone-map overhead a finer
/// split adds, small enough that a selective scan reads only a few row-groups'
/// worth of payload. Override per engine with
/// [`Engine::with_flush_row_group_rows`].
///
/// [ADR-0027]: ../../../docs/adr/0027-vectorized-execution-model.md
const DEFAULT_FLUSH_ROW_GROUP_ROWS: usize = 1024;

/// The highest commit instant and transaction id an engine's recovered state
/// contains — see [`Engine::recovery_marks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryMarks {
    /// The largest commit instant of any version or close. A caller resuming
    /// writes must stamp them strictly after this.
    pub max_commit: SystemTimeMicros,
    /// The largest transaction id of any version's or close's provenance. A
    /// caller allocating fresh transaction ids must start strictly after this,
    /// or post-restart commits would share provenance with recovered ones.
    pub max_txn_id: u64,
}

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
    /// Whether this table opts into valid-time — mirrors the catalog flag passed
    /// to [`Self::open`]/[`Self::recover`]. Selects the
    /// [`SegmentWriter`](crate::segment::SegmentWriter) flavor a [`Self::flush`]
    /// seals with, and the delta/segment payload framing ([STL-117]).
    valid_time: bool,
    /// A clone of the writer's WAL handle, for [`Self::checkpoint`] / [`Self::flush`].
    wal: Wal<D>,
    writer: DmlWriter<C, D>,
    delta: Delta<D>,
    index: ValidityIndex<D>,
    /// Every version read out of the validated sealed segments — the write
    /// path's [`SealedLookup`](crate::systime::SealedLookup) (passed by reference,
    /// not re-cloned per op) and the read path's sealed tier. v0.1 keeps them
    /// resident; a per-key segment index is the follow-up the [`crate::systime`]
    /// module anticipates.
    sealed: SealedVersions,
    /// The validated, **committed** sealed segment filenames, in sorted order —
    /// observability for tests and the manifest hook a [`Self::flush`] appends to.
    /// Excludes orphan segments left by a torn flush (recovery drops those).
    segment_names: Vec<String>,
    /// The next sealed-segment index a [`Self::flush`] will allocate — equal to
    /// the committed segment count, i.e. `segment_names.len()`
    /// at the last commit. Segment files at this index or above are uncommitted
    /// orphans.
    next_segment_index: u64,
    /// The WAL offset recovery resumes replay from — advanced by [`Self::flush`]
    /// past every record the flushed segments now cover. [`LogOffset::ZERO`] until
    /// the first flush, so a checkpoint-only engine replays the whole log.
    replay_floor: LogOffset,
    /// The durable WAL fence loaded at recovery / last stored by
    /// [`Self::checkpoint`] or [`Self::flush`]. [`None`] means no checkpoint has
    /// been taken — replay covers the whole log and no segment is trusted.
    checkpoint: Option<LogOffset>,
    /// Upper bound on rows per row-group a [`Self::flush`] seals segments with
    /// ([STL-197]). Defaults to [`DEFAULT_FLUSH_ROW_GROUP_ROWS`]; override with
    /// [`Self::with_flush_row_group_rows`]. A flush wider than this splits into
    /// several independently skippable row-groups ([STL-155]).
    flush_row_group_rows: usize,
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
            valid_time,
            wal,
            writer,
            delta,
            index,
            sealed: SealedVersions::new(Vec::new()),
            segment_names: Vec::new(),
            next_segment_index: 0,
            replay_floor: LogOffset::ZERO,
            checkpoint: None,
            flush_row_group_rows: DEFAULT_FLUSH_ROW_GROUP_ROWS,
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
    /// exactly ([ADR-0023], [STL-102] DoD). v0.1 replays the **full** WAL and
    /// rebuilds the index from it (not from a persisted index snapshot, per
    /// [ADR-0023]); the loaded checkpoint is the *durability fence* that gates
    /// torn-tail tolerance, not yet a replay-skip (the STL-133 / STL-136 seam).
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
        // A single table's WAL has no cross-table commit to gate on, so every record
        // applies — the per-table sims/tests that drive this never write two-phase
        // records ([STL-215]). The session recovery driver, which *does* coordinate
        // across tables, uses `recover_with_commits` instead.
        Self::recover_with_commits(disk, clock, valid_time, &CommittedTxns::All)
    }

    /// [`recover`](Self::recover), but gating **two-phase** WAL records on the set of
    /// transactions whose commit marker is durable ([STL-215]). A record tagged as a
    /// leg of a multi-table `COMMIT` is replayed only if `committed` admits its
    /// transaction; otherwise the marker never became durable and the leg is
    /// discarded, so the transaction recovers all-or-none across every table it
    /// wrote. Plain (single-table / auto-commit) records always apply. The session
    /// recovery driver builds `committed` from the engine commit log and passes it to
    /// every table's recover.
    ///
    /// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
    ///
    /// # Errors
    ///
    /// As [`recover`](Self::recover).
    pub fn recover_with_commits(
        disk: D,
        clock: C,
        valid_time: bool,
        committed: &CommittedTxns,
    ) -> Result<Self, EngineError> {
        // 1. Load the durable recovery point: where replay resumes (`floor`), the
        //    torn-tail boundary (`fence`), and how many sealed segments committed
        //    flushes vouched (`segment_count`). No record ⇒ no flush ever
        //    committed: replay the whole log from the origin and trust no segment.
        let recovery = checkpoint::load(&disk)?;
        let (floor, fence, segment_count) = recovery
            .map_or((LogOffset::ZERO, LogOffset::ZERO, 0), |p: RecoveryPoint| {
                (p.replay_floor, p.durable_fence, p.segment_count)
            });

        // 2. Drop orphan segments — any `seg-*` at or above the committed count was
        //    written by a flush whose checkpoint record never became durable. The
        //    records they hold are still in the WAL (the flush advances the floor
        //    only *after* its checkpoint record is durable), so the orphan is
        //    re-flushed from replay. Removing it keeps the next flush's `create`
        //    from colliding; best-effort, since a leftover only forces an
        //    overwrite next time ([STL-177] crash-during-flush safety).
        for name in &list_segment_names(&disk)? {
            let committed = segment_index_of(name).is_some_and(|idx| idx < segment_count);
            if !committed {
                let _ = disk.remove(name);
            }
        }

        // 3. Validate and read the committed segments (`seg-0 … seg-{count-1}`) by
        //    checksum. `SegmentReader::open` checks the header + footer CRC; reading
        //    the versions and retractions forces every per-column-chunk CRC, so a
        //    torn page in a *committed* segment is caught here and recovery refuses
        //    rather than serving corrupt history.
        let segment_names: Vec<String> = (0..segment_count).map(segment_name).collect();
        let mut sealed_versions = Vec::new();
        let mut sealed_retractions = Vec::new();
        for name in &segment_names {
            let reader = SegmentReader::open(&disk, name)?;
            sealed_versions.extend(reader.read_versions()?);
            sealed_retractions.extend(reader.read_retractions()?);
        }

        // 4. Open the non-durable tiers — both discard any stale spill left by the
        //    crashed process; the log is about to repopulate them.
        let wal = Wal::open(disk.clone(), WalConfig::default())?;
        let mut delta = Delta::open(disk.clone(), DeltaConfig::default())?;
        let mut index = ValidityIndex::open(disk.clone(), ValidityConfig::default())?;

        // 5. Rebuild the validity index from the committed segment store alone —
        //    supersession closes from version adjacency, deletion closes from the
        //    persisted retraction tombstones ([ADR-0023], STL-143).
        rebuild_index_from_segments(
            sealed_versions.iter().cloned(),
            sealed_retractions,
            &mut index,
        )?;

        // 6. Replay the WAL **tail** — from `floor` forward, not the log origin.
        //    Everything before the floor is durable in the committed segments
        //    rebuilt in step 5; replaying it again would only re-derive the same
        //    versions (deduped by `(sys_from, seq)`) and the same write-once
        //    closes, so bounding the replay is a pure speedup, not a semantic
        //    change. `recover_replay` tolerates a torn record past the durable
        //    `fence` (the unsynced tail of a mid-write crash) while treating
        //    corruption *before* the fence — a committed-but-unflushed record — as
        //    a fatal fault ([`dml::recover_replay`], [STL-177]).
        dml::recover_replay(
            &wal,
            &mut delta,
            &mut index,
            Checkpoint(floor),
            fence,
            committed,
        )?;

        let writer = DmlWriter::new(wal.clone(), clock, valid_time);
        Ok(Self {
            disk,
            valid_time,
            wal,
            writer,
            delta,
            index,
            sealed: SealedVersions::new(sealed_versions),
            segment_names,
            next_segment_index: segment_count,
            replay_floor: floor,
            checkpoint: recovery.map(|p| p.durable_fence),
            flush_row_group_rows: DEFAULT_FLUSH_ROW_GROUP_ROWS,
        })
    }

    /// Override the rows-per-row-group bound [`Self::flush`] seals segments with
    /// ([STL-197]), replacing the `DEFAULT_FLUSH_ROW_GROUP_ROWS` default. A
    /// smaller bound splits a flush into more, finer row-groups — each
    /// independently skippable by the read path ([STL-155]) — at the cost of more
    /// per-chunk overhead; `0` is clamped to `1`, the same clamp
    /// [`SegmentWriter::with_max_row_group_rows`] applies. Builder-style so the
    /// many [`open`](Self::open) / [`recover`](Self::recover) call sites that want
    /// the default stay untouched; the flush-recovery sim sweeps and the
    /// read-accounting tests seed a small bound through it.
    #[must_use]
    pub fn with_flush_row_group_rows(mut self, rows: usize) -> Self {
        self.flush_row_group_rows = rows.max(1);
        self
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
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        Ok(self.writer.insert(
            &mut self.delta,
            &mut self.index,
            &self.sealed,
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
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<DmlOutcome, EngineError> {
        Ok(self.writer.update(
            &mut self.delta,
            &mut self.index,
            &self.sealed,
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
        Ok(self.writer.delete(
            &mut self.delta,
            &mut self.index,
            &self.sealed,
            key,
            txn_id,
            principal,
        )?)
    }

    /// Close every business key system-live at `at`, appending a retraction (a
    /// close with no successor) for each — the storage half of `DROP TABLE`
    /// ([STL-211]). Returns the number of rows closed.
    ///
    /// A `DROP TABLE` is otherwise a catalog-only transition (the schema chain's
    /// open tail closes); the rows in the tier are never touched. Because the
    /// session keeps a dropped table's tier resident to preserve history — and
    /// reuses it if the name is re-created — a name re-created on that tier would
    /// inherit the dropped era's still-open rows in a *current* read, and
    /// re-inserting one of their business keys would be refused as a duplicate
    /// ([`DmlOutcome`]'s `KeyExists`). Retracting the live rows fixes both: they
    /// ceased to be asserted the moment the table ceased to exist.
    ///
    /// `at` is the **liveness snapshot**, not the close timestamp. It selects
    /// which versions to retract — every still-open version has `sys_from ≤ at`,
    /// so it resolves live; a version opened later (the re-created era) has
    /// `sys_from > at` and is left untouched. Each retraction then commits at a
    /// fresh instant drawn from the writer's clock, strictly after `at` under a
    /// monotonic clock: the closes are *new* post-drop assertions on the system
    /// axis, not back-dated to `at`. Pass `at` as the drop instant (the commit
    /// clock's current high-water) so exactly the era's live rows are selected.
    ///
    /// Append-only, per [ADR-0023]: each close stamps a write-once `sys_to` on
    /// the prior version and mutates no committed record, so an `AS OF` read
    /// **inside** the dropped era (any snapshot `< at`) still resolves the row
    /// as open then — exactly as before the drop.
    ///
    /// Each close is an independent auto-commit retraction, mirroring a sequence
    /// of `DELETE`s; closing one key cannot change another's liveness, so the
    /// open set resolved up front is stable across the loop.
    ///
    /// [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
    /// [STL-211]: https://allegromusic.atlassian.net/browse/STL-211
    ///
    /// # Errors
    ///
    /// [`EngineError::Delta`] / [`EngineError::Validity`] if enumerating the
    /// live keys or resolving them fails; [`EngineError::Dml`] if staging a
    /// close fails.
    pub fn close_all_open(
        &mut self,
        at: Snapshot,
        txn_id: TxnId,
        principal: &Principal,
    ) -> Result<usize, EngineError> {
        // Every distinct business key across both tiers, narrowed to those with
        // a version open at `at`. A `delete` needs a live version to close, so
        // the filter is also what skips a key already retired in a deletion gap.
        let mut open: Vec<BusinessKey> = Vec::new();
        for key in self.resident_keys()? {
            if self.as_of(&key, at)?.is_some() {
                open.push(key);
            }
        }
        for key in &open {
            self.delete(key, txn_id, principal.clone())?;
        }
        Ok(open.len())
    }

    /// **Recovery re-derivation** of a `DROP TABLE`'s storage closes ([STL-220]).
    ///
    /// [`close_all_open`](Self::close_all_open) is an *auto-commit* sequence of
    /// closes — its WAL records are durability-deferred, exactly like any
    /// `INSERT`/`DELETE` — but the session's catalog `DropTable` record is
    /// fsynced (the DDL acknowledgement point, [ADR-0028]). A crash in the window
    /// after that fsync but before the closes reach the WAL would recover the
    /// catalog name dropped yet the rows still system-live, re-opening the
    /// [STL-211] leak on a later re-create. This re-applies the drop's closes
    /// from the durable catalog record at recovery, so the retired state is a
    /// pure function of the fsynced log and needs no separate durability.
    ///
    /// Unlike `close_all_open` — which, called live with `at = now`, selects
    /// every currently-open row — this is **idempotent and re-created-era safe**,
    /// because at recovery `at` (the drop instant) is in the *past*:
    ///
    /// * It closes a key only if that key's **current** open version (resolved at
    ///   `now`, the recovered high-water) began at or before the drop
    ///   (`sys_from <= at`). A row the live close already made durable has no open
    ///   version now and is skipped — re-running converges to the same state.
    /// * A row opened in a *re-created* era has `sys_from > at` and is left
    ///   untouched, so a name re-created before the crash keeps its new era while
    ///   the dropped era is retired.
    ///
    /// Re-deriving only the **latest** drop per name is sufficient: the tier WAL
    /// is append-only, so a lost close implies every later record (including a
    /// re-created era's inserts) was lost too — at most one era is ever open at
    /// recovery, and the `sys_from <= at` guard closes it iff it predates the
    /// drop. Each close commits at a fresh post-recovery instant drawn from the
    /// clock (`> now`), an append-only retraction per [ADR-0023]; an `AS OF` read
    /// inside the dropped era is unaffected. Returns the number of rows closed.
    ///
    /// `now` must be the recovered high-water (every open version has
    /// `sys_from <= now`, so it resolves live there). Pass `at` as the drop
    /// instant recorded in the catalog log.
    ///
    /// [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    /// [STL-211]: https://allegromusic.atlassian.net/browse/STL-211
    /// [STL-220]: https://allegromusic.atlassian.net/browse/STL-220
    ///
    /// # Errors
    ///
    /// [`EngineError::Delta`] / [`EngineError::Validity`] if enumerating the
    /// live keys or resolving them fails; [`EngineError::Dml`] if staging a
    /// close fails.
    pub fn close_dropped_era(
        &mut self,
        at: Snapshot,
        now: Snapshot,
        txn_id: TxnId,
        principal: &Principal,
    ) -> Result<usize, EngineError> {
        let mut stale: Vec<BusinessKey> = Vec::new();
        for key in self.resident_keys()? {
            // Close the key only if its *current* open version belongs to the
            // dropped era. A key already retired by the live close has no open
            // version now (skipped); a key opened in a re-created era resolves to
            // `sys_from > at` (preserved). `delete` then closes exactly this
            // resolved version, so the two snapshots stay consistent.
            if let Some(cur) = self.as_of(&key, now)?
                && cur.sys_from <= at.0
            {
                stale.push(key);
            }
        }
        for key in &stale {
            self.delete(key, txn_id, principal.clone())?;
        }
        Ok(stale.len())
    }

    /// Every distinct business key resident across both tiers (delta + sealed) —
    /// the shared key enumeration of [`close_all_open`](Self::close_all_open) and
    /// [`close_dropped_era`](Self::close_dropped_era).
    fn resident_keys(&self) -> Result<BTreeSet<BusinessKey>, EngineError> {
        let mut keys: BTreeSet<BusinessKey> = BTreeSet::new();
        // `staged_versions` hands back owned `Version`s, so the key moves out;
        // `sealed.versions` borrows the resident set, so its key is cloned.
        for v in self.delta.staged_versions()? {
            keys.insert(v.business_key);
        }
        for v in self.sealed.versions() {
            keys.insert(v.business_key.clone());
        }
        Ok(keys)
    }

    /// Open a **group-commit** buffer ([`DmlWriter::begin_group`], [STL-192]).
    ///
    /// Until [`commit_group`](Self::commit_group) or [`abort_group`](Self::abort_group),
    /// each [`insert`](Self::insert) / [`update`](Self::update) / [`delete`](Self::delete)
    /// applies to the delta/index but defers its WAL record, so a multi-statement
    /// transaction can be logged as one record and made durable with one fsync —
    /// the crash-atomic boundary that recovers all-or-none.
    pub fn begin_group(&mut self) {
        self.writer.begin_group();
    }

    /// Group-commit the open buffer ([`DmlWriter::commit_group`], [STL-192]): append
    /// the transaction's writes as a single WAL record and fsync once. Returns the
    /// durable end after the fsync.
    ///
    /// # Errors
    ///
    /// [`EngineError::Dml`] if the append or fsync fails. A torn or unwritten append
    /// recovers to nothing; but if the append succeeds and only the fsync fails the
    /// staged record's durability is **indeterminate** (a later `tick` could
    /// otherwise still flush it). That fsync failure now **poisons** the shared WAL
    /// ([STL-217]): every subsequent write through this engine is refused
    /// ([`is_poisoned`](Self::is_poisoned)) until the operator restarts into
    /// [`recover`](Self::recover), so the staged record can never be flushed as a
    /// clean op — see [`DmlWriter::commit_group`].
    pub fn commit_group(&mut self) -> Result<LogOffset, EngineError> {
        Ok(self.writer.commit_group()?)
    }

    /// Group-commit the open buffer as **one leg of a multi-table transaction**
    /// ([`DmlWriter::commit_group_two_phase`], [STL-215]): append the writes as a
    /// single two-phase WAL record tagged with `txn_id` and fsync once. The leg is
    /// durable but inert until the session driver appends `txn_id`'s commit marker
    /// after every table's leg is durable; recovery replays it only if that marker
    /// is present, so the transaction recovers all-or-none across tables. Returns the
    /// durable end after the fsync.
    ///
    /// [STL-215]: https://allegromusic.atlassian.net/browse/STL-215
    ///
    /// # Errors
    ///
    /// As [`commit_group`](Self::commit_group).
    pub fn commit_group_two_phase(&mut self, txn_id: TxnId) -> Result<LogOffset, EngineError> {
        Ok(self.writer.commit_group_two_phase(txn_id)?)
    }

    /// Discard the open group buffer without logging it — the transaction aborted
    /// ([`DmlWriter::abort_group`], [STL-192], [STL-216]). The buffered writes were
    /// applied to the delta/index as they were staged, so this **rolls them back in
    /// place**: with no WAL record the aborted writes are never durable, and undoing
    /// them in memory leaves the live engine matching what a crash recovery would
    /// reconstruct — none of the transaction's writes — without a restart.
    pub fn abort_group(&mut self) {
        self.writer.abort_group(&mut self.delta, &mut self.index);
    }

    /// Whether the engine's WAL is **poisoned** — a prior fsync
    /// ([`commit_group`](Self::commit_group), [`checkpoint`](Self::checkpoint), or
    /// [`flush`](Self::flush)) failed, so its staged record's durability is
    /// indeterminate. Per the WAL contract (invariant 2) that is a crash, not a
    /// clean abort: every subsequent write is refused with [`WalError::Poisoned`]
    /// until the operator restarts into [`recover`](Self::recover), which opens a
    /// fresh, unpoisoned WAL ([STL-217]). An operator — or the session engine that
    /// wraps a set of these tables — observing a poisoned engine must stop serving
    /// and recover.
    ///
    /// [STL-217]: https://allegromusic.atlassian.net/browse/STL-217
    pub fn is_poisoned(&self) -> bool {
        self.wal.is_poisoned()
    }

    /// Take a **checkpoint**: group-commit fsync the WAL, then record the new
    /// durable end as the last fully-flushed offset in the checkpoint file. Leaves
    /// the replay floor and committed-segment count untouched — this is the
    /// lightweight durability fence, not a flush ([`Self::flush`]).
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
        checkpoint::store(
            &self.disk,
            RecoveryPoint {
                replay_floor: self.replay_floor,
                durable_fence: fence,
                segment_count: self.next_segment_index,
            },
        )?;
        self.checkpoint = Some(fence);
        Ok(fence)
    }

    /// **Flush** the delta tier into a fresh sealed segment and advance the replay
    /// floor past the records it now covers, so the next recovery replays only the
    /// WAL tail ([STL-177], [feature-plan B.5]). Returns the new replay floor.
    ///
    /// The sequence is ordered for crash safety:
    ///
    /// 1. `fsync` the WAL — everything up to `fence` is now durable.
    /// 2. **Snapshot** the delta's staged versions and retractions *without*
    ///    draining ([`Delta::staged_versions`]) — a later failure leaves the tier
    ///    intact and the WAL authoritative.
    /// 3. Seal them into `seg-{next}` and `fsync` it (an orphan at that index from
    ///    a prior torn flush is removed first, so `create` never collides).
    /// 4. Append the checkpoint record — `{floor = fence, fence, segment_count+1}`.
    ///    **This is the atomic commit point**: until it is durable the new segment
    ///    is an orphan recovery ignores, replaying the WAL instead.
    /// 5. Only now drop the flushed rows from the delta and fold them into the
    ///    resident sealed set, advancing the floor and committed count.
    ///
    /// A flush with an empty delta records the (unchanged) floor as a degenerate
    /// checkpoint and writes no segment.
    ///
    /// # Errors
    ///
    /// [`EngineError::Wal`] on the fsync, [`EngineError::Delta`] reading staged
    /// rows, [`EngineError::Segment`] sealing the segment, or [`EngineError::Io`]
    /// writing the checkpoint record. On any error the delta tier is unchanged.
    pub fn flush(&mut self) -> Result<LogOffset, EngineError> {
        // 1. Make the WAL durable; `fence` is the post-fsync end of the log.
        self.wal.tick()?;
        let fence = self.wal.durable_end();

        // 2. Snapshot the delta without draining it.
        let versions = self.delta.staged_versions()?;
        let retractions = self.delta.staged_retractions();
        if versions.is_empty() && retractions.is_empty() {
            // Nothing staged ⇒ no unflushed records since the last flush, so the
            // floor is already current. Record the fence (a degenerate flush) and
            // leave the floor / count untouched.
            checkpoint::store(
                &self.disk,
                RecoveryPoint {
                    replay_floor: self.replay_floor,
                    durable_fence: fence,
                    segment_count: self.next_segment_index,
                },
            )?;
            self.checkpoint = Some(fence);
            return Ok(self.replay_floor);
        }

        // 3. Seal the snapshot into the next segment. Clear any orphan first.
        let idx = self.next_segment_index;
        let name = segment_name(idx);
        let _ = self.disk.remove(&name);
        // Bound each row-group so a wide flush splits into several skippable
        // row-groups ([STL-197]) — the same `with_max_row_group_rows` knob the
        // segment tests and the SnapshotScan oracle drive, now sourced from the
        // engine's flush policy. The default ([`DEFAULT_FLUSH_ROW_GROUP_ROWS`])
        // keeps narrow flushes a single row-group, byte-identical to before.
        let mut writer = if self.valid_time {
            SegmentWriter::create_valid_time(&self.disk, &name)?
        } else {
            SegmentWriter::create(&self.disk, &name)?
        }
        .with_max_row_group_rows(self.flush_row_group_rows);
        for v in &versions {
            writer.push(v.clone())?;
        }
        for c in retractions {
            writer.push_retraction(c)?;
        }
        writer.finish()?; // fsyncs — the segment is now durable
        // Directory fence ([STL-232]): make the segment's *entry* durable
        // before the checkpoint vouches for it. Without this, a crash could
        // keep the (appended, fsync'd) manifest record while losing the
        // just-created file it names — recovery would then fail to open a
        // vouched segment instead of ignoring an orphan.
        self.disk.sync_dir()?;

        // 4. Commit: the checkpoint record vouches the segment and advances the
        //    floor past every record it covers.
        checkpoint::store(
            &self.disk,
            RecoveryPoint {
                replay_floor: fence,
                durable_fence: fence,
                segment_count: idx + 1,
            },
        )?;

        // 5. The segment and its committing record are durable, so adopt them.
        //    Advance the in-memory recovery point and fold the versions into the
        //    resident sealed set *first* — `extend` appends in place, so a flush
        //    stays O(flushed batch), not O(total sealed history). Only then free
        //    the delta's now-redundant staged rows, **best-effort**: a cleanup
        //    failure must not abort after the commit point and strand the
        //    in-memory state behind the durable manifest (a later flush would then
        //    overwrite the just-committed segment).
        self.sealed.extend(versions);
        self.segment_names.push(name);
        self.next_segment_index = idx + 1;
        self.replay_floor = fence;
        self.checkpoint = Some(fence);
        self.delta.discard_flushed();
        Ok(fence)
    }

    /// The version of `key` live at `snapshot` — the `AS OF` read, merging the
    /// delta tier and the sealed segments with the validity index supplying each
    /// version's `sys_to` ([`merge::resolve_open`]). [`None`] if `key` has no
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
        // The purpose-built single-key resolver: it folds only this key's delta +
        // sealed candidates, overlays just its closes, and returns the version live
        // at `snapshot` — no full-keyset chain build for a point lookup.
        let delta_candidates = self.delta.candidate_versions(key)?;
        Ok(merge::resolve_open(
            &delta_candidates,
            self.sealed.versions(),
            &self.index,
            key,
            snapshot,
        )?)
    }

    /// Convenience over [`Self::as_of`]: just the payload of the live version.
    ///
    /// The outer `Option` is row presence (`None` ⇒ no live version at the
    /// snapshot); the inner `Option` is the payload, which is itself `None` for a
    /// SQL `NULL` cell ([STL-154]) — kept distinct from `Some(vec![])` (an empty
    /// payload).
    ///
    /// # Errors
    ///
    /// As [`Self::as_of`].
    pub fn as_of_payload(
        &self,
        key: &BusinessKey,
        snapshot: Snapshot,
    ) -> Result<Option<Option<Vec<u8>>>, EngineError> {
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

    /// The highest commit instant and transaction id present in this engine's
    /// state — over every version (delta **and** sealed) and every close
    /// (supersession or retraction) the validity index holds.
    ///
    /// The session-level recovery driver ([STL-210], [ADR-0028]) reads this
    /// right after [`Self::recover`] to position its commit clock and
    /// transaction-id allocator strictly past everything the table already
    /// committed — the "position the clock past the recovered high-water mark"
    /// obligation [`Self::recover`]'s docs place on the caller. Zeros when the
    /// table has never committed anything.
    ///
    /// [STL-210]: https://allegromusic.atlassian.net/browse/STL-210
    /// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
    ///
    /// # Errors
    ///
    /// [`EngineError::Delta`] / [`EngineError::Validity`] if a backing spill
    /// cannot be read.
    pub fn recovery_marks(&self) -> Result<RecoveryMarks, EngineError> {
        let mut marks = RecoveryMarks {
            max_commit: SystemTimeMicros(0),
            max_txn_id: 0,
        };
        let mut fold = |committed_at: SystemTimeMicros, txn_id: u64| {
            marks.max_commit = marks.max_commit.max(committed_at);
            marks.max_txn_id = marks.max_txn_id.max(txn_id);
        };
        for v in &self.delta.staged_versions()? {
            fold(v.provenance.committed_at, v.provenance.txn_id.0);
        }
        for v in self.sealed.versions() {
            fold(v.provenance.committed_at, v.provenance.txn_id.0);
        }
        // A close is its own commit (a supersession's or a delete's): its
        // `closed_by` provenance carries the closing transaction's instant/id,
        // which for a deletion is not represented by any version row.
        for interval in self.index.materialize()?.values() {
            fold(interval.closed_by.committed_at, interval.closed_by.txn_id.0);
        }
        Ok(marks)
    }

    /// The durable WAL fence loaded at recovery / last recorded by
    /// [`Self::checkpoint`] or [`Self::flush`] — the committed/unsynced boundary.
    /// [`None`] if no checkpoint has been taken.
    #[must_use]
    pub const fn durable_fence(&self) -> Option<LogOffset> {
        self.checkpoint
    }

    /// The WAL offset recovery would resume replay from — advanced by
    /// [`Self::flush`] past the records the sealed segments now cover.
    /// [`LogOffset::ZERO`] until the first flush. Observability for the
    /// bounded-replay tests ([STL-177]).
    #[must_use]
    pub const fn replay_floor(&self) -> LogOffset {
        self.replay_floor
    }

    /// The validated sealed segment filenames, sorted — observability for tests.
    #[must_use]
    pub fn segment_names(&self) -> &[String] {
        &self.segment_names
    }

    /// The delta tier handle, for a read operator (the `SnapshotScan` in
    /// `stele-exec`) that needs to fold the resident rows itself. The point-lookup
    /// [`as_of`](Self::as_of) merges the tiers internally; a full vectorized scan
    /// borrows the tiers directly via this and [`index`](Self::index) /
    /// [`open_segment_readers`](Self::open_segment_readers).
    #[must_use]
    pub const fn delta(&self) -> &Delta<D> {
        &self.delta
    }

    /// The validity index handle — the system-time `sys_to` ends a read operator
    /// overlays onto the delta + sealed candidates. See [`delta`](Self::delta).
    #[must_use]
    pub const fn index(&self) -> &ValidityIndex<D> {
        &self.index
    }

    /// Re-open a [`SegmentReader`] over each validated sealed segment, in sorted
    /// order — the sealed tier a full scan reads alongside the [`delta`](Self::delta)
    /// tier. Each reader re-validates the segment header/footer CRC on open; the
    /// engine keeps only the segment *names* resident (not open file handles), so
    /// a scan materializes readers on demand here.
    ///
    /// # Errors
    ///
    /// [`EngineError::Segment`] if a segment fails to open or re-validate.
    pub fn open_segment_readers(&self) -> Result<Vec<SegmentReader<D::File>>, EngineError> {
        self.segment_names
            .iter()
            .map(|name| SegmentReader::open(&self.disk, name).map_err(EngineError::from))
            .collect()
    }
}

/// Build the canonical sealed-segment filename for `index`.
pub(crate) fn segment_name(index: u64) -> String {
    format!("{SEGMENT_FILENAME_PREFIX}{index:020}{SEGMENT_FILENAME_SUFFIX}")
}

/// Parse the segment index out of a `seg-{index:020}.seg` filename, or [`None`]
/// if `name` is not a sealed-segment filename — the inverse of [`segment_name`],
/// used to tell a committed segment from an orphan by index.
fn segment_index_of(name: &str) -> Option<u64> {
    name.strip_prefix(SEGMENT_FILENAME_PREFIX)?
        .strip_suffix(SEGMENT_FILENAME_SUFFIX)?
        .parse()
        .ok()
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

    #[test]
    fn segment_index_of_inverts_segment_name() {
        // The orphan/committed partition depends on parsing the index back out of
        // a segment filename — the exact inverse of `segment_name`.
        for n in [0u64, 1, 42, u64::from(u32::MAX)] {
            assert_eq!(segment_index_of(&segment_name(n)), Some(n));
        }
        // Foreign names from the other disk namespaces are not segments.
        assert_eq!(segment_index_of("wal-00000000000000000000.log"), None);
        assert_eq!(segment_index_of(checkpoint::CHECKPOINT_FILENAME), None);
        assert_eq!(segment_index_of("seg-not-a-number.seg"), None);
    }

    /// A monotonic step clock — each reading is one µs past the last, so successive
    /// writes get distinct, increasing `sys_from`s.
    struct StepClock(std::sync::atomic::AtomicI64);
    impl Clock for StepClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
        }
    }

    /// A failed WAL fsync inside [`Engine::checkpoint`] poisons the engine
    /// ([STL-217]): it then refuses every further write, and recovery from the same
    /// disk reconstructs the committed prefix that *was* written before the failure.
    #[test]
    fn a_failed_fsync_poisons_the_engine_and_recovery_is_sound() {
        use crate::backend::{FaultOp, Faults, MemDisk};

        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        let principal = Principal::new(b"op".to_vec());
        let key = BusinessKey::new(b"k".to_vec());

        let mut engine = Engine::open(
            disk.clone(),
            StepClock(std::sync::atomic::AtomicI64::new(0)),
            false,
        )
        .expect("open");
        // One auto-commit insert reaches the WAL (append-only, not yet fsynced).
        let inserted = engine
            .insert(
                key.clone(),
                None,
                Some(b"v1".to_vec()),
                0,
                TxnId(1),
                principal.clone(),
            )
            .expect("insert");

        // The next fsync fails: the durability point of a `checkpoint`.
        faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
        assert!(
            engine.checkpoint().is_err(),
            "the injected fsync fault fails the checkpoint"
        );
        assert!(engine.is_poisoned(), "a failed fsync poisons the engine");

        // Further writes are refused at the WAL append — even though the scheduled
        // fault was already consumed, the poison stands until recovery. (A fresh
        // key, so resolution succeeds and the write actually reaches the log.)
        let key2 = BusinessKey::new(b"k2".to_vec());
        let err = engine
            .insert(key2, None, Some(b"v2".to_vec()), 0, TxnId(2), principal)
            .expect_err("a poisoned engine refuses writes");
        assert!(matches!(
            err,
            EngineError::Dml(DmlError::Wal(WalError::Poisoned))
        ));
        assert!(
            engine.checkpoint().is_err(),
            "a poisoned checkpoint is refused"
        );
        drop(engine);

        // Recovery opens a fresh, unpoisoned WAL and replays the log. The first
        // insert's record was appended before the failed fsync, so the recovered
        // engine serves it; the refused second insert never reached the log.
        let recovered = Engine::recover(
            disk,
            StepClock(std::sync::atomic::AtomicI64::new(1_000_000)),
            false,
        )
        .expect("recover");
        assert!(!recovered.is_poisoned(), "recovery starts unpoisoned");
        assert_eq!(
            recovered
                .as_of_payload(&key, Snapshot(inserted.commit))
                .expect("as_of")
                .flatten()
                .as_deref(),
            Some(b"v1".as_slice()),
            "the committed-before-failure write survives recovery",
        );
    }
}
