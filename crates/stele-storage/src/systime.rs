//! System-time versioning — the spine of the bitemporal record model.
//!
//! Every logical row carries a system-time interval `[sys_from, sys_to)`: when
//! the *database* held this version ([architecture §2](../../../docs/02-architecture.md#2-the-bitemporal-record-model)).
//! `sys_to = `[`SYSTEM_TIME_OPEN`] (`+∞`) marks the current version. System time
//! is **always present** — invariant 4 of [architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)
//! and [assumption O3](../../../docs/assumptions.md).
//!
//! This module owns the *write-path temporal resolution* the executor performs
//! before staging rows into the delta tier
//! ([architecture §3.4](../../../docs/02-architecture.md#34-write-path-sequence)):
//!
//! * **`sys_from` is stamped, never supplied.** A writer hands over a key, a
//!   payload, and the commit's provenance (`txn_id` + `principal`);
//!   [`SysTimeWriter`] sets `sys_from` to the transaction's commit timestamp.
//!   There is no API that lets a caller choose it. `committed_at` is stamped the
//!   same way (it equals `sys_from` on this path); `txn_id` and `principal` are
//!   the caller's to supply — the transaction manager hands them down at commit
//!   ([architecture §8](../../../docs/02-architecture.md#8-lineage--provenance-subsystem),
//!   invariant 5). Provenance is stored inline on the version, never
//!   reconstructed.
//! * **Updates close the prior period.** A [`SysTimeWriter::update`] stages a
//!   new open version *and* a [`Close`] that materializes the previous version's
//!   `sys_to` into the [validity index](crate::validity) (it abuts: the old
//!   period ends exactly where the new one begins). A [`SysTimeWriter::delete`]
//!   closes without re-opening — the "tombstone = logical period-close" of
//!   [architecture §3.1](../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving).
//!
//! ## A close is an appended index entry, never a record mutation ([ADR-0023])
//!
//! A committed version is append-only and is **never rewritten** to record its
//! end. Closing the prior period materializes its `sys_to` **once** into the
//! [validity index](crate::validity), keyed `(business_key, sys_from)` — a
//! write-once append, regardless of whether the version body still lives in the
//! delta tier or has been flushed into a sealed segment. The read path overlays
//! that materialized end at resolution time ([`crate::merge`]). This is what
//! replaced the older "re-stage the closed version" / "append a close marker"
//! machinery (STL-91/127): there is now a single close mechanism for both tiers.
//!
//! The SQL DML surface (INSERT / UPDATE / DELETE statements) is built on top of
//! these primitives in [STL-94]; this module is the engine mechanism, not the
//! SQL layer.
//!
//! ## Commit-timestamp monotonicity
//!
//! The per-key chain is totally ordered by `(sys_from, seq)`: the **real** commit
//! µs first, then the per-commit sequence number that breaks a same-tick tie
//! ([ADR-0024](../../../docs/adr/0024-time-representation.md)). [`SysTimeWriter`]
//! stamps `sys_from` with exactly what the clock reads and **never mutates it** —
//! two commits at the same µs keep that µs and are ordered by their distinct
//! `seq` (STL-145). This replaced the older `max(clock.now(), previous + 1)`
//! *force-bump*, which manufactured a strictly-increasing `sys_from` by lying
//! about the timestamp; the honest µs plus `seq` is the [ADR-0024] model.
//!
//! **The monotonicity guard survives only as a non-regression check.** The
//! writer still tracks the high-water mark of the `sys_from` it has stamped, but
//! it no longer bumps a stalled reading — it only *rejects* a clock that reads
//! strictly **earlier** than that mark ([`SysTimeError::ClockRegressed`]), since
//! a backwards-moving clock would let a new version sort before an existing one.
//! A repeated (stalled) tick is allowed; a regressing one is refused.
//!
//! **Scope of the guard.** The high-water mark lives *in the writer instance* —
//! it starts empty on [`SysTimeWriter::new`] and resets if the writer is
//! recreated (e.g. after a restart). So the non-regression guarantee holds
//! *within one writer's lifetime*, not across restarts: a caller that constructs
//! a fresh writer must supply a commit clock that does not read earlier than the
//! newest `sys_from` already persisted, and a `seq` that continues to increase.
//! Re-establishing that high-water mark on recovery, and global commit ordering
//! across transactions and (later) nodes, is the transaction manager's job
//! ([architecture §9](../../../docs/02-architecture.md#9-transaction--concurrency-model),
//! [ADR-0022](../../../docs/adr/0022-clock-synchronization-and-ordering.md)); this
//! guard is what keeps the single-writer storage path correct on its own.
//!
//! ## Closing across a flush boundary
//!
//! Once a key's open version has been flushed into a **sealed segment**, closing
//! it is *still* just an appended [`Close`] in the validity index — invariant 1
//! is honored trivially because no record, sealed or staged, is ever mutated. The
//! writer consults a [`SealedLookup`] only to *find* a live version that no longer
//! lives in the delta tier ([ADR-0023], realigning STL-127).

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros};

use crate::backend::{Disk, DiskFile};
use crate::delta::{BusinessKey, Delta, Snapshot, Version};
use crate::merge;
use crate::segment::{ColumnId, Predicate, SegmentError, SegmentReader, ZoneBound};
use crate::validity::{Close, ValidityError, ValidityIndex};

/// One entry of a resolved redo set — the unit the WAL logs and a forward write
/// or a crash replay applies ([`crate::dml`]).
///
/// An `INSERT` resolves to a single [`Redo::Insert`]; an `UPDATE` to a
/// [`Redo::Close`] (the prior period's materialized end) followed by a
/// [`Redo::Insert`] (the new open version); a `DELETE` to a single
/// [`Redo::Retract`]. The shared `apply` step dispatches each: an insert stages
/// into the delta tier, a close materializes into the validity index, and a
/// retraction does both — materializes into the index *and* stages the tombstone
/// for the next flush.
///
/// ## Why a delete is a `Retract`, not a `Close`
///
/// A [`Redo::Close`] is a **supersession**: the prior period's end abuts the new
/// open version that the same commit appends. That adjacency — a version's
/// `sys_to` equals the next version's `sys_from` — means a from-scratch rebuild
/// from segments can *re-derive* the close from the version chain alone, so a
/// supersession close needs no durable record of its own.
///
/// A [`Redo::Retract`] is a **delete**: a close with **no successor version**.
/// Adjacency cannot represent it — a later re-insert would be mis-read as the
/// successor, silently resurrecting the row across the deletion gap
/// ([docs/16 §12](../../../docs/16-bitemporal-semantics.md#12-deletes-retractions--the-deletion-gap),
/// [ADR-0023]). So a retraction is a **first-class durable record**: it carries
/// the same [`Close`] payload, but the write path tags it distinctly so it is
/// persisted into segments at flush ([`crate::segment`]) and can never be lost to
/// a validity-index rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Redo {
    /// Stage a new open version into the delta tier.
    Insert(Version),
    /// Materialize a prior version's end into the validity index — a
    /// supersession close, re-derivable from version adjacency on rebuild.
    Close(Close),
    /// A logical delete: a close with no successor. Materializes into the
    /// validity index **and** stages a durable tombstone for the next flush, so
    /// the deletion gap survives a from-scratch index rebuild ([ADR-0023]).
    Retract(Close),
}

/// A read-only lookup over the sealed segments the writer must consult to
/// resolve a key's live version that has already been flushed out of the delta
/// tier ([ADR-0023], realigning STL-127).
///
/// The writer always checks the delta tier first; this trait covers the
/// "already sealed" case. It returns the key's sealed versions **raw** — every
/// one open/unresolved, before any validity-index overlay — because the writer
/// owns the index and overlays it itself ([`crate::merge::resolve_open`]).
pub trait SealedLookup {
    /// Every version of `key` stored across the sealed segments, in any order.
    ///
    /// # Errors
    ///
    /// Surfaces a [`SegmentError`] if a backing segment cannot be read.
    fn versions_for(&self, key: &BusinessKey) -> Result<Vec<Version>, SegmentError>;
}

/// A [`SealedLookup`] for a table with no sealed segments — every lookup is
/// empty. Used by callers that have not wired a segment set through yet (the
/// valid-time writer, the simulation harness).
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptySealed;

impl SealedLookup for EmptySealed {
    fn versions_for(&self, _key: &BusinessKey) -> Result<Vec<Version>, SegmentError> {
        Ok(Vec::new())
    }
}

/// A [`SealedLookup`] backed by versions already read out of one or more sealed
/// segments (e.g. via [`SegmentReader::read_versions`](crate::segment::SegmentReader::read_versions)).
///
/// v0.1 keeps the whole set resident and filters per key on each lookup; the
/// query executor will later resolve a key via the zone-map / secondary index
/// rather than a full scan ([architecture §3.3](../../../docs/02-architecture.md#33-how-b-tree-and-columnstore-coexist)).
#[derive(Debug, Default, Clone)]
pub struct SealedVersions {
    versions: Vec<Version>,
}

impl SealedVersions {
    /// Build a lookup over `versions` — typically the concatenation of every
    /// relevant segment's [`read_versions`](crate::segment::SegmentReader::read_versions).
    #[must_use]
    pub const fn new(versions: Vec<Version>) -> Self {
        Self { versions }
    }

    /// The underlying sealed versions — so a holder (e.g. the
    /// [`Engine`](crate::engine::Engine)) can both pass `&self` as a
    /// [`SealedLookup`] to the write path *and* read the rows for its merge path,
    /// without keeping a second copy.
    #[must_use]
    pub fn versions(&self) -> &[Version] {
        &self.versions
    }

    /// Append `versions` to the resident set in place — the rows a flush just
    /// sealed into a new segment ([`Engine::flush`](crate::engine::Engine::flush)).
    /// Appends without cloning the existing prefix, so a flush stays `O(flushed
    /// batch)` rather than `O(total sealed history)`.
    pub fn extend(&mut self, versions: impl IntoIterator<Item = Version>) {
        self.versions.extend(versions);
    }
}

impl SealedLookup for SealedVersions {
    fn versions_for(&self, key: &BusinessKey) -> Result<Vec<Version>, SegmentError> {
        Ok(self
            .versions
            .iter()
            .filter(|v| &v.business_key == key)
            .cloned()
            .collect())
    }
}

/// A [`SealedLookup`] over a borrowed set of open [`SegmentReader`]s, pruned per
/// key by each segment's resident zone map.
///
/// This is the lookup the DML / valid-time write path hands its
/// [`ValidTimeWriter`](crate::validtime::ValidTimeWriter) so a supersession can
/// close a live version that lives only in a sealed segment, **before** reading a
/// single column chunk of a segment the key cannot be in ([STL-140]).
///
/// Unlike [`SealedVersions`], which keeps every segment's rows resident and
/// filters the whole set on each lookup, this consults only the segments a
/// business-key zone-map prune cannot rule out: a `[min, max]` business-key
/// range that does not bracket `key` proves the key is absent, so that segment
/// is skipped without I/O ([`ZoneMap::might_contain`](crate::segment::ZoneMap::might_contain),
/// [architecture §3.3](../../../docs/02-architecture.md#33-how-b-tree-and-columnstore-coexist)).
/// A kept segment is materialized with [`SegmentReader::read_versions`] and
/// filtered to `key`; v0.1 has no per-key secondary index yet, so within a kept
/// segment the scan is still whole-segment — the per-key index is the follow-up
/// the [module docs](self) anticipate.
///
/// The readers are borrowed, not owned: the caller (the executor / DML write
/// path) holds the table's segment set and rebuilds the lookup per operation, so
/// it always reflects the segments live *at that commit* — a flush between two
/// writes is picked up on the next lookup.
pub struct SealedSegments<'a, F: DiskFile> {
    readers: &'a [SegmentReader<F>],
}

// `SegmentReader` is not `Debug` (it guards a `DiskFile` handle), so derive is
// out; surface the segment count and elide the readers.
impl<F: DiskFile> std::fmt::Debug for SealedSegments<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedSegments")
            .field("segments", &self.readers.len())
            .finish_non_exhaustive()
    }
}

impl<'a, F: DiskFile> SealedSegments<'a, F> {
    /// Build a lookup over `readers` — the open segments of one table.
    #[must_use]
    pub const fn new(readers: &'a [SegmentReader<F>]) -> Self {
        Self { readers }
    }
}

impl<F: DiskFile> SealedLookup for SealedSegments<'_, F> {
    fn versions_for(&self, key: &BusinessKey) -> Result<Vec<Version>, SegmentError> {
        let predicate = Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: ZoneBound::Bytes(key.as_bytes().to_vec()),
        };
        // Resolve liveness at the open sentinel so the system-time slice never
        // prunes: the writer probes at a freshly-allocated commit `≥` every
        // persisted `sys_from`, so any segment holding the key must be kept
        // regardless of how old its rows are. Only the business-key zone map gets
        // to rule a segment out here.
        let snapshot = Snapshot(SYSTEM_TIME_OPEN);
        let mut out = Vec::new();
        for reader in self.readers {
            if !reader.might_contain(&predicate, snapshot) {
                continue;
            }
            out.extend(
                reader
                    .read_versions()?
                    .into_iter()
                    .filter(|v| &v.business_key == key),
            );
        }
        Ok(out)
    }
}

/// Errors surfaced from the system-time write path.
#[derive(Debug, thiserror::Error)]
pub enum SysTimeError {
    /// [`SysTimeWriter::insert`] on a key that already has a live (open)
    /// version. Re-opening it would create two overlapping open intervals;
    /// the caller wanted an `update`.
    #[error("business key already has a live version")]
    KeyExists,

    /// [`SysTimeWriter::update`] / [`SysTimeWriter::delete`] on a key with no
    /// live version — nothing to close.
    #[error("business key has no live version")]
    KeyNotFound,

    /// The system-time domain is exhausted: the next commit timestamp would
    /// reach the `+∞` open sentinel ([`SYSTEM_TIME_OPEN`]). A `sys_from` at the
    /// sentinel would be indistinguishable from an open period and break
    /// snapshot resolution, so the write is refused instead. Practically
    /// unreachable — it needs a clock reading at `i64::MAX` or ~9.2e18 commits —
    /// but enforced in **all** builds, not just debug.
    #[error("system-time domain exhausted: next commit would reach the +∞ sentinel")]
    TimeExhausted,

    /// The commit clock read **earlier** than a `sys_from` this writer already
    /// stamped. Since [ADR-0024] (STL-145) the writer keeps the real µs and orders
    /// same-tick commits by `seq` rather than force-bumping the timestamp, so a
    /// repeated tick is fine — but a clock that moves *backwards* would let a new
    /// version sort before an existing one and break the per-key timeline, so it
    /// is refused. Re-establishing the high-water mark on recovery and global
    /// ordering across transactions remain the transaction manager's job
    /// ([architecture §9]); this guard keeps the single-writer path correct.
    #[error("commit clock regressed below the last stamped sys_from")]
    ClockRegressed,

    /// An error bubbled up from the delta tier (I/O on a spill, or a frame too
    /// large to encode).
    #[error(transparent)]
    Delta(#[from] crate::delta::DeltaError),

    /// An error bubbled up from the validity index — a write-once conflict
    /// (a concurrent supersession of the same version), or a spill-read failure
    /// ([ADR-0023]).
    #[error(transparent)]
    Validity(#[from] ValidityError),

    /// An error bubbled up while consulting the sealed segments through a
    /// [`SealedLookup`] — e.g. a backing segment that failed its checksum on
    /// read.
    #[error(transparent)]
    Sealed(#[from] SegmentError),
}

/// Stamps commit timestamps and resolves the per-key `[sys_from, sys_to)` chain
/// as writes flow into the delta tier and the validity index.
///
/// One writer owns the monotonic commit-timestamp counter for the rows it
/// stamps; see the [module docs](self) for why that monotonicity is what makes
/// the chain non-overlapping and gap-free.
#[derive(Debug)]
pub struct SysTimeWriter<C: Clock> {
    clock: C,
    last_commit: Option<SystemTimeMicros>,
}

impl<C: Clock> SysTimeWriter<C> {
    /// Create a writer that draws commit timestamps from `clock`.
    pub const fn new(clock: C) -> Self {
        Self {
            clock,
            last_commit: None,
        }
    }

    /// The most recent commit timestamp this writer issued, if any.
    #[must_use]
    pub const fn last_commit(&self) -> Option<SystemTimeMicros> {
        self.last_commit
    }

    /// Open the first version of `key`: stage `[commit, +∞)`.
    ///
    /// Returns the stamped `sys_from`.
    ///
    /// The liveness check spans both tiers and the index: a key whose live
    /// version has been flushed into a sealed segment is still live, so
    /// re-opening it is rejected just as it would be for a version still in the
    /// delta. Pass [`EmptySealed`] when the table has no sealed segments.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyExists`] if `key` already has a live version — use
    /// [`Self::update`] to supersede it. Delta/index/segment errors propagate as
    /// the matching [`SysTimeError`] variants.
    #[allow(clippy::too_many_arguments)] // tier handles + key/payload + provenance triple
    pub fn insert<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        if resolve_live(delta, sealed, index, &key, commit)?.is_some() {
            return Err(SysTimeError::KeyExists);
        }
        apply(
            delta,
            index,
            vec![Redo::Insert(open_version(
                key, commit, seq, payload, txn_id, principal,
            ))],
        )?;
        Ok(commit)
    }

    /// Supersede the live version of `key`: materialize the prior period's end
    /// into the index at `commit` and open a new one `[commit, +∞)`. The two
    /// intervals abut, so the chain stays gap-free.
    ///
    /// The prior period is closed by an appended [`Close`] in the validity index
    /// — never a record mutation — wherever its body lives ([ADR-0023]). The new
    /// open version lands in the delta tier.
    ///
    /// Returns the stamped `sys_from` of the new version.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version; otherwise the
    /// delta/index/segment errors.
    #[allow(clippy::too_many_arguments)] // tier handles + key/payload + provenance triple
    pub fn update<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior =
            resolve_live(delta, sealed, index, &key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        // The superseding transaction both closes the prior period (stamping its
        // identity as `closed_by`) and opens the new one — same `txn_id` /
        // `principal` for both halves. The new open version carries the caller's
        // `seq`; the close carries the *prior* version's `seq` as part of its
        // (sys_from, seq) match key (STL-145).
        apply(
            delta,
            index,
            vec![
                Redo::Close(close_of(&key, &prior, commit, txn_id, principal.clone())),
                Redo::Insert(open_version(key, commit, seq, payload, txn_id, principal)),
            ],
        )?;
        Ok(commit)
    }

    /// Close the live version of `key` without re-opening — a logical delete.
    /// Afterwards the key has no version live at any snapshot `≥ commit`.
    ///
    /// The deleting transaction's `txn_id` + `principal` are recorded as the
    /// [`Close`]'s `closed_by` provenance — a delete is a logical period-close
    /// that "carries its own provenance" ([architecture §3.1], [STL-118]). Unlike
    /// an [`update`](Self::update), a delete leaves no successor version, so this
    /// is the only record of who performed it.
    ///
    /// Returns the `commit` at which the period was closed.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version; otherwise the
    /// delta/index/segment errors.
    pub fn delete<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior =
            resolve_live(delta, sealed, index, key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        apply(
            delta,
            index,
            vec![Redo::Retract(close_of(
                key, &prior, commit, txn_id, principal,
            ))],
        )?;
        Ok(commit)
    }

    /// Resolve an insert into the redo set it stages — **without** touching the
    /// delta tier or the index. Returns the stamped commit timestamp and the
    /// redo set (a single open version `[commit, +∞)`).
    ///
    /// This is the resolution half of the write path: it stamps the commit
    /// timestamp and builds the redo, but leaves *applying* it to the caller.
    /// [`Self::insert`] applies straight to the delta + index; the DML write path
    /// ([`crate::dml`]) logs the redo to the WAL first, then applies the same set
    /// — so a forward write and a crash-recovery replay run identical operations.
    ///
    /// Liveness here spans the **delta tier, the sealed segments, and the
    /// index** — a key whose live version has been flushed into a sealed segment
    /// still has one, so re-opening it is rejected just as for a delta-resident
    /// version. Pass [`EmptySealed`] when the table has no sealed segments.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyExists`] if `key` already has a live version (the
    /// commit timestamp is still consumed, matching [`Self::insert`]);
    /// [`SysTimeError::TimeExhausted`] from the allocator; a [`SealedLookup`]
    /// read error.
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/payload + seq + provenance triple
    pub fn stage_insert<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        if resolve_live(delta, sealed, index, &key, commit)?.is_some() {
            return Err(SysTimeError::KeyExists);
        }
        Ok((
            commit,
            vec![Redo::Insert(open_version(
                key, commit, seq, payload, txn_id, principal,
            ))],
        ))
    }

    /// Resolve an update into the redo set it stages — the prior version's close
    /// plus the new open version `[commit, +∞)` — without touching the delta tier
    /// or index. See [`Self::stage_insert`].
    ///
    /// Liveness spans the delta tier, the sealed segments, and the index (see
    /// [`Self::stage_insert`]); pass [`EmptySealed`] when there are no sealed
    /// segments.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version;
    /// [`SysTimeError::TimeExhausted`]; delta/index/segment read errors.
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/payload + seq + provenance triple
    pub fn stage_update<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior =
            resolve_live(delta, sealed, index, &key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        let close = close_of(&key, &prior, commit, txn_id, principal.clone());
        let opened = open_version(key, commit, seq, payload, txn_id, principal);
        Ok((commit, vec![Redo::Close(close), Redo::Insert(opened)]))
    }

    /// Resolve a delete into the redo set it stages — the prior version's close,
    /// with no successor — without touching the delta tier or index. See
    /// [`Self::stage_insert`] for why resolution is split from application, and
    /// [`Self::delete`] for the tombstone-provenance semantics.
    ///
    /// Liveness spans the delta tier, the sealed segments, and the index (see
    /// [`Self::stage_insert`]); pass [`EmptySealed`] when there are no sealed
    /// segments.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version;
    /// [`SysTimeError::TimeExhausted`]; delta/index/segment read errors.
    pub fn stage_delete<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        sealed: &S,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior =
            resolve_live(delta, sealed, index, key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        Ok((
            commit,
            vec![Redo::Retract(close_of(
                key, &prior, commit, txn_id, principal,
            ))],
        ))
    }

    /// Allocate the next commit timestamp: the clock's **real** reading, refused
    /// only if it regressed below the last one or reached the `+∞` sentinel. See
    /// the [module docs](self).
    ///
    /// The timestamp is **never mutated** ([ADR-0024], STL-145): a clock that
    /// reads the same µs as the previous commit is accepted as-is, and `seq` (the
    /// caller's per-commit counter) gives the two same-tick versions their total
    /// order. The monotonicity guard survives only as a *non-regression check*.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::ClockRegressed`] if the reading is strictly less than the
    /// last stamped `sys_from`. [`SysTimeError::TimeExhausted`] if it would reach
    /// the `+∞` open sentinel — refused in all builds so a real `sys_from` can
    /// never masquerade as an open period. `last_commit` is left untouched on
    /// either error, so a retry behaves identically.
    fn next_commit_ts(&mut self) -> Result<SystemTimeMicros, SysTimeError> {
        let now = self.clock.now();
        if matches!(self.last_commit, Some(prev) if now < prev) {
            return Err(SysTimeError::ClockRegressed);
        }
        if now >= SYSTEM_TIME_OPEN {
            return Err(SysTimeError::TimeExhausted);
        }
        self.last_commit = Some(now);
        Ok(now)
    }
}

/// Apply a resolved redo set: stage each [`Redo::Insert`] into the delta tier and
/// materialize each [`Redo::Close`] into the validity index, in order.
///
/// The single application point shared by every write path. A forward write
/// (via [`SysTimeWriter::insert`] and friends), the WAL-logging DML writer
/// ([`crate::dml::DmlWriter`]), and a crash-recovery replay ([`crate::dml::replay`])
/// all funnel their resolved redo sets through here, so "the same code path under
/// sim and under real I/O" is structural, not a promise. Re-inserting the same
/// `(business_key, sys_from)` is the delta tier's idempotent replace, an identical
/// re-close is the index's idempotent write-once, and re-staging the same
/// retraction is the delta tier's idempotent tombstone — so replaying an
/// already-applied record is a no-op.
///
/// A [`Redo::Retract`] applies to **both** structures: it materializes the
/// deleted period's end into the validity index (so reads resolve the gap
/// immediately, exactly as a supersession close would) **and** stages the
/// tombstone into the delta tier so the next flush persists it durably into a
/// segment ([ADR-0023]). The index close comes first: if it rejects a conflicting
/// re-close the delta is left untouched, so the operation neither half-applies nor
/// leaves a tombstone for a close that never took.
///
/// # Errors
///
/// [`SysTimeError::Delta`] from a delta apply; [`SysTimeError::Validity`] from an
/// index close (e.g. a write-once conflict).
pub(crate) fn apply<D: Disk, I: Disk>(
    delta: &mut Delta<D>,
    index: &mut ValidityIndex<I>,
    redos: Vec<Redo>,
) -> Result<(), SysTimeError> {
    for redo in redos {
        match redo {
            Redo::Insert(version) => delta.insert(version)?,
            Redo::Close(close) => index.insert_close(close)?,
            Redo::Retract(close) => {
                index.insert_close(close.clone())?;
                delta.stage_retraction(close);
            }
        }
    }
    Ok(())
}

/// Apply a resolved redo set **resident** — identical to [`apply`] but the
/// versions and closes never spill to disk ([STL-216]).
///
/// This is the group-commit apply step ([`crate::dml::DmlWriter`]): a
/// multi-statement transaction's writes apply to the delta/index as they go (so a
/// later write resolves against an earlier one), but they stay in memory so the
/// whole set can be rolled back with [`undo`] if the commit aborts before its WAL
/// record is durable — a spilled row/close is on disk and not removable in place.
/// The deferred spill is harmless: resident bytes are a soft bound and the next
/// auto-commit [`apply`] reconciles the over-threshold tier.
///
/// # Errors
///
/// As [`apply`]: [`SysTimeError::Delta`] from a delta apply; [`SysTimeError::Validity`]
/// from an index close (e.g. a write-once conflict).
pub(crate) fn apply_resident<D: Disk, I: Disk>(
    delta: &mut Delta<D>,
    index: &mut ValidityIndex<I>,
    redos: Vec<Redo>,
) -> Result<(), SysTimeError> {
    for redo in redos {
        match redo {
            Redo::Insert(version) => delta.insert_resident(version)?,
            Redo::Close(close) => index.insert_close_resident(close)?,
            Redo::Retract(close) => {
                index.insert_close_resident(close.clone())?;
                delta.stage_retraction(close);
            }
        }
    }
    Ok(())
}

/// Undo a redo set previously staged with [`apply_resident`] — the in-memory
/// inverse of [`apply`] ([STL-216]).
///
/// Reverses each entry by its `(business_key, sys_from, seq)`: an [`Redo::Insert`]
/// removes the staged version, a [`Redo::Close`] removes the materialized close
/// (re-opening the version it ended), and a [`Redo::Retract`] removes both its
/// tombstone and its close. The set is the group-commit buffer ([`crate::dml`]) —
/// exactly what this transaction applied — so removing each entry by key restores
/// the tiers to their pre-transaction state, identical to what a crash (which
/// finds no durable record for the aborted transaction) reconstructs on recovery.
///
/// Infallible: every removal is an in-memory map operation, and a redo whose entry
/// is already absent (it was superseded within the same transaction, or never
/// landed) is simply skipped. Apply order does not matter — each entry is keyed
/// independently — but the set is walked in reverse for symmetry with [`apply`].
pub(crate) fn undo<D: Disk, I: Disk>(
    delta: &mut Delta<D>,
    index: &mut ValidityIndex<I>,
    redos: Vec<Redo>,
) {
    for redo in redos.into_iter().rev() {
        match redo {
            Redo::Insert(version) => {
                delta.remove_version(&version.business_key, version.sys_from, version.seq);
            }
            Redo::Close(close) => {
                index.remove_close(&close.business_key, close.sys_from, close.seq);
            }
            Redo::Retract(close) => {
                delta.remove_retraction(&close.business_key, close.sys_from, close.seq);
                index.remove_close(&close.business_key, close.sys_from, close.seq);
            }
        }
    }
}

/// Build the [`Close`] that materializes `prior`'s end at `commit`, stamping the
/// closing transaction's provenance.
///
/// The closed version's **birth provenance is preserved untouched** — closing a
/// period is bookkeeping by the superseding/deleting transaction, not a rewrite
/// of who wrote the closed version. The `Close` adds `closed_by`: the `txn_id` /
/// `principal` of the transaction performing the close, with `committed_at`
/// stamped to `commit` (which equals the new `sys_to`). For a `delete` there is
/// no successor version to carry that identity, so the `Close` is the only place
/// "who closed this period" survives ([architecture §3.1], [STL-118]).
fn close_of(
    key: &BusinessKey,
    prior: &Version,
    commit: SystemTimeMicros,
    txn_id: TxnId,
    principal: Principal,
) -> Close {
    Close {
        business_key: key.clone(),
        sys_from: prior.sys_from,
        // The closed version's own `seq` completes the (sys_from, seq) match key,
        // so a same-tick supersession closes the right sibling (STL-145).
        seq: prior.seq,
        sys_to: commit,
        closed_by: Provenance::new(txn_id, commit, principal),
    }
}

/// Build an open version `[commit, +∞)` for `key`, stamping provenance.
///
/// `committed_at` is set to `commit` — the writer stamps it exactly as it
/// stamps `sys_from`. `seq`, `txn_id`, and `principal` come from the caller (the
/// transaction manager), per [architecture §8](../../../docs/02-architecture.md#8-lineage--provenance-subsystem):
/// `seq` is the per-commit total-order tiebreak
/// ([ADR-0024](../../../docs/adr/0024-time-representation.md), STL-141) drawn
/// from the manager's `Committed` value alongside `txn_id`.
const fn open_version(
    key: BusinessKey,
    commit: SystemTimeMicros,
    seq: u64,
    payload: Option<Vec<u8>>,
    txn_id: TxnId,
    principal: Principal,
) -> Version {
    Version::open(
        key,
        commit,
        seq,
        Provenance::new(txn_id, commit, principal),
        payload,
    )
}

/// The version of `key` live at `at`, resolved across the **delta tier, the
/// sealed segments, and the validity index** ([`crate::merge::resolve_open`]).
///
/// `at` is the freshly-allocated commit timestamp, `≥` every `sys_from` already
/// on the key's chain (it may now *equal* the newest one at a shared tick, since
/// the writer no longer force-bumps, STL-145). The resolver scans up to
/// `(at, u64::MAX)`, so the open version with the greatest `(sys_from, seq)` is
/// the one returned — scanning at [`SYSTEM_TIME_OPEN`] would instead exclude it,
/// since the resolver's `sys_to > at` test fails at that exact point.
fn resolve_live<D: Disk, I: Disk, S: SealedLookup>(
    delta: &Delta<D>,
    sealed: &S,
    index: &ValidityIndex<I>,
    key: &BusinessKey,
    at: SystemTimeMicros,
) -> Result<Option<Version>, SysTimeError> {
    let delta_versions = delta.candidate_versions(key)?;
    let sealed_versions = sealed.versions_for(key)?;
    Ok(merge::resolve_open(
        &delta_versions,
        &sealed_versions,
        index,
        key,
        Snapshot(at),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    /// A clock whose reading the test sets explicitly — including stalled and
    /// regressing sequences, to prove the monotonic guard holds the chain
    /// invariant regardless of what the wall clock does. Backed by an atomic so
    /// it satisfies the `Clock: Send + Sync` bound without `unsafe`.
    struct StubClock(AtomicI64);
    impl StubClock {
        const fn new(start: i64) -> Self {
            Self(AtomicI64::new(start))
        }
        fn set(&self, micros: i64) {
            self.0.store(micros, Ordering::Relaxed);
        }
    }
    impl Clock for StubClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(self.0.load(Ordering::Relaxed))
        }
    }

    #[test]
    fn a_stalled_clock_repeats_the_tick_instead_of_bumping_it() {
        // The force-bump is gone (STL-145): a stalled clock yields the *same* µs,
        // not `previous + 1`. Same-tick commits are ordered by `seq`, not by a
        // manufactured timestamp, so the writer hands back exactly what the clock
        // reads on each call.
        let clock = StubClock::new(100);
        let mut writer = SysTimeWriter::new(clock);

        let a = writer.next_commit_ts().unwrap();
        let b = writer.next_commit_ts().unwrap(); // clock still at 100
        writer.clock.set(10_000);
        let c = writer.next_commit_ts().unwrap();

        assert_eq!(a, SystemTimeMicros(100));
        assert_eq!(
            b,
            SystemTimeMicros(100),
            "a repeated tick is kept, not bumped"
        );
        assert_eq!(c, SystemTimeMicros(10_000));
        assert_eq!(writer.last_commit(), Some(SystemTimeMicros(10_000)));
    }

    #[test]
    fn a_regressing_clock_is_refused() {
        // A clock that moves *backwards* below the high-water mark would let a new
        // version sort before an existing one; the non-regression guard rejects
        // it rather than mutating the timestamp. `last_commit` is untouched on
        // error, so a retry at a non-regressing reading behaves identically.
        let clock = StubClock::new(100);
        let mut writer = SysTimeWriter::new(clock);
        assert_eq!(writer.next_commit_ts().unwrap(), SystemTimeMicros(100));
        writer.clock.set(99);
        assert!(matches!(
            writer.next_commit_ts(),
            Err(SysTimeError::ClockRegressed)
        ));
        assert_eq!(
            writer.last_commit(),
            Some(SystemTimeMicros(100)),
            "the high-water mark is left intact on a rejected regression",
        );
        // Recovering to the mark (a repeated tick) is allowed again.
        writer.clock.set(100);
        assert_eq!(writer.next_commit_ts().unwrap(), SystemTimeMicros(100));
    }

    #[test]
    fn commit_at_the_open_sentinel_is_refused_in_all_builds() {
        let clock = StubClock::new(SYSTEM_TIME_OPEN.0);
        let mut writer = SysTimeWriter::new(clock);
        assert!(matches!(
            writer.next_commit_ts(),
            Err(SysTimeError::TimeExhausted)
        ));
        assert_eq!(writer.last_commit(), None);
    }

    #[test]
    fn commit_one_below_the_sentinel_is_allowed_then_the_sentinel_is_refused() {
        // A real reading one µs below the sentinel is a legal `sys_from`. Without
        // the force-bump a repeated reading there is *still* allowed (same tick,
        // ordered by seq) — only a clock that actually advances to the sentinel is
        // refused, since a `sys_from` at `+∞` would masquerade as an open period.
        let clock = StubClock::new(SYSTEM_TIME_OPEN.0 - 1);
        let mut writer = SysTimeWriter::new(clock);
        assert_eq!(
            writer.next_commit_ts().unwrap(),
            SystemTimeMicros(SYSTEM_TIME_OPEN.0 - 1)
        );
        assert_eq!(
            writer.next_commit_ts().unwrap(),
            SystemTimeMicros(SYSTEM_TIME_OPEN.0 - 1),
            "a repeated sub-sentinel tick is kept, not bumped into the sentinel",
        );
        writer.clock.set(SYSTEM_TIME_OPEN.0);
        assert!(matches!(
            writer.next_commit_ts(),
            Err(SysTimeError::TimeExhausted)
        ));
    }
}
