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
//! * **Updates close the prior period.** An [`SysTimeWriter::update`] stages a
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
//! The per-key chain is non-overlapping and gap-free **iff** the commit
//! timestamps it is stamped with strictly increase. [`SysTimeWriter`] guarantees
//! that locally: each commit timestamp is `max(clock.now(), previous + 1)`, so a
//! stalled or regressing wall clock can never produce two versions with the same
//! `sys_from` or an out-of-order close.
//!
//! **Scope of the guard.** The `previous + 1` high-water mark lives *in the
//! writer instance* — it starts empty on [`SysTimeWriter::new`] and resets if the
//! writer is recreated (e.g. after a restart). So the monotonicity guarantee
//! holds *within one writer's lifetime*, not across restarts: a caller that
//! constructs a fresh writer must supply a commit clock that does not read
//! earlier than the newest `sys_from` already persisted. Re-establishing that
//! high-water mark on recovery, and global commit ordering across transactions
//! and (later) nodes, is the transaction manager's job
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

use crate::backend::Disk;
use crate::delta::{BusinessKey, Delta, Snapshot, Version};
use crate::merge;
use crate::segment::SegmentError;
use crate::validity::{Close, ValidityError, ValidityIndex};

/// One entry of a resolved redo set — the unit the WAL logs and a forward write
/// or a crash replay applies ([`crate::dml`]).
///
/// An `INSERT` resolves to a single [`Redo::Insert`]; an `UPDATE` to a
/// [`Redo::Close`] (the prior period's materialized end) followed by a
/// [`Redo::Insert`] (the new open version); a `DELETE` to a single
/// [`Redo::Close`]. The shared `apply` step dispatches each: an insert stages into the delta
/// tier, a close materializes into the validity index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Redo {
    /// Stage a new open version into the delta tier.
    Insert(Version),
    /// Materialize a prior version's end into the validity index.
    Close(Close),
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
        payload: Vec<u8>,
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
                key, commit, payload, txn_id, principal,
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
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior =
            resolve_live(delta, sealed, index, &key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        // The superseding transaction both closes the prior period (stamping its
        // identity as `closed_by`) and opens the new one — same `txn_id` /
        // `principal` for both halves.
        apply(
            delta,
            index,
            vec![
                Redo::Close(close_of(&key, &prior, commit, txn_id, principal.clone())),
                Redo::Insert(open_version(key, commit, payload, txn_id, principal)),
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
            vec![Redo::Close(close_of(
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
    /// Liveness here is resolved against the **delta tier and the index** only
    /// (no sealed lookup), matching the delta-resident DML path.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyExists`] if `key` already has a live version (the
    /// commit timestamp is still consumed, matching [`Self::insert`]);
    /// [`SysTimeError::TimeExhausted`] from the allocator.
    pub fn stage_insert<D: Disk, I: Disk>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        key: BusinessKey,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        if current_open(delta, index, &key, commit)?.is_some() {
            return Err(SysTimeError::KeyExists);
        }
        Ok((
            commit,
            vec![Redo::Insert(open_version(
                key, commit, payload, txn_id, principal,
            ))],
        ))
    }

    /// Resolve an update into the redo set it stages — the prior version's close
    /// plus the new open version `[commit, +∞)` — without touching the delta tier
    /// or index. See [`Self::stage_insert`].
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version;
    /// [`SysTimeError::TimeExhausted`]; delta/index read errors.
    pub fn stage_update<D: Disk, I: Disk>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        key: BusinessKey,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior = current_open(delta, index, &key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        let close = close_of(&key, &prior, commit, txn_id, principal.clone());
        let opened = open_version(key, commit, payload, txn_id, principal);
        Ok((commit, vec![Redo::Close(close), Redo::Insert(opened)]))
    }

    /// Resolve a delete into the redo set it stages — the prior version's close,
    /// with no successor — without touching the delta tier or index. See
    /// [`Self::stage_insert`] for why resolution is split from application, and
    /// [`Self::delete`] for the tombstone-provenance semantics.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version;
    /// [`SysTimeError::TimeExhausted`]; delta/index read errors.
    pub fn stage_delete<D: Disk, I: Disk>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        let prior = current_open(delta, index, key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
        Ok((
            commit,
            vec![Redo::Close(close_of(
                key, &prior, commit, txn_id, principal,
            ))],
        ))
    }

    /// Allocate the next commit timestamp: at least the clock's reading, and
    /// strictly greater than the previous one. See the [module docs](self).
    ///
    /// # Errors
    ///
    /// [`SysTimeError::TimeExhausted`] if the next timestamp would reach the
    /// `+∞` open sentinel — refused in all builds so a real `sys_from` can never
    /// masquerade as an open period. `last_commit` is left untouched on error,
    /// so a retry behaves identically.
    fn next_commit_ts(&mut self) -> Result<SystemTimeMicros, SysTimeError> {
        let now = self.clock.now();
        let ts = match self.last_commit {
            Some(prev) if now <= prev => SystemTimeMicros(prev.0.saturating_add(1)),
            _ => now,
        };
        if ts >= SYSTEM_TIME_OPEN {
            return Err(SysTimeError::TimeExhausted);
        }
        self.last_commit = Some(ts);
        Ok(ts)
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
/// `(business_key, sys_from)` is the delta tier's idempotent replace, and an
/// identical re-close is the index's idempotent write-once, so replaying an
/// already-applied record is a no-op.
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
        }
    }
    Ok(())
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
        sys_to: commit,
        closed_by: Provenance::new(txn_id, commit, principal),
    }
}

/// Build an open version `[commit, +∞)` for `key`, stamping provenance.
///
/// `committed_at` is set to `commit` — the writer stamps it exactly as it
/// stamps `sys_from`. `txn_id` and `principal` come from the caller (the
/// transaction manager), per [architecture §8](../../../docs/02-architecture.md#8-lineage--provenance-subsystem).
const fn open_version(
    key: BusinessKey,
    commit: SystemTimeMicros,
    payload: Vec<u8>,
    txn_id: TxnId,
    principal: Principal,
) -> Version {
    Version::open(
        key,
        commit,
        Provenance::new(txn_id, commit, principal),
        payload,
    )
}

/// The version of `key` live at `at`, resolved across the **delta tier, the
/// sealed segments, and the validity index** ([`crate::merge::resolve_open`]).
///
/// `at` is the freshly-allocated commit timestamp, strictly greater than every
/// `sys_from` already on the key's chain, so the open version (if one exists) is
/// always the one returned — scanning at [`SYSTEM_TIME_OPEN`] would instead
/// exclude it, since the resolver's `sys_to > at` test fails at that exact point.
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

/// The version of `key` live at `at`, resolved across the **delta tier and the
/// validity index** only (no sealed segments) — the resolver the staging path
/// uses, matching the delta-resident DML write path.
fn current_open<D: Disk, I: Disk>(
    delta: &Delta<D>,
    index: &ValidityIndex<I>,
    key: &BusinessKey,
    at: SystemTimeMicros,
) -> Result<Option<Version>, SysTimeError> {
    let delta_versions = delta.candidate_versions(key)?;
    Ok(merge::resolve_open(
        &delta_versions,
        &[],
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
    fn commit_timestamps_strictly_increase_despite_a_stalled_clock() {
        let clock = StubClock::new(100);
        let mut writer = SysTimeWriter::new(clock);

        let a = writer.next_commit_ts().unwrap();
        let b = writer.next_commit_ts().unwrap();
        writer.clock.set(50);
        let c = writer.next_commit_ts().unwrap();
        writer.clock.set(10_000);
        let d = writer.next_commit_ts().unwrap();

        assert_eq!(a, SystemTimeMicros(100));
        assert_eq!(b, SystemTimeMicros(101));
        assert_eq!(c, SystemTimeMicros(102));
        assert_eq!(d, SystemTimeMicros(10_000));
        assert!(a < b && b < c && c < d);
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
    fn commit_one_below_the_sentinel_is_allowed_but_the_next_is_refused() {
        let clock = StubClock::new(SYSTEM_TIME_OPEN.0 - 1);
        let mut writer = SysTimeWriter::new(clock);
        assert_eq!(
            writer.next_commit_ts().unwrap(),
            SystemTimeMicros(SYSTEM_TIME_OPEN.0 - 1)
        );
        assert!(matches!(
            writer.next_commit_ts(),
            Err(SysTimeError::TimeExhausted)
        ));
    }
}
