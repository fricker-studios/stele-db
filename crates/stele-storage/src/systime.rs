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
//! * **Updates close the prior period.** An [`SysTimeWriter::update`] writes a
//!   new open version *and* a logical close on the previous version's `sys_to`
//!   (it abuts: the old period ends exactly where the new one begins). A
//!   [`SysTimeWriter::delete`] closes without re-opening — the "tombstone =
//!   logical period-close" of [architecture §3.1](../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving).
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
//! earlier than the newest `sys_from` already persisted — otherwise the first
//! commit of the new writer could stamp behind existing versions. Re-establishing
//! that high-water mark on recovery, and global commit ordering across
//! transactions and (later) nodes, is the transaction manager's job
//! ([architecture §9](../../../docs/02-architecture.md#9-transaction--concurrency-model),
//! [ADR-0022](../../../docs/adr/0022-clock-synchronization-and-ordering.md)); this
//! guard is what keeps the single-writer storage path correct on its own.
//!
//! ```ignore
//! let mut writer = SysTimeWriter::new(SystemClock);
//! // `EmptySealed`: this table has no sealed segments yet, so the writer only
//! // consults the delta tier. Hand it a `SealedVersions` once segments exist.
//! writer.insert(&mut delta, &EmptySealed, key.clone(), b"v0".to_vec())?;   // [c0, +∞)
//! writer.update(&mut delta, &EmptySealed, key.clone(), b"v1".to_vec())?;   // closes c0 at c1; [c1, +∞)
//! // delta now holds two versions for `key`: [c0, c1) and [c1, +∞).
//! ```
//!
//! ## Closing across a flush boundary
//!
//! Once a key's open version has been flushed into a **sealed segment**, the
//! close can no longer re-stage it — invariant 1 forbids mutating a sealed
//! segment. [`SysTimeWriter::update`] / [`SysTimeWriter::delete`] then append a
//! [`CloseMarker`] instead, naming the sealed
//! version and the new `sys_to`; the read path ([`crate::merge`]) folds the
//! marker onto the sealed version to surface the closed interval. The writer
//! tells the two cases apart through the [`SealedLookup`] it is handed ([STL-127]).

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros};

use crate::delta::{BusinessKey, CloseMarker, Delta, DeltaError, Snapshot, Version};
use crate::merge::{self, LiveLocation};
use crate::segment::SegmentError;
use crate::wal::Disk;

/// A read-only lookup over the sealed segments the writer must consult to
/// resolve a key's live version that has already been flushed out of the delta
/// tier ([STL-127]).
///
/// The writer always checks the delta tier first; this trait covers the
/// "already sealed" case. It returns the key's sealed versions **raw** — open
/// or closed, before any delta close-marker is folded in — because the writer
/// owns the markers and folds them itself ([`crate::merge::resolve_open`]).
pub trait SealedLookup {
    /// Every version of `key` stored across the sealed segments, in any order.
    ///
    /// # Errors
    ///
    /// Surfaces a [`SegmentError`] if a backing segment cannot be read.
    fn versions_for(&self, key: &BusinessKey) -> Result<Vec<Version>, SegmentError>;
}

/// A [`SealedLookup`] for a table with no sealed segments — every lookup is
/// empty.
///
/// The writer then behaves exactly as the delta-only path did before [STL-127].
/// Used by callers that have not wired a segment set through yet (the valid-time
/// writer, the simulation harness).
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

    /// An error bubbled up while consulting the sealed segments through a
    /// [`SealedLookup`] — e.g. a backing segment that failed its checksum on
    /// read ([STL-127]).
    #[error(transparent)]
    Sealed(#[from] SegmentError),
}

/// Stamps commit timestamps and maintains the per-key `[sys_from, sys_to)`
/// chain as writes flow into the delta tier.
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

    /// The most recent commit timestamp this writer issued, if any. Exposed for
    /// observability and tests; callers do not need it to drive writes.
    #[must_use]
    pub const fn last_commit(&self) -> Option<SystemTimeMicros> {
        self.last_commit
    }

    /// Open the first version of `key`: stage `[commit, +∞)`.
    ///
    /// Returns the stamped `sys_from`.
    ///
    /// The liveness check spans both tiers: a key whose live version has been
    /// flushed into a sealed segment is still live, so re-opening it is rejected
    /// just as it would be for a version still in the delta ([STL-127]). Pass
    /// [`EmptySealed`] when the table has no sealed segments.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyExists`] if `key` already has a live version — use
    /// [`Self::update`] to supersede it. Delta-tier errors propagate as
    /// [`SysTimeError::Delta`]; segment-read errors as [`SysTimeError::Sealed`].
    pub fn insert<D: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        sealed: &S,
        key: BusinessKey,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        if resolve_live(delta, sealed, &key, commit)?.is_some() {
            return Err(SysTimeError::KeyExists);
        }
        apply(
            delta,
            vec![open_version(key, commit, payload, txn_id, principal)],
        )?;
        Ok(commit)
    }

    /// Supersede the live version of `key`: close the prior period at `commit`
    /// and open a new one `[commit, +∞)`. The two intervals abut, so the chain
    /// stays gap-free.
    ///
    /// The prior period is closed where it lives: re-staged in place if still in
    /// the delta tier, or — if it has already been flushed into a sealed segment
    /// — closed by an appended [`CloseMarker`], since invariant 1 forbids
    /// mutating the segment ([STL-127]). Either way the new open version lands in
    /// the delta tier.
    ///
    /// Returns the stamped `sys_from` of the new version.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version. Delta-tier
    /// errors propagate as [`SysTimeError::Delta`]; segment-read errors as
    /// [`SysTimeError::Sealed`].
    pub fn update<D: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        sealed: &S,
        key: BusinessKey,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        // The superseding transaction both closes the prior period (stamping its
        // identity as `closed_by`) and opens the new one — same `txn_id` /
        // `principal` for both halves.
        close_prior(delta, sealed, &key, commit, txn_id, principal.clone())?;
        apply(
            delta,
            vec![open_version(key, commit, payload, txn_id, principal)],
        )?;
        Ok(commit)
    }

    /// Close the live version of `key` without re-opening — a logical delete.
    /// Afterwards the key has no version live at any snapshot `≥ commit`.
    ///
    /// The deleting transaction's `txn_id` + `principal` are recorded as the
    /// closed version's `closed_by` provenance — a delete is a logical
    /// period-close that "carries its own provenance"
    /// ([architecture §3.1](../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving),
    /// [STL-118]). Unlike an [`update`](Self::update), a delete leaves no
    /// successor version, so this is the only record of who performed it.
    ///
    /// Returns the `commit` at which the period was closed.
    ///
    /// Like [`update`](Self::update), the close lands where the live version
    /// lives: re-staged in the delta tier, or appended as a [`CloseMarker`] when
    /// the version is already sealed ([STL-127]).
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version. Delta-tier
    /// errors propagate as [`SysTimeError::Delta`]; segment-read errors as
    /// [`SysTimeError::Sealed`].
    pub fn delete<D: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        sealed: &S,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, SysTimeError> {
        let commit = self.next_commit_ts()?;
        close_prior(delta, sealed, key, commit, txn_id, principal)?;
        Ok(commit)
    }

    /// Resolve an insert into the redo set it stages — **without** touching the
    /// delta tier. Returns the stamped commit timestamp and the version(s) to
    /// apply (a single open version `[commit, +∞)`).
    ///
    /// This is the resolution half of the write path: it stamps the commit
    /// timestamp and builds the rows, but leaves *applying* them to the caller.
    /// [`Self::insert`] applies them straight to the delta; the DML write path
    /// ([`crate::dml`]) logs them to the WAL first, then applies the same set —
    /// so a forward write and a crash-recovery replay run identical inserts
    /// ([architecture §3.4](../../../docs/02-architecture.md#34-write-path-sequence)).
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyExists`] if `key` already has a live version (the
    /// commit timestamp is still consumed, matching [`Self::insert`]);
    /// [`SysTimeError::TimeExhausted`] from the commit-timestamp allocator.
    pub fn stage_insert<D: Disk>(
        &mut self,
        delta: &Delta<D>,
        key: BusinessKey,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Version>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        if current_open(delta, &key, commit)?.is_some() {
            return Err(SysTimeError::KeyExists);
        }
        Ok((
            commit,
            vec![open_version(key, commit, payload, txn_id, principal)],
        ))
    }

    /// Resolve an update into the redo set it stages — the prior version closed
    /// at `commit` plus the new open version `[commit, +∞)` — without touching
    /// the delta tier. The two abut, so the chain stays gap-free. See
    /// [`Self::stage_insert`] for why resolution is split from application.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version;
    /// [`SysTimeError::TimeExhausted`] from the commit-timestamp allocator;
    /// delta-tier read errors as [`SysTimeError::Delta`].
    pub fn stage_update<D: Disk>(
        &mut self,
        delta: &Delta<D>,
        key: BusinessKey,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Version>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        // The superseding transaction both closes the prior period (stamping its
        // identity as `closed_by`) and opens the new one — same `txn_id` /
        // `principal` for both halves.
        let closed = closed_prior_version(delta, &key, commit, txn_id, principal.clone())?;
        let opened = open_version(key, commit, payload, txn_id, principal);
        Ok((commit, vec![closed, opened]))
    }

    /// Resolve a delete into the redo set it stages — the prior version closed
    /// at `commit`, with no successor — without touching the delta tier. See
    /// [`Self::stage_insert`] for why resolution is split from application, and
    /// [`Self::delete`] for the tombstone-provenance semantics.
    ///
    /// # Errors
    ///
    /// [`SysTimeError::KeyNotFound`] if `key` has no live version;
    /// [`SysTimeError::TimeExhausted`] from the commit-timestamp allocator;
    /// delta-tier read errors as [`SysTimeError::Delta`].
    pub fn stage_delete<D: Disk>(
        &mut self,
        delta: &Delta<D>,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Version>), SysTimeError> {
        let commit = self.next_commit_ts()?;
        let closed = closed_prior_version(delta, key, commit, txn_id, principal)?;
        Ok((commit, vec![closed]))
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
        // A commit must stay strictly below the +∞ open sentinel, or it would be
        // indistinguishable from an open period. Enforced in every build, not
        // just via debug_assert — the cost is one comparison.
        if ts >= SYSTEM_TIME_OPEN {
            return Err(SysTimeError::TimeExhausted);
        }
        self.last_commit = Some(ts);
        Ok(ts)
    }
}

/// Apply a resolved redo set to the delta tier: insert each version in order.
///
/// The single application point shared by every write path. A forward write
/// (via [`SysTimeWriter::insert`] and friends), the WAL-logging DML writer
/// ([`crate::dml::DmlWriter`]), and a crash-recovery replay
/// ([`crate::dml::replay`]) all funnel their resolved versions through here, so
/// "the same code path under sim and under real I/O" is structural, not a
/// promise. Re-inserting the same `(business_key, sys_from)` is the delta tier's
/// idempotent replace, which is what makes replay safe to run over already-
/// applied records.
///
/// Returns the raw [`DeltaError`] so each caller maps it onto its own error type
/// (`SysTimeError::Delta` here, `DmlError::Delta` in [`crate::dml`]) via `?`.
pub(crate) fn apply<D: Disk>(
    delta: &mut Delta<D>,
    versions: Vec<Version>,
) -> Result<(), DeltaError> {
    for version in versions {
        delta.insert(version)?;
    }
    Ok(())
}

/// Build (but do **not** apply) the closed form of `key`'s current open version:
/// the same row re-stamped with `sys_to = commit` and the closing transaction's
/// provenance. Applying it is an idempotent replace of the same
/// `(business_key, sys_from)`, so the period is updated in place rather than
/// duplicated.
///
/// **Delta-tier only.** This is the resolution half the staging path uses
/// ([`SysTimeWriter::stage_update`] / [`SysTimeWriter::stage_delete`]), which the
/// WAL-logging DML writer drives — so a close it resolves can be logged to the
/// WAL before it is applied. The cross-tier close that also handles a live
/// version already flushed into a sealed segment is [`close_prior`] ([STL-127]);
/// wiring that through the staging/WAL path is a follow-up ([STL-128]).
///
/// The prior version's **birth provenance is preserved untouched**: closing a
/// period is bookkeeping by the superseding transaction, not a rewrite of who
/// wrote the closed version, so `txn_id` / `committed_at` / `principal` keep
/// their original values. What the close *adds* is `closed_by` — the `txn_id` /
/// `principal` of the transaction performing the close, with `committed_at`
/// stamped to `commit` (which equals the new `sys_to`). For a `delete` there is
/// no successor version to carry that identity, so recording it here is the only
/// place "who closed this period" survives ([architecture §3.1](../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving),
/// [STL-118]).
fn closed_prior_version<D: Disk>(
    delta: &Delta<D>,
    key: &BusinessKey,
    commit: SystemTimeMicros,
    txn_id: TxnId,
    principal: Principal,
) -> Result<Version, SysTimeError> {
    let mut prior = current_open(delta, key, commit)?.ok_or(SysTimeError::KeyNotFound)?;
    prior.sys_to = commit;
    prior.closed_by = Some(Provenance::new(txn_id, commit, principal));
    Ok(prior)
}

/// Close `key`'s current open version at `commit`, stamping the closing
/// transaction's provenance — **wherever that version lives** ([STL-127]).
///
/// * **Still in the delta tier** ([`LiveLocation::Delta`]): re-stage it with
///   `sys_to = commit` and `closed_by`. Re-inserting the same
///   `(business_key, sys_from)` is the delta tier's idempotent replace, so the
///   period is updated in place rather than duplicated — the original [STL-91]
///   path.
/// * **Already sealed** ([`LiveLocation::Sealed`]): append a [`CloseMarker`]
///   naming the sealed version's `sys_from` and the new `sys_to`. The segment is
///   never touched — invariant 1. The read path folds the marker onto the sealed
///   version to surface the closed interval.
///
/// The forward-path counterpart to [`closed_prior_version`]: it both *resolves*
/// and *applies*, and it is cross-tier. The prior version's birth provenance is
/// preserved untouched in either case — corrections append, never rewrite
/// ([STL-118]).
fn close_prior<D: Disk, S: SealedLookup>(
    delta: &mut Delta<D>,
    sealed: &S,
    key: &BusinessKey,
    commit: SystemTimeMicros,
    txn_id: TxnId,
    principal: Principal,
) -> Result<(), SysTimeError> {
    match resolve_live(delta, sealed, key, commit)?.ok_or(SysTimeError::KeyNotFound)? {
        LiveLocation::Delta(mut prior) => {
            prior.sys_to = commit;
            prior.closed_by = Some(Provenance::new(txn_id, commit, principal));
            delta.insert(prior)?;
        }
        LiveLocation::Sealed(prior) => {
            delta.insert_close_marker(CloseMarker {
                business_key: key.clone(),
                sys_from: prior.sys_from,
                sys_to: commit,
                closed_by: Provenance::new(txn_id, commit, principal),
            });
        }
    }
    Ok(())
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
    Version {
        business_key: key,
        sys_from: commit,
        sys_to: SYSTEM_TIME_OPEN,
        provenance: Provenance::new(txn_id, commit, principal),
        // Open: no period-close yet, so no closing provenance ([STL-118]).
        closed_by: None,
        payload,
    }
}

/// The version of `key` that is live at `at`, resolved **across both tiers** and
/// reporting which tier holds its body ([`LiveLocation`]).
///
/// Gathers the key's staged delta candidates, the delta tier's close markers,
/// and the key's sealed versions, then folds the markers and picks the live
/// version ([`crate::merge::resolve_open`]). `at` is the freshly-allocated commit
/// timestamp, strictly greater than every `sys_from` already on the key's chain,
/// so the open version (if one exists) is always the one resolved — scanning at
/// [`SYSTEM_TIME_OPEN`] would instead exclude it, since the resolver's
/// `sys_to > at` test fails at that exact point.
fn resolve_live<D: Disk, S: SealedLookup>(
    delta: &Delta<D>,
    sealed: &S,
    key: &BusinessKey,
    at: SystemTimeMicros,
) -> Result<Option<LiveLocation>, SysTimeError> {
    let delta_versions = delta.candidate_versions(key)?;
    let sealed_versions = sealed.versions_for(key)?;
    let markers: Vec<CloseMarker> = delta
        .close_markers()
        .filter(|m| &m.business_key == key)
        .cloned()
        .collect();
    Ok(merge::resolve_open(
        &delta_versions,
        &markers,
        &sealed_versions,
        key,
        Snapshot(at),
    ))
}

/// The version of `key` that is live at `at`, if any — **delta tier only**.
///
/// `at` is the freshly-allocated commit timestamp, strictly greater than every
/// `sys_from` already on the key's chain, so the open version (if one exists) is
/// always live at `at`. Scanning at [`SYSTEM_TIME_OPEN`] would *not* work: an
/// open version has `sys_to == SYSTEM_TIME_OPEN`, which the resolver's
/// `sys_to > at` test excludes at that exact point.
///
/// This is the delta-only resolver the staging path uses ([`SysTimeWriter::stage_insert`],
/// [`closed_prior_version`]); the cross-tier resolver that also consults sealed
/// segments is [`resolve_live`] ([STL-127]).
fn current_open<D: Disk>(
    delta: &Delta<D>,
    key: &BusinessKey,
    at: SystemTimeMicros,
) -> Result<Option<Version>, DeltaError> {
    // `range_scan` returns at most one live version per key.
    let live = delta.range_scan(key.clone()..=key.clone(), Snapshot(at))?;
    Ok(live.into_iter().next())
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
        // Clock does not move: the guard must still advance.
        let b = writer.next_commit_ts().unwrap();
        // Clock regresses: still must advance.
        writer.clock.set(50);
        let c = writer.next_commit_ts().unwrap();
        // Clock jumps far ahead: take the larger value.
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
        // A clock reading at the +∞ sentinel must not be stamped as a real
        // commit — it would be indistinguishable from an open period. Enforced
        // via a typed error, not a debug-only assertion.
        let clock = StubClock::new(SYSTEM_TIME_OPEN.0);
        let mut writer = SysTimeWriter::new(clock);
        assert!(matches!(
            writer.next_commit_ts(),
            Err(SysTimeError::TimeExhausted)
        ));
        // The failed allocation left no high-water mark behind.
        assert_eq!(writer.last_commit(), None);
    }

    #[test]
    fn commit_one_below_the_sentinel_is_allowed_but_the_next_is_refused() {
        // The monotonic guard would push the *next* commit to the sentinel; that
        // step must fail rather than wrap into +∞.
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
