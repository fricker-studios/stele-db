//! Valid-time ingestion — the second axis of the bitemporal record model.
//!
//! System-time ([`crate::systime`]) records *when the database held a version*
//! and is always present. **Valid-time** records *when a fact is true in the
//! modeled world* and is **per-table opt-in** (invariant 4 of
//! [architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants),
//! [architecture §2](../../../docs/02-architecture.md#2-the-bitemporal-record-model)).
//! Unlike system-time, it is *supplied by the writer*, never stamped from a
//! clock.
//!
//! This module is the write-path ingestion for that second axis. It does two
//! things and deliberately nothing more:
//!
//! 1. **Enforces the per-table policy.** A write to a valid-time table *must*
//!    carry a `[valid_from, valid_to)` pair; a write to a system-only table
//!    *must not*. The mismatch is a typed error, not a silent default — see
//!    [`ValidTimeError::ValidTimeRequired`] / [`ValidTimeError::ValidTimeNotSupported`].
//! 2. **Carries the interval inside the version payload.** The delta tier keeps
//!    `payload` opaque ([`crate::delta::Version`]) and does not interpret
//!    valid-time — exactly as that type's docs promised this ticket would do.
//!    The interval rides as a fixed 16-byte little-endian prefix on the stored
//!    payload, which a reader recovers with [`unframe_payload`].
//!
//! ```text
//! stored payload (valid-time table) = | valid_from: i64 | valid_to: i64 | user payload … |
//! stored payload (system-only)      = | user payload … |
//! ```
//!
//! Whether the prefix is present is governed by the table's catalog flag
//! (`stele_catalog::TableTemporal::valid_time_enabled`), the same way a columnar
//! layout is schema-driven rather than self-describing — the reader knows the
//! schema, so the bytes need carry no tag. The prefix above is the *delta-tier*
//! framing: it rides on [`Version::payload`](crate::delta::Version) through the
//! WAL and memtable. At flush the segment writer lifts the interval into
//! first-class `valid_from` / `valid_to` columns for zone-map pruning ([STL-117])
//! and stores only the **bare** user payload in the segment, dropping the now
//! redundant prefix ([STL-119]) — see
//! [`SegmentWriter::create_valid_time`](crate::segment::SegmentWriter::create_valid_time).
//! On read the segment re-frames the payload from those columns
//! ([`reframe_payload`]) so a reconstructed `Version` is byte-identical; the
//! [`crate::segment`] zone maps prune the valid axis generically.
//!
//! Provenance ([STL-93]) is *not* in the payload: unlike valid-time it is
//! always-on and first-class, carried as dedicated [`Version`](crate::delta::Version)
//! fields and segment columns. [`ValidTimeWriter`] simply forwards the caller's
//! `txn_id` / `principal` down to the system-time writer, which stamps them at
//! commit.
//!
//! ## What this is *not*
//!
//! This is ingestion, not query semantics. [`ValidInterval::contains`] is a
//! plain half-open interval test — a building block — **not** an `AS OF`
//! resolver. Picking, per business key, the version whose system *and* valid
//! intervals both contain a query point, and the correctness oracle that proves
//! it, are a separate ticket (this story's Definition of Done defers them). No
//! code here selects among versions on the valid axis.
//!
//! ```ignore
//! // valid-time table: every write carries the interval; both axes populated.
//! let mut w = ValidTimeWriter::new(SystemClock, true);
//! let v = ValidInterval::new(ValidTimeMicros(day(1)), VALID_TIME_OPEN)?;
//! // A supersession resolves the prior live version across the delta tier and
//! // the sealed segments: pass `EmptySealed` when the table has none, or a
//! // `SealedSegments` built from its segment set (the DML path does the latter).
//! w.insert(&mut delta, &mut index, &EmptySealed, key.clone(), Some(v),
//!          b"salary=100".to_vec(), txn_id, principal)?;
//! ```

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};

use crate::backend::Disk;
use crate::delta::{BusinessKey, Delta};
use crate::systime::{Redo, SealedLookup, SysTimeError, SysTimeWriter};
use crate::validity::ValidityIndex;

/// Length of the little-endian valid-time prefix stamped on a stored payload:
/// two `i64` boundaries (`valid_from`, `valid_to`).
pub const VALID_TIME_PREFIX_LEN: usize = 16;

/// Errors surfaced from the valid-time write path.
#[derive(Debug, thiserror::Error)]
pub enum ValidTimeError {
    /// `valid_from >= valid_to`: the period is empty or inverted. Valid-time
    /// intervals are half-open `[from, to)`, so the start must be strictly
    /// before the end.
    #[error("valid-time interval is empty or inverted: valid_from ({0}) must be < valid_to ({1})")]
    EmptyInterval(i64, i64),

    /// A write to a **valid-time** table supplied no `[valid_from, valid_to)`
    /// pair. The second axis is mandatory once a table opts in — there is no
    /// default the engine could invent for "when is this true in the world."
    #[error("table opts into valid-time but the write supplied no valid-time interval")]
    ValidTimeRequired,

    /// A write to a **system-only** table supplied a valid-time pair. The table
    /// has no valid-time period to put it in; accepting it silently would let
    /// the caller believe a second axis is being tracked when it is not.
    #[error("table is system-only but the write supplied a valid-time interval")]
    ValidTimeNotSupported,

    /// A stored payload from a valid-time table was shorter than the 16-byte
    /// prefix — a truncated or mis-tagged frame.
    #[error("stored payload too short to hold a valid-time interval prefix")]
    Truncated,

    /// An error bubbled up from the system-time write path.
    #[error(transparent)]
    SysTime(#[from] SysTimeError),
}

/// A half-open valid-time interval `[from, to)` — when a fact is true in the
/// modeled world.
///
/// Constructed through [`ValidInterval::new`], which rejects empty/inverted
/// intervals, so a `ValidInterval` value is always well-formed. `to` may be
/// [`VALID_TIME_OPEN`](stele_common::time::VALID_TIME_OPEN) for a fact with no
/// known end ("valid until further notice").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidInterval {
    /// Inclusive start of the period.
    pub from: ValidTimeMicros,
    /// Exclusive end of the period; `+∞` for an open-ended fact.
    pub to: ValidTimeMicros,
}

impl ValidInterval {
    /// Build a half-open `[from, to)` valid-time interval.
    ///
    /// # Errors
    ///
    /// [`ValidTimeError::EmptyInterval`] if `from >= to`.
    pub const fn new(from: ValidTimeMicros, to: ValidTimeMicros) -> Result<Self, ValidTimeError> {
        if from.0 >= to.0 {
            return Err(ValidTimeError::EmptyInterval(from.0, to.0));
        }
        Ok(Self { from, to })
    }

    /// Whether `point` lies in `[from, to)`. A plain half-open membership test —
    /// the primitive a reader filters with. See the [module docs](self): this is
    /// not an `AS OF` resolver and does not look across versions.
    #[must_use]
    pub const fn contains(&self, point: ValidTimeMicros) -> bool {
        self.from.0 <= point.0 && point.0 < self.to.0
    }

    /// Append this interval's 16-byte little-endian prefix to `out`.
    fn encode_prefix(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.from.0.to_le_bytes());
        out.extend_from_slice(&self.to.0.to_le_bytes());
    }

    /// Read an interval from the head of a stored payload.
    ///
    /// # Errors
    ///
    /// [`ValidTimeError::Truncated`] if fewer than [`VALID_TIME_PREFIX_LEN`]
    /// bytes are available.
    fn decode_prefix(bytes: &[u8]) -> Result<Self, ValidTimeError> {
        if bytes.len() < VALID_TIME_PREFIX_LEN {
            return Err(ValidTimeError::Truncated);
        }
        let from = i64::from_le_bytes(bytes[0..8].try_into().expect("8-byte slice converts"));
        let to = i64::from_le_bytes(bytes[8..16].try_into().expect("8-byte slice converts"));
        // Re-validate the invariant on decode so corrupted bytes can't create an
        // invalid interval.
        Self::new(ValidTimeMicros(from), ValidTimeMicros(to))
    }
}

/// Build the stored payload for a write, applying the table's valid-time policy.
///
/// * valid-time table (`enabled`) **with** an interval → 16-byte prefix + payload.
/// * valid-time table **without** an interval → [`ValidTimeError::ValidTimeRequired`].
/// * system-only table **with** an interval → [`ValidTimeError::ValidTimeNotSupported`].
/// * system-only table **without** an interval → payload unchanged.
///
/// A `None` (SQL `NULL`) user payload ([STL-154]) passes through unchanged on a
/// system-only table — the `None` is carried distinctly all the way to the
/// durable record. On a valid-time table the interval prefix must still be
/// stored, so a `NULL` user value degrades to an empty bare payload behind the
/// prefix; this edge is unreachable from v0.1 DML (a valid-time table is never
/// the two-column `(key, payload)` shape the binder requires), but is defined
/// rather than left to panic.
///
/// # Errors
///
/// The two policy-mismatch variants above.
pub fn frame_payload(
    enabled: bool,
    valid: Option<ValidInterval>,
    user_payload: Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, ValidTimeError> {
    match (enabled, valid) {
        (true, Some(interval)) => {
            let user = user_payload.unwrap_or_default();
            let mut out = Vec::with_capacity(VALID_TIME_PREFIX_LEN + user.len());
            interval.encode_prefix(&mut out);
            out.extend_from_slice(&user);
            Ok(Some(out))
        }
        (true, None) => Err(ValidTimeError::ValidTimeRequired),
        (false, None) => Ok(user_payload),
        (false, Some(_)) => Err(ValidTimeError::ValidTimeNotSupported),
    }
}

/// Recover `(valid interval, user payload)` from a stored payload, given the
/// table's valid-time policy. The inverse of [`frame_payload`]; this is what
/// lets a read filter on the valid axis.
///
/// Returns the borrowed user-payload slice (no copy). For a system-only table
/// the interval is `None` and the slice is the whole payload.
///
/// # Errors
///
/// [`ValidTimeError::Truncated`] if a valid-time payload is shorter than the
/// prefix.
pub fn unframe_payload(
    enabled: bool,
    stored: &[u8],
) -> Result<(Option<ValidInterval>, &[u8]), ValidTimeError> {
    if enabled {
        let interval = ValidInterval::decode_prefix(stored)?;
        Ok((Some(interval), &stored[VALID_TIME_PREFIX_LEN..]))
    } else {
        Ok((None, stored))
    }
}

/// Rebuild the framed payload from a bare user payload and its raw valid-time
/// boundaries, reproducing byte-for-byte the 16-byte little-endian prefix that
/// [`frame_payload`] would have stored.
///
/// The byte-level inverse of the strip [`unframe_payload`] performs. A
/// valid-time *segment* stores only the bare payload, with the interval lifted
/// into first-class `valid_from` / `valid_to` columns ([STL-117]) and the
/// redundant prefix dropped ([STL-119]); on read the segment recovers the
/// boundaries from those i64 columns and calls this to reconstruct the framed
/// payload, so a rebuilt [`crate::delta::Version`] round-trips exactly as
/// written.
///
/// Takes the raw boundary microseconds rather than a [`ValidInterval`] because
/// that is precisely what the i64 columns yield, and reconstruction must
/// reproduce the stored bytes verbatim — re-imposing [`ValidInterval::new`]'s
/// `from < to` check here would only invent a failure mode the stored bytes
/// cannot exhibit.
#[must_use]
pub fn reframe_payload(valid_from: i64, valid_to: i64, user: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALID_TIME_PREFIX_LEN + user.len());
    out.extend_from_slice(&valid_from.to_le_bytes());
    out.extend_from_slice(&valid_to.to_le_bytes());
    out.extend_from_slice(user);
    out
}

/// Default cap on the number of intervals a [`ValidIntervalSummary`] retains
/// ([STL-241]). Each interval costs 16 footer bytes (two `i64` boundaries), so
/// 256 bounds a segment's summary at ~4 KiB regardless of how many distinct
/// valid windows its rows carry; a segment with more coalesced intervals than
/// this has the closest-adjacent ones merged (a sound widening — see
/// [`ValidIntervalSummary::build`]). Purely a writer-side choice: the on-disk
/// section is length-prefixed, so changing it needs no format bump.
///
/// [STL-241]: https://allegromusic.atlassian.net/browse/STL-241
pub(crate) const DEFAULT_VALID_INTERVAL_CAP: usize = 256;

/// A bounded, coalesced summary of the `[valid_from, valid_to)` intervals a
/// sealed segment's rows carry — the per-segment valid-time interval index
/// ([ADR-0025], [STL-241]).
///
/// ## The scatter problem it solves
///
/// System-time prunes well via zone maps because it is monotonic: a segment's
/// `sys_from` min/max is tight. **Valid-time is not** — Stele's signature
/// workload is backdated corrections, which land in *today's* segment carrying
/// *old* valid-times, so the segment's `valid_from` / `valid_to` min/max spans
/// almost the whole timeline and the zone-map valid-axis skips ([STL-173])
/// prune nothing. But the spanned envelope is mostly *gaps*: the actual covered
/// windows are sparse. This summary records the **union** of the covered
/// windows, so a `FOR VALID_TIME AS OF v` read whose `v` falls in a gap skips
/// the whole segment even though its min/max envelope contains `v`.
///
/// ## The superset contract
///
/// Like the segment bloom ([`crate::bloom::KeyBloom`], [STL-238]) the summary is
/// **advisory and read-gating only**: [`covers`](Self::covers) may answer `true`
/// for a `v` no row actually holds (a false positive — the row-level valid
/// filter then drops nothing extra), but it must **never** answer `false` for a
/// `v` some row holds. [`build`](Self::build) upholds this by storing a *superset*
/// of the rows' intervals: coalescing tiles them exactly, and the gap-merge that
/// bounds the count only ever *widens* coverage. So a segment is pruned only when
/// the summary proves no row is valid at `v` — never a false negative, exactly
/// the soundness obligation the executor's correctness oracles pin.
///
/// ## Derived, never durable
///
/// The summary rides the immutable segment it summarizes — written into the
/// footer at flush, recomputed verbatim on compaction, reloaded on cold boot —
/// so it survives flush / compaction / recovery with **no separate derived
/// structure to rebuild**, the same posture the validity index ([ADR-0023]) and
/// the segment bloom take.
///
/// [ADR-0025]: ../../../docs/adr/0025-valid-time-indexing.md
/// [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
/// [STL-241]: https://allegromusic.atlassian.net/browse/STL-241
/// [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
/// [STL-173]: https://allegromusic.atlassian.net/browse/STL-173
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidIntervalSummary {
    /// Disjoint, non-touching half-open `[from, to)` intervals, sorted ascending
    /// by `from` — the coalesced (and, if over the cap, gap-merged) union of the
    /// segment's row intervals. Never empty for a present summary (the writer
    /// omits the section for a segment with no rows).
    intervals: Vec<(i64, i64)>,
}

impl ValidIntervalSummary {
    /// Build a summary over the segment's `(valid_from, valid_to)` pairs, capped
    /// at `cap` intervals (clamped to at least 1).
    ///
    /// Empty or inverted pairs (`from >= to`) are dropped — they cover no point,
    /// so omitting them keeps the union exact. The kept intervals are sorted and
    /// **coalesced** (overlapping or touching windows merge into one, which tiles
    /// them exactly), then, if more than `cap` disjoint intervals remain, the two
    /// adjacent intervals separated by the **smallest gap** are merged repeatedly
    /// until the count fits. Merging across a gap only *adds* coverage, so the
    /// result stays a superset of the rows' intervals — sound for pruning, just
    /// less selective.
    pub(crate) fn build(pairs: impl IntoIterator<Item = (i64, i64)>, cap: usize) -> Self {
        let mut ivs: Vec<(i64, i64)> = pairs.into_iter().filter(|&(f, t)| f < t).collect();
        ivs.sort_unstable();
        // Coalesce overlapping / touching intervals. `[a, b)` and `[c, d)` with
        // `c <= b` union to `[a, max(b, d))` — half-open, so a touch (`c == b`)
        // tiles seamlessly and merges too.
        let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ivs.len());
        for (f, t) in ivs {
            match merged.last_mut() {
                Some(last) if f <= last.1 => last.1 = last.1.max(t),
                _ => merged.push((f, t)),
            }
        }
        // Bound the count by merging the smallest gaps first — the widening that
        // adds the least phantom coverage. `cap` is small and `merged` shrinks by
        // one each pass, so the O(n·cap) loop is fine at flush time.
        let cap = cap.max(1);
        while merged.len() > cap {
            let mut best = 0usize;
            let mut best_gap = i128::MAX;
            for i in 0..merged.len() - 1 {
                // i128 so an `i64::MAX − i64::MIN` gap cannot overflow.
                let gap = i128::from(merged[i + 1].0) - i128::from(merged[i].1);
                if gap < best_gap {
                    best_gap = gap;
                    best = i;
                }
            }
            merged[best].1 = merged[best].1.max(merged[best + 1].1);
            merged.remove(best + 1);
        }
        Self { intervals: merged }
    }

    /// Whether some retained interval contains `point` — the `valid at V` stab.
    /// A `false` **proves** no row in the segment is valid at `point` (the
    /// superset contract), so the scan may skip the whole segment.
    pub(crate) fn covers(&self, point: i64) -> bool {
        // The intervals are disjoint and sorted by `from`, so the only candidate
        // is the last one whose `from <= point`; check `point < to` on it.
        let idx = self.intervals.partition_point(|&(from, _)| from <= point);
        idx > 0 && point < self.intervals[idx - 1].1
    }

    /// Whether the summary is empty — no covered point. A built summary is empty
    /// only when every pair was empty/inverted; the writer never persists one.
    pub(crate) const fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    /// Append the summary to `out` for footer persistence: interval count (`u32`
    /// LE) then that many `(valid_from: i64 LE, valid_to: i64 LE)` pairs.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(
            &u32::try_from(self.intervals.len())
                .expect("interval count is capped well below u32::MAX")
                .to_le_bytes(),
        );
        for (from, to) in &self.intervals {
            out.extend_from_slice(&from.to_le_bytes());
            out.extend_from_slice(&to.to_le_bytes());
        }
    }

    /// The encoded length in bytes — `4 + 16 * interval_count`. Lets the footer
    /// writer size its buffer without a trial encode.
    pub(crate) const fn encoded_len(&self) -> usize {
        4 + self.intervals.len() * 16
    }

    /// Decode a summary written by [`Self::encode`], returning it and the number
    /// of bytes consumed.
    ///
    /// # Errors
    ///
    /// A static reason if the buffer is short, the count is zero (a present
    /// section always summarizes at least one interval — the writer omits the
    /// section otherwise), or any interval is empty/inverted (`from >= to`,
    /// which `build` never emits). The footer CRC already guards integrity, so
    /// these checks are a fail-closed cross-check on a successfully-checksummed
    /// footer, mirroring the segment bloom's decode validation.
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, usize), &'static str> {
        let count = bytes
            .get(0..4)
            .ok_or("missing valid-interval count")?
            .try_into()
            .expect("4-byte slice");
        let count = u32::from_le_bytes(count) as usize;
        if count == 0 {
            return Err("zero valid-interval count");
        }
        let end = 4 + count * 16;
        let body = bytes
            .get(4..end)
            .ok_or("truncated valid-interval section")?;
        let mut intervals = Vec::with_capacity(count);
        for pair in body.chunks_exact(16) {
            let from = i64::from_le_bytes(pair[0..8].try_into().expect("8-byte slice"));
            let to = i64::from_le_bytes(pair[8..16].try_into().expect("8-byte slice"));
            if from >= to {
                return Err("empty or inverted valid interval in footer");
            }
            intervals.push((from, to));
        }
        Ok((Self { intervals }, end))
    }
}

/// Stamps both temporal axes as writes flow into the delta tier: system-time
/// via an inner [`SysTimeWriter`], valid-time via the per-table policy enforced
/// here.
///
/// One writer is bound to one table's valid-time setting (`enabled`), which
/// mirrors `stele_catalog::TableTemporal::valid_time_enabled` for that table.
/// The DML layer ([STL-94]) constructs the writer from the catalog flag; this
/// crate stays free of a catalog dependency by taking the resolved policy as a
/// `bool`.
///
/// Each write threads a caller-supplied [`SealedLookup`] down to the bare
/// [`SysTimeWriter`], so a supersession resolves the prior live version across
/// the delta tier, the sealed segments, **and** the validity index ([STL-140]).
/// The DML write path ([`crate::dml`]) builds that lookup from the table's
/// segment set — typically a [`SealedSegments`](crate::systime::SealedSegments)
/// that zone-map–prunes per key; a table with no sealed segments passes
/// [`EmptySealed`](crate::systime::EmptySealed). A sealed live version is closed
/// the same way a delta-resident one is: a write-once append to the validity
/// index regardless of tier ([ADR-0023]).
#[derive(Debug)]
pub struct ValidTimeWriter<C: Clock> {
    inner: SysTimeWriter<C>,
    valid_time: bool,
}

impl<C: Clock> ValidTimeWriter<C> {
    /// Create a writer for a table whose valid-time opt-in is `valid_time`,
    /// drawing commit timestamps from `clock` for the system axis.
    #[must_use]
    pub const fn new(clock: C, valid_time: bool) -> Self {
        Self {
            inner: SysTimeWriter::new(clock),
            valid_time,
        }
    }

    /// Whether this writer's table tracks valid-time.
    #[must_use]
    pub const fn valid_time_enabled(&self) -> bool {
        self.valid_time
    }

    /// The most recent system-time commit this writer stamped, if any. Passthrough
    /// to the inner [`SysTimeWriter::last_commit`].
    #[must_use]
    pub const fn last_commit(&self) -> Option<SystemTimeMicros> {
        self.inner.last_commit()
    }

    /// Open the first version of `key`, populating **both** axes: a system-time
    /// period `[commit, +∞)` and (for a valid-time table) the supplied
    /// `[valid_from, valid_to)`.
    ///
    /// Returns the stamped system-time `sys_from`.
    ///
    /// # Errors
    ///
    /// Policy mismatches ([`ValidTimeError::ValidTimeRequired`] /
    /// [`ValidTimeError::ValidTimeNotSupported`]) before anything is staged;
    /// otherwise whatever the system-time path returns (e.g.
    /// [`SysTimeError::KeyExists`]).
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/valid/payload + provenance triple
    pub fn insert<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, ValidTimeError> {
        let framed = frame_payload(self.valid_time, valid, payload)?;
        Ok(self
            .inner
            .insert(delta, index, sealed, key, framed, seq, txn_id, principal)?)
    }

    /// Supersede the live version of `key`: close the prior system-time period
    /// and open a new one carrying the new valid-time interval. The prior
    /// version keeps its own valid interval — corrections append, never mutate.
    ///
    /// Returns the stamped system-time `sys_from` of the new version.
    ///
    /// # Errors
    ///
    /// Policy mismatches as in [`Self::insert`]; otherwise the system-time path's
    /// errors (e.g.
    /// [`SysTimeError::KeyNotFound`]).
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/valid/payload + provenance triple
    pub fn update<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, ValidTimeError> {
        let framed = frame_payload(self.valid_time, valid, payload)?;
        Ok(self
            .inner
            .update(delta, index, sealed, key, framed, seq, txn_id, principal)?)
    }

    /// Close the live version of `key` on the system axis without re-opening — a
    /// logical delete. Carries no valid-time interval: a delete records *when
    /// the database stopped holding the row*, which is a system-time fact; the
    /// closed version retains the valid interval it was written with.
    ///
    /// The deleting transaction's `txn_id` + `principal` are recorded as the
    /// closed version's `closed_by` provenance, forwarded verbatim to the
    /// system-time path ([STL-118]) — valid-time has no bearing on *who* closed
    /// a period.
    ///
    /// Returns the system-time `commit` at which the period was closed.
    ///
    /// # Errors
    ///
    /// The system-time path's errors (e.g.
    /// [`SysTimeError::KeyNotFound`]).
    pub fn delete<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &mut Delta<D>,
        index: &mut ValidityIndex<I>,
        sealed: &S,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, ValidTimeError> {
        Ok(self
            .inner
            .delete(delta, index, sealed, key, txn_id, principal)?)
    }

    /// Resolve an insert into the redo set it stages — both temporal axes
    /// populated, framed payload built — **without** touching the delta tier.
    /// The valid-time framing happens here; the rest delegates to
    /// [`SysTimeWriter::stage_insert`]. The DML write path ([`crate::dml`]) logs
    /// the returned versions to the WAL before applying them.
    ///
    /// # Errors
    ///
    /// Policy mismatches as in [`Self::insert`]; otherwise the system-time
    /// resolution's errors (e.g. [`SysTimeError::KeyExists`]).
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/valid/payload + provenance triple
    pub fn stage_insert<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), ValidTimeError> {
        let framed = frame_payload(self.valid_time, valid, payload)?;
        Ok(self
            .inner
            .stage_insert(delta, index, sealed, key, framed, seq, txn_id, principal)?)
    }

    /// Resolve an update into the redo set it stages — the prior version closed
    /// plus a new open version carrying the new valid interval — without
    /// touching the delta tier. See [`Self::stage_insert`].
    ///
    /// # Errors
    ///
    /// Policy mismatches as in [`Self::insert`]; otherwise the system-time
    /// resolution's errors (e.g. [`SysTimeError::KeyNotFound`]).
    #[allow(clippy::too_many_arguments)] // tier handles + sealed + key/valid/payload + provenance triple
    pub fn stage_update<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        sealed: &S,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Option<Vec<u8>>,
        seq: u64,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), ValidTimeError> {
        let framed = frame_payload(self.valid_time, valid, payload)?;
        Ok(self
            .inner
            .stage_update(delta, index, sealed, key, framed, seq, txn_id, principal)?)
    }

    /// Resolve a delete into the redo set it stages — the prior version closed,
    /// no successor — without touching the delta tier. Carries no valid-time
    /// interval (a delete is a system-time fact); see [`Self::delete`].
    ///
    /// # Errors
    ///
    /// The system-time resolution's errors (e.g. [`SysTimeError::KeyNotFound`]).
    pub fn stage_delete<D: Disk, I: Disk, S: SealedLookup>(
        &mut self,
        delta: &Delta<D>,
        index: &ValidityIndex<I>,
        sealed: &S,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<(SystemTimeMicros, Vec<Redo>), ValidTimeError> {
        Ok(self
            .inner
            .stage_delete(delta, index, sealed, key, txn_id, principal)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::time::VALID_TIME_OPEN;

    fn iv(from: i64, to: i64) -> ValidInterval {
        ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed")
    }

    #[test]
    fn empty_or_inverted_intervals_are_rejected() {
        assert!(matches!(
            ValidInterval::new(ValidTimeMicros(5), ValidTimeMicros(5)),
            Err(ValidTimeError::EmptyInterval(5, 5))
        ));
        assert!(matches!(
            ValidInterval::new(ValidTimeMicros(9), ValidTimeMicros(3)),
            Err(ValidTimeError::EmptyInterval(9, 3))
        ));
    }

    #[test]
    fn contains_is_half_open() {
        let v = iv(10, 20);
        assert!(!v.contains(ValidTimeMicros(9)));
        assert!(v.contains(ValidTimeMicros(10))); // inclusive start
        assert!(v.contains(ValidTimeMicros(19)));
        assert!(!v.contains(ValidTimeMicros(20))); // exclusive end
    }

    #[test]
    fn open_ended_interval_contains_everything_after_its_start() {
        let v = ValidInterval::new(ValidTimeMicros(100), VALID_TIME_OPEN).unwrap();
        assert!(!v.contains(ValidTimeMicros(99)));
        assert!(v.contains(ValidTimeMicros(100)));
        assert!(v.contains(ValidTimeMicros(i64::MAX - 1)));
    }

    #[test]
    fn frame_then_unframe_round_trips_on_a_valid_time_table() {
        let framed = frame_payload(true, Some(iv(7, 42)), Some(b"salary=100".to_vec()))
            .unwrap()
            .expect("valid-time framing always yields a present payload");
        assert_eq!(framed.len(), VALID_TIME_PREFIX_LEN + b"salary=100".len());

        let (interval, user) = unframe_payload(true, &framed).unwrap();
        assert_eq!(interval, Some(iv(7, 42)));
        assert_eq!(user, b"salary=100");
    }

    #[test]
    fn system_only_table_passes_payload_through_untouched() {
        let framed = frame_payload(false, None, Some(b"row".to_vec())).unwrap();
        assert_eq!(framed, Some(b"row".to_vec()));
        let (interval, user) = unframe_payload(false, framed.as_deref().unwrap()).unwrap();
        assert_eq!(interval, None);
        assert_eq!(user, b"row");
    }

    #[test]
    fn system_only_table_carries_a_null_payload_through() {
        // A `None` (SQL NULL) user payload passes through unchanged on a
        // system-only table — never collapsing into empty bytes ([STL-154]).
        assert_eq!(frame_payload(false, None, None).unwrap(), None);
    }

    #[test]
    fn policy_mismatches_are_rejected_both_ways() {
        // Valid-time table, no interval supplied.
        assert!(matches!(
            frame_payload(true, None, Some(b"x".to_vec())),
            Err(ValidTimeError::ValidTimeRequired)
        ));
        // System-only table, interval supplied.
        assert!(matches!(
            frame_payload(false, Some(iv(1, 2)), Some(b"x".to_vec())),
            Err(ValidTimeError::ValidTimeNotSupported)
        ));
    }

    #[test]
    fn unframe_rejects_a_truncated_valid_time_payload() {
        let short = vec![0u8; VALID_TIME_PREFIX_LEN - 1];
        assert!(matches!(
            unframe_payload(true, &short),
            Err(ValidTimeError::Truncated)
        ));
    }

    #[test]
    fn reframe_is_the_byte_inverse_of_the_segment_strip() {
        // A valid-time segment stores the bare payload + the interval columns;
        // `reframe_payload` rebuilds exactly the framed bytes `frame_payload`
        // produced, so a reconstructed Version round-trips byte-for-byte.
        let framed = frame_payload(true, Some(iv(7, 42)), Some(b"salary=100".to_vec()))
            .unwrap()
            .expect("valid-time framing always yields a present payload");
        let (interval, user) = unframe_payload(true, &framed).unwrap();
        let interval = interval.unwrap();
        let reframed = reframe_payload(interval.from.0, interval.to.0, user);
        assert_eq!(reframed, framed, "reframe rebuilds the exact stored bytes");
    }

    #[test]
    fn reframe_handles_an_empty_user_payload() {
        let reframed = reframe_payload(1, 2, b"");
        assert_eq!(reframed.len(), VALID_TIME_PREFIX_LEN);
        let (interval, user) = unframe_payload(true, &reframed).unwrap();
        assert_eq!(interval, Some(iv(1, 2)));
        assert!(user.is_empty());
    }

    #[test]
    fn empty_user_payload_is_legal_with_a_prefix() {
        let framed = frame_payload(true, Some(iv(1, 2)), Some(Vec::new()))
            .unwrap()
            .expect("valid-time framing always yields a present payload");
        assert_eq!(framed.len(), VALID_TIME_PREFIX_LEN);
        let (interval, user) = unframe_payload(true, &framed).unwrap();
        assert_eq!(interval, Some(iv(1, 2)));
        assert!(user.is_empty());
    }

    // --- ValidIntervalSummary (STL-241) ------------------------------------

    fn summary(pairs: &[(i64, i64)], cap: usize) -> ValidIntervalSummary {
        ValidIntervalSummary::build(pairs.iter().copied(), cap)
    }

    #[test]
    fn summary_coalesces_overlapping_and_touching_windows() {
        // [0,10) and [10,20) touch; [5,15) overlaps both. The union is one
        // window [0,20). [100,110) is disjoint and stays separate.
        let s = summary(&[(0, 10), (10, 20), (5, 15), (100, 110)], 256);
        // Every original point is still covered; the gap between is not.
        for p in [0, 9, 10, 19, 100, 109] {
            assert!(s.covers(p), "point {p} must stay covered after coalescing");
        }
        assert!(!s.covers(20), "exclusive end of the merged window");
        assert!(!s.covers(50), "the gap between disjoint windows prunes");
        assert!(!s.covers(110), "exclusive end of the second window");
    }

    #[test]
    fn summary_covers_is_half_open_and_empty_outside() {
        let s = summary(&[(10, 20)], 256);
        assert!(!s.covers(9));
        assert!(s.covers(10)); // inclusive start
        assert!(s.covers(19));
        assert!(!s.covers(20)); // exclusive end
    }

    #[test]
    fn summary_drops_empty_pairs_and_an_all_empty_input_covers_nothing() {
        // `from >= to` pairs cover no point, so they never widen the union.
        let s = summary(&[(5, 5), (9, 3)], 256);
        assert!(s.is_empty());
        assert!(!s.covers(4));
        assert!(!s.covers(5));
    }

    #[test]
    fn the_scatter_case_a_gap_inside_the_minmax_envelope_prunes() {
        // The load-bearing STL-241 case the zone-map min/max cannot prune: a
        // backdated window [0,10) and a current open window [100, +∞) share a
        // segment, so the envelope is [0, +∞) and spans every probe — yet the
        // summary proves the gap at 50 holds no row.
        let s = summary(&[(0, 10), (100, i64::MAX)], 256);
        assert!(s.covers(5));
        assert!(
            !s.covers(50),
            "a point in the coverage gap is provably empty"
        );
        assert!(s.covers(100));
        assert!(s.covers(i64::MAX - 1), "open-ended window covers up to +∞");
    }

    #[test]
    fn capping_merges_the_smallest_gaps_and_never_drops_a_covered_point() {
        // Five disjoint windows, each a single point wide, with a small gap
        // before the last pair. Capping to 2 must keep covering every original
        // point (the superset contract) while collapsing to 2 intervals.
        let windows = [(0, 1), (10, 11), (20, 21), (30, 31), (31, 32)];
        let capped = summary(&windows, 2);
        let full = summary(&windows, 256);
        assert!(
            capped.encoded_len() <= full.encoded_len(),
            "capping must not grow the summary"
        );
        // Soundness: every point the full (exact) summary covers, the capped one
        // still covers — capping only ever *adds* phantom coverage.
        for p in -5..=40 {
            if full.covers(p) {
                assert!(capped.covers(p), "capping dropped covered point {p}");
            }
        }
    }

    #[test]
    fn summary_encode_decode_round_trips_and_reports_consumed_len() {
        let s = summary(&[(0, 10), (100, i64::MAX)], 256);
        let mut buf = Vec::new();
        s.encode(&mut buf);
        assert_eq!(buf.len(), s.encoded_len());
        // A trailing byte proves `decode` reports the exact consumed length.
        buf.push(0xAB);
        let (decoded, consumed) = ValidIntervalSummary::decode(&buf).expect("round-trips");
        assert_eq!(consumed, s.encoded_len());
        assert_eq!(decoded, s);
    }

    #[test]
    fn summary_decode_rejects_malformed() {
        assert!(ValidIntervalSummary::decode(&[]).is_err(), "empty buffer");
        // Zero count — the writer never persists an empty summary.
        assert!(ValidIntervalSummary::decode(&[0, 0, 0, 0]).is_err());
        // Count claims one interval the buffer does not hold.
        assert!(ValidIntervalSummary::decode(&[1, 0, 0, 0]).is_err());
        // A well-formed length carrying an inverted interval (to <= from).
        let mut bad = 1u32.to_le_bytes().to_vec();
        bad.extend_from_slice(&20i64.to_le_bytes());
        bad.extend_from_slice(&10i64.to_le_bytes());
        assert!(ValidIntervalSummary::decode(&bad).is_err());
    }
}
