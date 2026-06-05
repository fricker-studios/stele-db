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
//! schema, so the bytes need carry no tag. The segment writer lifts this prefix
//! into first-class `valid_from` / `valid_to` columns at flush for zone-map
//! pruning ([STL-117]) — see
//! [`SegmentWriter::create_valid_time`](crate::segment::SegmentWriter::create_valid_time);
//! the [`crate::segment`] zone maps then prune the valid axis generically.
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
//! w.insert(&mut delta, key.clone(), Some(v), b"salary=100".to_vec())?;
//! ```

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};

use crate::delta::{BusinessKey, Delta};
use crate::systime::{SysTimeError, SysTimeWriter};
use crate::wal::Disk;

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
/// # Errors
///
/// The two policy-mismatch variants above.
pub fn frame_payload(
    enabled: bool,
    valid: Option<ValidInterval>,
    user_payload: Vec<u8>,
) -> Result<Vec<u8>, ValidTimeError> {
    match (enabled, valid) {
        (true, Some(interval)) => {
            let mut out = Vec::with_capacity(VALID_TIME_PREFIX_LEN + user_payload.len());
            interval.encode_prefix(&mut out);
            out.extend_from_slice(&user_payload);
            Ok(out)
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

/// Stamps both temporal axes as writes flow into the delta tier: system-time
/// via an inner [`SysTimeWriter`], valid-time via the per-table policy enforced
/// here.
///
/// One writer is bound to one table's valid-time setting (`enabled`), which
/// mirrors `stele_catalog::TableTemporal::valid_time_enabled` for that table.
/// The DML layer ([STL-94]) constructs the writer from the catalog flag; this
/// crate stays free of a catalog dependency by taking the resolved policy as a
/// `bool`.
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
    pub fn insert<D: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, ValidTimeError> {
        let framed = frame_payload(self.valid_time, valid, payload)?;
        Ok(self.inner.insert(delta, key, framed, txn_id, principal)?)
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
    pub fn update<D: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        key: BusinessKey,
        valid: Option<ValidInterval>,
        payload: Vec<u8>,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, ValidTimeError> {
        let framed = frame_payload(self.valid_time, valid, payload)?;
        Ok(self.inner.update(delta, key, framed, txn_id, principal)?)
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
    pub fn delete<D: Disk>(
        &mut self,
        delta: &mut Delta<D>,
        key: &BusinessKey,
        txn_id: TxnId,
        principal: Principal,
    ) -> Result<SystemTimeMicros, ValidTimeError> {
        Ok(self.inner.delete(delta, key, txn_id, principal)?)
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
        let framed = frame_payload(true, Some(iv(7, 42)), b"salary=100".to_vec()).unwrap();
        assert_eq!(framed.len(), VALID_TIME_PREFIX_LEN + b"salary=100".len());

        let (interval, user) = unframe_payload(true, &framed).unwrap();
        assert_eq!(interval, Some(iv(7, 42)));
        assert_eq!(user, b"salary=100");
    }

    #[test]
    fn system_only_table_passes_payload_through_untouched() {
        let framed = frame_payload(false, None, b"row".to_vec()).unwrap();
        assert_eq!(framed, b"row");
        let (interval, user) = unframe_payload(false, &framed).unwrap();
        assert_eq!(interval, None);
        assert_eq!(user, b"row");
    }

    #[test]
    fn policy_mismatches_are_rejected_both_ways() {
        // Valid-time table, no interval supplied.
        assert!(matches!(
            frame_payload(true, None, b"x".to_vec()),
            Err(ValidTimeError::ValidTimeRequired)
        ));
        // System-only table, interval supplied.
        assert!(matches!(
            frame_payload(false, Some(iv(1, 2)), b"x".to_vec()),
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
    fn empty_user_payload_is_legal_with_a_prefix() {
        let framed = frame_payload(true, Some(iv(1, 2)), Vec::new()).unwrap();
        assert_eq!(framed.len(), VALID_TIME_PREFIX_LEN);
        let (interval, user) = unframe_payload(true, &framed).unwrap();
        assert_eq!(interval, Some(iv(1, 2)));
        assert!(user.is_empty());
    }
}
