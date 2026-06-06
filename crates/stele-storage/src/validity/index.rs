//! [`ValidityIndex`] and its entry types. See [the module docs](super) for the
//! design rationale.

use std::collections::BTreeMap;
use std::io;

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;

use crate::backend::Disk;
use crate::delta::BusinessKey;

use super::spill;

/// The materialized **end** of one version's system-time period: the value half
/// of a validity-index entry.
///
/// `sys_to` is the system-time at which the version was superseded or deleted —
/// the period is `[sys_from, sys_to)`. `closed_by` is the provenance of the
/// transaction that performed the close: for a delete there is no successor
/// version, so this is the only record of who closed the period ([STL-118]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedInterval {
    /// The system-time at which the version was superseded/deleted (exclusive
    /// end of `[sys_from, sys_to)`).
    pub sys_to: SystemTimeMicros,
    /// Provenance of the transaction that closed the period.
    pub closed_by: Provenance,
}

/// An appended **close** record: the validity-index entry, and the unit the WAL
/// logs so the index is rebuildable ([the "appended close" of STL-127, realigned
/// under ADR-0023](super)).
///
/// It names the version it closes by `(business_key, sys_from, seq)` — the same
/// key the delta tier and sealed segments cluster a version chain by once `seq`
/// is load-bearing ([ADR-0024], STL-141 Part B) — and supplies the materialized
/// `sys_to` plus the closing transaction's provenance. The closed version's body
/// (payload, birth provenance) is never touched: a close is bookkeeping by the
/// superseding/deleting transaction, not a rewrite of who wrote the closed
/// version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    /// The business key of the version being closed.
    pub business_key: BusinessKey,
    /// The `sys_from` of the version this close refers to — part of the match key.
    pub sys_from: SystemTimeMicros,
    /// The `seq` of the version this close refers to — the per-commit tiebreak
    /// that completes the match key ([ADR-0024]). Two versions of one key can
    /// share a `sys_from` once the writer no longer force-bumps the timestamp
    /// (STL-145), so `(sys_from, seq)` is what uniquely names the closed version;
    /// keying on `sys_from` alone would let one close collide with and overwrite
    /// the other.
    pub seq: u64,
    /// The materialized end stamped on the closed period; the interval becomes
    /// `[sys_from, sys_to)`.
    pub sys_to: SystemTimeMicros,
    /// Provenance of the transaction that performed the close.
    pub closed_by: Provenance,
}

/// Fixed header size for the [`Close`] binary frame: `business_len`/`principal_len`
/// `u32` (8) + `sys_from`/`sys_to`/`closed_at` `i64` (24) + `seq`/`closed_txn`
/// `u64` (16).
const HEADER_LEN: usize = 48;

/// Per-frame ceiling for an encoded [`Close`] (16 MiB) — the same bound the
/// delta tier and the WAL apply, so a close frame can never legitimately exceed
/// what the WAL will accept.
pub const MAX_CLOSE_FRAME_LEN: usize = 16 * 1024 * 1024;

impl Close {
    /// The `(business_key, sys_from, seq)` this close materializes an end for.
    fn target(&self) -> (BusinessKey, SystemTimeMicros, u64) {
        (self.business_key.clone(), self.sys_from, self.seq)
    }

    /// The value half of the entry.
    fn interval(&self) -> ClosedInterval {
        ClosedInterval {
            sys_to: self.sys_to,
            closed_by: self.closed_by.clone(),
        }
    }

    /// Bytes this close contributes to the in-memory / on-spill byte accounting —
    /// its encoded size.
    #[must_use]
    pub fn encoded_size(&self) -> usize {
        HEADER_LEN + self.business_key.as_bytes().len() + self.closed_by.principal.0.len()
    }

    /// Verify the close can be encoded — component sizes fit `u32` and the frame
    /// stays under [`MAX_CLOSE_FRAME_LEN`].
    fn check_encodable(&self) -> Result<(), ValidityError> {
        if u32::try_from(self.business_key.as_bytes().len()).is_err()
            || u32::try_from(self.closed_by.principal.0.len()).is_err()
            || self.encoded_size() > MAX_CLOSE_FRAME_LEN
        {
            return Err(ValidityError::TooLarge(self.encoded_size()));
        }
        Ok(())
    }

    /// Encode into `out`, appending bytes. Shared by the WAL committer and the
    /// spill writer so both paths use one wire format. Layout (little-endian):
    ///
    /// ```text
    /// | business_len:u32 | principal_len:u32 | sys_from:i64 | seq:u64 | sys_to:i64 |
    /// | closed_txn:u64 | closed_at:i64 | business_key bytes … | principal bytes … |
    /// ```
    ///
    /// `seq` sits next to `sys_from`, the timestamp it disambiguates — the closed
    /// version's per-commit tiebreak ([ADR-0024], STL-145).
    ///
    /// # Errors
    ///
    /// [`ValidityError::TooLarge`] when the frame would exceed [`MAX_CLOSE_FRAME_LEN`].
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ValidityError> {
        self.check_encodable()?;
        let business_len = u32::try_from(self.business_key.as_bytes().len())
            .map_err(|_| ValidityError::TooLarge(self.encoded_size()))?;
        let principal_len = u32::try_from(self.closed_by.principal.0.len())
            .map_err(|_| ValidityError::TooLarge(self.encoded_size()))?;
        out.reserve(self.encoded_size());
        out.extend_from_slice(&business_len.to_le_bytes());
        out.extend_from_slice(&principal_len.to_le_bytes());
        out.extend_from_slice(&self.sys_from.0.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.sys_to.0.to_le_bytes());
        out.extend_from_slice(&self.closed_by.txn_id.0.to_le_bytes());
        out.extend_from_slice(&self.closed_by.committed_at.0.to_le_bytes());
        out.extend_from_slice(self.business_key.as_bytes());
        out.extend_from_slice(self.closed_by.principal.as_bytes());
        Ok(())
    }

    /// Convenience: encode to a fresh `Vec<u8>`.
    ///
    /// # Errors
    ///
    /// Forwards [`ValidityError::TooLarge`] from [`Self::encode`].
    pub fn encoded(&self) -> Result<Vec<u8>, ValidityError> {
        let mut v = Vec::with_capacity(self.encoded_size());
        self.encode(&mut v)?;
        Ok(v)
    }

    /// Decode from the head of `bytes`. Returns the parsed [`Close`] and the
    /// number of bytes consumed, so a caller reading concatenated frames (spill
    /// reload) can drive a cursor.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] when the frame's declared lengths do not match
    /// the bytes available or exceed the per-frame ceiling.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), ValidityError> {
        if bytes.len() < HEADER_LEN {
            return Err(ValidityError::Corrupt("short read on close header"));
        }
        let rd_u32 = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().expect("4 bytes"));
        let rd_i64 = |o: usize| i64::from_le_bytes(bytes[o..o + 8].try_into().expect("8 bytes"));
        let rd_u64 = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().expect("8 bytes"));
        let business_len = rd_u32(0) as usize;
        let principal_len = rd_u32(4) as usize;
        let sys_from = rd_i64(8);
        let seq = rd_u64(16);
        let sys_to = rd_i64(24);
        let closed_txn = rd_u64(32);
        let closed_at = rd_i64(40);
        let total = HEADER_LEN
            .checked_add(business_len)
            .and_then(|v| v.checked_add(principal_len))
            .ok_or(ValidityError::Corrupt("frame length overflows usize"))?;
        if total > MAX_CLOSE_FRAME_LEN {
            return Err(ValidityError::Corrupt(
                "frame length exceeds MAX_CLOSE_FRAME_LEN",
            ));
        }
        if bytes.len() < total {
            return Err(ValidityError::Corrupt("frame body shorter than declared"));
        }
        let bk_end = HEADER_LEN + business_len;
        let business_key = BusinessKey::new(bytes[HEADER_LEN..bk_end].to_vec());
        let principal = Principal::new(bytes[bk_end..total].to_vec());
        Ok((
            Self {
                business_key,
                sys_from: SystemTimeMicros(sys_from),
                seq,
                sys_to: SystemTimeMicros(sys_to),
                closed_by: Provenance {
                    txn_id: TxnId(closed_txn),
                    committed_at: SystemTimeMicros(closed_at),
                    principal,
                },
            },
            total,
        ))
    }
}

/// A per-segment **upper bound** on the system-time *ends* of a set of versions,
/// derived from the [`ValidityIndex`] ([`ValidityIndex::sys_upper_bound`]).
///
/// It answers the question a sealed segment's zone map cannot — the segment
/// stores only `sys_from`, never `sys_to` (v6, [ADR-0023]) — namely
/// *whether every row in a segment was already superseded at a snapshot*. That
/// restores the upper-bound ("all rows already superseded") half of the
/// system-time segment prune the zone map lost when `sys_to` left the record
/// ([STL-139], the complement of `min(sys_from) > snapshot`).
///
/// The bound is **conservative on the open side**: a version with no
/// materialized close is still *open* (its period end is `+∞`), so any open
/// version anywhere in the set forces [`SysUpperBound::Unbounded`] and the set
/// can never be pruned on this axis. Only when *every* version is closed is the
/// bound the greatest materialized `sys_to` — the exact figure a stored
/// `max(sys_to)` zone would once have given, now read from the derived index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SysUpperBound {
    /// Every version in the set is closed; this is the greatest materialized
    /// `sys_to` over them. A snapshot at or after it sees none of them. An empty
    /// set is vacuously all-superseded and reports `Bounded(SystemTimeMicros::MIN)`,
    /// so it prunes at every snapshot.
    Bounded(SystemTimeMicros),
    /// At least one version is still open (no materialized close), so the set's
    /// period end is `+∞` — it can never be pruned on the upper bound.
    Unbounded,
}

impl SysUpperBound {
    /// Whether every version this bound summarizes was superseded **at or before**
    /// `snapshot` — i.e. none can be visible at `snapshot` on the upper-bound side.
    ///
    /// `true` is the planner's licence to skip the segment: a closed period
    /// `[sys_from, sys_to)` is invisible at `snapshot` once `sys_to <= snapshot`
    /// (the end is exclusive), so a bound `max(sys_to) <= snapshot` proves no row
    /// can be visible. [`SysUpperBound::Unbounded`] (an open version present) is
    /// never superseded — it returns `false`, so the segment is conservatively
    /// kept (never a false negative).
    #[must_use]
    pub fn superseded_at_or_before(self, snapshot: SystemTimeMicros) -> bool {
        match self {
            Self::Bounded(max_sys_to) => max_sys_to <= snapshot,
            Self::Unbounded => false,
        }
    }
}

/// Tuning knobs for the validity index.
#[derive(Debug, Clone, Copy)]
pub struct ValidityConfig {
    /// Freeze the in-memory entries to a spill file once their encoded byte
    /// count would exceed this value — mirrors [`crate::delta::DeltaConfig`].
    pub spill_threshold_bytes: u64,
}

impl Default for ValidityConfig {
    fn default() -> Self {
        // Match the delta tier's default so "a comfortable chunk of recent
        // activity" means the same thing on both structures.
        Self {
            spill_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Errors surfaced from the validity index.
#[derive(Debug, thiserror::Error)]
pub enum ValidityError {
    /// A conflicting close for an already-closed `(business_key, sys_from)`: the
    /// version's end was materialized once and cannot be re-materialized to a
    /// *different* value. This is the per-key serialization point — a second,
    /// racing supersession of the same version loses and must retry ([ADR-0023]).
    #[error("version (key, sys_from) is already closed with a different sys_to")]
    AlreadyClosed,

    /// A [`Close`] frame on disk failed to decode — a torn write on the spill
    /// path or a stale on-disk file a caller failed to discard via [`super::ValidityIndex::open`].
    #[error("validity-index on-disk frame corrupt: {0}")]
    Corrupt(&'static str),

    /// A [`Close`]'s encoded size exceeded the per-frame ceiling
    /// ([`MAX_CLOSE_FRAME_LEN`]).
    #[error("close frame too large: {0} bytes (max 16 MiB)")]
    TooLarge(usize),

    /// An I/O failure on the spill path.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

/// The validity-index handle: in-memory entries with local-disk spill, owning
/// one [`Disk`] handle for its spill files (a namespace distinct from the WAL
/// and the delta tier).
///
/// `ValidityIndex` is a **peer** of the [`Delta`](crate::delta::Delta) tier,
/// not a part of it: the write path stages a version into the delta *and*
/// materializes its predecessor's close into this index, and the read path folds
/// the two together ([`crate::merge`]). Like the delta, it makes **no durability
/// claim** — on crash, [`Self::open`] discards stale spills and the caller drives
/// WAL replay back through [`Self::insert_close`].
pub struct ValidityIndex<D: Disk> {
    disk: D,
    config: ValidityConfig,
    /// `(business_key, sys_from, seq) → materialized end`. Resident entries; the
    /// spilled ones live in `live_spills`. The `seq` is part of the key so two
    /// same-`sys_from` versions of one key each get their own close instead of
    /// colliding (STL-145, [ADR-0024]).
    mem: BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval>,
    /// Running sum of [`Close::encoded_size`] for the resident entries.
    byte_size: u64,
    /// Next spill index to allocate — `max(existing) + 1` at open even though the
    /// existing files are discarded, so a delayed remove can't clash a new write.
    next_spill_index: u64,
    /// Spills written this lifetime, ascending by index. Each carries an
    /// in-memory key-range + bloom summary ([`spill::SpillMeta`]) so a point
    /// lookup reads only the spills that may hold the key ([STL-142]).
    live_spills: Vec<spill::SpillMeta>,
}

impl<D: Disk> ValidityIndex<D> {
    /// Open the validity index backed by `disk`. Any spill files left by a prior
    /// (crashed) process are **discarded** — the WAL is the canonical truth, and
    /// re-loading a stale spill would disagree with what replay reconstructs.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Io`] if the backing disk cannot be listed/cleared.
    pub fn open(disk: D, config: ValidityConfig) -> Result<Self, ValidityError> {
        let existing = spill::list_spills(&disk)?;
        let next_spill_index = existing.last().map_or(0, |&i| i + 1);
        spill::discard_stale_spills(&disk)?;
        Ok(Self {
            disk,
            config,
            mem: BTreeMap::new(),
            byte_size: 0,
            next_spill_index,
            live_spills: Vec::new(),
        })
    }

    /// Materialize a version's end into the index — **write-once**.
    ///
    /// Re-applying the *identical* close (same `sys_to` and `closed_by`) is
    /// idempotent and returns `Ok` — the property WAL replay relies on. A
    /// *conflicting* close for an already-closed `(business_key, sys_from)` is
    /// refused with [`ValidityError::AlreadyClosed`]: the per-key serialization
    /// point ([ADR-0023]).
    ///
    /// # Errors
    ///
    /// [`ValidityError::AlreadyClosed`] on a conflicting re-close;
    /// [`ValidityError::TooLarge`] / [`ValidityError::Corrupt`] / [`ValidityError::Io`]
    /// from the encode/spill path.
    pub fn insert_close(&mut self, close: Close) -> Result<(), ValidityError> {
        close.check_encodable()?;
        if let Some(existing) = self.close_of(&close.business_key, close.sys_from, close.seq)? {
            return if existing == close.interval() {
                Ok(()) // idempotent replay
            } else {
                Err(ValidityError::AlreadyClosed)
            };
        }
        let incoming = close.encoded_size() as u64;
        let projected = self.byte_size.saturating_add(incoming);
        if projected > self.config.spill_threshold_bytes && !self.mem.is_empty() {
            self.spill_in_memory()?;
        }
        self.byte_size = self.byte_size.saturating_add(incoming);
        // Consume `close` by moving its parts into the entry — no clone, and the
        // owned argument is genuinely used up (not merely borrowed).
        let Close {
            business_key,
            sys_from,
            seq,
            sys_to,
            closed_by,
        } = close;
        self.mem.insert(
            (business_key, sys_from, seq),
            ClosedInterval { sys_to, closed_by },
        );
        Ok(())
    }

    /// The materialized end of the version `(key, sys_from, seq)`, or `None` while
    /// the version is still open (no close has committed).
    ///
    /// `seq` is the per-commit tiebreak that, with `sys_from`, uniquely names the
    /// version ([ADR-0024], STL-145): two versions of one key can share a
    /// `sys_from`, so the lookup must match on `seq` too or it could return the
    /// wrong version's end.
    ///
    /// Checks the resident entries first, then only the spills whose in-memory
    /// summary says they *may* hold `key` — a point lookup reads `O(matching
    /// spills)`, not every spill ([STL-142]). Closes are tiny and usually
    /// resident, so spilling stays rare to begin with ([the module docs](super)).
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn close_of(
        &self,
        key: &BusinessKey,
        sys_from: SystemTimeMicros,
        seq: u64,
    ) -> Result<Option<ClosedInterval>, ValidityError> {
        if let Some(interval) = self.mem.get(&(key.clone(), sys_from, seq)) {
            return Ok(Some(interval.clone()));
        }
        for meta in &self.live_spills {
            if !meta.may_contain(key) {
                continue;
            }
            for close in spill::read_spill(&self.disk, meta.index)? {
                if &close.business_key == key && close.sys_from == sys_from && close.seq == seq {
                    return Ok(Some(close.interval()));
                }
            }
        }
        Ok(None)
    }

    /// Every materialized close for `key`, keyed by the version's
    /// `(sys_from, seq)`.
    ///
    /// The read path ([`crate::merge`]) overlays these onto the key's candidate
    /// versions to stamp each one's `sys_to` / `closed_by` before resolving a
    /// snapshot. Keying on `(sys_from, seq)` keeps two same-`sys_from` versions'
    /// closes distinct (STL-145). Merges the resident entries with every spill.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn closes_for(
        &self,
        key: &BusinessKey,
    ) -> Result<BTreeMap<(SystemTimeMicros, u64), ClosedInterval>, ValidityError> {
        use std::ops::Bound::Included;
        let mut out: BTreeMap<(SystemTimeMicros, u64), ClosedInterval> = BTreeMap::new();
        // Spilled entries first, resident last — a resident entry supersedes a
        // spilled one for the same version (they never disagree, but this keeps
        // the merge order well-defined, mirroring the delta tier). Only spills
        // whose summary may hold `key` are read ([STL-142]).
        for meta in &self.live_spills {
            if !meta.may_contain(key) {
                continue;
            }
            for close in spill::read_spill(&self.disk, meta.index)? {
                if &close.business_key == key {
                    out.insert((close.sys_from, close.seq), close.interval());
                }
            }
        }
        let lo = (key.clone(), SystemTimeMicros(i64::MIN), u64::MIN);
        let hi = (key.clone(), SystemTimeMicros(i64::MAX), u64::MAX);
        for ((_, sys_from, seq), interval) in self.mem.range((Included(lo), Included(hi))) {
            out.insert((*sys_from, *seq), interval.clone());
        }
        Ok(out)
    }

    /// Materialized closes for a *set* of keys, reading the fewest spills.
    ///
    /// The read-path fold ([`crate::merge::fold_chains`]) needs every close for a
    /// handful of keys. Two strategies cost differently once the index has
    /// spilled: probing each key against the in-memory spill summaries and
    /// reading only the spills that may hold one of them, versus one full
    /// [`Self::materialize`] sweep that reads every spill once. This picks the
    /// cheaper by counting — it reads the **union** of spills that may hold a
    /// requested key, but only when that union is *smaller* than the full spill
    /// set; otherwise it falls back to the single sweep ([STL-142]).
    ///
    /// The returned map is keyed `(business_key, sys_from, seq)` like
    /// [`Self::materialize`]. In the full-sweep branch it contains every entry in
    /// the index, not just those for `keys`; the fold overlays by key, so the
    /// extras are simply ignored.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn closes_for_keys(
        &self,
        keys: &std::collections::BTreeSet<BusinessKey>,
    ) -> Result<BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval>, ValidityError> {
        use std::ops::Bound::Included;
        // Spills that may hold at least one requested key — each read at most once.
        // Built incrementally so that the instant every spill is selected (a full
        // sweep is then at least as cheap) we bail to `materialize` without
        // finishing the scan or setting up any per-key spill read.
        let mut selected: Vec<&spill::SpillMeta> = Vec::new();
        for meta in &self.live_spills {
            if keys.iter().any(|k| meta.may_contain(k)) {
                selected.push(meta);
                if selected.len() == self.live_spills.len() {
                    return self.materialize();
                }
            }
        }
        let mut out: BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval> =
            BTreeMap::new();
        for meta in selected {
            for close in spill::read_spill(&self.disk, meta.index)? {
                if keys.contains(&close.business_key) {
                    out.insert(close.target(), close.interval());
                }
            }
        }
        // Resident entries for each key — a contiguous run in the
        // `(key, sys_from, seq)` map, so range-scan just that run rather than the
        // whole map.
        for key in keys {
            let lo = (key.clone(), SystemTimeMicros(i64::MIN), u64::MIN);
            let hi = (key.clone(), SystemTimeMicros(i64::MAX), u64::MAX);
            for ((_, sys_from, seq), interval) in self.mem.range((Included(lo), Included(hi))) {
                out.insert((key.clone(), *sys_from, *seq), interval.clone());
            }
        }
        Ok(out)
    }

    /// Number of live spill files — the read path uses it to choose between a
    /// per-key probe and a full [`Self::materialize`] sweep.
    #[must_use]
    pub fn spill_count(&self) -> usize {
        self.live_spills.len()
    }

    /// The `(sys_from, seq)` of the version active at system-time `at` for `key`,
    /// when `at` falls inside a **materialized** (closed) interval — a direct
    /// range-containment lookup, no version-chain walk
    /// ([DoD of STL-133](super)).
    ///
    /// Returns the greatest `(sys_from, seq)` with `sys_from ≤ at` whose
    /// materialized `sys_to > at`. The `seq` component breaks a `sys_from` tie so
    /// the highest-`seq` version at a shared tick wins ([ADR-0024], STL-145); a
    /// same-tick superseded version closes degenerately (`sys_to == sys_from`) and
    /// so is filtered out by the `sys_to > at` test, never returned.
    /// `None` means `at` is not covered by any closed interval for the key: it
    /// is either before the key's first version or in its currently-*open* tail,
    /// which the read path resolves against the version set ([`crate::merge`]) —
    /// the index alone does not track the open head, which is not write-once.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn active_at(
        &self,
        key: &BusinessKey,
        at: SystemTimeMicros,
    ) -> Result<Option<(SystemTimeMicros, u64)>, ValidityError> {
        let closes = self.closes_for(key)?;
        Ok(closes
            .range(..=(at, u64::MAX))
            .next_back()
            .filter(|(_, interval)| interval.sys_to > at)
            .map(|(&key, _)| key))
    }

    /// The system-time **upper bound** over a set of versions named by
    /// `(business_key, sys_from)` — the greatest materialized `sys_to`, or
    /// [`SysUpperBound::Unbounded`] if any of them is still open.
    ///
    /// This is the validity index's half of the restored "all rows already
    /// superseded" segment prune ([STL-139]): a planner hands it a sealed
    /// segment's version identities (e.g. [`SegmentReader::version_keys`]) and,
    /// when the result [`SysUpperBound::superseded_at_or_before`] the read
    /// snapshot, skips the segment without materializing its bulk column chunks.
    /// Soundness rests on the open-side conservatism — a version absent from the
    /// index is *open*, so a single open version forces `Unbounded` and the
    /// segment is kept (never a false negative).
    ///
    /// Short-circuits to `Unbounded` on the first open version. The hot path —
    /// resident entries, no spills — is **allocation-free**: each version's key
    /// is *moved* into the `BTreeMap` probe tuple rather than cloned (unlike
    /// [`Self::close_of`], whose borrowed signature forces a clone). Only the rare
    /// resident-miss-with-spills case falls back to [`Self::close_of`]'s
    /// per-lookup spill scan. Takes owned `(BusinessKey, SystemTimeMicros, u64)`
    /// items — the version's `(key, sys_from, seq)` identity (STL-145) — for
    /// exactly this reason; feed it [`SegmentReader::version_keys`] directly.
    ///
    /// [`SegmentReader::version_keys`]: crate::segment::SegmentReader::version_keys
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn sys_upper_bound<I>(&self, versions: I) -> Result<SysUpperBound, ValidityError>
    where
        I: IntoIterator<Item = (BusinessKey, SystemTimeMicros, u64)>,
    {
        let mut max_sys_to = SystemTimeMicros(i64::MIN);
        for (key, sys_from, seq) in versions {
            // Move the key into the lookup tuple — the resident probe needs no
            // clone. Recover it from the tuple only when a resident miss with
            // live spills forces a spill scan.
            let probe = (key, sys_from, seq);
            if let Some(interval) = self.mem.get(&probe) {
                max_sys_to = max_sys_to.max(interval.sys_to);
                continue;
            }
            if self.live_spills.is_empty() {
                // Not resident and nothing spilled ⇒ the version is open ⇒ +∞.
                return Ok(SysUpperBound::Unbounded);
            }
            let (key, sys_from, seq) = probe;
            match self.close_of(&key, sys_from, seq)? {
                Some(interval) => max_sys_to = max_sys_to.max(interval.sys_to),
                // An open version's end is +∞: the set can never be pruned.
                None => return Ok(SysUpperBound::Unbounded),
            }
        }
        Ok(SysUpperBound::Bounded(max_sys_to))
    }

    /// The full set of entries, resident + spilled, as one map. The canonical
    /// form for comparing two indexes for *exact* equality — the property the
    /// "rebuild-from-WAL reproduces the exact index" seed sweep asserts
    /// ([DoD of STL-133](super)).
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn materialize(
        &self,
    ) -> Result<BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval>, ValidityError> {
        let mut out: BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval> =
            BTreeMap::new();
        for meta in &self.live_spills {
            for close in spill::read_spill(&self.disk, meta.index)? {
                out.insert(close.target(), close.interval());
            }
        }
        for (k, v) in &self.mem {
            out.insert(k.clone(), v.clone());
        }
        Ok(out)
    }

    /// Total number of materialized entries, resident + spilled.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn len(&self) -> Result<usize, ValidityError> {
        Ok(self.materialize()?.len())
    }

    /// Whether the index holds any entry.
    ///
    /// # Errors
    ///
    /// [`ValidityError::Corrupt`] / [`ValidityError::Io`] loading a spill file.
    pub fn is_empty(&self) -> Result<bool, ValidityError> {
        Ok(self.mem.is_empty() && self.live_spills.is_empty())
    }

    /// Resident (un-spilled) byte count — the figure the spill threshold is
    /// taken against.
    #[must_use]
    pub const fn byte_size(&self) -> u64 {
        self.byte_size
    }

    fn spill_in_memory(&mut self) -> Result<(), ValidityError> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let closes: Vec<Close> = self
            .mem
            .iter()
            .map(|((business_key, sys_from, seq), interval)| Close {
                business_key: business_key.clone(),
                sys_from: *sys_from,
                seq: *seq,
                sys_to: interval.sys_to,
                closed_by: interval.closed_by.clone(),
            })
            .collect();
        let idx = self.next_spill_index;
        self.next_spill_index += 1;
        let meta = spill::write_spill(&self.disk, idx, &closes)?;
        self.live_spills.push(meta);
        self.mem.clear();
        self.byte_size = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemDisk;

    fn close(key: &[u8], sys_from: i64, sys_to: i64, closer: &[u8]) -> Close {
        // `seq` defaults to 0 here — the same-`sys_from` tiebreak is exercised in
        // the bitemporal oracle; these unit tests use distinct `sys_from` values.
        close_seq(key, sys_from, 0, sys_to, closer)
    }

    fn close_seq(key: &[u8], sys_from: i64, seq: u64, sys_to: i64, closer: &[u8]) -> Close {
        Close {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            seq,
            sys_to: SystemTimeMicros(sys_to),
            closed_by: Provenance::new(
                TxnId(u64::try_from(sys_to).unwrap_or(0)),
                SystemTimeMicros(sys_to),
                Principal::new(closer.to_vec()),
            ),
        }
    }

    fn index() -> ValidityIndex<MemDisk> {
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open")
    }

    #[test]
    fn close_frame_round_trips() {
        let c = close(b"acct", 10, 20, b"deleter");
        let bytes = c.encoded().expect("encode");
        let (parsed, consumed) = Close::decode(&bytes).expect("decode");
        assert_eq!(parsed, c);
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed.encoded_size(), bytes.len());
    }

    #[test]
    fn empty_key_and_principal_frame_is_exactly_the_header() {
        let mut c = close(b"", 1, 2, b"");
        c.closed_by.principal = Principal::new(Vec::new());
        let bytes = c.encoded().expect("encode");
        assert_eq!(bytes.len(), HEADER_LEN);
        let (parsed, n) = Close::decode(&bytes).expect("decode");
        assert_eq!(parsed, c);
        assert_eq!(n, HEADER_LEN);
    }

    #[test]
    fn truncated_close_frame_is_corruption() {
        let bytes = close(b"k", 1, 2, b"x").encoded().expect("encode");
        let err = Close::decode(&bytes[..bytes.len() - 1]).unwrap_err();
        assert!(matches!(err, ValidityError::Corrupt(_)));
    }

    #[test]
    fn insert_and_lookup_round_trips() {
        let mut idx = index();
        idx.insert_close(close(b"k", 10, 20, b"a")).expect("insert");
        let got = idx
            .close_of(&BusinessKey::new(b"k".to_vec()), SystemTimeMicros(10), 0)
            .expect("lookup");
        assert_eq!(got.unwrap().sys_to, SystemTimeMicros(20));
        // A version with no close is open.
        assert!(
            idx.close_of(&BusinessKey::new(b"k".to_vec()), SystemTimeMicros(20), 0)
                .expect("lookup")
                .is_none()
        );
    }

    #[test]
    fn identical_reclose_is_idempotent() {
        let mut idx = index();
        idx.insert_close(close(b"k", 10, 20, b"a")).expect("first");
        idx.insert_close(close(b"k", 10, 20, b"a"))
            .expect("identical re-close is a no-op");
        assert_eq!(idx.len().expect("len"), 1);
    }

    #[test]
    fn conflicting_reclose_is_refused() {
        let mut idx = index();
        idx.insert_close(close(b"k", 10, 20, b"a")).expect("first");
        // Same target, different sys_to — the per-key serialization point.
        let err = idx.insert_close(close(b"k", 10, 21, b"a")).unwrap_err();
        assert!(matches!(err, ValidityError::AlreadyClosed));
    }

    #[test]
    fn active_at_is_range_containment() {
        let mut idx = index();
        // Two closed intervals tile [0,10) and [10,20); [20,+inf) is the open
        // tail (not in the index).
        idx.insert_close(close(b"k", 0, 10, b"a")).expect("c0");
        idx.insert_close(close(b"k", 10, 20, b"b")).expect("c1");
        let k = BusinessKey::new(b"k".to_vec());
        assert_eq!(
            idx.active_at(&k, SystemTimeMicros(5)).expect("at 5"),
            Some((SystemTimeMicros(0), 0)),
        );
        assert_eq!(
            idx.active_at(&k, SystemTimeMicros(15)).expect("at 15"),
            Some((SystemTimeMicros(10), 0)),
        );
        // At the exact close point the interval is already exclusive-closed.
        assert_eq!(
            idx.active_at(&k, SystemTimeMicros(10)).expect("at 10"),
            Some((SystemTimeMicros(10), 0)),
        );
        // Beyond every closed interval ⇒ open tail, not the index's to answer.
        assert_eq!(
            idx.active_at(&k, SystemTimeMicros(25)).expect("at 25"),
            None
        );
        // Before the first version.
        idx.insert_close(close(b"k", 0, 10, b"a")).expect("idem");
    }

    #[test]
    fn same_sys_from_closes_keyed_by_seq_do_not_collide() {
        // Two versions of one key share sys_from=10 (the writer no longer
        // force-bumps the timestamp, STL-145). Each is closed to a *different*
        // sys_to. Keyed on sys_from alone the second close would be rejected as a
        // conflicting re-close of the first; the (sys_from, seq) key keeps them
        // distinct, so both materialize and resolve independently.
        let mut idx = index();
        idx.insert_close(close_seq(b"k", 10, 0, 20, b"a"))
            .expect("seq 0");
        idx.insert_close(close_seq(b"k", 10, 1, 30, b"b"))
            .expect("seq 1");
        let k = BusinessKey::new(b"k".to_vec());
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(10), 0)
                .unwrap()
                .unwrap()
                .sys_to,
            SystemTimeMicros(20),
        );
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(10), 1)
                .unwrap()
                .unwrap()
                .sys_to,
            SystemTimeMicros(30),
        );
        assert_eq!(idx.len().expect("len"), 2, "both closes survive");
        // A conflicting re-close of the *same* (sys_from, seq) is still refused.
        let err = idx
            .insert_close(close_seq(b"k", 10, 0, 21, b"a"))
            .unwrap_err();
        assert!(matches!(err, ValidityError::AlreadyClosed));
    }

    #[test]
    fn spill_then_lookup_merges_disk_and_memory() {
        // Tiny threshold so the second insert spills the first.
        let mut idx = ValidityIndex::open(
            MemDisk::new(),
            ValidityConfig {
                spill_threshold_bytes: 1,
            },
        )
        .expect("open");
        idx.insert_close(close(b"k", 0, 10, b"a")).expect("c0");
        idx.insert_close(close(b"k", 10, 20, b"b")).expect("c1");
        // The first entry is on a spill, the second resident — both must resolve.
        let k = BusinessKey::new(b"k".to_vec());
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(0), 0)
                .expect("spilled")
                .unwrap()
                .sys_to,
            SystemTimeMicros(10),
        );
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(10), 0)
                .expect("resident")
                .unwrap()
                .sys_to,
            SystemTimeMicros(20),
        );
        assert_eq!(idx.len().expect("len"), 2);
        // A conflicting re-close of the *spilled* entry is still refused.
        let err = idx.insert_close(close(b"k", 0, 11, b"a")).unwrap_err();
        assert!(matches!(err, ValidityError::AlreadyClosed));
    }

    // --- closes_for_keys (STL-142) ------------------------------------------

    fn spilling_index() -> ValidityIndex<MemDisk> {
        // Tiny threshold so closes spill almost immediately.
        ValidityIndex::open(
            MemDisk::new(),
            ValidityConfig {
                spill_threshold_bytes: 1,
            },
        )
        .expect("open")
    }

    #[test]
    fn closes_for_keys_matches_a_filtered_materialize() {
        // Whichever branch it takes, the result for the requested keys must equal
        // the full materialization restricted to those keys.
        let mut idx = spilling_index();
        for (k, sf) in [
            (b"a".as_slice(), 0),
            (b"a", 100),
            (b"b", 10),
            (b"c", 5),
            (b"d", 7),
        ] {
            idx.insert_close(close(k, sf, sf + 1, b"x"))
                .expect("insert");
        }
        let all = idx.materialize().expect("materialize");
        let keys: std::collections::BTreeSet<BusinessKey> =
            [bk(b"a"), bk(b"c")].into_iter().collect();
        let got = idx.closes_for_keys(&keys).expect("subset");
        let want: BTreeMap<_, _> = all
            .iter()
            .filter(|((k, _, _), _)| keys.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(got, want);
        // The single-key set still resolves both of `a`'s closes.
        let one: std::collections::BTreeSet<BusinessKey> = std::iter::once(bk(b"a")).collect();
        let got_a = idx.closes_for_keys(&one).expect("one");
        assert_eq!(got_a.len(), 2);
    }

    #[test]
    fn closes_for_keys_full_sweep_branch_returns_everything() {
        // When the requested set spans every spill, the sweep branch fires and
        // returns the whole index — a superset the fold overlays by key.
        let mut idx = spilling_index();
        idx.insert_close(close(b"a", 0, 1, b"x")).expect("c0");
        idx.insert_close(close(b"b", 0, 1, b"x")).expect("c1");
        let keys: std::collections::BTreeSet<BusinessKey> =
            [bk(b"a"), bk(b"b")].into_iter().collect();
        let got = idx.closes_for_keys(&keys).expect("sweep");
        assert_eq!(got, idx.materialize().expect("materialize"));
    }

    // --- sys_upper_bound (STL-139) ------------------------------------------

    fn bk(key: &[u8]) -> BusinessKey {
        BusinessKey::new(key.to_vec())
    }

    #[test]
    fn upper_bound_of_all_closed_versions_is_the_greatest_sys_to() {
        let mut idx = index();
        idx.insert_close(close(b"a", 0, 100, b"x")).expect("c0");
        idx.insert_close(close(b"a", 100, 250, b"x")).expect("c1");
        idx.insert_close(close(b"b", 10, 200, b"x")).expect("c2");
        let keys = [
            (bk(b"a"), SystemTimeMicros(0), 0),
            (bk(b"a"), SystemTimeMicros(100), 0),
            (bk(b"b"), SystemTimeMicros(10), 0),
        ];
        let bound = idx.sys_upper_bound(keys).expect("bound");
        assert_eq!(bound, SysUpperBound::Bounded(SystemTimeMicros(250)));
    }

    #[test]
    fn a_single_open_version_forces_unbounded() {
        let mut idx = index();
        idx.insert_close(close(b"a", 0, 100, b"x")).expect("c0");
        // (b, 10) has no close → it is open, so the whole set is unbounded even
        // though `a` is closed.
        let keys = [
            (bk(b"a"), SystemTimeMicros(0), 0),
            (bk(b"b"), SystemTimeMicros(10), 0),
        ];
        let bound = idx.sys_upper_bound(keys).expect("bound");
        assert_eq!(bound, SysUpperBound::Unbounded);
        assert!(
            !bound.superseded_at_or_before(SystemTimeMicros(i64::MAX)),
            "an open version is never superseded, at any snapshot",
        );
    }

    #[test]
    fn empty_set_is_vacuously_superseded() {
        let idx = index();
        let bound = idx
            .sys_upper_bound(std::iter::empty::<(BusinessKey, SystemTimeMicros, u64)>())
            .expect("bound");
        assert_eq!(bound, SysUpperBound::Bounded(SystemTimeMicros(i64::MIN)));
        assert!(bound.superseded_at_or_before(SystemTimeMicros(0)));
    }

    #[test]
    fn superseded_boundary_is_inclusive() {
        // max(sys_to) == snapshot: the period end is exclusive, so the row is
        // already superseded *at* the snapshot and the segment prunes.
        let mut idx = index();
        idx.insert_close(close(b"a", 0, 100, b"x")).expect("c0");
        let keys = [(bk(b"a"), SystemTimeMicros(0), 0)];
        let bound = idx.sys_upper_bound(keys).expect("bound");
        assert!(bound.superseded_at_or_before(SystemTimeMicros(100)));
        assert!(!bound.superseded_at_or_before(SystemTimeMicros(99)));
    }

    #[test]
    fn upper_bound_resolves_spilled_closes() {
        // Tiny threshold so the first close spills; the bound must still see it.
        let mut idx = ValidityIndex::open(
            MemDisk::new(),
            ValidityConfig {
                spill_threshold_bytes: 1,
            },
        )
        .expect("open");
        idx.insert_close(close(b"a", 0, 100, b"x")).expect("c0");
        idx.insert_close(close(b"a", 100, 300, b"x")).expect("c1");
        let keys = [
            (bk(b"a"), SystemTimeMicros(0), 0),
            (bk(b"a"), SystemTimeMicros(100), 0),
        ];
        let bound = idx.sys_upper_bound(keys).expect("bound");
        assert_eq!(bound, SysUpperBound::Bounded(SystemTimeMicros(300)));
    }
}
