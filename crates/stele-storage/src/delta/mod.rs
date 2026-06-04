//! Delta tier — the row-oriented in-memory store with local-disk spill.
//!
//! The delta tier is where recent writes live before they are folded into
//! sealed columnar segments by compaction
//! ([architecture §3.1](../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving)).
//! It sits between the WAL (durability) and the sealed segments (bulk storage),
//! and is the structure the executor merges with sealed segments to answer
//! `AS OF` snapshot reads
//! ([architecture §3.5](../../../docs/02-architecture.md#35-read-path--as-of-flow)).
//!
//! ## Shape
//!
//! Versions are kept sorted by `(business_key, sys_from)` — version chains
//! are physically local — and the public API is exactly the one called out
//! by [STL-87]:
//!
//! ```ignore
//! let mut delta = Delta::open(disk, DeltaConfig::default())?;
//! delta.insert(version)?;
//! let live = delta.range_scan(.., Snapshot(snapshot_time))?;
//! let drained = delta.flush_to_segment()?;
//! ```
//!
//! When `delta.byte_size() > config.spill_threshold_bytes`, the next insert
//! freezes the current in-memory contents to a numbered spill file and
//! resumes filling memory from empty. Reads transparently merge the in-memory
//! tier with every spill file currently on disk.
//!
//! ## Crash semantics
//!
//! The delta itself **makes no durability claim** — see [`crate::wal`]'s
//! invariant 2. On crash, [`Delta::open`] discards any prior spill files and
//! the caller drives WAL replay back through [`Delta::insert`] to reconstruct
//! the pre-crash state. The crash-replay-equivalence property is
//! test-enforced under multiple deterministic seeds in
//! `tests/delta.rs`.

mod mem;
mod spill;
mod version;

use std::io;
use std::ops::RangeBounds;

use crate::wal::Disk;

pub use version::{BusinessKey, Snapshot, Version};

use mem::MemTier;

/// Common filename prefix for delta spill files.
pub(crate) const SPILL_FILENAME_PREFIX: &str = "delta-spill-";

/// Tuning knobs for the delta tier.
#[derive(Debug, Clone, Copy)]
pub struct DeltaConfig {
    /// Trigger a spill once the in-memory store's encoded byte count would
    /// exceed this value. Counted against [`Version::encoded_size`] so the
    /// threshold has the same meaning whether bytes are in memory or on the
    /// spill file.
    pub spill_threshold_bytes: u64,
}

impl Default for DeltaConfig {
    fn default() -> Self {
        // 64 MiB matches Postgres's default WAL segment size and the WAL's
        // default segment cap — three places where "a comfortable chunk of
        // recent activity" lives. Tuned later under benchmark guidance.
        Self {
            spill_threshold_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Errors surfaced from the delta tier.
#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    /// A spill file on disk failed to decode. Indicates either a torn write
    /// on the spill path or a stale on-disk file from a prior process the
    /// caller failed to discard via [`Delta::open`].
    #[error("delta-tier on-disk frame corrupt: {0}")]
    Corrupt(&'static str),

    /// A `Version`'s encoded size exceeded the per-frame ceiling
    /// ([`MAX_VERSION_FRAME_LEN`](version::MAX_VERSION_FRAME_LEN)). Returned
    /// from [`Delta::insert`] and from the encode path so callers get a typed
    /// error instead of a panic.
    #[error("version frame too large: {0} bytes (max 16 MiB)")]
    TooLarge(usize),

    /// An I/O failure on the spill path.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

/// The delta-tier handle.
///
/// `Delta` owns one [`Disk`] handle for spill files. The WAL uses a separate
/// handle today — until [STL-90] unifies the storage backend, the two are
/// expected to be distinct namespaces (in the filesystem case, separate
/// directories) so their filename schemes can never alias.
pub struct Delta<D: Disk> {
    disk: D,
    config: DeltaConfig,
    mem: MemTier,
    /// Next spill index to allocate. Initialized at `open` time to
    /// `max(existing spill indices) + 1`, even though the existing ones are
    /// discarded — so a delayed `disk.remove` racing a fresh insert can never
    /// land on a clashing name.
    next_spill_index: u64,
    /// Spill indices we have written this lifetime and have not yet flushed
    /// into a sealed segment. Kept in ascending order.
    live_spills: Vec<u64>,
}

impl<D: Disk> Delta<D> {
    /// Open the delta tier backed by `disk`.
    ///
    /// Any spill files left behind by a prior (crashed) process are
    /// **discarded**: the WAL is the canonical truth for the delta's
    /// contents, and re-loading a stale spill would create read state that
    /// disagrees with what WAL replay is about to produce.
    pub fn open(disk: D, config: DeltaConfig) -> Result<Self, DeltaError> {
        let existing = spill::list_spills(&disk)?;
        let next_spill_index = existing.last().map_or(0, |&i| i + 1);
        spill::discard_stale_spills(&disk)?;
        Ok(Self {
            disk,
            config,
            mem: MemTier::new(),
            next_spill_index,
            live_spills: Vec::new(),
        })
    }

    /// Insert `version`. Re-inserting the same `(business_key, sys_from)` is
    /// idempotent — this is the property WAL replay relies on.
    ///
    /// If the in-memory byte count would exceed the configured threshold,
    /// the current contents spill first and `version` lands in a fresh
    /// in-memory store.
    pub fn insert(&mut self, version: Version) -> Result<(), DeltaError> {
        version.check_encodable()?;
        let incoming = version.encoded_size() as u64;
        let projected = self.mem.byte_size().saturating_add(incoming);
        // Spill the current mem tier first when its existing contents plus the
        // incoming row would cross the threshold. Two soft edges to call out:
        //
        // * `mem.byte_size() > 0` — we don't spill an empty tier; that's a
        //   no-op and would just create a zero-row spill file.
        // * A single row larger than `spill_threshold_bytes` therefore lands
        //   in an empty mem tier and *does* briefly exceed the threshold.
        //   The next insert observes the over-threshold mem and spills it
        //   then. The threshold is a steady-state soft bound on resident
        //   bytes, not a hard per-insert ceiling — which matches the
        //   row-oriented LSM convention and the WAL's group-commit spirit.
        if projected > self.config.spill_threshold_bytes && self.mem.byte_size() > 0 {
            self.spill_in_memory()?;
        }
        self.mem.insert(version);
        Ok(())
    }

    /// Total encoded bytes currently held in memory (not counting spills).
    #[must_use]
    pub const fn byte_size(&self) -> u64 {
        self.mem.byte_size()
    }

    /// True iff at least one spill file is currently on disk.
    #[must_use]
    // `Vec::is_empty` is not yet `const fn`, so this can't be either.
    #[allow(clippy::missing_const_for_fn)]
    pub fn is_spilled(&self) -> bool {
        !self.live_spills.is_empty()
    }

    /// Snapshot read over `key_range`.
    ///
    /// For each business key in `key_range`, emit the version whose
    /// `[sys_from, sys_to)` interval contains `snapshot`. Output is sorted by
    /// business key. The merge across the in-memory tier and spill files is
    /// done by collecting all candidate versions per key and resolving the
    /// snapshot pick at the end — so a spill containing a closed period and
    /// an in-memory row containing the live one produce the live row, even
    /// if the closed one was written more recently.
    ///
    /// # Errors
    ///
    /// Surfaces I/O or corruption errors loading spill files. The in-memory
    /// path itself is infallible.
    pub fn range_scan<R>(
        &self,
        key_range: R,
        snapshot: Snapshot,
    ) -> Result<Vec<Version>, DeltaError>
    where
        R: RangeBounds<BusinessKey>,
    {
        // Materialise every candidate row in `(key, sys_from)` order into a
        // single chain-per-key map, then pick the snapshot's live version
        // for each. Doing the merge here (rather than picking per-source)
        // means a closed-period row in a spill and the matching live row in
        // memory both contribute; the snapshot resolver picks correctly.
        use std::collections::BTreeMap;
        use stele_common::time::SystemTimeMicros;
        let mut merged: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>> =
            BTreeMap::new();
        let mut take = |v: Version| {
            if !in_range(&v.business_key, &key_range) {
                return;
            }
            merged
                .entry(v.business_key.clone())
                .or_default()
                .insert(v.sys_from, v);
        };
        for v in self.mem.iter() {
            take(v.clone());
        }
        for &idx in &self.live_spills {
            for v in spill::read_spill(&self.disk, idx)? {
                take(v);
            }
        }
        let mut out = Vec::new();
        for (_, chain) in merged {
            if let Some(v) = pick_live(&chain, snapshot) {
                out.push(v);
            }
        }
        Ok(out)
    }

    /// Promote every in-memory row plus every spill into a single sorted
    /// sequence and clear the delta. Removes the consumed spill files.
    ///
    /// At v0.1 this hands the rows back to the caller — the segment writer
    /// lands with [STL-88]. Once it exists, the segment writer slots in
    /// here without changing this API.
    ///
    /// # Errors
    ///
    /// Surfaces I/O or corruption errors loading spill files.
    pub fn flush_to_segment(&mut self) -> Result<Vec<Version>, DeltaError> {
        use std::collections::BTreeMap;
        use stele_common::time::SystemTimeMicros;
        let mut merged: BTreeMap<BusinessKey, BTreeMap<SystemTimeMicros, Version>> =
            BTreeMap::new();
        for v in self.mem.drain_sorted() {
            merged
                .entry(v.business_key.clone())
                .or_default()
                .insert(v.sys_from, v);
        }
        for &idx in &self.live_spills {
            for v in spill::read_spill(&self.disk, idx)? {
                merged
                    .entry(v.business_key.clone())
                    .or_default()
                    .insert(v.sys_from, v);
            }
        }
        // Remove spills only after a successful merge — if a read failed
        // halfway through, the caller still has the option of re-reading on
        // the next attempt.
        for idx in std::mem::take(&mut self.live_spills) {
            spill::remove_spill(&self.disk, idx)?;
        }
        Ok(merged
            .into_values()
            .flat_map(BTreeMap::into_values)
            .collect())
    }

    fn spill_in_memory(&mut self) -> Result<(), DeltaError> {
        let rows = self.mem.drain_sorted();
        if rows.is_empty() {
            return Ok(());
        }
        let idx = self.next_spill_index;
        self.next_spill_index += 1;
        spill::write_spill(&self.disk, idx, &rows)?;
        self.live_spills.push(idx);
        Ok(())
    }
}

/// Resolve a key's chain at `snapshot`: the greatest `sys_from ≤ snapshot`
/// whose `sys_to > snapshot`. Mirrors the in-memory tier's resolver, but works
/// over the merged map.
fn pick_live(
    chain: &std::collections::BTreeMap<stele_common::time::SystemTimeMicros, Version>,
    snapshot: Snapshot,
) -> Option<Version> {
    chain
        .range(..=snapshot.0)
        .next_back()
        .map(|(_, v)| v)
        .filter(|v| v.sys_to > snapshot.0)
        .cloned()
}

/// Inclusive-on-start / per-bound-as-stated membership test, matching the
/// `RangeBounds` contract used by [`std::collections::BTreeMap::range`].
fn in_range<R: RangeBounds<BusinessKey>>(key: &BusinessKey, range: &R) -> bool {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let above_start = match range.start_bound() {
        Included(s) => key >= s,
        Excluded(s) => key > s,
        Unbounded => true,
    };
    let below_end = match range.end_bound() {
        Included(e) => key <= e,
        Excluded(e) => key < e,
        Unbounded => true,
    };
    above_start && below_end
}
