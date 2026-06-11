//! Cross-segment system-time close — integration tests (STL-133, [ADR-0023]).
//!
//! These exercise closing a prior system-time period when the open version has
//! already been flushed out of the delta tier into a **sealed segment**. The
//! close cannot re-stage the version (invariant 1 forbids mutating a sealed
//! segment) — instead it is a write-once append to the derived
//! [validity index](stele_storage::validity::ValidityIndex): the version's
//! materialized `sys_to` lives there, never on the record. A read overlays the
//! index onto the sealed version to surface the closed interval
//! ([`merge::fold_chains`]).
//!
//! * **Round-trip** — insert → `flush_to_segment` → update; a snapshot scan
//!   merging segment + delta + index returns exactly two versions, the sealed one
//!   closed at the new commit plus a new open one.
//! * **Interval invariant across a flush boundary** — for any business key, the
//!   `[sys_from, sys_to)` intervals stay non-overlapping and gap-free with
//!   updates/deletes interleaved with flushes, over a seed sweep.
//! * **Differential oracle across a flush boundary** ([STL-135], DoD bullet 1) —
//!   the same random INSERT/UPDATE/DELETE workload is replayed against a naive
//!   in-memory reference model ([06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)),
//!   and every key's reconstructed chain (version `sys_from` + index `sys_to`) is
//!   asserted *element-for-element equal* to the model — a stronger check than the
//!   structural invariant above.
//! * **Invariant 1** — no code path mutates the sealed segment; its bytes are
//!   byte-for-byte unchanged after a cross-segment close (the close is an index
//!   append, not a segment rewrite).

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::backend::{Disk, DiskFile, MemDisk};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::merge;
use stele_storage::segment::{SegmentReader, SegmentWriter};
use stele_storage::systime::{SealedVersions, SysTimeWriter};
use stele_storage::validity::{ValidityConfig, ValidityIndex};

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

fn new_delta() -> Delta<MemDisk> {
    Delta::open(MemDisk::new(), DeltaConfig::default()).unwrap()
}

fn new_index() -> ValidityIndex<MemDisk> {
    ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).unwrap()
}

/// A hand-driven clock behind a shared atomic — satisfies `Clock: Send + Sync`.
#[derive(Clone)]
struct StubClock(Arc<AtomicI64>);
impl StubClock {
    fn new(start: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start)))
    }
    fn advance(&self, by: i64) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }
}
impl Clock for StubClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.load(Ordering::Relaxed))
    }
}

/// Tiny xorshift64* — deterministic, dependency-free; matches the other storage
/// tests so a failing seed reproduces bit-for-bit (ADR-0010).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }
    const fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    const fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Write `rows` into a fresh sealed segment and read every version back — the
/// real columnar flush boundary, not a stand-in. Returns what a reader sees
/// (open/unresolved birth state — the materialized end lives in the index).
fn seal(disk: &MemDisk, name: &str, rows: Vec<Version>) -> Vec<Version> {
    let mut writer = SegmentWriter::create(disk, name).expect("create segment");
    for v in rows {
        writer.push(v).expect("push");
    }
    writer.finish().expect("finish");
    SegmentReader::open(disk, name)
        .expect("open segment")
        .read_versions()
        .expect("read versions")
}

/// The raw on-disk bytes of a sealed segment — used to prove invariant 1.
fn raw_bytes(disk: &MemDisk, name: &str) -> Vec<u8> {
    let file = disk.open(name).expect("open file");
    let mut buf = vec![0u8; file.len() as usize];
    let n = file.read_at(0, &mut buf).expect("read");
    assert_eq!(n, buf.len(), "short read on segment bytes");
    buf
}

/// Reconstruct one key's full version chain across the sealed pool and the delta
/// tier, overlaying each version's materialized end from the validity index —
/// the cross-tier read path a query executor performs ([`merge::fold_chains`]).
fn merged_chain(
    delta: &Delta<MemDisk>,
    sealed: &[Version],
    index: &ValidityIndex<MemDisk>,
    key: &BusinessKey,
) -> Vec<Version> {
    let delta_versions = delta.candidate_versions(key).expect("candidates");
    let sealed_versions = sealed.iter().filter(|v| &v.business_key == key).cloned();
    let chains =
        merge::fold_chains(sealed_versions.chain(delta_versions), index).expect("fold chains");
    chains
        .get(key)
        .map(|c| c.values().cloned().collect())
        .unwrap_or_default()
}

/// Wrap one key's reconstructed chain in the `BTreeMap` shape
/// [`merge::resolve_snapshot`] expects, so a test can snapshot-resolve it.
fn chains_of(
    key: &BusinessKey,
    chain: &[Version],
) -> BTreeMap<BusinessKey, BTreeMap<(SystemTimeMicros, u64), Version>> {
    let mut chains = BTreeMap::new();
    chains.insert(
        key.clone(),
        chain
            .iter()
            .map(|v| ((v.sys_from, v.seq), v.clone()))
            .collect(),
    );
    chains
}

// --- Round-trip --------------------------------------------------------------

#[test]
fn update_across_a_flush_boundary_closes_the_sealed_version_via_the_index() {
    let mut delta = new_delta();
    let mut index = new_index();
    let seg_disk = MemDisk::new();
    let clock = StubClock::new(1_000);
    let mut writer = SysTimeWriter::new(clock.clone());
    let key = BusinessKey::new(b"acct-42".to_vec());

    // Insert, then flush the open version out of the delta into a sealed segment.
    let c0 = writer
        .insert(
            &mut delta,
            &mut index,
            &SealedVersions::default(),
            key.clone(),
            Some(b"balance=100".to_vec()),
            0,
            TxnId(10),
            Principal::new(b"writer-a".to_vec()),
        )
        .unwrap();
    let sealed = seal(&seg_disk, "seg-0.seg", delta.flush_to_segment().unwrap());
    assert_eq!(sealed.len(), 1, "one open version flushed");
    assert_eq!(sealed[0].sys_to, SYSTEM_TIME_OPEN, "it is sealed open");
    let bytes_after_seal = raw_bytes(&seg_disk, "seg-0.seg");

    // The update's live version now lives ONLY in the sealed segment.
    clock.advance(1_000);
    let lookup = SealedVersions::new(sealed.clone());
    let c1 = writer
        .update(
            &mut delta,
            &mut index,
            &lookup,
            key.clone(),
            Some(b"balance=150".to_vec()),
            0,
            TxnId(20),
            Principal::new(b"writer-b".to_vec()),
        )
        .unwrap();
    assert!(c0 < c1);

    // The close did not re-stage a full version — it appended exactly one entry
    // to the validity index, targeting the sealed version.
    assert_eq!(index.len().unwrap(), 1, "exactly one materialized close");
    let closed_interval = index.close_of(&key, c0, 0).unwrap().expect("c0 is closed");
    assert_eq!(closed_interval.sys_to, c1, "the index closes c0 at c1");

    // Invariant 1: the sealed segment's bytes are unchanged by the close.
    assert_eq!(
        raw_bytes(&seg_disk, "seg-0.seg"),
        bytes_after_seal,
        "the sealed segment must not be mutated by a cross-segment close",
    );

    // The merged read returns exactly two versions: the sealed one closed at c1,
    // and the new open one — with the sealed version's body and birth provenance
    // intact, plus the close provenance from the index overlay.
    let chain = merged_chain(&delta, &sealed, &index, &key);
    assert_eq!(
        chain.len(),
        2,
        "segment + delta + index merge ⇒ two versions"
    );
    let (closed, open) = (&chain[0], &chain[1]);

    assert_eq!(closed.sys_from, c0);
    assert_eq!(
        closed.sys_to, c1,
        "the sealed version is closed at the update"
    );
    assert_eq!(
        closed.payload.as_deref(),
        Some(&b"balance=100"[..]),
        "body preserved from segment"
    );
    assert_eq!(
        closed.provenance.txn_id,
        TxnId(10),
        "birth provenance intact"
    );
    assert_eq!(closed.provenance.committed_at, c0);
    assert_eq!(
        closed.closed_by,
        Some(Provenance::new(
            TxnId(20),
            c1,
            Principal::new(b"writer-b".to_vec())
        )),
        "the close records the superseding transaction",
    );

    assert_eq!(open.sys_from, c1);
    assert_eq!(open.sys_to, SYSTEM_TIME_OPEN, "the new version stays open");
    assert_eq!(open.payload.as_deref(), Some(&b"balance=150"[..]));
    assert_eq!(open.closed_by, None);

    // Snapshot resolution sees one live version on each side of the close.
    let chains = chains_of(&key, &chain);
    let before = merge::resolve_snapshot(&chains, Snapshot(c0));
    assert_eq!(before.len(), 1);
    assert_eq!(
        before[0].sys_from, c0,
        "before the close, the old version is live"
    );
    let after = merge::resolve_snapshot(&chains, Snapshot(c1));
    assert_eq!(
        after[0].sys_from, c1,
        "at the close, the new version is live"
    );
}

#[test]
fn delete_across_a_flush_boundary_closes_the_sealed_version_and_leaves_no_open() {
    let mut delta = new_delta();
    let mut index = new_index();
    let seg_disk = MemDisk::new();
    let clock = StubClock::new(50);
    let mut writer = SysTimeWriter::new(clock.clone());
    let key = BusinessKey::new(b"k".to_vec());

    let c0 = writer
        .insert(
            &mut delta,
            &mut index,
            &SealedVersions::default(),
            key.clone(),
            Some(b"v".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    let sealed = seal(&seg_disk, "seg.seg", delta.flush_to_segment().unwrap());

    clock.advance(100);
    let lookup = SealedVersions::new(sealed.clone());
    let deleted_at = writer
        .delete(
            &mut delta,
            &mut index,
            &lookup,
            &key,
            TxnId(2),
            Principal::new(b"deleter".to_vec()),
        )
        .unwrap();

    // A delete-across-flush is an index close too — no successor version staged.
    assert_eq!(
        delta.candidate_versions(&key).unwrap().len(),
        0,
        "no new version staged"
    );
    let chain = merged_chain(&delta, &sealed, &index, &key);
    assert_eq!(
        chain.len(),
        1,
        "the key has exactly the one (now closed) version"
    );
    assert_eq!(chain[0].sys_from, c0);
    assert_eq!(
        chain[0].sys_to, deleted_at,
        "delete closes the sealed period"
    );
    assert_ne!(chain[0].sys_to, SYSTEM_TIME_OPEN);
    assert_eq!(
        chain[0].closed_by,
        Some(Provenance::new(
            TxnId(2),
            deleted_at,
            Principal::new(b"deleter".to_vec())
        )),
        "the tombstone carries the deleting transaction's provenance",
    );

    // Nothing is live after the delete.
    let chains = chains_of(&key, &chain);
    assert!(merge::resolve_snapshot(&chains, Snapshot(SystemTimeMicros(i64::MAX - 1))).is_empty());
}

#[test]
fn insert_on_a_key_live_only_in_a_segment_is_rejected() {
    // The liveness check must span tiers: a key whose live version is sealed is
    // still live, so a re-insert is a KeyExists error, not a silent second open.
    let mut delta = new_delta();
    let mut index = new_index();
    let seg_disk = MemDisk::new();
    let mut writer = SysTimeWriter::new(StubClock::new(1));
    let key = BusinessKey::new(b"dup".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &SealedVersions::default(),
            key.clone(),
            Some(b"a".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    let sealed = seal(&seg_disk, "s.seg", delta.flush_to_segment().unwrap());

    let lookup = SealedVersions::new(sealed);
    let err = writer
        .insert(
            &mut delta,
            &mut index,
            &lookup,
            key,
            Some(b"b".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .unwrap_err();
    assert!(
        matches!(err, stele_storage::systime::SysTimeError::KeyExists),
        "re-inserting a key live in a segment must be rejected, got {err:?}",
    );
}

// --- Interval invariant across a flush boundary ------------------------------

/// What op created a given version — the boundary to it abuts iff it was an
/// update; an insert after a delete opens a fresh period with a real gap.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Born {
    Insert,
    Update,
}

/// Assert one key's reconstructed `chain` upholds the bitemporal invariant:
/// strictly-increasing, non-overlapping intervals; every superseded period
/// closed; update boundaries abut while delete→re-insert boundaries leave a gap
/// (read off `born`); and exactly one open period iff the key is currently
/// `live`, always the newest.
fn assert_chain_invariant(seed: u64, k: usize, chain: &[Version], born: &[Born], live: bool) {
    assert_eq!(
        chain.len(),
        born.len(),
        "seed {seed} key {k}: chain length must match the ops that opened a period",
    );
    if chain.is_empty() {
        return;
    }

    for (i, w) in chain.windows(2).enumerate() {
        let (lo, hi) = (&w[0], &w[1]);
        // Totally ordered by (sys_from, seq): starts may tie at a shared tick,
        // with seq breaking the tie (STL-145).
        assert!(
            (lo.sys_from, lo.seq) < (hi.sys_from, hi.seq),
            "seed {seed} key {k}: chain not ordered by (sys_from, seq)",
        );
        assert!(
            lo.sys_to <= hi.sys_from,
            "seed {seed} key {k}: intervals overlap",
        );
        assert_ne!(
            lo.sys_to, SYSTEM_TIME_OPEN,
            "seed {seed} key {k}: a superseded period is still open",
        );
        // An update abuts the prior period exactly. An insert after a delete
        // opens a fresh period no earlier than the close — a real gap when the
        // re-insert is a later tick, or a degenerate (empty) gap when the delete
        // and re-insert land on the *same* tick (`lo.sys_to == hi.sys_from`).
        let born_hi = born[i + 1];
        match born_hi {
            Born::Update => assert_eq!(
                lo.sys_to, hi.sys_from,
                "seed {seed} key {k}: consecutive update periods must abut",
            ),
            Born::Insert => assert!(
                lo.sys_to <= hi.sys_from,
                "seed {seed} key {k}: delete→re-insert gap must not be negative",
            ),
        }
    }

    // At most one open period, and only when the key is currently live.
    let open = chain
        .iter()
        .filter(|v| v.sys_to == SYSTEM_TIME_OPEN)
        .count();
    if live {
        assert_eq!(
            open, 1,
            "seed {seed} key {k}: a live key has one open period"
        );
        assert_eq!(
            chain.last().unwrap().sys_to,
            SYSTEM_TIME_OPEN,
            "seed {seed} key {k}: the open period is the newest",
        );
    } else {
        assert_eq!(
            open, 0,
            "seed {seed} key {k}: a deleted key has no open period"
        );
    }
}

#[test]
fn chains_stay_non_overlapping_and_gap_free_across_flush_boundaries() {
    const KEY_POOL: u64 = 5;

    for seed in 0u64..120 {
        let mut rng = Rng::new(seed);
        let mut delta = new_delta();
        // The validity index is derived state that persists across flushes — it
        // is the authority for every version's end, delta-resident or sealed.
        let mut index = new_index();
        let seg_disk = MemDisk::new();
        let clock = StubClock::new(1);
        let mut writer = SysTimeWriter::new(clock.clone());

        // Everything flushed so far, as a reader sees it; rebuilt per flush.
        let mut sealed: Vec<Version> = Vec::new();
        let mut next_seg = 0u64;
        // Per key: liveness and the op that created each version, in order.
        let mut live = vec![false; KEY_POOL as usize];
        let mut born: Vec<Vec<Born>> = vec![Vec::new(); KEY_POOL as usize];

        let ops = 40 + rng.range(40);
        for op in 0..ops {
            // Advance the clock by a small, occasionally-zero amount. A zero
            // advance is a stall: two commits share a `sys_from` and `seq` orders
            // them (STL-145) — so this sweep also exercises same-tick chains
            // across a flush boundary, where no version may be dropped.
            clock.advance((rng.range(3)) as i64);

            // ~1 in 5 ops is a flush: drain the delta's versions into a new
            // sealed segment and fold them into the reader-visible pool. The
            // closes stay in the index (the authority), not the segment.
            if rng.range(5) == 0 {
                let drained = delta.flush_to_segment().unwrap();
                if !drained.is_empty() {
                    let name = format!("seg-{next_seg}.seg");
                    next_seg += 1;
                    sealed.extend(seal(&seg_disk, &name, drained));
                }
                continue;
            }

            let k = rng.range(KEY_POOL) as usize;
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let lookup = SealedVersions::new(sealed.clone());
            let txn = TxnId(op);
            let payload = format!("s{seed}-k{k}-o{op}").into_bytes();

            // Distinct, increasing per-commit seq — the manager's total-order
            // tiebreak, so same-`sys_from` versions never collide.
            let seq = op;
            if live[k] {
                // Half the time supersede, half the time delete.
                if rng.range(2) == 0 {
                    writer
                        .delete(&mut delta, &mut index, &lookup, &key, txn, who())
                        .unwrap();
                    live[k] = false;
                } else {
                    writer
                        .update(
                            &mut delta,
                            &mut index,
                            &lookup,
                            key,
                            Some(payload),
                            seq,
                            txn,
                            who(),
                        )
                        .unwrap();
                    born[k].push(Born::Update);
                }
            } else {
                writer
                    .insert(
                        &mut delta,
                        &mut index,
                        &lookup,
                        key,
                        Some(payload),
                        seq,
                        txn,
                        who(),
                    )
                    .unwrap();
                live[k] = true;
                born[k].push(Born::Insert);
            }
        }

        // Reconstruct each key's full chain across every segment + the delta +
        // the index and assert the bitemporal invariant holds across the flush
        // boundaries.
        for k in 0..KEY_POOL as usize {
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let chain = merged_chain(&delta, &sealed, &index, &key);
            assert_chain_invariant(seed, k, &chain, &born[k], live[k]);
        }
    }
}

// --- Differential oracle across a flush boundary (STL-135, DoD bullet 1) ------

/// A deliberately naive reference model of the bitemporal record per
/// [16 §1](../../../docs/16-bitemporal-semantics.md): every key maps to its full
/// list of versions, built by replaying the op log with no tiers, no merging, and
/// no index — "too simple to be wrong" ([06 §4]). The engine's reconstructed
/// chain is asserted *differential-equal* to this. The model materializes each
/// version's `sys_to` itself when a later op closes the period — which is exactly
/// what the validity index must reproduce on the engine side, the heart of
/// [STL-135] / [ADR-0023].
#[derive(Default)]
struct Oracle {
    chains: BTreeMap<BusinessKey, Vec<OracleVersion>>,
}

/// The projection the differential test compares on: the system-time interval,
/// the body, the birth provenance, and the close provenance. The model holds it
/// directly; an engine [`Version`] is reduced to it by [`project`].
#[derive(Clone, PartialEq, Eq, Debug)]
struct OracleVersion {
    sys_from: SystemTimeMicros,
    /// Per-commit tiebreak (STL-145). The model carries it so the differential
    /// verifies the engine preserves `seq` end-to-end through the delta frame,
    /// the sealed-segment column, and the (sys_from, seq)-keyed index.
    seq: u64,
    sys_to: SystemTimeMicros,
    payload: Vec<u8>,
    provenance: Provenance,
    closed_by: Option<Provenance>,
}

impl Oracle {
    /// Open a new period `[commit, +∞)` for `key` — the model side of an INSERT,
    /// or of the new version an UPDATE opens.
    fn open(
        &mut self,
        key: &BusinessKey,
        commit: SystemTimeMicros,
        seq: u64,
        payload: Vec<u8>,
        who: &Provenance,
    ) {
        self.chains
            .entry(key.clone())
            .or_default()
            .push(OracleVersion {
                sys_from: commit,
                seq,
                sys_to: SYSTEM_TIME_OPEN,
                payload,
                provenance: who.clone(),
                closed_by: None,
            });
    }

    /// Close `key`'s currently-open period at `commit`, stamping the closer — the
    /// model side of the write-once validity-index entry an UPDATE or DELETE makes.
    fn close(&mut self, key: &BusinessKey, commit: SystemTimeMicros, closer: &Provenance) {
        let last = self
            .chains
            .get_mut(key)
            .expect("a live key has a chain")
            .last_mut()
            .expect("a live key has an open version");
        assert_eq!(
            last.sys_to, SYSTEM_TIME_OPEN,
            "the model only ever closes a currently-open period",
        );
        last.sys_to = commit;
        last.closed_by = Some(closer.clone());
    }

    /// One key's modeled chain, oldest first (empty if the key was never written).
    fn chain(&self, key: &BusinessKey) -> Vec<OracleVersion> {
        self.chains.get(key).cloned().unwrap_or_default()
    }
}

/// Reduce an engine [`Version`] to the fields the oracle models, so the two are
/// compared on identical terms: the engine's overlay supplies `sys_to` /
/// `closed_by` from the index, the body and birth provenance from the record.
fn project(v: &Version) -> OracleVersion {
    OracleVersion {
        sys_from: v.sys_from,
        seq: v.seq,
        sys_to: v.sys_to,
        payload: v.payload.clone().unwrap(),
        provenance: v.provenance.clone(),
        closed_by: v.closed_by.clone(),
    }
}

#[test]
fn engine_chain_is_differential_equal_to_the_oracle_across_flush_boundaries() {
    const KEY_POOL: u64 = 5;

    for seed in 0u64..120 {
        let mut rng = Rng::new(seed);
        let mut delta = new_delta();
        // The validity index is the authority for every version's end across the
        // flush boundary; the model never sees it — a divergence means the engine
        // mis-materialized a close.
        let mut index = new_index();
        let seg_disk = MemDisk::new();
        let clock = StubClock::new(1);
        let mut writer = SysTimeWriter::new(clock.clone());

        let mut sealed: Vec<Version> = Vec::new();
        let mut next_seg = 0u64;
        let mut live = vec![false; KEY_POOL as usize];
        let mut oracle = Oracle::default();

        let ops = 40 + rng.range(40);
        for op in 0..ops {
            clock.advance((rng.range(3)) as i64);

            // ~1 in 5 ops flushes the delta into a fresh sealed segment. The
            // model is oblivious to tiering, which is the point: a flush must not
            // change any reconstructed chain — the close stays in the index.
            if rng.range(5) == 0 {
                let drained = delta.flush_to_segment().unwrap();
                if !drained.is_empty() {
                    let name = format!("seg-{next_seg}.seg");
                    next_seg += 1;
                    let rows = seal(&seg_disk, &name, drained);
                    // DoD bullet 2: a sealed version is raw-*open* — its end is
                    // never on the record, only ever in the validity index.
                    assert!(
                        rows.iter().all(|v| v.sys_to == SYSTEM_TIME_OPEN),
                        "seed {seed}: a flushed version must carry no sys_to (it lives in the index)",
                    );
                    sealed.extend(rows);
                }
                continue;
            }

            let k = rng.range(KEY_POOL) as usize;
            let key = BusinessKey::new(vec![b'k', k as u8]);
            // The writer only resolves the *current* key against the sealed pool,
            // so hand it just that key's sealed versions rather than cloning the
            // whole (growing) pool every op.
            let lookup = SealedVersions::new(
                sealed
                    .iter()
                    .filter(|v| v.business_key == key)
                    .cloned()
                    .collect(),
            );
            let txn = TxnId(op);
            let principal = Principal::new(format!("p{op}").into_bytes());
            let payload = format!("s{seed}-k{k}-o{op}").into_bytes();

            if live[k] {
                if rng.range(2) == 0 {
                    let at = writer
                        .delete(
                            &mut delta,
                            &mut index,
                            &lookup,
                            &key,
                            txn,
                            principal.clone(),
                        )
                        .unwrap();
                    oracle.close(&key, at, &Provenance::new(txn, at, principal));
                    live[k] = false;
                } else {
                    let at = writer
                        .update(
                            &mut delta,
                            &mut index,
                            &lookup,
                            key.clone(),
                            Some(payload.clone()),
                            op,
                            txn,
                            principal.clone(),
                        )
                        .unwrap();
                    // An update closes the prior period and opens the new one at
                    // the same commit, so both halves share that provenance
                    // (committed_at == the boundary).
                    let prov = Provenance::new(txn, at, principal);
                    oracle.close(&key, at, &prov);
                    oracle.open(&key, at, op, payload, &prov);
                }
            } else {
                let at = writer
                    .insert(
                        &mut delta,
                        &mut index,
                        &lookup,
                        key.clone(),
                        Some(payload.clone()),
                        op,
                        txn,
                        principal.clone(),
                    )
                    .unwrap();
                oracle.open(&key, at, op, payload, &Provenance::new(txn, at, principal));
                live[k] = true;
            }
        }

        // Reconstruct each key's chain across every segment + the delta + the
        // index and assert it matches the model element-for-element.
        for k in 0..KEY_POOL as usize {
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let engine = merged_chain(&delta, &sealed, &index, &key);
            assert_oracle_equal(seed, k, &engine, &oracle.chain(&key));
        }
    }
}

/// Assert one key's engine `chain` is differential-equal to the `model` and,
/// directly, upholds the structural sub-properties the DoD names — so a failure
/// reports *which* property broke, not just "≠ model".
///
/// Equality already subsumes "non-overlapping and gap-free": the model abuts on
/// update and only gaps on delete→re-insert, so any stray gap, overlap, or
/// mis-close diverges. The windowed sub-properties run *first* so a structural
/// break is reported as that specific property rather than as a bulk "≠ model"
/// panic; the element-for-element equality is the final, stricter check.
fn assert_oracle_equal(seed: u64, k: usize, chain: &[Version], model: &[OracleVersion]) {
    let engine: Vec<OracleVersion> = chain.iter().map(project).collect();
    for w in engine.windows(2) {
        let (lo, hi) = (&w[0], &w[1]);
        assert!(
            (lo.sys_from, lo.seq) < (hi.sys_from, hi.seq),
            "seed {seed} key {k}: chain not ordered by (sys_from, seq)",
        );
        assert!(
            lo.sys_to <= hi.sys_from,
            "seed {seed} key {k}: intervals overlap",
        );
        assert_ne!(
            lo.sys_to, SYSTEM_TIME_OPEN,
            "seed {seed} key {k}: a superseded period is still open",
        );
    }
    assert_eq!(
        engine, model,
        "seed {seed} key {k}: engine chain diverged from the oracle",
    );
}
