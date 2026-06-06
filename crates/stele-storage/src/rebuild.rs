//! From-scratch validity-index rebuild from the **segment store alone** —
//! versions + persisted retractions — independent of the WAL ([ADR-0023], STL-143).
//!
//! Routine recovery rebuilds the [validity index](crate::validity) from the WAL
//! (`checkpoint + tail`, [`crate::dml::replay`]). But the WAL is truncated once
//! its records are durably folded into segments, so a *from-scratch* rebuild must
//! reconstruct every `sys_to` from the sealed segments themselves. This module is
//! that path.
//!
//! ## Two kinds of close, two sources of truth
//!
//! A version's system-time end is materialized once into the index when the
//! version is closed ([ADR-0023]). There are exactly two ways a version closes,
//! and they reconstruct differently:
//!
//! * **Supersession** — an `UPDATE` closes the prior period and opens a new one in
//!   the *same atomic commit*. The prior version's `sys_to` therefore equals the
//!   successor version's `sys_from`, and (because the write path stamps both
//!   halves with the same `txn_id` / `commit` / `principal`, see
//!   [`crate::systime`]) the prior's `closed_by` equals the successor's birth
//!   `provenance` exactly. So a supersession close is **re-derivable from version
//!   adjacency** — no durable close record is needed. This is the only role
//!   adjacency plays: a fast-path reconstruction of supersessions, *never* the
//!   authority on where a period ends.
//!
//! * **Retraction (delete)** — a `DELETE` closes the period with **no successor**.
//!   Adjacency cannot represent it: a later re-insert of the same key would be
//!   mis-read as the successor, inferring the deleted version open right up to the
//!   re-insert and **silently resurrecting the row across the deletion gap**
//!   ([docs/16 §12](../../../docs/16-bitemporal-semantics.md#12-deletes-retractions--the-deletion-gap)).
//!   So a retraction is a **first-class durable record**, persisted into the
//!   segment store as a tombstone row ([`crate::segment::SegmentReader::read_retractions`])
//!   and applied here as the authority.
//!
//! ## The algorithm
//!
//! 1. Apply every **retraction** to the index — authoritative. The set of
//!    retracted `(business_key, sys_from, seq)` is remembered.
//! 2. For each key, walk its versions in `(sys_from, seq)` order and, for every
//!    adjacent pair `(prior, next)`, materialize the supersession close
//!    `prior.sys_to = next.sys_from` (`closed_by = next.provenance`) — **unless
//!    `prior` was retracted**, in which case the retraction already closed it and
//!    adjacency must not touch it. With a same-tick supersession the close is
//!    degenerate (`sys_to == prior.sys_from`) and the higher-`seq` sibling is the
//!    one left open. The last version of each key stays open.
//!
//! This reproduces the WAL-replay index **exactly**: supersession closes match by
//! construction, deletion closes come from the persisted tombstones, and no
//! version is ever double-closed. An implementation that skipped step 1 (pure
//! adjacency) would close a deleted version at the *re-insert* time instead of the
//! delete time — the resurrection bug the oracle in `tests/rebuild.rs` is built to
//! catch.

use std::collections::{BTreeMap, BTreeSet};

use crate::backend::Disk;
use crate::delta::{BusinessKey, Version};
use crate::validity::{Close, ValidityError, ValidityIndex};

/// Rebuild `index` from the segment store alone — no reference to the WAL.
///
/// `index` should be freshly opened (empty); on success it holds exactly the
/// closes a WAL replay would have produced.
///
/// `versions` is the union of every sealed segment's
/// [`read_versions`](crate::segment::SegmentReader::read_versions); `retractions`
/// the union of every segment's
/// [`read_retractions`](crate::segment::SegmentReader::read_retractions).
/// Ordering does not matter — versions are grouped and sorted internally.
///
/// # Errors
///
/// [`ValidityError`] if a close cannot be materialized — e.g. the index's spill
/// path fails, or two records conflict on the same `(business_key, sys_from)`
/// (which a consistent segment store never produces).
pub fn rebuild_index_from_segments<I: Disk>(
    versions: impl IntoIterator<Item = Version>,
    retractions: impl IntoIterator<Item = Close>,
    index: &mut ValidityIndex<I>,
) -> Result<(), ValidityError> {
    // 1. Retractions are authoritative — apply them and remember which versions
    //    they close, so adjacency below never re-closes a deleted version. The
    //    retracted set is keyed by the full `(key, sys_from, seq)` identity so a
    //    delete of one same-tick version does not mask its sibling (STL-145).
    let mut retracted: BTreeSet<(BusinessKey, stele_common::time::SystemTimeMicros, u64)> =
        BTreeSet::new();
    for close in retractions {
        retracted.insert((close.business_key.clone(), close.sys_from, close.seq));
        index.insert_close(close)?;
    }

    // 2. Group versions per key, sorted by `(sys_from, seq)` (BTreeMap key order),
    //    then materialize supersession closes from adjacency — skipping any
    //    version a retraction already closed. With ties, the "next" version in a
    //    window is the `(sys_from, seq)` successor, so a same-tick supersession
    //    closes the prior degenerately (`sys_to == next.sys_from == prior.sys_from`)
    //    and the higher-`seq` sibling stays open ([ADR-0024], [ADR-0023]).
    let mut chains: BTreeMap<
        BusinessKey,
        BTreeMap<(stele_common::time::SystemTimeMicros, u64), Version>,
    > = BTreeMap::new();
    for v in versions {
        chains
            .entry(v.business_key.clone())
            .or_default()
            .insert((v.sys_from, v.seq), v);
    }
    for (key, chain) in &chains {
        let ordered: Vec<&Version> = chain.values().collect();
        for pair in ordered.windows(2) {
            let prior = pair[0];
            let next = pair[1];
            if retracted.contains(&(key.clone(), prior.sys_from, prior.seq)) {
                // A delete already closed this version at the delete time; the
                // next version is a *re-insert*, not a supersession. Closing here
                // would resurrect the row across the deletion gap.
                continue;
            }
            index.insert_close(Close {
                business_key: key.clone(),
                sys_from: prior.sys_from,
                seq: prior.seq,
                // Supersession: the prior period ends exactly where the next
                // begins, closed by the superseding transaction — whose identity
                // is the next version's birth provenance ([`crate::systime`]).
                sys_to: next.sys_from,
                closed_by: next.provenance.clone(),
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::provenance::{Principal, Provenance, TxnId};
    use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};

    use crate::backend::MemDisk;
    use crate::validity::ValidityConfig;

    fn key(k: &[u8]) -> BusinessKey {
        BusinessKey::new(k.to_vec())
    }

    /// A version born at `sys_from`, stamped by transaction `txn` — mirroring the
    /// write path's `open_version` (`committed_at == sys_from`). `seq` defaults to
    /// 0; use [`version_seq`] for same-tick chains.
    fn version(k: &[u8], sys_from: i64, txn: u64) -> Version {
        version_seq(k, sys_from, 0, txn)
    }

    /// A version born at `(sys_from, seq)`, stamped by transaction `txn`.
    fn version_seq(k: &[u8], sys_from: i64, seq: u64, txn: u64) -> Version {
        Version::open(
            key(k),
            SystemTimeMicros(sys_from),
            seq,
            Provenance::new(
                TxnId(txn),
                SystemTimeMicros(sys_from),
                Principal::new(b"writer".to_vec()),
            ),
            format!("v@{sys_from}+{seq}").into_bytes(),
        )
    }

    /// A retraction closing `(k, target, seq)` at `closed_at`, by transaction
    /// `txn`. `seq` is the closed version's tiebreak; tests targeting a
    /// distinct-tick version use `0`.
    fn retraction(k: &[u8], target: i64, seq: u64, closed_at: i64, txn: u64) -> Close {
        Close {
            business_key: key(k),
            sys_from: SystemTimeMicros(target),
            seq,
            sys_to: SystemTimeMicros(closed_at),
            closed_by: Provenance::new(
                TxnId(txn),
                SystemTimeMicros(closed_at),
                Principal::new(b"deleter".to_vec()),
            ),
        }
    }

    fn index() -> ValidityIndex<MemDisk> {
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open")
    }

    #[test]
    fn adjacency_reconstructs_supersession_closes() {
        // INSERT@10 -> UPDATE@20 -> UPDATE@30, no deletes. Adjacency must close
        // 10->20 and 20->30; the last version (30) stays open.
        let mut idx = index();
        rebuild_index_from_segments(
            vec![
                version(b"k", 10, 1),
                version(b"k", 20, 2),
                version(b"k", 30, 3),
            ],
            Vec::new(),
            &mut idx,
        )
        .expect("rebuild");
        let k = key(b"k");
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(10), 0)
                .unwrap()
                .unwrap()
                .sys_to,
            SystemTimeMicros(20)
        );
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(20), 0)
                .unwrap()
                .unwrap()
                .sys_to,
            SystemTimeMicros(30)
        );
        assert!(
            idx.close_of(&k, SystemTimeMicros(30), 0).unwrap().is_none(),
            "open tail"
        );
        // closed_by of the 10->20 close is the superseding (sys_from=20) txn.
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(10), 0)
                .unwrap()
                .unwrap()
                .closed_by
                .txn_id,
            TxnId(2),
        );
    }

    #[test]
    fn retraction_wins_over_adjacency_across_the_deletion_gap() {
        // The canonical resurrection scenario, at the rebuild-unit level:
        // INSERT@10 -> UPDATE@20 -> UPDATE@30 -> DELETE@40 -> re-INSERT@50.
        // Versions in the segment store: 10, 20, 30, 50 (the delete opens none).
        // Retraction: close (k,30) at 40.
        let mut idx = index();
        rebuild_index_from_segments(
            vec![
                version(b"k", 10, 1),
                version(b"k", 20, 2),
                version(b"k", 30, 3),
                version(b"k", 50, 5),
            ],
            vec![retraction(b"k", 30, 0, 40, 4)],
            &mut idx,
        )
        .expect("rebuild");
        let k = key(b"k");
        // 30 is closed at 40 (the delete), NOT 50 (the re-insert) — no resurrection.
        let c30 = idx.close_of(&k, SystemTimeMicros(30), 0).unwrap().unwrap();
        assert_eq!(
            c30.sys_to,
            SystemTimeMicros(40),
            "deleted version closes at the delete, not the re-insert"
        );
        assert_eq!(
            c30.closed_by.txn_id,
            TxnId(4),
            "delete provenance preserved"
        );
        // The deletion gap [40,50): nothing is active. active_at(45) is the open
        // tail of *nothing* — no close contains 45, and 50 opens only at 50.
        assert!(
            idx.active_at(&k, SystemTimeMicros(45)).unwrap().is_none(),
            "no version active in the deletion gap",
        );
        // 50 is the open tail.
        assert!(idx.close_of(&k, SystemTimeMicros(50), 0).unwrap().is_none());
        // Sanity: the supersession closes are still right.
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(10), 0)
                .unwrap()
                .unwrap()
                .sys_to,
            SystemTimeMicros(20)
        );
        assert_eq!(
            idx.close_of(&k, SystemTimeMicros(20), 0)
                .unwrap()
                .unwrap()
                .sys_to,
            SystemTimeMicros(30)
        );
    }

    #[test]
    fn a_single_open_version_has_no_close() {
        let mut idx = index();
        rebuild_index_from_segments(vec![version(b"k", 10, 1)], Vec::new(), &mut idx)
            .expect("rebuild");
        assert!(
            idx.close_of(&key(b"k"), SystemTimeMicros(10), 0)
                .unwrap()
                .is_none(),
            "the lone version is open — sys_to is the +inf sentinel, never materialized",
        );
        // Guard against a stray close at the open sentinel.
        assert_eq!(idx.len().unwrap(), 0);
        let _ = SYSTEM_TIME_OPEN;
    }

    #[test]
    fn same_tick_supersession_rebuilds_a_degenerate_close_without_dropping_a_version() {
        // Two versions of `k` share sys_from=10 (seq 0 then seq 1) — the writer no
        // longer force-bumps, so a same-tick UPDATE leaves both on the chain
        // (STL-145). Adjacency closes the seq-0 version degenerately at 10 (its
        // own sys_from) and leaves seq-1 open. A by-(sys_from, seq) rebuild must
        // re-derive exactly that — keyed on sys_from alone, the two would collide
        // and one would be lost.
        let mut idx = index();
        rebuild_index_from_segments(
            vec![version_seq(b"k", 10, 0, 1), version_seq(b"k", 10, 1, 2)],
            Vec::new(),
            &mut idx,
        )
        .expect("rebuild");
        let k = key(b"k");
        // The seq-0 version is closed degenerately at its own sys_from.
        let c0 = idx.close_of(&k, SystemTimeMicros(10), 0).unwrap().unwrap();
        assert_eq!(
            c0.sys_to,
            SystemTimeMicros(10),
            "degenerate same-tick close"
        );
        assert_eq!(
            c0.closed_by.txn_id,
            TxnId(2),
            "closed by the seq-1 successor"
        );
        // The seq-1 version is the open tail — no materialized close.
        assert!(
            idx.close_of(&k, SystemTimeMicros(10), 1).unwrap().is_none(),
            "the higher-seq sibling stays open",
        );
        assert_eq!(
            idx.len().unwrap(),
            1,
            "exactly one close, neither version lost"
        );
    }
}
