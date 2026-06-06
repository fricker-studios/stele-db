//! Cross-tier close through the **DML / valid-time staging path** (STL-140).
//!
//! [`xtier_close`](super) already proves the bare [`SysTimeWriter`] closes a
//! sealed-only version via the validity index. These tests pin the same property
//! one layer up — the path a SQL `UPDATE` / `DELETE` actually travels — now that a
//! real [`SealedLookup`] is threaded through [`ValidTimeWriter`] and
//! [`DmlWriter`]. Before STL-140 the staging path forwarded `EmptySealed`, so a
//! live version that had been flushed out of the delta into a sealed segment was
//! invisible to a supersession and the close spuriously failed `KeyNotFound`.
//!
//! * **DML UPDATE across a flush boundary** — insert → flush to a sealed segment
//!   → update through [`SealedSegments`]: the sealed version is closed in the
//!   validity index at the new commit and the new open version stages in the
//!   delta. The cross-tier read path then resolves the new version live *after*
//!   the commit and the (now-closed) sealed version live just *before* it — an
//!   end-to-end oracle, not just a structural check.
//! * **DML DELETE across a flush boundary** — the sealed version is retracted; no
//!   version is live afterward.
//! * **Valid-time UPDATE across a flush boundary** — the same through
//!   [`ValidTimeWriter`] on a valid-time table: the superseded sealed version
//!   keeps its original valid interval, the new one carries the new interval.
//! * **The `EmptySealed` regression guard** — the very same UPDATE with an empty
//!   lookup still fails `KeyNotFound`, proving it is the threaded lookup, not
//!   something else, that makes the cross-tier close resolve.
//! * **Zone-map prune (DoD bullet 2)** — [`SealedSegments`] consults only the
//!   segments a business-key zone-map prune cannot rule out: a segment whose key
//!   range does not bracket the probed key is skipped, and the answer is computed
//!   from the surviving segments alone.

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{
    Clock, SYSTEM_TIME_OPEN, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros,
};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::{DmlError, DmlWriter};
use stele_storage::merge;
use stele_storage::segment::{ColumnId, Predicate, SegmentReader, SegmentWriter, ZoneBound};
use stele_storage::systime::{EmptySealed, SealedLookup, SealedSegments};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, ValidTimeWriter, unframe_payload};
use stele_storage::wal::{Wal, WalConfig};

// --- harness ---------------------------------------------------------------

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

fn new_delta() -> Delta<MemDisk> {
    Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta")
}

fn new_index() -> ValidityIndex<MemDisk> {
    ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open index")
}

fn new_wal() -> Wal<MemDisk> {
    Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal")
}

/// A deterministic, strictly-increasing clock — one tick per `now()`, matching
/// the other storage tests so a failing case reproduces bit-for-bit (ADR-0010).
struct StepClock(AtomicI64);
impl StepClock {
    const fn new(start: i64) -> Self {
        Self(AtomicI64::new(start))
    }
}
impl Clock for StepClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

/// Flush the delta into a fresh sealed segment and reopen it for read — the real
/// columnar flush boundary, after which the drained versions live *only* in the
/// segment. `valid_time` selects the valid-time segment layout (interval lifted
/// into first-class columns, STL-117).
fn seal(
    disk: &MemDisk,
    name: &str,
    delta: &mut Delta<MemDisk>,
    valid_time: bool,
) -> SegmentReader<MemFile> {
    let rows = delta.flush_to_segment().expect("flush");
    let mut w = if valid_time {
        SegmentWriter::create_valid_time(disk, name).expect("create valid-time segment")
    } else {
        SegmentWriter::create(disk, name).expect("create segment")
    };
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    SegmentReader::open(disk, name).expect("open segment")
}

/// Write `rows` straight into a sealed segment and reopen it — used to build the
/// zone-map prune fixtures with hand-chosen business-key ranges.
fn seal_rows(disk: &MemDisk, name: &str, rows: Vec<Version>) -> SegmentReader<MemFile> {
    let mut w = SegmentWriter::create(disk, name).expect("create segment");
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    SegmentReader::open(disk, name).expect("open segment")
}

fn version(key: &[u8], sys_from: i64, payload: &[u8]) -> Version {
    Version::open(
        BusinessKey::new(key.to_vec()),
        SystemTimeMicros(sys_from),
        0,
        Provenance::new(
            TxnId(u64::try_from(sys_from).unwrap_or(0)),
            SystemTimeMicros(sys_from),
            who(),
        ),
        payload.to_vec(),
    )
}

// --- DML UPDATE across a flush boundary ------------------------------------

#[test]
fn dml_update_closes_a_version_that_lives_only_in_a_sealed_segment() {
    let seg_disk = MemDisk::new();
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let key = BusinessKey::new(b"acct-42".to_vec());

    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"balance=100".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;

    // Flush the open version out of the delta; it now lives only in the segment.
    let reader = seal(&seg_disk, "seg-0.seg", &mut delta, false);
    assert!(
        delta
            .candidate_versions(&key)
            .expect("candidates")
            .is_empty(),
        "the live version was flushed out of the delta"
    );

    let readers = [reader];
    let sealed = SealedSegments::new(&readers);
    let c1 = dml
        .update(
            &mut delta,
            &mut index,
            &sealed,
            key.clone(),
            None,
            b"balance=150".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .expect("update closes the sealed version")
        .commit;
    assert!(c0 < c1);

    // The close materialized as exactly one validity-index entry targeting the
    // sealed version — no record was re-staged (invariant 1).
    assert_eq!(index.len().expect("len"), 1, "one materialized close");
    let closed = index
        .close_of(&key, c0)
        .expect("lookup")
        .expect("c0 is closed");
    assert_eq!(
        closed.sys_to, c1,
        "the index closes the sealed version at c1"
    );

    // The new open version is staged in the delta.
    let staged = delta.candidate_versions(&key).expect("candidates");
    assert_eq!(staged.len(), 1, "the new open version stages in the delta");
    assert_eq!(staged[0].sys_from, c1);
    assert_eq!(staged[0].payload, b"balance=150");

    // Read-path oracle: resolve the live version across segment + delta + index.
    let sealed_versions = readers[0].read_versions().expect("read versions");
    let delta_versions = delta.candidate_versions(&key).expect("candidates");
    let live_now = merge::resolve_open(
        &delta_versions,
        &sealed_versions,
        &index,
        &key,
        Snapshot(c1),
    )
    .expect("resolve")
    .expect("a version is live at c1");
    assert_eq!(
        live_now.payload, b"balance=150",
        "the new version is live at c1"
    );

    let just_before = Snapshot(SystemTimeMicros(c1.0 - 1));
    let live_prior =
        merge::resolve_open(&delta_versions, &sealed_versions, &index, &key, just_before)
            .expect("resolve")
            .expect("the sealed version is live just before c1");
    assert_eq!(
        live_prior.payload, b"balance=100",
        "the sealed version was live before c1"
    );
    assert_eq!(live_prior.sys_to, c1, "and is overlaid closed at c1");
}

// --- DML DELETE across a flush boundary ------------------------------------

#[test]
fn dml_delete_retracts_a_version_that_lives_only_in_a_sealed_segment() {
    let seg_disk = MemDisk::new();
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let key = BusinessKey::new(b"acct-7".to_vec());

    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"row".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;

    let reader = seal(&seg_disk, "seg-0.seg", &mut delta, false);
    let readers = [reader];
    let sealed = SealedSegments::new(&readers);

    let c1 = dml
        .delete(&mut delta, &mut index, &sealed, &key, TxnId(2), who())
        .expect("delete retracts the sealed version")
        .commit;
    assert!(c0 < c1);
    assert_eq!(
        index
            .close_of(&key, c0)
            .expect("lookup")
            .expect("c0 closed")
            .sys_to,
        c1,
        "the index retracts the sealed version at c1"
    );

    // No version is live after the retraction.
    let sealed_versions = readers[0].read_versions().expect("read versions");
    let delta_versions = delta.candidate_versions(&key).expect("candidates");
    let after = Snapshot(SystemTimeMicros(c1.0 + 10));
    let live = merge::resolve_open(&delta_versions, &sealed_versions, &index, &key, after)
        .expect("resolve");
    assert!(
        live.is_none(),
        "the deleted key has no live version after the retraction"
    );
}

// --- Regression guard: an empty lookup still cannot see the sealed version ---

#[test]
fn an_empty_lookup_still_cannot_close_the_sealed_version() {
    let seg_disk = MemDisk::new();
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let key = BusinessKey::new(b"acct-42".to_vec());

    dml.insert(
        &mut delta,
        &mut index,
        &EmptySealed,
        key.clone(),
        None,
        b"v0".to_vec(),
        0,
        TxnId(1),
        who(),
    )
    .expect("insert");
    let _reader = seal(&seg_disk, "seg-0.seg", &mut delta, false);

    // With `EmptySealed` the flushed live version is invisible — the update sees
    // no live version and fails, exactly the gap STL-140 closes. The real
    // `SealedSegments` lookup (exercised above) is what makes it resolve.
    let err = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            None,
            b"v1".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .expect_err("an empty lookup cannot see the sealed version");
    assert!(
        matches!(err, DmlError::Resolve(_)),
        "resolution fails KeyNotFound"
    );
}

// --- Valid-time UPDATE across a flush boundary -----------------------------

#[test]
fn valid_time_update_closes_a_sealed_version_and_keeps_its_interval() {
    let seg_disk = MemDisk::new();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut vw = ValidTimeWriter::new(StepClock::new(1_000), true);
    let key = BusinessKey::new(b"emp-1".to_vec());

    let iv0 = ValidInterval::new(ValidTimeMicros(100), ValidTimeMicros(200)).expect("from < to");
    let c0 = vw
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(iv0),
            b"role=ic".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert");

    // Flush into a valid-time segment; the live version lives only there.
    let reader = seal(&seg_disk, "vt-0.seg", &mut delta, true);
    let readers = [reader];
    let sealed = SealedSegments::new(&readers);

    let iv1 = ValidInterval::new(ValidTimeMicros(200), VALID_TIME_OPEN).expect("from < to");
    let c1 = vw
        .update(
            &mut delta,
            &mut index,
            &sealed,
            key.clone(),
            Some(iv1),
            b"role=lead".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .expect("update closes the sealed valid-time version");
    assert!(c0 < c1);
    assert_eq!(
        index
            .close_of(&key, c0)
            .expect("lookup")
            .expect("c0 closed")
            .sys_to,
        c1,
        "the index closes the sealed version at c1"
    );

    let sealed_versions = readers[0].read_versions().expect("read versions");
    let delta_versions = delta.candidate_versions(&key).expect("candidates");

    // After c1 the new fact is live, carrying the new (open-ended) interval.
    let live_now = merge::resolve_open(
        &delta_versions,
        &sealed_versions,
        &index,
        &key,
        Snapshot(c1),
    )
    .expect("resolve")
    .expect("live at c1");
    let (valid_now, user_now) = unframe_payload(true, &live_now.payload).expect("unframe");
    assert_eq!(
        valid_now,
        Some(iv1),
        "the new version carries the new interval"
    );
    assert_eq!(user_now, b"role=lead");

    // Just before c1 the superseded sealed version is live and keeps *its own*
    // original interval — corrections append, never mutate.
    let just_before = Snapshot(SystemTimeMicros(c1.0 - 1));
    let live_prior =
        merge::resolve_open(&delta_versions, &sealed_versions, &index, &key, just_before)
            .expect("resolve")
            .expect("live before c1");
    let (valid_prior, user_prior) = unframe_payload(true, &live_prior.payload).expect("unframe");
    assert_eq!(
        valid_prior,
        Some(iv0),
        "the superseded version keeps its interval"
    );
    assert_eq!(user_prior, b"role=ic");
}

// --- Zone-map prune (DoD bullet 2) -----------------------------------------

#[test]
fn lookup_consults_only_segments_a_zone_map_cannot_rule_out() {
    let disk = MemDisk::new();
    // Segment A holds keys in ["aaa", "acct"]; segment B holds ["zoo", "zzz"].
    let seg_a = seal_rows(
        &disk,
        "a.seg",
        vec![
            version(b"aaa", 10, b"a-low"),
            version(b"acct", 20, b"a-high"),
        ],
    );
    let seg_b = seal_rows(
        &disk,
        "b.seg",
        vec![
            version(b"zoo", 30, b"b-low"),
            version(b"zzz", 40, b"b-high"),
        ],
    );

    let open = Snapshot(SYSTEM_TIME_OPEN);
    let pred = |k: &[u8]| Predicate::Eq {
        column: ColumnId::BusinessKey,
        value: ZoneBound::Bytes(k.to_vec()),
    };
    // The prune decisions the lookup is built on: "acct" is bracketed only by A,
    // "zzz" only by B, "mmm" by neither.
    assert!(seg_a.might_contain(&pred(b"acct"), open));
    assert!(!seg_b.might_contain(&pred(b"acct"), open));
    assert!(!seg_a.might_contain(&pred(b"mmm"), open));
    assert!(!seg_b.might_contain(&pred(b"mmm"), open));

    let readers = [seg_a, seg_b];
    let sealed = SealedSegments::new(&readers);

    // A key bracketed by exactly one segment resolves from that segment alone —
    // the segment the zone map rules out is never consulted.
    let acct = BusinessKey::new(b"acct".to_vec());
    let got = sealed.versions_for(&acct).expect("lookup");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload, b"a-high");

    let zzz = BusinessKey::new(b"zzz".to_vec());
    let got = sealed.versions_for(&zzz).expect("lookup");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload, b"b-high");

    // A key bracketed by neither segment prunes both — no versions, no scan.
    let mmm = BusinessKey::new(b"mmm".to_vec());
    assert!(sealed.versions_for(&mmm).expect("lookup").is_empty());
}
