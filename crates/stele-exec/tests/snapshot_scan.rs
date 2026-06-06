//! `SnapshotScan` read-path integration tests (STL-100).
//!
//! These drive the operator the way a query would: write through the real DML
//! path, then scan an MVCC snapshot and assert the resolved, projected batch.
//!
//! * **The four-statement identity demo** (docs/05) — `INSERT` then `UPDATE`,
//!   then read `AS OF` a snapshot before the update and get the *pre-update*
//!   value back, plus the post-update value at a later snapshot.
//! * **Cross-tier merge** — the live version is in a sealed segment before the
//!   update and in the delta after it; one scan resolves the right one at each
//!   snapshot (STL-140 sealed lookup on the write side, this operator on the
//!   read side).
//! * **Projection** picks and orders the materialized columns.
//! * **Predicate** filters a multi-key table down to one row.
//! * **Zone-map prune accounting** (DoD bullet 2) — the number of segments read
//!   equals the number the zone maps did not prune.

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};
use stele_exec::{Column, SnapshotScan};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::DmlWriter;
use stele_storage::segment::{ColumnId, Predicate, SegmentReader, SegmentWriter, ZoneBound};
use stele_storage::systime::{EmptySealed, SealedSegments};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
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

/// Deterministic clock — one tick per `now()` (ADR-0010), matching the storage
/// tests so a failure reproduces bit-for-bit.
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

/// `id INT PRIMARY KEY` modeled as the business key: the id's big-endian bytes,
/// so byte-wise key order matches integer order.
fn key_of(id: i64) -> BusinessKey {
    BusinessKey::new(id.to_be_bytes().to_vec())
}

/// The same id as a zone-map bound, for an `id = ?` predicate.
fn key_bound(id: i64) -> ZoneBound {
    ZoneBound::Bytes(id.to_be_bytes().to_vec())
}

/// Hand-build a single-key sealed segment with an open version at `sys_from`,
/// for the zone-map prune fixtures.
fn seal_one(disk: &MemDisk, name: &str, key: &[u8], sys_from: i64) -> SegmentReader<MemFile> {
    let mut w = SegmentWriter::create(disk, name).expect("create segment");
    w.push(Version::open(
        BusinessKey::new(key.to_vec()),
        SystemTimeMicros(sys_from),
        0,
        Provenance::new(TxnId(1), SystemTimeMicros(sys_from), who()),
        b"payload".to_vec(),
    ))
    .expect("push");
    w.finish().expect("finish");
    SegmentReader::open(disk, name).expect("open segment")
}

/// Flush the delta into a fresh sealed segment and reopen it for read — the real
/// columnar flush boundary, after which the drained versions live only in the
/// segment.
fn seal(disk: &MemDisk, name: &str, delta: &mut Delta<MemDisk>) -> SegmentReader<MemFile> {
    let rows = delta.flush_to_segment().expect("flush");
    let mut w = SegmentWriter::create(disk, name).expect("create segment");
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    SegmentReader::open(disk, name).expect("open segment")
}

/// Pull the single bytes value of column `col` from a one-row batch result.
fn one_bytes(out: &stele_exec::ScanOutput, col: ColumnId) -> Vec<u8> {
    let (_, column) = out
        .batch
        .columns
        .iter()
        .find(|(c, _)| *c == col)
        .expect("projected column present");
    match column {
        Column::Bytes(rows) => {
            assert_eq!(rows.len(), 1, "expected exactly one row");
            rows[0].clone()
        }
        Column::I64(_) => panic!("column {col:?} is i64, expected bytes"),
    }
}

// --- the four-statement identity demo (delta only) -------------------------

#[test]
fn identity_demo_reads_the_pre_update_value_as_of() {
    // CREATE TABLE account ...; INSERT (1, 100); UPDATE balance = 250 WHERE id = 1.
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let key = key_of(1);

    let c_insert = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"100".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;
    let c_update = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            None,
            b"250".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit;
    assert!(c_insert < c_update);

    let segments: [SegmentReader<MemFile>; 0] = [];

    // SELECT balance FROM account FOR SYSTEM_TIME AS OF (before the update) WHERE id = 1
    //   → 100, the value before the update.
    let before = SnapshotScan::new(&delta, &index, &segments, Snapshot(c_insert))
        .project(vec![ColumnId::Payload])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: key_bound(1),
        })
        .execute()
        .expect("scan before update");
    assert_eq!(before.batch.rows, 1);
    assert_eq!(one_bytes(&before, ColumnId::Payload), b"100");

    // AS OF the update commit → the post-update value.
    let after = SnapshotScan::new(&delta, &index, &segments, Snapshot(c_update))
        .project(vec![ColumnId::Payload])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: key_bound(1),
        })
        .execute()
        .expect("scan after update");
    assert_eq!(one_bytes(&after, ColumnId::Payload), b"250");
}

// --- cross-tier merge: live version straddles a flush boundary -------------

#[test]
fn cross_tier_merge_resolves_segment_then_delta() {
    let seg_disk = MemDisk::new();
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let key = key_of(1);

    let c_insert = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"100".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;

    // Flush: the inserted version now lives only in the sealed segment.
    let reader = seal(&seg_disk, "seg-0.seg", &mut delta);
    let segments = [reader];

    // UPDATE closes the sealed version (resolved through the sealed lookup) and
    // stages the new open version in the delta.
    let sealed = SealedSegments::new(&segments);
    let c_update = dml
        .update(
            &mut delta,
            &mut index,
            &sealed,
            key,
            None,
            b"250".to_vec(),
            0,
            TxnId(2),
            who(),
        )
        .expect("update across flush boundary")
        .commit;

    // AS OF the insert: the live version is in the SEGMENT → 100. The segment is
    // read (zone maps cannot prune the key it holds).
    let before = SnapshotScan::new(&delta, &index, &segments, Snapshot(c_insert))
        .project(vec![ColumnId::Payload])
        .execute()
        .expect("scan before update");
    assert_eq!(one_bytes(&before, ColumnId::Payload), b"100");
    assert_eq!(before.stats.segments_scanned, 1);
    assert_eq!(before.stats.segments_total, 1);

    // AS OF the update: the live version is in the DELTA → 250.
    let after = SnapshotScan::new(&delta, &index, &segments, Snapshot(c_update))
        .project(vec![ColumnId::Payload])
        .execute()
        .expect("scan after update");
    assert_eq!(one_bytes(&after, ColumnId::Payload), b"250");
}

// --- projection ------------------------------------------------------------

#[test]
fn projection_selects_and_orders_columns() {
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let key = key_of(7);

    let commit = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"payload".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;

    let segments: [SegmentReader<MemFile>; 0] = [];
    let out = SnapshotScan::new(&delta, &index, &segments, Snapshot(commit))
        .project(vec![
            ColumnId::Payload,
            ColumnId::BusinessKey,
            ColumnId::SysFrom,
        ])
        .execute()
        .expect("scan");

    assert_eq!(out.batch.rows, 1);
    let ids: Vec<ColumnId> = out.batch.columns.iter().map(|(c, _)| *c).collect();
    assert_eq!(
        ids,
        vec![ColumnId::Payload, ColumnId::BusinessKey, ColumnId::SysFrom],
        "columns come back in projection order",
    );
    assert_eq!(one_bytes(&out, ColumnId::BusinessKey), key.as_bytes());
    match &out.batch.columns[2].1 {
        Column::I64(v) => assert_eq!(v, &[commit.0]),
        Column::Bytes(_) => panic!("sys_from is an i64 column"),
    }
}

#[test]
fn projecting_an_unsupported_column_errors() {
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);
    let commit = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key_of(1),
            None,
            b"x".to_vec(),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;

    let segments: [SegmentReader<MemFile>; 0] = [];
    let err = SnapshotScan::new(&delta, &index, &segments, Snapshot(commit))
        .project(vec![ColumnId::ValidFrom])
        .execute()
        .expect_err("valid-time column is not projectable at v0.1");
    assert!(matches!(
        err,
        stele_exec::ScanError::UnsupportedProjection(ColumnId::ValidFrom)
    ));
}

// --- predicate -------------------------------------------------------------

#[test]
fn predicate_filters_to_one_key() {
    let wal = new_wal();
    let mut delta = new_delta();
    let mut index = new_index();
    let mut dml = DmlWriter::new(wal, StepClock::new(1_000), false);

    let mut last = SystemTimeMicros(0);
    for id in 1..=3 {
        last = dml
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                key_of(id),
                None,
                format!("row-{id}").into_bytes(),
                0,
                TxnId(u64::try_from(id).unwrap()),
                who(),
            )
            .expect("insert")
            .commit;
    }

    let segments: [SegmentReader<MemFile>; 0] = [];

    // No predicate → all three keys.
    let all = SnapshotScan::new(&delta, &index, &segments, Snapshot(last))
        .project(vec![ColumnId::BusinessKey])
        .execute()
        .expect("scan all");
    assert_eq!(all.batch.rows, 3);

    // id = 2 → exactly one row.
    let one = SnapshotScan::new(&delta, &index, &segments, Snapshot(last))
        .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: key_bound(2),
        })
        .execute()
        .expect("scan id=2");
    assert_eq!(one.batch.rows, 1);
    assert_eq!(one_bytes(&one, ColumnId::BusinessKey), key_of(2).as_bytes());
    assert_eq!(one_bytes(&one, ColumnId::Payload), b"row-2");
}

// --- zone-map prune accounting (DoD bullet 2) ------------------------------

#[test]
fn segment_reads_equal_zone_map_survivors() {
    let disk = MemDisk::new();
    let delta = new_delta();
    let index = new_index();

    // Three single-key segments with disjoint key ranges. A point predicate on
    // the middle key's zone is provably outside the other two.
    let segments = [
        seal_one(&disk, "seg-a.seg", b"a", 10),
        seal_one(&disk, "seg-m.seg", b"m", 10),
        seal_one(&disk, "seg-z.seg", b"z", 10),
    ];

    let out = SnapshotScan::new(&delta, &index, &segments, Snapshot(SystemTimeMicros(1_000)))
        .project(vec![ColumnId::BusinessKey])
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: ZoneBound::Bytes(b"m".to_vec()),
        })
        .execute()
        .expect("scan");

    assert_eq!(out.stats.segments_total, 3);
    assert_eq!(
        out.stats.segments_scanned, 1,
        "only the segment whose key range brackets 'm' is read",
    );
    assert_eq!(out.stats.segments_pruned, 2);
    assert_eq!(out.batch.rows, 1);
    assert_eq!(one_bytes(&out, ColumnId::BusinessKey), b"m");
}
