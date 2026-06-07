//! Valid-time ingestion integration tests.
//!
//! Scope (STL-92):
//!
//! * **Both axes populated** — DoD bullet 1: a write to a valid-time table lands
//!   a version with a system-time period (from the inner system-time writer)
//!   *and* a recoverable valid-time interval, and a read can filter on either
//!   axis (system snapshot via the delta tier's `range_scan`; valid point via
//!   the decoded interval's `contains`).
//! * **Per-table policy** — a system-only table rejects a supplied interval; a
//!   valid-time table rejects a write that omits it.
//! * **Corrections append** — an update opens a new valid period while the
//!   superseded version keeps the interval it was written with; a delete is a
//!   system-axis fact and leaves the prior valid interval intact.
//!
//! These scan the delta tier back to assert what was stored, mirroring the
//! system-time integration tests. They live here until the `stele-sim` storage
//! scenarios subsume them ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::significant_drop_tightening,
    clippy::type_complexity
)]

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, ValidTimeError, ValidTimeWriter, unframe_payload};
use stele_storage::wal::{Disk, DiskFile};

/// A throwaway principal for valid-time write-path tests; provenance values are
/// asserted in `provenance.rs`.
fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

// --- MemDisk: minimal in-memory Disk for tests ------------------------------

#[derive(Default, Clone)]
struct MemDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
}

impl MemDisk {
    fn new() -> Self {
        Self::default()
    }
}

struct MemFile {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Disk for MemDisk {
    type File = MemFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        let mut files = self.inner.lock().unwrap();
        if files.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{name} already exists"),
            ));
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        files.insert(name.to_string(), Arc::clone(&bytes));
        Ok(MemFile { bytes })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(MemFile { bytes })
    }

    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.inner.lock().unwrap().keys().cloned().collect())
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        let mut files = self.inner.lock().unwrap();
        if files.remove(name).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()));
        }
        Ok(())
    }
}

impl DiskFile for MemFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let src = self.bytes.lock().unwrap();
        let start = offset as usize;
        if start >= src.len() {
            return Ok(0);
        }
        let end = (start + buf.len()).min(src.len());
        let n = end - start;
        buf[..n].copy_from_slice(&src[start..end]);
        Ok(n)
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn len(&self) -> u64 {
        self.bytes.lock().unwrap().len() as u64
    }
}

// --- StubClock: a clock the test drives by hand -----------------------------

#[derive(Clone)]
struct StubClock(Arc<AtomicI64>);
impl StubClock {
    fn new(start: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start)))
    }
    fn set(&self, micros: i64) {
        self.0.store(micros, Ordering::Relaxed);
    }
}
impl stele_common::time::Clock for StubClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.load(Ordering::Relaxed))
    }
}

fn new_delta() -> Delta<MemDisk> {
    Delta::open(MemDisk::new(), DeltaConfig::default()).unwrap()
}

fn new_index() -> ValidityIndex<MemDisk> {
    ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).unwrap()
}

const fn vt(micros: i64) -> ValidTimeMicros {
    ValidTimeMicros(micros)
}

fn interval(from: i64, to: i64) -> ValidInterval {
    ValidInterval::new(vt(from), vt(to)).expect("well-formed interval")
}

/// Drain the delta and group every stored version by business key, preserving
/// the `(business_key, sys_from)` order — each `Vec<Version>` is one key's full
/// chain, oldest first.
///
/// Under [ADR-0023] a flushed version's system-time end is not on the record;
/// it lives in the [`ValidityIndex`]. We overlay each version's materialized
/// `sys_to`/`closed_by` from the index — the read path's resolution — so the
/// returned chain carries the closed ends the tests assert on, leaving the open
/// tail untouched. The valid-time interval is unaffected: it stays framed in the
/// payload regardless.
///
/// Destructive: call once, after all writes.
fn drain_chains(
    delta: &mut Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
) -> BTreeMap<BusinessKey, Vec<Version>> {
    let mut map: BTreeMap<BusinessKey, Vec<Version>> = BTreeMap::new();
    for mut v in delta.flush_to_segment().unwrap() {
        if let Some(ci) = index.close_of(&v.business_key, v.sys_from, v.seq).unwrap() {
            v.sys_to = ci.sys_to;
            v.closed_by = Some(ci.closed_by);
        }
        map.entry(v.business_key.clone()).or_default().push(v);
    }
    map
}

// --- Both axes populated + reads filter on either (DoD bullet 1) -------------

#[test]
fn insert_into_a_valid_time_table_populates_both_axes_and_reads_filter_on_either() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(1_000);
    let mut writer = ValidTimeWriter::new(clock, true);
    let key = BusinessKey::new(b"emp-1".to_vec());

    // The fact is true in the world over [100, 200); the database records it now.
    let sys_from = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(interval(100, 200)),
            Some(b"role=ic".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();

    // --- Filter on the SYSTEM axis: range_scan resolves the live version. ---
    let live = delta
        .range_scan(key.clone()..=key, Snapshot(SystemTimeMicros(1_500)), &index)
        .unwrap();
    assert_eq!(live.len(), 1, "exactly one version live at the snapshot");
    let stored = &live[0];
    assert_eq!(stored.sys_from, sys_from);
    assert_eq!(
        stored.sys_to, SYSTEM_TIME_OPEN,
        "current version stays open"
    );

    // --- Filter on the VALID axis: recover the interval and test membership. ---
    let (valid, user) = unframe_payload(true, stored.payload.as_deref().unwrap()).unwrap();
    let valid = valid.expect("valid-time table carries an interval");
    assert_eq!(valid, interval(100, 200));
    assert_eq!(user, b"role=ic", "user payload survives the framing");
    assert!(valid.contains(vt(150)), "true in the world at t=150");
    assert!(
        !valid.contains(vt(200)),
        "half-open: not true at the end point"
    );
    assert!(!valid.contains(vt(50)), "not yet true at t=50");
}

#[test]
fn update_opens_a_new_valid_period_and_the_superseded_one_keeps_its_interval() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(1_000);
    let mut writer = ValidTimeWriter::new(clock.clone(), true);
    let key = BusinessKey::new(b"emp-1".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(interval(100, 200)),
            Some(b"role=ic".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    clock.set(2_000);
    // A correction: the world-truth changes; the new fact is open-ended.
    writer
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(interval(200, i64::MAX)),
            Some(b"role=lead".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .unwrap();

    let mut chains = drain_chains(&mut delta, &index);
    let versions = chains.remove(&key).expect("key has a chain");
    assert_eq!(versions.len(), 2, "insert + update ⇒ two versions");

    // System axis: the prior period is closed and abuts the new open one.
    assert_ne!(versions[0].sys_to, SYSTEM_TIME_OPEN);
    assert_eq!(versions[0].sys_to, versions[1].sys_from, "abut, no gap");
    assert_eq!(versions[1].sys_to, SYSTEM_TIME_OPEN);

    // Valid axis: the superseded version keeps its original interval; corrections
    // append rather than mutate.
    let (closed_valid, closed_user) =
        unframe_payload(true, versions[0].payload.as_deref().unwrap()).unwrap();
    assert_eq!(closed_valid, Some(interval(100, 200)));
    assert_eq!(closed_user, b"role=ic");

    let (open_valid, open_user) =
        unframe_payload(true, versions[1].payload.as_deref().unwrap()).unwrap();
    assert_eq!(
        open_valid,
        Some(ValidInterval::new(vt(200), VALID_TIME_OPEN).unwrap())
    );
    assert_eq!(open_user, b"role=lead");
}

#[test]
fn delete_closes_the_system_period_and_preserves_the_valid_interval() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(10);
    let mut writer = ValidTimeWriter::new(clock, true);
    let key = BusinessKey::new(b"emp-1".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(interval(100, 200)),
            Some(b"role=ic".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();
    let closed_at = writer
        .delete(
            &mut delta,
            &mut index,
            &EmptySealed,
            &key,
            TxnId(7),
            Principal::new(b"deleter".to_vec()),
        )
        .unwrap();

    // Nothing is live after the delete on the system axis.
    let live = delta
        .range_scan(.., Snapshot(SystemTimeMicros(i64::MAX - 1)), &index)
        .unwrap();
    assert!(live.is_empty(), "deleted key has no live version");

    let mut chains = drain_chains(&mut delta, &index);
    let versions = chains.remove(&key).expect("key has a chain");
    assert_eq!(versions.len(), 1);
    assert_eq!(
        versions[0].sys_to, closed_at,
        "delete closes the system period"
    );
    // The valid interval the row was written with is untouched by the delete.
    let (valid, _) = unframe_payload(true, versions[0].payload.as_deref().unwrap()).unwrap();
    assert_eq!(valid, Some(interval(100, 200)));
    // The delete's identity is recorded as close-provenance on the system axis,
    // forwarded verbatim through the valid-time writer (STL-118).
    assert_eq!(
        versions[0].closed_by,
        Some(Provenance::new(
            TxnId(7),
            closed_at,
            Principal::new(b"deleter".to_vec())
        )),
        "valid-time delete still records who closed the period"
    );
}

// --- Per-table policy -------------------------------------------------------

#[test]
fn valid_time_table_requires_an_interval_on_every_write() {
    let mut delta = new_delta();
    let mut index = new_index();
    let mut writer = ValidTimeWriter::new(StubClock::new(1), true);
    let key = BusinessKey::new(b"k".to_vec());

    let err = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            None,
            Some(b"x".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap_err();
    assert!(matches!(err, ValidTimeError::ValidTimeRequired));
}

#[test]
fn system_only_table_rejects_a_supplied_interval() {
    let mut delta = new_delta();
    let mut index = new_index();
    let mut writer = ValidTimeWriter::new(StubClock::new(1), false);
    let key = BusinessKey::new(b"k".to_vec());

    let err = writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            Some(interval(1, 2)),
            Some(b"x".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap_err();
    assert!(matches!(err, ValidTimeError::ValidTimeNotSupported));
}

#[test]
fn system_only_table_stores_payload_with_no_prefix() {
    let mut delta = new_delta();
    let mut index = new_index();
    let clock = StubClock::new(500);
    let mut writer = ValidTimeWriter::new(clock, false);
    let key = BusinessKey::new(b"k".to_vec());

    writer
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            Some(b"plain".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .unwrap();

    let live = delta
        .range_scan(key.clone()..=key, Snapshot(SystemTimeMicros(600)), &index)
        .unwrap();
    assert_eq!(live.len(), 1);
    // Stored verbatim — no 16-byte interval prefix on a system-only table.
    assert_eq!(live[0].payload.as_deref(), Some(&b"plain"[..]));
    let (valid, user) = unframe_payload(false, live[0].payload.as_deref().unwrap()).unwrap();
    assert_eq!(valid, None);
    assert_eq!(user, b"plain");
}
