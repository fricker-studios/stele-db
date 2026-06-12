//! Backend conformance ([STL-90] DoD bullet 1, [STL-232]).
//!
//! The generic contract lives in [`stele_storage::backend::conformance`] so
//! every backend — here `local` and `memory`, and the seeded `FaultDisk` in
//! `stele-sim` — runs the **identical** checks. This file drives the suite for
//! the two shipped backends and adds the storage-level round-trip (a sealed
//! segment written and read through each backend) plus the `memory` fault
//! schedule's own semantics.
//!
//! The temp-dir helper avoids a `tempfile` dependency: a unique directory under
//! the OS temp dir, removed on drop. Uniqueness comes from the pid plus a
//! process-local counter — no wall-clock or RNG, so it stays sim-friendly.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;
use stele_storage::backend::conformance;
use stele_storage::backend::{Disk, DiskFile, FaultOp, Faults, LocalDisk, MemDisk};
use stele_storage::delta::{BusinessKey, Version};
use stele_storage::segment::{SegmentReader, SegmentWriter};

// --- temp-dir RAII for the LocalDisk runs -----------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        // `create_dir` (not `create_dir_all`) so we never reuse a directory left
        // behind by a crashed run or a recycled PID — retry with a fresh counter
        // on the off chance the name already exists, guaranteeing an empty dir.
        loop {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("stele-backend-{}-{n}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self { path },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => panic!("create temp dir: {e}"),
            }
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// --- the shared contract, driven for both backends --------------------------

#[test]
fn local_backend_satisfies_contract() {
    let tmp = TempDir::new();
    let disk = LocalDisk::open(tmp.path()).expect("open LocalDisk");
    conformance::disk_contract(&disk);
}

#[test]
fn memory_backend_satisfies_contract() {
    conformance::disk_contract(&MemDisk::new());
}

#[test]
fn local_positioned_reads_never_move_the_append_cursor() {
    let tmp = TempDir::new();
    let disk = LocalDisk::open(tmp.path()).expect("open LocalDisk");
    conformance::positioned_reads_never_move_the_append_cursor(&disk);
}

#[test]
fn memory_positioned_reads_never_move_the_append_cursor() {
    conformance::positioned_reads_never_move_the_append_cursor(&MemDisk::new());
}

#[test]
fn local_backend_rejects_non_flat_names() {
    let tmp = TempDir::new();
    let disk = LocalDisk::open(tmp.path()).expect("open LocalDisk");
    conformance::rejects_non_flat_names(&disk);
}

#[test]
fn memory_backend_rejects_non_flat_names() {
    conformance::rejects_non_flat_names(&MemDisk::new());
}

// --- a real storage test, run unchanged on both backends --------------------

fn sample_versions() -> Vec<Version> {
    let blob = vec![0x5Au8; 4096];
    vec![
        // Segments store only *birth* state (v6, ADR-0023): the period end /
        // closer live in the validity index, not on the record. So every
        // version pushed into a segment is constructed open via `Version::open`,
        // and the round-trip asserts the birth fields survive on both backends.
        Version::open(
            BusinessKey::new(b"a".to_vec()),
            SystemTimeMicros(10),
            0,
            Provenance::new(
                TxnId(10),
                SystemTimeMicros(10),
                Principal::new(b"svc".to_vec()),
            ),
            Some(b"a-v0".to_vec()),
        ),
        Version::open(
            BusinessKey::new(b"a".to_vec()),
            SystemTimeMicros(20),
            0,
            Provenance::new(
                TxnId(20),
                SystemTimeMicros(20),
                Principal::new(b"svc".to_vec()),
            ),
            Some(b"a-v1".to_vec()),
        ),
        Version::open(
            BusinessKey::new(b"big".to_vec()),
            SystemTimeMicros(1),
            0,
            Provenance::new(
                TxnId(1),
                SystemTimeMicros(1),
                Principal::new(b"svc".to_vec()),
            ),
            Some(blob),
        ),
    ]
}

/// Write a sealed segment and read every version back. This is the segment
/// round-trip from `tests/segment.rs`, but generic over the backend — proof
/// that higher-level storage code runs unchanged on `local` and `memory`.
fn segment_round_trip<D: Disk>(disk: &D) {
    let versions = sample_versions();
    let mut writer = SegmentWriter::create(disk, "seg-0001").expect("create segment");
    for v in &versions {
        writer.push(v.clone()).expect("push");
    }
    writer.finish().expect("finish");

    let reader = SegmentReader::open(disk, "seg-0001").expect("open segment");
    assert_eq!(reader.row_count(), versions.len() as u64);
    assert_eq!(reader.read_versions().expect("read_versions"), versions);
}

#[test]
fn segment_round_trip_local() {
    let tmp = TempDir::new();
    let disk = LocalDisk::open(tmp.path()).expect("open LocalDisk");
    segment_round_trip(&disk);
}

#[test]
fn segment_round_trip_memory() {
    segment_round_trip(&MemDisk::new());
}

// --- the memory backend's deterministic fault schedule -----------------------

#[test]
fn memory_fault_injection_fires_in_scheduled_order() {
    let faults = Faults::new();
    // The next append fails, then the next sync, then the next directory fence
    // — in that order.
    faults.schedule(FaultOp::Append, io::ErrorKind::Other);
    faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
    faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
    let disk = MemDisk::with_faults(faults);
    assert_eq!(disk.faults().pending(), 3);

    let mut f = disk.create("x").expect("create is not scheduled to fail");
    // First append hits the scheduled fault.
    let err = f.append(b"data").expect_err("scheduled append fault");
    assert_eq!(err.kind(), io::ErrorKind::Other);
    // The fault was consumed; the retry succeeds.
    f.append(b"data").expect("append after fault clears");
    // Next sync hits the second scheduled fault, then clears.
    assert_eq!(
        f.sync().expect_err("scheduled sync fault").kind(),
        io::ErrorKind::Other
    );
    f.sync().expect("sync after fault clears");
    // The directory fence is schedulable like any other op ([STL-232]).
    assert_eq!(
        disk.sync_dir().expect_err("scheduled fence fault").kind(),
        io::ErrorKind::Other
    );
    disk.sync_dir().expect("fence after fault clears");
    assert_eq!(disk.faults().pending(), 0);
}
