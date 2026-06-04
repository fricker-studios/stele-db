//! Backend conformance suite ([STL-90], DoD bullet 1).
//!
//! The whole point of the [`Disk`] seam is that storage code does not care
//! which backend it runs on. This file encodes that as an executable promise:
//! one generic test body, [`run_disk_contract`], plus one generic
//! [`segment_round_trip`], are each run **unchanged** against both shipped
//! backends — [`LocalDisk`] (real filesystem, in a temp dir) and [`MemDisk`]
//! (heap-backed). If a behaviour differs between them, one of these tests fails.
//!
//! The temp-dir helper avoids a `tempfile` dependency: a unique directory under
//! the OS temp dir, removed on drop. Uniqueness comes from the pid plus a
//! process-local counter — no wall-clock or RNG, so it stays sim-friendly.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
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
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("stele-backend-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
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

// --- the generic contract ---------------------------------------------------

/// Exercises every guarantee in the [`Disk`] / [`DiskFile`] contract. Generic
/// over the backend, so the assertions are literally identical for `local` and
/// `memory`.
fn run_disk_contract<D: Disk>(disk: &D)
where
    D::File: std::fmt::Debug,
{
    // create → append → read back through the same handle.
    let mut f = disk.create("alpha").expect("create alpha");
    assert!(f.is_empty());
    f.append(b"hello").expect("append");
    assert_eq!(f.len(), 5, "len tracks appended bytes before sync");

    let mut buf = [0u8; 8];
    let n = f.read_at(0, &mut buf).expect("read_at 0");
    assert_eq!(
        &buf[..n],
        b"hello",
        "read sees appended (not-yet-synced) bytes"
    );

    // Short read at EOF: a read straddling the end returns only what exists.
    let n = f.read_at(3, &mut buf).expect("read_at near EOF");
    assert_eq!(&buf[..n], b"lo");
    // A read fully past EOF returns 0, never an error.
    assert_eq!(f.read_at(100, &mut buf).expect("read past EOF"), 0);

    // sync is the durability point; it must succeed on a healthy backend.
    f.append(b" world").expect("append more");
    f.sync().expect("sync");
    assert_eq!(f.len(), 11);
    drop(f);

    // Persistence across handles: a fresh open sees the synced bytes and
    // reports the right length.
    let mut reopened = disk.open("alpha").expect("reopen alpha");
    assert_eq!(
        reopened.len(),
        11,
        "reopened length comes from the backing store"
    );
    let n = reopened.read_at(0, &mut buf).expect("read reopened");
    assert_eq!(&buf[..n], b"hello wo");

    // Append-after-reopen continues at end-of-file.
    reopened.append(b"!").expect("append after reopen");
    assert_eq!(reopened.len(), 12);
    let mut tail = [0u8; 1];
    assert_eq!(reopened.read_at(11, &mut tail).expect("read tail"), 1);
    assert_eq!(&tail, b"!");
    drop(reopened);

    // create is exclusive: a second create of the same name is AlreadyExists.
    let err = disk.create("alpha").expect_err("create existing must fail");
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    // open of a missing file is NotFound.
    let err = disk.open("ghost").expect_err("open missing must fail");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    // list reflects what exists (order is unspecified — sort before asserting).
    disk.create("beta").expect("create beta");
    let mut names = disk.list().expect("list");
    names.sort();
    assert_eq!(names, vec!["alpha".to_owned(), "beta".to_owned()]);

    // remove deletes; removing a missing file is NotFound.
    disk.remove("beta").expect("remove beta");
    assert_eq!(
        disk.list().expect("list after remove"),
        vec!["alpha".to_owned()]
    );
    let err = disk.remove("beta").expect_err("remove missing must fail");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn local_backend_satisfies_contract() {
    let tmp = TempDir::new();
    let disk = LocalDisk::open(tmp.path()).expect("open LocalDisk");
    run_disk_contract(&disk);
}

#[test]
fn memory_backend_satisfies_contract() {
    let disk = MemDisk::new();
    run_disk_contract(&disk);
}

// --- a real storage test, run unchanged on both backends --------------------

fn sample_versions() -> Vec<Version> {
    let blob = vec![0x5Au8; 4096];
    vec![
        Version {
            business_key: BusinessKey::new(b"a".to_vec()),
            sys_from: SystemTimeMicros(10),
            sys_to: SystemTimeMicros(20),
            payload: b"a-v0".to_vec(),
        },
        Version {
            business_key: BusinessKey::new(b"a".to_vec()),
            sys_from: SystemTimeMicros(20),
            sys_to: SYSTEM_TIME_OPEN,
            payload: b"a-v1".to_vec(),
        },
        Version {
            business_key: BusinessKey::new(b"big".to_vec()),
            sys_from: SystemTimeMicros(1),
            sys_to: SYSTEM_TIME_OPEN,
            payload: blob,
        },
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

// --- fault injection is memory-only ----------------------------------------

#[test]
fn memory_fault_injection_fires_in_scheduled_order() {
    let faults = Faults::new();
    // The next append fails, then the next sync fails — in that order.
    faults.schedule(FaultOp::Append, io::ErrorKind::Other);
    faults.schedule(FaultOp::Sync, io::ErrorKind::Other);
    let disk = MemDisk::with_faults(faults);
    assert_eq!(disk.faults().pending(), 2);

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
    assert_eq!(disk.faults().pending(), 0);
}
