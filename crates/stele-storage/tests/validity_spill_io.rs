//! Validity-index spill read-path I/O counters ([STL-142]).
//!
//! Once the validity index spills, a **point** or small-key lookup must not scan
//! every spill end-to-end. A [`CountingDisk`] (counting `read_at` calls — each
//! [`read_spill`] of a non-empty spill is exactly one) proves it:
//!
//! * a point [`close_of`](ValidityIndex::close_of) reads only the one spill that
//!   can hold the key (`1`), not all of them;
//! * a full [`materialize`](ValidityIndex::materialize) is the linear baseline it
//!   improves on — it reads every spill;
//! * a small [`fold_chains`] reads only the matching spills, while a fold over
//!   every key falls back to the single full sweep (no read amplification).
//!
//! This mirrors the zone-map I/O-counter tests for the segment prune.

#![allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;
use stele_storage::delta::{BusinessKey, Version};
use stele_storage::merge::fold_chains;
use stele_storage::validity::{Close, ValidityConfig, ValidityIndex};
use stele_storage::wal::{Disk, DiskFile};

// --- CountingDisk: a heap disk that counts read_at calls --------------------

#[derive(Default, Clone)]
struct CountingDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
    reads: Arc<AtomicU64>,
}

impl CountingDisk {
    fn new() -> Self {
        Self::default()
    }

    fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }

    fn reset_reads(&self) {
        self.reads.store(0, Ordering::SeqCst);
    }
}

struct CountingFile {
    bytes: Arc<Mutex<Vec<u8>>>,
    reads: Arc<AtomicU64>,
}

impl Disk for CountingDisk {
    type File = CountingFile;

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
        Ok(CountingFile {
            bytes,
            reads: Arc::clone(&self.reads),
        })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(CountingFile {
            bytes,
            reads: Arc::clone(&self.reads),
        })
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

impl DiskFile for CountingFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.reads.fetch_add(1, Ordering::SeqCst);
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

// --- Helpers ----------------------------------------------------------------

fn bk(key: &str) -> BusinessKey {
    BusinessKey::new(key.as_bytes().to_vec())
}

/// A fixed-width key so every close encodes to the same size and the spill
/// threshold lands deterministically.
fn key_at(i: usize) -> String {
    format!("k{i:03}")
}

fn close(key: &str, sys_from: i64) -> Close {
    Close {
        business_key: bk(key),
        sys_from: SystemTimeMicros(sys_from),
        seq: 0,
        sys_to: SystemTimeMicros(sys_from + 10),
        closed_by: Provenance::new(
            TxnId(1),
            SystemTimeMicros(sys_from),
            Principal::new(b"p".to_vec()),
        ),
    }
}

fn open_version(key: &str, sys_from: i64) -> Version {
    Version::open(
        bk(key),
        SystemTimeMicros(sys_from),
        0,
        Provenance::new(
            TxnId(1),
            SystemTimeMicros(sys_from),
            Principal::new(b"birth".to_vec()),
        ),
        b"body".to_vec(),
    )
}

/// Build a validity index whose closes spill two keys per file. Returns the disk
/// (for read counting), the index, and the spill count. Keys `k000..k{n-1}` are
/// inserted in sorted order, so each spill covers a disjoint contiguous range.
fn spilled_index(n: usize) -> (CountingDisk, ValidityIndex<CountingDisk>, usize) {
    // Two closes per spill: threshold = 2 × one close's encoded size.
    let unit = close(&key_at(0), 10).encoded_size() as u64;
    let disk = CountingDisk::new();
    let mut idx = ValidityIndex::open(
        disk.clone(),
        ValidityConfig {
            spill_threshold_bytes: 2 * unit,
        },
    )
    .expect("open");
    for i in 0..n {
        idx.insert_close(close(&key_at(i), 10)).expect("insert");
    }
    let spills = idx.spill_count();
    (disk, idx, spills)
}

// --- DoD: point lookups are sub-linear in the spilled close count -----------

#[test]
fn point_close_of_reads_only_the_matching_spill() {
    let (disk, idx, spills) = spilled_index(40);
    assert!(spills >= 8, "expected many spills, got {spills}");

    // A spilled key: exactly one spill can hold it.
    disk.reset_reads();
    let got = idx
        .close_of(&bk(&key_at(4)), SystemTimeMicros(10), 0)
        .expect("lookup");
    assert_eq!(got.unwrap().sys_to, SystemTimeMicros(20));
    assert_eq!(
        disk.reads(),
        1,
        "point lookup reads one spill, not {spills}"
    );
    assert!(disk.reads() < spills as u64, "sub-linear in spilled closes");
}

#[test]
fn point_close_of_for_an_absent_key_reads_nothing() {
    let (disk, idx, _spills) = spilled_index(40);
    // Out of every spill's key range → pruned without a single read.
    disk.reset_reads();
    let got = idx
        .close_of(&bk("zzz9"), SystemTimeMicros(10), 0)
        .expect("lookup");
    assert!(got.is_none());
    assert_eq!(disk.reads(), 0, "an absent key reads no spill");
}

#[test]
fn materialize_is_the_linear_baseline() {
    let (disk, idx, spills) = spilled_index(40);
    disk.reset_reads();
    let all = idx.materialize().expect("materialize");
    assert_eq!(all.len(), 40);
    assert_eq!(
        disk.reads(),
        spills as u64,
        "a full sweep reads every spill — the cost a point lookup avoids",
    );
}

#[test]
fn small_fold_reads_only_matching_spills_large_fold_sweeps() {
    let (disk, idx, spills) = spilled_index(40);

    // Small fold: one key → one matching spill.
    disk.reset_reads();
    let chains = fold_chains(vec![open_version(&key_at(4), 10)], &idx).expect("fold");
    let v = &chains[&bk(&key_at(4))][&(SystemTimeMicros(10), 0)];
    assert_eq!(
        v.sys_to,
        SystemTimeMicros(20),
        "close overlaid from the index"
    );
    assert_eq!(
        disk.reads(),
        1,
        "small fold touches one spill, not {spills}"
    );

    // Whole-key fold: spans every spill → the single full sweep, no amplification.
    disk.reset_reads();
    let versions: Vec<Version> = (0..40).map(|i| open_version(&key_at(i), 10)).collect();
    let chains = fold_chains(versions, &idx).expect("fold");
    assert_eq!(chains.len(), 40);
    assert_eq!(
        disk.reads(),
        spills as u64,
        "a fold over every key sweeps once, never re-reading per key",
    );
}
