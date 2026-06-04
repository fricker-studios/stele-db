//! Delta-tier integration tests.
//!
//! Scope (STL-87):
//!
//! * snapshot semantics — DoD bullet 2 (a read at snapshot `s` returns the
//!   latest version per key with `sys` interval ∋ `s`);
//! * spill round-trip — bytes above `spill_threshold_bytes` move to disk and
//!   read back identically;
//! * crash-replay equivalence — DoD bullet 1 (delta + WAL replay reproduces
//!   the pre-crash state for every sim seed).
//!
//! The crash-replay sweep iterates a hundred deterministic seeds and asserts
//! the post-replay delta and the pre-crash delta produce identical range
//! scans at multiple snapshots. This is the shape `stele-sim` will eventually
//! drive; it lives here as an integration test until the sim harness lands
//! the storage/txn scenarios.

#![allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::needless_collect,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::wal::{Checkpoint, Disk, DiskFile, Wal, WalConfig};

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

// --- Test helpers -----------------------------------------------------------

fn version(key: &[u8], sys_from: i64, sys_to: SystemTimeMicros, payload: &[u8]) -> Version {
    Version {
        business_key: BusinessKey::new(key.to_vec()),
        sys_from: SystemTimeMicros(sys_from),
        sys_to,
        payload: payload.to_vec(),
    }
}

/// Tiny xorshift64* — deterministic, no dependency, plenty good for randomized
/// scenario generation. Seeded from a u64 so a failing seed is a number we
/// can pass back in.
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        // Avoid the zero fixpoint that traps a bare xorshift.
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

// --- Snapshot semantics -----------------------------------------------------

/// DoD bullet 2: a read at snapshot `s` returns the latest version per key
/// with `sys` interval ∋ `s`.
#[test]
fn snapshot_returns_latest_live_version_per_key() {
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).unwrap();

    // Key "a": closed at sys=10, reopened, closed at sys=20, then open
    // forever from sys=20.
    delta
        .insert(version(b"a", 0, SystemTimeMicros(10), b"v0"))
        .unwrap();
    delta
        .insert(version(b"a", 10, SystemTimeMicros(20), b"v1"))
        .unwrap();
    delta
        .insert(version(b"a", 20, SYSTEM_TIME_OPEN, b"v2"))
        .unwrap();
    // Key "b": one version, still open from sys=15.
    delta
        .insert(version(b"b", 15, SYSTEM_TIME_OPEN, b"only"))
        .unwrap();

    let at = |s: i64| delta.range_scan(.., Snapshot(SystemTimeMicros(s))).unwrap();

    // Before "b" was written, "b" should be absent.
    let s5 = at(5);
    assert_eq!(s5.len(), 1);
    assert_eq!(s5[0].business_key.as_bytes(), b"a");
    assert_eq!(s5[0].payload, b"v0");

    // At s=15, both keys are live: "a" on v1, "b" on its only version.
    let s15 = at(15);
    assert_eq!(s15.len(), 2);
    assert_eq!(s15[0].business_key.as_bytes(), b"a");
    assert_eq!(s15[0].payload, b"v1");
    assert_eq!(s15[1].business_key.as_bytes(), b"b");
    assert_eq!(s15[1].payload, b"only");

    // At s=25, "a" should be on v2.
    let s25 = at(25);
    assert_eq!(s25[0].payload, b"v2");
}

/// `[sys_from, sys_to)` is half-open: a version closed at exactly `s` is not
/// live at `s`.
#[test]
fn half_open_period_excludes_sys_to_at_snapshot() {
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).unwrap();
    delta
        .insert(version(b"k", 0, SystemTimeMicros(10), b"closed"))
        .unwrap();
    let live = delta
        .range_scan(.., Snapshot(SystemTimeMicros(10)))
        .unwrap();
    assert!(live.is_empty(), "[0,10) must not be live at s=10");
}

// --- Spill round-trip -------------------------------------------------------

/// Bytes above the configured threshold land on a spill file; reads return
/// the same versions as if the writes had stayed in memory.
#[test]
fn spill_round_trips_versions_to_disk_and_back() {
    let disk = MemDisk::new();
    // Tiny threshold so a handful of inserts triggers a spill.
    let mut delta = Delta::open(
        disk.clone(),
        DeltaConfig {
            spill_threshold_bytes: 128,
        },
    )
    .unwrap();

    let mut written: Vec<Version> = Vec::new();
    for i in 0u64..50 {
        let key = format!("k-{i:04}");
        let v = version(
            key.as_bytes(),
            i as i64,
            SYSTEM_TIME_OPEN,
            format!("payload-{i}").as_bytes(),
        );
        written.push(v.clone());
        delta.insert(v).unwrap();
    }
    assert!(
        delta.is_spilled(),
        "50 records past a 128B threshold must spill"
    );
    assert!(
        disk.list()
            .unwrap()
            .iter()
            .filter(|n| n.starts_with("delta-spill-"))
            .count()
            >= 1,
        "at least one spill file should be on disk"
    );

    let live = delta
        .range_scan(.., Snapshot(SystemTimeMicros(i64::MAX - 1)))
        .unwrap();
    // Same multiset of payloads.
    let mut got_payloads: Vec<Vec<u8>> = live.iter().map(|v| v.payload.clone()).collect();
    let mut want_payloads: Vec<Vec<u8>> = written.iter().map(|v| v.payload.clone()).collect();
    got_payloads.sort();
    want_payloads.sort();
    assert_eq!(got_payloads, want_payloads, "spilled reads must round-trip");
}

/// Flush merges in-memory + spills into a single sorted sequence and removes
/// the spill files.
#[test]
fn flush_merges_memory_and_spills_then_clears() {
    let disk = MemDisk::new();
    let mut delta = Delta::open(
        disk.clone(),
        DeltaConfig {
            spill_threshold_bytes: 96,
        },
    )
    .unwrap();
    for i in 0u64..40 {
        delta
            .insert(version(
                format!("k-{i:04}").as_bytes(),
                i as i64,
                SYSTEM_TIME_OPEN,
                b"x",
            ))
            .unwrap();
    }
    assert!(delta.is_spilled());

    let drained = delta.flush_to_segment().unwrap();
    assert_eq!(drained.len(), 40);
    // Keys must come out sorted.
    let keys: Vec<&[u8]> = drained.iter().map(|v| v.business_key.as_bytes()).collect();
    for w in keys.windows(2) {
        assert!(w[0] < w[1], "flush output must be sorted by business_key");
    }
    assert_eq!(delta.byte_size(), 0);
    assert!(!delta.is_spilled());
    // No leftover spill files on disk.
    let remaining: Vec<String> = disk
        .list()
        .unwrap()
        .into_iter()
        .filter(|n| n.starts_with("delta-spill-"))
        .collect();
    assert!(remaining.is_empty(), "flush must remove spill files");
}

/// Stale spill files on `disk` at open time must not influence reads.
#[test]
fn open_discards_pre_existing_spill_files() {
    let disk = MemDisk::new();

    // Hand-craft a "stale" spill file with one row that doesn't belong to
    // any live workload, the way a crashed prior process might have left it.
    {
        let mut delta = Delta::open(
            disk.clone(),
            DeltaConfig {
                spill_threshold_bytes: 32,
            },
        )
        .unwrap();
        for i in 0u64..10 {
            delta
                .insert(version(
                    format!("stale-{i:02}").as_bytes(),
                    i as i64,
                    SYSTEM_TIME_OPEN,
                    b"stale",
                ))
                .unwrap();
        }
        assert!(delta.is_spilled());
        // Drop `delta` without flushing — simulates a crash that leaves spill
        // files on disk.
    }
    assert!(
        disk.list()
            .unwrap()
            .iter()
            .any(|n| n.starts_with("delta-spill-")),
        "precondition: spill files should exist on disk"
    );

    // A fresh Delta::open must wipe them.
    let delta = Delta::open(disk.clone(), DeltaConfig::default()).unwrap();
    let live = delta.range_scan(.., Snapshot(SYSTEM_TIME_OPEN)).unwrap();
    assert!(
        live.is_empty(),
        "freshly-opened delta must not surface stale spill rows"
    );
    let remaining: Vec<String> = disk
        .list()
        .unwrap()
        .into_iter()
        .filter(|n| n.starts_with("delta-spill-"))
        .collect();
    assert!(
        remaining.is_empty(),
        "Delta::open must remove pre-existing spill files"
    );
}

// --- WAL replay equivalence -------------------------------------------------

/// Drive a randomised insert workload, encode each version into the WAL,
/// snapshot the pre-crash delta's read state, then drop the delta and rebuild
/// it from WAL replay. The post-replay delta must produce identical range
/// scans at every probed snapshot. This is DoD bullet 1, exercised across a
/// sweep of deterministic seeds.
#[test]
fn delta_plus_wal_replay_reproduces_pre_crash_state_under_seed_sweep() {
    for seed in 0u64..100 {
        let mut rng = Rng::new(seed);
        let wal_disk = MemDisk::new();
        let spill_disk = MemDisk::new();
        let wal = Wal::open(
            wal_disk.clone(),
            WalConfig {
                segment_size_bytes: 1024,
            },
        )
        .unwrap();

        // Smaller threshold than the workload total → spill at least once
        // for most seeds. Reads still must equal the pre-crash state.
        let mut pre = Delta::open(
            spill_disk.clone(),
            DeltaConfig {
                spill_threshold_bytes: 256,
            },
        )
        .unwrap();

        // Generate a workload: random business keys (drawn from a small
        // pool so we get version chains per key) with monotonically
        // increasing sys_from.
        const KEY_POOL: usize = 8;
        let mut sys = 1i64;
        let record_count = 20 + rng.range(40) as usize;
        for _ in 0..record_count {
            let key_idx = rng.range(KEY_POOL as u64) as usize;
            let key = format!("k-{key_idx:02}");
            // Close prior versions implicitly: each new write opens at the
            // current sys clock; tests on the read path check half-open
            // resolution. Half the time, leave sys_to open; the rest, write
            // an already-closed version (gap in coverage) to stress the
            // resolver.
            let close_now = rng.next_u64() & 1 == 0;
            let sys_to = if close_now {
                SystemTimeMicros(sys + 1)
            } else {
                SYSTEM_TIME_OPEN
            };
            let payload = format!("seed{seed}-{sys}").into_bytes();
            let v = version(key.as_bytes(), sys, sys_to, &payload);
            // Append the encoded version to the WAL — this is what the txn
            // manager will do at commit time once [STL-94] lands.
            wal.append(&v.encoded()).unwrap();
            pre.insert(v).unwrap();
            sys += 1 + rng.range(5) as i64;
        }
        wal.tick().unwrap();

        // Probe several snapshots covering the workload's time range.
        let probes: Vec<Snapshot> = (0..6)
            .map(|i| Snapshot(SystemTimeMicros(i * sys / 5)))
            .collect();
        let before: Vec<Vec<Version>> = probes
            .iter()
            .map(|s| pre.range_scan(.., *s).unwrap())
            .collect();

        // "Crash": drop `pre`. Open a fresh delta on a fresh spill disk
        // (mirrors a clean restart where the spill dir is empty), then
        // replay the WAL.
        drop(pre);
        let fresh_spill_disk = MemDisk::new();
        let mut post = Delta::open(
            fresh_spill_disk,
            DeltaConfig {
                spill_threshold_bytes: 256,
            },
        )
        .unwrap();
        for record in wal.replay_from(Checkpoint::BEGIN) {
            let bytes = record.expect("clean WAL replay");
            let (v, consumed) = Version::decode(&bytes).expect("decode replay record");
            assert_eq!(consumed, bytes.len(), "WAL record == one version frame");
            post.insert(v).unwrap();
        }

        let after: Vec<Vec<Version>> = probes
            .iter()
            .map(|s| post.range_scan(.., *s).unwrap())
            .collect();

        assert_eq!(
            before, after,
            "seed {seed}: post-replay delta must produce identical range scans"
        );
    }
}
