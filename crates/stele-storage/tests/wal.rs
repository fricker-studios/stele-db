//! WAL integration tests — exercises the durability contract end-to-end.
//!
//! Scope (STL-86):
//!
//! * append + replay round-trips a million records identically;
//! * group commit — many pending `commit` futures resolve on one `tick`;
//! * segment rotation respects the size cap;
//! * torn-write model: CRC detection stops replay, replay refuses to proceed.
//!
//! Uses an in-test `MemDisk` (intentionally private to this file): a real
//! filesystem-backed `Disk` lands with STL-90, and the seeded-fault virtual
//! disk lands with STL-109.

// `significant_drop_tightening` flags the MemDisk helpers — they're test-only
// in-memory plumbing, not a lock-contention concern. `cast_possible_truncation`
// is fine for `offset as usize` in test code that never holds files > usize.
// `type_complexity` allows the nested-Arc<Mutex<_>> MemDisk storage shape.
#![allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use stele_storage::wal::{Checkpoint, Disk, DiskFile, LogOffset, Wal, WalConfig};

// --- MemDisk: minimal in-memory Disk for tests ------------------------------

#[derive(Default, Clone)]
struct MemDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
}

impl MemDisk {
    fn new() -> Self {
        Self::default()
    }

    /// Truncate `name` to `new_len` bytes — used to simulate a torn-write tail.
    fn truncate(&self, name: &str, new_len: u64) {
        let files = self.inner.lock().unwrap();
        let f = files.get(name).expect("file");
        let mut bytes = f.lock().unwrap();
        bytes.truncate(new_len as usize);
    }

    /// Flip a single byte at `offset` in `name` — used to simulate mid-record
    /// bit-rot.
    fn flip_byte(&self, name: &str, offset: u64) {
        let files = self.inner.lock().unwrap();
        let f = files.get(name).expect("file");
        let mut bytes = f.lock().unwrap();
        bytes[offset as usize] ^= 0xFF;
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
        // In-memory: nothing to flush. The point of MemDisk is that `append`
        // is already visible; `sync` becoming a no-op is what makes the test
        // deterministic.
        Ok(())
    }

    fn len(&self) -> u64 {
        self.bytes.lock().unwrap().len() as u64
    }
}

// --- Test executor: spin-poll a single future without tokio -----------------

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(v) = future.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
}

// --- Tests ------------------------------------------------------------------

/// DoD bullet: append + replay round-trips a million records identically.
#[test]
fn million_record_round_trip() {
    let disk = MemDisk::new();
    let wal = Wal::open(
        disk,
        WalConfig {
            segment_size_bytes: 4 * 1024 * 1024, // force lots of rotations
        },
    )
    .unwrap();

    const N: u32 = 1_000_000;
    let mut last_pos = LogOffset::ZERO;
    for i in 0..N {
        last_pos = wal.append(&i.to_le_bytes()).unwrap();
    }
    wal.tick().unwrap();
    // Sanity: a final commit at `last_pos` resolves immediately after tick.
    block_on(wal.commit(last_pos)).unwrap();

    let mut count: u32 = 0;
    for record in wal.replay_from(Checkpoint::BEGIN) {
        let payload = record.expect("clean replay");
        assert_eq!(payload, count.to_le_bytes().to_vec(), "record {count}");
        count += 1;
    }
    assert_eq!(count, N);
}

/// DoD bullet: torn-write detection. A truncated tail causes the next replay to
/// surface one corruption error and *stop* — never silently returning a partial
/// record.
#[test]
fn torn_tail_truncation_is_detected_and_halts_replay() {
    let disk = MemDisk::new();
    let wal = Wal::open(disk.clone(), WalConfig::default()).unwrap();

    let mut positions = Vec::new();
    for i in 0u32..100 {
        positions.push(wal.append(&i.to_le_bytes()).unwrap());
    }
    wal.tick().unwrap();

    // Simulate a torn write: chop the last byte of the last record off the
    // tail of the active segment.
    let last_seg = positions.last().unwrap().segment_index;
    let last_end = positions.last().unwrap().byte_offset;
    let name = format!("wal-{last_seg:020}.log");
    disk.truncate(&name, last_end - 1);

    let mut clean = 0usize;
    let mut saw_err = false;
    for record in wal.replay_from(Checkpoint::BEGIN) {
        if record.is_err() {
            saw_err = true;
            break;
        }
        clean += 1;
    }
    assert_eq!(clean, 99, "the 99 records before the torn one replay clean");
    assert!(saw_err, "torn tail must surface as an error");

    // Replay iterator must refuse to yield further records after the error —
    // even on a fresh iterator instance, which independently re-detects the
    // corruption at the same offset.
    let mut second = wal.replay_from(Checkpoint::BEGIN);
    let mut got = 0;
    let mut hit_err = false;
    for item in &mut second {
        if item.is_err() {
            hit_err = true;
            assert!(second.next().is_none(), "iterator stops after corruption");
            break;
        }
        got += 1;
    }
    assert_eq!(got, 99);
    assert!(hit_err);
}

/// DoD bullet: CRC catches a flipped byte mid-record (not just truncation).
#[test]
fn flipped_byte_inside_record_is_detected() {
    let disk = MemDisk::new();
    let wal = Wal::open(disk.clone(), WalConfig::default()).unwrap();
    for i in 0u32..50 {
        wal.append(&i.to_le_bytes()).unwrap();
    }
    wal.tick().unwrap();

    // Flip a byte in the middle of the file — should land inside some
    // record's payload and fail its CRC.
    disk.flip_byte("wal-00000000000000000000.log", 64);

    let mut saw_err = false;
    for record in wal.replay_from(Checkpoint::BEGIN) {
        if record.is_err() {
            saw_err = true;
            break;
        }
    }
    assert!(saw_err, "bit-rot must surface as an error");
}

/// DoD piece: `commit()` is a future; many pending commits resolve in one tick.
#[test]
fn group_commit_resolves_many_futures_in_one_tick() {
    let disk = MemDisk::new();
    let wal = Wal::open(disk, WalConfig::default()).unwrap();

    let mut commits = Vec::new();
    for i in 0u32..32 {
        let pos = wal.append(&i.to_le_bytes()).unwrap();
        commits.push(Box::pin(wal.commit(pos)));
    }

    // Poll each future once so it registers its waker with the WAL — futures
    // don't register on construction (that's lazy by design).
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    for c in &mut commits {
        assert!(
            c.as_mut().poll(&mut cx).is_pending(),
            "no tick yet — should not be ready"
        );
    }

    let woken = wal.tick().unwrap();
    assert_eq!(woken, 32, "one tick should wake every pending commit");

    for c in commits {
        block_on(c).unwrap();
    }
}

/// Rotation must produce distinct segment files and replay across them
/// transparently.
#[test]
fn segment_rotation_keeps_records_replayable() {
    let disk = MemDisk::new();
    // 96-byte segments: with 8-byte headers + 8-byte payloads = 16 bytes per
    // record, we get exactly 6 records per segment.
    let wal = Wal::open(
        disk.clone(),
        WalConfig {
            segment_size_bytes: 96,
        },
    )
    .unwrap();

    for i in 0u64..30 {
        wal.append(&i.to_le_bytes()).unwrap();
    }
    wal.tick().unwrap();

    let mut segments = disk.list().unwrap();
    segments.sort();
    assert_eq!(segments.len(), 5, "30 records / 6-per-segment = 5 files");

    let payloads: Vec<u64> = wal
        .replay_from(Checkpoint::BEGIN)
        .map(|r| {
            let bytes = r.expect("clean replay");
            u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
        })
        .collect();
    assert_eq!(payloads, (0u64..30).collect::<Vec<_>>());
}

/// `replay_from(Checkpoint)` skips records ≤ the checkpoint.
#[test]
fn replay_from_checkpoint_skips_prefix() {
    let disk = MemDisk::new();
    let wal = Wal::open(disk, WalConfig::default()).unwrap();

    let mut positions = Vec::new();
    for i in 0u32..10 {
        positions.push(wal.append(&i.to_le_bytes()).unwrap());
    }
    wal.tick().unwrap();

    // Resume after record index 4 (so records 5..10 should replay).
    let checkpoint = Checkpoint(positions[4]);
    let got: Vec<u32> = wal
        .replay_from(checkpoint)
        .map(|r| {
            let b = r.unwrap();
            u32::from_le_bytes(b.try_into().unwrap())
        })
        .collect();
    assert_eq!(got, (5u32..10).collect::<Vec<_>>());
}
