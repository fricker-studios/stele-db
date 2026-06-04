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

/// A `Waker` that counts how many times it was woken — used by tests that
/// need to assert "each task got notified", not just "n wakeups happened".
struct CountingWaker {
    woken: std::sync::atomic::AtomicUsize,
}
impl CountingWaker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            woken: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    fn count(&self) -> usize {
        self.woken.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.woken.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
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

/// Two `commit()` futures targeting the **same** `LogOffset` must both be
/// woken on tick — earlier wakers must not be overwritten.
#[test]
fn multiple_commits_at_same_offset_both_resolve() {
    let disk = MemDisk::new();
    let wal = Wal::open(disk, WalConfig::default()).unwrap();

    let pos = wal.append(b"shared").unwrap();
    let mut first = Box::pin(wal.commit(pos));
    let mut second = Box::pin(wal.commit(pos));

    // Two distinct tasks → two distinct wakers. Use CountingWakers so we can
    // assert that *each one specifically* is woken exactly once, not just
    // that `tick()` returned a count of 2.
    let first_cw = CountingWaker::new();
    let second_cw = CountingWaker::new();
    let first_waker = Waker::from(first_cw.clone());
    let second_waker = Waker::from(second_cw.clone());
    let mut cx_a = Context::from_waker(&first_waker);
    let mut cx_b = Context::from_waker(&second_waker);
    assert!(first.as_mut().poll(&mut cx_a).is_pending());
    assert!(second.as_mut().poll(&mut cx_b).is_pending());

    let woken = wal.tick().unwrap();
    assert_eq!(
        woken, 2,
        "tick must wake both futures sharing the same target"
    );
    assert_eq!(
        first_cw.count(),
        1,
        "first task's waker must fire exactly once"
    );
    assert_eq!(
        second_cw.count(),
        1,
        "second task's waker must fire exactly once"
    );

    block_on(first).unwrap();
    block_on(second).unwrap();
}

/// Segment rotation fsyncs the closing segment, so a `commit()` whose target
/// lies in that segment must resolve as soon as rotation happens — without
/// waiting for the next explicit `tick()`.
#[test]
fn rotation_resolves_commits_in_closing_segment() {
    let disk = MemDisk::new();
    // 80-byte segments → 5 records per segment (16 bytes each).
    let wal = Wal::open(
        disk,
        WalConfig {
            segment_size_bytes: 80,
        },
    )
    .unwrap();

    // Fill segment 0. Take a commit on the last record before the boundary.
    let mut last_in_seg0 = LogOffset::ZERO;
    for i in 0u64..5 {
        last_in_seg0 = wal.append(&i.to_le_bytes()).unwrap();
    }
    let mut commit = Box::pin(wal.commit(last_in_seg0));
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    assert!(
        commit.as_mut().poll(&mut cx).is_pending(),
        "no fsync has happened yet"
    );

    // Now append one more — this forces a rotation, which fsyncs segment 0.
    let _ = wal.append(&5u64.to_le_bytes()).unwrap();

    // The commit's target was in the (now closed and fsync'd) segment, so it
    // must be ready without us calling `tick()` first.
    assert!(matches!(commit.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
}

/// `replay_from(...)` surfaces I/O failures from disk-listing as the first
/// iterator item (then stops) — never silently as "no records".
#[test]
fn replay_surfaces_disk_list_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Wraps a `MemDisk` and errors from `list()` after the first call,
    /// modeling a transient FS failure (e.g. EIO mid-replay).
    struct ExplodingList {
        inner: MemDisk,
        list_calls: AtomicUsize,
    }
    struct ExplodingFile(<MemDisk as Disk>::File);

    impl Disk for ExplodingList {
        type File = ExplodingFile;
        fn create(&self, name: &str) -> io::Result<Self::File> {
            self.inner.create(name).map(ExplodingFile)
        }
        fn open(&self, name: &str) -> io::Result<Self::File> {
            self.inner.open(name).map(ExplodingFile)
        }
        fn list(&self) -> io::Result<Vec<String>> {
            // First call (during `Wal::open`) succeeds. Replay's own
            // `known_segments` is the second call — fail it.
            let n = self.list_calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                self.inner.list()
            } else {
                Err(io::Error::other("boom"))
            }
        }
        fn remove(&self, name: &str) -> io::Result<()> {
            self.inner.remove(name)
        }
    }
    impl DiskFile for ExplodingFile {
        fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.0.append(bytes)
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read_at(offset, buf)
        }
        fn sync(&mut self) -> io::Result<()> {
            self.0.sync()
        }
        fn len(&self) -> u64 {
            self.0.len()
        }
    }

    let disk = ExplodingList {
        inner: MemDisk::new(),
        list_calls: AtomicUsize::new(0),
    };
    let wal = Wal::open(disk, WalConfig::default()).unwrap();
    wal.append(b"one").unwrap();
    wal.tick().unwrap();

    let mut it = wal.replay_from(Checkpoint::BEGIN);
    let first = it.next().expect("must yield error");
    assert!(
        first.is_err(),
        "first item should surface the listing error"
    );
    assert!(it.next().is_none(), "iterator stops after the error");
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
