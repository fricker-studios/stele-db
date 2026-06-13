//! `MemDisk` — a heap-backed [`Disk`] for tests and the simulation harness.
//!
//! Thread-safe and `Clone`-shareable: every clone of a `MemDisk` sees the same
//! files, and a [`MemFile`] handle reads and appends to shared bytes, so a
//! writer and a later re-`open`-ed reader observe one another exactly as they
//! would on a real disk. `sync` is a no-op — there is nothing to flush — which
//! makes the visible/durable distinction a *modelling* choice the
//! fault-injection hooks below can exploit.
//!
//! ## Fault injection (optional, deterministic)
//!
//! A [`Faults`] schedule lets a test or sim seed make specific operations fail
//! in a reproducible order — e.g. "the next `sync` returns `Other`" to model a
//! lost write. The schedule is a FIFO consulted per operation, so a given seed
//! always injects the same faults at the same points; there is no internal
//! randomness. One append fault carries a *torn* prefix
//! ([`Faults::schedule_torn_append`], [STL-299]): the append physically lands a
//! leading slice of the bytes passed to it before failing, modelling a partial
//! physical write the WAL must detect and poison on. (At this layer the appended
//! slice is the caller's raw bytes — for the WAL, the framed record's header +
//! payload — not an application-level "payload".) The richer seeded-fault virtual disk
//! (latency, reordering, bit-flips) is [STL-109] — this is the minimal seam it
//! builds on.
//!
//! [STL-299]: https://allegromusic.atlassian.net/browse/STL-299

use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};

use super::{Disk, DiskFile, validate_name};

/// The operation a scheduled [`Fault`] targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    /// [`Disk::create`].
    Create,
    /// [`Disk::open`].
    Open,
    /// [`DiskFile::append`].
    Append,
    /// [`DiskFile::read_at`].
    ReadAt,
    /// [`DiskFile::sync`].
    Sync,
    /// [`Disk::list`].
    List,
    /// [`Disk::remove`].
    Remove,
    /// [`Disk::sync_dir`].
    SyncDir,
}

/// One scheduled failure: the next time `op` runs, it returns an error of
/// [`ErrorKind`](io::ErrorKind) `kind` instead of succeeding.
#[derive(Debug, Clone, Copy)]
pub struct Fault {
    /// Which operation this fault fires on.
    pub op: FaultOp,
    /// The [`io::ErrorKind`] the failing operation reports.
    pub kind: io::ErrorKind,
    /// For a [`FaultOp::Append`] fault: how many leading bytes of the appended
    /// slice the append physically writes *before* failing — modelling a **torn**
    /// append (a partial physical write that then errors, [STL-299]). The slice is
    /// whatever the caller passed to [`DiskFile::append`] (for the WAL, a framed
    /// record — header + payload — not an application "payload"). `0` (the default for
    /// every fault scheduled via [`Faults::schedule`]) is a *clean* failure:
    /// nothing lands, so the WAL stays consistent and is not poisoned ([STL-295]).
    /// A non-zero prefix leaves stray bytes past the WAL's staged end — the case
    /// the WAL must detect and poison on. Ignored for non-`Append` ops.
    ///
    /// [STL-295]: https://allegromusic.atlassian.net/browse/STL-295
    /// [STL-299]: https://allegromusic.atlassian.net/browse/STL-299
    pub torn_prefix: usize,
}

/// A deterministic, FIFO schedule of [`Fault`]s shared by a [`MemDisk`] and all
/// of its [`MemFile`] handles.
///
/// An operation fires the *head* fault only if the head targets that operation;
/// otherwise the head waits. So scheduling `[Sync]` means "the next `sync`
/// fails, whatever happens first", and `[Append, Sync]` means "the next
/// `append` fails, then the next `sync` fails".
#[derive(Debug, Clone, Default)]
pub struct Faults {
    queue: Arc<Mutex<VecDeque<Fault>>>,
}

impl Faults {
    /// An empty schedule — no faults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a fault: the next `op` to run will fail *cleanly* with `kind` —
    /// nothing is written (the [`torn_prefix`](Fault::torn_prefix) is `0`). For a
    /// partial physical write, use [`schedule_torn_append`](Self::schedule_torn_append).
    pub fn schedule(&self, op: FaultOp, kind: io::ErrorKind) {
        self.queue.lock().unwrap().push_back(Fault {
            op,
            kind,
            torn_prefix: 0,
        });
    }

    /// Schedule a **torn** append: the next [`append`](DiskFile::append)
    /// physically writes `prefix_len` leading bytes of the slice passed to it,
    /// then fails with `kind`. This models a partial physical write — bytes land
    /// on disk past the WAL's staged end, yet the call returns `Err` — which the
    /// WAL must detect and treat as a crash by poisoning, unlike a clean append
    /// failure that writes nothing ([STL-299]). `prefix_len` is capped at the
    /// appended slice's length by [`MemFile::append`], so an over-long value
    /// writes the whole slice and still fails.
    ///
    /// [STL-299]: https://allegromusic.atlassian.net/browse/STL-299
    pub fn schedule_torn_append(&self, kind: io::ErrorKind, prefix_len: usize) {
        self.queue.lock().unwrap().push_back(Fault {
            op: FaultOp::Append,
            kind,
            torn_prefix: prefix_len,
        });
    }

    /// How many scheduled faults have not yet fired.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    /// If the head fault targets `op`, consume and return it; otherwise leave the
    /// schedule untouched and return `None`. [`MemFile::append`] consumes the
    /// whole [`Fault`] this way so it can honour a torn append's
    /// [`torn_prefix`](Fault::torn_prefix); ops with no partial-write modelling
    /// go through [`check`](Self::check).
    fn take(&self, op: FaultOp) -> Option<Fault> {
        let mut queue = self.queue.lock().unwrap();
        if queue.front().is_some_and(|f| f.op == op) {
            queue.pop_front()
        } else {
            None
        }
    }

    /// If the head fault targets `op`, consume it and return its error.
    fn check(&self, op: FaultOp) -> io::Result<()> {
        self.take(op).map_or(Ok(()), |f| {
            Err(io::Error::new(f.kind, "stele-sim: injected fault"))
        })
    }
}

/// One file's bytes, shared by every [`MemFile`] handle open on it.
type FileBytes = Arc<Mutex<Vec<u8>>>;
/// The disk's name → bytes map, shared by every clone of a [`MemDisk`].
type FileMap = Arc<Mutex<HashMap<String, FileBytes>>>;

/// A heap-backed [`Disk`]. Cloning shares the same file set.
#[derive(Debug, Clone, Default)]
pub struct MemDisk {
    inner: FileMap,
    faults: Faults,
}

impl MemDisk {
    /// A fresh, empty disk with no fault injection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A fresh, empty disk that fails operations per `faults`. The returned
    /// disk shares the schedule, so the caller can keep scheduling faults
    /// after construction.
    #[must_use]
    pub fn with_faults(faults: Faults) -> Self {
        Self {
            inner: Arc::default(),
            faults,
        }
    }

    /// The fault schedule driving this disk.
    #[must_use]
    pub const fn faults(&self) -> &Faults {
        &self.faults
    }
}

impl Disk for MemDisk {
    type File = MemFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        validate_name(name)?;
        self.faults.check(FaultOp::Create)?;
        let bytes = {
            let mut files = self.inner.lock().unwrap();
            if files.contains_key(name) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{name} already exists"),
                ));
            }
            let bytes: FileBytes = Arc::new(Mutex::new(Vec::new()));
            files.insert(name.to_owned(), Arc::clone(&bytes));
            bytes
        };
        Ok(MemFile {
            bytes,
            faults: self.faults.clone(),
        })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        validate_name(name)?;
        self.faults.check(FaultOp::Open)?;
        let bytes = {
            let files = self.inner.lock().unwrap();
            files
                .get(name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_owned()))?
                .clone()
        };
        Ok(MemFile {
            bytes,
            faults: self.faults.clone(),
        })
    }

    fn list(&self) -> io::Result<Vec<String>> {
        self.faults.check(FaultOp::List)?;
        Ok(self.inner.lock().unwrap().keys().cloned().collect())
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        validate_name(name)?;
        self.faults.check(FaultOp::Remove)?;
        if self.inner.lock().unwrap().remove(name).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, name.to_owned()));
        }
        Ok(())
    }

    fn sync_dir(&self) -> io::Result<()> {
        // The heap namespace is atomically durable — the fence has nothing to
        // flush. The fault hook stays, so a test can prove a caller fences
        // where the contract demands it.
        self.faults.check(FaultOp::SyncDir)
    }
}

/// A single file within a [`MemDisk`]. Holds a shared handle to the file's
/// bytes, so appends through one handle are visible to reads through another.
#[derive(Debug)]
pub struct MemFile {
    bytes: Arc<Mutex<Vec<u8>>>,
    faults: Faults,
}

impl DiskFile for MemFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Some(fault) = self.faults.take(FaultOp::Append) {
            // Model a *torn* append: physically land `torn_prefix` leading bytes
            // (capped at `bytes`), then fail. The default `torn_prefix == 0`
            // is a *clean* failure — nothing lands, leaving the WAL's bookkeeping
            // consistent with the file (STL-295); a non-zero prefix leaves stray
            // bytes past the staged end that the WAL detects and poisons on
            // (STL-299). Like a real partial write, this advances the file's
            // `len()` by exactly what landed — the signal the WAL keys off.
            let landed = fault.torn_prefix.min(bytes.len());
            if landed > 0 {
                self.bytes
                    .lock()
                    .unwrap()
                    .extend_from_slice(&bytes[..landed]);
            }
            return Err(io::Error::new(fault.kind, "stele-sim: injected fault"));
        }
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.faults.check(FaultOp::ReadAt)?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let src = self.bytes.lock().unwrap();
        if start >= src.len() {
            return Ok(0);
        }
        // `saturating_add` so a huge `buf.len()` can't overflow `usize` when
        // `start` is already near the top of the address space.
        let end = start.saturating_add(buf.len()).min(src.len());
        let n = end - start;
        buf[..n].copy_from_slice(&src[start..end]);
        drop(src);
        Ok(n)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.faults.check(FaultOp::Sync)
    }

    fn len(&self) -> u64 {
        self.bytes.lock().unwrap().len() as u64
    }
}
