//! `Disk` and `DiskFile` — the runtime-agnostic I/O traits the WAL writes through.
//!
//! These are the **only** way the WAL touches storage. A production binary will
//! wire in a real filesystem-backed implementation; `stele-sim` drives a virtual
//! disk with seeded fault injection ([ADR-0010](../../../../docs/adr/0010-deterministic-simulation-testing.md)).
//!
//! The trait surface is deliberately narrow — just what the WAL needs (append,
//! read-at, fsync, list, create/open). The broader, workspace-wide
//! `StorageBackend` from [STL-90] will generalize or supersede this; until then
//! keeping the trait scoped here avoids speculative design.
//!
//! No `tokio` types appear here, satisfying invariant 7
//! ([architecture §12](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).

use std::io;

/// Directory-like handle. The WAL stores all of its segment files under one
/// `Disk` — equivalent to a single directory in a filesystem-backed
/// implementation.
pub trait Disk: Send + Sync + 'static {
    /// File handle returned by [`create`](Self::create) / [`open`](Self::open).
    type File: DiskFile;

    /// Create a new file. Errors with `AlreadyExists` if it already exists.
    fn create(&self, name: &str) -> io::Result<Self::File>;

    /// Open an existing file for append + random read.
    fn open(&self, name: &str) -> io::Result<Self::File>;

    /// List file names currently in this disk. Order is unspecified — callers
    /// must sort.
    fn list(&self) -> io::Result<Vec<String>>;
}

/// A single append-only file within a [`Disk`].
///
/// Append is logically `O_APPEND`: writes go to end-of-file and never overwrite
/// existing bytes. `sync` is the durability point — until `sync` returns, the
/// engine has no claim that appended bytes survive a crash. This mirrors the
/// architectural rule that **the WAL fsync is the only durability point**
/// (invariant 2).
pub trait DiskFile: Send {
    /// Append `bytes` to the file. On success the bytes are *visible* to
    /// subsequent reads on the same `Disk`, but not yet *durable*.
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Read into `buf` starting at `offset`. Returns the number of bytes read;
    /// 0 means EOF. Short reads at EOF are normal and must be tolerated.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Flush + fsync. After this returns, every previously-appended byte is
    /// durable.
    fn sync(&mut self) -> io::Result<()>;

    /// Current logical length in bytes.
    fn len(&self) -> u64;

    /// True iff the file is zero-length.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
