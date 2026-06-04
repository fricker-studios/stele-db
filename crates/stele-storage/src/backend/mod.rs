//! Pluggable storage backend — the runtime-agnostic I/O seam ([STL-90]).
//!
//! Every byte the storage engine reads or writes flows through the [`Disk`] /
//! [`DiskFile`] traits. A production binary wires in [`local::LocalDisk`] (real
//! filesystem); tests and the deterministic simulation harness drive
//! [`memory::MemDisk`] (heap-backed, with optional fault injection). A future
//! `s3` backend ([ADR-0007](../../../../docs/adr/0007-storage-compute-separation.md),
//! v0.3+) slots in behind the same trait.
//!
//! ## Why this is the "StorageBackend" the ticket names
//!
//! The trait pair here *is* the pluggable `StorageBackend` of [STL-90]. It grew
//! up inside the WAL ([STL-86]) because that was the first writer to need it,
//! then segments ([STL-88]) and the delta tier ([STL-87]) adopted it. This
//! module is its promotion out of `wal` into a workspace-level seam; `wal`
//! re-exports the names for source compatibility.
//!
//! ## Append-only, not random-write
//!
//! The ticket sketches a `write_at` primitive, but Stele's storage is
//! append-only: the WAL only ever appends, and a sealed segment is immutable
//! once written ([architecture §12 invariant 1](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//! So [`DiskFile`] exposes [`append`](DiskFile::append) (logical `O_APPEND`),
//! never a positional overwrite. Positional *reads* ([`read_at`](DiskFile::read_at))
//! are fine — sealed segments are read at arbitrary offsets.
//!
//! ## Determinism
//!
//! No `tokio` types and no wall-clock reads appear on this surface, satisfying
//! invariant 7 ([architecture §12](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)):
//! the same `Disk` trait is driven by real OS resources in production and by
//! the deterministic [`memory`] backend under the sim scheduler.

use std::io;
use std::path::{Component, Path};

pub mod local;
pub mod memory;

pub use local::{LocalDisk, LocalFile};
pub use memory::{Fault, FaultOp, Faults, MemDisk, MemFile};

/// Validate a file name against the flat-namespace rule every [`Disk`] shares
/// (see the trait docs): a name must be a single *normal* path component — no
/// separators, no `.`/`..`, non-empty. Returns `Err(InvalidInput)` otherwise.
///
/// Centralizing this keeps every backend's namespace identical, so a name that
/// the in-memory disk accepts can never be one a real filesystem would reject.
pub(crate) fn validate_name(name: &str) -> io::Result<()> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid backend file name: {name:?}"),
        )),
    }
}

/// Directory-like handle. A backend stores all of its files under one `Disk` —
/// equivalent to a single directory in a filesystem-backed implementation.
///
/// ## Flat namespace
///
/// A `Disk` is a *flat* namespace: a file name must be a single normal path
/// component (no `/`, no `.`/`..`, non-empty). [`create`](Self::create),
/// [`open`](Self::open), and [`remove`](Self::remove) reject anything else with
/// [`io::ErrorKind::InvalidInput`] *before* touching storage, so a name can
/// never escape the disk root. Every backend enforces this identically (via the
/// crate-internal `validate_name`); the backend conformance suite asserts it for
/// both.
pub trait Disk: Send + Sync + 'static {
    /// File handle returned by [`create`](Self::create) / [`open`](Self::open).
    type File: DiskFile;

    /// Create a new file. Errors with `AlreadyExists` if it already exists, or
    /// `InvalidInput` if `name` violates the flat-namespace rule.
    fn create(&self, name: &str) -> io::Result<Self::File>;

    /// Open an existing file for append + random read. Errors with `NotFound`
    /// if absent, or `InvalidInput` for a non-flat `name`.
    fn open(&self, name: &str) -> io::Result<Self::File>;

    /// List file names currently in this disk. Order is unspecified — callers
    /// must sort.
    fn list(&self) -> io::Result<Vec<String>>;

    /// Remove a file by name. Errors with `NotFound` if it does not exist, or
    /// `InvalidInput` for a non-flat `name`.
    ///
    /// The WAL itself does not delete its segments — sealed log files are
    /// recoverable forever, by design. `remove` is here for *ephemeral*
    /// artefacts on the same disk handle: today, the delta tier's spill
    /// files ([STL-87]); later, compaction's temporary segment buffers. A
    /// filesystem-backed [`Disk`] implements this as [`std::fs::remove_file`];
    /// the in-memory disk models the same.
    fn remove(&self, name: &str) -> io::Result<()>;
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
