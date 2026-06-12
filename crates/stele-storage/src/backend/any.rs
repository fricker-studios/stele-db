//! Runtime backend selection — [`BackendKind`] + the [`AnyDisk`] dispatch wrapper ([STL-116]).
//!
//! [`Disk`] is deliberately *not* object-safe: its associated `type File` means
//! `Box<dyn Disk>` will not compile. To let the engine choose a backend at boot
//! from `stele.toml` ([05 — configuration](../../../../docs/05-dev-environment.md#configuration)),
//! we dispatch through a small enum instead. [`AnyDisk`] holds one concrete
//! backend and forwards every [`Disk`] call to it; [`AnyFile`] does the same for
//! the file handle. The indirection is one `match` per call — no allocation, no
//! vtable — and adding the `s3` backend ([ADR-0007](../../../../docs/adr/0007-storage-compute-separation.md))
//! later is a new variant here.

use std::fmt;
use std::io;
use std::path::Path;
use std::str::FromStr;

use super::{Disk, DiskFile, LocalDisk, LocalFile, MemDisk, MemFile};

/// Which storage backend the engine runs on, chosen once at boot.
///
/// Parsed from the `stele.toml` `[storage] backend` string via [`FromStr`];
/// see [`AnyDisk::open`] for construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// Real filesystem under a data directory ([`LocalDisk`]).
    Local,
    /// Heap-backed, ephemeral ([`MemDisk`]) — tests, the sim harness, and
    /// throwaway runs.
    Memory,
}

impl BackendKind {
    /// The canonical config spelling — the inverse of [`FromStr`].
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Memory => "memory",
        }
    }
}

impl fmt::Display for BackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error from parsing an unknown `[storage] backend` value. Its [`Display`] names
/// the offending input and the accepted set, so the config layer can surface a
/// clear message without re-deriving it.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseBackendKindError {
    got: String,
}

impl ParseBackendKindError {
    /// The unrecognized backend string, as written in the config.
    #[must_use]
    pub fn unknown(&self) -> &str {
        &self.got
    }
}

impl fmt::Display for ParseBackendKindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown storage backend {:?}, expected \"local\" or \"memory\"",
            self.got
        )
    }
}

impl std::error::Error for ParseBackendKindError {}

impl FromStr for BackendKind {
    type Err = ParseBackendKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(Self::Local),
            "memory" => Ok(Self::Memory),
            other => Err(ParseBackendKindError {
                got: other.to_owned(),
            }),
        }
    }
}

/// A storage backend selected at boot, dispatching [`Disk`] calls to the concrete
/// impl behind it. Construct one with [`open`](Self::open) from a [`BackendKind`].
#[derive(Debug, Clone)]
pub enum AnyDisk {
    /// A real-filesystem backend.
    Local(LocalDisk),
    /// An in-memory backend.
    Mem(MemDisk),
}

impl AnyDisk {
    /// Construct the backend named by `kind`.
    ///
    /// [`Local`](BackendKind::Local) is rooted at `data_dir` (created if absent,
    /// per [`LocalDisk::open`]); [`Memory`](BackendKind::Memory) is ephemeral and
    /// ignores `data_dir`.
    ///
    /// # Errors
    /// Propagates any error opening the underlying backend (e.g. creating the
    /// local data directory).
    pub fn open(kind: BackendKind, data_dir: impl AsRef<Path>) -> io::Result<Self> {
        match kind {
            BackendKind::Local => Ok(Self::Local(LocalDisk::open(data_dir)?)),
            BackendKind::Memory => Ok(Self::Mem(MemDisk::new())),
        }
    }

    /// The [`BackendKind`] this disk dispatches to.
    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        match self {
            Self::Local(_) => BackendKind::Local,
            Self::Mem(_) => BackendKind::Memory,
        }
    }
}

impl Disk for AnyDisk {
    type File = AnyFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        match self {
            Self::Local(d) => d.create(name).map(AnyFile::Local),
            Self::Mem(d) => d.create(name).map(AnyFile::Mem),
        }
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        match self {
            Self::Local(d) => d.open(name).map(AnyFile::Local),
            Self::Mem(d) => d.open(name).map(AnyFile::Mem),
        }
    }

    fn list(&self) -> io::Result<Vec<String>> {
        match self {
            Self::Local(d) => d.list(),
            Self::Mem(d) => d.list(),
        }
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        match self {
            Self::Local(d) => d.remove(name),
            Self::Mem(d) => d.remove(name),
        }
    }

    fn sync_dir(&self) -> io::Result<()> {
        match self {
            Self::Local(d) => d.sync_dir(),
            Self::Mem(d) => d.sync_dir(),
        }
    }
}

/// A file handle within an [`AnyDisk`], dispatching [`DiskFile`] calls to the
/// concrete backend's handle.
#[derive(Debug)]
pub enum AnyFile {
    /// A [`LocalDisk`] file handle.
    Local(LocalFile),
    /// A [`MemDisk`] file handle.
    Mem(MemFile),
}

impl DiskFile for AnyFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::Local(f) => f.append(bytes),
            Self::Mem(f) => f.append(bytes),
        }
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Local(f) => f.read_at(offset, buf),
            Self::Mem(f) => f.read_at(offset, buf),
        }
    }

    fn sync(&mut self) -> io::Result<()> {
        match self {
            Self::Local(f) => f.sync(),
            Self::Mem(f) => f.sync(),
        }
    }

    fn len(&self) -> u64 {
        match self {
            Self::Local(f) => f.len(),
            Self::Mem(f) => f.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_kind_parses_known_strings() {
        assert_eq!("local".parse::<BackendKind>(), Ok(BackendKind::Local));
        assert_eq!("memory".parse::<BackendKind>(), Ok(BackendKind::Memory));
    }

    #[test]
    fn backend_kind_rejects_unknown_with_clear_error() {
        let err = "s3".parse::<BackendKind>().unwrap_err();
        assert_eq!(err.unknown(), "s3");
        let msg = err.to_string();
        // The message must name the bad input and the accepted set.
        assert!(msg.contains("\"s3\""), "{msg}");
        assert!(msg.contains("local") && msg.contains("memory"), "{msg}");
    }

    #[test]
    fn backend_kind_string_roundtrips() {
        for kind in [BackendKind::Local, BackendKind::Memory] {
            assert_eq!(kind.as_str().parse::<BackendKind>(), Ok(kind));
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }

    #[test]
    fn parsing_is_case_sensitive() {
        // The config grammar is exact; "Local"/"MEMORY" are errors, not aliases.
        assert!("Local".parse::<BackendKind>().is_err());
        assert!("MEMORY".parse::<BackendKind>().is_err());
    }

    #[test]
    fn memory_disk_reports_its_kind_and_dispatches() {
        let disk = AnyDisk::open(BackendKind::Memory, "/ignored/for/memory").unwrap();
        assert_eq!(disk.kind(), BackendKind::Memory);

        // A round-trip through the dispatch wrapper behaves like the backend.
        let mut f = disk.create("wal-000001.log").unwrap();
        f.append(b"hello").unwrap();
        f.sync().unwrap();
        assert_eq!(f.len(), 5);

        let mut buf = [0u8; 5];
        let reader = disk.open("wal-000001.log").unwrap();
        assert_eq!(reader.read_at(0, &mut buf).unwrap(), 5);
        assert_eq!(&buf, b"hello");

        assert_eq!(disk.list().unwrap(), vec!["wal-000001.log".to_owned()]);
        disk.remove("wal-000001.log").unwrap();
        assert!(disk.list().unwrap().is_empty());
    }

    #[test]
    fn local_disk_reports_its_kind_and_dispatches() {
        let dir = std::env::temp_dir().join(format!("stele-anydisk-{}", std::process::id()));
        let disk = AnyDisk::open(BackendKind::Local, &dir).unwrap();
        assert_eq!(disk.kind(), BackendKind::Local);

        let mut f = disk.create("seg-1.dat").unwrap();
        f.append(b"abc").unwrap();
        f.sync().unwrap();
        let mut buf = [0u8; 3];
        assert_eq!(f.read_at(0, &mut buf).unwrap(), 3);
        assert_eq!(&buf, b"abc");

        disk.remove("seg-1.dat").unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}
