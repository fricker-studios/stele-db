//! `LocalDisk` — a [`Disk`] backed by a real filesystem directory.
//!
//! A thin wrapper over [`std::fs`]: each [`Disk`] is one directory, each
//! [`DiskFile`] one file inside it. Files are opened in append + random-read
//! mode; writes use `O_APPEND` semantics and positional reads use `pread(2)`
//! ([`FileExt::read_at`]), so an in-flight reader never disturbs the append
//! cursor. [`sync`](DiskFile::sync) is `fsync(2)` — the engine's only
//! durability point.
//!
//! Unix-only by construction (`pread`). Stele's supported targets are Linux and
//! macOS ([04 — CI/CD](../../../../docs/04-cicd.md)); a Windows port would add a
//! `seek_read`-based path here.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use super::{Disk, DiskFile, validate_name};

/// A [`Disk`] rooted at a filesystem directory. All files live directly under
/// `root`; the directory is created on construction if it does not exist.
#[derive(Debug, Clone)]
pub struct LocalDisk {
    root: PathBuf,
}

impl LocalDisk {
    /// Open (creating if necessary) a `LocalDisk` rooted at `root`.
    ///
    /// # Errors
    /// Propagates any error creating `root` or confirming it is a directory.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Resolve `name` to a path inside `root`, enforcing the [`Disk`] flat-
    /// namespace rule ([`validate_name`]) so a caller-supplied name can't escape
    /// the disk root.
    fn path(&self, name: &str) -> io::Result<PathBuf> {
        validate_name(name)?;
        Ok(self.root.join(name))
    }
}

impl Disk for LocalDisk {
    type File = LocalFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(self.path(name)?)?;
        Ok(LocalFile { file, len: 0 })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(self.path(name)?)?;
        let len = file.metadata()?.len();
        Ok(LocalFile { file, len })
    }

    fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(name) = entry.file_name().to_str()
            {
                names.push(name.to_owned());
            }
        }
        Ok(names)
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        fs::remove_file(self.path(name)?)
    }
}

/// A single file within a [`LocalDisk`].
///
/// `len` is tracked locally rather than re-`stat`-ed per call: appends are the
/// only writer and update it in lock-step, so it always matches the file's
/// logical length without a syscall on the hot read path.
#[derive(Debug)]
pub struct LocalFile {
    file: File,
    len: u64,
}

impl DiskFile for LocalFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        use std::io::Write as _;
        // The file was opened with `O_APPEND`, so every byte lands at
        // end-of-file regardless of any prior `read_at`.
        self.file.write_all(bytes)?;
        self.len += bytes.len() as u64;
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // `read_at` is `pread(2)`: it ignores and does not move the file
        // offset, so it cannot race the append cursor.
        self.file.read_at(buf, offset)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn len(&self) -> u64 {
        self.len
    }
}
