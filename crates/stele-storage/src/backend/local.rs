//! `LocalDisk` — a [`Disk`] backed by a real filesystem directory.
//!
//! A thin wrapper over [`std::fs`]: each [`Disk`] is one directory, each
//! [`DiskFile`] one file inside it. Files are opened in append + random-read
//! mode; writes use append semantics — `O_APPEND` on Unix, `FILE_APPEND_DATA`
//! on Windows — so every write lands at end-of-file regardless of where any
//! reader is positioned. Positional reads are `pread(2)` (`FileExt::read_at`)
//! on Unix and `seek_read` (`ReadFile` with an explicit offset) on Windows.
//! [`sync`](DiskFile::sync) is `fsync(2)` / `FlushFileBuffers` — the engine's
//! only durability point.
//!
//! The one platform difference worth knowing (STL-160): `pread` never touches
//! the file cursor, while Windows `seek_read` may move it. That asymmetry is
//! harmless here *because* the handle is append-mode: `FILE_APPEND_DATA`
//! writes ignore the cursor and append at EOF, exactly like `O_APPEND`. The
//! backend conformance suite pins this invariant on every platform
//! (`tests/backend.rs`, the append-cursor contract), and the Windows CI leg
//! runs it on `x86_64-pc-windows-msvc`
//! ([04 — CI/CD](../../../../docs/04-cicd.md)).

use std::fs::{self, File, OpenOptions};
use std::io;
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

    #[cfg(unix)]
    fn sync_dir(&self) -> io::Result<()> {
        // The directory fence is a real fsync of the root directory: POSIX
        // separates a file's contents from its directory entry, so a crash
        // after `DiskFile::sync` can keep the bytes yet lose the name. Opening
        // a directory read-only for fsync is the standard idiom.
        File::open(&self.root)?.sync_all()
    }

    #[cfg(windows)]
    fn sync_dir(&self) -> io::Result<()> {
        // Windows has no supported directory-handle flush: `CreateFile` on a
        // directory needs `FILE_FLAG_BACKUP_SEMANTICS` (std's `File::open`
        // does not pass it), and NTFS journals metadata operations on its own.
        // Like SQLite and LevelDB on this platform, the fence is a no-op.
        Ok(())
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
        // The file was opened in append mode (`O_APPEND` on Unix,
        // `FILE_APPEND_DATA` on Windows), so every byte lands at end-of-file
        // regardless of any prior `read_at`.
        //
        // This is `write_all` unrolled (see [`write_all_counting`]) so a *torn*
        // append — a partial physical write that then errors, e.g. `ENOSPC`
        // after some bytes have already landed — still advances `len` by exactly
        // what reached the file. Plain `write_all` discards that partial count,
        // which would leave `len()` equal to the WAL's staged end and make a tear
        // read as a *clean* failure: `Wal::append` keys its torn-detection off
        // `len() > staged_end` to poison the log ([STL-299]/[STL-305]). `MemFile`
        // already advances `len()` by what a torn fault landed, so this brings the
        // real backend to parity — a torn group-commit append now fails closed on
        // a real filesystem, not only under the simulator.
        let (written, result) = write_all_counting(&mut self.file, bytes);
        self.len += written;
        result
    }

    #[cfg(unix)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // `read_at` is `pread(2)`: it ignores and does not move the file
        // offset, so it cannot race the append cursor.
        use std::os::unix::fs::FileExt as _;
        self.file.read_at(buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // `seek_read` is `ReadFile` with an explicit offset. Unlike `pread` it
        // may move the file cursor, which is harmless on this handle: it was
        // opened append-mode (`FILE_APPEND_DATA`), so writes ignore the cursor
        // and land at EOF, same as `O_APPEND`. A read entirely past EOF is
        // `ERROR_HANDLE_EOF`, which std maps to `Ok(0)` — matching `pread`.
        use std::os::windows::fs::FileExt as _;
        self.file.seek_read(buf, offset)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn len(&self) -> u64 {
        self.len
    }
}

/// [`std::io::Write::write_all`], but reports how many bytes physically reached
/// `w` even when the write ultimately fails.
///
/// `write_all` returns `()` on success and discards its progress on error, so a
/// caller cannot tell a *torn* write (a partial physical write that then errors)
/// from a *clean* one (nothing landed). [`LocalFile::append`] needs that
/// distinction to keep `len()` in lock-step with the file on the error path —
/// the signal the WAL's torn-append poison keys off ([STL-299]/[STL-305]). The
/// returned count is the total bytes accepted by `w` across the loop; on the
/// `Ok` path it equals `bytes.len()`. `Interrupted` (EINTR) is retried, matching
/// `write_all`.
///
/// [STL-299]: https://allegromusic.atlassian.net/browse/STL-299
/// [STL-305]: https://allegromusic.atlassian.net/browse/STL-305
fn write_all_counting(w: &mut impl io::Write, bytes: &[u8]) -> (u64, io::Result<()>) {
    let mut written = 0usize;
    while written < bytes.len() {
        match w.write(&bytes[written..]) {
            // `write_all` treats a 0-byte write as `WriteZero`; the bytes already
            // accepted in earlier iterations still count toward `written`.
            Ok(0) => {
                return (
                    written as u64,
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to append whole buffer",
                    )),
                );
            }
            Ok(n) => written += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return (written as u64, Err(e)),
        }
    }
    (written as u64, Ok(()))
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::write_all_counting;

    /// An [`io::Write`] that accepts at most `budget` bytes (in ≤4-byte chunks,
    /// so a multi-byte buffer drives several `write` calls), then fails — a
    /// deterministic, portable stand-in for a torn physical write (a partial
    /// `write` that then errors, e.g. `ENOSPC`), with no real disk-full needed.
    struct TornWriter {
        budget: usize,
        landed: Vec<u8>,
    }

    impl io::Write for TornWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.budget == 0 {
                return Err(io::Error::other("out of space"));
            }
            let n = buf.len().min(self.budget).min(4);
            self.landed.extend_from_slice(&buf[..n]);
            self.budget -= n;
            Ok(n)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A *torn* append: some bytes land, then the write fails. The reported count
    /// must equal exactly the bytes that reached the writer — that is the length
    /// `Wal::append` compares against its staged end to detect the tear and poison
    /// ([STL-299]/[STL-305]).
    #[test]
    fn write_all_counting_reports_a_torn_prefix() {
        let mut w = TornWriter {
            budget: 6,
            landed: Vec::new(),
        };
        let (written, result) = write_all_counting(&mut w, b"hello world");
        assert!(result.is_err(), "an exhausted budget fails the append");
        assert_eq!(written, 6, "len advances by exactly the bytes that landed");
        assert_eq!(
            w.landed, b"hello ",
            "only the landed prefix reached the file"
        );
    }

    /// A *clean* failure — nothing lands — reports zero, so the WAL sees its
    /// bookkeeping still matches the file and stays un-poisoned ([STL-295]).
    #[test]
    fn write_all_counting_clean_failure_reports_zero() {
        let mut w = TornWriter {
            budget: 0,
            landed: Vec::new(),
        };
        let (written, result) = write_all_counting(&mut w, b"data");
        assert!(result.is_err());
        assert_eq!(written, 0, "a clean failure advances len by nothing");
        assert!(w.landed.is_empty());
    }

    /// The success path writes everything and reports the full length — identical
    /// accounting to the `write_all` it replaces.
    #[test]
    fn write_all_counting_full_write_reports_all() {
        let mut w = TornWriter {
            budget: 100,
            landed: Vec::new(),
        };
        let (written, result) = write_all_counting(&mut w, b"hello world");
        assert!(result.is_ok());
        assert_eq!(written, 11);
        assert_eq!(w.landed, b"hello world");
    }
}
