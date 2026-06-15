//! Online full backup + restore ([STL-249], [ADR-0032]).
//!
//! A **backup** is a consistent, self-describing copy of everything the engine
//! needs to be itself again — the sealed columnar segments, every per-table WAL,
//! the durable catalog log, and the hash-chained commit log — taken at a
//! checkpoint/flush *fence* while the server stays up. A **restore** materializes
//! a fresh data directory from a backup, verifying every byte against the
//! manifest before handing the directory to normal recovery
//! ([`SessionEngine::recover`](crate::SessionEngine::recover)).
//!
//! ## Byte-for-byte by construction
//!
//! Backup and restore both copy at the [`Disk`] level: list the source, read each
//! file's bytes verbatim, write them to the target. Nothing is re-encoded, so the
//! restored immutable set is *byte-for-byte identical* to the source — the second
//! clause of the v0.3 exit criterion ([docs/03 §v0.3](../../../docs/03-roadmap.md),
//! [docs/01 §B.6](../../../docs/01-feature-plan.md#b6--backup-restore--snapshots)).
//! Working through the trait — not `std::fs` — also keeps the storage-backend
//! seam honest ([STL-232]): the same code backs up a local directory or, later, an
//! object store ([ADR-0007]).
//!
//! ## The fence, and what "online" means here
//!
//! [`SessionEngine::backup`](crate::SessionEngine::backup) first
//! [`flush`](crate::SessionEngine::flush)es every table (sealing each delta into
//! an immutable segment) and [`checkpoint`](crate::SessionEngine::checkpoint)s
//! (fsyncing every WAL), so the on-disk set is a complete, recoverable snapshot;
//! the *fence instant* is the commit clock's high-water mark at that point. The
//! copy then runs synchronously, holding the session lock for its duration — the
//! same brief stop-the-world `FLUSH`/`COMPACT` already are ([STL-219]). The server
//! never goes down and no connection is dropped; concurrent writers simply queue
//! behind the admin statement, and anything they commit *after* the fence is not
//! in the backup. A fully non-blocking, streaming online backup (copying while
//! writers proceed) is a deliberate follow-up the manifest's recorded fence makes
//! room for ([STL-249] notes).
//!
//! ## Tamper evidence
//!
//! The [`BackupManifest`] records a SHA-256 ([`stele_common::hash`]) of every file
//! plus a self-digest over its own body. [`restore_disk`] refuses if any file's
//! hash, or the manifest's own digest, does not match — a single flipped byte in
//! any backed-up file is caught before the data dir is materialized. Recovery then
//! adds a second, independent layer: segment checksums ([02 §3.2]) and the
//! commit-log hash chain ([STL-178], [ADR-0031]) re-verify on boot.
//!
//! [STL-178]: https://allegromusic.atlassian.net/browse/STL-178
//! [STL-219]: https://allegromusic.atlassian.net/browse/STL-219
//! [STL-232]: https://allegromusic.atlassian.net/browse/STL-232
//! [STL-249]: https://allegromusic.atlassian.net/browse/STL-249
//! [ADR-0007]: ../../../docs/adr/0007-storage-compute-separation.md
//! [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
//! [02 §3.2]: ../../../docs/02-architecture.md#32-on-disk-segment-format

use std::io;

use stele_common::hash::{Digest, SHA256_LEN, sha256};
use stele_storage::backend::{Disk, DiskFile};

/// The backup-manifest format version this build writes ([ADR-0032]). Restore
/// refuses a manifest from a newer, unknown format rather than guessing at its
/// shape.
///
/// [ADR-0032]: ../../../docs/adr/0032-backup-manifest-format.md
pub const MANIFEST_VERSION: u32 = 1;

/// The manifest's filename within a backup directory — the one file restore reads
/// first to learn what every other file should be.
pub const MANIFEST_FILENAME: &str = "MANIFEST";

/// First-line magic identifying a Stele backup manifest ([ADR-0032]).
const MANIFEST_MAGIC: &str = "STELE-BACKUP-MANIFEST";

/// One backed-up file: its name, length, and content hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// The file's name on the disk (a flat backend name — no path separators).
    pub name: String,
    /// Its length in bytes, redundant with the hash but a cheap first check.
    pub len: u64,
    /// SHA-256 of the file's contents — the per-file tamper check.
    pub sha256: Digest,
}

/// A backup's self-describing contents ([ADR-0032]).
///
/// Carries the format/version metadata, the fence instant, the commit-chain head
/// it vouches for, and a hashed inventory of every file. Serialized as a small,
/// human-readable text file ([`MANIFEST_FILENAME`]) so an operator can inspect a
/// backup with `cat`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    /// The manifest format version ([`MANIFEST_VERSION`]).
    pub manifest_version: u32,
    /// The `stele-engine` crate version that produced the backup — a cross-version
    /// restore can warn rather than silently mis-handle a future on-disk format.
    pub stele_version: String,
    /// The fence instant: the commit clock's high-water mark when the backup was
    /// taken. Every committed write with `sys_from <= fence` is captured, so every
    /// `AS OF` read at or before it answers identically on the restored copy.
    pub fence_micros: i64,
    /// The hash-chained commit log's head at the fence ([ADR-0031], [STL-178]) —
    /// the chain anchor the backup vouches for, recorded for forensics and the
    /// point-in-time recovery the manifest is designed to extend ([STL-249]).
    ///
    /// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
    /// [STL-178]: https://allegromusic.atlassian.net/browse/STL-178
    pub commit_head: Digest,
    /// Every file in the backup, sorted by name for a deterministic manifest.
    pub files: Vec<FileEntry>,
}

impl BackupManifest {
    /// Render the manifest to its on-disk text form ([ADR-0032]): a header block,
    /// one `<sha256> <len> <name>` line per file, then a trailing
    /// `digest <sha256>` over everything above it.
    #[must_use]
    pub fn encode(&self) -> String {
        use std::fmt::Write as _;
        let mut body = String::new();
        // Writing to a `String` is infallible, so the `write!` results are ignored.
        let _ = writeln!(body, "{MANIFEST_MAGIC} {}", self.manifest_version);
        let _ = writeln!(body, "stele-version {}", self.stele_version);
        let _ = writeln!(body, "fence-micros {}", self.fence_micros);
        let _ = writeln!(body, "commit-head {}", self.commit_head.to_hex());
        let _ = writeln!(body, "files {}", self.files.len());
        for f in &self.files {
            let _ = writeln!(body, "{} {} {}", f.sha256.to_hex(), f.len, f.name);
        }
        let digest = sha256(body.as_bytes());
        format!("{body}digest {}\n", digest.to_hex())
    }

    /// Parse a manifest from its on-disk text form, verifying the self-digest.
    ///
    /// # Errors
    ///
    /// [`RestoreError::ManifestMalformed`] if the text is not a well-formed
    /// manifest or its version is unknown; [`RestoreError::ManifestDigestMismatch`]
    /// if the trailing digest does not match the body (the manifest itself was
    /// tampered with).
    pub fn decode(bytes: &[u8]) -> Result<Self, RestoreError> {
        let malformed = |m: &str| RestoreError::ManifestMalformed(m.to_owned());
        let text = std::str::from_utf8(bytes).map_err(|_| malformed("manifest is not UTF-8"))?;

        // The digest line is the final line; everything before it (including its
        // trailing newline) is the digested body.
        let trimmed = text.strip_suffix('\n').unwrap_or(text);
        let (body_str, digest_line) = trimmed
            .rsplit_once('\n')
            .ok_or_else(|| malformed("missing digest line"))?;
        let body = format!("{body_str}\n");
        let claimed = digest_line
            .strip_prefix("digest ")
            .ok_or_else(|| malformed("expected a trailing `digest` line"))?;
        let claimed = digest_from_hex(claimed).ok_or_else(|| malformed("invalid digest hex"))?;
        if sha256(body.as_bytes()) != claimed {
            return Err(RestoreError::ManifestDigestMismatch);
        }

        let mut lines = body.lines();
        let header = |lines: &mut std::str::Lines, key: &str| -> Result<String, RestoreError> {
            let line = lines
                .next()
                .ok_or_else(|| malformed(&format!("missing `{key}` line")))?;
            line.strip_prefix(key)
                .and_then(|rest| rest.strip_prefix(' '))
                .map(str::to_owned)
                .ok_or_else(|| malformed(&format!("expected `{key}` line")))
        };

        let manifest_version: u32 = header(&mut lines, MANIFEST_MAGIC)?
            .parse()
            .map_err(|_| malformed("invalid manifest version"))?;
        if manifest_version != MANIFEST_VERSION {
            return Err(RestoreError::ManifestMalformed(format!(
                "unsupported manifest version {manifest_version} (this build reads {MANIFEST_VERSION})"
            )));
        }
        let stele_version = header(&mut lines, "stele-version")?;
        let fence_micros: i64 = header(&mut lines, "fence-micros")?
            .parse()
            .map_err(|_| malformed("invalid fence-micros"))?;
        let commit_head = digest_from_hex(&header(&mut lines, "commit-head")?)
            .ok_or_else(|| malformed("invalid commit-head hex"))?;
        let file_count: usize = header(&mut lines, "files")?
            .parse()
            .map_err(|_| malformed("invalid file count"))?;

        let mut files = Vec::with_capacity(file_count);
        for line in lines {
            // `<sha256-hex> <len> <name>` — split into exactly three fields so a
            // name containing spaces still round-trips (the remainder is the name).
            let mut parts = line.splitn(3, ' ');
            let sha = parts
                .next()
                .and_then(digest_from_hex)
                .ok_or_else(|| malformed("invalid file hash"))?;
            let len: u64 = parts
                .next()
                .ok_or_else(|| malformed("missing file length"))?
                .parse()
                .map_err(|_| malformed("invalid file length"))?;
            let name = parts
                .next()
                .ok_or_else(|| malformed("missing file name"))?
                .to_owned();
            files.push(FileEntry {
                name,
                len,
                sha256: sha,
            });
        }
        if files.len() != file_count {
            return Err(malformed(&format!(
                "file count {file_count} does not match the {} listed entries",
                files.len()
            )));
        }
        // Reject duplicate names: the inventory must be unambiguous, and a duplicate
        // would otherwise surface late and opaquely as `AlreadyExists` when restore
        // re-creates the name.
        let mut seen = std::collections::BTreeSet::new();
        for f in &files {
            if !seen.insert(f.name.as_str()) {
                return Err(malformed(&format!("duplicate file name {:?}", f.name)));
            }
        }

        Ok(Self {
            manifest_version,
            stele_version,
            fence_micros,
            commit_head,
            files,
        })
    }
}

/// A failure taking a backup.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// The target directory already holds files. Backup refuses to mix its output
    /// into an existing directory so a backup is unambiguously one fence's state.
    #[error("backup target is not empty: refusing to overwrite existing files")]
    TargetNotEmpty,
    /// An I/O failure reading the source or writing the target.
    #[error("backup I/O: {0}")]
    Io(#[from] io::Error),
}

/// A failure restoring from a backup.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The target data directory already holds files. Restore refuses to merge
    /// into a non-empty directory.
    #[error("restore target is not empty: restore into a fresh data directory")]
    TargetNotEmpty,
    /// The backup directory has no [`MANIFEST_FILENAME`].
    #[error("backup manifest not found: {MANIFEST_FILENAME} is missing")]
    ManifestMissing,
    /// The manifest text is not a well-formed, supported manifest.
    #[error("backup manifest is malformed: {0}")]
    ManifestMalformed(String),
    /// The manifest's self-digest does not match its body — the manifest itself
    /// was altered.
    #[error("backup manifest digest mismatch: the manifest was tampered with")]
    ManifestDigestMismatch,
    /// A file listed in the manifest is absent from the backup.
    #[error("backup is missing the file {name:?} the manifest lists")]
    MissingFile {
        /// The missing file's name.
        name: String,
    },
    /// A backed-up file's contents do not match the hash the manifest recorded —
    /// the file was altered after the backup was taken.
    #[error("backup file {name:?} failed its checksum: it was tampered with or corrupted")]
    ChecksumMismatch {
        /// The corrupt file's name.
        name: String,
    },
    /// An I/O failure reading the backup or writing the target.
    #[error("restore I/O: {0}")]
    Io(#[from] io::Error),
}

/// Whether `name` belongs in a backup: every file *except* the manifest itself and
/// the ephemeral spill tiers.
///
/// Delta and validity-index spill files (`*-spill-*`) are rebuildable scratch that
/// recovery discards on open ([`stele_storage`]'s `Delta::open` /
/// `ValidityIndex::open`), never part of the immutable, recovery-relevant set the
/// ticket names. After a flush the delta is drained, so in practice none remain at
/// the fence — but excluding them by contract keeps a backup to exactly the
/// immutable set even if a stale spill lingers.
fn is_backed_up(name: &str) -> bool {
    name != MANIFEST_FILENAME && !name.contains("-spill-")
}

/// Read a backend file in full through [`DiskFile::read_at`]. `pub(crate)` so the
/// backup/restore oracle can compare files byte-for-byte without re-deriving the
/// read loop.
pub(crate) fn read_all<D: Disk>(disk: &D, name: &str) -> io::Result<Vec<u8>> {
    let file = disk.open(name)?;
    let len = file.len();
    let cap = usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file too large for memory"))?;
    let mut buf = vec![0u8; cap];
    let mut off = 0u64;
    while off < len {
        let read = file.read_at(off, &mut buf[usize::try_from(off).unwrap_or(usize::MAX)..])?;
        if read == 0 {
            // A 0-byte read before reaching `len` means the file is shorter than it
            // reported — it changed under us, or the read was inconsistent. On the
            // backup/restore integrity path that must fail closed: hashing a
            // truncated buffer would otherwise produce a "valid" backup missing
            // data. (Backup holds the session lock and the set is append-only, so
            // this should not happen — but a silent truncation here is exactly the
            // class of bug a backup tool must never have.)
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short read on {name:?}: expected {len} bytes, got {off}"),
            ));
        }
        off += read as u64;
    }
    Ok(buf)
}

/// Create `name` on `disk` and write `bytes` durably (append + fsync).
fn write_all<D: Disk>(disk: &D, name: &str, bytes: &[u8]) -> io::Result<()> {
    let mut file = disk.create(name)?;
    file.append(bytes)?;
    file.sync()?;
    Ok(())
}

/// Copy the immutable set from `src` to the (empty) `dst`, writing a
/// [`BackupManifest`] last.
///
/// The caller is responsible for having fenced `src` (flush + checkpoint) and for
/// the `fence_micros` / `commit_head` it records. Files are copied verbatim and
/// the manifest is written *after* every data file is durable, so a backup
/// directory that contains a manifest is a complete one.
///
/// # Errors
///
/// [`BackupError::TargetNotEmpty`] if `dst` already holds files;
/// [`BackupError::Io`] on any read/write failure.
pub fn backup_disk<S: Disk, T: Disk>(
    src: &S,
    dst: &T,
    fence_micros: i64,
    commit_head: Digest,
) -> Result<BackupManifest, BackupError> {
    if !dst.list()?.is_empty() {
        return Err(BackupError::TargetNotEmpty);
    }

    let mut names: Vec<String> = src
        .list()?
        .into_iter()
        .filter(|n| is_backed_up(n))
        .collect();
    names.sort(); // a deterministic manifest order, independent of `list` order.

    let mut files = Vec::with_capacity(names.len());
    for name in &names {
        let bytes = read_all(src, name)?;
        let sha = sha256(&bytes);
        write_all(dst, name, &bytes)?;
        files.push(FileEntry {
            name: name.clone(),
            len: bytes.len() as u64,
            sha256: sha,
        });
    }

    let manifest = BackupManifest {
        manifest_version: MANIFEST_VERSION,
        stele_version: env!("CARGO_PKG_VERSION").to_owned(),
        fence_micros,
        commit_head,
        files,
    };
    // The manifest is the last thing written; the directory fence then makes the
    // whole set — data files and manifest — durable together.
    write_all(dst, MANIFEST_FILENAME, manifest.encode().as_bytes())?;
    dst.sync_dir()?;
    Ok(manifest)
}

/// Materialize the (empty) `dst` data directory from the backup in `src`.
///
/// Every file is verified against the manifest before it is written. The returned
/// manifest lets the caller report or cross-check what was restored; the caller
/// then runs normal recovery against `dst`.
///
/// Verification is fail-closed and happens before `dst` is touched per file: the
/// manifest's self-digest first, then each file's SHA-256. A mismatch leaves `dst`
/// partially written but returns an error — the caller discards the directory
/// rather than booting it.
///
/// # Errors
///
/// [`RestoreError`] if `dst` is non-empty, the manifest is missing/malformed/
/// tampered, a listed file is absent, or any file fails its checksum.
pub fn restore_disk<S: Disk, T: Disk>(src: &S, dst: &T) -> Result<BackupManifest, RestoreError> {
    if !dst.list()?.is_empty() {
        return Err(RestoreError::TargetNotEmpty);
    }

    let manifest = read_manifest(src)?;
    for entry in &manifest.files {
        // Verify before writing, per file — a mismatch leaves `dst` partially
        // written but returns an error, so the caller discards the directory.
        let bytes = read_verified(src, entry)?;
        write_all(dst, &entry.name, &bytes)?;
    }
    dst.sync_dir()?;
    Ok(manifest)
}

/// Validate the backup in `src` **without** materializing it — the read-only
/// sibling of [`restore_disk`], behind the admin API's `RestorePlan` ([STL-254]).
///
/// Runs exactly the verification a restore does — decode the manifest (its
/// self-digest), then check every listed file's length and SHA-256 — but writes
/// nothing and needs no target. Returns the decoded [`BackupManifest`] so the
/// caller can report what the backup vouches for.
///
/// # Errors
///
/// [`RestoreError`] if the manifest is missing/malformed/tampered, a listed file
/// is absent, or any file fails its checksum.
///
/// [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
pub fn verify_disk<S: Disk>(src: &S) -> Result<BackupManifest, RestoreError> {
    let manifest = read_manifest(src)?;
    for entry in &manifest.files {
        read_verified(src, entry)?;
    }
    Ok(manifest)
}

/// Read and decode the backup manifest from `src` (verifying its self-digest).
fn read_manifest<S: Disk>(src: &S) -> Result<BackupManifest, RestoreError> {
    let manifest_bytes = match src.open(MANIFEST_FILENAME) {
        Ok(_) => read_all(src, MANIFEST_FILENAME)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(RestoreError::ManifestMissing),
        Err(e) => return Err(RestoreError::Io(e)),
    };
    BackupManifest::decode(&manifest_bytes)
}

/// Read one backed-up file and check it against its manifest `entry` (length +
/// SHA-256), returning its verified bytes.
fn read_verified<S: Disk>(src: &S, entry: &FileEntry) -> Result<Vec<u8>, RestoreError> {
    let bytes = match src.open(&entry.name) {
        Ok(_) => read_all(src, &entry.name)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(RestoreError::MissingFile {
                name: entry.name.clone(),
            });
        }
        Err(e) => return Err(RestoreError::Io(e)),
    };
    if bytes.len() as u64 != entry.len || sha256(&bytes) != entry.sha256 {
        return Err(RestoreError::ChecksumMismatch {
            name: entry.name.clone(),
        });
    }
    Ok(bytes)
}

/// Parse a lowercase-hex SHA-256 string into a [`Digest`]; `None` if it is not
/// exactly 64 hex digits.
fn digest_from_hex(s: &str) -> Option<Digest> {
    if s.len() != SHA256_LEN * 2 {
        return None;
    }
    let mut out = [0u8; SHA256_LEN];
    let bytes = s.as_bytes();
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = char::from(bytes[i * 2]).to_digit(16)?;
        let lo = char::from(bytes[i * 2 + 1]).to_digit(16)?;
        // Each nibble is < 16, so the byte is < 256 — the `try_from` never fails.
        *slot = u8::try_from(hi * 16 + lo).ok()?;
    }
    Some(Digest(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_storage::backend::MemDisk;

    fn sample_manifest() -> BackupManifest {
        BackupManifest {
            manifest_version: MANIFEST_VERSION,
            stele_version: "9.9.9".to_owned(),
            fence_micros: 1_234_567,
            commit_head: sha256(b"head"),
            files: vec![
                FileEntry {
                    name: "stele.catalog".to_owned(),
                    len: 3,
                    sha256: sha256(b"abc"),
                },
                FileEntry {
                    name: "t00000000000000000000-seg-0.seg".to_owned(),
                    len: 0,
                    sha256: sha256(b""),
                },
            ],
        }
    }

    #[test]
    fn manifest_round_trips() {
        let manifest = sample_manifest();
        let decoded = BackupManifest::decode(manifest.encode().as_bytes()).expect("decode");
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn hex_round_trips() {
        let d = sha256(b"stele");
        assert_eq!(digest_from_hex(&d.to_hex()), Some(d));
        assert_eq!(digest_from_hex("xyz"), None);
        assert_eq!(digest_from_hex(&"0".repeat(63)), None, "wrong length");
        assert_eq!(digest_from_hex(&"g".repeat(64)), None, "non-hex");
    }

    #[test]
    fn a_flipped_byte_in_the_manifest_body_is_caught() {
        let text = sample_manifest().encode();
        // Flip a digit in the fence-micros line; the self-digest no longer matches.
        let tampered = text.replacen("1234567", "7654321", 1);
        assert_ne!(text, tampered);
        assert!(matches!(
            BackupManifest::decode(tampered.as_bytes()),
            Err(RestoreError::ManifestDigestMismatch)
        ));
    }

    #[test]
    fn decode_rejects_duplicate_file_names() {
        // A well-formed-looking manifest that lists the same file twice is
        // ambiguous; decode rejects it rather than letting restore fail late and
        // opaquely on the second create.
        let mut m = sample_manifest();
        m.files.push(FileEntry {
            name: m.files[0].name.clone(),
            len: 1,
            sha256: sha256(b"x"),
        });
        let err = BackupManifest::decode(m.encode().as_bytes()).unwrap_err();
        assert!(
            matches!(&err, RestoreError::ManifestMalformed(msg) if msg.contains("duplicate")),
            "expected a duplicate-name error, got {err:?}"
        );
    }

    #[test]
    fn a_future_manifest_version_is_refused() {
        let mut text = sample_manifest();
        text.manifest_version = MANIFEST_VERSION + 1;
        // Re-encode so the self-digest is valid; the version itself must be rejected.
        let err = BackupManifest::decode(text.encode().as_bytes()).unwrap_err();
        assert!(
            matches!(err, RestoreError::ManifestMalformed(m) if m.contains("version")),
            "expected a version error"
        );
    }

    #[test]
    fn is_backed_up_excludes_manifest_and_spills() {
        assert!(is_backed_up("stele.catalog"));
        assert!(is_backed_up("stele.commits"));
        assert!(is_backed_up("t00000000000000000000-seg-3.seg"));
        assert!(is_backed_up("t00000000000000000000-wal-0.log"));
        assert!(is_backed_up("t00000000000000000000-stele.checkpoint"));
        assert!(!is_backed_up(MANIFEST_FILENAME));
        assert!(!is_backed_up("t00000000000000000000-delta-spill-2.row"));
        assert!(!is_backed_up("t00000000000000000000-validity-spill-1.row"));
    }

    #[test]
    fn backup_then_restore_round_trips_bytes_and_excludes_spills() {
        let src = MemDisk::new();
        write_all(&src, "stele.catalog", b"catalog-bytes").unwrap();
        write_all(&src, "stele.commits", b"commit-bytes").unwrap();
        write_all(&src, "t00000000000000000000-seg-0.seg", b"segment").unwrap();
        write_all(
            &src,
            "t00000000000000000000-delta-spill-0.row",
            b"ephemeral",
        )
        .unwrap();

        let backup = MemDisk::new();
        let manifest = backup_disk(&src, &backup, 42, sha256(b"head")).expect("backup");
        // The spill is excluded; the three durable files plus the manifest land.
        assert_eq!(manifest.files.len(), 3);
        assert!(!manifest.files.iter().any(|f| f.name.contains("-spill-")));
        assert_eq!(manifest.fence_micros, 42);

        let restored = MemDisk::new();
        restore_disk(&backup, &restored).expect("restore");

        // Byte-for-byte: every durable file matches the source; no spill, no manifest.
        let mut restored_names = restored.list().unwrap();
        restored_names.sort();
        assert_eq!(
            restored_names,
            vec![
                "stele.catalog".to_owned(),
                "stele.commits".to_owned(),
                "t00000000000000000000-seg-0.seg".to_owned(),
            ]
        );
        for name in &restored_names {
            assert_eq!(
                read_all(&restored, name).unwrap(),
                read_all(&src, name).unwrap(),
                "{name} restored byte-for-byte"
            );
        }
    }

    #[test]
    fn restore_refuses_a_flipped_byte_in_a_data_file() {
        let src = MemDisk::new();
        write_all(&src, "stele.catalog", b"the-original-catalog").unwrap();
        let backup = MemDisk::new();
        backup_disk(&src, &backup, 1, Digest::ZERO).expect("backup");

        // Tamper: rewrite a backed-up file with same-length but different bytes.
        // (A fresh MemDisk file; `create` rejects an existing name, so remove first.)
        backup.remove("stele.catalog").unwrap();
        write_all(&backup, "stele.catalog", b"the-TAMPERED-catalog").unwrap();

        let restored = MemDisk::new();
        let err = restore_disk(&backup, &restored).unwrap_err();
        assert!(
            matches!(err, RestoreError::ChecksumMismatch { name } if name == "stele.catalog"),
            "a flipped byte must be caught at restore"
        );
    }

    #[test]
    fn verify_validates_a_good_backup_and_catches_tampering() {
        // RestorePlan's primitive ([STL-254]): the same checks restore runs, but
        // read-only — no target written.
        let src = MemDisk::new();
        write_all(&src, "stele.catalog", b"catalog-bytes").unwrap();
        write_all(&src, "t00000000000000000000-seg-0.seg", b"segment").unwrap();
        let backup = MemDisk::new();
        let taken = backup_disk(&src, &backup, 7, sha256(b"head")).expect("backup");

        // A clean backup verifies and returns the same manifest the backup wrote.
        assert_eq!(verify_disk(&backup).expect("verify"), taken);

        // A flipped byte in a data file is caught — exactly like restore.
        backup.remove("stele.catalog").unwrap();
        write_all(&backup, "stele.catalog", b"catalog-XXXXX").unwrap();
        assert!(
            matches!(verify_disk(&backup), Err(RestoreError::ChecksumMismatch { name }) if name == "stele.catalog"),
            "verify must catch a flipped data byte"
        );
    }

    #[test]
    fn verify_refuses_a_tampered_manifest_and_a_missing_one() {
        // A tampered manifest body fails its self-digest.
        let src = MemDisk::new();
        write_all(&src, "stele.catalog", b"x").unwrap();
        let backup = MemDisk::new();
        backup_disk(&src, &backup, 1234, Digest::ZERO).unwrap();
        let original = String::from_utf8(read_all(&backup, MANIFEST_FILENAME).unwrap()).unwrap();
        let tampered = original.replacen("1234", "4321", 1);
        assert_ne!(original, tampered);
        backup.remove(MANIFEST_FILENAME).unwrap();
        write_all(&backup, MANIFEST_FILENAME, tampered.as_bytes()).unwrap();
        assert!(matches!(
            verify_disk(&backup),
            Err(RestoreError::ManifestDigestMismatch)
        ));

        // No manifest at all is a missing-manifest error.
        assert!(matches!(
            verify_disk(&MemDisk::new()),
            Err(RestoreError::ManifestMissing)
        ));
    }

    #[test]
    fn restore_refuses_a_missing_manifest_and_a_non_empty_target() {
        let backup = MemDisk::new();
        let restored = MemDisk::new();
        assert!(matches!(
            restore_disk(&backup, &restored),
            Err(RestoreError::ManifestMissing)
        ));

        // A non-empty target is refused before anything is read.
        let src = MemDisk::new();
        write_all(&src, "stele.catalog", b"x").unwrap();
        let backup = MemDisk::new();
        backup_disk(&src, &backup, 1, Digest::ZERO).unwrap();
        let occupied = MemDisk::new();
        write_all(&occupied, "stale", b"y").unwrap();
        assert!(matches!(
            restore_disk(&backup, &occupied),
            Err(RestoreError::TargetNotEmpty)
        ));
    }

    #[test]
    fn backup_refuses_a_non_empty_target() {
        let src = MemDisk::new();
        write_all(&src, "stele.catalog", b"x").unwrap();
        let dst = MemDisk::new();
        write_all(&dst, "leftover", b"y").unwrap();
        assert!(matches!(
            backup_disk(&src, &dst, 0, Digest::ZERO),
            Err(BackupError::TargetNotEmpty)
        ));
    }
}
