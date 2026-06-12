//! Executable backend-conformance suite ([STL-90], [STL-232]).
//!
//! The whole point of the [`Disk`] seam is that storage code does not care
//! which backend it runs on. This module encodes that as a set of generic,
//! panicking check functions run **unchanged** against every shipped backend:
//! `local` and `memory` in `stele-storage` (`tests/backend.rs`), and the
//! seeded fault-injecting `FaultDisk` in `stele-sim`
//! (`tests/backend_conformance.rs`). If a behaviour differs between backends,
//! one of these checks fails.
//!
//! **Test support only.** Every function here panics on a contract violation;
//! none belongs on a production code path. The module lives in the library
//! (not `tests/`) solely so other crates' backends can run the identical
//! checks.
//!
//! ## Conformance expectations for a new backend
//!
//! A backend that passes this suite has demonstrated the contract storage
//! relies on; a future object-store backend
//! ([ADR-0007](../../../../docs/adr/0007-storage-compute-separation.md), v0.4)
//! must run these checks too. The load-bearing clauses, spelled out:
//!
//! * **Flat namespace.** A file name is a single normal path component;
//!   anything else is `InvalidInput` *before* storage is touched (the
//!   crate-internal `validate_name`, shared by every backend).
//! * **Exclusive create, openable forever.** `create` of an existing name is
//!   `AlreadyExists`; `open` of a missing one is `NotFound`; `list` reflects
//!   exactly what exists (order unspecified); `remove` of a missing name is
//!   `NotFound`.
//! * **Append-only files.** Appends land at end-of-file no matter what
//!   positional reads happened in between ([STL-160]) — there is no
//!   in-place rewrite anywhere on this surface, which is precisely what makes
//!   the contract implementable by an object store (multipart/append uploads,
//!   immutable objects). The WAL/delta tier — the only latency-critical
//!   appender — stays on the local tier per ADR-0007 regardless.
//! * **Reads tolerate EOF.** A read straddling end-of-file returns the short
//!   count; a read entirely past it returns `Ok(0)`, never an error.
//! * **Visible before durable.** Appended bytes are readable through the same
//!   `Disk` immediately; durability is claimed only by
//!   [`DiskFile::sync`] (contents) and [`Disk::sync_dir`] (namespace — the
//!   directory fence, [STL-232]). A backend whose namespace mutations are
//!   atomically durable implements the fence as a successful no-op.
//!
//! Crash/fault *semantics* — what the engine must do when these operations
//! fail — are pinned where the failing operation is wired (WAL rotation
//! poisons on a failed fence, flush aborts before the checkpoint vouches, …)
//! and swept by the seeded `FaultDisk` in `stele-sim`.

use std::io;

use super::{Disk, DiskFile};

/// Exercise every guarantee in the [`Disk`] / [`DiskFile`] contract on a
/// fresh, empty `disk`. Panics on the first violation.
///
/// Leaves files behind — give each check its own disk.
pub fn disk_contract<D: Disk>(disk: &D) {
    // create → append → read back through the same handle.
    let mut f = disk.create("alpha").expect("create alpha");
    assert!(f.is_empty());
    f.append(b"hello").expect("append");
    assert_eq!(f.len(), 5, "len tracks appended bytes before sync");

    let mut buf = [0u8; 8];
    let n = f.read_at(0, &mut buf).expect("read_at 0");
    assert_eq!(
        &buf[..n],
        b"hello",
        "read sees appended (not-yet-synced) bytes"
    );

    // Short read at EOF: a read straddling the end returns only what exists.
    let n = f.read_at(3, &mut buf).expect("read_at near EOF");
    assert_eq!(&buf[..n], b"lo");
    // A read fully past EOF returns 0, never an error.
    assert_eq!(f.read_at(100, &mut buf).expect("read past EOF"), 0);

    // sync is the contents durability point; it must succeed on a healthy
    // backend — and so must the namespace fence after a create ([STL-232]).
    f.append(b" world").expect("append more");
    f.sync().expect("sync");
    disk.sync_dir().expect("sync_dir after create");
    assert_eq!(f.len(), 11);
    drop(f);

    // Persistence across handles: a fresh open sees the synced bytes and
    // reports the right length.
    let mut reopened = disk.open("alpha").expect("reopen alpha");
    assert_eq!(
        reopened.len(),
        11,
        "reopened length comes from the backing store"
    );
    let n = reopened.read_at(0, &mut buf).expect("read reopened");
    assert_eq!(&buf[..n], b"hello wo");

    // Append-after-reopen continues at end-of-file.
    reopened.append(b"!").expect("append after reopen");
    assert_eq!(reopened.len(), 12);
    let mut tail = [0u8; 1];
    assert_eq!(reopened.read_at(11, &mut tail).expect("read tail"), 1);
    assert_eq!(&tail, b"!");
    drop(reopened);

    // create is exclusive: a second create of the same name is AlreadyExists.
    let err = disk
        .create("alpha")
        .map(|_| ())
        .expect_err("create existing must fail");
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    // open of a missing file is NotFound.
    let err = disk
        .open("ghost")
        .map(|_| ())
        .expect_err("open missing must fail");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    // list reflects what exists (order is unspecified — sort before asserting).
    disk.create("beta").expect("create beta");
    let mut names = disk.list().expect("list");
    names.sort();
    assert_eq!(names, vec!["alpha".to_owned(), "beta".to_owned()]);

    // remove deletes; the fence covers removals too; removing a missing file
    // is NotFound.
    disk.remove("beta").expect("remove beta");
    disk.sync_dir().expect("sync_dir after remove");
    assert_eq!(
        disk.list().expect("list after remove"),
        vec!["alpha".to_owned()]
    );
    let err = disk.remove("beta").expect_err("remove missing must fail");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

/// [STL-160]: a positioned read must never disturb the append cursor — every
/// append lands at end-of-file no matter what reads happened in between.
///
/// Unix gets this from `pread(2)` (no cursor involved at all); Windows gets it
/// from `FILE_APPEND_DATA` append-mode writes (`seek_read` *may* move the
/// cursor, and appends must not care). The interleaving below parks the cursor
/// at the front of the file before every append, then asserts the file is the
/// exact concatenation of the appended chunks.
///
/// Panics on the first violation. Leaves files behind — give each check its
/// own disk.
pub fn positioned_reads_never_move_the_append_cursor<D: Disk>(disk: &D) {
    let mut f = disk.create("interleaved").expect("create");
    let mut expected = Vec::new();
    for i in 0..32u8 {
        // Variable-length chunks so a cursor bug can't hide behind alignment.
        let chunk = vec![i; usize::from(i % 7) + 1];
        f.append(&chunk).expect("append");
        expected.extend_from_slice(&chunk);

        // Park any cursor at the *front* of the file…
        let mut probe = [0u8; 4];
        let n = f.read_at(0, &mut probe).expect("probe front");
        assert_eq!(&probe[..n], &expected[..n]);
        // …and straddle EOF for good measure (short read, never an error).
        let mut tail = [0u8; 8];
        let n = f
            .read_at(expected.len() as u64 - 1, &mut tail)
            .expect("probe tail");
        assert_eq!(&tail[..n], &expected[expected.len() - 1..]);
    }

    // A fresh handle's cursor starts at 0; read first, then append — the
    // append must still land at EOF.
    drop(f);
    let mut f = disk.open("interleaved").expect("reopen");
    let mut probe = [0u8; 1];
    assert_eq!(f.read_at(0, &mut probe).expect("read front"), 1);
    f.append(b"Z").expect("append after positioned read");
    expected.push(b'Z');

    assert_eq!(f.len(), expected.len() as u64);
    let mut got = vec![0u8; expected.len()];
    let n = f.read_at(0, &mut got).expect("read everything back");
    assert_eq!(n, expected.len());
    assert_eq!(got, expected, "every append landed at end-of-file");
}

/// The [`Disk`] flat-namespace rule: a name must be a single normal path
/// component, else `InvalidInput` — *before* any storage is touched.
///
/// Panics on the first violation. Requires (and asserts) an empty disk.
pub fn rejects_non_flat_names<D: Disk>(disk: &D) {
    for bad in ["../escape", "sub/dir", "/abs", "", ".", ".."] {
        assert_eq!(
            disk.create(bad).map(|_| ()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
            "create({bad:?}) must be rejected"
        );
        assert_eq!(
            disk.open(bad).map(|_| ()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
            "open({bad:?}) must be rejected"
        );
        assert_eq!(
            disk.remove(bad).unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
            "remove({bad:?}) must be rejected"
        );
    }
    // Nothing leaked into the namespace.
    assert!(disk.list().expect("list").is_empty());
}

/// Run the whole suite, drawing a fresh disk from `fresh` for each check (the
/// checks leave files behind and assume a clean namespace).
pub fn run_all<D: Disk>(mut fresh: impl FnMut() -> D) {
    disk_contract(&fresh());
    positioned_reads_never_move_the_append_cursor(&fresh());
    rejects_non_flat_names(&fresh());
}
