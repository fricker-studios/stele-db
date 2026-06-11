//! Crash-recovery driver integration ([STL-102], [architecture §3.6]).
//!
//! [`Engine::recover`] walks the whole boot flow — validate sealed segments by
//! checksum, load the last checkpoint, replay the WAL forward, rebuild the delta
//! tier and validity index — behind one call. These tests pin the pieces the
//! ticket's Definition of Done calls for:
//!
//! 1. **The roadmap exit criterion** ([docs/03]): insert → update → kill
//!    mid-write → recover → `AS OF` returns the correct pre-crash value.
//! 2. **The validity index is rebuilt *exactly***: a recovered engine's index
//!    materializes byte-for-byte to the pre-crash one ([ADR-0023]).
//! 3. **A torn WAL tail is dropped**: a record left half-written by a crash mid
//!    append never resurrects a partial write; recovery converges to the durable
//!    prefix.
//! 4. **A corrupt sealed segment is refused**: recovery fails loudly rather than
//!    serving history that failed its checksum (invariant: segments are
//!    self-checksummed, corruption is detectable).
//!
//! The deterministic, million-seed kill-and-recover convergence sweep lives in
//! the simulation harness (`stele_sim::run_engine_recover_seed`); this file pins
//! the focused, named cases.

#![allow(clippy::cast_possible_wrap)]

use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};
use stele_storage::backend::{Disk, DiskFile, MemDisk};
use stele_storage::delta::{BusinessKey, Snapshot};
use stele_storage::engine::{Engine, EngineError};

// --- harness ---------------------------------------------------------------

/// A deterministic, strictly-increasing clock — one tick per `now()`. Matches the
/// other storage tests so a failing case reproduces bit-for-bit ([ADR-0010]).
struct StepClock(AtomicI64);
impl StepClock {
    const fn new(start: i64) -> Self {
        Self(AtomicI64::new(start))
    }
}
impl Clock for StepClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

fn key(k: &[u8]) -> BusinessKey {
    BusinessKey::new(k.to_vec())
}

/// Copy every file of `src` into a fresh [`MemDisk`], applying `mutate` to the
/// bytes of the file named `target` (and passing every other file through
/// verbatim). Models an out-of-band on-disk fault — a truncated WAL tail or a
/// flipped segment byte — that a crash could leave behind, which the append-only
/// [`MemDisk`] cannot otherwise express in place.
fn clone_disk_mutating(
    src: &MemDisk,
    target: &str,
    mutate: impl Fn(Vec<u8>) -> Vec<u8>,
) -> MemDisk {
    let out = MemDisk::new();
    for name in src.list().expect("list") {
        let file = src.open(&name).expect("open src");
        let mut bytes = vec![0u8; usize::try_from(file.len()).expect("len fits usize")];
        let read = file.read_at(0, &mut bytes).expect("read");
        bytes.truncate(read);
        if name == target {
            bytes = mutate(bytes);
        }
        let mut dst = out.create(&name).expect("create dst");
        dst.append(&bytes).expect("append");
        dst.sync().expect("sync");
    }
    out
}

/// The single WAL segment filename (workloads here stay well under the 64 MiB
/// rotation threshold, so there is exactly one).
const WAL_SEGMENT: &str = "wal-00000000000000000000.log";
/// A sealed-segment filename under the engine's `seg-*.seg` discovery namespace.
const SEGMENT: &str = "seg-00000000000000000000.seg";

// --- 1. roadmap exit criterion: insert/update/kill/recover/AS OF -----------

/// The v0.1 roadmap exit criterion, driven entirely through [`Engine`]: insert
/// `100`, update to `250`, take a checkpoint, **kill** (drop the engine, keep the
/// disk), [`Engine::recover`], and an `AS OF` before the update still returns the
/// pre-crash value `100` — resolved through the rebuilt validity index — while
/// `AS OF` now returns `250`.
#[test]
fn insert_update_kill_recover_serves_the_correct_as_of() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let (c0, c1) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let c0 = engine
            .insert(k.clone(), None, Some(b"100".to_vec()), 0, TxnId(1), who())
            .expect("insert")
            .commit;
        let c1 = engine
            .update(k.clone(), None, Some(b"250".to_vec()), 0, TxnId(2), who())
            .expect("update")
            .commit;
        engine.checkpoint().expect("checkpoint"); // fsync + record the durable fence
        (c0, c1)
        // engine dropped here — the in-memory delta and index are gone (the crash)
    };

    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c0)).expect("as_of"),
        Some(Some(b"100".to_vec())),
        "recovered AS OF past → 100",
    );
    assert_eq!(
        recovered
            .as_of_payload(&k, Snapshot(SystemTimeMicros(c1.0 - 1)))
            .expect("as_of"),
        Some(Some(b"100".to_vec())),
        "half-open: 100 right up to (but excluding) the close",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c1)).expect("as_of"),
        Some(Some(b"250".to_vec())),
        "recovered AS OF now → 250",
    );
    // The checkpoint persisted the durable fence and recovery loaded it.
    assert!(
        recovered.durable_fence().is_some(),
        "recovery loaded the checkpoint fence",
    );
}

// --- 2. the validity index is rebuilt exactly ------------------------------

/// A mixed insert/update/delete history, then a kill and [`Engine::recover`]: the
/// recovered validity index must materialize byte-for-byte to the pre-crash one —
/// the rebuildability guarantee of [ADR-0023] through the recovery driver.
#[test]
fn recovery_rebuilds_the_exact_validity_index() {
    let disk = MemDisk::new();
    let a = key(b"a");
    let b = key(b"b");

    let before = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open");
        engine
            .insert(a.clone(), None, Some(b"a0".to_vec()), 0, TxnId(1), who())
            .expect("insert a");
        engine
            .insert(b.clone(), None, Some(b"b0".to_vec()), 0, TxnId(2), who())
            .expect("insert b");
        engine
            .update(a.clone(), None, Some(b"a1".to_vec()), 0, TxnId(3), who())
            .expect("update a");
        engine.delete(&b, TxnId(4), who()).expect("delete b");
        engine
            .update(a, None, Some(b"a2".to_vec()), 0, TxnId(5), who())
            .expect("update a again");
        engine.checkpoint().expect("checkpoint");
        engine.materialize_index().expect("materialize")
    };

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000), false).expect("recover");
    let after = recovered.materialize_index().expect("materialize");
    assert_eq!(
        before, after,
        "recovery must rebuild the validity index exactly",
    );

    // Recovering a second time from the same disk is idempotent — same index.
    let again = Engine::recover(disk, StepClock::new(2_000), false)
        .expect("recover again")
        .materialize_index()
        .expect("materialize");
    assert_eq!(after, again, "recovery is idempotent");
}

// --- 3. a torn WAL tail is dropped -----------------------------------------

/// A crash mid-append leaves a half-written final record. Recovery must stop at
/// that torn record (the WAL's torn-write contract) and converge to the durable
/// prefix — never resurrecting the partial write. Modeled by checkpointing after
/// the first commit (so its record is durable, *under* the fence) and truncating
/// the WAL mid-way through the second commit's record (the torn tail, *past* the
/// fence) — exactly the unsynced-tail shape `recover_replay` is meant to tolerate.
#[test]
fn recover_drops_a_torn_wal_tail() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let (c0, w0) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let first = engine
            .insert(k.clone(), None, Some(b"100".to_vec()), 0, TxnId(1), who())
            .expect("insert");
        // Checkpoint here: the first commit is now durable and the fence sits at
        // its record's end, so the second commit lands strictly past the fence.
        engine.checkpoint().expect("checkpoint");
        // A second commit whose WAL record we will truncate mid-write.
        engine
            .update(k.clone(), None, Some(b"250".to_vec()), 0, TxnId(2), who())
            .expect("update");
        (first.commit, first.wal)
    };

    // Truncate the WAL a few bytes into the *second* record: replay applies the
    // first commit, then hits a torn frame and stops.
    let cut = usize::try_from(w0.byte_offset).expect("offset fits usize") + 4;
    let torn = clone_disk_mutating(&disk, WAL_SEGMENT, |mut bytes| {
        bytes.truncate(cut.min(bytes.len()));
        bytes
    });

    let recovered = Engine::recover(torn, StepClock::new(1_000_000), false).expect("recover");
    // The first commit survived; the torn second commit was dropped entirely —
    // the value is `100` at the first commit *and* still `100` "now".
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c0)).expect("as_of"),
        Some(Some(b"100".to_vec())),
        "the durable first commit survived",
    );
    assert_eq!(
        recovered
            .as_of_payload(&k, Snapshot(SystemTimeMicros(c0.0 + 1_000)))
            .expect("as_of"),
        Some(Some(b"100".to_vec())),
        "the torn update was dropped — the insert stays live, no resurrection of the partial write",
    );
    // No close was materialized: the supersession never durably happened.
    assert!(
        recovered
            .materialize_index()
            .expect("materialize")
            .is_empty(),
        "a torn supersession leaves no close in the rebuilt index",
    );
}

/// The other side of the fence: corruption *inside* the durable prefix — a record
/// the checkpoint vouched durable — is fatal, not a tolerated tail. Both commits
/// are checkpointed (the fence sits past both), then the first record's bytes are
/// corrupted; recovery must refuse rather than silently truncate durable history.
#[test]
fn recover_rejects_corruption_inside_the_durable_prefix() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let w0 = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let first = engine
            .insert(k.clone(), None, Some(b"100".to_vec()), 0, TxnId(1), who())
            .expect("insert");
        engine
            .update(k, None, Some(b"250".to_vec()), 0, TxnId(2), who())
            .expect("update");
        // Checkpoint *after both* commits — the fence vouches both records durable.
        engine.checkpoint().expect("checkpoint");
        first.wal
    };

    // Flip a byte inside the first record (under the fence): a CRC failure the
    // checkpoint promised could not happen — real durable-region corruption.
    let flip_at = usize::try_from(w0.byte_offset).expect("offset fits usize") - 1;
    let corrupt = clone_disk_mutating(&disk, WAL_SEGMENT, |mut bytes| {
        bytes[flip_at] ^= 0xFF;
        bytes
    });

    match Engine::recover(corrupt, StepClock::new(1_000_000), false) {
        Ok(_) => panic!("corruption inside the durable prefix must fail recovery"),
        Err(EngineError::Dml(_)) => {}
        Err(other) => panic!("expected a WAL-replay error, got {other:?}"),
    }
}

// --- 4. a corrupt committed segment is refused -----------------------------

/// A *committed* sealed segment (one the checkpoint manifest vouches) whose bytes
/// were corrupted must fail recovery's checksum validation — recovery refuses
/// rather than serving history that failed its checksum. A clean copy of the same
/// segment recovers fine, proving the failure is the corruption and not the setup.
#[test]
fn recover_rejects_a_corrupt_committed_segment() {
    let disk = MemDisk::new();
    let k = key(b"k");

    // A committed flush: the insert is sealed into seg-0 and vouched by the
    // checkpoint manifest, so recovery validates that segment by checksum.
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(10), false).expect("open");
        engine
            .insert(k, None, Some(b"v".to_vec()), 0, TxnId(1), who())
            .expect("insert");
        engine.flush().expect("flush");
        assert_eq!(
            engine.segment_names().len(),
            1,
            "the flush sealed one segment"
        );
    }

    // A clean recover validates the committed segment and succeeds.
    Engine::recover(disk.clone(), StepClock::new(1), false).expect("clean recover");

    // Flip a byte in the middle of the segment payload; recovery must reject it.
    let corrupt = clone_disk_mutating(&disk, SEGMENT, |mut bytes| {
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        bytes
    });
    match Engine::recover(corrupt, StepClock::new(1), false) {
        Ok(_) => panic!("a corrupt committed segment must fail recovery, not pass"),
        Err(EngineError::Segment(_)) => {}
        Err(other) => panic!("expected a segment checksum error, got {other:?}"),
    }
}

// --- 5. flush bounds replay to the WAL tail (STL-177) -----------------------

/// After a [`Engine::flush`], recovery replays **only the tail**: the flushed
/// prefix is rebuilt from the sealed segment, not the WAL, so corruption in the
/// pre-floor WAL is irrelevant. Proven directly — flush the whole history, flip a
/// byte deep inside the (now-redundant) WAL prefix, and recovery still succeeds
/// with the correct `AS OF`, where a full-log replay would have hit the corruption
/// and failed.
#[test]
fn flush_bounds_recovery_to_the_wal_tail() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let (c0, c1) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let c0 = engine
            .insert(k.clone(), None, Some(b"100".to_vec()), 0, TxnId(1), who())
            .expect("insert")
            .commit;
        let c1 = engine
            .update(k.clone(), None, Some(b"250".to_vec()), 0, TxnId(2), who())
            .expect("update")
            .commit;
        // Flush seals both versions into seg-0 and advances the floor to the log
        // end — there is no tail left to replay.
        let floor = engine.flush().expect("flush");
        assert!(
            floor > stele_storage::wal::LogOffset::ZERO,
            "floor advanced"
        );
        assert_eq!(engine.segment_names().len(), 1, "one sealed segment");
        (c0, c1)
    };

    // Corrupt the WAL *prefix* — a record the flush already folded into the
    // segment. A full-log replay would choke on it; a tail-bounded replay never
    // reads it.
    let corrupt = clone_disk_mutating(&disk, WAL_SEGMENT, |mut bytes| {
        bytes[8] ^= 0xFF; // a byte well inside the first (flushed) record
        bytes
    });

    let recovered = Engine::recover(corrupt, StepClock::new(1_000_000), false)
        .expect("recovery ignores the corrupt, already-flushed WAL prefix");
    assert_eq!(
        recovered.segment_names().len(),
        1,
        "the committed segment is read back",
    );
    assert!(
        recovered.replay_floor() > stele_storage::wal::LogOffset::ZERO,
        "recovery resumes from the advanced floor, not the log origin",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c0)).expect("as_of"),
        Some(Some(b"100".to_vec())),
        "pre-update value served from the sealed segment + rebuilt index",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c1)).expect("as_of"),
        Some(Some(b"250".to_vec())),
        "post-update value served from the sealed segment + rebuilt index",
    );
}

/// A flush mid-history, then more writes, then a kill with no final flush:
/// recovery composes the **segment prefix** (rebuilt from seg-0) with the **WAL
/// tail** (replayed from the floor). Every `AS OF` boundary resolves correctly and
/// the validity index is rebuilt exactly.
#[test]
fn flush_then_tail_writes_recover_correctly() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let (c0, c1, c2, before) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let c0 = engine
            .insert(k.clone(), None, Some(b"100".to_vec()), 0, TxnId(1), who())
            .expect("insert")
            .commit;
        let c1 = engine
            .update(k.clone(), None, Some(b"250".to_vec()), 0, TxnId(2), who())
            .expect("update")
            .commit;
        // Flush the prefix into seg-0; the floor now sits past these two records.
        engine.flush().expect("flush");
        // Tail writes after the flush — these live only in the WAL until recovery.
        let c2 = engine
            .update(k.clone(), None, Some(b"400".to_vec()), 0, TxnId(3), who())
            .expect("tail update")
            .commit;
        let before = engine.materialize_index().expect("materialize");
        (c0, c1, c2, before)
        // dropped — no final flush, so the tail is recovered via WAL replay
    };

    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c0)).expect("as_of"),
        Some(Some(b"100".to_vec())),
        "segment-prefix value",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c1)).expect("as_of"),
        Some(Some(b"250".to_vec())),
        "segment-prefix value after the in-segment supersession",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c2)).expect("as_of"),
        Some(Some(b"400".to_vec())),
        "tail value replayed from the WAL on top of the segment prefix",
    );
    assert_eq!(
        before,
        recovered.materialize_index().expect("materialize"),
        "the index rebuilt from segment + tail equals the pre-crash one",
    );
}

/// The crash-during-flush invariant at the storage layer: a segment written by a
/// flush whose checkpoint record never became durable is an **orphan** recovery
/// ignores, falling back to the WAL. Modeled by flushing cleanly, then removing
/// the checkpoint file (the manifest never committed) **and** corrupting the
/// segment (a torn write) — recovery must still succeed by replaying the WAL,
/// proving the orphan was never trusted.
#[test]
fn recover_ignores_an_uncommitted_orphan_segment() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let (c0, c1) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let c0 = engine
            .insert(k.clone(), None, Some(b"100".to_vec()), 0, TxnId(1), who())
            .expect("insert")
            .commit;
        let c1 = engine
            .update(k.clone(), None, Some(b"250".to_vec()), 0, TxnId(2), who())
            .expect("update")
            .commit;
        engine.flush().expect("flush"); // writes seg-0 + the committing manifest
        (c0, c1)
    };

    // Drop the checkpoint file (the manifest that committed seg-0) and corrupt the
    // orphaned segment — exactly the on-disk shape of a crash after the segment
    // fsync but before the checkpoint record was durable.
    let orphaned = {
        let out = MemDisk::new();
        for name in disk.list().expect("list") {
            if name == "stele.checkpoint" {
                continue; // the manifest never became durable
            }
            let file = disk.open(&name).expect("open");
            let mut bytes = vec![0u8; usize::try_from(file.len()).expect("len")];
            let read = file.read_at(0, &mut bytes).expect("read");
            bytes.truncate(read);
            if name == SEGMENT {
                let mid = bytes.len() / 2;
                bytes[mid] ^= 0xFF; // the orphan is also torn
            }
            let mut dst = out.create(&name).expect("create");
            dst.append(&bytes).expect("append");
            dst.sync().expect("sync");
        }
        out
    };

    // No manifest ⇒ seg-0 is an uncommitted orphan: recovery ignores it (never
    // validating its corrupt bytes) and rebuilds purely from the WAL.
    let recovered = Engine::recover(orphaned, StepClock::new(1_000_000), false)
        .expect("recovery falls back to the WAL for an uncommitted orphan segment");
    assert!(
        recovered.segment_names().is_empty(),
        "the orphan segment is not adopted",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c0)).expect("as_of"),
        Some(Some(b"100".to_vec())),
        "pre-update value rebuilt from the WAL",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c1)).expect("as_of"),
        Some(Some(b"250".to_vec())),
        "post-update value rebuilt from the WAL",
    );
}

/// A second flush appends a second committed segment and advances the floor
/// again — recovery reads both and replays nothing.
#[test]
fn two_flushes_commit_two_segments_and_bound_replay() {
    let disk = MemDisk::new();
    let a = key(b"a");
    let b = key(b"b");

    let (ca, cb) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open");
        let ca = engine
            .insert(a.clone(), None, Some(b"a0".to_vec()), 0, TxnId(1), who())
            .expect("insert a")
            .commit;
        engine.flush().expect("flush 1");
        let cb = engine
            .insert(b.clone(), None, Some(b"b0".to_vec()), 0, TxnId(2), who())
            .expect("insert b")
            .commit;
        let floor2 = engine.flush().expect("flush 2");
        assert_eq!(engine.segment_names().len(), 2, "two committed segments");
        assert_eq!(engine.replay_floor(), floor2, "floor at the second flush");
        (ca, cb)
    };

    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    assert_eq!(
        recovered.segment_names().len(),
        2,
        "both segments read back"
    );
    assert_eq!(
        recovered.as_of_payload(&a, Snapshot(ca)).expect("as_of"),
        Some(Some(b"a0".to_vec())),
    );
    assert_eq!(
        recovered.as_of_payload(&b, Snapshot(cb)).expect("as_of"),
        Some(Some(b"b0".to_vec())),
    );
}

// --- 6. the checkpoint records the last fully-flushed WAL offset ------------

/// [`Engine::checkpoint`] advances the persisted durable fence: after a second
/// checkpoint past more writes, recovery loads the newer offset.
#[test]
fn checkpoint_advances_the_persisted_durable_fence() {
    let disk = MemDisk::new();
    let k = key(b"k");

    let fence1;
    let fence2;
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open");
        engine
            .insert(k.clone(), None, Some(b"v0".to_vec()), 0, TxnId(1), who())
            .expect("insert");
        fence1 = engine.checkpoint().expect("checkpoint 1");
        engine
            .update(k, None, Some(b"v1".to_vec()), 0, TxnId(2), who())
            .expect("update");
        fence2 = engine.checkpoint().expect("checkpoint 2");
    }
    assert!(
        fence2 > fence1,
        "the second checkpoint advanced past more writes"
    );

    let recovered = Engine::recover(disk, StepClock::new(1_000), false).expect("recover");
    assert_eq!(
        recovered.durable_fence(),
        Some(fence2),
        "recovery loads the most recent checkpoint",
    );
}

// --- close-all-open: the DROP TABLE storage half (STL-211) ------------------

/// `close_all_open` retires every system-live row with an append-only close at
/// the drop instant — the storage half of `DROP TABLE` ([STL-211]). The closes
/// must (a) skip a key already in a deletion gap, (b) leave an `AS OF` read
/// inside the pre-drop era untouched (append-only, [ADR-0023]), and (c) survive
/// recovery, since the session keeps the tier resident and a re-created name
/// reuses it.
#[test]
fn close_all_open_retires_live_rows_and_survives_recovery() {
    let disk = MemDisk::new();
    let a = key(b"a");
    let b = key(b"b");
    let c = key(b"c");

    // StepClock ticks one micro per write: a@1, b@2, c@3, delete-c closes at 4.
    // Instant 4 is the "drop": a and b are open, c is already gone.
    let drop_at = Snapshot(SystemTimeMicros(4));
    let now = Snapshot(SystemTimeMicros(100));

    let mut engine = Engine::open(disk.clone(), StepClock::new(1), false).expect("open");
    engine
        .insert(a.clone(), None, Some(b"a0".to_vec()), 0, TxnId(1), who())
        .expect("insert a");
    engine
        .insert(b.clone(), None, Some(b"b0".to_vec()), 0, TxnId(2), who())
        .expect("insert b");
    engine
        .insert(c.clone(), None, Some(b"c0".to_vec()), 0, TxnId(3), who())
        .expect("insert c");
    engine.delete(&c, TxnId(4), who()).expect("delete c");

    let closed = engine
        .close_all_open(drop_at, TxnId(9), &who())
        .expect("close all open");
    assert_eq!(
        closed, 2,
        "only the two open keys (a, b) close; the deleted c is skipped"
    );

    // After the drop, a current read finds nothing — every row is closed.
    assert!(engine.as_of(&a, now).expect("a").is_none(), "a is closed");
    assert!(engine.as_of(&b, now).expect("b").is_none(), "b is closed");
    assert!(
        engine.as_of(&c, now).expect("c").is_none(),
        "c was already deleted"
    );

    // Append-only: an AS OF read at the drop instant still resolves the rows
    // open then — the closes added a `sys_to`, they did not erase the versions.
    assert!(
        engine.as_of(&a, drop_at).expect("a@drop").is_some(),
        "a is still open AS OF the drop instant"
    );
    assert!(
        engine.as_of(&b, drop_at).expect("b@drop").is_some(),
        "b is still open AS OF the drop instant"
    );
    assert!(
        engine.as_of(&c, drop_at).expect("c@drop").is_none(),
        "c was retracted before the drop, so it is absent there too"
    );

    // The closes are durable: recovery reconstructs the same answers.
    engine.checkpoint().expect("checkpoint");
    drop(engine);
    let recovered = Engine::recover(disk, StepClock::new(1_000), false).expect("recover");
    assert!(
        recovered.as_of(&a, now).expect("a").is_none(),
        "the close survived recovery"
    );
    assert!(recovered.as_of(&b, now).expect("b").is_none());
    assert!(
        recovered.as_of(&a, drop_at).expect("a@drop").is_some(),
        "AS OF still sees the pre-drop open row after recovery"
    );
}
