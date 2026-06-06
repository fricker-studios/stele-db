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
use stele_storage::delta::{BusinessKey, Snapshot, Version};
use stele_storage::engine::{Engine, EngineError};
use stele_storage::segment::SegmentWriter;

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
            .insert(k.clone(), None, b"100".to_vec(), 0, TxnId(1), who())
            .expect("insert")
            .commit;
        let c1 = engine
            .update(k.clone(), None, b"250".to_vec(), 0, TxnId(2), who())
            .expect("update")
            .commit;
        engine.checkpoint().expect("checkpoint"); // fsync + record the durable fence
        (c0, c1)
        // engine dropped here — the in-memory delta and index are gone (the crash)
    };

    let recovered = Engine::recover(disk, StepClock::new(1_000_000), false).expect("recover");
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c0)).expect("as_of"),
        Some(b"100".to_vec()),
        "recovered AS OF past → 100",
    );
    assert_eq!(
        recovered
            .as_of_payload(&k, Snapshot(SystemTimeMicros(c1.0 - 1)))
            .expect("as_of"),
        Some(b"100".to_vec()),
        "half-open: 100 right up to (but excluding) the close",
    );
    assert_eq!(
        recovered.as_of_payload(&k, Snapshot(c1)).expect("as_of"),
        Some(b"250".to_vec()),
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
            .insert(a.clone(), None, b"a0".to_vec(), 0, TxnId(1), who())
            .expect("insert a");
        engine
            .insert(b.clone(), None, b"b0".to_vec(), 0, TxnId(2), who())
            .expect("insert b");
        engine
            .update(a.clone(), None, b"a1".to_vec(), 0, TxnId(3), who())
            .expect("update a");
        engine.delete(&b, TxnId(4), who()).expect("delete b");
        engine
            .update(a, None, b"a2".to_vec(), 0, TxnId(5), who())
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
/// prefix — never resurrecting the partial write. Modeled by truncating the WAL
/// just past the first commit's record, so the second commit's record is torn.
#[test]
fn recover_drops_a_torn_wal_tail() {
    let disk = MemDisk::new();
    let k = key(b"account-1");

    let (c0, w0) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        let first = engine
            .insert(k.clone(), None, b"100".to_vec(), 0, TxnId(1), who())
            .expect("insert");
        // A second commit whose WAL record we will truncate mid-write.
        engine
            .update(k.clone(), None, b"250".to_vec(), 0, TxnId(2), who())
            .expect("update");
        engine.checkpoint().expect("checkpoint");
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
        Some(b"100".to_vec()),
        "the durable first commit survived",
    );
    assert_eq!(
        recovered
            .as_of_payload(&k, Snapshot(SystemTimeMicros(c0.0 + 1_000)))
            .expect("as_of"),
        Some(b"100".to_vec()),
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

// --- 4. a corrupt sealed segment is refused --------------------------------

/// A sealed segment whose bytes were corrupted must fail recovery's checksum
/// validation — recovery refuses rather than serving history that failed its
/// checksum. A clean copy of the same segment recovers fine, proving the failure
/// is the corruption and not the setup.
#[test]
fn recover_rejects_a_corrupt_sealed_segment() {
    let disk = MemDisk::new();

    // Write one valid sealed segment under the engine's discovery namespace.
    let mut w = SegmentWriter::create(&disk, SEGMENT).expect("create segment");
    w.push(Version::open(
        key(b"k"),
        SystemTimeMicros(10),
        0,
        stele_common::provenance::Provenance::new(TxnId(1), SystemTimeMicros(10), who()),
        b"v".to_vec(),
    ))
    .expect("push");
    w.finish().expect("finish");

    // A clean recover validates the segment and succeeds.
    Engine::recover(disk.clone(), StepClock::new(1), false).expect("clean recover");

    // Flip a byte in the middle of the segment payload; recovery must reject it.
    let corrupt = clone_disk_mutating(&disk, SEGMENT, |mut bytes| {
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        bytes
    });
    match Engine::recover(corrupt, StepClock::new(1), false) {
        Ok(_) => panic!("a corrupt sealed segment must fail recovery, not pass"),
        Err(EngineError::Segment(_)) => {}
        Err(other) => panic!("expected a segment checksum error, got {other:?}"),
    }
}

// --- 5. the checkpoint records the last fully-flushed WAL offset ------------

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
            .insert(k.clone(), None, b"v0".to_vec(), 0, TxnId(1), who())
            .expect("insert");
        fence1 = engine.checkpoint().expect("checkpoint 1");
        engine
            .update(k, None, b"v1".to_vec(), 0, TxnId(2), who())
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
