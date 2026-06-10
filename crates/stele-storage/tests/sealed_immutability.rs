//! Immutable-segment invariant oracle ([STL-186], [architecture §12] invariant 1).
//!
//! "No in-place mutation of a sealed segment. Ever." — enforced here as a test,
//! not a code comment. Every sealed segment is content-hashed (SHA-256 over the
//! full file bytes) the moment its flush commits, and the recorded digests are
//! re-verified after every later lifecycle step that could plausibly touch the
//! file: further DML that *supersedes* rows living inside the sealed segment,
//! later flushes (which persist those closes as retraction tombstones in **new**
//! segments), checkpoints, `AS OF` reads, index materialization, crash recovery,
//! and post-recovery writes. If any code path ever rewrites a committed segment
//! in place, the digest comparison here fails the gate (`just check` runs this
//! file under nextest).
//!
//! Compaction (v0.3) is explicitly out of scope: it *rewrites into new segments*
//! and retires old ones — never edits one in place — and gets its own oracle when
//! it lands.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::hash::{Digest, sha256};
use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_storage::backend::{Disk, DiskFile, MemDisk};
use stele_storage::delta::{BusinessKey, Snapshot};
use stele_storage::engine::Engine;
use stele_storage::validtime::{ValidInterval, unframe_payload};

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

/// Read the complete byte content of one file on `disk`.
fn read_all(disk: &MemDisk, name: &str) -> Vec<u8> {
    let file = disk.open(name).expect("open");
    let mut bytes = vec![0u8; usize::try_from(file.len()).expect("len fits usize")];
    let read = file.read_at(0, &mut bytes).expect("read");
    bytes.truncate(read);
    bytes
}

/// Content-hash every sealed segment (`seg-*.seg`) currently on `disk`.
fn segment_digests(disk: &MemDisk) -> BTreeMap<String, Digest> {
    let mut out = BTreeMap::new();
    for name in disk.list().expect("list") {
        // Mirrors the engine's own discovery of its zero-padded
        // `seg-{index:020}.seg` filenames (`engine::segment_name`).
        let is_segment = name
            .strip_prefix("seg-")
            .and_then(|rest| rest.strip_suffix(".seg"))
            .is_some();
        if is_segment {
            let digest = sha256(&read_all(disk, &name));
            out.insert(name, digest);
        }
    }
    out
}

/// The invariant check: every segment in `sealed` must still exist on `disk`
/// with byte-identical content. `step` names the lifecycle step just performed,
/// so a violation reports exactly where the mutation happened.
fn assert_sealed_unchanged(disk: &MemDisk, sealed: &BTreeMap<String, Digest>, step: &str) {
    let now = segment_digests(disk);
    for (name, expected) in sealed {
        match now.get(name) {
            None => panic!("sealed segment {name} disappeared after {step}"),
            Some(actual) if actual != expected => panic!(
                "sealed segment {name} was mutated in place after {step}: \
                 sealed as sha256:{}, now sha256:{}",
                expected.to_hex(),
                actual.to_hex(),
            ),
            Some(_) => {}
        }
    }
}

/// Record the digests of any segments not yet tracked in `sealed` — called right
/// after a flush commits, so each committed segment's *recorded* digest is the
/// one taken at seal time (`or_insert` never overwrites an earlier record, even
/// though re-listing the disk re-hashes every segment present).
fn adopt_new_segments(disk: &MemDisk, sealed: &mut BTreeMap<String, Digest>) {
    for (name, digest) in segment_digests(disk) {
        sealed.entry(name).or_insert(digest);
    }
}

// --- the lifecycle oracle ----------------------------------------------------

/// The headline check: sealed segments are byte-identical across the **full
/// DML → flush → checkpoint → read → crash → recover → write-again lifecycle**.
///
/// The history is built so that later steps have maximal motive to touch sealed
/// bytes: rows sealed into `seg-0` are superseded (updated and deleted) *after*
/// sealing, so their closes must land as retraction tombstones in `seg-1` —
/// never as edits to `seg-0`. Recovery then rebuilds the validity index from
/// those same segments, and post-recovery writes seal a third segment.
#[test]
fn sealed_segments_are_byte_identical_across_the_full_lifecycle() {
    let disk = MemDisk::new();
    let a = key(b"a");
    let b = key(b"b");
    let c = key(b"c");
    let mut sealed = BTreeMap::new();

    let (c_a1, c_b0, before) = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");

        // Epoch 1: two live keys, one in-delta supersession; seal into seg-0.
        engine
            .insert(a.clone(), None, Some(b"a0".to_vec()), 0, TxnId(1), who())
            .expect("insert a");
        let c_b0 = engine
            .insert(b.clone(), None, Some(b"b0".to_vec()), 0, TxnId(2), who())
            .expect("insert b")
            .commit;
        let c_a1 = engine
            .update(a.clone(), None, Some(b"a1".to_vec()), 0, TxnId(3), who())
            .expect("update a")
            .commit;
        engine.flush().expect("flush 1");
        adopt_new_segments(&disk, &mut sealed);
        assert_eq!(sealed.len(), 1, "flush 1 sealed seg-0");

        // Epoch 2: supersede rows that live *inside* sealed seg-0 — the closes
        // must materialize as tombstones in the next segment, not edits here.
        engine
            .update(a.clone(), None, Some(b"a2".to_vec()), 0, TxnId(4), who())
            .expect("update a over sealed");
        assert_sealed_unchanged(&disk, &sealed, "an UPDATE superseding a sealed row");
        engine.delete(&b, TxnId(5), who()).expect("delete b");
        assert_sealed_unchanged(&disk, &sealed, "a DELETE closing a sealed row");

        engine.flush().expect("flush 2");
        assert_sealed_unchanged(&disk, &sealed, "the second flush");
        adopt_new_segments(&disk, &mut sealed);
        assert_eq!(sealed.len(), 2, "flush 2 sealed seg-1");

        // A durability fence and reads — none of these may write segment bytes.
        engine.checkpoint().expect("checkpoint");
        assert_sealed_unchanged(&disk, &sealed, "a checkpoint");
        assert_eq!(
            engine.as_of_payload(&a, Snapshot(c_a1)).expect("as_of"),
            Some(Some(b"a1".to_vec())),
        );
        let before = engine.materialize_index().expect("materialize");
        assert_sealed_unchanged(&disk, &sealed, "AS OF reads + index materialization");

        // A WAL-tail write that stays unflushed across the crash, so recovery
        // exercises segment-prefix + tail-replay composition.
        engine
            .insert(c, None, Some(b"c0".to_vec()), 0, TxnId(6), who())
            .expect("tail insert c");
        (c_a1, c_b0, before)
        // engine dropped here — the crash
    };
    assert_sealed_unchanged(&disk, &sealed, "the crash (engine drop)");

    // Recovery validates both committed segments, rebuilds the index from them,
    // and replays the WAL tail — all read-only with respect to sealed bytes.
    let mut recovered =
        Engine::recover(disk.clone(), StepClock::new(1_000_000), false).expect("recover");
    assert_sealed_unchanged(&disk, &sealed, "crash recovery");
    assert_eq!(recovered.segment_names().len(), 2, "both segments adopted");
    assert_eq!(
        recovered.as_of_payload(&a, Snapshot(c_a1)).expect("as_of"),
        Some(Some(b"a1".to_vec())),
        "history inside seg-0 still served",
    );
    assert_eq!(
        recovered.as_of_payload(&b, Snapshot(c_b0)).expect("as_of"),
        Some(Some(b"b0".to_vec())),
        "the deleted key's pre-delete history still served",
    );
    assert_eq!(
        before,
        recovered.materialize_index().expect("materialize"),
        "the index rebuilt from sealed segments + WAL tail equals the pre-crash one",
    );
    assert_sealed_unchanged(&disk, &sealed, "post-recovery reads");

    // Life goes on: post-recovery DML and a third flush seal a *new* segment and
    // leave the first two untouched.
    recovered
        .update(a, None, Some(b"a3".to_vec()), 0, TxnId(7), who())
        .expect("post-recovery update");
    recovered.flush().expect("flush 3");
    assert_sealed_unchanged(&disk, &sealed, "a post-recovery flush");
    adopt_new_segments(&disk, &mut sealed);
    assert_eq!(sealed.len(), 3, "flush 3 sealed seg-2");
    drop(recovered);

    // A second recovery over all three segments is read-only too.
    Engine::recover(disk.clone(), StepClock::new(2_000_000), false).expect("recover again");
    assert_sealed_unchanged(&disk, &sealed, "a second recovery");
}

/// The same invariant with valid-time enabled: bitemporal rows carry the extra
/// `ValidFrom`/`ValidTo` segment columns, and superseding a sealed bitemporal row
/// closes it on both axes — still strictly via tombstones in later segments.
#[test]
fn sealed_segments_stay_immutable_in_valid_time_mode() {
    let disk = MemDisk::new();
    let k = key(b"policy-1");
    let mut sealed = BTreeMap::new();

    let c0 = {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), true).expect("open");
        let c0 = engine
            .insert(
                k.clone(),
                Some(
                    ValidInterval::new(ValidTimeMicros(100), VALID_TIME_OPEN)
                        .expect("well-formed interval"),
                ),
                Some(b"v0".to_vec()),
                0,
                TxnId(1),
                who(),
            )
            .expect("insert")
            .commit;
        engine.flush().expect("flush 1");
        adopt_new_segments(&disk, &mut sealed);
        assert_eq!(sealed.len(), 1, "flush 1 sealed seg-0");

        // Supersede the sealed bitemporal row, then seal the close.
        engine
            .update(
                k.clone(),
                Some(
                    ValidInterval::new(ValidTimeMicros(200), VALID_TIME_OPEN)
                        .expect("well-formed interval"),
                ),
                Some(b"v1".to_vec()),
                0,
                TxnId(2),
                who(),
            )
            .expect("update over sealed");
        engine.flush().expect("flush 2");
        assert_sealed_unchanged(&disk, &sealed, "a bitemporal supersession + flush");
        adopt_new_segments(&disk, &mut sealed);
        c0
    };
    assert_sealed_unchanged(&disk, &sealed, "the crash (engine drop)");

    let recovered = Engine::recover(disk.clone(), StepClock::new(1_000_000), true)
        .expect("recover a valid-time table");
    // Valid-time payloads are stored framed with their interval prefix; unframe
    // before comparing the business payload.
    let framed = recovered
        .as_of_payload(&k, Snapshot(c0))
        .expect("as_of")
        .flatten()
        .expect("key live with a payload at c0");
    let (interval, payload) = unframe_payload(true, &framed).expect("unframe");
    assert_eq!(
        payload, b"v0",
        "pre-supersession bitemporal history still served from seg-0",
    );
    assert_eq!(
        interval,
        Some(ValidInterval::new(ValidTimeMicros(100), VALID_TIME_OPEN).expect("interval")),
        "the sealed row's valid interval round-tripped",
    );
    assert_sealed_unchanged(&disk, &sealed, "valid-time crash recovery + reads");
}

// --- the oracle has teeth ----------------------------------------------------

/// Detection sensitivity: the digest comparison fails on **any** single-byte,
/// length-preserving in-place mutation — the one corruption shape a CRC-style
/// spot check could conceivably miss. Every byte of every sealed segment is
/// flipped in turn and each flip must change the content hash. (Growth and
/// truncation trivially change the hashed byte stream too.)
#[test]
fn the_digest_oracle_detects_a_flip_of_any_sealed_byte() {
    let disk = MemDisk::new();
    {
        let mut engine = Engine::open(disk.clone(), StepClock::new(1_000), false).expect("open");
        engine
            .insert(key(b"k"), None, Some(b"v".to_vec()), 0, TxnId(1), who())
            .expect("insert");
        engine.flush().expect("flush");
    }

    let sealed = segment_digests(&disk);
    assert!(!sealed.is_empty(), "the flush sealed a segment");
    for (name, original) in &sealed {
        let bytes = read_all(&disk, name);
        for at in 0..bytes.len() {
            let mut mutated = bytes.clone();
            mutated[at] ^= 0xFF;
            assert_ne!(
                &sha256(&mutated),
                original,
                "a flip of byte {at} in {name} must change the content hash",
            );
        }
    }
}
