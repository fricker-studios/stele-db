//! History-preserving compaction oracle ([STL-231], [ADR-0030]).
//!
//! [Testing strategy §4] names "a compaction that drops a version" as a headline
//! bug class, so compaction lands with its oracle in the same change. The pieces:
//!
//! 1. **Differential equality across the swap.** Over a seed sweep of random
//!    histories (system-only *and* valid-time tables), every `AS OF` read at
//!    every snapshot — plus the fully-materialized validity index — must be
//!    identical **before compaction, after compaction, and after a recovery
//!    that rebuilds from the compacted segment store alone**. The last leg is
//!    the sharp one: it proves the consolidated segment still carries every
//!    version row and every retraction tombstone the inputs did
//!    ([`rebuild_index_from_segments`], [ADR-0023]).
//! 2. **The deletion gap survives.** The STL-143 resurrection bug class,
//!    pinned explicitly: a deleted key must stay deleted across compaction +
//!    rebuild, not resurrect via adjacency inference.
//! 3. **Crash safety at both edges of the swap.** A failure *before* the
//!    manifest append leaves the inputs live and the output a dead orphan; a
//!    failure *after* (retirement interrupted) leaves the output live and
//!    recovery sweeps the retired files. Never half ([ADR-0030]).
//! 4. **Retirement is not mutation** (invariant 1, extending [STL-186]'s
//!    oracle): every segment name present both before and after an operation
//!    has byte-identical content; a *failed* compaction leaves the inputs
//!    byte-identical too.
//!
//! [STL-231]: https://allegromusic.atlassian.net/browse/STL-231
//! [Testing strategy §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::hash::{Digest, sha256};
use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, ValidTimeMicros};
use stele_storage::backend::{Disk, DiskFile, FaultOp, Faults, MemDisk};
use stele_storage::delta::{BusinessKey, Snapshot, Version};
use stele_storage::engine::Engine;
use stele_storage::validity::ClosedInterval;
use stele_storage::validtime::ValidInterval;

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

/// Tiny xorshift64* — deterministic, dependency-free; matches the other storage
/// tests so a failing seed reproduces bit-for-bit ([ADR-0010]).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    const fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    const fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// The business key for pool slot `k` — `['k', k]`.
fn key_of(k: u8) -> BusinessKey {
    BusinessKey::new(vec![b'k', k])
}

/// Read the complete byte content of one file on `disk`.
fn read_all(disk: &MemDisk, name: &str) -> Vec<u8> {
    let file = disk.open(name).expect("open");
    let mut bytes = vec![0u8; usize::try_from(file.len()).expect("len fits usize")];
    let read = file.read_at(0, &mut bytes).expect("read");
    bytes.truncate(read);
    bytes
}

/// Content-hash every sealed segment (`seg-*.seg`) currently on `disk` — the
/// immutability probe shared with the [STL-186] oracle.
fn segment_digests(disk: &MemDisk) -> BTreeMap<String, Digest> {
    let mut out = BTreeMap::new();
    for name in disk.list().expect("list") {
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

/// Invariant 1 across an operation: every segment name present **both** before
/// and after has byte-identical content. Disappearing (retirement) and
/// appearing (a fresh output) are legal; mutation in place never is.
fn assert_no_segment_mutated(
    before: &BTreeMap<String, Digest>,
    after: &BTreeMap<String, Digest>,
    step: &str,
) {
    for (name, digest) in before {
        if let Some(now) = after.get(name) {
            assert_eq!(
                digest.to_hex(),
                now.to_hex(),
                "sealed segment {name} was mutated in place by {step}",
            );
        }
    }
}

/// The full observable read surface of an engine: every key's resolved version
/// at **every** snapshot in `0..=t_max` (payload, provenance, and the
/// index-overlaid `sys_to` / `closed_by` all participate via `Version`
/// equality), plus the fully-materialized validity index.
type ReadSurface = (
    BTreeMap<(u8, i64), Option<Version>>,
    BTreeMap<(BusinessKey, SystemTimeMicros, u64), ClosedInterval>,
);

fn capture<C: Clock>(engine: &Engine<C, MemDisk>, keys: u8, t_max: i64) -> ReadSurface {
    let mut reads = BTreeMap::new();
    for k in 0..keys {
        let key = key_of(k);
        for t in 0..=t_max {
            let resolved = engine
                .as_of(&key, Snapshot(SystemTimeMicros(t)))
                .expect("as_of");
            reads.insert((k, t), resolved);
        }
    }
    (reads, engine.materialize_index().expect("materialize"))
}

const KEYS: u8 = 6;
const OPS: usize = 48;

/// Drive a random seeded history through the engine: inserts, updates, and
/// deletes over a small key pool, flushing at fixed points so the history is
/// spread across several sealed segments plus a still-staged delta tail.
/// Returns the first unused clock tick (the capture grid's upper bound).
fn run_workload(engine: &mut Engine<StepClock, MemDisk>, seed: u64, valid_time: bool) -> i64 {
    let mut rng = Rng::new(seed);
    let mut live: BTreeSet<u8> = BTreeSet::new();
    let mut seq = 0u64;
    for op in 0..OPS {
        // Flush at the quarter boundaries — several sealed segments, while the
        // final quarter stays staged in the delta across the compaction.
        if op > 0 && op % (OPS / 4) == 0 {
            engine.flush().expect("flush");
        }
        seq += 1;
        let txn = TxnId(seq);
        let k = rng.range(u64::from(KEYS)) as u8;
        let valid = valid_time.then(|| {
            let from = rng.range(100) as i64;
            let to = from + 1 + rng.range(100) as i64;
            ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("interval")
        });
        let payload = Some(format!("v{seq}-k{k}").into_bytes());
        if live.contains(&k) {
            if rng.range(5) == 0 {
                engine.delete(&key_of(k), txn, who()).expect("delete");
                live.remove(&k);
            } else {
                engine
                    .update(key_of(k), valid, payload, seq, txn, who())
                    .expect("update");
            }
        } else {
            engine
                .insert(key_of(k), valid, payload, seq, txn, who())
                .expect("insert");
            live.insert(k);
        }
    }
    OPS as i64 + 2
}

// --- 1. the seeded differential across the swap -----------------------------

/// The DoD's headline: every `AS OF` read returns identical results before
/// compaction, after compaction, **and after recovering from the compacted
/// segment store alone** — across a seed sweep, on both table flavors.
fn as_of_reads_are_identical_across_compaction(valid_time: bool) {
    for seed in 0..24u64 {
        let disk = MemDisk::new();
        let mut engine =
            Engine::recover(disk.clone(), StepClock::new(0), valid_time).expect("boot");
        let t_max = run_workload(&mut engine, seed, valid_time);

        let inputs = engine.segment_names().to_vec();
        assert!(
            inputs.len() >= 2,
            "seed {seed}: the workload must give compaction real input ({inputs:?})",
        );
        let before = capture(&engine, KEYS, t_max);
        let digests_before = segment_digests(&disk);

        let outcome = engine.compact().expect("compact");
        assert_eq!(outcome.segments_in, inputs.len(), "seed {seed}");
        let output = outcome.output.clone().expect("an output was written");
        assert_eq!(
            engine.segment_names(),
            std::slice::from_ref(&output),
            "seed {seed}: the live set swapped to the single output",
        );
        let digests_after = segment_digests(&disk);
        assert_no_segment_mutated(&digests_before, &digests_after, "compact");
        assert_eq!(
            digests_after.keys().collect::<Vec<_>>(),
            vec![&output],
            "seed {seed}: the inputs were retired from disk; only the output remains",
        );

        let after = capture(&engine, KEYS, t_max);
        assert_eq!(before, after, "seed {seed}: compaction changed a read");

        // The crash: drop the engine, rebuild from the compacted store + WAL
        // tail. The rebuilt index and every read must come back identical.
        drop(engine);
        let recovered =
            Engine::recover(disk, StepClock::new(t_max + 1), valid_time).expect("recover");
        assert_eq!(
            recovered.segment_names(),
            &[output],
            "seed {seed}: recovery trusts exactly the compacted output",
        );
        let post_recovery = capture(&recovered, KEYS, t_max);
        assert_eq!(
            before, post_recovery,
            "seed {seed}: recovery from the compacted store changed a read",
        );
    }
}

#[test]
fn as_of_reads_are_identical_across_compaction_system_only() {
    as_of_reads_are_identical_across_compaction(false);
}

#[test]
fn as_of_reads_are_identical_across_compaction_valid_time() {
    as_of_reads_are_identical_across_compaction(true);
}

// --- 2. the deletion gap (STL-143 resurrection bug class) -------------------

/// Compacting must carry the retraction tombstones, not re-infer closes from
/// version adjacency: a deleted key stays deleted across compaction *and*
/// across a rebuild from the compacted store — the insert → delete → (gap)
/// history must not resurrect.
#[test]
fn the_deletion_gap_survives_compaction_and_rebuild() {
    let disk = MemDisk::new();
    let mut engine = Engine::recover(disk.clone(), StepClock::new(0), false).expect("boot");
    let k = key_of(1);

    engine
        .insert(k.clone(), None, Some(b"100".to_vec()), 1, TxnId(1), who())
        .expect("insert"); // sys_from = 0
    engine
        .update(k.clone(), None, Some(b"250".to_vec()), 2, TxnId(2), who())
        .expect("update"); // sys_from = 1 (closes 0)
    engine.flush().expect("flush 1");
    engine.delete(&k, TxnId(3), who()).expect("delete"); // closes 1 at t=2
    engine
        .insert(key_of(2), None, Some(b"77".to_vec()), 4, TxnId(4), who())
        .expect("unrelated insert keeps the segment non-empty");
    engine.flush().expect("flush 2");

    let probe = |e: &Engine<StepClock, MemDisk>, label: &str| {
        let at = |t: i64| {
            e.as_of(&k, Snapshot(SystemTimeMicros(t)))
                .expect("as_of")
                .map(|v| v.payload.expect("non-NULL payload"))
        };
        assert_eq!(at(0), Some(b"100".to_vec()), "{label}: first version");
        assert_eq!(at(1), Some(b"250".to_vec()), "{label}: updated version");
        assert_eq!(at(2), None, "{label}: deleted at t=2");
        assert_eq!(at(50), None, "{label}: the gap holds — no resurrection");
    };
    probe(&engine, "before compaction");

    let outcome = engine.compact().expect("compact");
    assert_eq!(outcome.segments_in, 2);
    probe(&engine, "after compaction");

    drop(engine);
    let recovered = Engine::recover(disk, StepClock::new(100), false).expect("recover");
    probe(&recovered, "after rebuild from the compacted store");
}

// --- 3. crash safety at both edges of the swap ------------------------------

/// Build a healthy two-segment engine over `disk` with a known read surface.
fn two_segment_engine(disk: &MemDisk) -> (Engine<StepClock, MemDisk>, ReadSurface, i64) {
    let mut engine = Engine::recover(disk.clone(), StepClock::new(0), false).expect("boot");
    engine
        .insert(key_of(1), None, Some(b"a".to_vec()), 1, TxnId(1), who())
        .expect("insert");
    engine.flush().expect("flush 1");
    engine
        .insert(key_of(2), None, Some(b"b".to_vec()), 2, TxnId(2), who())
        .expect("insert");
    engine
        .update(key_of(1), None, Some(b"a2".to_vec()), 3, TxnId(3), who())
        .expect("update");
    engine.flush().expect("flush 2");
    let t_max = 8;
    let surface = capture(&engine, 3, t_max);
    (engine, surface, t_max)
}

/// A failure **before** the swap commits (here: the directory fence after the
/// output was written) must leave the inputs live — in memory and across a
/// recovery, where the unvouched output is a dead orphan that gets removed —
/// and must not have touched a byte of them.
#[test]
fn a_failure_before_the_swap_leaves_the_inputs_live_and_the_output_orphaned() {
    let faults = Faults::new();
    let disk = MemDisk::with_faults(faults.clone());
    let (mut engine, surface, t_max) = two_segment_engine(&disk);
    let inputs = engine.segment_names().to_vec();
    let digests_before = segment_digests(&disk);

    // The only `sync_dir` on the compaction path is its directory fence, after
    // the output segment is fully written and fsync'd — the crash point right
    // before the swap could commit.
    faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
    engine.compact().expect_err("the fenced compaction fails");
    assert_eq!(faults.pending(), 0, "the fault fired at the fence");

    assert_eq!(
        engine.segment_names(),
        inputs.as_slice(),
        "the inputs are still the live set",
    );
    assert_eq!(
        capture(&engine, 3, t_max),
        surface,
        "no read changed under the failed compaction",
    );
    let digests_now = segment_digests(&disk);
    assert_no_segment_mutated(&digests_before, &digests_now, "the failed compaction");
    assert_eq!(
        digests_now.len(),
        3,
        "the orphan output is still on disk (2 live inputs + 1 dead orphan)",
    );

    // Recovery: the manifest never vouched the output, so it is removed; the
    // inputs serve, and a healthy retry compacts cleanly.
    drop(engine);
    let mut recovered = Engine::recover(disk.clone(), StepClock::new(50), false).expect("recover");
    assert_eq!(
        recovered.segment_names(),
        inputs.as_slice(),
        "recovery trusts exactly the inputs",
    );
    assert_eq!(segment_digests(&disk).len(), 2, "the orphan was swept");
    assert_eq!(capture(&recovered, 3, t_max), surface);

    let outcome = recovered.compact().expect("the retry compacts");
    assert_eq!(outcome.segments_in, 2);
    assert_eq!(capture(&recovered, 3, t_max), surface);
}

/// A failure **after** the swap commits (retirement interrupted) must leave the
/// output live: the lingering retired inputs are dead weight reads never touch,
/// and recovery sweeps them.
#[test]
fn an_interrupted_retirement_leaves_the_output_live_and_recovery_sweeps_the_inputs() {
    let faults = Faults::new();
    let disk = MemDisk::with_faults(faults.clone());
    let (mut engine, surface, t_max) = two_segment_engine(&disk);

    // Three removes on the compaction path: the pre-create orphan clear at the
    // output's name, then the two input retirements. Fail them all — the
    // "crash before cleanup" shape.
    for _ in 0..3 {
        faults.schedule(FaultOp::Remove, io::ErrorKind::Other);
    }
    let outcome = engine.compact().expect("retirement is best-effort");
    assert_eq!(faults.pending(), 0, "all three removes were attempted");
    assert_eq!(outcome.segments_in, 2);
    let output = outcome.output.expect("output written");

    assert_eq!(
        engine.segment_names(),
        std::slice::from_ref(&output),
        "the live set is the output alone, despite the lingering files",
    );
    assert_eq!(
        segment_digests(&disk).len(),
        3,
        "the retired inputs linger on disk",
    );
    assert_eq!(
        capture(&engine, 3, t_max),
        surface,
        "reads never touch the retired files",
    );

    drop(engine);
    let recovered = Engine::recover(disk.clone(), StepClock::new(50), false).expect("recover");
    assert_eq!(
        recovered.segment_names(),
        &[output],
        "recovery trusts exactly the committed output",
    );
    assert_eq!(
        segment_digests(&disk).len(),
        1,
        "recovery swept the retired inputs",
    );
    assert_eq!(capture(&recovered, 3, t_max), surface);
}

// --- 4. the no-op edge -------------------------------------------------------

/// With fewer than two live segments there is nothing to merge: `compact` is a
/// no-op (and therefore idempotent — a second `COMPACT` right after a first is
/// free), and it never touches the delta tier.
#[test]
fn compact_below_two_segments_is_a_noop_and_compaction_is_idempotent() {
    let disk = MemDisk::new();
    let mut engine = Engine::recover(disk, StepClock::new(0), false).expect("boot");

    // Zero segments (delta only): no-op, the staged row stays staged.
    engine
        .insert(key_of(1), None, Some(b"a".to_vec()), 1, TxnId(1), who())
        .expect("insert");
    let outcome = engine.compact().expect("compact");
    assert_eq!(outcome.segments_in, 0);
    assert_eq!(outcome.output, None);
    assert!(engine.segment_names().is_empty(), "nothing was sealed");

    // One segment: still a no-op.
    engine.flush().expect("flush");
    let one = engine.segment_names().to_vec();
    let outcome = engine.compact().expect("compact");
    assert_eq!((outcome.segments_in, outcome.output), (0, None));
    assert_eq!(engine.segment_names(), one.as_slice(), "live set untouched");

    // Two segments: a real merge — and the immediate re-run is the no-op again.
    engine
        .insert(key_of(2), None, Some(b"b".to_vec()), 2, TxnId(2), who())
        .expect("insert");
    engine.flush().expect("flush");
    let outcome = engine.compact().expect("compact");
    assert_eq!(outcome.segments_in, 2);
    let outcome = engine.compact().expect("compact again");
    assert_eq!((outcome.segments_in, outcome.output), (0, None));
}
