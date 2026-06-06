//! Read-path & recovery `AS OF` correctness oracle (STL-136, [ADR-0023]).
//!
//! STL-133/134/135 moved `sys_to` off the record entirely: a version stores only
//! its birth state (`sys_from` + payload + provenance), and its system-time *end*
//! lives once in the derived, rebuildable [validity index]. This file pins the two
//! halves of STL-136 — that the **read path** resolves a version's close *through*
//! that index on every `AS OF`, and that **recovery** rebuilds the index from the
//! WAL so a recovered engine serves the same answers a crash discarded.
//!
//! The three things the ticket's Definition of Done calls for:
//!
//! 1. **The four-statement identity demo** ([docs/05]) — `INSERT 100` → `UPDATE
//!    250` → `SELECT … FOR SYSTEM_TIME AS OF (past)` returns **100**, the value
//!    *before* the update. Here the insert is flushed into a sealed segment before
//!    the update, so the close is a write-once index entry over a sealed version
//!    (no `sys_to` is stored anywhere) and the pre-update value is recovered purely
//!    by overlaying the index at read time.
//! 2. **Kill-mid-write → recover → `AS OF` correct.** Drop the in-memory delta and
//!    index (the crash), rebuild both from the WAL alone ([`dml::replay`]), and the
//!    recovered engine answers the same `AS OF` — including the pre-crash value.
//! 3. **Differential-equal to a reference oracle.** Over a seed sweep of random
//!    histories, a hand-coded linear-scan model is the source of truth: at *every*
//!    boundary snapshot the engine's index-resolved read must match it exactly,
//!    before and after recovery. (The reference is in-process and deterministic per
//!    [ADR-0010]; a DuckDB differential belongs at the SQL/executor layer, not in
//!    this runtime-agnostic core, and rides STL-100/138.)

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SYSTEM_TIME_OPEN, SystemTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot, Version};
use stele_storage::dml::{self, DmlWriter};
use stele_storage::merge;
use stele_storage::segment::{SegmentReader, SegmentWriter};
use stele_storage::systime::{EmptySealed, SealedVersions, SysTimeWriter};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::wal::{Checkpoint, Wal, WalConfig};

// --- harness ---------------------------------------------------------------

/// A deterministic, strictly-increasing clock — one tick per `now()`. Matches the
/// other storage tests' `StepClock` so a failing case reproduces bit-for-bit
/// ([ADR-0010]).
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

/// A hand-driven clock behind a shared atomic — for the focused demo, where the
/// test sets the exact wall position around a flush boundary.
#[derive(Clone)]
struct StubClock(Arc<AtomicI64>);
impl StubClock {
    fn new(start: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start)))
    }
    fn advance(&self, by: i64) {
        self.0.fetch_add(by, Ordering::Relaxed);
    }
}
impl Clock for StubClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.load(Ordering::Relaxed))
    }
}

/// Tiny xorshift64* — deterministic, dependency-free; matches the other storage
/// tests so a failing seed reproduces bit-for-bit ([ADR-0010]).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

// --- 1. the four-statement identity demo -----------------------------------

/// The v0.1 identity ([docs/05]), at the storage layer and across a flush
/// boundary: insert `balance = 100`, seal it into a segment, then update to
/// `250`. The update cannot mutate the sealed version (invariant 1) — it writes
/// the close once into the validity index. A snapshot read *before* the update
/// must still return `100`, resolved by overlaying that index onto the sealed,
/// otherwise-open version. After the update it returns `250`.
#[test]
fn four_statement_identity_demo_resolves_the_pre_update_value_via_the_index() {
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let seg_disk = MemDisk::new();
    let clock = StubClock::new(1_000);
    let mut writer = SysTimeWriter::new(clock.clone());
    let key = BusinessKey::new(b"account-1".to_vec());

    // INSERT INTO account VALUES (1, 100);
    let c0 = writer
        .insert(
            &mut delta,
            &mut index,
            &SealedVersions::default(),
            key.clone(),
            b"100".to_vec(),
            TxnId(1),
            who(),
        )
        .expect("insert");

    // The open version is flushed out of the delta into a sealed segment, so its
    // close can only ever be an index entry — there is no `sys_to` column to set.
    let sealed = seal(
        &seg_disk,
        "account-0.seg",
        delta.flush_to_segment().expect("flush"),
    );
    assert_eq!(sealed.len(), 1);
    assert_eq!(
        sealed[0].sys_to, SYSTEM_TIME_OPEN,
        "sealed open, end unresolved"
    );

    // UPDATE account SET balance = 250 WHERE id = 1;
    clock.advance(1_000_000); // a second later, mirroring the demo's `interval '1 second'`
    let c1 = writer
        .update(
            &mut delta,
            &mut index,
            &SealedVersions::new(sealed.clone()),
            key.clone(),
            b"250".to_vec(),
            TxnId(2),
            who(),
        )
        .expect("update");
    assert!(c0 < c1);

    // SELECT … FOR SYSTEM_TIME AS OF (now() - interval '1 second') → 100.
    // The pre-update value is resolved purely from the index overlay over the
    // sealed version; nothing stored the close.
    assert_eq!(
        as_of(&delta, &sealed, &index, &key, c0),
        Some(b"100".to_vec()),
        "time-travel before the update returns the pre-update value via the index",
    );
    assert_eq!(
        as_of(&delta, &sealed, &index, &key, SystemTimeMicros(c1.0 - 1)),
        Some(b"100".to_vec()),
        "the prior period is live right up to (but excluding) the close — half-open",
    );

    // SELECT … (current) → 250.
    assert_eq!(
        as_of(&delta, &sealed, &index, &key, c1),
        Some(b"250".to_vec()),
        "at and after the close the new version is live",
    );

    // And the close lives only in the index, never on a record.
    assert_eq!(
        index.len().expect("len"),
        1,
        "exactly one materialized close"
    );
    assert_eq!(
        index
            .close_of(&key, c0)
            .expect("lookup")
            .expect("c0 closed")
            .sys_to,
        c1,
        "the index closes the pre-update period at the update's commit",
    );
}

// --- 2. kill-mid-write → recover → AS OF correct ---------------------------

/// The v0.1 roadmap exit criterion ([docs/03]): insert, update, **kill**,
/// recover, and an `AS OF` query returns the correct pre-crash value. The crash
/// is modeled by discarding the in-memory delta and index outright and rebuilding
/// both from the WAL alone ([`dml::replay`]) — the WAL is the only durable truth
/// ([architecture §3.6]). The recovered index must resolve the pre-update value
/// exactly as the live engine did.
#[test]
fn kill_mid_write_then_recover_serves_the_correct_as_of() {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    let mut dml = DmlWriter::new(wal.clone(), StepClock::new(1_000), false);
    let key = BusinessKey::new(b"account-1".to_vec());

    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"100".to_vec(),
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;
    let c1 = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            None,
            b"250".to_vec(),
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit;
    wal.tick().expect("fsync"); // the writes are durable

    // --- crash: the in-memory delta and index are gone. Rebuild from the WAL. ---
    let mut recovered = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut recovered_index =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
    dml::replay(
        &wal,
        &mut recovered,
        &mut recovered_index,
        Checkpoint::BEGIN,
    )
    .expect("replay");

    // The rebuilt index resolves the prior period's close, so AS OF before the
    // update returns the pre-crash value, and AS OF now returns the update.
    let read = |s: SystemTimeMicros| -> Option<Vec<u8>> {
        let live = recovered
            .range_scan(.., Snapshot(s), &recovered_index)
            .expect("scan");
        live.into_iter()
            .find(|v| v.business_key == key)
            .map(|v| v.payload)
    };
    assert_eq!(
        read(c0),
        Some(b"100".to_vec()),
        "recovered AS OF past → 100"
    );
    assert_eq!(
        read(SystemTimeMicros(c1.0 - 1)),
        Some(b"100".to_vec()),
        "recovered read is half-open: 100 right up to the close",
    );
    assert_eq!(read(c1), Some(b"250".to_vec()), "recovered AS OF now → 250");

    // The close was reconstructed into the index, not onto any record.
    assert_eq!(
        recovered_index
            .close_of(&key, c0)
            .expect("lookup")
            .expect("closed")
            .sys_to,
        c1,
        "recovery rebuilt the prior period's close in the validity index",
    );
}

// --- 3. differential reference oracle, before and after recovery -----------

/// One period in the reference model: a half-open `[from, to)` system-time
/// interval carrying the payload asserted for it. `to == SYSTEM_TIME_OPEN` is the
/// currently-live period.
#[derive(Clone)]
struct Period {
    from: i64,
    to: i64,
    payload: Vec<u8>,
}

/// The reference answer at snapshot `s`: for each key, the payload of the period
/// whose `[from, to)` contains `s` (at most one — the 2D-tiling invariant). A
/// linear scan — the deliberately-naive oracle the engine's index-resolved read
/// is checked against.
fn reference_as_of(model: &[Vec<Period>], s: i64) -> BTreeMap<BusinessKey, Vec<u8>> {
    let mut live = BTreeMap::new();
    for (k, periods) in model.iter().enumerate() {
        if let Some(p) = periods.iter().find(|p| p.from <= s && s < p.to) {
            live.insert(BusinessKey::new(vec![b'k', k as u8]), p.payload.clone());
        }
    }
    live
}

/// The engine's answer at snapshot `s`: range-scan the delta tier, resolving each
/// version's end from the validity index, and project to `key → payload`.
///
/// A duplicate key is a hard failure, not a silent overwrite: at any snapshot at
/// most one version per key may be live (the [2D-tiling invariant], docs/16 §5),
/// so two live rows for one key is exactly the correctness bug this oracle exists
/// to catch — collecting into the map blind would hide it.
fn engine_as_of(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    s: i64,
) -> BTreeMap<BusinessKey, Vec<u8>> {
    let mut live = BTreeMap::new();
    for v in delta
        .range_scan(.., Snapshot(SystemTimeMicros(s)), index)
        .expect("scan")
    {
        let key = v.business_key.clone();
        assert!(
            live.insert(v.business_key, v.payload).is_none(),
            "@ s={s}: range_scan returned two live versions for {key:?} — \
             the at-most-one-active-version invariant is broken",
        );
    }
    live
}

/// Over a seed sweep of random INSERT/UPDATE/DELETE histories: at *every* boundary
/// snapshot, the live engine, a crash-recovered engine, and a hand-coded reference
/// oracle all return byte-identical `AS OF` results. This is the read-path heart of
/// STL-136 — index-resolved visibility, proven differentially and proven to survive
/// recovery, with the half-open boundaries probed exhaustively.
#[test]
fn recovery_rebuilds_the_index_and_serves_correct_as_of_under_seed_sweep() {
    const KEY_POOL: u64 = 6;
    const START: i64 = 1_000;

    for seed in 0u64..200 {
        let mut rng = Rng::new(seed);
        let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
        let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
        // System-time only: payloads are raw, so the reference value is the bytes.
        let mut dml = DmlWriter::new(wal.clone(), StepClock::new(START), false);

        let mut model: Vec<Vec<Period>> = vec![Vec::new(); KEY_POOL as usize];
        let mut live = vec![false; KEY_POOL as usize];
        let mut hi = START;

        let ops = 20 + rng.range(40);
        for op in 0..ops {
            let k = rng.range(KEY_POOL) as usize;
            let key = BusinessKey::new(vec![b'k', k as u8]);
            let txn = TxnId(op);
            let payload = format!("k{k}-op{op}").into_bytes();

            if live[k] {
                if rng.range(2) == 0 {
                    let c = dml
                        .delete(&mut delta, &mut index, &EmptySealed, &key, txn, who())
                        .expect("delete")
                        .commit;
                    close_open(&mut model[k], c.0);
                    live[k] = false;
                    hi = hi.max(c.0);
                } else {
                    let c = dml
                        .update(
                            &mut delta,
                            &mut index,
                            &EmptySealed,
                            key,
                            None,
                            payload.clone(),
                            txn,
                            who(),
                        )
                        .expect("update")
                        .commit;
                    close_open(&mut model[k], c.0);
                    model[k].push(Period {
                        from: c.0,
                        to: SYSTEM_TIME_OPEN.0,
                        payload,
                    });
                    hi = hi.max(c.0);
                }
            } else {
                let c = dml
                    .insert(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        None,
                        payload.clone(),
                        txn,
                        who(),
                    )
                    .expect("insert")
                    .commit;
                model[k].push(Period {
                    from: c.0,
                    to: SYSTEM_TIME_OPEN.0,
                    payload,
                });
                live[k] = true;
                hi = hi.max(c.0);
            }
        }
        wal.tick().expect("fsync");

        // Crash and rebuild the delta *and* index purely from the WAL.
        let mut recovered = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
        let mut recovered_index =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
        dml::replay(
            &wal,
            &mut recovered,
            &mut recovered_index,
            Checkpoint::BEGIN,
        )
        .expect("replay");

        // Probe every integer snapshot from just before the first commit to just
        // past the last — exhaustively exercising the half-open boundaries that
        // are the classic off-by-one trap ([docs/06]).
        for s in (START - 2)..=(hi + 2) {
            let expected = reference_as_of(&model, s);
            assert_eq!(
                engine_as_of(&delta, &index, s),
                expected,
                "seed {seed} @ s={s}: live read must match the reference oracle",
            );
            assert_eq!(
                engine_as_of(&recovered, &recovered_index, s),
                expected,
                "seed {seed} @ s={s}: recovered read must match the reference oracle",
            );
        }
    }
}

// --- helpers ---------------------------------------------------------------

/// Close the model's currently-open period at `commit`. There is always exactly
/// one open period when this is called.
fn close_open(chain: &mut [Period], commit: i64) {
    let open = chain.last_mut().expect("a live key has an open period");
    assert_eq!(
        open.to, SYSTEM_TIME_OPEN.0,
        "the period being closed was open"
    );
    open.to = commit;
}

/// Write `rows` into a fresh sealed segment and read every version back — the real
/// columnar flush boundary. Readers see open/unresolved birth state; the end lives
/// in the index.
fn seal(disk: &MemDisk, name: &str, rows: Vec<Version>) -> Vec<Version> {
    let mut writer = SegmentWriter::create(disk, name).expect("create segment");
    for v in rows {
        writer.push(v).expect("push");
    }
    writer.finish().expect("finish");
    SegmentReader::open(disk, name)
        .expect("open segment")
        .read_versions()
        .expect("read versions")
}

/// The payload live for `key` at snapshot `s`, reading across the sealed pool and
/// the delta tier with the validity index supplying each version's end — the
/// cross-tier `AS OF` a query executor performs ([`merge::fold_chains`] +
/// [`merge::resolve_snapshot`]).
fn as_of(
    delta: &Delta<MemDisk>,
    sealed: &[Version],
    index: &ValidityIndex<MemDisk>,
    key: &BusinessKey,
    s: SystemTimeMicros,
) -> Option<Vec<u8>> {
    let delta_versions = delta.candidate_versions(key).expect("candidates");
    let sealed_versions = sealed.iter().filter(|v| &v.business_key == key).cloned();
    let chains = merge::fold_chains(sealed_versions.chain(delta_versions), index).expect("fold");
    merge::resolve_snapshot(&chains, Snapshot(s))
        .into_iter()
        .find(|v| &v.business_key == key)
        .map(|v| v.payload)
}
