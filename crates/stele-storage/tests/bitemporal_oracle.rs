//! Bitemporal correctness oracle aligned to the formal spec (STL-138, [docs/16],
//! [docs/06] §4/§10, [ADR-0023]).
//!
//! STL-134/135/136 proved the *system* axis differentially: a hand-coded reference
//! and the engine agree on every `AS OF (sys)` read. This file lifts the reference
//! to the **full bitemporal model** the spec defines — every assertion is a
//! `(key, sys_interval, valid_interval, value)` tuple, and an `AS OF (sys, valid)`
//! query is answered by **brute force** over those tuples ([docs/16 §3]). It keeps
//! the system-time end off the record exactly as the engine does: a version's
//! `sys_to` is *materialized as the next assertion's `sys_from`* (or open),
//! matching [ADR-0023] — the reference never stores a `sys_to` the writer didn't
//! imply.
//!
//! Three things land here, all runtime-agnostic and deterministic ([ADR-0010]):
//!
//! 1. **Both-axis differential.** Over a seed sweep of random INSERT/UPDATE/DELETE
//!    histories on a *valid-time* table, at every `(sys, valid)` grid point the
//!    live engine, a crash-recovered engine, and the brute-force reference return
//!    byte-identical results — the half-open boundaries on *both* axes probed
//!    exhaustively (the classic off-by-one trap, [docs/06 §4]).
//! 2. **A deliberate `sys_to` mutation is caught.** Materializing a close one tick
//!    later than the successor's `sys_from` (the canonical [ADR-0023] violation)
//!    makes the prior version linger into its successor's reign; the differential
//!    equality fires on it — proven both in a focused hand-built case and across
//!    the seed sweep.
//! 3. **Metamorphic invariance** ([docs/16 §9], [docs/06 §10]): splitting a
//!    valid-time interval into two adjacent identical-value pieces, reordering the
//!    asserted tuples, and coalescing the split back must not change *any* query
//!    result.
//!
//! The second, independent **DuckDB** differential ([docs/06 §10]) is deliberately
//! *not* here: it reimplements the queries at the SQL/executor layer and diffs
//! there, which needs the executor (STL-100) and a new external dependency — out
//! of scope for this runtime-agnostic core. It landed as STL-144 in
//! [`stele-exec/tests/duckdb_differential.rs`], riding the [`SnapshotScan`] read
//! path.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{
    Clock, SYSTEM_TIME_OPEN, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros,
};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::{self, DmlWriter};
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, unframe_payload};
use stele_storage::wal::{Checkpoint, Wal, WalConfig};

// --- knobs -----------------------------------------------------------------

/// Distinct business keys exercised per seed. Small so the per-seed `(sys, valid)`
/// grid stays exhaustively probeable while keys still collide and supersede.
const KEY_POOL: u8 = 4;
/// First system-time commit; the sweep probes from just before this.
const START: i64 = 1_000;
/// Upper bound of the (bounded) valid-time domain. Intervals live in `[0, VMAX]`
/// so every integer valid point — and thus every half-open boundary — is probed.
const VMAX: i64 = 24;

// --- harness ---------------------------------------------------------------

/// A deterministic, strictly-increasing clock — one tick per `now()`. Matches the
/// other storage oracles' `StepClock` so a failing seed reproduces bit-for-bit
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

/// A deterministic clock that issues each µs **twice** before advancing, so two
/// consecutive commits land on the *same* `sys_from`. This is the same-tick
/// collision the force-bump used to paper over (STL-145): with it gone, the two
/// versions keep the shared µs and are ordered only by their distinct `seq`. The
/// reading never moves backwards, so the writer's non-regression guard accepts
/// every tick.
struct StallClock(AtomicI64);
impl StallClock {
    const fn new(start: i64) -> Self {
        // `2*start` so the emitted value (`counter / 2`) begins at `start`.
        Self(AtomicI64::new(start * 2))
    }
}
impl Clock for StallClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed) / 2)
    }
}

/// Tiny xorshift64* — deterministic, dependency-free; matches the other storage
/// oracles so a failing seed reproduces bit-for-bit ([ADR-0010]).
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

/// A random well-formed valid-time interval inside `[0, vmax]`. Occasionally
/// open-ended (`[from, +∞)`) to exercise the `+∞` sentinel on the valid axis.
fn gen_valid(rng: &mut Rng, vmax: i64) -> ValidInterval {
    let from = rng.range((vmax - 1) as u64) as i64; // 0..=vmax-2
    if rng.range(4) == 0 {
        ValidInterval::new(ValidTimeMicros(from), VALID_TIME_OPEN).expect("open interval")
    } else {
        let span = 1 + rng.range((vmax - from) as u64) as i64; // 1..=vmax-from
        ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(from + span))
            .expect("bounded interval")
    }
}

// --- the reference model ---------------------------------------------------

/// One asserted version in the bitemporal reference: a half-open system-time
/// period `[sys_from, sys_to)` carrying a half-open valid-time interval
/// `[valid_from, valid_to)` and a value. `sys_to == SYSTEM_TIME_OPEN` is the
/// currently-live period; per [ADR-0023] the end is *not* asserted independently —
/// it is materialized as the next assertion's `sys_from`.
#[derive(Clone, Debug)]
struct Tuple {
    key: u8,
    sys_from: i64,
    /// Per-commit sequence number ([ADR-0024], STL-145). With the force-bump
    /// gone, two versions of one key can share a `sys_from`; `(sys_from, seq)` is
    /// the total order, and a same-tick supersession closes the prior version
    /// degenerately (`sys_to == sys_from`) so only the higher-`seq` version is
    /// ever live. The brute-force resolver below needs no special case — a
    /// degenerate `[c, c)` system period contains no point — but the field rides
    /// on every tuple so the reference and engine fold the same identity.
    seq: u64,
    sys_to: i64,
    valid_from: i64,
    valid_to: i64,
    value: Vec<u8>,
}

/// `AS OF (s, v)` by brute force over the tuple set ([docs/16 §3]): the value of
/// the **unique** tuple whose system interval contains `s` *and* whose valid
/// interval contains `v`, or ABSENT. Uniqueness is the 2D-tiling invariant
/// ([docs/16 §5]); a second live tuple is the bug this oracle exists to catch, so
/// it is asserted, not silently resolved.
fn reference_as_of(model: &[Tuple], key: u8, s: i64, v: i64) -> Option<Vec<u8>> {
    let live: Vec<&Tuple> = model
        .iter()
        .filter(|t| {
            t.key == key && t.sys_from <= s && s < t.sys_to && t.valid_from <= v && v < t.valid_to
        })
        .collect();
    assert!(
        live.len() <= 1,
        "more than one tuple live at (s={s}, v={v}) for key {key} — 2D-tiling broken",
    );
    // docs/16 §3: among the versions visible at `(s, v)`, the live one is the
    // maximal by `(sys_from, seq)` — the tiebreak that totally orders same-tick
    // commits (STL-145). Half-open tiling already makes this set a singleton (a
    // same-tick supersession leaves the prior with a degenerate `[c, c)` period
    // that contains no point), so the max *is* that sole element; resolving by
    // `(sys_from, seq)` explicitly is what folds `seq` into the reference's order.
    live.into_iter()
        .max_by_key(|t| (t.sys_from, t.seq))
        .map(|t| t.value.clone())
}

/// A *buggy* resolver that models a stale `sys_to`: when two versions overlap on
/// the system axis it returns the **prior** one (smallest `sys_from`) instead of
/// flagging the overlap. This is what a too-late close does in practice — the old
/// value lingers. Used only to prove the oracle catches such a mutation.
fn buggy_as_of(model: &[Tuple], key: u8, s: i64, v: i64) -> Option<Vec<u8>> {
    model
        .iter()
        .filter(|t| {
            t.key == key && t.sys_from <= s && s < t.sys_to && t.valid_from <= v && v < t.valid_to
        })
        .min_by_key(|t| t.sys_from)
        .map(|t| t.value.clone())
}

/// Close the model's currently-open period for `key` at `commit` — the reference
/// analogue of materializing a close in the validity index ([ADR-0023]). Exactly
/// one open period exists for a live key.
fn close_open(model: &mut [Tuple], key: u8, commit: i64) {
    let open = model
        .iter_mut()
        .rev()
        .find(|t| t.key == key && t.sys_to == SYSTEM_TIME_OPEN.0)
        .expect("a live key has exactly one open period");
    open.sys_to = commit;
}

/// Clone the model with the first closed period's `sys_to` shifted by `delta` —
/// the canonical [ADR-0023] violation (a close materialized later than the
/// successor's `sys_from`). `None` when no period is closed (an insert-only seed).
fn mutate_first_close(model: &[Tuple], delta: i64) -> Option<Vec<Tuple>> {
    let idx = model.iter().position(|t| t.sys_to != SYSTEM_TIME_OPEN.0)?;
    let mut out = model.to_vec();
    out[idx].sys_to += delta;
    Some(out)
}

// --- the engine read side --------------------------------------------------

/// The engine's system-live versions at snapshot `s`, projected to
/// `key → (valid interval, user payload)`. Range-scan resolves the system axis
/// through the validity index; the valid interval is recovered from the framed
/// payload ([`unframe_payload`]). A duplicate key is a hard failure: at most one
/// version per key may be system-live ([docs/16 §5]).
fn engine_live(
    delta: &Delta<MemDisk>,
    index: &ValidityIndex<MemDisk>,
    s: i64,
) -> BTreeMap<u8, (ValidInterval, Vec<u8>)> {
    let mut live = BTreeMap::new();
    for v in delta
        .range_scan(.., Snapshot(SystemTimeMicros(s)), index)
        .expect("scan")
    {
        let key = v.business_key.0[1];
        let (interval, user) =
            unframe_payload(true, v.payload.as_deref().unwrap()).expect("unframe");
        let interval = interval.expect("valid-time table carries an interval");
        assert!(
            live.insert(key, (interval, user.to_vec())).is_none(),
            "@ s={s}: range_scan returned two live versions for key {key} — \
             the at-most-one-active-version invariant is broken",
        );
    }
    live
}

/// The engine's `AS OF (s, v)` for one key, built from a system-live row: the user
/// payload iff its valid interval contains `v`, else ABSENT.
fn engine_point(live: Option<&(ValidInterval, Vec<u8>)>, v: i64) -> Option<Vec<u8>> {
    live.and_then(|(interval, user)| interval.contains(ValidTimeMicros(v)).then(|| user.clone()))
}

// --- the seeded scenario ---------------------------------------------------

/// A built history: the live engine, a crash-recovered engine, the reference
/// model, and the last system-time commit observed.
struct Scenario {
    delta: Delta<MemDisk>,
    index: ValidityIndex<MemDisk>,
    recovered: Delta<MemDisk>,
    recovered_index: ValidityIndex<MemDisk>,
    model: Vec<Tuple>,
    hi: i64,
}

/// Apply one seed's random INSERT/UPDATE/DELETE history (valid-time table) to both
/// the engine and the reference, fsync, then crash-rebuild a second engine from the
/// WAL alone ([`dml::replay`]).
fn run_seed(seed: u64) -> Scenario {
    // The default sweep uses a strictly-increasing clock — distinct `sys_from` per
    // commit. The same-tick variant ([`run_seed_same_tick`]) feeds a stalling
    // clock so commits collide on `sys_from` and `seq` carries the order.
    run_seed_with(seed, StepClock::new(START))
}

/// Like [`run_seed`] but with a clock that issues each µs twice, so consecutive
/// commits share a `sys_from` and `(sys_from, seq)` ordering is exercised
/// end-to-end (STL-145).
fn run_seed_same_tick(seed: u64) -> Scenario {
    run_seed_with(seed, StallClock::new(START))
}

fn run_seed_with<C: Clock>(seed: u64, clock: C) -> Scenario {
    let mut rng = Rng::new(seed);
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
    // `true`: valid-time opt-in — every write carries an interval (the second axis).
    let mut dml = DmlWriter::new(wal.clone(), clock, true);

    let mut model: Vec<Tuple> = Vec::new();
    let mut live = vec![false; KEY_POOL as usize];
    let mut hi = START;

    let ops = 16 + rng.range(32);
    for op in 0..ops {
        let k = rng.range(u64::from(KEY_POOL)) as u8;
        let key = BusinessKey::new(vec![b'k', k]);
        let txn = TxnId(op);
        let value = format!("k{k}-op{op}").into_bytes();

        if live[k as usize] {
            if rng.range(2) == 0 {
                let c = dml
                    .delete(&mut delta, &mut index, &EmptySealed, &key, txn, who())
                    .expect("delete")
                    .commit;
                close_open(&mut model, k, c.0);
                live[k as usize] = false;
                hi = hi.max(c.0);
            } else {
                let interval = gen_valid(&mut rng, VMAX);
                let c = dml
                    .update(
                        &mut delta,
                        &mut index,
                        &EmptySealed,
                        key,
                        Some(interval),
                        Some(value.clone()),
                        op,
                        txn,
                        who(),
                    )
                    .expect("update")
                    .commit;
                close_open(&mut model, k, c.0);
                model.push(Tuple {
                    key: k,
                    sys_from: c.0,
                    seq: op,
                    sys_to: SYSTEM_TIME_OPEN.0,
                    valid_from: interval.from.0,
                    valid_to: interval.to.0,
                    value,
                });
                hi = hi.max(c.0);
            }
        } else {
            let interval = gen_valid(&mut rng, VMAX);
            let c = dml
                .insert(
                    &mut delta,
                    &mut index,
                    &EmptySealed,
                    key,
                    Some(interval),
                    Some(value.clone()),
                    op,
                    txn,
                    who(),
                )
                .expect("insert")
                .commit;
            model.push(Tuple {
                key: k,
                sys_from: c.0,
                seq: op,
                sys_to: SYSTEM_TIME_OPEN.0,
                valid_from: interval.from.0,
                valid_to: interval.to.0,
                value,
            });
            live[k as usize] = true;
            hi = hi.max(c.0);
        }
    }
    wal.tick().expect("fsync");

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

    Scenario {
        delta,
        index,
        recovered,
        recovered_index,
        model,
        hi,
    }
}

// --- 1. the both-axis differential (+ in-sweep mutation catch) --------------

/// Over a seed sweep, at every `(sys, valid)` grid point the live engine, the
/// crash-recovered engine, and the brute-force reference agree byte-for-byte —
/// both half-open axes probed exhaustively. And for every seed with a closed
/// period, a `sys_to + 1` mutation of the reference diverges from the engine
/// somewhere in the grid, so the differential would have caught it.
#[test]
fn bitemporal_as_of_is_differential_equal_across_both_axes_and_recovery() {
    for seed in 0u64..120 {
        let Scenario {
            delta,
            index,
            recovered,
            recovered_index,
            model,
            hi,
        } = run_seed(seed);

        let mutated = mutate_first_close(&model, 1);
        let mut caught = false;

        for s in (START - 2)..=(hi + 2) {
            let live = engine_live(&delta, &index, s);
            let rlive = engine_live(&recovered, &recovered_index, s);
            for k in 0..KEY_POOL {
                let eng = live.get(&k);
                let reng = rlive.get(&k);
                for v in (-2)..=(VMAX + 2) {
                    let expected = reference_as_of(&model, k, s, v);
                    let got = engine_point(eng, v);
                    assert_eq!(
                        got, expected,
                        "seed {seed} @ (s={s}, v={v}) key {k}: live engine vs reference",
                    );
                    let rgot = engine_point(reng, v);
                    assert_eq!(
                        rgot, expected,
                        "seed {seed} @ (s={s}, v={v}) key {k}: recovered engine vs reference",
                    );
                    if let Some(m) = &mutated
                        && got != buggy_as_of(m, k, s, v)
                    {
                        caught = true;
                    }
                }
            }
        }

        if mutated.is_some() {
            assert!(
                caught,
                "seed {seed}: a deliberate sys_to+1 mutation must diverge from the engine \
                 somewhere in the (sys, valid) grid",
            );
        }
    }
}

// --- 1b. same-tick differential (seq is load-bearing) -----------------------

/// The whole differential, but driven by a clock that issues each µs twice
/// ([`StallClock`]), so consecutive commits collide on `sys_from` and the only
/// thing separating them is `seq` (STL-145). The brute-force reference (which
/// tiles a same-tick supersession as a degenerate `[c, c)` prior plus an open
/// successor) and the engine — live *and* crash-recovered from the WAL — must
/// still agree byte-for-byte at every `(sys, valid)` grid point. Keyed on
/// `sys_from` alone, a same-tick version would be dropped from a chain or an
/// index entry and the grids would diverge; this is the test that proves they do
/// not.
#[test]
fn same_tick_commits_are_differential_equal_across_both_axes_and_recovery() {
    for seed in 0u64..80 {
        let Scenario {
            delta,
            index,
            recovered,
            recovered_index,
            model,
            hi,
        } = run_seed_same_tick(seed);

        for s in (START - 2)..=(hi + 2) {
            let live = engine_live(&delta, &index, s);
            let rlive = engine_live(&recovered, &recovered_index, s);
            for k in 0..KEY_POOL {
                let eng = live.get(&k);
                let reng = rlive.get(&k);
                for v in (-2)..=(VMAX + 2) {
                    let expected = reference_as_of(&model, k, s, v);
                    assert_eq!(
                        engine_point(eng, v),
                        expected,
                        "seed {seed} @ (s={s}, v={v}) key {k}: same-tick live engine vs reference",
                    );
                    assert_eq!(
                        engine_point(reng, v),
                        expected,
                        "seed {seed} @ (s={s}, v={v}) key {k}: same-tick recovered engine vs reference",
                    );
                }
            }
        }
    }
}

/// The minimal, unmistakable same-tick case ([ADR-0024]/docs/16 §3, STL-145):
/// an `INSERT` then an `UPDATE` of one key at an **identical** clock µs, with the
/// transaction manager handing each a distinct `seq`. Both versions keep the
/// shared `sys_from`; the engine orders them by `seq`, so the post-`UPDATE` value
/// is the one live at and after that tick. A `sys_from`-only chain would have
/// dropped one of the two — here neither is lost, and a crash-recovered engine
/// reproduces the identical result through the seq-bearing WAL close.
#[test]
fn two_commits_at_one_tick_are_ordered_by_seq() {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
    let mut dml = DmlWriter::new(wal.clone(), StallClock::new(START), true);
    let key = BusinessKey::new(vec![b'k', 0]);
    let valid = ValidInterval::new(ValidTimeMicros(0), ValidTimeMicros(100)).expect("interval");

    // seq 0 then seq 1 — the manager's per-commit counter. The stalled clock
    // hands both the same µs.
    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(valid),
            Some(b"A".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit
        .0;
    let c1 = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            Some(valid),
            Some(b"B".to_vec()),
            1,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit
        .0;
    assert_eq!(
        c0, c1,
        "the stalled clock forces both commits onto one sys_from"
    );
    wal.tick().expect("fsync");

    // At and after the shared tick, the higher-seq UPDATE wins; the INSERT is
    // superseded (its degenerate [c, c) period is never live), not dropped.
    let live = engine_live(&delta, &index, c1);
    assert_eq!(
        engine_point(live.get(&0), 50),
        Some(b"B".to_vec()),
        "the higher-seq version at the shared tick is the live one",
    );
    // Strictly before the tick the key did not exist.
    assert!(!engine_live(&delta, &index, c0 - 1).contains_key(&0));

    // Crash-recover from the WAL alone and re-probe: the seq-bearing close must
    // round-trip so the rebuilt engine resolves identically.
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
    let rlive = engine_live(&recovered, &recovered_index, c1);
    assert_eq!(
        engine_point(rlive.get(&0), 50),
        Some(b"B".to_vec()),
        "the recovered engine reproduces the seq-ordered result",
    );
}

// --- 2. focused, documented sys_to-mutation catch --------------------------

/// The canonical [ADR-0023] failure, made unmistakable: `INSERT "A"` then
/// `UPDATE "B"` (both valid `[0,100)`) in a real engine, with the reference model
/// pinned to the *engine's own* commit times `c0 < c1`. At the supersession
/// boundary `(c1, 50)` the truth is `"B"`. Shifting the prior period's close one
/// tick late lets `"A"` linger to `[c0, c1 + 1)`; a stale-close read then returns
/// `"A"`, diverging from the engine's `"B"` — exactly the equality the differential
/// asserts.
#[test]
fn a_deliberate_sys_to_mutation_is_caught_by_the_oracle() {
    // The engine answers the history; `c0`/`c1` are its actual commit times, so the
    // reference and the probes ride the same timeline as the engine.
    let (c0, c1, truth) = two_version_engine();
    let model = vec![
        Tuple {
            key: 0,
            sys_from: c0,
            seq: 0,
            sys_to: c1,
            valid_from: 0,
            valid_to: 100,
            value: b"A".to_vec(),
        },
        Tuple {
            key: 0,
            sys_from: c1,
            seq: 0,
            sys_to: SYSTEM_TIME_OPEN.0,
            valid_from: 0,
            valid_to: 100,
            value: b"B".to_vec(),
        },
    ];

    assert_eq!(
        truth,
        Some(b"B".to_vec()),
        "the engine returns the post-update value at the boundary",
    );
    assert_eq!(
        truth,
        reference_as_of(&model, 0, c1, 50),
        "the aligned reference agrees with the engine at the boundary",
    );

    // Now corrupt the close: sys_to materialized one tick past the successor.
    let mutated = mutate_first_close(&model, 1).expect("a closed period exists");
    assert_eq!(
        buggy_as_of(&mutated, 0, c1, 50),
        Some(b"A".to_vec()),
        "a stale close resurrects the prior value at the boundary",
    );
    assert_ne!(
        truth,
        buggy_as_of(&mutated, 0, c1, 50),
        "the oracle catches a sys_to materialized later than the successor's sys_from",
    );
}

/// Replay the focused `INSERT "A"` / `UPDATE "B"` history (valid `[0,100)`) into a
/// real engine and return `(insert_commit, update_commit, value AS OF (update_commit,
/// valid=50))` — the engine's ground truth at the supersession boundary, on its own
/// timeline so the focused test never hard-codes a clock position.
fn two_version_engine() -> (i64, i64, Option<Vec<u8>>) {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
    // No WAL replay here, so the writer can own the log outright.
    let mut dml = DmlWriter::new(wal, StepClock::new(START), true);
    let key = BusinessKey::new(vec![b'k', 0]);
    let valid = ValidInterval::new(ValidTimeMicros(0), ValidTimeMicros(100)).expect("interval");

    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &EmptySealed,
            key.clone(),
            Some(valid),
            Some(b"A".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit
        .0;
    let c1 = dml
        .update(
            &mut delta,
            &mut index,
            &EmptySealed,
            key,
            Some(valid),
            Some(b"B".to_vec()),
            0,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit
        .0;

    // Read at the supersession boundary: the update is live from `c1` onward.
    let live = engine_live(&delta, &index, c1);
    (c0, c1, engine_point(live.get(&0), 50))
}

// --- 3. metamorphic invariance ---------------------------------------------

/// Splitting a valid interval into two adjacent identical-value pieces, reordering
/// the asserted tuples, and coalescing the split back must not change *any* query
/// result ([docs/16 §9], [docs/06 §10]). Asserted over the same exhaustive grid
/// the differential uses.
#[test]
fn metamorphic_split_reorder_coalesce_preserve_all_query_results() {
    for seed in 0u64..60 {
        let Scenario { model, hi, .. } = run_seed(seed);
        let probes = probe_grid(hi);
        let base: Vec<Option<Vec<u8>>> = probes
            .iter()
            .map(|&(k, s, v)| reference_as_of(&model, k, s, v))
            .collect();

        // (a) reorder — a brute-force scan is order-independent.
        let mut reordered = model.clone();
        reordered.reverse();
        assert_probes(&reordered, &probes, &base, seed, "reorder");

        // (b) split — cut each bounded interval at its midpoint into two adjacent
        //     identical-value tuples over the same system period.
        let split = split_valid(&model);
        assert_probes(&split, &probes, &base, seed, "split");

        // (c) coalesce the split back — adjacent same-(key,sys,value) abutting
        //     intervals merge into one. Inverse of the split; results unchanged.
        let coalesced = coalesce_valid(&split);
        assert_probes(&coalesced, &probes, &base, seed, "coalesce");
    }
}

/// Every `(key, sys, valid)` grid point for a seed — the exhaustive probe set.
fn probe_grid(hi: i64) -> Vec<(u8, i64, i64)> {
    let mut probes = Vec::new();
    for k in 0..KEY_POOL {
        for s in (START - 2)..=(hi + 2) {
            for v in (-2)..=(VMAX + 2) {
                probes.push((k, s, v));
            }
        }
    }
    probes
}

/// Assert a transformed model answers every probe identically to `base`.
fn assert_probes(
    model: &[Tuple],
    probes: &[(u8, i64, i64)],
    base: &[Option<Vec<u8>>],
    seed: u64,
    label: &str,
) {
    for (&(k, s, v), expected) in probes.iter().zip(base) {
        assert_eq!(
            &reference_as_of(model, k, s, v),
            expected,
            "seed {seed} [{label}] @ (s={s}, v={v}) key {k}: transform changed a query result",
        );
    }
}

/// Split every bounded valid interval at its midpoint into two adjacent
/// identical-value tuples sharing the original system period. Open-ended and
/// length-1 intervals are left intact (nothing to split).
fn split_valid(model: &[Tuple]) -> Vec<Tuple> {
    let mut out = Vec::new();
    for t in model {
        if t.valid_to != VALID_TIME_OPEN.0 && t.valid_to - t.valid_from >= 2 {
            let mid = t.valid_from + (t.valid_to - t.valid_from) / 2;
            out.push(Tuple {
                valid_to: mid,
                ..t.clone()
            });
            out.push(Tuple {
                valid_from: mid,
                ..t.clone()
            });
        } else {
            out.push(t.clone());
        }
    }
    out
}

/// Coalesce consecutive tuples that share `(key, sys_from, sys_to, value)` and
/// whose valid intervals abut (`last.valid_to == next.valid_from`) — the inverse
/// of [`split_valid`].
// The final clause compares `valid_to` against the *next* tuple's `valid_from` on
// purpose — that is what "abut" means — so the cross-field comparison is intended,
// not the typo the lint suspects.
#[allow(clippy::suspicious_operation_groupings)]
fn coalesce_valid(model: &[Tuple]) -> Vec<Tuple> {
    let mut out: Vec<Tuple> = Vec::new();
    for t in model {
        if let Some(last) = out.last_mut()
            && last.key == t.key
            && last.sys_from == t.sys_from
            && last.sys_to == t.sys_to
            && last.value == t.value
            && last.valid_to == t.valid_from
        {
            last.valid_to = t.valid_to;
            continue;
        }
        out.push(t.clone());
    }
    out
}
