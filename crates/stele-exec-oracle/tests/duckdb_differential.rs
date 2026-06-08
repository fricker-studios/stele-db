//! DuckDB differential oracle for bitemporal `AS OF (sys, valid)` at the
//! SQL/executor layer (STL-144, [docs/06 §10], [docs/16]).
//!
//! This is the **second, independent** correctness oracle the testing strategy
//! demands. STL-138 landed the *in-process* half — a hand-coded Rust reference
//! aligned to the validity-index model, diffed against the storage tiers
//! directly ([`stele-storage/tests/bitemporal_oracle.rs`]). This file lands the
//! deferred **DuckDB** half: it reimplements the bitemporal version table *and*
//! the `AS OF` query **naïvely in SQL** inside an in-memory DuckDB, drives the
//! identical random history through Stele's [`SnapshotScan`] executor, and
//! asserts the two agree **byte-for-byte** over ≥10⁶ randomized `(op, probe)`
//! pairs. Zero divergence is the bar.
//!
//! Why this rides the executor (not the storage core):
//!
//! * It exercises the real read path a query takes — [`SnapshotScan`] merging the
//!   delta tier with sealed segments at an MVCC snapshot (STL-100) — which only
//!   exists above the runtime-agnostic core. The workload flushes the delta into
//!   sealed segments mid-history, so most probes resolve a version that has
//!   crossed the flush boundary (the cross-tier merge), not a delta-only read.
//! * DuckDB is an external C++ dependency. It is confined to this dedicated
//!   **`stele-exec-oracle`** crate (a dev-dependency here, never in `stele-exec`
//!   or any shipped crate), honoring [ADR-0010]: the deterministic storage/txn
//!   core never links it. The `bundled` feature vendors the DuckDB amalgamation,
//!   so no system library is required in CI. The crate is held off the per-PR
//!   `--workspace` runs and built only in the nightly gate (STL-158), so the
//!   multi-minute amalgamation compile never gates a PR.
//!
//! ## The two implementations being diffed
//!
//! * **Stele.** A valid-time table written through the real DML path
//!   ([`DmlWriter`]); reads run [`SnapshotScan`] at each system snapshot `s`,
//!   recover the per-key `(valid interval, payload)` from the framed payload
//!   ([`unframe_payload`]), and resolve the valid axis with a half-open
//!   membership test — exactly what a `FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS
//!   OF v` query lowers to.
//! * **DuckDB.** An append-only `versions(k, sys_from, seq, sys_to, vfrom, vto,
//!   val)` table maintained by SQL DML — an `INSERT` appends an open row, an
//!   `UPDATE`/`DELETE` closes the open row at the engine's own commit tick (and
//!   `UPDATE` appends a fresh open row). The `AS OF (s, v)` answer is a plain
//!   half-open interval-containment join. Independent code (a SQL engine, not the
//!   Rust resolver) computing the same truth ([docs/16 §3]).
//!
//! Both sides ride the **engine's actual commit timestamps**, so the timelines
//! line up tick-for-tick and the diff is over the *resolution*, not over two
//! divergent clocks.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use duckdb::{Connection, params};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros, VALID_TIME_OPEN, ValidTimeMicros};
use stele_exec::{Column, SnapshotScan};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::DmlWriter;
use stele_storage::segment::{ColumnId, SegmentReader, SegmentWriter};
use stele_storage::systime::SealedSegments;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::validtime::{ValidInterval, unframe_payload};
use stele_storage::wal::{Wal, WalConfig};

// --- knobs -----------------------------------------------------------------

/// Distinct business keys exercised per seed. Small so keys collide and
/// supersede often (the interesting case), while the per-seed `(sys, valid)`
/// grid stays cheap to probe exhaustively.
const KEY_POOL: u8 = 4;
/// First system-time commit; the sweep probes from just before it.
const START: i64 = 1_000;
/// Upper bound of the (bounded) valid-time domain. Intervals live in `[0, VMAX]`
/// so every integer valid point — and thus every half-open boundary — is probed.
const VMAX: i64 = 24;
/// Seeds in the sweep. Chosen so the total `(op, probe)` count clears the 10⁶
/// bar with margin; the test asserts the actual count rather than trusting this.
const SEEDS: u64 = 300;
/// The DoD bar: at least one million randomized ops + probes, zero divergence.
const OP_FLOOR: u64 = 1_000_000;

/// DuckDB sentinel for an open system-time period (`sys_to`). Mirrors
/// [`stele_common::time::SYSTEM_TIME_OPEN`] (`i64::MAX`); larger than any probed
/// snapshot, so `s < sys_to` is always true for a live row. The valid-axis open
/// sentinel ([`VALID_TIME_OPEN`]) rides straight through on `valid.to.0`, so it
/// needs no separate constant here.
const SYS_OPEN: i64 = i64::MAX;

// --- harness ---------------------------------------------------------------

/// Deterministic, strictly-increasing clock — one tick per `now()` ([ADR-0010]),
/// matching the storage oracles so a failing seed reproduces bit-for-bit. A
/// distinct `sys_from` per commit keeps the bitemporal tiling clean on both
/// sides; same-tick `seq` ordering is STL-145's concern, exercised by the
/// storage oracle.
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

/// Tiny xorshift64* — deterministic, dependency-free; matches the storage oracles
/// so a failing seed reproduces bit-for-bit ([ADR-0010]).
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

/// The business key for pool slot `k` — `['k', k]`, so the single byte at index 1
/// recovers the slot. Matches the storage oracle's encoding.
fn key_of(k: u8) -> BusinessKey {
    BusinessKey::new(vec![b'k', k])
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

// --- the DuckDB reference ---------------------------------------------------

/// The naïve bitemporal reference, implemented entirely in DuckDB SQL. Holds an
/// append-only `versions` table and maintains it with ordinary DML; the `AS OF`
/// answer is a half-open interval join. The independence from Stele's resolver is
/// the whole point — a second engine evaluating the same temporal truth.
struct DuckModel {
    conn: Connection,
}

impl DuckModel {
    fn new() -> Self {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute_batch(
            "CREATE TABLE versions (
                 k        BIGINT,
                 sys_from BIGINT,
                 seq      BIGINT,
                 sys_to   BIGINT,
                 vfrom    BIGINT,
                 vto      BIGINT,
                 val      BLOB
             );",
        )
        .expect("create versions table");
        Self { conn }
    }

    /// Wipe the table between seeds (one connection, reused — cheaper than a fresh
    /// in-memory database per seed).
    fn reset(&self) {
        self.conn
            .execute_batch("DELETE FROM versions;")
            .expect("truncate versions");
    }

    /// `INSERT`: append a new open period for `k` at the engine's commit tick.
    fn insert(&self, k: u8, commit: i64, seq: i64, valid: ValidInterval, val: &[u8]) {
        self.conn
            .execute(
                "INSERT INTO versions VALUES (?, ?, ?, ?, ?, ?, ?);",
                params![
                    i64::from(k),
                    commit,
                    seq,
                    SYS_OPEN,
                    valid.from.0,
                    valid.to.0,
                    val
                ],
            )
            .expect("duckdb insert");
    }

    /// Close `k`'s currently-open period at `commit` — the SQL analogue of
    /// materializing a close in the validity index. Exactly one open period
    /// exists for a live key, so the predicate touches a single row.
    fn close(&self, k: u8, commit: i64) {
        let n = self
            .conn
            .execute(
                "UPDATE versions SET sys_to = ? WHERE k = ? AND sys_to = ?;",
                params![commit, i64::from(k), SYS_OPEN],
            )
            .expect("duckdb close");
        assert_eq!(n, 1, "exactly one open period closes per supersede/delete");
    }

    /// `UPDATE`: close the prior period and append a new open one, both at
    /// `commit` (the supersession boundary `sys_to(prior) == sys_from(new)`).
    fn update(&self, k: u8, commit: i64, seq: i64, valid: ValidInterval, val: &[u8]) {
        self.close(k, commit);
        self.insert(k, commit, seq, valid, val);
    }

    /// Every `(s, v, k, val)` cell where some key is system-live *and* valid, over
    /// the inclusive grid `[s_lo, s_hi] × [v_lo, v_hi]`. One SQL statement does the
    /// whole grid: a generated `(s, v)` cross-product joined to `versions` by
    /// half-open containment on *both* axes. The 2D-tiling invariant ([docs/16 §5])
    /// makes this at most one row per `(s, v, k)`.
    fn grid(&self, s_lo: i64, s_hi: i64, v_lo: i64, v_hi: i64) -> Vec<(i64, i64, u8, Vec<u8>)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT a.s, b.v, ve.k, ve.val
                   FROM generate_series(?, ?) AS a(s)
                   CROSS JOIN generate_series(?, ?) AS b(v)
                   JOIN versions ve
                     ON ve.sys_from <= a.s AND a.s < ve.sys_to
                    AND ve.vfrom    <= b.v AND b.v < ve.vto
                  ORDER BY a.s, b.v, ve.k;",
            )
            .expect("prepare grid query");
        let rows = stmt
            .query_map(params![s_lo, s_hi, v_lo, v_hi], |row| {
                let s: i64 = row.get(0)?;
                let v: i64 = row.get(1)?;
                let k: i64 = row.get(2)?;
                let val: Vec<u8> = row.get(3)?;
                Ok((s, v, k as u8, val))
            })
            .expect("run grid query");
        rows.map(|r| r.expect("grid row")).collect()
    }
}

// --- the Stele engine side --------------------------------------------------

/// A table's storage tiers for one seed: the delta, the validity index, and the
/// sealed segments flushed out of the delta during the history. The segment
/// readers are self-sufficient ([`MemFile`] is `Arc`-backed), so the `MemDisk`
/// they were sealed on need not be retained here.
struct Tiers {
    delta: Delta<MemDisk>,
    index: ValidityIndex<MemDisk>,
    segments: Vec<SegmentReader<MemFile>>,
}

/// The executor's `(s, v, k, val)` cells over the same inclusive grid — the
/// counterpart to [`DuckModel::grid`]. For each snapshot `s` one [`SnapshotScan`]
/// resolves every live key across delta **and** sealed segments; the framed
/// payload yields the valid interval, and a half-open membership test settles the
/// valid axis for each `v`.
fn stele_grid(
    tiers: &Tiers,
    s_lo: i64,
    s_hi: i64,
    v_lo: i64,
    v_hi: i64,
) -> Vec<(i64, i64, u8, Vec<u8>)> {
    let mut out = Vec::new();
    for s in s_lo..=s_hi {
        let live = stele_live(tiers, s);
        for (k, (interval, user)) in &live {
            for v in v_lo..=v_hi {
                if interval.contains(ValidTimeMicros(v)) {
                    out.push((s, v, *k, user.clone()));
                }
            }
        }
    }
    out.sort();
    out
}

/// Resolve, via the executor, the per-key system-live `(valid interval, user
/// payload)` at snapshot `s`. A duplicate key is a hard failure: at most one
/// version per key may be system-live ([docs/16 §5]).
fn stele_live(tiers: &Tiers, s: i64) -> BTreeMap<u8, (ValidInterval, Vec<u8>)> {
    let out = SnapshotScan::new(
        &tiers.delta,
        &tiers.index,
        &tiers.segments,
        Snapshot(SystemTimeMicros(s)),
    )
    .project(vec![ColumnId::BusinessKey, ColumnId::Payload])
    .execute()
    .expect("snapshot scan");

    let keys = bytes_column(&out, ColumnId::BusinessKey);
    let payloads = bytes_column(&out, ColumnId::Payload);
    assert_eq!(keys.len(), payloads.len());

    let mut live = BTreeMap::new();
    for (key_bytes, payload) in keys.iter().zip(&payloads) {
        let k = key_bytes[1];
        let (interval, user) = unframe_payload(true, payload).expect("unframe");
        let interval = interval.expect("valid-time table carries an interval");
        assert!(
            live.insert(k, (interval, user.to_vec())).is_none(),
            "@ s={s}: executor returned two live versions for key {k} — \
             the at-most-one-active-version invariant is broken",
        );
    }
    live
}

/// Pull a projected bytes column out of a scan result by id.
fn bytes_column(out: &stele_exec::ScanOutput, col: ColumnId) -> Vec<Vec<u8>> {
    let (_, column) = out
        .batch
        .columns
        .iter()
        .find(|(c, _)| *c == col)
        .expect("projected column present");
    match column {
        Column::Bytes(rows) => rows.iter().map(|c| c.clone().unwrap()).collect(),
        Column::I64(_) => panic!("column {col:?} is i64, expected bytes"),
    }
}

// --- the seeded workload ----------------------------------------------------

/// One seed's outcome: the built tiers, the DuckDB model holding the identical
/// history, the last commit tick observed, and the number of DML ops applied.
struct Seed {
    tiers: Tiers,
    hi: i64,
    dml_ops: u64,
}

/// Apply one seed's random `INSERT`/`UPDATE`/`DELETE` history (valid-time table)
/// to **both** the Stele engine and the DuckDB model, flushing the delta into
/// sealed segments at random so the executor reads resolve across tiers. Both
/// sides ride the engine's actual commit ticks. `duck` is wiped before use.
fn run_seed(seed: u64, duck: &DuckModel) -> Seed {
    duck.reset();
    let mut rng = Rng::new(seed);

    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
    let seg_disk = MemDisk::new();
    let mut segments: Vec<SegmentReader<MemFile>> = Vec::new();
    // `true`: valid-time opt-in — every write carries an interval (the second axis).
    let mut dml = DmlWriter::new(wal, StepClock::new(START), true);

    let mut alive = vec![false; KEY_POOL as usize];
    let mut hi = START;
    let mut dml_ops = 0u64;
    let mut flushes = 0usize;

    let ops = 16 + rng.range(32);
    for op in 0..ops {
        let k = rng.range(u64::from(KEY_POOL)) as u8;
        let key = key_of(k);
        let txn = TxnId(op);
        let value = format!("k{k}-op{op}").into_bytes();

        if alive[k as usize] {
            if rng.range(2) == 0 {
                // DELETE: close the open period on both sides.
                let sealed = SealedSegments::new(&segments);
                let c = dml
                    .delete(&mut delta, &mut index, &sealed, &key, txn, who())
                    .expect("delete")
                    .commit;
                duck.close(k, c.0);
                alive[k as usize] = false;
                hi = hi.max(c.0);
            } else {
                // UPDATE: supersede the open period with a new one.
                let interval = gen_valid(&mut rng, VMAX);
                let sealed = SealedSegments::new(&segments);
                let c = dml
                    .update(
                        &mut delta,
                        &mut index,
                        &sealed,
                        key,
                        Some(interval),
                        Some(value.clone()),
                        op,
                        txn,
                        who(),
                    )
                    .expect("update")
                    .commit;
                duck.update(k, c.0, op as i64, interval, &value);
                hi = hi.max(c.0);
            }
        } else {
            // INSERT: open the first period for a dormant key.
            let interval = gen_valid(&mut rng, VMAX);
            let sealed = SealedSegments::new(&segments);
            let c = dml
                .insert(
                    &mut delta,
                    &mut index,
                    &sealed,
                    key,
                    Some(interval),
                    Some(value.clone()),
                    op,
                    txn,
                    who(),
                )
                .expect("insert")
                .commit;
            duck.insert(k, c.0, op as i64, interval, &value);
            alive[k as usize] = true;
            hi = hi.max(c.0);
        }
        dml_ops += 1;

        // Occasionally flush the delta into a sealed segment, so subsequent reads
        // resolve live versions that have crossed the columnar flush boundary
        // (the cross-tier merge STL-100 added) and subsequent closes ride the
        // sealed-lookup marker path (STL-140).
        if rng.range(5) == 0 {
            if let Some(reader) = flush(&seg_disk, flushes, &mut delta) {
                segments.push(reader);
                flushes += 1;
            }
        }
    }

    Seed {
        tiers: Tiers {
            delta,
            index,
            segments,
        },
        hi,
        dml_ops,
    }
}

/// Drain the delta into a fresh sealed segment and reopen it for read — the real
/// columnar flush boundary. Returns `None` when the delta is empty (nothing to
/// seal), so we never write a zero-row segment.
fn flush(disk: &MemDisk, n: usize, delta: &mut Delta<MemDisk>) -> Option<SegmentReader<MemFile>> {
    let rows = delta.flush_to_segment().expect("flush");
    if rows.is_empty() {
        return None;
    }
    let name = format!("seg-{n}.seg");
    let mut w = SegmentWriter::create(disk, &name).expect("create segment");
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    Some(SegmentReader::open(disk, &name).expect("open segment"))
}

// --- 1. the differential ----------------------------------------------------

/// Over the seed sweep, at every `(sys, valid)` grid point the Stele executor and
/// the naïve DuckDB reference return byte-identical results — both half-open axes
/// probed exhaustively, across the delta/sealed-segment merge. The DoD floor of
/// ≥10⁶ randomized `(op, probe)` pairs is asserted, not assumed.
#[test]
fn duckdb_differential_matches_executor_over_a_million_ops() {
    let duck = DuckModel::new();
    let mut total_ops: u64 = 0;

    for seed in 0..SEEDS {
        let Seed { tiers, hi, dml_ops } = run_seed(seed, &duck);

        let (s_lo, s_hi) = (START - 2, hi + 2);
        let (v_lo, v_hi) = (-2, VMAX + 2);

        let expected = duck.grid(s_lo, s_hi, v_lo, v_hi);
        let got = stele_grid(&tiers, s_lo, s_hi, v_lo, v_hi);

        assert_eq!(
            got, expected,
            "seed {seed}: executor diverged from the DuckDB reference over the \
             (sys, valid) grid",
        );

        // Count the work: every DML op plus every probed grid cell (present or
        // absent — an absent cell is a probe whose answer is "no row").
        let s_cells = (s_hi - s_lo + 1) as u64;
        let v_cells = (v_hi - v_lo + 1) as u64;
        total_ops += dml_ops + s_cells * v_cells * u64::from(KEY_POOL);
    }

    assert!(
        total_ops >= OP_FLOOR,
        "differential exercised {total_ops} ops/probes; DoD floor is {OP_FLOOR} — raise SEEDS",
    );
}

// --- 2. the harness can actually fail ---------------------------------------

/// Guards against a vacuous oracle: if the DuckDB reference is fed a deliberately
/// stale close (`sys_to` materialized one tick *past* the successor's `sys_from`,
/// the canonical [ADR-0023] violation), the prior value lingers into its
/// successor's reign and the grid diff against the (correct) executor **must**
/// fire. A differential that cannot detect this bug would be worthless.
#[test]
fn duckdb_oracle_detects_a_deliberate_stale_close() {
    let duck = DuckModel::new();
    // A minimal two-version history on one key, valid `[0, 100)`: INSERT "A",
    // then UPDATE "B". Drive Stele honestly; corrupt only the DuckDB mirror.
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("delta");
    let mut index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("idx");
    let segments: Vec<SegmentReader<MemFile>> = Vec::new();
    let mut dml = DmlWriter::new(wal, StepClock::new(START), true);
    let valid = ValidInterval::new(ValidTimeMicros(0), ValidTimeMicros(100)).expect("interval");

    let sealed = SealedSegments::new(&segments);
    let c0 = dml
        .insert(
            &mut delta,
            &mut index,
            &sealed,
            key_of(0),
            Some(valid),
            Some(b"A".to_vec()),
            0,
            TxnId(1),
            who(),
        )
        .expect("insert")
        .commit;
    let sealed = SealedSegments::new(&segments);
    let c1 = dml
        .update(
            &mut delta,
            &mut index,
            &sealed,
            key_of(0),
            Some(valid),
            Some(b"B".to_vec()),
            1,
            TxnId(2),
            who(),
        )
        .expect("update")
        .commit;

    // The DuckDB mirror, but with the prior period's close shoved one tick late —
    // "A" now overlaps "B" on the system axis at `[c1, c1+1)`.
    duck.insert(0, c0.0, 0, valid, b"A");
    duck.conn
        .execute(
            "UPDATE versions SET sys_to = ? WHERE k = 0 AND sys_to = ?;",
            params![c1.0 + 1, SYS_OPEN],
        )
        .expect("stale close");
    duck.insert(0, c1.0, 1, valid, b"B");

    let tiers = Tiers {
        delta,
        index,
        segments,
    };
    let (s_lo, s_hi) = (START - 2, c1.0 + 2);
    let (v_lo, v_hi) = (-2, VMAX + 2);

    let expected = duck.grid(s_lo, s_hi, v_lo, v_hi);
    let got = stele_grid(&tiers, s_lo, s_hi, v_lo, v_hi);
    assert_ne!(
        got, expected,
        "a stale close in the reference must diverge from the executor — \
         otherwise the differential proves nothing",
    );
}
