//! DuckDB differential oracle for bitemporal `AS OF (sys, valid)` driven through
//! the **whole SQL bind→exec pipeline** (STL-167, the v0.2 correctness exit gate).
//!
//! The sibling `tests/duckdb_differential.rs` (STL-144) diffs DuckDB against
//! Stele's [`SnapshotScan`] executor *directly* — it builds the storage tiers by
//! hand, calls the scan, and resolves the valid axis with a hand-coded membership
//! test. That proves the read path a query *lowers to*. This file closes the gap
//! the v0.2 exit criterion names: it drives the **public SQL surface** end to end —
//!
//! * **writes** run as SQL `INSERT`/`UPDATE`/`DELETE` naming the valid period
//!   columns, so the binder lifts each interval onto the framed payload
//!   ([STL-194]) and the engine applies the system-axis close/open ([STL-166]);
//! * **reads** run as SQL `SELECT … FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS OF v`,
//!   so the parser lifts both axes ([STL-162]), the binder resolves them, and the
//!   engine threads them into the both-axes scan ([STL-163]/[STL-164]);
//! * **period predicates** (`WHERE PERIOD(a, b) <pred> PERIOD(c, d)`, [STL-165])
//!   gate the read, diffed against DuckDB's own boolean evaluation of the same
//!   half-open relation.
//!
//! Both sides ride the **engine's actual commit ticks**: after each SQL write the
//! oracle reads [`SessionEngine::commit_clock`] — the instant that write was
//! stamped with — and mirrors the op into DuckDB at exactly that tick, so the two
//! timelines line up commit-for-commit and the diff is over the *resolution*, not
//! over two divergent clocks. Over the seed sweep the engine and the naïve DuckDB
//! reference agree byte-for-byte at ≥10⁶ randomized `(op, probe)` pairs; a failure
//! prints the seed and the first diverging `(s, v)` point as a minimal repro.
//!
//! DuckDB is confined to this nightly-only crate (a dev-dependency, never linked
//! into a shipped crate; held off the per-PR `--workspace` runs, [STL-158]), so
//! the bundled C++ amalgamation never gates a PR and the runtime-agnostic core
//! never links it ([ADR-0010]).
//!
//! [`SnapshotScan`]: stele_exec::SnapshotScan
//! [`SessionEngine::commit_clock`]: stele_engine::SessionEngine::commit_clock

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::{BTreeMap, BTreeSet};

use duckdb::{Connection, params};

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};
use stele_engine::{SessionEngine, StatementOutcome};
use stele_sql::Statement;
use stele_storage::backend::MemDisk;

// --- knobs -----------------------------------------------------------------

/// Distinct business keys exercised per seed. Small so keys collide and supersede
/// often (the interesting case), while the per-seed `(sys, valid)` grid stays
/// cheap to sweep exhaustively.
const KEY_POOL: i64 = 4;
/// Upper bound of the (bounded) valid-time domain. Intervals live in `[0, VMAX]`
/// so every integer valid point — and thus every half-open boundary — is probed.
const VMAX: i64 = 24;
/// Seeds in the sweep. Chosen so the realized `(op, probe)` count clears the 10⁶
/// bar with margin (≈1.2M at 340 seeds); the test asserts the actual count rather
/// than trusting this. Deterministic, so the count is fixed, never flaky.
const SEEDS: u64 = 340;
/// The DoD bar: at least one million randomized ops + probes, zero divergence.
const OP_FLOOR: u64 = 1_000_000;

/// Open-period sentinels, mirroring the engine's `SYSTEM_TIME_OPEN` /
/// `VALID_TIME_OPEN` (both `i64::MAX`): a row with `sys_to == OPEN` is
/// system-live, one with `vto == OPEN` is valid `[from, +∞)`. Every probed
/// snapshot and valid instant is far below `i64::MAX`, so `s < sys_to` and
/// `v < vto` hold for an open row. (The in-crate STL-194 oracle uses the same
/// `i64::MAX` sentinel.)
const OPEN: i64 = i64::MAX;

// --- harness ---------------------------------------------------------------

/// A trivial inner clock pinned at the origin. The engine wraps it in a
/// `MonotonicClock`, which hands out a **strictly increasing** instant on every
/// reading regardless of the inner clock — so each committed write gets a distinct
/// `sys_from` and [`SessionEngine::commit_clock`] reports the last one ([ADR-0010]
/// determinism: a failing seed reproduces bit-for-bit).
#[derive(Clone, Copy)]
struct OriginClock;
impl Clock for OriginClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

/// Tiny xorshift64* — deterministic, dependency-free; matches the sibling oracle
/// so a failing seed reproduces bit-for-bit.
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
    fn range(&mut self, n: i64) -> i64 {
        (self.next_u64() % n as u64) as i64
    }
}

/// Parse exactly one SQL statement, asserting the input was a single statement —
/// the oracle only ever feeds one, and silently dropping a stray second statement
/// could mask a malformed query.
fn parse_one(sql: &str) -> Statement {
    let mut stmts = stele_sql::parse(sql).expect("parse");
    assert_eq!(
        stmts.len(),
        1,
        "the oracle feeds exactly one statement, got {}",
        stmts.len(),
    );
    stmts.pop().expect("exactly one statement")
}

/// The `(id, balance)` cells of a both-axes table, in the engine's canonical
/// [`ScalarValue::encode`] byte form — the unit the differential compares. One
/// point's value: a sorted list of `(id bytes, balance bytes)`, duplicates
/// preserved (so a corrupted reference returning two versions for one key is a
/// *divergence*, not a silently-collapsed map entry).
type Point = Vec<(Vec<u8>, Vec<u8>)>;
/// The whole `(sys, valid)` grid: each probed point that resolved at least one
/// row, mapped to its [`Point`].
type Grid = BTreeMap<(i64, i64), Point>;

/// Encode an integer as the four little-endian bytes of an `INT` cell — exactly
/// what a `SELECT id` / `SELECT balance` returns for this workload, so the DuckDB
/// reference and the engine are compared byte-for-byte.
fn int4_bytes(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    ScalarValue::Int4(i32::try_from(v).expect("value fits i32")).encode(&mut out);
    out
}

/// Decode an `INT` cell back to its value — used only to render a readable repro
/// on divergence.
fn decode_int4(bytes: &[u8]) -> i32 {
    // `decode(Int4, …)` only ever yields an `Int4`, so the other arm is
    // unreachable; it deliberately does not `{:?}`-format the `ScalarValue`
    // (CodeQL's cleartext-logging query taints the enum's `Uuid` variant —
    // a false positive in this Int4-only test, cf. STL-170 / STL-207).
    match ScalarValue::decode(LogicalType::Int4, bytes).expect("decode int4") {
        ScalarValue::Int4(v) => v,
        _ => panic!("decode(Int4, …) must yield an Int4 cell"),
    }
}

/// Render a point as `(id, balance)` integer pairs for a failure message.
fn decode_point(point: &Point) -> Vec<(i32, i32)> {
    point
        .iter()
        .map(|(id, bal)| (decode_int4(id), decode_int4(bal)))
        .collect()
}

// --- the DuckDB reference ---------------------------------------------------

/// The naïve bitemporal reference, implemented entirely in DuckDB SQL: an
/// append-only `versions` table maintained by ordinary DML, with the `AS OF`
/// answer a half-open interval-containment join. A second engine evaluating the
/// same temporal truth is the whole point ([docs/16 §3]).
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
                 sys_to   BIGINT,
                 vfrom    BIGINT,
                 vto      BIGINT,
                 balance  BIGINT
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
            .expect("clear the versions table between seeds");
    }

    /// `INSERT`: append a new open system period for `k` at the engine's commit
    /// tick.
    fn insert(&self, k: i64, commit: i64, vfrom: i64, vto: i64, balance: i64) {
        self.conn
            .execute(
                "INSERT INTO versions VALUES (?, ?, ?, ?, ?, ?);",
                params![k, commit, OPEN, vfrom, vto, balance],
            )
            .expect("duckdb insert");
    }

    /// Close `k`'s currently-open system period at `commit` — the SQL analogue of
    /// materializing a close. Exactly one open period exists for a live key.
    fn close(&self, k: i64, commit: i64) {
        let n = self
            .conn
            .execute(
                "UPDATE versions SET sys_to = ? WHERE k = ? AND sys_to = ?;",
                params![commit, k, OPEN],
            )
            .expect("duckdb close");
        assert_eq!(n, 1, "exactly one open period closes per supersede/delete");
    }

    /// `UPDATE`: close the prior period and append a new open one, both at
    /// `commit` (the supersession boundary `sys_to(prior) == sys_from(new)`).
    fn update(&self, k: i64, commit: i64, vfrom: i64, vto: i64, balance: i64) {
        self.close(k, commit);
        self.insert(k, commit, vfrom, vto, balance);
    }

    /// Every `(s, v)` grid point that resolves a row, mapped to its sorted
    /// `(id, balance)` byte pairs — the reference counterpart to [`stele_grid`].
    /// One SQL statement does the whole grid: a generated `(s, v)` cross-product
    /// joined to `versions` by half-open containment on *both* axes. Duplicates
    /// are preserved (no `GROUP BY`) so a deliberately-broken reference shows its
    /// overlap as an extra row, not a collapsed one.
    fn grid(&self, s_lo: i64, s_hi: i64, v_lo: i64, v_hi: i64) -> Grid {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT a.s, b.v, ve.k, ve.balance
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
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .expect("run grid query");

        let mut grid: Grid = BTreeMap::new();
        for r in rows {
            let (s, v, k, balance) = r.expect("grid row");
            grid.entry((s, v))
                .or_default()
                .push((int4_bytes(k), int4_bytes(balance)));
        }
        for point in grid.values_mut() {
            point.sort();
        }
        grid
    }

    /// Evaluate a boolean SQL expression in DuckDB — the independent half-open
    /// period-predicate truth the engine's constant fold is diffed against.
    fn truth(&self, expr: &str) -> bool {
        self.conn
            .query_row(&format!("SELECT {expr};"), [], |row| row.get::<_, bool>(0))
            .expect("boolean eval")
    }
}

// --- the Stele engine side --------------------------------------------------

/// Run one `SELECT` and collect its `(id, balance)` cells as a sorted [`Point`].
/// Asserts the engine's at-most-one-live-version-per-key invariant ([docs/16 §5]):
/// a duplicate key from the engine is a hard failure, not a silent overwrite.
fn select_point(engine: &mut SessionEngine<OriginClock, MemDisk>, sql: &str) -> Point {
    let StatementOutcome::Rows(result) = engine.execute(&parse_one(sql)).expect("select") else {
        panic!("a SELECT must return rows");
    };
    let mut seen = BTreeSet::new();
    let mut point = Vec::new();
    for row in result.rows {
        let id = row[0].clone().expect("the id key is never NULL");
        let balance = row[1]
            .clone()
            .expect("the balance is never NULL in this workload");
        assert!(
            seen.insert(id.clone()),
            "the engine returned two live versions for one key — the at-most-one-live invariant broke",
        );
        point.push((id, balance));
    }
    point.sort();
    point
}

/// The engine's whole `(sys, valid)` grid, read entirely over SQL with both axes
/// pinned by literal-microsecond `AS OF` instants — one `SELECT` per probed point.
/// Only points that resolved a row are stored, so the map equals the DuckDB
/// reference's iff both axes agree everywhere.
fn stele_grid(
    engine: &mut SessionEngine<OriginClock, MemDisk>,
    s_lo: i64,
    s_hi: i64,
    v_lo: i64,
    v_hi: i64,
) -> Grid {
    let mut grid = Grid::new();
    for s in s_lo..=s_hi {
        for v in v_lo..=v_hi {
            let sql = format!(
                "SELECT id, balance FROM acct \
                 FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}"
            );
            let point = select_point(engine, &sql);
            if !point.is_empty() {
                grid.insert((s, v), point);
            }
        }
    }
    grid
}

// --- the seeded workload ----------------------------------------------------

/// One seed's outcome: the engine holding the applied history, the table's
/// creation instant (the system-axis floor), the last commit tick, and the DML
/// op count.
struct Seed {
    engine: SessionEngine<OriginClock, MemDisk>,
    create_c: i64,
    hi: i64,
    dml_ops: u64,
}

/// Apply one seed's random `INSERT`/`UPDATE`/`DELETE` history to a valid-time
/// table **entirely over SQL**, mirroring each committed op into the DuckDB model
/// at the engine's own commit tick. `duck` is wiped before use.
///
/// Each seed picks one of three seal schedules (a `rng.range(3)` draw), so the
/// sweep covers every tier regime the SnapshotScan read path can face:
///   * **delta-only** — never seal; every read resolves from the delta tier.
///   * **fully sealed** — seal the whole delta once *after* the last write; every
///     read resolves from sealed segments.
///   * **mixed** — seal *between* writes (a per-op coin), so a prior version lands
///     in a sealed segment and a later valid-time `UPDATE` reads it back from
///     there. That read-modify-write across the tier boundary is the path STL-226
///     fixed: a sealed prior version stores its payload bare (the interval rides
///     its own `valid_from`/`valid_to` columns), so the RMW must not strip a
///     framed prefix that is only present on a delta row. Before the fix this seed
///     tripped `RowCodecError::TrailingBytes`, which is why the seal used to be
///     restricted to end-of-history.
fn run_seed(seed: u64, duck: &DuckModel) -> Seed {
    duck.reset();
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), OriginClock);
    engine
        .execute(&parse_one(
            "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
        ))
        .expect("create valid-time table");
    let create_c = engine.commit_clock().0;

    let mut alive = vec![false; KEY_POOL as usize];
    let mut hi = create_c;
    let mut dml_ops = 0u64;

    // This seed's seal schedule (see the function note): 0 = delta-only, 1 = seal
    // once after the last write, 2 = seal between writes mid-history.
    let seal_mode = rng.range(3);

    let ops = 16 + rng.range(32);
    for op in 0..ops {
        let k = rng.range(KEY_POOL);
        let balance = op + 1;
        // A well-formed valid window inside `[0, VMAX]`, sometimes open-ended to
        // exercise the `+∞` sentinel and the open-period default.
        let from = rng.range(VMAX);
        let open = rng.range(4) == 0;
        let to = if open {
            OPEN
        } else {
            from + 1 + rng.range(VMAX - from)
        };

        if alive[k as usize] && rng.range(2) == 0 {
            // DELETE: close the open period with no successor.
            engine
                .execute(&parse_one(&format!("DELETE FROM acct WHERE id = {k}")))
                .expect("delete over SQL");
            let c = engine.commit_clock().0;
            duck.close(k, c);
            alive[k as usize] = false;
            hi = hi.max(c);
        } else if alive[k as usize] {
            // UPDATE: supersede the open period with a new one (a valid-time RMW).
            // Omitting `vt` defaults the new period open, mirroring the INSERT
            // open default.
            let set = if open {
                format!("SET balance = {balance}, vf = {from}")
            } else {
                format!("SET balance = {balance}, vf = {from}, vt = {to}")
            };
            engine
                .execute(&parse_one(&format!("UPDATE acct {set} WHERE id = {k}")))
                .expect("update over SQL");
            let c = engine.commit_clock().0;
            duck.update(k, c, from, to, balance);
            hi = hi.max(c);
        } else {
            // INSERT: open the first period for a dormant key. Naming only `vf`
            // opens `[from, +∞)`.
            let stmt = if open {
                format!("INSERT INTO acct (id, balance, vf) VALUES ({k}, {balance}, {from})")
            } else {
                format!("INSERT INTO acct VALUES ({k}, {balance}, {from}, {to})")
            };
            engine.execute(&parse_one(&stmt)).expect("insert over SQL");
            let c = engine.commit_clock().0;
            duck.insert(k, c, from, to, balance);
            alive[k as usize] = true;
            hi = hi.max(c);
        }
        dml_ops += 1;

        // Mixed schedule: seal between writes with a per-op coin, so a prior
        // version lands in a sealed segment and the next UPDATE/DELETE of that key
        // reads it back across the tier boundary (STL-226). A seal on an empty
        // delta is an idempotent no-op, so a coin right after a seal is harmless.
        if seal_mode == 2 && rng.range(3) == 0 {
            engine.flush().expect("seal the delta mid-history");
        }
    }

    // Fully-sealed schedule: seal the whole delta once after the last write so the
    // grid resolves entirely from sealed segments.
    if seal_mode == 1 {
        engine.flush().expect("seal the delta into segments");
    }

    Seed {
        engine,
        create_c,
        hi,
        dml_ops,
    }
}

/// The first `(s, v)` point at which the two grids disagree, rendered as a minimal
/// repro: a single `AS OF` query reproduces it under the printed seed.
fn first_divergence(got: &Grid, want: &Grid) -> String {
    let mut points: BTreeSet<(i64, i64)> = got.keys().copied().collect();
    points.extend(want.keys().copied());
    for (s, v) in points {
        let g = got.get(&(s, v));
        let w = want.get(&(s, v));
        if g != w {
            return format!(
                "first divergence at `FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}`:\n  \
                 engine = {:?}\n  duckdb = {:?}",
                g.map(decode_point),
                w.map(decode_point),
            );
        }
    }
    "(grids differ but no point-level divergence found — should be unreachable)".to_string()
}

// --- 1. the differential ----------------------------------------------------

/// Over the seed sweep, at every `(sys, valid)` grid point the engine — read
/// entirely through the SQL bind→exec path — and the naïve DuckDB reference return
/// byte-identical results, both half-open axes probed exhaustively across the
/// delta/sealed-segment merge. The DoD floor of ≥10⁶ randomized `(op, probe)`
/// pairs is asserted, not assumed; a failure prints the seed and the first
/// diverging point.
#[test]
fn duckdb_sql_path_differential_matches_engine_over_a_million_ops() {
    let duck = DuckModel::new();
    let mut total_ops: u64 = 0;

    for seed in 0..SEEDS {
        let Seed {
            mut engine,
            create_c,
            hi,
            dml_ops,
        } = run_seed(seed, &duck);

        // System axis from the table's creation through one past the last commit;
        // valid axis across `[0, VMAX]` and one past the upper end (the half-open
        // edges). The valid domain floors at 0 — `AS OF` rejects a negative
        // instant — so the lower bound stays at 0.
        let (s_lo, s_hi) = (create_c, hi + 1);
        let (v_lo, v_hi) = (0, VMAX + 1);

        let want = duck.grid(s_lo, s_hi, v_lo, v_hi);
        let got = stele_grid(&mut engine, s_lo, s_hi, v_lo, v_hi);

        assert!(
            got == want,
            "seed {seed}: the SQL-path engine diverged from the DuckDB reference.\n{}\n\
             reproduce: this is seed {seed} of `duckdb_sql_path_differential_matches_engine_over_a_million_ops`",
            first_divergence(&got, &want),
        );

        // Count the work: every DML op plus every probed grid cell (present or
        // absent — an absent cell is a probe whose answer is "no row").
        let s_cells = (s_hi - s_lo + 1) as u64;
        let v_cells = (v_hi - v_lo + 1) as u64;
        total_ops += dml_ops + s_cells * v_cells * KEY_POOL as u64;
    }

    eprintln!("SQL-path differential exercised {total_ops} ops/probes over {SEEDS} seeds");
    assert!(
        total_ops >= OP_FLOOR,
        "differential exercised {total_ops} ops/probes; DoD floor is {OP_FLOOR} — raise SEEDS",
    );
}

// --- 2. period predicates gate the bitemporal read --------------------------

/// A `WHERE PERIOD(a, b) <pred> PERIOD(c, d)` constant period predicate ([STL-165])
/// gates the bitemporal read all-or-nothing, and the gate the engine folds matches
/// DuckDB's independent boolean evaluation of the same half-open relation. Each of
/// the seven SQL:2011 predicates is exercised across operand quads that flip it
/// both true and false; the test asserts it saw both a passing and a failing gate,
/// so the differential cannot be vacuous.
#[test]
fn period_predicate_gates_the_bitemporal_read_vs_duckdb() {
    let duck = DuckModel::new();
    let mut engine = SessionEngine::open(MemDisk::new(), OriginClock);
    engine
        .execute(&parse_one(
            "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
        ))
        .expect("create valid-time table");
    // Two keys, both valid `[0, 100)`, committed at the same system reign.
    engine
        .execute(&parse_one("INSERT INTO acct VALUES (1, 10, 0, 100)"))
        .expect("insert key 1");
    engine
        .execute(&parse_one("INSERT INTO acct VALUES (2, 20, 0, 100)"))
        .expect("insert key 2");
    let s = engine.commit_clock().0;
    let v = 50;

    let ungated = select_point(
        &mut engine,
        &format!("SELECT id, balance FROM acct FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}"),
    );
    assert_eq!(
        ungated.len(),
        2,
        "both keys are live on both axes at (s, 50)"
    );

    // Each predicate's SQL keyword paired with its half-open truth as a DuckDB
    // boolean expression over the operands `a < b` (left period) and `c < d`.
    // `MEETS` is the surface spelling of IMMEDIATELY PRECEDES.
    let predicates: [&str; 7] = [
        "CONTAINS",
        "OVERLAPS",
        "EQUALS",
        "PRECEDES",
        "SUCCEEDS",
        "MEETS",
        "IMMEDIATELY SUCCEEDS",
    ];
    // Well-formed operands (`a < b`, `c < d`) chosen to flip the predicates across
    // both branches: nested, adjacent, equal, partial overlap, disjoint.
    let quads: [(i64, i64, i64, i64); 6] = [
        (10, 20, 12, 15),
        (10, 20, 20, 30),
        (10, 20, 10, 20),
        (20, 30, 10, 20),
        (10, 20, 15, 25),
        (10, 20, 30, 40),
    ];

    for pred in predicates {
        // Each predicate must flip across both branches *on its own*, so a later
        // edit to `quads` that makes one vacuously always-true/false is caught.
        let mut saw_pass = false;
        let mut saw_fail = false;
        for (a, b, c, d) in quads {
            let truth_expr = match pred {
                "CONTAINS" => format!("{a} <= {c} AND {d} <= {b}"),
                "OVERLAPS" => format!("{a} < {d} AND {c} < {b}"),
                "EQUALS" => format!("{a} = {c} AND {b} = {d}"),
                "PRECEDES" => format!("{b} <= {c}"),
                "SUCCEEDS" => format!("{d} <= {a}"),
                "MEETS" => format!("{b} = {c}"),
                "IMMEDIATELY SUCCEEDS" => format!("{a} = {d}"),
                other => panic!("unhandled predicate {other}"),
            };
            let truth = duck.truth(&truth_expr);

            let gated = select_point(
                &mut engine,
                &format!(
                    "SELECT id, balance FROM acct \
                     FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v} \
                     WHERE PERIOD({a}, {b}) {pred} PERIOD({c}, {d})"
                ),
            );
            let expected = if truth { ungated.clone() } else { Vec::new() };
            assert!(
                gated == expected,
                "`PERIOD({a}, {b}) {pred} PERIOD({c}, {d})`: DuckDB truth = {truth}, but the \
                 engine's gated read was {:?} (expected {:?})",
                decode_point(&gated),
                decode_point(&expected),
            );

            saw_pass |= truth;
            saw_fail |= !truth;
        }
        assert!(
            saw_pass && saw_fail,
            "predicate `{pred}` was never exercised on both branches — its operand \
             quads went vacuous",
        );
    }
}

// --- 3. the harness can actually fail ---------------------------------------

/// Guards against a vacuous oracle: if the DuckDB reference is fed a deliberately
/// stale close (`sys_to` materialized one tick *past* the successor's `sys_from`,
/// the canonical [ADR-0023] violation), the prior value lingers into its
/// successor's reign and the grid diff against the (correct) SQL-path engine
/// **must** fire. A differential that cannot detect this bug would be worthless.
#[test]
fn sql_path_oracle_detects_a_deliberate_stale_close() {
    let duck = DuckModel::new();
    let mut engine = SessionEngine::open(MemDisk::new(), OriginClock);
    engine
        .execute(&parse_one(
            "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
        ))
        .expect("create valid-time table");

    // A minimal two-version history on one key, valid `[0, 100)`: INSERT "1", then
    // UPDATE "2". Drive Stele honestly; corrupt only the DuckDB mirror.
    engine
        .execute(&parse_one("INSERT INTO acct VALUES (1, 1, 0, 100)"))
        .expect("insert");
    let c0 = engine.commit_clock().0;
    engine
        .execute(&parse_one(
            "UPDATE acct SET balance = 2, vf = 0, vt = 100 WHERE id = 1",
        ))
        .expect("update");
    let c1 = engine.commit_clock().0;
    assert!(c0 < c1, "the update commits strictly after the insert");

    // The DuckDB mirror, but with the prior period's close shoved one tick late —
    // "1" now overlaps "2" on the system axis at `[c1, c1+1)`.
    duck.insert(1, c0, 0, 100, 1);
    duck.conn
        .execute(
            "UPDATE versions SET sys_to = ? WHERE k = 1 AND sys_to = ?;",
            params![c1 + 1, OPEN],
        )
        .expect("stale close");
    duck.insert(1, c1, 0, 100, 2);

    let (s_lo, s_hi) = (c0, c1 + 1);
    let (v_lo, v_hi) = (0, VMAX + 1);
    let want = duck.grid(s_lo, s_hi, v_lo, v_hi);
    let got = stele_grid(&mut engine, s_lo, s_hi, v_lo, v_hi);

    assert!(
        got != want,
        "a stale close in the reference must diverge from the SQL-path engine — \
         otherwise the differential proves nothing",
    );
}
