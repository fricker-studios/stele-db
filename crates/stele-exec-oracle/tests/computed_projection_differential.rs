//! DuckDB differential oracle for **richer computed select-list expressions**
//! (STL-332), driven through Stele's whole SQL bind→exec pipeline.
//!
//! STL-303 projected a computed expression anchored on a single column; STL-332
//! lifts that to multi-column arithmetic (`k + a`), column-free arithmetic
//! (`1 + 2`), and an uncorrelated scalar subquery embedded **inside** an expression
//! (`a + (SELECT max(b) FROM s)`). This sweep builds the **same** randomized
//! two-table fixture — with NULLs seeded in both value columns, so 3VL propagation
//! through the arithmetic is exercised — in an in-memory `SessionEngine` and an
//! in-memory DuckDB, runs each computed-projection query *verbatim* (the text is
//! valid in both dialects), and asserts the projected cells agree value-for-value.
//!
//! Each query projects the never-NULL `id` first and `ORDER BY id`, so the two
//! result sets line up row-for-row and the diff is exact (NULL vs a value is a
//! divergence, never silently coalesced). The cardinality rule (`21000` for an
//! embedded subquery returning more than one row) and the no-implicit-coercion
//! posture are pinned by deterministic unit tests in `stele-engine`; this oracle
//! covers the *values*.
//!
//! Arithmetic is restricted to `+` / `-` / `*`: Stele folds integer divide-by-zero
//! to a NULL cell ([STL-207]) while DuckDB's posture differs, so `/` and `%` would
//! diff on behavior rather than value — that boundary is out of this oracle's scope.
//!
//! DuckDB is confined to this nightly-only crate (a dev-dependency, never linked
//! into a shipped crate; held off the per-PR `--workspace` runs, [STL-158]), so the
//! bundled C++ amalgamation never gates a PR and the runtime-agnostic core never
//! links it ([ADR-0010]).

#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use duckdb::Connection;

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};
use stele_engine::{SessionEngine, StatementOutcome};
use stele_sql::Statement;
use stele_storage::backend::MemDisk;

// --- knobs -----------------------------------------------------------------

/// Seeds in the sweep. Each runs every query template against a fresh random
/// fixture; deterministic, so the realized check count is fixed (never flaky).
const SEEDS: u64 = 300;
/// Rows per table per seed. Small so the subquery aggregates see a real set and
/// values collide, while the sweep stays cheap.
const ROWS: i64 = 6;
/// Distinct `k` values, kept tiny so the small integer domain never overflows the
/// `int4` arithmetic (`k * a` and the sums stay well inside `i32`).
const K_POOL: i64 = 4;
/// Distinct `a` values (same small-domain reasoning).
const A_POOL: i64 = 5;
/// One in `NULL_ODDS` generated `k` / `a` cells is NULL — enough that most seeds
/// exercise NULL propagation through the projected arithmetic (3VL).
const NULL_ODDS: u64 = 4;

/// The computed-projection query templates, each valid **verbatim** in both Stele
/// and DuckDB and paired with its projected column width. Every query projects the
/// never-NULL `id` first and orders by it, so the two result sets align row-for-row.
const QUERIES: &[(&str, usize)] = &[
    // Column + literal — the STL-303 single-anchor shape (regression guard).
    ("SELECT id, a + 1 AS c FROM t ORDER BY id", 2),
    // Multi-column arithmetic (STL-332).
    ("SELECT id, k + a AS c FROM t ORDER BY id", 2),
    // Three operands over two columns, mixing operators.
    ("SELECT id, k * a - a AS c FROM t ORDER BY id", 2),
    // Column-free arithmetic (STL-332) — broadcast on every row.
    ("SELECT id, 1 + 2 * 3 AS c FROM t ORDER BY id", 2),
    // Embedded uncorrelated scalar subquery operand (STL-332), resolved once.
    (
        "SELECT id, a + (SELECT max(a) FROM s) AS c FROM t ORDER BY id",
        2,
    ),
    // Two embedded subqueries combined, plus a column.
    (
        "SELECT id, k + (SELECT max(a) FROM s) - (SELECT min(k) FROM s) AS c FROM t ORDER BY id",
        2,
    ),
];

// --- harness ---------------------------------------------------------------

/// A trivial clock pinned at the origin; the engine's `MonotonicClock` still hands
/// out strictly increasing commit instants, so every write gets a distinct
/// `sys_from` ([ADR-0010] determinism — a failing seed reproduces bit-for-bit).
#[derive(Clone, Copy)]
struct OriginClock;
impl Clock for OriginClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

/// Tiny xorshift64* — deterministic and dependency-free, matching the sibling
/// oracles so a failing seed reproduces bit-for-bit.
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
    /// A value in `[0, pool)`, or `None` one time in [`NULL_ODDS`].
    fn opt(&mut self, pool: i64) -> Option<i64> {
        if self.next_u64() % NULL_ODDS == 0 {
            None
        } else {
            Some(self.range(pool))
        }
    }
}

/// Parse exactly one SQL statement (the oracle only ever feeds one).
fn parse_one(sql: &str) -> Statement {
    let mut stmts = stele_sql::parse(sql).expect("parse");
    assert_eq!(stmts.len(), 1, "feed exactly one statement");
    stmts.pop().expect("one statement")
}

/// One generated row: `(id, k, a)` with `id` a never-NULL key and `k` / `a`
/// nullable.
type Row = (i64, Option<i64>, Option<i64>);

/// Generate `ROWS` rows with ids `1..=ROWS`, random nullable `k` / `a`.
fn gen_rows(rng: &mut Rng) -> Vec<Row> {
    (1..=ROWS)
        .map(|id| (id, rng.opt(K_POOL), rng.opt(A_POOL)))
        .collect()
}

/// Render an optional integer as a SQL literal (`NULL` or the value) — identical
/// text for both engines.
fn sql_lit(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |v| v.to_string())
}

/// Decode a present integer result cell (`int4` or the aggregate `int8`) to its
/// value; a NULL cell stays `None`. The projected arithmetic over `int4` columns is
/// `int4`, but the decode is type-driven so a wider result still compares by value.
fn int_cell(ty: LogicalType, bytes: &[u8]) -> i64 {
    match ScalarValue::decode(ty, bytes).expect("decode integer cell") {
        ScalarValue::Int4(v) => i64::from(v),
        ScalarValue::Int8(v) => v,
        // The workload projects only integer columns. Name the type, not the value
        // (the CodeQL cleartext-logging heuristic taints the `ScalarValue` enum).
        other => panic!("expected an integer cell, got {:?}", other.logical_type()),
    }
}

/// Run one Stele `SELECT` and return its rows as decoded integer cells, in result
/// order (each query orders by `id`, so the order is total).
fn stele_rows(
    engine: &mut SessionEngine<OriginClock, MemDisk>,
    sql: &str,
) -> Vec<Vec<Option<i64>>> {
    let StatementOutcome::Rows(result) = engine.execute(&parse_one(sql)).expect("stele select")
    else {
        panic!("a SELECT must return rows");
    };
    result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(&result.columns)
                .map(|(cell, (_, ty))| cell.as_deref().map(|bytes| int_cell(*ty, bytes)))
                .collect()
        })
        .collect()
}

/// Run the same `SELECT` over the DuckDB reference, `width` integer columns, rows in
/// result order.
fn duck_rows(conn: &Connection, sql: &str, width: usize) -> Vec<Vec<Option<i64>>> {
    let mut stmt = conn.prepare(sql).expect("prepare reference query");
    let rows = stmt
        .query_map([], |row| {
            (0..width).map(|i| row.get::<_, Option<i64>>(i)).collect()
        })
        .expect("run reference query");
    rows.map(|r| r.expect("reference row")).collect()
}

/// Build the `(t, s)` fixture in a fresh `SessionEngine` and a fresh in-memory
/// DuckDB, inserting the **same** rows into both (Stele system-versioned, DuckDB
/// plain nullable). Returns both handles plus the generated rows for failure repros.
fn build_fixture(
    seed: u64,
) -> (
    SessionEngine<OriginClock, MemDisk>,
    Connection,
    Vec<Row>,
    Vec<Row>,
) {
    let mut rng = Rng::new(seed);
    let t_rows = gen_rows(&mut rng);
    let s_rows = gen_rows(&mut rng);

    let mut engine = SessionEngine::open(MemDisk::new(), OriginClock);
    for ddl in [
        "CREATE TABLE t (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
        "CREATE TABLE s (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
    ] {
        engine.execute(&parse_one(ddl)).expect("stele create");
    }
    let conn = Connection::open_in_memory().expect("open in-memory duckdb");
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER, k INTEGER, a INTEGER); \
         CREATE TABLE s (id INTEGER, k INTEGER, a INTEGER);",
    )
    .expect("duckdb create");

    for (table, rows) in [("t", &t_rows), ("s", &s_rows)] {
        for (id, k, a) in rows {
            let values = format!("({id}, {}, {})", sql_lit(*k), sql_lit(*a));
            engine
                .execute(&parse_one(&format!("INSERT INTO {table} VALUES {values}")))
                .expect("stele insert");
            conn.execute(&format!("INSERT INTO {table} VALUES {values};"), [])
                .expect("duckdb insert");
        }
    }
    (engine, conn, t_rows, s_rows)
}

// --- 1. the differential ----------------------------------------------------

#[test]
fn computed_projections_agree_with_duckdb() {
    let mut checks = 0u64;
    for seed in 0..SEEDS {
        let (mut engine, conn, t_rows, s_rows) = build_fixture(seed);
        for (query, width) in QUERIES {
            let got = stele_rows(&mut engine, query);
            let want = duck_rows(&conn, query, *width);
            assert_eq!(
                got, want,
                "seed {seed}: `{query}` diverged\n  t = {t_rows:?}\n  s = {s_rows:?}"
            );
            checks += 1;
        }
    }
    assert_eq!(
        checks,
        SEEDS * QUERIES.len() as u64,
        "every seed runs every query template against DuckDB"
    );
}

// --- 2. the harness can actually fail ---------------------------------------

/// Guards against a vacuous oracle: a deliberately *wrong* reference query
/// (`a + 2` against the engine's `a + 1`) must diverge over a fixture that has at
/// least one non-NULL `a`. A differential that cannot detect this would prove
/// nothing.
#[test]
fn computed_projection_oracle_detects_a_deliberate_divergence() {
    // Seed 0's `t` has at least one non-NULL `a` (checked below), so `a + 1` and
    // `a + 2` differ on that row.
    let (mut engine, conn, t_rows, _s) = build_fixture(0);
    assert!(
        t_rows.iter().any(|(_, _, a)| a.is_some()),
        "fixture must have a non-NULL `a` for the divergence to be observable",
    );
    let got = stele_rows(&mut engine, "SELECT id, a + 1 AS c FROM t ORDER BY id");
    let want = duck_rows(&conn, "SELECT id, a + 2 AS c FROM t ORDER BY id", 2);
    assert_ne!(
        got, want,
        "a wrong reference expression must diverge from the engine — otherwise the \
         differential proves nothing",
    );
}
