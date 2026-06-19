//! DuckDB differential oracle for **non-recursive CTEs and derived tables**
//! (STL-242), driven through Stele's whole SQL bind→exec pipeline.
//!
//! A `WITH name AS (SELECT …)` result — and a `FROM (SELECT …) AS d` derived table
//! — is materialized once and referenced like a table ([STL-242]). This sweep
//! builds the **same** randomized two-table fixture in an in-memory `SessionEngine`
//! and an in-memory DuckDB, runs each CTE / derived-table query — *verbatim*, the
//! text is valid in both dialects — against both, and asserts agreement. The
//! templates mirror the DoD: a plain CTE reference, CTE → CTE chaining, a CTE
//! joined to a base table, a derived table (standalone and joined), and a CTE
//! under aggregation.
//!
//! The fixture seeds NULLs in the join key (`k`) and the filtered value (`a`), so
//! the three-valued comparisons inside a CTE body and the NULL-key join (a NULL
//! never matches) are exercised on both engines, with DuckDB the independent
//! witness.
//!
//! DuckDB is confined to this nightly-only crate (a dev-dependency, never linked
//! into a shipped crate; held off the per-PR `--workspace` runs, [STL-158]), so
//! the bundled C++ amalgamation never gates a PR and the runtime-agnostic core
//! never links it ([ADR-0010]).

#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use duckdb::Connection;

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::{SessionEngine, StatementOutcome};
use stele_sql::Statement;
use stele_storage::backend::MemDisk;

// --- knobs -----------------------------------------------------------------

/// Seeds in the sweep. Each runs every query template against a fresh random
/// fixture; deterministic, so the realized check count is fixed (never flaky).
const SEEDS: u64 = 300;
/// Rows per table per seed. Small so join keys collide often (the join produces a
/// real set) while the sweep stays cheap.
const ROWS: i64 = 6;
/// Distinct join-key values, kept tiny so `c.k = s.k` groups overlap.
const K_POOL: i64 = 3;
/// Distinct filtered values.
const A_POOL: i64 = 5;
/// One in `NULL_ODDS` generated `k` / `a` cells is NULL — enough that most seeds
/// exercise the NULL-key join and NULL-comparison paths.
const NULL_ODDS: u64 = 4;

/// `id`-returning templates (column 0 is the `INT` business key), each valid
/// **verbatim** in both Stele and DuckDB.
const ID_QUERIES: &[&str] = &[
    // A plain CTE reference, with a `WHERE` in the body and over the CTE.
    "WITH big AS (SELECT id, a FROM t WHERE a >= 2) SELECT id FROM big WHERE a <= 4",
    // CTE → CTE chaining: `c2` reads `c1`.
    "WITH c1 AS (SELECT id, a FROM t WHERE a >= 1), \
          c2 AS (SELECT id, a FROM c1 WHERE a <= 3) \
     SELECT id FROM c2",
    // A CTE joined to a base table on the nullable key (NULL never matches).
    "WITH c AS (SELECT id, k FROM t) SELECT c.id FROM c JOIN s ON c.k = s.k",
    // A derived table (`FROM (SELECT …) AS d`), with a `WHERE` over it.
    "SELECT id FROM (SELECT id, a FROM t WHERE a >= 2) AS d WHERE a <= 4",
    // A derived table joined to a base table.
    "SELECT d.id FROM (SELECT id, k FROM t) AS d JOIN s ON d.k = s.k",
];

/// `count(*)`-returning templates (column 0 is a `BIGINT`/`INT8` count) — the
/// CTE-under-aggregation case. Compared separately from the `id` sets because the
/// result column is a different width.
const COUNT_QUERIES: &[&str] = &[
    "WITH big AS (SELECT id, a FROM t WHERE a >= 2) SELECT count(*) FROM big",
    "WITH c AS (SELECT id, a FROM t) SELECT count(*) FROM c WHERE a <= 3",
    "SELECT count(*) FROM (SELECT id, k FROM t WHERE k = 1) AS d",
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

/// Decode Stele result column 0 of every row to `i64`, sorted — works for an
/// `INT4` id or an `INT8` count.
fn stele_col0(engine: &mut SessionEngine<OriginClock, MemDisk>, sql: &str) -> Vec<i64> {
    let StatementOutcome::Rows(result) = engine.execute(&parse_one(sql)).expect("stele select")
    else {
        panic!("a SELECT must return rows");
    };
    let ty = result.columns[0].1;
    let mut out: Vec<i64> = result
        .rows
        .iter()
        .map(|row| {
            let bytes = row[0].as_ref().expect("column 0 is never NULL here");
            match ScalarValue::decode(ty, bytes).expect("decode column 0") {
                ScalarValue::Int4(v) => i64::from(v),
                ScalarValue::Int8(v) => v,
                other => panic!("unexpected column-0 type: {other:?}"),
            }
        })
        .collect();
    out.sort_unstable();
    out
}

/// Run the same query over the DuckDB reference, column 0 as `i64`, sorted.
fn duck_col0(conn: &Connection, sql: &str) -> Vec<i64> {
    let mut stmt = conn.prepare(sql).expect("prepare reference query");
    let mut out: Vec<i64> = stmt
        .query_map([], |row| {
            // INTEGER ids and BIGINT counts both widen to i64 cleanly.
            row.get::<_, i64>(0)
                .or_else(|_| row.get::<_, i32>(0).map(i64::from))
        })
        .expect("run reference query")
        .map(|r| r.expect("reference row"))
        .collect();
    out.sort_unstable();
    out
}

#[test]
fn ctes_and_derived_tables_agree_with_duckdb() {
    let mut checks = 0u64;
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let t_rows = gen_rows(&mut rng);
        let s_rows = gen_rows(&mut rng);

        // Stele: system-versioned tables, read at the present.
        let mut engine = SessionEngine::open(MemDisk::new(), OriginClock);
        for ddl in [
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
            "CREATE TABLE s (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
        ] {
            engine.execute(&parse_one(ddl)).expect("stele create");
        }
        // DuckDB: plain nullable tables.
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

        for query in ID_QUERIES.iter().chain(COUNT_QUERIES) {
            let got = stele_col0(&mut engine, query);
            let want = duck_col0(&conn, query);
            assert_eq!(
                got, want,
                "seed {seed}: `{query}` diverged\n  t = {t_rows:?}\n  s = {s_rows:?}"
            );
            checks += 1;
        }
    }
    assert_eq!(
        checks,
        SEEDS * (ID_QUERIES.len() + COUNT_QUERIES.len()) as u64,
        "every seed runs every query template against DuckDB"
    );
}
