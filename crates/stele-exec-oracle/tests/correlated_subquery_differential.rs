//! DuckDB differential oracle for **correlated subqueries** (STL-239), driven
//! through Stele's whole SQL bind→exec pipeline.
//!
//! Correlated `EXISTS` / `NOT EXISTS` / `IN` / `NOT IN` and a correlated scalar
//! lookup were each first shipped on [STL-239]'s per-row re-execution (performance
//! is explicitly not the v0.3 bar). Some shapes now **decorrelate** onto a single
//! hash join — `EXISTS` / `NOT EXISTS` onto a semi / anti join ([STL-317]), `IN`
//! onto a composite-key semi join ([STL-337]) — while `NOT IN`, a non-equality
//! correlation, and the scalar lookup stay per-row. This oracle is **path-agnostic**:
//! it builds the **same** randomized two-table fixture in an in-memory
//! `SessionEngine` and an in-memory DuckDB, runs each correlated query — *verbatim*,
//! the text is valid in both dialects — against both, and asserts the returned `id`
//! set agrees, so it witnesses that the decorrelated and per-row paths alike match
//! the reference.
//!
//! The fixture deliberately seeds NULLs in both the correlation key (`k`) and the
//! membership value (`a`), because the interesting divergences live there:
//!
//! * a **NULL correlation key** drops the outer row (the inner is empty for it — the
//!   per-row short-circuit `empty_inner_keeps`, or, decorrelated, a NULL join key
//!   that never matches) — DuckDB agrees by evaluating `s.k = NULL` as unknown;
//! * **`IN` / `NOT IN` over an inner set or membership value that is NULL** — `IN` is
//!   never TRUE when the membership value (either side) is NULL, and `NOT IN` over a
//!   set containing a NULL is never TRUE for any outer row whose set it lands in (the
//!   classic three-valued trap) — DuckDB is the independent witness that Stele gets
//!   the 3VL right, whether `IN` decorrelates to the composite semi join or `NOT IN`
//!   folds per row.
//!
//! DuckDB is confined to this nightly-only crate (a dev-dependency, never linked
//! into a shipped crate; held off the per-PR `--workspace` runs, [STL-158]), so
//! the bundled C++ amalgamation never gates a PR and the runtime-agnostic core
//! never links it ([ADR-0010]).

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
/// Rows per table per seed. Small so correlation keys collide often (the inner
/// returns a real *set*, the interesting case) while the sweep stays cheap.
const ROWS: i64 = 6;
/// Distinct correlation-key values, kept tiny so `s.k = t.k` groups overlap.
const K_POOL: i64 = 3;
/// Distinct membership values.
const A_POOL: i64 = 5;
/// One in `NULL_ODDS` generated `k` / `a` cells is NULL — enough that most seeds
/// exercise the NULL-correlation and NULL-member paths.
const NULL_ODDS: u64 = 4;

/// The correlated query templates, each valid **verbatim** in both Stele and
/// DuckDB. `EXISTS` / `IN` correlate on the non-unique `k` (so the inner is a set);
/// the scalar correlates on the unique `id` (so the inner is at most one row, never
/// a cardinality violation on either engine).
const QUERIES: &[&str] = &[
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.k)",
    "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k)",
    "SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k)",
    "SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.k = t.k)",
    "SELECT id FROM t WHERE a = (SELECT a FROM s WHERE s.id = t.id)",
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

/// Run one Stele `SELECT id …` and return the ids ascending.
fn stele_ids(engine: &mut SessionEngine<OriginClock, MemDisk>, sql: &str) -> Vec<i32> {
    let StatementOutcome::Rows(result) = engine.execute(&parse_one(sql)).expect("stele select")
    else {
        panic!("a SELECT must return rows");
    };
    let mut ids: Vec<i32> = result
        .rows
        .iter()
        .map(|row| {
            let bytes = row[0].as_ref().expect("id is never NULL");
            match ScalarValue::decode(LogicalType::Int4, bytes).expect("decode id") {
                ScalarValue::Int4(v) => v,
                _ => panic!("id is INT"),
            }
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Run the same `SELECT id …` over the DuckDB reference, ids ascending.
fn duck_ids(conn: &Connection, sql: &str) -> Vec<i32> {
    let mut stmt = conn.prepare(sql).expect("prepare reference query");
    let mut ids: Vec<i32> = stmt
        .query_map([], |row| row.get::<_, i32>(0))
        .expect("run reference query")
        .map(|r| r.expect("reference row"))
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn correlated_subqueries_agree_with_duckdb() {
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

        for query in QUERIES {
            let got = stele_ids(&mut engine, query);
            let want = duck_ids(&conn, query);
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
