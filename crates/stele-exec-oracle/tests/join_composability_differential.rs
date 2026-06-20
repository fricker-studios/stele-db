//! DuckDB differential oracle for **clause composability over a join** (STL-264),
//! driven through Stele's whole SQL bind→exec pipeline.
//!
//! STL-172 shipped the inner / left / semi / anti hash join; STL-264 makes the
//! join output feed the same downstream pipeline a single-table read uses — a
//! `WHERE` over the joined columns, a hash aggregate (`GROUP BY` / `COUNT`), and
//! the `DISTINCT` / `ORDER BY` / `LIMIT` tail — with qualified-name resolution
//! across both inputs. This sweep builds the **same** randomized two-table fixture
//! in an in-memory `SessionEngine` and an in-memory DuckDB, runs each composed
//! query — *verbatim*, the text is valid in both dialects — against both, and
//! asserts agreement.
//!
//! The fixture seeds NULLs in the join key (`k`) and the filtered value (`a`), so
//! the NULL-key join (a NULL never matches) and the three-valued `WHERE` are
//! exercised on both engines, with DuckDB the independent witness. Results are
//! compared as sorted column-0 multisets; the `ORDER BY … LIMIT` template uses the
//! key column so its kept multiset is deterministic regardless of tie order (exact
//! ordering is asserted by the in-process engine unit tests).
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
/// Distinct join-key values, kept tiny so `t.k = s.k` matches overlap.
const K_POOL: i64 = 3;
/// Distinct filtered values.
const A_POOL: i64 = 5;
/// One in `NULL_ODDS` generated `k` / `a` cells is NULL — enough that most seeds
/// exercise the NULL-key join and NULL-comparison paths.
const NULL_ODDS: u64 = 4;

/// `id`/`k`-returning templates (column 0 is an `INT`), each valid **verbatim** in
/// both Stele and DuckDB. Every one composes a join with at least one other clause.
const ID_QUERIES: &[&str] = &[
    // Inner join + WHERE on a qualified left column.
    "SELECT t.id FROM t JOIN s ON t.k = s.k WHERE t.a >= 2",
    // WHERE on an *unprojected* right column.
    "SELECT t.id FROM t JOIN s ON t.k = s.k WHERE s.a <= 3",
    // WHERE with integer arithmetic over a join column.
    "SELECT t.id FROM t JOIN s ON t.k = s.k WHERE t.a % 2 = 0",
    // LEFT join + WHERE on a left column keeps NULL-extended rows.
    "SELECT t.id FROM t LEFT JOIN s ON t.k = s.k WHERE t.a >= 2",
    // ORDER BY + LIMIT: the kept multiset of the 3 smallest `t.id` is deterministic.
    "SELECT t.id FROM t JOIN s ON t.k = s.k ORDER BY t.id LIMIT 3",
    // DISTINCT over the join output (a matched left id appears once).
    "SELECT DISTINCT t.id FROM t JOIN s ON t.k = s.k",
];

/// `count(*)`-returning templates (column 0 is a `BIGINT` / `INT8` count) — the
/// aggregate-over-join cases. Compared separately because the result column is a
/// different width.
const COUNT_QUERIES: &[&str] = &[
    // Total inner-join cardinality.
    "SELECT count(*) FROM t JOIN s ON t.k = s.k",
    // Filtered count.
    "SELECT count(*) FROM t JOIN s ON t.k = s.k WHERE t.a >= 2",
    // One count per group (GROUP BY a join column) — the multiset of group counts.
    "SELECT count(*) FROM t JOIN s ON t.k = s.k GROUP BY t.k",
    // LEFT-join cardinality (every left row at least once).
    "SELECT count(*) FROM t LEFT JOIN s ON t.k = s.k",
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
/// `INT4` id / key or an `INT8` count.
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
                // Not debug-formatted: a `{:?}` of `ScalarValue` (which has a
                // `Uuid` variant) trips CodeQL's cleartext-logging rule, a known
                // false positive in these test panics.
                _ => panic!("column 0 is neither INT4 nor INT8"),
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
            // INTEGER ids/keys and BIGINT counts both widen to i64 cleanly.
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
fn join_composability_agrees_with_duckdb() {
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
