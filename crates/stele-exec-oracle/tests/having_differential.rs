//! DuckDB differential oracle for **`HAVING` post-aggregation filtering** (STL-265),
//! driven through Stele's whole SQL bind→exec pipeline.
//!
//! STL-171 shipped the hash aggregate (`GROUP BY` / `COUNT` / `SUM` / `MIN` /
//! `MAX`); STL-265 adds the `HAVING` filter Postgres applies *after* aggregation
//! and *before* the `DISTINCT` / `ORDER BY` / `LIMIT` tail. This sweep builds the
//! same randomized single-table fixture in an in-memory `SessionEngine` and an
//! in-memory DuckDB, runs each `GROUP BY … HAVING …` template — *verbatim*, the
//! text is valid in both dialects — against both, and asserts agreement.
//!
//! The fixture seeds NULLs in both the grouping key (`g`) and the aggregated value
//! (`a`), so the NULL-group case (a NULL key forms its own group) and the
//! three-valued `HAVING` (a group whose `SUM`/`MIN` is NULL is never kept) are
//! exercised on both engines, with DuckDB the independent witness. Each template
//! filters on a different shape — `COUNT(*)`, a `SUM`/`COUNT`/`MIN`/`MAX` the
//! SELECT list does not project, an arithmetic of an aggregate, and the grouping
//! column itself. Results are compared as a sorted column-0 multiset (the
//! surviving groups' keys or counts, NULLs included); exact ordering is asserted
//! by the in-process engine unit tests.
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

/// Seeds in the sweep. Each runs every template against a fresh random fixture;
/// deterministic, so the realized check count is fixed (never flaky).
const SEEDS: u64 = 300;
/// Rows per seed. Small so grouping keys collide often (groups hold several rows,
/// the interesting case for `HAVING COUNT(*)`/`SUM`) while the sweep stays cheap.
const ROWS: i64 = 6;
/// Distinct grouping-key values, kept tiny so `g` collides into real groups.
const G_POOL: i64 = 3;
/// Distinct aggregated values.
const A_POOL: i64 = 5;
/// One in `NULL_ODDS` generated `g` / `a` cells is NULL — enough that most seeds
/// exercise the NULL-group and NULL-aggregate paths.
const NULL_ODDS: u64 = 4;

/// `GROUP BY … HAVING …` templates, each valid **verbatim** in both Stele and
/// DuckDB. Column 0 is an integer (the grouping key `g`, or a `COUNT(*)`), possibly
/// NULL (the NULL group's key) — compared as a sorted multiset.
const QUERIES: &[&str] = &[
    // HAVING on the projected COUNT(*): keep groups with more than one row.
    "SELECT g FROM t GROUP BY g HAVING COUNT(*) > 1",
    // HAVING on a SUM the SELECT list does not project (NULL-aware sum).
    "SELECT g FROM t GROUP BY g HAVING SUM(a) > 5",
    // HAVING on COUNT(a) — counts only non-NULL `a` in the group.
    "SELECT g FROM t GROUP BY g HAVING COUNT(a) >= 2",
    // HAVING on MIN / MAX of the nullable value (NULL aggregate ⇒ group dropped).
    "SELECT g FROM t GROUP BY g HAVING MIN(a) >= 1",
    "SELECT g FROM t GROUP BY g HAVING MAX(a) < 4",
    // HAVING on the grouping column itself (the NULL group is dropped: NULL >= 1).
    "SELECT g FROM t GROUP BY g HAVING g >= 1",
    // Integer arithmetic over an aggregate.
    "SELECT g FROM t GROUP BY g HAVING COUNT(*) * 2 > 3",
    // A not-equal on a (possibly-NULL) sum.
    "SELECT g FROM t GROUP BY g HAVING SUM(a) <> 0",
    // COUNT(*)-projecting template: column 0 is the surviving groups' counts.
    "SELECT COUNT(*) FROM t GROUP BY g HAVING SUM(a) > 3",
    // The ungrouped whole-table group, filtered by HAVING.
    "SELECT COUNT(*) FROM t HAVING COUNT(*) > 2",
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

/// One generated row: `(id, g, a)` with `id` a never-NULL key and `g` / `a`
/// nullable.
type Row = (i64, Option<i64>, Option<i64>);

/// Generate `ROWS` rows with ids `1..=ROWS`, random nullable `g` / `a`.
fn gen_rows(rng: &mut Rng) -> Vec<Row> {
    (1..=ROWS)
        .map(|id| (id, rng.opt(G_POOL), rng.opt(A_POOL)))
        .collect()
}

/// Render an optional integer as a SQL literal (`NULL` or the value) — identical
/// text for both engines.
fn sql_lit(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |v| v.to_string())
}

/// Decode Stele result column 0 of every row to `Option<i64>`, sorted — works for
/// an `INT4` grouping key or an `INT8` count, NULL group keys included.
fn stele_col0(engine: &mut SessionEngine<OriginClock, MemDisk>, sql: &str) -> Vec<Option<i64>> {
    let StatementOutcome::Rows(result) = engine.execute(&parse_one(sql)).expect("stele select")
    else {
        panic!("a SELECT must return rows");
    };
    let ty = result.columns[0].1;
    let mut out: Vec<Option<i64>> = result
        .rows
        .iter()
        .map(|row| {
            row[0].as_ref().map(|bytes| {
                match ScalarValue::decode(ty, bytes).expect("decode column 0") {
                    ScalarValue::Int4(v) => i64::from(v),
                    ScalarValue::Int8(v) => v,
                    // Not debug-formatted: a `{:?}` of `ScalarValue` (which has a
                    // `Uuid` variant) trips CodeQL's cleartext-logging rule, a known
                    // false positive in these test panics.
                    _ => panic!("column 0 is neither INT4 nor INT8"),
                }
            })
        })
        .collect();
    out.sort_unstable();
    out
}

/// Run the same query over the DuckDB reference, column 0 as `Option<i64>`, sorted.
fn duck_col0(conn: &Connection, sql: &str) -> Vec<Option<i64>> {
    let mut stmt = conn.prepare(sql).expect("prepare reference query");
    let mut out: Vec<Option<i64>> = stmt
        .query_map([], |row| {
            // INTEGER keys and BIGINT counts both widen to i64 cleanly; a NULL
            // group key reads as `None`.
            row.get::<_, Option<i64>>(0)
                .or_else(|_| row.get::<_, Option<i32>>(0).map(|o| o.map(i64::from)))
        })
        .expect("run reference query")
        .map(|r| r.expect("reference row"))
        .collect();
    out.sort_unstable();
    out
}

#[test]
fn having_agrees_with_duckdb() {
    let mut checks = 0u64;
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let rows = gen_rows(&mut rng);

        // Stele: a system-versioned table, read at the present.
        let mut engine = SessionEngine::open(MemDisk::new(), OriginClock);
        engine
            .execute(&parse_one(
                "CREATE TABLE t (id INT PRIMARY KEY, g INT, a INT) WITH SYSTEM VERSIONING",
            ))
            .expect("stele create");
        // DuckDB: a plain nullable table.
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute_batch("CREATE TABLE t (id INTEGER, g INTEGER, a INTEGER);")
            .expect("duckdb create");

        for (id, g, a) in &rows {
            let values = format!("({id}, {}, {})", sql_lit(*g), sql_lit(*a));
            engine
                .execute(&parse_one(&format!("INSERT INTO t VALUES {values}")))
                .expect("stele insert");
            conn.execute(&format!("INSERT INTO t VALUES {values};"), [])
                .expect("duckdb insert");
        }

        for query in QUERIES {
            let got = stele_col0(&mut engine, query);
            let want = duck_col0(&conn, query);
            assert_eq!(
                got, want,
                "seed {seed}: `{query}` diverged\n  rows = {rows:?}"
            );
            checks += 1;
        }
    }
    assert_eq!(
        checks,
        SEEDS * QUERIES.len() as u64,
        "every seed runs every HAVING template against DuckDB"
    );
}
