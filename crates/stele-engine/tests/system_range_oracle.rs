//! The system-time range-scan correctness oracle ([STL-244], [docs/06 §4]).
//!
//! A `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` read returns **every**
//! version whose system interval `[sys_from, sys_to)` overlaps the range — the
//! "show me the history" shape — not just the one live at a point. Getting the
//! *set* of returned versions right, at the half-open / closed boundaries, is the
//! temporal heart of the feature (the §4 "off-by-one on a half-open interval" bug
//! class), so every range answer the real [`SessionEngine`] gives is checked
//! against a deliberately-dumb in-process reference.
//!
//! The reference tracks the full version timeline as a plain `Vec` of records
//! (each write opens a version and closes the prior one), learning each write's
//! commit instant from [`SessionEngine::commit_clock`] — the same alignment the
//! bitemporal SQL oracle uses ([STL-167]). It then decides overlap by the dumbest
//! correct formulation: the version is active at the integer instants `[vf, vt)`,
//! the query covers `[lo, hi)` (half-open) or `[lo, hi]` (closed), and the two
//! *inclusive integer instant ranges* either intersect or they don't. That is an
//! independent derivation of the semantics, not a copy of the engine's overlap
//! predicate, so it is a real check; the [teeth test](#tests) injects the classic
//! off-by-one (a `<=` where a `<` belongs) and proves the differential catches it.
//!
//! The same workload is replayed with the delta tier flushed at three points
//! (never, midway, fully), so the answer is asserted identical across the
//! delta/sealed boundary — history a range scan must reconstruct the same way
//! whether a version is still staged or sealed into a segment.
//!
//! [docs/06 §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart
//! [STL-167]: https://allegromusic.atlassian.net/browse/STL-167
//! [STL-244]: https://allegromusic.atlassian.net/browse/STL-244

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's `MonotonicClock` turns its readings into
/// the strictly increasing `1, 2, 3, …` the writes need, deterministically — and
/// crucially, with this zero inner clock a *read* never advances the mark
/// ([`MonotonicClock::observe`]), so [`SessionEngine::commit_clock`] read right
/// after a write is exactly that write's commit instant.
#[derive(Debug, Clone, Copy)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

/// A deterministic `splitmix64` so a seed replays an identical workload.
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }
    const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("len fits u64");
        usize::try_from(self.next() % len).expect("index fits usize")
    }
    const fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }
}

/// One version in the reference timeline: the key `id`, its value `a` (nullable),
/// the recorded `sys_from`, and the resolved `sys_to` (`None` while still open).
#[derive(Debug, Clone)]
struct Ver {
    id: i32,
    a: Option<i32>,
    sys_from: i64,
    sys_to: Option<i64>,
}

impl Ver {
    /// The version's last active integer instant: `sys_to - 1` for a closed
    /// version, or "infinity" for an open one — the inclusive upper end of the
    /// `[sys_from, sys_to)` active set.
    fn last_active(&self) -> i64 {
        self.sys_to.map_or(i64::MAX, |to| to - 1)
    }
}

/// Encode a `ScalarValue` to its canonical wire bytes — the exact form a
/// `SelectResult` cell carries.
fn enc(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// When to flush the delta tier into sealed segments, so the range scan is
/// asserted identical across the delta/sealed boundary.
#[derive(Debug, Clone, Copy)]
enum FlushMode {
    /// Everything stays in the hot delta tier.
    Never,
    /// Flush once partway through the workload — a mix of sealed + delta versions.
    Midway,
    /// Flush after every write is staged — every version sealed into a segment.
    Full,
}

/// The reference timeline and the engine after applying a seeded workload under a
/// flush mode. The timeline is flush-independent (flushing never changes history),
/// which is exactly what the cross-mode assertion relies on.
struct World {
    engine: SessionEngine<ZeroClock, MemDisk>,
    versions: Vec<Ver>,
}

/// Apply a seeded workload of insert / update / delete / re-insert operations,
/// recording each version (and its commit instant) in the reference timeline.
fn build(seed: u64, flush: FlushMode) -> World {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
    );

    let mut versions: Vec<Ver> = Vec::new();
    // The index of each key's currently-open version, when it is live.
    let mut open: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
    let key_domain: Vec<i32> = (1..=4).collect();
    let value = |rng: &mut Rng| -> Option<i32> {
        if rng.one_in(4) {
            None
        } else {
            Some(i32::try_from(rng.index(5)).expect("small"))
        }
    };
    let sql_int = |v: Option<i32>| v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());

    let op_count = 12 + rng.index(13); // 12..=24 writes
    let flush_at = op_count / 2;
    for step in 0..op_count {
        let id = key_domain[rng.index(key_domain.len())];
        let live = open.contains_key(&id);
        // Pick an operation valid for the key's liveness: a live key can be
        // updated or deleted; a dead (or never-seen) key can only be inserted.
        // This keeps every statement a success the engine accepts (a point UPDATE /
        // DELETE of an absent key errors; an INSERT of a live key is a duplicate).
        let a = value(&mut rng);
        if live {
            if rng.one_in(3) {
                run(&mut engine, &format!("DELETE FROM t WHERE id = {id}"));
                let t = engine.commit_clock().0;
                let idx = open.remove(&id).expect("live");
                versions[idx].sys_to = Some(t);
            } else {
                run(
                    &mut engine,
                    &format!("UPDATE t SET a = {} WHERE id = {id}", sql_int(a)),
                );
                let t = engine.commit_clock().0;
                let idx = open.remove(&id).expect("live");
                versions[idx].sys_to = Some(t);
                versions.push(Ver {
                    id,
                    a,
                    sys_from: t,
                    sys_to: None,
                });
                open.insert(id, versions.len() - 1);
            }
        } else {
            run(
                &mut engine,
                &format!("INSERT INTO t VALUES ({id}, {})", sql_int(a)),
            );
            let t = engine.commit_clock().0;
            versions.push(Ver {
                id,
                a,
                sys_from: t,
                sys_to: None,
            });
            open.insert(id, versions.len() - 1);
        }

        if matches!(flush, FlushMode::Midway) && step == flush_at {
            run(&mut engine, "FLUSH");
        }
        if matches!(flush, FlushMode::Full) {
            run(&mut engine, "FLUSH");
        }
    }

    World { engine, versions }
}

/// Execute one statement, asserting it succeeds.
fn run(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    let stmt = parse(sql).expect("parse").remove(0);
    engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

/// The reference's expected rows for a range query: every version whose active
/// instant set `[sys_from, sys_to)` intersects the query's instant set, projected
/// as `[id, a, sys_from, sys_to]` — the engine's `SELECT *` range shape. `closed`
/// selects `BETWEEN [lo, hi]` over `FROM..TO [lo, hi)`.
///
/// Overlap is decided by intersecting two inclusive integer ranges — the version's
/// `[sys_from, last_active]` and the query's `[lo, query_hi]` — an independent
/// derivation of the half-open semantics, not the engine's predicate.
fn reference_rows(versions: &[Ver], lo: i64, hi: i64, closed: bool) -> Vec<Vec<Option<Vec<u8>>>> {
    let query_hi = if closed { hi } else { hi - 1 };
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = versions
        .iter()
        .filter(|v| {
            // Non-empty version and query, and the two inclusive instant ranges
            // `[sys_from, last_active]` and `[lo, query_hi]` intersect.
            v.sys_from <= v.last_active()
                && lo <= query_hi
                && v.sys_from.max(lo) <= v.last_active().min(query_hi)
        })
        .map(|v| {
            vec![
                Some(enc(&ScalarValue::Int4(v.id))),
                v.a.map(|a| enc(&ScalarValue::Int4(a))),
                Some(enc(&ScalarValue::TimestampTz(v.sys_from))),
                v.sys_to.map(|to| enc(&ScalarValue::TimestampTz(to))),
            ]
        })
        .collect();
    rows.sort();
    rows
}

/// The engine's rows for a range `SELECT *`, sorted to compare as a set.
fn engine_rows(
    engine: &mut SessionEngine<ZeroClock, MemDisk>,
    sql: &str,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(SelectResult { mut rows, .. }) = engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"))
    else {
        panic!("a range SELECT must return rows for `{sql}`");
    };
    rows.sort();
    rows
}

/// The distinct boundary instants worth probing: every `sys_from` / `sys_to` in
/// the timeline, each also `±1`, so a query edge lands exactly on, just before,
/// and just after every version boundary — the off-by-one surface.
fn boundary_instants(versions: &[Ver]) -> Vec<i64> {
    let mut marks: Vec<i64> = vec![0];
    for v in versions {
        for base in [v.sys_from, v.sys_to.unwrap_or(v.sys_from)] {
            marks.extend([base - 1, base, base + 1]);
        }
    }
    marks.sort_unstable();
    marks.dedup();
    marks
}

#[test]
fn range_scans_match_the_reference_across_seeds_flush_and_boundaries() {
    for seed in 0..40u64 {
        for flush in [FlushMode::Never, FlushMode::Midway, FlushMode::Full] {
            let World {
                mut engine,
                versions,
            } = build(seed, flush);
            let marks = boundary_instants(&versions);
            for (i, &lo) in marks.iter().enumerate() {
                for &hi in &marks[i..] {
                    // Half-open `FROM lo TO hi` needs lo < hi; closed `BETWEEN`
                    // needs lo <= hi (the binder rejects the rest).
                    if lo < hi {
                        let sql = format!("SELECT * FROM t FOR SYSTEM_TIME FROM {lo} TO {hi}");
                        assert_eq!(
                            engine_rows(&mut engine, &sql),
                            reference_rows(&versions, lo, hi, false),
                            "seed {seed}, {flush:?}: {sql}"
                        );
                    }
                    let sql = format!("SELECT * FROM t FOR SYSTEM_TIME BETWEEN {lo} AND {hi}");
                    assert_eq!(
                        engine_rows(&mut engine, &sql),
                        reference_rows(&versions, lo, hi, true),
                        "seed {seed}, {flush:?}: {sql}"
                    );
                }
            }
        }
    }
}

#[test]
fn the_oracle_has_teeth_off_by_one_on_the_half_open_upper() {
    // A reference that treats `FROM lo TO hi` as the *closed* `[lo, hi]` (the
    // classic off-by-one — a `<=` where the half-open form needs `<`) must be
    // caught by the very same differential the test above runs.
    let World {
        mut engine,
        versions,
    } = build(7, FlushMode::Never);
    let mut mismatch = false;
    let marks = boundary_instants(&versions);
    for (i, &lo) in marks.iter().enumerate() {
        for &hi in &marks[i..] {
            if lo >= hi {
                continue;
            }
            let sql = format!("SELECT * FROM t FOR SYSTEM_TIME FROM {lo} TO {hi}");
            let engine = engine_rows(&mut engine, &sql);
            // The buggy reference: closed semantics for a half-open query.
            let buggy = reference_rows(&versions, lo, hi, true);
            if engine != buggy {
                mismatch = true;
            }
        }
    }
    assert!(
        mismatch,
        "an off-by-one reference must disagree with the engine somewhere"
    );
}

#[test]
fn endpoint_columns_and_open_versions_render_as_expected() {
    // A focused, hand-checked timeline: insert v1, update to v2, delete — so key 1
    // has two closed versions; key 2 stays open.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
    );
    run(&mut engine, "INSERT INTO t VALUES (1, 100)");
    let t1 = engine.commit_clock().0;
    run(&mut engine, "UPDATE t SET a = 200 WHERE id = 1");
    let t2 = engine.commit_clock().0;
    run(&mut engine, "INSERT INTO t VALUES (2, 900)");
    let t3 = engine.commit_clock().0;
    run(&mut engine, "DELETE FROM t WHERE id = 1");
    let t4 = engine.commit_clock().0;

    // A range covering the whole timeline returns all three versions of these two
    // keys: key 1 = [t1,t2) value 100, [t2,t4) value 200; key 2 = [t3,+∞) open.
    let rows = engine_rows(
        &mut engine,
        &format!("SELECT * FROM t FOR SYSTEM_TIME FROM {t1} TO {}", t4 + 1),
    );
    let want = vec![
        vec![
            Some(enc(&ScalarValue::Int4(1))),
            Some(enc(&ScalarValue::Int4(100))),
            Some(enc(&ScalarValue::TimestampTz(t1))),
            Some(enc(&ScalarValue::TimestampTz(t2))),
        ],
        vec![
            Some(enc(&ScalarValue::Int4(1))),
            Some(enc(&ScalarValue::Int4(200))),
            Some(enc(&ScalarValue::TimestampTz(t2))),
            Some(enc(&ScalarValue::TimestampTz(t4))),
        ],
        vec![
            Some(enc(&ScalarValue::Int4(2))),
            Some(enc(&ScalarValue::Int4(900))),
            Some(enc(&ScalarValue::TimestampTz(t3))),
            // Still open: sys_to is NULL.
            None,
        ],
    ];
    let mut want = want;
    want.sort();
    assert_eq!(rows, want, "endpoint columns, open sys_to = NULL");
}

#[test]
fn a_named_projection_and_where_still_append_the_endpoints() {
    // `SELECT a … WHERE id = 1` over a range projects `[a, sys_from, sys_to]` and
    // returns only key 1's versions overlapping the range.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
    );
    run(&mut engine, "INSERT INTO t VALUES (1, 100)");
    let t1 = engine.commit_clock().0;
    run(&mut engine, "UPDATE t SET a = 200 WHERE id = 1");
    let t2 = engine.commit_clock().0;
    run(&mut engine, "INSERT INTO t VALUES (2, 900)"); // key 2: must be filtered out
    let t3 = engine.commit_clock().0;

    let stmt = parse(&format!(
        "SELECT a FROM t FOR SYSTEM_TIME FROM {t1} TO {} WHERE id = 1",
        t3 + 1
    ))
    .expect("parse")
    .remove(0);
    let StatementOutcome::Rows(result) = engine.execute(&stmt).expect("range select") else {
        panic!("expected rows");
    };
    let names: Vec<&str> = result.columns.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        ["a", "sys_from", "sys_to"],
        "projected col then endpoints"
    );
    let mut rows = result.rows;
    rows.sort();
    let mut want = vec![
        vec![
            Some(enc(&ScalarValue::Int4(100))),
            Some(enc(&ScalarValue::TimestampTz(t1))),
            Some(enc(&ScalarValue::TimestampTz(t2))),
        ],
        vec![
            Some(enc(&ScalarValue::Int4(200))),
            Some(enc(&ScalarValue::TimestampTz(t2))),
            None,
        ],
    ];
    want.sort();
    assert_eq!(rows, want, "only key 1's versions, endpoints appended");
}
