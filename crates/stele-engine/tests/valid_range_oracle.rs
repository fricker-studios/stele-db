//! The valid-time range-scan correctness oracle ([STL-328], [docs/06 §4]).
//!
//! `SELECT … FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` returns every
//! version **system-live at the statement snapshot** whose valid interval
//! `[valid_from, valid_to)` overlaps the range — the valid-axis mirror of the
//! STL-244 system range. Getting the *set* right at the half-open / closed
//! boundaries (the §4 "off-by-one on a half-open interval" bug class) — over a
//! genuinely **bitemporal** workload where corrections supersede on the system
//! axis and move the valid window — is the temporal heart of the feature, so
//! every range answer the real [`SessionEngine`] gives is checked against a
//! deliberately-dumb in-process reference.
//!
//! The reference tracks the full `(system, valid)` version timeline as a plain
//! `Vec` of records (each write opens a version and closes the prior one),
//! learning each write's commit instant from [`SessionEngine::commit_clock`] — the
//! same alignment the bitemporal SQL oracle uses ([STL-167]). It decides system
//! liveness and valid overlap by the dumbest correct formulation — two inclusive
//! integer-instant ranges either intersect or they don't — an independent
//! derivation of the semantics, not a copy of the engine's predicate. The
//! [teeth test](#tests) injects the classic off-by-one (a `<=` where a `<`
//! belongs) and proves the differential catches it.
//!
//! Both axes are swept: the system snapshot across every commit boundary (so the
//! system-live set the valid filter sees changes — supersession, deletion gaps,
//! re-insertion), the valid range across every window boundary. The same workload
//! is replayed with the delta flushed at three points (never, midway, fully), so
//! the answer is asserted identical across the delta/sealed boundary — history a
//! range scan must reconstruct the same way whether a version is staged (interval
//! framed on the payload) or sealed (interval in `valid_from` / `valid_to`
//! columns).
//!
//! [docs/06 §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart
//! [STL-167]: https://allegromusic.atlassian.net/browse/STL-167
//! [STL-328]: https://allegromusic.atlassian.net/browse/STL-328

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's `MonotonicClock` turns its readings into
/// the strictly increasing `1, 2, 3, …` the writes need, deterministically — and
/// with this zero inner clock a *read* never advances the mark, so
/// [`SessionEngine::commit_clock`] read right after a write is that write's commit
/// instant.
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
    fn below(&mut self, n: i64) -> i64 {
        let n = u64::try_from(n).expect("positive bound");
        i64::try_from(self.next() % n).expect("fits")
    }
}

/// One version in the reference timeline: key `id`, value `balance`, the system
/// period `[sys_from, sys_to)` (`sys_to == None` while open) and the valid period
/// `[vf, vt)` (`vt == i64::MAX` for an open-ended / `+∞` fact).
#[derive(Debug, Clone)]
struct Ver {
    id: i32,
    balance: i32,
    sys_from: i64,
    sys_to: Option<i64>,
    vf: i64,
    vt: i64,
}

/// Encode a `ScalarValue` to its canonical wire bytes — the exact form a
/// `SelectResult` cell carries.
fn enc(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// When to flush the delta tier into sealed segments.
#[derive(Debug, Clone, Copy)]
enum FlushMode {
    Never,
    Midway,
    Full,
}

/// The reference timeline and the engine after applying a seeded workload.
struct World {
    engine: SessionEngine<ZeroClock, MemDisk>,
    versions: Vec<Ver>,
    /// The table's first-commit instant (the `CREATE`); a `FOR SYSTEM_TIME AS OF`
    /// before it is a before-history error, so the sweep never probes below it.
    created: i64,
}

const KEY_POOL: i64 = 3;
const VMAX: i64 = 9;

/// Apply a seeded INSERT / UPDATE / DELETE history to a **valid-time** table —
/// each write carrying a (sometimes open-ended) valid window — recording every
/// version and its commit instant in the reference timeline.
fn build(seed: u64, flush: FlushMode) -> World {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    );
    let created = engine.commit_clock().0;

    let mut versions: Vec<Ver> = Vec::new();
    // The index of each key's currently-open version, when it is live.
    let mut open: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();

    let op_count = 12 + rng.below(13); // 12..=24 writes
    let flush_at = op_count / 2;
    for step in 0..op_count {
        let id = i32::try_from(rng.below(KEY_POOL)).expect("fits");
        let balance = i32::try_from(step + 1).expect("fits");
        let live = open.contains_key(&id);
        // A well-formed valid window inside `[0, VMAX]`, sometimes open-ended to
        // exercise the `+∞` valid_to (and its NULL endpoint rendering).
        let vf = rng.below(VMAX);
        let opened = rng.below(4) == 0;
        let vt = if opened {
            i64::MAX
        } else {
            vf + 1 + rng.below(VMAX - vf)
        };

        if live && rng.below(3) == 0 {
            run(&mut engine, &format!("DELETE FROM acct WHERE id = {id}"));
            let t = engine.commit_clock().0;
            let idx = open.remove(&id).expect("live");
            versions[idx].sys_to = Some(t);
        } else if live {
            let set = if opened {
                format!("SET balance = {balance}, vf = {vf}")
            } else {
                format!("SET balance = {balance}, vf = {vf}, vt = {vt}")
            };
            run(&mut engine, &format!("UPDATE acct {set} WHERE id = {id}"));
            let t = engine.commit_clock().0;
            let idx = open.remove(&id).expect("live");
            versions[idx].sys_to = Some(t);
            versions.push(Ver {
                id,
                balance,
                sys_from: t,
                sys_to: None,
                vf,
                vt,
            });
            open.insert(id, versions.len() - 1);
        } else {
            let stmt = if opened {
                format!("INSERT INTO acct (id, balance, vf) VALUES ({id}, {balance}, {vf})")
            } else {
                format!("INSERT INTO acct VALUES ({id}, {balance}, {vf}, {vt})")
            };
            run(&mut engine, &stmt);
            let t = engine.commit_clock().0;
            versions.push(Ver {
                id,
                balance,
                sys_from: t,
                sys_to: None,
                vf,
                vt,
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

    World {
        engine,
        versions,
        created,
    }
}

fn run(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    let stmt = parse(sql).expect("parse").remove(0);
    engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

/// The reference's expected rows for a valid range query at system snapshot `s`:
/// every version system-live at `s` whose valid interval `[vf, vt)` overlaps the
/// query window, projected as `[id, balance, valid_from, valid_to]` — the shape of
/// the engine's `SELECT id, balance, valid_from, valid_to` range read (the
/// endpoints named explicitly, [STL-329]). `closed` selects `BETWEEN [lo, hi]` over
/// `FROM..TO [lo, hi)`; `inclusive_vto` (the teeth variant) wrongly treats the
/// version's own upper as inclusive.
///
/// Overlap is decided by intersecting two inclusive integer ranges — the version's
/// `[vf, last_active]` and the query's `[lo, query_hi]` — an independent derivation
/// of the half-open semantics, not the engine's predicate.
fn reference_rows(
    versions: &[Ver],
    s: i64,
    lo: i64,
    hi: i64,
    closed: bool,
    inclusive_vto: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let query_hi = if closed { hi } else { hi - 1 };
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = versions
        .iter()
        .filter(|v| {
            let sys_to = v.sys_to.unwrap_or(i64::MAX);
            let sys_live = v.sys_from <= s && s < sys_to;
            // The version's last active valid instant: `vt - 1`, or `+∞` (open).
            let last_active = if inclusive_vto {
                v.vt
            } else {
                v.vt.saturating_sub(1)
            };
            let valid_overlap =
                v.vf <= last_active && lo <= query_hi && v.vf.max(lo) <= last_active.min(query_hi);
            sys_live && valid_overlap
        })
        .map(|v| {
            vec![
                Some(enc(&ScalarValue::Int4(v.id))),
                Some(enc(&ScalarValue::Int4(v.balance))),
                Some(enc(&ScalarValue::TimestampTz(v.vf))),
                // An open-ended (`+∞`) valid_to renders as NULL, like an open sys_to.
                (v.vt != i64::MAX).then(|| enc(&ScalarValue::TimestampTz(v.vt))),
            ]
        })
        .collect();
    rows.sort();
    rows
}

/// The engine's rows for a `SELECT id, balance, valid_from, valid_to` valid range,
/// sorted to compare as a set.
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

/// Execute a `SELECT`, returning its full result (columns + rows) — for the
/// composed-clause tests that check the output *shape*, not just the row set.
fn run_rows(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> SelectResult {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(result) = engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"))
    else {
        panic!("a SELECT must return rows for `{sql}`");
    };
    result
}

/// Decode a non-NULL `int4` cell.
fn dec_i32(cell: Option<&[u8]>) -> i32 {
    let bytes = cell.expect("non-NULL int4 cell");
    i32::from_le_bytes(bytes.try_into().expect("int4 is 4 bytes"))
}

/// Decode a non-NULL `int8` cell.
fn dec_i64(cell: Option<&[u8]>) -> i64 {
    let bytes = cell.expect("non-NULL int8 cell");
    i64::from_le_bytes(bytes.try_into().expect("int8 is 8 bytes"))
}

/// The distinct system commit boundaries worth probing as the snapshot `s`: every
/// `sys_from` / `sys_to`, each also `±1`, so the system-live set the valid filter
/// sees is probed exactly on, just before, and just after every supersession.
fn system_instants(versions: &[Ver]) -> Vec<i64> {
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
fn valid_range_scans_match_the_reference_across_seeds_axes_flush_and_boundaries() {
    let mut total_probes: u64 = 0;
    let mut rows_seen: u64 = 0;

    for seed in 0..16u64 {
        for flush in [FlushMode::Never, FlushMode::Midway, FlushMode::Full] {
            let World {
                mut engine,
                versions,
                created,
            } = build(seed, flush);
            // The valid sweep is the new boundary class, so it stays dense; the
            // system-live resolution is already exhaustively oracled at the point
            // (STL-194), so a strided sample of system snapshots is enough to confirm
            // the system-live set feeds the valid filter — and keeps the per-PR gate
            // quick.
            for (i, s) in system_instants(&versions).into_iter().enumerate() {
                // A snapshot before the table existed is a before-history error, not
                // a range read — the point path rejects it, so the sweep skips it.
                if s < created || i % 2 == 1 {
                    continue;
                }
                // Valid-window boundaries (non-negative — a folded range bound is a
                // bare microsecond literal), clamped to a tidy probe window.
                for lo in 0..=(VMAX + 1) {
                    for hi in lo..=(VMAX + 1) {
                        if lo < hi {
                            let sql = format!(
                                "SELECT id, balance, valid_from, valid_to FROM acct \
                                 FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME FROM {lo} TO {hi}"
                            );
                            let got = engine_rows(&mut engine, &sql);
                            assert_eq!(
                                got,
                                reference_rows(&versions, s, lo, hi, false, false),
                                "seed {seed}, {flush:?}: {sql}"
                            );
                            rows_seen += u64::try_from(got.len()).expect("fits");
                            total_probes += 1;
                        }
                        let sql = format!(
                            "SELECT id, balance, valid_from, valid_to FROM acct \
                             FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME BETWEEN {lo} AND {hi}"
                        );
                        assert_eq!(
                            engine_rows(&mut engine, &sql),
                            reference_rows(&versions, s, lo, hi, true, false),
                            "seed {seed}, {flush:?}: {sql}"
                        );
                        total_probes += 1;
                    }
                }
            }
        }
    }

    assert!(
        rows_seen > 0,
        "every probe was empty — the workload resolved nothing"
    );
    assert!(
        total_probes > 15_000,
        "differential probed only {total_probes} (s, lo, hi) cells — widen the sweep"
    );
}

#[test]
fn the_oracle_has_teeth_off_by_one_on_the_half_open_valid_upper() {
    // A reference that treats a version's `[vf, vt)` as `[vf, vt]` (the classic
    // off-by-one — a `<=` where the half-open form needs `<`) must be caught by the
    // very same differential the test above runs.
    let World {
        mut engine,
        versions,
        created,
    } = build(5, FlushMode::Midway);
    let mut mismatch = false;
    for s in system_instants(&versions) {
        if s < created {
            continue;
        }
        for lo in 0..=VMAX {
            for hi in (lo + 1)..=(VMAX + 1) {
                let sql = format!(
                    "SELECT id, balance, valid_from, valid_to FROM acct \
                     FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME FROM {lo} TO {hi}"
                );
                let engine = engine_rows(&mut engine, &sql);
                let buggy = reference_rows(&versions, s, lo, hi, false, true);
                if engine != buggy {
                    mismatch = true;
                }
            }
        }
    }
    assert!(
        mismatch,
        "an inclusive-vto reference must disagree with the engine somewhere"
    );
}

#[test]
fn select_star_shape_and_open_valid_to_renders_null() {
    // A focused, hand-checked timeline. key 1: a closed valid window; key 2: an
    // open-ended one (`vt` omitted → `+∞`). A whole-timeline valid range returns
    // both, with `SELECT *` exposing the user's `vf`/`vt` value columns *and* the
    // appended `valid_from`/`valid_to` endpoints — and an open `valid_to` as NULL.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    );
    run(&mut engine, "INSERT INTO acct VALUES (1, 100, 10, 20)");
    run(
        &mut engine,
        "INSERT INTO acct (id, balance, vf) VALUES (2, 200, 5)",
    );

    let stmt = parse("SELECT * FROM acct FOR VALID_TIME FROM 0 TO 100")
        .expect("parse")
        .remove(0);
    let StatementOutcome::Rows(result) = engine.execute(&stmt).expect("range select") else {
        panic!("expected rows");
    };
    let names: Vec<&str> = result.columns.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        ["id", "balance", "vf", "vt", "valid_from", "valid_to"],
        "user period columns, then the appended canonical endpoints",
    );

    // Locate each key's row by its id cell and check the two endpoint columns.
    let id_cell = |id: i32| Some(enc(&ScalarValue::Int4(id)));
    let row1 = result
        .rows
        .iter()
        .find(|r| r[0] == id_cell(1))
        .expect("key 1");
    assert_eq!(
        row1[4],
        Some(enc(&ScalarValue::TimestampTz(10))),
        "valid_from = 10"
    );
    assert_eq!(
        row1[5],
        Some(enc(&ScalarValue::TimestampTz(20))),
        "closed valid_to = 20"
    );
    let row2 = result
        .rows
        .iter()
        .find(|r| r[0] == id_cell(2))
        .expect("key 2");
    assert_eq!(
        row2[4],
        Some(enc(&ScalarValue::TimestampTz(5))),
        "valid_from = 5"
    );
    assert_eq!(row2[5], None, "an open-ended valid_to renders as NULL");
}

#[test]
fn a_named_projection_returns_its_columns_with_endpoints_nameable() {
    // The endpoints are addressable columns now ([STL-329]): a named projection over
    // a valid range returns exactly what it lists, and `valid_from` / `valid_to` are
    // projectable by name.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    );
    run(&mut engine, "INSERT INTO acct VALUES (1, 100, 10, 20)");
    run(&mut engine, "INSERT INTO acct VALUES (2, 900, 10, 20)"); // filtered out

    // `SELECT balance … WHERE id = 1` projects exactly `[balance]`.
    let bare = run_rows(
        &mut engine,
        "SELECT balance FROM acct FOR VALID_TIME FROM 0 TO 100 WHERE id = 1",
    );
    assert_eq!(
        bare.columns
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>(),
        ["balance"],
        "a named projection returns exactly its columns",
    );
    assert_eq!(bare.rows, vec![vec![Some(enc(&ScalarValue::Int4(100)))]]);

    // Naming the endpoints projects them.
    let named = run_rows(
        &mut engine,
        "SELECT balance, valid_from, valid_to FROM acct FOR VALID_TIME FROM 0 TO 100 WHERE id = 1",
    );
    assert_eq!(
        named
            .columns
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>(),
        ["balance", "valid_from", "valid_to"],
        "endpoints projectable by name",
    );
    assert_eq!(
        named.rows,
        vec![vec![
            Some(enc(&ScalarValue::Int4(100))),
            Some(enc(&ScalarValue::TimestampTz(10))),
            Some(enc(&ScalarValue::TimestampTz(20))),
        ]],
        "only key 1's row, named endpoints",
    );
}

#[test]
fn shaping_aggregates_and_provenance_compose_over_a_valid_range() {
    // The rest of the SELECT surface composes over a valid range ([STL-329]):
    // result-shaping (incl. ordering on `valid_from`), aggregation, and the
    // provenance pseudo-columns.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    );
    run(&mut engine, "INSERT INTO acct VALUES (1, 100, 10, 20)");
    run(&mut engine, "INSERT INTO acct VALUES (2, 200, 30, 40)");
    run(&mut engine, "INSERT INTO acct VALUES (3, 300, 50, 60)");

    // ORDER BY valid_from DESC + LIMIT 2 returns the two latest-starting windows.
    let shaped = run_rows(
        &mut engine,
        "SELECT balance FROM acct FOR VALID_TIME FROM 0 TO 100 ORDER BY valid_from DESC LIMIT 2",
    );
    let balances: Vec<i32> = shaped
        .rows
        .iter()
        .map(|r| dec_i32(r[0].as_deref()))
        .collect();
    assert_eq!(balances, [300, 200], "ORDER BY valid_from DESC, LIMIT 2");

    // COUNT(*) folds the whole valid range — all three windows overlap [0, 100).
    let count = run_rows(
        &mut engine,
        "SELECT count(*) FROM acct FOR VALID_TIME FROM 0 TO 100",
    );
    assert_eq!(
        dec_i64(count.rows[0][0].as_deref()),
        3,
        "three overlapping windows"
    );

    // Provenance projects from a valid range and matches a point read of the same
    // version (read at `now` on the system axis, the version's own valid start).
    let prov = run_rows(
        &mut engine,
        "SELECT id, _stele_txn_id, _stele_committed_at \
         FROM acct FOR VALID_TIME FROM 0 TO 100",
    );
    assert_eq!(prov.rows.len(), 3);
    for row in &prov.rows {
        let id = dec_i32(row[0].as_deref());
        let point = run_rows(
            &mut engine,
            &format!("SELECT _stele_txn_id, _stele_committed_at FROM acct WHERE id = {id}"),
        );
        assert_eq!(point.rows.len(), 1, "one live row per key at now");
        assert!(
            row[1].is_some() && row[2].is_some(),
            "provenance is never NULL"
        );
        assert_eq!(row[1], point.rows[0][0], "txn_id matches the point read");
        assert_eq!(
            row[2], point.rows[0][1],
            "committed_at matches the point read"
        );
    }
}

#[test]
fn no_system_qualifier_reads_the_current_valid_history() {
    // Without a `FOR SYSTEM_TIME` qualifier the system axis is `now`: a correction
    // that superseded the original window is what a valid range sees. key 1 valid
    // [0,10) then corrected to [20,30); at `now`, a range over [0,10) finds nothing
    // (that assertion was retracted on the system axis), [20,30) finds the current.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    );
    run(&mut engine, "INSERT INTO acct VALUES (1, 100, 0, 10)");
    run(
        &mut engine,
        "UPDATE acct SET balance = 200, vf = 20, vt = 30 WHERE id = 1",
    );

    let ids = |sql: &str, engine: &mut SessionEngine<ZeroClock, MemDisk>| -> Vec<Vec<u8>> {
        engine_rows(engine, sql)
            .into_iter()
            .map(|r| r[0].clone().expect("id"))
            .collect()
    };
    assert!(
        ids(
            "SELECT id, balance FROM acct FOR VALID_TIME FROM 0 TO 10",
            &mut engine
        )
        .is_empty(),
        "the original valid window was superseded on the system axis",
    );
    assert_eq!(
        ids(
            "SELECT id, balance FROM acct FOR VALID_TIME FROM 20 TO 30",
            &mut engine
        ),
        vec![enc(&ScalarValue::Int4(1))],
        "the current valid window is found at now",
    );
}
