//! The indexed≡unindexed equivalence oracle ([STL-233]).
//!
//! The substrate's one correctness obligation is that a secondary index can
//! change *speed* but never *results* — the superset contract
//! (`stele-engine`'s `secondary` module docs). This harness pins it the
//! [testing-strategy §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)
//! way: one seeded random workload (inserts, partial updates, deletes,
//! flushes, a mid-workload restart) is applied to **two** engines — one that
//! also executes the index DDL, one that never does (the forced full scan) —
//! and to a deliberately-dumb reference model. A probe matrix of projections ×
//! predicates is then swept over live reads *and* system-time `AS OF` reads at
//! every captured commit point, asserting the three agree byte-for-byte.
//!
//! Because the indexed engine executes extra (DDL) statements, its commit
//! instants drift from the unindexed engine's — so each workload position
//! captures *both* engines' commit clocks, and an `AS OF` probe addresses each
//! engine at its own instant for the same logical prefix.
//!
//! The probe-count assertion proves the indexed engine actually served reads
//! through the index (a silently-never-usable index would pass equivalence
//! vacuously), and the teeth test
//! (`the_oracle_catches_a_reference_that_ignores_the_where`) proves the
//! differential catches a reference that drops the `WHERE`.
//!
//! Sibling index tickets ([STL-237] B-tree ranges, [STL-238] hash/bloom,
//! [STL-241] valid-time) wire in here: add their `CREATE INDEX` forms to
//! [`IndexScript`] and their predicate shapes to [`probes`] rather than
//! writing their own harness.
//!
//! [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
//! [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
//! [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
//! [STL-241]: https://allegromusic.atlassian.net/browse/STL-241

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's `MonotonicClock` turns its readings
/// into the strictly increasing `1, 2, 3, …` the writes need, deterministically.
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

/// The reference row for `t (id INT PRIMARY KEY, a INT, b INT, c TEXT)`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: i32,
    a: Option<i32>,
    b: Option<i32>,
    c: Option<String>,
}

impl Row {
    /// The cell at schema position `col` in its canonical encoding — the exact
    /// bytes the engine's `SelectResult` carries.
    fn cell(&self, col: usize) -> Option<Vec<u8>> {
        let value = match col {
            0 => Some(ScalarValue::Int4(self.id)),
            1 => self.a.map(ScalarValue::Int4),
            2 => self.b.map(ScalarValue::Int4),
            3 => self.c.clone().map(ScalarValue::Text),
            _ => unreachable!("only four columns"),
        };
        value.map(|v| {
            let mut bytes = Vec::new();
            v.encode(&mut bytes);
            bytes
        })
    }
}

/// One engine under test plus the persistent disk that lets the harness
/// "crash" it (drop + [`SessionEngine::recover`]) mid-workload.
struct Db {
    disk: MemDisk,
    engine: SessionEngine<ZeroClock, MemDisk>,
}

impl Db {
    fn fresh() -> Self {
        let disk = MemDisk::new();
        let engine = SessionEngine::open(disk.clone(), ZeroClock);
        Self { disk, engine }
    }

    fn run(&mut self, sql: &str) -> StatementOutcome {
        let stmt = parse(sql).expect("parse").remove(0);
        self.engine
            .execute(&stmt)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Option<Vec<u8>>>> {
        let StatementOutcome::Rows(SelectResult { rows, .. }) = self.run(sql) else {
            panic!("`{sql}` must return rows");
        };
        rows
    }

    /// The crash/restart boundary: drop the engine, recover from the disk.
    /// The recovered engine rebuilds any live index with a fresh floor, so
    /// pre-restart `AS OF` probes exercise the floor fallback.
    fn restart(&mut self) {
        self.engine =
            SessionEngine::recover(self.disk.clone(), ZeroClock).expect("recover survives");
    }
}

/// The index DDL the indexed engine interleaves into the workload — the seam
/// the sibling index tickets extend with their kinds.
struct IndexScript {
    /// Workload position to `CREATE INDEX i_a ON t (a)` at.
    create_a: usize,
    /// Workload position to also `CREATE INDEX i_c ON t (c)` at, if any.
    create_c: Option<usize>,
    /// Workload position to `DROP INDEX i_a` at, if any (only scheduled when
    /// `i_c` exists by then, so at least one index is live at the end).
    drop_a: Option<usize>,
}

impl IndexScript {
    fn draw(rng: &mut Rng, ops: usize) -> Self {
        let create_a = rng.index(ops / 2);
        let create_c = rng.one_in(2).then(|| ops / 2 + rng.index(ops / 2));
        let drop_a = match create_c {
            Some(c_at) if rng.one_in(3) => {
                Some(c_at + 1 + rng.index(ops.saturating_sub(c_at + 1).max(1)))
            }
            _ => None,
        };
        Self {
            create_a,
            create_c,
            drop_a,
        }
    }

    /// The DDL to run when the workload reaches position `op`.
    fn ddl_at(&self, op: usize) -> Vec<&'static str> {
        let mut out = Vec::new();
        if op == self.create_a {
            out.push("CREATE INDEX i_a ON t (a)");
        }
        if self.create_c == Some(op) {
            out.push("CREATE INDEX i_c ON t (c)");
        }
        if self.drop_a == Some(op) {
            out.push("DROP INDEX i_a");
        }
        out
    }
}

/// One captured workload position: each engine's own commit instant plus the
/// reference's live rows at that point — the unit an `AS OF` probe addresses.
struct Capture {
    indexed_at: i64,
    unindexed_at: i64,
    rows: Vec<Row>,
}

/// A swept predicate, rendered to SQL and evaluated against the reference.
#[derive(Clone, Copy)]
enum Where {
    None,
    /// `id = k` — the business-key path (its own zone-map push-down).
    Key(i32),
    /// `a = v` — the indexed-column equality the probe serves.
    AEq(i32),
    /// `b = v` — an unindexed column; never probes.
    BEq(i32),
    /// `c = s` — text equality (indexed when the seed created `i_c`).
    CEq(&'static str),
    /// `a > v` — a non-equality; the substrate's rule never probes it.
    AGt(i32),
}

impl Where {
    fn sql(self) -> String {
        match self {
            Self::None => String::new(),
            Self::Key(k) => format!(" WHERE id = {k}"),
            Self::AEq(v) => format!(" WHERE a = {v}"),
            Self::BEq(v) => format!(" WHERE b = {v}"),
            Self::CEq(s) => format!(" WHERE c = '{s}'"),
            Self::AGt(v) => format!(" WHERE a > {v}"),
        }
    }

    /// The deliberately-dumb truth: does `row` pass? (NULL never matches.)
    fn keeps(self, row: &Row) -> bool {
        match self {
            Self::None => true,
            Self::Key(k) => row.id == k,
            Self::AEq(v) => row.a == Some(v),
            Self::BEq(v) => row.b == Some(v),
            Self::CEq(s) => row.c.as_deref() == Some(s),
            Self::AGt(v) => row.a.is_some_and(|a| a > v),
        }
    }
}

/// The probe matrix: projections × predicates, deterministic so coverage does
/// not depend on the seed. Values straddle present/absent so both the
/// candidate-window and the proves-empty probe arms fire.
fn probes() -> Vec<(&'static str, Vec<usize>, Where)> {
    let projections: [(&str, Vec<usize>); 3] = [
        ("*", vec![0, 1, 2, 3]),
        ("id", vec![0]),
        ("c, a", vec![3, 1]),
    ];
    let filters = [
        Where::None,
        Where::Key(1),
        Where::AEq(1),
        Where::AEq(2),
        Where::AEq(3),
        Where::AEq(7), // never written: the Empty-probe arm
        Where::BEq(10),
        Where::CEq("x"),
        Where::CEq("absent"),
        Where::AGt(1),
    ];
    let mut out = Vec::new();
    for (proj_sql, proj) in &projections {
        for filter in filters {
            out.push((*proj_sql, proj.clone(), filter));
        }
    }
    out
}

/// The reference answer: filter, order by encoded business key (the engines'
/// scan order), project. `ignore_filter` is the teeth-test seam — the correct
/// reference passes `false`.
fn reference(
    rows: &[Row],
    projection: &[usize],
    filter: Where,
    ignore_filter: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut matched: Vec<&Row> = rows
        .iter()
        .filter(|row| ignore_filter || filter.keeps(row))
        .collect();
    matched.sort_by_key(|row| row.cell(0));
    matched
        .iter()
        .map(|row| projection.iter().map(|&c| row.cell(c)).collect())
        .collect()
}

/// Draw one committed DML statement, mirroring its effect into the reference
/// model: an insert (NULLs included), a partial update of `a` or `c`, or a
/// delete — over small value domains so equality predicates match several rows.
fn next_dml(rng: &mut Rng, model: &mut Vec<Row>, next_id: &mut i32) -> String {
    let texts = ["x", "y", "z"];
    let int_or_null = |rng: &mut Rng, domain: &[i32]| -> Option<i32> {
        if rng.one_in(5) {
            None
        } else {
            Some(domain[rng.index(domain.len())])
        }
    };
    let text_or_null = |rng: &mut Rng| -> Option<String> {
        if rng.one_in(5) {
            None
        } else {
            Some(texts[rng.index(texts.len())].to_owned())
        }
    };
    let sql_int = |v: Option<i32>| v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
    let sql_text = |v: &Option<String>| {
        v.as_ref()
            .map_or_else(|| "NULL".to_owned(), |s| format!("'{s}'"))
    };

    match rng.index(4) {
        2 if !model.is_empty() => {
            let idx = rng.index(model.len());
            let id = model[idx].id;
            if rng.one_in(2) {
                let a = int_or_null(rng, &[1, 2, 3]);
                model[idx].a = a;
                format!("UPDATE t SET a = {} WHERE id = {id}", sql_int(a))
            } else {
                let c = text_or_null(rng);
                let sql = format!("UPDATE t SET c = {} WHERE id = {id}", sql_text(&c));
                model[idx].c = c;
                sql
            }
        }
        3 if !model.is_empty() => {
            let idx = rng.index(model.len());
            let id = model.remove(idx).id;
            format!("DELETE FROM t WHERE id = {id}")
        }
        // An insert — also the fallback when nothing is live to mutate.
        _ => {
            let row = Row {
                id: *next_id,
                a: int_or_null(rng, &[1, 2, 3]),
                b: int_or_null(rng, &[10, 20]),
                c: text_or_null(rng),
            };
            *next_id += 1;
            let sql = format!(
                "INSERT INTO t VALUES ({}, {}, {}, {})",
                row.id,
                sql_int(row.a),
                sql_int(row.b),
                sql_text(&row.c),
            );
            model.push(row);
            sql
        }
    }
}

/// Fold one agreed answer into the seed digest (FNV-style).
fn fold(digest: &mut u64, rows: &[Vec<Option<Vec<u8>>>]) {
    for row in rows {
        for cell in row {
            match cell {
                None => *digest = (*digest ^ 0xFF).wrapping_mul(0x0100_0000_01B3),
                Some(bytes) => {
                    for &byte in bytes {
                        *digest = (*digest ^ u64::from(byte)).wrapping_mul(0x0100_0000_01B3);
                    }
                }
            }
        }
        *digest = digest.wrapping_add(0x9E37_79B9_7F4A_7C15);
    }
}

/// Apply the seeded workload to both engines and the model, capturing each
/// committed position; then sweep the probe matrix over the live state and
/// every captured `AS OF` point, asserting indexed ≡ unindexed ≡ reference.
fn differential(seed: u64, ignore_filter: bool) -> u64 {
    let mut rng = Rng::new(seed);
    let mut indexed = Db::fresh();
    let mut unindexed = Db::fresh();
    let mut model: Vec<Row> = Vec::new();

    const CREATE: &str =
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c TEXT) WITH SYSTEM VERSIONING";
    indexed.run(CREATE);
    unindexed.run(CREATE);

    let ops = 16 + rng.index(17); // 16..=32 workload operations
    let script = IndexScript::draw(&mut rng, ops);
    let restart_at = rng.one_in(2).then(|| rng.index(ops));

    let mut captures: Vec<Capture> = Vec::new();
    let mut next_id = 1i32;
    for op in 0..ops {
        // The only difference between the two engines: the index DDL.
        for ddl in script.ddl_at(op) {
            indexed.run(ddl);
        }
        if restart_at == Some(op) {
            // Crash both: same logical state, fresh engines; the indexed one
            // rebuilds its structures with a recovery-instant floor.
            if rng.one_in(2) {
                indexed.run("FLUSH");
                unindexed.run("FLUSH");
            }
            indexed.restart();
            unindexed.restart();
        }

        // One committed DML, mirrored into the model.
        let dml = next_dml(&mut rng, &mut model, &mut next_id);
        indexed.run(&dml);
        unindexed.run(&dml);

        // Capture this committed position: each engine's own commit instant
        // (they drift — the indexed engine ran extra DDL statements) and the
        // model's rows, for the AS OF sweep below.
        captures.push(Capture {
            indexed_at: indexed.engine.commit_clock().0,
            unindexed_at: unindexed.engine.commit_clock().0,
            rows: model.clone(),
        });
    }

    // Sweep the probe matrix over the live state and a bounded sample of
    // captured AS OF points (always including the oldest and newest).
    let mut as_of_points: Vec<usize> = vec![0, captures.len() - 1];
    for _ in 0..3 {
        as_of_points.push(rng.index(captures.len()));
    }
    as_of_points.sort_unstable();
    as_of_points.dedup();

    let mut digest: u64 = 0xCBF2_9CE4_8422_2325;

    for (proj_sql, projection, filter) in probes() {
        // Live reads: indexed ≡ unindexed ≡ reference.
        let live_sql = format!("SELECT {proj_sql} FROM t{}", filter.sql());
        let with_index = indexed.rows(&live_sql);
        let full_scan = unindexed.rows(&live_sql);
        assert_eq!(
            with_index, full_scan,
            "seed {seed}: indexed and unindexed engines diverged on `{live_sql}`"
        );
        let want = reference(&model, &projection, filter, ignore_filter);
        assert_eq!(
            with_index, want,
            "seed {seed}: divergence from the reference on `{live_sql}`"
        );
        fold(&mut digest, &with_index);

        // AS OF reads at each sampled commit point — each engine addressed at
        // its own captured instant for the same logical prefix. Pre-floor
        // instants (before CREATE INDEX / after a restart) exercise the
        // full-scan fallback; post-floor ones the probe.
        for &point in &as_of_points {
            let capture = &captures[point];
            let a_sql = format!(
                "SELECT {proj_sql} FROM t FOR SYSTEM_TIME AS OF {}{}",
                capture.indexed_at,
                filter.sql()
            );
            let b_sql = format!(
                "SELECT {proj_sql} FROM t FOR SYSTEM_TIME AS OF {}{}",
                capture.unindexed_at,
                filter.sql()
            );
            let with_index = indexed.rows(&a_sql);
            let full_scan = unindexed.rows(&b_sql);
            assert_eq!(
                with_index, full_scan,
                "seed {seed}: engines diverged at capture {point} on `{a_sql}`"
            );
            let want = reference(&capture.rows, &projection, filter, ignore_filter);
            assert_eq!(
                with_index, want,
                "seed {seed}: reference divergence at capture {point} on `{a_sql}`"
            );
            fold(&mut digest, &with_index);
        }
    }

    // The equivalence must not be vacuous: the indexed engine actually served
    // reads through the index. (The unindexed engine, by construction, never
    // can — it has no index to probe.)
    assert!(
        indexed.engine.index_probe_count() > 0,
        "seed {seed}: no read ever probed the index — the harness lost its subject"
    );
    assert_eq!(unindexed.engine.index_probe_count(), 0);

    digest
}

#[test]
fn indexed_and_unindexed_engines_agree_across_seeds() {
    for seed in 0..48 {
        let _ = differential(seed, false);
    }
}

#[test]
fn the_workload_is_reproducible_and_seed_dependent() {
    let digests: Vec<u64> = (0..8).map(|seed| differential(seed, false)).collect();
    for (seed, &digest) in digests.iter().enumerate() {
        assert_eq!(
            digest,
            differential(seed as u64, false),
            "seed {seed} must replay to an identical digest"
        );
    }
    let distinct: std::collections::HashSet<u64> = digests.into_iter().collect();
    assert!(
        distinct.len() > 1,
        "the workload must actually depend on the seed"
    );
}

#[test]
#[should_panic(expected = "divergence")]
fn the_oracle_catches_a_reference_that_ignores_the_where() {
    // The teeth test: a reference that drops the `WHERE` must be caught by the
    // very same differential check — proof the byte-for-byte comparison bites.
    differential(7, true);
}
