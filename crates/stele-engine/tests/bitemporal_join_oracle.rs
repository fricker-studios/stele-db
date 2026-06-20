//! The bitemporal AS-OF join correctness oracle ([STL-243]).
//!
//! A temporal join must read **every** input at the *same* `(sys, valid)` point —
//! one consistent snapshot across the whole query
//! ([docs/16 §8](../../../docs/16-bitemporal-semantics.md#8-temporal-joins)). This
//! is the [required correctness oracle](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)
//! for that behavior, and it is a *differential* one straight off the ticket's
//! wording: the answer of a join taken under `FOR SYSTEM_TIME AS OF s FOR
//! VALID_TIME AS OF v` is checked against **joining the inputs read individually at
//! that same `(s, v)`**.
//!
//! The reference reads are the engine's own single-table both-axes `AS OF` path —
//! itself independently oracle-backed against a naive model ([STL-163]) and DuckDB
//! ([STL-167]) — so the only thing this oracle adds on top is a *deliberately dumb*
//! nested-loop join over those trusted rows (too simple to be wrong). If the real
//! join (a hash join over a wholly different columnar scan) agrees with it at every
//! probe, the join read each side at exactly the pinned `(s, v)`.
//!
//! A seeded random both-axes history (inserts, valid-window-shifting updates, and
//! system-time deletes) is applied to two valid-time tables; then a `(s, v)` grid
//! is swept for all four join shapes (inner / left / semi / anti) **twice** — once
//! on the live delta tier, once after [`flush`](SessionEngine::flush) seals every
//! version into segments — so the consistency holds across the flush/compaction
//! boundary too. The [teeth test](#tests) reads one input at a *different* valid
//! instant — exactly the inconsistency docs/16 §8 forbids — and proves the same
//! differential catches it.
//!
//! [STL-243]: https://allegromusic.atlassian.net/browse/STL-243
//! [STL-163]: https://allegromusic.atlassian.net/browse/STL-163
//! [STL-167]: https://allegromusic.atlassian.net/browse/STL-167

use std::collections::HashSet;

use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's [`MonotonicClock`] turns its readings into
/// the strictly increasing commit instants the writes need, deterministically.
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
    /// A uniform index into `0..len` (no `as` casts, so the truncation lints stay
    /// clean).
    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("len fits u64");
        usize::try_from(self.next() % len).expect("index fits usize")
    }
    /// True with probability `1/n`.
    const fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }
}

/// One reconstructed cell as the canonical bytes the engine's `SelectResult`
/// carries (`None` is a SQL `NULL`).
type Cell = Option<Vec<u8>>;
/// A reconstructed row.
type Row = Vec<Cell>;

/// The four left-driven join shapes the engine binds ([STL-172]): the SQL spelling,
/// a tag, and whether the right side's columns are projected (`SEMI` / `ANTI` are
/// left-only).
const JOINS: [(&str, JoinKind, bool); 4] = [
    ("JOIN", JoinKind::Inner, true),
    ("LEFT JOIN", JoinKind::Left, true),
    ("SEMI JOIN", JoinKind::Semi, false),
    ("ANTI JOIN", JoinKind::Anti, false),
];

#[derive(Clone, Copy)]
enum JoinKind {
    Inner,
    Left,
    Semi,
    Anti,
}

/// The valid-axis probe instants — spanning every window boundary the workload can
/// produce (`window` below draws starts `{0,10,20}` and ends up to `40`).
const VALID_GRID: [i64; 9] = [0, 5, 10, 15, 20, 25, 30, 35, 40];

/// Both tables share this shape: a business key, one value column, and the
/// valid-time period columns. The shared key domain (1..=`KEYS`) makes the join
/// match, and the partial per-table presence makes the unmatched-row shapes (left /
/// semi / anti) meaningful.
const KEYS: i32 = 4;

/// Run a `SELECT` against the engine and return its rows.
fn rows(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> Vec<Row> {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(SelectResult { rows, .. }) = engine.execute(&stmt).expect("select")
    else {
        panic!("SELECT must return rows for `{sql}`");
    };
    rows
}

/// Read one side individually at `(s, v)` — the trusted both-axes single-table `AS
/// OF` path ([STL-163] / [STL-167]) the differential references.
fn read_side(
    engine: &mut SessionEngine<ZeroClock, MemDisk>,
    table: &str,
    s: i64,
    v: i64,
) -> Vec<Row> {
    rows(
        engine,
        &format!("SELECT k, val FROM {table} FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}"),
    )
}

/// The deliberately-dumb reference join: a nested loop over the individually-read
/// rows (`[k, val]` each). A `NULL` key never matches (SQL 3VL); the key here is a
/// primary key, so this only guards the model. Inner emits each matching pair; left
/// keeps an unmatched left row `NULL`-extended; semi/anti keep the left row alone.
fn reference_join(kind: JoinKind, left: &[Row], right: &[Row]) -> Vec<Row> {
    let matches = |l: &Row, r: &Row| l[0].is_some() && l[0] == r[0];
    let mut out = Vec::new();
    for l in left {
        let mut matched = right.iter().filter(|r| matches(l, r)).peekable();
        match kind {
            JoinKind::Inner => {
                for r in matched {
                    out.push(vec![l[0].clone(), l[1].clone(), r[0].clone(), r[1].clone()]);
                }
            }
            JoinKind::Left => {
                if matched.peek().is_none() {
                    out.push(vec![l[0].clone(), l[1].clone(), None, None]);
                } else {
                    for r in matched {
                        out.push(vec![l[0].clone(), l[1].clone(), r[0].clone(), r[1].clone()]);
                    }
                }
            }
            JoinKind::Semi => {
                if matched.peek().is_some() {
                    out.push(vec![l[0].clone(), l[1].clone()]);
                }
            }
            JoinKind::Anti => {
                if matched.peek().is_none() {
                    out.push(vec![l[0].clone(), l[1].clone()]);
                }
            }
        }
    }
    out
}

/// Sort rows so the engine's join order (unspecified) is compared as a multiset.
fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

/// A seeded random valid-time window `[from, to)` (`from < to`), drawn from the
/// small domain `VALID_GRID` spans.
fn window(rng: &mut Rng) -> (i64, i64) {
    let starts = [0, 10, 20];
    let lens = [10, 15, 20];
    let from = starts[rng.index(starts.len())];
    (from, from + lens[rng.index(lens.len())])
}

/// Apply a seeded random both-axes history to two fresh valid-time tables and
/// return the engine plus the sorted, de-duplicated set of commit instants to
/// sweep the system axis over (each a real committed point, so no before-history
/// read). The tables are built in order, so every captured instant is at or after
/// the earliest table's first commit.
fn build(seed: u64) -> (SessionEngine<ZeroClock, MemDisk>, Vec<i64>) {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    for table in ["a", "b"] {
        let ddl = format!(
            "CREATE TABLE {table} (k INT PRIMARY KEY, val INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)"
        );
        engine
            .execute(&parse(&ddl).expect("parse").remove(0))
            .expect("create");
    }

    let mut instants = Vec::new();
    let commit = |engine: &SessionEngine<ZeroClock, MemDisk>, instants: &mut Vec<i64>| {
        instants.push(engine.commit_clock().0);
    };
    for table in ["a", "b"] {
        for key in 1..=KEYS {
            if rng.one_in(3) {
                continue; // not every key in every table — drives the unmatched shapes
            }
            let (vf, vt) = window(&mut rng);
            let val = 1 + i32::try_from(rng.index(3)).expect("fits");
            let sql = format!("INSERT INTO {table} VALUES ({key}, {val}, {vf}, {vt})");
            engine
                .execute(&parse(&sql).expect("parse").remove(0))
                .expect("insert");
            commit(&engine, &mut instants);

            if rng.one_in(2) {
                // A valid-window-shifting update: supersedes the prior version on the
                // system axis and opens a new valid window — both axes now load-bearing.
                let (vf2, vt2) = window(&mut rng);
                let val2 = 1 + i32::try_from(rng.index(3)).expect("fits");
                let sql = format!(
                    "UPDATE {table} SET val = {val2}, vf = {vf2}, vt = {vt2} WHERE k = {key}"
                );
                engine
                    .execute(&parse(&sql).expect("parse").remove(0))
                    .expect("update");
                commit(&engine, &mut instants);
            }
            if rng.one_in(4) {
                // A delete closes the system period (no valid interval) — the key is
                // system-gone from here on, visible only at earlier snapshots.
                let sql = format!("DELETE FROM {table} WHERE k = {key}");
                engine
                    .execute(&parse(&sql).expect("parse").remove(0))
                    .expect("delete");
                commit(&engine, &mut instants);
            }
        }
    }
    instants.sort_unstable();
    instants.dedup();
    (engine, instants)
}

/// Sweep the `(s, v)` grid for all four join shapes, asserting the real join equals
/// the reference join of the individually-read inputs, and fold a digest of the
/// agreed answers. `valid_skew` shifts *only side b's* reference read on the valid
/// axis — `0` is the consistent (honest) check; a non-zero value is the teeth.
fn differential(
    engine: &mut SessionEngine<ZeroClock, MemDisk>,
    instants: &[i64],
    valid_skew: i64,
) -> u64 {
    let mut digest: u64 = 0xCBF2_9CE4_8422_2325;
    for &s in instants {
        for &v in &VALID_GRID {
            // The reference reads are independent of the join shape, so read each
            // side once per `(s, v)` and reuse it across all four joins.
            let left = read_side(engine, "a", s, v);
            let right = read_side(engine, "b", s, v + valid_skew);
            for &(op, kind, keeps_right) in &JOINS {
                let proj = if keeps_right {
                    "a.k, a.val, b.k, b.val"
                } else {
                    "a.k, a.val"
                };
                let sql = format!(
                    "SELECT {proj} FROM a {op} b ON a.k = b.k \
                     FOR SYSTEM_TIME AS OF {s} FOR VALID_TIME AS OF {v}"
                );
                let got = sorted(rows(engine, &sql));
                let want = sorted(reference_join(kind, &left, &right));
                assert_eq!(got, want, "divergence on `{sql}` (valid_skew {valid_skew})");
                for row in &got {
                    for cell in row {
                        match cell {
                            None => digest = (digest ^ 0xFF).wrapping_mul(0x0100_0000_01B3),
                            Some(bytes) => {
                                for &byte in bytes {
                                    digest =
                                        (digest ^ u64::from(byte)).wrapping_mul(0x0100_0000_01B3);
                                }
                            }
                        }
                    }
                    digest = digest.wrapping_add(0x9E37_79B9_7F4A_7C15);
                }
            }
        }
    }
    digest
}

#[test]
fn join_under_both_axes_as_of_matches_individually_read_inputs() {
    // Each seed asserts (internally, at every probe) that the join under both-axes
    // `AS OF` equals joining the inputs read individually at that same `(s, v)` —
    // on the live delta tier, then again after a flush seals every version, so the
    // consistency holds across the flush/compaction boundary.
    for seed in 0..64 {
        let (mut engine, instants) = build(seed);
        let _ = differential(&mut engine, &instants, 0);
        engine.flush().expect("seal every tier");
        let _ = differential(&mut engine, &instants, 0);
    }
}

#[test]
fn the_workload_is_reproducible_and_seed_dependent() {
    let digests: Vec<u64> = (0..64)
        .map(|seed| {
            let (mut engine, instants) = build(seed);
            differential(&mut engine, &instants, 0)
        })
        .collect();
    for (seed, &digest) in digests.iter().enumerate() {
        let (mut engine, instants) = build(seed as u64);
        assert_eq!(
            digest,
            differential(&mut engine, &instants, 0),
            "seed {seed} must replay to an identical digest"
        );
    }
    let distinct: HashSet<u64> = digests.into_iter().collect();
    assert!(
        distinct.len() > 1,
        "the workload must actually depend on the seed"
    );
}

/// A deterministic fixture where side `b`'s visible value column flips across a
/// valid-window boundary, so reading it at a *different* valid instant changes the
/// join — the seam the teeth test pulls on.
fn skew_fixture() -> (SessionEngine<ZeroClock, MemDisk>, Vec<i64>) {
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    for table in ["a", "b"] {
        let ddl = format!(
            "CREATE TABLE {table} (k INT PRIMARY KEY, val INT, vf TIMESTAMP, vt TIMESTAMP) \
             WITH SYSTEM VERSIONING VALID TIME (vf, vt)"
        );
        engine
            .execute(&parse(&ddl).expect("parse").remove(0))
            .expect("create");
    }
    let exec = |engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str| {
        engine
            .execute(&parse(sql).expect("parse").remove(0))
            .expect("dml");
    };
    // a: key 1 live across the whole window.
    exec(&mut engine, "INSERT INTO a VALUES (1, 7, 0, 40)");
    // b: key 1 starts in [0, 20), then an update moves it to [20, 40) — so at the
    // latest system snapshot b key 1 is visible only at valid >= 20.
    exec(&mut engine, "INSERT INTO b VALUES (1, 100, 0, 20)");
    exec(
        &mut engine,
        "UPDATE b SET val = 200, vf = 20, vt = 40 WHERE k = 1",
    );
    let instants = vec![engine.commit_clock().0];
    (engine, instants)
}

#[test]
#[should_panic(expected = "divergence")]
fn reading_an_input_at_a_different_valid_instant_diverges() {
    // The teeth: a consistent join reads *both* inputs at one `(s, v)`. Reading
    // side b 15µs later on the valid axis is exactly the cross-input inconsistency
    // docs/16 §8 forbids — at v=10 the real join sees an empty b (its row lives at
    // valid >= 20), but the skewed reference reads b at v=25 and finds it, so the
    // very same differential diverges.
    let (mut engine, instants) = skew_fixture();
    // Sanity: the honest check passes on this fixture.
    let _ = differential(&mut engine, &instants, 0);
    differential(&mut engine, &instants, 15);
}
