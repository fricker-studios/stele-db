//! The projection + predicate correctness oracle ([STL-151]).
//!
//! Honoring the `SELECT` projection list and the `WHERE` predicate is
//! *data-correctness*, not a temporal property — a dropped filter or a mis-sliced
//! row returns the wrong answer, silently. So, in the spirit of the temporal
//! oracle ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)),
//! every projected/filtered answer the real [`SessionEngine`] gives is checked
//! against a deliberately-dumb in-process reference: a `Vec<Row>` that filters
//! with a plain `==` and projects by indexing. It is far too simple to be wrong,
//! which is the point — an independent check on the engine's row codec, scan
//! filter, and projection lowering.
//!
//! A seeded random multi-column history (inserts + partial updates, NULLs
//! included) is applied to both; then a fixed matrix of `(projection, WHERE col =
//! value)` probes is swept and the engine's rows are asserted byte-for-byte equal
//! to the reference's. The [teeth test](#tests) injects a *documented intentional
//! bug* (a reference that ignores the `WHERE`) and proves the very same
//! differential check catches it.
//!
//! [STL-151]: https://allegromusic.atlassian.net/browse/STL-151

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's [`MonotonicClock`] turns its readings into
/// the strictly increasing `1, 2, 3, …` the writes need, deterministically.
#[derive(Debug, Clone, Copy)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

/// A deterministic `splitmix64` so a seed replays an identical workload — no
/// dependency on the sim crate (this oracle drives the SQL path, not storage).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// A uniform index into `0..len` (no `as` casts, so the pedantic
    /// truncation lints stay clean).
    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("len fits u64");
        usize::try_from(self.next() % len).expect("index fits usize")
    }
    /// True with probability `1/n`.
    fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }
}

/// The reference row: the table is `t (id INT PRIMARY KEY, a INT, b INT, c TEXT)`,
/// so a row is its key plus three nullable value columns.
#[derive(Debug, Clone)]
struct Row {
    id: i32,
    a: Option<i32>,
    b: Option<i32>,
    c: Option<String>,
}

/// The four columns, by the index the binder/engine assign (0 = business key).
const COLUMNS: [(&str, LogicalType); 4] = [
    ("id", LogicalType::Int4),
    ("a", LogicalType::Int4),
    ("b", LogicalType::Int4),
    ("c", LogicalType::Text),
];

impl Row {
    /// The cell at column index `col` as its canonical encoding, or `None` for a
    /// SQL `NULL` — the exact bytes the engine's `SelectResult` carries.
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

/// One `WHERE col = value` predicate, or `None` for an unfiltered read.
type Filter = Option<(usize, ScalarValue)>;

/// The reference answer: filter by `==` on the cell's encoding (a NULL cell never
/// matches), order by the encoded business key (the engine's `BTreeMap` order),
/// then project the requested columns. `ignore_filter` is the seam for the teeth
/// test — the *correct* reference passes `false`.
fn reference(
    rows: &[Row],
    projection: &[usize],
    filter: &Filter,
    ignore_filter: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut matched: Vec<&Row> = rows
        .iter()
        .filter(|row| {
            let (Some((col, value)), false) = (filter, ignore_filter) else {
                return true;
            };
            let mut want = Vec::new();
            value.encode(&mut want);
            row.cell(*col).as_deref() == Some(want.as_slice())
        })
        .collect();
    matched.sort_by_key(|row| row.cell(0));
    matched
        .iter()
        .map(|row| projection.iter().map(|&c| row.cell(c)).collect())
        .collect()
}

/// Render `WHERE col = value` (or empty) as SQL — the comparand literal in the
/// column's lexical form.
fn where_sql(filter: &Filter) -> String {
    match filter {
        None => String::new(),
        Some((col, value)) => {
            let literal = match value {
                ScalarValue::Int4(v) => v.to_string(),
                ScalarValue::Text(s) => format!("'{s}'"),
                _ => unreachable!("the oracle only filters int4 / text columns"),
            };
            format!(" WHERE {} = {literal}", COLUMNS[*col].0)
        }
    }
}

/// The projection list as SQL: `*` for all columns, else the names in order.
fn projection_sql(projection: &[usize], all: bool) -> String {
    if all {
        "*".to_owned()
    } else {
        projection
            .iter()
            .map(|&c| COLUMNS[c].0)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Run a `SELECT` against the engine and return its rows.
fn engine_rows(
    engine: &mut SessionEngine<ZeroClock, MemDisk>,
    sql: &str,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(SelectResult { rows, .. }) = engine.execute(&stmt).expect("select")
    else {
        panic!("SELECT must return rows for `{sql}`");
    };
    rows
}

/// Build a seeded random table, applying it to a fresh engine and the reference.
fn build(seed: u64) -> (SessionEngine<ZeroClock, MemDisk>, Vec<Row>) {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    engine
        .execute(
            &parse(
                "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c TEXT) WITH SYSTEM VERSIONING",
            )
            .expect("parse")
            .remove(0),
        )
        .expect("create");

    // Small value domains so equality predicates match several rows; an
    // occasional NULL exercises the codec's nullable cells.
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

    let row_count = 5 + rng.index(11); // 5..=15 rows, unique ids 1..=row_count
    let mut rows: Vec<Row> = Vec::new();
    for id in 1..=row_count {
        let id = i32::try_from(id).expect("row id fits i32");
        let row = Row {
            id,
            a: int_or_null(&mut rng, &[1, 2, 3]),
            b: int_or_null(&mut rng, &[10, 20]),
            c: text_or_null(&mut rng),
        };
        engine
            .execute(
                &parse(&format!(
                    "INSERT INTO t VALUES ({}, {}, {}, {})",
                    row.id,
                    sql_int(row.a),
                    sql_int(row.b),
                    sql_text(&row.c),
                ))
                .expect("parse")
                .remove(0),
            )
            .expect("insert");
        rows.push(row);
    }

    // A few partial updates exercise the read-modify-write merge: each rewrites
    // only its named columns, so the others must survive in both engine and model.
    let updates = rng.index(5);
    for _ in 0..updates {
        let idx = rng.index(rows.len());
        let id = rows[idx].id;
        if rng.one_in(2) {
            let a = int_or_null(&mut rng, &[1, 2, 3]);
            engine
                .execute(
                    &parse(&format!("UPDATE t SET a = {} WHERE id = {id}", sql_int(a)))
                        .expect("parse")
                        .remove(0),
                )
                .expect("update a");
            rows[idx].a = a;
        } else {
            let c = text_or_null(&mut rng);
            engine
                .execute(
                    &parse(&format!(
                        "UPDATE t SET c = {} WHERE id = {id}",
                        sql_text(&c)
                    ))
                    .expect("parse")
                    .remove(0),
                )
                .expect("update c");
            rows[idx].c = c;
        }
    }

    (engine, rows)
}

/// The fixed probe matrix: a spread of projections crossed with a spread of
/// filters (matching, non-matching, and per-column), including the unfiltered
/// read. Deterministic, so coverage does not depend on the seed.
fn probes() -> Vec<(Vec<usize>, bool, Filter)> {
    let projections: [(Vec<usize>, bool); 6] = [
        (vec![0, 1, 2, 3], true), // SELECT *
        (vec![0], false),
        (vec![1], false),
        (vec![3, 1], false),
        (vec![2, 3, 0], false),
        (vec![0, 1, 2, 3], false),
    ];
    let filters: [Filter; 11] = [
        None,
        Some((0, ScalarValue::Int4(1))),
        Some((0, ScalarValue::Int4(3))),
        Some((0, ScalarValue::Int4(999))), // matches nothing
        Some((1, ScalarValue::Int4(1))),
        Some((1, ScalarValue::Int4(2))),
        Some((2, ScalarValue::Int4(10))),
        Some((2, ScalarValue::Int4(20))),
        Some((3, ScalarValue::Text("x".to_owned()))),
        Some((3, ScalarValue::Text("y".to_owned()))),
        Some((3, ScalarValue::Text("absent".to_owned()))), // matches nothing
    ];
    let mut out = Vec::new();
    for (projection, all) in projections {
        for filter in &filters {
            out.push((projection.clone(), all, filter.clone()));
        }
    }
    out
}

/// Sweep every probe and assert the engine agrees with the reference, returning a
/// digest of the agreed answers (so a seed's replay can be checked stable).
fn differential(seed: u64, ignore_filter: bool) -> u64 {
    let (mut engine, rows) = build(seed);
    let mut digest: u64 = 0xCBF2_9CE4_8422_2325;
    for (projection, all, filter) in probes() {
        let sql = format!(
            "SELECT {} FROM t{}",
            projection_sql(&projection, all),
            where_sql(&filter)
        );
        let got = engine_rows(&mut engine, &sql);
        let want = reference(&rows, &projection, &filter, ignore_filter);
        assert_eq!(got, want, "seed {seed}: divergence on `{sql}`");
        // Fold the agreed answer into the digest.
        for row in &got {
            for cell in row {
                match cell {
                    None => digest = (digest ^ 0xFF).wrapping_mul(0x0100_0000_01B3),
                    Some(bytes) => {
                        for &byte in bytes {
                            digest = (digest ^ u64::from(byte)).wrapping_mul(0x0100_0000_01B3);
                        }
                    }
                }
            }
            digest = digest.wrapping_add(0x9E37_79B9_7F4A_7C15);
        }
    }
    digest
}

#[test]
fn engine_matches_the_reference_across_seeds() {
    // Each seed asserts (internally) that every projected/filtered answer matches
    // the reference at every probe.
    for seed in 0..96 {
        let _ = differential(seed, false);
    }
}

#[test]
fn the_workload_is_reproducible_and_seed_dependent() {
    let digests: Vec<u64> = (0..96).map(|seed| differential(seed, false)).collect();
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
    // The teeth test: a reference that drops the `WHERE` (returns every row,
    // projected) must be caught by the very same differential check. A seed with
    // enough rows guarantees some filtered probe returns fewer rows than the
    // unfiltered table, so the bug diverges.
    differential(7, true);
}
