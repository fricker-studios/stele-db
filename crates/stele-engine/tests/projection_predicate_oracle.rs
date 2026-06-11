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
//! included) is applied to both; then a fixed matrix of `(projection, WHERE)`
//! probes is swept and the engine's rows are asserted byte-for-byte equal to the
//! reference's. The `WHERE` probes span every comparison operator and integer
//! `/` / `%` arithmetic ([STL-213]), not just the original `<col> = <literal>`
//! ([STL-151]). The [teeth test](#tests) injects a *documented intentional bug* (a
//! reference that ignores the `WHERE`) and proves the very same differential check
//! catches it.
//!
//! [STL-151]: https://allegromusic.atlassian.net/browse/STL-151
//! [STL-213]: https://allegromusic.atlassian.net/browse/STL-213

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
    const fn next(&mut self) -> u64 {
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
    const fn one_in(&mut self, n: u64) -> bool {
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

/// One `WHERE <col> [<arith> k] <cmp> <literal>` predicate, or `None` for an
/// unfiltered read — the STL-151 / STL-213 WHERE surface the oracle sweeps.
type Filter = Option<Where>;

/// A swept `WHERE` predicate: a column, optionally wrapped in an integer `% k` /
/// `/ k` ([STL-213]), compared against a literal.
#[derive(Clone)]
struct Where {
    /// The column the predicate reads.
    col: usize,
    /// An optional integer arithmetic applied to the (int) column first.
    arith: Option<(Arith, i32)>,
    /// The comparison operator.
    cmp: Cmp,
    /// The literal compared against (`Int4` for an int column, `Text` for col 3).
    value: ScalarValue,
}

/// The six comparison operators ([STL-213] broadens the binder past `=`).
#[derive(Clone, Copy)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// The two integer arithmetic operators STL-213's Definition of Done names.
#[derive(Clone, Copy)]
enum Arith {
    Div,
    Mod,
}

impl Cmp {
    /// The SQL spelling.
    const fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }

    /// Whether `a <cmp> b` holds — the deliberately-dumb truth, straight off the
    /// ordering.
    fn holds<T: Ord + ?Sized>(self, a: &T, b: &T) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        matches!(
            (self, a.cmp(b)),
            (Self::Eq, Equal)
                | (Self::Ne, Less | Greater)
                | (Self::Lt, Less)
                | (Self::Le, Less | Equal)
                | (Self::Gt, Greater)
                | (Self::Ge, Greater | Equal)
        )
    }
}

impl Arith {
    /// The SQL spelling.
    const fn sql(self) -> &'static str {
        match self {
            Self::Div => "/",
            Self::Mod => "%",
        }
    }

    /// Truncating division / remainder, matching the evaluator's checked ops (a
    /// zero divisor would be `None`; the probes never use one).
    const fn apply(self, value: i32, k: i32) -> Option<i32> {
        match self {
            Self::Div => value.checked_div(k),
            Self::Mod => value.checked_rem(k),
        }
    }
}

/// The reference answer: filter by evaluating the predicate in plain Rust (a NULL
/// cell, or a `None` arithmetic, never matches — the engine's 3VL stance), order by
/// the encoded business key (the engine's `BTreeMap` order), then project the
/// requested columns. `ignore_filter` is the seam for the teeth test — the *correct*
/// reference passes `false`.
fn reference(
    rows: &[Row],
    projection: &[usize],
    filter: &Filter,
    ignore_filter: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut matched: Vec<&Row> = rows
        .iter()
        .filter(|row| {
            let (Some(predicate), false) = (filter, ignore_filter) else {
                return true;
            };
            where_keeps(row, predicate)
        })
        .collect();
    matched.sort_by_key(|row| row.cell(0));
    matched
        .iter()
        .map(|row| projection.iter().map(|&c| row.cell(c)).collect())
        .collect()
}

/// Whether a row passes a `WHERE` predicate: decode the cell, apply any integer
/// arithmetic, then compare. A NULL cell (or a `None` arithmetic result) is never
/// kept — only a TRUE keeps the row, the same three-valued rule the engine's
/// `Filter` applies.
fn where_keeps(row: &Row, predicate: &Where) -> bool {
    match &predicate.value {
        // The text column (index 3) is compared lexicographically; no arithmetic.
        ScalarValue::Text(want) => row
            .c
            .as_deref()
            .is_some_and(|s| predicate.cmp.holds(s, want.as_str())),
        // The int columns (0 = id, 1 = a, 2 = b), optionally wrapped in arithmetic.
        ScalarValue::Int4(want) => {
            let cell = match predicate.col {
                0 => Some(row.id),
                1 => row.a,
                2 => row.b,
                _ => unreachable!("only columns 0..=2 are int4"),
            };
            let value = match (cell, predicate.arith) {
                (Some(v), Some((arith, k))) => arith.apply(v, k),
                (Some(v), None) => Some(v),
                (None, _) => None,
            };
            value.is_some_and(|v| predicate.cmp.holds(&v, want))
        }
        _ => unreachable!("the oracle filters int4 / text columns only"),
    }
}

/// Render a `WHERE` predicate (or empty) as SQL — the column, any arithmetic, the
/// operator, and the comparand literal in the column's lexical form.
fn where_sql(filter: &Filter) -> String {
    let Some(predicate) = filter else {
        return String::new();
    };
    let column = COLUMNS[predicate.col].0;
    let lhs = match predicate.arith {
        Some((arith, k)) => format!("{column} {} {k}", arith.sql()),
        None => column.to_owned(),
    };
    let literal = match &predicate.value {
        ScalarValue::Int4(v) => v.to_string(),
        ScalarValue::Text(s) => format!("'{s}'"),
        _ => unreachable!("the oracle only filters int4 / text columns"),
    };
    format!(" WHERE {lhs} {} {literal}", predicate.cmp.sql())
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
    // `i`: `col <cmp> int`; `t`: `c <cmp> 'text'`; `arith`: `col <op> k <cmp> int`.
    let i = |col, cmp, v| {
        Some(Where {
            col,
            arith: None,
            cmp,
            value: ScalarValue::Int4(v),
        })
    };
    let t = |cmp, s: &str| {
        Some(Where {
            col: 3,
            arith: None,
            cmp,
            value: ScalarValue::Text(s.to_owned()),
        })
    };
    let arith = |col, op, k, cmp, v| {
        Some(Where {
            col,
            arith: Some((op, k)),
            cmp,
            value: ScalarValue::Int4(v),
        })
    };
    let filters: Vec<Filter> = vec![
        None,
        // Equalities — the STL-151 surface (key push-down + value columns + text).
        i(0, Cmp::Eq, 1),
        i(0, Cmp::Eq, 999), // matches nothing
        i(1, Cmp::Eq, 1),
        i(2, Cmp::Eq, 10),
        t(Cmp::Eq, "x"),
        t(Cmp::Eq, "absent"), // matches nothing
        // Inequalities and ordering across every operator (STL-213).
        i(0, Cmp::Ge, 3),
        i(0, Cmp::Lt, 4),
        i(1, Cmp::Ne, 1),
        i(1, Cmp::Lt, 2),
        i(1, Cmp::Gt, 1),
        i(1, Cmp::Le, 2),
        i(2, Cmp::Gt, 10),
        t(Cmp::Lt, "y"),
        t(Cmp::Ge, "y"),
        t(Cmp::Ne, "x"),
        // Integer division / modulo on either int value column (STL-213).
        arith(1, Arith::Mod, 2, Cmp::Eq, 0),
        arith(1, Arith::Mod, 2, Cmp::Eq, 1),
        arith(1, Arith::Div, 2, Cmp::Eq, 1),
        arith(2, Arith::Div, 10, Cmp::Eq, 1),
        arith(2, Arith::Div, 10, Cmp::Gt, 1),
        arith(2, Arith::Mod, 10, Cmp::Ne, 0),
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
