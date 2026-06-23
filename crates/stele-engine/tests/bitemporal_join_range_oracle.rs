//! The temporal-range-over-a-JOIN correctness oracle ([STL-344], [STL-348],
//! [docs/06 §4]).
//!
//! A `FOR { SYSTEM_TIME | VALID_TIME } { FROM a TO b | BETWEEN a AND b }` range over
//! a join is the "history of the joined result over an interval" read. The join's
//! shape decides how the inputs' intervals combine — docs/16 §8's point join lifted
//! to an interval:
//!
//! * `INNER` ([STL-344]) — a joined tuple's period is the **intersection**
//!   `[max(from), min(to))` of its matched versions; an empty intersection (the two
//!   were never both live) does not join.
//! * `LEFT` / `SEMI` / `ANTI` ([STL-348]) — the **interval difference** of a left
//!   version's period against its matched right sub-intervals. A `LEFT` join keeps
//!   the matched rows and `NULL`-extends the *gaps* (the instants the left row was
//!   live with no overlapping match); `SEMI` keeps a left row over the sub-intervals
//!   it *did* have a match, `ANTI` over the gaps it did not. A match strictly inside
//!   a left version's window **fragments** it into several output rows.
//!
//! Getting that *set* — and the fragmenting endpoints — right at the half-open /
//! closed boundaries is the temporal heart of the feature, so it is checked against a
//! reference.
//!
//! This is a **differential** straight off the ticket's wording, in the [STL-243]
//! join-oracle mold: the real engine join (a hash join over a fresh columnar range
//! scan) is checked against **joining the inputs' single-table range reads** with a
//! deliberately-dumb nested loop. The single-table range reads are the engine's own
//! [STL-244] / [STL-328] path — itself oracle-backed against a naive timeline model —
//! so the only thing this oracle adds is the nested-loop join, the interval
//! intersection, an *independent* overlap predicate (two inclusive integer-instant
//! ranges intersect or they don't, never a copy of the engine's `overlaps`), and an
//! *independent* interval difference computed by a **breakpoint sweep** ([`ref_classify`])
//! rather than the engine's sort-merge. If the real join agrees at every probe, it
//! combined the same way over the same per-input version sets. The [teeth tests](#tests)
//! make the reference compute the interval *union* instead of the intersection
//! ([`Combine::Union`]) and the difference *all-or-nothing* instead of fragmenting
//! ([`Difference::AllOrNothing`]) — the two join-specific mistakes — and prove the
//! same differential catches each.
//!
//! Both axes are swept (system: every input ranges on the system axis; valid: every
//! input system-live at the snapshot, ranging on the valid axis), every join shape,
//! the two-table and a three-table left-deep chain (including mixed shapes), and the
//! workload is replayed across the delta/sealed flush boundary — history a range join
//! must reconstruct identically whether a version is staged or sealed.
//!
//! [docs/06 §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart
//! [STL-243]: https://allegromusic.atlassian.net/browse/STL-243
//! [STL-244]: https://allegromusic.atlassian.net/browse/STL-244
//! [STL-328]: https://allegromusic.atlassian.net/browse/STL-328
//! [STL-344]: https://allegromusic.atlassian.net/browse/STL-344
//! [STL-348]: https://allegromusic.atlassian.net/browse/STL-348

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

/// When to flush the delta tier into sealed segments, so the range join is asserted
/// identical across the delta/sealed boundary.
#[derive(Debug, Clone, Copy)]
enum FlushMode {
    Never,
    Midway,
    Full,
}

/// Whether the workload's tables carry a valid-time period — selects the range
/// axis the join is read over.
#[derive(Debug, Clone, Copy)]
enum Axis {
    System,
    Valid,
}

/// The integer key domain shared across tables (so joins match) and the valid-time
/// window ceiling.
const KEY_POOL: i64 = 3;
const VMAX: i64 = 9;

/// Encode a `ScalarValue` to its canonical wire bytes — the exact form a
/// `SelectResult` cell carries.
fn enc(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// Execute one statement, asserting it succeeds.
fn run(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    let stmt = parse(sql).expect("parse").remove(0);
    engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

/// Execute a `SELECT`, returning its rows.
fn select(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> Vec<Vec<Option<Vec<u8>>>> {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(SelectResult { rows, .. }) = engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"))
    else {
        panic!("a SELECT must return rows for `{sql}`");
    };
    rows
}

/// Decode a non-NULL `int8` / `timestamptz` cell (both little-endian `i64`).
fn dec_i64(cell: Option<&[u8]>) -> i64 {
    let bytes = cell.expect("non-NULL int8 cell");
    i64::from_le_bytes(bytes.try_into().expect("int8 is 8 bytes"))
}

/// Decode a non-NULL `int4` cell.
fn dec_i32(cell: Option<&[u8]>) -> i32 {
    let bytes = cell.expect("non-NULL int4 cell");
    i32::from_le_bytes(bytes.try_into().expect("int4 is 4 bytes"))
}

/// The `FOR { SYSTEM_TIME | VALID_TIME } { FROM lo TO hi | BETWEEN lo AND hi }`
/// range clause for the axis and bound inclusivity.
fn range_clause(axis: Axis, lo: i64, hi: i64, closed: bool) -> String {
    let dim = match axis {
        Axis::System => "SYSTEM_TIME",
        Axis::Valid => "VALID_TIME",
    };
    if closed {
        format!("FOR {dim} BETWEEN {lo} AND {hi}")
    } else {
        format!("FOR {dim} FROM {lo} TO {hi}")
    }
}

/// A row of canonical-encoded cells — the `SelectResult` row shape.
type Row = Vec<Option<Vec<u8>>>;

/// A list of `[from, to)` intervals on the ranged axis (`to == i64::MAX` for `+∞`).
type Intervals = Vec<(i64, i64)>;

/// One side's range-read row reduced to what the join needs: the join key (the
/// `id` PRIMARY KEY, never NULL), the carried value, and the version's interval
/// `[from, to)` on the ranged axis (`to == i64::MAX` for an open `+∞` end).
#[derive(Clone)]
struct SideRow {
    key: Vec<u8>,
    val: Option<Vec<u8>>,
    from: i64,
    to: i64,
}

/// The period-endpoint column names for the axis.
const fn endpoint_names(axis: Axis) -> (&'static str, &'static str) {
    match axis {
        Axis::System => ("sys_from", "sys_to"),
        Axis::Valid => ("valid_from", "valid_to"),
    }
}

/// A seeded bitemporal workload over `tables`, sharing one engine (so commits
/// interleave). Each write is an insert / valid-window-shifting update / delete on
/// a random key of a random table; the union of commit instants (the system-axis
/// probe grid) is returned alongside the engine.
fn build(
    seed: u64,
    axis: Axis,
    flush: FlushMode,
    tables: &[&str],
) -> (SessionEngine<ZeroClock, MemDisk>, Vec<i64>) {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    for t in tables {
        let ddl = match axis {
            Axis::System => {
                format!("CREATE TABLE {t} (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
            }
            Axis::Valid => format!(
                "CREATE TABLE {t} (id INT PRIMARY KEY, v INT, vf TIMESTAMP, vt TIMESTAMP) \
                 WITH SYSTEM VERSIONING VALID TIME (vf, vt)"
            ),
        };
        run(&mut engine, &ddl);
    }

    let mut commits: Vec<i64> = vec![0];
    // Per-(table, key) liveness, so each statement is one the engine accepts (no
    // duplicate insert, no update / delete of an absent key).
    let mut live: std::collections::HashSet<(usize, i32)> = std::collections::HashSet::new();

    let op_count = 16 + rng.below(17); // 16..=32 writes across the tables
    let flush_at = op_count / 2;
    for step in 0..op_count {
        let ti =
            usize::try_from(rng.below(i64::try_from(tables.len()).expect("fits"))).expect("fits");
        let table = tables[ti];
        let id = i32::try_from(rng.below(KEY_POOL)).expect("fits");
        let v = i32::try_from(step + 1).expect("fits");
        let is_live = live.contains(&(ti, id));
        // A well-formed valid window in `[0, VMAX]`, sometimes open-ended.
        let vf = rng.below(VMAX);
        let opened = rng.below(4) == 0;
        let vt = if opened {
            i64::MAX
        } else {
            vf + 1 + rng.below(VMAX - vf)
        };
        let valid_cols = |with_vt: bool| match axis {
            Axis::Valid if with_vt => format!(", vf = {vf}, vt = {vt}"),
            Axis::Valid => format!(", vf = {vf}"),
            Axis::System => String::new(),
        };

        if is_live && rng.below(3) == 0 {
            run(&mut engine, &format!("DELETE FROM {table} WHERE id = {id}"));
            live.remove(&(ti, id));
        } else if is_live {
            run(
                &mut engine,
                &format!(
                    "UPDATE {table} SET v = {v}{} WHERE id = {id}",
                    valid_cols(!opened)
                ),
            );
        } else {
            let stmt = match axis {
                Axis::System => format!("INSERT INTO {table} VALUES ({id}, {v})"),
                Axis::Valid if opened => {
                    format!("INSERT INTO {table} (id, v, vf) VALUES ({id}, {v}, {vf})")
                }
                Axis::Valid => format!("INSERT INTO {table} VALUES ({id}, {v}, {vf}, {vt})"),
            };
            run(&mut engine, &stmt);
            live.insert((ti, id));
        }
        commits.push(engine.commit_clock().0);

        if matches!(flush, FlushMode::Midway) && step == flush_at {
            run(&mut engine, "FLUSH");
        }
        if matches!(flush, FlushMode::Full) {
            run(&mut engine, "FLUSH");
        }
    }

    commits.sort_unstable();
    commits.dedup();
    (engine, commits)
}

/// Read one base table's single-table range over `[lo, hi)` / `[lo, hi]` as
/// [`SideRow`]s — the *trusted* [STL-244] / [STL-328] path the differential joins.
/// The projection is `id, v, <from>, <to>` so the row shape is uniform across axes.
fn side_rows(
    engine: &mut SessionEngine<ZeroClock, MemDisk>,
    table: &str,
    axis: Axis,
    lo: i64,
    hi: i64,
    closed: bool,
) -> Vec<SideRow> {
    let (from_col, to_col) = endpoint_names(axis);
    let clause = range_clause(axis, lo, hi, closed);
    let sql = format!("SELECT id, v, {from_col}, {to_col} FROM {table} {clause}");
    select(engine, &sql)
        .into_iter()
        .map(|r| SideRow {
            key: r[0].clone().expect("the id PRIMARY KEY is never NULL"),
            val: r[1].clone(),
            from: dec_i64(r[2].as_deref()),
            // An open (`+∞`) end renders NULL; carry it as i64::MAX so `min` keeps it.
            to: r[3].as_deref().map_or(i64::MAX, |b| dec_i64(Some(b))),
        })
        .collect()
}

/// The independent overlap predicate ([STL-244]'s "off-by-one" derivation, not the
/// engine's `overlaps`): the intersected interval is active at integer instants
/// `[from, to-1]` (or `+∞`), the query covers `[lo, hi-1]` (half-open) or `[lo, hi]`
/// (closed), and the two inclusive ranges either intersect or they don't.
fn range_overlaps(from: i64, to: i64, lo: i64, hi: i64, closed: bool) -> bool {
    let last_active = if to == i64::MAX { i64::MAX } else { to - 1 };
    let query_hi = if closed { hi } else { hi - 1 };
    from <= last_active && lo <= query_hi && from.max(lo) <= last_active.min(query_hi)
}

/// How the reference combines two paired intervals — the correct **intersection**,
/// or (the teeth variant) the **union**, the join-specific mistake.
#[derive(Clone, Copy)]
enum Combine {
    Intersect,
    Union,
}

impl Combine {
    fn apply(self, af: i64, at: i64, bf: i64, bt: i64) -> (i64, i64) {
        match self {
            Self::Intersect => (af.max(bf), at.min(bt)),
            Self::Union => (af.min(bf), at.max(bt)),
        }
    }
}

/// The join shape of one chain step — the reference's own enum (independent of the
/// engine's `JoinType`), spelled into SQL by [`RefJoin::keyword`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RefJoin {
    Inner,
    Left,
    Semi,
    Anti,
}

impl RefJoin {
    /// The SQL keyword introducing this step in the `FROM` chain.
    const fn keyword(self) -> &'static str {
        match self {
            Self::Inner => "JOIN",
            Self::Left => "LEFT JOIN",
            Self::Semi => "SEMI JOIN",
            Self::Anti => "ANTI JOIN",
        }
    }

    /// Whether the output carries this step's right columns (`INNER` / `LEFT`); a
    /// `SEMI` / `ANTI` step keeps only the accumulated left.
    const fn keeps_right(self) -> bool {
        matches!(self, Self::Inner | Self::Left)
    }
}

/// How the reference computes the interval *difference* (the `LEFT` gap / `ANTI` keep
/// / `SEMI` match regions) — the correct fragmenting **subtraction**, or (the teeth
/// variant) an **all-or-nothing** per-left-row decision that ignores a match landing
/// inside a left version's window: the [STL-348]-specific mistake.
#[derive(Clone, Copy)]
enum Difference {
    Subtract,
    AllOrNothing,
}

impl Difference {
    /// The gap sub-intervals a `LEFT` join NULL-extends / an `ANTI` join keeps.
    fn gaps(self, lf: i64, lt: i64, covers: &[(i64, i64)], uncovered: &[(i64, i64)]) -> Intervals {
        match self {
            // The true interval difference (fragmenting), from the breakpoint sweep.
            Self::Subtract => uncovered.to_vec(),
            // The bug: a left row is "unmatched" only with *no* overlapping match at
            // all — so a mid-window match wrongly suppresses the surrounding gaps.
            Self::AllOrNothing if covers.is_empty() => vec![(lf, lt)],
            Self::AllOrNothing => Vec::new(),
        }
    }

    /// The sub-intervals a `SEMI` join keeps (the matched region).
    fn covered(self, lf: i64, lt: i64, covers: &[(i64, i64)], covered: &[(i64, i64)]) -> Intervals {
        match self {
            // The true coalesced matched region, from the breakpoint sweep.
            Self::Subtract => covered.to_vec(),
            // The bug: a single match claims the *whole* left window, hiding the gaps
            // it actually leaves.
            Self::AllOrNothing if covers.is_empty() => Vec::new(),
            Self::AllOrNothing => vec![(lf, lt)],
        }
    }
}

/// Independently classify a left version's period `[lf, lt)` into its maximal
/// **covered** and **uncovered** sub-intervals against `covers` (each already clipped
/// to `[lf, lt)`), by a *breakpoint sweep* — every cover boundary inside `[lf, lt)`
/// splits the period into elementary segments, each entirely inside or outside every
/// cover, then consecutive same-class segments coalesce into maximal runs.
///
/// This is deliberately a *different* algorithm from the engine's sort-merge
/// ([`merge_covers`] / `interval_gaps`), so the differential is real evidence, not a
/// restatement. The open `+∞` end (`i64::MAX`) is just a large upper bound under `<`
/// / `min` / `max` — never incremented — so it sweeps without special-casing.
fn ref_classify(lf: i64, lt: i64, covers: &[(i64, i64)]) -> (Intervals, Intervals) {
    let mut pts: Vec<i64> = vec![lf, lt];
    for &(cf, ct) in covers {
        if lf < cf && cf < lt {
            pts.push(cf);
        }
        if lf < ct && ct < lt {
            pts.push(ct);
        }
    }
    pts.sort_unstable();
    pts.dedup();

    let mut covered: Vec<(i64, i64)> = Vec::new();
    let mut uncovered: Vec<(i64, i64)> = Vec::new();
    for win in pts.windows(2) {
        let (a, b) = (win[0], win[1]);
        // The segment is covered iff some cover fully contains it (it cannot straddle
        // a boundary — every boundary is a breakpoint).
        let is_covered = covers.iter().any(|&(cf, ct)| cf <= a && b <= ct);
        let run = if is_covered {
            &mut covered
        } else {
            &mut uncovered
        };
        match run.last_mut() {
            Some(last) if last.1 == a => last.1 = b,
            _ => run.push((a, b)),
        }
    }
    (covered, uncovered)
}

/// The reference rows for a range over a left-deep `id`-equijoin of `sides` with the
/// per-step shapes `steps` (`steps[i]` joins `sides[i + 1]` onto the chain). Folds
/// left-deep, combining each input's interval into the running one: an `INNER` /
/// `LEFT` step emits the matched rows (`combine`-d interval, dropping empty pairs); a
/// `LEFT` / `ANTI` step adds the gap rows and a `SEMI` step the matched-region rows,
/// both from the independent [`ref_classify`] difference (`difference` flips them to
/// the buggy all-or-nothing for the teeth test). The projection grows by a step's
/// right columns only for an `INNER` / `LEFT` step, matching the engine's addressable
/// output. A surviving row is kept iff its interval overlaps the query window, its
/// endpoints appended unclipped (open `to` → NULL).
fn reference(
    sides: &[Vec<SideRow>],
    steps: &[RefJoin],
    lo: i64,
    hi: i64,
    closed: bool,
    combine: Combine,
    difference: Difference,
) -> Vec<Vec<Option<Vec<u8>>>> {
    // Accumulated rows: the projected cells so far, plus the running interval.
    let mut acc: Vec<(Row, i64, i64)> = sides[0]
        .iter()
        .map(|s| (vec![Some(s.key.clone()), s.val.clone()], s.from, s.to))
        .collect();
    for (side, &step) in sides[1..].iter().zip(steps) {
        let mut next: Vec<(Row, i64, i64)> = Vec::new();
        for (cells, af, at) in &acc {
            // The left-deep chain joins every step on `id` — the accumulated row's
            // first cell (s0.id) against this side's key.
            let acc_key = cells[0].as_ref().expect("id never NULL");
            // The matched right sub-intervals clipped to the accumulated interval (the
            // covers the difference works over) and, for INNER / LEFT, the matched rows.
            let mut covers: Vec<(i64, i64)> = Vec::new();
            let mut matched: Vec<(Row, i64, i64)> = Vec::new();
            for s in side {
                if acc_key != &s.key {
                    continue;
                }
                let (from, to) = combine.apply(*af, *at, s.from, s.to);
                if from < to {
                    covers.push((from, to));
                    if step.keeps_right() {
                        let mut row = cells.clone();
                        row.push(Some(s.key.clone()));
                        row.push(s.val.clone());
                        matched.push((row, from, to));
                    }
                }
            }
            let (covered, uncovered) = ref_classify(*af, *at, &covers);
            match step {
                RefJoin::Inner => next.extend(matched),
                RefJoin::Left => {
                    next.extend(matched);
                    for (from, to) in difference.gaps(*af, *at, &covers, &uncovered) {
                        let mut row = cells.clone();
                        row.push(None); // NULL right id
                        row.push(None); // NULL right v
                        next.push((row, from, to));
                    }
                }
                RefJoin::Semi => {
                    for (from, to) in difference.covered(*af, *at, &covers, &covered) {
                        next.push((cells.clone(), from, to));
                    }
                }
                RefJoin::Anti => {
                    for (from, to) in difference.gaps(*af, *at, &covers, &uncovered) {
                        next.push((cells.clone(), from, to));
                    }
                }
            }
        }
        acc = next;
    }

    let mut rows: Vec<Vec<Option<Vec<u8>>>> = acc
        .into_iter()
        .filter(|(_, from, to)| range_overlaps(*from, *to, lo, hi, closed))
        .map(|(mut cells, from, to)| {
            cells.push(Some(enc(&ScalarValue::TimestampTz(from))));
            cells.push((to != i64::MAX).then(|| enc(&ScalarValue::TimestampTz(to))));
            cells
        })
        .collect();
    rows.sort();
    rows
}

/// The engine's rows for the range over a left-deep `id`-equijoin of `tables` with
/// the per-step shapes `steps`, projected to match [`reference`]'s shape (the seed's
/// columns, then each `INNER` / `LEFT` step's right columns — a `SEMI` / `ANTI` step
/// drops its right, so it is not projectable), sorted to compare as a set. Every step
/// joins on the seed's `id`, the chain the reference's `cells[0]` key mirrors.
fn engine_join(
    engine: &mut SessionEngine<ZeroClock, MemDisk>,
    tables: &[&str],
    steps: &[RefJoin],
    axis: Axis,
    lo: i64,
    hi: i64,
    closed: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    use std::fmt::Write as _;
    let (from_col, to_col) = endpoint_names(axis);
    let seed = tables[0];
    let mut proj: Vec<String> = vec![format!("{seed}.id, {seed}.v")];
    let mut joins = String::new();
    for (i, &step) in steps.iter().enumerate() {
        let t = tables[i + 1];
        write!(joins, " {} {t} ON {seed}.id = {t}.id", step.keyword()).expect("write to String");
        if step.keeps_right() {
            proj.push(format!("{t}.id, {t}.v"));
        }
    }
    let clause = range_clause(axis, lo, hi, closed);
    let sql = format!(
        "SELECT {}, {from_col}, {to_col} FROM {seed}{joins} {clause}",
        proj.join(", ")
    );
    let mut rows = select(engine, &sql);
    rows.sort();
    rows
}

/// The system-axis probe grid: every commit boundary, each also `±1`, so a query
/// edge lands exactly on, just before, and just after every supersession.
fn boundary_grid(commits: &[i64]) -> Vec<i64> {
    let mut marks: Vec<i64> = Vec::new();
    for &c in commits {
        marks.extend([c - 1, c, c + 1]);
    }
    marks.retain(|&m| m >= 0);
    marks.sort_unstable();
    marks.dedup();
    marks
}

#[test]
fn system_range_join_matches_the_reference_across_seeds_flush_and_boundaries() {
    let tables = ["l", "r"];
    let steps = [RefJoin::Inner];
    let mut probes: u64 = 0;
    let mut rows_seen: u64 = 0;
    for seed in 0..8u64 {
        for flush in [FlushMode::Never, FlushMode::Midway, FlushMode::Full] {
            let (mut engine, commits) = build(seed, Axis::System, flush, &tables);
            let marks = boundary_grid(&commits);
            for (i, &lo) in marks.iter().enumerate() {
                for &hi in &marks[i..] {
                    for &closed in &[false, true] {
                        if !closed && lo >= hi {
                            continue; // a half-open FROM..TO needs lo < hi
                        }
                        let sides = vec![
                            side_rows(&mut engine, "l", Axis::System, lo, hi, closed),
                            side_rows(&mut engine, "r", Axis::System, lo, hi, closed),
                        ];
                        let want = reference(
                            &sides,
                            &steps,
                            lo,
                            hi,
                            closed,
                            Combine::Intersect,
                            Difference::Subtract,
                        );
                        let got =
                            engine_join(&mut engine, &tables, &steps, Axis::System, lo, hi, closed);
                        assert_eq!(
                            got, want,
                            "seed {seed}, {flush:?}, [{lo},{hi}) closed={closed}"
                        );
                        rows_seen += u64::try_from(got.len()).expect("fits");
                        probes += 1;
                    }
                }
            }
        }
    }
    assert!(
        rows_seen > 0,
        "every probe was empty — the workload joined nothing"
    );
    assert!(
        probes > 2_000,
        "differential probed only {probes} cells — widen the sweep"
    );
}

#[test]
fn valid_range_join_matches_the_reference_across_seeds_flush_and_boundaries() {
    let tables = ["l", "r"];
    let steps = [RefJoin::Inner];
    let mut probes: u64 = 0;
    let mut rows_seen: u64 = 0;
    for seed in 0..8u64 {
        for flush in [FlushMode::Never, FlushMode::Midway, FlushMode::Full] {
            let (mut engine, _commits) = build(seed, Axis::Valid, flush, &tables);
            // The valid window boundaries, clamped to a tidy probe range; the system
            // axis stays at `now` (no FOR SYSTEM_TIME qualifier), so corrections that
            // superseded a window are excluded exactly as the single-table path is.
            for lo in 0..=(VMAX + 1) {
                for hi in lo..=(VMAX + 1) {
                    for &closed in &[false, true] {
                        if !closed && lo >= hi {
                            continue;
                        }
                        let sides = vec![
                            side_rows(&mut engine, "l", Axis::Valid, lo, hi, closed),
                            side_rows(&mut engine, "r", Axis::Valid, lo, hi, closed),
                        ];
                        let want = reference(
                            &sides,
                            &steps,
                            lo,
                            hi,
                            closed,
                            Combine::Intersect,
                            Difference::Subtract,
                        );
                        let got =
                            engine_join(&mut engine, &tables, &steps, Axis::Valid, lo, hi, closed);
                        assert_eq!(
                            got, want,
                            "seed {seed}, {flush:?}, [{lo},{hi}) closed={closed}"
                        );
                        rows_seen += u64::try_from(got.len()).expect("fits");
                        probes += 1;
                    }
                }
            }
        }
    }
    assert!(
        rows_seen > 0,
        "every probe was empty — the workload joined nothing"
    );
    assert!(
        probes > 2_000,
        "differential probed only {probes} cells — widen the sweep"
    );
}

#[test]
fn three_way_left_deep_range_join_matches_the_reference() {
    // The N-way left-deep chain ([STL-323]): `a JOIN b ON a.id=b.id JOIN c ON
    // a.id=c.id` intersects all three inputs' intervals. A lighter sweep (the
    // two-table tests already cover the boundary surface densely) over both axes.
    for axis in [Axis::System, Axis::Valid] {
        let tables = ["a", "b", "c"];
        let steps = [RefJoin::Inner, RefJoin::Inner];
        for seed in 0..6u64 {
            let (mut engine, commits) = build(seed, axis, FlushMode::Midway, &tables);
            let marks = match axis {
                Axis::System => boundary_grid(&commits),
                Axis::Valid => (0..=(VMAX + 1)).collect(),
            };
            for (i, &lo) in marks.iter().enumerate() {
                for &hi in marks[i..].iter().step_by(2) {
                    if lo >= hi {
                        continue;
                    }
                    let sides: Vec<Vec<SideRow>> = tables
                        .iter()
                        .map(|t| side_rows(&mut engine, t, axis, lo, hi, false))
                        .collect();
                    let want = reference(
                        &sides,
                        &steps,
                        lo,
                        hi,
                        false,
                        Combine::Intersect,
                        Difference::Subtract,
                    );
                    let got = engine_join(&mut engine, &tables, &steps, axis, lo, hi, false);
                    assert_eq!(got, want, "{axis:?} seed {seed}, [{lo},{hi})");
                }
            }
        }
    }
}

#[test]
fn the_oracle_has_teeth_union_instead_of_intersection() {
    // A reference that *unions* the paired intervals instead of intersecting them —
    // the join-specific mistake (a tuple's period is the overlap of its inputs, not
    // their span) — must be caught by the very same differential the tests above
    // run. The union keeps pairs that never co-existed and reports wrong endpoints,
    // so it disagrees with the engine somewhere.
    let tables = ["l", "r"];
    let steps = [RefJoin::Inner];
    let mut mismatch = false;
    for axis in [Axis::System, Axis::Valid] {
        let (mut engine, commits) = build(3, axis, FlushMode::Never, &tables);
        let marks = match axis {
            Axis::System => boundary_grid(&commits),
            Axis::Valid => (0..=(VMAX + 1)).collect(),
        };
        for (i, &lo) in marks.iter().enumerate() {
            for &hi in &marks[i..] {
                if lo >= hi {
                    continue;
                }
                let sides = vec![
                    side_rows(&mut engine, "l", axis, lo, hi, false),
                    side_rows(&mut engine, "r", axis, lo, hi, false),
                ];
                let buggy = reference(
                    &sides,
                    &steps,
                    lo,
                    hi,
                    false,
                    Combine::Union,
                    Difference::Subtract,
                );
                let got = engine_join(&mut engine, &tables, &steps, axis, lo, hi, false);
                if got != buggy {
                    mismatch = true;
                }
            }
        }
    }
    assert!(
        mismatch,
        "a union-instead-of-intersection reference must disagree with the engine"
    );
}

#[test]
fn select_star_exposes_endpoints_and_open_to_renders_null() {
    // A focused, hand-checked system-axis timeline proving the intersection and the
    // `SELECT *` output shape. l(id=1): v=10 over [t1, t2), then v=11 over [t2, +∞).
    // r(id=1): v=20 over [t3, +∞). Joined over the whole history, the tuple
    // (l.v=10, r.v=20) is live over [t3, t2) (10's window starts before r exists),
    // and (l.v=11, r.v=20) over [t3 or t2, +∞).
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE l (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    );
    run(
        &mut engine,
        "CREATE TABLE r (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    );
    run(&mut engine, "INSERT INTO l VALUES (1, 10)");
    let _t1 = engine.commit_clock().0;
    run(&mut engine, "INSERT INTO r VALUES (1, 20)");
    let t3 = engine.commit_clock().0;
    run(&mut engine, "UPDATE l SET v = 11 WHERE id = 1");
    let t2 = engine.commit_clock().0; // closes l.v=10 at t2, opens l.v=11
    let upper = t2 + 10;

    let rows = {
        let mut r = select(
            &mut engine,
            &format!("SELECT * FROM l JOIN r ON l.id = r.id FOR SYSTEM_TIME FROM 0 TO {upper}"),
        );
        r.sort();
        r
    };
    // `SELECT *` = [l.id, l.v, r.id, r.v, sys_from, sys_to].
    let row = |lv: i32, rv: i32, from: i64, to: Option<i64>| {
        vec![
            Some(enc(&ScalarValue::Int4(1))),
            Some(enc(&ScalarValue::Int4(lv))),
            Some(enc(&ScalarValue::Int4(1))),
            Some(enc(&ScalarValue::Int4(rv))),
            Some(enc(&ScalarValue::TimestampTz(from))),
            to.map(|t| enc(&ScalarValue::TimestampTz(t))),
        ]
    };
    // l.v=10 ∩ r.v=20 = [max(t1,t3), min(t2,+∞)) = [t3, t2); l.v=11 ∩ r.v=20 =
    // [max(t2,t3), +∞) = [t2, +∞) (open → NULL sys_to).
    let mut want = vec![row(10, 20, t3, Some(t2)), row(11, 20, t2, None)];
    want.sort();
    assert_eq!(rows, want, "intersected intervals, open sys_to = NULL");
}

#[test]
fn named_endpoints_order_by_and_count_compose_over_a_range_join() {
    // The rest of the SELECT surface composes ([STL-264] tail over the range-join
    // output): the endpoints are nameable, ORDER BY on one is legal, and an
    // aggregate folds the joined-interval rows.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE l (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    );
    run(
        &mut engine,
        "CREATE TABLE r (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    );
    run(&mut engine, "INSERT INTO r VALUES (1, 20)");
    run(&mut engine, "INSERT INTO l VALUES (1, 100)");
    run(&mut engine, "UPDATE l SET v = 200 WHERE id = 1");
    run(&mut engine, "UPDATE l SET v = 300 WHERE id = 1");
    let upper = engine.commit_clock().0 + 1;

    // Three l-versions each join r=20; ORDER BY sys_from DESC then read l.v.
    let ordered = select(
        &mut engine,
        &format!(
            "SELECT l.v, sys_from FROM l JOIN r ON l.id = r.id \
             FOR SYSTEM_TIME FROM 0 TO {upper} ORDER BY sys_from DESC"
        ),
    );
    let vs: Vec<i32> = ordered
        .iter()
        .map(|row| dec_i32(row[0].as_deref()))
        .collect();
    assert_eq!(
        vs,
        [300, 200, 100],
        "ORDER BY sys_from DESC over the range join"
    );

    // COUNT(*) over the same range join folds the three joined-interval rows.
    let count = select(
        &mut engine,
        &format!("SELECT count(*) FROM l JOIN r ON l.id = r.id FOR SYSTEM_TIME FROM 0 TO {upper}"),
    );
    assert_eq!(count.len(), 1);
    assert_eq!(
        dec_i64(count[0][0].as_deref()),
        3,
        "three joined-interval rows"
    );
}

#[test]
fn non_inner_two_table_range_join_matches_the_reference() {
    // [STL-348] the outer / filtering shapes over a range: a `LEFT` / `SEMI` / `ANTI`
    // join is the interval **difference** of each left version against its matched
    // right sub-intervals. Swept on both axes across the flush boundary and the dense
    // probe grid (commit ±1 on the system axis, every valid-window instant), against
    // the reference's *independent* breakpoint-sweep difference.
    let tables = ["l", "r"];
    let mut probes: u64 = 0;
    let mut rows_seen: u64 = 0;
    for step in [RefJoin::Left, RefJoin::Semi, RefJoin::Anti] {
        let steps = [step];
        for axis in [Axis::System, Axis::Valid] {
            for seed in 0..4u64 {
                for flush in [FlushMode::Never, FlushMode::Midway, FlushMode::Full] {
                    let (mut engine, commits) = build(seed, axis, flush, &tables);
                    let marks = match axis {
                        Axis::System => boundary_grid(&commits),
                        Axis::Valid => (0..=(VMAX + 1)).collect(),
                    };
                    for (i, &lo) in marks.iter().enumerate() {
                        for &hi in &marks[i..] {
                            for &closed in &[false, true] {
                                if !closed && lo >= hi {
                                    continue;
                                }
                                let sides = vec![
                                    side_rows(&mut engine, "l", axis, lo, hi, closed),
                                    side_rows(&mut engine, "r", axis, lo, hi, closed),
                                ];
                                let want = reference(
                                    &sides,
                                    &steps,
                                    lo,
                                    hi,
                                    closed,
                                    Combine::Intersect,
                                    Difference::Subtract,
                                );
                                let got =
                                    engine_join(&mut engine, &tables, &steps, axis, lo, hi, closed);
                                assert_eq!(
                                    got, want,
                                    "{step:?} {axis:?} seed {seed}, {flush:?}, [{lo},{hi}) closed={closed}"
                                );
                                rows_seen += u64::try_from(got.len()).expect("fits");
                                probes += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(
        rows_seen > 0,
        "every probe was empty — the workload joined nothing"
    );
    assert!(
        probes > 3_000,
        "differential probed only {probes} cells — widen the sweep"
    );
}

#[test]
fn mixed_shape_three_way_range_join_matches_the_reference() {
    // The interval difference composes through a left-deep chain ([STL-323]): an
    // outer / filtering step's fragmented (and, for `LEFT`, NULL-extended) rows feed
    // the next step, every step still keyed on the seed's `id`. A few representative
    // mixed chains, both axes, a lighter sweep.
    let tables = ["a", "b", "c"];
    let chains = [
        [RefJoin::Left, RefJoin::Inner],
        [RefJoin::Inner, RefJoin::Left],
        [RefJoin::Semi, RefJoin::Inner],
        [RefJoin::Inner, RefJoin::Anti],
        [RefJoin::Left, RefJoin::Anti],
    ];
    for steps in chains {
        for axis in [Axis::System, Axis::Valid] {
            for seed in 0..4u64 {
                let (mut engine, commits) = build(seed, axis, FlushMode::Midway, &tables);
                let marks = match axis {
                    Axis::System => boundary_grid(&commits),
                    Axis::Valid => (0..=(VMAX + 1)).collect(),
                };
                for (i, &lo) in marks.iter().enumerate() {
                    for &hi in marks[i..].iter().step_by(2) {
                        if lo >= hi {
                            continue;
                        }
                        let sides: Vec<Vec<SideRow>> = tables
                            .iter()
                            .map(|t| side_rows(&mut engine, t, axis, lo, hi, false))
                            .collect();
                        let want = reference(
                            &sides,
                            &steps,
                            lo,
                            hi,
                            false,
                            Combine::Intersect,
                            Difference::Subtract,
                        );
                        let got = engine_join(&mut engine, &tables, &steps, axis, lo, hi, false);
                        assert_eq!(got, want, "{steps:?} {axis:?} seed {seed}, [{lo},{hi})");
                    }
                }
            }
        }
    }
}

#[test]
fn the_oracle_has_teeth_all_or_nothing_difference() {
    // A reference whose interval difference is *all-or-nothing* per left row — a match
    // anywhere claims (`SEMI`) or clears (`LEFT` / `ANTI`) the **whole** left window,
    // ignoring that a mid-window match fragments it ([STL-348]'s named risk) — must be
    // caught by the same differential. The workload produces fragmenting matches, so
    // the buggy reference disagrees with the engine somewhere for each shape (and the
    // correct reference agrees everywhere, the sweep above).
    let tables = ["l", "r"];
    for step in [RefJoin::Left, RefJoin::Semi, RefJoin::Anti] {
        let steps = [step];
        let mut mismatch = false;
        for axis in [Axis::System, Axis::Valid] {
            for seed in 0..4u64 {
                let (mut engine, commits) = build(seed, axis, FlushMode::Never, &tables);
                let marks = match axis {
                    Axis::System => boundary_grid(&commits),
                    Axis::Valid => (0..=(VMAX + 1)).collect(),
                };
                for (i, &lo) in marks.iter().enumerate() {
                    for &hi in &marks[i..] {
                        if lo >= hi {
                            continue;
                        }
                        let sides = vec![
                            side_rows(&mut engine, "l", axis, lo, hi, false),
                            side_rows(&mut engine, "r", axis, lo, hi, false),
                        ];
                        let buggy = reference(
                            &sides,
                            &steps,
                            lo,
                            hi,
                            false,
                            Combine::Intersect,
                            Difference::AllOrNothing,
                        );
                        let got = engine_join(&mut engine, &tables, &steps, axis, lo, hi, false);
                        if got != buggy {
                            mismatch = true;
                        }
                    }
                }
            }
        }
        assert!(
            mismatch,
            "an all-or-nothing {step:?} difference must disagree with the engine"
        );
    }
}

#[test]
fn left_and_anti_fragment_a_left_version_around_a_mid_window_match() {
    // A focused, hand-checked system-axis timeline. l(id=1) v=10 is live over the
    // whole history [t1, +∞); r(id=1) v=20 exists only over [t2, t3) (inserted, then
    // deleted). The mid-history match splits l's window into:
    //   matched  (l=10, r=20)  over [t2, t3)
    //   gaps     (l=10, NULL)   over [t1, t2)  and  [t3, +∞)
    // `LEFT` keeps all three, `ANTI` keeps the two gaps, `SEMI` keeps the one match.
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE l (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    );
    run(
        &mut engine,
        "CREATE TABLE r (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    );
    run(&mut engine, "INSERT INTO l VALUES (1, 10)");
    let t1 = engine.commit_clock().0;
    run(&mut engine, "INSERT INTO r VALUES (1, 20)");
    let t2 = engine.commit_clock().0;
    run(&mut engine, "DELETE FROM r WHERE id = 1");
    let t3 = engine.commit_clock().0;
    let upper = t3 + 5;

    let id1 = Some(enc(&ScalarValue::Int4(1)));
    let l10 = Some(enc(&ScalarValue::Int4(10)));
    let r20 = Some(enc(&ScalarValue::Int4(20)));
    let ts = |t: i64| Some(enc(&ScalarValue::TimestampTz(t)));

    // LEFT: [l.id, l.v, r.id, r.v, sys_from, sys_to] — matched row + the two gaps.
    let mut left = select(
        &mut engine,
        &format!("SELECT * FROM l LEFT JOIN r ON l.id = r.id FOR SYSTEM_TIME FROM 0 TO {upper}"),
    );
    left.sort();
    let mut want_left = vec![
        vec![id1.clone(), l10.clone(), id1.clone(), r20, ts(t2), ts(t3)],
        vec![id1.clone(), l10.clone(), None, None, ts(t1), ts(t2)],
        vec![id1.clone(), l10.clone(), None, None, ts(t3), None],
    ];
    want_left.sort();
    assert_eq!(
        left, want_left,
        "LEFT fragments l around the mid-window match"
    );

    // ANTI: l's columns only — the two gap fragments.
    let mut anti = select(
        &mut engine,
        &format!(
            "SELECT l.id, l.v, sys_from, sys_to FROM l ANTI JOIN r ON l.id = r.id \
             FOR SYSTEM_TIME FROM 0 TO {upper}"
        ),
    );
    anti.sort();
    let mut want_anti = vec![
        vec![id1.clone(), l10.clone(), ts(t1), ts(t2)],
        vec![id1.clone(), l10.clone(), ts(t3), None],
    ];
    want_anti.sort();
    assert_eq!(anti, want_anti, "ANTI keeps the gap fragments");

    // SEMI: l's columns only — the single matched sub-interval.
    let semi = select(
        &mut engine,
        &format!(
            "SELECT l.id, l.v, sys_from, sys_to FROM l SEMI JOIN r ON l.id = r.id \
             FOR SYSTEM_TIME FROM 0 TO {upper}"
        ),
    );
    assert_eq!(
        semi,
        vec![vec![id1, l10, ts(t2), ts(t3)]],
        "SEMI keeps the matched sub-interval"
    );
}
