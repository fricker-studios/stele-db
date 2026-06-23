//! Read-your-own-writes over a temporal **range** scan correctness oracle
//! ([STL-343], [docs/06 §4]).
//!
//! A `SELECT … FOR { SYSTEM_TIME | VALID_TIME } { FROM a TO b | BETWEEN a AND b }`
//! read inside an open transaction must reflect the transaction's own buffered
//! `INSERT` / `UPDATE` / `DELETE` — the range-scan analogue of the single-table
//! ([STL-203] / [STL-223]) and join ([STL-325]) read-your-own-writes. The two axes
//! need *different* oracles, because a buffered write is observed at the pinned
//! snapshot `now` (the value `now()` folds to), and a committed write can never land
//! there — it commits strictly *after* `now`:
//!
//! * **System axis — reference model.** A buffered write opens a `[now, +∞)` version
//!   and closes the prior live one at `now`. A system-time range distinguishes those
//!   instants, so "stage vs commit" would diverge by exactly the one instant RYOW
//!   collapses; a committed buffer is therefore *not* a valid reference. Instead a
//!   deliberately-dumb in-process timeline (each write opens a version and closes the
//!   prior one — [STL-244]'s reference, extended with the buffer's `[now, +∞)` /
//!   close-at-`now` effect) is the model, and overlap is decided by intersecting two
//!   inclusive integer instant ranges — an independent derivation of the half-open /
//!   closed semantics, not the engine's predicate. Swept across seeds, three flush
//!   modes, and every boundary `±1`.
//!
//! * **Valid axis — two engines.** A valid range returns the system-live version per
//!   key (≤ 1), filtered on its `[valid_from, valid_to)`; its output carries no system
//!   instant, so staging a buffer and committing it produce the *same* answer. That is
//!   the [STL-223] / [STL-325] differential: one engine **stages** a random buffer in
//!   an open transaction (the overlay path), a second **commits** the identical buffer
//!   (the durable apply + committed read), and the in-transaction valid-range read must
//!   equal the committed one across a swept grid. Delta-only (no mid-history flush), so
//!   the reference's commit-side read-modify-write never hits the sealed-tier
//!   [STL-226] path, exactly as the STL-223 oracle is scoped.
//!
//! Two checks ride along on each axis, as in the single-table and join oracles:
//! another (auto-commit) reader never sees the buffer, and dropping the transaction
//! (`ROLLBACK`) leaves only the committed base. A teeth assertion proves the buffer
//! actually moved a range read, so neither differential is vacuously comparing two
//! unchanged reads.
//!
//! [docs/06 §4]: ../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart
//! [STL-203]: https://allegromusic.atlassian.net/browse/STL-203
//! [STL-223]: https://allegromusic.atlassian.net/browse/STL-223
//! [STL-244]: https://allegromusic.atlassian.net/browse/STL-244
//! [STL-325]: https://allegromusic.atlassian.net/browse/STL-325
//! [STL-343]: https://allegromusic.atlassian.net/browse/STL-343

use std::collections::{BTreeSet, HashMap};

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's `MonotonicClock` turns its readings into the
/// strictly increasing `1, 2, 3, …` commit instants the writes need, and — crucially
/// — a *read* never advances the mark, so a transaction's pinned snapshot equals the
/// last committed instant and [`SessionEngine::commit_clock`] reports it exactly.
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
    /// A uniform value in `0..n` (no `as` casts, so the truncation lints stay clean).
    fn below(&mut self, n: i64) -> i64 {
        let n = u64::try_from(n).expect("bound fits u64");
        i64::try_from(self.next() % n).expect("value fits i64")
    }
    const fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }
}

type Row = Vec<Option<Vec<u8>>>;

/// Encode a `ScalarValue` to its canonical wire bytes — the exact form a
/// `SelectResult` cell carries.
fn enc(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// Execute one statement on an engine (auto-commit), asserting it succeeds.
fn run(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    engine
        .execute(&parse(sql).expect("parse").remove(0))
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

/// The sorted rows of a `SELECT` outcome — sorted so the unspecified row order
/// (overlay vs committed scan, business-key vs scan order) compares as a multiset.
fn sorted(outcome: StatementOutcome) -> Vec<Row> {
    let StatementOutcome::Rows(SelectResult { mut rows, .. }) = outcome else {
        panic!("a SELECT must return rows");
    };
    rows.sort();
    rows
}

/// An auto-commit `SELECT`'s sorted rows.
fn auto_rows(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> Vec<Row> {
    sorted(
        engine
            .execute(&parse(sql).expect("parse").remove(0))
            .unwrap_or_else(|e| panic!("`{sql}`: {e}")),
    )
}

// ===========================================================================
// System axis — reference-model differential.
// ===========================================================================

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
    /// The version's last active integer instant: `sys_to - 1` for a closed version,
    /// or "infinity" for an open one — the inclusive upper end of `[sys_from, sys_to)`.
    fn last_active(&self) -> i64 {
        self.sys_to.map_or(i64::MAX, |to| to - 1)
    }
}

/// When to flush the delta tier, so the answer is asserted identical across the
/// delta/sealed boundary (history a range scan must reconstruct the same way whether
/// a version is still staged or sealed).
#[derive(Debug, Clone, Copy)]
enum FlushMode {
    Never,
    Midway,
    Full,
}

/// The reference's expected `SELECT *` rows for a system range over a timeline:
/// every version whose active instant set `[sys_from, sys_to)` intersects the query's,
/// projected `[id, a, sys_from, sys_to]`. Overlap is two inclusive integer ranges
/// intersecting — an independent derivation of the half-open / closed semantics.
fn reference_rows(versions: &[Ver], lo: i64, hi: i64, closed: bool) -> Vec<Row> {
    let query_hi = if closed { hi } else { hi - 1 };
    let mut rows: Vec<Row> = versions
        .iter()
        .filter(|v| {
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

/// Every boundary instant worth probing — each `sys_from` / `sys_to` and `±1`, so a
/// query edge lands exactly on, just before, and just after every version boundary
/// (including the buffer's `[now, +∞)` opens and close-at-`now`, since the overlaid
/// timeline carries them).
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

/// A seeded committed base on a fresh engine: the version timeline (`versions`) and
/// the index of each key's currently-open version (`open`), plus the engine itself.
/// Mirrors the [STL-244] system-range oracle's `build`.
fn system_base(
    seed: u64,
    flush: FlushMode,
) -> (
    SessionEngine<ZeroClock, MemDisk>,
    Vec<Ver>,
    HashMap<i32, usize>,
) {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
    );

    let mut versions: Vec<Ver> = Vec::new();
    let mut open: HashMap<i32, usize> = HashMap::new();
    let key_domain: Vec<i32> = (1..=4).collect();
    let value = |rng: &mut Rng| -> Option<i32> {
        if rng.one_in(4) {
            None
        } else {
            Some(i32::try_from(rng.index(5)).expect("small"))
        }
    };
    let sql_int = |v: Option<i32>| v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());

    let op_count = 6 + rng.index(7); // 6..=12 base writes
    let flush_at = op_count / 2;
    for step in 0..op_count {
        let id = key_domain[rng.index(key_domain.len())];
        let a = value(&mut rng);
        if open.contains_key(&id) {
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
    (engine, versions, open)
}

/// Run one seed of the system-range differential, returning whether the buffer ever
/// moved a probed read (the suite-level teeth check).
fn run_system_seed(seed: u64, flush: FlushMode) -> bool {
    let (mut engine, versions, open) = system_base(seed, flush);

    // The transaction's pinned snapshot — what `now()` folds to and where the buffer
    // is observed. `begin` pins `observe()`, which under `ZeroClock` is the high-water
    // mark `commit_clock` reports, and no read advances it.
    let s = engine.commit_clock().0;
    let mut txn = engine.begin();

    // Stage a random, well-formed buffer (tracking liveness so every staged write is
    // one the engine accepts), recording the keys it touched and its surviving live
    // state — the reference for the overlay's close-prior / open-`[s, +∞)` effect.
    let mut rng = Rng::new(seed ^ 0xA5A5_5A5A_0F0F_F0F0);
    let key_domain: Vec<i32> = (1..=4).collect();
    let mut buf_live: HashMap<i32, Option<i32>> = open
        .iter()
        .map(|(id, idx)| (*id, versions[*idx].a))
        .collect();
    let mut buffered: BTreeSet<i32> = BTreeSet::new();
    let value = |rng: &mut Rng| -> Option<i32> {
        if rng.one_in(4) {
            None
        } else {
            Some(5 + i32::try_from(rng.index(5)).expect("small"))
        }
    };
    let sql_int = |v: Option<i32>| v.map_or_else(|| "NULL".to_owned(), |n| n.to_string());
    let buffer_ops = 3 + rng.index(5); // 3..=7 staged writes
    for _ in 0..buffer_ops {
        let id = key_domain[rng.index(key_domain.len())];
        let a = value(&mut rng);
        let live = buf_live.contains_key(&id);
        let sql = if live && rng.one_in(3) {
            buf_live.remove(&id);
            format!("DELETE FROM t WHERE id = {id}")
        } else if live {
            buf_live.insert(id, a);
            format!("UPDATE t SET a = {} WHERE id = {id}", sql_int(a))
        } else {
            buf_live.insert(id, a);
            format!("INSERT INTO t VALUES ({id}, {})", sql_int(a))
        };
        engine
            .stage_dml(&parse(&sql).expect("parse").remove(0), &mut txn)
            .unwrap_or_else(|e| panic!("stage `{sql}`: {e}"));
        buffered.insert(id);
    }

    // Build the overlaid timeline the in-transaction read must match: close each
    // touched key's currently-open version at `s`, then open a `[s, +∞)` version for
    // each touched key the buffer leaves live.
    let mut overlaid = versions.clone();
    for id in &buffered {
        if let Some(idx) = open.get(id)
            && overlaid[*idx].sys_to.is_none()
        {
            overlaid[*idx].sys_to = Some(s);
        }
    }
    for (id, a) in &buf_live {
        if buffered.contains(id) {
            overlaid.push(Ver {
                id: *id,
                a: *a,
                sys_from: s,
                sys_to: None,
            });
        }
    }

    // The differential: every boundary probe, half-open and closed, must equal the
    // overlaid reference inside the transaction.
    let marks = boundary_instants(&overlaid);
    let mut moved = false;
    for (i, &lo) in marks.iter().enumerate() {
        for &hi in &marks[i..] {
            for closed in [false, true] {
                if !closed && lo >= hi {
                    continue; // half-open `FROM lo TO hi` needs lo < hi
                }
                let sql = if closed {
                    format!("SELECT * FROM t FOR SYSTEM_TIME BETWEEN {lo} AND {hi}")
                } else {
                    format!("SELECT * FROM t FOR SYSTEM_TIME FROM {lo} TO {hi}")
                };
                let got = sorted(
                    engine
                        .execute_in_txn(&parse(&sql).expect("parse").remove(0), &mut txn)
                        .unwrap_or_else(|e| panic!("`{sql}`: {e}")),
                );
                let want = reference_rows(&overlaid, lo, hi, closed);
                assert_eq!(got, want, "seed {seed}, {flush:?}: {sql}");
                moved |= want != reference_rows(&versions, lo, hi, closed);
            }
        }
    }

    // A wide probe covering the whole timeline and `s`: another (auto-commit) reader
    // sees only the committed base while the transaction is open …
    let top = marks.last().copied().unwrap_or(0) + 2;
    let wide = format!("SELECT * FROM t FOR SYSTEM_TIME FROM 0 TO {top}");
    assert_eq!(
        auto_rows(&mut engine, &wide),
        reference_rows(&versions, 0, top, false),
        "seed {seed}, {flush:?}: the open buffer leaked into another reader's range",
    );
    // … and after ROLLBACK only the committed base remains.
    drop(txn);
    assert_eq!(
        auto_rows(&mut engine, &wide),
        reference_rows(&versions, 0, top, false),
        "seed {seed}, {flush:?}: a rolled-back buffer left a trace in a range",
    );
    moved
}

#[test]
fn system_range_read_your_own_writes_matches_the_overlaid_timeline() {
    let mut moved = false;
    for seed in 0..16u64 {
        for flush in [FlushMode::Never, FlushMode::Midway, FlushMode::Full] {
            moved |= run_system_seed(seed, flush);
        }
    }
    assert!(
        moved,
        "the buffer never changed a system-range read — the differential never exercised the overlay",
    );
}

#[test]
fn system_range_overlay_is_interval_aware_at_now() {
    // The hand-checked crux of the ticket: a buffered write opens `[now, +∞)` and
    // closes the prior at `now`, so the *upper bound* decides whether it appears.
    let (mut engine, _versions, _open) = system_base(0, FlushMode::Never);
    let s = engine.commit_clock().0;
    let mut txn = engine.begin();
    // A fresh key, so its only version is the buffered `[s, +∞)` one.
    engine
        .stage_dml(
            &parse("INSERT INTO t VALUES (99, 7)")
                .expect("parse")
                .remove(0),
            &mut txn,
        )
        .expect("stage insert");

    // `BETWEEN … AND now()` (closed upper at `s`) observes the buffered version …
    let closed = sorted(
        engine
            .execute_in_txn(
                &parse(&format!(
                    "SELECT a FROM t FOR SYSTEM_TIME BETWEEN {s} AND {s} WHERE id = 99"
                ))
                .expect("parse")
                .remove(0),
                &mut txn,
            )
            .expect("closed range"),
    );
    assert_eq!(
        closed,
        vec![vec![Some(enc(&ScalarValue::Int4(7)))]],
        "BETWEEN … AND now() sees it"
    );

    // … while the half-open `FROM … TO now()` (exclusive upper at `s`) cannot.
    let half_open = sorted(
        engine
            .execute_in_txn(
                &parse(&format!(
                    "SELECT a FROM t FOR SYSTEM_TIME FROM 0 TO {s} WHERE id = 99"
                ))
                .expect("parse")
                .remove(0),
                &mut txn,
            )
            .expect("half-open range"),
    );
    assert!(
        half_open.is_empty(),
        "FROM … TO now() excludes the [now, +∞) buffered version"
    );
}

// ===========================================================================
// Valid axis — two-engine differential.
// ===========================================================================

const KEY_POOL: i64 = 3;
const VMAX: i64 = 8;

/// Run one statement to completion on an engine (auto-commit), discarding the outcome.
fn exec(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    engine
        .execute(&parse(sql).expect("parse").remove(0))
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

/// Generate one well-formed valid-time DML against a randomly chosen key, advancing
/// `alive` so the workload never inserts a live key (`KeyExists`); an absent-key
/// `UPDATE` / `DELETE` is a tolerated no-op. A fraction of updates keep the prior
/// value (period-only `SET`) to exercise the read-modify-write carry-over.
fn gen_valid_write(rng: &mut Rng, alive: &mut [bool], tick: i64) -> String {
    let k = rng.below(KEY_POOL);
    let ku = usize::try_from(k).expect("key fits usize");
    let ki = i32::try_from(k).expect("key fits i32");
    let val = i32::try_from(tick + 1).expect("value fits i32");
    let from = rng.below(VMAX);
    let open = rng.one_in(4);
    let to = if open {
        i64::MAX
    } else {
        from + 1 + rng.below(VMAX - from)
    };

    if alive[ku] && rng.one_in(2) {
        alive[ku] = false;
        format!("DELETE FROM vt WHERE id = {ki}")
    } else if alive[ku] {
        let keep_val = rng.one_in(3);
        let set = if keep_val && open {
            format!("SET vf = {from}")
        } else if keep_val {
            format!("SET vf = {from}, vt = {to}")
        } else if open {
            format!("SET val = {val}, vf = {from}")
        } else {
            format!("SET val = {val}, vf = {from}, vt = {to}")
        };
        format!("UPDATE vt {set} WHERE id = {ki}")
    } else {
        alive[ku] = true;
        if open {
            format!("INSERT INTO vt (id, val, vf) VALUES ({ki}, {val}, {from})")
        } else {
            format!("INSERT INTO vt VALUES ({ki}, {val}, {from}, {to})")
        }
    }
}

/// The valid-range reads probed per seed: a `SELECT *` (user columns + endpoints), a
/// bare projection (no endpoints), and an endpoint projection — half-open and closed,
/// over the whole `[0, VMAX]` grid.
fn valid_probes() -> Vec<String> {
    let mut reads = Vec::new();
    for shape in [
        "SELECT *",
        "SELECT id, val",
        "SELECT id, val, valid_from, valid_to",
    ] {
        for a in 0..=VMAX {
            for b in 0..=VMAX + 1 {
                if a < b {
                    reads.push(format!("{shape} FROM vt FOR VALID_TIME FROM {a} TO {b}"));
                }
                if a <= b {
                    reads.push(format!(
                        "{shape} FROM vt FOR VALID_TIME BETWEEN {a} AND {b}"
                    ));
                }
            }
        }
    }
    reads
}

/// Run one seed of the valid-range differential, returning whether the buffer moved a
/// read (the teeth probe).
fn run_valid_seed(seed: u64) -> bool {
    let mut rng = Rng::new(seed);
    let mut sut = SessionEngine::open(MemDisk::new(), ZeroClock);
    let mut reference = SessionEngine::open(MemDisk::new(), ZeroClock);
    let ddl = "CREATE TABLE vt (id INT PRIMARY KEY, val INT, vf TIMESTAMP, vt TIMESTAMP) \
               WITH SYSTEM VERSIONING VALID TIME (vf, vt)";
    exec(&mut sut, ddl);
    exec(&mut reference, ddl);

    // A committed base both engines share, applied identically (auto-commit, no flush
    // — keeps the reference's commit-side RMW off the sealed-tier path, [STL-226]).
    let mut alive = vec![false; usize::try_from(KEY_POOL).expect("fits")];
    let mut tick: i64 = 0;
    let base_ops = 3 + rng.below(4);
    for _ in 0..base_ops {
        let sql = gen_valid_write(&mut rng, &mut alive, tick);
        tick += 1;
        exec(&mut sut, &sql);
        exec(&mut reference, &sql);
    }
    // The committed-base plain read another reader keeps seeing while the txn is open.
    let base_plain = auto_rows(
        &mut sut,
        "SELECT id, val FROM vt FOR VALID_TIME FROM 0 TO 1000000",
    );

    // Build a random buffer: STAGE it on `sut`, COMMIT it on `reference`.
    let mut txn = sut.begin();
    let buffer_ops = 3 + rng.below(5);
    for _ in 0..buffer_ops {
        let sql = gen_valid_write(&mut rng, &mut alive, tick);
        tick += 1;
        sut.stage_dml(&parse(&sql).expect("parse").remove(0), &mut txn)
            .expect("stage on sut");
        exec(&mut reference, &sql);
    }

    // The differential: the in-transaction valid-range overlay equals the committed
    // valid-range read for every probed read shape and grid cell.
    for sql in valid_probes() {
        let got = sorted(
            sut.execute_in_txn(&parse(&sql).expect("parse").remove(0), &mut txn)
                .unwrap_or_else(|e| panic!("overlay `{sql}`: {e}")),
        );
        let want = sorted(
            reference
                .execute(&parse(&sql).expect("parse").remove(0))
                .unwrap_or_else(|e| panic!("commit `{sql}`: {e}")),
        );
        assert_eq!(
            got, want,
            "seed {seed}: overlay vs commit diverged on `{sql}`"
        );
    }

    // Teeth: did the buffer actually move the plain read?
    let overlay_plain = sorted(
        sut.execute_in_txn(
            &parse("SELECT id, val FROM vt FOR VALID_TIME FROM 0 TO 1000000")
                .expect("parse")
                .remove(0),
            &mut txn,
        )
        .expect("overlay plain"),
    );
    let moved = overlay_plain != base_plain;

    // Another (auto-commit) reader on the staging engine never sees the buffer …
    assert_eq!(
        auto_rows(
            &mut sut,
            "SELECT id, val FROM vt FOR VALID_TIME FROM 0 TO 1000000"
        ),
        base_plain,
        "seed {seed}: the open buffer leaked into another reader's valid range",
    );
    // … and ROLLBACK (drop the buffer) leaves only the committed base.
    drop(txn);
    assert_eq!(
        auto_rows(
            &mut sut,
            "SELECT id, val FROM vt FOR VALID_TIME FROM 0 TO 1000000"
        ),
        base_plain,
        "seed {seed}: a rolled-back buffer left a trace in a valid range",
    );
    moved
}

#[test]
fn valid_range_read_your_own_writes_matches_committing_the_buffer() {
    let mut moved = false;
    for seed in 0..40u64 {
        moved |= run_valid_seed(seed);
    }
    assert!(
        moved,
        "the buffer never changed a valid-range read — the differential never exercised the overlay",
    );
}
