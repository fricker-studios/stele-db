//! Read-your-own-writes over a **join** correctness oracle ([STL-325]).
//!
//! A `SELECT … JOIN …` inside an open transaction must read the transaction's own
//! buffered `INSERT` / `UPDATE` / `DELETE` on *either* side — exactly as a
//! single-table read does ([STL-203] / [STL-223]) — and a `FOR VALID_TIME AS OF v`
//! join in the block must overlay the buffer and then pin the valid axis. This is the
//! [required correctness oracle](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)
//! for that behavior, in the differential shape STL-223 established: one engine
//! **stages** a random buffer in an open transaction (the overlay path the join now
//! threads), a second engine **commits** the identical buffer via auto-commit (the
//! durable apply + committed-read path), and the in-transaction join read must equal
//! the committed join read — plain and across a swept valid grid, for an `INNER` and
//! a `LEFT` join.
//!
//! Two checks ride along, as in the single-table oracle: another (auto-commit) reader
//! on the staging engine never sees the buffer, and dropping the transaction
//! (`ROLLBACK`) leaves only the committed base. A teeth assertion proves the buffer
//! actually moved a join read, so the differential is not vacuously comparing two
//! unchanged reads.
//!
//! The reference engine's committed both-axes join is itself the STL-243 oracle's
//! subject, so agreement here isolates the *one* new thing: each join side's scan is
//! correctly overlaid with the buffer before the hash join.
//!
//! [STL-325]: https://allegromusic.atlassian.net/browse/STL-325
//! [STL-203]: https://allegromusic.atlassian.net/browse/STL-203
//! [STL-223]: https://allegromusic.atlassian.net/browse/STL-223
//! [STL-243]: https://allegromusic.atlassian.net/browse/STL-243

use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's `MonotonicClock` turns its readings into the
/// strictly increasing commit instants the writes need, deterministically.
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
    /// A uniform value in `0..n` (no `as` casts, so the truncation lints stay clean).
    fn below(&mut self, n: i64) -> i64 {
        let n = u64::try_from(n).expect("bound fits u64");
        i64::try_from(self.next() % n).expect("value fits i64")
    }
    /// True with probability `1/n`.
    const fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }
}

type Row = Vec<Option<Vec<u8>>>;

const KEY_POOL: i64 = 3;
const VMAX: i64 = 10;
const SEEDS: u64 = 48;
/// The valid-time tables joined on their shared key domain. Three tables exercise an
/// N-way left-deep chain ([STL-323]), so the overlay must reach the *intermediate*
/// `join_step` (the middle input), not just the seed and the final step.
const TABLES: [&str; 3] = ["a", "b", "c"];
/// The inner join read whose buffered-vs-committed agreement is the core check (and
/// the "did the buffer move a read?" teeth probe).
const JOIN_PLAIN: &str = "SELECT a.id, a.val, b.val FROM a JOIN b ON a.id = b.id";

/// Both sides share this shape: a key, one value column, and the valid-time period.
fn create_sql(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, val INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)"
    )
}

/// Run a statement to completion on an engine (auto-commit), discarding the outcome.
fn exec(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    engine
        .execute(&parse(sql).expect("parse").remove(0))
        .expect("auto-commit statement");
}

/// A `SELECT`'s rows, sorted so the join order (unspecified) is compared as a
/// multiset — the overlaid side comes back business-key ordered, the committed side
/// in scan order, so only a sort makes the two engines comparable.
fn sorted(outcome: StatementOutcome) -> Vec<Row> {
    let StatementOutcome::Rows(SelectResult { mut rows, .. }) = outcome else {
        panic!("a SELECT must return rows");
    };
    rows.sort();
    rows
}

/// Generate one well-formed valid-time DML against a randomly chosen side, advancing
/// `alive` (which keys hold a live version, per table) so the workload never inserts
/// a live key (`KeyExists`) — an absent-key `UPDATE` / `DELETE` is a tolerated 0-row
/// no-op. `tick` distinguishes successive values; a fraction of updates keep the
/// prior value (period-only `SET`) to exercise the read-modify-write carry-over.
fn gen_write(rng: &mut Rng, alive: &mut [Vec<bool>], tick: i64) -> String {
    let t = usize::try_from(rng.below(i64::try_from(TABLES.len()).expect("fits")))
        .expect("table index fits");
    let table = TABLES[t];
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

    if alive[t][ku] && rng.one_in(2) {
        alive[t][ku] = false;
        format!("DELETE FROM {table} WHERE id = {ki}")
    } else if alive[t][ku] {
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
        format!("UPDATE {table} {set} WHERE id = {ki}")
    } else {
        alive[t][ku] = true;
        if open {
            format!("INSERT INTO {table} (id, val, vf) VALUES ({ki}, {val}, {from})")
        } else {
            format!("INSERT INTO {table} VALUES ({ki}, {val}, {from}, {to})")
        }
    }
}

/// Run one seed of the differential, returning `(join_probes, rows_seen,
/// buffer_moved_a_join_read)`. A random committed base is applied identically to a
/// staging engine and a reference; a random buffer is then STAGED on the staging
/// engine (the join overlay path) and COMMITTED on the reference (durable apply +
/// committed join). A swept valid grid over an `INNER` and a `LEFT` join, plus the
/// plain reads, must agree at every probe.
fn run_seed(seed: u64) -> (u64, u64, bool) {
    let mut rng = Rng::new(seed);
    let mut sut = SessionEngine::open(MemDisk::new(), ZeroClock);
    let mut reference = SessionEngine::open(MemDisk::new(), ZeroClock);
    for table in TABLES {
        let ddl = create_sql(table);
        exec(&mut sut, &ddl);
        exec(&mut reference, &ddl);
    }

    // A committed base both engines share, applied identically (auto-commit).
    let mut alive = vec![vec![false; usize::try_from(KEY_POOL).expect("fits")]; TABLES.len()];
    let mut tick: i64 = 0;
    let base_ops = 3 + rng.below(4);
    for _ in 0..base_ops {
        let sql = gen_write(&mut rng, &mut alive, tick);
        tick += 1;
        exec(&mut sut, &sql);
        exec(&mut reference, &sql);
    }
    // The committed-base inner join another reader must keep seeing while the
    // transaction is open and after it rolls back.
    let base_join = sorted(
        sut.execute(&parse(JOIN_PLAIN).expect("parse").remove(0))
            .expect("base join"),
    );

    // Build a random buffer: STAGE it on `sut`, COMMIT it on `reference`.
    let mut txn = sut.begin();
    let buffer_ops = 4 + rng.below(6);
    for _ in 0..buffer_ops {
        let sql = gen_write(&mut rng, &mut alive, tick);
        tick += 1;
        sut.stage_dml(&parse(&sql).expect("parse").remove(0), &mut txn)
            .expect("stage on sut");
        exec(&mut reference, &sql);
    }

    // The differential: the in-transaction join overlay equals the committed join,
    // plain and across a swept valid grid. Two-table `INNER` / `LEFT` joins exercise
    // the seed + final `join_step` overlay; the three-table left-deep chains ([STL-323])
    // add the *intermediate* `join_step` (the middle input `b`), so the overlay must
    // reach every input of the chain, not just the ends.
    let mut probes: u64 = 0;
    let mut rows_seen: u64 = 0;
    for plain in [
        "SELECT a.id, a.val, b.val FROM a JOIN b ON a.id = b.id",
        "SELECT a.id, a.val, b.val FROM a LEFT JOIN b ON a.id = b.id",
        "SELECT a.id, a.val, b.val, c.val FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
        "SELECT a.id, a.val, b.val, c.val FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id",
    ] {
        let plain = plain.to_string();
        let mut reads = vec![plain.clone()];
        reads.extend((0..=VMAX).map(|v| format!("{plain} FOR VALID_TIME AS OF {v}")));
        for sql in reads {
            let got = sorted(
                sut.execute_in_txn(&parse(&sql).expect("parse").remove(0), &mut txn)
                    .expect("overlay join read"),
            );
            let want = sorted(
                reference
                    .execute(&parse(&sql).expect("parse").remove(0))
                    .expect("committed join read"),
            );
            assert_eq!(
                got, want,
                "seed {seed}: overlay vs commit diverged on `{sql}`"
            );
            rows_seen += u64::try_from(got.len()).expect("fits");
            probes += 1;
        }
    }

    // The teeth: did the buffer actually move the inner join read?
    let overlay_join = sorted(
        sut.execute_in_txn(&parse(JOIN_PLAIN).expect("parse").remove(0), &mut txn)
            .expect("overlay inner join"),
    );
    let moved = overlay_join != base_join;

    // Another (auto-commit) reader on the staging engine never sees the buffer.
    assert_eq!(
        sorted(
            sut.execute(&parse(JOIN_PLAIN).expect("parse").remove(0))
                .expect("auto-commit join mid-txn")
        ),
        base_join,
        "seed {seed}: the open transaction's buffer leaked into a join by another reader",
    );
    // ROLLBACK (drop the buffer): only the committed base remains.
    drop(txn);
    assert_eq!(
        sorted(
            sut.execute(&parse(JOIN_PLAIN).expect("parse").remove(0))
                .expect("join after rollback")
        ),
        base_join,
        "seed {seed}: a rolled-back buffer left a trace in a join",
    );
    (probes, rows_seen, moved)
}

#[test]
fn join_read_your_own_writes_matches_committing_the_buffer() {
    let mut probes: u64 = 0;
    let mut rows_seen: u64 = 0;
    let mut buffer_moved_a_read = false;
    for seed in 0..SEEDS {
        let (p, r, moved) = run_seed(seed);
        probes += p;
        rows_seen += r;
        buffer_moved_a_read |= moved;
    }
    assert!(
        rows_seen > 0,
        "every join probe was empty — the workload joined nothing"
    );
    assert!(
        probes > 1_000,
        "differential probed only {probes} join cells — widen the sweep"
    );
    assert!(
        buffer_moved_a_read,
        "the buffer never changed a join read — the differential never exercised the overlay",
    );
}
