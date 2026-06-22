//! The per-query execution stats the engine threads into `SelectResult` for the
//! "see the engine" footer ([STL-201]).
//!
//! These pin the *shape* of what the footer reports: a committed-only table
//! `SELECT` carries the scan's `ScanStats` (segment + row-group accounting), an
//! `AS OF` read marks itself time-travel, a join carries **both sides' scans
//! summed** ([STL-318]), and a read-your-own-writes overlay or a provenance read
//! carries its **base scan** ([STL-318]). Only a read with no single scan to
//! account for — a join whose side is a materialized CTE / derived table — carries
//! no stats, so the footer is suppressed. The exact `ScanStats` partition is
//! pinned at the scan layer; this is the end-to-end engine proof that it survives
//! to the result.
//!
//! [STL-201]: https://allegromusic.atlassian.net/browse/STL-201
//! [STL-318]: https://allegromusic.atlassian.net/browse/STL-318

use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

#[derive(Clone)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

struct Db {
    engine: SessionEngine<ZeroClock, MemDisk>,
}

impl Db {
    fn fresh() -> Self {
        Self {
            engine: SessionEngine::open(MemDisk::new(), ZeroClock),
        }
    }

    fn run(&mut self, sql: &str) -> StatementOutcome {
        let stmt = parse(sql).expect("parse").remove(0);
        self.engine
            .execute(&stmt)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn select(&mut self, sql: &str) -> SelectResult {
        let StatementOutcome::Rows(result) = self.run(sql) else {
            panic!("`{sql}` must return rows");
        };
        result
    }
}

#[test]
fn a_delta_only_select_reports_no_sealed_segments() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    db.run("INSERT INTO t VALUES (1, 10), (2, 20)");

    let result = db.select("SELECT id, v FROM t");
    let stats = result
        .stats
        .expect("a plain table SELECT carries scan stats");
    assert_eq!(stats.rows, 2);
    // Nothing was flushed, so every row was served from the in-memory delta: no
    // sealed segment was offered to the scan.
    assert_eq!(stats.segments_total, 0);
    assert_eq!(stats.segments_scanned, 0);
    assert!(!stats.time_travel, "a live read is not time-travel");
}

#[test]
fn a_flushed_select_accounts_segments_and_a_point_probe_prunes() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE t (id TEXT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    // Three sealed segments, one key each — their business-key zone maps do not
    // overlap, so a point probe for one key is provably absent from the other two.
    for seg in 0..3 {
        db.run(&format!("INSERT INTO t VALUES ('k{seg}', {seg})"));
        db.run("FLUSH");
    }

    // A full scan reads every sealed segment.
    let all = db.select("SELECT id, v FROM t");
    let stats = all.stats.expect("stats");
    assert_eq!(stats.rows, 3);
    assert_eq!(stats.segments_total, 3);
    assert_eq!(stats.segments_scanned, 3);
    assert_eq!(stats.segments_pruned(), 0, "a full scan prunes nothing");

    // A point read on the indexed business key prunes the segments that cannot
    // hold it — the footer's "see the engine" payload.
    let one = db.select("SELECT v FROM t WHERE id = 'k1'");
    let stats = one.stats.expect("stats");
    assert_eq!(stats.rows, 1);
    assert_eq!(stats.segments_total, 3);
    assert!(
        stats.segments_pruned() >= 1,
        "a point probe must skip at least one sealed segment, pruned {}",
        stats.segments_pruned(),
    );
    assert_eq!(
        stats.segments_scanned + stats.segments_pruned(),
        stats.segments_total,
        "every segment is either scanned or pruned",
    );
}

#[test]
fn an_as_of_read_marks_the_footer_time_travel() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    db.run("INSERT INTO t VALUES (1, 10)");

    let live = db.select("SELECT id, v FROM t");
    assert!(!live.stats.expect("stats").time_travel);

    // The flag tracks the explicit `FOR SYSTEM_TIME AS OF` clause, not the
    // resolved instant — a system-axis time-travel read reads "snapshot @ …".
    let past = db.select("SELECT id, v FROM t FOR SYSTEM_TIME AS OF now()");
    assert!(
        past.stats.expect("stats").time_travel,
        "an explicit system-time AS OF marks the footer time-travel",
    );
}

#[test]
fn a_join_reports_both_sides_summed_scan_stats() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE a (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    db.run("CREATE TABLE b (id INT PRIMARY KEY, w INT) WITH SYSTEM VERSIONING");
    // `a` is flushed into two sealed segments, `b` into one — so the join's summed
    // segment accounting is provably the two scans added, not one side alone.
    db.run("INSERT INTO a VALUES (1, 10)");
    db.run("FLUSH");
    db.run("INSERT INTO a VALUES (2, 20)");
    db.run("FLUSH");
    db.run("INSERT INTO b VALUES (1, 100)");
    db.run("FLUSH");

    // A join reads two scans; the footer now reports their summed `ScanStats`
    // ([STL-318]). A join pushes no per-side `WHERE` down, so each side full-scans:
    // every segment scanned, none pruned, summed across both sides.
    let joined = db.select("SELECT a.v, b.w FROM a JOIN b ON a.id = b.id");
    assert_eq!(joined.rows.len(), 1);
    let stats = joined
        .stats
        .expect("a join now carries both sides' summed scan stats");
    assert_eq!(stats.segments_total, 3, "two a-segments + one b-segment");
    assert_eq!(stats.segments_scanned, 3, "a join full-scans both sides");
    assert_eq!(
        stats.segments_pruned(),
        0,
        "no per-side predicate is pushed down"
    );
    assert_eq!(
        stats.rows, 1,
        "the footer reports the join's returned row count"
    );
    assert!(!stats.time_travel, "a live join is not time-travel");
}

#[test]
fn a_join_over_a_materialized_side_suppresses_stats() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE a (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    db.run("CREATE TABLE b (id INT PRIMARY KEY, w INT) WITH SYSTEM VERSIONING");
    db.run("INSERT INTO a VALUES (1, 10)");
    db.run("INSERT INTO b VALUES (1, 100)");

    // One join side is a materialized CTE — its storage reads were accounted when
    // it was materialized, not by the join — so the join has no single scan to
    // report and the footer is suppressed (rather than under-reporting half the read).
    let joined =
        db.select("WITH c AS (SELECT id, v FROM a) SELECT c.v, b.w FROM c JOIN b ON c.id = b.id");
    assert_eq!(joined.rows.len(), 1);
    assert!(
        joined.stats.is_none(),
        "a join with a materialized (CTE) side carries no single scan accounting",
    );
}

#[test]
fn a_read_your_own_writes_read_reports_its_base_scan() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    // Two sealed segments in the committed base the transaction reads over.
    db.run("INSERT INTO t VALUES (1, 10)");
    db.run("FLUSH");
    db.run("INSERT INTO t VALUES (2, 20)");
    db.run("FLUSH");

    // Inside a transaction, buffer a write and read the table back. The overlay path
    // takes an *unfiltered* base scan (the `WHERE` is re-applied after the overlay,
    // never pushed down), so the footer reports that full base scan — every committed
    // segment scanned, nothing pruned — over the post-overlay returned row count.
    let mut txn = db.engine.begin();
    let insert = parse("INSERT INTO t VALUES (3, 30)")
        .expect("parse")
        .remove(0);
    db.engine
        .execute_in_txn(&insert, &mut txn)
        .expect("buffered insert");
    let select = parse("SELECT id, v FROM t WHERE id = 1")
        .expect("parse")
        .remove(0);
    let StatementOutcome::Rows(result) = db
        .engine
        .execute_in_txn(&select, &mut txn)
        .expect("read-your-own-writes select")
    else {
        panic!("a SELECT must return rows");
    };

    let stats = result
        .stats
        .expect("a read-your-own-writes read now reports its base scan");
    assert_eq!(stats.segments_total, 2);
    assert_eq!(
        stats.segments_scanned, 2,
        "the overlay base scan reads every committed segment",
    );
    assert_eq!(
        stats.segments_pruned(),
        0,
        "the overlay path pushes no predicate down to prune with",
    );
    assert_eq!(
        stats.rows, 1,
        "WHERE id = 1 returns one row after the overlay"
    );
}

#[test]
fn a_provenance_read_reports_its_scan() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    db.run("INSERT INTO t VALUES (1, 10)");
    db.run("FLUSH");
    db.run("INSERT INTO t VALUES (2, 20)");
    db.run("FLUSH");

    // Naming a provenance pseudo-column ([STL-247]) takes the provenance scan path,
    // a full unfiltered scan; it now threads that scan's accounting up rather than
    // suppressing the footer.
    let result = db.select("SELECT id, _stele_txn_id FROM t");
    let stats = result
        .stats
        .expect("a provenance read now reports its scan");
    assert_eq!(stats.rows, 2);
    assert_eq!(stats.segments_total, 2);
    assert_eq!(stats.segments_scanned, 2, "the provenance path full-scans");
    assert_eq!(stats.segments_pruned(), 0);
}
