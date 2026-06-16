//! The per-query execution stats the engine threads into `SelectResult` for the
//! "see the engine" footer ([STL-201]).
//!
//! These pin the *shape* of what the footer reports: a committed-only table
//! `SELECT` carries the scan's `ScanStats` (segment + row-group accounting), an
//! `AS OF` read marks itself time-travel, and a read with no single scan to
//! account for (a join) carries no stats so the footer is suppressed. The exact
//! `ScanStats` partition is pinned at the scan layer; this is the end-to-end
//! engine proof that it survives to the result.
//!
//! [STL-201]: https://allegromusic.atlassian.net/browse/STL-201

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
fn a_join_suppresses_stats_so_its_footer_is_hidden() {
    let mut db = Db::fresh();
    db.run("CREATE TABLE a (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    db.run("CREATE TABLE b (id INT PRIMARY KEY, w INT) WITH SYSTEM VERSIONING");
    db.run("INSERT INTO a VALUES (1, 10)");
    db.run("INSERT INTO b VALUES (1, 100)");

    // A join reads two scans; per-query accounting over a join is a follow-up, so
    // it carries no stats and the wire layer suppresses the footer.
    let joined = db.select("SELECT a.v, b.w FROM a JOIN b ON a.id = b.id");
    assert_eq!(joined.rows.len(), 1);
    assert!(
        joined.stats.is_none(),
        "a join read carries no scan stats (footer suppressed)",
    );
}
