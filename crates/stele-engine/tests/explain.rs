//! `EXPLAIN [ANALYZE]` end-to-end through the engine ([STL-260]).
//!
//! `EXPLAIN` renders the bound plan as a `QUERY PLAN` row set; `EXPLAIN ANALYZE`
//! executes the statement for real and annotates each operator with true rows and
//! wall time. These pin the rendered plan *shape* (deterministic) and the presence
//! and correctness of the analyzed actuals — wall time is `0us` here because the
//! test harness installs no metric time source (the determinism guarantee
//! `EXPLAIN ANALYZE` relies on under test), so the lines are stable.

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

    /// The `QUERY PLAN` lines an `EXPLAIN` produced.
    fn plan(&mut self, sql: &str) -> Vec<String> {
        let result = self.select(sql);
        // Note: don't `{:?}` the engine result type in the message — CodeQL's
        // cleartext-logging rule taints its type graph (it reaches `ScalarValue`),
        // a known false positive on Debug-in-test-messages. `assert_eq!` prints the
        // count on failure anyway.
        assert_eq!(result.columns.len(), 1, "EXPLAIN carries one column");
        assert_eq!(result.columns[0].0, "QUERY PLAN");
        assert!(
            result.stats.is_none(),
            "EXPLAIN's own output is synthetic and carries no stats footer"
        );
        result
            .rows
            .iter()
            .map(|row| {
                let cell = row[0].as_ref().expect("plan line is never NULL");
                String::from_utf8(cell.clone()).expect("plan line is UTF-8")
            })
            .collect()
    }
}

fn account(db: &mut Db) {
    db.run("CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING");
    db.run("INSERT INTO account VALUES (1, 100), (2, 250), (3, 50)");
}

#[test]
fn explain_renders_a_scan_pipeline_with_no_actuals() {
    let mut db = Db::fresh();
    account(&mut db);

    let plan = db.plan("EXPLAIN SELECT id, balance FROM account");
    let joined = plan.join("\n");
    assert!(joined.contains("Snapshot Scan on account"), "{joined}");
    assert!(joined.contains("Explode Payload"), "{joined}");
    assert!(joined.contains("Output: id, balance"), "{joined}");
    assert!(joined.contains("Prune: sys_from <="), "{joined}");
    // A bare EXPLAIN never executes, so it carries no measured actuals or footer.
    assert!(
        !joined.contains("actual rows"),
        "bare EXPLAIN shows no actuals: {joined}"
    );
    assert!(!joined.contains("Execution Time"), "{joined}");
}

#[test]
fn explain_shows_a_value_filter_condition() {
    let mut db = Db::fresh();
    account(&mut db);

    let joined = db
        .plan("EXPLAIN SELECT id FROM account WHERE balance > 100")
        .join("\n");
    assert!(joined.contains("Filter"), "{joined}");
    assert!(joined.contains("Cond: balance > 100"), "{joined}");
}

#[test]
fn explain_names_the_chosen_index() {
    let mut db = Db::fresh();
    account(&mut db);
    db.run("CREATE INDEX i_balance ON account (balance)");

    let joined = db
        .plan("EXPLAIN SELECT id FROM account WHERE balance = 100")
        .join("\n");
    assert!(
        joined.contains("Index: i_balance (btree, =)"),
        "the equality read should pick the b-tree index: {joined}"
    );
}

#[test]
fn explain_shows_aggregate_group_key() {
    let mut db = Db::fresh();
    account(&mut db);

    let joined = db
        .plan("EXPLAIN SELECT balance, count(*) FROM account GROUP BY balance")
        .join("\n");
    assert!(joined.contains("Aggregate"), "{joined}");
    assert!(joined.contains("Group Key: balance"), "{joined}");
    assert!(joined.contains("Aggregates: count(*)"), "{joined}");
}

#[test]
fn explain_analyze_annotates_true_rows_and_a_footer() {
    let mut db = Db::fresh();
    account(&mut db);

    let plan = db.plan("EXPLAIN ANALYZE SELECT id FROM account WHERE balance >= 100");
    let joined = plan.join("\n");
    // Two of the three rows have balance >= 100 (100 and 250).
    assert!(
        joined.contains("Filter  (actual rows=2 time=0us)"),
        "filter keeps 2 rows: {joined}"
    );
    // The scan saw all three rows.
    assert!(
        joined.contains("Snapshot Scan on account  (actual rows=3 time=0us)"),
        "scan saw 3 rows: {joined}"
    );
    // The scan node carries a prune-accounting buffers line under ANALYZE.
    assert!(
        joined.contains("segments"),
        "buffers line present: {joined}"
    );
    // The whole-statement footer.
    assert!(
        plan.last()
            .is_some_and(|l| l.starts_with("Execution Time:")),
        "footer present: {joined}"
    );
}

#[test]
fn explain_analyze_executes_dml_for_real() {
    let mut db = Db::fresh();
    account(&mut db);

    let joined = db
        .plan("EXPLAIN ANALYZE INSERT INTO account VALUES (4, 400)")
        .join("\n");
    assert!(joined.contains("Insert on account"), "{joined}");

    // ANALYZE ran the insert for real — the row is now visible.
    let rows = db
        .select("SELECT id, balance FROM account WHERE id = 4")
        .rows;
    assert_eq!(rows.len(), 1, "EXPLAIN ANALYZE INSERT committed the row");
}

#[test]
fn explain_of_a_plain_insert_does_not_execute() {
    let mut db = Db::fresh();
    account(&mut db);

    let joined = db
        .plan("EXPLAIN INSERT INTO account VALUES (9, 900)")
        .join("\n");
    assert!(joined.contains("Insert on account"), "{joined}");

    // A bare EXPLAIN is plan-only: the row was *not* inserted.
    let rows = db.select("SELECT id FROM account WHERE id = 9").rows;
    assert!(rows.is_empty(), "bare EXPLAIN must not execute the insert");
}

#[test]
fn explain_rejects_a_ddl_inner() {
    let mut db = Db::fresh();
    let stmt = parse("EXPLAIN CREATE TABLE t (id INT) WITH SYSTEM VERSIONING")
        .expect("parse")
        .remove(0);
    let err = db.engine.execute(&stmt).expect_err("DDL has no query plan");
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("explain"), "error names EXPLAIN: {err}");
    assert!(
        msg.contains("ddl"),
        "error is DDL-specific, not the generic SELECT/DML message: {err}"
    );
}

#[test]
fn explain_analyze_scan_narrows_under_an_index() {
    let mut db = Db::fresh();
    account(&mut db);
    db.run("CREATE INDEX i_balance ON account (balance)");
    db.run("FLUSH");

    // With the index, the scan resolves only the candidates the index window
    // admits, so its actual row count is below the table's total — the plan makes
    // the index's effect observable, which is the point of EXPLAIN ANALYZE.
    let joined = db
        .plan("EXPLAIN ANALYZE SELECT id FROM account WHERE balance >= 100")
        .join("\n");
    assert!(
        joined.contains("Index: i_balance (btree, >=)"),
        "the range read picks the b-tree index: {joined}"
    );
    assert!(
        joined.contains("Snapshot Scan on account  (actual rows=2 time=0us)"),
        "the index narrowed the scan to the 2 matching rows: {joined}"
    );
}
