//! `EXPLAIN [ANALYZE]` over the wire, driven by the real `tokio-postgres` client
//! ([STL-260]).
//!
//! `EXPLAIN` renders the plan as the Postgres convention — one text column named
//! `QUERY PLAN`, one row per line — so `psql` and stock drivers display it with no
//! special-casing. This proves the column name, the one-line-per-row shape, and
//! the `ANALYZE` actuals render through the front end, not just in-process. Wall
//! time is `0us` because the test installs no metric time source.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The `QUERY PLAN` lines of a simple-query `EXPLAIN` reply, in order.
fn plan_lines(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                Some(row.get("QUERY PLAN").expect("QUERY PLAN column").to_owned())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_renders_query_plan_over_the_wire() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO account VALUES (1, 100), (2, 250), (3, 50)")
        .await
        .expect("insert rows");

    // Bare EXPLAIN: the plan shape, one line per row under the `QUERY PLAN` column.
    let plan = client
        .simple_query("EXPLAIN SELECT id, balance FROM account")
        .await
        .expect("EXPLAIN runs");
    let lines = plan_lines(&plan);
    let joined = lines.join("\n");
    assert!(
        joined.contains("Snapshot Scan on account"),
        "plan names the scan: {joined}"
    );
    assert!(joined.contains("Output: id, balance"), "{joined}");
    assert!(!joined.contains("actual rows"), "bare EXPLAIN: {joined}");

    // EXPLAIN ANALYZE: the same plan, now with measured actuals + a footer.
    let analyze = client
        .simple_query("EXPLAIN ANALYZE SELECT id FROM account WHERE balance >= 100")
        .await
        .expect("EXPLAIN ANALYZE runs");
    let lines = plan_lines(&analyze);
    let joined = lines.join("\n");
    assert!(
        joined.contains("Snapshot Scan on account  (actual rows=3 time=0us)"),
        "scan saw 3 rows: {joined}"
    );
    assert!(
        joined.contains("(actual rows=2 time=0us)"),
        "filter kept 2 rows: {joined}"
    );
    assert!(
        lines
            .last()
            .is_some_and(|l| l.starts_with("Execution Time:")),
        "footer present: {joined}"
    );

    // Extended (prepared) protocol: Parse / Bind / Describe / Execute for an
    // EXPLAIN must also render the `QUERY PLAN` column ([STL-260]).
    let rows = client
        .query("EXPLAIN SELECT id FROM account", &[])
        .await
        .expect("prepared EXPLAIN runs over the extended protocol");
    assert!(
        rows.iter().any(|r| r
            .get::<_, &str>("QUERY PLAN")
            .contains("Snapshot Scan on account")),
        "prepared EXPLAIN names the scan"
    );

    drop(client);
    let _ = driver.await;
}
