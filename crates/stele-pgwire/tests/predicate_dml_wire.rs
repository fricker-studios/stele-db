//! Predicate-driven `UPDATE` / `DELETE` over the Postgres wire (STL-229).
//!
//! STL-229 lifts the v0.1 `WHERE <key> = <literal>` restriction: an `UPDATE` /
//! `DELETE` accepts the same `WHERE` predicates the SELECT path evaluates —
//! including no `WHERE` at all (whole-table) — via a scan-then-write plan. A
//! stock `tokio-postgres` client drives the statements end to end, asserting
//! the `UPDATE n` / `DELETE n` command tags count exactly the matched live
//! rows, for auto-commit and inside `BEGIN … COMMIT` (where the scan must see
//! the transaction's own buffered writes — read-your-own-writes, STL-203).
//! The predicate-selection correctness oracle lives with the engine
//! (`predicate_dml_matches_a_reference_model_over_seeded_workloads`); this
//! proves the same plan and tags reach a real client.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

mod common;

/// Connect a `tokio-postgres` client to a fresh in-memory server.
async fn connect() -> (Client, tokio::task::JoinHandle<()>) {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, driver)
}

/// Run one statement over the **simple-query** protocol and return the row
/// count its `CommandComplete` tag carried (`UPDATE 2` → `2`) — the count
/// `tokio-postgres` parses out of the tag the server sent.
async fn tag_count(client: &Client, sql: &str) -> u64 {
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("statement `{sql}`: {e}"));
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(count) => Some(*count),
            _ => None,
        })
        .unwrap_or_else(|| panic!("statement `{sql}` returned no command tag"))
}

/// The sorted `(id, v)` rows of `SELECT * FROM t`.
async fn rows(client: &Client, table: &str) -> Vec<(i32, Option<i32>)> {
    let messages = client
        .simple_query(&format!("SELECT * FROM {table}"))
        .await
        .expect("select");
    let mut out: Vec<(i32, Option<i32>)> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0).expect("id").parse().expect("id parses"),
                row.get(1).map(|v| v.parse().expect("v parses")),
            )),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn predicate_dml_tags_count_matched_rows_in_auto_commit() {
    let (client, driver) = connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create");
    for (id, v) in [(1, 10), (2, 30), (3, 40), (4, 20)] {
        client
            .simple_query(&format!("INSERT INTO t VALUES ({id}, {v})"))
            .await
            .expect("insert");
    }

    // A value-column predicate — impossible before STL-229 — touches exactly
    // the matching rows and tags their count.
    assert_eq!(
        tag_count(&client, "UPDATE t SET v = 0 WHERE v > 20").await,
        2
    );
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(10)), (2, Some(0)), (3, Some(0)), (4, Some(20))]
    );

    // The extended protocol reports the same count through `execute`.
    let n = client
        .execute("DELETE FROM t WHERE v = 0", &[])
        .await
        .expect("predicate delete");
    assert_eq!(n, 2, "the extended-protocol tag counts the matched rows");

    // Whole-table forms: no WHERE matches every live row; nothing matched is 0.
    assert_eq!(tag_count(&client, "UPDATE t SET v = 5").await, 2);
    assert_eq!(tag_count(&client, "DELETE FROM t WHERE v > 99").await, 0);
    assert_eq!(tag_count(&client, "DELETE FROM t").await, 2);
    assert_eq!(rows(&client, "t").await, vec![]);

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn predicate_dml_in_a_transaction_sees_its_own_writes() {
    let (client, driver) = connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO t VALUES (1, 100)")
        .await
        .expect("insert");

    // Inside the block: the INSERT is only buffered, yet the whole-table UPDATE
    // must match it (read-your-own-writes) and the tag must count it.
    client.batch_execute("BEGIN").await.expect("begin");
    assert_eq!(tag_count(&client, "INSERT INTO t VALUES (2, 200)").await, 1);
    assert_eq!(
        tag_count(&client, "UPDATE t SET v = 0 WHERE v >= 100").await,
        2,
        "the buffered INSERT joins the committed row in the matched set"
    );
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(0)), (2, Some(0))],
        "the block reads its own bulk write before COMMIT"
    );
    client.batch_execute("COMMIT").await.expect("commit");

    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(0)), (2, Some(0))],
        "the combined effect is durable after COMMIT"
    );

    // ROLLBACK discards a whole predicate statement with the buffer.
    client.batch_execute("BEGIN").await.expect("begin");
    assert_eq!(tag_count(&client, "DELETE FROM t").await, 2);
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(0)), (2, Some(0))],
        "the rolled-back whole-table DELETE left no trace"
    );

    drop(client);
    let _ = driver.await;
}
