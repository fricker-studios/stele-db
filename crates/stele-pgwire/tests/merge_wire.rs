//! `MERGE` over the Postgres wire (STL-230).
//!
//! A stock `tokio-postgres` client drives the v0.3 upsert workhorse end to end:
//! mixed matched/not-matched `VALUES` batches and a table source, in auto-commit
//! and inside `BEGIN … COMMIT` (where the probe must see the transaction's own
//! buffered writes — read-your-own-writes, STL-203), asserting the `MERGE n`
//! command tag counts exactly the acted-on source rows. Atomicity is asserted as
//! a test, not an inspection: a statement whose source affects one target row
//! twice fails with SQLSTATE `21000` (`cardinality_violation`) and leaves the
//! table byte-for-byte unchanged — none of its other rows apply. The upsert
//! correctness oracle lives with the engine
//! (`merge_matches_a_reference_model_over_seeded_workloads`); this proves the
//! same plan, tags, and failure posture reach a real client.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
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
/// count its `CommandComplete` tag carried (`MERGE 2` → `2`) — the count
/// `tokio-postgres` parses out of the tag the server sent. (The tag's verb is
/// pinned by the pgwire unit test `command_tags_render_per_postgres_convention`;
/// the client library exposes only the parsed count.)
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
async fn merge_upserts_mixed_batches_in_auto_commit() {
    let (client, driver) = connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create");
    for (id, v) in [(1, 10), (2, 20)] {
        client
            .simple_query(&format!("INSERT INTO t VALUES ({id}, {v})"))
            .await
            .expect("insert");
    }

    // Mixed batch: key 1 exists (updated), keys 3 and 4 don't (inserted). The
    // tag counts every acted-on source row.
    assert_eq!(
        tag_count(
            &client,
            "MERGE INTO t USING (VALUES (1, 100), (3, 300), (4, NULL)) AS s (id, v) \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        .await,
        3
    );
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(100)), (2, Some(20)), (3, Some(300)), (4, None)]
    );

    // The extended protocol reports the same count through `execute` (Parse /
    // Bind / Describe — a MERGE describes as no-data — / Execute).
    let n = client
        .execute(
            "MERGE INTO t USING (VALUES (2, 200), (5, 500)) AS s (id, v) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
            &[],
        )
        .await
        .expect("extended-protocol MERGE");
    assert_eq!(n, 2, "the extended-protocol tag counts acted-on rows");

    // Single-arm forms skip the other rows — and an all-skipped batch is MERGE 0.
    assert_eq!(
        tag_count(
            &client,
            "MERGE INTO t USING (VALUES (1, 0), (9, 0)) AS s (id, v) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v"
        )
        .await,
        1
    );
    assert_eq!(
        tag_count(
            &client,
            "MERGE INTO t USING (VALUES (1, 7)) AS s (id, v) ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        .await,
        0
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_reads_a_table_source() {
    let (client, driver) = connect().await;
    client
        .batch_execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING; \
             CREATE TABLE staged (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert target");
    for (id, v) in [(1, 100), (2, 200)] {
        client
            .simple_query(&format!("INSERT INTO staged VALUES ({id}, {v})"))
            .await
            .expect("insert source");
    }

    assert_eq!(
        tag_count(
            &client,
            "MERGE INTO t USING staged AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        .await,
        2
    );
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(100)), (2, Some(200))]
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_in_a_transaction_sees_its_own_writes() {
    let (client, driver) = connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert");

    // Inside the block: the INSERT of key 2 is only buffered, yet the MERGE
    // probe must see it (read-your-own-writes) and take the MATCHED arm for it.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO t VALUES (2, 20)")
        .await
        .expect("buffered insert");
    assert_eq!(
        tag_count(
            &client,
            "MERGE INTO t USING (VALUES (1, 100), (2, 200), (3, 300)) AS s (id, v) \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        .await,
        3
    );
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(100)), (2, Some(200)), (3, Some(300))],
        "the block reads its own MERGE before COMMIT"
    );
    client.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(100)), (2, Some(200)), (3, Some(300))],
        "the combined effect is durable after COMMIT"
    );

    // ROLLBACK discards a buffered MERGE entirely.
    client.batch_execute("BEGIN").await.expect("begin");
    assert_eq!(
        tag_count(
            &client,
            "MERGE INTO t USING (VALUES (1, 0), (9, 9)) AS s (id, v) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        .await,
        2
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        rows(&client, "t").await,
        vec![(1, Some(100)), (2, Some(200)), (3, Some(300))],
        "the rolled-back MERGE left no trace"
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_merge_leaves_the_table_unchanged() {
    let (client, driver) = connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert");
    let before = rows(&client, "t").await;

    // Source rows 2 and 3 both write key 7: the statement fails with the
    // standard's cardinality violation — and *nothing* applies, not the update
    // to key 1 either (the atomicity DoD, asserted on the data).
    let err = client
        .simple_query(
            "MERGE INTO t USING (VALUES (1, 100), (7, 70), (7, 71)) AS s (id, v) \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        )
        .await
        .expect_err("a row affected twice must fail the whole statement");
    assert_eq!(
        err.code(),
        Some(&SqlState::CARDINALITY_VIOLATION),
        "got {err:?}"
    );
    assert_eq!(
        rows(&client, "t").await,
        before,
        "a failed MERGE leaves the table unchanged"
    );

    // The same failure inside a block aborts the block (25P02 until ROLLBACK)
    // and stages nothing.
    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .simple_query(
            "MERGE INTO t USING (VALUES (7, 70), (7, 71)) AS s (id, v) ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        )
        .await
        .expect_err("the duplicate fails at staging");
    assert_eq!(err.code(), Some(&SqlState::CARDINALITY_VIOLATION));
    let err = client
        .simple_query("SELECT * FROM t")
        .await
        .expect_err("the block is aborted until ROLLBACK");
    assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        rows(&client, "t").await,
        before,
        "the aborted block left no trace"
    );

    drop(client);
    let _ = driver.await;
}
