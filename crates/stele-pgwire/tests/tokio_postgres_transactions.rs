//! Multi-statement transactions over the wire, driven by the real
//! `tokio-postgres` client (STL-174 Definition of Done, bullet 2).
//!
//! `BEGIN … COMMIT` is atomic — every buffered write lands together — and
//! `BEGIN … ROLLBACK` discards the lot. The transaction state is per connection
//! and persists across simple-query messages, so each `BEGIN`/DML/`COMMIT` is
//! sent as its own `simple_query` to prove the connection carries the state
//! between messages (not just within one batch). Both paths ride the `Q` loop
//! the v0.1 front end speaks; the extended protocol is a v0.2 concern.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The number of rows in a `SELECT … FROM account` reply.
fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Every `id` cell of a `SELECT id …` reply, as owned strings.
fn ids(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get("id").expect("id column").to_owned()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_is_atomic_and_rollback_discards() {
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

    // --- COMMIT path: two inserts inside one transaction land together. ----
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert 1");
    client
        .simple_query("INSERT INTO account VALUES (2, 200)")
        .await
        .expect("insert 2");

    // Before COMMIT the rows are buffered — a read on the same connection (which
    // sees committed state, not the buffer) returns nothing.
    let mid_txn = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select mid-transaction");
    assert_eq!(row_count(&mid_txn), 0, "buffered writes are invisible");

    client.simple_query("COMMIT").await.expect("commit");

    let after_commit = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after commit");
    assert_eq!(ids(&after_commit), vec!["1", "2"], "both inserts committed");

    // --- ROLLBACK path: a buffered insert is discarded. --------------------
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO account VALUES (3, 300)")
        .await
        .expect("insert 3");
    client.simple_query("ROLLBACK").await.expect("rollback");

    let after_rollback = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after rollback");
    assert_eq!(
        ids(&after_rollback),
        vec!["1", "2"],
        "the rolled-back insert never applied; only the committed rows remain"
    );

    drop(client);
    let _ = driver.await;
}

/// A whole transaction in a single batched simple-query message — `BEGIN`, two
/// writes, and `COMMIT` arrive together and still commit atomically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batched_transaction_commits_atomically() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;\
             BEGIN;\
             INSERT INTO account VALUES (1, 100);\
             INSERT INTO account VALUES (2, 200);\
             COMMIT;",
        )
        .await
        .expect("batched transaction");

    let rows = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select");
    assert_eq!(
        ids(&rows),
        vec!["1", "2"],
        "the batched COMMIT applied both"
    );

    drop(client);
    let _ = driver.await;
}
