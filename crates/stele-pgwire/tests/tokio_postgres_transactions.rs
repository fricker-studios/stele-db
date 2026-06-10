//! Multi-statement transactions over the wire, driven by the real
//! `tokio-postgres` client (STL-174 Definition of Done, bullet 2; STL-175
//! snapshot isolation).
//!
//! `BEGIN … COMMIT` is atomic — every buffered write lands together — and
//! `BEGIN … ROLLBACK` discards the lot. The transaction state is per connection
//! and persists across simple-query messages, so each `BEGIN`/DML/`COMMIT` is
//! sent as its own `simple_query` to prove the connection carries the state
//! between messages (not just within one batch). Under **snapshot isolation**
//! (STL-175) a transaction reads one consistent snapshot pinned at `BEGIN`, and a
//! write-write conflict surfaces at `COMMIT` as a retryable serialization failure
//! (SQLSTATE `40001`) — both exercised here across two connections. Both paths
//! ride the `Q` loop the v0.1 front end speaks; the extended protocol is a v0.2
//! concern.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The number of rows in a `SELECT … FROM account` reply.
fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Every `balance` cell of a `SELECT balance …` reply, in row order.
fn balances(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                Some(row.get("balance").expect("balance column").to_owned())
            }
            _ => None,
        })
        .collect()
}

/// Every `id` cell of a `SELECT id …` reply, **sorted** as owned strings. The
/// query carries no `ORDER BY` (and the v0.1 scan does not order rows), so the
/// values are sorted here to make the assertions independent of scan/physical
/// layout order.
fn ids(messages: &[SimpleQueryMessage]) -> Vec<String> {
    let mut ids: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get("id").expect("id column").to_owned()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
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

    // Before COMMIT the rows are buffered, and the transaction reads its pinned
    // snapshot (taken at BEGIN, before these inserts) — so the read returns
    // nothing: neither the write buffer nor any newer state is visible (STL-175).
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

/// Snapshot isolation over the wire (STL-175): a transaction reads one consistent
/// snapshot for its whole life. `a` opens a transaction, pinning a snapshot that
/// sees `balance = 100`; `b` then auto-commits `balance = 200`; `a`'s in-transaction
/// `SELECT` still reads `100`. After `a` ends its transaction, it reads the latest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_reads_a_stable_snapshot_over_the_wire() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect a");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect b");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("seed 100");

    // `a` pins its snapshot at BEGIN (it sees balance = 100).
    a.simple_query("BEGIN").await.expect("a begin");

    // `b` auto-commits a newer value on another connection.
    b.simple_query("UPDATE account SET balance = 200 WHERE id = 1")
        .await
        .expect("b update");

    // `a` still reads its pinned snapshot, not `b`'s commit.
    let mid = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads in-transaction");
    assert_eq!(
        balances(&mid),
        vec!["100"],
        "the transaction reads its pinned snapshot, not the concurrent commit"
    );

    a.simple_query("COMMIT").await.expect("a commit");

    // Outside the transaction `a` is its own snapshot and sees the latest value.
    let after = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads after commit");
    assert_eq!(
        balances(&after),
        vec!["200"],
        "after the transaction ends, the next statement sees the latest committed state"
    );

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

/// First-committer-wins write-write conflict over the wire (STL-175): two
/// transactions pin the same snapshot and both write `id = 1`; the first to COMMIT
/// wins, and the second's COMMIT is a **retryable** serialization failure
/// (SQLSTATE `40001`) — the signal a client uses to retry the whole transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_write_conflict_surfaces_a_retryable_error() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect a");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect b");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("seed");

    // Both transactions begin (pinning the same snapshot) before either commits,
    // and both stage a write to id = 1.
    a.simple_query("BEGIN").await.expect("a begin");
    a.simple_query("UPDATE account SET balance = 200 WHERE id = 1")
        .await
        .expect("a update");
    b.simple_query("BEGIN").await.expect("b begin");
    b.simple_query("UPDATE account SET balance = 300 WHERE id = 1")
        .await
        .expect("b update");

    // First committer wins.
    a.simple_query("COMMIT").await.expect("a commits");

    // The loser's COMMIT is a retryable serialization failure (40001).
    let err = b
        .simple_query("COMMIT")
        .await
        .expect_err("b's commit must conflict");
    assert_eq!(
        err.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "a write-write conflict maps to 40001 (serialization_failure), which clients retry: {err}"
    );

    // The winner's value is what persisted; the loser touched nothing.
    let rows = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("select");
    assert_eq!(
        balances(&rows),
        vec!["200"],
        "first committer's write is the one that persisted"
    );

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}
