//! Read-your-own-writes on a *valid-time* table over the Postgres wire (STL-223).
//!
//! A transaction's mid-flight reads of a valid-time table reflect its own buffered
//! `INSERT` / `UPDATE` / `DELETE` at the right valid periods — both a plain read and
//! a `FOR VALID_TIME AS OF v` read — while another connection sees nothing until
//! `COMMIT`, and `ROLLBACK` discards. Valid-time history is written entirely over SQL
//! (STL-194), so the whole scenario rides the simple-query loop the v0.1 front end
//! speaks. The exhaustive in-process differential against committing the same buffer
//! is `valid_time_read_your_own_writes_matches_committing_the_buffer` in
//! `stele-engine`; this proves the path reaches end-to-end over the wire across two
//! connections.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

mod common;

const CREATE: &str = "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
     WITH SYSTEM VERSIONING VALID TIME (vf, vt)";

/// The `(id, balance)` pairs of a `SELECT id, balance …` reply, **sorted** (the v0.1
/// scan does not order rows, so the assertions sort to be layout-independent).
fn pairs(messages: &[SimpleQueryMessage]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some((
                row.get("id").expect("id column").to_owned(),
                row.get("balance").expect("balance column").to_owned(),
            )),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// Spawn a fresh in-memory server and connect a client to it, returning the client,
/// its connection-driver task, and the bound address (for a second connection).
async fn server_and_client() -> (
    Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    std::net::SocketAddr,
) {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, driver) = connect(addr).await;
    (client, driver, addr)
}

/// Connect one client to `addr`, spawning its connection driver.
async fn connect(
    addr: std::net::SocketAddr,
) -> (
    Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    (client, tokio::spawn(connection))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_time_transaction_reads_its_own_writes_over_the_wire() {
    let (a, a_driver, addr) = server_and_client().await;
    let (b, b_driver) = connect(addr).await;

    a.batch_execute(CREATE).await.expect("create");
    // Committed base: key 1 valid over [10, 20).
    a.simple_query("INSERT INTO acct VALUES (1, 100, 10, 20)")
        .await
        .expect("seed committed base");

    // Inside a transaction, a buffered INSERT then an UPDATE that widens key 2's
    // period to [30, 50) and changes its balance — closing the prior period and
    // opening the new one.
    a.simple_query("BEGIN").await.expect("begin");
    a.simple_query("INSERT INTO acct VALUES (2, 200, 30, 40)")
        .await
        .expect("buffered insert key 2");
    a.simple_query("UPDATE acct SET balance = 250, vf = 30, vt = 50 WHERE id = 2")
        .await
        .expect("buffered update key 2");

    // A plain read sees the committed row and the buffered (updated) one.
    let plain = a
        .simple_query("SELECT id, balance FROM acct")
        .await
        .expect("plain read");
    assert_eq!(
        pairs(&plain),
        vec![("1".into(), "100".into()), ("2".into(), "250".into())],
        "the transaction reads its own buffered writes alongside the committed row"
    );

    // `FOR VALID_TIME AS OF` admits each key only inside its own period.
    let probe = |v: i64| {
        let a = &a;
        async move {
            let sql = format!("SELECT id, balance FROM acct FOR VALID_TIME AS OF {v}");
            pairs(
                &a.simple_query(&sql)
                    .await
                    .expect("valid-time AS OF read in txn"),
            )
        }
    };
    assert_eq!(
        probe(15).await,
        vec![("1".into(), "100".into())],
        "valid 15 lands in key 1's [10,20) only"
    );
    assert_eq!(
        probe(45).await,
        vec![("2".into(), "250".into())],
        "valid 45 lands in key 2's widened [30,50) with the updated balance"
    );
    assert!(
        probe(25).await.is_empty(),
        "valid 25 is in neither key's period (key 2 starts at 30)"
    );

    // Another connection sees only the committed base until COMMIT.
    let other = b
        .simple_query("SELECT id, balance FROM acct")
        .await
        .expect("other reader mid-transaction");
    assert_eq!(
        pairs(&other),
        vec![("1".into(), "100".into())],
        "another connection sees only the committed base until COMMIT"
    );

    a.simple_query("COMMIT").await.expect("commit");

    // After COMMIT the other connection sees the committed valid-time update.
    let after = b
        .simple_query("SELECT id, balance FROM acct FOR VALID_TIME AS OF 45")
        .await
        .expect("after commit as of 45");
    assert_eq!(
        pairs(&after),
        vec![("2".into(), "250".into())],
        "the committed UPDATE is visible to the other connection at v=45"
    );

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_time_transaction_rollback_discards_over_the_wire() {
    let (a, a_driver, _addr) = server_and_client().await;

    a.batch_execute(CREATE).await.expect("create");
    // Committed base: key 1 valid over [10, 20).
    a.simple_query("INSERT INTO acct VALUES (1, 100, 10, 20)")
        .await
        .expect("seed committed base");

    // A transaction inserts a new key and deletes the committed one, reads its own
    // writes, then rolls back.
    a.simple_query("BEGIN").await.expect("begin");
    a.simple_query("INSERT INTO acct VALUES (2, 200, 30, 40)")
        .await
        .expect("buffered insert key 2");
    a.simple_query("DELETE FROM acct WHERE id = 1")
        .await
        .expect("buffered delete key 1");
    let mid = a
        .simple_query("SELECT id, balance FROM acct")
        .await
        .expect("mid-transaction read");
    assert_eq!(
        pairs(&mid),
        vec![("2".into(), "200".into())],
        "the transaction sees its buffered INSERT and not its deleted committed row"
    );

    a.simple_query("ROLLBACK").await.expect("rollback");

    // Nothing reached storage: only the committed base remains, period intact.
    let after = a
        .simple_query("SELECT id, balance FROM acct")
        .await
        .expect("after rollback");
    assert_eq!(
        pairs(&after),
        vec![("1".into(), "100".into())],
        "the rolled-back buffer never applied; only the committed base remains"
    );
    let at15 = a
        .simple_query("SELECT id, balance FROM acct FOR VALID_TIME AS OF 15")
        .await
        .expect("after rollback as of 15");
    assert_eq!(
        pairs(&at15),
        vec![("1".into(), "100".into())],
        "the committed key 1 is still valid at 15 after the rollback"
    );

    drop(a);
    let _ = a_driver.await;
}
