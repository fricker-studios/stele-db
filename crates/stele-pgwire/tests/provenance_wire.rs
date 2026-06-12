//! Provenance pseudo-columns over the wire ([STL-247] Definition of Done: "the
//! three pseudo-columns return correct values over the wire … hidden from
//! `SELECT *`").
//!
//! A stock `tokio-postgres` client selects `_stele_txn_id`, `_stele_committed_at`,
//! and `_stele_principal` over the simple-query protocol and reads each rendered
//! cell, confirming the pseudo-columns resolve, carry their stored values, and are
//! usable in a `WHERE` — and that `SELECT *` does not surface them (the Postgres
//! system-column posture). The transaction ids are deterministic (a per-session
//! counter, one per committed write), so they are asserted exactly; the commit
//! instant is wall-clock under [`SystemClock`], so it is asserted present and
//! well-formed rather than to a fixed value.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage, SimpleQueryRow};

mod common;

/// The first data row of a simple-query reply.
fn first_row(messages: &[SimpleQueryMessage]) -> Option<&SimpleQueryRow> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_reads_provenance_pseudo_columns() {
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
    // First committed write — transaction id 1.
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert");

    // The three provenance pseudo-columns resolve and read back over the wire.
    let read = client
        .simple_query(
            "SELECT id, _stele_txn_id, _stele_committed_at, _stele_principal FROM account",
        )
        .await
        .expect("select provenance");
    let row = first_row(&read).expect("one row");
    assert_eq!(row.get("id"), Some("1"));
    assert_eq!(
        row.get("_stele_txn_id"),
        Some("1"),
        "the first committed write is transaction 1",
    );
    assert_eq!(
        row.get("_stele_principal"),
        Some("stele"),
        "the wire write principal",
    );
    let committed_at = row
        .get("_stele_committed_at")
        .expect("a commit instant cell");
    assert!(
        committed_at.contains('+'),
        "the commit instant renders as a timestamptz (got {committed_at:?})",
    );

    // A second write advances the transaction id; the live row reports the latest.
    client
        .simple_query("UPDATE account SET balance = 250 WHERE id = 1")
        .await
        .expect("update");
    let after_update = client
        .simple_query("SELECT _stele_txn_id FROM account")
        .await
        .expect("select txn id");
    assert_eq!(
        first_row(&after_update).expect("row").get("_stele_txn_id"),
        Some("2"),
        "the update is transaction 2",
    );

    // Usable in WHERE: filter by the writing transaction.
    let by_txn = client
        .simple_query("SELECT id FROM account WHERE _stele_txn_id = 2")
        .await
        .expect("filter by txn id");
    assert_eq!(first_row(&by_txn).expect("row").get("id"), Some("1"));
    let by_missing_txn = client
        .simple_query("SELECT id FROM account WHERE _stele_txn_id = 999")
        .await
        .expect("filter by an unused txn id");
    assert!(
        first_row(&by_missing_txn).is_none(),
        "no live row was written by transaction 999",
    );

    // `SELECT *` hides the pseudo-columns — only the user columns come back.
    let star = client
        .simple_query("SELECT * FROM account")
        .await
        .expect("select star");
    let star_row = first_row(&star).expect("one row");
    let names: Vec<&str> = star_row
        .columns()
        .iter()
        .map(tokio_postgres::SimpleColumn::name)
        .collect();
    assert_eq!(
        names,
        vec!["id", "balance"],
        "SELECT * surfaces only the user columns",
    );

    drop(client);
    let _ = driver.await;
}
