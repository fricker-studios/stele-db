//! End-to-end CRUD round-trip driven by the real `tokio-postgres` client
//! (STL-147 Definition of Done, bullet 2).
//!
//! A stock Postgres driver connects to a live [`Server`], then runs
//! `CREATE → INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT` over the
//! **simple-query** protocol — the slice v0.1 speaks. (`tokio-postgres`'s typed
//! `query` / `execute` use the extended Parse/Bind/Execute protocol, which is a
//! v0.2 concern; `simple_query` / `batch_execute` ride the `Q` loop this ticket
//! wired through.) Proving a third-party client — not just the in-crate synthetic
//! one — drives the front end is the point of this test.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The single balance cell of a one-row `SELECT … id, balance …` reply, or
/// `None` when the reply carried no rows.
fn single_balance(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => {
            Some(row.get("balance").expect("balance column").to_owned())
        }
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_drives_a_crud_round_trip() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    // The connection object owns the actual socket I/O; drive it on its own task.
    let driver = tokio::spawn(connection);

    // CREATE — the identity-demo table.
    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");

    // INSERT — simple-query protocol; the CommandComplete count comes back as the
    // SimpleQueryMessage::CommandComplete payload.
    let inserted = client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert");
    assert!(
        inserted
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(1))),
        "INSERT affects one row"
    );

    // SELECT — the inserted balance reads back.
    let after_insert = client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select after insert");
    assert_eq!(single_balance(&after_insert).as_deref(), Some("100"));

    // UPDATE then read the new value.
    let updated = client
        .simple_query("UPDATE account SET balance = 250 WHERE id = 1")
        .await
        .expect("update");
    assert!(
        updated
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(1))),
        "UPDATE affects one row"
    );
    let after_update = client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select after update");
    assert_eq!(single_balance(&after_update).as_deref(), Some("250"));

    // DELETE then confirm the live read is empty.
    let deleted = client
        .simple_query("DELETE FROM account WHERE id = 1")
        .await
        .expect("delete");
    assert!(
        deleted
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(1))),
        "DELETE affects one row"
    );
    let after_delete = client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select after delete");
    assert_eq!(single_balance(&after_delete), None, "row gone after DELETE");

    // Close the client; the connection driver then resolves cleanly.
    drop(client);
    let _ = driver.await;
}
