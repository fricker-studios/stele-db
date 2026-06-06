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

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{Server, SharedSession};
use stele_storage::backend::MemDisk;
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::{NoTls, SimpleQueryMessage};

/// Stand up a `Server` on an ephemeral port over a fresh in-memory session and
/// return its address. The server task runs for the test's lifetime (the test
/// drops the runtime at the end, which aborts it).
async fn spawn_server() -> SocketAddr {
    // Reserve a free port via a throwaway bind, then hand it to the real server.
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reserved.local_addr().unwrap();
    drop(reserved);

    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    tokio::spawn(Server::new(addr, session).run());

    // `Server::run` binds asynchronously; wait until it accepts before returning.
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server did not come up on {addr} within the retry budget");
}

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
    let addr = spawn_server().await;

    // `sslmode=disable` skips the SSL negotiation entirely (the server would
    // refuse it anyway); no auth in v0.1, so any user/dbname is accepted.
    let conn_str = format!(
        "host=127.0.0.1 port={} user=stele dbname=stele sslmode=disable",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
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
