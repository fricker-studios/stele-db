//! Cold-boot recovery **over the wire** (STL-210 Definition of Done): a stock
//! `tokio-postgres` client creates a table and writes rows against one server,
//! the server "process" is restarted — the engine dropped with no graceful
//! shutdown and a fresh one booted through [`SessionEngine::recover`] over the
//! same disk, exactly as `stele-server::run` boots — and a second client reads
//! the same rows back, current and `AS OF`, with the catalog resolving the
//! table at its schema.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

mod common;

/// Connect a client to `addr`, driving its connection on its own task.
async fn connect(addr: std::net::SocketAddr) -> Client {
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    tokio::spawn(connection);
    client
}

/// The single `balance` cell of a `SELECT … balance …` reply, or `None` when
/// the reply carried no rows.
fn single_balance(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => {
            Some(row.get("balance").expect("balance column").to_owned())
        }
        _ => None,
    })
}

/// Wall-clock microseconds since the Unix epoch — the same reading
/// [`SystemClock`] stamps commits with, so a captured instant is a usable
/// `FOR SYSTEM_TIME AS OF` literal.
fn wall_micros() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch");
    i64::try_from(dur.as_micros()).expect("fits i64")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_server_serves_the_same_rows_over_the_wire() {
    // First boot: through `recover` over an empty disk — the same
    // unconditional path `stele-server::run` takes — which is a fresh session.
    let disk = MemDisk::new();
    let session: SharedSession = Arc::new(Mutex::new(
        SessionEngine::recover(disk.clone(), SystemClock).expect("first boot over an empty disk"),
    ));
    let addr = common::spawn_server(session).await;
    let client = connect(addr).await;

    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert");

    // Capture a wall-clock instant strictly between the INSERT's and the
    // UPDATE's commits: the commit clock follows the wall clock, so a short
    // sleep on each side keeps the captured microsecond unambiguous.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mid = wall_micros();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client
        .simple_query("UPDATE account SET balance = 250 WHERE id = 1")
        .await
        .expect("update");

    // "Kill" the server: drop our engine handle with no checkpoint, no flush,
    // no goodbye — committed state must be recoverable from the disk alone.
    drop(client);

    // Second boot: recover over the same disk and serve on a fresh port.
    let recovered: SharedSession = Arc::new(Mutex::new(
        SessionEngine::recover(disk, SystemClock).expect("recover from on-disk state"),
    ));
    let addr = common::spawn_server(recovered).await;
    let client = connect(addr).await;

    // The current read sees the post-update value…
    let now = client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select after restart");
    assert_eq!(single_balance(&now).as_deref(), Some("250"));

    // …and the AS OF read between the two commits still sees the original —
    // history survived the restart, not just the latest state.
    let as_of = client
        .simple_query(&format!(
            "SELECT id, balance FROM account FOR SYSTEM_TIME AS OF {mid}"
        ))
        .await
        .expect("as-of select after restart");
    assert_eq!(single_balance(&as_of).as_deref(), Some("100"));

    // A write against the recovered table commits — the tier is live, not a
    // read-only reconstruction.
    client
        .simple_query("INSERT INTO account VALUES (2, 7)")
        .await
        .expect("insert after restart");
    let count = client
        .simple_query("SELECT COUNT(*) AS balance FROM account")
        .await
        .expect("count after restart");
    assert_eq!(single_balance(&count).as_deref(), Some("2"));
}
