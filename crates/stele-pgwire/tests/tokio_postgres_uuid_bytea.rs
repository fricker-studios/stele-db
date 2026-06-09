//! End-to-end wire round-trip for the v0.2 `UUID` and `BYTEA` scalar types
//! (STL-181 Definition of Done: each type round-trips over the wire; the catalog
//! stores them).
//!
//! A stock `tokio-postgres` client `CREATE`s a table with `UUID` and `BYTEA`
//! columns, `INSERT`s a row written in the Postgres textual forms (`'550e…'`,
//! `'\xDEADBEEF'`), then `SELECT`s it back over the **simple-query** protocol —
//! which carries every value in text format, so this exercises the new text
//! encoders without the binary path (deferred to STL-183 [G23]). The values come
//! back in their canonical Postgres rendering, proving the parse → fold → store →
//! read → encode loop closes for both new types.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The `(uid, digest)` cells of the one-row reply, or `None` if no row came back.
fn uid_and_digest(messages: &[SimpleQueryMessage]) -> Option<(String, String)> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some((
            row.get("uid").expect("uid column").to_owned(),
            row.get("digest").expect("digest column").to_owned(),
        )),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uuid_and_bytea_round_trip_over_the_wire() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // A table whose value columns are the two new types; the catalog must accept
    // and store `UUID` / `BYTEA` declarations.
    client
        .batch_execute(
            "CREATE TABLE doc (id INT PRIMARY KEY, uid UUID, digest BYTEA) \
             WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table with uuid + bytea columns");

    // INSERT the values in the textual forms a Postgres client writes. The
    // backslash is literal here (standard-conforming strings), so the bytea hex
    // input reaches the parser as `\xdeadbeef`.
    let inserted = client
        .simple_query(
            "INSERT INTO doc VALUES \
             (1, '550e8400-e29b-41d4-a716-446655440000', '\\xdeadbeef')",
        )
        .await
        .expect("insert uuid + bytea row");
    assert!(
        inserted
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(1))),
        "INSERT affects one row"
    );

    // SELECT them back: both render in their canonical Postgres text form.
    let read = client
        .simple_query("SELECT id, uid, digest FROM doc")
        .await
        .expect("select after insert");
    assert_eq!(
        uid_and_digest(&read),
        Some((
            "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            "\\xdeadbeef".to_owned(),
        )),
    );

    drop(client);
    let _ = driver.await;
}
