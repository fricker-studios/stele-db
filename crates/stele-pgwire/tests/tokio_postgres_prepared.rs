//! End-to-end **prepared-`SELECT`** round-trip driven by the real `tokio-postgres`
//! client over the extended query protocol with **binary** results (STL-212
//! Definition of Done, bullet 2).
//!
//! `tokio-postgres`'s `query` prepares the statement first — Parse + Describe('S')
//! — and builds its result-column list from the statement-level `RowDescription`.
//! Before STL-212 that reply was `NoData`, so the driver saw a zero-column result
//! and a binary prepared `SELECT` could not be exercised through it at all;
//! STL-183's binary encoders were oracled against `postgres-types` instead. This
//! test closes that loop: a genuine `tokio-postgres` `query` round-trips real
//! binary `int4` / `int8` / `text` cells, with its column metadata coming straight
//! from the statement-level `RowDescription` STL-212 now emits.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::NoTls;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_prepared_select_binary_round_trips() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    // The connection object owns the actual socket I/O; drive it on its own task.
    let driver = tokio::spawn(connection);

    // A three-column table — business key `id` (int4), plus `balance` (int8) and
    // `note` (text) value columns, so the round-trip spans two integer widths and
    // a variable-length text cell.
    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance BIGINT, note TEXT) \
             WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO account VALUES (1, 5000000000, 'hello')")
        .await
        .expect("insert");

    // `query` runs the extended protocol: Parse + Describe('S') (where it reads the
    // result columns from the statement-level RowDescription this ticket adds),
    // then Bind with binary result formats, Execute, and binary cell decode. Before
    // STL-212 the Describe('S') reply was `NoData` → zero columns → no usable rows.
    let rows = client
        .query("SELECT id, balance, note FROM account", &[])
        .await
        .expect("prepared binary SELECT");
    assert_eq!(rows.len(), 1, "exactly the one inserted row");

    // The column metadata is built entirely from the statement-level RowDescription.
    let names: Vec<&str> = rows[0]
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, ["id", "balance", "note"]);

    // Each cell decoded from its binary wire form under the type the RowDescription
    // advertised: int4, int8, and text.
    let id: i32 = rows[0].get("id");
    let balance: i64 = rows[0].get("balance");
    let note: &str = rows[0].get("note");
    assert_eq!(id, 1);
    assert_eq!(balance, 5_000_000_000);
    assert_eq!(note, "hello");

    // A parameterless prepared `SELECT` with an explicit WHERE describes the same
    // shape (the filter is stripped from the describe), and the binary read still
    // round-trips the single matching row.
    let filtered = client
        .query("SELECT id, balance FROM account WHERE id = 1", &[])
        .await
        .expect("prepared binary SELECT with WHERE");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].get::<_, i32>("id"), 1);
    assert_eq!(filtered[0].get::<_, i64>("balance"), 5_000_000_000);

    // Close the client; the connection driver then resolves cleanly.
    drop(client);
    let _ = driver.await;
}
