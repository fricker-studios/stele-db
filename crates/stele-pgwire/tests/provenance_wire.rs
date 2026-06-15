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
//!
//! The second test is the [STL-300] end-to-end round trip: a row written over a
//! connection that identifies as `alice` reads back `_stele_principal = 'alice'`,
//! and a second `bob` connection sharing the one engine stamps `bob` — the
//! connection identity is threaded into the stored write principal, not a constant.
//!
//! [STL-300]: https://allegromusic.atlassian.net/browse/STL-300

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
        "the write principal is the connection's user — here `stele`, from the \
         connection string ([STL-300])",
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

    // Dropping the client closes the connection; the driver task must then finish
    // cleanly — a late protocol error or panic surfaces here rather than passing
    // silently.
    drop(client);
    driver
        .await
        .expect("pgwire driver task did not panic")
        .expect("pgwire connection closed cleanly");
}

/// The connection's identity is stamped as the write principal ([STL-300]): a row
/// written over a connection identifying as `alice` reports `_stele_principal =
/// 'alice'`, while a second `bob` connection sharing the **one** engine stamps
/// `bob`. Because the engine is shared behind a single mutex, this only holds if the
/// principal is set per statement under the dispatch lock rather than as a single
/// shared field — so interleaving the two connections' writes is the real test.
///
/// Coverage spans the live write-stamping paths: the auto-commit single write, the
/// multi-row write group, and the multi-statement `COMMIT` (alice, simple protocol),
/// plus an extended-protocol auto-commit write (bob).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_connection_user_is_the_stored_write_principal() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    // Two clients on the one shared engine: alice over the simple-query protocol,
    // bob over the extended protocol.
    let (alice, alice_conn) = tokio_postgres::connect(&common::conn_str_as(addr, "alice"), NoTls)
        .await
        .expect("alice connects");
    let alice_driver = tokio::spawn(alice_conn);
    let (bob, bob_conn) = tokio_postgres::connect(&common::conn_str_as(addr, "bob"), NoTls)
        .await
        .expect("bob connects");
    let bob_driver = tokio::spawn(bob_conn);

    // alice creates the table and inserts one row (auto-commit single write).
    alice
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    alice
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("alice insert");

    // bob writes *between* alice's writes (extended protocol, auto-commit): the
    // shared engine's principal must flip to bob for this statement and back to alice
    // for her next one — a single shared field set once at connect would get this
    // wrong.
    bob.execute("INSERT INTO account VALUES (10, 999)", &[])
        .await
        .expect("bob insert");

    // alice: a multi-row write group, then a multi-statement COMMIT updating row 1.
    alice
        .simple_query("INSERT INTO account VALUES (2, 200), (3, 300)")
        .await
        .expect("alice multi-row insert");
    alice
        .batch_execute("BEGIN; UPDATE account SET balance = 150 WHERE id = 1; COMMIT")
        .await
        .expect("alice transaction");

    // Read every live row's principal. alice's rows (the updated 1 and the multi-row
    // 2, 3) report alice; bob's row 10 reports bob.
    let read = alice
        .simple_query("SELECT id, _stele_principal FROM account ORDER BY id")
        .await
        .expect("select principals");
    let principals: Vec<(String, String)> = read
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some((
                row.get("id").expect("id cell").to_owned(),
                row.get("_stele_principal")
                    .expect("principal cell")
                    .to_owned(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        principals,
        vec![
            ("1".to_owned(), "alice".to_owned()),
            ("2".to_owned(), "alice".to_owned()),
            ("3".to_owned(), "alice".to_owned()),
            ("10".to_owned(), "bob".to_owned()),
        ],
        "each row records the identity of the connection that wrote it",
    );

    drop(alice);
    drop(bob);
    alice_driver
        .await
        .expect("alice driver task did not panic")
        .expect("alice connection closed cleanly");
    bob_driver
        .await
        .expect("bob driver task did not panic")
        .expect("bob connection closed cleanly");
}
