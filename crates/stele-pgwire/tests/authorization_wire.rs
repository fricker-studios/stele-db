//! Authorization over the wire ([ADR-0034]): a denial must reach a real driver
//! as SQLSTATE `42501`, the code stock clients classify as a permission failure.
//!
//! The engine-level boundary is covered in `stele-engine/tests/authorization.rs`;
//! what this pins is the *wire contract*. It matters because the DDL and query
//! paths use two different SQLSTATE mappers, and the DDL one ends in a
//! catch-all — so a missing arm there would silently report `XX000` and no
//! driver would recognize the failure as a permission problem.
//!
//! [ADR-0034]: ../../../docs/adr/0034-role-based-access-control.md

mod common;

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

/// A server whose `account` table is owned by `alice`, with `mallory` a role
/// that holds nothing.
async fn spawn() -> std::net::SocketAddr {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (operator, conn) = tokio_postgres::connect(&common::conn_str_as(addr, "stele"), NoTls)
        .await
        .expect("operator connects");
    let driver = tokio::spawn(conn);
    operator
        .batch_execute(
            "CREATE USER alice PASSWORD 'pw'; \
             CREATE USER mallory PASSWORD 'pw'",
        )
        .await
        .expect("create roles");
    drop(operator);
    driver.await.expect("driver").expect("closed cleanly");

    let (alice, conn) = tokio_postgres::connect(&common::conn_str_as(addr, "alice"), NoTls)
        .await
        .expect("alice connects");
    let driver = tokio::spawn(conn);
    alice
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING; \
             INSERT INTO account VALUES (1, 100)",
        )
        .await
        .expect("alice creates and seeds");
    drop(alice);
    driver.await.expect("driver").expect("closed cleanly");

    addr
}

#[tokio::test]
async fn a_denial_reaches_the_driver_as_42501() {
    let addr = spawn().await;
    let (mallory, conn) = tokio_postgres::connect(&common::conn_str_as(addr, "mallory"), NoTls)
        .await
        .expect("mallory connects");
    let driver = tokio::spawn(conn);

    // The query path (simple + extended) and the DDL path use different
    // SQLSTATE mappers — check one of each, plus role DDL and an admin verb.
    let read = mallory
        .simple_query("SELECT id FROM account")
        .await
        .expect_err("mallory may not read");
    assert_eq!(read.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));

    let write = mallory
        .execute("INSERT INTO account VALUES (2, 2)", &[])
        .await
        .expect_err("mallory may not write");
    assert_eq!(write.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));

    // Role DDL takes the DDL mapper, whose catch-all would otherwise report
    // `XX000` — this is the arm that is not compiler-enforced.
    let takeover = mallory
        .batch_execute("ALTER USER alice PASSWORD 'stolen'")
        .await
        .expect_err("mallory may not rotate alice's password");
    assert_eq!(takeover.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));

    let exfiltrate = mallory
        .batch_execute("BACKUP TO '/tmp/stele-authz-wire-should-not-exist'")
        .await
        .expect_err("mallory may not back up the database");
    assert_eq!(exfiltrate.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));

    // The connection survives every denial — a refused statement is an ordinary
    // error, not a protocol break.
    let after = mallory
        .simple_query("SELECT 1")
        .await
        .expect("the session still works after a denial");
    assert!(!after.is_empty());

    drop(mallory);
    driver.await.expect("driver").expect("closed cleanly");
}

#[tokio::test]
async fn a_granted_role_works_over_the_wire() {
    let addr = spawn().await;
    let (alice, alice_conn) = tokio_postgres::connect(&common::conn_str_as(addr, "alice"), NoTls)
        .await
        .expect("alice connects");
    let alice_driver = tokio::spawn(alice_conn);
    alice
        .batch_execute("GRANT SELECT, INSERT ON account TO mallory")
        .await
        .expect("alice grants");

    let (mallory, conn) = tokio_postgres::connect(&common::conn_str_as(addr, "mallory"), NoTls)
        .await
        .expect("mallory connects");
    let driver = tokio::spawn(conn);

    let rows = mallory
        .query("SELECT id FROM account", &[])
        .await
        .expect("granted read");
    assert_eq!(rows.len(), 1);
    mallory
        .execute("INSERT INTO account VALUES (2, 200)", &[])
        .await
        .expect("granted write");
    // …but only the verbs granted.
    let denied = mallory
        .execute("DELETE FROM account WHERE id = 1", &[])
        .await
        .expect_err("DELETE was not granted");
    assert_eq!(denied.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));

    drop(mallory);
    drop(alice);
    driver.await.expect("driver").expect("closed cleanly");
    alice_driver.await.expect("driver").expect("closed cleanly");
}
