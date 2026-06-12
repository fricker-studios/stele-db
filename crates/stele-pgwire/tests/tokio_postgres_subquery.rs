//! End-to-end uncorrelated subqueries over the wire, driven by the real
//! `tokio-postgres` client (STL-234 Definition of Done: "Scalar / IN / EXISTS
//! work over the wire with correct NULL and empty-set semantics").
//!
//! A stock Postgres driver connects to a live [`Server`], creates an outer `t`
//! and an inner `s`, then runs each subquery predicate — a scalar comparison,
//! `IN` / `NOT IN`, `EXISTS` / `NOT EXISTS` — over the **simple-query** protocol
//! and asserts the result set. A scalar subquery returning more than one row must
//! surface SQLSTATE `21000` (`cardinality_violation`) the way Postgres does.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The `id` column of a `SELECT id FROM t WHERE …` reply, as a sorted `Vec` (a
/// `WHERE` does not order its output, so callers compare row *sets*).
fn ids(messages: &[SimpleQueryMessage]) -> Vec<String> {
    let mut out: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get("id").expect("id cell").to_owned()),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_uncorrelated_subqueries() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let _driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING; \
             CREATE TABLE s (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    for insert in [
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO t VALUES (2, 20)",
        "INSERT INTO t VALUES (3, 30)",
        "INSERT INTO s VALUES (1, 10)",
        "INSERT INTO s VALUES (2, 30)",
        "INSERT INTO s VALUES (3, NULL)",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // Scalar: `a = (SELECT a FROM s WHERE id = 1)` folds to `a = 10` → row 1.
    let reply = client
        .simple_query("SELECT id FROM t WHERE a = (SELECT a FROM s WHERE id = 1)")
        .await
        .expect("scalar subquery");
    assert_eq!(ids(&reply), vec!["1".to_owned()]);

    // IN: `a IN {10, 30}` (the NULL member is inert) → rows 1 and 3.
    let reply = client
        .simple_query("SELECT id FROM t WHERE a IN (SELECT a FROM s)")
        .await
        .expect("IN subquery");
    assert_eq!(ids(&reply), vec!["1".to_owned(), "3".to_owned()]);

    // NOT IN over a set that contains a NULL matches no row (the 3VL trap).
    let reply = client
        .simple_query("SELECT id FROM t WHERE a NOT IN (SELECT a FROM s)")
        .await
        .expect("NOT IN subquery");
    assert!(ids(&reply).is_empty(), "NOT IN with a NULL keeps no row");

    // EXISTS over a non-empty inner keeps every outer row.
    let reply = client
        .simple_query("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE a > 100)")
        .await
        .expect("EXISTS subquery");
    assert!(ids(&reply).is_empty(), "no s row has a > 100");
    let reply = client
        .simple_query("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE a > 100)")
        .await
        .expect("NOT EXISTS subquery");
    assert_eq!(
        ids(&reply),
        vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]
    );

    // A scalar subquery returning more than one row is the standard's 21000.
    let err = client
        .simple_query("SELECT id FROM t WHERE a = (SELECT a FROM s)")
        .await
        .expect_err("a multi-row scalar subquery must fail");
    assert_eq!(err.code(), Some(&SqlState::CARDINALITY_VIOLATION));
}
