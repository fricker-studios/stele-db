//! End-to-end computed projections and scalar subqueries in the SELECT list over
//! the wire, driven by the real `tokio-postgres` client (STL-303 Definition of
//! Done: "`SELECT a, (SELECT max(b) FROM s), a + 1 AS plus FROM t` returns the
//! computed columns over the wire; NULL / cardinality (`21000`) semantics match
//! Postgres").
//!
//! A stock Postgres driver connects to a live [`Server`], creates an outer `t` and
//! an inner `s`, then projects a bare column, an arithmetic expression, a constant
//! literal, and an uncorrelated scalar subquery — asserting the values, the
//! aliases, NULL propagation, and the `21000` cardinality violation a multi-row
//! scalar subquery raises.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The string cells of one named column across a simple-query reply, in row order
/// (`None` is a SQL `NULL`). The simple-query protocol returns every value as text,
/// so callers parse as needed.
fn column<'a>(messages: &'a [SimpleQueryMessage], name: &str) -> Vec<Option<&'a str>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(name)),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_projects_computed_columns_and_a_scalar_subquery() {
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
             CREATE TABLE s (id INT PRIMARY KEY, b INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    for insert in [
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO t VALUES (2, 20)",
        "INSERT INTO t VALUES (3, NULL)",
        "INSERT INTO s VALUES (1, 5)",
        "INSERT INTO s VALUES (2, 25)",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // The DoD shape: a bare column, a scalar subquery, an arithmetic alias, and a
    // constant — all in one SELECT, ordered so the rows compare positionally.
    let reply = client
        .simple_query(
            "SELECT id, (SELECT max(b) FROM s) AS m, a + 1 AS plus, 7 AS seven \
             FROM t ORDER BY id",
        )
        .await
        .expect("computed projection");

    // The scalar subquery `max(b)` = 25, broadcast to every row.
    assert_eq!(
        column(&reply, "m"),
        vec![Some("25"), Some("25"), Some("25")]
    );
    // `a + 1` per row, with the NULL row propagating NULL (3VL), not 1.
    assert_eq!(column(&reply, "plus"), vec![Some("11"), Some("21"), None]);
    // The constant literal, broadcast unchanged.
    assert_eq!(
        column(&reply, "seven"),
        vec![Some("7"), Some("7"), Some("7")]
    );
    // The bare column passes through.
    assert_eq!(column(&reply, "id"), vec![Some("1"), Some("2"), Some("3")]);

    // An empty inner makes a projected scalar subquery NULL on every row.
    let reply = client
        .simple_query("SELECT id, (SELECT b FROM s WHERE id = 99) AS m FROM t ORDER BY id")
        .await
        .expect("empty scalar subquery");
    assert_eq!(column(&reply, "m"), vec![None, None, None]);

    // A projected scalar subquery returning more than one row is the standard's
    // 21000 (cardinality_violation), exactly as in the WHERE clause.
    let err = client
        .simple_query("SELECT id, (SELECT b FROM s) FROM t")
        .await
        .expect_err("a multi-row scalar subquery must fail");
    assert_eq!(err.code(), Some(&SqlState::CARDINALITY_VIOLATION));
}
