//! End-to-end result shaping over the Postgres wire (STL-263).
//!
//! `ORDER BY` / `LIMIT` / `OFFSET` / `DISTINCT` — alone and composed — driven
//! by a stock `tokio-postgres` client through the whole parse → bind → execute
//! → encode loop, asserting Postgres-equivalent rows **in wire order** (unlike
//! the other wire suites, order is the thing under test, so nothing here
//! sorts). The engine-side semantics live in the `stele-engine` shaping tests
//! and the exec-level `shape` unit tests; the nightly DuckDB differential
//! (`stele-exec-oracle`) diffs the same clauses against a second engine.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

mod common;

/// Connect a `tokio-postgres` client to a fresh in-memory server, returning the
/// client and the spawned connection driver (awaited at the end so the test
/// does not leak the task).
async fn connect() -> (Client, tokio::task::JoinHandle<()>) {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, driver)
}

/// The first column of each data row, **in wire order**, `None` for a SQL NULL.
fn first_column(messages: &[SimpleQueryMessage]) -> Vec<Option<String>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
            _ => None,
        })
        .collect()
}

/// Run `sql` and return its rows' first column in wire order.
async fn column(client: &Client, sql: &str) -> Vec<Option<String>> {
    first_column(
        &client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("select `{sql}`: {e}")),
    )
}

/// Shorthand: the expected column as owned present values.
fn vals(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_owned())).collect()
}

/// Seed `t(id, a, b)` with duplicate values and a NULL:
/// `(1, 20, 'x'), (2, 10, 'y'), (3, 20, 'x'), (4, NULL, 'z'), (5, 10, 'y')`.
async fn seed(client: &Client) {
    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT) WITH SYSTEM VERSIONING")
        .await
        .expect("create t");
    for row in [
        "(1, 20, 'x')",
        "(2, 10, 'y')",
        "(3, 20, 'x')",
        "(4, NULL, 'z')",
        "(5, 10, 'y')",
    ] {
        client
            .simple_query(&format!("INSERT INTO t VALUES {row}"))
            .await
            .expect("insert t row");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_limit_offset_shape_rows_over_the_wire() {
    let (client, driver) = connect().await;
    seed(&client).await;

    // Multi-key, mixed direction; ASC places the NULL `a` last.
    assert_eq!(
        column(&client, "SELECT id FROM t ORDER BY a, id DESC").await,
        vals(&["5", "2", "3", "1", "4"])
    );
    // DESC places the NULL first — the Postgres default placement.
    assert_eq!(
        column(&client, "SELECT a FROM t ORDER BY a DESC, id").await,
        vec![
            None,
            Some("20".into()),
            Some("20".into()),
            Some("10".into()),
            Some("10".into()),
        ]
    );
    // A plain SELECT may order on a column it does not project.
    assert_eq!(
        column(&client, "SELECT b FROM t ORDER BY a, id").await,
        vals(&["y", "y", "x", "x", "z"])
    );
    // OFFSET/LIMIT slice the ordered result; FETCH FIRST is the standard alias.
    assert_eq!(
        column(&client, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1").await,
        vals(&["2", "3"])
    );
    assert_eq!(
        column(
            &client,
            "SELECT id FROM t ORDER BY id OFFSET 3 ROWS FETCH FIRST 1 ROWS ONLY"
        )
        .await,
        vals(&["4"])
    );
    // LIMIT 0 and an OFFSET past the end are valid empty results.
    assert!(column(&client, "SELECT id FROM t LIMIT 0").await.is_empty());
    assert!(
        column(&client, "SELECT id FROM t ORDER BY id OFFSET 99")
            .await
            .is_empty()
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_deduplicates_and_composes_over_the_wire() {
    let (client, driver) = connect().await;
    seed(&client).await;

    // DISTINCT over one column: the duplicated 10s and 20s collapse; ordered
    // for a pinned expectation, NULL last under ASC.
    assert_eq!(
        column(&client, "SELECT DISTINCT a FROM t ORDER BY a").await,
        vec![Some("10".into()), Some("20".into()), None]
    );
    // DISTINCT is over the full projected row, composed with ORDER BY + LIMIT
    // (pipeline order: DISTINCT → ORDER BY → LIMIT).
    assert_eq!(
        column(
            &client,
            "SELECT DISTINCT a, b FROM t ORDER BY a DESC LIMIT 2"
        )
        .await,
        vec![None, Some("20".into())]
    );

    // DISTINCT + ORDER BY on a non-projected column is Postgres's 42P10.
    let err = client
        .simple_query("SELECT DISTINCT a FROM t ORDER BY id")
        .await
        .expect_err("DISTINCT + unprojected ORDER BY must fail");
    assert_eq!(
        err.code(),
        Some(&SqlState::INVALID_COLUMN_REFERENCE),
        "42P10 over the wire, got: {err}"
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_output_shapes_over_the_wire() {
    let (client, driver) = connect().await;
    seed(&client).await;

    // Groups: a=10 ×2, a=20 ×2, a=NULL ×1 — ordered by the aggregate output
    // column, ties by the grouping column, then limited.
    assert_eq!(
        column(
            &client,
            "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY count DESC, a LIMIT 2"
        )
        .await,
        vals(&["10", "20"])
    );

    drop(client);
    let _ = driver.await;
}
