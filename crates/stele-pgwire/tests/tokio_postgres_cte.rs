//! End-to-end non-recursive CTEs and derived tables over the wire, driven by the
//! real `tokio-postgres` client (STL-242 Definition of Done: "Multi-CTE queries
//! (CTE→CTE chaining, CTE joined to a base table, CTE under aggregation) return
//! correct results over the wire").
//!
//! A stock Postgres driver connects to a live [`Server`], creates base tables,
//! then runs `WITH …` queries and `FROM (SELECT …) AS d` derived tables over the
//! **simple-query** protocol and asserts the result set. The cases mirror the
//! DoD: a plain CTE reference, multiple CTEs, a CTE that reads an earlier CTE, a
//! CTE joined to a base table, a CTE under aggregation, and a derived table.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The `id` column of a reply, numerically sorted (a `WHERE` / CTE does not order
/// its output, so callers compare row *sets*).
fn ids(messages: &[SimpleQueryMessage]) -> Vec<i32> {
    let mut out: Vec<i32> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                row.get("id")
                    .expect("id cell")
                    .parse()
                    .expect("id is an int"),
            ),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

/// Collect a reply's data rows into a sorted `Vec` of the named columns' values
/// (`None` for a SQL `NULL` cell). Output order is unspecified, so compare sets.
fn rows(messages: &[SimpleQueryMessage], columns: &[&str]) -> Vec<Vec<Option<String>>> {
    let mut out: Vec<Vec<Option<String>>> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                columns
                    .iter()
                    .map(|c| row.get(c).map(ToOwned::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// The single scalar cell of a one-row, one-column reply (e.g. an aggregate).
fn scalar(messages: &[SimpleQueryMessage], column: &str) -> Option<String> {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(column).map(ToOwned::to_owned)),
            _ => None,
        })
        .expect("a data row")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_ctes_and_derived_tables() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let _driver = tokio::spawn(connection);

    // `t` (id, a) plus a join partner `s` (id, label).
    client
        .batch_execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING; \
             CREATE TABLE s (id INT PRIMARY KEY, label TEXT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    for insert in [
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO t VALUES (2, 20)",
        "INSERT INTO t VALUES (3, 30)",
        "INSERT INTO t VALUES (4, 40)",
        "INSERT INTO s VALUES (1, 'one')",
        "INSERT INTO s VALUES (2, 'two')",
        "INSERT INTO s VALUES (3, 'three')",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // 1. A plain CTE reference, with a `WHERE` and projection over the CTE.
    let reply = client
        .simple_query(
            "WITH big AS (SELECT id, a FROM t WHERE a >= 20) SELECT id FROM big WHERE a < 40",
        )
        .await
        .expect("plain CTE reference");
    assert_eq!(ids(&reply), vec![2, 3]);

    // 2. A derived table in FROM (`FROM (SELECT …) AS d`).
    let reply = client
        .simple_query("SELECT id FROM (SELECT id, a FROM t WHERE a > 15) AS d WHERE a <> 30")
        .await
        .expect("derived table");
    assert_eq!(ids(&reply), vec![2, 4]);

    // 3. CTE → CTE chaining: `hi` reads `big`.
    let reply = client
        .simple_query(
            "WITH big AS (SELECT id, a FROM t WHERE a >= 20), \
                  hi AS (SELECT id, a FROM big WHERE a >= 30) \
             SELECT id FROM hi",
        )
        .await
        .expect("CTE chaining");
    assert_eq!(ids(&reply), vec![3, 4]);

    // 4. A CTE joined to a base table.
    let reply = client
        .simple_query(
            "WITH small AS (SELECT id, a FROM t WHERE a <= 30) \
             SELECT small.id, s.label FROM small JOIN s ON small.id = s.id",
        )
        .await
        .expect("CTE joined to a base table");
    assert_eq!(
        rows(&reply, &["id", "label"]),
        vec![
            vec![Some("1".to_owned()), Some("one".to_owned())],
            vec![Some("2".to_owned()), Some("two".to_owned())],
            vec![Some("3".to_owned()), Some("three".to_owned())],
        ]
    );

    // 5. A CTE under aggregation (ungrouped + grouped).
    let reply = client
        .simple_query(
            "WITH big AS (SELECT id, a FROM t WHERE a >= 20) SELECT count(*) AS id FROM big",
        )
        .await
        .expect("CTE under aggregation");
    assert_eq!(scalar(&reply, "id"), Some("3".to_owned()));

    let reply = client
        .simple_query("WITH c AS (SELECT id, a FROM t) SELECT sum(a) AS total FROM c WHERE id <= 2")
        .await
        .expect("CTE aggregate with WHERE");
    assert_eq!(scalar(&reply, "total"), Some("30".to_owned()));

    // 6. A `name(col, …)` column-alias list renames the CTE's output columns: the
    // outer query addresses `k`/`v`, not the inner `id`/`a`.
    let reply = client
        .simple_query("WITH c(k, v) AS (SELECT id, a FROM t) SELECT k FROM c WHERE v = 40")
        .await
        .expect("CTE column aliases");
    assert_eq!(rows(&reply, &["k"]), vec![vec![Some("4".to_owned())]]);

    // 7. A derived table joined to a base table.
    let reply = client
        .simple_query(
            "SELECT d.id, s.label \
             FROM (SELECT id, a FROM t WHERE a < 30) AS d JOIN s ON d.id = s.id",
        )
        .await
        .expect("derived table joined to a base table");
    assert_eq!(
        rows(&reply, &["id", "label"]),
        vec![
            vec![Some("1".to_owned()), Some("one".to_owned())],
            vec![Some("2".to_owned()), Some("two".to_owned())],
        ]
    );
}
