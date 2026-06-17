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

/// The `id` column of a `SELECT id FROM t WHERE …` reply, as a numerically
/// sorted `Vec` (a `WHERE` does not order its output, so callers compare row
/// *sets*; sorting by value, not lexically, keeps the comparison robust if the
/// fixture grows past single digits).
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
    assert_eq!(ids(&reply), vec![1]);

    // IN: `a IN {10, 30}` (the NULL member is inert) → rows 1 and 3.
    let reply = client
        .simple_query("SELECT id FROM t WHERE a IN (SELECT a FROM s)")
        .await
        .expect("IN subquery");
    assert_eq!(ids(&reply), vec![1, 3]);

    // NOT IN over a set that contains a NULL matches no row (the 3VL trap).
    let reply = client
        .simple_query("SELECT id FROM t WHERE a NOT IN (SELECT a FROM s)")
        .await
        .expect("NOT IN subquery");
    assert!(ids(&reply).is_empty(), "NOT IN with a NULL keeps no row");

    // EXISTS over an *empty* inner (no s row has a > 100) keeps no outer row;
    // NOT EXISTS over the same empty inner keeps them all.
    let reply = client
        .simple_query("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE a > 100)")
        .await
        .expect("EXISTS subquery");
    assert!(ids(&reply).is_empty(), "no s row has a > 100");
    let reply = client
        .simple_query("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE a > 100)")
        .await
        .expect("NOT EXISTS subquery");
    assert_eq!(ids(&reply), vec![1, 2, 3]);

    // A scalar subquery returning more than one row is the standard's 21000.
    let err = client
        .simple_query("SELECT id FROM t WHERE a = (SELECT a FROM s)")
        .await
        .expect_err("a multi-row scalar subquery must fail");
    assert_eq!(err.code(), Some(&SqlState::CARDINALITY_VIOLATION));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_correlated_subqueries() {
    // STL-239 Definition of Done: correlated `EXISTS` / `NOT EXISTS` / `IN` and
    // scalar correlated subqueries return Postgres-equivalent results over the wire,
    // including the `NOT IN`-with-NULL three-valued trap evaluated per outer row.
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let _driver = tokio::spawn(connection);

    // `k` is non-unique, so a correlation on it yields a set per outer row.
    client
        .batch_execute(
            "CREATE TABLE t (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING; \
             CREATE TABLE s (id INT PRIMARY KEY, k INT, a INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    for insert in [
        "INSERT INTO t VALUES (1, 100, 5)",
        "INSERT INTO t VALUES (2, 100, 7)",
        "INSERT INTO t VALUES (3, 200, 9)",
        "INSERT INTO t VALUES (4, 300, 1)",
        "INSERT INTO s VALUES (10, 100, 5)",
        "INSERT INTO s VALUES (11, 100, 6)",
        "INSERT INTO s VALUES (12, 200, NULL)",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // Correlated EXISTS: keep the outer rows whose `k` appears in `s` (100, 200).
    let reply = client
        .simple_query("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.k)")
        .await
        .expect("correlated EXISTS");
    assert_eq!(ids(&reply), vec![1, 2, 3]);

    // Correlated NOT EXISTS keeps the complement (k = 300 has no s row).
    let reply = client
        .simple_query("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k)")
        .await
        .expect("correlated NOT EXISTS");
    assert_eq!(ids(&reply), vec![4]);

    // Correlated IN, per-row set membership:
    //   id 1 (k=100, a=5): {5, 6} ∋ 5 → keep; id 2 (a=7): ∌ → drop; id 3 (k=200,
    //   a=9): {NULL} → not TRUE → drop; id 4 (k=300): {} → drop.
    let reply = client
        .simple_query("SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k)")
        .await
        .expect("correlated IN");
    assert_eq!(ids(&reply), vec![1]);

    // Correlated NOT IN: id 3's inner {NULL} makes `9 NOT IN (NULL)` unknown (the
    // per-row trap, dropped); id 1's `5 NOT IN (5, 6)` is false; id 2's `7 NOT IN
    // (5, 6)` is true → keep; id 4's empty inner makes `NOT IN ()` true → keep.
    let reply = client
        .simple_query("SELECT id FROM t WHERE a NOT IN (SELECT a FROM s WHERE s.k = t.k)")
        .await
        .expect("correlated NOT IN");
    assert_eq!(ids(&reply), vec![2, 4]);

    // Correlated scalar lookup `a = (SELECT a FROM s WHERE s.id = t.id)`: add one
    // `s` row keyed by id so exactly outer id 1 has a matching inner scalar (5 = 5);
    // every other outer row sees an empty inner → NULL → dropped.
    client
        .simple_query("INSERT INTO s VALUES (1, 100, 5)")
        .await
        .expect("insert s.id=1");
    let reply = client
        .simple_query("SELECT id FROM t WHERE a = (SELECT a FROM s WHERE s.id = t.id)")
        .await
        .expect("correlated scalar");
    assert_eq!(ids(&reply), vec![1]);
}
