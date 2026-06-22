//! End-to-end `JOIN` over the wire, driven by the real `tokio-postgres` client
//! (STL-172 Definition of Done: "wire test through SessionEngine"; STL-323 N-way).
//!
//! A stock Postgres driver connects to a live [`Server`], creates three tables
//! (`users`, `orders`, `products`), inserts rows, then runs each join type — inner,
//! left, semi, anti — and an N-way left-deep chain (`users JOIN orders JOIN
//! products`) over the **simple-query** protocol and asserts the combined result.
//! Proving the join path renders correctly through the front end, not just
//! in-process, is the point.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// Collect a simple-query reply's data rows into a sorted `Vec` of the named
/// columns' values (`None` for a SQL `NULL` cell). A join does not order its
/// output, so callers compare row *sets*.
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

// One self-contained wire session that exercises every join shape (inner / left /
// semi / anti and an N-way chain) end to end — long, but splitting it would
// duplicate the connect + fixture setup without adding coverage.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_each_join_type() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // `users` (id, name), `orders` (oid, uid, pid) joinable on users.id = orders.uid,
    // and `products` (pid, label) joinable on orders.pid = products.pid (STL-323).
    client
        .batch_execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING; \
             CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, pid INT) WITH SYSTEM VERSIONING; \
             CREATE TABLE products (pid INT PRIMARY KEY, label TEXT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    for insert in [
        "INSERT INTO users VALUES (1, 'alice')",
        "INSERT INTO users VALUES (2, 'bob')",
        "INSERT INTO users VALUES (3, 'carol')",
        "INSERT INTO orders VALUES (10, 1, 100)",
        "INSERT INTO orders VALUES (11, 1, 200)",
        "INSERT INTO orders VALUES (12, 2, 100)",
        "INSERT INTO products VALUES (100, 'widget')",
        "INSERT INTO products VALUES (200, 'gadget')",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // INNER: alice has two orders, bob one, carol none.
    let reply = client
        .simple_query(
            "SELECT users.name, orders.oid FROM users JOIN orders ON users.id = orders.uid",
        )
        .await
        .expect("inner join");
    assert_eq!(
        rows(&reply, &["name", "oid"]),
        vec![
            vec![Some("alice".to_owned()), Some("10".to_owned())],
            vec![Some("alice".to_owned()), Some("11".to_owned())],
            vec![Some("bob".to_owned()), Some("12".to_owned())],
        ]
    );

    // LEFT: carol survives with a NULL right side.
    let reply = client
        .simple_query(
            "SELECT users.name, orders.oid FROM users LEFT JOIN orders ON users.id = orders.uid",
        )
        .await
        .expect("left join");
    assert_eq!(
        rows(&reply, &["name", "oid"]),
        vec![
            vec![Some("alice".to_owned()), Some("10".to_owned())],
            vec![Some("alice".to_owned()), Some("11".to_owned())],
            vec![Some("bob".to_owned()), Some("12".to_owned())],
            vec![Some("carol".to_owned()), None],
        ]
    );

    // SEMI: each left row with at least one order, once.
    let reply = client
        .simple_query("SELECT name FROM users SEMI JOIN orders ON users.id = orders.uid")
        .await
        .expect("semi join");
    assert_eq!(
        rows(&reply, &["name"]),
        vec![vec![Some("alice".to_owned())], vec![Some("bob".to_owned())]]
    );

    // ANTI: the left rows with no order.
    let reply = client
        .simple_query("SELECT name FROM users ANTI JOIN orders ON users.id = orders.uid")
        .await
        .expect("anti join");
    assert_eq!(
        rows(&reply, &["name"]),
        vec![vec![Some("carol".to_owned())]]
    );

    // The CommandComplete tag reports the joined row count.
    assert!(
        reply
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(1))),
        "ANTI join returns one row"
    );

    // N-way left-deep chain (STL-323): the second `ON` references the middle input
    // (`orders.pid`), so the chain joins (users ⋈ orders) against products. Both
    // products exist, so every matched order resolves a label; carol is dropped.
    let reply = client
        .simple_query(
            "SELECT users.name, orders.oid, products.label FROM users \
             JOIN orders ON users.id = orders.uid \
             JOIN products ON orders.pid = products.pid",
        )
        .await
        .expect("n-way join");
    assert_eq!(
        rows(&reply, &["name", "oid", "label"]),
        vec![
            vec![
                Some("alice".to_owned()),
                Some("10".to_owned()),
                Some("widget".to_owned())
            ],
            vec![
                Some("alice".to_owned()),
                Some("11".to_owned()),
                Some("gadget".to_owned())
            ],
            vec![
                Some("bob".to_owned()),
                Some("12".to_owned()),
                Some("widget".to_owned())
            ],
        ]
    );

    drop(client);
    let _ = driver.await;
}
