//! End-to-end `JOIN` over the wire, driven by the real `tokio-postgres` client
//! (STL-172 Definition of Done: "wire test through SessionEngine").
//!
//! A stock Postgres driver connects to a live [`Server`], creates two tables
//! (`users` and `orders`), inserts rows, then runs each join type — inner, left,
//! semi, anti — over the **simple-query** protocol and asserts the combined
//! result. Proving the join path renders correctly through the front end, not just
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_each_join_type() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // `users` (id, name) and `orders` (oid, uid), joinable on users.id = orders.uid.
    client
        .batch_execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING; \
             CREATE TABLE orders (oid INT PRIMARY KEY, uid INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    for insert in [
        "INSERT INTO users VALUES (1, 'alice')",
        "INSERT INTO users VALUES (2, 'bob')",
        "INSERT INTO users VALUES (3, 'carol')",
        "INSERT INTO orders VALUES (10, 1)",
        "INSERT INTO orders VALUES (11, 1)",
        "INSERT INTO orders VALUES (12, 2)",
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

    drop(client);
    let _ = driver.await;
}

/// Read-your-own-writes over a join ([STL-325]): a `SELECT … JOIN …` inside a
/// transaction reflects the transaction's own buffered writes on **either** side,
/// while another connection sees only committed state until `COMMIT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_join_reads_its_own_writes() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect writer");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect other reader");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING; \
         CREATE TABLE orders (oid INT PRIMARY KEY, uid INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create tables");
    // Committed base: alice(1) with order 10.
    a.simple_query("INSERT INTO users VALUES (1, 'alice')")
        .await
        .expect("seed user");
    a.simple_query("INSERT INTO orders VALUES (10, 1)")
        .await
        .expect("seed order");

    let join = "SELECT users.name, orders.oid FROM users JOIN orders ON users.id = orders.uid";
    let full = vec![
        vec![Some("alicia".to_owned()), Some("10".to_owned())],
        vec![Some("bob".to_owned()), Some("11".to_owned())],
        vec![Some("carol".to_owned()), Some("12".to_owned())],
    ];

    // In a transaction, buffer writes to BOTH sides: rename alice, add bob + carol
    // and their orders.
    a.simple_query("BEGIN").await.expect("begin");
    a.simple_query("UPDATE users SET name = 'alicia' WHERE id = 1")
        .await
        .expect("buffered update");
    a.simple_query("INSERT INTO users VALUES (2, 'bob')")
        .await
        .expect("buffered insert user 2");
    a.simple_query("INSERT INTO users VALUES (3, 'carol')")
        .await
        .expect("buffered insert user 3");
    a.simple_query("INSERT INTO orders VALUES (11, 2)")
        .await
        .expect("buffered insert order 11");
    a.simple_query("INSERT INTO orders VALUES (12, 3)")
        .await
        .expect("buffered insert order 12");

    // The in-transaction join reflects the buffered writes on both sides.
    let mine = a.simple_query(join).await.expect("in-txn join");
    assert_eq!(
        rows(&mine, &["name", "oid"]),
        full,
        "the in-transaction join reads its own buffered writes on both sides",
    );

    // Another connection still sees only the committed base join.
    let other = b
        .simple_query(join)
        .await
        .expect("other reader mid-transaction");
    assert_eq!(
        rows(&other, &["name", "oid"]),
        vec![vec![Some("alice".to_owned()), Some("10".to_owned())]],
        "another connection sees only committed state until COMMIT",
    );

    a.simple_query("COMMIT").await.expect("commit");

    // After COMMIT the other connection sees the full join.
    let after = b
        .simple_query(join)
        .await
        .expect("other reader after commit");
    assert_eq!(
        rows(&after, &["name", "oid"]),
        full,
        "the committed join is visible to the other connection",
    );

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

/// `ROLLBACK` discards a join's buffered writes ([STL-325]): after rolling back, the
/// join reads only the committed base — nothing reached storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_join_rollback_discards() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT) WITH SYSTEM VERSIONING; \
             CREATE TABLE orders (oid INT PRIMARY KEY, uid INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create tables");
    client
        .simple_query("INSERT INTO users VALUES (1, 'alice')")
        .await
        .expect("seed user");
    client
        .simple_query("INSERT INTO orders VALUES (10, 1)")
        .await
        .expect("seed order");

    let join = "SELECT users.name, orders.oid FROM users JOIN orders ON users.id = orders.uid";
    let base = vec![vec![Some("alice".to_owned()), Some("10".to_owned())]];

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO users VALUES (2, 'bob')")
        .await
        .expect("buffered insert user");
    client
        .simple_query("INSERT INTO orders VALUES (11, 2)")
        .await
        .expect("buffered insert order");
    // The transaction sees its own buffered join row before rolling back.
    let mid = client.simple_query(join).await.expect("in-txn join");
    assert_eq!(
        rows(&mid, &["name", "oid"]),
        vec![
            vec![Some("alice".to_owned()), Some("10".to_owned())],
            vec![Some("bob".to_owned()), Some("11".to_owned())],
        ],
        "the transaction reads its own buffered join row",
    );

    client.simple_query("ROLLBACK").await.expect("rollback");

    // Nothing reached storage: only the committed base join remains.
    let after = client.simple_query(join).await.expect("after rollback");
    assert_eq!(
        rows(&after, &["name", "oid"]),
        base,
        "the rolled-back buffer never applied; only the committed base join remains",
    );

    drop(client);
    let _ = driver.await;
}
