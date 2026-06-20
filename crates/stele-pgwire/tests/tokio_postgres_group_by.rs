//! End-to-end `GROUP BY` + aggregates over the wire, driven by the real
//! `tokio-postgres` client (STL-171 Definition of Done: "wire test through
//! SessionEngine returns grouped rows").
//!
//! A stock Postgres driver connects to a live [`Server`], creates a three-column
//! table, inserts rows, then runs grouped and ungrouped aggregate queries over
//! the **simple-query** protocol and asserts the grouped result. Proving the
//! aggregate path renders correctly through the front end — not just in-process —
//! is the point.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// Collect a simple-query reply's data rows into `(region → (count, total))`,
/// reading the three columns by name.
fn grouped(messages: &[SimpleQueryMessage]) -> HashMap<Option<String>, (String, String)> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                let region = row.get("region").map(ToOwned::to_owned);
                let count = row.get("count").expect("count column").to_owned();
                let total = row.get("total").expect("total column").to_owned();
                Some((region, (count, total)))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_group_by_aggregates() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // A three-column table: business key `id`, plus value columns `region` and
    // `amount` to group and aggregate over.
    client
        .batch_execute(
            "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT) \
             WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    for insert in [
        "INSERT INTO sales VALUES (1, 'east', 100)",
        "INSERT INTO sales VALUES (2, 'east', 200)",
        "INSERT INTO sales VALUES (3, 'west', 50)",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // Grouped aggregate: a row per region with its count and summed amount.
    let reply = client
        .simple_query(
            "SELECT region, COUNT(*) AS count, SUM(amount) AS total FROM sales GROUP BY region",
        )
        .await
        .expect("group by select");
    let rows = grouped(&reply);
    assert_eq!(rows.len(), 2, "two regions");
    assert_eq!(
        rows.get(&Some("east".to_owned())),
        Some(&("2".to_owned(), "300".to_owned()))
    );
    assert_eq!(
        rows.get(&Some("west".to_owned())),
        Some(&("1".to_owned(), "50".to_owned()))
    );

    // The CommandComplete tag reports the grouped row count (2), not the input.
    assert!(
        reply
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(2))),
        "GROUP BY returns two rows"
    );

    // Ungrouped aggregate over the whole table: one row.
    let reply = client
        .simple_query("SELECT COUNT(*) AS count, SUM(amount) AS total FROM sales")
        .await
        .expect("ungrouped select");
    let total = reply.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some((
            row.get("count").expect("count").to_owned(),
            row.get("total").expect("total").to_owned(),
        )),
        _ => None,
    });
    assert_eq!(total, Some(("3".to_owned(), "350".to_owned())));

    drop(client);
    let _ = driver.await;
}

/// The grouping keys (column `region`) of a simple-query reply's data rows.
fn regions(messages: &[SimpleQueryMessage]) -> Vec<Option<String>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get("region").map(ToOwned::to_owned)),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_runs_having() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT) \
             WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    for insert in [
        "INSERT INTO sales VALUES (1, 'east', 100)",
        "INSERT INTO sales VALUES (2, 'east', 200)",
        "INSERT INTO sales VALUES (3, 'west', 50)",
    ] {
        client.simple_query(insert).await.expect("insert");
    }

    // HAVING filters groups after aggregation (STL-265): only `east` has > 1 sale.
    let reply = client
        .simple_query(
            "SELECT region, COUNT(*) AS count, SUM(amount) AS total FROM sales \
             GROUP BY region HAVING COUNT(*) > 1",
        )
        .await
        .expect("group by having select");
    let rows = grouped(&reply);
    assert_eq!(rows.len(), 1, "only east has more than one sale");
    assert_eq!(
        rows.get(&Some("east".to_owned())),
        Some(&("2".to_owned(), "300".to_owned()))
    );
    // The CommandComplete tag reports the post-HAVING row count (1).
    assert!(
        reply
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(1))),
        "HAVING leaves one group"
    );

    // HAVING can filter on an aggregate the SELECT list never projects: east sums
    // to 300 (> 250), west to 50.
    let reply = client
        .simple_query("SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 250")
        .await
        .expect("having on an unprojected aggregate");
    assert_eq!(
        regions(&reply),
        vec![Some("east".to_owned())],
        "only east sums above 250"
    );

    // A non-grouped column in HAVING is Postgres's 42803 grouping_error — a stock
    // driver classifies it the same as Postgres would.
    let err = client
        .simple_query("SELECT region FROM sales GROUP BY region HAVING amount > 1")
        .await
        .expect_err("an ungrouped HAVING column must fail");
    assert_eq!(
        err.code(),
        Some(&SqlState::GROUPING_ERROR),
        "42803 over the wire, got: {err}"
    );

    drop(client);
    let _ = driver.await;
}
