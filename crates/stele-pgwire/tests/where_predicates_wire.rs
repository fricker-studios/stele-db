//! End-to-end `WHERE` predicates over the Postgres wire (STL-213).
//!
//! STL-213 wires the binder / `run_select` to emit the extended evaluator nodes on
//! the live SQL path: integer `/` and `%` arithmetic, comparisons over the new
//! scalar types (here `timestamptz`), and a per-row `PERIOD(from, to)` predicate
//! lowered to `Expr::Period`. A stock `tokio-postgres` client drives each through
//! the whole parse → bind → execute → encode loop over the **simple-query**
//! protocol, asserting the right rows survive. The engine-side correctness oracles
//! live with the engine (`projection_predicate_oracle`, the inline timestamptz /
//! per-row-period oracles); these prove the same predicates reach the evaluator
//! across the wire.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

mod common;

/// Connect a `tokio-postgres` client to a fresh in-memory server, returning the
/// client and the spawned connection driver (awaited at the end so the test does
/// not leak the task).
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

/// The sorted `id` column of a `simple_query` reply's data rows.
fn ids(messages: &[SimpleQueryMessage]) -> Vec<i32> {
    let mut out: Vec<i32> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                row.get("id")
                    .expect("id column")
                    .parse::<i32>()
                    .expect("id renders as an integer"),
            ),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

/// Run `sql` and return its rows' sorted ids.
async fn select_ids(client: &Client, sql: &str) -> Vec<i32> {
    ids(&client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("select `{sql}`: {e}")))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_division_and_modulo_filter_over_the_wire() {
    let (client, driver) = connect().await;
    client
        .batch_execute("CREATE TABLE n (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create n");
    for (id, v) in [(1, 0), (2, 3), (3, 4), (4, 7), (5, -7)] {
        client
            .simple_query(&format!("INSERT INTO n VALUES ({id}, {v})"))
            .await
            .expect("insert n row");
    }

    // v % 2 = 0 → even v: 0 (id 1) and 4 (id 3).
    assert_eq!(
        select_ids(&client, "SELECT id FROM n WHERE v % 2 = 0").await,
        vec![1, 3]
    );
    // v / 2 = 2 → truncating toward zero: only 4 / 2 (id 3); 7 / 2 = 3, 3 / 2 = 1.
    assert_eq!(
        select_ids(&client, "SELECT id FROM n WHERE v / 2 = 2").await,
        vec![3]
    );
    // The remainder follows the dividend's sign: -7 % 2 = -1 (id 5) only.
    assert_eq!(
        select_ids(&client, "SELECT id FROM n WHERE v % 2 = -1").await,
        vec![5]
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamptz_comparison_filters_over_the_wire() {
    let (client, driver) = connect().await;
    client
        .batch_execute(
            "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE) \
             WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create ev");
    for (id, literal) in [
        (1, "2024-01-15 00:00:00Z"),
        (2, "2024-06-01 12:00:00Z"),
        (3, "2025-01-01 00:00:00Z"),
    ] {
        client
            .simple_query(&format!("INSERT INTO ev VALUES ({id}, '{literal}')"))
            .await
            .expect("insert ev row");
    }

    // A non-equality comparison over a temporal column — rejected before STL-213.
    assert_eq!(
        select_ids(
            &client,
            "SELECT id FROM ev WHERE ts > '2024-03-01 00:00:00Z'"
        )
        .await,
        vec![2, 3]
    );
    // The comparand is normalized to UTC first, so a `-04` spelling of row 2's
    // instant filters by instant, not by lexical form: `08:00-04` == `12:00Z`.
    assert_eq!(
        select_ids(
            &client,
            "SELECT id FROM ev WHERE ts >= '2024-06-01 08:00:00-04'"
        )
        .await,
        vec![2, 3]
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_row_period_predicate_filters_over_the_wire() {
    let (client, driver) = connect().await;
    client
        .batch_execute(
            "CREATE TABLE pr (id INT PRIMARY KEY, vf BIGINT, vt BIGINT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create pr");
    // Each row's own half-open `[vf, vt)` period, built from two value columns.
    for (id, vf, vt) in [(1, 10, 40), (2, 20, 25), (3, 0, 5)] {
        client
            .simple_query(&format!("INSERT INTO pr VALUES ({id}, {vf}, {vt})"))
            .await
            .expect("insert pr row");
    }

    // CONTAINS [20, 30): only [10, 40) (id 1) wholly contains it ([20, 25) is too
    // short, [0, 5) disjoint).
    assert_eq!(
        select_ids(
            &client,
            "SELECT id FROM pr WHERE PERIOD(vf, vt) CONTAINS PERIOD(20, 30)"
        )
        .await,
        vec![1]
    );
    // OVERLAPS [20, 30): [10, 40) and [20, 25) both intersect it; [0, 5) does not.
    assert_eq!(
        select_ids(
            &client,
            "SELECT id FROM pr WHERE PERIOD(vf, vt) OVERLAPS PERIOD(20, 30)"
        )
        .await,
        vec![1, 2]
    );

    drop(client);
    let _ = driver.await;
}
