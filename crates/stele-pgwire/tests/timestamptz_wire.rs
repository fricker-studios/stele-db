//! `TIMESTAMPTZ` round-trip over the Postgres wire protocol, across time-zone
//! offsets (STL-189).
//!
//! Unlike the STL-150 golden (which stages typed values in-process because the
//! SQL `INSERT` path could not yet express them), this drives the *whole* path
//! over the wire on a stock `tokio-postgres` client: `CREATE TABLE` with a
//! `timestamptz` column, two `INSERT`s whose literals name the **same instant in
//! different zones**, and a `SELECT` reading both back. The point is the
//! normalization: a `+05` literal and a `-05` literal that denote one UTC instant
//! must store identically and render identically (`+00`), proving the engine is
//! UTC-internal end-to-end.
//!
//! `timestamptz` is the one civil-time type with a DML literal codec at v0.2
//! ([`stele_common::datetime`]); the zone-less `timestamp` / `date` literal codecs
//! remain deferred (mirrors the `AS OF` stance), and binary-format wire encoding
//! is a separate concern (STL-183 / [G23]).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// Collect a `simple_query` reply into an `id → ts` map over its data rows.
fn rows_by_id(messages: &[SimpleQueryMessage]) -> BTreeMap<String, String> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some((
                row.get("id").expect("id column").to_owned(),
                row.get("ts").expect("ts column").to_owned(),
            )),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_timestamptz_round_trips_across_offsets() {
    let engine = SessionEngine::open(MemDisk::new(), SystemClock);
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    client
        .simple_query(
            "CREATE TABLE events (id INT PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE) \
             WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create timestamptz table over the wire");

    // The two offsets name the *same* instant — 2024-01-15 07:00:00 UTC. A third
    // row pins a distinct instant with fractional seconds and an explicit `Z`, so
    // the rendering is exercised beyond the single normalized value.
    for (id, literal) in [
        (1, "2024-01-15 12:00:00+05"),
        (2, "2024-01-15 02:00:00-05"),
        (3, "2023-11-14 22:13:20.5Z"),
    ] {
        client
            .simple_query(&format!("INSERT INTO events VALUES ({id}, '{literal}')"))
            .await
            .unwrap_or_else(|e| panic!("insert {literal} over the wire: {e}"));
    }

    let rows = rows_by_id(
        &client
            .simple_query("SELECT id, ts FROM events")
            .await
            .expect("select timestamptz over the wire"),
    );

    // Both offset spellings normalized to the same UTC instant and render with the
    // `+00` offset Stele always emits (it is UTC-internal).
    assert_eq!(
        rows.get("1").map(String::as_str),
        Some("2024-01-15 07:00:00+00"),
        "the +05 literal must normalize to 07:00:00 UTC"
    );
    assert_eq!(
        rows.get("2").map(String::as_str),
        Some("2024-01-15 07:00:00+00"),
        "the -05 literal must normalize to the same UTC instant as the +05 one"
    );
    assert_eq!(
        rows.get("1"),
        rows.get("2"),
        "two zone spellings of one instant must round-trip byte-identically"
    );
    assert_eq!(
        rows.get("3").map(String::as_str),
        Some("2023-11-14 22:13:20.5+00"),
        "fractional seconds survive, with the UTC offset after them"
    );

    drop(client);
    driver
        .await
        .expect("connection driver task joined")
        .expect("the connection closed without a protocol error");
}
