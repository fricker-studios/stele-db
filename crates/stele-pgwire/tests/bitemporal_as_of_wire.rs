//! Bitemporal `AS OF` (system + valid) end-to-end over the Postgres wire
//! protocol (STL-164).
//!
//! The system-time half of this — `FOR SYSTEM_TIME AS OF` time-travel over the
//! wire — is already covered by the five-minute identity-demo smoke
//! (`ci/identity-demo-smoke.sh`) and the STL-147 CRUD round-trip. This test adds
//! the *second axis*: a stock `tokio-postgres` client runs
//! `SELECT … FOR SYSTEM_TIME AS OF s FOR VALID_TIME AS OF v` against a live
//! [`Server`] and gets back the one cell live on **both** axes at `(s, v)`.
//!
//! ## Why the history is staged in-process
//!
//! Same reason as the STL-150 golden: the SQL `INSERT`/`UPDATE` path cannot yet
//! set a valid-time interval — it always writes `valid: None`, which a valid-time
//! table rejects ([`frame_payload`](stele_storage::validtime::frame_payload)
//! returns `ValidTimeRequired`). Expressing an interval in SQL is the deferred
//! binder work this ticket explicitly excludes. So the bitemporal *history* is
//! staged through the typed [`SessionEngine::insert`]/[`update`] (which take an
//! explicit `Option<ValidInterval>`), and the one thing under test — the
//! both-axes `AS OF` *read* routed through `SessionEngine::execute` and the
//! pgwire query loop — is exercised entirely over the wire. The underlying
//! resolution is the oracle-backed one from STL-163; this proves the glue reaches
//! it end-to-end.
//!
//! The `AS OF` instants are literal microseconds (`resolve_as_of` reads a bare
//! integer as micros), pinned to the exact commit ticks the staging writes
//! return — so the assertion is deterministic regardless of the wall clock.

use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, TxnId};
use stele_common::row_codec;
use stele_common::time::{SystemClock, SystemTimeMicros, ValidTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use stele_storage::delta::BusinessKey;
use stele_storage::validtime::ValidInterval;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The canonical byte encoding of a [`ScalarValue`] — the same bytes the SQL
/// write path folds a literal to, so a staged row reads back identically.
fn enc(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// A valid-time row's stored payload: the three value columns `(balance, vf, vt)`
/// packed by the row codec. The valid interval itself rides the framed prefix
/// `engine.insert` adds, so the period cells are redundant scaffolding — only
/// `balance` is read back. (Materializing the period columns from the interval is
/// the deferred binder/executor work; see the module header.)
fn payload(balance: i32, from: i64, to: i64) -> Option<Vec<u8>> {
    row_codec::encode_payload(&[
        Some(enc(&ScalarValue::Int4(balance))),
        Some(enc(&ScalarValue::Timestamp(from))),
        Some(enc(&ScalarValue::Timestamp(to))),
    ])
}

fn iv(from: i64, to: i64) -> ValidInterval {
    ValidInterval::new(ValidTimeMicros(from), ValidTimeMicros(to)).expect("well-formed interval")
}

/// The single `balance` cell of a one-row reply, or `None` when the reply carried
/// no rows (no version live on both axes).
fn balance(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => {
            Some(row.get("balance").expect("balance column").to_owned())
        }
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_postgres_reads_a_both_axes_as_of_cell() {
    // Stage the valid-axis sibling of the identity demo on a fresh session:
    //   INSERT id=1, balance=100, valid [10, 20)  → commit c1
    //   UPDATE id=1, balance=250, valid [20, 30)  → commit c2
    // The update supersedes the insert on the system axis and carries a disjoint
    // valid window, so the two axes select independently.
    let mut engine = SessionEngine::open(MemDisk::new(), SystemClock);
    let create = stele_sql::parse(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    )
    .expect("parse CREATE")
    .into_iter()
    .next()
    .expect("one statement");
    engine.execute(&create).expect("create valid-time table");

    let who = || Principal::new(b"stele".to_vec());
    let key = || BusinessKey::new(enc(&ScalarValue::Int4(1)));
    let c1 = engine
        .insert(
            "account",
            key(),
            Some(iv(10, 20)),
            payload(100, 10, 20),
            0,
            TxnId(1),
            who(),
        )
        .expect("stage insert")
        .commit;
    let c2 = engine
        .update(
            "account",
            key(),
            Some(iv(20, 30)),
            payload(250, 20, 30),
            0,
            TxnId(2),
            who(),
        )
        .expect("stage update")
        .commit;
    assert!(
        c1.0 < c2.0,
        "the update must commit strictly after the insert"
    );

    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // The both-axes read, parameterized by the two literal-microsecond instants.
    let read = |sys: SystemTimeMicros, valid: i64| {
        let client = &client;
        async move {
            let sql = format!(
                "SELECT balance FROM account \
                 FOR SYSTEM_TIME AS OF {} FOR VALID_TIME AS OF {valid}",
                sys.0
            );
            balance(
                &client
                    .simple_query(&sql)
                    .await
                    .expect("both-axes select over the wire"),
            )
        }
    };

    // Pre-update system + first valid window → 100.
    assert_eq!(read(c1, 15).await.as_deref(), Some("100"));
    // Post-update system + second valid window → 250.
    assert_eq!(read(c2, 25).await.as_deref(), Some("250"));
    // Post-update system + first valid window → no row: v1 is superseded on the
    // system axis and v2's window `[20, 30)` excludes 15. Only the valid instant
    // differs from the 250 case, so the valid axis is load-bearing over the wire.
    assert_eq!(read(c2, 15).await, None);
    // Pre-update system + second valid window → no row: v1 is system-live but its
    // window `[10, 20)` excludes 25. Only the system instant differs from the 100
    // case, so the system axis is load-bearing too.
    assert_eq!(read(c1, 25).await, None);

    drop(client);
    let _ = driver.await;
}
