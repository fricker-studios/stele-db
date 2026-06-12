//! Wire-level oracle for `AS OF now()` tracking real time between writes
//! ([STL-227]).
//!
//! The reported repro: insert, update, let the database sit **idle**, then ask
//! for `FOR SYSTEM_TIME AS OF (now() - interval '1 second')`. Before the fix the
//! read snapshot came from the commit clock's high-water mark — frozen at the
//! last commit while idle — so `now() - 1s` resolved to one second before the
//! *update* forever, and the query kept returning the pre-update value no matter
//! how much real time had passed. With the engine observing its clock per fresh
//! snapshot, the offsets select system-time eras the way a Postgres user expects.
//!
//! The session's inner clock is a settable [`SteppedClock`] shared with the test,
//! so "wait 10 seconds" is a deterministic step, not a real sleep.
//!
//! [STL-227]: https://allegromusic.atlassian.net/browse/STL-227

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// A settable clock shared between the test (which steps it) and the session
/// engine (which observes it) — deterministic stand-in for wall-clock idle time.
#[derive(Debug, Clone)]
struct SteppedClock(Arc<AtomicI64>);

impl SteppedClock {
    fn new(start: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start)))
    }

    fn set(&self, micros: i64) {
        self.0.store(micros, Ordering::Release);
    }
}

impl Clock for SteppedClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.load(Ordering::Acquire))
    }
}

/// The single projected `balance` cell of a simple-query result, or `None` when
/// the query returned no row.
fn balance(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(ToOwned::to_owned),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn as_of_now_minus_interval_tracks_idle_time_over_the_wire() {
    let clock = SteppedClock::new(1_000_000_000);
    let engine = SessionEngine::open(MemDisk::new(), clock.clone());
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // The reporter's timeline, driven entirely over the wire: insert at t+10s,
    // update at t+15s, then 12 idle seconds with no commits.
    client
        .simple_query(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create over the wire");
    clock.set(1_010_000_000);
    client
        .simple_query("INSERT INTO account (id, balance) VALUES (1, 100)")
        .await
        .expect("insert over the wire");
    clock.set(1_015_000_000);
    client
        .simple_query("UPDATE account SET balance = 250 WHERE id = 1")
        .await
        .expect("update over the wire");
    clock.set(1_027_000_000);

    let read = |offset: &'static str| {
        let client = &client;
        async move {
            let sql = format!(
                "SELECT balance FROM account \
                 FOR SYSTEM_TIME AS OF (now() - interval '{offset}') WHERE id = 1"
            );
            balance(&client.simple_query(&sql).await.expect("as-of select"))
        }
    };

    // now() - 1s lands well past the update: the new value, even though the
    // last commit was 12 (virtual) seconds ago — the frozen-`now()` bug would
    // have answered 100 here.
    assert_eq!(read("1 second").await.as_deref(), Some("250"));
    // now() - 15s lands between the insert and the update: the old value.
    assert_eq!(read("15 second").await.as_deref(), Some("100"));
    // now() - 20s lands after the CREATE but before the insert: no row yet.
    assert_eq!(read("20 second").await, None);

    drop(client);
    driver.await.expect("driver").expect("clean shutdown");
}
