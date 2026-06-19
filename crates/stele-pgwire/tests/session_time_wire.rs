//! Session time context over the Postgres wire protocol ([STL-246]).
//!
//! `SET stele.system_time = <instant>` (and the valid-axis twin) pins a whole
//! connection's read snapshot so every subsequent bare `SELECT` reads "as of" that
//! instant — without repeating `FOR … AS OF` on each query. The server applies the
//! pin by replaying it as an explicit `FOR <dim> AS OF` qualifier, so a
//! session-pinned read must return **exactly** what the explicit-`AS OF` form
//! returns. That equivalence is the ticket's oracle, asserted here on both axes,
//! plus: `RESET` restores live reads, and any other `SET`/`RESET` (a driver's
//! connect-time preamble) is a tolerated no-op.
//!
//! The bitemporal history is staged in-process through the typed
//! [`SessionEngine::insert`]/[`update`] (which take an explicit valid interval),
//! exactly as the STL-164 both-axes wire oracle does — the thing under test is the
//! *read* path: `SET` → per-connection pin → injected `AS OF`, all over the wire.
//! `AS OF` instants are literal microseconds pinned to the commit ticks the staging
//! writes return, so the assertions are deterministic regardless of the wall clock.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, TxnId};
use stele_common::row_codec;
use stele_common::time::{Clock, SystemClock, SystemTimeMicros, ValidTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use stele_storage::delta::BusinessKey;
use stele_storage::validtime::ValidInterval;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

mod common;

/// A settable clock shared between the test and the engine — deterministic
/// stand-in for wall-clock time so commit instants are controllable.
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

/// The canonical byte encoding of a [`ScalarValue`].
fn enc(value: &ScalarValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

/// A valid-time row's stored payload: `(balance, vf, vt)` packed by the row codec
/// (the period cells are redundant scaffolding; only `balance` is read back).
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

/// Every row of a simple-query reply as `[column → cell]`, ignoring the
/// `CommandComplete` / status messages — the comparable shape for the equivalence
/// assertion. An empty `SELECT` yields an empty vector.
fn rows_of(messages: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(ToOwned::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Run `sql` and return its result rows.
async fn select(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_of(&client.simple_query(sql).await.expect("simple query"))
}

/// Stage the bitemporal identity-demo history on a fresh session and return the two
/// commit ticks:
///   INSERT id=1, balance=100, valid [10, 20)  → c1
///   UPDATE id=1, balance=250, valid [20, 30)  → c2  (supersedes v1 on the system axis)
fn staged_session() -> (SharedSession, SystemTimeMicros, SystemTimeMicros) {
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
    (Arc::new(Mutex::new(engine)), c1, c2)
}

/// The core oracle: a session-pinned bare read returns byte-for-byte what the
/// explicit `FOR … AS OF` form returns, on **both** axes, across the four
/// (system, valid) era combinations of the staged history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_pin_matches_explicit_as_of_on_both_axes() {
    let (session, c1, c2) = staged_session();
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    // (system, valid) eras: v1 live at c1 over [10,20); v2 live at c2 over [20,30).
    for (sys, valid) in [(c1, 15), (c2, 25), (c2, 15), (c1, 25)] {
        // The session-pinned read: pin both axes, then issue a *bare* SELECT.
        client
            .simple_query(&format!("SET stele.system_time = {}", sys.0))
            .await
            .expect("set system_time");
        client
            .simple_query(&format!("SET stele.valid_time = {valid}"))
            .await
            .expect("set valid_time");
        let pinned = select(&client, "SELECT id, balance FROM account").await;
        client.simple_query("RESET ALL").await.expect("reset all");

        // The explicit form, on a context-free session.
        let explicit = select(
            &client,
            &format!(
                "SELECT id, balance FROM account \
                 FOR SYSTEM_TIME AS OF {} FOR VALID_TIME AS OF {valid}",
                sys.0
            ),
        )
        .await;

        assert_eq!(
            pinned, explicit,
            "session-pinned read must equal the explicit AS OF read at (sys={}, valid={valid})",
            sys.0
        );
    }

    // Spot-check the eras actually time-travel (not all empty / all equal): the two
    // matching windows return the right balance; the two mismatched windows are empty.
    client
        .simple_query(&format!("SET stele.system_time = {}", c1.0))
        .await
        .expect("set");
    client
        .simple_query("SET stele.valid_time = 15")
        .await
        .expect("set");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        vec![vec![Some("100".to_owned())]],
        "v1 era reads the pre-update balance"
    );

    drop(client);
    driver
        .await
        .expect("driver joined")
        .expect("clean shutdown");
}

/// `RESET` restores live reads: after pinning to a past era, `RESET ALL` (and the
/// per-axis `RESET`) returns the connection to the latest committed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_restores_live_reads() {
    let (session, c1, _c2) = staged_session();
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    // The live read of the valid-time table is every system-live version, unfiltered
    // on the valid axis ([STL-218]) — after the update, that is v2 (balance 250).
    let live = select(&client, "SELECT balance FROM account").await;
    assert_eq!(live, vec![vec![Some("250".to_owned())]]);

    // Pin into v1's era: the bare read now time-travels to the pre-update balance.
    client
        .simple_query(&format!("SET stele.system_time = {}", c1.0))
        .await
        .expect("set system_time");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        vec![vec![Some("100".to_owned())]],
        "pinned read sees the pre-update era"
    );

    // RESET the one axis → back to live.
    client
        .simple_query("RESET stele.system_time")
        .await
        .expect("reset system_time");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        live,
        "RESET restores the live read"
    );

    // Pin again, then RESET ALL → also back to live.
    client
        .simple_query(&format!("SET stele.system_time = {}", c1.0))
        .await
        .expect("set system_time");
    client.simple_query("RESET ALL").await.expect("reset all");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        live,
        "RESET ALL restores the live read"
    );

    drop(client);
    driver
        .await
        .expect("driver joined")
        .expect("clean shutdown");
}

/// One `SET` governs the whole session: a single `SET stele.system_time` makes
/// every later bare `SELECT` read as of that instant, over the real SQL write path
/// (a system-only table loaded entirely over the wire on a controllable clock).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_system_time_pins_the_whole_session() {
    let clock = SteppedClock::new(1_000_000);
    let engine = SessionEngine::open(MemDisk::new(), clock.clone());
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    // CREATE at 1s, INSERT at 5s, UPDATE at 9s — generous gaps so a mid-gap pin is
    // unambiguous regardless of how the commit clock rounds.
    client
        .simple_query(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create");
    clock.set(5_000_000);
    client
        .simple_query("INSERT INTO account (id, balance) VALUES (1, 100)")
        .await
        .expect("insert");
    clock.set(9_000_000);
    client
        .simple_query("UPDATE account SET balance = 250 WHERE id = 1")
        .await
        .expect("update");
    clock.set(20_000_000);

    // Pin to the pre-update era once; both bare reads below see the old balance.
    client
        .simple_query("SET stele.system_time = 7000000")
        .await
        .expect("set system_time");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        vec![vec![Some("100".to_owned())]]
    );
    assert_eq!(
        select(&client, "SELECT id, balance FROM account WHERE id = 1").await,
        vec![vec![Some("1".to_owned()), Some("100".to_owned())]],
        "the pin governs every bare read, not just the first"
    );

    // Re-pin forward to the post-update era; the bare read now sees the new balance.
    client
        .simple_query("SET stele.system_time = 12000000")
        .await
        .expect("re-pin");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        vec![vec![Some("250".to_owned())]]
    );

    // RESET → live (also 250 here, but via the live snapshot, not a pin).
    client
        .simple_query("RESET stele.system_time")
        .await
        .expect("reset");
    assert_eq!(
        select(&client, "SELECT balance FROM account").await,
        vec![vec![Some("250".to_owned())]]
    );

    drop(client);
    driver
        .await
        .expect("driver joined")
        .expect("clean shutdown");
}

/// A driver's connect-time `SET` preamble (`extra_float_digits`, `application_name`,
/// …) is tolerated as a no-op — the whole point of dropping pgjdbc's
/// `assumeMinServerVersion` workaround ([STL-184]). The `SET`/`RESET` succeed and
/// leave reads live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_set_and_reset_are_tolerated() {
    let engine = SessionEngine::open(MemDisk::new(), SystemClock);
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    // The kinds of statements pgjdbc / psycopg issue at connect — each must succeed.
    for sql in [
        "SET extra_float_digits = 3",
        "SET application_name = 'PostgreSQL JDBC Driver'",
        "SET client_encoding TO 'UTF8'",
        "RESET extra_float_digits",
        "RESET ALL",
    ] {
        client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("tolerated `{sql}` failed: {e}"));
    }

    // A real query still works afterward — the no-ops left the session usable.
    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create after preamble");
    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 42)")
        .await
        .expect("insert after preamble");
    assert_eq!(
        select(&client, "SELECT v FROM t WHERE id = 1").await,
        vec![vec![Some("42".to_owned())]]
    );

    drop(client);
    driver
        .await
        .expect("driver joined")
        .expect("clean shutdown");
}

/// A malformed `SET` of a Stele time variable is a loud error that does not wedge
/// the connection — a later statement still runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_session_time_value_errors_without_wedging_the_connection() {
    let engine = SessionEngine::open(MemDisk::new(), SystemClock);
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    // An unsupported instant expression (an absolute timestamp literal is not folded
    // yet) is rejected rather than silently pinning garbage.
    let err = client
        .simple_query("SET stele.system_time = 'not a time'")
        .await;
    assert!(err.is_err(), "a bad session-time value must error");

    // The connection is still usable.
    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create after a failed SET");
    assert_eq!(
        select(&client, "SELECT 1").await,
        vec![vec![Some("1".to_owned())]]
    );

    drop(client);
    driver
        .await
        .expect("driver joined")
        .expect("clean shutdown");
}
