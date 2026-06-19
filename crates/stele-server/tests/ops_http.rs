//! End-to-end ops HTTP listener tests ([STL-253]).
//!
//! Two halves of the ticket's Definition of Done:
//!
//! * **`/readyz` flips correctly** — `503` before recovery, `200` once the
//!   recovered session is installed, back to `503` while the engine reports a
//!   poisoned WAL ([STL-217]), and `200` again when it clears.
//! * **`/metrics` under load** — a stock `tokio-postgres` client drives DDL /
//!   DML / `SELECT` / transactions / `CHECKPOINT` through the real pg-wire
//!   listener, and the scrape must carry the documented series with sane
//!   values; a second scrape after more load proves the counters are
//!   monotonic.
//!
//! [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
//! [STL-217]: https://allegromusic.atlassian.net/browse/STL-217

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use stele_common::metrics::SharedMetrics;
use stele_common::time::SystemClock;
use stele_common::types::LogicalType;
use stele_engine::{
    EngineError, IsolationLevel, SessionEngine, SessionTransaction, StatementOutcome,
    TableDescription,
};
use stele_pgwire::{Server as PgServer, SessionHandle, SharedSession};
use stele_server::ops::{OpsServer, OpsState};
use stele_sql::Statement;
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::NoTls;

/// Bind the ops listener on an ephemeral port over `state` and serve it.
async fn spawn_ops(state: Arc<OpsState>) -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let bound = OpsServer::new(addr, state)
        .bind()
        .await
        .expect("bind ops listener");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

/// One raw `GET`, returning `(status line, body)`.
async fn http_get(addr: SocketAddr, path: &str) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to ops listener");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .expect("write request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf-8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("header/body split");
    let status = head.lines().next().expect("status line").to_owned();
    (status, body.to_owned())
}

/// The value of the metric series named exactly `series` in an exposition
/// `body` (label set included, e.g. `stele_statements_total{kind="select"}`).
fn metric_value(body: &str, series: &str) -> u64 {
    body.lines()
        .find_map(|line| {
            let (name, value) = line.rsplit_once(' ')?;
            (name == series).then(|| value.parse().expect("integer metric value"))
        })
        .unwrap_or_else(|| panic!("series {series} not found in scrape:\n{body}"))
}

/// Process-uptime micros — the same shape the production server installs.
fn test_uptime_micros() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    u64::try_from(EPOCH.get_or_init(Instant::now).elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// A [`SessionHandle`] that delegates to a real engine but lets the test force
/// the poisoned answer — flipping a real WAL fsync failure end-to-end needs
/// the simulator's fault disk, which the probe contract doesn't depend on:
/// `/readyz` keys off [`SessionHandle::is_poisoned`] alone.
struct PoisonToggle {
    inner: SessionEngine<SystemClock, MemDisk>,
    poisoned: Arc<AtomicBool>,
}

impl SessionHandle for PoisonToggle {
    fn execute(&mut self, stmt: &Statement) -> Result<StatementOutcome, EngineError> {
        self.inner.execute(stmt)
    }

    fn describe_live_tables(&self) -> Vec<TableDescription> {
        self.inner.describe_live_tables()
    }

    fn describe(
        &self,
        stmt: &Statement,
    ) -> Result<Option<Vec<(String, LogicalType)>>, EngineError> {
        self.inner.describe(stmt)
    }

    fn describe_in_txn(
        &self,
        stmt: &Statement,
        txn: &SessionTransaction,
    ) -> Result<Option<Vec<(String, LogicalType)>>, EngineError> {
        self.inner.describe_in_txn(stmt, txn)
    }

    fn begin_with_isolation(&self, isolation: IsolationLevel) -> SessionTransaction {
        self.inner.begin_with_isolation(isolation)
    }

    fn execute_in_txn(
        &mut self,
        stmt: &Statement,
        txn: &mut SessionTransaction,
    ) -> Result<StatementOutcome, EngineError> {
        self.inner.execute_in_txn(stmt, txn)
    }

    fn repin_snapshot(&self, txn: &mut SessionTransaction) {
        self.inner.repin_snapshot(txn);
    }

    fn commit(&mut self, txn: SessionTransaction) -> Result<(), EngineError> {
        self.inner.commit(txn)
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed) || self.inner.is_poisoned()
    }

    fn metrics(&self) -> SharedMetrics {
        Arc::clone(self.inner.metrics())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_flips_across_recovery_and_wal_poison() {
    let state = Arc::new(OpsState::new());
    let ops = spawn_ops(Arc::clone(&state)).await;

    // The listener is up before recovery: alive, but not ready.
    let (status, _) = http_get(ops, "/healthz").await;
    assert!(status.contains("200"), "{status}");
    let (status, body) = http_get(ops, "/readyz").await;
    assert!(status.contains("503"), "{status}");
    assert!(body.contains("recovery"), "{body}");
    let (status, _) = http_get(ops, "/metrics").await;
    assert!(status.contains("503"), "{status}");

    // Recovery completes (an empty disk recovers to a fresh session).
    let inner = SessionEngine::recover(MemDisk::new(), SystemClock).expect("recover");
    let poisoned = Arc::new(AtomicBool::new(false));
    let session: SharedSession = Arc::new(Mutex::new(PoisonToggle {
        inner,
        poisoned: Arc::clone(&poisoned),
    }));
    state.set_ready(session);

    let (status, body) = http_get(ops, "/readyz").await;
    assert!(status.contains("200"), "{status}: {body}");
    let (status, body) = http_get(ops, "/metrics").await;
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("stele_connections_total"), "{body}");

    // A poisoned WAL must flip readiness off — the [STL-217] consumer.
    poisoned.store(true, Ordering::Relaxed);
    let (status, body) = http_get(ops, "/readyz").await;
    assert!(status.contains("503"), "{status}");
    assert!(body.contains("poisoned"), "{body}");

    // ... and back, once the engine no longer reports poison (in production
    // that is a restart into recovery; the probe just reflects the engine).
    poisoned.store(false, Ordering::Relaxed);
    let (status, _) = http_get(ops, "/readyz").await;
    assert!(status.contains("200"), "{status}");
}

// One linear load script and its scrape assertions — splitting it would only
// hide the order the load was applied in.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_carry_the_documented_series_under_wire_load() {
    // Boot the same composition `run()` makes: one engine, shared session,
    // pg-wire listener + ops listener, real time source.
    let engine = SessionEngine::open(MemDisk::new(), SystemClock);
    engine.metrics().install_time_source(test_uptime_micros);
    let session: SharedSession = Arc::new(Mutex::new(engine));

    let state = Arc::new(OpsState::new());
    state.set_ready(Arc::clone(&session));
    let ops = spawn_ops(state).await;

    let pg_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let bound = PgServer::new(pg_addr, session)
        .bind()
        .await
        .expect("bind pgwire");
    let pg_addr = bound.local_addr();
    tokio::spawn(bound.serve());

    let conn_str = format!(
        "host=127.0.0.1 port={} user=stele dbname=stele sslmode=disable",
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);

    // Load: DDL, auto-commit DML, a SELECT, a committed and a rolled-back
    // transaction, and an admin CHECKPOINT.
    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert 1");
    client
        .simple_query("INSERT INTO account VALUES (2, 200)")
        .await
        .expect("insert 2");
    client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select");
    client
        .batch_execute("BEGIN; INSERT INTO account VALUES (3, 300); COMMIT")
        .await
        .expect("committed txn");
    client
        .batch_execute("BEGIN; INSERT INTO account VALUES (4, 400); ROLLBACK")
        .await
        .expect("rolled-back txn");
    client.simple_query("CHECKPOINT").await.expect("checkpoint");

    let (status, first) = http_get(ops, "/metrics").await;
    assert!(status.contains("200"), "{status}");

    // Presence + sane values for the documented series.
    assert_eq!(metric_value(&first, "stele_connections_total"), 1);
    assert_eq!(metric_value(&first, "stele_connections_active"), 1);
    assert_eq!(
        metric_value(&first, "stele_statements_total{kind=\"select\"}"),
        1
    );
    assert_eq!(
        metric_value(&first, "stele_statements_total{kind=\"insert\"}"),
        4
    );
    assert_eq!(
        metric_value(&first, "stele_statements_total{kind=\"ddl\"}"),
        1
    );
    assert_eq!(
        metric_value(&first, "stele_statements_total{kind=\"admin\"}"),
        1
    );
    assert_eq!(metric_value(&first, "stele_txn_commits_total"), 1);
    assert_eq!(metric_value(&first, "stele_txn_rollbacks_total"), 1);
    assert_eq!(metric_value(&first, "stele_txn_conflicts_total"), 0);
    assert_eq!(metric_value(&first, "stele_rows_returned_total"), 2);
    assert_eq!(metric_value(&first, "stele_rows_written_total"), 4);
    assert!(
        metric_value(&first, "stele_statement_seconds_count{kind=\"select\"}") >= 1,
        "select latency histogram must have observations"
    );
    assert!(
        metric_value(&first, "stele_wal_appends_total") >= 3,
        "two auto-commit writes + one group commit at minimum"
    );
    assert!(
        metric_value(&first, "stele_wal_fsync_seconds_count") >= 1,
        "CHECKPOINT group-commit ticks the WAL"
    );
    assert_eq!(metric_value(&first, "stele_checkpoint_seconds_count"), 1);
    // Scan accounting is present (zero here: the rows still live in the
    // unsealed delta tier, so no sealed segment was scanned or pruned).
    assert_eq!(metric_value(&first, "stele_scan_segments_scanned_total"), 0);
    assert_eq!(
        metric_value(&first, "stele_scan_segments_pruned_zone_total"),
        0
    );

    // More load, then a second scrape: every counter is monotonic and the
    // exercised ones strictly grew.
    client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select 2");
    client
        .simple_query("SELECT id, balance FROM account")
        .await
        .expect("select 3");

    let (_, second) = http_get(ops, "/metrics").await;
    assert_eq!(
        metric_value(&second, "stele_statements_total{kind=\"select\"}"),
        3,
        "two more SELECTs since the first scrape"
    );
    for series in [
        "stele_connections_total",
        "stele_statements_total{kind=\"insert\"}",
        "stele_txn_commits_total",
        "stele_rows_returned_total",
        "stele_wal_appends_total",
        "stele_wal_fsync_seconds_count",
    ] {
        assert!(
            metric_value(&second, series) >= metric_value(&first, series),
            "{series} went backwards between scrapes"
        );
    }
}
