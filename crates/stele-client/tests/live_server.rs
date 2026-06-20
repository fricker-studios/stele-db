//! `stele-client` against a **live server** — the STL-255 Definition of Done.
//!
//! The crate's promise is that `stele-client` can health-check, trigger a backup,
//! and read introspection against a running Stele server. This boots an in-process
//! engine behind the admin HTTP/JSON gateway on an ephemeral port — exactly the
//! composition [`stele_server::run`] makes — then drives the **blocking** SDK
//! client at it from a `spawn_blocking` task while the gateway runs on the async
//! runtime, mirroring how the `stele` shell calls it.
//!
//! [STL-255]: https://allegromusic.atlassian.net/browse/STL-255

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use stele_client::{Client, Config, Error};
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_server::admin::http::AdminHttp;
use stele_server::admin::{AdminAuth, AdminService};
use stele_server::ops::{OpsServer, OpsState};
use stele_storage::backend::MemDisk;

/// The bearer token configured on the booted admin surface.
const TOKEN: &str = "stl255-client-token-7b2e";

/// A booted admin surface: the gateway's listen address plus the engine handle
/// the test seeds DDL/DML through so introspection has something to read.
struct Harness {
    addr: SocketAddr,
    engine: Arc<Mutex<SessionEngine<SystemClock, MemDisk>>>,
}

/// Boot an in-memory engine behind the admin HTTP/JSON gateway on an ephemeral
/// ops port, with `TOKEN` configured. Mirrors `stele_server::run`'s composition.
async fn boot() -> Harness {
    let engine = Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let core = AdminService::new(Arc::clone(&engine));
    let auth = Arc::new(AdminAuth::new(vec![TOKEN.to_owned()]));

    let state = Arc::new(OpsState::new());
    let session: SharedSession = engine.clone();
    state.set_ready(session);
    state.set_admin(Arc::new(AdminHttp::new(core, Arc::clone(&auth))));
    let ops = OpsServer::new("127.0.0.1:0".parse().unwrap(), state)
        .bind()
        .await
        .expect("bind ops listener");
    let addr = ops.local_addr();
    tokio::spawn(ops.serve());

    Harness { addr, engine }
}

/// Run SQL statements through the engine to give the test durable, introspectable
/// state.
fn seed(engine: &Arc<Mutex<SessionEngine<SystemClock, MemDisk>>>, sql: &[&str]) {
    let mut engine = engine.lock().unwrap();
    for stmt in sql {
        let parsed = stele_sql::parse(stmt).expect("parse");
        engine.execute(&parsed[0]).expect("execute");
    }
}

/// A unique, fresh (non-existent) temp directory for a backup target.
fn fresh_backup_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("stele-client-{}-{tag}-{n}", std::process::id(),));
    let _ = std::fs::remove_dir_all(&dir); // backup requires an empty/absent target
    dir
}

/// Connection settings for the booted gateway, with the valid bearer token.
fn config(addr: SocketAddr) -> Config {
    Config::new(addr.ip().to_string(), addr.port(), Some(TOKEN.to_owned()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_backup_and_introspection_against_a_live_server() {
    let h = boot().await;
    // Insert then update so the version history has two rows to read back.
    seed(
        &h.engine,
        &[
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            "INSERT INTO account VALUES (1, 100)",
            "UPDATE account SET balance = 200 WHERE id = 1",
        ],
    );

    let config = config(h.addr);
    let dir = fresh_backup_dir("roundtrip");
    let backup_path = dir.to_string_lossy().into_owned();

    // The blocking SDK client runs off the runtime; the gateway serves it on the
    // runtime's worker threads.
    tokio::task::spawn_blocking(move || {
        let client = Client::new(config);

        // 1. Health — the liveness check.
        let health = client.health().expect("health");
        assert!(health.is_serving(), "{health:?}");

        // Engine state: ready, a non-empty version, and our one table.
        let status = client.status().expect("status");
        assert!(status.ready, "{status:?}");
        assert!(!status.wal_poisoned, "{status:?}");
        assert!(!status.server_version.is_empty(), "{status:?}");
        assert_eq!(status.table_count, 1);
        assert_eq!(status.tables[0].name, "account");

        // 2. Backup — trigger a consistent online backup, get its manifest.
        let manifest = client.backup(&backup_path).expect("backup");
        assert!(manifest.file_count >= 1, "a backup writes durable files");
        assert_eq!(
            manifest.commit_head.len(),
            64,
            "commit head is a hex SHA-256"
        );

        // The same client's RestorePlan validates the directory it just produced —
        // the STL-249 manifest + checksum gate, end to end through the SDK.
        let plan = client.restore_plan(&backup_path).expect("restore-plan");
        assert!(plan.valid, "freshly-taken backup must validate: {plan:?}");
        assert_eq!(
            plan.manifest.expect("manifest").commit_head,
            manifest.commit_head
        );
        // A non-existent directory is a verdict (`valid = false`), not an error.
        let bogus = client
            .restore_plan(&format!("{backup_path}-does-not-exist"))
            .expect("restore-plan on a bogus path is still Ok");
        assert!(!bogus.valid);
        assert!(bogus.error.unwrap().contains("does not exist"));

        // 3. Introspection — read the version history through the SDK. An insert
        //    then an update is two versions, with real column headers.
        let versions = client.versions("account", None).expect("versions");
        let names: Vec<&str> = versions.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(
            names.contains(&"txid") && names.contains(&"op"),
            "{names:?}"
        );
        assert_eq!(versions.rows.len(), 2, "insert + update = two versions");

        // A per-key filter folds the SQL literal to the key type.
        let one = client
            .versions("account", Some("1"))
            .expect("versions by key");
        assert_eq!(one.rows.len(), 2, "both versions of key 1");

        // Segment metadata is the other introspection read; it carries a header
        // and renders rows.
        let segments = client.segments("account").expect("segments");
        assert!(!segments.columns.is_empty(), "segment metadata has columns");
    })
    .await
    .expect("blocking client task");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_missing_and_wrong_tokens() {
    let h = boot().await;
    let addr = h.addr;

    tokio::task::spawn_blocking(move || {
        // No token: refused locally, before any socket is opened.
        let no_token = Client::new(Config::new(addr.ip().to_string(), addr.port(), None));
        assert!(matches!(no_token.health(), Err(Error::NoToken)));

        // Wrong token: the gateway answers 401.
        let wrong = Client::new(Config::new(
            addr.ip().to_string(),
            addr.port(),
            Some("not-the-token".to_owned()),
        ));
        match wrong.health() {
            Err(Error::Status { code, .. }) => assert_eq!(code, "401"),
            other => panic!("expected a 401, got {other:?}"),
        }
    })
    .await
    .expect("blocking client task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_table_is_a_not_found_status() {
    let h = boot().await;
    let config = config(h.addr);

    tokio::task::spawn_blocking(move || {
        let client = Client::new(config);
        match client.segments("ghost") {
            Err(Error::Status { code, .. }) => assert_eq!(code, "404"),
            other => panic!("expected a 404 for an unknown table, got {other:?}"),
        }
    })
    .await
    .expect("blocking client task");
}
