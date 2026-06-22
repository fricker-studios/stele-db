//! End-to-end admin / control-plane API tests ([STL-254], [ADR-0016]).
//!
//! The ticket's Definition of Done, both transports:
//!
//! * **HTTP/JSON gateway** (curl's face) on the shared ops listener and a real
//!   **gRPC client** both hit `Health` / `Status` / `Backup` end-to-end.
//! * **Token auth**: every method is rejected without a valid bearer token
//!   (HTTP `401`, gRPC `UNAUTHENTICATED`) and accepted with one.
//! * **Backup round-trips**: a backup triggered through the API produces a
//!   directory that the API's own `RestorePlan` then validates (the STL-249
//!   manifest + checksum gate).
//! * **Introspection** (`Versions`) renders real rows through the same path the
//!   SQL wire uses.
//!
//! [STL-254]: https://allegromusic.atlassian.net/browse/STL-254
//! [ADR-0016]: ../../../docs/adr/0016-admin-control-plane-api.md

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{SharedSession, TlsMode, TlsReloader, TlsSettings};
use stele_server::admin::grpc::GrpcAdmin;
use stele_server::admin::http::AdminHttp;
use stele_server::admin::proto::admin_service_client::AdminServiceClient;
use stele_server::admin::proto::{
    BackupRequest, HealthRequest, ReloadTlsRequest, RestorePlanRequest, StatusRequest,
    VersionsRequest,
};
use stele_server::admin::{AdminAuth, AdminService};
use stele_server::ops::{OpsServer, OpsState};
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

/// The bearer token configured on every booted admin surface in this suite.
const TOKEN: &str = "test-admin-token-9f3a";

/// A booted admin surface: the two listen addresses, plus the engine handle the
/// test drives DDL/DML through to give introspection something to read.
struct Harness {
    ops_addr: SocketAddr,
    grpc_addr: SocketAddr,
    engine: Arc<Mutex<SessionEngine<SystemClock, MemDisk>>>,
}

/// Boot an in-memory engine behind both admin transports on ephemeral ports, with
/// `TOKEN` configured. Mirrors the composition `stele_server::run` makes.
async fn boot() -> Harness {
    boot_inner(None).await
}

/// As [`boot`], but installs `reloader` on both transports so the `ReloadTls`
/// trigger has reloadable `[tls]` material (STL-326); `None` mirrors a plaintext /
/// loopback / self-signed-fallback boot, where `ReloadTls` reports nothing to do.
async fn boot_inner(reloader: Option<TlsReloader>) -> Harness {
    let engine = Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let core = AdminService::new(Arc::clone(&engine));
    let auth = Arc::new(AdminAuth::new(vec![TOKEN.to_owned()]));

    // HTTP/JSON gateway on the shared ops listener. The typed handle coerces to
    // the `SharedSession` trait object (method-call clone, so the coercion lands
    // on the binding rather than inside `Arc::clone`'s generic).
    let state = Arc::new(OpsState::new());
    let session: SharedSession = engine.clone();
    state.set_ready(session);
    let mut admin_http = AdminHttp::new(core.clone(), Arc::clone(&auth));
    if let Some(reloader) = &reloader {
        admin_http = admin_http.with_tls_reloader(reloader);
    }
    state.set_admin(Arc::new(admin_http));
    let ops = OpsServer::new("127.0.0.1:0".parse().unwrap(), state)
        .bind()
        .await
        .expect("bind ops listener");
    let ops_addr = ops.local_addr();
    tokio::spawn(ops.serve());

    // gRPC listener on its own ephemeral port.
    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind grpc listener");
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let mut grpc = GrpcAdmin::new(core, auth);
    if let Some(reloader) = &reloader {
        grpc = grpc.with_tls_reloader(reloader);
    }
    tokio::spawn(stele_server::admin::grpc::serve(grpc_listener, grpc));

    Harness {
        ops_addr,
        grpc_addr,
        engine,
    }
}

/// A [`TlsReloader`] over a freshly self-signed cert/key pair written to a unique
/// temp dir — enough material for `reload()` to re-read and succeed, without
/// standing up a TLS listener (the cert-actually-rotates-on-the-wire oracle lives
/// in tests/admin_tls.rs).
fn temp_reloader(tag: &str) -> TlsReloader {
    let key = rcgen::KeyPair::generate().expect("generate key");
    let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("cert params");
    let cert = params.self_signed(&key).expect("self-sign");
    let dir = std::env::temp_dir().join(format!("stele-admin-reload-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, key.serialize_pem()).expect("write key");
    TlsReloader::load(TlsSettings {
        cert: cert_path,
        key: key_path,
        client_ca: None,
        mode: TlsMode::Required,
    })
    .expect("load reloader")
}

/// One raw HTTP request, returning `(status line, body)`. `token`/`body` are
/// optional (an absent token exercises the reject path).
async fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let auth_line = token.map_or_else(String::new, |t| format!("Authorization: Bearer {t}\r\n"));
    let content_line = body.map_or_else(String::new, |b| {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            b.len()
        )
    });
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n{auth_line}{content_line}\r\n{}",
        body.unwrap_or("")
    );
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf-8");
    let (h, b) = text.split_once("\r\n\r\n").expect("head/body split");
    (h.lines().next().unwrap().to_owned(), b.to_owned())
}

/// Connect a gRPC client to `addr` (explicit endpoint → channel, so no reliance
/// on a `String: TryInto<Endpoint>` impl).
async fn connect_grpc(addr: SocketAddr) -> AdminServiceClient<Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect()
        .await
        .expect("grpc connect");
    AdminServiceClient::new(channel)
}

/// A gRPC request carrying the bearer `TOKEN`.
fn authed<T>(message: T) -> Request<T> {
    let mut req = Request::new(message);
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {TOKEN}")).unwrap(),
    );
    req
}

/// A unique, fresh (non-existent) temp directory for a backup target.
fn fresh_backup_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "stele-admin-{}-{}-{tag}-{n}",
        std::process::id(),
        std::thread::current()
            .name()
            .unwrap_or("t")
            .replace("::", "_"),
    ));
    let _ = std::fs::remove_dir_all(&dir); // backup requires an empty/absent target
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_gateway_health_status_and_auth() {
    let h = boot().await;

    // 401 without a token; 200 with.
    let (status, body) = http(h.ops_addr, "GET", "/v1alpha1/health", None, None).await;
    assert!(status.contains("401"), "{status}: {body}");
    let (status, _) = http(h.ops_addr, "GET", "/v1alpha1/health", Some("wrong"), None).await;
    assert!(status.contains("401"), "wrong token must 401: {status}");

    let (status, body) = http(h.ops_addr, "GET", "/v1alpha1/health", Some(TOKEN), None).await;
    assert!(status.contains("200"), "{status}: {body}");
    assert!(body.contains("SERVING"), "{body}");

    let (status, body) = http(h.ops_addr, "GET", "/v1alpha1/status", Some(TOKEN), None).await;
    assert!(status.contains("200"), "{status}");
    assert!(body.contains("\"ready\":true"), "{body}");
    assert!(body.contains("server_version"), "{body}");

    // An unknown admin endpoint is 404 (once authenticated).
    let (status, _) = http(h.ops_addr, "GET", "/v1alpha1/nope", Some(TOKEN), None).await;
    assert!(status.contains("404"), "{status}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_health_status_and_auth() {
    let h = boot().await;
    let mut client = connect_grpc(h.grpc_addr).await;

    // No token → UNAUTHENTICATED.
    let err = client
        .health(Request::new(HealthRequest {}))
        .await
        .expect_err("missing token must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err:?}");

    // With the token → SERVING + a sane status.
    let health = client
        .health(authed(HealthRequest {}))
        .await
        .expect("health");
    assert_eq!(health.into_inner().status, 1, "ServingStatus::SERVING");

    let status = client
        .status(authed(StatusRequest {}))
        .await
        .expect("status");
    let status = status.into_inner();
    assert!(status.ready);
    assert!(!status.wal_poisoned);
    assert!(!status.server_version.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_backup_then_restore_plan_round_trips() {
    let h = boot().await;

    // Give the engine durable content so the backup captures real files.
    {
        let mut engine = h.engine.lock().unwrap();
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
            "INSERT INTO t VALUES (1, 100)",
        ] {
            let stmts = stele_sql::parse(sql).expect("parse");
            engine.execute(&stmts[0]).expect("execute");
        }
    }

    let mut client = connect_grpc(h.grpc_addr).await;

    let dir = fresh_backup_dir("grpc-backup");
    let path = dir.to_string_lossy().into_owned();

    // Trigger a backup through the API.
    let backup = client
        .backup(authed(BackupRequest { path: path.clone() }))
        .await
        .expect("backup")
        .into_inner();
    let manifest = backup.manifest.expect("a manifest summary");
    assert!(manifest.file_count >= 1, "a backup writes durable files");
    assert_eq!(
        manifest.commit_head.len(),
        64,
        "commit head is a hex SHA-256"
    );
    assert!(dir.join("MANIFEST").exists(), "the backup wrote a MANIFEST");

    // The API's own RestorePlan validates the directory it just produced — the
    // STL-249 manifest + checksum gate, end to end through the admin surface.
    let plan = client
        .restore_plan(authed(RestorePlanRequest { path: path.clone() }))
        .await
        .expect("restore plan")
        .into_inner();
    assert!(
        plan.valid,
        "freshly-taken backup must validate: {}",
        plan.error
    );
    assert_eq!(
        plan.manifest.expect("manifest").commit_head,
        manifest.commit_head
    );

    // A non-existent directory is reported invalid, not an error.
    let bogus = client
        .restore_plan(authed(RestorePlanRequest {
            path: format!("{path}-does-not-exist"),
        }))
        .await
        .expect("restore plan on a bogus path is still Ok")
        .into_inner();
    assert!(!bogus.valid);
    assert!(bogus.error.contains("does not exist"), "{}", bogus.error);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_backup_round_trips() {
    let h = boot().await;
    let dir = fresh_backup_dir("http-backup");
    let path = dir.to_string_lossy().into_owned();

    // Backup is a POST with a JSON body; 401 without a token.
    let body = format!("{{\"path\":{}}}", serde_json_string(&path));
    let (status, _) = http(h.ops_addr, "POST", "/v1alpha1/backup", None, Some(&body)).await;
    assert!(status.contains("401"), "no token must 401: {status}");

    let (status, reply) = http(
        h.ops_addr,
        "POST",
        "/v1alpha1/backup",
        Some(TOKEN),
        Some(&body),
    )
    .await;
    assert!(status.contains("200"), "{status}: {reply}");
    assert!(reply.contains("manifest"), "{reply}");
    assert!(dir.join("MANIFEST").exists(), "backup wrote a MANIFEST");

    // RestorePlan over HTTP validates it.
    let (status, reply) = http(
        h.ops_addr,
        "POST",
        "/v1alpha1/restore-plan",
        Some(TOKEN),
        Some(&body),
    )
    .await;
    assert!(status.contains("200"), "{status}: {reply}");
    assert!(reply.contains("\"valid\":true"), "{reply}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn introspection_versions_renders_rows() {
    let h = boot().await;

    // Give the engine some history to introspect.
    {
        let mut engine = h.engine.lock().unwrap();
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
            "INSERT INTO t VALUES (1, 100)",
            "UPDATE t SET v = 200 WHERE id = 1",
        ] {
            let stmts = stele_sql::parse(sql).expect("parse");
            engine.execute(&stmts[0]).expect("execute");
        }
    }

    let mut client = connect_grpc(h.grpc_addr).await;

    // The whole-table version list: an INSERT then an UPDATE = two versions.
    let table = client
        .versions(authed(VersionsRequest {
            table: "t".to_owned(),
            key: None,
        }))
        .await
        .expect("versions")
        .into_inner();
    let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        names.contains(&"txid") && names.contains(&"op"),
        "{names:?}"
    );
    assert_eq!(table.rows.len(), 2, "insert + update = two versions");

    // A per-key filter, passed as a SQL literal, folds to the key type.
    let one = client
        .versions(authed(VersionsRequest {
            table: "t".to_owned(),
            key: Some("1".to_owned()),
        }))
        .await
        .expect("versions by key")
        .into_inner();
    assert_eq!(one.rows.len(), 2, "both versions of key 1");

    // An unknown table is NOT_FOUND.
    let err = client
        .versions(authed(VersionsRequest {
            table: "ghost".to_owned(),
            key: None,
        }))
        .await
        .expect_err("unknown table");
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_tls_requires_a_token() {
    // Auth gates the trigger before it ever consults the reloader, so a tokenless
    // call is rejected even on a boot with no reloadable material.
    let h = boot().await;

    let (status, _) = http(h.ops_addr, "POST", "/v1alpha1/reload-tls", None, Some("")).await;
    assert!(status.contains("401"), "no token must 401: {status}");

    let mut client = connect_grpc(h.grpc_addr).await;
    let err = client
        .reload_tls(Request::new(ReloadTlsRequest {}))
        .await
        .expect_err("missing token must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_tls_without_reloadable_material_is_a_precondition_failure() {
    // A plaintext / loopback / self-signed-fallback boot has nothing to reload: the
    // request is well-formed but the server cannot satisfy it — HTTP 409 / gRPC
    // FAILED_PRECONDITION, the live posture untouched.
    let h = boot().await;

    let (status, body) = http(
        h.ops_addr,
        "POST",
        "/v1alpha1/reload-tls",
        Some(TOKEN),
        Some(""),
    )
    .await;
    assert!(status.contains("409"), "{status}: {body}");
    assert!(body.contains("no reloadable"), "{body}");

    let mut client = connect_grpc(h.grpc_addr).await;
    let err = client
        .reload_tls(authed(ReloadTlsRequest {}))
        .await
        .expect_err("no reloadable material must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_tls_with_a_reloader_succeeds_on_both_transports() {
    // With reloadable [tls] material installed, an authenticated trigger reloads it
    // and echoes the cert path back — the cross-platform, signal-free path that
    // Windows operators take in place of SIGHUP.
    let reloader = temp_reloader("api-success");
    let h = boot_inner(Some(reloader)).await;

    let (status, body) = http(
        h.ops_addr,
        "POST",
        "/v1alpha1/reload-tls",
        Some(TOKEN),
        Some(""),
    )
    .await;
    assert!(status.contains("200"), "{status}: {body}");
    assert!(body.contains("\"reloaded\":true"), "{body}");
    assert!(body.contains("server.crt"), "echoes the cert path: {body}");

    let mut client = connect_grpc(h.grpc_addr).await;
    let reply = client
        .reload_tls(authed(ReloadTlsRequest {}))
        .await
        .expect("reload over gRPC")
        .into_inner();
    assert!(
        reply.cert_path.ends_with("server.crt"),
        "cert path echoed: {}",
        reply.cert_path
    );
}

/// Minimal JSON string-escape for embedding a filesystem path in a request body
/// (paths here never contain control characters; quote/backslash suffice).
fn serde_json_string(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}
