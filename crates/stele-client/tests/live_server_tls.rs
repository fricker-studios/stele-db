//! `stele-client` against a **TLS-terminated** admin gateway — the STL-320
//! Definition of Done.
//!
//! STL-311 taught the admin surface to terminate TLS; this proves the SDK reaches
//! it encrypted. The test boots the same in-process engine behind the ops HTTP/JSON
//! gateway as [`live_server`](super), but wraps the listener in the shared `[tls]`
//! material (`ServerTls::load` + [`AcceptorSource`]) exactly as
//! [`stele_server::run`] does, then drives the **blocking** SDK client at
//! `https://` from a `spawn_blocking` task.
//!
//! Coverage:
//! * **verified** (`Tls::verify`): handshake + a health / status / backup
//!   round-trip over TLS, the gateway pinned to a CA and verified by host name.
//! * **encrypt-only** (`Tls::encrypt`): the same handshake with no trust anchor
//!   (libpq's `require`) — encryption without a CA on hand.
//! * **plaintext is refused**: a `tls: None` client against the TLS listener fails
//!   (the token never crosses in cleartext).
//! * **a wrong CA is refused**: `verify` against a CA that did not sign the server
//!   certificate fails the handshake — `verify` is real, not theatre.
//!
//! [STL-320]: https://allegromusic.atlassian.net/browse/STL-320

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use stele_client::{Client, Config, Error, Tls};
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{ServerTls, SharedSession, TlsMode, TlsSettings};
use stele_server::admin::http::AdminHttp;
use stele_server::admin::{AdminAuth, AdminService};
use stele_server::ops::{OpsServer, OpsState};
use stele_server::tls::AcceptorSource;
use stele_storage::backend::MemDisk;

/// The bearer token configured on the booted admin surface.
const TOKEN: &str = "stl320-client-tls-token-9f4c";

/// The DNS SAN the server certificate carries. The client connects to
/// `127.0.0.1` but verifies against this name (set via `Tls::with_server_name`),
/// the libpq IP-connect / DNS-verify pattern.
const SERVER_NAME: &str = "localhost";

// ---------------------------------------------------------------------------
// Test PKI (minted fresh per test; the server half written to disk as PEM)
// ---------------------------------------------------------------------------

/// A self-signed CA that can sign leaf certificates.
struct TestCa {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    pem: String,
}

fn mint_ca(name: &str) -> TestCa {
    let key = rcgen::KeyPair::generate().expect("generate CA key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    let cert = params.clone().self_signed(&key).expect("self-sign CA");
    TestCa {
        issuer: rcgen::Issuer::new(params, key),
        pem: cert.pem(),
    }
}

/// A server leaf `(cert, key)` PEM pair signed by `ca`, carrying [`SERVER_NAME`]
/// as a DNS SAN.
fn mint_server_leaf(ca: &TestCa) -> (String, String) {
    let key = rcgen::KeyPair::generate().expect("generate leaf key");
    let mut params =
        rcgen::CertificateParams::new(vec![SERVER_NAME.to_owned()]).expect("leaf params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "stele admin-tls server");
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.signed_by(&key, &ca.issuer).expect("sign leaf");
    (cert.pem(), key.serialize_pem())
}

/// One test's certificate material on disk: the server cert/key (`ServerTls::load`
/// input), the CA that signed it, and an *unrelated* CA for the wrong-anchor case.
struct Pki {
    server_cert: PathBuf,
    server_key: PathBuf,
    trusted_ca: PathBuf,
    wrong_ca: PathBuf,
}

fn mint_pki(test: &str) -> Pki {
    let server_ca = mint_ca("stele admin-tls server CA");
    let (server_cert_pem, server_key_pem) = mint_server_leaf(&server_ca);
    let wrong_ca = mint_ca("stele admin-tls unrelated CA");

    let dir = std::env::temp_dir().join(format!("stele-client-tls-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let write = |name: &str, pem: &str| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).expect("write PEM");
        path
    };
    Pki {
        server_cert: write("server.crt", &server_cert_pem),
        server_key: write("server.key", &server_key_pem),
        trusted_ca: write("trusted-ca.crt", &server_ca.pem),
        wrong_ca: write("wrong-ca.crt", &wrong_ca.pem),
    }
}

// ---------------------------------------------------------------------------
// Server plumbing — boot the admin gateway over TLS on an ephemeral port
// ---------------------------------------------------------------------------

/// A booted TLS admin surface: the gateway's listen address plus the engine handle
/// the test seeds through so introspection / backup have something to read.
struct Harness {
    addr: SocketAddr,
    engine: Arc<Mutex<SessionEngine<SystemClock, MemDisk>>>,
}

/// Boot an in-memory engine behind the admin HTTP/JSON gateway on a **TLS-wrapped**
/// ephemeral ops port, mirroring `stele_server::run`'s composition.
async fn boot_tls(pki: &Pki) -> Harness {
    let settings = TlsSettings {
        cert: pki.server_cert.clone(),
        key: pki.server_key.clone(),
        client_ca: None,
        mode: TlsMode::Required,
    };
    let server_tls = ServerTls::load(&settings).expect("load TLS material");
    let source = AcceptorSource::fixed(&server_tls);

    let engine = Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let core = AdminService::new(Arc::clone(&engine));
    let auth = Arc::new(AdminAuth::new(vec![TOKEN.to_owned()]));

    let state = Arc::new(OpsState::new());
    let session: SharedSession = engine.clone();
    state.set_ready(session);
    state.set_admin(Arc::new(AdminHttp::new(core, Arc::clone(&auth))));
    let ops = OpsServer::new("127.0.0.1:0".parse().unwrap(), state)
        .with_tls(Some(source))
        .bind()
        .await
        .expect("bind ops listener");
    let addr = ops.local_addr();
    tokio::spawn(ops.serve());

    Harness { addr, engine }
}

/// Run SQL statements through the engine to give the test introspectable state.
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
    let dir =
        std::env::temp_dir().join(format!("stele-client-tls-{}-{tag}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir); // backup requires an empty/absent target
    dir
}

/// Connection settings for the booted gateway, with the valid bearer token and the
/// given TLS posture — built through the public construction API.
fn config(addr: SocketAddr, tls: Option<Tls>) -> Config {
    let base = Config::new(addr.ip().to_string(), addr.port(), Some(TOKEN.to_owned()));
    match tls {
        Some(tls) => base.with_tls(tls),
        None => base,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_tls_health_status_and_backup_round_trip() {
    let pki = mint_pki("verified");
    let h = boot_tls(&pki).await;
    seed(
        &h.engine,
        &[
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
            "INSERT INTO account VALUES (1, 100)",
        ],
    );

    // Verify against the CA that signed the server cert, checking the DNS name the
    // certificate carries (we still dial the listener's 127.0.0.1).
    let tls = Tls::verify(&pki.trusted_ca).with_server_name(SERVER_NAME);
    let config = config(h.addr, Some(tls));
    let dir = fresh_backup_dir("verified");
    let backup_path = dir.to_string_lossy().into_owned();

    tokio::task::spawn_blocking(move || {
        let client = Client::new(config);

        // Handshake + the liveness check, over TLS.
        let health = client.health().expect("health over TLS");
        assert!(health.is_serving(), "{health:?}");

        // Engine state and a real online backup, both over the encrypted transport.
        let status = client.status().expect("status over TLS");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.table_count, 1);
        assert_eq!(status.tables[0].name, "account");

        let manifest = client.backup(&backup_path).expect("backup over TLS");
        assert!(manifest.file_count >= 1, "a backup writes durable files");
        assert_eq!(
            manifest.commit_head.len(),
            64,
            "commit head is a hex SHA-256"
        );

        // The restore-plan validates the directory it just produced — the whole
        // backup gate, end to end through the SDK over TLS.
        let plan = client
            .restore_plan(&backup_path)
            .expect("restore-plan over TLS");
        assert!(plan.valid, "freshly-taken backup must validate: {plan:?}");
    })
    .await
    .expect("blocking client task");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypt_only_tls_handshakes_without_a_ca() {
    let pki = mint_pki("encrypt-only");
    let h = boot_tls(&pki).await;

    // No trust anchor (libpq's `require`): encrypt without verifying the server.
    // The handshake still completes and the call is served.
    let config = config(h.addr, Some(Tls::encrypt()));
    tokio::task::spawn_blocking(move || {
        let client = Client::new(config);
        let health = client.health().expect("encrypt-only health over TLS");
        assert!(health.is_serving(), "{health:?}");
    })
    .await
    .expect("blocking client task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_client_is_refused_by_the_tls_listener() {
    let pki = mint_pki("plaintext-refused");
    let h = boot_tls(&pki).await;

    // A plaintext client (no TLS) writes a bare HTTP request into a listener that
    // expects a TLS ClientHello; the gateway never answers a valid HTTP reply, so
    // the round-trip fails. The point: the bearer token never crosses in cleartext.
    let config = config(h.addr, None);
    tokio::task::spawn_blocking(move || {
        let client = Client::new(config);
        assert!(
            client.health().is_err(),
            "a plaintext client must not reach the TLS admin gateway"
        );
    })
    .await
    .expect("blocking client task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_ca_fails_verification() {
    let pki = mint_pki("wrong-ca");
    let h = boot_tls(&pki).await;

    // Verify against a CA that did NOT sign the server certificate: the chain does
    // not build, so the handshake fails as a transport error — proving `verify` is
    // a real check, not encryption theatre.
    let tls = Tls::verify(&pki.wrong_ca).with_server_name(SERVER_NAME);
    let config = config(h.addr, Some(tls));
    tokio::task::spawn_blocking(move || {
        let client = Client::new(config);
        assert!(
            matches!(client.health(), Err(Error::Transport(_))),
            "an untrusted server certificate must fail verification"
        );
    })
    .await
    .expect("blocking client task");
}
