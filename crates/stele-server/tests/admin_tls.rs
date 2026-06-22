//! Admin-surface TLS, end-to-end over real sockets ([STL-311]).
//!
//! The ticket's Definition of Done — both admin transports reachable encrypted,
//! plaintext avoided, and the handshake exercised:
//!
//! * **gRPC over TLS**: a real `tonic` client with a CA-trusting
//!   [`ClientTlsConfig`](tonic::transport::ClientTlsConfig) completes the
//!   handshake and calls `Health` / `Status`.
//! * **HTTPS gateway**: a raw `tokio-rustls` client (the `curl --cacert` face)
//!   reaches `/v1alpha1/health` on the TLS-wrapped ops listener.
//! * **Plaintext is refused** on the TLS gRPC listener — a plaintext client
//!   never reaches the service.
//! * **mTLS** (optional, shared with pg-wire): the gateway honours a `client_ca`,
//!   accepting a client that presents a chaining certificate and rejecting one
//!   that presents none.
//!
//! Each test mints fresh PKI with `rcgen` and reuses the **same** `[tls]`
//! certificate loader (`ServerTls::load`) the daemon uses for `stele.toml` paths,
//! booting the admin surface exactly as [`stele_server::run`] composes it.
//!
//! [STL-311]: https://allegromusic.atlassian.net/browse/STL-311

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{ServerTls, SharedSession, TlsMode, TlsReloader, TlsSettings};
use stele_server::admin::grpc::GrpcAdmin;
use stele_server::admin::http::AdminHttp;
use stele_server::admin::proto::admin_service_client::AdminServiceClient;
use stele_server::admin::proto::{HealthRequest, StatusRequest};
use stele_server::admin::{AdminAuth, AdminService};
use stele_server::ops::{OpsServer, OpsState};
use stele_server::tls::AcceptorSource;
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

/// The bearer token configured on every booted admin surface in this suite.
const TOKEN: &str = "test-admin-tls-token-7b21";

/// The SAN / verification name the server certificate carries. The clients
/// connect to `127.0.0.1` but verify against this name.
const SERVER_NAME: &str = "localhost";

// ---------------------------------------------------------------------------
// Test PKI (minted fresh per test; the server halves written to disk as PEM)
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

/// A leaf `(cert, key)` PEM pair signed by `ca`, with subject CN `cn`, an
/// optional DNS SAN, and the given extended-key-usage purpose.
fn mint_leaf(
    ca: &TestCa,
    cn: &str,
    san: Option<&str>,
    eku: rcgen::ExtendedKeyUsagePurpose,
) -> (String, String) {
    let key = rcgen::KeyPair::generate().expect("generate leaf key");
    let sans: Vec<String> = san.map(str::to_owned).into_iter().collect();
    let mut params = rcgen::CertificateParams::new(sans).expect("leaf params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.extended_key_usages = vec![eku];
    let cert = params.signed_by(&key, &ca.issuer).expect("sign leaf");
    (cert.pem(), key.serialize_pem())
}

/// One test's certificate material: the server cert/key on disk (the
/// `ServerTls::load` input), the CA the client trusts, and the optional mTLS
/// client CA plus an accepted client identity.
struct Pki {
    server_cert: PathBuf,
    server_key: PathBuf,
    server_ca_pem: String,
    client_ca: PathBuf,
    client_identity: (String, String),
}

fn mint_pki(test: &str) -> Pki {
    let server_ca = mint_ca("stele admin-tls server CA");
    let client_ca = mint_ca("stele admin-tls client CA");
    let (server_cert_pem, server_key_pem) = mint_leaf(
        &server_ca,
        "stele admin-tls server",
        Some(SERVER_NAME),
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    );
    let client_identity = mint_leaf(
        &client_ca,
        "admin-tls-client",
        None,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    );

    let dir = std::env::temp_dir().join(format!("stele-admin-tls-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let write = |name: &str, pem: &str| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).expect("write PEM");
        path
    };
    Pki {
        server_cert: write("server.crt", &server_cert_pem),
        server_key: write("server.key", &server_key_pem),
        server_ca_pem: server_ca.pem,
        client_ca: write("client-ca.crt", &client_ca.pem),
        client_identity,
    }
}

// ---------------------------------------------------------------------------
// Server plumbing — boot the admin surface over TLS on ephemeral ports
// ---------------------------------------------------------------------------

/// A booted TLS admin surface: the two listen addresses.
struct Harness {
    ops_addr: SocketAddr,
    grpc_addr: SocketAddr,
}

/// Boot an in-memory engine behind both admin transports over TLS, mirroring the
/// composition `stele_server::run` makes. `mtls` switches on the `client_ca`.
async fn boot_tls(pki: &Pki, mtls: bool) -> Harness {
    let settings = TlsSettings {
        cert: pki.server_cert.clone(),
        key: pki.server_key.clone(),
        client_ca: mtls.then(|| pki.client_ca.clone()),
        mode: TlsMode::Required,
    };
    let server_tls = ServerTls::load(&settings).expect("load TLS material");
    let source = AcceptorSource::fixed(&server_tls);

    let engine = Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let core = AdminService::new(Arc::clone(&engine));
    let auth = Arc::new(AdminAuth::new(vec![TOKEN.to_owned()]));

    // HTTP/JSON gateway on the TLS-wrapped ops listener.
    let state = Arc::new(OpsState::new());
    let session: SharedSession = engine.clone();
    state.set_ready(session);
    state.set_admin(Arc::new(AdminHttp::new(core.clone(), Arc::clone(&auth))));
    let ops = OpsServer::new("127.0.0.1:0".parse().unwrap(), state)
        .with_tls(Some(source.clone()))
        .bind()
        .await
        .expect("bind ops listener");
    let ops_addr = ops.local_addr();
    tokio::spawn(ops.serve());

    // gRPC listener over TLS on its own ephemeral port.
    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind grpc listener");
    let grpc_addr = grpc_listener.local_addr().unwrap();
    tokio::spawn(stele_server::admin::grpc::serve_tls(
        grpc_listener,
        GrpcAdmin::new(core, auth),
        source,
    ));

    Harness {
        ops_addr,
        grpc_addr,
    }
}

/// Boot the admin surface over **reloadable** TLS: the `AcceptorSource` and both
/// transports share `reloader`'s cell, so a `ReloadTls` trigger swaps the cert the
/// listeners present without a restart ([STL-326]). Mirrors [`boot_tls`] otherwise.
///
/// [STL-326]: https://allegromusic.atlassian.net/browse/STL-326
async fn boot_tls_reloadable(reloader: &TlsReloader) -> Harness {
    let source = AcceptorSource::reloading(reloader);

    let engine = Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let core = AdminService::new(Arc::clone(&engine));
    let auth = Arc::new(AdminAuth::new(vec![TOKEN.to_owned()]));

    let state = Arc::new(OpsState::new());
    let session: SharedSession = engine.clone();
    state.set_ready(session);
    state.set_admin(Arc::new(
        AdminHttp::new(core.clone(), Arc::clone(&auth)).with_tls_reloader(reloader),
    ));
    let ops = OpsServer::new("127.0.0.1:0".parse().unwrap(), state)
        .with_tls(Some(source.clone()))
        .bind()
        .await
        .expect("bind ops listener");
    let ops_addr = ops.local_addr();
    tokio::spawn(ops.serve());

    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind grpc listener");
    let grpc_addr = grpc_listener.local_addr().unwrap();
    tokio::spawn(stele_server::admin::grpc::serve_tls(
        grpc_listener,
        GrpcAdmin::new(core, auth).with_tls_reloader(reloader),
        source,
    ));

    Harness {
        ops_addr,
        grpc_addr,
    }
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

/// Connect a gRPC client to `addr` over TLS, trusting `ca_pem` and verifying the
/// server against [`SERVER_NAME`].
async fn connect_grpc_tls(addr: SocketAddr, ca_pem: &str) -> AdminServiceClient<Channel> {
    // tonic's TLS needs a process-wide default crypto provider installed once.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .domain_name(SERVER_NAME);
    let channel = Endpoint::from_shared(format!("https://{addr}"))
        .expect("endpoint")
        .tls_config(tls)
        .expect("client tls config")
        .connect()
        .await
        .expect("grpc TLS connect");
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

/// A rustls client config trusting `ca_pem`, optionally presenting `identity`
/// (mTLS). Mirrors the pg-wire TLS wire tests' client.
fn rustls_client_config(ca_pem: &str, identity: Option<&(String, String)>) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(cert.expect("parse CA PEM")).expect("add root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("protocol versions")
        .with_root_certificates(roots);
    match identity {
        Some((cert_pem, key_pem)) => {
            let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("parse client cert PEM");
            let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).expect("parse client key");
            builder
                .with_client_auth_cert(certs, key)
                .expect("client identity")
        }
        None => builder.with_no_client_auth(),
    }
}

/// Issue one HTTPS request over a fresh TLS connection to `addr`, returning the
/// `(status line, body)`. `identity` presents a client certificate (mTLS).
async fn https(
    addr: SocketAddr,
    config: rustls::ClientConfig,
    method: &str,
    path: &str,
    token: Option<&str>,
) -> std::io::Result<(String, String)> {
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr).await?;
    let server_name = ServerName::try_from(SERVER_NAME).unwrap();
    let mut stream = connector.connect(server_name, tcp).await?;

    let auth_line = token.map_or_else(String::new, |t| format!("Authorization: Bearer {t}\r\n"));
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {SERVER_NAME}\r\nConnection: close\r\n{auth_line}\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8(raw).expect("utf-8");
    let (head, body) = text.split_once("\r\n\r\n").expect("head/body split");
    Ok((head.lines().next().unwrap().to_owned(), body.to_owned()))
}

/// Open a TLS connection to `addr`, complete the handshake trusting `ca_pem`, and
/// return the server's leaf-certificate DER — so a test can watch the served
/// certificate change across a reload.
async fn https_leaf(addr: SocketAddr, ca_pem: &str) -> Vec<u8> {
    let connector = tokio_rustls::TlsConnector::from(Arc::new(rustls_client_config(ca_pem, None)));
    let tcp = TcpStream::connect(addr).await.expect("connect");
    let server_name = ServerName::try_from(SERVER_NAME).unwrap();
    let stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");
    let (_io, conn) = stream.get_ref();
    conn.peer_certificates()
        .expect("server presented certificates")
        .first()
        .expect("a leaf certificate")
        .as_ref()
        .to_vec()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_over_tls_health_and_status() {
    let pki = mint_pki("grpc");
    let h = boot_tls(&pki, false).await;
    let mut client = connect_grpc_tls(h.grpc_addr, &pki.server_ca_pem).await;

    // The bearer token still gates every call — over the encrypted channel.
    let err = client
        .health(Request::new(HealthRequest {}))
        .await
        .expect_err("missing token must be rejected even over TLS");
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err:?}");

    let health = client
        .health(authed(HealthRequest {}))
        .await
        .expect("health over TLS");
    assert_eq!(health.into_inner().status, 1, "ServingStatus::SERVING");

    let status = client
        .status(authed(StatusRequest {}))
        .await
        .expect("status over TLS")
        .into_inner();
    assert!(status.ready);
    assert!(!status.server_version.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_gateway_health_and_auth() {
    let pki = mint_pki("https");
    let h = boot_tls(&pki, false).await;

    // 401 without a token, 200 with — the curl --cacert face, over TLS.
    let (status, _) = https(
        h.ops_addr,
        rustls_client_config(&pki.server_ca_pem, None),
        "GET",
        "/v1alpha1/health",
        None,
    )
    .await
    .expect("https request");
    assert!(status.contains("401"), "no token must 401: {status}");

    let (status, body) = https(
        h.ops_addr,
        rustls_client_config(&pki.server_ca_pem, None),
        "GET",
        "/v1alpha1/health",
        Some(TOKEN),
    )
    .await
    .expect("https request");
    assert!(status.contains("200"), "{status}: {body}");
    assert!(body.contains("SERVING"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_grpc_is_refused_on_the_tls_listener() {
    let pki = mint_pki("plaintext-refused");
    let h = boot_tls(&pki, false).await;

    // A plaintext gRPC client against the TLS listener: the connection's HTTP/2
    // preface is read as a TLS record and the handshake never completes, so no
    // call reaches the service. Either the connect or the first RPC fails — the
    // point is that plaintext never succeeds.
    let result = async {
        let channel = Endpoint::from_shared(format!("http://{}", h.grpc_addr))?
            .connect()
            .await?;
        AdminServiceClient::new(channel)
            .health(authed(HealthRequest {}))
            .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    assert!(
        result.is_err(),
        "a plaintext gRPC client must not reach the TLS admin listener"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_gateway_requires_a_client_certificate() {
    let pki = mint_pki("mtls");
    let h = boot_tls(&pki, true).await; // client_ca configured

    // A client presenting the right certificate completes the handshake and is
    // served (the bearer token still authorizes).
    let (status, body) = https(
        h.ops_addr,
        rustls_client_config(&pki.server_ca_pem, Some(&pki.client_identity)),
        "GET",
        "/v1alpha1/health",
        Some(TOKEN),
    )
    .await
    .expect("mTLS request with a client cert");
    assert!(status.contains("200"), "{status}: {body}");
    assert!(body.contains("SERVING"), "{body}");

    // A client presenting NO certificate fails the handshake — the request never
    // reaches the gateway.
    let result = https(
        h.ops_addr,
        rustls_client_config(&pki.server_ca_pem, None),
        "GET",
        "/v1alpha1/health",
        Some(TOKEN),
    )
    .await;
    assert!(
        result.is_err(),
        "mTLS must reject a client that presents no certificate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_tls_trigger_rotates_the_served_certificate() {
    // The cross-platform DoD (STL-326): a signal-free, token-authenticated trigger
    // rotates the certificate the listener serves — no restart, no SIGHUP. The
    // oracle: capture the leaf on the wire, stage a new pair on disk, fire the
    // trigger, and watch a NEW connection present the rotated leaf.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // One CA, two server leaves (A then B). The client trusts the CA, so it
    // verifies either leaf; the rotation is observable as a different leaf DER.
    let ca = mint_ca("stele admin-tls reload CA");
    let server_leaf = |name: &str| {
        mint_leaf(
            &ca,
            name,
            Some(SERVER_NAME),
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        )
    };
    let (cert_a, key_a) = server_leaf("stele server A");
    let dir = std::env::temp_dir().join(format!("stele-admin-tls-reload-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, &cert_a).expect("write cert A");
    std::fs::write(&key_path, &key_a).expect("write key A");

    let reloader = TlsReloader::load(TlsSettings {
        cert: cert_path.clone(),
        key: key_path.clone(),
        client_ca: None,
        mode: TlsMode::Required,
    })
    .expect("load reloader");
    let h = boot_tls_reloadable(&reloader).await;

    // The leaf currently on the wire — cert A.
    let leaf_a = https_leaf(h.ops_addr, &ca.pem).await;

    // Stage a fresh leaf B on disk. Until the trigger fires, the listener keeps
    // serving A — a reload is explicit, not an mtime watch.
    let (cert_b, key_b) = server_leaf("stele server B");
    std::fs::write(&cert_path, &cert_b).expect("rotate cert to B");
    std::fs::write(&key_path, &key_b).expect("rotate key to B");
    assert_eq!(
        https_leaf(h.ops_addr, &ca.pem).await,
        leaf_a,
        "no trigger yet → the listener still serves cert A"
    );

    // Fire the cross-platform trigger over the HTTPS gateway (the curl face).
    let (status, body) = https(
        h.ops_addr,
        rustls_client_config(&ca.pem, None),
        "POST",
        "/v1alpha1/reload-tls",
        Some(TOKEN),
    )
    .await
    .expect("reload-tls request");
    assert!(status.contains("200"), "{status}: {body}");
    assert!(body.contains("\"reloaded\":true"), "{body}");

    // A new connection now presents cert B — rotated without a restart.
    let leaf_b = https_leaf(h.ops_addr, &ca.pem).await;
    assert_ne!(leaf_b, leaf_a, "after the trigger the served leaf is B");

    let _ = std::fs::remove_dir_all(&dir);
}
