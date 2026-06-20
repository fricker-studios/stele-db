//! TLS on the pg-wire startup path, end-to-end over real sockets (STL-251).
//!
//! Each test mints a fresh PKI with `rcgen` (a CA, a server certificate for
//! `localhost`, and — for the mTLS tests — client certificates from the right
//! and from a rogue CA), boots a [`Server`] with [`ServerTls`] loaded from the
//! PEM files on disk (the same loader `stele-server` uses for `stele.toml`
//! paths), and drives the `SSLRequest` negotiation with a raw socket +
//! `tokio-rustls` client:
//!
//! * `SSLRequest` → `S` → handshake → `SELECT 1` round-trips encrypted.
//! * `tls = "optional"` still accepts a plaintext startup.
//! * `tls = "required"` refuses plaintext with `FATAL` SQLSTATE `28000`.
//! * Without TLS configured, `SSLRequest` still gets the v0.1 `N` refusal.
//! * mTLS accepts the right client certificate and rejects a missing or
//!   rogue-CA one — the Definition-of-Done reject cases.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{Server, ServerTls, SharedSession, TlsMode, TlsReloader, TlsSettings};
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Test PKI — minted fresh per test, written to disk as PEM (the loader's input)
// ---------------------------------------------------------------------------

/// One test's certificate material on disk plus the in-memory client halves.
struct Pki {
    /// Server certificate (PEM, `localhost` SAN) — what `[tls] cert` points at.
    server_cert: PathBuf,
    /// Server private key (PEM) — what `[tls] key` points at.
    server_key: PathBuf,
    /// The CA that signs the server certificate — retained so the hot-reload tests
    /// (STL-293) can mint a *fresh* server leaf from the SAME CA, write it over the
    /// cert/key paths, and have the test client still trust the rotated cert.
    server_ca: TestCa,
    /// CA the *server certificate* chains to; the test client's trust anchor.
    ca_pem: String,
    /// CA the server trusts for **mTLS** — what `[tls] client_ca` points at.
    client_ca: PathBuf,
    /// (cert, key) PEM signed by [`Self::client_ca`] — the accepted identity.
    client_identity: (String, String),
    /// (cert, key) PEM signed by an unrelated CA — the rejected identity.
    rogue_identity: (String, String),
}

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

/// The subject Common Name minted into the accepted client certificate. The
/// mTLS-principal test (`mtls_cert_identity_becomes_the_write_principal`) asserts
/// this lands in `_stele_principal`, overriding the startup `user` (`stele`).
const CLIENT_CERT_CN: &str = "alice-cert";

/// The subject Common Name of the server certificate, shared by the initial cert
/// and the rotated cert the hot-reload tests mint (STL-293).
const SERVER_CERT_CN: &str = "stele tls test server";

/// A leaf (cert, key) PEM pair signed by `ca`, with subject CN `cn`.
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

/// Mint the full PKI for one test and write the server-side halves to disk
/// under a fresh scratch directory (the [`ServerTls::load`] input).
fn mint_pki(test: &str) -> Pki {
    let server_ca = mint_ca("stele test server CA");
    let client_ca = mint_ca("stele test client CA");
    let rogue_ca = mint_ca("stele test rogue CA");

    let (server_cert_pem, server_key_pem) = mint_leaf(
        &server_ca,
        SERVER_CERT_CN,
        Some("localhost"),
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    );
    let client_identity = mint_leaf(
        &client_ca,
        CLIENT_CERT_CN,
        None,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    );
    let rogue_identity = mint_leaf(
        &rogue_ca,
        "stele tls test rogue",
        None,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    );

    let dir = std::env::temp_dir().join(format!("stele-tls-wire-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let write = |name: &str, pem: &str| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).expect("write PEM");
        path
    };
    Pki {
        server_cert: write("server.crt", &server_cert_pem),
        server_key: write("server.key", &server_key_pem),
        ca_pem: server_ca.pem.clone(),
        server_ca,
        client_ca: write("client-ca.crt", &client_ca.pem),
        client_identity,
        rogue_identity,
    }
}

// ---------------------------------------------------------------------------
// Server + client plumbing
// ---------------------------------------------------------------------------

fn fresh_session() -> SharedSession {
    Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)))
}

/// Boot a TLS-enabled server on an ephemeral port; `client_ca` switches on mTLS.
async fn spawn_tls_server(pki: &Pki, mode: TlsMode, mtls: bool) -> SocketAddr {
    let settings = TlsSettings {
        cert: pki.server_cert.clone(),
        key: pki.server_key.clone(),
        client_ca: mtls.then(|| pki.client_ca.clone()),
        mode,
    };
    let tls = ServerTls::load(&settings).expect("load TLS material");
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), fresh_session())
        .with_tls(tls)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

/// Boot a TLS-enabled server behind a [`TlsReloader`] (STL-293) so the test can
/// rotate the on-disk cert/key and call [`TlsReloader::reload`]. Returns the bound
/// address and the reloader handle.
async fn spawn_reloadable_tls_server(pki: &Pki, mode: TlsMode) -> (SocketAddr, TlsReloader) {
    let settings = TlsSettings {
        cert: pki.server_cert.clone(),
        key: pki.server_key.clone(),
        client_ca: None,
        mode,
    };
    let reloader = TlsReloader::load(settings).expect("load TLS material");
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), fresh_session())
        .with_tls_reloader(&reloader)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    (addr, reloader)
}

/// The DER bytes of the leaf certificate the server presented on `stream` — what
/// the hot-reload tests compare across a rotation to prove which cert is live.
fn server_leaf_der(stream: &tokio_rustls::client::TlsStream<TcpStream>) -> Vec<u8> {
    stream
        .get_ref()
        .1
        .peer_certificates()
        .expect("server presented a certificate")
        .first()
        .expect("a leaf certificate")
        .as_ref()
        .to_vec()
}

/// A rustls client config trusting `ca_pem`, optionally presenting `identity`.
fn client_config(ca_pem: &str, identity: Option<&(String, String)>) -> rustls::ClientConfig {
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

/// A rustls client config that accepts ANY server certificate — used only to
/// drive the self-signed-fallback server (STL-304), whose CA-less certificate
/// cannot be chained to a trust anchor. This mirrors a `sslmode=require` client:
/// encrypt the connection, but do not authenticate the server. NEVER a posture
/// real clients should use against a CA-issued cert.
fn insecure_client_config() -> rustls::ClientConfig {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::UnixTime;
    use rustls::{DigitallySignedStruct, SignatureScheme};

    #[derive(Debug)]
    struct AcceptAny(Arc<rustls::crypto::CryptoProvider>);

    impl ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        .with_no_client_auth()
}

/// The 8-byte `SSLRequest` startup-shape message.
const fn ssl_request() -> [u8; 8] {
    let mut buf = [0u8; 8];
    let len = 8_i32.to_be_bytes();
    let code = 80_877_103_i32.to_be_bytes();
    let mut i = 0;
    while i < 4 {
        buf[i] = len[i];
        buf[i + 4] = code[i];
        i += 1;
    }
    buf
}

/// Send `SSLRequest`, assert the server answers `S`, and run the handshake.
async fn tls_connect(
    addr: SocketAddr,
    config: rustls::ClientConfig,
) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut tcp = TcpStream::connect(addr).await?;
    tcp.write_all(&ssl_request()).await?;
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await?;
    assert_eq!(answer[0], b'S', "server should accept the SSLRequest");
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    connector
        .connect(ServerName::try_from("localhost").expect("server name"), tcp)
        .await
}

/// The plaintext `StartupMessage` for user/database `stele`.
fn startup_message() -> Vec<u8> {
    let body = b"user\0stele\0database\0stele\0\0";
    let mut msg = Vec::with_capacity(8 + body.len());
    msg.extend_from_slice(&i32::try_from(8 + body.len()).unwrap().to_be_bytes());
    msg.extend_from_slice(&196_608_i32.to_be_bytes()); // protocol 3.0
    msg.extend_from_slice(body);
    msg
}

/// Read one typed backend message: `(type byte, payload)`.
async fn read_message<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).await?;
    let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]);
    let mut payload = vec![0u8; usize::try_from(len - 4).expect("sane length")];
    stream.read_exact(&mut payload).await?;
    Ok((head[0], payload))
}

/// Send the startup message and drain the OK bundle through `ReadyForQuery`.
async fn complete_startup<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> std::io::Result<()> {
    stream.write_all(&startup_message()).await?;
    loop {
        let (kind, _) = read_message(stream).await?;
        match kind {
            b'Z' => return Ok(()),
            b'E' => {
                return Err(std::io::Error::other("server returned ErrorResponse"));
            }
            _ => {}
        }
    }
}

/// Run `SELECT 1` over the simple-query protocol and return the single cell.
async fn select_one<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> String {
    let sql = b"SELECT 1\0";
    let mut msg = Vec::with_capacity(5 + sql.len());
    msg.push(b'Q');
    msg.extend_from_slice(&i32::try_from(4 + sql.len()).unwrap().to_be_bytes());
    msg.extend_from_slice(sql);
    stream.write_all(&msg).await.expect("send query");

    let mut cell = None;
    loop {
        let (kind, payload) = read_message(stream).await.expect("read reply");
        match kind {
            b'D' => {
                // DataRow: u16 column count, then (i32 len, bytes) per cell.
                let len = i32::from_be_bytes([payload[2], payload[3], payload[4], payload[5]]);
                let len = usize::try_from(len).expect("non-NULL cell");
                cell = Some(String::from_utf8(payload[6..6 + len].to_vec()).unwrap());
            }
            b'Z' => return cell.expect("a DataRow before ReadyForQuery"),
            b'E' => panic!("query failed: {payload:?}"),
            _ => {}
        }
    }
}

/// Run one simple-query statement and return the cells of its first `DataRow`
/// (each `None` for a SQL NULL), or an empty vector for a statement that returns
/// no rows (`CREATE`/`INSERT`). Panics on an `ErrorResponse` so a failed DDL/DML
/// fails the test at the call site rather than silently.
async fn simple_query_row<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    sql: &str,
) -> Vec<Option<String>> {
    let mut msg = Vec::with_capacity(6 + sql.len());
    msg.push(b'Q');
    msg.extend_from_slice(&i32::try_from(4 + sql.len() + 1).unwrap().to_be_bytes());
    msg.extend_from_slice(sql.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await.expect("send query");

    let mut first_row: Option<Vec<Option<String>>> = None;
    loop {
        let (kind, payload) = read_message(stream).await.expect("read reply");
        match kind {
            b'D' if first_row.is_none() => {
                // DataRow: u16 column count, then per cell (i32 len, bytes); -1 = NULL.
                let cols = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                let mut pos = 2;
                let mut cells = Vec::with_capacity(cols);
                for _ in 0..cols {
                    let len = i32::from_be_bytes([
                        payload[pos],
                        payload[pos + 1],
                        payload[pos + 2],
                        payload[pos + 3],
                    ]);
                    pos += 4;
                    if len < 0 {
                        cells.push(None);
                    } else {
                        let len = usize::try_from(len).expect("sane cell length");
                        cells.push(Some(
                            String::from_utf8(payload[pos..pos + len].to_vec()).unwrap(),
                        ));
                        pos += len;
                    }
                }
                first_row = Some(cells);
            }
            b'Z' => return first_row.unwrap_or_default(),
            b'E' => {
                let (_, code, message) = parse_error_fields(&payload);
                panic!("statement {sql:?} failed: {code} {message}");
            }
            _ => {}
        }
    }
}

/// Decode the `(severity, sqlstate, message)` out of an `ErrorResponse` payload.
fn parse_error_fields(payload: &[u8]) -> (String, String, String) {
    let (mut severity, mut code, mut message) = (String::new(), String::new(), String::new());
    let mut pos = 0;
    while pos < payload.len() && payload[pos] != 0 {
        let field = payload[pos];
        pos += 1;
        let end = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .map_or(payload.len(), |n| pos + n);
        let value = String::from_utf8_lossy(&payload[pos..end]).into_owned();
        pos = end + 1;
        match field {
            b'S' => severity = value,
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    (severity, code, message)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_one_round_trips_over_tls() {
    let pki = mint_pki("roundtrip");
    let addr = spawn_tls_server(&pki, TlsMode::Required, false).await;

    let mut stream = tls_connect(addr, client_config(&pki.ca_pem, None))
        .await
        .expect("TLS handshake");
    complete_startup(&mut stream).await.expect("startup");
    assert_eq!(select_one(&mut stream).await, "1");
}

#[tokio::test]
async fn self_signed_server_round_trips_select_one() {
    // STL-304: a server booted with an ephemeral self-signed cert (the non-dev
    // no-TLS non-loopback fallback) still completes the TLS handshake and serves
    // queries. The client cannot verify the CA-less certificate, so it uses a
    // no-verification verifier — exactly a `sslmode=require` client.
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), fresh_session())
        .with_tls(ServerTls::self_signed(TlsMode::Required).expect("self-signed TLS"))
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());

    let mut stream = tls_connect(addr, insecure_client_config())
        .await
        .expect("TLS handshake against the self-signed server");
    complete_startup(&mut stream).await.expect("startup");
    assert_eq!(select_one(&mut stream).await, "1");
}

#[tokio::test]
async fn optional_mode_still_accepts_plaintext_startup() {
    let pki = mint_pki("optional-plaintext");
    let addr = spawn_tls_server(&pki, TlsMode::Optional, false).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    complete_startup(&mut stream)
        .await
        .expect("plaintext startup");
    assert_eq!(select_one(&mut stream).await, "1");
}

#[tokio::test]
async fn required_mode_refuses_plaintext_startup_with_fatal_28000() {
    let pki = mint_pki("required-plaintext");
    let addr = spawn_tls_server(&pki, TlsMode::Required, false).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(&startup_message())
        .await
        .expect("send plaintext startup");

    let (kind, payload) = read_message(&mut stream).await.expect("read refusal");
    assert_eq!(kind, b'E', "plaintext startup should get an ErrorResponse");
    let (severity, code, message) = parse_error_fields(&payload);
    assert_eq!(severity, "FATAL");
    assert_eq!(code, "28000");
    assert!(
        message.contains("TLS"),
        "message names the cause: {message}"
    );

    // Nothing follows the FATAL: the server hangs up.
    let mut rest = Vec::new();
    let n = stream.read_to_end(&mut rest).await.expect("read EOF");
    assert_eq!(n, 0, "connection closes after the refusal");
}

#[tokio::test]
async fn ssl_request_still_refused_with_n_when_tls_unconfigured() {
    // No `with_tls`: the v0.1 behavior — refuse with `N`, plaintext fallback OK.
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), fresh_session())
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&ssl_request()).await.expect("SSLRequest");
    let mut answer = [0u8; 1];
    stream.read_exact(&mut answer).await.expect("read answer");
    assert_eq!(answer[0], b'N', "unconfigured server refuses TLS");

    // The libpq fallback: proceed plaintext on the same connection.
    complete_startup(&mut stream)
        .await
        .expect("plaintext fallback");
    assert_eq!(select_one(&mut stream).await, "1");
}

#[tokio::test]
async fn mtls_accepts_a_client_certificate_from_the_trusted_ca() {
    let pki = mint_pki("mtls-accept");
    let addr = spawn_tls_server(&pki, TlsMode::Required, true).await;

    let config = client_config(&pki.ca_pem, Some(&pki.client_identity));
    let mut stream = tls_connect(addr, config).await.expect("mTLS handshake");
    complete_startup(&mut stream).await.expect("startup");
    assert_eq!(select_one(&mut stream).await, "1");
}

#[tokio::test]
async fn mtls_cert_identity_becomes_the_write_principal() {
    // STL-291: the verified mTLS client certificate's subject CN becomes the
    // connection's write principal, so provenance records who *actually*
    // connected — overriding the unauthenticated startup `user`. The server runs
    // the default `trust` auth (no SCRAM), the precedence case where the cert is
    // the strongest verified identity.
    let pki = mint_pki("mtls-principal");
    let addr = spawn_tls_server(&pki, TlsMode::Required, true).await;

    let config = client_config(&pki.ca_pem, Some(&pki.client_identity));
    let mut stream = tls_connect(addr, config).await.expect("mTLS handshake");
    // `startup_message` identifies as user `stele`; the client certificate's CN is
    // `alice-cert` (CLIENT_CERT_CN). The verified cert must win.
    complete_startup(&mut stream).await.expect("startup");

    simple_query_row(
        &mut stream,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    )
    .await;
    simple_query_row(&mut stream, "INSERT INTO t VALUES (1, 100)").await;
    let row = simple_query_row(&mut stream, "SELECT _stele_principal FROM t").await;

    assert_eq!(
        row.first().and_then(Option::as_deref),
        Some(CLIENT_CERT_CN),
        "the verified mTLS cert CN is the write principal, not the startup user `stele`",
    );
}

#[tokio::test]
async fn mtls_rejects_a_missing_client_certificate() {
    let pki = mint_pki("mtls-missing");
    let addr = spawn_tls_server(&pki, TlsMode::Required, true).await;

    // No identity: the handshake (or the first read after it — TLS 1.3 lets
    // the client finish before the server verdict arrives) must fail. It must
    // never reach a query.
    let attempt = async {
        let mut stream = tls_connect(addr, client_config(&pki.ca_pem, None)).await?;
        complete_startup(&mut stream).await
    };
    attempt
        .await
        .expect_err("a certificate-less client must be rejected");
}

#[tokio::test]
async fn mtls_rejects_a_client_certificate_from_the_wrong_ca() {
    let pki = mint_pki("mtls-rogue");
    let addr = spawn_tls_server(&pki, TlsMode::Required, true).await;

    let config = client_config(&pki.ca_pem, Some(&pki.rogue_identity));
    let attempt = async {
        let mut stream = tls_connect(addr, config).await?;
        complete_startup(&mut stream).await
    };
    attempt
        .await
        .expect_err("a rogue-CA client certificate must be rejected");
}

// ---------------------------------------------------------------------------
// Hot-reload (STL-293): rotate the cert/key under load without a restart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reload_swaps_the_cert_for_new_connections_and_keeps_in_flight_sessions() {
    let pki = mint_pki("reload-swap");
    let (addr, reloader) = spawn_reloadable_tls_server(&pki, TlsMode::Required).await;

    // An in-flight session, established before the rotation: it must keep working
    // across the swap (it keeps the acceptor it handshook with).
    let mut before = tls_connect(addr, client_config(&pki.ca_pem, None))
        .await
        .expect("initial TLS handshake");
    let cert_before = server_leaf_der(&before);
    complete_startup(&mut before).await.expect("startup");

    // Rotate the on-disk pair to a fresh leaf signed by the SAME CA (so the client
    // still trusts it), then reload — the operator's `kill -HUP` path.
    let (new_cert, new_key) = mint_leaf(
        &pki.server_ca,
        SERVER_CERT_CN,
        Some("localhost"),
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    );
    std::fs::write(&pki.server_cert, &new_cert).expect("write rotated cert");
    std::fs::write(&pki.server_key, &new_key).expect("write rotated key");
    reloader.reload().expect("reload the rotated pair");

    // A NEW connection presents the rotated certificate and serves queries.
    let mut after = tls_connect(addr, client_config(&pki.ca_pem, None))
        .await
        .expect("TLS handshake after reload");
    let cert_after = server_leaf_der(&after);
    complete_startup(&mut after)
        .await
        .expect("startup after reload");
    assert_ne!(
        cert_before, cert_after,
        "a connection accepted after the reload must present the rotated certificate"
    );
    assert_eq!(select_one(&mut after).await, "1");

    // The in-flight session, handshook before the swap, still round-trips.
    assert_eq!(
        select_one(&mut before).await,
        "1",
        "an established session survives the rotation"
    );
}

#[tokio::test]
async fn broken_reload_keeps_serving_the_previous_cert() {
    let pki = mint_pki("reload-broken");
    let (addr, reloader) = spawn_reloadable_tls_server(&pki, TlsMode::Required).await;

    let mut before = tls_connect(addr, client_config(&pki.ca_pem, None))
        .await
        .expect("initial TLS handshake");
    let cert_before = server_leaf_der(&before);
    complete_startup(&mut before).await.expect("startup");

    // Corrupt the cert file (a torn write / wrong file mid-rotation). The reload
    // must FAIL and leave the running acceptor untouched — the listener stays up.
    std::fs::write(&pki.server_cert, b"not a certificate").expect("corrupt cert file");
    reloader
        .reload()
        .expect_err("a broken pair must not swap in");

    // A new connection still presents the ORIGINAL certificate and serves queries,
    // and the in-flight session is unaffected.
    let mut after = tls_connect(addr, client_config(&pki.ca_pem, None))
        .await
        .expect("TLS handshake survives the broken reload");
    let cert_after = server_leaf_der(&after);
    complete_startup(&mut after)
        .await
        .expect("startup after broken reload");
    assert_eq!(
        cert_before, cert_after,
        "a broken rotation keeps the previously loaded certificate"
    );
    assert_eq!(select_one(&mut after).await, "1");
    assert_eq!(select_one(&mut before).await, "1");
}
