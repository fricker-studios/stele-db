//! SCRAM-SHA-256-PLUS channel binding (`tls-server-end-point`) over TLS,
//! end-to-end over real sockets (STL-297).
//!
//! Channel binding folds the hash of the server certificate the client actually
//! saw into the SASL proof (RFC 5802 §6 / RFC 5929), so a man-in-the-middle that
//! terminates TLS with a *different* certificate cannot relay a captured proof.
//! These tests stand up a server that is both TLS-required and SCRAM-required —
//! the combination STL-297 targets — and drive it two ways:
//!
//! * **`tokio-postgres`** through a small `rustls` channel-binding adapter, with
//!   `channel_binding=require`: a stock driver with its own independent SCRAM
//!   implementation negotiates `SCRAM-SHA-256-PLUS` (it is advertised first, so
//!   libpq-style clients prefer it) and round-trips a query. `require` makes the
//!   test fail unless PLUS was actually offered and used.
//! * **A raw `tokio-rustls` socket** speaking the SASL messages by hand, pinning
//!   the security Definition-of-Done: the advertised list, a correct binding that
//!   authenticates, a **tampered binding that is refused**, the RFC downgrade
//!   rule (a `y` flag over a PLUS-advertising channel is refused), and that plain
//!   SCRAM still works over TLS for a client that opts out with `n`.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, ServerName};
use stele_common::hash::sha256;
use stele_common::scram;
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{AuthMode, Server, ServerTls, SharedSession, TlsMode, TlsSettings};
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};
use tokio_rustls::TlsConnector;

// ---------------------------------------------------------------------------
// Test PKI — a CA + a server leaf for localhost / 127.0.0.1, written to disk
// (the `ServerTls::load` input, the same loader `stele-server` uses).
// ---------------------------------------------------------------------------

struct Pki {
    /// Server certificate (PEM) — what `[tls] cert` points at.
    server_cert: PathBuf,
    /// Server private key (PEM) — what `[tls] key` points at.
    server_key: PathBuf,
    /// The CA the server certificate chains to — the client's trust anchor.
    ca_pem: String,
}

/// Mint a fresh PKI for one test under a scratch directory. The leaf carries
/// both a `localhost` DNS SAN (for the raw-socket client) and a `127.0.0.1` IP
/// SAN (for the `tokio-postgres` client connecting by address), and is signed
/// ECDSA-P256/SHA-256 so it yields a `tls-server-end-point` binding.
fn mint_pki(test: &str) -> Pki {
    let ca_key = rcgen::KeyPair::generate().expect("CA key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "stele scram-plus test CA");
    let ca_cert = ca_params
        .clone()
        .self_signed(&ca_key)
        .expect("self-sign CA");
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let leaf_key = rcgen::KeyPair::generate().expect("leaf key");
    let mut leaf_params =
        rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("leaf params");
    leaf_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "stele scram-plus test server");
    leaf_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &issuer)
        .expect("sign leaf");

    let dir = std::env::temp_dir().join(format!("stele-scram-plus-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let server_cert = dir.join("server.crt");
    let server_key = dir.join("server.key");
    std::fs::write(&server_cert, leaf_cert.pem()).expect("write cert");
    std::fs::write(&server_key, leaf_key.serialize_pem()).expect("write key");
    Pki {
        server_cert,
        server_key,
        ca_pem: ca_cert.pem(),
    }
}

/// Boot a TLS-required, SCRAM-required server on an ephemeral port, with one
/// user created through the real SQL path.
async fn spawn_server(pki: &Pki, user: &str, password: &str) -> SocketAddr {
    let settings = TlsSettings {
        cert: pki.server_cert.clone(),
        key: pki.server_key.clone(),
        client_ca: None,
        mode: TlsMode::Required,
    };
    let tls = ServerTls::load(&settings).expect("load TLS material");

    let mut engine = SessionEngine::open(MemDisk::new(), SystemClock);
    let sql = format!("CREATE USER {user} PASSWORD '{password}'");
    let stmt = &stele_sql::parse(&sql).expect("parse CREATE USER")[0];
    engine.execute(stmt).expect("create user");
    let session: SharedSession = Arc::new(Mutex::new(engine));

    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .with_tls(tls)
        .with_auth(AuthMode::Scram)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

/// A `rustls` client config trusting `ca_pem` (no client certificate).
fn client_config(ca_pem: &str) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(cert.expect("parse CA PEM")).expect("add root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

// ---------------------------------------------------------------------------
// tokio-postgres ⇄ rustls channel-binding adapter
//
// A minimal `MakeTlsConnect` (the role `tokio-postgres-rustls` plays in
// production) so a stock `tokio-postgres` client can run channel binding over
// our `tokio-rustls` stream. It exposes the same `tls-server-end-point` value
// the server computes — the SHA-256 of the end-entity certificate — which is
// correct for the SHA-256-signed test leaf.
// ---------------------------------------------------------------------------

/// The stream type `tokio-postgres` hands the connector — its own `Socket`
/// wrapper, not a bare `TcpStream` — so the adapter is generic over the IO.
trait Io: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Io for T {}

#[derive(Clone)]
struct RustlsConnect(Arc<rustls::ClientConfig>);

impl<S: Io> MakeTlsConnect<S> for RustlsConnect {
    type Stream = RustlsStream<S>;
    type TlsConnect = RustlsConnector;
    type Error = std::io::Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        let server_name = ServerName::try_from(domain.to_owned())
            .map_err(|_| std::io::Error::other(format!("invalid server name: {domain}")))?;
        Ok(RustlsConnector {
            config: self.0.clone(),
            server_name,
        })
    }
}

struct RustlsConnector {
    config: Arc<rustls::ClientConfig>,
    server_name: ServerName<'static>,
}

impl<S: Io> TlsConnect<S> for RustlsConnector {
    type Stream = RustlsStream<S>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = std::io::Result<RustlsStream<S>>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let connector = TlsConnector::from(self.config);
        Box::pin(async move {
            let tls = connector.connect(self.server_name, stream).await?;
            Ok(RustlsStream(tls))
        })
    }
}

struct RustlsStream<S>(tokio_rustls::client::TlsStream<S>);

impl<S: Io> AsyncRead for RustlsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S: Io> AsyncWrite for RustlsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S: Io> TlsStream for RustlsStream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        self.0
            .get_ref()
            .1
            .peer_certificates()
            .and_then(<[_]>::first)
            .map_or_else(ChannelBinding::none, |cert| {
                ChannelBinding::tls_server_end_point(sha256(cert.as_ref()).as_bytes().to_vec())
            })
    }
}

// ---------------------------------------------------------------------------
// Raw-socket SASL helpers (the proof math from `stele_common::scram`)
// ---------------------------------------------------------------------------

const SSL_REQUEST: [u8; 8] = {
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
};

/// Negotiate the `SSLRequest`, assert `S`, and complete the TLS handshake to a
/// `localhost`-named `rustls` client stream.
async fn tls_connect(
    addr: SocketAddr,
    config: rustls::ClientConfig,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut tcp = TcpStream::connect(addr).await.expect("connect");
    tcp.write_all(&SSL_REQUEST).await.expect("SSLRequest");
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await.expect("read S/N");
    assert_eq!(answer[0], b'S', "server should accept the SSLRequest");
    TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .expect("TLS handshake")
}

/// The `tls-server-end-point` binding as the client computes it: SHA-256 of the
/// end-entity certificate the handshake presented.
fn peer_endpoint_binding(stream: &tokio_rustls::client::TlsStream<TcpStream>) -> Vec<u8> {
    let cert = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(<[_]>::first)
        .expect("server presented a certificate");
    sha256(cert.as_ref()).as_bytes().to_vec()
}

async fn write_startup<S: AsyncWrite + Unpin>(stream: &mut S, user: &str) {
    let body = format!("user\0{user}\0database\0stele\0\0");
    let len = 8 + body.len();
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&i32::try_from(len).unwrap().to_be_bytes());
    buf.extend_from_slice(&196_608_i32.to_be_bytes()); // protocol 3.0
    buf.extend_from_slice(body.as_bytes());
    stream.write_all(&buf).await.expect("write startup");
}

async fn read_msg<S: AsyncRead + Unpin>(stream: &mut S) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.expect("read header");
    let len = i32::from_be_bytes(header[1..5].try_into().unwrap());
    let mut payload = vec![0u8; usize::try_from(len - 4).unwrap()];
    stream.read_exact(&mut payload).await.expect("read payload");
    (header[0], payload)
}

/// Read an authentication request (`'R'`), returning `(code, data-after-code)`.
async fn read_auth<S: AsyncRead + Unpin>(stream: &mut S) -> (i32, Vec<u8>) {
    let (kind, payload) = read_msg(stream).await;
    assert_eq!(kind, b'R', "expected an authentication request");
    let code = i32::from_be_bytes(payload[..4].try_into().unwrap());
    (code, payload[4..].to_vec())
}

async fn write_sasl_initial<S: AsyncWrite + Unpin>(
    stream: &mut S,
    mechanism: &str,
    client_first: &str,
) {
    let mut data = Vec::new();
    data.extend_from_slice(mechanism.as_bytes());
    data.push(0);
    data.extend_from_slice(&i32::try_from(client_first.len()).unwrap().to_be_bytes());
    data.extend_from_slice(client_first.as_bytes());
    write_p(stream, &data).await;
}

async fn write_p<S: AsyncWrite + Unpin>(stream: &mut S, data: &[u8]) {
    let mut buf = Vec::with_capacity(5 + data.len());
    buf.push(b'p');
    buf.extend_from_slice(&i32::try_from(4 + data.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(data);
    stream.write_all(&buf).await.expect("write SASL response");
}

/// Pull `r=`/`s=`/`i=` out of a server-first-message.
fn parse_server_first(data: &[u8]) -> (String, Vec<u8>, u32) {
    let text = std::str::from_utf8(data).expect("server-first is UTF-8");
    let (mut nonce, mut salt, mut iters) = (None, None, None);
    for attr in text.split(',') {
        if let Some(v) = attr.strip_prefix("r=") {
            nonce = Some(v.to_owned());
        } else if let Some(v) = attr.strip_prefix("s=") {
            salt = Some(scram::b64_decode(v).expect("salt decodes"));
        } else if let Some(v) = attr.strip_prefix("i=") {
            iters = Some(v.parse().expect("iterations"));
        }
    }
    (nonce.unwrap(), salt.unwrap(), iters.unwrap())
}

/// The advertised SASL mechanism names (the list ends at the first empty name).
fn sasl_mechanisms(data: &[u8]) -> Vec<String> {
    data.split(|&b| b == 0)
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .take_while(|s| !s.is_empty())
        .collect()
}

/// The SQLSTATE (`C` field) of an `ErrorResponse` payload.
fn error_sqlstate(payload: &[u8]) -> String {
    let mut cursor = payload;
    while let Some((&code, rest)) = cursor.split_first() {
        if code == 0 {
            break;
        }
        let nul = rest.iter().position(|&b| b == 0).expect("field NUL");
        if code == b'C' {
            return String::from_utf8_lossy(&rest[..nul]).into_owned();
        }
        cursor = &rest[nul + 1..];
    }
    panic!("ErrorResponse without a SQLSTATE field");
}

/// Drive one SASL exchange from just after `write_startup` to its verdict.
///
/// `gs2_header` is the literal header the client sends (`n,,`, `y,,`, or
/// `p=tls-server-end-point,,`); `cbind` is the channel-binding data appended to
/// the `c=` value (and folded into the proof) under channel binding. Returns
/// `Ok(())` on `AuthenticationOk`, or `Err(sqlstate)` on an `ErrorResponse`.
async fn scram_exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    mechanism: &str,
    gs2_header: &str,
    cbind: Option<&[u8]>,
    password: &str,
    client_nonce: &str,
) -> Result<(), String> {
    let (code, _) = read_auth(stream).await;
    assert_eq!(code, 10, "expected AuthenticationSASL");

    let client_first_bare = format!("n=,r={client_nonce}");
    let client_first = format!("{gs2_header}{client_first_bare}");
    write_sasl_initial(stream, mechanism, &client_first).await;

    // Server-first, or a refusal at the client-first message (downgrade/flag).
    let (kind, payload) = read_msg(stream).await;
    match kind {
        b'E' => return Err(error_sqlstate(&payload)),
        b'R' => assert_eq!(
            i32::from_be_bytes(payload[..4].try_into().unwrap()),
            11,
            "expected AuthenticationSASLContinue"
        ),
        other => panic!("unexpected message {other:?} where server-first was due"),
    }
    let server_first = String::from_utf8(payload[4..].to_vec()).unwrap();
    let (server_nonce, salt, iterations) = parse_server_first(&payload[4..]);

    let mut cbind_input = gs2_header.as_bytes().to_vec();
    if let Some(data) = cbind {
        cbind_input.extend_from_slice(data);
    }
    let without_proof = format!("c={},r={server_nonce}", scram::b64_encode(&cbind_input));
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let proof = scram::client_proof(password, &salt, iterations, auth_message.as_bytes());
    let client_final = format!("{without_proof},p={}", scram::b64_encode(&proof));
    write_p(stream, client_final.as_bytes()).await;

    loop {
        let (kind, payload) = read_msg(stream).await;
        match kind {
            b'E' => return Err(error_sqlstate(&payload)),
            // AuthenticationSASLFinal (12) precedes AuthenticationOk (0).
            b'R' if i32::from_be_bytes(payload[..4].try_into().unwrap()) == 0 => return Ok(()),
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tls_server_advertises_scram_sha_256_plus_first() {
    // On TLS both mechanisms are offered, PLUS first so a channel-binding-capable
    // client prefers it (libpq's selection rule).
    let pki = mint_pki("advertise");
    let addr = spawn_server(&pki, "alice", "s3cret").await;
    let mut stream = tls_connect(addr, client_config(&pki.ca_pem)).await;
    write_startup(&mut stream, "alice").await;
    let (code, data) = read_auth(&mut stream).await;
    assert_eq!(code, 10);
    assert_eq!(
        sasl_mechanisms(&data),
        vec!["SCRAM-SHA-256-PLUS".to_owned(), "SCRAM-SHA-256".to_owned()],
    );
}

#[tokio::test]
async fn tokio_postgres_negotiates_scram_sha_256_plus_over_tls() {
    // A stock driver with `channel_binding=require`: it fails unless the server
    // both advertised PLUS and validated the binding. Independent SCRAM
    // implementation ⇒ true interoperability.
    let pki = mint_pki("interop");
    let addr = spawn_server(&pki, "alice", "s3cret").await;
    let connector = RustlsConnect(Arc::new(client_config(&pki.ca_pem)));
    let conn_str = format!(
        "host=127.0.0.1 port={} user=alice password=s3cret dbname=stele \
         sslmode=require channel_binding=require",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, connector)
        .await
        .expect("SCRAM-SHA-256-PLUS should authenticate over TLS");
    tokio::spawn(connection);
    let rows = client.query("SELECT 1", &[]).await.expect("query");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn correct_channel_binding_authenticates() {
    // Hand-rolled PLUS with the genuine endpoint binding: the c= check and the
    // proof both pass.
    let pki = mint_pki("plus-ok");
    let addr = spawn_server(&pki, "alice", "s3cret").await;
    let mut stream = tls_connect(addr, client_config(&pki.ca_pem)).await;
    let cbind = peer_endpoint_binding(&stream);
    write_startup(&mut stream, "alice").await;
    let result = scram_exchange(
        &mut stream,
        "SCRAM-SHA-256-PLUS",
        "p=tls-server-end-point,,",
        Some(&cbind),
        "s3cret",
        "plusnonce",
    )
    .await;
    assert_eq!(result, Ok(()), "genuine channel binding authenticates");
}

#[tokio::test]
async fn a_tampered_channel_binding_is_refused() {
    // The MITM case: the client's proof is valid, but the binding it presents is
    // not the one the server computes from its own certificate, so the c= check
    // fails. This is the property channel binding exists to enforce.
    let pki = mint_pki("tamper");
    let addr = spawn_server(&pki, "alice", "s3cret").await;
    let mut stream = tls_connect(addr, client_config(&pki.ca_pem)).await;
    let mut cbind = peer_endpoint_binding(&stream);
    cbind[0] ^= 0xFF; // a different endpoint than the one the server presents
    write_startup(&mut stream, "alice").await;
    let result = scram_exchange(
        &mut stream,
        "SCRAM-SHA-256-PLUS",
        "p=tls-server-end-point,,",
        Some(&cbind),
        "s3cret",
        "tampernonce",
    )
    .await;
    assert_eq!(
        result,
        Err("08P01".to_owned()),
        "a tampered channel binding must be refused"
    );
}

#[tokio::test]
async fn the_y_flag_is_refused_when_plus_is_advertised() {
    // `y` means "I support channel binding but you don't advertise it". Over a
    // connection where the server DID advertise PLUS, that can only be a MITM
    // having stripped the offer — the RFC 5802 §6 downgrade rule (STL-297 flips
    // the STL-252 accept here).
    let pki = mint_pki("downgrade");
    let addr = spawn_server(&pki, "alice", "s3cret").await;
    let mut stream = tls_connect(addr, client_config(&pki.ca_pem)).await;
    write_startup(&mut stream, "alice").await;
    let result = scram_exchange(
        &mut stream,
        "SCRAM-SHA-256",
        "y,,",
        None,
        "s3cret",
        "downgradenonce",
    )
    .await;
    assert_eq!(
        result,
        Err("08P01".to_owned()),
        "a stripped-PLUS downgrade must be refused"
    );
}

#[tokio::test]
async fn plain_scram_still_authenticates_over_tls() {
    // A client that opts out of channel binding (`n`) still authenticates with
    // plain SCRAM over the encrypted channel — PLUS is offered, never required.
    let pki = mint_pki("plain-over-tls");
    let addr = spawn_server(&pki, "alice", "s3cret").await;
    let mut stream = tls_connect(addr, client_config(&pki.ca_pem)).await;
    write_startup(&mut stream, "alice").await;
    let result = scram_exchange(
        &mut stream,
        "SCRAM-SHA-256",
        "n,,",
        None,
        "s3cret",
        "plainnonce",
    )
    .await;
    assert_eq!(result, Ok(()), "plain SCRAM works over TLS");
}
