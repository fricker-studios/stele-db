//! SCRAM-SHA-256 authentication on the startup path, end-to-end over real
//! sockets (STL-252).
//!
//! Two kinds of client drive the [`Server`]:
//!
//! * **`tokio-postgres`** — a stock driver with its own independent SCRAM
//!   implementation, proving interoperability: the right password
//!   authenticates and round-trips a query, a wrong password and an unknown
//!   user are refused with SQLSTATE `28P01`, and `ALTER`/`DROP USER` take
//!   effect on the next connection.
//! * **A raw socket** speaking the SASL messages by hand (the proof math from
//!   `stele_common::scram`), pinning the nonce-freshness Definition-of-Done:
//!   two exchanges never share a server nonce, the server-final signature
//!   verifies against the derived `ServerKey`, and a **captured exchange
//!   replays nothing** — resending a recorded client-final against a fresh
//!   exchange is refused.

mod common;

use std::sync::{Arc, Mutex};

use stele_common::scram::{self, ScramVerifier};
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{AuthMode, Server, SharedSession};
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

// ---------------------------------------------------------------------------
// Plumbing
// ---------------------------------------------------------------------------

/// A fresh engine with `users` pre-created via the real SQL path.
fn session_with_users(users: &[(&str, &str)]) -> SharedSession {
    let mut engine = SessionEngine::open(MemDisk::new(), SystemClock);
    for (name, password) in users {
        let sql = format!("CREATE USER {name} PASSWORD '{password}'");
        let stmt = &stele_sql::parse(&sql).expect("parse")[0];
        engine.execute(stmt).expect("create user");
    }
    Arc::new(Mutex::new(engine))
}

/// Boot a SCRAM-required server on an ephemeral port.
async fn spawn_scram_server(session: SharedSession) -> std::net::SocketAddr {
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .with_auth(AuthMode::Scram)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

fn conn_str(addr: std::net::SocketAddr, user: &str, password: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user={user} password={password} dbname=stele sslmode=disable",
        addr.port()
    )
}

/// Connect expecting a refusal, returning the driver error. (A plain
/// `expect_err` needs `Debug` on the success pair, which the driver lacks.)
async fn connect_refused(
    addr: std::net::SocketAddr,
    user: &str,
    password: &str,
) -> tokio_postgres::Error {
    match tokio_postgres::connect(&conn_str(addr, user, password), NoTls).await {
        Ok(_) => panic!("connection for {user:?} should have been refused"),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Driver interoperability (tokio-postgres speaks SCRAM natively)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scram_authenticates_a_known_user_end_to_end() {
    let addr = spawn_scram_server(session_with_users(&[("alice", "s3cret")])).await;
    let (client, connection) = tokio_postgres::connect(&conn_str(addr, "alice", "s3cret"), NoTls)
        .await
        .expect("SCRAM authentication should succeed");
    tokio::spawn(connection);
    let rows = client.query("SELECT 1", &[]).await.expect("query");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn wrong_password_is_refused_with_28p01() {
    let addr = spawn_scram_server(session_with_users(&[("alice", "s3cret")])).await;
    let err = connect_refused(addr, "alice", "wrong").await;
    assert_eq!(
        err.code(),
        Some(&SqlState::INVALID_PASSWORD),
        "expected 28P01, got: {err}"
    );
}

#[tokio::test]
async fn unknown_user_is_refused_with_28p01() {
    // Same SQLSTATE and message shape as a wrong password — the doomed mock
    // exchange keeps the error channel from enumerating users.
    let addr = spawn_scram_server(session_with_users(&[("alice", "s3cret")])).await;
    let err = connect_refused(addr, "mallory", "whatever").await;
    assert_eq!(
        err.code(),
        Some(&SqlState::INVALID_PASSWORD),
        "expected 28P01, got: {err}"
    );
}

#[tokio::test]
async fn alter_and_drop_user_take_effect_on_the_next_connection() {
    let addr = spawn_scram_server(session_with_users(&[("alice", "first")])).await;

    // Authenticate and rotate the password over the wire itself.
    let (client, connection) = tokio_postgres::connect(&conn_str(addr, "alice", "first"), NoTls)
        .await
        .expect("initial password works");
    tokio::spawn(connection);
    client
        .simple_query("ALTER USER alice PASSWORD 'second'")
        .await
        .expect("rotate over the wire");

    // The old password is dead, the new one lives.
    let err = connect_refused(addr, "alice", "first").await;
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD));
    let (client, connection) = tokio_postgres::connect(&conn_str(addr, "alice", "second"), NoTls)
        .await
        .expect("rotated password works");
    tokio::spawn(connection);

    // Drop and verify the next authentication is refused.
    client
        .simple_query("DROP USER alice")
        .await
        .expect("drop over the wire");
    let err = connect_refused(addr, "alice", "second").await;
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD));
}

#[tokio::test]
async fn trust_mode_accepts_without_a_password() {
    // The default (and dev) posture is unchanged by STL-252.
    let session = session_with_users(&[]);
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("trust mode needs no password");
    tokio::spawn(connection);
    let rows = client.query("SELECT 1", &[]).await.expect("query");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

// ---------------------------------------------------------------------------
// Raw-socket exchange — nonce freshness + replay refusal (DoD)
// ---------------------------------------------------------------------------

const PROTOCOL_3_0: i32 = 196_608;

async fn write_startup(stream: &mut TcpStream, user: &str) {
    let body = format!("user\0{user}\0database\0stele\0\0");
    let len = 8 + body.len();
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&i32::try_from(len).unwrap().to_be_bytes());
    buf.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
    buf.extend_from_slice(body.as_bytes());
    stream.write_all(&buf).await.expect("write startup");
}

async fn read_msg(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.expect("read header");
    let len = i32::from_be_bytes(header[1..5].try_into().unwrap());
    let mut payload = vec![0u8; usize::try_from(len - 4).unwrap()];
    stream.read_exact(&mut payload).await.expect("read payload");
    (header[0], payload)
}

/// Read an `'R'` authentication request and return `(code, data)`.
async fn read_auth(stream: &mut TcpStream) -> (i32, Vec<u8>) {
    let (kind, payload) = read_msg(stream).await;
    assert_eq!(kind, b'R', "expected an authentication request");
    let code = i32::from_be_bytes(payload[..4].try_into().unwrap());
    (code, payload[4..].to_vec())
}

async fn write_sasl_initial(stream: &mut TcpStream, client_first: &str) {
    let mut data = Vec::new();
    data.extend_from_slice(b"SCRAM-SHA-256\0");
    data.extend_from_slice(&i32::try_from(client_first.len()).unwrap().to_be_bytes());
    data.extend_from_slice(client_first.as_bytes());
    write_p(stream, &data).await;
}

async fn write_p(stream: &mut TcpStream, data: &[u8]) {
    let mut buf = Vec::with_capacity(5 + data.len());
    buf.push(b'p');
    buf.extend_from_slice(&i32::try_from(4 + data.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(data);
    stream.write_all(&buf).await.expect("write SASL response");
}

/// Pull `r=`/`s=`/`i=` out of a server-first-message.
fn parse_server_first(data: &[u8]) -> (String, Vec<u8>, u32) {
    let text = std::str::from_utf8(data).expect("server-first is UTF-8");
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    for attr in text.split(',') {
        if let Some(v) = attr.strip_prefix("r=") {
            nonce = Some(v.to_owned());
        } else if let Some(v) = attr.strip_prefix("s=") {
            salt = Some(scram::b64_decode(v).expect("salt decodes"));
        } else if let Some(v) = attr.strip_prefix("i=") {
            iterations = Some(v.parse().expect("iteration count"));
        }
    }
    (
        nonce.expect("server nonce"),
        salt.expect("salt"),
        iterations.expect("iterations"),
    )
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

/// One recorded successful exchange: what a wire eavesdropper would capture.
struct CapturedExchange {
    client_first: String,
    server_nonce: String,
    client_final: String,
}

/// Drive a full hand-rolled SCRAM exchange for `user`/`password` with a fixed
/// client nonce, asserting success and verifying the server's own signature.
async fn run_exchange(
    addr: std::net::SocketAddr,
    user: &str,
    password: &str,
    client_nonce: &str,
) -> CapturedExchange {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    write_startup(&mut stream, user).await;

    // AuthenticationSASL advertises exactly SCRAM-SHA-256.
    let (code, data) = read_auth(&mut stream).await;
    assert_eq!(code, 10, "expected AuthenticationSASL");
    assert_eq!(&data, b"SCRAM-SHA-256\0\0", "mechanism list");

    let client_first_bare = format!("n=,r={client_nonce}");
    let client_first = format!("n,,{client_first_bare}");
    write_sasl_initial(&mut stream, &client_first).await;

    let (code, data) = read_auth(&mut stream).await;
    assert_eq!(code, 11, "expected AuthenticationSASLContinue");
    let server_first = String::from_utf8(data.clone()).expect("UTF-8");
    let (server_nonce, salt, iterations) = parse_server_first(&data);
    assert!(
        server_nonce.starts_with(client_nonce) && server_nonce.len() > client_nonce.len(),
        "server nonce extends the client's"
    );

    let without_proof = format!("c=biws,r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let proof = scram::client_proof(password, &salt, iterations, auth_message.as_bytes());
    let client_final = format!("{without_proof},p={}", scram::b64_encode(&proof));
    write_p(&mut stream, client_final.as_bytes()).await;

    // SASLFinal carries v=ServerSignature — verify it against the derived
    // verifier, mutually authenticating the server.
    let (code, data) = read_auth(&mut stream).await;
    assert_eq!(code, 12, "expected AuthenticationSASLFinal");
    let verifier = ScramVerifier::derive(password, &salt, iterations);
    let expected = format!(
        "v={}",
        scram::b64_encode(&verifier.server_signature(auth_message.as_bytes()))
    );
    assert_eq!(String::from_utf8(data).expect("UTF-8"), expected);

    let (code, _) = read_auth(&mut stream).await;
    assert_eq!(code, 0, "expected AuthenticationOk");

    CapturedExchange {
        client_first,
        server_nonce,
        client_final,
    }
}

#[tokio::test]
async fn server_nonces_are_fresh_and_a_captured_exchange_does_not_replay() {
    let addr = spawn_scram_server(session_with_users(&[("alice", "s3cret")])).await;

    // Exchange A succeeds; record everything an eavesdropper would see.
    let captured = run_exchange(addr, "alice", "s3cret", "fixedclientnonce").await;

    // Exchange B: replay the captured client-first byte-for-byte. The server
    // must answer with a *fresh* nonce…
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    write_startup(&mut stream, "alice").await;
    let (code, _) = read_auth(&mut stream).await;
    assert_eq!(code, 10);
    write_sasl_initial(&mut stream, &captured.client_first).await;
    let (code, data) = read_auth(&mut stream).await;
    assert_eq!(code, 11);
    let (fresh_nonce, _, _) = parse_server_first(&data);
    assert_ne!(
        fresh_nonce, captured.server_nonce,
        "an exchange must never reuse a server nonce"
    );

    // …so the captured client-final — valid proof and all — replays nothing.
    write_p(&mut stream, captured.client_final.as_bytes()).await;
    let (kind, payload) = read_msg(&mut stream).await;
    assert_eq!(kind, b'E', "a replayed exchange must be refused");
    let sqlstate = error_sqlstate(&payload);
    assert!(
        sqlstate == "08P01" || sqlstate == "28P01",
        "refusal carries an authentication/protocol SQLSTATE, got {sqlstate}"
    );
}

#[tokio::test]
async fn channel_binding_demand_is_refused_without_plus() {
    // `p=…` demands SCRAM-SHA-256-PLUS, which the server does not advertise.
    let addr = spawn_scram_server(session_with_users(&[("alice", "s3cret")])).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    write_startup(&mut stream, "alice").await;
    let (code, _) = read_auth(&mut stream).await;
    assert_eq!(code, 10);
    write_sasl_initial(&mut stream, "p=tls-server-end-point,,n=,r=abcdefgh").await;
    let (kind, payload) = read_msg(&mut stream).await;
    assert_eq!(kind, b'E');
    assert_eq!(error_sqlstate(&payload), "08P01");
}

#[tokio::test]
async fn startup_without_a_user_is_refused_under_scram() {
    let addr = spawn_scram_server(session_with_users(&[("alice", "s3cret")])).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // A startup message carrying only `database`.
    let body = "database\0stele\0\0";
    let len = 8 + body.len();
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&i32::try_from(len).unwrap().to_be_bytes());
    buf.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
    buf.extend_from_slice(body.as_bytes());
    stream.write_all(&buf).await.expect("write startup");
    let (kind, payload) = read_msg(&mut stream).await;
    assert_eq!(kind, b'E');
    assert_eq!(error_sqlstate(&payload), "28000");
}
