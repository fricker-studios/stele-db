//! Extended-protocol `Bind` parameter-count enforcement over the wire (STL-222).
//!
//! Postgres rejects a `Bind` whose supplied parameter count disagrees with the
//! prepared statement's `$n` placeholder count as a protocol violation; Stele used
//! to silently drop surplus parameters (surfaced in the STL-219 Copilot review:
//! an admin command, or any zero-placeholder statement like `SELECT 1`, ran anyway
//! with the extras dropped). This suite drives the raw extended-query message flow
//! — `tokio-postgres` validates the count client-side and so cannot send a
//! mismatched `Bind` — to prove both halves of the Definition of Done:
//!
//! * a parameterless statement bound with one parameter is refused with
//!   `SQLSTATE 08P01` (`protocol_violation`), and
//! * the same statement bound with zero parameters still binds and executes.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;

mod common;

const PROTOCOL_3_0: i32 = 196_608;

// ---------------------------------------------------------------------------
// Minimal extended-protocol client
// ---------------------------------------------------------------------------

/// A decoded backend message: its type byte and raw payload (length-prefix
/// stripped).
struct Frame {
    kind: u8,
    payload: Vec<u8>,
}

/// Frame a typed frontend message: type byte, `Int32` length (covering the length
/// field + body), then the body.
fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let len = i32::try_from(4 + body.len()).expect("message body fits in i32");
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(kind);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A `StartupMessage` body for protocol 3.0 with `user`/`database` params (v0.1
/// has no auth, so any values are accepted).
fn startup() -> Vec<u8> {
    let mut params = Vec::new();
    for (k, v) in [("user", "stele"), ("database", "stele")] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0); // terminating empty key

    let len = i32::try_from(8 + params.len()).expect("startup fits in i32");
    let mut out = Vec::with_capacity(8 + params.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
    out.extend_from_slice(&params);
    out
}

/// A `Parse` body: statement name, query (both NUL-terminated), then an `Int16`
/// count of parameter type OIDs followed by that many `Int32` OIDs.
fn parse_body(name: &str, query: &str, oids: &[u32]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(name.as_bytes());
    b.push(0);
    b.extend_from_slice(query.as_bytes());
    b.push(0);
    b.extend_from_slice(&i16::try_from(oids.len()).unwrap().to_be_bytes());
    for oid in oids {
        b.extend_from_slice(&oid.to_be_bytes());
    }
    b
}

/// A `Bind` body with no format codes (all text) and no result format codes; each
/// parameter is a text value (`None` = SQL NULL, the `-1` length sentinel).
fn bind_body(portal: &str, statement: &str, params: &[Option<&[u8]>]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(statement.as_bytes());
    b.push(0);
    b.extend_from_slice(&0i16.to_be_bytes()); // zero parameter format codes → all text
    b.extend_from_slice(&i16::try_from(params.len()).unwrap().to_be_bytes());
    for p in params {
        match p {
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                b.extend_from_slice(&i32::try_from(bytes.len()).unwrap().to_be_bytes());
                b.extend_from_slice(bytes);
            }
        }
    }
    b.extend_from_slice(&0i16.to_be_bytes()); // zero result format codes
    b
}

/// An `Execute` body: portal name then an `Int32` row cap (`0` = no limit).
fn execute_body(portal: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(&0i32.to_be_bytes());
    b
}

/// Read one backend frame (type byte + `Int32` length + body).
async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.expect("read header");
    let len = i32::from_be_bytes(header[1..5].try_into().unwrap());
    let body_len = usize::try_from(len - 4).expect("non-negative body length");
    let mut payload = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut payload).await.expect("read body");
    }
    Frame {
        kind: header[0],
        payload,
    }
}

/// Drain frames up to and including the next `ReadyForQuery` ('Z').
async fn drain_until_ready(stream: &mut TcpStream) -> Vec<Frame> {
    let mut frames = Vec::new();
    loop {
        let f = read_frame(stream).await;
        let done = f.kind == b'Z';
        frames.push(f);
        if done {
            return frames;
        }
    }
}

/// The `SQLSTATE` ('C' field) carried by an `ErrorResponse` payload, if present.
fn sqlstate(payload: &[u8]) -> Option<String> {
    let mut i = 0;
    while i < payload.len() && payload[i] != 0 {
        let code = payload[i];
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let text = String::from_utf8_lossy(&payload[start..i]).into_owned();
        i += 1; // skip the field's NUL terminator
        if code == b'C' {
            return Some(text);
        }
    }
    None
}

/// Connect and complete the startup handshake, leaving the stream idle at the
/// first `ReadyForQuery`.
async fn connect(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&startup()).await.expect("write startup");
    stream.flush().await.expect("flush startup");
    drain_until_ready(&mut stream).await; // AuthOk → ParameterStatus* → BackendKeyData → ReadyForQuery
    stream
}

fn spawn() -> impl std::future::Future<Output = SocketAddr> {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    common::spawn_server(session)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bind_with_too_many_parameters_is_rejected() {
    let addr = spawn().await;
    let mut stream = connect(addr).await;

    // Prepare a parameterless statement, then Bind it with one parameter — the
    // exact mismatch the ticket calls out. Postgres answers `08P01`.
    let mut batch = frame(b'P', &parse_body("", "SELECT 1", &[]));
    batch.extend(frame(b'B', &bind_body("", "", &[Some(b"1")])));
    batch.extend(frame(b'S', &[])); // Sync
    stream.write_all(&batch).await.expect("write P/B/S");
    stream.flush().await.expect("flush");

    let frames = drain_until_ready(&mut stream).await;
    // ParseComplete succeeds; the Bind is what fails.
    assert!(
        frames.iter().any(|f| f.kind == b'1'),
        "Parse of the parameterless statement still completes",
    );
    let error = frames
        .iter()
        .find(|f| f.kind == b'E')
        .expect("Bind is rejected with an ErrorResponse");
    assert_eq!(
        sqlstate(&error.payload).as_deref(),
        Some("08P01"),
        "parameter-count mismatch is a protocol violation",
    );
    // The surplus parameter must not have produced a portal that runs anyway.
    assert!(
        !frames.iter().any(|f| f.kind == b'2'),
        "no BindComplete is sent for the rejected Bind",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bind_with_the_matching_count_still_binds_and_executes() {
    let addr = spawn().await;
    let mut stream = connect(addr).await;

    // The same parameterless statement, bound with zero parameters, binds and runs.
    let mut batch = frame(b'P', &parse_body("", "SELECT 1", &[]));
    batch.extend(frame(b'B', &bind_body("", "", &[])));
    batch.extend(frame(b'E', &execute_body("")));
    batch.extend(frame(b'S', &[])); // Sync
    stream.write_all(&batch).await.expect("write P/B/E/S");
    stream.flush().await.expect("flush");

    let frames = drain_until_ready(&mut stream).await;
    assert!(
        !frames.iter().any(|f| f.kind == b'E'),
        "a matching parameter count is not rejected",
    );
    assert!(
        frames.iter().any(|f| f.kind == b'2'),
        "BindComplete is sent for the matching Bind",
    );
    assert!(
        frames.iter().any(|f| f.kind == b'D'),
        "Execute streams the SELECT 1 row",
    );
    assert!(
        frames.iter().any(|f| f.kind == b'C'),
        "Execute finishes with CommandComplete",
    );
}
