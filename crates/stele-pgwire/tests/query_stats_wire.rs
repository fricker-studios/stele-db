//! The opt-in query-stats `NoticeResponse` trailer over the wire (STL-201).
//!
//! Drives the raw simple-query flow (so the test controls the `StartupMessage`
//! parameters `tokio-postgres` would not send) to prove both halves of the
//! gating:
//!
//! * a connection that opted in with `stele_stats=on` receives a `NoticeResponse`
//!   carrying the parseable stats line — after the rows, before `CommandComplete`;
//! * a connection that did **not** opt in (every psql / JDBC / psycopg client)
//!   receives no such notice, so the trailer is invisible to the driver gate.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use stele_common::query_stats::QueryStats;
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;

mod common;

const PROTOCOL_3_0: i32 = 196_608;

struct Frame {
    kind: u8,
    payload: Vec<u8>,
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let len = i32::try_from(4 + body.len()).expect("body fits in i32");
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(kind);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A `StartupMessage`, optionally opting in to the query-stats trailer.
fn startup(opt_in_stats: bool) -> Vec<u8> {
    let mut params = Vec::new();
    let mut pairs = vec![("user", "stele"), ("database", "stele")];
    if opt_in_stats {
        pairs.push(("stele_stats", "on"));
    }
    for (k, v) in pairs {
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

/// Drain frames up to and including the next `ReadyForQuery` ('Z'), timeout-bounded.
async fn drain_until_ready(stream: &mut TcpStream) -> Vec<Frame> {
    let drain = async {
        let mut frames = Vec::new();
        loop {
            let f = read_frame(stream).await;
            let done = f.kind == b'Z';
            frames.push(f);
            if done {
                return frames;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("server replies with ReadyForQuery within the timeout")
}

/// A field-coded message body's value for `code` (the `ErrorResponse` /
/// `NoticeResponse` layout: repeated `Byte1 code` + NUL-terminated value).
fn field(payload: &[u8], code: u8) -> Option<String> {
    let mut i = 0;
    while i < payload.len() && payload[i] != 0 {
        let c = payload[i];
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let text = String::from_utf8_lossy(&payload[start..i]).into_owned();
        i += 1; // skip the field NUL
        if c == code {
            return Some(text);
        }
    }
    None
}

/// Send a simple `Query` and drain its reply up to `ReadyForQuery`.
async fn run_query(stream: &mut TcpStream, sql: &str) -> Vec<Frame> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    stream
        .write_all(&frame(b'Q', &body))
        .await
        .expect("write Q");
    stream.flush().await.expect("flush");
    drain_until_ready(stream).await
}

async fn connect(addr: SocketAddr, opt_in_stats: bool) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(&startup(opt_in_stats))
        .await
        .expect("write startup");
    stream.flush().await.expect("flush startup");
    drain_until_ready(&mut stream).await;
    stream
}

fn spawn() -> impl std::future::Future<Output = SocketAddr> {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    common::spawn_server(session)
}

/// The parsed stats line carried by a `NoticeResponse` ('N') frame, if any.
fn stats_from(frames: &[Frame]) -> Option<QueryStats> {
    frames
        .iter()
        .filter(|f| f.kind == b'N')
        .find_map(|f| QueryStats::parse_notice(&field(&f.payload, b'M')?))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_opted_in_connection_gets_the_stats_trailer_before_command_complete() {
    let addr = spawn().await;
    let mut stream = connect(addr, true).await;

    run_query(
        &mut stream,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    )
    .await;
    run_query(&mut stream, "INSERT INTO t VALUES (1, 10), (2, 20)").await;
    let frames = run_query(&mut stream, "SELECT id, v FROM t").await;

    let stats = stats_from(&frames).expect("the SELECT carries a stats NoticeResponse");
    assert_eq!(stats.rows, 2, "the footer reports the returned row count");
    assert!(!stats.time_travel, "a live read is not time-travel");
    assert_eq!(stats.segments_total, 0, "unflushed → no sealed segments");

    // The trailer must precede CommandComplete (it annotates the just-finished
    // result, like a notice raised during execution).
    let notice_at = frames.iter().position(|f| f.kind == b'N').expect("notice");
    let complete_at = frames
        .iter()
        .position(|f| f.kind == b'C')
        .expect("CommandComplete");
    assert!(
        notice_at < complete_at,
        "the stats notice comes before CommandComplete",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_did_not_opt_in_gets_no_stats_trailer() {
    let addr = spawn().await;
    let mut stream = connect(addr, false).await;

    run_query(
        &mut stream,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    )
    .await;
    run_query(&mut stream, "INSERT INTO t VALUES (1, 10)").await;
    let frames = run_query(&mut stream, "SELECT id, v FROM t").await;

    // No NoticeResponse at all — a stock driver's wire stream is byte-identical to
    // before STL-201, so the psql / JDBC / psycopg gate is unaffected.
    assert!(
        !frames.iter().any(|f| f.kind == b'N'),
        "a non-opted-in connection receives no NoticeResponse",
    );
    // The rows still arrive normally.
    assert_eq!(
        frames.iter().filter(|f| f.kind == b'D').count(),
        1,
        "the row is still streamed",
    );
}
