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
//!
//! The same gating holds on the **extended-query** path — `Parse`/`Bind`/`Execute`/
//! `Sync` (STL-319), the protocol JDBC / psycopg / pgAdmin speak — where the trailer
//! is emitted once the portal fully drains, and never on a `PortalSuspended` partial
//! fetch.

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

// ---------------------------------------------------------------------------
// Extended-query (Parse / Bind / Execute / Sync) path (STL-319)
// ---------------------------------------------------------------------------

/// A `Parse` body: statement name + query (both NUL-terminated), then an `Int16`
/// parameter-OID count (zero — the SELECTs under test take no parameters).
fn parse_body(name: &str, query: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(name.as_bytes());
    b.push(0);
    b.extend_from_slice(query.as_bytes());
    b.push(0);
    b.extend_from_slice(&0i16.to_be_bytes()); // no parameter type OIDs
    b
}

/// A `Bind` body for a parameterless portal: no parameter format codes, no
/// parameters, no result format codes (every column ships text).
fn bind_body(portal: &str, statement: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(statement.as_bytes());
    b.push(0);
    b.extend_from_slice(&0i16.to_be_bytes()); // zero parameter format codes
    b.extend_from_slice(&0i16.to_be_bytes()); // zero parameters
    b.extend_from_slice(&0i16.to_be_bytes()); // zero result format codes
    b
}

/// An `Execute` body: portal name then an `Int32` row cap (`0` = every remaining
/// row; a positive cap stops with `PortalSuspended` and resumes on the next one).
fn execute_body(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(portal.as_bytes());
    b.push(0);
    b.extend_from_slice(&max_rows.to_be_bytes());
    b
}

/// A `Describe` body targeting a *portal* ('P'): the message a JDBC / psycopg
/// client sends before Execute to learn the result shape.
fn describe_portal_body(name: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(b'P'); // 'P' = portal (vs 'S' = prepared statement)
    b.extend_from_slice(name.as_bytes());
    b.push(0);
    b
}

/// Seed the shared two-row versioned fixture over the simple-query path, leaving
/// the stream idle at `ReadyForQuery` for the extended-query batch that reads it.
async fn seed_two_rows(stream: &mut TcpStream) {
    run_query(
        stream,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING",
    )
    .await;
    run_query(stream, "INSERT INTO t VALUES (1, 10), (2, 20)").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_opted_in_extended_query_gets_the_stats_trailer_before_command_complete() {
    let addr = spawn().await;
    let mut stream = connect(addr, true).await;
    seed_two_rows(&mut stream).await;

    // Parse / Bind / Execute / Sync the SELECT — the path `stele shell` never takes
    // but JDBC / psycopg / pgAdmin do. One batch, then the trailing Sync.
    let mut batch = frame(b'P', &parse_body("", "SELECT id, v FROM t"));
    batch.extend(frame(b'B', &bind_body("", "")));
    batch.extend(frame(b'E', &execute_body("", 0)));
    batch.extend(frame(b'S', &[]));
    stream.write_all(&batch).await.expect("write P/B/E/S");
    stream.flush().await.expect("flush");
    let frames = drain_until_ready(&mut stream).await;

    let stats =
        stats_from(&frames).expect("the extended-query SELECT carries a stats NoticeResponse");
    assert_eq!(stats.rows, 2, "the footer reports the scanned row count");
    assert!(!stats.time_travel, "a live read is not time-travel");
    assert_eq!(stats.segments_total, 0, "unflushed → no sealed segments");

    // The trailer annotates the just-finished portal: after the final DataRow,
    // before CommandComplete.
    let last_row_at = frames
        .iter()
        .rposition(|f| f.kind == b'D')
        .expect("DataRow");
    let notice_at = frames.iter().position(|f| f.kind == b'N').expect("notice");
    let complete_at = frames
        .iter()
        .position(|f| f.kind == b'C')
        .expect("CommandComplete");
    assert!(
        last_row_at < notice_at,
        "the notice follows the final DataRow"
    );
    assert!(
        notice_at < complete_at,
        "the stats notice comes before CommandComplete",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_non_opted_in_extended_query_gets_no_stats_trailer() {
    let addr = spawn().await;
    let mut stream = connect(addr, false).await;
    seed_two_rows(&mut stream).await;

    let mut batch = frame(b'P', &parse_body("", "SELECT id, v FROM t"));
    batch.extend(frame(b'B', &bind_body("", "")));
    batch.extend(frame(b'E', &execute_body("", 0)));
    batch.extend(frame(b'S', &[]));
    stream.write_all(&batch).await.expect("write P/B/E/S");
    stream.flush().await.expect("flush");
    let frames = drain_until_ready(&mut stream).await;

    // A stock driver's extended-query stream stays byte-identical: no NoticeResponse.
    assert!(
        !frames.iter().any(|f| f.kind == b'N'),
        "a non-opted-in extended query receives no NoticeResponse",
    );
    assert_eq!(
        frames.iter().filter(|f| f.kind == b'D').count(),
        2,
        "both rows still stream",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_portal_suspended_partial_fetch_defers_the_trailer_until_completion() {
    let addr = spawn().await;
    let mut stream = connect(addr, true).await;
    seed_two_rows(&mut stream).await;

    // Cap the first Execute at a single row (it stops with `PortalSuspended`), then
    // a second Execute drains the rest. The trailer must appear exactly once, and
    // only at completion — not riding the partial fetch.
    let mut batch = frame(b'P', &parse_body("", "SELECT id, v FROM t"));
    batch.extend(frame(b'B', &bind_body("", "")));
    batch.extend(frame(b'E', &execute_body("", 1)));
    batch.extend(frame(b'E', &execute_body("", 0)));
    batch.extend(frame(b'S', &[]));
    stream.write_all(&batch).await.expect("write P/B/E/E/S");
    stream.flush().await.expect("flush");
    let frames = drain_until_ready(&mut stream).await;

    assert_eq!(
        frames.iter().filter(|f| f.kind == b'N').count(),
        1,
        "exactly one stats trailer for the whole portal, not one per Execute",
    );
    let suspended_at = frames
        .iter()
        .position(|f| f.kind == b's')
        .expect("PortalSuspended");
    let notice_at = frames.iter().position(|f| f.kind == b'N').expect("notice");
    let complete_at = frames
        .iter()
        .position(|f| f.kind == b'C')
        .expect("CommandComplete");
    assert!(
        suspended_at < notice_at,
        "the partial fetch's PortalSuspended carries no trailer; it waits for completion",
    );
    assert!(
        notice_at < complete_at,
        "the trailer precedes the final CommandComplete",
    );
    let stats = stats_from(&frames).expect("the completed portal carries the stats");
    assert_eq!(
        stats.rows, 2,
        "the footer reports the full scan, not the final chunk",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_then_execute_still_emits_the_trailer() {
    // A realistic driver issues `Describe('P')` before `Execute`; that runs and
    // caches the read, so the Execute must still find — and emit — the stats.
    let addr = spawn().await;
    let mut stream = connect(addr, true).await;
    seed_two_rows(&mut stream).await;

    let mut batch = frame(b'P', &parse_body("", "SELECT id, v FROM t"));
    batch.extend(frame(b'B', &bind_body("", "")));
    batch.extend(frame(b'D', &describe_portal_body("")));
    batch.extend(frame(b'E', &execute_body("", 0)));
    batch.extend(frame(b'S', &[]));
    stream.write_all(&batch).await.expect("write P/B/D/E/S");
    stream.flush().await.expect("flush");
    let frames = drain_until_ready(&mut stream).await;

    assert!(
        frames.iter().any(|f| f.kind == b'T'),
        "Describe('P') replies a RowDescription",
    );
    let stats = stats_from(&frames).expect("a Describe before Execute does not consume the stats");
    assert_eq!(stats.rows, 2, "the footer reports the scanned row count");
    let notice_at = frames.iter().position(|f| f.kind == b'N').expect("notice");
    let complete_at = frames
        .iter()
        .position(|f| f.kind == b'C')
        .expect("CommandComplete");
    assert!(
        notice_at < complete_at,
        "the stats notice still precedes CommandComplete",
    );
}
