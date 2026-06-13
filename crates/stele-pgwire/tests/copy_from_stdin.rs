//! `COPY <table> FROM STDIN` bulk load over the wire ([STL-236] Definition of
//! Done).
//!
//! Two protocol paths reach the same [`run_copy_in`](stele_pgwire) driver:
//!
//! * **Extended query** — what `tokio-postgres` (`copy_in`) and psycopg use: a
//!   prepared `COPY` statement, `Bind`/`Execute`, then the `CopyData` stream. The
//!   `tokio_postgres_*` tests below drive a *real* third-party client end to end:
//!   a multi-thousand-row load, a CSV load, an all-or-nothing parse failure, and a
//!   COPY inside a `BEGIN` block with read-your-own-writes.
//! * **Simple query** — what `psql \copy` uses: a `Q` `COPY` message, then the
//!   same `CopyData`/`CopyDone` stream. The `simple_query_*` tests drive that path
//!   over a raw socket (`tokio-postgres` has no simple-protocol COPY), proving the
//!   reassembly of a row split across `CopyData` messages and the zero-rows-on-
//!   failure guarantee.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::SinkExt;
use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// A fresh in-memory server with the identity-demo `account` table already created.
async fn spawn_with_account() -> (tokio_postgres::Client, SocketAddr) {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    (client, addr)
}

/// The number of `SELECT` rows in a simple-query reply.
fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// The `balance` of the single row a `SELECT … WHERE id = …` returned, if any.
fn one_balance(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some(row.get("balance").expect("balance").to_owned()),
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// Extended-protocol COPY — the `tokio-postgres` (real driver) path
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_in_loads_many_rows_text_format() {
    let (client, _addr) = spawn_with_account().await;

    // A multi-thousand-row text payload (TAB-delimited, `\N`-NULL defaults).
    use std::fmt::Write as _;
    const N: i32 = 5_000;
    let mut payload = String::new();
    for id in 1..=N {
        writeln!(payload, "{id}\t{}", id * 10).unwrap();
    }

    let sink = client
        .copy_in("COPY account FROM STDIN")
        .await
        .expect("copy_in");
    let mut sink = std::pin::pin!(sink);
    sink.send(Bytes::from(payload))
        .await
        .expect("send copy data");
    let n = sink.finish().await.expect("finish copy");
    assert_eq!(n, u64::try_from(N).unwrap(), "COPY n counts every row");

    // Every row is visible, and a spot-checked row reads back its value.
    let all = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select all");
    assert_eq!(row_count(&all), usize::try_from(N).unwrap());
    let spot = client
        .simple_query("SELECT id, balance FROM account WHERE id = 4999")
        .await
        .expect("select one");
    assert_eq!(one_balance(&spot).as_deref(), Some("49990"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_in_csv_with_header() {
    let (client, _addr) = spawn_with_account().await;

    // CSV with a header line to skip and a quoted (here numeric) field.
    let payload = "id,balance\n1,100\n2,\"200\"\n3,300\n";
    let sink = client
        .copy_in("COPY account FROM STDIN WITH (FORMAT csv, HEADER)")
        .await
        .expect("copy_in csv");
    let mut sink = std::pin::pin!(sink);
    sink.send(Bytes::from_static(payload.as_bytes()))
        .await
        .expect("send csv");
    let n = sink.finish().await.expect("finish csv");
    assert_eq!(n, 3, "the header line is not counted");

    let spot = client
        .simple_query("SELECT id, balance FROM account WHERE id = 2")
        .await
        .expect("select");
    assert_eq!(one_balance(&spot).as_deref(), Some("200"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_in_parse_failure_leaves_zero_rows() {
    let (client, _addr) = spawn_with_account().await;

    // Row 2's balance is not an integer: the whole COPY must fail and leave zero
    // rows — the all-or-nothing DoD guarantee, proven by a follow-up SELECT.
    let payload = "1\t100\n2\toops\n3\t300\n";
    let sink = client
        .copy_in("COPY account FROM STDIN")
        .await
        .expect("copy_in");
    let mut sink = std::pin::pin!(sink);
    sink.send(Bytes::from_static(payload.as_bytes()))
        .await
        .expect("send");
    let err = sink.finish().await.expect_err("a bad row fails the COPY");
    // Postgres classifies a bad COPY value as invalid_text_representation (22P02).
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02"),
        "{err:?}"
    );

    // The connection recovers (the failed COPY latched to Sync), and no rows landed.
    let all = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after failed copy");
    assert_eq!(row_count(&all), 0, "a parse failure leaves zero rows");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_in_inside_a_transaction_is_ryow_then_commits() {
    let (client, _addr) = spawn_with_account().await;

    client.batch_execute("BEGIN").await.expect("begin");
    let sink = client
        .copy_in("COPY account FROM STDIN")
        .await
        .expect("copy_in in txn");
    let mut sink = std::pin::pin!(sink);
    sink.send(Bytes::from_static(b"1\t100\n2\t200\n"))
        .await
        .expect("send");
    let n = sink.finish().await.expect("finish");
    assert_eq!(n, 2);

    // Read-your-own-writes: the same transaction sees its staged COPY rows.
    let in_txn = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("ryow select");
    assert_eq!(row_count(&in_txn), 2, "the txn sees its own COPY");

    client.batch_execute("COMMIT").await.expect("commit");
    let after = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after commit");
    assert_eq!(row_count(&after), 2, "the COPY is durable after COMMIT");
}

// ---------------------------------------------------------------------------
// Simple-query COPY — the raw `psql \copy` path
// ---------------------------------------------------------------------------

const PROTOCOL_3_0: i32 = 196_608;

struct Frame {
    kind: u8,
    payload: Vec<u8>,
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let len = i32::try_from(4 + body.len()).expect("fits i32");
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(kind);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A `Query` ('Q') body: the SQL string, NUL-terminated.
fn query(sql: &str) -> Vec<u8> {
    let mut b = sql.as_bytes().to_vec();
    b.push(0);
    frame(b'Q', &b)
}

fn startup() -> Vec<u8> {
    let mut params = Vec::new();
    for (k, v) in [("user", "stele"), ("database", "stele")] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0);
    let len = i32::try_from(8 + params.len()).expect("fits i32");
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
    let body_len = usize::try_from(len - 4).expect("non-negative body");
    let mut payload = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut payload).await.expect("read body");
    }
    Frame {
        kind: header[0],
        payload,
    }
}

/// Read frames until (and including) `kind`, bounded by a timeout.
async fn read_until(stream: &mut TcpStream, kind: u8) -> Vec<Frame> {
    let drain = async {
        let mut frames = Vec::new();
        loop {
            let f = read_frame(stream).await;
            let done = f.kind == kind;
            frames.push(f);
            if done {
                return frames;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("server replies within timeout")
}

/// The command tag ('C' frame) text, or `None`.
fn command_tag(frames: &[Frame]) -> Option<String> {
    frames.iter().find_map(|f| {
        (f.kind == b'C').then(|| {
            let end = f
                .payload
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(f.payload.len());
            String::from_utf8_lossy(&f.payload[..end]).into_owned()
        })
    })
}

/// The SQLSTATE ('C' field of an ErrorResponse 'E' frame), or `None`.
fn error_sqlstate(frames: &[Frame]) -> Option<String> {
    let e = frames.iter().find(|f| f.kind == b'E')?;
    let mut i = 0;
    while i < e.payload.len() && e.payload[i] != 0 {
        let code = e.payload[i];
        i += 1;
        let start = i;
        while i < e.payload.len() && e.payload[i] != 0 {
            i += 1;
        }
        if code == b'C' {
            return Some(String::from_utf8_lossy(&e.payload[start..i]).into_owned());
        }
        i += 1;
    }
    None
}

async fn raw_connect(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&startup()).await.expect("write startup");
    stream.flush().await.expect("flush");
    read_until(&mut stream, b'Z').await; // through the first ReadyForQuery
    stream
}

fn spawn_raw_account() -> (SharedSession, impl std::future::Future<Output = SocketAddr>) {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    (Arc::clone(&session), common::spawn_server(session))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simple_query_copy_round_trip() {
    let (_session, server) = spawn_raw_account();
    let addr = server.await;
    let mut stream = raw_connect(addr).await;

    // CREATE over simple query.
    stream
        .write_all(&query(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        ))
        .await
        .expect("write create");
    read_until(&mut stream, b'Z').await;

    // COPY: the server replies CopyInResponse ('G') and waits for data — no
    // ReadyForQuery yet.
    stream
        .write_all(&query("COPY account FROM STDIN"))
        .await
        .expect("write copy");
    let g = read_frame(&mut stream).await;
    assert_eq!(
        g.kind, b'G',
        "server opens copy-in mode with CopyInResponse"
    );

    // Stream the data split across two CopyData frames so row 2 ("2\t200") spans
    // the boundary — exercising the reassembly the lexer relies on.
    stream
        .write_all(&frame(b'd', b"1\t100\n2\t2"))
        .await
        .expect("copy data 1");
    stream
        .write_all(&frame(b'd', b"00\n3\t300\n"))
        .await
        .expect("copy data 2");
    stream
        .write_all(&frame(b'c', &[]))
        .await
        .expect("copy done");
    stream.flush().await.expect("flush");

    let frames = read_until(&mut stream, b'Z').await;
    assert_eq!(command_tag(&frames).as_deref(), Some("COPY 3"));

    // The rows are visible.
    stream
        .write_all(&query("SELECT id FROM account"))
        .await
        .expect("write select");
    let rows = read_until(&mut stream, b'Z').await;
    let data_rows = rows.iter().filter(|f| f.kind == b'D').count();
    assert_eq!(data_rows, 3, "all three COPY rows are visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simple_query_copy_parse_failure_leaves_zero_rows() {
    let (_session, server) = spawn_raw_account();
    let addr = server.await;
    let mut stream = raw_connect(addr).await;

    stream
        .write_all(&query(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        ))
        .await
        .expect("write create");
    read_until(&mut stream, b'Z').await;

    stream
        .write_all(&query("COPY account FROM STDIN"))
        .await
        .expect("write copy");
    assert_eq!(read_frame(&mut stream).await.kind, b'G');

    // Row 2 is malformed — the whole COPY fails.
    stream
        .write_all(&frame(b'd', b"1\t100\n2\tnope\n3\t300\n"))
        .await
        .expect("copy data");
    stream
        .write_all(&frame(b'c', &[]))
        .await
        .expect("copy done");
    stream.flush().await.expect("flush");

    let frames = read_until(&mut stream, b'Z').await;
    assert_eq!(
        error_sqlstate(&frames).as_deref(),
        Some("22P02"),
        "a bad COPY value is invalid_text_representation"
    );

    // Zero rows landed.
    stream
        .write_all(&query("SELECT id FROM account"))
        .await
        .expect("write select");
    let rows = read_until(&mut stream, b'Z').await;
    assert_eq!(
        rows.iter().filter(|f| f.kind == b'D').count(),
        0,
        "a parse failure leaves zero rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simple_query_copy_client_fail_aborts() {
    let (_session, server) = spawn_raw_account();
    let addr = server.await;
    let mut stream = raw_connect(addr).await;

    stream
        .write_all(&query(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        ))
        .await
        .expect("write create");
    read_until(&mut stream, b'Z').await;

    stream
        .write_all(&query("COPY account FROM STDIN"))
        .await
        .expect("write copy");
    assert_eq!(read_frame(&mut stream).await.kind, b'G');

    // The client aborts with CopyFail after sending one row.
    stream
        .write_all(&frame(b'd', b"1\t100\n"))
        .await
        .expect("copy data");
    let mut fail = b"client changed its mind".to_vec();
    fail.push(0);
    stream
        .write_all(&frame(b'f', &fail))
        .await
        .expect("copy fail");
    stream.flush().await.expect("flush");

    let frames = read_until(&mut stream, b'Z').await;
    assert_eq!(
        error_sqlstate(&frames).as_deref(),
        Some("57014"),
        "CopyFail surfaces as query_canceled"
    );

    // Nothing was applied.
    stream
        .write_all(&query("SELECT id FROM account"))
        .await
        .expect("write select");
    let rows = read_until(&mut stream, b'Z').await;
    assert_eq!(rows.iter().filter(|f| f.kind == b'D').count(), 0);
}
