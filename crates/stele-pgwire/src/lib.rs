//! Postgres wire-protocol front end — startup handshake + simple-query loop.
//!
//! The pgwire front end is the **highest-leverage adoption decision** in Stele
//! ([ADR-0003](../../../docs/adr/0003-postgres-wire-protocol-early.md)): adopt the
//! protocol, inherit the entire driver / ORM / BI / admin ecosystem.
//!
//! ## v0.1 scope (this crate, today)
//!
//! * Listen on a TCP socket (default `0.0.0.0:5454`, [ADR-0017](../../../docs/adr/0017-default-network-port-5454.md)).
//! * Negotiate the startup phase: refuse SSL / GSS, parse `StartupMessage`,
//!   issue `AuthenticationOk` (no auth in v0.1), report a handful of
//!   `ParameterStatus` keys, send `BackendKeyData`, then `ReadyForQuery`.
//! * On the first `Query` message, return a polite `ErrorResponse` (SQLSTATE
//!   `0A000` — `feature_not_supported`) and another `ReadyForQuery`.
//! * Honor `Terminate` (`X`) by closing the connection.
//!
//! That is the thinnest end-to-end slice: `psql -h localhost -p 5454 -d stele`
//! connects, prints `stele=>`, runs a `SELECT 1`, sees a not-implemented error,
//! and `\q` works cleanly.
//!
//! ## Not in v0.1
//!
//! * Extended Query (Parse / Bind / Execute) — slated for **v0.2**
//!   ([docs/03-roadmap.md](../../../docs/03-roadmap.md)).
//! * `COPY` — v0.3.
//! * SCRAM-SHA-256 auth + TLS — v0.3.
//!
//! ## Architectural constraint
//!
//! The pgwire crate owns the async runtime boundary so the downstream
//! storage/txn core can stay runtime-agnostic
//! ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).

#![allow(clippy::missing_errors_doc)]

use std::io;
use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument, warn};

pub use stele_common::DEFAULT_PG_PORT;

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

// Special "startup-shape" request codes (8-byte messages, no message-type byte).
const SSL_REQUEST_CODE: i32 = 80_877_103;
const GSS_ENC_REQUEST_CODE: i32 = 80_877_104;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;

// Supported protocol versions. We accept 3.0 and 3.2; anything else gets refused.
const PROTOCOL_3_0: i32 = 196_608;
const PROTOCOL_3_2: i32 = 196_610;

// Message types we currently emit or consume on the post-startup stream.
const MSG_AUTHENTICATION: u8 = b'R';
const MSG_BACKEND_KEY_DATA: u8 = b'K';
const MSG_PARAMETER_STATUS: u8 = b'S';
const MSG_READY_FOR_QUERY: u8 = b'Z';
const MSG_ERROR_RESPONSE: u8 = b'E';
const MSG_QUERY: u8 = b'Q';
const MSG_TERMINATE: u8 = b'X';

// SQLSTATE codes we return.
const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";
const SQLSTATE_PROTOCOL_VIOLATION: &str = "08P01";

// DoS guard: cap how large a single frame we will allocate for. The Postgres
// protocol notionally allows up to ~1 GiB messages; in practice v0.1 traffic is
// startup params (≤ KiB) and short simple-query strings. A malicious client can
// advertise a multi-GiB length to OOM us, so we refuse frames over these bounds
// before allocating anything.
const MAX_STARTUP_PAYLOAD_SIZE: usize = 64 * 1024; // 64 KiB
const MAX_MESSAGE_PAYLOAD_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

// Reported server identity. We expose a real Postgres major so client-side
// version checks don't refuse us; the build component declares Stele.
const REPORTED_SERVER_VERSION: &str = "16.0 (Stele 0.1.0-dev)";

/// pgwire front-end entry point. Bind, accept, dispatch.
#[derive(Debug, Clone, Copy)]
pub struct Server {
    pub listen_addr: SocketAddr,
}

impl Server {
    #[must_use]
    pub const fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    /// Bind the listen socket and serve connections until cancelled by the caller.
    ///
    /// The caller owns shutdown — wire this into `tokio::select!` against a
    /// signal future for graceful drain.
    #[instrument(skip_all, fields(addr = %self.listen_addr))]
    pub async fn run(self) -> io::Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let bound = listener.local_addr()?;
        info!(addr = %bound, "stele-pgwire: listening");

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    // Transient accept errors should not kill the listener.
                    error!(error = %e, "accept failed");
                    continue;
                }
            };
            debug!(%peer, "accepted connection");
            // Disable Nagle — short Postgres messages don't benefit from coalescing.
            let _ = stream.set_nodelay(true);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, peer).await {
                    warn!(%peer, error = %e, "connection closed with error");
                }
            });
        }
    }
}

/// Errors that escape an individual connection handler. They are logged by the
/// listener loop and do not affect other connections.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("protocol violation: {0}")]
    Protocol(&'static str),

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(i32),

    #[error("client cancelled startup")]
    Cancelled,
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

#[instrument(skip(stream), fields(%peer))]
async fn handle_connection(mut stream: TcpStream, peer: SocketAddr) -> Result<(), WireError> {
    // --- 1. Startup phase --------------------------------------------------
    let startup = read_startup(&mut stream).await?;
    debug!(?startup.params, "startup complete");

    // --- 2. Send the OK bundle: AuthOk → ParameterStatus → BackendKeyData → ReadyForQuery
    write_authentication_ok(&mut stream).await?;
    for (k, v) in default_parameter_status() {
        write_parameter_status(&mut stream, k, v).await?;
    }
    // BackendKeyData lets clients later issue CancelRequest. We don't honor
    // cancellation in v0.1, but the message itself is part of a clean handshake.
    write_backend_key_data(&mut stream, 0, 0).await?;
    write_ready_for_query(&mut stream).await?;

    // --- 3. Message loop --------------------------------------------------
    loop {
        let Some(msg) = read_typed_message(&mut stream).await? else {
            debug!("peer closed connection");
            return Ok(());
        };
        match msg.kind {
            MSG_TERMINATE => {
                debug!("received Terminate");
                return Ok(());
            }
            MSG_QUERY => {
                // The first non-trivial Query goes here. v0.1 returns a
                // not-implemented error and stays in the loop, which is enough
                // for `psql` to print the error and keep its session alive.
                //
                // A Query payload MUST be a NUL-terminated cstring. If the
                // terminator is missing, surface that as a protocol violation
                // rather than silently treating it as an empty query — masking
                // it would let framing desync go unnoticed.
                let Some(q) = cstring_from(&msg.payload) else {
                    warn!("Query payload missing NUL terminator");
                    write_error_response(
                        &mut stream,
                        "ERROR",
                        SQLSTATE_PROTOCOL_VIOLATION,
                        "Query message missing NUL terminator",
                    )
                    .await?;
                    write_ready_for_query(&mut stream).await?;
                    continue;
                };
                info!(query = %q, "received simple query (not implemented in v0.1)");
                write_error_response(
                    &mut stream,
                    "ERROR",
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "the Stele engine has no executor yet — see docs/03-roadmap.md (v0.1)",
                )
                .await?;
                write_ready_for_query(&mut stream).await?;
            }
            other => {
                // Sync ('S'), Flush ('H'), and friends arrive once Extended Query
                // lands (v0.2). Until then, anything unexpected is a protocol
                // violation we surface politely rather than disconnecting silently.
                warn!(message_type = %char::from(other), "unsupported message type in v0.1");
                write_error_response(
                    &mut stream,
                    "ERROR",
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "message type not implemented in v0.1",
                )
                .await?;
                write_ready_for_query(&mut stream).await?;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Startup-phase parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StartupMessage {
    // Read but not yet branched on — we accept 3.0 and 3.2 identically in v0.1.
    // Stored so the field is available when GSS/SCRAM negotiation lands.
    #[allow(dead_code)]
    protocol_version: i32,
    params: Vec<(String, String)>,
}

/// Read the startup phase, transparently handling repeated SSL/GSS refusals.
async fn read_startup(stream: &mut TcpStream) -> Result<StartupMessage, WireError> {
    loop {
        let (length, code) = read_startup_header(stream).await?;
        match code {
            SSL_REQUEST_CODE => {
                // We refuse TLS in v0.1. The client will fall back to plaintext
                // and resend a StartupMessage.
                stream.write_all(b"N").await?;
                stream.flush().await?;
                continue;
            }
            GSS_ENC_REQUEST_CODE => {
                stream.write_all(b"N").await?;
                stream.flush().await?;
                continue;
            }
            CANCEL_REQUEST_CODE => {
                // CancelRequest is fire-and-forget — drain and close.
                let mut sink = vec![0u8; 8];
                stream.read_exact(&mut sink).await?;
                return Err(WireError::Cancelled);
            }
            PROTOCOL_3_0 | PROTOCOL_3_2 => {
                // Read the rest of the startup payload.
                let payload_len = usize::try_from(length)
                    .map_err(|_| WireError::Protocol("startup length negative"))?
                    .checked_sub(8)
                    .ok_or(WireError::Protocol("startup length too short"))?;
                if payload_len > MAX_STARTUP_PAYLOAD_SIZE {
                    return Err(WireError::Protocol("startup payload exceeds limit"));
                }
                let mut payload = vec![0u8; payload_len];
                stream.read_exact(&mut payload).await?;
                let params = parse_startup_params(&payload)?;
                return Ok(StartupMessage {
                    protocol_version: code,
                    params,
                });
            }
            v => return Err(WireError::UnsupportedVersion(v)),
        }
    }
}

/// Read the 8-byte startup-shape header (length + code).
async fn read_startup_header(stream: &mut TcpStream) -> Result<(i32, i32), WireError> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let length = i32::from_be_bytes(header[0..4].try_into().expect("4 bytes"));
    let code = i32::from_be_bytes(header[4..8].try_into().expect("4 bytes"));
    if length < 8 {
        return Err(WireError::Protocol("startup length < 8"));
    }
    Ok((length, code))
}

fn parse_startup_params(payload: &[u8]) -> Result<Vec<(String, String)>, WireError> {
    // Payload is a sequence of (cstring, cstring) pairs terminated by an empty cstring.
    let mut out = Vec::new();
    let mut cursor = payload;
    loop {
        let Some(key) = read_cstring(&mut cursor) else {
            return Err(WireError::Protocol("startup params truncated key"));
        };
        if key.is_empty() {
            return Ok(out);
        }
        let Some(value) = read_cstring(&mut cursor) else {
            return Err(WireError::Protocol("startup params truncated value"));
        };
        out.push((key, value));
    }
}

fn read_cstring(cursor: &mut &[u8]) -> Option<String> {
    let nul = cursor.iter().position(|&b| b == 0)?;
    let (head, rest) = cursor.split_at(nul);
    let s = String::from_utf8_lossy(head).into_owned();
    // Skip the NUL.
    *cursor = &rest[1..];
    Some(s)
}

// ---------------------------------------------------------------------------
// Post-startup framing
// ---------------------------------------------------------------------------

struct TypedMessage {
    kind: u8,
    payload: BytesMut,
}

async fn read_typed_message(stream: &mut TcpStream) -> Result<Option<TypedMessage>, WireError> {
    let mut header = [0u8; 5];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let kind = header[0];
    let length = i32::from_be_bytes(header[1..5].try_into().expect("4 bytes"));
    if length < 4 {
        return Err(WireError::Protocol("message length < 4"));
    }
    let payload_len =
        usize::try_from(length - 4).map_err(|_| WireError::Protocol("message length negative"))?;
    if payload_len > MAX_MESSAGE_PAYLOAD_SIZE {
        return Err(WireError::Protocol("message payload exceeds limit"));
    }
    let mut payload = BytesMut::with_capacity(payload_len);
    payload.resize(payload_len, 0);
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok(Some(TypedMessage { kind, payload }))
}

fn cstring_from(payload: &[u8]) -> Option<String> {
    let mut cursor = payload;
    read_cstring(&mut cursor)
}

// ---------------------------------------------------------------------------
// Outbound message builders
// ---------------------------------------------------------------------------

async fn write_authentication_ok(stream: &mut TcpStream) -> io::Result<()> {
    // 'R' + len(8) + Int32 0 (AuthenticationOk)
    let mut buf = BytesMut::with_capacity(9);
    buf.put_u8(MSG_AUTHENTICATION);
    buf.put_i32(8);
    buf.put_i32(0);
    stream.write_all(&buf).await
}

async fn write_parameter_status(stream: &mut TcpStream, key: &str, value: &str) -> io::Result<()> {
    let payload_len = key.len() + 1 + value.len() + 1;
    let mut buf = BytesMut::with_capacity(5 + payload_len);
    buf.put_u8(MSG_PARAMETER_STATUS);
    buf.put_i32(i32::try_from(4 + payload_len).unwrap_or(i32::MAX));
    buf.put_slice(key.as_bytes());
    buf.put_u8(0);
    buf.put_slice(value.as_bytes());
    buf.put_u8(0);
    stream.write_all(&buf).await
}

async fn write_backend_key_data(stream: &mut TcpStream, pid: i32, secret: i32) -> io::Result<()> {
    // 'K' + len(12) + Int32 pid + Int32 secret
    let mut buf = BytesMut::with_capacity(13);
    buf.put_u8(MSG_BACKEND_KEY_DATA);
    buf.put_i32(12);
    buf.put_i32(pid);
    buf.put_i32(secret);
    stream.write_all(&buf).await
}

async fn write_ready_for_query(stream: &mut TcpStream) -> io::Result<()> {
    // 'Z' + len(5) + 'I' (idle, not in a transaction)
    let mut buf = BytesMut::with_capacity(6);
    buf.put_u8(MSG_READY_FOR_QUERY);
    buf.put_i32(5);
    buf.put_u8(b'I');
    stream.write_all(&buf).await
}

async fn write_error_response(
    stream: &mut TcpStream,
    severity: &str,
    sqlstate: &str,
    message: &str,
) -> io::Result<()> {
    // 'E' + len + sequence of (Byte1 field-code, cstring) + terminating Byte1 0.
    // Fields: S=Severity, V=Severity (non-localized, 9.6+), C=SQLSTATE, M=Message.
    let mut payload = BytesMut::new();
    for (code, text) in [
        (b'S', severity),
        (b'V', severity),
        (b'C', sqlstate),
        (b'M', message),
    ] {
        payload.put_u8(code);
        payload.put_slice(text.as_bytes());
        payload.put_u8(0);
    }
    payload.put_u8(0); // terminator

    let len = i32::try_from(4 + payload.len()).unwrap_or(i32::MAX);
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.put_u8(MSG_ERROR_RESPONSE);
    frame.put_i32(len);
    frame.put_slice(&payload);
    stream.write_all(&frame).await
}

/// Parameters that real psql / pgx / pgwire-compatible drivers read at startup.
/// None of these encode Stele semantics; they exist to keep clients happy.
///
/// Returned as a concrete array so the future driving it stays `Send`
/// (an `impl IntoIterator` return type does not propagate `Send` bounds across
/// `.await` points, which `tokio::spawn` requires).
const fn default_parameter_status() -> [(&'static str, &'static str); 7] {
    [
        ("server_version", REPORTED_SERVER_VERSION),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_startup_params_to_terminator() {
        // key1\0value1\0\0
        let payload = b"user\0stele\0database\0stele\0\0";
        let parsed = parse_startup_params(payload).expect("parse ok");
        assert_eq!(
            parsed,
            vec![
                ("user".to_string(), "stele".to_string()),
                ("database".to_string(), "stele".to_string()),
            ]
        );
    }

    #[test]
    fn truncated_startup_params_is_an_error() {
        // Missing trailing \0 terminator on the empty key.
        let payload = b"user\0stele\0";
        assert!(parse_startup_params(payload).is_err());
    }

    #[test]
    fn read_cstring_consumes_through_nul() {
        let buf: &[u8] = b"hello\0world\0";
        let mut cursor: &[u8] = buf;
        assert_eq!(read_cstring(&mut cursor).as_deref(), Some("hello"));
        assert_eq!(read_cstring(&mut cursor).as_deref(), Some("world"));
        assert!(cursor.is_empty());
    }

    #[tokio::test]
    async fn handshake_completes_and_error_is_returned_on_query() {
        use tokio::io::AsyncWriteExt;
        // Bind to an ephemeral port and drive a synthetic client end-to-end.
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        let bound = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(stream, peer).await
        });

        let mut client = TcpStream::connect(bound).await.unwrap();

        // Send a 3.0 StartupMessage with user=stele\0database=stele\0\0.
        let body = b"user\0stele\0database\0stele\0\0";
        let length = 8 + body.len();
        let mut startup = BytesMut::with_capacity(length);
        startup.put_i32(i32::try_from(length).unwrap());
        startup.put_i32(PROTOCOL_3_0);
        startup.put_slice(body);
        client.write_all(&startup).await.unwrap();

        // Expect AuthenticationOk first.
        let mut hdr = [0u8; 5];
        client.read_exact(&mut hdr).await.unwrap();
        assert_eq!(hdr[0], MSG_AUTHENTICATION);
        let auth_len = i32::from_be_bytes(hdr[1..5].try_into().unwrap());
        // Authentication payload after the length is 4 bytes (Int32 0).
        let auth_payload_len = usize::try_from(auth_len - 4).unwrap();
        let mut auth_payload = vec![0u8; auth_payload_len];
        client.read_exact(&mut auth_payload).await.unwrap();
        assert_eq!(auth_payload, vec![0, 0, 0, 0]);

        // Drain ParameterStatus / BackendKeyData messages until ReadyForQuery.
        loop {
            let mut h = [0u8; 5];
            client.read_exact(&mut h).await.unwrap();
            let len = usize::try_from(i32::from_be_bytes(h[1..5].try_into().unwrap())).unwrap();
            let mut payload = vec![0u8; len - 4];
            if !payload.is_empty() {
                client.read_exact(&mut payload).await.unwrap();
            }
            if h[0] == MSG_READY_FOR_QUERY {
                assert_eq!(payload, b"I");
                break;
            }
        }

        // Send a simple query and expect an ErrorResponse + ReadyForQuery.
        let query = b"SELECT 1\0";
        let qlen = i32::try_from(4 + query.len()).unwrap();
        let mut q = BytesMut::with_capacity(5 + query.len());
        q.put_u8(MSG_QUERY);
        q.put_i32(qlen);
        q.put_slice(query);
        client.write_all(&q).await.unwrap();

        // First reply should be 'E'.
        let mut eh = [0u8; 5];
        client.read_exact(&mut eh).await.unwrap();
        assert_eq!(eh[0], MSG_ERROR_RESPONSE);
        let elen = usize::try_from(i32::from_be_bytes(eh[1..5].try_into().unwrap())).unwrap();
        let mut epayload = vec![0u8; elen - 4];
        client.read_exact(&mut epayload).await.unwrap();
        assert!(
            epayload
                .windows(5)
                .any(|w| w == SQLSTATE_FEATURE_NOT_SUPPORTED.as_bytes()),
            "SQLSTATE should be embedded in the error payload"
        );

        // Followed by ReadyForQuery 'Z'.
        let mut zh = [0u8; 5];
        client.read_exact(&mut zh).await.unwrap();
        assert_eq!(zh[0], MSG_READY_FOR_QUERY);

        // Close cleanly with Terminate.
        let term: [u8; 5] = [MSG_TERMINATE, 0, 0, 0, 4];
        client.write_all(&term).await.unwrap();
        drop(client);

        server.await.unwrap().unwrap();
    }

    // Compile-time sanity: the DoS guards must be non-zero, fit in i32 so the
    // length cast can't truncate, and startup ≤ message (startup is smaller).
    const _: () = {
        assert!(MAX_MESSAGE_PAYLOAD_SIZE > 0);
        assert!(MAX_MESSAGE_PAYLOAD_SIZE <= i32::MAX as usize);
        assert!(MAX_STARTUP_PAYLOAD_SIZE <= MAX_MESSAGE_PAYLOAD_SIZE);
    };

    #[tokio::test]
    async fn query_without_nul_terminator_returns_protocol_violation() {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(stream, peer).await
        });

        let mut client = TcpStream::connect(bound).await.unwrap();

        // StartupMessage.
        let body = b"user\0stele\0\0";
        let length = 8 + body.len();
        let mut startup = BytesMut::with_capacity(length);
        startup.put_i32(i32::try_from(length).unwrap());
        startup.put_i32(PROTOCOL_3_0);
        startup.put_slice(body);
        client.write_all(&startup).await.unwrap();

        // Drain handshake until ReadyForQuery.
        loop {
            let mut h = [0u8; 5];
            client.read_exact(&mut h).await.unwrap();
            let len = usize::try_from(i32::from_be_bytes(h[1..5].try_into().unwrap())).unwrap();
            let mut payload = vec![0u8; len - 4];
            if !payload.is_empty() {
                client.read_exact(&mut payload).await.unwrap();
            }
            if h[0] == MSG_READY_FOR_QUERY {
                break;
            }
        }

        // Send a Query missing the trailing NUL.
        let query = b"SELECT 1"; // no \0
        let qlen = i32::try_from(4 + query.len()).unwrap();
        let mut q = BytesMut::with_capacity(5 + query.len());
        q.put_u8(MSG_QUERY);
        q.put_i32(qlen);
        q.put_slice(query);
        client.write_all(&q).await.unwrap();

        // Expect ErrorResponse carrying SQLSTATE 08P01.
        let mut eh = [0u8; 5];
        client.read_exact(&mut eh).await.unwrap();
        assert_eq!(eh[0], MSG_ERROR_RESPONSE);
        let elen = usize::try_from(i32::from_be_bytes(eh[1..5].try_into().unwrap())).unwrap();
        let mut epayload = vec![0u8; elen - 4];
        client.read_exact(&mut epayload).await.unwrap();
        assert!(
            epayload
                .windows(5)
                .any(|w| w == SQLSTATE_PROTOCOL_VIOLATION.as_bytes()),
            "SQLSTATE 08P01 should be embedded in the error payload"
        );

        // Followed by ReadyForQuery.
        let mut zh = [0u8; 5];
        client.read_exact(&mut zh).await.unwrap();
        assert_eq!(zh[0], MSG_READY_FOR_QUERY);

        let term: [u8; 5] = [MSG_TERMINATE, 0, 0, 0, 4];
        client.write_all(&term).await.unwrap();
        drop(client);
        server.await.unwrap().unwrap();
    }
}
