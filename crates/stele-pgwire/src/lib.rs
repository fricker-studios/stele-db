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
//! * Run a **simple-query (`Q`) loop**: parse the SQL string with
//!   [`stele_sql::parse`], and reply with the result protocol — a constant
//!   `SELECT` (e.g. `SELECT 1`) returns `RowDescription` + `DataRow` +
//!   `CommandComplete`; an empty query returns `EmptyQueryResponse`; a parse
//!   failure returns `ErrorResponse` (SQLSTATE `42601`). A single
//!   `ReadyForQuery` closes out the whole message.
//! * Honor `Terminate` (`X`) by closing the connection.
//!
//! That is the thinnest *useful* end-to-end slice: `psql -h localhost -p 5454`
//! connects, prints `stele=>`, runs `SELECT 1`, sees the `1` come back, and
//! `\q` works cleanly.
//!
//! ## Not yet wired through the loop
//!
//! [STL-104] landed the **wire-format mechanism** — the outbound message
//! encoders and the [`CommandTag`] strings — proven with the constant-`SELECT`
//! path, and [STL-105] added the **per-type text encoder set**
//! (`INT4`/`INT8`/`TEXT`/`BOOL`/`TIMESTAMP`/`DATE`, in the `text_format` module)
//! that any `DataRow` value is rendered through. Routing statements that touch storage
//! rides on the pieces that build on these:
//!
//! * **`CREATE` / `DROP TABLE`** routing (parse → `bind_ddl` → catalog) is
//!   [STL-131], which also owns the server-session `Catalog` + commit clock.
//! * **`INSERT` / `UPDATE` / `DELETE`** and **table `SELECT`** routing need that
//!   same server-session engine; until it exists they return a polite
//!   `ErrorResponse` (SQLSTATE `0A000` — `feature_not_supported`).
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

mod text_format;

use std::io;
use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument, warn};

pub use stele_common::DEFAULT_PG_PORT;

use stele_common::types::{LogicalType, ScalarValue};

// The wire front end leans on stele-sql for parsing; `sqlparser` is re-exported
// from there, so matching on the AST adds no new dependency.
use stele_sql::Statement;
use stele_sql::sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement as SqlStatement, UnaryOperator, Value,
};

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
const MSG_ROW_DESCRIPTION: u8 = b'T';
const MSG_DATA_ROW: u8 = b'D';
const MSG_COMMAND_COMPLETE: u8 = b'C';
const MSG_EMPTY_QUERY_RESPONSE: u8 = b'I';

// SQLSTATE codes we return.
const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";
const SQLSTATE_PROTOCOL_VIOLATION: &str = "08P01";
const SQLSTATE_SYNTAX_ERROR: &str = "42601";

// Text format code for `RowDescription` fields (binary is 1; a v0.2 concern).
// The per-type OID and `typlen` advertised per field now come from the value's
// [`LogicalType`] (`pg_oid` / [`text_format::pg_typlen`]).
const FORMAT_TEXT: i16 = 0;

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
                // The whole simple-query message produces exactly one
                // ReadyForQuery, regardless of how many statements it carried or
                // whether one of them errored (Postgres aborts the batch on the
                // first error). `handle_simple_query` writes the per-statement
                // replies; the trailing ReadyForQuery is ours.
                handle_simple_query(&mut stream, &q).await?;
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
// Simple-query dispatch
// ---------------------------------------------------------------------------

/// The Postgres `CommandComplete` tag for a finished statement.
///
/// The tag string follows Postgres convention exactly, because clients parse it
/// (e.g. `tokio-postgres` reads the trailing count to report affected rows):
/// `SELECT n`, `INSERT 0 n` (the leading `0` is the long-dead OID field, still
/// emitted as `0`), `UPDATE n`, `DELETE n`, `CREATE TABLE`, `DROP TABLE`.
///
/// Only [`CommandTag::Select`] is wired to a live path in this ticket; the DML
/// and DDL variants exist so the routing tickets that build on the wire format
/// ([STL-131] and the DML-routing follow-up) have one tested place to render
/// their tags rather than re-deriving the convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandTag {
    /// `SELECT n` — `n` rows returned.
    Select(u64),
    /// `INSERT 0 n` — `n` rows inserted (the `0` is the legacy OID field).
    Insert(u64),
    /// `UPDATE n` — `n` rows updated.
    Update(u64),
    /// `DELETE n` — `n` rows deleted.
    Delete(u64),
    /// `CREATE TABLE`.
    CreateTable,
    /// `DROP TABLE`.
    DropTable,
}

impl CommandTag {
    /// Render the tag exactly as it goes into the `CommandComplete` payload.
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            Self::Select(n) => format!("SELECT {n}"),
            Self::Insert(n) => format!("INSERT 0 {n}"),
            Self::Update(n) => format!("UPDATE {n}"),
            Self::Delete(n) => format!("DELETE {n}"),
            Self::CreateTable => "CREATE TABLE".to_owned(),
            Self::DropTable => "DROP TABLE".to_owned(),
        }
    }
}

/// One column of a single-row simple-query result: its reported name, its
/// [`LogicalType`] (which drives the `RowDescription` OID + `typlen`), and the
/// row's cell value — `None` is SQL `NULL`, rendered as the length-`-1`
/// sentinel in the `DataRow`.
///
/// The type is carried rather than the OID so a column always renders its value
/// ([`text_format::encode_text`]) and describes itself ([`LogicalType::pg_oid`],
/// [`text_format::pg_typlen`]) from one source of truth.
struct ResultColumn {
    name: String,
    ty: LogicalType,
    value: Option<ScalarValue>,
}

/// Handle one simple-query (`Q`) message: parse the SQL, then reply for each
/// `;`-separated statement. Does **not** emit the trailing `ReadyForQuery` — the
/// caller owns that, so the whole message produces exactly one.
///
/// Dispatch in v0.1:
/// * empty / whitespace-only input → `EmptyQueryResponse`;
/// * a parse failure → `ErrorResponse` (SQLSTATE `42601`), no further statements;
/// * a constant `SELECT` (tableless, integer-literal projection) →
///   `RowDescription` + one `DataRow` + `CommandComplete`;
/// * anything that touches storage → `ErrorResponse` (SQLSTATE `0A000`), and the
///   batch stops there, mirroring Postgres aborting on the first error.
async fn handle_simple_query(stream: &mut TcpStream, sql: &str) -> Result<(), WireError> {
    if sql.trim().is_empty() {
        debug!("empty simple query");
        return write_empty_query_response(stream)
            .await
            .map_err(WireError::Io);
    }

    let statements = match stele_sql::parse(sql) {
        Ok(statements) => statements,
        Err(e) => {
            info!(query = %sql, error = %e, "simple query failed to parse");
            return write_error_response(stream, "ERROR", SQLSTATE_SYNTAX_ERROR, &e.to_string())
                .await
                .map_err(WireError::Io);
        }
    };

    // An all-comment / all-whitespace string parses to zero statements — that is
    // an empty query, not a row-less success.
    if statements.is_empty() {
        debug!("simple query carried no statements");
        return write_empty_query_response(stream)
            .await
            .map_err(WireError::Io);
    }

    for stmt in &statements {
        if let Some(columns) = constant_select(stmt) {
            write_row_description(stream, &columns).await?;
            write_data_row(stream, &columns).await?;
            write_command_complete(stream, &CommandTag::Select(1)).await?;
        } else {
            // Table reads, DML, and DDL all need the server-session engine that
            // does not exist yet; route them politely and abort the batch.
            info!(query = %sql, "statement not yet routable through the simple-query loop");
            write_error_response(
                stream,
                "ERROR",
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "v0.1 simple query runs constant SELECT only; table reads, DML, \
                 and DDL over the wire arrive in STL-131 and the DML-routing follow-up",
            )
            .await?;
            return Ok(());
        }
    }
    Ok(())
}

/// Recognize a constant, tableless `SELECT` whose projection is integer literals
/// — `SELECT 1`, `SELECT 1, 2 AS k`. Returns the columns to send back, or `None`
/// for anything that needs the binder/executor (a `FROM`, a `WHERE`, non-integer
/// or computed expressions). Integer-only keeps this honest: it is the canonical
/// connectivity smoke test, not a back-door scalar evaluator. The full v0.1
/// scalar set has text encoders ([`text_format`]); they reach the wire through
/// the table-read path the routing tickets add, not through this literal probe.
fn constant_select(stmt: &Statement) -> Option<Vec<ResultColumn>> {
    let SqlStatement::Query(query) = &stmt.body else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    // Tableless and unfiltered only — a `FROM` or `WHERE` belongs to the binder.
    if !select.from.is_empty() || select.selection.is_some() {
        return None;
    }
    if select.projection.is_empty() {
        return None;
    }

    let mut columns = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return None,
        };
        let parsed = integer_literal(expr)?;
        // A literal that fits `i32` is `int4`, matching Postgres's typing of a
        // small integer constant; anything wider escalates to `int8`.
        let value =
            i32::try_from(parsed).map_or_else(|_| ScalarValue::Int8(parsed), ScalarValue::Int4);
        columns.push(ResultColumn {
            // Postgres labels an unaliased expression column `?column?`.
            name: alias.unwrap_or_else(|| "?column?".to_owned()),
            ty: value.logical_type(),
            value: Some(value),
        });
    }
    Some(columns)
}

/// Fold an integer-literal expression to its value, or `None` for anything that
/// is not one. Handles a leading sign (`SELECT -1` parses as unary `-` over a
/// `Number`, not a negative literal), since an unsigned `SELECT 1` and a signed
/// `SELECT -1` are both basic connectivity smoke tests. Decimals and
/// out-of-`i64`-range values fall through to the binder/executor path.
fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(digits, _) => digits.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp { op, expr } => {
            let inner = integer_literal(expr)?;
            match op {
                UnaryOperator::Plus => Some(inner),
                UnaryOperator::Minus => inner.checked_neg(),
                _ => None,
            }
        }
        _ => None,
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

/// `EmptyQueryResponse` ('I') — the reply to a whitespace-only / comment-only
/// query. Carries no payload; it stands in for the `CommandComplete` a real
/// statement would have sent.
async fn write_empty_query_response(stream: &mut TcpStream) -> io::Result<()> {
    let buf: [u8; 5] = [MSG_EMPTY_QUERY_RESPONSE, 0, 0, 0, 4];
    stream.write_all(&buf).await
}

/// The Postgres column-count fields in `RowDescription` / `DataRow` are Int16,
/// so a result wider than `i16::MAX` columns cannot be described. Reject it
/// rather than clamp the count and emit a frame whose body and header disagree.
fn column_count(columns: &[ResultColumn]) -> Result<i16, WireError> {
    i16::try_from(columns.len())
        .map_err(|_| WireError::Protocol("result has more than 32767 columns"))
}

/// Build the `RowDescription` ('T') payload — one field descriptor per column.
///
/// Per field: name (cstring), table OID (Int32), column attr number (Int16),
/// type OID (Int32), type size (Int16), type modifier (Int32), format code
/// (Int16). We have no real relation behind these columns, so table OID and
/// attr number are `0`, and the type modifier is `-1` (none). The OID and size
/// come from each column's [`LogicalType`].
fn row_description_payload(columns: &[ResultColumn]) -> Result<BytesMut, WireError> {
    let count = column_count(columns)?;
    let mut payload = BytesMut::new();
    payload.put_i16(count);
    for col in columns {
        payload.put_slice(col.name.as_bytes());
        payload.put_u8(0);
        payload.put_i32(0); // table OID — not a stored relation
        payload.put_i16(0); // column attribute number
        // `pg_oid` is a `u32`; the wire field is an Int32. The v0.1 OIDs are all
        // small well-known values (≤ 1114), so this conversion never fails.
        payload.put_i32(i32::try_from(col.ty.pg_oid()).expect("v0.1 pg_oid fits in i32"));
        payload.put_i16(text_format::pg_typlen(col.ty));
        payload.put_i32(-1); // type modifier
        payload.put_i16(FORMAT_TEXT);
    }
    Ok(payload)
}

/// Build the `DataRow` ('D') payload — one cell per column, in text format. A
/// `None` cell is SQL `NULL`, encoded as the length-`-1` sentinel with no value
/// bytes; a present value is rendered through [`text_format::encode_text`].
fn data_row_payload(columns: &[ResultColumn]) -> Result<BytesMut, WireError> {
    let count = column_count(columns)?;
    let mut payload = BytesMut::new();
    payload.put_i16(count);
    for col in columns {
        match &col.value {
            None => payload.put_i32(-1),
            Some(value) => {
                let text = text_format::encode_text(value);
                let bytes = text.as_bytes();
                payload.put_i32(i32::try_from(bytes.len()).unwrap_or(i32::MAX));
                payload.put_slice(bytes);
            }
        }
    }
    Ok(payload)
}

/// `RowDescription` ('T').
async fn write_row_description(
    stream: &mut TcpStream,
    columns: &[ResultColumn],
) -> Result<(), WireError> {
    let payload = row_description_payload(columns)?;
    write_framed(stream, MSG_ROW_DESCRIPTION, &payload).await?;
    Ok(())
}

/// `DataRow` ('D').
async fn write_data_row(stream: &mut TcpStream, columns: &[ResultColumn]) -> Result<(), WireError> {
    let payload = data_row_payload(columns)?;
    write_framed(stream, MSG_DATA_ROW, &payload).await?;
    Ok(())
}

/// `CommandComplete` ('C') — the statement's [`CommandTag`] as a cstring.
async fn write_command_complete(stream: &mut TcpStream, tag: &CommandTag) -> io::Result<()> {
    let rendered = tag.render();
    let mut payload = BytesMut::with_capacity(rendered.len() + 1);
    payload.put_slice(rendered.as_bytes());
    payload.put_u8(0);
    write_framed(stream, MSG_COMMAND_COMPLETE, &payload).await
}

/// Frame a payload as a typed message: 1-byte kind + Int32 length (inclusive of
/// the length field) + payload.
async fn write_framed(stream: &mut TcpStream, kind: u8, payload: &[u8]) -> io::Result<()> {
    let len = i32::try_from(4 + payload.len()).unwrap_or(i32::MAX);
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.put_u8(kind);
    frame.put_i32(len);
    frame.put_slice(payload);
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

    /// Read one typed backend message: `(kind, payload)` with the 5-byte header
    /// stripped. Panics on EOF — a test that loses the connection mid-protocol
    /// should fail loudly.
    async fn read_message(client: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        client
            .read_exact(&mut header)
            .await
            .expect("message header");
        let len = usize::try_from(i32::from_be_bytes(header[1..5].try_into().unwrap())).unwrap();
        let mut payload = vec![0u8; len - 4];
        if !payload.is_empty() {
            client
                .read_exact(&mut payload)
                .await
                .expect("message payload");
        }
        (header[0], payload)
    }

    /// Send a simple-query (`Q`) message carrying `sql` (NUL-terminated).
    async fn send_query(client: &mut TcpStream, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut q = BytesMut::with_capacity(5 + body.len());
        q.put_u8(MSG_QUERY);
        q.put_i32(i32::try_from(4 + body.len()).unwrap());
        q.put_slice(&body);
        client.write_all(&q).await.unwrap();
    }

    /// Stand up `handle_connection` on an ephemeral port, complete the startup
    /// handshake from the client side, and return `(server_join, client)` poised
    /// at `ReadyForQuery`.
    async fn connect_past_handshake() -> (tokio::task::JoinHandle<Result<(), WireError>>, TcpStream)
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(stream, peer).await
        });
        let mut client = TcpStream::connect(bound).await.unwrap();

        let body = b"user\0stele\0database\0stele\0\0";
        let length = 8 + body.len();
        let mut startup = BytesMut::with_capacity(length);
        startup.put_i32(i32::try_from(length).unwrap());
        startup.put_i32(PROTOCOL_3_0);
        startup.put_slice(body);
        client.write_all(&startup).await.unwrap();

        loop {
            let (kind, _) = read_message(&mut client).await;
            if kind == MSG_READY_FOR_QUERY {
                break;
            }
        }
        (server, client)
    }

    /// Send `Terminate`, drop the client, and join the server handler.
    async fn terminate(
        server: tokio::task::JoinHandle<Result<(), WireError>>,
        mut client: TcpStream,
    ) {
        let term: [u8; 5] = [MSG_TERMINATE, 0, 0, 0, 4];
        client.write_all(&term).await.unwrap();
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[test]
    fn command_tags_render_per_postgres_convention() {
        assert_eq!(CommandTag::Select(0).render(), "SELECT 0");
        assert_eq!(CommandTag::Select(42).render(), "SELECT 42");
        assert_eq!(CommandTag::Insert(3).render(), "INSERT 0 3");
        assert_eq!(CommandTag::Update(1).render(), "UPDATE 1");
        assert_eq!(CommandTag::Delete(0).render(), "DELETE 0");
        assert_eq!(CommandTag::CreateTable.render(), "CREATE TABLE");
        assert_eq!(CommandTag::DropTable.render(), "DROP TABLE");
    }

    #[test]
    fn constant_select_recognizes_integer_literals_only() {
        let one = stele_sql::parse("SELECT 1").unwrap();
        let cols = constant_select(&one[0]).expect("SELECT 1 is constant");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "?column?");
        assert_eq!(cols[0].ty, LogicalType::Int4);
        assert_eq!(cols[0].value, Some(ScalarValue::Int4(1)));

        // Alias is honored; a wide literal escalates to INT8.
        let aliased = stele_sql::parse("SELECT 5000000000 AS big").unwrap();
        let cols = constant_select(&aliased[0]).expect("constant");
        assert_eq!(cols[0].name, "big");
        assert_eq!(cols[0].ty, LogicalType::Int8);
        assert_eq!(cols[0].value, Some(ScalarValue::Int8(5_000_000_000)));

        // A leading sign is folded — `-1` parses as unary minus over a Number.
        let neg = stele_sql::parse("SELECT -1").unwrap();
        let cols = constant_select(&neg[0]).expect("SELECT -1 is constant");
        assert_eq!(cols[0].ty, LogicalType::Int4);
        assert_eq!(cols[0].value, Some(ScalarValue::Int4(-1)));
        let pos = stele_sql::parse("SELECT +5 AS five").unwrap();
        assert_eq!(
            constant_select(&pos[0]).unwrap()[0].value,
            Some(ScalarValue::Int4(5))
        );

        // A table read, a filter, or a non-integer expression is not constant.
        for sql in [
            "SELECT * FROM t",
            "SELECT 1 WHERE 1=1",
            "SELECT 'x'",
            "SELECT 1.5",
        ] {
            let stmt = stele_sql::parse(sql).unwrap();
            assert!(
                constant_select(&stmt[0]).is_none(),
                "{sql} must defer to the binder"
            );
        }
    }

    /// Parse a `DataRow` payload into its per-column cells: `None` is the NULL
    /// sentinel (length `-1`), `Some(bytes)` is a present text-format value.
    fn parse_data_row(payload: &[u8]) -> Vec<Option<Vec<u8>>> {
        let count = i16::from_be_bytes(payload[0..2].try_into().unwrap());
        let mut cells = Vec::new();
        let mut pos = 2;
        for _ in 0..count {
            let len = i32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;
            match usize::try_from(len) {
                Err(_) => cells.push(None), // length -1 → NULL
                Ok(n) => {
                    let end = pos + n;
                    cells.push(Some(payload[pos..end].to_vec()));
                    pos = end;
                }
            }
        }
        assert_eq!(pos, payload.len(), "DataRow payload fully consumed");
        cells
    }

    #[test]
    fn data_row_encodes_null_as_negative_one_length() {
        // A NULL cell is the length `-1` sentinel with no value bytes; present
        // cells carry their text-format bytes. (STL-105 Definition of Done.)
        let columns = vec![
            ResultColumn {
                name: "a".into(),
                ty: LogicalType::Int4,
                value: Some(ScalarValue::Int4(7)),
            },
            ResultColumn {
                name: "b".into(),
                ty: LogicalType::Text,
                value: None,
            },
            ResultColumn {
                name: "c".into(),
                ty: LogicalType::Text,
                value: Some(ScalarValue::Text("hi".into())),
            },
        ];
        let payload = data_row_payload(&columns).expect("payload");
        assert_eq!(
            parse_data_row(&payload),
            vec![Some(b"7".to_vec()), None, Some(b"hi".to_vec())]
        );
    }

    #[test]
    fn data_row_renders_every_scalar_type_in_text_format() {
        // Drive each v0.1 type through the real DataRow builder so the wire path
        // — not just the encoder unit — proves the Postgres text rendering.
        let columns = vec![
            ResultColumn {
                name: "i4".into(),
                ty: LogicalType::Int4,
                value: Some(ScalarValue::Int4(-42)),
            },
            ResultColumn {
                name: "i8".into(),
                ty: LogicalType::Int8,
                value: Some(ScalarValue::Int8(5_000_000_000)),
            },
            ResultColumn {
                name: "t".into(),
                ty: LogicalType::Text,
                value: Some(ScalarValue::Text("hé🦀".into())),
            },
            ResultColumn {
                name: "b".into(),
                ty: LogicalType::Bool,
                value: Some(ScalarValue::Bool(false)),
            },
            ResultColumn {
                name: "ts".into(),
                ty: LogicalType::Timestamp,
                value: Some(ScalarValue::Timestamp(1_700_000_000_000_000)),
            },
            ResultColumn {
                name: "d".into(),
                ty: LogicalType::Date,
                value: Some(ScalarValue::Date(19_675)),
            },
        ];
        let cells = parse_data_row(&data_row_payload(&columns).expect("payload"));
        let rendered: Vec<String> = cells
            .into_iter()
            .map(|c| String::from_utf8(c.expect("non-null")).unwrap())
            .collect();
        assert_eq!(
            rendered,
            vec![
                "-42",
                "5000000000",
                "hé🦀",
                "f",
                "2023-11-14 22:13:20",
                "2023-11-14",
            ]
        );
    }

    #[test]
    fn row_description_advertises_pg_oid_and_typlen_per_type() {
        // Each field's dataTypeOID + typlen come from the column's LogicalType.
        let columns: Vec<ResultColumn> = LogicalType::ALL
            .iter()
            .map(|&ty| ResultColumn {
                name: ty.pg_type_name().to_owned(),
                ty,
                value: None,
            })
            .collect();
        let payload = row_description_payload(&columns).expect("payload");
        let count = i16::from_be_bytes(payload[0..2].try_into().unwrap());
        assert_eq!(usize::try_from(count).unwrap(), LogicalType::ALL.len());

        let mut pos = 2;
        for &ty in &LogicalType::ALL {
            // name cstring
            let name_end = payload[pos..].iter().position(|&b| b == 0).unwrap() + pos;
            assert_eq!(&payload[pos..name_end], ty.pg_type_name().as_bytes());
            pos = name_end + 1;
            pos += 4 + 2; // table OID + attr number
            let oid = i32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let typlen = i16::from_be_bytes(payload[pos..pos + 2].try_into().unwrap());
            pos += 2;
            let typmod = i32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let format = i16::from_be_bytes(payload[pos..pos + 2].try_into().unwrap());
            pos += 2;
            assert_eq!(oid, i32::try_from(ty.pg_oid()).unwrap(), "{ty} OID");
            assert_eq!(typlen, text_format::pg_typlen(ty), "{ty} typlen");
            assert_eq!(typmod, -1, "{ty} typmod is none");
            assert_eq!(format, FORMAT_TEXT, "{ty} is text format");
        }
        assert_eq!(pos, payload.len());
    }

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
    async fn handshake_completes_and_select_one_round_trips() {
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

        // Send `SELECT 1` and expect the full result protocol:
        // RowDescription, one DataRow, CommandComplete, then ReadyForQuery.
        send_query(&mut client, "SELECT 1").await;

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_ROW_DESCRIPTION, "first reply is RowDescription");
        // Int16 field count == 1, then the field name `?column?`.
        assert_eq!(i16::from_be_bytes(payload[0..2].try_into().unwrap()), 1);
        assert!(
            payload.windows(8).any(|w| w == b"?column?"),
            "unaliased column is named ?column?"
        );

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_DATA_ROW, "second reply is DataRow");
        // Int16 column count == 1, Int32 value length == 1, value byte '1'.
        assert_eq!(i16::from_be_bytes(payload[0..2].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(payload[2..6].try_into().unwrap()), 1);
        assert_eq!(&payload[6..], b"1");

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_COMMAND_COMPLETE, "third reply is CommandComplete");
        assert_eq!(payload, b"SELECT 1\0", "tag is `SELECT 1`");

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_READY_FOR_QUERY);
        assert_eq!(payload, b"I");

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

    #[tokio::test]
    async fn ssl_then_gss_requests_are_refused_then_handshake_proceeds() {
        use tokio::io::AsyncWriteExt;
        // The startup phase must tolerate an SSLRequest and a GSSEncRequest ahead
        // of the real StartupMessage, answering each negotiation probe with a lone
        // 'N' (TLS/GSS unsupported in v0.1) and then completing the handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(stream, peer).await
        });

        let mut client = TcpStream::connect(bound).await.unwrap();

        // Both negotiation probes share the 8-byte startup shape: Int32 length(8)
        // + Int32 code. Each must be refused with a single 'N'.
        for code in [SSL_REQUEST_CODE, GSS_ENC_REQUEST_CODE] {
            let mut probe = BytesMut::with_capacity(8);
            probe.put_i32(8);
            probe.put_i32(code);
            client.write_all(&probe).await.unwrap();
            let mut b = [0u8; 1];
            client.read_exact(&mut b).await.unwrap();
            assert_eq!(
                b[0], b'N',
                "negotiation code {code} must be refused with 'N'"
            );
        }

        // Now the real StartupMessage — the handshake should proceed to ReadyForQuery.
        let body = b"user\0stele\0database\0stele\0\0";
        let length = 8 + body.len();
        let mut startup = BytesMut::with_capacity(length);
        startup.put_i32(i32::try_from(length).unwrap());
        startup.put_i32(PROTOCOL_3_0);
        startup.put_slice(body);
        client.write_all(&startup).await.unwrap();

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

        let term: [u8; 5] = [MSG_TERMINATE, 0, 0, 0, 4];
        client.write_all(&term).await.unwrap();
        drop(client);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_boots_and_refuses_ssl_with_n() {
        use tokio::io::AsyncWriteExt;
        // DoD bullet 2, encoded as a regression test: booting the public listener
        // and probing it with an SSLRequest yields the 'N' refusal byte. Reserve a
        // free port via a throwaway bind, then hand it to the real `Server::run`.
        let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = reserved.local_addr().unwrap();
        drop(reserved);

        let handle = tokio::spawn(Server::new(addr).run());

        // `Server::run` binds asynchronously; connect-retry until it is listening
        // (up to ~2s, generous for a loaded CI runner). Bail out loudly if the
        // server task itself exits early — e.g. a bind failure — instead of
        // spinning the whole budget against a socket that will never come up and
        // then panicking with a misleading "timed out" message.
        let mut maybe_client = None;
        for _ in 0..200 {
            assert!(
                !handle.is_finished(),
                "server task exited before accepting a connection (bind error on {addr}?)"
            );
            if let Ok(c) = TcpStream::connect(addr).await {
                maybe_client = Some(c);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let mut client =
            maybe_client.expect("server should bind and accept within the 2s retry budget");

        let mut ssl = BytesMut::with_capacity(8);
        ssl.put_i32(8);
        ssl.put_i32(SSL_REQUEST_CODE);
        client.write_all(&ssl).await.unwrap();

        let mut b = [0u8; 1];
        client.read_exact(&mut b).await.unwrap();
        assert_eq!(b[0], b'N', "a TCP probe must see the 'N' SSL-refusal byte");

        drop(client);
        handle.abort();
    }

    #[tokio::test]
    async fn select_with_alias_round_trips_named_column() {
        let (server, mut client) = connect_past_handshake().await;
        send_query(&mut client, "SELECT 7 AS answer").await;

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_ROW_DESCRIPTION);
        assert!(payload.windows(6).any(|w| w == b"answer"));

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_DATA_ROW);
        assert_eq!(&payload[6..], b"7");

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_COMMAND_COMPLETE);
        assert_eq!(payload, b"SELECT 1\0");

        let (kind, _) = read_message(&mut client).await;
        assert_eq!(kind, MSG_READY_FOR_QUERY);
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn empty_query_yields_empty_query_response() {
        let (server, mut client) = connect_past_handshake().await;
        // Whitespace / a bare semicolon carry no statement.
        send_query(&mut client, "   ").await;

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_EMPTY_QUERY_RESPONSE);
        assert!(payload.is_empty());

        let (kind, _) = read_message(&mut client).await;
        assert_eq!(kind, MSG_READY_FOR_QUERY, "still exactly one ReadyForQuery");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn syntax_error_reports_sqlstate_42601() {
        let (server, mut client) = connect_past_handshake().await;
        send_query(&mut client, "SELECT FROM WHERE").await;

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_ERROR_RESPONSE);
        assert!(
            payload
                .windows(5)
                .any(|w| w == SQLSTATE_SYNTAX_ERROR.as_bytes()),
            "a parse failure carries SQLSTATE 42601"
        );

        let (kind, _) = read_message(&mut client).await;
        assert_eq!(kind, MSG_READY_FOR_QUERY);
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn table_read_is_politely_deferred() {
        let (server, mut client) = connect_past_handshake().await;
        // A real table read needs the server-session engine (STL-131); until
        // then it is feature_not_supported, never a crash or a wrong answer.
        send_query(&mut client, "SELECT balance FROM account").await;

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_ERROR_RESPONSE);
        assert!(
            payload
                .windows(5)
                .any(|w| w == SQLSTATE_FEATURE_NOT_SUPPORTED.as_bytes()),
            "an un-routable statement carries SQLSTATE 0A000"
        );

        let (kind, _) = read_message(&mut client).await;
        assert_eq!(kind, MSG_READY_FOR_QUERY);
        terminate(server, client).await;
    }
}
