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
//! ## Statements that touch storage
//!
//! [STL-104] landed the **wire-format mechanism** — the outbound message
//! encoders and the [`CommandTag`] strings — proven with the constant-`SELECT`
//! path, and [STL-105] added the **per-type text encoder set**
//! (`INT4`/`INT8`/`TEXT`/`BOOL`/`TIMESTAMP`/`DATE`, in the `text_format` module)
//! that any `DataRow` value is rendered through. Routing statements that touch
//! storage builds on those, against the server-session engine:
//!
//! * **`CREATE` / `DROP TABLE`** routing (parse → `bind_ddl` → catalog) is
//!   [STL-131], which also owns the server-session `Catalog` + commit clock.
//! * **table `SELECT`** and **`INSERT` / `UPDATE` / `DELETE`** routing is
//!   [STL-147]: the loop hands each parsed statement to
//!   [`SessionEngine::execute`], which binds and runs it, then encodes the
//!   resulting rows ([`SelectResult`]) or affected-row count ([`DmlSummary`])
//!   back onto the wire. v0.1 maps the table's primary-key column to the business
//!   key and its single value column to the opaque payload; a general
//!   multi-column row codec is a v0.2 concern.
//!
//! ## Extended Query (v0.2)
//!
//! [STL-182] adds the **extended-query** state machine — `Parse` / `Bind` /
//! `Describe` / `Execute` / `Sync` / `Close` — and a per-connection
//! prepared-statement + portal cache, so a driver can prepare a parameterized
//! statement once and execute it with bound values. The protocol decoding and
//! the `$1 … $n` → literal substitution live in the `extended` module; this one
//! owns the async handlers, the per-connection caches, and the result streaming.
//! Only **text-format** parameters and results are handled here; binary encoders
//! ride in with [STL-77] \[G23\].
//!
//! ## Not yet
//!
//! * `COPY` — v0.3.
//! * SCRAM-SHA-256 auth + TLS — v0.3.
//!
//! [STL-182]: https://allegromusic.atlassian.net/browse/STL-182
//! [STL-77]: https://allegromusic.atlassian.net/browse/STL-77
//!
//! ## Architectural constraint
//!
//! The pgwire crate owns the async runtime boundary so the downstream
//! storage/txn core can stay runtime-agnostic
//! ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).

#![allow(clippy::missing_errors_doc)]

mod extended;
mod pg_catalog;
mod text_format;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument, warn};

pub use stele_common::DEFAULT_PG_PORT;

use stele_catalog::CatalogError;
use stele_common::time::Clock;
use stele_common::types::{DecodeError, LogicalType, ScalarValue};
use stele_engine::{
    DmlSummary, EngineError, SelectResult, SessionEngine, SessionTransaction, StatementOutcome,
    TableDescription,
};
use stele_storage::backend::Disk;

// The wire front end leans on stele-sql for parsing; `sqlparser` is re-exported
// from there, so matching on the AST adds no new dependency.
use stele_sql::select::SelectError;
use stele_sql::sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement as SqlStatement, UnaryOperator, Value,
};
use stele_sql::{BindError, DmlError, Statement, bind_ddl};

use pg_catalog::Introspection;

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

// Extended-query message types (STL-182). The message-type byte is
// direction-specific, so several reuse a letter the backend emits for something
// else on the *outbound* stream (inbound `D` is Describe vs outbound DataRow,
// inbound `E` is Execute vs outbound ErrorResponse, inbound `C` is Close vs
// outbound CommandComplete, inbound `S` is Sync vs outbound ParameterStatus).
const MSG_PARSE: u8 = b'P';
const MSG_BIND: u8 = b'B';
const MSG_DESCRIBE: u8 = b'D';
const MSG_EXECUTE: u8 = b'E';
const MSG_SYNC: u8 = b'S';
const MSG_CLOSE: u8 = b'C';
const MSG_FLUSH: u8 = b'H';
// …and the replies unique to the extended protocol.
const MSG_PARSE_COMPLETE: u8 = b'1';
const MSG_BIND_COMPLETE: u8 = b'2';
const MSG_CLOSE_COMPLETE: u8 = b'3';
const MSG_PARAMETER_DESCRIPTION: u8 = b't';
const MSG_NO_DATA: u8 = b'n';
const MSG_PORTAL_SUSPENDED: u8 = b's';

// SQLSTATE codes we return.
const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";
const SQLSTATE_PROTOCOL_VIOLATION: &str = "08P01";
const SQLSTATE_SYNTAX_ERROR: &str = "42601";
// DDL-routing SQLSTATEs (STL-131): the standard Postgres codes for the catalog
// failures a `CREATE`/`DROP TABLE` can hit, so a stock client classifies them
// the way it would against Postgres.
const SQLSTATE_DUPLICATE_TABLE: &str = "42P07";
const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";
const SQLSTATE_DUPLICATE_COLUMN: &str = "42701";
const SQLSTATE_UNDEFINED_COLUMN: &str = "42703";
const SQLSTATE_INVALID_TABLE_DEFINITION: &str = "42P16";
const SQLSTATE_INTERNAL_ERROR: &str = "XX000";
// A literal in a `WHERE` / `VALUES` that does not match its column's type — the
// code Postgres returns for an unparsable value (STL-147 DML routing).
const SQLSTATE_INVALID_TEXT_REPRESENTATION: &str = "22P02";
// A statement issued while the transaction is already aborted — Postgres ignores
// commands until the block ends (`COMMIT`/`ROLLBACK`), STL-174.
const SQLSTATE_IN_FAILED_TRANSACTION: &str = "25P02";
// Extended-query lifecycle errors (STL-182): preparing a name that already
// exists, and naming a prepared statement / portal that does not.
const SQLSTATE_DUPLICATE_PSTATEMENT: &str = "42P05";
const SQLSTATE_INVALID_PSTATEMENT_NAME: &str = "26000";
const SQLSTATE_INVALID_CURSOR_NAME: &str = "34000";

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

/// What the simple-query loop needs from the session engine.
///
/// The engine's `<Clock, Disk>` generics are erased here so the connection
/// handler and [`Server`] carry one concrete handle type rather than threading
/// generics through every layer.
///
/// This trait, behind a [`SharedSession`], is where the ticket's "decide where
/// the `Catalog` + commit clock live" lands ([STL-131]): **one** [`SessionEngine`]
/// shared across every connection, so a `CREATE TABLE` on any connection is
/// visible to the next statement — including a later `\d` — instead of
/// per-connection state a reconnect would silently lose. (Durable catalog state
/// across a *restart* still needs catalog persistence, a separate concern.)
///
/// [STL-131]: https://allegromusic.atlassian.net/browse/STL-131
pub trait SessionHandle: Send {
    /// Run one parsed statement against the session — see
    /// [`SessionEngine::execute`].
    fn execute(&mut self, stmt: &Statement) -> Result<StatementOutcome, EngineError>;

    /// The live tables and their columns at the current snapshot, for the
    /// `pg_catalog` `\d` shim — see [`SessionEngine::describe_live_tables`].
    fn describe_live_tables(&self) -> Vec<TableDescription>;

    /// Bind and buffer a DML statement into the connection's open transaction,
    /// returning its affected-row summary or `Ok(None)` if it is not DML — see
    /// [`SessionEngine::stage_dml`] ([STL-174]).
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    fn stage_dml(
        &self,
        stmt: &Statement,
        txn: &mut SessionTransaction,
    ) -> Result<Option<DmlSummary>, EngineError>;

    /// Apply a transaction's buffered writes as a unit — see
    /// [`SessionEngine::commit`] ([STL-174]).
    ///
    /// [STL-174]: https://allegromusic.atlassian.net/browse/STL-174
    fn commit(&mut self, txn: SessionTransaction) -> Result<(), EngineError>;
}

impl<C, D> SessionHandle for SessionEngine<C, D>
where
    C: Clock + Clone + Send + 'static,
    D: Disk + Clone + Send + 'static,
{
    fn execute(&mut self, stmt: &Statement) -> Result<StatementOutcome, EngineError> {
        Self::execute(self, stmt)
    }

    fn describe_live_tables(&self) -> Vec<TableDescription> {
        Self::describe_live_tables(self)
    }

    fn stage_dml(
        &self,
        stmt: &Statement,
        txn: &mut SessionTransaction,
    ) -> Result<Option<DmlSummary>, EngineError> {
        Self::stage_dml(self, stmt, txn)
    }

    fn commit(&mut self, txn: SessionTransaction) -> Result<(), EngineError> {
        Self::commit(self, txn)
    }
}

/// A session handle shared across connections: one engine behind a mutex, with
/// the `Arc` cloned into each connection task.
///
/// The guard is taken only for the **synchronous** `execute` / `describe_live_tables`
/// call and dropped before any `await`, so a lock is never held across wire I/O
/// (and one slow client cannot stall another mid-statement). The runtime-agnostic
/// core stays unaware of `tokio`: this is a plain [`std::sync::Mutex`], locked and
/// released entirely within synchronous helpers.
pub type SharedSession = Arc<Mutex<dyn SessionHandle>>;

/// pgwire front-end entry point. Bind, accept, dispatch.
#[derive(Clone)]
pub struct Server {
    listen_addr: SocketAddr,
    session: SharedSession,
}

impl Server {
    #[must_use]
    pub fn new(listen_addr: SocketAddr, session: SharedSession) -> Self {
        Self {
            listen_addr,
            session,
        }
    }

    /// Bind the listen socket now, returning a [`BoundServer`] whose
    /// [`local_addr`](BoundServer::local_addr) reports the address actually
    /// bound before the accept loop runs.
    ///
    /// Binding up front is what lets a caller pass port `0` (an ephemeral port)
    /// and then learn the real port with **no race**: the listener is already
    /// holding the socket when the address is read, so nothing can grab the port
    /// in between (the old "reserve a `:0` listener, drop it, re-bind on its
    /// address" dance had exactly that window). The returned listener already
    /// accepts connections into its backlog, so a client may connect before
    /// [`serve`](BoundServer::serve) is even awaited — no connect-retry needed.
    pub async fn bind(self) -> io::Result<BoundServer> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(BoundServer {
            listener,
            local_addr,
            session: self.session,
        })
    }

    /// Bind the listen socket and serve connections until cancelled by the caller.
    ///
    /// A convenience over [`bind`](Server::bind) + [`serve`](BoundServer::serve)
    /// for callers that don't need the bound address. The caller owns shutdown —
    /// wire this into `tokio::select!` against a signal future for graceful drain.
    pub async fn run(self) -> io::Result<()> {
        self.bind().await?.serve().await
    }
}

/// A [`Server`] that has already bound its listen socket.
///
/// Its [`local_addr`] is readable before [`serve`] starts the accept loop, so a
/// caller that bound an ephemeral (`:0`) port learns the real address with no
/// reserve-drop race.
///
/// [`local_addr`]: BoundServer::local_addr
/// [`serve`]: BoundServer::serve
pub struct BoundServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    session: SharedSession,
}

impl BoundServer {
    /// The address the listen socket is actually bound to — the resolved port
    /// when the caller asked for an ephemeral `:0`.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and dispatch connections until cancelled by the caller.
    ///
    /// The caller owns shutdown — wire this into `tokio::select!` against a
    /// signal future for graceful drain.
    #[instrument(skip_all, fields(addr = %self.local_addr))]
    pub async fn serve(self) -> io::Result<()> {
        info!(addr = %self.local_addr, "stele-pgwire: listening");

        loop {
            let (stream, peer) = match self.listener.accept().await {
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
            let session = Arc::clone(&self.session);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, peer, session).await {
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

/// The per-connection transaction state — the `ReadyForQuery` status indicator
/// made real ([STL-174]).
///
/// A connection is either auto-committing each statement ([`Idle`](Self::Idle)),
/// inside an explicit `BEGIN` block buffering writes ([`Active`](Self::Active)),
/// or inside one that hit an error and is now aborted ([`Failed`](Self::Failed),
/// rejecting everything until `COMMIT`/`ROLLBACK` ends the block). The state lives
/// on the connection task's stack, not the shared engine, so each connection's
/// transaction is independent. The variant maps directly to the Postgres
/// `ReadyForQuery` byte — `I` / `T` / `E` — that closes out every message.
enum ConnTxn {
    /// No transaction open; each statement auto-commits. `ReadyForQuery` = `I`.
    Idle,
    /// Inside `BEGIN`, buffering writes until `COMMIT`. `ReadyForQuery` = `T`.
    Active(SessionTransaction),
    /// Inside a transaction that errored; statements are refused until the block
    /// ends. `ReadyForQuery` = `E`.
    Failed,
}

impl ConnTxn {
    /// The `ReadyForQuery` transaction-status byte for this state.
    const fn status_byte(&self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::Active(_) => b'T',
            Self::Failed => b'E',
        }
    }

    /// Move an open (non-failed) transaction into the aborted state, discarding
    /// its buffered writes; a no-op when idle (an auto-commit statement error does
    /// not open a transaction). Called when a statement errors so the trailing
    /// `ReadyForQuery` reports `E`.
    fn mark_failed(&mut self) {
        if matches!(self, Self::Active(_)) {
            *self = Self::Failed;
        }
    }
}

/// The transaction-control statements the front end handles itself rather than
/// routing to the engine ([STL-174]). `ROLLBACK TO <savepoint>` is intentionally
/// not one of these (savepoints are [STL-176]) — it falls through to the engine,
/// which rejects it as unsupported.
enum TxnControl {
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    /// `COMMIT` / `END`.
    Commit,
    /// `ROLLBACK` / `ABORT` (without a savepoint target).
    Rollback,
}

/// Classify a statement as transaction control, or `None` for anything the engine
/// routes. A `ROLLBACK TO <savepoint>` is deliberately *not* matched here.
const fn txn_control(stmt: &Statement) -> Option<TxnControl> {
    match &stmt.body {
        SqlStatement::StartTransaction { .. } => Some(TxnControl::Begin),
        SqlStatement::Commit { .. } => Some(TxnControl::Commit),
        SqlStatement::Rollback {
            savepoint: None, ..
        } => Some(TxnControl::Rollback),
        _ => None,
    }
}

#[instrument(skip(stream, session), fields(%peer))]
async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    session: SharedSession,
) -> Result<(), WireError> {
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
    write_ready_for_query(&mut stream, ConnTxn::Idle.status_byte()).await?;

    // The connection's transaction state — auto-commit (`Idle`) until a `BEGIN`
    // opens an explicit block. Persists across messages for the life of the
    // connection (STL-174).
    let mut txn = ConnTxn::Idle;

    // --- 3. Message loop --------------------------------------------------
    // The extended-query caches (prepared statements + portals) and the
    // "skip until Sync" error latch live for the whole connection.
    let mut state = ConnState::default();
    loop {
        let Some(msg) = read_typed_message(&mut stream).await? else {
            debug!("peer closed connection");
            return Ok(());
        };

        // After an error inside an extended-query batch, Postgres discards every
        // message until the next Sync, which re-opens the connection with a fresh
        // ReadyForQuery. Terminate still ends the connection immediately.
        if state.skip_until_sync && msg.kind != MSG_SYNC && msg.kind != MSG_TERMINATE {
            debug!(message_type = %char::from(msg.kind), "skipping until Sync");
            continue;
        }

        match msg.kind {
            MSG_TERMINATE => {
                debug!("received Terminate");
                return Ok(());
            }
            MSG_SYNC => {
                // Sync closes an extended-query batch: clear the error latch and
                // report ready, carrying the current transaction-status byte
                // (STL-174). A bare Sync outside any batch is a harmless no-op that
                // still owes a ReadyForQuery.
                state.skip_until_sync = false;
                write_ready_for_query(&mut stream, txn.status_byte()).await?;
            }
            MSG_PARSE => handle_parse(&mut stream, &mut state, &msg.payload).await?,
            MSG_BIND => handle_bind(&mut stream, &mut state, &msg.payload).await?,
            MSG_DESCRIBE => {
                handle_describe(&mut stream, &mut state, &msg.payload, &session, &mut txn).await?;
            }
            MSG_EXECUTE => {
                handle_execute(&mut stream, &mut state, &msg.payload, &session, &mut txn).await?;
            }
            MSG_CLOSE => handle_close(&mut stream, &mut state, &msg.payload).await?,
            MSG_FLUSH => {
                // We write replies straight to the socket with no backend buffer,
                // so Flush only needs to push them past the OS send buffer.
                stream.flush().await?;
            }
            MSG_QUERY => {
                // A simple Query runs outside the extended protocol and destroys
                // the unnamed prepared statement and portal (Postgres §53.2.3).
                state.prepared.remove("");
                state.portals.remove("");
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
                    write_ready_for_query(&mut stream, txn.status_byte()).await?;
                    continue;
                };
                // The whole simple-query message produces exactly one
                // ReadyForQuery, regardless of how many statements it carried or
                // whether one of them errored (Postgres aborts the batch on the
                // first error). `handle_simple_query` writes the per-statement
                // replies and advances the transaction state; the trailing
                // ReadyForQuery — carrying the resulting status byte — is ours.
                handle_simple_query(&mut stream, &q, &session, &mut txn).await?;
                write_ready_for_query(&mut stream, txn.status_byte()).await?;
            }
            other => {
                // Simple + extended query and the lifecycle messages are all
                // handled above. What is left (`COPY` data/done, function call)
                // is outside the current surface; an unknown type mid-stream is a
                // framing hazard, so we fail the connection rather than guess at
                // where the next message starts.
                warn!(message_type = %char::from(other), "unsupported message type");
                return Err(WireError::Protocol("unsupported message type"));
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
/// [`CommandTag::Select`] is on the live path (constant `SELECT`, the
/// `pg_catalog` shim, and table reads). The `INSERT`/`UPDATE`/`DELETE` variants
/// render committed DML, mapped from the engine's [`DmlSummary`] (STL-147). DDL
/// routing instead writes the engine's own tag
/// ([`DdlOutcome::command_tag`](stele_sql::DdlOutcome::command_tag)) to the
/// `CommandComplete` directly, so [`CommandTag::CreateTable`] /
/// [`CommandTag::DropTable`] stay as the one tested place that pins the
/// convention.
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
#[derive(Clone)]
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
/// * a `pg_catalog` `\d` introspection query → `RowDescription` + `DataRow`s from
///   the live catalog (the minimal shim, STL-131);
/// * `CREATE` / `DROP TABLE` → routed through the session engine; success is a
///   `CommandComplete` with the engine's tag, a failure an `ErrorResponse` that
///   aborts the batch (STL-131);
/// * a constant `SELECT` (tableless, integer-literal projection) →
///   `RowDescription` + one `DataRow` + `CommandComplete`;
/// * a table `SELECT` → `RowDescription` + a `DataRow` per row + `CommandComplete`
///   (`SELECT n`), the rows resolved at the read snapshot (and any `AS OF`) by the
///   session engine (STL-147);
/// * an `INSERT` / `UPDATE` / `DELETE` → `CommandComplete` (`INSERT 0 n` /
///   `UPDATE n` / `DELETE n`) once the write commits — or, inside a `BEGIN`
///   block, once it is *buffered* (STL-147, STL-174);
/// * `BEGIN` / `COMMIT` / `ROLLBACK` → handled by the front end against the
///   connection's `txn` state, not the engine's routing (STL-174);
/// * any non-control statement failing → `ErrorResponse` with the Postgres
///   SQLSTATE for the failure; the batch stops there, mirroring Postgres aborting
///   on the first error, and an open transaction moves to the aborted state.
///
/// Transactional DDL is **not** modelled: a `CREATE`/`DROP TABLE` inside a `BEGIN`
/// block auto-commits at once and is not undone by a later `ROLLBACK` (only DML is
/// buffered). Drivers' transaction blocks are DML in practice; rolling back DDL is
/// a later concern.
async fn handle_simple_query(
    stream: &mut TcpStream,
    sql: &str,
    session: &SharedSession,
    txn: &mut ConnTxn,
) -> Result<(), WireError> {
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
            // A parse failure inside an open transaction aborts it, like any other
            // statement error.
            txn.mark_failed();
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
        // (0) Transaction control — `BEGIN` / `COMMIT` / `ROLLBACK` manage the
        // connection's `txn` state and never reach the engine's routing. Checked
        // first so `COMMIT`/`ROLLBACK` can still end a transaction that is in the
        // aborted state.
        if let Some(ctl) = txn_control(stmt) {
            let proceed = match ctl {
                TxnControl::Begin => {
                    run_begin(stream, txn).await?;
                    true
                }
                // A failed COMMIT writes an ErrorResponse and returns `false`, so
                // the batch aborts here like any other statement error — nothing
                // more may follow an error on the wire.
                TxnControl::Commit => run_commit(stream, session, txn).await?,
                TxnControl::Rollback => {
                    run_rollback(stream, txn).await?;
                    true
                }
            };
            if !proceed {
                return Ok(());
            }
            continue;
        }

        // (0b) Inside an aborted transaction every other statement is refused
        // until the block ends — Postgres's `in_failed_sql_transaction`. The batch
        // stops here, leaving the transaction aborted for the trailing
        // ReadyForQuery to report (`E`).
        if matches!(txn, ConnTxn::Failed) {
            write_error_response(
                stream,
                "ERROR",
                SQLSTATE_IN_FAILED_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            )
            .await?;
            return Ok(());
        }

        // (1) `pg_catalog` introspection (`psql \d`) — answered from the live
        // catalog through the minimal shim, ahead of every other route since
        // these are `SELECT`s that would otherwise fall to the deferral arm.
        if let Some(intro) = pg_catalog::classify(stmt) {
            let (header, rows) = introspection_reply(&intro, session);
            write_row_description(stream, &header).await?;
            for row in &rows {
                write_data_row(stream, row).await?;
            }
            let n = u64::try_from(rows.len()).unwrap_or(u64::MAX);
            write_command_complete(stream, &CommandTag::Select(n)).await?;
            continue;
        }

        // (2) DDL — `CREATE` / `DROP TABLE` routed through the session engine
        // (STL-131). `bind_ddl` is the classifier: `Ok` means it is DDL, a
        // non-`NotDdl` error means it is malformed DDL we surface as such.
        match bind_ddl(stmt) {
            Ok(_) => match run_ddl(session, stmt) {
                Ok(tag) => write_command_complete_tag(stream, tag).await?,
                Err(e) => {
                    info!(query = %sql, error = %e, "DDL failed");
                    write_error_response(stream, "ERROR", sqlstate_for(&e), &e.to_string()).await?;
                    txn.mark_failed();
                    return Ok(());
                }
            },
            Err(BindError::NotDdl) => {
                // (3) A constant `SELECT` (STL-104) is answered without touching
                // storage. Everything else — a table read or `INSERT`/`UPDATE`/
                // `DELETE` — routes through the session engine (STL-147), buffered
                // into `txn` instead of committed when a transaction is open.
                if let Some(columns) = constant_select(stmt) {
                    write_row_description(stream, &columns).await?;
                    write_data_row(stream, &columns).await?;
                    write_command_complete(stream, &CommandTag::Select(1)).await?;
                } else if !run_statement(stream, stmt, session, txn).await? {
                    // The statement errored; the reply and SQLSTATE are already on
                    // the wire and the batch aborts (Postgres stops on the first
                    // error), mirroring the DDL arm above. An open transaction is
                    // now aborted.
                    txn.mark_failed();
                    return Ok(());
                }
            }
            Err(e) => {
                // Malformed DDL — surface the bind error and abort the batch.
                info!(query = %sql, error = %e, "DDL bind failed");
                write_error_response(stream, "ERROR", SQLSTATE_SYNTAX_ERROR, &e.to_string())
                    .await?;
                txn.mark_failed();
                return Ok(());
            }
        }
    }
    Ok(())
}

/// `BEGIN` / `START TRANSACTION` — open an explicit transaction block ([STL-174]).
///
/// From idle this enters [`ConnTxn::Active`] with an empty write buffer. A `BEGIN`
/// already inside a transaction (active or aborted) leaves the state untouched —
/// Postgres warns but stays in the block — and still reports `BEGIN`.
async fn run_begin(stream: &mut TcpStream, txn: &mut ConnTxn) -> Result<(), WireError> {
    if matches!(txn, ConnTxn::Idle) {
        *txn = ConnTxn::Active(SessionTransaction::new());
    }
    write_command_complete_tag(stream, "BEGIN")
        .await
        .map_err(WireError::Io)
}

/// `COMMIT` / `END` — apply the transaction's buffered writes as a unit
/// ([STL-174]).
///
/// From [`ConnTxn::Active`] the buffer is applied through
/// [`SessionHandle::commit`] (one shared transaction id) and the state returns to
/// idle. A `COMMIT` of an aborted transaction rolls it back and reports `ROLLBACK`,
/// matching Postgres; a `COMMIT` with no open transaction is a warning-only no-op
/// that still reports `COMMIT`.
///
/// Returns whether the batch may continue: `Ok(true)` on success, `Ok(false)` when
/// the commit replay failed — an `ErrorResponse` was written, so the caller must
/// stop processing this message (nothing may follow an error on the wire).
async fn run_commit(
    stream: &mut TcpStream,
    session: &SharedSession,
    txn: &mut ConnTxn,
) -> Result<bool, WireError> {
    match std::mem::replace(txn, ConnTxn::Idle) {
        ConnTxn::Active(buffered) => match commit_txn(session, buffered) {
            Ok(()) => write_command_complete_tag(stream, "COMMIT").await?,
            Err(e) => {
                // The commit replay failed partway. The transaction is over (state
                // already reset to idle), but — unlike a clean ROLLBACK — any writes
                // applied before the failure are NOT undone: `SessionEngine::commit`
                // replays through the per-write WAL path with no rollback (the
                // crash-atomic group-commit follow-up). Surface the error and abort
                // the batch; the trailing ReadyForQuery reports idle.
                info!(error = %e, "COMMIT failed");
                write_error_response(stream, "ERROR", sqlstate_for_query(&e), &e.to_string())
                    .await?;
                return Ok(false);
            }
        },
        // Postgres rolls a failed transaction back on COMMIT and reports ROLLBACK.
        ConnTxn::Failed => write_command_complete_tag(stream, "ROLLBACK").await?,
        // No open transaction — a warning-only no-op that still reports COMMIT.
        ConnTxn::Idle => write_command_complete_tag(stream, "COMMIT").await?,
    }
    Ok(true)
}

/// `ROLLBACK` / `ABORT` — discard the transaction's buffered writes ([STL-174]).
///
/// Returns to idle from any state, dropping an [`ConnTxn::Active`] buffer (nothing
/// it staged ever reaches storage) or clearing a [`ConnTxn::Failed`] block. A
/// `ROLLBACK` with no open transaction still reports `ROLLBACK`.
async fn run_rollback(stream: &mut TcpStream, txn: &mut ConnTxn) -> Result<(), WireError> {
    *txn = ConnTxn::Idle;
    write_command_complete_tag(stream, "ROLLBACK")
        .await
        .map_err(WireError::Io)
}

/// Apply a transaction's buffered writes under the session lock, taking and
/// releasing the mutex entirely within this synchronous call (never held across
/// the caller's `await` writes). A poisoned mutex is recovered, as in [`run_ddl`].
fn commit_txn(session: &SharedSession, txn: SessionTransaction) -> Result<(), EngineError> {
    session
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .commit(txn)
}

/// Route a bound-DDL statement through the shared session engine and return its
/// `CommandComplete` tag (`CREATE TABLE` / `DROP TABLE`).
///
/// The mutex guard is taken and released entirely here — a synchronous call —
/// so it is never held across the caller's `await` writes. A poisoned mutex is
/// recovered rather than propagated, so one panicking connection cannot wedge
/// the whole server.
fn run_ddl(session: &SharedSession, stmt: &Statement) -> Result<&'static str, EngineError> {
    let mut engine = session.lock().unwrap_or_else(PoisonError::into_inner);
    match engine.execute(stmt)? {
        StatementOutcome::Ddl { tag } => Ok(tag),
        // `bind_ddl` already classified this as DDL, so `execute` routes it to the
        // DDL arm; any other outcome would be an internal contract break.
        StatementOutcome::Rows(_) | StatementOutcome::Dml(_) => Err(EngineError::Unsupported(
            "DDL statement unexpectedly produced a non-DDL outcome",
        )),
    }
}

/// Route a table `SELECT` or an `INSERT` / `UPDATE` / `DELETE` through the session
/// engine and write its reply. Returns `Ok(true)` on success and `Ok(false)` when
/// the statement errored (the `ErrorResponse` is already written and the caller
/// aborts the batch), reserving `Err` for an I/O failure on the socket.
///
/// All result-row cells are decoded up front, so a decode failure surfaces as a
/// single `ErrorResponse` rather than a `RowDescription` followed by a torn row
/// stream.
async fn run_statement(
    stream: &mut TcpStream,
    stmt: &Statement,
    session: &SharedSession,
    txn: &mut ConnTxn,
) -> Result<bool, WireError> {
    match run_query(session, stmt, txn) {
        Ok(StatementOutcome::Rows(result)) => match decode_result_rows(&result) {
            Ok(data_rows) => {
                write_row_description(stream, &result_header(&result)).await?;
                for row in &data_rows {
                    write_data_row(stream, row).await?;
                }
                let n = u64::try_from(data_rows.len()).unwrap_or(u64::MAX);
                write_command_complete(stream, &CommandTag::Select(n)).await?;
                Ok(true)
            }
            Err(e) => {
                error!(error = %e, "result cell failed to decode");
                write_error_response(stream, "ERROR", SQLSTATE_INTERNAL_ERROR, &e.to_string())
                    .await?;
                Ok(false)
            }
        },
        Ok(StatementOutcome::Dml(summary)) => {
            write_command_complete(stream, &command_tag_for(summary)).await?;
            Ok(true)
        }
        // DDL is handled by the caller's `bind_ddl` arm and never reaches here, but
        // honor its tag rather than mislabel it if the routing ever shifts.
        Ok(StatementOutcome::Ddl { tag }) => {
            write_command_complete_tag(stream, tag).await?;
            Ok(true)
        }
        Err(e) => {
            info!(error = %e, "statement failed");
            write_error_response(stream, "ERROR", sqlstate_for_query(&e), &e.to_string()).await?;
            Ok(false)
        }
    }
}

/// Run one statement against the shared session engine, taking and releasing the
/// mutex entirely within this synchronous call (never held across the caller's
/// `await` writes). A poisoned mutex is recovered, as in [`run_ddl`].
///
/// When a transaction is open ([`ConnTxn::Active`]) an `INSERT`/`UPDATE`/`DELETE`
/// is **buffered** into it rather than committed — its `CommandComplete` reports
/// the would-be affected count and the write applies later at `COMMIT` ([STL-174]).
/// A `SELECT` (or anything not DML) still runs immediately against the committed
/// state; the buffer is write-only until commit.
fn run_query(
    session: &SharedSession,
    stmt: &Statement,
    txn: &mut ConnTxn,
) -> Result<StatementOutcome, EngineError> {
    let mut engine = session.lock().unwrap_or_else(PoisonError::into_inner);
    if let ConnTxn::Active(buffered) = txn {
        if let Some(summary) = engine.stage_dml(stmt, buffered)? {
            return Ok(StatementOutcome::Dml(summary));
        }
    }
    engine.execute(stmt)
}

/// The `RowDescription` field descriptors for a [`SelectResult`] — one per
/// projected column, named and typed from the engine's projection.
fn result_header(result: &SelectResult) -> Vec<ResultColumn> {
    result
        .columns
        .iter()
        .map(|(name, ty)| field(name, *ty))
        .collect()
}

/// Decode every cell of a [`SelectResult`] into `DataRow`-ready [`ResultColumn`]s.
///
/// A present cell carries the value's canonical encoding ([`ScalarValue::encode`]);
/// decoding it under the column's [`LogicalType`] is the exact inverse, so a value
/// written through the DML path round-trips. A `None` cell is a SQL `NULL`
/// ([STL-154]): it carries no bytes and renders as the length-`-1` `DataRow`
/// sentinel (via [`null_cell`]). A decode failure on a present cell means the
/// stored bytes do not match the column type (corruption, or an opaque payload
/// staged outside the wire path) and is surfaced rather than rendered wrong.
fn decode_result_rows(result: &SelectResult) -> Result<Vec<Vec<ResultColumn>>, DecodeError> {
    result
        .rows
        .iter()
        .map(|raw| {
            result
                .columns
                .iter()
                .zip(raw)
                .map(|((_, ty), cell_bytes)| match cell_bytes {
                    None => Ok(null_cell(*ty)),
                    Some(bytes) => Ok(cell(ScalarValue::decode(*ty, bytes)?)),
                })
                .collect()
        })
        .collect()
}

/// The `CommandComplete` tag for a committed DML operation.
const fn command_tag_for(summary: DmlSummary) -> CommandTag {
    match summary {
        DmlSummary::Insert(n) => CommandTag::Insert(n),
        DmlSummary::Update(n) => CommandTag::Update(n),
        DmlSummary::Delete(n) => CommandTag::Delete(n),
    }
}

/// The standard Postgres SQLSTATE for a `SELECT` / DML routing failure, so a stock
/// client classifies it the way it would against Postgres.
///
/// DDL-specific catalog failures reuse [`sqlstate_for`]; the cases unique to the
/// read / write path are an unknown table (`42P01`), an unknown column (`42703`),
/// and a bad literal in a `WHERE` or `VALUES` (`22P02`, invalid text
/// representation). Shapes outside the v0.1 surface map to `0A000`
/// (`feature_not_supported`).
const fn sqlstate_for_query(err: &EngineError) -> &'static str {
    match err {
        EngineError::Bind(_) => SQLSTATE_SYNTAX_ERROR,
        EngineError::Select(SelectError::UnknownTable(_) | SelectError::TableNotLive { .. })
        | EngineError::Dml(DmlError::UnknownTable(_) | DmlError::TableNotLive { .. })
        | EngineError::UnknownTable(_) => SQLSTATE_UNDEFINED_TABLE,
        // A named column the schema does not contain — Postgres's undefined_column,
        // distinct from undefined_table, so a client can branch on it.
        EngineError::Select(SelectError::UnknownColumn { .. })
        | EngineError::Dml(DmlError::UnknownColumn { .. }) => SQLSTATE_UNDEFINED_COLUMN,
        EngineError::Dml(DmlError::BadLiteral { .. } | DmlError::TypeMismatch { .. }) => {
            SQLSTATE_INVALID_TEXT_REPRESENTATION
        }
        EngineError::Select(_) | EngineError::Dml(_) | EngineError::Unsupported(_) => {
            SQLSTATE_FEATURE_NOT_SUPPORTED
        }
        // Catalog/storage/scan errors are unexpected on the read/write path but
        // map cleanly rather than panicking if the contract ever shifts. A row
        // codec failure is corrupt stored bytes — an internal error, like storage.
        EngineError::Catalog(_) | EngineError::ValidTimePolicyChange { .. } => sqlstate_for(err),
        EngineError::Storage(_) | EngineError::Scan(_) | EngineError::RowCodec(_) => {
            SQLSTATE_INTERNAL_ERROR
        }
        // A schema that changed under a bound write — concurrent-ish schema drift.
        EngineError::SchemaChanged { .. } => SQLSTATE_INTERNAL_ERROR,
    }
}

/// Build the `(RowDescription header, DataRows)` reply for a recognized
/// `pg_catalog` introspection query, reading the live tables under the session
/// lock and releasing it before any wire write.
///
/// Shapes are fixed and documented (see [`pg_catalog`]): a relation lookup
/// returns `(oid, nspname, relname)` for the named table (zero rows if absent);
/// an attribute lookup returns `(attname, atttypname, attnum)` per column of the
/// table whose synthetic `oid` matches (zero rows if none).
fn introspection_reply(
    intro: &Introspection,
    session: &SharedSession,
) -> (Vec<ResultColumn>, Vec<Vec<ResultColumn>>) {
    let live = session
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .describe_live_tables();

    match intro {
        Introspection::Relation { name } => {
            let header = vec![
                field("oid", LogicalType::Int4),
                field("nspname", LogicalType::Text),
                field("relname", LogicalType::Text),
            ];
            let rows = live
                .iter()
                .find(|t| &t.name == name)
                .map(|t| {
                    vec![vec![
                        cell(ScalarValue::Int4(oid_as_i32(&t.name))),
                        cell(ScalarValue::Text("public".to_owned())),
                        cell(ScalarValue::Text(t.name.clone())),
                    ]]
                })
                .unwrap_or_default();
            (header, rows)
        }
        Introspection::Attributes { oid } => {
            let header = vec![
                field("attname", LogicalType::Text),
                field("atttypname", LogicalType::Text),
                field("attnum", LogicalType::Int4),
            ];
            let rows = live
                .iter()
                .find(|t| pg_catalog::oid_for(&t.name) == *oid)
                .map(|t| {
                    t.columns
                        .iter()
                        .enumerate()
                        .map(|(i, (col_name, col_ty))| {
                            let attnum = i32::try_from(i + 1).unwrap_or(i32::MAX);
                            vec![
                                cell(ScalarValue::Text(col_name.clone())),
                                cell(ScalarValue::Text(col_ty.pg_type_name().to_owned())),
                                cell(ScalarValue::Int4(attnum)),
                            ]
                        })
                        .collect()
                })
                .unwrap_or_default();
            (header, rows)
        }
    }
}

/// A `RowDescription` field: a named column of a given type, with no cell value
/// (the value is carried per-row in the `DataRow`s).
fn field(name: &str, ty: LogicalType) -> ResultColumn {
    ResultColumn {
        name: name.to_owned(),
        ty,
        value: None,
    }
}

/// A `DataRow` cell carrying a present value; the name is unused by the
/// `DataRow` encoder, so it is left empty.
const fn cell(value: ScalarValue) -> ResultColumn {
    ResultColumn {
        name: String::new(),
        ty: value.logical_type(),
        value: Some(value),
    }
}

/// A `DataRow` cell for a SQL `NULL` of the given column type ([STL-154]): no
/// value, so [`data_row_payload`] emits the length-`-1` sentinel. The type is
/// carried for parity with [`cell`] even though a NULL cell renders no bytes.
const fn null_cell(ty: LogicalType) -> ResultColumn {
    ResultColumn {
        name: String::new(),
        ty,
        value: None,
    }
}

/// A table's synthetic `oid` as a clean `int4` (the hash is masked into the
/// non-negative `i32` range, so the conversion never loses information).
fn oid_as_i32(name: &str) -> i32 {
    i32::try_from(pg_catalog::oid_for(name)).unwrap_or(i32::MAX)
}

/// The standard Postgres SQLSTATE for a DDL-routing failure, so a stock client
/// classifies it the way it would against Postgres.
const fn sqlstate_for(err: &EngineError) -> &'static str {
    match err {
        EngineError::Bind(_) => SQLSTATE_SYNTAX_ERROR,
        EngineError::Catalog(CatalogError::TableAlreadyExists(_)) => SQLSTATE_DUPLICATE_TABLE,
        EngineError::Catalog(CatalogError::UnknownTable(_)) => SQLSTATE_UNDEFINED_TABLE,
        EngineError::Catalog(CatalogError::DuplicateColumn(_)) => SQLSTATE_DUPLICATE_COLUMN,
        EngineError::Catalog(_) | EngineError::ValidTimePolicyChange { .. } => {
            SQLSTATE_INVALID_TABLE_DEFINITION
        }
        // Storage/scan/select/unknown-table/unsupported can't arise from a DDL
        // route, but map them rather than panic if the contract ever shifts.
        _ => SQLSTATE_INTERNAL_ERROR,
    }
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
// Extended-query state machine (STL-182)
// ---------------------------------------------------------------------------

/// A parsed-but-unbound prepared statement in the per-connection cache.
struct Prepared {
    /// The single parsed statement, or `None` for an empty query string —
    /// Postgres lets you prepare an empty statement; its Execute later replies
    /// `EmptyQueryResponse`.
    stmt: Option<Statement>,
    /// Declared parameter type OIDs in `$1 … $n` order (`0` = unspecified).
    param_oids: Vec<u32>,
}

/// A bound portal: a prepared statement with its parameters substituted, plus
/// the lazily-cached result of running it.
struct PortalEntry {
    /// The bound statement, or `None` for an empty-query portal.
    stmt: Option<Statement>,
    /// Populated on the first Describe / Execute. Caching means a Describe that
    /// runs the read and a later Execute that drains it agree on one result, and
    /// a row-capped Execute can resume from where the previous one stopped.
    executed: Option<Executed>,
}

/// The outcome of running a portal's statement, cached for streaming.
enum Executed {
    /// A row-returning statement: the `RowDescription` header, every decoded
    /// row, and how many have been streamed so far (for resumable Execute).
    Rows {
        header: Vec<ResultColumn>,
        rows: Vec<Vec<ResultColumn>>,
        sent: usize,
    },
    /// A statement that completes with only a `CommandComplete` tag (DML / DDL).
    Completed { tag: String },
}

/// Per-connection extended-query state: the prepared-statement and portal caches
/// plus the "discard until Sync" error latch.
#[derive(Default)]
struct ConnState {
    prepared: HashMap<String, Prepared>,
    portals: HashMap<String, PortalEntry>,
    /// Set when an extended-query message errors; the loop then drops every
    /// message until the next Sync, per the Postgres protocol.
    skip_until_sync: bool,
}

/// A failed execution, carried with the Postgres SQLSTATE to report.
struct ExecError {
    sqlstate: &'static str,
    message: String,
}

/// Whether a statement produces a result-set (and therefore a `RowDescription`)
/// rather than just a `CommandComplete` — the `pg_catalog` shim, a constant
/// `SELECT`, and any `Query` return rows; DDL and DML do not.
fn returns_rows(stmt: &Statement) -> bool {
    pg_catalog::classify(stmt).is_some()
        || constant_select(stmt).is_some()
        || matches!(stmt.body, SqlStatement::Query(_))
}

/// Parse a prepared-statement query string into its single [`Statement`], or
/// `None` for an empty / comment-only string. An error string is returned for a
/// parse failure or a multi-statement string (a prepared statement holds exactly
/// one command).
fn parse_single(query: &str) -> Result<Option<Statement>, String> {
    if query.trim().is_empty() {
        return Ok(None);
    }
    match stele_sql::parse(query) {
        Ok(mut stmts) => match stmts.len() {
            0 => Ok(None),
            1 => Ok(Some(stmts.remove(0))),
            _ => Err("cannot insert multiple commands into a prepared statement".to_owned()),
        },
        Err(e) => Err(e.to_string()),
    }
}

/// Run a portal's statement once and cache the outcome, mirroring the
/// simple-query dispatch (`pg_catalog` → DDL → constant `SELECT` → engine). A
/// row-returning read is side-effect-free, so running it at Describe time and
/// reusing it at Execute is safe.
///
/// `txn` is threaded into [`run_query`] so an extended-query `INSERT`/`UPDATE`/
/// `DELETE` buffers into an open `BEGIN` block exactly like a simple-query one
/// (STL-174); a `SELECT` runs immediately against committed state regardless.
fn execute_stmt(
    session: &SharedSession,
    stmt: &Statement,
    txn: &mut ConnTxn,
) -> Result<Executed, ExecError> {
    if let Some(intro) = pg_catalog::classify(stmt) {
        let (header, rows) = introspection_reply(&intro, session);
        return Ok(Executed::Rows {
            header,
            rows,
            sent: 0,
        });
    }
    match bind_ddl(stmt) {
        Ok(_) => {
            let tag = run_ddl(session, stmt).map_err(|e| ExecError {
                sqlstate: sqlstate_for(&e),
                message: e.to_string(),
            })?;
            Ok(Executed::Completed {
                tag: tag.to_owned(),
            })
        }
        Err(BindError::NotDdl) => {
            if let Some(columns) = constant_select(stmt) {
                let header = columns.iter().map(|c| field(&c.name, c.ty)).collect();
                return Ok(Executed::Rows {
                    header,
                    rows: vec![columns],
                    sent: 0,
                });
            }
            match run_query(session, stmt, txn) {
                Ok(StatementOutcome::Rows(result)) => {
                    let rows = decode_result_rows(&result).map_err(|e| ExecError {
                        sqlstate: SQLSTATE_INTERNAL_ERROR,
                        message: e.to_string(),
                    })?;
                    Ok(Executed::Rows {
                        header: result_header(&result),
                        rows,
                        sent: 0,
                    })
                }
                Ok(StatementOutcome::Dml(summary)) => Ok(Executed::Completed {
                    tag: command_tag_for(summary).render(),
                }),
                Ok(StatementOutcome::Ddl { tag }) => Ok(Executed::Completed {
                    tag: tag.to_owned(),
                }),
                Err(e) => Err(ExecError {
                    sqlstate: sqlstate_for_query(&e),
                    message: e.to_string(),
                }),
            }
        }
        Err(e) => Err(ExecError {
            sqlstate: SQLSTATE_SYNTAX_ERROR,
            message: e.to_string(),
        }),
    }
}

/// Ensure a portal's statement has been run, caching the outcome on the portal.
/// A no-op if it ran already (Describe then Execute share the one result).
fn ensure_executed(
    state: &mut ConnState,
    portal: &str,
    session: &SharedSession,
    stmt: &Statement,
    txn: &mut ConnTxn,
) -> Result<(), ExecError> {
    if state
        .portals
        .get(portal)
        .is_some_and(|p| p.executed.is_some())
    {
        return Ok(());
    }
    let executed = execute_stmt(session, stmt, txn)?;
    if let Some(entry) = state.portals.get_mut(portal) {
        entry.executed = Some(executed);
    }
    Ok(())
}

/// Write an `ErrorResponse` and latch the connection into skip-until-Sync — the
/// extended-query failure path (no trailing `ReadyForQuery`; the client's Sync
/// re-opens the batch).
async fn fail_extended(
    stream: &mut TcpStream,
    state: &mut ConnState,
    sqlstate: &str,
    message: &str,
) -> Result<(), WireError> {
    write_error_response(stream, "ERROR", sqlstate, message).await?;
    state.skip_until_sync = true;
    Ok(())
}

/// `Parse` ('P'): parse the query, store it under its name, reply `ParseComplete`.
async fn handle_parse(
    stream: &mut TcpStream,
    state: &mut ConnState,
    payload: &[u8],
) -> Result<(), WireError> {
    let Some(msg) = extended::parse_parse(payload) else {
        return Err(WireError::Protocol("malformed Parse message"));
    };
    // Re-preparing a *named* statement without closing it first is an error;
    // the unnamed statement ("") is silently replaced.
    if !msg.name.is_empty() && state.prepared.contains_key(&msg.name) {
        let m = format!("prepared statement \"{}\" already exists", msg.name);
        return fail_extended(stream, state, SQLSTATE_DUPLICATE_PSTATEMENT, &m).await;
    }
    let stmt = match parse_single(&msg.query) {
        Ok(stmt) => stmt,
        Err(e) => return fail_extended(stream, state, SQLSTATE_SYNTAX_ERROR, &e).await,
    };
    state.prepared.insert(
        msg.name,
        Prepared {
            stmt,
            param_oids: msg.param_oids,
        },
    );
    write_parse_complete(stream).await?;
    Ok(())
}

/// `Bind` ('B'): substitute the parameters into the named statement, creating a
/// portal, reply `BindComplete`. Binary parameter / result formats are refused
/// (text-only until the binary encoders land in \[G23\]).
async fn handle_bind(
    stream: &mut TcpStream,
    state: &mut ConnState,
    payload: &[u8],
) -> Result<(), WireError> {
    let Some(msg) = extended::parse_bind(payload) else {
        return Err(WireError::Protocol("malformed Bind message"));
    };
    // Clone the statement + OIDs so the prepared-cache borrow is released before
    // we mutate the portal cache.
    let Some(prepared) = state.prepared.get(&msg.statement) else {
        let m = format!("prepared statement \"{}\" does not exist", msg.statement);
        return fail_extended(stream, state, SQLSTATE_INVALID_PSTATEMENT_NAME, &m).await;
    };
    let (pstmt, param_oids) = (prepared.stmt.clone(), prepared.param_oids.clone());
    if msg.param_formats.iter().any(|&f| f != FORMAT_TEXT) {
        let m = "binary-format parameters are not yet supported";
        return fail_extended(stream, state, SQLSTATE_FEATURE_NOT_SUPPORTED, m).await;
    }
    if msg.result_formats.iter().any(|&f| f != FORMAT_TEXT) {
        let m = "binary-format results are not yet supported";
        return fail_extended(stream, state, SQLSTATE_FEATURE_NOT_SUPPORTED, m).await;
    }

    let mut values = Vec::with_capacity(msg.params.len());
    for (i, raw) in msg.params.iter().enumerate() {
        let oid = param_oids.get(i).copied().unwrap_or(0);
        match extended::param_to_value(oid, raw.as_deref()) {
            Ok(value) => values.push(value),
            Err(e) => {
                return fail_extended(
                    stream,
                    state,
                    SQLSTATE_INVALID_TEXT_REPRESENTATION,
                    &e.to_string(),
                )
                .await;
            }
        }
    }

    let bound = match pstmt {
        None => None,
        Some(stmt) => match extended::substitute(&stmt, &values) {
            Ok(s) => Some(s),
            Err(e) => {
                return fail_extended(stream, state, SQLSTATE_PROTOCOL_VIOLATION, &e.to_string())
                    .await;
            }
        },
    };
    state.portals.insert(
        msg.portal,
        PortalEntry {
            stmt: bound,
            executed: None,
        },
    );
    write_bind_complete(stream).await?;
    Ok(())
}

/// `Describe` ('D'): report the shape of a prepared statement (its parameter
/// types, then `NoData`) or a portal (its `RowDescription`, or `NoData` for a
/// write / empty portal).
async fn handle_describe(
    stream: &mut TcpStream,
    state: &mut ConnState,
    payload: &[u8],
    session: &SharedSession,
    txn: &mut ConnTxn,
) -> Result<(), WireError> {
    let Some(target) = extended::parse_target(payload) else {
        return Err(WireError::Protocol("malformed Describe message"));
    };
    match target {
        extended::Target::Statement(name) => {
            let Some(prepared) = state.prepared.get(&name) else {
                let m = format!("prepared statement \"{name}\" does not exist");
                return fail_extended(stream, state, SQLSTATE_INVALID_PSTATEMENT_NAME, &m).await;
            };
            let oids = prepared.param_oids.clone();
            write_parameter_description(stream, &oids).await?;
            // The row shape of an unbound statement needs its parameters resolved;
            // we report NoData and surface the real RowDescription from a
            // Describe-portal after Bind. (Statement-level row description is a
            // follow-up — see STL-183 fan-out.)
            write_no_data(stream).await?;
            Ok(())
        }
        extended::Target::Portal(name) => {
            handle_describe_portal(stream, state, &name, session, txn).await
        }
    }
}

/// The portal arm of [`handle_describe`]: run a row-returning portal (caching the
/// result) and reply `RowDescription`; reply `NoData` for a write or empty portal.
async fn handle_describe_portal(
    stream: &mut TcpStream,
    state: &mut ConnState,
    name: &str,
    session: &SharedSession,
    txn: &mut ConnTxn,
) -> Result<(), WireError> {
    let Some(portal) = state.portals.get(name) else {
        let m = format!("portal \"{name}\" does not exist");
        return fail_extended(stream, state, SQLSTATE_INVALID_CURSOR_NAME, &m).await;
    };
    let Some(stmt) = portal.stmt.clone() else {
        return write_no_data(stream).await.map_err(WireError::Io);
    };
    if !returns_rows(&stmt) {
        return write_no_data(stream).await.map_err(WireError::Io);
    }
    if let Err(e) = ensure_executed(state, name, session, &stmt, txn) {
        return fail_extended(stream, state, e.sqlstate, &e.message).await;
    }
    let header = match state.portals.get(name).and_then(|p| p.executed.as_ref()) {
        Some(Executed::Rows { header, .. }) => header.clone(),
        _ => Vec::new(),
    };
    write_row_description(stream, &header).await?;
    Ok(())
}

/// `Execute` ('E'): run the portal (if not already), then stream up to `max_rows`
/// `DataRow`s. Exhausting the portal ends with `CommandComplete`; stopping early
/// at the row cap ends with `PortalSuspended`, leaving the rest for the next
/// Execute.
///
/// Per the extended-query protocol, Execute does **not** emit a `RowDescription`
/// — that is the reply to `Describe`. A client learns the result columns by
/// issuing `Describe` on the statement or portal first (every mainstream driver
/// does); re-sending the row description on Execute would be a duplicate the
/// Describe-then-Execute flow does not expect.
async fn handle_execute(
    stream: &mut TcpStream,
    state: &mut ConnState,
    payload: &[u8],
    session: &SharedSession,
    txn: &mut ConnTxn,
) -> Result<(), WireError> {
    let Some(msg) = extended::parse_execute(payload) else {
        return Err(WireError::Protocol("malformed Execute message"));
    };
    // Inside an aborted transaction block, every statement is refused until
    // COMMIT/ROLLBACK ends it — same rule the simple-query path enforces (STL-174).
    if matches!(txn, ConnTxn::Failed) {
        return fail_extended(
            stream,
            state,
            SQLSTATE_IN_FAILED_TRANSACTION,
            "current transaction is aborted, commands ignored until end of transaction block",
        )
        .await;
    }
    let Some(portal) = state.portals.get(&msg.portal) else {
        let m = format!("portal \"{}\" does not exist", msg.portal);
        return fail_extended(stream, state, SQLSTATE_INVALID_CURSOR_NAME, &m).await;
    };
    let Some(stmt) = portal.stmt.clone() else {
        // An empty-query portal replies EmptyQueryResponse, with no command tag.
        return write_empty_query_response(stream)
            .await
            .map_err(WireError::Io);
    };
    if let Err(e) = ensure_executed(state, &msg.portal, session, &stmt, txn) {
        return fail_extended(stream, state, e.sqlstate, &e.message).await;
    }

    let entry = state.portals.get_mut(&msg.portal).expect("portal present");
    match entry.executed.as_mut().expect("executed cached") {
        Executed::Rows { rows, sent, .. } => {
            let remaining = rows.len() - *sent;
            // `max_rows <= 0` means "every remaining row"; a positive cap is
            // clamped to what is left.
            let take = if msg.max_rows <= 0 {
                remaining
            } else {
                usize::try_from(msg.max_rows)
                    .unwrap_or(remaining)
                    .min(remaining)
            };
            let start = *sent;
            let end = start + take;
            for row in &rows[start..end] {
                write_data_row(stream, row).await?;
            }
            *sent = end;
            if *sent < rows.len() {
                write_portal_suspended(stream).await?;
            } else {
                let n = u64::try_from(take).unwrap_or(u64::MAX);
                write_command_complete(stream, &CommandTag::Select(n)).await?;
            }
            Ok(())
        }
        Executed::Completed { tag } => {
            write_command_complete_tag(stream, tag).await?;
            Ok(())
        }
    }
}

/// `Close` ('C'): drop a prepared statement or portal (idempotent — closing an
/// absent name is not an error), reply `CloseComplete`.
async fn handle_close(
    stream: &mut TcpStream,
    state: &mut ConnState,
    payload: &[u8],
) -> Result<(), WireError> {
    let Some(target) = extended::parse_target(payload) else {
        return Err(WireError::Protocol("malformed Close message"));
    };
    match target {
        // Closing a statement should also close portals derived from it; we do
        // not track that linkage, so a named portal outlives its statement until
        // its own Close (a documented follow-up).
        extended::Target::Statement(name) => {
            state.prepared.remove(&name);
        }
        extended::Target::Portal(name) => {
            state.portals.remove(&name);
        }
    }
    write_close_complete(stream).await?;
    Ok(())
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

async fn write_ready_for_query(stream: &mut TcpStream, status: u8) -> io::Result<()> {
    // 'Z' + len(5) + status byte: 'I' (idle), 'T' (in a transaction block), or
    // 'E' (in a failed transaction block) — STL-174.
    let mut buf = BytesMut::with_capacity(6);
    buf.put_u8(MSG_READY_FOR_QUERY);
    buf.put_i32(5);
    buf.put_u8(status);
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
        // The RowDescription dataTypeOID field is a 4-byte OID. Write the `u32`
        // bits directly rather than narrowing to `i32` — narrowing would panic
        // on a future OID > i32::MAX, and `put_u32` emits exactly the big-endian
        // bytes a Postgres backend does.
        payload.put_u32(col.ty.pg_oid());
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
                // The DataRow length prefix is an Int32. Clamping an oversized
                // value would desync the client (prefix would not match the
                // bytes written), so refuse it rather than emit a torn frame.
                let len = i32::try_from(bytes.len())
                    .map_err(|_| WireError::Protocol("DataRow value exceeds 2 GiB"))?;
                payload.put_i32(len);
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
    write_command_complete_tag(stream, &tag.render()).await
}

/// `CommandComplete` ('C') for a tag string produced elsewhere — the DDL route
/// writes the engine's own tag ([`DdlOutcome::command_tag`](stele_sql::DdlOutcome::command_tag))
/// directly rather than round-tripping it through [`CommandTag`].
async fn write_command_complete_tag(stream: &mut TcpStream, tag: &str) -> io::Result<()> {
    let mut payload = BytesMut::with_capacity(tag.len() + 1);
    payload.put_slice(tag.as_bytes());
    payload.put_u8(0);
    write_framed(stream, MSG_COMMAND_COMPLETE, &payload).await
}

/// A payload-less typed message: 1-byte kind + Int32 length `4`. The extended
/// protocol's acknowledgements (`ParseComplete`, `BindComplete`, `CloseComplete`,
/// `NoData`, `PortalSuspended`) are all this shape.
async fn write_empty_framed(stream: &mut TcpStream, kind: u8) -> io::Result<()> {
    let buf: [u8; 5] = [kind, 0, 0, 0, 4];
    stream.write_all(&buf).await
}

/// `ParseComplete` ('1').
async fn write_parse_complete(stream: &mut TcpStream) -> io::Result<()> {
    write_empty_framed(stream, MSG_PARSE_COMPLETE).await
}

/// `BindComplete` ('2').
async fn write_bind_complete(stream: &mut TcpStream) -> io::Result<()> {
    write_empty_framed(stream, MSG_BIND_COMPLETE).await
}

/// `CloseComplete` ('3').
async fn write_close_complete(stream: &mut TcpStream) -> io::Result<()> {
    write_empty_framed(stream, MSG_CLOSE_COMPLETE).await
}

/// `NoData` ('n') — the reply to Describe on a statement / portal that returns
/// no result columns.
async fn write_no_data(stream: &mut TcpStream) -> io::Result<()> {
    write_empty_framed(stream, MSG_NO_DATA).await
}

/// `PortalSuspended` ('s') — a row-capped Execute stopped with rows still to
/// come; the next Execute on the same portal resumes.
async fn write_portal_suspended(stream: &mut TcpStream) -> io::Result<()> {
    write_empty_framed(stream, MSG_PORTAL_SUSPENDED).await
}

/// `ParameterDescription` ('t') — the parameter type OIDs of a prepared
/// statement, in `$1 … $n` order (`0` = the server is left to infer the type).
async fn write_parameter_description(
    stream: &mut TcpStream,
    oids: &[u32],
) -> Result<(), WireError> {
    let count =
        i16::try_from(oids.len()).map_err(|_| WireError::Protocol("more than 32767 parameters"))?;
    let mut payload = BytesMut::with_capacity(2 + oids.len() * 4);
    payload.put_i16(count);
    for &oid in oids {
        payload.put_u32(oid);
    }
    write_framed(stream, MSG_PARAMETER_DESCRIPTION, &payload).await?;
    Ok(())
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

    use stele_common::time::SystemTimeMicros;
    use stele_storage::backend::MemDisk;

    /// A constant inner clock; the engine's [`MonotonicClock`](stele_engine) turns
    /// its readings into the strictly increasing `1, 2, 3, …` the DDL timeline
    /// needs, and keeps the tests deterministic (no wall-clock reads).
    #[derive(Debug, Clone, Copy)]
    struct TestClock;
    impl Clock for TestClock {
        fn now(&self) -> SystemTimeMicros {
            SystemTimeMicros(0)
        }
    }

    /// A fresh server session over an in-memory backend — the real
    /// [`SessionEngine`], so the DDL and `\d` tests exercise the production route
    /// end to end (a `CREATE TABLE` actually registers a table and stands up its
    /// tiers). Connection-protocol tests that never touch storage just ignore it.
    fn test_session() -> SharedSession {
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), TestClock)))
    }

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

    /// Send a simple query and collect every backend message up to (but not
    /// including) the trailing `ReadyForQuery` — the whole reply to one `Q`.
    async fn run_simple(client: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
        run_simple_with_status(client, sql).await.0
    }

    /// As [`run_simple`], but also return the trailing `ReadyForQuery`
    /// transaction-status byte (`I` / `T` / `E`) — STL-174.
    async fn run_simple_with_status(client: &mut TcpStream, sql: &str) -> (Vec<(u8, Vec<u8>)>, u8) {
        send_query(client, sql).await;
        let mut msgs = Vec::new();
        loop {
            let (kind, payload) = read_message(client).await;
            if kind == MSG_READY_FOR_QUERY {
                return (msgs, payload[0]);
            }
            msgs.push((kind, payload));
        }
    }

    /// The field names of a `RowDescription` payload, skipping each field's
    /// fixed 18-byte metadata tail (table OID, attr, type OID, typlen, typmod,
    /// format).
    fn parse_row_description_names(payload: &[u8]) -> Vec<String> {
        let count = i16::from_be_bytes(payload[0..2].try_into().unwrap());
        let mut names = Vec::new();
        let mut pos = 2;
        for _ in 0..count {
            let end = payload[pos..].iter().position(|&b| b == 0).unwrap() + pos;
            names.push(String::from_utf8(payload[pos..end].to_vec()).unwrap());
            pos = end + 1 + 18;
        }
        names
    }

    /// The `CommandComplete` tag string (NUL stripped) from its payload.
    fn command_tag(payload: &[u8]) -> String {
        String::from_utf8(payload[..payload.len() - 1].to_vec()).unwrap()
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
            handle_connection(stream, peer, test_session()).await
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

    /// The `CommandComplete` tag carried in a query reply, if any.
    fn reply_tag(msgs: &[(u8, Vec<u8>)]) -> Option<String> {
        msgs.iter()
            .find(|(kind, _)| *kind == MSG_COMMAND_COMPLETE)
            .map(|(_, payload)| command_tag(payload))
    }

    #[tokio::test]
    async fn ready_for_query_reports_transaction_status_across_a_commit() {
        // The trailing ReadyForQuery byte tracks the transaction block: `I` idle,
        // `T` inside BEGIN…COMMIT, back to `I` after COMMIT (STL-174).
        let (server, mut client) = connect_past_handshake().await;

        let (_, status) = run_simple_with_status(
            &mut client,
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await;
        assert_eq!(status, b'I', "idle after a bare DDL statement");

        let (begin, status) = run_simple_with_status(&mut client, "BEGIN").await;
        assert_eq!(reply_tag(&begin).as_deref(), Some("BEGIN"));
        assert_eq!(status, b'T', "inside the transaction block after BEGIN");

        let (_, status) =
            run_simple_with_status(&mut client, "INSERT INTO account VALUES (1, 100)").await;
        assert_eq!(status, b'T', "still in the block after a buffered INSERT");

        let (commit, status) = run_simple_with_status(&mut client, "COMMIT").await;
        assert_eq!(reply_tag(&commit).as_deref(), Some("COMMIT"));
        assert_eq!(status, b'I', "idle again after COMMIT");

        // The committed row is visible afterwards.
        let rows = run_simple(&mut client, "SELECT id FROM account").await;
        let data_rows = rows.iter().filter(|(k, _)| *k == MSG_DATA_ROW).count();
        assert_eq!(data_rows, 1, "the committed INSERT is readable");

        terminate(server, client).await;
    }

    #[tokio::test]
    async fn an_error_in_a_transaction_aborts_it_until_rollback() {
        // A statement error inside BEGIN aborts the block: ReadyForQuery flips to
        // `E`, further statements are refused with 25P02, and ROLLBACK clears it
        // back to `I` (STL-174).
        let (server, mut client) = connect_past_handshake().await;
        run_simple(
            &mut client,
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await;

        let (_, status) = run_simple_with_status(&mut client, "BEGIN").await;
        assert_eq!(status, b'T');

        // A write against an unknown table errors and aborts the transaction.
        let (errored, status) =
            run_simple_with_status(&mut client, "INSERT INTO nope VALUES (1, 1)").await;
        assert!(
            errored.iter().any(|(k, _)| *k == MSG_ERROR_RESPONSE),
            "the bad write reports an error"
        );
        assert_eq!(status, b'E', "the transaction is now aborted");

        // Any further statement is refused until the block ends.
        let (refused, status) = run_simple_with_status(&mut client, "SELECT 1").await;
        let sqlstate = refused
            .iter()
            .find(|(k, _)| *k == MSG_ERROR_RESPONSE)
            .map(|(_, payload)| {
                // ErrorResponse fields: each a code byte + cstring; find 'C'.
                let mut cursor = &payload[..];
                let mut code = String::new();
                while !cursor.is_empty() && cursor[0] != 0 {
                    let field = cursor[0];
                    let end = cursor[1..].iter().position(|&b| b == 0).unwrap() + 1;
                    if field == b'C' {
                        code = String::from_utf8(cursor[1..end].to_vec()).unwrap();
                    }
                    cursor = &cursor[end + 1..];
                }
                code
            })
            .expect("error response");
        assert_eq!(sqlstate, SQLSTATE_IN_FAILED_TRANSACTION);
        assert_eq!(status, b'E', "still aborted");

        // ROLLBACK ends the block and returns to idle.
        let (rolled_back, status) = run_simple_with_status(&mut client, "ROLLBACK").await;
        assert_eq!(reply_tag(&rolled_back).as_deref(), Some("ROLLBACK"));
        assert_eq!(status, b'I', "idle again after ROLLBACK");

        terminate(server, client).await;
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
            if len == -1 {
                cells.push(None); // -1 is the *only* NULL sentinel
            } else {
                let n = usize::try_from(len).expect("a non-NULL length is non-negative");
                let end = pos + n;
                cells.push(Some(payload[pos..end].to_vec()));
                pos = end;
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
            ResultColumn {
                name: "u".into(),
                ty: LogicalType::Uuid,
                value: Some(ScalarValue::Uuid([
                    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55,
                    0x44, 0x00, 0x00,
                ])),
            },
            ResultColumn {
                name: "by".into(),
                ty: LogicalType::Bytea,
                value: Some(ScalarValue::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF])),
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
                "550e8400-e29b-41d4-a716-446655440000",
                "\\xdeadbeef",
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
            handle_connection(stream, peer, test_session()).await
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
            handle_connection(stream, peer, test_session()).await
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
            handle_connection(stream, peer, test_session()).await
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
        // and probing it with an SSLRequest yields the 'N' refusal byte. `bind`
        // up front (STL-152) reports the real ephemeral port with no reserve-drop
        // window, and the socket already accepts into its backlog, so the connect
        // needs no retry loop.
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let bound = Server::new(addr, test_session())
            .bind()
            .await
            .expect("bind ephemeral port");
        let addr = bound.local_addr();
        let handle = tokio::spawn(bound.serve());

        let mut client = TcpStream::connect(addr).await.expect("connect to server");

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
    async fn select_from_an_unknown_table_is_an_undefined_table_error() {
        let (server, mut client) = connect_past_handshake().await;
        // The table was never created, so the binder cannot resolve it — the
        // standard Postgres undefined-table SQLSTATE, not a crash or wrong answer.
        send_query(&mut client, "SELECT balance FROM account").await;

        let (kind, payload) = read_message(&mut client).await;
        assert_eq!(kind, MSG_ERROR_RESPONSE);
        assert!(
            payload
                .windows(5)
                .any(|w| w == SQLSTATE_UNDEFINED_TABLE.as_bytes()),
            "a read of an unknown table carries SQLSTATE 42P01"
        );

        let (kind, _) = read_message(&mut client).await;
        assert_eq!(kind, MSG_READY_FOR_QUERY);
        terminate(server, client).await;
    }

    /// The whole `(value cells of every DataRow)` of a simple-query reply, each
    /// cell rendered to its text-format string (skips `RowDescription` /
    /// `CommandComplete`). One inner `Vec` per row.
    fn data_row_text(msgs: &[(u8, Vec<u8>)]) -> Vec<Vec<String>> {
        msgs.iter()
            .filter(|(kind, _)| *kind == MSG_DATA_ROW)
            .map(|(_, payload)| {
                parse_data_row(payload)
                    .into_iter()
                    .map(|c| String::from_utf8(c.expect("non-null cell")).unwrap())
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn insert_then_table_select_round_trips_the_row() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        // INSERT replies with exactly one CommandComplete tagged `INSERT 0 1`.
        let inserted = run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await;
        assert_eq!(
            inserted.len(),
            1,
            "DML emits only CommandComplete: {inserted:?}"
        );
        assert_eq!(inserted[0].0, MSG_COMMAND_COMPLETE);
        assert_eq!(command_tag(&inserted[0].1), "INSERT 0 1");

        // The table read returns the (id, balance) row, decoded back to text from
        // the canonical encoding the INSERT wrote.
        let selected = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert_eq!(selected[0].0, MSG_ROW_DESCRIPTION);
        assert_eq!(
            parse_row_description_names(&selected[0].1),
            vec!["id", "balance"]
        );
        assert_eq!(data_row_text(&selected), vec![vec!["1", "100"]]);
        assert_eq!(command_tag(&selected.last().unwrap().1), "SELECT 1");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn update_and_delete_tag_their_row_counts() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;
        run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await;

        let updated =
            run_simple(&mut client, "UPDATE account SET balance = 250 WHERE id = 1").await;
        assert_eq!(updated.len(), 1);
        assert_eq!(command_tag(&updated[0].1), "UPDATE 1");

        // The latest read sees the updated value.
        let after_update = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert_eq!(data_row_text(&after_update), vec![vec!["1", "250"]]);

        let deleted = run_simple(&mut client, "DELETE FROM account WHERE id = 1").await;
        assert_eq!(deleted.len(), 1);
        assert_eq!(command_tag(&deleted[0].1), "DELETE 1");

        // After the delete the live read is empty (`SELECT 0`, no DataRows).
        let after_delete = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert!(
            data_row_text(&after_delete).is_empty(),
            "row gone after DELETE"
        );
        assert_eq!(command_tag(&after_delete.last().unwrap().1), "SELECT 0");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn as_of_select_reads_the_pre_update_value_over_the_wire() {
        // The identity demo's heart over the wire, made deterministic with an
        // integer AS OF: the test clock stamps CREATE/INSERT/UPDATE at sys_from
        // 1/2/3, so `AS OF 2` resolves to the inserted balance, not the updated
        // one. (`now() - interval` needs real elapsed time; the integer form pins
        // the instant for CI.)
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await; // sys_from 1
        run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await; // sys_from 2
        run_simple(&mut client, "UPDATE account SET balance = 250 WHERE id = 1").await; // sys_from 3

        let historical = run_simple(
            &mut client,
            "SELECT id, balance FROM account FOR SYSTEM_TIME AS OF 2",
        )
        .await;
        assert_eq!(
            data_row_text(&historical),
            vec![vec!["1", "100"]],
            "AS OF 2 returns the pre-update balance over the wire"
        );
        assert_eq!(command_tag(&historical.last().unwrap().1), "SELECT 1");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn dml_against_an_unknown_table_is_an_undefined_table_error() {
        let (server, mut client) = connect_past_handshake().await;
        // No table created — the binder cannot resolve `account`, so the INSERT is
        // refused with the undefined-table SQLSTATE (42P01), never a wrong write.
        let msgs = run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, MSG_ERROR_RESPONSE);
        assert!(
            msgs[0]
                .1
                .windows(5)
                .any(|w| w == SQLSTATE_UNDEFINED_TABLE.as_bytes()),
            "DML on an unknown table carries SQLSTATE 42P01: {:?}",
            msgs[0].1
        );
        terminate(server, client).await;
    }

    const CREATE_ACCOUNT: &str =
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING";

    #[tokio::test]
    async fn create_table_over_the_wire_returns_command_complete() {
        let (server, mut client) = connect_past_handshake().await;
        let msgs = run_simple(&mut client, CREATE_ACCOUNT).await;
        // A CREATE replies with exactly one CommandComplete tagged `CREATE TABLE`
        // — no RowDescription/DataRow — then the caller's ReadyForQuery.
        assert_eq!(msgs.len(), 1, "DDL emits only CommandComplete: {msgs:?}");
        assert_eq!(msgs[0].0, MSG_COMMAND_COMPLETE);
        assert_eq!(command_tag(&msgs[0].1), "CREATE TABLE");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn drop_table_over_the_wire_returns_command_complete() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;
        let msgs = run_simple(&mut client, "DROP TABLE account").await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, MSG_COMMAND_COMPLETE);
        assert_eq!(command_tag(&msgs[0].1), "DROP TABLE");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn drop_if_exists_absent_is_a_command_complete_not_an_error() {
        let (server, mut client) = connect_past_handshake().await;
        let msgs = run_simple(&mut client, "DROP TABLE IF EXISTS nope").await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, MSG_COMMAND_COMPLETE, "IF EXISTS no-op succeeds");
        assert_eq!(command_tag(&msgs[0].1), "DROP TABLE");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn re_creating_a_table_is_a_duplicate_table_error() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;
        // The second CREATE of the same name fails; the catalog error surfaces as
        // an ErrorResponse carrying the duplicate-table SQLSTATE, and the engine
        // state is unchanged.
        let msgs = run_simple(&mut client, CREATE_ACCOUNT).await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, MSG_ERROR_RESPONSE);
        assert!(
            msgs[0]
                .1
                .windows(5)
                .any(|w| w == SQLSTATE_DUPLICATE_TABLE.as_bytes()),
            "a re-create carries SQLSTATE 42P07: {:?}",
            msgs[0].1
        );
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn dropping_an_unknown_table_is_an_undefined_table_error() {
        let (server, mut client) = connect_past_handshake().await;
        // DROP without IF EXISTS of a never-created table is an error (42P01).
        let msgs = run_simple(&mut client, "DROP TABLE ghost").await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, MSG_ERROR_RESPONSE);
        assert!(
            msgs[0]
                .1
                .windows(5)
                .any(|w| w == SQLSTATE_UNDEFINED_TABLE.as_bytes()),
            "an unknown DROP carries SQLSTATE 42P01: {:?}",
            msgs[0].1
        );
        terminate(server, client).await;
    }

    /// The ticket's Definition of Done, realized with an in-process synthetic
    /// client (there is no `psql` in CI — the real-binary golden is STL-150):
    /// `CREATE TABLE account …` then `\d account` resolves the table's columns
    /// over the wire. `\d` is the two `pg_catalog` introspection queries `psql`
    /// fires — relation lookup then attribute list — driven here directly.
    #[tokio::test]
    async fn psql_backslash_d_resolves_a_created_tables_columns() {
        let (server, mut client) = connect_past_handshake().await;

        // CREATE the demo table.
        let created = run_simple(&mut client, CREATE_ACCOUNT).await;
        assert_eq!(command_tag(&created[0].1), "CREATE TABLE");

        // `\d` step 1: resolve `account` in pg_class to its oid.
        let lookup = run_simple(
            &mut client,
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname = 'account'",
        )
        .await;
        assert_eq!(lookup[0].0, MSG_ROW_DESCRIPTION);
        assert_eq!(
            parse_row_description_names(&lookup[0].1),
            vec!["oid", "nspname", "relname"]
        );
        assert_eq!(lookup[1].0, MSG_DATA_ROW, "one row: the relation exists");
        let row = parse_data_row(&lookup[1].1);
        let oid = String::from_utf8(row[0].clone().expect("oid present")).unwrap();
        assert_eq!(row[2].as_deref(), Some(b"account".as_ref()), "relname");
        assert_eq!(command_tag(&lookup[2].1), "SELECT 1");

        // `\d` step 2: list the relation's columns from pg_attribute by that oid.
        let attrs = run_simple(
            &mut client,
            &format!(
                "SELECT a.attname FROM pg_catalog.pg_attribute a \
                 WHERE a.attrelid = {oid} AND a.attnum > 0 ORDER BY a.attnum"
            ),
        )
        .await;
        assert_eq!(attrs[0].0, MSG_ROW_DESCRIPTION);
        assert_eq!(
            parse_row_description_names(&attrs[0].1),
            vec!["attname", "atttypname", "attnum"]
        );
        // Two DataRows — the table's two columns, in declaration order.
        let columns: Vec<(String, String, String)> = attrs
            .iter()
            .filter(|(kind, _)| *kind == MSG_DATA_ROW)
            .map(|(_, payload)| {
                let cells = parse_data_row(payload);
                let text = |i: usize| String::from_utf8(cells[i].clone().unwrap()).unwrap();
                (text(0), text(1), text(2))
            })
            .collect();
        assert_eq!(
            columns,
            vec![
                ("id".to_owned(), "int4".to_owned(), "1".to_owned()),
                ("balance".to_owned(), "int4".to_owned(), "2".to_owned()),
            ],
            "\\d account lists both columns with their types"
        );
        let tag = command_tag(&attrs.last().unwrap().1);
        assert_eq!(tag, "SELECT 2");

        terminate(server, client).await;
    }

    #[tokio::test]
    async fn backslash_d_on_a_missing_table_is_empty_not_an_error() {
        let (server, mut client) = connect_past_handshake().await;
        // No table created — the relation lookup resolves to zero rows (psql then
        // prints "Did not find any relation named ..."), never an ErrorResponse.
        let lookup = run_simple(
            &mut client,
            "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname = 'ghost'",
        )
        .await;
        assert_eq!(lookup[0].0, MSG_ROW_DESCRIPTION);
        assert!(
            lookup.iter().all(|(kind, _)| *kind != MSG_DATA_ROW),
            "no rows for an unknown relation"
        );
        assert_eq!(command_tag(&lookup.last().unwrap().1), "SELECT 0");
        terminate(server, client).await;
    }

    #[tokio::test]
    async fn create_then_select_one_still_round_trips_on_the_same_connection() {
        // DDL routing must not disturb the constant-SELECT path that shares the
        // loop: a CREATE followed by `SELECT 1` both work on one connection.
        let (server, mut client) = connect_past_handshake().await;
        let created = run_simple(&mut client, CREATE_ACCOUNT).await;
        assert_eq!(command_tag(&created[0].1), "CREATE TABLE");

        let select = run_simple(&mut client, "SELECT 1").await;
        assert_eq!(select[0].0, MSG_ROW_DESCRIPTION);
        assert_eq!(select[1].0, MSG_DATA_ROW);
        assert_eq!(&select[1].1[6..], b"1");
        assert_eq!(command_tag(&select[2].1), "SELECT 1");
        terminate(server, client).await;
    }

    // -----------------------------------------------------------------------
    // Extended-query protocol (STL-182)
    // -----------------------------------------------------------------------

    // Well-known Postgres OIDs the tests declare for typed parameters.
    const OID_INT4: u32 = 23;

    /// Append a NUL-terminated cstring.
    fn put_cstr(buf: &mut BytesMut, s: &str) {
        buf.put_slice(s.as_bytes());
        buf.put_u8(0);
    }

    /// Frame and send one extended-query message (`kind` + length + body).
    async fn send_msg(client: &mut TcpStream, kind: u8, body: &[u8]) {
        let mut m = BytesMut::with_capacity(5 + body.len());
        m.put_u8(kind);
        m.put_i32(i32::try_from(4 + body.len()).unwrap());
        m.put_slice(body);
        client.write_all(&m).await.unwrap();
    }

    /// `Parse`: name the statement, the SQL, and the parameter type OIDs.
    async fn send_parse(client: &mut TcpStream, name: &str, query: &str, oids: &[u32]) {
        let mut b = BytesMut::new();
        put_cstr(&mut b, name);
        put_cstr(&mut b, query);
        b.put_i16(i16::try_from(oids.len()).unwrap());
        for &o in oids {
            b.put_u32(o);
        }
        send_msg(client, MSG_PARSE, &b).await;
    }

    /// `Bind`: all parameters + results in text format (zero format codes). A
    /// `None` parameter is a SQL `NULL`.
    async fn send_bind(client: &mut TcpStream, portal: &str, stmt: &str, params: &[Option<&str>]) {
        let mut b = BytesMut::new();
        put_cstr(&mut b, portal);
        put_cstr(&mut b, stmt);
        b.put_i16(0); // zero param format codes → all text
        b.put_i16(i16::try_from(params.len()).unwrap());
        for p in params {
            match p {
                None => b.put_i32(-1),
                Some(s) => {
                    b.put_i32(i32::try_from(s.len()).unwrap());
                    b.put_slice(s.as_bytes());
                }
            }
        }
        b.put_i16(0); // zero result format codes → all text
        send_msg(client, MSG_BIND, &b).await;
    }

    /// `Describe` a statement (`b'S'`) or portal (`b'P'`).
    async fn send_describe(client: &mut TcpStream, target: u8, name: &str) {
        let mut b = BytesMut::new();
        b.put_u8(target);
        put_cstr(&mut b, name);
        send_msg(client, MSG_DESCRIBE, &b).await;
    }

    /// `Execute` a portal, capping the row count (`0` = no cap).
    async fn send_execute(client: &mut TcpStream, portal: &str, max_rows: i32) {
        let mut b = BytesMut::new();
        put_cstr(&mut b, portal);
        b.put_i32(max_rows);
        send_msg(client, MSG_EXECUTE, &b).await;
    }

    /// `Close` a statement (`b'S'`) or portal (`b'P'`).
    async fn send_close(client: &mut TcpStream, target: u8, name: &str) {
        let mut b = BytesMut::new();
        b.put_u8(target);
        put_cstr(&mut b, name);
        send_msg(client, MSG_CLOSE, &b).await;
    }

    /// `Sync`.
    async fn send_sync(client: &mut TcpStream) {
        send_msg(client, MSG_SYNC, &[]).await;
    }

    /// Read backend messages up to (not including) the next `ReadyForQuery` — the
    /// whole reply to an extended batch closed by `Sync`.
    async fn drain_to_ready(client: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        let mut msgs = Vec::new();
        loop {
            let (kind, payload) = read_message(client).await;
            if kind == MSG_READY_FOR_QUERY {
                break;
            }
            msgs.push((kind, payload));
        }
        msgs
    }

    /// The DoD, end-to-end: a parameterized `INSERT` Parses once and Executes
    /// twice with different bound parameters, and the rows land. Proving "Parse
    /// once, Bind/Execute many" — the heart of the extended protocol.
    #[tokio::test]
    async fn parameterized_insert_parses_once_executes_twice() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        // Parse the statement a single time, then Bind + Execute the first row.
        send_parse(
            &mut client,
            "ins",
            "INSERT INTO account VALUES ($1, $2)",
            &[OID_INT4, OID_INT4],
        )
        .await;
        send_bind(&mut client, "", "ins", &[Some("1"), Some("100")]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;

        let first = drain_to_ready(&mut client).await;
        assert_eq!(first[0].0, MSG_PARSE_COMPLETE, "ParseComplete");
        assert_eq!(first[1].0, MSG_BIND_COMPLETE, "BindComplete");
        assert_eq!(first[2].0, MSG_COMMAND_COMPLETE);
        assert_eq!(command_tag(&first[2].1), "INSERT 0 1");

        // Re-Bind the *same* prepared statement — no second Parse — with new
        // parameters and Execute again.
        send_bind(&mut client, "", "ins", &[Some("2"), Some("200")]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;

        let second = drain_to_ready(&mut client).await;
        assert_eq!(
            second.len(),
            2,
            "only BindComplete + CommandComplete: {second:?}"
        );
        assert_eq!(second[0].0, MSG_BIND_COMPLETE);
        assert_eq!(command_tag(&second[1].1), "INSERT 0 1");

        // Both rows are present.
        let rows = run_simple(&mut client, "SELECT id, balance FROM account").await;
        let mut got = data_row_text(&rows);
        got.sort();
        assert_eq!(got, vec![vec!["1", "100"], vec!["2", "200"]]);
        terminate(server, client).await;
    }

    /// A parameterized `UPDATE`'s `SET` value and `WHERE` key both bind from
    /// parameters, and the write commits.
    #[tokio::test]
    async fn parameterized_update_binds_set_and_where() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;
        run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await;

        send_parse(
            &mut client,
            "",
            "UPDATE account SET balance = $1 WHERE id = $2",
            &[OID_INT4, OID_INT4],
        )
        .await;
        send_bind(&mut client, "", "", &[Some("250"), Some("1")]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;

        let msgs = drain_to_ready(&mut client).await;
        assert_eq!(command_tag(&msgs.last().unwrap().1), "UPDATE 1");

        let rows = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert_eq!(data_row_text(&rows), vec![vec!["1", "250"]]);
        terminate(server, client).await;
    }

    /// Describe on a portal returns the result `RowDescription`; Execute then
    /// streams the rows and a `CommandComplete`.
    #[tokio::test]
    async fn describe_portal_then_execute_streams_rows() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;
        run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await;

        send_parse(&mut client, "sel", "SELECT id, balance FROM account", &[]).await;
        send_bind(&mut client, "", "sel", &[]).await;
        send_describe(&mut client, b'P', "").await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;

        let msgs = drain_to_ready(&mut client).await;
        // ParseComplete, BindComplete, RowDescription, DataRow, CommandComplete.
        assert_eq!(msgs[0].0, MSG_PARSE_COMPLETE);
        assert_eq!(msgs[1].0, MSG_BIND_COMPLETE);
        assert_eq!(msgs[2].0, MSG_ROW_DESCRIPTION);
        assert_eq!(
            parse_row_description_names(&msgs[2].1),
            vec!["id", "balance"]
        );
        assert_eq!(msgs[3].0, MSG_DATA_ROW);
        assert_eq!(
            parse_data_row(&msgs[3].1),
            vec![Some(b"1".to_vec()), Some(b"100".to_vec())]
        );
        assert_eq!(command_tag(&msgs[4].1), "SELECT 1");
        terminate(server, client).await;
    }

    /// Describe on a *statement* reports its parameter types (`ParameterDescription`)
    /// followed by `NoData` (statement-level row description is deferred).
    #[tokio::test]
    async fn describe_statement_reports_parameter_types() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        send_parse(
            &mut client,
            "s",
            "SELECT id FROM account WHERE id = $1",
            &[OID_INT4],
        )
        .await;
        send_describe(&mut client, b'S', "s").await;
        send_sync(&mut client).await;

        let msgs = drain_to_ready(&mut client).await;
        assert_eq!(msgs[0].0, MSG_PARSE_COMPLETE);
        assert_eq!(msgs[1].0, MSG_PARAMETER_DESCRIPTION);
        // Int16 count == 1, then the single Int32 OID 23 (int4).
        assert_eq!(i16::from_be_bytes(msgs[1].1[0..2].try_into().unwrap()), 1);
        assert_eq!(
            u32::from_be_bytes(msgs[1].1[2..6].try_into().unwrap()),
            OID_INT4
        );
        assert_eq!(msgs[2].0, MSG_NO_DATA);
        terminate(server, client).await;
    }

    /// A row cap suspends the portal: the first Execute returns one row +
    /// `PortalSuspended`, the next resumes and finishes with `CommandComplete`.
    #[tokio::test]
    async fn execute_row_cap_suspends_then_resumes_portal() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;
        run_simple(&mut client, "INSERT INTO account VALUES (1, 100)").await;
        run_simple(&mut client, "INSERT INTO account VALUES (2, 200)").await;

        send_parse(&mut client, "", "SELECT id, balance FROM account", &[]).await;
        send_bind(&mut client, "", "", &[]).await;
        send_execute(&mut client, "", 1).await; // cap at one row
        send_sync(&mut client).await;

        let first = drain_to_ready(&mut client).await;
        // ParseComplete, BindComplete, one DataRow, PortalSuspended.
        assert_eq!(first[0].0, MSG_PARSE_COMPLETE);
        assert_eq!(first[1].0, MSG_BIND_COMPLETE);
        let rows = first.iter().filter(|(k, _)| *k == MSG_DATA_ROW).count();
        assert_eq!(rows, 1, "only one row before the suspend: {first:?}");
        assert_eq!(first.last().unwrap().0, MSG_PORTAL_SUSPENDED);

        // Resume: the remaining row, then CommandComplete.
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;
        let second = drain_to_ready(&mut client).await;
        let rows = second.iter().filter(|(k, _)| *k == MSG_DATA_ROW).count();
        assert_eq!(rows, 1, "the second row resumes: {second:?}");
        assert_eq!(second.last().unwrap().0, MSG_COMMAND_COMPLETE);
        assert_eq!(command_tag(&second.last().unwrap().1), "SELECT 1");
        terminate(server, client).await;
    }

    /// Closing a portal destroys it: a later Execute of that portal is an
    /// invalid-cursor error, and the connection recovers at the next Sync.
    #[tokio::test]
    async fn close_portal_then_execute_is_invalid_cursor() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        send_parse(&mut client, "", "SELECT id, balance FROM account", &[]).await;
        send_bind(&mut client, "p", "", &[]).await;
        send_close(&mut client, b'P', "p").await;
        send_execute(&mut client, "p", 0).await;
        send_sync(&mut client).await;

        let msgs = drain_to_ready(&mut client).await;
        assert_eq!(msgs[0].0, MSG_PARSE_COMPLETE);
        assert_eq!(msgs[1].0, MSG_BIND_COMPLETE);
        assert_eq!(msgs[2].0, MSG_CLOSE_COMPLETE);
        // The Execute of the closed portal errors with SQLSTATE 34000.
        assert_eq!(msgs[3].0, MSG_ERROR_RESPONSE);
        assert!(
            msgs[3]
                .1
                .windows(5)
                .any(|w| w == SQLSTATE_INVALID_CURSOR_NAME.as_bytes()),
            "closed-portal Execute carries SQLSTATE 34000: {:?}",
            msgs[3].1
        );
        terminate(server, client).await;
    }

    /// An extended-query error discards every following message until Sync: a
    /// Bind to a missing statement errors, the Execute after it is swallowed, and
    /// only Sync produces the recovering `ReadyForQuery`.
    #[tokio::test]
    async fn error_in_batch_skips_messages_until_sync() {
        let (server, mut client) = connect_past_handshake().await;

        // Bind references a prepared statement that was never Parsed.
        send_bind(&mut client, "", "ghost", &[]).await;
        // This Execute must be discarded (the portal was never created anyway).
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;

        let msgs = drain_to_ready(&mut client).await;
        // Exactly one reply before ReadyForQuery: the ErrorResponse. The Execute
        // produced nothing.
        assert_eq!(msgs.len(), 1, "only the error, Execute swallowed: {msgs:?}");
        assert_eq!(msgs[0].0, MSG_ERROR_RESPONSE);
        assert!(
            msgs[0]
                .1
                .windows(5)
                .any(|w| w == SQLSTATE_INVALID_PSTATEMENT_NAME.as_bytes()),
            "Bind to a missing statement carries SQLSTATE 26000: {:?}",
            msgs[0].1
        );

        // The connection is healthy again: a fresh extended round-trips.
        send_parse(&mut client, "", "SELECT 1", &[]).await;
        send_bind(&mut client, "", "", &[]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;
        let ok = drain_to_ready(&mut client).await;
        assert_eq!(ok[0].0, MSG_PARSE_COMPLETE);
        assert_eq!(command_tag(&ok.last().unwrap().1), "SELECT 1");
        terminate(server, client).await;
    }

    /// A NULL parameter (length `-1`) binds as SQL `NULL`: an `INSERT` of a NULL
    /// payload round-trips back as the NULL cell over the wire.
    #[tokio::test]
    async fn null_parameter_binds_as_sql_null() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        send_parse(
            &mut client,
            "",
            "INSERT INTO account VALUES ($1, $2)",
            &[OID_INT4, OID_INT4],
        )
        .await;
        send_bind(&mut client, "", "", &[Some("1"), None]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;
        let msgs = drain_to_ready(&mut client).await;
        assert_eq!(command_tag(&msgs.last().unwrap().1), "INSERT 0 1");

        let rows = run_simple(&mut client, "SELECT id, balance FROM account").await;
        let data = rows
            .iter()
            .find(|(k, _)| *k == MSG_DATA_ROW)
            .map(|(_, p)| parse_data_row(p))
            .expect("one data row");
        assert_eq!(
            data,
            vec![Some(b"1".to_vec()), None],
            "balance bound to NULL"
        );
        terminate(server, client).await;
    }

    /// `Flush` is answered (it forces buffered output out) and does not disturb
    /// the in-flight extended batch.
    #[tokio::test]
    async fn flush_does_not_break_the_batch() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        send_parse(&mut client, "", "SELECT 1", &[]).await;
        send_msg(&mut client, MSG_FLUSH, &[]).await;
        // ParseComplete is available immediately after the Flush.
        let (kind, _) = read_message(&mut client).await;
        assert_eq!(kind, MSG_PARSE_COMPLETE);

        send_bind(&mut client, "", "", &[]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;
        let msgs = drain_to_ready(&mut client).await;
        assert_eq!(command_tag(&msgs.last().unwrap().1), "SELECT 1");
        terminate(server, client).await;
    }

    /// Read backend messages up to AND including the next `ReadyForQuery`,
    /// returning its transaction-status byte (`I`/`T`/`E`).
    async fn drain_capturing_status(client: &mut TcpStream) -> u8 {
        loop {
            let (kind, payload) = read_message(client).await;
            if kind == MSG_READY_FOR_QUERY {
                return payload[0];
            }
        }
    }

    /// An extended-query `INSERT` issued inside a simple-query `BEGIN` block
    /// buffers into the transaction (STL-174 integration): `Sync` reports `T`
    /// mid-block, `COMMIT` applies the write, and `ROLLBACK` discards it.
    #[tokio::test]
    async fn extended_dml_participates_in_a_begin_block() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        // Open a block, then INSERT through the extended path.
        run_simple(&mut client, "BEGIN").await;
        send_parse(
            &mut client,
            "",
            "INSERT INTO account VALUES ($1, $2)",
            &[OID_INT4, OID_INT4],
        )
        .await;
        send_bind(&mut client, "", "", &[Some("1"), Some("100")]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;
        // The buffered INSERT still tags its would-be count, and the status byte
        // shows we are inside a transaction (`T`).
        let status = drain_capturing_status(&mut client).await;
        assert_eq!(status, b'T', "Sync inside BEGIN reports in-transaction");

        // Not yet visible (still buffered).
        let mid = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert!(
            data_row_text(&mid).is_empty(),
            "write buffered until COMMIT"
        );

        // COMMIT applies it.
        run_simple(&mut client, "COMMIT").await;
        let after = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert_eq!(data_row_text(&after), vec![vec!["1", "100"]]);
        terminate(server, client).await;
    }

    /// The rollback half: an extended `INSERT` buffered in a `BEGIN` block is
    /// discarded by `ROLLBACK`.
    #[tokio::test]
    async fn extended_dml_rolled_back_in_a_begin_block_is_discarded() {
        let (server, mut client) = connect_past_handshake().await;
        run_simple(&mut client, CREATE_ACCOUNT).await;

        run_simple(&mut client, "BEGIN").await;
        send_parse(
            &mut client,
            "",
            "INSERT INTO account VALUES ($1, $2)",
            &[OID_INT4, OID_INT4],
        )
        .await;
        send_bind(&mut client, "", "", &[Some("9"), Some("900")]).await;
        send_execute(&mut client, "", 0).await;
        send_sync(&mut client).await;
        drain_to_ready(&mut client).await;

        run_simple(&mut client, "ROLLBACK").await;
        let after = run_simple(&mut client, "SELECT id, balance FROM account").await;
        assert!(
            data_row_text(&after).is_empty(),
            "ROLLBACK discarded the write"
        );
        terminate(server, client).await;
    }
}
