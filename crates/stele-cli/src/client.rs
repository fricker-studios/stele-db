//! A minimal, blocking Postgres wire-protocol client — just enough for the
//! `stele shell` REPL ([STL-185]).
//!
//! Speaks the **simple-query** slice of the protocol the Stele front end
//! serves: a `StartupMessage`, then `Query` round-trips collecting
//! `RowDescription` / `DataRow` / `CommandComplete` until `ReadyForQuery`.
//! Everything arrives in text format, so cells decode straight to strings.
//!
//! **Authentication** ([STL-296]): against a trust-auth server `AuthenticationOk`
//! arrives unconditionally; against one running `auth = "scram"` the server sends
//! `AuthenticationSASL` and the client runs the SASL SCRAM-SHA-256 exchange —
//! sending its proof and, crucially, **verifying the server's signature** before
//! trusting the connection (mutual authentication). The proof math is the
//! vendored, RFC-vectored [`stele_common::scram`]; this file owns only the wire
//! framing, the mirror of the server's `stele_pgwire::scram` (a dev-only
//! dependency here, so it is named, not linked).
//!
//! **Channel binding** ([STL-334]): over TLS, when the server advertises
//! `SCRAM-SHA-256-PLUS` ([STL-297]), the client prefers it — computing the RFC
//! 5929 `tls-server-end-point` binding from the *negotiated* server certificate
//! and folding it into the `c=` value, so a man-in-the-middle that terminates TLS
//! with a different certificate cannot relay the proof. The binding's hash is
//! selected from the leaf certificate's signature algorithm exactly as the server
//! does (`endpoint_channel_binding`), so both sides compute the identical `c=`.
//! Off TLS, against a plain-only server, or for a certificate we cannot bind, the
//! client keeps the plain `SCRAM-SHA-256` + `n` path (it never sends `y`).
//!
//! Deliberately hand-rolled rather than pulling in a client crate:
//! `tokio-postgres` is pinned as a **dev-only** dependency workspace-wide (a
//! shipped `stele` binary must not grow its supply-chain surface), and the
//! ~hundred lines here double as a second, independent reading of the wire
//! format the `stele-pgwire` server emits.
//!
//! **TLS** ([STL-251]) rides the same `rustls` the server stack already pins
//! (no new supply-chain surface) through its *blocking* [`rustls::StreamOwned`]
//! adapter, behind a libpq-style [`SslMode`]: send `SSLRequest`, handshake on
//! `S`, fall back (or refuse to) on `N`. As in libpq, `require` and below
//! encrypt **without verifying the server's identity**; only `verify-full`
//! checks the certificate against a CA (`--tls-ca`) and the host name. To reach
//! a server running mTLS (`[tls] client_ca`), the shell **presents a client
//! certificate** ([STL-292]) with `--tls-cert`/`--tls-key` — libpq's
//! `sslcert`/`sslkey` — under any encrypting mode.
//!
//! Errors split deliberately: a *SQL* failure (`ErrorResponse`) is data — it
//! comes back as [`Reply::Error`] and the connection stays usable — while a
//! *transport* failure (socket death, malformed frame) is `Err` and the caller
//! should drop the connection.
//!
//! [STL-185]: https://allegromusic.atlassian.net/browse/STL-185
//! [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
//! [STL-292]: https://allegromusic.atlassian.net/browse/STL-292
//! [STL-296]: https://allegromusic.atlassian.net/browse/STL-296
//! [STL-297]: https://allegromusic.atlassian.net/browse/STL-297
//! [STL-334]: https://allegromusic.atlassian.net/browse/STL-334

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, bail};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};

use stele_common::hash::sha256;
use stele_common::query_stats::QueryStats;
use stele_common::scram::{self, ScramVerifier};
use x509_parser::oid_registry::{OID_PKCS1_SHA256WITHRSA, OID_SIG_ECDSA_WITH_SHA256};
use x509_parser::prelude::{FromDer as _, X509Certificate};

use crate::render::Column;

// Backend message types this client consumes (the post-startup stream).
const MSG_AUTHENTICATION: u8 = b'R';
const MSG_READY_FOR_QUERY: u8 = b'Z';
const MSG_ERROR_RESPONSE: u8 = b'E';
const MSG_ROW_DESCRIPTION: u8 = b'T';
const MSG_DATA_ROW: u8 = b'D';
const MSG_COMMAND_COMPLETE: u8 = b'C';
const MSG_EMPTY_QUERY_RESPONSE: u8 = b'I';
const MSG_NOTICE_RESPONSE: u8 = b'N';
// Frontend message types this client emits.
const MSG_QUERY: u8 = b'Q';
const MSG_TERMINATE: u8 = b'X';
// SASLInitialResponse / SASLResponse share the password-message type byte.
const MSG_SASL: u8 = b'p';

// SASL authentication request codes — the Int32 inside an `Authentication`
// ('R') message ([STL-296], RFC 5802 / RFC 7677).
const AUTH_OK: i32 = 0;
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

/// SQLSTATE `28P01` (`invalid_password`) — the code the server returns for a
/// failed SCRAM proof *and* for an unknown user (the two are deliberately
/// indistinguishable, so an attacker cannot enumerate users). The shell treats
/// it as "the password was wrong" and, interactively, re-prompts ([STL-335]).
const SQLSTATE_INVALID_PASSWORD: &str = "28P01";

/// Plain `SCRAM-SHA-256` — no channel binding. Always offered (alongside `-PLUS`
/// over TLS), and the floor the shell uses off TLS, against a plain-only server,
/// or for a certificate it cannot bind. The `n` gs2 flag the client pairs with it
/// ("I do not support channel binding") is accepted on either transport.
const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// `SCRAM-SHA-256-PLUS` — `tls-server-end-point` channel binding (STL-334), the
/// mechanism the shell prefers when it runs over TLS and the server advertises it
/// (STL-297). Selecting it binds the SASL proof to the certificate the TLS
/// handshake actually negotiated.
const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// The gs2 header for plain SCRAM: no channel binding (`n`), no authzid. Its
/// base64 (`biws`) is the whole `c=` value under plain SCRAM.
const GS2_HEADER_PLAIN: &str = "n,,";

/// The gs2 header for `SCRAM-SHA-256-PLUS`: channel binding required (`p=`) of the
/// `tls-server-end-point` type, no authzid. Under PLUS the `c=` value is the
/// base64 of this header followed by the binding data.
const GS2_HEADER_PLUS: &str = "p=tls-server-end-point,,";

/// Raw client-nonce entropy — 18 bytes → 24 base64 characters, matching the
/// server's nonce width and Postgres.
const CLIENT_NONCE_RAW_LEN: usize = 18;

/// pg-wire protocol version 3.0, as the `StartupMessage` carries it.
const PROTOCOL_VERSION: i32 = 196_608;

/// The `SSLRequest` startup-shape code (length 8, no message-type byte).
const SSL_REQUEST_CODE: i32 = 80_877_103;

/// Upper bound on a single backend message body. The server's replies are
/// row-at-a-time and small; anything larger means a desynchronized stream.
const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024;

/// One result set: the `RowDescription` columns (name + type OID) plus every
/// `DataRow`, cells decoded from text format (`None` = SQL `NULL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<String>>>,
    /// The server's per-query execution stats ([STL-201]), when it delivered them
    /// (the `NoticeResponse` trailer). `None` when the server sent none — a build
    /// without the trailer, a read with no scan to account for, or stats not
    /// requested — so the shell suppresses the footer.
    pub stats: Option<QueryStats>,
}

/// A decoded `ErrorResponse`: the fields the shell renders as the psql-style
/// `ERROR:` / `SQLSTATE:` / `HINT:` block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub severity: String,
    /// The SQLSTATE code (field `C`), empty when the server omitted it.
    pub code: String,
    pub message: String,
    /// Optional hint (field `H`).
    pub hint: Option<String>,
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.severity, self.message)
    }
}

/// [`Client::connect`] failed because the server requested SCRAM authentication
/// but no password was supplied ([STL-296]). The caller distinguishes this from
/// every other failure (via [`anyhow::Error::downcast_ref`]) so it can prompt
/// for a password and retry — an interactive shell — or report that `PGPASSWORD`
/// must be set — a scripted one.
///
/// [STL-296]: https://allegromusic.atlassian.net/browse/STL-296
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasswordRequired {
    /// The user the server is asking us to authenticate.
    pub user: String,
}

impl std::fmt::Display for PasswordRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server requires a password for user {:?}; set the PGPASSWORD environment variable",
            self.user
        )
    }
}

impl std::error::Error for PasswordRequired {}

/// [`Client::connect`] failed mid-SCRAM because the server rejected the
/// credentials — an `ErrorResponse` during the SASL exchange ([STL-335]).
/// Carries the decoded error so the caller can both report it and decide whether
/// to retry: an **interactive** shell re-prompts for a password when this is a
/// wrong-password rejection ([`Self::is_password_rejection`], SQLSTATE `28P01`),
/// the way psql does; a scripted shell, or any non-password SASL error, surfaces
/// it unchanged. Distinguished from other failures via
/// [`anyhow::Error::downcast_ref`].
///
/// [STL-335]: https://allegromusic.atlassian.net/browse/STL-335
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthFailed {
    /// The decoded `ErrorResponse` the server sent; its `code` is the SQLSTATE.
    pub error: ServerError,
}

impl AuthFailed {
    /// Whether the rejection is a wrong-password error (SQLSTATE `28P01`) — the
    /// only SASL failure an interactive shell answers by re-prompting. A policy
    /// refusal or a protocol fault is not a password problem and surfaces as-is.
    #[must_use]
    pub fn is_password_rejection(&self) -> bool {
        self.error.code == SQLSTATE_INVALID_PASSWORD
    }
}

impl std::fmt::Display for AuthFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Keeps the pre-STL-335 wording ("authentication failed — …") so existing
        // diagnostics and tests read the same.
        write!(f, "authentication failed — {}", self.error)
    }
}

impl std::error::Error for AuthFailed {}

/// What one statement inside a simple-query round-trip produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A row-returning statement: header + rows.
    Rows(ResultSet),
    /// A non-row statement's `CommandComplete` tag (`INSERT 0 1`, `CREATE TABLE`, …).
    Command(String),
    /// The server rejected the statement (`ErrorResponse`). The connection
    /// itself is still good.
    Error(ServerError),
    /// An empty query string (`EmptyQueryResponse`).
    Empty,
}

/// How (whether) the connection is encrypted — libpq's `sslmode`, as
/// `stele shell --tls` spells it ([STL-251]).
///
/// [STL-251]: https://allegromusic.atlassian.net/browse/STL-251
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Plaintext; no `SSLRequest` is sent.
    Disable,
    /// Try TLS, fall back to plaintext if the server refuses (libpq's default).
    #[default]
    Prefer,
    /// TLS or fail. Like libpq's `require`, the server's identity is **not**
    /// verified — this protects against eavesdropping, not impersonation.
    Require,
    /// TLS, and verify the server certificate against `--tls-ca` plus the
    /// host name.
    VerifyFull,
}

/// TLS connect options: the mode, the `verify-full` trust anchor, and the
/// optional mTLS client-certificate pair.
#[derive(Debug, Clone, Default)]
pub struct TlsOpts {
    pub mode: SslMode,
    /// PEM CA bundle for [`SslMode::VerifyFull`].
    pub ca: Option<PathBuf>,
    /// PEM client-certificate chain to present for mTLS (`--tls-cert`). Requires
    /// [`Self::key`]; the server must be configured with `[tls] client_ca`.
    pub cert: Option<PathBuf>,
    /// PEM private key for [`Self::cert`] (`--tls-key`).
    pub key: Option<PathBuf>,
}

/// The blocking duplex transport a [`Client`] runs over — plain TCP or the
/// rustls-wrapped stream. Blanket-implemented; reads are buffered by the
/// `BufReader` around it, writes pass straight through (`BufReader` does not
/// buffer writes).
trait Transport: Read + Write + Send {}
impl<T: Read + Write + Send> Transport for T {}

/// A negotiated transport paired with the connection's `tls-server-end-point`
/// channel binding (`None` off TLS or for an unbindable certificate) — what
/// [`negotiate_tls`] hands back to [`Client::connect`].
type NegotiatedTransport = (Box<dyn Transport>, Option<Vec<u8>>);

/// A live connection running the simple-query protocol.
pub struct Client {
    stream: BufReader<Box<dyn Transport>>,
    /// Transaction status byte from the last `ReadyForQuery`:
    /// `I` idle, `T` in a transaction, `E` in a failed transaction.
    txn_status: u8,
    /// The connection's RFC 5929 `tls-server-end-point` channel binding (STL-334),
    /// computed once from the server certificate the TLS handshake negotiated.
    /// `Some` ⇒ over TLS with a bindable leaf certificate, so SCRAM prefers
    /// `SCRAM-SHA-256-PLUS` and folds these bytes into `c=`; `None` ⇒ plaintext,
    /// or a certificate whose signature hash we do not bind (plain SCRAM then).
    channel_binding: Option<Vec<u8>>,
    /// The SCRAM mechanism this connection authenticated with (STL-334), surfaced
    /// by `\conninfo`: `Some("SCRAM-SHA-256-PLUS")` when channel binding was
    /// negotiated over TLS, `Some("SCRAM-SHA-256")` for plain SCRAM, `None` for a
    /// trust-auth connection that ran no SASL exchange.
    scram_mechanism: Option<&'static str>,
}

impl Client {
    /// Connect — negotiating TLS per `tls` — and complete the startup handshake.
    ///
    /// `password` is used only if the server requests SCRAM authentication
    /// (`auth = "scram"`); a trust-auth server ignores it. When the server
    /// requests SCRAM and `password` is `None`, this fails with
    /// [`PasswordRequired`] so the caller can prompt and retry.
    ///
    /// # Errors
    /// Fails if the TCP connect or TLS negotiation fails, the server requests an
    /// authentication method the shell does not speak (or SCRAM without a
    /// password), the SCRAM exchange fails (wrong password / unknown user /
    /// unverifiable server signature), or startup itself returns an
    /// `ErrorResponse`.
    pub fn connect(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        tls: &TlsOpts,
        password: Option<&str>,
    ) -> anyhow::Result<Self> {
        let stream = TcpStream::connect((host, port))
            .with_context(|| format!("connecting to {host}:{port}"))?;
        stream.set_nodelay(true).ok();
        // The TLS path also yields the channel binding for the negotiated
        // certificate (STL-334); plaintext has none.
        let (transport, channel_binding) = match tls.mode {
            SslMode::Disable => (Box::new(stream) as Box<dyn Transport>, None),
            _ => negotiate_tls(stream, host, tls)?,
        };
        let mut client = Self {
            stream: BufReader::new(transport),
            txn_status: b'I',
            channel_binding,
            scram_mechanism: None,
        };

        client.send(0, &startup_payload(user, database))?;
        loop {
            let (kind, payload) = client.read_message()?;
            match kind {
                MSG_AUTHENTICATION => {
                    let code = be_i32(&payload, 0).context("malformed Authentication")?;
                    match code {
                        // AuthenticationOk — trust auth, or the tail of a SCRAM
                        // exchange that already succeeded.
                        AUTH_OK => {}
                        AUTH_SASL => {
                            let Some(password) = password else {
                                return Err(PasswordRequired {
                                    user: user.to_owned(),
                                }
                                .into());
                            };
                            let mechanisms =
                                payload.get(4..).context("AuthenticationSASL truncated")?;
                            client.scram_authenticate(password, mechanisms)?;
                        }
                        other => bail!(
                            "server requested authentication (type {other}); \
                             stele shell supports trust and SCRAM-SHA-256 only"
                        ),
                    }
                }
                MSG_READY_FOR_QUERY => {
                    client.txn_status = payload.first().copied().unwrap_or(b'I');
                    return Ok(client);
                }
                MSG_ERROR_RESPONSE => bail!("startup failed — {}", parse_error(&payload)),
                // ParameterStatus, BackendKeyData, notices: informational.
                _ => {}
            }
        }
    }

    /// Run one simple-query round-trip (which may carry several statements)
    /// and collect a [`Reply`] per statement, in order.
    ///
    /// # Errors
    /// Only on transport failure; SQL errors come back as [`Reply::Error`].
    pub fn simple_query(&mut self, sql: &str) -> anyhow::Result<Vec<Reply>> {
        let mut body = Vec::with_capacity(sql.len() + 1);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        self.send(MSG_QUERY, &body)?;

        let mut replies = Vec::new();
        let mut current: Option<ResultSet> = None;
        loop {
            let (kind, payload) = self.read_message()?;
            match kind {
                MSG_ROW_DESCRIPTION => {
                    current = Some(ResultSet {
                        columns: parse_row_description(&payload)?,
                        rows: Vec::new(),
                        stats: None,
                    });
                }
                MSG_NOTICE_RESPONSE => {
                    // The query-stats trailer ([STL-201]) rides a NoticeResponse,
                    // which shares the ErrorResponse field layout — its message
                    // ('M') field carries the stats line. Attach it to the result
                    // set it annotates (it arrives after the rows, before
                    // CommandComplete); any other notice parses to `None` and is
                    // ignored.
                    if let Some(set) = current.as_mut()
                        && let Some(stats) =
                            QueryStats::parse_notice(&parse_error(&payload).message)
                    {
                        set.stats = Some(stats);
                    }
                }
                MSG_DATA_ROW => {
                    let set = current
                        .as_mut()
                        .context("DataRow arrived before RowDescription")?;
                    set.rows.push(parse_data_row(&payload)?);
                }
                MSG_COMMAND_COMPLETE => {
                    // A row-returning statement renders as its rows; the tag
                    // (`SELECT n`) is implied by the row count. Anything else
                    // surfaces its tag.
                    replies.push(match current.take() {
                        Some(set) => Reply::Rows(set),
                        None => Reply::Command(read_cstring(&payload, &mut 0)?),
                    });
                }
                MSG_EMPTY_QUERY_RESPONSE => replies.push(Reply::Empty),
                MSG_ERROR_RESPONSE => {
                    // The statement (and the rest of the batch) failed, but the
                    // stream stays framed: drain to ReadyForQuery as usual.
                    current = None;
                    replies.push(Reply::Error(parse_error(&payload)));
                }
                MSG_READY_FOR_QUERY => {
                    self.txn_status = payload.first().copied().unwrap_or(b'I');
                    return Ok(replies);
                }
                // ParameterStatus / BackendKeyData and any other mid-stream
                // informational message: skip.
                _ => {}
            }
        }
    }

    /// Transaction status from the last `ReadyForQuery` (`I` / `T` / `E`).
    pub const fn txn_status(&self) -> u8 {
        self.txn_status
    }

    /// The SCRAM mechanism this connection authenticated with, if any — what
    /// `\conninfo` reports ([STL-334]). `Some("SCRAM-SHA-256-PLUS")` over a
    /// channel-bound TLS connection, `Some("SCRAM-SHA-256")` for plain SCRAM,
    /// `None` for a trust-auth connection.
    ///
    /// [STL-334]: https://allegromusic.atlassian.net/browse/STL-334
    pub const fn scram_mechanism(&self) -> Option<&'static str> {
        self.scram_mechanism
    }

    /// Write one frontend message. `kind == 0` means the untyped startup shape
    /// (length + payload, no message-type byte).
    ///
    /// Writes go through the `BufReader`'s inner transport directly — the
    /// protocol is strictly request-response, so there is never buffered
    /// *read* data in flight when we write.
    fn send(&mut self, kind: u8, body: &[u8]) -> anyhow::Result<()> {
        let len = i32::try_from(body.len() + 4).context("message too large")?;
        let mut frame = Vec::with_capacity(body.len() + 5);
        if kind != 0 {
            frame.push(kind);
        }
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(body);
        self.stream
            .get_mut()
            .write_all(&frame)
            .context("writing to server")
    }

    /// Read one backend message: type byte + length-prefixed payload.
    fn read_message(&mut self) -> anyhow::Result<(u8, Vec<u8>)> {
        let mut head = [0_u8; 5];
        self.stream
            .read_exact(&mut head)
            .context("reading from server (connection closed?)")?;
        let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]);
        let body_len = usize::try_from(len)
            .ok()
            .and_then(|l| l.checked_sub(4))
            .context("malformed message length")?;
        if body_len > MAX_MESSAGE_LEN {
            bail!("backend message of {body_len} bytes exceeds the sanity limit");
        }
        let mut payload = vec![0_u8; body_len];
        self.stream
            .read_exact(&mut payload)
            .context("reading message payload")?;
        Ok((head[0], payload))
    }

    /// Run the client half of the SASL SCRAM-SHA-256 exchange ([STL-296], RFC
    /// 5802 / RFC 7677): pick the mechanism, send the client-first message,
    /// prove possession of `password` against the server's challenge, and —
    /// before trusting the connection — **verify the server's signature**, which
    /// proves the peer holds this user's stored verifier (mutual authentication,
    /// so an impostor that could not is rejected here even though our proof would
    /// have satisfied it). `mechanisms` is the NUL-separated mechanism list from
    /// `AuthenticationSASL`. Returns once `AuthenticationSASLFinal` verifies; the
    /// trailing `AuthenticationOk` is consumed by the [`connect`](Self::connect)
    /// loop.
    fn scram_authenticate(&mut self, password: &str, mechanisms: &[u8]) -> anyhow::Result<()> {
        // Prefer SCRAM-SHA-256-PLUS when this connection carries a channel binding
        // and the server advertises it; otherwise plain SCRAM (STL-334). Cloned out
        // of `self` so the borrow does not outlive the `&mut self` wire writes.
        let binding = self.channel_binding.clone();
        let (mechanism, cb) = select_mechanism(mechanisms, binding.as_deref())?;

        // --- C: SASLInitialResponse — mechanism + client-first-message.
        let client_nonce = client_nonce()?;
        let client_first = scram_client_first(cb.gs2_header, &client_nonce);
        let mut initial = Vec::with_capacity(mechanism.len() + 5 + client_first.len());
        initial.extend_from_slice(mechanism.as_bytes());
        initial.push(0);
        initial.extend_from_slice(
            &i32::try_from(client_first.len())
                .context("client-first message too large")?
                .to_be_bytes(),
        );
        initial.extend_from_slice(client_first.as_bytes());
        self.send(MSG_SASL, &initial)?;

        // --- S: AuthenticationSASLContinue — server-first-message.
        let server_first = self.read_sasl_message(AUTH_SASL_CONTINUE)?;
        let (server_nonce, salt, iterations) = parse_server_first(&server_first)?;
        // The server nonce must *extend* the one we sent, or it is not answering
        // this exchange (a stale or spoofed challenge).
        if !server_nonce.starts_with(&client_nonce) || server_nonce.len() == client_nonce.len() {
            bail!("SCRAM server nonce did not extend the client nonce");
        }

        // --- C: SASLResponse — the client-final-message and the server signature
        // to expect back, both folded over the same AuthMessage. Under PLUS the
        // channel binding rides the `c=` value, binding the proof to the server
        // certificate the TLS handshake negotiated.
        let (client_final, expected_sig) = scram_client_final(
            password,
            cb,
            &client_nonce,
            &server_first,
            &server_nonce,
            &salt,
            iterations,
        );
        self.send(MSG_SASL, client_final.as_bytes())?;

        // --- S: AuthenticationSASLFinal — verify the server signature (`v=`).
        let server_final = self.read_sasl_message(AUTH_SASL_FINAL)?;
        let server_sig_b64 = server_final
            .strip_prefix("v=")
            .context("malformed SCRAM server-final message (no v=)")?;
        let server_sig =
            scram::b64_decode(server_sig_b64).context("server signature is not valid base64")?;
        if !scram::ct_eq(&server_sig, &expected_sig) {
            bail!(
                "SCRAM server signature did not verify — the server does not hold this user's \
                 password (possible impostor or man-in-the-middle)"
            );
        }
        self.scram_mechanism = Some(mechanism);
        Ok(())
    }

    /// Read one backend message mid-SASL and return the SCRAM text of an
    /// `Authentication` ('R') message whose code is `expected`. An
    /// `ErrorResponse` (wrong password / unknown user — SQLSTATE `28P01`) is
    /// surfaced as a typed [`AuthFailed`], so an interactive caller can re-prompt;
    /// any other message is a protocol violation.
    fn read_sasl_message(&mut self, expected: i32) -> anyhow::Result<String> {
        let (kind, payload) = self.read_message()?;
        match kind {
            MSG_AUTHENTICATION => {
                let code = be_i32(&payload, 0).context("malformed Authentication during SASL")?;
                if code != expected {
                    bail!(
                        "unexpected authentication code {code} during SASL (expected {expected})"
                    );
                }
                let body = payload
                    .get(4..)
                    .context("Authentication message truncated")?;
                String::from_utf8(body.to_vec())
                    .context("SCRAM message from server is not valid UTF-8")
            }
            MSG_ERROR_RESPONSE => Err(AuthFailed {
                error: parse_error(&payload),
            }
            .into()),
            other => bail!("unexpected message type {other:#04x} during SASL authentication"),
        }
    }
}

// ---------------------------------------------------------------------------
// TLS negotiation (STL-251)
// ---------------------------------------------------------------------------

/// Send the `SSLRequest`, and on `S` run the rustls handshake over the socket,
/// returning the encrypted transport and the connection's `tls-server-end-point`
/// channel binding (STL-334) — `Some` for a bindable negotiated certificate,
/// `None` otherwise. On a `Prefer` fallback to plaintext (`N`) there is no
/// binding.
///
/// `tls.mode` is never [`SslMode::Disable`] here (the caller short-circuits it).
fn negotiate_tls(
    mut stream: TcpStream,
    host: &str,
    tls: &TlsOpts,
) -> anyhow::Result<NegotiatedTransport> {
    let mode = tls.mode;
    let mut request = [0_u8; 8];
    request[..4].copy_from_slice(&8_i32.to_be_bytes());
    request[4..].copy_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
    stream.write_all(&request).context("sending SSLRequest")?;

    let mut answer = [0_u8; 1];
    stream
        .read_exact(&mut answer)
        .context("reading SSLRequest answer")?;
    match answer[0] {
        b'S' => {
            let config = tls_config(tls)?;
            let name = ServerName::try_from(host.to_owned())
                .with_context(|| format!("{host:?} is not a valid TLS server name"))?;
            let mut conn = rustls::ClientConnection::new(Arc::new(config), name)
                .context("initializing TLS")?;
            // The handshake would otherwise run lazily on the first read/write;
            // drive it to completion now so the negotiated server certificate is
            // available (`peer_certificates()` is populated only post-handshake)
            // for the channel binding. The socket is blocking, so `complete_io`
            // runs the handshake fully; a failure — an untrusted certificate under
            // verify-full, or an mTLS server we cannot satisfy — surfaces here with
            // a precise context instead of as a later startup transport error.
            conn.complete_io(&mut stream)
                .context("completing the TLS handshake")?;
            let channel_binding = conn
                .peer_certificates()
                .and_then(<[_]>::first)
                .and_then(|leaf| endpoint_channel_binding(leaf.as_ref()));
            Ok((
                Box::new(rustls::StreamOwned::new(conn, stream)),
                channel_binding,
            ))
        }
        b'N' if mode == SslMode::Prefer => Ok((Box::new(stream), None)),
        b'N' => bail!(
            "server refused TLS but --tls {} requires it (configure [tls] on the server, \
             or connect with --tls prefer/disable)",
            if mode == SslMode::Require {
                "require"
            } else {
                "verify-full"
            },
        ),
        other => bail!("unexpected SSLRequest answer byte {other:#04x}"),
    }
}

/// The rustls client config for `tls`: the server-verification posture from the
/// mode, then the mTLS client certificate (if `--tls-cert`/`--tls-key` are set).
fn tls_config(tls: &TlsOpts) -> anyhow::Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .context("selecting TLS protocol versions")?;
    // Resolve the server-trust posture; both arms land in the `WantsClientCert`
    // state, so the client-auth decision is made once, after the match.
    let builder =
        match tls.mode {
            SslMode::Disable => unreachable!("disable never negotiates"),
            // Encrypt-only, exactly libpq's `require`/`prefer`: any certificate is
            // accepted, so this defeats passive eavesdropping but NOT an active
            // man-in-the-middle. `verify-full` is the authenticated mode.
            SslMode::Prefer | SslMode::Require => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(
                    provider.signature_verification_algorithms,
                ))),
            SslMode::VerifyFull => {
                let ca = tls
                    .ca
                    .as_deref()
                    .context("--tls verify-full requires --tls-ca <ca.pem>")?;
                let mut roots = rustls::RootCertStore::empty();
                for cert in CertificateDer::pem_file_iter(ca)
                    .with_context(|| format!("reading CA bundle {}", ca.display()))?
                {
                    roots
                        .add(cert.context("parsing CA certificate")?)
                        .context("adding CA certificate to the trust store")?;
                }
                builder.with_root_certificates(roots)
            }
        };
    finish_client_auth(builder, tls.cert.as_deref(), tls.key.as_deref())
}

/// Finish the client config by deciding whether to present an mTLS client
/// certificate ([STL-292]). Both `--tls-cert` and `--tls-key` must be given
/// together — only one is a clear configuration error, raised *before* any file
/// is read. The CLI enforces this pairing at parse time too (clap `requires`);
/// this guard also covers callers that build [`TlsOpts`] directly. The PEM
/// loading mirrors the server-side `ServerTls::load`.
///
/// [STL-292]: https://allegromusic.atlassian.net/browse/STL-292
fn finish_client_auth(
    builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    cert: Option<&Path>,
    key: Option<&Path>,
) -> anyhow::Result<rustls::ClientConfig> {
    match (cert, key) {
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(_), None) => bail!("--tls-cert requires --tls-key (the client certificate's key)"),
        (None, Some(_)) => {
            bail!("--tls-key requires --tls-cert (the client certificate to present)")
        }
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .with_context(|| format!("reading client certificate {}", cert.display()))?
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("parsing client certificate {}", cert.display()))?;
            let key = PrivateKeyDer::from_pem_file(key)
                .with_context(|| format!("reading client key {}", key.display()))?;
            builder
                .with_client_auth_cert(chain, key)
                .context("configuring the mTLS client certificate")
        }
    }
}

/// The `require`-mode verifier: accepts any server certificate (no chain or
/// host-name check) while still verifying the handshake *signatures*, so the
/// peer must at least hold the key for the certificate it presented. This is
/// the standard rustls "danger" pattern and matches libpq `sslmode=require`.
#[derive(Debug)]
struct AcceptAnyServerCert(rustls::crypto::WebPkiSupportedAlgorithms);

impl ServerCertVerifier for AcceptAnyServerCert {
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
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Best-effort goodbye; the server treats a plain close fine too.
        let _ = self.send(MSG_TERMINATE, &[]);
    }
}

/// The `StartupMessage` body: protocol version + `user` / `database` params, plus
/// the `stele_stats` opt-in for the query-stats trailer ([STL-201]).
///
/// The shell always opts in so the channel is open; whether the footer is *drawn*
/// is the client-side `--stats` setting. The server delivers the trailer to any
/// client that sends this parameter — but no mainstream driver does, so psql and
/// the JDBC / psycopg gate never receive it.
fn startup_payload(user: &str, database: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    for (key, value) in [
        ("user", user),
        ("database", database),
        ("stele_stats", "on"),
    ] {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    body
}

// ---------------------------------------------------------------------------
// SASL SCRAM-SHA-256 helpers (STL-296)
// ---------------------------------------------------------------------------

/// Whether the NUL-separated SASL mechanism list offers `wanted`.
fn mechanism_offered(list: &[u8], wanted: &str) -> bool {
    list.split(|&b| b == 0).any(|m| m == wanted.as_bytes())
}

/// The channel-binding inputs the SCRAM client messages carry (STL-334): the gs2
/// header (always) and, under `SCRAM-SHA-256-PLUS`, the `tls-server-end-point`
/// data folded into `c=`. Bundling the two keeps them consistent — a `p=` header
/// always travels with cbind-data, an `n,,` header never does — and is what
/// [`scram_client_first`] and [`scram_client_final`] thread through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ChannelBinding<'a> {
    /// The gs2 header — `n,,` plain, `p=tls-server-end-point,,` under PLUS.
    gs2_header: &'static str,
    /// The cbind-data appended after the header in `c=` — `Some` only under PLUS.
    cbind: Option<&'a [u8]>,
}

/// Choose the SASL mechanism and its [`ChannelBinding`] for this connection
/// (STL-334): `SCRAM-SHA-256-PLUS` with the `tls-server-end-point` binding when
/// the transport carries one (`binding` is `Some`) and the server advertises
/// PLUS; otherwise plain `SCRAM-SHA-256` with the `n` header and no binding — the
/// path used off TLS, against a plain-only server, or for a certificate we cannot
/// bind.
///
/// The shell never sends the `y` flag: it always claims "no channel binding"
/// (`n`) when it does not use PLUS, rather than "you didn't offer it", which a
/// PLUS-advertising server refuses as a downgrade (RFC 5802 §6). `Err` only when
/// the server offers neither mechanism the shell speaks.
fn select_mechanism<'a>(
    mechanisms: &[u8],
    binding: Option<&'a [u8]>,
) -> anyhow::Result<(&'static str, ChannelBinding<'a>)> {
    match binding {
        Some(data) if mechanism_offered(mechanisms, SCRAM_SHA_256_PLUS) => Ok((
            SCRAM_SHA_256_PLUS,
            ChannelBinding {
                gs2_header: GS2_HEADER_PLUS,
                cbind: Some(data),
            },
        )),
        _ if mechanism_offered(mechanisms, SCRAM_SHA_256) => Ok((
            SCRAM_SHA_256,
            ChannelBinding {
                gs2_header: GS2_HEADER_PLAIN,
                cbind: None,
            },
        )),
        _ => bail!(
            "server offered SASL mechanisms {:?}, but stele shell speaks {SCRAM_SHA_256} \
             (and {SCRAM_SHA_256_PLUS} over TLS)",
            String::from_utf8_lossy(mechanisms)
        ),
    }
}

/// The RFC 5929 `tls-server-end-point` channel binding for the negotiated server
/// certificate: the DER-encoded leaf hashed with the digest of its signature
/// algorithm. Mirrors the server's selection (`stele_pgwire::tls`) so both sides
/// compute the identical `c=`.
///
/// RFC 5929 §4.1 derives the hash from the certificate's `signatureAlgorithm`.
/// This binds the SHA-256 case — an RSA-SHA-256 or ECDSA-SHA-256 leaf, every
/// modern certificate — and returns `None` for any other signature hash
/// (SHA-384/512 is the filed follow-up STL-330; legacy MD5/SHA-1 is not worth a
/// path). `None` means the client degrades to plain SCRAM over the encrypted
/// channel rather than computing a binding the server would not — the safe
/// degrade, and the symmetric mirror of the server advertising PLUS only for the
/// certificates it can bind.
fn endpoint_channel_binding(cert_der: &[u8]) -> Option<Vec<u8>> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    let sig_alg = &cert.signature_algorithm.algorithm;
    (*sig_alg == OID_PKCS1_SHA256WITHRSA || *sig_alg == OID_SIG_ECDSA_WITH_SHA256)
        .then(|| sha256(cert_der).as_bytes().to_vec())
}

/// The SCRAM client-first message — `<gs2-header>n=,r=<nonce>`: the gs2 header
/// (`n,,` for plain SCRAM, `p=tls-server-end-point,,` under channel binding), an
/// empty `n=` username (the identity is the startup `user`, as Postgres does), and
/// the client nonce.
fn scram_client_first(gs2_header: &str, client_nonce: &str) -> String {
    format!("{gs2_header}n=,r={client_nonce}")
}

/// Build the client-final message and the `ServerSignature` to expect back,
/// both folded over the one AuthMessage (RFC 5802 §3:
/// `client-first-bare "," server-first "," client-final-without-proof`). Pure —
/// the I/O method is a thin frame around it, so the proof/signature math is unit-
/// tested against `stele_common::scram`'s server side without a socket.
fn scram_client_final(
    password: &str,
    cb: ChannelBinding<'_>,
    client_nonce: &str,
    server_first: &str,
    server_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> (String, [u8; 32]) {
    // `c=` is base64 of the gs2 header followed by the channel-binding data
    // (RFC 5802 §6): just the header under plain SCRAM (`n,,` → `biws`), the header
    // plus the `tls-server-end-point` bytes under PLUS — exactly what the server
    // recomputes and checks, so a proof captured against a different TLS endpoint
    // fails the server's `c=` comparison.
    let mut cbind_input = cb.gs2_header.as_bytes().to_vec();
    if let Some(data) = cb.cbind {
        cbind_input.extend_from_slice(data);
    }
    let channel_binding = scram::b64_encode(&cbind_input);
    let without_proof = format!("c={channel_binding},r={server_nonce}");
    let auth_message = format!("n=,r={client_nonce},{server_first},{without_proof}");
    let proof = scram::client_proof(password, salt, iterations, auth_message.as_bytes());
    let client_final = format!("{without_proof},p={}", scram::b64_encode(&proof));
    let server_sig =
        ScramVerifier::derive(password, salt, iterations).server_signature(auth_message.as_bytes());
    (client_final, server_sig)
}

/// A fresh client nonce: 18 bytes of OS entropy as base64 (24 characters), all
/// within the printable RFC 5802 nonce alphabet. Fresh per exchange, so each
/// proof signs a distinct combined nonce — a captured exchange replays nothing.
fn client_nonce() -> anyhow::Result<String> {
    let mut raw = [0_u8; CLIENT_NONCE_RAW_LEN];
    getrandom::fill(&mut raw).context("generating a SCRAM client nonce")?;
    Ok(scram::b64_encode(&raw))
}

/// Pull `r=` (combined nonce), `s=` (base64 salt), and `i=` (iteration count)
/// out of a SCRAM server-first-message. `Err` on any missing or malformed
/// attribute — server input is parsed strictly, not repaired.
fn parse_server_first(msg: &str) -> anyhow::Result<(String, Vec<u8>, u32)> {
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    for attr in msg.split(',') {
        if let Some(v) = attr.strip_prefix("r=") {
            nonce = Some(v.to_owned());
        } else if let Some(v) = attr.strip_prefix("s=") {
            salt = scram::b64_decode(v);
        } else if let Some(v) = attr.strip_prefix("i=") {
            // Reject `i=0` (and any non-numeric count): SCRAM/PBKDF2 requires at
            // least one iteration — 0 would silently degrade `scram::hi` to a
            // single round and deviates from RFC 5802.
            iterations = v.parse::<u32>().ok().filter(|&i| i > 0);
        }
    }
    match (nonce, salt, iterations) {
        (Some(nonce), Some(salt), Some(iterations)) => Ok((nonce, salt, iterations)),
        _ => bail!("malformed SCRAM server-first message: {msg:?}"),
    }
}

/// Columns (name + type OID) out of a `RowDescription` payload.
fn parse_row_description(payload: &[u8]) -> anyhow::Result<Vec<Column>> {
    let mut pos = 0;
    let nfields = be_u16(payload, &mut pos).context("malformed RowDescription")?;
    let mut columns = Vec::with_capacity(usize::from(nfields));
    for _ in 0..nfields {
        let name = read_cstring(payload, &mut pos).context("malformed RowDescription")?;
        // Fixed-width remainder of the field descriptor: table oid (4),
        // attnum (2), type oid (4), typlen (2), typmod (4), format (2).
        // OIDs are unsigned — a high-bit extension OID is legal on the wire.
        let type_oid = be_u32(payload, pos + 6).context("RowDescription truncated")?;
        pos += 18;
        if pos > payload.len() {
            bail!("RowDescription truncated");
        }
        columns.push(Column { name, type_oid });
    }
    Ok(columns)
}

/// Text-format cells out of a `DataRow` payload (`None` = NULL).
fn parse_data_row(payload: &[u8]) -> anyhow::Result<Vec<Option<String>>> {
    let mut pos = 0;
    let ncols = be_u16(payload, &mut pos).context("malformed DataRow")?;
    let mut cells = Vec::with_capacity(usize::from(ncols));
    for _ in 0..ncols {
        let len = be_i32(payload, pos).context("malformed DataRow")?;
        pos += 4;
        if len == -1 {
            cells.push(None);
            continue;
        }
        let len = usize::try_from(len).context("malformed DataRow cell length")?;
        let end = pos.checked_add(len).filter(|&e| e <= payload.len());
        let Some(end) = end else {
            bail!("DataRow cell overruns the payload");
        };
        cells.push(Some(
            String::from_utf8_lossy(&payload[pos..end]).into_owned(),
        ));
        pos = end;
    }
    Ok(cells)
}

/// Decode an `ErrorResponse` payload into its severity / SQLSTATE / message /
/// hint fields.
fn parse_error(payload: &[u8]) -> ServerError {
    let mut error = ServerError {
        severity: "ERROR".to_owned(),
        code: String::new(),
        message: "(no message)".to_owned(),
        hint: None,
    };
    let mut pos = 0;
    while let Some(&code) = payload.get(pos) {
        if code == 0 {
            break;
        }
        pos += 1;
        let Ok(value) = read_cstring(payload, &mut pos) else {
            break;
        };
        match code {
            b'S' => error.severity = value,
            b'C' => error.code = value,
            b'M' => error.message = value,
            b'H' => error.hint = Some(value),
            _ => {}
        }
    }
    error
}

/// A NUL-terminated string starting at `*pos`; advances past the terminator.
fn read_cstring(payload: &[u8], pos: &mut usize) -> anyhow::Result<String> {
    let rest = payload.get(*pos..).context("string runs off the payload")?;
    let nul = rest
        .iter()
        .position(|&b| b == 0)
        .context("unterminated string")?;
    let s = String::from_utf8_lossy(&rest[..nul]).into_owned();
    *pos += nul + 1;
    Ok(s)
}

/// Big-endian `u16` at `*pos`; advances.
fn be_u16(payload: &[u8], pos: &mut usize) -> Option<u16> {
    let bytes = payload.get(*pos..*pos + 2)?;
    *pos += 2;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

/// Big-endian `i32` at `at` (no advance — callers step manually).
fn be_i32(payload: &[u8], at: usize) -> Option<i32> {
    let bytes = payload.get(at..at + 4)?;
    Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Big-endian `u32` at `at` — for fields that are unsigned on the wire (OIDs).
fn be_u32(payload: &[u8], at: usize) -> Option<u32> {
    let bytes = payload.get(at..at + 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A plain-SCRAM [`ChannelBinding`] — gs2 `n,,`, no cbind-data.
    fn plain_cb() -> ChannelBinding<'static> {
        ChannelBinding {
            gs2_header: GS2_HEADER_PLAIN,
            cbind: None,
        }
    }

    /// A `SCRAM-SHA-256-PLUS` [`ChannelBinding`] over `data` — gs2
    /// `p=tls-server-end-point,,` with the endpoint bytes folded into `c=`.
    fn plus_cb(data: &[u8]) -> ChannelBinding<'_> {
        ChannelBinding {
            gs2_header: GS2_HEADER_PLUS,
            cbind: Some(data),
        }
    }

    /// `N` bytes of OS entropy — the seed for the throwaway test credentials.
    fn test_bytes<const N: usize>() -> [u8; N] {
        let mut bytes = [0u8; N];
        getrandom::fill(&mut bytes).expect("OS entropy");
        bytes
    }

    /// A throwaway SCRAM password from OS entropy. The proof/round-trip tests hold
    /// for any value, so generating it keeps a hard-coded credential — and CodeQL's
    /// `hard-coded-cryptographic-value` finding — out of the source, the same
    /// reason the pgwire `scram_plus_wire` tests generate theirs (STL-297).
    fn test_password() -> String {
        scram::b64_encode(&test_bytes::<12>())
    }

    /// A throwaway 16-byte SCRAM salt from OS entropy (see [`test_password`]).
    fn test_salt() -> [u8; 16] {
        test_bytes::<16>()
    }

    /// Build a `RowDescription` payload for the given `(name, type oid)`s.
    fn row_description(fields: &[(&str, u32)]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&u16::try_from(fields.len()).unwrap().to_be_bytes());
        for (name, oid) in fields {
            p.extend_from_slice(name.as_bytes());
            p.push(0);
            p.extend_from_slice(&[0_u8; 6]); // table oid + attnum
            p.extend_from_slice(&oid.to_be_bytes());
            p.extend_from_slice(&[0_u8; 8]); // typlen + typmod + format
        }
        p
    }

    /// Build a `DataRow` payload from text cells (`None` = NULL).
    fn data_row(cells: &[Option<&str>]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&u16::try_from(cells.len()).unwrap().to_be_bytes());
        for cell in cells {
            match cell {
                Some(text) => {
                    p.extend_from_slice(&i32::try_from(text.len()).unwrap().to_be_bytes());
                    p.extend_from_slice(text.as_bytes());
                }
                None => p.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        p
    }

    #[test]
    fn row_description_yields_names_and_type_oids_in_order() {
        let payload = row_description(&[("id", 23), ("balance", 20)]);
        let cols = parse_row_description(&payload).unwrap();
        assert_eq!(
            cols.iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect::<Vec<_>>(),
            [("id", 23), ("balance", 20)]
        );
    }

    #[test]
    fn truncated_row_description_is_an_error_not_a_panic() {
        let mut payload = row_description(&[("id", 23)]);
        payload.truncate(payload.len() - 4);
        assert!(parse_row_description(&payload).is_err());
    }

    #[test]
    fn data_row_decodes_text_and_null_cells() {
        let payload = data_row(&[Some("100"), None, Some("")]);
        assert_eq!(
            parse_data_row(&payload).unwrap(),
            vec![Some("100".to_owned()), None, Some(String::new())]
        );
    }

    #[test]
    fn data_row_cell_overrun_is_an_error() {
        let mut p = Vec::new();
        p.extend_from_slice(&1_u16.to_be_bytes());
        p.extend_from_slice(&100_i32.to_be_bytes()); // claims 100 bytes, has 2
        p.extend_from_slice(b"xy");
        assert!(parse_data_row(&p).is_err());
    }

    #[test]
    fn error_response_decodes_all_fields() {
        let mut p = Vec::new();
        for (code, value) in [
            (b'S', "ERROR"),
            (b'C', "42P01"),
            (b'M', "no such table"),
            (b'H', "Try \\dt."),
        ] {
            p.push(code);
            p.extend_from_slice(value.as_bytes());
            p.push(0);
        }
        p.push(0);
        let err = parse_error(&p);
        assert_eq!(err.severity, "ERROR");
        assert_eq!(err.code, "42P01");
        assert_eq!(err.message, "no such table");
        assert_eq!(err.hint.as_deref(), Some("Try \\dt."));
        assert_eq!(err.to_string(), "ERROR: no such table");
    }

    #[test]
    fn one_of_the_client_cert_pair_is_a_clear_error() {
        // Supplying only one of --tls-cert / --tls-key is a configuration error,
        // raised before any file is touched (so the nonexistent paths never
        // matter) — STL-292.
        let only_cert = TlsOpts {
            mode: SslMode::Require,
            ca: None,
            cert: Some(PathBuf::from("/nope/client.crt")),
            key: None,
        };
        let err = tls_config(&only_cert).unwrap_err().to_string();
        assert!(err.contains("--tls-key"), "{err}");

        let only_key = TlsOpts {
            mode: SslMode::Require,
            ca: None,
            cert: None,
            key: Some(PathBuf::from("/nope/client.key")),
        };
        let err = tls_config(&only_key).unwrap_err().to_string();
        assert!(err.contains("--tls-cert"), "{err}");
    }

    #[test]
    fn startup_payload_carries_protocol_and_params() {
        let p = startup_payload("alice", "stele");
        assert_eq!(&p[..4], &PROTOCOL_VERSION.to_be_bytes());
        // The shell always opts in to the query-stats trailer (STL-201).
        assert_eq!(
            &p[4..],
            b"user\0alice\0database\0stele\0stele_stats\0on\0\0"
        );
    }

    // -- SASL SCRAM-SHA-256 (STL-296) --------------------------------------

    #[test]
    fn mechanism_offered_finds_plain_scram_among_the_list() {
        // Plain server, and a TLS server that lists PLUS first — both offer it.
        assert!(mechanism_offered(b"SCRAM-SHA-256\0\0", "SCRAM-SHA-256"));
        assert!(mechanism_offered(
            b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0",
            "SCRAM-SHA-256"
        ));
        // A list that names only PLUS does not offer plain SCRAM (substring of a
        // longer name must not match).
        assert!(!mechanism_offered(
            b"SCRAM-SHA-256-PLUS\0\0",
            "SCRAM-SHA-256"
        ));
    }

    #[test]
    fn parse_server_first_extracts_nonce_salt_and_iterations() {
        let salt = b"\x00\x11\x22\x33\x44\x55\x66\x77";
        let msg = format!("r=abcdEFGH,s={},i=4096", scram::b64_encode(salt));
        let (nonce, parsed_salt, iterations) = parse_server_first(&msg).expect("parses");
        assert_eq!(nonce, "abcdEFGH");
        assert_eq!(parsed_salt, salt);
        assert_eq!(iterations, 4096);
    }

    #[test]
    fn parse_server_first_rejects_malformed_messages() {
        // Missing salt, missing iterations, a non-numeric count, a zero count
        // (PBKDF2 needs >= 1), and bad base64.
        for msg in [
            "r=abc,i=4096",
            "r=abc,s=AAAA",
            "r=abc,s=AAAA,i=lots",
            "r=abc,s=AAAA,i=0",
            "r=abc,s=!!,i=1",
        ] {
            assert!(parse_server_first(msg).is_err(), "{msg:?}");
        }
    }

    #[test]
    fn client_first_is_the_no_channel_binding_gs2_shape() {
        assert_eq!(
            scram_client_first(GS2_HEADER_PLAIN, "noncenonce"),
            "n,,n=,r=noncenonce"
        );
    }

    #[test]
    fn client_first_carries_the_plus_gs2_header_under_channel_binding() {
        // Under PLUS the gs2 header names the binding type; the bare part (the
        // identity + nonce) is identical to plain SCRAM.
        assert_eq!(
            scram_client_first(GS2_HEADER_PLUS, "noncenonce"),
            "p=tls-server-end-point,,n=,r=noncenonce"
        );
    }

    /// The mechanism/header/binding choice (STL-334): PLUS only with a binding AND
    /// the server's offer; plain `n` (never `y`) for every fallback; an error when
    /// the server speaks neither.
    #[test]
    fn select_mechanism_prefers_plus_only_when_bound_and_offered() {
        let binding = [7u8; 32];
        let both = b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0";
        let plain_only = b"SCRAM-SHA-256\0\0";

        // Over TLS with a binding, PLUS offered: prefer PLUS, fold the binding in.
        assert_eq!(
            select_mechanism(both, Some(&binding)).unwrap(),
            (SCRAM_SHA_256_PLUS, plus_cb(&binding))
        );
        // A binding we have, but the server lists only plain: plain `n`, no binding.
        assert_eq!(
            select_mechanism(plain_only, Some(&binding)).unwrap(),
            (SCRAM_SHA_256, plain_cb())
        );
        // TLS but no usable binding (unbindable cert), PLUS offered: still plain `n`
        // — never `y`, which a PLUS-advertising server treats as a downgrade.
        assert_eq!(
            select_mechanism(both, None).unwrap(),
            (SCRAM_SHA_256, plain_cb())
        );
        // Off TLS (no binding), plain only: the unchanged plain path.
        assert_eq!(
            select_mechanism(plain_only, None).unwrap(),
            (SCRAM_SHA_256, plain_cb())
        );
        // A server speaking neither (e.g. a future-only mechanism) is an error.
        assert!(select_mechanism(b"SCRAM-SHA-512\0\0", Some(&binding)).is_err());
    }

    /// `endpoint_channel_binding` mirrors the server: the SHA-256 of an
    /// ECDSA-SHA-256 leaf's DER (rcgen's default), and `None` for non-certificate
    /// bytes (never a panic — the certificate arrives over the wire).
    #[test]
    fn endpoint_channel_binding_is_the_sha256_of_a_sha256_cert() {
        let key = rcgen::KeyPair::generate().expect("key");
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("self-sign");
        let der = cert.der().as_ref();

        let cbind = endpoint_channel_binding(der).expect("SHA-256 binding");
        assert_eq!(cbind, sha256(der).as_bytes().to_vec());
        assert_eq!(cbind.len(), 32, "SHA-256 is 32 bytes");
        assert!(endpoint_channel_binding(b"not a certificate").is_none());
    }

    /// The proof the client builds verifies against a verifier derived from the
    /// same password, and the server signature the client expects is exactly the
    /// one that verifier emits — the interop contract the server enforces, run
    /// here with the server played in-process by `stele_common::scram`.
    #[test]
    fn client_final_interoperates_with_the_server_verifier() {
        let password = "pencil";
        let salt = b"0123456789abcdef";
        let iterations = scram::DEFAULT_ITERATIONS;
        let client_nonce = "clientnonce";
        // The server appends its own entropy to the client nonce.
        let server_nonce = "clientnonceSERVERENTROPY";
        let server_first = format!(
            "r={server_nonce},s={},i={iterations}",
            scram::b64_encode(salt)
        );

        let (client_final, expected_sig) = scram_client_final(
            password,
            plain_cb(),
            client_nonce,
            &server_first,
            server_nonce,
            salt,
            iterations,
        );

        // The server re-derives AuthMessage from the bytes it received (the
        // client-first-bare it saw, its own server-first, and the without-proof
        // prefix of client-final) and checks the proof.
        let (without_proof, proof_b64) = client_final
            .rsplit_once(",p=")
            .expect("client-final has a proof");
        let proof: [u8; 32] = scram::b64_decode(proof_b64)
            .expect("proof base64")
            .try_into()
            .expect("32-byte proof");
        let auth_message = format!("n=,r={client_nonce},{server_first},{without_proof}");
        let verifier = ScramVerifier::derive(password, salt, iterations);
        assert!(
            verifier.verify_client_proof(auth_message.as_bytes(), &proof),
            "the client proof must satisfy the server verifier"
        );
        assert_eq!(
            expected_sig,
            verifier.server_signature(auth_message.as_bytes()),
            "the client must expect exactly the server's signature"
        );
    }

    /// The channel-binding interop oracle (STL-334): under PLUS the client's `c=`
    /// is exactly `base64(gs2-header || tls-server-end-point)` — the value the
    /// server recomputes (`stele_pgwire::scram`) — its proof verifies against the
    /// server verifier, and the expected signature matches. This is the cross-side
    /// `c=` agreement the wire check enforces, run with the server played
    /// in-process by `stele_common::scram`.
    #[test]
    fn plus_client_final_binds_to_the_endpoint_and_interoperates() {
        let password = test_password();
        let salt = test_salt();
        let iterations = scram::DEFAULT_ITERATIONS;
        let client_nonce = "clientnonce";
        let server_nonce = "clientnonceSERVERENTROPY";
        let server_first = format!(
            "r={server_nonce},s={},i={iterations}",
            scram::b64_encode(&salt)
        );
        // Stands in for the SHA-256 of the negotiated server certificate.
        let cbind = test_bytes::<32>();

        let (client_final, expected_sig) = scram_client_final(
            &password,
            plus_cb(&cbind),
            client_nonce,
            &server_first,
            server_nonce,
            &salt,
            iterations,
        );

        // The `c=` the client sends is exactly what the server folds and checks:
        // base64 of the gs2 header followed by the endpoint binding.
        let (without_proof, proof_b64) = client_final
            .rsplit_once(",p=")
            .expect("client-final has a proof");
        let mut expected_cbind = GS2_HEADER_PLUS.as_bytes().to_vec();
        expected_cbind.extend_from_slice(&cbind);
        assert_eq!(
            without_proof,
            format!("c={},r={server_nonce}", scram::b64_encode(&expected_cbind)),
            "the c= value must match the server's expected channel binding"
        );

        // The proof verifies against the verifier, and the client expects exactly
        // the server's signature back.
        let proof: [u8; 32] = scram::b64_decode(proof_b64)
            .expect("proof base64")
            .try_into()
            .expect("32-byte proof");
        let auth_message = format!("n=,r={client_nonce},{server_first},{without_proof}");
        let verifier = ScramVerifier::derive(&password, &salt, iterations);
        assert!(
            verifier.verify_client_proof(auth_message.as_bytes(), &proof),
            "the PLUS client proof must satisfy the server verifier"
        );
        assert_eq!(
            expected_sig,
            verifier.server_signature(auth_message.as_bytes()),
            "the client must expect exactly the server's signature"
        );
    }

    /// A `c=` computed against a *different* endpoint does not match the one for the
    /// genuine certificate — the property the server's `c=` check turns into a
    /// rejection of a MITM that terminates TLS with its own certificate (STL-334).
    #[test]
    fn plus_channel_binding_differs_by_endpoint() {
        let password = test_password();
        let salt = test_salt();
        let iterations = scram::DEFAULT_ITERATIONS;
        let client_nonce = "cn";
        let server_nonce = "cnSERVER";
        let server_first = format!(
            "r={server_nonce},s={},i={iterations}",
            scram::b64_encode(&salt)
        );
        let bind_a = [0x11u8; 32]; // the genuine server certificate's hash
        let bind_b = [0x22u8; 32]; // a MITM certificate's hash (distinct on purpose)

        let c_value = |cbind: &[u8]| {
            let (final_msg, _) = scram_client_final(
                &password,
                plus_cb(cbind),
                client_nonce,
                &server_first,
                server_nonce,
                &salt,
                iterations,
            );
            final_msg
                .split_once(',')
                .expect("c= is the first field")
                .0
                .to_owned()
        };

        // Different endpoints ⇒ different c=, so the server's comparison rejects the
        // wrong one; the genuine binding matches its own expected value.
        assert_ne!(c_value(&bind_a), c_value(&bind_b));
        let mut expected = GS2_HEADER_PLUS.as_bytes().to_vec();
        expected.extend_from_slice(&bind_a);
        assert_eq!(
            c_value(&bind_a),
            format!("c={}", scram::b64_encode(&expected))
        );
    }

    #[test]
    fn a_wrong_password_proof_is_refused_by_the_verifier() {
        let salt = test_salt();
        let iterations = scram::DEFAULT_ITERATIONS;
        let server_nonce = "cnSERVER";
        let server_first = format!(
            "r={server_nonce},s={},i={iterations}",
            scram::b64_encode(&salt)
        );
        // The client proves with the wrong password against a verifier for the
        // right one — the proof must not verify. The two differ by construction.
        let right = test_password();
        let wrong = format!("{right}-wrong");
        let (client_final, _) = scram_client_final(
            &wrong,
            plain_cb(),
            "cn",
            &server_first,
            server_nonce,
            &salt,
            iterations,
        );
        let (without_proof, proof_b64) = client_final.rsplit_once(",p=").unwrap();
        let proof: [u8; 32] = scram::b64_decode(proof_b64).unwrap().try_into().unwrap();
        let auth_message = format!("n=,r=cn,{server_first},{without_proof}");
        let verifier = ScramVerifier::derive(&right, &salt, iterations);
        assert!(!verifier.verify_client_proof(auth_message.as_bytes(), &proof));
    }

    #[test]
    fn password_required_error_names_the_user_and_points_at_pgpassword() {
        let err = PasswordRequired {
            user: "alice".to_owned(),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("alice"), "{rendered}");
        assert!(rendered.contains("PGPASSWORD"), "{rendered}");
    }

    #[test]
    fn auth_failed_flags_only_a_wrong_password_for_re_prompting() {
        // `rejected` is the decoded server error, not a credential — named so
        // CodeQL's `password`-substring heuristic does not misread the Display
        // round-trip below as cleartext logging of a secret.
        let rejected = AuthFailed {
            error: ServerError {
                severity: "FATAL".to_owned(),
                code: SQLSTATE_INVALID_PASSWORD.to_owned(),
                message: "authentication failed for user \"alice\"".to_owned(),
                hint: None,
            },
        };
        // A 28P01 is the wrong-password case an interactive shell re-prompts on,
        // and its rendering keeps the pre-STL-335 "authentication failed" wording.
        assert!(rejected.is_password_rejection());
        let rendered = rejected.to_string();
        assert!(rendered.contains("authentication failed"), "{rendered}");

        // A non-28P01 SASL-time error is not a password problem, so it is not
        // re-prompted — it surfaces as-is.
        let policy = AuthFailed {
            error: ServerError {
                severity: "FATAL".to_owned(),
                code: "28000".to_owned(),
                message: "connection requires TLS".to_owned(),
                hint: None,
            },
        };
        assert!(!policy.is_password_rejection());
    }
}
