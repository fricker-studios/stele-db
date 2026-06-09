//! A minimal, blocking Postgres wire-protocol client — just enough for the
//! `stele shell` REPL ([STL-185]).
//!
//! Speaks the **simple-query** slice of the protocol the Stele front end
//! serves: a `StartupMessage` (the server runs trust auth — `AuthenticationOk`
//! arrives unconditionally), then `Query` round-trips collecting
//! `RowDescription` / `DataRow` / `CommandComplete` until `ReadyForQuery`.
//! Everything arrives in text format, so cells decode straight to strings.
//!
//! Deliberately hand-rolled rather than pulling in a client crate:
//! `tokio-postgres` is pinned as a **dev-only** dependency workspace-wide (a
//! shipped `stele` binary must not grow its supply-chain surface), and the
//! ~hundred lines here double as a second, independent reading of the wire
//! format the `stele-pgwire` server emits.
//!
//! Errors split deliberately: a *SQL* failure (`ErrorResponse`) is data — it
//! comes back as [`Reply::Error`] and the connection stays usable — while a
//! *transport* failure (socket death, malformed frame) is `Err` and the caller
//! should drop the connection.
//!
//! [STL-185]: https://allegromusic.atlassian.net/browse/STL-185

use std::io::{BufReader, Read as _, Write as _};
use std::net::TcpStream;

use anyhow::{Context as _, bail};

// Backend message types this client consumes (the post-startup stream).
const MSG_AUTHENTICATION: u8 = b'R';
const MSG_READY_FOR_QUERY: u8 = b'Z';
const MSG_ERROR_RESPONSE: u8 = b'E';
const MSG_ROW_DESCRIPTION: u8 = b'T';
const MSG_DATA_ROW: u8 = b'D';
const MSG_COMMAND_COMPLETE: u8 = b'C';
const MSG_EMPTY_QUERY_RESPONSE: u8 = b'I';
// Frontend message types this client emits.
const MSG_QUERY: u8 = b'Q';
const MSG_TERMINATE: u8 = b'X';

/// pg-wire protocol version 3.0, as the `StartupMessage` carries it.
const PROTOCOL_VERSION: i32 = 196_608;

/// Upper bound on a single backend message body. The server's replies are
/// row-at-a-time and small; anything larger means a desynchronized stream.
const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024;

/// One result set: the `RowDescription` column names plus every `DataRow`,
/// cells decoded from text format (`None` = SQL `NULL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// What one statement inside a simple-query round-trip produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A row-returning statement: header + rows.
    Rows(ResultSet),
    /// A non-row statement's `CommandComplete` tag (`INSERT 0 1`, `CREATE TABLE`, …).
    Command(String),
    /// The server rejected the statement (`ErrorResponse`), already rendered
    /// as `SEVERITY: message`. The connection itself is still good.
    Error(String),
    /// An empty query string (`EmptyQueryResponse`).
    Empty,
}

/// A live connection running the simple-query protocol.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    /// Transaction status byte from the last `ReadyForQuery`:
    /// `I` idle, `T` in a transaction, `E` in a failed transaction.
    txn_status: u8,
}

impl Client {
    /// Connect and complete the startup handshake.
    ///
    /// # Errors
    /// Fails if the TCP connect fails, the server demands authentication
    /// (trust-only for now), or startup itself returns an `ErrorResponse`.
    pub fn connect(host: &str, port: u16, user: &str, database: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect((host, port))
            .with_context(|| format!("connecting to {host}:{port}"))?;
        stream.set_nodelay(true).ok();
        let reader = BufReader::new(stream.try_clone().context("cloning socket handle")?);
        let mut client = Self {
            reader,
            writer: stream,
            txn_status: b'I',
        };

        client.send(0, &startup_payload(user, database))?;
        loop {
            let (kind, payload) = client.read_message()?;
            match kind {
                MSG_AUTHENTICATION => {
                    let code = be_i32(&payload, 0).context("malformed Authentication")?;
                    if code != 0 {
                        bail!(
                            "server requested authentication (type {code}); \
                             stele shell only supports trust auth for now"
                        );
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
                    });
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
                // NoticeResponse / ParameterStatus mid-stream: skip.
                _ => {}
            }
        }
    }

    /// Transaction status from the last `ReadyForQuery` (`I` / `T` / `E`).
    pub const fn txn_status(&self) -> u8 {
        self.txn_status
    }

    /// Write one frontend message. `kind == 0` means the untyped startup shape
    /// (length + payload, no message-type byte).
    fn send(&mut self, kind: u8, body: &[u8]) -> anyhow::Result<()> {
        let len = i32::try_from(body.len() + 4).context("message too large")?;
        let mut frame = Vec::with_capacity(body.len() + 5);
        if kind != 0 {
            frame.push(kind);
        }
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(body);
        self.writer.write_all(&frame).context("writing to server")
    }

    /// Read one backend message: type byte + length-prefixed payload.
    fn read_message(&mut self) -> anyhow::Result<(u8, Vec<u8>)> {
        let mut head = [0_u8; 5];
        self.reader
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
        self.reader
            .read_exact(&mut payload)
            .context("reading message payload")?;
        Ok((head[0], payload))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Best-effort goodbye; the server treats a plain close fine too.
        let _ = self.send(MSG_TERMINATE, &[]);
    }
}

/// The `StartupMessage` body: protocol version + `user` / `database` params.
fn startup_payload(user: &str, database: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    for (key, value) in [("user", user), ("database", database)] {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    body
}

/// Column names out of a `RowDescription` payload.
fn parse_row_description(payload: &[u8]) -> anyhow::Result<Vec<String>> {
    let mut pos = 0;
    let nfields = be_u16(payload, &mut pos).context("malformed RowDescription")?;
    let mut columns = Vec::with_capacity(usize::from(nfields));
    for _ in 0..nfields {
        columns.push(read_cstring(payload, &mut pos).context("malformed RowDescription")?);
        // Fixed-width remainder of the field descriptor: table oid (4),
        // attnum (2), type oid (4), typlen (2), typmod (4), format (2).
        pos += 18;
        if pos > payload.len() {
            bail!("RowDescription truncated");
        }
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

/// Render an `ErrorResponse` payload as `SEVERITY: message`.
fn parse_error(payload: &[u8]) -> String {
    let mut severity = "ERROR".to_owned();
    let mut message = "(no message)".to_owned();
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
            b'S' => severity = value,
            b'M' => message = value,
            _ => {}
        }
    }
    format!("{severity}: {message}")
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `RowDescription` payload for the given column names.
    fn row_description(names: &[&str]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&u16::try_from(names.len()).unwrap().to_be_bytes());
        for name in names {
            p.extend_from_slice(name.as_bytes());
            p.push(0);
            p.extend_from_slice(&[0_u8; 18]); // oid/attnum/typoid/typlen/typmod/format
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
    fn row_description_yields_column_names_in_order() {
        let payload = row_description(&["id", "balance"]);
        assert_eq!(parse_row_description(&payload).unwrap(), ["id", "balance"]);
    }

    #[test]
    fn truncated_row_description_is_an_error_not_a_panic() {
        let mut payload = row_description(&["id"]);
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
    fn error_response_renders_severity_and_message() {
        let mut p = Vec::new();
        for (code, value) in [(b'S', "ERROR"), (b'C', "42P01"), (b'M', "no such table")] {
            p.push(code);
            p.extend_from_slice(value.as_bytes());
            p.push(0);
        }
        p.push(0);
        assert_eq!(parse_error(&p), "ERROR: no such table");
    }

    #[test]
    fn startup_payload_carries_protocol_and_params() {
        let p = startup_payload("alice", "stele");
        assert_eq!(&p[..4], &PROTOCOL_VERSION.to_be_bytes());
        assert_eq!(&p[4..], b"user\0alice\0database\0stele\0\0");
    }
}
