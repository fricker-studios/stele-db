//! Server-side SASL SCRAM-SHA-256 on the startup path ([STL-252]).
//!
//! Drives the Postgres `AuthenticationSASL` message family ([RFC 5802] /
//! [RFC 7677]) between the `StartupMessage` and the `AuthenticationOk` the OK
//! bundle opens with:
//!
//! ```text
//! S: AuthenticationSASL        ('R', code 10) — mechanism list
//! C: SASLInitialResponse       ('p')          — mechanism + client-first-message
//! S: AuthenticationSASLContinue('R', code 11) — server-first-message
//! C: SASLResponse              ('p')          — client-final-message
//! S: AuthenticationSASLFinal   ('R', code 12) — server-final-message (v=…)
//! ```
//!
//! The math lives in [`stele_common::scram`] (vendored, RFC-vectored); this
//! module owns the wire framing, the message grammar, and the policy:
//!
//! * `SCRAM-SHA-256` is always advertised. On a TLS connection whose certificate
//!   yields a `tls-server-end-point` binding, `SCRAM-SHA-256-PLUS` is advertised
//!   first ([STL-297], [RFC 5929]): the client proves the SASL exchange against
//!   the hash of the server certificate it actually saw, so a MITM that
//!   terminates TLS with a different certificate cannot relay the proof. Off TLS
//!   (no binding) only plain SCRAM is offered, and the `y` gs2 flag — "I support
//!   channel binding but you didn't advertise it" — is accepted, the RFC's
//!   downgrade rule for a server without PLUS. **When PLUS *was* advertised, the
//!   `y` flag is refused** instead: it means a MITM stripped the PLUS offer.
//! * The identity authenticated is the **startup `user` parameter**; the
//!   `n=` name inside the SCRAM message is ignored, as Postgres does.
//! * An unknown user runs a **doomed mock exchange** (fresh random verifier)
//!   and fails with the same `28P01` as a wrong password, so the error
//!   channel does not enumerate users.
//! * Server nonces are fresh OS entropy per exchange — a captured exchange
//!   replays nothing, because the proof signs the new server nonce.
//!
//! [RFC 5802]: https://www.rfc-editor.org/rfc/rfc5802
//! [RFC 5929]: https://www.rfc-editor.org/rfc/rfc5929
//! [RFC 7677]: https://www.rfc-editor.org/rfc/rfc7677
//! [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
//! [STL-297]: https://allegromusic.atlassian.net/browse/STL-297

use std::io;
use std::sync::PoisonError;

use bytes::{BufMut, BytesMut};
use stele_common::scram::{self, ScramVerifier};
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::{
    MSG_AUTHENTICATION, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_AUTHORIZATION,
    SQLSTATE_INVALID_PASSWORD, SQLSTATE_PROTOCOL_VIOLATION, SharedSession, StartupMessage, Wire,
    WireError, read_typed_message, write_error_response,
};

/// The mechanism always advertised and accepted.
pub(crate) const MECHANISM: &str = "SCRAM-SHA-256";

/// The channel-binding mechanism, advertised and accepted only on a TLS
/// connection that yields a `tls-server-end-point` binding (STL-297).
pub(crate) const MECHANISM_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// The one channel-binding type understood — the certificate-endpoint binding
/// of RFC 5929, the type every Postgres client uses.
const CBIND_TYPE: &str = "tls-server-end-point";

// AuthenticationSASL* type codes ('R' message, first Int32 of the payload).
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

/// Raw server-nonce entropy per exchange — 18 bytes → 24 base64 characters,
/// matching Postgres.
const SERVER_NONCE_RAW_LEN: usize = 18;

/// Run the SASL exchange and return the authenticated user name.
///
/// On any **server-initiated** refusal the client has already received a
/// `FATAL` `ErrorResponse` (the right SQLSTATE per failure shape — `28P01` for
/// a failed proof or unknown user, `08P01` for a malformed exchange,
/// `28000`/`0A000` for policy) and the connection unwinds with
/// [`WireError::AuthFailed`]. The one exception is the client closing the
/// connection mid-exchange (EOF): there is no peer to tell, so that unwinds as
/// [`WireError::Protocol`] with no reply.
/// `channel_binding` carries the connection's `tls-server-end-point` data
/// (RFC 5929) when the session runs over a TLS stream whose certificate
/// supports it: `Some` ⇒ advertise and accept `SCRAM-SHA-256-PLUS` and fold the
/// binding into the `c=` check; `None` (plaintext, or a certificate we cannot
/// bind) ⇒ plain `SCRAM-SHA-256` only.
pub(crate) async fn authenticate<S: Wire>(
    stream: &mut S,
    startup: &StartupMessage,
    session: &SharedSession,
    channel_binding: Option<&[u8]>,
) -> Result<String, WireError> {
    let Some(user) = startup
        .params
        .iter()
        .find(|(k, _)| k == "user")
        .map(|(_, v)| v.clone())
    else {
        return fail(
            stream,
            SQLSTATE_INVALID_AUTHORIZATION,
            "no PostgreSQL user name specified in startup packet",
        )
        .await;
    };

    // --- S: AuthenticationSASL — the mechanism list (PLUS first when channel
    // binding is available, so a capable client prefers it).
    write_auth_request(stream, AUTH_SASL, &sasl_mechanism_list(channel_binding)).await?;
    stream.flush().await?;

    // --- C: SASLInitialResponse — chosen mechanism + client-first-message.
    // Malformed framing is a protocol violation we refuse with a `FATAL`
    // `08P01`, the same as every other malformed-exchange path.
    let payload = read_sasl_message(stream).await?;
    let (mechanism, client_first_raw) = match parse_sasl_initial(&payload) {
        Ok(parsed) => parsed,
        Err(message) => return fail(stream, SQLSTATE_PROTOCOL_VIOLATION, message).await,
    };
    // PLUS is a valid choice only when we advertised it (channel binding present).
    let chose_plus = mechanism == MECHANISM_PLUS;
    if !(mechanism == MECHANISM || (chose_plus && channel_binding.is_some())) {
        return fail(
            stream,
            SQLSTATE_PROTOCOL_VIOLATION,
            &format!("client selected an invalid SASL authentication mechanism: {mechanism:?}"),
        )
        .await;
    }
    let client_first = match parse_client_first(&client_first_raw) {
        Ok(parsed) => parsed,
        Err(reject) => return fail(stream, reject.sqlstate, reject.message).await,
    };

    // Reconcile the gs2 channel-binding flag with the selected mechanism and what
    // the server advertised. `cbind_data` is `Some` exactly when channel binding
    // is in force, and carries the bytes that follow the gs2 header in `c=`.
    let cbind_data =
        match reconcile_channel_binding(&client_first.cbind_flag, chose_plus, channel_binding) {
            Ok(data) => data,
            Err(reject) => return fail(stream, reject.sqlstate, reject.message).await,
        };

    // --- Verifier lookup. An unknown user gets a fresh random verifier and a
    // full, doomed exchange (anti-enumeration — see [`lookup_verifier`]).
    let (verifier, known) = lookup_verifier(session, &user)?;

    // --- S: AuthenticationSASLContinue — server-first-message. The server
    // nonce appends fresh entropy to the client's: every exchange signs a
    // different nonce, which is what makes a captured exchange unreplayable.
    let (server_nonce, server_first) = server_first_message(&client_first.nonce, &verifier)?;
    write_auth_request(stream, AUTH_SASL_CONTINUE, server_first.as_bytes()).await?;
    stream.flush().await?;

    // --- C: SASLResponse — client-final-message.
    let payload = read_sasl_message(stream).await?;
    let Ok(client_final_raw) = String::from_utf8(payload.to_vec()) else {
        return fail(
            stream,
            SQLSTATE_PROTOCOL_VIOLATION,
            "SASL response is not valid UTF-8",
        )
        .await;
    };
    let client_final = match parse_client_final(&client_final_raw) {
        Ok(parsed) => parsed,
        Err(reject) => return fail(stream, reject.sqlstate, reject.message).await,
    };
    // `c=` is base64(gs2-header [|| cbind-data]). For plain SCRAM that is just
    // the gs2 header the client first sent; under channel binding the server's
    // `tls-server-end-point` data follows it, so a proof captured against a
    // different TLS endpoint — a MITM's — fails to match here.
    let mut expected_cbind = client_first.gs2_header.clone().into_bytes();
    if let Some(data) = cbind_data {
        expected_cbind.extend_from_slice(data);
    }
    if client_final.channel_binding != scram::b64_encode(&expected_cbind) {
        return fail(
            stream,
            SQLSTATE_PROTOCOL_VIOLATION,
            "malformed SCRAM message: channel binding does not match the initial gs2 header",
        )
        .await;
    }
    if client_final.nonce != server_nonce {
        // A stale nonce is exactly what a replayed capture presents.
        return fail(
            stream,
            SQLSTATE_PROTOCOL_VIOLATION,
            "malformed SCRAM message: nonce does not match this exchange",
        )
        .await;
    }

    // --- Verify the proof over AuthMessage = client-first-bare ","
    // server-first "," client-final-without-proof (RFC 5802 §3).
    let auth_message = format!(
        "{},{server_first},{}",
        client_first.bare, client_final.without_proof
    );
    // Verify before consulting `known`, so an unknown user costs the same
    // work as a wrong password.
    let proof_ok = verifier.verify_client_proof(auth_message.as_bytes(), &client_final.proof);
    if !(proof_ok && known) {
        debug!(user = %user, "SCRAM authentication failed");
        return fail(
            stream,
            SQLSTATE_INVALID_PASSWORD,
            &format!("password authentication failed for user \"{user}\""),
        )
        .await;
    }

    // --- S: AuthenticationSASLFinal — prove *we* hold the verifier.
    let server_final = format!(
        "v={}",
        scram::b64_encode(&verifier.server_signature(auth_message.as_bytes()))
    );
    write_auth_request(stream, AUTH_SASL_FINAL, server_final.as_bytes()).await?;
    // Flush like the earlier challenges: this is the last auth write before control
    // returns to the connection's message loop, and while that loop also flushes
    // before its first read, keeping the pattern here means no auth reply is ever
    // left buffered in a TLS stream — the deadlock class fixed for query replies.
    stream.flush().await?;
    debug!(user = %user, "SCRAM authentication succeeded");
    Ok(user)
}

/// The `AuthenticationSASL` mechanism list: `SCRAM-SHA-256-PLUS` first
/// (preferred) when channel binding is available, then plain `SCRAM-SHA-256`,
/// terminated by the empty name that ends the list.
fn sasl_mechanism_list(channel_binding: Option<&[u8]>) -> BytesMut {
    let mut mechanisms = BytesMut::new();
    if channel_binding.is_some() {
        mechanisms.put_slice(MECHANISM_PLUS.as_bytes());
        mechanisms.put_u8(0);
    }
    mechanisms.put_slice(MECHANISM.as_bytes());
    mechanisms.put_u8(0);
    mechanisms.put_u8(0);
    mechanisms
}

/// Look up the stored verifier for `user`, falling back to a fresh random
/// **mock** verifier when the user does not exist. The returned `bool` is
/// whether the user is real; the caller runs the full exchange either way and
/// only consults the flag at the final proof check, so an unknown user costs
/// the same work and fails identically to a wrong password (anti-enumeration).
fn lookup_verifier(
    session: &SharedSession,
    user: &str,
) -> Result<(ScramVerifier, bool), WireError> {
    let stored = {
        let guard = session.lock().unwrap_or_else(PoisonError::into_inner);
        guard.auth_verifier(user)
    };
    let known = stored.is_some();
    let verifier = match stored {
        Some(v) => v,
        None => mock_verifier()?,
    };
    Ok((verifier, known))
}

/// Build the server-first-message and the combined nonce it carries. The
/// server nonce appends fresh OS entropy to the client's, so every exchange
/// signs a different nonce — what makes a captured exchange unreplayable.
fn server_first_message(
    client_nonce: &str,
    verifier: &ScramVerifier,
) -> Result<(String, String), WireError> {
    let mut raw_nonce = [0u8; SERVER_NONCE_RAW_LEN];
    getrandom::fill(&mut raw_nonce).map_err(io::Error::from)?;
    let server_nonce = format!("{client_nonce}{}", scram::b64_encode(&raw_nonce));
    let server_first = format!(
        "r={server_nonce},s={},i={}",
        scram::b64_encode(&verifier.salt),
        verifier.iterations
    );
    Ok((server_nonce, server_first))
}

/// Write the `FATAL` refusal and unwind with [`WireError::AuthFailed`]. The
/// `Result`'s success type is generic so call sites can `return fail(…)`
/// from any context.
async fn fail<S: Wire, T>(stream: &mut S, sqlstate: &str, message: &str) -> Result<T, WireError> {
    write_error_response(stream, "FATAL", sqlstate, message).await?;
    stream.flush().await?;
    Err(WireError::AuthFailed)
}

/// A mock verifier for an unknown user: fresh random salt and key material,
/// so the doomed exchange is shaped exactly like a real one and the final
/// proof check can never pass.
fn mock_verifier() -> Result<ScramVerifier, WireError> {
    let mut salt = vec![0u8; scram::SALT_LEN];
    let mut stored_key = [0u8; 32];
    let mut server_key = [0u8; 32];
    getrandom::fill(&mut salt).map_err(io::Error::from)?;
    getrandom::fill(&mut stored_key).map_err(io::Error::from)?;
    getrandom::fill(&mut server_key).map_err(io::Error::from)?;
    Ok(ScramVerifier {
        iterations: scram::DEFAULT_ITERATIONS,
        salt,
        stored_key,
        server_key,
    })
}

/// Write one `AuthenticationSASL*` request: `'R' | len | code | data`.
async fn write_auth_request<S: Wire>(stream: &mut S, code: i32, data: &[u8]) -> io::Result<()> {
    let len = i32::try_from(8 + data.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SASL payload too large"))?;
    let mut buf = BytesMut::with_capacity(9 + data.len());
    buf.put_u8(MSG_AUTHENTICATION);
    buf.put_i32(len);
    buf.put_i32(code);
    buf.put_slice(data);
    stream.write_all(&buf).await
}

/// Read the next typed message and require it to be a SASL response (`'p'`).
///
/// A **live** out-of-grammar message (the client sent something other than a
/// SASL response) is a protocol violation we refuse with a `FATAL` `08P01`
/// before closing, so the client gets a concrete error rather than a silent
/// drop. The one quiet case is EOF: the client closed the connection, so there
/// is no peer to send a `FATAL` to — that unwinds as [`WireError::Protocol`].
async fn read_sasl_message<S: Wire>(stream: &mut S) -> Result<BytesMut, WireError> {
    let Some(msg) = read_typed_message(stream).await? else {
        return Err(WireError::Protocol(
            "connection closed during authentication",
        ));
    };
    if msg.kind != b'p' {
        return fail(
            stream,
            SQLSTATE_PROTOCOL_VIOLATION,
            "expected a SASL response during authentication",
        )
        .await;
    }
    Ok(msg.payload)
}

/// Split a `SASLInitialResponse` payload: NUL-terminated mechanism name, then
/// an `Int32`-length-prefixed initial client response (−1 = absent, which
/// SCRAM never sends). The `Err` is the protocol-violation message the caller
/// turns into a `FATAL` `08P01`.
fn parse_sasl_initial(payload: &[u8]) -> Result<(String, String), &'static str> {
    let nul = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or("SASLInitialResponse missing mechanism")?;
    let mechanism =
        String::from_utf8(payload[..nul].to_vec()).map_err(|_| "SASL mechanism is not UTF-8")?;
    let rest = &payload[nul + 1..];
    if rest.len() < 4 {
        return Err("SASLInitialResponse truncated");
    }
    let declared = i32::from_be_bytes(rest[..4].try_into().expect("4 bytes"));
    let body = &rest[4..];
    let expected = i32::try_from(body.len()).unwrap_or(i32::MAX);
    if declared < 0 || declared != expected {
        return Err("SASLInitialResponse length does not match its payload");
    }
    let initial =
        String::from_utf8(body.to_vec()).map_err(|_| "SCRAM client-first message is not UTF-8")?;
    Ok((mechanism, initial))
}

/// A policy/grammar refusal that owes the client a `FATAL` before closing.
#[derive(Debug, PartialEq, Eq)]
struct Reject {
    sqlstate: &'static str,
    message: &'static str,
}

/// The gs2 channel-binding flag of a `client-first-message` (RFC 5802 §7). The
/// policy that reconciles it with the selected mechanism lives in
/// [`authenticate`]; parsing only records what the client asked for.
#[derive(Debug, PartialEq, Eq)]
enum CbindFlag {
    /// `n` — the client does not support channel binding.
    NotSupported,
    /// `y` — the client supports channel binding but believes the server did
    /// not advertise a `-PLUS` mechanism.
    SupportedButUnused,
    /// `p=<type>` — the client requires channel binding of the named type.
    Required(String),
}

/// Reconcile the gs2 channel-binding flag with the selected mechanism and what
/// the server advertised (RFC 5802 §6, RFC 7677, STL-297). Returns the
/// cbind-data to fold into the client's `c=` check — `Some` exactly when channel
/// binding is in force — or a [`Reject`] the caller turns into a `FATAL`.
///
/// `chose_plus` is whether the client selected `SCRAM-SHA-256-PLUS`;
/// `channel_binding` is `Some` exactly when the server advertised PLUS (it has a
/// `tls-server-end-point` binding for this connection). The mechanism check in
/// [`authenticate`] guarantees `chose_plus ⇒ channel_binding.is_some()`.
fn reconcile_channel_binding<'a>(
    flag: &CbindFlag,
    chose_plus: bool,
    channel_binding: Option<&'a [u8]>,
) -> Result<Option<&'a [u8]>, Reject> {
    // A flag that contradicts the selected mechanism is malformed either way:
    // `p` demands PLUS, while `n`/`y` are only valid without it.
    let flag_matches_mechanism = matches!(flag, CbindFlag::Required(_)) == chose_plus;
    if !flag_matches_mechanism {
        return Err(Reject {
            sqlstate: SQLSTATE_PROTOCOL_VIOLATION,
            message: "malformed SCRAM message: channel-binding flag does not match the \
                      selected SASL mechanism",
        });
    }
    match flag {
        // `p=<type>`: the client requires channel binding of a type we implement.
        CbindFlag::Required(cb_type) if cb_type == CBIND_TYPE => Ok(channel_binding),
        CbindFlag::Required(_) => Err(Reject {
            sqlstate: SQLSTATE_PROTOCOL_VIOLATION,
            message: "unsupported SCRAM channel binding type",
        }),
        // `y`: the client supports channel binding but believes the server does
        // not advertise it. If we *did* advertise PLUS, a MITM stripped the offer
        // — RFC 5802 §6 downgrade detection (STL-297 flips the STL-252 accept on
        // TLS).
        CbindFlag::SupportedButUnused if channel_binding.is_some() => Err(Reject {
            sqlstate: SQLSTATE_PROTOCOL_VIOLATION,
            message: "channel binding required: this server advertised SCRAM-SHA-256-PLUS but \
                      the client requested none (possible man-in-the-middle downgrade)",
        }),
        // `n`, or `y` off TLS (no binding advertised): no channel binding in force.
        CbindFlag::NotSupported | CbindFlag::SupportedButUnused => Ok(None),
    }
}

/// The parsed `client-first-message` (RFC 5802 §7).
#[derive(Debug)]
struct ClientFirst {
    /// The gs2 header verbatim (e.g. `n,,` or `p=tls-server-end-point,,`) — the
    /// bytes `c=` must re-present base64-encoded (with the cbind-data appended
    /// under channel binding) in the client-final message.
    gs2_header: String,
    /// The channel-binding flag the header carried.
    cbind_flag: CbindFlag,
    /// `client-first-message-bare` verbatim — the first third of AuthMessage.
    bare: String,
    /// The client nonce (`r=`).
    nonce: String,
}

fn parse_client_first(raw: &str) -> Result<ClientFirst, Reject> {
    const MALFORMED: Reject = Reject {
        sqlstate: SQLSTATE_PROTOCOL_VIOLATION,
        message: "malformed SCRAM client-first message",
    };

    // gs2-cbind-flag, then the comma that ends it: 'n'/'y' are one byte;
    // 'p=<type>' runs the type to the next comma. `after_flag` resumes at that
    // comma so the authzid handling below is identical for every flag.
    let (cbind_flag, after_flag) = match raw.as_bytes().first() {
        Some(b'n') => (CbindFlag::NotSupported, raw.get(1..).ok_or(MALFORMED)?),
        Some(b'y') => (
            CbindFlag::SupportedButUnused,
            raw.get(1..).ok_or(MALFORMED)?,
        ),
        Some(b'p') => {
            let after_eq = raw.strip_prefix("p=").ok_or(MALFORMED)?;
            let comma = after_eq.find(',').ok_or(MALFORMED)?;
            let cb_type = &after_eq[..comma];
            if cb_type.is_empty() {
                return Err(MALFORMED);
            }
            (CbindFlag::Required(cb_type.to_owned()), &after_eq[comma..])
        }
        _ => return Err(MALFORMED),
    };
    let rest = after_flag.strip_prefix(',').ok_or(MALFORMED)?;
    let (authzid, bare) = rest.split_once(',').ok_or(MALFORMED)?;
    if !authzid.is_empty() {
        // `a=…`: Postgres refuses an authorization identity too.
        return Err(Reject {
            sqlstate: SQLSTATE_FEATURE_NOT_SUPPORTED,
            message: "client uses authorization identity, but it is not supported",
        });
    }
    let gs2_header = &raw[..raw.len() - bare.len()];

    // client-first-message-bare = [reserved-mext ","] username "," nonce [","…]
    let mut attrs = bare.split(',');
    let username = attrs.next().ok_or(MALFORMED)?;
    if username.starts_with("m=") {
        return Err(Reject {
            sqlstate: SQLSTATE_FEATURE_NOT_SUPPORTED,
            message: "SCRAM mandatory extensions are not supported",
        });
    }
    // The `n=` username is parsed for shape but deliberately ignored — the
    // identity authenticated is the startup `user` parameter, as in Postgres.
    if !username.starts_with("n=") {
        return Err(MALFORMED);
    }
    let nonce = attrs
        .next()
        .and_then(|a| a.strip_prefix("r="))
        .ok_or(MALFORMED)?;
    if nonce.is_empty()
        || !nonce
            .bytes()
            .all(|b| (0x21..=0x7E).contains(&b) && b != b',')
    {
        return Err(MALFORMED);
    }
    Ok(ClientFirst {
        gs2_header: gs2_header.to_owned(),
        cbind_flag,
        bare: bare.to_owned(),
        nonce: nonce.to_owned(),
    })
}

/// The parsed `client-final-message` (RFC 5802 §7).
#[derive(Debug)]
struct ClientFinal {
    /// The `c=` value — base64 of the gs2 header the client claims it sent.
    channel_binding: String,
    /// The `r=` value — must equal this exchange's combined nonce.
    nonce: String,
    /// Everything before `,p=` verbatim — the last third of AuthMessage.
    without_proof: String,
    /// The decoded `p=` proof.
    proof: [u8; 32],
}

fn parse_client_final(raw: &str) -> Result<ClientFinal, Reject> {
    const MALFORMED: Reject = Reject {
        sqlstate: SQLSTATE_PROTOCOL_VIOLATION,
        message: "malformed SCRAM client-final message",
    };

    // The proof is always the final attribute; nonces and base64 never
    // contain ',', so the split is unambiguous.
    let idx = raw.rfind(",p=").ok_or(MALFORMED)?;
    let without_proof = &raw[..idx];
    let proof: [u8; 32] = scram::b64_decode(&raw[idx + 3..])
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(MALFORMED)?;

    let mut attrs = without_proof.split(',');
    let channel_binding = attrs
        .next()
        .and_then(|a| a.strip_prefix("c="))
        .ok_or(MALFORMED)?;
    let nonce = attrs
        .next()
        .and_then(|a| a.strip_prefix("r="))
        .ok_or(MALFORMED)?;
    Ok(ClientFinal {
        channel_binding: channel_binding.to_owned(),
        nonce: nonce.to_owned(),
        without_proof: without_proof.to_owned(),
        proof,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_first_parses_each_gs2_cbind_flag() {
        let parsed = parse_client_first("n,,n=alice,r=abcdef").expect("n flag");
        assert_eq!(parsed.gs2_header, "n,,");
        assert_eq!(parsed.cbind_flag, CbindFlag::NotSupported);
        assert_eq!(parsed.bare, "n=alice,r=abcdef");
        assert_eq!(parsed.nonce, "abcdef");

        let parsed = parse_client_first("y,,n=,r=xyz").expect("y flag");
        assert_eq!(parsed.gs2_header, "y,,");
        assert_eq!(parsed.cbind_flag, CbindFlag::SupportedButUnused);

        // `p=…` now parses into the requested type, verbatim header and all; the
        // reconciliation with the mechanism happens in `authenticate`.
        let parsed = parse_client_first("p=tls-server-end-point,,n=a,r=x").expect("p flag");
        assert_eq!(parsed.gs2_header, "p=tls-server-end-point,,");
        assert_eq!(
            parsed.cbind_flag,
            CbindFlag::Required("tls-server-end-point".to_owned())
        );
        assert_eq!(parsed.bare, "n=a,r=x");
    }

    #[test]
    fn client_first_rejects_a_p_flag_with_no_type() {
        // `p=` with an empty type name is malformed (the comma immediately
        // follows the `=`).
        let err = parse_client_first("p=,,n=a,r=x").expect_err("empty cbind type");
        assert_eq!(err.sqlstate, SQLSTATE_PROTOCOL_VIOLATION);
    }

    #[test]
    fn mechanism_list_offers_plus_first_only_with_channel_binding() {
        // Off TLS (no binding): plain SCRAM only.
        assert_eq!(&sasl_mechanism_list(None)[..], b"SCRAM-SHA-256\0\0");
        // On TLS (binding present): PLUS first, then plain, so a capable client
        // prefers PLUS.
        assert_eq!(
            &sasl_mechanism_list(Some(&[0u8; 32]))[..],
            b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0",
        );
    }

    #[test]
    fn channel_binding_in_force_only_for_a_plus_p_exchange() {
        let cbind = [7u8; 32];
        let required = CbindFlag::Required(CBIND_TYPE.to_owned());

        // PLUS + p=tls-server-end-point: binding is in force and threaded through.
        assert_eq!(
            reconcile_channel_binding(&required, true, Some(&cbind)),
            Ok(Some(&cbind[..])),
        );
        // Plain + n: no binding, fine on either transport.
        assert_eq!(
            reconcile_channel_binding(&CbindFlag::NotSupported, false, Some(&cbind)),
            Ok(None),
        );
        assert_eq!(
            reconcile_channel_binding(&CbindFlag::NotSupported, false, None),
            Ok(None),
        );
        // Plain + y off TLS: the without-PLUS rule still accepts it.
        assert_eq!(
            reconcile_channel_binding(&CbindFlag::SupportedButUnused, false, None),
            Ok(None),
        );
    }

    #[test]
    fn channel_binding_rejects_downgrades_and_mechanism_mismatches() {
        let cbind = [7u8; 32];
        let required = CbindFlag::Required(CBIND_TYPE.to_owned());

        // The downgrade: `y` over a channel where PLUS *was* advertised.
        assert!(
            reconcile_channel_binding(&CbindFlag::SupportedButUnused, false, Some(&cbind)).is_err(),
            "y with PLUS advertised is a stripped-offer downgrade",
        );
        // Flag/mechanism mismatches are malformed in both directions.
        assert!(
            reconcile_channel_binding(&CbindFlag::NotSupported, true, Some(&cbind)).is_err(),
            "n cannot pair with the PLUS mechanism",
        );
        assert!(
            reconcile_channel_binding(&required, false, None).is_err(),
            "p cannot pair with the plain mechanism",
        );
        // An unknown channel-binding type under PLUS is refused.
        let unknown = CbindFlag::Required("tls-unique".to_owned());
        assert!(
            reconcile_channel_binding(&unknown, true, Some(&cbind)).is_err(),
            "only tls-server-end-point is implemented",
        );
    }

    #[test]
    fn client_first_rejects_authzid_and_mandatory_extensions() {
        let err = parse_client_first("n,a=admin,n=alice,r=abc").expect_err("authzid");
        assert_eq!(err.sqlstate, SQLSTATE_FEATURE_NOT_SUPPORTED);
        let err = parse_client_first("n,,m=ext,n=alice,r=abc").expect_err("mext");
        assert_eq!(err.sqlstate, SQLSTATE_FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn client_first_rejects_malformed_shapes() {
        for raw in [
            "",
            "n,",
            "n,,r=abc",
            "n,,n=alice",
            "n,,n=alice,r=",
            "x,,n=a,r=b",
        ] {
            assert!(parse_client_first(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn client_final_splits_on_the_last_proof_attribute() {
        let proof = scram::b64_encode(&[0x42; 32]);
        let raw = format!("c=biws,r=noncenonce,p={proof}");
        let parsed = parse_client_final(&raw).expect("parses");
        assert_eq!(parsed.channel_binding, "biws");
        assert_eq!(parsed.nonce, "noncenonce");
        assert_eq!(parsed.without_proof, "c=biws,r=noncenonce");
        assert_eq!(parsed.proof, [0x42; 32]);
    }

    #[test]
    fn client_final_rejects_missing_or_short_proof() {
        assert!(parse_client_final("c=biws,r=nonce").is_err(), "no proof");
        let short = scram::b64_encode(&[1u8; 8]);
        assert!(
            parse_client_final(&format!("c=biws,r=n,p={short}")).is_err(),
            "proof must be 32 bytes"
        );
        assert!(parse_client_final("r=n,c=biws,p=AAAA").is_err(), "order");
    }
}
