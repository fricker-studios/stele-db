//! SCRAM-SHA-256 primitives ([RFC 5802] / [RFC 7677]) — the math behind
//! Postgres-compatible password authentication ([STL-252]).
//!
//! Stele stores **verifiers**, never passwords: `CREATE USER … PASSWORD …`
//! derives a [`ScramVerifier`] (salted, iterated — the RFC's `StoredKey` /
//! `ServerKey` pair) that the durable catalog log persists, and the pg-wire
//! SASL exchange proves possession of the password against it without the
//! password ever crossing the wire. This module holds the pure, deterministic
//! pieces both sides of that split share: the key-derivation functions
//! ([`hi`], [`hmac_sha256`]), the verifier itself, the proof verification the
//! server runs, and the base64 codec SCRAM messages use. Entropy (salts,
//! nonces) is deliberately **not** here — callers inject it, keeping this
//! crate clock- and RNG-free like the rest of the dependency root
//! ([ADR-0010]).
//!
//! ## Why vendored
//!
//! Like the SHA-256 in [`crate::hash`] (whose compression function this module
//! builds on), HMAC, PBKDF2, and base64 are small, fully-specified standards
//! kept dependency-free on purpose: every third-party crate is a supply-chain
//! surface and a `cargo-deny` decision, and the authentication path is exactly
//! where a hidden transitive dependency is least welcome. Correctness is
//! pinned to the published known-answer vectors in the tests below — [RFC
//! 4231] for HMAC, [RFC 7914] §11 for PBKDF2-HMAC-SHA-256, [RFC 4648] §10 for
//! base64, and the full [RFC 7677] §3 example exchange end-to-end. The one
//! deliberate exception is the SASLprep normalization below ([ADR-0033]): its
//! Unicode tables are too large to vendor and would have to track Unicode
//! versions by hand, so [`prepare_password`] leans on the `stringprep` crate.
//!
//! ## Normalization (SASLprep, [STL-298])
//!
//! Postgres applies SASLprep ([RFC 4013]) to passwords before hashing and falls
//! back to the raw bytes when normalization fails; SCRAM clients normalize the
//! same way before computing their proof ([RFC 5802] §5.1, `SaltedPassword :=
//! Hi(Normalize(password), …)`). [`prepare_password`] does exactly this, and
//! both password-ingesting paths — [`ScramVerifier::derive`] (the `CREATE`/
//! `ALTER USER` verifier) and [`client_proof`] (the client's proof) — run
//! through it, so the two sides always normalize identically. ASCII passwords —
//! the overwhelming case — take the crate's zero-alloc fast path and stay
//! byte-identical; a non-ASCII password decomposed on one side and composed on
//! the other now NFKC-fold to the same bytes and interoperate. Inputs SASLprep
//! rejects (prohibited control/bidi characters) fall back to their raw bytes on
//! both sides, matching Postgres, so they still authenticate against themselves.
//!
//! [RFC 4013]: https://www.rfc-editor.org/rfc/rfc4013
//! [RFC 4231]: https://www.rfc-editor.org/rfc/rfc4231
//! [RFC 4648]: https://www.rfc-editor.org/rfc/rfc4648
//! [RFC 5802]: https://www.rfc-editor.org/rfc/rfc5802
//! [RFC 7677]: https://www.rfc-editor.org/rfc/rfc7677
//! [RFC 7914]: https://www.rfc-editor.org/rfc/rfc7914
//! [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
//! [STL-298]: https://allegromusic.atlassian.net/browse/STL-298
//! [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md
//! [ADR-0033]: ../../../docs/adr/0033-saslprep-password-normalization.md

use core::fmt;
use std::borrow::Cow;

use crate::hash::{SHA256_LEN, sha256};

/// The iteration count new verifiers are derived with — Postgres's
/// `scram_iterations` default, the interoperability floor every driver
/// handles.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// The salt length (bytes) new verifiers are derived with — what Postgres
/// generates for `pg_authid`.
pub const SALT_LEN: usize = 16;

/// A stored SCRAM-SHA-256 verifier — what `CREATE USER` persists in place of
/// a password ([RFC 5802] §3: `StoredKey := H(ClientKey)`,
/// `ServerKey := HMAC(SaltedPassword, "Server Key")`).
///
/// Holding a verifier permits *verifying* a client and *being* this server —
/// not authenticating as the user. It is still sensitive (it admits an
/// offline dictionary attack and server impersonation), so `Debug` redacts
/// everything but the public parameters.
///
/// [RFC 5802]: https://www.rfc-editor.org/rfc/rfc5802
#[derive(Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    /// The PBKDF2 iteration count the salt was applied with.
    pub iterations: u32,
    /// The per-user random salt (public: the server hands it to any client
    /// that opens an exchange).
    pub salt: Vec<u8>,
    /// `H(ClientKey)` — what the client's proof is checked against.
    pub stored_key: [u8; SHA256_LEN],
    /// `HMAC(SaltedPassword, "Server Key")` — what signs the server-final
    /// message so the client can authenticate *us*.
    pub server_key: [u8; SHA256_LEN],
}

impl fmt::Debug for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Key material stays out of logs and assertion messages.
        f.debug_struct("ScramVerifier")
            .field("iterations", &self.iterations)
            .field("salt_len", &self.salt.len())
            .field("stored_key", &"<redacted>")
            .field("server_key", &"<redacted>")
            .finish()
    }
}

impl ScramVerifier {
    /// Derive the verifier for `password` under `salt` and `iterations`
    /// ([RFC 5802] §3). Pure: the caller supplies the salt (fresh OS entropy
    /// at `CREATE USER`, the stored one when re-deriving in tests).
    ///
    /// [RFC 5802]: https://www.rfc-editor.org/rfc/rfc5802
    #[must_use]
    pub fn derive(password: &str, salt: &[u8], iterations: u32) -> Self {
        let salted = hi(&prepare_password(password), salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key).0;
        let server_key = hmac_sha256(&salted, b"Server Key");
        Self {
            iterations,
            salt: salt.to_vec(),
            stored_key,
            server_key,
        }
    }

    /// Check a client's proof against this verifier ([RFC 5802] §3):
    /// `ClientKey := ClientProof XOR HMAC(StoredKey, AuthMessage)`, accepted
    /// iff `H(ClientKey) = StoredKey`. Constant-time on the final compare.
    #[must_use]
    pub fn verify_client_proof(&self, auth_message: &[u8], proof: &[u8; SHA256_LEN]) -> bool {
        let signature = hmac_sha256(&self.stored_key, auth_message);
        let mut client_key = [0u8; SHA256_LEN];
        for (out, (p, s)) in client_key.iter_mut().zip(proof.iter().zip(&signature)) {
            *out = p ^ s;
        }
        ct_eq(&sha256(&client_key).0, &self.stored_key)
    }

    /// The `ServerSignature` for `auth_message` — the `v=` value of the
    /// server-final message, proving to the client we hold the verifier.
    #[must_use]
    pub fn server_signature(&self, auth_message: &[u8]) -> [u8; SHA256_LEN] {
        hmac_sha256(&self.server_key, auth_message)
    }
}

/// The client-side proof for `auth_message` ([RFC 5802] §3:
/// `ClientProof := ClientKey XOR HMAC(StoredKey, AuthMessage)`).
///
/// The server never computes this — it lives here so the wire tests (and a
/// future `stele shell` client) can act as a real SCRAM client against the
/// same pinned vectors.
#[must_use]
pub fn client_proof(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> [u8; SHA256_LEN] {
    let salted = hi(&prepare_password(password), salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key).0;
    let signature = hmac_sha256(&stored_key, auth_message);
    let mut proof = [0u8; SHA256_LEN];
    for (out, (k, s)) in proof.iter_mut().zip(client_key.iter().zip(&signature)) {
        *out = k ^ s;
    }
    proof
}

/// Normalize a password with SASLprep ([RFC 4013]) before it is fed to [`hi`].
///
/// This is the single seam both derivation sites ([`ScramVerifier::derive`] and
/// [`client_proof`]) route through, so a verifier and the proof checked against
/// it always agree on the bytes ([STL-298], [ADR-0033]). It returns the prepared
/// bytes on success and **falls back to the raw UTF-8 bytes** when SASLprep
/// rejects the input (a prohibited control or bidi character) — exactly what
/// Postgres's `pg_saslprep` does. Because both sides fall back identically, even
/// an un-preppable password authenticates against itself; what the normalization
/// buys is that a client which NFKC-composes a decomposed password (or maps a
/// non-ASCII space) still matches a verifier derived from the other form. ASCII
/// input takes `stringprep`'s zero-alloc fast path and is returned borrowed,
/// byte-for-byte unchanged.
///
/// [RFC 4013]: https://www.rfc-editor.org/rfc/rfc4013
/// [STL-298]: https://allegromusic.atlassian.net/browse/STL-298
/// [ADR-0033]: ../../../docs/adr/0033-saslprep-password-normalization.md
#[must_use]
pub fn prepare_password(password: &str) -> Cow<'_, [u8]> {
    match stringprep::saslprep(password) {
        // `saslprep` borrows ASCII unchanged and owns a normalized String
        // otherwise; either way we hand `hi` the bytes.
        Ok(Cow::Borrowed(s)) => Cow::Borrowed(s.as_bytes()),
        Ok(Cow::Owned(s)) => Cow::Owned(s.into_bytes()),
        // Prohibited / bidi / unassigned input: hash the raw bytes, like Postgres.
        Err(_) => Cow::Borrowed(password.as_bytes()),
    }
}

/// HMAC-SHA-256 ([RFC 2104]): `H((K' ^ opad) || H((K' ^ ipad) || msg))` with
/// a 64-byte block size.
///
/// [RFC 2104]: https://www.rfc-editor.org/rfc/rfc2104
#[must_use]
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; SHA256_LEN] {
    const BLOCK: usize = 64;
    // K': hash an over-long key, then zero-pad to the block size.
    let mut padded = [0u8; BLOCK];
    if key.len() > BLOCK {
        padded[..SHA256_LEN].copy_from_slice(&sha256(key).0);
    } else {
        padded[..key.len()].copy_from_slice(key);
    }

    let mut inner = Vec::with_capacity(BLOCK + msg.len());
    inner.extend(padded.iter().map(|b| b ^ 0x36));
    inner.extend_from_slice(msg);
    let inner_digest = sha256(&inner);

    let mut outer = Vec::with_capacity(BLOCK + SHA256_LEN);
    outer.extend(padded.iter().map(|b| b ^ 0x5c));
    outer.extend_from_slice(&inner_digest.0);
    sha256(&outer).0
}

/// `Hi(str, salt, i)` ([RFC 5802] §2.2) — PBKDF2-HMAC-SHA-256 with a one-block
/// (32-byte) output: `U1 := HMAC(str, salt || INT(1))`, `Un := HMAC(str,
/// Un-1)`, result `U1 XOR … XOR Ui`.
///
/// [RFC 5802]: https://www.rfc-editor.org/rfc/rfc5802
#[must_use]
pub fn hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; SHA256_LEN] {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &block);
    let mut out = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (acc, byte) in out.iter_mut().zip(&u) {
            *acc ^= byte;
        }
    }
    out
}

/// Constant-time byte-slice equality.
///
/// The compare every proof / signature check goes through, so a mismatch's
/// position never shows in timing. The length check short-circuits, but
/// lengths here are public constants.
#[must_use]
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    for (x, y) in a.iter().zip(b) {
        acc |= x ^ y;
    }
    acc == 0
}

// ---------------------------------------------------------------------------
// Base64 (RFC 4648 §4, standard alphabet, padded) — the encoding SCRAM
// attribute values (nonces, salts, proofs, signatures) travel in.
// ---------------------------------------------------------------------------

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode `bytes` as standard padded base64.
#[must_use]
pub fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(char::from(B64_ALPHABET[usize::from(b0 >> 2)]));
        out.push(char::from(
            B64_ALPHABET[usize::from(((b0 & 0x03) << 4) | (b1 >> 4))],
        ));
        if chunk.len() > 1 {
            out.push(char::from(
                B64_ALPHABET[usize::from(((b1 & 0x0F) << 2) | (b2 >> 6))],
            ));
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(char::from(B64_ALPHABET[usize::from(b2 & 0x3F)]));
        } else {
            out.push('=');
        }
    }
    out
}

/// Decode standard base64, padded or unpadded. `None` on any malformed input
/// (bad character, bad length, non-canonical trailing bits) — authentication
/// input is hostile, so nothing is repaired.
#[must_use]
pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    const fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    // Validate padding canonicality before decoding, rather than just trimming
    // every trailing '='. Padding, when present, is a run of `=` at the very
    // end (no embedded `=`), 1 or 2 characters (never more), and makes the
    // total length a multiple of 4 — so `"Zg="` (under-padded), `"Zm9v===="`
    // (over-padded), and `"Z=m9"` (embedded) are all rejected, not repaired.
    let unpadded = bytes.iter().take_while(|&&b| b != b'=').count();
    if bytes[unpadded..].iter().any(|&b| b != b'=') {
        return None; // a non-`=` byte after the first `=`: padding is not a clean tail
    }
    let pad = bytes.len() - unpadded;
    if pad > 0 {
        // The only canonical pad for `unpadded % 4` of 2 or 3 is 2 or 1
        // respectively; 0 and 1 admit no padding at all.
        let canonical_pad = (4 - unpadded % 4) % 4;
        if bytes.len() % 4 != 0 || pad != canonical_pad {
            return None;
        }
    }
    let trimmed = &bytes[..unpadded];
    let mut out = Vec::with_capacity(trimmed.len() * 3 / 4);
    for chunk in trimmed.chunks(4) {
        let mut vals = [0u8; 4];
        for (slot, &c) in vals.iter_mut().zip(chunk) {
            *slot = val(c)?;
        }
        match chunk.len() {
            4 => {
                out.push((vals[0] << 2) | (vals[1] >> 4));
                out.push((vals[1] << 4) | (vals[2] >> 2));
                out.push((vals[2] << 6) | vals[3]);
            }
            3 => {
                // Trailing bits must be zero — reject non-canonical encodings.
                if vals[2] & 0x03 != 0 {
                    return None;
                }
                out.push((vals[0] << 2) | (vals[1] >> 4));
                out.push((vals[1] << 4) | (vals[2] >> 2));
            }
            2 => {
                if vals[1] & 0x0F != 0 {
                    return None;
                }
                out.push((vals[0] << 2) | (vals[1] >> 4));
            }
            // A single leftover character can never encode a whole byte.
            _ => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        use core::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
            .collect()
    }

    /// RFC 4231 known-answer vectors for HMAC-SHA-256 — short key (test case
    /// 1), block-boundary data (case 2), and an over-block key that exercises
    /// the hash-the-key path (case 6).
    #[test]
    fn hmac_rfc4231_vectors() {
        // Case 1: 20-byte 0x0b key, "Hi There".
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Case 2: key "Jefe", data "what do ya want for nothing?".
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Case 6: 131-byte key (> the 64-byte block, so K' = H(K)).
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    /// RFC 7914 §11 PBKDF2-HMAC-SHA-256 vectors. `hi` is the single-block
    /// (dkLen = 32) PBKDF2, so it must equal the first 32 bytes of each.
    #[test]
    fn hi_matches_pbkdf2_sha256_vectors() {
        assert_eq!(
            hex(&hi(b"passwd", b"salt", 1)),
            "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc"
        );
        assert_eq!(
            hex(&hi(b"Password", b"NaCl", 80_000)),
            "4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56"
        );
    }

    /// The full RFC 7677 §3 example exchange, end-to-end: user "user",
    /// password "pencil", the published salt/nonces — the derived verifier
    /// must accept the published client proof and emit the published server
    /// signature.
    #[test]
    fn rfc7677_example_exchange_round_trips() {
        let salt = b64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let verifier = ScramVerifier::derive("pencil", &salt, 4096);

        let auth_message: &[u8] = b"n=user,r=rOprNGfwEbeRWgbNEkqO,\
            r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,\
            c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

        // The published client proof verifies…
        let proof: [u8; SHA256_LEN] = b64_decode("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
            .expect("proof")
            .try_into()
            .expect("32 bytes");
        assert!(verifier.verify_client_proof(auth_message, &proof));
        // …matches the one our client-side derivation computes…
        assert_eq!(client_proof("pencil", &salt, 4096, auth_message), proof);
        // …and the server signature matches the published server-final value.
        assert_eq!(
            b64_encode(&verifier.server_signature(auth_message)),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );

        // A one-byte-different proof is refused.
        let mut bad = proof;
        bad[0] ^= 0x01;
        assert!(!verifier.verify_client_proof(auth_message, &bad));
        // The right proof against a different auth message is refused (a
        // replayed capture under fresh nonces lands here).
        assert!(!verifier.verify_client_proof(b"different auth message", &proof));
    }

    /// The wrong password derives a verifier that refuses the right
    /// password's proof, and vice versa.
    #[test]
    fn wrong_password_is_refused() {
        let salt = unhex("00112233445566778899aabbccddeeff");
        let verifier = ScramVerifier::derive("correct horse", &salt, DEFAULT_ITERATIONS);
        let msg = b"n=u,r=abc,r=abcdef,s=ABCD,i=4096,c=biws,r=abcdef";
        let good = client_proof("correct horse", &salt, DEFAULT_ITERATIONS, msg);
        let bad = client_proof("battery staple", &salt, DEFAULT_ITERATIONS, msg);
        assert!(verifier.verify_client_proof(msg, &good));
        assert!(!verifier.verify_client_proof(msg, &bad));
    }

    /// RFC 4648 §10 base64 vectors, both directions, plus malformed-input
    /// rejection.
    #[test]
    fn base64_rfc4648_vectors() {
        let cases: &[(&[u8], &str)] = &[
            (b"", ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
        ];
        for (raw, encoded) in cases {
            assert_eq!(b64_encode(raw), *encoded);
            assert_eq!(b64_decode(encoded).as_deref(), Some(*raw));
        }
        // Hostile input: bad characters, impossible lengths, non-canonical
        // trailing bits.
        assert_eq!(b64_decode("Zm9!"), None);
        assert_eq!(b64_decode("Z"), None);
        assert_eq!(b64_decode("Zh=="), None, "non-zero trailing bits");
        assert_eq!(b64_decode("Zm9="), None, "non-zero trailing bits");
    }

    #[test]
    fn base64_rejects_non_canonical_padding() {
        // Hostile auth input must not be repaired: under-, over-, and
        // misplaced padding are all rejected rather than silently trimmed.
        assert_eq!(b64_decode("Zg="), None, "under-padded (needs ==)");
        assert_eq!(b64_decode("Zm9v===="), None, "over-padded");
        assert_eq!(b64_decode("Z=m9"), None, "embedded '='");
        assert_eq!(b64_decode("Zm8=="), None, "wrong pad count for length");
        // The canonical encodings of the same lengths still decode.
        assert_eq!(b64_decode("Zg==").as_deref(), Some(&b"f"[..]));
        assert_eq!(b64_decode("Zm8=").as_deref(), Some(&b"fo"[..]));
    }

    #[test]
    fn b64_round_trips_arbitrary_bytes() {
        // Every byte value, at every chunk alignment.
        let all: Vec<u8> = (0..=255).collect();
        for start in 0..3 {
            let slice = &all[start..];
            assert_eq!(
                b64_decode(&b64_encode(slice)).as_deref(),
                Some(slice),
                "alignment {start}"
            );
        }
    }

    #[test]
    fn ct_eq_compares_exactly() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
    }

    #[test]
    fn verifier_debug_redacts_key_material() {
        let verifier = ScramVerifier::derive("secret", b"0123456789abcdef", 1);
        let rendered = format!("{verifier:?}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(!rendered.contains(&hex(&verifier.stored_key)), "{rendered}");
    }

    /// SASLprep ([STL-298]) closes the interop gap a raw-bytes floor leaves
    /// open: a verifier derived from one Unicode form of a password accepts a
    /// proof computed from another, because both derivation sites NFKC-normalize
    /// first. This is the case that fails byte-for-byte today.
    #[test]
    fn saslprep_composed_and_decomposed_passwords_interoperate() {
        let salt = unhex("00112233445566778899aabbccddeeff");
        let msg = b"n=u,r=abc,r=abcdef,s=ABCD,i=4096,c=biws,r=abcdef";

        // "café": precomposed U+00E9 vs. "e" + combining acute U+0301 — distinct
        // byte strings that fold to the same thing under NFKC.
        let composed = "caf\u{e9}";
        let decomposed = "cafe\u{301}";
        assert_ne!(
            composed.as_bytes(),
            decomposed.as_bytes(),
            "the two forms must differ on the wire, or this proves nothing"
        );

        // Server stores a verifier for one form; a client proves with the other.
        let verifier = ScramVerifier::derive(composed, &salt, DEFAULT_ITERATIONS);
        let proof = client_proof(decomposed, &salt, DEFAULT_ITERATIONS, msg);
        assert!(
            verifier.verify_client_proof(msg, &proof),
            "decomposed → composed"
        );

        // …and symmetrically, to pin that neither direction is privileged.
        let verifier = ScramVerifier::derive(decomposed, &salt, DEFAULT_ITERATIONS);
        let proof = client_proof(composed, &salt, DEFAULT_ITERATIONS, msg);
        assert!(
            verifier.verify_client_proof(msg, &proof),
            "composed → decomposed"
        );
    }

    /// SASLprep mapping (RFC 4013 §2.1): a non-ASCII space folds to U+0020 and a
    /// "mapped to nothing" code point is dropped, while ASCII passes through
    /// byte-for-byte on the crate's fast path.
    #[test]
    fn saslprep_maps_and_passes_ascii_through() {
        assert_eq!(
            prepare_password("pencil").as_ref(),
            b"pencil",
            "ASCII unchanged"
        );
        assert_eq!(
            prepare_password("a\u{a0}b").as_ref(),
            b"a b",
            "U+00A0 NBSP maps to a plain space"
        );
        assert_eq!(
            prepare_password("a\u{ad}b").as_ref(),
            b"ab",
            "U+00AD soft hyphen is mapped to nothing"
        );
    }

    /// Prohibited input (here U+0007 BEL, a C.2.1 control) makes SASLprep fail;
    /// like Postgres we then hash the raw bytes. Both sides fall back the same
    /// way, so the password still authenticates against itself.
    #[test]
    fn saslprep_falls_back_to_raw_on_prohibited_input() {
        let pw = "pa\u{7}ss";
        assert_eq!(
            prepare_password(pw).as_ref(),
            pw.as_bytes(),
            "rejected input is hashed raw, not repaired"
        );

        let salt = unhex("00112233445566778899aabbccddeeff");
        let msg = b"n=u,r=abc,r=abcdef,s=ABCD,i=4096,c=biws,r=abcdef";
        let verifier = ScramVerifier::derive(pw, &salt, DEFAULT_ITERATIONS);
        let proof = client_proof(pw, &salt, DEFAULT_ITERATIONS, msg);
        assert!(verifier.verify_client_proof(msg, &proof));
    }
}
