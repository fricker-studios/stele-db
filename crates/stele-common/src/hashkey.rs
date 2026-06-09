//! Portable hash-key function v1 — a deterministic digest over an ordered list
//! of typed, nullable values, **identical across engine versions, platforms, and
//! client languages**.
//!
//! This is the integration-groundwork primitive
//! ([ADR-0011](../../../docs/adr/0011-hash-distribution-integration-groundwork.md)):
//! the foundation any hash-keyed modeling pattern needs, letting an *external*
//! model (or a client in another language) compute the **same** business/hash
//! keys the engine does. The engine stays ignorant of what those keys *mean* —
//! Data Vault, RCM, and every other downstream concept live above the bright line
//! ([ADR-0009](../../../docs/adr/0009-data-vault-conceptual-seam.md)); this module
//! only hashes typed values.
//!
//! ## The byte-spec is frozen
//!
//! The on-the-wire encoding here is a long-term, externally-observable format
//! commitment, exactly like the on-disk segment format — a client that recomputes
//! a key against v1 must get the engine's v1 digest forever. So the encoding is
//! **pinned, versioned, and independent** of the storage-private
//! [`ScalarValue::encode`](crate::types::ScalarValue::encode) (which may change as
//! codecs evolve). The full rules and the canonical test vectors are published in
//! [`docs/hash-key-v1.md`](../../../docs/hash-key-v1.md); the [`vectors`] table
//! below is the same data, checked in and asserted by this module's tests so the
//! doc and the code cannot silently drift.
//!
//! ## Encoding (v1)
//!
//! The hashed message is, in order:
//!
//! ```text
//! MAGIC = b"STLHK1"                 (6 bytes — "Stele Hash Key", spec version 1)
//! argc  = u32 big-endian            (number of arguments)
//! for each argument, in order:
//!     tag = u8                      (the type tag below)
//!     len = u64 big-endian          (length of body in bytes)
//!     body                          (len bytes, big-endian for fixed-width types)
//! ```
//!
//! and the digest is `SHA-256(message)` ([`crate::hash::sha256`]), rendered as
//! lowercase hex by the SQL surface.
//!
//! | type | tag | body |
//! |---|---|---|
//! | NULL | `0x00` | *(empty)* |
//! | BOOL | `0x01` | 1 byte: `0x00` false / `0x01` true |
//! | INT4 | `0x02` | 4-byte big-endian two's-complement `i32` |
//! | INT8 | `0x03` | 8-byte big-endian two's-complement `i64` |
//! | TEXT | `0x04` | UTF-8 bytes, verbatim |
//! | TIMESTAMP | `0x05` | 8-byte big-endian `i64` microseconds since the Unix epoch (UTC) |
//! | DATE | `0x06` | 4-byte big-endian `i32` days since the Unix epoch |
//! | TIMESTAMPTZ | `0x07` | 8-byte big-endian `i64` microseconds since the Unix epoch (UTC) |
//! | PERIOD | `0x08` | 16 bytes: two big-endian `i64` µs bounds, `from` then `to` (open upper = `i64::MAX`) |
//!
//! ### Why these choices
//!
//! * **Length-prefix framing** makes argument concatenation injective: `hash('a',
//!   'b')` can never collide with `hash('ab')`, and a zero-length body (empty
//!   TEXT) is distinct from `NULL` because the *tag* differs.
//! * **Type-tagged** values mean `hash(1::int4)` ≠ `hash(1::int8)`: the digest is
//!   over the *typed* value, so a client must encode with the same type tag. This
//!   is the safe, unambiguous choice over a "numeric-agnostic" hash, which would
//!   be a silent-collision footgun.
//! * **Big-endian** (network byte order) bodies are the portable convention, so a
//!   client in any language encodes the same bytes regardless of its host.
//! * **No Unicode normalization** in v1: TEXT is hashed as its UTF-8 bytes
//!   verbatim. Normalizing would pull in a Unicode-tables dependency
//!   ([Cargo.toml](../../../Cargo.toml) treats every crate as supply-chain
//!   surface) and bake a Unicode version into a frozen format. Callers that need
//!   `'café'` (NFC) and `'cafe\u{301}'` (NFD) to hash alike normalize *before*
//!   calling — a documented v1 limitation, revisited if a future `hash` version is
//!   cut.

use crate::hash::{Digest, sha256};
use crate::types::ScalarValue;

/// The magic prefix of a v1 hash-key message: ASCII `"STLHK1"` — "Stele Hash
/// Key", spec version `1`. Versions the function and domain-separates a hash-key
/// digest from the bare-`sha256` commit-log hash ([ADR-0026]) of the same bytes.
const MAGIC: &[u8] = b"STLHK1";

// Per-type tags. Frozen: a tag's meaning never changes, and a new type takes the
// next free value rather than reusing one.
const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_INT4: u8 = 0x02;
const TAG_INT8: u8 = 0x03;
const TAG_TEXT: u8 = 0x04;
const TAG_TIMESTAMP: u8 = 0x05;
const TAG_DATE: u8 = 0x06;
const TAG_TIMESTAMPTZ: u8 = 0x07;
const TAG_PERIOD: u8 = 0x08;

/// Compute the v1 portable hash-key digest of `args`, in order.
///
/// Each argument is an `Option<ScalarValue>` — `None` is SQL `NULL`, encoded as
/// the distinct NULL frame (tag `0x00`). The returned [`Digest`] is the 32-byte
/// SHA-256 of the framed message; the SQL `hash(...)` surface renders it as
/// lowercase hex.
///
/// Deterministic and allocation-light, so it runs under the simulation scheduler
/// like the rest of the runtime-agnostic core.
///
/// ```
/// use stele_common::hashkey::hash_key;
/// use stele_common::types::ScalarValue;
///
/// // Same inputs → same digest, on any platform.
/// let a = hash_key(&[Some(ScalarValue::Text("acme".to_owned())), Some(ScalarValue::Int4(42))]);
/// let b = hash_key(&[Some(ScalarValue::Text("acme".to_owned())), Some(ScalarValue::Int4(42))]);
/// assert_eq!(a, b);
///
/// // Length-framing keeps argument boundaries: hash('a','b') ≠ hash('ab').
/// let split = hash_key(&[Some(ScalarValue::Text("a".to_owned())), Some(ScalarValue::Text("b".to_owned()))]);
/// let joined = hash_key(&[Some(ScalarValue::Text("ab".to_owned()))]);
/// assert_ne!(split, joined);
/// ```
#[must_use]
pub fn hash_key(args: &[Option<ScalarValue>]) -> Digest {
    let mut msg = Vec::with_capacity(MAGIC.len() + 4 + args.len() * 16);
    msg.extend_from_slice(MAGIC);
    // `argc` (u32 big-endian) frames the arity, so a different number of
    // (length-framed) arguments can never produce the same byte stream. A SQL
    // `hash(...)` call cannot approach `u32::MAX` arguments.
    let argc = u32::try_from(args.len()).expect("argument count fits u32");
    msg.extend_from_slice(&argc.to_be_bytes());
    for arg in args {
        encode_arg(arg.as_ref(), &mut msg);
    }
    sha256(&msg)
}

/// Append one argument's `[tag][len:u64be][body]` frame to `msg`.
fn encode_arg(arg: Option<&ScalarValue>, msg: &mut Vec<u8>) {
    // The body is borrowed (a slice of `msg`-bound bytes or a stack array), so no
    // per-argument heap allocation — a long `TEXT` body streams straight through.
    // `None` is the NULL frame with an empty body.
    match arg {
        None => push_frame(msg, TAG_NULL, &[]),
        Some(ScalarValue::Bool(b)) => push_frame(msg, TAG_BOOL, &[u8::from(*b)]),
        Some(ScalarValue::Int4(v)) => push_frame(msg, TAG_INT4, &v.to_be_bytes()),
        Some(ScalarValue::Int8(v)) => push_frame(msg, TAG_INT8, &v.to_be_bytes()),
        Some(ScalarValue::Text(s)) => push_frame(msg, TAG_TEXT, s.as_bytes()),
        Some(ScalarValue::Timestamp(v)) => push_frame(msg, TAG_TIMESTAMP, &v.to_be_bytes()),
        Some(ScalarValue::Date(v)) => push_frame(msg, TAG_DATE, &v.to_be_bytes()),
        // `timestamptz` is UTC-internal, so its body is the same big-endian µs
        // instant as `timestamp`; the distinct tag keeps the two from aliasing.
        Some(ScalarValue::TimestampTz(v)) => push_frame(msg, TAG_TIMESTAMPTZ, &v.to_be_bytes()),
        // A `period` frames its half-open `[from, to)` bounds as two big-endian
        // i64 µs values, lower then upper (an open upper bound is `i64::MAX`).
        Some(ScalarValue::Period(iv)) => {
            let mut body = [0u8; 16];
            body[..8].copy_from_slice(&iv.from.to_be_bytes());
            body[8..].copy_from_slice(&iv.to.to_be_bytes());
            push_frame(msg, TAG_PERIOD, &body);
        }
    }
}

/// Append a `[tag][len:u64be][body]` frame to `msg`. A `body` longer than
/// `u64::MAX` is impossible (it would not fit in memory), so the length cast is
/// infallible.
fn push_frame(msg: &mut Vec<u8>, tag: u8, body: &[u8]) {
    msg.push(tag);
    let len = u64::try_from(body.len()).expect("body length fits u64");
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(body);
}

/// One published hash-key test vector.
///
/// A human label, the argument list, and the expected lowercase-hex digest.
/// [`vectors`] returns the canonical set, mirrored verbatim in
/// [`docs/hash-key-v1.md`](../../../docs/hash-key-v1.md).
pub struct Vector {
    /// A short description of what the vector exercises.
    pub label: &'static str,
    /// The argument list, in order.
    pub args: Vec<Option<ScalarValue>>,
    /// The expected SHA-256 digest, lowercase hex (64 chars).
    pub hex: &'static str,
}

/// The canonical v1 hash-key test vectors.
///
/// The **published** cross-platform determinism witnesses. This module's
/// `vectors_are_stable` test recomputes each one; a change here is a change to the
/// frozen format and must be matched in
/// [`docs/hash-key-v1.md`](../../../docs/hash-key-v1.md).
///
/// `ScalarValue::Text` owns a `String`, so the argument lists are built at call
/// time rather than as a `const`.
#[must_use]
pub fn vectors() -> Vec<Vector> {
    let text = |s: &str| Some(ScalarValue::Text(s.to_owned()));
    vec![
        Vector {
            label: "no arguments",
            args: vec![],
            hex: "c3ae0194dddca9863f78e86574fa26d75441dd94643ea5ef3196058761db054f",
        },
        Vector {
            label: "single NULL",
            args: vec![None],
            hex: "11e5cc31f5ab758bacb047a87c2e7400b17e99508b39efa45bd05fb3486fcca0",
        },
        Vector {
            label: "empty text",
            args: vec![text("")],
            hex: "cd43236bcc33b39a3530894687eb75fd004687e3aebf79e9739864a576b80578",
        },
        Vector {
            label: "text 'acme'",
            args: vec![text("acme")],
            hex: "809cf7dd76e429f04505df3c8df09e59e8c081fdd404bfe1d6e330aee4c2f82e",
        },
        Vector {
            label: "int4 42",
            args: vec![Some(ScalarValue::Int4(42))],
            hex: "96d0d0ba3e2c2426333516658650326edc8ac00fb3225299a1b24e21c56cf0c9",
        },
        Vector {
            label: "int8 42",
            args: vec![Some(ScalarValue::Int8(42))],
            hex: "29a3cc3ace5b15675d46dc9e13804f0ab65feaf17dc14090b3d9ccdfba6c7061",
        },
        Vector {
            label: "bool true",
            args: vec![Some(ScalarValue::Bool(true))],
            hex: "4c7d2e3d4dd051d20ab6da771cf4b167e4014b08d8b2d5b4e6b92da26b890ba9",
        },
        Vector {
            label: "composite ('acme', 42, NULL)",
            args: vec![text("acme"), Some(ScalarValue::Int4(42)), None],
            hex: "61c9586983296ab2e403396f6ce20b01e2adc2514633fc63bb9d5a1e0ce85a20",
        },
        Vector {
            label: "timestamptz 1700000000000000",
            args: vec![Some(ScalarValue::TimestampTz(1_700_000_000_000_000))],
            hex: "8720f97303210b1ae946845bfaad4a52d062189d52e098b5b4b3278708b2db31",
        },
        Vector {
            label: "period [10, 20)",
            args: vec![Some(ScalarValue::Period(
                crate::period::Interval::new(10, 20).expect("well-formed interval"),
            ))],
            hex: "35ccfb9906f082824c65f8e0ef866f4439cb8e4217a9ff5d3890d95fdff0379c",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published vectors recompute to their checked-in digests — the
    /// cross-run / cross-platform determinism witness the DoD rests on. The
    /// digests are frozen: editing the format must update both these and
    /// `docs/hash-key-v1.md`.
    #[test]
    fn vectors_are_stable() {
        for v in vectors() {
            assert_eq!(
                hash_key(&v.args).to_hex(),
                v.hex,
                "vector {:?} drifted",
                v.label
            );
        }
    }

    /// Length-framing makes the argument boundary load-bearing: a 2-argument key
    /// and the 1-argument concatenation of the same bytes differ.
    #[test]
    fn framing_separates_arguments() {
        let split = hash_key(&[
            Some(ScalarValue::Text("a".to_owned())),
            Some(ScalarValue::Text("b".to_owned())),
        ]);
        let joined = hash_key(&[Some(ScalarValue::Text("ab".to_owned()))]);
        assert_ne!(split, joined);
    }

    /// The type tag is part of the digest: the same numeric magnitude under two
    /// types hashes differently, so a hash key can't silently alias across types.
    #[test]
    fn type_tag_distinguishes_values() {
        assert_ne!(
            hash_key(&[Some(ScalarValue::Int4(1))]),
            hash_key(&[Some(ScalarValue::Int8(1))]),
        );
    }

    /// NULL and empty TEXT share a zero-length body but differ by tag, so the two
    /// are never confused.
    #[test]
    fn null_is_distinct_from_empty_text() {
        assert_ne!(
            hash_key(&[None]),
            hash_key(&[Some(ScalarValue::Text(String::new()))]),
        );
    }

    /// Argument order matters: a key is a sequence, not a set.
    #[test]
    fn argument_order_matters() {
        assert_ne!(
            hash_key(&[Some(ScalarValue::Int4(1)), Some(ScalarValue::Int4(2))]),
            hash_key(&[Some(ScalarValue::Int4(2)), Some(ScalarValue::Int4(1))]),
        );
    }
}
