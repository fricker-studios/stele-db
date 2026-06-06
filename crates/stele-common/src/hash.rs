//! SHA-256 ([FIPS 180-4]) — the hash behind the tamper-evident commit log.
//!
//! The verifiable audit log ([ADR-0026](../../../docs/adr/0026-verifiable-audit-log.md))
//! hash-chains each commit to its predecessor, and the Merkle inclusion /
//! consistency proofs (~v0.5) build on the same hash. That makes the hash a
//! long-term, externally-observable format commitment, so it lives here in the
//! dependency root where every layer — the commit-log writer today, the Merkle
//! tree tomorrow — names one implementation.
//!
//! ## Why a vendored implementation
//!
//! Like the CRC32C in `stele_storage::checksum` and the simulation PRNG, this
//! is a small, fully-specified standard kept dependency-free on purpose: the
//! workspace treats every third-party crate as a supply-chain surface and a
//! `cargo-deny` decision ([Cargo.toml](../../../Cargo.toml)), and a cryptographic
//! primitive on the audit path is exactly where a hidden transitive dependency is
//! least welcome. Correctness is pinned to the [FIPS 180-4] known-answer vectors
//! in the tests below; the exact hash function is ADR-0026's to choose, and it
//! chose a standard one.
//!
//! [FIPS 180-4]: https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf

/// The length in bytes of a SHA-256 digest.
pub const SHA256_LEN: usize = 32;

/// A SHA-256 digest — 32 bytes. The genesis link of a hash chain is the
/// all-zero digest ([`Digest::ZERO`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest(pub [u8; SHA256_LEN]);

impl Digest {
    /// The all-zero digest — the conventional genesis predecessor for a hash
    /// chain, distinct from any real SHA-256 output in practice.
    pub const ZERO: Self = Self([0u8; SHA256_LEN]);

    /// Borrow the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; SHA256_LEN] {
        &self.0
    }

    /// Render the digest as a lowercase hex string — the stable, copy-pasteable
    /// form for log lines, proofs, and test failures.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(SHA256_LEN * 2);
        for b in self.0 {
            // Two lowercase hex nibbles per byte; `from_digit` is infallible for
            // a value < 16.
            s.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble < 16"));
            s.push(char::from_digit(u32::from(b & 0x0F), 16).expect("nibble < 16"));
        }
        s
    }
}

/// Compute the SHA-256 digest of `bytes`.
///
/// A direct, allocation-free implementation of [FIPS 180-4] §6.2. Used by the
/// hash-chained commit log; deterministic and runtime-agnostic, so it runs under
/// the simulation scheduler like the rest of the storage/txn core.
#[must_use]
pub fn sha256(bytes: &[u8]) -> Digest {
    let mut h = H0;

    // Process every full 64-byte block of the message.
    let mut blocks = bytes.chunks_exact(64);
    for block in &mut blocks {
        compress(
            &mut h,
            block.try_into().expect("chunks_exact yields 64 bytes"),
        );
    }

    // The padding block(s): the trailing partial block, a `0x80` byte, zero
    // padding, and the 64-bit big-endian bit length. This needs either one or
    // two extra blocks depending on how much room the tail leaves for the length.
    let rest = blocks.remainder();
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut tail = [0u8; 128];
    tail[..rest.len()].copy_from_slice(rest);
    tail[rest.len()] = 0x80;
    let tail_len = if rest.len() < 56 { 64 } else { 128 };
    tail[tail_len - 8..tail_len].copy_from_slice(&bit_len.to_be_bytes());
    for block in tail[..tail_len].chunks_exact(64) {
        compress(&mut h, block.try_into().expect("64-byte block"));
    }

    let mut out = [0u8; SHA256_LEN];
    for (word, chunk) in h.iter().zip(out.chunks_exact_mut(4)) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    Digest(out)
}

/// Initial hash values — the fractional parts of the square roots of the first
/// eight primes ([FIPS 180-4] §5.3.3).
const H0: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

/// Round constants — the fractional parts of the cube roots of the first
/// sixty-four primes ([FIPS 180-4] §4.2.2).
const K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// The SHA-256 compression function over one 64-byte block ([FIPS 180-4] §6.2.2).
// `a`..`h` and `w` are the working-variable names FIPS 180-4 uses verbatim;
// renaming them to satisfy the lint would make the algorithm *harder* to check
// against the standard, so the single-char names are deliberate here.
#[allow(clippy::many_single_char_names)]
fn compress(h: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for (i, chunk) in block.chunks_exact(4).enumerate() {
        w[i] = u32::from_be_bytes(chunk.try_into().expect("4-byte word"));
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }

    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }

    for (slot, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
        *slot = slot.wrapping_add(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FIPS 180-4 / NIST CAVP known-answer vectors — the canonical proof the
    /// implementation matches the standard, byte for byte.
    #[test]
    fn fips_known_answer_vectors() {
        // Empty message.
        assert_eq!(
            sha256(b"").to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        // "abc" — one block, the canonical §B.1 example.
        assert_eq!(
            sha256(b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        );
        // 56 bytes — the boundary that forces a second padding block (the tail
        // leaves no room for the 8-byte length in its own block).
        assert_eq!(
            sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq").to_hex(),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        );
    }

    /// A message spanning multiple full blocks plus a partial tail — exercises
    /// the block loop and the message-schedule extension across blocks.
    #[test]
    fn multi_block_message() {
        // One million 'a's — FIPS 180-4 §B.3. A classic long-message vector.
        let msg = vec![b'a'; 1_000_000];
        assert_eq!(
            sha256(&msg).to_hex(),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
        );
    }

    /// The 55/56-byte boundary: 55 bytes fits the length in one padding block,
    /// 56 spills to a second. Both must hash correctly.
    #[test]
    fn padding_boundary_is_correct() {
        // Distinct lengths across the one-/two-block padding boundary give
        // distinct digests — a sanity check that padding encodes the length, not
        // just the bytes. (Determinism itself is covered by `digest_is_deterministic`.)
        assert_ne!(sha256(&[b'x'; 55]), sha256(&[b'x'; 56]));
        assert_ne!(sha256(&[b'x'; 63]), sha256(&[b'x'; 64]));
    }

    #[test]
    fn digest_is_deterministic() {
        assert_eq!(sha256(b"stele"), sha256(b"stele"));
        assert_ne!(sha256(b"stele"), sha256(b"stele "));
    }

    #[test]
    fn zero_is_the_genesis_link() {
        assert_eq!(Digest::ZERO.as_bytes(), &[0u8; SHA256_LEN]);
        assert_eq!(Digest::ZERO.to_hex(), "0".repeat(64));
        // A real hash is overwhelmingly unlikely to be the genesis sentinel.
        assert_ne!(sha256(b""), Digest::ZERO);
    }
}
