//! The SHA-2 hashes ([FIPS 180-4]) the engine relies on.
//!
//! **SHA-256** backs the tamper-evident commit log; **SHA-384 / SHA-512** back
//! RFC 5929 `tls-server-end-point` channel binding on the auth path (STL-330).
//!
//! The verifiable audit log ([ADR-0026](../../../docs/adr/0026-verifiable-audit-log.md))
//! hash-chains each commit to its predecessor with SHA-256, and the Merkle
//! inclusion / consistency proofs (~v0.5) build on the same hash. That makes the
//! hash a long-term, externally-observable format commitment, so it lives here in
//! the dependency root where every layer — the commit-log writer today, the Merkle
//! tree tomorrow — names one implementation.
//!
//! SHA-384/512 join it for a sibling reason: RFC 5929 §4.1 selects the channel
//! binding hash from the server certificate's signature algorithm, so a
//! SHA-384/512-signed cert must be bound with that stronger digest, for
//! `stele_pgwire`'s `tls-server-end-point` channel binding ([STL-330]). They are
//! vendored here next to SHA-256 so the dependency-sensitive auth path names the
//! same in-tree implementations the commit log does.
//!
//! ## Why a vendored implementation
//!
//! Like the CRC32C in `stele_storage::checksum` and the simulation PRNG, these
//! are small, fully-specified standards kept dependency-free on purpose: the
//! workspace treats every third-party crate as a supply-chain surface and a
//! `cargo-deny` decision ([Cargo.toml](../../../Cargo.toml)), and a cryptographic
//! primitive on the audit and auth paths is exactly where a hidden transitive
//! dependency is least welcome. Correctness is pinned to the [FIPS 180-4]
//! known-answer vectors in the tests below; the exact hash functions are
//! ADR-0026's (commit log) and RFC 5929's (channel binding) to choose, and both
//! chose standard ones.
//!
//! [FIPS 180-4]: https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
//! [STL-330]: https://allegromusic.atlassian.net/browse/STL-330

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

/// The length in bytes of a SHA-512 digest.
pub const SHA512_LEN: usize = 64;

/// The length in bytes of a SHA-384 digest.
pub const SHA384_LEN: usize = 48;

/// Compute the SHA-512 digest of `bytes` ([FIPS 180-4] §6.4).
///
/// A direct, allocation-free implementation. Vendored for the same supply-chain
/// reason as [`sha256`] — a cryptographic primitive on the auth path is where a
/// hidden transitive dependency is least welcome — and used for the RFC 5929
/// `tls-server-end-point` binding of a SHA-512-signed server certificate
/// (STL-330). Deterministic and runtime-agnostic.
///
/// [FIPS 180-4]: https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
#[must_use]
pub fn sha512(bytes: &[u8]) -> [u8; SHA512_LEN] {
    let h = sha512_core(bytes, H0_512);
    let mut out = [0u8; SHA512_LEN];
    for (word, chunk) in h.iter().zip(out.chunks_exact_mut(8)) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// Compute the SHA-384 digest of `bytes` ([FIPS 180-4] §6.5).
///
/// SHA-384 is SHA-512 with a distinct initial hash value and the output
/// truncated to its first six 64-bit words (48 bytes); the two share one
/// `sha512_core`. Same vendoring rationale and channel-binding use as
/// [`sha512`] (STL-330).
///
/// [FIPS 180-4]: https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
#[must_use]
pub fn sha384(bytes: &[u8]) -> [u8; SHA384_LEN] {
    let h = sha512_core(bytes, H0_384);
    let mut out = [0u8; SHA384_LEN];
    // SHA-384 emits the first six of the eight working words.
    for (word, chunk) in h.iter().take(6).zip(out.chunks_exact_mut(8)) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// The SHA-512 message schedule + round function over the whole message, from an
/// initial hash value `h0` ([FIPS 180-4] §6.4). SHA-512 and SHA-384 differ only
/// in `h0` and how much of the result they keep, so they share this core.
fn sha512_core(bytes: &[u8], h0: [u64; 8]) -> [u64; 8] {
    let mut h = h0;

    // Process every full 128-byte block of the message.
    let mut blocks = bytes.chunks_exact(128);
    for block in &mut blocks {
        compress512(
            &mut h,
            block.try_into().expect("chunks_exact yields 128 bytes"),
        );
    }

    // The padding block(s): the trailing partial block, a `0x80` byte, zero
    // padding, and the 128-bit big-endian bit length. This needs one or two
    // extra blocks depending on whether the tail leaves room for the 16-byte
    // length (the boundary is at 112 = 128 − 16).
    let rest = blocks.remainder();
    let bit_len = (bytes.len() as u128).wrapping_mul(8);
    let mut tail = [0u8; 256];
    tail[..rest.len()].copy_from_slice(rest);
    tail[rest.len()] = 0x80;
    let tail_len = if rest.len() < 112 { 128 } else { 256 };
    tail[tail_len - 16..tail_len].copy_from_slice(&bit_len.to_be_bytes());
    for block in tail[..tail_len].chunks_exact(128) {
        compress512(&mut h, block.try_into().expect("128-byte block"));
    }

    h
}

/// The SHA-512 compression function over one 128-byte block ([FIPS 180-4] §6.4.2).
// `a`..`h` and `w` are FIPS 180-4's working-variable names verbatim; the
// single-char names are deliberate (renaming would make the round harder to
// check against the standard), exactly as in [`compress`].
#[allow(clippy::many_single_char_names)]
fn compress512(h: &mut [u64; 8], block: &[u8; 128]) {
    let mut w = [0u64; 80];
    for (i, chunk) in block.chunks_exact(8).enumerate() {
        w[i] = u64::from_be_bytes(chunk.try_into().expect("8-byte word"));
    }
    for i in 16..80 {
        let s0 = w[i - 15].rotate_right(1) ^ w[i - 15].rotate_right(8) ^ (w[i - 15] >> 7);
        let s1 = w[i - 2].rotate_right(19) ^ w[i - 2].rotate_right(61) ^ (w[i - 2] >> 6);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }

    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
    for i in 0..80 {
        let s1 = e.rotate_right(14) ^ e.rotate_right(18) ^ e.rotate_right(41);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K512[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(28) ^ a.rotate_right(34) ^ a.rotate_right(39);
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

/// SHA-512 initial hash values — fractional parts of the square roots of the
/// first eight primes ([FIPS 180-4] §5.3.5).
const H0_512: [u64; 8] = [
    0x6a09_e667_f3bc_c908,
    0xbb67_ae85_84ca_a73b,
    0x3c6e_f372_fe94_f82b,
    0xa54f_f53a_5f1d_36f1,
    0x510e_527f_ade6_82d1,
    0x9b05_688c_2b3e_6c1f,
    0x1f83_d9ab_fb41_bd6b,
    0x5be0_cd19_137e_2179,
];

/// SHA-384 initial hash values — fractional parts of the square roots of the
/// ninth through sixteenth primes ([FIPS 180-4] §5.3.4).
const H0_384: [u64; 8] = [
    0xcbbb_9d5d_c105_9ed8,
    0x629a_292a_367c_d507,
    0x9159_015a_3070_dd17,
    0x152f_ecd8_f70e_5939,
    0x6733_2667_ffc0_0b31,
    0x8eb4_4a87_6858_1511,
    0xdb0c_2e0d_64f9_8fa7,
    0x47b5_481d_befa_4fa4,
];

/// Round constants shared by SHA-512 and SHA-384 — fractional parts of the cube
/// roots of the first eighty primes ([FIPS 180-4] §4.2.3).
const K512: [u64; 80] = [
    0x428a_2f98_d728_ae22,
    0x7137_4491_23ef_65cd,
    0xb5c0_fbcf_ec4d_3b2f,
    0xe9b5_dba5_8189_dbbc,
    0x3956_c25b_f348_b538,
    0x59f1_11f1_b605_d019,
    0x923f_82a4_af19_4f9b,
    0xab1c_5ed5_da6d_8118,
    0xd807_aa98_a303_0242,
    0x1283_5b01_4570_6fbe,
    0x2431_85be_4ee4_b28c,
    0x550c_7dc3_d5ff_b4e2,
    0x72be_5d74_f27b_896f,
    0x80de_b1fe_3b16_96b1,
    0x9bdc_06a7_25c7_1235,
    0xc19b_f174_cf69_2694,
    0xe49b_69c1_9ef1_4ad2,
    0xefbe_4786_384f_25e3,
    0x0fc1_9dc6_8b8c_d5b5,
    0x240c_a1cc_77ac_9c65,
    0x2de9_2c6f_592b_0275,
    0x4a74_84aa_6ea6_e483,
    0x5cb0_a9dc_bd41_fbd4,
    0x76f9_88da_8311_53b5,
    0x983e_5152_ee66_dfab,
    0xa831_c66d_2db4_3210,
    0xb003_27c8_98fb_213f,
    0xbf59_7fc7_beef_0ee4,
    0xc6e0_0bf3_3da8_8fc2,
    0xd5a7_9147_930a_a725,
    0x06ca_6351_e003_826f,
    0x1429_2967_0a0e_6e70,
    0x27b7_0a85_46d2_2ffc,
    0x2e1b_2138_5c26_c926,
    0x4d2c_6dfc_5ac4_2aed,
    0x5338_0d13_9d95_b3df,
    0x650a_7354_8baf_63de,
    0x766a_0abb_3c77_b2a8,
    0x81c2_c92e_47ed_aee6,
    0x9272_2c85_1482_353b,
    0xa2bf_e8a1_4cf1_0364,
    0xa81a_664b_bc42_3001,
    0xc24b_8b70_d0f8_9791,
    0xc76c_51a3_0654_be30,
    0xd192_e819_d6ef_5218,
    0xd699_0624_5565_a910,
    0xf40e_3585_5771_202a,
    0x106a_a070_32bb_d1b8,
    0x19a4_c116_b8d2_d0c8,
    0x1e37_6c08_5141_ab53,
    0x2748_774c_df8e_eb99,
    0x34b0_bcb5_e19b_48a8,
    0x391c_0cb3_c5c9_5a63,
    0x4ed8_aa4a_e341_8acb,
    0x5b9c_ca4f_7763_e373,
    0x682e_6ff3_d6b2_b8a3,
    0x748f_82ee_5def_b2fc,
    0x78a5_636f_4317_2f60,
    0x84c8_7814_a1f0_ab72,
    0x8cc7_0208_1a64_39ec,
    0x90be_fffa_2363_1e28,
    0xa450_6ceb_de82_bde9,
    0xbef9_a3f7_b2c6_7915,
    0xc671_78f2_e372_532b,
    0xca27_3ece_ea26_619c,
    0xd186_b8c7_21c0_c207,
    0xeada_7dd6_cde0_eb1e,
    0xf57d_4f7f_ee6e_d178,
    0x06f0_67aa_7217_6fba,
    0x0a63_7dc5_a2c8_98a6,
    0x113f_9804_bef9_0dae,
    0x1b71_0b35_131c_471b,
    0x28db_77f5_2304_7d84,
    0x32ca_ab7b_40c7_2493,
    0x3c9e_be0a_15c9_bebc,
    0x431d_67c4_9c10_0d4c,
    0x4cc5_d4be_cb3e_42b6,
    0x597f_299c_fc65_7e2a,
    0x5fcb_6fab_3ad6_faec,
    0x6c44_198c_4a47_5817,
];

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

    /// Lowercase hex of a digest, for comparing against the published vectors.
    fn hex(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble < 16"));
            s.push(char::from_digit(u32::from(b & 0x0F), 16).expect("nibble < 16"));
        }
        s
    }

    /// The 112-byte two-block message from FIPS 180-4 §B.4 — long enough that the
    /// `0x80` pad plus the 16-byte length spill past the first 128-byte block,
    /// exercising the second padding block for both SHA-384 and SHA-512.
    const TWO_BLOCK: &[u8] =
        b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";

    /// FIPS 180-4 / NIST CAVP known-answer vectors for SHA-512 (§6.4) — the
    /// canonical proof the vendored implementation matches the standard.
    #[test]
    fn sha512_fips_known_answer_vectors() {
        // Empty message.
        assert_eq!(
            hex(&sha512(b"")),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
             47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
        );
        // "abc" — the canonical §B.1 example.
        assert_eq!(
            hex(&sha512(b"abc")),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
        );
        // 112 bytes — forces the two-block padding boundary.
        assert_eq!(
            hex(&sha512(TWO_BLOCK)),
            "8e959b75dae313da8cf4f72814fc143f8f7779c6eb9f7fa17299aeadb6889018\
             501d289e4900f7e4331b99dec4b5433ac7d329eeb6dd26545e96e55b874be909",
        );
    }

    /// FIPS 180-4 / NIST CAVP known-answer vectors for SHA-384 (§6.5).
    #[test]
    fn sha384_fips_known_answer_vectors() {
        assert_eq!(
            hex(&sha384(b"")),
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da\
             274edebfe76f65fbd51ad2f14898b95b",
        );
        assert_eq!(
            hex(&sha384(b"abc")),
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed\
             8086072ba1e7cc2358baeca134c825a7",
        );
        assert_eq!(
            hex(&sha384(TWO_BLOCK)),
            "09330c33f71147e83d192fc782cd1b4753111b173b3b05d22fa08086e3b0f712\
             fcc7c71a557e2db966c3e9fa91746039",
        );
    }

    /// A message spanning many full blocks plus a partial tail — the classic
    /// one-million-'a's long-message vector (FIPS 180-4 §B.3) for both digests.
    #[test]
    fn sha512_and_sha384_multi_block_message() {
        let msg = vec![b'a'; 1_000_000];
        assert_eq!(
            hex(&sha512(&msg)),
            "e718483d0ce769644e2e42c7bc15b4638e1f98b13b2044285632a803afa973eb\
             de0ff244877ea60a4cb0432ce577c31beb009c5c2c49aa2e4eadb217ad8cc09b",
        );
        assert_eq!(
            hex(&sha384(&msg)),
            "9d0e1809716474cb086e834e310a4a1ced149e9c00f248527972cec5704c2a5b\
             07b8b3dc38ecc4ebae97ddd87f3d8985",
        );
    }

    /// The 111/112-byte boundary: 111 bytes leaves room for the 16-byte length in
    /// one padding block, 112 spills to a second. Both must hash correctly, and
    /// distinctly — a sanity check that the padding encodes the length.
    #[test]
    fn sha512_padding_boundary_is_correct() {
        assert_ne!(sha512(&[b'x'; 111]), sha512(&[b'x'; 112]));
        assert_ne!(sha512(&[b'x'; 127]), sha512(&[b'x'; 128]));
        assert_ne!(sha384(&[b'x'; 111]), sha384(&[b'x'; 112]));
    }

    #[test]
    fn digest_lengths_match_the_standard() {
        assert_eq!(sha512(b"stele").len(), SHA512_LEN);
        assert_eq!(sha384(b"stele").len(), SHA384_LEN);
        // SHA-384 is not a truncation of SHA-512 (distinct IVs) — the shared
        // prefix must differ, or the distinct-IV wiring is wrong.
        assert_ne!(sha384(b"stele")[..], sha512(b"stele")[..SHA384_LEN]);
    }
}
