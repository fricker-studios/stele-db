//! `SeededRng` — a ChaCha20-backed deterministic PRNG ([STL-108]).
//!
//! The simulator's only source of randomness. A single `u64` seed expands into a
//! 256-bit ChaCha20 key; the cipher's keystream *is* the random stream. ChaCha20
//! gives a far longer period and better statistical spread than the crate's older
//! `xorshift64*` [`crate::Rng`], which matters when the scheduler uses it to
//! explore the space of task interleavings ([`crate::scheduler`]).
//!
//! Implemented inline rather than pulling `rand_chacha` — keeping the workspace's
//! third-party surface small is a deliberate project value (see the root
//! `Cargo.toml`), and the [`crate::Rng`] before it set the dependency-free
//! precedent. The block function is the reference ChaCha20 from
//! [RFC 8439](https://datatracker.ietf.org/doc/html/rfc8439) §2.3 and is checked
//! against the §2.3.2 test vector in the unit tests below.

/// ChaCha20's four constant words: the ASCII of `"expand 32-byte k"`, read as
/// little-endian `u32`s (RFC 8439 §2.3).
const CONSTANTS: [u32; 4] = [0x6170_7865, 0x3320_646e, 0x7962_2d32, 0x6b20_6574];

/// The ChaCha quarter-round (RFC 8439 §2.1) applied in place to four state words.
///
/// The single-letter indices `a`/`b`/`c`/`d` mirror RFC 8439's own notation.
#[inline]
#[allow(clippy::many_single_char_names)]
fn quarter_round(s: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize) {
    s[a] = s[a].wrapping_add(s[b]);
    s[d] = (s[d] ^ s[a]).rotate_left(16);
    s[c] = s[c].wrapping_add(s[d]);
    s[b] = (s[b] ^ s[c]).rotate_left(12);
    s[a] = s[a].wrapping_add(s[b]);
    s[d] = (s[d] ^ s[a]).rotate_left(8);
    s[c] = s[c].wrapping_add(s[d]);
    s[b] = (s[b] ^ s[c]).rotate_left(7);
}

/// The ChaCha20 block function (RFC 8439 §2.3): 20 rounds (10 column + diagonal
/// double-rounds) over the initial state, added back to it. Returns the 16-word
/// keystream block.
fn chacha20_block(key: &[u32; 8], counter: u32, nonce: &[u32; 3]) -> [u32; 16] {
    let mut state = [0u32; 16];
    state[0..4].copy_from_slice(&CONSTANTS);
    state[4..12].copy_from_slice(key);
    state[12] = counter;
    state[13..16].copy_from_slice(nonce);

    let mut working = state;
    for _ in 0..10 {
        // Column rounds.
        quarter_round(&mut working, 0, 4, 8, 12);
        quarter_round(&mut working, 1, 5, 9, 13);
        quarter_round(&mut working, 2, 6, 10, 14);
        quarter_round(&mut working, 3, 7, 11, 15);
        // Diagonal rounds.
        quarter_round(&mut working, 0, 5, 10, 15);
        quarter_round(&mut working, 1, 6, 11, 12);
        quarter_round(&mut working, 2, 7, 8, 13);
        quarter_round(&mut working, 3, 4, 9, 14);
    }
    for (w, s) in working.iter_mut().zip(state.iter()) {
        *w = w.wrapping_add(*s);
    }
    working
}

/// A deterministic ChaCha20-backed PRNG seeded from a single `u64`.
///
/// Same seed ⇒ identical stream, on any machine — the property the whole
/// deterministic-simulation strategy rests on
/// ([ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)). The
/// keystream is produced one 64-byte block at a time and drained word by word; a
/// fresh block is generated (with an incremented counter) when the buffer empties.
#[derive(Debug, Clone)]
pub struct SeededRng {
    key: [u32; 8],
    nonce: [u32; 3],
    counter: u32,
    buf: [u32; 16],
    /// Index of the next unused word in `buf`; `16` means "empty, refill first".
    pos: usize,
}

impl SeededRng {
    /// Seed the generator. The `u64` is expanded with `splitmix64` into the full
    /// 256-bit key *and* the 96-bit nonce, so even adjacent seeds produce
    /// well-separated cipher state (and therefore unrelated streams). Both are
    /// derived from the seed — there is no fixed key material or nonce.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut s = seed;
        // splitmix64 — a standard, well-mixed `u64 -> u64` step.
        let mut splitmix = move || {
            s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        // Split one splitmix64 output into two little-endian `u32` words.
        let split = |v: u64| {
            let b = v.to_le_bytes();
            [
                u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
                u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
            ]
        };
        // Key (256-bit) and nonce (96-bit) are both built straight from
        // splitmix64 output — every word traces to the seed, with no constant
        // array in the dataflow (so neither is a fixed value).
        let mut key = [0u32; 8];
        for pair in key.chunks_exact_mut(2) {
            pair.copy_from_slice(&split(splitmix()));
        }
        let [n0, n1] = split(splitmix());
        let [n2, _] = split(splitmix());
        Self {
            key,
            nonce: [n0, n1, n2],
            counter: 0,
            buf: [0; 16],
            pos: 16,
        }
    }

    /// Generate the next keystream block and reset the read cursor.
    fn refill(&mut self) {
        self.buf = chacha20_block(&self.key, self.counter, &self.nonce);
        self.counter = self.counter.wrapping_add(1);
        self.pos = 0;
    }

    /// Next pseudo-random `u32`.
    pub fn next_u32(&mut self) -> u32 {
        if self.pos >= self.buf.len() {
            self.refill();
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        v
    }

    /// Next pseudo-random `u64`, low word first.
    pub fn next_u64(&mut self) -> u64 {
        let lo = u64::from(self.next_u32());
        let hi = u64::from(self.next_u32());
        (hi << 32) | lo
    }

    /// Uniform `usize` in `0..bound`. `bound` must be non-zero (a zero `bound`
    /// panics on the modulo).
    ///
    /// Uses rejection sampling, so the result is unbiased even when `bound` does
    /// not divide 2⁶⁴ — the scheduler relies on this for a genuinely uniform
    /// choice among ready tasks.
    pub fn below_usize(&mut self, bound: usize) -> usize {
        let bound = bound as u64;
        // Reject the unfair tail: the top `2^64 % bound` values would over-
        // represent the low residues. `0 - bound` wraps to `2^64 - bound`, whose
        // remainder mod `bound` is exactly `2^64 % bound`.
        let reject_below = 0u64.wrapping_sub(bound) % bound;
        loop {
            let v = self.next_u64();
            if v >= reject_below {
                return usize::try_from(v % bound).expect("value < bound fits usize");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pack 32 key bytes into the 8 little-endian words ChaCha20 uses.
    fn key_words(bytes: &[u8; 32]) -> [u32; 8] {
        let mut k = [0u32; 8];
        for (w, chunk) in k.iter_mut().zip(bytes.chunks_exact(4)) {
            *w = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        k
    }

    /// RFC 8439 §2.3.2 block-function test vector — the canonical proof that the
    /// ChaCha20 core is byte-correct.
    #[test]
    fn rfc8439_block_vector() {
        let key_bytes: [u8; 32] = std::array::from_fn(|i| u8::try_from(i).expect("i < 32")); // 00,01,..,1f
        let key = key_words(&key_bytes);
        let nonce = [
            u32::from_le_bytes([0x00, 0x00, 0x00, 0x09]),
            u32::from_le_bytes([0x00, 0x00, 0x00, 0x4a]),
            u32::from_le_bytes([0x00, 0x00, 0x00, 0x00]),
        ];
        let counter = 1u32;

        let out = chacha20_block(&key, counter, &nonce);

        // The expected serialized keystream block from RFC 8439 §2.3.2.
        #[rustfmt::skip]
        let expected_bytes: [u8; 64] = [
            0x10, 0xf1, 0xe7, 0xe4, 0xd1, 0x3b, 0x59, 0x15, 0x50, 0x0f, 0xdd, 0x1f, 0xa3, 0x20, 0x71, 0xc4,
            0xc7, 0xd1, 0xf4, 0xc7, 0x33, 0xc0, 0x68, 0x03, 0x04, 0x22, 0xaa, 0x9a, 0xc3, 0xd4, 0x6c, 0x4e,
            0xd2, 0x82, 0x64, 0x46, 0x07, 0x9f, 0xaa, 0x09, 0x14, 0xc2, 0xd7, 0x05, 0xd9, 0x8b, 0x02, 0xa2,
            0xb5, 0x12, 0x9c, 0xd1, 0xde, 0x16, 0x4e, 0xb9, 0xcb, 0xd0, 0x83, 0xe8, 0xa2, 0x50, 0x3c, 0x4e,
        ];
        let mut expected = [0u32; 16];
        for (w, chunk) in expected.iter_mut().zip(expected_bytes.chunks_exact(4)) {
            *w = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        assert_eq!(out, expected);
    }

    #[test]
    fn same_seed_same_stream() {
        let mut a = SeededRng::new(0xDEAD_BEEF);
        let mut b = SeededRng::new(0xDEAD_BEEF);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = SeededRng::new(1);
        let mut b = SeededRng::new(2);
        // Astronomically unlikely to collide on the first draw if the seeds are
        // properly expanded into distinct keys.
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn block_boundary_is_seamless() {
        // Draw across the 16-word block boundary; refill must be transparent.
        let mut r = SeededRng::new(42);
        let drawn: Vec<u32> = (0..40).map(|_| r.next_u32()).collect();
        let mut r2 = SeededRng::new(42);
        for (i, &want) in drawn.iter().enumerate() {
            assert_eq!(r2.next_u32(), want, "word {i} diverged across refill");
        }
    }

    #[test]
    fn below_usize_in_range() {
        let mut r = SeededRng::new(7);
        for _ in 0..10_000 {
            assert!(r.below_usize(5) < 5);
        }
    }
}
