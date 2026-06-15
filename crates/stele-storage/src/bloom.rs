//! A small Bloom filter over byte keys — the shared membership primitive behind
//! the validity-index spill summary ([STL-142]) and the per-segment business-key
//! filter the hash/bloom index family persists in each sealed segment ([STL-238]).
//!
//! ## No false negatives
//!
//! A `false` from [`KeyBloom::maybe_contains`] **proves** the key was never
//! inserted, so a caller may soundly skip the work the key would have driven (a
//! spill read, a segment scan). A positive answer may be wrong — the caller then
//! does the work and finds nothing, which is correct, just not free. The
//! false-positive rate is bounded by the `bits_per_key` sizing and
//! [`BLOOM_HASHES`]; it never affects correctness, only how much work is skipped.
//!
//! ## Determinism
//!
//! Hashing is FNV-1a with two independent seeds combined by double hashing
//! (`h1 + i·h2`), so the same key set always yields the same filter — it never
//! perturbs the simulation digest, and (being read-gating only) could not change
//! a result even if it did. The bit count is always a power of two, so the
//! address mask is `bit_count - 1`.
//!
//! ## Persistence ([STL-238])
//!
//! The spill bloom is in-memory only (a spill carries no durability claim). The
//! per-segment bloom is written into the sealed segment's footer via
//! [`KeyBloom::encode`] and read back with [`KeyBloom::decode`], so it survives
//! flush, compaction, and recovery exactly because it rides the immutable
//! segment it summarizes — there is no separate derived structure to rebuild.
//!
//! [STL-142]: https://allegromusic.atlassian.net/browse/STL-142
//! [STL-238]: https://allegromusic.atlassian.net/browse/STL-238

/// Number of bit probes per key. Four keeps the false-positive rate low at the
/// default ~12-bits-per-key sizing without over-hashing. Frozen for the on-disk
/// segment bloom: [`KeyBloom::decode`] refuses any other value, so a future
/// change is a deliberate, version-gated format break rather than a silent
/// mis-probe of existing segments.
pub(crate) const BLOOM_HASHES: usize = 4;

/// Default bits per key. The validity spill ([STL-142]) sizes itself here, and
/// the per-segment bloom writer's knob
/// ([`SegmentWriter::with_bloom_bits_per_key`](crate::segment::SegmentWriter::with_bloom_bits_per_key))
/// defaults to it: ~12 bits/key with [`BLOOM_HASHES`] probes keeps the
/// false-positive rate near 1%.
pub(crate) const DEFAULT_BITS_PER_KEY: usize = 12;

/// Errors decoding a persisted bloom ([STL-238]).
#[derive(Debug, thiserror::Error)]
pub(crate) enum BloomError {
    /// The encoded bloom is truncated or structurally invalid (a non-power-of-two
    /// bit count, an unknown probe count, or a short buffer).
    #[error("malformed segment bloom: {0}")]
    Malformed(&'static str),
}

/// A tiny Bloom filter over byte keys. See the [module docs](self) for the
/// no-false-negatives contract and the determinism argument.
#[derive(Debug, Clone)]
pub(crate) struct KeyBloom {
    /// Bit storage; the number of addressable bits is `bits.len() * 64`, always a
    /// power of two so the address mask is `bit_count - 1`.
    bits: Vec<u64>,
    /// `bit_count - 1`, for masking a hash down to a bit position.
    mask: u64,
}

impl KeyBloom {
    /// Build a filter over `keys` at `bits_per_key` bits per key (duplicates are
    /// harmless). Sized to `bits_per_key * len` rounded up to a power of two with
    /// a 512-bit floor, so even a one- or two-key set keeps the false-positive
    /// rate negligible.
    pub(crate) fn build<'a>(
        bits_per_key: usize,
        keys: impl ExactSizeIterator<Item = &'a [u8]>,
    ) -> Self {
        let bit_count = keys
            .len()
            .saturating_mul(bits_per_key.max(1))
            .next_power_of_two()
            .max(512) as u64;
        let mask = bit_count - 1;
        let mut bits = vec![0u64; (bit_count / 64) as usize];
        for key in keys {
            for pos in bit_positions(mask, key) {
                bits[(pos / 64) as usize] |= 1u64 << (pos % 64);
            }
        }
        Self { bits, mask }
    }

    /// Whether every bit for `bytes` is set — `false` proves absence.
    pub(crate) fn maybe_contains(&self, bytes: &[u8]) -> bool {
        bit_positions(self.mask, bytes)
            .into_iter()
            .all(|pos| self.bits[(pos / 64) as usize] & (1u64 << (pos % 64)) != 0)
    }

    /// Append the bloom to `out` for footer persistence ([STL-238]): probe count
    /// (`u8`), word count (`u32` LE), then that many little-endian `u64` words.
    /// The probe count is stored for self-description; [`Self::decode`] requires
    /// it to equal [`BLOOM_HASHES`].
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.push(u8::try_from(BLOOM_HASHES).expect("BLOOM_HASHES fits in u8"));
        out.extend_from_slice(
            &u32::try_from(self.bits.len())
                .expect("bloom word count fits in u32")
                .to_le_bytes(),
        );
        for word in &self.bits {
            out.extend_from_slice(&word.to_le_bytes());
        }
    }

    /// The encoded length in bytes — `1 + 4 + 8 * word_count`. Lets the footer
    /// writer size its buffer without a trial encode.
    pub(crate) const fn encoded_len(&self) -> usize {
        1 + 4 + self.bits.len() * 8
    }

    /// Decode a bloom written by [`Self::encode`], returning it and the number of
    /// bytes consumed.
    ///
    /// # Errors
    ///
    /// [`BloomError::Malformed`] if the buffer is short, the probe count is not
    /// [`BLOOM_HASHES`], the word count is zero, or the resulting bit count is not
    /// a power of two (every filter [`Self::build`] produces is).
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, usize), BloomError> {
        let (&hashes, rest) = bytes
            .split_first()
            .ok_or(BloomError::Malformed("missing probe count"))?;
        if usize::from(hashes) != BLOOM_HASHES {
            return Err(BloomError::Malformed("unsupported probe count"));
        }
        let word_count = rest
            .get(0..4)
            .ok_or(BloomError::Malformed("missing word count"))?;
        let word_count = u32::from_le_bytes(word_count.try_into().expect("4 bytes")) as usize;
        if word_count == 0 {
            return Err(BloomError::Malformed("zero word count"));
        }
        let bit_count = (word_count as u64).saturating_mul(64);
        if !bit_count.is_power_of_two() {
            return Err(BloomError::Malformed("bit count is not a power of two"));
        }
        let words_start = 1 + 4;
        let words_end = words_start + word_count * 8;
        let word_bytes = bytes
            .get(words_start..words_end)
            .ok_or(BloomError::Malformed("truncated bit words"))?;
        let bits = word_bytes
            .chunks_exact(8)
            .map(|w| u64::from_le_bytes(w.try_into().expect("8 bytes")))
            .collect();
        Ok((
            Self {
                bits,
                mask: bit_count - 1,
            },
            words_end,
        ))
    }
}

/// The [`BLOOM_HASHES`] bit positions `bytes` maps to under `mask`, by double
/// hashing two independent FNV-1a seeds (`h1 + i·h2`).
fn bit_positions(mask: u64, bytes: &[u8]) -> [u64; BLOOM_HASHES] {
    let h1 = fnv1a(0xcbf2_9ce4_8422_2325, bytes);
    // OR-in 1 so the stride is odd and never degenerates to a single bit.
    let h2 = fnv1a(0x9e37_79b9_7f4a_7c15, bytes) | 1;
    std::array::from_fn(|i| h1.wrapping_add((i as u64).wrapping_mul(h2)) & mask)
}

/// FNV-1a over `bytes`, seeded so two calls give two independent hashes.
fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_has_no_false_negatives() {
        let keys: Vec<&[u8]> = vec![b"alpha", b"gamma", b"omega", b"zeta"];
        let bloom = KeyBloom::build(DEFAULT_BITS_PER_KEY, keys.iter().copied());
        for k in &keys {
            assert!(bloom.maybe_contains(k), "member must test positive");
        }
    }

    #[test]
    fn bloom_prunes_some_in_range_hole() {
        // Across many absent keys, the bloom must reject at least one — its only
        // guarantee is no false negatives, so asserting *which* key is rejected
        // would over-constrain (a bloom is allowed false positives).
        let bloom = KeyBloom::build(
            DEFAULT_BITS_PER_KEY,
            [b"aaa".as_slice(), b"zzz".as_slice()].into_iter(),
        );
        let pruned = (0..1000)
            .map(|i| format!("m{i:03}").into_bytes())
            .filter(|k| !bloom.maybe_contains(k))
            .count();
        assert!(pruned > 0, "the bloom must prune at least one absent key");
    }

    #[test]
    fn larger_bits_per_key_lowers_the_false_positive_rate() {
        // The configurable knob is load-bearing: more bits per key must not raise
        // the false-positive count over a fixed absent-key probe set. Comparing
        // counts over the same probe set is comparing rates, no floats needed.
        let members: Vec<Vec<u8>> = (0..256).map(|i| format!("k{i:04}").into_bytes()).collect();
        let absent: Vec<Vec<u8>> = (0..4000)
            .map(|i| format!("absent{i:05}").into_bytes())
            .collect();
        let fp_count = |bits_per_key: usize| {
            let bloom = KeyBloom::build(bits_per_key, members.iter().map(Vec::as_slice));
            absent.iter().filter(|k| bloom.maybe_contains(k)).count()
        };
        assert!(
            fp_count(16) <= fp_count(4),
            "more bits per key must not increase the false-positive count",
        );
    }

    #[test]
    fn encode_decode_round_trips() {
        let keys: Vec<Vec<u8>> = (0..50).map(|i| format!("key-{i}").into_bytes()).collect();
        let bloom = KeyBloom::build(DEFAULT_BITS_PER_KEY, keys.iter().map(Vec::as_slice));
        let mut buf = Vec::new();
        // A trailing byte proves `decode` reports the exact consumed length.
        bloom.encode(&mut buf);
        assert_eq!(buf.len(), bloom.encoded_len());
        buf.push(0xAB);
        let (decoded, consumed) = KeyBloom::decode(&buf).expect("round-trips");
        assert_eq!(consumed, bloom.encoded_len());
        for k in &keys {
            assert!(
                decoded.maybe_contains(k),
                "decoded bloom keeps every member"
            );
        }
    }

    #[test]
    fn decode_rejects_malformed() {
        let k = u8::try_from(BLOOM_HASHES).unwrap();
        assert!(KeyBloom::decode(&[]).is_err(), "empty buffer");
        // Unknown probe count.
        assert!(KeyBloom::decode(&[9, 8, 0, 0, 0]).is_err());
        // Zero word count.
        assert!(KeyBloom::decode(&[k, 0, 0, 0, 0]).is_err());
        // Word count claims more words than the buffer holds.
        assert!(KeyBloom::decode(&[k, 8, 0, 0, 0]).is_err());
    }
}
