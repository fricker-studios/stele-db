//! CRC32C ([Castagnoli], poly `0x1EDC6F41` reflected = `0x82F63B78`).
//!
//! Shared by every checksum the storage engine writes today —
//! WAL record framing ([`crate::wal`]) and sealed-segment page/footer
//! integrity ([`crate::segment`]) — and exported for the durable logs the
//! layers above frame the same way (the session catalog log, [ADR-0028]).
//! A single implementation keeps the "frame survives torn writes" contract
//! uniform and means a future hardware-accelerated drop-in benefits every
//! consumer at once.
//!
//! The kernel is **slice-by-8**: eight 256-entry tables let the hot loop fold
//! eight input bytes per iteration instead of one, a several-fold throughput
//! gain over the classic byte-at-a-time table walk on the paths that checksum
//! every byte the engine persists. The tables are built in a `const` block
//! from the same polynomial, so the function stays deterministic, safe, and
//! dependency-free; byte-for-byte compatibility with the historical
//! one-table implementation (the on-disk format's CRC) is pinned by the
//! differential test below.
//!
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
//!
//! [Castagnoli]: https://datatracker.ietf.org/doc/html/rfc3720#appendix-B.4

/// Castagnoli polynomial 0x1EDC6F41, reflected for LSB-first processing.
const CRC_POLY_REFLECTED: u32 = 0x82F6_3B78;

/// Slice-by-8 lookup tables. `TABLES[0]` is the classic byte-at-a-time table;
/// `TABLES[k][i]` extends it to the CRC contribution of a byte `k` positions
/// earlier in the 8-byte window (`t[k][i] = (t[k-1][i] >> 8) ^ t[0][t[k-1][i] & 0xFF]`).
const TABLES: [[u32; 256]; 8] = {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ CRC_POLY_REFLECTED
            };
            j += 1;
        }
        t[0][i as usize] = crc;
        i += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    t
};

/// Compute the CRC32C of `bytes`.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut state = Crc32c::new();
    state.update(bytes);
    state.finish()
}

/// Incremental CRC32C over multiple slices — `new` → `update`… → `finish`
/// yields exactly [`crc32c`] of the concatenation.
///
/// Lets a caller checksum a frame that lives in discontiguous pieces (a chunk
/// header minus its own CRC field, followed by the payload) without first
/// copying them into one contiguous buffer.
#[derive(Debug, Clone, Copy)]
pub struct Crc32c(u32);

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32c {
    /// A fresh state — no bytes folded in yet.
    #[must_use]
    pub const fn new() -> Self {
        Self(!0)
    }

    /// Fold `bytes` into the running CRC.
    pub fn update(&mut self, bytes: &[u8]) {
        let mut crc = self.0;
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            // Two aligned LE words; the low word folds the current CRC in.
            let low = u32::from_le_bytes(chunk[0..4].try_into().expect("4 bytes")) ^ crc;
            let high = u32::from_le_bytes(chunk[4..8].try_into().expect("4 bytes"));
            crc = TABLES[7][(low & 0xFF) as usize]
                ^ TABLES[6][((low >> 8) & 0xFF) as usize]
                ^ TABLES[5][((low >> 16) & 0xFF) as usize]
                ^ TABLES[4][(low >> 24) as usize]
                ^ TABLES[3][(high & 0xFF) as usize]
                ^ TABLES[2][((high >> 8) & 0xFF) as usize]
                ^ TABLES[1][((high >> 16) & 0xFF) as usize]
                ^ TABLES[0][(high >> 24) as usize];
        }
        for &b in chunks.remainder() {
            crc = TABLES[0][((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
        }
        self.0 = crc;
    }

    /// The CRC32C of everything folded in so far. Non-consuming, so a caller
    /// may checkpoint a running frame checksum and keep folding.
    #[must_use]
    pub const fn finish(&self) -> u32 {
        !self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical CRC32C test vector — `"123456789"` per RFC 3720 Appendix B.4.
    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn crc32c_empty_is_zero() {
        // CRC32C of the empty string is 0 (per the reflected/inverted convention).
        assert_eq!(crc32c(b""), 0);
    }

    /// The historical byte-at-a-time implementation, kept as the reference the
    /// slice-by-8 kernel must match bit-for-bit: these CRCs are stamped into
    /// the on-disk format, so any divergence would refuse every existing file.
    fn crc32c_reference(bytes: &[u8]) -> u32 {
        let mut crc: u32 = !0;
        for &b in bytes {
            let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
            crc = TABLES[0][idx] ^ (crc >> 8);
        }
        !crc
    }

    /// Slice-by-8 must agree with the reference at every length that
    /// exercises the 8-byte main loop and each possible remainder tail,
    /// across shifting content.
    #[test]
    #[allow(clippy::cast_possible_truncation)] // deliberate: keep the low byte of the mix
    fn slice_by_8_matches_reference_at_every_alignment() {
        let data: Vec<u8> = (0..1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        for len in 0..=64 {
            assert_eq!(
                crc32c(&data[..len]),
                crc32c_reference(&data[..len]),
                "len {len}"
            );
        }
        for start in 0..8 {
            assert_eq!(
                crc32c(&data[start..]),
                crc32c_reference(&data[start..]),
                "start {start}"
            );
        }
    }

    /// Splitting the input across `update` calls at any boundary must equal
    /// the one-shot digest — the contract `read_chunk`'s two-piece frame
    /// checksum relies on.
    #[test]
    fn incremental_updates_match_one_shot() {
        let data: Vec<u8> = (0..=255).collect();
        let whole = crc32c(&data);
        for split in [0, 1, 7, 8, 9, 128, 255, 256] {
            let mut state = Crc32c::new();
            state.update(&data[..split]);
            state.update(&data[split..]);
            assert_eq!(state.finish(), whole, "split {split}");
        }
    }
}
