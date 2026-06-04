//! CRC32C ([Castagnoli], poly `0x1EDC6F41` reflected = `0x82F63B78`).
//!
//! Shared by every checksum the storage engine writes today —
//! WAL record framing ([`crate::wal`]) and sealed-segment page/footer
//! integrity ([`crate::segment`]). A single implementation keeps the
//! "frame survives torn writes" contract uniform and means a future
//! hardware-accelerated drop-in benefits every consumer at once.
//!
//! [Castagnoli]: https://datatracker.ietf.org/doc/html/rfc3720#appendix-B.4

/// Castagnoli polynomial 0x1EDC6F41, reflected for LSB-first processing.
const CRC_POLY_REFLECTED: u32 = 0x82F6_3B78;

const TABLE: [u32; 256] = {
    let mut t = [0u32; 256];
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
        t[i as usize] = crc;
        i += 1;
    }
    t
};

/// Compute the CRC32C of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &b in bytes {
        let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
    }
    !crc
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
}
