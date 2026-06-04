//! WAL record encoding.
//!
//! Frame layout (little-endian, fixed 8-byte header):
//!
//! ```text
//! +--------+--------+----------------+
//! | len:u32| crc:u32| payload (len)  |
//! +--------+--------+----------------+
//! ```
//!
//! `crc` is CRC32C ([Castagnoli], poly `0x1EDC6F41` reflected = `0x82F63B78`)
//! over the *payload only*. The framing fields are recoverable on their own —
//! a corrupt frame is one whose CRC does not match its declared length.
//!
//! CRC32C is implemented inline (no external dependency). A hardware-accelerated
//! impl can drop in later behind the same function name without touching
//! callers.
//!
//! [Castagnoli]: https://datatracker.ietf.org/doc/html/rfc3720#appendix-B.4

pub(crate) const HEADER_LEN: usize = 8;

/// Maximum payload size (16 MiB).
///
/// Keeps `len` interpretable as a 32-bit checked field and caps the allocation
/// a corrupt-length frame can demand during replay. More than enough for any
/// single WAL record we expect to write at v0.1.
pub const MAX_PAYLOAD_LEN: u32 = 16 * 1024 * 1024;

/// Encode `payload` into a freshly-framed record (header + payload bytes).
pub(crate) fn encode(payload: &[u8], out: &mut Vec<u8>) {
    let len =
        u32::try_from(payload.len()).expect("payload len must fit in u32 — checked by Wal::append");
    let crc = crc32c(payload);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// Parsed header — does not yet imply the payload is intact.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Header {
    pub len: u32,
    pub crc: u32,
}

/// Parse a header from `buf[..8]`. Returns `None` if `buf` is too short.
pub(crate) fn parse_header(buf: &[u8]) -> Option<Header> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
    let crc = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
    Some(Header { len, crc })
}

/// Verify `payload` matches `header.crc`.
pub(crate) fn verify(header: Header, payload: &[u8]) -> bool {
    header.len as usize == payload.len() && crc32c(payload) == header.crc
}

// --- CRC32C (Castagnoli, reflected) -----------------------------------------

// Castagnoli polynomial 0x1EDC6F41, reflected for LSB-first processing.
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

    #[test]
    fn round_trip_encode_decode() {
        let payload = b"hello, wal";
        let mut buf = Vec::new();
        encode(payload, &mut buf);
        let header = parse_header(&buf).expect("header");
        assert_eq!(header.len as usize, payload.len());
        assert!(verify(header, &buf[HEADER_LEN..]));
    }

    #[test]
    fn corrupt_payload_fails_verify() {
        let mut buf = Vec::new();
        encode(b"hello", &mut buf);
        buf[HEADER_LEN] ^= 0x01; // flip a bit in the payload
        let header = parse_header(&buf).expect("header");
        assert!(!verify(header, &buf[HEADER_LEN..]));
    }
}
