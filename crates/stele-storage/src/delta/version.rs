//! `Version` — the unit of work that flows through the delta tier.
//!
//! A version is the in-memory representation of one row in a logical row's
//! bitemporal version chain ([architecture §2](../../../../docs/02-architecture.md#2-the-bitemporal-record-model)):
//!
//! * `business_key` — the user/PK or hash key (opaque bytes).
//! * `sys_from` — system-time at which this version became current.
//! * `sys_to` — system-time at which it was superseded; `SYSTEM_TIME_OPEN`
//!   while the version is the current one.
//! * `payload` — the column values, encoded by a layer above. The delta tier
//!   treats this as opaque bytes.
//!
//! Valid-time columns are *deliberately absent* at this layer: valid-time is a
//! per-table opt-in ([architecture §12 invariant 4](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants))
//! and the delta tier itself does not interpret them — they will travel inside
//! `payload` once [STL-92] lands.
//!
//! ## Encoding
//!
//! [`Version::encode`] / [`Version::decode`] share one binary frame, used for
//! both WAL records and spill files. Layout (little-endian, header is 24 B):
//!
//! ```text
//! +----------------+----------------+-----------+-----------+----------------+--------------+
//! | business_len:u32| payload_len:u32| sys_from:i64| sys_to:i64| business_key  | payload      |
//! +----------------+----------------+-----------+-----------+----------------+--------------+
//! ```
//!
//! No CRC here — the WAL frames its own records ([`crate::wal`]), and spill
//! files are non-durable by design (the delta is rebuilt from WAL on crash).

use stele_common::time::SystemTimeMicros;

use super::DeltaError;

/// Header size in bytes for the [`Version`] binary encoding.
pub(crate) const HEADER_LEN: usize = 24;

/// Per-frame ceiling that guards against runaway allocations when decoding a
/// corrupt frame. The WAL itself enforces `MAX_PAYLOAD_LEN = 16 MiB`, so a
/// delta-tier frame can never legitimately exceed that.
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Opaque business-key bytes — the user/PK or hash key for a logical row.
///
/// The delta tier does not interpret the bytes; equality and ordering are the
/// usual byte-wise lexicographic comparison. Callers that want a fixed-width
/// hash key can supply it directly; callers with variable-length keys can pass
/// them as-is. Either way, the sort order is `(business_key, sys_from)`, so
/// every version of one key forms a physically-local cluster
/// ([architecture §3.1](../../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving)).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BusinessKey(pub Vec<u8>);

impl BusinessKey {
    /// Construct from anything that can be turned into a `Vec<u8>`.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Borrow the underlying bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A read snapshot on the system-time axis. The delta tier uses this when
/// resolving range scans: per business key, return the version whose
/// `[sys_from, sys_to)` interval contains the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Snapshot(pub SystemTimeMicros);

/// One version of one logical row, as seen by the delta tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub business_key: BusinessKey,
    pub sys_from: SystemTimeMicros,
    pub sys_to: SystemTimeMicros,
    pub payload: Vec<u8>,
}

impl Version {
    /// Number of bytes this version contributes to the delta's in-memory
    /// accounting — i.e. the size of its encoded representation. We charge the
    /// encoded size (not just `business_key.len() + payload.len()`) so that
    /// the same threshold value behaves consistently whether the bytes are in
    /// memory or just-written to a spill file.
    #[must_use]
    pub fn encoded_size(&self) -> usize {
        HEADER_LEN + self.business_key.0.len() + self.payload.len()
    }

    /// Encode into `out`, appending bytes to it. Used by both the WAL
    /// committer and the spill writer so the two paths share one wire format.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let business_len = u32::try_from(self.business_key.0.len())
            .expect("business_key fits in u32 — bounded by MAX_FRAME_LEN");
        let payload_len = u32::try_from(self.payload.len())
            .expect("payload fits in u32 — bounded by MAX_FRAME_LEN");
        out.reserve(self.encoded_size());
        out.extend_from_slice(&business_len.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&self.sys_from.0.to_le_bytes());
        out.extend_from_slice(&self.sys_to.0.to_le_bytes());
        out.extend_from_slice(&self.business_key.0);
        out.extend_from_slice(&self.payload);
    }

    /// Convenience: encode to a fresh `Vec<u8>`.
    #[must_use]
    pub fn encoded(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.encoded_size());
        self.encode(&mut v);
        v
    }

    /// Decode from the head of `bytes`. Returns the parsed `Version` and the
    /// number of bytes consumed. Callers that read concatenated records (e.g.
    /// spill reload) drive a loop on the returned cursor.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaError::Corrupt`] when the frame's declared lengths do
    /// not match the bytes available, or exceed the per-frame ceiling. The
    /// WAL layer ([`crate::wal`]) catches in-flight corruption via CRC32C;
    /// this check defends the delta tier against a truncated or oversized
    /// spill file.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), DeltaError> {
        if bytes.len() < HEADER_LEN {
            return Err(DeltaError::Corrupt("short read on version header"));
        }
        let business_len = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("4-byte slice always converts"),
        ) as usize;
        let payload_len = u32::from_le_bytes(
            bytes[4..8]
                .try_into()
                .expect("4-byte slice always converts"),
        ) as usize;
        let sys_from = i64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .expect("8-byte slice always converts"),
        );
        let sys_to = i64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .expect("8-byte slice always converts"),
        );
        let total = HEADER_LEN
            .checked_add(business_len)
            .and_then(|v| v.checked_add(payload_len))
            .ok_or(DeltaError::Corrupt("frame length overflows usize"))?;
        if total > MAX_FRAME_LEN {
            return Err(DeltaError::Corrupt("frame length exceeds MAX_FRAME_LEN"));
        }
        if bytes.len() < total {
            return Err(DeltaError::Corrupt("frame body shorter than declared"));
        }
        let business_key = BusinessKey(bytes[HEADER_LEN..HEADER_LEN + business_len].to_vec());
        let payload = bytes[HEADER_LEN + business_len..total].to_vec();
        Ok((
            Self {
                business_key,
                sys_from: SystemTimeMicros(sys_from),
                sys_to: SystemTimeMicros(sys_to),
                payload,
            },
            total,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::time::SYSTEM_TIME_OPEN;

    fn v(key: &[u8], sys_from: i64, payload: &[u8]) -> Version {
        Version {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            sys_to: SYSTEM_TIME_OPEN,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let original = v(b"alex", 42, b"hello world");
        let bytes = original.encoded();
        let (parsed, consumed) = Version::decode(&bytes).expect("decode");
        assert_eq!(parsed, original);
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed.encoded_size(), bytes.len());
    }

    #[test]
    fn decode_streams_back_to_back_frames() {
        let a = v(b"a", 1, b"one");
        let b = v(b"bb", 2, b"two");
        let mut buf = Vec::new();
        a.encode(&mut buf);
        b.encode(&mut buf);

        let (parsed_a, n) = Version::decode(&buf).expect("decode a");
        assert_eq!(parsed_a, a);
        let (parsed_b, m) = Version::decode(&buf[n..]).expect("decode b");
        assert_eq!(parsed_b, b);
        assert_eq!(n + m, buf.len());
    }

    #[test]
    fn empty_key_and_payload_are_legal() {
        // A zero-length business_key and payload is a degenerate but valid
        // case — the encoding shouldn't special-case it.
        let original = v(b"", 0, b"");
        let bytes = original.encoded();
        assert_eq!(bytes.len(), HEADER_LEN);
        let (parsed, n) = Version::decode(&bytes).expect("decode");
        assert_eq!(parsed, original);
        assert_eq!(n, HEADER_LEN);
    }

    #[test]
    fn truncated_header_is_corruption() {
        let bytes = v(b"k", 7, b"x").encoded();
        let err = Version::decode(&bytes[..HEADER_LEN - 1]).unwrap_err();
        assert!(matches!(err, DeltaError::Corrupt(_)));
    }

    #[test]
    fn truncated_body_is_corruption() {
        let bytes = v(b"key", 7, b"value").encoded();
        let err = Version::decode(&bytes[..bytes.len() - 1]).unwrap_err();
        assert!(matches!(err, DeltaError::Corrupt(_)));
    }
}
