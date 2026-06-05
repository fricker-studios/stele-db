//! `Version` — the unit of work that flows through the delta tier.
//!
//! A version is the in-memory representation of one row in a logical row's
//! bitemporal version chain ([architecture §2](../../../../docs/02-architecture.md#2-the-bitemporal-record-model)):
//!
//! * `business_key` — the user/PK or hash key (opaque bytes).
//! * `sys_from` — system-time at which this version became current.
//! * `sys_to` — system-time at which it was superseded; `SYSTEM_TIME_OPEN`
//!   while the version is the current one.
//! * `provenance` — the [`Provenance`] triple (`txn_id`, `committed_at`,
//!   `principal`) captured at commit and stored inline on every version
//!   ([architecture §12 invariant 5](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//!   Unlike valid-time, provenance is **always present**, never opt-in, and is
//!   carried as first-class fields here (and as first-class columns in a sealed
//!   segment) rather than inside `payload` — so it survives WAL replay and
//!   compresses on its own column statistics ([STL-93]).
//! * `payload` — the column values, encoded by a layer above. The delta tier
//!   treats this as opaque bytes.
//!
//! Valid-time columns are *deliberately absent* at this layer: valid-time is a
//! per-table opt-in ([architecture §12 invariant 4](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants))
//! and the delta tier itself does not interpret them — they travel inside
//! `payload` as a fixed prefix ([`crate::validtime`], [STL-92]). Provenance is
//! the opposite: always-on and first-class, never in the payload.
//!
//! ## Encoding
//!
//! [`Version::encode`] / [`Version::decode`] share one binary frame, used for
//! both WAL records and spill files. Layout (little-endian, fixed header is
//! 44 B; the three variable-length fields follow in order):
//!
//! ```text
//! +-----------------+----------------+------------------+------------+----------+-----------+----------------+
//! | business_len:u32| payload_len:u32| principal_len:u32| sys_from:i64| sys_to:i64| txn_id:u64 | committed_at:i64|
//! +-----------------+----------------+------------------+------------+----------+-----------+----------------+
//! | business_key bytes … | payload bytes … | principal bytes … |
//! +----------------------+-----------------+-------------------+
//! ```
//!
//! No CRC here — the WAL frames its own records ([`crate::wal`]), and spill
//! files are non-durable by design (the delta is rebuilt from WAL on crash).

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;

use super::DeltaError;

/// Fixed header size in bytes for the [`Version`] binary encoding: three `u32`
/// lengths (12) + `sys_from`/`sys_to`/`committed_at` `i64` (24) + `txn_id` `u64`
/// (8).
pub(crate) const HEADER_LEN: usize = 44;

/// Per-frame ceiling for a delta-tier `Version` (16 MiB).
///
/// Guards against runaway allocations when decoding a corrupt frame **and**
/// against producing an unreadable frame at encode time. The WAL itself
/// enforces `MAX_PAYLOAD_LEN = 16 MiB`, so a delta-tier frame can never
/// legitimately exceed that.
pub const MAX_VERSION_FRAME_LEN: usize = 16 * 1024 * 1024;

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
    /// Inline provenance captured at commit — invariant 5. Always present.
    pub provenance: Provenance,
    pub payload: Vec<u8>,
}

impl Version {
    /// Number of bytes this version contributes to the delta's in-memory
    /// accounting — i.e. the size of its encoded representation. We charge the
    /// encoded size (not just the variable-length field lengths) so that the
    /// same threshold value behaves consistently whether the bytes are in
    /// memory or just-written to a spill file.
    #[must_use]
    pub fn encoded_size(&self) -> usize {
        HEADER_LEN
            + self.business_key.0.len()
            + self.payload.len()
            + self.provenance.principal.0.len()
    }

    /// Verify the version can be encoded — i.e. its component sizes fit in
    /// `u32` and the total stays under [`MAX_VERSION_FRAME_LEN`]. The same
    /// preflight runs at [`super::Delta::insert`], so callers writing the WAL
    /// path get the size error before bytes hit the log.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaError::TooLarge`] when any variable-length component
    /// overflows `u32` or the encoded frame would exceed
    /// [`MAX_VERSION_FRAME_LEN`].
    pub fn check_encodable(&self) -> Result<(), DeltaError> {
        if u32::try_from(self.business_key.0.len()).is_err()
            || u32::try_from(self.payload.len()).is_err()
            || u32::try_from(self.provenance.principal.0.len()).is_err()
        {
            return Err(DeltaError::TooLarge(self.encoded_size()));
        }
        let size = self.encoded_size();
        if size > MAX_VERSION_FRAME_LEN {
            return Err(DeltaError::TooLarge(size));
        }
        Ok(())
    }

    /// Encode into `out`, appending bytes to it. Used by both the WAL
    /// committer and the spill writer so the two paths share one wire format.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaError::TooLarge`] when the version is larger than
    /// [`MAX_VERSION_FRAME_LEN`] — the same precondition [`Self::check_encodable`]
    /// reports.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), DeltaError> {
        self.check_encodable()?;
        // `try_from` here is redundant with `check_encodable` above, but
        // makes `encode` self-checking — any caller that bypassed the
        // preflight still gets the typed error instead of a panic.
        let business_len = u32::try_from(self.business_key.0.len())
            .map_err(|_| DeltaError::TooLarge(self.encoded_size()))?;
        let payload_len = u32::try_from(self.payload.len())
            .map_err(|_| DeltaError::TooLarge(self.encoded_size()))?;
        let principal_len = u32::try_from(self.provenance.principal.0.len())
            .map_err(|_| DeltaError::TooLarge(self.encoded_size()))?;
        out.reserve(self.encoded_size());
        out.extend_from_slice(&business_len.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&principal_len.to_le_bytes());
        out.extend_from_slice(&self.sys_from.0.to_le_bytes());
        out.extend_from_slice(&self.sys_to.0.to_le_bytes());
        out.extend_from_slice(&self.provenance.txn_id.0.to_le_bytes());
        out.extend_from_slice(&self.provenance.committed_at.0.to_le_bytes());
        out.extend_from_slice(&self.business_key.0);
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&self.provenance.principal.0);
        Ok(())
    }

    /// Convenience: encode to a fresh `Vec<u8>`.
    ///
    /// # Errors
    ///
    /// Forwards [`DeltaError::TooLarge`] from [`Self::encode`].
    pub fn encoded(&self) -> Result<Vec<u8>, DeltaError> {
        let mut v = Vec::with_capacity(self.encoded_size());
        self.encode(&mut v)?;
        Ok(v)
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
        let principal_len = u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .expect("4-byte slice always converts"),
        ) as usize;
        let sys_from = i64::from_le_bytes(
            bytes[12..20]
                .try_into()
                .expect("8-byte slice always converts"),
        );
        let sys_to = i64::from_le_bytes(
            bytes[20..28]
                .try_into()
                .expect("8-byte slice always converts"),
        );
        let txn_id = u64::from_le_bytes(
            bytes[28..36]
                .try_into()
                .expect("8-byte slice always converts"),
        );
        let committed_at = i64::from_le_bytes(
            bytes[36..44]
                .try_into()
                .expect("8-byte slice always converts"),
        );
        let total = HEADER_LEN
            .checked_add(business_len)
            .and_then(|v| v.checked_add(payload_len))
            .and_then(|v| v.checked_add(principal_len))
            .ok_or(DeltaError::Corrupt("frame length overflows usize"))?;
        if total > MAX_VERSION_FRAME_LEN {
            return Err(DeltaError::Corrupt(
                "frame length exceeds MAX_VERSION_FRAME_LEN",
            ));
        }
        if bytes.len() < total {
            return Err(DeltaError::Corrupt("frame body shorter than declared"));
        }
        // Body fields follow the fixed header in declaration order:
        // business_key, then payload, then principal.
        let bk_end = HEADER_LEN + business_len;
        let payload_end = bk_end + payload_len;
        let business_key = BusinessKey(bytes[HEADER_LEN..bk_end].to_vec());
        let payload = bytes[bk_end..payload_end].to_vec();
        let principal = Principal(bytes[payload_end..total].to_vec());
        Ok((
            Self {
                business_key,
                sys_from: SystemTimeMicros(sys_from),
                sys_to: SystemTimeMicros(sys_to),
                provenance: Provenance {
                    txn_id: TxnId(txn_id),
                    committed_at: SystemTimeMicros(committed_at),
                    principal,
                },
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
            // Provenance varies with sys_from so the round-trip exercises real
            // values in every field, not a constant the codec could mishandle.
            provenance: Provenance {
                txn_id: TxnId(u64::try_from(sys_from).unwrap_or(0)),
                committed_at: SystemTimeMicros(sys_from),
                principal: Principal::new(b"tester".to_vec()),
            },
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let original = v(b"alex", 42, b"hello world");
        let bytes = original.encoded().expect("encode");
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
        a.encode(&mut buf).expect("encode a");
        b.encode(&mut buf).expect("encode b");

        let (parsed_a, n) = Version::decode(&buf).expect("decode a");
        assert_eq!(parsed_a, a);
        let (parsed_b, m) = Version::decode(&buf[n..]).expect("decode b");
        assert_eq!(parsed_b, b);
        assert_eq!(n + m, buf.len());
    }

    #[test]
    fn empty_key_and_payload_are_legal() {
        // A zero-length business_key, payload, *and* principal is a degenerate
        // but valid case — the encoding shouldn't special-case it. All three
        // variable-length fields empty means the frame is exactly the fixed
        // header.
        let mut original = v(b"", 0, b"");
        original.provenance.principal = Principal::new(Vec::new());
        let bytes = original.encoded().expect("encode");
        assert_eq!(bytes.len(), HEADER_LEN);
        let (parsed, n) = Version::decode(&bytes).expect("decode");
        assert_eq!(parsed, original);
        assert_eq!(n, HEADER_LEN);
    }

    #[test]
    fn truncated_header_is_corruption() {
        let bytes = v(b"k", 7, b"x").encoded().expect("encode");
        let err = Version::decode(&bytes[..HEADER_LEN - 1]).unwrap_err();
        assert!(matches!(err, DeltaError::Corrupt(_)));
    }

    #[test]
    fn truncated_body_is_corruption() {
        let bytes = v(b"key", 7, b"value").encoded().expect("encode");
        let err = Version::decode(&bytes[..bytes.len() - 1]).unwrap_err();
        assert!(matches!(err, DeltaError::Corrupt(_)));
    }

    /// A frame that exceeds `MAX_VERSION_FRAME_LEN` at encode time must
    /// surface `DeltaError::TooLarge` — not a panic on the `u32::try_from`
    /// cast.
    #[test]
    fn oversized_frame_is_too_large_not_panic() {
        // 16 MiB + 1 of payload pushes the frame past MAX_VERSION_FRAME_LEN.
        let big = Version {
            business_key: BusinessKey::new(b"k".to_vec()),
            sys_from: SystemTimeMicros(0),
            sys_to: SYSTEM_TIME_OPEN,
            provenance: Provenance {
                txn_id: TxnId(1),
                committed_at: SystemTimeMicros(0),
                principal: Principal::new(b"tester".to_vec()),
            },
            payload: vec![0u8; MAX_VERSION_FRAME_LEN + 1],
        };
        let err = big.check_encodable().unwrap_err();
        assert!(matches!(err, DeltaError::TooLarge(_)));
        let err = big.encode(&mut Vec::new()).unwrap_err();
        assert!(matches!(err, DeltaError::TooLarge(_)));
    }
}
