//! `Version` — the unit of work that flows through the delta tier.
//!
//! A version is the in-memory representation of one row in a logical row's
//! bitemporal version chain ([architecture §2](../../../../docs/02-architecture.md#2-the-bitemporal-record-model)):
//!
//! * `business_key` — the user/PK or hash key (opaque bytes).
//! * `sys_from` — system-time at which this version became current.
//! * `seq` — the per-commit monotonic sequence number ([ADR-0024]). It gives a
//!   total order to writes that share the same µs `sys_from`, so same-tick
//!   commits are deterministically ordered without mutating their timestamp. A
//!   `u64` assigned by the transaction manager at commit ([STL-99]), carried
//!   inline like provenance; v0.1 stamps it but the per-key chain does not yet
//!   *order* on `(sys_from, seq)` — that is the follow-up ([STL-141] Part B).
//! * `provenance` — the [`Provenance`] triple (`txn_id`, `committed_at`,
//!   `principal`) captured at commit and stored inline on every version
//!   ([architecture §12 invariant 5](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//!   Unlike valid-time, provenance is **always present**, never opt-in, and is
//!   carried as first-class fields here (and as first-class columns in a sealed
//!   segment) rather than inside `payload` — so it survives WAL replay and
//!   compresses on its own column statistics ([STL-93]).
//! * `payload` — the column values, encoded by a layer above, or `None` for a
//!   SQL `NULL` cell ([STL-154]). The delta tier treats a present payload as
//!   opaque bytes and a `None` as a distinct, value-less cell — the two are kept
//!   apart on the durable record so a `NULL` survives WAL replay rather than
//!   collapsing to empty bytes.
//!
//! ## `sys_to` / `closed_by` are a **resolution overlay**, never persisted
//!
//! A version's system-time **end** (`sys_to`) and the provenance of the
//! transaction that closed it (`closed_by`) are **not stored on the record**
//! ([ADR-0023](../../../../docs/adr/0023-append-only-record-model-validity-index.md)):
//! a committed version is append-only and never mutated, so closing its period
//! cannot rewrite it. The end is materialized once into the derived, rebuildable
//! [validity index](crate::validity) and **overlaid** onto the version at read
//! time ([`crate::merge`]).
//!
//! The [`Version::sys_to`] and [`Version::closed_by`] fields exist only as that
//! transient overlay: a version read raw from the WAL, a spill, or a sealed
//! segment carries [`SYSTEM_TIME_OPEN`] and `None` — the *unresolved* state — and
//! the read path stamps the real end from the index. They are therefore
//! **deliberately absent from the binary frame** ([`Version::encode`] /
//! [`Version::decode`]) and from [`Version::encoded_size`]: the past, once
//! recorded, never changes, and nothing on the durable record can be rewritten to
//! say otherwise. This is what makes the append-only / tamper-evidence claims
//! hold under scrutiny.
//!
//! Valid-time columns are *deliberately absent* at this layer too: valid-time is a
//! per-table opt-in ([architecture §12 invariant 4](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants))
//! and the delta tier itself does not interpret them — they travel inside
//! `payload` as a fixed prefix ([`crate::validtime`], [STL-92]). Provenance is
//! the opposite: always-on and first-class, never in the payload.
//!
//! ## Encoding
//!
//! [`Version::encode`] / [`Version::decode`] share one binary frame, used for
//! both WAL records and spill files. Layout (little-endian, fixed header is
//! [`HEADER_LEN`] B; the three variable-length fields follow in order):
//!
//! ```text
//! +------------------+----------------+------------------+
//! | business_len:u32 | payload_len:u32| principal_len:u32|
//! +------------------+----------------+------------------+
//! | sys_from:i64 | txn_id:u64 | committed_at:i64 | seq:u64 |
//! +--------------+------------+------------------+---------+
//! | business_key bytes … | payload bytes … | principal bytes … |
//! +----------------------+-----------------+-------------------+
//! ```
//!
//! A SQL `NULL` payload ([STL-154]) is encoded by setting `payload_len` to the
//! reserved sentinel [`PAYLOAD_NULL_SENTINEL`] (`u32::MAX`) with **no** payload
//! bytes in the body. A real payload can never reach that length — the frame is
//! capped at [`MAX_VERSION_FRAME_LEN`] (16 MiB) — so the sentinel is
//! unambiguous and needs no separate null flag.
//!
//! There is no `sys_to` or close-provenance group in the frame — those are the
//! validity-index overlay described above, not part of the durable record.
//!
//! No CRC here — the WAL frames its own records ([`crate::wal`]), and spill
//! files are non-durable by design (the delta is rebuilt from WAL on crash).

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};

use super::DeltaError;

/// Fixed header size in bytes for the [`Version`] binary encoding: three `u32`
/// lengths (12) + `sys_from`/`committed_at` `i64` (16) + `txn_id` `u64` (8) +
/// `seq` `u64` (8).
pub(crate) const HEADER_LEN: usize = 44;

/// Per-frame ceiling for a delta-tier `Version` (16 MiB).
///
/// Guards against runaway allocations when decoding a corrupt frame **and**
/// against producing an unreadable frame at encode time. The WAL itself
/// enforces `MAX_PAYLOAD_LEN = 16 MiB`, so a delta-tier frame can never
/// legitimately exceed that.
pub const MAX_VERSION_FRAME_LEN: usize = 16 * 1024 * 1024;

/// The `payload_len` value reserved to mean "this version's payload is SQL
/// `NULL`" ([STL-154]). A present payload is bounded by [`MAX_VERSION_FRAME_LEN`]
/// (16 MiB), so it can never legitimately reach `u32::MAX`; reusing that
/// otherwise-impossible length as a sentinel lets a `None` payload round-trip
/// through the frame with no separate null flag and no format-version bump (the
/// frame is unversioned, and the value was previously unreachable).
pub(crate) const PAYLOAD_NULL_SENTINEL: u32 = u32::MAX;

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
///
/// `sys_to` and `closed_by` are a **read-time resolution overlay**, not part of
/// the durable record — see the module-level docs. On a version read raw from
/// the WAL, a spill, or a sealed segment they are [`SYSTEM_TIME_OPEN`] / `None`
/// (unresolved); the read path stamps the real end from the
/// [validity index](crate::validity).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub business_key: BusinessKey,
    pub sys_from: SystemTimeMicros,
    /// Per-commit monotonic sequence number ([ADR-0024]) — the total-order
    /// tiebreak for writes sharing the same µs `sys_from`. Assigned by the
    /// transaction manager at commit ([STL-99]) and carried inline, **always
    /// present** like provenance. Persisted on both the durable record (the
    /// delta frame and the sealed segment), unlike the `sys_to` / `closed_by`
    /// overlay below.
    pub seq: u64,
    /// Resolution overlay: the system-time end of this version's period, or
    /// [`SYSTEM_TIME_OPEN`] when unresolved/open. **Never persisted** — sourced
    /// from the [validity index](crate::validity) at read time ([ADR-0023]).
    pub sys_to: SystemTimeMicros,
    /// Inline provenance captured at commit — invariant 5. Always present.
    pub provenance: Provenance,
    /// Resolution overlay: the provenance of the transaction that closed this
    /// version's period, or `None` when unresolved/open. **Never persisted** —
    /// sourced from the [validity index](crate::validity) at read time
    /// ([ADR-0023], [STL-118]).
    pub closed_by: Option<Provenance>,
    /// The column values encoded by a layer above, or `None` for a SQL `NULL`
    /// cell ([STL-154]). Persisted distinctly on the durable record (see the
    /// `PAYLOAD_NULL_SENTINEL` frame encoding) so a `NULL` is never confused
    /// with an empty payload.
    pub payload: Option<Vec<u8>>,
}

impl Version {
    /// Build an **open** (unresolved) version: <code>sys_to = [SYSTEM_TIME_OPEN]</code>,
    /// `closed_by = None`. The shape every raw decode / segment read produces;
    /// the validity-index overlay supplies the end at read time.
    #[must_use]
    pub const fn open(
        business_key: BusinessKey,
        sys_from: SystemTimeMicros,
        seq: u64,
        provenance: Provenance,
        payload: Option<Vec<u8>>,
    ) -> Self {
        Self {
            business_key,
            sys_from,
            seq,
            sys_to: SYSTEM_TIME_OPEN,
            provenance,
            closed_by: None,
            payload,
        }
    }

    /// Number of bytes this version contributes to the delta's in-memory
    /// accounting — i.e. the size of its encoded representation. The overlay
    /// (`sys_to` / `closed_by`) is **not** counted: it is not part of the frame.
    #[must_use]
    pub fn encoded_size(&self) -> usize {
        HEADER_LEN
            + self.business_key.0.len()
            + self.payload.as_ref().map_or(0, Vec::len)
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
            || self
                .payload
                .as_ref()
                .is_some_and(|p| u32::try_from(p.len()).is_err())
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
    /// The `sys_to` / `closed_by` overlay is intentionally omitted — it is the
    /// validity-index's to materialize, not the record's to carry ([ADR-0023]).
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
        // A `None` payload writes the reserved sentinel and no body bytes; a
        // present payload writes its real length (bounded well below the
        // sentinel by `check_encodable`).
        let payload_len = match &self.payload {
            None => PAYLOAD_NULL_SENTINEL,
            Some(p) => {
                u32::try_from(p.len()).map_err(|_| DeltaError::TooLarge(self.encoded_size()))?
            }
        };
        let principal_len = u32::try_from(self.provenance.principal.0.len())
            .map_err(|_| DeltaError::TooLarge(self.encoded_size()))?;
        out.reserve(self.encoded_size());
        out.extend_from_slice(&business_len.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&principal_len.to_le_bytes());
        out.extend_from_slice(&self.sys_from.0.to_le_bytes());
        out.extend_from_slice(&self.provenance.txn_id.0.to_le_bytes());
        out.extend_from_slice(&self.provenance.committed_at.0.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.business_key.0);
        if let Some(p) = &self.payload {
            out.extend_from_slice(p);
        }
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
    /// The decoded version is **open** (<code>sys_to = [SYSTEM_TIME_OPEN]</code>,
    /// `closed_by = None`): the frame carries no end, so resolution is left to
    /// the validity-index overlay ([ADR-0023]).
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
        // Fixed-offset readers over the header — `bytes.len() >= HEADER_LEN`
        // is checked above, so every fixed field is in bounds.
        let rd_u32 = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().expect("4 bytes"));
        let rd_i64 = |o: usize| i64::from_le_bytes(bytes[o..o + 8].try_into().expect("8 bytes"));
        let rd_u64 = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().expect("8 bytes"));
        let business_len = rd_u32(0) as usize;
        // The reserved sentinel marks a SQL `NULL` payload: no body bytes, and
        // the field contributes nothing to the frame length.
        let payload_raw = rd_u32(4);
        let payload_is_null = payload_raw == PAYLOAD_NULL_SENTINEL;
        let payload_len = if payload_is_null {
            0
        } else {
            payload_raw as usize
        };
        let principal_len = rd_u32(8) as usize;
        let sys_from = rd_i64(12);
        let txn_id = rd_u64(20);
        let committed_at = rd_i64(28);
        let seq = rd_u64(36);
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
        // business_key, payload, then principal.
        let bk_end = HEADER_LEN + business_len;
        let payload_end = bk_end + payload_len;
        let principal_end = payload_end + principal_len;
        let business_key = BusinessKey(bytes[HEADER_LEN..bk_end].to_vec());
        let payload = if payload_is_null {
            None
        } else {
            Some(bytes[bk_end..payload_end].to_vec())
        };
        let principal = Principal(bytes[payload_end..principal_end].to_vec());
        Ok((
            Self::open(
                business_key,
                SystemTimeMicros(sys_from),
                seq,
                Provenance {
                    txn_id: TxnId(txn_id),
                    committed_at: SystemTimeMicros(committed_at),
                    principal,
                },
                payload,
            ),
            total,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(key: &[u8], sys_from: i64, payload: &[u8]) -> Version {
        // `seq` varies with `sys_from` so a non-zero, non-constant value rides
        // through every round-trip — a codec that dropped or transposed the
        // field would be caught, not masked by a zero default.
        Version::open(
            BusinessKey::new(key.to_vec()),
            SystemTimeMicros(sys_from),
            u64::try_from(sys_from)
                .unwrap_or(0)
                .wrapping_mul(7)
                .wrapping_add(1),
            // Provenance varies with sys_from so the round-trip exercises real
            // values in every field, not a constant the codec could mishandle.
            Provenance {
                txn_id: TxnId(u64::try_from(sys_from).unwrap_or(0)),
                committed_at: SystemTimeMicros(sys_from),
                principal: Principal::new(b"tester".to_vec()),
            },
            Some(payload.to_vec()),
        )
    }

    #[test]
    fn null_payload_round_trips_and_is_distinct_from_empty() {
        // A `None` (SQL NULL) payload survives the frame and decodes back to
        // `None` — never collapsing into the empty-bytes `Some(vec![])`.
        let mut null = v(b"k", 9, b"");
        null.payload = None;
        let bytes = null.encoded().expect("encode");
        let (parsed, consumed) = Version::decode(&bytes).expect("decode");
        assert_eq!(parsed, null);
        assert_eq!(parsed.payload, None, "NULL stays NULL");
        assert_eq!(consumed, bytes.len());

        // The same row with an *empty* payload encodes to different bytes and
        // decodes to `Some(vec![])`, proving the two are not conflated.
        let empty = v(b"k", 9, b"");
        assert_eq!(empty.payload, Some(Vec::new()));
        assert_ne!(
            empty.encoded().expect("encode"),
            bytes,
            "empty payload and NULL payload must not share a frame"
        );
    }

    #[test]
    fn encode_decode_round_trip() {
        let original = v(b"alex", 42, b"hello world");
        assert_ne!(original.seq, 0, "fixture carries a non-zero seq");
        let bytes = original.encoded().expect("encode");
        let (parsed, consumed) = Version::decode(&bytes).expect("decode");
        assert_eq!(parsed, original);
        assert_eq!(parsed.seq, original.seq, "seq survives the delta frame");
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed.encoded_size(), bytes.len());
    }

    #[test]
    fn overlay_is_not_persisted_and_decodes_open() {
        // A version whose overlay was resolved (closed) encodes the *same* bytes
        // as its open form — the end lives in the validity index, not the frame —
        // and always decodes back to the open/unresolved state.
        let open = v(b"acct", 7, b"balance=0");
        // Same birth state as `open`, but with the (transient) overlay set.
        let mut closed = v(b"acct", 7, b"balance=0");
        closed.sys_to = SystemTimeMicros(99);
        closed.closed_by = Some(Provenance::new(
            TxnId(4242),
            SystemTimeMicros(99),
            Principal::new(b"deleter".to_vec()),
        ));
        assert_eq!(
            closed.encoded().expect("encode"),
            open.encoded().expect("encode"),
            "the overlay must not change the frame bytes",
        );
        let (parsed, _) = Version::decode(&closed.encoded().expect("encode")).expect("decode");
        assert_eq!(parsed, open, "decode yields the open/unresolved version");
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
        let big = Version::open(
            BusinessKey::new(b"k".to_vec()),
            SystemTimeMicros(0),
            1,
            Provenance {
                txn_id: TxnId(1),
                committed_at: SystemTimeMicros(0),
                principal: Principal::new(b"tester".to_vec()),
            },
            Some(vec![0u8; MAX_VERSION_FRAME_LEN + 1]),
        );
        let err = big.check_encodable().unwrap_err();
        assert!(matches!(err, DeltaError::TooLarge(_)));
        let err = big.encode(&mut Vec::new()).unwrap_err();
        assert!(matches!(err, DeltaError::TooLarge(_)));
    }
}
