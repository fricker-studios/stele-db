//! The **row payload codec**: pack a row's value columns into the single stored
//! payload, and unpack them back ([STL-151]).
//!
//! Stele storage holds each row as a business key plus **one** opaque payload
//! blob ([`ColumnId::Payload`](../../stele-storage/src/segment/format.rs)) — there
//! is no per-column physical storage at v0.2. A table wider than `(key, value)`
//! therefore needs its non-key columns *packed* into that one blob on write and
//! *sliced* back out on read. That mapping is this module, and only this module:
//! it is purely about byte framing, one level above
//! [`ScalarValue::encode`](crate::types::ScalarValue::encode) (which turns a
//! single cell into bytes) and one level below the catalog (which says how many
//! value columns a row has, and of what type).
//!
//! ## The framing
//!
//! A row's *value cells* are its columns after the business key — `n` of them for
//! an `(n+1)`-column table. Each cell is already a `ScalarValue` encoding, or
//! `None` for a SQL `NULL` ([STL-154]). [`encode_payload`] maps the cell list to
//! the stored payload:
//!
//! * **0 value columns** (a key-only table) → `None`: there is nothing to store.
//! * **1 value column** → the cell **verbatim** (`Some(bytes)` / `None`). This is
//!   byte-for-byte the v0.1 single-payload shape, so existing data and the
//!   lower-level typed write path keep working unchanged — the common
//!   `(key, value)` table pays no framing overhead and round-trips exactly.
//! * **2+ value columns** → a self-delimiting **frame**: for each cell, a presence
//!   byte (`0` = `NULL`, `1` = present) followed, when present, by a little-endian
//!   `u64` length and the cell's raw bytes. The frame is always `Some(_)`.
//!
//! [`decode_payload`] is the exact inverse for the same value-column **count** —
//! which the caller takes from the catalog schema, never from the bytes (the
//! frame carries lengths, not types, exactly as
//! [`ScalarValue`](crate::types::ScalarValue) encodings are type-directed on the
//! way back in).
//!
//! [STL-151]: https://allegromusic.atlassian.net/browse/STL-151
//! [STL-154]: https://allegromusic.atlassian.net/browse/STL-154

/// Why slicing a framed payload back into cells failed.
///
/// Every variant means the stored bytes do not match the value-column count the
/// caller supplied — i.e. the payload is corrupt or was written under a different
/// schema, not merely unexpected. A well-formed frame this codec produced for
/// `count` columns always decodes back without error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowCodecError {
    /// The frame ended in the middle of a cell — a presence byte or a length /
    /// value run was cut short.
    #[error("framed payload truncated: needed {needed} more byte(s) for {what}")]
    Truncated {
        /// What the decoder was reading when the bytes ran out.
        what: &'static str,
        /// How many more bytes that read needed.
        needed: usize,
    },
    /// The frame held more bytes than the value-column count accounts for — a
    /// count/payload disagreement (e.g. decoding under the wrong schema width).
    #[error("framed payload has {extra} trailing byte(s) after {count} cell(s)")]
    TrailingBytes {
        /// The value-column count the decode was driven by.
        count: usize,
        /// How many bytes were left over.
        extra: usize,
    },
    /// A cell's recorded length does not fit this platform's `usize` — only
    /// reachable on a 32-bit target reading a >4 GiB cell length, which is itself
    /// a corruption signal.
    #[error("framed payload cell length {len} does not fit usize")]
    LengthOverflow {
        /// The out-of-range length read from the frame.
        len: u64,
    },
    /// A cell's presence byte was neither `0` (NULL) nor `1` (present) — a
    /// corrupt frame. Caught rather than treated as "present", which would read
    /// arbitrary following bytes as a length.
    #[error("framed payload has an invalid cell presence tag {tag} (expected 0 or 1)")]
    InvalidTag {
        /// The out-of-range presence byte read from the frame.
        tag: u8,
    },
}

/// Pack a row's value cells into the stored payload form. See the
/// [module docs](self) for the framing; [`decode_payload`] is the inverse.
///
/// `cells` are the row's columns *after* the business key, each already a
/// [`ScalarValue`](crate::types::ScalarValue) encoding (`Some`) or a SQL `NULL`
/// (`None`), in column order.
#[must_use]
pub fn encode_payload(cells: &[Option<Vec<u8>>]) -> Option<Vec<u8>> {
    match cells {
        // Key-only table: nothing to store.
        [] => None,
        // Single value column: store the cell verbatim — the v0.1 shape, so no
        // framing overhead and exact backward compatibility.
        [only] => only.clone(),
        // Two or more: a self-delimiting frame, always present.
        cells => {
            let mut out = Vec::new();
            for cell in cells {
                match cell {
                    None => out.push(0),
                    Some(bytes) => {
                        out.push(1);
                        out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                        out.extend_from_slice(bytes);
                    }
                }
            }
            Some(out)
        }
    }
}

/// Slice a stored payload back into `count` value cells — the inverse of
/// [`encode_payload`] for the same value-column count.
///
/// `count` is the number of value columns (the table's column count minus the
/// business key), taken from the catalog schema. The returned vector always has
/// exactly `count` entries, each `Some(bytes)` for a present cell or `None` for a
/// SQL `NULL`.
///
/// # Errors
///
/// [`RowCodecError`] if a framed payload (`count >= 2`) is truncated, carries
/// trailing bytes, or records a cell length that does not fit `usize` — all of
/// which mean the bytes do not match `count`.
pub fn decode_payload(
    count: usize,
    payload: Option<&[u8]>,
) -> Result<Vec<Option<Vec<u8>>>, RowCodecError> {
    match count {
        // Key-only table: no cells regardless of what (if anything) is stored.
        0 => Ok(Vec::new()),
        // Single value column: the whole payload is that one cell, verbatim.
        1 => Ok(vec![payload.map(<[u8]>::to_vec)]),
        // Two or more: parse the frame. A missing payload (never produced by the
        // 2+ path) is read defensively as an all-`NULL` row rather than an error.
        count => {
            let Some(mut rest) = payload else {
                return Ok(vec![None; count]);
            };
            let mut cells = Vec::with_capacity(count);
            for _ in 0..count {
                let (&tag, after_tag) = rest.split_first().ok_or(RowCodecError::Truncated {
                    what: "a cell presence byte",
                    needed: 1,
                })?;
                match tag {
                    0 => {
                        cells.push(None);
                        rest = after_tag;
                    }
                    1 => {
                        let (len_bytes, after_len) = take(after_tag, 8, "a cell length")?;
                        let len = u64::from_le_bytes(len_bytes.try_into().expect("took 8 bytes"));
                        let len = usize::try_from(len)
                            .map_err(|_| RowCodecError::LengthOverflow { len })?;
                        let (value, after_value) = take(after_len, len, "a cell value")?;
                        cells.push(Some(value.to_vec()));
                        rest = after_value;
                    }
                    tag => return Err(RowCodecError::InvalidTag { tag }),
                }
            }
            if !rest.is_empty() {
                return Err(RowCodecError::TrailingBytes {
                    count,
                    extra: rest.len(),
                });
            }
            Ok(cells)
        }
    }
}

/// Split `n` bytes off the front of `buf`, or report exactly how many were
/// missing if it is too short.
const fn take<'a>(
    buf: &'a [u8],
    n: usize,
    what: &'static str,
) -> Result<(&'a [u8], &'a [u8]), RowCodecError> {
    if buf.len() < n {
        return Err(RowCodecError::Truncated {
            what,
            needed: n - buf.len(),
        });
    }
    Ok(buf.split_at(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode then decode `cells` for their own column count and assert the row
    /// comes back identical — the codec's round-trip contract.
    fn round_trip(cells: &[Option<Vec<u8>>]) {
        let payload = encode_payload(cells);
        let decoded = decode_payload(cells.len(), payload.as_deref()).expect("decode");
        assert_eq!(decoded, cells, "round-trip changed the row");
    }

    #[test]
    fn key_only_row_has_no_payload_and_no_cells() {
        assert_eq!(encode_payload(&[]), None);
        assert_eq!(decode_payload(0, None), Ok(Vec::new()));
        // Even a (defensively) present payload yields no cells for a 0-column row.
        assert_eq!(decode_payload(0, Some(b"junk")), Ok(Vec::new()));
    }

    #[test]
    fn single_value_column_is_stored_verbatim() {
        // The v0.1 shape: the payload IS the one cell, byte-for-byte, with no
        // framing — so pre-STL-151 data and the typed write path are unchanged.
        assert_eq!(
            encode_payload(&[Some(b"100".to_vec())]),
            Some(b"100".to_vec())
        );
        assert_eq!(
            decode_payload(1, Some(b"100")),
            Ok(vec![Some(b"100".to_vec())])
        );
        // A NULL single value is a missing payload, distinct from an empty one.
        assert_eq!(encode_payload(&[None]), None);
        assert_eq!(decode_payload(1, None), Ok(vec![None]));
        assert_eq!(encode_payload(&[Some(Vec::new())]), Some(Vec::new()));
        assert_eq!(decode_payload(1, Some(b"")), Ok(vec![Some(Vec::new())]));
    }

    #[test]
    fn multi_column_rows_round_trip_with_nulls_and_empties() {
        round_trip(&[Some(b"a".to_vec()), Some(b"bb".to_vec())]);
        round_trip(&[None, Some(b"x".to_vec())]);
        round_trip(&[Some(b"x".to_vec()), None]);
        round_trip(&[None, None, None]);
        round_trip(&[Some(Vec::new()), Some(b"nonempty".to_vec()), None]);
        round_trip(&[
            Some(vec![0, 1, 2, 255]),
            Some(b"unicode \x00 bytes".to_vec()),
            None,
            Some(b"last".to_vec()),
        ]);
    }

    #[test]
    fn a_multi_column_frame_is_always_present_even_when_all_null() {
        // Distinct from the single-column NULL (which is a missing payload): a
        // 2+-column row always stores a frame, so the column count is recoverable.
        let payload = encode_payload(&[None, None]);
        assert!(payload.is_some(), "a 2-column row always frames");
        assert_eq!(decode_payload(2, payload.as_deref()), Ok(vec![None, None]));
    }

    #[test]
    fn a_truncated_frame_is_rejected() {
        // A present cell claims 4 bytes of value but only 2 follow.
        let mut frame = vec![1u8];
        frame.extend_from_slice(&4u64.to_le_bytes());
        frame.extend_from_slice(b"ab");
        assert!(matches!(
            decode_payload(2, Some(&frame)),
            Err(RowCodecError::Truncated { .. })
        ));
    }

    #[test]
    fn an_invalid_presence_tag_is_rejected() {
        // A tag other than 0/1 is corruption — not silently read as "present".
        let frame = vec![2u8, 0, 0];
        assert_eq!(
            decode_payload(2, Some(&frame)),
            Err(RowCodecError::InvalidTag { tag: 2 })
        );
    }

    #[test]
    fn trailing_bytes_after_the_last_cell_are_rejected() {
        // A well-formed two-cell frame with one extra byte glued on.
        let mut frame = encode_payload(&[Some(b"a".to_vec()), Some(b"b".to_vec())]).expect("frame");
        frame.push(0xFF);
        assert_eq!(
            decode_payload(2, Some(&frame)),
            Err(RowCodecError::TrailingBytes { count: 2, extra: 1 })
        );
    }

    #[test]
    fn decoding_under_the_wrong_count_is_caught() {
        // A three-cell frame decoded as two cells leaves the third cell's bytes
        // trailing; decoded as four runs the frame out mid-read.
        let frame = encode_payload(&[
            Some(b"a".to_vec()),
            Some(b"b".to_vec()),
            Some(b"c".to_vec()),
        ])
        .expect("frame");
        assert!(matches!(
            decode_payload(2, Some(&frame)),
            Err(RowCodecError::TrailingBytes { .. })
        ));
        assert!(matches!(
            decode_payload(4, Some(&frame)),
            Err(RowCodecError::Truncated { .. })
        ));
    }
}
