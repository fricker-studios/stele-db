//! Zero-copy join output gather ([STL-224]).
//!
//! The hash join assembles its output by naming each side's matched rows by index
//! over the side's shared column buffers — a [`GatheredColumns`] view — rather than
//! deep-copying each surviving cell per matched row pair. This is the join
//! counterpart of the STL-214 [`Filter`](stele_exec::Filter) selection: keep the
//! buffer, carry the indices, copy nothing until the wire result is materialized.
//!
//! Proven here by cell-address aliasing, the same way STL-214's
//! `filter_selects_surviving_rows_without_copying_payload_cells` proves the filter
//! path — a surviving output cell is the *same heap allocation* as the source cell,
//! across the two degrees of freedom a join adds over a filter: a repeated index
//! (a one-to-many match emits a left row more than once) and an absent index (a
//! `LEFT` join's `NULL`-extended right side).
//!
//! [STL-224]: https://allegromusic.atlassian.net/browse/STL-224
//! [STL-214]: https://allegromusic.atlassian.net/browse/STL-214

use stele_common::types::ScalarValue;
use stele_exec::{Column, GatheredColumns};

/// A present (non-NULL) value cell holding `value`'s canonical encoding — the bytes
/// a value column stores. Always `Some`; a bare `None` is the SQL NULL at call sites.
#[allow(clippy::unnecessary_wraps)]
fn cell(value: &ScalarValue) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    value.encode(&mut buf);
    Some(buf)
}

#[test]
fn join_gathers_surviving_rows_without_copying_payload_cells() {
    // One side's payload column over three physical rows. Record each surviving
    // cell's byte-buffer address *before* the column is moved into the gather — a
    // gather that references (rather than re-allocates) hands these very addresses
    // back.
    let payloads = Column::Bytes(
        vec![
            Some(b"alpha".to_vec()),
            Some(b"beta".to_vec()),
            Some(b"gamma".to_vec()),
        ]
        .into(),
    );
    let Column::Bytes(src) = &payloads else {
        unreachable!("payload is bytes")
    };
    let row0_ptr = src[0].as_ref().map(Vec::as_ptr);
    let row2_ptr = src[2].as_ref().map(Vec::as_ptr);

    // Output rows draw physical rows [2, 0, None, 2]: a forward pick, a reorder, a
    // NULL-extended row (a LEFT join's unmatched right), and a *repeat* of row 2 (a
    // one-to-many match). None of these copy a cell.
    let gather = GatheredColumns::new(vec![payloads], vec![Some(2), Some(0), None, Some(2)]);

    assert_eq!(
        gather.rows(),
        4,
        "the selection length is the output height"
    );
    assert!(!gather.is_empty());

    // The shared column still holds every physical row — the selection narrows it,
    // and each surviving cell is the same heap allocation as the input: a deep copy
    // would have re-allocated its bytes at a different address.
    assert_eq!(
        gather.bytes(0, 0).map(<[u8]>::as_ptr),
        row2_ptr,
        "the gather copied the surviving payload bytes instead of selecting them",
    );
    assert_eq!(gather.bytes(0, 1).map(<[u8]>::as_ptr), row0_ptr);
    assert_eq!(
        gather.bytes(0, 3).map(<[u8]>::as_ptr),
        row2_ptr,
        "a repeated index (one-to-many) still aliases the one source allocation",
    );

    // A `None` index is a NULL cell — the LEFT join's NULL-extended right side.
    assert_eq!(gather.bytes(0, 2), None, "a None index is a NULL cell");

    // The values themselves are correct, NULLs included.
    assert_eq!(gather.bytes(0, 0), Some(&b"gamma"[..]));
    assert_eq!(gather.bytes(0, 1), Some(&b"alpha"[..]));
    assert_eq!(gather.bytes(0, 3), Some(&b"gamma"[..]));
}

#[test]
fn join_gather_distinguishes_a_stored_null_from_a_null_extension() {
    // A column whose physical row 1 is a stored SQL NULL (`None`), distinct from a
    // NULL produced by an absent (`None`) selection index. Both read back as NULL —
    // the gather must not confuse "this row's cell is NULL" with "no row selected".
    let payloads = Column::Bytes(vec![cell(&ScalarValue::Int4(7)), None].into());
    let gather = GatheredColumns::new(vec![payloads], vec![Some(1), None, Some(0)]);

    assert_eq!(
        gather.bytes(0, 0),
        None,
        "a stored NULL cell reads back NULL"
    );
    assert_eq!(gather.bytes(0, 1), None, "an absent index reads back NULL");
    assert_eq!(gather.bytes(0, 2), cell(&ScalarValue::Int4(7)).as_deref());
}
