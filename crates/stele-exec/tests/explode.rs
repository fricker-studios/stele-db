//! `ExplodePayload` operator unit tests (STL-206).
//!
//! The operator is the vectorized mirror of [`row_codec::decode_payload`]: it
//! slices the opaque `Payload` blob a scan emits into the table's value columns
//! as first-class typed columns, so a value-column `Filter` can run vectorized on
//! the live query path. These tests drive it with *synthetic* `[BusinessKey,
//! Payload]` batches — no storage needed — to pin the framing edge cases the
//! codec defines: a key-only table (no value columns), a single verbatim value,
//! a multi-column frame with NULLs, NULL propagation, and a corrupt frame.

use stele_common::row_codec::encode_payload;
use stele_exec::{Batch, Column, ExplodePayload, Operator, ScanError};
use stele_storage::segment::ColumnId;

/// A source operator that hands out pre-built batches in order — the explode
/// operator's upstream stand-in.
struct VecSource(std::vec::IntoIter<Batch>);

impl Operator for VecSource {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        Ok(self.0.next())
    }
}

/// A `[BusinessKey, Payload]` batch — the shape `run_select`'s scan emits.
fn scan_batch(keys: &[&[u8]], payloads: Vec<Option<Vec<u8>>>) -> Batch {
    assert_eq!(keys.len(), payloads.len(), "one payload per key");
    Batch {
        columns: vec![
            (
                ColumnId::BusinessKey,
                Column::Bytes(keys.iter().map(|k| Some(k.to_vec())).collect()),
            ),
            (ColumnId::Payload, Column::Bytes(payloads.into())),
        ],
        rows: keys.len(),
    }
}

fn source(batches: Vec<Batch>) -> VecSource {
    VecSource(batches.into_iter())
}

/// Drain an operator, returning every batch it emits.
fn drain(mut op: impl Operator) -> Vec<Batch> {
    let mut batches = Vec::new();
    while let Some(b) = op.next().expect("operator pull") {
        batches.push(b);
    }
    batches
}

/// The cells of the column at `position` across a sequence of batches, row-major.
fn column_cells(batches: &[Batch], position: usize) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    for b in batches {
        match &b.columns[position].1 {
            Column::Bytes(cells) => out.extend(cells.iter().cloned()),
            Column::I64(_) => panic!("exploded column {position} is i64, expected bytes"),
        }
    }
    out
}

/// A present cell — the readable counterpart to a bare `None` in the expected
/// `Vec<Option<Vec<u8>>>` literals below.
#[allow(clippy::unnecessary_wraps)]
fn some(bytes: &[u8]) -> Option<Vec<u8>> {
    Some(bytes.to_vec())
}

#[test]
fn a_single_value_column_is_the_payload_verbatim() {
    // value_count == 1: the payload *is* the one cell, byte-for-byte (the v0.1
    // shape). Output is [key, value], both bytes columns.
    let batch = scan_batch(&[b"k1", b"k2"], vec![some(b"v1"), some(b"v2")]);
    let out = drain(ExplodePayload::new(source(vec![batch]), 1));

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].columns.len(), 2, "key + one value column");
    assert_eq!(out[0].rows, 2);
    // Position 0 is the business key, passed through unchanged.
    assert_eq!(out[0].columns[0].0, ColumnId::BusinessKey);
    assert_eq!(column_cells(&out, 0), vec![some(b"k1"), some(b"k2")]);
    // Position 1 is the exploded value column — tagged Payload (the blob it came
    // from); position, not the id, distinguishes value columns.
    assert_eq!(out[0].columns[1].0, ColumnId::Payload);
    assert_eq!(column_cells(&out, 1), vec![some(b"v1"), some(b"v2")]);
}

#[test]
fn a_null_single_value_stays_null() {
    // A missing payload is a NULL single value (distinct from an empty one).
    let batch = scan_batch(&[b"k1", b"k2"], vec![None, some(b"")]);
    let out = drain(ExplodePayload::new(source(vec![batch]), 1));
    assert_eq!(column_cells(&out, 1), vec![None, some(b"")]);
}

#[test]
fn a_multi_column_frame_explodes_into_one_column_per_cell() {
    // value_count == 2: each payload is a self-delimiting frame; explode it into
    // two columns, transposing the per-row cells.
    let p0 = encode_payload(&[some(b"a"), some(b"bb")]);
    let p1 = encode_payload(&[None, some(b"x")]);
    let p2 = encode_payload(&[some(b""), None]);
    let batch = scan_batch(&[b"k1", b"k2", b"k3"], vec![p0, p1, p2]);

    let out = drain(ExplodePayload::new(source(vec![batch]), 2));
    assert_eq!(out[0].columns.len(), 3, "key + two value columns");
    assert_eq!(
        column_cells(&out, 0),
        vec![some(b"k1"), some(b"k2"), some(b"k3")]
    );
    assert_eq!(column_cells(&out, 1), vec![some(b"a"), None, some(b"")]);
    assert_eq!(column_cells(&out, 2), vec![some(b"bb"), some(b"x"), None]);
}

#[test]
fn a_key_only_table_drops_the_payload() {
    // value_count == 0: a key-only table stores no value cells, so the payload is
    // dropped and only the business key survives.
    let batch = scan_batch(&[b"k1", b"k2"], vec![some(b"junk"), None]);
    let out = drain(ExplodePayload::new(source(vec![batch]), 0));
    assert_eq!(out[0].columns.len(), 1, "key only");
    assert_eq!(out[0].columns[0].0, ColumnId::BusinessKey);
    assert_eq!(column_cells(&out, 0), vec![some(b"k1"), some(b"k2")]);
}

#[test]
fn a_corrupt_frame_surfaces_a_row_codec_error() {
    // A frame that claims a present cell (tag 1) but carries no length/value is
    // truncated — the codec error propagates as `ScanError::RowCodec`, never a
    // panic or a silently-wrong row.
    let batch = scan_batch(&[b"k1"], vec![Some(vec![1u8])]);
    let mut op = ExplodePayload::new(source(vec![batch]), 2);
    match op.next() {
        Err(ScanError::RowCodec(_)) => {}
        other => panic!("expected RowCodec error, got {other:?}"),
    }
}

#[test]
fn explode_preserves_batching_and_row_order() {
    // The operator explodes each upstream batch independently and in order, so a
    // chunked scan keeps its row order across the explode.
    let b1 = scan_batch(&[b"k1", b"k2"], vec![some(b"v1"), some(b"v2")]);
    let b2 = scan_batch(&[b"k3"], vec![some(b"v3")]);
    let out = drain(ExplodePayload::new(source(vec![b1, b2]), 1));
    assert_eq!(out.len(), 2, "one output batch per input batch");
    assert_eq!(out.iter().map(|b| b.rows).collect::<Vec<_>>(), [2, 1]);
    assert_eq!(
        column_cells(&out, 1),
        vec![some(b"v1"), some(b"v2"), some(b"v3")]
    );
}

#[test]
fn an_empty_stream_explodes_to_nothing() {
    let out = drain(ExplodePayload::new(source(vec![]), 2));
    assert!(out.is_empty());
}
