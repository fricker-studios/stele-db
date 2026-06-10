//! `Filter` operator tests (STL-170 `[C10]`).
//!
//! Two things are proven here:
//!
//! * **operator mechanics** — `Filter` keeps a batch's `TRUE` rows across a
//!   multi-batch stream, never surfaces a fully-filtered batch as an empty one
//!   (the [`Operator`] contract), and passes through the columns it does not
//!   reference; and
//! * **row-at-a-time equivalence** (the second DoD bullet) — for the STL-151
//!   `<column> = <literal>` cases, the vectorized `Filter` keeps exactly the
//!   rows the row-at-a-time path (`engine::run_select`) keeps. That path
//!   compares the cell's stored bytes to the literal's encoding and treats a
//!   NULL cell as never-equal; this test reproduces that rule and asserts the
//!   surviving row sets are identical, value columns and NULLs included.

use stele_common::types::{LogicalType, ScalarValue};
use stele_exec::{Batch, CmpOp, Column, Expr, ExprError, Filter, Operator, ScanError};
use stele_storage::segment::ColumnId;

// --- a queue-backed source operator ----------------------------------------

/// An [`Operator`] that hands out a fixed queue of batches — lets the `Filter`
/// tests drive multi-batch streams without standing up the storage tiers (the
/// storage-backed pipeline is covered by `tests/operator.rs`). Per the operator
/// contract it only ever yields the non-empty batches it was given.
struct VecSource {
    batches: std::vec::IntoIter<Batch>,
}

impl VecSource {
    fn new(batches: Vec<Batch>) -> Self {
        Self {
            batches: batches.into_iter(),
        }
    }
}

impl Operator for VecSource {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        Ok(self.batches.next())
    }
}

/// Drain an operator, concatenating every emitted batch's column `index` (bytes)
/// into one row-major vector — and assert no emitted batch is empty.
fn drain_bytes(mut op: impl Operator, index: usize) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    while let Some(batch) = op.next().expect("filter pull") {
        assert!(batch.rows > 0, "operators never emit an empty batch");
        let (_, column) = &batch.columns[index];
        match column {
            Column::Bytes(cells) => out.extend(cells.iter().cloned()),
            Column::I64(_) => panic!("column {index} is i64, expected bytes"),
        }
    }
    out
}

/// A present (non-NULL) value cell holding `value`'s encoding — the bytes the
/// row codec stores. Always `Some`: it is the cell *constructor* for a present
/// value, paired with a bare `None` for a SQL NULL at the call sites.
#[allow(clippy::unnecessary_wraps)]
fn cell(value: &ScalarValue) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    value.encode(&mut buf);
    Some(buf)
}

// --- operator mechanics ----------------------------------------------------

#[test]
fn filter_keeps_true_rows_and_skips_fully_filtered_batches() {
    // Three batches of a single text column; keep only "v3", which lives alone
    // in the middle batch — so the first and last batches filter to empty and
    // must be skipped rather than emitted as zero-row batches.
    let col = |vals: &[&str]| {
        Column::Bytes(
            vals.iter()
                .map(|s| cell(&ScalarValue::Text((*s).to_owned())))
                .collect(),
        )
    };
    let batch = |vals: &[&str]| Batch {
        columns: vec![(ColumnId::Payload, col(vals))],
        rows: vals.len(),
    };
    let source = VecSource::new(vec![
        batch(&["v1", "v2"]),
        batch(&["v3", "v4"]),
        batch(&["v5"]),
    ]);

    let predicate = Expr::col(0).compare(CmpOp::Eq, Expr::lit(ScalarValue::Text("v3".to_owned())));
    let filter = Filter::new(source, predicate, vec![LogicalType::Text]);

    assert_eq!(
        drain_bytes(filter, 0),
        vec![cell(&ScalarValue::Text("v3".to_owned()))],
    );
}

#[test]
fn filter_passes_through_unreferenced_columns() {
    // Two columns; the predicate only references column 0 (an int4 key), so the
    // opaque column 1 rides along untouched — its schema entry is never read.
    let keys = Column::Bytes(vec![
        cell(&ScalarValue::Int4(1)),
        cell(&ScalarValue::Int4(2)),
        cell(&ScalarValue::Int4(3)),
    ]);
    let payloads = Column::Bytes(vec![
        Some(b"one".to_vec()),
        Some(b"two".to_vec()),
        Some(b"three".to_vec()),
    ]);
    let batch = Batch {
        columns: vec![(ColumnId::BusinessKey, keys), (ColumnId::Payload, payloads)],
        rows: 3,
    };
    let predicate = Expr::col(0).compare(CmpOp::Eq, Expr::lit(ScalarValue::Int4(2)));
    // The unreferenced column 1 is given an out-of-scope type on purpose: a
    // correct Filter never decodes it, so this must not error.
    let mut filter = Filter::new(
        VecSource::new(vec![batch]),
        predicate,
        vec![LogicalType::Int4, LogicalType::Period],
    );
    let out = filter.next().expect("pull").expect("one batch");
    assert_eq!(out.rows, 1);
    // The surviving row carries both columns, aligned: key 2 and payload "two".
    assert_eq!(
        out.columns[0].1,
        Column::Bytes(vec![cell(&ScalarValue::Int4(2))])
    );
    assert_eq!(out.columns[1].1, Column::Bytes(vec![Some(b"two".to_vec())]));
}

#[test]
fn a_referenced_column_with_no_schema_type_is_a_distinct_error() {
    // The predicate references column 1, but the schema gives only one type —
    // a schema/type-vector gap, reported distinctly from a missing batch column.
    let batch = Batch {
        columns: vec![
            (
                ColumnId::BusinessKey,
                Column::Bytes(vec![cell(&ScalarValue::Int4(1))]),
            ),
            (ColumnId::Payload, Column::Bytes(vec![Some(b"x".to_vec())])),
        ],
        rows: 1,
    };
    let predicate = Expr::col(1).compare(CmpOp::Eq, Expr::lit(ScalarValue::Int4(0)));
    let mut filter = Filter::new(
        VecSource::new(vec![batch]),
        predicate,
        vec![LogicalType::Int4],
    );
    match filter.next() {
        Err(ScanError::Eval(ExprError::ColumnTypeMissing {
            index: 1,
            schema_len: 1,
        })) => {}
        other => panic!("expected ColumnTypeMissing, got {other:?}"),
    }
}

// --- row-at-a-time equivalence (the second DoD bullet) ---------------------

/// One test row: an int4 key plus a nullable int4 and a nullable text value
/// column — the `(business_key, value cells…)` shape `run_select` reconstructs.
struct Row {
    key: i32,
    balance: Option<i32>,
    name: Option<&'static str>,
}

impl Row {
    /// The row's cells in schema order, each encoded as the row codec stores it
    /// (`None` for a SQL NULL) — the bytes the row-at-a-time filter compares.
    fn cells(&self) -> [Option<Vec<u8>>; 3] {
        [
            cell(&ScalarValue::Int4(self.key)),
            self.balance.map(|v| cell(&ScalarValue::Int4(v)).unwrap()),
            self.name
                .map(|s| cell(&ScalarValue::Text(s.to_owned())).unwrap()),
        ]
    }
}

/// `engine::run_select`'s filter rule for `<column index> = <literal>`: keep a
/// row iff that column's stored bytes equal the literal's encoding; a NULL cell
/// (`None`) never equals a non-null literal. Returns the surviving rows' keys.
fn row_at_a_time(rows: &[Row], column_index: usize, literal: &ScalarValue) -> Vec<i32> {
    let want = cell(literal);
    rows.iter()
        .filter(|r| r.cells()[column_index] == want)
        .map(|r| r.key)
        .collect()
}

/// The same filter run through the vectorized `Filter` operator: build a batch
/// of byte columns, filter on `col == literal`, and read the surviving keys back.
fn vectorized(rows: &[Row], column_index: usize, literal: &ScalarValue) -> Vec<i32> {
    let column =
        |pick: &dyn Fn(&Row) -> Option<Vec<u8>>| Column::Bytes(rows.iter().map(pick).collect());
    // Columns are tagged with arbitrary distinct ids — `Filter` addresses them
    // by position, and `ColumnId` has no value-column variants — so any three
    // distinct tags serve.
    let batch = Batch {
        columns: vec![
            (ColumnId::BusinessKey, column(&|r| r.cells()[0].clone())),
            (ColumnId::Payload, column(&|r| r.cells()[1].clone())),
            (ColumnId::Principal, column(&|r| r.cells()[2].clone())),
        ],
        rows: rows.len(),
    };
    let schema = vec![LogicalType::Int4, LogicalType::Int4, LogicalType::Text];
    let predicate = Expr::col(column_index).compare(CmpOp::Eq, Expr::lit(literal.clone()));
    let mut filter = Filter::new(VecSource::new(vec![batch]), predicate, schema);

    // Read the surviving keys back out of column 0.
    let mut keys = Vec::new();
    while let Some(out) = filter.next().expect("pull") {
        let Column::Bytes(cells) = &out.columns[0].1 else {
            panic!("key column is bytes");
        };
        for c in cells {
            let bytes = c.as_ref().expect("a key is never null");
            match ScalarValue::decode(LogicalType::Int4, bytes).expect("decode key") {
                ScalarValue::Int4(k) => keys.push(k),
                // Unreachable: an int4 column always decodes to int4. The arm
                // names no value (a ScalarValue Debug trips CodeQL's
                // cleartext-logging taint, and there is nothing to print here).
                _ => panic!("key column did not decode as int4"),
            }
        }
    }
    keys
}

#[test]
fn vectorized_filter_matches_row_at_a_time_for_equality_predicates() {
    // A spread that exercises equality matches, misses, NULL cells, and repeats.
    let rows = [
        Row {
            key: 1,
            balance: Some(100),
            name: Some("alice"),
        },
        Row {
            key: 2,
            balance: Some(200),
            name: Some("bob"),
        },
        Row {
            key: 3,
            balance: None,
            name: Some("bob"),
        },
        Row {
            key: 4,
            balance: Some(100),
            name: None,
        },
        Row {
            key: 5,
            balance: Some(-7),
            name: Some("世界"),
        },
        Row {
            key: 6,
            balance: None,
            name: None,
        },
        Row {
            key: 7,
            balance: Some(200),
            name: Some("alice"),
        },
    ];

    // (column index, literal) probes across the key, the int value column, and
    // the text value column — including values that match a NULL row's "absent"
    // cell (which must still never match).
    let probes: [(usize, ScalarValue); 7] = [
        (0, ScalarValue::Int4(3)),                 // key hit
        (0, ScalarValue::Int4(99)),                // key miss
        (1, ScalarValue::Int4(100)),               // value hit (two rows), skips NULLs
        (1, ScalarValue::Int4(-7)),                // negative value hit
        (1, ScalarValue::Int4(999)),               // value miss
        (2, ScalarValue::Text("bob".to_owned())),  // text hit (two rows), skips NULLs
        (2, ScalarValue::Text("世界".to_owned())), // multi-byte text hit
    ];

    for (column_index, literal) in probes {
        assert_eq!(
            vectorized(&rows, column_index, &literal),
            row_at_a_time(&rows, column_index, &literal),
            "column {column_index} = {literal:?}",
        );
    }
}
