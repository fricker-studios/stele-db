//! Operator-framework integration tests (STL-169).
//!
//! Drive the batch-at-a-time pull pipeline the way a query would: write through
//! the real DML path, then pull a `scan → project` pipeline and assert
//!
//! * **result-equivalence** — concatenating every batch the pipeline emits
//!   reproduces, row-for-row, the single batch [`SnapshotScan::execute`] returns
//!   today (the DoD's "result-equivalent to today's single-batch scan");
//! * **batch sizing** — a configured `batch_rows` is honored: each batch holds
//!   at most that many rows and the windows tile the result exactly;
//! * **projection** — the [`Project`] operator selects and reorders its child's
//!   columns, and names a column the child did not emit → `MissingColumn`;
//! * **empty stream** — a scan that resolves no rows emits no batches;
//! * **zero-copy batches** — slicing or cloning a column, and the source's
//!   emitted batch windows, share one backing cell buffer rather than copying
//!   payload bytes ([STL-191]), shown by cell-address aliasing.
//!
//! [STL-191]: https://allegromusic.atlassian.net/browse/STL-191

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use std::sync::atomic::{AtomicI64, Ordering};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};
use stele_exec::{
    Batch, Column, DEFAULT_BATCH_SIZE, Operator, Project, ScanError, ScanSource, SnapshotScan,
};
use stele_storage::backend::{MemDisk, MemFile};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::DmlWriter;
use stele_storage::segment::{ColumnId, Predicate, SegmentReader, ZoneBound};
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::wal::{Wal, WalConfig};

// --- harness ---------------------------------------------------------------

fn who() -> Principal {
    Principal::new(b"tester".to_vec())
}

/// `id INT PRIMARY KEY` as the business key: big-endian bytes so byte order
/// matches integer order.
fn key_of(id: i64) -> BusinessKey {
    BusinessKey::new(id.to_be_bytes().to_vec())
}

/// Deterministic clock — one tick per `now()` (ADR-0010).
struct StepClock(AtomicI64);
impl Clock for StepClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

const NO_SEGMENTS: [SegmentReader<MemFile>; 0] = [];

/// A delta+index holding `count` live single-key rows (`id = 1..=count`, payload
/// `b"v{id}"`), staged through the real DML path, plus a [`Snapshot`] at the last
/// commit at which every row is live.
fn table_with_rows(count: i64) -> (Delta<MemDisk>, ValidityIndex<MemDisk>, Snapshot) {
    let wal = Wal::open(MemDisk::new(), WalConfig::default()).expect("open wal");
    let mut delta = Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta");
    let mut index =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open index");
    let mut dml = DmlWriter::new(wal, StepClock(AtomicI64::new(1_000)), false);
    let mut last = SystemTimeMicros(0);
    for id in 1..=count {
        last = dml
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                key_of(id),
                None,
                Some(format!("v{id}").into_bytes()),
                0,
                TxnId(id.unsigned_abs()),
                who(),
            )
            .expect("insert")
            .commit;
    }
    // The versions are open (`sys_to == +inf`, half-open), so the last commit
    // resolves every inserted row as live.
    (delta, index, Snapshot(last))
}

/// Drain an operator to exhaustion, returning every batch it emits.
fn drain(mut op: impl Operator) -> Vec<Batch> {
    let mut batches = Vec::new();
    while let Some(b) = op.next().expect("operator pull") {
        batches.push(b);
    }
    batches
}

/// Flatten the bytes column `col` across a sequence of batches into one row-major
/// vector — the concatenation the pull pipeline is supposed to preserve.
fn bytes_of(batches: &[Batch], col: ColumnId) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    for b in batches {
        let (_, column) = b
            .columns
            .iter()
            .find(|(c, _)| *c == col)
            .expect("projected column present");
        match column {
            Column::Bytes(rows) => out.extend(rows.iter().cloned()),
            Column::I64(_) => panic!("column {col:?} is i64, expected bytes"),
        }
    }
    out
}

// --- result-equivalence ----------------------------------------------------

#[test]
fn pipeline_is_result_equivalent_to_single_batch_execute() {
    let (delta, index, snap) = table_with_rows(5);
    let projection = vec![ColumnId::BusinessKey, ColumnId::Payload];

    // Today's single-batch path.
    let direct = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap)
        .project(projection.clone())
        .execute()
        .expect("execute");

    // The pull pipeline: scan source → project. A small batch size forces
    // multiple batches so the concatenation is actually exercised.
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap)
        .project(projection.clone())
        .into_source(2);
    let batches = drain(Project::new(source, projection.clone()));

    // Same total rows, same per-column values in the same order.
    let total: usize = batches.iter().map(|b| b.rows).sum();
    assert_eq!(total, direct.batch.rows);
    for &col in &projection {
        assert_eq!(
            bytes_of(&batches, col),
            bytes_of(std::slice::from_ref(&direct.batch), col),
            "column {col:?} differs between pipeline and execute()",
        );
    }
}

// --- batch sizing ----------------------------------------------------------

#[test]
fn batch_size_is_honored_and_windows_tile_the_result() {
    let (delta, index, snap) = table_with_rows(5);
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap).into_source(2);
    let batches = drain(source);

    // 5 rows in windows of 2 → [2, 2, 1].
    assert_eq!(
        batches.iter().map(|b| b.rows).collect::<Vec<_>>(),
        [2, 2, 1]
    );
    for b in &batches {
        assert!(b.rows <= 2, "no batch exceeds the configured size");
        // Every column in a batch is the same height as the batch.
        for (_, col) in &b.columns {
            assert_eq!(col.len(), b.rows);
        }
    }
}

#[test]
fn one_batch_when_result_fits_the_batch_size() {
    let (delta, index, snap) = table_with_rows(3);
    let source =
        SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap).into_source(DEFAULT_BATCH_SIZE);
    let batches = drain(source);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].rows, 3);
}

#[test]
fn zero_batch_size_is_clamped_to_one() {
    let (delta, index, snap) = table_with_rows(3);
    let source = ScanSource::new(SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap), 0);
    let batches = drain(source);
    assert_eq!(
        batches.iter().map(|b| b.rows).collect::<Vec<_>>(),
        [1, 1, 1]
    );
}

// --- projection ------------------------------------------------------------

#[test]
fn project_selects_and_reorders_columns() {
    let (delta, index, snap) = table_with_rows(2);
    // Source emits the full always-on column set; Project narrows + reorders to
    // (Payload, BusinessKey) — the reverse of storage order.
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap).into_source(8);
    let batches = drain(Project::new(
        source,
        vec![ColumnId::Payload, ColumnId::BusinessKey],
    ));
    assert_eq!(batches.len(), 1);
    let cols: Vec<ColumnId> = batches[0].columns.iter().map(|(c, _)| *c).collect();
    assert_eq!(cols, [ColumnId::Payload, ColumnId::BusinessKey]);
}

#[test]
fn project_can_emit_a_column_more_than_once() {
    let (delta, index, snap) = table_with_rows(2);
    // A degenerate projection that names BusinessKey twice: the first selection
    // moves the column out, the repeat clones the already-emitted copy.
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap).into_source(8);
    let batches = drain(Project::new(
        source,
        vec![ColumnId::BusinessKey, ColumnId::BusinessKey],
    ));
    assert_eq!(batches.len(), 1);
    let cols: Vec<ColumnId> = batches[0].columns.iter().map(|(c, _)| *c).collect();
    assert_eq!(cols, [ColumnId::BusinessKey, ColumnId::BusinessKey]);
    // Both copies carry identical data.
    assert_eq!(batches[0].columns[0].1, batches[0].columns[1].1);
}

#[test]
fn project_for_a_column_the_child_did_not_emit_errors() {
    let (delta, index, snap) = table_with_rows(2);
    // Source projects only BusinessKey; Project then asks for Payload, which the
    // child never emitted → MissingColumn.
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap)
        .project(vec![ColumnId::BusinessKey])
        .into_source(8);
    let mut project = Project::new(source, vec![ColumnId::Payload]);
    match project.next() {
        Err(ScanError::MissingColumn(ColumnId::Payload)) => {}
        other => panic!("expected MissingColumn(Payload), got {other:?}"),
    }
}

// --- empty stream ----------------------------------------------------------

#[test]
fn empty_result_emits_no_batches() {
    let (delta, index, snap) = table_with_rows(3);
    // A predicate no row satisfies (id = 999) resolves to zero rows.
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap)
        .filter(Predicate::Eq {
            column: ColumnId::BusinessKey,
            value: ZoneBound::Bytes(999_i64.to_be_bytes().to_vec()),
        })
        .into_source(4);
    let batches = drain(source);
    assert!(batches.is_empty(), "empty scan must emit no batches");
}

// --- stats -----------------------------------------------------------------

#[test]
fn source_exposes_scan_stats_after_first_pull() {
    let (delta, index, snap) = table_with_rows(2);
    let mut source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap).into_source(8);
    assert!(source.stats().is_none(), "no stats before the first pull");
    source.next().expect("first pull");
    let stats = source.stats().expect("stats after first pull");
    // No sealed segments in this fixture, so the accounting is all zeros.
    assert_eq!(stats.segments_total, 0);
}

// --- zero-copy representation (STL-191) -------------------------------------

/// The payload column's cells as the raw windowed slice — for the aliasing
/// assertions below, which compare cell *addresses*, not contents.
fn payload_cells(batch: &Batch) -> &[Option<Vec<u8>>] {
    let (_, column) = batch
        .columns
        .iter()
        .find(|(c, _)| *c == ColumnId::Payload)
        .expect("payload projected");
    match column {
        Column::Bytes(cells) => cells.as_slice(),
        Column::I64(_) => panic!("payload is a bytes column"),
    }
}

#[test]
fn slice_shares_the_backing_cells_instead_of_copying() {
    let col = Column::Bytes(
        vec![
            Some(b"alpha".to_vec()),
            Some(b"beta".to_vec()),
            None,
            Some(b"gamma".to_vec()),
        ]
        .into(),
    );
    let Column::Bytes(all) = &col else {
        unreachable!()
    };

    let window = col.slice(1, 2);
    let Column::Bytes(sliced) = &window else {
        panic!("slice preserves the column variant")
    };

    // The window's cells are the very same memory as the original's — a slice
    // is a refcount bump over the shared buffer, not a copy of the cells…
    assert!(std::ptr::eq(&raw const all[1], &raw const sliced[0]));
    assert!(std::ptr::eq(&raw const all[2], &raw const sliced[1]));
    // …so a cell's payload bytes stay the same heap allocation, byte pointer
    // and all (a deep copy would re-allocate them).
    assert_eq!(
        all[1].as_ref().map(Vec::as_ptr),
        sliced[0].as_ref().map(Vec::as_ptr),
    );
    // Equality still sees only the window's contents, not the buffer around it.
    assert_eq!(
        window,
        Column::Bytes(vec![Some(b"beta".to_vec()), None].into())
    );
}

#[test]
// The clone *is* the unit under test — its sharing is what the assertion probes.
#[allow(clippy::redundant_clone)]
fn clone_shares_the_backing_cells_instead_of_copying() {
    // `Project` re-emits columns by move, and its degenerate duplicate-projection
    // path by clone — which must be equally shallow.
    let col = Column::I64(vec![1, 2, 3].into());
    let copy = col.clone();
    let (Column::I64(a), Column::I64(b)) = (&col, &copy) else {
        unreachable!()
    };
    assert!(std::ptr::eq(&raw const a[0], &raw const b[0]));
}

#[test]
fn scan_source_batches_are_windows_over_one_shared_buffer() {
    // End-to-end through the pull pipeline: each emitted batch is a zero-copy
    // window over the one resolved payload column, so adjacent batches' cells
    // sit end-to-end in the same backing allocation — batch N+1 begins exactly
    // where batch N ends.
    let (delta, index, snap) = table_with_rows(5);
    let source = SnapshotScan::new(&delta, &index, &NO_SEGMENTS, snap).into_source(2);
    let batches = drain(source);
    assert_eq!(batches.len(), 3, "5 rows in windows of 2");
    for pair in batches.windows(2) {
        let (a, b) = (payload_cells(&pair[0]), payload_cells(&pair[1]));
        assert_eq!(
            a.as_ptr().wrapping_add(a.len()),
            b.as_ptr(),
            "adjacent batches must window the same shared buffer",
        );
    }
}
