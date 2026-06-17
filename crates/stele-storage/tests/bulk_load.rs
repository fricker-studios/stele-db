//! Bulk-load fast-path mechanics ([STL-240]).
//!
//! The chunked bulk group is the storage half of `COPY`-scale ingest: a large load
//! streams through the delta in chunks, each chunk committed as one **two-phase** WAL
//! record + fsync, all sharing one `txn_id` and vouched by a single commit record at
//! the engine layer. These tests pin the three properties the ticket's Definition of
//! Done turns on, at the mechanism level (the engine-layer orchestration + AS OF
//! oracle lives in `stele-engine`):
//!
//! * **O(chunks) fsyncs, not O(rows).** A [`CountingDisk`] backing the WAL counts one
//!   `sync` per chunk, regardless of how many rows the chunk carries.
//! * **Bounded memory.** Bulk inserts apply *spilling*, so the in-memory delta stays
//!   under its byte threshold across a load far larger than it — the resident tier is
//!   bounded while the spilled total grows with the data.
//! * **All-or-nothing recovery.** The chunk records are inert until a commit record
//!   vouches their `txn_id`: replaying with the transaction **absent** from the
//!   committed set recovers **zero** rows (a crash mid-load), and with it **present**
//!   recovers **every** row — exactly the [`CommittedTxns`] gate a multi-table commit
//!   already rides ([STL-215]).
//!
//! The abort path (a wholesale delta discard, since spilled rows are not removable in
//! place) is covered too.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::significant_drop_tightening,
    clippy::type_complexity
)]

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};
use stele_storage::backend::MemDisk;
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Snapshot};
use stele_storage::dml::{self, CommittedTxns, DmlWriter};
use stele_storage::systime::EmptySealed;
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::wal::{Checkpoint, Disk, DiskFile, Wal, WalConfig};

// --- a sync-counting in-memory disk ----------------------------------------

/// A heap-backed [`Disk`] that counts every `sync` on its files — so a test can
/// assert the WAL fsynced once per chunk, not once per row. Mirrors the read-counting
/// `CountingDisk` in `validity_spill_io.rs`, counting durability points instead.
#[derive(Default, Clone)]
struct CountingDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
    syncs: Arc<AtomicU64>,
}

impl CountingDisk {
    fn new() -> Self {
        Self::default()
    }

    fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::SeqCst)
    }
}

struct CountingFile {
    bytes: Arc<Mutex<Vec<u8>>>,
    syncs: Arc<AtomicU64>,
}

impl Disk for CountingDisk {
    type File = CountingFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        let mut files = self.inner.lock().unwrap();
        if files.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{name} already exists"),
            ));
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        files.insert(name.to_string(), Arc::clone(&bytes));
        Ok(CountingFile {
            bytes,
            syncs: Arc::clone(&self.syncs),
        })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(CountingFile {
            bytes,
            syncs: Arc::clone(&self.syncs),
        })
    }

    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.inner.lock().unwrap().keys().cloned().collect())
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        let mut files = self.inner.lock().unwrap();
        if files.remove(name).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()));
        }
        Ok(())
    }

    fn sync_dir(&self) -> io::Result<()> {
        Ok(())
    }
}

impl DiskFile for CountingFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let src = self.bytes.lock().unwrap();
        let start = offset as usize;
        if start >= src.len() {
            return Ok(0);
        }
        let end = (start + buf.len()).min(src.len());
        let n = end - start;
        buf[..n].copy_from_slice(&src[start..end]);
        Ok(n)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.bytes.lock().unwrap().len() as u64
    }
}

// --- harness ----------------------------------------------------------------

/// A deterministic, strictly-increasing clock — one tick per `now()`.
struct StepClock(AtomicI64);
impl StepClock {
    const fn new(start: i64) -> Self {
        Self(AtomicI64::new(start))
    }
}
impl Clock for StepClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

fn who() -> Principal {
    Principal::new(b"bulk-loader".to_vec())
}

/// A bulk writer whose WAL is sync-counted and whose delta spills at a tiny threshold,
/// so a few hundred rows comfortably exceed it. Returns the writer, its delta/index,
/// and the WAL's disk (for the sync count). The delta shares the WAL's disk *type*
/// ([`DmlWriter`] ties them) but is a distinct instance, so the returned counter is the
/// WAL's alone.
fn new_bulk_writer(
    spill_threshold_bytes: u64,
) -> (
    DmlWriter<StepClock, CountingDisk>,
    Delta<CountingDisk>,
    ValidityIndex<MemDisk>,
    CountingDisk,
) {
    let wal_disk = CountingDisk::new();
    let wal = Wal::open(wal_disk.clone(), WalConfig::default()).expect("open wal");
    let delta = Delta::open(
        CountingDisk::new(),
        DeltaConfig {
            spill_threshold_bytes,
        },
    )
    .expect("open delta");
    let index = ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open index");
    let writer = DmlWriter::new(wal, StepClock::new(1), false);
    (writer, delta, index, wal_disk)
}

/// One bulk insert of a distinct key carrying a small payload.
fn insert_row(
    dml: &mut DmlWriter<StepClock, CountingDisk>,
    delta: &mut Delta<CountingDisk>,
    index: &mut ValidityIndex<MemDisk>,
    i: usize,
    txn_id: TxnId,
) {
    dml.insert(
        delta,
        index,
        &EmptySealed,
        BusinessKey::new(format!("k{i:08}").into_bytes()),
        None,
        Some(format!("payload-for-row-{i:08}").into_bytes()),
        0,
        txn_id,
        who(),
    )
    .expect("bulk insert stages");
}

/// Drive `chunks * per_chunk` rows through the bulk path, fsyncing once per chunk.
/// Leaves the group ended (the chunk records are durable). Returns the total rows.
fn run_bulk_load(
    dml: &mut DmlWriter<StepClock, CountingDisk>,
    delta: &mut Delta<CountingDisk>,
    index: &mut ValidityIndex<MemDisk>,
    txn_id: TxnId,
    chunks: usize,
    per_chunk: usize,
) -> usize {
    dml.begin_bulk_group();
    let mut next = 0;
    for _ in 0..chunks {
        for _ in 0..per_chunk {
            insert_row(dml, delta, index, next, txn_id);
            next += 1;
        }
        dml.commit_bulk_chunk(txn_id).expect("commit chunk");
    }
    dml.end_bulk_group();
    next
}

fn live_count(delta: &Delta<CountingDisk>, index: &ValidityIndex<MemDisk>) -> usize {
    delta
        .range_scan(.., Snapshot(SystemTimeMicros(i64::MAX - 1)), index)
        .expect("scan")
        .len()
}

// --- tests ------------------------------------------------------------------

#[test]
fn bulk_chunks_fsync_once_per_chunk_and_keep_the_delta_bounded() {
    // A tiny threshold so the load (8 chunks × 64 rows = 512 rows) dwarfs it.
    let threshold = 256;
    let (mut dml, mut delta, mut index, wal_disk) = new_bulk_writer(threshold);

    let chunks = 8;
    let per_chunk = 64;
    let total = run_bulk_load(
        &mut dml,
        &mut delta,
        &mut index,
        TxnId(1),
        chunks,
        per_chunk,
    );
    assert_eq!(total, chunks * per_chunk);

    // O(chunks), not O(rows): exactly one WAL fsync per chunk.
    assert_eq!(
        wal_disk.syncs(),
        chunks as u64,
        "the WAL fsyncs once per chunk, not once per row"
    );

    // Bounded memory: the load spilled, and the resident tier stayed under the
    // threshold (plus at most one over-threshold row, the documented soft edge).
    assert!(delta.is_spilled(), "a load past the threshold spills");
    assert!(
        delta.byte_size() <= threshold * 2,
        "resident bytes stay bounded ({} vs threshold {threshold})",
        delta.byte_size()
    );

    // Every row is nonetheless staged and live.
    assert_eq!(live_count(&delta, &index), total);
}

#[test]
fn bulk_load_recovers_all_or_nothing() {
    let (mut dml, mut delta, mut index, _wal_disk) = new_bulk_writer(256);
    let txn = TxnId(7);
    let total = run_bulk_load(&mut dml, &mut delta, &mut index, txn, 6, 50);
    let wal = dml.wal().clone();
    let fence = wal.durable_end();

    // Crash *before* the commit record: the transaction is absent from the committed
    // set, so every chunk record is discarded — zero rows recover.
    {
        let mut rdelta = Delta::open(CountingDisk::new(), DeltaConfig::default()).expect("delta");
        let mut rindex =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let applied = dml::recover_replay(
            &wal,
            &mut rdelta,
            &mut rindex,
            Checkpoint::BEGIN,
            fence,
            &CommittedTxns::Only(BTreeSet::new()),
        )
        .expect("replay");
        assert_eq!(applied, 0, "an uncommitted bulk load replays nothing");
        assert_eq!(live_count(&rdelta, &rindex), 0);
    }

    // Crash *after* the commit record: the transaction is vouched, so every chunk
    // record replays — all rows recover.
    {
        let mut rdelta = Delta::open(CountingDisk::new(), DeltaConfig::default()).expect("delta");
        let mut rindex =
            ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("index");
        let mut committed = BTreeSet::new();
        committed.insert(txn);
        let applied = dml::recover_replay(
            &wal,
            &mut rdelta,
            &mut rindex,
            Checkpoint::BEGIN,
            fence,
            &CommittedTxns::Only(committed),
        )
        .expect("replay");
        assert_eq!(applied, total, "a committed bulk load replays every row");
        assert_eq!(live_count(&rdelta, &rindex), total);
    }
}

#[test]
fn bulk_abort_discards_the_spilled_delta() {
    let (mut dml, mut delta, mut index, _wal_disk) = new_bulk_writer(256);
    dml.begin_bulk_group();
    for i in 0..200 {
        insert_row(&mut dml, &mut delta, &mut index, i, TxnId(3));
        if i % 50 == 49 {
            dml.commit_bulk_chunk(TxnId(3)).expect("commit chunk");
        }
    }
    assert!(delta.is_spilled(), "the aborted load had spilled");

    // Abort rolls the load back wholesale — the delta is empty (its already-written
    // chunk records stay inert without a commit record, dropped on recovery).
    dml.abort_group(&mut delta, &mut index);
    assert_eq!(delta.byte_size(), 0, "abort clears the resident tier");
    assert!(!delta.is_spilled(), "abort drops the spill files");
    assert_eq!(live_count(&delta, &index), 0);
}
