//! Inline-provenance integration tests.
//!
//! Scope (STL-93) — the two Definition-of-Done bullets:
//!
//! 1. **Every persisted version carries all three provenance columns,
//!    populated.** A write driven through the real system-time write path is
//!    flushed into a sealed segment and read back; `txn_id`, `committed_at`,
//!    and `principal` survive end-to-end, with `committed_at` equal to the
//!    stamped commit (`sys_from`) and `txn_id` / `principal` exactly what the
//!    caller (the transaction manager) supplied.
//! 2. **Provenance compresses comparably to the data columns** — the three
//!    provenance columns add ≤ 10% on top of a representative segment.
//!
//! Provenance is *always on* (invariant 5), so unlike valid-time there is no
//! opt-in to exercise: a write that omitted it would not type-check.

#![allow(
    clippy::significant_drop_tightening,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::{Clock, SystemTimeMicros};
use stele_storage::delta::{BusinessKey, Delta, DeltaConfig, Version};
use stele_storage::segment::{ColumnId, SegmentReader, SegmentWriter};
use stele_storage::systime::{EmptySealed, SysTimeWriter};
use stele_storage::validity::{ValidityConfig, ValidityIndex};
use stele_storage::wal::{Disk, DiskFile};

// --- MemDisk: minimal in-memory Disk for tests ------------------------------

#[derive(Default, Clone)]
struct MemDisk {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
}

impl MemDisk {
    fn new() -> Self {
        Self::default()
    }
}

struct MemFile {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Disk for MemDisk {
    type File = MemFile;

    fn create(&self, name: &str) -> io::Result<Self::File> {
        let mut files = self.inner.lock().unwrap();
        if files.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                name.to_string(),
            ));
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        files.insert(name.to_string(), Arc::clone(&bytes));
        Ok(MemFile { bytes })
    }

    fn open(&self, name: &str) -> io::Result<Self::File> {
        let files = self.inner.lock().unwrap();
        let bytes = files
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name.to_string()))?
            .clone();
        Ok(MemFile { bytes })
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
}

impl DiskFile for MemFile {
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
        Ok(())
    }

    fn len(&self) -> u64 {
        self.bytes.lock().unwrap().len() as u64
    }
}

/// Deterministic, strictly-increasing clock — so `committed_at` values are
/// predictable and the test never depends on the wall clock.
struct StubClock(AtomicI64);
impl Clock for StubClock {
    fn now(&self) -> SystemTimeMicros {
        // Advance by one micro per read; the writer's monotonic guard would
        // cope with a flat clock too, but a moving one keeps sys_from distinct.
        SystemTimeMicros(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

fn new_delta() -> Delta<MemDisk> {
    Delta::open(MemDisk::new(), DeltaConfig::default()).expect("open delta")
}

fn flush_to_segment(disk: &MemDisk, name: &str, rows: Vec<Version>) -> SegmentReader<MemFile> {
    let mut w = SegmentWriter::create(disk, name).expect("create segment");
    for v in rows {
        w.push(v).expect("push");
    }
    w.finish().expect("finish");
    SegmentReader::open(disk, name).expect("open segment")
}

// --- DoD bullet 1: every persisted version carries populated provenance ------

#[test]
fn every_persisted_version_carries_its_three_provenance_columns() {
    // Drive writes through the real system-time write path: the transaction
    // manager's contribution (txn_id + principal) is handed down per write,
    // and the writer stamps committed_at = the commit timestamp.
    let mut delta = new_delta();
    // The write path materializes period ends into the validity index; these
    // are all fresh inserts, so the index stays empty, but the writer still
    // requires it (v6, ADR-0023).
    let mut index =
        ValidityIndex::open(MemDisk::new(), ValidityConfig::default()).expect("open index");
    let mut writer = SysTimeWriter::new(StubClock(AtomicI64::new(1_000)));

    // key -> (txn_id, principal) we handed to the writer.
    let mut expected: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();
    for i in 0..16u64 {
        let key = format!("acct-{i:02}").into_bytes();
        let principal = format!("user-{i}").into_bytes();
        writer
            .insert(
                &mut delta,
                &mut index,
                &EmptySealed,
                BusinessKey::new(key.clone()),
                format!("balance={i}").into_bytes(),
                TxnId(i),
                Principal::new(principal.clone()),
            )
            .unwrap();
        expected.insert(key, (i, principal));
    }

    // Flush the delta into a sealed segment and read every version back.
    let rows = delta.flush_to_segment().expect("flush");
    let seg_disk = MemDisk::new();
    let reader = flush_to_segment(&seg_disk, "prov.seg", rows);

    // All three provenance columns must physically exist in the segment.
    for col in [ColumnId::TxnId, ColumnId::CommittedAt, ColumnId::Principal] {
        assert!(
            reader.column_byte_len(col).is_some(),
            "provenance column {col:?} must be present in the segment"
        );
    }

    let versions = reader.read_versions().expect("read versions");
    assert_eq!(versions.len(), expected.len(), "every write is persisted");
    for v in &versions {
        let (txn, principal) = expected
            .get(v.business_key.as_bytes())
            .expect("known business key");
        assert_eq!(v.provenance.txn_id, TxnId(*txn), "txn_id round-trips");
        assert_eq!(
            v.provenance.principal.as_bytes(),
            principal.as_slice(),
            "principal round-trips",
        );
        // committed_at is captured at commit and equals the stamped sys_from on
        // the single-writer path — never reconstructed.
        assert_eq!(
            v.provenance.committed_at, v.sys_from,
            "committed_at is the commit timestamp",
        );
        assert_ne!(
            v.provenance.committed_at,
            SystemTimeMicros(0),
            "committed_at is populated, not a default",
        );
    }
}

// --- DoD bullet 2: provenance overhead stays small --------------------------

/// On-disk bytes the columns in `cols` occupy across the segment.
fn columns_bytes(reader: &SegmentReader<MemFile>, cols: &[ColumnId]) -> u64 {
    cols.iter()
        .map(|&c| reader.column_byte_len(c).unwrap_or(0))
        .sum()
}

#[test]
fn provenance_columns_add_under_ten_percent_overhead() {
    // A representative segment: a wide audit/analytical fact row (the workload
    // Stele targets), bulk-ingested by one principal across a handful of
    // transactions — so txn_id is near-monotonic and the principal repeats, the
    // shapes architecture §3.2 calls "compress well".
    const ROWS: u64 = 4096;
    const PAYLOAD_LEN: usize = 512; // a wide fact row
    let principal = Principal::new(b"svc-ingest@node-01".to_vec());

    let mut rows = Vec::with_capacity(ROWS as usize);
    for i in 0..ROWS {
        // 24-byte business key (a hash key + tag is a realistic width).
        let key = format!("hk-{i:020}").into_bytes();
        let payload = vec![0x7Au8; PAYLOAD_LEN];
        rows.push(Version::open(
            BusinessKey::new(key),
            SystemTimeMicros(i64::try_from(i).unwrap() + 1),
            Provenance::new(
                TxnId(i / 64), // ~64 rows per transaction: a bulk-ingest batch
                SystemTimeMicros(i64::try_from(i).unwrap() + 1),
                principal.clone(),
            ),
            payload,
        ));
    }

    let disk = MemDisk::new();
    let reader = flush_to_segment(&disk, "bench.seg", rows);

    let provenance = columns_bytes(
        &reader,
        &[ColumnId::TxnId, ColumnId::CommittedAt, ColumnId::Principal],
    );
    // Everything that is *not* provenance — the baseline a no-provenance
    // segment would have written. There is no `sys_to` column (v6, ADR-0023):
    // the period end lives in the validity index, not the segment.
    let baseline = columns_bytes(
        &reader,
        &[ColumnId::BusinessKey, ColumnId::SysFrom, ColumnId::Payload],
    );

    // "Overhead" = the fraction the provenance columns add on top of the
    // baseline. The bound is generous (≤ 10%) and, crucially, met *with the
    // Plain codec* — no compression yet. Once the delta/FOR/RLE codecs land
    // (separate STL-76 tickets) the monotonic txn_id and the repeated principal
    // collapse toward zero, so this is the floor, not the steady state.
    let overhead = provenance as f64 / baseline as f64;
    assert!(
        overhead <= 0.10,
        "provenance overhead {overhead:.4} exceeds 10% \
         (provenance={provenance} B, baseline={baseline} B)"
    );
}
