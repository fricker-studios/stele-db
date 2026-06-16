//! End-to-end `COPY` bulk-load fast path through `SessionEngine` ([STL-240]).
//!
//! The storage-layer mechanics (one fsync per chunk, bounded resident delta,
//! all-or-nothing recovery via the commit gate) are pinned in `stele-storage`'s
//! `bulk_load.rs`; these drive the *whole* `copy_apply` path and pin the
//! engine-visible Definition of Done:
//!
//! * **O(chunks) fsyncs, not O(rows).** A sync-counting disk shows a multi-thousand
//!   row `COPY` fsyncs a handful of times, nowhere near once per row.
//! * **AS OF is all-or-nothing.** A snapshot taken *before* the load sees none of its
//!   rows; the live snapshot after it sees every row — the load commits at one logical
//!   point even though it streamed in many chunks.
//! * **A kill mid-load recovers to zero.** A `COPY` that fails partway (a duplicate
//!   key) leaves the table empty in the live engine *and* after a restart — the chunk
//!   records stay inert without the commit record the load never wrote.
//! * **A committed load survives a restart whole.**

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::significant_drop_tightening,
    clippy::type_complexity
)]

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;
use stele_storage::wal::{Disk, DiskFile};

/// `now()` is a constant; the engine's `MonotonicClock` turns it into the strictly
/// increasing `1, 2, 3, …` writes need, so a row's `sys_from` is its commit order.
#[derive(Clone, Copy)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

// --- a sync-counting in-memory disk (counts every file fsync) ---------------

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
                name.to_string(),
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

// --- helpers ----------------------------------------------------------------

const CREATE: &str = "CREATE TABLE t (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING";

/// `(id, v)` field rows for a `COPY`, ids `0..n`.
fn rows(n: usize) -> Vec<Vec<Option<String>>> {
    (0..n)
        .map(|i| vec![Some(i.to_string()), Some((i * 10).to_string())])
        .collect()
}

fn run<C: Clock + Clone, D: Disk + Clone>(engine: &mut SessionEngine<C, D>, sql: &str) {
    let stmt = parse(sql).expect("parse").remove(0);
    engine
        .execute(&stmt)
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

fn count_rows<C: Clock + Clone, D: Disk + Clone>(
    engine: &mut SessionEngine<C, D>,
    sql: &str,
) -> usize {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(SelectResult { rows, .. }) = engine.execute(&stmt).expect("select")
    else {
        panic!("`{sql}` must return rows");
    };
    rows.len()
}

fn copy_stmt() -> stele_sql::Statement {
    parse("COPY t FROM STDIN").expect("parse").remove(0)
}

// --- tests ------------------------------------------------------------------

#[test]
fn large_copy_commits_and_as_of_is_all_or_nothing() {
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(&mut engine, CREATE);

    // The instant just before the load — every loaded row commits strictly after it.
    let before = engine.commit_clock();

    let n = 5_000; // past BULK_COPY_CHUNK_ROWS = 4096 (the chunked path)
    let loaded = engine.copy_apply(&copy_stmt(), &rows(n)).expect("copy");
    assert_eq!(loaded, n as u64, "COPY n counts every row");

    let after = engine.commit_clock();

    // AS OF before the load: the table exists but holds none of the load.
    assert_eq!(
        count_rows(
            &mut engine,
            &format!("SELECT id FROM t FOR SYSTEM_TIME AS OF {}", before.0)
        ),
        0,
        "a snapshot before the load sees none of it",
    );
    // Live, and AS OF after: every row.
    assert_eq!(count_rows(&mut engine, "SELECT id FROM t"), n);
    assert_eq!(
        count_rows(
            &mut engine,
            &format!("SELECT id FROM t FOR SYSTEM_TIME AS OF {}", after.0)
        ),
        n,
        "a snapshot after the load sees all of it",
    );
}

#[test]
fn large_copy_fsyncs_per_chunk_not_per_row() {
    let disk = CountingDisk::new();
    let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
    run(&mut engine, CREATE);

    let n = 5_000; // 2 chunks at BULK_COPY_CHUNK_ROWS = 4096
    let before = disk.syncs();
    engine.copy_apply(&copy_stmt(), &rows(n)).expect("copy");
    let load_syncs = disk.syncs() - before;

    // Fsync-bounded, not row-bounded: a 5k-row load fsyncs a handful of times (a few
    // chunk records + the isolating flush + the commit record), nowhere near 5k.
    assert!(
        load_syncs < 32,
        "a {n}-row COPY fsynced {load_syncs} times — expected O(chunks), not O(rows)"
    );
}

#[test]
fn committed_bulk_copy_survives_a_restart() {
    let disk = MemDisk::new();
    let n = 5_000;
    {
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        run(&mut engine, CREATE);
        engine.copy_apply(&copy_stmt(), &rows(n)).expect("copy");
        // engine dropped here — a clean process exit with the load committed.
    }
    let mut recovered = SessionEngine::recover(disk, ZeroClock).expect("recover");
    assert_eq!(
        count_rows(&mut recovered, "SELECT id FROM t"),
        n,
        "a committed bulk load recovers whole",
    );
}

#[test]
fn aborted_bulk_copy_leaves_zero_rows_live_and_after_recovery() {
    let disk = MemDisk::new();
    {
        let mut engine = SessionEngine::open(disk.clone(), ZeroClock);
        run(&mut engine, CREATE);

        // A load whose last row re-keys row 0 (already live, and by now spilled): the
        // duplicate aborts the whole statement after several chunks have committed
        // their (inert) records.
        let n = 5_000;
        let mut data = rows(n);
        data.push(vec![Some("0".to_string()), Some("999".to_string())]);
        let result = engine.copy_apply(&copy_stmt(), &data);
        assert!(result.is_err(), "a duplicate key aborts the load");

        // The live engine shows the table empty — the whole load rolled back.
        assert_eq!(
            count_rows(&mut engine, "SELECT id FROM t"),
            0,
            "an aborted bulk load leaves the table unchanged",
        );
    }
    // And a restart agrees: the chunk records never got a commit record, so recovery
    // discards them.
    let mut recovered = SessionEngine::recover(disk, ZeroClock).expect("recover");
    assert_eq!(
        count_rows(&mut recovered, "SELECT id FROM t"),
        0,
        "an aborted bulk load recovers to zero rows",
    );
}
