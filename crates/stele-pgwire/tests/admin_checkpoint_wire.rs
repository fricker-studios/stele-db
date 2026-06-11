//! The operator-facing storage admin commands over the wire (STL-219).
//!
//! A stock `tokio-postgres` client connects to a live [`Server`] and issues
//! `CHECKPOINT` / `FLUSH` over the **simple-query** protocol. The test proves the
//! two halves of the ticket's Definition of Done:
//!
//! * **Returns cleanly.** Each command completes without a wire error and the
//!   driver receives a `CommandComplete` (no rows) — the full parse → route →
//!   `SessionEngine::{checkpoint,flush}` → `CommandComplete` path works against a
//!   third-party client, not just the in-crate synthetic one.
//! * **Flushes.** The effect is observed on the shared backing disk: `FLUSH`
//!   seals each table's delta into a `seg-*.seg` segment (bounded recovery,
//!   STL-177/195), while the lightweight `CHECKPOINT` seals nothing.
//!
//! The exact `CommandComplete` tag strings (`CHECKPOINT` / `FLUSH`) are pinned by
//! the `stele-engine` unit test; `tokio-postgres` surfaces a tag's row count, not
//! its text, so the wire assertion here is "completed, no rows".

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::{Disk, MemDisk};
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The number of sealed segment files (`…seg-NNN.seg`) resident on `disk` across
/// every table's namespace — the on-disk evidence a flush produced. The `seg-`
/// infix is unique to segment files among the tier's filenames (WAL, delta
/// spills, checkpoint/catalog manifests carry none).
fn segment_files(disk: &MemDisk) -> usize {
    disk.list()
        .expect("list backing disk")
        .iter()
        .filter(|name| name.contains("seg-"))
        .count()
}

/// Assert `messages` is a single clean `CommandComplete` with no result rows —
/// what an admin command replies with.
fn assert_completed_no_rows(messages: &[SimpleQueryMessage], what: &str) {
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))),
        "{what} replies with CommandComplete",
    );
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_))),
        "{what} returns no rows",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_and_flush_drive_the_engine_over_the_wire() {
    // Keep a handle to the shared backing disk so the flush effect is observable.
    let disk = MemDisk::new();
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(disk.clone(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        )
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert");
    assert_eq!(
        segment_files(&disk),
        0,
        "no segment sealed before any flush"
    );

    // CHECKPOINT — the lightweight fence: returns cleanly, seals nothing.
    let messages = client.simple_query("CHECKPOINT").await.expect("checkpoint");
    assert_completed_no_rows(&messages, "CHECKPOINT");
    assert_eq!(
        segment_files(&disk),
        0,
        "CHECKPOINT fences the WAL but seals no segment",
    );

    // FLUSH — seals the delta into a segment, observable on the backing disk.
    let messages = client.simple_query("FLUSH").await.expect("flush");
    assert_completed_no_rows(&messages, "FLUSH");
    assert_eq!(
        segment_files(&disk),
        1,
        "FLUSH sealed the table's delta into a segment",
    );

    drop(client);
    let _ = driver.await;
}
