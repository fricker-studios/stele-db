//! Multi-statement transactions over the wire, driven by the real
//! `tokio-postgres` client (STL-174 Definition of Done, bullet 2; STL-175
//! snapshot isolation; STL-176 savepoints).
//!
//! `BEGIN … COMMIT` is atomic — every buffered write lands together — and
//! `BEGIN … ROLLBACK` discards the lot. The transaction state is per connection
//! and persists across simple-query messages, so each `BEGIN`/DML/`COMMIT` is
//! sent as its own `simple_query` to prove the connection carries the state
//! between messages (not just within one batch). Under **snapshot isolation**
//! (STL-175) a transaction reads one consistent snapshot pinned at `BEGIN` — with
//! its own buffered writes overlaid on it (STL-203, read-your-own-writes) — and a
//! write-write conflict surfaces at `COMMIT` as a retryable serialization failure
//! (SQLSTATE `40001`) — both exercised here across two connections. **Savepoints**
//! (STL-176) extend the same loop: `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` /
//! `RELEASE SAVEPOINT` carve nested rollback points out of the one buffered write
//! set, and `ROLLBACK TO` undoes only the writes staged after the savepoint while
//! the transaction continues. After an error inside the block, `ROLLBACK TO` a
//! pre-error savepoint **recovers** the aborted transaction rather than losing the
//! whole block — Postgres's `in_failed_sql_transaction` escape hatch (STL-205) —
//! while `SAVEPOINT` / `RELEASE` stay refused there. Both paths ride the `Q` loop
//! the v0.1 front end speaks; the extended protocol is a v0.2 concern.
//!
//! **Isolation breadth** (STL-248): the level is selectable per transaction —
//! `BEGIN ISOLATION LEVEL READ COMMITTED` (or `SET TRANSACTION ISOLATION LEVEL …`
//! inside the block) re-pins a fresh snapshot per statement, so a transaction
//! observes commits made after it began; `SERIALIZABLE` (SSI, a v0.7 opt-in) is
//! rejected rather than silently downgraded. The default stays `REPEATABLE READ`
//! (snapshot isolation), exercised by the stable-snapshot test above.

use std::sync::{Arc, Mutex};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// Every `balance` cell of a `SELECT balance …` reply, in row order.
fn balances(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                Some(row.get("balance").expect("balance column").to_owned())
            }
            _ => None,
        })
        .collect()
}

/// Every `id` cell of a `SELECT id …` reply, **sorted** as owned strings. The
/// query carries no `ORDER BY` (and the v0.1 scan does not order rows), so the
/// values are sorted here to make the assertions independent of scan/physical
/// layout order.
fn ids(messages: &[SimpleQueryMessage]) -> Vec<String> {
    let mut ids: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get("id").expect("id column").to_owned()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

/// The SQLSTATE of a failed `simple_query`, for the savepoint error-path asserts.
fn sqlstate(err: &tokio_postgres::Error) -> Option<&str> {
    err.code().map(tokio_postgres::error::SqlState::code)
}

/// Connect a fresh client to `addr` and `CREATE TABLE account`, returning the
/// client and its connection driver task. Shared setup for the savepoint tests.
async fn account_client(
    addr: std::net::SocketAddr,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
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
    (client, driver)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_is_atomic_and_rollback_discards() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
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

    // --- COMMIT path: two inserts inside one transaction land together. ----
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert 1");
    client
        .simple_query("INSERT INTO account VALUES (2, 200)")
        .await
        .expect("insert 2");

    // Before COMMIT the rows are still only buffered (nothing has reached storage),
    // but the transaction reads *its own* buffered writes overlaid on its pinned
    // snapshot — read-your-own-writes (STL-203). (That other connections still see
    // nothing until COMMIT is the snapshot isolation asserted in
    // `a_transaction_reads_a_stable_snapshot_over_the_wire`.)
    let mid_txn = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select mid-transaction");
    assert_eq!(
        ids(&mid_txn),
        vec!["1", "2"],
        "the transaction reads its own buffered inserts"
    );

    client.simple_query("COMMIT").await.expect("commit");

    let after_commit = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after commit");
    assert_eq!(ids(&after_commit), vec!["1", "2"], "both inserts committed");

    // --- ROLLBACK path: a buffered insert is discarded. --------------------
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO account VALUES (3, 300)")
        .await
        .expect("insert 3");
    client.simple_query("ROLLBACK").await.expect("rollback");

    let after_rollback = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after rollback");
    assert_eq!(
        ids(&after_rollback),
        vec!["1", "2"],
        "the rolled-back insert never applied; only the committed rows remain"
    );

    drop(client);
    let _ = driver.await;
}

/// A whole transaction in a single batched simple-query message — `BEGIN`, two
/// writes, and `COMMIT` arrive together and still commit atomically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batched_transaction_commits_atomically() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    client
        .batch_execute(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;\
             BEGIN;\
             INSERT INTO account VALUES (1, 100);\
             INSERT INTO account VALUES (2, 200);\
             COMMIT;",
        )
        .await
        .expect("batched transaction");

    let rows = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select");
    assert_eq!(
        ids(&rows),
        vec!["1", "2"],
        "the batched COMMIT applied both"
    );

    drop(client);
    let _ = driver.await;
}

/// DDL inside a transaction over the wire (STL-175 regression guard): a
/// `BEGIN; CREATE TABLE …; INSERT …; COMMIT` resolves the just-created table for
/// the buffered `INSERT`. DDL inside a block auto-commits and advances the pinned
/// snapshot, so binding the `INSERT` against it sees the new table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_then_dml_inside_one_transaction_over_the_wire() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING")
        .await
        .expect("create inside the transaction");
    client
        .simple_query("INSERT INTO t VALUES (1, 100)")
        .await
        .expect("insert resolves the table created in the same block");
    client.simple_query("COMMIT").await.expect("commit");

    let rows = client
        .simple_query("SELECT balance FROM t")
        .await
        .expect("select");
    assert_eq!(
        balances(&rows),
        vec!["100"],
        "the in-transaction CREATE + buffered INSERT both took effect"
    );

    drop(client);
    let _ = driver.await;
}

/// Snapshot isolation over the wire (STL-175): a transaction reads one consistent
/// snapshot for its whole life. `a` opens a transaction, pinning a snapshot that
/// sees `balance = 100`; `b` then auto-commits `balance = 200`; `a`'s in-transaction
/// `SELECT` still reads `100`. After `a` ends its transaction, it reads the latest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_reads_a_stable_snapshot_over_the_wire() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect a");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect b");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("seed 100");

    // `a` pins its snapshot at BEGIN (it sees balance = 100).
    a.simple_query("BEGIN").await.expect("a begin");

    // `b` auto-commits a newer value on another connection.
    b.simple_query("UPDATE account SET balance = 200 WHERE id = 1")
        .await
        .expect("b update");

    // `a` still reads its pinned snapshot, not `b`'s commit.
    let mid = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads in-transaction");
    assert_eq!(
        balances(&mid),
        vec!["100"],
        "the transaction reads its pinned snapshot, not the concurrent commit"
    );

    a.simple_query("COMMIT").await.expect("a commit");

    // Outside the transaction `a` is its own snapshot and sees the latest value.
    let after = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads after commit");
    assert_eq!(
        balances(&after),
        vec!["200"],
        "after the transaction ends, the next statement sees the latest committed state"
    );

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

/// First-committer-wins write-write conflict over the wire (STL-175): two
/// transactions pin the same snapshot and both write `id = 1`; the first to COMMIT
/// wins, and the second's COMMIT is a **retryable** serialization failure
/// (SQLSTATE `40001`) — the signal a client uses to retry the whole transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_write_conflict_surfaces_a_retryable_error() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect a");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect b");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("seed");

    // Both transactions begin (pinning the same snapshot) before either commits,
    // and both stage a write to id = 1.
    a.simple_query("BEGIN").await.expect("a begin");
    a.simple_query("UPDATE account SET balance = 200 WHERE id = 1")
        .await
        .expect("a update");
    b.simple_query("BEGIN").await.expect("b begin");
    b.simple_query("UPDATE account SET balance = 300 WHERE id = 1")
        .await
        .expect("b update");

    // First committer wins.
    a.simple_query("COMMIT").await.expect("a commits");

    // The loser's COMMIT is a retryable serialization failure (40001).
    let err = b
        .simple_query("COMMIT")
        .await
        .expect_err("b's commit must conflict");
    assert_eq!(
        err.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "a write-write conflict maps to 40001 (serialization_failure), which clients retry: {err}"
    );

    // The winner's value is what persisted; the loser touched nothing.
    let rows = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("select");
    assert_eq!(
        balances(&rows),
        vec!["200"],
        "first committer's write is the one that persisted"
    );

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_to_savepoint_undoes_only_later_writes() {
    // The DoD of STL-176: ROLLBACK TO undoes only the writes staged after the
    // savepoint; the pre-savepoint write survives, and a statement issued after
    // the rollback continues in the same transaction and commits.
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, driver) = account_client(addr).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert 1 (before the savepoint)");
    client
        .simple_query("SAVEPOINT sp1")
        .await
        .expect("savepoint");
    client
        .simple_query("INSERT INTO account VALUES (2, 200)")
        .await
        .expect("insert 2 (after the savepoint)");
    client
        .simple_query("INSERT INTO account VALUES (3, 300)")
        .await
        .expect("insert 3 (after the savepoint)");
    client
        .simple_query("ROLLBACK TO SAVEPOINT sp1")
        .await
        .expect("rollback to savepoint");
    // The transaction is still open: a later write joins the survivors.
    client
        .simple_query("INSERT INTO account VALUES (4, 400)")
        .await
        .expect("insert 4 (continues the same transaction)");
    client.simple_query("COMMIT").await.expect("commit");

    let after = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after commit");
    assert_eq!(
        ids(&after),
        vec!["1", "4"],
        "the pre-savepoint insert and the post-rollback insert commit; the two staged \
         after the savepoint are undone"
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_keeps_writes_and_a_nested_rollback_undoes_only_its_own() {
    // RELEASE drops a savepoint but keeps its writes; an enclosing savepoint then
    // still rolls back the lot. BEGIN; 1; SAVEPOINT a; 2; SAVEPOINT b; 3;
    // RELEASE b (keeps 1,2,3); ROLLBACK TO a (drops 2,3); COMMIT → only 1.
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, driver) = account_client(addr).await;

    for sql in [
        "BEGIN",
        "INSERT INTO account VALUES (1, 100)",
        "SAVEPOINT a",
        "INSERT INTO account VALUES (2, 200)",
        "SAVEPOINT b",
        "INSERT INTO account VALUES (3, 300)",
        "RELEASE SAVEPOINT b",
        "ROLLBACK TO SAVEPOINT a",
        "COMMIT",
    ] {
        client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let after = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after commit");
    assert_eq!(
        ids(&after),
        vec!["1"],
        "RELEASE b kept 2 and 3 buffered, but ROLLBACK TO a then discarded them; only 1 commits"
    );

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn savepoint_error_paths_report_postgres_sqlstates() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, driver) = account_client(addr).await;

    // (1) A savepoint statement outside any transaction block — 25P01.
    let err = client
        .simple_query("SAVEPOINT sp1")
        .await
        .expect_err("SAVEPOINT outside a transaction is rejected");
    assert_eq!(sqlstate(&err), Some("25P01"), "no active transaction");

    // (2) ROLLBACK TO a savepoint that does not exist — 3B001 — and it aborts the
    // transaction, so a following statement is refused until the block ends.
    client.simple_query("BEGIN").await.expect("begin");
    let err = client
        .simple_query("ROLLBACK TO SAVEPOINT nope")
        .await
        .expect_err("unknown savepoint is rejected");
    assert_eq!(sqlstate(&err), Some("3B001"), "savepoint does not exist");

    let err = client
        .simple_query("SELECT id FROM account")
        .await
        .expect_err("the transaction is now aborted");
    assert_eq!(
        sqlstate(&err),
        Some("25P02"),
        "commands are ignored until the aborted block ends"
    );

    // ROLLBACK ends the block; the connection is usable again.
    client
        .simple_query("ROLLBACK")
        .await
        .expect("rollback ends the aborted block");
    client
        .simple_query("SELECT id FROM account")
        .await
        .expect("the connection works after the block ends");

    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_to_savepoint_recovers_an_aborted_transaction() {
    // The DoD of STL-205 — Postgres's `in_failed_sql_transaction` escape hatch. An
    // error inside a BEGIN block aborts it, but a ROLLBACK TO a savepoint that
    // predates the error recovers the transaction instead of losing the whole
    // block: the pre-savepoint write survives, the post-savepoint write (and the
    // failed statement's effect) are undone, and the transaction continues to a
    // clean COMMIT.
    //
    //   BEGIN; INSERT 1; SAVEPOINT sp; INSERT 2; <error>;
    //   ROLLBACK TO sp; INSERT 3; COMMIT  →  commits {1, 3}.
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;
    let (client, driver) = account_client(addr).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("insert 1 (before the savepoint)");
    client
        .simple_query("SAVEPOINT sp")
        .await
        .expect("savepoint");
    client
        .simple_query("INSERT INTO account VALUES (2, 200)")
        .await
        .expect("insert 2 (after the savepoint — to be undone by the recovery)");

    // A write against an unknown table errors and aborts the block.
    let err = client
        .simple_query("INSERT INTO nope VALUES (9, 9)")
        .await
        .expect_err("the bad write aborts the transaction");
    assert_eq!(
        sqlstate(&err),
        Some("42P01"),
        "unknown table reported as undefined_table"
    );

    // SAVEPOINT and RELEASE stay refused in the aborted block (Postgres parity) —
    // only ROLLBACK TO can recover it.
    let err = client
        .simple_query("SAVEPOINT sp2")
        .await
        .expect_err("SAVEPOINT is refused in an aborted block");
    assert_eq!(sqlstate(&err), Some("25P02"), "SAVEPOINT while aborted");
    let err = client
        .simple_query("RELEASE SAVEPOINT sp")
        .await
        .expect_err("RELEASE is refused in an aborted block");
    assert_eq!(sqlstate(&err), Some("25P02"), "RELEASE while aborted");

    // ROLLBACK TO the pre-error savepoint recovers the block: it is active again.
    client
        .simple_query("ROLLBACK TO SAVEPOINT sp")
        .await
        .expect("rollback to a pre-error savepoint recovers the aborted transaction");
    client
        .simple_query("INSERT INTO account VALUES (3, 300)")
        .await
        .expect("insert 3 (the recovered transaction continues)");
    client.simple_query("COMMIT").await.expect("commit");

    let after = client
        .simple_query("SELECT id FROM account")
        .await
        .expect("select after commit");
    assert_eq!(
        ids(&after),
        vec!["1", "3"],
        "the pre-savepoint insert and the post-recovery insert commit; the write staged \
         after the savepoint and the failed statement are undone — the error was recovered, \
         not the whole transaction lost"
    );

    drop(client);
    let _ = driver.await;
}

/// READ COMMITTED over the wire (STL-248): `BEGIN ISOLATION LEVEL READ COMMITTED`
/// re-pins a fresh snapshot per statement, so a transaction's successive reads
/// observe a value another connection commits mid-transaction — the statement-level
/// snapshot advance, in deliberate contrast to the REPEATABLE READ default
/// (`a_transaction_reads_a_stable_snapshot_over_the_wire`, where the same
/// interleaving leaves the in-block read unchanged).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_sees_concurrent_commits_mid_transaction() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect a");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect b");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("seed 100");

    // `a` opens a READ COMMITTED block and reads the seeded value.
    a.simple_query("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("a begin read committed");
    let before = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads before the concurrent commit");
    assert_eq!(balances(&before), vec!["100"], "the block first sees 100");

    // `b` auto-commits a newer value on another connection, mid-block.
    b.simple_query("UPDATE account SET balance = 200 WHERE id = 1")
        .await
        .expect("b update");

    // Under READ COMMITTED `a`'s next read re-pins and sees `b`'s commit.
    let after = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads after the concurrent commit");
    assert_eq!(
        balances(&after),
        vec!["200"],
        "READ COMMITTED re-pins per statement: the same transaction observes the concurrent commit"
    );

    a.simple_query("COMMIT").await.expect("a commit");

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

/// `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` selects the level mid-block
/// (STL-248): the same statement-level snapshot advance as `BEGIN ISOLATION LEVEL
/// READ COMMITTED`, reached via the `SET` spelling drivers/ORMs emit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_transaction_isolation_level_switches_a_block_to_read_committed() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (a, a_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect a");
    let a_driver = tokio::spawn(a_conn);
    let (b, b_conn) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect b");
    let b_driver = tokio::spawn(b_conn);

    a.batch_execute(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO account VALUES (1, 100)")
        .await
        .expect("seed 100");

    a.simple_query("BEGIN").await.expect("a begin");
    a.simple_query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("a set read committed");

    b.simple_query("UPDATE account SET balance = 200 WHERE id = 1")
        .await
        .expect("b update");

    let after = a
        .simple_query("SELECT balance FROM account")
        .await
        .expect("a reads after the concurrent commit");
    assert_eq!(
        balances(&after),
        vec!["200"],
        "after SET TRANSACTION ISOLATION LEVEL READ COMMITTED the block re-pins per statement"
    );

    a.simple_query("COMMIT").await.expect("a commit");

    drop(a);
    drop(b);
    let _ = a_driver.await;
    let _ = b_driver.await;
}

/// SERIALIZABLE is not implemented (true SSI is a v0.7 opt-in, ADR-0008):
/// `BEGIN ISOLATION LEVEL SERIALIZABLE` is rejected with feature_not_supported
/// (0A000) rather than silently downgraded to snapshot isolation — an honest
/// refusal, not a false serializability promise (STL-248).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serializable_isolation_is_rejected_not_downgraded() {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect");
    let driver = tokio::spawn(connection);

    let err = client
        .simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect_err("serializable must be rejected, not silently downgraded");
    assert_eq!(
        err.code(),
        Some(&SqlState::FEATURE_NOT_SUPPORTED),
        "serializable isolation reports 0A000 (feature_not_supported): {err}"
    );

    // The connection is still usable after the refusal — a plain BEGIN works.
    client
        .simple_query("BEGIN")
        .await
        .expect("plain begin works");
    client.simple_query("COMMIT").await.expect("commit");

    drop(client);
    let _ = driver.await;
}
