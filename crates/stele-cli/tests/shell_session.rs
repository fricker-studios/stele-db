//! Scripted `stele shell` sessions against a live in-process engine — the
//! STL-185 Definition of Done.
//!
//! Each test boots a [`SessionEngine`] + pgwire [`Server`] on an ephemeral
//! port, then spawns the **real `stele` binary** (`CARGO_BIN_EXE_stele`) with
//! `shell --host … --port …`, pipes a scripted session into stdin, and asserts
//! on the rendered stdout/stderr. This exercises the whole stack the way a
//! user does: argv → clap → blocking pg-wire client → simple-query loop →
//! table renderer.

use std::io::Write as _;
use std::net::SocketAddr;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{Server, SharedSession};
use stele_storage::backend::MemDisk;

/// Boot a fresh engine + pgwire server on an ephemeral port (STL-152: the
/// socket is bound before the address is returned, so no connect race).
async fn spawn_server() -> SocketAddr {
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

/// Run `stele shell` against `addr`, feed it `script` on stdin, and collect
/// its output. A deadline guards against a hung shell taking CI with it: the
/// child is killed (and the test fails) rather than waiting forever.
fn run_shell(addr: SocketAddr, script: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_stele"))
        .args([
            "shell",
            "--host",
            "127.0.0.1",
            "--port",
            &addr.port().to_string(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn stele shell");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(script.as_bytes())
        .expect("write script");
    // stdin handle dropped above → EOF after the script.

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match child.try_wait().expect("poll shell") {
            Some(_) => break,
            None if Instant::now() > deadline => {
                child.kill().ok();
                // Reap the killed child (dropping a `Child` does not `wait()`)
                // so a timed-out test never leaves a zombie behind.
                child.wait().ok();
                panic!("stele shell did not exit within the deadline");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    // The session output is small (well under pipe-buffer size), so collecting
    // after exit cannot deadlock.
    child.wait_with_output().expect("collect shell output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_session_creates_inserts_selects_and_describes() {
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
INSERT INTO account VALUES (2, 250);
SELECT id, balance
  FROM account;
\\d account
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script))
        .await
        .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stderr.is_empty(), "clean session wrote to stderr: {stderr}");

    // DDL + DML surface their CommandComplete tags.
    assert!(stdout.contains("CREATE TABLE"), "{stdout}");
    assert_eq!(stdout.matches("INSERT 0 1").count(), 2, "{stdout}");

    // The (multi-line) SELECT renders as a psql-style table.
    assert!(stdout.contains(" id | balance "), "{stdout}");
    assert!(stdout.contains("----+---------"), "{stdout}");
    assert!(stdout.contains(" 1  | 100"), "{stdout}");
    assert!(stdout.contains(" 2  | 250"), "{stdout}");
    assert!(stdout.contains("(2 rows)"), "{stdout}");

    // `\d account` resolves through the pg_catalog shim to the live columns.
    assert!(stdout.contains("Table \"public.account\""), "{stdout}");
    assert!(stdout.contains(" Column "), "{stdout}");
    assert!(stdout.contains("id"), "{stdout}");
    assert!(stdout.contains("balance"), "{stdout}");
    assert!(stdout.contains("int4"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_errors_go_to_stderr_and_the_session_continues() {
    let addr = spawn_server().await;
    // No `\q`: the EOF after the last line must also end the shell cleanly.
    let script = "\
SELECT balance FROM nowhere;
\\d missing
SELECT 1;
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script))
        .await
        .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The bad statement is reported on stderr, psql-style…
    assert!(stderr.contains("ERROR"), "{stderr}");
    // …and the missing relation gets the psql wording on stdout.
    assert!(
        stdout.contains("Did not find any relation named \"missing\"."),
        "{stdout}"
    );
    // The session survived both: the final SELECT still ran and rendered.
    assert!(stdout.contains("?column?"), "{stdout}");
    assert!(stdout.contains("(1 row)"), "{stdout}");
}
