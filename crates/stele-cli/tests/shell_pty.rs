//! Interactive (PTY-backed) `stele shell` regression tests.
//!
//! The scripted tests in `shell_session.rs` pipe stdin, so the shell takes its
//! non-interactive line loop and never drives the rustyline editor at all. The
//! only way to exercise — and regression-guard — interactive input handling is to
//! give the binary a real pseudo-terminal, so `stdin`/`stdout` are both TTYs and
//! `stele shell` enters its REPL.
//!
//! [STL-306]: the REPL reads input through a fixed 1 KiB buffer. rustyline only
//! carries the *leftover* of that buffer across `readline()` calls when its
//! `buffer-redux` feature is on; without it, a block of statements arriving faster
//! than the shell can process them — most visibly a **paste** — is read once,
//! consumed down to its first line, and the rest is silently dropped. The shell
//! then wedges in `read()` waiting for input that is already gone. This test
//! pastes a large block and asserts every statement actually reaches the engine.
#![cfg(unix)]

use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{Server, SharedSession};
use stele_storage::backend::MemDisk;

/// Boot a fresh engine + pgwire server on an ephemeral port (mirrors the helper
/// in `shell_session.rs`; integration-test files do not share a module).
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

/// A live `stele shell` driven over a pseudo-terminal: write keystrokes with
/// [`PtyShell::send`] / [`PtyShell::paste`], read everything the shell has emitted
/// so far with [`PtyShell::snapshot`].
struct PtyShell {
    child: Child,
    /// The master side, for feeding input to the child's stdin.
    input: File,
    /// Everything read off the master so far (filled by a background thread, so
    /// the shell never blocks writing output — a real terminal always drains).
    output: Arc<Mutex<Vec<u8>>>,
}

impl PtyShell {
    /// Spawn the real `stele` binary attached to a fresh PTY, talking to `addr`.
    fn spawn(addr: SocketAddr) -> Self {
        let winsize = nix::pty::Winsize {
            ws_row: 24,
            ws_col: 80,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let pty = nix::pty::openpty(Some(&winsize), None).expect("openpty");
        // The child gets the slave as stdin + stdout + stderr, so both ends are
        // TTYs and `stele shell` runs its interactive rustyline loop.
        let stdin: Stdio = pty.slave.try_clone().expect("dup slave").into();
        let stdout: Stdio = pty.slave.try_clone().expect("dup slave").into();
        let stderr: Stdio = pty.slave.try_clone().expect("dup slave").into();
        // A private HOME keeps the test off the developer's real ~/.stele_history
        // (and its file lock).
        let home = std::env::temp_dir().join(format!("stele-pty-home-{}", std::process::id()));
        std::fs::create_dir_all(&home).ok();
        let child = Command::new(env!("CARGO_BIN_EXE_stele"))
            .args([
                "shell",
                "--no-color",
                "--host",
                "127.0.0.1",
                "--port",
                &addr.port().to_string(),
            ])
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .env("HOME", &home)
            .env("TERM", "xterm")
            .spawn()
            .expect("spawn interactive shell");
        // Drop the test's own handle to the slave so the master observes EOF once
        // the child exits.
        drop(pty.slave);

        let input = File::from(pty.master);
        let mut reader = input.try_clone().expect("clone master for reader");
        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&output);
        std::thread::spawn(move || {
            let mut buf = [0_u8; 4096];
            // A closed master reads as EOF (0) on Linux or EIO on macOS; either
            // ends the drain.
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 {
                    break;
                }
                sink.lock().unwrap().extend_from_slice(&buf[..n]);
            }
        });
        Self {
            child,
            input,
            output,
        }
    }

    /// Type `data` (small enough not to fill the PTY input buffer).
    fn send(&mut self, data: &str) {
        self.input.write_all(data.as_bytes()).expect("write to pty");
    }

    /// Paste a large block: fed from a background thread so that if the shell
    /// wedges mid-block (the bug under test) the writer stalls there instead of
    /// blocking the test's polling — the assertion's deadline still fires.
    fn paste(&self, data: String) {
        let mut writer = self.input.try_clone().expect("clone master for feeder");
        std::thread::spawn(move || {
            let _ = writer.write_all(data.as_bytes());
            let _ = writer.flush();
        });
    }

    /// Everything the shell has emitted so far, lossily decoded (the stream
    /// carries rustyline's cursor escapes; assertions match plain substrings).
    fn snapshot(&self) -> String {
        String::from_utf8_lossy(&self.output.lock().unwrap()).into_owned()
    }

    /// Poll until `pred` holds over the accumulated output. Returns `false` on the
    /// overall `deadline`, or early if the output stops growing for `stall` — a
    /// wedged shell produces nothing further, so there is no point waiting out the
    /// full deadline.
    fn wait_until(&self, deadline: Duration, stall: Duration, pred: impl Fn(&str) -> bool) -> bool {
        let start = Instant::now();
        let mut last_len = 0;
        let mut last_growth = Instant::now();
        loop {
            let snap = self.snapshot();
            if pred(&snap) {
                return true;
            }
            if snap.len() != last_len {
                last_len = snap.len();
                last_growth = Instant::now();
            }
            if start.elapsed() > deadline || last_growth.elapsed() > stall {
                return false;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for PtyShell {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interactive_shell_processes_a_large_pasted_block_without_losing_input() {
    // STL-306: drive a real terminal, paste a block of `N` inserts faster than the
    // shell drains them, and confirm every one reaches the engine. Before the fix
    // (rustyline's `buffer-redux` feature off) the shell read the first line or two
    // and dropped the rest, processing only a handful before wedging in `read()`.
    const N: usize = 300;
    let addr = spawn_server().await;

    tokio::task::spawn_blocking(move || {
        let mut shell = PtyShell::spawn(addr);
        // The banner confirms the REPL is up and connected.
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("Connected to database")),
            "interactive shell never connected:\n{}",
            shell.snapshot()
        );

        // Establish the table first (one statement, reliably processed). Inserts
        // run after it on the same sequential connection regardless of timing.
        shell.send(
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;\n",
        );
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("CREATE TABLE")),
            "CREATE TABLE never completed:\n{}",
            shell.snapshot()
        );

        // The paste: `N` complete statements as one fast block. No bracketed-paste
        // markers — just raw lines, the case rustyline truncates without
        // `buffer-redux`.
        let mut block = String::new();
        for i in 1..=N {
            let _ = writeln!(block, "INSERT INTO account VALUES ({i}, {});", i * 10);
        }
        shell.paste(block);

        // Each accepted insert echoes a single `INSERT 0 1` reply tag (the echoed
        // input lines do not contain that string), so the count is an exact tally
        // of statements that actually reached the engine.
        let all_landed = shell.wait_until(Duration::from_secs(60), Duration::from_secs(12), |o| {
            o.matches("INSERT 0 1").count() >= N
        });
        let snap = shell.snapshot();
        let seen = snap.matches("INSERT 0 1").count();
        assert!(
            all_landed,
            "only {seen}/{N} pasted inserts reached the engine — input was dropped \
             (is rustyline's `buffer-redux` feature enabled?)"
        );

        // And the shell is still alive afterwards: a follow-up query returns.
        shell.send("SELECT count(*) FROM account;\n");
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains(&format!("{N}"))),
            "shell did not answer a query after the paste:\n{}",
            shell.snapshot()
        );
    })
    .await
    .expect("pty shell task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_still_learns_a_table_created_this_session() {
    // The companion change to STL-306 stops refreshing ⇥-completion after *every*
    // statement and does it only after DDL. Guard the behavior that matters: a
    // table created in this session is immediately completable. (The refresh runs
    // synchronously in the loop before the next `readline`, so there is no race —
    // the editor only reads the partial line after completion has been re-read.)
    let addr = spawn_server().await;

    tokio::task::spawn_blocking(move || {
        let mut shell = PtyShell::spawn(addr);
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("Connected to database")),
            "interactive shell never connected:\n{}",
            shell.snapshot()
        );

        // A distinctively named table so the completion can't be a SQL keyword or a
        // column name, and so it cannot already be on the line from anything we typed.
        shell.send(
            "CREATE TABLE zqxwidget (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;\n",
        );
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("CREATE TABLE")),
            "CREATE TABLE never completed:\n{}",
            shell.snapshot()
        );

        // Everything from here is new output; the unique prefix `zqxw` should
        // expand to the freshly-created `zqxwidget` on ⇥ — which can only happen if
        // the DDL refreshed the completer's identifiers.
        let mark = shell.snapshot().len();
        shell.send("SELECT * FROM zqxw\t");
        let completed = shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| {
            o.get(mark..).is_some_and(|tail| tail.contains("zqxwidget"))
        });
        assert!(
            completed,
            "⇥ did not complete `zqxw` to the table created this session — the \
             post-DDL completion refresh regressed:\n{}",
            shell.snapshot().get(mark..).unwrap_or_default()
        );
    })
    .await
    .expect("pty shell task");
}
