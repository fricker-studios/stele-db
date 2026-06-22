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
use stele_pgwire::{AuthMode, Server, SharedSession};
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

/// Boot a SCRAM-required server with `users` pre-created via the real SQL path
/// (STL-296), for the interactive password-prompt test.
async fn spawn_scram_server(users: &[(&str, &str)]) -> SocketAddr {
    let mut engine = SessionEngine::open(MemDisk::new(), SystemClock);
    for (name, password) in users {
        let sql = format!("CREATE USER {name} PASSWORD '{password}'");
        let stmt = &stele_sql::parse(&sql).expect("parse CREATE USER")[0];
        engine.execute(stmt).expect("create user");
    }
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .with_auth(AuthMode::Scram)
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
        Self::spawn_args(addr, &[])
    }

    /// Spawn the real `stele` binary on a fresh PTY with `extra` shell flags
    /// (e.g. `--user`), talking to `addr`.
    fn spawn_args(addr: SocketAddr, extra: &[&str]) -> Self {
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
        // (and its file lock). The ephemeral server port makes it unique per
        // PtyShell, so two PTY tests in the same test binary never share a history
        // file. Fail loudly rather than let a broken temp dir surface later as an
        // inscrutable shell error.
        let home = std::env::temp_dir().join(format!(
            "stele-pty-home-{}-{}",
            std::process::id(),
            addr.port()
        ));
        std::fs::create_dir_all(&home).expect("create scratch HOME");
        let child = Command::new(env!("CARGO_BIN_EXE_stele"))
            .args([
                "shell",
                "--no-color",
                "--host",
                "127.0.0.1",
                "--port",
                &addr.port().to_string(),
            ])
            .args(extra)
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .env("HOME", &home)
            .env("TERM", "xterm")
            // Strip any ambient PGPASSWORD/PGPASSFILE so the no-password path — and
            // thus the interactive prompt under test — is exercised
            // deterministically. The private HOME has no `~/.pgpass`, so with
            // PGPASSFILE cleared the password file never resolves (STL-335).
            .env_remove("PGPASSWORD")
            .env_remove("PGPASSFILE")
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

        // And the shell is still alive afterwards: a follow-up query returns. Look
        // at only the output after this point and for the result's row-count
        // trailer (`(1 row …)`) — a render artifact of a *returned* result, never
        // echoed, so it can't be matched by the echoed query or the pasted inserts
        // (which print `INSERT 0 1`). Matching the bare count value would: `300`
        // already appears in the echoed `(300, 3000)` insert.
        let mark = shell.snapshot().len();
        shell.send("SELECT count(*) FROM account;\n");
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .get(mark..)
                .is_some_and(|tail| tail.contains("(1 row"))),
            "shell did not answer a query after the paste:\n{}",
            shell.snapshot().get(mark..).unwrap_or_default()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interactive_shell_prompts_for_a_scram_password_without_echoing_it() {
    // STL-296: against a `auth = "scram"` server with no PGPASSWORD, an
    // interactive shell prompts for the password, reads it with echo off, and
    // connects. The only way to exercise the no-echo terminal prompt is over a
    // real PTY (the scripted tests cover the PGPASSWORD path).
    let addr = spawn_scram_server(&[("alice", "hunter2pw")]).await;

    tokio::task::spawn_blocking(move || {
        let mut shell = PtyShell::spawn_args(addr, &["--user", "alice"]);

        // The shell asks for a password before it can connect (so no banner yet).
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("Password for user alice")),
            "shell never prompted for a SCRAM password:\n{}",
            shell.snapshot()
        );

        // Type the password + Enter. With echo disabled the slave never reflects
        // these bytes, so they must not appear in the captured stream.
        shell.send("hunter2pw\n");

        // It authenticates and the REPL comes up…
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("Connected to database")),
            "shell did not connect after the password was entered:\n{}",
            shell.snapshot()
        );

        // …and a query runs past authentication. Look only at output after this
        // point for the result trailer (`(1 row …)`), a render artifact never echoed.
        let mark = shell.snapshot().len();
        shell.send("SELECT 1;\n");
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .get(mark..)
                .is_some_and(|tail| tail.contains("(1 row"))),
            "shell did not answer a query after SCRAM auth:\n{}",
            shell.snapshot().get(mark..).unwrap_or_default()
        );

        // The password was never echoed to the terminal — not at the prompt, not
        // anywhere in the session.
        let snap = shell.snapshot();
        assert!(
            !snap.contains("hunter2pw"),
            "the password was echoed to the terminal:\n{snap}"
        );
    })
    .await
    .expect("pty shell task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interactive_shell_reprompts_on_a_wrong_scram_password() {
    // STL-335: a wrong password at the interactive prompt no longer drops the user
    // back to their shell — psql-style, the prompt loops until the right password is
    // entered (or the bounded retry count is hit). Drive a real PTY: type a wrong
    // password first (expect a *second* prompt), then the correct one (expect a
    // connection). The only way to exercise the re-prompt loop is interactively;
    // the scripted path surfaces the error instead.
    let addr = spawn_scram_server(&[("alice", "hunter2pw")]).await;

    tokio::task::spawn_blocking(move || {
        let mut shell = PtyShell::spawn_args(addr, &["--user", "alice"]);

        // First prompt.
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("Password for user alice")),
            "shell never prompted for a SCRAM password:\n{}",
            shell.snapshot()
        );

        // A wrong password: the shell must re-prompt rather than exit. The second
        // prompt is the signal the first attempt was rejected and retried.
        shell.send("wrong-password\n");
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .matches("Password for user alice")
                .count()
                >= 2),
            "shell did not re-prompt after a wrong password:\n{}",
            shell.snapshot()
        );
        // …and it reported *why* before re-prompting (psql prints the rejection).
        assert!(
            shell.snapshot().contains("authentication failed"),
            "the re-prompt should follow a reported authentication failure:\n{}",
            shell.snapshot()
        );

        // The correct password on the re-prompt connects.
        shell.send("hunter2pw\n");
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .contains("Connected to database")),
            "shell did not connect after the correct password on re-prompt:\n{}",
            shell.snapshot()
        );

        // …and a query runs past authentication.
        let mark = shell.snapshot().len();
        shell.send("SELECT 1;\n");
        assert!(
            shell.wait_until(Duration::from_secs(15), Duration::from_secs(12), |o| o
                .get(mark..)
                .is_some_and(|tail| tail.contains("(1 row"))),
            "shell did not answer a query after re-prompt auth:\n{}",
            shell.snapshot().get(mark..).unwrap_or_default()
        );

        // Neither password was ever echoed to the terminal.
        let snap = shell.snapshot();
        assert!(
            !snap.contains("hunter2pw") && !snap.contains("wrong-password"),
            "a password was echoed to the terminal:\n{snap}"
        );
    })
    .await
    .expect("pty shell task");
}
