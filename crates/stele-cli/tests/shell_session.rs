//! Scripted `stele shell` sessions against a live in-process engine — the
//! STL-185 Definition of Done, extended with the STL-198 design surface.
//!
//! Each test boots a [`SessionEngine`] + pgwire [`Server`] on an ephemeral
//! port, then spawns the **real `stele` binary** (`CARGO_BIN_EXE_stele`) with
//! `shell --host … --port …`, pipes a scripted session into stdin, and asserts
//! on the rendered stdout/stderr. Because stdin is a pipe (not a TTY), the
//! shell suppresses the banner, prompts, and every ANSI escape — these
//! assertions double as the guarantee that scripted output stays byte-clean.

use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{AuthMode, Server, ServerTls, SharedSession, TlsMode, TlsSettings};
use stele_server::admin::http::AdminHttp;
use stele_server::admin::{AdminAuth, AdminService};
use stele_server::ops::{OpsServer, OpsState};
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

/// A self-signed CA + server certificate for `127.0.0.1`, written as PEM under
/// a scratch dir (STL-251). Returns the cert/key paths for the server and the
/// CA path for `--tls verify-full`.
fn mint_tls(test: &str) -> (PathBuf, PathBuf, PathBuf) {
    let ca_key = rcgen::KeyPair::generate().expect("CA key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "stele shell test CA");
    let ca_cert = ca_params.clone().self_signed(&ca_key).expect("CA cert");
    let ca_pem = ca_cert.pem();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let key = rcgen::KeyPair::generate().expect("server key");
    // The shell dials --host 127.0.0.1, so verify-full needs the IP SAN.
    let params =
        rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()]).expect("server params");
    let cert = params.signed_by(&key, &issuer).expect("server cert");

    let dir = std::env::temp_dir().join(format!("stele-shell-tls-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let write = |name: &str, pem: &str| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).expect("write PEM");
        path
    };
    (
        write("server.crt", &cert.pem()),
        write("server.key", &key.serialize_pem()),
        write("ca.crt", &ca_pem),
    )
}

/// Boot a TLS-required engine + pgwire server; returns the address + CA path.
async fn spawn_tls_server(test: &str) -> (SocketAddr, PathBuf) {
    let (cert, key, ca) = mint_tls(test);
    let tls = ServerTls::load(&TlsSettings {
        cert,
        key,
        client_ca: None,
        mode: TlsMode::Required,
    })
    .expect("load TLS material");
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .with_tls(tls)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    (addr, ca)
}

/// Run `stele shell` (plus `extra` flags) against `addr`, feed it `script` on
/// stdin, and collect its output. A deadline guards against a hung shell
/// taking CI with it: the child is killed (and the test fails) rather than
/// waiting forever.
fn run_shell(addr: SocketAddr, script: &str, extra: &[&str]) -> Output {
    run_shell_env(addr, script, extra, &[])
}

/// Like [`run_shell`], but with environment overrides: each `(key, value)` sets
/// the variable, or **removes** it when `value` is `None` — so an ambient
/// `PGPASSWORD` in the developer's shell cannot leak into a test. Drives the
/// SCRAM auth sessions (STL-296).
fn run_shell_env(
    addr: SocketAddr,
    script: &str,
    extra: &[&str],
    env: &[(&str, Option<&str>)],
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stele"));
    command
        .args([
            "shell",
            "--host",
            "127.0.0.1",
            "--port",
            &addr.port().to_string(),
        ])
        .args(extra)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // STL-335: the shell now consults `~/.pgpass`/`$PGPASSFILE` between PGPASSWORD
    // and the prompt. Make every scripted session hermetic — a private empty HOME
    // and no PGPASSFILE — so a developer's real password file cannot leak in and
    // silently authenticate a "no-password" test. The `.pgpass` tests below
    // override these through `env`. (HOME is otherwise unused by a scripted shell —
    // history is interactive-only.)
    let home = Scratch::new("home");
    command.env("HOME", home.path()).env_remove("PGPASSFILE");
    for (key, value) in env {
        match value {
            Some(v) => {
                command.env(key, v);
            }
            None => {
                command.env_remove(key);
            }
        }
    }
    let mut child = command.spawn().expect("spawn stele shell");
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
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stderr.is_empty(), "clean session wrote to stderr: {stderr}");
    // Piped sessions must stay byte-clean: no ANSI escapes anywhere.
    assert!(
        !stdout.contains('\x1b'),
        "escapes in piped output: {stdout}"
    );

    // DDL + DML surface their CommandComplete tags.
    assert!(stdout.contains("CREATE TABLE"), "{stdout}");
    assert_eq!(stdout.matches("INSERT 0 1").count(), 2, "{stdout}");

    // The (multi-line) SELECT renders psql-style; int4 cells right-align.
    assert!(stdout.contains(" id | balance "), "{stdout}");
    assert!(stdout.contains("----+---------"), "{stdout}");
    assert!(stdout.contains("  1 |     100"), "{stdout}");
    assert!(stdout.contains("  2 |     250"), "{stdout}");
    assert!(stdout.contains("(2 rows)"), "{stdout}");

    // `\d account` resolves through the pg_catalog shim to the live columns,
    // and reports the always-on system versioning (architecture §12).
    assert!(stdout.contains("Table \"public.account\""), "{stdout}");
    assert!(stdout.contains(" Column "), "{stdout}");
    assert!(stdout.contains("id"), "{stdout}");
    assert!(stdout.contains("balance"), "{stdout}");
    assert!(stdout.contains("int4"), "{stdout}");
    assert!(stdout.contains("System versioning: ENABLED"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unbounded_select_is_capped_so_a_big_table_cannot_flood_the_shell() {
    // STL-306: a whole-table `SELECT` over the simple-query protocol the shell
    // speaks used to stream every row at once — flooding the terminal (which
    // drains through a tiny pty buffer) until it hung. A bare SELECT now stops at
    // the 1000-row interactive default; an explicit LIMIT still returns exactly
    // what the caller asked for.
    use std::fmt::Write as _;
    let addr = spawn_server().await;
    let mut script = String::from(
        "CREATE TABLE big (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING;\n\
         INSERT INTO big VALUES ",
    );
    for i in 1..=1100 {
        if i > 1 {
            script.push(',');
        }
        let _ = write!(script, "({i},{})", i * 2);
    }
    // A bare read (capped) then a small explicit LIMIT (honored). Output stays
    // well under the stdin/stdout pipe buffer so `run_shell` cannot deadlock.
    script.push_str(";\nSELECT id FROM big;\nSELECT id FROM big LIMIT 5;\n\\q\n");

    let output = tokio::task::spawn_blocking(move || run_shell(addr, &script, &[]))
        .await
        .expect("shell task");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The table really holds 1100 rows…
    assert!(stdout.contains("INSERT 0 1100"), "{stdout}");
    // …but the unqualified SELECT stops at the 1000-row default…
    assert!(
        stdout.contains("(1000 rows)"),
        "bare SELECT capped: {stdout}"
    );
    // …and an explicit LIMIT passes straight through, uncapped.
    assert!(
        stdout.contains("(5 rows)"),
        "explicit LIMIT honored: {stdout}"
    );
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
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The bad statement renders the psql error block on stderr…
    assert!(stderr.contains("ERROR:  "), "{stderr}");
    // …and the missing relation gets the psql wording on stdout.
    assert!(
        stdout.contains("Did not find any relation named \"missing\"."),
        "{stdout}"
    );
    // The session survived both: the final SELECT still ran and rendered.
    assert!(stdout.contains("?column?"), "{stdout}");
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn design_surface_meta_commands_round_trip() {
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
\\dt
\\conninfo
\\l
\\timing
SELECT id, balance FROM account;
\\x
SELECT id, balance FROM account;
\\x
\\json
SELECT id, balance FROM account;
\\json
\\?
UPDATE account SET balance = 250 WHERE id = 1;
\\history account 1
\\timeline account 1
\\lineage account 1
\\status
\\zz
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // \dt — the new pg_catalog table-list shape, end to end.
    assert!(stdout.contains("List of relations"), "{stdout}");
    assert!(
        stdout.contains(" public | account | table | system "),
        "{stdout}"
    );

    // \conninfo and \l reflect the live connection parameters.
    assert!(
        stdout.contains(
            "You are connected to database \"stele\" as user \"stele\" via pg-wire on 127.0.0.1"
        ),
        "{stdout}"
    );
    assert!(stdout.contains("List of databases"), "{stdout}");

    // \timing prints the toggle message and a Time: line after the SELECT.
    assert!(stdout.contains("Timing is on."), "{stdout}");
    assert!(stdout.contains("Time: "), "{stdout}");

    // \x — psql-style expanded records, then back to aligned.
    assert!(stdout.contains("Expanded display is on."), "{stdout}");
    assert!(stdout.contains("-[ RECORD 1 ]"), "{stdout}");
    assert!(stdout.contains("balance | 100"), "{stdout}");
    assert!(stdout.contains("Expanded display is off."), "{stdout}");

    // \json — typed values, NULL-safe, numerics unquoted.
    assert!(stdout.contains("Output format is json."), "{stdout}");
    assert!(stdout.contains("{\"id\": 1, \"balance\": 100}"), "{stdout}");

    // \? lists the whole designed surface, including the future tiers.
    assert!(stdout.contains("Meta-commands"), "{stdout}");
    assert!(stdout.contains("list meta-commands"), "{stdout}");
    assert!(stdout.contains("\\asof <ts|reset>"), "{stdout}");
    assert!(
        stdout.contains("verify the tamper-evident hash chain"),
        "{stdout}"
    );

    // \history — the live version-history surface (STL-199), end to end: two
    // versions of key 1, the current one flagged, with the append-only trailer.
    assert!(
        stdout.contains("Version history — public.account  where id = 1"),
        "{stdout}"
    );
    assert!(
        stdout.contains("INSERT") && stdout.contains("UPDATE"),
        "{stdout}"
    );
    assert!(stdout.contains("● current"), "{stdout}");
    assert!(
        stdout.contains("2 versions retained; nothing was overwritten."),
        "{stdout}"
    );

    // \timeline — the bar chart over the balance measure, current flagged.
    assert!(stdout.contains("Timeline — "), "{stdout}");
    assert!(stdout.contains("public.account.balance"), "{stdout}");
    assert!(stdout.contains("◀ as of now()"), "{stdout}");

    // \lineage — the provenance tree, one branch per version.
    assert!(stdout.contains("Lineage — "), "{stdout}");
    assert!(stdout.contains("balance = 250"), "{stdout}");

    // The admin tier is wired (STL-200): `\status` is dispatched, not stubbed —
    // and with no token configured on this plain server it is refused with the
    // bearer-token hint rather than run.
    assert!(stderr.contains("requires a bearer token"), "{stderr}");

    // Unknown meta-command: the psql error block, on stderr.
    assert!(stderr.contains("ERROR:  invalid command \\zz"), "{stderr}");
    assert!(stderr.contains("SQLSTATE: 42601"), "{stderr}");
    assert!(
        stderr.contains("HINT:  Try \\? for a list of meta-commands."),
        "{stderr}"
    );
}

/// `\audit` verifies the live commit hash chain end to end (STL-302), and
/// `\lineage` now carries the `hash ← prevHash` line. The chain is intact on a
/// clean session, the first version chains from genesis, and a bare `\audit`
/// defaults to the first relation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_verifies_the_commit_chain_and_lineage_shows_hashes() {
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 250 WHERE id = 1;
\\audit account
\\audit
\\lineage account 1
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // \audit — the header, the intact verdict, the chain links, and the genesis
    // anchor at the root of the chain.
    assert!(stdout.contains("Audit — public.account"), "{stdout}");
    assert!(stdout.contains("chain intact"), "{stdout}");
    assert!(stdout.contains("link"), "{stdout}");
    assert!(stdout.contains("genesis"), "{stdout}");
    assert!(stdout.contains("←"), "{stdout}");
    // Two versions of key 1, each its own vN line.
    assert!(stdout.contains("v1") && stdout.contains("v2"), "{stdout}");
    // A bare \audit defaults to the first (only) relation — account audited twice.
    assert_eq!(
        stdout.matches("Audit — public.account").count(),
        2,
        "{stdout}"
    );

    // \lineage now carries the hash ← prevHash chain line (STL-302).
    assert!(stdout.contains("Lineage — "), "{stdout}");
    assert!(stdout.contains("hash "), "{stdout}");

    assert!(!stderr.contains("ERROR"), "no error expected:\n{stderr}");
}

/// The `\asof` time-travel context (STL-199) injects a server-accepted
/// `FOR SYSTEM_TIME AS OF` qualifier into a subsequent bare `SELECT`, then clears
/// it. Uses `now()` so the round-trip is deterministic on the wall clock — the
/// past-time-travel *semantics* are oracled at the engine and pgwire layers; here
/// we prove the shell splices a well-formed qualifier end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asof_context_injects_a_system_time_qualifier() {
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
\\asof now()
SELECT balance FROM account;
\\asof reset
SELECT balance FROM account;
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Setting and clearing both print their context line.
    assert!(
        stdout.contains("Time-travel context set: AS OF now()."),
        "{stdout}"
    );
    assert!(stdout.contains("Time-travel context cleared"), "{stdout}");
    // The time-traveled SELECT and the live one both return the row (the injected
    // qualifier parsed and ran), and no error reached stderr.
    assert_eq!(stdout.matches("100").count(), 2, "{stdout}");
    assert!(!stderr.contains("ERROR"), "no error expected:\n{stderr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn border_styles_and_row_numbers_render_from_flags() {
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
INSERT INTO account VALUES (2, 250);
SELECT id, balance FROM account;
\\q
";
    let output = tokio::task::spawn_blocking(move || {
        run_shell(addr, script, &["--border", "markdown", "--row-numbers"])
    })
    .await
    .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Markdown border + 1-based row numbers, numerics right-aligned.
    assert!(stdout.contains("| # | id | balance |"), "{stdout}");
    assert!(stdout.contains("| - | -- | ------- |"), "{stdout}");
    assert!(stdout.contains("| 1 |  1 |     100 |"), "{stdout}");
    assert!(stdout.contains("| 2 |  2 |     250 |"), "{stdout}");
    assert!(stdout.contains("(2 rows)"), "{stdout}");
}

// ---------------------------------------------------------------------------
// TLS sessions (STL-251)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_require_session_round_trips_encrypted() {
    let (addr, _ca) = spawn_tls_server("require").await;
    let script = "SELECT 1;\n\\q\n";
    let output =
        tokio::task::spawn_blocking(move || run_shell(addr, script, &["--tls", "require"]))
            .await
            .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stderr.is_empty(), "clean session wrote to stderr: {stderr}");
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_select_over_tls_does_not_deadlock() {
    // Regression for the TLS reply-flush deadlock: the server wrote a whole-table
    // SELECT reply into its `tokio_rustls` stream — which buffers plaintext until a
    // flush — and then blocked reading the next message without flushing. So over an
    // encrypted connection the client waited forever for rows that were never pushed
    // to the socket while the server waited for a request that never came. A
    // plaintext socket hid it (writes go straight out), and small replies escaped as
    // TLS records filled, so only a result past a few hundred rows deadlocked. If
    // this regresses, the shell never exits and `run_shell` kills it at the deadline,
    // so `status.success()` is false.
    use std::fmt::Write as _;
    let (addr, _ca) = spawn_tls_server("large-select").await;
    let mut script = String::from(
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;\n\
         INSERT INTO account VALUES ",
    );
    for i in 1..=500 {
        if i > 1 {
            script.push(',');
        }
        let _ = write!(script, "({i},{})", i * 100);
    }
    // The result (~500 short rows) stays well under the stdin/stdout pipe buffer, so
    // `run_shell` itself cannot deadlock — only the TLS reply path under test can.
    script.push_str(";\nSELECT * FROM account;\n\\q\n");

    let output =
        tokio::task::spawn_blocking(move || run_shell(addr, &script, &["--tls", "require"]))
            .await
            .expect("shell task");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "shell hung or errored over TLS:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("INSERT 0 500"), "{stdout}");
    assert!(
        stdout.contains("(500 rows)"),
        "the whole-table SELECT must return every row over TLS:\n{stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_verify_full_checks_the_server_against_the_ca() {
    let (addr, ca) = spawn_tls_server("verify-full").await;
    let ca = ca.to_str().expect("utf-8 path").to_owned();
    let script = "SELECT 1;\n\\q\n";
    let output = tokio::task::spawn_blocking(move || {
        run_shell(addr, script, &["--tls", "verify-full", "--tls-ca", &ca])
    })
    .await
    .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_require_fails_loudly_against_a_plaintext_server() {
    // A server without TLS answers the SSLRequest with `N`; `--tls require`
    // must refuse to continue rather than silently downgrade.
    let addr = spawn_server().await;
    let output =
        tokio::task::spawn_blocking(move || run_shell(addr, "\\q\n", &["--tls", "require"]))
            .await
            .expect("shell task");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "shell must fail when the server refuses required TLS"
    );
    assert!(stderr.contains("refused TLS"), "{stderr}");
}

// ---------------------------------------------------------------------------
// mTLS sessions (STL-292): the shell presents a client certificate
// ---------------------------------------------------------------------------

/// Every PEM path the mTLS round-trip needs: a server cert for `127.0.0.1`
/// (chaining to a server CA the shell trusts via `--tls-ca`) plus a client
/// identity (chaining to a *separate* client CA the server trusts via
/// `[tls] client_ca`) the shell presents with `--tls-cert`/`--tls-key`.
struct MtlsPki {
    server_cert: PathBuf,
    server_key: PathBuf,
    server_ca: PathBuf,
    client_ca: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

/// A self-signed CA: its PEM plus an issuer that signs leaves.
fn mint_ca(cn: &str) -> (String, rcgen::Issuer<'static, rcgen::KeyPair>) {
    let key = rcgen::KeyPair::generate().expect("CA key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    let pem = params.clone().self_signed(&key).expect("CA cert").pem();
    (pem, rcgen::Issuer::new(params, key))
}

/// Mint a full mTLS PKI for one test (two independent CAs) and write every PEM
/// under a scratch dir. The server cert carries the `127.0.0.1` SAN the shell
/// dials, so the same material drives `--tls verify-full` in both directions.
fn mint_mtls(test: &str) -> MtlsPki {
    let (server_ca_pem, server_issuer) = mint_ca("stele shell mtls server CA");
    let (client_ca_pem, client_issuer) = mint_ca("stele shell mtls client CA");

    let server_key = rcgen::KeyPair::generate().expect("server key");
    let server_params =
        rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()]).expect("server params");
    let server_cert = server_params
        .signed_by(&server_key, &server_issuer)
        .expect("server cert");

    let client_key = rcgen::KeyPair::generate().expect("client key");
    let mut client_params =
        rcgen::CertificateParams::new(Vec::<String>::new()).expect("client params");
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "stele-shell-client");
    client_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client_params
        .signed_by(&client_key, &client_issuer)
        .expect("client cert");

    let dir = std::env::temp_dir().join(format!("stele-shell-mtls-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let write = |name: &str, pem: &str| -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).expect("write PEM");
        path
    };
    MtlsPki {
        server_cert: write("server.crt", &server_cert.pem()),
        server_key: write("server.key", &server_key.serialize_pem()),
        server_ca: write("server-ca.crt", &server_ca_pem),
        client_ca: write("client-ca.crt", &client_ca_pem),
        client_cert: write("client.crt", &client_cert.pem()),
        client_key: write("client.key", &client_key.serialize_pem()),
    }
}

/// Boot a TLS-required engine + pgwire server that *demands* a client
/// certificate chaining to `pki.client_ca`.
async fn spawn_mtls_server(pki: &MtlsPki) -> SocketAddr {
    let tls = ServerTls::load(&TlsSettings {
        cert: pki.server_cert.clone(),
        key: pki.server_key.clone(),
        client_ca: Some(pki.client_ca.clone()),
        mode: TlsMode::Required,
    })
    .expect("load mTLS material");
    let session: SharedSession =
        Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .with_tls(tls)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_session_presents_the_client_certificate() {
    // The full mutual handshake: the shell verifies the server (verify-full +
    // --tls-ca) AND presents its own certificate (--tls-cert/--tls-key) to a
    // server that requires one. A query round-trips only if both halves succeed.
    let pki = mint_mtls("present");
    let addr = spawn_mtls_server(&pki).await;
    let server_ca = pki.server_ca.to_str().expect("utf-8 path").to_owned();
    let client_cert = pki.client_cert.to_str().expect("utf-8 path").to_owned();
    let client_key = pki.client_key.to_str().expect("utf-8 path").to_owned();
    let script = "SELECT 1;\n\\q\n";
    let output = tokio::task::spawn_blocking(move || {
        run_shell(
            addr,
            script,
            &[
                "--tls",
                "verify-full",
                "--tls-ca",
                &server_ca,
                "--tls-cert",
                &client_cert,
                "--tls-key",
                &client_key,
            ],
        )
    })
    .await
    .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stderr.is_empty(), "clean session wrote to stderr: {stderr}");
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_server_rejects_a_session_without_a_client_certificate() {
    // Guards the positive test from passing vacuously: the same server, dialed
    // *without* --tls-cert/--tls-key, must fail the handshake — proving it truly
    // requires the client certificate the positive test supplies.
    let pki = mint_mtls("absent");
    let addr = spawn_mtls_server(&pki).await;
    let server_ca = pki.server_ca.to_str().expect("utf-8 path").to_owned();
    let output = tokio::task::spawn_blocking(move || {
        run_shell(
            addr,
            "SELECT 1;\n\\q\n",
            &["--tls", "verify-full", "--tls-ca", &server_ca],
        )
    })
    .await
    .expect("shell task");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a session without a client certificate must fail against an mTLS server"
    );
    assert!(!stderr.is_empty(), "the failure must be reported on stderr");
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256 authentication (STL-296)
// ---------------------------------------------------------------------------

/// Boot a SCRAM-required engine + pgwire server with `users` pre-created through
/// the real SQL path (mirrors the pgwire `scram_wire` test). Returns the address.
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

/// STL-296 Definition of Done: against a `auth = "scram"` server the shell
/// authenticates with the password from `PGPASSWORD` and round-trips a query.
/// This is the env-var path — no terminal needed — so it runs scripted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_session_authenticates_via_pgpassword() {
    let addr = spawn_scram_server(&[("alice", "s3cret")]).await;
    let script = "SELECT 1;\n\\q\n";
    let output = tokio::task::spawn_blocking(move || {
        run_shell_env(
            addr,
            script,
            &["--user", "alice"],
            &[("PGPASSWORD", Some("s3cret"))],
        )
    })
    .await
    .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.is_empty(),
        "a clean SCRAM session wrote to stderr: {stderr}"
    );
    // The query ran past authentication.
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

/// A wrong password is refused: the server fails the proof (SQLSTATE `28P01`) and
/// the shell exits non-zero with an authentication error — not a panic or a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_session_rejects_a_wrong_password() {
    let addr = spawn_scram_server(&[("alice", "s3cret")]).await;
    let output = tokio::task::spawn_blocking(move || {
        run_shell_env(
            addr,
            "SELECT 1;\n\\q\n",
            &["--user", "alice"],
            &[("PGPASSWORD", Some("wrong-password"))],
        )
    })
    .await
    .expect("shell task");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a wrong password must fail the shell"
    );
    assert!(
        stderr.contains("authentication failed"),
        "expected an auth-failed error, got: {stderr}"
    );
}

/// With no password and no terminal to prompt at (scripted), a SCRAM server's
/// request is a clear, actionable failure that points at `PGPASSWORD` — not a
/// silent hang or an empty-password attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_session_without_a_password_points_at_pgpassword() {
    let addr = spawn_scram_server(&[("alice", "s3cret")]).await;
    let output = tokio::task::spawn_blocking(move || {
        run_shell_env(
            addr,
            "SELECT 1;\n\\q\n",
            &["--user", "alice"],
            // Remove any ambient PGPASSWORD so the no-password path is exercised.
            &[("PGPASSWORD", None)],
        )
    })
    .await
    .expect("shell task");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a SCRAM server with no password supplied must fail"
    );
    assert!(
        stderr.contains("PGPASSWORD"),
        "expected guidance to set PGPASSWORD, got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// `~/.pgpass` password file (STL-335)
// ---------------------------------------------------------------------------

/// Write a `.pgpass` file with `contents` and unix mode `mode` under a fresh
/// scratch dir (kept alive by the returned [`Scratch`]); returns its path as a
/// UTF-8 string for `$PGPASSFILE`. The permission bits are the point of the
/// test, so they are set explicitly rather than left to the umask.
#[cfg(unix)]
fn write_pgpass(label: &str, contents: &str, mode: u32) -> (Scratch, String) {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = Scratch::new(label);
    let path = dir.path().join("pgpass");
    std::fs::write(&path, contents).expect("write pgpass");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod pgpass");
    let path = path.to_str().expect("utf-8 path").to_owned();
    (dir, path)
}

/// With no `PGPASSWORD`, the shell reads the SCRAM password from the libpq
/// password file (`$PGPASSFILE`), in psql's resolution slot. A `0600` file whose
/// line matches host/port/database/user authenticates end to end.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_session_authenticates_via_pgpass_file() {
    let addr = spawn_scram_server(&[("alice", "s3cret")]).await;
    // The shell dials --host 127.0.0.1, the database defaults to "stele", and the
    // user is alice — so this is an all-fields-pinned match, port included.
    let line = format!("127.0.0.1:{}:stele:alice:s3cret\n", addr.port());
    let (dir, pgpass) = write_pgpass("pgpass-ok", &line, 0o600);
    let output = tokio::task::spawn_blocking(move || {
        run_shell_env(
            addr,
            "SELECT 1;\n\\q\n",
            &["--user", "alice"],
            &[("PGPASSWORD", None), ("PGPASSFILE", Some(&pgpass))],
        )
    })
    .await
    .expect("shell task");
    // The scratch dir (and its .pgpass) must outlive the child the shell spawns;
    // `dir` already lives to end-of-scope, but drop it explicitly here so the
    // requirement reads at the call site rather than relying on the binding name.
    drop(dir);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.is_empty(),
        "a clean .pgpass session wrote to stderr: {stderr}"
    );
    // The query ran past authentication — the password came from the file.
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

/// libpq behavior: a `.pgpass` with group or world access is ignored — the shell
/// warns and falls through to the no-password path rather than read a secret from
/// a file other users can see. Scripted, that surfaces the "set PGPASSWORD"
/// guidance (proving the exposed file was *not* silently used).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_readable_pgpass_file_is_ignored_with_a_warning() {
    let addr = spawn_scram_server(&[("alice", "s3cret")]).await;
    let line = format!("127.0.0.1:{}:stele:alice:s3cret\n", addr.port());
    // 0640 — group-readable, so too permissive for a password file.
    let (dir, pgpass) = write_pgpass("pgpass-perm", &line, 0o640);
    let output = tokio::task::spawn_blocking(move || {
        run_shell_env(
            addr,
            "SELECT 1;\n\\q\n",
            &["--user", "alice"],
            &[("PGPASSWORD", None), ("PGPASSFILE", Some(&pgpass))],
        )
    })
    .await
    .expect("shell task");
    // Keep the scratch .pgpass alive until the shell process has finished with it.
    drop(dir);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "an ignored .pgpass must not authenticate"
    );
    // The libpq permissions warning…
    assert!(
        stderr.contains("group or world access"),
        "expected a permissions warning, got: {stderr}"
    );
    // …and the fall-through to the no-password guidance (the secret was not used).
    assert!(
        stderr.contains("PGPASSWORD"),
        "expected fall-through to PGPASSWORD guidance, got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// SCRAM-SHA-256-PLUS channel binding over TLS (STL-334)
// ---------------------------------------------------------------------------

/// Boot a TLS-required **and** SCRAM-required server — the STL-334 combination —
/// with `users` pre-created through the SQL path. `mint_tls` mints an
/// ECDSA/SHA-256-signed leaf, so the server advertises `SCRAM-SHA-256-PLUS`
/// alongside plain SCRAM. Returns the address and the CA path for `verify-full`.
async fn spawn_tls_scram_server(test: &str, users: &[(&str, &str)]) -> (SocketAddr, PathBuf) {
    let (cert, key, ca) = mint_tls(test);
    let tls = ServerTls::load(&TlsSettings {
        cert,
        key,
        client_ca: None,
        mode: TlsMode::Required,
    })
    .expect("load TLS material");
    let mut engine = SessionEngine::open(MemDisk::new(), SystemClock);
    for (name, password) in users {
        let sql = format!("CREATE USER {name} PASSWORD '{password}'");
        let stmt = &stele_sql::parse(&sql).expect("parse CREATE USER")[0];
        engine.execute(stmt).expect("create user");
    }
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let bound = Server::new("127.0.0.1:0".parse().unwrap(), session)
        .with_tls(tls)
        .with_auth(AuthMode::Scram)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    (addr, ca)
}

/// STL-334 Definition of Done: over TLS against a PLUS-advertising server the
/// shell authenticates with `SCRAM-SHA-256-PLUS` end to end.
///
/// `\conninfo` reports the mechanism the connection actually negotiated, which is
/// what pins the **PLUS** path rather than a silent fallback. Auth merely
/// *succeeding* would not prove it: the server accepts plain SCRAM with the `n`
/// flag even when it advertises PLUS (it only refuses `y` as a downgrade), so a
/// client that failed to compute the channel binding and fell back to plain
/// `SCRAM-SHA-256` over TLS would still connect. By asserting `\conninfo` shows
/// `SCRAM-SHA-256-PLUS` we require the channel-bound mechanism specifically — and
/// because the server validates `c=` against its own certificate, that mechanism
/// only succeeds when the shell computed the binding from the certificate the
/// handshake actually presented. Driven over `verify-full` so the whole
/// handshake → channel-binding → SCRAM path runs end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_plus_session_authenticates_over_tls() {
    let (addr, ca) = spawn_tls_scram_server("scram-plus", &[("alice", "s3cret")]).await;
    let ca = ca.to_str().expect("utf-8 path").to_owned();
    let script = "\\conninfo\nSELECT 1;\n\\q\n";
    let output = tokio::task::spawn_blocking(move || {
        run_shell_env(
            addr,
            script,
            &["--user", "alice", "--tls", "verify-full", "--tls-ca", &ca],
            &[("PGPASSWORD", Some("s3cret"))],
        )
    })
    .await
    .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "SCRAM-SHA-256-PLUS over TLS must authenticate:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.is_empty(),
        "a clean SCRAM-PLUS session wrote to stderr: {stderr}"
    );
    // The pin: the shell negotiated channel binding, not a silent plain-`n`
    // fallback (which the server would also accept over TLS).
    assert!(
        stdout.contains("Authenticated with SCRAM-SHA-256-PLUS"),
        "the shell must negotiate SCRAM-SHA-256-PLUS over TLS, not plain SCRAM:\n{stdout}"
    );
    assert!(
        stdout.contains("channel binding"),
        "conninfo should note the channel binding is in force:\n{stdout}"
    );
    // The query also ran past authentication over the channel-bound connection.
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

/// `\segments` (STL-301) renders the columnar segment + zone-map table end to
/// end: a sealed segment (after `FLUSH`) plus the resident hot tier, the key zone
/// over the flushed range, and the inspect-segment trailer. A bare `\segments`
/// with no table is a usage error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segments_introspection_renders_sealed_and_hot() {
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
INSERT INTO account VALUES (2, 200);
FLUSH;
INSERT INTO account VALUES (3, 300);
\\segments account
\\segments
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The titled table with both tiers: one sealed segment, one hot.
    assert!(stdout.contains("Segments — public.account"), "{stdout}");
    assert!(stdout.contains("sealed"), "{stdout}");
    assert!(stdout.contains("hot"), "{stdout}");
    // The zone-map cell over the key column, spanning the two flushed keys.
    assert!(stdout.contains("id ∈ [1, 2]"), "{stdout}");
    // The sealed segment has an on-disk size; the inspect-segment trailer points
    // at its footer.
    assert!(stdout.contains("KB"), "{stdout}");
    assert!(
        stdout.contains("stele admin inspect-segment seg-"),
        "{stdout}"
    );

    // A bare \segments (no table) is a usage error on stderr.
    assert!(stderr.contains("\\segments needs a table"), "{stderr}");
}

// ---------------------------------------------------------------------------
// Admin / control-plane tier ([STL-200])
//
// These boot the engine behind BOTH the pg-wire server (SQL + the temporal tier)
// AND the ops listener's HTTP/JSON admin gateway (STL-254), then drive the real
// `stele` binary against both — `--port` for pg-wire, `--admin-port` +
// `--admin-token` for the control plane.
// ---------------------------------------------------------------------------

/// The token the admin gateway is configured with for these tests.
const ADMIN_TOKEN: &str = "test-admin-token";

/// A live server exposing both surfaces over the one engine.
struct AdminServer {
    /// The pg-wire listen address.
    pg: SocketAddr,
    /// The ops listener address (the admin HTTP/JSON gateway shares it).
    ops: SocketAddr,
    /// Holds the engine's data + backup scratch dir alive for the test.
    scratch: Scratch,
}

/// Boot one engine shared by the pg-wire server and the admin HTTP/JSON gateway
/// (the same wiring `stele_server::run` does), each on its own ephemeral port.
async fn spawn_admin_server(label: &str) -> AdminServer {
    let scratch = Scratch::new(label);
    // The engine handle is shared two ways, exactly as the server does it: a typed
    // handle for the admin core, and the same handle coerced to the pg-wire /
    // ops `SharedSession` trait object.
    let engine = Arc::new(Mutex::new(SessionEngine::open(MemDisk::new(), SystemClock)));
    // The concrete handle coerces to the pg-wire / ops `SharedSession` trait object
    // (a method-call clone so the unsized coercion lands at the binding).
    let session: SharedSession = engine.clone();

    let bound = Server::new("127.0.0.1:0".parse().unwrap(), Arc::clone(&session))
        .bind()
        .await
        .expect("bind pg-wire port");
    let pg = bound.local_addr();
    tokio::spawn(bound.serve());

    // The ops listener with the admin gateway mounted, token-gated.
    let auth = Arc::new(AdminAuth::new(vec![ADMIN_TOKEN.to_owned()]));
    let core = AdminService::new(Arc::clone(&engine));
    let ops_state = Arc::new(OpsState::new());
    ops_state.set_ready(Arc::clone(&session));
    ops_state.set_admin(Arc::new(AdminHttp::new(core, auth)));
    let ops = OpsServer::new("127.0.0.1:0".parse().unwrap(), Arc::clone(&ops_state))
        .bind()
        .await
        .expect("bind ops port");
    let ops_addr = ops.local_addr();
    tokio::spawn(ops.serve());

    AdminServer {
        pg,
        ops: ops_addr,
        scratch,
    }
}

/// Run `stele shell` wired to both surfaces (`pg` for pg-wire, the admin flags
/// for the control plane on `ops_port`), feeding it `script`, off the async
/// runtime so the blocking poll loop never starves the server tasks.
async fn run_admin_shell(pg: SocketAddr, ops_port: u16, script: String) -> Output {
    let ops_port = ops_port.to_string();
    tokio::task::spawn_blocking(move || {
        run_shell(
            pg,
            &script,
            &["--admin-port", &ops_port, "--admin-token", ADMIN_TOKEN],
        )
    })
    .await
    .expect("shell task")
}

/// `\status` / `\backup` / `\restore` / `\pitr` end to end against the admin API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_tier_status_backup_restore_and_pitr() {
    let server = spawn_admin_server("admin-tier").await;
    let backup_dir = server.scratch.path().join("snap1");
    let backup_arg = backup_dir.display().to_string();
    let script = format!(
        "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 200 WHERE id = 1;
\\status
\\backup --to {backup_arg}
\\restore {backup_arg}
\\pitr now() account 1
\\pitr now() account 999
\\q
"
    );
    let output = run_admin_shell(server.pg, server.ops.port(), script).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // \status — the control-plane header, real counts, a healthy verdict.
    assert!(stdout.contains("Engine status"), "{stdout}");
    assert!(stdout.contains("control-plane"), "{stdout}");
    assert!(stdout.contains("● healthy"), "{stdout}");
    // The kv label is column-padded, so match the value side: one relation.
    assert!(stdout.contains("1 · segments"), "{stdout}");
    assert!(stdout.contains("account (2 cols"), "{stdout}");

    // \backup — a manifest summary and the shipped verdict.
    assert!(stdout.contains("Backup"), "{stdout}");
    assert!(stdout.contains("commit head"), "{stdout}");
    assert!(stdout.contains("manifest"), "{stdout}");
    assert!(
        stdout.contains(&format!("backup written to {backup_arg}")),
        "{stdout}"
    );

    // \restore — the dry-run validation of the backup just taken.
    assert!(stdout.contains("Restore"), "{stdout}");
    assert!(stdout.contains("sha256 verified ✓"), "{stdout}");
    assert!(stdout.contains("would restore"), "{stdout}");
    assert!(stdout.contains("stele restore --from"), "{stdout}");

    // \pitr — the temporal-correctness hook: the AS OF value at the target is the
    // current committed version (200, not the superseded 100), and it matches the
    // append-only history.
    assert!(stdout.contains("Point-in-time recovery"), "{stdout}");
    assert!(stdout.contains("balance = 200"), "{stdout}");
    assert!(
        stdout.contains("FOR SYSTEM_TIME AS OF matches a committed version"),
        "{stdout}"
    );
    // A key that never existed → a consistent absence.
    assert!(stdout.contains("account 999 = ∅"), "{stdout}");

    // The whole admin tier ran without a single SQL/transport error.
    assert!(stderr.is_empty(), "admin tier wrote to stderr: {stderr}");
}

/// `\inspect-segment` renders a real footer summary for a sealed segment, and a
/// clear not-found error for a bogus id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_inspect_segment_renders_footer() {
    let server = spawn_admin_server("admin-inspect").await;

    // First session: build a sealed segment and read its id off the \segments
    // trailer (the engine assigns it, so the test does not hard-code the format).
    let setup = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
INSERT INTO account VALUES (2, 200);
FLUSH;
INSERT INTO account VALUES (3, 300);
\\segments account
\\q
";
    let seg_out = run_admin_shell(server.pg, server.ops.port(), setup.to_owned()).await;
    let seg_stdout = String::from_utf8_lossy(&seg_out.stdout);
    assert!(seg_out.status.success(), "{seg_stdout}");
    let seg_id = seg_stdout
        .lines()
        .find_map(|line| line.split("inspect-segment ").nth(1))
        .map(str::trim)
        .expect("a sealed segment id in the \\segments trailer")
        .to_owned();
    assert!(
        seg_id.starts_with("seg-"),
        "unexpected segment id: {seg_id}"
    );

    // Second session: inspect that segment, then a bogus one.
    let script = format!(
        "\
\\inspect-segment {seg_id}
\\inspect-segment account no-such-seg
\\q
"
    );
    let output = run_admin_shell(server.pg, server.ops.port(), script).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The footer summary from real metadata: state, the sealed two-row range, the
    // business-key zone, and a size. No fabricated per-column statistics.
    assert!(
        stdout.contains(&format!("Segment {seg_id} — public.account")),
        "{stdout}"
    );
    assert!(stdout.contains("sealed (immutable)"), "{stdout}");
    assert!(stdout.contains("rows 2"), "{stdout}");
    assert!(stdout.contains("id ∈ [1, 2]"), "{stdout}");
    assert!(stdout.contains("KB"), "{stdout}");

    // The bogus id is a clear not-found error on stderr.
    assert!(stderr.contains("not found in account"), "{stderr}");
}

/// Without a token the admin tier is refused locally — no round-trip, an
/// actionable hint — while SQL on pg-wire still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_tier_without_a_token_is_refused() {
    // A plain pg-wire server (no admin flags passed to the shell).
    let addr = spawn_server().await;
    let script = "\
\\status
SELECT 1;
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // \status is refused with the token hint…
    assert!(stderr.contains("requires a bearer token"), "{stderr}");
    assert!(stderr.contains("STELE_ADMIN_TOKEN"), "{stderr}");
    // …but the SQL session is unaffected.
    assert!(stdout.contains("(1 row)"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_stats_footer_renders_under_the_flag() {
    // STL-201: `--stats` draws the "see the engine" footer under each result. The
    // server delivers the scan accounting over a NoticeResponse the shell parses;
    // this is the end-to-end proof the channel and the renderer connect.
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
SELECT id, balance FROM account;
\\q
";

    // Compact: a one-liner. The rows live in the in-memory delta (nothing flushed),
    // so the footer says so rather than inventing a segment scan.
    let compact =
        tokio::task::spawn_blocking(move || run_shell(addr, script, &["--stats", "compact"]))
            .await
            .expect("shell task");
    let stdout = String::from_utf8_lossy(&compact.stdout);
    assert!(compact.status.success(), "{stdout}");
    assert!(
        stdout.contains("live @ now()"),
        "compact footer missing: {stdout}"
    );
    assert!(
        stdout.contains("no sealed segments (delta only)"),
        "compact footer should note the delta-only read: {stdout}"
    );

    // Detailed: the multi-line breakdown.
    let detailed =
        tokio::task::spawn_blocking(move || run_shell(addr, script, &["--stats", "detailed"]))
            .await
            .expect("shell task");
    let stdout = String::from_utf8_lossy(&detailed.stdout);
    assert!(detailed.status.success(), "{stdout}");
    assert!(
        stdout.contains("query stats"),
        "detailed header missing: {stdout}"
    );
    assert!(stdout.contains("rows returned"), "{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_query_stats_footer_by_default_when_scripted() {
    // A scripted (piped) session defaults `--stats` off, so captured output stays
    // byte-clean — the footer never appears unless asked for ([STL-201]).
    let addr = spawn_server().await;
    let script = "\
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
SELECT id, balance FROM account;
\\q
";
    let output = tokio::task::spawn_blocking(move || run_shell(addr, script, &[]))
        .await
        .expect("shell task");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    assert!(
        !stdout.contains("live @ now()") && !stdout.contains("query stats"),
        "no footer should appear by default in a scripted session: {stdout}"
    );
}

/// A unique scratch directory under the OS temp dir, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("stele-cli-{label}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
