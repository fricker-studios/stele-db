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
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stele_common::time::SystemClock;
use stele_engine::SessionEngine;
use stele_pgwire::{Server, ServerTls, SharedSession, TlsMode, TlsSettings};
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_stele"))
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

    // The still-stubbed admin tier points at its ticket.
    assert!(
        stdout.contains("NOTICE:  \\status") && stdout.contains("STL-200"),
        "{stdout}"
    );

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
