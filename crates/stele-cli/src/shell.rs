//! The `stele shell` REPL — STL-185 mechanics wearing the STL-198 design:
//! Datum-brand ANSI theming, the prototype's banner/prompt/meta surface, four
//! table styles, `\x` / `\json` modes, `\timing`, and rustyline editing.
//!
//! Two input paths share one statement handler:
//!
//! * **Interactive** (stdin is a TTY): rustyline line editing — ↑/↓ history
//!   (100 entries, consecutive dups collapsed, persisted to `~/.stele_history`),
//!   ⇥ completion of meta-commands / SQL keywords / live table & column names,
//!   live syntax highlighting, ⌃L clear, ⌃C cancels the buffered statement, ⌃D
//!   quits.
//! * **Scripted** (piped): the plain `BufRead` loop. No prompts, no banner, no
//!   escapes — output stays byte-clean for tests and pipelines.
//!
//! SQL errors render as the psql-style `ERROR:` / `SQLSTATE:` / `HINT:` block
//! on stderr and the session continues; transport errors end the shell.

use std::borrow::Cow;
use std::io::{BufRead, IsTerminal as _, Write};
use std::time::Instant;

use anyhow::Context as _;

use crate::client::{Client, Reply, ResultSet, ServerError};
use crate::highlight;
use crate::render::{self, BorderStyle, Column, TableOpts};
use crate::theme::{Role, Seg, Theme, paint_segs};

/// Connection + presentation options for `stele shell` (from clap in `main`).
pub struct Opts {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    pub tls: crate::client::TlsOpts,
    pub border: BorderStyle,
    pub row_nums: bool,
    pub no_color: bool,
}

/// Follow-up ticket the not-yet-wired command tier points at. The version-history
/// temporal commands (`\asof` / `\history` / `\timeline` / `\lineage`, STL-199),
/// segment introspection (`\segments`, STL-301), and the audit chain (`\audit`,
/// STL-302) are all live; the admin tier remains.
const ADMIN_TICKET: &str = "STL-200";

/// Per-session display state (the prototype's toggles), plus the two themes —
/// stdout and stderr detect color independently.
// The toggles are genuinely independent booleans (the prototype's switch set),
// not an enum in disguise.
#[allow(clippy::struct_excessive_bools)]
struct Session {
    theme: Theme,
    err_theme: Theme,
    border: BorderStyle,
    row_nums: bool,
    timing: bool,
    expanded: bool,
    json: bool,
    /// The session `\asof` time-travel context: a `FOR SYSTEM_TIME AS OF` expression
    /// (verbatim, server-resolved) injected into subsequent bare `SELECT`s, or
    /// `None` for the live present ([STL-199]).
    asof: Option<String>,
    interactive: bool,
    host: String,
    port: u16,
    user: String,
    dbname: String,
}

/// What a handled line tells the loop to do next.
enum Flow {
    Continue,
    Quit,
}

/// Connect and run the REPL over stdin/stdout until `\q` or EOF.
///
/// # Errors
/// Fails on connect failure or a mid-session transport failure; SQL errors are
/// reported inline and do not end the session.
pub fn run(opts: &Opts) -> anyhow::Result<()> {
    let mut client = Client::connect(&opts.host, opts.port, &opts.user, &opts.dbname, &opts.tls)
        .context("starting stele shell")?;
    let stdin = std::io::stdin();
    // Interactive needs BOTH ends on a terminal: with stdout redirected
    // (`stele shell > file`, `| tee`) the rustyline editor would spray
    // prompts and refresh escapes into the capture, so that runs scripted.
    let interactive = stdin.is_terminal() && std::io::stdout().is_terminal();
    let mut session = Session {
        theme: if opts.no_color {
            Theme::plain()
        } else {
            Theme::detect(std::io::stdout().is_terminal())
        },
        err_theme: if opts.no_color {
            Theme::plain()
        } else {
            Theme::detect(std::io::stderr().is_terminal())
        },
        border: opts.border,
        row_nums: opts.row_nums,
        // Interactive sessions time every statement out of the box — the
        // round-trip lands in the result trailer (`(N rows · X.XXX ms)`).
        // Scripted runs default off so piped output stays deterministic and
        // byte-clean; `\timing` overrides either way.
        timing: interactive,
        expanded: false,
        json: false,
        asof: None,
        interactive,
        host: opts.host.clone(),
        port: opts.port,
        user: opts.user.clone(),
        dbname: opts.dbname.clone(),
    };

    if interactive {
        let mut out = std::io::stdout().lock();
        banner(&session, &mut out)?;
        drop(out);
        repl_interactive(&mut client, &mut session)
    } else {
        repl_scripted(&mut client, &mut session, stdin.lock(), std::io::stdout())
    }
}

/// The scripted (piped) loop: plain lines in, plain results out.
fn repl_scripted(
    client: &mut Client,
    session: &mut Session,
    input: impl BufRead,
    mut out: impl Write,
) -> anyhow::Result<()> {
    let mut buffer = String::new();
    for line in input.lines() {
        let line = line.context("reading stdin")?;
        if matches!(
            handle_line(client, session, &mut out, &mut buffer, &line)?,
            Flow::Quit
        ) {
            return Ok(());
        }
    }
    Ok(())
}

/// Editor type alias. With the `with-file-history` feature on (STL-221),
/// `DefaultHistory` is rustyline's `FileHistory`, so cross-session persistence —
/// file-locked load + append-on-exit, native `0600` file mode — comes straight
/// from [`Editor::load_history`](rustyline::Editor::load_history) /
/// [`Editor::append_history`](rustyline::Editor::append_history); no hand-rolled
/// serialization.
type ShellEditor = rustyline::Editor<ShellHelper, rustyline::history::DefaultHistory>;

/// The interactive loop: rustyline editing + history + live highlighting.
///
/// History persists across sessions in `~/.stele_history` — a file-locked load
/// before the loop, append-on-exit after it (on `\q`, ⌃D, and even a mid-session
/// transport error), so concurrent shells merge rather than clobber and the file
/// keeps the last 100 (deduped) statements. Both are best-effort: a missing file
/// (first run) or an I/O error never derails the session.
fn repl_interactive(client: &mut Client, session: &mut Session) -> anyhow::Result<()> {
    let config = rustyline::Config::builder()
        .max_history_size(100)
        .context("configuring history size")?
        .history_ignore_dups(true)
        .context("configuring history dedupe")?
        .auto_add_history(false)
        .build();
    let mut rl: ShellEditor =
        rustyline::Editor::with_config(config).context("initializing line editor")?;
    let history_path = history_file_path();
    if let Some(path) = &history_path {
        // First run: no file yet → `load_history` errs with NotFound, ignored.
        let _ = rl.load_history(path);
    }
    rl.set_helper(Some(ShellHelper {
        theme: session.theme,
        // ⇥ completion starts knowing the catalog as it stands at connect; the
        // loop refreshes it after each statement.
        identifiers: fetch_identifiers(client).unwrap_or_default(),
    }));

    let outcome = interactive_loop(client, session, &mut rl);

    if let Some(path) = &history_path {
        // Append this session's new entries under a file lock, so a concurrent
        // shell exiting at the same time keeps its own entries too (STL-221).
        let _ = rl.append_history(path);
    }
    outcome
}

/// The body of [`repl_interactive`], split out so history is saved on every
/// exit path (the `?`/`return` sites below all unwind back through the caller).
fn interactive_loop(
    client: &mut Client,
    session: &mut Session,
    rl: &mut ShellEditor,
) -> anyhow::Result<()> {
    use rustyline::error::ReadlineError;

    let mut out = std::io::stdout();
    let mut buffer = String::new();
    // Raw lines of the statement being assembled — recorded into history as
    // ONE entry when the statement sends, so ↑ recalls the whole statement
    // (psql behavior), not its last fragment.
    let mut pending = String::new();
    loop {
        let prompt = prompt_text(client.txn_status(), &buffer);
        match rl.readline(prompt) {
            Ok(line) => {
                let is_meta = buffer.trim().is_empty() && line.trim_start().starts_with('\\');
                if is_meta {
                    let _ = rl.add_history_entry(line.trim());
                } else if !line.trim().is_empty() {
                    if !pending.is_empty() {
                        pending.push('\n');
                    }
                    pending.push_str(&line);
                }
                let flow = handle_line(client, session, &mut out, &mut buffer, &line)?;
                if buffer.trim().is_empty() && !pending.is_empty() {
                    let _ = rl.add_history_entry(pending.as_str());
                    pending.clear();
                    // A statement just ran — CREATE/DROP/ALTER may have moved
                    // the catalog, so re-read the identifiers ⇥ completes
                    // against. Best-effort: a dead connection resurfaces on the
                    // next real query rather than here.
                    if let Ok(identifiers) = fetch_identifiers(client)
                        && let Some(helper) = rl.helper_mut()
                    {
                        helper.identifiers = identifiers;
                    }
                }
                if matches!(flow, Flow::Quit) {
                    return Ok(());
                }
            }
            // ⌃C cancels the in-flight statement buffer, keeps the session.
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                pending.clear();
            }
            // ⌃D at the prompt quits, like psql.
            Err(ReadlineError::Eof) => return Ok(()),
            Err(e) => return Err(e).context("reading input"),
        }
    }
}

/// The plain prompt text for the current buffer/transaction state. The
/// continuation prompt follows the prototype (`stele-# `); the `*`/`!`
/// transaction markers are a deliberate psql-ism the prototype lacks.
const fn prompt_text(txn_status: u8, buffer: &str) -> &'static str {
    if buffer.trim_ascii().is_empty() {
        match txn_status {
            b'T' => "stele*=# ",
            b'E' => "stele!=# ",
            _ => "stele=# ",
        }
    } else {
        "stele-# "
    }
}

/// Feed one input line through meta-command dispatch / statement buffering.
fn handle_line(
    client: &mut Client,
    session: &mut Session,
    out: &mut impl Write,
    buffer: &mut String,
    line: &str,
) -> anyhow::Result<Flow> {
    // Meta-commands are lines of their own, between statements.
    if buffer.trim().is_empty()
        && let Some(meta) = parse_meta(line)
    {
        return dispatch_meta(client, session, out, &meta);
    }
    buffer.push_str(line);
    buffer.push('\n');
    if statement_terminated(buffer) {
        let sql = std::mem::take(buffer);
        run_statement(client, session, sql.trim(), out)?;
    }
    Ok(Flow::Continue)
}

/// Whether the buffer ends in a statement-terminating `;` — quote- and
/// comment-aware, so a `;` at a line break inside a `'…'` literal (or behind
/// `--`) keeps the continuation prompt instead of sending a torn statement.
fn statement_terminated(buffer: &str) -> bool {
    let mut last_significant = None;
    let mut chars = buffer.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // Consume the literal, honoring '' escapes; an unterminated
                // literal swallows the rest (still mid-string → not done).
                loop {
                    match chars.next() {
                        None => return false,
                        Some('\'') => {
                            if chars.peek() == Some(&'\'') {
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        Some(_) => {}
                    }
                }
                last_significant = Some('\'');
            }
            '-' if chars.peek() == Some(&'-') => {
                // Line comment: skip to end of line.
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            c if c.is_whitespace() => {}
            c => last_significant = Some(c),
        }
    }
    last_significant == Some(';')
}

// ---------------------------------------------------------------------------
// Meta-commands
// ---------------------------------------------------------------------------

/// A recognized backslash meta-command (after alias resolution).
#[derive(Debug, PartialEq, Eq)]
enum Meta<'a> {
    Quit,
    Help,
    SqlHelp,
    Describe(Option<&'a str>),
    ListTables,
    ListDbs,
    ConnInfo,
    /// `\timing [on|off]` — bare toggles, an argument sets.
    Timing(Option<&'a str>),
    /// `\x [on|off]`.
    Expanded(Option<&'a str>),
    /// `\json [on|off]`.
    Json(Option<&'a str>),
    /// `\asof <expr…|reset>` — set or clear the session time-travel context. The
    /// argument is the rest of the line (a multi-word `FOR SYSTEM_TIME AS OF`
    /// expression); `None` / `reset` clears it.
    AsOf(Option<&'a str>),
    /// `\history T [pk]` — a row's (or a table's) append-only version timeline.
    History {
        table: &'a str,
        key: Option<&'a str>,
    },
    /// `\timeline T <pk>` — a value across system-time, as a bar chart.
    Timeline {
        table: &'a str,
        key: &'a str,
    },
    /// `\lineage T <pk>` — provenance: which txn wrote each version.
    Lineage {
        table: &'a str,
        key: &'a str,
    },
    /// `\segments T` — columnar segment + zone-map introspection.
    Segments {
        table: &'a str,
    },
    /// `\audit [T]` — verify the tamper-evident commit hash chain; `T` defaults to
    /// the first relation when omitted.
    Audit {
        table: Option<&'a str>,
    },
    Clear,
    Connect,
    /// A designed-but-not-yet-wired command (temporal or admin tier).
    NotYet {
        cmd: &'a str,
        ticket: &'static str,
        why: &'static str,
    },
    /// A recognized command with arguments it cannot honor.
    BadArgs {
        message: String,
        hint: &'static str,
    },
    Unknown(&'a str),
}

/// Resolve a toggle's `[on|off]` argument: bare flips, `on`/`off` set.
fn toggle_value(current: bool, arg: Option<&str>) -> Result<bool, ServerError> {
    match arg {
        None => Ok(!current),
        Some(a) if a.eq_ignore_ascii_case("on") => Ok(true),
        Some(a) if a.eq_ignore_ascii_case("off") => Ok(false),
        Some(other) => Err(usage_error(
            format!("unrecognized value \"{other}\": expected on or off"),
            r"e.g. \timing on",
        )),
    }
}

/// Parse a meta-command line; `None` means the line is SQL.
fn parse_meta(line: &str) -> Option<Meta<'_>> {
    let trimmed = line.trim();
    // `cmd` is the first whitespace-delimited word; `remainder` is everything after
    // it (kept whole for `\asof`, whose argument is a multi-word expression), and
    // `arg` / a second `parts.next()` are its first two tokens (for `\history T pk`).
    let rest = trimmed.strip_prefix('\\')?.trim_start();
    let (cmd, remainder) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(c, r)| (c, r.trim()));
    let mut parts = remainder.split_whitespace();
    let arg = parts.next();
    Some(match cmd {
        "q" => Meta::Quit,
        "?" => Meta::Help,
        "h" | "help" => Meta::SqlHelp,
        "d" => arg.map_or(Meta::ListTables, |name| Meta::Describe(Some(name))),
        // A pattern argument is not supported yet — surface that instead of
        // silently listing everything (the faithful shim is a later ticket).
        "dt" => match arg {
            None => Meta::ListTables,
            Some(_) => Meta::BadArgs {
                message: r"\dt does not support patterns yet".to_owned(),
                hint: r"Bare \dt lists every table.",
            },
        },
        "l" | "list" => Meta::ListDbs,
        "conninfo" => Meta::ConnInfo,
        "timing" => Meta::Timing(arg),
        "x" => Meta::Expanded(arg),
        "json" => Meta::Json(arg),
        "clear" | "c!" => Meta::Clear,
        "c" | "connect" => Meta::Connect,
        // The version-history temporal tier — live (STL-199). `\asof` takes the
        // whole remainder (a `FOR SYSTEM_TIME AS OF` expression); the others a
        // table and an optional / required key.
        "asof" => Meta::AsOf(match remainder {
            "" => None,
            r if r.eq_ignore_ascii_case("reset") => None,
            r => Some(r),
        }),
        "history" => arg.map_or_else(
            || Meta::BadArgs {
                message: r"\history needs a table".to_owned(),
                hint: r"e.g. \history account 1  (omit the key to list every row)",
            },
            |table| Meta::History {
                table,
                key: parts.next(),
            },
        ),
        "timeline" => match (arg, parts.next()) {
            (Some(table), Some(key)) => Meta::Timeline { table, key },
            _ => Meta::BadArgs {
                message: r"\timeline needs a table and a primary key".to_owned(),
                hint: r"e.g. \timeline account 1",
            },
        },
        "lineage" => match (arg, parts.next()) {
            (Some(table), Some(key)) => Meta::Lineage { table, key },
            _ => Meta::BadArgs {
                message: r"\lineage needs a table and a primary key".to_owned(),
                hint: r"e.g. \lineage account 1",
            },
        },
        "segments" => arg.map_or_else(
            || Meta::BadArgs {
                message: r"\segments needs a table".to_owned(),
                hint: r"e.g. \segments account",
            },
            |table| Meta::Segments { table },
        ),
        // `\audit [T]` — the tamper-evident commit hash chain ([STL-302]); the
        // table is optional (defaults to the first relation).
        "audit" => Meta::Audit { table: arg },
        "status" | "backup" | "restore" | "pitr" | "inspect-segment" | "inspect" => Meta::NotYet {
            cmd,
            ticket: ADMIN_TICKET,
            why: "speaks the admin control-plane API (v0.3)",
        },
        _ => Meta::Unknown(trimmed),
    })
}

/// Execute one meta-command.
fn dispatch_meta(
    client: &mut Client,
    session: &mut Session,
    out: &mut impl Write,
    meta: &Meta<'_>,
) -> anyhow::Result<Flow> {
    match meta {
        Meta::Quit => return Ok(Flow::Quit),
        Meta::Help => help(session, out)?,
        Meta::SqlHelp => sql_help(session, out)?,
        Meta::Describe(Some(name)) => describe(client, session, name, out)?,
        Meta::Describe(None) | Meta::ListTables => list_tables(client, session, out)?,
        Meta::ListDbs => list_databases(session, out)?,
        Meta::ConnInfo => conninfo(session, out)?,
        Meta::Timing(arg) => match toggle_value(session.timing, *arg) {
            Err(e) => print_error(session, &e),
            Ok(v) => {
                session.timing = v;
                let msg = if v { "Timing is on." } else { "Timing is off." };
                write_segs(session, out, &[(Role::Mut, msg.to_owned())])?;
            }
        },
        Meta::Expanded(arg) => match toggle_value(session.expanded, *arg) {
            Err(e) => print_error(session, &e),
            Ok(v) => {
                session.expanded = v;
                let msg = if v {
                    "Expanded display is on."
                } else {
                    "Expanded display is off."
                };
                write_segs(session, out, &[(Role::Mut, msg.to_owned())])?;
            }
        },
        Meta::Json(arg) => match toggle_value(session.json, *arg) {
            Err(e) => print_error(session, &e),
            Ok(v) => {
                session.json = v;
                let msg = if v {
                    "Output format is json."
                } else {
                    "Output format is aligned table."
                };
                write_segs(session, out, &[(Role::Mut, msg.to_owned())])?;
            }
        },
        Meta::AsOf(expr) => {
            session.asof = expr.map(str::to_owned);
            let msg = session.asof.as_ref().map_or_else(
                || "Time-travel context cleared — reading the live present.".to_owned(),
                |e| format!("Time-travel context set: AS OF {e}."),
            );
            write_segs(session, out, &[(Role::Mut, msg)])?;
        }
        Meta::History { table, key } => history(client, session, table, *key, out)?,
        Meta::Timeline { table, key } => timeline(client, session, table, key, out)?,
        Meta::Lineage { table, key } => lineage(client, session, table, key, out)?,
        Meta::Segments { table } => segments(client, session, table, out)?,
        Meta::Audit { table } => audit(client, session, *table, out)?,
        Meta::Clear => {
            if session.interactive {
                // Clear screen + scrollback, home the cursor.
                write!(out, "\x1b[2J\x1b[3J\x1b[H").context("writing results")?;
                out.flush().context("flushing results")?;
            }
        }
        Meta::Connect => write_segs(
            session,
            out,
            &[(
                Role::Mut,
                format!(
                    "Only one database in dev mode: \"{}\" on {}:{}.",
                    session.dbname, session.host, session.port
                ),
            )],
        )?,
        Meta::NotYet { cmd, ticket, why } => write_segs(
            session,
            out,
            &[(
                Role::Note,
                format!("NOTICE:  \\{cmd} {why} — coming with {ticket}."),
            )],
        )?,
        Meta::BadArgs { message, hint } => {
            print_error(session, &usage_error(message.clone(), hint));
        }
        Meta::Unknown(cmd) => print_error(
            session,
            &usage_error(
                format!("invalid command {cmd}"),
                r"Try \? for a list of meta-commands.",
            ),
        ),
    }
    Ok(Flow::Continue)
}

/// The `\?` registry — the full designed surface, including the tiers that
/// still point at their tickets, so the design is discoverable.
fn help(session: &Session, out: &mut impl Write) -> anyhow::Result<()> {
    let mut lines: Vec<Vec<Seg>> = vec![vec![(Role::Head, "Meta-commands".to_owned())]];
    let entry = |cmd: &str, desc: &str| -> Vec<Seg> {
        vec![
            (Role::Acc, format!("  {cmd:<19}")),
            (Role::Mut, format!("  {desc}")),
        ]
    };
    let blank = Vec::new;
    for (cmd, desc) in [
        (r"\?", "list meta-commands"),
        (r"\h", "SQL syntax help"),
        (r"\d [name]", "describe a table (or list relations)"),
        (r"\dt", "list tables"),
        (r"\l", "list databases"),
        (r"\conninfo", "current connection"),
    ] {
        lines.push(entry(cmd, desc));
    }
    lines.push(blank());
    for (cmd, desc) in [
        (
            r"\asof <ts|reset>",
            "set a session AS OF (time-travel) context",
        ),
        (r"\history T [pk]", "append-only version timeline of a row"),
        (r"\timeline T <pk>", "a value across system-time"),
        (
            r"\lineage T <pk>",
            "provenance — which txn wrote each version",
        ),
        (r"\audit [T]", "verify the tamper-evident hash chain"),
        (r"\segments T", "columnar segments + zone maps"),
    ] {
        lines.push(entry(cmd, desc));
    }
    lines.push(blank());
    for (cmd, desc) in [
        (r"\status", "engine health  (control-plane)"),
        (
            r"\backup [--to URI]",
            "consistent snapshot backup  (control-plane)",
        ),
        (r"\restore URI", "restore from a backup  (control-plane)"),
        (
            r"\pitr <ts>",
            "point-in-time recovery plan  (control-plane)",
        ),
        (r"\inspect-segment ID", "dump a segment footer + zone maps"),
    ] {
        lines.push(entry(cmd, desc));
    }
    lines.push(blank());
    for (cmd, desc) in [
        (r"\timing", "toggle query timing"),
        (r"\x", "toggle expanded display"),
        (r"\json", "toggle aligned / json output"),
        (r"\clear", "clear the screen  (⌃L)"),
        (r"\q", "quit"),
    ] {
        lines.push(entry(cmd, desc));
    }
    lines.push(blank());
    lines.push(vec![
        (Role::Head, "  SQL".to_owned()),
        (
            Role::Mut,
            "   statements end with ;  ·  time-travel any query with ".to_owned(),
        ),
        (Role::Kw, "FOR SYSTEM_TIME AS OF <ts>".to_owned()),
    ]);
    write_lines(session, out, &lines)
}

/// The `\h` SQL crib — the statement shapes the binder accepts today, plus the
/// thesis line.
fn sql_help(session: &Session, out: &mut impl Write) -> anyhow::Result<()> {
    let lines: Vec<Vec<Seg>> = vec![
        vec![(
            Role::Head,
            "SQL — Stele speaks PostgreSQL over pg-wire".to_owned(),
        )],
        vec![
            (Role::Kw, "  CREATE TABLE".to_owned()),
            (Role::Mut, " t (...) ".to_owned()),
            (Role::Kw, "WITH SYSTEM VERSIONING".to_owned()),
            (Role::Mut, " [, ".to_owned()),
            (Role::Kw, "VALID TIME".to_owned()),
            (Role::Mut, " (vf, vt)]".to_owned()),
            (Role::Dim, ";".to_owned()),
        ],
        vec![
            (Role::Mut, "    column types: ".to_owned()),
            (
                Role::Kw,
                "INT BIGINT TEXT/VARCHAR BOOL TIMESTAMP[TZ] DATE UUID BYTEA".to_owned(),
            ),
        ],
        vec![
            (Role::Kw, "  INSERT".to_owned()),
            (Role::Mut, " INTO t VALUES (...);   ".to_owned()),
            (Role::Kw, "UPDATE".to_owned()),
            (Role::Mut, " t SET c = v WHERE ...;   ".to_owned()),
            (Role::Kw, "DELETE".to_owned()),
            (Role::Mut, " FROM t WHERE ...;".to_owned()),
        ],
        vec![
            (Role::Kw, "  SELECT".to_owned()),
            (Role::Mut, " ... FROM t ".to_owned()),
            (Role::Kw, "FOR SYSTEM_TIME AS OF".to_owned()),
            (Role::Mut, " (now() - interval '1 second')".to_owned()),
            (Role::Dim, ";".to_owned()),
            (Role::Mut, "   (also ".to_owned()),
            (Role::Kw, "FOR VALID_TIME AS OF".to_owned()),
            (Role::Mut, ")".to_owned()),
        ],
        vec![
            (Role::Mut, "    with ".to_owned()),
            (Role::Kw, "GROUP BY".to_owned()),
            (Role::Mut, " + COUNT/SUM/MIN/MAX/AVG, ".to_owned()),
            (Role::Kw, "JOIN".to_owned()),
            (Role::Mut, " ... ON a.x = b.y, ".to_owned()),
            (Role::Kw, "WHERE PERIOD".to_owned()),
            (Role::Mut, "(...) OVERLAPS/CONTAINS/...".to_owned()),
        ],
        vec![
            (Role::Kw, "  BEGIN".to_owned()),
            (Role::Mut, "; ...; ".to_owned()),
            (Role::Kw, "COMMIT".to_owned()),
            (Role::Mut, "/".to_owned()),
            (Role::Kw, "ROLLBACK".to_owned()),
            (Role::Mut, ";   savepoints: ".to_owned()),
            (Role::Kw, "SAVEPOINT".to_owned()),
            (Role::Mut, " s / ROLLBACK TO s / RELEASE s".to_owned()),
        ],
        vec![
            (Role::Mut, "  admin: ".to_owned()),
            (Role::Kw, "CHECKPOINT".to_owned()),
            (Role::Mut, ";   ".to_owned()),
            (Role::Kw, "FLUSH".to_owned()),
            (Role::Mut, ";   ".to_owned()),
            (Role::Kw, "COMPACT".to_owned()),
            (Role::Dim, ";".to_owned()),
        ],
        vec![(
            Role::Dim,
            "  UPDATE and DELETE never destroy data — prior versions stay queryable via AS OF."
                .to_owned(),
        )],
    ];
    write_lines(session, out, &lines)
}

/// `\dt` (and bare `\d`) — list every live relation via the pg_catalog shim's
/// table-list shape (STL-198 server side).
fn list_tables(client: &mut Client, session: &Session, out: &mut impl Write) -> anyhow::Result<()> {
    let replies =
        client.simple_query("SELECT c.relname FROM pg_catalog.pg_class c ORDER BY c.relname")?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(());
    }
    // Resolve the relname column by name from the RowDescription — never by
    // position, so a projection-honoring server (real Postgres, a future
    // faithful shim) fails loudly below instead of listing nothing.
    let names: Vec<String> = match first_result_set(&replies) {
        None => Vec::new(),
        Some(set) => {
            let Some(idx) = set.columns.iter().position(|c| c.name == "relname") else {
                print_error(
                    session,
                    &usage_error(
                        "unexpected table-list reply from server (no relname column)".to_owned(),
                        "stele shell's \\dt speaks the Stele pg_catalog shim.",
                    ),
                );
                return Ok(());
            };
            set.rows
                .iter()
                .filter_map(|row| row.get(idx).cloned().flatten())
                .collect()
        }
    };
    if names.is_empty() {
        return write_segs(
            session,
            out,
            &[(Role::Mut, "No relations found.".to_owned())],
        );
    }
    let columns = [
        text_col("Schema"),
        text_col("Name"),
        text_col("Type"),
        text_col("Versioning"),
    ];
    let rows: Vec<Vec<Option<String>>> = names
        .into_iter()
        .map(|name| {
            vec![
                Some("public".to_owned()),
                Some(name),
                Some("table".to_owned()),
                // System-time versioning is an architectural invariant — every
                // Stele table has it (architecture §12).
                Some("system".to_owned()),
            ]
        })
        .collect();
    let mut lines = vec![vec![(Role::Head, "List of relations".to_owned())]];
    lines.extend(render::table_lines(
        &columns,
        &rows,
        session.table_opts(true),
    ));
    write_lines(session, out, &lines)
}

/// `\d <table>` — the two-query `pg_catalog` introspection sequence
/// (STL-131): `pg_class` by name → synthetic oid, then `pg_attribute` by that
/// oid → one row per column.
fn describe(
    client: &mut Client,
    session: &Session,
    name: &str,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let escaped = name.replace('\'', "''");
    let replies = client.simple_query(&format!(
        "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname = '{escaped}'"
    ))?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(());
    }
    let oid = first_result_set(&replies)
        .and_then(|set| set.rows.first())
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .map(str::to_owned);
    let Some(oid) = oid else {
        return not_found(session, out, name);
    };
    // The shim mints oids as non-negative i32s; anything else means we are not
    // talking to a Stele server, so say so rather than interpolating it back.
    let oid: u32 = oid.parse().context("unexpected relation oid from server")?;

    let replies = client.simple_query(&format!(
        "SELECT a.attname, a.atttypname, a.attnum FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = {oid} AND a.attnum > 0"
    ))?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(());
    }
    let Some(set) = first_result_set(&replies) else {
        return not_found(session, out, name);
    };
    let rows: Vec<Vec<Option<String>>> = set
        .rows
        .iter()
        .map(|row| {
            let cell = |i: usize| row.get(i).cloned().flatten();
            vec![cell(0), cell(1)]
        })
        .collect();

    let mut lines = vec![vec![(Role::Head, format!("Table \"public.{name}\""))]];
    lines.extend(render::table_lines(
        &[text_col("Column"), text_col("Type")],
        &rows,
        session.table_opts(false),
    ));
    lines.push(vec![
        (Role::Mut, "System versioning: ".to_owned()),
        (Role::Ok, "ENABLED".to_owned()),
        (Role::Dim, "  ·  history retained append-only".to_owned()),
    ]);
    write_lines(session, out, &lines)
}

/// `\l` — the single dev database, from the live connection parameters.
fn list_databases(session: &Session, out: &mut impl Write) -> anyhow::Result<()> {
    let columns = [
        text_col("Name"),
        text_col("Host"),
        text_col("Mode"),
        text_col("Owner"),
    ];
    let rows = vec![vec![
        Some(session.dbname.clone()),
        Some(format!("{}:{}", session.host, session.port)),
        Some("dev".to_owned()),
        Some(session.user.clone()),
    ]];
    let mut lines = vec![vec![(Role::Head, "List of databases".to_owned())]];
    lines.extend(render::table_lines(
        &columns,
        &rows,
        session.table_opts(true),
    ));
    write_lines(session, out, &lines)
}

/// `\conninfo` — one segmented status line.
fn conninfo(session: &Session, out: &mut impl Write) -> anyhow::Result<()> {
    write_segs(
        session,
        out,
        &[
            (Role::Mut, "You are connected to database ".to_owned()),
            (Role::Head, format!("\"{}\"", session.dbname)),
            (Role::Mut, " as user ".to_owned()),
            (Role::Head, format!("\"{}\"", session.user)),
            (Role::Mut, " via pg-wire on ".to_owned()),
            (Role::Acc, session.host.clone()),
            (Role::Dim, format!(":{} (dev).", session.port)),
        ],
    )
}

// ---------------------------------------------------------------------------
// Temporal introspection (\asof \history \timeline \lineage) — STL-199
// ---------------------------------------------------------------------------

/// Splice the session `\asof` context into a statement: a bare `SELECT` without
/// its own system-time qualifier gains a trailing `FOR SYSTEM_TIME AS OF <expr>`
/// the server resolves. Everything else — a write, a query that already time-
/// travels, or a multi-statement batch — passes through untouched.
///
/// The clause is appended at the end because Stele's parser **lifts** every
/// `FOR { SYSTEM_TIME | VALID_TIME } AS OF` qualifier off the token stream
/// wherever it sits before handing the rest to `sqlparser` (STL-162), so position
/// does not matter and the appended expression runs cleanly to end-of-statement.
fn apply_asof<'a>(sql: &'a str, asof: Option<&str>) -> Cow<'a, str> {
    let Some(asof) = asof else {
        return Cow::Borrowed(sql);
    };
    let trimmed = sql.trim();
    let had_semi = trimmed.ends_with(';');
    let body = trimmed.trim_end_matches(';').trim_end();
    let lower = body.to_ascii_lowercase();
    // Only a single bare SELECT, and never one that already pins the system axis.
    // A `;` left in the body means a multi-statement batch (or a literal we will
    // not risk mis-splicing) — leave it alone rather than guess where the clause
    // belongs.
    if !lower.starts_with("select") || lower.contains("for system_time as of") || body.contains(';')
    {
        return Cow::Borrowed(sql);
    }
    let rewritten = format!("{body} FOR SYSTEM_TIME AS OF {asof}");
    Cow::Owned(if had_semi {
        format!("{rewritten};")
    } else {
        rewritten
    })
}

/// Whether a `\history` key token is safe to splice verbatim into the
/// introspection query: no `;` (which would start a second statement) and no
/// control characters. A text key the user quotes (`'alice'`) passes; a `;`
/// inside that quoted literal is the one case this conservatively rejects, which
/// is fine for a structured shell command.
fn key_is_safe(key: &str) -> bool {
    !key.contains(';') && !key.contains(char::is_control)
}

/// Run the `stele_history` introspection query for `table` (optionally one `key`)
/// and return its result set, or `None` after handling the empty / error cases:
/// a server error renders the psql block, an empty timeline a "No versions"
/// notice. The shared fetch behind `\history` / `\timeline` / `\lineage`.
fn fetch_history(
    client: &mut Client,
    session: &Session,
    table: &str,
    key: Option<&str>,
    out: &mut impl Write,
) -> anyhow::Result<Option<ResultSet>> {
    // The key rides verbatim as a SQL literal the server folds to the key type
    // (so a text key must be quoted: `\history users 'alice'`). Reject one that
    // could break out of the single statement — a `;` or a control character would
    // turn this structured command into a multi-statement batch — before building
    // the query.
    if let Some(k) = key
        && !key_is_safe(k)
    {
        print_error(
            session,
            &usage_error(
                format!(
                    "invalid key {k:?}: a history key may not contain ';' or control characters"
                ),
                r"e.g. \history account 1  ·  text keys are quoted: \history users 'alice'",
            ),
        );
        return Ok(None);
    }
    // The table name is a string literal (single quotes doubled).
    let table_lit = table.replace('\'', "''");
    let query = key.map_or_else(
        || format!("SELECT * FROM stele_history('{table_lit}')"),
        |k| format!("SELECT * FROM stele_history('{table_lit}', {k})"),
    );
    let replies = client.simple_query(&query)?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(None);
    }
    let set = first_result_set(&replies).cloned().unwrap_or(ResultSet {
        columns: Vec::new(),
        rows: Vec::new(),
    });
    if set.rows.is_empty() {
        let what = key.map_or_else(
            || format!("table {table}"),
            |k| format!("{table} where key = {k}"),
        );
        write_segs(
            session,
            out,
            &[(Role::Mut, format!("No versions for {what}."))],
        )?;
        return Ok(None);
    }
    Ok(Some(set))
}

/// The metadata-prefix width of a `stele_history` reply: `txid, op, sys_from,
/// sys_to, current, principal` precede the table's own columns ([STL-199]).
const HISTORY_META_COLS: usize = 6;

/// The table column to chart / surface in `\timeline` / `\lineage`: the first
/// whose name reads like a measure (`balance` / `amount` / `total` / `value`),
/// else the last — matching the prototype's heuristic. Returns its offset within
/// the value columns and its name. `value_cols` is never empty (every table has
/// at least its key column).
fn measure_column(value_cols: &[Column]) -> (usize, &str) {
    let is_measure = |name: &str| {
        let n = name.to_ascii_lowercase();
        ["balance", "amount", "total", "value"]
            .iter()
            .any(|m| n.contains(m))
    };
    value_cols
        .iter()
        .position(|c| is_measure(&c.name))
        .map_or_else(
            || {
                let last = value_cols.len().saturating_sub(1);
                (last, value_cols[last].name.as_str())
            },
            |i| (i, value_cols[i].name.as_str()),
        )
}

/// The `pk` column's name — the first of the table's own columns (the business
/// key), or a fallback when a reply carries none.
fn key_column_name(value_cols: &[Column]) -> &str {
    value_cols.first().map_or("id", |c| c.name.as_str())
}

/// `\history T [pk]` — the append-only version timeline as a table: `txid`, `op`,
/// the table's value columns, the resolved `sys_period`, and the `state` glyph,
/// oldest first, with the retained-count trailer.
fn history(
    client: &mut Client,
    session: &Session,
    table: &str,
    key: Option<&str>,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let Some(set) = fetch_history(client, session, table, key, out)? else {
        return Ok(());
    };
    let value_cols = set.columns.get(HISTORY_META_COLS..).unwrap_or(&[]);
    let pk_col = key_column_name(value_cols);
    let title = key.map_or_else(
        || format!("Version history — public.{table}"),
        |k| format!("Version history — public.{table}  where {pk_col} = {k}"),
    );

    // Render columns: txid, op, <value columns>, sys_period, state.
    let mut columns = vec![
        Column {
            name: "txid".to_owned(),
            type_oid: 20,
        },
        text_col("op"),
    ];
    columns.extend(value_cols.iter().cloned());
    columns.push(text_col("sys_period"));
    columns.push(text_col("state"));

    let rows: Vec<Vec<Option<String>>> = set
        .rows
        .iter()
        .map(|r| {
            let cell = |i: usize| r.get(i).cloned().flatten();
            let sys_from = r.get(2).and_then(Option::as_deref).unwrap_or("");
            let sys_to = r.get(3).and_then(Option::as_deref);
            let current = r.get(4).and_then(Option::as_deref) == Some("t");
            let period = format!("[{sys_from}, {})", sys_to.unwrap_or("∞"));
            let state = if current { "● current" } else { "superseded" };
            let mut row = vec![cell(0), cell(1)];
            row.extend((HISTORY_META_COLS..set.columns.len()).map(cell));
            row.push(Some(period));
            row.push(Some(state.to_owned()));
            row
        })
        .collect();

    let mut lines = vec![vec![(Role::Head, title)]];
    lines.extend(render::table_lines(
        &columns,
        &rows,
        session.table_opts(false),
    ));
    let n = set.rows.len();
    lines.push(vec![
        (Role::Dim, "append-only — ".to_owned()),
        (
            Role::Mut,
            format!(
                "{n} version{} retained; nothing was overwritten.",
                if n == 1 { "" } else { "s" }
            ),
        ),
    ]);
    write_lines(session, out, &lines)
}

/// `\timeline T <pk>` — a measure column across system-time as a bar chart, one
/// row per version (time · op glyph · value · bar), the current version flagged.
fn timeline(
    client: &mut Client,
    session: &Session,
    table: &str,
    key: &str,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let Some(set) = fetch_history(client, session, table, Some(key), out)? else {
        return Ok(());
    };
    let value_cols = set.columns.get(HISTORY_META_COLS..).unwrap_or(&[]);
    if value_cols.is_empty() {
        return write_segs(
            session,
            out,
            &[(Role::Mut, format!("No value column to chart in {table}."))],
        );
    }
    let (vcol_off, vcol_name) = measure_column(value_cols);
    let vcol_idx = HISTORY_META_COLS + vcol_off;
    let pk_col = key_column_name(value_cols);

    // Parse the measure to scale the bars; a non-numeric measure draws a unit bar.
    let nums: Vec<Option<f64>> = set
        .rows
        .iter()
        .map(|r| {
            r.get(vcol_idx)
                .and_then(Option::as_deref)
                .and_then(|s| s.parse::<f64>().ok())
        })
        .collect();
    let max = nums.iter().flatten().copied().fold(1.0_f64, f64::max);
    const WIDTH: f64 = 26.0;

    let mut lines = vec![vec![
        (Role::Head, "Timeline — ".to_owned()),
        (Role::Acc, format!("public.{table}.{vcol_name}")),
        (
            Role::Dim,
            format!("  where {pk_col} = {key}   (system-time →)"),
        ),
    ]];
    for (i, r) in set.rows.iter().enumerate() {
        let sys_from = r.get(2).and_then(Option::as_deref).unwrap_or("");
        let time = sys_from.get(11..19).unwrap_or(sys_from);
        let op = r.get(1).and_then(Option::as_deref).unwrap_or("");
        let current = r.get(4).and_then(Option::as_deref) == Some("t");
        let value = r.get(vcol_idx).and_then(Option::as_deref).unwrap_or("");
        let glyph = match op {
            "INSERT" => "✚",
            "UPDATE" => "◆",
            _ => "✕",
        };
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "bar length is a small, clamped count of glyphs"
        )]
        let len = nums[i].map_or(1, |v| ((v / max) * WIDTH).round().max(1.0) as usize);
        let accent = if current { Role::Acc } else { Role::Dim };
        let mut segs = vec![
            (Role::Mut, format!("  {time}  ")),
            (accent, glyph.to_owned()),
            (Role::Num, format!(" {value:>6}")),
            (accent, format!("  {}", "▇".repeat(len))),
        ];
        if current {
            segs.push((Role::Ok, "  ◀ as of now()".to_owned()));
        }
        lines.push(segs);
    }
    lines.push(vec![
        (Role::Dim, "  query any point: ".to_owned()),
        (
            Role::Mut,
            format!("SELECT {vcol_name} FROM {table} FOR SYSTEM_TIME AS OF '<ts>' WHERE …"),
        ),
    ]);
    write_lines(session, out, &lines)
}

/// `\lineage T <pk>` — provenance as a tree: each version's `txn` / `op` /
/// instant, then its measure value and the principal that wrote it, then its
/// tamper-evident `hash ← prevHash` commit-chain link ([STL-302]).
fn lineage(
    client: &mut Client,
    session: &Session,
    table: &str,
    key: &str,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let Some(set) = fetch_history(client, session, table, Some(key), out)? else {
        return Ok(());
    };
    // The matching commit-chain hashes for the same key, in the same version order
    // (both replies fold the one timeline), so they zip by index. Fetched quietly:
    // the provenance tree still renders if the audit surface is unavailable — the
    // `hash ← prevHash` line is simply omitted (the key already passed
    // `fetch_history`'s `key_is_safe` gate, so splicing it is safe).
    let audit = client
        .simple_query(&audit_query(table, Some(key)))
        .ok()
        .filter(|replies| first_error(replies).is_none())
        .and_then(|replies| first_result_set(&replies).cloned());

    let value_cols = set.columns.get(HISTORY_META_COLS..).unwrap_or(&[]);
    let (vcol_off, vcol_name) = measure_column(value_cols);
    let vcol_idx = HISTORY_META_COLS + vcol_off;
    let pk_col = key_column_name(value_cols);

    let mut lines = vec![vec![
        (Role::Head, "Lineage — ".to_owned()),
        (Role::Acc, format!("public.{table}  where {pk_col} = {key}")),
    ]];
    let n = set.rows.len();
    for (i, r) in set.rows.iter().enumerate() {
        let last = i + 1 == n;
        let txid = r.first().and_then(Option::as_deref).unwrap_or("?");
        let op = r.get(1).and_then(Option::as_deref).unwrap_or("");
        let sys_from = r.get(2).and_then(Option::as_deref).unwrap_or("");
        let principal = r.get(5).and_then(Option::as_deref).unwrap_or("");
        let value = r.get(vcol_idx).and_then(Option::as_deref).unwrap_or("");
        let op_role = if op == "INSERT" { Role::Ok } else { Role::Acc };
        let trunk = if last { "      " } else { "  │   " };
        lines.push(vec![
            (Role::Div, (if last { "  └ " } else { "  ├ " }).to_owned()),
            (Role::Head, format!("v{}  ", i + 1)),
            (Role::Mut, format!("txn {txid}  ")),
            (op_role, format!("{op:<6}")),
            (Role::Mut, format!("  {sys_from}")),
        ]);
        lines.push(vec![
            (Role::Div, trunk.to_owned()),
            (Role::Text, format!("{vcol_name} = {value}")),
            (Role::Dim, "   by ".to_owned()),
            (Role::Mut, principal.to_owned()),
            (Role::Dim, " via pg-wire".to_owned()),
        ]);
        // The commit-chain link: this version's hash and the predecessor it chains
        // from ([STL-302]). The audit reply's version rows align 1:1 with the
        // history rows; a row with no chain record (a rare unchained commit) shows
        // an em dash rather than a fabricated hash.
        if let Some(a) = audit.as_ref().and_then(|a| a.rows.get(i)) {
            lines.push(vec![
                (Role::Div, trunk.to_owned()),
                (Role::Dim, "hash ".to_owned()),
                (Role::Num, hash_cell(a, 2)),
                (Role::Dim, " ← ".to_owned()),
                (Role::Mut, prev_hash_cell(a, 3)),
            ]);
        }
    }
    write_lines(session, out, &lines)
}

/// The `stele_audit('t'[, key])` introspection query the `\audit` and `\lineage`
/// commands issue — the `\audit` analogue of `fetch_history`'s `stele_history`
/// query ([STL-302]). The table name is a string literal (single quotes doubled);
/// the key rides verbatim, already gated by [`key_is_safe`] at its call sites.
fn audit_query(table: &str, key: Option<&str>) -> String {
    let table_lit = table.replace('\'', "''");
    key.map_or_else(
        || format!("SELECT * FROM stele_audit('{table_lit}')"),
        |k| format!("SELECT * FROM stele_audit('{table_lit}', {k})"),
    )
}

/// A SHA-256 hex digest is 64 chars; the shell shows a stable 12-char prefix —
/// enough to read the chain links at a glance without wrapping the line (the
/// server keeps the full digest). An all-zero digest is the genesis anchor.
fn short_hash(hex: &str) -> String {
    hex.get(..12).unwrap_or(hex).to_owned()
}

/// Whether `hex` is the genesis anchor — an all-zero digest (the chain's root,
/// `Digest::ZERO` server-side).
fn is_genesis(hex: &str) -> bool {
    !hex.is_empty() && hex.bytes().all(|b| b == b'0')
}

/// Render an audit row's `hash` cell at `idx` as a short digest, or an em dash for
/// an unchained version.
fn hash_cell(row: &[Option<String>], idx: usize) -> String {
    row.get(idx)
        .and_then(Option::as_deref)
        .map_or_else(|| "—".to_owned(), short_hash)
}

/// Render an audit row's `prev_hash` cell at `idx`: `genesis` for the anchor, a
/// short digest otherwise, or an em dash for an unchained version.
fn prev_hash_cell(row: &[Option<String>], idx: usize) -> String {
    match row.get(idx).and_then(Option::as_deref) {
        None => "—".to_owned(),
        Some(p) if is_genesis(p) => "genesis".to_owned(),
        Some(p) => short_hash(p),
    }
}

/// Run `stele_audit('t')` for `table` and return its result set, or `None` after
/// handling the error case (a server error renders the psql block). The shared
/// fetch behind `\audit`; `\lineage` reads the same surface quietly inline.
fn fetch_audit(
    client: &mut Client,
    session: &Session,
    table: &str,
    out: &mut impl Write,
) -> anyhow::Result<Option<ResultSet>> {
    let replies = client.simple_query(&audit_query(table, None))?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(None);
    }
    // `stele_audit` always returns at least the verdict row, so an empty set means
    // the server did not recognize the call — surface that rather than a blank audit.
    match first_result_set(&replies) {
        Some(set) if !set.rows.is_empty() => Ok(Some(set.clone())),
        _ => {
            write_segs(
                session,
                out,
                &[(Role::Mut, format!("No audit data for {table}."))],
            )?;
            Ok(None)
        }
    }
}

/// The first relation's name (alphabetical), via the same `pg_catalog` shim `\dt`
/// reads — the default table for a bare `\audit`. `None` (with a notice) when the
/// catalog is empty or the reply is unexpected.
fn first_table(
    client: &mut Client,
    session: &Session,
    out: &mut impl Write,
) -> anyhow::Result<Option<String>> {
    let replies =
        client.simple_query("SELECT c.relname FROM pg_catalog.pg_class c ORDER BY c.relname")?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(None);
    }
    let name = first_result_set(&replies).and_then(|set| {
        let idx = set.columns.iter().position(|c| c.name == "relname")?;
        set.rows
            .iter()
            .find_map(|row| row.get(idx).cloned().flatten())
    });
    if name.is_none() {
        write_segs(
            session,
            out,
            &[(Role::Mut, "No relations to audit.".to_owned())],
        )?;
    }
    Ok(name)
}

/// `\audit [T]` — verify the tamper-evident commit hash chain ([STL-302]): each
/// version of `T` as `vN  op  hash ← prevHash`, then the `✓ chain intact` /
/// `✗ chain broken` verdict from the server's `verify_chain` pass over the durable
/// commit log. `T` defaults to the first relation.
fn audit(
    client: &mut Client,
    session: &Session,
    table: Option<&str>,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let table = match table {
        Some(t) => t.to_owned(),
        None => match first_table(client, session, out)? {
            Some(t) => t,
            None => return Ok(()),
        },
    };
    let Some(set) = fetch_audit(client, session, &table, out)? else {
        return Ok(());
    };
    let cell =
        |r: &[Option<String>], i: usize| r.get(i).and_then(Option::as_deref).map(str::to_owned);

    // The verdict rides every row; read it off the first (always present, even when
    // the timeline is empty).
    let verdict_row = &set.rows[0];
    let chain_ok = cell(verdict_row, 4).as_deref() == Some("t");
    let chain_len = cell(verdict_row, 5).unwrap_or_else(|| "0".to_owned());
    let chain_head = cell(verdict_row, 6).unwrap_or_default();

    let mut lines = vec![vec![
        (Role::Head, "Audit — ".to_owned()),
        (Role::Acc, format!("public.{table}")),
        (
            Role::Dim,
            "   tamper-evident commit hash chain (SHA-256)".to_owned(),
        ),
    ]];

    // Version rows carry a txid; an empty timeline yields only the verdict row.
    let versions: Vec<&Vec<Option<String>>> = set
        .rows
        .iter()
        .filter(|r| r.first().is_some_and(Option::is_some))
        .collect();
    for (i, r) in versions.iter().enumerate() {
        let op = r.get(1).and_then(Option::as_deref).unwrap_or("");
        lines.push(vec![
            (Role::Mut, format!("  {:<4}", format!("v{}", i + 1))),
            (Role::Text, format!("{op:<7}")),
            (Role::Num, hash_cell(r, 2)),
            (Role::Dim, "  ← ".to_owned()),
            (Role::Mut, prev_hash_cell(r, 3)),
        ]);
    }
    if versions.is_empty() {
        lines.push(vec![(
            Role::Mut,
            format!("  no versions in {table} to audit"),
        )]);
    }

    if chain_ok {
        let plural = if chain_len == "1" { "" } else { "s" };
        lines.push(vec![
            (Role::Ok, "  ✓ ".to_owned()),
            (Role::Ok, "chain intact".to_owned()),
            (
                Role::Mut,
                format!(
                    " · {chain_len} link{plural} · head {}",
                    short_hash(&chain_head)
                ),
            ),
        ]);
    } else {
        lines.push(vec![
            (Role::Warn, "  ✗ ".to_owned()),
            (Role::Warn, "chain broken — tampering detected".to_owned()),
        ]);
    }
    write_lines(session, out, &lines)
}

/// `\segments T` — per-table columnar segment + zone-map introspection ([STL-301]),
/// reading the `stele_segments` wire surface: one row per sealed segment (oldest
/// first) then the resident delta (hot) tier, the `hot` row highlighted and an
/// inspect-segment trailer pointing at the newest sealed segment's footer.
fn segments(
    client: &mut Client,
    session: &Session,
    table: &str,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    // The table name is a string literal (single quotes doubled), like `\history`.
    let table_lit = table.replace('\'', "''");
    let replies = client.simple_query(&format!("SELECT * FROM stele_segments('{table_lit}')"))?;
    if let Some(err) = first_error(&replies) {
        print_error(session, err);
        return Ok(());
    }
    let set = first_result_set(&replies).cloned().unwrap_or(ResultSet {
        columns: Vec::new(),
        rows: Vec::new(),
    });
    // No segments and no resident delta — the table is empty.
    if set.rows.is_empty() {
        return write_segs(
            session,
            out,
            &[(Role::Mut, format!("No segments — {table} is empty."))],
        );
    }

    // The wire reply's fixed columns, in order: segment, state, rows, sys_min,
    // sys_max, key_column, key_min, key_max, bytes ([STL-301]).
    let columns = vec![
        text_col("Segment"),
        text_col("State"),
        Column {
            name: "Rows".to_owned(),
            type_oid: 20, // int8 — right-aligned
        },
        text_col("Sys-time range"),
        text_col("Zone map"),
        text_col("Size"),
    ];
    let rows: Vec<Vec<Option<String>>> = set
        .rows
        .iter()
        .map(|r| {
            let cell = |i: usize| r.get(i).and_then(Option::as_deref);
            vec![
                Some(cell(0).unwrap_or("").to_owned()),
                Some(cell(1).unwrap_or("").to_owned()),
                Some(cell(2).unwrap_or("0").to_owned()),
                Some(sys_range(cell(3), cell(4))),
                Some(zone_map_cell(cell(5).unwrap_or("key"), cell(6), cell(7))),
                Some(size_cell(cell(8))),
            ]
        })
        .collect();

    let mut lines = vec![vec![
        (Role::Head, "Segments — ".to_owned()),
        (Role::Acc, format!("public.{table}")),
        (Role::Dim, "   columnar · append-only".to_owned()),
    ]];
    // The hot (un-flushed delta) row is highlighted — the prototype's warn-on-hot.
    lines.extend(render::table_lines_warn(
        &columns,
        &rows,
        session.table_opts(false),
        |r| r.get(1).and_then(Option::as_deref) == Some("hot"),
    ));
    // Trailer: the newest sealed segment, the one with a footer to inspect. A
    // table with only a hot tier has none yet, so the hint is omitted.
    if let Some(last_sealed) = set
        .rows
        .iter()
        .rev()
        .find(|r| r.get(1).and_then(Option::as_deref) == Some("sealed"))
    {
        let id = last_sealed.first().and_then(Option::as_deref).unwrap_or("");
        lines.push(vec![
            (Role::Dim, "  inspect a segment footer: ".to_owned()),
            (Role::Mut, format!("stele admin inspect-segment {id}")),
        ]);
    }
    write_lines(session, out, &lines)
}

/// The `\segments` system-time range cell: each endpoint's time-of-day, the
/// prototype's compact `min … max`.
fn sys_range(min: Option<&str>, max: Option<&str>) -> String {
    format!("{} … {}", time_of_day(min), time_of_day(max))
}

/// The `HH:MM:SS` of a wire `timestamptz` (`YYYY-MM-DD HH:MM:SS[.frac]+00`), or
/// an em dash when absent — the prototype's time-of-day slice, trimming the date,
/// fractional seconds, and zone.
fn time_of_day(ts: Option<&str>) -> String {
    let Some(ts) = ts else {
        return "—".to_owned();
    };
    ts.split_once(' ')
        .map_or(ts, |(_, time)| time)
        .split(['.', '+'])
        .next()
        .unwrap_or(ts)
        .to_owned()
}

/// The `\segments` zone-map cell: `<col> ∈ [<min>, <max>]` over the segment's key
/// column. A missing bound shows an em dash.
fn zone_map_cell(col: &str, min: Option<&str>, max: Option<&str>) -> String {
    format!("{col} ∈ [{}, {}]", min.unwrap_or("—"), max.unwrap_or("—"))
}

/// The `\segments` size cell: kibibytes to one decimal (the prototype's `X.X KB`),
/// or an em dash for the in-memory hot tier (a `NULL` byte size).
fn size_cell(bytes: Option<&str>) -> String {
    bytes
        .and_then(|b| b.parse::<f64>().ok())
        .map_or_else(|| "—".to_owned(), |b| format!("{:.1} KB", b / 1024.0))
}

// ---------------------------------------------------------------------------
// SQL execution + rendering
// ---------------------------------------------------------------------------

/// Send one buffered statement (or batch) and render every reply. With timing
/// on, the round-trip rides the result trailer (`(N rows · X.XXX ms)`) when the
/// batch yields exactly one row set; otherwise — DML/DDL tags, `\json` output,
/// errors, or a multi-statement batch (one measurement cannot be attributed to
/// any single set) — the batch gets one psql-style `Time:` line at the end.
fn run_statement(
    client: &mut Client,
    session: &Session,
    sql: &str,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    // Apply the session `\asof` time-travel context: a bare `SELECT` gains a
    // `FOR SYSTEM_TIME AS OF <expr>` qualifier the server resolves ([STL-199]).
    let sql = apply_asof(sql, session.asof.as_deref());
    let started = Instant::now();
    let replies = client.simple_query(&sql)?;
    let timed = session.timing.then(|| started.elapsed());
    // The whole round-trip is measured once, so the trailer may carry it only
    // when there is exactly one row set to pin it to (and the table/expanded
    // renderers will actually draw a trailer — JSON output has none).
    let sole_row_set = !session.json
        && replies
            .iter()
            .filter(|r| matches!(r, Reply::Rows(_)))
            .count()
            == 1;
    let trailer_time = if sole_row_set { timed } else { None };
    for reply in replies {
        match reply {
            Reply::Rows(set) => {
                let lines = if session.json {
                    render::json_lines(&set.columns, &set.rows)
                } else if session.expanded {
                    render::expanded_lines(&set.columns, &set.rows, trailer_time)
                } else {
                    render::table_lines(&set.columns, &set.rows, session.result_opts(trailer_time))
                };
                write_lines(session, out, &lines)?;
            }
            Reply::Command(tag) => write_segs(session, out, &[(Role::Text, tag)])?,
            Reply::Error(err) => print_error(session, &err),
            Reply::Empty => {}
        }
    }
    if let Some(elapsed) = timed
        && !sole_row_set
    {
        write_segs(
            session,
            out,
            &[(Role::Mut, format!("Time: {}", render::fmt_elapsed(elapsed)))],
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Output plumbing
// ---------------------------------------------------------------------------

/// The startup banner (interactive sessions only) — the prototype's lines with
/// live connection values.
fn banner(session: &Session, out: &mut impl Write) -> anyhow::Result<()> {
    let lines: Vec<Vec<Seg>> = vec![
        vec![(
            Role::Banner,
            format!(
                "stele shell ({}) — reference pg-wire client",
                env!("CARGO_PKG_VERSION")
            ),
        )],
        vec![
            (Role::Mut, "Type ".to_owned()),
            (Role::Acc, r"\?".to_owned()),
            (Role::Mut, " for help · ".to_owned()),
            (Role::Acc, r"\q".to_owned()),
            (Role::Mut, " to quit · ".to_owned()),
            (Role::Acc, "↑↓".to_owned()),
            (Role::Mut, " history".to_owned()),
        ],
        vec![],
        vec![
            (Role::Ok, "● ".to_owned()),
            (Role::Mut, "Connected to database ".to_owned()),
            (Role::Head, format!("\"{}\"", session.dbname)),
            (Role::Mut, " on ".to_owned()),
            (Role::Acc, format!("{}:{}", session.host, session.port)),
            (Role::Dim, "  (dev · pg-wire 3.0 · BUSL-1.1)".to_owned()),
        ],
        vec![
            (
                Role::Mut,
                "  append-only · bitemporal · audit-native".to_owned(),
            ),
            (Role::Dim, " — history is never destroyed".to_owned()),
        ],
        vec![],
    ];
    write_lines(session, out, &lines)
}

/// Write one segmented line through the stdout theme.
fn write_segs(session: &Session, out: &mut impl Write, segs: &[Seg]) -> anyhow::Result<()> {
    writeln!(out, "{}", paint_segs(session.theme, segs)).context("writing results")?;
    out.flush().context("flushing results")
}

/// Write a block of segmented lines through the stdout theme.
fn write_lines(session: &Session, out: &mut impl Write, lines: &[Vec<Seg>]) -> anyhow::Result<()> {
    for segs in lines {
        writeln!(out, "{}", paint_segs(session.theme, segs)).context("writing results")?;
    }
    out.flush().context("flushing results")
}

/// The psql-style error block, on stderr: `ERROR:` + message, `SQLSTATE:` when
/// the server sent a code, `HINT:` when it sent one. Brand gold, never red.
fn print_error(session: &Session, err: &ServerError) {
    let t = &session.err_theme;
    eprintln!(
        "{}{}",
        t.paint(Role::Err, &format!("{}:  ", err.severity)),
        t.paint(Role::Err, &err.message)
    );
    if !err.code.is_empty() {
        eprintln!(
            "{}{}",
            t.paint(Role::Mut, "SQLSTATE: "),
            t.paint(Role::Warn, &err.code)
        );
    }
    if let Some(hint) = &err.hint {
        eprintln!(
            "{}{}",
            t.paint(Role::Hint, "HINT:  "),
            t.paint(Role::Hint, hint)
        );
    }
}

/// A client-side usage error in the server error shape (SQLSTATE 42601).
fn usage_error(message: String, hint: &str) -> ServerError {
    ServerError {
        severity: "ERROR".to_owned(),
        code: "42601".to_owned(),
        message,
        hint: Some(hint.to_owned()),
    }
}

/// The psql "did not find" line for `\d` misses.
fn not_found(session: &Session, out: &mut impl Write, name: &str) -> anyhow::Result<()> {
    write_segs(
        session,
        out,
        &[(
            Role::Mut,
            format!("Did not find any relation named \"{name}\"."),
        )],
    )
}

/// A text-typed render column (OID 25).
fn text_col(name: &str) -> Column {
    Column {
        name: name.to_owned(),
        type_oid: 25,
    }
}

/// The first row-returning reply in a batch, if any.
fn first_result_set(replies: &[Reply]) -> Option<&ResultSet> {
    replies.iter().find_map(|r| match r {
        Reply::Rows(set) => Some(set),
        _ => None,
    })
}

/// The first SQL error in a batch, if any.
fn first_error(replies: &[Reply]) -> Option<&ServerError> {
    replies.iter().find_map(|r| match r {
        Reply::Error(err) => Some(err),
        _ => None,
    })
}

impl Session {
    /// Table options for the current toggles — meta-command tables (`\dt`,
    /// `\l`), which measure no round-trip.
    const fn table_opts(&self, count: bool) -> TableOpts {
        TableOpts {
            style: self.border,
            row_nums: self.row_nums,
            count,
            elapsed: None,
        }
    }

    /// Table options for a measured query result: the count trailer, carrying
    /// the round-trip when timing is on.
    const fn result_opts(&self, elapsed: Option<std::time::Duration>) -> TableOpts {
        TableOpts {
            style: self.border,
            row_nums: self.row_nums,
            count: true,
            elapsed,
        }
    }
}

// ---------------------------------------------------------------------------
// Completion data + history file
// ---------------------------------------------------------------------------

/// Every table name and column name currently live in the catalog — the pool
/// ⇥ completion draws identifiers from. Built from the **same** introspection
/// queries `\dt` and `\d` issue (STL-198 / STL-131): one `pg_class` scan for
/// the table list, then the two-step `pg_class` → `pg_attribute` lookup per
/// table for its columns. Returned sorted and de-duplicated.
///
/// Best-effort by design: a SQL-level failure (an unexpected catalog reply)
/// contributes no names rather than aborting; only a transport failure errs.
fn fetch_identifiers(client: &mut Client) -> anyhow::Result<Vec<String>> {
    let mut identifiers = std::collections::BTreeSet::new();
    for table in catalog_table_names(client)? {
        for column in table_column_names(client, &table)? {
            identifiers.insert(column);
        }
        identifiers.insert(table);
    }
    Ok(identifiers.into_iter().collect())
}

/// The live table names — the `\dt` query (a `pg_class` scan with no name
/// filter). An unexpected reply shape yields no names.
fn catalog_table_names(client: &mut Client) -> anyhow::Result<Vec<String>> {
    let replies =
        client.simple_query("SELECT c.relname FROM pg_catalog.pg_class c ORDER BY c.relname")?;
    if first_error(&replies).is_some() {
        return Ok(Vec::new());
    }
    let Some(set) = first_result_set(&replies) else {
        return Ok(Vec::new());
    };
    // Resolve `relname` by name, never by position — same contract as
    // `list_tables`: a projection-honoring server contributes nothing rather
    // than the wrong column.
    let Some(idx) = set.columns.iter().position(|c| c.name == "relname") else {
        return Ok(Vec::new());
    };
    Ok(set
        .rows
        .iter()
        .filter_map(|row| row.get(idx).cloned().flatten())
        .collect())
}

/// One table's column names — the `\d <table>` two-step: resolve the relation's
/// oid in `pg_class`, then read its `pg_attribute` rows. A miss (no such table,
/// odd reply) yields no names.
fn table_column_names(client: &mut Client, name: &str) -> anyhow::Result<Vec<String>> {
    let escaped = name.replace('\'', "''");
    let replies = client.simple_query(&format!(
        "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname = '{escaped}'"
    ))?;
    if first_error(&replies).is_some() {
        return Ok(Vec::new());
    }
    let oid = first_result_set(&replies)
        .and_then(|set| set.rows.first())
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .and_then(|s| s.parse::<u32>().ok());
    let Some(oid) = oid else {
        return Ok(Vec::new());
    };
    let replies = client.simple_query(&format!(
        "SELECT a.attname, a.atttypname, a.attnum FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = {oid} AND a.attnum > 0"
    ))?;
    if first_error(&replies).is_some() {
        return Ok(Vec::new());
    }
    let Some(set) = first_result_set(&replies) else {
        return Ok(Vec::new());
    };
    // `attname` is the first projected column.
    Ok(set
        .rows
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .collect())
}

/// `~/.stele_history`, or `None` when `$HOME` is unset (history then lives only
/// in memory for the session). Stele is Unix-only ([STL-159]), so `$HOME` is
/// the right resolver and no `dirs`-style probe crate is warranted.
fn history_file_path() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|home| std::path::Path::new(&home).join(".stele_history"))
}

// ---------------------------------------------------------------------------
// rustyline glue
// ---------------------------------------------------------------------------

/// Colors the prompt and the live input line, and completes meta-commands /
/// SQL keywords / live identifiers on ⇥. Hinting stays a no-op.
struct ShellHelper {
    theme: Theme,
    /// Live table + column names ⇥ completion draws on, refreshed from the
    /// catalog after each statement (see [`fetch_identifiers`]).
    identifiers: Vec<String>,
}

impl rustyline::Helper for ShellHelper {}

/// The backslash meta-commands ⇥ completes against — the full designed surface
/// (the `\?` registry), matching the prototype's completion pool.
const META_NAMES: &[&str] = &[
    r"\?",
    r"\h",
    r"\d",
    r"\dt",
    r"\l",
    r"\conninfo",
    r"\asof",
    r"\history",
    r"\timeline",
    r"\lineage",
    r"\audit",
    r"\segments",
    r"\status",
    r"\backup",
    r"\restore",
    r"\pitr",
    r"\inspect-segment",
    r"\timing",
    r"\x",
    r"\json",
    r"\clear",
    r"\q",
];

/// SQL keywords ⇥ completes against — the statement surface the binder
/// actually accepts today (no `ORDER BY`/`LIMIT`: those are still rejected
/// clauses, and completion must not suggest syntax the server bounces).
/// Identifiers (table / column names) come live from the catalog
/// ([`fetch_identifiers`]) — this list deliberately drops the prototype's
/// hardcoded demo names (`account`, `balance`, …).
const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE FROM",
    "MERGE INTO",
    "USING",
    "WHEN MATCHED THEN UPDATE SET",
    "WHEN NOT MATCHED THEN INSERT",
    "CREATE TABLE",
    "DROP TABLE",
    "WITH SYSTEM VERSIONING",
    "VALID TIME",
    "FOR SYSTEM_TIME AS OF",
    "FOR VALID_TIME AS OF",
    "GROUP BY",
    "JOIN",
    "LEFT JOIN",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "CHECKPOINT",
    "FLUSH",
    "COMPACT",
    "PERIOD",
    "now()",
    "interval",
];

/// Characters that make up a SQL completion token — letters, `_`, and the
/// parens of `now()`, matching the prototype's `[A-Za-z_()]` word class.
const fn is_sql_token_char(c: char) -> bool {
    c.is_ascii_alphabetic() || matches!(c, '_' | '(' | ')')
}

/// ⇥ completion, faithful to the prototype: **unique-match only, no menu**.
/// Returns the byte offset where the replacement begins and at most one
/// candidate (already suffixed with a space); an empty list leaves the line
/// untouched (ambiguous or no match — no cycling list).
///
/// Two modes, picked by what precedes the cursor:
/// * a lone backslash word (`\hi`) completes a meta-command name;
/// * anything else completes the trailing identifier token against the SQL
///   keywords and the live catalog names — so `\d cust⇥` and `SELECT … accou⇥`
///   both work.
fn complete(line: &str, pos: usize, identifiers: &[String]) -> (usize, Vec<String>) {
    let head = &line[..pos];
    let trimmed = head.trim_start();
    if trimmed.starts_with('\\') && !trimmed.contains(char::is_whitespace) {
        let start = pos - trimmed.len();
        // Keep exact matches in the pool (unlike the SQL branch): if the input
        // is already a valid command that is *also* a prefix of a longer one
        // (`\d` vs `\dt`), that's two candidates → ambiguous → no completion,
        // so ⇥ never rewrites `\d` and `\d <table>` stays reachable.
        let matches = META_NAMES
            .iter()
            .filter(|name| name.starts_with(trimmed))
            .map(|name| format!("{name} "))
            .collect();
        return unique_completion(start, matches);
    }

    // Trailing `[A-Za-z_()]+` token before the cursor.
    let mut start = pos;
    for (i, c) in head.char_indices().rev() {
        if is_sql_token_char(c) {
            start = i;
        } else {
            break;
        }
    }
    let token = &head[start..pos];
    if token.is_empty() {
        return (pos, Vec::new());
    }
    let lower = token.to_ascii_lowercase();
    let matches = SQL_KEYWORDS
        .iter()
        .map(|kw| (*kw).to_owned())
        .chain(identifiers.iter().cloned())
        .filter(|cand| {
            let folded = cand.to_ascii_lowercase();
            folded.starts_with(&lower) && folded != lower
        })
        .map(|cand| format!("{cand} "))
        .collect();
    unique_completion(start, matches)
}

/// Keep a completion only when it is unambiguous: exactly one candidate
/// replaces the token, otherwise nothing happens (no menu, the prototype's
/// rule).
fn unique_completion(start: usize, mut matches: Vec<String>) -> (usize, Vec<String>) {
    if matches.len() == 1 {
        (start, vec![matches.remove(0)])
    } else {
        (start, Vec::new())
    }
}

impl rustyline::completion::Completer for ShellHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        Ok(complete(line, pos, &self.identifiers))
    }
}

impl rustyline::hint::Hinter for ShellHelper {
    type Hint = String;
}

impl rustyline::validate::Validator for ShellHelper {}

impl rustyline::highlight::Highlighter for ShellHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> std::borrow::Cow<'l, str> {
        if !self.theme.colored() || line.is_empty() {
            return std::borrow::Cow::Borrowed(line);
        }
        std::borrow::Cow::Owned(paint_segs(self.theme, &highlight::tokenize(line)))
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> std::borrow::Cow<'b, str> {
        if !default || !self.theme.colored() {
            return std::borrow::Cow::Borrowed(prompt);
        }
        let role = if prompt.starts_with("stele-") {
            Role::Cont
        } else {
            Role::Prompt
        };
        std::borrow::Cow::Owned(self.theme.paint(role, prompt))
    }

    fn highlight_char(
        &self,
        _line: &str,
        _pos: usize,
        _kind: rustyline::highlight::CmdKind,
    ) -> bool {
        self.theme.colored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_commands_parse_with_aliases() {
        assert_eq!(parse_meta(r"\q"), Some(Meta::Quit));
        assert_eq!(parse_meta(r"\?"), Some(Meta::Help));
        assert_eq!(parse_meta(r"\help"), Some(Meta::SqlHelp));
        assert_eq!(
            parse_meta(r"\d account"),
            Some(Meta::Describe(Some("account")))
        );
        // Bare \d lists relations, like the prototype.
        assert_eq!(parse_meta(r"\d"), Some(Meta::ListTables));
        assert_eq!(parse_meta(r"\dt"), Some(Meta::ListTables));
        assert_eq!(parse_meta(r"\list"), Some(Meta::ListDbs));
        assert_eq!(parse_meta(r"\c!"), Some(Meta::Clear));
        assert_eq!(parse_meta(r"\connect"), Some(Meta::Connect));
        assert_eq!(parse_meta("SELECT 1;"), None);
    }

    #[test]
    fn designed_tiers_resolve_to_their_tickets() {
        // The still-stubbed admin tier points at STL-200. (`\segments` is live as
        // of STL-301, `\audit` as of STL-302.)
        let Some(Meta::NotYet { ticket, .. }) = parse_meta(r"\inspect seg-0002") else {
            panic!("expected NotYet");
        };
        assert_eq!(ticket, ADMIN_TICKET);
    }

    #[test]
    fn temporal_commands_parse_their_table_and_key() {
        assert_eq!(
            parse_meta(r"\history account 1"),
            Some(Meta::History {
                table: "account",
                key: Some("1"),
            })
        );
        // \history's key is optional (whole-table timeline).
        assert_eq!(
            parse_meta(r"\history account"),
            Some(Meta::History {
                table: "account",
                key: None,
            })
        );
        assert_eq!(
            parse_meta(r"\timeline account 1"),
            Some(Meta::Timeline {
                table: "account",
                key: "1",
            })
        );
        assert_eq!(
            parse_meta(r"\lineage account 1"),
            Some(Meta::Lineage {
                table: "account",
                key: "1",
            })
        );
        // \segments takes a bare table.
        assert_eq!(
            parse_meta(r"\segments account"),
            Some(Meta::Segments { table: "account" })
        );
        // \audit's table is optional (defaults to the first relation).
        assert_eq!(
            parse_meta(r"\audit account"),
            Some(Meta::Audit {
                table: Some("account"),
            })
        );
        assert_eq!(parse_meta(r"\audit"), Some(Meta::Audit { table: None }));
        // \timeline / \lineage require a key; \history / \segments a table.
        assert!(matches!(
            parse_meta(r"\timeline account"),
            Some(Meta::BadArgs { .. })
        ));
        assert!(matches!(
            parse_meta(r"\history"),
            Some(Meta::BadArgs { .. })
        ));
        assert!(matches!(
            parse_meta(r"\segments"),
            Some(Meta::BadArgs { .. })
        ));
    }

    #[test]
    fn asof_takes_a_multi_word_expression_and_resets() {
        // The whole remainder is the AS OF expression, spaces and all.
        assert_eq!(
            parse_meta(r"\asof now() - interval '1 second'"),
            Some(Meta::AsOf(Some("now() - interval '1 second'")))
        );
        // Bare and `reset` both clear.
        assert_eq!(parse_meta(r"\asof"), Some(Meta::AsOf(None)));
        assert_eq!(parse_meta(r"\asof reset"), Some(Meta::AsOf(None)));
        assert_eq!(parse_meta(r"\asof RESET"), Some(Meta::AsOf(None)));
    }

    #[test]
    fn asof_rewrites_only_bare_selects() {
        // A bare SELECT gains the qualifier; the trailing `;` is preserved.
        assert_eq!(
            apply_asof("SELECT * FROM account;", Some("2")),
            "SELECT * FROM account FOR SYSTEM_TIME AS OF 2;"
        );
        // No context set → untouched.
        assert_eq!(
            apply_asof("SELECT * FROM account;", None),
            "SELECT * FROM account;"
        );
        // A write is never time-traveled.
        assert_eq!(
            apply_asof("INSERT INTO account VALUES (1, 2);", Some("2")),
            "INSERT INTO account VALUES (1, 2);"
        );
        // A query that already pins the system axis is left as written.
        let already = "SELECT * FROM account FOR SYSTEM_TIME AS OF 5;";
        assert_eq!(apply_asof(already, Some("2")), already);
        // A multi-statement batch is not spliced.
        let batch = "SELECT 1; SELECT 2;";
        assert_eq!(apply_asof(batch, Some("2")), batch);
    }

    #[test]
    fn history_key_rejects_statement_breakers() {
        // Plain int / quoted-text keys are fine.
        assert!(key_is_safe("1"));
        assert!(key_is_safe("'alice'"));
        // A `;` or a control character could break out of the single statement.
        assert!(!key_is_safe("1;DROP TABLE account"));
        assert!(!key_is_safe("1\nSELECT 1"));
    }

    #[test]
    fn unknown_meta_is_a_usage_error_not_sql() {
        assert_eq!(parse_meta(r"\x9 on"), Some(Meta::Unknown(r"\x9 on")));
    }

    #[test]
    fn toggles_accept_on_off_and_reject_junk() {
        assert_eq!(parse_meta(r"\timing on"), Some(Meta::Timing(Some("on"))));
        assert_eq!(toggle_value(false, None), Ok(true));
        assert_eq!(toggle_value(true, None), Ok(false));
        // `on` is idempotent — running it twice never inverts (psql habit).
        assert_eq!(toggle_value(true, Some("on")), Ok(true));
        assert_eq!(toggle_value(true, Some("OFF")), Ok(false));
        let err = toggle_value(false, Some("maybe")).unwrap_err();
        assert_eq!(err.code, "42601");
    }

    #[test]
    fn dt_with_a_pattern_is_rejected_not_silently_unfiltered() {
        assert!(matches!(
            parse_meta(r"\dt acc*"),
            Some(Meta::BadArgs { .. })
        ));
    }

    #[test]
    fn statement_termination_is_quote_and_comment_aware() {
        assert!(statement_terminated("SELECT 1;"));
        assert!(statement_terminated("SELECT 1; \n"));
        assert!(statement_terminated("SELECT 1 -- tail comment\n;"));
        assert!(!statement_terminated("SELECT 1"));
        // A `;` at a line break inside a string literal is NOT a terminator…
        assert!(!statement_terminated("INSERT INTO t VALUES ('a;\n"));
        // …until the literal closes and a real `;` follows.
        assert!(statement_terminated("INSERT INTO t VALUES ('a;\nb');\n"));
        // '' escape keeps the literal open across an apparent close.
        assert!(!statement_terminated("SELECT 'it''s;\n"));
        // A `;` swallowed by a line comment does not terminate.
        assert!(!statement_terminated("SELECT 1 -- done;\n"));
    }

    #[test]
    fn prompt_reflects_buffer_and_txn_state() {
        assert_eq!(prompt_text(b'I', ""), "stele=# ");
        assert_eq!(prompt_text(b'T', ""), "stele*=# ");
        assert_eq!(prompt_text(b'E', "  \n"), "stele!=# ");
        // Continuation beats transaction state.
        assert_eq!(prompt_text(b'T', "SELECT"), "stele-# ");
    }

    #[test]
    fn usage_error_carries_the_psql_fields() {
        let err = usage_error("invalid command \\zz".to_owned(), "Try \\?");
        assert_eq!(err.code, "42601");
        assert_eq!(err.severity, "ERROR");
        assert_eq!(err.hint.as_deref(), Some("Try \\?"));
    }

    #[test]
    fn completes_a_unique_meta_command() {
        // `\au` uniquely prefixes \audit; the candidate carries a trailing space.
        assert_eq!(complete(r"\au", 3, &[]), (0, vec![r"\audit ".to_owned()]));
    }

    #[test]
    fn ambiguous_meta_prefix_does_not_complete() {
        // `\t` prefixes both \timing and \timeline — no menu, no completion.
        let (_, cands) = complete(r"\t", 2, &[]);
        assert!(cands.is_empty(), "{cands:?}");
    }

    #[test]
    fn a_meta_command_with_a_longer_sibling_does_not_complete() {
        // `\d` and `\dt` both match `\d`, so ⇥ stays its hand — `\d <table>`
        // must remain reachable (regression: don't rewrite `\d` → `\dt`).
        let (_, cands) = complete(r"\d", 2, &[]);
        assert!(cands.is_empty(), "{cands:?}");
    }

    #[test]
    fn a_complete_meta_command_with_no_sibling_just_gains_a_space() {
        // `\conninfo` is unique with no longer extension → ⇥ appends a space
        // (the prototype keeps exact matches in the meta pool).
        assert_eq!(
            complete(r"\conninfo", 9, &[]),
            (0, vec![r"\conninfo ".to_owned()])
        );
    }

    #[test]
    fn completes_a_unique_sql_keyword_case_insensitively() {
        // Lower-case input completes to the canonical keyword casing.
        assert_eq!(complete("sel", 3, &[]), (0, vec!["SELECT ".to_owned()]));
    }

    #[test]
    fn ambiguous_sql_keyword_prefix_does_not_complete() {
        // `SE` prefixes both SELECT and SET.
        let (_, cands) = complete("SE", 2, &[]);
        assert!(cands.is_empty(), "{cands:?}");
    }

    #[test]
    fn an_exact_keyword_token_is_left_alone() {
        // A token already equal to a keyword has no longer completion.
        let (_, cands) = complete("SELECT", 6, &[]);
        assert!(cands.is_empty(), "{cands:?}");
    }

    #[test]
    fn completes_a_live_identifier_at_the_cursor() {
        let ids = vec!["account".to_owned(), "balance".to_owned()];
        let line = "SELECT * FROM accou";
        // Only the trailing token is replaced; the offset points at its start.
        assert_eq!(
            complete(line, line.len(), &ids),
            (14, vec!["account ".to_owned()])
        );
    }

    #[test]
    fn completes_a_table_name_after_a_describe() {
        // `\d cust` has whitespace → not a meta word → identifier completion.
        let ids = vec!["customer".to_owned()];
        assert_eq!(
            complete(r"\d cust", 7, &ids),
            (3, vec!["customer ".to_owned()])
        );
    }

    #[test]
    fn ambiguous_identifier_prefix_does_not_complete() {
        let ids = vec!["account".to_owned(), "accrued".to_owned()];
        let line = "SELECT acc";
        let (_, cands) = complete(line, line.len(), &ids);
        assert!(cands.is_empty(), "{cands:?}");
    }

    #[test]
    fn history_file_path_is_under_home() {
        // No env mutation: just assert the shape when $HOME is present (it is in
        // CI and dev shells alike).
        if let Some(path) = history_file_path() {
            assert!(path.ends_with(".stele_history"), "{path:?}");
        }
    }

    #[test]
    fn history_persists_across_sessions_at_0600_via_file_history() {
        // STL-221: `with-file-history` makes `DefaultHistory` a `FileHistory`,
        // which appends-on-exit (file-locked) and creates the file `0600`. This
        // also guards the feature staying enabled: with it off, `DefaultHistory`
        // is `MemHistory` whose save/append/load are no-ops, so the file would
        // never appear (0600 check fails) and the reload would recall nothing.
        use rustyline::history::{DefaultHistory, History as _, SearchDirection};

        // A throwaway path under the temp dir — never the real ~/.stele_history.
        let dir = std::env::temp_dir().join(format!(".stele-hist-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("history");

        let config = rustyline::Config::builder()
            .max_history_size(100)
            .expect("history size")
            .history_ignore_dups(true)
            .expect("history dedupe")
            .build();

        // Session one: record two statements (one multi-line) and append on exit.
        let mut first = DefaultHistory::with_config(&config);
        first.add("SELECT 1;").expect("add");
        first.add("SELECT id\n  FROM account;").expect("add");
        first.append(&path).expect("append");

        // History can carry a credential in a literal — it must be owner-only.
        // Mode bits are a Unix concept; on Windows the file inherits the user
        // profile's ACL instead, so this half of the test is Unix-only
        // (STL-160) while the persistence half runs everywhere.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "history mode {mode:o}, expected 0600");
        }

        // Session two: a fresh history loads the prior session's entries, the
        // multi-line statement intact as one entry.
        let mut second = DefaultHistory::with_config(&config);
        second.load(&path).expect("load");
        assert_eq!(second.len(), 2, "expected both statements recalled");
        let last = second
            .get(1, SearchDirection::Forward)
            .expect("get")
            .expect("entry present");
        assert_eq!(last.entry.as_ref(), "SELECT id\n  FROM account;");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
