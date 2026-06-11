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
    pub border: BorderStyle,
    pub row_nums: bool,
    pub no_color: bool,
}

/// Follow-up tickets the not-yet-wired command tiers point at.
const TEMPORAL_TICKET: &str = "STL-199";
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
    let mut client = Client::connect(&opts.host, opts.port, &opts.user, &opts.dbname)
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
        timing: false,
        expanded: false,
        json: false,
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
    let rest = trimmed.strip_prefix('\\')?;
    let mut parts = rest.split_whitespace();
    let cmd = parts.next().unwrap_or("");
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
        "asof" | "history" | "timeline" | "lineage" | "audit" | "segments" => Meta::NotYet {
            cmd,
            ticket: TEMPORAL_TICKET,
            why: "needs the temporal introspection surface",
        },
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
        (r"\segments [T]", "columnar segments + zone maps"),
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

/// The `\h` SQL crib — the four statement shapes plus the thesis line.
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
            (Role::Dim, ";".to_owned()),
        ],
        vec![
            (Role::Kw, "  INSERT".to_owned()),
            (Role::Mut, " INTO t VALUES (...);   ".to_owned()),
            (Role::Kw, "UPDATE".to_owned()),
            (Role::Mut, " t SET c = v WHERE ...;".to_owned()),
        ],
        vec![
            (Role::Kw, "  SELECT".to_owned()),
            (Role::Mut, " ... FROM t ".to_owned()),
            (Role::Kw, "FOR SYSTEM_TIME AS OF".to_owned()),
            (Role::Mut, " (now() - interval '1 second')".to_owned()),
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
// SQL execution + rendering
// ---------------------------------------------------------------------------

/// Send one buffered statement (or batch) and render every reply.
fn run_statement(
    client: &mut Client,
    session: &Session,
    sql: &str,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let replies = client.simple_query(sql)?;
    let elapsed = started.elapsed();
    for reply in replies {
        match reply {
            Reply::Rows(set) => {
                let lines = if session.json {
                    render::json_lines(&set.columns, &set.rows)
                } else if session.expanded {
                    render::expanded_lines(&set.columns, &set.rows)
                } else {
                    render::table_lines(&set.columns, &set.rows, session.table_opts(true))
                };
                write_lines(session, out, &lines)?;
            }
            Reply::Command(tag) => write_segs(session, out, &[(Role::Text, tag)])?,
            Reply::Error(err) => print_error(session, &err),
            Reply::Empty => {}
        }
    }
    if session.timing {
        let ms = elapsed.as_secs_f64() * 1000.0;
        write_segs(session, out, &[(Role::Mut, format!("Time: {ms:.3} ms"))])?;
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
    /// Table options for the current toggles.
    const fn table_opts(&self, count: bool) -> TableOpts {
        TableOpts {
            style: self.border,
            row_nums: self.row_nums,
            count,
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

/// SQL keywords ⇥ completes against. Identifiers (table / column names) come
/// live from the catalog ([`fetch_identifiers`]) — this list deliberately drops
/// the prototype's hardcoded demo names (`account`, `balance`, …).
const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "CREATE TABLE",
    "WITH SYSTEM VERSIONING",
    "FOR SYSTEM_TIME AS OF",
    "now()",
    "interval",
    "ORDER BY",
    "LIMIT",
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
        let Some(Meta::NotYet { ticket, .. }) = parse_meta(r"\history account 1") else {
            panic!("expected NotYet");
        };
        assert_eq!(ticket, TEMPORAL_TICKET);
        let Some(Meta::NotYet { ticket, .. }) = parse_meta(r"\inspect seg-0002") else {
            panic!("expected NotYet");
        };
        assert_eq!(ticket, ADMIN_TICKET);
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
        use std::os::unix::fs::PermissionsExt as _;

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
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "history mode {mode:o}, expected 0600");

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
