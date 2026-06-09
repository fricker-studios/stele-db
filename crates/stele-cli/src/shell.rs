//! The `stele shell` REPL ([STL-185]): read statements from stdin, run them
//! over pg-wire via the in-crate [`Client`], render results psql-style.
//!
//! Behavior notes, all chosen to keep scripted sessions (the integration test,
//! pipes, heredocs) byte-clean:
//!
//! * Prompts and the banner print **only when stdin is a TTY**; a piped session
//!   produces nothing but results.
//! * A statement is sent when its buffer ends with `;` — statements may span
//!   lines, and meta-commands are recognized only at a statement boundary.
//! * SQL errors print to **stderr** (`ERROR: …`) and the session continues;
//!   transport errors end the shell with a non-zero exit.
//!
//! [STL-185]: https://allegromusic.atlassian.net/browse/STL-185

use std::fmt::Write as _;
use std::io::{BufRead, IsTerminal as _, Write};

use anyhow::Context as _;

use crate::client::{Client, Reply, ResultSet};

/// Connection parameters for `stele shell` (from clap in `main`).
pub struct Opts {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
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
    let interactive = stdin.is_terminal();
    if interactive {
        println!(
            "stele shell — connected to {}:{} as {}.\nType \\q to quit, \\d <table> to describe a table; statements end with ;",
            opts.host, opts.port, opts.user
        );
    }
    repl(&mut client, stdin.lock(), std::io::stdout(), interactive)
}

/// The REPL proper, over injected streams so the loop is host-agnostic.
fn repl(
    client: &mut Client,
    input: impl BufRead,
    mut out: impl Write,
    interactive: bool,
) -> anyhow::Result<()> {
    let mut buffer = String::new();
    if interactive {
        prompt(client, &buffer, &mut out)?;
    }
    for line in input.lines() {
        let line = line.context("reading stdin")?;

        // Meta-commands are lines of their own, between statements.
        if buffer.trim().is_empty() {
            match parse_meta(&line) {
                Some(Meta::Quit) => return Ok(()),
                Some(Meta::Describe(Some(name))) => {
                    describe(client, name, &mut out)?;
                    if interactive {
                        prompt(client, &buffer, &mut out)?;
                    }
                    continue;
                }
                Some(Meta::Describe(None)) => {
                    eprintln!(r"\d needs a table name in v0.2 — usage: \d <table>");
                    if interactive {
                        prompt(client, &buffer, &mut out)?;
                    }
                    continue;
                }
                Some(Meta::Unknown(cmd)) => {
                    eprintln!(r"unknown meta-command {cmd} — try \d <table> or \q");
                    if interactive {
                        prompt(client, &buffer, &mut out)?;
                    }
                    continue;
                }
                None => {}
            }
        }

        buffer.push_str(&line);
        buffer.push('\n');
        if statement_complete(&buffer) {
            let sql = std::mem::take(&mut buffer);
            run_statement(client, sql.trim(), &mut out)?;
        }
        if interactive {
            prompt(client, &buffer, &mut out)?;
        }
    }
    Ok(())
}

/// Send one buffered statement (or batch) and render every reply.
fn run_statement(client: &mut Client, sql: &str, out: &mut impl Write) -> anyhow::Result<()> {
    for reply in client.simple_query(sql)? {
        match reply {
            Reply::Rows(set) => out
                .write_all(render_table(&set.columns, &set.rows).as_bytes())
                .context("writing results")?,
            Reply::Command(tag) => writeln!(out, "{tag}").context("writing results")?,
            Reply::Error(message) => eprintln!("{message}"),
            Reply::Empty => {}
        }
    }
    out.flush().context("flushing results")
}

/// `\d <table>` — drive the two-query `pg_catalog` introspection sequence the
/// server's shim answers (STL-131): `pg_class` by name → synthetic oid, then
/// `pg_attribute` by that oid → one row per column.
fn describe(client: &mut Client, name: &str, out: &mut impl Write) -> anyhow::Result<()> {
    let escaped = name.replace('\'', "''");
    let replies = client.simple_query(&format!(
        "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname = '{escaped}'"
    ))?;
    let oid = first_result_set(&replies)
        .and_then(|set| set.rows.first())
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .map(str::to_owned);
    let Some(oid) = oid else {
        writeln!(out, "Did not find any relation named \"{name}\".")
            .and_then(|()| out.flush())
            .context("writing results")?;
        return Ok(());
    };
    // The shim mints oids as non-negative i32s; anything else means we are not
    // talking to a Stele server, so say so rather than interpolating it back.
    let oid: u32 = oid.parse().context("unexpected relation oid from server")?;

    let replies = client.simple_query(&format!(
        "SELECT a.attname, a.atttypname, a.attnum FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = {oid} AND a.attnum > 0"
    ))?;
    let columns = first_result_set(&replies).map(|set| {
        set.rows
            .iter()
            .map(|row| {
                let cell = |i: usize| row.get(i).cloned().flatten();
                vec![cell(0), cell(1)]
            })
            .collect::<Vec<_>>()
    });
    let Some(columns) = columns else {
        writeln!(out, "Did not find any relation named \"{name}\".")
            .and_then(|()| out.flush())
            .context("writing results")?;
        return Ok(());
    };

    writeln!(out, "Table \"public.{name}\"").context("writing results")?;
    out.write_all(render_table(&["Column".to_owned(), "Type".to_owned()], &columns).as_bytes())
        .context("writing results")?;
    out.flush().context("flushing results")
}

/// The first row-returning reply in a batch, if any.
fn first_result_set(replies: &[Reply]) -> Option<&ResultSet> {
    replies.iter().find_map(|r| match r {
        Reply::Rows(set) => Some(set),
        _ => None,
    })
}

/// Write the prompt for the current buffer/transaction state and flush.
fn prompt(client: &Client, buffer: &str, out: &mut impl Write) -> anyhow::Result<()> {
    let p = if buffer.trim().is_empty() {
        match client.txn_status() {
            b'T' => "stele*=> ",
            b'E' => "stele!=> ",
            _ => "stele=> ",
        }
    } else {
        "stele-> "
    };
    write!(out, "{p}")
        .and_then(|()| out.flush())
        .context("writing prompt")
}

/// A recognized backslash meta-command.
#[derive(Debug, PartialEq, Eq)]
enum Meta<'a> {
    Quit,
    Describe(Option<&'a str>),
    Unknown(&'a str),
}

/// Parse a meta-command line; `None` means the line is SQL.
fn parse_meta(line: &str) -> Option<Meta<'_>> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix('\\')?;
    let mut parts = rest.split_whitespace();
    match parts.next() {
        Some("q") => Some(Meta::Quit),
        Some("d") => Some(Meta::Describe(parts.next())),
        _ => Some(Meta::Unknown(trimmed)),
    }
}

/// A buffered statement is ready once it ends with `;`.
fn statement_complete(buffer: &str) -> bool {
    buffer.trim_end().ends_with(';')
}

/// Render a result set psql-style: padded header, dashed separator, one line
/// per row (NULL renders empty), and a `(N rows)` trailer.
fn render_table(columns: &[String], rows: &[Vec<Option<String>>]) -> String {
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            rows.iter()
                .filter_map(|row| row.get(i))
                .map(|cell| cell.as_deref().unwrap_or("").chars().count())
                .chain([name.chars().count()])
                .max()
                .unwrap_or(0)
        })
        .collect();

    let mut text = String::new();
    let render_line = |cells: &mut dyn Iterator<Item = &str>| {
        cells
            .zip(&widths)
            .map(|(cell, width)| format!(" {cell:<width$} "))
            .collect::<Vec<_>>()
            .join("|")
    };
    let _ = writeln!(
        text,
        "{}",
        render_line(&mut columns.iter().map(String::as_str))
    );
    let _ = writeln!(
        text,
        "{}",
        widths
            .iter()
            .map(|w| "-".repeat(w + 2))
            .collect::<Vec<_>>()
            .join("+")
    );
    for row in rows {
        let _ = writeln!(
            text,
            "{}",
            render_line(&mut row.iter().map(|c| c.as_deref().unwrap_or("")))
        );
    }
    let n = rows.len();
    let noun = if n == 1 { "row" } else { "rows" };
    let _ = writeln!(text, "({n} {noun})");
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.to_owned()
    }

    #[test]
    fn render_table_pads_columns_and_counts_rows() {
        let rendered = render_table(
            &[s("id"), s("balance")],
            &[
                vec![Some(s("1")), Some(s("100"))],
                vec![Some(s("2")), Some(s("250"))],
            ],
        );
        let expected = " id | balance \n----+---------\n 1  | 100     \n 2  | 250     \n(2 rows)\n";
        assert_eq!(rendered, expected);
    }

    #[test]
    fn render_table_singular_row_and_empty_null() {
        let rendered = render_table(&[s("a"), s("b")], &[vec![Some(s("x")), None]]);
        assert!(rendered.contains("(1 row)"), "{rendered}");
        // NULL renders as an empty (padded) cell, psql's default.
        assert!(rendered.contains(" x | "), "{rendered}");
    }

    #[test]
    fn render_table_with_no_rows_still_prints_header() {
        let rendered = render_table(&[s("oid")], &[]);
        assert!(rendered.starts_with(" oid \n-----\n"), "{rendered}");
        assert!(rendered.ends_with("(0 rows)\n"), "{rendered}");
    }

    #[test]
    fn meta_commands_parse_at_statement_boundaries() {
        assert_eq!(parse_meta(r"\q"), Some(Meta::Quit));
        assert_eq!(parse_meta(r"  \q  "), Some(Meta::Quit));
        assert_eq!(
            parse_meta(r"\d account"),
            Some(Meta::Describe(Some("account")))
        );
        assert_eq!(parse_meta(r"\d"), Some(Meta::Describe(None)));
        assert_eq!(parse_meta(r"\x on"), Some(Meta::Unknown(r"\x on")));
        assert_eq!(parse_meta("SELECT 1;"), None);
    }

    #[test]
    fn statements_complete_only_on_a_trailing_semicolon() {
        assert!(statement_complete("SELECT 1;"));
        assert!(statement_complete("SELECT 1; \n"));
        assert!(!statement_complete("SELECT 1"));
        assert!(!statement_complete("SELECT id,\n"));
    }
}
