//! libpq-compatible password-file (`~/.pgpass`) lookup for `stele shell`
//! ([STL-335]).
//!
//! When neither `PGPASSWORD` nor a prompt supplies the SCRAM password, the shell
//! consults the libpq password file — the same source, in the same precedence
//! slot (after `PGPASSWORD`, before the interactive prompt), that `psql` uses. A
//! file holds one `host:port:database:user:password` rule per line; the first
//! line whose first four fields match the connection wins, and its (de-escaped)
//! password is returned.
//!
//! Parity with libpq's `passwordFromFile`:
//! * **Path** — `$PGPASSFILE` when set, else `~/.pgpass`
//!   (`%APPDATA%\postgresql\pgpass.conf` on Windows).
//! * **Permissions** — on unix a file any *group* or *world* bit is set on
//!   (`0o077`) is **ignored with a warning**; the secret must be `0600` or less.
//!   (libpq does not check permissions off unix.)
//! * **Matching** — a literal `*` field matches anything; every other field is
//!   compared verbatim. `\` escapes the next character, so `\:` and `\\` are
//!   literal colons/backslashes inside a field, and the password (the last
//!   field) may itself contain colons.
//! * **Skipped lines** — blank lines and `#` comments.
//!
//! This module only *reads* the file; the resolution order lives in
//! `shell::connect`.
//!
//! [STL-335]: https://allegromusic.atlassian.net/browse/STL-335

use std::path::{Path, PathBuf};

/// Look up the password for `(host, port, database, user)` in the libpq
/// password file. Returns `None` when there is no file, the file is ignored
/// (too permissive / not a plain file), or no line matches — every case in which
/// the caller should fall through to the next password source.
pub fn lookup(host: &str, port: u16, database: &str, user: &str) -> Option<String> {
    let path = file_path()?;
    lookup_in(&path, host, port, database, user)
}

/// The password-file path: `$PGPASSFILE` when set and non-empty, else the
/// platform default. `None` when neither is resolvable (e.g. no `HOME`).
fn file_path() -> Option<PathBuf> {
    match std::env::var_os("PGPASSFILE") {
        Some(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => default_path(),
    }
}

/// `~/.pgpass`, from `$HOME`.
#[cfg(not(windows))]
fn default_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").filter(|h| !h.is_empty())?;
    Some(Path::new(&home).join(".pgpass"))
}

/// `%APPDATA%\postgresql\pgpass.conf`, libpq's Windows default.
#[cfg(windows)]
fn default_path() -> Option<PathBuf> {
    let appdata = std::env::var_os("APPDATA").filter(|a| !a.is_empty())?;
    Some(Path::new(&appdata).join("postgresql").join("pgpass.conf"))
}

/// Look up in a specific file — the testable core: the permission gate, then a
/// first-match scan of the rules.
fn lookup_in(path: &Path, host: &str, port: u16, database: &str, user: &str) -> Option<String> {
    if !usable(path) {
        return None;
    }
    let contents = std::fs::read_to_string(path).ok()?;
    let port = port.to_string();
    contents
        .lines()
        .find_map(|line| match_line(line, host, &port, database, user))
}

/// Whether the password file should be read at all: it exists, is a plain file,
/// and (on unix) is not group/world-accessible. A too-permissive or non-plain
/// file is ignored with a warning — libpq's behavior; a missing or unreadable
/// file is skipped silently (it just means "no `.pgpass`").
fn usable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        eprintln!(
            "WARNING: password file \"{}\" is not a plain file",
            path.display()
        );
        return false;
    }
    permission_ok(&meta, path)
}

/// libpq ignores a password file with any group/world permission bit set
/// (`0o077`): the secret must be `u=rw` (`0600`) or less.
#[cfg(unix)]
fn permission_ok(meta: &std::fs::Metadata, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    if meta.mode() & 0o077 != 0 {
        eprintln!(
            "WARNING: password file \"{}\" has group or world access; \
             permissions should be u=rw (0600) or less",
            path.display()
        );
        return false;
    }
    true
}

/// libpq does not check password-file permissions off unix.
#[cfg(not(unix))]
fn permission_ok(_meta: &std::fs::Metadata, _path: &Path) -> bool {
    true
}

/// If `line` is a rule whose `host:port:database:user` fields match the
/// connection, return its de-escaped password. Blank lines, `#` comments, and
/// malformed (not exactly five-field) lines never match.
fn match_line(line: &str, host: &str, port: &str, database: &str, user: &str) -> Option<String> {
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let fields = raw_fields(line);
    if fields.len() != 5 {
        return None;
    }
    (field_matches(&fields[0], host)
        && field_matches(&fields[1], port)
        && field_matches(&fields[2], database)
        && field_matches(&fields[3], user))
    .then(|| deescape(&fields[4]))
}

/// Split a `.pgpass` line into its (raw, still-escaped) colon-separated fields,
/// honoring `\` so an escaped `\:` does not separate. At most five fields: after
/// the fourth separator the remainder is the password verbatim, so a password
/// may contain unescaped colons (matching libpq).
fn raw_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::with_capacity(5);
    let mut cur = String::new();
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                // Keep the backslash *and* the escaped char; `deescape` resolves
                // them once a field is selected. A trailing lone `\` is kept as-is.
                cur.push('\\');
                if let Some(next) = chars.next() {
                    cur.push(next);
                }
            }
            ':' if fields.len() < 4 => fields.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    fields.push(cur);
    fields
}

/// Whether a raw rule field matches a connection value: a bare `*` is the libpq
/// wildcard (an *escaped* `\*` is a literal asterisk, so the raw check comes
/// first); otherwise the de-escaped field must equal the value.
fn field_matches(raw: &str, value: &str) -> bool {
    raw == "*" || deescape(raw) == value
}

/// Resolve `\`-escapes in a field: `\x` becomes `x` for any `x`, so `\:` and
/// `\\` are literal. A trailing lone `\` is dropped.
fn deescape(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_returns_the_password() {
        let line = "db.example.com:5454:stele:alice:s3cret";
        assert_eq!(
            match_line(line, "db.example.com", "5454", "stele", "alice"),
            Some("s3cret".to_owned())
        );
    }

    #[test]
    fn a_mismatch_in_any_field_does_not_match() {
        let line = "db.example.com:5454:stele:alice:s3cret";
        assert_eq!(
            match_line(line, "other-host", "5454", "stele", "alice"),
            None
        );
        assert_eq!(
            match_line(line, "db.example.com", "5555", "stele", "alice"),
            None
        );
        assert_eq!(
            match_line(line, "db.example.com", "5454", "other", "alice"),
            None
        );
        assert_eq!(
            match_line(line, "db.example.com", "5454", "stele", "bob"),
            None
        );
    }

    #[test]
    fn wildcards_match_any_value_in_their_field() {
        let line = "*:*:*:*:universal";
        assert_eq!(
            match_line(line, "anything", "1", "whatever", "nobody"),
            Some("universal".to_owned())
        );
        // A wildcard host with a pinned user.
        let pinned = "*:*:*:alice:alice-pw";
        assert_eq!(
            match_line(pinned, "h", "5454", "stele", "alice"),
            Some("alice-pw".to_owned())
        );
        assert_eq!(match_line(pinned, "h", "5454", "stele", "bob"), None);
    }

    #[test]
    fn comments_and_blank_lines_never_match() {
        assert_eq!(match_line("", "h", "5454", "db", "u"), None);
        assert_eq!(match_line("# *:*:*:*:nope", "h", "5454", "db", "u"), None);
    }

    #[test]
    fn too_few_fields_is_not_a_match() {
        // Four fields (no password column) is malformed → skipped, not a match
        // with an empty password.
        assert_eq!(match_line("h:5454:db:u", "h", "5454", "db", "u"), None);
    }

    #[test]
    fn an_empty_password_field_is_a_valid_match() {
        assert_eq!(
            match_line("h:5454:db:u:", "h", "5454", "db", "u"),
            Some(String::new())
        );
    }

    #[test]
    fn a_password_may_contain_unescaped_colons() {
        // Everything past the fourth colon is the password, colons and all.
        assert_eq!(
            match_line("h:5454:db:u:pa:ss:word", "h", "5454", "db", "u"),
            Some("pa:ss:word".to_owned())
        );
    }

    #[test]
    fn backslash_escapes_a_colon_inside_a_field() {
        // A database literally named "a:b" is written "a\:b" so the colon does
        // not separate fields.
        let line = r"h:5454:a\:b:u:pw";
        assert_eq!(
            match_line(line, "h", "5454", "a:b", "u"),
            Some("pw".to_owned())
        );
    }

    #[test]
    fn backslash_escapes_a_backslash() {
        let line = r"h:5454:a\\b:u:pw";
        assert_eq!(
            match_line(line, "h", "5454", r"a\b", "u"),
            Some("pw".to_owned())
        );
    }

    #[test]
    fn an_escaped_asterisk_is_a_literal_not_a_wildcard() {
        // `\*` is a literal asterisk: it matches only a value that is "*", not
        // any value the way a bare `*` wildcard would.
        let line = r"\*:5454:db:u:pw";
        assert_eq!(
            match_line(line, "*", "5454", "db", "u"),
            Some("pw".to_owned())
        );
        assert_eq!(match_line(line, "real-host", "5454", "db", "u"), None);
    }

    #[test]
    fn lookup_in_returns_the_first_matching_line() {
        let dir = std::env::temp_dir().join(format!("stele-pgpass-first-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let path = dir.join("pgpass");
        std::fs::write(
            &path,
            "# a comment\n\
             other:5454:stele:alice:wrong\n\
             h:5454:stele:alice:right\n\
             h:5454:stele:alice:later\n",
        )
        .expect("write pgpass");
        set_owner_only(&path);

        assert_eq!(
            lookup_in(&path, "h", 5454, "stele", "alice"),
            Some("right".to_owned())
        );
        // A connection nothing matches falls through to `None`.
        assert_eq!(lookup_in(&path, "h", 5454, "stele", "nobody"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_missing_file_is_silently_skipped() {
        let path = std::env::temp_dir().join(format!("stele-pgpass-absent-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert_eq!(lookup_in(&path, "h", 5454, "db", "u"), None);
    }

    /// On unix, a `.pgpass` any group/world bit is set on is ignored — the secret
    /// is never read from a file other users can see.
    #[cfg(unix)]
    #[test]
    fn a_group_or_world_readable_file_is_ignored() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!("stele-pgpass-perm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let path = dir.join("pgpass");
        std::fs::write(&path, "h:5454:stele:alice:s3cret\n").expect("write pgpass");

        // 0640 (group-readable) is too permissive → ignored.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
            .expect("chmod 0640");
        assert_eq!(lookup_in(&path, "h", 5454, "stele", "alice"), None);

        // 0600 is fine → the password is read.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod 0600");
        assert_eq!(
            lookup_in(&path, "h", 5454, "stele", "alice"),
            Some("s3cret".to_owned())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Give the scratch file `0600` on unix so the permission gate does not
    /// reject it; a no-op elsewhere.
    #[cfg(unix)]
    fn set_owner_only(path: &Path) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    }

    #[cfg(not(unix))]
    fn set_owner_only(_path: &Path) {}
}
