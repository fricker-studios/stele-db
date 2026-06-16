//! The `COPY ... FROM STDIN` data lexer ([STL-236]): split a streamed text/CSV
//! byte buffer into rows of field strings, recognizing the null marker.
//!
//! This is the wire-format half the ticket assigns to `stele-pgwire`: the
//! `CopyData` stream is reassembled into one buffer (a COPY row may span several
//! `CopyData` messages, or one message may carry many rows), then split here into
//! `Vec<Vec<Option<String>>>` — one entry per row, one field per column, `None`
//! for the null marker. The binder ([`stele_sql::bind_copy_rows`]) then folds each
//! field through the column's codec. Keeping the lexing here (next to
//! [`text_format`](crate::text_format), the value encoder) leaves the engine and
//! binder unaware of the wire format.
//!
//! Both Postgres textual formats are supported, with Postgres's exact defaults
//! (set in [`CopyFormat`](stele_sql::CopyFormat)):
//!
//! * **text** — TAB-delimited, one row per `\n` (a trailing `\r` is stripped), the
//!   field `\N` is NULL, and backslash escapes (`\t` `\n` `\r` `\\` …) are decoded.
//!   An embedded delimiter or newline in a value is always escaped by `COPY TO`, so
//!   a literal delimiter/newline byte is unambiguously a separator.
//! * **CSV** — delimiter-separated (default `,`), fields optionally quoted (default
//!   `"`), a doubled quote (or the escape char + quote) is a literal quote, and an
//!   *unquoted* field equal to the null string (default empty) is NULL — a quoted
//!   field never is. Delimiters and newlines inside quotes are literal.
//!
//! [STL-236]: https://allegromusic.atlassian.net/browse/STL-236

use stele_sql::{CopyFormat, CopyFormatKind};

/// The text-format end-of-data marker (`\.` on a line by itself). Protocol-3
/// clients end a `COPY` with the `CopyDone` message, not this marker, but psql can
/// still emit it, so it is honored defensively: a `\.` line stops the data.
const END_MARKER: &str = "\\.";

/// Whether a parsed record is the lone `\.` end-of-data marker — a non-allocating
/// check (comparing against `[Some(END_MARKER.to_owned())]` would allocate a
/// `String` per record).
fn is_end_marker(row: &[Option<String>]) -> bool {
    matches!(row, [Some(field)] if field == END_MARKER)
}

/// Lex a reassembled `COPY` data buffer into rows of field values under `format`.
///
/// Each row is a vector of optional field strings aligned to the COPY column list;
/// `None` is the null marker the binder turns into a SQL `NULL` cell. A `HEADER`
/// format drops the first row here.
///
/// # Errors
///
/// A short message if the buffer is not valid UTF-8 (the only encoding the engine
/// stores), if a text-format byte escape (`\NNN`/`\xHH`) decodes to a non-UTF-8
/// byte sequence, or, in CSV, if a quoted field is left unterminated at end of
/// input.
pub(crate) fn lex(data: &[u8], format: &CopyFormat) -> Result<Vec<Vec<Option<String>>>, String> {
    let text = std::str::from_utf8(data).map_err(|_| "COPY data is not valid UTF-8".to_owned())?;
    let mut rows = match format.kind {
        CopyFormatKind::Text => lex_text(text, format)?,
        CopyFormatKind::Csv => lex_csv(text, format)?,
    };
    if format.header && !rows.is_empty() {
        rows.remove(0);
    }
    Ok(rows)
}

/// Lex the **text** format: rows on `\n`, fields on the delimiter, `\N` for NULL,
/// backslash escapes decoded. An embedded delimiter/newline is always escaped by
/// `COPY TO`, so splitting on the literal byte is unambiguous.
///
/// # Errors
///
/// A short message if a field's byte escape (`\NNN`/`\xHH`) decodes to a non-UTF-8
/// byte sequence (see [`unescape_text`]).
fn lex_text(text: &str, format: &CopyFormat) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut rows = Vec::new();
    for raw_line in split_lines(text) {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line == END_MARKER {
            break;
        }
        let mut row = Vec::new();
        for field in line.split(format.delimiter) {
            let cell = if field == format.null {
                None
            } else {
                Some(unescape_text(field)?)
            };
            row.push(cell);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Split a buffer into newline-terminated lines, dropping the empty tail a final
/// `\n` leaves so `"a\n"` is one row, not two. An empty buffer is no rows.
fn split_lines(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&str> = text.split('\n').collect();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    lines
}

/// Decode the text-format backslash escapes `COPY TO` emits and return the field
/// as a UTF-8 string. The named control escapes (`\b \f \n \r \t \v`) and `\\`
/// decode to their one byte; `\NNN` (1–3 octal digits) and `\xHH` (1–2 hex digits)
/// decode to the single byte they name (mod 256, like Postgres); a bare `\x` with
/// no hex digit, and any other `\<c>`, is the literal `c` (so an escaped delimiter
/// or backslash round-trips).
///
/// Decoding is byte-wise — a byte escape can name any `0x00`–`0xFF` byte, so a run
/// of `\NNN`/`\xHH` may assemble a multi-byte UTF-8 sequence (e.g. `\303\251` →
/// `é`). The assembled bytes are then validated as UTF-8, matching Postgres's
/// per-attribute decode. Non-escape bytes (including the continuation bytes of a
/// multi-byte char) pass through verbatim.
///
/// # Errors
///
/// A short message if the decoded bytes are not valid UTF-8 (the only encoding the
/// engine stores) — e.g. a lone `\377`, which names the byte `0xFF`.
fn unescape_text(field: &str) -> Result<String, String> {
    if !field.contains('\\') {
        return Ok(field.to_owned());
    }
    let bytes = field.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        i += 1; // consume the backslash
        let Some(&next) = bytes.get(i) else {
            // A trailing lone backslash: keep it verbatim.
            out.push(b'\\');
            break;
        };
        match next {
            // `\NNN` — 1–3 octal digits name one byte; the low 3 bits left free by
            // each shift make `|` exactly Postgres's `(val << 3) + digit`, and the
            // u8 shift discards the high bit of a `\4xx`–`\7xx` value (its `& 0377`).
            b'0'..=b'7' => {
                let mut byte = 0u8;
                for _ in 0..3 {
                    match bytes.get(i) {
                        Some(&d @ b'0'..=b'7') => {
                            byte = (byte << 3) | (d - b'0');
                            i += 1;
                        }
                        _ => break,
                    }
                }
                out.push(byte);
            }
            // `\xHH` — `x` then 1–2 hex digits name one byte; a bare `\x` (no hex
            // digit follows) is the literal `x`, matching Postgres.
            b'x' => {
                i += 1; // consume the `x` marker
                if let Some(hi) = bytes.get(i).and_then(|&d| hex_val(d)) {
                    i += 1;
                    let mut byte = hi;
                    if let Some(lo) = bytes.get(i).and_then(|&d| hex_val(d)) {
                        byte = (byte << 4) | lo;
                        i += 1;
                    }
                    out.push(byte);
                } else {
                    out.push(b'x');
                }
            }
            b'b' => {
                out.push(0x08);
                i += 1;
            }
            b'f' => {
                out.push(0x0C);
                i += 1;
            }
            b'n' => {
                out.push(b'\n');
                i += 1;
            }
            b'r' => {
                out.push(b'\r');
                i += 1;
            }
            b't' => {
                out.push(b'\t');
                i += 1;
            }
            b'v' => {
                out.push(0x0B);
                i += 1;
            }
            // `\\` → `\`, and `\<anything-else>` → that byte (Postgres's fallback).
            // For a backslash before a multi-byte char this pushes only the lead
            // byte, but the continuation bytes then pass through verbatim above, so
            // the char is reassembled intact.
            _ => {
                out.push(next);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| "COPY text field is not valid UTF-8".to_owned())
}

/// The value of a single ASCII hex digit, or `None` if `b` is not one.
const fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Lex the **CSV** format with a quote-aware state machine: delimiters and
/// newlines inside quotes are literal, a doubled quote (or escape + quote) is a
/// literal quote, and an *unquoted* field equal to the null string is NULL.
fn lex_csv(text: &str, format: &CopyFormat) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut row: Vec<Option<String>> = Vec::new();
    let mut field = String::new();
    let mut quoted = false; // this field opened with a quote
    let mut in_quotes = false; // currently inside the quotes
    let mut field_started = false; // any char seen since the field began

    let mut chars = text.chars().peekable();
    // Whether anything at all has been consumed for the current (possibly final)
    // record — drives whether a trailing record with no newline is emitted.
    let mut pending = false;

    let push_field = |row: &mut Vec<Option<String>>, field: &mut String, quoted: &mut bool| {
        let value = std::mem::take(field);
        // Only an unquoted field can be the null marker; a quoted "" is "".
        let cell = if !*quoted && value == format.null {
            None
        } else {
            Some(value)
        };
        row.push(cell);
        *quoted = false;
    };

    while let Some(c) = chars.next() {
        pending = true;
        if in_quotes {
            if c == format.escape && format.escape != format.quote {
                // Escape char: the next char is taken literally.
                if let Some(n) = chars.next() {
                    field.push(n);
                }
            } else if c == format.quote {
                if chars.peek() == Some(&format.quote) {
                    // Doubled quote → one literal quote.
                    field.push(format.quote);
                    chars.next();
                } else {
                    // Closing quote.
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        // Not in quotes.
        if c == format.quote && !field_started {
            in_quotes = true;
            quoted = true;
            field_started = true;
        } else if c == format.delimiter {
            push_field(&mut row, &mut field, &mut quoted);
            field_started = false;
        } else if c == '\n' {
            // End of record. A trailing `\r` belongs to the line ending, not the
            // value (an embedded CR lives inside quotes and never reaches here).
            if field.ends_with('\r') {
                field.pop();
            }
            push_field(&mut row, &mut field, &mut quoted);
            field_started = false;
            // A lone `\.` record is the defensive end-of-data marker.
            if is_end_marker(&row) {
                return Ok(rows);
            }
            rows.push(std::mem::take(&mut row));
            pending = false;
        } else {
            field.push(c);
            field_started = true;
        }
    }

    if in_quotes {
        return Err("COPY CSV data ended inside a quoted field".to_owned());
    }
    // A final record with no trailing newline still counts.
    if pending {
        push_field(&mut row, &mut field, &mut quoted);
        if !is_end_marker(&row) {
            rows.push(row);
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_format() -> CopyFormat {
        CopyFormat::defaults(CopyFormatKind::Text)
    }
    fn csv_format() -> CopyFormat {
        CopyFormat::defaults(CopyFormatKind::Csv)
    }

    #[allow(clippy::unnecessary_wraps)] // builds the Option<String> cell shape the lexer returns
    fn s(v: &str) -> Option<String> {
        Some(v.to_owned())
    }

    #[test]
    fn text_splits_tab_rows_and_decodes_null() {
        let data = b"1\t100\n2\t\\N\n3\t300\n";
        assert_eq!(
            lex(data, &text_format()).unwrap(),
            vec![
                vec![s("1"), s("100")],
                vec![s("2"), None],
                vec![s("3"), s("300")],
            ]
        );
    }

    #[test]
    fn text_final_row_without_newline_is_kept() {
        assert_eq!(
            lex(b"1\t100", &text_format()).unwrap(),
            vec![vec![s("1"), s("100")]]
        );
    }

    #[test]
    fn text_strips_crlf_and_decodes_escapes() {
        // CRLF line ending, and an escaped tab + newline + backslash inside a value.
        let data = b"1\ta\\tb\r\n2\tc\\nd\\\\e\r\n";
        assert_eq!(
            lex(data, &text_format()).unwrap(),
            vec![vec![s("1"), s("a\tb")], vec![s("2"), s("c\nd\\e")],]
        );
    }

    #[test]
    fn text_decodes_octal_byte_escapes() {
        // 1–3 octal digits name one byte (mod 256, like Postgres); decoding stops
        // at the third digit or the first non-octal char.
        assert_eq!(unescape_text("\\001").unwrap(), "\u{01}");
        assert_eq!(unescape_text("\\1").unwrap(), "\u{01}");
        assert_eq!(unescape_text("\\12").unwrap(), "\n"); // octal 12 == 0x0A
        assert_eq!(unescape_text("\\101").unwrap(), "A"); // octal 101 == 0x41
        assert_eq!(unescape_text("\\0011").unwrap(), "\u{01}1"); // 3 digits, then '1'
        assert_eq!(unescape_text("a\\176b").unwrap(), "a~b"); // octal 176 == 0x7E
    }

    #[test]
    fn text_decodes_hex_byte_escapes() {
        // `\x` then 1–2 hex digits (either case) name one byte; a bare `\x` (or `\x`
        // not followed by a hex digit) is the literal `x`, matching Postgres.
        assert_eq!(unescape_text("\\x41").unwrap(), "A");
        assert_eq!(unescape_text("\\x4").unwrap(), "\u{04}");
        assert_eq!(unescape_text("\\x4a").unwrap(), "J"); // 0x4A
        assert_eq!(unescape_text("\\x4A").unwrap(), "J"); // upper-case digit
        assert_eq!(unescape_text("\\x411").unwrap(), "A1"); // 2 digits, then '1'
        assert_eq!(unescape_text("\\x").unwrap(), "x"); // bare marker → literal
        assert_eq!(unescape_text("\\xg").unwrap(), "xg"); // no hex digit → literal
    }

    #[test]
    fn text_byte_escapes_assemble_multibyte_utf8() {
        // A run of byte escapes builds a multi-byte UTF-8 sequence: 0xC3 0xA9 == "é".
        assert_eq!(unescape_text("\\303\\251").unwrap(), "é");
        assert_eq!(unescape_text("\\xc3\\xa9").unwrap(), "é");
        // Mixed with literal text around it.
        assert_eq!(unescape_text("<\\303\\251>").unwrap(), "<é>");
    }

    #[test]
    fn text_named_and_fallback_escapes_unchanged() {
        // The named control escapes and the literal-char fallback still decode as
        // before; `\X` (upper-case) is not a hex marker, so it is the literal 'X'.
        assert_eq!(
            unescape_text("\\b\\f\\n\\r\\t\\v").unwrap(),
            "\u{08}\u{0C}\n\r\t\u{0B}"
        );
        assert_eq!(unescape_text("a\\\\b").unwrap(), "a\\b"); // `\\` → `\`
        assert_eq!(unescape_text("a\\.b").unwrap(), "a.b"); // `\<other>` → other
        assert_eq!(unescape_text("a\\").unwrap(), "a\\"); // trailing lone backslash
        assert_eq!(unescape_text("\\X41").unwrap(), "X41"); // `\X` is literal 'X'
    }

    #[test]
    fn text_row_decodes_octal_and_hex_fields() {
        // The byte escapes flow through the full row lexer (hex 0x41='A', octal 102='B').
        let data = b"1\t\\x41\\102\n";
        assert_eq!(
            lex(data, &text_format()).unwrap(),
            vec![vec![s("1"), s("AB")]]
        );
    }

    #[test]
    fn text_byte_escape_to_invalid_utf8_is_an_error() {
        // `\377` names the lone byte 0xFF, which is not valid UTF-8 — the field
        // fails validation, matching Postgres rejecting the bad byte sequence.
        assert!(lex(b"1\t\\377\n", &text_format()).is_err());
    }

    #[test]
    fn text_empty_buffer_is_no_rows() {
        assert_eq!(
            lex(b"", &text_format()).unwrap(),
            Vec::<Vec<Option<String>>>::new()
        );
    }

    #[test]
    fn text_dot_marker_ends_data() {
        let data = b"1\t100\n\\.\n2\t200\n";
        assert_eq!(
            lex(data, &text_format()).unwrap(),
            vec![vec![s("1"), s("100")]]
        );
    }

    #[test]
    fn csv_splits_commas_and_empty_is_null() {
        // Unquoted empty field is NULL; quoted empty field is the empty string.
        let data = b"1,100\n2,\n3,\"\"\n";
        assert_eq!(
            lex(data, &csv_format()).unwrap(),
            vec![
                vec![s("1"), s("100")],
                vec![s("2"), None],
                vec![s("3"), s("")],
            ]
        );
    }

    #[test]
    fn csv_quoted_field_keeps_delimiters_newlines_and_doubled_quotes() {
        // A quoted value containing a comma, an embedded newline, and a doubled
        // quote ("") that decodes to one quote.
        let data = b"1,\"a,b\nc\"\n2,\"he said \"\"hi\"\"\"\n";
        assert_eq!(
            lex(data, &csv_format()).unwrap(),
            vec![vec![s("1"), s("a,b\nc")], vec![s("2"), s("he said \"hi\"")],]
        );
    }

    #[test]
    fn csv_header_is_skipped() {
        let mut fmt = csv_format();
        fmt.header = true;
        let data = b"id,balance\n1,100\n2,200\n";
        assert_eq!(
            lex(data, &fmt).unwrap(),
            vec![vec![s("1"), s("100")], vec![s("2"), s("200")]]
        );
    }

    #[test]
    fn csv_custom_delimiter_and_null() {
        let mut fmt = csv_format();
        fmt.delimiter = '|';
        fmt.null = "NULL".to_owned();
        let data = b"1|100\n2|NULL\n";
        assert_eq!(
            lex(data, &fmt).unwrap(),
            vec![vec![s("1"), s("100")], vec![s("2"), None]]
        );
    }

    #[test]
    fn csv_unterminated_quote_is_an_error() {
        let data = b"1,\"oops\n";
        assert!(lex(data, &csv_format()).is_err());
    }

    #[test]
    fn invalid_utf8_is_an_error() {
        assert!(lex(&[0xff, 0xfe], &text_format()).is_err());
    }

    #[test]
    fn csv_final_row_without_newline_is_kept() {
        assert_eq!(
            lex(b"1,100", &csv_format()).unwrap(),
            vec![vec![s("1"), s("100")]]
        );
    }
}
