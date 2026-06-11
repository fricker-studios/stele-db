//! Result rendering for `stele shell` ([STL-198]) — the four table border
//! styles, psql-style expanded records (`\x`), and JSON output (`\json`),
//! exactly as the design prototype's `render.js` draws them.
//!
//! Everything returns styled segment lines ([`Seg`]); the caller paints them
//! through the [`Theme`](crate::theme::Theme), which is the identity when the
//! session is piped — so scripted output stays plain text.

use crate::theme::{Role, Seg};

/// A result column: wire name plus the Postgres type OID from the
/// `RowDescription`, which decides numeric right-alignment and JSON quoting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
}

impl Column {
    /// Numeric types right-align (int2/int4/int8, float4/float8, numeric).
    const fn right_align(&self) -> bool {
        matches!(self.type_oid, 20 | 21 | 23 | 700 | 701 | 1700)
    }

    /// Booleans (`t`/`f` on the wire) become JSON `true`/`false`.
    const fn is_bool(&self) -> bool {
        self.type_oid == 16
    }
}

/// Result-table border style (`--border`, default `psql`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum BorderStyle {
    /// psql-compatible ASCII: ` a | b ` over `---+---`.
    #[default]
    Psql,
    /// Full box-drawing frame.
    Unicode,
    /// GitHub-flavored markdown table.
    Markdown,
    /// Borderless two-space layout with a thin header rule.
    Clean,
}

/// Rendering options for one result table.
#[derive(Debug, Clone, Copy)]
pub struct TableOpts {
    pub style: BorderStyle,
    /// Prepend a 1-based `#` column.
    pub row_nums: bool,
    /// Append the `(N rows)` trailer.
    pub count: bool,
}

/// A rendered line: styled segments, no trailing newline.
pub type Line = Vec<Seg>;

/// Flatten rows into display cells — escaped, with the optional 1-based `#`
/// column materialized so widths/alignment treat it like any other column.
fn display_cells(ncols: usize, rows: &[Vec<Option<String>>], row_nums: bool) -> Vec<Vec<String>> {
    rows.iter()
        .enumerate()
        .map(|(i, row)| {
            let mut out = Vec::with_capacity(ncols + usize::from(row_nums));
            if row_nums {
                out.push((i + 1).to_string());
            }
            out.extend((0..ncols).map(|c| {
                row.get(c)
                    .and_then(Option::as_deref)
                    .map(display_cell)
                    .unwrap_or_default()
            }));
            out
        })
        .collect()
}

/// Render a result set as an aligned table.
pub fn table_lines(columns: &[Column], rows: &[Vec<Option<String>>], opts: TableOpts) -> Vec<Line> {
    let mut cols: Vec<(String, bool)> = Vec::new();
    if opts.row_nums {
        cols.push(("#".to_owned(), true));
    }
    cols.extend(columns.iter().map(|c| (c.name.clone(), c.right_align())));
    let cells = display_cells(columns.len(), rows, opts.row_nums);

    let widths: Vec<usize> = cols
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            cells
                .iter()
                .map(|row| width_of(&row[i]))
                .chain([width_of(name).max(1)])
                .max()
                .unwrap_or(1)
        })
        .collect();

    let pad = |text: &str, width: usize, right: bool| -> String {
        let fill = width.saturating_sub(width_of(text));
        if right {
            format!("{}{text}", " ".repeat(fill))
        } else {
            format!("{text}{}", " ".repeat(fill))
        }
    };
    // Headers are always left-aligned; only data cells honor numeric alignment.
    let header: Vec<String> = cols
        .iter()
        .zip(&widths)
        .map(|((name, _), w)| pad(name, *w, false))
        .collect();
    let data: Vec<Vec<String>> = cells
        .iter()
        .map(|row| {
            row.iter()
                .zip(&cols)
                .zip(&widths)
                .map(|((cell, (_, right)), w)| pad(cell, *w, *right))
                .collect()
        })
        .collect();

    let mut lines: Vec<Line> = Vec::new();
    let rule = |l: &str, m: &str, r: &str, dash: &str, extra: usize| -> String {
        let body: Vec<String> = widths.iter().map(|w| dash.repeat(w + extra)).collect();
        format!("{l}{}{r}", body.join(m))
    };
    match opts.style {
        BorderStyle::Psql => {
            lines.push(vec![(Role::Head, format!(" {} ", header.join(" | ")))]);
            lines.push(vec![(Role::Div, rule("", "+", "", "-", 2))]);
            for row in &data {
                lines.push(vec![(Role::Text, format!(" {} ", row.join(" | ")))]);
            }
        }
        BorderStyle::Unicode => {
            lines.push(vec![(Role::Div, rule("┌", "┬", "┐", "─", 2))]);
            lines.push(vec![(Role::Head, format!("│ {} │", header.join(" │ ")))]);
            lines.push(vec![(Role::Div, rule("├", "┼", "┤", "─", 2))]);
            for row in &data {
                lines.push(vec![(Role::Text, format!("│ {} │", row.join(" │ ")))]);
            }
            lines.push(vec![(Role::Div, rule("└", "┴", "┘", "─", 2))]);
        }
        BorderStyle::Markdown => {
            lines.push(vec![(Role::Head, format!("| {} |", header.join(" | ")))]);
            lines.push(vec![(Role::Div, rule("| ", " | ", " |", "-", 0))]);
            for row in &data {
                lines.push(vec![(Role::Text, format!("| {} |", row.join(" | ")))]);
            }
        }
        BorderStyle::Clean => {
            lines.push(vec![(Role::Head, format!("  {}", header.join("   ")))]);
            lines.push(vec![(
                Role::Div,
                format!("  {}", rule("", "   ", "", "─", 0)),
            )]);
            for row in &data {
                lines.push(vec![(Role::Text, format!("  {}", row.join("   ")))]);
            }
        }
    }
    if opts.count {
        lines.push(vec![(Role::Mut, count_line(rows.len()))]);
    }
    lines
}

/// psql-style expanded records (`\x`): one `-[ RECORD N ]…` divider per row,
/// then `name | value` per field.
pub fn expanded_lines(columns: &[Column], rows: &[Vec<Option<String>>]) -> Vec<Line> {
    let w = columns.iter().map(|c| width_of(&c.name)).max().unwrap_or(0);
    let mut lines: Vec<Line> = Vec::new();
    for (ri, row) in rows.iter().enumerate() {
        let hdr = format!("-[ RECORD {} ]", ri + 1);
        // `+` sits at column w+1, directly over the field lines' `|`.
        let fill = (w + 1).saturating_sub(hdr.chars().count());
        lines.push(vec![(
            Role::Div,
            format!("{hdr}{}+{}", "-".repeat(fill), "-".repeat(22)),
        )]);
        for (ci, col) in columns.iter().enumerate() {
            let value = row
                .get(ci)
                .and_then(Option::as_deref)
                .map(display_cell)
                .unwrap_or_default();
            let fill = w.saturating_sub(width_of(&col.name));
            lines.push(vec![
                (Role::Mut, format!("{}{} ", col.name, " ".repeat(fill))),
                (Role::Dim, "| ".to_owned()),
                (Role::Text, value),
            ]);
        }
    }
    lines.push(vec![(Role::Mut, count_line(rows.len()))]);
    lines
}

/// JSON output (`\json`): one array of objects, two-space indent. `NULL` →
/// `null`; numeric and boolean columns emit unquoted when their wire text is
/// valid JSON for that type, falling back to a string otherwise.
pub fn json_lines(columns: &[Column], rows: &[Vec<Option<String>>]) -> Vec<Line> {
    let mut out = String::from("[");
    for (ri, row) in rows.iter().enumerate() {
        out.push_str(if ri == 0 { "\n" } else { ",\n" });
        out.push_str("  {");
        for (ci, col) in columns.iter().enumerate() {
            if ci > 0 {
                out.push_str(", ");
            }
            out.push_str(&json_string(&col.name));
            out.push_str(": ");
            out.push_str(&json_value(col, row.get(ci).and_then(Option::as_deref)));
        }
        out.push('}');
    }
    if !rows.is_empty() {
        out.push('\n');
    }
    out.push(']');
    out.lines()
        .map(|l| vec![(Role::Text, l.to_owned())])
        .collect()
}

/// One JSON cell value per the column's wire type.
fn json_value(col: &Column, cell: Option<&str>) -> String {
    let Some(text) = cell else {
        return "null".to_owned();
    };
    if col.is_bool() {
        match text {
            "t" => return "true".to_owned(),
            "f" => return "false".to_owned(),
            _ => {}
        }
    }
    if col.right_align() && text.parse::<f64>().is_ok_and(f64::is_finite) {
        return text.to_owned();
    }
    json_string(text)
}

/// Minimal JSON string escaping (quotes, backslash, control characters).
fn json_string(text: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                // Writing to a `String` is infallible.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The `(N rows)` trailer, singular for one row.
fn count_line(n: usize) -> String {
    format!("({n} row{})", if n == 1 { "" } else { "s" })
}

/// Terminal display width — CJK/emoji occupy two columns, combining marks
/// zero — so padded frames stay aligned for any cell text.
fn width_of(s: &str) -> usize {
    unicode_width::UnicodeWidthStr::width(s)
}

/// A cell as displayed in an aligned frame: control characters (an embedded
/// newline in a TEXT value, a tab, …) are escaped so a single wire cell can
/// never split or skew a bordered row. JSON mode escapes on its own; the raw
/// value always round-trips over the wire untouched.
fn display_cell(text: &str) -> String {
    if !text.chars().any(char::is_control) {
        return text.to_owned();
    }
    text.chars()
        .flat_map(|c| {
            if c.is_control() {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, oid: u32) -> Column {
        Column {
            name: name.to_owned(),
            type_oid: oid,
        }
    }

    fn text(lines: &[Line]) -> String {
        lines
            .iter()
            .map(|segs| segs.iter().map(|(_, t)| t.as_str()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn sample() -> (Vec<Column>, Vec<Vec<Option<String>>>) {
        (
            vec![col("id", 23), col("name", 25)],
            vec![
                vec![Some("1".to_owned()), Some("alice".to_owned())],
                vec![Some("20".to_owned()), None],
            ],
        )
    }

    const fn opts(style: BorderStyle) -> TableOpts {
        TableOpts {
            style,
            row_nums: false,
            count: true,
        }
    }

    #[test]
    fn psql_style_right_aligns_numerics_and_blanks_null() {
        let (c, r) = sample();
        assert_eq!(
            text(&table_lines(&c, &r, opts(BorderStyle::Psql))),
            " id | name  \n----+-------\n  1 | alice \n 20 |       \n(2 rows)"
        );
    }

    #[test]
    fn unicode_style_draws_the_full_frame() {
        let (c, r) = sample();
        assert_eq!(
            text(&table_lines(&c, &r, opts(BorderStyle::Unicode))),
            "┌────┬───────┐\n│ id │ name  │\n├────┼───────┤\n│  1 │ alice │\n│ 20 │       │\n└────┴───────┘\n(2 rows)"
        );
    }

    #[test]
    fn markdown_style_uses_width_dashes() {
        let (c, r) = sample();
        assert_eq!(
            text(&table_lines(&c, &r, opts(BorderStyle::Markdown))),
            "| id | name  |\n| -- | ----- |\n|  1 | alice |\n| 20 |       |\n(2 rows)"
        );
    }

    #[test]
    fn clean_style_is_borderless_with_a_header_rule() {
        let (c, r) = sample();
        assert_eq!(
            text(&table_lines(&c, &r, opts(BorderStyle::Clean))),
            "  id   name \n  ──   ─────\n   1   alice\n  20        ".to_owned() + "\n(2 rows)"
        );
    }

    #[test]
    fn row_numbers_prepend_a_right_aligned_hash_column() {
        let (c, r) = sample();
        let rendered = text(&table_lines(
            &c,
            &r,
            TableOpts {
                style: BorderStyle::Psql,
                row_nums: true,
                count: true,
            },
        ));
        assert!(rendered.starts_with(" # | id | name  \n"), "{rendered}");
        assert!(rendered.contains("\n 1 |  1 | alice \n"), "{rendered}");
    }

    #[test]
    fn singular_row_count() {
        let (c, _) = sample();
        let one = vec![vec![Some("1".to_owned()), Some("x".to_owned())]];
        assert!(text(&table_lines(&c, &one, opts(BorderStyle::Psql))).ends_with("(1 row)"));
    }

    #[test]
    fn expanded_records_match_psql_shape() {
        let (c, r) = sample();
        let rendered = text(&expanded_lines(&c, &r));
        assert_eq!(
            rendered,
            "-[ RECORD 1 ]+----------------------\nid   | 1\nname | alice\n-[ RECORD 2 ]+----------------------\nid   | 20\nname | \n(2 rows)"
        );
    }

    #[test]
    fn expanded_divider_plus_sits_over_the_field_pipe() {
        // With a name wider than the record header, `+` must land at column
        // w+1 — exactly over the `|` of the field lines (psql geometry).
        let cols = vec![col("system_time_col", 25)];
        let rows = vec![vec![Some("x".to_owned())]];
        let rendered = text(&expanded_lines(&cols, &rows));
        let lines: Vec<&str> = rendered.lines().collect();
        let plus = lines[0].find('+').expect("divider has +");
        let pipe = lines[1].find('|').expect("field has |");
        assert_eq!(plus, pipe, "{rendered}");
    }

    #[test]
    fn wide_glyph_cells_pad_by_display_width() {
        // "日本語" is 3 chars but 6 terminal columns; the frame must not skew.
        let cols = vec![col("t", 25)];
        let rows = vec![
            vec![Some("日本語".to_owned())],
            vec![Some("abcdef".to_owned())],
        ];
        let rendered = text(&table_lines(&cols, &rows, opts(BorderStyle::Unicode)));
        let widths: Vec<usize> = rendered
            .lines()
            .filter(|l| l.starts_with('│'))
            .map(unicode_width::UnicodeWidthStr::width)
            .collect();
        assert!(!widths.is_empty());
        assert!(
            widths.windows(2).all(|w| w[0] == w[1]),
            "frame rows differ in display width: {rendered}"
        );
    }

    #[test]
    fn control_characters_in_cells_are_escaped_not_framed_raw() {
        let cols = vec![col("v", 25)];
        let rows = vec![vec![Some("line one\nline two".to_owned())]];
        let rendered = text(&table_lines(&cols, &rows, opts(BorderStyle::Psql)));
        // One physical data row, newline rendered as the \n escape.
        assert!(rendered.contains("line one\\nline two"), "{rendered}");
        assert_eq!(rendered.lines().count(), 4, "{rendered}"); // header + rule + row + count
    }

    #[test]
    fn json_emits_typed_values_and_null() {
        let (c, r) = sample();
        assert_eq!(
            text(&json_lines(&c, &r)),
            "[\n  {\"id\": 1, \"name\": \"alice\"},\n  {\"id\": 20, \"name\": null}\n]"
        );
    }

    #[test]
    fn json_escapes_strings_and_handles_bool_and_empty() {
        let cols = vec![col("ok", 16), col("note", 25)];
        let rows = vec![vec![Some("t".to_owned()), Some("a\"b\\c\n".to_owned())]];
        assert_eq!(
            text(&json_lines(&cols, &rows)),
            "[\n  {\"ok\": true, \"note\": \"a\\\"b\\\\c\\n\"}\n]"
        );
        assert_eq!(text(&json_lines(&cols, &[])), "[]");
    }
}
