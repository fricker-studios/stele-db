//! SQL syntax highlighting for the shell's live input line ([STL-198]).
//!
//! A small hand-rolled scanner (no regex dependency) matching the prototype's
//! tokenizer: line comments, `'…'` strings with `''` escapes, numbers, words
//! classified against the keyword/function/type lists, operator runs, and a
//! special case for `\meta` command lines.

use crate::theme::{Role, Seg};

/// SQL keywords — rendered in the bold Lapis keyword color.
const KEYWORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "insert",
    "into",
    "values",
    "update",
    "set",
    "delete",
    "create",
    "drop",
    "table",
    "primary",
    "key",
    "with",
    "system",
    "versioning",
    "for",
    "system_time",
    "as",
    "of",
    "between",
    "and",
    "or",
    "not",
    "null",
    "order",
    "by",
    "limit",
    "desc",
    "asc",
    "begin",
    "commit",
    "rollback",
    "period",
];

/// Functions and type names — both rendered in the function color.
const FUNCS_AND_TYPES: &[&str] = &[
    "now",
    "interval",
    "count",
    "sum",
    "min",
    "max",
    "avg",
    "hash",
    "int",
    "integer",
    "bigint",
    "text",
    "varchar",
    "bool",
    "boolean",
    "timestamp",
    "timestamptz",
    "tstzrange",
    "date",
    "uuid",
    "bytea",
];

/// Characters that form operator/punctuation runs (rendered dim).
const fn is_op(c: char) -> bool {
    matches!(
        c,
        ':' | '('
            | ')'
            | ','
            | '.'
            | ';'
            | '-'
            | '+'
            | '*'
            | '/'
            | '%'
            | '='
            | '<'
            | '>'
            | '!'
            | '|'
    )
}

/// Segments for a `\meta` command line: leading whitespace plain, the
/// `\command` word in accent, the remainder muted.
fn meta_segments(line: &str, trimmed: &str) -> Vec<Seg> {
    let lead = &line[..line.len() - trimmed.len()];
    let cmd_len = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
    let mut segs = Vec::new();
    if !lead.is_empty() {
        segs.push((Role::Text, lead.to_owned()));
    }
    segs.push((Role::Acc, trimmed[..cmd_len].to_owned()));
    if cmd_len < trimmed.len() {
        segs.push((Role::Mut, trimmed[cmd_len..].to_owned()));
    }
    segs
}

/// The byte index just past a `'…'` literal starting at `start`, honoring `''`
/// escapes; an unterminated literal runs to end of line.
fn string_end(line: &str, start: usize) -> usize {
    let mut end = start + 1;
    loop {
        match line[end..].find('\'') {
            None => return line.len(),
            Some(q) => {
                end += q + 1;
                if line[end..].starts_with('\'') {
                    end += 1; // '' escape, keep scanning
                } else {
                    return end;
                }
            }
        }
    }
}

/// Tokenize one input line into styled segments.
pub fn tokenize(line: &str) -> Vec<Seg> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('\\') {
        return meta_segments(line, trimmed);
    }

    let bytes = line.as_bytes();
    let mut segs: Vec<Seg> = Vec::new();
    let mut push = |role: Role, text: &str| {
        if text.is_empty() {
            return;
        }
        // Merge adjacent same-role runs so plain output stays compact.
        if let Some((last_role, last)) = segs.last_mut() {
            if *last_role == role {
                last.push_str(text);
                return;
            }
        }
        segs.push((role, text.to_owned()));
    };

    let mut i = 0;
    while i < bytes.len() {
        let rest = &line[i..];
        let c = rest.chars().next().expect("non-empty rest");

        // -- line comment swallows the remainder.
        if rest.starts_with("--") {
            push(Role::Note, rest);
            break;
        }
        // '…' string with '' escapes.
        if c == '\'' {
            let end = string_end(line, i);
            push(Role::Str, &line[i..end]);
            i = end;
            continue;
        }
        // Number: digits with an optional single decimal point.
        if c.is_ascii_digit() {
            let mut end = i;
            let mut seen_dot = false;
            for (off, ch) in rest.char_indices() {
                if ch.is_ascii_digit() {
                    end = i + off + 1;
                } else if ch == '.'
                    && !seen_dot
                    && line[i + off + 1..].starts_with(|d: char| d.is_ascii_digit())
                {
                    seen_dot = true;
                    end = i + off + 1;
                } else {
                    break;
                }
            }
            push(Role::Num, &line[i..end]);
            i = end;
            continue;
        }
        // Word: identifier / keyword / function / type.
        if c.is_alphabetic() || c == '_' {
            let end = rest
                .find(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
                .map_or(line.len(), |o| i + o);
            let word = &line[i..end];
            let lower = word.to_ascii_lowercase();
            let role = if KEYWORDS.contains(&lower.as_str()) {
                Role::Kw
            } else if FUNCS_AND_TYPES.contains(&lower.as_str()) {
                Role::Func
            } else {
                Role::Text
            };
            push(role, word);
            i = end;
            continue;
        }
        // Operator / punctuation runs.
        if is_op(c) {
            let end = rest
                .find(|ch: char| !is_op(ch))
                .map_or(line.len(), |o| i + o);
            push(Role::Dim, &line[i..end]);
            i = end;
            continue;
        }
        // Whitespace and anything else: plain.
        push(Role::Text, &line[i..i + c.len_utf8()]);
        i += c.len_utf8();
    }
    segs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roles(line: &str) -> Vec<(Role, String)> {
        tokenize(line)
    }

    #[test]
    fn keywords_strings_and_numbers_classify() {
        let segs = roles("SELECT balance FROM account WHERE id = 1;");
        assert!(segs.contains(&(Role::Kw, "SELECT".to_owned())), "{segs:?}");
        // Identifiers merge with their surrounding whitespace (same role).
        assert!(
            segs.contains(&(Role::Text, " balance ".to_owned())),
            "{segs:?}"
        );
        assert!(segs.contains(&(Role::Kw, "FROM".to_owned())), "{segs:?}");
        assert!(segs.contains(&(Role::Num, "1".to_owned())), "{segs:?}");
    }

    #[test]
    fn string_literal_with_doubled_quote_is_one_token() {
        let segs = roles("INSERT INTO t VALUES ('it''s');");
        assert!(
            segs.contains(&(Role::Str, "'it''s'".to_owned())),
            "{segs:?}"
        );
    }

    #[test]
    fn unterminated_string_colors_to_end_of_line() {
        let segs = roles("SELECT 'oops");
        assert_eq!(segs.last().unwrap(), &(Role::Str, "'oops".to_owned()));
    }

    #[test]
    fn line_comment_swallows_the_rest() {
        let segs = roles("SELECT 1 -- the answer; SELECT 2");
        assert_eq!(
            segs.last().unwrap(),
            &(Role::Note, "-- the answer; SELECT 2".to_owned())
        );
    }

    #[test]
    fn meta_command_line_splits_command_and_args() {
        assert_eq!(
            roles(r"\d account"),
            vec![
                (Role::Acc, r"\d".to_owned()),
                (Role::Mut, " account".to_owned()),
            ]
        );
    }

    #[test]
    fn functions_and_types_use_the_function_color() {
        let segs = roles("SELECT now() FROM t");
        assert!(segs.contains(&(Role::Func, "now".to_owned())), "{segs:?}");
        let segs = roles("CREATE TABLE t (a INT)");
        assert!(segs.contains(&(Role::Func, "INT".to_owned())), "{segs:?}");
    }

    #[test]
    fn reassembled_segments_reproduce_the_input() {
        for line in [
            "SELECT id, balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second');",
            r"  \timing on",
            "UPDATE t SET v = 1.5 WHERE s = 'x''y' -- tail",
            "",
        ] {
            let joined: String = tokenize(line).into_iter().map(|(_, t)| t).collect();
            assert_eq!(joined, line);
        }
    }
}
