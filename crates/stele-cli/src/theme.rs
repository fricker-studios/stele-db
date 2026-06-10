//! Datum-brand ANSI theming for `stele shell` ([STL-198]).
//!
//! The semantic roles and exact colors come from the locked design prototype
//! (`.ai/prototypes/stele-cli`, the `shell.css` dark "ink" theme — one Lapis
//! accent, warm warn-gold for errors, **no red**). Only foreground colors are
//! painted; the user's terminal keeps its own background.
//!
//! Color is opt-out at three levels, in order: not a TTY (scripted sessions
//! stay byte-clean — the STL-185 integration tests depend on it), `NO_COLOR`
//! (the <https://no-color.org> convention), `TERM=dumb`. Truecolor is used when
//! `COLORTERM` advertises it; otherwise the 256-color approximation.
//!
//! [STL-198]: https://allegromusic.atlassian.net/browse/STL-198

/// A semantic color role from the prototype's `c-*` classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Primary prompt `stele=# ` — Lapis accent, bold.
    Prompt,
    /// Continuation prompt `stele-# ` — dimmed accent.
    Cont,
    /// Plain text / identifiers (the default ink).
    Text,
    /// Operators, punctuation, faint detail.
    Dim,
    /// Muted messages, the row-count line.
    Mut,
    /// Table headers and titles — ink, bold.
    Head,
    /// Table borders and rules.
    Div,
    /// Warnings (and the brand's error gold).
    Warn,
    /// Success — checkmarks, healthy dots.
    Ok,
    /// Accent text — meta-command names, hosts, AS-OF values.
    Acc,
    /// Function and type tokens.
    Func,
    /// String literals.
    Str,
    /// Numeric literals and hashes.
    Num,
    /// SQL keywords — accent, bold.
    Kw,
    /// `ERROR:` text — bold warn-gold (deliberately not red).
    Err,
    /// `HINT:` text — dimmed accent.
    Hint,
    /// `NOTICE:` / annotations — muted italic.
    Note,
    /// Banner body text.
    Banner,
}

/// `(r, g, b, 256-index, bold, italic)` per role — the dark "ink" palette.
const fn style(role: Role) -> (u8, u8, u8, u8, bool, bool) {
    match role {
        // Keywords share the bold Lapis prompt styling.
        Role::Prompt | Role::Kw => (111, 155, 239, 75, true, false), // #6f9bef Lapis
        Role::Cont | Role::Hint => (82, 116, 184, 67, false, false), // #5274b8 accent-dim
        Role::Text => (232, 234, 240, 255, false, false),            // #e8eaf0 ink
        // Borders use the faint ink — a legible stand-in for the prototype's
        // near-background #2a2f3a, which vanishes on non-brand backgrounds.
        Role::Dim | Role::Div => (90, 97, 109, 241, false, false), // #5a616d faint
        Role::Mut => (134, 141, 153, 245, false, false),           // #868d99 muted
        Role::Head => (232, 234, 240, 255, true, false),           // ink, bold
        Role::Warn => (214, 168, 95, 179, false, false),           // #d6a85f
        Role::Ok => (111, 180, 148, 72, false, false),             // #6fb494
        Role::Acc => (111, 155, 239, 75, false, false),            // #6f9bef
        Role::Func => (147, 167, 216, 110, false, false),          // #93a7d8
        Role::Str => (147, 185, 163, 109, false, false),           // #93b9a3
        Role::Num => (211, 171, 116, 180, false, false),           // #d3ab74
        Role::Err => (214, 168, 95, 179, true, false),             // bold warn — no red
        Role::Note => (134, 141, 153, 245, false, true),           // muted, italic
        Role::Banner => (189, 195, 206, 251, false, false),        // #bdc3ce ink-2
    }
}

/// How (and whether) to emit color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorMode {
    /// No escapes at all — output is byte-identical to the plain text.
    Plain,
    /// `\x1b[38;5;N m` indexed color.
    Indexed,
    /// `\x1b[38;2;R;G;B m` 24-bit color.
    True,
}

/// A resolved theme: paints text per [`Role`] in the detected [`ColorMode`].
#[derive(Debug, Clone, Copy)]
pub struct Theme {
    mode: ColorMode,
}

impl Theme {
    /// A theme that never emits escapes.
    pub const fn plain() -> Self {
        Self {
            mode: ColorMode::Plain,
        }
    }

    /// Resolve the color mode for a stream that is (or is not) a terminal,
    /// honoring `NO_COLOR` and `TERM=dumb`, preferring truecolor when
    /// `COLORTERM` advertises it.
    pub fn detect(is_tty: bool) -> Self {
        let mode = if !is_tty
            || std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
            || std::env::var_os("TERM").is_some_and(|t| t == "dumb")
        {
            ColorMode::Plain
        } else if std::env::var("COLORTERM")
            .is_ok_and(|v| v.contains("truecolor") || v.contains("24bit"))
        {
            ColorMode::True
        } else {
            ColorMode::Indexed
        };
        Self { mode }
    }

    /// Whether this theme emits any escapes at all.
    pub const fn colored(self) -> bool {
        !matches!(self.mode, ColorMode::Plain)
    }

    /// Paint `text` in `role` (identity in plain mode; empty stays empty).
    pub fn paint(self, role: Role, text: &str) -> String {
        if !self.colored() || text.is_empty() {
            return text.to_owned();
        }
        let (r, g, b, idx, bold, italic) = style(role);
        let mut sgr = String::new();
        if bold {
            sgr.push_str("1;");
        }
        if italic {
            sgr.push_str("3;");
        }
        match self.mode {
            ColorMode::True => format!("\x1b[{sgr}38;2;{r};{g};{b}m{text}\x1b[0m"),
            ColorMode::Indexed => format!("\x1b[{sgr}38;5;{idx}m{text}\x1b[0m"),
            ColorMode::Plain => unreachable!("guarded by colored()"),
        }
    }
}

/// One styled run of text.
pub type Seg = (Role, String);

/// Render a line of segments through the theme (no trailing newline).
pub fn paint_segs(theme: Theme, segs: &[Seg]) -> String {
    segs.iter()
        .map(|(role, text)| theme.paint(*role, text))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_theme_is_byte_identical() {
        let t = Theme::plain();
        assert_eq!(t.paint(Role::Err, "ERROR:  boom"), "ERROR:  boom");
        assert!(!t.colored());
    }

    #[test]
    fn truecolor_paints_lapis_prompt_bold() {
        let t = Theme {
            mode: ColorMode::True,
        };
        assert_eq!(
            t.paint(Role::Prompt, "stele=# "),
            "\x1b[1;38;2;111;155;239mstele=# \x1b[0m"
        );
    }

    #[test]
    fn indexed_mode_uses_the_256_fallback() {
        let t = Theme {
            mode: ColorMode::Indexed,
        };
        assert_eq!(t.paint(Role::Ok, "✓"), "\x1b[38;5;72m✓\x1b[0m");
    }

    #[test]
    fn empty_text_never_gains_escapes() {
        let t = Theme {
            mode: ColorMode::True,
        };
        assert_eq!(t.paint(Role::Kw, ""), "");
    }

    #[test]
    fn segments_concatenate_in_order() {
        let t = Theme::plain();
        let line = paint_segs(
            t,
            &[
                (Role::Mut, "Type ".to_owned()),
                (Role::Acc, "\\?".to_owned()),
                (Role::Mut, " for help".to_owned()),
            ],
        );
        assert_eq!(line, "Type \\? for help");
    }
}
