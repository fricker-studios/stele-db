//! Build script: capture the short git commit at compile time so `stele version`
//! can report the exact source the binary was built from.
//!
//! Falls back to `unknown` whenever git or the `.git` metadata is unavailable —
//! e.g. building from a source tarball or inside a minimal Docker stage. The
//! commit is exposed to the crate as the `STELE_GIT_COMMIT` env var, read with
//! `env!` in `main.rs`.

use std::process::Command;

fn main() {
    let commit = git_short_commit().unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=STELE_GIT_COMMIT={commit}");

    // Re-run when HEAD moves so the embedded commit stays in sync with the tree.
    // `--git-path` resolves correctly for both plain checkouts and worktrees;
    // when git is absent the commit is already `unknown`, so tracking is moot.
    for file in ["HEAD", "logs/HEAD"] {
        if let Some(path) = git_path(file) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

/// `git rev-parse --short HEAD`, or `None` if git is missing / this is not a
/// checkout / the command fails.
fn git_short_commit() -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let commit = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    (!commit.is_empty()).then_some(commit)
}

/// Resolve a path inside the git dir (e.g. `HEAD`) to a concrete filesystem path
/// via `git rev-parse --git-path`, so `rerun-if-changed` works under worktrees.
fn git_path(file: &str) -> Option<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--git-path", file])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let path = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    (!path.is_empty()).then_some(path)
}
