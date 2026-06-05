# Stele task runner — wraps cargo with the same gates CI runs.
# `just check` is the one command to run before pushing.

default: dev

# Run the engine in dev mode (verbose tracing, no auth, :5454).
dev *args:
    cargo run -p stele-server -- {{args}}

# Compile the whole workspace.
build:
    cargo build --workspace --all-targets

# Unit + integration + doctests. Uses nextest if installed, falls back to `cargo test`.
test:
    cargo nextest run --workspace --all-features 2>/dev/null || cargo test --workspace --all-features
    cargo test --doc --workspace --all-features

# Auto-format the tree.
fmt:
    cargo fmt --all

# fmt-check + clippy (warnings = errors) + typos. Mirrors the CI `quick` job.
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    # Spelling, same as CI's `quick` job. Run strictly when `typos` is installed;
    # otherwise note it (so a missing tool doesn't mask real failures, and CI
    # still catches them). Install with: cargo install --locked typos-cli
    if command -v typos >/dev/null 2>&1; then typos; else echo "note: typos-cli not installed — skipping (CI runs it)"; fi

# Rustdoc build with warnings denied. Mirrors the CI `docs build` job — a
# broken intra-doc link or bad doc comment fails CI even when tests pass.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Pre-push gate — mirrors the CI quick + test + docs jobs (the per-PR required
# checks runnable with just the pinned toolchain). The MSRV build and
# cargo-deny run as their own CI jobs; use `just deny` for the latter.
check: lint test doc

# Deterministic simulation seeds with fault injection.
sim seeds="100":
    cargo run -p stele-sim --release -- --seeds {{seeds}} --fault-injection on

# Reproduce one failure deterministically.
sim-seed seed:
    cargo run -p stele-sim --release -- --seed {{seed}}

# Time-boxed fuzz target.
fuzz target:
    cargo +nightly fuzz run {{target}}

# Criterion benchmarks.
bench:
    cargo bench --workspace

# Supply-chain checks (licenses, bans, advisories).
deny:
    cargo deny check

# Invoke the stele CLI passing trailing args through.
cli *args:
    cargo run -p stele-cli -- {{args}}

# Build the canonical Docker image.
docker-build:
    docker build -f docker/Dockerfile -t stele:dev .
