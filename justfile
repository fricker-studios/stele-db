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
# `stele-exec-oracle` is excluded (it compiles the bundled DuckDB amalgamation —
# minutes); run it with `just oracle`. Mirrors the per-PR CI test job (STL-158).
test:
    cargo nextest run --workspace --exclude stele-exec-oracle --all-features 2>/dev/null || cargo test --workspace --exclude stele-exec-oracle --all-features
    cargo test --doc --workspace --exclude stele-exec-oracle --all-features

# Auto-format the tree.
fmt:
    cargo fmt --all

# fmt-check + clippy (warnings = errors) + typos. Mirrors the CI `quick` job.
# fmt/clippy (and test/doc) are deterministic with just the pinned toolchain, so
# they are the "green locally ⇒ green in CI" core. Spelling is **best-effort**:
# `typos` needs a separate install, so when it is absent this prints a note and
# moves on rather than forcing every contributor to install it — CI's `quick`
# job still runs it, so a typo can only land red there, never silently merge.
lint:
    cargo fmt --all --check
    cargo clippy --workspace --exclude stele-exec-oracle --all-targets --all-features -- -D warnings
    # Strict when installed (real failures propagate); a note when not.
    # Install to match CI with: cargo install --locked --version 1.39.0 typos-cli
    if command -v typos >/dev/null 2>&1; then typos; else echo "note: typos-cli not installed — skipping (best-effort; CI's quick job runs it)"; fi

# Rustdoc build with warnings denied. Mirrors the CI `docs build` job — a
# broken intra-doc link or bad doc comment fails CI even when tests pass.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Pre-push gate — mirrors the CI quick + test + docs jobs (the per-PR required
# checks runnable with just the pinned toolchain). The MSRV build and
# cargo-deny run as their own CI jobs; use `just deny` for the latter.
check: lint test doc

# The DuckDB differential oracle (STL-144) — excluded from `just check` because
# it compiles the bundled DuckDB C++ amalgamation (minutes). This is the only
# command that builds `stele-exec-oracle`; CI runs it in the nightly gate (STL-158).
oracle:
    cargo nextest run -p stele-exec-oracle --all-features 2>/dev/null || cargo test -p stele-exec-oracle

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

# Five-minute-path smoke test (STL-112): build the image, run the engine, drive
# the four-statement identity demo over pg-wire, assert the AS OF query returns
# 100. Mirrors the CI `five-minute path` job. Needs Docker + psql.
docker-smoke: docker-build
    docker rm -f stele-smoke >/dev/null 2>&1 || true
    docker run -d --name stele-smoke -p 5454:5454 stele:dev --dev
    ci/identity-demo-smoke.sh localhost 5454; status=$?; docker rm -f stele-smoke >/dev/null 2>&1 || true; exit $status
