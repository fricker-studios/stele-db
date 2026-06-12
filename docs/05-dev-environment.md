# 05 — Development Environment

> **Status:** Founding dev-experience plan. Paths/commands describe the *intended* layout; they become real as the workspace lands.
> **Read with:** [04 — CI/CD](04-cicd.md) (same toolchain, in CI) · [06 — Testing Strategy](06-testing-strategy.md) (how to run the suites) · [ADR-0005](adr/0005-reproducible-builds-pinned-toolchain.md).

The goal is a **"clone to a running engine in minutes"** path, with a hermetic, reproducible toolchain that matches CI exactly. A contributor should never debug their environment — only Stele.

## The five-minute path (the headline promise)

```bash
git clone https://github.com/<org>/stele-db
cd stele-db
# Option A — native (Rust toolchain auto-pinned by rust-toolchain.toml)
cargo run -p stele-server -- --dev          # starts the engine on :5454 (pg-wire)

# in another shell — connect with any Postgres client:
psql -h localhost -p 5454 -d stele          # or:  cargo run -p stele-cli -- shell
```

```sql
-- prove the identity in four statements:
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 250 WHERE id = 1;
SELECT balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1;
--   → 100   (time-travel: the value *before* the update)
```

If that works on a fresh clone, the dev environment is doing its job.

---

## Toolchain (pinned, hermetic)

| Tool | Version source | Purpose |
|---|---|---|
| **Rust** | `rust-toolchain.toml` (edition 2024, ≥ 1.89) | The compiler. Auto-installed by rustup on first `cargo` invocation. |
| **Cargo** | bundled with Rust | Build/test/run; workspace. |
| **rustfmt, clippy** | pinned via toolchain components | Format + lint (match CI). |
| **cargo-nextest** | pinned in `bootstrap` | Fast, reliable test runner. |
| **cargo-deny, cargo-audit** | pinned | Supply-chain checks. |
| **cargo-fuzz** (nightly) | pinned | Fuzz targets. |
| **just** | pinned | Task runner (thin wrapper over cargo; see below). |

`rust-toolchain.toml` is the single source of truth, so **native, devcontainer, Nix, and CI all use the same compiler:**

```toml
# rust-toolchain.toml
[toolchain]
channel    = "1.89.0"          # bumped deliberately; also Stele's MSRV
components = ["rustfmt", "clippy", "rust-src"]
profile    = "minimal"
```

---

## Repository layout (intended)

```
stele-db/
├── Cargo.toml                 # workspace
├── rust-toolchain.toml        # pinned compiler (= MSRV)
├── deny.toml                  # cargo-deny config (licenses, bans, advisories)
├── justfile                   # task runner entrypoints
├── flake.nix / devbox.json    # optional hermetic shells
├── .devcontainer/             # VS Code / Codespaces
├── .github/workflows/         # CI/CD ([04])
├── docker/                    # canonical Dockerfile(s)
├── crates/
│   ├── stele-common/          # types, errors, time
│   ├── stele-storage/         # segments, WAL, delta, compaction
│   ├── stele-sim/             # virtual clock/disk/net + deterministic scheduler
│   ├── stele-catalog/         # versioned metadata
│   ├── stele-txn/             # MVCC, snapshots
│   ├── stele-sql/             # parser, binder, planner, optimizer
│   ├── stele-exec/            # vectorized operators
│   ├── stele-pgwire/          # Postgres wire front end
│   ├── stele-lineage/         # provenance
│   ├── stele-server/          # daemon (binary)
│   └── stele-cli/             # `stele` binary
├── fuzz/                      # cargo-fuzz targets
├── benches/                   # criterion benchmarks
├── tests/                     # cross-crate integration tests
└── docs/                      # you are here
```

(The crate graph is detailed in [02 §11](02-architecture.md#11-crate--module-decomposition-intended).)

---

## Task runner: `just`

A `justfile` gives memorable commands that wrap cargo and **mirror CI exactly**, so "green locally" means "green in CI":

```make
# justfile
default: dev

dev:            cargo run -p stele-server -- --dev
build:          cargo build --workspace
test:           cargo nextest run --workspace && cargo test --doc
fmt:            cargo fmt --all
lint:           cargo fmt --all --check && cargo clippy --all-targets -- -D warnings
check:          just lint && just test          # the pre-push gate
sim seeds="100": cargo run -p stele-sim --release -- --seeds {{seeds}} --fault-injection on
sim-seed seed:  cargo run -p stele-sim --release -- --seed {{seed}}   # reproduce one failure
fuzz target:    cargo +nightly fuzz run {{target}}
bench:          cargo bench --workspace
deny:           cargo deny check && cargo audit
cli *args:      cargo run -p stele-cli -- {{args}}
docker-build:   docker build -f docker/Dockerfile -t stele:dev .
```

`just check` is the **one command** a contributor runs before pushing — it is the local mirror of the CI merge gate ([04](04-cicd.md)).

---

## Running, building, testing

```bash
just dev                  # run the engine in dev mode (verbose logs, no auth, :5454)
just build                # compile the whole workspace
just test                 # unit + integration + doctests (nextest)
just lint                 # fmt-check + clippy (warnings = errors)
just check                # lint + test  → run this before every push
just sim 1000             # 1000 deterministic simulation seeds with fault injection
just sim-seed 42          # replay exactly the failure that seed 42 produced
just bench                # criterion benchmarks (compare against baseline)
just cli shell            # open the interactive stele shell
```

**Dev mode** (`--dev`) disables auth, enables verbose `tracing`, uses a local-disk storage backend in a scratch dir, and turns on extra assertions — never for production (which, per the [Charter](00-charter.md#8-the-trust-gate-no-production-data-stated-plainly), doesn't exist yet anyway).

---

## Logging

The engine emits **structured logs** through [`tracing`](https://docs.rs/tracing) — never `println!` in non-test code — with a per-connection span carrying the client's peer address, so concurrent connections stay legible. The global subscriber is installed once at server startup (STL-107).

**Verbosity** is an [`EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) directive read from `RUST_LOG`. When `RUST_LOG` is unset, the default is mode-dependent:

| Mode            | Default filter      |
| --------------- | ------------------- |
| dev (`--dev`)   | `info,stele=debug`  |
| operator (file) | `info`              |

`RUST_LOG`, when set, always wins. Directives are crate/module targets (Stele's crates compile to `stele_*` targets):

```bash
RUST_LOG=stele_pgwire=trace just dev   # trace the wire front end; everything else stays at info
RUST_LOG=warn just dev                 # quiet — warnings and errors only
RUST_LOG=stele=debug,stele_storage=trace just dev
```

**Format** is text by default; set `STELE_LOG_FORMAT=json` for one JSON object per line, the shape a production log shipper ingests. Any other value (or unset) is the human-readable text formatter — a typo degrades to readable logs, it never drops output.

```bash
STELE_LOG_FORMAT=json stele-server --config stele.toml   # production: structured JSON
```

---

## The `stele` CLI

One binary, two modes:

```bash
stele shell                          # interactive SQL shell (history, multiline, \dt etc.)
stele query "SELECT 1"               # one-shot query
stele server --config stele.toml     # run the engine (same as stele-server)
stele admin backup --to s3://...      # operational subcommands (as they land)
stele admin restore --from s3://...
stele admin inspect-segment <path>    # dump a segment's footer/zone maps (debug)
stele version                        # build + format-version info
```

The shell speaks to the engine over pg-wire (so it's also a reference client), and supports Stele's temporal niceties (e.g., `\\asof <timestamp>` to set a session as-of context).

---

## The canonical Docker image

The single blessed way to run Stele without a Rust toolchain. Multi-stage, minimal final image, multi-arch, published on release ([04](04-cicd.md)) to `ghcr.io/fricker-studios/stele` (canonical) and mirrored to Docker Hub as [`frickerstudios/stele`](https://hub.docker.com/r/frickerstudios/stele).

```dockerfile
# docker/Dockerfile
FROM rust:1.89-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p stele-server -p stele-cli

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /src/target/release/stele-server /usr/local/bin/stele-server
COPY --from=build /src/target/release/stele         /usr/local/bin/stele
EXPOSE 5454
ENTRYPOINT ["stele-server"]
CMD ["--config", "/etc/stele/stele.toml"]
```

Run it:

```bash
docker run --rm -p 5454:5454 frickerstudios/stele:latest --dev
# or from the canonical registry, pinned to a release:
docker run --rm -p 5454:5454 ghcr.io/fricker-studios/stele:v0.2.0 --dev
```

A `docker-compose.yml` is provided for a one-command local stack (engine + MinIO as an S3-compatible store once tiering lands):

```yaml
# docker-compose.yml (excerpt)
services:
  stele:
    image: ghcr.io/fricker-studios/stele:latest
    command: ["--dev"]
    ports: ["5454:5454"]
    depends_on: [minio]
  minio:                       # S3-compatible store for tiering dev (v0.3+)
    image: minio/minio
    command: server /data
    ports: ["9000:9000"]
```

---

## Hermetic shells (pick your poison)

All three resolve to the **same pinned toolchain** so results are reproducible across machines ([ADR-0005](adr/0005-reproducible-builds-pinned-toolchain.md)).

### Devcontainer / GitHub Codespaces
`.devcontainer/devcontainer.json` gives a zero-setup, browser-or-VS-Code environment:

```json
{
  "name": "stele-db",
  "image": "mcr.microsoft.com/devcontainers/rust:1-bookworm",
  "features": { "ghcr.io/devcontainers/features/docker-in-docker:2": {} },
  "postCreateCommand": "cargo install just cargo-nextest && just build",
  "customizations": { "vscode": { "extensions": ["rust-lang.rust-analyzer", "tamasfe.even-better-toml"] } }
}
```

"Open in Codespaces" → working engine, no local install.

### Nix (maximally hermetic)
A `flake.nix` dev shell pins *everything* (compiler, tools, even system libs) for bit-reproducible environments:

```nix
# flake.nix (excerpt)
{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.rust-overlay.url = "github:oxalica/rust-overlay";
  outputs = { self, nixpkgs, rust-overlay, ... }: let
    pkgs = import nixpkgs { overlays = [ rust-overlay.overlays.default ]; };
    rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
  in {
    devShells.default = pkgs.mkShell {
      buildInputs = [ rust pkgs.just pkgs.cargo-nextest pkgs.cargo-deny ];
    };
  };
}
```

`nix develop` → identical shell on any machine.

### devbox (lighter than Nix)
For contributors who want Nix-backed reproducibility without writing Nix:

```json
// devbox.json
{ "packages": ["rustup", "just", "cargo-nextest"], "shell": { "init_hook": ["rustup show"] } }
```

`devbox shell` → ready.

---

## Editor setup

- **rust-analyzer** is the assumed LSP; `.vscode/settings.json` and `.zed/` configs are checked in with sensible defaults (clippy on save, format on save).
- A `.editorconfig` enforces whitespace conventions across editors.
- `even-better-toml` (or equivalent) for the many `*.toml` configs.

---

## Contributor onboarding checklist

A new contributor's first session:

1. `git clone` → `cd stele-db`
2. `just dev` (toolchain auto-installs) → engine running on `:5454`
3. `psql -h localhost -p 5454 -d stele` → run the four-statement identity demo above
4. `just check` → confirm a clean local gate
5. `just sim 100` → watch deterministic simulation run
6. Read [02 — Architecture](02-architecture.md) and the [ADR index](adr/README.md)
7. Pick a `good-first-issue`; open a PR with a Conventional-Commit title

If any step takes more than a few minutes (compile time aside) or needs undocumented setup, that's a **bug in this document** — fix it here.

---

## Configuration

Engine config is a single `stele.toml` (with env-var overrides), e.g.:

```toml
# stele.toml
[server]
listen     = "0.0.0.0:5454"   # Stele's default pg-wire port (ADR-0017); override freely
data_dir   = "/var/lib/stele"

[storage]
backend    = "local"            # local | memory | s3
# [storage.s3] bucket = "stele-cold"  endpoint = "http://minio:9000"

[tls]                           # TLS on pg-wire (STL-251)
mode       = "required"         # required (default) | optional | disabled
cert       = "/etc/stele/server.crt"
key        = "/etc/stele/server.key"
# client_ca = "/etc/stele/clients.crt"  # set to require mTLS client certs

[storage.cache]
hot_cache_bytes = "8GiB"

[wal]
fsync      = "on_commit"        # group commit; the durability point ([02 §3.4])

[telemetry]
metrics    = "0.0.0.0:9090"     # Prometheus/OpenMetrics
tracing    = "info"
```

A ready-to-copy sample lives at [`stele.example.toml`](../stele.example.toml) in the repo root — `cp stele.example.toml stele.toml` and edit. Only `[server] listen`/`data_dir`, `[storage] backend`, and `[tls]` are read today (STL-116, STL-251); the other sections above are reserved (the parser ignores unknown sections) and land in later tickets.

**Secure defaults** ([10 §4](10-security-and-compliance.md#4-data-protection--encryption), STL-251): a config-file (non-dev) run **without `[tls]` may only bind loopback** — the server refuses to start on a non-loopback `listen` rather than silently serve plaintext. Configure `[tls]`, bind `127.0.0.1`, or use `--dev`.

`backend = "local"` — the default, and the fully-realized reference backend (STL-232) — roots everything at `data_dir`; what lands in that directory and the durability discipline behind it (file **and** directory fsync, torn-tail tolerance, fsync-failure poisoning) are documented in [02 — architecture §3.7](02-architecture.md#37-on-disk-layout--durability-discipline-local-backend). `backend = "memory"` runs the identical contract on the heap — nothing survives a restart; both backends (plus the sim's fault disk) pass the shared conformance suite (`stele_storage::backend::conformance`).

Dev mode (`--dev`) supplies safe defaults so a contributor needs **no config file** to get running — config is for operators, not for the five-minute path.
