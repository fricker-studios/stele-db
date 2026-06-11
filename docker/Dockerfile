# Canonical Stele image — the blessed way to run the engine without a local Rust
# toolchain. See docs/05-dev-environment.md (the five-minute path) and
# docs/04-cicd.md (the release pipeline that publishes this to ghcr.io).
#
# Multi-stage: a full Rust toolchain builds the release binaries, then they are
# copied into a distroless runtime — no shell, no package manager, minimal attack
# surface. The build stage pins Rust 1.89 (== rust-toolchain.toml / the MSRV).

FROM rust:1.89-slim AS build
WORKDIR /src
COPY . .
# `--locked` builds the committed Cargo.lock exactly, so the image is
# reproducible (ADR-0005). Only the two shipping binaries are built.
RUN cargo build --release --locked -p stele-server -p stele-cli

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /src/target/release/stele-server /usr/local/bin/stele-server
COPY --from=build /src/target/release/stele         /usr/local/bin/stele
EXPOSE 5454
ENTRYPOINT ["stele-server"]
# Operators mount a config and run the default; the five-minute path overrides
# with `--dev` (see docker-compose.yml and `docker run … --dev`).
CMD ["--config", "/etc/stele/stele.toml"]
