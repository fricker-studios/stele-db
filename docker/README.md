# Stele

**A from-scratch, append-only, bitemporal, audit-native analytical database engine.**

Stele treats history as the primary key of reality. Every fact is stored with *when it was true in the world* (valid time) and *when the system learned it* (system time), and nothing is ever destructively overwritten. "What did this table look like last Tuesday, as we understood it at month-end close?" is a first-class query — not an archaeology project.

- **Source & docs:** [github.com/fricker-studios/stele-db](https://github.com/fricker-studios/stele-db)
- **Changelog:** [CHANGELOG.md](https://github.com/fricker-studios/stele-db/blob/main/CHANGELOG.md)
- **Wire protocol:** PostgreSQL-compatible (default port **5454**) — bring your existing drivers, ORMs, and BI tools.

> ⚠️ **Pre-1.0.** The API is still stabilizing and `latest` may move across breaking changes. Stele does not yet hold production data — pin exact versions (or digests) for anything you care about.

## Quick start

```bash
docker run --rm -p 5454:5454 frickerstudios/stele:latest --dev
```

Then connect with any Postgres client and run the thesis in four SQL statements:

```bash
psql -h localhost -p 5454 -d stele
```

```sql
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
INSERT INTO account VALUES (1, 100);
UPDATE account SET balance = 250 WHERE id = 1;
-- wait a beat, then time-travel to before the update:
SELECT balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1;
--   → 100   (the value *before* the update — history is never destroyed)
```

`--dev` runs with scratch storage and no config file. For real use, mount a config and data directory:

```bash
docker run -p 5454:5454 \
  -v stele-data:/var/lib/stele \
  -v ./stele.toml:/etc/stele/stele.toml:ro \
  frickerstudios/stele:v0.2.0
```

The image also ships the `stele` CLI/REPL:

```bash
docker run --rm -it --entrypoint stele frickerstudios/stele:latest --host host.docker.internal
```

## Tags

| Tag | Meaning |
|---|---|
| `v0.2.0` (`vX.Y.Z`) | Exact release — immutable once published. |
| `v0.2` / `0.2` (`vX.Y` / `X.Y`) | Latest patch of a minor line (moves). |
| `latest` | Latest release (pre-1.0: may move across breaking changes). |
| `@sha256:…` | Pin by digest — recommended for production. |

**Platforms:** `linux/amd64`, `linux/arm64` (multi-arch manifest — `docker pull` picks the right one).

This repository mirrors the canonical registry at `ghcr.io/fricker-studios/stele`; both receive identical, signed images (same digests).

## Image details

Multi-stage build: a pinned Rust toolchain compiles the release binaries, which are copied into a **distroless** runtime — no shell, no package manager, minimal attack surface.

- **Entrypoint:** `stele-server` (plus the `stele` CLI alongside it)
- **Port:** `5454`
- **Default config path:** `/etc/stele/stele.toml` (overridden by `--dev`)

## Verifying the image

Every release is keyless-signed with cosign and carries SLSA build provenance:

```bash
cosign verify \
  --certificate-identity-regexp '^https://github.com/fricker-studios/stele-db/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  frickerstudios/stele:v0.2.0

gh attestation verify oci://docker.io/frickerstudios/stele:v0.2.0 \
  --repo fricker-studios/stele-db
```

## License

[Business Source License 1.1](https://github.com/fricker-studios/stele-db/blob/main/LICENSE), converting to Apache License 2.0 four years after each release. Source-available and self-hostable.
