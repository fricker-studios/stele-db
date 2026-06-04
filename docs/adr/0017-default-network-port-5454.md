# ADR-0017 — Default network port: 5454 (pg-wire), configurable

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in follow-up session)
- **Related:** [ADR-0003](0003-postgres-wire-protocol-early.md) (pg-wire) · [05 — Dev Environment](../05-dev-environment.md#configuration) · [08 — Packaging](../08-packaging-distribution-and-releases.md) · [assumption A27](../assumptions.md)

## Context

Stele speaks the Postgres wire protocol ([ADR-0003](0003-postgres-wire-protocol-early.md)) but is its own engine with its own identity. Defaulting to Postgres's port **5432** would (a) clash with an actual Postgres running on the same host — a common dev and deploy reality — and (b) blur Stele's identity, implying it *is* Postgres rather than a distinct engine that happens to be wire-compatible. We want a distinct, recognizable, *safe* default port.

Port-selection constraints (verified against the IANA registry and Linux defaults):
- **Above 1024** — so binding doesn't require root.
- **Below 32768** — to stay out of the Linux default ephemeral range (32768–60999), which the OS hands out for outbound connections; a server bound there hits intermittent startup collisions.
- **Not a common service**, and specifically not the tempting pg-adjacent ports already taken — **5433** (Yugabyte YSQL / conventional "second Postgres") and **5444** (EDB Postgres Advanced Server).
- Ideally **recognizable** as pg-wire family.

(A fun rejected option: leetspeak "STELE" = **57313** lands *inside* the Linux ephemeral range, so it would be a flaky default — the name doesn't fit in a safe port.)

## Decision

**The default listen port is `5454` (TCP), and it is configurable** (config file, CLI flag, and env var; consumers may override it freely — [05 §config](../05-dev-environment.md#configuration)).

`5454` is **unassigned** in the IANA service registry, sits in the safe band (1024–32767, below Linux ephemeral), and reads as the **"54xx Postgres family"** neighborhood — signaling wire compatibility while staying clearly distinct from 5432. It is also trivially memorable and typeable.

We deliberately **do not also bind 5432**: a clean identity means clients simply pass `-p 5454`. The Docker image `EXPOSE`s 5454 and the compose stack maps `5454:5454` ([05 §docker](../05-dev-environment.md#the-canonical-docker-image)). The number is treated as part of the brand and should be noted in any future IANA port-registration effort.

## Consequences

### Positive
- No clash with a local Postgres on 5432 — Stele and Postgres coexist on one host out of the box.
- Clear identity: the port says "pg-wire family, but its own engine."
- Safe by construction: above root threshold, below the ephemeral range, IANA-unassigned, not a known service.
- Memorable and easy to type.

### Negative / costs
- Users pointing pg ecosystem tools at Stele must specify the non-default port (`-p 5454` / a connection-string port) since those tools assume 5432. Minor, and documented in the quickstart ([05](../05-dev-environment.md#the-five-minute-path-the-headline-promise)).

### Neutral / follow-ups
- Fully overridable, so any operator who *wants* 5432 (e.g., for drop-in tooling) can set it.
- If a future "drop-in Postgres replacement" mode ever becomes a goal, revisit whether to offer 5432 as an opt-in default — but identity wins for now.
- Pursue IANA registration of `5454` for Stele if/when the project warrants it.
