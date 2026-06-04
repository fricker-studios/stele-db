# Contributing to Stele

Thanks for your interest in Stele — a from-scratch, append-only, bitemporal, audit-native database engine.

> **Project status:** pre-1.0, design-stage, and deliberately no-deadline. It holds **no production data** ([trust gate](../docs/06-testing-strategy.md#9-what-tested-enough-to-hold-real-data-means-the-trust-gate-operationalized)). Quality and correctness win over speed.

## Before you start

- Read the [Charter](../docs/00-charter.md) — especially the [non-goals](../docs/00-charter.md#3-the-guardrail--lead-with-the-non-goal). Contributions that pull Stele toward the "beat ClickHouse and Postgres at once" graveyard, or that smuggle Data Vault / RCM concepts into the engine, will be declined.
- Skim the [Architecture](../docs/02-architecture.md) and the [ADR index](../docs/adr/README.md).
- **Significant decisions need an ADR** ([template](../docs/adr/_template.md)). If your change is architecturally load-bearing, open the ADR first.

## Dev setup

Follow the [five-minute path](../docs/05-dev-environment.md#the-five-minute-path-the-headline-promise):

```bash
git clone https://github.com/fricker-studios/stele-db
cd stele-db
just dev            # build + run (toolchain auto-pins via rust-toolchain.toml)
just check          # fmt + clippy + tests — run this before every push
```

`just check` is the local mirror of the CI merge gate.

## Pull requests

- **Branches** are short-lived: `feature/*`, `fix/*`, `docs/*`. Rebase before merge.
- **[Conventional Commits](https://www.conventionalcommits.org/)** are required — the changelog and versioning automation depend on them. Your PR title becomes the squash-commit message.
- **Tests:** no temporal feature ships without a [correctness oracle](../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart). CI must be green.
- Keep PRs focused; update docs/ADRs alongside code.

## Testing bar

Stele lives or dies on correctness. See the [Testing Strategy](../docs/06-testing-strategy.md): unit + property tests, fuzzing, deterministic simulation testing, and oracles for all bitemporal/as-of behavior.

## Licensing of contributions

Stele is licensed under the [Business Source License 1.1](../LICENSE) (converting to Apache-2.0). By contributing, you agree your contributions are provided under the project's license (inbound = outbound). A formal CLA/DCO may be introduced before the first external release — see [07 — Licensing & OSS](../docs/07-licensing-and-oss.md#contribution-model).

## Conduct & questions

- Be excellent to each other — see the [Code of Conduct](CODE_OF_CONDUCT.md).
- Questions and ideas: [GitHub Discussions](https://github.com/fricker-studios/stele-db/discussions).
- Security issues: **do not** open a public issue — see [SECURITY.md](SECURITY.md).
