<!-- PR title must follow Conventional Commits, e.g. "feat(storage): add zone-map pruning" -->

## What & why

<!-- What does this change do, and why? Link any issue/ADR. -->

Closes #

## Checklist

- [ ] PR title is a [Conventional Commit](https://www.conventionalcommits.org/)
- [ ] `just check` passes locally (fmt + clippy + tests)
- [ ] Tests added/updated — and any temporal/as-of behavior has a [correctness oracle](../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)
- [ ] Docs updated if behavior/architecture changed
- [ ] Significant architectural decisions captured in an [ADR](../docs/adr/README.md)
- [ ] No production data / secrets introduced
