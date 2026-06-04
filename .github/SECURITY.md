# Security Policy

Stele is an audit-native engine intended for sensitive, regulated data, so we take security seriously even at this early stage. See [10 — Security & Compliance](../docs/10-security-and-compliance.md) for the full model.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

Instead, use one of:

- **GitHub private vulnerability reporting** — [open a private advisory](https://github.com/fricker-studios/stele-db/security/advisories/new) (preferred).
- **Email** — security@steledb.com.

Please include: a description of the issue, steps to reproduce or a proof-of-concept, affected version/commit, and any suggested remediation.

## What to expect

- We aim to acknowledge a report promptly and keep you updated as we investigate.
- We practice **coordinated disclosure**: we'll work with you on a fix and a disclosure timeline, and credit you (with your permission) in the advisory.
- Please give us reasonable time to remediate before any public disclosure.

## Supported versions

Pre-1.0, only the latest released version (and `main`) receives security fixes. A formal support policy arrives at [v1.0](../docs/08-packaging-distribution-and-releases.md#7-versioning--compatibility-policy-the-important-part).

## Scope

Stele currently holds **no production data** and is in design/early development. Reports about the engine, its dependencies (supply chain), and the build/release pipeline are all in scope.
