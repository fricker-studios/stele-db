# ADR-0034 — Role-based access control: roles, ownership, and table privileges

- **Status:** Accepted
- **Date:** 2026-07-27
- **Deciders:** Project owner + systems design
- **Related:** [10 §6](../10-security-and-compliance.md#6-authorization) · [ADR-0018](0018-security-auditability-pillar.md) · [ADR-0028](0028-durable-catalog-log.md) · [ADR-0016](0016-admin-control-plane-api.md) · [audit 2026-07 §1.1](../audit-2026-07.md)

## Context

Authentication landed across STL-252 / STL-297 / STL-298 / STL-300: a connection
proves an identity by SCRAM-SHA-256 or a verified mTLS certificate, and that
identity is stamped into every version's provenance. **Nothing consumed it for
access control.** The July 2026 audit found the consequence: `apply_user_ddl`
takes no principal and performs no privilege check, and `execute_at` routes
`StatementBody::Admin` unconditionally — so any session that can authenticate
can run `ALTER USER admin PASSWORD …`, `DROP USER`, or `BACKUP TO '/anywhere'`.

That makes one account compromise a total compromise, and it makes the whole
authentication investment a provenance feature rather than a security boundary.
It is the largest gap between what [10 — Security & Compliance](../10-security-and-compliance.md)
describes and what the engine enforces.

[10 §6](../10-security-and-compliance.md#6-authorization) already commits to the
shape: `GRANT`/`REVOKE` on objects, roles composed for least privilege, one
authorization model shared with the admin/control-plane API ([ADR-0016]), and a
temporal rule — *time travel must never become a privilege-escalation channel.*
This ADR decides how that lands.

## Decision

**1. One namespace: roles are users.** Following Postgres, a role is the single
principal type; a role with a password verifier can log in. The existing user
store becomes the role store rather than gaining a sibling, so there is exactly
one place an identity is defined and exactly one place to look when auditing who
can do what.

**2. Two role attributes to start: `SUPERUSER` and ownership.** A superuser
bypasses every privilege check. Every table records the role that created it as
its **owner**; the owner implicitly holds all privileges on it and is the only
non-superuser who may `DROP` it or `GRANT` on it. These two carry the whole
administrative model without a separate admin-rights vocabulary.

**3. Table privileges: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.** Granted to a
named role or to the `PUBLIC` pseudo-role, which every role holds implicitly.
`ALL PRIVILEGES` is sugar for the four. This is the minimum that makes the
read/write split — the separation-of-duties case [10 §7](../10-security-and-compliance.md#7-access-auditing--monitoring)
names, an auditor who can read but not modify — actually expressible.

**4. Superuser gates the operations that have no object.** User/role DDL
(`CREATE`/`ALTER`/`DROP USER`) and the admin verbs (`BACKUP`, `CHECKPOINT`,
`FLUSH`, `COMPACT`) are superuser-only. `CREATE TABLE` is open to any role — you
own what you create — which keeps the ordinary path unprivileged.

**5. Checks resolve against the *current* policy, never the read snapshot.**
This is the temporal rule from [10 §6](../10-security-and-compliance.md#6-authorization),
and it is the one decision here that is specific to a bitemporal engine. A
`SELECT … FOR SYSTEM_TIME AS OF <past>` is authorized against the grants that
exist **now**, not the grants that existed at the as-of instant. The alternative
— resolving policy at the snapshot — would mean a revoked grant could be
re-entered by time-travelling to before the `REVOKE`, turning `AS OF` into a
privilege-escalation channel. It also means a grant is effective immediately for
history the role could not previously read, which is the intended reading of
"you cannot read data as-of a time before you had access" applied to the policy
rather than the data.

**6. Grants and role attributes are durable in the catalog log** ([ADR-0028]),
as new record variants alongside the DDL they sit with. They are therefore
hash-chained and tamper-evident on the same terms as schema history
([ADR-0031]), replayed by the same fold, and covered by the same fail-closed
posture. Privileges are *not* bitemporal: the catalog log is the authority for
current policy, consistent with decision 5.

**7. Reuse sqlparser's native `GRANT`/`REVOKE` AST.** sqlparser 0.62 parses
`GRANT SELECT, INSERT ON TABLE t TO role`, `GRANT ALL PRIVILEGES`, `TO PUBLIC`,
and the `REVOKE` mirror — all under the existing dialect, with no hand-lifting of
the kind `CHECKPOINT` needs ([STL-219]). Binding the real AST rather than
hand-rolling grammar keeps the surface Postgres-shaped for free and means a
driver's generated DCL parses as written.

**8. The built-in `stele` identity is the bootstrap superuser.** A fresh
database has no roles, so something must be able to create the first one. The
server identity `stele` — already the default principal for an unauthenticated
startup and for a direct embedded writer (STL-300) — is superuser by
construction and needs no stored credential. Under `auth = "scram"` no client
can authenticate *as* `stele` (it has no verifier), so the bootstrap path is
explicit: start once on loopback with `auth = "trust"` (or `--dev`), create a
superuser with a password, then switch to `scram`.

## Consequences

**What this buys.** The authentication boundary becomes a security boundary: a
compromised low-privilege account can no longer rewrite the superuser's password
or exfiltrate the database through `BACKUP`. Separation of duties becomes
expressible. And because the admin/control-plane API resolves the same role
store, [ADR-0016]'s "one authorization model, not two" holds by construction
rather than by convention.

**What it costs.** Every statement now resolves the tables it touches against
the grant map before executing — an ordered-map lookup per table per statement
(the grant and owner maps are `BTreeMap`s, matching the determinism the rest of
the engine prefers), taken under the engine lock that already serializes
dispatch, so it does not change the concurrency story. The larger cost is that the check must sit at *every*
dispatch entry point; the audit found the commit-log poison guarded one of seven
such points, and a privilege check with the same gap would be worse than none.
The mitigation is structural: one `authorize` helper, called from the same
chokepoints, with a test that asserts each entry point denies.

**Deliberately deferred**, each because it is a real design in its own right and
none blocks the boundary above:

- **Role membership** (`GRANT analyst TO alice`) — sqlparser has no grammar for
  it, so it needs the hand-lift treatment plus a membership closure with cycle
  detection. Until then, grants go to roles directly.
- **`WITH GRANT OPTION`** — parsed by sqlparser, rejected by the binder rather
  than silently ignored. Re-granting is owner/superuser-only for now.
- **Column-level privileges** — sqlparser carries the column lists on
  `Action::Select`/`Insert`/`Update`; the binder rejects them rather than
  ignoring them, so the syntax cannot silently under-enforce.
- **Row-level security**, ABAC, and the certificate↔role mapping [10 §5](../10-security-and-compliance.md#5-authentication)
  describes — all v0.5+.

**Trust mode is unchanged and still not a boundary.** Under `auth = "trust"` a
client names itself, so it can name a superuser. That is what trust means, it is
already warned about at boot, and authorization does not repair it — the honest
framing is that trust is weak *authentication*, and this ADR is about
*authorization*. The one place it matters is bootstrap (decision 8), which is
why that path is loopback-and-explicit.
