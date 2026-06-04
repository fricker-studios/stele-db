# ADR-0011 — Integration-groundwork primitives: hash distribution, DV/Solvia-ready but DV-agnostic

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in follow-up to founding session)
- **Related:** [ADR-0009](0009-data-vault-conceptual-seam.md) (keeps DV out) · [ADR-0006](0006-distribution-later-shared-storage.md) (distribution) · [ADR-0007](0007-storage-compute-separation.md) (storage/compute) · [01 §A.5, §B.7](../01-feature-plan.md#a5--hash-keys--mergeupsert) · [02 §10](../02-architecture.md#10-distribution--consensus-later-phase) · [Charter §7](../00-charter.md#7-the-solvia-seam-designed-for-decoupled)

## Context

[ADR-0009](0009-data-vault-conceptual-seam.md) correctly keeps Data Vault and Solvia/RCM concepts **out** of the engine. But its stance was deliberately minimal — "avoid decisions that would make Stele a poor host… **nothing more**." That is *don't-preclude*, which is weaker than what we actually want. The project owner clarified two things:

1. **Lay reasonable groundwork** so the eventual Data Vault / Solvia integration is *seamless* — explicitly calling out **hash distribution** — rather than merely not preventing it.
2. **Keep the full distributed + plug-and-play cloud-storage capability** firmly in scope ("we'll probably honestly need it"), not as a someday-maybe.

The tension to resolve: how do we build *groundwork* for a specific downstream consumer (Solvia/DV) without **coupling** the engine to it and violating [ADR-0009](0009-data-vault-conceptual-seam.md)?

The resolution is a bright-line test: **we build only primitives that are independently justified for a general distributed analytical + audit engine, and that happen to make DV/Solvia integration seamless.** Data Vault's hubs/links/satellites map *onto* hash keys, hash distribution, and co-located joins — but those are generic sharded-database primitives, not DV concepts. So we can build the substrate openly while the engine stays ignorant of what a hub is.

## Decision

**We will build a small set of general "integration-groundwork" primitives, each independently justified, designed so DV/Solvia integration is seamless without the engine containing any DV/RCM concept:**

1. **Stable, portable, versioned hash-key functions.** The built-in hash-key functions ([01 §A.5](../01-feature-plan.md#a5--hash-keys--mergeupsert)) are **deterministic and identical across engine versions, platforms, and client languages**, with a published spec. This lets external models (and clients) compute the *same* business/hash keys the engine does — the foundation any hash-keyed modeling pattern (DV included) needs. *(Builds on v0.2 hash keys.)*

2. **Hash-based data distribution / partitioning.** In the distributed phase, a table may declare a **distribution key** (typically a hash key); rows are partitioned across nodes by the hash of that key. This is the standard scale-out primitive for sharded analytical engines — and it is exactly what makes a hash-keyed model distribute cleanly. *(Distributed phase, v2.0+; builds on [ADR-0006](0006-distribution-later-shared-storage.md).)*

3. **Key co-location / co-partitioning.** Related tables can be declared to **share a distribution** so that joins between co-located keys stay node-local (no shuffle). Stated generally, this is "co-partition tables that are frequently joined on the same key" — which generically covers the hub↔satellite and hub↔link access patterns without naming them. *(Distributed phase, v2.0+.)*

4. **Hash-keyed bitemporal MERGE + inline lineage as the integration substrate.** Already first-class ([01 §A.5](../01-feature-plan.md#a5--hash-keys--mergeupsert), [§A.4](../01-feature-plan.md#a4--lineage--provenance-first-class)); reaffirmed here as the historization/provenance substrate any audit model loads through.

**And we reaffirm:** full distribution ([ADR-0006](0006-distribution-later-shared-storage.md)) and **plug-and-play, pluggable, S3-compatible cloud storage with storage/compute separation** ([ADR-0007](0007-storage-compute-separation.md)) are firmly in scope. "Later phase" is a **sequencing** decision (single-node correctness first, [Charter §3](../00-charter.md#3-the-guardrail--lead-with-the-non-goal)), **not** a deprioritization or a scope cut.

### The bright line (unchanged from [ADR-0009](0009-data-vault-conceptual-seam.md))

| Counts as **groundwork** (we build it) | Counts as **coupling** (we never build it) |
|---|---|
| Stable, portable hash functions | A `hash_key` type that knows it's a "hub key" |
| Hash distribution / partitioning by a declared key | A `hub`, `link`, or `satellite` table type |
| Co-location of tables joined on the same key | A built-in hub↔satellite relationship concept |
| Bitemporal MERGE keyed on hash; inline lineage | A `claim`, `adjustment`, or any RCM entity |
| Generic "distribution key" + "co-partition" DDL | DV-shaped or Solvia-shaped DDL/APIs |

The test for any future PR: *"Would a sharded analytical/audit database with no knowledge of Data Vault still want this primitive?"* If yes, it's groundwork. If it only makes sense because Data Vault exists, it's coupling — reject it, citing this ADR and [ADR-0009](0009-data-vault-conceptual-seam.md).

## Consequences

### Positive
- The eventual Solvia/DV integration is **seamless by construction** — hash keys, distribution, and co-located joins are already there and already general.
- Every primitive is **independently valuable**: hash distribution and co-partitioning are table stakes for any distributed analytical engine, so we lose nothing by building them generally.
- Strengthens [ADR-0009](0009-data-vault-conceptual-seam.md) from "don't preclude" to "actively ready" **without** weakening its bright line.
- Confirms the distributed/cloud capability is a real commitment, so foundational decisions (immutability, shared storage, stable hashing) are made with it in mind from the start.

### Negative / costs
- Subtle vigilance required: "groundwork" is a slope, and the bright-line table above exists precisely because it's tempting to slide a DV-shaped convenience in under the "groundwork" banner. The PR test must be applied honestly.
- Distribution-key and co-location DDL adds surface area to the (later) distributed phase; it must be designed generally, not around DV's specific shapes.
- Some of this groundwork (distribution, co-location) lands in the v2.0+ phase, so the payoff is deferred — accepted, since the primitives aren't needed single-node.

### Neutral / follow-ups
- The **distribution-key / co-partition DDL and the hash-function spec** get their own design docs + ADR amendments when the distributed phase begins ([roadmap v2.0+](../03-roadmap.md#v20--distribution-era)).
- The hash-function spec (algorithm, normalization, encoding) must be pinned early (it's portability-critical and hard to change post-data) — tracked alongside [on-disk format open question O2](../assumptions.md).
- If hosting Solvia later reveals a *genuinely general* missing primitive, it's added generally with its own ADR — never as a DV/RCM-shaped one ([ADR-0009](0009-data-vault-conceptual-seam.md) follow-up still governs).
