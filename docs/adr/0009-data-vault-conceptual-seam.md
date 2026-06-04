# ADR-0009 — Data Vault / Solvia kept out of the engine (conceptual seam only)

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (stated in founding brief)
- **Related:** [ADR-0011](0011-hash-distribution-integration-groundwork.md) (the groundwork we *do* build) · [Charter §5, §7](../00-charter.md#5-scope) · [02 — Architecture §8](../02-architecture.md#8-lineage--provenance-subsystem) · [01 §A.4–A.5](../01-feature-plan.md#a4--lineage--provenance-first-class)

## Context

Stele is intended to *eventually* become the storage engine under **Solvia** (a lab-RCM SaaS), and RCM data is a natural bitemporal/audit workload. There is a real temptation to bake **Data Vault** modeling (hubs, links, satellites) or RCM concepts (claims, adjustments) into the engine "to save time later." This is a trap: it would couple a general-purpose engine to one application's logical model, narrow its audience, and violate the [Charter's](../00-charter.md#7-the-solvia-seam-designed-for-decoupled) decoupling discipline. Data Vault is a *logical modeling pattern*, not a storage feature.

## Decision

**We will keep Data Vault and all Solvia/RCM specifics out of the Stele engine.** Stele provides the **general primitives** that make Data Vault and audit cheap and fast — bitemporality, append-only historization, first-class **hash keys** ([01 §A.5](../01-feature-plan.md#a5--hash-keys--mergeupsert)), fast `MERGE`, and inline **lineage/provenance** ([02 §8](../02-architecture.md#8-lineage--provenance-subsystem)) — but it never contains a `hub`, `link`, `satellite`, `claim`, or any RCM concept. Data Vault and RCM logic live **on top of** Stele, in Solvia, as a separate product.

Beyond keeping those concepts out, we *do* lay reasonable **general-primitive groundwork** so the eventual integration is seamless — stable/portable hash functions, hash-based distribution and co-location, hash-keyed temporal MERGE, and inline lineage — but **only** primitives that are independently justified for any distributed analytical/audit engine. That groundwork is its own decision, [ADR-0011](0011-hash-distribution-integration-groundwork.md); *this* ADR governs the bright line (what stays **out**). The test for any feature: *"would a sharded analytical/audit DB with no knowledge of Data Vault still want this?"* — if no, it's coupling and we reject it. Until Stele earns trust in the open, the two remain **fully decoupled** ([Charter §7–8](../00-charter.md#7-the-solvia-seam-designed-for-decoupled)).

## Consequences

### Positive
- Stele stays a general-purpose engine with a broad audience, not a single-app backend.
- The primitives are designed well *because* they must serve any audit/temporal modeling pattern, not just Data Vault.
- Clean separation means Stele's open-source credibility isn't entangled with a proprietary product.

### Negative / costs
- Some integration work is deferred to "later" rather than amortized now — accepted deliberately.
- Requires ongoing vigilance: PRs or designs that smuggle RCM/Data-Vault concepts into the engine must be rejected, citing this ADR ([Charter §10 anti-charter](../00-charter.md#10-what-would-make-this-fail-anti-charter)).

### Neutral / follow-ups
- If, far in the future, hosting Solvia reveals a *genuinely general* primitive Stele lacks, we add it as a general feature (with its own ADR) — never as an RCM/Data-Vault-shaped one.
