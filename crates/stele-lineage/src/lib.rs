//! Lineage & provenance — captured at commit, stored **inline** with each version.
//!
//! Two tiers ([`docs/02-architecture.md` §8](../../../docs/02-architecture.md#8-lineage--provenance-subsystem)):
//!
//! 1. **Per-row transaction provenance** (v0.2) — who/what/when. Always-on, cheap.
//! 2. **Derivation lineage** (v0.7+, opt-in) — the row-computed-from-inputs graph.
//!
//! This crate is the substrate that makes audit *and* Data Vault cheap to
//! build **on top of Stele**, while the engine itself stays ignorant of what a
//! hub or a claim is ([ADR-0009](../../../docs/adr/0009-data-vault-conceptual-seam.md)).
//!
//! Scaffold only at v0.1.

#![allow(dead_code)]
