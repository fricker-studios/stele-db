# ADR-0013 — Kubernetes/OpenShift operator + Helm, OperatorHub & OpenShift-certified

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project owner (confirmed in follow-up session)
- **Related:** [09 §5](../09-ecosystem-and-products.md#5-kubernetes--openshift-operator) · [08](../08-packaging-distribution-and-releases.md) · [ADR-0007](0007-storage-compute-separation.md) · [ADR-0016](0016-admin-control-plane-api.md) · [assumption A23](../assumptions.md)

## Context

Stele needs a first-class story for running on Kubernetes and OpenShift — the dominant platform for self-hosted databases. The options span a spectrum: a plain **Helm chart** (simple, declarative install, but no active lifecycle management), a full **operator** (watches CRDs and reconciles — automates backup, scaling, upgrades, failover), or both. There's also a certification axis: community listing vs **Red Hat OpenShift certification** (real effort, but unlocks enterprise/regulated adopters who require certified operators).

A stateful database operator is non-trivial, and much of its value (scaling, failover) depends on engine capabilities that arrive later (storage/compute separation, replication). So *sequencing* matters as much as the choice.

## Decision

**We will ship both a Helm chart and a full operator, list on OperatorHub, and pursue Red Hat OpenShift certification** ([assumption A23](../assumptions.md)).

- **Helm chart** (the simple path) lands first at **v0.5** — declarative install/config for users who don't need lifecycle automation.
- **Operator** lands at **v0.7**, reconciling CRDs: `SteleCluster` (topology, storage backend, resources), `SteleBackup`/`SteleRestore` (scheduled + on-demand to object storage), `SteleUser`/`SteleRole` (declarative auth). CRDs graduate `v1alpha1`→`v1` with **conversion webhooks** so stored resources never break ([08 §7](../08-packaging-distribution-and-releases.md#7-versioning--compatibility-policy-the-important-part)).
- The operator drives the engine through the **admin/control-plane API** ([ADR-0016](0016-admin-control-plane-api.md)) and performs **format-compatibility-aware rolling upgrades**.
- **Packaging:** OLM bundle on **OperatorHub**, OCI Helm chart, images from `ghcr.io`. Pursue **OpenShift certified** status around **v1.0** and climb operator **capability levels** (Basic → Seamless Upgrades → Full Lifecycle → Deep Insights → Autopilot) as engine features (scaling, failover) mature.
- Distributed-cluster management arrives with the [distribution phase](0006-distribution-later-shared-storage.md) at **v2.0+**.

## Consequences

### Positive
- Covers both audiences: Helm for the "just install it" crowd, operator for teams wanting automated Day-2 ops.
- OperatorHub + OpenShift certification unlocks enterprise/regulated adopters (a natural fit for an audit-native engine).
- Storage/compute separation ([ADR-0007](0007-storage-compute-separation.md)) makes compute scaling unusually clean for the operator to manage.

### Negative / costs
- A correct stateful-DB operator is significant, ongoing engineering; certification adds process and maintenance overhead.
- Capability levels depend on engine features that land later — the operator's value grows over several milestones rather than arriving complete.
- Maintaining CRD conversion webhooks across versions is real work (but non-negotiable for not breaking users).

### Neutral / follow-ups
- CRD schemas and the reconcile/upgrade state machine get their own design doc when operator work begins (v0.7).
- Re-evaluate the OpenShift-certification timing if enterprise demand arrives earlier or later than expected.
