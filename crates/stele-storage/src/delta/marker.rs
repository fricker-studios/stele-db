//! `CloseMarker` — a cross-tier logical period-close.
//!
//! When a key's live version has already been flushed out of the delta tier
//! into a **sealed segment**, closing its system-time period cannot re-stage the
//! version: invariant 1 forbids mutating a sealed segment
//! ([architecture §12](../../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants)).
//! Instead the close is recorded as an **appended delta record** that names the
//! version it closes and the new `sys_to` — the "tombstones are logical
//! period-closes … they carry their own provenance" of
//! [architecture §3.1](../../../../docs/02-architecture.md#31-tiered-layout-lsm-flavored-history-preserving)
//! ([STL-127]).
//!
//! A marker is deliberately *not* a [`Version`](super::Version): it carries no
//! `payload` and no birth `provenance`, because those already live — intact and
//! immutable — on the sealed version it refers to. The read path
//! ([`crate::merge`]) folds a marker onto its target by matching
//! `(business_key, sys_from)`, overlaying the new `sys_to` and the closing
//! transaction's provenance. Compaction folds markers into the rewritten
//! segment so the closed interval becomes intrinsic — a later milestone; until
//! then a marker rides in the delta tier alongside live versions.

use stele_common::provenance::Provenance;
use stele_common::time::SystemTimeMicros;

use super::version::BusinessKey;

/// An appended delta record that closes a sealed version's system-time period.
///
/// Identifies its target by `(business_key, sys_from)` — the same key the delta
/// tier and sealed segments cluster a version chain by — and supplies the new
/// `sys_to` plus the closing transaction's provenance. The target version's body
/// (payload, birth provenance) is untouched: a close is bookkeeping by the
/// superseding/deleting transaction, never a rewrite of who wrote the closed
/// version ([STL-118], [STL-127]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseMarker {
    /// The business key of the version being closed.
    pub business_key: BusinessKey,
    /// The `sys_from` of the *sealed* version this marker closes — the match
    /// key the read path folds the marker onto.
    pub sys_from: SystemTimeMicros,
    /// The new `sys_to` stamped on the closed period (the closing commit). The
    /// closed interval becomes `[sys_from, sys_to)`.
    pub sys_to: SystemTimeMicros,
    /// Provenance of the transaction that performed the close — recorded as the
    /// target version's `closed_by`. For a delete there is no successor version,
    /// so this marker is the only record of who closed the period ([STL-118]).
    pub closed_by: Provenance,
}
