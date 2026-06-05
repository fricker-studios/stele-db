//! The validity index — the derived, rebuildable home for `sys_to`.
//!
//! A version record carries `sys_from` and is **never mutated**; its system-time
//! *end* (`sys_to`) is not stored on the record at all
//! ([ADR-0023](../../../docs/adr/0023-append-only-record-model-validity-index.md)).
//! Instead, when a superseding assertion or a delete commits, the prior version's
//! end is materialized **once** into this index, keyed `(business_key, sys_from)`
//! → `sys_to` (+ the closing transaction's provenance). The entry is **written
//! once and never updated** — the past, once recorded, does not change; valid-time
//! corrections are new assertions, not edits.
//!
//! ## An accelerator, not an authority
//!
//! The index is **derived state, fully rebuildable from the WAL/commit log**
//! ([ADR-0023](../../../docs/adr/0023-append-only-record-model-validity-index.md)):
//! losing or corrupting it cannot corrupt history — it is reconstructed by
//! replaying the appended [`Close`] records the write path logs. This keeps the
//! [verifiable log](../../../docs/adr/0026-verifiable-audit-log.md) authoritative.
//! On recovery [`ValidityIndex::open`] discards any stale spill files and the
//! caller replays the WAL's close records back through [`ValidityIndex::insert_close`],
//! exactly mirroring the delta tier's rebuild ([`crate::delta`]).
//!
//! ## Write-once + per-key serialization
//!
//! [`ValidityIndex::insert_close`] is **write-once**: re-applying the *identical*
//! close is idempotent (the property WAL replay relies on), but a *conflicting*
//! close for an already-closed `(business_key, sys_from)` is refused with
//! [`ValidityError::AlreadyClosed`]. That refusal is the per-key serialization
//! point of [ADR-0023](../../../docs/adr/0023-append-only-record-model-validity-index.md):
//! two concurrent supersessions of the same version cannot both close it — the
//! loser retries.
//!
//! ## Range-containment lookup
//!
//! Because each version's end is materialized, "the version of `key` active at
//! system-time `S`" is a direct **range-containment** lookup
//! ([`ValidityIndex::active_at`]) — the greatest `sys_from ≤ S` whose materialized
//! `sys_to > S` — with no walk of the version chain to discover where each
//! interval ends. An `S` beyond every closed interval falls in the key's *open*
//! tail, which the read path resolves against the version set ([`crate::merge`]).

mod index;
mod spill;

pub use index::{
    Close, ClosedInterval, MAX_CLOSE_FRAME_LEN, ValidityConfig, ValidityError, ValidityIndex,
};
