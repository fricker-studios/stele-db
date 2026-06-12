//! Provenance — who/what/when wrote each version, captured at commit.
//!
//! Stele records, **inline on every stored version**, the three facts that make
//! audit cheap and let a Data Vault be built on top of the engine without the
//! engine knowing what a hub or a satellite is
//! ([architecture §8](../../../docs/02-architecture.md#8-lineage--provenance-subsystem),
//! [ADR-0009](../../../docs/adr/0009-data-vault-conceptual-seam.md)):
//!
//! * [`TxnId`] — the transaction that wrote the version.
//! * `committed_at` — the commit timestamp ([`SystemTimeMicros`]).
//! * [`Principal`] — who or what issued the write.
//!
//! This is **invariant 5** of
//! [architecture §12](../../../docs/02-architecture.md#12-cross-cutting-architectural-invariants):
//! provenance is *inline and captured at commit, never reconstructed after the
//! fact*. Unlike valid-time it is **always present** — there is no per-table
//! opt-in — so the types here carry no `Option`.
//!
//! These types live in `stele-common` (the dependency root) because both the
//! storage core — which stamps and stores them — and the transaction manager —
//! which supplies the [`TxnId`] and [`Principal`] at commit — need to name them
//! without depending on each other.

use crate::time::SystemTimeMicros;
use crate::types::LogicalType;

/// A transaction identifier — the writing transaction for a version.
///
/// Monotonic and assigned by the transaction manager
/// ([architecture §2](../../../docs/02-architecture.md#2-the-bitemporal-record-model)
/// models it as a `u64`). Monotonicity is what lets the on-disk `txn_id` column
/// compress well — successive versions carry near-constant or strictly-rising
/// ids ([architecture §3.2](../../../docs/02-architecture.md#32-on-disk-segment-format)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

/// Who or what issued a write — opaque identity bytes.
///
/// Conceptually *text* ([architecture §2](../../../docs/02-architecture.md#2-the-bitemporal-record-model)),
/// but the storage layer keeps it opaque (like
/// [`BusinessKey`](../../stele_storage/delta/struct.BusinessKey.html)): the
/// transaction/session layer decides the convention (a user name, a service
/// account, a session id) and the engine stores the bytes verbatim. Equality
/// and ordering are the usual byte-wise comparison.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Principal(pub Vec<u8>);

impl Principal {
    /// Construct from anything that can be turned into a `Vec<u8>` — a `&str`,
    /// `String`, or raw bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Borrow the underlying identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The provenance triple stored inline on every version.
///
/// ## Why `committed_at` is stored, not derived from `sys_from`
///
/// In the single-writer storage path `committed_at` equals the version's
/// `sys_from` — both are the commit timestamp. It is nonetheless stored as its
/// own fact, for two reasons:
///
/// 1. **Invariant 5** requires provenance to be *inline, never reconstructed*.
///    Deriving `_stele_committed_at` from `sys_from` at read time would be
///    reconstruction.
/// 2. The two diverge under distribution. `sys_from` is the *logical*
///    system-time coordinate used for `AS OF` resolution; in a multi-node
///    deployment it is assigned by a Hybrid Logical Clock
///    ([architecture §10](../../../docs/02-architecture.md#10-distribution--consensus-later-phase)),
///    whereas `committed_at` is the *physical* wall-clock commit instant. Keeping
///    `committed_at` a first-class stored fact keeps the audit answer honest
///    when that day comes, with no format change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    /// The transaction that wrote this version.
    pub txn_id: TxnId,
    /// The commit timestamp — when the database durably accepted the write.
    pub committed_at: SystemTimeMicros,
    /// Who or what issued the write.
    pub principal: Principal,
}

impl Provenance {
    /// Bundle the three provenance facts.
    #[must_use]
    pub const fn new(txn_id: TxnId, committed_at: SystemTimeMicros, principal: Principal) -> Self {
        Self {
            txn_id,
            committed_at,
            principal,
        }
    }
}

/// The SQL name of the [`TxnId`] provenance pseudo-column ([STL-247]).
///
/// [STL-247]: https://allegromusic.atlassian.net/browse/STL-247
pub const TXN_ID_COLUMN: &str = "_stele_txn_id";

/// The SQL name of the `committed_at` provenance pseudo-column ([STL-247]).
pub const COMMITTED_AT_COLUMN: &str = "_stele_committed_at";

/// The SQL name of the [`Principal`] provenance pseudo-column ([STL-247]).
pub const PRINCIPAL_COLUMN: &str = "_stele_principal";

/// The three provenance **pseudo-columns** and their SQL types, in canonical order.
///
/// The queryable surface over the [`Provenance`] stored inline on every version
/// ([STL-247], [architecture §8](../../../docs/02-architecture.md#8-lineage--provenance-subsystem)).
/// They read a row's provenance inline in a `SELECT`, the way Postgres exposes
/// `xmin` / `ctid`: a name not in any table's user schema, so it is **hidden**
/// from `SELECT *` and `\d` and surfaces only when named explicitly. The order
/// is the fixed layout a read appends after the table's own columns —
/// [`TXN_ID_COLUMN`], then [`COMMITTED_AT_COLUMN`], then [`PRINCIPAL_COLUMN`].
///
/// Types mirror the stored facts: the writing [`TxnId`] is an `int8` (the `u64`
/// id carried as its `i64` bit pattern, the same lossless round-trip the segment
/// format uses), the commit instant a `timestamptz`, and the [`Principal`] the
/// `text` identity bytes.
///
/// [STL-247]: https://allegromusic.atlassian.net/browse/STL-247
pub const PSEUDO_COLUMNS: [(&str, LogicalType); 3] = [
    (TXN_ID_COLUMN, LogicalType::Int8),
    (COMMITTED_AT_COLUMN, LogicalType::TimestampTz),
    (PRINCIPAL_COLUMN, LogicalType::Text),
];

/// The SQL type of the provenance pseudo-column named `name`, or `None`.
///
/// The lookup the binder uses to resolve a projected or `WHERE` name that is not a
/// user column ([STL-247]).
#[must_use]
pub fn pseudo_column_type(name: &str) -> Option<LogicalType> {
    PSEUDO_COLUMNS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, ty)| *ty)
}

/// Whether `name` is one of the provenance pseudo-columns ([STL-247]).
///
/// A read resolves a projected or `WHERE` name against the table's own columns
/// **first**, so a (discouraged) user column that happened to share one of these
/// names would shadow the pseudo-column rather than collide — the Postgres
/// system-column posture.
#[must_use]
pub fn is_pseudo_column(name: &str) -> bool {
    PSEUDO_COLUMNS.iter().any(|(n, _)| *n == name)
}
