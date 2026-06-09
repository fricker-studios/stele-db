//! Transaction manager — MVCC layered on the append-only store.
//!
//! Snapshot isolation is the v1 default; serializable (SSI) is a later opt-in
//! ([`docs/02-architecture.md` §9](../../../docs/02-architecture.md#9-transaction--concurrency-model),
//! [ADR-0008](../../../docs/adr/0008-mvcc-on-append-only.md)).
//!
//! The append-only store already *is* multi-version — every business key carries
//! a chain of system-time-stamped versions — so MVCC needs little new machinery.
//! What it does need is a single authority over the system-time axis *across*
//! transactions, and that is [`TxnManager`]:
//!
//! * **Snapshot acquisition.** [`TxnManager::begin`] hands a transaction a read
//!   snapshot — a system-time point drawn through the injectable
//!   [`Clock`](stele_common::time::Clock). Reads resolve at that snapshot via
//!   [`Transaction::snapshot`] →
//!   [`Delta::range_scan`](stele_storage::delta::Delta::range_scan).
//! * **Commit-time assignment.** [`TxnManager::commit`] assigns the commit
//!   timestamp the transaction's versions are stamped with (`sys_from = commit`)
//!   and durably logs a [`CommitRecord`] to the WAL.
//! * **Conflict detection.** Two transactions that overlap on the same key cannot
//!   both commit; the first wins and the loser gets [`TxnError::Conflict`], a
//!   clean retry signal.
//!
//! The mechanism and its correctness argument live in [`manager`]; the durable
//! commit marker in [`commit_record`].

pub mod chain;
pub mod commit_record;
pub mod manager;

pub use chain::{
    ChainError, ChainHead, RecoveredChain, verify_chain, verify_chain_recover, verify_chain_to,
};
pub use commit_record::{CommitRecord, CommitRecordError};
pub use manager::{Committed, RecoverError, Transaction, TxnError, TxnManager};
