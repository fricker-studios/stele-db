//! Write-Ahead Log — Stele's durability point.
//!
//! The WAL is an append-only, segmented commit log with CRC32C-checksummed
//! records and group-commit fsync. **The WAL fsync is the only durability point
//! in the engine** ([architecture §3.4, §12 invariant 2](../../../../docs/02-architecture.md#34-write-path-sequence));
//! everything downstream (delta tier, sealed segments, tiering) is recoverable
//! from it.
//!
//! ## Surface
//!
//! ```ignore
//! let wal = Wal::open(disk, WalConfig::default())?;
//! let pos = wal.append(b"<redo payload>")?;
//! let durable = wal.commit(pos);   // a future
//! wal.tick()?;                     // drain — group-commit fsync
//! durable.await?;                  // resolves once tick covers `pos`
//!
//! for rec in wal.replay_from(Checkpoint::BEGIN) {
//!     let payload = rec?;          // CRC-validated
//!     // ...
//! }
//! ```
//!
//! ## Invariants enforced here
//!
//! 1. **Durability is at fsync, not append.** `append` *stages* a record;
//!    an fsync of the underlying file is what makes it durable. Two paths
//!    fsync today: [`Wal::tick`] (the group-commit drain) and the
//!    segment-boundary sync inside rotation. The `commit` future resolves
//!    once *any* fsync has covered its target — typically via `tick`, but
//!    rotation can also satisfy a pending commit when its target is in the
//!    closing segment.
//! 2. **Replay never proceeds past corruption.** A CRC-mismatched or
//!    short-read frame stops the iterator after yielding one `Err`. This is
//!    the torn-write contract from
//!    [testing strategy §6](../../../../docs/06-testing-strategy.md#6-crash--recovery-testing).
//! 3. **No `tokio::*` types appear on this module's surface.** I/O enters
//!    through the [`Disk`] / [`DiskFile`] traits; `commit` is a plain
//!    [`std::future::Future`].

mod disk;
mod log;
mod record;
mod replay;
mod segment;

pub use disk::{Disk, DiskFile};
pub use log::{Checkpoint, Commit, LogOffset, Wal, WalConfig, WalError};
pub use record::MAX_PAYLOAD_LEN;
pub use replay::Replay;
