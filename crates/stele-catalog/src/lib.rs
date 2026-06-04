//! Versioned catalog & metadata.
//!
//! The catalog is itself bitemporal — an `AS OF` query in the past resolves
//! columns using the schema that was in effect *then*
//! ([`docs/02-architecture.md` §5](../../../docs/02-architecture.md#5-catalog--metadata)).
//!
//! Scaffold only at v0.1; resolution against `sys_time` snapshots lands with
//! the binder in [`stele-sql`].

#![allow(dead_code)]
