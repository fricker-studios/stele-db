//! The versioned catalog.
//!
//! Each table is a **chain of schema versions on the system-time axis** — the
//! same bitemporal shape the storage core gives user rows
//! ([architecture §2](../../../docs/02-architecture.md#2-the-bitemporal-record-model),
//! [§5](../../../docs/02-architecture.md#5-catalog--metadata)). Every DDL change
//! appends a new version stamped with the system time it took effect, closing
//! the prior version on the same axis. [`Catalog::resolve`] then returns the
//! schema whose system-time interval contains a snapshot, so a name resolved
//! `AS OF` a past instant sees the columns that were in effect *then* — the
//! property that makes time-travel survive schema evolution.

use std::collections::BTreeMap;

use stele_common::time::{SYSTEM_TIME_OPEN, SystemTimeMicros};

use crate::schema::{ColumnDef, SchemaId, TableSchema};
use crate::{CatalogError, TableTemporal};

/// One system-time-bounded version of a table's schema: the table held the shape
/// `schema` for every system time in the half-open interval `[sys_from, sys_to)`.
/// The current version's `sys_to` is the [`SYSTEM_TIME_OPEN`] sentinel.
#[derive(Debug, Clone)]
struct SchemaVersion {
    sys_from: SystemTimeMicros,
    sys_to: SystemTimeMicros,
    schema: TableSchema,
}

/// A versioned catalog of table schemas.
///
/// Holds, per table, the ordered chain of schema versions its DDL history has
/// produced. The chain is gap-free and non-overlapping by construction: a DDL
/// change closes the open version exactly where the new one starts, so the
/// versions tile `[creation, +∞)` and exactly one is open at any time.
///
/// v0.1 keeps the catalog **in memory**. Persisting it onto the same
/// sealed-segment substrate as user tables ("eat our own dog food") and wiring
/// the [`SchemaId`] into the segment-footer write path are follow-ups; the
/// resolution semantics this type fixes are what the binder and the on-disk
/// `schema_id` reference both build on.
#[derive(Debug, Default)]
pub struct Catalog {
    /// Per table, its schema versions ordered by `sys_from`, oldest first. The
    /// last entry is always the open one (`sys_to == SYSTEM_TIME_OPEN`).
    tables: BTreeMap<String, Vec<SchemaVersion>>,
    /// Monotonic schema-id allocator. Ids are never reused, so a footer's
    /// recorded id always names exactly one historical schema.
    next_schema_id: u32,
}

impl Catalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Hand out the next never-reused schema id.
    fn alloc_schema_id(&mut self) -> SchemaId {
        let id = SchemaId(self.next_schema_id);
        self.next_schema_id += 1;
        id
    }

    /// Register a brand-new table whose first schema version takes effect at
    /// system time `at`.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::TableAlreadyExists`] if `name` is already registered.
    /// - [`CatalogError::DuplicateColumn`] / [`CatalogError::InvalidColumnName`]
    ///   if the column list is malformed.
    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnDef>,
        temporal: TableTemporal,
        at: SystemTimeMicros,
    ) -> Result<SchemaId, CatalogError> {
        let name = name.into();
        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableAlreadyExists(name));
        }
        let schema_id = self.alloc_schema_id();
        let schema = TableSchema::new(schema_id, columns, temporal)?;
        self.tables.insert(
            name,
            vec![SchemaVersion {
                sys_from: at,
                sys_to: SYSTEM_TIME_OPEN,
                schema,
            }],
        );
        Ok(schema_id)
    }

    /// Append a column to an existing table, effective at system time `at`.
    ///
    /// Closes the table's current open schema version at `at` and opens a new
    /// one — under a fresh [`SchemaId`] — carrying the added column. A read
    /// `AS OF` any snapshot before `at` still [`resolve`](Self::resolve)s to the
    /// old schema, which is exactly the bitemporal-catalog guarantee.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::UnknownTable`] if `name` is not registered.
    /// - [`CatalogError::DuplicateColumn`] if the table already has the column.
    /// - [`CatalogError::NonMonotonicSchemaChange`] if `at` is not strictly after
    ///   the current version's `sys_from` — system time never moves backward, and
    ///   a zero-width version would break the gap-free/non-overlapping invariant.
    pub fn add_column(
        &mut self,
        name: &str,
        column: ColumnDef,
        at: SystemTimeMicros,
    ) -> Result<SchemaId, CatalogError> {
        let current = self
            .tables
            .get(name)
            .ok_or_else(|| CatalogError::UnknownTable(name.to_owned()))?
            .last()
            .expect("a registered table always has at least one schema version");

        if at <= current.sys_from {
            return Err(CatalogError::NonMonotonicSchemaChange {
                table: name.to_owned(),
                at: at.0,
                current_from: current.sys_from.0,
            });
        }
        if current.schema.column(column.name()).is_some() {
            return Err(CatalogError::DuplicateColumn(column.name().to_owned()));
        }

        let mut columns = current.schema.columns().to_vec();
        columns.push(column);
        let temporal = current.schema.temporal().clone();
        let schema_id = self.alloc_schema_id();
        let schema = TableSchema::new(schema_id, columns, temporal)?;

        // Close the prior version at `at` and append the new open one — the two
        // intervals meet exactly, leaving no gap and no overlap.
        let versions = self
            .tables
            .get_mut(name)
            .expect("table presence checked above");
        versions.last_mut().expect("non-empty checked above").sys_to = at;
        versions.push(SchemaVersion {
            sys_from: at,
            sys_to: SYSTEM_TIME_OPEN,
            schema,
        });
        Ok(schema_id)
    }

    /// Resolve a table name to the schema in effect at `snapshot` — the
    /// binder-facing read ([architecture §6](../../../docs/02-architecture.md#6-query-layer)).
    ///
    /// Returns `None` if the table does not exist *or* did not yet exist at
    /// `snapshot` (its first version starts strictly after it). Containment is
    /// half-open: the returned version satisfies `sys_from <= snapshot < sys_to`,
    /// matching how the storage core bounds a row's system-time interval.
    #[must_use]
    pub fn resolve(&self, table_name: &str, snapshot: SystemTimeMicros) -> Option<&TableSchema> {
        let versions = self.tables.get(table_name)?;
        versions
            .iter()
            .find(|v| v.sys_from <= snapshot && snapshot < v.sys_to)
            .map(|v| &v.schema)
    }

    /// Look up a schema by the id a sealed segment's footer records.
    ///
    /// Footers store the [`SchemaId`] their rows were written under
    /// ([architecture §3.2](../../../docs/02-architecture.md#32-on-disk-segment-format));
    /// this reverses that reference so a scan can interpret old bytes under their
    /// own schema regardless of how the table has since evolved.
    #[must_use]
    pub fn schema_by_id(&self, id: SchemaId) -> Option<&TableSchema> {
        self.tables
            .values()
            .flatten()
            .map(|v| &v.schema)
            .find(|s| s.schema_id() == id)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss
    )]

    use super::*;
    use stele_common::types::LogicalType;

    /// Tiny xorshift64* — deterministic, dependency-free (the workspace keeps no
    /// proptest/quickcheck dep; seeded determinism is the house style, ADR-0010).
    struct Rng(u64);
    impl Rng {
        const fn new(seed: u64) -> Self {
            Self(seed ^ 0x9E37_79B9_7F4A_7C15)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn range(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    fn col(name: &str) -> ColumnDef {
        ColumnDef::new(name, LogicalType::Int8).expect("non-empty name")
    }

    #[test]
    fn create_then_resolve_returns_the_schema_inside_its_interval() {
        let mut cat = Catalog::new();
        let id = cat
            .create_table(
                "t",
                vec![col("a")],
                TableTemporal::system_only(),
                SystemTimeMicros(100),
            )
            .expect("create");

        // Before creation: nothing.
        assert!(cat.resolve("t", SystemTimeMicros(99)).is_none());
        // At and after creation: the schema, by id too.
        let s = cat.resolve("t", SystemTimeMicros(100)).expect("resolves");
        assert_eq!(s.schema_id(), id);
        assert_eq!(
            cat.resolve("t", SystemTimeMicros(1_000))
                .map(TableSchema::schema_id),
            Some(id)
        );
        assert_eq!(cat.schema_by_id(id).map(TableSchema::schema_id), Some(id));
        assert!(cat.resolve("missing", SystemTimeMicros(100)).is_none());
    }

    #[test]
    fn duplicate_table_and_column_and_unknown_table_are_errors() {
        let mut cat = Catalog::new();
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(1),
        )
        .expect("create");

        assert_eq!(
            cat.create_table(
                "t",
                vec![col("b")],
                TableTemporal::system_only(),
                SystemTimeMicros(2)
            ),
            Err(CatalogError::TableAlreadyExists("t".to_owned()))
        );
        assert_eq!(
            cat.add_column("t", col("a"), SystemTimeMicros(2)),
            Err(CatalogError::DuplicateColumn("a".to_owned()))
        );
        assert_eq!(
            cat.add_column("missing", col("x"), SystemTimeMicros(2)),
            Err(CatalogError::UnknownTable("missing".to_owned()))
        );
        assert_eq!(
            cat.create_table(
                "u",
                vec![col("a"), col("a")],
                TableTemporal::system_only(),
                SystemTimeMicros(3)
            ),
            Err(CatalogError::DuplicateColumn("a".to_owned()))
        );
    }

    #[test]
    fn a_schema_change_must_advance_system_time() {
        let mut cat = Catalog::new();
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(10),
        )
        .expect("create");
        // At or before the current version's start is rejected — no backward or
        // zero-width versions.
        assert_eq!(
            cat.add_column("t", col("b"), SystemTimeMicros(10)),
            Err(CatalogError::NonMonotonicSchemaChange {
                table: "t".to_owned(),
                at: 10,
                current_from: 10,
            })
        );
        assert_eq!(
            cat.add_column("t", col("b"), SystemTimeMicros(5)),
            Err(CatalogError::NonMonotonicSchemaChange {
                table: "t".to_owned(),
                at: 5,
                current_from: 10,
            })
        );
    }

    /// DoD bullet 2: over a randomized DDL history, every table's schema-version
    /// intervals tile `[creation, +∞)` with no gaps and no overlaps, with exactly
    /// one open tail and strictly positive widths. Walks the private interval
    /// chain directly — the structural invariant the public `resolve` rests on.
    #[test]
    fn schema_version_intervals_are_gap_free_and_non_overlapping() {
        let mut rng = Rng::new(0xCA7A_106D);
        for _ in 0..200 {
            let mut cat = Catalog::new();
            let tables = ["a", "b", "c"];
            let mut clock = 1_i64;
            for t in tables {
                clock += 1 + rng.range(5) as i64;
                cat.create_table(
                    t,
                    vec![col("c0")],
                    TableTemporal::system_only(),
                    SystemTimeMicros(clock),
                )
                .expect("create");
            }
            let adds = rng.range(40);
            for i in 0..adds {
                clock += 1 + rng.range(5) as i64;
                let t = tables[rng.range(tables.len() as u64) as usize];
                let name = format!("c{}", i + 1);
                cat.add_column(t, col(&name), SystemTimeMicros(clock))
                    .expect("add column");
            }
            for t in tables {
                assert_contiguous(&cat, t);
            }
        }
    }

    /// Assert one table's version chain is a gap-free, non-overlapping tiling
    /// with a single open tail.
    fn assert_contiguous(cat: &Catalog, table: &str) {
        let versions = &cat.tables[table];
        assert!(!versions.is_empty(), "{table} has no versions");
        for w in versions.windows(2) {
            assert!(
                w[0].sys_from < w[0].sys_to,
                "{table}: zero/negative-width version"
            );
            assert_eq!(
                w[0].sys_to, w[1].sys_from,
                "{table}: gap or overlap between adjacent versions"
            );
        }
        let last = versions.last().expect("non-empty");
        assert!(last.sys_from < last.sys_to, "{table}: zero-width tail");
        assert_eq!(last.sys_to, SYSTEM_TIME_OPEN, "{table}: tail must be open");
        assert_eq!(
            versions
                .iter()
                .filter(|v| v.sys_to == SYSTEM_TIME_OPEN)
                .count(),
            1,
            "{table}: exactly one version may be open"
        );
    }
}
