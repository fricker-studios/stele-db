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
/// produced. Adjacent versions never overlap: a schema change closes the open
/// version exactly where the next one starts. A live table's chain tiles
/// `[creation, +∞)` with exactly one open tail; a [dropped](Self::drop_table)
/// table's tail is *closed* (and re-creating the name later may leave a gap for
/// the dropped era) — so resolution is by interval containment, never by
/// assuming an open tail.
///
/// v0.1 keeps the catalog **in memory**. Persisting it onto the same
/// sealed-segment substrate as user tables ("eat our own dog food") and wiring
/// the [`SchemaId`] into the segment-footer write path are follow-ups; the
/// resolution semantics this type fixes are what the binder and the on-disk
/// `schema_id` reference both build on.
#[derive(Debug)]
pub struct Catalog {
    /// Per table, its schema versions ordered by `sys_from`, oldest first. The
    /// last entry is the open one (`sys_to == SYSTEM_TIME_OPEN`) while the table
    /// is live; a dropped table's last entry is closed instead.
    tables: BTreeMap<String, Vec<SchemaVersion>>,
    /// Monotonic schema-id allocator. Ids are never reused, so a footer's
    /// recorded id always names exactly one historical schema. Starts at
    /// [`FIRST_SCHEMA_ID`], reserving `0` (see [`SchemaId`]).
    next_schema_id: u32,
}

/// The first id the catalog allocates. Id `0` is reserved for the implicit v0.1
/// segment schema (`SCHEMA_ID_IMPLICIT_VERSION` in `stele-storage`), so a sealed
/// segment's `schema_id == 0` can never be mistaken for a catalog-allocated
/// table schema once the footer→catalog lookup lands.
const FIRST_SCHEMA_ID: u32 = 1;

impl Default for Catalog {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            next_schema_id: FIRST_SCHEMA_ID,
        }
    }
}

impl Catalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The table's currently *live* (open-tailed) schema version, or `None` if
    /// the table is absent or has been dropped.
    ///
    /// A dropped table keeps its history — older versions still
    /// [`resolve`](Self::resolve) `AS OF` the past — but has no open tail, so it
    /// is not a valid target for a mutation (`add_column`, `drop_table`) and a
    /// fresh `create_table` of the same name continues its timeline rather than
    /// colliding.
    fn open_version(&self, name: &str) -> Option<&SchemaVersion> {
        let last = self.tables.get(name)?.last()?;
        (last.sys_to == SYSTEM_TIME_OPEN).then_some(last)
    }

    /// Hand out the next never-reused schema id.
    ///
    /// Uses a checked increment: ids must stay unique for footer→schema
    /// resolution, so an exhausted `u32` space fails loudly rather than wrapping
    /// and silently reusing an id.
    fn alloc_schema_id(&mut self) -> Result<SchemaId, CatalogError> {
        let id = SchemaId(self.next_schema_id);
        self.next_schema_id = self
            .next_schema_id
            .checked_add(1)
            .ok_or(CatalogError::SchemaIdExhausted)?;
        Ok(id)
    }

    /// Register a table whose first schema version takes effect at system time
    /// `at`.
    ///
    /// If `name` was previously dropped via [`drop_table`](Self::drop_table), this *continues*
    /// that name's timeline: a fresh schema version is appended after the gap the
    /// drop left, so a read `AS OF` an instant inside the old, dropped era still
    /// resolves the original schema while reads after `at` see the new one. A
    /// name that is currently live cannot be re-created.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::SystemTimeExhausted`] if `at` is at or past the
    ///   open-interval sentinel [`SYSTEM_TIME_OPEN`].
    /// - [`CatalogError::TableAlreadyExists`] if `name` is already live.
    /// - [`CatalogError::TableRecreatedBeforeDrop`] if `name` was dropped and `at`
    ///   precedes the drop — re-creation may not overlap the dropped era.
    /// - [`CatalogError::DuplicateColumn`] / [`CatalogError::InvalidColumnName`]
    ///   if the column list is malformed.
    /// - [`CatalogError::SchemaIdExhausted`] if the `u32` id space is used up.
    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        columns: Vec<ColumnDef>,
        temporal: TableTemporal,
        at: SystemTimeMicros,
    ) -> Result<SchemaId, CatalogError> {
        let name = name.into();
        if at >= SYSTEM_TIME_OPEN {
            return Err(CatalogError::SystemTimeExhausted {
                table: name,
                at: at.0,
            });
        }
        if self.open_version(&name).is_some() {
            return Err(CatalogError::TableAlreadyExists(name));
        }
        let schema_id = self.alloc_schema_id()?;
        let schema = TableSchema::new(schema_id, columns, temporal)?;
        let version = SchemaVersion {
            sys_from: at,
            sys_to: SYSTEM_TIME_OPEN,
            schema,
        };
        match self.tables.get_mut(&name) {
            // A previously dropped name: extend its existing version chain. The
            // new open version must begin at or after the prior (closed) version's
            // end, or the two system-time intervals would overlap.
            Some(versions) => {
                let dropped_at = versions
                    .last()
                    .expect("a registered table always has at least one schema version")
                    .sys_to;
                if at < dropped_at {
                    return Err(CatalogError::TableRecreatedBeforeDrop {
                        table: name,
                        at: at.0,
                        dropped_at: dropped_at.0,
                    });
                }
                versions.push(version);
            }
            None => {
                self.tables.insert(name, vec![version]);
            }
        }
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
    /// - [`CatalogError::SystemTimeExhausted`] if `at` is at or past the
    ///   open-interval sentinel [`SYSTEM_TIME_OPEN`] — closing the prior version
    ///   there and opening a new one at the same sentinel would silently drop the
    ///   change for every finite snapshot.
    /// - [`CatalogError::DuplicateColumn`] if the table already has the column.
    /// - [`CatalogError::NonMonotonicSchemaChange`] if `at` is not strictly after
    ///   the current version's `sys_from` — system time never moves backward, and
    ///   a zero-width version would break the gap-free/non-overlapping invariant.
    /// - [`CatalogError::SchemaIdExhausted`] if the `u32` id space is used up.
    pub fn add_column(
        &mut self,
        name: &str,
        column: ColumnDef,
        at: SystemTimeMicros,
    ) -> Result<SchemaId, CatalogError> {
        // Only a live table can take a schema change; a dropped name does not
        // currently exist, so adding to it would silently resurrect it.
        let current = self
            .open_version(name)
            .ok_or_else(|| CatalogError::UnknownTable(name.to_owned()))?;

        if at >= SYSTEM_TIME_OPEN {
            return Err(CatalogError::SystemTimeExhausted {
                table: name.to_owned(),
                at: at.0,
            });
        }
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
        let schema_id = self.alloc_schema_id()?;
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

    /// Logically drop a table, effective at system time `at`.
    ///
    /// The drop is a **catalog version transition, not a deletion**: it closes
    /// the table's open schema version at `at` and appends nothing. From `at`
    /// onward the name no longer [`resolve`](Self::resolve)s, but a read `AS OF`
    /// any earlier instant still sees the table under the schema that was live
    /// then — its history, and every sealed segment, is untouched (the
    /// no-in-place-mutation invariant). The name may later be re-created with
    /// [`create_table`](Self::create_table).
    ///
    /// Returns the [`SchemaId`] of the version that was closed.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::UnknownTable`] if `name` is absent or already dropped —
    ///   only a live table can be dropped.
    /// - [`CatalogError::SystemTimeExhausted`] if `at` is at or past the
    ///   open-interval sentinel [`SYSTEM_TIME_OPEN`]; closing there would leave a
    ///   zero-width final era no finite snapshot could fall in.
    /// - [`CatalogError::NonMonotonicSchemaChange`] if `at` is not strictly after
    ///   the open version's start — system time never moves backward, and a
    ///   zero-width version would break the gap-free/non-overlapping invariant.
    pub fn drop_table(
        &mut self,
        name: &str,
        at: SystemTimeMicros,
    ) -> Result<SchemaId, CatalogError> {
        let (sys_from, schema_id) = {
            let current = self
                .open_version(name)
                .ok_or_else(|| CatalogError::UnknownTable(name.to_owned()))?;
            (current.sys_from, current.schema.schema_id())
        };
        if at >= SYSTEM_TIME_OPEN {
            return Err(CatalogError::SystemTimeExhausted {
                table: name.to_owned(),
                at: at.0,
            });
        }
        if at <= sys_from {
            return Err(CatalogError::NonMonotonicSchemaChange {
                table: name.to_owned(),
                at: at.0,
                current_from: sys_from.0,
            });
        }
        // Close the open tail in place; appending no successor is what makes the
        // table cease to exist from `at` onward.
        self.tables
            .get_mut(name)
            .expect("open version checked above")
            .last_mut()
            .expect("a registered table always has at least one schema version")
            .sys_to = at;
        Ok(schema_id)
    }

    /// Resolve a table name to the schema in effect at `snapshot` — the
    /// binder-facing read ([architecture §6](../../../docs/02-architecture.md#6-query-layer)).
    ///
    /// Returns `None` if the table does not exist, did not yet exist at
    /// `snapshot` (its first version starts strictly after it), or had already
    /// been [dropped](Self::drop_table) by then (the drop closed its final era at
    /// or before `snapshot`). Containment is half-open: the returned version
    /// satisfies `sys_from <= snapshot < sys_to`, matching how the storage core
    /// bounds a row's system-time interval.
    ///
    /// Versions are kept in ascending `sys_from` order, so the lookup is an
    /// `O(log n)` binary search rather than a scan — planning-time resolution
    /// stays cheap as a table's DDL history grows.
    #[must_use]
    pub fn resolve(&self, table_name: &str, snapshot: SystemTimeMicros) -> Option<&TableSchema> {
        let versions = self.tables.get(table_name)?;
        // The number of versions that start at or before `snapshot`; the
        // candidate is the last of them. `0` means `snapshot` precedes the
        // table's first version.
        let started = versions.partition_point(|v| v.sys_from <= snapshot);
        let candidate = &versions[started.checked_sub(1)?];
        // A dropped (and possibly re-created) name can leave gaps between
        // versions, so this `sys_to` check also rejects a snapshot that falls
        // past the candidate's end — whether that is the dropped era's tail or a
        // gap before the next re-creation.
        (snapshot < candidate.sys_to).then_some(&candidate.schema)
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

    #[test]
    fn allocation_reserves_id_zero_for_the_implicit_segment_schema() {
        let mut cat = Catalog::new();
        // First catalog-allocated id is 1, not 0 — 0 belongs to storage's
        // implicit v0.1 segment schema, so a footer's `schema_id == 0` can never
        // collide with a catalog-allocated table schema.
        let first = cat
            .create_table(
                "t",
                vec![col("a")],
                TableTemporal::system_only(),
                SystemTimeMicros(1),
            )
            .expect("create");
        assert_eq!(first, SchemaId(1));
        let second = cat
            .add_column("t", col("b"), SystemTimeMicros(2))
            .expect("add column");
        assert_eq!(second, SchemaId(2));
    }

    #[test]
    fn a_schema_change_at_or_past_the_open_sentinel_is_rejected() {
        let mut cat = Catalog::new();
        // create_table cannot stamp the +∞ sentinel — it would be a zero-width,
        // unresolvable version.
        assert_eq!(
            cat.create_table(
                "t",
                vec![col("a")],
                TableTemporal::system_only(),
                SYSTEM_TIME_OPEN
            ),
            Err(CatalogError::SystemTimeExhausted {
                table: "t".to_owned(),
                at: i64::MAX,
            })
        );
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(1),
        )
        .expect("create");
        // …and neither can a later add_column, even though MAX is "after" the
        // current version's finite start.
        assert_eq!(
            cat.add_column("t", col("b"), SYSTEM_TIME_OPEN),
            Err(CatalogError::SystemTimeExhausted {
                table: "t".to_owned(),
                at: i64::MAX,
            })
        );
    }

    #[test]
    fn drop_table_logically_closes_the_open_version_without_deleting_history() {
        let mut cat = Catalog::new();
        let id = cat
            .create_table(
                "t",
                vec![col("a")],
                TableTemporal::system_only(),
                SystemTimeMicros(100),
            )
            .expect("create");

        // Dropping closes the open era and returns the id it closed.
        assert_eq!(cat.drop_table("t", SystemTimeMicros(200)), Ok(id));

        // Before the drop the table still resolves (history preserved); at and
        // after the drop it is gone — a catalog transition, not a deletion.
        assert_eq!(
            cat.resolve("t", SystemTimeMicros(150))
                .map(TableSchema::schema_id),
            Some(id)
        );
        assert!(cat.resolve("t", SystemTimeMicros(200)).is_none());
        assert!(cat.resolve("t", SystemTimeMicros(250)).is_none());
        // …and the id is still reachable by footer lookup for old segments.
        assert_eq!(cat.schema_by_id(id).map(TableSchema::schema_id), Some(id));
    }

    #[test]
    fn drop_table_unknown_or_already_dropped_is_unknown_table() {
        let mut cat = Catalog::new();
        assert_eq!(
            cat.drop_table("missing", SystemTimeMicros(1)),
            Err(CatalogError::UnknownTable("missing".to_owned()))
        );
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(1),
        )
        .expect("create");
        cat.drop_table("t", SystemTimeMicros(2)).expect("drop");
        // A second drop sees no live version.
        assert_eq!(
            cat.drop_table("t", SystemTimeMicros(3)),
            Err(CatalogError::UnknownTable("t".to_owned()))
        );
    }

    #[test]
    fn drop_table_must_advance_system_time_and_cannot_use_the_open_sentinel() {
        let mut cat = Catalog::new();
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(10),
        )
        .expect("create");
        // At or before the open version's start: no backward or zero-width close.
        assert_eq!(
            cat.drop_table("t", SystemTimeMicros(10)),
            Err(CatalogError::NonMonotonicSchemaChange {
                table: "t".to_owned(),
                at: 10,
                current_from: 10,
            })
        );
        // The +∞ sentinel can never be a finite drop instant.
        assert_eq!(
            cat.drop_table("t", SYSTEM_TIME_OPEN),
            Err(CatalogError::SystemTimeExhausted {
                table: "t".to_owned(),
                at: i64::MAX,
            })
        );
    }

    #[test]
    fn a_dropped_name_can_be_re_created_and_continues_its_timeline() {
        let mut cat = Catalog::new();
        let first = cat
            .create_table(
                "t",
                vec![col("a")],
                TableTemporal::system_only(),
                SystemTimeMicros(100),
            )
            .expect("create");
        cat.drop_table("t", SystemTimeMicros(200)).expect("drop");

        // Re-create after the drop — a fresh schema version under a new id.
        let second = cat
            .create_table(
                "t",
                vec![col("b")],
                TableTemporal::system_only(),
                SystemTimeMicros(300),
            )
            .expect("re-create");
        assert_ne!(first, second);

        // The old era resolves to the old schema, the dropped gap to nothing, and
        // the new era to the new schema — the name's whole timeline survives.
        assert_eq!(
            cat.resolve("t", SystemTimeMicros(150))
                .map(TableSchema::schema_id),
            Some(first)
        );
        assert!(cat.resolve("t", SystemTimeMicros(250)).is_none());
        assert_eq!(
            cat.resolve("t", SystemTimeMicros(350))
                .map(TableSchema::schema_id),
            Some(second)
        );
    }

    #[test]
    fn re_creating_a_name_before_its_drop_overlaps_and_is_rejected() {
        let mut cat = Catalog::new();
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(100),
        )
        .expect("create");
        cat.drop_table("t", SystemTimeMicros(200)).expect("drop");
        assert_eq!(
            cat.create_table(
                "t",
                vec![col("b")],
                TableTemporal::system_only(),
                SystemTimeMicros(150)
            ),
            Err(CatalogError::TableRecreatedBeforeDrop {
                table: "t".to_owned(),
                at: 150,
                dropped_at: 200,
            })
        );
    }

    #[test]
    fn add_column_on_a_dropped_table_does_not_resurrect_it() {
        let mut cat = Catalog::new();
        cat.create_table(
            "t",
            vec![col("a")],
            TableTemporal::system_only(),
            SystemTimeMicros(100),
        )
        .expect("create");
        cat.drop_table("t", SystemTimeMicros(200)).expect("drop");
        assert_eq!(
            cat.add_column("t", col("b"), SystemTimeMicros(300)),
            Err(CatalogError::UnknownTable("t".to_owned()))
        );
        // Still dropped — the failed add changed nothing.
        assert!(cat.resolve("t", SystemTimeMicros(300)).is_none());
    }

    /// DoD bullet 2: over a randomized create + `add_column` history (no drops),
    /// every table's schema-version intervals tile `[creation, +∞)` with no gaps
    /// and no overlaps, with exactly one open tail and strictly positive widths.
    /// Walks the private interval chain directly — the structural invariant the
    /// public `resolve` rests on for *live* tables.
    ///
    /// Dropping relaxes this: a dropped table's tail is closed (no open tail),
    /// and re-creating the name may leave a gap between eras. Non-overlap and
    /// positive widths still hold; those drop/re-create cases are pinned by the
    /// dedicated `drop_table` tests above rather than this gap-free sweep.
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
