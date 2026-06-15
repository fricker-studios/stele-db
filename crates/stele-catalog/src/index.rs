//! Secondary-index metadata ([STL-233]).
//!
//! The catalog records *which* secondary indexes exist — name, table, kind, and
//! the indexed column(s) — while the access structures themselves live with the
//! engine. An index is **derived and rebuildable** state (the validity-index
//! posture, [ADR-0023]): it can change how fast a query answers, never *what*
//! it answers, so its metadata is deliberately **not** versioned on the
//! system-time axis the way table schemas are. A past `AS OF` read does not
//! need to know what indexes existed then — it would get the same rows either
//! way — so the catalog keeps only the *live* index set: `CREATE INDEX` adds an
//! entry, `DROP INDEX` removes it, and dropping a table removes the indexes
//! that referenced it.
//!
//! Durability mirrors table DDL: the session engine appends each index DDL
//! mutation to the durable catalog log ([ADR-0028]) and replays it on boot,
//! reproducing this live set exactly.
//!
//! [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
//! [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md

use crate::CatalogError;

/// The access-structure family an index is built with.
///
/// The v0.3 substrate ships the default ordered structure and the equality-only
/// hash family ([STL-238]); the valid-time interval family plugs in as a further
/// variant ([STL-241]) without touching the lifecycle around them.
///
/// [STL-238]: https://allegromusic.atlassian.net/browse/STL-238
/// [STL-241]: https://allegromusic.atlassian.net/browse/STL-241
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexKind {
    /// An ordered (B-tree-shaped) index over one value column — the default,
    /// as in Postgres' bare `CREATE INDEX`. Serves equality *and* range probes.
    #[default]
    BTree,
    /// A hash index over one value column (`CREATE INDEX … USING HASH`) — serves
    /// equality probes only (it cannot walk its keys in value order, so it
    /// declines ranges), and pairs with the per-segment bloom filters that
    /// accelerate hash-key point lookups and `MERGE` probes ([STL-238]).
    Hash,
}

/// One live secondary index: its name, the table it accelerates, its
/// [`IndexKind`], and the value column(s) it covers.
///
/// Index names share one namespace across the catalog (a `DROP INDEX` names no
/// table, so the name alone must resolve). The column list is carried as a
/// `Vec` so multi-column indexes are a metadata-compatible extension, but the
/// v0.3 substrate creates single-column indexes only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    name: String,
    table: String,
    kind: IndexKind,
    columns: Vec<String>,
}

impl IndexDef {
    /// Build an index definition.
    ///
    /// Shape-only validation (non-empty name, at least one column); whether the
    /// table and columns *exist* is checked by
    /// [`Catalog::create_index`](crate::Catalog::create_index), which sees the
    /// live schema.
    ///
    /// # Errors
    ///
    /// [`CatalogError::InvalidIndexName`] if `name` is empty;
    /// [`CatalogError::IndexHasNoColumns`] if `columns` is empty.
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        kind: IndexKind,
        columns: Vec<String>,
    ) -> Result<Self, CatalogError> {
        let name = name.into();
        if name.is_empty() {
            return Err(CatalogError::InvalidIndexName);
        }
        let table = table.into();
        if columns.is_empty() {
            return Err(CatalogError::IndexHasNoColumns(name));
        }
        Ok(Self {
            name,
            table,
            kind,
            columns,
        })
    }

    /// The index's name — unique across the live index set.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The table the index accelerates.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The access-structure family.
    #[must_use]
    pub const fn kind(&self) -> IndexKind {
        self.kind
    }

    /// The indexed value column names, in declaration order.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}
