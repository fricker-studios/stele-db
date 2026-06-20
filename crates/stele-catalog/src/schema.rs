//! A table's shape at one point on the system-time axis.
//!
//! A [`TableSchema`] is one *version* of a table — the ordered column list plus
//! its temporal configuration — tagged with the [`SchemaId`] a sealed segment's
//! footer records, so an old segment can still be read under the schema it was
//! written with even after the table has since evolved
//! ([architecture §3.2](../../../docs/02-architecture.md#32-on-disk-segment-format),
//! [§5](../../../docs/02-architecture.md#5-catalog--metadata)).

use stele_common::types::LogicalType;

use crate::{CatalogError, TableTemporal};

/// Stable identifier for one schema version of a table.
///
/// Allocated by the [`Catalog`](crate::Catalog) — monotonic, never reused — and
/// stored in each sealed segment's footer (`schema_id`, a `u32`) so a read can
/// resolve the exact column layout a segment was written under, even after the
/// table's schema has since changed. Id `0` is **reserved** for the implicit
/// v0.1 segment schema (`SCHEMA_ID_IMPLICIT_VERSION` in `stele-storage`), so the
/// catalog allocates from `1` upward — a footer's `schema_id == 0` therefore
/// never ambiguously resolves to a catalog-allocated table schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaId(pub u32);

/// A single column: its name and logical type.
///
/// Nullability is intentionally absent for the same reason it is absent from
/// [`ScalarValue`](stele_common::types::ScalarValue) — v0.1 models a nullable
/// cell one level up as `Option<ScalarValue>`; the catalog fixes only the name
/// and the type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    name: String,
    ty: LogicalType,
}

impl ColumnDef {
    /// Build a column definition.
    ///
    /// # Errors
    ///
    /// [`CatalogError::InvalidColumnName`] if `name` is empty — a column needs a
    /// name to be resolvable.
    pub fn new(name: impl Into<String>, ty: LogicalType) -> Result<Self, CatalogError> {
        let name = name.into();
        if name.is_empty() {
            return Err(CatalogError::InvalidColumnName);
        }
        Ok(Self { name, ty })
    }

    /// The column's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The column's logical type.
    #[must_use]
    pub const fn ty(&self) -> LogicalType {
        self.ty
    }
}

/// One version of a table's shape: its ordered columns and temporal config,
/// tagged with the [`SchemaId`] the segments written under it carry.
///
/// Constructed only by the [`Catalog`](crate::Catalog), which owns id allocation
/// and the system-time interval each version is valid for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    schema_id: SchemaId,
    columns: Vec<ColumnDef>,
    temporal: TableTemporal,
}

impl TableSchema {
    /// Build a schema, rejecting a malformed column set.
    ///
    /// # Errors
    ///
    /// [`CatalogError::DuplicateColumn`] if two columns share a name — names
    /// must be distinct within a schema to be unambiguously resolvable.
    pub(crate) fn new(
        schema_id: SchemaId,
        columns: Vec<ColumnDef>,
        temporal: TableTemporal,
    ) -> Result<Self, CatalogError> {
        for (i, col) in columns.iter().enumerate() {
            if columns[..i].iter().any(|prev| prev.name() == col.name()) {
                return Err(CatalogError::DuplicateColumn(col.name().to_owned()));
            }
        }
        Ok(Self {
            schema_id,
            columns,
            temporal,
        })
    }

    /// Build an **ephemeral** schema for a query-local relation — a CTE or a
    /// derived table ([STL-242]) — that is *not* registered in the catalog.
    ///
    /// A `WITH name AS (SELECT …)` result, or a `FROM (SELECT …) AS d` derived
    /// table, has a real shape (its output columns' names and types) the binder
    /// must resolve references against, but no catalog identity and no schema
    /// evolution: it lives only for the duration of the one statement. So it
    /// carries the **reserved** [`SchemaId(0)`](SchemaId) sentinel (never a
    /// catalog-allocated id, which start at `1`) and the
    /// [system-only](TableTemporal::system_only) temporal config — a query-local
    /// relation has no valid-time period, so a `FOR VALID_TIME AS OF` over one is
    /// the documented unsupported read. Unlike a catalog-allocated schema, the
    /// engine never resolves an ephemeral schema's rows from storage — it
    /// materializes the relation's defining query once and reads that.
    ///
    /// # Errors
    ///
    /// [`CatalogError::DuplicateColumn`] if two output columns share a name —
    /// names must be distinct for a reference to resolve unambiguously.
    ///
    /// [STL-242]: https://allegromusic.atlassian.net/browse/STL-242
    pub fn ephemeral(columns: Vec<ColumnDef>) -> Result<Self, CatalogError> {
        Self::new(SchemaId(0), columns, TableTemporal::system_only())
    }

    /// The id segments written under this schema record in their footer.
    #[must_use]
    pub const fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// The columns, in declaration order.
    #[must_use]
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// The column with this name, if the schema has one.
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name() == name)
    }

    /// The table's temporal configuration at this schema version.
    #[must_use]
    pub const fn temporal(&self) -> &TableTemporal {
        &self.temporal
    }
}
