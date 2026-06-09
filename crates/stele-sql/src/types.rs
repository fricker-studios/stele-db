//! Lowering SQL surface types to Stele's logical type vocabulary.
//!
//! The parser produces `sqlparser` [`DataType`] nodes for column declarations
//! (`id INT`, `name TEXT`, …) — these are *syntactic*: they carry the type as
//! written. This module is the seam where the SQL frontend resolves that syntax
//! to the semantic [`LogicalType`] vocabulary owned by `stele-common`
//! (STL-96) — the type set the catalog stores and the executor and pgwire
//! encoder ultimately read. Keeping the mapping here, at the frontend, means the
//! set of spellings the SQL surface accepts lives next to the parser that
//! produces them, upstream of the catalog (STL-98).
//!
//! Vocabulary: `INT`/`INTEGER` → `Int4`, `BIGINT` → `Int8`, `TEXT` → `Text`,
//! `BOOL`/`BOOLEAN` → `Bool`, `TIMESTAMP` (no time zone) → `Timestamp`,
//! `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ` → `TimestampTz`, `DATE` → `Date`.
//! Anything else — `VARCHAR`, `CHAR`, … — is rejected as
//! [`ParseError::UnsupportedType`]; those are deliberate later additions, not
//! silent re-labellings (see [`LogicalType::Timestamp`]).

use sqlparser::ast::{DataType, TimezoneInfo};
use stele_common::types::LogicalType;

use crate::error::ParseError;

/// Resolve a parsed SQL [`DataType`] to a Stele [`LogicalType`].
///
/// # Errors
///
/// Returns [`ParseError::UnsupportedType`] if the SQL type is outside the v0.1
/// vocabulary.
///
/// ```
/// use sqlparser::ast::DataType;
/// use stele_sql::logical_type;
/// use stele_common::types::LogicalType;
///
/// assert_eq!(logical_type(&DataType::Int(None)).unwrap(), LogicalType::Int4);
/// assert_eq!(logical_type(&DataType::Text).unwrap(), LogicalType::Text);
/// assert!(logical_type(&DataType::Varchar(None)).is_err());
/// ```
pub fn logical_type(data_type: &DataType) -> Result<LogicalType, ParseError> {
    match data_type {
        DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) => Ok(LogicalType::Int4),
        DataType::BigInt(_) | DataType::Int8(_) => Ok(LogicalType::Int8),
        DataType::Text => Ok(LogicalType::Text),
        DataType::Bool | DataType::Boolean => Ok(LogicalType::Bool),
        // Bare `TIMESTAMP` (no zone) stores a UTC instant with no offset on the
        // wire; `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ` normalizes the offset to
        // UTC and renders one back ([`LogicalType::TimestampTz`], STL-189).
        DataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            Ok(LogicalType::Timestamp)
        }
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
            Ok(LogicalType::TimestampTz)
        }
        DataType::Date => Ok(LogicalType::Date),
        other => Err(ParseError::UnsupportedType(other.to_string())),
    }
}
