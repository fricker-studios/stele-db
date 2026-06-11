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
//! `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ` → `TimestampTz` ([STL-189]),
//! `DATE` → `Date`, `UUID` → `Uuid`, `BYTEA` → `Bytea` ([STL-181]). The
//! character-*varying* family — `VARCHAR`, `CHARACTER VARYING`, `CHAR VARYING`,
//! `NVARCHAR`, each with or without a length — also lowers to `Text`, the way
//! Postgres treats an unconstrained `varchar`; a declared length is accepted as
//! documentation but **not enforced** (no typmod machinery yet — enforcement is
//! a later ticket). Anything else — notably the blank-padding `CHAR(n)`, whose
//! semantics `Text` cannot honor — is rejected as
//! [`ParseError::UnsupportedType`]; those are deliberate later additions, not
//! silent re-labellings (see [`LogicalType::Timestamp`]).
//!
//! [STL-181]: https://allegromusic.atlassian.net/browse/STL-181
//! [STL-189]: https://allegromusic.atlassian.net/browse/STL-189

use sqlparser::ast::{DataType, TimezoneInfo};
use stele_common::types::LogicalType;

use crate::error::ParseError;

/// Resolve a parsed SQL [`DataType`] to a Stele [`LogicalType`].
///
/// # Errors
///
/// Returns [`ParseError::UnsupportedType`] if the SQL type is outside the
/// vocabulary.
///
/// ```
/// use sqlparser::ast::DataType;
/// use stele_sql::logical_type;
/// use stele_common::types::LogicalType;
///
/// assert_eq!(logical_type(&DataType::Int(None)).unwrap(), LogicalType::Int4);
/// assert_eq!(logical_type(&DataType::Text).unwrap(), LogicalType::Text);
/// // The varying family is TEXT under the hood (length unenforced)…
/// assert_eq!(logical_type(&DataType::Varchar(None)).unwrap(), LogicalType::Text);
/// // …but blank-padded CHAR(n) stays out of the vocabulary.
/// assert!(logical_type(&DataType::Char(None)).is_err());
/// ```
pub fn logical_type(data_type: &DataType) -> Result<LogicalType, ParseError> {
    match data_type {
        DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) => Ok(LogicalType::Int4),
        DataType::BigInt(_) | DataType::Int8(_) => Ok(LogicalType::Int8),
        // The character-varying spellings are all `Text` storage-side; an
        // optional declared length is accepted but not enforced (module docs).
        DataType::Text
        | DataType::Varchar(_)
        | DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::Nvarchar(_) => Ok(LogicalType::Text),
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
        DataType::Uuid => Ok(LogicalType::Uuid),
        DataType::Bytea => Ok(LogicalType::Bytea),
        other => Err(ParseError::UnsupportedType(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_the_full_vocabulary() {
        assert_eq!(
            logical_type(&DataType::Int(None)).unwrap(),
            LogicalType::Int4
        );
        assert_eq!(
            logical_type(&DataType::BigInt(None)).unwrap(),
            LogicalType::Int8
        );
        assert_eq!(logical_type(&DataType::Text).unwrap(), LogicalType::Text);
        assert_eq!(logical_type(&DataType::Boolean).unwrap(), LogicalType::Bool);
        assert_eq!(logical_type(&DataType::Date).unwrap(), LogicalType::Date);
        // Bare TIMESTAMP and its zone-bearing spellings map to the two distinct
        // instant types (STL-189).
        assert_eq!(
            logical_type(&DataType::Timestamp(None, TimezoneInfo::None)).unwrap(),
            LogicalType::Timestamp
        );
        assert_eq!(
            logical_type(&DataType::Timestamp(None, TimezoneInfo::WithTimeZone)).unwrap(),
            LogicalType::TimestampTz
        );
        // STL-181 additions.
        assert_eq!(logical_type(&DataType::Uuid).unwrap(), LogicalType::Uuid);
        assert_eq!(logical_type(&DataType::Bytea).unwrap(), LogicalType::Bytea);
    }

    #[test]
    fn varying_character_spellings_lower_to_text() {
        use sqlparser::ast::CharacterLength;
        // With and without a declared length — the length is documentation only.
        let fifty = Some(CharacterLength::IntegerLength {
            length: 50,
            unit: None,
        });
        for dt in [
            DataType::Varchar(None),
            DataType::Varchar(fifty),
            DataType::CharacterVarying(fifty),
            DataType::CharVarying(None),
            DataType::Nvarchar(fifty),
        ] {
            assert_eq!(logical_type(&dt).unwrap(), LogicalType::Text, "{dt}");
        }
    }

    #[test]
    fn rejects_types_outside_the_vocabulary() {
        // Deliberate non-mappings: CHAR(n) blank-pads (Text cannot honor that)
        // and BLOB is not the Postgres spelling (BYTEA is).
        assert!(logical_type(&DataType::Char(None)).is_err());
        assert!(logical_type(&DataType::Blob(None)).is_err());
    }
}
