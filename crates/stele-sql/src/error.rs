//! Parse-time error type for the Stele SQL frontend.

use sqlparser::parser::ParserError;
use sqlparser::tokenizer::TokenizerError;

/// An error raised while tokenizing or parsing SQL text.
///
/// Wraps the two failure modes of the underlying `sqlparser-rs` pipeline and
/// adds a Stele-specific variant for malformed temporal grammar that the
/// generic parser cannot diagnose on its own.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// The tokenizer rejected the input before parsing began.
    #[error("tokenizer error: {0}")]
    Tokenize(#[from] TokenizerError),

    /// `sqlparser-rs` rejected the (temporal-clauses-stripped) statement.
    #[error("syntax error: {0}")]
    Syntax(#[from] ParserError),

    /// A Stele temporal clause (`WITH SYSTEM VERSIONING`, `VALID TIME (..)`,
    /// `FOR { SYSTEM_TIME | VALID_TIME } AS OF ..`) was malformed.
    #[error("temporal grammar error: {0}")]
    Temporal(String),

    /// A column declared a SQL type outside Stele's type vocabulary.
    #[error(
        "unsupported column type {0} — supported: INT, BIGINT, TEXT, VARCHAR, NVARCHAR, \
         BOOL, TIMESTAMP, TIMESTAMPTZ, DATE, UUID, BYTEA"
    )]
    UnsupportedType(String),
}
