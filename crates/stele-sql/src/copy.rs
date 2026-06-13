//! Binding `COPY <table> FROM STDIN` — the SQL-layer half of pg-wire bulk load
//! ([STL-236]).
//!
//! `COPY t FROM STDIN` is the standard Postgres bulk-load door: the client opens
//! it, streams a text/CSV row stream over the wire, and the server appends every
//! row as one crash-atomic group. This module is the binder for that statement —
//! the sibling of [`bind_dml`](crate::bind_dml) — split in two:
//!
//! * [`bind_copy`] validates the statement against the catalog (the target table,
//!   an optional column list, the format options) and resolves the field→column
//!   mapping into a [`BoundCopy`] plan. The wire layer binds *before* it streams
//!   data, so it can advertise the column count + format in `CopyInResponse` and
//!   reject a bad `COPY` (unknown table, COPY TO, binary) up front.
//! * [`bind_copy_rows`] folds the streamed field text into the same per-row
//!   [`InsertRow`] a multi-row `INSERT` ([STL-228]) produces, reusing the shared
//!   text-field codec (`fold::fold_text_field`) so a value loaded by `COPY` is
//!   byte-identical to the same value written by `INSERT`.
//!
//! The engine then applies those rows through the existing multi-row-insert group
//! commit ([STL-192]/[STL-216]), so a parse failure on any row leaves **zero**
//! rows. The wire framing, the CSV/text *lexing* (delimiter / quoting / the null
//! marker), and the `CopyData`/`CopyDone`/`CopyFail` sub-protocol live in
//! `stele-pgwire`; this module only sees already-split field strings.
//!
//! ## What this rejects (with a clear error, never a wrong load)
//!
//! `COPY TO STDOUT` (export — a later ticket), `COPY` to/from a file or program
//! (the server has no client-side file access), binary format, `COPY (query) TO`,
//! and `COPY` into a **valid-time** table (the period-column lifting a valid-time
//! `INSERT` does is out of scope here — also a follow-up). The defaults and the
//! supported options match Postgres: text format is TAB-delimited with `\N` for
//! NULL; CSV is comma-delimited, double-quoted, with the empty unquoted field for
//! NULL.
//!
//! [STL-236]: https://allegromusic.atlassian.net/browse/STL-236
//! [STL-228]: https://allegromusic.atlassian.net/browse/STL-228
//! [STL-192]: https://allegromusic.atlassian.net/browse/STL-192
//! [STL-216]: https://allegromusic.atlassian.net/browse/STL-216

use sqlparser::ast::{
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget,
    Statement as SqlStatement,
};
use stele_catalog::SchemaId;
use stele_common::types::LogicalType;

use crate::ast::Statement;
use crate::dml::{InsertRow, bare_name, resolve_shape};
use crate::fold::{self, FoldError};
use crate::select::BindContext;

/// Why binding or loading a `COPY` failed.
///
/// Rendered by the engine/wire layer to a Postgres `ErrorResponse`. The
/// table/column resolution errors are reused from the DML binder
/// ([`DmlError`](crate::DmlError)) so `COPY` and `INSERT` report an unknown table
/// or column identically.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CopyError {
    /// A table/column resolution failure shared with the DML binder — an unknown
    /// or non-live table, an unknown/duplicate column, or an empty (key-less)
    /// table.
    #[error(transparent)]
    Bind(#[from] crate::DmlError),

    /// A `COPY` shape outside this ticket's scope — `COPY TO`, a file/program
    /// endpoint, binary format, a query source, or a valid-time target table.
    /// Carries a human description; the wire layer maps it to `0A000`
    /// (`feature_not_supported`).
    #[error("unsupported COPY: {0}")]
    Unsupported(String),

    /// A `WITH (…)` option that is malformed or not understood (e.g. an unknown
    /// `FORMAT`, or a non-UTF8 `ENCODING`). Maps to `42601` (`syntax_error`).
    #[error("invalid COPY option: {0}")]
    BadOption(String),

    /// A data row carried a different number of fields than the target column
    /// count. Maps to `22P02` (`invalid_text_representation`).
    #[error("COPY row has {found} field(s), expected {expected}")]
    FieldCountMismatch {
        /// The number of fields the target expects (the column-list length, or the
        /// table's column count when no list was given).
        expected: usize,
        /// The number of fields the offending row carried.
        found: usize,
    },

    /// A row left the business key column NULL (the null marker, or an omitted
    /// column). A value column may be NULL ([STL-154]); the key may not. Maps to
    /// `22P02`.
    #[error("COPY into {table:?}: the business key column {column:?} cannot be NULL")]
    NullKey {
        /// The target table.
        table: String,
        /// The business key column.
        column: String,
    },

    /// A field's text could not be folded to its column's type. Maps to `22P02`.
    #[error("COPY into {table:?} column {column:?}: {reason}")]
    Field {
        /// The target table.
        table: String,
        /// The column being loaded.
        column: String,
        /// The folding failure's human reason.
        reason: String,
    },

    /// Wraps a per-row failure with its 1-based position in the stream, so the
    /// diagnostic names the offending row — like a multi-row `INSERT`
    /// ([`DmlError::RowError`](crate::DmlError)).
    #[error("COPY row {row}: {source}")]
    Row {
        /// The 1-based position of the offending row in the data stream.
        row: usize,
        /// The underlying per-row failure.
        #[source]
        source: Box<CopyError>,
    },
}

/// The on-the-wire format of a `COPY` data stream.
///
/// What the pg-wire lexer needs to split the byte stream into rows and fields, and
/// to recognize the null marker. Defaults follow Postgres exactly
/// ([`CopyFormat::defaults`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFormat {
    /// `text` (the default — TAB-delimited, backslash escapes, `\N` NULL) or
    /// `csv` (comma-delimited, double-quoted, empty unquoted field = NULL).
    pub kind: CopyFormatKind,
    /// The field delimiter — TAB for text, `,` for CSV unless overridden.
    pub delimiter: char,
    /// The NULL marker — `\N` for text, the empty string for CSV unless
    /// overridden. A field equal to this (unquoted, in CSV) is a SQL `NULL`.
    pub null: String,
    /// The CSV quote character (default `"`). Unused in text format.
    pub quote: char,
    /// The CSV escape character (default = the quote character). Unused in text
    /// format.
    pub escape: char,
    /// Whether the first data line is a header to skip (`HEADER`). Only meaningful
    /// for CSV in Postgres; honored for both here.
    pub header: bool,
}

/// `COPY`'s two textual formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormatKind {
    /// The default text format: TAB-delimited, backslash escapes, `\N` for NULL.
    Text,
    /// CSV: delimiter-separated, quotable fields, empty unquoted field = NULL.
    Csv,
}

impl CopyFormat {
    /// The Postgres default format for `kind`: TAB / `\N` for text, `,` / `""` /
    /// empty-NULL for CSV.
    #[must_use]
    pub fn defaults(kind: CopyFormatKind) -> Self {
        match kind {
            CopyFormatKind::Text => Self {
                kind,
                delimiter: '\t',
                null: "\\N".to_owned(),
                quote: '"',
                escape: '"',
                header: false,
            },
            CopyFormatKind::Csv => Self {
                kind,
                delimiter: ',',
                null: String::new(),
                quote: '"',
                escape: '"',
                header: false,
            },
        }
    }
}

/// A bound `COPY <table> FROM STDIN`: the resolved table, the streamed-data
/// format, and the field→column mapping the row binder applies. Produced by
/// [`bind_copy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCopy {
    /// The target table.
    pub table: String,
    /// The schema version the table resolved to at the bind snapshot.
    pub schema_id: SchemaId,
    /// The data-stream format (text/CSV + delimiter/null/quote/escape/header).
    pub format: CopyFormat,
    /// The number of input fields each data row must carry: the explicit column
    /// list's length, or the table's column count when no list was given. The wire
    /// layer advertises this in `CopyInResponse` and every row is checked against it.
    pub field_count: usize,
    /// For each schema column in declaration order, the index of the input field
    /// that supplies it, or `None` when the column is omitted from an explicit
    /// list (legal only for a value column — an omitted key is rejected when a row
    /// binds). The first entry is the business key.
    col_to_field: Vec<Option<usize>>,
    /// Each schema column's name, in declaration order (aligned with
    /// [`col_to_field`](Self::col_to_field)) — for diagnostics.
    col_names: Vec<String>,
    /// Each schema column's type, in declaration order (aligned with
    /// [`col_to_field`](Self::col_to_field)) — the codec each field folds through.
    col_types: Vec<LogicalType>,
}

impl BoundCopy {
    /// The `CopyInResponse` shape the wire layer advertises: the field count and
    /// the format (the format's only wire-relevant bit is text vs binary — always
    /// text here).
    #[must_use]
    pub fn shape(&self) -> CopyShape {
        CopyShape {
            columns: self.field_count,
            format: self.format.clone(),
        }
    }
}

/// What the wire layer needs before it streams `COPY` data: how many columns to
/// advertise in `CopyInResponse`, and the format to lex the byte stream with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyShape {
    /// The number of columns to advertise (and to check each data row against).
    pub columns: usize,
    /// The data-stream format.
    pub format: CopyFormat,
}

/// Bind a `COPY <table> [(col, …)] FROM STDIN [WITH (…)]` against the catalog,
/// resolving the target, the field→column mapping, and the stream format.
///
/// # Errors
///
/// [`CopyError::Unsupported`] for a `COPY` shape outside scope (`COPY TO`, a
/// file/program endpoint, binary, a query source, or a valid-time table);
/// [`CopyError::BadOption`] for a malformed `WITH` option; [`CopyError::Bind`] for
/// an unknown/non-live table or an unknown/duplicate column in the list.
pub fn bind_copy(stmt: &Statement, ctx: &BindContext) -> Result<BoundCopy, CopyError> {
    let Some(SqlStatement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        ..
    }) = stmt.sql()
    else {
        // The caller classifies COPY before calling this; reaching here is a bug.
        return Err(CopyError::Unsupported("not a COPY statement".to_owned()));
    };
    if *to {
        return Err(CopyError::Unsupported(
            "COPY TO (data export) is not supported yet".to_owned(),
        ));
    }
    if !matches!(target, CopyTarget::Stdin) {
        return Err(CopyError::Unsupported(
            "COPY FROM is supported only with STDIN, not a file or program".to_owned(),
        ));
    }
    let CopySource::Table {
        table_name,
        columns,
    } = source
    else {
        return Err(CopyError::Unsupported(
            "COPY (query) is an export form and is not supported".to_owned(),
        ));
    };

    let table = bare_name(table_name)?;
    let (schema, key_col, _value_cols) = resolve_shape(ctx, &table)?;

    // Valid-time COPY (the period-column lifting an `INSERT` does) is out of scope
    // for this ticket — reject it rather than silently dropping the valid axis.
    if schema.temporal().valid_time_enabled() {
        return Err(CopyError::Unsupported(format!(
            "COPY into the valid-time table {table:?} is not supported yet"
        )));
    }

    let format = parse_format(options, legacy_options)?;

    let all_cols = schema.columns();
    let col_names: Vec<String> = all_cols.iter().map(|c| c.name().to_owned()).collect();
    let col_types: Vec<LogicalType> = all_cols.iter().map(stele_catalog::ColumnDef::ty).collect();

    // The field→column mapping. With no explicit list every column is supplied
    // positionally; with a list, each named column takes the field at its position
    // and the rest are omitted (NULL for a value column, rejected for the key).
    let (field_count, col_to_field) = if columns.is_empty() {
        let n = all_cols.len();
        (n, (0..n).map(Some).collect())
    } else {
        let mut names: Vec<String> = Vec::with_capacity(columns.len());
        for ident in columns {
            let name = ident.value.clone();
            if schema.column(&name).is_none() {
                return Err(crate::DmlError::UnknownColumn {
                    table: table.clone(),
                    column: name,
                }
                .into());
            }
            if names.iter().any(|prev| prev == &name) {
                return Err(crate::DmlError::DuplicateColumn {
                    table: table.clone(),
                    column: name,
                }
                .into());
            }
            names.push(name);
        }
        let map: Vec<Option<usize>> = all_cols
            .iter()
            .map(|c| names.iter().position(|n| n == c.name()))
            .collect();
        (names.len(), map)
    };

    // An explicit list that omits the business key cannot supply it — reject at
    // bind, before a single row streams, rather than per row.
    if col_to_field[0].is_none() {
        return Err(CopyError::NullKey {
            table,
            column: key_col.name().to_owned(),
        });
    }

    Ok(BoundCopy {
        table,
        schema_id: schema.schema_id(),
        format,
        field_count,
        col_to_field,
        col_names,
        col_types,
    })
}

/// Fold a streamed `COPY` data stream's rows into per-row [`InsertRow`]s under
/// `plan`, reusing the shared text-field codec so the load matches an `INSERT`.
///
/// Each row is a vector of fields aligned to the `COPY` column list (or the table
/// columns); `None` is the null marker the wire lexer already recognized. A
/// failure names the 1-based offending row ([`CopyError::Row`]); the engine
/// applies the whole set as one atomic group, so any failure here leaves zero
/// rows.
///
/// # Errors
///
/// [`CopyError::Row`] wrapping a [`CopyError::FieldCountMismatch`],
/// [`CopyError::NullKey`], or [`CopyError::Field`] for the first row that does not
/// bind.
pub fn bind_copy_rows(
    plan: &BoundCopy,
    rows: &[Vec<Option<String>>],
) -> Result<Vec<InsertRow>, CopyError> {
    rows.iter()
        .enumerate()
        .map(|(i, fields)| {
            bind_copy_row(plan, fields).map_err(|source| CopyError::Row {
                row: i + 1,
                source: Box::new(source),
            })
        })
        .collect()
}

/// Bind one `COPY` data row: check its field count, align the fields to the schema
/// columns, fold each present field through its column's codec, and assemble the
/// [`InsertRow`] (business key + value columns; no valid-time interval — a
/// valid-time target is rejected at [`bind_copy`]).
fn bind_copy_row(plan: &BoundCopy, fields: &[Option<String>]) -> Result<InsertRow, CopyError> {
    if fields.len() != plan.field_count {
        return Err(CopyError::FieldCountMismatch {
            expected: plan.field_count,
            found: fields.len(),
        });
    }
    let mut cells: Vec<Option<stele_common::types::ScalarValue>> =
        Vec::with_capacity(plan.col_to_field.len());
    for ((slot, ty), name) in plan
        .col_to_field
        .iter()
        .zip(&plan.col_types)
        .zip(&plan.col_names)
    {
        let cell = match slot {
            // An omitted column folds to a NULL cell — rejected below if it is the key.
            None => None,
            Some(field_idx) => match &fields[*field_idx] {
                // The wire lexer mapped the null marker to an absent field.
                None => None,
                Some(text) => {
                    Some(
                        fold::fold_text_field(text, *ty).map_err(|err| CopyError::Field {
                            table: plan.table.clone(),
                            column: name.clone(),
                            reason: fold_reason(&err, *ty),
                        })?,
                    )
                }
            },
        };
        cells.push(cell);
    }

    let mut cells = cells.into_iter();
    // The first column is the business key (resolve_shape's split-first); it must
    // be present and non-NULL.
    let key = cells.next().flatten().ok_or_else(|| CopyError::NullKey {
        table: plan.table.clone(),
        column: plan.col_names[0].clone(),
    })?;
    let values: Vec<_> = cells.collect();
    Ok(InsertRow {
        key,
        values,
        valid: None,
    })
}

/// Collapse a [`FoldError`] into a one-line human reason for a `COPY` field
/// failure — the text-field counterpart of [`fold_literal`](crate::fold_literal)'s
/// key-literal mapping.
fn fold_reason(err: &FoldError, ty: LogicalType) -> String {
    match err {
        // `fold_text_field` never produces `Null` (the lexer resolves the marker).
        FoldError::Null => format!("a NULL value is not valid for {ty}"),
        FoldError::TypeMismatch { found } => format!("value is {found}, expected {ty}"),
        FoldError::BadLiteral { literal, reason } => reason.map_or_else(
            || format!("value {literal:?} is not a valid {ty}"),
            |reason| format!("value {literal:?} is not a valid {ty}: {reason}"),
        ),
        FoldError::UnsupportedType(ty) => format!("{ty} columns cannot be loaded by COPY"),
    }
}

/// Resolve the `COPY` stream format from the modern `WITH (…)` options and the
/// legacy bare options, starting from the format-kind's Postgres defaults and
/// applying each explicit override.
fn parse_format(
    options: &[CopyOption],
    legacy: &[CopyLegacyOption],
) -> Result<CopyFormat, CopyError> {
    // First settle the kind (text vs CSV), since it drives the defaults, then
    // apply the explicit delimiter/null/quote/escape/header overrides over them.
    let mut kind = CopyFormatKind::Text;
    for opt in options {
        if let CopyOption::Format(name) = opt {
            kind = match name.value.to_ascii_lowercase().as_str() {
                "text" => CopyFormatKind::Text,
                "csv" => CopyFormatKind::Csv,
                "binary" => {
                    return Err(CopyError::Unsupported(
                        "COPY ... WITH (FORMAT binary) is not supported".to_owned(),
                    ));
                }
                other => {
                    return Err(CopyError::BadOption(format!("unknown FORMAT {other:?}")));
                }
            };
        }
    }
    for opt in legacy {
        match opt {
            CopyLegacyOption::Csv(_) => kind = CopyFormatKind::Csv,
            CopyLegacyOption::Binary => {
                return Err(CopyError::Unsupported(
                    "COPY ... BINARY is not supported".to_owned(),
                ));
            }
            _ => {}
        }
    }

    let mut format = CopyFormat::defaults(kind);

    for opt in options {
        match opt {
            CopyOption::Format(_) => {} // already handled
            CopyOption::Delimiter(c) => format.delimiter = *c,
            CopyOption::Null(s) => format.null.clone_from(s),
            CopyOption::Header(b) => format.header = *b,
            CopyOption::Quote(c) => format.quote = *c,
            CopyOption::Escape(c) => format.escape = *c,
            CopyOption::Encoding(enc) => reject_non_utf8(enc)?,
            other => {
                return Err(CopyError::Unsupported(format!(
                    "COPY option {other} is not supported"
                )));
            }
        }
    }
    for opt in legacy {
        match opt {
            CopyLegacyOption::Binary | CopyLegacyOption::Csv(_) => {} // handled above
            CopyLegacyOption::Delimiter(c) => format.delimiter = *c,
            other => {
                return Err(CopyError::Unsupported(format!(
                    "legacy COPY option {other} is not supported"
                )));
            }
        }
    }
    // The legacy `CSV ( … )` sub-options (HEADER / QUOTE / ESCAPE), so
    // `COPY t FROM STDIN CSV HEADER` works as well as the modern WITH form.
    for opt in legacy {
        if let CopyLegacyOption::Csv(csv) = opt {
            for c in csv {
                match c {
                    CopyLegacyCsvOption::Header => format.header = true,
                    CopyLegacyCsvOption::Quote(q) => format.quote = *q,
                    CopyLegacyCsvOption::Escape(e) => format.escape = *e,
                    other => {
                        return Err(CopyError::Unsupported(format!(
                            "legacy CSV option {other} is not supported"
                        )));
                    }
                }
            }
        }
    }

    Ok(format)
}

/// Accept only a UTF-8 `ENCODING` (the engine's one on-disk encoding); reject any
/// other so a mis-encoded load fails loudly rather than storing mojibake.
fn reject_non_utf8(enc: &str) -> Result<(), CopyError> {
    let normalized = enc.to_ascii_uppercase().replace(['-', '_'], "");
    if normalized == "UTF8" {
        Ok(())
    } else {
        Err(CopyError::Unsupported(format!(
            "COPY ENCODING {enc:?} is not supported (only UTF8)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DmlError;
    use crate::parse;
    use stele_catalog::{Catalog, ColumnDef, TableTemporal, ValidTimeSpec};
    use stele_common::time::SystemTimeMicros;
    use stele_common::types::ScalarValue;

    const NOW: SystemTimeMicros = SystemTimeMicros(2_000_000_000_000_000);

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse(sql).expect("parse");
        assert_eq!(stmts.len(), 1, "expected one statement");
        stmts.remove(0)
    }

    /// The identity-demo `account (id INT, balance INT)` table.
    fn account_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "account",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                ],
                TableTemporal::system_only(),
                SystemTimeMicros(1_000),
            )
            .expect("create account");
        catalog
    }

    fn valid_time_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "vt",
                vec![
                    ColumnDef::new("id", LogicalType::Int4).expect("col"),
                    ColumnDef::new("balance", LogicalType::Int4).expect("col"),
                    ColumnDef::new("vf", LogicalType::Timestamp).expect("col"),
                    ColumnDef::new("vt", LogicalType::Timestamp).expect("col"),
                ],
                TableTemporal::with_valid_time(ValidTimeSpec::new("vf", "vt").expect("spec")),
                SystemTimeMicros(1_000),
            )
            .expect("create vt");
        catalog
    }

    fn bind(sql: &str, catalog: &Catalog) -> Result<BoundCopy, CopyError> {
        let ctx = BindContext {
            snapshot: NOW,
            catalog,
        };
        bind_copy(&parse_one(sql), &ctx)
    }

    /// A field vector from owned strings; `None` is the wire null marker.
    fn row(fields: &[Option<&str>]) -> Vec<Option<String>> {
        fields.iter().map(|f| f.map(ToOwned::to_owned)).collect()
    }

    #[test]
    fn binds_plain_copy_with_text_defaults() {
        let plan = bind("COPY account FROM STDIN", &account_catalog()).expect("bind");
        assert_eq!(plan.table, "account");
        assert_eq!(plan.field_count, 2);
        assert_eq!(plan.col_to_field, vec![Some(0), Some(1)]);
        assert_eq!(plan.format, CopyFormat::defaults(CopyFormatKind::Text));
        // Text defaults: TAB delimiter, `\N` NULL.
        assert_eq!(plan.format.delimiter, '\t');
        assert_eq!(plan.format.null, "\\N");
    }

    #[test]
    fn column_list_remaps_field_order() {
        // `(balance, id)` supplies balance first, id second — so the key column
        // `id` (schema position 0) reads field index 1, and `balance` reads 0.
        let plan = bind("COPY account (balance, id) FROM STDIN", &account_catalog()).expect("bind");
        assert_eq!(plan.field_count, 2);
        assert_eq!(plan.col_to_field, vec![Some(1), Some(0)]);
    }

    #[test]
    fn csv_format_takes_csv_defaults() {
        let plan = bind(
            "COPY account FROM STDIN WITH (FORMAT csv)",
            &account_catalog(),
        )
        .expect("bind");
        assert_eq!(plan.format.kind, CopyFormatKind::Csv);
        assert_eq!(plan.format.delimiter, ',');
        assert_eq!(plan.format.null, "");
        assert_eq!(plan.format.quote, '"');
    }

    #[test]
    fn with_options_override_defaults() {
        let plan = bind(
            "COPY account FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL 'NULL', HEADER, QUOTE '~')",
            &account_catalog(),
        )
        .expect("bind");
        assert_eq!(plan.format.kind, CopyFormatKind::Csv);
        assert_eq!(plan.format.delimiter, '|');
        assert_eq!(plan.format.null, "NULL");
        assert!(plan.format.header);
        assert_eq!(plan.format.quote, '~');
    }

    #[test]
    fn legacy_csv_header_is_honored() {
        let plan = bind("COPY account FROM STDIN CSV HEADER", &account_catalog()).expect("bind");
        assert_eq!(plan.format.kind, CopyFormatKind::Csv);
        assert!(plan.format.header);
    }

    #[test]
    fn copy_to_is_unsupported() {
        assert!(matches!(
            bind("COPY account TO STDOUT", &account_catalog()),
            Err(CopyError::Unsupported(_))
        ));
    }

    #[test]
    fn copy_from_file_is_unsupported() {
        assert!(matches!(
            bind("COPY account FROM '/tmp/x.csv'", &account_catalog()),
            Err(CopyError::Unsupported(_))
        ));
    }

    #[test]
    fn binary_format_is_unsupported() {
        assert!(matches!(
            bind(
                "COPY account FROM STDIN WITH (FORMAT binary)",
                &account_catalog()
            ),
            Err(CopyError::Unsupported(_))
        ));
    }

    #[test]
    fn valid_time_table_is_unsupported() {
        assert!(matches!(
            bind("COPY vt FROM STDIN", &valid_time_catalog()),
            Err(CopyError::Unsupported(_))
        ));
    }

    #[test]
    fn unknown_table_is_a_bind_error() {
        assert!(matches!(
            bind("COPY ghost FROM STDIN", &account_catalog()),
            Err(CopyError::Bind(DmlError::UnknownTable(_)))
        ));
    }

    #[test]
    fn unknown_and_duplicate_columns_are_rejected() {
        assert!(matches!(
            bind("COPY account (id, nope) FROM STDIN", &account_catalog()),
            Err(CopyError::Bind(DmlError::UnknownColumn { .. }))
        ));
        assert!(matches!(
            bind("COPY account (id, id) FROM STDIN", &account_catalog()),
            Err(CopyError::Bind(DmlError::DuplicateColumn { .. }))
        ));
    }

    #[test]
    fn column_list_omitting_the_key_is_rejected() {
        assert!(matches!(
            bind("COPY account (balance) FROM STDIN", &account_catalog()),
            Err(CopyError::NullKey { .. })
        ));
    }

    #[test]
    fn binds_rows_folding_each_field() {
        let plan = bind("COPY account FROM STDIN", &account_catalog()).expect("bind");
        let rows = bind_copy_rows(
            &plan,
            &[
                row(&[Some("1"), Some("100")]),
                row(&[Some("2"), Some("-5")]),
            ],
        )
        .expect("bind rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, ScalarValue::Int4(1));
        assert_eq!(rows[0].values, vec![Some(ScalarValue::Int4(100))]);
        assert_eq!(rows[1].key, ScalarValue::Int4(2));
        assert_eq!(rows[1].values, vec![Some(ScalarValue::Int4(-5))]);
        assert!(rows[0].valid.is_none());
    }

    #[test]
    fn null_value_cell_is_accepted_null_key_is_not() {
        let plan = bind("COPY account FROM STDIN", &account_catalog()).expect("bind");
        // A NULL value column folds to an absent cell.
        let ok = bind_copy_rows(&plan, &[row(&[Some("1"), None])]).expect("bind rows");
        assert_eq!(ok[0].values, vec![None]);
        // A NULL key is rejected, naming the offending row.
        assert!(matches!(
            bind_copy_rows(&plan, &[row(&[Some("1"), Some("1")]), row(&[None, Some("2")])]),
            Err(CopyError::Row {
                row: 2,
                source,
            }) if matches!(*source, CopyError::NullKey { .. })
        ));
    }

    #[test]
    fn a_field_count_mismatch_names_the_row() {
        let plan = bind("COPY account FROM STDIN", &account_catalog()).expect("bind");
        assert!(matches!(
            bind_copy_rows(&plan, &[row(&[Some("1"), Some("2"), Some("3")])]),
            Err(CopyError::Row {
                row: 1,
                source,
            }) if matches!(*source, CopyError::FieldCountMismatch { expected: 2, found: 3 })
        ));
    }

    #[test]
    fn a_bad_field_value_names_the_row_and_column() {
        let plan = bind("COPY account FROM STDIN", &account_catalog()).expect("bind");
        let err = bind_copy_rows(
            &plan,
            &[
                row(&[Some("1"), Some("100")]),
                row(&[Some("2"), Some("not-an-int")]),
            ],
        )
        .expect_err("row 2 should fail");
        assert!(
            matches!(&err, CopyError::Row { row: 2, source } if matches!(**source, CopyError::Field { .. })),
            "{err:?}"
        );
    }

    #[test]
    fn omitted_value_column_loads_as_null() {
        // `(id)` supplies only the key; the omitted `balance` value column is NULL.
        let plan = bind("COPY account (id) FROM STDIN", &account_catalog()).expect("bind");
        assert_eq!(plan.field_count, 1);
        let rows = bind_copy_rows(&plan, &[row(&[Some("7")])]).expect("bind rows");
        assert_eq!(rows[0].key, ScalarValue::Int4(7));
        assert_eq!(rows[0].values, vec![None]);
    }
}
