//! A **minimal** `pg_catalog` introspection shim — just enough for `psql`'s
//! `\d <table>` to resolve a table's columns over the wire ([STL-131]).
//!
//! `\d account` is not one request: `psql` expands it into a short sequence of
//! `SELECT`s against the system catalogs. The two that carry the answer are
//!
//! 1. a lookup in `pg_catalog.pg_class` by `relname` → the relation's `oid`;
//! 2. a lookup in `pg_catalog.pg_attribute` by `attrelid` (that `oid`) → one row
//!    per column.
//!
//! This module recognizes those two shapes structurally from the parsed AST
//! ([`classify`]) and the front end answers them from the session engine's live
//! catalog. It is deliberately *not* a faithful `pg_catalog`: it returns a fixed,
//! documented projection rather than mirroring `psql`'s exact (version-specific)
//! column lists, and it ignores the relation-metadata / index / constraint
//! queries `\d` also fires (those resolve to empty and `\d` still prints the
//! column table). A faithful shim validated against a real `psql` binary is a
//! later ticket ([STL-150] owns the psql-in-CI harness; the full `pg_catalog` /
//! `information_schema` surface is a v0.5 roadmap item).
//!
//! The `oid` is synthetic: [`oid_for`] hashes the table name to a stable,
//! positive value, and the attribute lookup reverses it by rescanning the live
//! tables for the one whose name hashes to the queried `oid`. No catalog state
//! is added — the mapping is a pure function of the (small) set of live names.
//!
//! [STL-131]: https://allegromusic.atlassian.net/browse/STL-131
//! [STL-150]: https://allegromusic.atlassian.net/browse/STL-150

use stele_sql::Statement;
use stele_sql::sqlparser::ast::{
    Expr, SetExpr, Statement as SqlStatement, TableFactor, UnaryOperator, Value,
};

/// A recognized `pg_catalog` introspection query from the `\d` / `\dt` sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Introspection {
    /// `pg_class` lookup by `relname` — resolve a relation to its `oid`.
    Relation {
        /// The relation name the query filters on.
        name: String,
    },
    /// `pg_attribute` lookup by `attrelid` — list a relation's columns.
    Attributes {
        /// The synthetic `oid` ([`oid_for`]) the query filters on.
        oid: u32,
    },
    /// `pg_class` scan with no name filter — list every live relation (`\dt`,
    /// STL-198).
    TableList,
}

/// Recognize one of the two `\d` introspection shapes, or `None` for any other
/// statement (which the caller routes normally).
///
/// Recognition is structural: the primary relation in the `FROM` must be
/// `pg_class` / `pg_attribute` (bare or `pg_catalog`-qualified), and the filter
/// value is the first matching literal in the `WHERE` — a string for the
/// relation name (regex anchors `^(…)$` stripped, so both `relname = 'account'`
/// and `relname ~ '^(account)$'` work), an integer for the `attrelid` oid.
pub(crate) fn classify(stmt: &Statement) -> Option<Introspection> {
    let SqlStatement::Query(query) = &stmt.body else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let relation = primary_relation_name(select)?;
    let where_clause = select.selection.as_ref();

    match relation.to_ascii_lowercase().as_str() {
        "pg_class" => {
            // A name filter resolves one relation; a scan with no string
            // literal in the WHERE (or no WHERE at all) is the `\dt` shape —
            // list everything (STL-198). Literals elsewhere (projection,
            // ORDER BY) are not consulted, like the relation lookup above.
            where_clause.and_then(first_string_literal).map_or(
                Some(Introspection::TableList),
                |literal| {
                    Some(Introspection::Relation {
                        name: strip_regex_anchors(&literal),
                    })
                },
            )
        }
        "pg_attribute" => {
            let oid = where_clause.and_then(first_int_literal)?;
            // A negative or out-of-range attrelid can't match a real `oid_for`
            // value (those are non-zero and within the i32 range), so it stays an
            // introspection query and resolves to zero rows — an empty `\d`.
            // Saturate to 0 rather than returning `None`, which would drop out of
            // introspection and wrongly answer `feature_not_supported`.
            let oid = u32::try_from(oid).unwrap_or(0);
            Some(Introspection::Attributes { oid })
        }
        _ => None,
    }
}

/// A stable, positive synthetic `oid` for a table name.
///
/// FNV-1a over the name, masked into the non-negative `i32` range and forced
/// non-zero so it renders cleanly as an `int4` and is never `0` (which Postgres
/// uses for "no relation"). Collisions across the handful of live tables are
/// vanishingly unlikely and, if they ever occurred, would only mislabel a `\d`,
/// never corrupt data.
pub(crate) fn oid_for(name: &str) -> u32 {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for byte in name.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash & 0x7fff_ffff) | 1
}

/// The name of the primary relation in the `FROM` (the first entry's base
/// table), as its last name part — so `pg_catalog.pg_class` and a bare
/// `pg_class` both yield `"pg_class"`. `None` if the `FROM` is empty or its base
/// is not a plain table.
fn primary_relation_name(select: &stele_sql::sqlparser::ast::Select) -> Option<String> {
    let from = select.from.first()?;
    let TableFactor::Table { name, .. } = &from.relation else {
        return None;
    };
    name.0.last()?.as_ident().map(|id| id.value.clone())
}

/// Strip a `^(…)$` regex wrapper `psql` uses (`'^(account)$'` → `account`); a
/// plain literal passes through unchanged.
fn strip_regex_anchors(literal: &str) -> String {
    literal
        .strip_prefix("^(")
        .and_then(|s| s.strip_suffix(")$"))
        .unwrap_or(literal)
        .to_owned()
}

/// The first single-quoted string literal anywhere in an expression tree.
fn first_string_literal(expr: &Expr) -> Option<String> {
    walk(expr, &mut |e| match e {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    })
}

/// The first integer numeric literal anywhere in an expression tree, honoring a
/// leading unary minus (`-1` parses as `-(1)`, not a negative literal).
fn first_int_literal(expr: &Expr) -> Option<i64> {
    walk(expr, &mut |e| match e {
        Expr::Value(v) => int_value(&v.value),
        // A negated literal is matched at the `UnaryOp` node (pre-order), before
        // the walker would otherwise descend to the unsigned inner number.
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(v) => int_value(&v.value).and_then(i64::checked_neg),
            _ => None,
        },
        _ => None,
    })
}

/// An integer numeric literal's value, or `None` for any non-integer literal.
fn int_value(value: &Value) -> Option<i64> {
    match value {
        Value::Number(digits, _) => digits.parse::<i64>().ok(),
        _ => None,
    }
}

/// Pre-order walk of the predicate-shaped expressions `\d`'s filters use,
/// returning the first node `f` maps to `Some`. Covers the binary/unary/nested
/// and value nodes a `WHERE relname = '…' AND attnum > 0`-style clause is built
/// from; other shapes simply yield no match.
fn walk<T>(expr: &Expr, f: &mut impl FnMut(&Expr) -> Option<T>) -> Option<T> {
    if let Some(hit) = f(expr) {
        return Some(hit);
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => walk(left, f).or_else(|| walk(right, f)),
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => walk(expr, f),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        stele_sql::parse(sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one statement")
    }

    #[test]
    fn recognizes_pg_class_lookup_by_relname_equality() {
        let stmt = parse_one(
            "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname = 'account'",
        );
        assert_eq!(
            classify(&stmt),
            Some(Introspection::Relation {
                name: "account".to_owned()
            })
        );
    }

    #[test]
    fn recognizes_pg_class_lookup_by_regex_with_anchors_stripped() {
        // psql's real `\d` filters with `relname ~ '^(account)$'`.
        let stmt =
            parse_one("SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname ~ '^(account)$'");
        assert_eq!(
            classify(&stmt),
            Some(Introspection::Relation {
                name: "account".to_owned()
            })
        );
    }

    #[test]
    fn recognizes_pg_attribute_lookup_by_attrelid() {
        let oid = oid_for("account");
        let stmt = parse_one(&format!(
            "SELECT a.attname FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = {oid} AND a.attnum > 0 ORDER BY a.attnum"
        ));
        assert_eq!(classify(&stmt), Some(Introspection::Attributes { oid }));
    }

    #[test]
    fn out_of_range_attrelid_stays_introspection_with_an_unmatchable_oid() {
        // A negative / overflowing attrelid still classifies as a pg_attribute
        // lookup (so it answers an empty `\d`, not feature_not_supported); it
        // saturates to 0, which `oid_for` never produces, so it matches no table.
        for filter in ["a.attrelid = -1", "a.attrelid = 99999999999999"] {
            let stmt = parse_one(&format!(
                "SELECT a.attname FROM pg_catalog.pg_attribute a WHERE {filter}"
            ));
            assert_eq!(classify(&stmt), Some(Introspection::Attributes { oid: 0 }));
        }
    }

    #[test]
    fn bare_pg_class_name_is_recognized() {
        let stmt = parse_one("SELECT oid FROM pg_class WHERE relname = 'x'");
        assert_eq!(
            classify(&stmt),
            Some(Introspection::Relation {
                name: "x".to_owned()
            })
        );
    }

    #[test]
    fn pg_class_scan_without_a_name_literal_is_a_table_list() {
        // The `\dt` shape: no WHERE at all, or a WHERE with no string literal.
        for sql in [
            "SELECT c.relname FROM pg_catalog.pg_class c ORDER BY c.relname",
            "SELECT relname FROM pg_class WHERE oid > 0",
        ] {
            assert_eq!(
                classify(&parse_one(sql)),
                Some(Introspection::TableList),
                "{sql}"
            );
        }
    }

    #[test]
    fn pg_class_with_a_name_literal_stays_a_relation_lookup() {
        // The `\d <table>` shape must not regress into a list (STL-131).
        let stmt = parse_one("SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname ~ '^(t)$'");
        assert_eq!(
            classify(&stmt),
            Some(Introspection::Relation {
                name: "t".to_owned()
            })
        );
    }

    #[test]
    fn ordinary_queries_are_not_introspection() {
        // A user table read, a constant select, and DDL are all left alone.
        assert_eq!(classify(&parse_one("SELECT balance FROM account")), None);
        assert_eq!(classify(&parse_one("SELECT 1")), None);
        assert_eq!(
            classify(&parse_one("CREATE TABLE t (a INT) WITH SYSTEM VERSIONING")),
            None
        );
    }

    #[test]
    fn oid_is_stable_positive_and_nonzero() {
        assert_eq!(oid_for("account"), oid_for("account"));
        assert_ne!(oid_for("account"), 0);
        // Fits the non-negative i32 range so it renders as a clean int4.
        assert!(i32::try_from(oid_for("account")).is_ok());
        assert_ne!(oid_for("account"), oid_for("ledger"));
    }
}
