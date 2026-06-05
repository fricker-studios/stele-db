# SQL grammar — temporal extensions

> **Status:** v0.1 parser bootstrap (STL-97). Tracks the parser only; binding,
> planning, and execution land in later tickets.
> **Read with:** [02 — Architecture §6](02-architecture.md#6-query-layer).

Stele's SQL frontend (`stele-sql`) starts from [`sqlparser-rs`][sqlparser] and
layers a small **temporal grammar** on top — the bitemporal constructs that make
Stele *Stele* and that standard SQL has no AST node for. This document is the
reference for that grammar and its v0.1 implementation status.

The bulk of SQL (expressions, `SELECT`/`INSERT`/`UPDATE`/`DELETE`,
`CREATE TABLE` column definitions, …) is parsed by `sqlparser-rs` unchanged; see
its docs for that surface. Only the Stele-specific additions are described here.

## Entry point

```rust
let statements: Vec<stele_sql::Statement> = stele_sql::parse(sql)?;
```

`parse` accepts one or more `;`-separated statements. Each returned
[`Statement`] pairs:

- `body` — the underlying `sqlparser-rs` `Statement`, with Stele's non-standard
  clauses stripped so it is always a clean, standard-SQL AST; and
- `temporal` — the temporal grammar lifted out of those clauses, as typed
  annotations the binder can act on.

### How it is parsed

`sqlparser-rs` parses `FOR SYSTEM_TIME AS OF` natively (Stele enables it via the
dialect flag `supports_table_versioning`), but has no grammar for the other
clauses below. Rather than fork the parser this early, `parse` runs a small pass
over the **token stream**: it lifts the non-standard clauses into `temporal`,
rewrites `VALID_TIME` → `SYSTEM_TIME` so the qualifier parses, and hands the
standard remainder to `sqlparser-rs`. The lifted time axis is recovered
afterward from the recorded order.

## Temporal constructs

### `FOR { SYSTEM_TIME | VALID_TIME } AS OF <expr>` — time-travel select

```sql
SELECT balance FROM account
  FOR SYSTEM_TIME AS OF (now() - interval '1 second')
  WHERE id = 1;
```

A table reference may carry an `AS OF` qualifier selecting the version of each
row visible at an instant. `<expr>` is any scalar expression; the binder and
optimizer fold it to a concrete timestamp.

Captured as `Temporal::as_of: Vec<AsOf>`, one entry per qualifier in
left-to-right source order, each with:

| Axis          | Meaning                              | v0.1 status |
|---------------|--------------------------------------|-------------|
| `SYSTEM_TIME` | when a fact was *recorded*           | implemented |
| `VALID_TIME`  | when a fact was *true in the world*  | **parsed, not yet implemented** |

`VALID_TIME AS OF` is intentionally **accepted** by the parser and tagged
`TimeDimension::Valid` (`is_implemented() == false`) so the binder can reject it
with a precise message, rather than the parser silently mis-parsing or
misleadingly accepting it. Full valid-time `AS OF` is post-v0.1.

### `CREATE TABLE … WITH SYSTEM VERSIONING` — opt into system-time history

```sql
CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING;
```

Marks the table as system-versioned: every row keeps its full system-time
history (`sys_from`/`sys_to`). Captured as `Temporal::system_versioning: bool`.
The clause follows the column list.

### `CREATE TABLE … VALID TIME (from, to)` — opt into a valid-time period

```sql
CREATE TABLE booking (id INT, valid_from TIMESTAMP, valid_to TIMESTAMP)
  WITH SYSTEM VERSIONING VALID TIME (valid_from, valid_to);
```

Declares the two columns that form the table's application-time (valid-time)
period. Captured as `Temporal::valid_time: Option<ValidTimePeriod>`. May appear
in either order relative to `WITH SYSTEM VERSIONING`.

## Type vocabulary

Column types in `CREATE TABLE` are parsed as standard `sqlparser` `DataType`
nodes (syntactic). [`stele_sql::logical_type`] lowers them to the semantic
`LogicalType` vocabulary owned by `stele-common` (STL-96) — the seam between the
parser and the catalog (STL-98) / executor / pgwire encoder. The v0.1 set:

| SQL surface type                       | `LogicalType` | Postgres OID |
|----------------------------------------|---------------|--------------|
| `INT`, `INTEGER`                       | `Int4`        | 23           |
| `BIGINT`                               | `Int8`        | 20           |
| `TEXT`                                 | `Text`        | 25           |
| `BOOL`, `BOOLEAN`                      | `Bool`        | 16           |
| `TIMESTAMP` (no time zone)             | `Timestamp`   | 1114         |
| `DATE`                                 | `Date`        | 1082         |

Anything else — `VARCHAR`, `CHAR`, `REAL`, `TIMESTAMP WITH TIME ZONE`, … — is
rejected (`ParseError::UnsupportedType`) rather than silently coerced; these are
deliberate later additions.

## AST shape

```text
Statement
├── body: sqlparser::ast::Statement     // standard SQL, clauses stripped
└── temporal: Temporal
    ├── system_versioning: bool         // WITH SYSTEM VERSIONING
    ├── valid_time: Option<ValidTimePeriod>   // VALID TIME (from, to)
    └── as_of: Vec<AsOf>                 // FOR <axis> AS OF <expr>
        ├── dimension: TimeDimension { System | Valid }
        └── timestamp: sqlparser::ast::Expr
```

A statement with no temporal grammar carries `Temporal::default()` (all empty);
`Statement::is_temporal()` reports the difference.

## Not yet supported (deferred)

These are recognized as future work, not silently accepted. Some are already
*parsed* (e.g. `FOR VALID_TIME AS OF`) but deferred in *implementation*:

- `FOR VALID_TIME AS OF` execution (parsed and tagged; binder rejects — above).
- Period predicates (`WHERE … CONTAINS PERIOD …`, `OVERLAPS`).
- `AS OF` on DML and on system-versioned `DROP`/`ALTER`.
- A hand-written parser — revisited only if `sqlparser-rs` becomes a constraint
  ([Architecture §6](02-architecture.md#6-query-layer)).

[sqlparser]: https://docs.rs/sqlparser
[`Statement`]: ../crates/stele-sql/src/ast.rs
