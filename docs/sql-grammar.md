# SQL grammar — temporal extensions

> **Status:** v0.1 parser bootstrap (STL-97) + DDL binding (STL-95) + the
> `SELECT … FOR SYSTEM_TIME AS OF` query binder (STL-101), extended to the
> **valid axis** — `FOR VALID_TIME AS OF` now binds too (STL-162) — plus the
> SQL:2011 **period predicates** (`CONTAINS` / `OVERLAPS` / `PRECEDES` /
> `SUCCEEDS` / `EQUALS` / `MEETS`, STL-165). The parser, the `CREATE TABLE` /
> `DROP TABLE` binder, both-axes AS-OF snapshot resolution, and period predicates
> (constant operands, STL-165, and per-row value-column operands, STL-193) are
> live; the executor's joint `(sys, valid)` resolution and wiring the bound plan
> through the pgwire query loop land in later tickets.
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

`sqlparser-rs` has no grammar for the clauses below, and allows only one
`FOR … AS OF` qualifier per table — too few for a bitemporal `… FOR SYSTEM_TIME
AS OF s FOR VALID_TIME AS OF v`. Rather than fork the parser this early, `parse`
runs a small pass over the **token stream**: it lifts the non-standard clauses
into `temporal`, including **every** `FOR { SYSTEM_TIME | VALID_TIME } AS OF
<expr>` qualifier — parsing each `<expr>` with `sqlparser-rs`'s own expression
parser — and hands the clean standard-SQL remainder to `sqlparser-rs`. The lifted
qualifiers (with their axis, in source order) are the single source of truth; the
dialect leaves `supports_table_versioning` **off**, so `sqlparser-rs` never parses
a versioned table itself.

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

| Axis          | Meaning                              | status |
|---------------|--------------------------------------|--------|
| `SYSTEM_TIME` | when a fact was *recorded*           | binds + executes |
| `VALID_TIME`  | when a fact was *true in the world*  | **binds (STL-162); executor resolution lands in STL-163** |

A table may carry **one qualifier per axis**, in either order, so a bitemporal
point names both. A `VALID_TIME AS OF` against a table with no valid-time period
is rejected at bind time (`SelectError::ValidTimeUnsupported`) — there is no valid
axis to travel.

### `PERIOD(from, to) <predicate> PERIOD(from, to)` — period predicates

```sql
SELECT * FROM booking
  WHERE PERIOD(1700000000000000, 1700000003600000000) OVERLAPS PERIOD(now() - interval '1 hour', now());
```

The SQL:2011 **period predicates** ask range questions over two half-open
`[from, to)` periods ([STL-165]). Each is a boolean relation between a left and a
right `PERIOD(from, to)` operand:

| Predicate                | True (for left `a`, right `b`) iff |
|--------------------------|------------------------------------|
| `a CONTAINS b`           | `a.from ≤ b.from` and `b.to ≤ a.to` |
| `a OVERLAPS b`           | `a.from < b.to` and `b.from < a.to` (share a point) |
| `a EQUALS b`             | identical bounds                   |
| `a PRECEDES b`           | `a.to ≤ b.from`                    |
| `a SUCCEEDS b`           | `b.to ≤ a.from`                    |
| `a IMMEDIATELY PRECEDES b` (a.k.a. `a MEETS b`) | `a.to == b.from` |
| `a IMMEDIATELY SUCCEEDS b` | `a.from == b.to`                 |

Because periods are **half-open**, a touching boundary lands on the right side of
the line: `PERIOD(10, 20) PRECEDES PERIOD(20, 30)` is **true** (they meet, sharing
no point), while `PERIOD(10, 20) OVERLAPS PERIOD(20, 30)` is **false**. The end of
a period may be `+∞` (an open period). The exhaustive boundary truth table is
pinned in `stele-exec`'s tests.

Like `FOR … AS OF`, the predicate is lifted off the token stream (`sqlparser-rs`
has no grammar for these keywords) and bound separately. **v0.2 status**, each a
deliberate boundary, not a silent gap:

- A period predicate is recognized only as the **entire** `WHERE` clause (it
  begins with `PERIOD`). Combining it with other conditions
  (`… OVERLAPS … AND id = 1`) is rejected, not silently half-applied.
- Each `PERIOD(from, to)` endpoint is either a **constant instant** — folded the
  same way an `AS OF` operand is (`now()`, `now() ± interval '…'`, an integer
  microsecond literal) — or a **value column** of the row, read as microseconds
  (a `BIGINT` / `TIMESTAMP` / `TIMESTAMPTZ` column; the two kinds may be mixed in
  one operand). With only constant endpoints the whole predicate is a constant
  truth value the executor applies once (a false predicate returns no rows); with
  a column endpoint the executor builds each row's `[from, to)` from its cells and
  keeps only the rows the predicate accepts ([STL-193]). A column endpoint of any
  other type is rejected at bind time, never silently mis-scaled.
- A **constant** operand that folds to an empty or reversed `[from, to)`
  (`from ≥ to`) is a bind error — half-open periods require `from < to`. A
  per-row operand can only be checked once its cells are known, so a NULL or
  empty/reversed row period is *unknown* and excludes that row rather than
  failing the query.

Captured as `Temporal::period_predicate: Option<PeriodPredicateClause>`; bound to
a `BoundPeriodPredicate` of two `Interval`s and a `PeriodPredicate`, evaluated by
[`stele_exec::evaluate`].

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

## DDL binding (STL-95)

Beyond parsing, `stele_sql::bind_ddl` lowers a `CREATE TABLE` / `DROP TABLE`
into a `DdlStatement` that `apply`s to a `stele-catalog` `Catalog`:

- **`CREATE TABLE`** requires `WITH SYSTEM VERSIONING` — every Stele table is
  system-versioned (invariant 4), so the clause is mandatory rather than
  silently assumed; a bare `CREATE TABLE` is rejected. `VALID TIME (f, t)` must
  name two declared `TIMESTAMP` columns. Columns lower through
  [`logical_type`](#type-vocabulary).
- **`DROP TABLE`** is a *logical* drop — a catalog version transition that closes
  the table's open schema version, never a segment deletion. A read `AS OF` an
  instant before the drop still sees the table; the name may later be re-created.
  `DROP TABLE IF EXISTS` of an absent table is a no-op.
- **Rejected in v0.1**, each with a roadmap pointer: constraints other than a
  column-level `PRIMARY KEY` (`FOREIGN KEY`/`REFERENCES`, `UNIQUE`, `CHECK`,
  `NOT NULL`, `DEFAULT`, …), table-level constraints, indexes,
  `CREATE TABLE … AS SELECT`, `LIKE`/`CLONE`, `IF NOT EXISTS`, `OR REPLACE`,
  temporary/external tables, schema-qualified names, and `DROP … CASCADE`.
  `PRIMARY KEY` is **accepted but not enforced** (no uniqueness/index yet), so
  the identity-demo `CREATE TABLE account (id INT PRIMARY KEY, balance INT) …`
  binds.

## Query binding (STL-101, STL-162)

`stele_sql::bind_select` lowers a `SELECT … [FOR SYSTEM_TIME AS OF <s>]
[FOR VALID_TIME AS OF <v>]` into a `BoundSelect` — a single table, the schema
live at the resolved system-time snapshot, that `snapshot`, an optional
`valid_snapshot`, and a projection — ready for the executor to lower to a
`SnapshotScan` (STL-100):

- **Each `AS OF <expr>` is folded to a concrete instant.** Supported forms:
  `now()` (folds to the transaction snapshot), `now() ± interval '<n> <unit>'`
  (seconds … weeks; calendar units month/year are rejected — they have no fixed
  microsecond length), and a bare integer read as explicit microseconds. Both
  axes fold the same way. Absolute `TIMESTAMP '…'` literals are **not** folded yet
  (no civil-time codec) — they are rejected, not silently mis-resolved.
- **No system `AS OF` ⇒ the transaction snapshot.** A plain `SELECT` reads the
  present. **No valid `AS OF` ⇒ `valid_snapshot` is `None`** — the executor reads
  the present of the valid axis.
- **At most one qualifier per axis**, in either order; a repeated axis is
  `SelectError::MultipleAsOf`. A `FOR VALID_TIME AS OF` against a table with no
  valid-time period is `SelectError::ValidTimeUnsupported`.
- **The table is resolved against the versioned catalog at that snapshot**, so a
  past `AS OF` binds under the schema that was live *then*. A snapshot *before
  the table's first commit* is the documented **before-history** error
  (`SelectError::BeforeHistory`) — never a silent empty read; a name the catalog
  never registered is the distinct `UnknownTable`.
- **The resolved system snapshot is the `sys_from ≤ s` push-down** the executor
  applies to segment-level zone-map pruning (the close bound comes from the
  validity index — [ADR-0023], STL-133). The binder does not re-implement the
  prune; carrying the snapshot *is* the rewrite. Joint `(sys, valid)` version
  resolution from `valid_snapshot` is the executor's job (STL-163).
- **Rejected in v0.1**: joins / set operations / subqueries (single-table scan
  only), schema-qualified table names, and projections other than `*` or bare
  column names. The `WHERE` clause stays on the AST for the executor-glue layer
  (pgwire, STL-104) to lower.

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
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMPTZ` | `TimestampTz` | 1184      |
| `DATE`                                 | `Date`        | 1082         |

`TimestampTz` is stored UTC-internal: a literal's zone offset is normalized to
UTC on input and rendered back with a `+00` offset (STL-189; the codec lives in
[`stele_common::datetime`]). A `timestamptz` literal *can* be written in DML; the
zone-less `TIMESTAMP` / `DATE` have no literal codec yet (mirrors `AS OF`).

Anything else — `VARCHAR`, `CHAR`, `REAL`, … — is rejected
(`ParseError::UnsupportedType`) rather than silently coerced; these are
deliberate later additions.

## AST shape

```text
Statement
├── body: sqlparser::ast::Statement     // standard SQL, clauses stripped
└── temporal: Temporal
    ├── system_versioning: bool         // WITH SYSTEM VERSIONING
    ├── valid_time: Option<ValidTimePeriod>   // VALID TIME (from, to)
    ├── as_of: Vec<AsOf>                 // FOR <axis> AS OF <expr>
    │   ├── dimension: TimeDimension { System | Valid }
    │   └── timestamp: sqlparser::ast::Expr
    └── period_predicate: Option<PeriodPredicateClause>   // PERIOD(..) <pred> PERIOD(..)
        ├── left:  PeriodExpr { from, to: sqlparser::ast::Expr }
        ├── predicate: PeriodPredicate { Contains | Overlaps | Equals | … }
        └── right: PeriodExpr { from, to: sqlparser::ast::Expr }
```

A statement with no temporal grammar carries `Temporal::default()` (all empty);
`Statement::is_temporal()` reports the difference.

## Not yet supported (deferred)

These are recognized as future work, not silently accepted. Some are already
*bound* (e.g. `FOR VALID_TIME AS OF`) but deferred in *execution*:

- `FOR VALID_TIME AS OF` executor resolution (binds and carries the instant; the
  joint `(sys, valid)` version pick is STL-163 — above).
- A **constant `TIMESTAMP` / `DATE` literal** as a period endpoint or `AS OF`
  operand — the zone-less civil-time codec is still missing, so a constant
  endpoint is a bare integer-microsecond literal for now (per-row endpoints read
  real `TIMESTAMP` columns; see [above](#periodfrom-to-predicate-periodfrom-to--period-predicates)).
- `AS OF` on DML and on system-versioned `DROP`/`ALTER`.
- A hand-written parser — revisited only if `sqlparser-rs` becomes a constraint
  ([Architecture §6](02-architecture.md#6-query-layer)).

[sqlparser]: https://docs.rs/sqlparser
[`Statement`]: ../crates/stele-sql/src/ast.rs
