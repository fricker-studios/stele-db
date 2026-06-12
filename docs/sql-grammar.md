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
  `NOT NULL`, `DEFAULT`, …), table-level constraints,
  `CREATE TABLE … AS SELECT`, `LIKE`/`CLONE`, `IF NOT EXISTS`, `OR REPLACE`,
  temporary/external tables, schema-qualified names, and `DROP … CASCADE`.
  `PRIMARY KEY` is **accepted but not enforced** (no uniqueness/index yet), so
  the identity-demo `CREATE TABLE account (id INT PRIMARY KEY, balance INT) …`
  binds.

### `CREATE INDEX` / `DROP INDEX` — secondary indexes (STL-233, STL-237)

```sql
CREATE INDEX i_balance ON account (balance);
DROP INDEX i_balance;
DROP INDEX IF EXISTS i_balance;
```

The v0.3 secondary-index substrate: a **named, single-column** index in the
default (B-tree) kind on a **value column** — the business key (the table's
first column) is always indexed by storage and is refused. An index is
*derived, rebuildable* state (ADR-0023): only the DDL metadata is durable
(ADR-0028 catalog log), the access structure is built from the table's tiers,
maintained on every committed write, and rebuilt on cold boot. It can change a
query's *speed*, never its *results* — the indexed≡unindexed equivalence
oracle pins exactly that. `DROP TABLE` drops the table's indexes with it; the
re-created name starts index-free. `DROP INDEX IF EXISTS` of an absent index
is a no-op.

A read uses the index rule-based, when the `WHERE` is a bare
`<indexed column> <cmp> <literal>` comparison: `=` probes the entry exactly
(STL-233), and `<` `<=` `>` `>=` probe a candidate range walked in the
column type's *value* order (STL-237) — the ordered structure keys its
entries memcomparably, so integer and temporal columns range correctly
across the sign boundary. `FLOAT8` and `PERIOD` columns decline range
service (their encodings don't byte-order by value; equality still probes),
`<>` never probes (no window covers a complement), and a predicate-driven
`UPDATE`/`DELETE` (STL-229) routes its scan through the same probe.

Rejected with a roadmap pointer until their sibling tickets land: `UNIQUE`,
`USING <kind>` (hash/bloom is STL-238, the valid-time interval kind STL-241),
multi-column and expression columns, partial indexes (`… WHERE`), `INCLUDE`,
`CONCURRENTLY`, `IF NOT EXISTS`, and per-column `ASC`/`DESC`/`NULLS` ordering.

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

## Result shaping (STL-263)

`ORDER BY`, `LIMIT`/`OFFSET`/`FETCH`, and `DISTINCT` bind and execute on the
single-table `SELECT` path — plain and aggregate reads, under `AS OF` on either
axis, and inside transactions (the read-your-own-writes overlay is shaped like
committed rows). The executor applies them in the Postgres pipeline order:

```text
WHERE → [GROUP BY/aggregates] → DISTINCT → ORDER BY → OFFSET → LIMIT
```

- **`ORDER BY col [ASC|DESC], …`** — bare column names, multi-key, first key
  outermost. A name resolves against the **select list first** (an aggregate
  query's output columns, aliases included); a plain non-`DISTINCT` query may
  also sort on an unprojected schema column, as Postgres allows. NULL placement
  is the Postgres default — **NULLS LAST under `ASC`, NULLS FIRST under
  `DESC`** — and every shipped type orders (`UUID`/`BYTEA` byte-wise, where
  Postgres is byte-wise). Rejected with the reason: expressions/ordinals as
  keys, and an explicit `NULLS FIRST`/`NULLS LAST` override.
- **`LIMIT n` / `OFFSET m` / `FETCH FIRST n ROWS ONLY`** — non-negative integer
  literals (`FETCH FIRST` is the standard `LIMIT` alias; an omitted count reads
  as 1). `LIMIT ALL` is explicitly unlimited; `LIMIT 0` is a valid empty read;
  an `OFFSET` past the end is empty, never an error. Rejected: negative or
  non-literal counts, `WITH TIES`, `PERCENT`, the MySQL `LIMIT off, n`.
- **`SELECT DISTINCT`** — deduplicates the **full projected row** (NULLs equal,
  the `GROUP BY` rule): exactly `GROUP BY` every output column with no
  aggregates, and it reuses that hash machinery. With `DISTINCT`, an `ORDER BY`
  key must appear in the select list — sorting on a discarded column is
  Postgres's **42P10** (`invalid_column_reference`), returned with the same
  wording. `DISTINCT ON (…)` is rejected (a different operation, not
  approximated).

Result shaping over a **join** read is rejected (`ORDER BY`/`LIMIT`/`DISTINCT`
over a `JOIN`, the same posture as `WHERE` and aggregates over a join) — join
composability is STL-264. Top-N pushdown and sort spill are performance work,
deliberately out of v0.3 scope.

## DML row selection (STL-229)

`UPDATE` / `DELETE` select the rows they write with the **same `WHERE`
vocabulary the `SELECT` path evaluates** (STL-213): a single comparison anchored
on one column — any of `=`, `<>`, `<`, `<=`, `>`, `>=`, with integer `+ - * / %`
arithmetic on either side — or **no `WHERE` at all**, which is a whole-table
write. The v0.1 `WHERE <key> = <literal>`-only restriction is lifted.

```sql
UPDATE account SET balance = 0 WHERE balance > 100;  -- value-column predicate
DELETE FROM account WHERE id <= 3;                   -- non-equality key predicate
DELETE FROM account;                                 -- whole table
```

Two plans, one semantics:

- **Point fast path** — a `WHERE` that is exactly `<key> = <literal>` (either
  operand order) lowers to the existing single-key write, with no scan. Its
  pre-STL-229 contract is kept: writing an **absent** key is a statement error
  (`KeyNotFound`), not a 0-row tag.
- **Scan-then-write** — everything else (any other predicate, or no `WHERE`)
  enumerates the matching **live** business keys at the statement snapshot, then
  applies one per-key write per match as a **single atomic group** (one WAL
  record, one fsync — the STL-192 group-commit discipline): a failure applying
  any row of the set aborts the whole statement, leaving the table unchanged.

The `UPDATE n` / `DELETE n` command tag counts the **matched live rows at the
statement snapshot** (`0` when nothing matches). Inside a `BEGIN … COMMIT` block
the matching scan overlays the transaction's own buffered writes
(read-your-own-writes, STL-203) — an `INSERT` staged earlier in the block is
matchable — and the matched set is fixed at the statement, not re-evaluated at
`COMMIT`. On a valid-time table the selection is **system-axis-only** (it picks
among system-live rows; an `UPDATE` still supplies the new version's period
through its `SET`, as for a point write — STL-194); a valid-time-*scoped* bulk
write is future work. A period predicate (`WHERE PERIOD(…) … PERIOD(…)`) is not
accepted on DML.

## MERGE upsert (STL-230)

First-class `MERGE` — the statement shape of the v0.3 historization workhorse.
Source rows probe the target by **business key** at the statement snapshot; the
`MATCHED` arm updates, the `NOT MATCHED` arm inserts, and the whole statement
applies as **one crash-atomic group** over the STL-229 scan-then-write plan.

```sql
MERGE INTO account [AS a]
USING (VALUES (1, 100), (3, 300)) AS s (id, balance)   -- or: USING feed [AS s]
ON account.id = s.id                                    -- the business-key equality
WHEN MATCHED THEN UPDATE SET balance = s.balance
WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.balance);
```

The supported subset:

- **Source**: a `VALUES` list with named alias columns (`AS s (c1, …)`, folded
  to typed rows at bind — each column's type is inferred from where it is used,
  and conflicting uses are a bind error), or a **plain table** (read at the
  statement snapshot when the plan expands). Subqueries beyond `VALUES` are
  rejected.
- **`ON`**: exactly one equality joining the target's business key to one source
  column, either operand order. A non-key target column, `AND` chains, or
  expressions are rejected.
- **Arms**: at most one `WHEN MATCHED THEN UPDATE SET …` and one `WHEN NOT
  MATCHED THEN INSERT … VALUES …`, either alone — a source row whose arm is
  absent is skipped. Arm values are literals or source-column references
  (`s.c`); referencing the target row is rejected. The `INSERT` arm follows the
  plain-`INSERT` column-list discipline (positional with no list; every target
  column must be supplied).

Semantics, pinned:

- Per-source-row arm resolution happens **at execution**, against the target's
  live keys at the statement snapshot. Inside a `BEGIN … COMMIT` block both the
  probe and a table source overlay the transaction's own buffered writes
  (read-your-own-writes, STL-203), and the per-key writes are what the buffer
  holds — the acted-on set is fixed at the statement, not at `COMMIT`.
- The `MERGE n` command tag counts the **acted-on source rows** (each one update
  or one insert; skipped rows don't count).
- The write set applies as a single atomic group (one WAL record, one fsync —
  STL-192): a failure on any row leaves the table unchanged (STL-216).
- Two source rows resolving to the **same target row** fail the statement with
  SQLSTATE `21000` (`cardinality_violation`) before any write applies — the
  standard's deterministic posture; so do `NULL` flowing into the inserted
  business key (a `NULL` join key itself simply never matches).
- A `MERGE` into a **valid-time table** is rejected: valid-period close/open
  semantics are the temporal-MERGE sibling (STL-235). `WHEN NOT MATCHED BY
  SOURCE`, `WHEN MATCHED THEN DELETE`, clause predicates (`WHEN … AND <expr>`),
  and `OUTPUT` are rejected.

## Type vocabulary

Column types in `CREATE TABLE` are parsed as standard `sqlparser` `DataType`
nodes (syntactic). [`stele_sql::logical_type`] lowers them to the semantic
`LogicalType` vocabulary owned by `stele-common` (STL-96) — the seam between the
parser and the catalog (STL-98) / executor / pgwire encoder. The set:

| SQL surface type                       | `LogicalType` | Postgres OID |
|----------------------------------------|---------------|--------------|
| `INT`, `INTEGER`                       | `Int4`        | 23           |
| `BIGINT`                               | `Int8`        | 20           |
| `TEXT`, `VARCHAR[(n)]`, `CHARACTER VARYING[(n)]`, `CHAR VARYING[(n)]`, `NVARCHAR[(n)]` | `Text` | 25 |
| `BOOL`, `BOOLEAN`                      | `Bool`        | 16           |
| `TIMESTAMP` (no time zone)             | `Timestamp`   | 1114         |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMPTZ` | `TimestampTz` | 1184      |
| `DATE`                                 | `Date`        | 1082         |
| `UUID`                                 | `Uuid`        | 2950         |
| `BYTEA`                                | `Bytea`       | 17           |

The character-*varying* spellings are all `Text` under the hood — a declared
length (`VARCHAR(50)`) is accepted as documentation but **not enforced** (no
typmod machinery; enforcement is a later ticket), matching how Postgres treats
an unconstrained `varchar`.

All three civil-time types have DML literal codecs in
[`stele_common::datetime`]. `TimestampTz` is stored UTC-internal: a literal's
zone offset is normalized to UTC on input and rendered back with a `+00` offset
(STL-189). The zone-less `TIMESTAMP` shares the grammar but **rejects** an
explicit zone offset rather than Postgres-style silently ignoring it (dropping
an offset the user wrote would change the instant they named); `DATE` is the
pure `YYYY-MM-DD` form. Typed-string literals (`TIMESTAMP '…'`, `DATE '…'`,
`UUID '…'`) fold when the declared type **lowers to the column's
`LogicalType`** — so `VARCHAR '…'` folds into a `TEXT` column, since both lower
to `Text`; a mismatch after lowering is a type error, never an implicit cast.

Anything else — the blank-padding `CHAR(n)`, `REAL`, … — is rejected
(`ParseError::UnsupportedType`, with the supported vocabulary in the message)
rather than silently coerced; these are deliberate later additions.

## AST shape

```text
Statement
├── body: StatementBody
│   ├── Sql(sqlparser::ast::Statement)   // standard SQL, clauses stripped
│   └── Admin(AdminCommand)              // CHECKPOINT | FLUSH | COMPACT — no sqlparser grammar
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
`Statement::is_temporal()` reports the difference. `Statement::sql()` returns the
standard-SQL body, or `None` for an admin command — the seam the binders and the
wire layer read so an admin command cleanly classifies as "none of the SQL routes".

## Admin commands (STL-219, STL-231)

Operator-facing storage commands. `sqlparser` has no grammar for them,
so they are recognized at the token level — the same lift the temporal clauses use
— and represented as a `StatementBody::Admin` body rather than a `sqlparser` node.
All take no arguments; a trailing token is an error. The engine routes each to the
matching session-wide operation and replies with the command's own
`CommandComplete` tag.

| Command | Engine op | Effect |
|---|---|---|
| `CHECKPOINT` | `SessionEngine::checkpoint` | Lightweight WAL fence over every table — fsync + record the fence, no seal. |
| `FLUSH` | `SessionEngine::flush` | Seal every table's delta into a segment and advance its replay floor (bounded recovery — STL-177 / STL-195). |
| `COMPACT` | `SessionEngine::compact` | Flush, then merge every table's sealed segments into one read-optimized segment, retiring the inputs — history-preserving (STL-231, ADR-0030). |

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
