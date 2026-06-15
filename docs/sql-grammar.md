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

### `CREATE INDEX` / `DROP INDEX` — secondary indexes (STL-233, STL-237, STL-238)

```sql
CREATE INDEX i_balance ON account (balance);            -- ordered (B-tree) default
CREATE INDEX i_balance ON account USING HASH (balance); -- equality-only hash kind
DROP INDEX i_balance;
DROP INDEX IF EXISTS i_balance;
```

The v0.3 secondary-index substrate: a **named, single-column** index in the
default (B-tree) kind or the `USING HASH` equality kind (STL-238), on a **value
column** — the business key (the table's first column) is always indexed by
storage and is refused. An index is
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
across the sign boundary. A `USING HASH` index serves only `=` (it cannot walk
its keys in value order, so a range probe on it falls back to a full scan);
`FLOAT8` and `PERIOD` columns decline range service (their encodings don't
byte-order by value; equality still probes), `<>` never probes (no window
covers a complement), and a predicate-driven `UPDATE`/`DELETE` (STL-229) routes
its scan through the same probe.

Independently of any declared index, every sealed segment carries a **per-segment
bloom filter over the business key** (STL-238): a point read or `MERGE` probe by
business key skips a whole segment whose bloom proves the key absent — the
hash/scatter-key case zone maps cannot prune. It is advisory (read-gating only,
configurable false-positive rate) and rides the immutable segment, so it survives
flush, compaction, and recovery. The skips surface as
`stele_scan_segments_pruned_bloom_total` in the metrics.

Rejected with a roadmap pointer until their sibling tickets land: `UNIQUE`,
other `USING <kind>` (GIN/GiST/BRIN; the valid-time interval kind is STL-241),
multi-column and expression columns, partial indexes (`… WHERE`), `INCLUDE`,
`CONCURRENTLY`, `IF NOT EXISTS`, and per-column `ASC`/`DESC`/`NULLS` ordering.

### `CREATE USER` / `ALTER USER` / `DROP USER` — authentication (STL-252)

```sql
CREATE USER alice PASSWORD 's3cret';
ALTER USER alice WITH PASSWORD 'rotated';   -- WITH is optional
DROP USER alice;
DROP USER IF EXISTS alice;
```

The pg-wire authentication user store (docs/10 §5). Like the admin commands,
the family is **lifted at the token level**: `sqlparser` parses `CREATE USER`
with Snowflake's `KEY = VALUE` option grammar, which rejects the Postgres
`PASSWORD '…'` form — so the lift owns the grammar and a malformed tail is a
loud syntax error. The engine derives a salted, iterated **SCRAM-SHA-256
verifier** (RFC 7677) and appends it to the durable catalog log (ADR-0028) —
the password itself is never stored, and the parsed AST redacts it from
`Debug` output. Command tags are Postgres's role tags (`CREATE ROLE` /
`ALTER ROLE` / `DROP ROLE`); a duplicate `CREATE USER` is `42710`, an unknown
`ALTER`/`DROP USER` is `42704`. User names are matched verbatim (no
case-folding), like table and column names. The empty password is rejected at
parse time.

Out of scope until their tickets land: role options (`LOGIN`, `SUPERUSER`,
`CREATEDB`, …), `CREATE ROLE`/`GRANT` (RBAC is v0.5), `IF NOT EXISTS`, and
`RENAME TO`.

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
- **Beyond the single-table scan**: a two-table join binds (STL-172) and an
  uncorrelated `WHERE` subquery binds (STL-234, see [Subquery
  predicates](#subquery-predicates-stl-234)). **Rejected**: set operations,
  schema-qualified table names, and projections other than `*` or bare column
  names (a computed or aliased select item — including a scalar subquery in the
  select list — is not yet projected). The `WHERE` clause stays on the AST for
  the executor-glue layer (pgwire, STL-104) to lower.

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

## Subquery predicates (STL-234)

A `WHERE` may be a single **uncorrelated subquery** predicate — the inner query
references no outer column, so it is evaluated **once** and its result folded
into the outer row filter. Three shapes bind:

```sql
SELECT id FROM t WHERE a = (SELECT max(a) FROM s);             -- scalar comparison
SELECT id FROM t WHERE a IN (SELECT a FROM s);                 -- [NOT] IN
SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE a > 100); -- [NOT] EXISTS
```

- **Scalar** — `<column> <cmp> (SELECT <scalar>)` (either operand order). The
  inner returns exactly one column, and at runtime **at most one row** — more is
  the standard's cardinality violation (SQLSTATE `21000`, refused before the
  outer scan). **No row ⇒ the scalar is `NULL`**, so the comparison is unknown
  for every row and none match.
- **`<column> [NOT] IN (SELECT <col>)`** — membership in the inner's
  single-column set, under SQL three-valued logic. `IN` keeps a row when the
  column equals a non-`NULL` member; an **empty (or all-`NULL`) set keeps no
  row**. `NOT IN` keeps a row when the column differs from every member, but a
  **`NULL` anywhere in the set keeps no row** (the classic three-valued trap),
  and an empty set keeps every row.
- **`[NOT] EXISTS (SELECT …)`** — row-presence only; the inner's select-list is
  irrelevant (`SELECT 1`, `SELECT *`, `SELECT k` are equivalent). Being
  uncorrelated, the test is one constant for the whole scan.

**Snapshot rule.** The inner query inherits the outer statement's resolved
`(sys, valid)` snapshot — one consistent snapshot per statement (docs/16 §6). A
`… (SELECT … FROM s) … FOR SYSTEM_TIME AS OF p` reads `s` at `p` too; inside a
transaction the inner also sees the read-your-own-writes buffer (STL-203).

The outer operand of a scalar / `IN` comparison must be a bare value column, and
the inner's single column must match its type (no implicit coercion). **Not yet
bound** (each a tracked follow-up): a scalar subquery in the **select list**
(needs expression projection), **correlated** subqueries (STL-239), subqueries in
`FROM` / CTEs (STL-242), and a subquery composed with `AND` / `OR` or set over a
join. Bound as `BoundSelect::subquery_filter` (mutually exclusive with the plain
and period `WHERE` shapes); the engine's `resolve_filter` runs the inner once and
folds it into the same `FilterPlan` the plain path produces.

## Multi-row INSERT (STL-228)

`INSERT INTO t VALUES (…), (…), …` binds **every** row and applies them as one
crash-atomic group — all rows commit together or, if any row fails, none do. The
single-row v0.1 restriction (STL-149) is lifted.

```sql
INSERT INTO account VALUES (1, 100), (2, 200), (3, 300);  -- INSERT 0 3
```

- **Per-row binding.** Each row folds exactly like its own single-row `INSERT`
  (positional, or by an explicit column list reused across every row); an arity,
  type, or bad-literal failure names the offending row (1-based, in statement
  order). A malformed *column list* is a statement-level error, not attributed to
  a row.
- **One atomic group.** The rows expand into one per-row write applied through the
  group-commit path (one WAL record, one fsync — the STL-192 discipline): a
  failure on any row — a duplicate key, or a key repeated within the statement —
  aborts the whole statement via the STL-216 abort rollback, leaving **zero**
  rows. Inside a `BEGIN … COMMIT` block the rows buffer like any other write
  (read-your-own-writes, STL-203) and commit with the transaction.
- **Command tag.** `INSERT 0 N` counts every inserted row (the leading `0` is the
  legacy OID field); a single-row `INSERT` is unchanged (`INSERT 0 1`).

`INSERT … SELECT` stays out of scope (a clear bind error, never a wrong write).
This is the statement-sized stepping stone to v0.3 bulk ingest (`COPY`).

## Bulk load — `COPY … FROM STDIN` (STL-236)

`COPY <table> [(col, …)] FROM STDIN [WITH (…)]` is the standard Postgres bulk-load
door, spoken over the pg-wire COPY sub-protocol (`CopyInResponse` → `CopyData`* →
`CopyDone`/`CopyFail`). It is the wire half of v0.3 bulk ingest — `psql \copy` and a
psycopg / `tokio-postgres` `copy()` load a file straight into a table.

```sql
COPY account FROM STDIN;                       -- text: TAB-delimited, \N = NULL
COPY account (id, balance) FROM STDIN WITH (FORMAT csv, HEADER);
-- the data rows stream over the wire, then: COPY 3
```

- **Formats.** `text` (default — TAB delimiter, backslash escapes, `\N` for NULL)
  and `csv` (comma delimiter, `"`-quoted fields, doubled-quote escape, empty
  unquoted field = NULL). `WITH (…)` overrides `DELIMITER`, `NULL`, `QUOTE`,
  `ESCAPE`, `HEADER`; the legacy `CSV [HEADER]` form is also accepted. Defaults
  match Postgres exactly.
- **Column mapping.** Positional by default (a row must carry one field per
  column); an explicit `(col, …)` list maps each field to its named column, and an
  omitted value column loads as `NULL` (the business key may not be omitted or
  `NULL`). Each field folds through the **same per-type codec** an `INSERT`
  literal does, so a `COPY`-loaded value is byte-identical to the inserted one.
- **One atomic group.** Every row binds, then the whole load applies as a single
  crash-atomic group (the STL-192 group commit): a parse failure on row *k* — a bad
  value, wrong field count, or `NULL` key — aborts the entire `COPY` and leaves
  **zero** rows (the STL-216 abort posture), reported as `22P02`. Inside a
  `BEGIN … COMMIT` block the rows buffer like any other write — a `SELECT` in the
  same transaction sees them (read-your-own-writes, STL-203) — and commit with the
  transaction.
- **Command tag.** `COPY n` counts the loaded rows. A client `CopyFail` aborts with
  `57014`, leaving zero rows.
- **Out of scope (clear errors, never a wrong load).** `COPY … TO` (export), a
  file/program endpoint (`COPY … FROM '/path'`), binary format, and `COPY` into a
  valid-time table — each a `0A000` (`feature_not_supported`). `COPY` carrying its
  own valid-time interval is a follow-up.

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
- `WHEN NOT MATCHED BY SOURCE`, `WHEN MATCHED THEN DELETE`, clause predicates
  (`WHEN … AND <expr>`), and `OUTPUT` are rejected.

### Valid-time historization (STL-235)

On a table with a valid axis (`… VALID TIME (vf, vt)`) `MERGE` is the
historization workhorse: the arms carry the period columns exactly as a plain
`INSERT` / `UPDATE` does (STL-194), and each arm's bounds are lifted into the
`[from, to)` interval the new version asserts.

```sql
MERGE INTO acct USING (VALUES (1, 200), (3, 300)) AS s (id, balance)
ON acct.id = s.id
WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = now()        -- close prior, open [now, +∞)
WHEN NOT MATCHED THEN INSERT (id, balance, vf) VALUES (s.id, s.balance, now());
```

- A **matched** row gets the joint system+valid **close/open** (STL-166): the
  prior version is closed on the system axis and a new one opens carrying the
  matched arm's interval. An **unmatched** row inserts with the not-matched arm's
  interval. The two arms may name different intervals.
- The period bounds fold as **instants** — an integer microsecond value, `now()`,
  or `now() ± interval` (not civil-time literals), the same surface as a plain
  valid-time write. The start (`vf`) is mandatory; the end (`vt`) defaults to an
  open period when omitted.
- The bound must be a **statement-level instant**, not a source column: a
  per-source-row valid interval (`… vf = s.valid_from`) is rejected for now (a
  deferred follow-up), so the close/open instant is fixed at bind.
- No auto-coalescing (assumption A40): facts are stored exactly as asserted, and
  the 2-D `(system × valid)` tiling holds — at most one live version per key at
  any `(sys, valid)` point, with deletion gaps only where a `DELETE` intends one
  ([16 §5](16-bitemporal-semantics.md#5-the-2d-tiling-invariant),
  [§9](16-bitemporal-semantics.md#9-coalescing-a-documented-choice)).

## Provenance pseudo-columns (STL-247)

Every stored version carries its **provenance** inline — who/what/when wrote it
(architecture [§8](02-architecture.md#8-lineage--provenance-subsystem),
invariant 5). Three **pseudo-columns** read that provenance inline in a `SELECT`,
the way Postgres exposes system columns like `xmin` and `ctid`:

| Pseudo-column          | Type          | Value (from the version's [`Provenance`]) |
|------------------------|---------------|-------------------------------------------|
| `_stele_txn_id`        | `int8`        | the writing transaction's id (the `u64` carried as its `i64` bit pattern) |
| `_stele_committed_at`  | `timestamptz` | the commit instant — the version's `sys_from` |
| `_stele_principal`     | `text`        | the writing identity |

```sql
SELECT id, balance, _stele_txn_id, _stele_committed_at, _stele_principal
  FROM account;
SELECT id FROM account WHERE _stele_txn_id = 42;             -- usable in WHERE
SELECT id, _stele_txn_id FROM account FOR SYSTEM_TIME AS OF 1700000000000000;
```

Semantics, pinned:

- **Hidden, like Postgres system columns.** A pseudo-column is reachable only
  when **named explicitly** — it is not part of `SELECT *`, the `\d` shim, or any
  table's declared schema. A read resolves a projected/`WHERE` name against the
  table's own columns first, so a (discouraged) user column of the same name
  shadows the pseudo-column rather than colliding.
- **The value is the version's own provenance, at any read shape.** A plain read
  returns the live version's; a `FOR SYSTEM_TIME AS OF` read returns the version
  live *then* — its **original** writing transaction and commit instant, not the
  latest — because provenance is immutable on the version. The values come from
  the version metadata / commit log, never a user column. They ride the same
  `SnapshotScan` as the data, so a future range read (`STL-244`) inherits them
  with no extra work.
- **`_stele_principal` value.** The engine stamps the placeholder identity
  `stele` on every wire-issued write today. Threading the connection's
  startup-message user (and then the SCRAM-verified user — `STL-252`) into the
  stored principal **upgrades its trustworthiness without changing this surface**
  and is tracked separately; until then the column honestly reports the
  server-stamped writing principal.
- **Read-your-own-writes.** Inside a `BEGIN` block, a row a statement *buffered*
  but has not committed has no commit provenance yet, so its three pseudo-column
  cells read `NULL`; committed rows read their stored provenance as usual.
- **Out of scope** (deferred, not silently dropped): derivation lineage (the
  v0.6 graph, assumption A17) and a `_stele_statement` column — a `_stele_*`
  name other than the three above is an `UnknownColumn`, never a silent
  pass-through. Aggregating or `GROUP BY`-ing a pseudo-column, and projecting one
  through a `JOIN`, are not bound (single-table provenance only).

[`Provenance`]: ../crates/stele-common/src/provenance.rs

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
│   ├── Admin(AdminCommand)              // CHECKPOINT | FLUSH | COMPACT | BACKUP TO '<path>' — no sqlparser grammar
│   └── User(UserDdl)                    // CREATE | ALTER | DROP USER (STL-252)
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
standard-SQL body, or `None` for an admin command or user DDL — the seam the
binders and the wire layer read so a lifted statement cleanly classifies as
"none of the SQL routes".

## Admin commands (STL-219, STL-231, STL-249)

Operator-facing storage commands. `sqlparser` has no grammar for them,
so they are recognized at the token level — the same lift the temporal clauses use
— and represented as a `StatementBody::Admin` body rather than a `sqlparser` node.
All but `BACKUP` take no arguments; a trailing token (or, for `BACKUP`, a missing
`TO '<path>'`) is an error, never a silent strip. The engine routes each to the
matching session-wide operation and replies with the command's own
`CommandComplete` tag.

| Command | Engine op | Effect |
|---|---|---|
| `CHECKPOINT` | `SessionEngine::checkpoint` | Lightweight WAL fence over every table — fsync + record the fence, no seal. |
| `FLUSH` | `SessionEngine::flush` | Seal every table's delta into a segment and advance its replay floor (bounded recovery — STL-177 / STL-195). |
| `COMPACT` | `SessionEngine::compact` | Flush, then merge every table's sealed segments into one read-optimized segment, retiring the inputs — history-preserving (STL-231, ADR-0030). |
| `BACKUP TO '<path>'` | `SessionEngine::backup` | Fence (flush + checkpoint), then copy the immutable set + a manifest into the local directory `<path>` — a consistent, online full backup (STL-249, ADR-0032). Restore with the `stele restore` CLI verb. |

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
