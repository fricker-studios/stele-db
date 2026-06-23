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

**Over a `JOIN` (STL-243).** A `FOR … AS OF` on either axis applies to a join too —
a two-table join or an N-way left-deep chain (STL-323) — and the rule is one
**consistent `(sys, valid)` snapshot across the whole query**: every input is read
at the *same* pinned point
([docs/16 §8](16-bitemporal-semantics.md#8-temporal-joins)). v0.3 supports the
**statement-level** form (`SELECT … FROM a JOIN b … FOR SYSTEM_TIME AS OF s
[FOR VALID_TIME AS OF v]`), which applies to all inputs; this is the floor. At most
**one qualifier per axis per statement** is allowed: every `FOR … AS OF` is lifted
off the token stream regardless of placement, so the SQL:2011 *per-table* spelling
(`FROM a FOR SYSTEM_TIME AS OF s JOIN b …`) is just syntactic sugar for that single
statement-level pin. Writing the qualifier on *both* inputs is rejected
(`SelectError::MultipleAsOf`) **even when the two name the same instant** — the
binder rejects a repeated axis by count, not by value — so joining inputs at
distinct points (`a FOR SYSTEM_TIME AS OF s1 JOIN b FOR SYSTEM_TIME AS OF s2`) is
out of scope a fortiori. A `FOR VALID_TIME AS OF` pin requires **every** input to
have a valid axis; a system-only side (a plain table, or a CTE / derived table) is
rejected (`ValidTimeUnsupported`), since the pin cannot travel an axis that side
lacks. Inner / left / semi / anti joins are covered, and the pinned snapshot
composes with a `WHERE` / aggregates / `ORDER BY` / `LIMIT` / `DISTINCT` over the
join output (STL-264) and across an N-way chain (STL-323, see
[Joins](#joins-stl-172-stl-264-stl-323)); `RIGHT` / `FULL` / non-equi joins
(STL-270) remain follow-ups.

A `FOR { SYSTEM_TIME | VALID_TIME } { FROM a TO b | BETWEEN a AND b }` **range**
over a join (STL-344, STL-348) is the interval generalization of that pinned read —
"the history of the joined result over the queried window" (`[a, b)` for the
half-open `FROM..TO`, `[a, b]` for the closed `BETWEEN`, the same boundary contract as
the single-table range below). Instead of one version per input at a point, every
input is range-scanned and the join shape decides how the matched intervals combine
(docs/16 §8's pointwise rule lifted to an interval): an **`INNER`** step
**intersects** them — a joined tuple's period is `[max(from), min(to))`, a pair whose
intervals never overlap does not join — while a **`LEFT` / `SEMI` / `ANTI`** step
takes the **interval difference** of a left version's period against its matched
right sub-intervals (STL-348). A `LEFT` join keeps the intersected matched rows and
`NULL`-extends each maximal *gap* the left row was live with no overlapping match;
`SEMI` keeps the left row over the sub-intervals it matched, `ANTI` over the gaps —
so a match inside a left version's window **fragments** it into several output rows.
The period endpoints (`sys_from` / `sys_to`, or `valid_from` / `valid_to`) are
exposed on the join output exactly as a single-table range exposes them
(STL-244/STL-328/STL-329). The whole left-deep chain is supported across mixed join
shapes, including one whose input is a CTE / derived table: a materialized side has no
axis to range, so it is read once at the statement snapshot and treated as a degenerate
`[−∞, +∞)`-live relation (STL-349) — the identity for the interval intersection, so a
joined tuple's period comes from the ranged base sides (at least one input must be a
base table, else the range has no axis to range and is rejected). A materialized side
is admitted only under the `INNER` shape; a CTE / derived input in a `LEFT` / `SEMI` /
`ANTI` range join (interval difference over an unbounded period) remains a follow-up.
The differential oracle
(`crates/stele-engine/tests/bitemporal_join_range_oracle.rs`) checks the range join
against joining the inputs' single-table range reads — an independent breakpoint-sweep
reference for the difference — both axes, across the flush/seal boundary, with a
CTE-side workload covering the materialized input.

### `FOR SYSTEM_TIME { FROM a TO b | BETWEEN a AND b }` — temporal range scans (STL-244)

```sql
-- Every version of every row whose system interval overlaps [t1, t2)
-- (here t1 and t2 are one hour apart, both in microseconds):
SELECT * FROM account FOR SYSTEM_TIME FROM 1700000000000000 TO 1700003600000000;
-- The closed form: overlap with [t1, t2] (upper bound inclusive):
SELECT id, balance FROM account FOR SYSTEM_TIME BETWEEN 1700000000000000 AND 1700003600000000
  WHERE id = 1;
```

Where `AS OF` reads the **one** version live at a point, a **range** read returns
**all** versions whose system-time interval `[sys_from, sys_to)` *overlaps* the
range — the "show me the history" query shape. Lifted off the token stream like
`AS OF` (`sqlparser` has no grammar for it), captured as `Temporal::range`, and
bound into `BoundSelect::system_range`.

**Half-open vs closed (the boundary contract).** Both endpoints fold the same way
an `AS OF` operand does (`now()`, `now() ± interval`, an integer-microsecond
literal). The two spellings differ **only** on the upper bound, per the canonical
half-open µs model ([docs/16 §2](16-bitemporal-semantics.md#2-intervals)):

| Spelling | Query range | A version `[vf, vt)` is returned iff |
|---|---|---|
| `FROM a TO b` | `[a, b)` (half-open) | `vf < vt` and `vt > a` and `vf < b` |
| `BETWEEN a AND b` | `[a, b]` (closed) | `vf < vt` and `vt > a` and `vf ≤ b` |

A degenerate same-tick version (`vf == vt`) covers no instant and is never
returned. `FROM a TO b` requires `a < b`; `BETWEEN a AND b` requires `a ≤ b`
(`a == b` is the single instant `a`); an empty or reversed range is a bind error,
mirroring the §2 reversed/zero-length rejection. The half-open vs closed `<`/`≤`
difference is the "off-by-one on a half-open interval" bug class the §4 oracle
(`crates/stele-engine/tests/system_range_oracle.rs`) pins against a reference
model across the flush/seal boundary.

**Output shape (the projection contract).** A range read's output **schema** is the
table's own columns followed by the appended period endpoints **`sys_from`** and
**`sys_to`** (both `TIMESTAMPTZ`) — so `SELECT *` is `[user columns…, sys_from,
sys_to]`. The endpoints are *addressable* columns ([STL-329]): a named projection
returns exactly what it lists (`SELECT a` is `[a]`; `SELECT a, sys_from` is
`[a, sys_from]`), and they are nameable in the projection, `ORDER BY`, and a value
`WHERE` like any column. `sys_to` is `NULL` for a still-current (open) version. This
is the row shape STL-199's `\history` consumes (it names the endpoints explicitly).

**Composing the SELECT surface ([STL-329], [STL-345]).** The rest of the read
surface composes over a range, routed through the shared `finish_select` pipeline
exactly as a point read is: result-shaping (`DISTINCT` / `ORDER BY` — including on
`sys_from` / `sys_to` — / `LIMIT` / `OFFSET`), aggregation / `GROUP BY` over the
range output, the provenance pseudo-columns ([STL-247]) projected from the range,
and a period-predicate `WHERE` ([STL-345]) — `WHERE PERIOD(a, b) <pred> PERIOD(c, d)`
folds its constant case to keep-all / keep-none and per-row-evaluates an endpoint
that names a value column ([STL-165] / [STL-193]), over each reconstructed range
row. (Grouping on the endpoints is bounded by the same rule as any read: `GROUP BY`
keys are restricted to `INT4` / `INT8` / `BOOL` / `TEXT` today, so `GROUP BY
sys_from` — `TIMESTAMPTZ` — is the pre-existing unsupported-grouping-type error, not
range-specific.) A range scan is now subject to the simple-query default row cap
too, like any plain read ([below](#default-row-cap-on-the-simple-query-path-stl-306)).

**Over a `JOIN` ([STL-344], [STL-348]).** A range qualifies a join too — the "history
of the joined result over an interval". Each input is range-scanned and the join shape
decides how the matched intervals combine. An **`INNER`** step **intersects** them: a
joined tuple's period is `[max(from), min(to))` over its inputs (docs/16 §8's point
rule lifted to an interval), so a pair whose intervals don't overlap never joins, and
the **intersected** endpoints (`sys_from` / `sys_to`, unclipped — the tuple's actual
period) are exposed on the output. A **`LEFT` / `SEMI` / `ANTI`** step takes the
**interval difference** of a left version's period against its matched right
sub-intervals ([STL-348]): `LEFT` emits the matched rows and `NULL`-extends each gap
the left row was live with no overlapping match, `SEMI` keeps the left row over the
sub-intervals it matched, `ANTI` over the gaps — so a match inside a left version's
window **fragments** it into several output rows. The whole left-deep chain is
supported across mixed join shapes (`a JOIN b … LEFT JOIN c …`); the business-key
match, NULL handling, and the `WHERE` / aggregate / `ORDER BY` / `LIMIT` tail are the
same as the point join's ([STL-264]). The un-ranged axis follows the single-table
convention (a system range is valid-agnostic; a valid range pins the system snapshot
and ranges the valid axis). See **Over a `JOIN`** in
[time-travel select](#for--system_time--valid_time--as-of-expr--time-travel-select).

**Still rejected (tracked follow-ups), never silently dropped.** Combining a system
range with **any** `AS OF` point qualifier; a `JOIN` whose input is a CTE / derived
table (no axis to range, [STL-349] — a base-table `JOIN` of any shape is supported
above); a subquery `WHERE`; a computed / scalar-subquery select item; a CTE /
derived-table source; the read-your-own-writes overlay (a range reads the committed
snapshot only, unlike a point read or a join, [STL-325]); and `FOR SYSTEM_TIME ALL`
(the trivially-full range). (The **valid axis** is the parallel form below
([STL-328](#for-valid_time--from-a-to-b--between-a-and-b---valid-time-range-scans-stl-328)),
which relaxes the `AS OF` rule.)

### `FOR VALID_TIME { FROM a TO b | BETWEEN a AND b }` — valid-time range scans (STL-328)

```sql
-- Every version whose valid interval overlaps the business window [a, b),
-- as understood at the current system time:
SELECT * FROM booking FOR VALID_TIME FROM 1700000000000000 TO 1700003600000000;
-- Pinned to a past system snapshot — the history over a valid window *as we
-- believed it as of* a past instant (`v(k, S_past, V_range)`):
SELECT id, room FROM booking
  FOR SYSTEM_TIME AS OF 1699990000000000 FOR VALID_TIME BETWEEN 1700000000000000 AND 1700003600000000
  WHERE id = 1;
```

The valid-axis mirror of the system range above ([STL-244](#for-system_time--from-a-to-b--between-a-and-b---temporal-range-scans-stl-244)),
bound into `BoundSelect::valid_range`. Where the system range ranges over the
*recording* axis at every system instant, the valid range fixes **one** system
snapshot (now, or a `FOR SYSTEM_TIME AS OF` pin) and returns every version
system-live there whose valid interval `[valid_from, valid_to)` *overlaps* the
window — "our current (or as-of-past) understanding of the business history over
`[a, b)`". Requires a valid-time table (`ValidTimeUnsupported` otherwise).

**Half-open vs closed** is identical to the system axis — same `<`/`≤` upper-bound
contract, same empty/reversed bind error — applied to the version's
`[valid_from, valid_to)`:

| Spelling | Query range | A version `[vf, vt)` is returned iff |
|---|---|---|
| `FROM a TO b` | `[a, b)` (half-open) | `vf < vt` and `vt > a` and `vf < b` |
| `BETWEEN a AND b` | `[a, b]` (closed) | `vf < vt` and `vt > a` and `vf ≤ b` |

**Output shape.** A valid range's output schema appends the period endpoints
**`valid_from`** and **`valid_to`** (both `TIMESTAMPTZ`) to the table's columns — so
`SELECT *` is `[user columns…, valid_from, valid_to]`, and the endpoints are
addressable by name just like the system axis ([STL-329]). `valid_to` is `NULL` for
an open-ended ("until changed", `+∞`) fact, mirroring how `sys_to` renders for an
open system period.

**As-of composition.** A `FOR SYSTEM_TIME AS OF s` pin is *allowed* with a valid
range — it fixes the system snapshot the valid history is read at (the cross-axis
`v(k, S, V_range)`, the range-extension of the both-axes point read). A
`FOR VALID_TIME AS OF` is rejected (a point and a range on the *same* axis). The
same SELECT surface composes over a valid range as a system one ([STL-329],
[STL-345]): result-shaping, aggregation / `GROUP BY`, projected provenance
pseudo-columns, and a period-predicate `WHERE` — a per-row `WHERE PERIOD(vf, vt)
<pred> PERIOD(c, d)` over the version's own valid columns is the natural valid-axis
shape ([STL-345]); the same shapes a system range rejects (subquery `WHERE`, `JOIN`,
CTE, computed projection, read-your-own-writes) are rejected here too. The
correctness oracle (`crates/stele-engine/tests/valid_range_oracle.rs`,
[docs/06 §4](06-testing-strategy.md#4-correctness-oracles-the-temporal-heart))
sweeps a bitemporal workload across both axes and the flush/seal boundary against a
reference model, with the same off-by-one teeth check.

### `SET stele.{system,valid}_time` — session time context (STL-246)

```sql
SET stele.system_time = now() - interval '1 hour';   -- pin the whole session
SELECT balance FROM account WHERE id = 1;            -- reads as of an hour ago
RESET stele.system_time;                              -- back to live
```

A connection can pin its read snapshot on either axis so **every** subsequent bare
`SELECT` reads "as of" that instant, without repeating `FOR … AS OF` on each query.

| Statement | Effect |
|---|---|
| `SET stele.system_time = <expr>` | Pin the system axis to the instant `<expr>` resolves to. |
| `SET stele.valid_time = <expr>`  | Pin the valid axis (only meaningful for a valid-time table). |
| `RESET stele.system_time` / `RESET stele.valid_time` | Clear one axis — live reads on it again. |
| `RESET ALL` | Clear both axes. |
| any other `SET`/`RESET` | A tolerated no-op (see below). |

* **`<expr>` accepts the same expression *shapes* as a `FOR … AS OF` operand** —
  `now()`, an integer microsecond instant, `now() ± interval '…'`, plus the alias
  `'now'` for `now()`. Unlike an `AS OF` operand (whose `now()` folds to the
  reading statement/transaction), the `SET` value is **evaluated once, at the
  moment of the `SET`**: `now()` is that statement's instant (the server clock
  observed fresh, **not** an open transaction's `BEGIN` snapshot), and the pin
  holds that fixed instant until changed or `RESET` — it is not a moving target.
* **Equivalence (the oracle).** A session-pinned read returns byte-for-byte what
  the explicit form returns: the server applies the pin by replaying it as an
  explicit `FOR <dim> AS OF <instant>` qualifier on each bare single-table
  `SELECT`. So `SET stele.system_time = X; SELECT … FROM t` ≡ `SELECT … FROM t FOR
  SYSTEM_TIME AS OF X`. An **explicit** `FOR … AS OF` in a statement overrides the
  session pin for that axis (the statement's own qualifier always wins).
* **Scope.** This is **session** state (per connection), not transactional: a pin
  set inside a `BEGIN` block survives a `ROLLBACK`, matching Postgres `SET` (the
  transactional `SET LOCAL` is not supported). Inside a transaction a pin makes the
  block's reads time-travel exactly as an explicit `AS OF` does there — so under a
  system pin, read-your-own-writes is suppressed (a past read shows only committed
  history), while a valid pin keeps it ([STL-203], [STL-223]).
* **Over a `JOIN` (STL-325).** The pin applies over a two-table join too, per axis by
  applicability: the **system** pin is always injected (the system axis is always
  present), and the **valid** pin is injected only when *every* input has a valid
  axis — the same check an explicit `FOR VALID_TIME AS OF` over a join makes
  ([STL-243]). When an input is system-only the valid pin is **silently withheld**
  (the join reads live on the valid axis), never injected into a `ValidTimeUnsupported`
  error — so a session valid pin can never break a working join (or a system-only
  single-table read). Read-your-own-writes threads through the join the same way it
  does a single-table read: an in-transaction join overlays the transaction's own
  buffered writes on each side ([STL-203], [STL-223]).
* **Tolerant `SET`.** Every variable other than the two `stele.*` time variables is
  accepted as a **no-op** (reported `SET`/`RESET`). This lets a stock Postgres
  driver's connect-time preamble — pgjdbc's `extra_float_digits` /
  `application_name`, etc. — succeed without a workaround ([STL-184]); only the two
  Stele time variables carry behavior.

Recognized at the token level (a `StatementBody::Session`), like the admin commands
— Stele owns the `SET` surface rather than handing it to `sqlparser`.

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
- **Beyond the single-table scan**: an equi-join binds (STL-172) — two-table or an
  N-way left-deep chain (STL-323) — and composes with the rest of the `SELECT`
  surface — `WHERE`, aggregates / `GROUP BY`, and the `DISTINCT` / `ORDER BY` /
  `LIMIT` tail over the join output, with qualified-name resolution across every
  input (STL-264, see [Joins](#joins-stl-172-stl-264-stl-323)) — a
  `WHERE` subquery binds — uncorrelated (STL-234) and correlated (STL-239), see
  [Subquery predicates](#subquery-predicates-stl-234-stl-239) — and a `WITH` list
  of non-recursive CTEs plus `FROM (SELECT …)` derived tables binds (STL-242, see
  [CTEs and derived tables](#ctes-and-derived-tables-stl-242)). The select list
  projects computed expressions and a scalar subquery, not just `*` / bare columns
  (STL-303, see [Expression select items](#expression-select-items-stl-303)).
  **Rejected**: set operations and schema-qualified table names. The `WHERE` clause
  stays on the AST for the executor-glue layer (pgwire, STL-104) to lower.

### Expression select items (STL-303)

Beyond `*` and bare column names, the select list projects a **computed
expression** and a **scalar subquery** (uncorrelated or correlated), each optionally
`AS`-aliased. `bind_select` lowers the list to projection items the executor evaluates
per row; `SELECT *` stays the all-columns fast path:

```sql
SELECT a + b, 1 + 2, a + (SELECT max(b) FROM s), 7 AS seven FROM t
```

- **Bare column** — `a`, optionally `AS x`. The output name is the alias, else the
  column's own name. A provenance pseudo-column (STL-247) is projectable here on a
  base table.
- **Computed expression** — the `WHERE` scalar vocabulary (STL-213, STL-332):
  integer arithmetic over **any number of columns** (`a + b`, `k * a - 1`), with
  folded literals (`a + 1`), **column-free** arithmetic (`1 + 2`), and an embedded
  uncorrelated **scalar subquery** operand (`a + (SELECT max(b) FROM s)`, resolved
  once and substituted as a constant). Each column / subquery types itself
  independently; Stele does not implicitly coerce, so an arithmetic node's two
  operands must share one integer type — an `int4` *literal* widens to meet an
  `int8` operand, but two concrete operands of different types are rejected. A
  column-free literal is `1` → `int4`, `'x'` → `text`, `TRUE` → `bool`. NULL
  propagates through arithmetic (3VL), exactly as in a `WHERE`; an embedded subquery
  resolving to no row makes the whole expression NULL, and one returning more than
  one row is SQLSTATE `21000`. An unaliased computed expression takes the Postgres
  `?column?` fallback name. A provenance pseudo-column is **not** usable inside a
  computed expression (the evaluator decodes schema columns only) — it is projectable
  solely as a bare column.
- **Scalar subquery** — `(SELECT … )`. An **uncorrelated** one (STL-303) is resolved
  **once** at the statement snapshot (the STL-234 fold, materialising a value instead
  of a row filter) and broadcast as a constant column; a **correlated** one (STL-331,
  the inner references an outer column) is re-run once per outer row with that row's
  value substituted — the STL-239 per-row machinery producing a projected cell. Either
  way: no inner row ⇒ SQL `NULL`; more than one inner row ⇒ SQLSTATE `21000`
  (`cardinality_violation`) — for an uncorrelated inner raised even when the outer
  produces no rows, for a correlated inner per offending outer row. A NULL correlation
  value short-circuits to `NULL` without a run. An unaliased subquery inherits the
  inner's sole output column name. The inner inherits the outer's `(sys, valid)`
  snapshot (docs/16 §6), so every re-execution of a correlated inner reads that one
  consistent snapshot.
- `ORDER BY` and `DISTINCT` resolve over the projected output columns — a computed
  alias sorts/deduplicates by the expression's value.
- **Evaluation timing is eager** (STL-341). Every projection item is evaluated for
  **every qualifying row** (each row that survives `WHERE`), and only then does the
  result-shaping pipeline ([Result shaping](#result-shaping-stl-263-stl-265)) apply
  `DISTINCT` / `ORDER BY` / `OFFSET` / `LIMIT` over the materialized columns (eager
  materialize → shape → gather). A computed expression, an uncorrelated scalar
  subquery, and a correlated scalar subquery are therefore all evaluated even for a
  row that `LIMIT` / `OFFSET` ultimately discards. The **observable** divergence
  from Postgres follows, in its clearest shape: a malformed **correlated** scalar — one
  returning more than one inner row for some outer row — behind a `LIMIT` / `OFFSET` with **no `ORDER BY`**
  raises SQLSTATE `21000` (`cardinality_violation`) for that row even when it lies
  outside the window, where Postgres evaluates a SELECT-list SubPlan **lazily** (only
  for the rows it emits) and returns the windowed rows without ever touching it. The
  posture is **intentional** and fail-safe — it can only ever substitute an *error* for
  rows, never wrong data, and only for a malformed query. The divergence is widest under
  a bare `LIMIT` / `OFFSET`. A `DISTINCT` removes it — deduplicating the projected row
  must compute every value — as does an `ORDER BY` **on the computed / subquery item
  itself**, since a sort key is evaluated for every input row. An `ORDER BY` on some
  *other* column need not: Postgres may take the ordering from an index and stop at the
  window, never evaluating the offending item, so the same `21000` can still surface
  there. The rule is uniform across the three projection paths — the uncorrelated InitPlan
  case (STL-303) raises `21000` even when the outer produces no rows, and STL-331 only
  extended the same eager evaluation to the correlated case.

**Rejected** (each a tracked follow-up): a scalar **function** call other than the
constant-folded `hash(...)`; a *correlated* subquery embedded inside an expression
(an uncorrelated embedded subquery binds — STL-332).

## Result shaping (STL-263, STL-265)

`HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`/`FETCH`, and `DISTINCT` bind and execute on
the single-table `SELECT` path — plain and aggregate reads, under `AS OF` on either
axis, and inside transactions (the read-your-own-writes overlay is shaped like
committed rows). The executor applies them in the Postgres pipeline order:

```text
WHERE → [GROUP BY/aggregates → HAVING] → DISTINCT → ORDER BY → OFFSET → LIMIT
```

- **`HAVING <predicate>`** (STL-265, richer predicates STL-327) — the
  post-aggregation filter, applied after the `GROUP BY` folds and before the
  shaping tail: a group is kept iff the predicate is **`TRUE`** for it (a `FALSE`
  or `NULL` group drops, the `WHERE` rule). One of the six comparisons, each side a
  grouping column, an aggregate call, a literal, or an integer arithmetic of one.
  **Both sides may anchor** (`COUNT(*) > SUM(amount)`, `dept > COUNT(*)` — the
  aggregate-to-aggregate / column-to-aggregate analog of a column-to-column
  `WHERE`), comparing across the numeric types the evaluator promotes to a common
  type, and a **`FLOAT8` `AVG`** operand compares against a literal or another
  aggregate (STL-327). An aggregate the `HAVING` names but the select list does not
  (`SELECT region … HAVING SUM(amount) > 100`) is computed and filtered on without
  being emitted. A bare column that is not a grouping column is Postgres's **42803**
  (`grouping_error`), the same code a non-aggregated select item draws. `HAVING`
  composes over a **join** too, resolving its operands through the join scope
  (STL-327). **Rejected** with the reason: a non-comparison or boolean-connective
  `HAVING`, a comparison of two incomparable types (a text grouping column against a
  numeric aggregate), `FLOAT8` arithmetic (the evaluator has no float arithmetic
  kernel), and a subquery inside `HAVING` (rides the subquery tickets).
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

Result shaping composes over a **join** read too (STL-264): the same pipeline runs
over the join's output rows — see [Joins](#joins-stl-172-stl-264-stl-323). Top-N pushdown
and sort spill are performance work, deliberately out of v0.3 scope.

### Default row cap on the simple-query path (STL-306)

A `SELECT` that names **no `LIMIT`** is unbounded — `SELECT * FROM t` reads the
whole table. Over the **simple** query protocol (`psql`, the `stele` shell, any
ad-hoc tool) the client consumes the entire result in one shot, so an accidental
whole-table read floods the terminal and the client's memory — observably
hanging the shell. The wire front end therefore treats an unbounded **plain
single-table** `SELECT` (no finite `LIMIT`/`FETCH` count) as an implicit
**`LIMIT 1000`**, injected before binding so it rides the normal result-shaping
path above. Any `OFFSET` the caller gave is preserved, and a **subquery is never
touched** (capping a `WHERE … IN (SELECT …)` would change the result, not just
truncate it). An explicit `LIMIT n` — including one above the cap — always wins;
`LIMIT ALL` reads as unbounded and is capped like a bare read.

The cap is injected only on the **plain single-table** read path. A **`JOIN`**
(which now accepts an explicit `LIMIT`/`ORDER BY`/`DISTINCT`, STL-264, but is not
auto-capped), a **table-valued function** (`stele_history`/`stele_audit`/
`stele_segments` introspection), a **set operation**, an `INSERT … SELECT` source,
and a constant `SELECT` are all left intact, so the cap never turns a working
statement into an error.

The **extended** query protocol is exempt: a driver (JDBC, psycopg, pgAdmin)
sets its own row count through the portal's `Execute` `max_rows`, so an
automated consumer fetches exactly what it requested.

## Joins (STL-172, STL-264, STL-323)

An equi-join binds and runs (STL-172): `INNER` / `JOIN`, `LEFT [OUTER]`, and
`LEFT SEMI` / `LEFT ANTI`, joined on a single `ON left.col = right.col` equality
(either operand order; the two key columns must share a type). Either side may be a
base table, a CTE, or a `FROM (SELECT …)` derived table. **N-way joins** chain these
two-table joins **left-deep** (STL-323): `a JOIN b ON … JOIN c ON …` materializes
`a ⋈ b`, then joins that output against `c`, and so on in syntactic order (no join
reordering / cost-based planning). Each step's `ON` may relate the freshly joined
input to *any* already-joined input — `c.x = a.y` is as valid as `c.x = b.y`.

```sql
SELECT u.name, p.label, count(*) AS n
FROM users u JOIN orders o ON u.id = o.uid JOIN products p ON o.pid = p.pid
WHERE o.total >= 100
GROUP BY u.name, p.label
ORDER BY n DESC
LIMIT 10;
```

The join's **output is a relation like any other**, and the rest of the `SELECT`
surface composes over it (STL-264), in the Postgres pipeline order
(`WHERE → [GROUP BY/aggregates → HAVING] → DISTINCT → ORDER BY → OFFSET → LIMIT`):

- **Addressable columns.** The output is the seed input's columns, then each
  `INNER` / `LEFT` step's right input's, in the chain's left-deep order (a `SEMI` /
  `ANTI` step keeps only the accumulated left, so its right input is not
  addressable). A column reference — in the projection, `WHERE`, `GROUP BY`, or
  `ORDER BY` — is a bare name (resolved if it is unique across the output) or a
  qualified `table.col` / `alias.col`; an ambiguous bare name is an error
  (`42702`-style), as Postgres requires.
- **`WHERE`** over the joined columns, including a column the projection does not
  select, applied after the join. The same single-comparison shape the single-table
  path binds (six operators, integer arithmetic of one anchor column — STL-213).
- **Aggregates / `GROUP BY` / `HAVING`** over the join output (STL-171, STL-327):
  `COUNT` / `SUM` / `MIN` / `MAX` / `AVG`, grouped on any output column(s), with an
  optional post-aggregation `HAVING` (the same predicate vocabulary as a
  single-table read, two-anchor and `FLOAT8` operands included) resolving through
  the join scope.
- **`DISTINCT` / `ORDER BY` / `OFFSET` / `LIMIT`** over the join output (STL-263),
  with the same semantics (NULL placement, the `DISTINCT` 42P10 rule) as a
  single-table read.

A join also **time-travels**: a statement-level `FOR … AS OF` on either axis reads
every input at one consistent `(sys, valid)` snapshot (STL-243, see
[Over a `JOIN`](#for--system_time--valid_time--as-of-expr--time-travel-select) and
docs/16 §8); the composed clauses above run over that snapshot's output.

A join is also **read-your-own-writes** consistent (STL-325): inside a transaction
each input's scan — every input in the left-deep chain — is overlaid with the
transaction's own buffered `INSERT` / `UPDATE` / `DELETE` for that table before the
join, so a `SELECT … JOIN …` after a staged write reflects it, exactly as a
single-table read does ([STL-203] / [STL-223]), and `ROLLBACK` discards it. A session
`SET stele.{system,valid}_time` pin also applies over a join (see
[session time context](#set-stelesystemvalid_time--session-time-context-stl-246)).

**Not yet** (rejected, never silently mis-bound): `RIGHT` / `FULL OUTER` and
non-equi `ON` conditions (STL-270); join reordering / cost-based planning (the chain
runs in syntactic, left-deep order); a period predicate over a join. A join read is
also not auto-capped by the
[default row cap](#default-row-cap-on-the-simple-query-path-stl-306) — give it an
explicit `LIMIT`.

## Subquery predicates (STL-234, STL-239)

A `WHERE` may be a single subquery predicate. An **uncorrelated** one (the inner
query references no outer column) is evaluated **once** and its result folded
into the outer row filter; a **correlated** one (the inner references an outer
column, [below](#correlated-subqueries-stl-239)) is re-run per outer row. Three
shapes bind, correlated or not:

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
transaction the inner also sees the read-your-own-writes buffer (STL-203). A
correlated inner re-runs at that **same** snapshot for every outer row, so the
rule holds per re-execution.

The outer operand of a scalar / `IN` comparison must be a bare value column, and
the inner's single column must match its type (no implicit coercion). Bound as
`BoundSelect::subquery_filter` (mutually exclusive with the plain and period
`WHERE` shapes); for an uncorrelated subquery the engine's `resolve_filter` runs
the inner once and folds it into the same `FilterPlan` the plain path produces.

### Correlated subqueries (STL-239)

When the inner's `WHERE` relates one of its columns to an **outer** column, the
subquery is *correlated*: its result depends on the outer row, so the engine
re-runs the inner once per outer row with that row's value substituted, dropping
the row unless its predicate holds. All three shapes correlate:

```sql
SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.k);       -- [NOT] EXISTS
SELECT id FROM t WHERE a IN (SELECT a FROM s WHERE s.k = t.k);         -- [NOT] IN
SELECT id FROM t WHERE a = (SELECT a FROM s WHERE s.id = t.id);        -- scalar lookup
```

- The correlated `WHERE` must be **one comparison** relating a bare inner column
  to an outer column — `inner.c <cmp> outer.c`, either operand order (the engine
  lowers a single-comparison `WHERE`, so the correlation *is* the whole inner
  `WHERE`). The two columns must share a type. A bare name resolves to the inner
  when the inner table has it (the innermost-scope rule); qualify with the table
  name or alias otherwise. An alias **replaces** the table name as the relation's
  exposed name, which is what lets a self-correlation (`FROM t … (SELECT 1 FROM t
  inner WHERE inner.k = t.k)`) name both sides.
- **NULL semantics carry per row.** A NULL outer correlation value makes the inner
  empty (`inner.c = NULL` is unknown for every inner row): `EXISTS` is then false,
  `IN` false, a scalar `NULL`. The `NOT IN`-with-a-NULL-member trap is evaluated
  against *each* outer row's inner set. Both are checked against DuckDB in the
  nightly differential oracle (`correlated_subquery_differential.rs`).
- **Decorrelation (STL-317, STL-337, STL-346).** A correlated `EXISTS` / `NOT
  EXISTS`, `IN`, or `NOT IN`, whose correlation is an **equality** on the key
  (`inner.k = outer.k`) and whose inner is a plain single-table scan is lowered to a
  **hash join** (STL-172's machinery) — one inner scan instead of `O(outer rows)`
  re-executions:
  - **`EXISTS` / `NOT EXISTS`** → a single-key **semi** / **anti** join on the
    correlation key. "∃ an inner row for this outer row" is exactly "the outer key is
    a member of the inner key set", so `SEMI` answers `EXISTS` and `ANTI` `NOT
    EXISTS`; a NULL key never matches, reproducing the per-row rule above (a NULL
    outer key drops under `EXISTS`, survives under `NOT EXISTS`).
  - **`IN`** → a **composite-key semi** join on `(correlation key, membership value)`.
    `t.a IN (SELECT s.a FROM s WHERE s.k = t.k)` keeps a row iff some inner row has
    `s.k = t.k` **and** `s.a = t.a` (a two-key test), so each side's pair is folded
    into one synthetic key that is NULL when *either* component is NULL — reproducing
    `IN`'s three-valued logic (a NULL membership value or correlation key, on either
    side, never makes `IN` true).
  - **`NOT IN`** → a **NULL-aware composite-key anti** join on the same
    `(correlation key, membership value)`. A plain composite anti join is *not* `NOT
    IN`: under 3VL a NULL membership value anywhere in an outer row's correlation
    group makes `NOT IN` unknown for it (drop), yet the anti join keeps it (its
    composite never matches). So the engine layers **per-correlation-group NULL
    tracking** over the anti join — for each correlation key, whether its inner group
    is non-empty and whether it holds a NULL member — and drops the over-kept rows: a
    NULL `t.a` over a non-empty group, or a non-empty group with a NULL member. An
    empty group keeps (`NOT IN ()` is true), as does a NULL correlation key (its group
    is empty).

  The inner still runs at the outer's `(sys, valid)` snapshot and over the
  read-your-own-writes overlay, so the per-statement snapshot rule (docs/16 §6) holds
  across both join inputs. The decorrelated and per-row paths return identical results
  — the nightly DuckDB oracle (`correlated_subquery_differential.rs`) covers both.
- **Still per-row** (the `O(outer rows × inner cost)` fallback the v0.3 bar permits):
  a **non-equality** correlation (`<` / `>` / … — a range, not key-set membership);
  and a correlated **scalar** lookup.

A scalar subquery also binds in the **select list** — **uncorrelated** (STL-303) or
**correlated** (STL-331), see
[Expression select items](#expression-select-items-stl-303).

**Not yet bound** (each a tracked follow-up): a subquery composed with `AND` / `OR`
or set over a join, a correlated `WHERE` with more than the one correlation
comparison, and lateral joins. (`WITH` CTEs and `FROM (SELECT …)` derived tables
*do* bind — see [CTEs and derived tables](#ctes-and-derived-tables-stl-242).)

## CTEs and derived tables (STL-242)

A query may name intermediate results with a `WITH` list and read inline
subqueries in `FROM`. Both lower to the **same** shape — a *materialized
relation*: the defining query runs once at the statement snapshot, its rows are
captured, and a reference reads them like a table.

```sql
-- A named CTE, referenced in the main query.
WITH big AS (SELECT id, a FROM t WHERE a >= 20) SELECT id FROM big WHERE a < 40;

-- Multiple CTEs; a later one may read an earlier one (CTE → CTE chaining).
WITH big AS (SELECT id, a FROM t WHERE a >= 20),
     hi  AS (SELECT id, a FROM big WHERE a >= 30)
SELECT id FROM hi;

-- A CTE joined to a base table (either join side may be a CTE / derived table).
WITH small AS (SELECT id, a FROM t WHERE a <= 30)
SELECT small.id, s.label FROM small JOIN s ON small.id = s.id;

-- A CTE under aggregation.
WITH big AS (SELECT id, a FROM t WHERE a >= 20) SELECT count(*) FROM big;

-- A derived table in FROM (an inline, single-use CTE named by its alias).
SELECT id FROM (SELECT id, a FROM t WHERE a > 15) AS d WHERE a <> 30;

-- A `name(col, …)` list renames the relation's output columns.
WITH c(k, v) AS (SELECT id, a FROM t) SELECT k FROM c WHERE v = 40;
```

- **A reference resolves to a CTE first, then the catalog** — a CTE name shadows a
  base table of the same name (the SQL scoping rule). A `WITH` name introduced
  twice is an error; a later CTE sees every earlier one in the same list.
- **A derived table must have an alias** (`FROM (SELECT …) AS d`); it lowers to a
  single-use CTE named by that alias, so it composes with everything a CTE does —
  including a join side.
- **The body binds against a normal `SELECT` surface** — projection, `WHERE`,
  `GROUP BY`/aggregates, `ORDER BY`/`LIMIT`, and a nested `WITH` — and a CTE /
  derived table feeds the outer query's `WHERE`, aggregate, projection, and joins
  through the very same pipeline a base table does. DuckDB differential
  spot-checks live in the nightly `cte_differential.rs` oracle.
- **Temporal rule:** a CTE evaluates at the statement's snapshot, the same one
  consistent `(sys, valid)` point the rest of the statement and any subquery read
  (docs/16 §6). A query-local relation has no valid-time period, so a
  `FOR VALID_TIME AS OF` over a CTE / derived table is `ValidTimeUnsupported`.
- **Not in scope** (tracked follow-ups): `WITH RECURSIVE` (v0.5) and
  data-modifying `WITH` are rejected; a `LATERAL` derived table is rejected; a
  `WHERE` subquery cannot itself name an enclosing CTE; and a single-relation
  read still addresses a column by its bare name (a qualified `d.col` is bound
  only in a join, as before).

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

### Valid-time historization (STL-235, STL-308)

On a table with a valid axis (`… VALID TIME (vf, vt)`) `MERGE` is the
historization workhorse: the arms carry the period columns exactly as a plain
`INSERT` / `UPDATE` does (STL-194), and each arm's bounds are lifted into the
`[from, to)` interval the new version asserts.

```sql
MERGE INTO acct USING (VALUES (1, 200), (3, 300)) AS s (id, balance)
ON acct.id = s.id
WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = now()        -- close prior, open [now, +∞)
WHEN NOT MATCHED THEN INSERT (id, balance, vf) VALUES (s.id, s.balance, now());

-- STL-308: each source row asserts its own effective window.
MERGE INTO acct USING (VALUES (1, 200, 5, 10), (3, 300, 7, 9)) AS s (id, balance, vfrom, vto)
ON acct.id = s.id
WHEN MATCHED THEN UPDATE SET balance = s.balance, vf = s.vfrom, vt = s.vto
WHEN NOT MATCHED THEN INSERT (id, balance, vf, vt) VALUES (s.id, s.balance, s.vfrom, s.vto);
```

- A **matched** row gets the joint system+valid **close/open** (STL-166): the
  prior version is closed on the system axis and a new one opens carrying the
  matched arm's interval. An **unmatched** row inserts with the not-matched arm's
  interval. The two arms may name different intervals.
- A period bound is either a **statement-level instant** — an integer microsecond
  value, `now()`, or `now() ± interval` (not civil-time literals), the same
  surface as a plain valid-time write, folded at bind so every affected key opens
  the same interval — or a **per-source-row source column** (`vf = s.valid_from`,
  STL-308), so each affected key carries its **own** `[from, to)` interval, the
  natural shape when historizing a batch whose rows each assert a different
  effective date. The start (`vf`) is mandatory; the end (`vt`) defaults to an
  open period when omitted.
- A per-row source bound reconciles to a microsecond instant: a `VALUES` cell is
  an integer (the same convention as a literal bound), a table source's column is
  `TIMESTAMP` / `TIMESTAMPTZ`. The interval is derived per row at execution, so an
  empty/reversed or `NULL` per-row bound is rejected there rather than at bind.
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
- **`_stele_principal` value.** A wire-issued write stamps the **connection's
  identity** (`STL-300`): under `auth = "trust"` the unauthenticated startup-message
  `user`, under `auth = "scram"` the SCRAM-verified user (`STL-252`). The principal
  is set per statement under the same engine lock as the write, so a row records
  *who* wrote it even though connections share one engine. A direct, non-wire writer
  (an embedded `SessionEngine`) defaults to the server identity `stele`. This changed
  the stored *value*, not the surface above — resolution, hiding, and `WHERE`
  usability are unchanged.
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
│   ├── User(UserDdl)                    // CREATE | ALTER | DROP USER (STL-252)
│   └── Session(SessionCommand)          // SET/RESET stele.{system,valid}_time, or a tolerated SET (STL-246)
└── temporal: Temporal
    ├── system_versioning: bool         // WITH SYSTEM VERSIONING
    ├── valid_time: Option<ValidTimePeriod>   // VALID TIME (from, to)
    ├── as_of: Vec<AsOf>                 // FOR <axis> AS OF <expr>
    │   ├── dimension: TimeDimension { System | Valid }
    │   └── timestamp: sqlparser::ast::Expr
    ├── range: Option<TemporalRange>     // FOR <axis> { FROM a TO b | BETWEEN a AND b }
    │   ├── dimension: TimeDimension { System | Valid }
    │   ├── from / to: sqlparser::ast::Expr
    │   └── closed_upper: bool           // BETWEEN (closed) vs FROM..TO (half-open)
    └── period_predicate: Option<PeriodPredicateClause>   // PERIOD(..) <pred> PERIOD(..)
        ├── left:  PeriodExpr { from, to: sqlparser::ast::Expr }
        ├── predicate: PeriodPredicate { Contains | Overlaps | Equals | … }
        └── right: PeriodExpr { from, to: sqlparser::ast::Expr }
```

A statement with no temporal grammar carries `Temporal::default()` (all empty);
`Statement::is_temporal()` reports the difference. `Statement::sql()` returns the
standard-SQL body, or `None` for an admin command, user DDL, or session command —
the seam the binders and the wire layer read so a lifted statement cleanly
classifies as "none of the SQL routes".

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
