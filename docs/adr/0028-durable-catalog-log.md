# ADR-0028 — Durable catalog: a dedicated append-only DDL log at the session level

- **Status:** Accepted
- **Date:** 2026-06-10
- **Deciders:** Project owner + systems design
- **Related:** [02 §5](../02-architecture.md#5-catalog--metadata) · [02 §12](../02-architecture.md#12-cross-cutting-architectural-invariants) (invariant 2) · [ADR-0023](0023-append-only-record-model-validity-index.md) · STL-210 / STL-148 / STL-102 / STL-177

## Context

Committed user data already survives a restart — the WAL is the durability point
(invariant 2) and `Engine::recover` (STL-102, STL-177) rebuilds a *table's* tiers
from its disk deterministically. But the **catalog** (which tables exist, their
versioned schemas at each system time, the valid-time policy, and each table's
on-disk namespace assignment) lives only in `SessionEngine`'s memory. After a
restart the catalog is empty, no tier is reopened, and the surviving WALs are
orphaned: data is durable but unreachable. Recovery cannot even *enumerate* the
tables without durable catalog state.

Constraints: the storage/txn core stays runtime-agnostic and deterministic
(ADR-0010); sealed segments are never mutated (invariant 1); the durability
point must remain a log fsync (invariant 2); DDL is rare relative to DML, so its
durability cost is irrelevant; and time-travel must survive schema evolution, so
the *history* of DDL (each version's `sys_from`/`sys_to`), not just the current
table set, must be recoverable.

Options considered:

- **(a) Piggyback DDL records on a storage WAL.** The WALs are per-table —
  created *under a table's namespace* — so recovery would need to know the
  namespaces to find the log that says which namespaces exist. Breaking the
  cycle would need a distinguished "namespace zero" WAL plus a new record kind
  in the storage WAL format, pushing catalog vocabulary (names, column types,
  temporal policy) into `stele-storage`, which deliberately knows nothing of it.
- **(b) Dog-food the catalog as a bitemporal system table** on the
  sealed-segment substrate — the long-term direction the catalog docs sketch.
  It has the same bootstrap cycle (reading a table needs the catalog) plus a
  bigger one: it depends on flush/compaction maturity the engine does not have
  yet (flush is not even exposed at the session level, STL-195). Premature now.
- **(c) A dedicated append-only catalog log** at the session level: one small
  file on the shared (un-namespaced) disk, one self-checksummed record per DDL
  mutation, fsynced before the DDL is acknowledged. The catalog's own WAL.

## Decision

**We will persist the catalog as a dedicated append-only DDL log —
`stele.catalog` on the session's shared disk — owned by `stele-engine`, and
boot `SessionEngine` through a `recover` path that replays it.**

- **One record per acknowledged DDL mutation**, written *before* the in-memory
  catalog mutation is acknowledged: `CreateTable { at, namespace, name,
  columns, temporal }` and `DropTable { at, name }` at v0.2 (`ALTER` joins when
  it becomes SQL-reachable). Each record is framed `magic | length | payload |
  CRC32C` and **fsynced before the statement returns** — the catalog log fsync
  is the durability point for DDL, the same invariant-2 shape the row WAL gives
  DML. No record is ever rewritten; the log is append-only.
- **Write-ahead with atomic in-memory commit:** the mutation is validated by
  applying it to a *copy* of the catalog, then the record is appended and
  fsynced, then the copy is committed. A failure at any step leaves both the
  log and the live catalog exactly as they were — the log never holds a record
  for a refused statement, and the session never holds state the log missed.
- **Replay rebuilds, deterministically.** `SessionEngine::recover` replays the
  records in order at their recorded `at` instants, which reproduces the exact
  schema-version chains *and* the exact `SchemaId` allocation order; reopens
  every recorded namespace through `Engine::recover` (dropped names included —
  their history must keep answering `AS OF` reads); restores the
  namespace↔table mapping from the records rather than re-deriving it (a
  re-created name reuses its retained namespace, so history is neither
  duplicated nor orphaned); and repositions the commit clock and transaction-id
  allocator past every recovered instant/id.
- **Torn-tail contract, fail-closed on corruption:** a *partial* trailing
  record is tolerated and ignored — its fsync never returned, so the DDL was
  never acknowledged. So is a tail that does not begin with the record magic:
  the framing cannot tell a magic-corrupted record from the zero/garbage fill
  a crashed append leaves, so replay stops there (the magic bytes are the one
  4-byte window where damage is read as a torn tail rather than detected).
  A *complete* record with intact magic whose CRC fails is a hard recovery
  error: it was acknowledged, so serving without it would silently drop a
  table-set change (mirrors `dml::recover_replay`'s fence semantics).
- The log is **authoritative for DDL** (unlike the validity index, which is
  derived): it is the only durable copy of the catalog timeline. It is tiny —
  one short record per DDL statement ever executed — so unbounded growth is a
  non-issue at v0.2; snapshot/compaction rides the future migration to (b).

## Consequences

### Positive
- A restarted server boots from disk: enumerate tables from the log, reopen
  tiers, replay row WALs — closing the v0.1 "tables vanish on restart" gap with
  no change to any storage-tier format.
- DDL durability has the same auditable shape as DML durability: an append-only,
  checksummed log whose fsync is the acknowledgement point (invariant 2).
- Replay reproduces schema history *bitemporally* (each version at its original
  `sys_from`), so `AS OF` reads across restarts resolve old schemas — including
  across drop/re-create — and `SchemaId`s stay stable for future footer lookups.

### Negative / costs
- A second durable-log mechanism beside the row WAL (more code, two fsync
  paths) — accepted because the alternatives couple layers or block on
  not-yet-built machinery.
- The log grows forever (one record per DDL). Harmless at realistic DDL rates,
  but a catalog with heavy programmatic DDL churn would eventually want the
  snapshotting that option (b) brings.
- Catalog durability is per-`SessionEngine`-disk; a future multi-node Stele
  needs the catalog on shared storage (consistent with ADR-0006's CP posture).

### Neutral / follow-ups
- **Hash-chained for tamper-evidence (STL-307, [ADR-0031](0031-live-server-verifiable-commit-log.md)).**
  The CRC framing above catches *accidental* corruption, but a privileged
  operator can recompute it; STL-307 added a per-record SHA-256 `prev_hash` link
  to the catalog log (the same chain shape the commit log uses), verified
  fail-closed on `recover`, so forging catalog history is detectable — invariant
  10 extended to DDL. This is a framing addition (`magic | len | prev_hash |
  payload | crc`); the record vocabulary and replay semantics here are unchanged.
- `ALTER TABLE` (e.g. `add_column`) gets its record kind when it becomes
  SQL-reachable; the framing reserves the kind byte.
- Migrating the catalog onto the sealed-segment substrate (option b) supersedes
  this log when flush/compaction mature; the replay semantics fixed here (apply
  at recorded `at`, identical id allocation) carry over unchanged.
- Session-level flush exposure (STL-195) composes with, and does not change,
  this recovery path: the delta rebuilds from the WAL either way.
