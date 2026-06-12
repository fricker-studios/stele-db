# 02 — Architecture

> **Status:** Founding architecture. Diagrams are normative intent, not committed code.
> **Read with:** [01 — Feature Plan](01-feature-plan.md) (what) · [03 — Roadmap](03-roadmap.md) (when) · [ADR index](adr/README.md) (decisions behind each box).

This document describes Stele's system architecture from the wire down to the bytes (and out to object storage), plus the later distribution plan. Diagrams are [Mermaid](https://mermaid.js.org/) and render on GitHub.

> **Guardrail in architectural form:** the storage engine is **columnar-first with an adequate point-access path**, not a row store with columns bolted on. Every layout decision favors scans, temporal range pruning, and append throughput over single-row write latency. See [Charter §3](00-charter.md#3-the-guardrail--lead-with-the-non-goal).

---

## 1. System overview

Five layers, top to bottom: **wire front end → query layer → transaction & catalog → storage engine → physical storage (local + object store).** A lineage/provenance subsystem and an observability subsystem run alongside.

```mermaid
flowchart TB
    subgraph CLIENT["Clients"]
        psql["psql / pgcli"]
        drivers["Drivers & ORMs<br/>(JDBC, psycopg, pgx, SQLAlchemy)"]
        bi["BI & admin tools<br/>(DBeaver, Grafana, Metabase)"]
        cli["stele CLI"]
    end

    subgraph FRONT["Wire front end"]
        pgwire["Postgres wire protocol<br/>(simple + extended query, COPY)"]
        authn["AuthN / TLS<br/>(SCRAM-SHA-256)"]
    end

    subgraph QUERY["Query layer"]
        parser["Parser → AST"]
        binder["Binder / name resolution<br/>(catalog-aware)"]
        planner["Logical planner"]
        optimizer["Cost-based optimizer<br/>(+ temporal rewrite rules)"]
        executor["Vectorized executor<br/>(Arrow-shaped batches)"]
    end

    subgraph TXN["Transaction & catalog"]
        txnmgr["Transaction manager<br/>(MVCC, snapshots)"]
        catalog["Catalog / metadata<br/>(versioned)"]
        lineage["Lineage & provenance<br/>subsystem"]
    end

    subgraph STORAGE["Storage engine"]
        wal["WAL<br/>(append-only commit log)"]
        memdelta["Delta tier<br/>(row-oriented, in-memory + spill)"]
        segs["Sealed segments<br/>(immutable columnar)"]
        idx["Indexes & zone maps<br/>(B-tree, hash, bloom, min/max)"]
        compaction["Compaction / merge<br/>(history-preserving)"]
        tiering["Tiering & cache manager"]
    end

    subgraph PHYS["Physical storage"]
        localdisk["Local NVMe<br/>(WAL, hot cache, delta spill)"]
        objstore["S3-compatible object store<br/>(cold sealed segments)"]
    end

    obs["Observability<br/>(tracing, metrics, EXPLAIN)"]

    CLIENT --> pgwire --> authn --> parser
    cli --> pgwire
    parser --> binder --> planner --> optimizer --> executor
    binder -. reads .-> catalog
    optimizer -. stats .-> catalog
    executor --> txnmgr
    txnmgr --> wal
    txnmgr -. snapshot .-> segs
    executor --> memdelta
    executor --> segs
    executor -. prune .-> idx
    txnmgr --> lineage
    memdelta --> compaction --> segs
    segs --> tiering
    tiering --> localdisk
    tiering --> objstore
    wal --> localdisk
    QUERY -.-> obs
    STORAGE -.-> obs
```

**Reading the picture:** a query enters over pg-wire, is parsed/bound/planned/optimized, and executes against a **consistent MVCC snapshot**. Reads merge the row-oriented **delta tier** (recent writes) with the columnar **sealed segments** (the bulk), pruning with zone maps and indexes. Writes append to the WAL and the delta tier; compaction later folds the delta into new immutable segments; tiering moves cold segments to object storage. Provenance is captured at commit.

---

## 2. The bitemporal record model

This is the conceptual heart. Every logical row is a **chain of versions**, each tagged on two independent time axes.

- **System time** `[sys_from, sys_to)` — when the *database* held this version. Always present; set by the committing transaction. Half-open. **`sys_to` is not stored on the record** — it is *derived* (the next version's `sys_from`, or `+∞` for the current version) and materialized **once** into the validity index ([ADR-0023](adr/0023-append-only-record-model-validity-index.md)). Times are µs/int64 with a `+∞` sentinel and a per-commit `seq` tiebreak ([ADR-0024](adr/0024-time-representation.md)).
- **Valid time** `[valid_from, valid_to)` — when the fact is *true in the modeled world*. Per-table opt-in. Supplied by the writer.
- **Decision/knowledge time** — a third axis is *reserved but off by default*; the record + catalog shape leave room to add it later with no re-architecting ([assumption A2'](assumptions.md)).

```mermaid
erDiagram
    LOGICAL_ROW ||--o{ VERSION : "has history of"
    VERSION ||--o| VALIDITY_INDEX : "sys_to materialized once into"
    VERSION {
        bytes  business_key      "user/PK or hash key"
        ts     sys_from          "system-time start (commit)"
        ts     valid_from        "valid-time start (opt-in)"
        ts     valid_to          "valid-time end (opt-in)"
        u64    seq               "per-commit total-order tiebreak"
        u64    txn_id            "writing transaction"
        ts     committed_at      "commit timestamp"
        text   principal         "who/what wrote it"
        bytes  payload           "the column values"
    }
    VALIDITY_INDEX {
        bytes  business_key      "key"
        ts     sys_from          "the version this closes"
        ts     sys_to            "DERIVED, written once (never on the record)"
    }
```

> **No stored `sys_to`.** Version records are append-only and never mutated; a version's system-time end lives only in the *derived, rebuildable* validity index, written once when the superseding assertion — or a delete's **retraction record** — commits ([ADR-0023](adr/0023-append-only-record-model-validity-index.md)). Retractions are persisted (WAL + payload-less tombstone rows in segments) so an index rebuild can never lose a deletion gap ([16 §12](16-bitemporal-semantics.md#12-deletes-retractions--the-deletion-gap)). This is what makes the append-only / tamper-evidence claims hold under scrutiny.

A **bitemporal query** picks a point (or range) on each axis. "As we believed on 2026-01-31 (system), about the state of the world on 2026-01-15 (valid)" selects, per business key, the version whose `sys` interval contains 2026-01-31 *and* whose `valid` interval contains 2026-01-15.

```mermaid
flowchart LR
    subgraph axes["A single business key's version space"]
        direction TB
        note["X axis = valid time · Y axis = system time<br/>each rectangle = one stored version covering a 2D region<br/>an AS OF (sys, valid) point lands in exactly one rectangle"]
    end
```

> Because corrections *append* a new version (closing the prior one on the **system** axis while possibly back-dating on the **valid** axis), Stele can always answer "what did we think then" and "what was actually true then" independently. This is the property that makes audit and retroactive correction trivial — and it is why the store is append-only ([ADR-0002](adr/0002-on-disk-storage-format.md)).

---

## 3. Storage engine internals

### 3.1 Tiered layout (LSM-flavored, history-preserving)

Stele uses an **LSM-inspired** two-tier shape, adapted so that compaction **never discards history**:

```mermaid
flowchart TB
    write["Write / MERGE"] --> wal["WAL append<br/>(durability point)"]
    wal --> delta["Delta tier<br/>row-oriented, sorted by key+sys_time<br/>in-memory, spills to local disk"]
    delta -->|"checkpoint / size threshold"| flush["Flush"]
    flush --> seg["New sealed segment<br/>(immutable, columnar)"]
    seg --> L0["Segment level L0"]
    L0 -->|compaction| L1["Segment level L1<br/>(merged, read-optimized)"]
    L1 -->|compaction| L2["Segment level L2<br/>(larger, colder)"]
    L2 -->|tiering| cold["Object storage (S3)"]

    subgraph meta["Per-segment metadata (always hot)"]
        zm["Zone maps: min/max per column<br/>incl. sys_time & valid_time ranges"]
        bf["Bloom filters on hash keys"]
        footer["Footer: schema, encodings, offsets"]
    end
    seg -.-> meta
```

**Key differences from a vanilla LSM:**
- Compaction merges and re-encodes for read efficiency but **retains every version** (unless an explicit, audited retention policy says otherwise — off by default).
- Segments are sorted/clustered by `(business_key, sys_from)` so a key's version chain is physically local and time-range pruning is cheap.
- "Tombstones" are **logical period-closes**, not deletions; they carry their own provenance.

As of v0.3 (STL-231) compaction is the manual `COMPACT` admin command: it
flushes the delta, merges every live segment into one consolidated segment
(re-clustered by `(business_key, sys_from, seq)`, retraction tombstones carried
verbatim), and **atomically swaps** the live set in the per-table segment
manifest ([ADR-0030](adr/0030-segment-manifest-retirement.md)) — inputs are
retired whole, never mutated. Level policy and background scheduling (the
L0→L1→L2 ladder above) layer on later; time-era clustering is v0.7
([ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md)).

### 3.2 On-disk segment format

A sealed segment is an **immutable, self-describing columnar file** (Stele's own format — see [ADR-0002](adr/0002-on-disk-storage-format.md)), conceptually Parquet/ORC-like but designed around the bitemporal record and append-only segments:

```mermaid
flowchart TB
    subgraph SEGMENT["Sealed segment file"]
        direction TB
        hdr["Header: magic, format version"]
        subgraph rg["Row group / stripe (× N)"]
            colA["Column chunk: business_key<br/>(dict + bitpack)"]
            colS["Column chunk: sys_from / sys_to<br/>(delta + FOR)"]
            colV["Column chunk: valid_from / valid_to"]
            colP["Column chunks: payload columns<br/>(per-column codec)"]
            prov["Column chunks: txn_id, committed_at, principal"]
        end
        footer["Footer: per-column stats (zone maps),<br/>bloom filters, encoding metadata,<br/>schema id, checksum"]
    end
    hdr --> rg --> footer
```

- **Self-describing & versioned:** the header carries a format version; the footer carries the schema id (so the [versioned catalog](#5-catalog--metadata) can interpret old segments after schema evolution).
- **Checksummed:** page- and footer-level checksums; corruption is detectable (and tested against torn-write models in [06](06-testing-strategy.md)).
- **Codec per column:** dictionary, RLE, bit-packing, frame-of-reference, delta — chosen by the writer from column statistics.
- **Provenance columns are first-class**, not a side table — they compress well (txn_id is monotonic; committed_at is delta-friendly).

### 3.3 How B-tree and columnstore coexist

The columnstore is the **primary** structure (scans, aggregation, temporal range). The B-tree/hash indexes are **secondary, optional access paths** that map a key (or hash key) to the segment + row-group where its current/version rows live — giving *adequate* point lookups without compromising the columnar layout.

```mermaid
flowchart LR
    q1["Analytical query<br/>(scan / aggregate / AS OF range)"] --> zm["Zone-map pruning<br/>(skip segments by sys/valid/value range)"] --> scan["Vectorized columnar scan"]
    q2["Point lookup / MERGE probe<br/>(by key or hash key)"] --> sec["Secondary index<br/>(B-tree / hash + bloom)"] --> locate["Locate segment + row-group"] --> fetch["Fetch minimal columns<br/>(late materialization)"]
    scan --> result["Result batches"]
    fetch --> result
```

The columnstore never depends on the secondary indexes for correctness — they are an accelerator. Drop them and analytical queries are unaffected; only point lookups slow down. This keeps the **asymmetric performance contract** honest.

### 3.4 Write path (sequence)

```mermaid
sequenceDiagram
    participant C as Client (pg-wire)
    participant E as Executor
    participant T as Txn manager
    participant W as WAL
    participant D as Delta tier
    participant L as Lineage
    C->>E: INSERT / UPDATE / MERGE (within txn)
    E->>T: acquire snapshot + txn_id
    E->>E: resolve temporal semantics<br/>(close prior sys/valid periods, open new)
    E->>W: append redo records (not yet durable)
    E->>D: stage new versions in delta tier
    C->>E: COMMIT
    E->>W: fsync (group commit) ✅ durability point
    E->>T: assign commit timestamp = sys_from
    E->>L: record per-row provenance (txn, principal, committed_at)
    E-->>C: CommandComplete
    Note over D: later: checkpoint flushes delta → sealed segment
```

The **durability point is the WAL fsync at commit.** Everything after (delta→segment flush, compaction, tiering) is asynchronous and recoverable from the WAL.

### 3.5 Read path / as-of (flow)

```mermaid
flowchart TB
    start["SELECT ... FOR SYSTEM_TIME AS OF :s<br/>[FOR VALID_TIME AS OF :v]"] --> snap["Pick MVCC snapshot<br/>(default :s = snapshot time)"]
    snap --> prune["Zone-map prune segments<br/>by sys/valid/value ranges"]
    prune --> merge["Merge delta tier + sealed segments"]
    merge --> pick["Per business key, choose version where<br/>sys interval ∋ :s AND valid interval ∋ :v"]
    pick --> filter["Apply predicates (pushed down)"]
    filter --> project["Late-materialize projected columns"]
    project --> out["Vectorized result batches → pg-wire"]
```

### 3.6 Crash recovery

On startup: load the checkpoint/segment-manifest record ([ADR-0030](adr/0030-segment-manifest-retirement.md)), sweep any segment file the manifest's live list does not name (a torn flush/compaction's orphan, or a retired compaction input), validate the live segments by checksum, **replay the WAL** forward (idempotently) to reconstruct the delta tier and re-open period sentinels, then resume. Recovery is **deterministic** and is exercised under fault injection in the [simulation harness](06-testing-strategy.md).

```mermaid
flowchart LR
    boot["Boot"] --> verify["Verify sealed segments<br/>(checksums)"] --> cp["Load latest checkpoint"] --> replay["Replay WAL from checkpoint<br/>(idempotent redo)"] --> rebuild["Rebuild delta tier + open sentinels"] --> ready["Ready (consistent)"]
```

### 3.7 On-disk layout & durability discipline (local backend)

Every byte the engine persists flows through the storage-backend trait
(`stele_storage::backend::Disk` — STL-90/STL-232), and the **`local`** backend —
the default ([05 — configuration](05-dev-environment.md#configuration)) — is its
complete reference implementation: one real directory (`[server] data_dir`)
holding a **flat namespace** of **append-only** files. The `memory` backend
models the identical contract on the heap (tests, sim, throwaway runs); an
object-store backend slots in at v0.4 (§4, [ADR-0007](adr/0007-storage-compute-separation.md)).
A shared conformance suite (`stele_storage::backend::conformance`) runs
unchanged against `local`, `memory`, and the sim's seeded fault disk, and
records the contract a new backend must meet.

**Data-dir layout.** Engine-wide logs sit at the root; each table's tier is
namespaced by a fixed-width prefix `t{NNNNNNNNNNNNNNNNNNNN}-` (its catalog
namespace index, zero-padded to 20 digits) so independent tables never collide
in the flat namespace:

| File | Scope | Contents |
|---|---|---|
| `stele.catalog` | root | append-only catalog log: every acknowledged DDL ([ADR-0028](adr/0028-durable-catalog-log.md)) |
| `stele.commits` | root | cross-table commit-marker log ([ADR-0029](adr/0029-cross-table-commit-marker.md)) |
| `t…-wal-{n}.log` | per table | WAL segments, numbered from 0, rotated by size |
| `t…-stele.checkpoint` | per table | append-only checkpoint / segment manifest: replay floor, durable fence, live segment list ([ADR-0030](adr/0030-segment-manifest-retirement.md)) |
| `t…-seg-{n}.seg` | per table | sealed, immutable columnar segments (§3.2) |
| `t…-delta-spill-{n}.row`, `t…-validity-spill-{n}.row` | per table | ephemeral spill files — reconstructible from WAL replay, deletable |

All `{n}` are zero-padded to 20 digits so lexicographic and numeric order
agree. Unrecognized names are ignored (forward compatibility); a sealed
segment the manifest's live list does not name is **dead** — an **orphan**
(debris of a crashed flush or compaction) or a **retired** compaction input
whose cleanup was interrupted — and recovery removes it
([ADR-0030](adr/0030-segment-manifest-retirement.md)). One fsync'd manifest
append is the atomic commit point for every segment-set transition: a flush
adds its segment to the list; a `COMPACT` swaps the inputs for the
consolidated output.

**Fsync discipline (file *and* directory).** The trait has two durability
points, and both are real fsyncs on `local`:

* `DiskFile::sync` (`fsync(2)`/`FlushFileBuffers`) makes a file's *contents*
  durable. It is issued at the WAL group-commit tick — **the** durability
  point (§3.4) — at segment seal (`SegmentWriter::finish`), and after every
  checkpoint/catalog/commit-marker append.
* `Disk::sync_dir` — the **directory fence** (STL-232) — makes the *namespace*
  durable: POSIX separates a file's bytes from its directory entry, and
  recovery rediscovers files by listing the directory, so a synced record in
  an unlinked file would be silently lost. The engine fences after creating
  any file recovery relies on: WAL open/rotation (before any record in the new
  segment can count as durable), flush (after sealing the segment, *before*
  the checkpoint vouches for it), and each root log's first creation. On
  Windows there is no supported directory flush; NTFS metadata journaling
  covers the gap and the fence is a documented no-op.

**Torn-tail posture.** Appends can tear (power loss mid-write). Every
append-only log tolerates a torn *tail*: a partial or magic-less trailing
frame is the unacknowledged debris of a crashed append and is skipped (WAL
replay, checkpoint scan, catalog/commit-marker replay all stop cleanly), while
a *complete* frame whose CRC fails is corruption of acknowledged history and
**fails closed**. Sealed segments are checksummed per page and verified at
recovery (§3.6) — flipping any byte fails the read.

**Error taxonomy.** Namespace misuse surfaces before I/O (`InvalidInput` for a
non-flat name; `AlreadyExists`/`NotFound` for create/open-remove conflicts);
reads past EOF are short counts or `Ok(0)`, never errors. A **failed fsync is
a crash, not a clean abort** (STL-217): a failed WAL `sync` or rotation fence
poisons the WAL — every subsequent write refuses with `WalError::Poisoned`
until the operator restarts into recovery — because a record staged before a
failed fsync has *indeterminate* durability and must never be flushed later
under the guise of an aborted operation. A failed fence elsewhere fails the
operation before anything is acknowledged (a flush aborts before the
checkpoint vouches; a first catalog append fails before the DDL is
acknowledged).

---

## 4. Object-storage tiering & storage/compute separation

Cold sealed segments live in an **S3-compatible object store** behind a pluggable backend trait (`local`, `memory`, `s3`). A **hot cache** on local NVMe holds recently/frequently read segments. Metadata (catalog, segment index, zone maps) stays resident.

```mermaid
flowchart TB
    subgraph compute["Compute node (largely stateless)"]
        exec["Executor"]
        cache["Hot segment cache<br/>(local NVMe, LRU)"]
        metacache["Resident metadata<br/>(catalog, zone maps, segment index)"]
    end
    subgraph shared["Shared durable storage"]
        objstore["S3-compatible object store<br/>(immutable sealed segments + manifest)"]
        wallog["WAL (durable)"]
    end
    exec --> cache
    cache -->|miss| objstore
    exec --> metacache
    exec --> wallog
    objstore -. "immutable: segments never mutate,<br/>so caching is trivially coherent" .-> cache
```

Because segments are **immutable**, cache coherence is free: a cached segment can never be stale. This is a direct dividend of the append-only design and is what makes storage/compute separation (and, later, multiple stateless readers over one dataset) clean. See [ADR-0007](adr/0007-storage-compute-separation.md).

### Storage lifecycle: tiered archival (controlling append-only growth)

Append-only means total data volume only grows — so without a cost strategy, object-storage bills grow unbounded. Stele manages this with **tiered archival** ([ADR-0021](adr/0021-storage-lifecycle-tiered-archival.md)), which is *distinct from retention/expiry* ([01 §A.2](01-feature-plan.md#a2--append-only--immutable-storage--historization)): tiering **keeps every byte** (append-only + audit guarantees intact) and only moves cold data to cheaper storage.

The bitemporal model supplies a **principled staleness signal for free**: **system-time age** tells the engine exactly which versions are *superseded history* vs *current*. Current versions stay hot; superseded versions age **down** the tier ladder. Compaction clusters segments by **time-era**, so a cold segment is *purely* old history and never drags a live row into archive.

```mermaid
flowchart TB
    hot["Hot — local NVMe cache<br/>(current + recently read)"]
    warm["Warm — S3 Standard"]
    cold["Cold — S3-IA / Glacier Instant<br/>(cheaper, still ms reads)"]
    frozen["Frozen — Glacier Deep Archive<br/>(~23x cheaper, 12–48h restore)"]
    hot -->|"superseded, by system-time age"| warm --> cold --> frozen
    frozen -.->|"explicit RESTORE (async)"| hot
    meta["Resident metadata + zone maps<br/>(ALWAYS hot, never archived)"]
    meta -. "prune first → rehydrate only matching segments" .-> frozen
```

Two properties keep retrieval cheap and predictable:

- **Metadata and zone maps are never archived.** An `AS OF` query prunes against resident zone maps *first* and only rehydrates the handful of segments that actually match — you never thaw a whole partition to answer a narrow query.
- **Frozen data needs an explicit, async restore.** The **tier-aware planner** detects when a query would touch Glacier-class data and returns `restore required` + a handle (with a cost/latency estimate) rather than silently hanging for hours; the user issues a `RESTORE` (or admin-API) call to rehydrate, then re-queries. Cold tiers with millisecond retrieval (S3-IA / Glacier Instant) are read transparently.

Tiering is **engine-native and pluggable**: Stele decides per-segment placement (by system-time/policy) and sets the storage class on write/migration, working across any S3-compatible backend; delegating to S3 Intelligent-Tiering is an optional backend mode. Policy is configurable per namespace/table with conservative defaults — no surprise archival. Crucially, **the data always still exists** — archival changes cost and latency, never durability or auditability.

---

## 5. Catalog & metadata

The catalog is **itself versioned** (it lives on the same bitemporal substrate conceptually), so that **time-travel survives schema changes**: an `AS OF` read in the past resolves columns using the schema that was in effect *then*.

```mermaid
flowchart LR
    ddl["DDL (CREATE/ALTER/DROP)"] --> catw["Catalog writer"]
    catw --> schemas["Schema versions<br/>(each tagged with sys_time)"]
    binder["Binder / planner"] -. "resolve names as of snapshot" .-> schemas
    schemas --> segfooter["Segment footers reference schema id"]
    catalog_shim["pg_catalog / information_schema shims"] --> schemas
    catalog_shim -. "so pg tools introspect Stele" .-> tools["BI / admin tools"]
```

The catalog also exposes **`pg_catalog`/`information_schema` shims** so the Postgres ecosystem's introspection (and thus drivers/BI tools) works against Stele.

**Namespaces as isolation + lifecycle units.** Schemas/namespaces are a first-class boundary: each can carry its own [encryption key, residency, and access policy](10-security-and-compliance.md#9-hardening--operational-security), and supports an **audited drop** that decommissions a whole namespace as a clean break — the basis for tenant offboarding and [namespace-drop erasure](10-security-and-compliance.md#the-append-only-vs-right-to-erasure-tension-handled-not-hand-waved). This is a *general* tenancy primitive: the app (e.g., Solvia) maps tenants to namespaces; the engine never knows what a tenant *is* ([ADR-0009](adr/0009-data-vault-conceptual-seam.md), [ADR-0020](adr/0020-crypto-shredding-erasure.md)).

**Manifest/catalog scalability is a first-class concern.** In append-only, object-tiered systems the **catalog/manifest usually becomes the bottleneck before the data layer does** (the Hive-metastore / early-Iceberg lesson): millions of segment/manifest entries make catalog queries and commit times the limiter. The catalog is designed for this — compact, incrementally-snapshotted manifest state, with catalog query latency and commit time tracked as file count grows ([06](06-testing-strategy.md), [14](14-performance-and-benchmarking.md)).

---

## 6. Query layer

```mermaid
flowchart LR
    sql["SQL text"] --> parse["Parser<br/>(hand-written or sqlparser-rs)"]
    parse --> ast["AST"]
    ast --> bind["Binder<br/>(name & type resolution,<br/>temporal period binding)"]
    bind --> logical["Logical plan"]
    logical --> rewrite["Rewrite rules<br/>(predicate pushdown, temporal<br/>normalization, projection pruning)"]
    rewrite --> cost["Cost-based optimizer<br/>(stats from catalog + zone maps)"]
    cost --> physical["Physical plan<br/>(vectorized operators)"]
    physical --> exec["Executor<br/>(pull/push hybrid, Arrow batches)"]
```

- **Parser:** start from `sqlparser-rs` to move fast, with Stele-specific temporal grammar; revisit a hand-written parser only if needed.
- **Optimizer:** rule-based first (pushdown, pruning, temporal predicate normalization), cost-based as statistics mature. **Temporal-aware rules** are the differentiating part — e.g., pushing an `AS OF` predicate into segment-level `sys_time` zone-map pruning.
- **Executor:** vectorized, **batch-at-a-time pull (Volcano)** over **Arrow-shaped** columnar batches ([assumption A7](assumptions.md)) for SIMD-friendliness and ecosystem interop — one object-safe operator trait, with `SnapshotScan` as a source operator ([ADR-0027](adr/0027-vectorized-execution-model.md)). The execution core is written to run under the deterministic simulation scheduler ([06](06-testing-strategy.md)).

---

## 7. Postgres wire-protocol front end

Adopting the [Postgres wire protocol](https://www.postgresql.org/docs/current/protocol.html) inherits the entire driver/ORM/BI/admin ecosystem — the single highest-leverage adoption decision ([ADR-0003](adr/0003-postgres-wire-protocol-early.md)). It lands **early and incrementally**.

```mermaid
flowchart TB
    conn["TCP connection"] --> startup["Startup + TLS negotiation"]
    startup --> auth["SCRAM-SHA-256 auth"]
    auth --> loop{"Message loop"}
    loop -->|"Simple Query 'Q'"| simple["Parse → execute → RowDescription + DataRows"]
    loop -->|"Extended: Parse/Bind/Execute"| extended["Prepared statements + parameter binding"]
    loop -->|"COPY"| copy["Bulk ingest path"]
    simple --> ready["ReadyForQuery"]
    extended --> ready
    copy --> ready
    ready --> loop
```

**Phasing:** *simple query* in **v0.1** (so `psql` connects and runs `SELECT`/`INSERT`); *extended query* (prepared/bind) in **v0.2** (drivers/ORMs need it); *COPY* in **v0.3**. Temporal SQL extensions ride on top of standard pg syntax where they don't conflict; where SQL:2011 and Postgres diverge, the choice is documented ([assumption A9](assumptions.md)).

> We implement the **protocol**, not Postgres's semantics wholesale. Stele is not Postgres-compatible at the planner/MVCC level — it is wire- and introspection-compatible enough to inherit tooling. That boundary is deliberate and documented in [ADR-0003](adr/0003-postgres-wire-protocol-early.md).

---

## 8. Lineage & provenance subsystem

Provenance is captured at **commit** and stored **inline** with each version (not in a side audit table):

```mermaid
flowchart LR
    commit["Commit"] --> capture["Capture: txn_id, committed_at,<br/>principal, statement digest"]
    capture --> inline["Store inline as provenance columns<br/>on each written version"]
    inline --> query["Queryable via pseudo-columns:<br/>_stele_txn_id, _stele_committed_at, _stele_principal"]
    inline --> audit["Append-only commit log = audit trail"]
    audit -.->|"v1.0+"| crypto["Optional Merkle/hash-chaining<br/>(tamper-evident)"]
    query -.->|"v0.7+ opt-in"| deriv["Derivation lineage graph<br/>(row computed-from inputs)"]
```

Two tiers of provenance:
1. **Per-row transaction provenance** (Must, v0.2): who/what/when wrote each version. Cheap, always-on.
2. **Derivation lineage** (Later, opt-in, v0.7+): a graph of "this row was computed from those inputs by that statement." Powerful but expensive; off by default. See [01 §A.4](01-feature-plan.md#a4--lineage--provenance-first-class).

This is the substrate that makes audit *and* Data Vault cheap to build **on top of Stele** — without Stele knowing what a hub or a claim is ([ADR-0009](adr/0009-data-vault-conceptual-seam.md)).

---

## 9. Transaction & concurrency model

MVCC is layered directly on the append-only store, which already *is* a multi-version store ([ADR-0008](adr/0008-mvcc-on-append-only.md)):

- A transaction reads a **snapshot** = a system-time point; it sees, per key, the latest version whose `sys` interval contains the snapshot.
- Writes append new versions with `sys_from = commit_time`; **snapshot isolation** is the v1 default.
- Conflicts (write-write on the same key within overlapping snapshots) are detected and the loser retries.
- **Serializable (SSI)** is a later opt-in (v0.7).
- Garbage *is not* collected by default (append-only); space management is via tiering and explicit, audited retention policies only.

```mermaid
sequenceDiagram
    participant R as Reader (snapshot @ s)
    participant K as Key K version chain
    participant W as Writer (commits @ c > s)
    R->>K: read K
    K-->>R: version with sys interval ∋ s
    W->>K: append new version (sys_from = c)
    Note over R: Reader's snapshot s is unaffected by c —<br/>still sees the old version (snapshot isolation)
```

---

## 10. Distribution & consensus (later phase)

Distribution is **designed-for, built-later** (Charter §3, [ADR-0006](adr/0006-distribution-later-shared-storage.md)). The intended shape leans on the immutable + shared-object-storage foundation: **stateless-ish compute over shared storage**, with **Raft** for control-plane metadata consensus — *not* a from-scratch Paxos or a TrueTime-style clock.

```mermaid
flowchart TB
    subgraph control["Control plane (Raft group)"]
        m1["Meta node 1 (leader)"]
        m2["Meta node 2"]
        m3["Meta node 3"]
        m1 --- m2 --- m3 --- m1
    end
    subgraph computeN["Compute nodes (stateless over shared storage)"]
        c1["Compute 1"]
        c2["Compute 2"]
        c3["Compute N"]
    end
    subgraph datap["Shared data plane"]
        obj["S3-compatible object store<br/>(immutable segments + manifest)"]
        durwal["Durable WAL / log service"]
    end
    control -. "segment manifest,<br/>schema, txn coordination" .-> computeN
    computeN --> obj
    computeN --> durwal
    obj -. "immutability ⇒ trivially shareable<br/>across readers" .-> computeN
```

Why this shape: immutable segments mean **read scale-out is nearly free** (any node can read any cached segment with no coherence protocol). The hard part — and what Raft solves — is agreeing on *which segments are current* (the manifest) and coordinating commit order. Consistency for this phase is validated with **Jepsen-style testing before any multi-node production claim** ([06](06-testing-strategy.md), Charter §8). **Stele is CP:** under a network partition it **rejects writes rather than accept-and-reconcile divergent histories** — audit integrity is chosen over availability, because reconciling conflicting system-time histories would corrupt the audit record ([ADR-0006](adr/0006-distribution-later-shared-storage.md)).

**Data distribution & co-location.** Within this shape, a table may declare a **distribution key** — typically a [stable hash key](01-feature-plan.md#a5--hash-keys--mergeupsert) — and rows partition across nodes by its hash; frequently-joined tables can be **co-located** (co-partitioned on the same key) so those joins stay node-local with no shuffle. These are generic sharded-analytics primitives, but they are deliberately part of the [integration groundwork](adr/0011-hash-distribution-integration-groundwork.md) ([ADR-0011](adr/0011-hash-distribution-integration-groundwork.md)): they make hash-keyed models — Data Vault among them — distribute and join cleanly, while the engine stays ignorant of what a hub or satellite is ([ADR-0009](adr/0009-data-vault-conceptual-seam.md)).

**Clocks & cross-node time ordering.** Stele's spine is *system-time* ([§2](#2-the-bitemporal-record-model)), so in a distributed deployment time ordering across nodes is **correctness-critical**, not cosmetic ([ADR-0022](adr/0022-clock-synchronization-and-ordering.md)). NTP alone is insufficient (real drift is tens–hundreds of ms), so:

- **NTP is a hard requirement** on every node (chrony/PHC), enforced by the [operator](09-ecosystem-and-products.md#5-kubernetes--openshift-operator) as preflight + continuous check — the *baseline*, not the guarantee.
- **Hybrid Logical Clocks (HLC)** assign system-time across nodes (physical time + logical counter) so **causal ordering survives bounded skew** — the CockroachDB/YugabyteDB approach.
- A **configurable max-skew bound** is enforced: a node drifting beyond it **fences itself** (stops serving) rather than mis-order history — fail-safe over fail-wrong.
- **PTP / cloud time-sync** (e.g. Amazon Time Sync at microsecond accuracy) narrows the bound; a TrueTime-style commit-wait path is optional where high-precision time exists.

```mermaid
flowchart TB
    ntp["NTP / PTP on every node<br/>(operator-enforced)"] --> hlc["Hybrid Logical Clock<br/>(physical + logical counter)"]
    hlc --> skew{"Skew over max bound?"}
    skew -->|"no"| order["Causally-consistent system-time<br/>ordering across nodes"]
    skew -->|"yes"| fence["Node fences itself<br/>(stop serving) — fail-safe"]
```

Single-node and read-replica deployments are unaffected — one writer clock, no cross-node ordering problem.

---

## 11. Crate / module decomposition (intended)

A Cargo workspace; boundaries chosen so the **deterministic storage core** can run under the simulation harness independent of the async runtime ([assumption A13](assumptions.md)).

```mermaid
flowchart TB
    subgraph ws["Cargo workspace"]
        common["stele-common<br/>(types, errors, time)"]
        storage["stele-storage<br/>(segments, WAL, delta, compaction)"]
        sim["stele-sim<br/>(virtual clock/disk/net,<br/>deterministic scheduler)"]
        catalog2["stele-catalog<br/>(versioned metadata)"]
        txn2["stele-txn<br/>(MVCC, snapshots)"]
        sql2["stele-sql<br/>(parser, binder, planner, optimizer)"]
        exec2["stele-exec<br/>(vectorized operators)"]
        pg["stele-pgwire<br/>(protocol front end)"]
        lineage2["stele-lineage<br/>(provenance)"]
        server["stele-server<br/>(daemon, wiring, config)"]
        clibin["stele-cli<br/>(stele binary)"]
    end
    storage --> common
    sim --> storage
    catalog2 --> common
    txn2 --> storage
    sql2 --> catalog2
    exec2 --> storage
    exec2 --> txn2
    pg --> sql2
    pg --> exec2
    lineage2 --> txn2
    server --> pg
    server --> lineage2
    clibin --> server
```

> The `stele-sim` crate provides the injectable virtual clock, deterministic RNG, and simulated disk/network that the storage/txn core runs against — the FoundationDB/TigerBeetle pattern ([06](06-testing-strategy.md)). Keeping the core runtime-agnostic is an architectural constraint, not an afterthought.

> `stele-exec`'s "vectorized operators" are a **batch-at-a-time Volcano pull** pipeline over Arrow-shaped batches — one object-safe operator trait, `SnapshotScan` adapted as a source operator ([ADR-0027](adr/0027-vectorized-execution-model.md)).

---

## 12. Cross-cutting architectural invariants

These are test-enforced ([06](06-testing-strategy.md)) and amendable only via ADR:

1. **No in-place mutation of a sealed segment.** Ever.
2. **The WAL fsync is the only durability point.** Everything downstream is recoverable from it.
3. **Immutability ⇒ trivial cache/replica coherence.** No segment is ever stale.
4. **System-time is always present; valid-time is per-table opt-in.** ([assumption O3](assumptions.md))
5. **Provenance is inline and captured at commit**, never reconstructed after the fact.
6. **The columnstore is correct without any secondary index.** Indexes are accelerators only.
7. **The storage/txn core is deterministic** and runnable under the simulation scheduler.
8. **History within a dataset is immutable; a whole namespace has a lifecycle.** Sealed segments are never rewritten (invariant 1), but creating and *dropping* an entire namespace is a legitimate, audited, coarse operation — a drop is implemented as destroying the namespace's key, not mutating segments ([ADR-0020](adr/0020-crypto-shredding-erasure.md)).
9. **`sys_to` is never stored on a record.** The append-only log is the source of truth; a version's system-time end lives only in the *derived, rebuildable* validity index ([ADR-0023](adr/0023-append-only-record-model-validity-index.md)).
10. **The commit log is hash-chained and verifiable.** Tampering with any historical record is detectable; an auditor can verify inclusion/consistency without trusting the operator ([ADR-0026](adr/0026-verifiable-audit-log.md)).
11. **Distributed Stele is CP.** Writes are rejected under partition; divergent histories are never reconciled ([ADR-0006](adr/0006-distribution-later-shared-storage.md)).

Each box in the diagrams above traces to an [ADR](adr/README.md); each ADR traces back to the [Charter](00-charter.md).
