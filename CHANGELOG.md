# Changelog

All notable changes to Stele are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com) and the project adheres to
[Semantic Versioning](https://semver.org).

## [0.2.0] - 2026-06-12

### Features

- *sql*: Bind FOR VALID_TIME AS OF onto the bound plan (STL-162) (#78)
- *exec*: Vectorized operator framework — batch-at-a-time pull pipeline (STL-169) (#77)
- *engine*: Multi-statement BEGIN/COMMIT/ROLLBACK through pgwire (STL-174) (#79)
- *pgwire*: Extended-query protocol + prepared-statement cache (STL-182) (#80)
- *exec*: Both-axes (system + valid) version resolution in SnapshotScan (STL-163) (#81)
- *storage*: Distinguish empty/absent zone-map bounds for bytes columns (STL-120) (#82)
- *engine*: Honor SELECT projection + WHERE via a row codec (STL-151) (#83)
- *sql*: SQL:2011 period predicates over half-open intervals (STL-165) (#86)
- *engine*: Route bitemporal AS OF (sys + valid) through SessionEngine + pgwire (STL-164) (#85)
- *sql*: Portable hash-key function + published byte-spec (STL-179) (#90)
- *types*: First-class PERIOD type backing system/valid time (STL-180) (#87)
- *storage*: Checkpoint flushes delta tier to sealed segments + bounds recovery replay (STL-177) (#92)
- *txn*: Verify the commit-log hash chain on recovery + tamper-evidence test (STL-178) (#88)
- *sql*: TIMESTAMPTZ UTC-internal + half-open boundary semantics (STL-189) (#91)
- *types*: Add UUID + BYTEA scalar types with wire text encoders (STL-181) (#89)
- *exec*: Row-group-scoped late materialization in SnapshotScan (STL-155) (#96)
- *cli*: Interactive query shell over pg-wire (STL-185) (#95)
- *cli*: Datum-brand shell visual design + psql-parity meta-commands (STL-198) (#97)
- *engine*: Savepoints (SAVEPOINT / ROLLBACK TO / RELEASE) over multi-statement txns (STL-176) (#98)
- *exec*: Vectorized scalar expression evaluator + Filter operator (STL-170) (#99)
- *engine*: Snapshot isolation across multi-statement transactions (STL-175) (#100)
- *exec*: Wire vectorized Filter onto the live SELECT path (STL-206) (#102)
- *exec*: Zone-map row-group block skipping in the vectorized scan (STL-173) (#103)
- *exec*: Hash GROUP BY + aggregates (COUNT/SUM/MIN/MAX/AVG) (STL-171) (#104)
- *exec*: AVG returns a true fractional FLOAT8 average (STL-209) (#105)
- *exec*: Hash join operators — inner/left/semi/anti (STL-172) (#106)
- *engine*: Durable catalog log + SessionEngine cold-boot recovery (STL-210) (#107)
- *exec*: Vectorized evaluator — temporal/uuid/bytea/period types + div/mod (STL-207) (#108)
- *pgwire*: Binary-format encoders + format-code negotiation (STL-183) (#109)
- *exec*: Zero-copy shared-buffer Column for batch slicing + projection (STL-191) (#110)
- *engine*: Crash-atomic group commit — one WAL record + one fsync per COMMIT (STL-192) (#111)
- *sql*: Per-row period predicates over value columns (STL-193) (#112)
- *sql*: Write valid-time intervals through INSERT/UPDATE (STL-194) (#113)
- *engine*: Drive storage flush/checkpoint from SessionEngine (STL-195) (#115)
- *cli*: ⇥ completion from the live catalog + persisted shell history (STL-202) (#116)
- *sql*: Operator-facing CHECKPOINT/FLUSH admin command over pgwire (STL-219) (#118)
- *storage*: Bound Engine::flush row-groups so scans benefit from late materialization (STL-197) (#119)
- *engine*: Prune the MVCC write index below the oldest live snapshot (STL-204) (#120)
- *engine*: Read-your-own-writes — overlay buffered txn writes on snapshot reads (STL-203) (#121)
- *pgwire*: ROLLBACK TO SAVEPOINT recovers an aborted transaction (STL-205) (#122)
- *pgwire*: Statement-level RowDescription on Describe (STL-212) (#123)
- *sql*: Emit Div/Mod, new-type comparisons, and per-row PERIOD on the live WHERE path (STL-213) (#124)
- *engine*: Cross-table crash-atomic commit via per-transaction commit marker (STL-215) (#126)
- *exec*: Zero-copy Filter row selection via selection-vector batches (STL-214) (#125)
- *storage*: Poison the WAL on an fsync failure (STL-217) (#129)
- *storage*: Roll back an aborted group commit's in-memory writes (STL-216) (#130)
- *cli*: Adopt rustyline with-file-history for shell history (STL-221) (#136)
- *engine*: Valid-time read-your-own-writes overlay (STL-223) (#139)
- *sql,cli*: Varchar-family column types, civil-time DML literals, shell timing trailer (#140)
- *storage,ci*: Portable positioned-read backend + Windows CI leg (STL-160) (#143)

### Bug Fixes

- *exec*: Strip the valid-time frame on a no-pin read (STL-218) (#114)
- *engine*: Close the dropped era's rows at DROP TABLE (STL-211) (#117)
- *engine*: Re-derive a dropped era's storage closes at recovery (STL-220) (#132)
- *pgwire*: Reject extended Bind with a mismatched parameter count (STL-222) (#131)
- *engine*: Read a valid-time UPDATE's prior version bare across tiers (STL-226) (#137)
- *engine*: Observe the clock for fresh read snapshots so AS OF now() tracks idle time (STL-227) (#144)

### Performance

- *exec*: Zero-copy selection-vector join output gather (STL-224) (#138)

### Documentation

- *adr*: ADR-0027 vectorized execution model (STL-188) (#93)
- *config*: Add a sample stele.toml for operators (STL-208) (#101)

### Testing

- *exec*: Prove bitemporal DML deletion-gap survives index rebuild (STL-166) (#84)
- *storage*: Sealed-segment immutability oracle (STL-186) (#94)
- *sim*: Snapshot-isolation + provenance correctness oracle (STL-168) (#133)
- *oracle*: SQL-path bitemporal AS OF (sys, valid) DuckDB differential (STL-167) (#135)
- *ci*: JDBC + psycopg parameterized-query driver gate (STL-184) (#141)
- *sim*: Vectorized-exec + mixed commit/rollback txn scenarios under fault injection (STL-187) (#142)

### Build System

- Bump toml from 0.8.23 to 1.1.2+spec-1.1.0 (#127)
- Bump workspace MSRV + pinned toolchain to Rust 1.89.0 (STL-225) (#134)

### CI/CD

- Bump taiki-e/install-action from 2.81.4 to 2.81.10 (#128)

## [0.1.0] - 2026-06-08

### Features

- *storage*: Append-only WAL with group-commit fsync and CRC records (STL-86) (#8)
- *storage*: Delta tier with row-oriented store and disk spill (STL-87) (#11)
- *storage*: Sealed segment file format (STL-88) (#12)
- *storage*: Segment zone maps for planner pruning (STL-89) (#13)
- *storage*: Pluggable backend trait with local + in-memory impls (STL-90) (#14)
- *storage*: System-time version writer with close-prior-period (STL-91) (#15)
- *storage*: Per-table valid-time opt-in (STL-92) (#17)
- *storage*: Inline provenance columns on every written version (STL-93) (#18)
- *storage*: Bounded-prefix zone-map stats for large bytes columns (STL-115) (#19)
- *storage*: Wire stele.toml [storage] backend selection (STL-116) (#20)
- *storage*: Valid_from/valid_to segment columns for zone-map pruning (STL-117) (#21)
- *storage*: Record delete/tombstone provenance on period-close (STL-118) (#22)
- *storage*: Store bare payload in valid-time segments, reframe on read (STL-119) (#23)
- *storage*: DML insert/update/delete through WAL → delta tier (STL-94) (#24)
- *storage*: Close a sealed open version via an appended close marker (STL-127) (#25)
- *types*: Essential scalar + temporal type set (STL-96) (#26)
- *query*: SQL parser bootstrap + temporal grammar (STL-97) (#27)
- *catalog*: Versioned schema resolution at a system-time snapshot (STL-98) (#28)
- *query*: Bind CREATE TABLE / DROP TABLE DDL to the catalog (STL-95) (#31)
- *txn*: MVCC snapshot acquisition + commit-time assignment (STL-99) (#32)
- *storage*: Derived validity index — sys_to off the record (STL-133) (#33)
- *txn*: Per-commit seq + hash-chained commit log (STL-137) (#37)
- *storage*: Persist retractions; rebuild validity index from segments (STL-143) (#38)
- *storage*: Validity-index–backed segment prune (STL-139) (#39)
- *storage*: Thread a real SealedLookup through the valid-time / DML staging path (STL-140) (#43)
- *storage*: Carry per-commit seq on the version record (STL-141) (#44)
- *storage*: Order per-key chains by (sys_from, seq); drop the sys_from force-bump (STL-145) (#46)
- *exec*: SnapshotScan merges delta tier + sealed segments (STL-100) (#45)
- *sql*: Bind SELECT … FOR SYSTEM_TIME AS OF into a snapshot-scan plan (STL-101) (#48)
- *storage*: Crash-recovery driver — segment checksum + WAL replay from checkpoint (STL-102) (#49)
- *pgwire*: Simple-query Q-loop result protocol + SELECT 1 (STL-104) (#50)
- *pgwire*: Per-type text encoders for the v0.1 scalar set (STL-105) (#51)
- *engine*: Server-session engine — Catalog + commit clock + per-table tiers (STL-148) (#52)
- *pgwire*: Route CREATE/DROP TABLE DDL + minimal pg_catalog \d shim (STL-131) (#53)
- *sql*: Bind INSERT/UPDATE/DELETE into a BoundDml the engine applies (STL-149) (#54)
- *pgwire*: Route table SELECT + INSERT/UPDATE/DELETE through the simple-query loop (STL-147) (#55)
- *cli*: Stele version reports the build git commit (STL-106) (#57)
- *server*: STELE_LOG_FORMAT=json toggle + document logging baseline (STL-107) (#56)
- *stele-sim*: Seeded fault-injecting virtual disk (STL-109) (#58)
- *sim*: Virtual clock + ChaCha20 RNG + cooperative scheduler (STL-108) (#59)
- *stele-sim*: AS OF result-equivalence oracle vs in-memory reference (STL-111) (#61)
- *stele-sim*: Scenario registry — sweep many, replay one (STL-110) (#60)
- *exec*: SnapshotScan late materialization + validity-index segment prune (STL-146) (#63)
- *pgwire*: Report bound local_addr to kill the reserve-drop port race (STL-152) (#65)
- *pgwire*: End-to-end SQL NULL cell over the table-read path (STL-154) (#67)

### Bug Fixes

- *ci*: Run nightly sanitizers under +nightly toolchain (#30)

### Performance

- *storage*: Prune validity-index spill reads on point/small lookups (STL-142) (#42)

### Testing

- *pgwire*: Cover SSL/GSS N-refusal + server-boot probe (STL-103) (#29)
- *storage*: Valid-axis zone-map oracle + sys_to migration note (STL-134) (#34)
- *storage*: Read-path & recovery AS-OF correctness oracle (STL-136) (#35)
- *storage*: Differential oracle for the validity-index close write path (STL-135) (#36)
- *storage*: Bitemporal AS-OF reference oracle + metamorphic checks (STL-138) (#40)
- *pgwire*: Psql-golden wire text-encoder round-trip (STL-150) (#62)
- *stele-exec*: DuckDB differential oracle for bitemporal AS OF (STL-144) (#64)
- *sim*: Drive crash-recovery sweeps under FaultDisk injection (STL-153) (#66)

### Build System

- Bump thiserror from 1.0.69 to 2.0.18 (#3)

### CI/CD

- Bump actions/checkout from 4 to 6 (#2)
- Enforce Conventional Commits on PR titles (STL-85) (#5)
- Enable CodeQL static analysis for Rust + Actions (STL-113) (#6)
- Pin GITHUB_TOKEN to least-privilege on ci + nightly workflows (STL-114) (#7)
- Bump github/codeql-action from 4.36.1 to 4.36.2 (#10)
- Bump taiki-e/install-action from 2.81.3 to 2.81.4 (#9)
- Pin rust-toolchain to 1.85.0 instead of stable (#41)
- *nightly*: Fix sanitizer matrix — build-std for ABI, drop invalid UBSan (#47)
- Add Docker image + five-minute-path identity-demo smoke gate (STL-112) (#68)
- *release*: Add tag-driven release pipeline (STL-121) (#69)
- *release*: Merge per-crate CycloneDX BOMs into one workspace SBOM (STL-156) (#70)
- *release*: Add SLSA build provenance attestation (STL-157) (#71)
- Isolate the DuckDB differential oracle off the per-PR path (STL-158) (#73)
- *release*: Drop the untested Windows target from the release matrix (STL-159) (#74)
- *release*: Migrate retired macos-13 Intel runner to macos-15-intel (STL-161) (#75)
- *release*: Pin cosign to v2.6.3 so sign-blob keeps working (STL-190) (#76)


