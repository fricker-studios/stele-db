# Changelog

All notable changes to Stele are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com) and the project adheres to
[Semantic Versioning](https://semver.org).

## [0.1.0] - 2026-06-07

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


