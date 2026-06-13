# ADR-0032 — Backup manifest: a self-describing, tamper-evident inventory of a fenced snapshot

- **Status:** Accepted
- **Date:** 2026-06-13
- **Deciders:** Project owner + systems design
- **Related:** [01 §B.6](../01-feature-plan.md#b6--backup-restore--snapshots) · [03 §v0.3](../03-roadmap.md#v03--historization-indexing--operability) · [11 §5](../11-operations-and-runbooks.md#5-backup--restore) · [ADR-0007](0007-storage-compute-separation.md) (storage/compute seam) · [ADR-0026](0026-verifiable-audit-log.md) / [ADR-0031](0031-live-server-verifiable-commit-log.md) (hash chain) · [ADR-0030](0030-segment-manifest-retirement.md) (segment manifest) · STL-249 / STL-232 / STL-178 / STL-177

## Context

Online full backup + restore is the second clause of the [v0.3 exit
criterion](../03-roadmap.md#v03--historization-indexing--operability): a backup
taken under live write load, restored into a fresh data dir, must round-trip
**byte-for-byte** and answer every `AS OF` read at or before the backup's fence
identically. A backup is a copy of the engine's immutable set — sealed segments,
per-table WALs, the durable catalog log ([ADR-0028]), the hash-chained commit log
([ADR-0031]), and the per-table checkpoint manifests ([ADR-0030]) — taken at a
checkpoint/flush *fence* and copied through the [`Disk` backend
trait](../02-architecture.md#37-pluggable-storage-backends) ([STL-232]) so the
same path serves a local directory today and an object store later ([ADR-0007]).

A backup needs one extra file the engine does not otherwise produce: a
**manifest** describing what the backup contains, so restore can (a) materialize
exactly the right files, (b) detect tampering or bit-rot before booting the data,
and (c) record the fence instant the snapshot is consistent at. Point-in-time
recovery (v0.4, [03 §v0.4](../03-roadmap.md#v05--it-is-a-real-database)) will
extend this same artifact, so its format is an on-disk contract worth pinning.

Constraints: the manifest must be **verifiable without the engine** (restore
checks it before any recovery runs); it must add **no new dependency** to the
deterministic core ([ADR-0010]); a flipped byte in *any* backed-up file — the
manifest included — must be caught at restore; and it should be **operator-
inspectable** (a backup you cannot read is a backup you cannot trust).

Options considered for the encoding:

- **(a) A compact binary record** in the house style of the checkpoint / catalog
  / commit logs (magic + fields + CRC32C). Consistent, but opaque: an operator
  cannot `cat` a backup to see what it holds, and a backup is exactly the artifact
  you reach for when something has gone wrong.
- **(b) JSON / TOML via `serde`.** Readable, but pulls a serialization dependency
  into `stele-engine` — the one crate the supply-chain posture
  ([Cargo.toml](../../Cargo.toml), [ADR-0010]) most wants dependency-free.
- **(c) A small, hand-parsed line-oriented text format.** Readable, dependency-
  free, trivially diffable, and easy to spec exactly. The parse surface is tiny
  (a fixed header plus one line per file).

For the checksum, the existing CRC32C ([`stele_storage::checksum`]) detects
corruption but is not tamper-evident; the vendored SHA-256
([`stele_common::hash`], [ADR-0026]) is already the audit-path hash and is what
the commit-log chain ([ADR-0031]) uses, so it is the natural choice for a
tamper-evident inventory.

## Decision

**A backup directory contains the backed-up files (verbatim, under their backend
names) plus a single `MANIFEST`: a hand-parsed, UTF-8, line-oriented text file
that lists every file with its SHA-256, carries the fence instant and the
commit-chain head, and is closed by a self-digest over its own body.**

- **Filename & magic.** `MANIFEST`, first line `STELE-BACKUP-MANIFEST <version>`.
  The format version is **1**; restore refuses an unknown (newer) version rather
  than guessing its shape.
- **Body (one field per line, space-separated key then value):**

  ```text
  STELE-BACKUP-MANIFEST 1
  stele-version <crate version that produced the backup>
  fence-micros <i64 — the commit clock's high-water mark at the fence>
  commit-head <64 lowercase hex — the commit-log chain head this backup vouches for>
  files <N>
  <sha256-hex> <len-decimal> <name>          # × N, sorted by name
  digest <64 lowercase hex>                  # SHA-256 over every byte above this line
  ```

  A file line splits into exactly three fields, name last, so a name containing
  spaces still round-trips. The trailing `digest` is computed over the entire body
  preceding it; restore recomputes and compares it, so a flipped byte anywhere in
  the manifest is caught.
- **What a backup contains = the immutable set.** Every file the engine's disk
  holds **except** ephemeral spill tiers (`*-spill-*`, the delta- and validity-
  index scratch recovery discards on open). After the fence's flush the delta is
  drained, so in practice none remain; excluding them by contract keeps a backup
  to exactly the recovery-relevant set even if a stale spill lingers.
- **The fence.** Backup first flushes (sealing every delta into an immutable
  segment) and checkpoints (fsyncing every WAL), then records
  `fence-micros = clock.current()`. Every committed write with `sys_from ≤
  fence` is captured; every `AS OF` read at or before the fence answers
  identically on the restored copy. Files are copied **verbatim** at the `Disk`
  level (no re-encoding), so the immutable set restores byte-for-byte.
- **Two independent tamper layers.** Restore verifies the manifest self-digest,
  then each file's SHA-256, *before* writing it — fail-closed. Recovery then
  re-verifies independently: segment checksums ([02 §3.2]) and the commit-log
  hash chain ([ADR-0031], [STL-178]) on boot. A single flipped byte is caught at
  the manifest layer; the chain is the deeper backstop the audit pillar promises.
- **Manifest is not data.** Restore materializes only the files the manifest
  lists into the target data dir; the `MANIFEST` itself stays a backup artifact
  and is never copied into a live data directory.

## Consequences

### Positive
- An operator can read a backup's manifest directly — what it holds, each file's
  size and hash, the fence instant — without any tooling.
- No new dependency in `stele-engine`; the format reuses the vendored SHA-256
  already on the audit path.
- Byte-for-byte restore is structural: copying verbatim at the `Disk` level, with
  per-file hashes, makes "the restored set equals the source" a checkable fact,
  not a hope.
- Backend-agnostic: the manifest names files, not paths, so the same backup/restore
  path works for any `Disk` backend ([ADR-0007] object store, v0.4).

### Negative / costs
- A hand-rolled parser (rather than a battle-tested serializer); mitigated by a
  tiny, strict grammar and round-trip / tamper tests.
- SHA-256 over every file is more work than CRC32C; acceptable for v0.3 single-
  node volumes, and the tamper-evidence is the point. Hashing large segments is a
  cost a future incremental backup ([03 §v0.4](../03-roadmap.md#v05--it-is-a-real-database)) reduces by hashing only new segments.
- The manifest holds the whole inventory in one file; a backup with very many
  segments yields a large manifest. Compaction keeps segment counts low, and the
  per-file lines are small.

### Neutral / follow-ups
- **PITR (v0.4)** extends this manifest — likely a per-backup *generation* and a
  parent reference so an incremental backup names only the segments added since
  its base; the `commit-head` and `fence-micros` already anchor a backup in the
  chain's timeline, which is the hook PITR needs.
- **Streaming / non-blocking online backup** ([STL-309]). v0.3 takes the backup
  synchronously under the session lock (the brief stop-the-world `FLUSH`/`COMPACT`
  already are); the recorded fence makes a future lock-free copy (read prefixes at
  the fence while writers proceed) a format-compatible change, not a new contract.
- **Encryption** of backups end-to-end ([10 §4](../10-security-and-compliance.md#4-data-protection--encryption)) is a v0.7 concern; the manifest's per-file hashes verify integrity, not confidentiality.

[ADR-0007]: 0007-storage-compute-separation.md
[ADR-0010]: 0010-deterministic-simulation-testing.md
[ADR-0026]: 0026-verifiable-audit-log.md
[ADR-0028]: 0028-durable-catalog-log.md
[ADR-0030]: 0030-segment-manifest-retirement.md
[ADR-0031]: 0031-live-server-verifiable-commit-log.md
[02 §3.2]: ../02-architecture.md#32-on-disk-segment-format
[STL-178]: https://allegromusic.atlassian.net/browse/STL-178
[STL-232]: https://allegromusic.atlassian.net/browse/STL-232
[STL-309]: https://allegromusic.atlassian.net/browse/STL-309
