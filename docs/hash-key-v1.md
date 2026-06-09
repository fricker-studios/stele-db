# Portable hash-key spec — v1

> **Status:** Frozen. This is the canonical, version-pinned byte encoding behind
> the SQL `hash(...)` function. A client that recomputes a key against **v1** must
> get the engine's **v1** digest forever, on any platform and in any language. A
> future change to the encoding is a *new version* (`v2`, magic `STLHK2`), never an
> edit to this one.
>
> **Related:** [ADR-0011](adr/0011-hash-distribution-integration-groundwork.md)
> (integration-groundwork primitives) · [ADR-0009](adr/0009-data-vault-conceptual-seam.md)
> (the DV/RCM bright line) · [feature-plan §A.5](01-feature-plan.md#a5--hash-keys--mergeupsert)
> · reference implementation: [`stele_common::hashkey`](../crates/stele-common/src/hashkey.rs).

## Why this exists

A built-in hash function for business keys, **stable across engine versions,
platforms, and client languages**, is the integration-groundwork primitive any
hash-keyed modeling pattern needs ([ADR-0011](adr/0011-hash-distribution-integration-groundwork.md)).
Publishing the byte-spec lets an *external* model (or a client written in another
language) compute the **same** keys the engine does — the precondition for
hash-keyed loads, idempotent ingest, and (later) hash distribution.

The engine stays ignorant of what those keys *mean*: Data Vault hubs, RCM claims,
and every other downstream concept live above the bright line
([ADR-0009](adr/0009-data-vault-conceptual-seam.md)). This spec only defines how a
list of typed values becomes a digest.

## Algorithm

The digest is **SHA-256** ([FIPS 180-4]) over a framed message built from the
argument list. SHA-256 is already the workspace's committed hash (the
tamper-evident commit log, [ADR-0026](adr/0026-verifiable-audit-log.md)), so the
hash-key function adds no new cryptographic primitive.

## Message encoding

The hashed message is, in order:

```text
MAGIC = "STLHK1"           6 bytes, ASCII — "Stele Hash Key", spec version 1
argc                       u32, big-endian — the number of arguments
for each argument, in order:
    tag                    u8 — the type tag (table below)
    len                    u64, big-endian — length of body in bytes
    body                   len bytes — big-endian for fixed-width types
```

and the digest is `SHA-256(message)`. The SQL surface renders it as a **64-char
lowercase hex** string (`TEXT`).

### Type tags and bodies

| type        | tag    | body                                                        |
|-------------|--------|-------------------------------------------------------------|
| `NULL`      | `0x00` | *(empty)*                                                   |
| `BOOL`      | `0x01` | 1 byte: `0x00` false / `0x01` true                          |
| `INT4`      | `0x02` | 4-byte big-endian two's-complement `i32`                    |
| `INT8`      | `0x03` | 8-byte big-endian two's-complement `i64`                    |
| `TEXT`      | `0x04` | UTF-8 bytes, verbatim                                        |
| `TIMESTAMP` | `0x05` | 8-byte big-endian `i64`, microseconds since the Unix epoch (UTC) |
| `DATE`      | `0x06` | 4-byte big-endian `i32`, days since the Unix epoch          |
| `TIMESTAMPTZ` | `0x07` | 8-byte big-endian `i64`, microseconds since the Unix epoch (UTC) |
| `PERIOD`    | `0x08` | 16 bytes: two big-endian `i64` µs bounds, `from` then `to` (open upper = `i64::MAX`) |
| `UUID`      | `0x09` | the 16 raw UUID bytes, network order                        |
| `BYTEA`     | `0x0A` | the raw bytes, verbatim                                      |

Tags are frozen: a tag's meaning never changes, and a new type takes the next
free value rather than reusing one. `TIMESTAMPTZ` / `PERIOD` (STL-189 / STL-180)
and `UUID` / `BYTEA` (STL-181) were appended additively as those types joined the
logical type set; the original `0x00`–`0x06` encodings are unchanged.

### Design rationale

- **Length-prefix framing** makes argument concatenation injective: `hash('a',
  'b')` can never collide with `hash('ab')`. A zero-length body (empty `TEXT`) is
  distinct from `NULL` because the *tag* differs.
- **`argc` prefix** frames the arity, so a different number of arguments can never
  produce the same byte stream.
- **Type-tagged values** mean `hash(1::int4)` ≠ `hash(1::int8)`: the digest is
  over the *typed* value, and a client must encode with the same type tag. This is
  the safe, unambiguous choice over a "numeric-agnostic" hash, which would be a
  silent-collision footgun.
- **Big-endian** (network byte order) bodies are the portable convention, so a
  client encodes identical bytes regardless of its host architecture.
- **Magic prefix** versions the function and domain-separates a hash-key digest
  from a bare SHA-256 of the same logical bytes (e.g. the commit-log hash), so the
  two can never alias.

### v1 limitations (deliberate)

- **No Unicode normalization.** `TEXT` is hashed as its UTF-8 bytes verbatim, so
  `'café'` (NFC) and `'cafe\u{301}'` (NFD) hash differently. Normalizing would bake
  a Unicode version into a frozen format and pull in a Unicode-tables dependency.
  Callers that need normalized equality normalize **before** hashing. Revisited if
  a `v2` is cut.
- **SQL literal coverage.** Over the wire, `hash(...)` argument folding is
  type-inference-free, so it produces the literal shapes the v0.2 parser folds
  without a target type — string (`TEXT`), integer (`INT4`/`INT8`), boolean
  (`BOOL`), and `NULL`. The civil-time, range, and binary types (`TIMESTAMP`,
  `DATE`, `TIMESTAMPTZ`, `PERIOD`, `UUID`, `BYTEA`) are not auto-selected from a
  bare literal in a `hash(...)` call, but the spec defines every type's encoding
  so a client building keys directly is fully specified. The digest is returned
  as `TEXT` hex; the dedicated `UUID` / `BYTEA` scalar types landed in
  [STL-181](https://allegromusic.atlassian.net/browse/STL-181) (F21).

## SQL surface

```sql
SELECT hash('acme');                 -- one TEXT column named "hash"
SELECT hash('acme', 42, NULL) AS bk; -- a composite key, aliased
SELECT hash();                       -- the well-defined empty key
```

`hash(...)` over literal arguments is evaluated as a tableless constant (the same
path that answers `SELECT 1`). A `hash(col)` over a column reference is **not**
this surface — per-row hashing over a scan is separate work, not part of v1.

## Test vectors

These are the canonical determinism witnesses, asserted by the reference
implementation's `vectors_are_stable` test
([`stele_common::hashkey`](../crates/stele-common/src/hashkey.rs)). They are the
same data the code checks in; the two must not drift.

| arguments                       | SHA-256 digest (lowercase hex)                                     |
|---------------------------------|--------------------------------------------------------------------|
| *(no arguments)*                | `c3ae0194dddca9863f78e86574fa26d75441dd94643ea5ef3196058761db054f` |
| `NULL`                          | `11e5cc31f5ab758bacb047a87c2e7400b17e99508b39efa45bd05fb3486fcca0` |
| `''` (empty `TEXT`)             | `cd43236bcc33b39a3530894687eb75fd004687e3aebf79e9739864a576b80578` |
| `'acme'` (`TEXT`)               | `809cf7dd76e429f04505df3c8df09e59e8c081fdd404bfe1d6e330aee4c2f82e` |
| `42` (`INT4`)                   | `96d0d0ba3e2c2426333516658650326edc8ac00fb3225299a1b24e21c56cf0c9` |
| `42` (`INT8`)                   | `29a3cc3ace5b15675d46dc9e13804f0ab65feaf17dc14090b3d9ccdfba6c7061` |
| `true` (`BOOL`)                 | `4c7d2e3d4dd051d20ab6da771cf4b167e4014b08d8b2d5b4e6b92da26b890ba9` |
| `'acme'`, `42` (`INT4`), `NULL` | `61c9586983296ab2e403396f6ce20b01e2adc2514633fc63bb9d5a1e0ce85a20` |
| `1700000000000000` (`TIMESTAMPTZ`) | `8720f97303210b1ae946845bfaad4a52d062189d52e098b5b4b3278708b2db31` |
| `[10, 20)` (`PERIOD`)           | `35ccfb9906f082824c65f8e0ef866f4439cb8e4217a9ff5d3890d95fdff0379c` |
| `550e8400-…-446655440000` (`UUID`) | `8c149c5fb901c43282f7b67163b24a1f1de2bb3c72df46707a37b4bc4f0ae811` |
| `\xdeadbeef` (`BYTEA`)          | `81ed9d4be87550352ed909232a1b669bde775b2ae106ae2d892dc475bd043fc5` |
| `\x` (empty `BYTEA`)            | `79649e69f651affd02e15ab04eaae96fb60f72387989148d4fee9769cc1fa278` |

[FIPS 180-4]: https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
