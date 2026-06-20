# ADR-0033 — SASLprep password normalization: depend on `stringprep`, do not vendor

- **Status:** Accepted
- **Date:** 2026-06-19
- **Deciders:** Project owner + systems design
- **Related:** [10 §5](../10-security-and-compliance.md#5-authentication) · [ADR-0010](0010-deterministic-simulation-testing.md) · [ADR-0018](0018-security-auditability-pillar.md) · STL-298 / STL-252 / STL-296

## Context

SCRAM-SHA-256 ([STL-252]) derives a verifier from the password and checks a
client's proof against it; for that to work the two sides must agree on the
*bytes* that go into `Hi(password, salt, i)`. Postgres applies **SASLprep**
([RFC 4013] — the stringprep profile for passwords: map non-ASCII spaces to
U+0020, drop "mapped to nothing" code points, **NFKC**-normalize, reject a
prohibited control/bidi/unassigned set) before hashing, and falls back to the
raw UTF-8 bytes when SASLprep fails. SCRAM clients normalize the same way before
computing their proof ([RFC 5802] §5.1, `SaltedPassword := Hi(Normalize(pw),…)`).

STL-252 shipped a deliberate v0.3 floor: **raw UTF-8 bytes, unconditionally**.
ASCII passwords — the overwhelming case — are byte-identical under SASLprep, so
the floor interoperates with Postgres clients for ASCII. But a non-ASCII
password that a client normalizes (a client sending NFKC-composed `é` against a
verifier we derived from a decomposed `é`, or a mapped non-ASCII space) fails to
authenticate against a raw-bytes verifier. STL-298 closes that gap.

The rest of `stele_common::scram` is **vendored on purpose** (HMAC, PBKDF2,
base64, the SHA-256 it builds on): each are small, fully specified, pinned to
published RFC test vectors, and the authentication path is exactly where a
hidden transitive dependency is least welcome. SASLprep does not fit that mold.
NFKC and the RFC 3454 prohibited-character classes are **large Unicode tables**
(hundreds of KB of decomposition/composition and property data) that track the
Unicode version; hand-vendoring them is neither "small" nor "fully specified by
a short RFC", and a stale hand-copied table is a correctness *and* a security
bug (a character we fail to fold or fail to prohibit). Alternatives considered:

- **(a) Vendor the SASLprep tables.** Rejected: the table volume defeats the
  "small, auditable, pinned-to-vectors" rationale that justifies vendoring the
  rest of the module, and we would own Unicode-version maintenance forever.
- **(b) A minimal hand-rolled NFC/NFKC over a curated subset.** Rejected:
  "curated subset" is precisely how a normalization bug ships — divergence from
  Postgres on some code point we did not anticipate, silently breaking auth.
- **(c) Depend on `stringprep`.** The ecosystem's narrow, single-purpose crate
  (it is what `postgres-protocol` uses); already present in our lock as a
  dev-transitive. Its `saslprep` matches Postgres's profile, including the
  ASCII zero-alloc fast path.

## Decision

**We take a direct dependency on the `stringprep` crate for SASLprep, rather
than vendoring Unicode normalization. `stele_common::scram::prepare_password`
runs `stringprep::saslprep` and falls back to the raw UTF-8 bytes on error
(Postgres's behavior); both password-ingesting paths — `ScramVerifier::derive`
(the `CREATE`/`ALTER USER` verifier) and `client_proof` (the client proof, the
seam STL-296 will use) — route through it, so the two sides always normalize
identically.**

- **Placement.** The dependency lives in `stele-common`, the workspace
  dependency root, because both derivation sites already go through this crate
  and a single shared `prepare_password` is the only way to guarantee the two
  sides cannot diverge. This is the correctness-decisive property: SCRAM breaks
  the instant the verifier side and the proof side normalize differently.
- **Fallback semantics.** On `Err` (prohibited / bidi / unassigned input) we
  hash the raw bytes, exactly as Postgres's `pg_saslprep` caller does. Because
  both sides fall back the same way, an un-preppable password still
  authenticates against itself; what normalization buys is cross-form interop.
- **Runtime-agnostic core is preserved.** `stringprep` is pure, deterministic,
  and free of I/O, threads, clocks, and global state, so it does not weaken the
  deterministic-simulation invariant ([ADR-0010]) — the constraint `stele-common`
  enforces is "no async runtime / no I/O / no global state", which this honors.
- **Supply chain.** `stringprep` (MIT) pulls `unicode-normalization`,
  `unicode-bidi`, and `unicode-properties` — all MIT / Apache-2.0 / Unicode,
  every license already in the `deny.toml` allow-list, every crate already in
  the lock. No new license allowance is required; `cargo deny check` stays green.

## Consequences

### Positive
- Non-ASCII passwords interoperate with real Postgres/libpq, psycopg, and JDBC
  SCRAM clients, which all SASLprep — the documented STL-252 gap closes.
- Correctness comes from a maintained, Unicode-version-tracking crate instead of
  a hand-copied table we would have to chase Unicode releases to keep current.
- One shared `prepare_password` makes "both sides normalize identically"
  structural, not a convention each call site must remember.

### Negative / costs
- The first third-party crate in the otherwise-vendored auth path, and three new
  (transitive) crates in the dependency root's tree — a real supply-chain surface
  this ADR accepts deliberately, bounded to a single, widely-used, pure crate.
- The storage/txn core links the Unicode tables transitively via `stele-common`
  even though it never authenticates; the tables are data, not runtime surface.
- We inherit `stringprep`'s reading of the RFC. Any divergence from Postgres on
  an exotic code point is now its bug to fix, not ours — acceptable, and far less
  likely than a divergence in a table we maintained by hand.

### Neutral / follow-ups
- `SCRAM-SHA-256-PLUS` channel binding and client-side SCRAM in `stele shell`
  ([STL-296]) remain the separately-filed v0.3 floors in [10 §5]; STL-296 reuses
  `prepare_password` for free through `client_proof`.
- If the vendoring posture is ever revisited (e.g. a `no_std` core that cannot
  link `stringprep`), gating the `scram` module — and this dependency — behind a
  crate feature is the natural lever; out of scope here.

[RFC 4013]: https://www.rfc-editor.org/rfc/rfc4013
[RFC 5802]: https://www.rfc-editor.org/rfc/rfc5802
[10 §5]: ../10-security-and-compliance.md#5-authentication
[ADR-0010]: 0010-deterministic-simulation-testing.md
[STL-252]: https://allegromusic.atlassian.net/browse/STL-252
[STL-296]: https://allegromusic.atlassian.net/browse/STL-296
