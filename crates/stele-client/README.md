# stele-client

Rust SDK for the [Stele](https://steledb.com) **admin / control-plane API**.

Stele is an append-only, bitemporal, audit-native analytical database. **SQL**
travels over the PostgreSQL wire protocol, so you use an existing Postgres driver
(`psycopg`, `pgx`, JDBC, …) for queries — Stele ships no SQL driver of its own.
Everything that is *not* SQL — health and status, backup and restore-plan, and
catalog / segment / version / commit-chain introspection — lives behind a
dedicated, versioned admin API. **`stele-client` is the typed client for that
surface.**

It is the shared substrate the `stele` CLI's admin tier, Studio, and the
Kubernetes operator build on (ADR-0016).

## Install

```toml
[dependencies]
stele-client = "0.3"
```

## Transport & footprint

The admin API offers two transports from one contract: typed **gRPC** and an
**HTTP/JSON gateway**. This crate speaks the HTTP/JSON gateway — the lighter
dependency footprint. Each call is one blocking request over a plain
`std::net::TcpStream` (or, with TLS, the same socket wrapped in `rustls`' blocking
`StreamOwned` adapter); there is **no async runtime and no HTTP framework** in your
dependency tree — body (de)serialization is `serde` + `serde_json`, and TLS reuses
the `rustls` the SQL surface already pins.

## Versioning

`stele-client` tracks the admin API's `v1alpha1` surface explicitly and follows
`0.x` SemVer (ADR-0014). Every route it calls is under `/v1alpha1/…`, and a
`v1beta1`/`v1` graduation of the API is a new client minor. Pre-1.0, minor
releases may break, and the client works against its own engine minor and one
back.

## Authentication

Every call carries a static bearer token (the server's `[admin] tokens` in
`stele.toml`). With no token configured the server rejects every request, so a
missing token is refused locally rather than spent on a round trip.

## TLS

Attach a `Tls` with `Config::with_tls` to dial the admin gateway over `https://`,
so the bearer token never crosses in cleartext off-loopback:

- `Tls::verify("/etc/stele/ca.pem")` — verify the server certificate against a CA
  bundle and the host name (libpq's `verify-full`). The authenticated posture.
- `Tls::encrypt()` — encrypt without verifying the server's identity (libpq's
  `require`): defeats eavesdropping, not an active man-in-the-middle.

Leave TLS unset for the loopback / TLS-terminating-proxy deployments the gateway
has always served in plaintext.

## Example

```rust,no_run
use stele_client::{Client, Config, Tls};

fn main() -> Result<(), stele_client::Error> {
    let client = Client::new(
        Config::new(
            "stele.internal",
            9090, // the ops listener the HTTP/JSON gateway shares
            // A missing or empty env var becomes `None`, so an unconfigured token is
            // refused locally (`Error::NoToken`) rather than spent on a 401 round-trip.
            std::env::var("STELE_ADMIN_TOKEN").ok().filter(|t| !t.is_empty()),
        )
        // Encrypted and authenticated against the gateway's CA bundle.
        .with_tls(Tls::verify("/etc/stele/ca.pem")),
    );

    // Liveness, then engine state.
    assert!(client.health()?.is_serving());
    let status = client.status()?;
    println!("stele {} · {} tables", status.server_version, status.table_count);

    // Trigger a consistent online backup, then validate it without applying.
    let manifest = client.backup("/var/lib/stele/backups/snap1")?;
    println!("backed up {} files", manifest.file_count);
    let plan = client.restore_plan("/var/lib/stele/backups/snap1")?;
    assert!(plan.valid);

    // Introspection: per-table segment + zone-map metadata.
    let segments = client.segments("account")?;
    println!("{} segments", segments.rows.len());
    Ok(())
}
```

## License

Business Source License 1.1 (`BUSL-1.1`). See [LICENSE](../../LICENSE).
