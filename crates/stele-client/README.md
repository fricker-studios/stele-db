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
`std::net::TcpStream`; there is **no async runtime and no HTTP framework** in your
dependency tree (only `serde` + `serde_json`).

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

> **TLS:** the admin gateway does not yet terminate TLS. Until it does, bind the
> ops listener to loopback or front it with a TLS-terminating proxy.

## Example

```rust,no_run
use stele_client::{Client, Config};

fn main() -> Result<(), stele_client::Error> {
    let client = Client::new(Config {
        host: "127.0.0.1".to_owned(),
        port: 9090, // the ops listener the HTTP/JSON gateway shares
        token: Some(std::env::var("STELE_ADMIN_TOKEN").unwrap_or_default()),
    });

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
