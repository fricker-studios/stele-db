//! `stele-server` binary — the engine daemon.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use stele_common::DEFAULT_PG_PORT;

#[derive(Parser, Debug)]
#[command(
    name = "stele-server",
    version,
    about = "Stele engine daemon. Speaks the Postgres wire protocol on :5454 by default."
)]
struct Args {
    /// Listen address for pg-wire. Default: 0.0.0.0:5454 ([ADR-0017]).
    /// Ignored when `--config` is given (the file's value wins).
    #[arg(long, default_value_t = default_listen())]
    listen: SocketAddr,

    /// Dev mode: verbose tracing, no auth, scratch storage. Never enable in production.
    /// Ignored when `--config` is given — a config file always runs in non-dev mode.
    #[arg(long, default_value_t = true)]
    dev: bool,

    /// Path to a `stele.toml`. When set, configuration (including the
    /// `[storage] backend`) comes from the file instead of dev defaults.
    #[arg(long)]
    config: Option<PathBuf>,
}

const fn default_listen() -> SocketAddr {
    SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        DEFAULT_PG_PORT,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = if let Some(path) = args.config {
        stele_server::Config::load(path)?
    } else {
        let mut cfg = stele_server::Config::dev();
        cfg.listen = args.listen;
        cfg.dev = args.dev;
        cfg
    };
    stele_server::run(cfg).await
}
