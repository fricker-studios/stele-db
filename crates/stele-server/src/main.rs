//! `stele-server` binary — the engine daemon.

use std::net::SocketAddr;

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
    #[arg(long, default_value_t = default_listen())]
    listen: SocketAddr,

    /// Dev mode: verbose tracing, no auth, scratch storage. Never enable in production.
    #[arg(long, default_value_t = true)]
    dev: bool,
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
    stele_server::run(stele_server::Config {
        listen: args.listen,
        dev: args.dev,
    })
    .await
}
