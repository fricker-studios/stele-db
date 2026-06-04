//! Library surface for the engine daemon.
//!
//! Kept thin: the `main` binary parses args and invokes [`run`].
//! `stele-cli` depends on this crate so that `stele server …` can dispatch the
//! same code path as `stele-server` directly.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Context as _;
use stele_common::DEFAULT_PG_PORT;
use stele_pgwire::Server as PgServer;
use tokio::signal;
use tracing::info;

/// Engine configuration. Plenty more knobs will land — keep this struct as the
/// single point everyone reads from.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub listen: SocketAddr,
    pub dev: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_PG_PORT),
            dev: true,
        }
    }
}

/// Boot the engine: install tracing, start the pgwire listener, wait for SIGINT.
pub async fn run(cfg: Config) -> anyhow::Result<()> {
    init_tracing(cfg.dev);
    info!(?cfg, "stele-server: starting");

    let pg = PgServer::new(cfg.listen);

    tokio::select! {
        res = pg.run() => res.context("pgwire listener exited")?,
        _ = signal::ctrl_c() => {
            info!("received SIGINT, shutting down");
        }
    }
    Ok(())
}

fn init_tracing(dev: bool) {
    use tracing_subscriber::{EnvFilter, fmt};

    let default_filter = if dev { "info,stele=debug" } else { "info" };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let _ = fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .try_init();
}
