//! Shared helpers for the pgwire integration tests (`tokio_postgres_crud`,
//! `psql_golden`).
//!
//! Kept under `tests/common/` (a module dir, not a top-level `tests/*.rs`) so
//! Cargo does not compile it as its own test binary — it is pulled into each
//! suite with `mod common;`.

// Each suite pulls in the whole module but uses only the helpers it needs (a
// raw-socket suite spawns a server without `conn_str`, for instance), so an
// unused helper in some binaries is expected rather than dead code.
#![allow(dead_code)]

use std::net::SocketAddr;

use stele_pgwire::{Server, SharedSession};

/// Start a [`Server`] over `session` on an ephemeral port and return its address.
///
/// Binds the listen socket up front (STL-152) and reports the real bound address
/// before spawning the accept loop, so there is **no** reserve-drop window and no
/// connect-retry: the listener already accepts into its backlog, so the caller can
/// connect immediately on the returned address.
pub async fn spawn_server(session: SharedSession) -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let bound = Server::new(addr, session)
        .bind()
        .await
        .expect("bind ephemeral port");
    let addr = bound.local_addr();
    tokio::spawn(bound.serve());
    addr
}

/// The libpq connection string for a Stele pgwire server at `addr`, connecting as
/// the conventional `stele` user.
///
/// `sslmode=disable` skips negotiation (the server refuses SSL anyway); under
/// `trust` (the test default) any user/dbname is accepted.
pub fn conn_str(addr: SocketAddr) -> String {
    conn_str_as(addr, "stele")
}

/// As [`conn_str`], but connecting as `user` — the startup-message identity the
/// server stamps as the connection's write principal under `trust` ([STL-300]).
pub fn conn_str_as(addr: SocketAddr, user: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user={user} dbname=stele sslmode=disable",
        addr.port()
    )
}
