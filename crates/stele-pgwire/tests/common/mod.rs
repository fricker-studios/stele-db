//! Shared helpers for the pgwire integration tests (`tokio_postgres_crud`,
//! `psql_golden`).
//!
//! Kept under `tests/common/` (a module dir, not a top-level `tests/*.rs`) so
//! Cargo does not compile it as its own test binary — it is pulled into each
//! suite with `mod common;`.

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

/// The libpq connection string for a Stele pgwire server at `addr`.
///
/// `sslmode=disable` skips negotiation (the server refuses SSL anyway); v0.1 has
/// no auth, so any user/dbname is accepted.
pub fn conn_str(addr: SocketAddr) -> String {
    format!(
        "host=127.0.0.1 port={} user=stele dbname=stele sslmode=disable",
        addr.port()
    )
}
