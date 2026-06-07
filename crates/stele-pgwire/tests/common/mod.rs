//! Shared helpers for the pgwire integration tests (`tokio_postgres_crud`,
//! `psql_golden`).
//!
//! Kept under `tests/common/` (a module dir, not a top-level `tests/*.rs`) so
//! Cargo does not compile it as its own test binary — it is pulled into each
//! suite with `mod common;`.

use std::net::SocketAddr;
use std::time::Duration;

use stele_pgwire::{Server, SharedSession};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

/// Start a [`Server`] over `session` on an ephemeral port and return its address,
/// once it accepts a full connection.
///
/// Reserves a free port with a throwaway bind, drops it, and hands the address to
/// the real server. The race this carries (another process can grab the port
/// before the server re-binds) is tracked by STL-152, which will let the server
/// report its own bound address — at which point this dance goes away for both
/// callers at once.
///
/// Readiness is probed by completing a **real Postgres startup handshake**
/// (`tokio_postgres::connect`), not a bare `TcpStream::connect`: a raw TCP connect
/// that is immediately dropped leaves the server's handler staring at EOF mid
/// startup framing (a spurious warning during test setup). The probe connection
/// is closed cleanly before returning.
pub async fn spawn_server(session: SharedSession) -> SocketAddr {
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reserved.local_addr().unwrap();
    drop(reserved);

    tokio::spawn(Server::new(addr, session).run());

    for _ in 0..200 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str(addr), NoTls).await {
            // Drive the connection just long enough to close it cleanly (a plain
            // Terminate), so the readiness probe leaves no half-open socket.
            let driver = tokio::spawn(connection);
            drop(client);
            let _ = driver.await;
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server did not come up on {addr} within the retry budget");
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
