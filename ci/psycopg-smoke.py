#!/usr/bin/env python3
"""psycopg driver gate (STL-184).

Proves a real Python Postgres driver — psycopg 3, the successor to psycopg2 —
runs a parameterized prepared query against Stele end-to-end: connect, create a
table, insert rows through `%s` placeholders, then execute a server-side
prepared `SELECT … WHERE id = %s` and assert the returned value. This is one
half of the v0.2 milestone exit criterion ("a JDBC/psycopg driver can run a
parameterized query"); the JDBC half is ci/jdbc-smoke.sh.

psycopg 3 drives the extended-query protocol exactly the way libpq does
(it links libpq via the `binary` extra): Parse / Bind / Describe / Execute,
with `prepare=True` forcing a *named* server-side prepared statement that is
re-executed with fresh parameters — the [STL-182] statement cache on the
server side.

Usage: ci/psycopg-smoke.py [host] [port] [sslmode]
  defaults: localhost 5454 disable (sslmode=require drives the STL-251 TLS leg)

Requires psycopg 3 (`pip install "psycopg[binary]"`); the CI job pins the
version. Exits non-zero — failing CI — on any mismatch or if the engine never
accepts connections.
"""

import sys
import time

import psycopg

HOST = sys.argv[1] if len(sys.argv) > 1 else "localhost"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5454
SSLMODE = sys.argv[3] if len(sys.argv) > 3 else "disable"


def connect_with_retry(deadline_s: float = 60.0) -> psycopg.Connection:
    """Wait for the engine to accept connections (cold container boot)."""
    deadline = time.monotonic() + deadline_s
    while True:
        try:
            return psycopg.connect(
                host=HOST,
                port=PORT,
                dbname="stele",
                user="stele",
                sslmode=SSLMODE,
                autocommit=True,
            )
        except psycopg.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def main() -> None:
    with connect_with_retry() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS driver_demo_psycopg")
        cur.execute(
            "CREATE TABLE driver_demo_psycopg (id INT PRIMARY KEY, label TEXT)"
            " WITH SYSTEM VERSIONING"
        )

        # Parameterized INSERT statements: psycopg converts `%s` to `$1`/`$2` and
        # binds the values over the wire — no client-side literal splicing.
        cur.execute("INSERT INTO driver_demo_psycopg VALUES (%s, %s)", (1, "alpha"))
        cur.execute("INSERT INTO driver_demo_psycopg VALUES (%s, %s)", (2, "beta"))

        # The exit-criterion query: a *prepared* parameterized SELECT.
        # `prepare=True` forces a named server-side prepared statement, so this
        # exercises Parse-once / Bind+Execute-many rather than the unnamed path.
        for wanted_id, wanted_label in ((2, "beta"), (1, "alpha")):
            cur.execute(
                "SELECT label FROM driver_demo_psycopg WHERE id = %s",
                (wanted_id,),
                prepare=True,
            )
            rows = cur.fetchall()
            if rows != [(wanted_label,)]:
                print(
                    f"FAIL: WHERE id = {wanted_id} returned {rows!r}, "
                    f"expected [({wanted_label!r},)]",
                    file=sys.stderr,
                )
                sys.exit(1)

    print(f"PASS: psycopg {psycopg.__version__} ran a parameterized prepared query")


if __name__ == "__main__":
    main()
