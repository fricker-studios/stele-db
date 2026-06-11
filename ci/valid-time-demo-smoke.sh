#!/usr/bin/env bash
# Valid-time five-minute-path smoke test (STL-194).
#
# The valid-axis sibling of ci/identity-demo-smoke.sh. It stages a *valid-time*
# history entirely over the Postgres wire protocol — `INSERT`/`UPDATE` naming the
# period columns, which the binder lifts into the framed `[from, to)` interval —
# and asserts that a `FOR VALID_TIME AS OF` query resolves the right cell. This is
# the gap STL-164 documented: its end-to-end proof had to stage history in-process
# because the write side could not set a valid interval over SQL. It now can.
#
# Usage: ci/valid-time-demo-smoke.sh [host] [port]
#   defaults: localhost 5454
#
# Requires `psql` (libpq) on PATH. Exits non-zero — failing CI — on any mismatch
# or if the engine never accepts connections.
set -euo pipefail

HOST="${1:-localhost}"
PORT="${2:-5454}"
PSQL=(psql -h "$HOST" -p "$PORT" -d stele -tA -v ON_ERROR_STOP=1)

# --- wait for the engine to accept connections (cold container boot) ----------
ready=
for _ in $(seq 1 60); do
  if "${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[ -n "$ready" ] || {
  echo "FAIL: engine never became ready on ${HOST}:${PORT}" >&2
  exit 1
}

# --- stage a valid-time history over SQL --------------------------------------
# A valid-time table tracks a second axis: when a fact is true in the modeled
# world. Period bounds are written as the same microsecond instants a `FOR
# VALID_TIME AS OF` reads (integer micros), not civil-time literals.
"${PSQL[@]}" -c "DROP TABLE IF EXISTS account_vt"
"${PSQL[@]}" -c "CREATE TABLE account_vt (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) WITH SYSTEM VERSIONING VALID TIME (vf, vt)"

# Key 1: a closed valid window [10, 20).
"${PSQL[@]}" -c "INSERT INTO account_vt VALUES (1, 100, 10, 20)"
# Key 2: an open-ended window [50, +inf) — naming only the start bound opens it.
"${PSQL[@]}" -c "INSERT INTO account_vt (id, balance, vf) VALUES (2, 777, 50)"
# A valid-time UPDATE opens a *new* version with a new window [20, 30) — the
# read-modify-write reads the prior (framed) row, supersedes it on the system
# axis, and frames the new interval.
"${PSQL[@]}" -c "UPDATE account_vt SET balance = 250, vf = 20, vt = 30 WHERE id = 1"

# Each query projects exactly the single `balance` column for the matching row;
# `-tA` makes the whole single-column row the value. An empty result is "no
# version live on the valid axis at that instant".
asof() { "${PSQL[@]}" -c "SELECT balance FROM account_vt FOR VALID_TIME AS OF $1 WHERE id = $2"; }

# The headline: positive valid-time AS OF reads back what SQL wrote.
key1_in="$(asof 25 1)"      # inside the updated window [20, 30) -> 250
key1_out="$(asof 15 1)"     # the pre-update window [10, 20) was superseded -> none
key2_open="$(asof 1000000 2)" # far past the open window's start -> 777
key2_before="$(asof 49 2)"  # before the open window's start -> none

echo "key1@25=${key1_in:-<none>} key1@15=${key1_out:-<none>} key2@1e6=${key2_open:-<none>} key2@49=${key2_before:-<none>}"

fail=0
check() { # name actual expected
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1 is '${2:-<none>}', expected '${3:-<none>}'" >&2
    fail=1
  fi
}
check "key 1 inside the updated valid window" "$key1_in" "250"
check "key 1 at a superseded valid instant"   "$key1_out" ""
check "key 2 in its open-ended valid window"  "$key2_open" "777"
check "key 2 before its valid window starts"  "$key2_before" ""
[ "$fail" -eq 0 ] || exit 1

echo "PASS: valid-time DML round-trips over SQL (AS OF resolves the framed interval)"
