#!/usr/bin/env bash
# Five-minute-path smoke test (STL-112).
#
# Drives the four-statement identity demo from docs/05-dev-environment.md against
# a running Stele engine over the Postgres wire protocol and asserts the headline
# promise: a `FOR SYSTEM_TIME AS OF` query time-travels to the balance *before*
# the update (100), while the live read sees the updated value (250).
#
# Usage: ci/identity-demo-smoke.sh [host] [port]
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

# --- the four-statement identity demo ----------------------------------------
"${PSQL[@]}" -c "DROP TABLE IF EXISTS account"
"${PSQL[@]}" -c "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING"
"${PSQL[@]}" -c "INSERT INTO account VALUES (1, 100)"

# The AS OF instant is `now() - interval '1 second'`. Sleep past that window so
# the snapshot lands strictly *between* the INSERT and the UPDATE — without this
# pause a sub-second demo resolves to before the row existed and returns nothing.
# This is what makes the time-travel assertion deterministic rather than racy.
sleep 2

"${PSQL[@]}" -c "UPDATE account SET balance = 250 WHERE id = 1"

# `SELECT balance` projects the (key, payload) pair = (id, balance) in v0.1, so
# the balance is the last `|`-separated field of the row (`1|100` -> `100`).
live_row="$("${PSQL[@]}" -c "SELECT balance FROM account WHERE id = 1")"
live_balance="${live_row##*|}"

asof_row="$("${PSQL[@]}" -c "SELECT balance FROM account FOR SYSTEM_TIME AS OF (now() - interval '1 second') WHERE id = 1")"
asof_balance="${asof_row##*|}"

echo "live balance = ${live_balance:-<none>} ; AS OF balance = ${asof_balance:-<none>}"

fail=0
if [ "$asof_balance" != "100" ]; then
  echo "FAIL: AS OF balance is '${asof_balance:-<none>}', expected 100 (the pre-update value)" >&2
  fail=1
fi
if [ "$live_balance" != "250" ]; then
  echo "FAIL: live balance is '${live_balance:-<none>}', expected 250 (the updated value)" >&2
  fail=1
fi
[ "$fail" -eq 0 ] || exit 1

echo "PASS: five-minute identity demo time-travels correctly (AS OF=100, live=250)"
