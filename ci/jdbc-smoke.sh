#!/usr/bin/env bash
# JDBC driver gate (STL-184) — wrapper for ci/JdbcSmoke.java.
#
# Fetches the official PostgreSQL JDBC driver (pgjdbc) at a pinned version from
# Maven Central, verifies its SHA-256 (the jar is an unpinned-by-git network
# fetch, so it gets the same checksum treatment ADR-0005 gives actions), then
# runs the single-file JdbcSmoke program against a running Stele engine.
#
# Usage: ci/jdbc-smoke.sh [host] [port]
#   defaults: localhost 5454
#
# Requires `java` 11+ (single-file source launch) and `curl` on PATH. Set
# PGJDBC_JAR to an already-downloaded jar to skip the fetch (it is still
# checksum-verified). Exits non-zero — failing CI — on any mismatch.
set -euo pipefail

HOST="${1:-localhost}"
PORT="${2:-5454}"

# Bump the version and checksum together. The checksum is the SHA-256 of
# https://repo1.maven.org/maven2/org/postgresql/postgresql/${PGJDBC_VERSION}/…jar,
# cross-checked against Maven Central's published .sha1 at pin time.
PGJDBC_VERSION="42.7.7"
PGJDBC_SHA256="157963d60ae66d607e09466e8c0cdf8087e9cb20d0159899ffca96bca2528460"

JAR="${PGJDBC_JAR:-${TMPDIR:-/tmp}/postgresql-${PGJDBC_VERSION}.jar}"
JAR_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${PGJDBC_VERSION}/postgresql-${PGJDBC_VERSION}.jar"

# Atomic fetch: download (with retries) to a temp file and rename into place,
# so an interrupted transfer never leaves a partial jar at $JAR.
fetch_jar() {
  local tmp
  tmp="$(mktemp "${JAR}.XXXXXX")"
  curl -fsSL --retry 3 --retry-delay 2 -o "$tmp" "$JAR_URL" || {
    rm -f "$tmp"
    return 1
  }
  mv "$tmp" "$JAR"
}

# `shasum -a 256` exists on both the GitHub ubuntu runners and macOS dev boxes
# (sha256sum does not ship with macOS).
verify_jar() {
  echo "${PGJDBC_SHA256}  ${JAR}" | shasum -a 256 -c - >/dev/null 2>&1
}

[ -f "$JAR" ] || fetch_jar
if ! verify_jar; then
  if [ -n "${PGJDBC_JAR:-}" ]; then
    # A caller-supplied jar is never replaced behind the caller's back.
    echo "FAIL: pgjdbc jar checksum mismatch (expected ${PGJDBC_SHA256})" >&2
    exit 1
  fi
  # A stale or corrupt cached download (e.g. an interrupted earlier fetch):
  # replace it once and re-verify before failing.
  rm -f "$JAR"
  fetch_jar
  verify_jar || {
    echo "FAIL: pgjdbc jar checksum mismatch after re-download (expected ${PGJDBC_SHA256})" >&2
    exit 1
  }
fi

exec java -cp "$JAR" "$(dirname "$0")/JdbcSmoke.java" "$HOST" "$PORT"
