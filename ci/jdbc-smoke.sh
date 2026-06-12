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
if [ ! -f "$JAR" ]; then
  curl -fsSL -o "$JAR" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PGJDBC_VERSION}/postgresql-${PGJDBC_VERSION}.jar"
fi

# `shasum -a 256` exists on both the GitHub ubuntu runners and macOS dev boxes
# (sha256sum does not ship with macOS).
echo "${PGJDBC_SHA256}  ${JAR}" | shasum -a 256 -c - >/dev/null || {
  echo "FAIL: pgjdbc jar checksum mismatch (expected ${PGJDBC_SHA256})" >&2
  exit 1
}

exec java -cp "$JAR" "$(dirname "$0")/JdbcSmoke.java" "$HOST" "$PORT"
