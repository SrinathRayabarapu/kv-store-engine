#!/usr/bin/env bash
# Shared helpers for KV store demo scripts. Source from other scripts:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
#   source "$SCRIPT_DIR/lib/common.sh"

set -euo pipefail

demo_root() {
  echo "${ROOT:?ROOT must be set}/demo"
}

jar_path() {
  echo "${ROOT:?}/target/kv-store-engine-1.0.0.jar"
}

ensure_jar() {
  local j
  j="$(jar_path)"
  if [[ ! -f "$j" ]]; then
    echo "JAR not found: $j — run: (cd \"$ROOT\" && mvn -q -DskipTests package)"
    exit 1
  fi
}

build_jar_if_missing() {
  local j
  j="$(jar_path)"
  if [[ ! -f "$j" ]]; then
    echo "Building fat JAR (mvn -q -DskipTests package)..."
    (cd "$ROOT" && mvn -q -DskipTests package)
  fi
}

wait_for_tcp_port() {
  local host="$1" port="$2" seconds="${3:-45}"
  local i=0
  while (( i < seconds * 10 )); do
    if (echo >/dev/tcp/"$host"/"$port") 2>/dev/null; then
      return 0
    fi
    sleep 0.1
    ((i++)) || true
  done
  echo "Timeout waiting for $host:$port"
  return 1
}

kv_client() {
  python3 "$SCRIPT_DIR/kv_client.py" "$@"
}
