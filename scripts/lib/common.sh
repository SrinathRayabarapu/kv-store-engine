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

# Prefix lines for panel-friendly narration (stdout).
demo_log() {
  printf '[demo] %s\n' "$*"
}

demo_section() {
  printf '\n[demo] ---------- %s ----------\n' "$*"
}

# Set DEMO_VERBOSE=1 to print per-hop TCP / REDIRECT trace from kv_client (stderr).
kv_client() {
  if [[ "${DEMO_VERBOSE:-}" == "1" ]]; then
    python3 "$SCRIPT_DIR/kv_client.py" -v "$@"
  else
    python3 "$SCRIPT_DIR/kv_client.py" "$@"
  fi
}

# Parse KEY=value lines after "---MACHINE---" from `kv_client probe --machine-trailer` output.
# Sets: RAFT_PROBE_OK RAFT_LEADER_NODE_ID RAFT_LEADER_KV_PORT RAFT_FOLLOWER_KV_PORTS
#       RAFT_FIRST_FOLLOWER_KV_PORT RAFT_SECOND_FOLLOWER_KV_PORT (and empty RAFT_PROBE_REASON on success).
raft_load_machine_trailer() {
  local text="$1"
  RAFT_PROBE_OK=
  RAFT_PROBE_REASON=
  RAFT_LEADER_NODE_ID=
  RAFT_LEADER_KV_PORT=
  RAFT_FOLLOWER_KV_PORTS=
  RAFT_FIRST_FOLLOWER_KV_PORT=
  RAFT_SECOND_FOLLOWER_KV_PORT=
  local in_machine=0
  local line k v
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == "---MACHINE---" ]]; then
      in_machine=1
      continue
    fi
    [[ "$in_machine" -ne 1 ]] && continue
    [[ -z "$line" ]] && continue
    k="${line%%=*}"
    v="${line#*=}"
    case "$k" in
      RAFT_PROBE_OK) RAFT_PROBE_OK="$v" ;;
      RAFT_PROBE_REASON) RAFT_PROBE_REASON="$v" ;;
      RAFT_LEADER_NODE_ID) RAFT_LEADER_NODE_ID="$v" ;;
      RAFT_LEADER_KV_PORT) RAFT_LEADER_KV_PORT="$v" ;;
      RAFT_FOLLOWER_KV_PORTS) RAFT_FOLLOWER_KV_PORTS="$v" ;;
      RAFT_FIRST_FOLLOWER_KV_PORT) RAFT_FIRST_FOLLOWER_KV_PORT="$v" ;;
      RAFT_SECOND_FOLLOWER_KV_PORT) RAFT_SECOND_FOLLOWER_KV_PORT="$v" ;;
    esac
  done <<< "$text"
}

# Grep recent RaftNode election / role lines from per-node JVM logs.
raft_jvm_log_digest() {
  local run_dir="${1:-$SCRIPT_DIR/run}"
  local n="${2:-15}"
  demo_section "JVM logs (Raft state transitions — timestamped)"
  local id f
  for id in 1 2 3; do
    f="$run_dir/raft-node-${id}.log"
    if [[ ! -f "$f" ]]; then
      demo_log "raft-node-${id}.log missing (skip)"
      continue
    fi
    demo_log "file=$f (tail 200 lines, last ~${n} Raft events)"
    local excerpt
    excerpt=$(tail -n 200 "$f" 2>/dev/null \
      | grep -E "started as FOLLOWER|became LEADER|starting election|stepped down|granted vote|denied vote|recognized .* as LEADER|advanced commitIndex|Raft cluster member" 2>/dev/null \
      | tail -n "$n" || true)
    if [[ -z "${excerpt//[$'\t\r\n ']/}" ]]; then
      demo_log "  (no matching Raft lines in this file yet)"
      continue
    fi
    while IFS= read -r line; do
      [[ -z "${line// }" ]] && continue
      demo_log "  (node${id}) $line"
    done <<< "$excerpt"
  done
}
