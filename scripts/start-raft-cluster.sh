#!/usr/bin/env bash
# Start a 3-node Raft cluster (three JVMs). Matches README peer wiring.
#
# This script only launches background processes and exits — same idea as
# start-single-node.sh. For probe + PUT/GET/RANGE demos, run:
#   ./scripts/demo-raft-cluster.sh
#
# Default client ports: 7777, 7778, 7779 — raft: 9777, 9778, 9779
# Data: demo/data-n1, demo/data-n2, demo/data-n3
#
# PID files: scripts/run/raft-node-{1,2,3}.pid
# Logs:     scripts/run/raft-node-{1,2,3}.log
#
# Set START_RAFT_QUIET=1 to suppress step-by-step [start-raft] lines.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

say() {
  if [[ "${START_RAFT_QUIET:-}" != "1" ]]; then
    echo "[start-raft] $*"
  fi
}

HOST="${HOST:-127.0.0.1}"
BASE="$(demo_root)"
RUN_DIR="$SCRIPT_DIR/run"
mkdir -p "$RUN_DIR"

say "project root: $ROOT"
say "ensuring fat JAR..."
build_jar_if_missing
ensure_jar
JAR="$(jar_path)"
say "using $(basename "$JAR")"

P1=7777
R1=9777
P2=7778
R2=9778
P3=7779
R3=9779

# Each node lists the other two peers (id@host:raftPort:clientPort).
start_node1() {
  local data="$BASE/data-n1"
  local pidf="$RUN_DIR/raft-node-1.pid"
  local logf="$RUN_DIR/raft-node-1.log"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    say "node 1 already running (PID $(cat "$pidf")); skip launch"
    return 0
  fi
  rm -f "$pidf"
  mkdir -p "$data"
  say "launching node 1 — KV :$P1  Raft :$R1  data $data"
  nohup java -jar "$JAR" \
    --node-id 1 --port "$P1" --raft-port "$R1" --data-dir "$data" \
    --advertise-host "$HOST" \
    --peer "2@${HOST}:${R2}:${P2}" \
    --peer "3@${HOST}:${R3}:${P3}" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
  say "node 1 JVM PID $(cat "$pidf")  log $logf"
}

start_node2() {
  local data="$BASE/data-n2"
  local pidf="$RUN_DIR/raft-node-2.pid"
  local logf="$RUN_DIR/raft-node-2.log"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    say "node 2 already running (PID $(cat "$pidf")); skip launch"
    return 0
  fi
  rm -f "$pidf"
  mkdir -p "$data"
  say "launching node 2 — KV :$P2  Raft :$R2  data $data"
  nohup java -jar "$JAR" \
    --node-id 2 --port "$P2" --raft-port "$R2" --data-dir "$data" \
    --advertise-host "$HOST" \
    --peer "1@${HOST}:${R1}:${P1}" \
    --peer "3@${HOST}:${R3}:${P3}" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
  say "node 2 JVM PID $(cat "$pidf")  log $logf"
}

start_node3() {
  local data="$BASE/data-n3"
  local pidf="$RUN_DIR/raft-node-3.pid"
  local logf="$RUN_DIR/raft-node-3.log"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    say "node 3 already running (PID $(cat "$pidf")); skip launch"
    return 0
  fi
  rm -f "$pidf"
  mkdir -p "$data"
  say "launching node 3 — KV :$P3  Raft :$R3  data $data"
  nohup java -jar "$JAR" \
    --node-id 3 --port "$P3" --raft-port "$R3" --data-dir "$data" \
    --advertise-host "$HOST" \
    --peer "1@${HOST}:${R1}:${P1}" \
    --peer "2@${HOST}:${R2}:${P2}" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
  say "node 3 JVM PID $(cat "$pidf")  log $logf"
}

say "starting three background JVMs (shell will return when KV ports accept TCP)..."
start_node1
start_node2
start_node3

say "waiting for KV client ports 127.0.0.1:$P1 $P2 $P3 (up to 60s each)..."
for p in "$P1" "$P2" "$P3"; do
  if ! wait_for_tcp_port 127.0.0.1 "$p" 60; then
    say "ERROR: port $p did not open — see $RUN_DIR/raft-node-*.log"
    exit 1
  fi
  say "KV port $p is accepting connections"
done

echo ""
echo "Raft cluster started (3 JVMs)."
echo "  Node 1  KV :$P1  Raft :$R1  data $BASE/data-n1"
echo "  Node 2  KV :$P2  Raft :$R2  data $BASE/data-n2"
echo "  Node 3  KV :$P3  Raft :$R3  data $BASE/data-n3"
echo "Logs under $RUN_DIR/raft-node-*.log"
echo "Allow ~3–5s for leader election before writes."
echo ""
echo "This script only starts servers (same as start-single-node.sh). For probe + operations demo:"
echo "  ./scripts/demo-raft-cluster.sh"
echo "Or probe roles only:"
echo "  ./scripts/kv_client.py --host 127.0.0.1 probe --kv-ports ${P1},${P2},${P3}"
echo "Stop cluster:"
echo "  ./scripts/stop-raft-cluster.sh"
