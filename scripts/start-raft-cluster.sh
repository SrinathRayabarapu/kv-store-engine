#!/usr/bin/env bash
# Start a 3-node Raft cluster (three JVMs). Matches README peer wiring.
#
# Default client ports: 7777, 7778, 7779 — raft: 9777, 9778, 9779
# Data: demo/data-n1, demo/data-n2, demo/data-n3
#
# PID files: scripts/run/raft-node-{1,2,3}.pid
# Logs:     scripts/run/raft-node-{1,2,3}.log

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

HOST="${HOST:-127.0.0.1}"
BASE="$(demo_root)"
RUN_DIR="$SCRIPT_DIR/run"
mkdir -p "$RUN_DIR"

build_jar_if_missing
ensure_jar
JAR="$(jar_path)"

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
    echo "Node 1 already running."
    return 0
  fi
  rm -f "$pidf"
  mkdir -p "$data"
  nohup java -jar "$JAR" \
    --node-id 1 --port "$P1" --raft-port "$R1" --data-dir "$data" \
    --advertise-host "$HOST" \
    --peer "2@${HOST}:${R2}:${P2}" \
    --peer "3@${HOST}:${R3}:${P3}" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
}

start_node2() {
  local data="$BASE/data-n2"
  local pidf="$RUN_DIR/raft-node-2.pid"
  local logf="$RUN_DIR/raft-node-2.log"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    echo "Node 2 already running."
    return 0
  fi
  rm -f "$pidf"
  mkdir -p "$data"
  nohup java -jar "$JAR" \
    --node-id 2 --port "$P2" --raft-port "$R2" --data-dir "$data" \
    --advertise-host "$HOST" \
    --peer "1@${HOST}:${R1}:${P1}" \
    --peer "3@${HOST}:${R3}:${P3}" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
}

start_node3() {
  local data="$BASE/data-n3"
  local pidf="$RUN_DIR/raft-node-3.pid"
  local logf="$RUN_DIR/raft-node-3.log"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    echo "Node 3 already running."
    return 0
  fi
  rm -f "$pidf"
  mkdir -p "$data"
  nohup java -jar "$JAR" \
    --node-id 3 --port "$P3" --raft-port "$R3" --data-dir "$data" \
    --advertise-host "$HOST" \
    --peer "1@${HOST}:${R1}:${P1}" \
    --peer "2@${HOST}:${R2}:${P2}" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
}

start_node1
start_node2
start_node3

for p in "$P1" "$P2" "$P3"; do
  wait_for_tcp_port 127.0.0.1 "$p" 60 || exit 1
done

echo "Raft cluster started (3 JVMs)."
echo "  Node 1  KV :$P1  Raft :$R1  data $BASE/data-n1"
echo "  Node 2  KV :$P2  Raft :$R2  data $BASE/data-n2"
echo "  Node 3  KV :$P3  Raft :$R3  data $BASE/data-n3"
echo "Logs under $RUN_DIR/raft-node-*.log"
echo "Allow ~3–5s for leader election before writes."
