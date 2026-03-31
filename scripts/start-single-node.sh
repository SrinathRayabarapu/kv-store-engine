#!/usr/bin/env bash
# Start a single-node KV server (no Raft). Data under demo/data-single by default.
#
# This script only launches one background JVM and exits — it does not run PUT/GET demos.
# For narrated operations use: ./scripts/demo-single-node.sh
#
# Env overrides:
#   PORT       — client port (default 7777)
#   DATA_DIR   — Bitcask data directory
#   START_SINGLE_QUIET=1 — omit [start-single] progress lines
#
# PID and logs: scripts/run/single-node.{pid,log}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

say() {
  if [[ "${START_SINGLE_QUIET:-}" != "1" ]]; then
    echo "[start-single] $*"
  fi
}

PORT="${PORT:-7777}"
DATA_DIR="${DATA_DIR:-$(demo_root)/data-single}"
RUN_DIR="$SCRIPT_DIR/run"
PID_FILE="$RUN_DIR/single-node.pid"
LOG_FILE="$RUN_DIR/single-node.log"

mkdir -p "$DATA_DIR" "$RUN_DIR"
say "project root: $ROOT"
say "ensuring fat JAR..."
build_jar_if_missing
ensure_jar
JAR="$(jar_path)"
say "using $(basename "$JAR")"

if [[ -f "$PID_FILE" ]]; then
  old="$(cat "$PID_FILE")"
  if kill -0 "$old" 2>/dev/null; then
    echo "Single node already running (PID $old, port $PORT). Stop with: scripts/stop-single-node.sh"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

say "launching JVM — KV :$PORT  data $DATA_DIR"
nohup java -jar "$JAR" --port "$PORT" --data-dir "$DATA_DIR" >>"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
say "PID $(cat "$PID_FILE")  log $LOG_FILE"
say "waiting for 127.0.0.1:$PORT (up to 45s)..."
if ! wait_for_tcp_port 127.0.0.1 "$PORT" 45; then
  echo "Server failed to listen; see $LOG_FILE"
  exit 1
fi
say "KV port $PORT is accepting connections"
echo ""
echo "Single-node KV server started (PID $(cat "$PID_FILE"), port $PORT, data $DATA_DIR)"
echo "Log: $LOG_FILE"
echo ""
echo "This script only starts the server. For PUT/GET/RANGE demo:"
echo "  ./scripts/demo-single-node.sh"
echo "Or one-off client:"
echo "  ./scripts/kv_client.py --port $PORT put mykey hello"
echo "Stop server:"
echo "  ./scripts/stop-single-node.sh"
