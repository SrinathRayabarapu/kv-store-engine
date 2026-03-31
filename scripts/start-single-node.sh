#!/usr/bin/env bash
# Start a single-node KV server (no Raft). Data under demo/data-single by default.
#
# Env overrides:
#   PORT       — client port (default 7777)
#   DATA_DIR   — Bitcask data directory
#
# PID and logs: scripts/run/single-node.{pid,log}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

PORT="${PORT:-7777}"
DATA_DIR="${DATA_DIR:-$(demo_root)/data-single}"
RUN_DIR="$SCRIPT_DIR/run"
PID_FILE="$RUN_DIR/single-node.pid"
LOG_FILE="$RUN_DIR/single-node.log"

mkdir -p "$DATA_DIR" "$RUN_DIR"
build_jar_if_missing
ensure_jar
JAR="$(jar_path)"

if [[ -f "$PID_FILE" ]]; then
  old="$(cat "$PID_FILE")"
  if kill -0 "$old" 2>/dev/null; then
    echo "Single node already running (PID $old, port $PORT). Stop with: scripts/stop-single-node.sh"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

nohup java -jar "$JAR" --port "$PORT" --data-dir "$DATA_DIR" >>"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
if ! wait_for_tcp_port 127.0.0.1 "$PORT" 45; then
  echo "Server failed to listen; see $LOG_FILE"
  exit 1
fi
echo "Single-node KV server started (PID $(cat "$PID_FILE"), port $PORT, data $DATA_DIR)"
echo "Log: $LOG_FILE"
