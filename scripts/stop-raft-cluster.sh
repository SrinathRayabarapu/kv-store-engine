#!/usr/bin/env bash
# Stop all three Raft nodes started by start-raft-cluster.sh.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$SCRIPT_DIR/run"

stop_pidfile() {
  local f="$1" label="$2"
  [[ -f "$f" ]] || return 0
  local pid
  pid="$(cat "$f")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping $label (PID $pid)..."
    kill "$pid" 2>/dev/null || true
    for _ in {1..50}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  else
    echo "Stale PID file for $label ($pid)."
  fi
  rm -f "$f"
}

stop_pidfile "$RUN_DIR/raft-node-1.pid" "raft node 1"
stop_pidfile "$RUN_DIR/raft-node-2.pid" "raft node 2"
stop_pidfile "$RUN_DIR/raft-node-3.pid" "raft node 3"
echo "Raft cluster stopped."
