#!/usr/bin/env bash
# Stop the single-node server started by start-single-node.sh (PID file in scripts/run/).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/run/single-node.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file at $PID_FILE (nothing to stop)."
  exit 0
fi

pid="$(cat "$PID_FILE")"
if kill -0 "$pid" 2>/dev/null; then
  echo "Stopping single-node server (PID $pid)..."
  kill "$pid" 2>/dev/null || true
  for _ in {1..50}; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "Force killing PID $pid"
    kill -9 "$pid" 2>/dev/null || true
  fi
else
  echo "Stale PID file (process $pid not running)."
fi
rm -f "$PID_FILE"
echo "Single-node server stopped."
