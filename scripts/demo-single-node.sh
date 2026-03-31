#!/usr/bin/env bash
# Interview demo: single-node server + exercise PUT, GET, DELETE, RANGE, BATCH_PUT.
#
# Env:
#   DEMO_LEAVE_RUNNING=1 — do not stop the server after the demo
#   PORT, DATA_DIR — passed through to start-single-node.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

echo "=== KV Store — single-node demo ==="
(cd "$ROOT" && mvn -q -DskipTests package)
ensure_jar

export PORT="${PORT:-7777}"
export DATA_DIR="${DATA_DIR:-$(demo_root)/data-single}"

"$SCRIPT_DIR/start-single-node.sh"

echo ""
echo "--- Operations (all go to localhost:$PORT) ---"
set +e
kv_client --host 127.0.0.1 --port "$PORT" put demo:hello "world"
kv_client --host 127.0.0.1 --port "$PORT" get demo:hello
kv_client --host 127.0.0.1 --port "$PORT" put demo:a "1"
kv_client --host 127.0.0.1 --port "$PORT" put demo:b "2"
kv_client --host 127.0.0.1 --port "$PORT" put demo:c "3"
echo "RANGE demo:a .. demo:z"
kv_client --host 127.0.0.1 --port "$PORT" range demo:a demo:z
echo "BATCH_PUT"
kv_client --host 127.0.0.1 --port "$PORT" batch-put batch:x=10 batch:y=20
kv_client --host 127.0.0.1 --port "$PORT" get batch:x
echo "DELETE demo:hello"
kv_client --host 127.0.0.1 --port "$PORT" delete demo:hello
kv_client --host 127.0.0.1 --port "$PORT" get demo:hello
set -e

echo ""
echo "=== Demo complete ==="
if [[ "${DEMO_LEAVE_RUNNING:-}" == "1" ]]; then
  echo "Server left running (DEMO_LEAVE_RUNNING=1). Stop with: scripts/stop-single-node.sh"
else
  "$SCRIPT_DIR/stop-single-node.sh"
fi
