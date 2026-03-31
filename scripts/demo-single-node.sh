#!/usr/bin/env bash
# Interview demo: single-node server + exercise PUT, GET, DELETE, RANGE, BATCH_PUT.
#
# Env:
#   DEMO_LEAVE_RUNNING=1 — do not stop the server after the demo
#   DEMO_VERBOSE=1       — kv_client prints each TCP hop / redirect to stderr
#   PORT, DATA_DIR       — passed through to start-single-node.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

demo_section "KV Store — single-node demo"
demo_log "Tip: set DEMO_VERBOSE=1 to see per-request TCP debug on stderr."
(cd "$ROOT" && mvn -q -DskipTests package)
ensure_jar

export PORT="${PORT:-7777}"
export DATA_DIR="${DATA_DIR:-$(demo_root)/data-single}"

demo_log "Starting server: port=$PORT data_dir=$DATA_DIR (no Raft; RequestHandler talks to BitcaskEngine only)."
"$SCRIPT_DIR/start-single-node.sh"
demo_log "Server is listening; client uses binary protocol (opcodes 0x01–0x05 per README)."

demo_section "Operations (initial contact 127.0.0.1:$PORT)"
set +e

demo_log "OP=PUT key=demo:hello value=\"world\" (opcode 0x01, expect OK)"
kv_client --host 127.0.0.1 --port "$PORT" put demo:hello "world"
demo_log "--- response above (stdout); stderr may show [kv_client] hops if DEMO_VERBOSE=1 ---"

demo_log "OP=GET key=demo:hello (opcode 0x02, expect OK + value line)"
kv_client --host 127.0.0.1 --port "$PORT" get demo:hello

demo_log "OP=PUT key=demo:a value=\"1\""
kv_client --host 127.0.0.1 --port "$PORT" put demo:a "1"
demo_log "OP=PUT key=demo:b value=\"2\""
kv_client --host 127.0.0.1 --port "$PORT" put demo:b "2"
demo_log "OP=PUT key=demo:c value=\"3\""
kv_client --host 127.0.0.1 --port "$PORT" put demo:c "3"

demo_log "OP=RANGE start=demo:a end=demo:z (opcode 0x04; lexicographic inclusive range)"
kv_client --host 127.0.0.1 --port "$PORT" range demo:a demo:z

demo_log "OP=BATCH_PUT pairs batch:x=10 batch:y=20 (opcode 0x05; sequential replicated puts in cluster mode; here single atomic batchPut)"
kv_client --host 127.0.0.1 --port "$PORT" batch-put batch:x=10 batch:y=20

demo_log "OP=GET key=batch:x"
kv_client --host 127.0.0.1 --port "$PORT" get batch:x

demo_log "OP=DELETE key=demo:hello (opcode 0x03, tombstone on disk)"
kv_client --host 127.0.0.1 --port "$PORT" delete demo:hello

demo_log "OP=GET key=demo:hello (expect NOT_FOUND / 0x01 after delete)"
kv_client --host 127.0.0.1 --port "$PORT" get demo:hello
set -e

demo_section "Demo complete"
if [[ "${DEMO_LEAVE_RUNNING:-}" == "1" ]]; then
  demo_log "Server left running (DEMO_LEAVE_RUNNING=1). Stop with: scripts/stop-single-node.sh"
else
  demo_log "Stopping server (scripts/stop-single-node.sh)."
  "$SCRIPT_DIR/stop-single-node.sh"
fi
