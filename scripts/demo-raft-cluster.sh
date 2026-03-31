#!/usr/bin/env bash
# Interview demo: 3-node Raft + operations via follower (auto-redirect to leader) and reads from another node.
#
# The Python client follows STATUS_REDIRECT to the elected leader for linearizable writes.
#
# Env:
#   DEMO_LEAVE_RUNNING=1 — keep cluster running after demo
#   RAFT_WARMUP_SEC    — seconds to wait after start (default 5)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

RAFT_WARMUP_SEC="${RAFT_WARMUP_SEC:-5}"

echo "=== KV Store — 3-node Raft demo ==="
(cd "$ROOT" && mvn -q -DskipTests package)
ensure_jar

"$SCRIPT_DIR/start-raft-cluster.sh"
echo "Waiting ${RAFT_WARMUP_SEC}s for leader election..."
sleep "$RAFT_WARMUP_SEC"

P1=7777
P2=7778
P3=7779

echo ""
echo "--- Write via node 2 (port $P2): client follows REDIRECT to leader ---"
set +e
kv_client --host 127.0.0.1 --port "$P2" put raft:interview "replicated-value"
rc=$?
if [[ $rc -ne 0 ]]; then
  set -e
  echo "Hint: if you see 'no leader elected', increase RAFT_WARMUP_SEC or wait and re-run kv_client manually."
  exit "$rc"
fi

echo ""
echo "--- Read via node 3 (port $P3); leader serves the read ---"
kv_client --host 127.0.0.1 --port "$P3" get raft:interview

echo ""
echo "--- RANGE and BATCH_PUT through initial contact on node 1 (port $P1) ---"
kv_client --host 127.0.0.1 --port "$P1" put raft:a "1"
kv_client --host 127.0.0.1 --port "$P1" put raft:b "2"
kv_client --host 127.0.0.1 --port "$P1" range raft:a raft:z
kv_client --host 127.0.0.1 --port "$P1" batch-put raft:x=10 raft:y=20
kv_client --host 127.0.0.1 --port "$P1" get raft:x

echo ""
echo "--- DELETE on node 2 (expect NOT_FOUND on final GET) ---"
kv_client --host 127.0.0.1 --port "$P2" delete raft:interview
kv_client --host 127.0.0.1 --port "$P1" get raft:interview
set -e

echo ""
echo "=== Raft demo complete ==="
if [[ "${DEMO_LEAVE_RUNNING:-}" == "1" ]]; then
  echo "Cluster left running. Stop with: scripts/stop-raft-cluster.sh"
else
  "$SCRIPT_DIR/stop-raft-cluster.sh"
fi
