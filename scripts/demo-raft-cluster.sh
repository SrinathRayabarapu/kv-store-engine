#!/usr/bin/env bash
# Interview demo: 3-node Raft + operations via follower (auto-redirect to leader) and other nodes.
#
# Env:
#   DEMO_LEAVE_RUNNING=1 — keep cluster running after demo
#   DEMO_VERBOSE=1       — kv_client logs each TCP hop / REDIRECT on stderr
#   RAFT_WARMUP_SEC      — seconds after start before probe + writes (default 5)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

RAFT_WARMUP_SEC="${RAFT_WARMUP_SEC:-5}"
RUN_DIR="$SCRIPT_DIR/run"

P1=7777
P2=7778
P3=7779
R1=9777
R2=9778
R3=9779

demo_section "KV Store — 3-node Raft demo"
demo_log "Topology: node1 KV :$P1 raft :$R1 | node2 KV :$P2 raft :$R2 | node3 KV :$P3 raft :$R3"
demo_log "Followers answer mutating and strong reads with STATUS_REDIRECT (0x03) + leader host:port; client follows redirects."
demo_log "Tip: DEMO_VERBOSE=1 enables [kv_client] hop trace on stderr. JVM logs: $RUN_DIR/raft-node-*.log"

(cd "$ROOT" && mvn -q -DskipTests package)
ensure_jar

demo_log "Starting three JVMs (start-raft-cluster.sh)..."
"$SCRIPT_DIR/start-raft-cluster.sh"

demo_log "Waiting ${RAFT_WARMUP_SEC}s for leader election / heartbeat stability..."
sleep "$RAFT_WARMUP_SEC"

demo_section "Cluster roles (wire-level probe + JVM log digest)"
demo_log "Probe: one PUT per KV port without following redirects — LEADER returns OK, FOLLOWERS return REDIRECT."
probe_rc=0
kv_client --host 127.0.0.1 probe --kv-ports "${P1},${P2},${P3}" || probe_rc=$?
if [[ "$probe_rc" -ne 0 ]]; then
  demo_log "Probe exited with rc=$probe_rc (no single leader yet — increase RAFT_WARMUP_SEC and retry)."
fi

demo_log "JVM excerpts are from java.util.logging (may include prior restarts in the same log file); wire probe above reflects live roles."
raft_jvm_log_digest "$RUN_DIR" 12

demo_section "Operations with narration"

set +e

demo_log "OP=PUT key=raft:interview value=\"replicated-value\" initial_contact=127.0.0.1:$P2 (node 2)"
demo_log "  Expect: follower returns REDIRECT; kv_client reconnects to leader KV port and completes PUT (Raft replication)."
kv_client --host 127.0.0.1 --port "$P2" put raft:interview "replicated-value"
rc=$?
if [[ $rc -ne 0 ]]; then
  set -e
  demo_log "PUT failed (rc=$rc). Hint: increase RAFT_WARMUP_SEC or check $RUN_DIR/raft-node-*.log"
  exit "$rc"
fi

demo_log "OP=GET key=raft:interview initial_contact=127.0.0.1:$P3 (node 3; may redirect to same leader)"
kv_client --host 127.0.0.1 --port "$P3" get raft:interview

demo_log "OP=PUT raft:a=1 and raft:b=2 via initial node 1 (port $P1)"
kv_client --host 127.0.0.1 --port "$P1" put raft:a "1"
kv_client --host 127.0.0.1 --port "$P1" put raft:b "2"

demo_log "OP=RANGE start=raft:a end=raft:z via port $P1 (leader serves range scan on local BitcaskEngine)"
kv_client --host 127.0.0.1 --port "$P1" range raft:a raft:z

demo_log "OP=BATCH_PUT raft:x=10 raft:y=20 via port $P1 (each entry replicated through Raft on leader)"
kv_client --host 127.0.0.1 --port "$P1" batch-put raft:x=10 raft:y=20

demo_log "OP=GET key=raft:x via port $P1"
kv_client --host 127.0.0.1 --port "$P1" get raft:x

demo_log "OP=DELETE key=raft:interview via follower port $P2 (redirect to leader, then replicated delete)"
kv_client --host 127.0.0.1 --port "$P2" delete raft:interview

demo_log "OP=GET key=raft:interview via port $P1 — expect NOT_FOUND after replicated delete"
kv_client --host 127.0.0.1 --port "$P1" get raft:interview
set -e

demo_section "Raft demo complete"
if [[ "${DEMO_LEAVE_RUNNING:-}" == "1" ]]; then
  demo_log "Cluster left running. Stop with: scripts/stop-raft-cluster.sh"
else
  demo_log "Stopping all nodes (scripts/stop-raft-cluster.sh)."
  "$SCRIPT_DIR/stop-raft-cluster.sh"
fi
