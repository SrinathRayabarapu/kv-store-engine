#!/usr/bin/env bash
# Interview demo: 3-node Raft + operations via follower (auto-redirect to leader) and other nodes.
#
# Env:
#   DEMO_LEAVE_RUNNING=1 — keep cluster running after demo
#   DEMO_VERBOSE=1       — kv_client logs each TCP hop / REDIRECT on stderr
#   RAFT_WARMUP_SEC      — seconds after start before probe + writes (default 5)
#   DEMO_FRESH_DATA=0    — do not delete demo/data-n{1,2,3} before start (default: delete so RANGE is not polluted by prior runs)

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

BASE="$(demo_root)"
if [[ "${DEMO_FRESH_DATA:-1}" != "0" ]]; then
  demo_log "Clearing persisted Raft data under ${BASE}/data-n1 … data-n3 (set DEMO_FRESH_DATA=0 to keep prior demo keys)."
  rm -rf "${BASE}/data-n1" "${BASE}/data-n2" "${BASE}/data-n3"
fi

demo_log "Starting three JVMs (start-raft-cluster.sh)..."
"$SCRIPT_DIR/start-raft-cluster.sh"

demo_log "Waiting ${RAFT_WARMUP_SEC}s for leader election / heartbeat stability..."
sleep "$RAFT_WARMUP_SEC"

demo_section "Cluster roles (wire-level probe + JVM log digest)"
demo_log "Probe: one PUT per KV port without following redirects — LEADER returns OK, FOLLOWERS return REDIRECT."
demo_log "Appends ---MACHINE--- trailer (same pass) so later ops use the live leader/follower KV ports (election is non-deterministic)."
probe_rc=0
PROBE_OUT=$(kv_client --host 127.0.0.1 probe --kv-ports "${P1},${P2},${P3}" --machine-trailer) || probe_rc=$?
printf '%s\n' "$PROBE_OUT"
raft_load_machine_trailer "$PROBE_OUT"

if [[ "$RAFT_PROBE_OK" != "1" ]]; then
  demo_log "Probe did not classify exactly one leader (RAFT_PROBE_OK=${RAFT_PROBE_OK:-?} ${RAFT_PROBE_REASON:+$RAFT_PROBE_REASON}). Increase RAFT_WARMUP_SEC or check $RUN_DIR/raft-node-*.log"
  exit 1
fi
if [[ -z "$RAFT_FIRST_FOLLOWER_KV_PORT" ]]; then
  demo_log "No follower returned REDIRECT — need at least one reachable follower to demo follower → leader redirect."
  exit 1
fi

LEADER_KV="${RAFT_LEADER_KV_PORT}"
FOLLOWER_KV="${RAFT_FIRST_FOLLOWER_KV_PORT}"
# Prefer a second follower for GET if the cluster exposed one (same leader redirect story).
GET_START_KV="${RAFT_SECOND_FOLLOWER_KV_PORT:-$FOLLOWER_KV}"

demo_log "Resolved from probe: LEADER KV :${LEADER_KV}; redirect demo uses follower KV :${FOLLOWER_KV} (PUT/DELETE). GET initial_contact :${GET_START_KV}."

demo_log "JVM excerpts are from java.util.logging (may include prior restarts in the same log file); wire probe above reflects live roles."
raft_jvm_log_digest "$RUN_DIR" 12

demo_section "Operations with narration"

set +e

demo_log "OP=PUT key=raft:interview value=\"replicated-value\" initial_contact=127.0.0.1:${FOLLOWER_KV} (probed follower — not hardcoded node 2)"
demo_log "  Expect: follower returns REDIRECT; kv_client reconnects to leader KV :${LEADER_KV} and completes PUT (Raft replication). Final line is OK from the leader hop."
kv_client --host 127.0.0.1 --port "$FOLLOWER_KV" put raft:interview "replicated-value"
rc=$?
if [[ $rc -ne 0 ]]; then
  set -e
  demo_log "PUT failed (rc=$rc). Hint: increase RAFT_WARMUP_SEC or check $RUN_DIR/raft-node-*.log"
  exit "$rc"
fi

demo_log "OP=GET key=raft:interview initial_contact=127.0.0.1:${GET_START_KV} (probed follower; may redirect to leader :${LEADER_KV})"
kv_client --host 127.0.0.1 --port "$GET_START_KV" get raft:interview

demo_log "OP=PUT raft:a=1 and raft:b=2 via probed leader KV :${LEADER_KV}"
kv_client --host 127.0.0.1 --port "$LEADER_KV" put raft:a "1"
kv_client --host 127.0.0.1 --port "$LEADER_KV" put raft:b "2"

demo_log "OP=BATCH_PUT raft:x=10 raft:y=20 via leader KV :${LEADER_KV} (each entry replicated through Raft on leader; before RANGE so the scan includes x,y on a fresh data dir)"
kv_client --host 127.0.0.1 --port "$LEADER_KV" batch-put raft:x=10 raft:y=20

demo_log "OP=RANGE start=raft:a end=raft:z via leader KV :${LEADER_KV} (leader serves range scan on local BitcaskEngine)"
kv_client --host 127.0.0.1 --port "$LEADER_KV" range raft:a raft:z

demo_log "OP=GET key=raft:x via leader KV :${LEADER_KV}"
kv_client --host 127.0.0.1 --port "$LEADER_KV" get raft:x

demo_log "OP=DELETE key=raft:interview via probed follower KV :${FOLLOWER_KV} (REDIRECT to leader :${LEADER_KV}, then replicated delete)"
kv_client --host 127.0.0.1 --port "$FOLLOWER_KV" delete raft:interview

demo_log "OP=GET key=raft:interview via leader KV :${LEADER_KV} — expect NOT_FOUND after replicated delete"
kv_client --host 127.0.0.1 --port "$LEADER_KV" get raft:interview
set -e

demo_section "Raft demo complete"
if [[ "${DEMO_LEAVE_RUNNING:-}" == "1" ]]; then
  demo_log "Cluster left running. Stop with: scripts/stop-raft-cluster.sh"
else
  demo_log "Stopping all nodes (scripts/stop-raft-cluster.sh)."
  "$SCRIPT_DIR/stop-raft-cluster.sh"
fi
