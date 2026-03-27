# Alignment with Moniepoint take-home prep (internal checklist)

This document maps [kv-store-engine](https://github.com/SrinathRayabarapu/kv-store-engine) to the expectations in `java-examples/docs/system-design/prep/fintech/moniepoint/` (README + `KV_Engine_Technical_Handbook.md` + `Moniepoint_Interview_Intelligence.md`).

## Tier 1 — Core (must ship)

| Item | Status | Evidence |
|------|--------|----------|
| RecordSerializer + tests | Met | `RecordSerializer`, `Step1_RecordSerializerTest` |
| DataFile + tests | Met | `DataFile`, `Step2_DataFileTest` |
| KeyDir + tests | Met | `KeyDir`, `KeyDirEntry`, `Step3_KeyDirTest` |
| BitcaskEngine — 5 API ops + tests | Met | `StorageEngine`, `BitcaskEngine`, `Step4_BitcaskEngineTest` |
| Compaction + tests | Met | `Compactor`, `Step5_CompactionTest` |
| Crash recovery + tests | Met | `CrashRecovery`, `HintFile`, `Step6_CrashRecoveryTest` |
| KVServer (NIO TCP) + integration tests | Met | `KVServer`, `Protocol`, `Step7_NetworkIntegrationTest` |
| README — architecture, decisions, run | Met | Root `README.md` |

### Handbook — network load test

- **Handbook:** 100 concurrent clients × 1,000 ops each, zero errors.  
- **Repo:** `Step7_NetworkIntegrationTest#stress_100Clients_1000Ops_handbookScale` — **100 × 1,000** PUT+GET pairs (200k RPC round-trips). Prints timing to stderr; README documents a sample observation.

### Handbook — hint recovery “5× faster”

- **Repo:** Correctness of hint vs replay is tested; **no automated 5× timing assertion**. Optional: add a dedicated perf test on a fixed dataset size.

## Tier 2 — Replication (Principal differentiator)

| Item | Status | Evidence |
|------|--------|----------|
| Raft leader election | Met | `RaftNode`, `Step8_RaftReplicationTest`, live cluster |
| Raft log replication + majority ACK | Met | `submitWrite`, `advanceCommitIndex`, tests |
| Failover + follower catch-up | Met | `Step8_RaftReplicationTest` |
| **TCP Raft + multi-process demo** | Met | `TcpRaftRpc`, `RaftRpcServer`, `RaftWireCodec`, `KVStoreMain` cluster CLI |
| **Client redirect from follower** | Met | `Protocol.STATUS_REDIRECT`, `RaftRouting`, `RequestHandler` |

**Tests:** `Step9_RaftTcpClusterIntegrationTest` — three nodes, real sockets on loopback, follower returns REDIRECT then client writes on leader.

## Tier 3 — Polish (nice to have)

| Item | Status |
|------|--------|
| JMH microbenchmarks | Not added (see README “What is JMH?” — optional follow-up) |
| Raft snapshot | Not implemented |
| Configurable fsync policy enum | Not implemented (documented as future work) |

## Non-negotiable constraints

| Constraint | Status |
|------------|--------|
| Standard library only in **main** sources | Met |
| JUnit in **test** scope only | Met |
| Binary TCP for **client** KV protocol | Met |
| Binary TCP for **Raft** (separate port) | Met |

## Deep-dive talking points

1. **Single-node write path:** `BitcaskEngine` lock → append → `KeyDir`.  
2. **Cluster write path:** leader `submitWrite` → replicate via `TcpRaftRpc` → majority → complete future → apply loop → `engine.put`.  
3. **Follower client:** `RaftRouting.redirectIfNotLeader()` → `encodeRedirect(host, port)`.  
4. **Gaps for production:** persist Raft state; snapshots; TLS; JMH p99 latency.
