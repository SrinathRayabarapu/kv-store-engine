# Alignment with Moniepoint take-home prep (internal checklist)

This document maps [kv-store-engine](https://github.com/SrinathRayabarapu/kv-store-engine) to the expectations captured in our prep materials under `java-examples/docs/system-design/prep/fintech/moniepoint/` (README + `KV_Engine_Technical_Handbook.md` + `Moniepoint_Interview_Intelligence.md`).

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
| README — architecture, decisions, run | Met | Root `README.md` (updated for honest scope) |

### Handbook nuance — network load test

- **Handbook:** 100 concurrent clients × 1,000 ops each, zero errors.  
- **Repo:** 20 clients × 100 ops — same concurrency pattern, lower scale. Acceptable for CI time; **say this explicitly in the panel** if asked about load.

### Handbook nuance — hint recovery “5× faster”

- **Handbook:** Hint recovery at least 5× faster than full replay on a 100 MB dataset.  
- **Repo:** Correctness of both paths is tested; **no fixed-size timing assertion** in CI. Optional follow-up: add a `@Tag("perf")` or JMH micro-benchmark.

## Tier 2 — Replication (Principal differentiator)

| Item | Status | Evidence |
|------|--------|----------|
| Raft leader election | Met (tests) | `RaftNode`, `Step8_RaftReplicationTest` |
| Raft log replication + majority ACK | Met (tests) | `submitWrite`, `advanceCommitIndex`, same suite |
| Failover + follower catch-up | Met (tests) | Partition / heal scenarios in `Step8_RaftReplicationTest` |
| **TCP Raft RPC + multi-JAR cluster** | **Not in entrypoint** | No `--raft` in `KVStoreMain`; no `RaftRpc` over sockets |

**Interview framing:** Glassdoor signal was “implement replication” — we **did**, in production-quality Java with explicit safety discussion, but **transport integration** is the honest gap. Offer the next step: `SocketRaftRpc` + leader redirect in `Protocol`.

## Non-negotiable constraints (prep README)

| Constraint | Status |
|------------|--------|
| Standard library only in **main** sources | Met — only `java.*` / `javax.*` + `com.kvstore.*` |
| JUnit in **test** scope only | Met — `pom.xml` |
| Binary TCP, not HTTP | Met — `Protocol` + `KVServer` |
| TDD / stepwise tests | Met — `Step1`…`Step8` suites + commit history |

## What to say in the deep-dive

1. **Write path:** lock order (`BitcaskEngine` write lock → append → `KeyDir`); CRC scope in `RecordSerializer`.  
2. **Read path:** `ConcurrentHashMap` + positional `FileChannel.read`.  
3. **Recovery:** hint vs replay; torn tail + CRC on last record of last file.  
4. **Raft:** election timeout randomization, log matching + `truncateAndAppend`, commit rule (majority, same term).  
5. **Gap:** persistent Raft state and TCP RPC — deliberate timebox; path to production listed in README limitations.
