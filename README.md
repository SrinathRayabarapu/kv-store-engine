# KV Store Engine

A persistent, network-available Key/Value storage engine built from scratch using **only Java standard library primitives** — no Spring, no Netty, no Guava, no external dependencies.

Inspired by the [Bitcask paper (Riak, 2010)](https://riak.com/assets/bitcask-intro.pdf) with [Raft consensus (Ongaro, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf) for multi-node replication and automatic failover.

---

## Architecture

### What `KVStoreMain` runs today (single-node, network-available)

The shipped JAR wires **TCP clients → `KVServer` → `BitcaskEngine`** directly. This matches the take-home core requirement: a persistent, **network-available** KV engine with a **binary TCP** protocol.

```
┌─────────────────────────────────────┐
│     KVServer (NIO Selector + pool)   │
│     Binary protocol over TCP         │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        BitcaskEngine (storage)       │
│  KeyDir + append-only DataFiles      │
│  Hint files + CrashRecovery          │
│  Compactor (merge / reclaim space)   │
└─────────────────────────────────────┘
```

### Raft (replication) — where it lives

**Raft is implemented** (`RaftNode`, `RaftLog`, `RaftRpc`) and **covered by tests** using an in-memory `RaftRpc` transport (`Step8_RaftReplicationTest`). That satisfies the Principal-level bar in our prep docs: leader election, log replication, majority commit, failover, and follower catch-up — **as an algorithmic layer you can demo in tests and in code review**.

It is **not** yet composed with `KVServer` / `KVStoreMain`: there is no TCP-based `RaftRpc`, no `--raft` CLI, and no `REDIRECT_TO_LEADER` responses on the wire. The handbook diagram below is the **target** full stack; see [docs/SUBMISSION_ALIGNMENT.md](docs/SUBMISSION_ALIGNMENT.md) for a checklist mapping.

```
(Target integration — future work)
  Client → KVServer → RaftNode → BitcaskEngine
```

### Why Bitcask Over LSM?

The assignment emphasizes **random writes** + **low-latency reads** + **crash friendliness** — Bitcask's three strongest properties. An LSM tree adds complexity (memtable, bloom filters, level-based compaction) that is justified only when sorted range queries dominate the workload. Since our range query requirement (`ReadKeyRange`) is secondary, the Bitcask model gives us excellent write throughput (append-only sequential I/O) and single-seek reads (in-memory index points directly to the value offset on disk) with far less implementation complexity.

The tradeoff: `ReadKeyRange` becomes O(n log n) per query because Bitcask has no sorted on-disk structure. We accept this explicitly and document it as a known limitation.

### Why Raft Over Paxos?

Raft was designed for understandability and has a clear separation of leader election, log replication, and safety. For a storage engine where all writes go through a single leader, Raft's leader-centric model maps naturally to the architecture. Paxos requires more engineering work to extend to log replication (Multi-Paxos) and has subtleties around liveness that Raft makes explicit.

### Why NIO Over Virtual Threads?

Java NIO's `Selector` model (single thread multiplexing many connections) is the canonical approach for high-connection-count servers. While Java 21 virtual threads could simplify the code, the NIO selector model demonstrates deeper understanding of non-blocking I/O — which is the point of this exercise.

---

## Performance Characteristics

| Operation | Time Complexity | Disk I/O |
|---|---|---|
| `PUT` | O(1) amortized | 1 sequential append |
| `GET` | O(1) | 1 random read (seek to offset) |
| `DELETE` | O(1) | 1 sequential append (tombstone) |
| `ReadKeyRange` | O(n + k log k) | k random reads |
| `BatchPut` | O(m) | m sequential appends (single write-lock hold; not one physical disk block) |
| Compaction | O(n) | Full read + write of live data |
| Recovery (hint files) | O(keys) | Read hint files only |
| Recovery (full replay) | O(data) | Read all data files |

Where: n = total keys, k = keys in range, m = batch size.

---

## Running Locally

### Prerequisites

- Java 21+
- Maven 3.8+

### Build

```bash
mvn clean package -DskipTests
```

### Run Single Node

```bash
java -jar target/kv-store-engine-1.0.0.jar \
  --port 7777 \
  --data-dir ./data
```

### Raft cluster (not in the JAR yet)

Multi-process Raft over TCP is **not** wired to `KVStoreMain`. To demonstrate replication for the panel, point reviewers at **`Step8_RaftReplicationTest`** and the `replication` package — or run:

```bash
mvn test -Dtest=com.kvstore.replication.Step8_RaftReplicationTest
```

### Run Tests

```bash
mvn clean test
```

**Performance / JMH:** not included yet (Tier 3 in our prep roadmap). The handbook’s “100 clients × 1000 ops” stress target is approximated by **20 clients × 100 ops** in `Step7_NetworkIntegrationTest` (same patterns, lower scale).

---

## Wire Protocol

Binary TCP protocol (not HTTP). All integers are big-endian.

### Request Format

```
┌──────────┬──────────┬──────────...─────┐
│ OpCode   │ Payload  │  Payload Bytes   │
│  1 byte  │  Length  │                  │
│          │  4 bytes │                  │
└──────────┴──────────┴──────────...─────┘
```

| OpCode | Operation | Payload |
|---|---|---|
| `0x01` | PUT | `keyLen(4) + key + valLen(4) + value` |
| `0x02` | GET | `keyLen(4) + key` |
| `0x03` | DELETE | `keyLen(4) + key` |
| `0x04` | RANGE | `startKeyLen(4) + startKey + endKeyLen(4) + endKey` |
| `0x05` | BATCH_PUT | `count(4) + [keyLen(4) + key + valLen(4) + value] * count` |

### Response Format

```
┌──────────┬──────────┬──────────...─────┐
│  Status  │ Payload  │  Payload Bytes   │
│  1 byte  │  Length  │                  │
│          │  4 bytes │                  │
└──────────┴──────────┴──────────...─────┘
```

| Status | Meaning |
|---|---|
| `0x00` | OK |
| `0x01` | NOT_FOUND |
| `0x02` | ERROR |
| `0x03` | RESERVED for future `REDIRECT_TO_LEADER` (not emitted by the server today) |

---

## On-Disk Format

### Data File Record

```
┌──────────┬──────────┬──────────┬──────────┬────────...────────┬────────...────────┐
│  CRC32   │Timestamp │ KeySize  │ ValSize  │    Key Bytes      │   Value Bytes     │
│  4 bytes │  8 bytes │  4 bytes │  4 bytes │  KeySize bytes    │  ValSize bytes    │
└──────────┴──────────┴──────────┴──────────┴────────...────────┴────────...────────┘
```

A tombstone (delete marker) has `ValSize = -1` and zero value bytes.

### Hint File Record

```
┌──────────┬──────────┬──────────┬──────────┬────────...────────┐
│Timestamp │ KeySize  │ ValSize  │ ValOffset│    Key Bytes      │
│  8 bytes │  4 bytes │  4 bytes │  8 bytes │  KeySize bytes    │
└──────────┴──────────┴──────────┴──────────┴────────...────────┘
```

---

## Design Decisions

| Decision | Chosen | Alternative Considered | Why |
|---|---|---|---|
| Storage model | Bitcask | LSM Tree | Simpler; optimal for random-write + point-read workload described in requirements |
| Consensus | Raft | Paxos | Clearer leader model; maps to single-writer architecture |
| Network I/O | Java NIO Selector | Virtual Threads / Thread-per-connection | Demonstrates non-blocking I/O mastery; lower resource usage at high connection counts |
| Key index | `ConcurrentHashMap` | `TreeMap` + locks | O(1) reads without per-read locking on the hot path |
| Write concurrency | `ReentrantLock` on active file | Global lock / Lock-free | Per-file lock allows concurrent reads; simpler than lock-free append |
| Serialization | Custom binary | JSON / Protobuf | Zero dependencies; minimal overhead; self-describing with CRC |
| Compaction trigger | Dead-bytes ratio > 50% | Timer-based / Manual | Balances write amplification vs. space amplification |
| fsync policy | `sync()` on file close / rotation paths | Per-write `force(true)` | Default favors throughput; a dedicated `SyncPolicy` enum is listed as future work in limitations |

---

## Known Limitations

| Limitation | Why | What We'd Do With More Time |
|---|---|---|
| All keys must fit in RAM | Bitcask design: KeyDir is entirely in-memory | Hybrid: switch to LSM when key set exceeds configured memory threshold |
| `ReadKeyRange` is O(n log n) | Bitcask has no sorted on-disk structure | Secondary `ConcurrentSkipListMap` for range-heavy workloads |
| Single writer per node | `ReentrantLock` on active DataFile | Partition key space across multiple active files |
| No auth / TLS | Not in scope | Add via `SSLContext` (standard library) |
| No read replicas | Simplifies consistency | Follower reads with `ReadIndex` protocol (Raft §6.4) |
| Raft snapshot not implemented | Time constraint | Required for production to bound log growth |
| Raft not integrated with `KVServer` / JAR entrypoint | TCP `RaftRpc` + CLI scope | Wire `RequestHandler` → leader `submitWrite`, followers return `0x03` + leader hint |
| No persistent Raft log / `votedFor` on disk | In-memory only for take-home timebox | Persist `currentTerm`, `votedFor`, log entries (paper §7) |
| `REDIRECT` status unused | Single-node TCP path only | Implement when Raft is on the wire |

---

## Project Structure

```
src/main/java/com/kvstore/
├── engine/
│   ├── StorageEngine.java          Interface: the 5 API operations
│   ├── BitcaskEngine.java          Core implementation wiring KeyDir + DataFile
│   ├── KeyDir.java                 In-memory index (ConcurrentHashMap wrapper)
│   ├── KeyDirEntry.java            Index entry: fileId, offset, size, timestamp
│   ├── DataFile.java               Append-only log file with FileChannel
│   ├── RecordSerializer.java       Binary encode/decode with CRC32
│   ├── HintFile.java               Compact index persistence for fast recovery
│   └── Compactor.java              Background dead-space reclamation
├── network/
│   ├── KVServer.java               NIO TCP server (Selector + worker pool)
│   ├── Protocol.java               Wire format codec
│   └── RequestHandler.java         Dispatches protocol ops to engine
├── replication/
│   ├── RaftNode.java               Raft state machine (leader/follower/candidate)
│   ├── RaftLog.java                Replicated log entries
│   ├── LogEntry.java               Single log entry (term, index, operation)
│   └── RaftRpc.java                RequestVote + AppendEntries RPCs
└── KVStoreMain.java                Entry point with CLI argument parsing
```

---

## Test Summary

**69 tests across 8 test suites — all green.**

| Suite | Tests | What It Verifies |
|---|---|---|
| `Step1_RecordSerializerTest` | 10 | Binary encode/decode, CRC32, tombstones, corruption detection, 1MB values |
| `Step2_DataFileTest` | 8 | Append-only writes, positional reads, persistence, 100 concurrent writers |
| `Step3_KeyDirTest` | 10 | O(1) put/get, sorted range queries, 100K concurrent reads, atomic snapshot |
| `Step4_BitcaskEngineTest` | 12 | All 5 API ops, 10K round-trip, overwrites, deletes, batch, file rotation, concurrency |
| `Step5_CompactionTest` | 6 | Dead space reclamation, read correctness during/after compaction, file cleanup |
| `Step6_CrashRecoveryTest` | 8 | Hint file recovery, full replay, torn writes, CRC mismatch, multi-file recovery |
| `Step7_NetworkIntegrationTest` | 7 | TCP PUT/GET/DELETE/RANGE/BATCH, **20×100** concurrent load (handbook target was 100×1000 — same style, smaller scale) |
| `Step8_RaftReplicationTest` | 8 | Leader election, majority ACK, failover, follower catch-up, write ordering |

---

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data (Riak, 2010)](https://riak.com/assets/bitcask-intro.pdf)
- [In Search of an Understandable Consensus Algorithm — Raft (Ongaro, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf)
- [Bigtable: A Distributed Storage System for Structured Data (Google, 2006)](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)
- [The Log-Structured Merge-Tree (O'Neil, 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
