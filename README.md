# KV Store Engine

A persistent, network-available Key/Value storage engine built from scratch using **only Java standard library primitives** — no Spring, no Netty, no Guava, no external dependencies.

Inspired by the [Bitcask paper (Riak, 2010)](https://riak.com/assets/bitcask-intro.pdf) with [Raft consensus (Ongaro, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf) for multi-node replication and automatic failover.

---

## Architecture

```
                          ┌─────────────────────────────────────┐
                          │           KVServer (NIO TCP)        │
                          │  Selector thread + Worker pool      │
                          │  Binary protocol: PUT/GET/DEL/RANGE │
                          └──────────┬──────────────────────────┘
                                     │
                          ┌──────────▼──────────────────────────┐
                          │         RaftNode (Consensus)        │
                          │  Leader election, log replication   │
                          │  Writes → Raft log → majority ACK  │
                          └──────────┬──────────────────────────┘
                                     │
                          ┌──────────▼──────────────────────────┐
                          │        BitcaskEngine (Storage)      │
                          │                                     │
                          │  ┌─────────┐    ┌──────────────┐   │
                          │  │ KeyDir  │    │  Data Files   │   │
                          │  │ (CMap)  │◄──►│ (append-only) │   │
                          │  └─────────┘    └──────────────┘   │
                          │       ▲          ┌──────────────┐   │
                          │       └──────────│  Hint Files   │   │
                          │    (fast rebuild) │ (index only)  │   │
                          │                  └──────────────┘   │
                          │         ┌──────────────┐            │
                          │         │  Compactor    │            │
                          │         │ (background)  │            │
                          │         └──────────────┘            │
                          └─────────────────────────────────────┘
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
| `BatchPut` | O(m) | 1 sequential append (m records) |
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

### Run 3-Node Raft Cluster

```bash
# Terminal 1 — Node 1 (will become leader)
java -jar target/kv-store-engine-1.0.0.jar \
  --port 7777 --data-dir ./data/node1 \
  --raft --node-id 1 --peers localhost:7778,localhost:7779

# Terminal 2 — Node 2
java -jar target/kv-store-engine-1.0.0.jar \
  --port 7778 --data-dir ./data/node2 \
  --raft --node-id 2 --peers localhost:7777,localhost:7779

# Terminal 3 — Node 3
java -jar target/kv-store-engine-1.0.0.jar \
  --port 7779 --data-dir ./data/node3 \
  --raft --node-id 3 --peers localhost:7777,localhost:7778
```

### Run Tests

```bash
# All tests (excluding performance benchmarks)
mvn test

# Include performance benchmarks
mvn test -Dgroups="perf"
```

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
| `0x03` | REDIRECT (includes leader address for Raft) |

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
| fsync policy | Configurable (default: ON_ROTATE) | Always / Never | ON_ROTATE trades small durability window for 10x write throughput |

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

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data (Riak, 2010)](https://riak.com/assets/bitcask-intro.pdf)
- [In Search of an Understandable Consensus Algorithm — Raft (Ongaro, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf)
- [Bigtable: A Distributed Storage System for Structured Data (Google, 2006)](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)
- [The Log-Structured Merge-Tree (O'Neil, 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
