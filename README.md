# KV Store Engine

A persistent, network-available Key/Value storage engine built from scratch using **only Java standard library primitives** — no Spring, no Netty, no Guava, no external dependencies.

Inspired by the [Bitcask paper (Riak, 2010)](https://riak.com/assets/bitcask-intro.pdf) with [Raft consensus (Ongaro, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf) for multi-node replication and automatic failover.

---

## Architecture

### Single-node mode (default)

**TCP clients → `KVServer` → `BitcaskEngine`**. No Raft overhead.

```
┌─────────────────────────────────────┐
│     KVServer (NIO Selector + pool)  │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        BitcaskEngine (storage)      │
└─────────────────────────────────────┘
```

### Multi-node mode (`--node-id` + `--raft-port` + `--peer`)

**TCP clients → `KVServer` → `RequestHandler` → (if leader) `RaftNode.submitWrite` → replicated log → apply to `BitcaskEngine`.**  
Followers return **`STATUS_REDIRECT` (0x03)** with the leader’s **client** `host:port` so clients can retry on the leader.

Raft peers talk on a **separate TCP port** (`--raft-port`) using `RaftWireCodec` + `TcpRaftRpc` + `RaftRpcServer` (RequestVote / AppendEntries).

```
┌──────────────┐     Raft RPC      ┌──────────────┐
│   Node 2     │◄────────────────►│   Node 1     │
│ KV + Raft    │                  │ KV + Raft    │
└──────┬───────┘                  └──────┬───────┘
       │                                 │
       └──────────── Raft RPC ───────────┘
                    Node 3

Per node:  Client KV protocol (--port)     Raft protocol (--raft-port)
```

**Integration tests:** `Step8_RaftReplicationTest` (in-memory RPC, fast), `Step9_RaftTcpClusterIntegrationTest` (real TCP on loopback, three nodes in one JVM).

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
- Python 3 (optional — only for `scripts/kv_client.py` and the demo shell scripts below)

### Build

```bash
mvn clean package -DskipTests
```

### Run single node

```bash
java -jar target/kv-store-engine-1.0.0.jar \
  --port 7777 \
  --data-dir ./data
```

### Run a 3-node Raft cluster (three terminals)

Use **one process per node**. Each peer is `id@host:raftPort:clientPort` (Raft RPC port first, then KV client port).

```bash
# Terminal 1
java -jar target/kv-store-engine-1.0.0.jar \
  --node-id 1 --port 7777 --raft-port 9777 --data-dir ./data/n1 \
  --advertise-host 127.0.0.1 \
  --peer 2@127.0.0.1:9778:7778 --peer 3@127.0.0.1:9779:7779

# Terminal 2
java -jar target/kv-store-engine-1.0.0.jar \
  --node-id 2 --port 7778 --raft-port 9778 --data-dir ./data/n2 \
  --advertise-host 127.0.0.1 \
  --peer 1@127.0.0.1:9777:7777 --peer 3@127.0.0.1:9779:7779

# Terminal 3
java -jar target/kv-store-engine-1.0.0.jar \
  --node-id 3 --port 7779 --raft-port 9779 --data-dir ./data/n3 \
  --advertise-host 127.0.0.1 \
  --peer 1@127.0.0.1:9777:7777 --peer 2@127.0.0.1:9778:7778
```

Point a test client at **any** node: writes to a **follower** receive `0x03 REDIRECT` + leader host/port; retry on the leader. Reads are also directed to the leader for a simple linearizable story.

### Demo scripts (single-node and Raft cluster)

These live under `scripts/` and use **`scripts/kv_client.py`** (Python 3) to speak the binary TCP protocol. Build the fat JAR first (`mvn package -DskipTests`); the demo scripts run Maven for you if needed.

| Script | Purpose |
|--------|---------|
| `scripts/demo-single-node.sh` | End-to-end single-node walkthrough (starts server, runs ops, stops). |
| `scripts/start-single-node.sh` / `scripts/stop-single-node.sh` | Run or stop one server (default KV port **7777**, data under `demo/data-single`). |
| `scripts/demo-raft-cluster.sh` | End-to-end 3-node Raft walkthrough (starts cluster, probes roles, runs ops, stops). |
| `scripts/start-raft-cluster.sh` / `scripts/stop-raft-cluster.sh` | Run or stop three nodes (KV **7777–7779**, Raft **9777–9779**, data under `demo/data-n1` … `n3`). |

The **`start-*` / `stop-*` scripts only launch or stop JVMs** and then return to your shell (they do not run PUT/GET). Use the matching **`demo-*.sh`** script when you want narrated client operations; `start-raft-cluster.sh` prints `[start-raft]` progress and a short “next steps” footer so it is obvious the process is background-only.

Optional environment variables: **`DEMO_VERBOSE=1`** — client prints each TCP hop and `REDIRECT` on stderr; **`DEMO_LEAVE_RUNNING=1`** — leave server(s) up after a demo; **`RAFT_WARMUP_SEC`** (default `5`) — pause after cluster start before the probe and writes; **`DEMO_FRESH_DATA=0`** — keep existing `demo/data-n1` … `n3` between runs (default is to **delete** those dirs before starting the cluster so **RANGE** does not list keys left from an earlier demo, e.g. `raft:x` / `raft:y` before the current run’s **BATCH_PUT**).

#### Single-node: what you run and what you see

```bash
./scripts/demo-single-node.sh
```

**Behavior:** There is **no Raft**. The server handles PUT, GET, DELETE, RANGE, and BATCH_PUT directly on the local `BitcaskEngine`. You should see **`[demo]`** lines naming each operation and opcode, then **`OK`** / **`NOT_FOUND`** from the client. **GET** prints the value on the next line after **`OK`**. **RANGE** lists matching keys as `key = value`. Deletes are tombstones on disk; a **GET** after **DELETE** returns **`NOT_FOUND`**.

Manual client (server must already be listening):

```bash
./scripts/kv_client.py --port 7777 put mykey "hello"
./scripts/kv_client.py --port 7777 get mykey
```

#### Cluster (Raft): what you run and what you see

```bash
./scripts/demo-raft-cluster.sh
```

**Behavior:** Three JVMs elect a **leader**. The script runs a **wire probe** first: one **PUT** per KV port **without** following redirects. Exactly **one** node should report **`role=LEADER`** (the server accepts the write on that replica); the others report **`role=FOLLOWER`** with **`redirect_leader_kv=127.0.0.1:<port>`** pointing at the leader’s **client** port. That matches the protocol: followers respond with **`0x03 REDIRECT`** and the leader’s address.

The same probe pass appends a **`---MACHINE---`** trailer (`probe --machine-trailer`) with **`RAFT_LEADER_KV_PORT`** and **`RAFT_FIRST_FOLLOWER_KV_PORT`**. The demo uses those ports for the follow-on operations so **PUT/DELETE “via follower”** and **PUT/RANGE “via leader”** always match **who actually won the election** (leader is not assumed to be node 1 / port 7777).

By default the demo **clears** `demo/data-n{1,2,3}` before starting JVMs so the **RANGE** output only contains keys written in **this** run. **BATCH_PUT** for `raft:x` / `raft:y` runs **before** **RANGE** so the narration matches the key set. If you keep old data (`DEMO_FRESH_DATA=0`), a previous run’s keys can still appear in **RANGE** “above” the current **BATCH_PUT** line in the log—those are **stale** keys on disk, not a protocol bug.

The script then performs **PUT** / **GET** / **RANGE** / **BATCH_PUT** / **DELETE** using `kv_client`, which **follows redirects automatically**. When the initial contact is a **follower**, you still end with **`OK`** on stdout after the client reconnects to the leader—there is no extra “redirect” line unless you set **`DEMO_VERBOSE=1`**, in which case **`[kv_client]`** on stderr shows the hop to the leader.

A short **JVM log digest** (`Raft node …`, **`became LEADER`**, elections) is printed from `scripts/run/raft-node-*.log`. Those files are **appended** across runs, so older election lines can appear; treat the **probe summary** as the live view of **leader vs followers**.

**Probe only** (global flags must come **before** the subcommand):

```bash
./scripts/kv_client.py --host 127.0.0.1 probe --kv-ports 7777,7778,7779
```

Append machine-readable **`KEY=value`** lines for shell scripts (same single probe pass):

```bash
./scripts/kv_client.py --host 127.0.0.1 probe --kv-ports 7777,7778,7779 --machine-trailer
```

### Run tests

```bash
mvn clean test
```

TCP cluster smoke test only:

```bash
mvn test -Dtest=com.kvstore.network.Step9_RaftTcpClusterIntegrationTest
```

### Load test (handbook scale) — measured numbers

`Step7_NetworkIntegrationTest#stress_100Clients_1000Ops_handbookScale` runs **100 concurrent clients × 1,000 PUT+GET pairs** (200,000 KV RPC round-trips) against a **single-node** server and prints a line to **stderr**:

```
STRESS_BENCHMARK clientCount=100 opsPerClient=1000 totalRpcRoundTrips=200000 wallMs=... aggregateKiloRpcPerSec=...
```

**Sample run** (developer laptop, Java 21, local SSD, March 2026): **~1.4 s** wall time, **~142 kRPC/s** aggregate (PUT+GET counted as two RPCs). Your hardware and CI will differ — re-run the test and copy the line for your README when publishing.

### What is JMH? (Tier 3 — optional)

**JMH** (Java Microbenchmark Harness) is the standard OpenJDK tool for **microbenchmarks**: it warms up the JVM, forks processes, and avoids common benchmarking pitfalls so you can publish trustworthy **per-operation latency** (e.g. p99 `GET` nanos). We have **not** added a JMH module here to keep the build dependency-free; the stress test above is a **macro** load test. Adding JMH would mean a separate Maven profile or module with `org.openjdk.jmh:jmh-core` **test** dependencies and annotated benchmark classes — reasonable follow-up if interviewers ask for p50/p99 numbers.

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
| `0x03` | **REDIRECT** — payload: `hostLen(4) + host UTF-8 + clientPort(4)` (leader KV endpoint); emitted when not leader in cluster mode |

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
| No persistent Raft log / `votedFor` on disk | In-memory only for take-home timebox | Persist `currentTerm`, `votedFor`, log entries (Raft paper §7) |

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
│   ├── Protocol.java               Wire format codec (incl. REDIRECT payload)
│   ├── RequestHandler.java         Engine + optional Raft leader routing
│   └── RaftRouting.java            Follower → REDIRECT to leader client address
├── replication/
│   ├── RaftNode.java               Raft state machine (leader/follower/candidate)
│   ├── RaftLog.java                Replicated log entries
│   ├── LogEntry.java               Single log entry (term, index, operation)
│   ├── RaftRpc.java                RequestVote + AppendEntries RPC interface
│   ├── RaftWireCodec.java          Binary Raft RPC encode/decode
│   ├── TcpRaftRpc.java             Outbound Raft RPC over TCP
│   └── RaftRpcServer.java          Inbound Raft TCP server
└── KVStoreMain.java                Entry point (single-node or cluster CLI)
```

---

## Test Summary

**71 tests across 9 test suites — all green.**

| Suite | Tests | What It Verifies |
|---|---|---|
| `Step1_RecordSerializerTest` | 10 | Binary encode/decode, CRC32, tombstones, corruption detection, 1MB values |
| `Step2_DataFileTest` | 8 | Append-only writes, positional reads, persistence, 100 concurrent writers |
| `Step3_KeyDirTest` | 10 | O(1) put/get, sorted range queries, 100K concurrent reads, atomic snapshot |
| `Step4_BitcaskEngineTest` | 12 | All 5 API ops, 10K round-trip, overwrites, deletes, batch, file rotation, concurrency |
| `Step5_CompactionTest` | 6 | Dead space reclamation, read correctness during/after compaction, file cleanup |
| `Step6_CrashRecoveryTest` | 8 | Hint file recovery, full replay, torn writes, CRC mismatch, multi-file recovery |
| `Step7_NetworkIntegrationTest` | 8 | TCP protocol; **20×100** concurrent load; **100×1000** handbook stress + stderr benchmark line |
| `Step8_RaftReplicationTest` | 8 | Raft with in-memory RPC: election, replication, failover, catch-up |
| `Step9_RaftTcpClusterIntegrationTest` | 1 | **3-node Raft over real TCP** (loopback): follower `REDIRECT`, write on leader |

---

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data (Riak, 2010)](https://riak.com/assets/bitcask-intro.pdf)
- [In Search of an Understandable Consensus Algorithm — Raft (Ongaro, 2014)](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf)
- [Bigtable: A Distributed Storage System for Structured Data (Google, 2006)](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)
- [The Log-Structured Merge-Tree (O'Neil, 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
