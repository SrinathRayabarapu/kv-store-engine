package com.kvstore.replication;

import com.kvstore.engine.StorageEngine;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Raft consensus node — implements leader election, log replication, and
 * commit-index advancement for a cluster of KV store nodes.
 *
 * <h3>State transitions (from the Raft paper, §5.2):</h3>
 * <pre>
 *   FOLLOWER ──(election timeout)──▶ CANDIDATE
 *   CANDIDATE ──(majority votes)──▶ LEADER
 *   CANDIDATE ──(higher term seen)──▶ FOLLOWER
 *   LEADER ──(higher term seen)──▶ FOLLOWER
 * </pre>
 *
 * <h3>Thread model:</h3>
 * <ul>
 *   <li>Election timer thread: fires election timeout, starts vote campaign</li>
 *   <li>Heartbeat thread (leader only): sends periodic AppendEntries RPCs</li>
 *   <li>Apply thread: applies committed log entries to the storage engine</li>
 *   <li>RPC handling is synchronous (called by the network layer)</li>
 * </ul>
 */
public class RaftNode implements Closeable {

    private static final Logger LOG = Logger.getLogger(RaftNode.class.getName());

    public enum State { FOLLOWER, CANDIDATE, LEADER }

    // --- Persistent state (survives restart — in production, would be on disk) ---
    private long currentTerm = 0;
    private Integer votedFor = null;
    private final RaftLog log = new RaftLog();

    // --- Volatile state ---
    private State state = State.FOLLOWER;
    private long commitIndex = 0;
    private long lastApplied = 0;
    private Integer leaderId = null;

    // --- Leader-only volatile state ---
    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();

    // --- Configuration ---
    private final int nodeId;
    private final List<Integer> peerIds;
    private final StorageEngine engine;
    private final RaftRpc rpc;
    private final ReentrantLock stateLock = new ReentrantLock();

    // --- Timers ---
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3, r -> {
        Thread t = new Thread(r, "raft-scheduler");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    private static final long HEARTBEAT_INTERVAL_MS = 150;
    private static final long ELECTION_TIMEOUT_MIN_MS = 500;
    private static final long ELECTION_TIMEOUT_MAX_MS = 1000;
    private final Random random = new Random();

    // --- Pending write tracking for synchronous replication ---
    private final Map<Long, CompletableFuture<Boolean>> pendingWrites = new ConcurrentHashMap<>();

    public RaftNode(int nodeId, List<Integer> peerIds, StorageEngine engine, RaftRpc rpc) {
        this.nodeId = nodeId;
        this.peerIds = new ArrayList<>(peerIds);
        this.engine = engine;
        this.rpc = rpc;
    }

    /**
     * Starts the Raft node: begins as a follower with a randomized election timeout.
     */
    public void start() {
        resetElectionTimer();
        startApplyLoop();
        LOG.info("Raft node " + nodeId + " started as FOLLOWER");
    }

    // ========== Leader Election (§5.2) ==========

    private void startElection() {
        stateLock.lock();
        try {
            currentTerm++;
            state = State.CANDIDATE;
            votedFor = nodeId;
            leaderId = null;
            LOG.info("Node " + nodeId + " starting election for term " + currentTerm);
        } finally {
            stateLock.unlock();
        }

        AtomicLong voteCount = new AtomicLong(1); // Vote for self
        long term = currentTerm;
        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();

        for (int peerId : peerIds) {
            scheduler.submit(() -> {
                RaftRpc.VoteResponse resp = rpc.requestVote(peerId,
                        new RaftRpc.VoteRequest(term, nodeId, lastLogIndex, lastLogTerm));

                if (resp == null) return;

                stateLock.lock();
                try {
                    if (resp.term() > currentTerm) {
                        stepDown(resp.term());
                        return;
                    }

                    if (state == State.CANDIDATE && resp.voteGranted() && resp.term() == currentTerm) {
                        long votes = voteCount.incrementAndGet();
                        int majority = (peerIds.size() + 1) / 2 + 1;
                        if (votes >= majority) {
                            becomeLeader();
                        }
                    }
                } finally {
                    stateLock.unlock();
                }
            });
        }

        resetElectionTimer();
    }

    private void becomeLeader() {
        state = State.LEADER;
        leaderId = nodeId;
        LOG.info("Node " + nodeId + " became LEADER for term " + currentTerm);

        // Initialize nextIndex and matchIndex for all peers
        long lastIdx = log.lastIndex();
        for (int peerId : peerIds) {
            nextIndex.put(peerId, lastIdx + 1);
            matchIndex.put(peerId, 0L);
        }

        cancelElectionTimer();
        startHeartbeat();
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        state = State.FOLLOWER;
        votedFor = null;
        cancelHeartbeat();
        resetElectionTimer();
    }

    // ========== Log Replication (§5.3) ==========

    /**
     * Submits a write to the Raft log. Returns a future that completes when
     * the entry is committed (majority ACK) or fails (leadership lost).
     *
     * <p>Only the leader accepts writes. Followers return null with the
     * current leader's ID for redirection.
     */
    public CompletableFuture<Boolean> submitWrite(byte opCode, String key, byte[] value) {
        stateLock.lock();
        try {
            if (state != State.LEADER) {
                return CompletableFuture.completedFuture(false);
            }

            long newIndex = log.lastIndex() + 1;
            LogEntry entry = new LogEntry(currentTerm, newIndex, opCode, key, value);
            log.append(entry);

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            pendingWrites.put(newIndex, future);

            // Replicate to peers immediately
            replicateToPeers();

            return future;
        } finally {
            stateLock.unlock();
        }
    }

    private void replicateToPeers() {
        for (int peerId : peerIds) {
            sendAppendEntries(peerId);
        }
    }

    private void sendAppendEntries(int peerId) {
        stateLock.lock();
        long term = currentTerm;
        long prevLogIndex;
        long prevLogTerm;
        List<LogEntry> entries;
        long leaderCommit;
        try {
            if (state != State.LEADER) return;
            long nextIdx = nextIndex.getOrDefault(peerId, log.lastIndex() + 1);
            prevLogIndex = nextIdx - 1;
            prevLogTerm = log.termAt(prevLogIndex);
            entries = log.getFrom(nextIdx);
            leaderCommit = commitIndex;
        } finally {
            stateLock.unlock();
        }

        scheduler.submit(() -> {
            RaftRpc.AppendResponse resp = rpc.appendEntries(peerId,
                    new RaftRpc.AppendRequest(term, nodeId, prevLogIndex, prevLogTerm,
                            entries, leaderCommit));

            if (resp == null) return;

            stateLock.lock();
            try {
                if (resp.term() > currentTerm) {
                    stepDown(resp.term());
                    return;
                }

                if (state != State.LEADER || term != currentTerm) return;

                if (resp.success()) {
                    long newMatchIndex = prevLogIndex + entries.size();
                    matchIndex.put(peerId, newMatchIndex);
                    nextIndex.put(peerId, newMatchIndex + 1);
                    advanceCommitIndex();
                } else {
                    // Conflict: walk back nextIndex
                    long currentNext = nextIndex.getOrDefault(peerId, 1L);
                    nextIndex.put(peerId, Math.max(1, currentNext - 1));
                    // Retry with the decremented nextIndex
                    sendAppendEntries(peerId);
                }
            } finally {
                stateLock.unlock();
            }
        });
    }

    /**
     * Advances commitIndex to the highest index replicated on a majority (§5.3/§5.4).
     * Must be called under stateLock.
     */
    private void advanceCommitIndex() {
        long lastIdx = log.lastIndex();
        long newCommit = commitIndex;
        for (long n = lastIdx; n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) continue;

            int replicatedCount = 1; // self
            for (int peerId : peerIds) {
                if (matchIndex.getOrDefault(peerId, 0L) >= n) {
                    replicatedCount++;
                }
            }

            int majority = (peerIds.size() + 1) / 2 + 1;
            if (replicatedCount >= majority) {
                newCommit = n;
                break;
            }
        }
        if (newCommit > commitIndex) {
            long oldCommit = commitIndex;
            commitIndex = newCommit;
            // Complete futures for newly committed indices (not lastApplied — that tracks apply lag)
            for (long idx = oldCommit + 1; idx <= commitIndex; idx++) {
                CompletableFuture<Boolean> future = pendingWrites.remove(idx);
                if (future != null) {
                    future.complete(true);
                }
            }
        }
    }

    // ========== RPC Handlers ==========

    /**
     * Handles an incoming RequestVote RPC (§5.2).
     */
    public RaftRpc.VoteResponse handleVoteRequest(RaftRpc.VoteRequest request) {
        stateLock.lock();
        try {
            if (request.term() > currentTerm) {
                stepDown(request.term());
            }

            boolean voteGranted = false;
            if (request.term() >= currentTerm
                    && (votedFor == null || votedFor == request.candidateId())
                    && isLogUpToDate(request.lastLogIndex(), request.lastLogTerm())) {
                votedFor = request.candidateId();
                voteGranted = true;
                resetElectionTimer();
            }

            return new RaftRpc.VoteResponse(currentTerm, voteGranted);
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Handles an incoming AppendEntries RPC (§5.3).
     */
    public RaftRpc.AppendResponse handleAppendEntries(RaftRpc.AppendRequest request) {
        stateLock.lock();
        try {
            if (request.term() < currentTerm) {
                return new RaftRpc.AppendResponse(currentTerm, false);
            }

            if (request.term() > currentTerm) {
                stepDown(request.term());
            }

            state = State.FOLLOWER;
            leaderId = request.leaderId();
            resetElectionTimer();

            // Check log consistency at prevLogIndex
            if (request.prevLogIndex() > 0) {
                long termAtPrev = log.termAt(request.prevLogIndex());
                if (request.prevLogIndex() > log.lastIndex() || termAtPrev != request.prevLogTerm()) {
                    return new RaftRpc.AppendResponse(currentTerm, false);
                }
            }

            // Append new entries, resolving conflicts
            if (!request.entries().isEmpty()) {
                long insertIndex = request.prevLogIndex() + 1;
                log.truncateAndAppend(insertIndex, request.entries());
            }

            // Update commit index
            if (request.leaderCommit() > commitIndex) {
                commitIndex = Math.min(request.leaderCommit(), log.lastIndex());
            }

            return new RaftRpc.AppendResponse(currentTerm, true);
        } finally {
            stateLock.unlock();
        }
    }

    private boolean isLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = log.lastTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= log.lastIndex();
    }

    // ========== Apply Committed Entries ==========

    private void startApplyLoop() {
        scheduler.scheduleWithFixedDelay(() -> {
            stateLock.lock();
            try {
                while (lastApplied < commitIndex) {
                    lastApplied++;
                    LogEntry entry = log.get(lastApplied);
                    if (entry == null) break;

                    if (entry.opCode() == LogEntry.OP_PUT) {
                        engine.put(entry.key(), entry.value());
                    } else if (entry.opCode() == LogEntry.OP_DELETE) {
                        engine.delete(entry.key());
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Apply loop error", e);
            } finally {
                stateLock.unlock();
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    // ========== Timers ==========

    private void resetElectionTimer() {
        cancelElectionTimer();
        long timeout = ELECTION_TIMEOUT_MIN_MS
                + random.nextLong(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        electionTimer = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
    }

    private void startHeartbeat() {
        cancelHeartbeat();
        heartbeatTimer = scheduler.scheduleAtFixedRate(
                this::replicateToPeers, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }
    }

    // ========== Accessors ==========

    public int getNodeId() { return nodeId; }
    public State getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public Integer getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public RaftLog getLog() { return log; }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
