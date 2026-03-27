package com.kvstore.replication;

import java.util.List;

/**
 * RPC interface for Raft inter-node communication.
 *
 * <p>Defines the two core RPCs from the Raft paper:
 * <ul>
 *   <li>{@link #requestVote} — candidate requests votes during election (§5.2)</li>
 *   <li>{@link #appendEntries} — leader replicates log entries / sends heartbeats (§5.3)</li>
 * </ul>
 *
 * <p>Implementations may use TCP sockets, in-memory queues (for testing), or
 * any transport. Returning null from either method indicates a network failure
 * (timeout / unreachable peer).
 */
public interface RaftRpc {

    /**
     * Sends a RequestVote RPC to the specified peer.
     *
     * @param peerId the target node
     * @param request the vote request
     * @return the peer's response, or null if the peer is unreachable
     */
    VoteResponse requestVote(int peerId, VoteRequest request);

    /**
     * Sends an AppendEntries RPC to the specified peer.
     *
     * @param peerId the target node
     * @param request the append request (may be a heartbeat if entries is empty)
     * @return the peer's response, or null if the peer is unreachable
     */
    AppendResponse appendEntries(int peerId, AppendRequest request);

    // --- Message types ---

    record VoteRequest(long term, int candidateId, long lastLogIndex, long lastLogTerm) {}

    record VoteResponse(long term, boolean voteGranted) {}

    record AppendRequest(
            long term,
            int leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit
    ) {}

    record AppendResponse(long term, boolean success) {}
}
