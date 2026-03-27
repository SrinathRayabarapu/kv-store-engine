package com.kvstore.replication;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory RPC transport for Raft testing. Routes RPCs directly between
 * registered {@link RaftNode} instances without any network I/O.
 *
 * <p>Supports simulating node failures by adding node IDs to the
 * {@link #partitioned} set — RPCs to/from partitioned nodes return null.
 */
class InMemoryRaftRpc implements RaftRpc {

    private final Map<Integer, RaftNode> nodes = new ConcurrentHashMap<>();
    final Set<Integer> partitioned = ConcurrentHashMap.newKeySet();

    void register(int nodeId, RaftNode node) {
        nodes.put(nodeId, node);
    }

    @Override
    public VoteResponse requestVote(int peerId, VoteRequest request) {
        if (partitioned.contains(peerId) || partitioned.contains(request.candidateId())) {
            return null;
        }
        RaftNode target = nodes.get(peerId);
        if (target == null) return null;
        return target.handleVoteRequest(request);
    }

    @Override
    public AppendResponse appendEntries(int peerId, AppendRequest request) {
        if (partitioned.contains(peerId) || partitioned.contains(request.leaderId())) {
            return null;
        }
        RaftNode target = nodes.get(peerId);
        if (target == null) return null;
        return target.handleAppendEntries(request);
    }
}
