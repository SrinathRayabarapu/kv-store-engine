package com.kvstore.network;

import com.kvstore.replication.RaftNode;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * When the KV server runs in a Raft cluster, this resolves whether the local
 * node is the leader and builds {@link Protocol#STATUS_REDIRECT} responses
 * for clients that must contact the leader's client port.
 */
public final class RaftRouting {

    private final RaftNode raft;
    /** nodeId → address clients use for KV protocol (not raft port). */
    private final Map<Integer, InetSocketAddress> clientEndpoints;

    public RaftRouting(RaftNode raft, Map<Integer, InetSocketAddress> clientEndpoints) {
        this.raft = raft;
        this.clientEndpoints = clientEndpoints;
    }

    public RaftNode getRaft() {
        return raft;
    }

    /**
     * @return {@code null} if this node is the leader; otherwise a complete
     *         encoded response (REDIRECT or ERROR) the handler should return immediately
     */
    public byte[] redirectIfNotLeader() {
        if (raft.getState() == RaftNode.State.LEADER) {
            return null;
        }
        Integer lid = raft.getLeaderId();
        if (lid == null) {
            return Protocol.encodeError("no leader elected — retry shortly");
        }
        InetSocketAddress leader = clientEndpoints.get(lid);
        if (leader == null) {
            return Protocol.encodeError("leader id " + lid + " has no known client address");
        }
        String host = leader.getHostString();
        if (host == null || host.isEmpty()) {
            host = leader.getAddress().getHostAddress();
        }
        return Protocol.encodeRedirect(host, leader.getPort());
    }
}
