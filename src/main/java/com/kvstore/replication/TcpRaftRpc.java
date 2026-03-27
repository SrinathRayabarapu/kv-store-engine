package com.kvstore.replication;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link RaftRpc} over TCP: one connection per RPC (simple, correct; pool could be added later).
 *
 * <p>Frame format: {@code int length} (big-endian) + payload bytes produced by {@link RaftWireCodec}.
 */
public final class TcpRaftRpc implements RaftRpc, Closeable {

    private static final Logger LOG = Logger.getLogger(TcpRaftRpc.class.getName());

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 10_000;

    private final Map<Integer, InetSocketAddress> peerRaftAddresses;
    private volatile boolean closed;

    public TcpRaftRpc(Map<Integer, InetSocketAddress> peerRaftAddresses) {
        this.peerRaftAddresses = peerRaftAddresses;
    }

    @Override
    public VoteResponse requestVote(int peerId, VoteRequest request) {
        byte[] body;
        try {
            body = RaftWireCodec.encodeVoteRequest(request);
        } catch (IOException e) {
            return null;
        }
        byte[] resp = send(peerId, body);
        if (resp == null) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(resp));
            byte type = in.readByte();
            if (type != RaftWireCodec.MSG_VOTE_RESPONSE) {
                return null;
            }
            return RaftWireCodec.decodeVoteResponse(in);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public AppendResponse appendEntries(int peerId, AppendRequest request) {
        byte[] body;
        try {
            body = RaftWireCodec.encodeAppendRequest(request);
        } catch (IOException e) {
            return null;
        }
        byte[] resp = send(peerId, body);
        if (resp == null) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(resp));
            byte type = in.readByte();
            if (type != RaftWireCodec.MSG_APPEND_RESPONSE) {
                return null;
            }
            return RaftWireCodec.decodeAppendResponse(in);
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] send(int peerId, byte[] payload) {
        if (closed) return null;
        InetSocketAddress addr = peerRaftAddresses.get(peerId);
        if (addr == null) {
            LOG.warning("No raft address for peer " + peerId);
            return null;
        }
        try (Socket s = new Socket()) {
            s.connect(addr, CONNECT_TIMEOUT_MS);
            s.setSoTimeout(READ_TIMEOUT_MS);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
            int len = in.readInt();
            if (len < 0 || len > 64 * 1024 * 1024) {
                return null;
            }
            byte[] r = new byte[len];
            in.readFully(r);
            return r;
        } catch (IOException e) {
            LOG.log(Level.FINE, "Raft RPC to peer " + peerId + " failed: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
