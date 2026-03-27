package com.kvstore.replication;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary codec for Raft RPC payloads over TCP (length-prefixed frames use this as body).
 *
 * <p>Message types: {@value #MSG_VOTE_REQUEST}, {@value #MSG_VOTE_RESPONSE},
 * {@value #MSG_APPEND_REQUEST}, {@value #MSG_APPEND_RESPONSE}.
 */
public final class RaftWireCodec {

    public static final byte MSG_VOTE_REQUEST   = 0x01;
    public static final byte MSG_VOTE_RESPONSE  = 0x02;
    public static final byte MSG_APPEND_REQUEST = 0x03;
    public static final byte MSG_APPEND_RESPONSE = 0x04;

    private RaftWireCodec() {
    }

    public static byte[] encodeVoteRequest(RaftRpc.VoteRequest r) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeByte(MSG_VOTE_REQUEST);
        out.writeLong(r.term());
        out.writeInt(r.candidateId());
        out.writeLong(r.lastLogIndex());
        out.writeLong(r.lastLogTerm());
        out.flush();
        return bos.toByteArray();
    }

    public static RaftRpc.VoteRequest decodeVoteRequest(DataInputStream in) throws IOException {
        long term = in.readLong();
        int candidateId = in.readInt();
        long lastLogIndex = in.readLong();
        long lastLogTerm = in.readLong();
        return new RaftRpc.VoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
    }

    public static byte[] encodeVoteResponse(RaftRpc.VoteResponse r) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeByte(MSG_VOTE_RESPONSE);
        out.writeLong(r.term());
        out.writeBoolean(r.voteGranted());
        out.flush();
        return bos.toByteArray();
    }

    public static RaftRpc.VoteResponse decodeVoteResponse(DataInputStream in) throws IOException {
        long term = in.readLong();
        boolean granted = in.readBoolean();
        return new RaftRpc.VoteResponse(term, granted);
    }

    public static byte[] encodeAppendRequest(RaftRpc.AppendRequest r) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeByte(MSG_APPEND_REQUEST);
        out.writeLong(r.term());
        out.writeInt(r.leaderId());
        out.writeLong(r.prevLogIndex());
        out.writeLong(r.prevLogTerm());
        out.writeLong(r.leaderCommit());
        List<LogEntry> entries = r.entries();
        out.writeInt(entries.size());
        for (LogEntry e : entries) {
            out.writeLong(e.term());
            out.writeLong(e.index());
            out.writeByte(e.opCode());
            byte[] kb = e.key().getBytes(StandardCharsets.UTF_8);
            out.writeInt(kb.length);
            out.write(kb);
            out.writeInt(e.value().length);
            out.write(e.value());
        }
        out.flush();
        return bos.toByteArray();
    }

    public static RaftRpc.AppendRequest decodeAppendRequest(DataInputStream in) throws IOException {
        long term = in.readLong();
        int leaderId = in.readInt();
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();
        long leaderCommit = in.readLong();
        int n = in.readInt();
        List<LogEntry> entries = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long eTerm = in.readLong();
            long eIndex = in.readLong();
            byte op = in.readByte();
            int kl = in.readInt();
            byte[] kb = new byte[kl];
            in.readFully(kb);
            String key = new String(kb, StandardCharsets.UTF_8);
            int vl = in.readInt();
            byte[] val = new byte[vl];
            in.readFully(val);
            entries.add(new LogEntry(eTerm, eIndex, op, key, val));
        }
        return new RaftRpc.AppendRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    public static byte[] encodeAppendResponse(RaftRpc.AppendResponse r) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeByte(MSG_APPEND_RESPONSE);
        out.writeLong(r.term());
        out.writeBoolean(r.success());
        out.flush();
        return bos.toByteArray();
    }

    public static RaftRpc.AppendResponse decodeAppendResponse(DataInputStream in) throws IOException {
        long term = in.readLong();
        boolean success = in.readBoolean();
        return new RaftRpc.AppendResponse(term, success);
    }

    /**
     * Dispatches on leading message byte and invokes the appropriate handler.
     */
    public static byte[] dispatch(byte[] payload, RaftNode node) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte type = in.readByte();
        return switch (type) {
            case MSG_VOTE_REQUEST -> {
                RaftRpc.VoteRequest req = decodeVoteRequest(in);
                RaftRpc.VoteResponse resp = node.handleVoteRequest(req);
                yield encodeVoteResponse(resp);
            }
            case MSG_APPEND_REQUEST -> {
                RaftRpc.AppendRequest req = decodeAppendRequest(in);
                RaftRpc.AppendResponse resp = node.handleAppendEntries(req);
                yield encodeAppendResponse(resp);
            }
            default -> throw new IOException("Unknown Raft message type: " + type);
        };
    }
}
