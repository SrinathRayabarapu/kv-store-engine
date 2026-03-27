package com.kvstore.network;

import com.kvstore.engine.StorageEngine;
import com.kvstore.replication.LogEntry;
import com.kvstore.replication.RaftNode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dispatches parsed protocol requests to the {@link StorageEngine} and
 * encodes the responses. When {@link RaftRouting} is present, mutating
 * operations and strongly consistent reads go through the Raft leader;
 * followers respond with {@link Protocol#STATUS_REDIRECT}.
 */
public class RequestHandler {

    private static final Logger LOG = Logger.getLogger(RequestHandler.class.getName());

    /** Max time to wait for a majority commit after appending on the leader. */
    private static final long RAFT_WRITE_TIMEOUT_SEC = 30;

    private final StorageEngine engine;
    private final RaftRouting raft;

    public RequestHandler(StorageEngine engine) {
        this(engine, null);
    }

    public RequestHandler(StorageEngine engine, RaftRouting raft) {
        this.engine = engine;
        this.raft = raft;
    }

    /**
     * Handles a parsed request and returns the encoded response bytes.
     */
    public byte[] handle(Protocol.ParsedRequest request) {
        try {
            return switch (request.opCode()) {
                case Protocol.OP_PUT -> handlePut(request.payload());
                case Protocol.OP_GET -> handleGet(request.payload());
                case Protocol.OP_DELETE -> handleDelete(request.payload());
                case Protocol.OP_RANGE -> handleRange(request.payload());
                case Protocol.OP_BATCH_PUT -> handleBatchPut(request.payload());
                default -> Protocol.encodeError("Unknown opcode: " + request.opCode());
            };
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Request handling error", e);
            return Protocol.encodeError(e.getMessage());
        }
    }

    private byte[] redirectOrNull() {
        if (raft == null) {
            return null;
        }
        return raft.redirectIfNotLeader();
    }

    private byte[] handlePut(ByteBuffer payload) throws Exception {
        String key = Protocol.readString(payload);
        byte[] value = Protocol.readBytes(payload);
        byte[] r = redirectOrNull();
        if (r != null) {
            return r;
        }
        if (raft != null) {
            CompletableFuture<Boolean> f = raft.getRaft().submitWrite(LogEntry.OP_PUT, key, value);
            if (!f.get(RAFT_WRITE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                return Protocol.encodeError("replication failed or lost leadership");
            }
            return Protocol.encodeOkEmpty();
        }
        engine.put(key, value);
        return Protocol.encodeOkEmpty();
    }

    private byte[] handleGet(ByteBuffer payload) throws Exception {
        String key = Protocol.readString(payload);
        byte[] r = redirectOrNull();
        if (r != null) {
            return r;
        }
        Optional<byte[]> value = engine.get(key);
        if (value.isPresent()) {
            return Protocol.encodeOk(value.get());
        }
        return Protocol.encodeNotFound();
    }

    private byte[] handleDelete(ByteBuffer payload) throws Exception {
        String key = Protocol.readString(payload);
        byte[] r = redirectOrNull();
        if (r != null) {
            return r;
        }
        if (raft != null) {
            CompletableFuture<Boolean> f = raft.getRaft().submitWrite(LogEntry.OP_DELETE, key, new byte[0]);
            if (!f.get(RAFT_WRITE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                return Protocol.encodeError("replication failed or lost leadership");
            }
            return Protocol.encodeOkEmpty();
        }
        engine.delete(key);
        return Protocol.encodeOkEmpty();
    }

    private byte[] handleRange(ByteBuffer payload) throws Exception {
        String startKey = Protocol.readString(payload);
        String endKey = Protocol.readString(payload);
        byte[] r = redirectOrNull();
        if (r != null) {
            return r;
        }
        List<Map.Entry<String, byte[]>> results = engine.readKeyRange(startKey, endKey);
        return Protocol.encodeRangeResponse(results);
    }

    private byte[] handleBatchPut(ByteBuffer payload) throws Exception {
        byte[] r = redirectOrNull();
        if (r != null) {
            return r;
        }
        int count = payload.getInt();
        List<String> keys = new ArrayList<>(count);
        List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(Protocol.readString(payload));
            values.add(Protocol.readBytes(payload));
        }
        if (raft != null) {
            RaftNode node = raft.getRaft();
            for (int i = 0; i < count; i++) {
                CompletableFuture<Boolean> f = node.submitWrite(LogEntry.OP_PUT, keys.get(i), values.get(i));
                if (!f.get(RAFT_WRITE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    return Protocol.encodeError("replication failed or lost leadership");
                }
            }
            return Protocol.encodeOkEmpty();
        }
        engine.batchPut(keys, values);
        return Protocol.encodeOkEmpty();
    }
}
