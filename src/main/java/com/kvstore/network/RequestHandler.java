package com.kvstore.network;

import com.kvstore.engine.StorageEngine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dispatches parsed protocol requests to the {@link StorageEngine} and
 * encodes the responses. Stateless — one instance is shared across all
 * connections.
 */
public class RequestHandler {

    private static final Logger LOG = Logger.getLogger(RequestHandler.class.getName());

    private final StorageEngine engine;

    public RequestHandler(StorageEngine engine) {
        this.engine = engine;
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

    private byte[] handlePut(ByteBuffer payload) {
        String key = Protocol.readString(payload);
        byte[] value = Protocol.readBytes(payload);
        engine.put(key, value);
        return Protocol.encodeOkEmpty();
    }

    private byte[] handleGet(ByteBuffer payload) {
        String key = Protocol.readString(payload);
        Optional<byte[]> value = engine.get(key);
        if (value.isPresent()) {
            return Protocol.encodeOk(value.get());
        } else {
            return Protocol.encodeNotFound();
        }
    }

    private byte[] handleDelete(ByteBuffer payload) {
        String key = Protocol.readString(payload);
        engine.delete(key);
        return Protocol.encodeOkEmpty();
    }

    private byte[] handleRange(ByteBuffer payload) {
        String startKey = Protocol.readString(payload);
        String endKey = Protocol.readString(payload);
        List<Map.Entry<String, byte[]>> results = engine.readKeyRange(startKey, endKey);
        return Protocol.encodeRangeResponse(results);
    }

    private byte[] handleBatchPut(ByteBuffer payload) {
        int count = payload.getInt();
        List<String> keys = new ArrayList<>(count);
        List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(Protocol.readString(payload));
            values.add(Protocol.readBytes(payload));
        }
        engine.batchPut(keys, values);
        return Protocol.encodeOkEmpty();
    }
}
