package com.kvstore.network;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Binary wire protocol codec for the KV store TCP server.
 *
 * <h3>Request format:</h3>
 * <pre>
 * ┌──────────┬────────────┬──────────...──────┐
 * │ OpCode   │ PayloadLen │  Payload Bytes    │
 * │  1 byte  │  4 bytes   │                   │
 * └──────────┴────────────┴──────────...──────┘
 * </pre>
 *
 * <h3>Response format:</h3>
 * <pre>
 * ┌──────────┬────────────┬──────────...──────┐
 * │  Status  │ PayloadLen │  Payload Bytes    │
 * │  1 byte  │  4 bytes   │                   │
 * └──────────┴────────────┴──────────...──────┘
 * </pre>
 */
public final class Protocol {

    public static final byte OP_PUT       = 0x01;
    public static final byte OP_GET       = 0x02;
    public static final byte OP_DELETE    = 0x03;
    public static final byte OP_RANGE     = 0x04;
    public static final byte OP_BATCH_PUT = 0x05;

    public static final byte STATUS_OK        = 0x00;
    public static final byte STATUS_NOT_FOUND = 0x01;
    public static final byte STATUS_ERROR     = 0x02;
    public static final byte STATUS_REDIRECT  = 0x03;

    /** Minimum request size: opcode(1) + payloadLen(4) = 5 bytes. */
    public static final int REQUEST_HEADER_SIZE = 5;

    private Protocol() {
    }

    // --- Request encoding (client side) ---

    public static byte[] encodePutRequest(String key, byte[] value) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 4 + keyBytes.length + 4 + value.length;
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_HEADER_SIZE + payloadLen);
        buf.put(OP_PUT);
        buf.putInt(payloadLen);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(value.length);
        buf.put(value);
        return buf.array();
    }

    public static byte[] encodeGetRequest(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 4 + keyBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_HEADER_SIZE + payloadLen);
        buf.put(OP_GET);
        buf.putInt(payloadLen);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        return buf.array();
    }

    public static byte[] encodeDeleteRequest(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 4 + keyBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_HEADER_SIZE + payloadLen);
        buf.put(OP_DELETE);
        buf.putInt(payloadLen);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        return buf.array();
    }

    public static byte[] encodeRangeRequest(String startKey, String endKey) {
        byte[] startBytes = startKey.getBytes(StandardCharsets.UTF_8);
        byte[] endBytes = endKey.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 4 + startBytes.length + 4 + endBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_HEADER_SIZE + payloadLen);
        buf.put(OP_RANGE);
        buf.putInt(payloadLen);
        buf.putInt(startBytes.length);
        buf.put(startBytes);
        buf.putInt(endBytes.length);
        buf.put(endBytes);
        return buf.array();
    }

    public static byte[] encodeBatchPutRequest(List<String> keys, List<byte[]> values) {
        int payloadLen = 4; // count
        for (int i = 0; i < keys.size(); i++) {
            byte[] keyBytes = keys.get(i).getBytes(StandardCharsets.UTF_8);
            payloadLen += 4 + keyBytes.length + 4 + values.get(i).length;
        }
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_HEADER_SIZE + payloadLen);
        buf.put(OP_BATCH_PUT);
        buf.putInt(payloadLen);
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            byte[] keyBytes = keys.get(i).getBytes(StandardCharsets.UTF_8);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            buf.putInt(values.get(i).length);
            buf.put(values.get(i));
        }
        return buf.array();
    }

    // --- Response encoding (server side) ---

    public static byte[] encodeResponse(byte status, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + payload.length);
        buf.put(status);
        buf.putInt(payload.length);
        buf.put(payload);
        return buf.array();
    }

    public static byte[] encodeOk(byte[] payload) {
        return encodeResponse(STATUS_OK, payload);
    }

    public static byte[] encodeOkEmpty() {
        return encodeResponse(STATUS_OK, new byte[0]);
    }

    public static byte[] encodeNotFound() {
        return encodeResponse(STATUS_NOT_FOUND, new byte[0]);
    }

    public static byte[] encodeError(String message) {
        return encodeResponse(STATUS_ERROR, message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Encodes a range query response: count(4) + [keyLen(4) + key + valLen(4) + value] * count.
     */
    public static byte[] encodeRangeResponse(List<Map.Entry<String, byte[]>> entries) {
        int payloadLen = 4;
        for (Map.Entry<String, byte[]> e : entries) {
            byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
            payloadLen += 4 + keyBytes.length + 4 + e.getValue().length;
        }
        ByteBuffer buf = ByteBuffer.allocate(payloadLen);
        buf.putInt(entries.size());
        for (Map.Entry<String, byte[]> e : entries) {
            byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            buf.putInt(e.getValue().length);
            buf.put(e.getValue());
        }
        return encodeOk(buf.array());
    }

    // --- Request parsing (server side) ---

    /**
     * Parsed request from the wire. The payload ByteBuffer is positioned
     * at the start of the operation-specific data.
     */
    public record ParsedRequest(byte opCode, ByteBuffer payload) {
    }

    /**
     * Attempts to parse a complete request from the buffer.
     * Returns null if not enough data is available yet.
     * On success, the consumed bytes are removed from the buffer.
     */
    public static ParsedRequest tryParseRequest(ByteBuffer buffer) {
        if (buffer.remaining() < REQUEST_HEADER_SIZE) {
            return null;
        }

        buffer.mark();
        byte opCode = buffer.get();
        int payloadLen = buffer.getInt();

        if (buffer.remaining() < payloadLen) {
            buffer.reset();
            return null;
        }

        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        return new ParsedRequest(opCode, ByteBuffer.wrap(payload));
    }

    /** Reads a length-prefixed string from the buffer. */
    public static String readString(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Reads a length-prefixed byte array from the buffer. */
    public static byte[] readBytes(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }

    // --- Response parsing (client side) ---

    public record ParsedResponse(byte status, byte[] payload) {
    }

    /**
     * Parses a response from raw bytes.
     */
    public static ParsedResponse parseResponse(ByteBuffer buffer) {
        if (buffer.remaining() < 5) return null;

        buffer.mark();
        byte status = buffer.get();
        int payloadLen = buffer.getInt();

        if (buffer.remaining() < payloadLen) {
            buffer.reset();
            return null;
        }

        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        return new ParsedResponse(status, payload);
    }

    /**
     * Decodes a range response payload into key/value entries.
     */
    public static List<Map.Entry<String, byte[]>> decodeRangePayload(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int count = buf.getInt();
        List<Map.Entry<String, byte[]>> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = readString(buf);
            byte[] value = readBytes(buf);
            entries.add(Map.entry(key, value));
        }
        return entries;
    }
}
