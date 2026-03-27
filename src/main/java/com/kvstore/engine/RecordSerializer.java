package com.kvstore.engine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Encodes and decodes on-disk records for the append-only data file.
 *
 * <h3>Binary format (big-endian):</h3>
 * <pre>
 * ┌──────────┬──────────┬──────────┬──────────┬──────...──────┬──────...──────┐
 * │  CRC32   │Timestamp │ KeySize  │ ValSize  │   Key Bytes   │  Value Bytes  │
 * │  4 bytes │  8 bytes │  4 bytes │  4 bytes │ KeySize bytes │ValSize bytes  │
 * └──────────┴──────────┴──────────┴──────────┴──────...──────┴──────...──────┘
 * </pre>
 *
 * <p>The CRC32 covers everything after itself (timestamp through value bytes).
 * This lets us detect both data corruption and torn writes — a partial record
 * written during a crash will fail the checksum on recovery.
 *
 * <p>A tombstone (delete marker) uses {@code valSize = -1} with zero value bytes.
 * This avoids a separate "deleted" flag and keeps the format fixed-width in the header.
 *
 * <p>Header size: {@value #HEADER_SIZE} bytes.
 */
public final class RecordSerializer {

    /** Fixed header: CRC32(4) + timestamp(8) + keySize(4) + valSize(4) = 20 bytes. */
    public static final int HEADER_SIZE = 20;

    /** Sentinel value size indicating a tombstone (delete marker). */
    public static final int TOMBSTONE_VALUE_SIZE = -1;

    private RecordSerializer() {
    }

    /**
     * Serializes a key/value pair into the on-disk binary format.
     *
     * @param key       UTF-8 key (non-null, non-empty)
     * @param value     value bytes (non-null; may be zero-length)
     * @param timestamp epoch millis — typically {@code System.currentTimeMillis()}
     * @return complete record bytes including CRC32 header
     */
    public static byte[] serialize(String key, byte[] value, long timestamp) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int totalSize = HEADER_SIZE + keyBytes.length + value.length;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(0); // placeholder for CRC32
        buffer.putLong(timestamp);
        buffer.putInt(keyBytes.length);
        buffer.putInt(value.length);
        buffer.put(keyBytes);
        buffer.put(value);

        byte[] result = buffer.array();
        int crc = computeCrc(result, 4, result.length - 4);
        ByteBuffer.wrap(result).putInt(crc);

        return result;
    }

    /**
     * Serializes a tombstone record (delete marker) for the given key.
     *
     * <p>The value size field is set to {@value #TOMBSTONE_VALUE_SIZE} and no value
     * bytes follow the key. During recovery, a tombstone removes the key from the
     * in-memory index.
     *
     * @param key       UTF-8 key to delete
     * @param timestamp epoch millis
     * @return complete tombstone record bytes
     */
    public static byte[] serializeTombstone(String key, long timestamp) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int totalSize = HEADER_SIZE + keyBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(0); // placeholder for CRC32
        buffer.putLong(timestamp);
        buffer.putInt(keyBytes.length);
        buffer.putInt(TOMBSTONE_VALUE_SIZE);
        buffer.put(keyBytes);

        byte[] result = buffer.array();
        int crc = computeCrc(result, 4, result.length - 4);
        ByteBuffer.wrap(result).putInt(crc);

        return result;
    }

    /**
     * Deserializes a record from raw bytes, verifying CRC32 integrity.
     *
     * @param data raw record bytes (must be at least {@value #HEADER_SIZE} bytes)
     * @return a {@link DeserializedRecord} containing the parsed fields
     * @throws CorruptedRecordException if the CRC32 check fails or the data is truncated
     */
    public static DeserializedRecord deserialize(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new CorruptedRecordException(
                    "Record too short: " + data.length + " bytes, minimum is " + HEADER_SIZE);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int storedCrc = buffer.getInt();
        long timestamp = buffer.getLong();
        int keySize = buffer.getInt();
        int valSize = buffer.getInt();

        int expectedCrc = computeCrc(data, 4, data.length - 4);
        if (storedCrc != expectedCrc) {
            throw new CorruptedRecordException(
                    "CRC32 mismatch: stored=0x" + Integer.toHexString(storedCrc)
                            + ", computed=0x" + Integer.toHexString(expectedCrc));
        }

        if (keySize < 0) {
            throw new CorruptedRecordException("Negative key size: " + keySize);
        }

        byte[] keyBytes = new byte[keySize];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        boolean tombstone = (valSize == TOMBSTONE_VALUE_SIZE);
        byte[] value;
        if (tombstone) {
            value = new byte[0];
        } else {
            if (valSize < 0) {
                throw new CorruptedRecordException("Invalid value size: " + valSize);
            }
            value = new byte[valSize];
            buffer.get(value);
        }

        return new DeserializedRecord(key, value, timestamp, tombstone);
    }

    /**
     * Reads the header from a ByteBuffer positioned at the start of a record,
     * returning the total record size (header + key + value bytes).
     * Does NOT advance the buffer position past the header.
     *
     * @param buffer positioned at the CRC32 field of a record
     * @return total record size in bytes, or -1 if not enough data for a header
     */
    public static int peekRecordSize(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            return -1;
        }
        int pos = buffer.position();
        buffer.position(pos + 12); // skip CRC32(4) + timestamp(8)
        int keySize = buffer.getInt();
        int valSize = buffer.getInt();
        buffer.position(pos); // restore

        int valueBytes = (valSize == TOMBSTONE_VALUE_SIZE) ? 0 : valSize;
        return HEADER_SIZE + keySize + valueBytes;
    }

    /**
     * Computes the total byte size of a record for a given key/value pair,
     * without allocating the full buffer.
     */
    public static int recordSize(String key, byte[] value) {
        return HEADER_SIZE + key.getBytes(StandardCharsets.UTF_8).length + value.length;
    }

    /**
     * Computes the total byte size of a tombstone record for a given key.
     */
    public static int tombstoneSize(String key) {
        return HEADER_SIZE + key.getBytes(StandardCharsets.UTF_8).length;
    }

    private static int computeCrc(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    /**
     * Immutable carrier for a deserialized record's fields.
     *
     * @param key       the UTF-8 key
     * @param value     the value bytes (empty array for tombstones)
     * @param timestamp epoch millis when the record was written
     * @param tombstone true if this record represents a deletion
     */
    public record DeserializedRecord(String key, byte[] value, long timestamp, boolean tombstone) {
    }
}
