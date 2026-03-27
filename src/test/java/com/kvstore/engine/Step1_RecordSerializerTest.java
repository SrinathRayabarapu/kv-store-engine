package com.kvstore.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 1 acceptance tests — verifies the binary record serializer that
 * underpins all on-disk data. A bug here corrupts every record.
 */
@DisplayName("Step 1 — RecordSerializer")
class Step1_RecordSerializerTest {

    @Test
    @DisplayName("Normal key/value round-trips through serialize → deserialize")
    void roundTrip_normalRecord() {
        String key = "user:1001";
        byte[] value = "John Doe".getBytes(StandardCharsets.UTF_8);
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);
        RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(bytes);

        assertEquals(key, record.key());
        assertArrayEquals(value, record.value());
        assertEquals(ts, record.timestamp());
        assertFalse(record.tombstone());
    }

    @Test
    @DisplayName("Empty value (zero-length byte array) round-trips correctly")
    void roundTrip_emptyValue() {
        String key = "empty-val";
        byte[] value = new byte[0];
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);
        RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(bytes);

        assertEquals(key, record.key());
        assertArrayEquals(value, record.value());
        assertFalse(record.tombstone());
    }

    @Test
    @DisplayName("Binary (non-UTF-8) value data round-trips correctly")
    void roundTrip_binaryData() {
        String key = "binary-key";
        byte[] value = new byte[256];
        for (int i = 0; i < 256; i++) {
            value[i] = (byte) i;
        }
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);
        RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(bytes);

        assertEquals(key, record.key());
        assertArrayEquals(value, record.value());
    }

    @Test
    @DisplayName("Unicode key (emoji, CJK, etc.) round-trips correctly")
    void roundTrip_unicodeKey() {
        String key = "用户:🚀:données";
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);
        RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(bytes);

        assertEquals(key, record.key());
        assertArrayEquals(value, record.value());
    }

    @Test
    @DisplayName("Tombstone has negative value size and tombstone flag set")
    void tombstone_serializedWithNegativeValueSize() {
        String key = "deleted-key";
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serializeTombstone(key, ts);
        RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(bytes);

        assertEquals(key, record.key());
        assertTrue(record.tombstone());
        assertEquals(0, record.value().length);

        // Verify the raw bytes contain TOMBSTONE_VALUE_SIZE (-1) at the valSize position
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.position(16); // skip CRC32(4) + timestamp(8) + keySize(4)
        assertEquals(RecordSerializer.TOMBSTONE_VALUE_SIZE, buf.getInt());
    }

    @Test
    @DisplayName("Flipping a single bit in the payload triggers CorruptedRecordException")
    void corruption_throwsCorruptedRecordException() {
        String key = "integrity-check";
        byte[] value = "important-data".getBytes(StandardCharsets.UTF_8);
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);

        // Flip one bit in the value region (well past the CRC32 header)
        int corruptIndex = bytes.length - 1;
        bytes[corruptIndex] ^= 0x01;

        assertThrows(CorruptedRecordException.class, () -> RecordSerializer.deserialize(bytes));
    }

    @Test
    @DisplayName("1 MB value round-trips correctly — verifies no buffer overflow")
    void largeValue_1MB_roundTrips_correctly() {
        String key = "big-value";
        byte[] value = new byte[1024 * 1024]; // 1 MB
        new Random(42).nextBytes(value);
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);
        RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(bytes);

        assertEquals(key, record.key());
        assertArrayEquals(value, record.value());
    }

    @Test
    @DisplayName("Truncated record (less than header size) throws CorruptedRecordException")
    void truncatedRecord_throwsCorruptedRecordException() {
        byte[] tooShort = new byte[10]; // less than 20-byte header
        assertThrows(CorruptedRecordException.class, () -> RecordSerializer.deserialize(tooShort));
    }

    @Test
    @DisplayName("peekRecordSize returns correct total size without advancing buffer")
    void peekRecordSize_returnsCorrectSize() {
        String key = "peek-test";
        byte[] value = "val".getBytes(StandardCharsets.UTF_8);
        long ts = System.currentTimeMillis();

        byte[] bytes = RecordSerializer.serialize(key, value, ts);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int size = RecordSerializer.peekRecordSize(buffer);

        assertEquals(bytes.length, size);
        assertEquals(0, buffer.position(), "Buffer position should be restored");
    }

    @Test
    @DisplayName("recordSize and tombstoneSize match actual serialized lengths")
    void sizeCalculation_matchesActualSerialization() {
        String key = "size-check";
        byte[] value = "data".getBytes(StandardCharsets.UTF_8);

        assertEquals(
                RecordSerializer.serialize(key, value, 0L).length,
                RecordSerializer.recordSize(key, value));

        assertEquals(
                RecordSerializer.serializeTombstone(key, 0L).length,
                RecordSerializer.tombstoneSize(key));
    }
}
