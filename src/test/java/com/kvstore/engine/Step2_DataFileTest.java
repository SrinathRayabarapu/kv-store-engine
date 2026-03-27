package com.kvstore.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 2 acceptance tests — verifies the append-only data file that stores
 * serialized records. A bug here causes data loss or read corruption.
 */
@DisplayName("Step 2 — DataFile")
class Step2_DataFileTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("append() returns the exact byte offset where the record was written")
    void append_returnsCorrectOffset() {
        try (DataFile df = new DataFile(tempDir, "00001")) {
            byte[] record1 = RecordSerializer.serialize("key1", "val1".getBytes(), 1L);
            byte[] record2 = RecordSerializer.serialize("key2", "val2".getBytes(), 2L);

            long offset1 = df.append(record1);
            long offset2 = df.append(record2);

            assertEquals(0, offset1, "First record should start at offset 0");
            assertEquals(record1.length, offset2, "Second record should start immediately after the first");
        }
    }

    @Test
    @DisplayName("read(offset, size) returns the exact bytes written at that offset")
    void read_returnsExactBytesWritten() {
        try (DataFile df = new DataFile(tempDir, "00001")) {
            byte[] record = RecordSerializer.serialize("test-key", "test-value".getBytes(), 100L);
            long offset = df.append(record);

            byte[] readBack = df.read(offset, record.length);

            assertArrayEquals(record, readBack);
        }
    }

    @Test
    @DisplayName("Multiple records written and read back individually")
    void multipleRecords_readBackCorrectly() {
        try (DataFile df = new DataFile(tempDir, "00001")) {
            int count = 100;
            long[] offsets = new long[count];
            byte[][] records = new byte[count][];

            for (int i = 0; i < count; i++) {
                records[i] = RecordSerializer.serialize("key-" + i, ("value-" + i).getBytes(), i);
                offsets[i] = df.append(records[i]);
            }

            for (int i = 0; i < count; i++) {
                byte[] readBack = df.read(offsets[i], records[i].length);
                assertArrayEquals(records[i], readBack, "Record " + i + " mismatch");
            }
        }
    }

    @Test
    @DisplayName("File persists after close and can be reopened for reading")
    void persistenceAfterClose() {
        byte[] record = RecordSerializer.serialize("persist-key", "persist-value".getBytes(), 42L);
        long offset;

        try (DataFile df = new DataFile(tempDir, "00001")) {
            offset = df.append(record);
        }

        // Reopen the same file
        try (DataFile df = new DataFile(tempDir, "00001")) {
            byte[] readBack = df.read(offset, record.length);
            assertArrayEquals(record, readBack);
            assertEquals(record.length, df.getWriteOffset(),
                    "Write offset should reflect existing file size on reopen");
        }
    }

    @Test
    @DisplayName("Deserialized data from file is identical to the original key/value")
    void roundTrip_throughFileAndDeserializer() {
        String key = "roundtrip";
        byte[] value = "data-integrity-check".getBytes(StandardCharsets.UTF_8);
        long ts = System.currentTimeMillis();

        byte[] serialized = RecordSerializer.serialize(key, value, ts);

        try (DataFile df = new DataFile(tempDir, "00001")) {
            long offset = df.append(serialized);
            byte[] raw = df.read(offset, serialized.length);
            RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(raw);

            assertEquals(key, record.key());
            assertArrayEquals(value, record.value());
            assertEquals(ts, record.timestamp());
            assertFalse(record.tombstone());
        }
    }

    @Test
    @DisplayName("100 concurrent writers produce 100 distinct non-overlapping offset ranges")
    void concurrentWriters_produceNonOverlappingOffsets() throws Exception {
        try (DataFile df = new DataFile(tempDir, "00001")) {
            int writerCount = 100;
            ExecutorService executor = Executors.newFixedThreadPool(writerCount);
            List<Future<long[]>> futures = new ArrayList<>();

            for (int i = 0; i < writerCount; i++) {
                final int idx = i;
                futures.add(executor.submit(() -> {
                    byte[] record = RecordSerializer.serialize("key-" + idx, ("val-" + idx).getBytes(), idx);
                    long offset = df.append(record);
                    return new long[]{offset, record.length};
                }));
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

            // Collect all (offset, length) pairs and verify no overlaps
            List<long[]> ranges = new ArrayList<>();
            for (Future<long[]> f : futures) {
                ranges.add(f.get());
            }

            // Sort by offset
            ranges.sort((a, b) -> Long.compare(a[0], b[0]));

            for (int i = 1; i < ranges.size(); i++) {
                long prevEnd = ranges.get(i - 1)[0] + ranges.get(i - 1)[1];
                long currStart = ranges.get(i)[0];
                assertTrue(prevEnd <= currStart,
                        "Overlap detected: prev ends at " + prevEnd + ", curr starts at " + currStart);
            }

            // Verify we can read back each record
            for (int i = 0; i < writerCount; i++) {
                long[] range = ranges.get(i);
                byte[] readBack = df.read(range[0], (int) range[1]);
                RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(readBack);
                assertNotNull(record.key());
                assertNotNull(record.value());
            }
        }
    }

    @Test
    @DisplayName("getWriteOffset tracks cumulative bytes written")
    void writeOffset_tracksCorrectly() {
        try (DataFile df = new DataFile(tempDir, "00001")) {
            assertEquals(0, df.getWriteOffset());

            byte[] r1 = RecordSerializer.serialize("a", "1".getBytes(), 1L);
            df.append(r1);
            assertEquals(r1.length, df.getWriteOffset());

            byte[] r2 = RecordSerializer.serialize("b", "2".getBytes(), 2L);
            df.append(r2);
            assertEquals(r1.length + r2.length, df.getWriteOffset());
        }
    }

    @Test
    @DisplayName("readAll returns complete file contents for crash recovery replay")
    void readAll_returnsCompleteContents() {
        try (DataFile df = new DataFile(tempDir, "00001")) {
            byte[] r1 = RecordSerializer.serialize("key1", "val1".getBytes(), 1L);
            byte[] r2 = RecordSerializer.serialize("key2", "val2".getBytes(), 2L);
            df.append(r1);
            df.append(r2);

            byte[] all = df.readAll();
            assertEquals(r1.length + r2.length, all.length);

            // Verify we can deserialize both records from the combined bytes
            RecordSerializer.DeserializedRecord rec1 = RecordSerializer.deserialize(
                    java.util.Arrays.copyOfRange(all, 0, r1.length));
            assertEquals("key1", rec1.key());

            RecordSerializer.DeserializedRecord rec2 = RecordSerializer.deserialize(
                    java.util.Arrays.copyOfRange(all, r1.length, all.length));
            assertEquals("key2", rec2.key());
        }
    }
}
