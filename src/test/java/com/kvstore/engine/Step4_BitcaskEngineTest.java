package com.kvstore.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 4 acceptance tests — verifies all 5 API operations of the BitcaskEngine.
 * This is where DataFile + KeyDir + RecordSerializer are wired together
 * into a functional storage engine.
 */
@DisplayName("Step 4 — BitcaskEngine (Core CRUD)")
class Step4_BitcaskEngineTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("put(k,v) then get(k) returns v for 10,000 random key/value pairs")
    void putThenGet_10kRandomPairs() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            Random rng = new Random(42);
            Map<String, byte[]> expected = new LinkedHashMap<>();

            for (int i = 0; i < 10_000; i++) {
                String key = "key-" + i;
                byte[] value = new byte[10 + rng.nextInt(100)];
                rng.nextBytes(value);
                engine.put(key, value);
                expected.put(key, value);
            }

            for (Map.Entry<String, byte[]> e : expected.entrySet()) {
                Optional<byte[]> actual = engine.get(e.getKey());
                assertTrue(actual.isPresent(), "Missing key: " + e.getKey());
                assertArrayEquals(e.getValue(), actual.get(), "Value mismatch for: " + e.getKey());
            }
        }
    }

    @Test
    @DisplayName("put(k,v1) then put(k,v2) then get(k) returns v2 (latest write wins)")
    void put_overwrite_latestWriteWins() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            byte[] v1 = "first".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "second".getBytes(StandardCharsets.UTF_8);

            engine.put("overwrite-key", v1);
            engine.put("overwrite-key", v2);

            byte[] result = engine.get("overwrite-key").orElseThrow();
            assertArrayEquals(v2, result);
        }
    }

    @Test
    @DisplayName("delete(k) then get(k) returns Optional.empty()")
    void delete_removesKey() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("delete-me", "value".getBytes());
            engine.delete("delete-me");

            assertTrue(engine.get("delete-me").isEmpty());
        }
    }

    @Test
    @DisplayName("get on non-existent key returns Optional.empty()")
    void get_nonExistentKey_returnsEmpty() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            assertTrue(engine.get("never-written").isEmpty());
        }
    }

    @Test
    @DisplayName("batchPut writes all keys atomically")
    void batchPut_writesAllKeys() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            List<String> keys = List.of("batch-a", "batch-b", "batch-c");
            List<byte[]> values = List.of(
                    "val-a".getBytes(), "val-b".getBytes(), "val-c".getBytes());

            engine.batchPut(keys, values);

            for (int i = 0; i < keys.size(); i++) {
                byte[] result = engine.get(keys.get(i)).orElseThrow();
                assertArrayEquals(values.get(i), result);
            }
        }
    }

    @Test
    @DisplayName("batchPut with mismatched sizes throws IllegalArgumentException")
    void batchPut_mismatchedSizes_throws() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            assertThrows(IllegalArgumentException.class,
                    () -> engine.batchPut(
                            List.of("a", "b"),
                            List.of("v1".getBytes())));
        }
    }

    @Test
    @DisplayName("readKeyRange returns correct sorted results")
    void readKeyRange_returnsSortedResults() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("banana", "yellow".getBytes());
            engine.put("apple", "red".getBytes());
            engine.put("cherry", "dark-red".getBytes());
            engine.put("date", "brown".getBytes());
            engine.put("avocado", "green".getBytes());

            List<Map.Entry<String, byte[]>> range = engine.readKeyRange("avocado", "cherry");

            assertEquals(3, range.size());
            assertEquals("avocado", range.get(0).getKey());
            assertEquals("banana", range.get(1).getKey());
            assertEquals("cherry", range.get(2).getKey());
            assertArrayEquals("green".getBytes(), range.get(0).getValue());
        }
    }

    @Test
    @DisplayName("readKeyRange excludes deleted keys")
    void readKeyRange_excludesDeletedKeys() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("a", "1".getBytes());
            engine.put("b", "2".getBytes());
            engine.put("c", "3".getBytes());
            engine.delete("b");

            List<Map.Entry<String, byte[]>> range = engine.readKeyRange("a", "c");
            assertEquals(2, range.size());
            assertEquals("a", range.get(0).getKey());
            assertEquals("c", range.get(1).getKey());
        }
    }

    @Test
    @DisplayName("File rotation occurs when max file size is exceeded")
    void fileRotation_whenMaxSizeExceeded() {
        // Use a small max file size to force rotation
        long smallMax = 200;
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, smallMax)) {
            // Write enough data to force at least one rotation
            for (int i = 0; i < 20; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }

            assertTrue(engine.getDataFiles().size() > 1,
                    "Expected multiple data files after rotation, got: " + engine.getDataFiles().size());

            // All keys should still be readable
            for (int i = 0; i < 20; i++) {
                assertTrue(engine.get("key-" + i).isPresent(), "Key lost after rotation: key-" + i);
            }
        }
    }

    @Test
    @DisplayName("Concurrent put/get operations complete without errors")
    void concurrentPutGet_noErrors() throws Exception {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            int writerCount = 4;
            int readerCount = 4;
            int opsPerThread = 5_000;
            ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            // Writers
            for (int w = 0; w < writerCount; w++) {
                final int writerId = w;
                futures.add(executor.submit(() -> {
                    try { startLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    for (int i = 0; i < opsPerThread; i++) {
                        engine.put("w" + writerId + "-k" + i, ("v" + i).getBytes());
                    }
                }));
            }

            // Readers
            for (int r = 0; r < readerCount; r++) {
                futures.add(executor.submit(() -> {
                    try { startLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    for (int i = 0; i < opsPerThread; i++) {
                        engine.get("w0-k" + (i % opsPerThread));
                    }
                }));
            }

            startLatch.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            for (Future<?> f : futures) {
                f.get(); // rethrow any exceptions
            }

            // Verify all written keys are readable
            for (int w = 0; w < writerCount; w++) {
                for (int i = 0; i < opsPerThread; i++) {
                    assertTrue(engine.get("w" + w + "-k" + i).isPresent());
                }
            }
        }
    }

    @Test
    @DisplayName("Empty value stored and retrieved correctly")
    void emptyValue_roundTrips() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("empty", new byte[0]);

            byte[] result = engine.get("empty").orElseThrow();
            assertEquals(0, result.length);
        }
    }

    @Test
    @DisplayName("Delete non-existent key is a no-op (no exception)")
    void delete_nonExistentKey_isNoOp() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            assertDoesNotThrow(() -> engine.delete("never-existed"));
        }
    }
}
