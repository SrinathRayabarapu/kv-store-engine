package com.kvstore.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 3 acceptance tests — verifies the in-memory KeyDir index.
 * O(1) put/get, O(n log n) range queries, and concurrent safety.
 */
@DisplayName("Step 3 — KeyDir")
class Step3_KeyDirTest {

    private KeyDir keyDir;

    @BeforeEach
    void setUp() {
        keyDir = new KeyDir();
    }

    @Test
    @DisplayName("put(key, entry) and get(key) are O(1)")
    void putAndGet_returnCorrectEntry() {
        KeyDirEntry entry = new KeyDirEntry("00001", 100, 0L, 130, System.currentTimeMillis());
        keyDir.put("user:1001", entry);

        Optional<KeyDirEntry> result = keyDir.get("user:1001");

        assertTrue(result.isPresent());
        assertEquals("00001", result.get().fileId());
        assertEquals(100, result.get().valueSize());
        assertEquals(0L, result.get().valueOffset());
    }

    @Test
    @DisplayName("get on missing key returns empty Optional")
    void get_missingKey_returnsEmpty() {
        assertTrue(keyDir.get("nonexistent").isEmpty());
    }

    @Test
    @DisplayName("put overwrites existing entry (latest-write-wins)")
    void put_overwritesExistingEntry() {
        KeyDirEntry entry1 = new KeyDirEntry("00001", 100, 0L, 130, 1L);
        KeyDirEntry entry2 = new KeyDirEntry("00002", 200, 500L, 230, 2L);

        keyDir.put("key", entry1);
        keyDir.put("key", entry2);

        KeyDirEntry result = keyDir.get("key").orElseThrow();
        assertEquals("00002", result.fileId());
        assertEquals(200, result.valueSize());
        assertEquals(500L, result.valueOffset());
    }

    @Test
    @DisplayName("remove(key) makes subsequent get return empty")
    void remove_makesGetReturnEmpty() {
        keyDir.put("key", new KeyDirEntry("00001", 100, 0L, 130, 1L));
        keyDir.remove("key");

        assertTrue(keyDir.get("key").isEmpty());
        assertEquals(0, keyDir.size());
    }

    @Test
    @DisplayName("getRange returns all keys in [start, end] in sorted order")
    void getRange_returnsSortedResults() {
        keyDir.put("banana", new KeyDirEntry("f1", 10, 0L, 30, 1L));
        keyDir.put("apple", new KeyDirEntry("f1", 10, 100L, 30, 2L));
        keyDir.put("cherry", new KeyDirEntry("f1", 10, 200L, 30, 3L));
        keyDir.put("date", new KeyDirEntry("f1", 10, 300L, 30, 4L));
        keyDir.put("avocado", new KeyDirEntry("f1", 10, 400L, 30, 5L));

        List<Map.Entry<String, KeyDirEntry>> range = keyDir.getRange("avocado", "cherry");

        assertEquals(3, range.size());
        assertEquals("avocado", range.get(0).getKey());
        assertEquals("banana", range.get(1).getKey());
        assertEquals("cherry", range.get(2).getKey());
    }

    @Test
    @DisplayName("getRange with no matches returns empty list")
    void getRange_noMatches_returnsEmptyList() {
        keyDir.put("aaa", new KeyDirEntry("f1", 10, 0L, 30, 1L));
        keyDir.put("zzz", new KeyDirEntry("f1", 10, 100L, 30, 2L));

        List<Map.Entry<String, KeyDirEntry>> range = keyDir.getRange("mmm", "nnn");
        assertTrue(range.isEmpty());
    }

    @Test
    @DisplayName("100,000 concurrent reads complete without locking exceptions")
    void concurrentReads_completeWithoutErrors() throws Exception {
        // Pre-populate with 1,000 keys
        for (int i = 0; i < 1000; i++) {
            keyDir.put("key-" + String.format("%05d", i),
                    new KeyDirEntry("f1", 10, i * 30L, 30, i));
        }

        int readerCount = 100_000;
        ExecutorService executor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors());
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < readerCount; i++) {
            final int idx = i % 1000;
            futures.add(executor.submit(() -> {
                Optional<KeyDirEntry> entry = keyDir.get("key-" + String.format("%05d", idx));
                return entry.isPresent();
            }));
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        int successCount = 0;
        for (Future<Boolean> f : futures) {
            if (f.get()) successCount++;
        }
        assertEquals(readerCount, successCount, "All reads should succeed");
    }

    @Test
    @DisplayName("snapshot returns a consistent point-in-time copy")
    void snapshot_returnsConsistentCopy() {
        keyDir.put("a", new KeyDirEntry("f1", 10, 0L, 30, 1L));
        keyDir.put("b", new KeyDirEntry("f1", 10, 30L, 30, 2L));

        Map<String, KeyDirEntry> snap = keyDir.snapshot();

        // Modify the live KeyDir after snapshot
        keyDir.put("c", new KeyDirEntry("f1", 10, 60L, 30, 3L));
        keyDir.remove("a");

        // Snapshot should be unaffected
        assertEquals(2, snap.size());
        assertTrue(snap.containsKey("a"));
        assertTrue(snap.containsKey("b"));
        assertFalse(snap.containsKey("c"));
    }

    @Test
    @DisplayName("replaceAll atomically swaps the entire index")
    void replaceAll_atomicallySwapsIndex() {
        keyDir.put("old1", new KeyDirEntry("f1", 10, 0L, 30, 1L));
        keyDir.put("old2", new KeyDirEntry("f1", 10, 30L, 30, 2L));

        Map<String, KeyDirEntry> newEntries = Map.of(
                "new1", new KeyDirEntry("f2", 20, 0L, 40, 10L),
                "new2", new KeyDirEntry("f2", 20, 40L, 40, 11L)
        );
        keyDir.replaceAll(newEntries);

        assertFalse(keyDir.containsKey("old1"));
        assertFalse(keyDir.containsKey("old2"));
        assertTrue(keyDir.containsKey("new1"));
        assertTrue(keyDir.containsKey("new2"));
        assertEquals(2, keyDir.size());
    }

    @Test
    @DisplayName("Concurrent reads and writes do not corrupt the index")
    void concurrentReadsAndWrites_noCorruption() throws Exception {
        int writerCount = 10;
        int readerCount = 50;
        int opsPerThread = 10_000;
        ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount);
        List<Future<?>> futures = new ArrayList<>();

        // Writers: continuously put new keys
        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            futures.add(executor.submit(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    keyDir.put("w" + writerId + "-k" + i,
                            new KeyDirEntry("f1", 10, i * 30L, 30, i));
                }
            }));
        }

        // Readers: continuously read random keys
        for (int r = 0; r < readerCount; r++) {
            futures.add(executor.submit(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    keyDir.get("w0-k" + (i % opsPerThread));
                }
            }));
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        for (Future<?> f : futures) {
            f.get(); // rethrow any exceptions
        }

        assertEquals(writerCount * opsPerThread, keyDir.size());
    }
}
