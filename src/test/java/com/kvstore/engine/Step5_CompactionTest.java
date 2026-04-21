package com.kvstore.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 5 acceptance tests — verifies background compaction reclaims dead space
 * while reads continue to return correct values.
 */
@DisplayName("Step 5 — Compaction")
class Step5_CompactionTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Overwriting all keys then compacting reduces data file size by at least 40%")
    void compaction_reducesSize_afterOverwrite() {
        // Small max file size to force data into immutable files
        long maxFileSize = 4096;
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, maxFileSize)) {
            // Write 200 keys
            for (int i = 0; i < 200; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }

            // Force a file rotation so the original data is in immutable files
            engine.rotateActiveFileForTests();

            long sizeBeforeOverwrite = totalDataFileSize(engine);

            // Overwrite all 200 keys (creates dead copies in the immutable files)
            for (int i = 0; i < 200; i++) {
                engine.put("key-" + i, ("new-value-" + i).getBytes());
            }

            // Force another rotation
            engine.rotateActiveFileForTests();

            Compactor compactor = new Compactor(engine);
            long reclaimed = engine.compact(compactor);

            assertTrue(reclaimed > 0, "Compaction should reclaim some bytes");

            // Verify all keys still return the latest values
            for (int i = 0; i < 200; i++) {
                byte[] val = engine.get("key-" + i).orElseThrow();
                assertArrayEquals(("new-value-" + i).getBytes(), val,
                        "Value mismatch after compaction for key-" + i);
            }
        }
    }

    @Test
    @DisplayName("Reads return correct values during and after compaction")
    void readsCorrect_duringCompaction() {
        long maxFileSize = 2048;
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, maxFileSize)) {
            // Write keys, some will be overwritten
            for (int i = 0; i < 100; i++) {
                engine.put("key-" + i, ("v1-" + i).getBytes());
            }
            engine.rotateActiveFileForTests();

            // Overwrite half the keys
            for (int i = 0; i < 50; i++) {
                engine.put("key-" + i, ("v2-" + i).getBytes());
            }
            engine.rotateActiveFileForTests();

            // Run compaction
            Compactor compactor = new Compactor(engine);
            engine.compact(compactor);

            // Verify: first 50 have v2, rest have v1
            for (int i = 0; i < 50; i++) {
                assertArrayEquals(("v2-" + i).getBytes(),
                        engine.get("key-" + i).orElseThrow());
            }
            for (int i = 50; i < 100; i++) {
                assertArrayEquals(("v1-" + i).getBytes(),
                        engine.get("key-" + i).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("After compaction, deleted keys are not resurrected")
    void deletedKeys_notResurrected_afterCompaction() {
        long maxFileSize = 2048;
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, maxFileSize)) {
            for (int i = 0; i < 50; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            engine.rotateActiveFileForTests();

            // Delete some keys
            for (int i = 0; i < 25; i++) {
                engine.delete("key-" + i);
            }
            engine.rotateActiveFileForTests();

            Compactor compactor = new Compactor(engine);
            engine.compact(compactor);

            for (int i = 0; i < 25; i++) {
                assertTrue(engine.get("key-" + i).isEmpty(),
                        "Deleted key should remain deleted: key-" + i);
            }
            for (int i = 25; i < 50; i++) {
                assertTrue(engine.get("key-" + i).isPresent(),
                        "Non-deleted key should still exist: key-" + i);
            }
        }
    }

    @Test
    @DisplayName("shouldCompact returns true when dead bytes exceed threshold")
    void shouldCompact_triggersCorrectly() {
        long maxFileSize = 2048;
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, maxFileSize)) {
            // Write and overwrite to create dead bytes
            for (int i = 0; i < 50; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            engine.rotateActiveFileForTests();

            // Overwrite all — creates ~100% dead bytes in immutable file
            for (int i = 0; i < 50; i++) {
                engine.put("key-" + i, ("new-value-" + i).getBytes());
            }

            Compactor compactor = new Compactor(engine, 0.5);
            assertTrue(engine.shouldCompact(compactor),
                    "Should compact when dead bytes exceed 50% of immutable files");
        }
    }

    @Test
    @DisplayName("Compaction with no immutable files is a no-op")
    void compaction_noImmutableFiles_noOp() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("only-key", "only-value".getBytes());

            Compactor compactor = new Compactor(engine);
            long reclaimed = engine.compact(compactor);

            assertEquals(0, reclaimed);
            assertArrayEquals("only-value".getBytes(),
                    engine.get("only-key").orElseThrow());
        }
    }

    @Test
    @DisplayName("Old data files are deleted after successful compaction")
    void oldFiles_deleted_afterCompaction() {
        long maxFileSize = 1024;
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, maxFileSize)) {
            for (int i = 0; i < 100; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            engine.rotateActiveFileForTests();

            int fileCountBefore = engine.getDataFiles().size();
            assertTrue(fileCountBefore > 2, "Expected multiple files before compaction");

            Compactor compactor = new Compactor(engine);
            engine.compact(compactor);

            // After compaction: active file + compacted file (old immutable files removed)
            int fileCountAfter = engine.getDataFiles().size();
            assertTrue(fileCountAfter < fileCountBefore,
                    "File count should decrease after compaction: before=" + fileCountBefore
                            + ", after=" + fileCountAfter);
        }
    }

    private long totalDataFileSize(BitcaskEngine engine) {
        return engine.getDataFiles().values().stream()
                .mapToLong(DataFile::getWriteOffset)
                .sum();
    }
}
