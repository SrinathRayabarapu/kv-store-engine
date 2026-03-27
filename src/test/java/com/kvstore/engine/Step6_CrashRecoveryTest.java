package com.kvstore.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 6 acceptance tests — verifies that the engine survives ungraceful
 * shutdown and recovers correctly from both hint files and full data replay.
 */
@DisplayName("Step 6 — Crash Recovery")
class Step6_CrashRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("After clean shutdown (hint files written), engine rebuilds KeyDir from hint files only")
    void cleanShutdown_recoversFromHintFiles() {
        // Phase 1: write data and shut down cleanly
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            for (int i = 0; i < 100; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            CrashRecovery.writeHintFiles(engine);
        }

        // Phase 2: reopen — should use hint files
        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            boolean usedHints = CrashRecovery.recover(engine2);
            assertTrue(usedHints, "Should recover from hint files");

            for (int i = 0; i < 100; i++) {
                Optional<byte[]> val = engine2.get("key-" + i);
                assertTrue(val.isPresent(), "Missing key after recovery: key-" + i);
                assertArrayEquals(("value-" + i).getBytes(), val.get());
            }
        }
    }

    @Test
    @DisplayName("After crash (no hint files), engine rebuilds KeyDir by replaying data files")
    void crash_recoversFromFullReplay() {
        // Phase 1: write data, do NOT write hint files (simulates crash)
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            for (int i = 0; i < 100; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            // No hint files written — simulates kill -9
        }

        // Phase 2: reopen — should use full replay
        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            boolean usedHints = CrashRecovery.recover(engine2);
            assertFalse(usedHints, "Should use full replay when no hint files");

            for (int i = 0; i < 100; i++) {
                Optional<byte[]> val = engine2.get("key-" + i);
                assertTrue(val.isPresent(), "Missing key after replay: key-" + i);
                assertArrayEquals(("value-" + i).getBytes(), val.get());
            }
        }
    }

    @Test
    @DisplayName("Overwritten keys resolve to latest value after recovery")
    void recovery_latestWriteWins() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("key", "v1".getBytes());
            engine.put("key", "v2".getBytes());
            engine.put("key", "v3".getBytes());
        }

        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            CrashRecovery.recover(engine2);

            assertArrayEquals("v3".getBytes(), engine2.get("key").orElseThrow(),
                    "Latest write should win after recovery");
        }
    }

    @Test
    @DisplayName("Deleted keys remain deleted after recovery")
    void recovery_deletedKeysRemainDeleted() {
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("alive", "yes".getBytes());
            engine.put("dead", "no".getBytes());
            engine.delete("dead");
        }

        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            CrashRecovery.recover(engine2);

            assertTrue(engine2.get("alive").isPresent());
            assertTrue(engine2.get("dead").isEmpty(), "Deleted key should remain deleted");
        }
    }

    @Test
    @DisplayName("Torn write (partial last record) is discarded gracefully")
    void tornWrite_lastRecordDiscarded() throws IOException {
        // Phase 1: write valid data
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("good-key", "good-value".getBytes());
        }

        // Phase 2: append garbage bytes to the data file (simulate torn write)
        Path dataFile = Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".data"))
                .findFirst()
                .orElseThrow();

        try (RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
            raf.seek(raf.length());
            // Write a partial header (less than a full record) — simulates crash mid-write
            raf.write(new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06});
        }

        // Phase 3: recover — should load the good record and discard the torn bytes
        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            CrashRecovery.recover(engine2);

            assertTrue(engine2.get("good-key").isPresent(),
                    "Valid record should survive torn write recovery");
            assertArrayEquals("good-value".getBytes(), engine2.get("good-key").orElseThrow());
        }
    }

    @Test
    @DisplayName("CRC mismatch on last record of last file is handled gracefully")
    void crcMismatch_lastRecord_discarded() throws IOException {
        // Phase 1: write data
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            engine.put("first", "1".getBytes());
            engine.put("second", "2".getBytes());
        }

        // Phase 2: corrupt the last few bytes of the data file
        Path dataFile = Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".data"))
                .findFirst()
                .orElseThrow();

        try (RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
            long len = raf.length();
            raf.seek(len - 1);
            byte b = raf.readByte();
            raf.seek(len - 1);
            raf.writeByte(b ^ 0xFF); // flip all bits of the last byte
        }

        // Phase 3: recover — should load the first record, discard the corrupted second
        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            CrashRecovery.recover(engine2);

            assertTrue(engine2.get("first").isPresent(), "First record should survive");
            // Second record may or may not survive depending on where the corruption is
            // The key point is that the engine starts without throwing
        }
    }

    @Test
    @DisplayName("Hint file recovery is faster than full replay (correctness check)")
    void hintFileRecovery_producesCorrectResults() {
        // Write a decent number of records
        try (BitcaskEngine engine = new BitcaskEngine(tempDir)) {
            for (int i = 0; i < 1000; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            // Overwrite some
            for (int i = 0; i < 500; i++) {
                engine.put("key-" + i, ("updated-" + i).getBytes());
            }
            // Delete some
            for (int i = 900; i < 1000; i++) {
                engine.delete("key-" + i);
            }
            CrashRecovery.writeHintFiles(engine);
        }

        // Recover via hint files
        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir)) {
            boolean usedHints = CrashRecovery.recover(engine2);
            assertTrue(usedHints);

            // Verify overwritten keys have updated values
            for (int i = 0; i < 500; i++) {
                assertArrayEquals(("updated-" + i).getBytes(),
                        engine2.get("key-" + i).orElseThrow());
            }
            // Verify non-overwritten keys
            for (int i = 500; i < 900; i++) {
                assertArrayEquals(("value-" + i).getBytes(),
                        engine2.get("key-" + i).orElseThrow());
            }
            // Verify deleted keys
            for (int i = 900; i < 1000; i++) {
                assertTrue(engine2.get("key-" + i).isEmpty());
            }
        }
    }

    @Test
    @DisplayName("Recovery across multiple data files (file rotation)")
    void recovery_acrossMultipleFiles() {
        long smallMax = 2048; // Force rotation
        try (BitcaskEngine engine = new BitcaskEngine(tempDir, smallMax)) {
            for (int i = 0; i < 200; i++) {
                engine.put("key-" + i, ("value-" + i).getBytes());
            }
            assertTrue(engine.getDataFiles().size() > 1, "Expected multiple files");
        }

        try (BitcaskEngine engine2 = new BitcaskEngine(tempDir, smallMax)) {
            CrashRecovery.recover(engine2);

            for (int i = 0; i < 200; i++) {
                assertTrue(engine2.get("key-" + i).isPresent(),
                        "Missing after multi-file recovery: key-" + i);
            }
        }
    }
}
