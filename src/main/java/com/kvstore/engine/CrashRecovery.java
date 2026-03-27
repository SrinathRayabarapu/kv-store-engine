package com.kvstore.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Rebuilds the engine's in-memory state after a restart.
 *
 * <h3>Two recovery paths:</h3>
 * <ol>
 *   <li><b>Hint file recovery</b> (clean shutdown): read compact hint files
 *       to rebuild the KeyDir in O(keys) time without touching data files.</li>
 *   <li><b>Full replay recovery</b> (crash / no hint files): scan all data
 *       files chronologically, deserialize every record, and reconstruct the
 *       KeyDir. A CRC32 failure on the last record of the newest file is
 *       treated as a torn write and safely discarded.</li>
 * </ol>
 *
 * <p>The hint file path is preferred whenever hint files exist. Full replay
 * is the fallback for crash scenarios where hint files were never written.
 */
public final class CrashRecovery {

    private static final Logger LOG = Logger.getLogger(CrashRecovery.class.getName());

    private CrashRecovery() {
    }

    /**
     * Recovers the engine state from disk. Opens all data files, rebuilds
     * the KeyDir, and sets the active file to the newest one.
     *
     * @param engine the engine to recover (must have dataDir set, KeyDir empty)
     * @return true if recovery used hint files, false if full replay was needed
     */
    public static boolean recover(BitcaskEngine engine) {
        Path dataDir = engine.getDataDir();
        List<String> fileIds = discoverDataFileIds(dataDir);

        if (fileIds.isEmpty()) {
            return false;
        }

        // Open all existing data files and register them with the engine
        for (String fileId : fileIds) {
            if (!engine.getDataFiles().containsKey(fileId)) {
                DataFile df = new DataFile(dataDir, fileId);
                engine.registerDataFile(df);
            }
        }

        // Set the newest existing file as active for reads during recovery.
        // After rebuilding, open a fresh file for new writes to avoid appending
        // to a file that may have a torn tail.
        String newestFileId = fileIds.get(fileIds.size() - 1);
        engine.setActiveFile(engine.getDataFiles().get(newestFileId));

        // Try hint file recovery first
        boolean hintFilesExist = fileIds.stream()
                .allMatch(id -> Files.exists(HintFile.hintPathFor(dataDir, id)));

        boolean usedHints;
        if (hintFilesExist) {
            recoverFromHintFiles(engine, fileIds);
            LOG.info("Recovery complete via hint files — " + engine.getKeyDir().size() + " keys loaded");
            usedHints = true;
        } else {
            recoverFromDataFiles(engine, fileIds);
            LOG.info("Recovery complete via full replay — " + engine.getKeyDir().size() + " keys loaded");
            usedHints = false;
        }

        // Open a fresh active file for new writes — avoids appending to a file
        // that may have a torn tail from a crash
        DataFile freshActive = engine.openNewDataFile();
        engine.setActiveFile(freshActive);

        return usedHints;
    }

    /**
     * Fast recovery path: reads hint files to rebuild the KeyDir without
     * touching the data files.
     */
    private static void recoverFromHintFiles(BitcaskEngine engine, List<String> fileIds) {
        KeyDir keyDir = engine.getKeyDir();

        for (String fileId : fileIds) {
            Path hintPath = HintFile.hintPathFor(engine.getDataDir(), fileId);
            List<HintFile.HintEntry> entries = HintFile.read(hintPath);

            for (HintFile.HintEntry hint : entries) {
                int recordSize = RecordSerializer.HEADER_SIZE
                        + hint.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + Math.max(hint.valueSize(), 0);

                KeyDirEntry existing = keyDir.get(hint.key()).orElse(null);
                if (existing == null || hint.timestamp() >= existing.timestamp()) {
                    keyDir.put(hint.key(), new KeyDirEntry(
                            fileId, hint.valueSize(), hint.valueOffset(), recordSize, hint.timestamp()));
                }
            }
        }
    }

    /**
     * Full replay recovery: scans all data files in chronological order,
     * deserializes every record, and rebuilds the KeyDir.
     *
     * <p>A CRC32 failure on the last record of the last file is treated as
     * a torn write (crash mid-append) and safely discarded. CRC failures
     * elsewhere indicate real corruption and are logged as warnings.
     */
    private static void recoverFromDataFiles(BitcaskEngine engine, List<String> fileIds) {
        KeyDir keyDir = engine.getKeyDir();

        for (int fileIndex = 0; fileIndex < fileIds.size(); fileIndex++) {
            String fileId = fileIds.get(fileIndex);
            DataFile dataFile = engine.getDataFiles().get(fileId);
            boolean isLastFile = (fileIndex == fileIds.size() - 1);

            byte[] fileData = dataFile.readAll();
            ByteBuffer buffer = ByteBuffer.wrap(fileData);

            while (buffer.hasRemaining()) {
                int recordStart = buffer.position();

                int recordSize = RecordSerializer.peekRecordSize(buffer);
                if (recordSize < 0 || buffer.remaining() < recordSize) {
                    if (isLastFile) {
                        LOG.info("Discarding " + buffer.remaining()
                                + " trailing bytes in last file (probable torn write)");
                    } else {
                        LOG.warning("Truncated record in file " + fileId + " at offset " + recordStart);
                    }
                    break;
                }

                byte[] recordBytes = new byte[recordSize];
                buffer.get(recordBytes);

                try {
                    RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(recordBytes);

                    if (record.tombstone()) {
                        keyDir.remove(record.key());
                    } else {
                        KeyDirEntry existing = keyDir.get(record.key()).orElse(null);
                        if (existing == null || record.timestamp() >= existing.timestamp()) {
                            keyDir.put(record.key(), new KeyDirEntry(
                                    fileId,
                                    record.value().length,
                                    recordStart,
                                    recordSize,
                                    record.timestamp()));
                        }
                    }
                } catch (CorruptedRecordException e) {
                    if (isLastFile && !buffer.hasRemaining()) {
                        LOG.info("CRC mismatch on last record of last file — torn write discarded");
                    } else {
                        LOG.log(Level.WARNING,
                                "Corrupted record in file " + fileId + " at offset " + recordStart, e);
                    }
                    break;
                }
            }
        }
    }

    /**
     * Writes hint files for all data files based on the current KeyDir state.
     * Called during clean shutdown.
     */
    public static void writeHintFiles(BitcaskEngine engine) {
        Map<String, KeyDirEntry> snapshot = engine.getKeyDir().snapshot();

        // Group entries by fileId
        Map<String, List<HintFile.HintEntry>> byFile = new HashMap<>();
        for (Map.Entry<String, KeyDirEntry> e : snapshot.entrySet()) {
            String key = e.getKey();
            KeyDirEntry kd = e.getValue();
            byFile.computeIfAbsent(kd.fileId(), k -> new ArrayList<>())
                    .add(new HintFile.HintEntry(key, kd.timestamp(), kd.valueSize(), kd.valueOffset()));
        }

        for (Map.Entry<String, List<HintFile.HintEntry>> e : byFile.entrySet()) {
            Path hintPath = HintFile.hintPathFor(engine.getDataDir(), e.getKey());
            HintFile.write(hintPath, e.getValue());
        }

        LOG.info("Hint files written for " + byFile.size() + " data file(s)");
    }

    /**
     * Discovers data file IDs in the data directory, sorted numerically.
     */
    static List<String> discoverDataFileIds(Path dataDir) {
        try (Stream<Path> files = Files.list(dataDir)) {
            return files
                    .filter(p -> p.toString().endsWith(".data"))
                    .map(p -> {
                        String name = p.getFileName().toString();
                        return name.substring(0, name.indexOf('.'));
                    })
                    .sorted()
                    .toList();
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }
}
