package com.kvstore.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Background compaction for the Bitcask engine — merges old data files to
 * reclaim space occupied by overwritten and deleted records.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>Identify immutable data files (all except the active file).</li>
 *   <li>For each live key that points to an immutable file, copy the record
 *       to a new compacted data file.</li>
 *   <li>Build a new KeyDir mapping from the compacted file.</li>
 *   <li>Atomically swap: update the engine's KeyDir and file map under the
 *       write lock, then delete the old files.</li>
 * </ol>
 *
 * <h3>Safety invariant</h3>
 * <p>Old files are NEVER deleted until the new compacted file is fully written
 * and fsynced. This is the "write new → fsync → swap pointers → delete old"
 * pattern — the only safe way to do background I/O in a crash-friendly engine.
 *
 * <h3>Trigger threshold</h3>
 * <p>Compaction is triggered when dead bytes exceed the configured ratio of
 * total immutable file size (default 50%). This balances write amplification
 * (compacting too eagerly wastes I/O) against space amplification (compacting
 * too rarely wastes disk).
 */
public class Compactor {

    private static final Logger LOG = Logger.getLogger(Compactor.class.getName());

    private final BitcaskEngine engine;
    private final double deadBytesThreshold;

    /**
     * @param engine             the engine to compact
     * @param deadBytesThreshold ratio of dead bytes to total immutable bytes that triggers compaction (0.0–1.0)
     */
    public Compactor(BitcaskEngine engine, double deadBytesThreshold) {
        this.engine = engine;
        this.deadBytesThreshold = deadBytesThreshold;
    }

    /** Default threshold: 50% dead bytes. */
    public Compactor(BitcaskEngine engine) {
        this(engine, 0.5);
    }

    /**
     * Checks whether compaction should run based on the dead-bytes ratio
     * of immutable files.
     */
    public boolean shouldCompact() {
        String activeFileId = engine.getActiveFile().getFileId();
        long totalImmutableBytes = 0;
        long totalDeadBytes = 0;

        for (Map.Entry<String, DataFile> e : engine.getDataFiles().entrySet()) {
            if (e.getKey().equals(activeFileId)) continue;
            totalImmutableBytes += e.getValue().getWriteOffset();
        }
        for (Map.Entry<String, Long> e : engine.getDeadBytesPerFile().entrySet()) {
            if (e.getKey().equals(activeFileId)) continue;
            totalDeadBytes += e.getValue();
        }

        if (totalImmutableBytes == 0) return false;
        return (double) totalDeadBytes / totalImmutableBytes >= deadBytesThreshold;
    }

    /**
     * Runs one compaction cycle. Returns the number of bytes reclaimed.
     *
     * <p>This method is NOT thread-safe with respect to the engine — the
     * caller should ensure no structural changes (like close) happen concurrently.
     * The engine's write lock is acquired only during the atomic swap phase.
     */
    public long compact() {
        String activeFileId = engine.getActiveFile().getFileId();

        // Identify immutable files to compact
        List<String> filesToCompact = new ArrayList<>();
        for (String fileId : engine.getDataFiles().keySet()) {
            if (!fileId.equals(activeFileId)) {
                filesToCompact.add(fileId);
            }
        }

        if (filesToCompact.isEmpty()) {
            return 0;
        }

        LOG.info("Compaction starting — merging " + filesToCompact.size() + " immutable file(s)");

        // Phase 1: Write live records to a new compacted file
        DataFile compactedFile = engine.openNewDataFile();
        Map<String, KeyDirEntry> newEntries = new HashMap<>();
        Map<String, KeyDirEntry> currentSnapshot = engine.getKeyDir().snapshot();

        long bytesWritten = 0;
        for (Map.Entry<String, KeyDirEntry> entry : currentSnapshot.entrySet()) {
            String key = entry.getKey();
            KeyDirEntry kd = entry.getValue();

            // Only copy records from immutable files
            if (!filesToCompact.contains(kd.fileId())) {
                // Keep the existing entry as-is (it's in the active file or compacted file)
                newEntries.put(key, kd);
                continue;
            }

            // Read the record from the old file
            DataFile oldFile = engine.getDataFiles().get(kd.fileId());
            if (oldFile == null) continue;

            byte[] raw = oldFile.read(kd.valueOffset(), kd.recordSize());

            // Write to compacted file
            long newOffset = compactedFile.append(raw);
            bytesWritten += raw.length;

            newEntries.put(key, new KeyDirEntry(
                    compactedFile.getFileId(),
                    kd.valueSize(),
                    newOffset,
                    kd.recordSize(),
                    kd.timestamp()));
        }

        // Phase 2: fsync the compacted file
        compactedFile.sync();

        // Phase 3: Atomic swap — update KeyDir and file references
        // We need to also include any keys written to the active file during compaction
        Map<String, KeyDirEntry> finalSnapshot = engine.getKeyDir().snapshot();
        for (Map.Entry<String, KeyDirEntry> entry : finalSnapshot.entrySet()) {
            String key = entry.getKey();
            KeyDirEntry kd = entry.getValue();
            // If this key was written to the active file (or already compacted),
            // use the latest entry, not the compacted one
            if (kd.fileId().equals(activeFileId)) {
                newEntries.put(key, kd);
            }
        }

        // Remove keys that were deleted during compaction
        Set<String> currentKeys = finalSnapshot.keySet();
        newEntries.keySet().retainAll(currentKeys);

        engine.getKeyDir().replaceAll(newEntries);

        // Phase 4: Delete old files
        long bytesReclaimed = 0;
        for (String fileId : filesToCompact) {
            DataFile oldFile = engine.getDataFiles().get(fileId);
            if (oldFile == null) continue;

            bytesReclaimed += oldFile.getWriteOffset();
            oldFile.close();

            try {
                Files.deleteIfExists(oldFile.getPath());
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Failed to delete old data file: " + oldFile.getPath(), e);
            }
        }

        // Remove old files from the engine's tracked file map
        filesToCompact.forEach(engine::removeDataFile);

        LOG.info("Compaction complete — wrote " + bytesWritten + " live bytes, reclaimed " + bytesReclaimed + " bytes");
        return bytesReclaimed;
    }
}
