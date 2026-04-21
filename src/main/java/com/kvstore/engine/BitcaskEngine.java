package com.kvstore.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Bitcask-inspired persistent key/value storage engine.
 *
 * <p>Wires together {@link DataFile} (append-only log), {@link KeyDir}
 * (in-memory index), and {@link RecordSerializer} (binary codec) into
 * the five API operations defined by {@link StorageEngine}.
 *
 * <h3>Write path</h3>
 * <ol>
 *   <li>Acquire write lock on the active data file</li>
 *   <li>Serialize the record (key, value, timestamp) with CRC32</li>
 *   <li>Append to the active data file → get byte offset</li>
 *   <li>Update the KeyDir with the new pointer</li>
 *   <li>Release lock</li>
 * </ol>
 *
 * <h3>Read path</h3>
 * <ol>
 *   <li>Look up key in KeyDir → get (fileId, offset, size)</li>
 *   <li>Positional read from the data file (no lock needed)</li>
 *   <li>Deserialize and verify CRC32</li>
 *   <li>Return value bytes</li>
 * </ol>
 *
 * <h3>File rotation</h3>
 * <p>When the active data file exceeds {@link #maxFileSize}, a new file is
 * opened. Old files remain readable for GETs and are eventually merged
 * by the compactor.
 */
public class BitcaskEngine implements StorageEngine {

    /** Default max file size before rotation: 256 MB. */
    public static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024L;
//    public static final long DEFAULT_MAX_FILE_SIZE = 256;

    private final Path dataDir;
    private final KeyDir keyDir;
    private final long maxFileSize;
    private final AtomicLong fileIdSeq;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** The currently active (writable) data file. Protected by {@link #lock} write side. */
    private DataFile activeFile;

    /** All open data files (active + immutable), keyed by fileId. */
    private final Map<String, DataFile> dataFiles = new LinkedHashMap<>();

    /** Tracks dead bytes per file for compaction threshold decisions. */
    private final Map<String, Long> deadBytesPerFile = new HashMap<>();

    /**
     * Opens or creates a BitcaskEngine rooted at the given directory.
     *
     * <p>If existing data files are found, the engine defers to
     * {@link CrashRecovery#recover(BitcaskEngine)} to rebuild state.
     * Otherwise, a fresh active data file is created.
     *
     * @param dataDir     the directory for data files (created if absent)
     * @param maxFileSize max bytes per data file before rotation
     */
    public BitcaskEngine(Path dataDir, long maxFileSize) {
        this.dataDir = dataDir;
        this.keyDir = new KeyDir();
        this.maxFileSize = maxFileSize;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create data directory: " + dataDir, e);
        }
        this.fileIdSeq = new AtomicLong(discoverMaxFileId() + 1);

        // Only create a new file for a fresh engine — recovery sets the active file
        if (fileIdSeq.get() == 1) {
            this.activeFile = openNewDataFile();
        }
    }

    /** Convenience constructor with default max file size. */
    public BitcaskEngine(Path dataDir) {
        this(dataDir, DEFAULT_MAX_FILE_SIZE);
    }

    @Override
    public void put(String key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");

        long timestamp = System.currentTimeMillis();
        byte[] record = RecordSerializer.serialize(key, value, timestamp);
        int recordSize = record.length;

        lock.writeLock().lock();
        try {
            // Track dead bytes from the overwritten entry (if any) for compaction threshold
            keyDir.get(key).ifPresent(oldEntry ->
                    deadBytesPerFile.merge(oldEntry.fileId(), (long) oldEntry.recordSize(), Long::sum));

            rotateIfNeeded(recordSize);
            long offset = activeFile.append(record);
            keyDir.put(key, new KeyDirEntry(
                    activeFile.getFileId(), value.length, offset, recordSize, timestamp));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key must not be null");

        lock.readLock().lock();
        try {
            Optional<KeyDirEntry> entryOpt = keyDir.get(key);
            if (entryOpt.isEmpty()) {
                return Optional.empty();
            }

            KeyDirEntry entry = entryOpt.get();
            DataFile file = dataFiles.get(entry.fileId());
            if (file == null) {
                return Optional.empty();
            }

            byte[] raw = file.read(entry.valueOffset(), entry.recordSize());
            RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(raw);
            return Optional.of(record.value());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Map.Entry<String, byte[]>> readKeyRange(String startKey, String endKey) {
        Objects.requireNonNull(startKey, "startKey must not be null");
        Objects.requireNonNull(endKey, "endKey must not be null");

        lock.readLock().lock();
        try {
            List<Map.Entry<String, KeyDirEntry>> entries = keyDir.getRange(startKey, endKey);
            List<Map.Entry<String, byte[]>> result = new ArrayList<>(entries.size());

            for (Map.Entry<String, KeyDirEntry> e : entries) {
                KeyDirEntry entry = e.getValue();
                DataFile file = dataFiles.get(entry.fileId());
                if (file == null) continue;

                byte[] raw = file.read(entry.valueOffset(), entry.recordSize());
                RecordSerializer.DeserializedRecord record = RecordSerializer.deserialize(raw);
                result.add(Map.entry(e.getKey(), record.value()));
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void batchPut(List<String> keys, List<byte[]> values) {
        Objects.requireNonNull(keys, "keys must not be null");
        Objects.requireNonNull(values, "values must not be null");
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(
                    "keys and values must have the same size: " + keys.size() + " vs " + values.size());
        }

        long timestamp = System.currentTimeMillis();
        lock.writeLock().lock();
        try {
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                byte[] value = values.get(i);
                byte[] record = RecordSerializer.serialize(key, value, timestamp);
                int recordSize = record.length;

                rotateIfNeeded(recordSize);
                long offset = activeFile.append(record);
                keyDir.put(key, new KeyDirEntry(
                        activeFile.getFileId(), value.length, offset, recordSize, timestamp));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key, "key must not be null");

        long timestamp = System.currentTimeMillis();
        byte[] tombstone = RecordSerializer.serializeTombstone(key, timestamp);

        lock.writeLock().lock();
        try {
            // Track dead bytes from the old entry (if any) for compaction decisions
            keyDir.get(key).ifPresent(oldEntry ->
                    deadBytesPerFile.merge(oldEntry.fileId(), (long) oldEntry.recordSize(), Long::sum));

            rotateIfNeeded(tombstone.length);
            activeFile.append(tombstone);
            keyDir.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            for (DataFile df : dataFiles.values()) {
                df.close();
            }
            dataFiles.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns whether immutable files have enough dead data to justify compaction.
     * Thread-safe; holds a read lock while evaluating.
     */
    public boolean shouldCompact(Compactor compactor) {
        Objects.requireNonNull(compactor, "compactor");
        lock.readLock().lock();
        try {
            return compactor.shouldCompact();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Runs one compaction cycle. Thread-safe; holds the write lock for the full merge
     * so file map and {@link KeyDir} stay consistent with concurrent readers and writers.
     */
    public long compact(Compactor compactor) {
        Objects.requireNonNull(compactor, "compactor");
        lock.writeLock().lock();
        try {
            return compactor.compact();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rotates to a new active data file (syncs the current file first). Exposed for tests
     * and diagnostics — not required for normal clients.
     */
    public void rotateActiveFileForTests() {
        lock.writeLock().lock();
        try {
            activeFile.sync();
            activeFile = openNewDataFile();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- Accessors for compaction and crash recovery ---

    public KeyDir getKeyDir() {
        return keyDir;
    }

    public Map<String, DataFile> getDataFiles() {
        return Collections.unmodifiableMap(dataFiles);
    }

    public DataFile getActiveFile() {
        return activeFile;
    }

    public Path getDataDir() {
        return dataDir;
    }

    public Map<String, Long> getDeadBytesPerFile() {
        return Collections.unmodifiableMap(deadBytesPerFile);
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    /**
     * Opens a new data file and registers it as active.
     * Called during initialization and file rotation.
     */
    DataFile openNewDataFile() {
        String id = String.format("%010d", fileIdSeq.getAndIncrement());
        DataFile df = new DataFile(dataDir, id);
        dataFiles.put(id, df);
        return df;
    }

    /**
     * Registers a pre-existing data file (used during crash recovery).
     */
    void registerDataFile(DataFile df) {
        dataFiles.put(df.getFileId(), df);
    }

    void setActiveFile(DataFile df) {
        this.activeFile = df;
    }

    /**
     * Removes a data file from the engine's tracked file map.
     * Used by the compactor after successfully merging old files.
     */
    void removeDataFile(String fileId) {
        dataFiles.remove(fileId);
    }

    /**
     * Rotates to a new data file if the current one would exceed the max size.
     * Must be called while holding {@link #lock}'s write lock.
     */
    private void rotateIfNeeded(int recordSize) {
        if (activeFile.getWriteOffset() + recordSize > maxFileSize) {
            activeFile.sync();
            activeFile = openNewDataFile();
        }
    }

    /**
     * Scans the data directory for existing data files and returns the highest
     * file ID found. Returns 0 if no files exist.
     */
    private long discoverMaxFileId() {
        try {
            return Files.list(dataDir)
                    .filter(p -> p.toString().endsWith(".data"))
                    .map(p -> {
                        String name = p.getFileName().toString();
                        return name.substring(0, name.indexOf('.'));
                    })
                    .mapToLong(Long::parseLong)
                    .max()
                    .orElse(0);
        } catch (IOException e) {
            return 0;
        }
    }

    /**
     * Tracks dead bytes when a key is overwritten. Called by put() to update
     * the dead-bytes map for compaction threshold calculation.
     */
    void trackOverwrite(String key) {
        keyDir.get(key).ifPresent(oldEntry ->
                deadBytesPerFile.merge(oldEntry.fileId(), (long) oldEntry.recordSize(), Long::sum));
    }
}
