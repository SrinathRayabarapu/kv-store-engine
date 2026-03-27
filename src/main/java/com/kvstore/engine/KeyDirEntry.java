package com.kvstore.engine;

/**
 * A pointer from the in-memory KeyDir to the exact location of a value on disk.
 *
 * <p>This is the core of Bitcask's O(1) read path: given a key, the KeyDir
 * returns one of these entries, and we perform exactly one disk seek to
 * {@code fileId} at {@code valueOffset} to read {@code valueSize} bytes.
 *
 * <p>The timestamp is used for conflict resolution during compaction and
 * crash recovery — when two records exist for the same key, the one with
 * the higher timestamp wins (latest-write-wins semantics).
 *
 * @param fileId      identifies which data file contains this value
 * @param valueSize   size of the value in bytes (excludes header and key)
 * @param valueOffset byte offset within the data file where the full record starts
 * @param recordSize  total size of the on-disk record (header + key + value)
 * @param timestamp   epoch millis — used for latest-write-wins resolution
 */
public record KeyDirEntry(
        String fileId,
        int valueSize,
        long valueOffset,
        int recordSize,
        long timestamp
) {
}
