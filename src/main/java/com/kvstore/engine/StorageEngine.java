package com.kvstore.engine;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The five core operations of a persistent key/value storage engine.
 *
 * <p>Every operation targets byte-array values keyed by UTF-8 strings.
 * Implementations must be safe for concurrent access from multiple threads.
 *
 * <p>Design note: we deliberately return {@code Optional<byte[]>} rather than
 * throwing on missing keys — callers should never need to catch an exception
 * on the hot read path.
 */
public interface StorageEngine extends Closeable {

    /**
     * Stores a key/value pair. If the key already exists, the previous value
     * is logically overwritten (latest-write-wins). The old value remains on
     * disk until reclaimed by compaction.
     *
     * @param key   non-null, non-empty UTF-8 key
     * @param value non-null byte array (may be zero-length)
     */
    void put(String key, byte[] value);

    /**
     * Retrieves the value associated with a key.
     *
     * @param key non-null, non-empty UTF-8 key
     * @return the value if the key exists and has not been deleted,
     *         {@link Optional#empty()} otherwise
     */
    Optional<byte[]> get(String key);

    /**
     * Returns all key/value pairs whose keys fall in the inclusive range
     * {@code [startKey, endKey]}, ordered lexicographically by key.
     *
     * <p>Complexity note: Bitcask has no sorted on-disk structure, so this
     * operation snapshots the full key set (O(n)), filters and sorts (O(k log k)),
     * then fetches each value from disk (O(k) seeks). This is the known tradeoff
     * we accept for O(1) point reads.
     *
     * @param startKey inclusive lower bound
     * @param endKey   inclusive upper bound
     * @return sorted list of matching entries; empty list if none match
     */
    List<Map.Entry<String, byte[]>> readKeyRange(String startKey, String endKey);

    /**
     * Atomically writes multiple key/value pairs. Either all keys are written
     * or none are — a crash mid-batch leaves the engine in a consistent state
     * because partially-written records fail CRC validation on recovery.
     *
     * @param keys   non-null list of keys (same length as values)
     * @param values non-null list of values (same length as keys)
     * @throws IllegalArgumentException if keys and values have different sizes
     */
    void batchPut(List<String> keys, List<byte[]> values);

    /**
     * Deletes a key by appending a tombstone record. The key is immediately
     * removed from the in-memory index. The tombstone occupies disk space
     * until reclaimed by compaction.
     *
     * @param key non-null, non-empty UTF-8 key
     */
    void delete(String key);
}
