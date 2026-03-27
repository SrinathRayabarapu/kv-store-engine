package com.kvstore.engine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The in-memory index that maps every live key to its on-disk location.
 *
 * <p>This is the beating heart of the Bitcask engine. Every {@code GET} resolves
 * through this map in O(1) — no disk I/O required to find where the value lives.
 *
 * <h3>Concurrency model</h3>
 * <ul>
 *   <li>{@link #put} and {@link #get} use {@link ConcurrentHashMap} — lock-free
 *       reads, segment-level locking on writes. No external synchronization needed.</li>
 *   <li>{@link #getRange} takes a snapshot of the key set under a
 *       {@link ReadWriteLock} read-lock, then sorts and filters outside the lock.
 *       This is the only O(n) operation and is the explicit tradeoff of the
 *       Bitcask model for range queries.</li>
 * </ul>
 *
 * <h3>Why ConcurrentHashMap over TreeMap + locks?</h3>
 * <p>Point reads are the hot path. {@code ConcurrentHashMap.get()} completes in
 * ~50ns without any lock acquisition. A {@code TreeMap} with a {@code ReadWriteLock}
 * adds ~200ns per read due to lock overhead. At 100K+ ops/sec, this matters.
 * We accept the O(n log n) cost on range queries because they are infrequent.
 */
public class KeyDir {

    private final ConcurrentHashMap<String, KeyDirEntry> map = new ConcurrentHashMap<>();
    private final ReadWriteLock snapshotLock = new ReentrantReadWriteLock();

    /**
     * Updates the index entry for a key. If the key already exists, the entry
     * is replaced — the old value's disk space becomes reclaimable by compaction.
     *
     * @param key   the key
     * @param entry the new disk pointer for this key
     */
    public void put(String key, KeyDirEntry entry) {
        snapshotLock.readLock().lock();
        try {
            map.put(key, entry);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    /**
     * Retrieves the disk pointer for a key.
     *
     * @param key the key to look up
     * @return the entry if the key exists, empty otherwise
     */
    public Optional<KeyDirEntry> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    /**
     * Removes a key from the index. Called when processing a tombstone during
     * recovery or when the engine's {@code delete()} is invoked.
     *
     * @param key the key to remove
     */
    public void remove(String key) {
        snapshotLock.readLock().lock();
        try {
            map.remove(key);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }

    /**
     * Returns all key/entry pairs whose keys fall in the inclusive range
     * {@code [startKey, endKey]}, sorted lexicographically by key.
     *
     * <p>Implementation: takes the write-lock to get a consistent snapshot of
     * the key set, then releases the lock before sorting. The sort happens
     * outside the lock so it never blocks concurrent writes.
     *
     * @param startKey inclusive lower bound
     * @param endKey   inclusive upper bound
     * @return sorted list of matching entries
     */
    public List<Map.Entry<String, KeyDirEntry>> getRange(String startKey, String endKey) {
        Map<String, KeyDirEntry> snapshot;
        snapshotLock.writeLock().lock();
        try {
            snapshot = new HashMap<>(map);
        } finally {
            snapshotLock.writeLock().unlock();
        }

        TreeMap<String, KeyDirEntry> sorted = new TreeMap<>(snapshot);
        return new ArrayList<>(sorted.subMap(startKey, true, endKey, true).entrySet());
    }

    /**
     * Returns an unmodifiable view of all entries. Used during compaction to
     * determine which records are live vs. dead.
     */
    public Map<String, KeyDirEntry> snapshot() {
        snapshotLock.writeLock().lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(map));
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    /**
     * Replaces the entire index atomically. Used after compaction completes
     * to swap in the new pointers.
     *
     * @param newEntries the compacted index
     */
    public void replaceAll(Map<String, KeyDirEntry> newEntries) {
        snapshotLock.writeLock().lock();
        try {
            map.clear();
            map.putAll(newEntries);
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }

    public int size() {
        return map.size();
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public Set<String> keys() {
        return Collections.unmodifiableSet(map.keySet());
    }
}
