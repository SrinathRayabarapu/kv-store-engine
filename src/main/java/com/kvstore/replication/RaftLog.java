package com.kvstore.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The replicated log — an ordered sequence of {@link LogEntry} items.
 *
 * <p>Raft uses 1-based indexing. Index 0 is a sentinel (the "empty" state
 * before any entries are appended). The log is append-only from the leader's
 * perspective; followers may truncate conflicting suffixes during replication.
 *
 * <p>Thread safety: all methods are synchronized. The Raft state machine
 * accesses the log from both the RPC handler thread and the apply-committed
 * loop, so serialization is necessary.
 */
public class RaftLog {

    /** Sentinel entry at index 0 — simplifies boundary checks. */
    private final List<LogEntry> entries = new ArrayList<>();

    public RaftLog() {
        // Index 0 sentinel: term 0, no-op
        entries.add(new LogEntry(0, 0, (byte) 0, "", new byte[0]));
    }

    public synchronized void append(LogEntry entry) {
        entries.add(entry);
    }

    public synchronized LogEntry get(long index) {
        if (index < 0 || index >= entries.size()) {
            return null;
        }
        return entries.get((int) index);
    }

    /** Returns the index of the last entry. */
    public synchronized long lastIndex() {
        return entries.size() - 1;
    }

    /** Returns the term of the last entry. */
    public synchronized long lastTerm() {
        return entries.get(entries.size() - 1).term();
    }

    /** Returns the term of the entry at the given index, or 0 if out of bounds. */
    public synchronized long termAt(long index) {
        if (index < 0 || index >= entries.size()) return 0;
        return entries.get((int) index).term();
    }

    /**
     * Returns entries from startIndex (inclusive) to the end of the log.
     * Used by the leader to build AppendEntries RPC payloads.
     */
    public synchronized List<LogEntry> getFrom(long startIndex) {
        if (startIndex >= entries.size()) return Collections.emptyList();
        return new ArrayList<>(entries.subList((int) startIndex, entries.size()));
    }

    /**
     * Truncates the log from the given index onwards (inclusive) and appends
     * the new entries. Used by followers to resolve log conflicts.
     *
     * <p>This is the core of Raft's log-matching invariant: if a follower's
     * log diverges from the leader's at a given index, all entries from that
     * point are replaced by the leader's entries.
     */
    public synchronized void truncateAndAppend(long fromIndex, List<LogEntry> newEntries) {
        while (entries.size() > fromIndex) {
            entries.remove(entries.size() - 1);
        }
        entries.addAll(newEntries);
    }

    public synchronized int size() {
        return entries.size();
    }
}
