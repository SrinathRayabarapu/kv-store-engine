package com.kvstore.replication;

import java.io.Serializable;

/**
 * A single entry in the Raft replicated log.
 *
 * <p>Each entry records a state-machine command (PUT or DELETE) along with
 * the Raft term and log index. Once committed (acknowledged by a majority),
 * the command is applied to the local {@link com.kvstore.engine.StorageEngine}.
 *
 * @param term    the Raft term when this entry was created by the leader
 * @param index   1-based position in the log (Raft uses 1-based indexing)
 * @param opCode  operation type: {@link #OP_PUT} or {@link #OP_DELETE}
 * @param key     the key affected
 * @param value   the value (empty for DELETE)
 */
public record LogEntry(long term, long index, byte opCode, String key, byte[] value) {

    public static final byte OP_PUT = 1;
    public static final byte OP_DELETE = 2;

    public static LogEntry put(long term, long index, String key, byte[] value) {
        return new LogEntry(term, index, OP_PUT, key, value);
    }

    public static LogEntry delete(long term, long index, String key) {
        return new LogEntry(term, index, OP_DELETE, key, new byte[0]);
    }
}
