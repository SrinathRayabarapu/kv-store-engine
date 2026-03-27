package com.kvstore.engine;

/**
 * Thrown when a record's CRC32 checksum does not match its payload.
 *
 * <p>This typically indicates either data corruption on disk or a torn write
 * caused by a crash mid-append. The caller (usually the recovery loop) should
 * handle this by discarding the corrupted record and treating it as the
 * logical end of the data file.
 */
public class CorruptedRecordException extends RuntimeException {

    public CorruptedRecordException(String message) {
        super(message);
    }

    public CorruptedRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
