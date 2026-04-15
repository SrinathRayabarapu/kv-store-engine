package com.kvstore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Single-line log formatter with millisecond-precision ISO-style timestamps.
 *
 * <p>Replaces the default JUL {@code SimpleFormatter} which emits two lines
 * per record (header + message) and uses a locale-dependent date format that
 * is hard to correlate across Raft cluster nodes.
 *
 * <p>Output format:
 * <pre>
 *   2026-04-13 14:30:05.123 [INFO   ] [RaftNode] Node 1 became LEADER for term 3
 * </pre>
 */
public final class LogFormatter extends Formatter {

    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    @Override
    public String format(LogRecord record) {
        String ts = TIMESTAMP.format(Instant.ofEpochMilli(record.getMillis()));
        String level = String.format("%-7s", record.getLevel().getName());
        String logger = shortName(record.getLoggerName());

        StringBuilder sb = new StringBuilder(128);
        sb.append(ts).append(" [").append(level).append("] [").append(logger).append("] ")
                .append(formatMessage(record)).append('\n');

        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            record.getThrown().printStackTrace(new PrintWriter(sw));
            sb.append(sw);
        }

        return sb.toString();
    }

    private static String shortName(String loggerName) {
        if (loggerName == null) return "root";
        int dot = loggerName.lastIndexOf('.');
        return dot >= 0 ? loggerName.substring(dot + 1) : loggerName;
    }
}
