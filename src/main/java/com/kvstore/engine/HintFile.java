package com.kvstore.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Compact index persistence for fast crash recovery.
 *
 * <p>On a clean shutdown, we write a hint file alongside each data file.
 * The hint file contains only the KeyDir entries — no values. On restart,
 * instead of replaying GBs of data files to rebuild the index, we read
 * only the compact hint files.
 *
 * <h3>Format (binary, big-endian):</h3>
 * <pre>
 * ┌──────────┬──────────┬──────────┬──────────┬────────...────────┐
 * │Timestamp │ KeySize  │ ValSize  │ ValOffset│    Key Bytes      │
 * │  8 bytes │  4 bytes │  4 bytes │  8 bytes │  KeySize bytes    │
 * └──────────┴──────────┴──────────┴──────────┴────────...────────┘
 * </pre>
 *
 * <p>No CRC on hint entries — the data files are the source of truth for
 * integrity. Hint files are a read-optimization only.
 */
public final class HintFile {

    /** Fixed header per hint entry: timestamp(8) + keySize(4) + valSize(4) + valOffset(8) = 24 bytes. */
    public static final int HINT_HEADER_SIZE = 24;

    private HintFile() {
    }

    /**
     * Writes hint entries for all KeyDir entries belonging to a specific data file.
     *
     * @param hintPath the path to write the hint file
     * @param entries  the KeyDir entries to persist (only those for this file)
     */
    public static void write(Path hintPath, List<HintEntry> entries) {
        try (FileChannel channel = FileChannel.open(hintPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            for (HintEntry entry : entries) {
                byte[] keyBytes = entry.key.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.allocate(HINT_HEADER_SIZE + keyBytes.length);
                buffer.putLong(entry.timestamp);
                buffer.putInt(keyBytes.length);
                buffer.putInt(entry.valueSize);
                buffer.putLong(entry.valueOffset);
                buffer.put(keyBytes);
                buffer.flip();
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }

            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write hint file: " + hintPath, e);
        }
    }

    /**
     * Reads all hint entries from a hint file.
     *
     * @param hintPath the path to the hint file
     * @return list of hint entries
     */
    public static List<HintEntry> read(Path hintPath) {
        List<HintEntry> entries = new ArrayList<>();

        try (FileChannel channel = FileChannel.open(hintPath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize == 0) return entries;

            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(fileSize, 64 * 1024));

            while (channel.read(buffer) > 0 || buffer.position() > 0) {
                buffer.flip();

                while (buffer.remaining() >= HINT_HEADER_SIZE) {
                    buffer.mark();
                    long timestamp = buffer.getLong();
                    int keySize = buffer.getInt();
                    int valueSize = buffer.getInt();
                    long valueOffset = buffer.getLong();

                    if (buffer.remaining() < keySize) {
                        buffer.reset();
                        break;
                    }

                    byte[] keyBytes = new byte[keySize];
                    buffer.get(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    entries.add(new HintEntry(key, timestamp, valueSize, valueOffset));
                }

                buffer.compact();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read hint file: " + hintPath, e);
        }

        return entries;
    }

    /**
     * Returns the hint file path for a given data file ID.
     */
    public static Path hintPathFor(Path dataDir, String fileId) {
        return dataDir.resolve(fileId + ".hint");
    }

    /**
     * A single entry in a hint file — everything needed to rebuild a KeyDir
     * entry without reading the full data file.
     */
    public record HintEntry(String key, long timestamp, int valueSize, long valueOffset) {
    }
}
