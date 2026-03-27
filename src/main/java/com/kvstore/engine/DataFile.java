package com.kvstore.engine;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single append-only log file backed by a {@link FileChannel}.
 *
 * <p>Each DataFile is identified by a monotonically increasing file ID.
 * All writes are appended sequentially — this is why Bitcask's write path
 * achieves near-sequential-I/O throughput even for random key patterns.
 *
 * <h3>Thread safety</h3>
 * <p>Writes are serialized by a {@link ReentrantLock} to guarantee that
 * offset reservation + write are atomic. Reads use positional I/O
 * ({@link FileChannel#read(ByteBuffer, long)}) which is safe for concurrent
 * access without external locking — multiple readers can read different offsets
 * in the same file concurrently.
 *
 * <h3>Durability</h3>
 * <p>The caller controls fsync policy. After {@link #append(byte[])} returns,
 * the data is written to the OS page cache. Call {@link #sync()} explicitly
 * to force the data to stable storage.
 */
public class DataFile implements Closeable {

    private final String fileId;
    private final Path path;
    private final FileChannel channel;
    private final ReentrantLock writeLock = new ReentrantLock();
    private long writeOffset;

    /**
     * Opens (or creates) a data file for append-only writing and positional reading.
     *
     * @param directory the directory containing data files
     * @param fileId    unique monotonic identifier (used in file name and KeyDir entries)
     * @throws UncheckedIOException if the file cannot be opened
     */
    public DataFile(Path directory, String fileId) {
        this.fileId = fileId;
        this.path = directory.resolve(fileId + ".data");
        try {
            this.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.writeOffset = channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open data file: " + path, e);
        }
    }

    /**
     * Appends raw bytes to the end of the file.
     *
     * <p>The write lock ensures that offset reservation and the actual write
     * are atomic — two concurrent appenders never receive overlapping offset ranges.
     *
     * @param data the complete record bytes (already serialized by {@link RecordSerializer})
     * @return the byte offset at which the record was written
     */
    public long append(byte[] data) {
        writeLock.lock();
        try {
            long offset = writeOffset;
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer, offset + written);
            }
            writeOffset += written;
            return offset;
        } catch (IOException e) {
            throw new UncheckedIOException("Append failed at offset " + writeOffset, e);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Reads exactly {@code length} bytes starting at {@code offset}.
     *
     * <p>Uses positional read — does not move the channel's internal position,
     * so multiple threads can read concurrently without synchronization.
     *
     * @param offset byte offset within the file
     * @param length number of bytes to read
     * @return the requested bytes
     * @throws UncheckedIOException if the read fails or returns fewer bytes than expected
     */
    public byte[] read(long offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        try {
            int totalRead = 0;
            while (totalRead < length) {
                int n = channel.read(buffer, offset + totalRead);
                if (n == -1) {
                    throw new UncheckedIOException(new IOException(
                            "Unexpected EOF at offset " + (offset + totalRead)
                                    + " while reading " + length + " bytes from " + path));
                }
                totalRead += n;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Read failed at offset " + offset + " in " + path, e);
        }
        return buffer.array();
    }

    /**
     * Reads all bytes from the file, starting at offset 0.
     * Used during crash recovery to replay the entire data file.
     *
     * @return all file contents as a byte array
     */
    public byte[] readAll() {
        try {
            long size = channel.size();
            if (size > Integer.MAX_VALUE) {
                throw new UncheckedIOException(new IOException(
                        "Data file too large for single read: " + size + " bytes"));
            }
            return read(0, (int) size);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read file size: " + path, e);
        }
    }

    /**
     * Forces all pending writes to stable storage.
     * Call this according to your fsync policy (every write, on rotation, on close).
     */
    public void sync() {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("fsync failed for " + path, e);
        }
    }

    public String getFileId() {
        return fileId;
    }

    public Path getPath() {
        return path;
    }

    /**
     * Current write offset — the byte position where the next append will start.
     * Also represents the total bytes written to this file.
     */
    public long getWriteOffset() {
        return writeOffset;
    }

    @Override
    public void close() {
        try {
            channel.force(true);
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close data file: " + path, e);
        }
    }
}
