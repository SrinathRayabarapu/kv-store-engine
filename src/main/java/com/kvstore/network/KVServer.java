package com.kvstore.network;

import com.kvstore.engine.StorageEngine;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Non-blocking TCP server using Java NIO's {@link Selector} model.
 *
 * <h3>Architecture</h3>
 * <p>A single selector thread multiplexes accept/read events across all
 * client connections. When a complete request is assembled from a connection's
 * read buffer, the request is dispatched to a fixed-size worker pool that
 * executes the engine operation and writes the response back to the channel.
 *
 * <h3>Why NIO Selector over thread-per-connection?</h3>
 * <p>With 100+ concurrent clients, thread-per-connection wastes OS threads
 * that mostly block on I/O. The selector model uses one thread for I/O
 * multiplexing and a small pool for CPU-bound engine work — matching the
 * physical core count rather than the connection count.
 */
public class KVServer implements Closeable {

    private static final Logger LOG = Logger.getLogger(KVServer.class.getName());

    private final int port;
    private final RequestHandler handler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * @param handler fully built handler (plain engine-only or Raft-aware)
     */
    public static KVServer withHandler(int port, RequestHandler handler) {
        return new KVServer(port, handler);
    }

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private ExecutorService workerPool;
    private Thread selectorThread;

    public KVServer(int port, StorageEngine engine) {
        this(port, new RequestHandler(engine));
    }

    public KVServer(int port, RequestHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    /**
     * Starts the server: binds to the port, opens the selector, and begins
     * accepting connections.
     */
    public void start() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        workerPool = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "kv-worker");
                    t.setDaemon(true);
                    return t;
                });

        running.set(true);
        selectorThread = new Thread(this::selectorLoop, "kv-selector");
        selectorThread.setDaemon(true);
        selectorThread.start();

        LOG.info("KVServer started on port " + port);
    }

    private void selectorLoop() {
        while (running.get()) {
            try {
                selector.select(100); // 100ms timeout to check running flag

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (!key.isValid()) continue;

                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            } catch (ClosedSelectorException e) {
                break;
            } catch (IOException e) {
                if (running.get()) {
                    LOG.log(Level.WARNING, "Selector loop error", e);
                }
            }
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client == null) return;

        client.configureBlocking(false);
        // Attach a read buffer to each connection
        client.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(64 * 1024));
    }

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        try {
            int bytesRead = client.read(buffer);
            if (bytesRead == -1) {
                key.cancel();
                client.close();
                return;
            }

            buffer.flip();

            // Try to parse one or more complete requests from the buffer
            Protocol.ParsedRequest request;
            while ((request = Protocol.tryParseRequest(buffer)) != null) {
                final Protocol.ParsedRequest req = request;

                // Dispatch to worker pool for engine execution
                workerPool.submit(() -> {
                    try {
                        byte[] response = handler.handle(req);
                        synchronized (client) {
                            client.write(ByteBuffer.wrap(response));
                        }
                    } catch (IOException e) {
                        LOG.log(Level.WARNING, "Error writing response", e);
                        try { client.close(); } catch (IOException ignored) {}
                    }
                });
            }

            // Compact the buffer to preserve any incomplete request data
            buffer.compact();

        } catch (IOException e) {
            key.cancel();
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        running.set(false);

        try {
            if (workerPool != null) {
                workerPool.shutdown();
                workerPool.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            if (serverChannel != null) serverChannel.close();
        } catch (IOException ignored) {}

        try {
            if (selector != null) selector.close();
        } catch (IOException ignored) {}

        if (selectorThread != null) {
            try {
                selectorThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("KVServer stopped");
    }
}
