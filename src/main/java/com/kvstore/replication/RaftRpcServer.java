package com.kvstore.replication;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Accepts incoming Raft RPC connections on a dedicated TCP port and dispatches
 * to {@link RaftNode} via {@link RaftWireCodec}.
 */
public final class RaftRpcServer implements Closeable {

    private static final Logger LOG = Logger.getLogger(RaftRpcServer.class.getName());

    private final int port;
    private final RaftNode node;
    private final ExecutorService acceptExecutor;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private ServerSocket serverSocket;

    public RaftRpcServer(int port, RaftNode node) {
        this.port = port;
        this.node = node;
        this.acceptExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-rpc-accept");
            t.setDaemon(true);
            return t;
        });
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "raft-rpc-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Binds the server socket and starts the accept loop (non-blocking for caller).
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        LOG.info("Raft RPC server listening on port " + port);
        acceptExecutor.submit(this::acceptLoop);
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket s = serverSocket.accept();
                s.setSoTimeout(30_000);
                workers.submit(() -> handleOne(s));
            } catch (IOException e) {
                if (running.get()) {
                    LOG.log(Level.FINE, "Raft accept error", e);
                }
            }
        }
    }

    private void handleOne(Socket s) {
        try (s) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            int len = in.readInt();
            if (len <= 0 || len > 64 * 1024 * 1024) {
                return;
            }
            byte[] req = new byte[len];
            in.readFully(req);
            byte[] resp = RaftWireCodec.dispatch(req, node);
            out.writeInt(resp.length);
            out.write(resp);
            out.flush();
        } catch (Exception e) {
            LOG.log(Level.FINE, "Raft RPC session error", e);
        }
    }

    @Override
    public void close() {
        running.set(false);
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        acceptExecutor.shutdownNow();
        workers.shutdown();
        try {
            workers.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Raft RPC server stopped on port " + port);
    }
}
