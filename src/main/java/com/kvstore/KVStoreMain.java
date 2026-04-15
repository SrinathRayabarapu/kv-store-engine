package com.kvstore;

import com.kvstore.engine.BitcaskEngine;
import com.kvstore.engine.CrashRecovery;
import com.kvstore.network.KVServer;
import com.kvstore.network.RaftRouting;
import com.kvstore.network.RequestHandler;
import com.kvstore.replication.RaftNode;
import com.kvstore.replication.RaftRpcServer;
import com.kvstore.replication.TcpRaftRpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;

/**
 * Entry point for the KV Store Engine.
 *
 * <p><b>Single-node:</b> {@code --port} and {@code --data-dir} only.
 *
 * <p><b>Raft cluster:</b> add {@code --node-id}, {@code --raft-port}, and one or more
 * {@code --peer id@host:raftPort:clientPort} (repeat per remote member). Each process
 * uses its {@code --port} for the KV client protocol and {@code --raft-port} for Raft RPC.
 */
public class KVStoreMain {

    private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(KVStoreMain.class.getName());

    public static void main(String[] args) throws IOException {
        configureLogging();

        int port = 7777;
        String dataDir = "./data";
        long maxFileSize = BitcaskEngine.DEFAULT_MAX_FILE_SIZE;
        Integer nodeId = null;
        Integer raftPort = null;
        String advertiseHost = "127.0.0.1";
        List<String> peerSpecs = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data-dir" -> dataDir = args[++i];
                case "--max-file-size" -> maxFileSize = Long.parseLong(args[++i]);
                case "--node-id" -> nodeId = Integer.parseInt(args[++i]);
                case "--raft-port" -> raftPort = Integer.parseInt(args[++i]);
                case "--advertise-host" -> advertiseHost = args[++i];
                case "--peer" -> peerSpecs.add(args[++i]);
                case "--help", "-h" -> {
                    printUsage();
                    return;
                }
                default -> {
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
                }
            }
        }

        boolean raftMode = nodeId != null || raftPort != null || !peerSpecs.isEmpty();
        if (raftMode) {
            if (nodeId == null || raftPort == null || peerSpecs.isEmpty()) {
                System.err.println("Raft mode requires --node-id, --raft-port, and at least one --peer");
                printUsage();
                System.exit(1);
            }
        }

        Path dataDirPath = Path.of(dataDir);
        BitcaskEngine engine = new BitcaskEngine(dataDirPath, maxFileSize);

        if (dataDirPath.toFile().exists() && Objects.requireNonNullElse(dataDirPath.toFile().list(), new String[0]).length > 0) {
            LOG.info("Existing data directory found — running recovery...");
            boolean usedHints = CrashRecovery.recover(engine);
            LOG.info("Recovery complete: " + (usedHints ? "via hint files" : "via full replay"));
        }

        KVServer server;
        RaftRpcServer raftServer = null;
        RaftNode raftNode = null;
        TcpRaftRpc tcpRaft = null;

        if (raftMode) {
            Map<Integer, InetSocketAddress> raftAddrs = new HashMap<>();
            Map<Integer, InetSocketAddress> clientAddrs = new HashMap<>();
            List<Integer> peerIds = new ArrayList<>();

            for (String spec : peerSpecs) {
                ParsedPeer p = parsePeer(spec);
                peerIds.add(p.id());
                raftAddrs.put(p.id(), new InetSocketAddress(p.host(), p.raftPort()));
                clientAddrs.put(p.id(), new InetSocketAddress(p.host(), p.clientPort()));
            }
            clientAddrs.put(nodeId, new InetSocketAddress(advertiseHost, port));

            tcpRaft = new TcpRaftRpc(raftAddrs);
            raftNode = new RaftNode(nodeId, peerIds, engine, tcpRaft);
            raftServer = new RaftRpcServer(raftPort, raftNode);
            raftServer.start();
            raftNode.start();

            RaftRouting routing = new RaftRouting(raftNode, clientAddrs);
            server = new KVServer(port, new RequestHandler(engine, routing));
            LOG.info("Raft cluster member nodeId=" + nodeId + " raftPort=" + raftPort + " clientPort=" + port);
        } else {
            server = new KVServer(port, engine);
        }

        server.start();

        LOG.info("KV Store Engine running on port " + port + " — data dir: " + dataDir);
        LOG.info("Press Ctrl+C to stop");

        final BitcaskEngine finalEngine = engine;
        final KVServer finalServer = server;
        final RaftRpcServer finalRaftSrv = raftServer;
        final RaftNode finalRaft = raftNode;
        final TcpRaftRpc finalTcp = tcpRaft;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received — writing hint files...");
            CrashRecovery.writeHintFiles(finalEngine);
            finalServer.close();
            if (finalRaftSrv != null) {
                finalRaftSrv.close();
            }
            if (finalRaft != null) {
                finalRaft.close();
            }
            if (finalTcp != null) {
                finalTcp.close();
            }
            finalEngine.close();
            LOG.info("Clean shutdown complete");
        }, "shutdown-hook"));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Installs {@link LogFormatter} on the root logger so every handler
     * (console, file redirect) emits single-line, timestamped records.
     */
    private static void configureLogging() {
        LogFormatter formatter = new LogFormatter();
        java.util.logging.Logger root = java.util.logging.Logger.getLogger("");
        for (java.util.logging.Handler h : root.getHandlers()) {
            h.setFormatter(formatter);
        }
    }

    private record ParsedPeer(int id, String host, int raftPort, int clientPort) {
    }

    /**
     * Format: {@code id@host:raftPort:clientPort}, e.g. {@code 2@127.0.0.1:9778:8778}.
     */
    static ParsedPeer parsePeer(String spec) {
        int at = spec.indexOf('@');
        if (at < 0) {
            throw new IllegalArgumentException("Peer must be id@host:raftPort:clientPort: " + spec);
        }
        int id = Integer.parseInt(spec.substring(0, at));
        String rest = spec.substring(at + 1);
        String[] parts = rest.split(":");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Peer must include host, raft port, client port: " + spec);
        }
        int clientPort = Integer.parseInt(parts[parts.length - 1]);
        int rp = Integer.parseInt(parts[parts.length - 2]);
        String host = String.join(":", Arrays.copyOfRange(parts, 0, parts.length - 2));
        return new ParsedPeer(id, host, rp, clientPort);
    }

    private static void printUsage() {
        System.out.println("""
                KV Store Engine — Bitcask + optional Raft (TCP) replication

                Single-node:
                  java -jar kv-store-engine.jar --port 7777 --data-dir ./data

                Raft cluster (one JVM per node; start all nodes, then use client against leader):
                  java -jar kv-store-engine.jar \\
                    --node-id 1 --port 7777 --raft-port 9777 --data-dir ./data/node1 \\
                    --advertise-host 127.0.0.1 \\
                    --peer 2@127.0.0.1:9778:7778 \\
                    --peer 3@127.0.0.1:9779:7779

                Options:
                  --port <n>              KV client TCP port (default: 7777)
                  --data-dir <path>       Data directory (default: ./data)
                  --max-file-size <bytes> Max data file size before rotation (default: 268435456)
                  --node-id <n>           Raft member id (required with --raft-port / --peer)
                  --raft-port <n>         TCP port for Raft RPC (RequestVote / AppendEntries)
                  --peer <spec>           Remote member: id@host:raftPort:clientPort (repeat)
                  --advertise-host <h>    Hostname clients use to reach THIS node's KV port (default: 127.0.0.1)
                  --help, -h              This message

                Followers return status 0x03 REDIRECT with leader host:port for the KV protocol.
                """);
    }
}
