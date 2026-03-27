package com.kvstore;

import com.kvstore.engine.BitcaskEngine;
import com.kvstore.engine.CrashRecovery;
import com.kvstore.network.KVServer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Entry point for the KV Store Engine.
 *
 * <h3>Single-node mode:</h3>
 * <pre>
 *   java -jar kv-store-engine.jar --port 7777 --data-dir ./data
 * </pre>
 *
 * <h3>CLI arguments:</h3>
 * <ul>
 *   <li>{@code --port <n>} — TCP port to listen on (default: 7777)</li>
 *   <li>{@code --data-dir <path>} — directory for data files (default: ./data)</li>
 *   <li>{@code --max-file-size <bytes>} — max data file size before rotation (default: 256MB)</li>
 * </ul>
 */
public class KVStoreMain {

    private static final Logger LOG = Logger.getLogger(KVStoreMain.class.getName());

    public static void main(String[] args) throws IOException {
        int port = 7777;
        String dataDir = "./data";
        long maxFileSize = BitcaskEngine.DEFAULT_MAX_FILE_SIZE;

        // Simple CLI argument parsing — no external libraries
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data-dir" -> dataDir = args[++i];
                case "--max-file-size" -> maxFileSize = Long.parseLong(args[++i]);
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

        Path dataDirPath = Path.of(dataDir);
        BitcaskEngine engine = new BitcaskEngine(dataDirPath, maxFileSize);

        // Recover existing data if present
        if (dataDirPath.toFile().exists() && dataDirPath.toFile().list().length > 0) {
            LOG.info("Existing data directory found — running recovery...");
            boolean usedHints = CrashRecovery.recover(engine);
            LOG.info("Recovery complete: " + (usedHints ? "via hint files" : "via full replay"));
        }

        KVServer server = new KVServer(port, engine);
        server.start();

        LOG.info("KV Store Engine running on port " + port + " — data dir: " + dataDir);
        LOG.info("Press Ctrl+C to stop");

        // Graceful shutdown hook — writes hint files and closes cleanly
        final BitcaskEngine finalEngine = engine;
        final KVServer finalServer = server;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received — writing hint files...");
            CrashRecovery.writeHintFiles(finalEngine);
            finalServer.close();
            finalEngine.close();
            LOG.info("Clean shutdown complete");
        }, "shutdown-hook"));

        // Block the main thread
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printUsage() {
        System.out.println("""
                KV Store Engine — Bitcask-inspired persistent key/value store
                
                Usage:
                  java -jar kv-store-engine.jar [options]
                
                Options:
                  --port <n>              TCP port (default: 7777)
                  --data-dir <path>       Data directory (default: ./data)
                  --max-file-size <bytes> Max file size before rotation (default: 268435456)
                  --help, -h              Show this help message
                
                Examples:
                  # Single node
                  java -jar kv-store-engine.jar --port 7777 --data-dir ./data
                
                  # Custom file size (64 MB)
                  java -jar kv-store-engine.jar --port 7777 --data-dir ./data --max-file-size 67108864
                """);
    }
}
