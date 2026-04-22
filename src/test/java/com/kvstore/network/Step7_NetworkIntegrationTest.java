package com.kvstore.network;

import com.kvstore.engine.BitcaskEngine;
import com.kvstore.engine.Compactor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 7 integration tests — verifies the NIO TCP server handles concurrent
 * connections and all 5 protocol operations correctly.
 */
@DisplayName("Step 7 — Network Integration")
class Step7_NetworkIntegrationTest {

    @TempDir
    Path tempDir;

    private BitcaskEngine engine;
    private KVServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        engine = new BitcaskEngine(tempDir);
        // Use port 0 to let the OS assign a free port
        port = findFreePort();
        server = new KVServer(port, engine);
        server.start();
        Thread.sleep(100); // Let the server start
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.close();
        if (engine != null) engine.close();
    }

    @Test
    @DisplayName("PUT then GET returns the stored value over TCP")
    void putThenGet_overTcp() throws Exception {
        try (SocketChannel client = connect()) {
            // PUT
            send(client, Protocol.encodePutRequest("tcp-key", "tcp-value".getBytes()));
            Protocol.ParsedResponse putResp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, putResp.status());

            // GET
            send(client, Protocol.encodeGetRequest("tcp-key"));
            Protocol.ParsedResponse getResp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, getResp.status());
            assertEquals("tcp-value", new String(getResp.payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("GET on non-existent key returns NOT_FOUND")
    void get_nonExistent_returnsNotFound() throws Exception {
        try (SocketChannel client = connect()) {
            send(client, Protocol.encodeGetRequest("missing"));
            Protocol.ParsedResponse resp = readResponse(client);
            assertEquals(Protocol.STATUS_NOT_FOUND, resp.status());
        }
    }

    @Test
    @DisplayName("DELETE removes a key accessible over TCP")
    void delete_overTcp() throws Exception {
        try (SocketChannel client = connect()) {
            send(client, Protocol.encodePutRequest("del-key", "val".getBytes()));
            readResponse(client);

            send(client, Protocol.encodeDeleteRequest("del-key"));
            Protocol.ParsedResponse delResp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, delResp.status());

            send(client, Protocol.encodeGetRequest("del-key"));
            Protocol.ParsedResponse getResp = readResponse(client);
            assertEquals(Protocol.STATUS_NOT_FOUND, getResp.status());
        }
    }

    @Test
    @DisplayName("RANGE query returns sorted results over TCP")
    void range_returnsSortedResults() throws Exception {
        try (SocketChannel client = connect()) {
            send(client, Protocol.encodePutRequest("banana", "yellow".getBytes()));
            readResponse(client);
            send(client, Protocol.encodePutRequest("apple", "red".getBytes()));
            readResponse(client);
            send(client, Protocol.encodePutRequest("cherry", "dark".getBytes()));
            readResponse(client);

            send(client, Protocol.encodeRangeRequest("apple", "cherry"));
            Protocol.ParsedResponse resp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, resp.status());

            List<Map.Entry<String, byte[]>> entries = Protocol.decodeRangePayload(resp.payload());
            assertEquals(3, entries.size());
            assertEquals("apple", entries.get(0).getKey());
            assertEquals("banana", entries.get(1).getKey());
            assertEquals("cherry", entries.get(2).getKey());
        }
    }

    @Test
    @DisplayName("COMPACT over TCP returns OK with reclaimed byte count (local maintenance)")
    void compact_overTcp() throws Exception {
        Path sub = tempDir.resolve("compact-tcp");
        int p = findFreePort();
        BitcaskEngine eng = new BitcaskEngine(sub, 512);
        Compactor comp = new Compactor(eng);
        KVServer srv = new KVServer(p, new RequestHandler(eng, comp));
        srv.start();
        Thread.sleep(100);
        try (SocketChannel client = SocketChannel.open()) {
            client.connect(new InetSocketAddress("localhost", p));
            client.configureBlocking(true);
            for (int i = 0; i < 20; i++) {
                send(client, Protocol.encodePutRequest("c-key-" + i, ("v-" + i).getBytes()));
                readResponse(client);
            }
            eng.rotateActiveFileForTests();
            for (int i = 0; i < 20; i++) {
                send(client, Protocol.encodePutRequest("c-key-" + i, ("v2-" + i).getBytes()));
                readResponse(client);
            }
            send(client, Protocol.encodeCompactRequest());
            Protocol.ParsedResponse resp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, resp.status());
            long reclaimed = Protocol.decodeOkLongPayload(resp.payload());
            assertTrue(reclaimed >= 0);
            send(client, Protocol.encodeGetRequest("c-key-0"));
            Protocol.ParsedResponse getResp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, getResp.status());
            assertTrue(new String(getResp.payload(), StandardCharsets.UTF_8).startsWith("v2-"));
        } finally {
            srv.close();
            eng.close();
        }
    }

    @Test
    @DisplayName("BATCH_PUT writes multiple keys atomically over TCP")
    void batchPut_overTcp() throws Exception {
        try (SocketChannel client = connect()) {
            List<String> keys = List.of("b1", "b2", "b3");
            List<byte[]> values = List.of("v1".getBytes(), "v2".getBytes(), "v3".getBytes());
            send(client, Protocol.encodeBatchPutRequest(keys, values));
            Protocol.ParsedResponse batchResp = readResponse(client);
            assertEquals(Protocol.STATUS_OK, batchResp.status());

            for (int i = 0; i < keys.size(); i++) {
                send(client, Protocol.encodeGetRequest(keys.get(i)));
                Protocol.ParsedResponse getResp = readResponse(client);
                assertEquals(Protocol.STATUS_OK, getResp.status());
                assertArrayEquals(values.get(i), getResp.payload());
            }
        }
    }

    /**
     * Handbook target: 100 concurrent TCP clients × 1,000 PUT+GET pairs each (200k RPC round-trips).
     * Prints wall-clock ms and aggregate throughput to stderr for documenting in README.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    @DisplayName("Stress: 100 clients × 1000 ops each (handbook-scale load test)")
    void stress_100Clients_1000Ops_handbookScale() throws Exception {
        int clientCount = 100;
        int opsPerClient = 1000;
        long t0 = System.nanoTime();
        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int c = 0; c < clientCount; c++) {
            final int clientId = c;
            futures.add(executor.submit(() -> {
                try (SocketChannel client = connect()) {
                    for (int i = 0; i < opsPerClient; i++) {
                        String key = "s" + clientId + "-k" + i;
                        byte[] value = ("v" + i).getBytes();
                        send(client, Protocol.encodePutRequest(key, value));
                        Protocol.ParsedResponse putResp = readResponse(client, 120_000);
                        if (putResp.status() != Protocol.STATUS_OK) {
                            return false;
                        }
                        send(client, Protocol.encodeGetRequest(key));
                        Protocol.ParsedResponse getResp = readResponse(client, 120_000);
                        if (getResp.status() != Protocol.STATUS_OK) {
                            return false;
                        }
                        if (!Arrays.equals(value, getResp.payload())) {
                            return false;
                        }
                    }
                    return true;
                }
            }));
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(25, TimeUnit.MINUTES));

        for (Future<Boolean> f : futures) {
            assertTrue(f.get(), "A stress client failed");
        }

        long wallMs = (System.nanoTime() - t0) / 1_000_000L;
        long totalOps = (long) clientCount * opsPerClient * 2L;
        double aggKops = totalOps / (wallMs / 1000.0) / 1000.0;
        System.err.println("STRESS_BENCHMARK clientCount=" + clientCount
                + " opsPerClient=" + opsPerClient
                + " totalRpcRoundTrips=" + totalOps
                + " wallMs=" + wallMs
                + " aggregateKiloRpcPerSec=" + String.format("%.2f", aggKops));
    }

    @Test
    @DisplayName("Multiple concurrent clients perform operations without errors")
    void concurrentClients_noErrors() throws Exception {
        int clientCount = 20;
        int opsPerClient = 100;
        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int c = 0; c < clientCount; c++) {
            final int clientId = c;
            futures.add(executor.submit(() -> {
                try (SocketChannel client = connect()) {
                    for (int i = 0; i < opsPerClient; i++) {
                        String key = "c" + clientId + "-k" + i;
                        byte[] value = ("v" + i).getBytes();

                        // PUT
                        send(client, Protocol.encodePutRequest(key, value));
                        Protocol.ParsedResponse putResp = readResponse(client);
                        if (putResp.status() != Protocol.STATUS_OK) return false;

                        // GET
                        send(client, Protocol.encodeGetRequest(key));
                        Protocol.ParsedResponse getResp = readResponse(client);
                        if (getResp.status() != Protocol.STATUS_OK) return false;
                        if (!new String(getResp.payload()).equals(new String(value))) return false;
                    }
                    return true;
                }
            }));
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        for (Future<Boolean> f : futures) {
            assertTrue(f.get(), "A client encountered an error");
        }
    }

    @Test
    @DisplayName("Malformed request (invalid opcode) returns ERROR, server stays alive")
    void malformedRequest_returnsError_serverSurvives() throws Exception {
        try (SocketChannel client = connect()) {
            // Send a request with invalid opcode 0xFF
            ByteBuffer buf = ByteBuffer.allocate(5);
            buf.put((byte) 0xFF);
            buf.putInt(0); // zero-length payload
            buf.flip();
            client.write(buf);

            Protocol.ParsedResponse resp = readResponse(client);
            assertEquals(Protocol.STATUS_ERROR, resp.status());

            // Server should still work — send a valid GET
            send(client, Protocol.encodeGetRequest("test"));
            Protocol.ParsedResponse getResp = readResponse(client);
            assertEquals(Protocol.STATUS_NOT_FOUND, getResp.status());
        }
    }

    // --- Helpers ---

    private SocketChannel connect() throws IOException {
        SocketChannel ch = SocketChannel.open();
        ch.connect(new InetSocketAddress("localhost", port));
        ch.configureBlocking(true);
        return ch;
    }

    private void send(SocketChannel channel, byte[] data) throws IOException {
        channel.write(ByteBuffer.wrap(data));
    }

    private Protocol.ParsedResponse readResponse(SocketChannel channel) throws IOException {
        return readResponse(channel, 5_000);
    }

    private Protocol.ParsedResponse readResponse(SocketChannel channel, int timeoutMs) throws IOException {
        ByteBuffer headerBuf = ByteBuffer.allocate(5);
        readFully(channel, headerBuf, timeoutMs);
        headerBuf.flip();
        byte status = headerBuf.get();
        int payloadLen = headerBuf.getInt();

        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) {
            ByteBuffer payloadBuf = ByteBuffer.wrap(payload);
            readFully(channel, payloadBuf, timeoutMs);
        }

        return new Protocol.ParsedResponse(status, payload);
    }

    private void readFully(SocketChannel channel, ByteBuffer buffer, int timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (buffer.hasRemaining()) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("Read timeout");
            }
            int n = channel.read(buffer);
            if (n == -1) throw new IOException("Connection closed");
        }
    }

    private int findFreePort() throws IOException {
        try (java.net.ServerSocket ss = new java.net.ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }
}
