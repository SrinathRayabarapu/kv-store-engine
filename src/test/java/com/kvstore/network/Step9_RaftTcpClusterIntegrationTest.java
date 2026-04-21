package com.kvstore.network;

import com.kvstore.engine.BitcaskEngine;
import com.kvstore.engine.Compactor;
import com.kvstore.replication.RaftNode;
import com.kvstore.replication.RaftRpcServer;
import com.kvstore.replication.TcpRaftRpc;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end Raft over real TCP sockets: three KV servers, three raft RPC ports,
 * same JVM (loopback). Proves multi-node replication and client REDIRECT.
 */
@DisplayName("Step 9 — Raft TCP cluster (multi-node)")
class Step9_RaftTcpClusterIntegrationTest {

    private static final String HOST = "127.0.0.1";

    @TempDir
    Path tempDir;

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(closeables);
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
        closeables.clear();
        nodes.clear();
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @DisplayName("3-node TCP Raft: PUT to follower returns REDIRECT; PUT to leader succeeds")
    void followerRedirects_putOnLeaderSucceeds() throws Exception {
        int k1 = freePort(), r1 = freePort();
        int k2 = freePort(), r2 = freePort();
        int k3 = freePort(), r3 = freePort();

        Map<Integer, InetSocketAddress> allClients = Map.of(
                1, new InetSocketAddress(HOST, k1),
                2, new InetSocketAddress(HOST, k2),
                3, new InetSocketAddress(HOST, k3));

        startNode(1, k1, r1, tempDir.resolve("n1"),
                Map.of(2, new InetSocketAddress(HOST, r2), 3, new InetSocketAddress(HOST, r3)), allClients);
        startNode(2, k2, r2, tempDir.resolve("n2"),
                Map.of(1, new InetSocketAddress(HOST, r1), 3, new InetSocketAddress(HOST, r3)), allClients);
        startNode(3, k3, r3, tempDir.resolve("n3"),
                Map.of(1, new InetSocketAddress(HOST, r1), 2, new InetSocketAddress(HOST, r2)), allClients);

        Thread.sleep(3_000);

        RaftNode leader = waitForLeader(10_000);
        assertNotNull(leader, "expected a leader");
        int leaderId = leader.getNodeId();

        int followerKvPort = -1;
        for (int id : List.of(1, 2, 3)) {
            if (id != leaderId) {
                followerKvPort = allClients.get(id).getPort();
                break;
            }
        }
        assertTrue(followerKvPort > 0);

        try (SocketChannel fc = openClient(followerKvPort)) {
            // testing follower redirect
            send(fc, Protocol.encodePutRequest("cluster-key", "from-test".getBytes()));
            Protocol.ParsedResponse redir = readResp(fc, 30_000);
            assertEquals(Protocol.STATUS_REDIRECT, redir.status(), "follower should redirect writes");

            InetSocketAddress leaderAddr = Protocol.decodeRedirectPayload(redir.payload());
            try (SocketChannel lc = openClient(leaderAddr.getPort())) {
                send(lc, Protocol.encodePutRequest("cluster-key", "from-test".getBytes()));
                Protocol.ParsedResponse ok = readResp(lc, 30_000);
                assertEquals(Protocol.STATUS_OK, ok.status());

                // Apply loop runs every 10ms — allow state machine to catch up before GET
                String value = null;
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline) {
                    send(lc, Protocol.encodeGetRequest("cluster-key"));
                    Protocol.ParsedResponse get = readResp(lc, 30_000);
                    if (get.status() == Protocol.STATUS_OK) {
                        value = new String(get.payload(), StandardCharsets.UTF_8);
                        break;
                    }
                    Thread.sleep(20);
                }
                assertEquals("from-test", value);
            }
        }
    }

    private void startNode(int nodeId, int kvPort, int raftPort, Path dataDir,
                           Map<Integer, InetSocketAddress> peerRaft,
                           Map<Integer, InetSocketAddress> allClients) throws Exception {
        List<Integer> peerIds = new ArrayList<>(peerRaft.keySet());
        Collections.sort(peerIds);

        BitcaskEngine engine = new BitcaskEngine(dataDir);
        TcpRaftRpc rpc = new TcpRaftRpc(new HashMap<>(peerRaft));
        RaftNode node = new RaftNode(nodeId, peerIds, engine, rpc);
        RaftRpcServer rs = new RaftRpcServer(raftPort, node);
        rs.start();
        node.start();
        RaftRouting routing = new RaftRouting(node, new HashMap<>(allClients));
        KVServer kv = new KVServer(kvPort, new RequestHandler(engine, routing, new Compactor(engine)));
        kv.start();

        nodes.add(node);
        closeables.add(kv);
        closeables.add(rs);
        closeables.add(node);
        closeables.add(rpc);
        closeables.add(engine);
    }

    private RaftNode waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : nodes) {
                if (n.getState() == RaftNode.State.LEADER) {
                    return n;
                }
            }
            Thread.sleep(100);
        }
        return null;
    }

    private static SocketChannel openClient(int port) throws IOException {
        SocketChannel ch = SocketChannel.open();
        ch.connect(new InetSocketAddress(HOST, port));
        ch.configureBlocking(true);
        return ch;
    }

    private static void send(SocketChannel ch, byte[] data) throws IOException {
        ch.write(ByteBuffer.wrap(data));
    }

    private static Protocol.ParsedResponse readResp(SocketChannel ch, int timeoutMs) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(5);
        readFully(ch, header, timeoutMs);
        header.flip();
        byte st = header.get();
        int len = header.getInt();
        byte[] payload = new byte[len];
        if (len > 0) {
            readFully(ch, ByteBuffer.wrap(payload), timeoutMs);
        }
        return new Protocol.ParsedResponse(st, payload);
    }

    private static void readFully(SocketChannel ch, ByteBuffer buf, int timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (buf.hasRemaining()) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("read timeout");
            }
            int n = ch.read(buf);
            if (n == -1) {
                throw new IOException("closed");
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }
}
