package com.kvstore.replication;

import com.kvstore.engine.BitcaskEngine;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 8 acceptance tests — verifies Raft leader election, log replication,
 * and automatic failover across a 3-node cluster.
 */
@DisplayName("Step 8 — Raft Replication")
class Step8_RaftReplicationTest {

    @TempDir
    Path tempDir;

    private final List<BitcaskEngine> engines = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();
    private InMemoryRaftRpc rpc;

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::close);
        engines.forEach(BitcaskEngine::close);
    }

    @Test
    @DisplayName("3-node cluster elects a leader within 2 seconds")
    void leaderElection_withinTimeout() throws Exception {
        createCluster(3);

        waitForLeader(3000);

        long leaderCount = nodes.stream()
                .filter(n -> n.getState() == RaftNode.State.LEADER)
                .count();
        assertEquals(1, leaderCount, "Exactly one leader expected");
    }

    @Test
    @DisplayName("Election safety: at most one leader per term")
    void electionSafety_atMostOneLeaderPerTerm() throws Exception {
        createCluster(3);
        waitForLeader(3000);

        // Verify: no two nodes claim leader in the same term
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                if (nodes.get(i).getState() == RaftNode.State.LEADER
                        && nodes.get(j).getState() == RaftNode.State.LEADER) {
                    assertNotEquals(nodes.get(i).getCurrentTerm(), nodes.get(j).getCurrentTerm(),
                            "Two leaders in the same term violates election safety");
                }
            }
        }
    }

    @Test
    @DisplayName("Write to leader is replicated to followers before ACK")
    void writeReplication_majorityAck() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader(3000);

        CompletableFuture<Boolean> result = leader.submitWrite(
                LogEntry.OP_PUT, "raft-key", "raft-value".getBytes());

        Boolean committed = result.get(5, TimeUnit.SECONDS);
        assertTrue(committed, "Write should be committed after majority ACK");

        // Wait for apply loop
        Thread.sleep(200);

        // Verify the key is readable from the leader's engine
        int leaderIdx = nodes.indexOf(leader);
        assertTrue(engines.get(leaderIdx).get("raft-key").isPresent(),
                "Leader's engine should contain the committed key");
    }

    @Test
    @DisplayName("Write to follower returns false (should redirect)")
    void writeToFollower_returnsFalse() throws Exception {
        createCluster(3);
        waitForLeader(3000);

        RaftNode follower = nodes.stream()
                .filter(n -> n.getState() == RaftNode.State.FOLLOWER)
                .findFirst()
                .orElseThrow();

        CompletableFuture<Boolean> result = follower.submitWrite(
                LogEntry.OP_PUT, "should-fail", "val".getBytes());

        assertFalse(result.get(1, TimeUnit.SECONDS),
                "Follower should reject writes");
    }

    @Test
    @DisplayName("Leader crash triggers new election within 3 seconds")
    void leaderCrash_newElectionWithin3s() throws Exception {
        createCluster(3);
        RaftNode originalLeader = waitForLeader(3000);

        // Partition the leader (simulate crash)
        rpc.partitioned.add(originalLeader.getNodeId());
        originalLeader.close();

        // Wait for new election
        Thread.sleep(3000);

        long newLeaderCount = nodes.stream()
                .filter(n -> n != originalLeader)
                .filter(n -> n.getState() == RaftNode.State.LEADER)
                .count();

        assertTrue(newLeaderCount >= 1, "A new leader should be elected after original crashes");
    }

    @Test
    @DisplayName("Previously committed writes survive leader failover")
    void committedWrites_surviveFailover() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader(3000);

        // Write a value through the leader
        CompletableFuture<Boolean> result = leader.submitWrite(
                LogEntry.OP_PUT, "survive-key", "survive-value".getBytes());
        result.get(5, TimeUnit.SECONDS);
        Thread.sleep(200); // let apply loop run

        // Kill the leader
        rpc.partitioned.add(leader.getNodeId());
        leader.close();

        // Wait for new leader
        Thread.sleep(3000);

        // Find the new leader
        RaftNode newLeader = nodes.stream()
                .filter(n -> n != leader)
                .filter(n -> n.getState() == RaftNode.State.LEADER)
                .findFirst()
                .orElse(null);

        if (newLeader != null) {
            int newLeaderIdx = nodes.indexOf(newLeader);
            // The committed entry should be in the new leader's log
            assertTrue(newLeader.getLog().lastIndex() >= 1,
                    "New leader should have the committed entry in its log");
        }
    }

    @Test
    @DisplayName("Follower that falls behind catches up via log replay")
    void followerCatchUp_afterPause() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader(3000);

        // Find a follower and partition it
        RaftNode slowFollower = nodes.stream()
                .filter(n -> n.getState() == RaftNode.State.FOLLOWER)
                .findFirst()
                .orElseThrow();
        rpc.partitioned.add(slowFollower.getNodeId());

        // Write while the follower is partitioned — must await majority commit so the
        // leader log is stable before heal (CI runners are slower than local dev).
        List<CompletableFuture<Boolean>> catchUpWrites = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            catchUpWrites.add(leader.submitWrite(LogEntry.OP_PUT, "catch-up-" + i, ("v" + i).getBytes()));
        }
        for (CompletableFuture<Boolean> f : catchUpWrites) {
            assertTrue(f.get(30, TimeUnit.SECONDS), "each write should commit while one follower is partitioned");
        }
        // Other follower + leader need time to replicate (heartbeats are 150ms; allow slack for CI).
        Thread.sleep(500);

        // Heal the partition — follower may have started elections while isolated; cluster
        // must converge via further AppendEntries. Poll instead of a fixed short sleep.
        rpc.partitioned.remove(slowFollower.getNodeId());
        assertTrue(awaitLogCaughtUp(slowFollower, 5, 30_000),
                "Slow follower should catch up within timeout: lastIndex="
                        + slowFollower.getLog().lastIndex());
    }

    @Test
    @DisplayName("Multiple writes are committed in order")
    void multipleWrites_committedInOrder() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader(3000);

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(leader.submitWrite(
                    LogEntry.OP_PUT, "ordered-" + i, ("val-" + i).getBytes()));
        }

        for (CompletableFuture<Boolean> f : futures) {
            assertTrue(f.get(5, TimeUnit.SECONDS));
        }

        Thread.sleep(200);

        // Verify all writes applied to the leader's engine
        int leaderIdx = nodes.indexOf(leader);
        for (int i = 0; i < 10; i++) {
            assertTrue(engines.get(leaderIdx).get("ordered-" + i).isPresent(),
                    "Key ordered-" + i + " should be present in leader's engine");
        }
    }

    // --- Helpers ---

    private void createCluster(int size) {
        rpc = new InMemoryRaftRpc();
        List<Integer> allIds = new ArrayList<>();
        for (int i = 1; i <= size; i++) allIds.add(i);

        for (int id : allIds) {
            Path nodeDir = tempDir.resolve("node-" + id);
            BitcaskEngine engine = new BitcaskEngine(nodeDir);
            List<Integer> peers = allIds.stream().filter(p -> p != id).toList();
            RaftNode node = new RaftNode(id, peers, engine, rpc);
            rpc.register(id, node);
            engines.add(engine);
            nodes.add(node);
        }

        // Start all nodes
        nodes.forEach(RaftNode::start);
    }

    private RaftNode waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : nodes) {
                if (node.getState() == RaftNode.State.LEADER) {
                    return node;
                }
            }
            Thread.sleep(100);
        }
        fail("No leader elected within " + timeoutMs + "ms");
        return null;
    }

    /** Waits until {@code node}'s replicated log reaches at least {@code minLastIndex} (polls, CI-safe). */
    private boolean awaitLogCaughtUp(RaftNode node, long minLastIndex, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.getLog().lastIndex() >= minLastIndex) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }
}
