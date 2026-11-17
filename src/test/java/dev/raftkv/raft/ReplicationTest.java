package dev.raftkv.raft;

import dev.raftkv.kv.KvCommand;
import dev.raftkv.kv.KvResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class ReplicationTest {

    @Test
    void writesAreReplicatedToEveryNode() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            TestCluster.Client client = c.client("a");
            assertTrue(client.put("x", "1").ok());
            KvResult r = client.get("x");
            assertTrue(r.found());
            assertEquals("1", r.value());
            c.awaitConverged(5_000);
            for (String id : c.ids) assertEquals("1", c.machine(id).readLocal("x").orElseThrow());
        }
    }

    @Test
    void deleteAndCompareAndSwap() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            TestCluster.Client cl = c.client("a");
            assertTrue(cl.cas("k", null, "v1").ok(), "CAS with null expected creates the key");
            assertFalse(cl.cas("k", null, "v2").ok(), "CAS must fail if the key exists");
            assertTrue(cl.cas("k", "v1", "v2").ok());
            assertEquals("v2", cl.get("k").value());
            assertTrue(cl.delete("k").found());
            assertFalse(cl.get("k").found());
        }
    }

    @Test
    void manySequentialWritesKeepOrder() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            TestCluster.Client cl = c.client("a");
            for (int i = 0; i < 200; i++) cl.put("k" + (i % 10), "v" + i);
            c.awaitConverged(5_000);
            for (int k = 0; k < 10; k++) {
                assertEquals("v" + (190 + k), c.machine("n1").readLocal("k" + k).orElseThrow());
            }
        }
    }

    @Test
    void concurrentProposalsAreAllCommitted() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            RaftNode leader = c.awaitLeader();
            CompletableFuture<?>[] fs = new CompletableFuture<?>[300];
            for (int i = 0; i < fs.length; i++) {
                fs[i] = leader.propose(KvCommand.put("k" + i, "v" + i, "bulk", i + 1).encode());
            }
            CompletableFuture.allOf(fs).get(10, TimeUnit.SECONDS);
            c.awaitConverged(5_000);
            assertEquals(300, c.machine("n2").size());
        }
    }

    @Test
    void restartedFollowerCatchesUp() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            RaftNode leader = c.awaitLeader();
            String follower = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            TestCluster.Client cl = c.client("a");
            cl.put("before", "1");
            c.crash(follower);
            for (int i = 0; i < 50; i++) cl.put("while-down-" + i, "x");
            c.start(follower);
            c.awaitConverged(10_000);
            assertEquals(51, c.machine(follower).size());
        }
    }

    @Test
    void followerRejectsDivergentHistoryAndAdoptsLeadersLog() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            RaftNode oldLeader = c.awaitLeader();
            String oldId = oldLeader.id();
            TestCluster.Client cl = c.client("a");
            assertTrue(cl.put("committed-1", "a").ok());

            // Cut the leader off, then hand it a write it can never commit.
            c.net.isolate(oldId);
            CompletableFuture<byte[]> doomed = oldLeader.propose(KvCommand.put("doomed", "x", "z", 1).encode());

            // The majority elects a new leader and makes progress without it.
            RaftNode newLeader = c.awaitLeaderOtherThan(10_000, oldId);
            assertTrue(c.client("b").put("committed-2", "b").ok());
            assertFalse(doomed.isDone() && !doomed.isCompletedExceptionally(), "a minority write must never commit");

            // Heal: the old leader must discard its divergent entry and adopt the new leader's log.
            c.net.heal();
            c.awaitConverged(10_000);
            assertFalse(c.machine(oldId).readLocal("doomed").isPresent(), "uncommitted entry must be gone everywhere");
            assertEquals("b", c.machine(oldId).readLocal("committed-2").orElseThrow());
            assertTrue(doomed.isCompletedExceptionally(), "the client of the doomed write must be told it failed");
            assertTrue(c.node(oldId).status().term() >= newLeader.status().term());
        }
    }
}
