package dev.raftkv.raft;

import dev.raftkv.kv.KvCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class PartitionTest {

    @Test
    void minorityCannotCommitWhileMajorityKeepsWorking() throws Exception {
        try (TestCluster c = new TestCluster(5, RaftConfig.fast()).startAll()) {
            RaftNode leader = c.awaitLeader();
            String buddy = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            List<String> minority = List.of(leader.id(), buddy);
            List<String> majority = c.ids.stream().filter(id -> !minority.contains(id)).toList();
            assertTrue(c.client("a").put("base", "1").ok());
            c.awaitConverged(5_000);

            c.net.partition(minority, majority);
            CompletableFuture<byte[]> stuck = leader.propose(KvCommand.put("minority-write", "x", "m", 1).encode());

            // The majority side elects its own leader and commits.
            TestCluster.waitUntil(() -> majority.stream().anyMatch(id -> c.node(id).isLeader()), 10_000, "majority leader");
            RaftNode majLeader = majority.stream().map(c::node).filter(RaftNode::isLeader).findFirst().orElseThrow();
            assertTrue(majLeader.propose(KvCommand.put("majority-write", "y", "j", 1).encode())
                    .get(5, java.util.concurrent.TimeUnit.SECONDS) != null);
            assertFalse(stuck.isDone() && !stuck.isCompletedExceptionally(), "minority write must not be acknowledged");

            // The old leader steps down by itself (check-quorum) instead of lingering as a zombie.
            TestCluster.waitUntil(() -> !leader.isLeader(), 5_000, "minority leader to step down");

            c.net.heal();
            c.awaitConverged(10_000);
            for (String id : c.ids) {
                assertEquals("y", c.machine(id).readLocal("majority-write").orElseThrow());
                assertFalse(c.machine(id).readLocal("minority-write").isPresent());
                assertEquals("1", c.machine(id).readLocal("base").orElseThrow());
            }
        }
    }

    @Test
    void preVotePreventsAPartitionedNodeFromDisruptingTheCluster() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast().withPreVote(true)).startAll()) {
            RaftNode leader = c.awaitLeader();
            long term = leader.status().term();
            String victim = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();

            c.net.isolate(victim);
            Thread.sleep(2_000); // many election timeouts in isolation
            assertEquals(term, c.node(victim).status().term(), "pre-vote must stop the isolated node inflating its term");

            c.net.heal();
            Thread.sleep(800);
            assertTrue(leader.isLeader(), "the leader must survive the rejoin");
            assertEquals(term, leader.status().term(), "no new election should have happened");
        }
    }

    /** The control experiment: without pre-vote the rejoining node does force a needless election. */
    @Test
    void withoutPreVoteARejoiningNodeDisruptsTheLeader() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast().withPreVote(false)).startAll()) {
            RaftNode leader = c.awaitLeader();
            long term = leader.status().term();
            String victim = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();

            c.net.isolate(victim);
            Thread.sleep(2_000);
            assertTrue(c.node(victim).status().term() > term + 2, "the isolated node keeps bumping its term");

            c.net.heal();
            TestCluster.waitUntil(() -> c.leader().map(n -> n.status().term() > term).orElse(false), 10_000,
                    "the higher term to force a new election");
            c.awaitLeader();
        }
    }

    @Test
    void survivesMessageLossAndLatency() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            c.net.setDropRate(0.15);
            c.net.setLatency(1, 15);
            TestCluster.Client cl = c.client("a");
            for (int i = 0; i < 40; i++) assertTrue(cl.put("k" + i, "v" + i).ok());
            c.net.setDropRate(0);
            c.net.setLatency(0, 0);
            c.awaitConverged(10_000);
            assertEquals(40, c.machine("n1").size());
        }
    }
}
