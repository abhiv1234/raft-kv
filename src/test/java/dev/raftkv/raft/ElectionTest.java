package dev.raftkv.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class ElectionTest {

    @Test
    void electsExactlyOneLeaderAndEveryoneAgreesWho() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            RaftNode leader = c.awaitLeader();
            TestCluster.waitUntil(() -> c.running().stream()
                    .allMatch(n -> leader.id().equals(n.status().leaderId())), 5_000, "all nodes to learn the leader");
            Thread.sleep(500); // leadership must be stable while heartbeats flow
            long leaders = c.running().stream().filter(RaftNode::isLeader).count();
            assertEquals(1, leaders);
            long term = leader.status().term();
            Thread.sleep(500);
            assertEquals(term, leader.status().term(), "a healthy cluster must not start new elections");
        }
    }

    @Test
    void singleNodeClusterElectsItselfAndCommits() throws Exception {
        try (TestCluster c = new TestCluster(1, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            assertTrue(c.client("a").put("k", "v").ok());
            assertEquals("v", c.machine("n1").readLocal("k").orElseThrow());
        }
    }

    @Test
    void electsNewLeaderWhenLeaderCrashes() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            RaftNode old = c.awaitLeader();
            long oldTerm = old.status().term();
            String oldId = old.id();
            c.crash(oldId);
            RaftNode next = c.awaitLeaderOtherThan(10_000, oldId);
            assertTrue(next.status().term() > oldTerm, "new leader must be in a higher term");
        }
    }

    @Test
    void crashedLeaderRejoinsAsFollower() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            String oldId = c.awaitLeader().id();
            c.crash(oldId);
            RaftNode next = c.awaitLeaderOtherThan(10_000, oldId);
            c.start(oldId);
            TestCluster.waitUntil(() -> c.node(oldId).status().role() == RaftNode.Role.FOLLOWER
                    && next.id().equals(c.node(oldId).status().leaderId()), 5_000, "old leader to follow the new one");
        }
    }

    @Test
    void noLeaderWithoutAMajority() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            List<String> ids = c.runningIds();
            c.crash(ids.get(0));
            c.crash(ids.get(1));
            Thread.sleep(1_500);
            RaftNode lone = c.node(ids.get(2));
            assertFalse(lone.isLeader(), "a minority must never elect a leader");
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> lone.propose(new byte[]{1}).get(1, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof NotLeaderException);
        }
    }

    /** Election safety: at most one leader per term, observed continuously through repeated failures. */
    @Test
    void atMostOneLeaderPerTermAcrossRepeatedFailovers() throws Exception {
        try (TestCluster c = new TestCluster(5, RaftConfig.fast()).startAll()) {
            Map<Long, Set<String>> leadersByTerm = new HashMap<>();
            AtomicBoolean stop = new AtomicBoolean();
            Thread observer = new Thread(() -> {
                while (!stop.get()) {
                    for (RaftNode n : c.running()) {
                        RaftNode.Status s = n.status();
                        if (s.role() == RaftNode.Role.LEADER) {
                            synchronized (leadersByTerm) {
                                leadersByTerm.computeIfAbsent(s.term(), t -> new HashSet<>()).add(s.id());
                            }
                        }
                    }
                    try { Thread.sleep(2); } catch (InterruptedException e) { return; }
                }
            });
            observer.start();
            for (int round = 0; round < 4; round++) {
                RaftNode l = c.awaitLeader();
                c.crash(l.id());
                c.awaitLeaderOtherThan(10_000, l.id());
                c.start(l.id());
            }
            Thread.sleep(300);
            stop.set(true);
            observer.join();
            synchronized (leadersByTerm) {
                assertTrue(leadersByTerm.size() >= 4, "expected several terms, saw " + leadersByTerm.keySet());
                leadersByTerm.forEach((term, ls) -> assertEquals(1, ls.size(), "term " + term + " had leaders " + ls));
            }
        }
    }
}
