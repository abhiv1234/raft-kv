package dev.raftkv.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class SnapshotTest {
    private static final RaftConfig SMALL = RaftConfig.fast().withSnapshotThreshold(20);

    @Test
    void logIsCompactedOnceThresholdIsReached() throws Exception {
        try (TestCluster c = new TestCluster(3, SMALL).startAll()) {
            c.awaitLeader();
            TestCluster.Client cl = c.client("a");
            for (int i = 0; i < 100; i++) cl.put("k" + i, "v" + i);
            c.awaitConverged(5_000);
            for (RaftNode n : c.running()) {
                assertTrue(n.status().snapshotIndex() > 0, n.id() + " should have snapshotted");
                assertTrue(n.retainedLogEntries() < 40, n.id() + " retained " + n.retainedLogEntries() + " entries");
            }
            assertEquals(100, c.machine("n1").size());
        }
    }

    @Test
    void laggingFollowerIsBroughtUpToDateWithInstallSnapshot() throws Exception {
        try (TestCluster c = new TestCluster(3, SMALL).startAll()) {
            RaftNode leader = c.awaitLeader();
            String lagger = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            TestCluster.Client cl = c.client("a");
            cl.put("seed", "1");
            c.crash(lagger);
            for (int i = 0; i < 150; i++) cl.put("k" + i, "v" + i); // far more than the threshold: the log is compacted past the lagger

            c.start(lagger);
            c.awaitConverged(15_000);
            assertEquals(151, c.machine(lagger).size());
            assertTrue(c.node(lagger).status().snapshotIndex() > 1,
                    "the follower can only have caught up via a snapshot, snapshotIndex=" + c.node(lagger).status().snapshotIndex());
        }
    }

    @Test
    void wholeClusterRestartsFromSnapshotPlusLog() throws Exception {
        try (TestCluster c = new TestCluster(3, SMALL).startAll()) {
            c.awaitLeader();
            TestCluster.Client cl = c.client("a");
            for (int i = 0; i < 70; i++) cl.put("k" + i, "v" + i);
            c.awaitConverged(5_000);
            for (String id : c.ids) c.crash(id);
            for (String id : c.ids) c.start(id);
            c.awaitLeader();
            assertTrue(cl.put("after", "restart").ok());
            c.awaitConverged(10_000);
            for (String id : c.ids) {
                assertEquals(71, c.machine(id).size());
                assertEquals("v69", c.machine(id).readLocal("k69").orElseThrow());
                assertTrue(c.node(id).status().snapshotIndex() > 0);
            }
        }
    }
}
