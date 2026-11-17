package dev.raftkv.raft;

import dev.raftkv.storage.FileStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class PersistenceTest {

    @Test
    void committedDataSurvivesAFullClusterRestart() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast()).startAll()) {
            c.awaitLeader();
            TestCluster.Client cl = c.client("a");
            for (int i = 0; i < 25; i++) cl.put("k" + i, "v" + i);
            c.awaitConverged(5_000);
            long termBefore = c.leader().orElseThrow().status().term();

            for (String id : c.ids) c.crash(id);
            for (String id : c.ids) c.start(id);

            c.awaitLeader();
            assertTrue(c.leader().orElseThrow().status().term() > termBefore, "terms must never go backwards across restarts");
            assertEquals("v24", cl.get("k24").value());
            c.awaitConverged(10_000);
            for (String id : c.ids) assertEquals(25, c.machine(id).size());
        }
    }

    /**
     * Election safety depends on a node never voting twice in one term, even across a crash. If the vote
     * were not persisted, a restarted node could vote for a second candidate and two leaders could win
     * the same term.
     */
    @Test
    void aVoteSurvivesARestartSoANodeNeverVotesTwiceInOneTerm() throws Exception {
        try (TestCluster c = new TestCluster(3, RaftConfig.fast())) {
            c.start("n1"); // the other two nodes stay down, so nothing else changes n1's state
            Rpc.RequestVoteReply first = (Rpc.RequestVoteReply) c.node("n1").handle(new Rpc.RequestVote(5, "n2", 0, 0, false));
            assertTrue(first.granted(), "n1 should vote for n2 in term 5");

            c.restart("n1");

            assertEquals(5, c.node("n1").status().term(), "the term must survive a restart");
            Rpc.RequestVoteReply second = (Rpc.RequestVoteReply) c.node("n1").handle(new Rpc.RequestVote(5, "n3", 0, 0, false));
            assertFalse(second.granted(), "n1 already voted in term 5 and must not vote again after restarting");
            Rpc.RequestVoteReply again = (Rpc.RequestVoteReply) c.node("n1").handle(new Rpc.RequestVote(5, "n2", 0, 0, false));
            assertTrue(again.granted(), "repeating the same vote for the same candidate is fine");
        }
    }

    @Test
    void worksWithRealFilesIncludingRestartOfEveryNode() throws Exception {
        Path root = Files.createTempDirectory("raftkv-cluster");
        try (TestCluster c = new TestCluster(3, RaftConfig.fast().withSnapshotThreshold(15))
                .withStorage(id -> {
                    try {
                        return new FileStorage(root.resolve(id), false);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }).startAll()) {
            c.awaitLeader();
            TestCluster.Client cl = c.client("a");
            for (int i = 0; i < 60; i++) cl.put("k" + i, "v" + i);
            c.awaitConverged(5_000);

            for (String id : c.ids) c.crash(id);
            for (String id : c.ids) c.start(id);
            c.awaitLeader();
            assertEquals("v59", cl.get("k59").value());
            assertTrue(cl.put("more", "data").ok());
            c.awaitConverged(10_000);
            for (String id : c.ids) assertEquals(61, c.machine(id).size());
        } finally {
            try (var walk = Files.walk(root)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }
}
