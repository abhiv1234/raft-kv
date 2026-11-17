package dev.raftkv.raft;

import dev.raftkv.kv.KvCommand;
import dev.raftkv.storage.MemoryStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Raft paper, Figure 8 / section 5.4.2: <b>a leader must not commit an entry from an earlier term
 * just because a majority of servers hold it</b>. Such an entry can still be overwritten by a later
 * leader, so committing it would break state-machine safety. It becomes committed only indirectly,
 * when an entry from the leader's own term (the no-op appended at election) commits after it.
 *
 * <p>Random fault injection almost never produces this interleaving, so the test builds it:
 * persisted states are pre-loaded as in the paper's scenario (c), and the simulated network holds
 * back the new leader's current-term no-op while still letting it replicate the old-term entry.
 */
@Timeout(120)
class Figure8Test {

    private static LogEntry entry(long term, String key, long seq) {
        return LogEntry.command(term, KvCommand.put(key, "v", "seed", seq).encode());
    }

    @Test
    void leaderDoesNotCommitAnOldTermEntryByCountingReplicas() throws Exception {
        try (TestCluster c = new TestCluster(5, RaftConfig.fast())) {
            // Pre-loaded history, all nodes in term 3:
            //   n1, n2 : [ (term1) , (term2) ]   <- the term-2 entry is the one at stake
            //   n3, n4 : [ (term1) ]
            //   n5     : never started (it would hold a conflicting term-3 entry in the paper's story)
            LogEntry e1 = entry(1, "a", 1), e2 = entry(2, "b", 2);
            for (String id : c.ids) ((MemoryStorage) c.storage(id)).saveTermAndVote(3, null);
            ((MemoryStorage) c.storage("n1")).append(1, List.of(e1, e2));
            ((MemoryStorage) c.storage("n2")).append(1, List.of(e1, e2));
            ((MemoryStorage) c.storage("n3")).append(1, List.of(e1));
            ((MemoryStorage) c.storage("n4")).append(1, List.of(e1));

            // Hold back the new leader's no-op: strip it from every AppendEntries, but let everything else through.
            c.net.setInterceptor((target, rpc) -> {
                if (rpc instanceof Rpc.AppendEntries ae && ae.entries().stream().anyMatch(en -> en.type() == LogEntry.Type.NOOP)) {
                    List<LogEntry> kept = new ArrayList<>();
                    for (LogEntry en : ae.entries()) {
                        if (en.type() == LogEntry.Type.NOOP) break;
                        kept.add(en);
                    }
                    return new Rpc.AppendEntries(ae.term(), ae.leaderId(), ae.prevLogIndex(), ae.prevLogTerm(), kept, ae.leaderCommit());
                }
                return rpc;
            });
            for (String id : List.of("n1", "n2", "n3", "n4")) c.start(id);

            // Only n1 or n2 can win (their logs are the most up to date). It starts term 4 and tries to bring everyone up.
            RaftNode leader = c.awaitLeader();
            assertTrue(leader.id().equals("n1") || leader.id().equals("n2"), "unexpected leader " + leader.id());
            assertEquals(4, leader.status().term());

            // The term-2 entry reaches a MAJORITY (n1, n2, n3, n4 all hold index 2)...
            TestCluster.waitUntil(() -> List.of("n1", "n2", "n3", "n4").stream()
                    .allMatch(id -> c.node(id).status().lastLogIndex() >= 2), 5_000, "the old-term entry to reach a majority");

            // ...yet it must NOT be committed, however long we wait: only a term-4 entry can commit it.
            for (int i = 0; i < 12; i++) {
                Thread.sleep(100);
                for (String id : List.of("n1", "n2", "n3", "n4")) {
                    assertEquals(0, c.node(id).status().commitIndex(),
                            id + " committed an old-term entry on a replica count alone (Figure 8 violation)");
                }
            }

            // Release the no-op: now the term-4 entry commits, and with it everything before it.
            c.net.setInterceptor(null);
            TestCluster.waitUntil(() -> List.of("n1", "n2", "n3", "n4").stream()
                    .allMatch(id -> c.node(id).status().commitIndex() >= 3), 5_000, "the no-op to commit everything before it");
            assertEquals("v", c.machine(leader.id()).readLocal("b").orElseThrow());
        }
    }
}
