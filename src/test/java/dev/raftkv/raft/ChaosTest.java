package dev.raftkv.raft;

import dev.raftkv.kv.KvCommand;
import dev.raftkv.kv.KvResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomised fault injection against a 5-node cluster: nodes crash and restart, the network
 * partitions and heals, and messages are dropped, all while clients keep writing. Afterwards the
 * test checks the invariants Raft promises:
 * <ul>
 *   <li><b>Durability:</b> an acknowledged write is never lost.</li>
 *   <li><b>Atomicity / exactly-once:</b> a CAS-based shared counter equals the number of increments
 *       clients were told succeeded (no lost updates, no double application on retry).</li>
 *   <li><b>State-machine safety:</b> after healing, every replica holds identical state.</li>
 * </ul>
 */
@Timeout(120)
class ChaosTest {
    private static final int REGISTER_CLIENTS = 3;
    private static final int COUNTER_CLIENTS = 3;
    private static final long RUN_MS = Long.getLong("chaos.ms", 8_000);

    @Test
    void invariantsHoldUnderRandomFaults() throws Exception {
        long seed = Long.getLong("chaos.seed", System.nanoTime());
        System.out.println("  [chaos] seed=" + seed + " (rerun with -Dchaos.seed=" + seed + ")");
        Random rnd = new Random(seed);

        try (TestCluster c = new TestCluster(5, RaftConfig.fast().withSnapshotThreshold(50)).startAll()) {
            c.awaitLeader();
            AtomicBoolean stop = new AtomicBoolean();
            List<Thread> threads = new ArrayList<>();

            // Register clients: each owns one key and writes 1, 2, 3, ... (value == its own sequence number).
            long[] acked = new long[REGISTER_CLIENTS];
            long[] attempted = new long[REGISTER_CLIENTS];
            for (int i = 0; i < REGISTER_CLIENTS; i++) {
                int idx = i;
                TestCluster.Client cl = c.client("reg-" + i);
                threads.add(new Thread(() -> {
                    long n = 0;
                    while (!stop.get()) {
                        n++;
                        attempted[idx] = n;
                        KvCommand cmd = KvCommand.put("reg-" + idx, Long.toString(n), cl.clientId(), n);
                        KvResult r = cl.tryExecute(cmd, 4_000);
                        if (r != null && r.ok()) acked[idx] = n;
                        else if (stop.get()) return;
                        else n--; // not acknowledged: retry the SAME logical op with the SAME seq
                    }
                }));
            }

            // Counter clients: read-modify-write with CAS. Each successful CAS must add exactly one.
            AtomicLong successfulIncrements = new AtomicLong();
            AtomicInteger counterAttempts = new AtomicInteger();
            for (int i = 0; i < COUNTER_CLIENTS; i++) {
                TestCluster.Client cl = c.client("ctr-" + i);
                threads.add(new Thread(() -> {
                    long seq = 0;
                    while (!stop.get()) {
                        KvResult cur = cl.tryExecute(KvCommand.get("counter", cl.clientId(), ++seq), 4_000);
                        if (cur == null) { seq--; continue; }
                        String expected = cur.found() ? cur.value() : null;
                        long next = expected == null ? 1 : Long.parseLong(expected) + 1;
                        KvCommand cas = KvCommand.cas("counter", expected, Long.toString(next), cl.clientId(), ++seq);
                        // The outcome of a CAS can be ambiguous (timeout, leader change). Keep re-sending the SAME
                        // (clientId, seq): the state machine de-duplicates, so we eventually learn the one true result.
                        // (After the run the cluster is healed, so this always terminates.)
                        KvResult r;
                        do { r = cl.tryExecute(cas, 4_000); } while (r == null);
                        counterAttempts.incrementAndGet();
                        if (r.ok()) successfulIncrements.incrementAndGet();
                    }
                }));
            }

            // Fault injector.
            Thread chaos = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        Thread.sleep(150 + rnd.nextInt(350));
                        List<String> up = c.runningIds();
                        List<String> down = new ArrayList<>(c.ids);
                        down.removeAll(up);
                        switch (rnd.nextInt(6)) {
                            case 0 -> { if (up.size() > 3) c.crash(up.get(rnd.nextInt(up.size()))); }
                            case 1 -> { if (!down.isEmpty()) c.start(down.get(rnd.nextInt(down.size()))); }
                            case 2 -> {
                                List<String> shuffled = new ArrayList<>(c.ids);
                                Collections.shuffle(shuffled, rnd);
                                c.net.partition(shuffled.subList(0, 2), shuffled.subList(2, 5));
                            }
                            case 3 -> c.net.isolate(c.ids.get(rnd.nextInt(c.ids.size())));
                            case 4 -> c.net.heal();
                            default -> c.net.setDropRate(rnd.nextBoolean() ? 0.0 : 0.08);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            threads.forEach(t -> { t.setDaemon(true); t.start(); });
            chaos.setDaemon(true);
            chaos.start();
            Thread.sleep(RUN_MS);

            // Heal everything, bring every node back, let the system settle.
            stop.set(true);
            chaos.join();
            c.net.heal();
            c.net.setDropRate(0);
            for (String id : c.ids) c.start(id);
            for (Thread t : threads) {
                t.join(30_000);
                assertTrue(!t.isAlive(), "a client thread never finished after the cluster was healed");
            }
            c.awaitLeader();
            c.awaitConverged(30_000);

            // --- Verify ---------------------------------------------------------------------------
            TestCluster.Client verifier = c.client("verifier");
            long totalAcked = 0;
            for (int i = 0; i < REGISTER_CLIENTS; i++) {
                KvResult r = verifier.get("reg-" + i);
                long actual = r.found() ? Long.parseLong(r.value()) : 0;
                totalAcked += acked[i];
                assertTrue(actual >= acked[i],
                        "reg-" + i + ": acknowledged write " + acked[i] + " was lost; final value " + actual);
                assertTrue(actual <= attempted[i],
                        "reg-" + i + ": final value " + actual + " was never even attempted (max " + attempted[i] + ")");
            }

            KvResult counter = verifier.get("counter");
            long finalCounter = counter.found() ? Long.parseLong(counter.value()) : 0;
            // Exactly-once: every increment a client was told succeeded is in the counter, and nothing else is.
            // A shortfall means a lost update; an excess means a retry was applied twice.
            assertEquals(successfulIncrements.get(), finalCounter,
                    "counter=" + finalCounter + " but " + successfulIncrements.get() + " increments were acknowledged");

            // All replicas identical (awaitConverged already waited for this; assert explicitly).
            for (String id : c.ids) assertEquals(c.machine("n1").copyData(), c.machine(id).copyData(), id + " diverged");

            System.out.println("  [chaos] acknowledged register writes=" + totalAcked
                    + ", counter=" + finalCounter + " (acknowledged increments=" + successfulIncrements.get() + ")");
            assertNotNull(c.leader().orElse(null));
            assertTrue(totalAcked + successfulIncrements.get() > 20, "the cluster made too little progress to be a meaningful test");
        }
    }
}
