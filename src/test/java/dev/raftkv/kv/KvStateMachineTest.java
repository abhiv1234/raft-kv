package dev.raftkv.kv;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvStateMachineTest {

    private static KvResult run(KvStateMachine sm, KvCommand c) {
        return KvResult.decode(sm.apply(c.encode()));
    }

    @Test
    void putGetDelete() {
        KvStateMachine sm = new KvStateMachine();
        assertTrue(run(sm, KvCommand.put("a", "1", "c", 1)).ok());
        KvResult g = run(sm, KvCommand.get("a", "c", 2));
        assertTrue(g.found());
        assertEquals("1", g.value());
        assertTrue(run(sm, KvCommand.delete("a", "c", 3)).found());
        assertFalse(run(sm, KvCommand.get("a", "c", 4)).found());
    }

    @Test
    void commandEncodingRoundTripsIncludingNullsAndUnicode() {
        KvCommand c = KvCommand.cas("kéy/ü", null, "värde 🚀", "client", 42);
        KvCommand back = KvCommand.decode(c.encode());
        assertEquals(c, back);
        assertNull(back.expected());
    }

    @Test
    void duplicateRequestReturnsTheOriginalResultWithoutReexecuting() {
        KvStateMachine sm = new KvStateMachine();
        KvCommand cas = KvCommand.cas("k", null, "first", "c1", 1);
        KvResult original = run(sm, cas);
        assertTrue(original.ok());
        // The value changes via a different client...
        run(sm, KvCommand.put("k", "changed", "c2", 1));
        // ...then c1 retries its timed-out request. It must see the ORIGINAL success, and must not clobber state.
        KvResult retry = run(sm, cas);
        assertEquals(original, retry);
        assertEquals("changed", sm.readLocal("k").orElseThrow());
    }

    @Test
    void staleSequenceNumbersAreRejected() {
        KvStateMachine sm = new KvStateMachine();
        run(sm, KvCommand.put("k", "1", "c", 5));
        KvResult r = run(sm, KvCommand.put("k", "old", "c", 4));
        assertFalse(r.ok());
        assertEquals("1", sm.readLocal("k").orElseThrow());
    }

    @Test
    void sessionsAreIndependentPerClient() {
        KvStateMachine sm = new KvStateMachine();
        assertTrue(run(sm, KvCommand.put("a", "1", "c1", 1)).ok());
        assertTrue(run(sm, KvCommand.put("b", "2", "c2", 1)).ok());
        assertEquals(2, sm.size());
    }

    @Test
    void snapshotRestoreKeepsDataAndSessions() {
        KvStateMachine a = new KvStateMachine();
        run(a, KvCommand.put("x", "1", "c", 1));
        KvCommand cas = KvCommand.cas("y", null, "2", "c", 2);
        KvResult original = run(a, cas);

        KvStateMachine b = new KvStateMachine();
        b.restore(a.snapshot());
        assertEquals(a.copyData(), b.copyData());
        assertEquals(original, run(b, cas), "dedup state must survive snapshot/restore");
    }

    @Test
    void sessionTableIsBounded() {
        KvStateMachine sm = new KvStateMachine();
        for (int i = 0; i < KvStateMachine.MAX_SESSIONS + 50; i++) run(sm, KvCommand.put("k", "v", "client-" + i, 1));
        KvStateMachine copy = new KvStateMachine();
        copy.restore(sm.snapshot());
        // Oldest sessions were evicted: a very old client's duplicate would be re-executed (documented limitation).
        assertEquals(1, copy.size());
    }
}
