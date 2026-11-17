# Presenting and defending this project

A project like this earns its value in the conversation, not in the repo. This guide gives you a structure for that conversation, the questions that tend to come up, and answers grounded in what this code actually does. Verify every answer against the code before you rely on it; interviewers go deep quickly.

## Make it yours first

Before you put this on your resume, make sure you can explain every line of `RaftNode`, `FileStorage` and `KvStateMachine` without looking. A few things that will build that understanding quickly:

1. Read the Raft paper's Figure 2 next to `RaftNode.java` and tick off each rule.
2. Break things on purpose and see which tests notice. Good candidates: remove the `currentTerm` check in `advanceCommit` (caught by `Figure8Test`), stop persisting `votedFor` (caught by `PersistenceTest`), remove the no-op in `becomeLeader`, or change how `onAppendEntries` handles a conflict. If a mutation survives, that is a gap in the tests, and writing the test that kills it is a strong story to tell.
3. Extend it with something of your own, such as ReadIndex reads or a linearizability check, and write down what you learned. A real story about a bug *you* hit is worth more than any prepared answer.
4. If asked how the project was built, be straightforward about the tools you used. What matters is that you understand the design and can reason about it.

## 30-second pitch

> "I built a linearizable key-value store on Raft in Java, from scratch. It has leader election with pre-vote and check-quorum, log replication, a crash-safe write-ahead log, snapshots with InstallSnapshot, and client sessions for exactly-once retries. I tested it with a simulated network where a chaos test crashes nodes, partitions the network and drops messages while clients write, then checks that no acknowledged write is lost, a CAS counter is exact, and all replicas converge."

## A 5-minute walkthrough (draw these)

1. **The picture**: three nodes, each with `RaftNode`, log, state machine and storage. Client to leader; leader to followers.
2. **A write's life**: `PUT` reaches the leader, appended to its log (fsync), replicated via `AppendEntries`, **committed when a majority has it**, applied to the state machine, future completed, client gets `200`.
3. **Leader death**: followers' election timers fire, pre-vote, then a real election. The vote rule guarantees the new leader has every committed entry.
4. **The subtle bit**: a new leader may only count replicas for *its own term's* entries, so it appends a no-op to commit the older ones (the Figure 8 problem).
5. **The client's problem**: a timeout means unknown outcome, so each request carries `(clientId, seq)` and the state machine de-duplicates.
6. **How you know it works**: the fault-injection tests and the invariants they check.

## Questions to expect, with answers

**Why Raft rather than Paxos?** Raft decomposes consensus into leader election, log replication and safety, with a strong leader and a log that only grows, which makes it far easier to implement correctly and to test. Multi-Paxos gives equivalent guarantees.

**What guarantees does a client get?** Writes and `GET`s are ordered through the replicated log, so operations are linearizable. Combined with sessions, retries have exactly-once effect. `?stale=true` reads are explicitly not linearizable.

**What if two nodes both think they are leader?** It can briefly happen (an old leader that has not noticed a new term), but never in the *same* term, and the old one cannot commit anything, because commits need a majority and a majority has moved to the higher term. Check-quorum makes the old leader step down by itself. See `PartitionTest.minorityCannotCommitWhileMajorityKeepsWorking`.

**Why can't a leader commit an old-term entry by counting replicas?** Figure 8 in the paper: an entry can be on a majority and still be overwritten by a later leader that never saw it. Counting replicas is only safe for entries of the leader's own term. Committing one of those commits everything before it. Hence the no-op at the start of each term.

**What is pre-vote and why did you add it?** A partitioned node keeps timing out and raising its term. On rejoining, its higher term forces the healthy leader to step down. Pre-vote makes it first ask "would you vote for me?" without changing anyone's term, and peers say no if they recently heard from a leader. My test runs both variants: with pre-vote the term is unchanged after the rejoin; without it, the term jumps and a new election happens.

**What happens if a node crashes while writing to disk?** Every WAL record has a length and CRC. A torn or corrupt tail is detected at recovery and discarded; that is safe because we never acknowledge before fsync. Meta and snapshot files are replaced atomically via rename. There is a test for a torn write and one for a crash between writing the snapshot and compacting the log.

**Why fsync before replying?** Raft's safety argument assumes a node that acknowledged an entry (or a vote) still has it after a restart. If a vote were lost, a node could vote twice in one term and break election safety. If an entry were lost, a "committed" entry might exist on fewer than a majority.

**How does a follower catch up if the leader already compacted the log?** The leader sees `nextIndex <= snapshotIndex` and sends `InstallSnapshot`. The follower replaces its state machine with the snapshot and continues from there. `SnapshotTest.laggingFollowerIsBroughtUpToDateWithInstallSnapshot` covers it.

**How do you avoid applying a retried request twice?** Each command has `(clientId, seq)`. The state machine keeps the last `(seq, result)` per client, and a duplicate returns the stored result without executing. The session table is part of the snapshot so this survives compaction. The CAS counter in the chaos test is the proof: under crashes and retries it equals the number of acknowledged increments exactly.

**Your reads go through the log. Isn't that slow?** Yes, deliberately: simplest correct choice. ReadIndex avoids appending: the leader notes its commit index, confirms it is still leader with a heartbeat round, waits until it has applied that index, then reads locally. Lease reads go further but depend on clock drift bounds. That is the first optimisation on my roadmap.

**How would you add or remove nodes?** Not implemented; membership is static. The paper's options: joint consensus (a two-phase configuration with overlapping majorities) or the simpler single-server-at-a-time change from the thesis. The key danger is two disjoint majorities existing during a switch.

**What are the performance bottlenecks?** One global lock; fsync under the lock with no group commit; one TCP connection per RPC; Java serialization; snapshot pause under the lock; every read replicated. I did not optimise because the goal was correctness and clarity. I'd start with group commit and ReadIndex.

**How do you know it's correct?** Targeted tests per property, plus a randomised chaos test checking durability, exactness of a CAS counter and replica convergence, plus mutation checks (I injected bugs such as committing without a majority and disabling de-duplication, and confirmed tests failed). **Be honest about the limit:** it is not a full linearizability check. Random chaos rarely hits very specific interleavings, so for the subtle rules I wrote deterministic tests (`Figure8Test` builds the paper's scenario by pre-loading persisted state and holding back the leader's no-op; `PersistenceTest` checks a vote survives a restart). Initially my chaos tests did *not* catch those two bugs, and I only found that by deliberately injecting them, which is exactly why I added the targeted tests. A history checker (Porcupine/Knossos) is the next step.

**Which invariants does the chaos test check?** Every acknowledged register write is present with a value at least as new as the last acknowledgement; the CAS counter equals the number of acknowledged successful increments; all replicas end with identical data.

**What does the leader do when it can't reach a majority?** Check-quorum: after an election timeout without contact from a majority it steps down, and pending client futures fail with `NotLeaderException`.

**What happens on a network partition during a write?** The side with a majority continues; the minority side cannot commit. If the old leader accepted a write, it is discarded when the partition heals (`ReplicationTest.followerRejectsDivergentHistoryAndAdoptsLeadersLog`).

**Why a single lock?** Raft's invariants cut across term, vote, log and commit index, and one lock makes the reasoning simple. The rule that keeps it safe is no network I/O under the lock. The cost is throughput, and I'd shard or pipeline only after profiling.

**Why are `noop` entries applied?** They aren't applied to the state machine (`NOOP` entries are skipped), but they occupy a log index, which is what lets the new leader commit older entries.

**What would you do differently in production?** Group commit, ReadIndex, async chunked snapshots, protobuf plus mTLS, membership changes, session expiry, metrics and tracing, and a Jepsen-style test harness.

## Gaps worth closing (and then talking about)

These are honest weaknesses an experienced interviewer may find. Closing one and describing how you did it makes a strong story:

- **Only a few deterministic interleaving tests.** `Figure8Test` shows the technique (pre-load persisted state, use `InMemoryNetwork.setInterceptor` to control exactly which messages get through). Write more in that style: a leader crashing between replicating and committing; a follower receiving `AppendEntries` out of order; `InstallSnapshot` racing with new entries.
- **No linearizability checker** over recorded histories.
- **No benchmarks.** Add a small harness, publish your own numbers, and be ready to explain what limits them.
- **Pre-vote and check-quorum interplay** with leadership transfer (not implemented) is a good design discussion.
