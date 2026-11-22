# Overlap legacy leader WAL sync with follower replication

The synchronous etcd engine currently persists a Ready before dispatching its
messages. A VM150 diagnostic at 100/300/1000 offered requests/s confirms almost
one Raft entry per raw batch at 300 requests/s, with both Raft and FSM WAL sync
on all three replicas. The leader WAL wait precedes the follower WAL wait.

For an unprotected leader only, dispatch MsgApp from a Ready concurrently with
persisting that Ready's entries, as permitted by etcd/raft's Ready contract.
Restrict eligibility to an empty HardState, empty snapshot, normal new entries,
and no committed configuration changes. All earlier Readies have completed
persistence; no term/vote/commit update from this Ready can precede durability.
Clone messages on the owner before disk persistence, using existing bounded
per-peer queues. Preserve deferred messages, ordering within each queue, and
exactly-once dispatch. Protected/native admission and snapshot paths retain
existing ordering. Do not change request batching or sync options.

The owner still waits for local Save before processing follower acknowledgments,
applying entries, resolving reads/proposals, or Advance. The FSM still commits
with Sync. Failed Save halts the engine without success or applied publication;
followers having received an uncommitted entry does not prove client success.

Validation: block real engine Save and prove replication begins while local
Save is held, but local apply and client ACK cannot finish. Check empty/nonempty
HardState, follower responses, snapshots, protected admission, membership,
exactly-once dispatch, failure, slow follower, and restart. Preserve existing
snapshot/MVCC publication tests. Measure before/after with no profiles in ABBA
on exclusive VM150, original 100/300/500/1000 requests/s x90s, concurrency32,
read50%, Sync, TLS, unique writes, and all ACK reads through three ingresses.
Latency benefits and throughput are empirical; no fault-time bound is promised.
