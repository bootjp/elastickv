# Deliver the final Raft commit notification across peer removal

Status: Implemented.

## Problem

`drainReady` queues outbound messages before applying committed entries.
Applying a removal immediately closes the removed peer's connection and cancels
its dispatch workers. The final `MsgApp` carrying the removal's commit index can
therefore be cancelled before receipt. The leader has removed the peer, but the
removed replica retains the removal entry as uncommitted and cannot stop itself.
`TestRemoveServerUpdatesConfiguration` intermittently fails for this reason.

## Decisions

- Identify removal targets from committed `ConfChange` and `ConfChangeV2` entries
  in a persisted Ready batch. Preserve the final outgoing `MsgApp` to each target
  whose commit index covers that removal. Use only messages produced by Raft;
  do not synthesize entries, terms, or commit indexes.
- Deliver those messages outside the ordinary dispatch queues, on independent
  short-lived connections to the addresses captured before peer removal.
  Use the existing unary `Send` RPC: its response acknowledges receiver queue
  admission, whereas `SendStream.Send` alone acknowledges only local buffering.
- Keep delivery asynchronous so a slow or unreachable removed node cannot block
  Raft apply, elections, or subsequent membership operations. Bound delivery by
  the existing dispatch timeout and cancel/join it with the engine's dispatch
  workers on shutdown. Close the independent connection on every outcome.
- Keep ordinary peer removal and worker cancellation immediate. A delayed final
  notification must neither close nor mutate a replacement peer's connection.
- Notification remains best effort under partitions or when Raft emits no final
  commit-covering append for a lagging target. `RemoveServer` continues to report
  local committed application, not a distributed shutdown guarantee. No wire
  format, membership/quorum rule, or persistence ordering changes.

## Validation

Add a real two-node transport regression that gates the final commit notification
at the receiver until `RemoveServer` has returned and the leader has applied the
removal. Releasing the gate must update the removed node's configuration and stop
it. Run with stream sending enabled/disabled and both dispatcher layouts.
Also cover delivery cancellation, unreachable targets, and peer replacement.
Run the race-enabled raft package and original flaky test repeatedly, the Go
suite, lint, and the relevant Jepsen membership harness tests.

## Review

Accepted for this repair: the final notification uses an existing Raft-produced
message and existing RPC, changes no quorum or durable state, does not block the
apply loop, and owns its connection independently of peer-cache generations.
Implementation and regression tests follow this proposal in the same change.

The implementation was reviewed separately through the repository's five lenses:

1. Data loss: Ready persistence still precedes all sends. No WAL, snapshot,
   quorum, or committed-entry application rule changes.
2. Concurrency / distributed failures: no notification goroutine accesses
   RawNode or the peer maps. Messages and addresses are copied on the event
   loop; shutdown cancels and joins delivery. Replacement connections are
   independently owned. Network partitions retain bounded best-effort behavior.
3. Performance: ordinary Ready batches only scan entry types; they allocate no
   notification map or connection. New connections are limited to removal
   notifications, with the existing five-second dispatch timeout.
4. Data consistency: delivery uses Raft-generated messages, preserving their
   order per removed peer. No synthetic commit index, timestamp, or entry is
   introduced. Receiver admission uses the existing Raft handler.
5. Test coverage: the receiver-gated two-node regression failed before the fix
   and passed afterward in all four stream/dispatcher combinations. Additional
   tests cover V1/V2 decoding, malformed changes, cancellation, deadlines, and
   replacement-connection ownership.

Validation: the original failing test passed 150 repetitions at CPU parallelism
1, 2, and 4. Race-enabled tests passed for the entire etcd engine package tree,
and its lint passed. The main Jepsen harness passed 310 tests / 955 assertions.
The normal-entry Ready scan benchmark allocated zero bytes for batches of 1,
64, and 1024 entries (outgoing messages and network/storage work excluded).
The combined Go suite exhausted the adapter package's default ten-minute
budget while only the two full-length gRPC sequences were still running, with
no assertion failures. All other packages passed. Using the CI split, the
remaining adapter tests passed in 266 seconds and both full-length gRPC
sequences passed in 533 seconds with a fresh fifteen-minute budget.
Full-repository lint retains two existing complexity findings in `kv/fsm.go`
and `kv/shard_store_prepare_evidence_test.go`. The unchanged Redis Jepsen
submodule's placeholder test cannot load the nonexistent `jepsen.redis`
namespace; this is separate from the passing main Jepsen harness.
