# Raft asynchronous storage writes

Status: Proposed
Author: bootjp
Date: 2026-10-30

## Problem And Evidence

The native quorum migration acceptance in PR #1357 failed twice on its first
63.5 MiB moving-range negative write. Both CI logs show the old leader at
index 20 while the two other voters elect term 3 from index 19; index 20 is
then replaced and the caller correctly receives outcome-unknown. The test
expected a confirmed typed size rejection, so this remains a CI failure.
Three local race repetitions and one single-CPU profiled run passed; these
do not reinterpret either failed CI history as success.

Engine.drainReady synchronously persists Ready before dispatching messages.
Its owner goroutine also drives RawNode.Tick. WAL serialization, Save and
segment cut/sync therefore block heartbeat generation, not just its network
queue. A 66,584,576-byte entry exceeds the upstream 64,000,000-byte WAL
segment threshold. This proves an amplification path, not the duration of
the observed CI stall. The single-entry protobuf framing still fits the
64 MiB transport bound, so a base64 or single-message limit explanation is
not supported by the inspected code.

## Decisions

Use the existing go.etcd.io/raft/v3 v3.7.0 AsyncStorageWrites protocol.
Do not hand-roll heartbeats while an unadvanced Ready is outstanding, send
durability-dependent responses early, disable WAL sync, raise election
timeouts to hide the stall, reduce the 64 MiB RawKV contract, or weaken the
quorum acceptance's expected rejection/snapshot/restart properties.

- RawNode remains confined to one owner goroutine. Tick, Step, proposal,
  configuration and storage response processing run there. Async mode uses
  the upstream local storage message protocol, not Ready/Advance mixed mode.
- Route MsgStorageAppend and MsgStorageApply into distinct owned FIFO work
  streams. Never reorder or drop local storage work, including on queue
  pressure. Bound outstanding work by bytes as well as count; exhausted
  capacity backpressures proposal admission without blocking the owner from
  servicing heartbeat/Step/storage-completion traffic. Any deferred work is
  retained in order, not silently discarded.
- Append work persists entries, term/vote/commit state and snapshots through
  the existing WAL/encryption path. Deliver every associated response only
  after its required writes are durable. Disk/encryption failure is a halt,
  not a deterministic per-request conflict. No success or quorum progress
  may be synthesized from cancellation or an uncompleted Save.
- Preserve the existing on-disk formats, storage/envelope cutover, writer
  registration and sync configuration. This change adds no Raft wire opcode,
  timestamp issuer, or snapshot format.
- Apply work remains strictly ordered and preserves per-request verdicts,
  atomic batched entries and durable applied-index semantics. Do not apply
  or acknowledge a local entry before its required local WAL durability.
  Completion returns through the owner before client proposal/read receipts
  are resolved. A slow data FSM no longer owns the heartbeat event loop.
- Configuration entries need an explicit owner/worker handshake: RawNode
  configuration changes stay owner-only, while FSM/configuration metadata
  writes stay ordered with data apply. No worker calls RawNode directly.
  Do not block the owner waiting for a worker that itself needs an owner
  callback. Preserve membership and migration capability admission.
- Snapshot installation, restore and creation are ordered barriers across
  append and apply streams. Retain snapshotMu's lock ordering and protected
  received-SST/object lifetime rules. Restore cannot race an older apply;
  an append acknowledgment cannot outlive failed installation; checkpoint
  metadata and applied state must describe one completed prefix. Keep
  already-covered restore-skip behavior and external restore lineage rules.
- ReadIndex/lease results wait for their required applied index, not merely
  a queued apply. Leadership transfer, check-quorum and pending request
  cancellation retain existing semantics. Unknown outcomes stay unknown;
  this design does not add a blind raw-write retry.
- Shutdown stops admission, cancels work, joins both workers and drains
  owner completions before closing storage or snapshot resources. Storage
  failure and worker panic stop the engine exactly once; pending callers
  receive appropriate failure/unknown classification, never false success.

## Acceptance

1. Add a deterministic runtime-red test on the actual engine loop: establish
   a durable leader, gate only a later user-entry Save, and prove heartbeat
   progress while no durability-dependent acknowledgment, client success,
   FSM application or applied-index advance is allowed. Use channel barriers
   and controlled ticks, not arbitrary sleeps or a production test hook.
2. Release the Save and verify the real proposal/FSM result. Gate/fail Save,
   reorder attempted completions, saturate queues, cancel requests, transfer
   leadership and shut down with pending work. No local work may disappear
   or become a false commit/abort receipt.
3. Verify crash/reopen at append/apply/configuration boundaries, term/vote
   durability, batched partial verdicts versus operational rollback, native
   binary/SST snapshots and restore-skip/external-restore paths.
4. Exercise encrypted off/on WAL and storage, registration/rotation/cutover,
   migration ownership/configuration controls, lease/ReadIndex freshness,
   streaming/unary transport, learner catch-up and snapshot delivery.
5. Keep the original 63.5 MiB negative probe, retained-left value, actual
   three-voter/learner configuration changes, native snapshots and restart
   checks. Run the full root/KV/store/adapter/engine race suites, unfiltered
   lint, relevant Jepsen, and final-push code/security review and CI.

This is a liveness/durability prerequisite discovered while validating S3.
It is a separate PR, not an S3 semantic change or completion of all design
documents. Publish measured limits and remaining failure conditions rather
than claiming progress through an indefinitely unavailable disk or CPU.
