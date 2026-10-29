# Raft asynchronous storage writes

Status: Proposed; revised after durability and lifecycle review
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

### Storage accounting and overload

Account retained payload/envelope bytes and work count across ingress,
RawNode-unstable data, queued/executing/deferred append work, and outstanding
apply work; moving a reference between stages does not release its reservation.
This is not a claim to bound the complete Go heap or the durable retained log.
Outbound deep clones also require a separate bounded byte reservation.

Reserve bulk capacity before accepting a local proposal or bulk network Step.
Local admission can return backpressure before proposal; network data messages
can be refused/dropped with normal Raft transport feedback before Step, because
the sender retries. Already emitted local storage messages cannot be dropped.
Keep independent ingress capacity for heartbeat, vote and completion traffic;
do not put them behind a full bulk channel. Configure MaxUncommittedEntriesSize,
MaxInflightBytes/Msgs and MaxCommittedSizePerReady, but do not mislabel the last
as an append bound. Release payload reservations only once their retained
storage/RawNode ownership ends, with separate apply accounting thereafter.

Allow at most one documented maximum-sized Ready overshoot when ownership
must transfer atomically; account that debt and close bulk admission until it
clears. A single valid near-64-MiB entry must fit without reducing RawKV's
contract. Reserve separately bounded control-work capacity. If non-droppable
control work exhausts that reserve during an indefinitely unavailable disk,
halt explicitly with a resource error instead of unbounded retention, blocking
the owner forever, dropping local work, or acknowledging it. This exceptional
halt is not the normal backpressure path and makes no liveness promise through
an indefinitely unavailable resource. Tests account every ownership transition,
overshoot, rejection and release, including follower ingress and snapshots.

### Durable prefix and response routing

Only the append/disk actor calls WAL Save/Sync, SaveSnap/Release, and persistent
peer/configuration snapshot operations. Snapshot capture/restore workers send
immutable disk tasks to it; they do not concurrently access the WAL writer.
Disk tasks are FIFO and never wait for an owner callback while holding a lock.

For every append message with responses, explicitly Sync the required written
prefix before releasing any response, even if that message is commit-only or
response-only. An empty message is not permission to skip a flush of prior
unsynced work. Adjacent tasks may batch writes and one flush, but every response
retains its original ordering and durable-prefix dependency. Preserve snapshot
file and directory durability in addition to WAL Sync. Save returning nil alone
is not a universal durability receipt; only a documented/tested durable Save
contract may later eliminate a redundant flush.

The actor returns opaque responses and its completed prefix to the owner.
The owner Steps self-directed responses into RawNode and dispatches remote
responses through transport; it does not discard them as ordinary local network
messages. A stale-term response may be ignored by RawNode, but its ownership
and accounting must still be retired correctly. Async mode never calls Advance.

### Configuration and snapshot phases

| Phase | Owner | Wait rule |
|---|---|---|
| Ordinary append | Disk actor | Save and required Sync, then completion; no FSM/owner callback |
| Ordinary committed data | Apply actor | FIFO, only issued after local stability; completion goes to owner |
| Configuration front | Apply actor then owner | Earlier applies complete; send owner request, with no locks held |
| Configuration transition | Owner | ApplyConfChange only; return immutable state/receipts, no disk or worker join |
| Configuration durability | Apply actor then disk actor | Capture prefix-coherent metadata, submit disk task, await buffered receipt |
| Configuration publication | Owner | Publish peer/membership/applied state only after ordered durable completion |
| Received snapshot disk | Disk actor | Persist owned snapshot resources/WAL; then publish disk receipt |
| Received snapshot restore | Apply actor | FIFO restore barrier after older apply; await disk receipt without locks |
| Received snapshot acknowledgment | Owner | Disk and restore receipts both required; later append responses stay ordered |
| Local checkpoint | Apply actor then disk actor | Capture one completed FSM/configuration prefix, persist it as a disk task |

The owner never holds snapshotMu while waiting for a worker. The disk actor
never calls the FSM, waits for restore, or requests an owner callback. The
apply actor can wait for disk or a short owner transition only with no storage
locks held. The owner continues processing those transitions/completions while
both actors run. This acyclic ownership order replaces cross-worker synchronous
calls into existing all-in-one persistence helpers.

Snapshot restore fences subsequent applies; disk work may continue in FIFO,
but the owner withholds dependent acknowledgments/publication until restore
completes. Retain received files/object pins until every dependent task and
durability receipt completes, including crash-safe snapshot reference capture.
Snapshot checkpoints use the configuration belonging to their applied prefix,
not a newer RawNode configuration. Encryption cutover, registration and key
state transitions participate in the same apply order; disk tasks receive the
correct immutable envelope/key-state inputs and never infer authority from a
later asynchronously published state.

### Quiescing shutdown

Shutdown is an owner-loop state, not an owner-blocking WaitGroup.Wait. Stop
new proposal/bulk admission and external callbacks, cancel queued cancellable
work, and keep pumping storage completions and configuration callbacks until
the worker-done signals and all outstanding ownership counters reach zero.
Only then join workers, release snapshot resources and close storage. An
already executing non-cancellable Save must actually return; a deadline is
not a synthesized completion. Keep diagnostics while waiting rather than
closing resources underneath it. Tests release their held Save in cleanup.

### Runtime and recovery gates

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
