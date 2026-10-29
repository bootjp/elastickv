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

## Local Implementation Checkpoint

The reviewed design is accepted; the implementation is not shipped or complete.
The first implementation checkpoint provides these independently tested pieces:

- Overflow-safe byte/item reservations with idempotent retirement, including
  zero-byte control work and concurrent acquisition/release.
- A FIFO worker with nonblocking admission. Reservations remain charged through
  execution and completed-but-not-retired receipts. A rejected submission keeps
  caller ownership; a failed operation fences later operations and returns each
  accepted token exactly once. Quiescing does not cancel an executing Save or
  discard completions when the owner must resume pumping them.
- Disk-only append conversion preserving entries, snapshots and full HardState.
  Every response-bearing append explicitly Syncs, including a response-only
  flush after previous unsynced work. The completion retains the original local
  message and opaque responses; it does not itself dispatch responses or claim
  that a snapshot has been restored.

Local verification on Darwin/arm64:

```sh
go test -race ./internal/raftengine/etcd \
  -run '^Test(AsyncStorageAppend|StorageWork)' -count=20
golangci-lint run ./... --timeout=5m
go test ./internal/raftengine/etcd -run '^$' \
  -bench '^BenchmarkStorageWorkerRoundTrip$' -benchmem -count=3
```

The focused race run passed in 7.150s. Native WAL clean-reopen cases preserve
term, cleared vote, commit, entry payload and inline snapshot contents. Separate
Save/Sync gates prove no disk completion while either required operation is held.
Lifecycle tests cover full completion queues, concurrent shutdown/admission/error,
independent bulk/control budgets, a successful prefix followed by error/panic, and
retirement only after owner consumption. A clean reopen is not a crash/power-loss
test. The actor-only benchmark measured 318.3-321.9 ns/op, 24 B/op and one
allocation/op on an Apple M1 Max; it measures neither WAL nor FSM throughput.

Five review passes for this checkpoint:

1. Data loss: disk failures preserve error identity and prevent subsequent work;
   neither quiesce nor cancellation fabricates success or closes storage early.
   Snapshot resource installation/recovery is still a separate integration gate.
2. Concurrency/distributed failures: worker/budget race cases pass; only immutable
   task inputs cross the boundary. No worker calls RawNode. Engine leadership,
   membership and shutdown callback integration is not yet validated.
3. Performance: queue/byte accounting is bounded and rejected reservations are
   rolled back. Actor overhead is measured separately from payload and disk work.
4. Consistency: complete term/vote/commit and opaque response order are retained.
   No timestamp policy or active FSM/apply behavior has changed.
5. Coverage: focused lifecycle/append/budget tests pass, including real WAL codecs.
   The actual Engine regression still detects stopped heartbeats during held
   user-entry Save; releasing Save still completes the real proposal. This is
   retained failing evidence, not a passed engine or full-suite acceptance.

The engine still uses synchronous Ready/Advance. These pieces must be integrated
with the owner loop, an ordered apply actor, configuration/snapshot handshakes,
end-to-end ingress/unstable/outbound accounting, control reserves, and pumping
shutdown before enabling AsyncStorageWrites. The runtime, encrypted recovery,
native quorum acceptance, full race/Jepsen and final-push review/CI gates above
remain open. Per-worker accounting alone does not satisfy the whole-engine bound.

### Nil-panic compatibility follow-up

A separate runtime-red check under `GODEBUG=panicnil=1` exposed a false success:
`recover()` returned nil for `panic(nil)`, making a value-only recovery test
indistinguishable from normal completion. The worker now records whether the
operation actually returned, independently of the recovered value. A genuine
nil/nil return remains successful; a nil panic is a fatal worker receipt.
The focused primitive race suite, including the compatibility case, passed
twenty repetitions (6.804s), and unfiltered lint reported zero issues.

Five follow-up passes: data-loss review confirms no panic-success receipt;
concurrency review keeps the environment override in a nonparallel test and
retains existing FIFO failure fencing; performance adds only a stack-local return
flag; consistency leaves tokens, durability and timestamp behavior unchanged;
coverage includes both nil-success and nil-panic controls plus existing queued
error/panic cases. This does not enable the engine pipeline or close its gates.

### Synchronous configuration phase separation

The real Engine V1/V2 committed-configuration paths now separate the owner-only
RawNode transition from ordered FSM/persistence work and public publication.
The transition captures a deep-cloned ConfState and an owned peer inventory;
it performs no FSM or disk callback. Persistence still calls
`recordFSMConfiguration` before the unchanged `persistConfigState`. Only a
successful return reaches membership publication, the pending-config fence,
admin receipt and applied-index advancement, in their original order.

This is a synchronous preparatory refactor, not an enabled asynchronous engine.
`persistConfigState`, snapshot locks, durable-index callbacks and existing
snapshot deferral behavior are unchanged. In particular, `ErrSnapshotDeferred`
still permits the existing replay-safe configuration publication; it is not
reclassified as an operational failure. The metadata persistence helper is not
yet disk-only and cannot be sent wholesale to the append worker.

V1/V2 tests inject failures at configuration recording, FSM snapshot capture,
durable-index bump, SaveSnap and Release. Each observes no successful admin
receipt, membership publication or applied-index advancement before success.
Success and snapshot-deferral controls preserve the existing sequence, and
later RawNode/peer-cache transitions cannot mutate the captured plan. These
focused race tests passed twenty repetitions (5.101s); existing snapshot,
restart, namespace and pending-configuration gates passed twenty repetitions
(25.008s), with additional membership/unknown-outcome checks passing five
repetitions (5.508s). This does not prove crash recovery beyond those existing
tests, and it does not turn the held-Save heartbeat regression green.

The complete local etcd-engine race package passed in 42.798s with only that
known-red, unpublished heartbeat diagnostic explicitly skipped; unfiltered
repository lint reported zero issues. This is not an all-tests-passing claim.

Five follow-up passes: data-loss review preserves failure and durable-index
ordering; concurrency review keeps every phase on the current owner and clones
the configuration plan; performance adds one bounded configuration-state copy,
not a user-payload copy; consistency retains V1/V2 publication and deferral
semantics; coverage includes phase-boundary failures and existing snapshot,
membership and restart checks. All asynchronous integration gates remain open.

### Normal apply, snapshot and shutdown boundaries

Normal committed entries now capture the immutable execution dependencies and
borrowed committed bytes, execute without Engine or RawNode access, then return
an explicit receipt to owner publication. The current path remains synchronous.
Execution still checks unwrap and HaltApply failures before publication, drops
data-mutating duplicates but replays volatile effects without an old apply-index
callback, and preserves a successful prefix before a later fatal entry. Ordinary
FSM error verdicts remain committed responses. No extra user-payload copy is
added. The immutable cutover callback is evaluated during ordered execution,
not when the owner captures an input: a preceding enable entry changes how its
queued successor must be decoded. A future apply actor must also sequence its
duplicate/replay cursor; a stale enqueue-time owner applied index is not enough.

Created/configuration snapshots retain their durable-index bump before SaveSnap.
Local snapshot persistence now exposes capture, disk SaveSnap, MemoryStorage
publication/compaction, disk Release and cleanup as separate synchronous phases.
Save and Release use the same captured snapshot. Disk-only methods have no FSM,
MemoryStorage or owner callbacks. Configuration snapshots still do not compact
MemoryStorage; received snapshots still bump only after durable WAL snapshot
persistence. Snapshot locks, benign out-of-date classifications and protected
resource release remain unchanged. This separation does not itself provide the
received-snapshot restore barrier or asynchronous checkpoint prefix handshake.

Shutdown authority publication is now a separate non-I/O operation. It publishes
Shutdown and the original fatal error, clears leader/quorum-ack mirrors and
invokes nonblocking lease invalidations before logging, worker joins or storage
teardown can wait. Pending request failure classification and resource teardown
remain synchronous and unchanged; there is not yet a completion-pumping shutdown
loop. A gated Storage.Close regression first reproduced stale liveness anchors
and delayed callbacks for both single-node and quorum anchors, in normal and
fatal shutdown. A separate gated fatal logger reproduced StateLeader, a live
anchor and no fatal error while the error log was held. Both actual runtime-red
checks failed before correction. The corrected shutdown tests pass twenty race
repetitions (2.050s), including a real snapshot worker held inside SaveSnap that
must join before storage closes. The logger gate is nonparallel and restores
the global logger after joining. An independent review also exposed standard
`log` output/flags leaking from the test: a child-cleanup regression reproduced
the leak before correction and now verifies both restored output and nonzero
flags. Related KV lease/timestamp-window callers pass
five race repetitions (4.441s); all production callback and liveness consumers
were audited. This does not prove a whole native-engine Close lifecycle.

Normal phase and existing apply/encryption/cold-start race checks passed twenty
repetitions (1.960s). Snapshot phase checks passed twenty race repetitions
(13.133s); existing WAL/snapshot/configuration/recovery selections also passed
(14.082s). Tests preserve successful-prefix publication, queued encryption
cutover, duplicate classification, snapshot failure ordering, retained pins and
no receipt before Release. Clean shutdown/reopen and injected failures are not
power-loss or full native-quorum acceptance.

Five follow-up passes: data-loss review keeps fatal versus committed verdicts
and snapshot durability order distinct; concurrency review keeps execution free
of mutable owner state, revokes authority before blocking teardown and retains
all worker joins; performance preserves borrowed payloads and small captured
metadata rather than cloning user data; consistency preserves replay, cutover,
snapshot publication and error semantics; coverage exercises delayed execution,
prefix failure and deterministic shutdown/snapshot gates. The actual held-user-
Save heartbeat failure and unchanged near-64-MiB quorum gate remain open. Do not
enable AsyncStorageWrites until their full integration requirements above pass.

### Unactivated normal-entry owner pipeline

An explicit internal Engine construction now exercises a native
`AsyncStorageWrites` RawNode with owner-only mutation, FIFO disk/apply actors,
opaque response routing and a completion-pumping shutdown. Production `Open`
still constructs the synchronous engine. No public flag selects this partial
runtime, and it must not be treated as the completed design or a resolution of
the recurrent near-64-MiB quorum failure.

The owner folds each mutation's Ready without calling Advance. Local append
responses reach RawNode or the transport only after actual Save/required Sync
and owner MemoryStorage publication. Apply tasks wait for their issued disk
prefix to retire, and only completed FSM receipts publish proposal outcomes or
the applied index. An apply actor sequences its own duplicate cursor and
evaluates encryption cutover during execution. Successful prefix receipts are
retained independently of a later task error. Neither actor accesses RawNode.

The metadata-only unstable ledger mirrors the pinned v3.7.0 tail identity and
current-term stabilization rules. Displaced suffixes and stable prefixes remain
charged until the complete unstable epoch empties, covering upstream backing
array retention. Actor references have independent byte/item reservations;
pointer arrays are detached without copying user payload. Finite ingress,
deferred-task, actor, completion and routing budgets retain their charges until
their respective owners retire them. This does NOT yet implement the complete
ingress/unstable/outbound and atomic-Ready overshoot contract above; in
particular, resource exhaustion is a terminal prototype error rather than the
specified debt-driven admission policy.

Transport ingress rejects local storage message types and local-thread
identities, so a peer cannot fabricate a local durability completion. Append
acknowledgments retain a separate control admission path when bulk work pauses.
Proposal admission reserves before copying, and pending proposal metadata does
not retain the original payload. On shutdown authority is revoked first, new
work stops, and actual disk/apply completions are consumed before actors join,
RawNode/unstable ownership is disposed, or storage closes. A late status refresh
cannot resurrect leader callbacks, the leader mirror or a quorum-ack anchor;
the native leader regression first reproduced that common-path defect before
its correction.

The shared two-voter real-WAL fixture now checks a valid native v3.7.0 progress
witness. The earlier diagnostic incorrectly expected the caller ReadIndex nonce
in heartbeat.Context: the pinned library encodes an internal little-endian read
round there and preserves the nonce in MsgReadIndexResp.Entries[0].Data. Both
sync and async paths now require the post-gate native challenge, its matching
nonce response and two subsequent periodic heartbeats, with the same two-second
deadline and unchanged no-early-ACK/FSM/index assertions. Classifier controls
reject old nonces, pre-challenge ticks, malformed responses and one-tick-only
histories. No quorum acknowledgment is fabricated in the native Engine tests.

With that same witness, the synchronous default still fails while a later user
Save is held (round=0, no nonce response or periodic heartbeat), then completes
the real proposal after release. Explicit async Save, required Sync and FSM
Apply gates instead maintain native heartbeat progress without premature
proposal success. Close held behind the real Save revokes authority immediately
and joins disk/apply actors before storage Close, without manufacturing a
successful client outcome. Ledger/admission/witness/native runtime selections
passed twenty race repetitions (31.027s); the complete engine race package passed
(47.642s), explicitly excluding only the unpublished known-red default-path
diagnostic. Repository-wide unfiltered lint reported zero issues.

Real-runtime fault cases additionally inject later user Save/required-Sync
failure while an earlier committed FSM apply remains held, and FSM HaltApply
or panic while the disk actor remains inside a later actual Save. They check
completed client-prefix preservation, no failed-client success, first-cause
retention across the other actor's later error and storage Close error, actor
joins before Close, retained reservations during held work and zero budgets
after disposal. These full run-loop checks do not themselves observe the failed
opaque response at RawNode or attest one multi-entry task's actual range.
A complementary test-owned native Engine therefore inspects the real deferred
apply message after actual WAL completion: it contains entries 2 through 4 in
one task and the original response carries that range in Entries. HaltApply or
panic at entry 3 publishes only the completed client prefix 2, never executes
entry 4, and leaves native applied at 1. An explicit negative control Steps the
original forbidden response only into the disposable test-owned RawNode and
observes applied become 4, proving the cursor witness detects that violation.
No production hook or synthetic quorum response is used. Both variants pass
twenty race repetitions (6.623s). This direct owner-boundary observation
complements rather than replaces full run-loop, shutdown and recovery gates.

Independent review found that routing-budget rejection after Ready() accepted
the batch discarded its opaque local message ownership. An actual native
higher-term Ready with one-byte routing headroom and an earlier real WAL Save
held reproduced the nil retained-Ready defect. Acquisition now retains the
Ready before fallible sizing/reservation. The regression synchronously reaches
owner failure/quiesce with Save still held and verifies the same Ready and
RawNode remain owned until both actors join; disposal then clears them and
retires all related reservations. Twenty race repetitions pass (4.056s), and
independent re-review confirms both the fix and the synchronized witness.

The final combined added-test selection, including all fault and native cursor
witnesses, passed twenty race repetitions (46.271s). The engine package passed
again (45.296s, with the same unpublished known-red diagnostic excluded), as did
the complete KV package (55.941s) and unchanged native quorum migration gate
(23.070s). Unfiltered repository lint reported zero issues. The latter two
still use the production synchronous runtime; a pass does not prove the
unactivated async runtime resolves the recurrent large-write failure.

The first push's CI lint exposed one unused sync-only diagnostic factory: the
local unpublished diagnostic referenced it, but the published tree did not.
The factory now lives with that local diagnostic, not the shared fixture. A
fresh export of only staged/tracked files (no unpublished files) passes full
repository lint and the entire engine race package without any skipped test
(47.392s). This fixes published-tree hygiene, not the default heartbeat stall.

The first pipeline Go CI also exposed a scheduling assumption in the fault
fixture: a leader callback can precede reopening bulk proposal admission. The
baseline was refused by the actual pre-request reservation, before fault
injection. A single-scheduler local run reproduced the same rejection. The
fixture now waits only on the private pre-admission budget sentinel; unknown,
leadership and other errors still terminate the call, and admission tests call
Engine.Propose directly. No production retry, timeout or capacity change is
introduced. Reservation/error producers and all callers were audited.

That stronger native repetition then exposed a real concurrent ownership bug:
WAL proto.Marshal writes an entry's internal size cache while the owner cloned
an outbound entry with a whole generated-struct copy. A small concurrent-marshal
regression reproduced the race before the fix (0.825s). Outbound entry cloning
now uses proto.CloneOf, preserving payload deep-copy, optional presence and
unknown wire fields without copying internal caches. The clone/wire controls
pass twenty race repetitions (2.021s); the actual cross-actor fault scenario
passes one hundred single-scheduler race repetitions (34.386s). Both ordinary
dispatch and retained removal notifications use this audited clone helper.
These results do not replace snapshot/configuration ownership acceptance.
The final tracked-files-only export passes unfiltered repository lint and the
entire engine race package with no skipped tests (47.629s). Independent review
and separate single-scheduler repetitions found no remaining clone/admission
finding. Fresh final-push review and CI are still required for publication.

Configuration application and received/local snapshots remain unsupported by
this internal runtime. Startup/configuration journals, snapshot restore and
checkpoint handshakes, full physical ownership/accounting, encrypted recovery,
large-write native quorum acceptance and activation still require implementation
and end-to-end verification. These partial tests do not prove crash/power-loss
recovery, overload availability or the requested complete engine state.
