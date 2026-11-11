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

### Initial unactivated normal-entry owner pipeline

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

### Shared Checkpoint Configuration Prefix

Preparing the ordered snapshot path exposed a defect in the production
synchronous baseline: after configuration snapshot capture returns
`ErrSnapshotDeferred`, a later ordinary checkpoint copied the old
MemoryStorage snapshot's ConfState. A successfully applied learner therefore
disappeared from the later checkpoint metadata even though the FSM prefix and
published membership included it. Native RawNode V1/V2 apply followed by the
real local-capture/persist path reproduced this for both inline and file
payloads before correction; all four cases failed on the missing learner.

Configuration state is now seeded from the selected Raft snapshot, not the
potentially leading peers file. Successful configuration publication records
an owned full ConfState. Local capture binds a clone to the same applied
prefix as the FSM snapshot, and the worker uses that captured state for
SaveSnap and MemoryStorage publication. It never queries a newer RawNode or
owner configuration later. An older capture retains its original membership
when a subsequent configuration is applied before its disk publication.
Received snapshots replace the history base. Missing configuration state and
invalid ordered publication fail closed, rather than substituting peer data.

The synchronous path can discard prior configuration versions immediately
after successful publication: every older capture already owns a clone, and
future captures run at the current applied prefix. This keeps indefinite FSM
snapshot deferral from accumulating a configuration history. The indexed
history helper does not itself prove the async snapshot lifecycle. Async
capture pins, peer inventories, queue-byte accounting, restore handshakes and
retirement under failure remain activation requirements; none is waived by
this synchronous correctness repair.

An ordinary capture is deferred while an unapplied configuration at or before
the already-durable FSM prefix still needs replay. A configuration proposal
strictly after that prefix does not block the older valid capture. This keeps
the cold-start optimization from checkpointing an incomplete configuration
replay merely because its FSM's data applied index was already ahead.

Independent native startup tests also reproduced an ahead FSM at index 3
being serialized under replayed configuration index 2 (both entry versions).
Configuration capture now returns the existing benign deferral before any FSM
serialization when its requested index is older than the applied FSM. The
highest pending-configuration fence alone was insufficient: an uncommitted
configuration at 6 masked a committed configuration at 4 while capturing 5.
A separate owner-only committed replay fence is seeded from the actual WAL
entries up to the startup FSM/commit boundary, cleared only after successful
configuration recording, and reset after a received snapshot. Uncommitted
future entries cannot hide or indefinitely extend that required replay.

The actual-WAL checkpoint/reopen matrix covers both entry versions and both
payload formats, checking selected snapshot index, restored data and learner
membership after Open. Controls cover all ConfState fields and clone ownership,
missing/nil state, invalid publication order, persistence failures, received
snapshot reset, split/masked replay and successful capture after replay clears.
Repeated snapshot deferral through 65 configuration changes per version retains
one synchronous history entry. The two additional P1 regressions failed under
race before correction (1.795s), then passed ten race repetitions (12.437s).
Independent final focused race verification passes (27.524s) with no remaining
synchronous finding. These are clean-reopen tests, not a power-loss proof or
automatic repair of previously persisted snapshots with incorrect membership.
The complete new configuration selection passes ten race repetitions
(258.793s). A fresh export of the exact staged tree, excluding all unpublished
files, passes the entire engine race package with no skip (75.177s) and full
repository lint (zero issues). A separate broad local default-timeout trial
failed during full-length gRPC sequences and backup/FUSE deadline tests under
parallel load; isolated unchanged deadline cases pass. This is not an all-Go
pass or a replacement for final-push CI's existing split full-race workflow.

### Unactivated configuration owner/apply handshake

The internal async runtime now handles committed ConfChange and ConfChangeV2
instead of rejecting them as unsupported. It remains unavailable through
production Open. Normal local checkpoints and received snapshot installation,
their pins/ownership, full physical budgets and debt-driven admission remain
required before activation; no near-64-MiB quorum or production throughput
claim follows from this checkpoint.

The apply actor pauses its ordered stream at a configuration and sends typed
callbacks through its completion stream while retaining the enclosing task.
The owner alone mutates RawNode, plans the immutable ConfState/peer inventory
and publishes MemoryStorage, peer/configuration caches and outcomes. Namespace
configuration recording, FSM snapshot serialization/durable-index updates and
peer-file/WAL snapshot I/O run on actors. SaveSnap precedes MemoryStorage
snapshot publication; release/cleanup precedes final configuration publication.
The task's opaque apply response is held through all configuration work and
any following entries, not released at the callback boundary.

Lease anchors are withdrawn during the unpublished configuration transition,
and ready reads do not consume a mixed configuration/FSM image. Publication
resets the anchor before normal progress resumes. Synchronous call sites retain
their previous publication/result sequence; the new transition flag is only
set by the internal async path. All changed publication, namespace, lease and
ready-read callers were audited. Configuration plans and retained prefix history
have separate finite byte/item reservations and are released after actual
completion/disposal, not after merely requesting cancellation.

Native RawNode/WAL tests cover V1/V2, held configuration recording, snapshot
capture, durable-index update, SaveSnap and Release, subsequent apply ordering,
whole-task opaque response release, self-removal, errors and completion-pumping
quiescence. A Unix FIFO withholds bytes from the actual peer-file reader while
the owner demonstrates native heartbeat progress; this is not a sleeping disk
mock. Real Pebble/KV snapshot/reopen controls check data and membership. These
clean-reopen and injected-boundary results do not prove arbitrary power loss,
the future protected-policy journal, or encrypted three-voter activation.

Before parent integration, the writer's new configuration selection passes three
race repetitions (33.536s); engine race passes 63.354s, excluding only the
pre-existing untracked synchronous heartbeat diagnostic, and full lint has zero
issues. The diagnostic remains unchanged and outside the published tree.
Independent review, main-dependency integration, exact staged-only export and
final-push normal/security review plus all CI are separate publication gates.

Independent review found no validated production-handshake blocker, including
native ahead-FSM and two-configurations-in-one-task response/ordering controls.
It did reproduce a P2 in the new I/O test fixture: if configuration recording
failed before the FIFO reader opened, its blocking writer open could not see
the cleanup release. The maintained real-runtime regression failed before
repair (1.806s), with a rescue reader joining the old helper after failure.
The helper now retries a nonblocking kernel open with cancellation; the actual
successful open and withheld bytes remain the normal-path progress witness.
Both normal I/O and early-failure join pass twenty race repetitions (13.133s).
The common-path extraction also retains the synchronous alarm-after-result
sequence, rather than moving logging before an existing proposal receipt.

The first parent main-integrated source export passes whole engine race
without skips (114.235s), whole KV race (107.177s), all SQS/FIFO and the real
DynamoDB leader-change control (45.934s), and complete lint. That export
predates the fixture/alarm follow-up; final publication requires a fresh
source export of those changes rather than reusing its receipts.

## Snapshot Handshake Implementation (Unactivated)

The explicit internal runtime now implements normal local checkpoints and
received snapshots. Production `Open` still constructs the synchronous
RawNode. This implementation does not satisfy the outstanding full physical
accounting, Ready overshoot/debt admission, outbound telemetry, large-write,
encrypted three-voter acceptance, or final composed-source publication gates.

Local checkpoints enter the FIFO apply stream. At its front, the actor asks
the owner for the actual completed index, term, full owned ConfState and peer
inventory. The actor captures the FSM handle and durable index; disk work
serializes/closes the handle and performs SaveSnap. Only the owner publishes
MemoryStorage and schedules FIFO Release/cleanup. Configuration captures use
the same disk serialization and resource lifecycle. Captures retain history
and file pins; outgoing snapshot requests retain a file pin until their last
dispatch/report use. These pins are separate from outbound byte accounting.

An incoming snapshot first enters the ordered apply stream, before owner Step
can replace RawNode's configuration. The owner validates membership and takes
the tuple. Its genuine native MsgStorageAppend becomes FIFO disk work: verify
the complete token payload, open its owned reader, SaveSnap and Save/Sync.
The apply actor then restores the FSM, namespace configuration and durable
index. Disk work closes the reader, persists peers and performs Release.
The owner installs MemoryStorage and publishes configuration/applied state
before routing the original opaque responses. Ordinary append completions
remain retained in sequence while installation is active. Old-term native
snapshot receipts still reach RawNode; they are not treated as stale network
messages. FSM restore consumes the verified reader on the apply actor;
transport streaming/spooling retains its existing I/O and validation path.

Received-file protections become registered leases at ingress admission, not
only when the owner consumes ingress. Failure and Close stop admission and
pump real completions. Disk finalization joins the apply actor before closing
remaining handles/readers; final owner retirement releases tuple/history pins
and reservations. No timeout manufactures successful or aborted completion.
The registry rejects new ownership after finalization. Cold-start restoration,
external restore lineage and retained-restorable-snapshot validation are not
replaced or bypassed by this runtime.

Native tests exercise three voters, real WAL/MemoryStorage, streamed gRPC
snapshot payloads, receiver heartbeat responses during held SaveSnap/restore/
Release, local capture/serialization/handle Close/durable-index holds, opaque
response suppression, actor joins, failure/reopen, and real Pebble/KV native
and SST-ingest snapshots. V1/V2 controls verify an older captured prefix stays
unchanged through a later deferred membership capture, then select the newer
local checkpoint and membership on reopen. Unit controls preserve all joint
ConfState fields, prune behind pins, retain pinned older file pairs, verify
full CRCs, and reject new leases after finalization. This preserves fields;
it does not broaden the existing joint-membership admission policy.

Two new-path regressions were reproduced before correction: queued volatile
work covered by a received snapshot was re-applied after restore, and received
file protection survived shutdown when its ingress had not been consumed.
The implementation now discards snapshot-covered queued work without creating
proposal receipts and includes unconsumed ingress in post-join cleanup. The
configuration ordering test also observes owner publication directly instead
of assuming its caller's forwarding goroutine has already run.

## Outbound Ownership And Snapshot Composition

The internal runtime now reserves outbound capacity before cloning a message.
Its separate bulk allowance is 512 MiB / 128 requests; heartbeat, vote,
ReadIndex and append-response traffic use an independent 8 MiB / 256-request
allowance. These limits are retained across queueing, RPC execution and a
blocked reliable snapshot-completion report. Peer removal, cancellation,
coalescing, failed admission and shutdown retire only their actual owned
requests. Network admission failure uses the existing Raft dispatch feedback;
it does not discard local storage work or create a durability receipt.

The charge conservatively allows twice the protobuf/envelope estimate for
byte-slice allocation rounding plus sixteen bytes per ConfState uint64 element,
including nested responses. Addition and multiplication saturate on overflow.
This is not an exact or complete Go-heap measurement. A real 63.5 MiB entry
still fits the unchanged allowance and is tested without reducing its payload.
Aggregate ingress/unstable/actor ownership and atomic-Ready overshoot/debt are
still separate unfinished acceptance requirements.

Two physical-capacity regressions failed before correction: 1,000 snapshot
voters held at least 8,200 bytes against a 3,412-byte charge; a 65,537-byte
entry retained 73,728-byte capacity against a 66,327-byte charge. A separate
independent reproduction found that shallow unknown protobuf fields retained
a shared 1 MiB backing array with only a few KiB reserved. The complete protobuf
is now cloned, including unknown fields and optional headers; repeated nil
message sentinels are preserved rather than normalized into manufactured
messages. The concurrent-WAL-marshal tests exercise that actual live clone
path, not a removed entry-only helper. Five unknown-field cases failed before
repair and the already-owned Entry case remained a positive control.

The new worker-exit drain also exposed a fixture/runtime integration error:
the old nonblocking receive helper ignored a closed channel's boolean result
and repeatedly returned a zero-value request. It now distinguishes closed and
empty channels; a direct regression and real cancellation/join tests cover it.
Both initially hung test runs remain failed evidence, not successful receipts.

Snapshot file pins and byte permits coexist on the same dispatch request.
Both are acquired before cloning and retained through the last dispatch/report
use; idempotent Close retires both. Composition controls check simultaneous
retirement, rejected clones acquiring no pin, and removed-peer lane drains
releasing every queued snapshot pin and byte reservation.

The final standalone outbound source passed focused race tests three times
(2.374s), whole engine race without short/skip options (93.646s), full lint,
and independent ownership re-review. The re-review includes unknown fields,
optional pointers, every slice and nested nils; no new validated P1/P2 remains
within that component. A staged-only snapshot/outbound export passed the broad
pipeline/snapshot/ownership race selection (59.988s) and full lint. Main through
0d5c5c230 was then merged without changing that engine source. These component
receipts are not final composed-repository CI, encrypted three-voter acceptance
or default activation. The original synchronous diagnostic stays outside the
published tree and remains intentionally unresolved until production activation.

A subsequent whole-KV run failed the existing process-kill recovery fixture
because its secondary was already committed before the explicit recovery
assertion. The same unchanged test also reproduces on an independent baseline;
its cause and fault boundary must be repaired or reconciled, not hidden by a
rerun, skip or relaxed assertion. Earlier KV passes do not qualify the final
composed source. Fresh full-suite, last-push code/security review and all CI
remain mandatory before publication readiness.

### Ingress ownership checkpoint

The internal async transport queue now obtains a conservative byte reservation
before making a complete protobuf clone. The prior pointer-slice detach retained
caller payload/context/unknown-field backing arrays and mutable protobuf headers.
Maintained regressions reproduced four 1-MiB backing arrays charged as only 783
bytes, and accepted-message changes after caller mutation. These are now owned
by the accepted queue item, including snapshot metadata, ConfState and unknown
fields. Budget rejection makes no clone. A full queue can reject after the
reserved temporary clone; that permit is released and the untransferred lease
is forgotten. The existing pre-Step error and separate control/snapshot
admission behavior remain unchanged.

The existing ingress reservation remains live through actual Step and folding
the resulting Ready. Downstream actor/unstable reservations are separate; this
change does not complete their physical accounting or atomic-Ready debt policy.
The synchronous/default engine remains unchanged and unactivated.

The initial ownership selection passes race3 (1.800s), the whole engine race
passes without skips (96.027s), and full lint reports zero issues. Added inline
snapshot ownership/retirement and 64-MiB-plus-envelope admission controls pass
race3 (1.772s). One intermediate test-helper run waited on the wrong ingress
lane and was stopped with its stack saved; the corrected test observes the
existing snapshot priority lane with a bounded receive, without changing
production routing, payload size, assertions or engine timeouts. Final-source
integration, independent review and fresh remote review/CI remain required.

### Composition lifecycle corrections

Independent native three-voter tests found two additional P2s. Fatal quiescence
stopped consuming reliable snapshot reports without signaling their terminal
escape: a full report channel prevented worker join and retained the outbound
byte permit and file pin until an external Close. The owner now signals close
after authority revocation and first-cause publication; actual dispatch request
retirement still follows the worker's return, not the shutdown notification.

Separately, a rejected snapshot's local SnapshotFailure stayed in a deferred
list that only the synchronous loop flushed. Capacity returning did not release
RawNode's StateSnapshot. The async owner now processes that feedback only after
the emitted Ready has been accepted and successfully routed. Pinned Raft v3.7
accepts Ready inside Ready(); SnapshotFailure pauses probing until a subsequent
heartbeat, rather than immediately regenerating snapshots in an owner loop.
Both failures were reproduced three times before correction. Maintained real
gRPC/three-voter regressions and positive lifetime/pressure controls pass race3
(18.300s), retaining the original failure checks and post-failure-only rescue.

A separate native acceptance slice uses actual encrypted Pebble stores and
three explicit async engines. With a real follower Sync held after large-entry
Save, fresh heartbeat/read-index progress continues while quorum commit, ACK,
FSM application and client success remain withheld. A 63.5-MiB value occupies
one 66,584,616-byte Raft entry. After actual durable quorum, streamed snapshot
catch-up and a fresh-FSM clean reopen retain the value digest, baseline,
three-voter membership and matching durable/published applied prefix. Three
race repetitions pass (37.260s), with full lint zero issues. This does not prove
encrypted Raft-WAL envelopes/cutover, key rotation, crash/rejoin, SST snapshots,
full accounting/debt, or production activation. The fixture leaves Raft
envelope cutover inactive; storage encryption is observed on real large values.

A distinct small-payload native fixture starts with Raft envelope cutover
already active. Three actual async engines use separate encrypted Pebble stores
and real gRPC transport. Holding a follower's real WAL Sync with the third voter
partitioned permits fresh heartbeats but no early ACK, quorum commit, FSM apply
or client success. The fixture decrypts the actual stored Raft entry, restores
a streamed snapshot, verifies its CRC and cutover header, and reopens a fresh
FSM from that exact snapshot plus a later encrypted committed WAL tail. The
snapshot retains its applied prefix and voter configuration; the tail remains
outside the selected snapshot and is recovered by real WAL replay.

Committed plaintext after active cutover, a tampered authentication tag and a
missing Raft cipher each halt without advancing the applied prefix or returning
client success. Reopening without the Raft cipher refuses the committed tail
rather than silently downgrading it. The component passes race3 (12.980s), an
independent parent race3 (13.093s), and full lint. This adds already-active
envelope/clean-reopen evidence, not evidence of the cutover transition, key
rotation, crash/rejoin, large envelope payloads or production activation. Those
gates and physical accounting/Ready debt remain open.

The independent process-kill fixture correction was merged as #1368 at
e939af5ba6cf9eeb6c6a3eea7754107f5ee25363 after latest-push normal/security
review and every CI gate. SIGKILL acceptance no longer returns success before
process death; kill errors propagate. The original native recovery assertions
remain unchanged. An additional premature-cancellation mutation escaped the
first observer in 10/20 trials; the corrected observer rejects all 20. Its
whole-KV race passes (105.064s), native SIGKILL/reopen race20 passes (26.798s),
and full lint reports zero issues. That test-only correction is now included
in this branch's main merge, so new composed-source checks can qualify the
actual latest source rather than a lucky rerun of the old fault boundary.

### Physical ownership/debt candidate: not an activation receipt

The explicit internal runtime now connects proposal/transport ingress, unstable
generations, append/apply tasks, snapshots and configuration history to bounded
reservation roots. The bulk root has normal allowance B = 512 MiB / 128 work
items and one shared emergency allowance of 512 MiB / 7 work items. This is a
single root allowance, not a new allowance granted to each Ready. Debt closes
new bulk proposal/transport admission and local checkpoint capture. Accepted
snapshots wait with their ingress permit and receive lease; priority traffic and
real actor completions remain pumpable. Impossible owner routing fails terminally
with the accepted Ready retained through actual actor join; it does not drop
opaque responses or fabricate successful completion.

The control root remains 8 MiB / 256 work items. Outbound clones deliberately
retain their independently reviewed budgets: 512 MiB / 128 bulk work items and
8 MiB / 256 control work items. StoragePipelineUsage reports these separately
and reports ReservedBytes/ReservedWork across all four roots. The configured
reservation ceiling is therefore 1552 MiB / 775 work items, not 1024 MiB for the
"total pipeline". Component counters are sampled under their respective locks,
not as one atomic heap census. Queue occupancy permits do not duplicate payload
ownership: the task's owner permit spans deferred, queued, executing and
unconsumed-completion states. Durable MemoryStorage/FSM data, transport codecs,
OS buffers, GC/allocator overhead and pre-reservation upstream allocations are
not claimed to be covered by this counter ceiling.

Ownership paths and actual retirement boundaries are:

- Proposal: reserve before caller payload copy/envelope/Propose; the resulting
  unstable ledger and emitted task reservations outlive the ingress reservation.
- Transport: reserve the complete clone projection before cloning/Step; transfer
  accepted snapshot leases and network ReadIndex permits rather than releasing
  them at queue removal. ReadIndex cancellation alone cannot release RawNode's
  retained context. Raft v3.7's pre-current-term-commit pending queue survives
  term/leader reset. Those permits survive too. A genuine current-term commit
  rebinds them to the new readOnly epoch before routing its Ready; only actual
  ReadState/response routing, a subsequent readOnly reset, or RawNode disposal
  retires them. Remote read permits wait until the entire Ready is routed.
- Unstable: retain backing capacity, unknown fields and one item per append
  generation, not per entry. Displaced suffixes and stable prefixes remain
  conservatively charged until the actual upstream epoch drains or RawNode is
  disposed. Append actor borrows have independent reservations.
- Append/apply: compact borrowed pointer arrays before retaining tasks; preserve
  task/receipt ownership through the real FIFO completion, owner publication and
  routing of the original opaque responses. Actor queue permits retire only at
  actual completion consumption.
- Configuration/snapshot: plans retain their own permits and exact ConfState,
  peer inventory, file/history pins and receive lease through the handshake's
  actual final disk/apply stage. Received-snapshot replacement resets the
  unstable base only after RawNode actually replaces it. Close/failure joins
  actors before disposing RawNode and releasing remaining pins/owners.
- Outbound: reserve before clone; queue removal, RPC return and pending reliable
  snapshot reports are not retirement. Close follows the worker's actual final
  use. The existing replacement-before-release and peer-removal/drain contracts
  remain unchanged.

**The proposed generic numerical Ready bound is still not proved.** The earlier
bill charged a separate Entry descriptor per remote message, even though pinned
Raft v3.7.0 `log.go:slice` and `storage.go:Entries` return shared Entry pointers
and, for pure stable/unstable ranges, the same pointer array. That bill failed
both one-voter/40-learner/65,536-entry and three-voter/57-learner/40,000-entry
success-asserting native regressions. Both are now maintained in the repository.

Ready accounting now visits actual Entry/message identities once and charges
each identical-start pointer array's largest exposed capacity. Different-start
overlaps, snapshots and distinct-object byte-slice aliases remain conservatively
charged separately; payload and unknown-field backing capacity are not omitted.
The walk stops when the Ready cannot fit, bounding its temporary identity maps.
It includes ReadStates and contexts, not just local append/apply messages.
Ready retains its own permit throughout routing; each local task/ledger acquires
its independent permit before takeover, while every actual outbound deep clone
still reserves its independent budget before copying. Clearing the successful
Ready precedes release. A failed Ready remains retained until real actor join.

The maintained fan-out cases include 63.5 MiB with forty real learner RawNodes.
Their initial protocol exchange and local response provenance use actual
volatile MemoryStorage publication, not fabricated ACKs. Outbound consumers are
then held at the real dispatch clone/enqueue boundary: admitted clones retain
their permits after Ready retirement, existing outbound congestion/retry policy
is unchanged, and clone retirement cannot retire local append/unstable owners.
These witnesses qualify actual Ready shape/routing and ownership, not durable
catch-up of forty learners. The separate native three-voter WAL/quorum/FSM large
value, held-Sync and snapshot/reopen tests remain required.

The read-lifetime regression uses actual three-voter elections, CheckQuorum
expiry and three canceled local plus three network reads across terms. All six
reservations survive the native pending queue, then survive its transfer to a
new-term readOnly queue with heartbeat responses withheld. Only the subsequent
real read-quorum responses retire them. Term reset is not treated as proof that
pendingReadIndexMessages died. The current-commit proof uses the owner unstable
shadow first (it may replace an older durable entry), then MemoryStorage.Term;
no actor or upstream-private-field reads are used in production.

The previous conditional 3A + 4H + M / 7U + 8 MiB argument was not an upstream
bound and is not an activation receipt. In particular, `log.go:slice` allocates
an extended pointer array for a mixed stable/unstable range, and `stepLeader`
can repeatedly call `maybeSendAppend` after one MsgAppResp. Unlike pure-range
fan-out, these can be distinct arrays. Membership, configured inflights and
actual mixed-range entry counts need a pre-mutation resource contract; a fixed
8-MiB remainder or topology alone is not a proof. No business payload target,
election timeout, B, R or accepted configuration size was reduced to pass tests.
Snapshot restore/Snapshot clones, startup/WAL first-entry bounds, configuration
decode expansion and allocation before reservation also remain numerical gates.
No maximum topology or special storage provenance is silently assumed. A full
physical B+R theorem, all-shape routing acceptance and production activation
remain open pending those bounds and independent review of the new source.

#### Follow-up: enforced preflights and an actual snapshot obstruction

A second native success regression covers 600,000 recovered stable one-byte
commands, three voters, MaxSizePerMsg=1 MiB and MaxInflightMsgs=1024. One genuine
follower ACK at index 53,255 previously caused the 11-message Ready walk to stop
above R (536,871,304 bytes). Existing stable objects were charged as new owners.
The fixed owner captures the exact MemoryStorage Entry range before Ready and
compares BOTH Entry identity and pointer-array cell identity, not just equal
index/term/data. Only base-owned objects/arrays borrowed by the non-yielding
Ready walk are excluded. Different concat arrays remain independently charged.

Every pipeline MemoryStorage Append/CreateSnapshot/Compact/ApplySnapshot path
now checks that no accepted Ready borrow is live. Successful routing clears the
Ready before releasing its permit. Failure retains it, and the mutation checks
also prohibit invalidating that base during failure/join. Actor tasks detach
pointer arrays and reserve full retained Entry/data before handoff, even when
the source was stable. Thus task payload/array retention AFTER compaction is
still charged; durable MemoryStorage's exclusion does not extend to such new
last references. The maintained exact-identity/compaction/takeover control and
the genuine 600,000-command catch-up success assertion exercise these boundaries.

Additional enforced guards, without changing B/R/C or business-entry size:

- Received snapshot front reserves the complete projected message clone AND
  snapshot clone before either clone. Rejected input cannot allocate the full
  payload first. Caller-owned backing is not silently adopted.
- Configuration wire is scanned with protowire before Unmarshal. Context and
  unknown byte expansion, repeated change descriptors, prospective joint-member
  records, temporary shallow Progress/maps and peer inventory are reserved before
  native ApplyConfChange. The four-Entry-allowance per member is a conservative
  control projection, not a theorem about arbitrary runtime allocator overhead.
  Changer.checkAndCopy shares existing inflight buffers rather than cloning them.
  The actual decoded plan is checked against that reservation. Existing uint16
  context fields at their maximum size remain accepted by the projection.
  Public ingress additionally reserves the peak proposal-decode projection
  before clone/Step, since RawNode.stepLeader itself unmarshals configuration
  proposals. Queue rejection/drain and completed owner consumption retire it;
  local admin proposal reserves the same bounded temporary decode headroom.
- Initial and received ConfState expansion has a resource-derived projection
  check against C before NewRawNode or snapshot cloning. Recovery scans borrowed
  entries before NewRawNode and rejects an individual retained Entry backing
  above U=72 MiB, including unknown capacity/metadata. The 64-MiB command plus
  1-KiB envelope remains accepted. This per-entry check is not a bound on the
  union of stable catch-up entries or mixed-array fan-out.

An input whose Ready has already been accepted but cannot be reserved/routed
keeps its original ingress reservation until actual joins and RawNode disposal.
This does not pretend to charge an oversized newly generated output, but avoids
releasing the already owned source while failed Ready still borrows it. Network
ReadIndex transfers its exact message HEADER as well as its permit: normal
ingress cleanup must not zero that header while pending/readOnly retains it.
Fatal cleanup clears failed Ready and failed-input headers only after disposal,
before retiring transferred read owners. Native read/config and join regressions
cover this distinction; a cleanup-zeroing regression was reproduced and fixed
without suppressing the original assertions.

**Generic R still has a concrete, independent-clone obstruction.** A private
native witness bootstraps one voter and nine real learners to a genuine stable
prefix, loses subsequent append packets without fabricating any ACK, and
publishes/applies a normal 63.5-MiB command locally. It serializes the actual FSM
snapshot using the supported inline path, installs/compacts that snapshot, and
reports the delayed transport failures. The next tiny admitted proposal calls
bcastAppend. Pinned MemoryStorage.Snapshot deep-clones the inline body separately
for all nine lagging learners BEFORE Ready reservation. Actual distinct Data
backings have capacity 599,334,912 bytes, exceeding R=536,870,912 by themselves.
This is new pipeline-held memory, not the original stable/FSM storage and not
the old duplicate Entry bill. The preserved success assertion fails terminally.
The witness uses volatile MemoryStorage/FSM publication, not a WAL durability,
encrypted cutover, or complete-heap measurement claim.

The existing APIs do not supply pre-clone snapshot size/provenance:
MemoryStorage.InitialState exposes ConfState but not snapshot Data, while
MemoryStorage.Snapshot performs the clone before returning. Snapshot.WriteTo
also supplies no pre-capture resident/pin-memory bound. Inspecting a clone after
it was allocated, dropping the accepted Ready, or excluding its new last
references cannot fix this pre-mutation contract.

Minimal compatible directions, requiring an explicit next handshake contract:

1. Normalize every local/received/recovered inline snapshot to the existing
   verified file/token format on the FIFO disk actor BEFORE installing it in
   the RawNode storage view. Seed owner-visible bounded descriptor metadata at
   bootstrap as well as publication; do not first discover it through a clone.
   This needs no new on-disk format or lower business payload limit, but must
   preserve retained-pair recovery and save/restore/publication ordering.
2. Alternatively supply an owner-seeded snapshot resident/clone bound and reserve
   total per-mutation fan-out before Step, with a typed retry/admission result
   before accepting work that cannot fit. An arbitrary Snapshot implementation
   additionally needs a resident/pin-size capability; its output-byte count alone
   cannot bound memory retained by its captured export handle.

Mixed stable/unstable concat still requires an owner-side prospective array
projection (actual stable/unstable boundary, entry counts, peer progress and
effective message/inflight settings) before Step/Propose/config mutation. Pure
borrowed stable ranges now have an exact base lifetime rule; newly allocated
concat arrays do not inherit that exclusion. This too has a concrete native
witness: one real voter plus 104 real learners, ten admitted 65,536-command
batches with original local append/apply responses after volatile publication,
remote packet loss, and delayed ReportUnreachable. Every drained prefix Ready
fits the old routing bill. The next tiny admitted proposal joins the 655,360
stable commands to one unstable command for every learner. Pinned util.go:extend
allocates each mixed range separately: 104 distinct arrays, each capacity
655,361, total pointer cells 545,260,352 bytes > R=536,870,912, before charging
any Entry objects or payloads. The unchanged success assertion fails. The
non-race 90-second run reaches the boundary in 6.539s; the race run times out
during prefix preparation at 90.658s and is NOT counted as race evidence of the
final allocation. The payloads, prefix assertions and deadline were unchanged.

The minimal mixed-range repair needs owner preflight of that prospective output
union, not an input-byte guard. When it cannot fit, a typed rejection/retry must
precede mutation/acceptance (including the network owner-admission handshake),
while ordinary catch-up control/completions remain pumpable. Descriptor-aware
upstream batching is another option, preserving the first-large-command rule;
silently truncating emitted messages or cloning them before checking is not.
Snapshot normalization and this headroom contract, not a larger R or smaller
successful payload, are the next required changes. No full generic R or
production activation receipt is issued.

### Inline Snapshot Normalization Candidate

The explicit async runtime now seeds an owner-only Storage facade from the
selected snapshot before starting its owner. The facade fences Snapshot until
the FIFO disk actor verifies an existing token or normalizes a legacy inline
snapshot into the existing FSM file/footer/token representation. Recovery reads
the existing native Raft snapshot file as a stream: it verifies the CRC32C of
the entire inner protobuf, checks the selected index/term/full ConfState and
unknown metadata, and copies only its body into the verified FSM file. Native
EnsureSnapshot scalar defaults are canonicalized for comparison; unknown fields
and all joint-configuration fields remain significant. Zero CRC rejection is
the same as the native snapshot reader. No new disk format is introduced.

For recovered inline snapshots the durable FSM file precedes SaveSnap of its
token. Only the actual disk completion lets the owner replace MemoryStorage
with the same snapshot tuple, HardState and retained entry tail. Already-token
recovery verifies the file without rewriting SaveSnap. Local/configuration
checkpoints always serialize to that existing file form. Received inline data
is normalized before native Step; the original ordered restore, SaveSnap/WAL,
durable-index, MemoryStorage publication and opaque-response handshakes remain
in force. A native stale snapshot follows native rejection/current-prefix
handling without writing over an existing same-index file.

Public snapshot admission validates ConfState expansion AND the descriptor's
unknown/metadata projection before ownership cloning or enqueue acceptance.
Configuration entry ingress reserves its bounded standard-decoder expansion
before validating V1/V2 nested protobuf grammar; malformed nested Single data
therefore cannot reach native stepLeader's panic. Maximum context field values
and the 64-MiB command plus envelope controls remain enabled.

Before each real MemoryStorage.Snapshot clone the facade reserves its token,
full configuration and unknown descriptor projection. Clones stay charged until
the accepted Ready is successfully routed to independently reserved tasks or
outbound clones. Failure keeps those permits until actual actor joins and
RawNode disposal. Normalization source/scratch reservations and file pins are
similarly retired only after actual completion or joins. Index-only owner
queries, including Status refresh, no longer clone inline snapshot bodies.
Received publication reserves the new independently cloned configuration-history
base before replacing/releasing the old history; its reservation lasts beyond
the receive plan and is released after the history's actual last reference.

The maintained nine-learner/63.5-MiB witness preserves the original proposal,
native local responses, lost packets, deadline and final success assertion.
It now runs real FIFO normalization before installation, verifies the actual
file's restored FSM contents, and requires all nine native snapshot sends.
Additional native controls cover selected Raft file CRC/metadata, retained WAL
tail clean reopen, public inline three-voter catch-up with held Restore and
no early opaque ACK, and the existing encrypted large-quorum snapshot/restart
path. Clean reopen is not crash/rotation/cutover certification.

This fixes the independent large-body snapshot-clone witness, NOT generic R or
the FSM capture-memory gate. The existing Snapshot capability still has no
pre-capture resident/pin bound. The memory store returns an immutable temp-file
handle, while native Pebble's WriteTo creates an iterator that may pin live
memtables/read state until serialization returns; SST export additionally owns
checkpoint/fallback state. Neither encoded output size nor a constant handle
metadata bill bounds that retention or capture's transient memory. A truthful
native capability must separately guarantee capture peak, retained state and
serialization pin/working memory, atomically with the captured read point,
before the engine can reserve and later release it at real Close/join. No such
store/FSM capability is claimed by this etcd-only change. It requires an
explicit lower-layer implementation, not an assumed zero or arbitrary fixed
cap. Mixed stable/unstable output union and topology/descriptor headroom remain
separate unresolved admission gates. B/R/C and production Open are unchanged.
