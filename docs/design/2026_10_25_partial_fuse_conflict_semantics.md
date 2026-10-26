# FUSE conflict resolution and operation outcomes

Status: Partial
Author: bootjp
Date: 2026-10-25

## Problem and scope

Ordinary filesystem applications should not implement an OCC retry protocol.
For supported regular-file and namespace operations, this proposal makes OCC
conflicts an internal scheduling event: a blocking request waits for a definitive
result or reports a bounded operational failure. It does not promise success
during an unavailable quorum, and does not equate an uncertain commit with an
aborted transaction.

The central change is a filesystem operation module that resolves one logical
request across transaction attempts, forwarding failures, and leader changes.
It needs a durable operation identity and an atomic result record, not merely an
`EAGAIN` loop in the FUSE adapter. The operation journal can publish a result
with its effects, but transaction decision resolution and authorized session
takeover remain pending. FUSE mutation routing and mounted validation remain
in M3-M5.

This improves the contention semantics of the supported filesystem subset. It
does **not** establish full POSIX conformance, distributed advisory locks,
cross-mount page-cache coherence, atomic cross-mount append, or SQLite WAL
support. Those gaps must remain explicit. In particular, a write at a fixed
offset is not safely replayable merely because the bytes are unchanged: a
second application of an old write can overwrite a later writer.

## Current evidence

References below are repository-relative paths and one-based source lines at
`84d4d900685473aa597e75494827ce3cf33b6411`; line numbers are review anchors,
not assertions about future code.

| Evidence | Current behavior and consequence |
| --- | --- |
| `internal/filesystem/fuseadapter/adapter.go:61`, `:133` | `ErrStaleHome`, typed OCC conflicts, and recognized forwarded conflicts map to `EAGAIN`. |
| `internal/filesystem/write_conflict.go:12` | Forwarded classification parses a gRPC error message. Its comment explicitly warns about lost-response ambiguity; this is not an authoritative per-operation outcome protocol. |
| `internal/filesystem/fuseadapter/adapter.go:180`, `:188`, `:196`, `:203` | Read, write, flush and fsync first mutate the handle lease; contention can fail before the actual operation. |
| `internal/filesystem/service.go:556`, `:679`, `:873` | Open has selective bounded retries; write builds one read/modify/write transaction; rename commits one namespace transaction and rejects cross-directory moves. There is no common logical-request retry policy. |
| `internal/filesystem/fuseadapter/server.go:211`, `:219` | Create deliberately does not replay an ambiguous failure. A genuine existing nonexclusive create uses `ESTALE` to request kernel path revalidation rather than incorrectly marking the existing inode newly created. |
| `internal/filesystem/fuseadapter/server_ops_test.go:274` | `TestRawFileSystemCreateDoesNotReplayAmbiguousWriteConflict` protects that non-replay property. Replace its expectations only when equivalent operation-identity protection exists. |
| `internal/filesystem/fuseadapter/adapter_test.go:33` | Existing tests encode `EAGAIN` mappings; changing only these expectations would not prove safe retries. |
| `internal/filesystem/service.go:1115`, `:1119`; `internal/filesystem/fuseadapter/server.go:371` | Flush returns nil; fsync reads attributes; FsyncDir returns success without a barrier. None is presently a distinct tracked durability barrier. |
| `internal/filesystem/recovery.go:28`, `:108` | Namespace intents prepare create/delete and support recovery. They do not retain the original caller's result, returned inode/handle, or write count, and are not a deduplication ledger. |
| `internal/filesystem/service.go:2630` | Filesystem transactions may span inode, directory, home, usage and chunk routes; `dispatchTxn` binds the read timestamp voucher. A single-group-only dedup scheme is insufficient. |
| `adapter/internal.go:229` | Internal forwarding verifies leadership and submits transactions. Its existing error return is not a durable operation-status lookup. |
| `internal/filesystem/fuseadapter/server.go:431`, `:451` | Request contexts join request interruption and mount cancellation. Resolution must cooperate with both without treating cancellation as proof of rollback. |
| `jepsen/src/elastickv/filesystem_workload.clj:18`, `:114`, `:175` | The harness uses 20 ms backoff but limits write retry to provable pre-mutation failures; possibly applied writes are indeterminate. This protects the checker, not arbitrary applications. |
| `jepsen/src/elastickv/sqlite_workload.clj:4`, `:18`, `:47` | SQLite tests use local-to-mount locks, separate databases and explicit retries with PERSIST journals; they do not demonstrate shared-database safety across mounts. |

`docs/filesystem.md` currently recommends application retries under contention.
Its suggestion that equal-offset rewrites, `EEXIST`, or `ENOENT` establish a
previous mutation's outcome must be replaced at rollout: concurrent replacement
makes those observations insufficient. The implemented filesystem design is
`docs/design/2026_02_24_implemented_filesystem_on_elastickv.md`; this proposal
changes its error/outcome contract without expanding its supported namespace
operations.

## Externally visible contract

### Blocking, nonblocking, and deadlines

Supported regular-file operations, including descriptors opened with
`O_NONBLOCK`, use the same conflict-resolution policy. Linux does not currently
make regular-file I/O nonblocking merely because that flag is set; returning
`EAGAIN` for an internal OCC conflict would invent a different contract. The
Linux man-pages project's [open(2)](https://man7.org/linux/man-pages/man2/open.2.html)
describes this distinction. Unsupported pipes, devices, nonblocking lock
operations, and `RWF_NOWAIT` are not introduced here. A future genuinely
nonblocking interface must either decline before admission with no mutation,
or use an explicitly specified async completion protocol; it cannot return
`EAGAIN` while leaving a mutation live.

Each request has one budget, including admission, local queueing, metadata
refresh, transactions, resolution and backoff. Proposed defaults are a 30 s
request deadline, 2 s maximum per network attempt, and up to 256 definitive
aborted attempts. The earlier of the FUSE caller deadline, mount shutdown, and
request deadline applies. Reserve up to the final 2 s of the budget for outcome
resolution/cancellation, rather than starting another mutation at that point.
These are tunable operational defaults to validate under load, not extensions
to the consistency model. Monotonic elapsed time controls waiting; backend
ordering continues to use authoritative timestamps/vouchers.

Use cancellation-aware exponential full-jitter backoff starting at 1 ms and
capped at 100 ms after a definitive OCC abort. Route changes trigger one fresh
route/home read before backoff. A bounded FIFO local queue reduces contention;
queue capacity and worker limits are explicit mount configuration. Admission
queue exhaustion returns `EBUSY` before durable submission. Attempt exhaustion
with all attempts conclusively aborted returns `EBUSY`; elapsed deadline with
conclusive non-commit returns `ETIMEDOUT`. Neither becomes `EAGAIN`.

A single FUSE write request commits all its bytes or none and returns its exact
recorded count. A large application `write(2)` may comprise several FUSE
requests: already completed requests are not rolled back or replayed when a
later request fails. Preserve kernel short-write behavior and never manufacture
a zero-byte success to hide a conflict. The Linux man-pages project's
[write(2)](https://man7.org/linux/man-pages/man2/write.2.html) documents short
writes and interruption; application-call atomicity beyond the FUSE request
size is outside this proposal.

### Outcome and errno table

| Internal state | Required action | Observable completion |
| --- | --- | --- |
| Authoritative OCC abort of the recorded attempt | Re-read and rebuild with a fresh valid snapshot under the same operation ID | Success or genuine semantic error; budget exhaustion as above |
| Stale home/route before submission, or conclusive epoch-check abort | Refresh placement, revalidate all operands and retry | No `EAGAIN`; deadline or genuine semantic error |
| Lost response, timeout after dispatch, unknown transaction decision, untyped legacy forwarding error | Resolve the recorded transaction/operation; do not replay logical mutation | Original result if resolved; `EIO` if still uncertain at deadline |
| Infrastructure outage before submission, or after a confirmed abort | Wait within budget; no ambiguity inferred merely from unavailability | `ETIMEDOUT`; known disconnected mount uses `ENOTCONN` |
| Interrupt before submission, or durable cancellation wins | Stop; no operation effect can later commit | `EINTR` |
| Interrupt racing a committed result | Recover and return the committed result if the FUSE reply can still be delivered | Success, never falsely claim a guaranteed non-effect |
| Interrupt/deadline while commit remains uncertain | Retain recovery state, mark mount degraded and stop new mutations | `EIO`; no promise that the request had no effect |
| Definitive filesystem condition | Do not retry the semantic error | `ENOENT`, `EEXIST`, `ENOTDIR`, `EISDIR`, `ENOTEMPTY`, `EXDEV`, `EINVAL`, or `EOPNOTSUPP` as applicable |
| Corruption, payload/identity mismatch, impossible result transition | Fail closed and alert | `EIO` |

Cancellation cannot simultaneously guarantee bounded response, successful
rollback and availability under a partition. This proposal explicitly chooses
bounded waiting and an honest uncertain `EIO`. It provides at-most-one logical
effect per admitted operation, not exactly-once delivery to the application.
A new application syscall after an `EIO` has a new identity; applications must
not be told that arbitrary replay is safe. Pending operations may still resolve
to committed after the original error. That is recorded and surfaced to the
operator, not silently retried as new work.

When outcome remains uncertain, quarantine **new mutations on the affected
mount** until its pending set is terminal; fail them with `EIO`. Continue
bounded status resolution and allow diagnostics. Other mounts can continue
using the ordinary transaction protocol; the unresolved operation retains its
original identity and atomic validation there. Quarantine prevents a local
retry loop from creating unbounded new unknown writes, but is not a global
rollback guarantee. Read results on the affected mount must also fail `EIO`
when they depend on an unresolved local predecessor. Unrelated reads may run.

The Linux kernel's [FUSE interrupt documentation](https://docs.kernel.org/filesystems/fuse/fuse.html)
allows interrupts to race normal replies. Test the actual go-fuse/kernel
interaction; an interrupt notification is not a transaction abort certificate.
Unmount stops new admission and waits only its configured shutdown budget;
unresolved work stays durable for recovery after restart.

## Decisions

### A small interface, with resolution inside the module

Place the external seam between filesystem semantic operations and transaction
execution, not in an errno-to-retry wrapper. Retain named filesystem methods
for callers. Each constructs a typed immutable intent and calls one operation
module interface:

```go
Execute(ctx context.Context, request Request) (Result, error)
```

`Request` carries the logical operation ID, typed operands, session, predecessor
information and deadline policy. `Result` carries the original semantic result
and a durability receipt; errors distinguish definitive failure from unknown
outcome. Do not expose a generic caller-provided retry closure, mutable
transaction bytes, or a public `Retryable bool`: these leak the proof obligations
back into every caller. The module owns snapshot rebuilding, operation records,
admission/queueing, retries, route refresh and outcome resolution. FUSE retains
only kernel translation and handle bookkeeping. Transport, clock/backoff and
transaction resolution are internal seams with production and deterministic
fault-injecting adapters, exercised through the same `Execute` interface.

Read-only operations use the existing read surface; lease maintenance goes
through the module and is coalesced per handle. No caller can bypass resolution
by calling the old direct mutation path after strict mode is activated.

### Identity, attempts, and atomic result publication

1. Allocate a cluster-registered random mount session incarnation and monotonic
   request sequence. Identity is `(filesystem UUID, session incarnation,
   sequence)`, independent of inode IDs, transport request IDs and process PID.
   Allocate it once before first dispatch, and preserve it across every retry.
   FUSE's request ID alone is not durable across remounts. Fingerprint the
   canonical immutable intent, including offset, bytes digest, names, flags,
   credentials relevant to validation and target inode/handle identities.
2. Durably admit a `Pending` record in a reserved, versioned `!fs|op|` family.
   Store allocated inode/handle/intent IDs and the payload needed for recovery
   (or immutable content-addressed payload references). Admission does not
   change user-visible data. Reject ID reuse with a different fingerprint.
   An acknowledged admission survives mount-process death.
3. Before dispatching an attempt, durably associate a unique transaction ID
   and generation with the pending record. An operation has only one unresolved
   generation. A takeover reads that association and resolves it; it never
   starts a second independent transaction merely because an RPC timed out.
4. Build an attempt from fresh authoritative metadata. The complete logical
   mutation and `Pending -> Committed(original result, receipt)` occur in the
   **same atomic transaction**, with the operation record/generation in the
   validated read set. The session's admission fence must also be validated.
   Two executors cannot both publish an effect. Returned create inode/handle,
   accepted byte count and original result attributes are retained; success
   reconstruction must not inspect a now-replaced path.
5. On definitive abort, first resolve/clean up the transaction's participants,
   then durably advance the generation and rebuild. A delayed old attempt is
   rejected by the record version/generation predicate. Genuine semantic
   failures become terminal `Rejected` records, with the validation evidence
   needed to serialize the failure against concurrent mutations. Internal OCC
   aborts are attempts, not terminal logical results.
6. On an unknown outcome, query the recorded transaction decision through an
   authoritative read/resolver. A missing operation result, absent file,
   matching file bytes, or a follower's stale read is **not** an abort proof.
   Transport retransmission must carry the same transaction ID and obey that
   protocol's dedup rules. If the transaction layer lacks this capability,
   implement and verify it before enabling logical retry; do not assume it.
7. Cancellation competes transactionally with commit on the same pending
   record/generation, publishing a terminal `Cancelled` tombstone only after
   the attempt is resolved or atomically fenced against commit. An old attempt
   cannot commit after that terminal state. A lost cancellation response also
   requires resolution. No quorum means cancellation is not yet proved.

Single-group deployments can publish result and effect in one Raft-applied
transaction. Multi-group deployments require the record to participate in the
existing atomic transaction decision along with **all** effects; a receipt
cannot become visible independently of participant commit recovery. A durable
commit decision is sufficient to prohibit replay but is not permission to
return success until the promised read/durability visibility is established.
The module must expose neither a partially committed effect nor an early
success from just the operation-record group. Existing transaction behavior
must be audited against these requirements in milestone 1.

Use a stable operation-record partition keyed by session, independent of file
home movement. This may add a cross-group participant even for a formerly
single-home request. Measure that cost; co-locating optimizations require a
stable lookup indirection and the same migration fencing before acceptance.
Do not relocate a record implicitly when its inode moves. Namespace intents,
usage deltas and orphan transitions are part of the same logical effect;
post-commit physical reclamation is separately idempotent and cannot change a
successful rename/unlink into an uncertain user-visible mutation.

### Failover, retention, and recovery

All terminal records, pending generations, session fences and recovery payloads
must be Raft-replicated and included in snapshots/restores. Leaders do not
keep dedup correctness in a local cache. After election, resolve old pending
transactions against their durable decision before serving duplicate attempts.
No new timestamp is issued by a follower; preserve the existing leader/read
voucher discipline when rebuilding snapshots.

A live mount acknowledges terminal results with a contiguous sequence
watermark plus bounded gaps after completing its FUSE reply attempt. An ACK
means the mount will never resubmit that identity; it does not prove that the
application received success. Compact full acknowledged results into a durable
reject-before watermark. Requests at or below it are rejected as retired,
never executed again. Keep full unacknowledged terminal results for the session
lifetime so a lost reply can still be resolved.

Bound resource use with per-session pending/result byte quotas. Reserve record
and payload capacity before admission; reject new work with `ENOSPC` if the
operation journal cannot retain correctness state. Do not evict unresolved
records under pressure. Session closure is a durable fence: resolve or cancel
all its pending work before deleting detailed results. Retain a compact retired
session tombstone (or an equivalent monotonically fenced registry) so arbitrarily
late packets cannot resurrect an expired ID. Time-based TTL alone is unsafe.
A future tombstone GC protocol needs a proof that admission cannot reuse that
incarnation; it is not part of initial rollout.

Associate sessions with a stable configured mount-owner identity and require
authorized, durable takeover of that identity before fencing a previous session;
an unrelated live mount must not be fenced merely because it shares a host or
path. After crash/remount create a new incarnation, fence and resolve the old
mount's pending operations before admitting replacements. Recovery may finish a
committed transaction or cancel an uncommitted one, but must not spontaneously
start a fresh user mutation generation after the originating mount is gone.
The operation journal is not an application syscall journal: success lost at
process death cannot be delivered to a dead process. Expose unresolved/late
committed outcomes to operators without replaying them.

## Operation-specific rules

| Operation | After a proven attempt abort | After uncertain dispatch / successful duplicate |
| --- | --- | --- |
| Create / mkdir / mknod | Revalidate parent/name/type/permissions; retain allocated IDs for this logical request | Return recorded created inode/handle exactly once. Never infer success from `EEXIST`. Preserve `O_EXCL`; genuine nonexclusive collision retains kernel revalidation path. |
| Rename / unlink / rmdir | Re-read all source, destination, parent, replacement and emptiness predicates | Resolve original result. Never reinterpret `ENOENT` as completed, and never rename a newly recreated source as replay. Existing cross-directory `EXDEV` remains. |
| Fixed-offset write | Re-read chunks, inode size and home epoch; patch the immutable original byte range into the new snapshot | Dedup against original result; never overwrite later bytes by replay. Account bytes/usage exactly once. |
| Truncate / setattr | Rebuild from current metadata and validate inode/home; preserve requested size/attributes | A repeated truncate could erase intervening extension. Resolve by ID, including cleanup intent; no blind replay. |
| Open | Retain one allocated handle; validate inode/ref fence and lease | Recover original handle, not a second lease. A genuine lookup/open replacement race remains a separate path-revalidation problem. |
| Read / lookup / getattr / readdir | Refresh route/snapshot only when required; do not assemble a read from incompatible snapshots | Pure reads may be reissued, but retain directory-cookie snapshot semantics. Resolve mutating lease refresh separately; a new lease cannot resurrect a reaped handle. |
| Flush / fsync / fdatasync | Resolve predecessor mutations and lease maintenance; wait for required receipts | Repeat a barrier on the same captured predecessor set, not the writes. Never declare success while earlier local writes are unresolved. |
| Release / lease renewal | Serialize/coalesce per handle; make release terminal and fence later renewal | Resolve duplicate release/renewal by ID. Do not ask applications to retry `close`; kernel release has no useful error return. Surface errors through flush/fsync and telemetry. |

Multi-step create/open/truncate must have explicit suboperation identities and
predecessors where the kernel issues separate FUSE requests. Do not claim that
one identity magically spans an entire `open(O_TRUNC)` syscall. Validate the
actual kernel ordering in mounted tests.

`O_APPEND` is not repaired by conflict retries. Current kernel-selected offsets
can be stale across mounts. A later append design must select EOF inside the
same deduplicated transaction as the write and retain the chosen offset/count,
including the distinction between append writes and explicit-offset writes.
Until then retain the documented single-writer limitation, and do not advertise
general concurrent-append safety as an acceptance result of this proposal.

## Ordering, durability, and SQLite

Within one mount, serialize mutations of the same inode and namespace parent
through a bounded fair queue; take multiple local queue keys in inode order.
Track handle predecessors for flush and inode predecessors for fsync, so fsync
includes all earlier accepted local writes to that inode across handles and
required inode metadata. Include already committed remote changes through the
authoritative inode snapshot; do not promise to wait for concurrent remote
requests that have not yet committed. Concurrent unsynchronized requests
may serialize in either order; completed-before-invoked operations must preserve
that real-time order. Unrelated inodes proceed concurrently. Local queueing is
an optimization: across mounts, transaction validation and atomic commit enforce
ordering. Never rely on a process mutex for distributed exclusion.

An unresolved operation blocks dependent operations; do not release the queue
and let an fsync acknowledge a hole. Session/generation fences, open-reference
fences and home epochs must be validated at commit, including races with lease
expiry, reaper, unlink, migration and failover. A lease that expired and was
reaped cannot be renewed as if its handle remained valid. Report stale-handle
failure (`ESTALE` in strict mode) and require reopen; do not silently attach the
handle to a different inode. Queueing must not prevent timely lease maintenance;
coalesce refreshes and prioritize renewals before their safety margin expires.

Fsync captures a finite predecessor set and waits for each committed receipt
and its required storage durability condition, rather than waiting indefinitely
for future writers. Receipts identify every participating group and sufficient
commit/apply/durable position. Fdatasync can share the stronger fsync path
initially. Directory fsync must cover prior namespace mutations; replace the existing
raw FsyncDir unconditional success before advertising directory barriers. A write-through design may discharge most receipt waits immediately,
but the implementation must prove Raft/WAL/store crash durability and error
propagation, rather than treating `GetAttr` as that proof. No success may hide a
previous failed or unknown write. Sticky unresolved/writeback errors must be
reported by subsequent barriers until resolved; release does not erase them.

SQLite rollback journals depend on lock exclusion and ordered journal/database
flushes, as described in SQLite's official
[locking documentation](https://www.sqlite.org/lockingv3.html) and
[atomic commit documentation](https://www.sqlite.org/atomiccommit.html).
Eliminating OCC `EAGAIN` is necessary application compatibility work but is not
sufficient evidence of these guarantees. Validate single-mount, multi-process
PERSIST/DELETE journal workloads with `synchronous=FULL`, actual byte-range
lock contention and crash recovery. Keep one database per mount in the current
Jepsen baseline. A shared database across mounts remains unsupported until a
separate distributed lock/fencing and cache coherence design is implemented;
WAL/shared-memory behavior is also outside this proposal. Do not map genuine
SQLite lock contention to backend OCC retries, or interpret `SQLITE_BUSY` as a
filesystem bug. Do not add application sleeps to mask `SQLITE_IOERR`.

## Alternatives and tradeoffs

| Alternative | Assessment |
| --- | --- |
| Return `EAGAIN` and document application retries | Smallest change, but ordinary tools and databases do not implement the needed outcome protocol. Rejected as the target semantics. |
| Retry every matching error in the adapter | Easy to test superficially; corrupts order after lost replies, repeats rename/truncate, and mistakes stale routing for aborted commit. Rejected. |
| Retry only proven aborts, return `EIO` for all ambiguity | Safe incremental milestone if the abort proof is end-to-end. Useful before the ledger, but lower availability after failover and not the complete proposal. |
| Lock all mutations in each mount | Reduces local lease conflicts, does not coordinate other mounts or solve ambiguity. Use only as a bounded scheduling optimization. |
| Distributed per-inode locks / single filesystem leader | Can reduce conflicts but adds lease fencing, hot spots and cross-directory lock ordering; still needs outcome identity after lost responses. Not a substitute for the ledger. |
| Dedup only transport transaction bytes | Prevents duplicate submission of one attempt, but cannot re-read/rebuild an aborted attempt or retain the logical create/handle result. Required lower-level support, insufficient alone. |
| Operation record atomically committed with effects | Chosen. Adds storage, admission/attempt round trips and potentially another shard participant; makes the proof and fault tests explicit. |

Initial implementation prioritizes correctness over minimum round trips. Do
not batch away admission/attempt publication until a model and fault tests show
that old attempts, cancellation and failover remain fenced. The hot-directory
usage counters and lease keys may still cause contention; expose that cost and
address it separately rather than weakening OCC validation.

## Observability and operational controls

Add counters by bounded operation kind and reason for definitive OCC aborts,
stale-home refreshes, routing retries, unknown outcomes, resolved commits,
resolved aborts, late commits, deadline failures and admission rejection. Add
histograms for total request/queue/resolution latency and attempts per request;
gauges for pending operations, oldest pending age, result/payload bytes,
quarantined mounts and retired-session count. Keep inode/path/session/op IDs out
of metric labels. Sample structured logs with those IDs, attempt generation,
transaction ID, home epoch, participant groups and final outcome; do not log
file contents. Expose a read-only operation-status diagnostic by identity.

Alert immediately on unknown outcomes/quarantine and corruption, and on sustained
retry rate, queue saturation or retention growth. A fault run must show whether
it failed due to contention, stale placement, unknown commit or unavailable
quorum; a single `EAGAIN` counter is insufficient. Admission limits protect
memory and journal disk, not silently truncate retained results.

## Rollout and rollback

1. Add typed, versioned transaction outcomes and capability negotiation while
   retaining legacy behavior. Prove all local/forwarded paths preserve outcome
   identity and unknown status. Legacy textual errors never certify non-commit.
2. Deploy servers that understand the new operation records, session fences and
   transaction identity to every relevant participant. Keep strict mounts off
   until feature negotiation confirms the entire filesystem routing set can
   enforce them. Do not fall back to unsafe legacy retry on mixed versions.
3. Enable strict semantics on a test filesystem, then a canary mount with bounded
   workloads. Feature mode is explicit and observable. Existing legacy mounts
   may coexist only if they preserve transaction isolation and cannot bypass
   the new ledger/session invariants; otherwise require a quiesced cutover.
4. Quiesce, drain/resolve pending operations and remount to change mode. Update
   `docs/filesystem.md` and error expectations, replacing application-retry
   advice with the contract above. Preserve existing on-disk inode/data formats;
   add versioned reserved metadata. Backup/restore must retain the ledger and
   fences together with data; the existing logical backup excludes filesystem
   keys and is not a migration/rollback mechanism.
5. Rollback can disable new strict admissions on a compatible binary while
   retaining resolution and readers for all new records. Never downgrade to a
   binary that ignores fences or deletes new records while pending attempts
   exist. Resolve/cancel pending work and retire sessions before any supported
   downgrade; retain a verified cold backup for disaster recovery. Quorum loss
   can block a safe downgrade even though request waiting remains bounded.

## Milestones and acceptance evidence

M1 and M2 journal infrastructure is implemented. FUSE requests continue to use
the existing mutation path until M3, where migration must preserve the new
participant receipt rows. Each implementation change should carry its
regression tests.

- **M1: Outcome model and proof obligations.** Audit local/forwarded single- and
  multi-group dispatch, transaction decision retention, leader failover, storage
  sync and error propagation. Specify typed `Committed`, `Aborted` and `Unknown`
  outcomes and operation-record codec. Model duplicate executors, delayed
  messages, cancellation, session fencing and cross-group recovery. Gate: every
  permitted retry has authoritative non-commit evidence, and no reachable trace
  applies a logical effect twice. If the current transaction resolver cannot
  supply durable attempt identity, implement that prerequisite first.
- **M2: Durable operation module.** Add identity, admission, generations, atomic
  effect/result publication, resolution, cancellation and retention. Exercise
  the production interface with a deterministic fault adapter. Gate: duplicate
  calls return the original result, changed payloads fail, unresolved records
  survive restart/snapshot, and retired identities never execute again.
- **M3: Filesystem integration.** Convert mutation categories and lease
  maintenance, add bounded scheduling/deadlines, preserve kernel create
  revalidation and partial-write handling. Gate: all supported operations under
  contention succeed or return genuine semantic errors with no application
  retry loop; sustained overload/outage produces the specified bounded errors.
- **M4: Ordering and barriers.** Track receipts/predecessors, verify durability
  and directory barriers, then validate the bounded SQLite scope. Gate: no
  successful fsync can overtake an unresolved preceding write, acknowledged
  data survives the specified crash model, and genuine locks remain distinct
  from OCC conflict handling.
- **M5: Fault campaigns and rollout.** Run race/unit suites, required Linux FUSE
  mounted tests and Jepsen, benchmark record and retry costs, document results
  and canary limits, then enable strict mode. A skipped mount test is not a pass.

### Remaining work after the bounded legacy-path changes

The merged baseline forwards definitive write conflicts, retries several
filesystem service operations within bounds, tracks local FUSE predecessors,
and tests mounted SQLite PERSIST/DELETE workloads.
This branch adds a transaction-primary generation fence and optional
journal-backed paths for fixed-offset FUSE writes, metadata-only setattr,
file unlink, empty-directory removal, and same-directory rename.
The adapter retains their successful operation IDs per inode.
Its flush and fsync paths resolve those IDs and check participant proofs.
Directory fsync checks the removal and rename receipts recorded for that parent.
The default mount path still uses the legacy mutations.
Other mutation categories lack journal-backed planners.
`Service.Fsync` still uses an attribute read after the adapter checks the
receipts for its supported journaled operations.
Other namespace mutations still have no durable directory receipt.
The baseline and optional-path tests do not satisfy the strict-mode gates
below.

The current Raft path persists Ready entries through `persistReadyToWAL`
before applying them.
The pinned etcd WAL calls sync for a Ready containing entries.
The Pebble FSM apply path defaults to `pebble.Sync`; its optional no-sync mode
depends on replay from the synced Raft WAL.
The local Pebble close/reopen test recovers a committed write and its receipt.
It does not exercise a Raft process or node crash.
This code audit supports the receipt approach but does not close the M4 gate.
Crash tests must cover failure between WAL persistence, FSM apply, proof
visibility, FUSE reply, and the next fsync for every participant group.

Complete the remaining work in dependency order:

1. **Durable attempt decision (M1/M2 prerequisite).** Persist a stable
   transaction identity for each publication attempt and expose an
   authoritative committed/aborted/unknown decision across local, forwarded,
   and multi-group dispatch. Retain the decision long enough to fence delayed
   traffic after leader change or restart. Implement `AdvanceGeneration` only
   when abort is proven; absence of a row, timeout, OCC error text, and lost
   response remain unknown. Test lost replies before proposal, after proposal,
   after primary decision, and after secondary application, including restart.
2. **Journal-backed FUSE mutations (M3).** Give each mounted request a stable
   operation ID, configured owner and session incarnation. Add typed planners
   and stored results for write/partial write, truncate/setattr, create/open,
   mkdir, unlink/rmdir, rename, release and lease maintenance. Route their
   effects and semantic rejections through admission, attempt, publication,
   resolution and acknowledgement. Resolve an unknown attempt under the same
   identity; never rebuild it as a new logical request. Preserve create
   revalidation, short counts, cancellation and bounded queue/deadline errors.
   Keep participant receipt rows available across home migration and session
   recovery. Mounted contention and fault tests must cover every mutation class.
3. **Durable barriers (M4).** Associate successful mutations with participant
   receipts and their predecessor order per file or directory. Make flush,
   fsync/fdatasync and directory fsync wait for all relevant earlier operations
   to become terminal. Audit the Raft/WAL/store sync contract and verify a
   durable receipt before returning fsync success. A read-only attribute fence
   and a local queue drain are insufficient. Test process and node crashes,
   journal-before-database ordering, and unknown predecessor errors.
4. **Fault campaign and rollout (M5).** Run race tests, the full Go suite and
   lint, required Linux FUSE mounts, both Jepsen suites, and strict no-retry
   syscall clients. Record seeds, versions, histories, failpoints and checker
   output. Benchmark single- and multi-group latency, throughput, retry cost,
   fairness and retained journal bytes. Enable strict mode only after all
   acceptance rows below pass; document canary limits and rollback conditions.

The strict mode must stay disabled while any of these gates is open. Existing
SQLite PERSIST/DELETE runs establish only the stated single-mount scope; they
do not establish shared-database or WAL safety across mounts.

Required acceptance matrix:

| Scenario | Evidence required |
| --- | --- |
| Concurrent disjoint-range writes sharing one chunk | Both ranges retained after internal abort/rebuild; neither caller sees OCC `EAGAIN`. |
| Overlapping writes; old writer loses committed reply; newer writer commits | Resolving old writer returns its original count and never overwrites newer bytes. Include mount restart and leader change. |
| Create with and without `O_EXCL`; lost commit reply; replacement by another client | Recover original inode/handle; no extra lease, false collision, accidental truncate or new file masquerading as original result. |
| Rename/unlink followed by recreation of the source name | Old duplicate cannot act on the new inode; namespace and usage changes occur once. |
| Truncate then concurrent extension/write | Delayed duplicate cannot shrink the later file; chunk cleanup cannot delete live extensions. |
| Lost response before proposal, after proposal, after commit, after result publication | Correct definitive/unknown classification at every injection point, for leader and follower mounts. |
| Multi-group failure after decision but before a participant applies | No premature success, no second logical effect, eventual consistent recovery or bounded unknown error. |
| Cancellation at queue/admission/dispatch/commit/reply boundaries | No effect after confirmed cancellation; committed result wins when known; uncertain cancellation stays recorded and returns `EIO`. |
| Home migration, stale route, old leader resumes | Fresh metadata rebuild only after safe outcome resolution; old generation/session cannot commit. |
| Renewal vs keepalive vs close vs lease reaper | No spurious read/fsync `EAGAIN`, no resurrection of dead handles, no user-level close replay. |
| Deadline/attempt/queue limits and sustained partition | Bounds include queue and backoff; no goroutine leak or hot loop; quarantine and later outcome resolution observable. |
| GC/ACK gaps, quota, session closure and late packets | Retained pending records, reject-before correctness, restored snapshots reject old identities; pressure never evicts proof. |
| Large write interrupted between FUSE requests | Exact completed prefix/short count at the kernel interface; no duplicated accepted request. |
| `O_NONBLOCK` regular files | Same contention semantics as blocking files; no fabricated `EAGAIN`. |
| fsync/fdatasync/directory fsync and process/node crashes | Receipt ordering and crash persistence, including journal-before-database ordering and reported errors. |
| SQLite single mount with multiple processes | Real lock exclusion, PERSIST and DELETE recovery, `integrity_check`, committed transaction history, no filesystem-error sleep wrapper. Shared-mount DB/WAL remain excluded. |

Run `go test -race ./internal/filesystem/... ./kv/... ./store/...` plus the full
repository tests and lint for implementation milestones. Run both required
Jepsen suites where coordination/replication changes apply, and Linux mounted
tests with `ELASTICKV_REQUIRE_FUSE=1`. Extend the filesystem Jepsen workload with
strict no-retry syscall clients for contention and operation-aware fault cases.
Keep unknown writes as indeterminate history operations; never turn them into
failed/no-effect events or replay them as a new write within the same event.
Retain decisive quiescent final-read checks. Capture seeds, versions, feature
mode, failpoints, histories and checker output. Existing harness retries can
remain as a legacy comparison, but are not acceptance evidence for strict mode.

Before rollout record uncontended and hot-inode throughput, p50/p95/p99 latency,
transaction/consensus round trips, conflict counts, queue fairness and retained
bytes per operation for single- and multi-group layouts. Numerical performance
limits require a measured baseline and maintainer approval; they are not assumed
by this proposal. Safety and bounded-resource gates are mandatory regardless of
performance.

## Open questions that block implementation or wider claims

- Does the existing transaction protocol expose a stable, queryable decision for
  every forwarded/multi-group attempt, and retain it long enough to fence late
  traffic? M1 must answer from code and fault evidence, not an error string.
- Can terminal semantic rejection be serialized cheaply with its read predicates,
  and can pending generation publication reuse existing transaction metadata
  without adding a new global hotspot? Optimization must preserve the model.
- Which exact commit/apply/WAL durability receipt is valid for each backend sync
  mode? Fsync claims are blocked until audited and crash-tested.
- Which kernel/go-fuse behaviors apply to interruption, create revalidation,
  `O_TRUNC`, directory fsync and large-write short counts on supported Linux
  versions? Mounted tests must determine the supported matrix.
- How should operators inspect and clear a quarantined mount after all pending
  outcomes are terminal? Initial policy is explicit remount after recovery;
  automatic resume is a later, separately tested convenience.
- Distributed locks, cache invalidation and atomic append need separate designs
  before claiming general multi-mount POSIX or shared SQLite compatibility.
