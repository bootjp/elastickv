# Online Split activation

Status: implemented online RawKV point path; broader mutation coverage and
additional failure campaigns remain open. Builds on atomic RawKV point capture.
Existing deployments do not require a rolling upgrade or data conversion.

## Scope and default

A normal server with a persistent Pebble store selects online capture for new
cross-group jobs. Same-group splits remain catalog operations. In-memory and
legacy fixture stores do not advertise the persistent capture capability.
The first online data path accepts RawKV point entries. Any intersecting
unsupported mutation invalidates the session durably before executing the
ordinary mutation. Invalidation aborts migration rather than rejecting the
user write. It must not be possible to seal an invalid session.

## Source

A source session records group, lineage, job, generation, moving range,
configuration, start index, sequential frame count, storage budget and seal.
Activation shares an indexed transaction with the retained source tracker and
the ownership generation nonce. A lost initial response rediscovers that exact
nonce from the source capability probe before retrying.
Reapplying activation never clears invalidation, advances the start position,
or reopens a sealed session. Capture data, frame, sequence and applied index
commit atomically. Frames, including empty ones, consume the bounded budget.
An unsupported writer first commits a monotone invalidation in a synchronous
metadata-only transaction without advancing the data applied index.
Only explicitly audited non-data controls are exempt. Capture is disabled for
encrypted stores until key retention is connected to journal lifetime;
activation of encryption during capture invalidates it before proceeding.

Base uses existing all-family retained-history export. Capture starts before
selecting its timestamp. After ALL base brackets are acknowledged, replay ALL
captured frames from the start in source order, including low timestamp and
same-timestamp rewrites. This reconstructs the final MVCC state despite
independent base iterators. Physical removal and unsupported state changes
invalidate capture. No base generation is reused after a restart requiring
reconstruction; retained persistent history allows the same base pass to resume.

After preflight and catch-up, existing durable source creation fences and lock
drain run. A separate source Raft control checks generation, configuration,
validity, durable fences and absence of unresolved prepares before sealing its
last frame S. The final position is produced by the FSM, never inferred from a
runner read. The sealed source remains fenced through Commit or explicit
catalog-authorized rollback.

## Target

The first target must be fresh: before accepting base, its FSM checks that it
has no transferable live MVCC versions, including tombstones and expired
values. Runtime-only metadata is excluded by an explicit allowlist. The
nonowner ownership partition and target capture manifest prevent ordinary
writes to the moving range before cutover.
Journal imports have a dedicated bracket, source identity, sequence, digest,
and persistent receipts. Data, receipt and accepted sequence commit in one
synced batch. Reject sequence gaps, changed content under a repeated sequence,
and source identity mismatch. Earlier base values may be overwritten by the
actual later source postimage at the same MVCC timestamp.
Ready(S) requires exactly the sealed sequence and closes BOTH base and journal
imports in the target FSM. All target apply members acknowledge Ready(S)
before the root catalog atomically publishes the target descriptor and the
job's cutover result. Existing source/target ownership proofs, clock floor,
read fence, rollback and cleanup checks remain authoritative.

## Request waiting and resources

Only a proven non-admitted or proven deterministic rejected single RawKV
point mutation may wait briefly and retry routing during handoff. Never retry
unknown outcomes, transport failures, joined errors or multi-request partial
outcomes. Bound waiting by the request context, a finite handoff deadline and
a concurrent waiter limit. ACK remains contingent on ordinary Raft apply.
The runner promptly re-enters after durable online progress, while repeating
leader, namespace, ownership and configuration admission on every attempt.
A stalled job retains ordinary backoff. Journal and receipt storage is bounded
per job and removed by bounded indexed cleanup after termination.

## Validation

Use persistent real-store FSM integration tests for capture/replay/seal,
restart after invalidation, empty frames, budget exhaustion, low timestamps,
same-timestamp rewrites, changed replay content, gaps, Ready(S) late imports,
and target freshness. Exercise real etcd Raft, concurrent RawKV load, source
and target failover/restart, and cutover/abort exclusivity. Compare successful
writes at the target, not merely runner phase completion. Run focused race,
full Go tests, lint and the Jepsen harness. Linux load validation is separate
from local unit or harness passes.

## Implementation decisions

Capture stores at most 16,384 frames and 256 MiB per source session. Quota
exhaustion invalidates migration atomically with the acknowledged source data;
it does not reject that user mutation. Unsupported ordinary mutations first
persist invalidation. Physical promotion/cleanup checks concrete affected live
rows in its indexed batch; unrelated private imports do not invalidate a seal.
Authorized source cleanup requires the exact committed NONOWNER proof, matching
job, lineage, range, floor, current configuration and durable read/write fences.
Closed headers remain terminal; range tombstones reclaim frames and receipts.

An immutable source seal retains its original capture configuration. If native
membership changes after sealing, existing manifests are reattested through an
indexed target control and fresh current-member probes of the exact seal. Old
ACK cursors alone never authorize cutover. Joining members must advertise the
online protocol before their membership authorization is committed.

A runner catch-up pass processes at most 128 frames, 200 ms and 4 MiB; it
checkpoints accepted receipts once per pass. Online progress uses a 10 ms
continuation delay. Preflight completion atomically records the source head in the job manifest.
Its sequence is a fixed catch-up goal across passes, ticks and runner restarts;
new exports never move that goal. Once reached, the runner reloads and readmits
the job and samples the current source head before installing the first fence.
Observed remaining lag must fit eight frames and 512 KiB. If it exceeds either
limit, the source stays writable and a new fixed goal is persisted with OCC
guards on the job, catalog version and source route. A concurrent catalog fence
therefore cannot be overwritten by a stale goal refresh. An already installed
fence resumes without refreshing its goal. Seal captures the remaining tail.
This avoids requiring an idle tick and avoids fencing a large backlog after a
stalled runner. These are observed lag budgets, not a real-time pause guarantee
under overload, partitions or slow storage.
RawKV handoff waiting is limited to two seconds, 256 concurrent waiters and
8 MiB including waiter overhead. Cancellation before another dispatch retains
the previous noncommit proof; a dispatched attempt returns its actual outcome.

Promotion may remove the target's staged descriptor and retire its admission
guard while a rejection from the old source is still returning to ingress.
The error-path handoff hint must therefore consult matching active source
trackers across the node's local groups after the route changes. Looking only
at the route's current group loses that evidence during source cleanup. This
lookup is not write authority: whole-mutation noncommit proof, a fresh dispatch,
the same bounded waiter/byte/deadline budgets, and normal proposal/apply guards
remain mandatory. Retiring the source tracker ends this hint as well.

## Remaining scope

The online path is currently for plaintext persistent RawKV point entries and
a fresh dedicated target. Collection and transactional entries conservatively
abort capture; encrypted journal key retention is not implemented. This does
not enable the automatic scheduler or provision groups. Larger journals,
general target reuse and broader failure/performance campaigns are future work.

A target lock is a freshness violation during initial preparation. After a
source seal, transient locks keep target reattestation/Ready retryable instead
of terminalizing the job. State and receipts remain unchanged until they drain.

## Unrelated prefix mutations in captured batches

A full batch may contain a captured point request and prefix requests whose
logical footprints do not intersect any active source session. Keep the
existing per-request footprint admission. For such requests the indexed
store additionally checks every matching physical key against the capture
filter, including prior writes in the same batch, before staging tombstones.
Reject the entire atomic entry if any selected capture key would change.
The explicit unfiltered shadow primitive still rejects prefix writes. An
intersecting prefix request continues to invalidate the session before normal
application. Preserve per-request outcomes, failure latching and atomic replay.

## Maintenance latency evaluation

A 256-version/512-KiB online maintenance-page experiment did not consistently
improve successful write p99 in the exclusive Linux A/B/B/A evaluation. Keep
the existing 1,024-version/4-MiB maintenance budgets and 16-MiB scan bound.
The experiment and ordinary synchronous WAL stalls are retained in the
evidence; reduced page size alone is not a demonstrated p99 correction.
