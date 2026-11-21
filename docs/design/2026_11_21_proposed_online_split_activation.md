# Online Split activation

Status: reviewed implementation plan. Builds on the online Split proposal and
its atomic RawKV point capture implementation. Existing deployments do not
require a rolling upgrade or data conversion.

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
Activation shares an indexed transaction with the retained source tracker.
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
