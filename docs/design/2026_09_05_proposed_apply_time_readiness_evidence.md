# Apply-time target readiness must not read the catalog watcher

Status: Proposed
Author: bootjp
Date: 2026-09-05

Parent: [2026_06_11_implemented_hotspot_split_milestone2_migration.md](2026_06_11_implemented_hotspot_split_milestone2_migration.md)

## 1. The problem

`kvFSM.verifyTargetReadinessForRange` runs during Raft apply and resolves the
routes it checks through `currentShardRoutesForRouteRange`, which calls
`f.routes.Current()` — the route engine's *current* cached view.

That view is filled by the catalog watcher from the default group's catalog, over
a stream with **no ordering relationship to the target group's Raft log**. Two
voters applying the same entry at the same index can therefore hold different
catalog versions: the leader may already see the cutover descriptor while a
follower is still behind.

This is the failure mode `CLAUDE.md` records for the `8668bdce` revert — apply
must not depend on state that is not replicated through the log it is applying.

## 2. Why it is not merely a stale read

`targetReadinessStatesSatisfied` returns `ErrRouteCutoverPending` when the routes
it can see do not yet satisfy the replicated readiness guard. Before this change
that was an ordinary FSM response, so the Raft engine advanced the applied index
and the lagging voter **skipped a committed write** while the leader applied it.
The divergence was silent and permanent: nothing re-delivers the entry.

## 3. What has already been done

`applyRequest` now converts `ErrRouteCutoverPending` into a halt
(`ErrTargetReadinessApply`). A voter that cannot prove readiness stops instead of
advancing past the entry, so the divergence becomes a loud, recoverable stop
rather than silent data loss.

This is a containment fix, not a correctness fix. It converts "wrong answer" into
"no answer", which is the right trade for an apply path, but a follower that is
merely behind on catalog delivery will now halt where it previously (wrongly)
continued. That is strictly safer and strictly noisier.

## 4. Options for making apply deterministic

### 4.1 Derive readiness from the entry (preferred)

Requests already carry `observed_route_version`, and the base branch established
the pattern: `routeFloorSnapshotForRequest` resolves
`f.routes.SnapshotAt(observedVer)` and fails with `ErrComposed1VersionGCd` when
that version is no longer retained. Readiness could resolve the same way, making
the verdict a pure function of (entry, replicated readiness guard, retained
catalog history).

Open questions:

- Unpinned entries have no observed version. Do they skip the readiness check
  (matching how the write-floor check treats them), or is pinning mandatory once
  a migration is armed?
- `SnapshotAt` needs the version to still be retained. What is the retention
  bound relative to the apply lag a halted follower can accumulate?

### 4.2 Replicate the routes the guard needs

The readiness guard itself is already replicated through the target group
(`raftEncodeTargetReadiness`). The route bounds it is checked against could be
carried in that same entry, so apply never consults the catalog cache at all.
Larger entries, but no dependency on catalog delivery order.

### 4.3 Gate on the guard alone

`targetReadinessStatesSatisfied` currently needs both the replicated guard *and*
a matching catalog view. If the guard carried enough to decide by itself, the
catalog read disappears. This is 4.2 with the bounds folded into the existing
state rather than the entry.

## 5. Recommendation

4.1, because the mechanism already exists on the write-floor path and reuses the
retention story rather than inventing one. 4.2 is the fallback if the retention
bound in §4.1 cannot be made to cover realistic follower lag.

## 6. Until then

The halt in §3 stands. It is not a substitute for this document: a cluster whose
catalog delivery lags will see followers halt, which is safe but disruptive, and
that pressure is the reason to land §4 rather than leave the halt as the answer.

## 7. Bounded native-lineage implementation checkpoint

The working implementation explores 4.2/4.3, not a watcher-history fallback.
Permanent per-range OWNER/NONOWNER certificates are separate from active
readiness guards and retention pins. The ownership certificate, job retirement,
guard removal, and applied index share one store lock/batch. MVCC snapshots emit
V6 only when ownership certificates exist; the MVCC-to-Pebble streaming path,
native/SST snapshots, and reopen preserve them.

The shared serving view prevents a read from observing an old catalog floor
with a newly cleared guard. NONOWNER does not reopen because a cache advances.
New requests lacking an exact ownership proof, or spanning a proved and
uncovered interval, temporarily receive a whole-request retry before mutation.
Independent requests coalesced into one Raft entry retain independent results.
Only a matching persisted COMMIT/ABORT resolution may drain through this gate.
A remote primary reference alone is not a local primary-intent participant.

This checkpoint is not a complete migration protocol and is not merge-ready:

- The runner must bind certificates to actual committed descriptors using a
  root-leader applied read barrier. VerifyLeader alone does not wait for apply.
- Source guard clear must use the new atomic ownership replacement; rollback
  must first commit its actual catalog version while retaining the guard.
- All proposal ingress must share admission and stamp an actual proved route
  version. The final apply check is still required for queued requests.
- An OWNER certificate currently proves exactly C. A trusted unchanged-
  descriptor refresh/attestation is required for unrelated catalog increments;
  blindly accepting every later observed version is not valid evidence.
- Target promotion/metadata clear needs a retained cleared OWNER certificate,
  with data-ready/floor evidence, and every new voter must attest support.
- The current record does not identify a verified Raft generation. External
  and fresh restore of migration-bearing payloads must refuse before publishing
  a new ConfState, including physical offload and cold-skip paths. Proof-free,
  epoch-only, and empty-metadata restores must continue to support ordinary KV;
  copied configuration indices are not migration lineage evidence.
- The earlier target-only apply-route prototype is not the completed protocol:
  its future-version predicate and participant footprint still need replacement
  by durable actual-C proofs and context-helper integration.

These remaining requirements must be completed and tested before publication.

The checkpoint audit additionally identified three unresolved interactions:
an unrelated range advancing the group floor can invalidate an older exact-C
OWNER proof; the pin check currently runs before establishing whether a request
intersects migration evidence; and helper-level matching-resolution admission
does not prove the subsequent target/composed/floor path or durable idempotent
retry succeeds. Separate immutable operation receipts from refreshable replicated
serving attestations, and test the complete apply path before claiming closure.

## 8. SOURCE creation and public admission checkpoint

SOURCE tracking uses the actual root descriptor after a root-leader applied
barrier. Its inherited write floor is separately marked known; a verified zero
is not the tracker's positive retention/HLC floor. The tracker is applied first
in the data log, then a configuration-bound SOURCE acquisition records its own
applied boundary. Until that acquisition, creation retries deterministically.
The immutable acquisition receipt retains this boundary across later unchanged-
descriptor REFRESH attestations and native snapshot/reopen. Source fences still
deny creation, while authenticated persisted COMMIT/ABORT resolutions drain.

Operational evidence requires generation, descriptor identity/hash, operation
receipt, configuration and applied boundary. The decoder remains compatible
with older records, but such unknown records cannot authorize their range.
Unrelated ordinary KV does not require migration evidence. SOURCE controls use
explicit opcode mode 6 and protocol 2; every voter must attest support before
the runner submits them. An epoch change invalidates SOURCE creation until a
new all-voter, configuration-bound attestation.

The common TransactionManager proposal path checks final requests after queue
waiting and preserves independent results for coalesced requests. ShardStore
scan/physical/exact-version entry points and coordinator read-key checks share
the durable serving gate. Apply repeats admission independently of proposal.
Unpinned or incompletely covered affected requests retry before any mutation;
there is no watcher or fabricated floor-zero fallback. Production wiring and
the authenticated cross-leader refresh consumer remain integration gates.

A later split of a certificate-bearing route can change the retained left
descriptor hash even when that prefix keeps its owner. Unchanged-descriptor
REFRESH cannot repair that case. A durable pre-effect catalog mutation gate or
an explicitly replicated retained-prefix reacquisition is still required;
this checkpoint does not claim sequential split availability or merge readiness.
