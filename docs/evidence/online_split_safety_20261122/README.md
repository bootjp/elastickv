# Online RawKV split safety

The implementation reviewed here is PR #1544 at
`ec15b79b897fcaad3dff2baf3f439b70f06af348`. The reviewed scope is one
cross-group split generation for persistent plaintext RawKV point operations.
The executed Jepsen scenarios used three independent persistent processes and
two real etcd Raft groups over authenticated TLS.

## What the checks cover

| Obligation | Implementation boundary | Verification |
| --- | --- | --- |
| A source mutation and its capture postimages are durable together, with monotone invalidation for unsupported changes | `CaptureOnlineMigrationEntry` and `publishOnlineMigrationFrame` in the [source store](../../../store/migration_online.go), with the indexed transaction in [raft_entry.go](../../../store/raft_entry.go) | Existing Go atomicity and restart tests establish the storage boundary. Lean and TLC use it as a transition premise. |
| Target data, receipt and cursor advance together in source order | `validateOnlineImport`, `onlineImportReceipt` and `stageOnlineImport` in the [target store](../../../store/migration_online_target.go) | TLC checks the received prefix. Lean derives the exact prefix from append, duplicate-receipt and restart transitions, then proves reconstruction for an arbitrary number of frames. |
| Source sealing fences the moving range. The target closes imports on the complete sealed tail. | `sealOnlineSplit` in the [runner](../../../adapter/split_job_online.go) and `validOnlineSourceSeal` / `validOnlineTargetReady` in the [target store](../../../store/migration_online_target.go) | Lean proves equality for every key/timestamp cell at cutover, including tombstones and same-timestamp rewrites. TLC rejects publication with a missing tail. |
| Only the correct owner may acknowledge writes. A canceled generation cannot publish. Cleanup happens after durable ownership change. | `applySplitJobCutoverBarriers`, `commitSplitJobCutover`, and `verifySplitJobCleanupSnapshot` in the [runner](../../../adapter/split_job_runner.go), with [replicated serving guards](../../../kv/fsm_migration_serving.go) | Lean proves the abstract phase properties. TLC tests stale-owner ACK, invalidation and early-cleanup failures. Jepsen checks values on both sides of the split after a leader pause and recovery. |

An image in [OnlineSplit.lean](../../../lean/OnlineSplit.lean) is a function
from a key/timestamp pair to a live value, tombstone or absence. Independent
base iterators must agree with the source at cells that never receive a
postimage. At a touched cell, replay converges to the last postimage
regardless of the value selected during base export. That covers equal and
out-of-order MVCC timestamps while preserving source frame order. The proof
assumes that the indexed source and target writes implement the respective
atomic transitions and that the serving guards enforce the modeled write
authority. It is a proof of this protocol abstraction, with source mapping
and executable tests for those boundaries. It leaves Go refinement, Raft
itself, crash-disk semantics, repeated generations and encrypted values
outside the theorem. `make lean-check` runs all proofs with warnings as errors.

[MCOnlineSplit.tla](../../../tla/online_split/MCOnlineSplit.tla) explores three
version cells, up to three source frames, two configuration epochs and a
post-cutover write. A duplicate receipt, dropped reply or controller restart
can leave durable state unchanged. The model assumes the same store atomicity
and current-owner serving guards. It checks safety and ownership monotonicity.
It places no deadline or fairness requirement on migration completion.
`make tla-check` includes the strict model, an implementation-shaped
revalidation model, and six counterexample configurations for out-of-order
import, missing tail, changed configuration, early cleanup, stale-owner ACK,
and publishing an invalidated generation.

## Configuration revalidation and catalog publication

`confirmOnlineCutover` probes each side at its current applied-members
configuration and revalidates the snapshot. `commitSplitJobCutover` runs a
later transaction over the catalog route, version and job keys. It has no
CAS against either data group's configuration index.

The stricter model requires the certificate to remain current until
publication. The implementation-shaped configuration permits
`Confirm(epoch=1) → ChangeConfiguration(epoch=2) → Publish(proof=1)`.
TLC produces a counterexample to `PublicationHasCurrentEvidence` for that
ordering. The same implementation-shaped configuration preserves data
and phase invariants under the modeled premises, which include local
serving guards and inherited durable Raft state on membership changes.

A Go diagnostic at the real runner's coordinator dispatch boundary injected
an applied-configuration index change in the persistent-store FSM fixture.
It observed Ready at index 9 and then index 71 at catalog commit. Cutover
version 4 published and the migrated value was readable. The fixture uses
simulated configuration advancement, so the observation establishes the
check-to-publication window. A real membership-change run is needed to
exercise the new apply member and stale ingress paths.

## Jepsen

The [online leader-failover case](../../../jepsen/SPLIT_REGRESSIONS.md) requires
a partial persisted preflight checkpoint with online capture active, then
ACKs for a moving-key overwrite and delete. The old catalog/source leader
must stop, a different process must become leader in a strictly greater
term, and the new leader must observe the same unfinished generation
without losing completed brackets. After recovery the job must reach DONE
with the imported and sealed sequences equal. A post-cutover RawPut must
also receive an ACK. The checker requires all 12 final reads through three
ingresses, including the retained value, the overwritten value, the missing
deleted key, and the post-cutover write. It rejects unknown outcomes for the
required mutations and missed injection windows.

Further real-cluster cases are described below.

1. Add or promote a target apply member between the final confirmation and
   catalog CAS, and isolate an ingress with an old route. Audit acknowledged
   writes and deletes through the old ingress and the new member after healing.
2. Kill and restart the target leader after durable Ready and before catalog
   publication. Repeat after publication and before source cleanup. Verify
   the immutable seal, no restored tombstones, and the post-cutover owner.
3. Drop a response after target import commit and re-deliver the exact batch
   followed by a conflicting batch at the same sequence. Verify one durable
   result, no cursor jump, and the complete final history.
4. Race abandon with publication, then re-split the moving child after
   recovery. Verify a single terminal decision and rejection of delayed
   old-generation cleanup and import messages.

## Results

- The existing preflight follower-stop scenario retained all 46 brackets and
  passed all six final reads. The online leader-failover scenario passed three
  times with terms 2→3 and each original leader represented (n3→n2, n1→n3 and
  n2→n1). All runs preserved eight completed brackets at failover, reached DONE
  with the imported sequence equal to the seal (2, 1 and 1), and passed all 12
  final reads per run. The last run used the final history checker and helper.
- The full Jepsen harness passed 443 tests and 6,199 assertions. The gRPC helper
  suite and focused Go race tests passed.
- Lean 4.24.0 accepted all proofs. The printed axiom lists contain only
  Lean's standard `propext` and `Quot.sound`, when applicable.
- TLC completed 1,033,826 distinct states in the strict model and 1,048,984
  in the implementation-shaped revalidation model, with empty work queues
  and no violations of their configured invariants. All six counterexample
  configurations failed on their nominated invariant and `make tla-check`
  passed the entire suite.
