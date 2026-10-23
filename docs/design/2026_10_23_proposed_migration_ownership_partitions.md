# Replicated ownership partitions for subsequent range splits

Status: Proposed; interface checkpoint only, not implemented or activation-ready
Author: bootjp
Date: 2026-10-23

This extends the bounded native-lineage ownership work in
[apply-time readiness evidence](2026_09_05_proposed_apply_time_readiness_evidence.md).
The Milestone 2 design's historical Implemented label does not mean the current
PR #1090 ownership integration is complete. That PR remains Draft and production
activation remains closed pending implementation, acceptance, review and CI.

## Contract

The canonical Go interface is
`store/migration_ownership_partition_types.go`. No store implements it at this
checkpoint. Immutable per-job Acquire/Clear receipts remain keyed by job ID.
The active, non-overlapping serving frontier has independent partition IDs;
several children can retain the same immutable job receipt. Never flatten the
job ledger into child records or deduplicate active children by job ID.

Same-owner partition controls preserve the complete prior union, role, known
floor, staged-data lineage and protected SOURCE boundary. References identify
the exact prior attestation, including every overlapping negative proof. A
NONOWNER cannot become OWNER through partition or unchanged-descriptor refresh.
Only an ordinary newer guarded acquisition with data-ready evidence can reopen
ownership. Semantic mismatches deterministically retry before effects.

Root geometry transactions atomically persist a manifest with their descriptor,
catalog-version, route allocation and working-source mutations. Only a manifest
reloaded after the root leader's linearizable applied-read barrier can attest
actual committed C. Data leaders install it in their own logs with generation
and applied-configuration preconditions. Apply never consults a catalog watcher
for authority. Historical NONOWNER groups discover retained manifest chains by
old descriptor identity through the authenticated refresh consumer, including
after job-history GC. They need not be online for a same-owner metadata split;
their old negative proof continues denying until reconciliation/acquisition.

Before a cross-group suffix move, commit and replicate a full same-owner
partition into retained left and working moving child. SOURCE tracker, catalog
fence, write floor, cutover and rollback then operate only on that moving child.
The job records its working source durably so retries do not allocate children
again. Same-group SplitRange uses this partition protocol too; subsequent splits
are supported, not permanently refused. An abandon may leave the harmless
same-owner metadata split but cannot lower floors or restore a negative owner.

## Persistence Hook Map

The store worker owns the following implementation hooks; the runtime owner
does not edit these files concurrently:

| File | Required hook |
| --- | --- |
| `store/migration_ownership_partition*.go` | New raw container `!migstage|ownership_partition`, codec v1, canonical identities/references, validators, atomic memory/Pebble transition and immutable receipts. |
| `store/migration_ownership.go` | Add `Partitions MigrationOwnershipPartitionState` to the coherent serving view; couple existing guarded Acquire/Clear/Refresh to the frontier in the same lock/batch as the old job ledger and applied index. SOURCE uses this same acquire path. |
| `store/migration_ownership_evidence.go` | Preserve full origin, known-floor and SOURCE data-log-boundary validation. Do not globally reject decodable legacy job records. |
| `store/mvcc_store.go` | New in-memory container, V7 trailer only when needed, V1..V6 compatibility, coherent snapshot capture and atomic restored-tree/metadata publication. |
| `store/snapshot_migration_metadata.go` | Validate on reopen and stage the exact new raw key in streaming MVCC-to-Pebble restore before publication. |
| `store/lsm_store.go` | Pass the V7 metadata into the temporary restore DB; native/SST carry the raw key and must validate it. Preserve encrypted snapshot plumbing. |
| `store/migration_versions.go` | Classify/exclude/protect the new raw metadata key in export/import/cleanup. Do not confuse decoded MVCC user keys with raw metadata. |
| `store/snapshot_external.go` | Refuse nonempty frontier or receipts before tree/DB publication under external rebase, including receipt-only evidence; ordinary proof-free restore remains compatible. |
| `internal/backup/snapshot_reader.go` | Mirror exact raw-key filtering for the standalone offline reader. This is the store worker's single assigned non-store hook. |

The new container is independent of `!migstage|ownership`. Both native histories
must survive reopen, MVCC streaming, native/SST/encrypted snapshots and voter
join. External restore must not reuse copied configuration epochs as lineage.
The geometry owner maintains protocol auth, Raft controls and public/FSM
admission in `proto`, `distribution`, `adapter` and `kv`; parent owns runtime
membership wiring. Size-policy changes belong to their separate owner.

## Helper Interface Handoff

The worker implements these helpers alongside the types (not present yet):

```go
MigrationOwnershipPartitionReferenceFor(MigrationOwnershipPartition) (MigrationOwnershipPartitionReference, error)
MigrationOwnershipLegacyReferenceFor(MigrationOwnershipState) (MigrationOwnershipPartitionReference, error)
MigrationOwnershipPartitionIDFor(MigrationOwnershipPartitionTransition, int) ([]byte, error)
MigrationOwnershipPartitionReceiptIDFor(MigrationOwnershipPartitionTransition) ([]byte, error)
MigrationOwnershipPartitionEvidenceValid(MigrationOwnershipPartitionState, MigrationOwnershipPartition) bool
MigrationOwnershipPartitionMetadataKey() []byte
IsMigrationOwnershipPartitionMetadataKey([]byte) bool
```

IDs/digests are domain-separated SHA-256 with canonical encoding, never ad hoc
JSON hashing by a caller. Partition IDs survive refresh; receipt IDs bind exact
operations. Legacy references require complete operational old evidence and
hash the entire current job state. Complete old positive and negative coverage
must be validated before adoption; unknown legacy records cannot seed authority.
The internal-only JobOperation receipt kind can be produced only by existing
validated Acquire/Clear operations, not external partition controls.

## Runtime Capability

The geometry capability is `cap_migration_partition_v1`, version 1. The planned
static probe is `ProbeMigrationStateResponse.ownership_partition_protocol = 10`;
field allocation must be kept disjoint from the size policy owner. The planned
option is `WithInternalMigrationPartitionProtocolReady(func() bool)`.

Static implemented support must be observable on a new learner before it has
an applied configuration. Operational ownership readiness, guard installation
and ACKs independently require a real nonzero applied configuration, generation
and all-member support. Static support never overrides production activation.

## Acceptance Gates

The independent harness owns existing-public-interface regressions for both
same-group resplit and cross-group suffix migration of a previously migrated
range. Retained-left reads and raw/1PC/PREPARE writes must survive the split and
unrelated later C without descriptor-hash guesses or floor changes. Also require
negative-owner safety, real move-back, full matching COMMIT/ABORT drain, exact and
conflicting retries, leader/config changes at each phase, three voters with
different caches, batch failure, job GC, reopen and every native snapshot path.

No focused test, type compilation or capability advertisement completes these
gates or declares PR #1090 merge/deployment ready.
