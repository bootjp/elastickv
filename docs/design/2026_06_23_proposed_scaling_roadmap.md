# Scaling roadmap ownership index

Status: Proposed roadmap index
Author: bootjp
Date: 2026-06-23
Last audited: 2026-07-18 against `origin/main` and GitHub pull requests

## 1. Scope

This document owns no runtime behavior. It is the canonical ownership and
dependency index for scaling work. A behavior change belongs to the focused
design document or pull request named below, not to this roadmap.

The audit follows these rules:

1. One focused design owns one mechanism. Its implementation may use a stacked
   pull request series when the change cannot be reviewed safely as one patch.
2. An implemented document and merged implementation are authoritative over a
   proposal or historical roadmap description.
3. An open pull request is in flight, not implemented on `main`.
4. A requirement with neither a focused design nor an implementation pull
   request is explicitly unowned. This roadmap only defines the boundary of the
   missing design; it does not silently become that design.

## 2. Relationship to the 2026-06-12 roadmap

This document supersedes
`2026_06_12_proposed_scaling_roadmap.md` as the roadmap index and sequencing
authority. It does not claim that all mechanisms proposed on 2026-06-12 have
been implemented or moved into focused designs.

The 2026-06-12 document remains historical input. The requirement-by-
requirement disposition in section 4 is authoritative. In particular,
"superseded" means ownership moved here or to a focused owner; it does not mean
"implemented".

## 3. Current canonical owners

Status is a snapshot from the audit date. Pull request state must be checked
again before merge or deployment.

| Scaling requirement | Canonical owner | Implementation evidence | Audited status |
|---|---|---|---|
| Multi-node, multi-group bootstrap | `2026_06_14_implemented_multinode_multigroup_bootstrap.md` | PR #1011 merged | Implemented on `main` |
| Learner membership primitive | `2026_04_26_implemented_raft_learner.md` | PR #1002 merged | Implemented on `main`; follower reads are separate |
| Leader balance | `2026_06_11_implemented_leader_balance_scheduler.md` | PR #1012 merged | Implemented on `main`; data placement is separate |
| Hotspot split M1 catalog and same-group split | `2026_02_18_implemented_hotspot_split_milestone1_pr.md` and `2026_02_18_partial_hotspot_shard_split.md` | PR #999 merged the catalog cleanup | Implemented M1; parent design remains partial |
| Hotspot split M2 migration | `2026_06_11_proposed_hotspot_split_milestone2_migration.md` | PRs #1084, #1085, #1088, #1090, and #1096 open; supporting stack members have merged | In flight; not on `main` as a complete migration plane |
| Hotspot split M3 automation | `2026_06_11_partial_hotspot_split_milestone3_automation.md` | PRs #1097 and #1104 open | In flight; not implemented on `main` |
| Per-group HLC renewal and default-group allocator bridge | `2026_04_16_partial_centralized_tso.md` | PR #998 merged | Implemented bridge; dedicated TSO remains in flight |
| Dedicated TSO group and durable routing | `2026_04_16_partial_centralized_tso.md` | PRs #1064, #1095, #1103, and #1108 open | In flight; stacked series is not on `main` |
| Shared Pebble block cache | PR #1082 | PR #1082 open | In flight; cache sharing only, not all resource-pool work |
| Raft gRPC streaming transport | `2026_04_18_implemented_raft_grpc_streaming_transport.md` | PR #1006 merged; PR #1048 merged the kill switch | Implemented on `main`; production multi-group soak evidence remains outstanding |
| S3 Raft blob offload | `2026_04_25_proposed_s3_raft_blob_offload.md` | PRs #1057 and #1063 open | In flight; payload offload is not on `main` |
| Live logical backup | `2026_04_29_proposed_logical_backup.md` | PR #1065 merged scan primitives; PRs #1056 and #1059 open | In flight; distinct from physical SST snapshot offload |

## 4. 2026-06-12 requirement audit

### 4.1 Routing scale-out

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 versioned catalog delta and streaming watch | Unimplemented and unowned | Write `*_proposed_route_catalog_delta_watch.md`; own the durable delta log, retention fallback, atomic mirror publication, capability negotiation, and stream reconnect semantics |
| M2 indexed route engine and copy-on-write history | Unimplemented and unowned | Write `*_proposed_route_catalog_index.md`; own the primary index, group secondary index, immutable snapshot sharing, memory bound, and migration from the slice representation |
| M3 batched catalog mutation | Unimplemented and unowned | Write `*_proposed_route_catalog_batching.md`; own batch conflict semantics, one-version publication, idempotency, limits, and interaction with the delta watch |

Hotspot split M2 and M3 do not own these catalog-scale mechanisms. They consume
the existing catalog and must not absorb the three designs above.

### 4.2 Multi-region

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 WAN Raft tuning and region-aware membership | Unimplemented and unowned | Write `*_proposed_wan_raft_membership.md`; own topology, timing bounds, region identity, quorum failure modes, and rollout |
| M2 region-local HLC | Unimplemented and unowned | Write `*_proposed_regional_timestamp_oracle.md`; reconcile regional issuance with the dedicated TSO invariant before choosing local ceilings or a global oracle |
| M3 regional catalog mirror | Unimplemented and unowned | Write `*_proposed_regional_catalog_mirror.md`; depend on the catalog delta/watch design and define freshness and failover contracts |
| M4 cross-region disaster recovery | Unimplemented and unowned | Write `*_proposed_cross_region_failover.md`; own authority, fencing, data completeness, operator approval, failback, and split-brain prevention |

Multi-node bootstrap is a prerequisite, not an implementation of multi-region
placement or failover.

### 4.3 Storage tier

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 SST ingest snapshot transfer | Unimplemented and unowned | Write `*_proposed_pebble_sst_snapshot_transfer.md`; own checkpoint consistency, file manifest, integrity, transport, ingest, cleanup, and fallback |
| M2 shared block cache | In flight | PR #1082 owns only process-wide cache sizing, lifetime, metrics, and tests |
| M2 per-shard Pebble tuning and write admission | Partially addressed operationally, but the proposed per-shard contract is unowned | Write `*_proposed_pebble_resource_governor.md`; own tuning scope, node-wide fairness, stall thresholds, admission errors, and adapter retry semantics |
| M3 sharded retention scheduling | Existing compaction is implemented; jitter, node budget, and hot-key dynamic retention are unimplemented and unowned | Write `*_proposed_sharded_mvcc_retention.md`; preserve the hard retention contract and active timestamp pins |
| M4 physical disaster-recovery snapshot offload | Unimplemented and unowned | Write `*_proposed_physical_snapshot_object_offload.md`; do not merge it into logical backup or S3 user-payload blob offload |

### 4.4 Coordinator and API gateway

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 timestamp issuance decoupling | Partially implemented and in flight | `2026_04_16_partial_centralized_tso.md`, PR #998, and the #1064/#1095/#1103/#1108 stack own this work |
| M2 follower and learner reads | Unimplemented and unowned | Write `*_proposed_follower_reads.md`; own leader-vouched read timestamps, apply watermarks, staleness/session contract, invalidation, routing, and Jepsen evidence |
| M3 cross-shard 2PC completion | Partially implemented; no focused end-to-end owner | Write `*_proposed_cross_shard_transaction_completion.md`; start from the existing `ShardedCoordinator` 2PC and Composed-1 guard, then own unsupported router paths, read-only validation, recovery, and adapter coverage |
| M4 resolver work delegation | Unimplemented and unowned | Write `*_proposed_lock_resolver_delegation.md`; own snapshot assignment, leader-vouched decisions, duplicate work, failover, admission, and Raft apply boundaries |
| M5 leader-proxy circuit breaker | Unimplemented and unowned for the data plane | Write `*_proposed_leader_proxy_circuit_breaker.md`; own retry budget, leader-change reset, backoff, adapter errors, and election-storm behavior |

The admin package's existing `ErrLeaderUnavailable` mapping is not evidence that
the general data-plane leader proxy has the proposed circuit breaker.

## 5. Additional gaps introduced by the 2026-06-23 roadmap

| Gap | Disposition | Required focused owner |
|---|---|---|
| Region/range balance scheduler | Unimplemented and unowned | `*_proposed_region_balance_scheduler.md`; depend on replica placement, multi-node bootstrap, and hotspot migration |
| Range merge | Unimplemented and unowned | `*_proposed_range_merge.md`; split same-group and cross-group merge into reviewable milestones and define transaction drain/fencing |
| Streaming transport multi-group soak | Implementation exists; evidence gap | Keep the transport contract in `2026_04_18_implemented_raft_grpc_streaming_transport.md`; add repeatable soak evidence without changing protocol semantics |
| Auto group lifecycle | Unimplemented and unowned | `*_proposed_auto_group_lifecycle.md`; depend on placement, migration, merge, and safe membership replacement |

## 6. Dependency order

The next focused designs should be written and implemented in this order:

1. Finish the open hotspot split M2/M3, shared-cache, dedicated-TSO, and S3 offload
   stacks without moving their mechanisms into this roadmap.
2. Catalog delta/watch, then catalog index and batched mutation.
3. Follower reads and cross-shard transaction completion, both gated on the
   dedicated timestamp invariant where required.
4. SST snapshot transfer, Pebble resource governance, and sharded retention.
5. Region balance and range merge.
6. WAN membership, regional timestamps, regional catalog, and cross-region
   failover in that order.
7. Auto group lifecycle only after placement, migration, merge, and membership
   replacement are independently safe.

## 7. Completion rule

This roadmap can be promoted from `proposed` only when every row is either:

- implemented on `main` with its focused owner promoted to `implemented`, or
- deliberately rejected with a recorded rationale in its focused owner.

An open pull request, a code primitive, or a superseded roadmap paragraph is
not sufficient evidence of completion.
