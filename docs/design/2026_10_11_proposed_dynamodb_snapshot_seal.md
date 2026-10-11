# DynamoDB snapshot seal for read-modify-write transactions

Status: Proposed
Author: bootjp
Date: 2026-10-11

## Failure and scope

Jepsen run [37207021947](https://github.com/bootjp/elastickv/actions/runs/37207021947) observed a successful DynamoDB list append disappear from a later read. A one-phase write can allocate commit timestamp `C`, then pause before proposing its Raft entry. Another write to a different key can advance the applied watermark beyond `C`. A later transaction fences the already-proposed log, reads the old item at snapshot `R > C`, and plans an overwrite. If the first entry arrives after this read, ordinary OCC only checks for versions with commit timestamp greater than `R`; it cannot identify the unseen write at `C`.

This applies to UpdateItem, PutItem, DeleteItem, BatchWriteItem, admin writes, and TransactWriteItems, including ConditionCheck. It also applies when the two requests enter different servers or leadership changes. Process-local item locks reduce races within one server but do not establish a distributed safety boundary.

## Required invariants

1. A read snapshot at timestamp `R` includes every previously proposed write that can commit at or before `R` in each group it reads.
2. A write allocated before the snapshot but proposed later cannot commit at or before `R`. It must fail with a retryable conflict and recompute against a new snapshot; ambiguous retries retain the existing exact-commit-timestamp dedup probe.
3. The boundary survives follower ingress, leader election, crash recovery, and a table spanning data groups. A local mutex or a lease-only read is insufficient.
4. All single-item entry points derive their local contention key from only the table's hash and range key attributes. ConditionCheck participates because an existing item can produce a same-value Put and a missing item still contributes a read dependency.

## Proposed protocol

Before planning a DynamoDB read-modify-write transaction, obtain the strict committed floor and dispatch a transaction that writes a reserved snapshot-seal key for every item the operation touches. Each seal key is derived from the canonical table, generation, and primary item key, and routes to the same data group as that table's items. The coordinator allocates one leader-owned commit timestamp `R` for this transaction, so multi-item seals share the same timestamp across groups. Wait for the seal transaction to apply, then read each item at `R` and dispatch the user transaction with `StartTS = R`.

Every item-writing transaction includes each affected item's seal key in `ReadKeys`. If a write was allocated before the seal but proposed afterward, the seal's committed version is newer than that write's original `StartTS`; the existing Raft-apply OCC check rejects it as a retryable conflict. If the write was proposed before the seal, the seal's Raft ordering makes it visible to the read at `R`. A later seal similarly conflicts with any earlier transaction that has not yet committed. The seal is ordinary MVCC data, persisted with the Raft applied index and restored with snapshots; no new wire format or independent floor state is needed.

The seal must be a Raft write rather than a client-side check of `LastCommitTS`: the latter cannot see an allocated, unproposed timestamp. A ReadIndex establishes the committed floor before the seal, but by itself does not prevent an older timestamp from arriving after the read. Local item locks can remain as a contention optimization and must never be relied on for correctness across nodes. Per-item seals allow writes to different items in the same table to proceed independently; hot-item contention and the extra Raft transaction must be measured under Jepsen and a benchmark.

## Failure handling and validation

- Failure to allocate `R`, reach any target leader, or apply a seal fails the request closed; it must not use a follower-local persistence timestamp.
- A leader change does not clear a seal. The successor restores the seal key before accepting item writes. The HLC physical-ceiling fence prevents the successor from issuing timestamps in the old leader's window.
- Tests must hold a write after allocation and before proposal, advance another key's watermark, seal and plan on a second server, then release the first write. Repeat with a leader change and with two data groups. The final item must contain both successful appends, or one operation must return a retryable failure.
- Jepsen should exercise follower ingress without an HTTP leader map and leader changes as well as the existing three-node append workload.

## PR sequence

The first commit in PR #1259 added strict ReadIndex fencing and process-local item locks. This proposal is included before landing the distributed snapshot-seal implementation. The PR must remain open for review until the two-server and multi-group regressions, Jepsen run, and contention measurement confirm the behavior.
