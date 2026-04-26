# Split-Queue FIFO for the SQS Adapter

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-04-26

---

## 1. Background and Motivation

elastickv's SQS adapter implements FIFO queues with **single-partition** semantics: every message in a `.fifo` queue lives in exactly one Raft group, ordered globally by send time, with the group lock keyed on `(queue, generation, MessageGroupId)` so messages with the same group ID deliver in order while messages with different group IDs can deliver in parallel.

This matches AWS's Standard FIFO contract for *modest* throughput — but AWS's **High Throughput FIFO** (HT-FIFO) feature lifts the per-FIFO-queue ceiling from 300 transactions per second (TPS) per API per region to 70,000+ TPS per region by **partitioning** the queue across multiple data planes, with ordering preserved *within* each `MessageGroupId`. AWS exposes this as the `DeduplicationScope` and `FifoThroughputLimit` queue attributes.

The Phase 1+2 of `docs/design/2026_04_24_partial_sqs_compatible_adapter.md` deliberately deferred this — the design doc §16.6 marks it as TODO and notes "**the** large item in Phase 3" because it touches replication topology, routing, FIFO group-lock semantics, the reaper, the metrics surface, and the migration path for queues that already exist.

This document is the proposal that unblocks the implementation. It is **not the implementation**: the work splits naturally into multiple PRs, and any one of them is too big to land without prior agreement on the partition assignment scheme, the migration story, and the rollback story. Concretely, this proposal is **gate-of-no-return** material — once a partitioned FIFO queue exists in production, the data layout cannot change without a full migration.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **Multiple partitions per FIFO queue**, each owned by its own Raft group, with `MessageGroupId` deterministically routed to a partition.
2. **Within-group ordering preserved**: any two messages with the same `MessageGroupId` land on the same partition and deliver in send order, exactly as today's single-partition FIFO does.
3. **Across-group parallelism**: different `MessageGroupId` values may land on different partitions and deliver concurrently. Effective throughput scales with the partition count when the producer's group ID distribution is even.
4. **Backward compatibility**: existing `.fifo` queues created before this feature stay single-partition forever (one partition is the special case of N=1). No migration runs implicitly.
5. **AWS-shape configuration**: `DeduplicationScope` (`messageGroup` | `queue`) and `FifoThroughputLimit` (`perMessageGroupId` | `perQueue`) are accepted via `CreateQueue` / `SetQueueAttributes` and rejected on Standard queues, matching AWS.
6. **Operational safety**: a partitioned FIFO queue's per-partition keyspace is reapable, observable, and survives leader failover with the same OCC discipline today's queues do.

### 2.2 Non-Goals

1. **Auto-rebalancing**. A queue's partition count is set at create time and never changes. AWS's HT-FIFO works the same way; resharding a live FIFO queue while preserving order is fundamentally hard and is not in this proposal.
2. **Cross-partition transactions**. Operations on a partitioned queue still touch one partition per request. There is no "atomic delete from all partitions"; admin operations like PurgeQueue iterate partitions and tolerate partial progress.
3. **Group-level throttling**. The §16.5 (per-queue throttling) proposal is forward-compatible with this design, but the per-`MessageGroupId` rate limit is its own follow-up.
4. **Pluggable partition assignment**. The hash function is fixed (FNV-1a 64 over the UTF-8 bytes of `MessageGroupId`); operators do not get a knob.
5. **Migration of existing single-partition `.fifo` queues to N partitions**. Out of scope. Operators who want HT-FIFO on an existing workload create a new queue, drain the old one, and switch producers.
6. **Standard (non-FIFO) queue partitioning**. Standard queues already parallelize across the consumer pool because they have no ordering contract; partitioning them adds complexity for no win.

---

## 3. Data Model

### 3.1 Partition identity

Each partitioned FIFO queue has `N` partitions where `N ∈ {1, 2, 4, 8, 16, 32}`. The `N=1` case is the existing single-partition layout, unchanged. Powers of two only so the hash → partition step is a `hash & (N-1)` (cheap and consistent) and so future `N` changes via offline rebuild stay tractable.

A partition is identified by the tuple `(queueName, partitionIndex)` where `partitionIndex ∈ [0, N)`. The key shape is **conditional on whether the queue is partitioned** (Codex P1 + Gemini high on PR #664 — naively inserting a `<partition>` segment into every key would shift offsets for `<gen>` and `<msgID>` and break readback of every existing message on disk):

- **Legacy / `PartitionCount = 0` / Standard queues** keep today's `!sqs|msg|data|<queue>|<gen>|<msgID>` byte-for-byte. No partition segment is written or read. Existing data on disk is unaffected; existing key constructors stay unchanged on this code path.
- **Partitioned FIFO queues (`PartitionCount > 1`)** use a *new* keyspace prefix that explicitly includes the partition: `!sqs|msg|data|p|<queue>|<partition>|<gen>|<msgID>` (note the extra `p|` discriminator after `data|`). The discriminator is what guarantees no collision with the legacy prefix even when `<partition> = 0` happens to match the first 8 bytes of a legacy `<gen>`.

The `p|` discriminator is **safe by name-validator construction**, not by accident: AWS SQS queue names (and elastickv's `validateQueueName`) admit only `[A-Za-z0-9_-]` plus the optional `.fifo` suffix, so no queue name can contain `|`. The existing `!sqs|msg|data|<queue>|...` segment is therefore terminated by a `|` that no queue name can produce, and the new `!sqs|msg|data|p|<queue>|...` segment starts with a literal byte sequence (`p|`) that cannot appear at the same position in any legacy key (it would require a queue name of `p`, which would still be followed by a `|` from the *segment* terminator, not from the queue name itself — but the prefix routing reads the bytes as `data|p|` vs `data|<segment>|`, and `<segment>` is base32-encoded so it never starts with the literal ASCII `p`). The implementation PR's name validator must continue to reject `|` in queue names; any future relaxation of that rule has to revisit this prefix scheme first.

Concretely, the implementation PR exposes **two named constructors** rather than a variadic dispatcher (Claude review on PR #664 flagged the variadic form as a footgun: `sqsMsgDataKey(q, gen, id, p0, p1)` would silently ignore `p1` and the compiler would not catch it). The dispatch lives at the call site, where `meta.PartitionCount` is already in scope:

```go
// Two distinct constructors, one per keyspace.
func legacyMsgDataKey(queueName string, gen uint64, messageID string) []byte
func partitionedMsgDataKey(queueName string, partition uint32, gen uint64, messageID string) []byte

// Dispatch at the call site. No variadic, no silent argument loss.
var dataKey []byte
if meta.PartitionCount > 1 {
    dataKey = partitionedMsgDataKey(queueName, partition, gen, msgID)
} else {
    dataKey = legacyMsgDataKey(queueName, gen, msgID)
}
```

The reaper enumerates **both** prefixes when reaping a queue, so a queue that was created legacy and later (in a hypothetical future migration) gains partitions does not strand its old data — out of scope today, but the prefix choice keeps that door open.

Scans on a partitioned queue use `!sqs|msg|data|p|<queue>|<partition>|` so a worker handling partition `k` never sees keys for partition `k+1`. Scans on a legacy queue use `!sqs|msg|data|<queue>|`, identical to today.

### 3.2 Queue meta extensions

`sqsQueueMeta` gains:

```go
type sqsQueueMeta struct {
    // ... existing fields ...

    // PartitionCount is the number of FIFO partitions for this queue.
    // 1 (or 0, treated as 1) means the existing single-partition
    // layout — no schema change. >1 enables HT-FIFO. Set at create
    // time; immutable after first SendMessage commits. Power-of-two
    // values only (validator rejects others).
    PartitionCount uint32 `json:"partition_count,omitempty"`

    // DeduplicationScope mirrors the AWS attribute. "messageGroup"
    // means the dedup window is per (partition, MessageGroupId)
    // pair; "queue" means it is per queue (the legacy behaviour).
    // Only meaningful when PartitionCount > 1.
    DeduplicationScope string `json:"deduplication_scope,omitempty"`

    // FifoThroughputLimit mirrors the AWS attribute. Defaults to
    // "perMessageGroupId" when PartitionCount > 1; the alternative
    // "perQueue" reduces the partition assignment to a single
    // partition (effectively N=1) and is mostly useful for clients
    // that want the AWS attribute set without the extra capacity.
    FifoThroughputLimit string `json:"fifo_throughput_limit,omitempty"`
}
```

`PartitionCount` is **immutable after first SendMessage**. The validator on `SetQueueAttributes` rejects any change; operators who want a different partition count create a new queue. Why immutable: changing it would require re-hashing every existing message into a new partition, which (a) breaks ordering for in-flight messages of every group whose hash bucket changed, and (b) is a multi-second / multi-minute operation that cannot be expressed as one OCC transaction.

### 3.3 Routing

`partitionFor(meta, messageGroupId) uint32`:

```go
// Single-partition path: cheap fast path. Three cases collapse here:
//   1. Standard queues + N=1 FIFOs (PartitionCount <= 1).
//   2. FIFOs explicitly configured with FifoThroughputLimit=perQueue,
//      which §3.2 documents as "reduces routing to a single partition
//      regardless of PartitionCount." A queue created with
//      PartitionCount=8 + perQueue MUST land every group on partition 0;
//      hashing across all 8 would directly contradict the documented
//      semantics that operators selected when they picked perQueue.
//      (Codex P2 + Claude on PR #664 caught this.)
if meta.PartitionCount <= 1 || meta.FifoThroughputLimit == "perQueue" {
    return 0
}
if messageGroupId == "" {
    // Defensive: a FIFO send without MessageGroupId is rejected
    // upstream by validateSendFIFOParams. If we somehow reach here,
    // route to partition 0 so the failure is contained, not fanned
    // out across every partition.
    return 0
}
hash := fnv.New64a()
_, _ = hash.Write([]byte(messageGroupId))
return uint32(hash.Sum64()) & (meta.PartitionCount - 1)
```

The choice of FNV-1a is deliberate: it is fast (no SIMD setup), has no key, and is identical across Go versions and architectures. Operators do not need this to be cryptographically strong — they need it to be deterministic and well-distributed, both of which FNV-1a satisfies.

### 3.4 Cross-shard placement

Partitions live in **separate Raft groups** when the queue's shard config maps each partition to a different group. The existing `kv/shard_router.go` plus the multi-shard configuration flags (`--raftSqsMap`) already support this — each `(queueName, partition)` pair becomes its own routing key.

For deployments that don't want one Raft group per partition (e.g. a small cluster with limited shard capacity), partitions can co-locate on the same group. The choice is operator-driven via the shard config; it does not affect correctness, only throughput scaling.

---

## 4. Request Flows

### 4.1 SendMessage on a partitioned FIFO

```
1. Decode → sqsSendMessageInput (existing).
2. validateSendFIFOParams: same as today (MessageGroupId required).
3. partitionIndex := partitionFor(meta, in.MessageGroupId).
4. Resolve the leader for (queue, partitionIndex) via shard_router.
   - Today's queue-per-shard router becomes (queue, partition)-per-shard.
5. Build the OCC OperationGroup with the right keyspace
   constructor for this queue's PartitionCount (named constructors
   per §3.1; no variadic):
     if meta.PartitionCount > 1 {
       dataKey  = partitionedMsgDataKey(queue, partitionIndex, gen, msgID)
       visKey   = partitionedMsgVisKey(queue, partitionIndex, gen, ...)
       groupKey = partitionedMsgGroupKey(queue, partitionIndex, gen, MessageGroupId)
     } else {
       dataKey  = legacyMsgDataKey(queue, gen, msgID)
       visKey   = legacyMsgVisKey(queue, gen, ...)
       groupKey = legacyMsgGroupKey(queue, gen, MessageGroupId)
     }
6. Dispatch through the leader of the resolved partition (existing
   leader-proxy path, unchanged).
```

Steps 1–2 are unchanged; step 3 is the new routing call (~10 lines); steps 4–6 are the existing send path with `partitionIndex` threaded through the key constructors. The dedup record written by step 6 keys on `(queue, partition, MessageGroupId, dedupID)` — when `DeduplicationScope = messageGroup`, this is correct by construction; when `DeduplicationScope = queue`, the validator rejects the request unless `PartitionCount = 1`.

### 4.2 ReceiveMessage on a partitioned FIFO

ReceiveMessage today scans `sqsMsgVisPrefixForQueue(queue, gen)` once. Under partitioning that becomes a scan **per partition** with **leader proxying for the partitions whose leader lives on a different node**:

```
1. Decode → sqsReceiveMessageInput (existing).
2. Compute partitionOrder := starting offset chosen by hashing the
   request's RequestId (or random when absent) so successive calls
   from the same consumer rotate which partition they hit first.
   This avoids head-of-line bias toward partition 0 under load.
3. For each partitionIndex in partitionOrder, until MaxNumberOfMessages
   are collected or every partition has been tried:
     a. Resolve the leader for (queue, partitionIndex).
     b. If this node is the leader: scan locally, deliver candidates.
     c. Otherwise: forward the request to the leader-of-partition via
        the existing leader-proxy machinery (proxyToLeader, extended
        to accept a partition argument so the proxy target is the
        right shard, not just "the queue's leader"). The proxied call
        carries an `X-Elastickv-Receive-Partition: <k>` header so the
        downstream handler knows to skip its own partition fanout and
        scan only partition k.
4. Aggregate the per-partition results, cap at MaxNumberOfMessages.
```

The point is that a consumer pinned to a single endpoint **must still see messages from every partition**, even partitions whose leader is elsewhere — otherwise the SDK's "ReceiveMessage returned nothing, sleep and retry" assumption silently leaks messages forever (Codex P1 + Gemini medium on PR #664). The cost is one extra hop per non-local-leader partition; for the common deployment where partitions are co-located on one Raft group, every partition's leader is the same node and there is no fanout. For deployments that spread partitions across nodes, the proxy fanout is exactly what AWS does internally — clients see uniform behaviour regardless of topology.

The proxy fanout is bounded: `partitionOrder` short-circuits as soon as `MaxNumberOfMessages` are collected, so a consumer asking for 1 message touches at most 1 remote leader (the one for the first non-empty partition in their rotation order). A FIFO with no in-flight messages costs N proxy round-trips to confirm empty, but FIFO consumers are expected to use long-poll (`WaitTimeSeconds`), which extends each call's budget.

### 4.3 PurgeQueue / DeleteQueue on a partitioned FIFO

Both verbs become **partition-iterative**: the handler loops over `[0, PartitionCount)`, dispatching the same OCC operation against each partition's leader. PurgeQueue's tombstone (Phase 2 from PR #638) is per-partition, keyed by the partition index in addition to the generation. The reaper already enumerates tombstones; the partition prefix becomes part of the tombstone key.

Per-call atomicity for the entire queue is **not** preserved — a Purge that fails on partition 2 of 8 leaves partitions 0 and 1 purged. AWS itself does not promise atomicity here either; `PurgeQueue` is documented as a deletion that "may take up to 60 seconds" and is best-effort across partitions. The handler retries with exponential backoff on each partition independently and only reports success when all partitions succeed.

### 4.4 ChangeMessageVisibility / DeleteMessage

These take a `ReceiptHandle` which already encodes the partition index (the receipt-handle codec adds an 8-byte segment for `partitionIndex` after the existing version byte). The handler decodes the partition from the handle and dispatches against the right shard. No fanout — these are single-partition operations.

---

## 5. Routing Layer Changes

`kv/shard_router.go` today routes by queue name. With partitions, the routing key becomes `(queueName, partitionIndex)`. The existing `--raftSqsMap` flag accepts entries keyed by queue name; the syntax extends to accept a partition suffix:

```
--raftSqsMap "orders.fifo:8=group-7,group-8,group-9,group-10,group-11,group-12,group-13,group-14"
```

Reads as: queue `orders.fifo` has `8` partitions, mapped to Raft groups `group-7` through `group-14` in partition order. Backward compatibility: queues without an explicit partition suffix keep the single-partition layout.

A queue whose `PartitionCount` in meta does not match the shard map's partition count is a configuration error. The CreateQueue handler resolves the count from the `Attributes` first, then verifies the shard map agrees; mismatch returns 400 `InvalidParameterValue`.

---

## 6. Reaper Implications

The retention reaper (`adapter/sqs_reaper.go` from PR #638) walks `sqsMsgByAgePrefix(queue, gen)`. With partitioning, the prefix becomes `sqsMsgByAgePrefix(queue, partitionIndex, gen)` and the reaper iterates partitions.

The per-queue scan budget (`sqsReaperPerQueueBudget`) becomes a per-partition budget — otherwise a queue with 32 partitions starves every other queue. Practical effect: a partitioned queue's reaper completes in `partitions × budget` time per cycle, scaling linearly. Acceptable because the reaper runs every 30s and the budget is sized so a single partition completes in well under that.

Tombstones written by `DeleteQueue` and `PurgeQueue` are per-partition (Section 4.3). The reaper enumerates tombstones across all partitions identically.

---

## 7. Migration

### 7.1 Existing queues stay single-partition

Every queue created before this feature has `PartitionCount = 0` (zero value); the routing function treats that as 1 (Section 3.3). No code path changes for those queues; their key layout is byte-identical.

### 7.2 New queues opt in

`CreateQueue` accepts the AWS-style attribute `FifoThroughputLimit = perMessageGroupId` plus a non-AWS `PartitionCount` attribute (or, for AWS-shape compatibility, infer the partition count from `DeduplicationScope = messageGroup` + a fixed default, e.g. 8). The doc proposes accepting both: AWS-shape callers can omit `PartitionCount` and get a sensible default; advanced callers can specify.

### 7.3 No live re-partitioning

Per §2.2 #5: changing a queue's partition count is not supported. A future migration tool could:

1. Create a new queue with the new partition count.
2. Set the old queue's `Attributes.RedrivePolicy` to point at the new queue.
3. Drain by consuming from old, redriving to new.
4. Cut over producers.
5. Delete old.

This is out of scope here.

---

## 8. Failure Modes and Edge Cases

1. **Proxy RTT under spread deployment**: ReceiveMessage on a queue whose partitions are spread across multiple Raft groups pays one extra round-trip per non-local-leader partition (§4.2 proxies them server-side, so a consumer pinned to one endpoint still sees every partition's messages — no false-empty failure). The cost is bounded: a request for `MaxNumberOfMessages = 1` short-circuits as soon as the first non-empty partition responds, so the *typical* extra hop count is one. The pathological case is a queue with N partitions where the consumer is asking "is anything here?" against an empty queue — that costs N proxy round-trips before returning empty. Mitigation: long-poll (`WaitTimeSeconds`) extends each call's budget so the cost amortises; latency-sensitive deployments can co-locate partitions on fewer Raft groups (at the cost of less throughput parallelism). A single-partition or co-located deployment pays nothing.

2. **Partition-leader churn**: a leader change on partition 3 causes that partition's ReceiveMessage to fail-over while partitions 0–2 and 4–7 keep serving. Existing `proxyToLeader` machinery handles the transition.

3. **Hot partition**: an unbalanced `MessageGroupId` distribution (e.g. 90% of traffic on group "user-1") makes one partition the bottleneck. This is fundamental to any hash-partitioned FIFO; the answer is operator-side group-ID rebalancing, not server-side magic. AWS's HT-FIFO has the same property.

4. **Receipt-handle from old version**: existing receipt handles encode no partition. When this feature lands, the receipt-handle codec gains a version byte distinguishing v1 (no partition) from v2 (with partition). v1 handles still work for single-partition queues forever; partitioned queues issue v2 only. ChangeMessageVisibility / DeleteMessage check the version before decoding the partition field.

5. **Mixed-version cluster**: a rolling upgrade where some nodes have HT-FIFO and others don't. The new feature gates on the queue's `PartitionCount > 1` field, which is set at create time; old nodes that try to scan a partitioned queue's keyspace will simply not find anything (the prefix has changed). The catalog rejects `CreateQueue` with `PartitionCount > 1` until every node in the cluster reports the new feature flag.

   **The capability advertisement mechanism**: each node's existing `/sqs_health` endpoint (`adapter/sqs.go: serveSQSHealthz`) gains a new field in its JSON body — `capabilities: ["htfifo"]` once this PR's code is in the binary. The catalog's CreateQueue handler reads the live node set from the distribution layer's node registry (the same registry used by `proxyToLeader` to locate leaders), polls `/sqs_health` on each, and gates `PartitionCount > 1` on every node reporting the `htfifo` capability. Nodes that don't respond within a short timeout are treated as not-yet-upgraded — a deliberate fail-closed default so a network blip does not let a partitioned queue land in a partially-upgraded cluster. This mirrors the §3.3.2 admin-forwarding upgrade gate from the admin dashboard design (PR #644), which uses the same "all-nodes-must-report" pattern for `AdminForward`.

---

## 9. Testing Strategy

1. **Unit tests** (`adapter/sqs_partition_test.go`):
   - `partitionFor` distribution: 100k random group IDs across 8 partitions land within ±5% of equal share.
   - `partitionFor` determinism: same group ID always returns same partition across runs / process restarts.
   - Edge: `PartitionCount = 0` and `1` route to partition 0 unconditionally.
   - Edge: empty `MessageGroupId` routes to partition 0 (defensive).

2. **End-to-end** (`adapter/sqs_partitioned_fifo_test.go`):
   - Create a queue with `PartitionCount = 4`, send 1000 messages with random group IDs, confirm ordered delivery within each group, parallel delivery across groups.
   - PurgeQueue iterates all partitions, leaves none orphaned.
   - DeleteQueue similarly.

3. **Receipt-handle round-trip**: v1 handle (legacy) on single-partition queue, v2 handle (with partition) on partitioned queue, both decode + ChangeMessageVisibility / DeleteMessage correctly. Cross-version handle rejection (v1 handle against partitioned queue → 400 `ReceiptHandleIsInvalid`).

4. **Jepsen** (`jepsen/sqs/htfifo/`): a new workload that stresses cross-partition delivery — many groups, many consumers, network partition mid-burst — and verifies (a) within-group ordering and (b) no message loss.

5. **Metrics / observability**: new `sqs_partition_messages_total{queue, partition, action}` counter so dashboards can spot hot partitions.

---

## 10. Open Questions

1. **Partition count limits**: 32 is the proposal's max. AWS HT-FIFO has no documented per-queue cap; 32 is enough for ~30,000 RPS per queue at the per-shard ~1,000 RPS limit. Higher would require larger per-queue meta records and more reaper cycles. Adjust later if operators demand more.

2. **Hash function**: FNV-1a is fast and stable but not cryptographically strong. An attacker who can pick `MessageGroupId` values can pin all traffic to one partition. Mitigation options:
   - Document that group IDs must be random / non-attacker-controlled.
   - Switch to xxHash64 with a process-startup-random seed (defeats the targeted attack but breaks determinism across processes — bad for the "where did this message land" question).
   - Accept the risk and document it.

   The proposal's working answer is **document and accept** — the feature is for cooperative operators, not adversarial multi-tenancy.

3. **Default `PartitionCount` for AWS-shape callers**: when a client sets `FifoThroughputLimit = perMessageGroupId` without specifying `PartitionCount`, what default? AWS's HT-FIFO is documented as "up to 70k TPS"; choosing 8 partitions gives ~8k TPS on elastickv's per-shard limits. 16? 32? Operator polling needed.

4. **Should `PartitionCount` be a per-shard configuration rather than per-queue?** Some operators may want every FIFO queue in a deployment to have the same partition count (one Raft topology). Adding a `--sqsDefaultFifoPartitionCount` flag handles that without changing the meta schema.

5. **Cross-partition ordering for visibility**: does a consumer need to see messages from partition 0 *before* partition 1 within a single ReceiveMessage call? The answer is no (within-group ordering is the only contract), but the test plan should pin this so a future "fairness" tweak does not accidentally introduce ordering across partitions.

---

## 11. Rollout Plan (Multi-PR)

| PR | Content | Reviewable in isolation? |
|---|---|---|
| 1 | This proposal doc lands. Operators have time to flag concerns. | Yes |
| 2 | Schema: `sqsQueueMeta.PartitionCount`, `DeduplicationScope`, `FifoThroughputLimit`. Routing function `partitionFor`. CreateQueue / SetQueueAttributes validation. **No** keyspace changes yet — feature is dormant. | Yes (catalog only) |
| 3 | Keyspace: thread `partitionIndex` through every `sqsMsg*Key` constructor, defaulting to 0 so existing queues stay byte-identical. | Yes (mechanical) |
| 4 | Routing layer: `kv/shard_router.go` accepts the `(queue, partition)` key. `--raftSqsMap` syntax extension. Mixed-version gate. | Yes (operator-config) |
| 5 | Send / Receive partition fanout. Receipt-handle v2 codec. | Yes (data-plane) |
| 6 | PurgeQueue / DeleteQueue partition iteration. Tombstone schema update. Reaper update. | Yes (control-plane) |
| 7 | Jepsen HT-FIFO workload. Metrics. | Yes (testing) |
| 8 | Partial-doc lifecycle bump: 3.D moves from TODO to Landed. Section 13 from §16.6 of the partial doc gets the as-built record. | Yes (docs) |

**Gate of no return**: PR 5 is the point where a partitioned FIFO queue can hold real data. Once any production cluster runs PR 5 and creates a partitioned queue, rolling back means draining and recreating the queue. PR 1–4 are reversible (no data layout change). Recorded in the PR descriptions.

---

## 12. Alternatives Considered

### 12.1 Skip HT-FIFO and document the per-queue cap

Operators who need >300 TPS create multiple FIFO queues and shard at the application level. **Rejected**: this pushes the partitioning burden onto every consumer of elastickv's SQS surface, and clients that already use AWS HT-FIFO have to reimplement their topology. The whole point of an SQS-compatible adapter is to let producers stay AWS-shaped.

### 12.2 Single Raft group, multiple visibility queues

Keep one Raft group per queue but partition the visibility index inside it. **Rejected**: the bottleneck on a single FIFO queue is the Raft proposal pipeline, not the visibility scan. Partitioning the index without partitioning the consensus group does not unlock any throughput.

### 12.3 Cross-partition transactions for PurgeQueue

Use a coordinator transaction that touches all partitions atomically. **Rejected**: the cross-shard transaction primitive does not yet exist for SQS, and AWS itself does not promise PurgeQueue atomicity. Per-partition iteration with retry is the standard answer.

### 12.4 Operator-configurable hash function

Let operators plug in their own `partitionFor`. **Rejected** at this stage: a fixed function is much easier to reason about for cross-version compatibility. Pluggable hashing is a Phase 5+ concern, if ever.
