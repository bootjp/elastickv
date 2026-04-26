# Per-Queue Throttling and Tenant Fairness for the SQS Adapter

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-04-26

---

## 1. Background and Motivation

elastickv's SQS adapter currently has **no per-queue rate limiting**. A single tenant's runaway producer can:

1. Saturate the leader's Raft proposal pipeline (one OCC dispatch per `SendMessage`), pushing latency on every other queue's writes through the same shard.
2. Exhaust the receive-path's visibility-index scan budget (`sqsVisScanPageLimit = 1024`), causing other tenants' `ReceiveMessage` calls to time out empty.
3. Fill the message keyspace fast enough that the retention reaper cannot keep up — the keyspace grows unbounded until the next manual purge.

Phase 3.C in [`docs/design/2026_04_24_partial_sqs_compatible_adapter.md`](2026_04_24_partial_sqs_compatible_adapter.md) §16.5 marks this as TODO. AWS itself enforces per-account / per-API limits ("standard request throttle of 3000 RPS per region per AWS account" plus per-API limits like 300 TPS for batch APIs); operators running elastickv as a multi-tenant SQS facade need an equivalent control plane. Without it, the only knobs are (a) shard-level capacity (too coarse — adding a shard requires a Raft membership change) and (b) external load-balancer rate limiting (no visibility into per-queue cost).

This document proposes per-queue token-bucket throttling, configured per-queue in queue meta, evaluated at the SQS-adapter layer on the leader, and surfaced as the same `Throttling.Sender` error AWS uses (so existing SDK retry/backoff logic engages naturally).

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **Per-queue rate limits** that an operator can set via `SetQueueAttributes` and read back via `GetQueueAttributes`. Limits are persisted on the queue meta record (one Raft commit, no separate keyspace).
2. **Per-action granularity** — `SendMessage` and `ReceiveMessage` have independent buckets so a slow consumer cannot pin the producer or vice versa. Batch verbs charge by entry count, not by call count.
3. **AWS-shape errors**: throttled requests return HTTP `400` + body `<Code>Throttling</Code>` with a `Retry-After` header. SDKs already special-case this code with exponential backoff; we do not invent a new code.
4. **Default-off**. Queues created before this feature, and queues created without explicit limits, are not throttled. Operators opt in per queue.
5. **No coordination per request**. Token replenishment is local to whichever node owns the bucket (the leader for the queue's shard); there is no Raft round-trip on the throttling check.
6. **Observable**: per-queue throttle counters are exposed via the existing Prometheus registry so dashboards can spot throttling before users do.

### 2.2 Non-Goals

1. **Cross-shard tenant fairness**. A noisy queue on shard A still affects other queues on shard A; the cross-shard story is the future "queue-level scheduling" RFC, not this one.
2. **Per-message-attribute limits** ("max 1MB/s of binary attributes"). Useful, but a bytes-budget on top of the count-budget is a follow-up.
3. **Cluster-wide limits** ("the entire elastickv deployment caps at 50,000 RPS"). Out of scope; operators put a global limit at the load balancer.
4. **Throttling on the read-only fast path** (`ListQueues`, `GetQueueUrl`, `GetQueueAttributes`). These are point reads or scans of the catalog; they do not touch user messages and have their own per-listener resource caps.
5. **Distributed token buckets** (Redis-backed, gossip-replicated, etc.). Per-leader buckets are sufficient for the queue-per-shard model; cross-shard distribution is a Phase 4 problem.

---

## 3. High-Level Design

```text
                  ┌────────────────────────────┐
                  │  SQS HTTP listener         │
   ┌──────────┐   │  (leader-resolved already) │
   │ producer │──▶│                            │
   └──────────┘   │  authorize → pickProtocol  │
                  │            │                │
                  │            ▼                │
                  │  per-queue bucket lookup   │
                  │            │                │
                  │   ┌────────┴─────────┐     │
                  │   ▼                  ▼     │
                  │  allow                throttle
                  │   │                  │     │
                  │   ▼                  ▼     │
                  │  handler         400 Throttling
                  └────────────────────────────┘
```

The check sits **between SigV4 authorisation and the existing handler dispatch**. By the time we reach this point we already know:

- the queue name (parsed from the request body's `QueueUrl` / `QueueName`);
- this node is the verified leader for the queue's shard (`isVerifiedSQSLeader`); any non-leader has been forwarded by `proxyToLeader` and re-evaluates the limit on landing;
- the action (`X-Amz-Target` for JSON, `Action` form parameter for query).

### 3.1 Where the bucket lives

A `bucketStore` instance hangs off `*SQSServer`. Internally:

```go
// adapter/sqs_throttle.go (new in implementation PR)
type bucketStore struct {
    // sync.Map rather than a single mu+map so the hot SendMessage /
    // ReceiveMessage path does not contend on a process-wide lock.
    // sync.Map's read-mostly optimisation matches the access pattern:
    // bucket lookup is overwhelmingly read (the bucket already exists),
    // and the rare insert-on-first-use happens once per (queue, action)
    // pair. Each bucket's own mutation (charge / refill) is guarded by
    // a per-bucket sync.Mutex inside *tokenBucket, scoped to one queue,
    // so cross-queue traffic never serialises on the same lock.
    // (Gemini medium on PR #664 flagged a single-mutex bucket store as
    // a hot-path contention point; this design avoids that.)
    buckets sync.Map  // map[bucketKey]*tokenBucket
    clock   func() time.Time
}

type bucketKey struct {
    queue  string
    action string  // "Send" | "Receive" | "*"
}

type tokenBucket struct {
    mu         sync.Mutex  // per-bucket; never held across the bucketStore
    capacity   float64     // burst size
    refillRate float64     // tokens per second
    tokens     float64     // current credit
    lastRefill time.Time
}
```

The `charge` operation:

1. `bucketStore.buckets.Load(key)` (lock-free read).
2. On miss, build the bucket from queue meta and `LoadOrStore` it (one-shot insert race tolerated — both racers will agree on the same configuration).
3. Acquire the bucket's own `mu`, refill based on elapsed time, take or reject the requested tokens, release `mu`.

No global lock is held during step 3; concurrent traffic on different queues runs in parallel.

**Cache invalidation on `SetQueueAttributes`**: when an operator updates the throttle config via `SetQueueAttributes`, the handler — *after* the Raft commit that persists the new `sqsQueueThrottle` — calls `buckets.Delete(key)` for every `bucketKey` belonging to the updated queue (`Send`, `Receive`, `*` — the canonical action values from the `bucketKey` struct above). Without this step the in-memory bucket would keep enforcing the old limits until the idle-eviction sweep removes the stale entry (default 1 h window), defeating the operator's intent to throttle a noisy tenant in real time. The `LoadOrStore` race with the `Delete` call is benign: the next request rebuilds from the freshly-committed meta, and the rebuilt bucket starts at full capacity (same semantics as the failover case documented below). Claude P1 on PR #664 caught the gap.

**Cache invalidation on `DeleteQueue`**: when a queue is deleted, the handler — *after* the Raft commit that purges the queue meta — calls `buckets.Delete(key)` for every `bucketKey` belonging to the deleted queue (`Send`, `Receive`, `*` — the canonical action values from the `bucketKey` struct above), mirroring the `SetQueueAttributes` path above. Without this step, a `DeleteQueue` immediately followed by `CreateQueue` with the same name would inherit the previous incarnation's in-memory bucket (current token balance, capacity, refill rate) until the 1 h idle-eviction sweep removes the stale entry — so an operator using `DeleteQueue`+`CreateQueue` to reset a noisy queue's state would be surprised to see the old throttle still in effect. (Alternative considered: include the queue `generation` in `bucketKey` so old and new incarnations cannot collide structurally; the explicit `Delete` is cheaper and matches the `SetQueueAttributes` pattern, so for Phase 3.C we keep `bucketKey = (queueName, action)` per the struct definition above — the `partition` field is added later by Phase 3.D, see §4.3 — and document the lifecycle requirement here.) Claude P1 on PR #664 fifth-round review.

The bucket map is per-process. On leader failover, a fresh bucket starts at full capacity on the new leader — there is no Raft replication of bucket state. **Why this is correct**: the worst-case behaviour of "fresh bucket on failover" is that a noisy queue gets one extra burst worth of bandwidth right after a leader change. Replicating bucket state would cost a Raft commit per token decrement, which would defeat the entire point of the token bucket. AWS's own rate limiter has the same property at region failover boundaries.

Buckets are created lazily on first request. They self-evict after a configurable idle window (default 1h) so a queue that goes silent does not keep its bucket entry forever.

### 3.2 Configuration on queue meta

`sqsQueueMeta` gains:

```go
type sqsQueueMeta struct {
    // ... existing fields ...

    // Throttle is the per-queue rate-limit configuration. Empty
    // value disables throttling (default). Set via SetQueueAttributes
    // with the AWS-style attribute names ThrottleSendCapacity /
    // ThrottleSendRefillPerSecond / etc. Persisted on the meta
    // record so a leader failover loads the configuration along
    // with the rest of the queue.
    Throttle *sqsQueueThrottle `json:"throttle,omitempty"`
}

type sqsQueueThrottle struct {
    SendCapacity         float64 `json:"send_capacity,omitempty"`
    SendRefillPerSecond  float64 `json:"send_refill_per_second,omitempty"`
    RecvCapacity         float64 `json:"recv_capacity,omitempty"`
    RecvRefillPerSecond  float64 `json:"recv_refill_per_second,omitempty"`
    DefaultCapacity      float64 `json:"default_capacity,omitempty"`
    DefaultRefillPerSecond float64 `json:"default_refill_per_second,omitempty"`
}
```

Setting `SendCapacity = 100, SendRefillPerSecond = 50` means: bursts up to 100 `SendMessage` requests, sustained 50 RPS, and any client overrun gets `Throttling`.

`Default*` fields catch any action not covered by an action-specific pair (so a future `PurgeQueue` rate limit costs nothing once defaults are wired).

**Config-field → bucket-action mapping** (Codex P1 on PR #664 sixth-round Codex review): the JSON config field-name prefixes use short forms (`Send*`, `Recv*`, `Default*`) but the in-memory `bucketKey.action` from §3.1 uses the canonical action vocabulary (`"Send"`, `"Receive"`, `"*"`). The mapping is fixed: `Send*` → `bucketKey{action:"Send"}`, `Recv*` → `bucketKey{action:"Receive"}`, `Default*` → `bucketKey{action:"*"}`. Cache invalidation paragraphs in §3.1 use the bucket-action vocabulary (the actual map keys). Use the config-field vocabulary when discussing the JSON contract (`SetQueueAttributes` payload, `GetQueueAttributes` response) and the bucket-action vocabulary when discussing the in-memory map. Implementation must apply this mapping when looking up buckets after a `SetQueueAttributes` commit.

The `SetQueueAttributes` validator enforces:

- All four `Send*` / `Recv*` fields must be either both zero (disabled) or both positive.
- Capacity ≥ refill (otherwise the bucket can never burst above the steady state).
- A hard ceiling per queue (e.g. 100,000 RPS) so a typo (`SendCapacity = 1e9`) does not silently mean "no limit at all" but rejects with `InvalidAttributeValue`.
- **Capacity ≥ max single-request charge** (Codex P1 on PR #664 sixth-round review). Per the §3.3 charging table, a `SendMessageBatch` charges up to 10 from the Send bucket and `DeleteMessageBatch` charges up to 10 from the Recv bucket (AWS caps both at 10 entries). Therefore: when `SendCapacity > 0` it must also be `≥ 10`, and when `RecvCapacity > 0` it must also be `≥ 10`. Without this rule, a queue configured with `SendCapacity = 5` enters a permanently unserviceable state for full batches — the bucket can never accumulate the 10 tokens a `SendMessageBatch(len=10)` requires, every full batch is rejected with `Throttling`, and `Retry-After` (§3.4) keeps reporting "wait N seconds" forever with no recovery path short of re-running `SetQueueAttributes`. The validator rejects with `InvalidAttributeValue` and an explicit message naming the per-bucket minimum so the operator sees the cause immediately. (`Default*` is exempt because the catch-all action set has no batch verb in scope for this PR; if a future PR moves a batch verb to the Default bucket, the same rule has to extend.)

### 3.3 Charging model

| Action | Charge |
|---|---|
| `SendMessage` | 1 from the Send bucket |
| `SendMessageBatch` | `len(Entries)` from the Send bucket (typically 1–10) |
| `ReceiveMessage` | 1 from the Recv bucket regardless of `MaxNumberOfMessages` |
| `DeleteMessage` | 1 from the Recv bucket (consumer-side action) |
| `DeleteMessageBatch` | `len(Entries)` from the Recv bucket |
| `ChangeMessageVisibility[Batch]` | same as Delete |
| Everything else (catalog ops, tag ops) | not throttled in this PR |

Batch verbs charge **before** dispatching individual entries. If the bucket has 3 tokens and the batch carries 10 entries, the call is rejected as a whole — partial throttling within a batch is harder to reason about and AWS itself rejects the whole call. Recorded in §11 (open questions) as a possible future tweak.

### 3.4 The `Throttling` envelope

On rejection:

| Protocol | Response |
|---|---|
| JSON | HTTP 400, body `{"__type":"Throttling","message":"Rate exceeded for queue '<name>' action '<action>'"}`, header `x-amzn-ErrorType: Throttling`, header `Retry-After: <seconds>` (computed per below) |
| Query | HTTP 400, body `<ErrorResponse><Error><Type>Sender</Type><Code>Throttling</Code><Message>...</Message></Error><RequestId>...</RequestId></ErrorResponse>`, headers as above |

`Retry-After` is computed from the *actual* refill rate AND the *requested* token count so neither slow refill nor large batches cause a busy-loop of premature retries (two consecutive Claude reviews on PR #664 caught both: first the `Retry-After: 1` constant lying for sub-1-RPS refill — `SendRefillPerSecond = 0.1` needs 10 s for the next token; then the formula's hardcoded numerator `1.0` lying for batch verbs that charge >1 token — a `SendMessageBatch` of 10 against `refillRate = 1.0` and 0 tokens needs 10 s, not 1):

```text
needed              := float64(requestedCount) - currentTokens
secondsToNextRefill := math.Ceil(needed / refillRate)
retryAfter          := max(1, int(secondsToNextRefill))   // never less than 1
```

`requestedCount` is the same value the charge step uses: `1` for single-message verbs, `len(Entries)` for batch verbs (§3.3). A `SendMessageBatch` of 10 against a bucket with `refillRate = 1.0` and 0 tokens correctly returns `Retry-After: 10`; a single `SendMessage` against `refillRate = 0.1` and 0 tokens correctly returns `Retry-After: 10`; the common case (single op, fast refill) keeps the floor of 1.

The minimum-1 floor matches `Retry-After`'s integer-second granularity (HTTP/1.1 §10.2.3). The validator (§3.2) keeps `refillRate > 0`, so the divide-by-zero guard is unnecessary in the formula above.

---

## 4. Implementation Path

### 4.1 Files touched

| File | Change |
|---|---|
| `adapter/sqs_throttle.go` (new) | `bucketStore`, `tokenBucket`, charging helper. ~250 lines. |
| `adapter/sqs_catalog.go` | Add `Throttle` field to `sqsQueueMeta`. Extend `applyAttributes` with the new `Throttle*` attribute names. Render the four Throttle fields in `queueMetaToAttributes` so `GetQueueAttributes("All")` surfaces them. |
| `adapter/sqs.go` | After `authorizeSQSRequest`, call `bucketStore.charge(queueName, action, count)`. On reject, write the `Throttling` envelope and return. |
| `adapter/sqs_throttle_test.go` (new) | Unit tests for bucket math (edge cases: idle drift, burst, partial refill, batch over-charge, default-off). ~300 lines. |
| `adapter/sqs_throttle_integration_test.go` (new) | End-to-end: configure a queue with low limits, send N messages back-to-back, confirm the (N+1)th gets `Throttling` with `Retry-After`. ~150 lines. |
| `monitoring/registry.go` | New counter `sqs_throttled_requests_total{queue, action}` and new **gauge** `sqs_throttle_tokens_remaining{queue, action}`. (Codex P2 on PR #664: tokens go up *and* down so a counter is the wrong instrument.) |
| `docs/design/2026_04_24_partial_sqs_compatible_adapter.md` §16.5 | Status update once this lands: TODO → Landed. |

### 4.2 OCC interaction

Throttling sits *outside* the OCC transaction — a rejected request never touches the coordinator. This is critical: the existing OCC retry loop in `sendMessageWithRetry` would otherwise loop on a permanent rate-limit failure, burning leader CPU. Confirmed by reading `sqs_messages.go: tryPurgeQueueOnce` and friends — none of them treat `sqsAPIError` codes as retryable.

### 4.3 Multi-shard correctness

Each queue is owned by exactly one shard (queue-per-shard routing in `kv/shard_router.go`). The leader of that shard owns the bucket. A request that lands on a follower is forwarded by `proxyToLeader` *before* the bucket check, so the bucket is always evaluated by the leader that is also doing the OCC dispatch — no risk of a follower checking against a stale bucket and the leader committing without checking.

Once Phase 3.D (split-queue FIFO) lands, a single queue may span multiple shards. At that point each *partition* gets its own bucket, **keyed by `(queueName, partitionID)`** — not by `MessageGroupId`. `MessageGroupId` is the *input* to `partitionFor`; using it directly as the bucket key would create one bucket per unique group value (unbounded, attacker-amplifiable map size, and hot groups would never share a budget). `partitionID` is bounded by `PartitionCount` so the worst-case bucket count per queue is tiny. The throttle proposal is forward-compatible: the bucket lookup key changes from `queueName` to `(queueName, partitionID)`, and the `bucketKey` struct in §3.1 grows a `partition uint32` field. Documented in §11. (Claude P1 on PR #664 caught the misnomer.)

---

## 5. AWS-Compatibility Surface

The throttling configuration is **non-AWS** — there is no `ThrottleSendCapacity` attribute in AWS SQS. Operators see it only when they explicitly read or set it via `GetQueueAttributes` / `SetQueueAttributes`. SDKs that strictly validate the attribute set will reject our extension on read; we mitigate by:

1. Adding the `Throttle*` names to `applyAttributes` only when the call is authenticated as an admin principal (the existing access-key-to-role mapping). Standard SQS clients see a 400 `InvalidAttributeName` when they try to set a `Throttle*` field, matching AWS's behaviour for unknown attributes.
2. Stripping `Throttle*` from the `GetQueueAttributes` response when the requesting principal is not an admin. Standard SQS clients see only the AWS-defined attribute set.
3. The throttling enforcement itself runs for every principal — admin or not. The configuration plane is admin-only; the data plane is universal.

---

## 6. Testing Strategy

1. **Bucket math unit tests** (`adapter/sqs_throttle_test.go`):
   - Fresh bucket allows up to capacity, then rejects.
   - After idle T seconds, refills exactly `T * refillRate` tokens (cap at capacity).
   - Batch charge of N rejects when `currentTokens < N`, no partial credit consumed.
   - Concurrent `charge` calls preserve the count invariant under `-race`.
   - Default-off: nil throttle config short-circuits to allow.

2. **End-to-end** (`adapter/sqs_throttle_integration_test.go`):
   - Configure a queue with `SendCapacity=10 SendRefillPerSecond=1`. (`SendCapacity` must be ≥ 10 per the §3.2 validator so the test mirrors a configuration the validator would actually accept; the earlier draft used 5 and would have been rejected at setup.)
   - Send 10 messages back-to-back → all 200.
   - Send 1 more immediately → 400 `Throttling` with `Retry-After: 1`.
   - Sleep 2s, send → 200 (refill happened).
   - Same shape for `ReceiveMessage`.

3. **Configuration round-trip**: `SetQueueAttributes` with throttle config → `GetQueueAttributes` returns the same values for an admin principal; returns 400 `InvalidAttributeName` for a standard principal.

4. **Cross-protocol parity**: throttled JSON and Query requests both surface `Throttling` (different envelope, same code).

5. **Failover behaviour** (3-node cluster): kill the current leader after 3 messages, confirm the next leader starts the bucket fresh and accepts up to capacity again. Log line records the failover so operators can correlate.

6. **Lint + race**: `go test -race ./adapter/...` must stay clean. The bucket store uses `sync.Mutex` (no atomic-only tricks); the race detector should have nothing to find.

---

## 7. Operational and Configuration

No new flags. Limits are per-queue, set via `SetQueueAttributes`. Defaults are zero (disabled).

Two new Prometheus instruments (Section 4.1) expose the throttling activity:

- `sqs_throttled_requests_total{queue, action}` — **counter**. Use `rate(...)` per queue in Grafana to spot the noisy tenant.
- `sqs_throttle_tokens_remaining{queue, action}` — **gauge** (Codex P2 on PR #664: token budgets go up *and* down over time, so a counter would mask the depletion that operators most need to see). Sample directly; trending toward zero is the early warning sign.

---

## 8. Failure Handling

1. **Bucket evicted while in flight**: a burst-after-eviction request creates a new bucket at capacity. Same as the failover case — at most one extra burst. Acceptable.
2. **Clock skew between leader and reqUest**: the bucket uses the leader's local wall clock for refill, so per-leader skew is irrelevant. Cross-leader skew is bounded by §3.1's "fresh bucket on failover".
3. **Over-saturated bucket store** (millions of queues): the eviction goroutine sweeps every `bucketEvictionInterval = 1m`. Per-queue map entries are ~80 bytes; 1M queues = 80MB worst case. Operators concerned about this can lower the eviction window.

---

## 9. Alternatives Considered

### 9.1 Replicate bucket state via Raft

Every `charge` proposes a bucket update through the FSM. **Rejected**: an extra Raft commit per `SendMessage` defeats the SQS adapter's existing throughput; AWS's own throttling does not replicate state.

### 9.2 External rate limiter (Envoy / NGINX in front)

**Rejected**: those layers do not see the `QueueName` (it's inside the request body), so per-queue limits are not expressible. They also do not know which node is the leader for a given queue.

### 9.3 In-memory per-IP rate limit

**Rejected**: producers behind NAT or a load balancer share an IP. AWS-shape per-queue limits are what operators actually want.

### 9.4 Token bucket on the catalog layer (one big shared bucket)

**Rejected**: defeats the multi-tenant goal. A single queue still pins everyone else.

---

## 10. Rollout Plan

| Phase | Content |
|---|---|
| 1 | Doc lands (this PR). No code yet. Operators have time to comment. |
| 2 | Implementation PR per §4.1. Default-off; existing queues unaffected. |
| 3 | Operators opt in per queue via `SetQueueAttributes`. Monitor `sqs_throttled_requests_total` for false positives. |
| 4 | Once stable, the partial doc's TODO list moves 3.C from TODO to Landed. |

---

## 11. Open Questions

1. **Partial-batch throttling**: should `SendMessageBatch` accept what fits and reject the rest, or all-or-nothing as proposed? AWS does the latter; sticking with AWS for parity is the conservative answer but a per-entry breakdown is more efficient under spiky load.
2. **Per-MessageGroupId throttling for FIFO**: a single producer hammering one MessageGroupId on a multi-partition FIFO (Phase 3.D) can still hot-spot one partition. A `(queue, group, action)` key would bound it, but adds map cardinality. Defer to the Phase 3.D design.
3. **Should the `Throttling` body name the offending action**? Right now the `<Message>` includes it. Useful for operators; SDKs ignore the message body. Trade-off is that the message is user-visible — a noisy producer's logs will spam the queue name. Probably fine; flagged here for review.
4. **Should idle-bucket eviction emit a metric**? Eviction is a normal lifecycle event but a sudden spike could indicate someone shotgun-creating queues. Probably yes; cheap to add.
