# S3 PUT admission control

> **Status: Proposed**
> Author: bootjp
> Date: 2026-04-25
>
> Companion to PR #636 (`s3ChunkBatchOps = 4`, Raft entry size aligned
> with `MaxSizePerMsg = 4 MiB` per PR #593) and to the workload-class
> isolation proposal (`docs/design/2026_04_24_proposed_workload_isolation.md`).
> Where PR #636 fixes the *per-entry* memory accounting and the
> isolation doc keeps Raft's CPU budget separate from heavy command
> paths, this doc bounds the *aggregate* in-flight memory that S3
> PUT body bytes can pin in the leader's Raft pipeline regardless of
> how many clients are uploading at once.

---

## 1. Problem

Even with PR #636 reducing the per-entry size to 4 MiB and aligning
with `MaxSizePerMsg`, the **aggregate** in-flight memory bound is
governed by client concurrency, which the server cannot currently
limit:

```
leader-side worst-case  =  concurrent_PUTs × pending_entries_per_PUT × entry_size
                        ≈  concurrent_PUTs × MaxInflight × 4 MiB
```

A single PUT's pipeline is already capped by `MaxInflight = 1024` Raft
in-flight messages times 4 MiB per entry = 4 GiB / peer (the bound PR
#593 advertises). What multiplies that bound is concurrent uploads:

- 4 simultaneous 5 GiB PUTs on a 4-vCPU 8 GiB-RAM node can pin
  `4 × min(MaxInflight × 4 MiB, body_remaining)` on the leader. If a
  follower stalls (GC pause, slow disk fsync) for several seconds, that
  worst case is realised before backpressure kicks in.
- The 2026-04-24 incident showed that one workload class can wedge the
  Go runtime for the entire cluster. S3 PUT body bytes pinned in the
  Raft pipeline play the same role as XREAD blobs in the Redis path:
  they grow with client behaviour, not with anything the operator can
  rate-limit per-handler.
- `GOMEMLIMIT = 1800 MiB` plus `--memory = 2500m` (PR #617) set the
  upper bound on heap, but they do not provide *flow control*. The Go
  GC starts thrashing well before the limit, and memwatch (PR #612)
  triggers graceful shutdown — a recovery mechanism, not a steady-state
  control.

There is no hard ceiling today on the aggregate body bytes that S3
puts onto the Raft pipeline. PR #636 makes the per-entry slot
predictable; this proposal makes the *number of slots in flight*
predictable too.

## 2. Goals & non-goals

**Goals**

- Hard cap on the total S3 PUT body bytes that have been read from
  HTTP but not yet committed to Raft. This cap is a tunable property
  of the S3 server, not of any single PUT.
- Steady-state behaviour where new PUTs are admitted at the rate Raft
  can drain. Bursting clients see HTTP 503 with `Retry-After`, not
  silent ballooning of leader memory.
- Composable with existing memwatch + GOMEMLIMIT. Admission failure
  must be visible (metric + log line per rejected request) so an
  operator can size the cap against a given memory budget.
- Read path is unaffected. GET / HEAD / LIST do not consume the same
  budget.

**Non-goals**

- No global QPS quota across all S3 verbs. Other verbs (GET, multipart
  initiate / complete) have well-bounded memory and don't need this.
- No per-bucket / per-key fairness. Fairness across tenants is a
  separate workload-class concern (handled by the workload isolation
  proposal). This doc only fixes the aggregate ceiling.
- No backpressure-via-TCP (`SO_RCVBUF` shrink). Decoupling from kernel
  buffers keeps the budget testable and avoids cross-OS surprises.
- No multi-region / multi-cluster admission. One leader's perspective.

## 3. Design

### 3.1 Where the budget is checked

Two natural insertion points exist on the PUT path:

| Insertion point | Granularity | When the request is rejected |
|---|---|---|
| (A) Before `prepareStreamingPutBody` accepts the first byte | Whole-object | Pre-Raft, before any reads from the body |
| (B) Inside the `flushBatch` loop, before `coordinator.Dispatch` | Per-batch | Mid-stream, after `s3ChunkBatchOps` chunks are buffered |

Recommended: **both, at different scales**.

- **(A) is the request-admission cap.** Use the `Content-Length`
  header (rejected when absent for PUTs that are not aws-chunked) to
  pre-charge the budget. If the budget would be exceeded we reply 503
  immediately. This matches how AWS S3 itself handles
  `SlowDown` / `ServiceUnavailable` and is the cheapest case to
  surface — the body is never read from the socket.
- **(B) is the in-flight cap.** Even after a request is admitted, its
  4 MiB batches can pile up if Raft becomes slow. The flush loop
  acquires a per-batch lease (4 MiB) from a shared semaphore *before*
  reading the next chunk window. If the semaphore is empty for longer
  than `dispatchAdmissionTimeout` (proposed default 30 s), the PUT
  fails with 503 mid-stream. Dispatch latency stays bounded by the
  semaphore size rather than by client behaviour.

```
client ─[Content-Length]─► (A) reserve full body bytes
                                │
                                ▼
                        prepareStreamingPutBody
                                │
                                ▼
        (B) acquire 4 MiB slot ◄┐ (released on Dispatch ack)
                                │
                                ▼
        coordinator.Dispatch ───┘
```

### 3.2 Concrete cap values

Default cap:

```go
const (
    // s3PutAdmissionMaxInflightBytes is the hard ceiling on S3 PUT body
    // bytes accepted by this node but not yet committed to Raft. Picked
    // so that, on a 3-node cluster with MaxInflight × MaxSizePerMsg =
    // 4 GiB / peer per PR #593, all in-flight PUT bytes plus their Raft
    // replication shadow stay under one quarter of GOMEMLIMIT (1800 MiB
    // per PR #617) — leaving headroom for Lua, scan buffers, and Pebble
    // memtables.
    s3PutAdmissionMaxInflightBytes = 256 << 20 // 256 MiB
    // dispatchAdmissionTimeout is how long a per-batch flush will wait
    // for a slot before giving up. The 256 MiB cap drains in ~2 s at
    // 1 Gbps under steady-state Raft throughput (256 MiB / 125 MB/s),
    // so 30 s is *not* sized against normal drain — it is the budget
    // for a transiently stalled follower (GC pause, slow disk fsync,
    // bounded leader re-election) to recover before we conclude the
    // cluster is genuinely overloaded. Longer stalls surface as 503,
    // which is the right signal: at that point the right action is
    // operator intervention (scale out, investigate the stall), not
    // continued accumulation.
    dispatchAdmissionTimeout = 30 * time.Second
)
```

The cap is intentionally **per-node** rather than cluster-wide:
admission is enforced on the node receiving the HTTP request, which is
also the node whose memory is at risk. Clients hitting a different
node simply get a different budget.

Both values are exposed as env vars (`ELASTICKV_S3_PUT_ADMISSION_MAX_INFLIGHT_BYTES`,
`ELASTICKV_S3_DISPATCH_ADMISSION_TIMEOUT`) with the constants as
defaults, following the pattern PR #593 established for
`ELASTICKV_RAFT_MAX_INFLIGHT_MSGS` etc.

### 3.3 Data structure

```go
type putAdmission struct {
    semaphore chan struct{}   // capacity == max / s3ChunkSize, charges in 1 MiB units
    inflight  atomic.Int64    // metric mirror; semaphore is the source of truth
}

func newPutAdmission(maxBytes int64) *putAdmission { … }

// peekHeadroom is admission A. It returns ErrAdmissionExhausted
// without acquiring slots when the requested byte count exceeds
// the *current* free capacity of the semaphore. It does NOT take
// out a reservation — the only effect is "fail fast at request
// entry instead of partway through the body" — so it cannot
// double-count against admission B.
func (a *putAdmission) peekHeadroom(bytes int64) error { … }

// acquire is admission B. It blocks until (bytes / s3ChunkSize)
// slots are available or ctx fires. The returned release closure
// MUST be called exactly once. If `bytes > capacity * s3ChunkSize`
// (a malformed client whose frame exceeds the entire budget),
// returns ErrAdmissionExhausted *immediately* without waiting —
// otherwise we would block until ctx (typically
// dispatchAdmissionTimeout) for a request that can never fit.
func (a *putAdmission) acquire(ctx context.Context, bytes int64) (func(), error) { … }
```

The two-step contract avoids the double-charge / unbounded-window
hazard the obvious "A is also an acquire" design would have:

1. **Admission A — request-entry headroom check (peek only).** When
   a PUT arrives with `Content-Length: N`, the handler calls
   `peekHeadroom(N)`. If the result is `ErrAdmissionExhausted`,
   reply 503 immediately and never read from the body. If `nil`,
   admission A is done — no slots have been taken out of the
   semaphore. This is intentionally racy with concurrent PUTs:
   admission A only promises "at the moment we asked, the budget
   *would have fit* this request"; it does not reserve the budget.
2. **Admission B — per-batch acquire/release (the only path that
   touches the semaphore).** The PUT handler then loops the body
   in `s3ChunkSize × s3ChunkBatchOps = 4 MiB` windows; each window
   acquires 4 MiB worth of slots via `acquire`, reads the next
   window from the body, dispatches, and releases on Dispatch
   ack. This is the bound that actually holds — at any instant the
   sum of held slots across all in-flight PUTs cannot exceed the
   semaphore's capacity. If admission A's racy estimate turns out
   to be wrong (another PUT raced in between A's check and the
   first B-acquire), the first B-acquire blocks until the
   contending PUT releases or `dispatchAdmissionTimeout` fires.

The semaphore is therefore charged **only by admission B**. Bytes
in flight = `held_B_slots × s3ChunkSize`, full stop; admission A is
a fast-fail gate, not a reservation. This is the model
implementations MUST follow — a "both A and B charge" design would
double-count every PUT against itself and an admission-A-only
design would lose its bound the moment a PUT's body exceeded its
declared `Content-Length` (chunked transfers, malformed clients).

### 3.3.1 aws-chunked transfers (`Content-Length: -1`)

A naïve "reserve `s3MaxObjectSizeBytes` (5 GiB) up front" is rejected:
on default tunables (`s3PutAdmissionMaxInflightBytes = 256 MiB`) a
single chunked PUT would consume **20×** the entire budget at request
admission time, head-of-line-blocking every other PUT until the
chunked stream finishes — exactly the failure mode admission control
exists to prevent. We therefore split chunked admission across two
mechanisms instead of pre-charging:

1. **Bootstrap reservation = `s3RaftEntryByteBudget` (4 MiB)** at
   request entry. This is enough to admit the request and let the
   awsChunkedReader produce its first decoded window. Chunked PUTs
   are not "free" — they still must beat the same admission queue
   as fixed-length PUTs at the per-batch level.
2. **Pay-as-you-decode** thereafter, charged via an
   `awsChunkedReader` progress callback. Each decoded chunk frame
   (typically up to 64 KiB on the wire after framing overhead)
   acquires a slot equal to the bytes about to flow into Pebble; the
   slot is released once the corresponding `coordinator.Dispatch`
   acks. This is the same path admission B uses for fixed-length
   PUTs — chunked traffic just hooks into it incrementally instead of
   pre-charging the worst case.

Failure modes:

- If the awsChunkedReader produces frames faster than Raft drains, the
  per-batch acquire blocks (capped by `dispatchAdmissionTimeout`).
  Beyond that timeout, mid-stream 503 closes the connection. The
  legacy "reserve 5 GiB" approach would have surfaced as 503 *at
  request entry* for unrelated PUTs; this approach surfaces as
  mid-stream 503 for the chunked PUT itself, which is the right
  blame attribution.
- If the awsChunkedReader frame size ever exceeds
  `s3RaftEntryByteBudget` (a malformed client), the per-batch acquire
  asks for more than the cap allows and we 503 immediately — same
  as a fixed-length PUT whose `Content-Length` exceeds the global
  cap.

This change moves chunked admission from M4 (originally "deferred
optimisation") into M1 (the first shippable milestone). M1 ships
with the progress-callback wired *unconditionally* for all chunked
PUTs; an env-var switch falls back to "bootstrap-only" charging
without the per-decode credit if a corner case requires it
(`ELASTICKV_S3_PUT_ADMISSION_CHUNKED_INCREMENTAL=false`, default
`true`). The fallback path keeps the 5 GiB-reservation hazard
behind an explicit operator decision rather than letting it
materialise by default.

### 3.4 Failure mode

- `503 Service Unavailable` with `Retry-After: 1` (small, jittered).
  AWS S3 SDK clients (boto3, aws-sdk-go-v2) auto-retry this code with
  exponential backoff out of the box.
- Body for the response is the standard S3 XML error envelope:
  `<Code>SlowDown</Code><Message>Reduce your request rate</Message>`.
  This is the AWS-defined code for admission rejection and matches
  what real S3 returns.
- Mid-stream rejection (admission B) closes the connection with a
  `connection: close` header so partial body reads do not corrupt the
  client's pipeline. The PUT handler also calls
  `cleanupManifestBlobs` for any partial blobs that already landed in
  Pebble.

### 3.5 Metrics

```
elastickv_s3_put_admission_inflight_bytes        gauge
elastickv_s3_put_admission_rejections_total      counter (labels:
                                                    stage    = "prereserve" | "perbatch",
                                                    protocol = "fixed-length" | "chunked")
elastickv_s3_put_admission_wait_seconds          histogram (labels: stage, protocol)
```

The `protocol` label distinguishes fixed-length PUTs (those with a
declared `Content-Length`, hitting admission A's `peekHeadroom`)
from aws-chunked PUTs (admission via §3.3.1's pay-as-you-decode).
This split is what makes the chunked-PUT 503 surface (§6) and the
rolling-upgrade alerting story actionable: a spike on
`stage="perbatch", protocol="chunked"` points at "chunked clients
beat Raft drain"; a spike on `stage="prereserve",
protocol="fixed-length"` points at "client concurrency exceeds
the per-node aggregate cap." Without the dimension the two
failure modes are indistinguishable in a single counter.

Grafana panel: inflight gauge with the cap as a horizontal line so
the operator sees how often the system saturates. Rejection rate
suggests bumping the cap or scaling out (more nodes spreads PUT load).

## 4. Interaction with related subsystems

- **PR #636 (entry size alignment).** The 4 MiB per-batch unit is
  the natural admission grain. The two changes are independent:
  alignment is necessary for the per-peer Raft bound to be correct;
  admission is necessary for the *aggregate* bound to be hard.
- **PR #612 (memwatch graceful shutdown).** Continues to function as
  the last-resort safety net. Admission control should fire at well
  below the memwatch threshold (`s3PutAdmissionMaxInflightBytes` is
  ~14% of `GOMEMLIMIT`) so memwatch sees a much lower steady-state
  pressure and the graceful-shutdown path stays a rare event.
- **Workload isolation proposal.** That doc proposes per-class CPU
  reservation for Raft. Admission control is the memory-axis sibling.
  Both are needed — limiting CPU does not bound queue depth.
- **`coordinator.Dispatch` retries.** Today the S3 path has its
  own retry loop (`s3TxnRetryMaxAttempts = 8` with exponential
  backoff capped at `s3TxnRetryMaxBackoff = 32 ms`). The admission
  contract is **hold-through-retry**: the per-batch slot acquired
  in admission B is released exactly once, on the *final* outcome
  of the retry chain (success ack, terminal error, or
  `dispatchAdmissionTimeout` expiring), not between attempts.
  Rationale: the bytes are still buffered in the PUT handler's
  pendingBatch slice for the entire retry window, so the budget
  must reflect them; a release-between-retries scheme would let a
  second PUT proceed while the first is still memory-resident,
  breaking the bound. The S3 PUT path uses the inbound
  `*http.Request` context for `coordinator.Dispatch` (no
  S3-specific Dispatch timeout — the HTTP server's
  `writeTimeout` / client-side cancellation is the upper bound on
  one Dispatch attempt), so the wall-clock cost of holding the
  slot through one full retry chain is bounded by
  `s3TxnRetryMaxAttempts × (single_dispatch_budget + s3TxnRetryMaxBackoff)`
  where `single_dispatch_budget` is whatever the request context
  permits at that moment. If the retry chain duration ever
  exceeds `dispatchAdmissionTimeout` the per-batch acquire on the
  *next* batch surfaces as 503 — the right failure mode
  (chronic dispatch failure → caller learns instead of silently
  consuming the budget).

## 5. Implementation plan

| Milestone | Scope | Risk |
|---|---|---|
| M1 | Add `putAdmission` type + per-node singleton + fixed-length `Content-Length` admission. Wire `prepareStreamingPutBody` to acquire / release. **aws-chunked progress-callback admission** (§3.3.1) ships in this milestone too — the conservative 5 GiB pre-charge fallback only sits behind `ELASTICKV_S3_PUT_ADMISSION_CHUNKED_INCREMENTAL=false`. Metric scaffolding (gauge + counter). | Medium. Chunked progress callback needs `awsChunkedReader` to expose a hook. |
| M2 | Per-batch admission B inside `flushBatch` for fixed-length PUTs. Add `dispatchAdmissionTimeout`. Mid-stream 503 with cleanup. (Chunked PUTs already use this path through their incremental charging.) | Medium. Cleanup path on partial failure. |
| M3 | Env-var tunables. Histogram metric. Grafana panel. | Low. |
| M4 | Per-tenant / per-bucket admission classes (handed off to the workload-isolation rollout). | Medium. Out-of-scope for the v1 cap. |

### Rolling upgrade

Admission is purely additive on the request entry path: a node
without the cap behaves identically to a node with the cap set
infinitely high. A mixed cluster (some nodes M1, some still on
`main`) is therefore safe — clients hitting the upgraded node see
admission, clients hitting an old node see no admission, but
neither path corrupts state. The default cap is intentionally
generous enough that even single-node M1 traffic falls below the
threshold under typical load, so the rollout signature is
"503 SlowDown rate goes from 0 to negligible" rather than a step
function. Operators can pin
`ELASTICKV_S3_PUT_ADMISSION_MAX_INFLIGHT_BYTES=$((1<<63))` to
disable the cap on M1 nodes during the burn-in window if desired.

The aws-chunked progress-callback path is the only behaviour
change visible to clients: a chunked PUT that would have succeeded
under the old "no admission" code can now 503 mid-stream when Raft
drain falls behind. This is by design — that is the failure mode
admission control exists to surface — but operators should expect
to see chunked-upload 503s where there were none before. The
`stage="perbatch", protocol="chunked"` rejection-counter label
isolates this signal; bumping the cap or
`ELASTICKV_S3_PUT_ADMISSION_CHUNKED_INCREMENTAL=false` (with the
HoL hazard re-introduced as a known trade-off) restores legacy
behaviour during incident response.

Acceptance criteria:

- `go test ./adapter/ -short -run TestS3PutAdmission` covers reject /
  admit / mid-stream-timeout paths.
- A loaded test that opens 32 concurrent PUTs of 100 MiB each must
  hold leader memory below `s3PutAdmissionMaxInflightBytes + epsilon`
  for the duration of the test.
- No regression in `Test_grpc_transaction` (which is currently the
  longest leader-stress test).

## 6. Risks

- **Tail-latency for legitimate clients.** A long-running PUT that
  loses a 4 MiB slot mid-stream returns 503 even though it is making
  progress. Mitigated by `dispatchAdmissionTimeout = 30s`, well above
  the steady-state Raft drain time. If we observe spurious 503s in
  practice, drop the timeout into a config knob and tune.
- **Operator confusion.** "Why does S3 return 503 when CPU is at
  20%?" Mitigated by a sharp Grafana panel and a clear `Retry-After`
  value so SDK behaviour is predictable.
- **New chunked-PUT 503 surface.** Pay-as-you-decode admission
  (§3.3.1) ships in M1 alongside fixed-length admission, so the
  legacy 5 GiB pre-charge hazard does not materialise as a
  steady-state risk. The residual risk it introduces is the
  inverse: a chunked PUT that would have silently succeeded under
  the no-admission code can now 503 mid-stream when Raft drain
  falls behind. This is by design — that is the failure mode
  admission control exists to surface — but it is the only
  client-visible behaviour change in M1 and is what operators
  should expect to see in dashboards. The
  `stage="perbatch", protocol="chunked"` label on the rejection
  counter (§3.5) isolates the signal; the operator escape hatch
  is `ELASTICKV_S3_PUT_ADMISSION_CHUNKED_INCREMENTAL=false`, which
  reverts to bootstrap-only charging at the cost of
  re-introducing the 5 GiB head-of-line hazard.

## 7. Out of scope (future work)

- Per-bucket admission classes (e.g. system buckets get their own
  budget). Punted to the workload-isolation rollout.
- Coordinated admission across the multi-region read replica path
  proposed in `docs/design/2026_04_18_proposed_raft_grpc_streaming_transport.md`.
- Token-bucket rate-shaping (e.g. bytes-per-second). The current
  proposal only bounds *concurrent* bytes; rate-shaping is a separate
  policy choice.
