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
    // for a slot before giving up. Sized comfortably above the Raft
    // in-flight queue's drain time at 1 Gbps (1024 × 4 MiB / 125 MB/s
    // ≈ 33 s) so a transient stall does not surface as 503; longer
    // stalls indicate the cluster is genuinely overloaded and rejection
    // is the right signal.
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

// reserve charges (bytes) units against the semaphore. Returns a
// release closure that the caller MUST call exactly once. Returns
// ErrAdmissionExhausted if the deadline elapses before the budget
// is available.
func (a *putAdmission) reserve(ctx context.Context, bytes int64) (func(), error) { … }
```

`reserve` is non-trivial because we need partial-charge semantics:

1. Pre-charge `Content-Length` total at request entry (admission A).
   The release fires in a deferred handler on the request goroutine
   regardless of success / failure.
2. Per-batch sub-lease (admission B) is *not* a separate budget — it's
   a synchronisation primitive on the existing semaphore. The PUT
   handler acquires `s3ChunkSize × s3ChunkBatchOps = 4 MiB` units
   before reading the next 4 MiB window from the body and releases
   them on `coordinator.Dispatch` ack. This way the same budget covers
   both the "request is in flight" and "this batch is buffered for
   Raft" phases.

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
elastickv_s3_put_admission_rejections_total      counter (label: stage="prereserve" | "perbatch")
elastickv_s3_put_admission_wait_seconds          histogram (label: stage)
```

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
- **`coordinator.Dispatch` retries.** Today the S3 path has its own
  retry loop (`s3TxnRetryMaxAttempts = 8`). Admission must release
  its budget around retries, otherwise a long retry chain double-
  counts.

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
- **aws-chunked overcharge.** A small chunked PUT charges 5 GiB
  against the budget until M4 lands. We accept this temporarily
  because aws-chunked traffic is rare in practice and the
  conservative cap fails safe.

## 7. Out of scope (future work)

- Per-bucket admission classes (e.g. system buckets get their own
  budget). Punted to the workload-isolation rollout.
- Coordinated admission across the multi-region read replica path
  proposed in `docs/design/2026_04_18_proposed_raft_grpc_streaming_transport.md`.
- Token-bucket rate-shaping (e.g. bytes-per-second). The current
  proposal only bounds *concurrent* bytes; rate-shaping is a separate
  policy choice.
