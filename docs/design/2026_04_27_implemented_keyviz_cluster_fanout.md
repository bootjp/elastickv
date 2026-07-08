---
status: implemented
phase: 2-C
parent_design: docs/admin_ui_key_visualizer_design.md
date: 2026-04-27
---

# KeyViz Cluster Fan-out (Phase 2-C)

## 1. Background

Phase 2-A (server-side sampler) and Phase 2-B (SPA heatmap) ship a
heatmap that shows **only the local node's view**. Per the parent
design §5.1 reads are recorded by the follower that serves them and
writes are recorded by the leader; pointing the SPA at a single node
therefore shows a partial picture — operators looking at a follower
see no writes, operators looking at a leader see only that leader's
writes.

Parent design §9.1 defines the desired end state: the admin layer
fans out to every node, merges responses with rules that respect
Raft leadership, and renders one combined heatmap with degraded-node
status surfaced inline.

**Rollout notes** (a round-1 review finding on PR #685 asked for explicit
rolling-upgrade and zero-downtime cutover plans). This change is
**off by default** (`--keyvizFanoutNodes` empty) and lives entirely
on the admin path — no data-plane impact, no schema migration, no
state stored on disk by this feature. Mixed-version clusters mid-
rollout are correct by construction:

- Wire format additions (§5) are forwards-compatible: an old node
  that does not know about `Fanout` / `conflict` simply omits them
  and the new aggregator treats the missing values as the merge-rule
  identity. An old SPA against a new server ignores the extra
  fields and renders the local view it always did.
- Operators flip `--keyvizFanoutNodes` per-node during the rollout.
  Until every node has the new binary AND the flag set, some peer
  HTTP calls will 404 — that surfaces as `ok=false` in the
  per-node status array, not as a failed user request.
- No dual-write proxy / blue-green required because there is no
  state to migrate; the worst-case partial deploy is "node X
  reports `ok=false`", not data loss.

This design scopes the implemented **Phase 2-C** cluster view:
cluster-wide heatmap, degraded-node banner, and the per-cell Raft
identity needed for the canonical §9.1 write merge.

## 2. Scope

### 2.1 In scope

- A static node list configured via `--keyvizFanoutNodes` (comma-
  separated admin HTTP endpoints).
- An aggregator inside `internal/admin` that issues parallel GETs to
  every configured node's `/admin/api/v1/keyviz/matrix`, merges the
  responses, and returns one combined `KeyVizMatrix` plus a per-node
  status array.
- Two merge rules, justified in §4:
  - **Reads**: sum across nodes (each node serves distinct local
    follower reads).
  - **Writes**: canonical per-cell fan-out merge keyed by
    `(bucketID, raftGroupID, leaderTerm, column)`: max within one
    `(group, term)` identity, sum across distinct leader terms for
    the same group/window, and surface `conflicts[j]=true` when
    two sources disagree inside the same `(group, term, column)`.
    The row-level `conflict` field remains as the OR of
    `conflicts[]` for older SPAs. If any source lacks
    `raftGroupID` or `leaderTerm`, that cell falls back to the
    legacy max-merge so mixed-version clusters never over-count.
- Degraded-mode response: when N nodes respond and M < N succeed,
  the response carries `{node, ok, error}` per node and the SPA
  shows a banner.
- Behaviour preserved when `--keyvizFanoutNodes` is unset: the
  endpoint serves the local view exactly as it does today.

### 2.2 Explicitly NOT in scope

- **Membership discovery via `GetClusterOverview`** and a membership
  cache. Static `--keyvizFanoutNodes` is the implemented Phase 2-C
  contract; discovery is a later operator-ergonomics feature, not a
  merge-semantics prerequisite.
- **Standalone `cmd/elastickv-admin` binary** (parent §3.1).
  Deferred indefinitely — we have not seen a use case the embedded
  in-node admin does not cover, and this design holds the
  embedded path as the go-forward.
- **Response-payload caching across browser polls.** Each request
  fans out fresh. Coalescing concurrent requests is a tomorrow
  problem if it shows up in benchmarks.

## 3. Configuration

```sh
elastickv \
  --address 127.0.0.1:50051 \
  --adminAddress 127.0.0.1:8080 \
  --keyvizEnabled \
  --keyvizFanoutNodes=10.0.0.1:8080,10.0.0.2:8080,10.0.0.3:8080
```

- Self is included implicitly: `internal/admin` matches each
  `--keyvizFanoutNodes` entry against `--adminAddress` and skips the
  network round-trip for the local entry. This keeps the
  configuration symmetric across all nodes (every node lists every
  node, including itself) so an operator can stamp the same flag
  onto every host. The matching rule is:
  - **Exact host:port equality** to `--adminAddress` is the primary
    case (e.g. both say `10.0.0.1:8080`).
  - **Wildcard-bind handling**: when the listener bound to
    `0.0.0.0` / `::`, an entry with the same port and a loopback
    host (`127.0.0.1` / `localhost` / `::1`) also counts as self.
    Operators who bind `--adminAddress=0.0.0.0:8080` but stamp
    `127.0.0.1:8080` into every node's flag still get the
    skip-fires behavior.
  - **Anything else** is treated as a peer. A reverse-proxy or
    DNS-aliased entry that names the same node by a different host
    will not match — the fan-out makes a loopback HTTP call to
    itself. This is harmless (it degrades to one extra round-trip
    per request) but wasteful; operators should prefer the literal
    `--adminAddress` value in the flag.
- Empty (or unset) flag → fan-out disabled, current behaviour.
- Each entry is a host:port. TLS is out of scope for Phase 2-C
  (intra-cluster admin traffic is assumed to ride a private
  network); the HTTP client uses `http://`. A follow-up will
  introduce `--keyvizFanoutTLS` once the rest of the admin path
  has TLS too.
- **Auth (Phase 2-C MVP)**: the aggregator forwards a whitelist of
  the inbound user's cookies (`admin_session` + `admin_csrf` only)
  on every peer call. The peer's `SessionAuth` middleware verifies
  `admin_session` against its own `--adminSessionSigningKey`;
  cluster nodes already share that key for HA, so a cookie minted
  on node A is verifiable on node B without any new infrastructure.
  Unrelated cookies the browser may carry (analytics, feature
  flags, other-app sessions on the same domain) are dropped at the
  fan-out boundary so they are not leaked across the internal
  network (security-medium review finding on PR #692).
- **Recursion guard**: peer requests carry an `X-Admin-Fanout-Peer`
  header. The receiving handler short-circuits its own fan-out
  when this header is set; without the guard, a symmetric
  configuration (every node lists every other node) would generate
  O(N²) HTTP calls per browser poll as each peer recursively
  fanned out. (P1 review finding on PR #692.)
  - The earlier draft of this paragraph said "anonymous on a
    private network" — that was wrong: the receiving side enforces
    session auth, so anonymous calls are rejected with 401 and the
    cluster heatmap collapses to "1 of N nodes responded". The
    cookie-forwarding scheme above is what actually works.
  - **Trust model**: forwarding a session cookie to peers is safe
    when (a) every peer is operator-configured and trusted, and
    (b) the network is private (cookies are HttpOnly but ride
    plaintext HTTP for now). `--keyvizFanoutNodes` is the
    operator's explicit trust list. **Do NOT point
    `--keyvizFanoutNodes` at an untrusted host** — the user's
    admin session would be replayed there.
  - Per-call timeout: 2 s default, override via
    `--keyvizFanoutTimeout`.
- **Auth (Phase 2-C+)**: a follow-up replaces cookie forwarding
  with a short-lived **inter-node** token derived from
  `ELASTICKV_ADMIN_SESSION_SIGNING_KEY`. Pre-shared, NOT a replay
  of the browser cookie — re-using the cookie couples browser
  session TTL to inter-node call validity, and a compromised
  browser session gains peer-call authority. The follow-up
  decouples the two paths so the inter-node call survives a
  browser-session expiry and a compromised cookie doesn't extend
  laterally.

## 4. Merge rules

The parent design §9.1 specifies the **canonical** merge rule:
collapse write samples to one value per
`(bucketID, raftGroupID, leaderTerm, windowStart)`, surface a
conflict when distinct sources disagree inside the same identity,
and sum across `leaderTerm` values for the same `(group, window)`.
The implemented JSON and proto rows carry `raft_group_ids` /
`leader_terms` arrays parallel to `values[]`, so leadership can flip
inside the requested window without losing per-column identity.

### 4.1 Reads → sum across nodes

Each node records the reads it served. A read served by node A is
not also served by node B. Summation is exact.

### 4.2 Writes → per-cell group/term dedupe

Under stable leadership, exactly one node (the current leader for
the route's group) records writes for a given
`(bucketID, raftGroupID, leaderTerm, column)`; other nodes report
0. Zero-valued contributions are ignored for write conflict
purposes, so follower zeros against the leader's non-zero value do
not perturb the result or raise `conflicts[j]`.

Under a leadership flip inside the requested window, the ex-leader
and new leader report different `leaderTerm` values in different
columns or even in the same column. The merge:

- takes max within the same `(bucketID, raftGroupID, leaderTerm,
  column)` identity, because Raft has at most one leader per group
  and term;
- sums the resolved values across distinct `leaderTerm` values for
  the same `(bucketID, raftGroupID, column)`, preserving both sides
  of a real leadership transition;
- sets `conflicts[j]=true` only when two sources report different
  non-zero values for the same `(group, term, column)` identity.

For mixed-version clusters or cells whose identity is unknown
(`raftGroupID == 0` or `leaderTerm == 0`), that cell falls back to
the legacy max-merge. This fallback is deliberately conservative:
it may under-count a transition involving an old peer, but it does
not over-count overlapping unknown and known-term samples.

### 4.3 Bytes counters

`read_bytes` follows `reads` (sum across nodes). `write_bytes`
follows `writes` (the same group/term dedupe, sum-across-terms, and
unknown-identity max fallback).

### 4.4 Row identity

Two rows from different nodes belong to the same logical row when
their `BucketID` matches. After the aggregator has collected **all**
per-node responses (i.e. waited for every peer's deadline to
resolve), `Start` / `End` / `Aggregate` / `RouteCount` / `RouteIDs`
are taken from the **lowest-indexed node in `--keyvizFanoutNodes`
whose response is non-empty for the row**. Selection by config-list
position rather than wall-clock arrival order is the load-bearing
property: peers respond in non-deterministic order, so picking on
first arrival would let `Start`/`End` flap between polls for the
same bucket. If two nodes disagree on `Start`/`End`
for the same `BucketID`, that indicates a routing-catalog
divergence the operator should investigate. Phase 2-C does not ship
a structured warning field for this case; the aggregator keeps the
deterministic metadata winner and still serves the merged response.
Surfacing catalog-divergence warnings in the per-node status payload
is a future wire extension.

### 4.5 Time alignment

All nodes use `keyvizStep` (default 1 s), but the shipped Phase 2-C
aggregator does not receive `keyvizStep` and does not bucket-align
peer column timestamps. It merges exact `column_unix_ms` values from
the local and peer matrices. Two nodes whose flush goroutines fire at
different millisecond offsets therefore produce adjacent merged
columns even when they represent the same logical sampling window.

Specifics:

- The aggregator unions exact `column_unix_ms` entries before merging.
- A column present in node A but absent in node B contributes
  only A's values for that bucket — B's missing column reads as
  zero (the merge-rule identity).
- Bucket-aligning timestamps to `keyvizStep` remains a future
  extension. It will require passing the effective step duration into
  the fan-out merge path so routine sub-step jitter can be coalesced
  without hiding larger clock drift.

## 5. Wire format

Response shape (additions to the existing `KeyVizMatrix` only,
no breaking changes — old SPA versions keep working):

```jsonc
{
  "column_unix_ms": [...],
  "rows": [
    {
      "bucket_id": "route:42",
      ...,
      "values": [...],
      "conflict": true,      // OR of conflicts[]
      "conflicts": [false, true, false],
      "raft_group_ids": [7, 7, 7],
      "leader_terms": [42, 43, 43]
    }
  ],
  "series": "writes",
  "generated_at": "...",

  // NEW fan-out metadata; absent when fan-out is disabled.
  "fanout": {
    "nodes": [
      {"node": "10.0.0.1:8080", "ok": true},
      {"node": "10.0.0.2:8080", "ok": true},
      {"node": "10.0.0.3:8080", "ok": false, "error": "context deadline exceeded"}
    ],
    "responded": 2,
    "expected": 3
  }
}
```

`FanoutNodeStatus` currently serializes only `node`, `ok`, and
`error`; `error` is omitted for successful nodes. Structured per-node
warnings such as catalog-divergence are not shipped in Phase 2-C. A
future warning array would be an additive wire extension.

`conflicts[]` is parallel to `values[]` and marks only the columns
where the fan-out merge saw disagreement inside the same
`(bucketID, raftGroupID, leaderTerm, column)` identity. `conflict`
is the row-level OR of `conflicts[]` and remains on the wire for
older SPAs that only know how to hatch a whole row.

`KeyVizRow.Conflict` is tagged `json:"conflict,omitempty"`, so
absence means `false`. Only the fan-out aggregator sets
`conflict = true` or allocates `conflicts[]`; in local-only mode and
cleanly merged rows, both `conflict` and `conflicts` are omitted.
`raft_group_ids[]` and `leader_terms[]` are omitted only for legacy
peers that do not emit per-column identity. Current JSON responses
still allocate arrays parallel to `values[]`; unknown identity is
represented by zero entries, matching `proto/admin.proto`'s
"term not tracked" sentinel.

## 6. SPA changes

- New `Fanout` block in the API client (TypeScript shape mirroring
  §5).
- A degraded-mode banner above the heatmap when `responded <
  expected`, listing which nodes failed and why.
- `KeyVizRow.conflicts[]` → per-cell hatching. `KeyVizRow.conflict`
  remains the row-level fallback for older clients and for any
  consumer that wants a cheap row summary.
- Header counter: "Cluster view (3 of 3 nodes)".

This is small enough to land in the same PR as the server side or
as an immediate follow-up; either way the wire format above is
forwards-compatible so an old SPA against a fan-out server still
renders correctly (it just ignores the new fields).

## 7. Implementation status

Implemented server slice:

- `--keyvizFanoutNodes` flag plumbing through `main.go` →
  `internal/admin` → handler.
- `KeyVizRow` JSON fields `conflicts`, `raft_group_ids`, and
  `leader_terms`, each parallel to `values[]`.
- Fan-out aggregator with the shipped §4 merge rules, degraded peer
  status, peer timeouts, bounded peer response bodies, cookie
  forwarding, and recursion guard. Timestamp bucket alignment and
  structured catalog-divergence warnings remain future extensions.
- Table-driven tests for reads, canonical write merge, unknown-term
  fallback, per-cell conflict, partial failure, peer ordering, URL
  forwarding, and response-size guard paths.

Implemented SPA slice:

- Degraded banner, conflict hatching, and cluster-view header
  counter.

Deferred follow-ups:

- Dynamic membership discovery via `GetClusterOverview`.
- Inter-node token auth replacing browser-cookie forwarding.

## 8. Five-lens review checklist

1. **Data loss** — Fan-out is read-only against the existing
   sampler. With per-cell `raftGroupID` / `leaderTerm` identity, the
   implemented write merge preserves distinct leadership terms rather
   than collapsing them to max. Mixed-version or unknown-identity
   cells fall back to max-merge, which may under-count but avoids
   inventing traffic; `conflicts[]` tells operators which cells are
   soft.
2. **Concurrency / distributed** — Per-node calls are issued in
   parallel with the configured per-call timeout (default 2 s,
   override via `--keyvizFanoutTimeout` per §3). The aggregator
   itself is single-goroutine (waits for all peer responses then
   merges). No shared mutable state between concurrent fan-out
   requests.
3. **Performance** — Aggregator cost is O(N × M × C) where N is
   nodes, M is rows per node, C is columns. At the 1024-row
   budget × 3 nodes × 60 columns this is ~180k cells per
   request — well below the SPA's existing render budget. No
   coordinated round trip per `Observe`; the existing hot path is
   not touched. **Per-peer response body is capped at 64 MiB**
   via `io.LimitReader` — a misbehaving or compromised peer that
   streams gigabytes back at us would otherwise pin a goroutine
   on the JSON decoder and balloon memory. Sizing: at the design
   ceiling of 1024 rows × 4096 columns × 8 B/uint64 the raw binary
   payload is ~32 MiB. For sparse heatmaps where most counters are
   ≤ 4 decimal digits (the common case), JSON's decimal text form
   is **smaller** than the 8-byte raw binary, so the 64 MiB cap
   gives comfortable headroom over the raw-binary ceiling. The cap
   does become a real ceiling for traffic-saturated heatmaps where
   counters routinely encode to ~20 digits, but those sit well
   above the realistic operational range. Beyond that, note that
   `MaxHistoryColumns` in the
   sampler is **100 000** at the upper bound; a peer that returns
   the full ring (~1024 rows × 100 000 cols ≈ 800 MiB raw) would
   trip the cap. The cap firing surfaces as a JSON-decode error
   from the peer (the LimitReader returns EOF mid-stream), which
   the aggregator records as `ok=false` with the decode error in
   that node's status entry plus a `WARN`-level server log. No
   partial data is accepted. Operators who actually want >64 MiB
   peer responses should override via a future flag; for now the
   conservative default is the correct trade. (Review finding on PR #685.)
4. **Data consistency** — Reads are exact in steady state and during
   transitions. Writes use the implemented §9.1 group/term merge
   when all contributors provide identity, so leadership transitions
   preserve both terms rather than collapsing to max. Cells with
   legacy or unknown identity fall back to conservative max-merge
   to avoid over-counting overlapping known and unknown samples.
5. **Test coverage** — `internal/admin/keyviz_fanout_test.go`
   covers canonical group/term merge, unknown-term fallback,
   per-cell conflict, row-level conflict OR, reads, partial failure,
   URL builder variants, per-node order, and the over-cap path.
   Existing handler tests remain unchanged when fan-out is disabled
   (the default). The synthetic-burst test at
   `keyviz/sampler_test.go` is unaffected.

## 9. Open questions

1. Should `--keyvizFanoutNodes` accept hostnames that resolve via
   DNS (so a Kubernetes headless service like
   `elastickv-admin:8080` works), or require pre-resolved
   addresses? **Resolved as: DNS allowed; resolution rides the Go
   stdlib resolver's process-wide cache plus the kernel's nscd
   cache when configured — no resolver cache local to the
   aggregator.** A transient DNS failure surfaces as a per-peer
   `ok=false`, not a hung request, because the `http.Client` honors
   the per-call deadline. Operators on networks with flaky DNS
   should colocate a caching resolver (the standard fix for any
   short-lived HTTP client, not specific to this feature).
   (Round-1 review finding on PR #685.)
2. **Resolved**: per-node call budget is a fixed 2 s default with
   `--keyvizFanoutTimeout` operator override. Tying the timeout to
   `keyvizStep` would let a 60-second-step config hold the request
   for ~2 minutes against a slow peer; the fixed default decouples
   the fan-out wall time from the sampling cadence. See §3.
3. Should the fan-out response include each node's `generated_at`
   so the SPA can detect skew? Probably yes — adds one timestamp
   per node entry, no real cost. Defer the SPA UI to PR 2.
