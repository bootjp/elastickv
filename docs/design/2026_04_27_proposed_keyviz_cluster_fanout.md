---
status: proposed
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
status surfaced inline. This proposal scopes a **minimum-viable
Phase 2-C** that ships the operator-visible value (cluster-wide
heatmap, degraded-node banner) without requiring the full proto
extension §9.1 calls for.

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
  - **Writes**: max across nodes, with a `conflict=true` flag on
    cells where the per-node values disagree (best-effort dedup;
    correct under stable leadership, conservative under leadership
    flip).
- Degraded-mode response: when N nodes respond and M < N succeed,
  the response carries `{node, ok, error}` per node and the SPA
  shows a banner.
- Behaviour preserved when `--keyvizFanoutNodes` is unset: the
  endpoint serves the local view exactly as it does today.

### 2.2 Explicitly NOT in scope

- **Membership discovery via `GetClusterOverview`** (parent §9.1).
  Static `--keyvizFanoutNodes` is the contract for Phase 2-C; the
  cache + refresh-interval machinery lands when the cluster grows
  beyond a hand-configured size.
- **Wire format extension with `raftGroupID` + `leaderTerm`**
  (parent §9.1). The MVP merge uses `max` over writes; the
  identity-based dedup §9.1 specifies will land in Phase 2-C+ when
  we extend the proto + JSON schemas.
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

- Self is included implicitly: `internal/admin` resolves
  `--adminAddress` and skips the network round-trip for the local
  entry. This keeps the configuration symmetric across all nodes
  (every node lists every node, including itself) so an operator
  can stamp the same flag onto every host.
- Empty (or unset) flag → fan-out disabled, current behaviour.
- Each entry is a host:port. TLS is out of scope for Phase 2-C
  (intra-cluster admin traffic is assumed to ride a private
  network); the HTTP client uses `http://`. A follow-up will
  introduce `--keyvizFanoutTLS` once the rest of the admin path
  has TLS too.
- The fan-out caller authenticates with the same admin session
  cookie the upstream request carried. The `internal/admin`
  middleware mints a short-lived inter-node token signed with the
  shared admin secret (already used for browser sessions) so a
  compromised browser cookie cannot be replayed beyond its TTL on
  a peer node. Per-call timeout: `2 × keyvizStep` (default 2 s)
  with `5 s` ceiling so a slow peer cannot hold the SPA poll open.

## 4. Merge rules

The parent design §9.1 specifies the **canonical** merge rule:
collapse write samples to one value per
`(bucketID, raftGroupID, leaderTerm, windowStart)`, surface
`conflict=true` when distinct sources disagree, sum across
`leaderTerm` values for the same `(group, window)`. That rule
requires `raftGroupID` + `leaderTerm` on every row — proto + JSON
extensions we have not built yet.

This MVP uses a **simpler, conservative substitute** that is
correct in steady state and never silently overcounts under
transitions:

### 4.1 Reads → sum across nodes

Each node records the reads it served. A read served by node A is
not also served by node B. Summation is exact.

### 4.2 Writes → max across nodes + conflict flag

Under stable leadership, exactly one node (the current leader for
the route's group) records writes for any given window; other
nodes report 0. `max` returns the leader's count; non-leader
zeros do not perturb it.

Under a leadership flip mid-window, both the ex-leader and the
new leader may report non-zero counts for the same
`(bucketID, windowStart)`:

- If the ex-leader's local cache has not yet expired its
  pre-transfer increment, both nodes report values.
- `max` returns the larger count and surfaces `conflict=true` so
  the SPA can hatch the cell.
- This is **conservative** — it understates the true window
  total (which is `ex_leader + new_leader`) by at most one
  leader's pre-transfer slice. The design accepts this trade
  in exchange for not overcounting; overcounting is the failure
  mode operators react to badly because it produces phantom
  hotspots.

The full §9.1 rule (sum across `leaderTerm`, dedupe within a
term) recovers the missing slice but requires `leaderTerm` on
the wire. We commit to landing it in Phase 2-C+ once the proto
extension lands.

### 4.3 Bytes counters

`read_bytes` and `write_bytes` follow `reads` and `writes`
respectively (sum and max). Same justification.

### 4.4 Row identity

Two rows from different nodes belong to the same logical row when
their `BucketID` matches. `Start` / `End` / `Aggregate` /
`RouteCount` / `RouteIDs` are taken from the **first non-empty
node response** (deterministic by node order in
`--keyvizFanoutNodes`). If two nodes disagree on `Start`/`End`
for the same `BucketID`, that indicates a routing-catalog
divergence the operator should investigate; we surface it as a
warning in the per-node status payload but do not block the
response.

### 4.5 Time alignment

All nodes use `keyvizStep` (default 1 s) so `column_unix_ms`
arrays line up modulo clock skew. The aggregator pivots on
`column_unix_ms[i]` exactly: a column present in node A but
absent in node B contributes only A's values for that timestamp,
with B's missing column treated as zero. Operators with NTP
drift > Step should fix NTP — the heatmap is not designed to
hide clock skew.

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
      "conflict": false      // NEW: true when MVP max-merge resolved disagreement
    }
  ],
  "series": "writes",
  "generated_at": "...",

  // NEW fan-out metadata; absent when fan-out is disabled.
  "fanout": {
    "nodes": [
      {"node": "10.0.0.1:8080", "ok": true,  "error": ""},
      {"node": "10.0.0.2:8080", "ok": true,  "error": ""},
      {"node": "10.0.0.3:8080", "ok": false, "error": "context deadline exceeded"}
    ],
    "responded": 2,
    "expected": 3
  }
}
```

`conflict` is per row — a coarser signal than parent §9.1's
per-cell flag. The cell-level flag will land with the
`leaderTerm`-based merge.

## 6. SPA changes

- New `Fanout` block in the API client (TypeScript shape mirroring
  §5).
- A degraded-mode banner above the heatmap when `responded <
  expected`, listing which nodes failed and why.
- `KeyVizRow.conflict` → cell hatching (an SVG overlay layered
  over the canvas, only over rows whose `conflict === true`). The
  hatch is per-row, not per-cell, until the wire format upgrade
  lets us be precise.
- Header counter: "Cluster view (3 of 3 nodes)".

This is small enough to land in the same PR as the server side or
as an immediate follow-up; either way the wire format above is
forwards-compatible so an old SPA against a fan-out server still
renders correctly (it just ignores the new fields).

## 7. Implementation plan

PR 1 (this proposal + a small slice):

- Land this design doc.
- Wire `--keyvizFanoutNodes` flag plumbing through `main.go` →
  `internal/admin` → handler.
- Add a fan-out aggregator with the §4 merge rules and a
  table-driven test covering: stable-leader (max wins, no
  conflict), mid-window flip (max wins, conflict=true), partial
  failure (one node times out, response carries the failure).
- Server-side only — SPA changes follow.

PR 2:

- SPA: degraded banner, conflict hatching, header counter.

PR 3 (Phase 2-C+):

- Extend proto + JSON with `raftGroupID` and `leaderTerm`.
- Replace §4.2 max-merge with the canonical
  `(bucketID, raftGroupID, leaderTerm, windowStart)` merge.
- Promote `conflict` from per-row to per-cell.

## 8. Five-lens review checklist

1. **Data loss** — Fan-out is read-only against the existing
   sampler. The conservative max-merge can under-count during a
   leadership flip; this is preferable to over-counting (which
   would invent traffic) and the conflict flag tells operators
   when the data is soft.
2. **Concurrency / distributed** — Per-node calls are issued in
   parallel with a 5-second ceiling per node. The aggregator
   itself is single-goroutine (waits for all peer responses then
   merges). No shared mutable state between concurrent fan-out
   requests.
3. **Performance** — Aggregator cost is O(N × M × C) where N is
   nodes, M is rows per node, C is columns. At the 1024-row
   budget × 3 nodes × 60 columns this is ~180k cells per
   request — well below the SPA's existing render budget. No
   coordinated round trip per `Observe`; the existing hot path is
   not touched.
4. **Data consistency** — Merge rules are conservative under
   leadership transitions (under-count + conflict flag, never
   over-count). Reads are exact in steady state and during
   transitions. The §9.1 canonical rule is preserved as a Phase
   2-C+ contract once we extend the wire format.
5. **Test coverage** — `internal/admin/keyviz_fanout_test.go`
   table-driven across the four scenarios in §7 PR-1. Existing
   handler tests unchanged when fan-out is disabled (the default).
   The synthetic-burst test at `keyviz/sampler_test.go` is
   unaffected.

## 9. Open questions

1. Should `--keyvizFanoutNodes` accept hostnames that resolve via
   DNS (so a Kubernetes headless service like
   `elastickv-admin:8080` works), or require pre-resolved
   addresses? Proposing **DNS lookups happen on every fan-out
   request** (no caching) — small clusters that this MVP targets
   do not need a resolver cache.
2. Should the per-node call budget be `2 × keyvizStep` or a fixed
   2 s? Tying it to `keyvizStep` means a 60-second-step config
   would let a slow peer hold the request for ~2 minutes. Fixed
   2 s with a `--keyvizFanoutTimeout` override for operators on
   weird networks is probably the right shape — proposing this.
3. Should the fan-out response include each node's `generated_at`
   so the SPA can detect skew? Probably yes — adds one timestamp
   per node entry, no real cost. Defer the SPA UI to PR 2.
