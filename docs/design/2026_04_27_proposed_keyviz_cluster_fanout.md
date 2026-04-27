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
status surfaced inline.

**Rollout notes** (Gemini round-1 PR #685 asked for explicit
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

This proposal scopes a **minimum-viable Phase 2-C** that ships the
operator-visible value (cluster-wide
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
  - **Writes**: max across nodes, with a row-level `conflict=true`
    flag raised when *any* per-node values disagreed for *any* cell
    in the row (best-effort dedup; correct under stable leadership,
    conservative under leadership flip). The flag is row-level for
    Phase 2-C and moves to per-cell when the proto extension lands
    in Phase 2-C+ — see §4.2 and §5 for the wire-format implication.
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
- **Auth (Phase 2-C MVP)**: the fan-out path is anonymous — the
  aggregator issues unauthenticated GETs to peers. This is only
  acceptable on a fully-private intra-cluster network, which is
  the contract `--keyvizFanoutNodes` documents. **Do NOT enable
  fan-out across an untrusted network until Phase 2-C+ ships
  proper auth.** Per-call timeout: 2 s default, override via
  `--keyvizFanoutTimeout`.
- **Auth (Phase 2-C+)**: a follow-up extends the fan-out path with
  a short-lived signed token derived from the existing admin
  session-signing key (`ELASTICKV_ADMIN_SESSION_SIGNING_KEY`).
  Pre-shared inter-node token, NOT a replay of the browser's
  session cookie — re-using the cookie would couple browser session
  TTL to inter-node call validity, and a compromised browser
  session would gain peer-call authority. The two paths are
  intentionally distinct. The earlier draft of this section
  conflated them; this paragraph supersedes it.

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
divergence the operator should investigate; we surface it as a
warning in the per-node status payload but do not block the
response.

### 4.5 Time alignment

All nodes use `keyvizStep` (default 1 s). The aggregator **bucket-
aligns** column timestamps to the nearest `keyvizStep` boundary
before pivoting, so two nodes whose flush goroutines fire fractions
of a second apart still land on the same merged column. Without
the alignment step a 50 ms NTP skew would split each window into
two adjacent merged columns, halving the displayed traffic in each.

Specifics:

- Both sides round `column_unix_ms[i]` down to the nearest
  multiple of `keyvizStep_ms` before merging.
- A column present in node A but absent in node B contributes
  only A's values for that bucket — B's missing column reads as
  zero (the merge-rule identity).
- Drift larger than `keyvizStep / 2` between nodes is still an
  operator problem (the heatmap should not paper over NTP issues
  that big), but routine sub-step jitter no longer causes column
  fragmentation. (Gemini round-1 PR #685.)

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
      {"node": "10.0.0.1:8080", "ok": true,  "error": "", "warnings": []},
      {"node": "10.0.0.2:8080", "ok": true,  "error": "",
        "warnings": [{"code": "catalog_divergence", "bucket_id": "route:42",
                      "detail": "start/end disagree with prior node"}]},
      {"node": "10.0.0.3:8080", "ok": false, "error": "context deadline exceeded", "warnings": []}
    ],
    "responded": 2,
    "expected": 3
  }
}
```

`warnings` is a per-node array of structured non-fatal signals.
Today the only emitter is `catalog_divergence` (§4.4 — `Start`/`End`
disagree across nodes for the same `BucketID`). Adding a new
warning code is a wire-format extension, not a breaking change:
old SPAs render the entry as a generic warning until they teach
themselves the new code. (Codex round-1 PR #685.)

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
   payload is ~32 MiB; JSON encodes uint64 traffic counters at a
   similar size for typical small values, so 64 MiB gives ≥2×
   headroom. Beyond that, note that `MaxHistoryColumns` in the
   sampler is **100 000** at the upper bound; a peer that returns
   the full ring (~1024 rows × 100 000 cols ≈ 800 MiB raw) would
   trip the cap. The cap firing surfaces as a JSON-decode error
   from the peer (the LimitReader returns EOF mid-stream), which
   the aggregator records as `ok=false` with the decode error in
   that node's status entry plus a `WARN`-level server log. No
   partial data is accepted. Operators who actually want >64 MiB
   peer responses should override via a future flag; for now the
   conservative default is the correct trade. (Gemini PR #685.)
4. **Data consistency** — Merge rules are conservative under
   leadership transitions (under-count + conflict flag, never
   over-count). Reads are exact in steady state and during
   transitions. The §9.1 canonical rule is preserved as a Phase
   2-C+ contract once we extend the wire format.
5. **Test coverage** — `internal/admin/keyviz_fanout_test.go`
   table-driven across the §7 PR-1 scenarios (stable-leader,
   leadership-flip, partial failure) plus url-builder variants,
   per-node order, and the over-cap path. Existing handler tests
   unchanged when fan-out is disabled (the default). The
   synthetic-burst test at `keyviz/sampler_test.go` is unaffected.

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
   (Gemini round-1 PR #685.)
2. **Resolved**: per-node call budget is a fixed 2 s default with
   `--keyvizFanoutTimeout` operator override. Tying the timeout to
   `keyvizStep` would let a 60-second-step config hold the request
   for ~2 minutes against a slow peer; the fixed default decouples
   the fan-out wall time from the sampling cadence. See §3.
3. Should the fan-out response include each node's `generated_at`
   so the SPA can detect skew? Probably yes — adds one timestamp
   per node entry, no real cost. Defer the SPA UI to PR 2.
