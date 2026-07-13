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

This implemented design records both the static-node-list Phase 2-C
fan-out and the Phase 2-C+ wire/merge extension that now carries
per-cell Raft group and leader-term identity.

## 2. Scope

### 2.1 In scope

- A static node list configured via `--keyvizFanoutNodes` (comma-
  separated admin HTTP endpoints).
- An aggregator inside `internal/admin` that issues parallel GETs to
  every configured node's `/admin/api/v1/keyviz/matrix`, merges the
  responses, and returns one combined `KeyVizMatrix` plus a per-node
  status array.
- Merge rules, justified in §4:
  - **Reads**: sum across nodes (each node serves distinct local
    follower reads).
  - **Writes**: when every non-zero cell has
    `(raftGroupID, leaderTerm)` identity, collapse duplicates within
    the same `(bucketID, raftGroupID, leaderTerm, column)` by taking
    max, then sum across distinct terms for the same group/window.
    A row-level `conflict=true` and per-cell `conflicts[j]=true`
    fire only when distinct sources disagree within the same
    `(bucketID, raftGroupID, leaderTerm, column)` identity. If any
    non-zero source omits group/term identity, the cell falls back
    to the Phase 2-C legacy max-merge so mixed-version peers never
    over-count.
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
- **Strict rejection of legacy peers without `raftGroupID` +
  `leaderTerm`**. Mixed-version clusters remain supported: cells
  with missing group/term identity use the legacy max-merge fallback
  described in §4.2.
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
  network (Gemini security-medium on PR #692).
- **Recursion guard**: peer requests carry an `X-Admin-Fanout-Peer`
  header. The receiving handler short-circuits its own fan-out
  when this header is set; without the guard, a symmetric
  configuration (every node lists every other node) would generate
  O(N²) HTTP calls per browser poll as each peer recursively
  fanned out. (Claude bot P1 on PR #692.)
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

The parent design §9.1 specifies the canonical write merge rule:
collapse write samples to one value per
`(bucketID, raftGroupID, leaderTerm, windowStart)`, surface
`conflict=true` when distinct sources disagree within the same
identity, and sum across `leaderTerm` values for the same
`(group, window)`. That is the implemented Phase 2-C+ contract.
Peers that do not yet emit group/term identity are still accepted via
the legacy max-merge fallback below.

### 4.1 Reads → sum across nodes

Each node records the reads it served. A read served by node A is
not also served by node B. Summation is exact.

### 4.2 Writes → group/term identity merge with legacy fallback

Under stable leadership, exactly one node (the current leader for
the route's group) records writes for any given window; other nodes
report 0. Zero-valued contributions are ignored by the write merge.

For non-zero cells that carry both `raftGroupID` and `leaderTerm`,
the aggregator:

- Groups contributions by `(bucketID, raftGroupID, leaderTerm,
  column_unix_ms)`.
- Takes `max` within one group/term identity. Equal reports are a
  duplicate observation of the same leader-term slice.
- Sets `conflicts[j]=true` and the row-level `conflict=true` when
  two sources report distinct non-zero values for the same
  group/term identity. That is the suspicious case: one Raft group
  should have at most one leader for a term.
- Sums the resulting per-term values across distinct `leaderTerm`
  values for the same group/window. A leadership flip inside the
  requested window therefore preserves the ex-leader slice plus the
  new-leader slice instead of under-counting with a simple max.

If any non-zero source for a cell has `raftGroupID=0` or
`leaderTerm=0`, the whole cell uses the legacy Phase 2-C fallback:
take the max across sources, and mark a conflict only when two
distinct non-zero values were observed. This preserves the
mixed-version safety property: an old peer can omit the new identity
fields without causing the aggregator to sum overlapping unknown
windows.

### 4.3 Bytes counters

`read_bytes` follows the read rule. `write_bytes` follows the write
rule: group/term identity merge when the cell has identity, legacy
max-merge fallback when it does not.

### 4.4 Row identity

Two rows from different nodes belong to the same logical row when
their `BucketID` matches. The merge order is deterministic and
local-first: `KeyVizFanout.Run` prepends the local matrix, then
appends successful peers in `--keyvizFanoutNodes` order. `Start` /
`End` / `Aggregate` / `RouteCount` / `RouteIDs` are copied from the
first matrix in that order that contains the bucket. Selection by
local-first configured order rather than wall-clock arrival order is
the load-bearing property: peers respond in non-deterministic order,
so picking on first arrival would let `Start`/`End` flap between
polls for the same bucket. If two nodes disagree on `Start`/`End`
for the same `BucketID`, that indicates a routing-catalog divergence
the operator should investigate; the current wire contract does not
emit a structured catalog-divergence warning, so operators must
correlate the row identity with peer status and the route catalog
when investigating that condition.

### 4.5 Time alignment

All nodes use `keyvizStep` (default 1 s), but the implemented
aggregator merges columns by the exact `column_unix_ms` value each
node returned. `mergeKeyVizMatrices` builds the sorted union of the
raw column timestamps and treats a missing source column as zero.
It does **not** round or bucket-align timestamps to the
`keyvizStep` boundary.

Specifics:

- Identical `column_unix_ms[i]` values from multiple nodes merge
  into one column.
- A column present in node A but absent in node B contributes
  only A's values for that bucket — B's missing column reads as
  zero (the merge-rule identity).
- Sub-step scheduler jitter or clock skew can therefore appear as
  adjacent columns rather than a single merged window. Operators
  should keep node clocks synchronized and interpret adjacent
  near-identical windows as a timing artifact when peer flush ticks
  are offset.

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
      "raft_group_ids": [...],
      "leader_terms": [...],
      "conflicts": [false, true, ...],
      "conflict": true       // present only when the row has a conflict
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
`error`. Future structured non-fatal signals such as catalog
divergence would require an explicit wire extension.

`raft_group_ids[]`, `leader_terms[]`, and `conflicts[]` are parallel
to `values[]`. `raft_group_ids[j]` and `leader_terms[j]` carry the
identity used by the write merge for column `j`; zero means identity
was not tracked and the legacy fallback was used for that cell.

`conflicts[]` marks the exact cells where the write merge observed a
disagreement. It is omitted when nil/no cell conflicted. `conflict`
is the row-level OR of `conflicts[]`; because
`KeyVizRow.Conflict` has `json:"conflict,omitempty"`, it is present
only when true. Clients must treat an absent `conflict` field as
false rather than relying on `"conflict" in row`.

## 6. SPA changes

- New `Fanout` block in the API client (TypeScript shape mirroring
  §5).
- A degraded-mode banner above the heatmap when `responded <
  expected`, listing which nodes failed and why.
- `KeyVizRow.conflicts[]` → per-cell hatching (an SVG overlay
  layered over the canvas). When an older server omits
  `conflicts[]`, the SPA falls back to row-level `conflict`.
- Header counter: "Cluster view (3 of 3 nodes)".

This is small enough to land in the same PR as the server side or
as an immediate follow-up; either way the wire format above is
forwards-compatible so an old SPA against a fan-out server still
renders correctly (it just ignores the new fields).

## 7. Implementation status

Phase 2-C is implemented as the static-node-list MVP described in
this document.

- `main.go` exposes `--keyvizFanoutNodes` and
  `--keyvizFanoutTimeout`, then threads the parsed fan-out config
  into `prepareAdminFromFlags`.
- `internal/admin/keyviz_fanout.go` implements the cluster fan-out
  aggregator, anti-recursion header, cookie whitelist forwarding,
  response-size cap, degraded peer status, and read/write merge
  rules.
- `internal/admin/keyviz_handler.go` suppresses recursive fan-out
  on peer requests and returns the merged `fanout` block when the
  operator configured peers.
- `web/admin/src/api/client.ts` and `web/admin/src/pages/KeyViz.tsx`
  carry the `fanout` response shape, degraded banner, per-cell
  conflict hatching, and cluster-view counter in the SPA.
- `internal/admin/keyviz_fanout_test.go`,
  `internal/admin/keyviz_handler_test.go`, and
  `docs/design/2026_05_25_implemented_keyviz_per_cell_conflict.md`
  cover the server-side fan-out, handler behavior, and Phase 2-C+
  per-cell conflict wire contract. Current SPA unit coverage is
  limited to adjacent parser/API infrastructure; it does not yet
  exercise the degraded banner, cluster counter, or hatching
  rendering paths.

Phase 2-C+ is also implemented: matrix rows carry `raft_group_ids`,
`leader_terms`, and per-cell `conflicts`; write merging uses the
canonical `(bucketID, raftGroupID, leaderTerm, column)` identity and
falls back to the legacy max-merge when a peer omits the group/term
identity.

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
