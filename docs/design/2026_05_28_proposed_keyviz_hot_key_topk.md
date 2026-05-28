---
status: proposed
phase: 2-A++
parent_design: docs/admin_ui_key_visualizer_design.md
related_design: docs/design/2026_05_25_proposed_keyviz_subrange_sampling.md
author: bootjp
date: 2026-05-28
---

# KeyViz Per-Cell Hot-Key Drill-down (Top-K, Phase 2-A++)

## 1. Background

Phase 2-A+ sub-range sampling (#836 + #841) shows hot **sub-ranges**:
the heatmap tells you *which region of the key space* is busy. The
sub-range design (`2026_05_25_proposed_keyviz_subrange_sampling.md` §2.2
/ §8) explicitly deferred naming the actual hottest **keys** —
"`Top-K can layer on later as a drill-down`." This is that follow-up.

Live use surfaced the need: with a single broad route (`route:1 =
[nil,nil)`), even K=64 sub-ranges are leading-byte-coarse (§3.2). When
an operator sees a hot cell, the natural next question is "**which key
inside that cell?**" — and the sub-range view alone cannot answer it.
This proposal adds an on-click drill-down that returns the top-K
**actual** hot keys for the clicked cell.

## 2. Goals / non-goals

### 2.1 Goals

- **Drill-down API**: given `(bucket_id, series, time_window)`, return
  the top-K actual hot keys (key bytes + count) for that cell.
- **Bounded memory**: per-route heavy-hitters sketch (Space-Saving),
  not unbounded per-key counters. `O(m)` per individual route.
- **Hot path stays near-lock-free**: `Observe` does at most a fast
  PRNG-gated sampled enqueue into a single bounded channel — never a
  map mutation, never a per-key lock — so the Phase 2-A+ allocation-free
  K=64 hot-path benchmark does not regress measurably when hot-keys are
  *disabled*, and degrades gracefully when *enabled*.
- **Fan-out aggregation**: a drill-down query fans out to every node,
  sums counts per key, returns the cluster-wide top-K.
- **Opt-in**: disabled by default behind `--keyvizHotKeysEnabled`. The
  feature retains real key bytes in memory and exposes them on the
  admin API; both the flag and the existing admin auth must be on for
  any key bytes to leave a node.

### 2.2 Non-goals

- **Exact top-K.** Space-Saving is approximate; the error bound (`N/m`,
  N = observed events, m = sketch capacity) is documented and returned
  in the response so callers know the noise floor. An exact-counter
  alternative is unbounded by key cardinality and is rejected.
- **Per-flush-window history.** v1 maintains a rolling/current
  snapshot per route (last refresh ≤ `keyvizStep`). Historical
  per-window top-K is a future extension.
- **Per-sub-bucket independent sketches.** `K_buckets × m × routes`
  blows memory up to `~64 × 64 × 10_000 = ~40M entries` worst case.
  Instead, **per-route** Space-Saving tracks the hottest keys for the
  whole route; the drill-down server filters the tracked set down to
  the clicked sub-range. The hot keys of a hot cell almost always
  appear in the route's top-`m` (they are what make the cell hot), so
  the approximation is sound.
- **Aggregate / virtual buckets.** Their `RouteID`s are synthetic and
  span many real routes; "top-K within an aggregate" is not meaningful.
  Aggregates are not tracked.
- **No proto change.** The drill-down is JSON-only on the admin HTTP
  path (mirrors how cluster fan-out for the matrix already works).

## 3. Algorithm: per-route Space-Saving

Each **individual** route owns a Space-Saving sketch with capacity
`m` (proposed default `m = 64`). On a new observation of key `k`:

1. If `k` is currently tracked, increment its counter.
2. Else if the sketch has spare capacity, insert `k` with count 1.
3. Else evict the entry with the **smallest** counter, replace its key
   with `k`, and set the new counter to `min_counter + 1`.

Guarantees (standard Space-Saving): a key with true frequency `f` over
`N` total observations is reported with error `≤ N/m`; any key with
`f > N/m` is **guaranteed** in the tracked set. Misra–Gries-equivalent
worst-case correctness, with the practical advantage that the reported
counter is an upper bound on the true frequency.

## 4. Hot-path strategy: bounded sampled queue + background aggregator

The crux. Naively touching a per-route map on every `Observe` would
serialise the hot path on a mutex (or worse, on the very keys we
*want* to identify, since they fire the lock most often). The proposed
flow is:

```text
   Observe (hot path)                  background goroutine
   ─────────────────                   ────────────────────
   if !hotKeysEnabled return                drain ch
   if rng()%sampleRate != 0 return          for (rid, kc) := range ch:
   kc := pool.Clone(key)                       per-route SS update (no lock —
   select {                                     single-writer)
     case ch <- {rid, kc}:                   periodic atomic.Pointer.Store of
     default: pool.Release(kc); drop          a frozen snapshot for queries
   }
```

Concretely:

- **Single global bounded channel** `chan hotKeyEvent` (default capacity
  `8192`). Per-route channels would multiply goroutines/locks; one
  channel + one consumer keeps the model simple and the hot path
  channel-pushes O(1).
- **Sampling**: 1-in-`R` Observes are enqueued (default `R = 16`).
  Hot keys are frequent enough to be sampled often; the Space-Saving
  guarantee scales with sampled-N. Use a per-shard fast PRNG (e.g.
  `runtime.fastrand64` if available, otherwise a Xoshiro/PCG seeded
  per-routine) so the sample check has no contention.
- **Non-blocking enqueue**: `select { case ch <- e: default: drop }`.
  Drop-on-full preserves the no-blocking-on-hot-path contract; under
  overload v1 prefers approximate top-K over latency.
- **Key clone**: the hot path must copy the key bytes because the
  caller is free to reuse / mutate the buffer after `Observe` returns
  (the existing `Observe` contract). To bound allocation, the clone
  comes from a `sync.Pool` of pre-sized byte slices; long keys
  (>`hotKeysMaxKeyLen`, default 1024 B) are skipped from sampling
  rather than tracked truncated (truncation would alias different
  keys into the same entry — worse than dropping).
- **Single background aggregator goroutine** drains the channel and
  updates the per-route Space-Saving sketches. Single-writer ⇒ no
  lock needed on the sketches' update path. The aggregator runs the
  whole time the sampler is enabled, just like the existing flush
  goroutine.
- **Query path**: the aggregator periodically (default every
  `keyvizStep`) copies each route's current top-`m` into an
  immutable snapshot and publishes it via `atomic.Pointer[topKSnapshot]`
  per slot. Drill-down reads load the pointer and read the snapshot
  lock-free. Snapshots are cheap (≤ m × small entry).

Hot-path cost estimate (target):

| `--keyvizHotKeysEnabled` | per-`Observe` overhead added |
|---|---|
| false (default) | one branch + early return (≤ 1 ns) |
| true, **un-sampled** (15/16 of calls) | PRNG read + modulo + branch (~5 ns) |
| true, **sampled** (1/16) | + key clone via pool + non-blocking channel send (~50–100 ns) |

Bench gate (lens 3): the K=1 disabled-case must stay within ±10% of
the current 8.7 ns / 0-alloc baseline; the enabled-sampled case is
bounded by the numbers above and reported, not gated.

## 5. Wire format & API

New endpoint, mirroring the existing keyviz matrix path:

```
GET /admin/api/v1/keyviz/hotkeys?
    bucket_id=route:1#3
   &series=writes
   &top=20
   &from_unix_ms=…&to_unix_ms=…
```

Response:

```jsonc
{
  "bucket_id": "route:1#3",
  "series":    "writes",
  "keys": [
    { "key_b64": "dXNlcjowMDAxOjA1MA==", "count": 12345 },
    { "key_b64": "...",                  "count":  9876 },
    ...
  ],
  "approximate":   true,
  "error_bound":   100,        // N/m at query time
  "snapshot_at":   "2026-05-28T01:23:45Z",
  "fanout": { ... }            // per-node status, same shape as matrix
}
```

`bucket_id` parses three forms (see sub-range §5.2):

- `route:<id>` — whole route's top-K.
- `route:<id>#<subIdx>` — server resolves the sub-bucket's
  `[Start, End)` from the slot's sub-layout and filters the route's
  tracked top-`m` to keys in that range.
- `virtual:<id>` — rejected (aggregates not tracked, §2.2).

`series` selects which counter family the sketch tracks. v1 ships
`writes` only (the dominant motivator); `reads` follows once the
sampling cost is measured. Sketches per (route, series) are
independent — `writes` lookups never inflate `reads` data, at the
cost of `series_count × m × routes` worst-case memory.

## 6. Fan-out

Drill-down queries fan out via the existing keyviz fan-out infra
(`internal/admin/keyviz_fanout.go`): one HTTP call per peer admin
listener, results merged. Merge rule: sum counts per `key_b64`, take
the global top-`top` by summed count. This converges to the
cluster-wide top-K under Space-Saving's approximation; report the
union error bound (max of per-node `N/m`).

Mixed-K (some nodes hot-keys-enabled, others not): unchanged. Peers
that don't sample omit their contribution; the merge proceeds with
whatever subset reported. `fanout.per_node_status` flags the
non-participating nodes.

## 7. Privacy & auth

This feature is the first place keyviz retains **actual user key
bytes** in memory and exposes them on the admin API.

- Off by default (`--keyvizHotKeysEnabled=false`). Setting `false`
  guarantees no key bytes are retained — the sampled path returns
  early before the clone.
- The drill-down endpoint sits behind the existing admin
  `SessionAuth` middleware. The full-access role is required;
  read-only sessions get 403.
- Each successful drill-down emits an audit log entry
  (`component=admin.keyviz.hotkeys`, principal, bucket_id, returned
  key count). The keys themselves are NOT logged (only counts) —
  bringing them into the audit log would defeat their containment
  inside the authenticated admin response.
- The sketch is in-memory only; nothing persists to Pebble or Raft.

## 8. Configuration

```
--keyvizHotKeysEnabled  bool  (default false)
--keyvizHotKeysPerRoute int   (default 64,  cap 256)
--keyvizHotKeysSampleRate int (default 16,  i.e. 1/16; cap 1024)
--keyvizHotKeysQueueSize  int (default 8192, cap 65536)
```

Memory at defaults: per route `m = 64` × `(avg key bytes + 16 B counter)`
≈ 5 KB; `MaxTrackedRoutes = 10_000` → ~50 MB worst case at the cap.
The general rule for operator help text: `m × avgKey × MaxTrackedRoutes`
bytes; raise `MaxTrackedRoutes` and lower `m` together.

## 9. Alternatives considered

- **Per-sub-bucket Space-Saving.** `K_buckets × m × routes` memory
  and `K_buckets ×` more sketches to update. Rejected (§2.2): the
  per-route sketch + range filter answers the drill-down with the
  same operator UX at a fraction of the cost.
- **Count-Min Sketch + heap.** Equivalent guarantees but two structures
  to manage. Space-Saving is simpler and the entries are already keys
  (no separate heap of `(key, count)` pairs).
- **Direct hot-path map (lock-sharded).** Sharding by route helps,
  but a single hot key in one route still hammers one shard's lock —
  exactly the workload the feature is built to identify. Rejected
  in favour of background aggregation.
- **Per-route channels.** N channels + N consumers (or a select
  fan-in) scales worse than one channel + one consumer at the route
  counts we target (`MaxTrackedRoutes = 10_000`).

## 10. Open questions (for review)

1. **Sample rate vs accuracy.** `R = 16` (sketched-N = total-N / 16)
   gives an `error_bound ≈ N / (m·R)` envelope. For an operator
   actually using the drill-down, is that good enough, or should the
   default be `R = 4`? Bench-driven decision.
2. **Snapshot cadence.** Publishing the immutable snapshot every
   `keyvizStep` (60 s default) ties drill-down freshness to the
   existing matrix cadence. A smaller interval improves freshness at
   the cost of pointer-swap rate. v1 proposes `keyvizStep`.
3. **`reads` in v1?** Writes-only halves the sketch memory and the
   queue traffic. Most BigTable-style drill-downs are write-driven,
   but cold-but-bursty read keyspaces benefit too. Ship writes-only
   in v1, read-series in v2?
4. **Mixed-K / mixed-feature fan-out.** Already handled by the
   matrix fan-out's degraded-node status; the new endpoint reuses it.
   Add a dedicated test (the same shape as the `mixed-K` matrix test
   from sub-range).

## 11. Self-review (per CLAUDE.md five lenses)

1. **Data loss** — read-only sampling. The hot path is allowed to drop
   on overload (bounded queue full → enqueue fails → counter not
   incremented for that observation). The Space-Saving guarantee
   degrades gracefully (smaller effective `N`, looser `error_bound`)
   rather than corrupting; documented in the response. No Raft / FSM /
   Pebble interaction.
2. **Concurrency / distributed** — hot path:
   one atomic-counter increment for sampling (PRNG state), one bounded
   channel `select-with-default`, **no mutex, no map mutation**. Single
   aggregator goroutine ⇒ single-writer on every sketch ⇒ updates need
   no lock. Snapshot publish via `atomic.Pointer.Store` ⇒ readers (the
   drill-down handler) load lock-free. The sketch's internal map is
   never read concurrently with mutation (snapshot is a separate
   value). `go test -race ./keyviz/...` gate.
3. **Performance** — see §4 table. New benchmark
   `BenchmarkObserveHotKeysEnabled/disabled/sampled` to pin the
   numbers. Memory bounded by §8 formula; raising `--keyvizMaxTrackedRoutes`
   without lowering `m` is the only foot-gun and the help text calls
   it out.
4. **Data consistency** — Space-Saving is approximate and the API
   returns the error bound + `approximate: true` so callers cannot
   confuse the result with exact counts. The route's *real* counts on
   the existing matrix are unchanged. Fan-out sum-then-top-K is
   commutative (sum across nodes, then sort by sum) so the merged
   result is independent of fan-out order.
5. **Test coverage** — Space-Saving unit tests (Misra–Gries oracle on
   small inputs); a `rapid` property that "true heavy hitters
   (`f > N/m`) are always reported"; aggregator-drains-channel test;
   drop-on-full preserves correctness for actual heavy hitters;
   sub-range filtering (drill-down on `route:1#3` returns only keys
   that bucket into sub-bucket 3); fan-out sum-then-top-K test;
   auth/audit test (read-only role rejected, audit entry emitted);
   `BenchmarkObserveHotKeysDisabled` ≤ 10 % over baseline. A new e2e
   test wires the sampler → aggregator → endpoint and asserts a
   hand-crafted hot key surfaces at the top.

## 12. Rollout

- Code (keyviz / internal/admin) + flag in one PR after this doc lands;
  default `--keyvizHotKeysEnabled=false` keeps every existing deploy
  unchanged.
- Deploy wiring follows the sub-range pattern: a new
  `KEYVIZ_HOT_KEYS_ENABLED` (+ tuning) env var in `deploy.env`, a
  matching `build_keyviz_flags` extension in `rolling-update.sh`,
  config-fp updated. Operators opt in per cluster.
- SPA wires a click handler on heatmap cells → fetches the new
  endpoint → renders a ranked list panel (key string + count + error
  bound + audit notice).
