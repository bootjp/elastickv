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

Guarantees (standard Space-Saving): a key with frequency `f` over the
sketch's observed stream of length `N_obs` is reported with error
`≤ N_obs / m`; any key with `f > N_obs / m` is **guaranteed** in the
tracked set. Misra–Gries-equivalent worst-case correctness, with the
practical advantage that the reported counter is an upper bound on the
sketch-observed frequency.

**With sampling (§4), the sketch's observed stream is the *sampled*
subset, not the original.** Let `R` be the sample rate (default `R = 16`,
i.e. 1-in-16) and `N_eff = N_total / R` the expected sampled-stream
length. The Space-Saving guarantees above apply to `N_eff`, not
`N_total`: a key with true frequency `f` over the original stream gets
its sampled frequency `f / R` reported with error `≤ N_eff / m`. The API
returns counts **scaled by `R`** so the operator sees an estimate of
true frequency (and the scaled error bound `R · N_eff / m = N_total / m`
is reported alongside as `error_bound`). The original-stream guarantee
is therefore **probabilistic, not deterministic** — a hot key can in
principle be missed if it is unusually unlucky in sampling, with
probability falling exponentially in the number of expected samples
(Chernoff). For the operator UX this is acceptable; the API marks the
result `approximate: true` and surfaces the scaling so callers cannot
mistake the figures for exact counts.

## 4. Hot-path strategy: bounded sampled queue + background aggregator

The crux. Naively touching a per-route map on every `Observe` would
serialise the hot path on a mutex (or worse, on the very keys we
*want* to identify, since they fire the lock most often). The proposed
flow is:

```text
   Observe (hot path)                       background goroutine
   ─────────────────                        ────────────────────
   if !hotKeysEnabled return                  drain ch
   if rand/v2.IntN(R) != 0 return             for (rid, kc) := range ch:
   if len(key) > maxKeyLen {                     per-route SS update (no
     skippedLong.Add(1); return                   lock — single-writer);
   }                                              SS COPIES key bytes,
   kc := pool.Clone(key)                          pool buffer released
   select {                                    periodic atomic.Pointer.Store
     case ch <- {rid, kc}:                       of a deep-copied snapshot
     default: dropped.Add(1)                      (also reads dropped /
       pool.Release(kc); drop                     skippedLong counters)
   }
```

Concretely:

- **Single global bounded channel** `chan hotKeyEvent` (default capacity
  `8192`). Per-route channels would multiply goroutines/locks; one
  channel + one consumer keeps the model simple and the hot path
  channel-pushes O(1).
- **Sampling**: 1-in-`R` Observes are enqueued (default `R = 16`).
  Hot keys are frequent enough to be sampled often; the Space-Saving
  guarantee scales with sampled-N. Implementation: the sample gate is
  **`math/rand/v2.IntN(R) == 0`** — the package-level function, which
  on Go 1.22+ is lock-free per-P and concurrency-safe (see
  `pkg.go.dev/math/rand/v2#IntN`'s thread-safety note). No `Source` or
  `PCG` instance is created or shared by the sampler itself: any
  attempt to keep one `math/rand/v2.PCG` per shard and call it from
  request goroutines is a data race (per-`PCG` state is not
  synchronized — Codex P1), so we lean on the standard library's
  built-in per-P generator instead. This gives us **both**
  properties — **probabilistic** (no phase-lock against periodic /
  ordered workloads, addressing the original L135 finding) **and
  race-free** (no shared mutable state we own, addressing L137) —
  with no added allocation on the hot path.
- **Non-blocking enqueue**: `select { case ch <- e: default: drop }`.
  Drop-on-full preserves the no-blocking-on-hot-path contract; under
  overload v1 prefers approximate top-K over latency. **Drops are
  counted, not silent**: an `atomic.Uint64` `dropped_samples` counter
  increments on every `default` branch (Codex P2). The aggregator
  copies the counter into each published snapshot, the drill-down
  response exposes it (§5), and a non-zero value flags the snapshot
  as **degraded** so a client cannot misread an overloaded sample as
  exact. Without this, `sampled_n × sample_rate` would silently
  under-estimate true activity by the drop rate.
- **Key clone**: the hot path must copy the key bytes because the
  caller is free to reuse / mutate the buffer after `Observe` returns
  (the existing `Observe` contract). To bound allocation the clone
  comes from a `sync.Pool` of pre-sized byte slices; long keys
  (>`hotKeysMaxKeyLen`, default 1024 B) are skipped from sampling
  rather than tracked truncated (truncation would alias different
  keys into the same entry — worse than dropping). **Long-key skips
  are counted**, not silent (Codex P2): an `atomic.Uint64`
  `skipped_long_keys` counter increments on the skip, the aggregator
  copies it into the snapshot, the drill-down response exposes it
  (§5), and a non-zero value contributes to `degraded` — so a
  workload whose actual hot key is longer than the cap cannot
  produce an empty drill-down result that misleads the operator into
  thinking no key was missed. **The pool buffer is strictly transient
  between `Observe` and the aggregator's update** (see below); it is
  never the storage that an SS entry or a published snapshot points at.
- **Single background aggregator goroutine** drains the channel and
  updates the per-route Space-Saving sketches. Single-writer ⇒ no
  lock needed on the sketches' update path. **The aggregator copies
  the key bytes out of the pool buffer into the SS entry's own
  allocation on insert/replace**, then releases the pool buffer
  immediately. So an SS entry never aliases a pool slice, and a
  subsequent eviction returns only the entry's own bytes to the GC
  (not to the pool) — closing the Gemini-flagged use-after-free
  window where a snapshot reader and a hot-path re-cycle could race
  on the same pool slice.
- **Query path**: the aggregator periodically (default every
  `keyvizStep`) builds a **fully deep-copied** snapshot (key bytes
  re-allocated again, independent of even the SS entry's storage,
  so eviction in the live sketch cannot mutate a published snapshot)
  and publishes it via `atomic.Pointer[topKSnapshot]` per slot.
  **Each publish also RESETS the route's live sketch** (Codex P2 L186):
  entries cleared, `sampled_n` / `dropped_samples` / `skipped_long_keys`
  reset. Without the reset the sketch would accumulate since the
  feature was enabled, so a key hot in an old window could still
  dominate a drill-down on the current cell. The reset gives each
  published snapshot exactly one `keyvizStep` window of observations —
  matching the matrix cell's per-Step semantics — while the next
  window fills the freshly-cleared sketch. The aggregator is the only
  writer to the live sketch and to its counters, so the reset and the
  snapshot deep-copy happen atomically from the rest of the system's
  perspective with no lock needed.
  Drill-down reads load the pointer and read the snapshot lock-free;
  snapshots are cheap (≤ m × small entry) and a new publish drops the
  previous snapshot to the GC without disturbing in-flight readers.

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

```http
GET /admin/api/v1/keyviz/hotkeys?
    route_id=1
   &sub_bucket=3
   &series=writes
   &top=20
   &from_unix_ms=…&to_unix_ms=…
```

`route_id` is required (a single individual route — aggregates are not
tracked, §2.2). `sub_bucket` is optional: omitted, the response is the
whole route's top-K; supplied, the server resolves the sub-bucket's
`[Start, End)` from the slot's immutable sub-layout (§3.2 of the
sub-range design) and filters the route's tracked top-`m` to keys in
that range. The filter MUST use the same **lexicographic byte
comparison** (`bytes.Compare`) as `subBucketIndex` does on the forward
path — any other comparator (locale-aware string compare, etc.) silently
mis-shelves binary keys. Two separate query params — not a single
`bucket_id` — because the matrix response's `bucket_id` strings embed
`#` (e.g. `route:1#3`), and `#` in a URL query is parsed as a
**fragment** by the client (the server would only see `route:1`);
separate params sidestep the encoding hazard entirely (CodeRabbit
Major). The SPA parses the matrix row's `bucket_id` into the two params
before issuing the request.

`from_unix_ms` / `to_unix_ms` are accepted for forward compatibility,
but v1 maintains only the **current snapshot** (see §2.2 — historical
per-window top-K is a future extension), so the server **rejects with
HTTP 400 (`error_code: "out_of_snapshot_window"`)** when the requested
window does not overlap `[snapshot_at − keyvizStep, snapshot_at]`.
Silently returning the current snapshot for a clicked **historical**
heatmap column would confidently name keys hot *now* as if they made
the *historical* cell hot — exactly the misread Codex P2 (L224)
flagged. The SPA disables the drill-down handler on heatmap columns
outside the current window so the operator never receives a 400 in
the normal flow. Omitting both `from`/`to` is treated as "current"
(no rejection). `snapshot_at` in the response always reports when the
returned snapshot was built.

**Drill-down approximation caveat (Codex P2):** because the sketch is
per-route, not per-sub-bucket, the filter can return an empty or
incomplete result for a sub-bucket whose traffic is spread across many
moderate keys, or when more than `m` hotter keys live elsewhere in the
same route. The returned set is then "the route's heavy hitters that
happen to fall in the clicked sub-range" — not necessarily "the keys
that made the cell hot." This is the price of per-route memory; the
response includes the `sample_rate` / `error_bound` so a client can
detect a thin result, and §10 records per-sub-bucket sketches as a
future enhancement when sub-cell precision proves to be needed.

Response:

```jsonc
{
  "route_id":   1,
  "sub_bucket": 3,            // omitted in the whole-route case
  "series":     "writes",
  "keys": [
    { "key_b64": "dXNlcjowMDAxOjA1MA==", "count": 12345 },
    { "key_b64": "...",                  "count":  9876 }
  ],
  "approximate":  true,
  // Counts above are scaled-to-true-frequency estimates (= sampled
  // sketch counter × sample_rate). The error bound is in the same
  // (scaled, true-frequency) units.
  "sample_rate":       16,
  "sampled_n":         500,   // observations the sketch saw
  "dropped_samples":   0,     // sampled but dropped by full-queue back-pressure
  "skipped_long_keys": 0,     // observations whose key exceeded --keyvizHotKeysMaxKeyLen
  "degraded":          false, // true ⇔ dropped_samples > 0 OR skipped_long_keys > 0
  "error_bound":       125,   // per-node bound = sample_rate × sampled_n / m; merged response is the §6 sum-of-bounds
  "snapshot_at":     "2026-05-28T01:23:45Z",
  "fanout": { ... }           // per-node status, same shape as matrix
}
```

The response makes the sampling adjustment explicit so callers cannot
confuse the figures with the matrix cell's exact counts (which are
unsampled, lossless atomic counters): `sample_rate` is the on-node
`R`, `sampled_n` is the post-sample stream length the sketch observed,
each `count` is `sampled-count × R`, and `error_bound` is the
worst-case per-key over-/under-count in the same scaled units
(= `sample_rate · sampled_n / m`). All three are reported so a client
can recover any of them.

`series` selects which counter family the sketch tracks. v1 ships
`writes` only (the dominant motivator); `reads` follows once the
sampling cost is measured. Sketches per (route, series) are
independent — `writes` lookups never inflate `reads` data, at the
cost of `series_count × m × routes` worst-case memory.

## 6. Fan-out

> **No replication-factor inflation on writes.** Summing across peers
> in fan-out does **not** triple-count writes on a 3-replica cluster
> because `observeMutation` (`kv/sharded_coordinator.go`) is called
> from the **coordinator's `Dispatch` / `groupMutations`** — i.e. only
> on the node that received the client request (effectively the group
> leader at the time of writing). The FSM apply path
> (`kv/fsm.go`) does NOT call into the sampler, so followers never
> observe a write that another node already did. Per route, at most
> one peer's sketch contains writes at any given moment, and a brief
> overlap window at a leadership flip is acceptable — same regime as
> the matrix path which dedupes via `(group, term)` at merge time.

Drill-down queries fan out via the existing keyviz fan-out infra
(`internal/admin/keyviz_fanout.go`): one HTTP call per peer admin
listener, results merged. Merge rule: each peer already returns its
counts **scaled to true-frequency estimates** (per §5), so merging is
a straight sum of `count` per `key_b64` across peers, take the global
top-`top` by summed count.

The merged `error_bound` is the **sum** of the per-peer `error_bound`
values, **not the max**: each peer can independently over- or
under-count a key by up to its own bound, so the worst-case error of
the cluster-wide sum is the sum of those bounds. Reporting the max
would understate the noise floor by up to a factor of `peer_count` and
mislead any client using the bound as a confidence interval (Gemini
medium / Codex P2). The merged `sample_rate` is reported as the
**max** of per-peer rates (uniform `R` across the cluster is the
intended operational posture; if a peer is configured with a smaller
`R` it contributes more accurate data and the merged result inherits
the coarser bound from the looser peer). `sampled_n` is reported as
the sum (informational: total sketch-observed events across the
cluster). `dropped_samples` and `skipped_long_keys` are each the **sum** across
peers, and `degraded` is the **OR** across all degradation signals
(`true` if any peer reports `dropped_samples > 0` OR
`skipped_long_keys > 0`). Either source alone is enough to mark the
result degraded — including a peer that successfully drained its queue
but skipped a too-long hot key (Codex P2 L333), which would otherwise
leave the cluster reporting `degraded = false` while omitting the very
key that made the cell hot.

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

```text
--keyvizHotKeysEnabled    bool (default false)
--keyvizHotKeysPerRoute   int  (default 64,   cap 256)        # m
--keyvizHotKeysSampleRate int  (default 16,   i.e. 1/16; cap 1024)  # R
--keyvizHotKeysQueueSize  int  (default 8192, cap 65536)
--keyvizHotKeysMaxKeyLen  int  (default 1024, cap 4096)        # bytes; longer keys skip sampling
```

`--keyvizMaxTrackedRoutes` (the existing flag from the Phase 2-A
sampler) doubles as the upper bound on per-route sketch count — raising
it without lowering `m` is the §11.3 foot-gun. It is not a new flag.

Memory, two perspectives:

- **Typical** (≈ 62 B average keys, default `m = 64`,
  `MaxTrackedRoutes = 10_000`): ≈ 50 MB.
- **Worst case** (every entry holds the `1024 B` cap key length, no
  pool sharing across routes): `m × (max_key + 16 B counter) ×
  MaxTrackedRoutes` ≈ **665 MB** at default `m = 64`, or **2.66 GB** at
  the `m = 256` cap. An operator sizing for safety must use this row
  unless their workload is known to use short keys.

`--keyvizHotKeysMaxKeyLen` (§8, default `1024`) is the dominant lever
beyond `m` — halve it and the worst-case memory halves; long-key
workloads should consider lowering it explicitly. The general rule for
the help text: `m × (max_key + 16) × MaxTrackedRoutes` bytes worst case.

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

1. **Sample rate vs accuracy.** The deterministic scaled error bound is
   `N_total / m` **independent of `R`** (the sampled-stream bound
   `N_sample / m = (N_total / R) / m` is scaled back by `R` for the
   API, giving `N_total / m`). What `R` actually controls is the
   **probability** of missing a heavy hitter (fewer samples → more
   Chernoff variance). A larger `R` therefore makes the deterministic
   bound no tighter, only more probabilistic; the trade-off is hot-path
   cost vs miss probability, not bound tightness. For an operator
   actually using the drill-down, is `R = 16`'s miss probability low
   enough, or should the
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
   one `math/rand/v2.IntN(R)` call (Go 1.22+ per-P, concurrency-safe,
   no shared sampler-owned state), one bounded channel
   `select-with-default`, **no mutex, no map mutation, no PCG state
   owned by the sampler**. Single aggregator goroutine ⇒ single-writer
   on every sketch ⇒ updates need no lock. Snapshot publish via
   `atomic.Pointer.Store` ⇒ readers (the drill-down handler) load
   lock-free. The sketch's internal map is never read concurrently
   with mutation (snapshot is a separate value).
   `dropped_samples` and `skipped_long_keys` are `atomic.Uint64`
   incremented from the hot path — their values are only read by the
   aggregator at snapshot-publish time and are not load-bearing for
   ordering relative to channel events (Go's `sync/atomic` does not
   expose sub-sequential ordering levels — all `atomic.Uint64`
   operations are sequentially consistent by the language spec, so
   the freedom we want here is "no synchronisation barrier with the
   channel send is required," not "relaxed memory order"). `go test
   -race ./keyviz/...` gate.
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
