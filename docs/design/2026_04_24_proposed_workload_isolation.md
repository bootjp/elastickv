# Workload-class isolation after 2026-04-24 XREAD starvation

> **Status: Proposed**
> Follow-up to the 2026-04-24 incident review and a companion to
> `docs/design/2026_04_24_proposed_resilience_roadmap.md` (items 5–7).
> That doc is about keeping memory pressure from building; this doc is
> about keeping one expensive command path from starving every other
> path that shares the same Go runtime. Read that first for the items-6
> admission-control shape; this doc extends and reconciles with it.

---

## 1. Trigger

On 2026-04-24 we had a two-phase production incident. Morning: all four
3 GB VM nodes were OOM-killed 22–169 times; `GOMEMLIMIT=1800MiB` +
`--memory=2500m` (PR #617), WAL auto-repair (PR #613), and memwatch
graceful shutdown (PR #612) contained the death spiral described in
the resilience roadmap items 1–4. Afternoon: once OOM was contained and
the cluster re-formed, Lua p99 stayed at 6–8 s and Raft commit p99 at
6–10 s. CPU profile on leader n4 between 14:40 and 15:30 UTC:

- One client host (`192.168.0.64`) opened 37 Redis connections and ran
  a tight `XREAD` loop at ~11 XREAD/s per connection.
- `loadStreamAt` (`adapter/redis_compat_helpers.go:497`) stores each
  stream as a single protobuf blob at `redisStreamKey(key)` and
  re-unmarshals the entire stream on every `XREAD`. On a large stream
  each XREAD is O(stream_size).
- `redcon.handle → RedisServer.xread → loadStreamAt →
  unmarshalStreamValue → proto.Unmarshal` took **81% of 14 active
  cores**. `mallocgc + growslice + smallscan` took another **~25%**
  because `GOMEMLIMIT=1800MiB` pinned the heap near the ceiling and the
  GC was firing hard.
- Raft goroutines couldn't get CPU. Leader n4 recorded **75,692
  step_queue_full events** (`Engine.StepQueueFullCount`) against 0–119
  on followers. MsgApp/MsgHeartbeatResp traffic was dropped at the
  step-queue boundary. Commit latency ballooned; lease and
  LinearizableRead paths timed out downstream. Every unrelated path —
  Lua, lease, LinearizableRead, the Raft Ready loop itself — was
  starved by the same root cause.

## 2. The architectural problem

"One XREAD loop broke raft" is a symptom. The gap is that **elastickv
has no workload-class isolation**. The Go runtime scheduler time-slices
goroutines round-robin across `GOMAXPROCS`. It has no notion of "raft
goroutines must always get CPU" or "one command path must not consume
more than N% of CPU." A single heavy command path saturates all Ps;
everything else sharing the runtime — including the Raft Ready loop,
whose timeliness is load-bearing for the whole cluster — stalls.

Fixing `loadStreamAt` specifically (Layer 4 below) is necessary but
only closes today's hotspot. The next unpredicted workload — a
large-cardinality `KEYS *`, a slow user Lua script, a DynamoDB `Scan`
on a wide table — reproduces the same failure against the same
runtime. The fix has to be structural: isolate workload classes so one
class's misbehavior is bounded in CPU share and cannot starve the Raft
control plane. This doc proposes four composable layers.

---

## 3. Layer 1 — Heavy-command bounded worker pool

### Problem it solves

`redcon` spawns one goroutine per connection and `dispatchCommand`
(`adapter/redis.go:575`) runs each command synchronously on it. A
37-connection client running `XREAD` dedicates 37 goroutines to
expensive work, which the Go scheduler multiplexes onto every
available P. No structural bound on how much of the machine one
expensive command shape can consume.

### Mechanism

Fixed-size worker pool, something like `2 × GOMAXPROCS`. Static
classification picks which commands offload to the pool; cheap
commands stay on the accept-goroutine path. Pool full → reply
`-BUSY server overloaded` and return. Redis clients already treat
`-BUSY` as retryable; reusing it means no client-library changes.

Static v1 classification (name-based only — no argument inspection,
so the dispatcher can gate on the command byte without allocating):

- **Pool-gated:** `XREAD`, `XRANGE`, `XREVRANGE`, `KEYS`, `SCAN`,
  `HGETALL`, `HVALS`, `HKEYS`, `SMEMBERS`, `SUNION`, `SINTER`,
  `ZRANGE`, `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `EVAL`/`EVALSHA`,
  `FCALL`/`FCALL_RO`, the `*SCAN` family.
- **Ungated:** `GET`, `SET`, `DEL`, `EXISTS`, `INCR`, `EXPIRE`, `TTL`,
  `HGET`, `HSET`, `LPUSH`/`RPUSH`, `XADD`, single-key fast paths.

**XADD during the Mode A migration window is a latent starvation
risk.** Mode A rewrites the entire legacy blob in the first XADD that
touches a migrated stream (see Layer 4, line 414). On a 100k-entry
stream that single "ungated" XADD does O(N) unmarshal + re-marshal +
per-entry Put, which is exactly the CPU profile Layer 1 is trying to
bound. v1 mitigations, ordered by preference:

1. Ship Layer 4 Mode A and Layer 1 together. The very first migration
   XADD is expensive, but it happens once per stream; subsequent XADDs
   are O(1) and genuinely ungated.
2. If Mode A lands ahead of Layer 1, XADD is promoted to gated **only
   while the periodic scan still reports legacy keys present** (see
   Layer 4 removal criterion). Once the legacy-keys-total counter
   reaches zero cluster-wide, XADD demotes back to ungated. This is
   dynamic classification for exactly one command for exactly the
   migration window; worth the complexity because the alternative is
   a repeat of the 2026-04-24 incident triggered by a single large
   stream's first write.

Either ordering is acceptable; (1) is simpler and preferred. The
doc does not pick an ordering — the implementer of whichever PR lands
second owns the promotion/demotion logic.

The entire `ZRANGE` family is gated, not only "full-range" variants —
arg inspection (e.g., detecting `LIMIT 0 N`) breaks the "classify by
command byte" simplicity, and a bounded `ZRANGE 0 10` contributes at
most one unmarshal per request (cheap). Dynamic (observed-cost)
classification is a follow-up; v1 bias is a boring, reviewable list.

**Blocking variants are NOT I/O-bound in the current implementation.**
`XREAD BLOCK ms`, `BLPOP`, `BRPOP`, `BZPOPMIN`/`MAX` look idle from
the outside but the adapter (`adapter/redis_compat_commands.go:xread`,
`:bzpopmin` around line 3432) implements them as a **busy-poll loop**:
on a miss it calls `time.Sleep(redisBusyPollBackoff)` and re-issues
the underlying KV+leader lookup. Every wake-up does CPU work and a
Raft leadership round-trip, then sleeps again. A pool-bypass for
"blocking" variants under this implementation would hand them
unbounded CPU on the fast path, the opposite of what we want.

**v1 resolution: keep the blocking variants gated** alongside the
other heavy commands. Reject with `-BUSY` when the pool is full,
same as XREAD. The behaviour is strictly worse than a true
condition-variable wake-up (which would be slot-free), but correct
under the existing busy-poll, and consistent with the rest of the
heavy-command accounting.

**Stacked follow-up to unblock a real bypass:** replace the
busy-poll with a condvar/notification hook fed by the write path.
Only after that lands can blocking variants honestly be called
I/O-bound; at that point carve them out of the pool with the
simplest form of arg inspection (`XREAD …BLOCK…`, `B*POP`) and
re-evaluate pool sizing. Tracked as a separate item in the stream
and list/zset adapters; not required by Layer 1 v1.

### Tradeoffs

- Adds an enqueue → pickup hop for gated commands. Pool-has-capacity
  case is a channel send; pool-full case is a fast `-BUSY`, strictly
  better than "serve slowly forever."
- The static list will drift. Need `elastickv_heavy_command_pool_depth`
  plus per-command latency so review can promote formerly-cheap
  commands to gated when they grow expensive.

### Risk to flag: the Lua-recursion trap

`EVAL`/`EVALSHA` is pool-gated; a Lua script then calls
`redis.call("XREAD", ...)` internally via
`adapter/redis_lua_context.go`. If the inner call *also* acquires a
pool slot, a pool fully occupied by Lua scripts that are all about to
make an inner call **deadlocks on itself** — every slot is held by an
outer Lua waiting for an inner call that can never start. Two options:

- **(A) Outer holds the slot; inner `redis.call` runs inline and
  ignores the pool.** No deadlock, inner cost shares outer accounting.
  **Recommended v1.**
- **(B) Inner `redis.call` bypasses the pool.** Equivalent safety;
  makes inner cost invisible to pool metrics.

(A) preserves "one client request = one slot." Ship must pick one
explicitly; do not discover this at test time.

**Implementation note for (A): context propagation.** `Submit`
identifies "inside a pool slot" by attaching a sentinel value to
`context.Context` (`ctxKeyInPoolSlot`). The Lua adapter threads that
`ctx` into every `redis.call` it makes; the dispatcher's pool-gate
check returns immediately when `ctx.Value(ctxKeyInPoolSlot) != nil`
instead of attempting another `Submit`. This is the only mechanism
that reliably distinguishes "new client request" from "inner call"
without tagging every goroutine or holding a pool-wide set of
goroutine IDs. The sentinel must be package-private so external
callers cannot fake it.

**Caveat — `runLuaScript` currently clobbers the parent ctx.**
`adapter/redis_lua.go:runLuaScript` builds its per-script context as
`context.WithTimeout(context.Background(), ...)`, which throws away
the `ctxKeyInPoolSlot` sentinel that `Submit` attached when it
dispatched the outer `EVAL`/`EVALSHA`. Option (A) is therefore not
implementable as-is — the inner `redis.call` would see a plain
background context and try to acquire another pool slot, triggering
the exact deadlock we were trying to avoid.

The implementation PR MUST fix this before enabling the pool gate
on Lua. The fix is to replace `context.Background()` with the
caller-supplied `ctx` (a timeout derived from it, not from
`Background`) so the sentinel propagates. This is a one-line change
but a blocker for Layer 1 v1; Layer 1 must not ship without it or
Lua inside the pool will self-deadlock under steady load.

### Recommended v1 shape

Package-level pool in `adapter/` with a `Submit(command, fn)` entry
point, sized `2 × runtime.GOMAXPROCS(0)` (env-overridable). Gated
commands in `dispatchCommand` call `Submit`; ungated stay
synchronous. Static list lives next to `dispatchCommand`. Pool-full →
`-BUSY server overloaded`. Lua follows option (A).

**Container-aware sizing.** Go 1.25+ (which this repo uses) derives
the default `GOMAXPROCS` from the cgroup v2 CPU quota on Linux
automatically, so in most cases `runtime.GOMAXPROCS(0)` already
reflects the container's share. Two caveats remain: (a) Go runtimes
older than 1.25 do not, and (b) explicitly setting `GOMAXPROCS`
disables the runtime's periodic quota-change detection, so an
operator who hard-codes the value in the deploy environment loses
auto-updates if the quota changes at runtime. v1 leaves the runtime
default in place and documents the two caveats; a `GOMAXPROCS` env
override is still honoured for operators who want explicit control.
`go.uber.org/automaxprocs` remains an option for pre-1.25 toolchains
but is not needed for this repo.

**Single pool vs per-class sub-pools.** v1 uses a single global pool.
The risk: a burst of `KEYS *` or `SCAN` from a management client can
exhaust all slots and force `-BUSY` onto latency-sensitive `XREAD` or
Lua requests. Two mitigations exist: (i) classify gated commands into
priority tiers and reserve a minimum slot share per tier (e.g., 50%
data-path, 25% scan, 25% Lua), (ii) ship separate sub-pools per
tier. Both add complexity that's only justified if we actually
observe a scan-command burst displacing data-path work. **v1 defers
sub-pools; observability must call this out so the need is
measurable.** New metric `elastickv_heavy_command_pool_submit_total`
labelled by command name is sufficient: if pool-full rejections
concentrate on `XREAD` while `KEYS` dominates successful submissions,
the tier split is warranted.

### Where in the code

- `adapter/redis.go:575` (`dispatchCommand`), `:631` (`Run`) — gate
  point.
- `adapter/redis_compat_commands.go:3950` (`xread`) — the specific
  case that triggered the incident.
- `adapter/redis_lua.go:111` (`runLuaScript`); inner `redis.call`
  through `adapter/redis_lua_context.go` must respect (A).
- New file, e.g., `adapter/redis_workpool.go`, for the pool itself.

### v1 vs later

- **v1:** static list, single global pool, reject on full.
- **Later:** per-class sub-pools (KEYS shouldn't be able to starve
  XREAD); dynamic reclassification; optional bounded queueing.

---

## 4. Layer 2 — Raft goroutines on locked OS threads

### Problem it solves

Even with Layer 1, a badly-sized pool or a genuinely overloaded node
can drive all Ps to 100%. The Go scheduler doesn't give priority to
any goroutine; it can't guarantee the Raft Ready loop
(`Engine.drainReady`, `internal/raftengine/etcd/engine.go:1389`) runs
within a wall-clock bound. Raft uses its own tick (default 100 ms) to
drive elections; if the Ready loop is starved for a few hundred ms,
the step queue backs up (the 75,692 `step_queue_full` events on n4)
and heartbeats drop. That looks the same as node failure — election
storm follows.

### Mechanism

`runtime.LockOSThread()` on the Ready-loop goroutine and on the
per-peer dispatcher goroutines (PR #522 / `perf/raft-dispatcher-lanes`).
A locked goroutine owns its OS thread; the Go scheduler treats it as
pinned and the OS scheduler sees it as a normal thread under Linux
CFS. CFS is harder to starve because it doesn't have a user-space
work queue that can grow without bound. Converts "N% of a fair
Go-scheduler slice (can go to zero)" into "1 OS thread under CFS (OS
gives at least a small share per scheduling period)."

### Tradeoffs — flagged honestly

This may hurt more than help on big hosts.

- 4-core VM: one locked thread = 25% wall-clock guaranteed to Raft.
  Good.
- 16-core host: one locked thread = 6.25%, **lower** than what Ready
  gets from the Go scheduler today under non-pathological load. Naive
  pinning makes Raft *slower* on big hosts.
- Would need "N locked threads" scaling with dispatcher lanes and
  follower count, not a fixed 1.
- `LockOSThread` has subtle cgo and GC interactions. Measure before
  committing.

### Recommended v1 shape

**Do not do this in v1 unless Layer 1 + Layer 4 are in place and
measurement still shows `step_queue_full` > 0 on the leader under
normal load.** If we do ship it: `runtime.LockOSThread()` on the
Ready-loop driver and on each dispatcher lane; configurable
"dedicated raft threads" count, default equal to dispatcher-lane
count, floored at 1; `elastickv_raft_thread_locked` gauge.

### Where in the code

- `internal/raftengine/etcd/engine.go:1389` — `drainReady`; its
  caller goroutine is what needs locking. No `dispatcher_lanes.go` on
  main today; per-peer dispatchers live in `grpc_transport.go`, and
  the in-flight lanes branch is `perf/raft-dispatcher-lanes`.

### v1 vs later

- **v1:** nothing — measure first. If Layer 1 + Layer 4 eliminate the
  starvation, this layer is complexity tax for no gain.
- **If forced:** single locked thread, then N once dispatcher-lanes
  lands.

---

## 5. Layer 3 — Per-client admission control

### Problem it solves

37 connections from one peer IP got 37× the share of any fair
allocation. Layer 1's worker pool is *global* fairness (across all
clients combined); Layer 3 is *per-tenant* fairness (one noisy client
can't monopolize the pool).

### Relationship to the resilience roadmap

This overlaps directly with **item 6 of
`docs/design/2026_04_24_proposed_resilience_roadmap.md`** — "Connection
/ in-flight admission control." That doc specifies a per-adapter
in-flight semaphore plus a per-connection cap and composes with
memwatch. Layer 3 **extends** that, it does **not** replace it: item 6
remains the authoritative spec for the global cap and memwatch
interaction; this layer adds per-peer-IP fairness on top. Either
ordering works; if Layer 3 ships first, item 6 still needs to land for
the memwatch-composition contract.

### Mechanism

At accept time in `RedisServer.Run` (`adapter/redis.go:631`), wrap
`redcon.Serve`'s listener so we reject connections from a peer IP
already at its per-IP cap. Strictly easier than in-flight counting —
one check per accept, not per command.

### Tradeoffs

- Per-peer-IP is what TCP gives us for free. It doesn't understand
  AUTH identities, NAT, or L4 proxies. Behind an L4 proxy the cap
  becomes useless. Not our deployment today; flag for v2.
- A per-IP cap is trivially defeated by IP rotation. This is not a
  security mechanism; it's fairness against a cooperative-but-badly-
  behaved client like the 37-connection XREAD loop on 2026-04-24.
- Long-lived WATCH / MULTI still count against the owner's cap.
  Acceptable; document.

### Recommended v1 shape

**Per-peer-IP connection cap, default `N=8`, env-configurable,
enforced at accept.** On reject, accept the TCP connection, write a
`-ERR max connections per client exceeded` RESP error, then close —
so the client sees a protocol-level message instead of a bare
`connection reset` or `EOF` that's indistinguishable from a real
network failure. Per-client in-flight semaphore is deferred: it
requires threading client identity through every dispatch, which is
a bigger change than 2026-04-24 justifies.

**Avoiding a reject-storm feedback loop.** A client with an
aggressive reconnect pool can answer each `-ERR max connections`
with an immediate new `connect()` — the server spends CPU on the
accept/write/close cycle and the client makes no progress. Two
mitigations: (a) **rate-limit the reject itself**: once a peer IP
has been rejected `R` times in the last second, *skip the RESP
write* for subsequent rejects and close the fd with `SetLinger(0)`.
Pure Go `net` cannot emit a true kernel-level `RST` without
`Accept()`; the connection is already accepted by the time we know
to reject it, so "cheap" here means "fewer syscalls per reject
(accept + close)," not "no accept." A true `RST`-before-accept
requires dropping to a raw listener (`syscall.Accept4` + direct
`SO_LINGER` setup or an eBPF filter), deferred to v2. (b) document
operator-side client configuration (e.g., for redis-rb:
`reconnect_attempts=3` plus an exponential backoff). (a) ships in
v1 behind a compile-time constant; (b) belongs in the ops runbook.

### Where in the code

- `adapter/redis.go:631` — `Run`, where `redcon.Serve` is called.
  Wrap the `net.Listener` with a counting layer indexed by
  `RemoteAddr().(*net.TCPAddr).IP.String()`.
- Metric: `elastickv_redis_per_peer_rejected_total`. Bound Prometheus
  cardinality with a top-N sketch (same pattern as resilience-roadmap
  item 7).

### Interaction with memwatch

Per roadmap item 6: when memwatch crosses its soft threshold,
admission control starts rejecting *before* the hard-threshold
graceful-shutdown fires. Admission threshold set **lower** than
memwatch's. Gives in-flight work room to drain. Layer 3 subscribes to
the same soft-threshold signal item 6 defines.

### v1 vs later

- **v1:** per-peer-IP connection cap at accept.
- **Later:** per-auth-identity cap once AUTH is real; per-client
  in-flight semaphore; PROXY-protocol-aware client identity.

---

## 6. Layer 4 — XREAD (and friends) O(N) → O(new)

### Problem it solves

The afternoon profile is unambiguous: `loadStreamAt`
(`adapter/redis_compat_helpers.go:497`) reads the entire stream as one
blob at `redisStreamKey(key)` and unmarshals it
(`unmarshalStreamValue`, `adapter/redis_storage_codec.go:90`) on
*every* XREAD call, regardless of how many new entries there are. At
11 XREAD/s × 37 connections on a large stream, that's an O(stream²)
load over time. XREAD's defining contract is "give me entries after
this ID"; our implementation ignores the "after" hint.

### Mechanism

Store each stream entry at its own key. Sketched loosely (implementer
picks the exact bytes):

- `!redis|stream|<key>|meta` — metadata (length, last ID, consumer
  groups, PEL summary).
- `!redis|stream|<key>|entry|<entryID>` — one entry per key.

XREAD: read meta once, prefix-scan from `afterID`, unmarshal only the
new entries. O(new), matching the XREAD spec.

### Migration path

Streams persist across restarts and can be large, so no flag-day
rewrite.

**Two migration modes** — simple (PR #620, v1 stream PR) and chunked
(stacked follow-up). The dual-read rule differs between them; the
distinction matters for correctness.

**Mode A — simple migration (PR #620 ships this):** the first write
rewrites the entire legacy blob and deletes it in one Raft commit.
At any given instant a stream is either entirely legacy or entirely
per-entry; there is no mixed state. Read rule:

1. On XREAD/XRANGE/XLEN/XREVRANGE, read the per-entry layout.
2. If the per-entry meta key is absent AND the legacy blob key
   exists, fall back to the legacy path.
3. On the next write, rewrite to per-entry and delete the legacy blob
   in the same commit.

**Mode B — chunked migration (follow-up):** each write drains at
most `STREAM_MIGRATION_CHUNK` (default 1 024) entries from the legacy
blob into per-entry keys, and leaves the rest in a *legacy-suffix*
key until a subsequent write drains more. During this window the
stream exists in BOTH layouts simultaneously: the oldest N entries
are per-entry, the newer M entries are still in the suffix blob.

Read rule for Mode B — **always merge both layouts**, do not
fall-through on "new layout empty":

1. Read `meta` if present; read all per-entry keys that match the
   requested ID range.
2. Read the legacy-suffix blob if present; **protobuf cannot decode a
   repeated field partially**, so the blob is fully unmarshaled and
   then filtered to the requested ID range in memory. There is no
   cheap "decode only the range" path without custom wire-format
   parsing, which is out of scope for v1.
3. Merge by ID order, deduplicate (the migrator is responsible for
   never writing the same ID in both layouts in a single commit), and
   return.

Because step 2 is a full unmarshal, the suffix-blob cost is O(N_suffix)
per read regardless of how narrow the requested ID range is. This
matches the Mode B cost model below and is the reason Mode B ships
together with the read-driven drain.

The v1 dual-read (Mode A) is safe because there is no mixed state.
Extending it verbatim to Mode B would return incomplete results
during chunked migration — entries still in the legacy suffix would
be invisible to readers until the suffix was fully drained. Mode B
must ship together with the "always merge" read rule.

**Mode B cost model.** Decoding the legacy-suffix blob on every read
is O(N_suffix) protobuf unmarshal — the exact cost that Layer 4 was
introduced to eliminate. A partially-migrated stream therefore
still has the pre-fix hot path, just bounded by the suffix size
rather than the full stream size. Two mitigations:

- The migrator uses a **read-driven drain**: when a read observes a
  suffix blob, it enqueues a low-priority rewrite-N-entries job so
  hot streams drain first. Cold streams drain on their next write.
- Exporting `elastickv_stream_legacy_suffix_entries{stream}` as a
  top-N sketch lets operators see which streams still carry a
  suffix and size the `STREAM_MIGRATION_CHUNK` accordingly.

Neither fully reclaims Layer 4's O(new) guarantee during the
migration window; operators who cannot tolerate the transient cost
must stay on Mode A and accept the single-txn cost of the initial
migration instead.

**Legacy-fallback removal criterion.** Just watching
`elastickv_stream_legacy_format_reads_total == 0` is insufficient —
a cold legacy-format stream that is neither read nor written for
the soak window would keep the counter at zero while still needing
the fallback. Add a paired counter
`elastickv_stream_legacy_format_keys_total`.

Naive implementation would scan `!redis|stream|<...>` prefix, but
**that prefix is shared by every per-entry key**
(`!redis|stream|<key>|entry|<id>`), so a scan over it is
O(total_entries_in_cluster), not O(legacy_blobs). In a deployment
with many large migrated streams this is the exact cost profile
Layer 4 was introduced to eliminate.

Two implementable options, pick whichever is cheaper in the target
backend:

1. **Bloom-filter / sidecar index.** On every write that creates a
   legacy blob record the logical stream name in a dedicated
   `!redis|stream_legacy_index|<key>` tombstone-style marker. The
   migration write that rewrites the stream deletes that marker in
   the same commit. The counter becomes `SCAN !redis|stream_legacy_index|`,
   bounded by the number of legacy blobs, not total entries.
2. **Layout-walk.** Iterate `!redis|stream|<key>|meta` keys and, for
   each stream, probe `!redis|stream|<key>` (the legacy blob key has
   no suffix). Scan cost is O(num_streams), not O(num_entries).
   Equivalent answer; avoids the sidecar index.

Both pass the key-pattern sanity check: legacy keys live at
`!redis|stream|<logical>` with no further suffix, per-entry keys at
`!redis|stream|<logical>|entry|<id>`, meta at
`!redis|stream|<logical>|meta`. The "prefix scan on `!redis|stream|`"
wording in earlier drafts was wrong and has been retracted; the
layout above is authoritative.

The fallback is safe to remove only when **both** counters are zero
across every node. The index/walk runs at the same cadence as
snapshot cleanup; its cost is bounded by the chosen option as
described.

The existing stream PR (#620) ships **Mode A only**. Chunked
migration (Mode B) is explicitly deferred and must not be enabled
before the merged-read rule lands alongside it.

### Other one-blob-per-key collections

Spot-check of `adapter/redis_compat_helpers.go` confirms the same
pattern:

- **Hashes** — `loadHashAt:373`, `unmarshalHashValue` (codec line 30).
  Affects `HGETALL`, `HVALS`, `HKEYS`.
- **Sets** — `loadSetAt:419`, `unmarshalSetValue` (codec line 49).
  Affects `SMEMBERS`, `SUNION`, `SINTER`.
- **Sorted sets** — `loadZSetAt:473`, `unmarshalZSetValue` (codec
  line 70). Affects full-range `ZRANGE`/`ZRANGEBYSCORE` without `LIMIT`.

Same shape, different damage profile. `HGETALL` on a wide hash is the
next XREAD waiting to happen. Same migration pattern generalizes;
scope as follow-up. Fixing streams is the incident-driven change; the
rest is architectural follow-through.

`KEYS *` is a different shape — it scans the keyspace rather than
loading one blob — and is best contained by Layer 1's worker pool,
not by a layout change.

### Recommended v1 shape

Per-entry layout for **streams only** in v1. Dual-format read, rewrite
on next write, metric for legacy reads. Hashes/sets/zsets stay on the
one-blob layout until Layer 1 + Layer 3 are in and we have operational
data about which ones matter.

### Where in the code

- `adapter/redis_compat_helpers.go:497` — `loadStreamAt`, replace
  with prefix-scan loader taking `afterID`.
- `adapter/redis_storage_codec.go:90` — `unmarshalStreamValue`; add
  per-entry unmarshal alongside.
- `adapter/redis_compat_types.go:173` — `redisStreamKey`; add
  companion helpers for entry and meta keys.
- `adapter/redis_compat_commands.go:3898` (`xreadOnce`), `:3950`
  (`xread`).
- `adapter/redis_compat_helpers.go:810` — write path; dual-write
  during migration.

### v1 vs later

- **v1:** streams migrated to per-entry with dual-read.
- **Later:** same pattern for hashes/sets/zsets; drop legacy fallback
  once metric says it's safe.

---

## 7. Sequencing

Recommended order of implementation:

1. **Layer 4 first.** Correctness-shaped bug, concentrated change
   (one adapter file plus a codec), removes the specific hotspot
   that took down production on 2026-04-24. Testable in isolation
   with the existing `adapter/redis_bullmq_compat_test.go`
   workload.
2. **Layer 1 second.** Generic defense for the next unknown
   hotspot. Static command list is small, reviewable, and composes
   with Layer 4. **Once Layer 4 ships, XREAD's per-call cost is
   O(new) so in steady state it is cheap**, but we deliberately
   keep it gated in Layer 1 v1 for three reasons: (i) a client can
   still request a huge ID range via XRANGE / a massive `COUNT` on
   XREAD that the adapter must scan; (ii) the legacy fallback path
   is still reachable during the migration soak window and that
   path is still O(n); (iii) revisiting the classification after
   Layer 4 + Layer 6 metric is a reviewable data-driven decision,
   not a v1 speculation. The `elastickv_heavy_command_pool_submit_total{cmd="XREAD"}`
   metric added in Layer 1 is the signal that tells us when XREAD
   can graduate to ungated.
3. **Layer 3 third.** Per-client fairness. Coordinate with the
   resilience roadmap item-6 work so we don't ship two overlapping
   admission-control mechanisms. If item 6 ships first, Layer 3 is
   a small addition on top; if Layer 3 ships first, item 6 still
   needs to land for the memwatch-composition contract.
4. **Layer 2 last, and only if measurement justifies it.** After
   Layer 1 + Layer 4 are in place, check whether
   `Engine.StepQueueFullCount()` on the leader is still nonzero under
   realistic load. If it is, reconsider locked OS threads. If it
   isn't, don't add the complexity.

---

## 8. Open questions

1. **Layer 1 — classification policy.** Static list will drift. When
   do we promote ungated → gated? Proposed rule of thumb:
   `elastickv_redis_command_duration_seconds` p99 > 10 ms in prod for
   a week is a gated-candidate. Needs a concrete metrics-review
   process; not yet defined.

2. **Layer 1 — `-BUSY` retry storms.** A client retrying every 10 ms
   on `-BUSY` is approximately the same failure mode in the other
   direction. Document recommended client behavior (exp backoff +
   jitter). Consider a small server-side delay before `-BUSY` under
   sustained full — but delay-on-reject is a slippery slope toward
   queueing, which v1 rejects. Flag for review.

3. **Layer 2 — how many locked OS threads?** One is wrong on 16-core
   hosts; N is wrong on 4-core hosts. Proposed heuristic:
   `max(1, num_raft_dispatcher_lanes)`. Lanes are a PR-in-flight
   (`perf/raft-dispatcher-lanes`); confirm coupling against that
   design before committing.

4. **Layer 3 — per-IP vs per-auth-identity.** Redis AUTH is trivially
   faked by any client that knows the shared password; per-auth buys
   nothing today. Document the limitation so the v2 upgrade path
   (mTLS or PROXY protocol identity) is unsurprising.

5. **Layer 4 — migration window.** When can the dual-read
   compatibility code go away? Proposed: **both**
   `elastickv_stream_legacy_format_reads_total` = 0 **and**
   `elastickv_stream_legacy_format_keys_total` = 0 for 30 days across
   all nodes → remove in a follow-up PR. `reads_total` alone would
   miss cold streams (see Layer 4 removal criterion); the paired
   counter closes that gap. 30 days is arbitrary; revisit.

6. **Interaction with memwatch (PR #612).** memwatch fires graceful
   shutdown on hard-threshold crossing. Admission (Layer 3 / roadmap
   item 6) must reject at a *lower* threshold so in-flight work has
   room to drain. Contract: "admission reject → drain → memwatch
   shutdown," not "memwatch shutdown → drop work." This needs an
   explicit soft-threshold signal from memwatch that admission
   subscribes to; not in memwatch's current design; should land with
   Layer 3 / item 6.

7. **Interaction with PR #617 (GOMEMLIMIT defaults).** Layer 4
   removes most allocation pressure; Layer 1 bounds per-path
   allocation. Neither should need `GOMEMLIMIT` re-tuning, but
   confirm under load that post-Layer-4 heap steady-state sits
   comfortably below the limit with headroom for memwatch's soft
   threshold to fire before GC death spiral.

8. **PR #613 (WAL auto-repair) and PR #616 (tailscale deploy).**
   Operational / deploy-path changes, independent of this doc's
   runtime behavior. No coordination needed beyond review.
