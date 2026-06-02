# Composed-1 M5 — Jepsen route-shuffle workload

Status: Proposed
Author: bootjp
Date: 2026-06-02
Parent design:
[`2026_05_29_proposed_composed1_cross_group_commit_guard.md`](2026_05_29_proposed_composed1_cross_group_commit_guard.md)

> **Forward-looking proposal, same posture as the parent doc.**
> Today's `SplitRange` is same-group only (per CLAUDE.md and
> `adapter/distribution_server.go`'s implementation), so the
> Composed-1 hazard the M3/M4 guard catches cannot yet be
> *triggered* in production. M5 ships the integration-test
> scaffolding — workload shape, nemesis, success criterion — so
> that:
>
> 1. The current `SplitRange` is exercised under realistic
>    concurrent multi-shard write load and proved non-regressing
>    (the workload finds **no** G1c, which is the baseline M4
>    contract).
> 2. When a future PR introduces a route-mutating RPC that DOES
>    shift ownership across groups (cross-group `SplitRange`,
>      `MoveRange`, online rebalancer), the M5 workload — with a
>      one-line nemesis change to call the new RPC — becomes the
>      integration-level proof that M3+M4 hold under cross-group
>      churn.

---

## 1. Goals and non-goals

### 1.1 Goals

- **G1.** Add a `route-shuffle` nemesis to the DynamoDB Jepsen
  suite that issues `SplitRange` against the cluster at a
  configurable cadence concurrently with the existing DynamoDB
  workload's `TransactWriteItems` traffic.
- **G2.** Force the workload to issue **multi-shard** transactions
  with high probability so the 2PC path (`dispatchMultiShardTxn`,
  `commitSecondaryTxns`, the new
  `ErrTxnSecondaryRouteShiftedAfterPrimaryCommit` sentinel) is
  exercised.
- **G3.** Verify the workload's Elle checker reports **zero G1c**
  cycles after the run. This is M4's "Done when" criterion
  promoted from a unit test to an integration test.
- **G4.** Land a tiny CLI helper (`cmd/elastickv-split`, or a
  subcommand in `elastickv-admin`) that issues a single
  `SplitRange` RPC by route ID + split key + expected version.
  The Jepsen nemesis shells out to it rather than re-implementing
  the gRPC client in Clojure.

### 1.2 Non-goals

- **NG1.** A cross-group `SplitRange` or `MoveRange` RPC. The
  parent doc explicitly defers this; M5 must not depend on it.
- **NG2.** Reproducing a real Composed-1 anomaly. With same-group
  `SplitRange` only, no such anomaly is reachable; the workload's
  job is to prove the gate is non-regressing today and to be
  ready for tomorrow.
- **NG3.** New Jepsen workload primitives **beyond** the new
  `dynamodb-append-multi-table-workload` variant (§3.3) and the
  new `route-shuffle-nemesis` (§3.2).  No new operation types,
  no new generators outside those two surfaces.  Pre-existing
  `dynamodb-append-workload` stays as-is for trend comparison
  with historical runs.
- **NG4.** Changing the DynamoDB adapter or Composed-1 code on
  the server side. M5 is purely test-harness work.

---

## 2. Why this matters now

PR #900 lands M1+M2+M3+M4 on `feat/composed1-m4-retry`. The unit
tests cover each milestone in isolation. Two gaps remain that
only an integration test can close:

1. **End-to-end ordering.** The M3 gate runs inside the FSM
   apply path; the M4 retry runs inside `ShardedCoordinator`;
   the new `ErrTxnSecondaryRouteShiftedAfterPrimaryCommit`
   sentinel surfaces from `commitSecondaryTxns`. Each is unit-
   tested in isolation. None of the tests run the full
   prewrite → primary-commit → secondary-commit chain on real
   Raft groups under concurrent client load, which is where
   subtle apply-ordering bugs hide.
2. **Workload realism.** PR #900's nine review rounds each
   surfaced an auto-pin overreach (read-write, caller-StartTS,
   2PC secondary, resolver-claimed). The review process is
   thorough, but a Jepsen run is the empirical check: if the
   workload's Elle checker finds G1c against the M4 build, the
   review missed something.

If we ship M4 to `main` without M5, every later change to
routing, OCC, or the FSM apply path lacks the integration-level
sentinel that would catch a regression. M5 closes that gap.

---

## 3. Design

### 3.1 SplitRange invocation CLI (`cmd/elastickv-split`)

A standalone Go binary, ~80 lines:

```bash
elastickv-split \
  --address 127.0.0.1:50051 \
  --route-id 100 \
  --split-key /q1/00001 \
  --expected-version 7
```

Reads the four flags, dials the leader, issues
`proto.Distribution/SplitRange`, prints the new catalog version
and the two child route IDs on success. Non-zero exit on any
error so the Jepsen nemesis sees the failure.

The CLI lives in `cmd/elastickv-split/main.go`. No tests beyond
a smoke test (`main_test.go`) that runs `elastickv-split --help`
and asserts non-zero exit on missing flags. The real coverage
is the Jepsen run itself.

**Alternative considered:** add a `split` subcommand to
`cmd/elastickv-admin/`. Rejected because `elastickv-admin` is
the HTTP fanout admin and conflating it with a gRPC control-
plane invocation would muddy its scope. A standalone tool is
clearer.

### 3.2 Route-shuffle nemesis (`jepsen/src/elastickv/composed1_nemesis.clj`)

A new Clojure file with a single `route-shuffle-nemesis`
function returning a `jepsen.nemesis/Nemesis` instance:

```clojure
(defn route-shuffle-nemesis
  "Periodically invokes elastickv-split against the cluster.
   :start  -> shuffle one route (pick a non-edge split key)
   :stop   -> no-op (splits are durable, no rollback)"
  [opts]
  (reify nemesis/Nemesis
    (setup! [this test] ...)
    (invoke! [this test op] ...
       ;; 1. call ListRoutes to find the route currently covering
       ;;    the chosen split key — route IDs change after every
       ;;    split, so a cached ID from setup is stale
       ;; 2. pick a split key inside that route's range
       ;; 3. shell out to elastickv-split with route-id +
       ;;    split-key + expected-version from ListRoutes
       )
    (teardown! [this test] ...)))
```

The nemesis is composed with the existing
`jepsen.nemesis.combined/nemesis-package` (partitions + kills)
via `jepsen.nemesis/compose`. The combined nemesis becomes the
workload's `:nemesis`.

**Split key picking strategy (gemini medium R1 + codex P1 #1
on f5d2ad7a).** Pick a split key from inside the DynamoDB
**table-route** key space. The exact encoding matters: the
route key is built by `dynamoRouteTableKey(encodeDynamoSegment(tableName))`
(`adapter/dynamodb.go:8337-8365`, `kv/shard_key.go:117-124`).
`encodeDynamoSegment` is base64 `RawURLEncoding` — for table
`jepsen_append_t1` the real route key is
`!ddb|route|table|am...` (base64 of the literal table name),
**not** `!ddb|route|table|jepsen_append_t1`. A prior revision
of this doc used the raw table name, which would sort after
every base64-encoded segment and leave all workload tables on
one side of the split.

There are TWO distinct split keys to construct.  Getting them
right is the difference between an honest non-regression
check and a silent no-op:

**(a) Setup-time boundary key (one-shot, in
`scripts/run-jepsen-local.sh`).** This is the key that
partitions the table-route keyspace between groups at cluster
launch via `--shardRanges`.  It IS a route boundary by
design — group 1 covers `[… , bk)`, group 2 covers
`[bk, …)`.  Constructed as:

```clojure
(str "!ddb|route|table|"
     (encode-dynamo-segment "jepsen_append_t2"))  ; base64 RawURLEncoding
```

**(b) Per-shuffle interior key (every nemesis `:start` invocation).**
After the setup-time boundary lands, the table-route key
`!ddb|route|table|<encode("jepsen_append_t2")>` is now the
**`Start`** of the right child route.  `validateSplitKey`
in `adapter/distribution_server.go:357-372` rejects
`splitKey == parent.Start` or `splitKey == parent.End` with
"split key at route boundary" — so reusing the setup-time key
would fail every nemesis tick after the first (codex P2 #2 on
3ca2a7f7).  In a 5-minute run expecting ≥10 shuffles, the
nemesis would no-op silently and the workload would never
exercise the catalog-version churn M3+M4 must absorb.

The nemesis MUST derive a fresh interior key per `:start`
that lies strictly between the current route's `Start` and
`End`.  Strategy:

```clojure
(defn fresh-interior-split-key
  "Returns a key strictly inside [route.start, route.end).
   The base64 RawURL alphabet (A-Z, a-z, 0-9, -, _) admits
   simple suffix appending: any byte from that alphabet
   appended to route.start sorts after route.start but
   before any byte that is not in the alphabet (such as ASCII
   '|' = 124, which is greater than every base64 char)."
  [route]
  ;; Append a fresh alphabetic byte plus an incrementing
  ;; counter; check the result is < route.end and retry with
  ;; a different prefix byte on collision.
  (str (:start route) "A" (System/nanoTime)))
```

Exact strategy details (counter persistence across nemesis
restarts, collision recovery, byte-vs-string coercion of
`ListRoutes` response fields — `(str (:start route) …)`
silently yields `"[B@…"` if `:start` is a Java byte array
rather than a string, a silent mis-routing footgun flagged
by claude[bot] on b752a894) are M5b implementation notes —
the design contract is: each shuffle's split key is strictly
interior to the route's current range.

The Clojure side gets a small helper (`composed1-nemesis/encode-dynamo-segment`)
that mirrors the Go `encodeDynamoSegment` exactly. The Go
implementation uses `base64.RawURLEncoding` — URL-safe charset
(`-`/`_`) **without `=` padding**. The Clojure side MUST use
`(.withoutPadding (java.util.Base64/getUrlEncoder))`;
`Base64/getEncoder` (standard alphabet with `+`/`/`) and the
default `Base64/getUrlEncoder` (URL-safe **with** padding) are
both wrong (claude[bot] P3 on 24812867).  The failure mode is
silent and non-obvious: a wrong encoding produces split keys
that fall outside every route's range, `ListRoutes` returns no
matching route, the nemesis logs and skips every `:start`,
and the run appears clean while the nemesis is a no-op.

The Composed-1 doc and the encoding helper land together so
any future change to the encoding surface is caught by an
unambiguous reference point.

A prior revision of this doc also wrongly explained the old
`/split/<int>` proposal: it said `/` < workload keys, but the
DynamoDB table-route prefix starts with `!` (ASCII 33) and
`/` is ASCII 47, so `/split/<int>` is lexicographically
**larger** than `!ddb|route|table|*` — workload keys would land
on the **left** shard, not the rightmost (claude[bot] P2 on
f5d2ad7a). The fix (anchoring to the table-route prefix)
is correct in either direction; the explanation above is now
also correct.

**Route ID resolution (gemini medium R2).** The nemesis CANNOT
rely on a single `ListRoutes` call + a local counter — every
successful split deletes the parent route ID and creates two
fresh child IDs, so a cached route ID is stale on the next
shuffle. On every `:start` invocation the nemesis re-queries
`ListRoutes`, walks the returned snapshot to find the route
whose range contains the chosen split key, and uses that
route's ID + the snapshot's `version` as the
`SplitRangeRequest`'s `expected_catalog_version`. Catalog
drift (another split landing concurrently between
`ListRoutes` and `SplitRange`) surfaces as
`ErrCatalogVersionMismatch` from the server; the nemesis logs
and refreshes on the next tick.

### 3.3 Multi-shard workload guarantee (revised post-codex P1 #2)

**Original §3.3 (single-key item split) and revised §3.3
(multi-table with setup-hook SplitRange) were both incomplete.**

Two facts about today's routing surface make the "split at
setup" approach insufficient on its own:

1. `kv/shard_key.go:94-124` normalises every DynamoDB
   table-metadata, item, and GSI key to a single per-table
   route key (`!ddb|route|table|<tableSegment>`).  So
   single-table workloads have one route key and cannot
   fan out across shards.
2. `adapter/distribution_server.go`'s `SplitRange` only
   creates child routes with the **parent's GroupID**.
   `dispatchTxn` enters `dispatchMultiShardTxn` only when
   mutations group to ≥2 distinct Raft **groups**, not just
   ≥2 routes (codex P1 #2 on f5d2ad7a).  A `SplitRange`
   inside the table-route keyspace produces two routes still
   in the same group — single-shard transaction path,
   2PC never fires.

**Revised strategy: multi-table workload + multi-group cluster
startup.** Both halves are required.

| Half | Mechanism |
|---|---|
| Multi-table workload | A NEW workload variant `dynamodb-append-multi-table-workload` creates N=4 tables (`jepsen_append_t1` … `jepsen_append_t4`) and writes to ≥2 distinct tables per `TransactWriteItems`.  The router maps each table to its own route key so cross-table txns engage multi-route routing. |
| Multi-group cluster | M5a extends `scripts/run-jepsen-local.sh` to launch a multi-group cluster.  Per `shard_config.go:61-99` (claude[bot] P2 on 24812867), `--shardRanges` alone is not enough: without `--raftGroups`, every shard range's `groupID` collapses into the single default group 1 and `groupMutations` returns `gids = [1]` for every key — single-shard path, 2PC never fires.  M5a must therefore add **five** coordinated launch-script changes:<br><br>1. **`--raftGroups`** declaring ≥2 groups (e.g. `1=127.0.0.1:50051,2=127.0.0.1:50054`).<br>2. **`--shardRanges`** boundary keys placing `t1-t2` in group 1 and `t3-t4` in group 2.<br>3. **Cluster topology decision — constrained by the current server.**  `shard_config.go:399-410` (`validateShardRanges`) requires every `--shardRanges` group ID to appear in that process's `--raftGroups`, and `main.go:764` (`buildShardGroups`) starts a local Raft runtime for every group listed in `--raftGroups` (codex P1 on 6f024aaa).  Consequence: "two processes, each hosting one group" is NOT supported — Process A with `--raftGroups "1=…"` and a `--shardRanges` entry pointing to group 2 fails validation; configuring both processes with `--raftGroups "1=…,2=…"` makes both try to host both groups and race on the Raft listeners.<br>The supported topology for M5a is **one process hosting both single-member groups** — two Raft addresses (`50051` for group 1, `50054` for group 2), shared by a single Dynamo endpoint (e.g. `63801`).  Each group is single-member (just this process) so `--raftBootstrap` works for both.  Cross-group txns go through the in-process router → 2PC fires across the two co-located groups.<br>**Limitation accepted for M5a:** because both groups live in one process, partition/kill nemeses can't separate them — only the route-shuffle nemesis (API-level) exercises the cross-group path meaningfully.  True distributed multi-group (multi-process) requires server-side support for "remote-only groups in `--raftGroups`" or equivalent, which is M6+ work and out of M5's scope.<br>4. **Per-process `--raftBootstrap` (boolean) — NOT `--raftBootstrapMembers`.**  `main.go:735-741` has a hard guard: `resolveBootstrapServers` returns `ErrBootstrapMembersRequireSingleGroup` whenever `--raftBootstrapMembers` is set and `--raftGroups` parses to more than one group (codex P2 + claude[bot] P2 on 3ca2a7f7).  For the single-process-two-groups topology, the process needs no peer discovery for either of its two single-member groups, so `--raftBootstrap` is necessary and sufficient.  The existing `--raftBootstrapMembers` in `run-jepsen-local.sh` must be **removed** for the M5a launch; it is only valid for single-group, multi-node formations.<br>5. **Single `--raftDynamoMap`** entry pointing the process's Dynamo address at itself (no cross-process leader fan-out is needed since there's only one process); port allocation simplifies — drop the `5005{1,2,3}`/`6380{1,2,3}` 3-node layout to a 1-process layout with two Raft ports (`50051`, `50054`) and one Dynamo port (`63801`). |

`encodeDynamoSegment("jepsen_append_t1")` etc. are computed at
script setup time and inlined into the `--shardRanges`
boundary keys.  An `m5_setup.sh` helper (or a Go binary, see
OQ-7) emits the boundary keys deterministically.

The Jepsen `db/setup!` hook does NOT issue any `SplitRange`.
Instead it calls `ListRoutes` once (gated to the first node
per `(when (= node (first (:nodes test))) …)`) and **verifies**
that the expected multi-group routing is in place; if not, it
fails fast with a clear error so the operator knows the
launch script needs to be re-run with the right
`--shardRanges`.  This avoids the gemini-medium R3 setup-hook
concurrency hazard entirely (no mutation, just a read).  It
also resolves the claude[bot] P3 follow-up about needing
ListRoutes for route-ID resolution in setup — the hook now
needs ListRoutes for **verification** rather than mutation.

| Concern | Resolution |
|---|---|
| Workload shape change | Append ops still write a single value per row; the change is the table they write to (one per row, ≥2 rows per txn — picked from a per-txn random subset of `t1…tN`). |
| Elle compatibility | The append checker keys on `(table, partition-key)` pairs already (the workload's history shape supports this); cross-table txns appear as multi-key ops, which Elle handles natively. |
| Comparison with historical runs | The pre-existing `dynamodb-append-workload` (single table, single group) stays as-is for trend comparison.  The M5 workload is a new variant alongside it. |

**Why the nemesis is still useful even though SplitRange is
same-group only.**  The route-shuffle nemesis (§3.2) issues
`SplitRange` calls that churn the catalog version + route IDs
without moving ownership across groups.  This exercises the
M3 ObservedRouteVersion drift detection and the M4 retry
path under concurrent route-mutating control-plane traffic,
which is the closest non-regression check the current routing
surface allows.  When a future cross-group `MoveRange` or
cross-group `SplitRange` lands, swapping that RPC into the
nemesis turns the workload from a "no-regression under
same-group churn" check into a "no G1c under cross-group
movement" check — matching the parent doc's forward-looking
posture.

### 3.4 Success criterion

The workload's existing Elle checker emits a `:valid?` boolean
and an `:anomalies` map keyed by anomaly type — `{:G0 […], :G1a
[…], :G1c […], :G-single […], …}`.  M5's pass condition:

```clojure
(and (:valid? results)
     (nil? (get (:anomalies results) :G1c))
     (nil? (get (:anomalies results) :G-single)))
```

A prior revision of this doc used `(filter #(= (:type %) :G1c)
…)` over `(:anomalies results)`, which is wrong: iterating a
map yields `[key val]` vectors with no `:type` field, so the
filter always returned an empty seq and the check would
silently pass on any G1c run (claude[bot] P2 on f5d2ad7a).
The corrected form above keys off the map directly.

`G1c` is the parent doc's explicit safety violation; `G-single`
is the closely-related single-item anomaly we already chase in
the existing workload. Other anomaly types (G0, G1a, G1b)
indicate orthogonal bugs and should also fail the run, but the
parent doc's M5 row names G1c as the headline criterion.
`(:valid? results)` is the canonical Elle pass/fail bit; the
explicit G1c / G-single checks are belt-and-suspenders so a
future Elle refactor that subdivides the cycle taxonomy still
fails on the specific anomalies M5 cares about.

### 3.5 Cadence

Default `:route-shuffle-interval` = `30s`. Configurable via the
test CLI. Rationale: the workload's typical txn rate is ~10
ops/sec across 5 concurrent clients (= 50 ops/sec), so a
30s shuffle puts ~1500 txns between shuffles — enough to
plausibly catch a mid-2PC race, but rare enough that the run
doesn't degenerate into "every txn races a split."

The route-shuffle nemesis composes with the existing
partition+kill nemesis. The combined nemesis fires at the
shortest of its members' intervals (Jepsen default
behaviour); kills/partitions remain at their existing 40s.

---

## 4. Milestone breakdown

Two phases. The phases land in this order; the first is
mergeable on its own.

| Phase | Title | Scope | Done when |
|---|---|---|---|
| M5a | CLI + multi-table workload + multi-group launch | `cmd/elastickv-split` binary; new `dynamodb-append-multi-table-workload` that creates N tables and writes to ≥2 tables per `TransactWriteItems`; **`scripts/run-jepsen-local.sh` extended to pass BOTH `--raftGroups` (declaring ≥2 groups) AND `--shardRanges` (placing tables across those groups)** — per §3.3, `--shardRanges` alone collapses every range to the single default group 1 and 2PC never fires; both flags are required for the multi-group contract.  Table-route keys for `t1…tN` are statically split across the declared groups; setup hook calls `ListRoutes` from the first node and verifies the multi-group routing is in place (read-only, no mutation). | `./scripts/run-jepsen-local.sh --workload dynamodb-append-multi-table` runs from t=0 with tables 1-2 owned by group 1 and tables 3-4 owned by group 2; the workload exercises `dispatchMultiShardTxn` (verifiable via server-side log markers or a probe metric); Elle finds zero G1c. |
| M5b | Route-shuffle nemesis | `jepsen/src/elastickv/composed1_nemesis.clj`; compose into the multi-table workload's nemesis package; CLI flag `--composed1-route-shuffle` (default off, on under `run-jepsen-local.sh`). Nemesis re-queries `ListRoutes` before every split and picks split keys from inside the table-route keyspace. | A `./scripts/run-jepsen-local.sh --workload dynamodb-append-multi-table --composed1-route-shuffle` run produces zero G1c after ≥10 shuffles during a 5-minute run. |

M5a is a single focused PR but no longer "small" after the
codex P1 #2 expansion: `cmd/elastickv-split` (Go CLI), the
new `dynamodb-append-multi-table-workload` (Clojure), the
`scripts/run-jepsen-local.sh` multi-group launch extension
(five coordinated changes — see §3.3 table), the setup-hook
verification (read-only `ListRoutes` + assertion), plus docs.
Likely to land as a single PR for atomicity (the workload
variant only makes sense alongside the multi-group launch and
the verification hook), but reviewable as four roughly
independent diff sections.  M5b carries the nemesis itself
plus the cadence-tuning analysis.

---

## 5. Open questions

- **OQ-1.** Should the nemesis also issue an `Abort`-shaped
  fault that interrupts an in-flight 2PC mid-prewrite? The
  existing partition nemesis effectively does this. Tentative
  answer: no, the partition nemesis is enough; adding a
  prewrite-interrupt would test `abortPreparedTxn`, which is
  out of M5's scope.
- **OQ-2.** Do we ship M5a + M5b in a single PR or two? Two
  is cleaner but doubles the review burden. With the §3.3
  revision M5a is now meaningfully bigger (a new workload
  variant, not just a setup hook), so two-PR is now the more
  likely shape. Decide at implementation time.
- **OQ-3.** Where does the new `cmd/elastickv-split` slot in
  the README and the `make` targets? Likely add it to
  `make tools`, mirror in `docs/operations/` (does this dir
  exist? — check at implementation). Out of scope for the
  design doc itself.
- **OQ-4** (resolved post-PR #900 merge). The parent doc
  rename `*_proposed_*.md` → `*_partial_*.md` should land as a
  separate small doc-only PR now that PR #900 is merged. When
  M5 ships, rename both this doc and the parent to
  `*_implemented_*.md` (tentative — keep both files separate
  so the M5 design history isn't lost).
- **OQ-5** (new, codex P1 follow-up). Is `N = 4` tables the
  right default? Trade-offs: more tables = better 2PC
  fan-out coverage but slower setup and noisier history. The
  workload's existing `:concurrency` defaults to 5, so 4
  tables means each client touches ~all of them per txn on
  average. Defer to implementation; revisit if the workload
  becomes I/O-bound on table-meta lookups.
- **OQ-6** (new, gemini medium R3 follow-up). The first-node
  gate for the setup verification assumes Jepsen's
  `(first (:nodes test))` is stable across nodes; verify this
  matches actual Jepsen semantics (it should — `:nodes` is the
  test config, not a per-node view). Out of scope to design
  more carefully; will test at M5a implementation.
- **OQ-7** (new, codex P1 #1 follow-up). The `--shardRanges`
  boundary keys for the multi-group launch (§3.3) need to be
  emitted as bytes that exactly match
  `dynamoRouteTableKey(encodeDynamoSegment(tableName))`. Two
  options: (a) a tiny Go helper (`cmd/elastickv-route-key`)
  that prints the encoded key for a given table name, called
  by `scripts/run-jepsen-local.sh` to build `--shardRanges`;
  (b) inline the base64 encoding in shell. Tentative answer:
  (a), because the encoding lives in Go and any drift would
  silently mis-route. Decide at implementation.

---

## 6. Self-review summary

Five-pass per CLAUDE.md:

1. **Data loss.** No new write paths; the CLI invokes the
   existing `SplitRange` RPC which already has full unit + e2e
   coverage. Nemesis-driven calls of an existing RPC can't lose
   committed writes (worst case: a split fails and the test
   fails, no data effect).
2. **Concurrency / distributed failures.** The nemesis runs
   under Jepsen's existing concurrency harness alongside
   partitions + kills. Combined behaviour is the *point* of the
   test — if anything breaks, the workload finds it. No new
   server-side concurrency code is being introduced.
3. **Performance.** Nemesis fires every 30s; CLI invocation is a
   single short-lived gRPC call. No measurable impact on hot
   paths.
4. **Data consistency.** This IS the data-consistency check
   (G1c = serializability violation). The success criterion is
   the property we want.
5. **Test coverage.** M5 ships the integration test; the
   smoke test on the CLI is the only unit-level coverage,
   correctly. The CLI's logic is thin enough that a smoke test
   plus the Jepsen run constitute adequate coverage.
