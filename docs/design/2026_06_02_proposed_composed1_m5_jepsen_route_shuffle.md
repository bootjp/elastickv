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
>   1. The current `SplitRange` is exercised under realistic
>      concurrent multi-shard write load and proved non-regressing
>      (the workload finds **no** G1c, which is the baseline M4
>      contract).
>   2. When a future PR introduces a route-mutating RPC that DOES
>      shift ownership across groups (cross-group `SplitRange`,
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
- **NG3.** New Jepsen workload primitives (new operation types,
  new generators outside the route-shuffle nemesis). The
  existing `dynamodb-append-workload` is the right surface.
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

```
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

```
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

**Split key picking strategy (gemini medium R1).** Pick a split
key from inside the DynamoDB **table-route** key space
(`!ddb|route|table|<tableSegment>` — see `kv/shard_key.go:94-124`).
Concretely, with N tables `jepsen_append_t1` …
`jepsen_append_tN` per §3.3, the route key for table `tK` is
`!ddb|route|table|jepsen_append_tK`. Splits happen between
adjacent table-route keys — e.g. between `…jepsen_append_t2`
and `…jepsen_append_t3`. This guarantees:

- The split key falls **inside** the active workload route
  range (not lexicographically before or after, which would
  leave all workload keys on one side of the split).
- Each side of the split owns a distinct set of tables, so
  cross-table `TransactWriteItems` actually exercises 2PC.

A prior revision of this doc proposed a `/split/<int>` prefix.
That was lexicographically smaller than the workload's keyspace
(`/` < `0` in ASCII), so every workload key ended up on the
rightmost shard and the 2PC path was never exercised. Fixed
above by anchoring split keys to the table-route prefix.

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

### 3.3 Multi-shard workload guarantee (revised post-codex P1)

**Original §3.3 (Option A: single-key split in workload keyspace)
was wrong.** `kv/shard_key.go:94-124` normalises every DynamoDB
table-metadata, item, and GSI key to a single per-table route
key (`!ddb|route|table|<tableSegment>`). So every
`jepsen_append` item resolves to the SAME catalog point
regardless of its partition-key value, and a `SplitRange`
inside the item keyspace cannot put two items on different
shards. The 2PC path (`dispatchMultiShardTxn`, secondary
commits, the new `ErrTxnSecondaryRouteShiftedAfterPrimaryCommit`
sentinel) would never fire — invalidating G2 (codex P1 on
PR #905).

**Revised strategy: multi-table workload.** The M5 workload
creates `N` tables (default `N = 4`): `jepsen_append_t1` …
`jepsen_append_t4`. Each `TransactWriteItems` operation writes
to **at least two** distinct tables. The router maps each
table to its own table-route key, so a cross-table txn
naturally fans out across whichever shards own those route
keys. The setup hook splits the table-route keyspace at
`!ddb|route|table|jepsen_append_t2` so tables 1 lives on one
shard and tables 2–4 on another from t=0.

| Concern | Resolution |
|---|---|
| Workload shape change | Append ops still write a single value per row; the change is the table they write to (one per row, ≥2 rows per txn — picked from a per-txn random subset of `t1…tN`). |
| Elle compatibility | The append checker keys on `(table, partition-key)` pairs already (the workload's history shape supports this); cross-table txns appear as multi-key ops, which Elle handles natively. |
| Comparison with historical runs | Historical runs used a single table — the M5 workload is a NEW workload variant `dynamodb-append-multi-table-workload` rather than a modification of `dynamodb-append-workload`. Both ship; the existing one stays for trend comparison. |

The setup hook (Jepsen `db/setup!`) is gated to run only on
the FIRST node (`(when (= node (first (:nodes test))) …)`) so
the initial split is not attempted concurrently by every
cluster node and does not cause catalog-version conflicts
during bootstrap (gemini medium R3).

### 3.4 Success criterion

The workload's existing Elle checker emits a `:valid?` boolean
and a list of detected cycles (`:G0`, `:G1a`, `:G1b`, `:G1c`,
`:G-single`, etc.). M5's pass condition:

```
(and (:valid? results)
     (zero? (count (filter #(= (:type %) :G1c) (:anomalies results))))
     (zero? (count (filter #(= (:type %) :G-single) (:anomalies results)))))
```

`G1c` is the parent doc's explicit safety violation; `G-single`
is the closely-related single-item anomaly we already chase in
the existing workload. Other anomaly types (G0, G1a, G1b)
indicate orthogonal bugs and should also fail the run, but the
parent doc's M5 row names G1c as the headline criterion.

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
| M5a | CLI + multi-table workload | `cmd/elastickv-split` binary; new `dynamodb-append-multi-table-workload` that creates N tables and writes to ≥2 tables per `TransactWriteItems`; setup hook (gated to first node) issues the initial split between table-route keys. | `./scripts/run-jepsen-local.sh --workload dynamodb-append-multi-table` runs from t=0 with tables split across 2 shards; the workload exercises `dispatchMultiShardTxn` (verifiable via server-side log markers or a probe metric); Elle finds zero G1c. |
| M5b | Route-shuffle nemesis | `jepsen/src/elastickv/composed1_nemesis.clj`; compose into the multi-table workload's nemesis package; CLI flag `--composed1-route-shuffle` (default off, on under `run-jepsen-local.sh`). Nemesis re-queries `ListRoutes` before every split and picks split keys from inside the table-route keyspace. | A `./scripts/run-jepsen-local.sh --workload dynamodb-append-multi-table --composed1-route-shuffle` run produces zero G1c after ≥10 shuffles during a 5-minute run. |

M5a is a small, focused PR (Go CLI + Clojure setup hook +
docs). M5b carries the nemesis itself plus the cadence-tuning
analysis.

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
  gate for setup splits assumes Jepsen's `(first (:nodes test))`
  is stable across nodes; verify this matches actual Jepsen
  semantics (it should — `:nodes` is the test config, not a
  per-node view). Out of scope to design more carefully; will
  test at M5a implementation.

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
