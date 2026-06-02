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
       ;; shell out to elastickv-split with a fresh split key
       )
    (teardown! [this test] ...)))
```

The nemesis is composed with the existing
`jepsen.nemesis.combined/nemesis-package` (partitions + kills)
via `jepsen.nemesis/compose`. The combined nemesis becomes the
workload's `:nemesis`.

**Split key picking strategy.** A simple monotonically-increasing
counter: every `:start` invocation appends a fresh integer
suffix to a fixed key prefix the workload reserves. This avoids
collisions with the workload's keyspace and guarantees the
split always picks a key that's between existing keys (so the
operation succeeds against a real catalog).

**Expected version.** The nemesis calls `ListRoutes` once at
setup to learn the current catalog version, then increments its
local copy by 1 after each successful split. Catalog drift
(another split landing concurrently) is rare in practice — if it
happens, the nemesis logs and refreshes from `ListRoutes`.

### 3.3 Multi-shard workload guarantee

The existing `dynamodb-append-workload` writes to a per-key
queue. With a single shard layout, every write goes to that
shard — no 2PC, no Composed-1 exposure.

M5 needs the workload to consistently span shards. Two options:

| Option | Mechanism | Pro | Con |
|---|---|---|---|
| **A** Force initial split | The test setup issues one `SplitRange` before the workload starts | Workload runs on 2+ shards from t=0 | Adds a setup step; needs a known split key |
| **B** Multi-key txns | Modify each `:append` op to write to ≥2 keys with deterministic routing across shards | Workload exercises 2PC even on a 1-shard layout | Changes the workload's operation shape (harder to compare against historical runs) |

**Choose A.** Less invasive to the workload, and the
route-shuffle nemesis itself increases the shard count over
time, giving organic multi-shard coverage.

The setup hook (`db/setup!` in Jepsen parlance, or the test's
`:setup` map) runs `elastickv-split` once with a split key in
the middle of the workload's keyspace.

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
| M5a | CLI + workload setup | `cmd/elastickv-split` binary; `dynamodb-append-workload`'s `:setup` issues the initial split; no nemesis yet. | `./scripts/run-jepsen-local.sh` runs unchanged but the cluster starts with 2 shards. Workload finds zero G1c (trivially, no shuffle). |
| M5b | Route-shuffle nemesis | `jepsen/src/elastickv/composed1_nemesis.clj`; compose into `dynamodb-append-workload`'s nemesis package; CLI flag `--composed1-route-shuffle` (default off, on under `run-jepsen-local.sh`). | A `./scripts/run-jepsen-local.sh` run with `--composed1-route-shuffle` produces zero G1c after ≥10 shuffles during a 5-minute run. |

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
- **OQ-2.** Do we ship M5a + M5b in a single PR or two? Two is
  cleaner but doubles the review burden. Tentative answer: two
  if M5a's CLI work runs ≥150 lines (likely); one if M5a fits in
  a single screen. Decide at implementation time.
- **OQ-3.** Where does the new `cmd/elastickv-split` slot in
  the README and the `make` targets? Likely add it to
  `make tools`, mirror in `docs/operations/` (does this dir
  exist? — check at implementation). Out of scope for the
  design doc itself.
- **OQ-4.** Should the M5 design doc rename happen with PR #900
  merge (since M1–M4 ship)? Yes per CLAUDE.md's lifecycle
  guidance: rename `*_proposed_*.md` → `*_partial_*.md` after
  PR #900 lands, then this M5 doc tracks the open milestone.
  When M5 ships, rename the parent to `*_implemented_*.md` and
  this M5 doc to `*_implemented_*.md` as well (or fold the M5
  content back into the parent — tentative answer: keep them
  separate so the M5 design history isn't lost).

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
