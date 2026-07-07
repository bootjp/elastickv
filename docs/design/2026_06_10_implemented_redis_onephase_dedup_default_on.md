# Redis one-phase txn dedup — flip default to on

Status: Implemented
Author: bootjp
Date: 2026-06-10

> **Status: implemented.** `adapter.RedisServer.onePhaseTxnDedup` is
> default-on, closing the rollout of the option-2 idempotency design landed by
> [`2026_05_21_proposed_txn_secondary_idempotency.md`][parent]. No new
> mechanism — the FSM probe (`dedupProbeOnePhase` in `kv/fsm.go`), the
> wire change (`TxnMeta.PrevCommitTS`, V2-only when non-zero), and every
> adapter-side reuse path (RPUSH/LPUSH, MULTI/EXEC, standalone SET, etc.)
> already ship and have been exercised by the dedup-mode Jepsen workflow
> for **12 consecutive green runs (2026-05-31 → 2026-06-10)**, exceeding
> the parent design's `M4` 7-day criterion. The rolling-upgrade reader
> (probe code) has been on every node in production for months; only the
> writer (emission default) remains.
>
> Triggered by Jepsen Scheduled Stress run
> [27033231956](https://github.com/bootjp/elastickv/actions/runs/27033231956/job/79790587770)
> on 2026-06-05 — the **dedup-OFF control baseline workflow**
> ([`jepsen-test-scheduled.yml`](../../.github/workflows/jepsen-test-scheduled.yml))
> surfaced `:duplicate-elements`, `:future-read`, `:G-single-item-realtime`,
> and `:G2-item-realtime` on the Redis list-append workload, exactly the
> anomaly shape the parent design was written to prevent. Issue
> [#937](https://github.com/bootjp/elastickv/issues/937) tracks that
> failure; this proposal closes it by retiring the unprotected default.

[parent]: 2026_05_21_proposed_txn_secondary_idempotency.md

## Background

The parent design landed an FSM-side dedup probe and an adapter-side
write-set reuse path keyed on a stale `commit_ts` ridden through
`OperationGroup.PrevCommitTS` into a V2 `TxnMeta`. The probe is always
present; emission of `prev_commit_ts != 0` is gated by
`RedisServer.onePhaseTxnDedup` (constructor: `WithOnePhaseTxnDedup`, env
rollback: `ELASTICKV_REDIS_ONEPHASE_DEDUP=0`). The gate now defaults on
because the cluster has uniformly upgraded — the parent's R5 "ship the reader
before the writer" sequencing is satisfied.

During rollout, two Jepsen workflows ran the same stress profile
(`--time-limit 150 --rate 10 --concurrency 8 --key-count 16
--max-writes-per-key 250 --max-txn-length 4`) every day against `main`:

| Workflow | Env | Purpose |
|---|---|---|
| [`jepsen-test-scheduled.yml`][scheduled] | `ELASTICKV_REDIS_ONEPHASE_DEDUP=0` during the temporary control window | Legacy-path baseline after the default flip. Retired on 2026-06-26. |
| `jepsen-test-scheduled-dedup.yml` | `ELASTICKV_REDIS_ONEPHASE_DEDUP=1` (on) | M4 validation to authorize default-on. Deleted on 2026-06-26 after dedup-on became the standard scheduled path. |

[scheduled]: ../../.github/workflows/jepsen-test-scheduled.yml

## M4 evidence

The parent design's `M4` criterion is *"7 consecutive days without
`:duplicate-elements` / `:G-single-item-realtime` in the dedup-mode
workflow."*

Dedup-mode run history on `main` from the retired
`jepsen-test-scheduled-dedup.yml` workflow:

| Date (UTC) | Run | Conclusion |
|---|---|---|
| 2026-06-10 04:50 | [27253991270](https://github.com/bootjp/elastickv/actions/runs/27253991270) | success |
| 2026-06-09 04:44 | [27184402445](https://github.com/bootjp/elastickv/actions/runs/27184402445) | success |
| 2026-06-08 04:57 | [27116904871](https://github.com/bootjp/elastickv/actions/runs/27116904871) | success |
| 2026-06-07 04:54 | [27083142341](https://github.com/bootjp/elastickv/actions/runs/27083142341) | success |
| 2026-06-06 04:40 | [27052820868](https://github.com/bootjp/elastickv/actions/runs/27052820868) | success |
| 2026-06-05 04:49 | [26996058409](https://github.com/bootjp/elastickv/actions/runs/26996058409) | success |
| 2026-06-04 04:57 | [26931749014](https://github.com/bootjp/elastickv/actions/runs/26931749014) | success |
| 2026-06-04 04:16 | [26930308744](https://github.com/bootjp/elastickv/actions/runs/26930308744) | success |
| 2026-06-03 04:58 | [26864667493](https://github.com/bootjp/elastickv/actions/runs/26864667493) | success |
| 2026-06-02 04:56 | [26799333263](https://github.com/bootjp/elastickv/actions/runs/26799333263) | success |
| 2026-06-01 04:58 | [26736041841](https://github.com/bootjp/elastickv/actions/runs/26736041841) | success |
| 2026-05-31 04:52 | [26703624971](https://github.com/bootjp/elastickv/actions/runs/26703624971) | success |

12 consecutive green runs over 10 calendar days, on the exact stress
profile that produced the parent design's trigger anomaly. M4 is met.

The control workflow (`jepsen-test-scheduled.yml`, gate off) continues
to surface `:duplicate-elements` and the related real-time cycle
anomalies as expected; the 2026-06-05 18:37 failure
([#937](https://github.com/bootjp/elastickv/issues/937)) is the
most recent observation. The control's purpose ends when default-on
lands — see *§Control workflow disposition* below.

## Proposal

### M1 — Flip the default

`adapter/redis.go` constructs the server with:

```go
onePhaseTxnDedup: os.Getenv("ELASTICKV_REDIS_ONEPHASE_DEDUP") == "1",
```

After this PR:

```go
// Default on; set ELASTICKV_REDIS_ONEPHASE_DEDUP=0 to opt out.
onePhaseTxnDedup: os.Getenv("ELASTICKV_REDIS_ONEPHASE_DEDUP") != "0",
```

The env-var sense inverts from opt-in to opt-out. `=1` (the dedup-mode
workflow's existing setting) and unset both resolve to `true`; only the
explicit `=0` opts out. `WithOnePhaseTxnDedup(bool)` continues to be the
constructor-level override and trumps the env var either way.

Comment and `WithOnePhaseTxnDedup`'s godoc are updated to describe the
new default; the in-file pointer to R5 stays (now reframed as "the
ordering constraint that authorized this default flip").

### M1a — Carve out standalone `SET` behind a separate sub-gate

PR #943 round-1 codex P1 flagged that `txnContext.applySet`
(`adapter/redis.go:2356-2360`) returns `WRONGTYPE` when the existing
key is a list/hash/set/zset/stream, while the legacy `setLegacy` →
`executeSet` → `replaceWithStringTxn` path correctly deletes the
collection's logical elements and writes the string. Real Redis lets
`SET k v` overwrite any existing type unconditionally; flipping
`onePhaseTxnDedup` default-on therefore would have silently changed
normal Redis overwrite behaviour for every standalone-`SET`-against-
collection configuration.

This proposal does not bring `applySet` to parity (collection-deletion
inside the dedup txn is a larger surgery, tracked as a separate
follow-up). Instead it introduces a dedicated sub-gate
`RedisServer.standaloneSetDedup` (default off, opt-in via
`WithStandaloneSetDedup(true)` or
`ELASTICKV_REDIS_ONEPHASE_DEDUP_SET=1`). The standalone-`SET` branch
in `set()` now requires **both** `onePhaseTxnDedup` **and**
`standaloneSetDedup` to be true before routing through
`runTransactionWithDedup`; the previous behaviour is preserved as the
default. MULTI/EXEC and list-push paths (the parent design's actual
M4-validated workloads) are unaffected.

The new field, option, env var, and the regression test
`TestRedis_SET_OverwritesList_UnderDefaultGate`
(`adapter/redis_set_overwrite_default_test.go`) all land in this PR.
The test seeds `RPUSH k x`, calls `SET k v`, asserts `OK` + a
subsequent `GET` returns `v` — it fails on the pre-fix build
(WRONGTYPE) and passes on the fixed build.

### M2 — Control workflow disposition

After default-on, `jepsen-test-scheduled.yml` would silently exercise
the same path as `jepsen-test-scheduled-dedup.yml` (unset env -> true),
so the two workflows would collapse to the same coverage. Two options:

| Option | Effect | Recommendation |
|---|---|---|
| **A** Keep the control by setting `ELASTICKV_REDIS_ONEPHASE_DEDUP=0` in `jepsen-test-scheduled.yml`'s job env. | Continues to publish the unprotected-path baseline; lets us notice if anomalies on the off-path ever vanish (suggesting the workload regressed in coverage). | Yes for the first 30 days post-flip. After that, retire if signal stays unchanged. |
| **B** Delete `jepsen-test-scheduled.yml` entirely. | One fewer cron consuming runners. Loses the legacy-path baseline. | No, until 30 days of post-flip data shows the dedup-mode workflow is the only signal we read. |

This PR picks **A**: add explicit `ELASTICKV_REDIS_ONEPHASE_DEDUP: "0"`
to `jepsen-test-scheduled.yml`'s top-level `env:` so the control retains
its meaning across the default flip. The 30-day retirement decision
becomes a follow-up issue.

Post-flip cleanup on 2026-06-26 retires that temporary control: the
`ELASTICKV_REDIS_ONEPHASE_DEDUP=0` opt-out was removed from
`jepsen-test-scheduled.yml`, and the dedicated dedup-mode workflow was deleted.

### M3 — Issue #937 closure

Update [#937](https://github.com/bootjp/elastickv/issues/937) with the
M4 evidence + this proposal link, then close as "expected baseline; the
dedup default flip in this PR is the fix." The audit hypothesis from
the issue body ("client-retry across connection-close bypasses the
probe") is **incorrect**: `PrevCommitTS` rides on the request envelope,
not on the client TCP connection, so any retry that re-enters the
adapter's `retryRedisWrite` loop (the path Jepsen exercises) carries
the probe regardless of whether the underlying connection closed. The
12-day green dedup-on streak on the identical workload demonstrates
the existing mechanism closes the gap; no new design surface is
required.

## R-references reused from the parent

This proposal does not introduce new risks; every risk from the parent
applies unchanged. The two that change posture under default-on:

- **R5 (FSM determinism across a rolling upgrade) — discharged.** The
  probe code shipped on every node in production months ago. A node
  receiving a V2 `TxnMeta` with `prev_commit_ts != 0` is now uniformly
  capable of running the probe and producing the same apply outcome as
  every peer. The ordering constraint that gated this default flip is
  satisfied; the constraint stays documented so the same sequencing is
  reused for future probe extensions (e.g. extending dedup to a new
  command family).
- **R6 (Operator rollback) — preserved.** Setting
  `ELASTICKV_REDIS_ONEPHASE_DEDUP=0` reverts to the pre-flip behaviour.
  This is a single-env-var change; no binary rollback required. The
  rollback contract is documented in the godoc on
  `WithOnePhaseTxnDedup` and in the comment at the env-var read site.

The parent's R1–R4 (result reconstruction on dedup no-op, retry-window
window, MVCC visibility, store-side compaction) are unaffected by the
default flip and do not need re-evaluation; the same code paths now
just run in production by default.

## Test plan

- Unit: keep the existing `adapter/redis_*_dedup_test.go` suites green
  unchanged. Confirm that tests which currently rely on the gate being
  off either pass the explicit `WithOnePhaseTxnDedup(false)` option or
  `t.Setenv("ELASTICKV_REDIS_ONEPHASE_DEDUP", "0")` — audit and fix in
  the implementation commit.
- Lint: `golangci-lint run` clean.
- CI: the per-PR Jepsen run (`jepsen-test.yml`) and both scheduled
  stress workflows must stay green for the first run after merge.
- Manual operator-check: a smoke run of the local Jepsen script
  (`./scripts/run-jepsen-local.sh`) without any env tweaks, confirming
  the gate is now on by default (look for the
  `r.onePhaseTxnDedup == true` branch in the adapter's startup log
  line, or inspect via the `RedisServer.onePhaseTxnDedup` field through
  a probe metric — if no metric exists today, log the active gate at
  `NewRedisServer` start; this is an optional sub-task).

## Out of scope

- **DynamoDB default-on.** Parallel work tracked by
  [`2026_06_03_implemented_dynamodb_onephase_dedup.md`][dyn-doc]'s `M2`.
  The DynamoDB dedup-mode workflow has its own 7-day criterion that
  this proposal does not certify. A separate proposal will land the
  DynamoDB flip once its own M2 is met.
- **S3, SQS dedup paths.** Not yet routed through one-phase reuse; the
  parent design's option-2 mechanism does not cover them. Out of scope
  for this PR; revisit when those adapters add reuse paths.
- **Removing the env var.** Keep the opt-out env var indefinitely as a
  fast operator rollback. A future cleanup may remove it once
  operational confidence is high; not in this PR.

[dyn-doc]: 2026_06_03_implemented_dynamodb_onephase_dedup.md

## Rollout

One PR, two commits:

1. **Doc commit**: add this file. Reviewable on its own — no behavior
   change.
2. **Implementation commit**: flip the env-var sense in
   `adapter/redis.go`, update the comment and godoc, add the explicit
   `ELASTICKV_REDIS_ONEPHASE_DEDUP=0` to `jepsen-test-scheduled.yml`'s
   job env, fix any test that previously relied on the default-off
   posture (audit during the commit).

After merge: monitor the next 2–3 daily runs of both scheduled
workflows. The dedup-mode workflow must stay green; the control
workflow may or may not surface anomalies — both outcomes are
informative. After the post-flip soak period, the control workflow was
retired and the standard scheduled workflow now covers the dedup-on path.
Roll back via env var (no binary revert) if anything
unexpected appears.
