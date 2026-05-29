# elastickv TLA+ specs

Machine-checkable safety models for the four cross-cutting subsystems
(HLC, OCC, MVCC, route catalog) per
[`docs/design/2026_05_28_partial_tla_safety_spec.md`](../docs/design/2026_05_28_partial_tla_safety_spec.md).

This directory is **independent of the Go module**; you can navigate and
run it without touching anything under the Go source tree.

## Status

| Milestone | Scope | Status |
|---|---|---|
| M1 | `lib/Raft.tla`, `lib/Env.tla`, `hlc/HLC.tla`, `make tla-check` | Landed |
| M2 | `occ/OCC.tla` (safety invariants OCC-1..OCC-5) | Landed |
| M3 | `mvcc/MVCC.tla` (MVCC-1 + MVCC-4; MVCC-2 / MVCC-3 deferred to M5) | Landed |
| M4 | `routes/Routes.tla` (Routes-1 + Routes-4; Routes-2 / Routes-3 trivially-satisfied in M4 abstraction, full range modelling deferred to M5) | Landed |
| M5 | `composed/Composed.tla` + CI integration | Not started |
| M6 | Liveness checking (`tla-check-deep`) | Not started |

## Install

The only runtime dependency is a Java VM (TLC ships as `tla2tools.jar`).

```sh
# JDK 17+ is required. Verify with:
java -version
```

`make tla-check` automatically downloads `tla2tools.jar` to
`.cache/tla/tla2tools.jar` on first use and verifies its SHA-256 against
a pinned value defined in the top-level `Makefile`
(`TLA_VERSION`, `TLA_JAR`, `TLA_SHA256`). The file is gitignored.

To pre-download without running TLC:

```sh
make tla-tools
```

## Run

From the repository root:

```sh
make tla-check
```

This runs TLC against the safe + gap configuration pair for every
module declared in `scripts/tla-check.sh` (currently HLC, OCC, MVCC,
and Routes; M5 will be added as it lands), and prints whether the
outcome matches the design contract:

- **Safe config (`tla/<module>/MC<MODULE>.cfg`)** — the correct
  design, with each module's preconditions or commit guards enabled.
  TLC must finish with no invariant violation.
- **Gap config (`tla/<module>/MC<MODULE>_gap.cfg`)** — the
  preconditions or guards disabled (`EnableSafety = FALSE`). TLC must
  produce a counterexample on the module's load-bearing invariant
  (`HLC4_NoRegressionAcrossTerms` for HLC; `OCC1_CommitTsAboveStart`
  for OCC). The harness inverts the exit code for the gap run AND
  greps for the specific invariant-violation string, so any other
  failure mode (parse error, deadlock, JVM crash, different
  invariant) fails the job rather than silently being treated as the
  expected gap evidence.

To run a single module by hand (skipping the harness):

```sh
JAR=.cache/tla/tla2tools.jar

# HLC safe (expected PASS):
cd tla/hlc
java -XX:+UseParallelGC -cp ../../$JAR -DTLA-Library=../lib \
    tlc2.TLC -config MCHLC.cfg MCHLC.tla

# HLC gap (expected FAIL on HLC4_NoRegressionAcrossTerms):
java -XX:+UseParallelGC -cp ../../$JAR -DTLA-Library=../lib \
    tlc2.TLC -config MCHLC_gap.cfg MCHLC.tla
```

The OCC module follows the same pattern under `tla/occ/` with
`MCOCC.cfg` / `MCOCC_gap.cfg`; MVCC follows under `tla/mvcc/` (see
below).  Future M4 / M5 modules will use the same pattern.

State-space bounds are set in the `.cfg` files (M1 defaults: 3 nodes,
2 terms, ≤ 3 IssueTimestamp ops, wall clock ≤ 3 ms, `LogicalMax = 1`;
M2 defaults: 2 keys, 2 txns, `MaxTs = 6`, `MaxOps = 3`).  Raise them
locally to deepen exploration; the bounded configs keep `make
tla-check` end-to-end in well under a second.

## What each module proves

### `lib/Env.tla`
Shared constants (`Nodes`, `MaxClockSkewMs`, `HlcPhysicalWindowMs`,
`MaxTerms`, `MaxOps`, `MaxWallTime`, `LogicalMax`) and the single
normative `ASSUME MaxClockSkewMs < HlcPhysicalWindowMs` — HLC-4
precondition (i). All downstream modules `EXTENDS Env`.

### `lib/Raft.tla`
Abstract Raft interface modelled per NG4 of the design doc: log matching
and leader completeness are assumed (etcd/raft is a verified black box),
and only the `currentTerm` / `leaderOf` / `BecomeLeader` / `IsLeader`
operators that other modules actually need are exposed. `Propose`,
`Apply`, and `InstallSnapshot` are placeholders for later milestones.

### `hlc/HLC.tla`
The HLC layer.  Encodes all three HLC-4 preconditions:

- **(i) bounded skew** — inherited via the Env ASSUME.
- **(ii) logical-counter handoff** — `BecomeLeader_HLC` calls
  `Observe(maxAppliedHLC)` on the elected node when
  `EnableSafety = TRUE`. This is strategy (c) from the design doc.
- **(iii) ceiling fence** — `IssueTimestamp` has
  `wallNow[n] < physicalCeiling[n]` as an enabling condition when
  `EnableSafety = TRUE`.

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `HLC1_PerNodeMonotonic` | Per-node committed ts are strictly ordered by issuance `seq` |
| `HLC1_Action` (PROPERTY) | Transition form: `hlcLast[n]` weakly increases on every step |
| `HLC2_NonNeg` | Residual `physicalCeiling \in Nat` content; harmless type sanity |
| `HLC2_Action` (PROPERTY) | Transition form: `physicalCeiling[n]` weakly increases on every step |
| `HLC3_LeaderOnly` | Every committed ts was issued by the leader of its term |
| `HLC4_NoRegressionAcrossTerms` | Earlier-term commits are strictly less than later-term commits |

### `hlc/MCHLC.tla` + `MCHLC.cfg` / `MCHLC_gap.cfg`
TLC model-check instance.  Two configurations, one module.  See [Run](#run).

### `occ/OCC.tla`
The OCC layer.  Models the Percolator-style 2PC transaction lifecycle
`Idle → Active → Prepared → Committed / Aborted` and the lock map
`(key, lock_ts) → start_ts`.  The lock-resolver
(`kv/lock_resolver.go`) is abstracted into the atomic lock drain
inside `Commit`/`Abort` for M2; the async-resolver case is deferred
to M5 (composed) — see the block comment in `OCC.tla` where
`LockResolve` would have lived.  HLC is abstracted to a single global
monotonic counter for M2; M5 will INSTANCE `HLC.tla` for the real
48/16 layout.  The `EnableSafety` CONSTANT gates the OCC-1 commit
guard so the same module drives the safe and gap configurations.

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `OCC1_CommitTsAboveStart` | Every committed txn has `commit_ts > start_ts` |
| `OCC2_NoWriteWriteConflict` | Two committed txns sharing a write key have distinct commit_ts and one started after the other committed (`commit_ts[earlier] <= start_ts[later]`) |
| `OCC3_ReadSnapshotStability` | Every read observation equals `LatestVisible(k, start_ts(t))` (the snapshot value remains stable under monotonic-ts allocation in BeginTxn / Commit) |
| `OCC4_NoStrandedLockAtQuiescence` | When all txns are in a terminal state, no lock remains |
| `OCC5_StartTsConsistency` | Every read observation is bounded by the txn's `start_ts` (= `read_ts` by OCC-5) |
| `OCC5_Action` (PROPERTY) | Transition form: `start_ts[t]` is assigned once at `BeginTxn` and never updated |
| `CommitTsAssignedOnce` (PROPERTY) | Transition form: `commit_ts[t]` is assigned once at `Commit` and never updated |

### `occ/MCOCC.tla` + `MCOCC.cfg` / `MCOCC_gap.cfg`
TLC model-check instance for OCC.  Same one-module / two-config layout
as MCHLC.  The gap config disables the OCC-1 commit guard; TLC
produces an `OCC1_CommitTsAboveStart` counterexample at depth ≈ 5.

### `mvcc/MVCC.tla`
The MVCC layer.  Single-node model of a per-key version chain
(`versions[k]`) with a `Compact` action that drains older versions
to bound storage growth.  Carries a ghost variable
`originalVersions[k]` that records every version ever written and
is never pruned — MVCC-4 compares the two to assert that no
committed entry has been lost across compaction.  HLC is abstracted
to a single monotonic counter (`tsCounter`); M5 (composed) will
INSTANCE `HLC.tla` for the real 48/16 layout.  The `EnableSafety`
CONSTANT gates the per-key "retain the latest commit_ts below the
new minRetained" rule inside `Compact` — under the gap config that
guard is removed and TLC surfaces a `MVCC-4` counterexample where a
read at `read_ts ≥ minRetained` now misses a value the original log
would have shown.

MVCC-2 (no committed version below the HLC physical ceiling) and
MVCC-3 (cross-node read consistency) are deferred to M5:

- MVCC-2 is properly an HLC.tla property — the ceiling discipline
  lives there.  M5 will integrate the modules and check the cross-
  spec form.
- MVCC-3 requires a multi-node model that the single-node M3 spec
  cannot express.  M5 is the right place for it.

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `MVCC1_VisibleVersionUnique` | No two distinct version records in `versions[k]` share a `commit_ts` |
| `MVCC4_NoLostCommitOnSnapshotInstall` | For every key and every `read_ts ≥ minRetained`, the visible version in `versions[k]` equals the visible version in the ghost `originalVersions[k]` |
| `MVCC_TsMonotonic` (PROPERTY) | Transition form: `tsCounter` weakly increases on every step |
| `MVCC_GhostMonotonic` (PROPERTY) | Transition form: `originalVersions[k]` only grows; `Compact` leaves it `UNCHANGED` |

### `mvcc/MCMVCC.tla` + `MCMVCC.cfg` / `MCMVCC_gap.cfg`
TLC model-check instance for MVCC.  Same one-module / two-config
layout as MCHLC and MCOCC.  The gap config disables the retention
guard inside `Compact`; TLC produces a
`MVCC4_NoLostCommitOnSnapshotInstall` counterexample at depth ≈ 3
(one `Write`, one `Compact(newMin > commit_ts)`).

### `routes/Routes.tla`
The route catalog + CatalogWatcher layer.  Models the durable catalog
as a single monotonic `catalogVersion` counter and each node's
`RouteEngine` as a per-node `engineVersion[n]` that re-syncs via the
`CatalogWatcherSync(n)` action (mirroring
`distribution/watcher.go SyncOnce`).  A ghost variable
`engineMaxObserved[n]` records the highest version each node has
ever observed, so Routes-4 (watcher fan-out monotonicity) can be
stated as a state invariant
(`engineVersion[n] ≥ engineMaxObserved[n]`).  The `EnableSafety`
CONSTANT gates the monotonicity guard inside `CatalogWatcherSync`
— under the gap config that guard is removed and TLC surfaces a
Routes-4 counterexample where a node fetches an older snapshot
after a newer one.

Routes-2 (coverage and disjointness of ranges) and Routes-3
(SplitRange catalog atomicity) are trivially satisfied in this M4
abstraction:

- Routes-2 — M4 does not model individual ranges explicitly.  Full
  range modelling (a `routes : Keys → GroupId` function whose
  totality implies both properties) is deferred to M5 (composed),
  where SplitRange exercising key-boundary changes is in scope.
- Routes-3 — every catalog update is a single TLA+ action
  (`ProposeRouteChange`) that bumps `catalogVersion` and the
  routes in one step, so atomicity is structural.  Asynchronous
  cross-node propagation is captured by Routes-4.

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `Routes1_VersionInRange` | `catalogVersion ∈ 0..MaxVersions`; combined with `Routes1_Action` this captures Routes-1 monotonicity |
| `Routes2_CoverageDisjoint` | Vacuously TRUE in M4 (range partition not explicitly modelled; see comment in `Routes.tla`) |
| `Routes3_SplitAtomicity` | Vacuously TRUE in M4 (single-step `ProposeRouteChange`; see comment in `Routes.tla`) |
| `Routes4_NoEngineRegression` | For every node `n`, `engineVersion[n] ≥ engineMaxObserved[n]` |
| `Routes1_Action` (PROPERTY) | Transition form: `catalogVersion` weakly increases on every step |
| `Routes4_GhostMonotonic` (PROPERTY) | Transition form: `engineMaxObserved[n]` weakly increases on every step |

### `routes/MCRoutes.tla` + `MCRoutes.cfg` / `MCRoutes_gap.cfg`
TLC model-check instance for Routes.  Same one-module / two-config
layout as the other modules.  The gap config disables the monotonicity
guard inside `CatalogWatcherSync`; TLC produces a
`Routes4_NoEngineRegression` counterexample at depth ≈ 3 (one
`ProposeRouteChange (v → 1)`, one `CatalogWatcherSync(n)` fetching
`v = 1`, one `CatalogWatcherSync(n)` fetching `v = 0` — regression).

## How to interpret a TLC failure

When TLC finds a counterexample on an invariant it prints:

```
Error: Invariant <Name> is violated.
```

followed by a state trace (one TLA+ record per state). Each state shows
the values of every spec variable after the named action fired. For a
counterexample-of-HLC-4 you can read off which action sequence violated
strict-greater commits across terms — this is exactly the schedule the
follow-up Go code fix must rule out.

To map a state back to the implementation:

1. The `State N: <ActionName line X col Y of module M>` header names the
   spec action. Look it up in the corresponding `.tla` file.
2. Each action documents its corresponding implementation anchor in a
   comment at the action definition. Example: `BecomeLeader_HLC` in
   `HLC.tla` corresponds to `ShardedCoordinator.RunHLCLeaseRenewal`
   detecting a new term and calling `hlc.Observe(fsm.MaxAppliedHLC())`.
3. The state's variable values map to the corresponding Go fields:
   `hlcLast[n]` ↔ `kv/hlc.go HLC.last`, `physicalCeiling[n]` ↔
   `kv/hlc.go HLC.physicalCeiling`, etc.

If the failure is from `tla-check`'s gap-config branch announcing
"unexpectedly passed", the spec or the toggle wiring has drifted — the
preconditions are no longer doing what the design doc says they do.

## Reading list

For reviewers new to TLA+:

- Leslie Lamport, *Specifying Systems* (PDF, free) — the canonical
  book; chapters 1–6 are enough to read the modules here.
- Leslie Lamport's TLA+ video lecture series on YouTube.
- Diego Ongaro's Raft TLA+ specification (the abstraction in
  `lib/Raft.tla` is intentionally consistent with the style there).
- Hillel Wayne, *Practical TLA+* — friendlier introduction to the
  syntax and to TLC model checking.

For the elastickv-specific design:

- [`docs/design/2026_05_28_partial_tla_safety_spec.md`](../docs/design/2026_05_28_partial_tla_safety_spec.md)
  — the proposal that this directory implements.
- [`docs/architecture_overview.md`](../docs/architecture_overview.md)
  — high-level subsystem diagrams that the modules abstract.
- [`CLAUDE.md`](../CLAUDE.md) — coding conventions; the "HLC" section
  in particular describes the leader-only issuance invariant the spec
  encodes as HLC-3.
