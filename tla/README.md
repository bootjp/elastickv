# elastickv TLA+ specs

Machine-checkable safety models for the four cross-cutting subsystems
(HLC, OCC, MVCC, route catalog) per
[`docs/design/2026_05_28_implemented_tla_safety_spec.md`](../docs/design/2026_05_28_implemented_tla_safety_spec.md).

This directory is **independent of the Go module**; you can navigate and
run it without touching anything under the Go source tree.

## Status

| Milestone | Scope | Status |
|---|---|---|
| M1 | `lib/Raft.tla`, `lib/Env.tla`, `hlc/HLC.tla`, `make tla-check` | Landed |
| M2 | `occ/OCC.tla` (safety invariants OCC-1..OCC-5, OCC-8) | Landed |
| M3 | `mvcc/MVCC.tla` (MVCC-1 + MVCC-4; MVCC-2 / MVCC-3 deferred to M5) | Landed |
| M4 | `routes/Routes.tla` (Routes-1 + Routes-4; Routes-2 / Routes-3 trivially-satisfied in M4 abstraction, full range modelling deferred to M5) | Landed |
| M5 | `composed/Composed.tla` (Composed-1 + Composed-1a + Composed-3; Composed-2 not modelled) | Landed |
| M6 | Liveness checking — OCC-L1 (every Active txn eventually terminates), Routes-L1 (eventual fan-out). HLC / MVCC / Composed liveness deferred. | Partial |

The modules describe the code on `main` (reconciled 2026-10-28, see
§12 of the design doc).  Where the code falls short of an invariant,
an expected-failure config pins the gap (`*_legacy_*_gap.cfg`,
`*_g0_gap.cfg`, `*_g1_gap.cfg`, `*_collision_gap.cfg`); when the code
is fixed that run reports "unexpectedly passed" and the model and
config must be updated with the fix.

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
Routes, and Composed; M6 liveness extension is optional and may
follow), and prints whether the outcome matches the design contract:

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
The HLC layer, as `kv/hlc.go` and the timestamp allocators implement it:

- **(i) bounded skew** — inherited via the Env ASSUME.
- **(ii) logical-counter handoff** — there is no election-time
  `Observe`.  `IssuanceMode` selects the allocator:
  - `"tso"` — `RaftTSOAllocator` (`kv/tso_raft.go`): the leader
    Observes the Raft-committed allocation floor before reserving and
    commits the new floor before handing the window out (strategy (a)).
    `EnableSafety` gates this handoff.
  - `"legacy_single"` — the non-sharded `Coordinate`: leader-only, no
    handoff.
  - `"legacy_sharded"` — `ShardedCoordinator` in legacy TSO mode: any
    node stamps from its own HLC (audit G10), no handoff.
- **(iii) ceiling fence** — `IssueTimestamp` is `NextFenced`: no fence
  before the first ceiling; otherwise it fails closed when the wall
  clock reached the ceiling or the next value's physical half would
  exceed it.

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

### `hlc/MCHLC.tla` + configs
| Config | Mode | Expected |
|---|---|---|
| `MCHLC.cfg` | TSO with the floor handoff | PASS |
| `MCHLC_gap.cfg` | TSO without the floor handoff | FAIL on `HLC4_NoRegressionAcrossTerms` |
| `MCHLC_legacy_single_gap.cfg` | non-sharded `Coordinate` (known gap) | FAIL on `HLC4_NoRegressionAcrossTerms` |
| `MCHLC_legacy_sharded_gap.cfg` | `ShardedCoordinator` legacy mode (known gap G10) | FAIL on `HLC3_LeaderOnly` |

### `occ/OCC.tla`
The OCC layer at the FSM / store level.  Reading transactions start
at the applied commit watermark (`adapter/ts.go snapshotTS`; MaxUint64,
modelled as `MaxTs`, on an empty store);
write-only ones get a fresh `start_ts`.  Single-shard transactions
take the one-phase path: `AllocateOnePhase` picks `commit_ts`, and
`ApplyOnePhase` later applies it (in any order) only if
`commit_ts > start_ts`, `commit_ts` is above the watermark
(`checkCommitTSFence`), no foreign lock sits on a write key, and no
write or read key has a version newer than `start_ts`; otherwise
`RejectOnePhase`.  Multi-shard transactions settle `commit_ts` first,
then `Prepare` validates read and write keys and locks the write keys,
and `Commit` applies with no fence and no revalidation.  `RawWrite`
is a non-transactional write that raises the watermark (refused on a
locked key); `Prepare` raises the watermark to `start_ts`, as the lock
rows' apply does.  The lock
resolver is abstracted into the atomic lock drain inside
`Commit`/`Abort`; timestamps come from one global counter (unique,
leader-issued).  `EnableSafety` gates the commit-ts > start-ts
discipline; `EnableTwoPhase` turns the 2PC path off; `MultiGroup`
gives every key its own Raft group and watermark (the snapshot start is
the maximum over groups, the one-phase fence is per group).

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `OCC1_CommitTsAboveStart` | Every committed txn has `commit_ts > start_ts` |
| `OCC2_NoWriteWriteConflict` | Two committed txns sharing a write key have distinct commit_ts and one started after the other committed (`commit_ts[earlier] <= start_ts[later]`) |
| `OCC3_ReadSnapshotStability` | Every read observation of a live txn still equals `LatestVisible(k, start_ts(t))` — fails on `main` through 2PC (G0) |
| `OCC4_NoStrandedLockAtQuiescence` | When all txns are in a terminal state, no lock remains |
| `OCC5_StartTsConsistency` | Every read observation is bounded by the txn's `start_ts` (= `read_ts` by OCC-5) |
| `OCC3b_EmptyStoreReadStability` | OCC-3 for txns that started on an empty store (start_ts = MaxUint64) — fails on `main`: they read the latest version |
| `OCC8_NoWriteSkew` | No two committed txns have rw-antidependencies in both directions — fails on `main` through a one-phase commit in a 2PC PREPARE / COMMIT window (G1) |
| `OCC5_Action` (PROPERTY) | Transition form: `start_ts[t]` is assigned once at `BeginTxn` and never updated |
| `CommitTsAssignedOnce` (PROPERTY) | Transition form: `commit_ts[t]` is assigned once at `Commit` and never updated |
| `OCC_L1_TxnTerminates` (LIVENESS, M6) | `<>[](∀t. txnState[t] ∈ {Idle, Committed, Aborted})` — every txn eventually settles, driven by `WF_vars(Abort(t))` and fairness on settling a proposed one-phase entry in `SpecLive` |

### `occ/MCOCC.tla` + configs
| Config | Checks | Expected |
|---|---|---|
| `MCOCC.cfg` | full model; OCC-1, OCC-2, OCC-4, OCC-5 | PASS |
| `MCOCC_onephase.cfg` | single-shard path only, one Raft group; every invariant incl. OCC-3, OCC-8 | PASS |
| `MCOCC_gap.cfg` | `EnableSafety = FALSE` | FAIL on `OCC1_CommitTsAboveStart` (depth ≈ 5) |
| `MCOCC_g0_gap.cfg` | full model (known gap G0) | FAIL on `OCC3_ReadSnapshotStability` (depth 9: 2PC allocates, a raw write raises the watermark, a reader reads, the 2PC commits below its snapshot) |
| `MCOCC_g1_gap.cfg` | full model (known gap G1) | FAIL on `OCC8_NoWriteSkew` (a one-phase commit applies between the other txn's PREPARE and COMMIT) |
| `MCOCC_emptystore_gap.cfg` | single-shard path (known gap: an empty-store start is MaxUint64) | FAIL on `OCC3b_EmptyStoreReadStability` |
| `MCOCC_multigroup_gap.cfg` | single-shard path, one Raft group per key (known gap: G0 across groups) | FAIL on `OCC3_ReadSnapshotStability` (a one-phase write allocated earlier applies on a lagging group, above that group's watermark but below a reader's max-over-groups snapshot) |
| `MCOCC_live.cfg` | `SpecLive`, OCC-L1 | PASS |

### `mvcc/MVCC.tla`
The MVCC layer.  Single-node model of a per-key version chain
(`versions[k]`) and the store's resumable compaction:
`BeginCompact` persists the pending watermark (reads below it fail
closed), `PruneKey` deletes one key's garbage (idempotent, any
order), `FinishCompact` commits the watermark.  The prune keeps every
version with `commit_ts > minTS` plus the newest one `<= minTS`
(`shouldDeleteCompactionVersion`).  A ghost `originalVersions[k]`
is the uncompacted chain — MVCC-4 compares the two — and a ghost
`acked` records every acknowledged write.  `EnableSafety` gates the
"keep the newest version `<= minTS`" rule.  With `UniqueTs = FALSE`,
`CollidingWrite` writes at a timestamp the key already has, as
non-leader stamping produces (G10); the raw apply path replaces the
existing version.

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
| `MVCC4_NoLostCommitOnSnapshotInstall` | For every key and every `read_ts` at or above the effective watermark, the visible version in `versions[k]` equals the visible version in the ghost `originalVersions[k]` |
| `MVCC5_AckedWriteNotOverwritten` | Every acknowledged write is still in the uncompacted chain |
| `MVCC_TsMonotonic` (PROPERTY) | Transition form: `tsCounter` weakly increases on every step |
| `MVCC_GhostMonotonic` (PROPERTY) | Transition form: the uncompacted chain never loses a timestamp |
| `MVCC_WatermarkMonotonic` (PROPERTY) | Transition form: the effective watermark never decreases |

### `mvcc/MCMVCC.tla` + configs
| Config | Expected |
|---|---|
| `MCMVCC.cfg` | PASS |
| `MCMVCC_gap.cfg` (`EnableSafety = FALSE`) | FAIL on `MVCC4_NoLostCommitOnSnapshotInstall` (depth 4: `Write`, `BeginCompact`, `PruneKey`) |
| `MCMVCC_collision_gap.cfg` (`UniqueTs = FALSE`, known gap G10) | FAIL on `MVCC5_AckedWriteNotOverwritten` |

### `routes/Routes.tla`
The route catalog + CatalogWatcher layer.  Models the durable catalog
as a single monotonic `catalogVersion` counter and each node's
`RouteEngine` as a per-node `engineVersion[n]` that re-syncs via the
`CatalogWatcherSync(n)` action (mirroring
`distribution/watcher.go SyncOnce`, which applies a bounded batch of
deltas per tick and so can land on an intermediate version).  A ghost variable
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
| `Routes_L1_EventualFanOut` (LIVENESS, M6) | `<>[](∀n. engineVersion[n] = catalogVersion)` — every node's RouteEngine eventually catches up to the durable catalog, driven by `WF_vars(CatalogWatcherSyncLatest(n))` in `SpecLive` |

### `routes/MCRoutes.tla` + `MCRoutes.cfg` / `MCRoutes_gap.cfg`
TLC model-check instance for Routes.  Same one-module / two-config
layout as the other modules.  The gap config disables the monotonicity
guard inside `CatalogWatcherSync`; TLC produces a
`Routes4_NoEngineRegression` counterexample at depth ≈ 3 (one
`ProposeRouteChange (v → 1)`, one `CatalogWatcherSync(n)` fetching
`v = 1`, one `CatalogWatcherSync(n)` fetching `v = 0` — regression).

### `composed/Composed.tla`
The cross-module composition.  M1..M4 each modelled a single
subsystem in isolation; M5 captures the seams between them — the
two safety properties that no single-module spec can express:

- **Composed-1** — every committed write key of a pinned txn (one that
  carries `ObservedRouteVersion`) was owned by the committing group at
  the txn's observed catalog version.  Ties OCC's commit decision to
  the Routes catalog snapshot the txn read at `BeginTxn`.  Unpinned
  txns (read keys, caller-supplied `StartTS`, partition-resolver keys)
  skip `verifyComposed1`; the model keeps only the current-owner
  requirement for them.  `AutoPin` pins an unpinned txn at the
  Dispatch-time catalog version (`maybeAutoPinObservedRouteVersion`).
- **Composed-1a** — strictly stronger apply-time fence: every
  committed write key was owned by the committing group at the
  CURRENT catalog version observed by the FSM at apply time
  (`currentVersionAtCommit[t]`).  Composed-1 alone permits the
  codex-P1 trace (observed-check passes; `ProposeRouteChange` moves
  the key after `WriteIntent`; commit lands on the old owner;
  readers at the new version miss the write).  Composed-1a is the
  spec-level witness for the design doc §4.4 apply-time fence;
  surfaced as a separate gap config so it is exercised independently.
- **Composed-3** — every pair of distinct committed transactions has
  distinct `commit_ts` (the strict-serialisability bound).

Composed-2 (read-after-write across a route change) is not modelled.
Cross-group migration has shipped, so the clause is reachable in the
implementation, but this module carries no data plane to state it
against; `Composed2_ReadAcrossSplitRange` is `TRUE` only to keep the
name in the output.  `ProposeRouteChange` moves keys across groups —
the canonical Composed-1 counterexample is a key move from `g1` to
`g2` followed by a pinned transaction that observed the pre-move
catalog but commits via `g2`.

The module does NOT `INSTANCE` the M1..M4 modules — INSTANCEing all
four would explode the state space well past M5's "<10 min at
default bounds" target from §8.1.  Composed.tla instead recreates
the minimal cross-module state needed to express Composed-1 and
Composed-3 (abstract HLC clock, catalog history, transaction
lifecycle).  The integration claim is that this projection preserves
the invariants the full product would assert.

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `Composed1_CommitToOwningGroup` | For every committed pinned txn `t` and every `k ∈ writeSet[t]`, `routes[observedVer[t]][k] = commitGroup[t]` |
| `Composed1a_CommitToCurrentOwner` | For every committed txn `t` and every `k ∈ writeSet[t]`, `routes[currentVersionAtCommit[t]][k] = commitGroup[t]` — strictly stronger than Composed-1, witnesses the §4.4 apply-time fence |
| `Composed2_ReadAcrossSplitRange` | Not modelled (`TRUE`) — kept for naming consistency |
| `Composed3_DistinctCommitTs` | Distinct committed txns have distinct `commit_ts` |
| `Composed_CatalogMonotonic` (PROPERTY) | Transition form: `catalogVersion` weakly increases |
| `Composed_TsMonotonic` (PROPERTY) | Transition form: `tsCounter` weakly increases |
| `Composed3_TsAction` (PROPERTY) | Transition form: every Commit strictly raises `tsCounter` |

### `composed/MCComposed.tla` + `MCComposed.cfg` / `MCComposed_gap.cfg`
TLC model-check instance for Composed.  Same one-module / two-config
layout as the other modules.  The gap config disables the
owning-group check in `Commit`; TLC produces a
`Composed1_CommitToOwningGroup` counterexample at depth ≈ 4:
`BeginTxn(t, TRUE)` → `WriteIntent(t, k1)` → `ProposeRouteChange(k1, g2)`
→ `Commit(t, g2)` where the txn observed `routes[0][k1] = g1` but
commits via `g2`.

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
   comment at the action definition. Example: `IssueTimestamp` in
   `HLC.tla` corresponds to `HLC.NextFenced` in `kv/hlc.go`, and in TSO
   mode to `RaftTSOAllocator.reserveBatchAfterLocked` in
   `kv/tso_raft.go`.
3. The state's variable values map to the corresponding Go fields:
   `hlcLast[n]` ↔ `kv/hlc.go HLC.last`, `physicalCeiling[n]` ↔
   `kv/hlc.go HLC.physicalCeiling`, etc.

If the failure is from `tla-check`'s gap-config branch announcing
"unexpectedly passed", either the spec or the toggle wiring has
drifted, or — for a known-gap config — the implementation gap was
fixed: update the module to model the fix, move the config's invariant
into the passing config, and delete the gap config.

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

- [`docs/design/2026_05_28_implemented_tla_safety_spec.md`](../docs/design/2026_05_28_implemented_tla_safety_spec.md)
  — the proposal that this directory implements.
- [`docs/architecture_overview.md`](../docs/architecture_overview.md)
  — high-level subsystem diagrams that the modules abstract.
- [`CLAUDE.md`](../CLAUDE.md) — coding conventions; the "HLC" section
  in particular describes the leader-only issuance invariant the spec
  encodes as HLC-3.
