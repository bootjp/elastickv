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
| M3 | `mvcc/MVCC.tla` (MVCC-1..MVCC-4) | Not started |
| M4 | `routes/Routes.tla` (Routes-1..Routes-4) | Not started |
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

This runs TLC against both configurations and prints whether the outcome
matches the design contract:

1. `tla/hlc/MCHLC.cfg` — the **correct design**, with HLC-4
   preconditions encoded as ASSUMEs / action guards. TLC must finish
   with no invariant violation.
2. `tla/hlc/MCHLC_gap.cfg` — the **gap configuration**, with the
   preconditions disabled (`EnableSafety = FALSE`). TLC must produce a
   counterexample on `HLC4_NoRegressionAcrossTerms` — this is the
   motivating evidence that strategy (c) handoff + the ceiling fence
   are necessary. The `tla-check` target inverts the exit code for
   this run so a TLC FAILURE here counts as PASS for CI.

To run a single module by hand (skipping the Makefile):

```sh
JAR=.cache/tla/tla2tools.jar

# Safe config (expected PASS):
cd tla/hlc
java -XX:+UseParallelGC -cp ../../$JAR -DTLA-Library=../lib \
    tlc2.TLC -config MCHLC.cfg MCHLC.tla

# Gap config (expected FAIL on HLC-4):
java -XX:+UseParallelGC -cp ../../$JAR -DTLA-Library=../lib \
    tlc2.TLC -config MCHLC_gap.cfg MCHLC.tla
```

State-space bounds are set in the `.cfg` files (M1 defaults: 3 nodes,
2 terms, ≤ 3 IssueTimestamp ops, wall clock ≤ 3 ms, `LogicalMax = 1`).
Raise them locally to deepen exploration; the M1 PR keeps them small so
the full `make tla-check` runs in well under a second.

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
`Idle → Active → Prepared → Committed / Aborted`, the lock map
`(key, lock_ts) → start_ts`, and the `LockResolve` action that turns
abandoned locks into versions or clears them.  HLC is abstracted to a
single global monotonic counter for M2; M5 (composed) will INSTANCE
`HLC.tla` for the real 48/16 layout.  The `EnableSafety` CONSTANT
gates the OCC-1 commit guard so the same module drives the safe and
gap configurations.

Invariants asserted:

| Invariant | Statement |
|---|---|
| `TypeOK` | Variable types are well-formed |
| `OCC1_CommitTsAboveStart` | Every committed txn has `commit_ts > start_ts` |
| `OCC2_NoWriteWriteConflict` | Two committed txns sharing a write key have distinct commit_ts and one started after the other committed (`commit_ts[earlier] <= start_ts[later]`) |
| `OCC3_ReadSnapshotStability` | Every read observation's `commit_ts <= start_ts` of the reader (lock-encoded reads only) |
| `OCC4_NoStrandedLockAtQuiescence` | When all txns are in a terminal state, no lock remains |
| `OCC5_StartTsConsistency` | Every read observation is bounded by the txn's `start_ts` (= `read_ts` by OCC-5) |
| `OCC5_Action` (PROPERTY) | Transition form: `start_ts[t]` is assigned once at `BeginTxn` and never updated |
| `CommitTsAssignedOnce` (PROPERTY) | Transition form: `commit_ts[t]` is assigned once at `Commit` and never updated |

### `occ/MCOCC.tla` + `MCOCC.cfg` / `MCOCC_gap.cfg`
TLC model-check instance for OCC.  Same one-module / two-config layout
as MCHLC.  The gap config disables the OCC-1 commit guard; TLC
produces an `OCC1_CommitTsAboveStart` counterexample at depth ≈ 5.

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
