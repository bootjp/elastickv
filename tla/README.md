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

This runs TLC against the safe + gap configuration pair for every
module declared in `scripts/tla-check.sh` (currently HLC and OCC; M3 /
M4 / M5 will be added as they land), and prints whether the outcome
matches the design contract:

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
`MCOCC.cfg` / `MCOCC_gap.cfg`; future M3 / M4 / M5 modules will too.

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
