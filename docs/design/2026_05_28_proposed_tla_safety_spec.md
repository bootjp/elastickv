# TLA+ Safety Specification for elastickv

Status: Proposed
Author: bootjp
Date: 2026-05-28

---

## 1. Background and Motivation

elastickv's data-durability and consistency safety arise from the composition
of four subsystems — Raft, HLC (with the leader-issued / Raft-agreed physical
ceiling), OCC, and MVCC — plus the route catalog that governs which Raft
group owns which key. Each subsystem has unit-test and Jepsen coverage, but
the **cross-subsystem invariants** (e.g. "no committed write is ever
invisible to a read at a strictly later timestamp", "no two committed writes
to the same key share a commit ts") are only exercised by the random
interleavings that happen to occur in tests.

The bugs that are hardest to find with random testing are those that require
specific orderings across these subsystems:

- A new Raft leader issues an HLC timestamp inside the previous leader's
  lease window because the physical ceiling lagged a membership change.
- A route catalog `SplitRange` lands between an OCC read-set capture and the
  commit, so the commit goes to a different group than the read.
- A snapshot read at `ts = T` traverses a Raft snapshot install that elides
  a committed write whose `commit_ts < T`.

This document proposes a **TLA+ specification suite** that models the
elastickv concurrency primitives directly and uses TLC to model-check the
above invariants under all schedules within bounded state. The goal is to
find ordering bugs in the **design** before they manifest in code, and to
have a permanent regression artefact that re-runs on every change to the
relevant subsystems.

The proposal is part of a broader safety-verification plan whose other
strands (in-code invariant assertions, expanded Jepsen workloads, Perennial
crash-safety evaluation for the Pebble WAL) are tracked under separate
design documents.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- **G1** — A reproducible TLA+ specification of HLC, OCC, MVCC, and route
  catalog semantics, runnable via TLC on every PR that touches the
  corresponding Go subsystems.
- **G2** — Machine-checked **safety invariants** (Section 5) under all
  schedules within the bounded state space.
- **G3** — Each spec stays small enough that TLC completes in under 10
  minutes on a developer laptop at its default configuration; deeper
  configurations are opt-in (Section 8.2).
- **G4** — The specs are written so that each module can be model-checked in
  isolation (`HLC` alone, `OCC` alone, …) **and** composed into a combined
  spec (`Composed`) that wires them via a small environment module.

### 2.2 Non-Goals

- **NG1** — **TLAPS proofs**. TLC model checking within bounded state is the
  scope. Machine-checked inductive proofs against an unbounded model are a
  separate, later decision and are not promised by this proposal.
- **NG2** — **Refinement between the TLA+ spec and the Go implementation.**
  The spec models the design, not the code. Bugs in the implementation that
  do not exist in the design (e.g. an off-by-one in a Go loop) are caught by
  unit tests, property tests, and Jepsen — not by TLA+. We do not commit to
  a formal refinement proof.
- **NG3** — **Modelling all adapter protocols**. The Redis, DynamoDB, S3,
  and SQS adapters translate to the core key/value/transaction surface; we
  model the core surface. Adapter-specific bugs (e.g. SigV4, Redis RESP
  framing) are out of scope.
- **NG4** — **Modelling etcd/raft itself.** We treat Raft as a verified
  black box that delivers `(group_id, term, index) → committed entry` with
  the standard log-matching + leader-completeness guarantees. The spec
  models the **interface** to Raft (Propose, Apply, leader changes,
  snapshot install) but not its internal state machine.

---

## 3. Subsystems to Model

For each subsystem the spec captures only the state and transitions
relevant to the invariants in Section 5. Implementation files referenced
here are anchors, not constraints — the spec is the source of truth for the
design abstraction.

| Subsystem | Implementation anchors | Modelled state |
|---|---|---|
| Raft (abstracted) | `internal/raftengine/etcd`, `kv/fsm.go` | per-group log of committed entries + current leader + term; snapshot install marker |
| HLC | `kv/hlc.go` (`Next`, `SetPhysicalCeiling`, `Observe`) | per-node `last`, per-node `physicalCeiling`; the ceiling is itself a Raft-applied entry |
| OCC | `kv/transaction.go`, `kv/lock_resolver.go`, `kv/fsm_occ_test.go` | per-txn `read_ts`, `write_set`, `read_set`, lock map keyed by `(key, lock_ts)` |
| MVCC | `store/mvcc_store.go` | per-key version chain `[(commit_ts, value, tombstone?)]` |
| Route catalog | `distribution/engine.go`, `distribution/catalog.go`, `distribution/watcher.go` | catalog version, per-node cached snapshot, in-flight `SplitRange` |

Raft is modelled as a `Raft(group_id)` module with operators `Propose`,
`Apply`, `BecomeLeader`, `InstallSnapshot`, that maintain the
log-matching and leader-completeness properties as **assumed**
invariants (`ASSUME` in TLA+). The composed spec then plugs the elastickv
state machines into this interface.

---

## 4. Spec Architecture

### 4.1 Layout

```
tla/
  README.md                     -- how to run TLC, what each module proves
  lib/
    Raft.tla                    -- abstract Raft interface
    Env.tla                     -- nodes, keys, time bounds shared by all modules
  hlc/
    HLC.tla                     -- physical ceiling + leader-only issuance
    MCHLC.tla                   -- TLC model-check instance with constants
    MCHLC.cfg                   -- TLC config (invariants, state constraint)
  occ/
    OCC.tla
    MCOCC.tla / MCOCC.cfg
  mvcc/
    MVCC.tla
    MCMVCC.tla / MCMVCC.cfg
  routes/
    Routes.tla
    MCRoutes.tla / MCRoutes.cfg
  composed/
    Composed.tla                -- HLC + OCC + MVCC + Routes composed
    MCComposed.tla / MCComposed.cfg
```

Each module exposes:

- **CONSTANTS** — symbolic sets and numeric bounds (nodes, keys, values,
  `MaxOps`, `MaxClockSkewMs`).
- **VARIABLES** — only the state owned by that subsystem.
- **Init** / **Next** — the state machine.
- **TypeInvariant** — well-formedness.
- **SafetyInvariants** — Section 5.

A module **does not** import another module's variables directly. Instead,
`Composed.tla` instantiates each module with a shared environment from
`lib/Env.tla`.

### 4.2 Why per-module first, then composed

- Each module fits in a small state space and is fast to check in CI.
- A bug found in `MCHLC` is localised to HLC semantics, not blamed on OCC.
- The composed spec finds **interaction** bugs that the per-module specs
  cannot — e.g. an OCC commit at `ts` while a leader change shifts the HLC
  ceiling — but is run less frequently because it is expensive.

---

## 5. Invariants to Prove

Numbered for traceability. Each invariant is asserted as a TLA+ `INVARIANT`
in the module that owns the relevant state, plus `Composed` where it
crosses module boundaries.

### 5.1 HLC

- **HLC-1 — Per-node monotonicity.** For any node `n`, `HLC[n].last` is
  monotonically non-decreasing across `Next` and `Observe`.
- **HLC-2 — Ceiling monotonicity.** `HLC[n].physicalCeiling` is monotonic
  per node, and equal across all nodes that have applied the same set of
  ceiling entries.
- **HLC-3 — Leader-only issuance.** Every `Next` that records a timestamp
  for persistence (i.e. backs an OCC `commit_ts`) happens on a node that is
  currently the leader of the relevant group at the term in which the
  commit is proposed. (Followers may observe ts via `Observe` but must not
  issue persistence ts.)
- **HLC-4 — No regression across leader change.** If a new leader is
  elected at term `t' > t`, every `Next()` it issues returns a ts strictly
  greater than every ts committed in term `t`.

### 5.2 OCC

- **OCC-1 — Commit-ts above read-ts.** For every committed transaction `T`,
  `commit_ts(T) > read_ts(T)`.
- **OCC-2 — No write-write conflict.** Two committed transactions whose
  write sets intersect have disjoint commit timestamps and one must have
  serialised after the other (a strict order witnessing the conflict).
- **OCC-3 — Read snapshot stability.** A transaction `T` that reads key `k`
  at `read_ts(T)` sees the value of the unique committed write to `k` with
  the largest `commit_ts ≤ read_ts(T)` (or "not present" if none exists).
- **OCC-4 — Lock release.** Every lock `(k, lock_ts)` is eventually
  released (committed → version installed, or aborted → lock cleared);
  lock-resolver paths do not strand a lock.

### 5.3 MVCC

- **MVCC-1 — Visible version uniqueness.** For each `(key, read_ts)` pair,
  at most one version is visible.
- **MVCC-2 — No version above ceiling.** No version with `commit_ts > 0` is
  written below the current HLC physical ceiling of the issuing leader.
- **MVCC-3 — Snapshot read consistency.** Two `Get(k, ts)` calls at the
  same `ts` return the same value across all nodes that have applied the
  Raft log up to the entry that committed that value.
- **MVCC-4 — No lost commit on snapshot install.** After an
  `InstallSnapshot` on a follower, every `(key, commit_ts)` that was
  committed in the snapshot range remains readable at any `ts ≥ commit_ts`.

### 5.4 Route catalog

- **Routes-1 — Version monotonicity.** Catalog version is strictly
  monotonic; no node ever applies a snapshot with a non-increasing version.
- **Routes-2 — Coverage and disjointness.** At every catalog version, the
  ranges form a partition of the keyspace (covering, non-overlapping).
- **Routes-3 — SplitRange atomicity.** `SplitRange` either fully applies
  (the parent range is replaced by exactly the two child ranges in the
  catalog and in every node's `RouteEngine`) or has no observable effect.
- **Routes-4 — Watcher fan-out monotonicity.** No node's `RouteEngine`
  observes catalog version `v2` before `v1` if `v1 < v2`.

### 5.5 Cross-subsystem (composed)

- **Composed-1 — Commit goes to the owning group.** For every committed
  write to key `k` at the catalog version visible to the transaction, the
  Raft group that accepts the commit is the one that owns `k` at that
  version.
- **Composed-2 — Read-after-write across SplitRange.** A successful
  commit to `k` at version `v` is readable at any later `read_ts` even if
  the catalog has advanced past `v` and `k` has moved to a different group
  (assumes the post-split target has loaded the data — this is the
  invariant that captures correctness of the data hand-off, not just the
  catalog hand-off).
- **Composed-3 — Strict serialisability bound.** The set of committed
  transactions is consistent with a serial order on `commit_ts` that
  respects real-time happens-before. (This is what Jepsen / `elle` would
  check; modelling it here lets us prove it under all schedules in the
  bounded space.)

---

## 6. Modelling Decisions

### 6.1 What to abstract

- **Network and time** — modelled as non-deterministic action choices, not
  as continuous quantities. "Clock skew" is bounded by `MaxClockSkewMs`,
  used by HLC actions when reading wall time.
- **Raft internals** — abstracted to a per-group log and a leader/term
  tuple; transitions are `Propose(entry)`, `Apply(entry)`, `BecomeLeader`,
  `InstallSnapshot`. Term increases monotonically; only the current leader
  can `Propose`; `Apply` is in log order.
- **Pebble / disk** — modelled as durable iff the Raft entry that produced
  the write has been `Apply`-ed. The Perennial-style crash semantics
  (uncommitted Pebble batches, fsync ordering) are out of scope for this
  spec.
- **Adapters** — modelled as direct callers of the OCC interface; protocol
  framing is irrelevant.

### 6.2 What to keep concrete

- **HLC bit layout** — the 48/16 split is modelled as a record
  `[wall_ms, logical]` with explicit overflow when `logical` reaches its
  bound. The invariants in Section 5.1 depend on the split being respected.
- **OCC read-set / write-set / commit-ts ordering** — kept concrete because
  this is where most of the interesting invariants live.
- **Route catalog version** — kept concrete because Routes-1 / Routes-4
  hinge on it.

### 6.3 Symmetry and state reduction

- Use `SYMMETRY` over the node set and the key set in TLC config.
- Use a `StateConstraint` that bounds the number of operations and the
  largest term per group.
- For invariants that are checkable in a small state (e.g. HLC-1), bound to
  3 nodes / 2 keys / 2 terms. For larger composed checks, raise bounds
  selectively.

---

## 7. Repository Layout and Tooling

### 7.1 Files

The specs live in a top-level `tla/` directory (Section 4.1) so they can
be navigated and run without entering the Go module. `tla/README.md`
documents how to install TLC (TLA+ Toolbox or `tla2tools.jar`) and the
exact commands to run each module.

### 7.2 Makefile targets

Two new targets, kept opt-in so the default `make test` is unchanged:

```make
tla-check:        ## run TLC on all modules with default bounds
tla-check-deep:   ## run TLC on Composed with extended bounds (~hours)
```

Both targets shell out to `tla2tools.jar` which is downloaded on first use
into `.cache/tla/` (matching the existing `.cache/` pattern from CLAUDE.md).
The downloaded jar is checksum-pinned. No system-wide install required.

### 7.3 CI integration (deferred)

`tla-check` is intended to run in CI on PRs that touch:

- `kv/hlc*.go`, `kv/transaction.go`, `kv/fsm.go`, `kv/lock_resolver.go`
- `store/mvcc_store.go`
- `distribution/`
- the spec files themselves

The exact CI wiring (workflow file, path filter) is deferred to the M1 PR
so it can be reviewed alongside a concrete spec rather than in the
abstract.

---

## 8. Milestones

Each milestone is a separate PR. Milestones land in order; the proposal is
**not** all-or-nothing — value accrues incrementally (every milestone
proves something on its own).

### 8.1 Milestone list

| ID | Scope | Output | Notes |
|---|---|---|---|
| **M1** | `lib/Raft.tla`, `lib/Env.tla`, `hlc/HLC.tla` with invariants HLC-1..HLC-4. `Makefile` `tla-check` target. `tla/README.md`. | Per-module TLA+ spec of HLC; TLC passes at default bounds (3 nodes, 2 terms, ≤20 ops). | Smallest viable PR; sets the directory layout precedent. |
| **M2** | `occ/OCC.tla` with invariants OCC-1..OCC-4. | OCC spec runnable in isolation. | Re-uses `lib/Raft.tla`; introduces lock map and read/write sets. |
| **M3** | `mvcc/MVCC.tla` with invariants MVCC-1..MVCC-4. | MVCC spec runnable in isolation. | Includes a small `InstallSnapshot` action to exercise MVCC-4. |
| **M4** | `routes/Routes.tla` with invariants Routes-1..Routes-4. | Route catalog spec runnable in isolation. | Models `SplitRange` and watcher polling. |
| **M5** | `composed/Composed.tla` with invariants Composed-1..Composed-3. CI integration. | Cross-subsystem spec; TLC at default bounds in <10 min. | Where interaction bugs surface. |
| **M6** *(optional)* | `tla-check-deep` configuration: larger bounds for `Composed`. | Deeper coverage runnable on a beefier machine / off-CI. | Out of scope of the initial proposal — included only as a placeholder for the follow-up decision. |

### 8.2 Doc lifecycle

This doc is `proposed` at M1 kickoff. It is renamed to `partial` when M1
lands (per CLAUDE.md's `*_partial_*.md` convention, since the proposal
spans multiple milestones). It is renamed to `implemented` when M5 lands.

---

## 9. Risks and Open Questions

1. **Abstraction faithfulness.** A TLA+ spec is only as useful as its
   correspondence to the implementation. We **explicitly do not promise
   refinement** (NG2), so a spec bug-free run does not prove the Go code
   correct. The mitigation is reviewer discipline: every spec change is
   reviewed against the implementation anchors in Section 3.

2. **State explosion in `Composed`.** With four subsystems and a non-trivial
   Raft abstraction, the composed state space can blow up. Mitigations:
   aggressive `SYMMETRY`, a `StateConstraint` that bounds operations and
   term counts, and the option to disable individual subsystems in
   `MCComposed.cfg` for targeted runs.

3. **TLA+ skill ramp.** TLA+ is not part of the day-to-day toolchain. M1 is
   deliberately small to give reviewers a concrete artefact to learn
   against. The `tla/README.md` will include a short reading list.

4. **Spec rot.** Specs that don't run break silently. `tla-check` in CI
   (deferred to M1, Section 7.3) is the primary mitigation. As a secondary
   mitigation, the doc lifecycle (Section 8.2) ties promotion to specs
   landing, so a stale `partial` status is a visible signal.

5. **Choice of TSO model.** The HLC spec models the current
   per-shard-leader ceiling. The centralized TSO proposal
   (`2026_04_16_proposed_centralized_tso.md`) would change that. The two
   docs are independent; if/when centralized TSO lands, `HLC.tla` (or a
   sibling `TSO.tla`) is updated as part of that PR. We do **not** block
   this proposal on the TSO decision.

6. **TLC tool licensing and binary distribution.** `tla2tools.jar` is
   MIT-licensed but not in any package manager we already depend on.
   Downloading it on first use is the simplest path; M1 will pin a
   specific version and checksum.

---

## 10. Out of Scope (Cross-Reference)

- **In-code invariant assertions** — covered by a separate proposal
  (planned, not yet filed); complementary to but distinct from the TLA+
  specs here.
- **Jepsen workload / nemesis expansion** — covered by a separate
  proposal (planned, not yet filed); the `clock_skew` nemesis there is
  the empirical analogue of HLC-4.
- **Perennial crash-safety evaluation for Pebble WAL** — covered by a
  separate, research-tier proposal (planned, not yet filed); orthogonal
  to this spec, which treats disk durability as "Apply implies durable".
- **Refinement from TLA+ to Go** — see NG2. Out of scope.

---

## 11. References

- `kv/hlc.go` — current HLC implementation and physical ceiling layout.
- `kv/transaction.go`, `kv/lock_resolver.go`, `kv/fsm_occ_test.go` — OCC
  surface and lock resolution.
- `store/mvcc_store.go` — MVCC over Pebble.
- `distribution/engine.go`, `distribution/catalog.go`,
  `distribution/watcher.go` — route catalog.
- `docs/architecture_overview.md` — system-level diagrams.
- `docs/design/2026_04_16_proposed_centralized_tso.md` — the TSO proposal
  that this spec is independent of (Section 9, risk 5).
- Diego Ongaro's Raft TLA+ specification — reference for the abstract
  `Raft.tla` interface.
- CockroachDB and TiDB MVCC / HLC TLA+ models — public prior art for
  Section 5.1–5.3 invariants.
