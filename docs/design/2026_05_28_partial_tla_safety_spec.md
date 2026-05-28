# TLA+ Safety Specification for elastickv

Status: Partial
Author: bootjp
Date: 2026-05-28

> **Implementation status.** M1 landed in PR #TBD (the PR opening this
> document's rename to `partial`). The HLC TLA+ module + `make tla-check`
> + the gap-config counterexample evidence are now part of the build.
> M2–M5 (OCC, MVCC, Routes, Composed) remain open per §8. The follow-up
> Go code changes that strategy (c) and the ceiling fence require (per
> §5.1 HLC-4 (ii) and (iii)) are tracked as separate issues against
> `kv/hlc.go`, `kv/sharded_coordinator.go`, and `kv/fsm.go`.

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
| HLC | `kv/hlc.go` (`Next`, `SetPhysicalCeiling`, `Observe`); `kv/coordinator.go` and `kv/sharded_coordinator.go` (`hlcRenewalInterval`, `hlcPhysicalWindowMs`, `RunHLCLeaseRenewal`) | per-node `last` (48-bit physical + 16-bit in-memory logical), per-node `physicalCeiling` (Raft-applied), per-leader `term` |
| OCC | `kv/transaction.go`, `kv/lock_resolver.go`, `kv/fsm_occ_test.go` | per-txn `start_ts` (= `read_ts`), `write_set`, `read_set`, lock map `(key, lock_ts) → start_ts` (the value records the owning transaction's `start_ts`, which corresponds to `txnLock.StartTS` in `kv/txn_codec.go` and equals `read_ts(T)` by OCC-5, so `owner(k, ts)` is derivable for OCC-L1) |
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

```text
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
- **SafetyInvariants** — Section 5.1–5.5.
- **LivenessProperties** — Section 5.6, checked with TLC `PROPERTY` under
  the relevant fairness assumptions (separately from the `INVARIANT` set).

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

Numbered for traceability. Sections 5.1–5.5 are **safety** properties,
asserted as TLA+ `INVARIANT` in the module that owns the relevant state
(plus `Composed` where they cross module boundaries). Section 5.6 lists the
**liveness** properties separately — these are temporal (`<>`, `[]<>`) and
are asserted as `PROPERTY` with explicit fairness assumptions, since TLC
cannot check them as state invariants.

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
  greater than every ts committed in term `t`. **Preconditions** that the
  spec asserts as `ASSUME`s and that the implementation must satisfy:
  - **(i) Bounded clock skew vs ceiling window.** `MaxClockSkewMs <
    hlcPhysicalWindowMs`. The hazard prevented by this bound is *not*
    a wall clock running ahead of the ceiling (that case uses wall
    time and is safe); the hazard is logical-counter overflow at the
    ceiling boundary: the old leader's logical half can wrap inside a
    ceiling window, bumping the issued wall part to `ceiling + 1` and
    committing `(ceiling + 1, 0)`. If a new leader's wall clock can
    independently reach `ceiling + 1` before a fresh ceiling is
    renewed, the new leader's first `Next()` can tie or undercut the
    old leader's last commit. Bounding inter-node skew to less than
    one ceiling window (< 3s with the current `hlcPhysicalWindowMs =
    3000ms`) keeps the window wide enough that the new leader cannot
    independently reach the overflow value before a renewal applies.
    Should be surfaced in operator docs as a cluster prerequisite.
  - **(ii) Logical-counter handoff.** The 16-bit logical half of the HLC
    is in-memory only (`kv/hlc.go` `h.last` is `atomic.Uint64`, not
    replicated), so an old leader can have committed `(ceiling_ms, k)`
    with `k ≥ 1` while a freshly elected leader that has only restored
    the same `ceiling_ms` from the FSM will issue `(ceiling_ms, 0)` —
    strictly less. For HLC-4 to hold the design must do **one** of:
    (a) carry the max committed logical alongside the ceiling in each
    Raft lease entry and restore both on apply; (b) advance the ceiling
    by at least one millisecond on every leader transition so a new
    leader's first `Next()` floors to `(ceiling+1, 0)` — **with a
    serving fence**: the new leader must refuse to serve persistence
    `Next()` calls until the election-triggered ceiling proposal has
    been applied, otherwise a client request landing between
    `BecomeLeader` and the first ceiling apply can still issue from
    the stale ceiling; or (c) on election, scan the FSM for the max
    committed HLC and call `hlc.Observe(maxHLC)` before serving any
    `Next()`, which advances `h.last` past every committed ts from the
    previous term without requiring a protocol change to the
    ceiling-entry format. **Default for M1: strategy (c).** It is the
    simplest to model in TLA+ (a single `BecomeLeader` action that
    calls `Observe(max_ts)`) and the smallest implementation diff
    (`ShardedCoordinator` calls `hlc.Observe(fsm.MaxAppliedHLC())` on
    detecting a new term, gated before the first persistence
    `Next()`). **Implementation note:** `fsm.MaxAppliedHLC()` does
    not exist today — `kv/fsm.go` tracks only the physical ceiling
    via `applyHLCLease` → `SetPhysicalCeiling`. Strategy (c) requires
    adding `MaxAppliedHLC()` to the FSM, which can either maintain a
    running max across applied write entries (cheap, preferred) or
    scan the MVCC store on election (expensive at production key
    counts, fallback only). "Simplest" here means "smallest API
    surface" — one new FSM method — not "trivial one-liner".
    Strategy (a) requires a Raft entry format change; (b) requires
    the serving fence above. The M1 spec encodes one of
    these as a normative requirement; absent a strong reason to
    deviate, M1 should adopt (c). If the current `kv/hlc.go` does not
    implement any of (a)/(b)/(c), the TLC run for HLC-4 is expected
    to surface a counterexample — surfacing that counterexample is
    one of the motivating reasons for this spec.
  - **(iii) Commit-time ceiling fencing.** Bounded skew (i) and logical
    handoff (ii) are not enough on their own: a leader that misses
    `RunHLCLeaseRenewal` for longer than `hlcPhysicalWindowMs` (e.g.
    partitioned from the quorum that owns the ceiling group — in the
    `ShardedCoordinator` the ceiling group is always
    `c.defaultGroup`, per `kv/sharded_coordinator.go`, so the failure
    scenario is: a node that is leader of a non-default shard but
    partitioned from the default group's quorum) but remains a Raft
    leader will continue advancing its wall clock past the last
    committed ceiling and can still issue persistence timestamps. A
    subsequent leader that has only restored the same stale ceiling
    and has a lagging wall clock can then issue a strictly smaller
    timestamp, violating HLC-4. The spec therefore requires every
    `Next()` that backs a persistence ts to be gated on
    `wall_now < physicalCeiling[n]` (the same `physicalCeiling[n]`
    variable as §3, matching `kv/hlc.go`'s field); a leader whose
    ceiling has expired must either re-apply a fresh ceiling via
    Raft before committing, or refuse to commit (fail-closed). The
    current implementation in `kv/sharded_coordinator.go`
    `RunHLCLeaseRenewal` only logs renewal failures and dispatch
    paths call `HLC.Next()` without checking ceiling freshness — this
    is the second design gap the spec is meant to surface. As with
    (ii), if the gap is real, TLC will produce a counterexample for
    HLC-4 under a model that lets renewals fail. **Availability
    note.** The fail-closed behaviour means a leader partitioned
    from the default group's quorum for longer than
    `hlcPhysicalWindowMs` cannot serve any persistence timestamp —
    every client commit is rejected until renewal succeeds. This is
    a CP, not AP, trade-off and operators must size
    `hlcPhysicalWindowMs` (currently 3s) relative to expected
    partition duration; see §9 risk 7.

### 5.2 OCC

- **OCC-1 — Commit-ts above read-ts.** For every committed transaction `T`,
  `commit_ts(T) > read_ts(T)`.
- **OCC-2 — No write-write conflict.** Two committed transactions whose
  write sets intersect have disjoint commit timestamps and one must have
  serialised after the other (a strict order witnessing the conflict).
- **OCC-3 — Read snapshot stability.** A transaction `T` that reads key `k`
  at `read_ts(T)` sees the value of the unique committed write to `k` with
  the largest `commit_ts ≤ read_ts(T)` (or "not present" if none exists).
  **Lock encoding.** OCC-3 is asserted only in states where no uncommitted
  lock for key `k` at any `lock_ts ≤ read_ts(T)` exists. When a reader
  encounters a conflicting lock, the spec models a `LockResolver` action
  (corresponding to `kv/lock_resolver.go`) that resolves the lock —
  committing it (the lock becomes a versioned write) or aborting it (the
  lock is cleared) — before the read proceeds. This avoids the spurious
  alternative that a reader sees stale data because a lock is silently
  ignored.
- **OCC-4 — No stranded lock at quiescence.** In any state where the
  lock-resolver and all transactions of a given `start_ts` are no longer
  scheduled (no enabled actions for that `start_ts`), no lock for that
  `start_ts` remains. This is a bounded safety form of "locks are
  eventually resolved"; the unbounded eventual-release statement is
  restated as a liveness property in Section 5.6 (OCC-L1).
- **OCC-5 — Start-ts consistency.** All read and write operations within a
  single transaction `T` share the same `start_ts(T)`, which equals
  `read_ts(T)`. In particular, for read-modify-write transactions, the
  write half re-uses the read half's timestamp; there is no per-operation
  re-read, so the transaction observes a single MVCC snapshot end-to-end.

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
- **Routes-3 — SplitRange catalog atomicity.** `SplitRange` either fully
  applies **in the catalog** (the parent range is atomically replaced by
  exactly the two child ranges, with a single catalog-version bump) or has
  no observable effect on the catalog. The handling node also updates its
  own `RouteEngine` synchronously with the catalog write
  (`adapter/distribution_server.go` `saveSplitResultViaCoordinator` +
  `applyEngineSnapshot`); propagation to **other** nodes' `RouteEngine`
  instances is asynchronous via `CatalogWatcher` polling
  (`distribution/watcher.go`) and is covered by Routes-4 rather than
  required to be simultaneous. The spec must not require cross-node atomic
  engine updates — doing so would exclude the stale-cache schedules this
  proposal is meant to analyse around SplitRange × OCC interactions.
- **Routes-4 — Watcher fan-out monotonicity.** No node's `RouteEngine`
  observes catalog version `v2` before `v1` if `v1 < v2`. Combined with
  Routes-1 (durable catalog monotonicity) this means each node's view of
  the catalog is a monotonically advancing prefix of the true catalog
  history — never a re-ordering, only a lag.

### 5.5 Cross-subsystem (composed)

- **Composed-1 — Commit goes to the owning group.** For every committed
  write to key `k` at the catalog version visible to the transaction, the
  Raft group that accepts the commit is the one that owns `k` at that
  version.
- **Composed-2 — Read-after-write across SplitRange.** A successful commit
  to `k` at version `v` is readable at any later `read_ts` at the same
  logical key, even after the catalog has advanced past `v`. Under the
  current `SplitRange` implementation, which is **same-group only** (per
  CLAUDE.md and `distribution/`), the split only changes range boundaries
  inside a single Raft group; `k` never crosses a group boundary, so the
  data hand-off is trivially correct because no data movement occurs. The
  invariant is therefore deliberately stated in terms that *would* also
  cover a future cross-group migration path: when (and only when)
  cross-group split lands, the post-split target must have replayed the
  pre-split data before the catalog hand-off becomes observable. Until
  then, Composed-2 is exercised only against the same-group case, and
  the cross-group clause is a forward-looking guard rail.
- **Composed-3 — Strict serialisability bound.** The set of committed
  transactions is consistent with a serial order on `commit_ts` that
  respects real-time happens-before. (This is what Jepsen / `elle` would
  check; modelling it here lets us prove it under all schedules in the
  bounded space.) **Real-time encoding (M5 modelling decision).** TLA+
  is a state-machine model with no inherent real-time, so the composed
  spec must encode "happens-before" explicitly. M5 adopts the
  **Raft-log-index proxy** with a clear scope: within a single Raft
  group, an operation whose committing entry has a strictly smaller
  log index than another's happens-before the other (same-group log
  index is a total order). **Across groups** the lexicographic
  `(group_id, index)` comparison is *not* a valid happens-before
  relation — it would make all of group 1's writes happen-before
  every group 2 write regardless of actual timing — so cross-group
  operations are treated as unordered by the proxy, and their
  serialisation is established via `commit_ts` order from HLC (which
  is the global clock all groups share). This combination —
  same-group: log index; cross-group: HLC commit_ts — is the spec's
  primary modelling choice for Composed-3. **Cross-group `commit_ts`
  collisions.** The HLC is *not* a single global counter: the
  physical ceiling is Raft-replicated and converges across nodes, but
  the 16-bit logical half is per-node in-memory (`kv/hlc.go`
  `h.last`), so two leaders of different groups on different nodes
  can independently produce identical 64-bit timestamps. Composed-3
  therefore requires either: (i) **distinct-ts model constraint** —
  the M5 model enforces globally distinct `commit_ts` values via a
  `DISTINCT_TS` `StateConstraint`, which is sound under
  `MaxClockSkewMs < hlcPhysicalWindowMs` (the renewal cadence makes
  natural collisions vanishingly rare); or (ii) **explicit
  tiebreaker** — equal-`commit_ts` cross-group operations are
  ordered by the producing leader's `(node_id, group_id)` lex pair,
  which is a fixed total order per node-pair within a single
  leadership term. M5 commits to **(i)** as the default
  (`MCComposed.cfg` ships with `DISTINCT_TS`); the tiebreaker (ii)
  is the fallback if removing `DISTINCT_TS` produces interesting
  counterexamples worth analysing. (Soundness note for (i): a
  `StateConstraint` prunes the reachable state space TLC explores;
  it does not by itself prove the invariant in pruned states.
  Composed-3 is therefore proved against the realistic regime
  where the HLC preconditions make natural collisions vanishingly
  rare; the pathological collision schedules are documented by (ii)
  so the choice of pruning is intentional, not a soundness gap.) If the M5 author finds the proxy
  itself too lossy (e.g. it makes Composed-3 vacuously true even
  with distinct ts), the further fallback is an auxiliary
  `realtime_order` history variable in `lib/Env.tla` that tracks
  `(start_time, end_time)` per operation; this is documented in the
  M5 PR rather than pre-committed here.

### 5.6 Liveness properties

Liveness properties cannot be expressed as state invariants; they require
TLA+ temporal operators (`<>`, `[]<>`) and fairness assumptions on the
relevant actions. They are checked via TLC `PROPERTY` (not `INVARIANT`)
under the fairness conditions stated alongside each property. Each
liveness property requires the bounded model to be checked with TLC's
liveness mode, which is significantly more expensive than safety checking;
expect these to be opt-in (Section 8.2 `tla-check-deep`).

- **OCC-L1 — Eventual lock release.** Every lock `(k, lock_ts)` whose
  owning transaction `T` is in `LiveTransactions` is eventually
  released — committed (→ versioned write installed) or aborted
  (→ lock cleared). Stated as
  `[](lockHeld(k, ts) ∧ owner(k, ts) ∈ LiveTransactions ⇒ <> ¬lockHeld(k, ts))`
  under weak fairness on the lock-resolver action and on the owning
  transaction's `Commit` / `Abort` transitions. The
  `LiveTransactions` qualifier is load-bearing: a transaction that
  crashes mid-flight (never reaches `Commit` or `Abort`) never
  releases its lock through the owner-driven path, so the universal
  would be vacuously falsifiable; the lock-resolver action is then
  the only path that can release the lock and its progress is
  guaranteed by weak fairness only for resolvers that are themselves
  in `LiveNodes`. Without these qualifiers a model can park forever
  in a state with an outstanding lock; TLC's liveness checker
  reports such a counterexample. The bounded safety counterpart is
  OCC-4 (no stranded lock at quiescence).

- **Routes-L1 — Eventual catalog convergence.** For every catalog version
  `v` that is committed durably, every **live** node's `RouteEngine`
  eventually observes version `≥ v`. Stated as
  `<>(∀ n ∈ LiveNodes: catalogVersion[n] ≥ v)` under weak fairness on
  the `CatalogWatcher.SyncOnce` action. The `LiveNodes` qualifier is
  load-bearing: a permanently-crashed node never resumes
  `CatalogWatcher.SyncOnce`, so `<>(∀ n: catalogVersion[n] ≥ v)`
  would be vacuously falsifiable by such a node and the liveness
  checker would emit spurious counterexamples. The spec variable
  `catalogVersion[n]` corresponds to `Engine.catalogVersion` in
  `distribution/engine.go` (one field per `RouteEngine`, exposed via
  `Engine.Version()`). This is the liveness counterpart of Routes-4
  (which only bounds the order of observations, not their occurrence).
  The same `LiveNodes` (or `LiveTransactions` for OCC-L1) qualifier
  applies to any other `∀ n` / `∀ T` liveness property added later.

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

- **Per-module symmetry.** `SYMMETRY` reductions depend on whether the
  module treats a constant set as ordered or unordered. Picking the wrong
  reduction can silently collapse non-equivalent states and hide the bugs
  this proposal is meant to surface.
  - **Nodes** are symmetric in every module (a node only matters relative
    to its role — leader / follower — not its identity).
  - **Keys** in `HLC.tla`: `HLC.tla` references no key set; `MCHLC.cfg`
    omits the `Keys` constant entirely (the symmetry question does not
    arise). In `OCC.tla` keys are symmetric — they participate in
    `lock_map` / `read_set` / `write_set` as indices only, with no
    ordering on the OCC invariants. Keys are **not symmetric** in
    `MVCC.tla` (versions are ordered by `commit_ts` and keys could in
    principle be ordered too), and are **definitely not symmetric** in
    `Routes.tla` and `Composed.tla` because keys participate in
    ordered ranges via `SplitRange`. Swapping arbitrary keys in those
    modules can move them across split boundaries and collapse states
    with different owners as equivalent — exactly the stale-route /
    data-handoff schedules the spec is meant to analyse.
  - **Values, terms, group IDs** — never symmetric (they label
    distinguishable resources).
- Use a `StateConstraint` that bounds the number of operations and the
  largest term per group.
- For invariants that are checkable in a small state (e.g. HLC-1), bound to
  3 nodes / 2 keys / 2 terms. For larger composed checks, raise bounds
  selectively.

---

## 7. Repository Layout and Tooling

### 7.1 Files

The specs live in a top-level `tla/` directory (Section 4.1) so they can
be navigated and run without entering the Go module. `tla/README.md` is
part of M1 (Section 8.1) and must cover, at minimum:

- **Install** — how to obtain `tla2tools.jar` (checksum-pinned download to
  `.cache/tla/`) and Java; what TLC version is expected.
- **Run** — the exact `make tla-check` invocation, plus the per-module
  TLC commands and their `MC*.cfg` config so a reviewer can re-run any
  single module by hand without the Makefile.
- **What each module proves** — a one-line summary per module listing the
  invariants and properties it covers (cross-referenced to Section 5).
- **Reading list** — a short pointer set for TLA+ newcomers (the TLA+ Book,
  Specifying Systems, Lamport's video lectures; Diego Ongaro's Raft
  spec; the CockroachDB / TiDB MVCC specs cited in Section 11).
- **How to interpret a TLC failure** — counterexample dumps, how to map a
  state trace back to the implementation, and where to file issues.

### 7.2 Makefile targets

Two new targets, kept opt-in so the default `make test` is unchanged:

```make
tla-check:        ## run TLC on all modules with default bounds
tla-check-deep:   ## run TLC on Composed with extended bounds (~hours)
```

Both targets shell out to `tla2tools.jar` which is downloaded on first use
into `.cache/tla/` (matching the existing `.cache/` pattern from CLAUDE.md).
The downloaded jar is checksum-pinned. No system-wide install required.

### 7.3 CI integration

`tla-check` runs in CI via `.github/workflows/tla-check.yml` on every PR
(and push to `main`) that touches:

- `tla/**` and `Makefile` (the spec and the harness themselves).
- `kv/hlc*.go`, `kv/coordinator.go`, `kv/sharded_coordinator.go`,
  `kv/transaction.go`, `kv/fsm.go`, `kv/lock_resolver.go` — the HLC
  renewal scheduler, the timestamp-issuance dispatch path, and the
  OCC commit-ts assignment all live in these files (per Section 3
  anchors); a change here can violate HLC/OCC invariants and must
  re-run `tla-check`.
- `store/mvcc_store.go`
- `distribution/**`
- `.github/workflows/tla-check.yml` (the workflow file itself).

The workflow uses Temurin JDK 21, caches `.cache/tla/tla2tools.jar`
keyed on the Makefile hash so version-pin changes invalidate the cache
automatically, and inherits the Makefile's gap-config validation: a
non-zero TLC exit on `MCHLC_gap.cfg` only counts as success if the
output contains the exact string
`Invariant HLC4_NoRegressionAcrossTerms is violated`. Any other failure
mode (parse error, deadlock, JVM crash, different invariant) fails the
job. The full M1 run completes in well under a minute.

**Spec-divergence AI review.** A companion workflow
`.github/workflows/tla-spec-ai-review.yml` fires on the same path
filter and posts a PR comment that pings the Claude and Codex
reviewers (`@claude review` + `@codex review`) with a per-subsystem
checklist of which spec invariants to verify the implementation has
not drifted from.  The chain is:

```
anchored-file change in PR
  -> tla-check.yml runs TLC (catches *spec* bugs)
  -> tla-spec-ai-review.yml posts the review-request comment
       -> claude.yml fires on @claude in the comment
       -> chatgpt-codex-connector bot fires on @codex review
            -> both bots review the diff focused on TLA+ divergence
```

`tla-check` and the AI-review request are complementary — the model
check verifies the spec passes; the AI reviewers verify the
implementation matches the spec.  Both fire on every push to an
anchored file, so a divergence cannot land silently.  The path filter
must stay in sync across all three places (the two workflow files and
the list in §7.3).

---

## 8. Milestones

Each milestone is a separate PR. Milestones land in order; the proposal is
**not** all-or-nothing — value accrues incrementally (every milestone
proves something on its own).

### 8.1 Milestone list

| ID | Scope | Output | Notes |
|---|---|---|---|
| **M1** | `lib/Raft.tla`, `lib/Env.tla`, `hlc/HLC.tla` with invariants HLC-1..HLC-4 (encoding all three HLC-4 preconditions — the bounded-skew `ASSUME`, the logical-handoff strategy, **and** the ceiling-fencing gate from (iii)). `Makefile` `tla-check` target. `tla/README.md`. | Per-module TLA+ spec of HLC; TLC passes at default bounds (3 nodes, 2 terms, ≤20 ops) **against the spec-of-the-correct-design**: the M1 spec encodes precondition (i) as a TLA+ `ASSUME` (constant predicate `MaxClockSkewMs < HlcPhysicalWindowMs`), and encodes (ii) and (iii) as **action guards** — strategy (c) lives in the `BecomeLeader` action (`LET maxAppliedHLC == MaxAppliedHLC(fsm[n]) IN hlcLast[n]' = max(hlcLast[n], maxAppliedHLC)`, where `MaxAppliedHLC` is the new FSM operator described in §5.1), and the ceiling-fencing gate lives in the `IssueTimestamp` action (`ENABLED ⇔ wallNow[n] < physicalCeiling[n]`). A companion `HLC_gap.tla` + `MCHLC_gap.cfg` pair that removes the `ASSUME` and relaxes those action guards is expected to surface counterexamples; that pair is committed alongside M1 as the motivating evidence that the preconditions are necessary. (TLA+ `ASSUME` is only valid for constant-level predicates, so describing behavioural requirements as `ASSUME`s would produce invalid TLA+; the distinction matters for M1 implementers.) | Smallest viable PR; sets the directory layout precedent. **Default logical-handoff strategy: (c)** — `BecomeLeader` action calls `Observe(maxHLC)` (Section 5.1, HLC-4 (ii)); deviations from (c) must be justified in the PR. |
| **M2** | `occ/OCC.tla` with safety invariants OCC-1..OCC-5. | OCC spec runnable in isolation. | Re-uses `lib/Raft.tla`; introduces lock map, read/write sets, and per-transaction `start_ts` consistency. Liveness (OCC-L1) is deferred to M6. |
| **M3** | `mvcc/MVCC.tla` with invariants MVCC-1..MVCC-4. | MVCC spec runnable in isolation. | Includes a small `InstallSnapshot` action to exercise MVCC-4. |
| **M4** | `routes/Routes.tla` with invariants Routes-1..Routes-4. | Route catalog spec runnable in isolation. | Models `SplitRange` (catalog-atomic + handling-node engine sync) and asynchronous `CatalogWatcher` polling on other nodes. Liveness (Routes-L1) is deferred to M6. |
| **M5** | `composed/Composed.tla` with safety invariants Composed-1..Composed-3. CI integration. | Cross-subsystem spec; TLC at default bounds in <10 min. | Where interaction bugs surface. Safety only; liveness is M6. |
| **M6** *(optional)* | `tla-check-deep` configuration: larger bounds for `Composed`, plus liveness checking for OCC-L1 and Routes-L1 with their fairness assumptions. | Deeper coverage runnable on a beefier machine / off-CI. | Out of scope of the initial proposal — included only as a placeholder for the follow-up decision. Liveness checking is significantly more expensive than safety checking, hence the deferral. |

### 8.2 Doc lifecycle

This doc is `proposed` at M1 kickoff. It is renamed to `partial` when M1
lands (per CLAUDE.md's `*_partial_*.md` convention, since the proposal
spans multiple milestones). It is renamed to `implemented` when M5's TLC
run passes in CI on `main` — i.e. when the composed spec is not only
written but actively guarded by the build, since the protective value of
the spec depends on it running, not just existing.

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

7. **Fail-closed availability under partition.** HLC-4 precondition
   (iii) makes the ceiling-fence behaviour normative: a leader
   partitioned from the default group's quorum for longer than
   `hlcPhysicalWindowMs` (currently 3s) cannot serve any persistence
   timestamp, so client commits are rejected until renewal succeeds.
   This is a CP, not AP, trade-off and is a stricter regime than the
   current implementation (which silently keeps issuing). Mitigation:
   the M1 spec encodes the fence as an **action guard** on
   `IssueTimestamp` (`ENABLED ⇔ wallNow[n] < physicalCeiling[n]`, per
   §8.1 — `ASSUME` is only valid for constant predicates and does not
   apply here); the follow-up implementation PR that adds the fence
   to `HLC.Next()` /
   `ShardedCoordinator.RunHLCLeaseRenewal` must (a) surface the new
   error in operator-visible metrics and (b) document the
   `hlcPhysicalWindowMs` tuning guidance — operators sizing the
   window must trade it against expected partition duration.

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
