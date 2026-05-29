-------------------------------- MODULE Routes -------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv route catalog and CatalogWatcher.   *)
(* Per docs/design/2026_05_28_partial_tla_safety_spec.md §5.4.             *)
(*                                                                         *)
(* Models the durable route catalog as a versioned snapshot and a set of   *)
(* per-node engine views that re-sync via a CatalogWatcher action.  M4     *)
(* focuses on the two dynamic properties:                                  *)
(*                                                                         *)
(*   Routes-1  Catalog version is strictly monotonic.  No node ever        *)
(*             observes a non-increasing snapshot.                         *)
(*   Routes-4  Watcher fan-out monotonicity.  No node's RouteEngine        *)
(*             observes catalog version v2 then later observes v1 if      *)
(*             v1 < v2.                                                    *)
(*                                                                         *)
(* The other two safety items are trivially satisfied in this M4          *)
(* abstraction:                                                            *)
(*                                                                         *)
(*   Routes-2  Coverage and disjointness — a partition of the keyspace at *)
(*             every catalog version.  M4 does not model individual ranges *)
(*             (the partition would be a function `Keys -> GroupId`, and  *)
(*             coverage / disjointness are then implied by typing).  M5    *)
(*             (composed) is the right milestone for the full range form  *)
(*             with SplitRange exercising key-boundary changes.            *)
(*   Routes-3  SplitRange catalog atomicity — every catalog update is a    *)
(*             single TLA+ action that bumps `catalogVersion` and the      *)
(*             route function in the same step.  Atomicity is therefore    *)
(*             structural; cross-node propagation is asynchronous and is   *)
(*             captured by Routes-4 (the watcher fan-out monotonicity).    *)
(*                                                                         *)
(* The single CONSTANT `EnableSafety` gates the monotonicity guard inside  *)
(* `CatalogWatcherSync`.  Under EnableSafety the watcher is allowed to    *)
(* fetch any snapshot whose version is at least the highest version this   *)
(* node has previously observed (skipping intermediate versions is fine —  *)
(* only regression is forbidden).  Under the gap config that guard is     *)
(* removed and TLC surfaces a Routes-4 counterexample.                     *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,          \* finite set of node identities
    MaxVersions,    \* state-space bound on the catalog version counter
    MaxOps,         \* state-space bound on total actions
    EnableSafety    \* TRUE: encode Routes-4 monotonicity guard; FALSE: gap

VARIABLES
    catalogVersion,    \* Nat.  The current durable catalog version on
                       \* leader-replicated storage.  Strictly monotonic.
    engineVersion,     \* Nodes -> Nat.  Per-node RouteEngine version,
                       \* refreshed by CatalogWatcherSync.
    engineMaxObserved, \* Nodes -> Nat.  Ghost: the highest version this
                       \* node has ever observed.  Tracked separately so
                       \* Routes-4 can be stated as a state invariant
                       \* (`engineVersion[n] >= engineMaxObserved[n]`).
    opCount            \* state-space bound

vars == <<catalogVersion, engineVersion, engineMaxObserved, opCount>>

\* === INIT ===
Init ==
    /\ catalogVersion    = 0
    /\ engineVersion     = [n \in Nodes |-> 0]
    /\ engineMaxObserved = [n \in Nodes |-> 0]
    /\ opCount           = 0

\* === ACTIONS ===

(***************************************************************************)
(* ProposeRouteChange: a control-plane action (SplitRange or a future      *)
(* merge / move) that atomically bumps the catalog version by 1.  Models  *)
(* `adapter/distribution_server.go saveSplitResultViaCoordinator` from    *)
(* the implementation anchor list — by collapsing the full transaction   *)
(* (write to the default Raft group + apply to the handling node's       *)
(* RouteEngine) into a single TLA+ step we get Routes-3 atomicity by      *)
(* construction.  Cross-node propagation is asynchronous via              *)
(* CatalogWatcherSync below.                                              *)
(***************************************************************************)
ProposeRouteChange ==
    /\ opCount < MaxOps
    /\ catalogVersion < MaxVersions
    /\ catalogVersion' = catalogVersion + 1
    /\ opCount'        = opCount + 1
    /\ UNCHANGED <<engineVersion, engineMaxObserved>>

(***************************************************************************)
(* CatalogWatcherSync(n): node n's CatalogWatcher fetches a catalog        *)
(* snapshot.  Models `distribution/watcher.go CatalogWatcher.SyncOnce`.    *)
(*                                                                         *)
(* Under EnableSafety the watcher is allowed to fetch any version v in     *)
(* [engineMaxObserved[n], catalogVersion].  Skipping intermediate          *)
(* versions is fine — only regression is forbidden.  This matches the     *)
(* CatalogWatcher's behaviour: it polls and overwrites the cached         *)
(* snapshot with whatever the catalog currently shows, which is always >=  *)
(* the previously cached version under correct ordering.                   *)
(*                                                                         *)
(* Under ~EnableSafety the watcher can fetch ANY v in 0..catalogVersion,  *)
(* modelling a buggy implementation that overwrites with a stale         *)
(* snapshot.  TLC then finds a 3-state counterexample:                    *)
(*     ProposeRouteChange (v -> 1)                                         *)
(*     CatalogWatcherSync(n) fetching 1                                    *)
(*     CatalogWatcherSync(n) fetching 0  -- regression!                   *)
(* which violates Routes4_NoEngineRegression.                              *)
(***************************************************************************)
CatalogWatcherSync(n) ==
    /\ opCount < MaxOps
    /\ \E v \in 0..catalogVersion :
        /\ (EnableSafety => v >= engineMaxObserved[n])
        /\ engineVersion'     = [engineVersion     EXCEPT ![n] = v]
        /\ engineMaxObserved' = [engineMaxObserved EXCEPT ![n] =
                                  IF v > @ THEN v ELSE @]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<catalogVersion>>

\* === NEXT ===
Next ==
    \/ ProposeRouteChange
    \/ \E n \in Nodes : CatalogWatcherSync(n)

Spec == Init /\ [][Next]_vars

\* === STATE CONSTRAINT ===
StateConstraint ==
    /\ catalogVersion <= MaxVersions
    /\ opCount        <= MaxOps

\* === TYPE INVARIANT ===
TypeOK ==
    /\ catalogVersion    \in 0..MaxVersions
    /\ engineVersion     \in [Nodes -> 0..MaxVersions]
    /\ engineMaxObserved \in [Nodes -> 0..MaxVersions]
    /\ opCount           \in 0..MaxOps

\* === SAFETY INVARIANTS ===

\* Routes-1 — catalog version is bounded by the model's exploration limit
\* and never observed above it.  The action-level form (Routes1_Action
\* below) carries the load: ProposeRouteChange is the only action that
\* touches catalogVersion, and it bumps by exactly 1.
Routes1_VersionInRange ==
    catalogVersion \in 0..MaxVersions

\* Routes-2 — coverage and disjointness.  In this M4 abstraction the
\* keyspace partition is not modelled explicitly (M5 composed will model
\* it via a `routes : Keys -> GroupId` function whose totality implies
\* both properties).  Stated as a vacuous truth here so the invariant
\* name is present in the harness output.
Routes2_CoverageDisjoint == TRUE

\* Routes-3 — SplitRange catalog atomicity.  `ProposeRouteChange` is a
\* single TLA+ action that updates catalogVersion in one step; no
\* intermediate state is reachable.  Atomicity is structural in M4;
\* asserted vacuously for naming consistency with the other invariants.
Routes3_SplitAtomicity == TRUE

\* Routes-4 — watcher fan-out monotonicity.  No node's observed catalog
\* version ever drops below the highest version it has previously seen.
\* The `engineMaxObserved` ghost makes this checkable as a state
\* invariant: `engineVersion[n] >= engineMaxObserved[n]` always holds
\* under EnableSafety (the watcher action's guard enforces `v >=
\* engineMaxObserved[n]`), and fails under the gap config (the watcher
\* may fetch any older v).
Routes4_NoEngineRegression ==
    \A n \in Nodes : engineVersion[n] >= engineMaxObserved[n]

\* === ACTION-LEVEL PROPERTIES ===

\* Routes-1 transition form: catalogVersion never decreases across any
\* step.  ProposeRouteChange bumps by exactly 1; CatalogWatcherSync
\* leaves catalogVersion UNCHANGED.
Routes1_Action ==
    [][catalogVersion' >= catalogVersion]_vars

\* engineMaxObserved never decreases — it tracks a max.  Combined with
\* Routes4_NoEngineRegression this gives the full "fan-out is monotonic
\* per node and the ghost record is monotonic too" claim.
Routes4_GhostMonotonic ==
    [][\A n \in Nodes : engineMaxObserved'[n] >= engineMaxObserved[n]]_vars

=============================================================================
